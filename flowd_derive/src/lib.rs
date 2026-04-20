use proc_macro::TokenStream;
use quote::quote;
use syn::{
    Attribute, Data, DeriveInput, Fields, GenericArgument, PathArguments, Type, parse_macro_input,
};

// ── Field-level attribute options ─────────────────────────────────────────────

struct FieldMapper {
    serialize: Option<syn::Path>,
    deserialize: Option<syn::Path>,
}

fn parse_mapper_attr(attrs: &[Attribute]) -> FieldMapper {
    let mut serialize = None;
    let mut deserialize = None;

    for attr in attrs {
        // Skip non-#[mapper(...)] attributes
        if !attr.path().is_ident("mapper") {
            continue;
        }

        // Parse key = "value" pairs inside #[mapper(...)]
        attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("serialize") {
                serialize = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("deserialize") {
                deserialize = Some(meta.value()?.parse()?);
            }
            Ok(())
        })
        .unwrap();
    }

    FieldMapper {
        serialize,
        deserialize,
    }
}

// ── Main derive macro ─────────────────────────────────────────────────────────

/// Derive macro that generates bidirectional conversions between a struct
/// and `Vec<(String, redis::Value)>` pairs for zero-copy Redis integration.
///
/// # What it generates
///
/// - `From<Struct> for Vec<(String, redis::Value)>` — infallible owned conversion.
/// - `TryFrom<Vec<(String, redis::Value)>> for Struct` — owned pairs.
/// - `TryFrom<&[(String, redis::Value)]> for Struct` — borrowed pairs.
/// - `TryFrom<Vec<(&str, &str)>> for Struct` — convenience for tests.
/// - `Job` trait impl (used by the task queue internals).
///
/// # Field-level attributes
///
/// Use `#[mapper(...)]` to customize serialization per field:
///
/// | Attribute | Signature | Effect |
/// |-----------|-----------|--------|
/// | `serialize = "path"` | `fn(&T) -> Result<String, E>` | Replaces default `Display` serialization |
/// | `deserialize = "path"` | `fn(&str) -> Result<T, E>` | Replaces default `FromRedisValue` deserialization |
///
/// # Type support
///
/// - **Required fields** — must implement `Display` (serialize) and
///   `FromRedisValue` (deserialize), or use custom attributes.
/// - **`Option<T>`** — `None` is omitted on serialization; a missing key
///   deserializes as `None`.
///
/// # Example
///
/// ```rust,ignore
/// use flowd::prelude::*;
///
/// fn ser_tags(tags: &Vec<String>) -> Result<String, String> {
///     Ok(tags.join(","))
/// }
/// fn de_tags(s: &str) -> Result<Vec<String>, String> {
///     Ok(s.split(',').map(String::from).collect())
/// }
///
/// #[derive(Debug, Job)]
/// struct Job {
///     url: String,
///     priority: u32,
///     #[mapper(serialize = "ser_tags", deserialize = "de_tags")]
///     tags: Vec<String>,
///     /// Optional fields are skipped when `None`.
///     assigned_to: Option<String>,
/// }
/// ```
#[doc(hidden)]
#[proc_macro_derive(Job, attributes(mapper))]
pub fn derive_job(input: TokenStream) -> TokenStream {
    // Parse the annotated struct
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = &input.ident;

    // Extract named fields — enums and tuple structs are rejected
    let fields = match &input.data {
        Data::Struct(s) => match &s.fields {
            Fields::Named(f) => &f.named,
            _ => panic!("Job only supports structs with named fields"),
        },
        _ => panic!("Job only supports structs"),
    };

    // Pre-compute field metadata used across all generated impls
    let field_count = fields.len();
    let field_names: Vec<_> = fields.iter().map(|f| f.ident.as_ref().unwrap()).collect();
    let field_keys: Vec<_> = field_names.iter().map(|f| f.to_string()).collect();

    // ── Serialize: struct field → (key, redis::Value) pair ───────────────────
    //   Three cases:
    //     1. #[mapper(serialize = "my_fn")]  → call custom fn, wrap String in BulkString
    //     2. Option<T> with no attribute     → skip pair if None
    //     3. Default                         → .to_string() wrapped in BulkString
    let to_pairs_pushes: Vec<_> = fields
        .iter()
        .map(|field| {
            let name = field.ident.as_ref().unwrap();
            let key = name.to_string();
            let ty = &field.ty;
            let mapper = parse_mapper_attr(&field.attrs);

            if let Some(ser_fn) = mapper.serialize {
                // Custom serializer — expected signature: fn(&T) -> Result<String, E>
                quote! {
                    pairs.push((
                        #key.to_string(),
                        flowd::redis::Value::BulkString(
                            #ser_fn(&val.#name)
                                .map_err(|e| format!("serialize error on `{}`: {}", #key, e))?
                                .into_bytes()
                        ),
                    ));
                }
            } else if is_option(ty) {
                // Option<T> — only push if Some, skip entirely for None
                quote! {
                    if let Some(ref inner) = val.#name {
                        pairs.push((
                            #key.to_string(),
                            flowd::redis::Value::BulkString(
                                inner.to_string().into_bytes()
                            ),
                        ));
                    }
                }
            } else {
                // Default — requires field type to implement Display
                quote! {
                    pairs.push((
                        #key.to_string(),
                        flowd::redis::Value::BulkString(
                            val.#name.to_string().into_bytes()
                        ),
                    ));
                }
            }
        })
        .collect();

    // ── Deserialize: (key, redis::Value) pair → struct field ─────────────────
    //   Builds a temporary HashMap<&str, &redis::Value> for O(1) lookups, then:
    //     1. #[mapper(deserialize = "my_fn")]  → convert Value→String, call custom fn
    //     2. Option<T> with no attribute       → from_redis_value or None if missing
    //     3. Default                           → from_redis_value (requires FromRedisValue)
    let from_pairs_fields: Vec<_> = fields
        .iter()
        .map(|field| {
            let name = field.ident.as_ref().unwrap();
            let key = name.to_string();
            let ty = &field.ty;
            let mapper = parse_mapper_attr(&field.attrs);

            // .copied() turns Option<&&Value> into Option<&Value> so it can
            // be passed to from_redis_value (which takes &Value)
            let lookup = quote! { map.get(#key).copied() };

            if let Some(de_fn) = mapper.deserialize {
                if is_option(ty) {
                    // Custom deserializer + Option — missing key becomes None
                    quote! {
                        #name: match #lookup {
                            Some(v) => {
                                let s: String = flowd::redis::from_redis_value_ref(v)
                                    .map_err(|e| format!("deserialize error on `{}`: {}", #key, e))?;
                                if s.is_empty() {
                                    None
                                } else {
                                    Some(#de_fn(&s)
                                        .map_err(|e| format!("deserialize error on `{}`: {}", #key, e))?)
                                }
                            }
                            None => None,
                        },
                    }
                } else {
                    // Custom deserializer — required field
                    quote! {
                        #name: {
                            let v = #lookup
                                .ok_or_else(|| format!("missing field: `{}`", #key))?;
                            let s: String = flowd::redis::from_redis_value_ref(v)
                                .map_err(|e| format!("deserialize error on `{}`: {}", #key, e))?;
                            #de_fn(&s)
                                .map_err(|e| format!("deserialize error on `{}`: {}", #key, e))?
                        },
                    }
                }
            } else if is_option(ty) {
                // No custom deserializer + Option<T> — missing key becomes None
                let inner = extract_option_inner(ty);
                quote! {
                    #name: match #lookup {
                        Some(v) => Some(
                            flowd::redis::from_redis_value_ref::<#inner>(v)
                                .map_err(|e| format!("parse error on `{}`: {}", #key, e))?
                        ),
                        None => None,
                    },
                }
            } else {
                // Default — requires field type to implement FromRedisValue
                quote! {
                    #name: flowd::redis::from_redis_value_ref::<#ty>(
                        #lookup.ok_or_else(|| format!("missing field: `{}`", #key))?
                    ).map_err(|e| format!("failed to parse field `{}`: {}", #key, e))?,
                }
            }
        })
        .collect();

    // Re-borrow so the Vecs can be interpolated in multiple quote! blocks
    // (quote! moves by default; referencing avoids that)
    let to_pairs_pushes = &to_pairs_pushes;
    let from_pairs_fields = &from_pairs_fields;

    // ── From<Struct> for Vec<(String, redis::Value)> ─────────────────────────
    let from_struct_impl = quote! {
        impl From<#struct_name> for Vec<(String, flowd::redis::Value)> {
            fn from(val: #struct_name) -> Self {
                #struct_name::try_to_pairs(val).unwrap_or_default()
            }
        }
    };

    // ── TryFrom<Vec<(String, redis::Value)>> for Struct (owned + borrowed) ───
    let try_from_impl = quote! {
        impl TryFrom<Vec<(String, flowd::redis::Value)>> for #struct_name {
            type Error = String;

            fn try_from(
                pairs: Vec<(String, flowd::redis::Value)>
            ) -> Result<Self, Self::Error> {
                Self::try_from(pairs.as_slice())
            }
        }

        impl TryFrom<&[(String, flowd::redis::Value)]> for #struct_name {
            type Error = String;

            fn try_from(
                pairs: &[(String, flowd::redis::Value)]
            ) -> Result<Self, Self::Error> {
                let map: std::collections::HashMap<&str, &flowd::redis::Value> = pairs
                    .iter()
                    .map(|(k, v)| (k.as_str(), v))
                    .collect();
                Ok(Self { #(#from_pairs_fields)* })
            }
        }
    };

    // ── TryFrom<Vec<(&str, &str)>> for Struct ────────────────────────────────
    // Convenience for testing — wraps each value in BulkString.
    let from_borrowed_impl = quote! {
        impl TryFrom<Vec<(&str, &str)>> for #struct_name {
            type Error = String;

            fn try_from(
                pairs: Vec<(&str, &str)>
            ) -> Result<Self, Self::Error> {
                let owned: Vec<(String, flowd::redis::Value)> = pairs
                    .into_iter()
                    .map(|(k, v)| (
                        k.to_string(),
                        flowd::redis::Value::BulkString(v.as_bytes().to_vec()),
                    ))
                    .collect();
                Self::try_from(owned)
            }
        }
    };

    // ── Job companion trait impl ────────────────────────────────────
    let job_trait_impl = quote! {
        impl #struct_name {
            pub fn try_to_pairs(
                self
            ) -> Result<Vec<(String, flowd::redis::Value)>, String> {
                let mut pairs = Vec::with_capacity(#field_count);
                let val = self;
                #(#to_pairs_pushes)*
                Ok(pairs)
            }
        }

        impl flowd::Job for #struct_name {
            fn try_to_pairs(
                self
            ) -> Result<Vec<(String, flowd::redis::Value)>, String> {
                #struct_name::try_to_pairs(self)
            }

            fn try_from_pairs(
                pairs: &[(String, flowd::redis::Value)]
            ) -> Result<Self, String> {
                Self::try_from(pairs)
            }

            fn fields() -> &'static [&'static str] {
                &[#(#field_keys),*]
            }
        }
    };

    quote! {
        #from_struct_impl
        #try_from_impl
        #from_borrowed_impl
        #job_trait_impl
    }
    .into()
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn is_option(ty: &Type) -> bool {
    if let Type::Path(tp) = ty {
        tp.path
            .segments
            .last()
            .map(|s| s.ident == "Option")
            .unwrap_or(false)
    } else {
        false
    }
}

fn extract_option_inner(ty: &Type) -> &Type {
    if let Type::Path(tp) = ty {
        if let PathArguments::AngleBracketed(args) = &tp.path.segments.last().unwrap().arguments {
            if let Some(GenericArgument::Type(inner)) = args.args.first() {
                return inner;
            }
        }
    }
    panic!("Could not extract inner type from Option<T>");
}
