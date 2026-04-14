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
        if !attr.path().is_ident("mapper") {
            continue;
        }

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

/// Derives:
/// - `From<TargetStruct> for Vec<(String, redis::Value)>`
/// - `TryFrom<Vec<(String, redis::Value)>> for TargetStruct`  (owned + borrowed slice)
/// - `TryFrom<Vec<(&str, &str)>> for TargetStruct`            (testing convenience)
/// - `HashMappable` companion trait impl
#[proc_macro_derive(HashMapper, attributes(mapper))]
pub fn derive_hash_mapper(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let struct_name = &input.ident;

    let fields = match &input.data {
        Data::Struct(s) => match &s.fields {
            Fields::Named(f) => &f.named,
            _ => panic!("HashMapper only supports structs with named fields"),
        },
        _ => panic!("HashMapper only supports structs"),
    };

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
                        hash_mapper::redis::Value::BulkString(
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
                            hash_mapper::redis::Value::BulkString(
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
                        hash_mapper::redis::Value::BulkString(
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

            let lookup = quote! { map.get(#key) };

            if let Some(de_fn) = mapper.deserialize {
                if is_option(ty) {
                    // Custom deserializer + Option — missing key becomes None
                    quote! {
                        #name: match #lookup {
                            Some(v) => {
                                let s: String = hash_mapper::redis::from_redis_value(v)
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
                            let s: String = hash_mapper::redis::from_redis_value(v)
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
                            hash_mapper::redis::from_redis_value::<#inner>(v)
                                .map_err(|e| format!("parse error on `{}`: {}", #key, e))?
                        ),
                        None => None,
                    },
                }
            } else {
                // Default — requires field type to implement FromRedisValue
                quote! {
                    #name: hash_mapper::redis::from_redis_value::<#ty>(
                        #lookup.ok_or_else(|| format!("missing field: `{}`", #key))?
                    ).map_err(|e| format!("failed to parse field `{}`: {}", #key, e))?,
                }
            }
        })
        .collect();

    // Re-borrow so we can use the Vecs in multiple quote! blocks
    let to_pairs_pushes = &to_pairs_pushes;
    let from_pairs_fields = &from_pairs_fields;

    // ── From<Struct> for Vec<(String, redis::Value)> ─────────────────────────
    let from_struct_impl = quote! {
        impl From<#struct_name> for Vec<(String, hash_mapper::redis::Value)> {
            fn from(val: #struct_name) -> Self {
                #struct_name::try_to_pairs(val).unwrap_or_default()
            }
        }
    };

    // ── TryFrom<Vec<(String, redis::Value)>> for Struct (owned + borrowed) ───
    let try_from_impl = quote! {
        impl TryFrom<Vec<(String, hash_mapper::redis::Value)>> for #struct_name {
            type Error = String;

            fn try_from(
                pairs: Vec<(String, hash_mapper::redis::Value)>
            ) -> Result<Self, Self::Error> {
                Self::try_from(pairs.as_slice())
            }
        }

        impl TryFrom<&[(String, hash_mapper::redis::Value)]> for #struct_name {
            type Error = String;

            fn try_from(
                pairs: &[(String, hash_mapper::redis::Value)]
            ) -> Result<Self, Self::Error> {
                let map: std::collections::HashMap<&str, &hash_mapper::redis::Value> = pairs
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
                let owned: Vec<(String, hash_mapper::redis::Value)> = pairs
                    .into_iter()
                    .map(|(k, v)| (
                        k.to_string(),
                        hash_mapper::redis::Value::BulkString(v.as_bytes().to_vec()),
                    ))
                    .collect();
                Self::try_from(owned)
            }
        }
    };

    // ── HashMappable companion trait impl ────────────────────────────────────
    let hash_mappable_impl = quote! {
        impl #struct_name {
            pub fn try_to_pairs(
                self
            ) -> Result<Vec<(String, hash_mapper::redis::Value)>, String> {
                let mut pairs = Vec::with_capacity(#field_count);
                let val = self;
                #(#to_pairs_pushes)*
                Ok(pairs)
            }
        }

        impl hash_mapper::HashMappable for #struct_name {
            fn try_to_pairs(
                self
            ) -> Result<Vec<(String, hash_mapper::redis::Value)>, String> {
                #struct_name::try_to_pairs(self)
            }

            fn try_from_pairs(
                pairs: &[(String, hash_mapper::redis::Value)]
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
        #hash_mappable_impl
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
