/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use darling::{FromDeriveInput, FromField, FromVariant};
use proc_macro2::TokenStream as TokenStream2;
use quote::{format_ident, quote};
use syn::{DeriveInput, Generics, Ident, Type};

/// Maximum number of array elements to generate env var mappings for.
///
/// For `Vec<T>` fields, mappings are generated for indices 0 through 9 (e.g.,
/// `FIELD_0_NAME`, `FIELD_1_NAME`, ..., `FIELD_9_NAME`). Environment variables
/// for indices beyond this limit are silently ignored.
///
/// This limit is sufficient for all known Iggy configuration arrays.
const MAX_ARRAY_ELEMENTS: usize = 10;

/// Container-level attributes for `#[config_env(...)]`
#[derive(Debug, FromDeriveInput)]
#[darling(attributes(config_env), supports(struct_named, enum_any))]
pub struct ConfigEnvOpts {
    ident: Ident,
    generics: Generics,
    data: darling::ast::Data<VariantOpts, FieldOpts>,

    /// Environment variable prefix (e.g., "IGGY_"). Only needed on root config.
    #[darling(default)]
    prefix: Option<String>,

    /// Metadata name for figment Provider (e.g., "iggy-server-config"). Only needed on root config.
    #[darling(default)]
    name: Option<String>,

    /// Tag field name for internally-tagged serde enums (mirrors `#[serde(tag = "...")]`).
    #[darling(default)]
    tag: Option<String>,
}

/// Variant-level attributes for `#[config_env(...)]` on enums
#[derive(Debug, FromVariant)]
#[darling(attributes(config_env))]
struct VariantOpts {
    #[allow(dead_code)]
    ident: Ident,
    fields: darling::ast::Fields<FieldOpts>,

    /// Skip this variant entirely
    #[darling(default)]
    skip: bool,
}

/// Field-level attributes for `#[config_env(...)]`
#[derive(Debug, FromField)]
#[darling(attributes(config_env))]
struct FieldOpts {
    ident: Option<Ident>,
    ty: Type,

    /// Explicit env var segment name (overrides field name conversion)
    #[darling(default)]
    name: Option<String>,

    /// Skip this field entirely
    #[darling(default)]
    skip: bool,

    /// Mark as secret (for builder documentation)
    #[darling(default)]
    secret: bool,

    /// Treat this field as a leaf value (not a nested config struct)
    #[darling(default)]
    leaf: bool,
}

/// Represents a single environment variable mapping (leaf field)
struct EnvMapping {
    const_name: Ident,
    /// Relative env name suffix (e.g., "ENABLED" not "IGGY_HTTP_ENABLED")
    env_suffix: String,
    /// Relative config path (e.g., "enabled" not "http.enabled")
    config_path: String,
    is_secret: bool,
    builder_method_name: Ident,
}

/// Information about a nested field for mapping collection
struct NestedFieldInfo {
    /// The element type (inner type for Vec/Arc/Box/Option)
    element_type: Type,
    /// Whether this is a Vec field requiring array index expansion
    is_vec: bool,
    /// The field name in UPPER_CASE for env var construction
    field_env_segment: String,
    /// The field name for config path construction
    field_name: String,
}

pub fn generate_impl(input: &DeriveInput) -> TokenStream2 {
    match ConfigEnvOpts::from_derive_input(input) {
        Ok(opts) => generate_from_opts(opts),
        Err(e) => e.write_errors(),
    }
}

fn generate_from_opts(opts: ConfigEnvOpts) -> TokenStream2 {
    let type_name = &opts.ident;
    let (impl_generics, ty_generics, where_clause) = opts.generics.split_for_impl();

    match opts.data {
        darling::ast::Data::Struct(fields) => generate_struct_impl(
            type_name,
            impl_generics,
            ty_generics,
            where_clause,
            opts.prefix,
            opts.name,
            fields.fields,
        ),
        darling::ast::Data::Enum(variants) => generate_enum_impl(
            type_name,
            impl_generics,
            ty_generics,
            where_clause,
            opts.tag,
            variants,
        ),
    }
}

/// Generate implementation for enum types.
/// Combines mappings from all variant inner types that implement ConfigEnvMappings.
fn generate_enum_impl(
    enum_name: &Ident,
    impl_generics: syn::ImplGenerics,
    ty_generics: syn::TypeGenerics,
    where_clause: Option<&syn::WhereClause>,
    tag: Option<String>,
    variants: Vec<VariantOpts>,
) -> TokenStream2 {
    // Collect inner types from newtype variants (single unnamed field)
    let variant_types: Vec<&Type> = variants
        .iter()
        .filter(|v| !v.skip)
        .filter_map(|v| {
            // Only handle newtype variants: Variant(InnerType)
            if v.fields.style == darling::ast::Style::Tuple && v.fields.fields.len() == 1 {
                let field = &v.fields.fields[0];
                if !field.skip {
                    return Some(&field.ty);
                }
            }
            None
        })
        .collect();

    let tag_mapping = tag.map(|tag_name| {
        let env_segment = tag_name.to_uppercase();
        quote! {
            all_mappings.push(configs::EnvVarMapping {
                env_name: #env_segment,
                config_path: #tag_name,
                is_secret: false,
            });
        }
    });

    if variant_types.is_empty() && tag_mapping.is_none() {
        return quote! {
            impl #impl_generics configs::ConfigEnvMappings for #enum_name #ty_generics #where_clause {
                fn env_mappings() -> &'static [configs::EnvVarMapping] {
                    &[]
                }
            }
        };
    }

    // Generate code to extend from each variant type
    let extends: Vec<TokenStream2> = variant_types
        .iter()
        .map(|ty| {
            quote! {
                all_mappings.extend_from_slice(<#ty as configs::ConfigEnvMappings>::env_mappings());
            }
        })
        .collect();

    quote! {
        impl #impl_generics configs::ConfigEnvMappings for #enum_name #ty_generics #where_clause {
            fn env_mappings() -> &'static [configs::EnvVarMapping] {
                static MAPPINGS: std::sync::OnceLock<Vec<configs::EnvVarMapping>> = std::sync::OnceLock::new();
                MAPPINGS.get_or_init(|| {
                    let mut all_mappings: Vec<configs::EnvVarMapping> = Vec::new();
                    #tag_mapping
                    #(#extends)*
                    all_mappings
                })
            }
        }
    }
}

/// Generate implementation for struct types.
fn generate_struct_impl(
    struct_name: &Ident,
    impl_generics: syn::ImplGenerics,
    ty_generics: syn::TypeGenerics,
    where_clause: Option<&syn::WhereClause>,
    prefix: Option<String>,
    name: Option<String>,
    fields: Vec<FieldOpts>,
) -> TokenStream2 {
    let prefix_str = prefix.as_deref().unwrap_or("");
    let has_prefix = !prefix_str.is_empty();

    // Metadata name: use provided or derive from type name (e.g., "ServerConfig" -> "server-config")
    let metadata_name = name.unwrap_or_else(|| {
        let type_name = struct_name.to_string();
        let mut result = String::new();
        for (i, c) in type_name.chars().enumerate() {
            if c.is_uppercase() && i > 0 {
                result.push('-');
            }
            result.push(c.to_ascii_lowercase());
        }
        result
    });

    let (mappings, nested_fields) = collect_mappings(&fields);

    let const_definitions = generate_const_definitions(&mappings, prefix_str);
    let mapping_entries = generate_mapping_entries(&mappings);
    let builder_methods = generate_builder_methods(&mappings, struct_name, prefix_str);
    let builder_name = format_ident!("{}EnvBuilder", struct_name);
    let mappings_count = mappings.len();

    // Generate code to include nested type mappings
    let nested_extends: Vec<TokenStream2> = nested_fields
        .iter()
        .map(|info| {
            let ty = &info.element_type;
            let field_env_segment = &info.field_env_segment;
            let field_name = &info.field_name;

            if info.is_vec {
                // For Vec fields, expand with array indices
                let max_elements = MAX_ARRAY_ELEMENTS;
                quote! {
                    let nested_mappings = <#ty as configs::ConfigEnvMappings>::env_mappings();
                    for i in 0..#max_elements {
                        for mapping in nested_mappings {
                            // Transform env_name by prepending FIELD_INDEX_
                            let new_env_name: &'static str = Box::leak(
                                format!("{}_{}_{}", #field_env_segment, i, mapping.env_name).into_boxed_str()
                            );
                            // Transform config_path by prepending field.index.
                            let new_config_path: &'static str = Box::leak(
                                format!("{}.{}.{}", #field_name, i, mapping.config_path).into_boxed_str()
                            );
                            all_mappings.push(configs::EnvVarMapping {
                                env_name: new_env_name,
                                config_path: new_config_path,
                                is_secret: mapping.is_secret,
                            });
                        }
                    }
                }
            } else {
                // For non-Vec nested types, transform by prepending field name
                quote! {
                    let nested_mappings = <#ty as configs::ConfigEnvMappings>::env_mappings();
                    for mapping in nested_mappings {
                        // Transform env_name by prepending FIELD_
                        let new_env_name: &'static str = Box::leak(
                            format!("{}_{}", #field_env_segment, mapping.env_name).into_boxed_str()
                        );
                        // Transform config_path by prepending field.
                        let new_config_path: &'static str = Box::leak(
                            format!("{}.{}", #field_name, mapping.config_path).into_boxed_str()
                        );
                        all_mappings.push(configs::EnvVarMapping {
                            env_name: new_env_name,
                            config_path: new_config_path,
                            is_secret: mapping.is_secret,
                        });
                    }
                }
            }
        })
        .collect();

    let prefix_application = if has_prefix {
        quote! {
            let all_mappings: Vec<configs::EnvVarMapping> = all_mappings
                .into_iter()
                .map(|m| configs::EnvVarMapping {
                    env_name: Box::leak(format!("{}{}", #prefix_str, m.env_name).into_boxed_str()),
                    config_path: m.config_path,
                    is_secret: m.is_secret,
                })
                .collect();
        }
    } else {
        quote! {}
    };

    // If there are nested types, we need to use OnceLock to combine mappings
    let env_mappings_impl = if nested_fields.is_empty() && !has_prefix {
        // Simple case: no nested fields, no prefix transformation needed
        quote! {
            fn env_mappings() -> &'static [configs::EnvVarMapping] {
                static MAPPINGS: [configs::EnvVarMapping; #mappings_count] = [
                    #(#mapping_entries),*
                ];
                &MAPPINGS
            }
        }
    } else {
        // Complex case: need OnceLock for dynamic construction
        quote! {
            fn env_mappings() -> &'static [configs::EnvVarMapping] {
                static MAPPINGS: std::sync::OnceLock<Vec<configs::EnvVarMapping>> = std::sync::OnceLock::new();
                MAPPINGS.get_or_init(|| {
                    let own_mappings: [configs::EnvVarMapping; #mappings_count] = [
                        #(#mapping_entries),*
                    ];
                    let mut all_mappings = Vec::from(own_mappings);
                    #(#nested_extends)*
                    #prefix_application
                    all_mappings
                })
            }
        }
    };

    quote! {
        impl #impl_generics #struct_name #ty_generics #where_clause {
            /// Environment variable prefix for this config type.
            pub const ENV_PREFIX: &'static str = #prefix_str;

            /// Metadata name for figment Provider.
            pub const ENV_PROVIDER_NAME: &'static str = #metadata_name;

            #(#const_definitions)*
        }

        impl #impl_generics configs::ConfigEnvMappings for #struct_name #ty_generics #where_clause {
            #env_mappings_impl
        }

        /// Type-safe builder for constructing environment variable maps for tests.
        #[derive(Debug, Default, Clone)]
        pub struct #builder_name {
            envs: std::collections::HashMap<String, String>,
        }

        impl #builder_name {
            /// Create a new builder instance.
            pub fn new() -> Self {
                Self::default()
            }

            #(#builder_methods)*

            /// Set a custom environment variable.
            pub fn custom(mut self, key: impl Into<String>, value: impl std::fmt::Display) -> Self {
                self.envs.insert(key.into(), value.to_string());
                self
            }

            /// Extend with another builder's values.
            pub fn extend(mut self, other: Self) -> Self {
                self.envs.extend(other.envs);
                self
            }

            /// Build the final environment variable map.
            pub fn build(self) -> std::collections::HashMap<String, String> {
                self.envs
            }

            /// Build and merge with existing environment variables.
            pub fn build_with(self, existing: std::collections::HashMap<String, String>) -> std::collections::HashMap<String, String> {
                let mut result = existing;
                result.extend(self.envs);
                result
            }
        }
    }
}

/// Check if a type is a Rust primitive that doesn't need nested expansion.
fn is_primitive_type(ty: &Type) -> bool {
    let Type::Path(type_path) = ty else {
        return false;
    };
    let Some(segment) = type_path.path.segments.last() else {
        return false;
    };

    let ident = segment.ident.to_string();

    // Rust primitives only
    if matches!(
        ident.as_str(),
        "bool"
            | "u8"
            | "u16"
            | "u32"
            | "u64"
            | "u128"
            | "i8"
            | "i16"
            | "i32"
            | "i64"
            | "i128"
            | "f32"
            | "f64"
            | "usize"
            | "isize"
            | "String"
            | "str"
    ) {
        return true;
    }

    // Option<T> - check inner type
    if ident == "Option"
        && let syn::PathArguments::AngleBracketed(args) = &segment.arguments
        && let Some(syn::GenericArgument::Type(inner)) = args.args.first()
    {
        return is_primitive_type(inner);
    }

    false
}

/// Extract type info, unwrapping Vec/Arc/Box/Option.
/// Returns (inner_type, is_vec).
fn extract_type_info(ty: &Type) -> (Type, bool) {
    let Type::Path(type_path) = ty else {
        return (ty.clone(), false);
    };
    let Some(segment) = type_path.path.segments.last() else {
        return (ty.clone(), false);
    };

    let ident = segment.ident.to_string();
    match ident.as_str() {
        "Vec" => {
            if let syn::PathArguments::AngleBracketed(args) = &segment.arguments
                && let Some(syn::GenericArgument::Type(inner)) = args.args.first()
            {
                let (inner_type, _) = extract_type_info(inner);
                return (inner_type, true);
            }
        }
        "Arc" | "Box" | "Rc" | "Option" => {
            if let syn::PathArguments::AngleBracketed(args) = &segment.arguments
                && let Some(syn::GenericArgument::Type(inner)) = args.args.first()
            {
                return extract_type_info(inner);
            }
        }
        _ => {}
    }
    (ty.clone(), false)
}

fn collect_mappings(fields: &[FieldOpts]) -> (Vec<EnvMapping>, Vec<NestedFieldInfo>) {
    let mut mappings = Vec::new();
    let mut nested_fields = Vec::new();

    for field in fields {
        if field.skip {
            continue;
        }

        let field_ident = match &field.ident {
            Some(ident) => ident,
            None => continue,
        };

        let field_name = field_ident.to_string();
        let field_env_segment = field_name.to_uppercase();

        // Check if this is a leaf type or needs nested handling
        let (inner_type, is_vec) = extract_type_info(&field.ty);

        // A field is a leaf if:
        // 1. It's explicitly marked with #[config_env(leaf)]
        // 2. It's a Rust primitive type (bool, u32, String, etc.)
        let is_leaf = field.leaf || is_primitive_type(&inner_type);

        if is_leaf {
            // Leaf field: generate a direct mapping with relative paths
            let env_suffix = field
                .name
                .clone()
                .unwrap_or_else(|| field_env_segment.clone());

            let const_name = format_ident!("ENV_VAR_{}", field_env_segment);
            let builder_method_name = format_ident!("{}", field_name);

            mappings.push(EnvMapping {
                const_name,
                env_suffix,
                config_path: field_name,
                is_secret: field.secret,
                builder_method_name,
            });
        } else {
            // Nested field: will include mappings from the nested type
            nested_fields.push(NestedFieldInfo {
                element_type: inner_type,
                is_vec,
                field_env_segment,
                field_name,
            });
        }
    }

    (mappings, nested_fields)
}

fn generate_const_definitions(mappings: &[EnvMapping], prefix: &str) -> Vec<TokenStream2> {
    mappings
        .iter()
        .map(|m| {
            let const_name = &m.const_name;
            let env_var_name = format!("{}{}", prefix, m.env_suffix);
            quote! {
                pub const #const_name: &'static str = #env_var_name;
            }
        })
        .collect()
}

fn generate_mapping_entries(mappings: &[EnvMapping]) -> Vec<TokenStream2> {
    mappings
        .iter()
        .map(|m| {
            let env_suffix = &m.env_suffix;
            let config_path = &m.config_path;
            let is_secret = m.is_secret;

            quote! {
                configs::EnvVarMapping {
                    env_name: #env_suffix,
                    config_path: #config_path,
                    is_secret: #is_secret,
                }
            }
        })
        .collect()
}

fn generate_builder_methods(
    mappings: &[EnvMapping],
    struct_name: &Ident,
    prefix: &str,
) -> Vec<TokenStream2> {
    mappings
        .iter()
        .map(|m| {
            let method_name = &m.builder_method_name;
            let const_name = &m.const_name;
            let env_var_name = format!("{}{}", prefix, m.env_suffix);
            let doc_secret = if m.is_secret {
                " (secret - will be masked in logs)"
            } else {
                ""
            };
            let doc = format!("Set `{}` environment variable.{}", env_var_name, doc_secret);

            quote! {
                #[doc = #doc]
                pub fn #method_name(mut self, value: impl std::fmt::Display) -> Self {
                    self.envs.insert(#struct_name::#const_name.into(), value.to_string());
                    self
                }
            }
        })
        .collect()
}
