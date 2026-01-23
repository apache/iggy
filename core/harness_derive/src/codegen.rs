/* Licensed to the Apache Software Foundation (ASF) under one
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

use crate::attrs::{ConfigOverride, IggyTestAttrs, TlsMode, Transport};
use crate::params::{
    DetectedParam, analyze_signature, fixture_params, matrix_params, needs_client, needs_fixtures,
    needs_harness, needs_harness_mut, needs_mcp_client,
};
use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};
use syn::{Ident, ItemFn};

/// Represents a single test variant configuration.
#[derive(Debug)]
struct TestVariant {
    transport: Transport,
    /// Generic config overrides as (path, value) pairs.
    config_values: Vec<(String, String)>,
    /// TLS config from transport (not from server attrs).
    tls: Option<TlsMode>,
    /// WebSocket TLS from transport (not from server attrs).
    websocket_tls: Option<TlsMode>,
}

impl TestVariant {
    fn suffix(&self) -> String {
        let mut parts = vec![self.transport.as_str().to_string()];

        for (path, value) in &self.config_values {
            let key = path.rsplit('.').next().unwrap_or(path);
            parts.push(format!("{}_{}", key, sanitize_value(value)));
        }

        parts.join("_")
    }
}

fn sanitize_value(s: &str) -> String {
    s.to_lowercase()
        .replace("kib", "kb")
        .replace("mib", "mb")
        .replace("gib", "gb")
        .chars()
        .filter(|c| c.is_alphanumeric() || *c == '_')
        .collect()
}

/// Compute cartesian product of config override variants.
fn cartesian_product(overrides: &[ConfigOverride]) -> Vec<Vec<(String, String)>> {
    if overrides.is_empty() {
        return vec![vec![]];
    }

    let mut result = vec![vec![]];

    for config_override in overrides {
        let variants = config_override.value.variants();
        let mut new_result = Vec::new();

        for existing in &result {
            for variant_value in &variants {
                let mut new_combo = existing.clone();
                if let Some(val) = variant_value {
                    new_combo.push((config_override.path.clone(), val.to_string()));
                }
                new_result.push(new_combo);
            }
        }

        result = new_result;
    }

    result
}

/// Generate all infrastructure variants from attributes.
fn generate_variants(attrs: &IggyTestAttrs) -> Vec<TestVariant> {
    let config_combos = cartesian_product(&attrs.server.config_overrides);

    let mut variants = Vec::new();

    for transport in &attrs.transports {
        let tls = transport.tls_mode();
        let websocket_tls = if transport.is_websocket() { tls } else { None };
        let tcp_tls = if transport.is_websocket() { None } else { tls };

        for combo in &config_combos {
            variants.push(TestVariant {
                transport: *transport,
                config_values: combo.clone(),
                tls: tcp_tls,
                websocket_tls,
            });
        }
    }

    variants
}

/// Generate test code from attributes and input function.
pub fn generate_tests(attrs: &IggyTestAttrs, input: &ItemFn) -> syn::Result<TokenStream> {
    let fn_name = &input.sig.ident;
    let fn_vis = &input.vis;
    let fn_body = &input.block;

    let params = analyze_signature(&input.sig)?;
    let has_client = needs_client(&params);
    let has_harness = needs_harness(&params);
    let has_harness_mut = needs_harness_mut(&params);
    let has_mcp_client = needs_mcp_client(&params);
    let matrix_params_list = matrix_params(&params);
    let has_matrix_params = !matrix_params_list.is_empty();

    let variants = generate_variants(attrs);

    if variants.len() == 1 && !has_matrix_params {
        return generate_single_test(
            fn_name,
            fn_vis,
            fn_body,
            &params,
            &variants[0],
            has_client,
            has_harness,
            has_harness_mut,
            has_mcp_client,
            attrs,
        );
    }

    if has_matrix_params {
        return generate_impl_functions_for_test_matrix(
            fn_name,
            fn_vis,
            fn_body,
            input,
            &params,
            &variants,
            has_client,
            has_harness,
            has_harness_mut,
            has_mcp_client,
            attrs,
        );
    }

    generate_test_module(
        fn_name,
        fn_vis,
        fn_body,
        &params,
        &variants,
        has_client,
        has_harness,
        has_harness_mut,
        has_mcp_client,
        attrs,
    )
}

#[allow(clippy::too_many_arguments)]
fn generate_single_test(
    fn_name: &Ident,
    fn_vis: &syn::Visibility,
    fn_body: &syn::Block,
    params: &[DetectedParam],
    variant: &TestVariant,
    has_client: bool,
    has_harness: bool,
    has_harness_mut: bool,
    has_mcp_client: bool,
    attrs: &IggyTestAttrs,
) -> syn::Result<TokenStream> {
    let has_fixtures = needs_fixtures(params);
    let fixture_setup = generate_fixture_setup(params);
    let fixture_envs = generate_fixture_envs_collection(params);
    let harness_setup =
        generate_harness_setup(variant, has_client, has_mcp_client, has_fixtures, attrs);
    let client_setup = if has_client {
        generate_client_setup()
    } else {
        quote!()
    };
    let fixture_seed = generate_fixture_seed(params);
    let start_and_seed = generate_start_and_seed(attrs, fixture_seed);
    let mcp_client_setup = generate_mcp_client_setup(has_mcp_client);
    let param_bindings = generate_param_bindings(params, has_harness, has_harness_mut);

    Ok(quote! {
        #[::tokio::test]
        #[::serial_test::parallel]
        #fn_vis async fn #fn_name() {
            #fixture_setup
            #fixture_envs
            #harness_setup
            #start_and_seed
            #client_setup
            #mcp_client_setup
            #param_bindings
            #fn_body
            __harness.stop().await.ok();
        }
    })
}

#[allow(clippy::too_many_arguments)]
fn generate_test_module(
    fn_name: &Ident,
    fn_vis: &syn::Visibility,
    fn_body: &syn::Block,
    params: &[DetectedParam],
    variants: &[TestVariant],
    has_client: bool,
    has_harness: bool,
    has_harness_mut: bool,
    has_mcp_client: bool,
    attrs: &IggyTestAttrs,
) -> syn::Result<TokenStream> {
    let has_fixtures = needs_fixtures(params);
    let fixture_setup = generate_fixture_setup(params);
    let fixture_envs = generate_fixture_envs_collection(params);
    let fixture_seed = generate_fixture_seed(params);
    let param_bindings = generate_param_bindings(params, has_harness, has_harness_mut);

    let mut test_fns = Vec::new();

    for variant in variants {
        let test_name = format_ident!("{}", variant.suffix());
        let harness_setup =
            generate_harness_setup(variant, has_client, has_mcp_client, has_fixtures, attrs);
        let client_setup = if has_client {
            generate_client_setup()
        } else {
            quote!()
        };
        let start_and_seed = generate_start_and_seed(attrs, fixture_seed.clone());
        let mcp_client_setup = generate_mcp_client_setup(has_mcp_client);

        test_fns.push(quote! {
            #[::tokio::test]
            #[::serial_test::parallel]
            async fn #test_name() {
                #fixture_setup
                #fixture_envs
                #harness_setup
                #start_and_seed
                #client_setup
                #mcp_client_setup
                #param_bindings
                #fn_body
                __harness.stop().await.ok();
            }
        });
    }

    Ok(quote! {
        #fn_vis mod #fn_name {
            use super::*;
            #(#test_fns)*
        }
    })
}

#[allow(clippy::too_many_arguments)]
fn generate_impl_functions_for_test_matrix(
    fn_name: &Ident,
    fn_vis: &syn::Visibility,
    fn_body: &syn::Block,
    input: &ItemFn,
    params: &[DetectedParam],
    variants: &[TestVariant],
    has_client: bool,
    has_harness: bool,
    has_harness_mut: bool,
    has_mcp_client: bool,
    attrs: &IggyTestAttrs,
) -> syn::Result<TokenStream> {
    let matrix_params_list: Vec<_> = params
        .iter()
        .filter_map(|p| {
            if let DetectedParam::MatrixParam { name, ty } = p {
                Some((name, ty))
            } else {
                None
            }
        })
        .collect();

    let param_names: Vec<_> = matrix_params_list.iter().map(|(name, _)| *name).collect();
    let param_types: Vec<_> = matrix_params_list.iter().map(|(_, ty)| *ty).collect();

    let other_attrs: Vec<_> = input
        .attrs
        .iter()
        .filter(|attr| {
            let path = attr.path();
            !path.is_ident("iggy_harness")
        })
        .collect();

    let has_fixtures = needs_fixtures(params);
    let fixture_setup = generate_fixture_setup(params);
    let fixture_envs = generate_fixture_envs_collection(params);
    let fixture_seed = generate_fixture_seed(params);
    let param_bindings = generate_param_bindings(params, has_harness, has_harness_mut);
    let start_and_seed = generate_start_and_seed(attrs, fixture_seed.clone());
    let mcp_client_setup = generate_mcp_client_setup(has_mcp_client);

    if variants.len() == 1 {
        let variant = &variants[0];
        let harness_setup =
            generate_harness_setup(variant, has_client, has_mcp_client, has_fixtures, attrs);
        let client_setup = if has_client {
            generate_client_setup()
        } else {
            quote!()
        };

        return Ok(quote! {
            #(#other_attrs)*
            #[::tokio::test]
            #[::serial_test::parallel]
            #fn_vis async fn #fn_name(#(#param_names: #param_types),*) {
                #fixture_setup
                #fixture_envs
                #harness_setup
                #start_and_seed
                #client_setup
                #mcp_client_setup
                #param_bindings
                #fn_body
                __harness.stop().await.ok();
            }
        });
    }

    let mut impl_fns = Vec::new();
    let mut test_fn_calls = Vec::new();

    for variant in variants {
        let impl_name = format_ident!("__impl_{}", variant.suffix());
        let harness_setup =
            generate_harness_setup(variant, has_client, has_mcp_client, has_fixtures, attrs);
        let client_setup = if has_client {
            generate_client_setup()
        } else {
            quote!()
        };

        impl_fns.push(quote! {
            async fn #impl_name(#(#param_names: #param_types),*) {
                #fixture_setup
                #fixture_envs
                #harness_setup
                #start_and_seed
                #client_setup
                #mcp_client_setup
                #param_bindings
                #fn_body
                __harness.stop().await.ok();
            }
        });

        let test_name = format_ident!("{}", variant.suffix());
        test_fn_calls.push(quote! {
            #(#other_attrs)*
            #[::tokio::test]
            #[::serial_test::parallel]
            async fn #test_name(#(#param_names: #param_types),*) {
                #impl_name(#(#param_names),*).await;
            }
        });
    }

    Ok(quote! {
        #fn_vis mod #fn_name {
            use super::*;
            #(#impl_fns)*
            #(#test_fn_calls)*
        }
    })
}

fn generate_tls_config_token(mode: TlsMode) -> TokenStream {
    match mode {
        TlsMode::SelfSigned => quote!(::integration::harness::TlsConfig::self_signed()),
        TlsMode::Generated => quote!(::integration::harness::TlsConfig::generated()),
    }
}

fn generate_harness_setup(
    variant: &TestVariant,
    has_client: bool,
    has_mcp_client: bool,
    has_fixtures: bool,
    attrs: &IggyTestAttrs,
) -> TokenStream {
    let transport = variant.transport.variant_ident();

    // Build config entries for runtime validation
    let config_entries: Vec<_> = variant
        .config_values
        .iter()
        .map(|(path, value)| quote!((#path.to_string(), #value.to_string())))
        .collect();

    let has_config_overrides = !config_entries.is_empty();

    // Generate the config override resolution
    let config_resolution = if has_config_overrides {
        quote! {
            let __config_overrides: ::std::collections::HashMap<String, String> =
                [#(#config_entries),*].into_iter().collect();
            let __extra_envs = ::integration::harness::resolve_config_paths(&__config_overrides)
                .unwrap_or_else(|e| panic!("invalid config path in #[iggy_harness]:\n{}", e));
        }
    } else {
        quote! {
            let __extra_envs = ::std::collections::HashMap::<String, String>::new();
        }
    };

    // Build server config with TLS from transport and explicit attrs
    let mut server_builder_calls = Vec::new();

    // Add TLS from transport
    if let Some(mode) = variant.tls {
        let tls_config = generate_tls_config_token(mode);
        server_builder_calls.push(quote!(.tls(#tls_config)));
    }
    if let Some(mode) = variant.websocket_tls {
        let tls_config = generate_tls_config_token(mode);
        server_builder_calls.push(quote!(.websocket_tls(#tls_config)));
    }

    // Add TLS from explicit server attrs (overrides transport)
    if let Some(ref tls) = attrs.server.tls {
        let tls_config = generate_tls_config_token(tls.mode);
        server_builder_calls.push(quote!(.tls(#tls_config)));
    }
    if let Some(ref tls) = attrs.server.websocket_tls {
        let tls_config = generate_tls_config_token(tls.mode);
        server_builder_calls.push(quote!(.websocket_tls(#tls_config)));
    }

    // Always add extra_envs (may be empty)
    server_builder_calls.push(quote!(.extra_envs(__extra_envs)));

    let server_config = quote! {
        ::integration::harness::TestServerConfig::builder()
            #(#server_builder_calls)*
            .build()
    };

    // Configure client with TLS based on transport
    let tls_mode = variant.tls.or(variant.websocket_tls);
    let client_config_method =
        Ident::new(variant.transport.client_config_method(), Span::call_site());
    let client_config = match tls_mode {
        Some(TlsMode::Generated) => {
            quote! {
                ::integration::harness::ClientConfig::#client_config_method()
                    .with_tls("localhost".to_string(), None, true)
            }
        }
        Some(TlsMode::SelfSigned) => {
            quote! {
                ::integration::harness::ClientConfig::#client_config_method()
                    .with_tls("localhost".to_string(), None, false)
            }
        }
        None => {
            quote!(::integration::harness::ClientConfig::#client_config_method())
        }
    };

    let mcp_builder_call = if has_mcp_client || attrs.server.mcp.is_some() {
        if let Some(ref mcp_attrs) = attrs.server.mcp {
            if let Some(ref consumer) = mcp_attrs.consumer_name {
                quote!(.mcp(::integration::harness::McpConfig::builder()
                    .consumer_name(#consumer)
                    .build()))
            } else {
                quote!(.default_mcp())
            }
        } else {
            quote!(.default_mcp())
        }
    } else {
        quote!()
    };

    let connector_builder_call = if let Some(ref connector_attrs) = attrs.server.connector {
        let config_path = connector_attrs
            .config_path
            .as_deref()
            .unwrap_or("connectors/config.toml");
        if has_fixtures {
            quote!(.connector(::integration::harness::ConnectorConfig::builder()
                .config_path(::std::path::PathBuf::from(#config_path))
                .extra_envs(__fixture_envs.clone())
                .build()))
        } else {
            quote!(.connector(::integration::harness::ConnectorConfig::builder()
                .config_path(::std::path::PathBuf::from(#config_path))
                .build()))
        }
    } else {
        quote!()
    };

    let client_builder_call = if has_client {
        quote!(.client(#client_config))
    } else {
        quote!(.primary_client(#client_config))
    };

    quote! {
        #config_resolution
        let mut __harness = ::integration::harness::TestHarness::builder()
            .server(#server_config)
            #client_builder_call
            #mcp_builder_call
            #connector_builder_call
            .build()
            .expect("failed to build test harness");
        let _ = ::integration::__macro_support::TransportProtocol::#transport;
    }
}

fn generate_client_setup() -> TokenStream {
    quote! {
        let __client = __harness.client();
    }
}

/// Generate the harness start call and seed handling.
///
/// When a seed function is present, uses `start_with_seed` to run seed
/// after server but before MCP and connector (which may depend on seed data).
/// Fixture seeds are combined with the global seed.
fn generate_start_and_seed(attrs: &IggyTestAttrs, fixture_seed: TokenStream) -> TokenStream {
    let has_fixture_seed = !fixture_seed.is_empty();
    match (&attrs.seed_fn, has_fixture_seed) {
        (Some(seed_fn), true) => {
            quote! {
                __harness.start_with_seed(|__seed_client| async move {
                    #seed_fn(&__seed_client).await?;
                    #fixture_seed
                    Ok(())
                }).await.expect("failed to start test harness");
            }
        }
        (Some(seed_fn), false) => {
            quote! {
                __harness.start_with_seed(|__seed_client| async move {
                    #seed_fn(&__seed_client).await
                }).await.expect("failed to start test harness");
            }
        }
        (None, true) => {
            quote! {
                __harness.start_with_seed(|__seed_client| async move {
                    #fixture_seed
                    Ok(())
                }).await.expect("failed to start test harness");
            }
        }
        (None, false) => {
            quote! {
                __harness.start().await.expect("failed to start test harness");
            }
        }
    }
}

fn generate_mcp_client_setup(has_mcp_client: bool) -> TokenStream {
    if has_mcp_client {
        quote! {
            let __mcp_client = __harness.mcp_client().await
                .expect("failed to create MCP client");
        }
    } else {
        quote!()
    }
}

/// Generate fixture setup calls (before harness setup).
fn generate_fixture_setup(params: &[DetectedParam]) -> TokenStream {
    let fixtures = fixture_params(params);
    if fixtures.is_empty() {
        return quote!();
    }

    let setup_calls: Vec<_> = fixtures
        .iter()
        .filter_map(|p| {
            if let DetectedParam::Fixture { name, ty } = p {
                let var_name = format_ident!("__fixture_{}", name);
                Some(quote! {
                    let #var_name = <#ty as ::integration::harness::TestFixture>::setup()
                        .await
                        .expect("failed to setup fixture");
                })
            } else {
                None
            }
        })
        .collect();

    quote!(#(#setup_calls)*)
}

/// Generate fixture envs collection (after fixture setup, before harness).
fn generate_fixture_envs_collection(params: &[DetectedParam]) -> TokenStream {
    let fixtures = fixture_params(params);
    if fixtures.is_empty() {
        return quote!();
    }

    let env_calls: Vec<_> = fixtures
        .iter()
        .filter_map(|p| {
            if let DetectedParam::Fixture { name, .. } = p {
                let var_name = format_ident!("__fixture_{}", name);
                Some(quote! {
                    __fixture_envs.extend(
                        ::integration::harness::TestFixture::connector_envs(&#var_name)
                    );
                })
            } else {
                None
            }
        })
        .collect();

    quote! {
        let mut __fixture_envs = ::std::collections::HashMap::<String, String>::new();
        #(#env_calls)*
    }
}

/// Generate fixture seed calls (inside start_with_seed closure).
///
/// Note: Currently disabled to avoid move semantics issues with async closures.
/// Fixtures that need to seed data should do so in the test body after harness start.
fn generate_fixture_seed(_params: &[DetectedParam]) -> TokenStream {
    // Fixture seeding is disabled for now because:
    // 1. The async move closure in start_with_seed captures the fixture by value
    // 2. This prevents using the fixture in the test body after seeding
    // 3. Most fixtures don't need to seed data - they just provide env vars
    //
    // If a fixture needs to seed data, it can be done manually in the test body:
    //   fixture.seed(&client).await.unwrap();
    quote!()
}

fn generate_param_bindings(
    params: &[DetectedParam],
    _has_harness: bool,
    _has_harness_mut: bool,
) -> TokenStream {
    let mut bindings = Vec::new();

    for param in params {
        match param {
            DetectedParam::Client { name } => {
                bindings.push(quote! {
                    let #name = &__client;
                });
            }
            DetectedParam::HarnessRef { name } => {
                bindings.push(quote! {
                    let #name = &__harness;
                });
            }
            DetectedParam::HarnessMut { name } => {
                bindings.push(quote! {
                    let #name = &mut __harness;
                });
            }
            DetectedParam::McpClient { name } => {
                bindings.push(quote! {
                    let #name = __mcp_client;
                });
            }
            DetectedParam::Fixture { name, .. } => {
                let fixture_var = format_ident!("__fixture_{}", name);
                bindings.push(quote! {
                    let #name = #fixture_var;
                });
            }
            DetectedParam::MatrixParam { .. } => {}
        }
    }

    quote!(#(#bindings)*)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::attrs::{ConfigOverride, ConfigValue};
    use proc_macro2::Span;

    #[test]
    fn variant_suffix_basic() {
        let v = TestVariant {
            transport: Transport::Tcp,
            config_values: vec![],
            tls: None,
            websocket_tls: None,
        };
        assert_eq!(v.suffix(), "tcp");
    }

    #[test]
    fn variant_suffix_full() {
        let v = TestVariant {
            transport: Transport::Http,
            config_values: vec![
                ("segment.size".to_string(), "1MiB".to_string()),
                ("segment.cache_indexes".to_string(), "all".to_string()),
                (
                    "partition.messages_required_to_save".to_string(),
                    "64".to_string(),
                ),
            ],
            tls: None,
            websocket_tls: None,
        };
        assert_eq!(
            v.suffix(),
            "http_size_1mb_cache_indexes_all_messages_required_to_save_64"
        );
    }

    #[test]
    fn variant_suffix_with_tls() {
        let v = TestVariant {
            transport: Transport::TcpTlsSelfSigned,
            config_values: vec![],
            tls: Some(TlsMode::SelfSigned),
            websocket_tls: None,
        };
        assert_eq!(v.suffix(), "tcp_tls_self_signed");

        let v = TestVariant {
            transport: Transport::TcpTlsGenerated,
            config_values: vec![],
            tls: Some(TlsMode::Generated),
            websocket_tls: None,
        };
        assert_eq!(v.suffix(), "tcp_tls_generated");
    }

    #[test]
    fn generate_variants_simple() {
        let attrs = IggyTestAttrs::with_transports(vec![Transport::Tcp]);
        let variants = generate_variants(&attrs);
        assert_eq!(variants.len(), 1);
        assert_eq!(variants[0].transport, Transport::Tcp);
    }

    #[test]
    fn generate_variants_transport_matrix() {
        let attrs = IggyTestAttrs::with_transports(vec![Transport::Tcp, Transport::Http]);
        let variants = generate_variants(&attrs);
        assert_eq!(variants.len(), 2);
    }

    #[test]
    fn generate_variants_full_matrix() {
        let attrs = IggyTestAttrs {
            transports: vec![Transport::Tcp, Transport::Http],
            server: crate::attrs::ServerAttrs {
                config_overrides: vec![
                    ConfigOverride {
                        path: "segment.size".to_string(),
                        value: ConfigValue::Matrix(vec!["512B".to_string(), "1MiB".to_string()]),
                        span: Span::call_site(),
                    },
                    ConfigOverride {
                        path: "segment.cache_indexes".to_string(),
                        value: ConfigValue::Matrix(vec!["none".to_string(), "all".to_string()]),
                        span: Span::call_site(),
                    },
                ],
                ..Default::default()
            },
            seed_fn: None,
        };
        let variants = generate_variants(&attrs);
        // 2 transports * 2 segment sizes * 2 cache modes = 8 variants
        assert_eq!(variants.len(), 8);
    }

    #[test]
    fn cartesian_product_empty() {
        let result = cartesian_product(&[]);
        assert_eq!(result, vec![vec![]]);
    }

    #[test]
    fn cartesian_product_single() {
        let overrides = vec![ConfigOverride {
            path: "segment.size".to_string(),
            value: ConfigValue::Matrix(vec!["512B".to_string(), "1MiB".to_string()]),
            span: Span::call_site(),
        }];
        let result = cartesian_product(&overrides);
        assert_eq!(result.len(), 2);
        assert_eq!(
            result[0],
            vec![("segment.size".to_string(), "512B".to_string())]
        );
        assert_eq!(
            result[1],
            vec![("segment.size".to_string(), "1MiB".to_string())]
        );
    }

    #[test]
    fn cartesian_product_multiple() {
        let overrides = vec![
            ConfigOverride {
                path: "a".to_string(),
                value: ConfigValue::Matrix(vec!["1".to_string(), "2".to_string()]),
                span: Span::call_site(),
            },
            ConfigOverride {
                path: "b".to_string(),
                value: ConfigValue::Matrix(vec!["x".to_string(), "y".to_string()]),
                span: Span::call_site(),
            },
        ];
        let result = cartesian_product(&overrides);
        assert_eq!(result.len(), 4);
    }
}
