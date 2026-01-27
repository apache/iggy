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

//! Proc macro for generating type-safe environment variable mappings for configuration structs.
//!
//! This crate provides `#[derive(ConfigEnv)]` which generates:
//! - Compile-time constants for all valid env var names
//! - Type-safe builder API for tests
//! - `ConfigEnvMappings` trait implementation for runtime discovery
//!
//! # Path Deduction
//!
//! The macro automatically deduces config paths from field names:
//! - Each struct generates **relative** mappings (just its own field names)
//! - Parents transform nested mappings by prepending the field name
//! - Only the root struct needs `prefix`
//!
//! # Type Inference
//!
//! - **Leaf types** (primitives, String, known value types): generate direct mappings
//! - **Nested types** (structs with `ConfigEnv` derive): include their mappings recursively
//! - **Vec<T>**: expands to indexed mappings (e.g., `FIELD_0_NAME`, `FIELD_1_NAME`, ...)
//! - **Arc<T>**, **Box<T>**, **Option<T>**: transparently unwrapped

mod config_env;

use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

/// Derive macro for generating environment variable mappings.
///
/// # Container Attributes
/// - `#[config_env(prefix = "IGGY_")]` - Sets the env var prefix (only on root config)
/// - `#[config_env(name = "iggy-server-config")]` - Sets the figment Provider metadata name
///
/// Root configs should specify both `prefix` and `name`:
/// ```ignore
/// #[derive(ConfigEnv)]
/// #[config_env(prefix = "IGGY_", name = "iggy-server-config")]
/// pub struct ServerConfig { ... }
/// ```
///
/// This generates constants `ServerConfig::ENV_PREFIX` and `ServerConfig::ENV_PROVIDER_NAME`.
///
/// # Field Attributes
/// - `#[config_env(name = "CUSTOM")]` - Override the env var segment name
/// - `#[config_env(skip)]` - Exclude this field from env var mapping
/// - `#[config_env(secret)]` - Mark field as secret (masked in logs)
/// - `#[config_env(leaf)]` - Treat as value type, not nested config (for custom types like enums)
///
/// # Type Handling
/// - **Primitives** (bool, u8-u128, i8-i128, f32, f64, String): automatically treated as leaves
/// - **Non-primitives**: assumed to be nested configs that implement `ConfigEnvMappings`
/// - Use `#[config_env(leaf)]` for value types that shouldn't be recursively expanded
///   (e.g., `IggyDuration`, `CompressionAlgorithm`, custom enums)
///
/// # Examples
///
/// ## Struct Configuration
/// ```ignore
/// // Root config with prefix and name
/// #[derive(ConfigEnv)]
/// #[config_env(prefix = "IGGY_", name = "iggy-server-config")]
/// pub struct ServerConfig {
///     pub http: HttpConfig,              // Nested: IGGY_HTTP_*
///     #[config_env(leaf)]
///     pub compression: CompressionAlgorithm,  // Leaf: IGGY_COMPRESSION
/// }
///
/// // Nested config - no prefix needed, paths deduced from field names
/// #[derive(ConfigEnv)]
/// pub struct HttpConfig {
///     pub enabled: bool,       // ENABLED -> parent adds HTTP_ -> IGGY_HTTP_ENABLED
///     pub port: u16,           // PORT -> IGGY_HTTP_PORT
///     #[config_env(leaf)]
///     pub timeout: IggyDuration,  // Leaf: TIMEOUT -> IGGY_HTTP_TIMEOUT
/// }
/// ```
///
/// ## Enum Configuration
/// ```ignore
/// // Enums with newtype variants collect mappings from inner types
/// #[derive(ConfigEnv)]
/// #[serde(tag = "type", rename_all = "lowercase")]
/// pub enum ConnectorConfig {
///     Local(LocalConfig),   // Includes LocalConfig's mappings
///     Http(HttpConfig),     // Includes HttpConfig's mappings
/// }
///
/// // Simple value enums should NOT derive ConfigEnv - mark the field as leaf instead
/// #[derive(Serialize, Deserialize)]
/// pub enum Transport { Tcp, Quic, Http }
///
/// #[derive(ConfigEnv)]
/// pub struct ServerConfig {
///     #[config_env(leaf)]  // Transport is a simple enum, not a nested config
///     pub transport: Transport,
/// }
/// ```
#[proc_macro_derive(ConfigEnv, attributes(config_env))]
pub fn derive_config_env(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    config_env::generate_impl(&input).into()
}
