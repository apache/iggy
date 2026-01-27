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

//! Procedural macro for ergonomic integration test definition.
//!
//! The `#[iggy_harness]` attribute replaces boilerplate harness setup with a declarative DSL.
//!
//! # Examples
//!
//! Simple test with default TCP transport:
//! ```ignore
//! #[iggy_harness]
//! async fn test_ping(client: &IggyClient) {
//!     client.ping().await.unwrap();
//! }
//! ```
//!
//! Test with transport matrix:
//! ```ignore
//! #[iggy_harness(transport = [Tcp, Http, Quic, WebSocket])]
//! async fn test_all_transports(client: &IggyClient) {
//!     client.ping().await.unwrap();
//! }
//! ```
//!
//! Test with server config matrix:
//! ```ignore
//! #[iggy_harness(server(
//!     segment_size = ["512B", "1MiB"],
//!     cache_indexes = ["none", "all"],
//! ))]
//! async fn test_caching(client: &IggyClient) {
//!     // 2 segment sizes × 2 cache modes = 4 tests
//! }
//! ```

mod attrs;
mod codegen;
mod params;

use proc_macro::TokenStream;
use syn::{ItemFn, parse_macro_input};

use attrs::IggyTestAttrs;
use codegen::generate_tests;

/// Attribute macro for declaring Iggy integration tests.
///
/// This macro handles test harness setup, client creation, and generates test variants
/// for infrastructure dimensions (transport, server config).
///
/// # Attributes
///
/// - `transport = [Tcp, Http, Quic, WebSocket]` - Generate variants for each transport
/// - `server(key = value)` - Static server config
/// - `server(key = [v1, v2])` - Generate variants for each config value
///
/// # Special Parameters
///
/// The test function can request special parameters:
/// - `client: &IggyClient` - Injected client connected to the test server
/// - `harness: &TestHarness` - Reference to the test harness (for data_path, etc.)
/// - `harness: &mut TestHarness` - Mutable reference (for restart_server, etc.)
///
/// Any other parameters are passed through from `#[test_matrix]`.
#[proc_macro_attribute]
pub fn iggy_harness(attr: TokenStream, item: TokenStream) -> TokenStream {
    let attrs = match syn::parse::<IggyTestAttrs>(attr) {
        Ok(a) => a,
        Err(e) => return e.to_compile_error().into(),
    };

    let input = parse_macro_input!(item as ItemFn);

    match generate_tests(&attrs, &input) {
        Ok(tokens) => tokens.into(),
        Err(e) => e.to_compile_error().into(),
    }
}
