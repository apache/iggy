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

//! Ergonomic test infrastructure for Iggy integration tests.
//!
//! # Example
//!
//! ```ignore
//! use integration::harness::TestHarness;
//!
//! #[tokio::test]
//! async fn test_basic_operations() {
//!     let mut harness = TestHarness::builder()
//!         .default_server()
//!         .root_tcp_client()  // Auto-logins as root
//!         .build()
//!         .unwrap();
//!
//!     harness.start().await.unwrap();
//!
//!     // Client is already logged in as root
//!     let client = harness.client();
//!     // client.create_stream(...).await.unwrap();
//!
//!     harness.stop().await.unwrap();
//! }
//! ```

pub mod config;
mod context;
mod error;
pub mod fixtures;
pub mod handle;
mod helpers;
mod orchestrator;
mod port_reserver;
pub mod seeds;
mod traits;

pub use config::{
    AutoLoginConfig, ClientConfig, ConnectorsRuntimeConfig, EncryptionConfig, IpAddrKind,
    McpConfig, TestServerConfig, TlsConfig, resolve_config_paths,
};

pub use context::{TestContext, get_test_directory};
pub use error::TestBinaryError;
pub use handle::{
    ClientBuilder, ClientHandle, ConnectorsRuntimeHandle, McpClient, McpHandle, ServerHandle,
};
pub use orchestrator::{TestHarness, TestHarnessBuilder, TestLogs};
pub use traits::{IggyServerDependent, Restartable, TestBinary};

pub use helpers::{
    USER_PASSWORD, assert_clean_system, create_user, delete_user, login_root, login_user,
};

pub use fixtures::TestFixture;
