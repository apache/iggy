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

//! Legacy test server utilities.
//!
//! This module provides the `ClientFactory` trait used by transport client implementations.
//!
//! For new tests, prefer using the [`crate::harness::TestHarness`] which provides
//! a more ergonomic API with automatic cleanup.

use async_trait::async_trait;
use iggy::prelude::ClientWrapper;
use iggy_common::TransportProtocol;

/// Factory trait for creating transport clients.
///
/// Implemented by each transport type (TCP, QUIC, HTTP, WebSocket) to provide
/// uniform client creation for tests.
#[async_trait]
pub trait ClientFactory: Sync + Send {
    /// Creates a new client connection.
    async fn create_client(&self) -> ClientWrapper;

    /// Returns the transport protocol this factory creates.
    fn transport(&self) -> TransportProtocol;

    /// Returns the server address this factory connects to.
    fn server_addr(&self) -> String;
}
