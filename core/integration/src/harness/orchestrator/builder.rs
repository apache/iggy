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

use super::harness::TestHarness;
use crate::harness::config::{ClientConfig, ConnectorsRuntimeConfig, McpConfig, TestServerConfig};
use crate::harness::context::TestContext;
use crate::harness::error::TestBinaryError;
use crate::harness::handle::{ConnectorsRuntimeHandle, McpHandle, ServerHandle};
use crate::harness::traits::TestBinary;
use std::sync::Arc;

/// Builder for TestHarness with fluent configuration API.
pub struct TestHarnessBuilder {
    test_name: Option<String>,
    server_config: Option<TestServerConfig>,
    mcp_config: Option<McpConfig>,
    connectors_runtime_config: Option<ConnectorsRuntimeConfig>,
    primary_transport: Option<iggy_common::TransportProtocol>,
    primary_client_config: Option<ClientConfig>,
    clients: Vec<ClientConfig>,
    cleanup: bool,
}

impl Default for TestHarnessBuilder {
    fn default() -> Self {
        Self {
            test_name: None,
            server_config: None,
            mcp_config: None,
            connectors_runtime_config: None,
            primary_transport: None,
            primary_client_config: None,
            clients: Vec::new(),
            cleanup: true,
        }
    }
}

impl TestHarnessBuilder {
    /// Override the test name (defaults to thread name or UUID).
    pub fn test_name(mut self, name: impl Into<String>) -> Self {
        self.test_name = Some(name.into());
        self
    }

    /// Configure the iggy-server.
    pub fn server(mut self, config: TestServerConfig) -> Self {
        self.server_config = Some(config);
        self
    }

    /// Configure the iggy-server with default settings.
    pub fn default_server(mut self) -> Self {
        self.server_config = Some(TestServerConfig::default());
        self
    }

    /// Configure the MCP server.
    pub fn mcp(mut self, config: McpConfig) -> Self {
        self.mcp_config = Some(config);
        self
    }

    /// Configure the MCP server with default settings.
    pub fn default_mcp(mut self) -> Self {
        self.mcp_config = Some(McpConfig::default());
        self
    }

    /// Configure the connectors runtime.
    pub fn connectors_runtime(mut self, config: ConnectorsRuntimeConfig) -> Self {
        self.connectors_runtime_config = Some(config);
        self
    }

    /// Configure the connectors runtime with default settings.
    pub fn default_connectors_runtime(mut self) -> Self {
        self.connectors_runtime_config = Some(ConnectorsRuntimeConfig::default());
        self
    }

    /// Add a TCP client.
    pub fn tcp_client(mut self) -> Self {
        self.clients.push(ClientConfig::tcp());
        self
    }

    /// Add a TCP client with TCP_NODELAY.
    pub fn tcp_client_nodelay(mut self) -> Self {
        self.clients.push(ClientConfig::tcp().with_nodelay());
        self
    }

    /// Add a TCP client that auto-logins as root.
    pub fn root_tcp_client(mut self) -> Self {
        self.clients.push(ClientConfig::root_tcp());
        self
    }

    /// Add a TCP client with TCP_NODELAY that auto-logins as root.
    pub fn root_tcp_client_nodelay(mut self) -> Self {
        self.clients.push(ClientConfig::root_tcp().with_nodelay());
        self
    }

    /// Add a QUIC client.
    pub fn quic_client(mut self) -> Self {
        self.clients.push(ClientConfig::quic());
        self
    }

    /// Add a QUIC client that auto-logins as root.
    pub fn root_quic_client(mut self) -> Self {
        self.clients.push(ClientConfig::root_quic());
        self
    }

    /// Add an HTTP client.
    pub fn http_client(mut self) -> Self {
        self.clients.push(ClientConfig::http());
        self
    }

    /// Add an HTTP client that auto-logins as root.
    pub fn root_http_client(mut self) -> Self {
        self.clients.push(ClientConfig::root_http());
        self
    }

    /// Add a WebSocket client.
    pub fn websocket_client(mut self) -> Self {
        self.clients.push(ClientConfig::websocket());
        self
    }

    /// Add a WebSocket client that auto-logins as root.
    pub fn root_websocket_client(mut self) -> Self {
        self.clients.push(ClientConfig::root_websocket());
        self
    }

    /// Add a custom client configuration.
    pub fn client(mut self, config: ClientConfig) -> Self {
        self.clients.push(config);
        self
    }

    /// Set the primary transport and client config without auto-connecting.
    /// Used by the macro when the test doesn't use `client: &IggyClient` parameter
    /// but still needs transport info for `new_client()` etc.
    pub fn primary_client(mut self, config: ClientConfig) -> Self {
        self.primary_transport = Some(config.transport);
        self.primary_client_config = Some(config);
        self
    }

    /// Set whether to cleanup test data on successful completion.
    pub fn cleanup(mut self, cleanup: bool) -> Self {
        self.cleanup = cleanup;
        self
    }

    /// Build the TestHarness. Does NOT start any binaries.
    pub fn build(self) -> Result<TestHarness, TestBinaryError> {
        let mut context = TestContext::new(self.test_name, self.cleanup)?;
        context.ensure_created()?;
        let context = Arc::new(context);

        let server = self
            .server_config
            .map(|config| ServerHandle::with_config(config, context.clone()));

        let mcp = self
            .mcp_config
            .map(|config| McpHandle::with_config(config, context.clone()));

        let connectors_runtime = self
            .connectors_runtime_config
            .map(|config| ConnectorsRuntimeHandle::with_config(config, context.clone()));

        Ok(TestHarness {
            context,
            server,
            mcp,
            connectors_runtime,
            clients: Vec::new(),
            client_configs: self.clients,
            primary_transport: self.primary_transport,
            primary_client_config: self.primary_client_config,
            started: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_builder_with_server() {
        let harness = TestHarness::builder()
            .server(
                TestServerConfig::builder()
                    .extra_envs(HashMap::from([(
                        "IGGY_SYSTEM_SEGMENT_SIZE".to_string(),
                        "1MiB".to_string(),
                    )]))
                    .build(),
            )
            .tcp_client()
            .build()
            .unwrap();

        assert!(harness.server.is_some());
        assert!(!harness.started);
        assert_eq!(harness.client_configs.len(), 1);
    }

    #[test]
    fn test_builder_with_mcp() {
        let harness = TestHarness::builder()
            .default_server()
            .default_mcp()
            .tcp_client()
            .build()
            .unwrap();

        assert!(harness.server.is_some());
        assert!(harness.mcp.is_some());
    }

    #[test]
    fn test_builder_multiple_clients() {
        let harness = TestHarness::builder()
            .default_server()
            .tcp_client()
            .http_client()
            .quic_client()
            .build()
            .unwrap();

        assert_eq!(harness.client_configs.len(), 3);
    }

    #[test]
    fn test_builder_root_tcp_client() {
        let harness = TestHarness::builder()
            .default_server()
            .root_tcp_client()
            .build()
            .unwrap();

        assert_eq!(harness.client_configs.len(), 1);
        assert!(harness.client_configs[0].auto_login.is_some());
    }

    #[test]
    fn test_builder_with_custom_config() {
        let harness = TestHarness::builder()
            .server(
                TestServerConfig::builder()
                    .quic_enabled(false)
                    .websocket_enabled(false)
                    .extra_envs(HashMap::from([
                        ("IGGY_SYSTEM_SEGMENT_SIZE".to_string(), "2MiB".to_string()),
                        ("TEST".to_string(), "value".to_string()),
                    ]))
                    .build(),
            )
            .build()
            .unwrap();

        assert!(harness.server.is_some());
    }
}
