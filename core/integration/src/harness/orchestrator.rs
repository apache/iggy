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

use crate::harness::config::{ClientConfig, ConnectorConfig, McpConfig, TestServerConfig};
use crate::harness::context::TestContext;
use crate::harness::error::TestBinaryError;
use crate::harness::handle::{ClientHandle, ConnectorHandle, McpClient, McpHandle, ServerHandle};
use crate::harness::traits::{IggyDependent, Restartable, TestBinary};
use crate::http_client::HttpClientFactory;
use crate::quic_client::QuicClientFactory;
use crate::tcp_client::TcpClientFactory;
use crate::test_server::ClientFactory;
use crate::websocket_client::WebSocketClientFactory;
use iggy::prelude::{
    ClientWrapper, DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME, IggyClient, UserClient,
};
use iggy_common::TransportProtocol;
use std::path::Path;
use std::sync::Arc;

/// Collected logs from all binaries in the harness.
#[derive(Debug)]
pub struct TestLogs {
    pub server: Option<(String, String)>,
    pub mcp: Option<(String, String)>,
    pub connector: Option<(String, String)>,
}

/// Orchestrates test binaries and clients for integration tests.
pub struct TestHarness {
    context: Arc<TestContext>,
    server: Option<ServerHandle>,
    mcp: Option<McpHandle>,
    connector: Option<ConnectorHandle>,
    clients: Vec<ClientHandle>,
    client_configs: Vec<ClientConfig>,
    primary_transport: Option<TransportProtocol>,
    primary_client_config: Option<ClientConfig>,
    started: bool,
}

impl std::fmt::Debug for TestHarness {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TestHarness")
            .field("test_name", &self.context.test_name())
            .field("started", &self.started)
            .field("has_server", &self.server.is_some())
            .field("has_mcp", &self.mcp.is_some())
            .field("has_connector", &self.connector.is_some())
            .field("client_count", &self.clients.len())
            .finish()
    }
}

impl TestHarness {
    pub fn builder() -> TestHarnessBuilder {
        TestHarnessBuilder::default()
    }

    /// Start all configured binaries and create clients.
    pub async fn start(&mut self) -> Result<(), TestBinaryError> {
        self.start_without_seed().await
    }

    async fn start_without_seed(&mut self) -> Result<(), TestBinaryError> {
        if self.started {
            return Err(TestBinaryError::AlreadyStarted);
        }

        // Start server first (required for other binaries)
        if let Some(ref mut server) = self.server {
            server.start()?;
        }

        // Start MCP if configured
        if let Some(ref mut mcp) = self.mcp {
            if let Some(tcp_addr) = self.server.as_ref().and_then(|s| s.tcp_addr()) {
                mcp.set_iggy_address(tcp_addr);
            }
            mcp.start()?;
            mcp.wait_ready().await?;
        }

        // Start connectors if configured
        if let Some(ref mut connector) = self.connector {
            if let Some(tcp_addr) = self.server.as_ref().and_then(|s| s.tcp_addr()) {
                connector.set_iggy_address(tcp_addr);
            }
            connector.start()?;
            connector.wait_ready().await?;
        }

        // Create and connect clients
        self.create_clients().await?;

        self.started = true;
        Ok(())
    }

    /// Start all configured binaries with a seed function that runs before dependent binaries.
    ///
    /// The seed function is called after the server starts but before MCP and connector,
    /// allowing streams/topics to be created that dependent binaries may need.
    ///
    /// # Example
    /// ```ignore
    /// harness.start_with_seed(|client| async move {
    ///     client.create_stream("test").await?;
    ///     Ok(())
    /// }).await?;
    /// ```
    pub async fn start_with_seed<F, Fut>(&mut self, seed: F) -> Result<(), TestBinaryError>
    where
        F: FnOnce(IggyClient) -> Fut,
        Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>>,
    {
        if self.started {
            return Err(TestBinaryError::AlreadyStarted);
        }

        // Start server first (required for other binaries)
        if let Some(ref mut server) = self.server {
            server.start()?;
        }

        // Run seed function before dependent binaries (MCP, connector may need seed data)
        let client = self.tcp_root_client().await?;
        seed(client)
            .await
            .map_err(|e| TestBinaryError::SeedFailed(e.to_string()))?;

        // Start MCP if configured
        if let Some(ref mut mcp) = self.mcp {
            if let Some(tcp_addr) = self.server.as_ref().and_then(|s| s.tcp_addr()) {
                mcp.set_iggy_address(tcp_addr);
            }
            mcp.start()?;
            mcp.wait_ready().await?;
        }

        // Start connectors if configured
        if let Some(ref mut connector) = self.connector {
            if let Some(tcp_addr) = self.server.as_ref().and_then(|s| s.tcp_addr()) {
                connector.set_iggy_address(tcp_addr);
            }
            connector.start()?;
            connector.wait_ready().await?;
        }

        // Create and connect clients
        self.create_clients().await?;

        self.started = true;
        Ok(())
    }

    /// Stop all binaries and disconnect clients.
    pub async fn stop(&mut self) -> Result<(), TestBinaryError> {
        // Disconnect clients first
        for client in &mut self.clients {
            client.disconnect().await;
        }
        self.clients.clear();

        // Stop binaries in reverse order
        if let Some(ref mut connector) = self.connector {
            connector.stop()?;
        }

        if let Some(ref mut mcp) = self.mcp {
            mcp.stop()?;
        }

        if let Some(ref mut server) = self.server {
            server.stop()?;
        }

        self.started = false;
        Ok(())
    }

    /// Restart the server and reconnect all clients.
    pub async fn restart_server(&mut self) -> Result<(), TestBinaryError> {
        let Some(ref mut server) = self.server else {
            return Err(TestBinaryError::MissingServer);
        };

        // Disconnect all clients
        for client in &mut self.clients {
            client.disconnect().await;
        }

        // Restart server
        server.restart()?;

        // Update client addresses and reconnect
        self.update_client_addresses();
        for client in &mut self.clients {
            client.connect().await?;
        }

        Ok(())
    }

    /// Get reference to the server handle.
    pub fn server(&self) -> &ServerHandle {
        self.server.as_ref().expect("Server not configured")
    }

    /// Get mutable reference to the server handle.
    pub fn server_mut(&mut self) -> &mut ServerHandle {
        self.server.as_mut().expect("Server not configured")
    }

    /// Get the first client (panics if no clients configured).
    pub fn client(&self) -> &ClientWrapper {
        self.clients
            .first()
            .expect("No clients configured")
            .inner()
            .expect("Client not connected")
    }

    /// Get all client handles.
    pub fn clients(&self) -> &[ClientHandle] {
        &self.clients
    }

    /// Get mutable reference to all client handles.
    pub fn clients_mut(&mut self) -> &mut [ClientHandle] {
        &mut self.clients
    }

    /// Get the MCP handle if configured.
    pub fn mcp(&self) -> Option<&McpHandle> {
        self.mcp.as_ref()
    }

    /// Create an MCP client (convenience method).
    pub async fn mcp_client(&self) -> Result<McpClient, TestBinaryError> {
        self.mcp
            .as_ref()
            .ok_or(TestBinaryError::MissingMcp)?
            .create_client()
            .await
    }

    /// Get the connector handle if configured.
    pub fn connector(&self) -> Option<&ConnectorHandle> {
        self.connector.as_ref()
    }

    /// Get the test directory path.
    pub fn test_dir(&self) -> &Path {
        self.context.base_dir()
    }

    /// Collect logs from all binaries.
    pub fn collect_logs(&self) -> TestLogs {
        TestLogs {
            server: self.server.as_ref().map(|s| s.collect_logs()),
            mcp: self.mcp.as_ref().map(|m| m.collect_logs()),
            connector: self.connector.as_ref().map(|c| c.collect_logs()),
        }
    }

    /// Get a TCP client factory for creating additional clients.
    pub fn tcp_client_factory(&self) -> Option<TcpClientFactory> {
        let server = self.server.as_ref()?;
        let addr = server.tcp_addr()?;

        // Find TCP client config to get TLS and nodelay settings
        let tcp_config = self
            .client_configs
            .iter()
            .find(|c| c.transport == TransportProtocol::Tcp)
            .or(self
                .primary_client_config
                .as_ref()
                .filter(|c| c.transport == TransportProtocol::Tcp));

        let (nodelay, tls_enabled, tls_domain, tls_validate_certificate) = tcp_config
            .map(|c| {
                (
                    c.tcp_nodelay,
                    c.tls_enabled,
                    c.tls_domain
                        .clone()
                        .unwrap_or_else(|| "localhost".to_string()),
                    c.tls_validate_certificate,
                )
            })
            .unwrap_or_default();

        // Get CA cert from server if TLS is enabled
        let tls_ca_file = if tls_enabled {
            server
                .tls_ca_cert_path()
                .map(|p| p.to_string_lossy().to_string())
        } else {
            None
        };

        Some(TcpClientFactory {
            server_addr: addr.to_string(),
            nodelay,
            tls_enabled,
            tls_domain,
            tls_ca_file,
            tls_validate_certificate,
        })
    }

    /// Get an HTTP client factory for creating additional clients.
    pub fn http_client_factory(&self) -> Option<HttpClientFactory> {
        self.server
            .as_ref()
            .and_then(|s| s.http_addr())
            .map(|addr| HttpClientFactory {
                server_addr: addr.to_string(),
            })
    }

    /// Get a QUIC client factory for creating additional clients.
    pub fn quic_client_factory(&self) -> Option<QuicClientFactory> {
        self.server
            .as_ref()
            .and_then(|s| s.quic_addr())
            .map(|addr| QuicClientFactory {
                server_addr: addr.to_string(),
            })
    }

    /// Get a WebSocket client factory for creating additional clients.
    pub fn websocket_client_factory(&self) -> Option<WebSocketClientFactory> {
        let server = self.server.as_ref()?;
        let addr = server.websocket_addr()?;

        // Find WebSocket client config to get TLS settings
        let ws_config = self
            .client_configs
            .iter()
            .find(|c| c.transport == TransportProtocol::WebSocket)
            .or(self
                .primary_client_config
                .as_ref()
                .filter(|c| c.transport == TransportProtocol::WebSocket));

        let (tls_enabled, tls_domain, tls_validate_certificate) = ws_config
            .map(|c| {
                (
                    c.tls_enabled,
                    c.tls_domain
                        .clone()
                        .unwrap_or_else(|| "localhost".to_string()),
                    c.tls_validate_certificate,
                )
            })
            .unwrap_or_default();

        // Get CA cert from server if TLS is enabled
        let tls_ca_file = if tls_enabled {
            server
                .tls_ca_cert_path()
                .map(|p| p.to_string_lossy().to_string())
        } else {
            None
        };

        Some(WebSocketClientFactory {
            server_addr: addr.to_string(),
            tls_enabled,
            tls_domain,
            tls_ca_file,
            tls_validate_certificate,
        })
    }

    /// Get all available client factories.
    #[allow(clippy::vec_box)]
    pub fn all_client_factories(&self) -> Vec<Box<dyn ClientFactory>> {
        let mut factories: Vec<Box<dyn ClientFactory>> = Vec::new();
        if let Some(f) = self.tcp_client_factory() {
            factories.push(Box::new(f));
        }
        if let Some(f) = self.http_client_factory() {
            factories.push(Box::new(f));
        }
        if let Some(f) = self.quic_client_factory() {
            factories.push(Box::new(f));
        }
        if let Some(f) = self.websocket_client_factory() {
            factories.push(Box::new(f));
        }
        factories
    }

    /// Create a new TCP client logged in as root.
    pub async fn tcp_root_client(&self) -> Result<IggyClient, TestBinaryError> {
        let factory = self
            .tcp_client_factory()
            .ok_or_else(|| TestBinaryError::InvalidState {
                message: "TCP transport not available".to_string(),
            })?;
        self.create_root_client(&factory).await
    }

    /// Create a new HTTP client logged in as root.
    pub async fn http_root_client(&self) -> Result<IggyClient, TestBinaryError> {
        let factory = self
            .http_client_factory()
            .ok_or_else(|| TestBinaryError::InvalidState {
                message: "HTTP transport not available".to_string(),
            })?;
        self.create_root_client(&factory).await
    }

    /// Create a new QUIC client logged in as root.
    pub async fn quic_root_client(&self) -> Result<IggyClient, TestBinaryError> {
        let factory = self
            .quic_client_factory()
            .ok_or_else(|| TestBinaryError::InvalidState {
                message: "QUIC transport not available".to_string(),
            })?;
        self.create_root_client(&factory).await
    }

    /// Create a new WebSocket client logged in as root.
    pub async fn websocket_root_client(&self) -> Result<IggyClient, TestBinaryError> {
        let factory =
            self.websocket_client_factory()
                .ok_or_else(|| TestBinaryError::InvalidState {
                    message: "WebSocket transport not available".to_string(),
                })?;
        self.create_root_client(&factory).await
    }

    pub fn transport(&self) -> Result<TransportProtocol, TestBinaryError> {
        self.client_configs
            .first()
            .map(|c| c.transport)
            .or(self.primary_transport)
            .ok_or_else(|| TestBinaryError::InvalidState {
                message: "No client transport configured".to_string(),
            })
    }

    pub async fn root_client(&self) -> Result<IggyClient, TestBinaryError> {
        match self.transport()? {
            TransportProtocol::Tcp => self.tcp_root_client().await,
            TransportProtocol::Http => self.http_root_client().await,
            TransportProtocol::Quic => self.quic_root_client().await,
            TransportProtocol::WebSocket => self.websocket_root_client().await,
        }
    }

    pub async fn root_clients(&self, count: usize) -> Result<Vec<IggyClient>, TestBinaryError> {
        let mut clients = Vec::with_capacity(count);
        for _ in 0..count {
            clients.push(self.root_client().await?);
        }
        Ok(clients)
    }

    pub async fn new_client(&self) -> Result<IggyClient, TestBinaryError> {
        let config = self
            .client_configs
            .first()
            .or(self.primary_client_config.as_ref())
            .ok_or_else(|| TestBinaryError::InvalidState {
                message: "No client transport configured".to_string(),
            })?;
        let factory: Box<dyn ClientFactory> = match config.transport {
            TransportProtocol::Tcp => {
                let f = self
                    .tcp_client_factory()
                    .ok_or_else(|| TestBinaryError::InvalidState {
                        message: "TCP transport not available".to_string(),
                    })?;
                Box::new(f)
            }
            TransportProtocol::Http => {
                let f =
                    self.http_client_factory()
                        .ok_or_else(|| TestBinaryError::InvalidState {
                            message: "HTTP transport not available".to_string(),
                        })?;
                Box::new(f)
            }
            TransportProtocol::Quic => {
                let f =
                    self.quic_client_factory()
                        .ok_or_else(|| TestBinaryError::InvalidState {
                            message: "QUIC transport not available".to_string(),
                        })?;
                Box::new(f)
            }
            TransportProtocol::WebSocket => {
                let f = self.websocket_client_factory().ok_or_else(|| {
                    TestBinaryError::InvalidState {
                        message: "WebSocket transport not available".to_string(),
                    }
                })?;
                Box::new(f)
            }
        };
        let client = factory.create_client().await;
        Ok(IggyClient::create(client, None, None))
    }

    pub async fn new_clients(&self, count: usize) -> Result<Vec<IggyClient>, TestBinaryError> {
        let mut clients = Vec::with_capacity(count);
        for _ in 0..count {
            clients.push(self.new_client().await?);
        }
        Ok(clients)
    }

    pub async fn tcp_new_client(&self) -> Result<IggyClient, TestBinaryError> {
        let factory = self
            .tcp_client_factory()
            .ok_or_else(|| TestBinaryError::InvalidState {
                message: "TCP transport not available".to_string(),
            })?;
        let client = factory.create_client().await;
        Ok(IggyClient::create(client, None, None))
    }

    pub async fn tcp_root_clients(&self, count: usize) -> Result<Vec<IggyClient>, TestBinaryError> {
        let mut clients = Vec::with_capacity(count);
        for _ in 0..count {
            clients.push(self.tcp_root_client().await?);
        }
        Ok(clients)
    }

    pub async fn http_new_client(&self) -> Result<IggyClient, TestBinaryError> {
        let factory = self
            .http_client_factory()
            .ok_or_else(|| TestBinaryError::InvalidState {
                message: "HTTP transport not available".to_string(),
            })?;
        let client = factory.create_client().await;
        Ok(IggyClient::create(client, None, None))
    }

    pub async fn http_root_clients(
        &self,
        count: usize,
    ) -> Result<Vec<IggyClient>, TestBinaryError> {
        let mut clients = Vec::with_capacity(count);
        for _ in 0..count {
            clients.push(self.http_root_client().await?);
        }
        Ok(clients)
    }

    pub async fn quic_new_client(&self) -> Result<IggyClient, TestBinaryError> {
        let factory = self
            .quic_client_factory()
            .ok_or_else(|| TestBinaryError::InvalidState {
                message: "QUIC transport not available".to_string(),
            })?;
        let client = factory.create_client().await;
        Ok(IggyClient::create(client, None, None))
    }

    pub async fn quic_root_clients(
        &self,
        count: usize,
    ) -> Result<Vec<IggyClient>, TestBinaryError> {
        let mut clients = Vec::with_capacity(count);
        for _ in 0..count {
            clients.push(self.quic_root_client().await?);
        }
        Ok(clients)
    }

    pub async fn websocket_new_client(&self) -> Result<IggyClient, TestBinaryError> {
        let factory =
            self.websocket_client_factory()
                .ok_or_else(|| TestBinaryError::InvalidState {
                    message: "WebSocket transport not available".to_string(),
                })?;
        let client = factory.create_client().await;
        Ok(IggyClient::create(client, None, None))
    }

    pub async fn websocket_root_clients(
        &self,
        count: usize,
    ) -> Result<Vec<IggyClient>, TestBinaryError> {
        let mut clients = Vec::with_capacity(count);
        for _ in 0..count {
            clients.push(self.websocket_root_client().await?);
        }
        Ok(clients)
    }

    async fn create_root_client(
        &self,
        factory: &dyn ClientFactory,
    ) -> Result<IggyClient, TestBinaryError> {
        let client = factory.create_client().await;
        let iggy_client = IggyClient::create(client, None, None);
        iggy_client
            .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
            .await
            .map_err(|e| TestBinaryError::InvalidState {
                message: format!("Failed to login as root: {e}"),
            })?;
        Ok(iggy_client)
    }

    async fn create_clients(&mut self) -> Result<(), TestBinaryError> {
        let Some(ref server) = self.server else {
            return Ok(());
        };

        for config in &self.client_configs {
            let address = match config.transport {
                TransportProtocol::Tcp => server.tcp_addr(),
                TransportProtocol::Http => server.http_addr(),
                TransportProtocol::Quic => server.quic_addr(),
                TransportProtocol::WebSocket => server.websocket_addr(),
            };

            let Some(address) = address else {
                return Err(TestBinaryError::InvalidState {
                    message: format!("{:?} transport not available on server", config.transport),
                });
            };

            // If server has generated TLS certs and client has TLS enabled,
            // inject the CA cert path into the client config
            let mut config = config.clone();
            if config.tls_enabled
                && let Some(ca_cert_path) = server.tls_ca_cert_path()
            {
                config.tls_ca_file = Some(ca_cert_path);
            }

            let mut client = ClientHandle::new(config, address);
            client.connect().await?;
            self.clients.push(client);
        }

        Ok(())
    }

    fn update_client_addresses(&mut self) {
        let Some(ref server) = self.server else {
            return;
        };

        for client in &mut self.clients {
            let address = match client.transport() {
                TransportProtocol::Tcp => server.tcp_addr(),
                TransportProtocol::Http => server.http_addr(),
                TransportProtocol::Quic => server.quic_addr(),
                TransportProtocol::WebSocket => server.websocket_addr(),
            };

            if let Some(addr) = address {
                client.update_address(addr);
            }
        }
    }
}

/// Builder for TestHarness with fluent configuration API.
pub struct TestHarnessBuilder {
    test_name: Option<String>,
    server_config: Option<TestServerConfig>,
    mcp_config: Option<McpConfig>,
    connector_config: Option<ConnectorConfig>,
    primary_transport: Option<TransportProtocol>,
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
            connector_config: None,
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

    /// Configure the connector runtime.
    pub fn connector(mut self, config: ConnectorConfig) -> Self {
        self.connector_config = Some(config);
        self
    }

    /// Configure the connector runtime with default settings.
    pub fn default_connector(mut self) -> Self {
        self.connector_config = Some(ConnectorConfig::default());
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

        let connector = self
            .connector_config
            .map(|config| ConnectorHandle::with_config(config, context.clone()));

        Ok(TestHarness {
            context,
            server,
            mcp,
            connector,
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
