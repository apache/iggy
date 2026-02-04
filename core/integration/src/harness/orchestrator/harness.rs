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

use super::builder::TestHarnessBuilder;
use crate::harness::config::ClientConfig;
use crate::harness::context::TestContext;
use crate::harness::error::TestBinaryError;
use crate::harness::handle::{
    ClientHandle, ConnectorsRuntimeHandle, McpClient, McpHandle, ServerHandle, ServerLogs,
};
use crate::harness::traits::{Restartable, TestBinary};
use crate::http_client::HttpClientFactory;
use crate::quic_client::QuicClientFactory;
use crate::tcp_client::TcpClientFactory;
use crate::test_server::ClientFactory;
use crate::websocket_client::WebSocketClientFactory;
use futures::executor::block_on;
use iggy::prelude::{
    ClientWrapper, DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME, IggyClient, UserClient,
};
use iggy_common::TransportProtocol;
use std::path::Path;
use std::sync::Arc;

/// Collected logs from all binaries in the harness.
#[derive(Debug)]
pub struct TestLogs {
    pub servers: Vec<ServerLogs>,
}

#[derive(Default)]
pub(super) struct TlsSettings {
    pub enabled: bool,
    pub domain: String,
    pub ca_file: Option<String>,
    pub validate_certificate: bool,
}

/// Orchestrates test binaries and clients for integration tests.
pub struct TestHarness {
    pub(super) context: Arc<TestContext>,
    pub(super) servers: Vec<ServerHandle>,
    pub(super) clients: Vec<ClientHandle>,
    pub(super) client_configs: Vec<ClientConfig>,
    pub(super) primary_transport: Option<TransportProtocol>,
    pub(super) primary_client_config: Option<ClientConfig>,
    pub(super) started: bool,
}

impl std::fmt::Debug for TestHarness {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let has_mcp = self.servers.iter().any(|s| s.mcp().is_some());
        let has_connectors = self
            .servers
            .iter()
            .any(|s| s.connectors_runtime().is_some());
        f.debug_struct("TestHarness")
            .field("test_name", &self.context.test_name())
            .field("started", &self.started)
            .field("server_count", &self.servers.len())
            .field("has_mcp", &has_mcp)
            .field("has_connectors_runtime", &has_connectors)
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
        self.start_internal(
            None::<
                fn(
                    IggyClient,
                )
                    -> std::future::Ready<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
            >,
        )
        .await
    }

    /// Start all configured binaries with a seed function that runs before dependent binaries.
    ///
    /// The seed function is called after the server starts but before MCP and connector,
    /// allowing streams/topics to be created that dependent binaries may need.
    pub async fn start_with_seed<F, Fut>(&mut self, seed: F) -> Result<(), TestBinaryError>
    where
        F: FnOnce(IggyClient) -> Fut,
        Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>>,
    {
        self.start_internal(Some(seed)).await
    }

    async fn start_internal<F, Fut>(&mut self, seed: Option<F>) -> Result<(), TestBinaryError>
    where
        F: FnOnce(IggyClient) -> Fut,
        Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>>,
    {
        if self.started {
            return Err(TestBinaryError::AlreadyStarted);
        }

        for server in &mut self.servers {
            server.start()?;
        }

        if let Some(seed_fn) = seed {
            let client = self.tcp_root_client().await?;
            seed_fn(client)
                .await
                .map_err(|e| TestBinaryError::SeedFailed(e.to_string()))?;
        }

        self.start_dependents().await?;
        self.create_clients().await?;

        self.started = true;
        Ok(())
    }

    async fn start_dependents(&mut self) -> Result<(), TestBinaryError> {
        for server in &mut self.servers {
            server.start_dependents().await?;
        }
        Ok(())
    }

    /// Stop all binaries and disconnect clients.
    pub async fn stop(&mut self) -> Result<(), TestBinaryError> {
        for client in &mut self.clients {
            client.disconnect().await;
        }
        self.clients.clear();

        for server in self.servers.iter_mut().rev() {
            server.stop_dependents()?;
            server.stop()?;
        }

        self.started = false;
        Ok(())
    }

    /// Restart the primary server and reconnect all clients.
    pub async fn restart_server(&mut self) -> Result<(), TestBinaryError> {
        if self.servers.is_empty() {
            return Err(TestBinaryError::MissingServer);
        }

        for client in &mut self.clients {
            client.disconnect().await;
        }

        self.servers[0].restart()?;

        self.update_client_addresses();
        for client in &mut self.clients {
            client.connect().await?;
        }

        Ok(())
    }

    /// Get reference to the first (primary) server handle.
    pub fn server(&self) -> &ServerHandle {
        self.servers.first().expect("No servers configured")
    }

    /// Get mutable reference to the first (primary) server handle.
    pub fn server_mut(&mut self) -> &mut ServerHandle {
        self.servers.first_mut().expect("No servers configured")
    }

    /// Get reference to a specific server node by index (for clusters).
    pub fn node(&self, index: usize) -> &ServerHandle {
        self.servers.get(index).unwrap_or_else(|| {
            panic!(
                "Node {} not configured (cluster has {} nodes)",
                index,
                self.servers.len()
            )
        })
    }

    /// Get mutable reference to a specific server node by index (for clusters).
    pub fn node_mut(&mut self, index: usize) -> &mut ServerHandle {
        let len = self.servers.len();
        self.servers
            .get_mut(index)
            .unwrap_or_else(|| panic!("Node {} not configured (cluster has {} nodes)", index, len))
    }

    /// Get reference to all servers.
    pub fn all_servers(&self) -> &[ServerHandle] {
        &self.servers
    }

    /// Get the number of server nodes (1 for single server, N for cluster).
    pub fn cluster_size(&self) -> usize {
        self.servers.len()
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

    /// Get the MCP handle from the primary server if configured.
    ///
    /// # Panics
    /// Panics if called on a cluster (multiple servers). Use `node(i).mcp()` instead.
    pub fn mcp(&self) -> Option<&McpHandle> {
        assert!(
            self.servers.len() <= 1,
            "mcp() is only available for single-server setups. Use node(i).mcp() for clusters."
        );
        self.servers.first().and_then(|s| s.mcp())
    }

    /// Create an MCP client (convenience method).
    ///
    /// # Panics
    /// Panics if called on a cluster (multiple servers). Use `node(i).mcp()` instead.
    pub async fn mcp_client(&self) -> Result<McpClient, TestBinaryError> {
        self.mcp()
            .ok_or(TestBinaryError::MissingMcp)?
            .create_client()
            .await
    }

    /// Get the connectors runtime handle from the primary server if configured.
    ///
    /// # Panics
    /// Panics if called on a cluster (multiple servers). Use `node(i).connectors_runtime()` instead.
    pub fn connectors_runtime(&self) -> Option<&ConnectorsRuntimeHandle> {
        assert!(
            self.servers.len() <= 1,
            "connectors_runtime() is only available for single-server setups. Use node(i).connectors_runtime() for clusters."
        );
        self.servers.first().and_then(|s| s.connectors_runtime())
    }

    /// Get the test directory path.
    pub fn test_dir(&self) -> &Path {
        self.context.base_dir()
    }

    /// Collect logs from all binaries.
    pub fn collect_logs(&self) -> TestLogs {
        TestLogs {
            servers: self.servers.iter().map(|s| s.collect_all_logs()).collect(),
        }
    }

    /// Get a TCP client factory for creating additional clients.
    pub fn tcp_client_factory(&self) -> Option<TcpClientFactory> {
        let server = self.servers.first()?;
        let addr = server.tcp_addr()?;
        let config = self.find_client_config(TransportProtocol::Tcp);
        let tls = self.extract_tls_settings(config, server);

        Some(TcpClientFactory {
            server_addr: addr.to_string(),
            nodelay: config.map(|c| c.tcp_nodelay).unwrap_or_default(),
            tls_enabled: tls.enabled,
            tls_domain: tls.domain,
            tls_ca_file: tls.ca_file,
            tls_validate_certificate: tls.validate_certificate,
        })
    }

    /// Get an HTTP client factory for creating additional clients.
    pub fn http_client_factory(&self) -> Option<HttpClientFactory> {
        self.servers
            .first()
            .and_then(|s| s.http_addr())
            .map(|addr| HttpClientFactory {
                server_addr: addr.to_string(),
            })
    }

    /// Get a QUIC client factory for creating additional clients.
    pub fn quic_client_factory(&self) -> Option<QuicClientFactory> {
        self.servers
            .first()
            .and_then(|s| s.quic_addr())
            .map(|addr| QuicClientFactory {
                server_addr: addr.to_string(),
            })
    }

    /// Get a WebSocket client factory for creating additional clients.
    pub fn websocket_client_factory(&self) -> Option<WebSocketClientFactory> {
        let server = self.servers.first()?;
        let addr = server.websocket_addr()?;
        let config = self.find_client_config(TransportProtocol::WebSocket);
        let tls = self.extract_tls_settings(config, server);

        Some(WebSocketClientFactory {
            server_addr: addr.to_string(),
            tls_enabled: tls.enabled,
            tls_domain: tls.domain,
            tls_ca_file: tls.ca_file,
            tls_validate_certificate: tls.validate_certificate,
        })
    }

    fn find_client_config(&self, transport: TransportProtocol) -> Option<&ClientConfig> {
        self.client_configs
            .iter()
            .find(|c| c.transport == transport)
            .or(self
                .primary_client_config
                .as_ref()
                .filter(|c| c.transport == transport))
    }

    fn extract_tls_settings(
        &self,
        config: Option<&ClientConfig>,
        server: &ServerHandle,
    ) -> TlsSettings {
        let (enabled, domain, validate) = config
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

        let ca_file = if enabled {
            server
                .tls_ca_cert_path()
                .map(|p| p.to_string_lossy().to_string())
        } else {
            None
        };

        TlsSettings {
            enabled,
            domain,
            ca_file,
            validate_certificate: validate,
        }
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

    fn client_factory_for(
        &self,
        transport: TransportProtocol,
    ) -> Result<Box<dyn ClientFactory>, TestBinaryError> {
        let factory: Box<dyn ClientFactory> = match transport {
            TransportProtocol::Tcp => Box::new(self.tcp_client_factory().ok_or_else(|| {
                TestBinaryError::InvalidState {
                    message: "TCP transport not available".to_string(),
                }
            })?),
            TransportProtocol::Http => Box::new(self.http_client_factory().ok_or_else(|| {
                TestBinaryError::InvalidState {
                    message: "HTTP transport not available".to_string(),
                }
            })?),
            TransportProtocol::Quic => Box::new(self.quic_client_factory().ok_or_else(|| {
                TestBinaryError::InvalidState {
                    message: "QUIC transport not available".to_string(),
                }
            })?),
            TransportProtocol::WebSocket => {
                Box::new(self.websocket_client_factory().ok_or_else(|| {
                    TestBinaryError::InvalidState {
                        message: "WebSocket transport not available".to_string(),
                    }
                })?)
            }
        };
        Ok(factory)
    }

    /// Create a new client logged in as root for the specified transport.
    pub async fn root_client_for(
        &self,
        transport: TransportProtocol,
    ) -> Result<IggyClient, TestBinaryError> {
        let factory = self.client_factory_for(transport)?;
        self.create_root_client(&*factory).await
    }

    /// Create multiple root clients for the specified transport.
    pub async fn root_clients_for(
        &self,
        transport: TransportProtocol,
        count: usize,
    ) -> Result<Vec<IggyClient>, TestBinaryError> {
        let mut clients = Vec::with_capacity(count);
        for _ in 0..count {
            clients.push(self.root_client_for(transport).await?);
        }
        Ok(clients)
    }

    /// Create a new unauthenticated client for the specified transport.
    pub async fn new_client_for(
        &self,
        transport: TransportProtocol,
    ) -> Result<IggyClient, TestBinaryError> {
        let factory = self.client_factory_for(transport)?;
        let client = factory.create_client().await;
        Ok(IggyClient::create(client, None, None))
    }

    pub async fn tcp_root_client(&self) -> Result<IggyClient, TestBinaryError> {
        self.root_client_for(TransportProtocol::Tcp).await
    }

    pub async fn http_root_client(&self) -> Result<IggyClient, TestBinaryError> {
        self.root_client_for(TransportProtocol::Http).await
    }

    pub async fn quic_root_client(&self) -> Result<IggyClient, TestBinaryError> {
        self.root_client_for(TransportProtocol::Quic).await
    }

    pub async fn websocket_root_client(&self) -> Result<IggyClient, TestBinaryError> {
        self.root_client_for(TransportProtocol::WebSocket).await
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
        self.root_client_for(self.transport()?).await
    }

    pub async fn root_clients(&self, count: usize) -> Result<Vec<IggyClient>, TestBinaryError> {
        self.root_clients_for(self.transport()?, count).await
    }

    pub async fn new_client(&self) -> Result<IggyClient, TestBinaryError> {
        let transport = self
            .client_configs
            .first()
            .map(|c| c.transport)
            .or(self.primary_client_config.as_ref().map(|c| c.transport))
            .ok_or_else(|| TestBinaryError::InvalidState {
                message: "No client transport configured".to_string(),
            })?;
        self.new_client_for(transport).await
    }

    pub async fn new_clients(&self, count: usize) -> Result<Vec<IggyClient>, TestBinaryError> {
        let mut clients = Vec::with_capacity(count);
        for _ in 0..count {
            clients.push(self.new_client().await?);
        }
        Ok(clients)
    }

    pub async fn tcp_new_client(&self) -> Result<IggyClient, TestBinaryError> {
        self.new_client_for(TransportProtocol::Tcp).await
    }

    pub async fn tcp_root_clients(&self, count: usize) -> Result<Vec<IggyClient>, TestBinaryError> {
        self.root_clients_for(TransportProtocol::Tcp, count).await
    }

    pub async fn http_new_client(&self) -> Result<IggyClient, TestBinaryError> {
        self.new_client_for(TransportProtocol::Http).await
    }

    pub async fn http_root_clients(
        &self,
        count: usize,
    ) -> Result<Vec<IggyClient>, TestBinaryError> {
        self.root_clients_for(TransportProtocol::Http, count).await
    }

    pub async fn quic_new_client(&self) -> Result<IggyClient, TestBinaryError> {
        self.new_client_for(TransportProtocol::Quic).await
    }

    pub async fn quic_root_clients(
        &self,
        count: usize,
    ) -> Result<Vec<IggyClient>, TestBinaryError> {
        self.root_clients_for(TransportProtocol::Quic, count).await
    }

    pub async fn websocket_new_client(&self) -> Result<IggyClient, TestBinaryError> {
        self.new_client_for(TransportProtocol::WebSocket).await
    }

    pub async fn websocket_root_clients(
        &self,
        count: usize,
    ) -> Result<Vec<IggyClient>, TestBinaryError> {
        self.root_clients_for(TransportProtocol::WebSocket, count)
            .await
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

    pub(super) async fn create_clients(&mut self) -> Result<(), TestBinaryError> {
        let Some(server) = self.servers.first() else {
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
        let Some(server) = self.servers.first() else {
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

impl Drop for TestHarness {
    fn drop(&mut self) {
        let _ = block_on(self.stop());
    }
}
