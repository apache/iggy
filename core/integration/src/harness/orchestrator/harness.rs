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
use crate::harness::config::{ClientConfig, ConnectorsRuntimeConfig};
use crate::harness::context::TestContext;
use crate::harness::error::TestBinaryError;
use crate::harness::handle::{
    ClientBuilder, ClientHandle, ConnectorsRuntimeHandle, McpClient, McpHandle, ServerConnection,
    ServerHandle, ServerLogs,
};
use crate::harness::shared::SharedServerInfo;
use crate::harness::traits::{IggyServerDependent, Restartable, TestBinary};
use futures::executor::block_on;
use iggy::prelude::{ClientWrapper, IggyClient};
use iggy_common::TransportProtocol;
use std::path::Path;
use std::sync::Arc;

/// Collected logs from all binaries in the harness.
#[derive(Debug)]
pub struct TestLogs {
    pub servers: Vec<ServerLogs>,
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
    pub(super) shared_server: Option<Arc<SharedServerInfo>>,
    pub(super) shared_connectors_runtime: Option<ConnectorsRuntimeHandle>,
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

    /// Create a harness that uses a shared server (no ownership of server process).
    ///
    /// Each test still gets its own `TestContext` (for logs), connector runtime,
    /// and clients. Only the iggy-server process is shared.
    pub async fn from_shared(
        shared: Arc<SharedServerInfo>,
        connectors_runtime_config: Option<ConnectorsRuntimeConfig>,
        primary_client_config: Option<ClientConfig>,
    ) -> Result<Self, TestBinaryError> {
        let mut context = TestContext::new(None, true)?;
        context.ensure_created()?;
        let context = Arc::new(context);

        let shared_cr = connectors_runtime_config
            .map(|cfg| ConnectorsRuntimeHandle::with_server_id(cfg, context.clone(), 0));

        let primary_transport = primary_client_config.as_ref().map(|c| c.transport);

        shared.acquire();

        Ok(TestHarness {
            context,
            servers: Vec::new(),
            clients: Vec::new(),
            client_configs: Vec::new(),
            primary_transport,
            primary_client_config,
            started: false,
            shared_server: Some(shared),
            shared_connectors_runtime: shared_cr,
        })
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

    /// Start a shared-server harness without a seed function.
    pub async fn start_shared(&mut self) -> Result<(), TestBinaryError> {
        self.start_shared_internal(
            None::<
                fn(
                    IggyClient,
                )
                    -> std::future::Ready<Result<(), Box<dyn std::error::Error + Send + Sync>>>,
            >,
        )
        .await
    }

    /// Start a shared-server harness with a seed function.
    ///
    /// The seed runs BEFORE the connector runtime starts, allowing it to
    /// create streams/topics that the connector expects to find.
    pub async fn start_shared_with_seed<F, Fut>(&mut self, seed: F) -> Result<(), TestBinaryError>
    where
        F: FnOnce(IggyClient) -> Fut,
        Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>>,
    {
        self.start_shared_internal(Some(seed)).await
    }

    async fn start_shared_internal<F, Fut>(
        &mut self,
        seed: Option<F>,
    ) -> Result<(), TestBinaryError>
    where
        F: FnOnce(IggyClient) -> Fut,
        Fut: std::future::Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>>,
    {
        if self.started {
            return Err(TestBinaryError::AlreadyStarted);
        }

        let shared = self
            .shared_server
            .as_ref()
            .ok_or_else(|| TestBinaryError::InvalidState {
                message: "start_shared called without shared server".to_string(),
            })?;

        let tcp_addr = shared
            .tcp_addr()
            .ok_or_else(|| TestBinaryError::InvalidState {
                message: "Shared server has no TCP address".to_string(),
            })?;

        // Seed runs before connector runtime - creates streams/topics
        if let Some(seed_fn) = seed {
            let client = self.tcp_root_client().await?;
            seed_fn(client)
                .await
                .map_err(|e| TestBinaryError::SeedFailed(e.to_string()))?;
        }

        // Connector runtime starts after seed - streams/topics exist now
        if let Some(ref mut cr) = self.shared_connectors_runtime {
            cr.set_iggy_address(tcp_addr);
            cr.start()?;
            cr.wait_ready().await?;
        }

        self.create_clients().await?;
        self.started = true;
        Ok(())
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

        // Stop per-test connector runtime (shared server mode)
        if let Some(ref mut cr) = self.shared_connectors_runtime {
            cr.stop()?;
        }

        // Release shared server ref - last test stops the server and cleans up
        if let Some(shared) = self.shared_server.take() {
            if std::thread::panicking() {
                shared.mark_failed();
            }
            shared.release();
        }

        // Stop owned servers (non-shared mode only)
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
    ///
    /// Not available in `shared_server` mode - the server is managed by `SharedServerRegistry`.
    pub fn server(&self) -> &ServerHandle {
        assert!(
            self.shared_server.is_none(),
            "server() is not available in shared_server mode"
        );
        self.servers.first().expect("No servers configured")
    }

    /// Get mutable reference to the first (primary) server handle.
    ///
    /// Not available in `shared_server` mode - the server is managed by `SharedServerRegistry`.
    pub fn server_mut(&mut self) -> &mut ServerHandle {
        assert!(
            self.shared_server.is_none(),
            "server_mut() is not available in shared_server mode"
        );
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
    /// Returns the per-test connector runtime for shared-server harnesses,
    /// or the server-owned one for standard harnesses.
    ///
    /// # Panics
    /// Panics if called on a cluster (multiple servers). Use `node(i).connectors_runtime()` instead.
    pub fn connectors_runtime(&self) -> Option<&ConnectorsRuntimeHandle> {
        if self.shared_connectors_runtime.is_some() {
            return self.shared_connectors_runtime.as_ref();
        }
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

    pub async fn root_client_for(
        &self,
        transport: TransportProtocol,
    ) -> Result<IggyClient, TestBinaryError> {
        self.client_builder_for(transport)?
            .with_root_login()
            .connect()
            .await
    }

    /// Create a new client logged in as root for the specified transport.
    pub fn client_builder_for(
        &self,
        transport: TransportProtocol,
    ) -> Result<ClientBuilder, TestBinaryError> {
        if let Some(ref shared) = self.shared_server {
            let connection = ServerConnection {
                tcp_addr: shared.tcp_addr(),
                http_addr: shared.http_addr(),
                quic_addr: shared.quic_addr(),
                websocket_addr: shared.websocket_addr(),
                tls: None,
                websocket_tls: None,
                tls_ca_cert_path: shared.tls_ca_cert_path().cloned(),
            };
            return Ok(ClientBuilder::new(transport, connection));
        }
        let server = self.servers.first().ok_or(TestBinaryError::MissingServer)?;
        match transport {
            TransportProtocol::Tcp => server.tcp_client(),
            TransportProtocol::Http => server.http_client(),
            TransportProtocol::Quic => server.quic_client(),
            TransportProtocol::WebSocket => server.websocket_client(),
        }
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
        self.client_builder_for(transport)?.connect().await
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

    pub(super) async fn create_clients(&mut self) -> Result<(), TestBinaryError> {
        // Resolve addresses from either owned server or shared server
        let (tcp, http, quic, ws, ca_cert) = if let Some(ref shared) = self.shared_server {
            (
                shared.tcp_addr(),
                shared.http_addr(),
                shared.quic_addr(),
                shared.websocket_addr(),
                shared.tls_ca_cert_path().cloned(),
            )
        } else if let Some(server) = self.servers.first() {
            (
                server.tcp_addr(),
                server.http_addr(),
                server.quic_addr(),
                server.websocket_addr(),
                server.tls_ca_cert_path(),
            )
        } else {
            return Ok(());
        };

        for config in &self.client_configs {
            let address = match config.transport {
                TransportProtocol::Tcp => tcp,
                TransportProtocol::Http => http,
                TransportProtocol::Quic => quic,
                TransportProtocol::WebSocket => ws,
            };

            let Some(address) = address else {
                return Err(TestBinaryError::InvalidState {
                    message: format!("{:?} transport not available on server", config.transport),
                });
            };

            let mut config = config.clone();
            if config.tls_enabled
                && let Some(ref path) = ca_cert
            {
                config.tls_ca_file = Some(path.clone());
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
