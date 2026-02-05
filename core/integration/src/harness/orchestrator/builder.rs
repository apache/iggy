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
use crate::harness::config::{
    ClientConfig, ConnectorsRuntimeConfig, IpAddrKind, McpConfig, TestServerConfig,
};
use crate::harness::context::TestContext;
use crate::harness::error::TestBinaryError;
use crate::harness::handle::ServerHandle;
use crate::harness::port_reserver::ClusterPortReserver;
use crate::harness::traits::TestBinary;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

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
    cluster_node_count: Option<usize>,
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
            cluster_node_count: None,
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

    /// Configure a multi-node cluster.
    ///
    /// When set, creates N server nodes configured as a cluster.
    /// The first node starts as leader candidate, others as followers.
    pub fn cluster_nodes(mut self, count: usize) -> Self {
        self.cluster_node_count = Some(count);
        self
    }

    /// Build the TestHarness. Does NOT start any binaries.
    pub fn build(self) -> Result<TestHarness, TestBinaryError> {
        let mut context = TestContext::new(self.test_name, self.cleanup)?;
        context.ensure_created()?;
        let context = Arc::new(context);

        let mut servers = build_servers(self.server_config, self.cluster_node_count, &context)?;

        // Attach MCP and connectors runtime to the first server (primary)
        if let Some(server) = servers.first_mut() {
            if let Some(config) = self.mcp_config {
                server.set_mcp_config(config);
            }
            if let Some(config) = self.connectors_runtime_config {
                server.set_connectors_runtime_config(config);
            }
        }

        // Default to TCP transport if server is configured but no client transport specified
        let primary_transport =
            if self.primary_transport.is_none() && !servers.is_empty() && self.clients.is_empty() {
                Some(iggy_common::TransportProtocol::Tcp)
            } else {
                self.primary_transport
            };

        Ok(TestHarness {
            context,
            servers,
            clients: Vec::new(),
            client_configs: self.clients,
            primary_transport,
            primary_client_config: self.primary_client_config,
            started: false,
        })
    }
}

fn build_servers(
    server_config: Option<TestServerConfig>,
    cluster_node_count: Option<usize>,
    context: &Arc<TestContext>,
) -> Result<Vec<ServerHandle>, TestBinaryError> {
    let Some(config) = server_config else {
        return Ok(Vec::new());
    };

    let node_count = cluster_node_count.unwrap_or(1);

    if node_count == 1 {
        return Ok(vec![ServerHandle::with_config(config, context.clone())]);
    }

    // Multi-node cluster: pre-reserve all ports
    let cluster_ports = ClusterPortReserver::reserve(node_count, config.ip_kind, &config)?;
    let all_addrs = cluster_ports.all_addresses();
    let cluster_name = format!("test-cluster-{}", Uuid::new_v4());

    let mut servers = Vec::with_capacity(node_count);
    for (i, (addrs, reserver)) in all_addrs
        .iter()
        .zip(cluster_ports.into_reservers())
        .enumerate()
    {
        let mut cluster_envs = build_cluster_envs(i, &cluster_name, &all_addrs, config.ip_kind);

        // Inject the pre-reserved port addresses
        if let Some(tcp) = addrs.tcp {
            cluster_envs.insert("IGGY_TCP_ADDRESS".to_string(), tcp.to_string());
        }
        if let Some(http) = addrs.http {
            cluster_envs.insert("IGGY_HTTP_ADDRESS".to_string(), http.to_string());
        }
        if let Some(quic) = addrs.quic {
            cluster_envs.insert("IGGY_QUIC_ADDRESS".to_string(), quic.to_string());
        }
        if let Some(websocket) = addrs.websocket {
            cluster_envs.insert("IGGY_WEBSOCKET_ADDRESS".to_string(), websocket.to_string());
        }

        let mut server = ServerHandle::with_cluster_config(
            config.clone(),
            context.clone(),
            i as u32,
            cluster_envs,
        );
        server.set_port_reserver(reserver);
        servers.push(server);
    }

    Ok(servers)
}

fn build_cluster_envs(
    node_index: usize,
    cluster_name: &str,
    all_addrs: &[crate::harness::port_reserver::ProtocolAddresses],
    ip_kind: IpAddrKind,
) -> HashMap<String, String> {
    let mut envs = HashMap::new();

    let loopback = match ip_kind {
        IpAddrKind::V4 => "127.0.0.1",
        IpAddrKind::V6 => "::1",
    };

    envs.insert("IGGY_CLUSTER_ENABLED".to_string(), "true".to_string());
    envs.insert("IGGY_CLUSTER_NAME".to_string(), cluster_name.to_string());
    envs.insert(
        "IGGY_CLUSTER_NODE_CURRENT_NAME".to_string(),
        format!("node-{}", node_index),
    );
    envs.insert(
        "IGGY_CLUSTER_NODE_CURRENT_IP".to_string(),
        loopback.to_string(),
    );

    // Add other nodes' addresses
    let mut other_index = 0;
    for (i, addrs) in all_addrs.iter().enumerate() {
        if i == node_index {
            continue;
        }

        envs.insert(
            format!("IGGY_CLUSTER_NODE_OTHERS_{other_index}_NAME"),
            format!("node-{i}"),
        );
        envs.insert(
            format!("IGGY_CLUSTER_NODE_OTHERS_{other_index}_IP"),
            loopback.to_string(),
        );

        if let Some(tcp) = addrs.tcp {
            envs.insert(
                format!("IGGY_CLUSTER_NODE_OTHERS_{other_index}_PORTS_TCP"),
                tcp.port().to_string(),
            );
        }
        if let Some(http) = addrs.http {
            envs.insert(
                format!("IGGY_CLUSTER_NODE_OTHERS_{other_index}_PORTS_HTTP"),
                http.port().to_string(),
            );
        }
        if let Some(quic) = addrs.quic {
            envs.insert(
                format!("IGGY_CLUSTER_NODE_OTHERS_{other_index}_PORTS_QUIC"),
                quic.port().to_string(),
            );
        }
        if let Some(websocket) = addrs.websocket {
            envs.insert(
                format!("IGGY_CLUSTER_NODE_OTHERS_{other_index}_PORTS_WEBSOCKET"),
                websocket.port().to_string(),
            );
        }

        other_index += 1;
    }

    envs
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

        assert_eq!(harness.servers.len(), 1);
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

        assert_eq!(harness.servers.len(), 1);
        assert!(harness.mcp().is_some());
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

        assert_eq!(harness.servers.len(), 1);
    }
}
