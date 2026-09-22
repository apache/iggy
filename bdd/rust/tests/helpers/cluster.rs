// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use crate::helpers::env::{follower_address, leader_address, server_address};
use iggy::prelude::*;
use std::net::{SocketAddr, ToSocketAddrs};
use std::sync::Arc;

/// Resolves the server address for a role and port from the environment
pub fn resolve_server_address(role: &str, port: u16) -> String {
    match (role.to_lowercase().as_str(), port) {
        ("leader", 8091) => leader_address(),
        ("follower", 8092) => follower_address(),
        (_, 8090) => server_address(),
        _ => panic!("no address mapping for role '{role}' on port {port}"),
    }
}

/// Creates and connects a client to the specified address
pub async fn create_and_connect_client(addr: &str) -> IggyClient {
    let config = TcpClientConfig {
        server_address: addr.to_string(),
        ..TcpClientConfig::default()
    };

    let client = TcpClient::create(Arc::new(config)).expect("Failed to create TCP client");
    Client::connect(&client)
        .await
        .expect("Client should connect");

    IggyClient::create(ClientWrapper::Tcp(client), None, None)
}

/// Whether two `host:port` spellings name the same endpoint.
///
/// A client that never redirected still holds the address it was given (a
/// host name, in the BDD compose network), while a redirected one holds the
/// address the roster published (an IP). Both name the same node, so they
/// are compared once resolved, like the Go and Java suites do.
pub fn is_same_endpoint(left: &str, right: &str) -> Result<bool, String> {
    let resolve = |address: &str| -> Result<Vec<SocketAddr>, String> {
        address
            .to_socket_addrs()
            .map(Iterator::collect)
            .map_err(|error| format!("Failed to resolve server address {address}: {error}"))
    };
    let left = resolve(left)?;
    let right = resolve(right)?;
    Ok(left.iter().any(|candidate| right.contains(candidate)))
}

/// Verifies that a client is connected to the expected port
pub async fn verify_client_connection(
    client: &IggyClient,
    expected_port: u16,
) -> Result<String, String> {
    let conn_info = client.get_connection_info().await;

    if !conn_info
        .server_address
        .contains(&format!(":{}", expected_port))
    {
        return Err(format!(
            "Expected connection to port {}, but connected to: {}",
            expected_port, conn_info.server_address
        ));
    }

    // Verify client can communicate
    client
        .ping()
        .await
        .map_err(|e| format!("Client cannot ping server: {}", e))?;

    Ok(conn_info.server_address)
}

/// Checks if cluster metadata contains a healthy leader node
pub async fn verify_leader_in_metadata(client: &IggyClient) -> Result<Option<ClusterNode>, String> {
    match client.get_cluster_metadata().await {
        Ok(metadata) => {
            let leader = metadata.nodes.into_iter().find(|n| {
                matches!(n.role, ClusterNodeRole::Leader)
                    && matches!(n.status, ClusterNodeStatus::Healthy)
            });
            Ok(leader)
        }
        Err(e) if is_clustering_unavailable(&e) => {
            // Clustering not enabled, this is OK
            Ok(None)
        }
        Err(e) => Err(format!("Failed to get cluster metadata: {}", e)),
    }
}

/// Checks if an error indicates clustering is not available
pub fn is_clustering_unavailable(error: &IggyError) -> bool {
    matches!(
        error,
        IggyError::FeatureUnavailable | IggyError::InvalidCommand
    )
}

/// Updates a node's role in the cluster configuration
pub fn update_node_role(
    nodes: &mut [ClusterNode],
    node_id: u32,
    port: u16,
    role: ClusterNodeRole,
) -> bool {
    if let Some(node) = nodes
        .iter_mut()
        .find(|n| n.name == format!("node-{}", node_id) && n.endpoints.tcp == port)
    {
        node.role = role;
        node.status = ClusterNodeStatus::Healthy;
        true
    } else {
        false
    }
}

/// Determines server type from port number
pub fn server_type_from_port(port: u16) -> &'static str {
    match port {
        8090 => "single",
        8091 => "leader",
        8092 => "follower",
        _ => "unknown",
    }
}
