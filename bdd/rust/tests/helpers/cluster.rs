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

use iggy::clients::client::IggyClient;
use iggy::prelude::*;
use integration::tcp_client::TcpClientFactory;
use integration::test_server::ClientFactory;
use std::env;

/// Resolves server address based on role and port, checking environment variables first
pub fn resolve_server_address(role: &str, port: u16) -> String {
    match (role.to_lowercase().as_str(), port) {
        ("leader", 8091) => {
            env::var("IGGY_TCP_ADDRESS_LEADER").unwrap_or_else(|_| "iggy-leader:8091".to_string())
        }
        ("follower", 8092) => env::var("IGGY_TCP_ADDRESS_FOLLOWER")
            .unwrap_or_else(|_| "iggy-follower:8092".to_string()),
        ("single", 8090) | (_, 8090) => {
            env::var("IGGY_TCP_ADDRESS").unwrap_or_else(|_| "iggy-server:8090".to_string())
        }
        _ => format!("iggy-server:{}", port),
    }
}

/// Creates and connects a client to the specified address
pub async fn create_and_connect_client(addr: &str) -> IggyClient {
    let client_factory = TcpClientFactory {
        server_addr: addr.to_string(),
        ..Default::default()
    };

    let client = client_factory.create_client().await;
    let client = IggyClient::create(client, None, None);

    client.connect().await.expect("Client should connect");
    client
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
        .find(|n| n.id == node_id && n.address.ends_with(&format!(":{}", port)))
    {
        node.role = role;
        node.status = ClusterNodeStatus::Healthy;
        true
    } else {
        false
    }
}

/// Extracts port number from an address string
pub fn extract_port_from_address(address: &str) -> Option<u16> {
    address
        .rsplit(':')
        .next()
        .and_then(|port_str| port_str.parse().ok())
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
