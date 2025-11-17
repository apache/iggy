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

use iggy_binary_protocol::ClusterClient;
use iggy_common::{
    ClusterMetadata, ClusterNodeRole, ClusterNodeStatus, IggyError, IggyErrorDiscriminants,
};
use std::net::SocketAddr;
use std::str::FromStr;
use tracing::{debug, info, warn};

/// Maximum number of leader redirections to prevent infinite loops
const MAX_LEADER_REDIRECTS: u8 = 3;

/// Check if we need to redirect to leader and return the leader address if redirection is needed
pub async fn check_and_redirect_to_leader<C: ClusterClient>(
    client: &C,
    current_address: &str,
) -> Result<Option<String>, IggyError> {
    debug!("Checking cluster metadata for leader detection");

    match client.get_cluster_metadata().await {
        Ok(metadata) => {
            debug!(
                "Got cluster metadata: {} nodes, cluster: {}",
                metadata.nodes.len(),
                metadata.name
            );
            process_cluster_metadata(&metadata, current_address)
        }
        Err(e) if is_feature_unavailable_error(&e) => {
            debug!("Cluster metadata feature unavailable - server doesn't support clustering");
            Ok(None)
        }
        Err(e) => {
            warn!(
                "Failed to get cluster metadata: {}, connection will continue on server node {}",
                e, current_address
            );
            Ok(None)
        }
    }
}

/// Process cluster metadata and determine if redirection is needed
fn process_cluster_metadata(
    metadata: &ClusterMetadata,
    current_address: &str,
) -> Result<Option<String>, IggyError> {
    let leader = metadata
        .nodes
        .iter()
        .find(|n| n.role == ClusterNodeRole::Leader && n.status == ClusterNodeStatus::Healthy);

    match leader {
        Some(leader_node) => {
            info!(
                "Found leader node: {} at {}",
                leader_node.name, leader_node.address
            );

            if !is_same_address(current_address, &leader_node.address) {
                info!(
                    "Current connection to {} is not the leader, will redirect to {}",
                    current_address, leader_node.address
                );
                Ok(Some(leader_node.address.clone()))
            } else {
                debug!("Already connected to leader at {}", current_address);
                Ok(None)
            }
        }
        None => {
            warn!(
                "No active leader found in cluster metadata, connection will continue on server node {}",
                current_address
            );
            Ok(None)
        }
    }
}

/// Check if two addresses refer to the same endpoint
/// Handles various formats like 127.0.0.1:8090 vs localhost:8090
fn is_same_address(addr1: &str, addr2: &str) -> bool {
    match (parse_address(addr1), parse_address(addr2)) {
        (Some(sock1), Some(sock2)) => sock1.ip() == sock2.ip() && sock1.port() == sock2.port(),
        _ => normalize_address(addr1) == normalize_address(addr2),
    }
}

/// Parse address string to SocketAddr, handling various formats
fn parse_address(addr: &str) -> Option<SocketAddr> {
    if let Ok(socket_addr) = SocketAddr::from_str(addr) {
        return Some(socket_addr);
    }

    let normalized = addr
        .replace("localhost", "127.0.0.1")
        .replace("[::]", "[::1]");

    SocketAddr::from_str(&normalized).ok()
}

/// Normalize address string for comparison
fn normalize_address(addr: &str) -> String {
    addr.to_lowercase()
        .replace("localhost", "127.0.0.1")
        .replace("[::]", "[::1]")
}

/// Check if the error indicates that the feature is unavailable
fn is_feature_unavailable_error(error: &IggyError) -> bool {
    matches!(
        error,
        IggyError::FeatureUnavailable | IggyError::InvalidCommand
    ) || error.as_code() == IggyErrorDiscriminants::FeatureUnavailable as u32
}

/// Struct to track leader redirection state
#[derive(Debug, Clone)]
pub struct LeaderRedirectionState {
    pub redirect_count: u8,
    pub last_leader_address: Option<String>,
}

impl LeaderRedirectionState {
    pub fn new() -> Self {
        Self {
            redirect_count: 0,
            last_leader_address: None,
        }
    }

    pub fn can_redirect(&self) -> bool {
        self.redirect_count < MAX_LEADER_REDIRECTS
    }

    pub fn increment_redirect(&mut self, leader_address: String) {
        self.redirect_count += 1;
        self.last_leader_address = Some(leader_address);
    }

    pub fn reset(&mut self) {
        self.redirect_count = 0;
        self.last_leader_address = None;
    }
}

impl Default for LeaderRedirectionState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_same_address() {
        assert!(is_same_address("127.0.0.1:8090", "127.0.0.1:8090"));
        assert!(is_same_address("localhost:8090", "127.0.0.1:8090"));
        assert!(!is_same_address("127.0.0.1:8090", "127.0.0.1:8091"));
        assert!(!is_same_address("192.168.1.1:8090", "127.0.0.1:8090"));
    }

    #[test]
    fn test_normalize_address() {
        assert_eq!(normalize_address("localhost:8090"), "127.0.0.1:8090");
        assert_eq!(normalize_address("LOCALHOST:8090"), "127.0.0.1:8090");
        assert_eq!(normalize_address("[::]:8090"), "[::1]:8090");
    }

    #[test]
    fn test_leader_redirection_state() {
        let mut state = LeaderRedirectionState::new();
        assert!(state.can_redirect());
        assert_eq!(state.redirect_count, 0);

        state.increment_redirect("127.0.0.1:8090".to_string());
        assert!(state.can_redirect());
        assert_eq!(state.redirect_count, 1);

        state.increment_redirect("127.0.0.1:8091".to_string());
        state.increment_redirect("127.0.0.1:8092".to_string());
        assert!(!state.can_redirect());
        assert_eq!(state.redirect_count, 3);

        state.reset();
        assert!(state.can_redirect());
        assert_eq!(state.redirect_count, 0);
    }
}
