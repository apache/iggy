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

use iggy_binary_protocol::codes::GET_CLUSTER_METADATA_CODE;
use iggy_common::ClusterClient;
use iggy_common::{
    ClusterMetadata, ClusterNode, ClusterNodeRole, ClusterNodeStatus, IggyError, TransportProtocol,
};
use std::net::SocketAddr;
use std::str::FromStr;
use tracing::{debug, info, warn};

/// Maximum number of leader redirections to prevent infinite loops
const MAX_LEADER_REDIRECTS: u8 = 3;

/// An auth-gated `get_cluster_metadata` read denied before sign-in.
///
/// The read is public API, so an unauthenticated caller can reach the gate on
/// any transport. Reconnecting cannot repair it: `connect()` would re-issue
/// the same unauthenticated read forever, so every transport must fail such a
/// request fast instead of entering its reconnect path. One definition, since
/// a transport missing this rule livelocks its reconnect loop.
pub(crate) fn is_unauthenticated_metadata_probe(code: u32, error: &IggyError) -> bool {
    code == GET_CLUSTER_METADATA_CODE && matches!(error, IggyError::Unauthenticated)
}

/// What one leader check learned from the cluster roster.
pub struct LeaderCheck {
    /// The leader's address, when it is not the node the client is on.
    pub redirect: Option<String>,
    /// Every endpoint the roster named for this transport. A client keeps
    /// them as failover candidates: the address it was configured with dies
    /// with its node, and the roster is unreachable exactly when it is
    /// needed, so it has to be remembered while the connection is healthy.
    pub endpoints: Vec<String>,
}

impl LeaderCheck {
    /// A check that learned nothing: stay where we are, remember no endpoint.
    fn inconclusive() -> Self {
        Self {
            redirect: None,
            endpoints: Vec::new(),
        }
    }
}

/// Check if we need to redirect to leader and return the leader address if redirection is needed
pub async fn check_and_redirect_to_leader<C: ClusterClient>(
    client: &C,
    current_address: &str,
    transport: TransportProtocol,
) -> Result<LeaderCheck, IggyError> {
    debug!("Checking cluster metadata for leader detection");

    // A cluster can be transiently leaderless: a restarted node cedes the
    // primaryship its stale view assigns it, and until the peers' election
    // completes the roster reports no leader. That window is roughly one
    // heartbeat timeout; poll through it instead of proceeding leaderless
    // (login against a non-primary replays for its whole read timeout).
    let deadline = tokio::time::Instant::now() + LEADERLESS_WAIT_BUDGET;
    loop {
        match client.get_cluster_metadata().await {
            Ok(metadata) => {
                debug!(
                    "Got cluster metadata: {} nodes, cluster: {}",
                    metadata.nodes.len(),
                    metadata.name
                );
                let endpoints = transport_endpoints(&metadata, transport);
                match process_cluster_metadata(&metadata, current_address, transport) {
                    Outcome::Redirect(address) => {
                        return Ok(LeaderCheck {
                            redirect: Some(address),
                            endpoints,
                        });
                    }
                    Outcome::LeaderIsCurrent => {
                        return Ok(LeaderCheck {
                            redirect: None,
                            endpoints,
                        });
                    }
                    Outcome::NoLeader => {
                        if tokio::time::Instant::now() >= deadline {
                            warn!(
                                "No active leader found in cluster metadata within {LEADERLESS_WAIT_BUDGET:?}, connection will continue on server node {current_address}",
                            );
                            // A leaderless roster still names where the nodes
                            // are, and that is what failover needs.
                            return Ok(LeaderCheck {
                                redirect: None,
                                endpoints,
                            });
                        }
                        tokio::time::sleep(LEADERLESS_POLL_INTERVAL).await;
                    }
                }
            }
            // The read is auth-gated everywhere, and this check runs after
            // sign-in, so an unauthenticated answer means the session died
            // between the two. Proceed on the current node and let the
            // caller's next request surface the eviction.
            Err(IggyError::Unauthenticated) => {
                debug!(
                    "Cluster metadata answered Unauthenticated; the session is gone, connection will continue on server node {current_address}"
                );
                return Ok(LeaderCheck::inconclusive());
            }
            Err(e) => {
                warn!(
                    "Failed to get cluster metadata: {}, connection will continue on server node {}",
                    e, current_address
                );
                return Ok(LeaderCheck::inconclusive());
            }
        }
    }
}

/// How long to wait for a transiently leaderless cluster to elect before
/// proceeding on the current node anyway.
const LEADERLESS_WAIT_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);
const LEADERLESS_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(250);

/// One leader-check verdict from a cluster-metadata snapshot.
enum Outcome {
    /// A healthy leader exists elsewhere; reconnect to it.
    Redirect(String),
    /// The current node is the leader (or the cluster is single-node).
    LeaderIsCurrent,
    /// No healthy leader is marked (e.g. mid-election).
    NoLeader,
}

/// Every node's address for `transport`, in roster order. A node that does
/// not expose the transport reports port 0 and is skipped: dialing it would
/// burn a failover attempt on an endpoint that cannot answer.
fn transport_endpoints(metadata: &ClusterMetadata, transport: TransportProtocol) -> Vec<String> {
    metadata
        .nodes
        .iter()
        .filter_map(|node| {
            let port = transport_port(node, transport);
            (port != 0).then(|| format!("{}:{port}", node.ip))
        })
        .collect()
}

fn transport_port(node: &ClusterNode, transport: TransportProtocol) -> u16 {
    match transport {
        TransportProtocol::Tcp => node.endpoints.tcp,
        TransportProtocol::Quic => node.endpoints.quic,
        TransportProtocol::Http => node.endpoints.http,
        TransportProtocol::WebSocket => node.endpoints.websocket,
    }
}

/// Process cluster metadata and determine if redirection is needed
fn process_cluster_metadata(
    metadata: &ClusterMetadata,
    current_address: &str,
    transport: TransportProtocol,
) -> Outcome {
    // If there's only one node in the cluster, no redirection is needed
    if metadata.nodes.len() == 1 {
        debug!(
            "Single-node cluster detected ({}), no leader redirection needed",
            metadata.nodes[0].name
        );
        return Outcome::LeaderIsCurrent;
    }

    let leader = metadata
        .nodes
        .iter()
        .find(|n| n.role == ClusterNodeRole::Leader && n.status == ClusterNodeStatus::Healthy);

    match leader {
        Some(leader_node) => {
            let leader_port = transport_port(leader_node, transport);
            let leader_address = format!("{}:{}", leader_node.ip, leader_port);

            info!(
                "Found leader node: {} at {} (using {} transport)",
                leader_node.name, leader_address, transport
            );

            if !is_same_address(current_address, &leader_address) {
                info!(
                    "Current connection to {} is not the leader, will redirect to {}",
                    current_address, leader_address
                );
                Outcome::Redirect(leader_address)
            } else {
                debug!("Already connected to leader at {}", current_address);
                Outcome::LeaderIsCurrent
            }
        }
        None => Outcome::NoLeader,
    }
}

/// Check if two addresses refer to the same endpoint
/// Handles various formats like 127.0.0.1:8090 vs localhost:8090
pub(crate) fn is_same_address(addr1: &str, addr2: &str) -> bool {
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
    fn only_unauthenticated_cluster_metadata_is_a_pre_login_probe() {
        assert!(is_unauthenticated_metadata_probe(
            GET_CLUSTER_METADATA_CODE,
            &IggyError::Unauthenticated,
        ));
        assert!(!is_unauthenticated_metadata_probe(
            GET_CLUSTER_METADATA_CODE,
            &IggyError::Disconnected,
        ));
        assert!(!is_unauthenticated_metadata_probe(
            GET_CLUSTER_METADATA_CODE + 1,
            &IggyError::Unauthenticated,
        ));
    }

    fn node(name: &str, ip: &str, tcp: u16, role: ClusterNodeRole) -> ClusterNode {
        ClusterNode {
            name: name.to_string(),
            ip: ip.to_string(),
            endpoints: iggy_common::TransportEndpoints::new(tcp, 0, 3000, 3001),
            role,
            status: ClusterNodeStatus::Healthy,
        }
    }

    #[test]
    fn the_roster_names_every_node_that_exposes_the_transport() {
        let metadata = ClusterMetadata {
            name: "iggy".to_string(),
            nodes: vec![
                node("iggy-1", "10.0.0.1", 8090, ClusterNodeRole::Leader),
                node("iggy-2", "10.0.0.2", 8090, ClusterNodeRole::Follower),
                node("iggy-3", "10.0.0.3", 8090, ClusterNodeRole::Follower),
            ],
        };

        assert_eq!(
            transport_endpoints(&metadata, TransportProtocol::Tcp),
            vec!["10.0.0.1:8090", "10.0.0.2:8090", "10.0.0.3:8090"]
        );
        // A node that does not expose the transport reports port 0; dialing
        // it would burn a failover attempt on an endpoint that cannot answer.
        assert!(transport_endpoints(&metadata, TransportProtocol::Quic).is_empty());
    }

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
