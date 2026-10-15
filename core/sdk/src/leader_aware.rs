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

use crate::links::connect::{is_same_address, is_same_spelling, node_address, transport_port};
use iggy_binary_protocol::codes::GET_CLUSTER_METADATA_CODE;
use iggy_common::ClusterClient;
use iggy_common::{
    ClusterMetadata, ClusterNodeRole, ClusterNodeStatus, IggyError, TransportProtocol,
};
use std::collections::VecDeque;
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
    /// Full roster size, including nodes that do not expose this transport.
    pub node_count: usize,
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
            node_count: 0,
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
                match process_cluster_metadata(&metadata, current_address, transport).await {
                    Outcome::Redirect(address) => {
                        return Ok(LeaderCheck {
                            node_count: metadata.nodes.len(),
                            redirect: Some(address),
                            endpoints,
                        });
                    }
                    Outcome::LeaderIsCurrent => {
                        return Ok(LeaderCheck {
                            node_count: metadata.nodes.len(),
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
                                node_count: metadata.nodes.len(),
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

/// Every endpoint the roster names for `transport`, empty when the read did
/// not answer.
///
/// No leader verdict and no waiting for an election: the caller is not moving
/// anywhere, it only wants somewhere to dial once the node it is on dies.
pub(crate) async fn read_transport_endpoints<C: ClusterClient>(
    client: &C,
    transport: TransportProtocol,
) -> (usize, Vec<String>) {
    match client.get_cluster_metadata().await {
        Ok(metadata) => (
            metadata.nodes.len(),
            transport_endpoints(&metadata, transport),
        ),
        Err(error) => {
            debug!("Failed to read the cluster roster: {error}");
            (0, Vec::new())
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
            (port != 0).then(|| node_address(node, port))
        })
        .collect()
}

/// Process cluster metadata and determine if redirection is needed
async fn process_cluster_metadata(
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
            let leader_address = node_address(leader_node, leader_port);

            info!(
                "Found leader node: {} at {} (using {} transport)",
                leader_node.name, leader_address, transport
            );

            if !is_same_address(current_address, &leader_address).await {
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

/// One bounded pass over the cluster endpoints a request has not tried yet.
///
/// The metadata leader check cannot repair a persistent not-admitted refusal:
/// metadata and partition consensus groups elect independently, so the
/// metadata leader can hold a follower replica of the partition a request
/// targets. `TransientNotAccepted` marks the request as never admitted and
/// safe to re-issue anywhere. The walk owns a roster snapshot for one request
/// and removes every endpoint as it is attempted, so a stale roster or a
/// failed dial cannot cycle the request back through nodes it already tried.
#[derive(Debug)]
pub(crate) struct RosterWalk {
    /// Whether the roster named at least one node when the walk was built.
    roster_known: bool,
    remaining: VecDeque<String>,
    attempted: Vec<String>,
}

impl RosterWalk {
    pub(crate) fn new(current: &str, roster: &[String]) -> Self {
        let mut ordered: VecDeque<String> = VecDeque::with_capacity(roster.len());
        let start = roster
            .iter()
            .position(|endpoint| is_same_spelling(endpoint, current))
            .map_or(0, |position| position + 1);

        for offset in 0..roster.len() {
            let endpoint = roster[(start + offset) % roster.len()].clone();
            if is_same_spelling(&endpoint, current)
                || ordered
                    .iter()
                    .any(|queued| is_same_spelling(queued, &endpoint))
            {
                continue;
            }
            ordered.push_back(endpoint);
        }

        Self {
            remaining: ordered,
            attempted: vec![current.to_owned()],
            roster_known: !roster.is_empty(),
        }
    }

    /// Record an endpoint chosen outside the roster walk, such as a metadata
    /// leader redirect. Returns whether this request had not tried it before.
    pub(crate) fn record_attempt(&mut self, endpoint: &str) -> bool {
        self.remaining
            .retain(|queued| !is_same_spelling(queued, endpoint));
        if self
            .attempted
            .iter()
            .any(|attempted| is_same_spelling(attempted, endpoint))
        {
            return false;
        }
        self.attempted.push(endpoint.to_owned());
        true
    }

    pub(crate) fn next(&mut self) -> Option<String> {
        let endpoint = self.remaining.pop_front()?;
        self.attempted.push(endpoint.clone());
        Some(endpoint)
    }

    /// True only when the roster itself names one node. An empty roster (its
    /// discovery failed) also leaves nothing to walk, but replaying that one
    /// address would be a guess, not a decision.
    pub(crate) fn is_single_endpoint(&self) -> bool {
        self.roster_known && self.remaining.is_empty() && self.attempted.len() == 1
    }
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
    use iggy_common::ClusterNode;
    use std::net::SocketAddr;
    use std::str::FromStr;

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

    fn node(replica_id: u8, name: &str, ip: &str, tcp: u16, role: ClusterNodeRole) -> ClusterNode {
        ClusterNode {
            replica_id,
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
                node(0, "iggy-1", "10.0.0.1", 8090, ClusterNodeRole::Leader),
                node(1, "iggy-2", "10.0.0.2", 8090, ClusterNodeRole::Follower),
                node(2, "iggy-3", "10.0.0.3", 8090, ClusterNodeRole::Follower),
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

    // A port appended to a bare IPv6 address parses as neither, so the roster
    // of an IPv6 cluster would name endpoints no dial can use while still
    // counting as somewhere to fail over to.
    #[test]
    fn an_ipv6_node_is_named_as_a_bracketed_address() {
        let metadata = ClusterMetadata {
            name: "iggy".to_string(),
            nodes: vec![
                node(0, "iggy-1", "::1", 8090, ClusterNodeRole::Leader),
                node(1, "iggy-2", "[fd00::2]", 8090, ClusterNodeRole::Follower),
            ],
        };

        let endpoints = transport_endpoints(&metadata, TransportProtocol::Tcp);
        assert_eq!(endpoints, vec!["[::1]:8090", "[fd00::2]:8090"]);
        for endpoint in endpoints {
            assert!(
                SocketAddr::from_str(&endpoint).is_ok(),
                "the roster named an endpoint no dial can parse: {endpoint}"
            );
        }
    }

    // The address a redirect hands to the next dial comes from the same
    // roster entry, so it has to be spelled the same way.
    #[tokio::test]
    async fn a_redirect_to_an_ipv6_leader_names_a_dialable_address() {
        let metadata = ClusterMetadata {
            name: "iggy".to_string(),
            nodes: vec![
                node(0, "iggy-1", "fd00::1", 8090, ClusterNodeRole::Leader),
                node(1, "iggy-2", "fd00::2", 8090, ClusterNodeRole::Follower),
            ],
        };

        match process_cluster_metadata(&metadata, "[fd00::2]:8090", TransportProtocol::Tcp).await {
            Outcome::Redirect(leader) => assert_eq!(leader, "[fd00::1]:8090"),
            Outcome::LeaderIsCurrent => panic!("the follower was taken for the leader"),
            Outcome::NoLeader => panic!("the roster named a healthy leader"),
        }
    }

    #[test]
    fn a_roster_walk_visits_each_other_endpoint_once() {
        let roster = vec![
            "10.0.0.1:8090".to_string(),
            "10.0.0.2:8090".to_string(),
            "10.0.0.3:8090".to_string(),
        ];
        let mut walk = RosterWalk::new("10.0.0.1:8090", &roster);
        assert_eq!(walk.next().as_deref(), Some("10.0.0.2:8090"));
        assert_eq!(walk.next().as_deref(), Some("10.0.0.3:8090"));
        assert_eq!(walk.next(), None);
        assert!(
            !walk.is_single_endpoint(),
            "exhausting a cluster does not make it a single node"
        );

        let mut from_last = RosterWalk::new("10.0.0.3:8090", &roster);
        assert_eq!(from_last.next().as_deref(), Some("10.0.0.1:8090"));
        assert_eq!(from_last.next().as_deref(), Some("10.0.0.2:8090"));
        assert_eq!(from_last.next(), None);
    }

    #[test]
    fn given_single_endpoint_roster_when_exhausted_should_allow_local_retry() {
        assert!(!RosterWalk::new("127.0.0.1:8090", &[]).is_single_endpoint());
        let mut walk = RosterWalk::new("127.0.0.1:8090", &["localhost:8090".to_owned()]);
        assert!(walk.is_single_endpoint());
        assert_eq!(walk.next(), None);
        assert!(walk.is_single_endpoint());
        walk.record_attempt("127.0.0.2:8090");
        assert!(!walk.is_single_endpoint());
    }

    #[test]
    fn a_roster_walk_never_revisits_redirects_or_duplicate_spellings() {
        let mut walk = RosterWalk::new(
            "localhost:8090",
            &[
                "127.0.0.1:8090".to_string(),
                "10.0.0.2:8090".to_string(),
                "10.0.0.2:8090".to_string(),
                "10.0.0.3:8090".to_string(),
            ],
        );
        assert!(walk.record_attempt("10.0.0.3:8090"));
        assert!(!walk.record_attempt("10.0.0.3:8090"));
        assert_eq!(walk.next().as_deref(), Some("10.0.0.2:8090"));
        assert_eq!(walk.next(), None);

        assert_eq!(RosterWalk::new("10.0.0.1:8090", &[]).next(), None);
        // Same endpoint under a different spelling still counts as nowhere.
        assert_eq!(
            RosterWalk::new("localhost:8090", &["127.0.0.1:8090".to_string()]).next(),
            None
        );
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
