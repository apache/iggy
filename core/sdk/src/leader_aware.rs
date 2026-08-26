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
                match process_cluster_metadata(&metadata, current_address, transport).await {
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

/// Every endpoint the roster names for `transport`, empty when the read did
/// not answer.
///
/// No leader verdict and no waiting for an election: the caller is not moving
/// anywhere, it only wants somewhere to dial once the node it is on dies.
pub(crate) async fn read_transport_endpoints<C: ClusterClient>(
    client: &C,
    transport: TransportProtocol,
) -> Vec<String> {
    match client.get_cluster_metadata().await {
        Ok(metadata) => transport_endpoints(&metadata, transport),
        Err(error) => {
            debug!("Failed to read the cluster roster: {error}");
            Vec::new()
        }
    }
}

/// How long to wait for a transiently leaderless cluster to elect before
/// proceeding on the current node anyway.
const LEADERLESS_WAIT_BUDGET: std::time::Duration = std::time::Duration::from_secs(5);
const LEADERLESS_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(250);

/// Bound on one name lookup made to compare two addresses.
const RESOLVE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

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

/// One node's `host:port`, bracketing a literal IPv6 address. Appending a
/// port to a bare `::1` yields a spelling no dial can parse, so an IPv6
/// cluster would hand out a roster of undialable entries that still count as
/// endpoints to fail over to.
fn node_address(node: &ClusterNode, port: u16) -> String {
    if node.ip.contains(':') && !node.ip.starts_with('[') {
        format!("[{}]:{port}", node.ip)
    } else {
        format!("{}:{port}", node.ip)
    }
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

/// Whether two addresses are written the same way, up to canonicalization
/// (`localhost` and `[::]` spellings, and a literal address compared as an
/// address rather than as text).
///
/// Cheap and non-blocking, which is the whole point: the resolving comparison
/// below is a `getaddrinfo`, and every caller reaches this first.
pub(crate) fn is_same_spelling(addr1: &str, addr2: &str) -> bool {
    match (parse_address(addr1), parse_address(addr2)) {
        (Some(sock1), Some(sock2)) => sock1.ip() == sock2.ip() && sock1.port() == sock2.port(),
        _ => normalize_address(addr1) == normalize_address(addr2),
    }
}

/// Check if two addresses refer to the same endpoint
/// Handles various formats like 127.0.0.1:8090 vs localhost:8090
///
/// A host name and the address it resolves to are one endpoint too: a client
/// configured as `iggy-server:8090` whose roster advertises `10.0.0.5:8090`
/// would otherwise dial that node twice per failover sweep, and a single-node
/// deployment would be treated as a cluster.
///
/// Resolution is the last resort, only when the spellings differ and at least
/// one side is not a literal address. It runs through the runtime's resolver
/// rather than `ToSocketAddrs`: name lookup is a blocking `getaddrinfo`, and
/// this is called from the connect and redirect paths, where stalling a
/// runtime worker on a slow resolver would stall every task sharing it.
pub(crate) async fn is_same_address(addr1: &str, addr2: &str) -> bool {
    is_same_address_with(addr1, addr2, resolve_all).await
}

/// [`is_same_address`] against a caller-provided resolver, so the fallback can
/// be exercised without depending on what the machine's resolver answers.
async fn is_same_address_with<R, F>(addr1: &str, addr2: &str, resolve: R) -> bool
where
    R: Fn(String) -> F,
    F: Future<Output = Option<Vec<SocketAddr>>>,
{
    if is_same_spelling(addr1, addr2) {
        return true;
    }

    // Two literal addresses that did not compare equal are different
    // endpoints; resolving them would only hand back what they already say.
    if parse_address(addr1).is_some() && parse_address(addr2).is_some() {
        return false;
    }

    let (Some(first), Some(second)) = (
        resolve(addr1.to_owned()).await,
        resolve(addr2.to_owned()).await,
    ) else {
        return false;
    };
    first.iter().any(|resolved| second.contains(resolved))
}

/// Every socket address a host:port spelling resolves to, `None` when the
/// resolver does not know the name or does not answer in time (which then
/// compares unequal, at worst costing one extra dial).
async fn resolve_all(addr: String) -> Option<Vec<SocketAddr>> {
    // A resolver that never answers must not own the request budget: this
    // comparison runs on the connect and redirect paths, the redirect one
    // inside the caller's request deadline, and `lookup_host` has no deadline
    // of its own.
    let lookup = tokio::time::timeout(RESOLVE_TIMEOUT, tokio::net::lookup_host(addr))
        .await
        .ok()?;
    let resolved: Vec<SocketAddr> = lookup.ok()?.collect();
    (!resolved.is_empty()).then_some(resolved)
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

    // A port appended to a bare IPv6 address parses as neither, so the roster
    // of an IPv6 cluster would name endpoints no dial can use while still
    // counting as somewhere to fail over to.
    #[test]
    fn an_ipv6_node_is_named_as_a_bracketed_address() {
        let metadata = ClusterMetadata {
            name: "iggy".to_string(),
            nodes: vec![
                node("iggy-1", "::1", 8090, ClusterNodeRole::Leader),
                node("iggy-2", "[fd00::2]", 8090, ClusterNodeRole::Follower),
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
                node("iggy-1", "fd00::1", 8090, ClusterNodeRole::Leader),
                node("iggy-2", "fd00::2", 8090, ClusterNodeRole::Follower),
            ],
        };

        match process_cluster_metadata(&metadata, "[fd00::2]:8090", TransportProtocol::Tcp).await {
            Outcome::Redirect(leader) => assert_eq!(leader, "[fd00::1]:8090"),
            Outcome::LeaderIsCurrent => panic!("the follower was taken for the leader"),
            Outcome::NoLeader => panic!("the roster named a healthy leader"),
        }
    }

    #[tokio::test]
    async fn test_is_same_address() {
        assert!(is_same_address("127.0.0.1:8090", "127.0.0.1:8090").await);
        assert!(is_same_address("localhost:8090", "127.0.0.1:8090").await);
        assert!(!is_same_address("127.0.0.1:8090", "127.0.0.1:8091").await);
        assert!(!is_same_address("192.168.1.1:8090", "127.0.0.1:8090").await);
    }

    /// A stand-in resolver: the BDD cluster's spelling of one node, which no
    /// canonicalization rewrites, so only the resolving comparison can equate
    /// the two. `None` for anything else, like a name the resolver does not
    /// know.
    async fn resolve_bdd_leader(addr: String) -> Option<Vec<SocketAddr>> {
        match addr.as_str() {
            "iggy-leader:8091" | "172.28.0.101:8091" => {
                Some(vec![SocketAddr::from(([172, 28, 0, 101], 8091))])
            }
            _ => None,
        }
    }

    // A host name and the address it resolves to name one endpoint. Exactly
    // the case the BDD cluster hits: the client dials `iggy-leader:8091` and
    // the roster advertises `172.28.0.101:8091`.
    #[tokio::test]
    async fn a_host_name_matches_the_address_it_resolves_to() {
        assert!(
            is_same_address_with("iggy-leader:8091", "172.28.0.101:8091", resolve_bdd_leader).await
        );
    }

    // A name the resolver does not know compares unequal rather than
    // erroring, and a resolvable name never matches another port.
    #[tokio::test]
    async fn an_unresolvable_name_or_another_port_is_a_different_endpoint() {
        assert!(
            !is_same_address_with(
                "iggy-follower:8092",
                "172.28.0.101:8091",
                resolve_bdd_leader
            )
            .await
        );
        assert!(
            !is_same_address_with("iggy-leader:8091", "172.28.0.101:8092", resolve_bdd_leader)
                .await
        );
        assert!(!is_same_address("no-such-host.invalid:8090", "127.0.0.1:8090").await);
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
