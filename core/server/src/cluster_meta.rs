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

//! Single source of cluster-metadata assembly.
//!
//! Both the HTTP `GET /cluster/metadata` handler and the binary
//! `GetClusterMetadata` reply build the same view: one entry per configured
//! roster node with its client ports, the current VSR primary marked
//! `Leader` and the rest `Follower`. The roster is config-only data, so it is
//! reported whenever the cluster is enabled; the leader marking is the only
//! part that needs live consensus, and it is passed in as `primary_index` so
//! each caller derives it from its own on-shard view (`None` when the serving
//! shard has no consensus - peer shards - in which case no node is marked
//! leader, but the full roster is still returned). The self-synthesized single
//! node is the cluster-disabled fallback, shared by both callers.

use configs::cluster::{ResolvedClusterNode, TransportPorts};
use iggy_common::{
    ClusterMetadata, ClusterNode, ClusterNodeRole, ClusterNodeStatus, TransportEndpoints,
};
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

/// Node name reported for the synthesized self node when no roster applies.
const SELF_NODE_NAME: &str = "iggy-node";

/// Cluster name reported when no roster is configured, matching the legacy
/// single-node label.
const SINGLE_NODE_CLUSTER_NAME: &str = "single-node";

/// Config-derived cluster topology reported by cluster-metadata reads.
///
/// Copied out of `ClusterConfig` at listener/shard start so both handlers stay
/// synchronous and never borrow live config. `self_*` describe this node and
/// back the cluster-disabled self-synthesis only.
pub struct ClusterRoster {
    pub enabled: bool,
    pub name: String,
    /// Roster nodes with selectors parsed once at roster build, so the
    /// per-request address resolution never re-parses config strings.
    pub nodes: Vec<ResolvedClusterNode>,
    /// This node's own address, reported for the synthesized self node.
    pub self_ip: String,
    /// This node's own client ports for the same self node (`None` = transport
    /// disabled).
    pub self_ports: TransportPorts,
    /// Metadata-group view, published by shard 0 (the only shard holding the
    /// consensus instance) so every shard's cluster-metadata read marks the
    /// current leader. `u64::MAX` until shard 0 first publishes.
    pub metadata_view: Arc<AtomicU64>,
}

/// Sentinel for "shard 0 has not published a view yet".
pub const METADATA_VIEW_UNKNOWN: u64 = u64::MAX;

impl ClusterRoster {
    /// A cluster-disabled roster with no self address. Used as the pre-bootstrap
    /// default before the real roster is installed; [`Self::cluster_metadata`]
    /// on it synthesizes a bare single node.
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            name: String::new(),
            nodes: Vec::new(),
            self_ip: String::new(),
            self_ports: TransportPorts::default(),
            metadata_view: Arc::new(AtomicU64::new(METADATA_VIEW_UNKNOWN)),
        }
    }

    /// The current metadata primary's roster index, from the shard-0-published
    /// view; `None` until the first publish or with no roster.
    pub fn current_primary_index(&self) -> Option<u8> {
        if self.nodes.is_empty() {
            return None;
        }
        let view = self.metadata_view.load(Ordering::Relaxed);
        if view == METADATA_VIEW_UNKNOWN {
            return None;
        }
        #[allow(clippy::cast_possible_truncation)]
        Some((view % self.nodes.len() as u64) as u8)
    }

    /// Build the [`ClusterMetadata`] view. With a configured, non-empty roster
    /// emit one node per entry, marking the node at `primary_index` the leader
    /// and the rest followers; `None` (no on-shard consensus) leaves every node
    /// a follower. Otherwise synthesize the single self node as the sole
    /// leader. `client_ip` is the requesting client's transport-level peer
    /// address, used to pick each node's advertised address from its
    /// per-client-network selectors; `None` (unknown peer) serves the
    /// catch-all address.
    pub fn cluster_metadata(
        &self,
        primary_index: Option<u8>,
        client_ip: Option<IpAddr>,
    ) -> ClusterMetadata {
        if self.enabled && !self.nodes.is_empty() {
            let nodes = self
                .nodes
                .iter()
                .map(|node| ClusterNode {
                    name: node.config().name.clone(),
                    ip: client_host(node, client_ip),
                    endpoints: ports_to_endpoints(&node.config().ports),
                    role: role_for(primary_index, node.config().replica_id),
                    status: ClusterNodeStatus::Healthy,
                })
                .collect();
            ClusterMetadata {
                name: self.name.clone(),
                nodes,
            }
        } else {
            self.self_metadata()
        }
    }

    fn self_metadata(&self) -> ClusterMetadata {
        ClusterMetadata {
            name: SINGLE_NODE_CLUSTER_NAME.to_owned(),
            nodes: vec![ClusterNode {
                name: SELF_NODE_NAME.to_owned(),
                ip: self.self_ip.clone(),
                endpoints: ports_to_endpoints(&self.self_ports),
                role: ClusterNodeRole::Leader,
                status: ClusterNodeStatus::Healthy,
            }],
        }
    }
}

/// Client-facing host in normalized form (lowercase hostname, canonical IP),
/// matching what boot validation compared and what redirect URLs render, so
/// textual config variants of one address publish identical metadata. The
/// per-client-network selectors, the catch-all `advertised_address`, and the
/// roster `ip` are consulted in that order ([`ResolvedClusterNode::advertised_for`]).
/// Metadata deliberately does NOT fail closed like the redirect path: a host
/// that parses as neither IP nor hostname (the roster `ip` is only validated
/// non-empty - Docker service names with underscores exist in the wild)
/// publishes verbatim via [`ResolvedClusterNode::raw_advertised_fallback`].
fn client_host(node: &ResolvedClusterNode, client_ip: Option<IpAddr>) -> String {
    node.advertised_for(client_ip).map_or_else(
        || node.raw_advertised_fallback().to_owned(),
        ToString::to_string,
    )
}

const fn role_for(primary_index: Option<u8>, replica_id: u8) -> ClusterNodeRole {
    match primary_index {
        Some(primary) if primary == replica_id => ClusterNodeRole::Leader,
        _ => ClusterNodeRole::Follower,
    }
}

fn ports_to_endpoints(ports: &TransportPorts) -> TransportEndpoints {
    TransportEndpoints::new(
        ports.tcp.unwrap_or(0),
        ports.quic.unwrap_or(0),
        ports.http.unwrap_or(0),
        ports.websocket.unwrap_or(0),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    use configs::cluster::{AdvertisedAddressSelector, ClusterNodeConfig};

    fn node_config(advertised_address: Option<String>) -> ClusterNodeConfig {
        ClusterNodeConfig {
            name: "node-0".to_owned(),
            ip: "10.0.0.1".to_owned(),
            advertised_address,
            advertised_addresses: Vec::new(),
            replica_id: 0,
            ports: TransportPorts::default(),
        }
    }

    fn roster_of(node: ClusterNodeConfig) -> ClusterRoster {
        ClusterRoster {
            enabled: true,
            name: "test-cluster".to_owned(),
            nodes: vec![node.into()],
            self_ip: "127.0.0.1".to_owned(),
            self_ports: TransportPorts::default(),
            metadata_view: Arc::new(AtomicU64::new(METADATA_VIEW_UNKNOWN)),
        }
    }

    fn roster(advertised_address: Option<String>) -> ClusterRoster {
        roster_of(node_config(advertised_address))
    }

    #[test]
    fn cluster_metadata_uses_advertised_address_when_configured() {
        let metadata = roster(Some("203.0.113.10".to_owned())).cluster_metadata(Some(0), None);

        assert_eq!(metadata.nodes[0].ip, "203.0.113.10");
    }

    #[test]
    fn cluster_metadata_falls_back_to_replica_ip() {
        let metadata = roster(None).cluster_metadata(Some(0), None);

        assert_eq!(metadata.nodes[0].ip, "10.0.0.1");
    }

    #[test]
    fn cluster_metadata_normalizes_advertised_hostname_to_lowercase() {
        let metadata =
            roster(Some("Broker.Example.COM".to_owned())).cluster_metadata(Some(0), None);

        assert_eq!(metadata.nodes[0].ip, "broker.example.com");
    }

    #[test]
    fn cluster_metadata_canonicalizes_advertised_ipv6_address() {
        for equivalent_address in ["2001:DB8::1", "[2001:db8::1]"] {
            let metadata =
                roster(Some(equivalent_address.to_owned())).cluster_metadata(Some(0), None);

            assert_eq!(
                metadata.nodes[0].ip, "2001:db8::1",
                "'{equivalent_address}' must publish canonical form"
            );
        }
    }

    #[test]
    fn cluster_metadata_passes_unparsable_replica_ip_verbatim() {
        let mut node = node_config(None);
        node.ip = "iggy_node".to_owned();

        let metadata = roster_of(node).cluster_metadata(Some(0), None);

        assert_eq!(metadata.nodes[0].ip, "iggy_node");
    }

    #[test]
    fn cluster_metadata_serves_the_selector_address_to_a_matching_client() {
        let mut node = node_config(Some("203.0.113.10".to_owned()));
        node.advertised_addresses = vec![AdvertisedAddressSelector {
            client_cidr: "10.0.0.0/16".to_owned(),
            address: "10.0.0.1".to_owned(),
        }];
        let cluster_roster = roster_of(node);

        let in_network =
            cluster_roster.cluster_metadata(Some(0), Some("10.0.9.9".parse().unwrap()));
        assert_eq!(in_network.nodes[0].ip, "10.0.0.1");

        let out_of_network =
            cluster_roster.cluster_metadata(Some(0), Some("198.51.100.7".parse().unwrap()));
        assert_eq!(out_of_network.nodes[0].ip, "203.0.113.10");
    }

    // Unit-level pin for the hostname-selector flow: it cannot run end to
    // end because the leader-aware SDK client redials any advertised
    // hostname other than `localhost` on every fresh connect, wedging the
    // integration harness readiness probe on an unresolvable name (see
    // cluster_metadata_vsr.rs).
    #[test]
    fn cluster_metadata_serves_a_hostname_selector_normalized_to_lowercase() {
        let mut node = node_config(Some("203.0.113.10".to_owned()));
        node.advertised_addresses = vec![AdvertisedAddressSelector {
            client_cidr: "10.0.0.0/16".to_owned(),
            address: "Broker.Internal.Test".to_owned(),
        }];

        let metadata = roster_of(node).cluster_metadata(Some(0), Some("10.0.9.9".parse().unwrap()));

        assert_eq!(metadata.nodes[0].ip, "broker.internal.test");
    }
}
