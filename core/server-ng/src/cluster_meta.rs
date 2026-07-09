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

use configs::cluster::{ClusterNodeConfig, TransportPorts};
use iggy_common::{
    ClusterMetadata, ClusterNode, ClusterNodeRole, ClusterNodeStatus, TransportEndpoints,
};

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
    pub nodes: Vec<ClusterNodeConfig>,
    /// This node's own address, reported for the synthesized self node.
    pub self_ip: String,
    /// This node's own client ports for the same self node (`None` = transport
    /// disabled).
    pub self_ports: TransportPorts,
}

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
        }
    }

    /// Build the [`ClusterMetadata`] view. With a configured, non-empty roster
    /// emit one node per entry, marking the node at `primary_index` the leader
    /// and the rest followers; `None` (no on-shard consensus) leaves every node
    /// a follower. Otherwise synthesize the single self node as the sole
    /// leader.
    pub fn cluster_metadata(&self, primary_index: Option<u8>) -> ClusterMetadata {
        if self.enabled && !self.nodes.is_empty() {
            let nodes = self
                .nodes
                .iter()
                .map(|node| ClusterNode {
                    name: node.name.clone(),
                    ip: node.ip.clone(),
                    endpoints: ports_to_endpoints(&node.ports),
                    role: role_for(primary_index, node.replica_id),
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
