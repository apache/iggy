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

use crate::shard::IggyShard;
use crate::streaming::utils::address::{extract_ip, extract_port};
use iggy_common::{
    ClusterMetadata, ClusterNode, ClusterNodeRole, ClusterNodeStatus, TransportEndpoints,
};
use tracing::trace;

impl IggyShard {
    pub fn get_cluster_metadata(&self) -> ClusterMetadata {
        let mut nodes = Vec::new();

        if !self.config.cluster.enabled {
            // Single-node: report ourselves as the sole leader; identity
            // fields fall back to transport addresses since no cluster list
            // is configured.
            let current_endpoints = self.get_actual_bound_ports().unwrap_or_else(|| {
                TransportEndpoints::new(
                    extract_port(&self.config.tcp.address),
                    extract_port(&self.config.quic.address),
                    extract_port(&self.config.http.address),
                    extract_port(&self.config.websocket.address),
                )
            });

            nodes.push(ClusterNode {
                name: "iggy-node".to_string(),
                ip: extract_ip(&self.config.tcp.address),
                endpoints: current_endpoints,
                role: ClusterNodeRole::Leader,
                status: ClusterNodeStatus::Healthy,
            });

            return ClusterMetadata {
                name: "single-node".to_string(),
                nodes,
            };
        }

        // Cluster mode: resolve the current node from the roster by the
        // runtime-supplied replica_id. Bootstrap already validated that
        // exactly one entry matches; missing `current_replica_id` here
        // would indicate a bootstrap bug, so fall back to a safe default
        // rather than panicking on the hot metadata path.
        let current_id = self.current_replica_id;
        let current_node = current_id
            .and_then(|id| {
                self.config
                    .cluster
                    .nodes
                    .iter()
                    .find(|node| node.replica_id == id)
            })
            .or_else(|| self.config.cluster.nodes.first());

        let Some(current_node) = current_node else {
            // Nodes list is empty even though cluster.enabled=true. Validator
            // refuses this state at startup; treat defensively if reached.
            return ClusterMetadata {
                name: self.config.cluster.name.clone(),
                nodes,
            };
        };

        // Use the actual bound ports for the current node so tests that bind
        // to port 0 still report the OS-assigned port over the wire.
        let current_endpoints = self.get_actual_bound_ports().unwrap_or_else(|| {
            TransportEndpoints::new(
                current_node
                    .ports
                    .tcp
                    .unwrap_or_else(|| extract_port(&self.config.tcp.address)),
                current_node
                    .ports
                    .quic
                    .unwrap_or_else(|| extract_port(&self.config.quic.address)),
                current_node
                    .ports
                    .http
                    .unwrap_or_else(|| extract_port(&self.config.http.address)),
                current_node
                    .ports
                    .websocket
                    .unwrap_or_else(|| extract_port(&self.config.websocket.address)),
            )
        });

        nodes.push(ClusterNode {
            name: current_node.name.clone(),
            ip: current_node.ip.clone(),
            endpoints: current_endpoints,
            role: if self.is_follower {
                ClusterNodeRole::Follower
            } else {
                ClusterNodeRole::Leader
            },
            status: ClusterNodeStatus::Healthy,
        });

        for peer in self
            .config
            .cluster
            .nodes
            .iter()
            .filter(|node| node.replica_id != current_node.replica_id)
        {
            let endpoints = TransportEndpoints::new(
                peer.ports
                    .tcp
                    .unwrap_or_else(|| extract_port(&self.config.tcp.address)),
                peer.ports
                    .quic
                    .unwrap_or_else(|| extract_port(&self.config.quic.address)),
                peer.ports
                    .http
                    .unwrap_or_else(|| extract_port(&self.config.http.address)),
                peer.ports
                    .websocket
                    .unwrap_or_else(|| extract_port(&self.config.websocket.address)),
            );

            nodes.push(ClusterNode {
                name: peer.name.clone(),
                ip: peer.ip.clone(),
                endpoints,
                role: if self.is_follower {
                    ClusterNodeRole::Leader
                } else {
                    ClusterNodeRole::Follower
                },
                status: ClusterNodeStatus::Healthy,
            });
        }

        ClusterMetadata {
            name: self.config.cluster.name.clone(),
            nodes,
        }
    }

    /// Get actual bound ports from the shard's bound addresses
    /// This is needed when server binds to port 0 (OS-assigned port)
    fn get_actual_bound_ports(&self) -> Option<TransportEndpoints> {
        let tcp_port = self
            .tcp_bound_address
            .get()
            .map(|addr| addr.port())
            .unwrap_or_else(|| extract_port(&self.config.tcp.address));

        let quic_port = self
            .quic_bound_address
            .get()
            .map(|addr| addr.port())
            .unwrap_or_else(|| extract_port(&self.config.quic.address));

        let http_port = self
            .http_bound_address
            .get()
            .map(|addr| addr.port())
            .unwrap_or_else(|| extract_port(&self.config.http.address));

        let websocket_port = self
            .websocket_bound_address
            .get()
            .map(|addr| addr.port())
            .unwrap_or_else(|| extract_port(&self.config.websocket.address));

        trace!(
            "Using actual bound ports - TCP: {}, QUIC: {}, HTTP: {}, WebSocket: {}",
            tcp_port, quic_port, http_port, websocket_port
        );

        Some(TransportEndpoints::new(
            tcp_port,
            quic_port,
            http_port,
            websocket_port,
        ))
    }
}
