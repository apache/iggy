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

        // Determine cluster name and current node name based on whether clustering is enabled
        let (cluster_name, current_node_name, current_ip) = if self.config.cluster.enabled {
            (
                self.config.cluster.name.clone(),
                self.config.cluster.node.current.name.clone(),
                self.config.cluster.node.current.ip.clone(),
            )
        } else {
            (
                "single-node".to_string(),
                "iggy-node".to_string(),
                extract_ip(&self.config.tcp.address),
            )
        };

        // Get the actual bound ports from the shard's bound addresses
        let current_endpoints = self.get_actual_bound_ports().unwrap_or_else(|| {
            TransportEndpoints::new(
                extract_port(&self.config.tcp.address),
                extract_port(&self.config.quic.address),
                extract_port(&self.config.http.address),
                extract_port(&self.config.websocket.address),
            )
        });

        // Add current node with appropriate role
        nodes.push(ClusterNode {
            name: current_node_name.clone(),
            ip: current_ip,
            endpoints: current_endpoints,
            role: if self.config.cluster.enabled && self.is_follower {
                ClusterNodeRole::Follower
            } else {
                // Always leader when clustering is disabled or when not a follower
                ClusterNodeRole::Leader
            },
            status: ClusterNodeStatus::Healthy,
        });

        // Only add other nodes if clustering is enabled
        if self.config.cluster.enabled {
            for other_node in &self.config.cluster.node.others {
                let endpoints = TransportEndpoints::new(
                    other_node
                        .ports
                        .tcp
                        .unwrap_or_else(|| extract_port(&self.config.tcp.address)),
                    other_node
                        .ports
                        .quic
                        .unwrap_or_else(|| extract_port(&self.config.quic.address)),
                    other_node
                        .ports
                        .http
                        .unwrap_or_else(|| extract_port(&self.config.http.address)),
                    other_node
                        .ports
                        .websocket
                        .unwrap_or_else(|| extract_port(&self.config.websocket.address)),
                );

                nodes.push(ClusterNode {
                    name: other_node.name.clone(),
                    ip: other_node.ip.clone(),
                    endpoints,
                    role: if self.is_follower {
                        ClusterNodeRole::Leader
                    } else {
                        ClusterNodeRole::Follower
                    },
                    status: ClusterNodeStatus::Healthy,
                });
            }
        }

        ClusterMetadata {
            name: cluster_name,
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
