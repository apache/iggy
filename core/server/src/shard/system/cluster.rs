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
use crate::streaming::session::Session;
use iggy_common::{ClusterMetadata, ClusterNode, ClusterNodeRole, ClusterNodeStatus, IggyError};
use tracing::trace;

impl IggyShard {
    pub fn get_cluster_metadata(&self, session: &Session) -> Result<ClusterMetadata, IggyError> {
        trace!("Getting cluster metadata for session: {session}");

        if !self.config.cluster.enabled {
            return Err(IggyError::FeatureUnavailable);
        }

        // TODO(hubcio): Clustering is not yet implemented
        // The leader/follower as well as node status are currently placeholder implementations.

        let cluster_name = self.config.cluster.name.clone();
        let cluster_id = self.config.cluster.id;

        // Cannot fail because we validated it in config
        let transport = self.config.cluster.transport;

        let own_node_id = self.config.cluster.node.id;

        let nodes: Vec<ClusterNode> = self
            .config
            .cluster
            .nodes
            .iter()
            .map(|node_config| {
                let (role, status) = if node_config.id == own_node_id {
                    (
                        if self.is_follower {
                            ClusterNodeRole::Follower
                        } else {
                            ClusterNodeRole::Leader
                        },
                        ClusterNodeStatus::Healthy,
                    )
                } else {
                    (
                        if self.is_follower {
                            ClusterNodeRole::Leader
                        } else {
                            ClusterNodeRole::Follower
                        },
                        ClusterNodeStatus::Healthy,
                    )
                };

                ClusterNode {
                    id: node_config.id,
                    name: node_config.name.clone(),
                    address: node_config.address.clone(),
                    role,
                    status,
                }
            })
            .collect();

        Ok(ClusterMetadata {
            name: cluster_name,
            id: cluster_id,
            transport,
            nodes,
        })
    }
}
