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

use crate::streaming::session::Session;
use crate::streaming::systems::system::System;
use iggy_common::IggyError;
use iggy_common::metadata::ClusterMetadata;
use iggy_common::node::ClusterNode;
use iggy_common::role::ClusterNodeRole;
use iggy_common::status::ClusterNodeStatus;
use tracing::trace;

impl System {
    pub fn get_cluster_metadata(&self, session: &Session) -> Result<ClusterMetadata, IggyError> {
        trace!("Getting cluster metadata for session: {session}");

        // TODO(hubcio): Clustering is not yet implemented
        // The leader/follower as well as node status are currently placeholder implementations.

        let nodes: Vec<ClusterNode> = self
            .cluster_config
            .nodes
            .iter()
            .map(|node_config| {
                let role = if node_config.id == 1 {
                    ClusterNodeRole::Leader
                } else {
                    ClusterNodeRole::Follower
                };

                let status = if self.cluster_config.enabled {
                    ClusterNodeStatus::Healthy
                } else {
                    ClusterNodeStatus::Stopping
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
            name: self.cluster_config.name.clone(),
            id: self.cluster_config.id,
            transport: self.cluster_config.transport.clone(),
            nodes,
        })
    }
}
