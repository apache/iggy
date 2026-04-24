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

use configs::ConfigEnv;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
#[serde(deny_unknown_fields)]
pub struct ClusterConfig {
    pub enabled: bool,
    pub name: String,
    /// Full roster of cluster members. Intended to be byte-identical across
    /// every node so operators ship one config. The running node's identity
    /// is supplied out-of-band via the `--replica-id` CLI flag, which
    /// selects the entry in this list that describes the current node.
    #[serde(default)]
    pub nodes: Vec<ClusterNodeConfig>,
}

#[derive(Debug, Deserialize, Serialize, Clone, ConfigEnv)]
pub struct ClusterNodeConfig {
    pub name: String,
    pub ip: String,
    /// Numeric replica ID for VSR consensus (0-based).
    ///
    /// Must be unique across [`ClusterConfig::nodes`] and strictly less than
    /// `nodes.len()`. Validated by [`ClusterConfig::validate`].
    pub replica_id: u8,
    pub ports: TransportPorts,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default, ConfigEnv)]
pub struct TransportPorts {
    pub tcp: Option<u16>,
    pub quic: Option<u16>,
    pub http: Option<u16>,
    pub websocket: Option<u16>,
    /// Dedicated port for replica-to-replica consensus traffic.
    pub tcp_replica: Option<u16>,
}
