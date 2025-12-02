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

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ClusterConfig {
    pub enabled: bool,
    pub name: String,
    pub node: NodeConfig,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct NodeConfig {
    pub current: CurrentNodeConfig,
    pub others: Vec<OtherNodeConfig>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct CurrentNodeConfig {
    pub name: String,
    pub ip: String,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct OtherNodeConfig {
    pub name: String,
    pub ip: String,
    pub ports: TransportPorts,
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct TransportPorts {
    pub tcp: Option<u16>,
    pub quic: Option<u16>,
    pub http: Option<u16>,
    pub websocket: Option<u16>,
}
