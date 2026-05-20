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

use crate::types::cluster::{
    role::ClusterNodeRole, status::ClusterNodeStatus, transport_endpoints::TransportEndpoints,
};
use serde::{Deserialize, Serialize};
use std::fmt::Display;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct ClusterNode {
    pub name: String,
    pub ip: String,
    pub endpoints: TransportEndpoints,
    pub role: ClusterNodeRole,
    pub status: ClusterNodeStatus,
}

impl Display for ClusterNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ClusterNode {{ name: {}, ip: {}, endpoints: {}, role: {}, status: {} }}",
            self.name, self.ip, self.endpoints, self.role, self.status
        )
    }
}
