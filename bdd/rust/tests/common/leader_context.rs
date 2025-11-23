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

use cucumber::World;
use iggy::clients::client::IggyClient;
use iggy::prelude::ClusterNode;
use std::collections::HashMap;

#[derive(Debug, World, Default)]
pub struct LeaderContext {
    // Clients by name (e.g., "main", "A", "B")
    pub clients: HashMap<String, IggyClient>,

    // Server configurations
    pub servers: ServerConfigs,

    // Cluster state
    pub cluster: ClusterState,

    // Test state tracking
    pub test_state: TestState,
}

#[derive(Debug, Default)]
pub struct ServerConfigs {
    // Server addresses by role (e.g., "leader" -> "iggy-leader:8091")
    pub addresses: HashMap<String, String>,
}

#[derive(Debug, Default)]
pub struct ClusterState {
    pub enabled: bool,
    pub nodes: Vec<ClusterNode>,
}

#[derive(Debug, Default)]
pub struct TestState {
    pub redirection_occurred: bool,
    pub last_stream_id: Option<u32>,
    pub last_stream_name: Option<String>,
}

impl LeaderContext {
    /// Gets or creates a client by name
    pub fn get_client(&self, name: &str) -> Option<&IggyClient> {
        self.clients.get(name)
    }

    /// Stores a client by name
    pub fn store_client(&mut self, name: String, client: IggyClient) {
        self.clients.insert(name, client);
    }

    /// Stores a server address by role
    pub fn store_server_addr(&mut self, role: String, addr: String) {
        self.servers.addresses.insert(role, addr);
    }

    /// Gets a server address by role
    pub fn get_server_addr(&self, role: &str) -> Option<&String> {
        self.servers.addresses.get(role)
    }

    /// Adds a node to the cluster configuration
    pub fn add_node(&mut self, node: ClusterNode) {
        self.cluster.nodes.push(node);
    }
}
