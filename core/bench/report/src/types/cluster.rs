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

use serde::{Deserialize, Serialize};

/// Role string that marks a node as the consensus leader; anything else counts as a follower.
const LEADER_ROLE: &str = "leader";

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct BenchmarkClusterInfo {
    /// Human-friendly cluster name
    pub name: String,
    /// Member nodes of the cluster
    pub nodes: Vec<BenchmarkClusterNode>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct BenchmarkClusterNode {
    /// Node name as reported by the roster
    pub name: String,
    /// Consensus role, e.g. "leader" or "follower"
    pub role: String,
    /// Lifecycle status, e.g. "healthy" or "starting"
    pub status: String,
}

impl BenchmarkClusterInfo {
    /// Human display label such as `3-node cluster (1 leader + 2 followers)`.
    pub fn label(&self) -> String {
        if self.nodes.is_empty() {
            return format!("cluster '{}'", self.name);
        }

        let leaders = self
            .nodes
            .iter()
            .filter(|node| node.role == LEADER_ROLE)
            .count();
        let followers = self.nodes.len() - leaders;

        format!(
            "{}-node cluster ({} + {})",
            self.nodes.len(),
            pluralize(leaders, "leader"),
            pluralize(followers, "follower"),
        )
    }
}

fn pluralize(count: usize, word: &str) -> String {
    if count == 1 {
        format!("{count} {word}")
    } else {
        format!("{count} {word}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(name: &str, role: &str) -> BenchmarkClusterNode {
        BenchmarkClusterNode {
            name: name.to_string(),
            role: role.to_string(),
            status: "healthy".to_string(),
        }
    }

    #[test]
    fn test_label_three_nodes_one_leader() {
        let cluster = BenchmarkClusterInfo {
            name: "vsr".to_string(),
            nodes: vec![
                node("node-1", "leader"),
                node("node-2", "follower"),
                node("node-3", "follower"),
            ],
        };
        assert_eq!(cluster.label(), "3-node cluster (1 leader + 2 followers)");
    }

    #[test]
    fn test_label_single_leader_node() {
        let cluster = BenchmarkClusterInfo {
            name: "solo".to_string(),
            nodes: vec![node("node-1", "leader")],
        };
        assert_eq!(cluster.label(), "1-node cluster (1 leader + 0 followers)");
    }

    #[test]
    fn test_label_empty_nodes_falls_back_to_name() {
        let cluster = BenchmarkClusterInfo {
            name: "unknown".to_string(),
            nodes: vec![],
        };
        assert_eq!(cluster.label(), "cluster 'unknown'");
    }

    #[test]
    fn test_cluster_info_round_trip() {
        let cluster = BenchmarkClusterInfo {
            name: "vsr".to_string(),
            nodes: vec![node("node-1", "leader"), node("node-2", "follower")],
        };
        let json = serde_json::to_string(&cluster).unwrap();
        let decoded: BenchmarkClusterInfo = serde_json::from_str(&json).unwrap();
        assert_eq!(cluster, decoded);
    }
}
