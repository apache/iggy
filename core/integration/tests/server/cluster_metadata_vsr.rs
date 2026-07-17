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

//! Binary `GetClusterMetadata` over a real VSR cluster. The reply must carry
//! the full configured roster with real client ports and live leader/follower
//! roles - not a single synthesized self node with zeroed ports.

use iggy::prelude::*;
use integration::iggy_harness;

/// One shard per node so the client connection is always served by shard 0,
/// where the metadata consensus that marks the leader lives. A request
/// round-robined to a peer shard would still get the full roster but with no
/// leader marked, since peer shards run no consensus.
#[iggy_harness(cluster_nodes = 2, server(system.sharding.cpu_allocation = "0..1"))]
async fn given_two_node_cluster_when_getting_cluster_metadata_should_return_full_roster(
    harness: &TestHarness,
) {
    let client = harness
        .node(0)
        .tcp_client()
        .expect("tcp client")
        .with_root_login()
        .connect()
        .await
        .expect("connect to node 0");

    let metadata = client
        .get_cluster_metadata()
        .await
        .expect("get cluster metadata");

    assert_eq!(
        metadata.nodes.len(),
        harness.cluster_size(),
        "every configured node must appear in the roster, got {metadata}"
    );
    for node in &metadata.nodes {
        assert_ne!(
            node.endpoints.tcp, 0,
            "node {} must report its real tcp port, not the stub's 0",
            node.name
        );
    }

    let leaders = metadata
        .nodes
        .iter()
        .filter(|node| node.role == ClusterNodeRole::Leader)
        .count();
    let followers = metadata
        .nodes
        .iter()
        .filter(|node| node.role == ClusterNodeRole::Follower)
        .count();
    assert_eq!(leaders, 1, "exactly one node must lead, got {metadata}");
    assert_eq!(
        followers,
        harness.cluster_size() - 1,
        "every other node must follow, got {metadata}"
    );
}
