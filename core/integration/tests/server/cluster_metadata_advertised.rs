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

//! `node.advertised_address` on a cluster-disabled server: what a client is
//! told about the one node in the roster.
//!
//! Without a roster the server has only its own bind address to reason from,
//! and a bind address answers which interfaces it accepts on - never where a
//! client reaches it. Behind any NAT (published container ports, a Service, a
//! load balancer) the two are different addresses, and this setting is the
//! only way to state the second one. The bind-derived fallback is pinned at
//! unit level (`cluster_meta.rs`); what needs a real server is that a declared
//! address survives config load and reaches the wire.

use iggy::prelude::*;
use integration::iggy_harness;

const ADVERTISED_ADDRESS: &str = "broker-1.example.com";

// The harness runs a VSR cluster by default, where the roster answers the
// client-facing address per node and this setting is deliberately ignored. One
// node with clustering off is the shape that reaches the self-synthesized
// path: `--replica-id 0` stays valid without a cluster, any higher id does not.
#[iggy_harness(
    cluster_nodes = 1,
    server(cluster.enabled = false, node.advertised_address = "broker-1.example.com")
)]
async fn given_a_declared_advertised_address_when_getting_cluster_metadata_should_publish_it(
    harness: &TestHarness,
) {
    let client = harness
        .node(0)
        .tcp_client()
        .expect("tcp client")
        .with_root_login()
        .connect()
        .await
        .expect("connect");

    let metadata = client
        .get_cluster_metadata()
        .await
        .expect("get cluster metadata");

    assert_eq!(
        metadata.nodes.len(),
        1,
        "a cluster-disabled server reports itself alone, got {metadata}"
    );
    assert_eq!(
        metadata.name, "single-node",
        "a cluster-disabled server reports the single-node label, got {metadata}"
    );
    let node = &metadata.nodes[0];
    // The harness binds a concrete loopback address, so this also pins the
    // precedence: a declaration outranks an address the bind could vouch for,
    // because only the declaration is a claim about reachability.
    assert_eq!(
        node.ip, ADVERTISED_ADDRESS,
        "the declared address must reach the wire verbatim"
    );
    assert_ne!(
        node.endpoints.tcp, 0,
        "the self node reports its real tcp port alongside the declared address"
    );
}
