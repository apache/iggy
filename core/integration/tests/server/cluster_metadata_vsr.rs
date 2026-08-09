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
//! roles - not a single synthesized self node with zeroed ports. The
//! selector suite below additionally proves per-client-network advertised
//! addresses (`cluster.nodes.advertised_addresses`) end to end: a loopback
//! client must see the selector address in binary metadata, HTTP metadata,
//! and the 307 leader-redirect `Location` - never the decoy selector on a
//! foreign network. Every selector here advertises `localhost`: it is the
//! one hostname the leader-aware SDK client's address comparison rewrites to
//! `127.0.0.1`, so any other value makes that client redial the advertised
//! host on every fresh connect - including the harness readiness probe,
//! which would wedge startup on an unresolvable name. A non-`localhost`
//! hostname selector therefore cannot run end to end until the SDK-side
//! comparison is fixed; that flow is pinned at unit level instead
//! (`cluster_meta.rs`, `cluster.rs`).

use std::time::Instant;

use iggy::prelude::*;
use integration::harness::TestHarness;
use integration::iggy_harness;
use reqwest::StatusCode;
use tokio::time::sleep;

use crate::server::http_client::{HttpClient, LOGIN_RETRY_INTERVAL, LOGIN_TIMEOUT};

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

const SELECTOR_CLUSTER_NODES: usize = 2;

/// Every test client in this file connects over loopback, so this CIDR is the
/// one the server must match its peer address against.
const LOOPBACK_CIDR: &str = "127.0.0.0/8";

/// Selector marker: distinct from the harness roster ip (`127.0.0.1`), so it
/// appears in metadata or a redirect only when the selector path ran - yet it
/// still resolves, which matters because the leader-aware SDK client redials
/// whatever address metadata advertises. Note `localhost` is also the one
/// hostname that client's `is_same_address` check rewrites to `127.0.0.1`,
/// so binary-transport tests must use it; the raw-hostname flow is pinned by
/// the HTTP-only [`HOSTNAME_SELECTOR_ADDRESS`] test instead.
const SELECTOR_ADDRESS: &str = "localhost";

/// Decoy selector on a network no test client connects from: its address in
/// metadata would mean CIDR matching is broken (matching everything, or
/// matching the server's own bind address instead of the peer), not that
/// selector precedence works.
const DECOY_CIDR: &str = "10.0.0.0/8";
const DECOY_ADDRESS: &str = "203.0.113.99";

/// A 2-node cluster whose roster gives every node a loopback-CIDR selector
/// plus the decoy selector that must never match. The roster (selectors
/// included) must be identical on every node, so each server process gets
/// the env vars for all nodes.
fn selector_cluster() -> TestHarness {
    let mut harness = TestHarness::builder()
        .default_server()
        .cluster_nodes(SELECTOR_CLUSTER_NODES)
        .build()
        .expect("build selector cluster harness");
    for node in 0..SELECTOR_CLUSTER_NODES {
        for roster_entry in 0..SELECTOR_CLUSTER_NODES {
            for (suffix, value) in [
                ("0_CLIENT_CIDR", LOOPBACK_CIDR),
                ("0_ADDRESS", SELECTOR_ADDRESS),
                ("1_CLIENT_CIDR", DECOY_CIDR),
                ("1_ADDRESS", DECOY_ADDRESS),
            ] {
                harness.node_mut(node).add_env(
                    format!("IGGY_CLUSTER_NODES_{roster_entry}_ADVERTISED_ADDRESSES_{suffix}"),
                    value,
                );
            }
        }
    }
    harness
}

/// Assert every roster node reports `expected_address` to this loopback
/// client; without the selector the same roster publishes `127.0.0.1`, and
/// broken CIDR matching would surface the decoy instead.
fn assert_advertised_addresses(
    nodes: impl IntoIterator<Item = (String, String)>,
    expected_address: &str,
) {
    let mut seen = 0;
    for (name, ip) in nodes {
        assert_eq!(
            ip, expected_address,
            "node '{name}' must advertise its loopback selector address to a loopback client"
        );
        seen += 1;
    }
    assert_eq!(seen, SELECTOR_CLUSTER_NODES, "full roster must be reported");
}

/// Extract `(name, ip)` per node from a `/cluster/metadata` HTTP reply.
async fn http_metadata_nodes(http: &HttpClient) -> Vec<(String, String)> {
    let response = http.get("/cluster/metadata").await;
    assert_eq!(response.status(), StatusCode::OK);
    let metadata: serde_json::Value = response.json().await.expect("decode cluster metadata");
    metadata["nodes"]
        .as_array()
        .expect("metadata must carry a nodes array")
        .iter()
        .map(|node| {
            (
                node["name"].as_str().expect("node name").to_owned(),
                node["ip"].as_str().expect("node ip").to_owned(),
            )
        })
        .collect()
}

#[tokio::test]
#[serial_test::parallel]
async fn given_matching_client_cidr_when_getting_binary_cluster_metadata_should_return_selector_addresses()
 {
    let mut harness = selector_cluster();
    harness.start().await.expect("start selector cluster");

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

    assert_advertised_addresses(
        metadata
            .nodes
            .iter()
            .map(|node| (node.name.clone(), node.ip.clone())),
        SELECTOR_ADDRESS,
    );
}

#[tokio::test]
#[serial_test::parallel]
async fn given_matching_client_cidr_when_getting_http_cluster_metadata_should_return_selector_addresses()
 {
    let mut harness = selector_cluster();
    harness.start().await.expect("start selector cluster");

    let http = HttpClient::login_root(&harness).await;
    assert_advertised_addresses(http_metadata_nodes(&http).await, SELECTOR_ADDRESS);
}

/// A linearizable read on the follower must 307 to the primary, and the
/// `Location` host must be the primary's SELECTOR address: the redirected
/// client is the same loopback peer, so pointing it at the catch-all (or the
/// roster ip) would route it off its network.
#[tokio::test]
#[serial_test::parallel]
async fn given_matching_client_cidr_when_redirected_to_primary_should_target_selector_address() {
    const READ_PATH: &str = "/streams?consistency=linearizable";

    let mut harness = selector_cluster();
    harness.start().await.expect("start selector cluster");

    // Redirects must surface, not be followed: the `Location` itself is what
    // is under test, and the selector hostname resolves by design (see
    // SELECTOR_ADDRESS), so a redirect-following client would chase it to the
    // primary, get a 200, and hide a wrong Location host. Bearers are
    // node-local in this keyless cluster, so each node gets its own login -
    // once, before the settle loop: every login commits a Register through
    // metadata consensus, so logging in per round would race the election the
    // loop waits out, and login retries would eat the shared settle budget.
    let mut sessions = Vec::with_capacity(SELECTOR_CLUSTER_NODES);
    for node in 0..SELECTOR_CLUSTER_NODES {
        let addr = harness.node(node).http_addr().expect("node http address");
        sessions.push(HttpClient::login_root_no_redirect(format!("http://{addr}")).await);
    }

    let deadline = Instant::now() + LOGIN_TIMEOUT;
    let mut verdicts: Vec<(usize, StatusCode, Option<String>)>;
    loop {
        verdicts = Vec::new();
        for (node, session) in sessions.iter().enumerate() {
            let response = session.get(READ_PATH).await;
            let location = response
                .headers()
                .get(reqwest::header::LOCATION)
                .map(|value| value.to_str().expect("Location header").to_owned());
            verdicts.push((node, response.status(), location));
        }
        // Settled view: exactly one primary serves the read locally and every
        // other node redirects. Anything else (a 503 follower that cannot
        // resolve the primary yet, a double-307 round mid-election) means the
        // view is still settling; retry within the shared warmup budget.
        let primary_count = verdicts
            .iter()
            .filter(|(_, status, _)| *status == StatusCode::OK)
            .count();
        let redirect_count = verdicts
            .iter()
            .filter(|(_, status, _)| *status == StatusCode::TEMPORARY_REDIRECT)
            .count();
        if primary_count == 1 && redirect_count == SELECTOR_CLUSTER_NODES - 1 {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "cluster did not settle on one primary and redirecting followers within {LOGIN_TIMEOUT:?}: {verdicts:?}"
        );
        sleep(LOGIN_RETRY_INTERVAL).await;
    }

    // The loop only breaks on one 200 with every other node at 307, so the
    // non-redirected partition is exactly the primary.
    let (redirected, primaries): (Vec<_>, Vec<_>) = verdicts
        .iter()
        .partition(|(_, status, _)| *status == StatusCode::TEMPORARY_REDIRECT);
    let (primary_index, _, _) = primaries[0];

    let primary_http_port = harness
        .node(*primary_index)
        .http_addr()
        .expect("primary http address")
        .port();
    let expected_location = format!("http://{SELECTOR_ADDRESS}:{primary_http_port}{READ_PATH}");
    for (follower, _, location) in redirected {
        assert_eq!(
            location.as_deref(),
            Some(expected_location.as_str()),
            "follower node {follower} must redirect this loopback client to the primary's selector address"
        );
    }
}
