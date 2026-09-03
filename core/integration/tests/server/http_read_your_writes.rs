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

//! Read-your-writes over the REST listener, end to end: an unqualified read
//! must never answer below the metadata op the same caller was already told
//! committed.
//!
//! The window is a node that HANDED OUT a committed op it has not applied yet.
//! On the REST plane that node is a follower running a `Register`: the session
//! its first authenticated request mints is forwarded to the primary
//! (`dispatch::submit_register_local_or_forward`), so the follower answers with
//! an epoch its own commit walk can still be behind. Everything the metadata
//! group committed below that epoch is therefore state the caller has been
//! promised and this follower may not have applied.
//!
//! Each round seeds a stream through the primary over TCP, then authenticates
//! on the follower, which binds an epoch above that create. The follower's next
//! read is the assertion: it must not answer from before the stream existed.
//! Logout is the authenticated request that binds it, deliberately - it tears
//! the session entry down again, so the floor the read waits on has to outlive
//! the session that established it.
//!
//! The seeding runs over TCP rather than the primary's own REST listener so the
//! only HTTP sessions in play are the follower's: a second long-lived REST
//! session would be competing for VSR client ids with the fresh register each
//! round mints, which is a different subject.
//!
//! The suite asserts the GUARANTEE, not the mechanism: whether the gate parked
//! is invisible from outside, and on a fast local cluster the follower often
//! applies within the same tick. A pre-write answer is unambiguous though - a
//! 404, or a list missing the stream, can only happen if the floor was never
//! recorded, was recorded under the wrong key, was dropped with the session, or
//! the wait was skipped. A 503 fails the assertions too, on purpose: that is
//! what the gate answers when the follower never catches up inside its budget.
//! The park, the wake and the expiry themselves are pinned deterministically
//! next to the gate, in `dispatch::reads` and `metadata::applied_frontier`.

use iggy::prelude::*;
use integration::iggy_harness;
use reqwest::StatusCode;
use serde_json::Value;

use crate::server::http_client::{
    HttpClient, leader_and_follower, node_url, until_primary_resolved,
};

/// Seed / read-back rounds. More than one because the lag is a race the test
/// cannot force: each round re-runs it with the follower's commit walk in a
/// different position relative to the epoch it just handed out.
const ROUNDS: u32 = 4;

/// The `name` of every stream in a `GET /streams` list body.
fn stream_names(body: &Value) -> Vec<String> {
    body.as_array()
        .expect("the stream list is a JSON array")
        .iter()
        .map(|stream| {
            stream["name"]
                .as_str()
                .expect("every stream carries a name")
                .to_owned()
        })
        .collect()
}

/// Three nodes, the smallest cluster with a quorum, one shard each so every
/// request is served by shard 0 where the metadata consensus lives. No
/// `http.jwt` secret and no `cluster.auth`: bearers are node-local and
/// follower-to-primary forwarding is off, so the follower answers its own
/// requests instead of relaying them (see `http_view_header`, which pins both
/// halves of that switch).
#[iggy_harness(cluster_nodes = 3, server(system.sharding.cpu_allocation = "0..1"))]
async fn given_a_follower_when_its_register_binds_a_committed_epoch_should_not_read_below_it(
    harness: &TestHarness,
) {
    let (leader, follower) = leader_and_follower(harness).await;
    let seeder = harness
        .root_client_for_node(leader)
        .await
        .expect("connect to the primary");

    for round in 0..ROUNDS {
        let stream = format!("read-your-writes-{round}");
        seeder
            .create_stream(&stream)
            .await
            .expect("the primary must commit the stream this round reads back");

        // Fresh bearer, then one authenticated request on the follower: it is
        // what forwards the `Register` and binds an epoch above the create.
        let http = HttpClient::login_root_no_redirect(node_url(harness, follower)).await;
        let logout = until_primary_resolved(|| http.delete("/users/logout")).await;
        assert_eq!(
            logout.status(),
            StatusCode::NO_CONTENT,
            "the follower must bind and end a forwarded session"
        );

        // The list read resolves nothing, so it cannot 404 its way into looking
        // correct: a stale answer here is a short list.
        let read = http.get("/streams").await;
        assert_eq!(read.status(), StatusCode::OK, "the stream list must serve");
        let names = stream_names(&read.json().await.expect("the stream list is JSON"));
        assert!(
            names.contains(&stream),
            "the follower listed streams from before the epoch it had just handed out: {names:?}"
        );

        // The entity read, where the stale answer is a 404 instead.
        let read = http.get(&format!("/streams/{stream}")).await;
        assert_eq!(
            read.status(),
            StatusCode::OK,
            "the follower answered a read below the epoch it had just handed out"
        );
        let body: Value = read.json().await.expect("stream details are JSON");
        assert_eq!(
            body["name"].as_str(),
            Some(stream.as_str()),
            "the read answered with another stream's state"
        );
    }
}
