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

//! The `iggy-view` response header: the serving node's current VSR view,
//! stamped on the success and redirect responses of authenticated flows and
//! withheld wherever it could reach a caller that proved no credential. Raw
//! `reqwest`, because the header itself is the contract.

use integration::iggy_harness;
use reqwest::{Response, StatusCode};
use serde_json::json;

use crate::server::http_client::{
    HttpClient, leader_and_follower, node_url, until_primary_resolved,
};

const VIEW_HEADER: &str = "iggy-view";

/// The view number a response carries, if any. A header that is present but
/// not a view number is a contract violation, not an absence.
fn view_number(response: &Response) -> Option<u64> {
    response.headers().get(VIEW_HEADER).map(|value| {
        value
            .to_str()
            .expect("iggy-view must be ASCII")
            .parse()
            .expect("iggy-view must be a view number")
    })
}

#[iggy_harness(cluster_nodes = 1)]
async fn given_an_authenticated_request_when_it_succeeds_should_carry_the_iggy_view_header(
    harness: &TestHarness,
) {
    let http = HttpClient::login_root(harness).await;

    let response = http.get("/streams").await;

    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        view_number(&response).is_some(),
        "a successful authenticated response must carry {VIEW_HEADER}"
    );
}

#[iggy_harness(cluster_nodes = 1)]
async fn given_a_request_without_credentials_when_it_is_rejected_should_omit_the_iggy_view_header(
    harness: &TestHarness,
) {
    let http = HttpClient::login_root(harness).await;

    let response = http.get_anonymous("/streams").await;

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert!(
        view_number(&response).is_none(),
        "an error response must not leak {VIEW_HEADER} to an unauthenticated caller"
    );
}

#[iggy_harness(cluster_nodes = 1)]
async fn given_the_ping_route_when_it_succeeds_should_omit_the_iggy_view_header(
    harness: &TestHarness,
) {
    let http = HttpClient::login_root(harness).await;

    let response = http.get_anonymous("/ping").await;

    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        view_number(&response).is_none(),
        "the pre-auth probe must not carry {VIEW_HEADER}"
    );
}

/// The view the primary stamps on its own successful response: the value a
/// follower's redirect or relay must agree with.
async fn primary_view(primary: &HttpClient) -> u64 {
    view_number(&primary.get("/streams").await).expect("the primary stamps its own view")
}

/// Three nodes, the smallest cluster that keeps a quorum through a leader
/// change, one shard each so every request is served by shard 0, where the
/// metadata consensus lives (see cluster_metadata_vsr.rs). No `http.jwt`
/// secret and no `cluster.auth`: bearers are node-local, forwarding is off,
/// and a follower answers a linearizable read with the 307 primary redirect.
#[iggy_harness(cluster_nodes = 3, server(system.sharding.cpu_allocation = "0..1"))]
async fn given_a_follower_when_it_redirects_a_linearizable_read_should_carry_the_iggy_view_header(
    harness: &TestHarness,
) {
    let (leader, follower) = leader_and_follower(harness).await;
    let primary = HttpClient::login_root_no_redirect(node_url(harness, leader)).await;
    let http = HttpClient::login_root_no_redirect(node_url(harness, follower)).await;
    // Views only grow, so bracketing the request lets a view change in flight
    // widen the accepted range instead of failing the test.
    let view_before = primary_view(&primary).await;

    let response = until_primary_resolved(|| http.get("/streams?consistency=linearizable")).await;
    let view_after = primary_view(&primary).await;

    assert_eq!(
        response.status(),
        StatusCode::TEMPORARY_REDIRECT,
        "a keyless follower must redirect a linearizable read to the primary"
    );
    let view = view_number(&response);
    assert!(
        view.is_some_and(|view| (view_before..=view_after).contains(&view)),
        "the primary redirect must carry the cluster's current view in {VIEW_HEADER}: \
         got {view:?}, expected within {view_before}..={view_after}"
    );
}

/// The same three-node shape with cluster-wide bearer key material, which
/// switches follower-to-primary forwarding on: a control-plane write posted
/// to the follower is answered by the primary, and the relayed response must
/// carry the header the primary stamped.
#[iggy_harness(
    cluster_nodes = 3,
    server(
        system.sharding.cpu_allocation = "0..1",
        http.jwt.encoding_secret = "0123456789abcdef0123456789abcdef",
        http.jwt.decoding_secret = "0123456789abcdef0123456789abcdef"
    )
)]
async fn given_a_follower_when_it_relays_a_forwarded_write_should_carry_the_iggy_view_header(
    harness: &TestHarness,
) {
    let (leader, follower) = leader_and_follower(harness).await;
    let primary = HttpClient::login_root_no_redirect(node_url(harness, leader)).await;
    let http = HttpClient::login_root_no_redirect(node_url(harness, follower)).await;
    let body = json!({ "name": "forwarded-stream" });
    let view_before = primary_view(&primary).await;

    let response = until_primary_resolved(|| http.post_json("/streams", &body)).await;
    let view_after = primary_view(&primary).await;

    assert_eq!(
        response.status(),
        StatusCode::OK,
        "the follower must relay the primary's answer to a control-plane write"
    );
    let view = view_number(&response);
    assert!(
        view.is_some_and(|view| (view_before..=view_after).contains(&view)),
        "a relayed response must carry the view the primary stamped in {VIEW_HEADER}: \
         got {view:?}, expected within {view_before}..={view_after}"
    );
}
