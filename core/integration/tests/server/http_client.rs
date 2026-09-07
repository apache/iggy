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

//! Shared HTTP transport plumbing for the server REST suites (`http_vsr`,
//! `http_rbac`): one authenticated `reqwest` session with the login-retry gate,
//! the generic verb helpers, and the cluster-shaped helpers the multi-node
//! suites share (which node is the leader, which is a follower, and the retry
//! a follower needs before it can resolve the primary). Each suite keeps its
//! own request shapes and assertions as extension methods on [`HttpClient`], so
//! the wire-contract and listener-behavior separation between the suites stays
//! intact.

use std::future::Future;
use std::time::{Duration, Instant};

use iggy::prelude::*;
use integration::harness::TestHarness;
use reqwest::{Response, StatusCode};
use serde_json::{Value, json};
use tokio::time::sleep;

/// The harness readiness gate waits for the dumped runtime config, which the
/// server writes before binding the HTTP listener; a bounded login retry
/// bridges that gap plus single-node consensus warmup.
pub const LOGIN_TIMEOUT: Duration = Duration::from_secs(15);
pub const LOGIN_RETRY_INTERVAL: Duration = Duration::from_millis(50);
pub const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// One authenticated HTTP session against the test server's shard-0 listener: a
/// `reqwest` client, the listener base URL, and the bearer to send. The bearer
/// is either a login JWT or a raw personal access token (the server resolves
/// either on the `Authorization: Bearer` header).
pub struct HttpClient {
    pub client: reqwest::Client,
    pub base_url: String,
    pub token: String,
}

impl HttpClient {
    /// Log in as root, retrying while the listener binds and consensus warms up.
    pub async fn login_root(harness: &TestHarness) -> Self {
        let addr = harness
            .server()
            .http_addr()
            .expect("HTTP transport not configured on test server");
        let client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .expect("build reqwest client");
        Self::login_root_with(client, format!("http://{addr}")).await
    }

    /// Log in as root against an explicit listener base URL, with redirects
    /// surfaced instead of followed - for suites that assert on `Location`
    /// (reqwest follows a 307 transparently by default). The explicit URL also
    /// reaches a follower node, which [`Self::login_root`] (pinned to node 0)
    /// cannot.
    pub async fn login_root_no_redirect(base_url: String) -> Self {
        let client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("build reqwest client");
        Self::login_root_with(client, base_url).await
    }

    /// Shared root-login retry loop over an arbitrary client + base URL.
    async fn login_root_with(client: reqwest::Client, base_url: String) -> Self {
        let body = json!({
            "username": DEFAULT_ROOT_USERNAME,
            "password": DEFAULT_ROOT_PASSWORD,
        });
        let deadline = Instant::now() + LOGIN_TIMEOUT;
        loop {
            let attempt = client
                .post(format!("{base_url}/users/login"))
                .json(&body)
                .send()
                .await;
            match attempt {
                Ok(response) if response.status().is_success() => {
                    return Self {
                        client,
                        base_url,
                        token: access_token(response).await,
                    };
                }
                // Listener not bound yet or consensus still warming up; any
                // non-5xx rejection (e.g. 401) is a real failure.
                Ok(response) if response.status().is_server_error() => {}
                Ok(response) => panic!("root login rejected: {}", response.status()),
                Err(error) if error.is_connect() => {}
                Err(error) => panic!("root login request failed: {error}"),
            }
            assert!(
                Instant::now() < deadline,
                "HTTP root login did not succeed within {LOGIN_TIMEOUT:?}"
            );
            sleep(LOGIN_RETRY_INTERVAL).await;
        }
    }

    /// Log in as an already-created user; single attempt (the server is warm
    /// once root exists). Reuses this session's client and base URL.
    pub async fn login(&self, username: &str, password: &str) -> Self {
        let body = json!({ "username": username, "password": password });
        let response = self
            .client
            .post(self.url("/users/login"))
            .json(&body)
            .send()
            .await
            .expect("login request");
        assert!(
            response.status().is_success(),
            "login for {username} failed: {}",
            response.status()
        );
        Self {
            client: self.client.clone(),
            base_url: self.base_url.clone(),
            token: access_token(response).await,
        }
    }

    /// A view of this session bound to a different bearer (a refreshed JWT or a
    /// PAT), reusing the same client and base URL.
    pub fn with_token(&self, token: String) -> Self {
        Self {
            client: self.client.clone(),
            base_url: self.base_url.clone(),
            token,
        }
    }

    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    pub async fn get(&self, path: &str) -> Response {
        self.client
            .get(self.url(path))
            .bearer_auth(&self.token)
            .send()
            .await
            .expect("get request")
    }

    /// Unauthenticated GET (no bearer): the public `/ping` probe and 401
    /// assertions on protected routes.
    pub async fn get_anonymous(&self, path: &str) -> Response {
        self.client
            .get(self.url(path))
            .send()
            .await
            .expect("anonymous get request")
    }

    pub async fn post_json(&self, path: &str, body: &Value) -> Response {
        self.client
            .post(self.url(path))
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .await
            .expect("post request")
    }

    pub async fn put_json(&self, path: &str, body: &Value) -> Response {
        self.client
            .put(self.url(path))
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .await
            .expect("put request")
    }

    pub async fn delete(&self, path: &str) -> Response {
        self.client
            .delete(self.url(path))
            .bearer_auth(&self.token)
            .send()
            .await
            .expect("delete request")
    }
}

/// `http://host:port` of a harness node's HTTP listener.
pub fn node_url(harness: &TestHarness, node: usize) -> String {
    let addr = harness.node(node).http_addr().expect("node http address");
    format!("http://{addr}")
}

/// Harness indexes of the node the roster marks `Leader` and of one it marks
/// `Follower`. The harness emits the roster in node order, so a roster
/// position is a harness index. Every node reads `Follower` until shard 0
/// publishes its first view, so the roster is polled within the shared
/// warmup budget until it marks a leader.
pub async fn leader_and_follower(harness: &TestHarness) -> (usize, usize) {
    let client = harness
        .root_client_for_node(0)
        .await
        .expect("connect to node 0");
    let deadline = Instant::now() + LOGIN_TIMEOUT;
    loop {
        let metadata = client
            .get_cluster_metadata()
            .await
            .expect("get cluster metadata");
        let position =
            |role: ClusterNodeRole| metadata.nodes.iter().position(|node| node.role == role);
        if let (Some(leader), Some(follower)) = (
            position(ClusterNodeRole::Leader),
            position(ClusterNodeRole::Follower),
        ) {
            return (leader, follower);
        }
        assert!(
            Instant::now() < deadline,
            "the roster did not mark a leader within {LOGIN_TIMEOUT:?}, got {metadata}"
        );
        sleep(LOGIN_RETRY_INTERVAL).await;
    }
}

/// Repeat `request` while the follower answers 503, which it does until it
/// can resolve the primary from its own view; bounded by the shared warmup
/// budget, as cluster_metadata_vsr does. A 503 is the retry-safe class: the
/// request provably never entered a pipeline.
pub async fn until_primary_resolved<F, Fut>(request: F) -> Response
where
    F: Fn() -> Fut,
    Fut: Future<Output = Response>,
{
    let deadline = Instant::now() + LOGIN_TIMEOUT;
    loop {
        let response = request().await;
        if response.status() != StatusCode::SERVICE_UNAVAILABLE {
            return response;
        }
        assert!(
            Instant::now() < deadline,
            "follower did not resolve the primary within {LOGIN_TIMEOUT:?}"
        );
        sleep(LOGIN_RETRY_INTERVAL).await;
    }
}

/// Extract the JWT from a successful login response.
pub async fn access_token(response: Response) -> String {
    let identity: IdentityInfo = response.json().await.expect("decode IdentityInfo");
    identity
        .access_token
        .expect("login must return an access token")
        .token
}
