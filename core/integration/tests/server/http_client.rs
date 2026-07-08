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

//! Shared HTTP transport plumbing for the server-ng REST suites (`http_vsr`,
//! `http_rbac`): one authenticated `reqwest` session with the login-retry gate
//! and the generic verb helpers. Each suite keeps its own request shapes and
//! assertions as extension methods on [`HttpClient`], so the wire-contract and
//! listener-behavior separation between the suites stays intact.

use std::time::{Duration, Instant};

use iggy::prelude::*;
use integration::harness::TestHarness;
use reqwest::Response;
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
/// is either a login JWT or a raw personal access token (server-ng resolves
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
        let base_url = format!("http://{addr}");
        let client = reqwest::Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .build()
            .expect("build reqwest client");

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

    /// Unauthenticated GET (no bearer) for the public `/ping` probe.
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

/// Extract the JWT from a successful login response.
pub async fn access_token(response: Response) -> String {
    let identity: IdentityInfo = response.json().await.expect("decode IdentityInfo");
    identity
        .access_token
        .expect("login must return an access token")
        .token
}
