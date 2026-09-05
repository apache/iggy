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

use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture};
use ring::hmac;
use std::collections::HashMap;
use std::net::TcpListener;

/// Both instances in `tests/connectors/http/source_config` share one listener,
/// so they must be handed the same pair of addresses.
const ENV_GITHUB_LISTEN_ADDR: &str = "IGGY_CONNECTORS_SOURCE_HTTP_GITHUB_PLUGIN_CONFIG_LISTEN_ADDR";
const ENV_GITHUB_ADMIN_LISTEN_ADDR: &str =
    "IGGY_CONNECTORS_SOURCE_HTTP_GITHUB_PLUGIN_CONFIG_ADMIN_LISTEN_ADDR";
const ENV_PARTNER_LISTEN_ADDR: &str =
    "IGGY_CONNECTORS_SOURCE_HTTP_PARTNER_PLUGIN_CONFIG_LISTEN_ADDR";
const ENV_PARTNER_ADMIN_LISTEN_ADDR: &str =
    "IGGY_CONNECTORS_SOURCE_HTTP_PARTNER_PLUGIN_CONFIG_ADMIN_LISTEN_ADDR";

/// Static endpoints declared in those configurations.
pub const GITHUB_ENDPOINT_ID: &str = "a3f8c2e1b9d04f7a8e6c1d2b3a4f5e6d";
pub const GITHUB_HMAC_SECRET: &str = "whsec_github_test";
pub const GITHUB_HMAC_HEADER: &str = "X-Hub-Signature-256";
pub const PARTNER_ENDPOINT_ID: &str = "0b7d9e2f4a6c8e1d3b5f7a9c2e4d6f81";
pub const PARTNER_BEARER_TOKEN: &str = "partner-token-test";
pub const MANAGEMENT_TOKEN: &str = "mgmt-token-test";
pub const GITHUB_INSTANCE: &str = "http_github";
pub const PARTNER_INSTANCE: &str = "http_partner";

/// Boots the webhook gateway on ports nobody else in this test run holds.
///
/// Unlike the container-backed fixtures there is no external system here: the
/// connector is itself the HTTP server, and the test's own client is the
/// webhook sender.
pub struct HttpSourceFixture {
    public_addr: String,
    admin_addr: String,
}

impl HttpSourceFixture {
    pub fn public_url(&self) -> String {
        format!("http://{}", self.public_addr)
    }

    pub fn admin_url(&self) -> String {
        format!("http://{}", self.admin_addr)
    }

    pub fn webhook_url(&self, endpoint_id: &str) -> String {
        format!("{}/e/{endpoint_id}", self.public_url())
    }

    /// The named path, which is the other half of the routing surface and the
    /// one a sender reaches without a per-endpoint secret.
    pub fn named_url(&self, topic_path: &str) -> String {
        format!("{}/topics/{topic_path}", self.public_url())
    }

    /// GitHub-style `sha256=<hex>` over the exact bytes that will be sent.
    pub fn github_signature(&self, body: &[u8]) -> String {
        let key = hmac::Key::new(hmac::HMAC_SHA256, GITHUB_HMAC_SECRET.as_bytes());
        format!("sha256={}", hex::encode(hmac::sign(&key, body).as_ref()))
    }

    /// Reserves both ephemeral ports, holding each listener until both are
    /// chosen, then releasing them so the connector can bind them.
    ///
    /// A fixed port would collide as soon as two of these tests run at once.
    /// Taking them one at a time let the first be handed out again while the
    /// second was still being picked; holding both closes that. The window
    /// between releasing them here and the connector binding stays open, and
    /// nothing inside this process can close it.
    fn reserve_port_pair() -> Result<(u16, u16), TestBinaryError> {
        let public = TcpListener::bind("127.0.0.1:0").map_err(TestBinaryError::Io)?;
        let admin = TcpListener::bind("127.0.0.1:0").map_err(TestBinaryError::Io)?;
        let public_port = public.local_addr().map_err(TestBinaryError::Io)?.port();
        let admin_port = admin.local_addr().map_err(TestBinaryError::Io)?.port();
        Ok((public_port, admin_port))
    }
}

#[async_trait]
impl TestFixture for HttpSourceFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let (public_port, admin_port) = Self::reserve_port_pair()?;
        let public_addr = format!("127.0.0.1:{public_port}");
        let admin_addr = format!("127.0.0.1:{admin_port}");
        Ok(Self {
            public_addr,
            admin_addr,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        HashMap::from([
            (ENV_GITHUB_LISTEN_ADDR.to_string(), self.public_addr.clone()),
            (
                ENV_GITHUB_ADMIN_LISTEN_ADDR.to_string(),
                self.admin_addr.clone(),
            ),
            (
                ENV_PARTNER_LISTEN_ADDR.to_string(),
                self.public_addr.clone(),
            ),
            (
                ENV_PARTNER_ADMIN_LISTEN_ADDR.to_string(),
                self.admin_addr.clone(),
            ),
        ])
    }
}
