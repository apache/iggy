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

//! End-to-end HTTPS proof for the server-ng shard-0 REST listener. The TLS
//! accept pump ([`server_ng::http::tls`]) and the `start()` HTTPS branch
//! unit-test in isolation; this is the only path that drives a live rustls
//! client over the wire and proves the response was actually served over
//! HTTP/2, negotiated via ALPN. A failure here (h1 fallback, handshake
//! rejection, or a plaintext bind) is a real defect in that path, not a test
//! artifact.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use iggy::prelude::*;
use integration::harness::TestHarness;
use reqwest::{Certificate, StatusCode, Version};
use serde_json::json;
use tokio::time::sleep;

/// The harness readiness gate fires on the dumped runtime config, which the
/// server writes before the HTTPS listener finishes binding; a bounded
/// connect-retry loop bridges that gap.
const READY_TIMEOUT: Duration = Duration::from_secs(15);
const READY_RETRY_INTERVAL: Duration = Duration::from_millis(50);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Absolute path to a repo loopback cert asset. The spawned server's CWD is a
/// temp dir, so relative paths break; `CARGO_MANIFEST_DIR` is the integration
/// crate, whose sibling `../certs` holds the checked-in loopback material.
fn cert_asset(file: &str) -> PathBuf {
    std::fs::canonicalize(format!("{}/../certs/{file}", env!("CARGO_MANIFEST_DIR")))
        .unwrap_or_else(|error| panic!("canonicalize repo cert asset {file}: {error}"))
}

/// Boot iggy-server-ng with `[http.tls]` enabled against the repo loopback
/// cert, then prove a real HTTPS request is served over HTTP/2. Single node:
/// the HTTP listener is shard-0-only and cluster formation is irrelevant to
/// what we are proving, so a standalone node keeps the signal clean.
#[tokio::test]
#[serial_test::parallel]
async fn given_http_tls_enabled_when_pinging_should_serve_https_over_http2() {
    let mut harness = TestHarness::builder()
        .default_server()
        .cluster_nodes(1)
        .build()
        .expect("build TLS harness");

    let cert = cert_asset("iggy_cert.pem");
    let key = cert_asset("iggy_key.pem");
    harness
        .server_mut()
        .add_env("IGGY_HTTP_TLS_ENABLED", "true");
    harness
        .server_mut()
        .add_env("IGGY_HTTP_TLS_CERT_FILE", cert.display().to_string());
    harness
        .server_mut()
        .add_env("IGGY_HTTP_TLS_KEY_FILE", key.display().to_string());

    harness
        .start()
        .await
        .expect("start server-ng with HTTPS enabled");

    let addr = harness
        .server()
        .http_addr()
        .expect("HTTP transport not configured on test server");
    // The loopback leaf's SAN is `DNS:localhost` only (no IP), so the client
    // must dial by hostname for hostname verification to pass.
    let base_url = format!("https://localhost:{}", addr.port());

    // Real chain + hostname verification: trust the repo CA that signed the
    // loopback leaf and let ALPN auto-negotiate (no prior-knowledge, which
    // would bypass ALPN). rustls is the only TLS backend compiled in.
    let ca =
        Certificate::from_pem(&std::fs::read(cert_asset("iggy_ca_cert.pem")).expect("read CA"))
            .expect("parse CA pem");
    let client = reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .use_rustls_tls()
        .add_root_certificate(ca)
        .build()
        .expect("build HTTPS client");

    // GET /ping, retrying only connect failures while the listener finishes
    // binding after readiness; any other error is a real handshake/protocol
    // defect and must surface.
    let deadline = Instant::now() + READY_TIMEOUT;
    let ping = loop {
        match client.get(format!("{base_url}/ping")).send().await {
            Ok(response) => break response,
            Err(error) if error.is_connect() => {
                assert!(
                    Instant::now() < deadline,
                    "HTTPS /ping never connected within {READY_TIMEOUT:?}: {error}"
                );
                sleep(READY_RETRY_INTERVAL).await;
            }
            Err(error) => panic!("HTTPS /ping failed (not a connect error): {error}"),
        }
    };
    assert_eq!(
        ping.status(),
        StatusCode::OK,
        "ping must answer 200 over HTTPS"
    );
    assert_eq!(
        ping.version(),
        Version::HTTP_2,
        "ALPN must negotiate HTTP/2 (server advertises h2 first), got {:?}",
        ping.version()
    );

    // A real request/response body over TLS, not just a static probe: root
    // login must round-trip a token.
    let login = client
        .post(format!("{base_url}/users/login"))
        .json(&json!({
            "username": DEFAULT_ROOT_USERNAME,
            "password": DEFAULT_ROOT_PASSWORD,
        }))
        .send()
        .await
        .expect("root login request over HTTPS");
    assert_eq!(
        login.status(),
        StatusCode::OK,
        "root login must answer 200 over HTTPS"
    );
    assert_eq!(
        login.version(),
        Version::HTTP_2,
        "login response must also be HTTP/2"
    );
    let identity: IdentityInfo = login.json().await.expect("decode IdentityInfo over HTTPS");
    assert!(
        identity
            .access_token
            .is_some_and(|token| !token.token.is_empty()),
        "login over HTTPS must return a non-empty access token"
    );
}
