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

//! End-to-end HTTPS proof for the server shard-0 REST listener. The TLS
//! accept pump ([`server::http::tls`]) and the `start()` HTTPS branch
//! unit-test in isolation; this is the only path that drives a live rustls
//! client over the wire and proves the response was actually served over
//! HTTP/2, negotiated via ALPN. A failure here (h1 fallback, handshake
//! rejection, or a plaintext bind) is a real defect in that path, not a test
//! artifact. The `/cluster/metadata` step additionally proves the TLS serve
//! loop's hand-stamped `ConnectInfo<ClientAddr>` reaches the extractors: the
//! plain listener gets that extension from axum's connect-info make-service,
//! the TLS loop stamps it itself, and only a client-IP-dependent response
//! can tell a missing stamp from a working one.

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

/// Selector marker distinct from the roster ip (`127.0.0.1`): it appears in
/// metadata only when the request carried the client's peer address, so it
/// distinguishes a working `ConnectInfo` stamp from the catch-all fallback a
/// missing stamp silently degrades to. `localhost` specifically, because the
/// cluster readiness probe logs in over the leader-aware binary client,
/// which redials any other advertised hostname and would wedge startup on an
/// unresolvable name.
const TLS_SELECTOR_ADDRESS: &str = "localhost";

/// Selectors covering the loopback client in both address families
/// (`localhost` may resolve to either).
const TLS_SELECTOR_ENVS: [(&str, &str); 4] = [
    ("0_CLIENT_CIDR", "127.0.0.0/8"),
    ("0_ADDRESS", TLS_SELECTOR_ADDRESS),
    ("1_CLIENT_CIDR", "::1/128"),
    ("1_ADDRESS", TLS_SELECTOR_ADDRESS),
];

const TLS_CLUSTER_NODES: usize = 2;

/// Absolute path to a repo loopback cert asset. The spawned server's CWD is a
/// temp dir, so relative paths break; `CARGO_MANIFEST_DIR` is the integration
/// crate, whose sibling `../certs` holds the checked-in loopback material.
fn cert_asset(file: &str) -> PathBuf {
    std::fs::canonicalize(format!("{}/../certs/{file}", env!("CARGO_MANIFEST_DIR")))
        .unwrap_or_else(|error| panic!("canonicalize repo cert asset {file}: {error}"))
}

/// Boot an iggy-server cluster with `[http.tls]` enabled against the repo
/// loopback cert, then prove a real HTTPS request is served over HTTP/2. Two
/// nodes rather than one because the `/cluster/metadata` assertion below
/// needs an enabled cluster roster (a single-node harness runs with the
/// cluster disabled, where metadata synthesizes a self node and selectors
/// are inert); the HTTPS requests themselves only ever talk to node 0.
#[tokio::test]
#[serial_test::parallel]
async fn given_http_tls_enabled_when_pinging_should_serve_https_over_http2() {
    let mut harness = TestHarness::builder()
        .default_server()
        .cluster_nodes(TLS_CLUSTER_NODES)
        .build()
        .expect("build TLS harness");

    let cert = cert_asset("iggy_cert.pem");
    let key = cert_asset("iggy_key.pem");
    for node in 0..TLS_CLUSTER_NODES {
        harness
            .node_mut(node)
            .add_env("IGGY_HTTP_TLS_ENABLED", "true");
        harness
            .node_mut(node)
            .add_env("IGGY_HTTP_TLS_CERT_FILE", cert.display().to_string());
        harness
            .node_mut(node)
            .add_env("IGGY_HTTP_TLS_KEY_FILE", key.display().to_string());
        // The roster (selectors included) must be identical on every node;
        // the selectors back the ConnectInfo-stamp assertion below.
        for roster_entry in 0..TLS_CLUSTER_NODES {
            for (suffix, value) in TLS_SELECTOR_ENVS {
                harness.node_mut(node).add_env(
                    format!("IGGY_CLUSTER_NODES_{roster_entry}_ADVERTISED_ADDRESSES_{suffix}"),
                    value,
                );
            }
        }
    }

    harness
        .start()
        .await
        .expect("start the server cluster with HTTPS enabled");

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
    let token = identity
        .access_token
        .expect("login over HTTPS must return an access token");
    assert!(
        !token.token.is_empty(),
        "login over HTTPS must return a non-empty access token"
    );

    // ConnectInfo-stamp proof: the TLS serve loop stamps the peer address by
    // hand (`http::tls::serve_connection`), and `Identity.client_ip` silently
    // degrades to the catch-all when the extension is missing. Only the
    // selector address in the metadata reply proves the stamp reached the
    // handler; the catch-all here is the roster ip `127.0.0.1`.
    let metadata = client
        .get(format!("{base_url}/cluster/metadata"))
        .bearer_auth(&token.token)
        .send()
        .await
        .expect("cluster metadata request over HTTPS");
    assert_eq!(
        metadata.status(),
        StatusCode::OK,
        "cluster metadata must answer 200 over HTTPS"
    );
    let metadata: serde_json::Value = metadata.json().await.expect("decode cluster metadata");
    let nodes = metadata["nodes"]
        .as_array()
        .expect("metadata must carry a nodes array");
    assert_eq!(
        nodes.len(),
        TLS_CLUSTER_NODES,
        "full roster must be reported"
    );
    for node in nodes {
        assert_eq!(
            node["ip"].as_str(),
            Some(TLS_SELECTOR_ADDRESS),
            "HTTPS metadata must serve the loopback selector address for node {}, \
             proving the TLS path stamped the client peer address",
            node["name"]
        );
    }
}
