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

//! Ephemeral listener ports end to end. The harness binds every enabled
//! transport to `:0` and learns the OS-chosen ports from the dumped runtime
//! config, which makes this the tripwire for two server contracts at once:
//! the dump must carry every bound address (the HTTP one included, or the
//! harness could not even find that listener), and cluster metadata must
//! report the ports as bound rather than as configured, on the binary and
//! the HTTP spine alike.

use std::collections::HashMap;

use iggy::prelude::*;
use integration::harness::{TestHarness, TestServerConfig};
use reqwest::StatusCode;

use crate::server::http_client::HttpClient;

/// One node with clustering off: a cluster roster names every port before
/// boot and cannot be ephemeral, and the cluster-disabled roster is the one
/// that synthesizes its self node from the bound ports.
#[tokio::test]
#[serial_test::parallel]
async fn given_ephemeral_ports_when_getting_cluster_metadata_should_report_bound_ports() {
    let mut harness = TestHarness::builder()
        .server(
            TestServerConfig::builder()
                .ephemeral_ports(true)
                .extra_envs(HashMap::from([(
                    "IGGY_CLUSTER_ENABLED".to_string(),
                    "false".to_string(),
                )]))
                .build(),
        )
        .cluster_nodes(1)
        .build()
        .expect("build harness");
    harness
        .start()
        .await
        .expect("start server on ephemeral ports");

    let server = harness.server();
    let discovered = [
        ("tcp", server.tcp_addr()),
        ("http", server.http_addr()),
        ("quic", server.quic_addr()),
        ("websocket", server.websocket_addr()),
    ]
    .map(|(transport, addr)| {
        let addr = addr.unwrap_or_else(|| panic!("{transport} address must be discovered"));
        assert_ne!(
            addr.port(),
            0,
            "{transport} must report the port the OS chose, not the configured 0"
        );
        addr.port()
    });
    let [tcp, http, quic, websocket] = discovered;

    let client = server
        .tcp_client()
        .expect("tcp client")
        .with_root_login()
        .connect()
        .await
        .expect("connect over the discovered tcp port");
    let binary = client
        .get_cluster_metadata()
        .await
        .expect("get cluster metadata");
    assert_eq!(
        binary.nodes.len(),
        1,
        "a cluster-disabled server reports itself alone, got {binary}"
    );
    let binary_endpoints = &binary.nodes[0].endpoints;
    assert_eq!(binary_endpoints.tcp, tcp, "binary metadata tcp port");
    assert_eq!(binary_endpoints.http, http, "binary metadata http port");
    assert_eq!(binary_endpoints.quic, quic, "binary metadata quic port");
    assert_eq!(
        binary_endpoints.websocket, websocket,
        "binary metadata websocket port"
    );

    let session = HttpClient::login_root(&harness).await;
    let response = session.get("/cluster/metadata").await;
    assert_eq!(response.status(), StatusCode::OK);
    let over_http: ClusterMetadata = response.json().await.expect("decode cluster metadata");
    assert_eq!(
        over_http.nodes.len(),
        1,
        "HTTP metadata must report the same single node, got {over_http}"
    );
    let http_endpoints = &over_http.nodes[0].endpoints;
    assert_eq!(http_endpoints.tcp, tcp, "HTTP metadata tcp port");
    assert_eq!(http_endpoints.http, http, "HTTP metadata http port");
    assert_eq!(http_endpoints.quic, quic, "HTTP metadata quic port");
    assert_eq!(
        http_endpoints.websocket, websocket,
        "HTTP metadata websocket port"
    );
}
