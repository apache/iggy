/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::server::scenarios::{
    cross_protocol_pat_scenario, delete_segments_scenario, message_size_scenario,
    segment_rotation_race_scenario, single_message_per_batch_scenario, tcp_tls_scenario,
    websocket_tls_scenario,
};
use iggy::prelude::*;
use integration::{
    harness::{TestHarness, TestServerConfig},
    http_client::HttpClientFactory,
    quic_client::QuicClientFactory,
    tcp_client::TcpClientFactory,
    test_server::{IpAddrKind, TestServer},
    test_tls_utils::generate_test_certificates,
    websocket_client::WebSocketClientFactory,
};
use serial_test::parallel;
use std::collections::HashMap;

#[tokio::test]
#[parallel]
async fn should_delete_segments_and_validate_filesystem() {
    let mut harness = TestHarness::builder()
        .server(
            TestServerConfig::builder()
                .extra_envs(HashMap::from([(
                    "IGGY_SYSTEM_SEGMENT_SIZE".to_string(),
                    "1MiB".to_string(),
                )]))
                .build(),
        )
        .build()
        .unwrap();

    harness.start().await.unwrap();

    let client = harness.tcp_root_client().await.unwrap();
    let data_path = harness.server().data_path();

    delete_segments_scenario::run(&client, &data_path).await;
}

// TCP TLS scenario is obviously specific to TCP transport, and requires special
// setup so it's not included in the matrix.
#[tokio::test]
#[parallel]
async fn tcp_tls_scenario_should_be_valid() {
    let temp_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
    let cert_dir = temp_dir.path();
    let cert_dir_str = cert_dir.to_str().unwrap();

    generate_test_certificates(cert_dir_str).expect("Failed to generate test certificates");

    let mut extra_envs = HashMap::new();
    extra_envs.insert("IGGY_TCP_TLS_ENABLED".to_string(), "true".to_string());
    extra_envs.insert(
        "IGGY_TCP_TLS_CERT_FILE".to_string(),
        cert_dir.join("test_cert.pem").to_str().unwrap().to_string(),
    );
    extra_envs.insert(
        "IGGY_TCP_TLS_KEY_FILE".to_string(),
        cert_dir.join("test_key.pem").to_str().unwrap().to_string(),
    );

    let mut test_server = TestServer::new(Some(extra_envs), true, None, IpAddrKind::V4);
    test_server.start();

    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let cert_path = cert_dir.join("test_cert.pem").to_str().unwrap().to_string();

    let client = IggyClientBuilder::new()
        .with_tcp()
        .with_server_address(server_addr)
        .with_tls_enabled(true)
        .with_tls_domain("localhost".to_string())
        .with_tls_ca_file(cert_path)
        .build()
        .expect("Failed to create TLS client");

    client
        .connect()
        .await
        .expect("Failed to connect TLS client");

    let client = IggyClient::create(ClientWrapper::Iggy(client), None, None);

    tcp_tls_scenario::run(&client).await;
}

#[tokio::test]
#[parallel]
async fn tcp_tls_self_signed_scenario_should_be_valid() {
    use iggy::clients::client_builder::IggyClientBuilder;

    let mut extra_envs = HashMap::new();
    extra_envs.insert("IGGY_TCP_TLS_ENABLED".to_string(), "true".to_string());
    extra_envs.insert("IGGY_TCP_TLS_SELF_SIGNED".to_string(), "true".to_string());

    let mut test_server = TestServer::new(Some(extra_envs), true, None, IpAddrKind::V4);
    test_server.start();

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let server_addr = test_server.get_raw_tcp_addr().unwrap();

    let client = IggyClientBuilder::new()
        .with_tcp()
        .with_server_address(server_addr)
        .with_tls_enabled(true)
        .with_tls_domain("localhost".to_string())
        .with_tls_validate_certificate(false)
        .build()
        .expect("Failed to create TLS client");

    client
        .connect()
        .await
        .expect("Failed to connect TLS client with self-signed cert");

    let client = IggyClient::create(ClientWrapper::Iggy(client), None, None);

    tcp_tls_scenario::run(&client).await;
}

#[tokio::test]
#[parallel]
async fn websocket_tls_scenario_should_be_valid() {
    let temp_dir = tempfile::TempDir::new().expect("Failed to create temp dir");
    let cert_dir = temp_dir.path();
    let cert_dir_str = cert_dir.to_str().unwrap();

    generate_test_certificates(cert_dir_str).expect("Failed to generate test certificates");

    let mut extra_envs = HashMap::new();
    extra_envs.insert("IGGY_WEBSOCKET_TLS_ENABLED".to_string(), "true".to_string());
    extra_envs.insert(
        "IGGY_WEBSOCKET_TLS_CERT_FILE".to_string(),
        cert_dir.join("test_cert.pem").to_str().unwrap().to_string(),
    );
    extra_envs.insert(
        "IGGY_WEBSOCKET_TLS_KEY_FILE".to_string(),
        cert_dir.join("test_key.pem").to_str().unwrap().to_string(),
    );

    let mut test_server = TestServer::new(Some(extra_envs), true, None, IpAddrKind::V4);
    test_server.start();

    let server_addr = test_server.get_websocket_addr().unwrap();
    let cert_path = cert_dir.join("test_cert.pem").to_str().unwrap().to_string();

    let client = IggyClientBuilder::new()
        .with_websocket()
        .with_server_address(server_addr)
        .with_tls_enabled(true)
        .with_tls_domain("localhost".to_string())
        .with_tls_ca_file(cert_path)
        .build()
        .expect("Failed to create WebSocket TLS client");

    client
        .connect()
        .await
        .expect("Failed to connect WebSocket TLS client");

    let client = IggyClient::create(ClientWrapper::Iggy(client), None, None);

    websocket_tls_scenario::run(&client).await;
}

// Message size scenario is specific to TCP transport to test the behavior around the maximum message size.
// When run on other transports, it will fail because both QUIC and HTTP have different message size limits.
#[tokio::test]
#[parallel]
async fn message_size_scenario() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let client_factory = TcpClientFactory {
        server_addr,
        ..Default::default()
    };

    message_size_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn should_handle_single_message_per_batch_with_delayed_persistence() {
    let mut extra_envs = HashMap::new();
    extra_envs.insert(
        "IGGY_SYSTEM_PARTITION_MESSAGES_REQUIRED_TO_SAVE".to_string(),
        "10000".to_string(),
    );

    let mut test_server = TestServer::new(Some(extra_envs), true, None, IpAddrKind::V4);
    test_server.start();

    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let client_factory = TcpClientFactory {
        server_addr,
        ..Default::default()
    };

    single_message_per_batch_scenario::run(&client_factory, 5).await;
}

/// This test configures the server to trigger frequent segment rotations and runs
/// multiple concurrent producers across all protocols (TCP, HTTP, QUIC, WebSocket)
/// to maximize the chance of hitting the race condition between persist_messages_to_disk
/// and handle_full_segment.
///
/// Server configuration:
/// - Very small segment size (512B) to trigger frequent rotations
/// - Short message_saver interval (1s) to add concurrent persist operations
/// - Small messages_required_to_save (32) to trigger more frequent saves
/// - cache_indexes = none to trigger clear_active_indexes path
///
/// Test configuration:
/// - 8 producers total (2 per protocol: TCP, HTTP, QUIC, WebSocket)
/// - All producers write to the same partition for maximum lock contention
#[tokio::test]
#[parallel]
async fn segment_rotation_scenario() {
    let mut extra_envs = HashMap::new();

    extra_envs.insert("IGGY_SYSTEM_SEGMENT_SIZE".to_string(), "512B".to_string());

    extra_envs.insert("IGGY_MESSAGE_SAVER_INTERVAL".to_string(), "1s".to_string());

    extra_envs.insert(
        "IGGY_SYSTEM_PARTITION_MESSAGES_REQUIRED_TO_SAVE".to_string(),
        "32".to_string(),
    );

    extra_envs.insert(
        "IGGY_SYSTEM_SEGMENT_CACHE_INDEXES".to_string(),
        "none".to_string(),
    );

    extra_envs.insert("IGGY_TCP_SOCKET_MIGRATION".to_string(), "false".to_string());

    extra_envs.insert(
        "IGGY_TCP_SOCKET_OVERRIDE_DEFAULTS".to_string(),
        "true".to_string(),
    );
    extra_envs.insert("IGGY_TCP_SOCKET_NODELAY".to_string(), "true".to_string());

    let mut test_server = TestServer::new(Some(extra_envs), true, None, IpAddrKind::V4);
    test_server.start();

    let tcp_factory = TcpClientFactory {
        server_addr: test_server.get_raw_tcp_addr().unwrap(),
        ..Default::default()
    };

    let http_factory = HttpClientFactory {
        server_addr: test_server.get_http_api_addr().unwrap(),
    };

    let quic_factory = QuicClientFactory {
        server_addr: test_server.get_quic_udp_addr().unwrap(),
    };

    let websocket_factory = WebSocketClientFactory {
        server_addr: test_server.get_websocket_addr().unwrap(),
        ..Default::default()
    };

    let factories: Vec<&dyn integration::test_server::ClientFactory> = vec![
        &tcp_factory,
        &http_factory,
        &quic_factory,
        &websocket_factory,
    ];

    segment_rotation_race_scenario::run(&factories).await;
}

#[tokio::test]
#[parallel]
async fn should_see_pat_created_via_http_when_listing_via_tcp() {
    cross_protocol_pat_scenario::run().await;
}

#[tokio::test]
#[parallel]
async fn should_see_pat_created_via_tcp_when_listing_via_http() {
    cross_protocol_pat_scenario::run_tcp_to_http().await;
}

#[tokio::test]
#[parallel]
async fn should_not_see_pat_deleted_via_http_when_listing_via_tcp() {
    cross_protocol_pat_scenario::run_delete_visibility().await;
}
