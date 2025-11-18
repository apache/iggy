/* Licensed to the Apache Software Foundation (ASF) under one
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
    delete_segments_scenario, message_size_scenario, tcp_tls_scenario, websocket_tls_scenario,
};
use iggy::prelude::*;
use integration::{
    tcp_client::TcpClientFactory,
    test_server::{IpAddrKind, TestServer},
    test_tls_utils::generate_test_certificates,
};
use serial_test::parallel;
use std::collections::HashMap;

// This test can run on any transport, but it requires both ClientFactory and
// TestServer parameters, which doesn't fit the unified matrix approach.
#[tokio::test]
#[parallel]
async fn should_delete_segments_and_validate_filesystem() {
    let mut extra_envs = HashMap::new();
    extra_envs.insert("IGGY_SYSTEM_SEGMENT_SIZE".to_string(), "1MiB".to_string());

    let mut test_server = TestServer::new(Some(extra_envs), true, None, IpAddrKind::V4);
    test_server.start();

    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let client_factory = TcpClientFactory {
        server_addr,
        ..Default::default()
    };

    delete_segments_scenario::run(&client_factory, &test_server).await;
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

    let client = iggy::clients::client::IggyClient::create(ClientWrapper::Iggy(client), None, None);

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
