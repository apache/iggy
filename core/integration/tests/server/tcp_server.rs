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
    consumer_group_join_scenario, consumer_group_with_multiple_clients_polling_messages_scenario,
    consumer_group_with_single_client_polling_messages_scenario, create_message_payload,
    delete_segments_scenario::test_delete_segments_scenario, message_headers_scenario,
    message_size_scenario, server_restart_scenario, stream_size_validation_scenario,
    system_scenario, tcp_tls_scenario, user_scenario,
};
use iggy::prelude::*;
use integration::{
    tcp_client::TcpClientFactory,
    test_server::{IpAddrKind, TestServer},
};
use serial_test::parallel;
use std::{collections::HashMap, thread::sleep};

#[tokio::test]
#[parallel]
async fn system_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let client_factory = TcpClientFactory {
        server_addr,
        ..Default::default()
    };
    system_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn user_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let client_factory = TcpClientFactory {
        server_addr,
        ..Default::default()
    };
    user_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn message_headers_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let client_factory = TcpClientFactory {
        server_addr,
        ..Default::default()
    };
    message_headers_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn create_message_payload_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let client_factory = TcpClientFactory {
        server_addr,
        ..Default::default()
    };
    create_message_payload::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn consumer_group_join_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let client_factory = TcpClientFactory {
        server_addr,
        ..Default::default()
    };
    consumer_group_join_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn consumer_group_with_single_client_polling_messages_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let client_factory = TcpClientFactory {
        server_addr,
        ..Default::default()
    };
    consumer_group_with_single_client_polling_messages_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn consumer_group_with_multiple_clients_polling_messages_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let client_factory = TcpClientFactory {
        server_addr,
        ..Default::default()
    };
    consumer_group_with_multiple_clients_polling_messages_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn stream_size_validation_scenario_should_be_valid() {
    let mut test_server = TestServer::default();
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let client_factory = TcpClientFactory {
        server_addr,
        ..Default::default()
    };
    stream_size_validation_scenario::run(&client_factory).await;
}

#[tokio::test]
#[parallel]
async fn message_size_scenario_should_be_valid() {
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
async fn should_delete_segments_via_tcp_binary_protocol() {
    let mut extra_envs = HashMap::new();
    extra_envs.insert("IGGY_SYSTEM_SEGMENT_SIZE".to_string(), "1MiB".to_string());

    let mut test_server = TestServer::new(Some(extra_envs), true, None, IpAddrKind::V4);
    test_server.start();

    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let client_factory = TcpClientFactory {
        server_addr,
        ..Default::default()
    };

    test_delete_segments_scenario(&client_factory, &test_server).await;
}

#[tokio::test]
#[parallel]
async fn should_verify_data_integrity_after_server_restart_with_open_segment_index_cache() {
    verify_data_integrity_after_server_restart_with_cache_setting("open_segment").await;
}

#[tokio::test]
#[parallel]
async fn should_verify_data_integrity_after_server_restart_with_all_index_cache() {
    verify_data_integrity_after_server_restart_with_cache_setting("all").await;
}

#[tokio::test]
#[parallel]
async fn should_verify_data_integrity_after_server_restart_with_no_index_cache() {
    verify_data_integrity_after_server_restart_with_cache_setting("none").await;
}

async fn verify_data_integrity_after_server_restart_with_cache_setting(cache_setting: &str) {
    let data_dir = format!("local_data_restart_test_{}", uuid::Uuid::new_v4().simple());

    let mut extra_env = HashMap::new();
    extra_env.insert("IGGY_SYSTEM_SEGMENT_SIZE".to_string(), "250KiB".to_string());
    extra_env.insert("IGGY_SYSTEM_PATH".to_string(), data_dir.clone());
    extra_env.insert(
        "IGGY_SYSTEM_SEGMENT_CACHE_INDEXES".to_string(),
        cache_setting.to_string(),
    );

    let mut test_server = TestServer::new(Some(extra_env.clone()), false, None, IpAddrKind::V4);
    test_server.start();

    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let client_factory = TcpClientFactory {
        server_addr,
        ..Default::default()
    };

    server_restart_scenario::run(&client_factory, 0).await;

    sleep(std::time::Duration::from_secs(2));

    test_server.stop();
    drop(test_server);

    let mut test_server = TestServer::new(Some(extra_env), false, None, IpAddrKind::V4);
    test_server.start();

    let server_addr = test_server.get_raw_tcp_addr().unwrap();
    let client_factory = TcpClientFactory {
        server_addr,
        ..Default::default()
    };

    server_restart_scenario::run(&client_factory, 12).await;

    test_server.stop();
    drop(test_server);
}

#[tokio::test]
#[parallel]
async fn tcp_tls_scenario_should_be_valid() {
    use iggy::clients::client_builder::IggyClientBuilder;
    use integration::test_tls_utils::generate_test_certificates;

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

    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

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

    let client = iggy::clients::client::IggyClient::create(Box::new(client), None, None);

    tcp_tls_scenario::run(&client).await;
}
