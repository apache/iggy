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

use futures::StreamExt;
use iggy::prelude::*;
use iggy_common::Credentials;
use integration::test_server::{IpAddrKind, TestServer};
use serial_test::parallel;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

const STREAM_NAME: &str = "stale-test-stream";
const TOPIC_NAME: &str = "stale-test-topic";
const CONSUMER_GROUP_NAME: &str = "stale-test-cg";
const PARTITIONS_COUNT: u32 = 1;
const TOTAL_MESSAGES: u32 = 10;

fn create_test_server() -> TestServer {
    let mut extra_envs = HashMap::new();
    extra_envs.insert("IGGY_HEARTBEAT_ENABLED".to_string(), "true".to_string());
    extra_envs.insert("IGGY_HEARTBEAT_INTERVAL".to_string(), "2s".to_string());
    extra_envs.insert(
        "IGGY_TCP_SOCKET_OVERRIDE_DEFAULTS".to_string(),
        "true".to_string(),
    );
    extra_envs.insert("IGGY_TCP_SOCKET_NODELAY".to_string(), "true".to_string());
    TestServer::new(Some(extra_envs), true, None, IpAddrKind::V4)
}

async fn create_client(server_addr: &str, heartbeat_interval: &str) -> IggyClient {
    let config = TcpClientConfig {
        server_address: server_addr.to_string(),
        heartbeat_interval: IggyDuration::from_str(heartbeat_interval).unwrap(),
        nodelay: true,
        ..TcpClientConfig::default()
    };
    let client = TcpClient::create(Arc::new(config)).unwrap();
    Client::connect(&client).await.unwrap();
    IggyClient::create(ClientWrapper::Tcp(client), None, None)
}

async fn create_reconnecting_client(server_addr: &str) -> IggyClient {
    let config = TcpClientConfig {
        server_address: server_addr.to_string(),
        heartbeat_interval: IggyDuration::from_str("1h").unwrap(),
        nodelay: true,
        auto_login: AutoLogin::Enabled(Credentials::UsernamePassword(
            DEFAULT_ROOT_USERNAME.to_string(),
            DEFAULT_ROOT_PASSWORD.to_string(),
        )),
        reconnection: TcpClientReconnectionConfig {
            enabled: true,
            max_retries: Some(5),
            interval: IggyDuration::from_str("500ms").unwrap(),
            reestablish_after: IggyDuration::from_str("100ms").unwrap(),
        },
        ..TcpClientConfig::default()
    };
    let client = TcpClient::create(Arc::new(config)).unwrap();
    Client::connect(&client).await.unwrap();
    IggyClient::create(ClientWrapper::Tcp(client), None, None)
}

async fn setup_resources(client: &IggyClient) {
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();

    client.create_stream(STREAM_NAME).await.unwrap();

    client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            PARTITIONS_COUNT,
            CompressionAlgorithm::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();

    client
        .create_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            CONSUMER_GROUP_NAME,
        )
        .await
        .unwrap();

    for i in 0..TOTAL_MESSAGES {
        let message = IggyMessage::from_str(&format!("message-{i}")).unwrap();
        let mut messages = vec![message];
        client
            .send_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                &Partitioning::partition_id(0),
                &mut messages,
            )
            .await
            .unwrap();
    }
}

/// Tests that a stale client receives clean errors and can manually reconnect.
#[tokio::test]
#[parallel]
async fn should_handle_stale_client_with_manual_reconnection() {
    let mut test_server = create_test_server();
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();

    let setup_client = create_client(&server_addr, "500ms").await;
    setup_resources(&setup_client).await;

    // Client with 1h heartbeat will become stale
    let stale_client = create_client(&server_addr, "1h").await;
    stale_client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();

    stale_client
        .join_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Identifier::named(CONSUMER_GROUP_NAME).unwrap(),
        )
        .await
        .unwrap();

    let consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());

    // Poll first 5 messages
    let mut messages_polled = 0;
    while messages_polled < 5 {
        let polled = stale_client
            .poll_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                None,
                &consumer,
                &PollingStrategy::next(),
                1,
                true,
            )
            .await
            .unwrap();
        messages_polled += polled.messages.len();
    }
    assert_eq!(messages_polled, 5);

    // Wait for heartbeat timeout (2s * 1.2 = 2.4s threshold)
    sleep(Duration::from_secs(4)).await;

    // Should get error after stale detection
    let mut got_error = false;
    for _ in 0..3 {
        if stale_client
            .poll_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                None,
                &consumer,
                &PollingStrategy::next(),
                1,
                true,
            )
            .await
            .is_err()
        {
            got_error = true;
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert!(got_error, "Expected error after heartbeat timeout");

    // Reconnect with new client
    drop(stale_client);
    let new_client = create_client(&server_addr, "500ms").await;
    new_client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    new_client
        .join_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Identifier::named(CONSUMER_GROUP_NAME).unwrap(),
        )
        .await
        .unwrap();

    // Poll remaining messages
    let mut remaining_polled = 0;
    let start = std::time::Instant::now();
    while remaining_polled < 5 && start.elapsed() < Duration::from_secs(5) {
        match new_client
            .poll_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                None,
                &consumer,
                &PollingStrategy::next(),
                1,
                true,
            )
            .await
        {
            Ok(polled) => remaining_polled += polled.messages.len(),
            Err(_) => sleep(Duration::from_millis(100)).await,
        }
    }
    assert_eq!(remaining_polled, 5);

    let _ = setup_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await;
    test_server.stop();
}

/// Tests that IggyConsumer automatically recovers after stale disconnect.
#[tokio::test]
#[parallel]
async fn should_handle_stale_client_with_auto_reconnection() {
    let mut test_server = create_test_server();
    test_server.start();
    let server_addr = test_server.get_raw_tcp_addr().unwrap();

    let setup_client = create_client(&server_addr, "500ms").await;
    setup_resources(&setup_client).await;

    let consumer_client = create_reconnecting_client(&server_addr).await;
    // Note: auto_login is enabled in create_reconnecting_client, so no manual login needed

    let mut consumer: IggyConsumer = consumer_client
        .consumer_group(CONSUMER_GROUP_NAME, STREAM_NAME, TOPIC_NAME)
        .unwrap()
        .batch_length(1)
        .poll_interval(IggyDuration::from_str("100ms").unwrap())
        .polling_strategy(PollingStrategy::next())
        .auto_join_consumer_group()
        .create_consumer_group_if_not_exists()
        .auto_commit(AutoCommit::When(AutoCommitWhen::PollingMessages))
        .polling_retry_interval(IggyDuration::from_str("500ms").unwrap())
        .build();

    consumer.init().await.unwrap();

    let mut messages_consumed = 0u32;
    let start = std::time::Instant::now();
    let timeout = Duration::from_secs(15);

    while messages_consumed < TOTAL_MESSAGES && start.elapsed() < timeout {
        let poll_result: Option<Result<ReceivedMessage, IggyError>> =
            tokio::time::timeout(Duration::from_millis(500), consumer.next())
                .await
                .ok()
                .flatten();

        if let Some(Ok(_)) = poll_result {
            messages_consumed += 1;
            if messages_consumed == 5 {
                // Sleep longer than heartbeat threshold (2s * 1.2 = 2.4s) to trigger staleness
                sleep(Duration::from_millis(4000)).await;
            }
        }
    }

    drop(consumer);
    let _ = setup_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await;
    test_server.stop();

    assert_eq!(
        messages_consumed, TOTAL_MESSAGES,
        "Should consume all messages after automatic reconnection"
    );
}
