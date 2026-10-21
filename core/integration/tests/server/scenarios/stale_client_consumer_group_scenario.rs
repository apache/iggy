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

use futures::StreamExt;
use iggy::prelude::*;
use iggy_common::{Credentials, Durability};
use integration::iggy_harness;
use secrecy::SecretString;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

/// Bounds the wait for a client that stopped sending to lose its group
/// membership. At the specs' 7s heartbeat interval, the server stops renewing
/// a silent session after 8.4s and ends it about 8.4s later. Generous relative
/// to that, because the report and expiry passes compete with the rest of the
/// suite.
const STALE_EVICTION_TIMEOUT: Duration = Duration::from_secs(30);
const STALE_EVICTION_RETRY_INTERVAL: Duration = Duration::from_millis(200);
/// How long a live member must keep its membership across a backup restart.
/// More than twice the 8.4s session timeout at the specs' 7s heartbeat
/// interval, so the wait outlasts the expiry of a lease that the restart
/// stopped renewing.
const MEMBERSHIP_HOLD_PERIOD: Duration = Duration::from_secs(20);

const STREAM_NAME: &str = "stale-test-stream";
const TOPIC_NAME: &str = "stale-test-topic";
const CONSUMER_GROUP_NAME: &str = "stale-test-cg";
const PARTITIONS_COUNT: u32 = 1;
const TOTAL_MESSAGES: u32 = 10;

enum Restart {
    Client,
    ServerShutdown,
    ServerCrash,
    ClusterCrash,
}

#[iggy_harness(cluster_nodes = 1, server(
    heartbeat.enabled = true,
    heartbeat.interval = "7s",
    consumer_group.heartbeat_interval = "500ms",
))]
async fn given_client_disconnect_when_member_is_replaced_should_resume_consumption(
    harness: &mut integration::harness::TestHarness,
) {
    verify_replacement_after_restart(harness, Restart::Client).await;
}

#[iggy_harness(cluster_nodes = 1, server(
    heartbeat.enabled = true,
    heartbeat.interval = "7s",
    consumer_group.heartbeat_interval = "500ms",
))]
async fn given_server_shutdown_when_member_is_replaced_should_resume_consumption(
    harness: &mut integration::harness::TestHarness,
) {
    verify_replacement_after_restart(harness, Restart::ServerShutdown).await;
}

#[iggy_harness(cluster_nodes = 1, server(
    heartbeat.enabled = true,
    heartbeat.interval = "7s",
    consumer_group.heartbeat_interval = "500ms",
))]
async fn given_server_crash_when_member_is_replaced_should_resume_consumption(
    harness: &mut integration::harness::TestHarness,
) {
    verify_replacement_after_restart(harness, Restart::ServerCrash).await;
}

#[iggy_harness(cluster_nodes = 1, server(
    heartbeat.enabled = false,
    heartbeat.interval = "7s",
    consumer_group.heartbeat_interval = "500ms",
))]
async fn given_heartbeat_disabled_when_member_is_replaced_after_crash_should_resume_consumption(
    harness: &mut integration::harness::TestHarness,
) {
    verify_replacement_after_restart(harness, Restart::ServerCrash).await;
}

#[iggy_harness(cluster_nodes = 3, server(
    heartbeat.enabled = false,
    heartbeat.interval = "7s",
    consumer_group.heartbeat_interval = "500ms",
))]
async fn given_cluster_node_restart_when_member_is_replaced_should_resume_consumption(
    harness: &mut integration::harness::TestHarness,
) {
    verify_replacement_after_restart(harness, Restart::ServerCrash).await;
}

#[iggy_harness(cluster_nodes = 3, server(
    heartbeat.enabled = false,
    heartbeat.interval = "7s",
    consumer_group.heartbeat_interval = "500ms",
))]
async fn given_cluster_restart_when_member_is_replaced_should_resume_consumption(
    harness: &mut integration::harness::TestHarness,
) {
    verify_replacement_after_restart(harness, Restart::ClusterCrash).await;
}

#[iggy_harness(cluster_nodes = 3, server(
    heartbeat.enabled = false,
    heartbeat.interval = "7s",
    consumer_group.heartbeat_interval = "500ms",
))]
async fn given_live_member_when_backup_restarts_should_preserve_membership(
    harness: &mut integration::harness::TestHarness,
) {
    let consumer_client =
        create_client(&harness.server().raw_tcp_addr().unwrap(), "1h", true).await;
    setup_resources(&consumer_client, Durability::Persisted).await;
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let group_id = Identifier::named(CONSUMER_GROUP_NAME).unwrap();
    consumer_client
        .join_consumer_group(&stream, &topic, &group_id)
        .await
        .unwrap();
    let before = integration::harness::wait_for_consumer_group_assignment(
        &consumer_client,
        &stream,
        &topic,
        &group_id,
        1,
        STALE_EVICTION_TIMEOUT,
    )
    .await;
    assert_eq!(before.members_count, 1);

    harness.kill_node(2).unwrap();
    harness.restart_node(2).unwrap();
    sleep(MEMBERSHIP_HOLD_PERIOD).await;

    let observer = harness.root_client_for_node(0).await.unwrap();
    let after = observer
        .get_consumer_group(&stream, &topic, &group_id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        after.members_count, 1,
        "a peer restart must preserve a live remote member"
    );
    assert_eq!(after.members[0].id, before.members[0].id);
    assert_eq!(after.members[0].partitions, before.members[0].partitions);
    consumer_client.get_me().await.unwrap();
}

async fn verify_replacement_after_restart(
    harness: &mut integration::harness::TestHarness,
    restart: Restart,
) {
    let server_addr = harness.server().raw_tcp_addr().unwrap();
    let original = create_client(&server_addr, "500ms", true).await;
    setup_resources(&original, Durability::Persisted).await;
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let group_id = Identifier::named(CONSUMER_GROUP_NAME).unwrap();
    let consumer = Consumer::group(group_id.clone());
    original
        .join_consumer_group(&stream, &topic, &group_id)
        .await
        .unwrap();
    integration::harness::wait_for_consumer_group_assignment(
        &original,
        &stream,
        &topic,
        &group_id,
        1,
        STALE_EVICTION_TIMEOUT,
    )
    .await;
    let before = original
        .poll_messages(
            &stream,
            &topic,
            None,
            &consumer,
            &PollingStrategy::next(),
            1,
            true,
        )
        .await
        .unwrap();
    assert_eq!(before.messages.len(), 1);
    // A poll reply does not acknowledge durable AUTO completion.
    original
        .store_consumer_offset(
            &consumer,
            &stream,
            &topic,
            Some(before.partition_id),
            before.messages[0].header.offset,
        )
        .await
        .expect("persist the restart cursor before stopping the original member");
    match restart {
        Restart::Client => {}
        Restart::ServerShutdown => harness.stop_node(0).unwrap(),
        Restart::ServerCrash => harness.kill_node(0).unwrap(),
        Restart::ClusterCrash => {
            for node in 0..harness.cluster_size() {
                harness.kill_node(node).unwrap();
            }
        }
    }
    original.disconnect().await.unwrap();
    drop(original);
    if matches!(restart, Restart::ClusterCrash) {
        for node in 0..harness.cluster_size() {
            harness.restart_node(node).unwrap();
        }
    } else if !matches!(restart, Restart::Client) {
        harness.restart_node(0).unwrap();
    }

    let replacement = create_client(&server_addr, "500ms", true).await;
    replacement
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    replacement
        .join_consumer_group(&stream, &topic, &group_id)
        .await
        .unwrap();

    // Metadata login can finish before the partition's recovered WAL is applied.
    let deadline = tokio::time::Instant::now() + STALE_EVICTION_TIMEOUT;
    let direct = loop {
        let direct = replacement
            .poll_messages(
                &stream,
                &topic,
                Some(0),
                &Consumer::default(),
                &PollingStrategy::offset(1),
                1,
                false,
            )
            .await
            .unwrap();
        if !direct.messages.is_empty() {
            break direct;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "retained data was not readable within {STALE_EVICTION_TIMEOUT:?}"
        );
        sleep(STALE_EVICTION_RETRY_INTERVAL).await;
    };
    assert_eq!(direct.messages.len(), 1, "retained data must be readable");

    let deadline = tokio::time::Instant::now() + STALE_EVICTION_TIMEOUT;
    loop {
        let polled = replacement
            .poll_messages(
                &stream,
                &topic,
                None,
                &consumer,
                &PollingStrategy::next(),
                1,
                true,
            )
            .await
            .expect("replacement group poll must succeed");
        if !polled.messages.is_empty() {
            assert_eq!(polled.messages[0].payload, direct.messages[0].payload);
            let group = replacement
                .get_consumer_group(&stream, &topic, &group_id)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                group.members_count, 1,
                "only the replacement member remains"
            );
            break;
        }
        if tokio::time::Instant::now() >= deadline {
            let group = replacement
                .get_consumer_group(&stream, &topic, &group_id)
                .await
                .unwrap()
                .unwrap();
            let offset = replacement
                .get_consumer_offset(&consumer, &stream, &topic, Some(0))
                .await
                .unwrap();
            panic!(
                "replacement polled empty for {STALE_EVICTION_TIMEOUT:?} despite readable data; group={group:?}; offset={offset:?}"
            );
        }
        sleep(STALE_EVICTION_RETRY_INTERVAL).await;
    }
}

async fn create_client(server_addr: &str, heartbeat_interval: &str, reconnect: bool) -> IggyClient {
    let config = TcpClientConfig {
        server_address: server_addr.to_string(),
        heartbeat_interval: NonZeroIggyDuration::from_str(heartbeat_interval).unwrap(),
        reconnection: TcpClientReconnectionConfig {
            enabled: reconnect,
            ..TcpClientReconnectionConfig::default()
        },
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
        heartbeat_interval: NonZeroIggyDuration::from_str("1h").unwrap(),
        nodelay: true,
        auto_login: AutoLogin::Enabled(Credentials::UsernamePassword(
            DEFAULT_ROOT_USERNAME.to_string(),
            SecretString::from(DEFAULT_ROOT_PASSWORD),
        )),
        reconnection: TcpClientReconnectionConfig {
            enabled: true,
            max_retries: Some(5),
            interval: NonZeroIggyDuration::from_str("500ms").unwrap(),
            reestablish_after: IggyDuration::from_str("100ms").unwrap(),
        },
        ..TcpClientConfig::default()
    };
    let client = TcpClient::create(Arc::new(config)).unwrap();
    Client::connect(&client).await.unwrap();
    IggyClient::create(ClientWrapper::Tcp(client), None, None)
}

async fn setup_resources(client: &IggyClient, durability: Durability) {
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();

    client.create_stream(STREAM_NAME).await.unwrap();

    client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(PARTITIONS_COUNT),
                message_expiry: Some(IggyExpiry::NeverExpire),
                durability,
                consumer_offset_durability: durability,
                ..TopicCreateOptions::default()
            },
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
#[iggy_harness(server(
    heartbeat.enabled = true,
    heartbeat.interval = "7s",
    consumer_group.heartbeat_interval = "500ms",
))]
async fn should_handle_stale_client_with_manual_reconnection(
    harness: &integration::harness::TestHarness,
) {
    let server_addr = harness.server().raw_tcp_addr().unwrap();

    let setup_client = create_client(&server_addr, "500ms", true).await;
    setup_resources(&setup_client, Durability::Persisted).await;

    // Rebinding after eviction would renew the logical session instead of letting it expire.
    let stale_client = create_client(&server_addr, "1h", false).await;
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

    // Wait for the server to end the stale client's session, observed through
    // `setup_client` rather than by polling `stale_client` itself: a poll
    // counts as activity and would keep resetting the staleness the test is
    // waiting for. That is also why the assertion below gets only a couple of
    // attempts -- each one refreshes liveness, so retrying harder makes
    // eviction less likely, not more.
    integration::harness::wait_for_consumer_group_assignment(
        &setup_client,
        &Identifier::named(STREAM_NAME).unwrap(),
        &Identifier::named(TOPIC_NAME).unwrap(),
        &Identifier::named(CONSUMER_GROUP_NAME).unwrap(),
        0,
        STALE_EVICTION_TIMEOUT,
    )
    .await;

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
    assert!(got_error, "Expected error after heartbeat eviction");

    // Reconnect with new client
    drop(stale_client);
    let new_client = create_client(&server_addr, "500ms", true).await;
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
}

/// Tests that IggyConsumer automatically recovers after stale disconnect. On one
/// node every eviction takes the client's only link down.
#[iggy_harness(cluster_nodes = [1, 3], server(
    heartbeat.enabled = true,
    heartbeat.interval = "7s",
    consumer_group.heartbeat_interval = "500ms",
))]
async fn should_handle_stale_client_with_auto_reconnection(
    harness: &integration::harness::TestHarness,
) {
    let server_addr = harness.server().raw_tcp_addr().unwrap();

    let setup_client = create_client(&server_addr, "500ms", true).await;
    setup_resources(&setup_client, Durability::Persisted).await;

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
        .polling_retry_interval(NonZeroIggyDuration::from_str("500ms").unwrap())
        .build();

    consumer.init().await.unwrap();

    let mut messages_consumed = 0u32;
    let start = std::time::Instant::now();
    let timeout = STALE_EVICTION_TIMEOUT + Duration::from_secs(15);

    while messages_consumed < TOTAL_MESSAGES && start.elapsed() < timeout {
        let poll_result: Option<Result<ReceivedMessage, IggyError>> =
            tokio::time::timeout(Duration::from_millis(500), consumer.next())
                .await
                .ok()
                .flatten();

        if let Some(Ok(_)) = poll_result {
            messages_consumed += 1;
            if messages_consumed == 5 {
                // Stay silent past the bound in which a silent client loses
                // its membership. The heartbeat verifier evicts the
                // consumer's links, but each link reconnects and binds the
                // same session again, which renews its lease. The setup
                // client runs no heartbeat, so these reads also keep its own
                // session alive.
                let deadline = tokio::time::Instant::now() + STALE_EVICTION_TIMEOUT;
                while tokio::time::Instant::now() < deadline {
                    let group = setup_client
                        .get_consumer_group(
                            &Identifier::named(STREAM_NAME).unwrap(),
                            &Identifier::named(TOPIC_NAME).unwrap(),
                            &Identifier::named(CONSUMER_GROUP_NAME).unwrap(),
                        )
                        .await
                        .unwrap()
                        .expect("consumer group exists");
                    assert_eq!(
                        group.members_count, 1,
                        "the rebound session must keep its membership: {group:?}"
                    );
                    sleep(STALE_EVICTION_RETRY_INTERVAL).await;
                }
            }
        }
    }

    drop(consumer);
    let _ = setup_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await;

    assert_eq!(
        messages_consumed, TOTAL_MESSAGES,
        "Should consume all messages after automatic reconnection"
    );
}
