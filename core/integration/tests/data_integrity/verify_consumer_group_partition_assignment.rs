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

use iggy::prelude::*;
use integration::iggy_harness;
use std::collections::HashSet;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::time::{Duration, sleep};

const STREAM_NAME: &str = "cg-partition-test-stream";
const TOPIC_NAME: &str = "cg-partition-test-topic";
const CONSUMER_GROUP_NAME: &str = "cg-partition-test-group";
const PARTITIONS_COUNT: u32 = 3;

async fn create_stale_tcp_client(server_addr: &str) -> IggyClient {
    let config = TcpClientConfig {
        server_address: server_addr.to_string(),
        heartbeat_interval: IggyDuration::from_str("1h").unwrap(),
        nodelay: true,
        ..TcpClientConfig::default()
    };
    let client = TcpClient::create(Arc::new(config)).unwrap();
    Client::connect(&client).await.unwrap();
    IggyClient::create(ClientWrapper::Tcp(client), None, None)
}

async fn create_tcp_client(server_addr: &str) -> IggyClient {
    let config = TcpClientConfig {
        server_address: server_addr.to_string(),
        heartbeat_interval: IggyDuration::from_str("500ms").unwrap(),
        nodelay: true,
        ..TcpClientConfig::default()
    };
    let client = TcpClient::create(Arc::new(config)).unwrap();
    Client::connect(&client).await.unwrap();
    IggyClient::create(ClientWrapper::Tcp(client), None, None)
}

#[iggy_harness(server(
    heartbeat.enabled = true,
    heartbeat.interval = "2s",
    tcp.socket.override_defaults = true,
    tcp.socket.nodelay = true
))]
async fn should_not_duplicate_partition_assignments_after_stale_client_cleanup(
    harness: &TestHarness,
) {
    let server_addr = harness.server().raw_tcp_addr().unwrap();

    let root_client = create_tcp_client(&server_addr).await;
    root_client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();

    // 1. Create stream, topic with 3 partitions, consumer group
    root_client.create_stream(STREAM_NAME).await.unwrap();
    root_client
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
    root_client
        .create_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            CONSUMER_GROUP_NAME,
        )
        .await
        .unwrap();

    // 2. Send a message to each partition
    for partition_id in 0..PARTITIONS_COUNT {
        let message = IggyMessage::from_str(&format!("message-partition-{partition_id}")).unwrap();
        let mut messages = vec![message];
        root_client
            .send_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                &Partitioning::partition_id(partition_id),
                &mut messages,
            )
            .await
            .unwrap();
    }

    // 3. Create 3 "stale" TCP clients (1h heartbeat - server will detect them as stale
    //    after ~2.4s because they won't send any heartbeat).
    let stale_client1 = create_stale_tcp_client(&server_addr).await;
    let stale_client2 = create_stale_tcp_client(&server_addr).await;
    let stale_client3 = create_stale_tcp_client(&server_addr).await;

    for client in [&stale_client1, &stale_client2, &stale_client3] {
        client
            .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
            .await
            .unwrap();
        client
            .join_consumer_group(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                &Identifier::named(CONSUMER_GROUP_NAME).unwrap(),
            )
            .await
            .unwrap();
    }

    // 4. Verify initial state: 3 members, each with 1 unique partition
    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 3, "Expected 3 members before kill");
    assert_unique_partition_assignments(&cg);

    // 5. Poll a message from stale_client1 WITHOUT committing offset (simulating in-flight work)
    let consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());
    let polled = stale_client1
        .poll_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            None,
            &consumer,
            &PollingStrategy::next(),
            1,
            false, // manual ack - offset NOT stored
        )
        .await
        .unwrap();
    assert!(
        !polled.messages.is_empty(),
        "stale_client1 should have polled at least one message"
    );

    // 6. DO NOT drop stale clients - simulating kill -9 (no TCP FIN).
    //    We keep them alive in scope but won't use them again.
    //    The server will detect them as stale after ~2.4s (heartbeat timeout).

    // 7. Wait for the server's heartbeat verifier to evict the stale clients.
    //    Server heartbeat interval = 2s, threshold = 2s * 1.2 = 2.4s.
    //    Stale clients' heartbeat interval is 1h so they won't ping.
    //    But they DID send one initial ping on connect, so we wait for that to expire.
    //    Give it 5s to be safe.
    sleep(Duration::from_secs(5)).await;

    // 8. Verify ghosts have been evicted
    let cg = get_consumer_group(&root_client).await;
    assert_eq!(
        cg.members_count, 0,
        "Expected 0 members after heartbeat eviction of stale clients, got {}. Members: {:?}",
        cg.members_count, cg.members
    );

    // 9. Now create 3 new clients and join same CG (simulating app restart after kill -9).
    let client1 = create_tcp_client(&server_addr).await;
    let client2 = create_tcp_client(&server_addr).await;
    let client3 = create_tcp_client(&server_addr).await;

    for client in [&client1, &client2, &client3] {
        client
            .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
            .await
            .unwrap();
        client
            .join_consumer_group(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                &Identifier::named(CONSUMER_GROUP_NAME).unwrap(),
            )
            .await
            .unwrap();
    }

    // 10. Verify exactly 3 members with unique partition assignments
    let cg = get_consumer_group(&root_client).await;
    assert_eq!(
        cg.members_count, 3,
        "Expected 3 members after new clients join, got {}. Members: {:?}",
        cg.members_count, cg.members
    );
    assert_unique_partition_assignments(&cg);

    // 11. Verify each new client can poll from its assigned partition
    let consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());
    let mut polled_partitions = HashSet::new();

    for (i, client) in [&client1, &client2, &client3].iter().enumerate() {
        let polled = client
            .poll_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                None,
                &consumer,
                &PollingStrategy::offset(0),
                10,
                false,
            )
            .await
            .unwrap();
        assert!(
            !polled.messages.is_empty(),
            "client{} should have messages but got none",
            i + 1
        );
        assert!(
            polled_partitions.insert(polled.partition_id),
            "client{} got partition {} which was already assigned to another client! \
             Duplicate partition assignment detected.",
            i + 1,
            polled.partition_id
        );
    }

    assert_eq!(
        polled_partitions.len(),
        PARTITIONS_COUNT as usize,
        "Expected each client to poll from a unique partition"
    );

    // Cleanup
    drop(stale_client1);
    drop(stale_client2);
    drop(stale_client3);

    root_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic], server(
    heartbeat.enabled = true,
    heartbeat.interval = "2s",
    tcp.socket.override_defaults = true,
    tcp.socket.nodelay = true
))]
async fn should_not_reshuffle_partitions_when_new_member_joins(harness: &TestHarness) {
    let root_client = harness.root_client().await.unwrap();

    // 1. Create stream, topic with 3 partitions, consumer group
    setup_stream_topic_cg(&root_client).await;

    // 2. Create 2 clients, join CG → each gets partitions via incremental assign
    let client1 = harness.new_client().await.unwrap();
    let client2 = harness.new_client().await.unwrap();

    for client in [&client1, &client2] {
        client
            .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
            .await
            .unwrap();
        join_cg(client).await;
    }

    // 3. Record the current partition assignments
    let cg_before = get_consumer_group(&root_client).await;
    assert_eq!(cg_before.members_count, 2);

    let assignments_before: Vec<(u32, Vec<u32>)> = cg_before
        .members
        .iter()
        .map(|m| (m.id, m.partitions.clone()))
        .collect();

    // 4. A 3rd client joins
    let client3 = harness.new_client().await.unwrap();
    client3
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client3).await;

    // 5. Verify: members that had exactly 1 partition still have it (stable assignment).
    //    Members that were over-assigned may have given up excess partitions - that's expected.
    let cg_after = get_consumer_group(&root_client).await;
    assert_eq!(cg_after.members_count, 3);

    for (old_id, old_partitions) in &assignments_before {
        let member = cg_after.members.iter().find(|m| m.id == *old_id).unwrap();
        // A member with 1 partition must keep it (no reshuffling mid-processing)
        if old_partitions.len() == 1 {
            assert_eq!(
                &member.partitions, old_partitions,
                "Member {old_id} had exactly 1 partition {old_partitions:?} but it changed to {:?}. \
                 Single-partition assignments must be stable when new members join!",
                member.partitions
            );
        }
        // A member with multiple partitions may give up excess, but must keep at least 1
        if !old_partitions.is_empty() {
            assert!(
                !member.partitions.is_empty(),
                "Member {old_id} had partitions {old_partitions:?} but lost all of them. \
                 Existing members must keep at least their fair share."
            );
        }
    }

    // 6. Verify all partitions are assigned uniquely
    assert_unique_partition_assignments(&cg_after);

    // Cleanup
    root_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic], server(
    heartbeat.enabled = true,
    heartbeat.interval = "2s",
    tcp.socket.override_defaults = true,
    tcp.socket.nodelay = true
))]
async fn should_skip_revoked_partitions_in_round_robin(harness: &TestHarness) {
    let root_client = harness.root_client().await.unwrap();
    setup_stream_topic_cg(&root_client).await;
    send_one_message_per_partition(&root_client).await;

    let client1 = harness.new_client().await.unwrap();
    client1
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client1).await;

    let consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());

    let mut polled_before = HashSet::new();
    for _ in 0..PARTITIONS_COUNT {
        let polled = client1
            .poll_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                None,
                &consumer,
                &PollingStrategy::next(),
                1,
                false,
            )
            .await
            .unwrap();
        assert_eq!(polled.messages.len(), 1);
        assert!(polled_before.insert(polled.partition_id));
    }
    assert_eq!(polled_before.len(), PARTITIONS_COUNT as usize);

    let client2 = harness.new_client().await.unwrap();
    client2
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client2).await;
    sleep(Duration::from_millis(100)).await;

    // Consumer1 polls again — round-robin skips revoked partitions
    let mut polled_after = HashSet::new();
    for _ in 0..PARTITIONS_COUNT {
        let polled = client1
            .poll_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                None,
                &consumer,
                &PollingStrategy::next(),
                1,
                false,
            )
            .await
            .unwrap();
        if !polled.messages.is_empty() {
            polled_after.insert(polled.partition_id);
        }
    }

    assert!(
        polled_after.len() < PARTITIONS_COUNT as usize,
        "Consumer1 should skip revoked partitions but got all {PARTITIONS_COUNT}: {polled_after:?}"
    );
    assert!(
        !polled_after.is_empty(),
        "Consumer1 must keep at least 1 partition"
    );

    root_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic], server(
    heartbeat.enabled = true,
    heartbeat.interval = "2s",
    tcp.socket.override_defaults = true,
    tcp.socket.nodelay = true
))]
async fn should_not_lose_messages_with_concurrent_polls_during_partition_add(
    harness: &TestHarness,
) {
    let root_client = harness.root_client().await.unwrap();

    setup_stream_topic_cg(&root_client).await;

    // Send 10 messages per partition
    for partition_id in 0..PARTITIONS_COUNT {
        for i in 0..10u32 {
            let message = IggyMessage::from_str(&format!("p{partition_id}-msg{i}")).unwrap();
            let mut messages = vec![message];
            root_client
                .send_messages(
                    &Identifier::named(STREAM_NAME).unwrap(),
                    &Identifier::named(TOPIC_NAME).unwrap(),
                    &Partitioning::partition_id(partition_id),
                    &mut messages,
                )
                .await
                .unwrap();
        }
    }

    // 1. Two consumers join
    let client1 = Arc::new(harness.new_client().await.unwrap());
    let client2 = Arc::new(harness.new_client().await.unwrap());
    for client in [&*client1, &*client2] {
        client
            .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
            .await
            .unwrap();
        join_cg(client).await;
    }

    sleep(Duration::from_millis(200)).await;

    // 2. Start concurrent poll loops
    let stop = Arc::new(AtomicBool::new(false));
    let total_polled_1 = Arc::new(AtomicU64::new(0));
    let total_polled_2 = Arc::new(AtomicU64::new(0));

    let poll_task_1 = {
        let client = client1.clone();
        let stop = stop.clone();
        let count = total_polled_1.clone();
        tokio::spawn(async move {
            let consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());
            while !stop.load(Ordering::Relaxed) {
                let polled = client
                    .poll_messages(
                        &Identifier::named(STREAM_NAME).unwrap(),
                        &Identifier::named(TOPIC_NAME).unwrap(),
                        None,
                        &consumer,
                        &PollingStrategy::next(),
                        1,
                        true,
                    )
                    .await;
                if let Ok(polled) = polled {
                    count.fetch_add(polled.messages.len() as u64, Ordering::Relaxed);
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
    };

    let poll_task_2 = {
        let client = client2.clone();
        let stop = stop.clone();
        let count = total_polled_2.clone();
        tokio::spawn(async move {
            let consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());
            while !stop.load(Ordering::Relaxed) {
                let polled = client
                    .poll_messages(
                        &Identifier::named(STREAM_NAME).unwrap(),
                        &Identifier::named(TOPIC_NAME).unwrap(),
                        None,
                        &consumer,
                        &PollingStrategy::next(),
                        1,
                        true,
                    )
                    .await;
                if let Ok(polled) = polled {
                    count.fetch_add(polled.messages.len() as u64, Ordering::Relaxed);
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
    };

    // 3. While polling, add 3 partitions
    sleep(Duration::from_millis(100)).await;
    root_client
        .create_partitions(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            3,
        )
        .await
        .unwrap();

    // Send messages to new partitions
    for partition_id in PARTITIONS_COUNT..PARTITIONS_COUNT + 3 {
        for i in 0..10u32 {
            let message = IggyMessage::from_str(&format!("p{partition_id}-msg{i}")).unwrap();
            let mut messages = vec![message];
            root_client
                .send_messages(
                    &Identifier::named(STREAM_NAME).unwrap(),
                    &Identifier::named(TOPIC_NAME).unwrap(),
                    &Partitioning::partition_id(partition_id),
                    &mut messages,
                )
                .await
                .unwrap();
        }
    }

    // Let polling continue for a bit after partition add
    sleep(Duration::from_secs(1)).await;
    stop.store(true, Ordering::Relaxed);
    let _ = poll_task_1.await;
    let _ = poll_task_2.await;

    // 4. Verify: no duplicates, all partitions assigned
    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 2);
    assert_unique_partition_assignments(&cg);
    let total: u32 = cg.members.iter().map(|m| m.partitions_count).sum();
    assert_eq!(
        total, 6,
        "All 6 partitions must be assigned. Members: {:?}",
        cg.members
    );

    // Both consumers should have polled something
    let p1 = total_polled_1.load(Ordering::Relaxed);
    let p2 = total_polled_2.load(Ordering::Relaxed);
    assert!(
        p1 + p2 > 0,
        "Consumers should have polled at least some messages"
    );

    // Cleanup
    root_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic], server(
    heartbeat.enabled = true,
    heartbeat.interval = "2s",
    tcp.socket.override_defaults = true,
    tcp.socket.nodelay = true
))]
async fn should_handle_partition_add_then_consumer_disconnect_then_new_join(harness: &TestHarness) {
    let root_client = harness.root_client().await.unwrap();

    setup_stream_topic_cg(&root_client).await;
    send_one_message_per_partition(&root_client).await;

    // 1. Two consumers join
    let client1 = harness.new_client().await.unwrap();
    let client2 = harness.new_client().await.unwrap();
    for client in [&client1, &client2] {
        client
            .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
            .await
            .unwrap();
        join_cg(client).await;
    }

    sleep(Duration::from_millis(200)).await;

    // Both poll and auto-commit
    let consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());
    for client in [&client1, &client2] {
        let _ = client
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
    }

    // 2. Add 3 partitions (3 → 6)
    root_client
        .create_partitions(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            3,
        )
        .await
        .unwrap();

    for partition_id in PARTITIONS_COUNT..PARTITIONS_COUNT + 3 {
        let message = IggyMessage::from_str(&format!("new-p{partition_id}")).unwrap();
        let mut messages = vec![message];
        root_client
            .send_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                &Partitioning::partition_id(partition_id),
                &mut messages,
            )
            .await
            .unwrap();
    }

    sleep(Duration::from_millis(500)).await;

    // 3. Consumer2 disconnects
    drop(client2);
    sleep(Duration::from_millis(500)).await;

    let cg = get_consumer_group(&root_client).await;
    assert_eq!(
        cg.members_count, 1,
        "Only consumer1 should remain. Members: {:?}",
        cg.members
    );
    assert_eq!(
        cg.members[0].partitions_count, 6,
        "Consumer1 should have all 6 partitions after consumer2 left. Members: {:?}",
        cg.members
    );

    // 4. New consumer3 joins
    let client3 = harness.new_client().await.unwrap();
    client3
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client3).await;

    sleep(Duration::from_millis(500)).await;

    // 5. Verify: both members have partitions, no duplicates
    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 2);
    assert_unique_partition_assignments(&cg);
    let total: u32 = cg.members.iter().map(|m| m.partitions_count).sum();
    assert_eq!(
        total, 6,
        "All 6 partitions must be assigned. Members: {:?}",
        cg.members
    );
    for member in &cg.members {
        assert!(
            member.partitions_count >= 1,
            "Both members must have partitions. Members: {:?}",
            cg.members
        );
    }

    // 6. Both can poll unique partitions
    let mut polled_partitions = HashSet::new();
    for client in [&client1, &client3] {
        let polled = client
            .poll_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                None,
                &consumer,
                &PollingStrategy::offset(0),
                1,
                false,
            )
            .await
            .unwrap();
        if !polled.messages.is_empty() {
            assert!(
                polled_partitions.insert(polled.partition_id),
                "Duplicate partition {} after disconnect + rejoin!",
                polled.partition_id
            );
        }
    }
    assert_eq!(
        polled_partitions.len(),
        2,
        "Both consumers should poll from unique partitions"
    );

    // Cleanup
    root_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic], server(
    heartbeat.enabled = true,
    heartbeat.interval = "2s",
    tcp.socket.override_defaults = true,
    tcp.socket.nodelay = true
))]
async fn should_handle_partition_delete_while_multiple_consumers_polling(harness: &TestHarness) {
    let root_client = harness.root_client().await.unwrap();

    // Start with 6 partitions
    root_client.create_stream(STREAM_NAME).await.unwrap();
    root_client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            6,
            CompressionAlgorithm::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();
    root_client
        .create_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            CONSUMER_GROUP_NAME,
        )
        .await
        .unwrap();

    // Send messages to all 6 partitions
    for partition_id in 0..6u32 {
        for i in 0..5u32 {
            let message = IggyMessage::from_str(&format!("p{partition_id}-m{i}")).unwrap();
            let mut messages = vec![message];
            root_client
                .send_messages(
                    &Identifier::named(STREAM_NAME).unwrap(),
                    &Identifier::named(TOPIC_NAME).unwrap(),
                    &Partitioning::partition_id(partition_id),
                    &mut messages,
                )
                .await
                .unwrap();
        }
    }

    // 1. Three consumers join (6 partitions / 3 = 2 each)
    let client1 = Arc::new(harness.new_client().await.unwrap());
    let client2 = Arc::new(harness.new_client().await.unwrap());
    let client3 = Arc::new(harness.new_client().await.unwrap());
    for client in [&*client1, &*client2, &*client3] {
        client
            .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
            .await
            .unwrap();
        join_cg(client).await;
    }

    sleep(Duration::from_millis(200)).await;

    // 2. Start concurrent poll loops
    let stop = Arc::new(AtomicBool::new(false));
    let consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());

    let mut poll_tasks = Vec::new();
    for client in [client1.clone(), client2.clone(), client3.clone()] {
        let stop = stop.clone();
        let consumer = consumer.clone();
        poll_tasks.push(tokio::spawn(async move {
            let mut count = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let polled = client
                    .poll_messages(
                        &Identifier::named(STREAM_NAME).unwrap(),
                        &Identifier::named(TOPIC_NAME).unwrap(),
                        None,
                        &consumer,
                        &PollingStrategy::next(),
                        1,
                        true,
                    )
                    .await;
                if let Ok(p) = polled {
                    count += p.messages.len() as u64;
                }
                sleep(Duration::from_millis(10)).await;
            }
            count
        }));
    }

    // 3. While all 3 consumers are polling, delete 3 partitions (6 → 3)
    sleep(Duration::from_millis(200)).await;
    root_client
        .delete_partitions(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            3,
        )
        .await
        .unwrap();

    // Let polling continue after delete
    sleep(Duration::from_secs(1)).await;
    stop.store(true, Ordering::Relaxed);
    for task in poll_tasks {
        let _ = task.await;
    }

    // 4. Verify: 3 members, 3 remaining partitions, no duplicates
    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 3);
    assert_unique_partition_assignments(&cg);
    let total: u32 = cg.members.iter().map(|m| m.partitions_count).sum();
    assert_eq!(
        total, 3,
        "Only 3 partitions should remain. Members: {:?}",
        cg.members
    );
    for member in &cg.members {
        assert_eq!(
            member.partitions_count, 1,
            "Each of 3 members should have exactly 1 of 3 remaining partitions. Members: {:?}",
            cg.members
        );
    }

    // Cleanup
    root_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic], server(
    heartbeat.enabled = true,
    heartbeat.interval = "2s",
    tcp.socket.override_defaults = true,
    tcp.socket.nodelay = true
))]
async fn should_reach_even_distribution_after_multiple_joins(harness: &TestHarness) {
    let root_client = harness.root_client().await.unwrap();

    // Start with 6 partitions for cleaner math
    root_client.create_stream(STREAM_NAME).await.unwrap();
    root_client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            6,
            CompressionAlgorithm::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();
    root_client
        .create_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            CONSUMER_GROUP_NAME,
        )
        .await
        .unwrap();

    for partition_id in 0..6u32 {
        let message = IggyMessage::from_str(&format!("msg-p{partition_id}")).unwrap();
        let mut messages = vec![message];
        root_client
            .send_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                &Partitioning::partition_id(partition_id),
                &mut messages,
            )
            .await
            .unwrap();
    }

    // 1. Consumer1 joins — gets all 6 partitions
    let client1 = harness.new_client().await.unwrap();
    client1
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client1).await;

    sleep(Duration::from_millis(100)).await;
    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members[0].partitions_count, 6);

    // 2. Consumer2 joins — cooperative rebalance moves some partitions
    //    (never polled → immediate completion)
    let client2 = harness.new_client().await.unwrap();
    client2
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client2).await;

    sleep(Duration::from_millis(200)).await;
    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 2);
    assert_unique_partition_assignments(&cg);
    let total: u32 = cg.members.iter().map(|m| m.partitions_count).sum();
    assert_eq!(total, 6);

    // 3. Consumer3 joins — another round of cooperative rebalance
    let client3 = harness.new_client().await.unwrap();
    client3
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client3).await;

    sleep(Duration::from_millis(200)).await;
    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 3);
    assert_unique_partition_assignments(&cg);
    let total: u32 = cg.members.iter().map(|m| m.partitions_count).sum();
    assert_eq!(total, 6);
    for member in &cg.members {
        assert!(
            member.partitions_count >= 1,
            "Every member must have at least 1 partition. Members: {:?}",
            cg.members
        );
    }

    // 4. Consumer4 joins — with 6 partitions and 4 members, cooperative rebalance
    //    should give member4 at least 1 partition (fair_share=1, remainder=2)
    let client4 = harness.new_client().await.unwrap();
    client4
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client4).await;

    sleep(Duration::from_millis(200)).await;
    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 4);
    assert_unique_partition_assignments(&cg);
    let total: u32 = cg.members.iter().map(|m| m.partitions_count).sum();
    assert_eq!(total, 6);
    for member in &cg.members {
        assert!(
            member.partitions_count >= 1,
            "Every member must have at least 1 partition with 4 members and 6 partitions. Members: {:?}",
            cg.members
        );
    }

    // 5. Consumer5 + Consumer6 join — 6 partitions, 6 consumers → exactly 1 each
    let client5 = harness.new_client().await.unwrap();
    let client6 = harness.new_client().await.unwrap();
    for client in [&client5, &client6] {
        client
            .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
            .await
            .unwrap();
        join_cg(client).await;
        sleep(Duration::from_millis(200)).await;
    }

    sleep(Duration::from_millis(200)).await;
    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 6);
    assert_unique_partition_assignments(&cg);
    let total: u32 = cg.members.iter().map(|m| m.partitions_count).sum();
    assert_eq!(total, 6);
    for member in &cg.members {
        assert_eq!(
            member.partitions_count, 1,
            "With 6 members and 6 partitions, each must have exactly 1. Members: {:?}",
            cg.members
        );
    }

    // 6. All 6 consumers poll — must get unique partitions
    let consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());
    let mut polled_partitions = HashSet::new();
    for client in [&client1, &client2, &client3, &client4, &client5, &client6] {
        let polled = client
            .poll_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                None,
                &consumer,
                &PollingStrategy::offset(0),
                10,
                false,
            )
            .await
            .unwrap();
        if !polled.messages.is_empty() {
            assert!(
                polled_partitions.insert(polled.partition_id),
                "Duplicate partition {} with 6 consumers on 6 partitions!",
                polled.partition_id
            );
        }
    }
    assert_eq!(
        polled_partitions.len(),
        6,
        "All 6 partitions must be covered by 6 consumers"
    );

    // Cleanup
    root_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic], server(
    heartbeat.enabled = true,
    heartbeat.interval = "2s",
    tcp.socket.override_defaults = true,
    tcp.socket.nodelay = true
))]
async fn should_split_evenly_when_consumer_joins_after_partitions_added(harness: &TestHarness) {
    let root_client = harness.root_client().await.unwrap();

    setup_stream_topic_cg(&root_client).await;
    send_one_message_per_partition(&root_client).await;

    // 1. Two consumers join, split 3 partitions (2+1)
    let client1 = harness.new_client().await.unwrap();
    let client2 = harness.new_client().await.unwrap();
    for client in [&client1, &client2] {
        client
            .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
            .await
            .unwrap();
        join_cg(client).await;
    }

    sleep(Duration::from_millis(200)).await;
    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 2);
    let total: u32 = cg.members.iter().map(|m| m.partitions_count).sum();
    assert_eq!(total, 3);

    // 2. Add 3 partitions (3 -> 6), triggers full rebalance (3 each)
    root_client
        .create_partitions(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            3,
        )
        .await
        .unwrap();

    sleep(Duration::from_millis(500)).await;

    // 3. Send messages to new partitions
    for partition_id in PARTITIONS_COUNT..PARTITIONS_COUNT + 3 {
        let message =
            IggyMessage::from_str(&format!("message-new-partition-{partition_id}")).unwrap();
        let mut messages = vec![message];
        root_client
            .send_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                &Partitioning::partition_id(partition_id),
                &mut messages,
            )
            .await
            .unwrap();
    }

    // 4. Consumer3 joins — now 3 consumers, 6 partitions → 2 each
    let client3 = harness.new_client().await.unwrap();
    client3
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client3).await;

    sleep(Duration::from_millis(500)).await;

    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 3);
    assert_unique_partition_assignments(&cg);
    let total: u32 = cg.members.iter().map(|m| m.partitions_count).sum();
    assert_eq!(
        total, 6,
        "All 6 partitions must be assigned. Members: {:?}",
        cg.members
    );
    for member in &cg.members {
        assert!(
            member.partitions_count >= 1,
            "Every member must have at least 1 partition after cooperative rebalance. Members: {:?}",
            cg.members
        );
    }

    // 5. Each consumer polls — must get unique partitions, no duplicates
    let consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());
    let mut polled_partitions = HashSet::new();
    for client in [&client1, &client2, &client3] {
        let polled = client
            .poll_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                None,
                &consumer,
                &PollingStrategy::offset(0),
                10,
                false,
            )
            .await
            .unwrap();
        if !polled.messages.is_empty() {
            assert!(
                polled_partitions.insert(polled.partition_id),
                "Duplicate partition {} polled by multiple consumers!",
                polled.partition_id
            );
        }
    }
    assert_eq!(
        polled_partitions.len(),
        3,
        "All 3 consumers should poll from unique partitions"
    );

    // Cleanup
    root_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic], server(
    heartbeat.enabled = true,
    heartbeat.interval = "2s",
    tcp.socket.override_defaults = true,
    tcp.socket.nodelay = true
))]
async fn should_not_duplicate_messages_when_partitions_added_during_polling(harness: &TestHarness) {
    let root_client = harness.root_client().await.unwrap();

    setup_stream_topic_cg(&root_client).await;
    send_one_message_per_partition(&root_client).await;

    // 1. Two consumers join and start polling
    let client1 = harness.new_client().await.unwrap();
    let client2 = harness.new_client().await.unwrap();
    for client in [&client1, &client2] {
        client
            .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
            .await
            .unwrap();
        join_cg(client).await;
    }

    let consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());

    // 2. Both consumers poll without committing (in-flight work)
    for client in [&client1, &client2] {
        let _ = client
            .poll_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                None,
                &consumer,
                &PollingStrategy::next(),
                1,
                false,
            )
            .await
            .unwrap();
    }

    // 3. Add partitions WHILE consumers are active with polled-but-uncommitted data
    root_client
        .create_partitions(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            3,
        )
        .await
        .unwrap();

    sleep(Duration::from_millis(500)).await;

    // 4. After rebalance, verify no partition is assigned to both consumers
    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 2);
    assert_unique_partition_assignments(&cg);
    let total: u32 = cg.members.iter().map(|m| m.partitions_count).sum();
    assert_eq!(
        total, 6,
        "All 6 partitions must be assigned. Members: {:?}",
        cg.members
    );

    // 5. Both consumers poll again — must not get same partition
    let mut seen_by_client: Vec<HashSet<u32>> = Vec::new();
    for client in [&client1, &client2] {
        let mut client_partitions = HashSet::new();
        for _ in 0..3 {
            let polled = client
                .poll_messages(
                    &Identifier::named(STREAM_NAME).unwrap(),
                    &Identifier::named(TOPIC_NAME).unwrap(),
                    None,
                    &consumer,
                    &PollingStrategy::offset(0),
                    1,
                    false,
                )
                .await
                .unwrap();
            if !polled.messages.is_empty() {
                client_partitions.insert(polled.partition_id);
            }
        }
        seen_by_client.push(client_partitions);
    }
    let overlap: HashSet<_> = seen_by_client[0].intersection(&seen_by_client[1]).collect();
    assert!(
        overlap.is_empty(),
        "Consumers polled overlapping partitions after rebalance: {:?}",
        overlap
    );

    // Cleanup
    root_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic], server(
    heartbeat.enabled = true,
    heartbeat.interval = "2s",
    tcp.socket.override_defaults = true,
    tcp.socket.nodelay = true
))]
async fn should_handle_delete_partitions_with_uncommitted_work(harness: &TestHarness) {
    let root_client = harness.root_client().await.unwrap();

    // Create with 6 partitions
    root_client.create_stream(STREAM_NAME).await.unwrap();
    root_client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            6,
            CompressionAlgorithm::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();
    root_client
        .create_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            CONSUMER_GROUP_NAME,
        )
        .await
        .unwrap();

    // Send messages to all 6 partitions
    for partition_id in 0..6u32 {
        let message = IggyMessage::from_str(&format!("msg-{partition_id}")).unwrap();
        let mut messages = vec![message];
        root_client
            .send_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                &Partitioning::partition_id(partition_id),
                &mut messages,
            )
            .await
            .unwrap();
    }

    // 1. Two consumers join, each gets 3 partitions
    let client1 = harness.new_client().await.unwrap();
    let client2 = harness.new_client().await.unwrap();
    for client in [&client1, &client2] {
        client
            .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
            .await
            .unwrap();
        join_cg(client).await;
    }

    // 2. Both consumers poll without committing
    let consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());
    for client in [&client1, &client2] {
        let _ = client
            .poll_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                None,
                &consumer,
                &PollingStrategy::next(),
                1,
                false,
            )
            .await
            .unwrap();
    }

    // 3. Delete 3 partitions while consumers have uncommitted work
    root_client
        .delete_partitions(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            3,
        )
        .await
        .unwrap();

    sleep(Duration::from_millis(500)).await;

    // 4. After rebalance: 3 remaining partitions, no duplicates, even split
    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 2);
    assert_unique_partition_assignments(&cg);
    let total: u32 = cg.members.iter().map(|m| m.partitions_count).sum();
    assert_eq!(
        total, 3,
        "3 remaining partitions must be assigned. Members: {:?}",
        cg.members
    );
    for member in &cg.members {
        assert!(
            member.partitions_count > 0,
            "Every member should have at least 1 partition. Members: {:?}",
            cg.members
        );
    }

    // 5. Consumers can still poll from remaining partitions without error
    for client in [&client1, &client2] {
        let polled = client
            .poll_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                None,
                &consumer,
                &PollingStrategy::offset(0),
                1,
                false,
            )
            .await
            .unwrap();
        assert!(
            !polled.messages.is_empty(),
            "Consumer should still be able to poll from remaining partitions"
        );
    }

    // Cleanup
    root_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic], server(
    heartbeat.enabled = true,
    heartbeat.interval = "2s",
    tcp.socket.override_defaults = true,
    tcp.socket.nodelay = true
))]
async fn should_handle_rapid_partition_changes_with_active_consumers(harness: &TestHarness) {
    let root_client = harness.root_client().await.unwrap();

    setup_stream_topic_cg(&root_client).await;
    send_one_message_per_partition(&root_client).await;

    // 1. Two consumers join
    let client1 = harness.new_client().await.unwrap();
    let client2 = harness.new_client().await.unwrap();
    for client in [&client1, &client2] {
        client
            .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
            .await
            .unwrap();
        join_cg(client).await;
    }

    sleep(Duration::from_millis(200)).await;

    // 2. Add partitions, then add more, then consumer3 joins — rapid changes
    root_client
        .create_partitions(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            3,
        )
        .await
        .unwrap();

    // Send messages to new partitions
    for partition_id in 3..6u32 {
        let message = IggyMessage::from_str(&format!("msg-{partition_id}")).unwrap();
        let mut messages = vec![message];
        root_client
            .send_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                &Partitioning::partition_id(partition_id),
                &mut messages,
            )
            .await
            .unwrap();
    }

    root_client
        .create_partitions(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            3,
        )
        .await
        .unwrap();

    // Send messages to newest partitions
    for partition_id in 6..9u32 {
        let message = IggyMessage::from_str(&format!("msg-{partition_id}")).unwrap();
        let mut messages = vec![message];
        root_client
            .send_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                &Partitioning::partition_id(partition_id),
                &mut messages,
            )
            .await
            .unwrap();
    }

    let client3 = harness.new_client().await.unwrap();
    client3
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client3).await;

    sleep(Duration::from_millis(500)).await;

    // 3. 9 partitions, 3 consumers → 3 each
    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 3);
    assert_unique_partition_assignments(&cg);
    let total: u32 = cg.members.iter().map(|m| m.partitions_count).sum();
    assert_eq!(
        total, 9,
        "All 9 partitions must be assigned. Members: {:?}",
        cg.members
    );
    for member in &cg.members {
        assert!(
            member.partitions_count >= 1,
            "Every member must have at least 1 partition after cooperative rebalance. Members: {:?}",
            cg.members
        );
    }

    // 4. All consumers poll — unique partitions only
    let consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());
    let mut polled_partitions = HashSet::new();
    for client in [&client1, &client2, &client3] {
        let polled = client
            .poll_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                None,
                &consumer,
                &PollingStrategy::offset(0),
                10,
                false,
            )
            .await
            .unwrap();
        if !polled.messages.is_empty() {
            assert!(
                polled_partitions.insert(polled.partition_id),
                "Duplicate partition {} after rapid changes!",
                polled.partition_id
            );
        }
    }
    assert_eq!(
        polled_partitions.len(),
        3,
        "All 3 consumers should poll from unique partitions"
    );

    // Cleanup
    root_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic], server(
    heartbeat.enabled = true,
    heartbeat.interval = "2s",
    tcp.socket.override_defaults = true,
    tcp.socket.nodelay = true
))]
async fn should_rebalance_after_adding_partitions(harness: &TestHarness) {
    let root_client = harness.root_client().await.unwrap();

    setup_stream_topic_cg(&root_client).await;
    send_one_message_per_partition(&root_client).await;

    // 1. Three consumers join, each gets 1 of 3 partitions
    let client1 = harness.new_client().await.unwrap();
    let client2 = harness.new_client().await.unwrap();
    let client3 = harness.new_client().await.unwrap();
    for client in [&client1, &client2, &client3] {
        client
            .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
            .await
            .unwrap();
        join_cg(client).await;
    }

    sleep(Duration::from_millis(200)).await;

    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 3);
    assert_unique_partition_assignments(&cg);
    for member in &cg.members {
        assert_eq!(
            member.partitions_count, 1,
            "Each member should have 1 partition before adding. Members: {:?}",
            cg.members
        );
    }

    // 2. Add 3 more partitions (3 -> 6 total)
    root_client
        .create_partitions(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            3,
        )
        .await
        .unwrap();

    sleep(Duration::from_millis(500)).await;

    // 3. After rebalance, each consumer should have 2 partitions (6/3)
    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 3);
    assert_unique_partition_assignments(&cg);
    let total: u32 = cg.members.iter().map(|m| m.partitions_count).sum();
    assert_eq!(
        total, 6,
        "All 6 partitions must be assigned. Members: {:?}",
        cg.members
    );
    for member in &cg.members {
        assert_eq!(
            member.partitions_count, 2,
            "Each member should have 2 partitions after adding 3 more. Members: {:?}",
            cg.members
        );
    }

    // Cleanup
    root_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic], server(
    heartbeat.enabled = true,
    heartbeat.interval = "2s",
    tcp.socket.override_defaults = true,
    tcp.socket.nodelay = true
))]
async fn should_rebalance_after_deleting_partitions(harness: &TestHarness) {
    let root_client = harness.root_client().await.unwrap();

    // Create with 6 partitions
    root_client.create_stream(STREAM_NAME).await.unwrap();
    root_client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            6,
            CompressionAlgorithm::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();
    root_client
        .create_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            CONSUMER_GROUP_NAME,
        )
        .await
        .unwrap();

    // 1. Three consumers join, each gets 2 of 6 partitions
    let client1 = harness.new_client().await.unwrap();
    let client2 = harness.new_client().await.unwrap();
    let client3 = harness.new_client().await.unwrap();
    for client in [&client1, &client2, &client3] {
        client
            .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
            .await
            .unwrap();
        join_cg(client).await;
    }

    sleep(Duration::from_millis(200)).await;

    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 3);
    let total: u32 = cg.members.iter().map(|m| m.partitions_count).sum();
    assert_eq!(total, 6);

    // 2. Delete 3 partitions (6 -> 3)
    root_client
        .delete_partitions(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            3,
        )
        .await
        .unwrap();

    sleep(Duration::from_millis(500)).await;

    // 3. After rebalance, each consumer should have 1 partition (3/3)
    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 3);
    assert_unique_partition_assignments(&cg);
    let total: u32 = cg.members.iter().map(|m| m.partitions_count).sum();
    assert_eq!(
        total, 3,
        "All 3 remaining partitions must be assigned. Members: {:?}",
        cg.members
    );
    for member in &cg.members {
        assert_eq!(
            member.partitions_count, 1,
            "Each member should have 1 partition after deleting 3. Members: {:?}",
            cg.members
        );
    }

    // Cleanup
    root_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic], server(
    heartbeat.enabled = true,
    heartbeat.interval = "2s",
    tcp.socket.override_defaults = true,
    tcp.socket.nodelay = true
))]
async fn should_handle_partition_add_during_pending_revocation(harness: &TestHarness) {
    let root_client = harness.root_client().await.unwrap();

    setup_stream_topic_cg(&root_client).await;
    send_one_message_per_partition(&root_client).await;

    // 1. Consumer1 joins, polls all 3 partitions without committing
    let client1 = harness.new_client().await.unwrap();
    client1
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client1).await;

    let consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());
    for _ in 0..PARTITIONS_COUNT {
        client1
            .poll_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                None,
                &consumer,
                &PollingStrategy::next(),
                1,
                false,
            )
            .await
            .unwrap();
    }

    // 2. Consumer2 joins — triggers pending revocations
    let client2 = harness.new_client().await.unwrap();
    client2
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client2).await;

    sleep(Duration::from_millis(100)).await;

    // 3. Add partitions WHILE revocations are pending
    root_client
        .create_partitions(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            3,
        )
        .await
        .unwrap();

    sleep(Duration::from_millis(500)).await;

    // 4. After full rebalance triggered by partition change,
    //    all 6 partitions should be distributed, no duplicates
    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 2);
    assert_unique_partition_assignments(&cg);
    let total: u32 = cg.members.iter().map(|m| m.partitions_count).sum();
    assert_eq!(
        total, 6,
        "All 6 partitions must be assigned after add during pending revocation. Members: {:?}",
        cg.members
    );
    for member in &cg.members {
        assert_eq!(
            member.partitions_count, 3,
            "Each of 2 members should have 3 of 6 partitions. Members: {:?}",
            cg.members
        );
    }

    // Cleanup
    root_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic], server(
    consumer_group.rebalancing_timeout = "3s",
    consumer_group.rebalancing_check_interval = "1s"
))]
async fn should_timeout_revocation(harness: &TestHarness) {
    let root_client = harness.root_client().await.unwrap();

    setup_stream_topic_cg(&root_client).await;
    send_one_message_per_partition(&root_client).await;

    // 1. Consumer1 joins, gets all 3 partitions
    let client1 = harness.new_client().await.unwrap();
    client1
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client1).await;

    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 1);
    assert_eq!(cg.members[0].partitions_count, PARTITIONS_COUNT);

    // 2. Consumer1 polls all partitions WITHOUT committing (manual ack)
    let consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());
    for _ in 0..PARTITIONS_COUNT {
        client1
            .poll_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                None,
                &consumer,
                &PollingStrategy::next(),
                1,
                false,
            )
            .await
            .unwrap();
    }

    // 3. Consumer2 joins - triggers pending revocations for 1 partition
    let client2 = harness.new_client().await.unwrap();
    client2
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client2).await;

    // 4. Verify consumer2 has no partitions yet (pending revocation, not completed)
    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 2);
    let client2_member = cg.members.iter().find(|m| m.partitions_count == 0);
    assert!(
        client2_member.is_some(),
        "Consumer2 should have 0 partitions while revocation is pending. Members: {:?}",
        cg.members
    );

    // 5. Consumer1 does NOT poll or commit — simulating idle-but-alive consumer.
    //    Wait for the revocation timeout (3s) + check interval (1s) + margin.
    sleep(Duration::from_secs(6)).await;

    // 6. The periodic checker should have force-completed the revocation.
    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 2);
    assert_unique_partition_assignments(&cg);

    let partitions_total: u32 = cg.members.iter().map(|m| m.partitions_count).sum();
    assert_eq!(
        partitions_total, PARTITIONS_COUNT,
        "All partitions must be assigned after timeout. Members: {:?}",
        cg.members
    );

    // Both members should have partitions now
    for member in &cg.members {
        assert!(
            member.partitions_count > 0,
            "Every member should have at least 1 partition after timeout. Members: {:?}",
            cg.members
        );
    }

    // 7. Consumer2 should be able to poll from its assigned partition
    let polled = client2
        .poll_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            None,
            &consumer,
            &PollingStrategy::offset(0),
            10,
            false,
        )
        .await
        .unwrap();
    assert!(
        !polled.messages.is_empty(),
        "Consumer2 should receive messages after revocation timeout"
    );

    // Cleanup
    root_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

#[iggy_harness(server(
    heartbeat.enabled = true,
    heartbeat.interval = "2s",
    tcp.socket.override_defaults = true,
    tcp.socket.nodelay = true
))]
async fn should_not_duplicate_after_reconnect_without_heartbeat(harness: &TestHarness) {
    let server_addr = harness.server().raw_tcp_addr().expect("tcp addr");
    let root_client = create_tcp_client(&server_addr).await;
    root_client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();

    // 1. Setup
    root_client.create_stream(STREAM_NAME).await.unwrap();
    root_client
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
    root_client
        .create_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            CONSUMER_GROUP_NAME,
        )
        .await
        .unwrap();

    // 2. Send 1 message to partition 1
    let mut messages = vec![IggyMessage::from_str("the-one-message").unwrap()];
    root_client
        .send_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Partitioning::partition_id(1),
            &mut messages,
        )
        .await
        .unwrap();

    let consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());

    // 3. Phase 1: 3 clients with stale heartbeat (1h) join CG, one polls, no commit
    let client1 = create_stale_tcp_client(&server_addr).await;
    let client2 = create_stale_tcp_client(&server_addr).await;
    let client3 = create_stale_tcp_client(&server_addr).await;

    for client in [&client1, &client2, &client3] {
        client
            .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
            .await
            .unwrap();
        join_cg(client).await;
    }

    // One polls the message (no commit)
    for client in [&client1, &client2, &client3] {
        let polled = client
            .poll_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                None,
                &consumer,
                &PollingStrategy::next(),
                1,
                false,
            )
            .await
            .unwrap();
        if !polled.messages.is_empty() {
            break;
        }
    }

    // 4. Wait for heartbeat to evict stale clients (1h heartbeat → server detects ~2.4s)
    sleep(Duration::from_secs(5)).await;

    let cg = get_consumer_group(&root_client).await;
    assert_eq!(
        cg.members_count, 0,
        "Stale clients should be evicted. Members: {:?}",
        cg.members
    );

    // 5. Phase 2: 3 new clients join, poll concurrently — exactly 1 must get the message
    let new_client1 = create_tcp_client(&server_addr).await;
    let new_client2 = create_tcp_client(&server_addr).await;
    let new_client3 = create_tcp_client(&server_addr).await;

    for client in [&new_client1, &new_client2, &new_client3] {
        client
            .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
            .await
            .unwrap();
        join_cg(client).await;
    }

    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 3);
    assert_unique_partition_assignments(&cg);

    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let strategy = PollingStrategy::offset(0);
    let (p1, p2, p3) = tokio::join!(
        new_client1.poll_messages(&stream, &topic, None, &consumer, &strategy, 10, false),
        new_client2.poll_messages(&stream, &topic, None, &consumer, &strategy, 10, false),
        new_client3.poll_messages(&stream, &topic, None, &consumer, &strategy, 10, false),
    );

    let mut consumers_with_message = 0u32;
    for polled in [
        p1.expect("poll 1"),
        p2.expect("poll 2"),
        p3.expect("poll 3"),
    ] {
        if !polled.messages.is_empty() {
            consumers_with_message += 1;
        }
    }

    assert_eq!(
        consumers_with_message, 1,
        "Exactly 1 consumer must get the message, but {consumers_with_message} got it!"
    );

    // Cleanup stale clients (prevent leaks)
    drop(client1);
    drop(client2);
    drop(client3);

    root_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic], server(
    tcp.socket.override_defaults = true,
    tcp.socket.nodelay = true
))]
async fn should_not_duplicate_partition_assignments_after_client_reconnect(harness: &TestHarness) {
    let root_client = harness
        .root_client()
        .await
        .expect("Failed to get root client");

    // 1. Create stream, topic with 3 partitions, consumer group
    root_client.create_stream(STREAM_NAME).await.unwrap();
    root_client
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
    root_client
        .create_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            CONSUMER_GROUP_NAME,
        )
        .await
        .unwrap();

    // 2. Send 1 message to partition 1
    let message = IggyMessage::from_str("the-one-message").unwrap();
    let mut messages = vec![message];
    root_client
        .send_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Partitioning::partition_id(1),
            &mut messages,
        )
        .await
        .unwrap();

    // 3. Create 3 clients (separate TCP connections), login, join same CG
    let client1 = harness.new_client().await.unwrap();
    let client2 = harness.new_client().await.unwrap();
    let client3 = harness.new_client().await.unwrap();

    let consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());

    for client in [&client1, &client2, &client3] {
        client
            .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
            .await
            .unwrap();
        client
            .join_consumer_group(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                &Identifier::named(CONSUMER_GROUP_NAME).unwrap(),
            )
            .await
            .unwrap();
    }

    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 3);
    assert_unique_partition_assignments(&cg);

    // 4. One consumer polls the message, manual ack — does NOT commit.
    //    Simulates: handler is mid-processing when app gets killed.
    for client in [&client1, &client2, &client3] {
        let polled = client
            .poll_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                None,
                &consumer,
                &PollingStrategy::next(),
                1,
                false, // manual ack — no commit
            )
            .await
            .unwrap();
        if !polled.messages.is_empty() {
            break; // one consumer got it, stop
        }
    }

    // 5. Kill all 3 clients (no ack sent)
    drop(client1);
    drop(client2);
    drop(client3);
    sleep(Duration::from_millis(500)).await;

    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 0);

    // 6. Restart: 3 new clients join same CG
    let new_client1 = harness.new_client().await.unwrap();
    let new_client2 = harness.new_client().await.unwrap();
    let new_client3 = harness.new_client().await.unwrap();

    for client in [&new_client1, &new_client2, &new_client3] {
        client
            .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
            .await
            .unwrap();
        client
            .join_consumer_group(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                &Identifier::named(CONSUMER_GROUP_NAME).unwrap(),
            )
            .await
            .unwrap();
    }

    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 3);
    assert_unique_partition_assignments(&cg);

    // 7. All 3 poll CONCURRENTLY — exactly ONE must get the message.
    let stream = Identifier::named(STREAM_NAME).expect("stream id");
    let topic = Identifier::named(TOPIC_NAME).expect("topic id");
    let strategy = PollingStrategy::offset(0);
    let (p1, p2, p3) = tokio::join!(
        new_client1.poll_messages(&stream, &topic, None, &consumer, &strategy, 10, false),
        new_client2.poll_messages(&stream, &topic, None, &consumer, &strategy, 10, false),
        new_client3.poll_messages(&stream, &topic, None, &consumer, &strategy, 10, false),
    );
    let mut consumers_with_message = 0u32;
    for polled in [
        p1.expect("poll 1"),
        p2.expect("poll 2"),
        p3.expect("poll 3"),
    ] {
        if !polled.messages.is_empty() {
            consumers_with_message += 1;
        }
    }

    assert_eq!(
        consumers_with_message, 1,
        "Exactly 1 consumer must get the message, but {consumers_with_message} got it!"
    );

    // Cleanup
    root_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

async fn get_consumer_group(client: &IggyClient) -> ConsumerGroupDetails {
    client
        .get_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Identifier::named(CONSUMER_GROUP_NAME).unwrap(),
        )
        .await
        .unwrap()
        .expect("Failed to get consumer group")
}

fn assert_unique_partition_assignments(cg: &ConsumerGroupDetails) {
    let mut all_partitions = HashSet::new();

    for member in &cg.members {
        for &partition in &member.partitions {
            assert!(
                all_partitions.insert(partition),
                "Partition {partition} is assigned to multiple members! \
                 This means two consumers will process the same messages. \
                 Consumer group members: {:?}",
                cg.members
            );
        }
    }
}

async fn setup_stream_topic_cg(client: &IggyClient) {
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
}

async fn send_one_message_per_partition(client: &IggyClient) {
    for partition_id in 0..PARTITIONS_COUNT {
        let message = IggyMessage::from_str(&format!("message-partition-{partition_id}")).unwrap();
        let mut messages = vec![message];
        client
            .send_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                &Partitioning::partition_id(partition_id),
                &mut messages,
            )
            .await
            .unwrap();
    }
}

async fn join_cg(client: &IggyClient) {
    client
        .join_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Identifier::named(CONSUMER_GROUP_NAME).unwrap(),
        )
        .await
        .unwrap();
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic], server(
    heartbeat.enabled = true,
    heartbeat.interval = "2s",
    tcp.socket.override_defaults = true,
    tcp.socket.nodelay = true
))]
async fn should_not_return_same_message_to_two_consumers_during_rebalance(harness: &TestHarness) {
    let root_client = harness.root_client().await.unwrap();

    setup_stream_topic_cg(&root_client).await;
    send_one_message_per_partition(&root_client).await;

    // 1. Consumer1 joins, gets all 3 partitions
    let client1 = harness.new_client().await.unwrap();
    client1
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client1).await;

    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 1);
    assert_eq!(cg.members[0].partitions_count, PARTITIONS_COUNT);

    // 2. Consumer1 polls a message WITHOUT committing (simulating in-flight processing)
    let consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());
    let polled1 = client1
        .poll_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            None,
            &consumer,
            &PollingStrategy::next(),
            1,
            false, // manual commit - offset NOT stored
        )
        .await
        .unwrap();
    assert_eq!(polled1.messages.len(), 1, "Consumer1 should get a message");
    let message_from_client1 = &polled1.messages[0];
    let partition_polled_by_client1 = polled1.partition_id;

    // 3. Consumer2 joins while consumer1 has in-flight work
    let client2 = harness.new_client().await.unwrap();
    client2
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client2).await;

    // 4. Consumer2 polls - it must NOT get the same partition as consumer1's
    //    in-flight message. Due to cooperative rebalance, the partition with
    //    in-flight work stays with consumer1 until committed.
    let polled2 = client2
        .poll_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            None,
            &consumer,
            &PollingStrategy::next(),
            1,
            false,
        )
        .await
        .unwrap();

    if !polled2.messages.is_empty() {
        assert_ne!(
            polled2.partition_id,
            partition_polled_by_client1,
            "Consumer2 got partition {} which consumer1 is still processing! \
             Message ID from consumer1: offset={}, partition={}. \
             Message ID from consumer2: offset={}, partition={}. \
             This is the duplicate processing bug!",
            polled2.partition_id,
            message_from_client1.header.offset,
            partition_polled_by_client1,
            polled2.messages[0].header.offset,
            polled2.partition_id,
        );
    }

    // 5. Now consumer1 commits the offset
    client1
        .store_consumer_offset(
            &consumer,
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(partition_polled_by_client1),
            message_from_client1.header.offset,
        )
        .await
        .unwrap();

    // 6. Give the server a moment to process the revocation completion
    sleep(Duration::from_millis(100)).await;

    // 7. After commit, the partition should have transferred. Verify final state.
    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 2);
    assert_unique_partition_assignments(&cg);

    // Cleanup
    root_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic], server(
    heartbeat.enabled = true,
    heartbeat.interval = "2s",
    tcp.socket.override_defaults = true,
    tcp.socket.nodelay = true
))]
async fn should_complete_revocation_on_auto_commit(harness: &TestHarness) {
    let root_client = harness.root_client().await.unwrap();

    setup_stream_topic_cg(&root_client).await;
    send_one_message_per_partition(&root_client).await;

    // 1. Consumer1 joins, polls with auto_commit=true
    let client1 = harness.new_client().await.unwrap();
    client1
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client1).await;

    let consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());

    // Poll all 3 partitions with auto-commit
    for _ in 0..PARTITIONS_COUNT {
        client1
            .poll_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                None,
                &consumer,
                &PollingStrategy::next(),
                1,
                true, // auto-commit
            )
            .await
            .unwrap();
    }

    // 2. Consumer2 joins - since consumer1 already committed all offsets,
    //    the revocations should complete immediately.
    let client2 = harness.new_client().await.unwrap();
    client2
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client2).await;

    // Small delay for revocation processing
    sleep(Duration::from_millis(100)).await;

    // 3. Verify partitions are distributed
    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 2);
    assert_unique_partition_assignments(&cg);

    // Both members should have partitions (not all stuck on consumer1)
    for member in &cg.members {
        assert!(
            !member.partitions.is_empty(),
            "Member {} has no partitions - revocation may not have completed. Members: {:?}",
            member.id,
            cg.members
        );
    }

    // Cleanup
    root_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic], server(
    heartbeat.enabled = true,
    heartbeat.interval = "2s",
    tcp.socket.override_defaults = true,
    tcp.socket.nodelay = true
))]
async fn should_transfer_never_polled_partitions_immediately(harness: &TestHarness) {
    let root_client = harness.root_client().await.unwrap();

    setup_stream_topic_cg(&root_client).await;
    // NOTE: no messages sent - all partitions are empty

    // 1. Consumer1 joins, gets all 3 partitions
    let client1 = harness.new_client().await.unwrap();
    client1
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client1).await;

    // 2. Consumer2 joins - partitions should be distributed immediately
    //    because consumer1 never polled anything
    let client2 = harness.new_client().await.unwrap();
    client2
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client2).await;

    // 3. Consumer3 joins
    let client3 = harness.new_client().await.unwrap();
    client3
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client3).await;

    // 4. All 3 members should have exactly 1 partition each - immediately,
    //    no waiting for commits
    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 3);
    assert_unique_partition_assignments(&cg);

    for member in &cg.members {
        assert_eq!(
            member.partitions_count, 1,
            "Each member should have exactly 1 partition. Member {} has {}. Members: {:?}",
            member.id, member.partitions_count, cg.members
        );
    }

    // Cleanup
    root_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic], server(
    heartbeat.enabled = true,
    heartbeat.interval = "2s",
    tcp.socket.override_defaults = true,
    tcp.socket.nodelay = true
))]
async fn should_rebalance_when_member_with_pending_revocation_leaves(harness: &TestHarness) {
    let root_client = harness.root_client().await.unwrap();

    setup_stream_topic_cg(&root_client).await;
    send_one_message_per_partition(&root_client).await;

    // 1. Consumer1 joins, polls WITHOUT commit (creates in-flight work)
    let client1 = harness.new_client().await.unwrap();
    client1
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client1).await;

    let consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());
    let _polled = client1
        .poll_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            None,
            &consumer,
            &PollingStrategy::next(),
            1,
            false,
        )
        .await
        .unwrap();

    // 2. Consumer2 joins - some partitions become pending revocation
    let client2 = harness.new_client().await.unwrap();
    client2
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client2).await;

    // 3. Consumer1 disconnects (graceful) - should trigger full rebalance
    //    clearing all pending revocations
    drop(client1);
    sleep(Duration::from_millis(500)).await;

    // 4. Consumer2 should now get ALL partitions via full rebalance
    let cg = get_consumer_group(&root_client).await;
    assert_eq!(
        cg.members_count, 1,
        "Only consumer2 should remain. Members: {:?}",
        cg.members
    );
    assert_eq!(
        cg.members[0].partitions_count, PARTITIONS_COUNT,
        "Consumer2 should have all partitions after consumer1 left. Members: {:?}",
        cg.members
    );

    // 5. Consumer2 can actually poll from all partitions
    let mut polled_partitions = HashSet::new();
    for _ in 0..PARTITIONS_COUNT {
        let polled = client2
            .poll_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                None,
                &consumer,
                &PollingStrategy::offset(0),
                1,
                false,
            )
            .await
            .unwrap();
        if !polled.messages.is_empty() {
            polled_partitions.insert(polled.partition_id);
        }
    }
    assert_eq!(
        polled_partitions.len(),
        PARTITIONS_COUNT as usize,
        "Consumer2 should be able to poll from all {PARTITIONS_COUNT} partitions"
    );

    // Cleanup
    root_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic], server(
    heartbeat.enabled = true,
    heartbeat.interval = "2s",
    tcp.socket.override_defaults = true,
    tcp.socket.nodelay = true
))]
async fn should_not_produce_duplicate_messages_with_sequential_consumer_joins(
    harness: &TestHarness,
) {
    let root_client = harness.root_client().await.unwrap();

    setup_stream_topic_cg(&root_client).await;

    // Send 5 messages to each partition
    for partition_id in 0..PARTITIONS_COUNT {
        for msg_idx in 0..5u32 {
            let message = IggyMessage::from_str(&format!("p{partition_id}-msg{msg_idx}")).unwrap();
            let mut messages = vec![message];
            root_client
                .send_messages(
                    &Identifier::named(STREAM_NAME).unwrap(),
                    &Identifier::named(TOPIC_NAME).unwrap(),
                    &Partitioning::partition_id(partition_id),
                    &mut messages,
                )
                .await
                .unwrap();
        }
    }

    let consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());

    // 1. Consumer1 joins, gets all 3 partitions
    let client1 = harness.new_client().await.unwrap();
    client1
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client1).await;

    // 2. Consumer1 polls 1 message WITHOUT committing (manual ack)
    let polled1 = client1
        .poll_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            None,
            &consumer,
            &PollingStrategy::next(),
            1,
            false, // manual ack - offset NOT committed
        )
        .await
        .unwrap();
    assert_eq!(polled1.messages.len(), 1);
    let first_partition = polled1.partition_id;
    let first_offset = polled1.messages[0].header.offset;

    // 3. Consumer2 joins while consumer1 has uncommitted in-flight message
    sleep(Duration::from_millis(50)).await;
    let client2 = harness.new_client().await.unwrap();
    client2
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client2).await;

    // 4. Consumer3 joins
    sleep(Duration::from_millis(50)).await;
    let client3 = harness.new_client().await.unwrap();
    client3
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client3).await;

    // 5. Consumer1 now commits the offset (simulating manual ack after processing)
    client1
        .store_consumer_offset(
            &consumer,
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(first_partition),
            first_offset,
        )
        .await
        .unwrap();

    // 6. Small delay for revocation completions
    sleep(Duration::from_millis(200)).await;

    // 7. All 3 consumers poll messages with manual ack - collect ALL messages
    let mut all_messages: Vec<(u32, u64)> = Vec::new();
    all_messages.push((first_partition, first_offset));

    for client in [&client1, &client2, &client3] {
        for _ in 0..20 {
            let polled = client
                .poll_messages(
                    &Identifier::named(STREAM_NAME).unwrap(),
                    &Identifier::named(TOPIC_NAME).unwrap(),
                    None,
                    &consumer,
                    &PollingStrategy::next(),
                    1,
                    false, // manual ack
                )
                .await
                .unwrap();
            if polled.messages.is_empty() {
                break;
            }
            for msg in &polled.messages {
                all_messages.push((polled.partition_id, msg.header.offset));
            }
            // Commit each message after "processing"
            for msg in &polled.messages {
                client
                    .store_consumer_offset(
                        &consumer,
                        &Identifier::named(STREAM_NAME).unwrap(),
                        &Identifier::named(TOPIC_NAME).unwrap(),
                        Some(polled.partition_id),
                        msg.header.offset,
                    )
                    .await
                    .unwrap();
            }
        }
    }

    // 8. Check for duplicates: same (partition_id, offset) must never appear twice
    let mut seen = HashSet::new();
    for (partition_id, offset) in &all_messages {
        assert!(
            seen.insert((*partition_id, *offset)),
            "DUPLICATE MESSAGE DETECTED! partition={partition_id}, offset={offset}. \
             Two consumers processed the same message. \
             All messages: {all_messages:?}"
        );
    }

    // 9. Verify we got all 15 messages (5 per partition x 3 partitions)
    assert_eq!(
        all_messages.len(),
        15,
        "Expected 15 total messages (5 per partition x 3 partitions), got {}. \
         Messages: {:?}",
        all_messages.len(),
        all_messages
    );

    // Cleanup
    root_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic], server(
    heartbeat.enabled = true,
    heartbeat.interval = "2s",
    tcp.socket.override_defaults = true,
    tcp.socket.nodelay = true
))]
async fn should_wait_for_manual_commit_before_completing_revocation(harness: &TestHarness) {
    let root_client = harness.root_client().await.unwrap();

    setup_stream_topic_cg(&root_client).await;
    send_one_message_per_partition(&root_client).await;

    // 1. Client1 joins, rapidly polls all 3 partitions WITHOUT committing
    let client1 = harness.new_client().await.unwrap();
    client1
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client1).await;

    let consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());
    let mut polled_offsets: Vec<(u32, u64)> = Vec::new();

    for _ in 0..PARTITIONS_COUNT {
        let polled = client1
            .poll_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                None,
                &consumer,
                &PollingStrategy::next(),
                1,
                false, // manual commit
            )
            .await
            .unwrap();
        assert_eq!(polled.messages.len(), 1);
        polled_offsets.push((polled.partition_id, polled.messages[0].header.offset));
    }

    // 2. Consumer2 joins - all partitions have in-flight work
    let client2 = harness.new_client().await.unwrap();
    client2
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client2).await;

    sleep(Duration::from_millis(100)).await;

    // 3. Client2 should get NOTHING - all partitions have pending revocations
    //    that can't complete because client1 hasn't committed
    let polled2 = client2
        .poll_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            None,
            &consumer,
            &PollingStrategy::next(),
            1,
            false,
        )
        .await
        .unwrap();
    assert!(
        polled2.messages.is_empty(),
        "Client2 should get NO messages while client1 has uncommitted in-flight work \
         on all partitions, but got {} messages from partition {}",
        polled2.messages.len(),
        polled2.partition_id
    );

    // 4. Client1 commits each partition one by one
    for (partition_id, offset) in &polled_offsets {
        client1
            .store_consumer_offset(
                &consumer,
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                Some(*partition_id),
                *offset,
            )
            .await
            .unwrap();
    }

    sleep(Duration::from_millis(200)).await;

    // 5. Now consumer2 should have partitions
    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 2);
    assert_unique_partition_assignments(&cg);

    for member in &cg.members {
        assert!(
            !member.partitions.is_empty(),
            "Member {} has no partitions after client1 committed. Members: {:?}",
            member.id,
            cg.members
        );
    }

    // Cleanup
    root_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic], server(
    heartbeat.enabled = true,
    heartbeat.interval = "2s",
    tcp.socket.override_defaults = true,
    tcp.socket.nodelay = true
))]
async fn should_redistribute_when_revocation_target_leaves(harness: &TestHarness) {
    let root_client = harness.root_client().await.unwrap();

    setup_stream_topic_cg(&root_client).await;
    send_one_message_per_partition(&root_client).await;

    // 1. Consumer1 joins, polls without commit (creates in-flight work)
    let client1 = harness.new_client().await.unwrap();
    client1
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client1).await;

    let consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());
    let _polled = client1
        .poll_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            None,
            &consumer,
            &PollingStrategy::next(),
            1,
            false,
        )
        .await
        .unwrap();

    // 2. Consumer2 joins - gets some partitions, some become pending revocation
    let client2 = harness.new_client().await.unwrap();
    client2
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client2).await;

    sleep(Duration::from_millis(100)).await;

    // 3. Consumer2 (the revocation TARGET) disconnects before revocation completes
    drop(client2);
    sleep(Duration::from_millis(500)).await;

    // 4. Consumer1 should now have ALL partitions back via full rebalance
    let cg = get_consumer_group(&root_client).await;
    assert_eq!(
        cg.members_count, 1,
        "Only consumer1 should remain. Members: {:?}",
        cg.members
    );
    assert_eq!(
        cg.members[0].partitions_count, PARTITIONS_COUNT,
        "Consumer1 should have all {} partitions after target left. Members: {:?}",
        PARTITIONS_COUNT, cg.members
    );

    // 5. Consumer1 can poll from all partitions
    let mut polled_partitions = HashSet::new();
    for _ in 0..PARTITIONS_COUNT {
        let polled = client1
            .poll_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                None,
                &consumer,
                &PollingStrategy::offset(0),
                1,
                false,
            )
            .await
            .unwrap();
        if !polled.messages.is_empty() {
            polled_partitions.insert(polled.partition_id);
        }
    }
    assert_eq!(
        polled_partitions.len(),
        PARTITIONS_COUNT as usize,
        "Consumer1 should poll from all partitions after target left"
    );

    // Cleanup
    root_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic], server(
    heartbeat.enabled = true,
    heartbeat.interval = "2s",
    tcp.socket.override_defaults = true,
    tcp.socket.nodelay = true
))]
async fn should_distribute_partitions_evenly_with_concurrent_joins(harness: &TestHarness) {
    let root_client = harness.root_client().await.unwrap();

    setup_stream_topic_cg(&root_client).await;
    send_one_message_per_partition(&root_client).await;

    // Create 3 clients and login
    let client1 = harness.new_client().await.unwrap();
    let client2 = harness.new_client().await.unwrap();
    let client3 = harness.new_client().await.unwrap();

    for client in [&client1, &client2, &client3] {
        client
            .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
            .await
            .unwrap();
    }

    // Join all 3 concurrently
    let stream_id = Identifier::named(STREAM_NAME).unwrap();
    let topic_id = Identifier::named(TOPIC_NAME).unwrap();
    let group_id = Identifier::named(CONSUMER_GROUP_NAME).unwrap();

    let (r1, r2, r3) = tokio::join!(
        client1.join_consumer_group(&stream_id, &topic_id, &group_id),
        client2.join_consumer_group(&stream_id, &topic_id, &group_id),
        client3.join_consumer_group(&stream_id, &topic_id, &group_id),
    );
    r1.unwrap();
    r2.unwrap();
    r3.unwrap();

    // Small delay for any pending revocation completions
    sleep(Duration::from_millis(200)).await;

    // Verify: 3 members, each with exactly 1 partition
    let cg = get_consumer_group(&root_client).await;
    assert_eq!(
        cg.members_count, 3,
        "Expected 3 members after concurrent joins, got {}. Members: {:?}",
        cg.members_count, cg.members
    );
    assert_unique_partition_assignments(&cg);

    for member in &cg.members {
        assert_eq!(
            member.partitions_count, 1,
            "Each member should have exactly 1 partition with concurrent joins. \
             Member {} has {}. Members: {:?}",
            member.id, member.partitions_count, cg.members
        );
    }

    // Each consumer polls - must get unique partitions
    let consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());
    let mut polled_partitions = HashSet::new();

    for (i, client) in [&client1, &client2, &client3].iter().enumerate() {
        let polled = client
            .poll_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                None,
                &consumer,
                &PollingStrategy::offset(0),
                10,
                false,
            )
            .await
            .unwrap();
        assert!(
            !polled.messages.is_empty(),
            "Client {i} should have messages but got none"
        );
        assert!(
            polled_partitions.insert(polled.partition_id),
            "Client {i} got partition {} already taken by another client! Duplicate!",
            polled.partition_id
        );
    }

    assert_eq!(
        polled_partitions.len(),
        3,
        "All 3 partitions must be covered"
    );

    // Cleanup
    root_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic], server(
    heartbeat.enabled = true,
    heartbeat.interval = "2s",
    tcp.socket.override_defaults = true,
    tcp.socket.nodelay = true
))]
async fn should_not_assign_partition_to_wrong_member_after_slab_reuse(harness: &TestHarness) {
    let root_client = harness.root_client().await.unwrap();

    setup_stream_topic_cg(&root_client).await;
    send_one_message_per_partition(&root_client).await;

    // 1. Consumer1 joins, gets all 3 partitions, polls without committing
    let client1 = harness.new_client().await.unwrap();
    client1
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client1).await;

    let consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());
    for _ in 0..PARTITIONS_COUNT {
        client1
            .poll_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                None,
                &consumer,
                &PollingStrategy::next(),
                1,
                false,
            )
            .await
            .unwrap();
    }

    // 2. Consumer2 joins — triggers pending revocation targeting consumer2's slab
    let client2 = harness.new_client().await.unwrap();
    client2
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client2).await;

    sleep(Duration::from_millis(100)).await;

    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 2);

    // 3. Consumer2 (revocation target) disconnects — its slab is freed
    drop(client2);
    sleep(Duration::from_secs(3)).await;

    // 4. Consumer3 joins — may reuse consumer2's old slab
    let client3 = harness.new_client().await.unwrap();
    client3
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client3).await;

    // 5. Consumer1 commits all offsets — revocation completion should detect
    //    slab reuse via target_member_id validation and trigger full rebalance
    for partition_id in 0..PARTITIONS_COUNT {
        client1
            .store_consumer_offset(
                &consumer,
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                Some(partition_id),
                0,
            )
            .await
            .unwrap();
    }

    sleep(Duration::from_millis(500)).await;

    // 6. Verify no partition duplication and all partitions assigned
    let cg = get_consumer_group(&root_client).await;
    assert_unique_partition_assignments(&cg);
    let total: u32 = cg.members.iter().map(|m| m.partitions_count).sum();
    assert_eq!(
        total, PARTITIONS_COUNT,
        "All partitions must be assigned after slab reuse scenario. Members: {:?}",
        cg.members
    );

    // Cleanup
    root_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic], server(
    heartbeat.enabled = true,
    heartbeat.interval = "2s",
    tcp.socket.override_defaults = true,
    tcp.socket.nodelay = true
))]
async fn should_not_complete_other_members_revocations_on_leave(harness: &TestHarness) {
    let root_client = harness.root_client().await.unwrap();

    setup_stream_topic_cg(&root_client).await;
    send_one_message_per_partition(&root_client).await;

    // 1. Consumer1 joins, gets all 3 partitions, polls without committing
    let client1 = harness.new_client().await.unwrap();
    client1
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client1).await;

    let consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());
    for _ in 0..PARTITIONS_COUNT {
        client1
            .poll_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                None,
                &consumer,
                &PollingStrategy::next(),
                1,
                false,
            )
            .await
            .unwrap();
    }

    // 2. Consumer2 joins — triggers pending revocation for 1 partition
    let client2 = harness.new_client().await.unwrap();
    client2
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client2).await;

    // 3. Consumer3 joins — triggers another pending revocation
    let client3 = harness.new_client().await.unwrap();
    client3
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    join_cg(&client3).await;

    sleep(Duration::from_millis(100)).await;

    // Consumer1 has in-flight work on all partitions (polled, not committed).
    // Consumer2 and consumer3 have 0 partitions (waiting for revocations).
    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 3);

    // 4. Consumer3 disconnects — triggers full rebalance (rebalance_members).
    //    Leave clears only consumer3's polled offsets, not consumer1's.
    drop(client3);
    sleep(Duration::from_secs(3)).await;

    // 5. After full rebalance, consumer1 and consumer2 split all partitions.
    //    The key invariant: no partition is assigned to two members.
    let cg = get_consumer_group(&root_client).await;
    assert_eq!(cg.members_count, 2);
    assert_unique_partition_assignments(&cg);
    let total: u32 = cg.members.iter().map(|m| m.partitions_count).sum();
    assert_eq!(
        total, PARTITIONS_COUNT,
        "All partitions must be assigned after leave. Members: {:?}",
        cg.members
    );
    for member in &cg.members {
        assert!(
            member.partitions_count > 0,
            "Both remaining members should have partitions after leave rebalance. Members: {:?}",
            cg.members
        );
    }

    // 6. Both consumers can poll without duplicates
    let mut all_partitions = HashSet::new();
    for client in [&client1, &client2] {
        let polled = client
            .poll_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                None,
                &consumer,
                &PollingStrategy::offset(0),
                1,
                false,
            )
            .await
            .unwrap();
        if !polled.messages.is_empty() {
            assert!(
                all_partitions.insert(polled.partition_id),
                "Duplicate partition {} after leave rebalance",
                polled.partition_id
            );
        }
    }
    assert_eq!(
        all_partitions.len(),
        2,
        "Both consumers should poll from unique partitions after leave"
    );

    // Cleanup
    root_client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}
