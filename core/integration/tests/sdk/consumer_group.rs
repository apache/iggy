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

use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;

use futures::StreamExt;
use iggy::prelude::*;
use integration::iggy_harness;
use tokio::time::timeout;

const STREAM_NAME: &str = "consumer-group-rejoin-stream";
const TOPIC_NAME: &str = "consumer-group-rejoin-topic";
const CONSUMER_GROUP_NAME: &str = "consumer-group-rejoin-group";
const CONSUMER_USERNAME: &str = "consumer-group-rejoin-user";
const CONSUMER_PASSWORD: &str = "password123";
const CONSUMER_REJOIN_TIMEOUT: Duration = Duration::from_secs(10);
const PARTITIONS_COUNT: u32 = 2;
const MESSAGES_PER_PARTITION: u32 = 5;
const READ_TIMEOUT: Duration = Duration::from_secs(10);

// Pins a 60s server heartbeat because harness clients never ping on their own:
// the SDK pinger is spawned by `IggyClient::connect`, which the harness builder
// does not call. At the shipped 30s interval an idle group member is reaped by
// the server's verifier, and the failure surfaces as a short member count
// instead of anything about the scenario under test.
#[iggy_harness(
    test_client_transport = [Tcp, WebSocket, Quic],
    server(heartbeat.enabled = true, heartbeat.interval = "60s")
)]
async fn consumer_group_retries_rejoin_after_failure(harness: &TestHarness) {
    let root_client = harness
        .root_client()
        .await
        .expect("Failed to get root client");
    let stream_id = Identifier::named(STREAM_NAME).unwrap();
    let topic_id = Identifier::named(TOPIC_NAME).unwrap();
    let group_id = Identifier::named(CONSUMER_GROUP_NAME).unwrap();
    let user_id = Identifier::named(CONSUMER_USERNAME).unwrap();
    let consumer_permissions = Permissions {
        global: GlobalPermissions {
            read_streams: true,
            ..Default::default()
        },
        streams: None,
    };

    root_client.create_stream(STREAM_NAME).await.unwrap();
    root_client
        .create_topic(
            &stream_id,
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(1),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();
    root_client
        .create_consumer_group(&stream_id, &topic_id, CONSUMER_GROUP_NAME)
        .await
        .unwrap();
    root_client
        .create_user(
            CONSUMER_USERNAME,
            CONSUMER_PASSWORD,
            UserStatus::Active,
            Some(consumer_permissions.clone()),
        )
        .await
        .unwrap();

    let consumer_client = harness.new_client().await.expect("Failed to create client");
    consumer_client
        .login_user(CONSUMER_USERNAME, CONSUMER_PASSWORD)
        .await
        .unwrap();

    let mut consumer = consumer_client
        .consumer_group(CONSUMER_GROUP_NAME, STREAM_NAME, TOPIC_NAME)
        .unwrap()
        .batch_length(1)
        .auto_join_consumer_group()
        .build();
    consumer.init().await.unwrap();

    let group = root_client
        .get_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .unwrap()
        .expect("Consumer group should exist");
    assert_eq!(group.members_count, 1);
    assert_eq!(group.members.len(), 1);

    let mut messages = vec![IggyMessage::from_str("message").unwrap()];
    root_client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .unwrap();

    consumer_client
        .leave_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .unwrap();

    let group = root_client
        .get_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .unwrap()
        .expect("Consumer group should exist");
    assert_eq!(group.members_count, 0);
    assert!(group.members.is_empty());

    root_client
        .update_permissions(
            &user_id,
            Some(Permissions {
                global: GlobalPermissions {
                    poll_messages: true,
                    ..Default::default()
                },
                streams: None,
            }),
        )
        .await
        .unwrap();

    let rejoin_result = timeout(CONSUMER_REJOIN_TIMEOUT, consumer.next())
        .await
        .expect("Consumer rejoin should fail before timeout")
        .expect("Consumer stream should remain open");
    assert!(matches!(rejoin_result, Err(IggyError::Unauthorized)));

    root_client
        .update_permissions(&user_id, Some(consumer_permissions))
        .await
        .unwrap();

    let received = timeout(CONSUMER_REJOIN_TIMEOUT, consumer.next())
        .await
        .expect("Consumer should recover before timeout")
        .expect("Consumer stream should remain open")
        .expect("Consumer should rejoin after its membership is revoked");
    assert_eq!(received.message.payload, "message");

    let group = root_client
        .get_consumer_group(&stream_id, &topic_id, &group_id)
        .await
        .unwrap()
        .expect("Consumer group should exist");
    assert_eq!(group.members_count, 1);
    assert_eq!(group.members.len(), 1);
}

// A group member polls its partitions round-robin. Under a strategy other than `next()` the
// continuation must be kept per partition: one shared cursor would ask the second partition for
// the offset reached in the first one and skip its beginning.
#[iggy_harness(
    test_client_transport = [Tcp, WebSocket, Quic],
    server(heartbeat.enabled = true, heartbeat.interval = "60s")
)]
async fn given_offset_strategy_when_member_polls_two_partitions_should_read_each_from_its_start(
    harness: &TestHarness,
) {
    let root_client = harness
        .root_client()
        .await
        .expect("Failed to get root client");
    let stream_id = Identifier::named(STREAM_NAME).unwrap();
    let topic_id = Identifier::named(TOPIC_NAME).unwrap();

    root_client.create_stream(STREAM_NAME).await.unwrap();
    root_client
        .create_topic(
            &stream_id,
            TOPIC_NAME,
            &TopicCreateOptions {
                partitions_count: Some(PARTITIONS_COUNT),
                message_expiry: Some(IggyExpiry::NeverExpire),
                ..TopicCreateOptions::default()
            },
        )
        .await
        .unwrap();
    for partition_id in 0..PARTITIONS_COUNT {
        let mut messages: Vec<IggyMessage> = (0..MESSAGES_PER_PARTITION)
            .map(|index| IggyMessage::from_str(&format!("{partition_id}-{index}")).unwrap())
            .collect();
        root_client
            .send_messages(
                &stream_id,
                &topic_id,
                &Partitioning::partition_id(partition_id),
                &mut messages,
            )
            .await
            .unwrap();
    }

    let mut consumer = root_client
        .consumer_group(CONSUMER_GROUP_NAME, STREAM_NAME, TOPIC_NAME)
        .unwrap()
        .polling_strategy(PollingStrategy::offset(0))
        .batch_length(MESSAGES_PER_PARTITION)
        .auto_commit(AutoCommit::Disabled)
        .build();
    consumer.init().await.unwrap();

    let expected_total = (PARTITIONS_COUNT * MESSAGES_PER_PARTITION) as usize;
    let mut offsets_by_partition: HashMap<u32, Vec<u64>> = HashMap::new();
    for _ in 0..expected_total {
        let received = timeout(READ_TIMEOUT, consumer.next())
            .await
            .expect("every partition must be read from its start before the timeout")
            .expect("consumer stream should remain open")
            .expect("polling must not fail");
        offsets_by_partition
            .entry(received.partition_id)
            .or_default()
            .push(received.message.header.offset);
    }
    consumer.shutdown().await.unwrap();

    let expected_offsets: Vec<u64> = (0..u64::from(MESSAGES_PER_PARTITION)).collect();
    for partition_id in 0..PARTITIONS_COUNT {
        assert_eq!(
            offsets_by_partition.get(&partition_id),
            Some(&expected_offsets),
            "partition {partition_id} must be read from offset 0 without gaps"
        );
    }
}
