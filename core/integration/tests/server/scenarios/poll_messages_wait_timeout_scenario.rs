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

use iggy::prelude::*;
use integration::harness::TestHarness;
use std::time::{Duration, Instant};
use tokio::time::sleep;
#[cfg(not(feature = "vsr"))]
use tokio::time::timeout;

const STREAM_NAME: &str = "poll-wait-timeout-stream";
const TOPIC_NAME: &str = "poll-wait-timeout-topic";
const CONSUMER_GROUP_NAME: &str = "poll-wait-timeout-group";
#[cfg(not(feature = "vsr"))]
const PARTITION_ID: u32 = 0;

#[cfg(not(feature = "vsr"))]
pub async fn run_semantics_checks(harness: &TestHarness) {
    let client = harness.root_client().await.expect("root client");
    let producer = harness.root_client().await.expect("producer client");
    setup_topic(&client, 1).await;

    verify_immediate_empty_polling(&client).await;
    verify_timeout_waits_and_returns_empty(&client).await;
    verify_existing_messages_return_without_waiting(&producer, &client).await;
    verify_timeout_auto_commit_boundaries(&producer, &client).await;

    client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

#[cfg(not(feature = "vsr"))]
pub async fn run_wake_after_append_checks(harness: &TestHarness) {
    let producer = harness.root_client().await.expect("producer client");
    let consumer_client = harness.root_client().await.expect("consumer client");

    producer.create_stream(STREAM_NAME).await.unwrap();
    producer
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            1,
            CompressionAlgorithm::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();

    let timeout = Duration::from_secs(5);
    let start = Instant::now();
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let consumer = Consumer::default();
    let strategy = PollingStrategy::offset(0);
    let poll = consumer_client.poll_messages_with_timeout(
        &stream,
        &topic,
        Some(0),
        &consumer,
        &strategy,
        1,
        false,
        timeout,
    );
    let send = async {
        sleep(Duration::from_millis(100)).await;
        let mut messages = vec![
            IggyMessage::builder()
                .id(1)
                .payload("wake-after-append".into())
                .build()
                .unwrap(),
        ];
        producer
            .send_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                &Partitioning::partition_id(PARTITION_ID),
                &mut messages,
            )
            .await
            .unwrap();
    };

    let (polled, _) = tokio::join!(poll, send);
    let polled = polled.unwrap();

    assert_eq!(polled.partition_id, PARTITION_ID);
    assert_eq!(polled.messages.len(), 1);
    assert!(
        start.elapsed() < timeout / 2,
        "poll should wake after a successful append instead of waiting for timeout"
    );

    producer
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

#[cfg(not(feature = "vsr"))]
async fn setup_topic(client: &IggyClient, partitions_count: u32) {
    client.create_stream(STREAM_NAME).await.unwrap();
    client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            partitions_count,
            CompressionAlgorithm::default(),
            None,
            IggyExpiry::NeverExpire,
            MaxTopicSize::ServerDefault,
        )
        .await
        .unwrap();
}

#[cfg(not(feature = "vsr"))]
async fn verify_immediate_empty_polling(client: &IggyClient) {
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let consumer = Consumer::new(Identifier::numeric(101).unwrap());
    let strategy = PollingStrategy::offset(0);

    let immediate = timeout(
        Duration::from_secs(1),
        client.poll_messages(
            &stream,
            &topic,
            Some(PARTITION_ID),
            &consumer,
            &strategy,
            1,
            false,
        ),
    )
    .await
    .expect("immediate poll should not wait")
    .unwrap();
    assert!(immediate.messages.is_empty());

    let zero_timeout = timeout(
        Duration::from_secs(1),
        client.poll_messages_with_timeout(
            &stream,
            &topic,
            Some(PARTITION_ID),
            &consumer,
            &strategy,
            1,
            false,
            Duration::ZERO,
        ),
    )
    .await
    .expect("zero timeout poll should not wait")
    .unwrap();
    assert!(zero_timeout.messages.is_empty());
}

#[cfg(not(feature = "vsr"))]
async fn verify_timeout_waits_and_returns_empty(client: &IggyClient) {
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let consumer = Consumer::new(Identifier::numeric(102).unwrap());
    let strategy = PollingStrategy::offset(0);
    let poll = client.poll_messages_with_timeout(
        &stream,
        &topic,
        Some(PARTITION_ID),
        &consumer,
        &strategy,
        1,
        false,
        Duration::from_millis(120),
    );
    tokio::pin!(poll);

    tokio::select! {
        result = &mut poll => panic!("non-zero timeout returned before its wait window: {result:?}"),
        () = sleep(Duration::from_millis(20)) => {}
    }

    let polled = poll.await.unwrap();
    assert!(polled.messages.is_empty());
}

#[cfg(not(feature = "vsr"))]
async fn verify_existing_messages_return_without_waiting(
    producer: &IggyClient,
    client: &IggyClient,
) {
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    send_one(producer, 1, "already-readable").await;

    let polled = timeout(
        Duration::from_secs(1),
        client.poll_messages_with_timeout(
            &stream,
            &topic,
            Some(PARTITION_ID),
            &Consumer::new(Identifier::numeric(103).unwrap()),
            &PollingStrategy::offset(0),
            1,
            false,
            Duration::from_secs(5),
        ),
    )
    .await
    .expect("readable data should return without waiting for timeout")
    .unwrap();

    assert_eq!(polled.partition_id, PARTITION_ID);
    assert_eq!(polled.messages.len(), 1);
    assert_eq!(polled.messages[0].payload.as_ref(), b"already-readable");
}

#[cfg(not(feature = "vsr"))]
async fn verify_timeout_auto_commit_boundaries(producer: &IggyClient, client: &IggyClient) {
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let empty_timeout_consumer = Consumer::new(Identifier::numeric(104).unwrap());
    let empty = client
        .poll_messages_with_timeout(
            &stream,
            &topic,
            Some(PARTITION_ID),
            &empty_timeout_consumer,
            &PollingStrategy::offset(1),
            1,
            true,
            Duration::from_millis(50),
        )
        .await
        .unwrap();

    assert!(empty.messages.is_empty());
    let offset_after_empty = client
        .get_consumer_offset(&empty_timeout_consumer, &stream, &topic, Some(PARTITION_ID))
        .await
        .unwrap();
    assert!(offset_after_empty.is_none());

    let auto_commit_consumer = Consumer::new(Identifier::numeric(105).unwrap());
    let auto_commit_strategy = PollingStrategy::offset(1);
    let poll = client.poll_messages_with_timeout(
        &stream,
        &topic,
        Some(PARTITION_ID),
        &auto_commit_consumer,
        &auto_commit_strategy,
        1,
        true,
        Duration::from_secs(5),
    );
    let send = async {
        sleep(Duration::from_millis(100)).await;
        send_one(producer, 2, "wake-auto-commit").await;
    };
    let (polled, _) = tokio::join!(poll, send);
    let polled = polled.unwrap();

    assert_eq!(polled.messages.len(), 1);
    let committed = client
        .get_consumer_offset(&auto_commit_consumer, &stream, &topic, Some(PARTITION_ID))
        .await
        .unwrap()
        .expect("wake with returned messages should auto-commit");
    assert_eq!(committed.stored_offset, polled.messages[0].header.offset);
}

#[cfg(not(feature = "vsr"))]
async fn send_one(client: &IggyClient, id: u128, payload: &str) {
    let mut messages = vec![
        IggyMessage::builder()
            .id(id)
            .payload(payload.to_owned().into())
            .build()
            .unwrap(),
    ];
    client
        .send_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Partitioning::partition_id(PARTITION_ID),
            &mut messages,
        )
        .await
        .unwrap();
}

pub async fn run_consumer_group_checks(harness: &TestHarness) {
    let client = harness.root_client().await.expect("root client");

    client.create_stream(STREAM_NAME).await.unwrap();
    client
        .create_topic(
            &Identifier::named(STREAM_NAME).unwrap(),
            TOPIC_NAME,
            2,
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
    client
        .join_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Identifier::named(CONSUMER_GROUP_NAME).unwrap(),
        )
        .await
        .unwrap();

    let mut messages = vec![
        IggyMessage::builder()
            .id(1)
            .payload("ready-on-partition-1".into())
            .build()
            .unwrap(),
    ];
    client
        .send_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Partitioning::partition_id(1),
            &mut messages,
        )
        .await
        .unwrap();

    let consumer = Consumer::group(Identifier::named(CONSUMER_GROUP_NAME).unwrap());
    let group = client
        .get_consumer_group(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Identifier::named(CONSUMER_GROUP_NAME).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(group.members[0].partitions, vec![0, 1]);

    wait_until_partition_readable(&client).await;

    let timeout = Duration::from_millis(750);
    let start = Instant::now();
    let polled = client
        .poll_messages_with_timeout(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(0),
            &consumer,
            &PollingStrategy::offset(0),
            1,
            true,
            timeout,
        )
        .await
        .unwrap();

    assert_eq!(polled.partition_id, 1);
    assert_eq!(polled.messages.len(), 1);
    assert!(
        start.elapsed() < timeout / 2,
        "poll should not wait on an empty owned partition while another owned partition has data"
    );

    client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

async fn wait_until_partition_readable(client: &IggyClient) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let direct = client
            .poll_messages(
                &Identifier::named(STREAM_NAME).unwrap(),
                &Identifier::named(TOPIC_NAME).unwrap(),
                Some(1),
                &Consumer::default(),
                &PollingStrategy::offset(0),
                1,
                false,
            )
            .await
            .unwrap();

        if direct.messages.len() == 1 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "partition 1 should become readable"
        );
        sleep(Duration::from_millis(25)).await;
    }
}
