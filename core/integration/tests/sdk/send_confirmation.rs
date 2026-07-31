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

//! Commit confirmations for `SendMessages`: which partition a batch landed in
//! and at which offset. server-ng answers a committed send with a confirmation
//! payload; the legacy server answers with an empty body, which the SDK reports
//! as a zeroed confirmation rather than as a decode failure.

use iggy::prelude::*;
use integration::iggy_harness;

const STREAM_NAME: &str = "confirmation-stream";
const TOPIC_NAME: &str = "confirmation-topic";
const MESSAGES_COUNT: u32 = 10;
const PARTITIONS_COUNT: u32 = 3;

// server-ng partition ids are 0-based (CreateTopic assigns them from 0).
#[cfg(feature = "vsr")]
const PARTITION_ID: u32 = 0;
/// Chunking for the direct producer: `CHUNKS * CHUNK_LENGTH` messages exceed
/// one request, so the send is split and every chunk confirms separately.
#[cfg(feature = "vsr")]
const CHUNK_LENGTH: u32 = 4;
#[cfg(feature = "vsr")]
const CHUNKS: u32 = 3;

fn batch(count: u32) -> Vec<IggyMessage> {
    (0..count)
        .map(|i| {
            IggyMessage::builder()
                .id(u128::from(i + 1))
                .payload(format!("payload-{i}").into())
                .build()
                .expect("message build")
        })
        .collect()
}

/// Returns the numeric ids the server assigned to the created stream and topic.
async fn create_stream_and_topic(client: &IggyClient, partitions_count: u32) -> (u32, u32) {
    let stream = client
        .create_stream(STREAM_NAME)
        .await
        .expect("create_stream");
    let topic = client
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
        .expect("create_topic");
    (stream.id, topic.id)
}

#[cfg(feature = "vsr")]
fn sole_confirmation(response: &SendMessagesResponse) -> &SendMessagesConfirmationResponse {
    assert_eq!(
        response.confirmations.len(),
        1,
        "a send routed to one partition confirms exactly one partition"
    );
    &response.confirmations[0]
}

/// Each transport carries the reply body on its own path, so the full
/// confirmation shape is pinned on all three.
#[cfg(feature = "vsr")]
#[iggy_harness(test_client_transport = [Tcp, WebSocket, Quic])]
async fn given_explicit_partition_when_sending_two_batches_should_confirm_advancing_base_offset(
    harness: &TestHarness,
) {
    let client = harness.new_client().await.unwrap();
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();

    let (stream_id, topic_id) = create_stream_and_topic(&client, 1).await;
    let stream = Identifier::named(STREAM_NAME).unwrap();
    let topic = Identifier::named(TOPIC_NAME).unwrap();
    let partitioning = Partitioning::partition_id(PARTITION_ID);

    let mut messages = batch(MESSAGES_COUNT);
    let response = client
        .send_messages(&stream, &topic, &partitioning, &mut messages)
        .await
        .expect("send_messages");
    let first = sole_confirmation(&response);

    assert_eq!(first.stream_id, stream_id, "confirmed stream id");
    assert_eq!(first.topic_id, topic_id, "confirmed topic id");
    assert_eq!(first.partition_id, PARTITION_ID, "confirmed partition id");
    assert_eq!(first.base_offset, 0, "the first batch starts at offset 0");

    let mut messages = batch(MESSAGES_COUNT);
    let response = client
        .send_messages(&stream, &topic, &partitioning, &mut messages)
        .await
        .expect("send_messages");
    let second = sole_confirmation(&response);

    assert_eq!(second.partition_id, PARTITION_ID, "same partition");
    assert_eq!(
        second.base_offset,
        u64::from(MESSAGES_COUNT),
        "the next batch starts where the previous one ended"
    );

    client.logout_user().await.unwrap();
}

#[cfg(feature = "vsr")]
#[iggy_harness]
async fn given_balanced_partitioning_when_sending_should_confirm_a_partition_of_the_topic(
    harness: &TestHarness,
) {
    let client = harness.new_client().await.unwrap();
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();

    let (stream_id, topic_id) = create_stream_and_topic(&client, PARTITIONS_COUNT).await;
    let mut messages = batch(MESSAGES_COUNT);
    let response = client
        .send_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Partitioning::balanced(),
            &mut messages,
        )
        .await
        .expect("send_messages");
    let confirmation = sole_confirmation(&response);

    assert_eq!(confirmation.stream_id, stream_id, "confirmed stream id");
    assert_eq!(confirmation.topic_id, topic_id, "confirmed topic id");
    assert!(
        confirmation.partition_id < PARTITIONS_COUNT,
        "balanced routing must land in one of the topic's {PARTITIONS_COUNT} partitions, got {}",
        confirmation.partition_id
    );
    assert_eq!(
        confirmation.base_offset, 0,
        "the chosen partition was empty before the send"
    );

    client.logout_user().await.unwrap();
}

#[cfg(feature = "vsr")]
#[iggy_harness]
async fn given_messages_key_partitioning_when_sending_should_confirm_a_partition_of_the_topic(
    harness: &TestHarness,
) {
    let client = harness.new_client().await.unwrap();
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();

    create_stream_and_topic(&client, PARTITIONS_COUNT).await;
    let mut messages = batch(MESSAGES_COUNT);
    let response = client
        .send_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Partitioning::messages_key_str("confirmation-key").unwrap(),
            &mut messages,
        )
        .await
        .expect("send_messages");
    let confirmation = sole_confirmation(&response);

    assert!(
        confirmation.partition_id < PARTITIONS_COUNT,
        "keyed routing must land in one of the topic's {PARTITIONS_COUNT} partitions, got {}",
        confirmation.partition_id
    );

    client.logout_user().await.unwrap();
}

#[cfg(feature = "vsr")]
#[iggy_harness]
async fn given_direct_producer_when_send_splits_into_chunks_should_confirm_every_chunk(
    harness: &TestHarness,
) {
    let client = harness.new_client().await.unwrap();
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();

    let (stream_id, topic_id) = create_stream_and_topic(&client, 1).await;
    let producer = client
        .producer(STREAM_NAME, TOPIC_NAME)
        .unwrap()
        .partitioning(Partitioning::partition_id(PARTITION_ID))
        .direct(DirectConfig::builder().batch_length(CHUNK_LENGTH).build())
        .build();

    let response = producer
        .send(batch(CHUNKS * CHUNK_LENGTH))
        .await
        .expect("producer send");

    assert_eq!(
        response.confirmations.len(),
        CHUNKS as usize,
        "a direct producer concatenates the confirmation of every chunk it split the send into"
    );
    for (chunk, confirmation) in response.confirmations.iter().enumerate() {
        assert_eq!(confirmation.stream_id, stream_id, "confirmed stream id");
        assert_eq!(confirmation.topic_id, topic_id, "confirmed topic id");
        assert_eq!(confirmation.partition_id, PARTITION_ID, "pinned partition");
        assert_eq!(
            confirmation.base_offset,
            chunk as u64 * u64::from(CHUNK_LENGTH),
            "chunk {chunk} must start where the previous chunk ended"
        );
    }

    client.logout_user().await.unwrap();
}

#[cfg(not(feature = "vsr"))]
#[iggy_harness]
async fn given_legacy_server_when_sending_should_report_a_zeroed_confirmation(
    harness: &TestHarness,
) {
    let client = harness.root_client().await.unwrap();

    create_stream_and_topic(&client, PARTITIONS_COUNT).await;
    let mut messages = batch(MESSAGES_COUNT);
    let response = client
        .send_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Partitioning::balanced(),
            &mut messages,
        )
        .await
        .expect("send_messages");

    assert_eq!(
        response.confirmations,
        vec![SendMessagesConfirmationResponse {
            stream_id: 0,
            topic_id: 0,
            partition_id: 0,
            base_offset: 0,
        }],
        "an empty legacy reply body degrades to a zeroed confirmation"
    );
}
