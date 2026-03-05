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

use crate::server::scenarios::{PARTITION_ID, STREAM_NAME, TOPIC_NAME, cleanup};
use bytes::Bytes;
use iggy::prelude::*;
use integration::harness::{TestHarness, assert_clean_system};

const MESSAGES_PER_BATCH: u32 = 10;

pub async fn run(harness: &TestHarness) {
    let client = harness
        .root_client()
        .await
        .expect("Failed to get root client");
    init_system(&client).await;

    // Step 1: Duplicate rejection
    // Send messages with IDs 1..10, then resend the same IDs.
    // Only the first batch should be persisted.
    let mut original_messages = build_messages(1, MESSAGES_PER_BATCH, "original");
    client
        .send_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Partitioning::partition_id(PARTITION_ID),
            &mut original_messages,
        )
        .await
        .unwrap();

    let mut duplicate_messages = build_messages(1, MESSAGES_PER_BATCH, "duplicate");
    client
        .send_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Partitioning::partition_id(PARTITION_ID),
            &mut duplicate_messages,
        )
        .await
        .unwrap();

    let polled = poll_all(&client, MESSAGES_PER_BATCH * 2).await;
    assert_eq!(
        polled.messages.len() as u32,
        MESSAGES_PER_BATCH,
        "Duplicate messages should have been dropped"
    );
    for msg in &polled.messages {
        let payload = std::str::from_utf8(&msg.payload).unwrap();
        assert!(
            payload.starts_with("original"),
            "Expected original payload, got: {payload}"
        );
    }

    // Step 2: Unique messages pass through
    // Send messages with new IDs 11..20 — these should all be accepted.
    let mut new_messages = build_messages(MESSAGES_PER_BATCH + 1, MESSAGES_PER_BATCH, "new-unique");
    client
        .send_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Partitioning::partition_id(PARTITION_ID),
            &mut new_messages,
        )
        .await
        .unwrap();

    let polled = poll_all(&client, MESSAGES_PER_BATCH * 3).await;
    assert_eq!(
        polled.messages.len() as u32,
        MESSAGES_PER_BATCH * 2,
        "Unique messages should have been accepted"
    );

    // Step 3: TTL expiry re-accepts previously seen IDs
    // The server is configured with a 2s dedup expiry. Wait for it to elapse,
    // then resend the original IDs — they should be accepted again.
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    let mut reused_messages = build_messages(1, MESSAGES_PER_BATCH, "after-ttl");
    client
        .send_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            &Partitioning::partition_id(PARTITION_ID),
            &mut reused_messages,
        )
        .await
        .unwrap();

    let polled = poll_all(&client, MESSAGES_PER_BATCH * 4).await;
    assert_eq!(
        polled.messages.len() as u32,
        MESSAGES_PER_BATCH * 3,
        "Previously seen IDs should be accepted again after TTL expiry"
    );

    cleanup(&client, false).await;
    assert_clean_system(&client).await;
}

async fn init_system(client: &IggyClient) {
    client.create_stream(STREAM_NAME).await.unwrap();
    client
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
}

fn build_messages(start_id: u32, count: u32, payload_prefix: &str) -> Vec<IggyMessage> {
    (0..count)
        .map(|i| {
            let id = (start_id + i) as u128;
            IggyMessage::builder()
                .id(id)
                .payload(Bytes::from(format!("{payload_prefix}-{i}")))
                .build()
                .expect("Failed to create message")
        })
        .collect()
}

async fn poll_all(client: &IggyClient, max_count: u32) -> PolledMessages {
    let consumer = Consumer::default();
    client
        .poll_messages(
            &Identifier::named(STREAM_NAME).unwrap(),
            &Identifier::named(TOPIC_NAME).unwrap(),
            Some(PARTITION_ID),
            &consumer,
            &PollingStrategy::offset(0),
            max_count,
            false,
        )
        .await
        .unwrap()
}
