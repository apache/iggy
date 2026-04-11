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

use bytes::Bytes;
use iggy::prelude::*;
use integration::harness::TestHarness;
use std::collections::HashSet;
use std::time::Duration;

const STREAM_NAME: &str = "dedup-test-stream";
const TOPIC_NAME: &str = "dedup-test-topic";
const MESSAGES_PER_BATCH: u32 = 10;
const DEDUP_TTL_SECS: u64 = 2;

pub async fn run(harness: &TestHarness) {
    let client = harness
        .root_client()
        .await
        .expect("Failed to get root client");

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

    let stream_id = Identifier::named(STREAM_NAME).unwrap();
    let topic_id = Identifier::named(TOPIC_NAME).unwrap();
    let partitioning = Partitioning::partition_id(0);
    let consumer = Consumer::default();

    // Step 1: Send 10 messages with id=0 (server auto-generates UUID)
    let mut auto_messages = build_messages("auto-id", &[0; MESSAGES_PER_BATCH as usize]);
    client
        .send_messages(&stream_id, &topic_id, &partitioning, &mut auto_messages)
        .await
        .unwrap();

    let polled = poll_all(&client, &stream_id, &topic_id, &consumer).await;
    assert_eq!(polled.messages.len(), MESSAGES_PER_BATCH as usize);
    let auto_ids: HashSet<u128> = polled.messages.iter().map(|m| m.header.id).collect();
    assert_eq!(
        auto_ids.len(),
        MESSAGES_PER_BATCH as usize,
        "All auto-generated IDs must be unique"
    );

    // Step 2: Send 10 messages with explicit IDs 1-10
    let explicit_ids: Vec<u128> = (1..=MESSAGES_PER_BATCH as u128).collect();
    let mut original_messages = build_messages("original", &explicit_ids);
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &partitioning,
            &mut original_messages,
        )
        .await
        .unwrap();

    let polled = poll_all(&client, &stream_id, &topic_id, &consumer).await;
    assert_eq!(polled.messages.len(), (MESSAGES_PER_BATCH * 2) as usize);

    // Step 3: Re-send IDs 1-10 with different payload — duplicates should be dropped
    let mut duplicate_messages = build_messages("duplicate", &explicit_ids);
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &partitioning,
            &mut duplicate_messages,
        )
        .await
        .unwrap();

    let polled = poll_all(&client, &stream_id, &topic_id, &consumer).await;
    assert_eq!(
        polled.messages.len(),
        (MESSAGES_PER_BATCH * 2) as usize,
        "Duplicate messages must not increase count"
    );
    for msg in &polled.messages[MESSAGES_PER_BATCH as usize..] {
        let payload = std::str::from_utf8(&msg.payload).unwrap();
        assert!(
            payload.starts_with("original-"),
            "Original payload must be preserved, got: {payload}"
        );
    }

    // Step 4: Send all-duplicate batch (regression test for empty batch panic)
    let mut all_dup_messages = build_messages("all-dup", &explicit_ids);
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &partitioning,
            &mut all_dup_messages,
        )
        .await
        .unwrap();

    let polled = poll_all(&client, &stream_id, &topic_id, &consumer).await;
    assert_eq!(
        polled.messages.len(),
        (MESSAGES_PER_BATCH * 2) as usize,
        "All-duplicate batch must not change count"
    );

    // Step 5: Send mixed batch — IDs 6-15 (6-10 are duplicates, 11-15 are new)
    let mixed_start = MESSAGES_PER_BATCH as u128 / 2 + 1; // 6
    let mixed_end = mixed_start + MESSAGES_PER_BATCH as u128 - 1; // 15
    let mixed_ids: Vec<u128> = (mixed_start..=mixed_end).collect();
    let new_ids_count = (mixed_end - MESSAGES_PER_BATCH as u128) as usize; // 5
    let mut mixed_messages = build_messages("mixed", &mixed_ids);
    client
        .send_messages(&stream_id, &topic_id, &partitioning, &mut mixed_messages)
        .await
        .unwrap();

    let expected_total = (MESSAGES_PER_BATCH * 2) as usize + new_ids_count; // 25
    let polled = poll_all(&client, &stream_id, &topic_id, &consumer).await;
    assert_eq!(polled.messages.len(), expected_total);

    for msg in &polled.messages {
        let payload = std::str::from_utf8(&msg.payload).unwrap();
        let id = msg.header.id;
        if auto_ids.contains(&id) {
            assert!(
                payload.starts_with("auto-id-"),
                "Auto-id message payload mismatch: {payload}"
            );
        } else if id <= MESSAGES_PER_BATCH as u128 {
            assert!(
                payload.starts_with("original-"),
                "Original message payload mismatch for id {id}: {payload}"
            );
        } else {
            assert!(
                payload.starts_with("mixed-"),
                "New message payload mismatch for id {id}: {payload}"
            );
        }
    }

    // Step 6: Verify monotonically increasing offsets
    for window in polled.messages.windows(2) {
        assert!(
            window[1].header.offset > window[0].header.offset,
            "Offsets not monotonically increasing: {} followed by {}",
            window[0].header.offset,
            window[1].header.offset
        );
    }

    // Step 7: Wait for TTL expiry, then re-send IDs 1-10
    tokio::time::sleep(Duration::from_secs(DEDUP_TTL_SECS + 1)).await;

    let mut after_ttl_messages = build_messages("after-ttl", &explicit_ids);
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &partitioning,
            &mut after_ttl_messages,
        )
        .await
        .unwrap();

    let expected_after_ttl = expected_total + MESSAGES_PER_BATCH as usize; // 35
    let polled = poll_all(&client, &stream_id, &topic_id, &consumer).await;
    assert_eq!(
        polled.messages.len(),
        expected_after_ttl,
        "After TTL expiry, previously seen IDs must be accepted again"
    );

    let ttl_messages: Vec<&IggyMessage> = polled.messages[expected_total..].iter().collect();
    for msg in &ttl_messages {
        let payload = std::str::from_utf8(&msg.payload).unwrap();
        assert!(
            payload.starts_with("after-ttl-"),
            "After-TTL payload mismatch: {payload}"
        );
    }

    for window in polled.messages.windows(2) {
        assert!(
            window[1].header.offset > window[0].header.offset,
            "Offsets not monotonically increasing after TTL: {} followed by {}",
            window[0].header.offset,
            window[1].header.offset
        );
    }

    client
        .delete_stream(&Identifier::named(STREAM_NAME).unwrap())
        .await
        .unwrap();
}

fn build_messages(prefix: &str, ids: &[u128]) -> Vec<IggyMessage> {
    ids.iter()
        .map(|&id| {
            let payload = if id == 0 {
                format!("{prefix}-auto")
            } else {
                format!("{prefix}-{id}")
            };
            let mut builder = IggyMessage::builder().payload(Bytes::from(payload));
            if id != 0 {
                builder = builder.id(id);
            }
            builder.build().expect("Failed to build message")
        })
        .collect()
}

async fn poll_all(
    client: &IggyClient,
    stream_id: &Identifier,
    topic_id: &Identifier,
    consumer: &Consumer,
) -> PolledMessages {
    client
        .poll_messages(
            stream_id,
            topic_id,
            Some(0),
            consumer,
            &PollingStrategy::offset(0),
            1000,
            false,
        )
        .await
        .unwrap()
}
