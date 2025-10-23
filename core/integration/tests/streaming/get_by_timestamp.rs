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

use super::bootstrap_test_environment;
use crate::streaming::common::test_setup::TestSetup;
use bytes::BytesMut;
use iggy::prelude::*;
use server::configs::cache_indexes::CacheIndexesConfig;
use server::configs::system::{PartitionConfig, SegmentConfig, SystemConfig};
use server::shard::namespace::IggyFullNamespace;
use server::shard::system::messages::PollingArgs;
use server::streaming::polling_consumer::PollingConsumer;
use server::streaming::segments::IggyMessagesBatchMut;
use server::streaming::traits::MainOps;
use std::collections::HashMap;
use std::str::FromStr;
use std::thread::sleep;
use test_case::test_matrix;

/*
 * Below helper functions are here only to make test function name more readable.
 */

fn msg_size(size: u64) -> IggyByteSize {
    IggyByteSize::from_str(&format!("{size}B")).unwrap()
}

fn segment_size(size: u64) -> IggyByteSize {
    IggyByteSize::from_str(&format!("{size}B")).unwrap()
}

fn msgs_req_to_save(count: u32) -> u32 {
    count
}

fn index_cache_all() -> CacheIndexesConfig {
    CacheIndexesConfig::All
}

fn index_cache_none() -> CacheIndexesConfig {
    CacheIndexesConfig::None
}

fn index_cache_open_segment() -> CacheIndexesConfig {
    CacheIndexesConfig::OpenSegment
}

fn small_batches() -> Vec<u32> {
    vec![3, 4, 5, 6, 7]
}

fn medium_batches() -> Vec<u32> {
    vec![10, 20, 30, 40]
}

fn large_batches() -> Vec<u32> {
    vec![100, 200, 300, 400]
}

fn very_large_batches() -> Vec<u32> {
    vec![500, 1000, 1500, 1000]
}

#[test_matrix(
    [msg_size(50), msg_size(1000), msg_size(20000)],
    [small_batches(), medium_batches(), large_batches(), very_large_batches()],
    [msgs_req_to_save(1), msgs_req_to_save(24), msgs_req_to_save(1000), msgs_req_to_save(10000)],
    [segment_size(10), segment_size(200), segment_size(10000000)],
    [index_cache_none(), index_cache_all(), index_cache_open_segment()])]
#[compio::test]
async fn test_get_messages_by_timestamp(
    message_size: IggyByteSize,
    batch_lengths: Vec<u32>,
    messages_required_to_save: u32,
    segment_size: IggyByteSize,
    cache_indexes: CacheIndexesConfig,
) {
    println!(
        "Running test with message_size: {message_size}, batches: {batch_lengths:?}, messages_required_to_save: {messages_required_to_save}, segment_size: {segment_size}, cache_indexes: {cache_indexes}"
    );

    let shard_id = 0;
    let setup = TestSetup::init().await;

    let total_messages_count = batch_lengths.iter().sum();

    let config = SystemConfig {
        path: setup.config.path.to_string(),
        partition: PartitionConfig {
            messages_required_to_save,
            enforce_fsync: true,
            ..Default::default()
        },
        segment: SegmentConfig {
            cache_indexes,
            size: segment_size,
            ..Default::default()
        },
        ..Default::default()
    };

    // Use the bootstrap method to create streams with proper slab structure
    let bootstrap_result = bootstrap_test_environment(shard_id as u16, &config)
        .await
        .unwrap();
    let streams = bootstrap_result.streams;
    let stream_identifier = bootstrap_result.stream_id;
    let topic_identifier = bootstrap_result.topic_id;
    let partition_id = bootstrap_result.partition_id;
    let task_registry = bootstrap_result.task_registry;

    // Create namespace for MainOps calls
    let namespace = IggyFullNamespace::new(
        stream_identifier.clone(),
        topic_identifier.clone(),
        partition_id,
    );

    let mut all_messages = Vec::with_capacity(total_messages_count as usize);

    // Generate all messages as defined in the test matrix
    for i in 1..=total_messages_count {
        let id = i as u128;
        let beginning_of_payload = format!("message {i}");
        let mut payload = BytesMut::new();
        payload.extend_from_slice(beginning_of_payload.as_bytes());
        payload.resize(message_size.as_bytes_usize(), 0xD);
        let payload = payload.freeze();

        let mut headers = HashMap::new();
        headers.insert(
            HeaderKey::new("key_1").unwrap(),
            HeaderValue::from_str("Value 1").unwrap(),
        );
        headers.insert(
            HeaderKey::new("key 2").unwrap(),
            HeaderValue::from_bool(true).unwrap(),
        );
        headers.insert(
            HeaderKey::new("key-3").unwrap(),
            HeaderValue::from_uint64(123456).unwrap(),
        );

        let message = IggyMessage::builder()
            .id(id)
            .payload(payload)
            .user_headers(headers)
            .build()
            .expect("Failed to create message with valid payload and headers");

        all_messages.push(message);
    }

    // Timestamp tracking for messages
    let initial_timestamp = IggyTimestamp::now();
    let mut batch_timestamps = Vec::with_capacity(batch_lengths.len());
    let mut current_pos = 0;

    // Append all batches as defined in the test matrix with separate timestamps
    for (batch_idx, &batch_len) in batch_lengths.iter().enumerate() {
        // Add a small delay between batches to ensure distinct timestamps
        sleep(std::time::Duration::from_millis(2));

        // If we've generated too many messages, skip the rest
        if current_pos + batch_len as usize > all_messages.len() {
            break;
        }

        println!(
            "Appending batch {}/{} with {} messages",
            batch_idx + 1,
            batch_lengths.len(),
            batch_len
        );

        let batch_end_pos = current_pos + batch_len as usize;
        let messages_slice_to_append = &all_messages[current_pos..batch_end_pos];

        let messages_size = messages_slice_to_append
            .iter()
            .map(|m| m.get_size_bytes().as_bytes_u64() as u32)
            .sum();

        let batch = IggyMessagesBatchMut::from_messages(messages_slice_to_append, messages_size);
        assert_eq!(batch.count(), batch_len);
        streams
            .append_messages(&config, &task_registry, &namespace, batch)
            .await
            .unwrap();

        // Capture the timestamp of this batch
        batch_timestamps.push(IggyTimestamp::now());
        current_pos += batch_len as usize;

        // Add a small delay between batches to ensure distinct timestamps
        sleep(std::time::Duration::from_millis(2));
    }

    let final_timestamp = IggyTimestamp::now();

    // Use the exact total messages count from the test matrix
    let total_sent_messages = total_messages_count;

    // Create a single consumer to reuse throughout the test
    let consumer =
        PollingConsumer::consumer(&Identifier::numeric(1).unwrap(), partition_id as usize);

    // Test 1: All messages from initial timestamp
    let args = PollingArgs::new(
        PollingStrategy::timestamp(initial_timestamp),
        total_sent_messages,
        false,
    );
    let (_, all_loaded_messages) = streams
        .poll_messages(&namespace, consumer, args)
        .await
        .unwrap();
    assert_eq!(
        all_loaded_messages.count(),
        total_sent_messages,
        "Expected {} messages from initial timestamp, but got {}",
        total_sent_messages,
        all_loaded_messages.count()
    );

    // Test 2: Get messages from middle timestamp (after 3rd batch)
    if batch_timestamps.len() >= 3 {
        // Use a timestamp that's just before the 3rd batch's timestamp to ensure we get messages
        // from that batch onwards
        let middle_timestamp = IggyTimestamp::from(batch_timestamps[2].as_micros() + 1000);

        // Calculate how many messages should be in batches after the 3rd
        let prior_batches_sum: u32 = batch_lengths[..3].iter().sum();
        let remaining_messages = total_sent_messages - prior_batches_sum;

        let args = PollingArgs::new(
            PollingStrategy::timestamp(middle_timestamp),
            remaining_messages,
            false,
        );
        let (_, middle_messages) = streams
            .poll_messages(&namespace, consumer, args)
            .await
            .unwrap();

        assert_eq!(
            middle_messages.count(),
            remaining_messages,
            "Expected {} messages from middle timestamp, but got {}",
            remaining_messages,
            middle_messages.count()
        );
    }

    // Test 3: No messages after final timestamp
    let args = PollingArgs::new(PollingStrategy::timestamp(final_timestamp), 1, false);
    let (_, no_messages) = streams
        .poll_messages(&namespace, consumer, args)
        .await
        .unwrap();
    assert_eq!(
        no_messages.count(),
        0,
        "Expected no messages after final timestamp, but got {}",
        no_messages.count()
    );

    // Test 4: Small subset from initial timestamp
    let subset_size = std::cmp::min(3, total_sent_messages);
    let args = PollingArgs::new(
        PollingStrategy::timestamp(initial_timestamp),
        subset_size,
        false,
    );
    let (_, subset_messages) = streams
        .poll_messages(&namespace, consumer, args)
        .await
        .unwrap();
    assert_eq!(
        subset_messages.count(),
        subset_size,
        "Expected {} messages in subset from initial timestamp, but got {}",
        subset_size,
        subset_messages.count()
    );

    // Test 5: Messages spanning multiple batches by timestamp
    if batch_timestamps.len() >= 4 {
        // Use a timestamp that's just before the 2nd batch's timestamp
        let span_timestamp = IggyTimestamp::from(batch_timestamps[1].as_micros() + 1000);
        let span_size = 8; // Should span across multiple batches

        let args = PollingArgs::new(PollingStrategy::timestamp(span_timestamp), span_size, false);
        let (_, spanning_messages) = streams
            .poll_messages(&namespace, consumer, args)
            .await
            .unwrap();

        assert_eq!(
            spanning_messages.count(),
            span_size,
            "Expected {} messages spanning multiple batches, but got {}",
            span_size,
            spanning_messages.count()
        );

        // Verify that all messages have timestamps >= our reference timestamp
        let span_timestamp_micros = span_timestamp.as_micros();

        // Test 6: Validate message content and ordering
        for batch in spanning_messages.iter() {
            for msg in batch.iter() {
                let msg_timestamp = msg.header().timestamp();
                assert!(
                    msg_timestamp >= span_timestamp_micros,
                    "Message timestamp {msg_timestamp} should be >= span timestamp {span_timestamp_micros}"
                );

                // Verify message content
                let loaded_id = msg.header().id();
                let original_offset = msg.header().offset() as usize;

                if original_offset < all_messages.len() {
                    let original_message = &all_messages[original_offset];
                    let original_id = original_message.header.id;

                    assert_eq!(
                        loaded_id,
                        original_id,
                        "Message ID mismatch at offset {}",
                        msg.header().offset(),
                    );

                    let loaded_payload = msg.payload();
                    let original_payload = &original_message.payload;
                    assert_eq!(
                        loaded_payload,
                        original_payload,
                        "Payload mismatch at offset {}",
                        msg.header().offset(),
                    );

                    let loaded_headers = msg.user_headers_map().unwrap().unwrap();
                    let original_headers =
                        HashMap::from_bytes(original_message.user_headers.clone().unwrap())
                            .unwrap();
                    assert_eq!(
                        loaded_headers,
                        original_headers,
                        "Headers mismatch at offset {}",
                        msg.header().offset(),
                    );
                }
            }
        }
    }
}
