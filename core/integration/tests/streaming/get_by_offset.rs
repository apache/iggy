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

use crate::streaming::common::test_setup::TestSetup;
use bytes::BytesMut;
use iggy::prelude::*;
use server::configs::cache_indexes::CacheIndexesConfig;
use server::configs::system::{PartitionConfig, SegmentConfig, SystemConfig};
use server::streaming::partitions::partition::Partition;
use server::streaming::segments::IggyMessagesBatchMut;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64};
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
async fn test_get_messages_by_offset(
    message_size: IggyByteSize,
    batch_lengths: Vec<u32>,
    messages_required_to_save: u32,
    segment_size: IggyByteSize,
    cache_indexes: CacheIndexesConfig,
) {
    println!(
        "Running test with message_size: {message_size}, batches: {batch_lengths:?}, messages_required_to_save: {messages_required_to_save}, segment_size: {segment_size}, cache_indexes: {cache_indexes}"
    );

    let setup = TestSetup::init().await;
    let stream_id = 1;
    let topic_id = 1;
    let partition_id = 1;

    let total_messages_count = batch_lengths.iter().sum();

    let config = Arc::new(SystemConfig {
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
    });

    let mut partition = Partition::create(
        stream_id,
        topic_id,
        partition_id,
        true,
        config.clone(),
        setup.storage.clone(),
        IggyExpiry::NeverExpire,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU32::new(0)),
        IggyTimestamp::now(),
    );

    setup.create_partitions_directory(stream_id, topic_id).await;
    partition.persist().await.unwrap();
    partition.open().await.unwrap();

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

    // Keep track of offsets after each batch
    let mut batch_offsets = Vec::with_capacity(batch_lengths.len());
    let mut current_pos = 0;

    // Append all batches as defined in the test matrix
    for (batch_idx, &batch_len) in batch_lengths.iter().enumerate() {
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
        partition.append_messages(batch).await.unwrap();

        batch_offsets.push(partition.current_offset);
        current_pos += batch_len as usize;
    }

    // Use the exact total messages count from the test matrix
    let total_sent_messages = total_messages_count;

    // Test 1: All messages from start
    let all_loaded_messages = partition
        .get_messages_by_offset(0, total_sent_messages)
        .await
        .unwrap();
    assert_eq!(
        all_loaded_messages.count(),
        total_sent_messages,
        "Expected {} messages from start, but got {}",
        total_sent_messages,
        all_loaded_messages.count()
    );

    // Test 2: Get messages from middle (after 3rd batch)
    if batch_offsets.len() >= 3 {
        let middle_offset = batch_offsets[2];
        let prior_batches_sum: u32 = batch_lengths[..3].iter().sum();
        let remaining_messages = total_sent_messages - prior_batches_sum;

        let middle_messages = partition
            .get_messages_by_offset(middle_offset + 1, remaining_messages)
            .await
            .unwrap();

        assert_eq!(
            middle_messages.count(),
            remaining_messages,
            "Expected {} messages from middle offset, but got {}",
            remaining_messages,
            middle_messages.count()
        );
    }

    // Test 3: No messages beyond final offset
    if !batch_offsets.is_empty() {
        let final_offset = *batch_offsets.last().unwrap();
        let no_messages = partition
            .get_messages_by_offset(final_offset + 1, 1)
            .await
            .unwrap();
        assert_eq!(
            no_messages.count(),
            0,
            "Expected no messages beyond final offset, but got {}",
            no_messages.count()
        );
    }

    // Test 4: Small subset from start
    let subset_size = std::cmp::min(3, total_sent_messages);
    let subset_messages = partition
        .get_messages_by_offset(0, subset_size)
        .await
        .unwrap();
    assert_eq!(
        subset_messages.count(),
        subset_size,
        "Expected {} messages in subset from start, but got {}",
        subset_size,
        subset_messages.count()
    );

    // Test 5: Messages spanning multiple batches
    if batch_offsets.len() >= 4 {
        let span_offset = batch_offsets[1] + 1; // Start from middle of 2nd batch
        let span_size = 8; // Should span across 2nd, 3rd, and into 4th batch
        let batches = partition
            .get_messages_by_offset(span_offset, span_size)
            .await
            .unwrap();
        assert_eq!(
            batches.count(),
            span_size,
            "Expected {} messages spanning multiple batches, but got {}",
            span_size,
            batches.count()
        );

        // Test 6: Validate message content and ordering for all messages
        let mut i = 0;

        for batch in batches.iter() {
            for msg in batch.iter() {
                let expected_offset = span_offset + i as u64;
                assert!(
                    msg.header().offset() >= expected_offset,
                    "Message offset {} at position {} should be >= expected offset {}",
                    msg.header().offset(),
                    i,
                    expected_offset
                );

                let original_offset = msg.header().offset() as usize;
                if original_offset < all_messages.len() {
                    let original_message = &all_messages[original_offset];

                    let loaded_id = msg.header().id();
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
                i += 1;
            }
        }
    }

    // Add sequential read test for all batch sizes
    println!("Verifying sequential reads, expecting {total_sent_messages} messages");

    let chunk_size = 500;
    let mut verified_count = 0;

    for chunk_start in (0..total_sent_messages).step_by(chunk_size as usize) {
        let read_size = std::cmp::min(chunk_size, total_sent_messages - chunk_start);

        let chunk = partition
            .get_messages_by_offset(chunk_start as u64, read_size)
            .await
            .unwrap();

        assert_eq!(
            chunk.count(),
            read_size,
            "Failed to read chunk at offset {chunk_start} with size {read_size}"
        );

        verified_count += chunk.count();

        println!(
            "Read chunk at offset {chunk_start} with size {read_size}, verified count: {verified_count}"
        );
    }

    assert_eq!(
        verified_count, total_sent_messages,
        "Sequential chunk reads didn't cover all messages"
    );
}
