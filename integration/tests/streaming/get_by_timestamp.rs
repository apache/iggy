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
use iggy::bytes_serializable::BytesSerializable;
use iggy::messages::send_messages::Message;
use iggy::models::header::{HeaderKey, HeaderValue};
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::expiry::IggyExpiry;
use iggy::utils::sizeable::Sizeable;
use iggy::utils::timestamp::IggyTimestamp;
use server::configs::resource_quota::MemoryResourceQuota;
use server::configs::system::{CacheConfig, PartitionConfig, SegmentConfig, SystemConfig};
use server::streaming::batching::appendable_batch_info::AppendableBatchInfo;
use server::streaming::partitions::partition::Partition;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::Arc;
use test_case::test_matrix;

/*
 * Below helper functions are here only to make test function name more readable.
 */

fn msg_size(size: u64) -> IggyByteSize {
    IggyByteSize::from_str(&format!("{}b", size)).unwrap()
}

fn segment_size(size: u64) -> IggyByteSize {
    IggyByteSize::from_str(&format!("{}b", size)).unwrap()
}

fn msgs_req_to_save(count: u32) -> u32 {
    count
}

fn msg_cache_size(size: u64) -> Option<IggyByteSize> {
    if size == 0 {
        return None;
    }
    Some(IggyByteSize::from_str(&format!("{}b", size)).unwrap())
}

fn index_cache_enabled() -> bool {
    true
}

fn index_cache_disabled() -> bool {
    false
}

#[test_matrix(
    [msg_size(50), msg_size(1000), msg_size(20000)],
    [msgs_req_to_save(3), msgs_req_to_save(10),  msgs_req_to_save(1000)],
    [segment_size(500), segment_size(2000), segment_size(100000)],
    [msg_cache_size(0), msg_cache_size(5000), msg_cache_size(50000), msg_cache_size(2000000)],
    [index_cache_disabled(), index_cache_enabled()])]
#[tokio::test]
async fn test_get_messages_by_timestamp(
    message_size: IggyByteSize,
    messages_required_to_save: u32,
    segment_size: IggyByteSize,
    msg_cache_size: Option<IggyByteSize>,
    index_cache_enabled: bool,
) {
    println!(
        "Running test with msg_cache_enabled: {}, messages_required_to_save: {}, segment_size: {}, message_size: {}, cache_indexes: {}",
        msg_cache_size.is_some(),
        messages_required_to_save,
        segment_size,
        message_size,
        index_cache_enabled
    );

    let setup = TestSetup::init().await;
    let stream_id = 1;
    let topic_id = 1;
    let partition_id = 1;

    // Define batch sizes for 5 appends
    let batch_sizes = [3, 4, 5, 6, 7];
    let total_messages: u32 = batch_sizes.iter().sum();

    let msg_cache_enabled = msg_cache_size.is_some();
    let msg_cache_size =
        MemoryResourceQuota::Bytes(msg_cache_size.unwrap_or(IggyByteSize::from_str("0").unwrap()));
    let config = Arc::new(SystemConfig {
        path: setup.config.path.to_string(),
        cache: CacheConfig {
            enabled: msg_cache_enabled,
            size: msg_cache_size,
        },
        partition: PartitionConfig {
            messages_required_to_save,
            enforce_fsync: true,
            ..Default::default()
        },
        segment: SegmentConfig {
            cache_indexes: index_cache_enabled,
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
    )
    .await;

    setup.create_partitions_directory(stream_id, topic_id).await;
    partition.persist().await.unwrap();

    let mut all_messages = Vec::with_capacity(total_messages as usize);
    for i in 1..=total_messages {
        let id = i as u128;
        let beginning_of_payload = format!("message {}", i);
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
        let message = Message {
            id,
            length: payload.len() as u32,
            payload: payload.clone(),
            headers: Some(headers),
        };
        all_messages.push(message);
    }

    let initial_timestamp = IggyTimestamp::now();
    let mut batch_timestamps = Vec::with_capacity(batch_sizes.len());
    let mut current_pos = 0;

    // Append all batches with timestamps
    for batch_size in batch_sizes {
        let batch = all_messages[current_pos..current_pos + batch_size as usize].to_vec();
        let batch_info = AppendableBatchInfo::new(
            batch
                .iter()
                .map(|msg| msg.get_size_bytes())
                .sum::<IggyByteSize>(),
            partition.partition_id,
        );
        partition
            .append_messages(batch_info, batch.clone(), None)
            .await
            .unwrap();

        batch_timestamps.push(IggyTimestamp::now());
        current_pos += batch_size as usize;
    }

    let final_timestamp = IggyTimestamp::now();

    // Test 1: All messages from initial timestamp
    let all_loaded_messages = partition
        .get_messages_by_timestamp(initial_timestamp, total_messages)
        .await
        .unwrap();
    assert_eq!(
        all_loaded_messages.len(),
        total_messages as usize,
        "Expected {} messages from initial timestamp, but got {}",
        total_messages,
        all_loaded_messages.len()
    );

    // Test 2: Get messages from middle timestamp (after 3rd batch)
    let middle_timestamp = batch_timestamps[2];
    let remaining_messages = total_messages - (batch_sizes[0] + batch_sizes[1] + batch_sizes[2]);
    let middle_messages = partition
        .get_messages_by_timestamp(middle_timestamp, remaining_messages)
        .await
        .unwrap();
    assert_eq!(
        middle_messages.len(),
        remaining_messages as usize,
        "Expected {} messages from middle timestamp, but got {}",
        remaining_messages,
        middle_messages.len()
    );

    // Test 3: No messages from final timestamp
    let no_messages = partition
        .get_messages_by_timestamp(final_timestamp, 1)
        .await
        .unwrap();
    assert_eq!(
        no_messages.len(),
        0,
        "Expected no messages from final timestamp, but got {}",
        no_messages.len()
    );

    // Test 4: Small subset from initial timestamp
    let subset_size = 3;
    let subset_messages = partition
        .get_messages_by_timestamp(initial_timestamp, subset_size)
        .await
        .unwrap();
    assert_eq!(
        subset_messages.len(),
        subset_size as usize,
        "Expected {} messages in subset from initial timestamp, but got {}",
        subset_size,
        subset_messages.len()
    );
    for i in 0..subset_messages.len() {
        let loaded_message = &subset_messages[i];
        let original_message = &all_messages[i];
        assert_eq!(
            loaded_message.id, original_message.id,
            "Message ID mismatch at position {}: expected {}, got {}",
            i, original_message.id, loaded_message.id
        );
        assert_eq!(
            loaded_message.payload, original_message.payload,
            "Payload mismatch at position {}: expected {:?}, got {:?}",
            i, original_message.payload, loaded_message.payload
        );
        assert_eq!(
            loaded_message
                .headers
                .as_ref()
                .map(|bytes| HashMap::from_bytes(bytes.clone()).unwrap()),
            original_message.headers,
            "Headers mismatch at position {}: expected {:?}, got {:?}",
            i,
            original_message.headers,
            loaded_message
                .headers
                .as_ref()
                .map(|bytes| HashMap::from_bytes(bytes.clone()).unwrap())
        );
        assert!(
            loaded_message.timestamp >= initial_timestamp.as_micros(),
            "Message timestamp {} at position {} is less than initial timestamp {}",
            loaded_message.timestamp,
            i,
            initial_timestamp
        );
    }

    // Test 5: Messages spanning multiple batches (from middle of 2nd batch timestamp)
    let span_timestamp = batch_timestamps[1];
    let span_size = 8; // Should span across 2nd, 3rd, and into 4th batch
    let spanning_messages = partition
        .get_messages_by_timestamp(span_timestamp, span_size)
        .await
        .unwrap();
    assert_eq!(
        spanning_messages.len(),
        span_size as usize,
        "Expected {} messages spanning multiple batches, but got {}",
        span_size,
        spanning_messages.len()
    );
    for msg in spanning_messages.iter() {
        assert!(
            msg.timestamp >= span_timestamp.as_micros(),
            "Message timestamp {} should be >= span timestamp {}",
            msg.timestamp,
            span_timestamp
        );
    }
}
