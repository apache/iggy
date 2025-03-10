use crate::streaming::common::test_setup::TestSetup;
use bytes::{Bytes, BytesMut};
use iggy::prelude::*;
use server::configs::resource_quota::MemoryResourceQuota;
use server::configs::system::{CacheConfig, PartitionConfig, SegmentConfig, SystemConfig};
use server::streaming::partitions::partition::Partition;
use server::streaming::segments::IggyMessagesMut;
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
    [msgs_req_to_save(10), msgs_req_to_save(24), msgs_req_to_save(1000)],
    [segment_size(500), segment_size(2000), segment_size(100000)],
    [msg_cache_size(0), msg_cache_size(5000), msg_cache_size(50000), msg_cache_size(2000000)],
    [index_cache_disabled(), index_cache_enabled()])]
#[tokio::test]
async fn test_get_messages_by_offset(
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
        let message = IggyMessage::builder()
            .id(id)
            .payload(payload)
            .headers(headers)
            .build();
        all_messages.push(message);
    }

    // Keep track of offsets after each batch
    let mut batch_offsets = Vec::with_capacity(batch_sizes.len());
    let mut current_pos = 0;

    // Append all batches
    for batch_size in batch_sizes {
        println!("batch_size: {}, current_pos: {}", batch_size, current_pos);
        let batch =
            IggyMessagesMut::from(&all_messages[current_pos..current_pos + batch_size as usize]);
        assert_eq!(batch.count(), batch_size);
        partition.append_messages(batch, None).await.unwrap();

        batch_offsets.push(partition.current_offset);
        current_pos += batch_size as usize;
    }

    // Test 1: All messages from start
    let all_loaded_messages = partition
        .get_messages_by_offset(0, total_messages)
        .await
        .unwrap();
    assert_eq!(
        all_loaded_messages.count(),
        total_messages,
        "Expected {} messages from start, but got {}",
        total_messages,
        all_loaded_messages.count()
    );

    // Test 2: Get messages from middle (after 3rd batch)
    let middle_offset = batch_offsets[2];
    let remaining_messages = total_messages - (batch_sizes[0] + batch_sizes[1] + batch_sizes[2]);
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

    // Test 3: No messages beyond final offset
    let final_offset = batch_offsets.last().unwrap();
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

    // Test 4: Small subset from start
    let subset_size = 3;
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
    let span_offset = batch_offsets[1] + 1; // Start from middle of 2nd batch
    let span_size = 8; // Should span across 2nd, 3rd, and into 4th batch
    let messages = partition
        .get_messages_by_offset(span_offset, span_size)
        .await
        .unwrap();
    assert_eq!(
        messages.count(),
        span_size,
        "Expected {} messages spanning multiple batches, but got {}",
        span_size,
        messages.count()
    );

    // Test 6: Validate message content and ordering
    // Convert to a Vec of IggyMessage to avoid lifetime issues
    let message_vec = messages.to_messages();

    for (i, msg) in message_vec.iter().enumerate() {
        let expected_offset = span_offset + i as u64;
        assert!(
            msg.msg_header().offset() >= expected_offset,
            "Message offset {} at position {} should be >= expected offset {}",
            msg.msg_header().offset(),
            i,
            expected_offset
        );

        // Verify message contents match original
        let original_idx = msg.msg_header().offset() as usize;

        let original_message = &all_messages[original_idx];
        let original_msg_header = &original_message.header;
        let original_headers = &original_message.headers.as_ref().unwrap().to_bytes();
        let original_payload = &original_message.payload;

        let msg_header = msg.msg_header();
        let msg_payload = msg.payload();
        let msg_headers = msg.headers();

        assert_eq!(
            msg_header.id(),
            original_msg_header.id,
            "Message ID mismatch at offset {}: expected {}, got {}",
            msg_header.offset(),
            original_msg_header.id,
            msg.msg_header().id()
        );
        assert_eq!(
            msg_payload,
            original_payload,
            "Payload mismatch at offset {}: expected {:?}, got {:?}",
            msg_header.offset(),
            original_payload,
            msg_payload
        );
        assert_eq!(
            &msg_headers,
            &original_headers,
            "Headers mismatch at offset {}: expected {:?}, got {:?}",
            msg_header.offset(),
            original_headers,
            msg_headers
        );
    }
}
