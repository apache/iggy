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
    [msgs_req_to_save(10), msgs_req_to_save(24)],
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
        let message = Message {
            id,
            length: payload.len() as u32,
            payload: payload.clone(),
            headers: Some(headers),
        };
        all_messages.push(message);
    }

    // Keep track of offsets after each batch
    let mut batch_offsets = Vec::with_capacity(batch_sizes.len());
    let mut current_pos = 0;

    // Append all batches
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

        batch_offsets.push(partition.current_offset);
        current_pos += batch_size as usize;
    }

    // Test 1: All messages from start
    let all_loaded_messages = partition
        .get_messages_by_offset(0, total_messages)
        .await
        .unwrap();
    assert_eq!(
        all_loaded_messages.len(),
        total_messages as usize,
        "Expected {} messages from start, but got {}",
        total_messages,
        all_loaded_messages.len()
    );

    // Test 2: Get messages from middle (after 3rd batch)
    let middle_offset = batch_offsets[2];
    let remaining_messages = total_messages - (batch_sizes[0] + batch_sizes[1] + batch_sizes[2]);
    let middle_messages = partition
        .get_messages_by_offset(middle_offset + 1, remaining_messages)
        .await
        .unwrap();
    assert_eq!(
        middle_messages.len(),
        remaining_messages as usize,
        "Expected {} messages from middle offset, but got {}",
        remaining_messages,
        middle_messages.len()
    );

    // Test 3: No messages beyond final offset
    let final_offset = batch_offsets.last().unwrap();
    let no_messages = partition
        .get_messages_by_offset(final_offset + 1, 1)
        .await
        .unwrap();
    assert_eq!(
        no_messages.len(),
        0,
        "Expected no messages beyond final offset, but got {}",
        no_messages.len()
    );

    // Test 4: Small subset from start
    let subset_size = 3;
    let subset_messages = partition
        .get_messages_by_offset(0, subset_size)
        .await
        .unwrap();
    assert_eq!(
        subset_messages.len(),
        subset_size as usize,
        "Expected {} messages in subset from start, but got {}",
        subset_size,
        subset_messages.len()
    );

    // Test 5: Messages spanning multiple batches
    let span_offset = batch_offsets[1] + 1; // Start from middle of 2nd batch
    let span_size = 8; // Should span across 2nd, 3rd, and into 4th batch
    let spanning_messages = partition
        .get_messages_by_offset(span_offset, span_size)
        .await
        .unwrap();
    assert_eq!(
        spanning_messages.len(),
        span_size as usize,
        "Expected {} messages spanning multiple batches, but got {}",
        span_size,
        spanning_messages.len()
    );

    // Test 6: Validate message content and ordering
    for (i, msg) in spanning_messages.iter().enumerate() {
        let expected_offset = span_offset + i as u64;
        assert!(
            msg.offset >= expected_offset,
            "Message offset {} at position {} should be >= expected offset {}",
            msg.offset,
            i,
            expected_offset
        );

        // Verify message contents match original
        let original_idx = msg.offset as usize;
        let original_message = &all_messages[original_idx];
        assert_eq!(
            msg.id, original_message.id,
            "Message ID mismatch at offset {}: expected {}, got {}",
            msg.offset, original_message.id, msg.id
        );
        assert_eq!(
            msg.payload, original_message.payload,
            "Payload mismatch at offset {}: expected {:?}, got {:?}",
            msg.offset, original_message.payload, msg.payload
        );
        assert_eq!(
            msg.headers
                .as_ref()
                .map(|bytes| HashMap::from_bytes(bytes.clone()).unwrap()),
            original_message.headers,
            "Headers mismatch at offset {}: expected {:?}, got {:?}",
            msg.offset,
            original_message.headers,
            msg.headers
                .as_ref()
                .map(|bytes| HashMap::from_bytes(bytes.clone()).unwrap())
        );
    }
}
