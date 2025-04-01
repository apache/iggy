use crate::streaming::common::test_setup::TestSetup;
use bytes::Bytes;
use iggy::prelude::*;
use server::configs::system::{PartitionConfig, SystemConfig};
use server::state::system::PartitionState;
use server::streaming::partitions::partition::Partition;
use server::streaming::segments::IggyMessagesBatchMut;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::Arc;

#[tokio::test]
async fn should_persist_messages_and_then_load_them_by_timestamp() {
    let setup = TestSetup::init().await;
    let stream_id = 1;
    let topic_id = 1;
    let partition_id = 1;
    let messages_count = 100;
    let config = Arc::new(SystemConfig {
        path: setup.config.path.to_string(),
        partition: PartitionConfig {
            messages_required_to_save: messages_count,
            enforce_fsync: true,
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

    let mut messages = Vec::with_capacity(messages_count as usize);
    let mut appended_messages = Vec::with_capacity(messages_count as usize);
    let mut messages_two = Vec::with_capacity(messages_count as usize);
    for i in 1..=messages_count {
        let id = i as u128;
        let payload = Bytes::from(format!("message {}", i));
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
            .build()
            .expect("Failed to create message with valid payload and headers");

        messages.push(message);
    }

    for i in (messages_count + 1)..=(messages_count * 2) {
        let id = i as u128;
        let payload = Bytes::from(format!("message {}", i));
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
            .payload(payload.clone())
            .headers(headers.clone())
            .build()
            .expect("Failed to create message with valid payload and headers");

        let message_clone = IggyMessage::builder()
            .id(id)
            .payload(payload)
            .headers(headers)
            .build()
            .expect("Failed to create message with valid payload and headers");

        appended_messages.push(message);
        messages_two.push(message_clone);
    }

    setup.create_partitions_directory(stream_id, topic_id).await;
    partition.persist().await.unwrap();
    let messages_size = messages
        .iter()
        .map(|msg| msg.get_size_bytes().as_bytes_u32())
        .sum::<u32>();
    let batch = IggyMessagesBatchMut::from_messages(&messages, messages_size);

    let messages_size_two = messages_two
        .iter()
        .map(|msg| msg.get_size_bytes().as_bytes_u32())
        .sum::<u32>();
    let batch_two = IggyMessagesBatchMut::from_messages(&messages_two, messages_size_two);

    partition.append_messages(batch, None).await.unwrap();
    let test_timestamp = IggyTimestamp::now();
    partition.append_messages(batch_two, None).await.unwrap();

    let loaded_messages = partition
        .get_messages_by_timestamp(test_timestamp, messages_count)
        .await
        .unwrap();

    assert_eq!(
        loaded_messages.count(),
        messages_count,
        "Unexpected loaded messages count"
    );
    (0..loaded_messages.count() as usize).for_each(|i| {
        let loaded_message = &loaded_messages.get(i).unwrap();
        let appended_message = &appended_messages[i];
        assert_eq!(
            loaded_message.header().id(),
            appended_message.header.id,
            "Message ID mismatch at position {i}"
        );
        assert_eq!(
            loaded_message.payload(),
            appended_message.payload,
            "Payload mismatch at position {i}",
        );
        assert!(
            loaded_message.header().timestamp() >= test_timestamp.as_micros(),
            "Message timestamp {} at position {} is less than test timestamp {}",
            loaded_message.header().timestamp(),
            i,
            test_timestamp
        );
        assert_eq!(
            loaded_message.user_headers_map().unwrap().unwrap(),
            appended_message.user_headers_map().unwrap().unwrap(),
            "Headers mismatch at position {i}",
        );
    });
}

#[tokio::test]
async fn should_persist_messages_and_then_load_them_from_disk() {
    let setup = TestSetup::init().await;
    let stream_id = 1;
    let topic_id = 1;
    let partition_id = 1;
    let messages_count = 1000;
    let config = Arc::new(SystemConfig {
        path: setup.config.path.to_string(),
        partition: PartitionConfig {
            messages_required_to_save: messages_count,
            enforce_fsync: true,
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

    let mut messages = Vec::with_capacity(messages_count as usize);
    let mut appended_messages = Vec::with_capacity(messages_count as usize);
    for i in 1..=messages_count {
        let id = i as u128;
        let payload = Bytes::from(format!("message {}", i));
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

        let appended_message =
            IggyMessage::with_id_and_headers(id, payload.clone(), headers.clone());
        let message = IggyMessage::with_id_and_headers(id, payload, headers);

        appended_messages.push(appended_message);
        messages.push(message);
    }

    setup.create_partitions_directory(stream_id, topic_id).await;
    partition.persist().await.unwrap();
    let messages_size = messages
        .iter()
        .map(|msg| msg.get_size_bytes().as_bytes_u32())
        .sum::<u32>();
    let batch = IggyMessagesBatchMut::from_messages(&messages, messages_size);
    partition.append_messages(batch, None).await.unwrap();
    assert_eq!(
        partition.unsaved_messages_count, 0,
        "Expected unsaved messages count to be 0, but got {}",
        partition.unsaved_messages_count
    );

    let now = IggyTimestamp::now();
    let mut loaded_partition = Partition::create(
        stream_id,
        topic_id,
        partition.partition_id,
        false,
        config.clone(),
        setup.storage.clone(),
        IggyExpiry::NeverExpire,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU32::new(0)),
        now,
    )
    .await;
    let partition_state = PartitionState {
        id: partition.partition_id,
        created_at: now,
    };
    loaded_partition.load(partition_state).await.unwrap();
    let loaded_messages = loaded_partition
        .get_messages_by_offset(0, messages_count)
        .await
        .unwrap();
    assert_eq!(
        loaded_messages.count(),
        messages_count,
        "Unexpected loaded messages count "
    );
    (0..messages_count as usize).for_each(|i| {
        let loaded_message = &loaded_messages.get(i).unwrap();
        let appended_message = &appended_messages[i];
        assert_eq!(
            loaded_message.header().id(),
            appended_message.header.id,
            "Message ID mismatch at position {i}",
        );
        assert_eq!(
            loaded_message.payload(),
            appended_message.payload,
            "Payload mismatch at position {i}",
        );
        assert_eq!(
            loaded_message.user_headers_map().unwrap().unwrap(),
            appended_message.user_headers_map().unwrap().unwrap(),
            "Headers mismatch at position {i}",
        );
    });
}
