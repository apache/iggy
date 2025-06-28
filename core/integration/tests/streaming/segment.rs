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
use crate::streaming::create_message;
use bytes::Bytes;
use iggy::prelude::locking::IggySharedMutFn;
use iggy::prelude::*;
use server::configs::server::{DataMaintenanceConfig, PersonalAccessTokenConfig};
use server::configs::system::{PartitionConfig, SegmentConfig, SystemConfig};
use server::streaming::segments::*;
use server::streaming::session::Session;
use std::fs::DirEntry;
use std::net::{Ipv4Addr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;
use tokio::fs;
use tokio::time::sleep;

#[tokio::test]
async fn should_persist_segment() {
    let setup = TestSetup::init().await;
    let stream_id = 1;
    let topic_id = 2;
    let partition_id = 3;
    let start_offsets = get_start_offsets();
    for start_offset in start_offsets {
        let mut segment = Segment::create(
            stream_id,
            topic_id,
            partition_id,
            start_offset,
            setup.config.clone(),
            IggyExpiry::NeverExpire,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            true,
        );

        setup
            .create_partition_directory(stream_id, topic_id, partition_id)
            .await;
        segment.persist().await.unwrap();
        assert_persisted_segment(
            &setup
                .config
                .get_partition_path(stream_id, topic_id, partition_id),
            start_offset,
        )
        .await;
    }
}

#[tokio::test]
async fn should_load_existing_segment_from_disk() {
    let setup = TestSetup::init().await;
    let stream_id = 1;
    let topic_id = 2;
    let partition_id = 3;
    let start_offsets = get_start_offsets();
    for start_offset in start_offsets {
        let mut segment = Segment::create(
            stream_id,
            topic_id,
            partition_id,
            start_offset,
            setup.config.clone(),
            IggyExpiry::NeverExpire,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            true,
        );
        setup
            .create_partition_directory(stream_id, topic_id, partition_id)
            .await;
        segment.persist().await.unwrap();
        assert_persisted_segment(
            &setup
                .config
                .get_partition_path(stream_id, topic_id, partition_id),
            start_offset,
        )
        .await;

        let mut loaded_segment = Segment::create(
            stream_id,
            topic_id,
            partition_id,
            start_offset,
            setup.config.clone(),
            IggyExpiry::NeverExpire,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            false,
        );
        loaded_segment.load_from_disk().await.unwrap();
        let loaded_messages = loaded_segment.get_messages_by_offset(0, 10).await.unwrap();

        assert_eq!(loaded_segment.partition_id(), segment.partition_id());
        assert_eq!(loaded_segment.start_offset(), segment.start_offset());
        assert_eq!(loaded_segment.end_offset(), segment.end_offset());
        assert_eq!(
            loaded_segment.get_messages_size(),
            segment.get_messages_size()
        );
        assert_eq!(loaded_segment.is_closed(), segment.is_closed());
        assert_eq!(
            loaded_segment.messages_file_path(),
            segment.messages_file_path()
        );
        assert_eq!(loaded_segment.index_file_path(), segment.index_file_path());
        assert!(loaded_messages.is_empty());
    }
}

#[tokio::test]
async fn should_persist_and_load_segment_with_messages() {
    let setup = TestSetup::init().await;
    let stream_id = 1;
    let topic_id = 2;
    let partition_id = 3;
    let start_offset = 0;
    let mut segment = Segment::create(
        stream_id,
        topic_id,
        partition_id,
        start_offset,
        setup.config.clone(),
        IggyExpiry::NeverExpire,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        true,
    );

    setup
        .create_partition_directory(stream_id, topic_id, partition_id)
        .await;
    segment.persist().await.unwrap();
    assert_persisted_segment(
        &setup
            .config
            .get_partition_path(stream_id, topic_id, partition_id),
        start_offset,
    )
    .await;
    let messages_count = 10;
    let mut messages = Vec::new();
    let mut messages_size = 0;
    for i in 0..messages_count {
        let message = IggyMessage::builder()
            .id(i as u128)
            .payload(Bytes::from("test"))
            .build()
            .expect("Failed to create message");
        messages_size += message.get_size_bytes().as_bytes_u32();
        messages.push(message);
    }
    let batch = IggyMessagesBatchMut::from_messages(&messages, messages_size);

    segment.append_batch(0, batch, None).await.unwrap();
    segment.persist_messages().await.unwrap();
    let mut loaded_segment = Segment::create(
        stream_id,
        topic_id,
        partition_id,
        start_offset,
        setup.config.clone(),
        IggyExpiry::NeverExpire,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        false,
    );
    loaded_segment.load_from_disk().await.unwrap();
    let messages = loaded_segment
        .get_messages_by_offset(0, messages_count)
        .await
        .unwrap();
    assert_eq!(messages.count(), messages_count);
}

#[tokio::test]
async fn given_at_least_one_not_expired_message_segment_should_not_be_expired() {
    let config = SystemConfig {
        partition: PartitionConfig {
            enforce_fsync: true,
            ..Default::default()
        },
        segment: SegmentConfig {
            size: IggyByteSize::from_str("10B").unwrap(), // small size to force expiration
            ..Default::default()
        },
        ..Default::default()
    };
    let setup = TestSetup::init_with_config(config).await;
    let stream_id = 1;
    let topic_id = 2;
    let partition_id = 3;
    let start_offset = 0;
    let message_expiry_us = 50000;
    let message_expiry = message_expiry_us.into();
    let mut segment = Segment::create(
        stream_id,
        topic_id,
        partition_id,
        start_offset,
        setup.config.clone(),
        message_expiry,
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        Arc::new(AtomicU64::new(0)),
        true,
    );

    setup
        .create_partition_directory(stream_id, topic_id, partition_id)
        .await;
    segment.persist().await.unwrap();
    assert_persisted_segment(
        &setup
            .config
            .get_partition_path(stream_id, topic_id, partition_id),
        start_offset,
    )
    .await;

    let nothing_expired_ts = IggyTimestamp::now();

    let first_message = vec![
        IggyMessage::builder()
            .payload(Bytes::from("expired"))
            .build()
            .expect("Failed to create message"),
    ];
    let first_message_size = first_message[0].get_size_bytes().as_bytes_u32();
    let first_batch = IggyMessagesBatchMut::from_messages(&first_message, first_message_size);
    segment.append_batch(0, first_batch, None).await.unwrap();

    sleep(Duration::from_micros(message_expiry_us / 2)).await;
    let first_message_expired_ts = IggyTimestamp::now();

    let second_message = vec![
        IggyMessage::builder()
            .payload(Bytes::from("not-expired"))
            .build()
            .expect("Failed to create message"),
    ];
    let second_message_size = second_message[0].get_size_bytes().as_bytes_u32();
    let second_batch = IggyMessagesBatchMut::from_messages(&second_message, second_message_size);
    segment.append_batch(1, second_batch, None).await.unwrap();

    let second_message_expired_ts =
        IggyTimestamp::now() + IggyDuration::from(message_expiry_us * 2);

    segment.persist_messages().await.unwrap();

    assert!(
        !segment.is_expired(nothing_expired_ts).await,
        "Segment should not be expired for nothing expired timestamp"
    );
    assert!(
        !segment.is_expired(first_message_expired_ts).await,
        "Segment should not be expired for first message expired timestamp"
    );
    assert!(
        segment.is_expired(second_message_expired_ts).await,
        "Segment should be expired for second message expired timestamp"
    );
}

/*
//TODO: Fixme use shard instead of system
#[tokio::test]
async fn should_delete_persisted_segments() -> Result<(), Box<dyn std::error::Error>> {
    let config = SystemConfig {
        segment: SegmentConfig {
            size: IggyByteSize::from_str("10B").unwrap(), // small size to force segment closure
            ..Default::default()
        },
        ..Default::default()
    };
    let setup = TestSetup::init_with_config(config).await;
    let mut system = System::new(
        setup.config.clone(),
        DataMaintenanceConfig::default(),
        PersonalAccessTokenConfig::default(),
    );

    // Properties
    let stream_id = Identifier::numeric(1)?;
    let stream_name = "test";
    let topic_name = "test_topic";
    let topic_id = Identifier::numeric(1)?;
    let partition_id = 1;

    setup
        .create_partition_directory(
            stream_id.get_u32_value()?,
            topic_id.get_u32_value()?,
            partition_id,
        )
        .await;

    let session = Session::new(1, 1, SocketAddr::new(Ipv4Addr::LOCALHOST.into(), 1234));
    system.init().await.unwrap();

    system
        .create_stream(&session, Some(stream_id.get_u32_value()?), stream_name)
        .await
        .unwrap();

    system
        .create_topic(
            &session,
            &stream_id,
            Some(topic_id.get_u32_value()?),
            topic_name,
            partition_id,
            IggyExpiry::default(),
            CompressionAlgorithm::default(),
            MaxTopicSize::default(),
            None,
        )
        .await?;

    let topic = system.find_topic(&session, &stream_id, &topic_id)?;
    let partitions = topic.get_partitions();
    let partition = partitions
        .first()
        .ok_or(IggyError::Error)
        .inspect_err(|_| log::error!("Cannot retrieve initial partition."))?;

    // Create multiple segments by sending messages one by one
    // Each message should be large enough to cause a segment to close
    for i in 0..5 {
        let mut partition_lock = partition.write().await;

        let payload = "payload that will force segment closure due to size";
        let message = create_message(i, payload);
        let messages = vec![message];

        let messages_size = messages
            .iter()
            .map(|m| m.get_size_bytes().as_bytes_u32())
            .sum();
        let batch = IggyMessagesBatchMut::from_messages(&messages, messages_size);

        partition_lock.append_messages(batch, None).await.unwrap();

        drop(partition_lock);

        sleep(Duration::from_millis(100)).await;
    }

    let partition_path = setup.config.get_partition_path(
        stream_id.get_u32_value()?,
        topic_id.get_u32_value()?,
        partition_id,
    );

    let initial_segments = get_segment_paths_for_partition(&partition_path);
    println!(
        "Created segments: {:?}",
        initial_segments
            .iter()
            .map(|e| e.file_name())
            .collect::<Vec<_>>()
    );

    // Since we've created 5 messages with large payloads, we should have multiple segments
    assert!(
        initial_segments.len() >= 4,
        "Expected at least 4 segments but got {}",
        initial_segments.len()
    );

    let mut initial_offsets = get_segment_offsets(&initial_segments);
    initial_offsets.sort();
    println!("Initial segment offsets (sorted): {initial_offsets:?}");

    let first_keep_count = 3usize;
    system
        .delete_segments(
            &session,
            &stream_id,
            &topic_id,
            partition_id,
            first_keep_count as u32,
        )
        .await?;

    let segments_after_first_delete = get_segment_paths_for_partition(&partition_path);
    println!(
        "Segments after first deletion (keep={}): {:?}",
        first_keep_count,
        segments_after_first_delete
            .iter()
            .map(|e| e.file_name())
            .collect::<Vec<_>>()
    );

    // We should have at most first_keep_count + 1 segments after first deletion
    // (keep_count closed segments + 1 open segment)
    assert!(
        segments_after_first_delete.len() <= first_keep_count + 1,
        "Should have at most {} segments after first deletion but got {}",
        first_keep_count + 1,
        segments_after_first_delete.len()
    );

    // Second attempt to delete segments - keeping only 1 closed segment + 1 open
    let second_keep_count = 1usize;
    system
        .delete_segments(
            &session,
            &stream_id,
            &topic_id,
            partition_id,
            second_keep_count as u32,
        )
        .await?;

    let segments_after_second_delete = get_segment_paths_for_partition(&partition_path);
    println!(
        "Segments after second deletion (keep={}): {:?}",
        second_keep_count,
        segments_after_second_delete
            .iter()
            .map(|e| e.file_name())
            .collect::<Vec<_>>()
    );

    // We should have at most second_keep_count + 1 segments after second deletion
    // (keep_count closed segments + 1 open segment)
    assert!(
        segments_after_second_delete.len() <= second_keep_count + 1,
        "Should have at most {} segments after second deletion but got {}",
        second_keep_count + 1,
        segments_after_second_delete.len()
    );

    let mut final_offsets = get_segment_offsets(&segments_after_second_delete);
    final_offsets.sort();

    let highest_initial_offsets: Vec<u64> = initial_offsets
        .iter()
        .rev()
        .take(second_keep_count + 1)
        .copied()
        .collect();

    for &offset in &final_offsets {
        assert!(
            highest_initial_offsets.contains(&offset),
            "Offset {offset} should not remain after final deletion"
        );
    }

    Ok(())
}
*/

// Helper function to extract segment offsets from DirEntry list
fn get_segment_offsets(segments: &[DirEntry]) -> Vec<u64> {
    segments
        .iter()
        .filter_map(|entry| {
            entry.file_name().to_str().and_then(|name| {
                name.split('.')
                    .next()
                    .and_then(|offset_str| offset_str.parse::<u64>().ok())
            })
        })
        .collect()
}

async fn assert_persisted_segment(partition_path: &str, start_offset: u64) {
    let segment_path = format!("{partition_path}/{start_offset:0>20}");
    let messages_file_path = format!("{segment_path}.{LOG_EXTENSION}");
    let index_path = format!("{segment_path}.{INDEX_EXTENSION}");
    assert!(fs::metadata(&messages_file_path).await.is_ok());
    assert!(fs::metadata(&index_path).await.is_ok());
}

fn get_segment_paths_for_partition(partition_path: &str) -> Vec<DirEntry> {
    std::fs::read_dir(partition_path)
        .map(|read_dir| {
            read_dir
                .filter_map(|dir_entry| {
                    dir_entry
                        .map(|dir_entry| {
                            match dir_entry
                                .path()
                                .extension()
                                .is_some_and(|ext| ext == LOG_EXTENSION)
                            {
                                true => Some(dir_entry),
                                false => None,
                            }
                        })
                        .ok()
                        .flatten()
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

fn get_start_offsets() -> Vec<u64> {
    vec![
        0, 1, 2, 9, 10, 99, 100, 110, 200, 1000, 1234, 12345, 100000, 9999999,
    ]
}
