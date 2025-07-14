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
use crate::streaming::create_messages;
use iggy::prelude::{IggyExpiry, IggyTimestamp, Sizeable};
use server::state::system::PartitionState;
use server::streaming::partitions::partition::Partition;
use server::streaming::segments::*;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64};
use compio::fs;

#[compio::test]
async fn should_persist_partition_with_segment() {
    let setup = TestSetup::init().await;
    let with_segment = true;
    let stream_id = 1;
    let topic_id = 2;
    setup.create_partitions_directory(stream_id, topic_id).await;
    let partition_ids = get_partition_ids();
    for partition_id in partition_ids {
        let mut partition = Partition::create(
            stream_id,
            topic_id,
            partition_id,
            with_segment,
            setup.config.clone(),
            setup.storage.clone(),
            IggyExpiry::NeverExpire,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU32::new(0)),
            IggyTimestamp::now(),
        );

        partition.persist().await.unwrap();

        assert_persisted_partition(&partition.partition_path, with_segment).await;
    }
}

#[compio::test]
async fn should_load_existing_partition_from_disk() {
    let setup = TestSetup::init().await;
    let with_segment = true;
    let stream_id = 1;
    let topic_id = 2;
    setup.create_partitions_directory(stream_id, topic_id).await;
    let partition_ids = get_partition_ids();
    for partition_id in partition_ids {
        let mut partition = Partition::create(
            stream_id,
            topic_id,
            partition_id,
            with_segment,
            setup.config.clone(),
            setup.storage.clone(),
            IggyExpiry::NeverExpire,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU32::new(0)),
            IggyTimestamp::now(),
        );
        partition.persist().await.unwrap();
        assert_persisted_partition(&partition.partition_path, with_segment).await;

        let now = IggyTimestamp::now();
        let mut loaded_partition = Partition::create(
            stream_id,
            topic_id,
            partition.partition_id,
            false,
            setup.config.clone(),
            setup.storage.clone(),
            IggyExpiry::NeverExpire,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU32::new(0)),
            now,
        );
        let partition_state = PartitionState {
            id: partition.partition_id,
            created_at: now,
        };
        loaded_partition.load(partition_state).await.unwrap();

        assert_eq!(loaded_partition.stream_id, partition.stream_id);
        assert_eq!(loaded_partition.partition_id, partition.partition_id);
        assert_eq!(loaded_partition.partition_path, partition.partition_path);
        assert_eq!(loaded_partition.current_offset, partition.current_offset);
        assert_eq!(
            loaded_partition.unsaved_messages_count,
            partition.unsaved_messages_count
        );
        assert_eq!(
            loaded_partition.get_segments().len(),
            partition.get_segments().len()
        );
        assert_eq!(
            loaded_partition.should_increment_offset,
            partition.should_increment_offset
        );
    }
}

#[compio::test]
async fn should_delete_existing_partition_from_disk() {
    let setup = TestSetup::init().await;
    let with_segment = true;
    let stream_id = 1;
    let topic_id = 2;
    setup.create_partitions_directory(stream_id, topic_id).await;
    let partition_ids = get_partition_ids();
    for partition_id in partition_ids {
        let mut partition = Partition::create(
            stream_id,
            topic_id,
            partition_id,
            with_segment,
            setup.config.clone(),
            setup.storage.clone(),
            IggyExpiry::NeverExpire,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU32::new(0)),
            IggyTimestamp::now(),
        );
        partition.persist().await.unwrap();
        assert_persisted_partition(&partition.partition_path, with_segment).await;

        partition.delete().await.unwrap();

        assert!(fs::metadata(&partition.partition_path).await.is_err());
    }
}

#[compio::test]
async fn should_purge_existing_partition_on_disk() {
    let setup = TestSetup::init().await;
    let with_segment = true;
    let stream_id = 1;
    let topic_id = 2;
    setup.create_partitions_directory(stream_id, topic_id).await;
    let partition_ids = get_partition_ids();
    for partition_id in partition_ids {
        let mut partition = Partition::create(
            stream_id,
            topic_id,
            partition_id,
            with_segment,
            setup.config.clone(),
            setup.storage.clone(),
            IggyExpiry::NeverExpire,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU32::new(0)),
            IggyTimestamp::now(),
        );
        partition.persist().await.unwrap();
        assert_persisted_partition(&partition.partition_path, with_segment).await;
        let messages = create_messages();
        let messages_count = messages.len() as u32;
        let messages_size: u32 = messages
            .iter()
            .map(|msg| msg.get_size_bytes().as_bytes_u32())
            .sum();
        let batch = IggyMessagesBatchMut::from_messages(&messages, messages_size);
        partition.append_messages(batch).await.unwrap();
        let loaded_messages = partition.get_messages_by_offset(0, 100).await.unwrap();
        assert_eq!(loaded_messages.count(), messages_count);
        partition.purge().await.unwrap();
        assert_eq!(partition.current_offset, 0);
        assert_eq!(partition.unsaved_messages_count, 0);
        assert!(!partition.should_increment_offset);
        let loaded_messages = partition.get_messages_by_offset(0, 100).await.unwrap();
        assert!(loaded_messages.is_empty());
    }
}

async fn assert_persisted_partition(partition_path: &str, with_segment: bool) {
    assert!(fs::metadata(&partition_path).await.is_ok());

    if with_segment {
        let start_offset = 0u64;
        let segment_path = format!("{partition_path}/{start_offset:0>20}");
        let messages_file_path = format!("{segment_path}.{LOG_EXTENSION}");
        let index_path = format!("{segment_path}.{INDEX_EXTENSION}");
        assert!(fs::metadata(&messages_file_path).await.is_ok());
        assert!(fs::metadata(&index_path).await.is_ok());
    }
}

fn get_partition_ids() -> Vec<u32> {
    vec![1, 2, 3, 5, 10, 100, 1000, 99999]
}
