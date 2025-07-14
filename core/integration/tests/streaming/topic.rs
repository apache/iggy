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
use ahash::AHashMap;
use iggy::prelude::*;
use server::state::system::{PartitionState, TopicState};
use server::streaming::polling_consumer::PollingConsumer;
use server::streaming::segments::IggyMessagesBatchMut;
use server::streaming::topics::topic::Topic;
use std::default::Default;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64};
use compio::fs;

#[compio::test]
async fn should_persist_topics_with_partitions_directories_and_info_file() {
    let setup = TestSetup::init().await;
    let stream_id = 1;
    let partitions_count = 3;
    setup.create_topics_directory(stream_id).await;
    let topic_ids = get_topic_ids();
    for topic_id in topic_ids {
        let name = format!("test-{topic_id}");
        let created_topic_info = Topic::create(
            stream_id,
            topic_id,
            &name,
            partitions_count,
            setup.config.clone(),
            setup.storage.clone(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU32::new(0)),
            IggyExpiry::NeverExpire,
            CompressionAlgorithm::default(),
            MaxTopicSize::ServerDefault,
            1,
        )
        .unwrap();
        let topic = created_topic_info.topic;

        topic.persist().await.unwrap();

        assert_persisted_topic(
            &topic.path,
            &setup.config.get_partitions_path(stream_id, topic_id),
            3,
        )
        .await;
    }
}

#[compio::test]
async fn should_load_existing_topic_from_disk() {
    let setup = TestSetup::init().await;
    let stream_id = 1;
    setup.create_topics_directory(stream_id).await;
    let partitions_count = 3;
    let topic_ids = get_topic_ids();
    for topic_id in topic_ids {
        let name = format!("test-{topic_id}");
        let created_topic_info = Topic::create(
            stream_id,
            topic_id,
            &name,
            partitions_count,
            setup.config.clone(),
            setup.storage.clone(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU32::new(0)),
            IggyExpiry::NeverExpire,
            CompressionAlgorithm::default(),
            MaxTopicSize::ServerDefault,
            1,
        )
        .unwrap();
        let topic = created_topic_info.topic;

        topic.persist().await.unwrap();
        assert_persisted_topic(
            &topic.path,
            &setup.config.get_partitions_path(stream_id, topic_id),
            partitions_count,
        )
        .await;

        let created_at = IggyTimestamp::now();
        let created_topic_info = Topic::empty(
            stream_id,
            topic_id,
            &name,
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU32::new(0)),
            setup.config.clone(),
            setup.storage.clone(),
        );
        let mut loaded_topic = created_topic_info.topic;
        let topic_state = TopicState {
            id: topic_id,
            name,
            partitions: if partitions_count == 0 {
                AHashMap::new()
            } else {
                (1..=partitions_count)
                    .map(|id| (id, PartitionState { id, created_at }))
                    .collect()
            },
            consumer_groups: Default::default(),
            compression_algorithm: Default::default(),
            message_expiry: IggyExpiry::NeverExpire,
            max_topic_size: MaxTopicSize::ServerDefault,
            replication_factor: Some(1),
            created_at: Default::default(),
        };
        loaded_topic.load(topic_state).await.unwrap();

        assert_eq!(loaded_topic.stream_id, topic.stream_id);
        assert_eq!(loaded_topic.topic_id, topic.topic_id);
        assert_eq!(loaded_topic.name, topic.name);
        assert_eq!(
            loaded_topic.compression_algorithm,
            topic.compression_algorithm
        );
        assert_eq!(loaded_topic.path, topic.path);
        assert_eq!(loaded_topic.get_partitions().len() as u32, partitions_count);
    }
}

#[compio::test]
async fn should_delete_existing_topic_from_disk() {
    let setup = TestSetup::init().await;
    let stream_id = 1;
    setup.create_topics_directory(stream_id).await;
    let partitions_count = 3;
    let topic_ids = get_topic_ids();
    for topic_id in topic_ids {
        let name = format!("test-{topic_id}");
        let created_topic_info = Topic::create(
            stream_id,
            topic_id,
            &name,
            partitions_count,
            setup.config.clone(),
            setup.storage.clone(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU32::new(0)),
            IggyExpiry::NeverExpire,
            CompressionAlgorithm::default(),
            MaxTopicSize::ServerDefault,
            1,
        )
        .unwrap();
        let topic = created_topic_info.topic;
        topic.persist().await.unwrap();
        assert_persisted_topic(
            &topic.path,
            &setup.config.get_partitions_path(stream_id, topic_id),
            partitions_count,
        )
        .await;

        topic.delete().await.unwrap();

        assert!(fs::metadata(&topic.path).await.is_err());
    }
}

#[compio::test]
async fn should_purge_existing_topic_on_disk() {
    let setup = TestSetup::init().await;
    let stream_id = 1;
    setup.create_topics_directory(stream_id).await;
    let partitions_count = 3;
    let topic_ids = get_topic_ids();
    for topic_id in topic_ids {
        let name = format!("test-{topic_id}");
        let created_topic_info = Topic::create(
            stream_id,
            topic_id,
            &name,
            partitions_count,
            setup.config.clone(),
            setup.storage.clone(),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU64::new(0)),
            Arc::new(AtomicU32::new(0)),
            IggyExpiry::NeverExpire,
            CompressionAlgorithm::default(),
            MaxTopicSize::ServerDefault,
            1,
        )
        .unwrap();
        let topic = created_topic_info.topic;
        topic.persist().await.unwrap();
        assert_persisted_topic(
            &topic.path,
            &setup.config.get_partitions_path(stream_id, topic_id),
            partitions_count,
        )
        .await;

        let messages = create_messages();
        let messages_count = messages.len() as u32;
        let batch_size = messages
            .iter()
            .map(|msg| msg.get_size_bytes().as_bytes_u32())
            .sum::<u32>();
        let batch = IggyMessagesBatchMut::from_messages(&messages, batch_size);
        let partition_id = topic
            .calculate_partition_id(&Partitioning::partition_id(1))
            .unwrap();
        topic.append_messages(partition_id, batch).await.unwrap();
        let (_, loaded_messages) = topic
            .get_messages(
                PollingConsumer::Consumer(1, 1),
                1,
                PollingStrategy::offset(0),
                100,
            )
            .await
            .unwrap();
        assert_eq!(loaded_messages.count(), messages_count);

        topic.purge().await.unwrap();
        let (metadata, loaded_messages) = topic
            .get_messages(
                PollingConsumer::Consumer(1, 1),
                1,
                PollingStrategy::offset(0),
                100,
            )
            .await
            .unwrap();
        assert_eq!(metadata.current_offset, 0);
        assert!(loaded_messages.is_empty());
    }
}

async fn assert_persisted_topic(topic_path: &str, partitions_path: &str, partitions_count: u32) {
    let topic_metadata = fs::metadata(topic_path).await.unwrap();
    assert!(topic_metadata.is_dir());
    for partition_id in 1..=partitions_count {
        let partition_path = format!("{partitions_path}/{partition_id}");
        let partition_metadata = fs::metadata(&partition_path).await.unwrap();
        assert!(partition_metadata.is_dir());
    }
}

fn get_topic_ids() -> Vec<u32> {
    vec![1, 2, 3, 5, 10, 100, 1000, 99999]
}
