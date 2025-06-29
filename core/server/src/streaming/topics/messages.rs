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

use crate::binary::handlers::messages::poll_messages_handler::IggyPollMetadata;
use crate::streaming::polling_consumer::PollingConsumer;
use crate::streaming::segments::{IggyMessagesBatchMut, IggyMessagesBatchSet};
use crate::streaming::topics::COMPONENT;
use crate::streaming::topics::topic::Topic;
use crate::streaming::utils::hash;
use ahash::AHashMap;
use error_set::ErrContext;
use iggy_common::locking::IggySharedMutFn;
use iggy_common::{IggyError, IggyExpiry, Partitioning, PartitioningKind, PollingKind};
use iggy_common::{IggyTimestamp, PollingStrategy};
use std::sync::atomic::Ordering;
use tracing::trace;

impl Topic {
    pub fn get_messages_count(&self) -> u64 {
        self.messages_count.load(Ordering::SeqCst)
    }

    pub async fn get_messages(
        &self,
        consumer: PollingConsumer,
        partition_id: u32,
        strategy: PollingStrategy,
        count: u32,
    ) -> Result<(IggyPollMetadata, IggyMessagesBatchSet), IggyError> {
        if !self.has_partitions() {
            return Err(IggyError::NoPartitions(self.topic_id, self.stream_id));
        }

        let partition = self.partitions.get(&partition_id);
        if partition.is_none() {
            return Err(IggyError::PartitionNotFound(
                partition_id,
                self.topic_id,
                self.stream_id,
            ));
        }

        let partition = partition.unwrap();
        let partition = partition.read().await;
        let value = strategy.value;
        let messages = match strategy.kind {
            PollingKind::Offset => partition.get_messages_by_offset(value, count).await,
            PollingKind::Timestamp => {
                partition
                    .get_messages_by_timestamp(value.into(), count)
                    .await
                    .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to get messages by timestamp: {value}, count: {count}"))
                                }
            PollingKind::First => partition.get_first_messages(count).await,
            PollingKind::Last => partition.get_last_messages(count).await,
            PollingKind::Next => partition.get_next_messages(consumer, count).await,
        }?;

        let metadata = IggyPollMetadata::new(partition_id, partition.current_offset);

        Ok((metadata, messages))
    }

    pub async fn append_messages(
        &self,
        partition_id: u32,
        batch: IggyMessagesBatchMut,
    ) -> Result<(), IggyError> {
        if !self.has_partitions() {
            return Err(IggyError::NoPartitions(self.topic_id, self.stream_id));
        }

        // Don't return an error if the topic is full and delete_oldest_segments is true.
        // Oldest segment will be removed eventually by MaintainMessages background job.
        if self.is_full() && self.config.topic.delete_oldest_segments {
            return Err(IggyError::TopicFull(self.topic_id, self.stream_id));
        }

        if batch.is_empty() {
            return Ok(());
        }

        self.append_messages_to_partition(batch, partition_id).await
    }

    pub async fn flush_unsaved_buffer(
        &self,
        partition_id: u32,
        fsync: bool,
    ) -> Result<(), IggyError> {
        let partition = self.partitions.get(&partition_id);
        partition
            .ok_or(IggyError::PartitionNotFound(
                partition_id,
                self.stream_id,
                self.stream_id,
            ))?
            .write()
            .await
            .flush_unsaved_buffer(fsync)
            .await
    }

    async fn append_messages_to_partition(
        &self,
        messages: IggyMessagesBatchMut,
        partition_id: u32,
    ) -> Result<(), IggyError> {
        let partition = self.partitions.get(&partition_id);
        partition
            .ok_or(IggyError::PartitionNotFound(
                partition_id,
                self.topic_id,
                self.stream_id,
            ))?
            .write()
            .await
            .append_messages(messages)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to append messages")
            })?;

        Ok(())
    }

    pub fn calculate_partition_id(&self, partitioning: &Partitioning) -> Result<u32, IggyError> {
        match partitioning.kind {
            PartitioningKind::Balanced => Ok(self.get_next_partition_id()),
            PartitioningKind::PartitionId => Ok(u32::from_le_bytes(
                partitioning.value[..partitioning.length as usize]
                    .try_into()
                    .map_err(|_| IggyError::InvalidNumberEncoding)?,
            )),
            PartitioningKind::MessagesKey => {
                Ok(self.calculate_partition_id_by_messages_key_hash(&partitioning.value))
            }
        }
    }

    fn get_next_partition_id(&self) -> u32 {
        let mut partition_id = self.current_partition_id.fetch_add(1, Ordering::SeqCst);
        let partitions_count = self.partitions.len() as u32;
        if partition_id > partitions_count {
            partition_id = 1;
            self.current_partition_id
                .swap(partition_id + 1, Ordering::SeqCst);
        }
        trace!("Next partition ID: {}", partition_id);
        partition_id
    }

    fn calculate_partition_id_by_messages_key_hash(&self, messages_key: &[u8]) -> u32 {
        let messages_key_hash = hash::calculate_32(messages_key);
        let partitions_count = self.get_partitions_count();
        let mut partition_id = messages_key_hash % partitions_count;
        if partition_id == 0 {
            partition_id = partitions_count;
        }
        trace!(
            "Calculated partition ID: {} for messages key: {:?}, hash: {}",
            partition_id, messages_key, messages_key_hash
        );
        partition_id
    }

    pub async fn get_expired_segments_start_offsets_per_partition(
        &self,
        now: IggyTimestamp,
    ) -> AHashMap<u32, Vec<u64>> {
        let mut expired_segments = AHashMap::new();
        if let IggyExpiry::ExpireDuration(_) = self.message_expiry {
            for (_, partition) in self.partitions.iter() {
                let partition = partition.read().await;
                let segments = partition.get_expired_segments_start_offsets(now).await;
                if !segments.is_empty() {
                    expired_segments.insert(partition.partition_id, segments);
                }
            }
        }
        expired_segments
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::system::SystemConfig;
    use crate::streaming::persistence::persister::FileWithSyncPersister;
    use crate::streaming::persistence::persister::PersisterKind;
    use crate::streaming::storage::SystemStorage;
    use crate::streaming::utils::MemoryPool;
    use bytes::Bytes;
    use iggy_common::CompressionAlgorithm;
    use iggy_common::{IggyMessage, MaxTopicSize};
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU32;
    use std::sync::atomic::AtomicU64;

    #[tokio::test]
    async fn given_partition_id_key_messages_should_be_appended_only_to_the_chosen_partition() {
        let partition_id = 1;
        let partitioning = Partitioning::partition_id(partition_id);
        let partitions_count = 3;
        let messages_count: u32 = 1000;
        let topic = init_topic(partitions_count).await;

        for entity_id in 1..=messages_count {
            let message = IggyMessage::builder()
                .id(entity_id as u128)
                .payload(Bytes::from(entity_id.to_string()))
                .build()
                .expect("Failed to create message with valid payload and headers");
            let messages = IggyMessagesBatchMut::from_messages(&[message], 1);
            let partition = topic.calculate_partition_id(&partitioning).unwrap();
            topic.append_messages(partition, messages).await.unwrap();
        }

        let partitions = topic.get_partitions();
        assert_eq!(partitions.len(), partitions_count as usize);
        for partition in partitions {
            let partition = partition.read().await;
            let partition_msgs_count = partition.get_messages_count();
            if partition.partition_id == partition_id {
                assert_eq!(partition_msgs_count, messages_count as u64);
            } else {
                assert_eq!(partition_msgs_count, 0);
            }
        }
    }

    #[tokio::test]
    async fn given_messages_key_key_messages_should_be_appended_to_the_calculated_partitions() {
        let partitions_count = 3;
        let messages_count = 1000;
        let topic = init_topic(partitions_count).await;

        for entity_id in 1..=messages_count {
            let partitioning = Partitioning::messages_key_u32(entity_id);
            let message = IggyMessage::builder()
                .id(entity_id as u128)
                .payload(Bytes::from("test message"))
                .build()
                .expect("Failed to create message with valid payload and headers");
            let messages = IggyMessagesBatchMut::from_messages(&[message], 1);
            let partition_id = topic
                .calculate_partition_id(&partitioning)
                .expect("Failed to calculate partition ID");
            topic.append_messages(partition_id, messages).await.unwrap();
        }

        let mut read_messages_count = 0;
        let partitions = topic.get_partitions();
        assert_eq!(partitions.len(), partitions_count as usize);
        for partition in partitions {
            let partition = partition.read().await;
            let partition_msgs_cnt = partition.get_messages_count();
            read_messages_count += partition_msgs_cnt;
            assert!(partition_msgs_cnt < messages_count as u64);
        }

        assert_eq!(read_messages_count, messages_count as u64);
    }

    #[tokio::test]
    async fn given_multiple_partitions_calculate_next_partition_id_should_return_next_partition_id_using_round_robin()
     {
        let partitions_count = 3;
        let messages_count = 1000;
        let topic = init_topic(partitions_count).await;

        let mut expected_partition_id = 0;
        for _ in 1..=messages_count {
            let partition_id = topic.get_next_partition_id();
            expected_partition_id += 1;
            if expected_partition_id > partitions_count {
                expected_partition_id = 1;
            }

            assert_eq!(partition_id, expected_partition_id);
        }
    }

    #[tokio::test]
    async fn given_multiple_partitions_calculate_partition_id_by_hash_should_return_next_partition_id()
     {
        let partitions_count = 3;
        let messages_count = 1000;
        let topic = init_topic(partitions_count).await;

        for entity_id in 1..=messages_count {
            let key = Partitioning::messages_key_u32(entity_id);
            let partition_id = topic.calculate_partition_id_by_messages_key_hash(&key.value);
            let entity_id_hash = hash::calculate_32(&key.value);
            let mut expected_partition_id = entity_id_hash % partitions_count;
            if expected_partition_id == 0 {
                expected_partition_id = partitions_count;
            }

            assert_eq!(partition_id, expected_partition_id);
        }
    }

    async fn init_topic(partitions_count: u32) -> Topic {
        let tempdir = tempfile::TempDir::new().unwrap();
        let config = Arc::new(SystemConfig {
            path: tempdir.path().to_str().unwrap().to_string(),
            ..Default::default()
        });
        let storage = Rc::new(SystemStorage::new(
            config.clone(),
            Arc::new(PersisterKind::FileWithSync(FileWithSyncPersister {})),
        ));
        MemoryPool::init_pool(config.clone());
        let stream_id = 1;
        let id = 2;
        let name = "test";
        let compression_algorithm = CompressionAlgorithm::None;
        let size_of_parent_stream = Arc::new(AtomicU64::new(0));
        let messages_count_of_parent_stream = Arc::new(AtomicU64::new(0));
        let segments_count_of_parent_stream = Arc::new(AtomicU32::new(0));

        let created_info = Topic::create(
            stream_id,
            id,
            name,
            partitions_count,
            config,
            storage,
            size_of_parent_stream,
            messages_count_of_parent_stream,
            segments_count_of_parent_stream,
            IggyExpiry::NeverExpire,
            compression_algorithm,
            MaxTopicSize::ServerDefault,
            1,
        )
        .unwrap();
        let topic = created_info.topic;
        topic.persist().await.unwrap();
        topic
    }
}
