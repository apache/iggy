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

use crate::streaming::batching::appendable_batch_info::AppendableBatchInfo;
use crate::streaming::models::messages::RetainedMessage;
use crate::streaming::polling_consumer::PollingConsumer;
use crate::streaming::topics::topic::Topic;
use crate::streaming::topics::COMPONENT;
use crate::streaming::utils::file::folder_size;
use crate::streaming::utils::hash;
use ahash::AHashMap;
use error_set::ErrContext;
use iggy::confirmation::Confirmation;
use iggy::error::IggyError;
use iggy::locking::IggySharedMutFn;
use iggy::messages::poll_messages::{PollingKind, PollingStrategy};
use iggy::messages::send_messages::{Message, Partitioning, PartitioningKind};
use iggy::models::messages::PolledMessages;
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::expiry::IggyExpiry;
use iggy::utils::sizeable::Sizeable;
use iggy::utils::timestamp::IggyTimestamp;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use tracing::{info, trace, warn};

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
    ) -> Result<PolledMessages, IggyError> {
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

        let messages = messages
            .into_iter()
            .map(|msg| msg.to_polled_message())
            .collect::<Result<Vec<_>, IggyError>>()?;
        Ok(PolledMessages {
            partition_id,
            current_offset: partition.current_offset,
            messages,
        })
    }

    pub async fn append_messages(
        &self,
        batch_size: IggyByteSize,
        partitioning: Partitioning,
        messages: Vec<Message>,
        confirmation: Option<Confirmation>,
    ) -> Result<(), IggyError> {
        if !self.has_partitions() {
            return Err(IggyError::NoPartitions(self.topic_id, self.stream_id));
        }

        // Don't return an error if the topic is full and delete_oldest_segments is true.
        // Oldest segment will be removed eventually by MaintainMessages background job.
        if self.is_full() && self.config.topic.delete_oldest_segments {
            return Err(IggyError::TopicFull(self.topic_id, self.stream_id));
        }

        if messages.is_empty() {
            return Ok(());
        }

        let partition_id = match partitioning.kind {
            PartitioningKind::Balanced => self.get_next_partition_id(),
            PartitioningKind::PartitionId => u32::from_le_bytes(
                partitioning.value[..partitioning.length as usize]
                    .try_into()
                    .map_err(|_| IggyError::InvalidNumberEncoding)?,
            ),
            PartitioningKind::MessagesKey => {
                self.calculate_partition_id_by_messages_key_hash(&partitioning.value)
            }
        };

        let appendable_batch_info = AppendableBatchInfo::new(batch_size, partition_id);
        self.append_messages_to_partition(appendable_batch_info, messages, confirmation)
            .await
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
        appendable_batch_info: AppendableBatchInfo,
        messages: Vec<Message>,
        confirmation: Option<Confirmation>,
    ) -> Result<(), IggyError> {
        let partition = self.partitions.get(&appendable_batch_info.partition_id);
        partition
            .ok_or({
                IggyError::PartitionNotFound(
                    appendable_batch_info.partition_id,
                    self.stream_id,
                    self.stream_id,
                )
            })?
            .write()
            .await
            .append_messages(appendable_batch_info, messages, confirmation)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to append messages")
            })?;

        Ok(())
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
            partition_id,
            messages_key,
            messages_key_hash
        );
        partition_id
    }

    pub(crate) async fn load_messages_from_disk_to_cache(&mut self) -> Result<(), IggyError> {
        if !self.config.cache.enabled {
            return Ok(());
        }
        let path = self.config.get_system_path();

        // TODO: load data from database instead of calculating the size on disk
        let total_size_on_disk_bytes = folder_size(&path)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get folder size, path: {path}")
            })
            .map_err(|_| IggyError::InvalidSizeBytes)?;

        for partition_lock in self.partitions.values_mut() {
            let mut partition = partition_lock.write().await;

            let end_offset = match partition.segments.last() {
                Some(segment) => segment.current_offset,
                None => {
                    warn!(
                        "No segments found for partition ID: {}, topic ID: {}, stream ID: {}",
                        partition.partition_id, partition.topic_id, partition.stream_id
                    );
                    continue;
                }
            };

            trace!(
               "Loading messages to cache for partition ID: {}, topic ID: {}, stream ID: {}, offset: 0 to {}...",
               partition.partition_id,
               partition.topic_id,
               partition.stream_id,
               end_offset
           );

            let partition_size_bytes = partition.get_size_bytes();
            let cache_limit_bytes = self.config.cache.size.clone().into();

            // Fetch data from disk proportional to the partition size
            // eg. 12 partitions, each has 300 MB, cache limit is 500 MB, so there is total 3600 MB of data on SSD.
            // 500 MB * (300 / 3600 MB) ~= 41.6 MB to load from cache (assuming all partitions have the same size on disk)
            let size_to_fetch_from_disk = (cache_limit_bytes.as_bytes_u64() as f64
                * (partition_size_bytes.as_bytes_u64() as f64
                    / total_size_on_disk_bytes.as_bytes_u64() as f64))
                as u64;
            let messages = partition
                .get_newest_messages_by_size(size_to_fetch_from_disk as u64)
                .await
                .with_error_context(|error| format!("{COMPONENT} (error: {error}) - failed to get newest messages by size: {size_to_fetch_from_disk}"))?;

            let sum = messages
                .iter()
                .map(|m| m.get_size_bytes())
                .sum::<IggyByteSize>();
            if !Self::cache_integrity_check(&messages) {
                warn!(
                   "Cache integrity check failed for partition ID: {}, topic ID: {}, stream ID: {}, offset: 0 to {}. Emptying cache...",
                   partition.partition_id, partition.topic_id, partition.stream_id, end_offset
               );
            } else if let Some(cache) = &mut partition.cache {
                for message in &messages {
                    cache.push_safe(message.clone());
                }

                info!(
                   "Loaded {} messages ({} bytes) to cache for partition ID: {}, topic ID: {}, stream ID: {}, offset: 0 to {}.",
                   messages.len(), sum, partition.partition_id, partition.topic_id, partition.stream_id, end_offset
               );
            } else {
                warn!(
                    "Cache is invalid for ID: {}, topic ID: {}, stream ID: {}, offset: 0 to {}",
                    partition.partition_id, partition.topic_id, partition.stream_id, end_offset
                );
            }
        }

        Ok(())
    }

    fn cache_integrity_check(cache: &[Arc<RetainedMessage>]) -> bool {
        if cache.is_empty() {
            warn!("Cache is empty!");
            return false;
        }

        let first_offset = cache[0].offset;
        let last_offset = cache[cache.len() - 1].offset;

        for i in 1..cache.len() {
            if cache[i].offset != cache[i - 1].offset + 1 {
                warn!("Offsets are not subsequent at index {} offset {}, for previous index {} offset is {}", i, cache[i].offset, i-1, cache[i-1].offset);
                return false;
            }
        }

        let expected_messages_count: u64 = last_offset - first_offset + 1;
        if cache.len() != expected_messages_count as usize {
            warn!(
                "Messages count is in cache ({}) not equal to expected messages count ({})",
                cache.len(),
                expected_messages_count
            );
            return false;
        }

        true
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
    use bytes::Bytes;
    use iggy::compression::compression_algorithm::CompressionAlgorithm;
    use iggy::utils::topic_size::MaxTopicSize;
    use std::sync::atomic::AtomicU32;
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;

    #[tokio::test]
    async fn given_partition_id_key_messages_should_be_appended_only_to_the_chosen_partition() {
        let partition_id = 1;
        let partitioning = Partitioning::partition_id(partition_id);
        let partitions_count = 3;
        let messages_count: u32 = 1000;
        let topic = init_topic(partitions_count).await;

        for entity_id in 1..=messages_count {
            let messages = vec![Message::new(Some(entity_id as u128), Bytes::new(), None)];
            let batch_size = messages
                .iter()
                .map(|msg| msg.get_size_bytes())
                .sum::<IggyByteSize>();
            topic
                .append_messages(batch_size, partitioning.clone(), messages, None)
                .await
                .unwrap();
        }

        let partitions = topic.get_partitions();
        assert_eq!(partitions.len(), partitions_count as usize);
        for partition in partitions {
            let partition = partition.read().await;
            let messages = partition.cache.as_ref().unwrap().to_vec();
            if partition.partition_id == partition_id {
                assert_eq!(messages.len() as u32, messages_count);
            } else {
                assert_eq!(messages.len() as u32, 0);
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
            let messages = vec![Message::new(Some(entity_id as u128), Bytes::new(), None)];
            let batch_size = messages
                .iter()
                .map(|msg| msg.get_size_bytes())
                .sum::<IggyByteSize>();
            topic
                .append_messages(batch_size, partitioning, messages, None)
                .await
                .unwrap();
        }

        let mut read_messages_count = 0;
        let partitions = topic.get_partitions();
        assert_eq!(partitions.len(), partitions_count as usize);
        for partition in partitions {
            let partition = partition.read().await;
            let messages = partition.cache.as_ref().unwrap().to_vec();
            read_messages_count += messages.len();
            assert!(messages.len() < messages_count as usize);
        }

        assert_eq!(read_messages_count, messages_count as usize);
    }

    #[tokio::test]
    async fn given_multiple_partitions_calculate_next_partition_id_should_return_next_partition_id_using_round_robin(
    ) {
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
    async fn given_multiple_partitions_calculate_partition_id_by_hash_should_return_next_partition_id(
    ) {
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
        let storage = Arc::new(SystemStorage::new(
            config.clone(),
            Arc::new(PersisterKind::FileWithSync(FileWithSyncPersister {})),
        ));
        let stream_id = 1;
        let id = 2;
        let name = "test";
        let compression_algorithm = CompressionAlgorithm::None;
        let size_of_parent_stream = Arc::new(AtomicU64::new(0));
        let messages_count_of_parent_stream = Arc::new(AtomicU64::new(0));
        let segments_count_of_parent_stream = Arc::new(AtomicU32::new(0));

        let topic = Topic::create(
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
        .await
        .unwrap();
        topic.persist().await.unwrap();
        topic
    }
}
