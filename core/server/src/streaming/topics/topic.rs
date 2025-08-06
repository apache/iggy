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

use crate::configs::system::SystemConfig;
use crate::streaming::partitions::partition::Partition;
use crate::streaming::polling_consumer::PollingConsumer;
use crate::streaming::storage::SystemStorage;
use crate::streaming::topics::consumer_group::ConsumerGroup;
use ahash::AHashMap;
use core::fmt;
use iggy_common::locking::IggyRwLock;
use iggy_common::{
    CompressionAlgorithm, Consumer, ConsumerKind, IggyByteSize, IggyError, IggyExpiry,
    IggyTimestamp, MaxTopicSize, Sizeable,
};
use std::cell::{Cell, RefCell};
use std::rc::Rc;

use std::sync::Arc;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use tracing::info;

const ALMOST_FULL_THRESHOLD: f64 = 0.9;

#[derive(Debug, Clone)]
pub struct Topic {
    pub stream_id: u32,
    pub topic_id: u32,
    pub name: String,
    pub path: String,
    pub partitions_path: String,
    pub(crate) size_bytes: Arc<AtomicU64>,
    pub(crate) size_of_parent_stream: Arc<AtomicU64>,
    pub(crate) messages_count_of_parent_stream: Arc<AtomicU64>,
    pub(crate) messages_count: Arc<AtomicU64>,
    pub(crate) segments_count_of_parent_stream: Arc<AtomicU32>,
    pub(crate) config: Arc<SystemConfig>,
    pub(crate) partitions: AHashMap<u32, IggyRwLock<Partition>>,
    pub(crate) storage: Rc<SystemStorage>,
    pub(crate) consumer_groups: RefCell<AHashMap<u32, ConsumerGroup>>,
    pub(crate) consumer_groups_ids: AHashMap<String, u32>,
    pub(crate) current_consumer_group_id: Arc<AtomicU32>,
    pub(crate) current_partition_id: Arc<AtomicU32>,
    pub message_expiry: IggyExpiry,
    pub compression_algorithm: CompressionAlgorithm,
    pub max_topic_size: MaxTopicSize,
    pub replication_factor: u8,
    pub created_at: IggyTimestamp,
}

pub struct CreatedTopicInfo {
    pub topic: Topic,
    pub partition_ids: Vec<u32>,
}

impl Topic {
    #[allow(clippy::too_many_arguments)]
    pub fn empty(
        stream_id: u32,
        topic_id: u32,
        name: &str,
        size_of_parent_stream: Arc<AtomicU64>,
        messages_count_of_parent_stream: Arc<AtomicU64>,
        segments_count_of_parent_stream: Arc<AtomicU32>,
        config: Arc<SystemConfig>,
        storage: Rc<SystemStorage>,
    ) -> CreatedTopicInfo {
        Topic::create(
            stream_id,
            topic_id,
            name,
            0,
            config,
            storage,
            size_of_parent_stream,
            messages_count_of_parent_stream,
            segments_count_of_parent_stream,
            IggyExpiry::NeverExpire,
            Default::default(),
            MaxTopicSize::ServerDefault,
            1,
        )
        .unwrap()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn create(
        stream_id: u32,
        topic_id: u32,
        name: &str,
        partitions_count: u32,
        config: Arc<SystemConfig>,
        storage: Rc<SystemStorage>,
        size_of_parent_stream: Arc<AtomicU64>,
        messages_count_of_parent_stream: Arc<AtomicU64>,
        segments_count_of_parent_stream: Arc<AtomicU32>,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: u8,
    ) -> Result<CreatedTopicInfo, IggyError> {
        let path = config.get_topic_path(stream_id as usize, topic_id as usize);
        let partitions_path = config.get_partitions_path(stream_id as usize, topic_id as usize);
        let mut topic = Topic {
            stream_id,
            topic_id,
            name: name.to_string(),
            partitions: AHashMap::new(),
            path,
            partitions_path,
            storage,
            size_bytes: Arc::new(AtomicU64::new(0)),
            size_of_parent_stream,
            messages_count_of_parent_stream,
            messages_count: Arc::new(AtomicU64::new(0)),
            segments_count_of_parent_stream,
            consumer_groups: RefCell::new(AHashMap::new()),
            consumer_groups_ids: AHashMap::new(),
            current_consumer_group_id: Arc::new(AtomicU32::new(1)),
            current_partition_id: Arc::new(AtomicU32::new(1)),
            message_expiry: Topic::get_message_expiry(message_expiry, &config),
            max_topic_size: Topic::get_max_topic_size(max_topic_size, &config)?,
            compression_algorithm,
            replication_factor,
            config,
            created_at: IggyTimestamp::now(),
        };

        let partition_ids = topic.add_partitions(partitions_count)?;
        Ok(CreatedTopicInfo {
            topic,
            partition_ids,
        })
    }

    pub fn is_full(&self) -> bool {
        match self.max_topic_size {
            MaxTopicSize::Unlimited => false,
            MaxTopicSize::ServerDefault => false,
            MaxTopicSize::Custom(size) => {
                self.size_bytes.load(Ordering::SeqCst) >= size.as_bytes_u64()
            }
        }
    }

    pub fn is_almost_full(&self) -> bool {
        match self.max_topic_size {
            MaxTopicSize::Unlimited => false,
            MaxTopicSize::ServerDefault => false,
            MaxTopicSize::Custom(size) => {
                self.size_bytes.load(Ordering::SeqCst)
                    >= (size.as_bytes_u64() as f64 * ALMOST_FULL_THRESHOLD) as u64
            }
        }
    }

    pub fn is_unlimited(&self) -> bool {
        matches!(self.max_topic_size, MaxTopicSize::Unlimited)
    }

    pub fn get_partitions(&self) -> Vec<IggyRwLock<Partition>> {
        self.partitions.values().cloned().collect()
    }

    pub fn get_partition(&self, partition_id: u32) -> Result<IggyRwLock<Partition>, IggyError> {
        match self.partitions.get(&partition_id) {
            Some(partition_arc) => Ok(partition_arc.clone()),
            None => Err(IggyError::PartitionNotFound(
                partition_id,
                self.topic_id,
                self.stream_id,
            )),
        }
    }

    pub fn resolve_consumer_with_partition_id(
        &self,
        consumer: &Consumer,
        client_id: u32,
        partition_id: Option<u32>,
        calculate_partition_id: bool,
    ) -> Result<Option<(PollingConsumer, u32)>, IggyError> {
        match consumer.kind {
            ConsumerKind::Consumer => {
                let partition_id = partition_id.unwrap_or(1);
                Ok(Some((
                    PollingConsumer::consumer(&consumer.id, partition_id),
                    partition_id,
                )))
            }
            ConsumerKind::ConsumerGroup => {
                let mut consumer_group = self.get_consumer_group_mut(&consumer.id)?;
                if let Some(partition_id) = partition_id {
                    return Ok(Some((
                        PollingConsumer::consumer_group(consumer_group.group_id, client_id),
                        partition_id,
                    )));
                }

                let partition_id = if calculate_partition_id {
                    consumer_group.calculate_partition_id(client_id)?
                } else {
                    consumer_group.get_current_partition_id(client_id)?
                };
                let Some(partition_id) = partition_id else {
                    return Ok(None);
                };

                Ok(Some((
                    PollingConsumer::consumer_group(consumer_group.group_id, client_id),
                    partition_id,
                )))
            }
        }
    }

    pub fn get_max_topic_size(
        max_topic_size: MaxTopicSize,
        config: &SystemConfig,
    ) -> Result<MaxTopicSize, IggyError> {
        match max_topic_size {
            MaxTopicSize::ServerDefault => Ok(config.topic.max_size),
            _ => {
                if max_topic_size.as_bytes_u64() < config.segment.size.as_bytes_u64() {
                    Err(IggyError::InvalidTopicSize(
                        max_topic_size,
                        config.segment.size,
                    ))
                } else {
                    Ok(max_topic_size)
                }
            }
        }
    }

    pub fn get_message_expiry(message_expiry: IggyExpiry, config: &SystemConfig) -> IggyExpiry {
        match message_expiry {
            IggyExpiry::ServerDefault => config.segment.message_expiry,
            _ => message_expiry,
        }
    }
}

impl Sizeable for Topic {
    fn get_size_bytes(&self) -> IggyByteSize {
        IggyByteSize::from(self.size_bytes.load(Ordering::SeqCst))
    }
}

impl fmt::Display for Topic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Topic {{ id: {}, stream ID: {}, name: {}, path: {}, partitions: {}, message_expiry: {}, max_topic_size: {}, replication_factor: {} }}",
            self.topic_id,
            self.stream_id,
            self.name,
            self.path,
            self.partitions.len(),
            self.message_expiry,
            self.max_topic_size,
            self.replication_factor,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::streaming::{
        persistence::persister::{FileWithSyncPersister, PersisterKind},
        utils::MemoryPool,
    };
    use iggy_common::locking::IggyRwLockFn;
    use std::str::FromStr;

    #[tokio::test]
    async fn should_be_created_given_valid_parameters() {
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
        let topic_id = 2;
        let name = "test";
        let partitions_count = 3;
        let message_expiry = IggyExpiry::NeverExpire;
        let compression_algorithm = CompressionAlgorithm::None;
        let max_topic_size = MaxTopicSize::Custom(IggyByteSize::from_str("2 GiB").unwrap());
        let replication_factor = 1;
        let path = config.get_topic_path(stream_id, topic_id);
        let size_of_parent_stream = Arc::new(AtomicU64::new(0));
        let messages_count_of_parent_stream = Arc::new(AtomicU64::new(0));
        let segments_count_of_parent_stream = Arc::new(AtomicU32::new(0));

        let topic_info = Topic::create(
            stream_id as u32,
            topic_id as u32,
            name,
            partitions_count,
            config,
            storage,
            messages_count_of_parent_stream,
            size_of_parent_stream,
            segments_count_of_parent_stream,
            message_expiry,
            compression_algorithm,
            max_topic_size,
            replication_factor,
        )
        .unwrap();
        let topic = topic_info.topic;
        assert_eq!(topic.stream_id, stream_id as u32);
        assert_eq!(topic.topic_id, topic_id as u32);
        assert_eq!(topic.path, path);
        assert_eq!(topic.name, name);
        assert_eq!(topic.partitions.len(), partitions_count as usize);
        assert_eq!(topic.message_expiry, message_expiry);

        for (id, partition) in topic.partitions {
            let partition = partition.read().await;
            assert_eq!(partition.stream_id, stream_id as u32);
            assert_eq!(partition.topic_id, topic.topic_id);
            assert_eq!(partition.partition_id, id);
            assert_eq!(partition.segments.len(), 1);
        }
    }
}
