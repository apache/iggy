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

use crate::metadata::TopicMeta;
use crate::shard::IggyShard;
use crate::streaming::topics::storage::{create_topic_file_hierarchy, delete_topic_directory};
use bytes::{BufMut, BytesMut};
use iggy_common::sharding::IggyNamespace;
use iggy_common::{
    CompressionAlgorithm, Identifier, IggyError, IggyExpiry, IggyTimestamp, MaxTopicSize,
};
use std::sync::Arc;

/// Info returned when a topic is deleted - contains what callers need for logging/events.
pub struct DeletedTopicInfo {
    pub id: usize,
    pub name: String,
    pub stream_id: usize,
}

impl IggyShard {
    #[allow(clippy::too_many_arguments)]
    pub async fn create_topic(
        &self,
        stream_id: &Identifier,
        name: String,
        message_expiry: IggyExpiry,
        compression: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: Option<u8>,
    ) -> Result<usize, IggyError> {
        let stream_id = self.resolve_stream_id(stream_id)?;

        let config = &self.config.system;
        let message_expiry = config.resolve_message_expiry(message_expiry);
        let max_topic_size = config.resolve_max_topic_size(max_topic_size)?;

        let name_arc = Arc::from(name.as_str());
        let parent_stats = self.metadata.get_stream_stats(stream_id).ok_or_else(|| {
            IggyError::StreamIdNotFound(Identifier::numeric(stream_id as u32).unwrap())
        })?;

        let name_exists = self.metadata.with_metadata(|m| {
            m.streams
                .get(stream_id)
                .map(|s| s.topic_index.contains_key(&name_arc))
                .unwrap_or(false)
        });
        if name_exists {
            return Err(IggyError::TopicNameAlreadyExists(
                name,
                Identifier::numeric(stream_id as u32).unwrap(),
            ));
        }

        let topic_id = self.metadata.next_topic_id(stream_id).ok_or_else(|| {
            IggyError::StreamIdNotFound(Identifier::numeric(stream_id as u32).unwrap())
        })?;
        create_topic_file_hierarchy(stream_id, topic_id, &self.config.system).await?;

        let created_at = IggyTimestamp::now();
        let stats = Arc::new(crate::streaming::stats::TopicStats::new(parent_stats));
        let topic_meta = TopicMeta {
            id: 0,
            name: name_arc,
            created_at,
            message_expiry,
            compression_algorithm: compression,
            max_topic_size,
            replication_factor: replication_factor.unwrap_or(1),
            stats,
            partitions: Vec::new(),
            consumer_groups: slab::Slab::new(),
            consumer_group_index: ahash::AHashMap::default(),
            round_robin_counter: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        };
        let assigned_id = self
            .writer()
            .add_topic(stream_id, topic_meta)
            .ok_or_else(|| {
                IggyError::StreamIdNotFound(Identifier::numeric(stream_id as u32).unwrap())
            })?;
        debug_assert_eq!(
            assigned_id, topic_id,
            "Topic ID mismatch: expected {topic_id}, got {assigned_id}"
        );

        self.metrics.increment_topics(1);
        Ok(topic_id)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn update_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: String,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: Option<u8>,
    ) -> Result<(), IggyError> {
        let (stream, topic) = self.resolve_topic_id(stream_id, topic_id)?;

        self.update_topic_base(
            stream,
            topic,
            name,
            message_expiry,
            compression_algorithm,
            max_topic_size,
            replication_factor.unwrap_or(1),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn update_topic_base(
        &self,
        stream: usize,
        topic: usize,
        name: String,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: u8,
    ) -> Result<(), IggyError> {
        self.writer().try_update_topic(
            &self.metadata,
            stream,
            topic,
            Arc::from(name.as_str()),
            message_expiry,
            compression_algorithm,
            max_topic_size,
            replication_factor,
        )
    }

    pub async fn delete_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<DeletedTopicInfo, IggyError> {
        let (stream, topic) = self.resolve_topic_id(stream_id, topic_id)?;

        let (partition_ids, messages_count, size_bytes, segments_count, parent_stats) =
            self.metadata.with_metadata(|m| {
                let stream_meta = m.streams.get(stream).expect("Stream metadata must exist");
                let topic_meta = stream_meta
                    .topics
                    .get(topic)
                    .expect("Topic metadata must exist");
                let pids: Vec<usize> = (0..topic_meta.partitions.len()).collect();
                (
                    pids,
                    topic_meta.stats.messages_count_inconsistent(),
                    topic_meta.stats.size_bytes_inconsistent(),
                    topic_meta.stats.segments_count_inconsistent(),
                    topic_meta.stats.parent().clone(),
                )
            });

        let topic_info = self.delete_topic_base(stream, topic);

        self.client_manager
            .delete_consumer_groups_for_topic(stream, topic_info.id);

        let namespaces_to_remove: Vec<_> = self
            .shards_table
            .iter()
            .filter_map(|entry| {
                let (ns, _) = entry.pair();
                if ns.stream_id() == stream && ns.topic_id() == topic_info.id {
                    Some(*ns)
                } else {
                    None
                }
            })
            .collect();

        for ns in namespaces_to_remove {
            self.remove_shard_table_record(&ns);
        }

        delete_topic_directory(stream, topic_info.id, &partition_ids, &self.config.system).await?;

        parent_stats.decrement_messages_count(messages_count);
        parent_stats.decrement_size_bytes(size_bytes);
        parent_stats.decrement_segments_count(segments_count);
        self.metrics.decrement_topics(1);
        Ok(topic_info)
    }

    fn delete_topic_base(&self, stream: usize, topic: usize) -> DeletedTopicInfo {
        let (topic_name, partition_ids) = self.metadata.with_metadata(|m| {
            let stream_meta = m.streams.get(stream).expect("Stream metadata must exist");
            let topic_meta = stream_meta
                .topics
                .get(topic)
                .expect("Topic metadata must exist");
            let name = topic_meta.name.to_string();
            let pids: Vec<usize> = (0..topic_meta.partitions.len()).collect();
            (name, pids)
        });

        {
            let mut partitions = self.local_partitions.borrow_mut();
            for partition_id in partition_ids {
                let ns = IggyNamespace::new(stream, topic, partition_id);
                partitions.remove(&ns);
            }
        }

        self.writer().delete_topic(stream, topic);

        DeletedTopicInfo {
            id: topic,
            name: topic_name,
            stream_id: stream,
        }
    }

    pub async fn purge_topic(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        let (stream, topic) = self.resolve_topic_id(stream_id, topic_id)?;

        let partition_ids = self.metadata.get_partition_ids(stream, topic);

        let mut all_consumer_paths = Vec::new();
        let mut all_group_paths = Vec::new();

        for partition_id in &partition_ids {
            let ns = IggyNamespace::new(stream, topic, *partition_id);
            if let Some(partition) = self.local_partitions.borrow().get(&ns) {
                all_consumer_paths.extend(
                    partition
                        .consumer_offsets
                        .pin()
                        .iter()
                        .map(|item| item.1.path.clone()),
                );
                all_group_paths.extend(
                    partition
                        .consumer_group_offsets
                        .pin()
                        .iter()
                        .map(|item| item.1.path.clone()),
                );
            }
        }

        for path in all_consumer_paths {
            self.delete_consumer_offset_from_disk(&path).await?;
        }
        for path in all_group_paths {
            self.delete_consumer_offset_from_disk(&path).await?;
        }

        self.purge_topic_base(stream, topic).await
    }

    pub async fn purge_topic_bypass_auth(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        let (stream, topic) = self.resolve_topic_id(stream_id, topic_id)?;
        self.purge_topic_base(stream, topic).await
    }

    pub(crate) async fn purge_topic_base(
        &self,
        stream: usize,
        topic: usize,
    ) -> Result<(), IggyError> {
        let partition_ids = self.metadata.get_partition_ids(stream, topic);

        for &partition_id in &partition_ids {
            let ns = IggyNamespace::new(stream, topic, partition_id);

            let has_partition = self.local_partitions.borrow().contains(&ns);
            if has_partition {
                self.delete_segments_base(stream, topic, partition_id, u32::MAX)
                    .await?;
            }
        }

        if let Some(topic_stats) = self.metadata.get_topic_stats(stream, topic) {
            topic_stats.zero_out_all();
        }

        for &partition_id in &partition_ids {
            let ns = IggyNamespace::new(stream, topic, partition_id);
            if let Some(partition_stats) = self.metadata.get_partition_stats(&ns) {
                partition_stats.zero_out_all();
            }
        }

        Ok(())
    }

    pub fn get_topic_from_metadata(&self, stream_id: usize, topic_id: usize) -> bytes::Bytes {
        self.metadata.with_metadata(|metadata| {
            let Some(stream_meta) = metadata.streams.get(stream_id) else {
                return bytes::Bytes::new();
            };
            let Some(topic_meta) = stream_meta.topics.get(topic_id) else {
                return bytes::Bytes::new();
            };

            let mut partition_ids: Vec<_> = topic_meta
                .partitions
                .iter()
                .enumerate()
                .map(|(k, _)| k)
                .collect();
            partition_ids.sort_unstable();

            let (total_size, total_messages) = {
                let mut size = 0u64;
                let mut messages = 0u64;
                for &partition_id in &partition_ids {
                    if let Some(stats) = metadata
                        .streams
                        .get(stream_id)
                        .and_then(|s| s.topics.get(topic_id))
                        .and_then(|t| t.partitions.get(partition_id))
                        .map(|p| p.stats.clone())
                    {
                        size += stats.size_bytes_inconsistent();
                        messages += stats.messages_count_inconsistent();
                    }
                }
                (size, messages)
            };

            let mut bytes = BytesMut::new();

            bytes.put_u32_le(topic_meta.id as u32);
            bytes.put_u64_le(topic_meta.created_at.into());
            bytes.put_u32_le(partition_ids.len() as u32);
            bytes.put_u64_le(topic_meta.message_expiry.into());
            bytes.put_u8(topic_meta.compression_algorithm.as_code());
            bytes.put_u64_le(topic_meta.max_topic_size.into());
            bytes.put_u8(topic_meta.replication_factor);
            bytes.put_u64_le(total_size);
            bytes.put_u64_le(total_messages);
            bytes.put_u8(topic_meta.name.len() as u8);
            bytes.put_slice(topic_meta.name.as_bytes());

            for &partition_id in &partition_ids {
                let partition_meta = topic_meta.partitions.get(partition_id);
                let created_at = partition_meta
                    .map(|m| m.created_at)
                    .unwrap_or_else(IggyTimestamp::now);

                let (segments_count, size_bytes, messages_count, offset) = partition_meta
                    .map(|p| {
                        (
                            p.stats.segments_count_inconsistent(),
                            p.stats.size_bytes_inconsistent(),
                            p.stats.messages_count_inconsistent(),
                            p.stats.current_offset(),
                        )
                    })
                    .unwrap_or((0, 0, 0, 0));

                bytes.put_u32_le(partition_id as u32);
                bytes.put_u64_le(created_at.into());
                bytes.put_u32_le(segments_count);
                bytes.put_u64_le(offset);
                bytes.put_u64_le(size_bytes);
                bytes.put_u64_le(messages_count);
            }

            bytes.freeze()
        })
    }

    pub fn get_topics_from_metadata(&self, stream_id: usize) -> bytes::Bytes {
        self.metadata.with_metadata(|metadata| {
            let mut bytes = BytesMut::new();

            let Some(stream_meta) = metadata.streams.get(stream_id) else {
                return bytes.freeze();
            };

            let mut topic_ids: Vec<_> = stream_meta.topics.iter().map(|(k, _)| k).collect();
            topic_ids.sort_unstable();

            for topic_id in topic_ids {
                let Some(topic_meta) = stream_meta.topics.get(topic_id) else {
                    continue;
                };

                let mut partition_ids: Vec<_> = topic_meta
                    .partitions
                    .iter()
                    .enumerate()
                    .map(|(k, _)| k)
                    .collect();
                partition_ids.sort_unstable();

                let (total_size, total_messages) = {
                    let mut size = 0u64;
                    let mut messages = 0u64;
                    for &partition_id in &partition_ids {
                        if let Some(stats) = topic_meta
                            .partitions
                            .get(partition_id)
                            .map(|p| p.stats.clone())
                        {
                            size += stats.size_bytes_inconsistent();
                            messages += stats.messages_count_inconsistent();
                        }
                    }
                    (size, messages)
                };

                bytes.put_u32_le(topic_meta.id as u32);
                bytes.put_u64_le(topic_meta.created_at.into());
                bytes.put_u32_le(partition_ids.len() as u32);
                bytes.put_u64_le(topic_meta.message_expiry.into());
                bytes.put_u8(topic_meta.compression_algorithm.as_code());
                bytes.put_u64_le(topic_meta.max_topic_size.into());
                bytes.put_u8(topic_meta.replication_factor);
                bytes.put_u64_le(total_size);
                bytes.put_u64_le(total_messages);
                bytes.put_u8(topic_meta.name.len() as u8);
                bytes.put_slice(topic_meta.name.as_bytes());
            }

            bytes.freeze()
        })
    }
}
