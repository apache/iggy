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

use crate::metadata::StreamMeta;
use crate::shard::IggyShard;
use crate::streaming::streams::storage::{create_stream_file_hierarchy, delete_stream_directory};
use bytes::{BufMut, BytesMut};
use iggy_common::sharding::IggyNamespace;
use iggy_common::{Identifier, IggyError, IggyTimestamp};
use std::sync::Arc;
/// Info returned when a stream is deleted - contains what callers need for logging/events.
pub struct DeletedStreamInfo {
    pub id: usize,
    pub name: String,
}

impl IggyShard {
    pub async fn create_stream(&self, name: String) -> Result<usize, IggyError> {
        let name_arc = Arc::from(name.as_str());
        if self.metadata.stream_name_exists(&name_arc) {
            return Err(IggyError::StreamNameAlreadyExists(name));
        }

        let stream_id = self.metadata.next_stream_id();
        create_stream_file_hierarchy(stream_id, &self.config.system).await?;

        let created_at = IggyTimestamp::now();
        let stats = Arc::new(crate::streaming::stats::StreamStats::default());
        let meta = StreamMeta::with_stats(0, name_arc, created_at, stats);
        let assigned_id = self.writer().add_stream(meta);
        debug_assert_eq!(
            assigned_id, stream_id,
            "Stream ID mismatch: expected {stream_id}, got {assigned_id}"
        );

        self.metrics.increment_streams(1);
        Ok(stream_id)
    }

    pub fn update_stream(&self, stream_id: &Identifier, name: String) -> Result<(), IggyError> {
        let stream_id = self.resolve_stream_id(stream_id)?;
        self.writer()
            .try_update_stream(&self.metadata, stream_id, Arc::from(name.as_str()))
    }

    fn delete_stream_base(&self, stream_id: usize) -> DeletedStreamInfo {
        let (stream_name, stats, topics_count, partitions_count, namespaces) =
            self.metadata.with_metadata(|m| {
                let stream_meta = m
                    .streams
                    .get(stream_id)
                    .expect("Stream metadata must exist");
                let namespaces: Vec<_> = stream_meta
                    .topics
                    .iter()
                    .flat_map(|(topic_id, topic)| {
                        topic
                            .partitions
                            .iter()
                            .enumerate()
                            .map(move |(partition_id, _)| {
                                IggyNamespace::new(stream_id, topic_id, partition_id)
                            })
                    })
                    .collect();
                let partitions_count: usize = stream_meta
                    .topics
                    .iter()
                    .map(|(_, t)| t.partitions.len())
                    .sum();
                (
                    stream_meta.name.to_string(),
                    stream_meta.stats.clone(),
                    stream_meta.topics.len(),
                    partitions_count,
                    namespaces,
                )
            });

        {
            let mut partitions = self.local_partitions.borrow_mut();
            for ns in namespaces {
                partitions.remove(&ns);
            }
        }

        self.metrics.decrement_streams(1);
        self.metrics.decrement_topics(topics_count as u32);
        self.metrics.decrement_partitions(partitions_count as u32);
        self.metrics
            .decrement_messages(stats.messages_count_inconsistent());
        self.metrics
            .decrement_segments(stats.segments_count_inconsistent());

        self.writer().delete_stream(stream_id);

        DeletedStreamInfo {
            id: stream_id,
            name: stream_name,
        }
    }

    pub async fn delete_stream(&self, id: &Identifier) -> Result<DeletedStreamInfo, IggyError> {
        let stream = self.resolve_stream_id(id)?;

        let topics_with_partitions = self.metadata.with_metadata(|m| {
            m.streams
                .get(stream)
                .map(|stream_meta| {
                    stream_meta
                        .topics
                        .iter()
                        .map(|(topic_id, topic)| {
                            let partition_ids: Vec<usize> = (0..topic.partitions.len()).collect();
                            (topic_id, partition_ids)
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default()
        });

        let stream_info = self.delete_stream_base(stream);

        self.client_manager
            .delete_consumer_groups_for_stream(stream);

        let namespaces_to_remove: Vec<_> = self
            .shards_table
            .iter()
            .filter_map(|entry| {
                let (ns, _) = entry.pair();
                if ns.stream_id() == stream {
                    Some(*ns)
                } else {
                    None
                }
            })
            .collect();

        for ns in namespaces_to_remove {
            self.remove_shard_table_record(&ns);
        }

        delete_stream_directory(stream, &topics_with_partitions, &self.config.system).await?;
        Ok(stream_info)
    }

    pub async fn purge_stream(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        let stream = self.resolve_stream_id(stream_id)?;
        self.purge_stream_base(stream).await
    }

    pub async fn purge_stream_bypass_auth(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        let stream = self.resolve_stream_id(stream_id)?;
        self.purge_stream_base(stream).await
    }

    async fn purge_stream_base(&self, stream_id: usize) -> Result<(), IggyError> {
        let topic_ids = self.metadata.get_topic_ids(stream_id);

        for topic_id in topic_ids {
            self.purge_topic_base(stream_id, topic_id).await?;
        }

        Ok(())
    }

    pub fn get_stream_from_metadata(&self, stream_id: usize) -> bytes::Bytes {
        self.metadata.with_metadata(|metadata| {
            let Some(stream_meta) = metadata.streams.get(stream_id) else {
                return bytes::Bytes::new();
            };

            let mut topic_ids: Vec<_> = stream_meta.topics.iter().map(|(k, _)| k).collect();
            topic_ids.sort_unstable();

            let (total_size, total_messages) = {
                let mut size = 0u64;
                let mut messages = 0u64;
                for &topic_id in &topic_ids {
                    if let Some(topic) = stream_meta.topics.get(topic_id) {
                        for partition_id in 0..topic.partitions.len() {
                            let ns = IggyNamespace::new(stream_id, topic_id, partition_id);
                            if let Some(stats) = metadata
                                .streams
                                .get(ns.stream_id())
                                .and_then(|s| s.topics.get(ns.topic_id()))
                                .and_then(|t| t.partitions.get(ns.partition_id()))
                                .map(|p| p.stats.clone())
                            {
                                size += stats.size_bytes_inconsistent();
                                messages += stats.messages_count_inconsistent();
                            }
                        }
                    }
                }
                (size, messages)
            };

            let mut bytes = BytesMut::new();

            bytes.put_u32_le(stream_meta.id as u32);
            bytes.put_u64_le(stream_meta.created_at.into());
            bytes.put_u32_le(topic_ids.len() as u32);
            bytes.put_u64_le(total_size);
            bytes.put_u64_le(total_messages);
            bytes.put_u8(stream_meta.name.len() as u8);
            bytes.put_slice(stream_meta.name.as_bytes());

            for &topic_id in &topic_ids {
                if let Some(topic_meta) = stream_meta.topics.get(topic_id) {
                    let partition_ids: Vec<_> = (0..topic_meta.partitions.len()).collect();

                    let (topic_size, topic_messages) = {
                        let mut size = 0u64;
                        let mut messages = 0u64;
                        for &partition_id in &partition_ids {
                            let ns = IggyNamespace::new(stream_id, topic_id, partition_id);
                            if let Some(stats) = metadata
                                .streams
                                .get(ns.stream_id())
                                .and_then(|s| s.topics.get(ns.topic_id()))
                                .and_then(|t| t.partitions.get(ns.partition_id()))
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
                    bytes.put_u64_le(topic_size);
                    bytes.put_u64_le(topic_messages);
                    bytes.put_u8(topic_meta.name.len() as u8);
                    bytes.put_slice(topic_meta.name.as_bytes());
                }
            }

            bytes.freeze()
        })
    }

    pub fn get_streams_from_metadata(&self) -> bytes::Bytes {
        self.metadata.with_metadata(|metadata| {
            let mut bytes = BytesMut::new();

            let mut stream_ids: Vec<_> = metadata.streams.iter().map(|(k, _)| k).collect();
            stream_ids.sort_unstable();

            for stream_id in stream_ids {
                let Some(stream_meta) = metadata.streams.get(stream_id) else {
                    continue;
                };

                let mut topic_ids: Vec<_> = stream_meta.topics.iter().map(|(k, _)| k).collect();
                topic_ids.sort_unstable();

                let (total_size, total_messages) = {
                    let mut size = 0u64;
                    let mut messages = 0u64;
                    for &topic_id in &topic_ids {
                        if let Some(topic) = stream_meta.topics.get(topic_id) {
                            for partition_id in 0..topic.partitions.len() {
                                let ns = IggyNamespace::new(stream_id, topic_id, partition_id);
                                if let Some(stats) = metadata
                                    .streams
                                    .get(ns.stream_id())
                                    .and_then(|s| s.topics.get(ns.topic_id()))
                                    .and_then(|t| t.partitions.get(ns.partition_id()))
                                    .map(|p| p.stats.clone())
                                {
                                    size += stats.size_bytes_inconsistent();
                                    messages += stats.messages_count_inconsistent();
                                }
                            }
                        }
                    }
                    (size, messages)
                };

                bytes.put_u32_le(stream_meta.id as u32);
                bytes.put_u64_le(stream_meta.created_at.into());
                bytes.put_u32_le(topic_ids.len() as u32);
                bytes.put_u64_le(total_size);
                bytes.put_u64_le(total_messages);
                bytes.put_u8(stream_meta.name.len() as u8);
                bytes.put_slice(stream_meta.name.as_bytes());
            }

            bytes.freeze()
        })
    }
}
