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

use super::COMPONENT;
use crate::shard::IggyShard;
use crate::shard::namespace::IggyNamespace;
use crate::streaming::session::Session;
use crate::streaming::streams::storage::{create_stream_file_hierarchy, delete_stream_directory};
use err_trail::ErrContext;
use iggy_common::{Identifier, IggyError};
use std::sync::Arc;

/// Info returned when a stream is deleted - contains what callers need for logging/events.
pub struct DeletedStreamInfo {
    pub id: usize,
    pub name: String,
}

impl IggyShard {
    pub async fn create_stream(&self, session: &Session, name: String) -> Result<usize, IggyError> {
        self.permissioner.create_stream(session.get_user_id())?;

        let created_at = iggy_common::IggyTimestamp::now();
        let (stream_id, _stats) = self
            .metadata
            .try_register_stream(Arc::from(name.as_str()), created_at)?;

        self.metrics.increment_streams(1);

        create_stream_file_hierarchy(stream_id, &self.config.system).await?;
        Ok(stream_id)
    }

    pub fn update_stream(
        &self,
        session: &Session,
        stream_id: &Identifier,
        name: String,
    ) -> Result<(), IggyError> {
        let stream = self.resolve_stream_id(stream_id)?;

        self.permissioner
            .update_stream(session.get_user_id(), stream)
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - failed to update stream, user ID: {}, stream ID: {}",
                    session.get_user_id(),
                    stream_id
                )
            })?;
        self.update_stream_base(stream, name)
    }

    fn update_stream_base(&self, stream_id: usize, name: String) -> Result<(), IggyError> {
        self.metadata
            .try_update_stream(stream_id, Arc::from(name.as_str()))
    }

    pub fn delete_stream_bypass_auth(
        &self,
        id: &Identifier,
    ) -> Result<DeletedStreamInfo, IggyError> {
        let stream = self.resolve_stream_id(id)?;
        Ok(self.delete_stream_base(stream))
    }

    fn delete_stream_base(&self, stream_id: usize) -> DeletedStreamInfo {
        let metadata = self.metadata.load();
        let stream_meta = metadata
            .streams
            .get(stream_id)
            .expect("Stream metadata must exist");
        let stream_name = stream_meta.name.to_string();
        let stats = stream_meta.stats.clone();

        let topics_count = stream_meta.topics.len();
        let partitions_count: usize = stream_meta
            .topics
            .iter()
            .map(|(_, topic)| topic.partitions.len())
            .sum();

        {
            let mut store = self.partition_store.borrow_mut();
            for (topic_id, topic) in stream_meta.topics.iter() {
                for (partition_id, _) in topic.partitions.iter() {
                    let ns = IggyNamespace::new(stream_id, topic_id, partition_id);
                    store.remove(&ns);
                }
            }
        }
        drop(metadata);

        self.metrics.decrement_streams(1);
        self.metrics.decrement_topics(topics_count as u32);
        self.metrics.decrement_partitions(partitions_count as u32);
        self.metrics
            .decrement_messages(stats.messages_count_inconsistent());
        self.metrics
            .decrement_segments(stats.segments_count_inconsistent());

        self.metadata.delete_stream(stream_id);

        DeletedStreamInfo {
            id: stream_id,
            name: stream_name,
        }
    }

    pub async fn delete_stream(
        &self,
        session: &Session,
        id: &Identifier,
    ) -> Result<DeletedStreamInfo, IggyError> {
        let stream = self.resolve_stream_id(id)?;

        self.permissioner
            .delete_stream(session.get_user_id(), stream)
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - permission denied to delete stream for user {}, stream ID: {}",
                    session.get_user_id(),
                    stream,
                )
            })?;

        let topics_with_partitions: Vec<(usize, Vec<usize>)> = {
            let metadata = self.metadata.load();
            metadata
                .streams
                .get(stream)
                .map(|stream_meta| {
                    stream_meta
                        .topics
                        .iter()
                        .map(|(topic_id, topic)| {
                            let partition_ids: Vec<usize> = topic.partitions.keys().collect();
                            (topic_id, partition_ids)
                        })
                        .collect()
                })
                .unwrap_or_default()
        };

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

    pub async fn purge_stream(
        &self,
        session: &Session,
        stream_id: &Identifier,
    ) -> Result<(), IggyError> {
        let stream = self.resolve_stream_id(stream_id)?;

        self.permissioner
            .purge_stream(session.get_user_id(), stream)
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - permission denied to purge stream for user {}, stream ID: {}",
                    session.get_user_id(),
                    stream,
                )
            })?;

        self.purge_stream_base(stream).await
    }

    pub async fn purge_stream_bypass_auth(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        let stream = self.resolve_stream_id(stream_id)?;
        self.purge_stream_base(stream).await
    }

    async fn purge_stream_base(&self, stream_id: usize) -> Result<(), IggyError> {
        let metadata = self.metadata.load();
        let topic_ids: Vec<usize> = metadata
            .streams
            .get(stream_id)
            .map(|stream| stream.topics.keys().collect())
            .unwrap_or_default();
        drop(metadata);

        for topic_id in topic_ids {
            self.purge_topic_base(stream_id, topic_id).await?;
        }

        Ok(())
    }

    pub fn get_stream_from_shared_metadata(&self, stream_id: usize) -> bytes::Bytes {
        use crate::shard::namespace::IggyNamespace;
        use bytes::{BufMut, BytesMut};

        let metadata = self.metadata.load();

        let Some(stream_meta) = metadata.streams.get(stream_id) else {
            return bytes::Bytes::new();
        };

        let mut topic_ids: Vec<_> = stream_meta.topics.keys().collect();
        topic_ids.sort_unstable();

        let (total_size, total_messages) = {
            let mut size = 0u64;
            let mut messages = 0u64;
            for &topic_id in &topic_ids {
                if let Some(topic) = stream_meta.topics.get(topic_id) {
                    for (partition_id, _) in topic.partitions.iter() {
                        let ns = IggyNamespace::new(stream_id, topic_id, partition_id);
                        if let Some(stats) = self.metadata.get_partition_stats(&ns) {
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
                let mut partition_ids: Vec<_> = topic_meta.partitions.keys().collect();
                partition_ids.sort_unstable();

                let (topic_size, topic_messages) = {
                    let mut size = 0u64;
                    let mut messages = 0u64;
                    for &partition_id in &partition_ids {
                        let ns = IggyNamespace::new(stream_id, topic_id, partition_id);
                        if let Some(stats) = self.metadata.get_partition_stats(&ns) {
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
    }

    pub fn get_streams_from_shared_metadata(&self) -> bytes::Bytes {
        use crate::shard::namespace::IggyNamespace;
        use bytes::{BufMut, BytesMut};

        let metadata = self.metadata.load();
        let mut bytes = BytesMut::new();

        let mut stream_ids: Vec<_> = metadata.streams.keys().collect();
        stream_ids.sort_unstable();

        for stream_id in stream_ids {
            let Some(stream_meta) = metadata.streams.get(stream_id) else {
                continue;
            };

            let mut topic_ids: Vec<_> = stream_meta.topics.keys().collect();
            topic_ids.sort_unstable();

            let (total_size, total_messages) = {
                let mut size = 0u64;
                let mut messages = 0u64;
                for &topic_id in &topic_ids {
                    if let Some(topic) = stream_meta.topics.get(topic_id) {
                        for (partition_id, _) in topic.partitions.iter() {
                            let ns = IggyNamespace::new(stream_id, topic_id, partition_id);
                            if let Some(stats) = self.metadata.get_partition_stats(&ns) {
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
    }
}
