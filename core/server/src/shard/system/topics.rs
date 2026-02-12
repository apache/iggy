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
use crate::shard::transmission::message::{ResolvedStream, ResolvedTopic};
use crate::streaming::topics::storage::{create_topic_file_hierarchy, delete_topic_directory};
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
        stream: ResolvedStream,
        name: String,
        message_expiry: IggyExpiry,
        compression: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: Option<u8>,
    ) -> Result<usize, IggyError> {
        let stream_id = stream.0;

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
        topic: ResolvedTopic,
        name: String,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: Option<u8>,
    ) -> Result<(), IggyError> {
        self.writer().try_update_topic(
            &self.metadata,
            topic.stream_id,
            topic.topic_id,
            Arc::from(name.as_str()),
            message_expiry,
            compression_algorithm,
            max_topic_size,
            replication_factor.unwrap_or(1),
        )
    }

    pub async fn delete_topic(&self, topic: ResolvedTopic) -> Result<DeletedTopicInfo, IggyError> {
        let stream = topic.stream_id;
        let topic_id = topic.topic_id;

        let (partition_ids, topic_name, messages_count, size_bytes, segments_count, parent_stats) =
            self.metadata.with_metadata(|m| {
                let stream_meta = m.streams.get(stream).expect("Stream metadata must exist");
                let topic_meta = stream_meta
                    .topics
                    .get(topic_id)
                    .expect("Topic metadata must exist");
                let pids: Vec<usize> = (0..topic_meta.partitions.len()).collect();
                (
                    pids,
                    topic_meta.name.to_string(),
                    topic_meta.stats.messages_count_inconsistent(),
                    topic_meta.stats.size_bytes_inconsistent(),
                    topic_meta.stats.segments_count_inconsistent(),
                    topic_meta.stats.parent().clone(),
                )
            });

        {
            let mut partitions = self.local_partitions.borrow_mut();
            for &partition_id in &partition_ids {
                let ns = IggyNamespace::new(stream, topic_id, partition_id);
                partitions.remove(&ns);
            }
        }

        self.writer().delete_topic(stream, topic_id);

        let topic_info = DeletedTopicInfo {
            id: topic_id,
            name: topic_name,
            stream_id: stream,
        };

        self.client_manager
            .delete_consumer_groups_for_topic(stream, topic_id);

        let namespaces_to_remove: Vec<_> = self
            .shards_table
            .iter()
            .filter_map(|entry| {
                let (ns, _) = entry.pair();
                if ns.stream_id() == stream && ns.topic_id() == topic_id {
                    Some(*ns)
                } else {
                    None
                }
            })
            .collect();

        for ns in namespaces_to_remove {
            self.remove_shard_table_record(&ns);
        }

        delete_topic_directory(stream, topic_id, &partition_ids, &self.config.system).await?;

        parent_stats.decrement_messages_count(messages_count);
        parent_stats.decrement_size_bytes(size_bytes);
        parent_stats.decrement_segments_count(segments_count);
        self.metrics.decrement_topics(1);
        Ok(topic_info)
    }

    /// Clears in-memory state for a topic: consumer offsets and stats.
    /// Called on the control plane before broadcasting to other shards.
    pub async fn purge_topic(&self, topic: ResolvedTopic) -> Result<(), IggyError> {
        let stream = topic.stream_id;
        let topic_id = topic.topic_id;
        let partition_ids = self.metadata.get_partition_ids(stream, topic_id);

        for &partition_id in &partition_ids {
            if let Some(offsets) =
                self.metadata
                    .get_partition_consumer_offsets(stream, topic_id, partition_id)
            {
                offsets.pin().clear();
            }
            if let Some(offsets) =
                self.metadata
                    .get_partition_consumer_group_offsets(stream, topic_id, partition_id)
            {
                offsets.pin().clear();
            }
        }

        if let Some(topic_stats) = self.metadata.get_topic_stats(stream, topic_id) {
            topic_stats.zero_out_all();
        }

        for &partition_id in &partition_ids {
            let ns = IggyNamespace::new(stream, topic_id, partition_id);
            if let Some(partition_stats) = self.metadata.get_partition_stats(&ns) {
                partition_stats.zero_out_all();
            }
        }

        Ok(())
    }

    /// Disk cleanup for local partitions: deletes consumer offset files and purges segments.
    /// Called on each shard (including shard 0) after in-memory state is cleared.
    pub(crate) async fn purge_topic_local(&self, topic: ResolvedTopic) -> Result<(), IggyError> {
        let stream = topic.stream_id;
        let topic_id = topic.topic_id;
        let partition_ids = self.metadata.get_partition_ids(stream, topic_id);

        for &partition_id in &partition_ids {
            let ns = IggyNamespace::new(stream, topic_id, partition_id);
            if !self.local_partitions.borrow().contains(&ns) {
                continue;
            }

            self.delete_all_consumer_offset_files(stream, topic_id, partition_id)
                .await?;
            self.purge_all_segments(stream, topic_id, partition_id)
                .await?;
        }

        Ok(())
    }
}
