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
use crate::metadata::PartitionMeta;
use crate::shard::IggyShard;
use crate::shard::calculate_shard_assignment;
use crate::shard::namespace::IggyNamespace;
use crate::shard::shard_local_partitions::PartitionData;
use crate::shard::transmission::event::PartitionInfo;
use crate::streaming::partitions::partition::{ConsumerGroupOffsets, ConsumerOffsets};
use crate::streaming::partitions::storage::create_partition_file_hierarchy;
use crate::streaming::partitions::storage::delete_partitions_from_disk;
use crate::streaming::segments::Segment;
use crate::streaming::segments::storage::create_segment_storage;
use crate::streaming::session::Session;
use crate::streaming::stats::PartitionStats;
use err_trail::ErrContext;
use iggy_common::Identifier;
use iggy_common::IggyError;
use iggy_common::IggyTimestamp;
use iggy_common::sharding::{LocalIdx, PartitionLocation, ShardId};
use std::sync::Arc;
use tracing::info;

impl IggyShard {
    fn validate_partition_permissions(
        &self,
        session: &Session,
        stream_id: usize,
        topic_id: usize,
        operation: &str,
    ) -> Result<(), IggyError> {
        let result = match operation {
            "create" => {
                self.permissioner
                    .create_partitions(session.get_user_id(), stream_id, topic_id)
            }
            "delete" => {
                self.permissioner
                    .delete_partitions(session.get_user_id(), stream_id, topic_id)
            }
            _ => return Err(IggyError::InvalidCommand),
        };

        result.error(|e: &IggyError| {
            format!(
                "{COMPONENT} (error: {e}) - permission denied to {operation} partitions for user {} on stream ID: {}, topic ID: {}",
                session.get_user_id(),
                stream_id,
                topic_id
            )
        })
    }

    pub async fn create_partitions(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<Vec<PartitionInfo>, IggyError> {
        let (stream, topic) = self.resolve_topic_id(stream_id, topic_id)?;

        self.validate_partition_permissions(session, stream, topic, "create")?;

        let created_at = IggyTimestamp::now();
        let shards_count = self.get_available_shards_count();

        let parent_stats = self
            .metadata
            .get_topic_stats(stream, topic)
            .expect("Parent topic stats must exist");

        let metas: Vec<PartitionMeta> = (0..partitions_count)
            .map(|_| PartitionMeta {
                id: 0,
                created_at,
                revision_id: 0,
                stats: Arc::new(PartitionStats::new(parent_stats.clone())),
                consumer_offsets: None,
                consumer_group_offsets: None,
            })
            .collect();

        let assigned_ids = self.metadata.add_partitions(stream, topic, metas);

        let partition_infos: Vec<PartitionInfo> = assigned_ids
            .iter()
            .map(|&id| PartitionInfo { id, created_at })
            .collect();

        self.metrics.increment_partitions(partitions_count);
        self.metrics.increment_segments(partitions_count);

        for info in &partition_infos {
            let partition_id = info.id;
            let ns = IggyNamespace::new(stream, topic, partition_id);
            let shard_id = ShardId::new(calculate_shard_assignment(&ns, shards_count));
            let is_current_shard = self.id == *shard_id;
            // TODO(hubcio): LocalIdx(0) is wrong.. When IggyPartitions is integrated into
            // IggyShard, this should use the actual index returned by IggyPartitions::insert().
            let location = PartitionLocation::new(shard_id, LocalIdx::new(0));
            self.insert_shard_table_record(ns, location);

            create_partition_file_hierarchy(stream, topic, partition_id, &self.config.system)
                .await?;

            if is_current_shard {
                self.init_partition_directly(stream, topic, partition_id, created_at)
                    .await?;
            }
        }
        Ok(partition_infos)
    }

    pub async fn init_log(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: usize,
    ) -> Result<(), IggyError> {
        let (stream, topic) = self.resolve_topic_id(stream_id, topic_id)?;

        let created_at = self
            .metadata
            .load()
            .streams
            .get(stream)
            .and_then(|s| s.topics.get(topic))
            .and_then(|t| t.partitions.get(partition_id))
            .map(|meta| meta.created_at)
            .unwrap_or_else(IggyTimestamp::now);

        self.init_partition_directly(stream, topic, partition_id, created_at)
            .await
    }

    /// Thread-safety: Uses `pending_partition_inits` to prevent TOCTOU races where
    /// multiple tasks could see needs_init=true and all try to initialize. Only the
    /// first task to mark the partition as pending will proceed; others return early.
    pub async fn init_partition_directly(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
        created_at: IggyTimestamp,
    ) -> Result<(), IggyError> {
        let ns = IggyNamespace::new(stream_id, topic_id, partition_id);

        // Check if partition already exists in partition_store
        // If it does, verify revision_id matches SharedMetadata
        // If not, the old entry is stale and needs to be removed
        let needs_init = {
            let store = self.partition_store.borrow();
            let metadata = self.metadata.load();
            let partition_meta = metadata
                .streams
                .get(stream_id)
                .and_then(|s| s.topics.get(topic_id))
                .and_then(|t| t.partitions.get(partition_id));

            match (store.get(&ns), partition_meta) {
                (Some(data), Some(meta)) if data.revision_id == meta.revision_id => false,
                (Some(_), _) => {
                    // Stale entry with different revision_id
                    drop(store);
                    self.partition_store.borrow_mut().remove(&ns);
                    true
                }
                (None, _) => true,
            }
        };

        if !needs_init {
            return Ok(());
        }

        {
            let mut pending = self.pending_partition_inits.borrow_mut();
            if pending.contains(&ns) {
                return Ok(());
            }
            pending.insert(ns);
        }

        let result = self
            .init_partition_directly_inner(stream_id, topic_id, partition_id, created_at, ns)
            .await;

        self.pending_partition_inits.borrow_mut().remove(&ns);

        result
    }

    async fn init_partition_directly_inner(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
        created_at: IggyTimestamp,
        ns: IggyNamespace,
    ) -> Result<(), IggyError> {
        info!(
            "Initializing partition in partition_store: partition ID: {} for topic ID: {} for stream ID: {}",
            partition_id, topic_id, stream_id
        );

        let stats = self
            .metadata
            .get_partition_stats_by_ids(stream_id, topic_id, partition_id)
            .expect("Partition stats must exist in SharedMetadata");

        let partition_path =
            self.config
                .system
                .get_partition_path(stream_id, topic_id, partition_id);

        let mut loaded_log = crate::bootstrap::load_segments(
            &self.config.system,
            stream_id,
            topic_id,
            partition_id,
            partition_path,
            stats.clone(),
        )
        .await?;

        // If no segments exist on disk (newly created partition), create an initial segment
        if !loaded_log.has_segments() {
            info!(
                "No segments found on disk for partition ID: {} for topic ID: {} for stream ID: {}, creating initial segment",
                partition_id, topic_id, stream_id
            );

            let start_offset = 0;
            let segment = Segment::new(
                start_offset,
                self.config.system.segment.size,
                self.config.system.segment.message_expiry,
            );

            let storage = create_segment_storage(
                &self.config.system,
                stream_id,
                topic_id,
                partition_id,
                0, // messages_size
                0, // indexes_size
                start_offset,
            )
            .await?;

            loaded_log.add_persisted_segment(segment, storage);
            stats.increment_segments_count(1);
        }

        let current_offset = loaded_log.active_segment().end_offset;

        let revision_id = self
            .metadata
            .load()
            .streams
            .get(stream_id)
            .and_then(|s| s.topics.get(topic_id))
            .and_then(|t| t.partitions.get(partition_id))
            .map(|meta| meta.revision_id)
            .unwrap_or(0);

        let consumer_offsets = Arc::new(ConsumerOffsets::with_capacity(0));
        let consumer_group_offsets = Arc::new(ConsumerGroupOffsets::with_capacity(0));

        self.metadata.set_partition_offsets(
            stream_id,
            topic_id,
            partition_id,
            consumer_offsets.clone(),
            consumer_group_offsets.clone(),
        );

        let partition_data = PartitionData::with_log(
            loaded_log,
            stats,
            std::sync::Arc::new(std::sync::atomic::AtomicU64::new(current_offset)),
            consumer_offsets,
            consumer_group_offsets,
            None, // message_deduplicator
            created_at,
            revision_id,
            current_offset > 0, // should_increment_offset - true if we have messages
        );

        self.partition_store.borrow_mut().insert(ns, partition_data);

        info!(
            "Initialized partition in partition_store: partition ID: {} for topic ID: {} for stream ID: {} with offset: {}",
            partition_id, topic_id, stream_id, current_offset
        );

        Ok(())
    }

    pub async fn delete_partitions(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<Vec<usize>, IggyError> {
        self.ensure_partitions_exist(stream_id, topic_id, partitions_count)?;

        let (stream, topic) = self.resolve_topic_id(stream_id, topic_id)?;

        self.validate_partition_permissions(session, stream, topic, "delete")?;

        let all_partition_ids: Vec<usize> = {
            let metadata = self.metadata.load();
            metadata
                .streams
                .get(stream)
                .and_then(|s| s.topics.get(topic))
                .map(|t| {
                    let mut ids: Vec<_> = t.partitions.keys().collect();
                    ids.sort_unstable();
                    ids
                })
                .unwrap_or_default()
        };

        let partitions_to_delete: Vec<usize> = all_partition_ids
            .into_iter()
            .rev()
            .take(partitions_count as usize)
            .collect();

        let topic_stats = self
            .metadata
            .load()
            .streams
            .get(stream)
            .and_then(|s| s.topics.get(topic))
            .map(|t| t.stats.clone());

        let mut deleted_ids = Vec::with_capacity(partitions_to_delete.len());
        let mut total_messages_count: u64 = 0;
        let mut total_segments_count: u32 = 0;
        let mut total_size_bytes: u64 = 0;

        for partition_id in &partitions_to_delete {
            let ns = IggyNamespace::new(stream, topic, *partition_id);

            if let Some(stats) =
                self.metadata
                    .get_partition_stats_by_ids(stream, topic, *partition_id)
            {
                total_segments_count += stats.segments_count_inconsistent();
                total_messages_count += stats.messages_count_inconsistent();
                total_size_bytes += stats.size_bytes_inconsistent();
            }

            self.remove_shard_table_record(&ns);
            self.partition_store.borrow_mut().remove(&ns);
            self.delete_partition_dir(stream, topic, *partition_id)
                .await?;

            deleted_ids.push(*partition_id);
        }

        self.metadata
            .delete_partitions(stream, topic, &partitions_to_delete);

        self.metrics
            .decrement_partitions(partitions_to_delete.len() as u32);
        self.metrics.decrement_segments(total_segments_count);

        if let Some(parent) = topic_stats {
            parent.decrement_messages_count(total_messages_count);
            parent.decrement_size_bytes(total_size_bytes);
            parent.decrement_segments_count(total_segments_count);
        }

        Ok(deleted_ids)
    }

    async fn delete_partition_dir(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
    ) -> Result<(), IggyError> {
        delete_partitions_from_disk(stream_id, topic_id, partition_id, &self.config.system).await
    }

    fn delete_partitions_base(
        &self,
        stream: usize,
        topic: usize,
        partitions_count: u32,
    ) -> Vec<usize> {
        let all_partition_ids: Vec<usize> = {
            let metadata = self.metadata.load();
            metadata
                .streams
                .get(stream)
                .and_then(|s| s.topics.get(topic))
                .map(|t| {
                    let mut ids: Vec<_> = t.partitions.keys().collect();
                    ids.sort_unstable();
                    ids
                })
                .unwrap_or_default()
        };

        let partition_ids: Vec<usize> = all_partition_ids
            .into_iter()
            .rev()
            .take(partitions_count as usize)
            .collect();

        {
            let mut store = self.partition_store.borrow_mut();
            for &partition_id in &partition_ids {
                let ns = IggyNamespace::new(stream, topic, partition_id);
                store.remove(&ns);
            }
        }

        self.metadata
            .delete_partitions(stream, topic, &partition_ids);

        partition_ids
    }

    pub fn delete_partitions_bypass_auth(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
        partition_ids: Vec<usize>,
    ) -> Result<(), IggyError> {
        self.ensure_partitions_exist(stream_id, topic_id, partitions_count)?;
        let (stream, topic) = self.resolve_topic_id(stream_id, topic_id)?;

        if partitions_count as usize != partition_ids.len() {
            return Err(IggyError::InvalidPartitionsCount);
        }

        let deleted_ids = self.delete_partitions_base(stream, topic, partitions_count);
        for (deleted_partition_id, actual_deleted_partition_id) in
            deleted_ids.iter().zip(partition_ids.iter())
        {
            assert_eq!(
                *deleted_partition_id, *actual_deleted_partition_id,
                "delete_partitions_bypass_auth: partition mismatch ID"
            );
        }
        Ok(())
    }
}
