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
use crate::shard::transmission::event::PartitionInfo;
use crate::streaming::partitions::consumer_group_offsets::ConsumerGroupOffsets;
use crate::streaming::partitions::consumer_offsets::ConsumerOffsets;
use crate::streaming::partitions::local_partition::LocalPartition;
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
use iggy_common::sharding::IggyNamespace;
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

        let count_before = self
            .metadata
            .get_partitions_count(stream, topic)
            .unwrap_or(0);
        let partition_ids: Vec<usize> =
            (count_before..count_before + partitions_count as usize).collect();
        let partition_infos: Vec<PartitionInfo> = partition_ids
            .iter()
            .map(|&id| PartitionInfo { id, created_at })
            .collect();

        for info in &partition_infos {
            create_partition_file_hierarchy(stream, topic, info.id, &self.config.system).await?;
        }

        let metas: Vec<PartitionMeta> = (0..partitions_count)
            .map(|_| PartitionMeta {
                id: 0,
                created_at,
                revision_id: 0,
                stats: Arc::new(PartitionStats::new(parent_stats.clone())),
                consumer_offsets: Some(Arc::new(ConsumerOffsets::with_capacity(0))),
                consumer_group_offsets: Some(Arc::new(ConsumerGroupOffsets::with_capacity(0))),
            })
            .collect();

        let assigned_ids = self
            .writer()
            .add_partitions(&self.metadata, stream, topic, metas);
        debug_assert_eq!(
            assigned_ids, partition_ids,
            "Partition IDs mismatch: expected {:?}, got {:?}",
            partition_ids, assigned_ids
        );

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

            if is_current_shard {
                self.ensure_partition(&ns).await?;
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
        let ns = IggyNamespace::new(stream, topic, partition_id);
        self.ensure_partition(&ns).await
    }

    /// Ensures partition is initialized in local_partitions. Idempotent.
    pub async fn ensure_partition(&self, ns: &IggyNamespace) -> Result<(), IggyError> {
        let Some(created_at) = self.partition_needs_init(ns) else {
            return Ok(());
        };

        {
            let mut pending = self.pending_partition_inits.borrow_mut();
            if pending.contains(ns) {
                return Ok(());
            }
            pending.insert(*ns);
        }

        let result = self.init_partition_inner(ns, created_at).await;

        self.pending_partition_inits.borrow_mut().remove(ns);

        result
    }

    fn partition_needs_init(&self, ns: &IggyNamespace) -> Option<IggyTimestamp> {
        let init_info =
            self.metadata
                .get_partition_init_info(ns.stream_id(), ns.topic_id(), ns.partition_id());

        let revision_id = init_info.as_ref().map(|m| m.revision_id);

        let needs_init = {
            let partitions = self.local_partitions.borrow();
            match (partitions.get(ns), revision_id) {
                (Some(data), Some(rev)) if data.revision_id == rev => false,
                (Some(_), _) => {
                    drop(partitions);
                    self.local_partitions.borrow_mut().remove(ns);
                    true
                }
                (None, _) => true,
            }
        };

        if needs_init {
            Some(
                init_info
                    .map(|m| m.created_at)
                    .unwrap_or_else(IggyTimestamp::now),
            )
        } else {
            None
        }
    }

    async fn init_partition_inner(
        &self,
        ns: &IggyNamespace,
        created_at: IggyTimestamp,
    ) -> Result<(), IggyError> {
        let stream_id = ns.stream_id();
        let topic_id = ns.topic_id();
        let partition_id = ns.partition_id();

        info!(
            "Initializing partition in local_partitions: partition ID: {} for topic ID: {} for stream ID: {}",
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
                0,
                0,
                start_offset,
            )
            .await?;

            loaded_log.add_persisted_segment(segment, storage);
            stats.increment_segments_count(1);
        }

        let current_offset = loaded_log.active_segment().end_offset;

        let (revision_id, consumer_offsets, consumer_group_offsets) = self
            .metadata
            .get_partition_init_info(stream_id, topic_id, partition_id)
            .map(|info| {
                (
                    info.revision_id,
                    info.consumer_offsets
                        .unwrap_or_else(|| Arc::new(ConsumerOffsets::with_capacity(0))),
                    info.consumer_group_offsets
                        .unwrap_or_else(|| Arc::new(ConsumerGroupOffsets::with_capacity(0))),
                )
            })
            .unwrap_or_else(|| {
                (
                    0,
                    Arc::new(ConsumerOffsets::with_capacity(0)),
                    Arc::new(ConsumerGroupOffsets::with_capacity(0)),
                )
            });

        let partition = LocalPartition::with_log(
            loaded_log,
            stats,
            std::sync::Arc::new(std::sync::atomic::AtomicU64::new(current_offset)),
            consumer_offsets,
            consumer_group_offsets,
            None,
            created_at,
            revision_id,
            current_offset > 0,
        );

        self.local_partitions.borrow_mut().insert(*ns, partition);

        info!(
            "Initialized partition in local_partitions: partition ID: {} for topic ID: {} for stream ID: {} with offset: {}",
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

        let all_partition_ids = self.metadata.get_partition_ids(stream, topic);

        let partitions_to_delete: Vec<usize> = all_partition_ids
            .into_iter()
            .rev()
            .take(partitions_count as usize)
            .collect();

        let topic_stats = self.metadata.get_topic_stats(stream, topic);

        let mut total_messages_count: u64 = 0;
        let mut total_segments_count: u32 = 0;
        let mut total_size_bytes: u64 = 0;

        for partition_id in &partitions_to_delete {
            if let Some(stats) =
                self.metadata
                    .get_partition_stats_by_ids(stream, topic, *partition_id)
            {
                total_segments_count += stats.segments_count_inconsistent();
                total_messages_count += stats.messages_count_inconsistent();
                total_size_bytes += stats.size_bytes_inconsistent();
            }
        }

        self.writer()
            .delete_partitions(stream, topic, partitions_to_delete.len() as u32);

        for partition_id in &partitions_to_delete {
            let ns = IggyNamespace::new(stream, topic, *partition_id);
            self.remove_shard_table_record(&ns);
            self.local_partitions.borrow_mut().remove(&ns);
        }

        for partition_id in &partitions_to_delete {
            self.delete_partition_dir(stream, topic, *partition_id)
                .await?;
        }

        self.metrics
            .decrement_partitions(partitions_to_delete.len() as u32);
        self.metrics.decrement_segments(total_segments_count);

        if let Some(parent) = topic_stats {
            parent.decrement_messages_count(total_messages_count);
            parent.decrement_size_bytes(total_size_bytes);
            parent.decrement_segments_count(total_segments_count);
        }

        Ok(partitions_to_delete)
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
        let all_partition_ids = self.metadata.get_partition_ids(stream, topic);

        let partition_ids: Vec<usize> = all_partition_ids
            .into_iter()
            .rev()
            .take(partitions_count as usize)
            .collect();

        {
            let mut partitions = self.local_partitions.borrow_mut();
            for &partition_id in &partition_ids {
                let ns = IggyNamespace::new(stream, topic, partition_id);
                partitions.remove(&ns);
            }
        }

        self.writer()
            .delete_partitions(stream, topic, partition_ids.len() as u32);

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
