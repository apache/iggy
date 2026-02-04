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

use crate::metadata::PartitionMeta;
use crate::shard::IggyShard;
use crate::shard::calculate_shard_assignment;
use crate::shard::transmission::event::PartitionInfo;
use crate::shard::transmission::message::ResolvedTopic;
use crate::streaming::partitions::consumer_group_offsets::ConsumerGroupOffsets;
use crate::streaming::partitions::consumer_offsets::ConsumerOffsets;
use crate::streaming::partitions::local_partition::LocalPartition;
use crate::streaming::partitions::storage::create_partition_file_hierarchy;
use crate::streaming::partitions::storage::delete_partitions_from_disk;
use crate::streaming::segments::Segment;
use crate::streaming::segments::storage::create_segment_storage;
use crate::streaming::stats::PartitionStats;
use iggy_common::Identifier;
use iggy_common::IggyError;
use iggy_common::IggyTimestamp;
use iggy_common::sharding::IggyNamespace;
use iggy_common::sharding::{LocalIdx, PartitionLocation, ShardId};
use std::sync::Arc;
use std::time::Duration;
use tracing::{info, warn};

const PARTITION_INIT_BASE_INTERVAL: Duration = Duration::from_micros(100);
const PARTITION_INIT_MAX_INTERVAL: Duration = Duration::from_millis(50);
const PARTITION_INIT_TIMEOUT: Duration = Duration::from_secs(5);

impl IggyShard {
    pub async fn create_partitions(
        &self,
        topic: ResolvedTopic,
        partitions_count: u32,
    ) -> Result<Vec<PartitionInfo>, IggyError> {
        let stream = topic.stream_id;
        let topic_id = topic.topic_id;

        let created_at = IggyTimestamp::now();
        let shards_count = self.get_available_shards_count();

        let parent_stats = self
            .metadata
            .get_topic_stats(stream, topic_id)
            .expect("Parent topic stats must exist");

        let count_before = self
            .metadata
            .get_partitions_count(stream, topic_id)
            .unwrap_or(0);
        let partition_ids: Vec<usize> =
            (count_before..count_before + partitions_count as usize).collect();
        let partition_infos: Vec<PartitionInfo> = partition_ids
            .iter()
            .map(|&id| PartitionInfo { id, created_at })
            .collect();

        for info in &partition_infos {
            create_partition_file_hierarchy(stream, topic_id, info.id, &self.config.system).await?;
        }

        let metas: Vec<PartitionMeta> = (0..partitions_count)
            .map(|_| PartitionMeta {
                id: 0,
                created_at,
                revision_id: 0,
                stats: Arc::new(PartitionStats::new(parent_stats.clone())),
                consumer_offsets: Arc::new(ConsumerOffsets::with_capacity(0)),
                consumer_group_offsets: Arc::new(ConsumerGroupOffsets::with_capacity(0)),
            })
            .collect();

        let assigned_ids = self
            .writer()
            .add_partitions(&self.metadata, stream, topic_id, metas);
        debug_assert_eq!(
            assigned_ids, partition_ids,
            "Partition IDs mismatch: expected {:?}, got {:?}",
            partition_ids, assigned_ids
        );

        self.metrics.increment_partitions(partitions_count);
        self.metrics.increment_segments(partitions_count);

        for info in &partition_infos {
            let partition_id = info.id;
            let ns = IggyNamespace::new(stream, topic_id, partition_id);
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

    /// Ensures partition is initialized in local_partitions. Idempotent.
    /// Returns error if partition doesn't exist in metadata.
    /// If another task is already initializing the partition, waits for it to complete.
    pub async fn ensure_partition(&self, ns: &IggyNamespace) -> Result<(), IggyError> {
        use std::time::Instant;

        let deadline = Instant::now() + PARTITION_INIT_TIMEOUT;
        let mut backoff = PARTITION_INIT_BASE_INTERVAL;

        loop {
            // partition_needs_init handles both fresh and stale entries:
            // - Returns None if fresh entry exists (same revision_id)
            // - Removes stale entry and returns Some if revision_id differs
            // - Returns Some if no entry exists
            let Some(created_at) = self.partition_needs_init(ns)? else {
                return Ok(());
            };

            if Instant::now() >= deadline {
                warn!(
                    "Partition initialization timed out after {:?} for stream: {}, topic: {}, partition: {}",
                    PARTITION_INIT_TIMEOUT,
                    ns.stream_id(),
                    ns.topic_id(),
                    ns.partition_id()
                );
                return Err(IggyError::TaskTimeout);
            }

            let is_pending = self.pending_partition_inits.borrow().contains(ns);
            if is_pending {
                compio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(PARTITION_INIT_MAX_INTERVAL);
                continue;
            }

            // Double-check after potential yield - another task may have claimed it
            {
                let mut pending = self.pending_partition_inits.borrow_mut();
                if pending.contains(ns) {
                    continue;
                }
                pending.insert(*ns);
            }

            let result = self.init_partition_inner(ns, created_at).await;
            self.pending_partition_inits.borrow_mut().remove(ns);
            return result;
        }
    }

    /// Returns `Ok(Some(timestamp))` if partition needs initialization,
    /// `Ok(None)` if already initialized, or `Err` if partition doesn't exist in metadata.
    fn partition_needs_init(&self, ns: &IggyNamespace) -> Result<Option<IggyTimestamp>, IggyError> {
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
            let created_at = init_info.map(|m| m.created_at).ok_or_else(|| {
                IggyError::PartitionNotFound(
                    ns.partition_id(),
                    Identifier::numeric(ns.topic_id() as u32).unwrap(),
                    Identifier::numeric(ns.stream_id() as u32).unwrap(),
                )
            })?;
            Ok(Some(created_at))
        } else {
            Ok(None)
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
            let segment = Segment::new(start_offset, self.config.system.segment.size);

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
                    info.consumer_offsets,
                    info.consumer_group_offsets,
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
        topic: ResolvedTopic,
        partitions_count: u32,
    ) -> Result<Vec<usize>, IggyError> {
        let stream = topic.stream_id;
        let topic_id = topic.topic_id;

        self.validate_partitions_count(topic, partitions_count)?;

        let all_partition_ids = self.metadata.get_partition_ids(stream, topic_id);

        let partitions_to_delete: Vec<usize> = all_partition_ids
            .into_iter()
            .rev()
            .take(partitions_count as usize)
            .collect();

        let topic_stats = self.metadata.get_topic_stats(stream, topic_id);

        let mut total_messages_count: u64 = 0;
        let mut total_segments_count: u32 = 0;
        let mut total_size_bytes: u64 = 0;

        for partition_id in &partitions_to_delete {
            if let Some(stats) =
                self.metadata
                    .get_partition_stats_by_ids(stream, topic_id, *partition_id)
            {
                total_segments_count += stats.segments_count_inconsistent();
                total_messages_count += stats.messages_count_inconsistent();
                total_size_bytes += stats.size_bytes_inconsistent();
            }
        }

        self.writer()
            .delete_partitions(stream, topic_id, partitions_to_delete.len() as u32);

        for partition_id in &partitions_to_delete {
            let ns = IggyNamespace::new(stream, topic_id, *partition_id);
            self.remove_shard_table_record(&ns);
            self.local_partitions.borrow_mut().remove(&ns);
        }

        for partition_id in &partitions_to_delete {
            self.delete_partition_dir(stream, topic_id, *partition_id)
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
}
