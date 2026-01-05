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
use crate::partition_store::PartitionData;
use crate::shard::IggyShard;
use crate::shard::calculate_shard_assignment;
use crate::streaming::partitions::partition;
use crate::streaming::partitions::storage::create_partition_file_hierarchy;
use crate::streaming::partitions::storage::delete_partitions_from_disk;
use crate::streaming::segments::Segment;
use crate::streaming::segments::storage::create_segment_storage;
use crate::streaming::session::Session;
use err_trail::ErrContext;
use iggy_common::Identifier;
use iggy_common::IggyError;
use iggy_common::sharding::{IggyNamespace, LocalIdx, PartitionLocation, ShardId};
use std::sync::Arc;
use std::sync::atomic::Ordering;
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
    ) -> Result<Vec<usize>, IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_topic_exists(stream_id, topic_id)?;
        let numeric_stream_id = self
            .shared_metadata
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
        let numeric_topic_id = self
            .shared_metadata
            .get_topic_id(numeric_stream_id, topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;

        self.validate_partition_permissions(
            session,
            numeric_stream_id,
            numeric_topic_id,
            "create",
        )?;

        // Create partitions in SharedMetadata FIRST to get offset Arcs (source of truth)
        let partition_metas =
            self.shared_metadata
                .create_partitions(stream_id, topic_id, partitions_count)?;

        // Get topic stats from SharedStatsStore (single source of truth for stats)
        let parent_stats = self
            .shared_stats
            .get_topic_stats(numeric_stream_id, numeric_topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;

        self.metrics.increment_partitions(partitions_count);
        self.metrics.increment_segments(partitions_count);

        let shards_count = self.get_available_shards_count();
        let mut partitions = Vec::new();

        for partition_meta in partition_metas.iter() {
            let partition_id = partition_meta.id;

            // Create PartitionStats directly using parent from SharedStatsStore
            let stats = Arc::new(crate::streaming::stats::PartitionStats::new(Arc::clone(
                &parent_stats,
            )));

            // Create consumer offset maps directly using the proper types
            let consumer_offsets = Arc::new(partition::ConsumerOffsets::with_capacity(0));
            let consumer_group_offsets =
                Arc::new(partition::ConsumerGroupOffsets::with_capacity(0));

            // Register partition stats in SharedStatsStore for cross-shard visibility
            self.shared_stats.register_partition_stats(
                numeric_stream_id,
                numeric_topic_id,
                partition_id,
                Arc::clone(&stats),
            );

            // Register consumer offsets in SharedConsumerOffsetsStore for cross-shard visibility
            self.shared_consumer_offsets.register_consumer_offsets(
                numeric_stream_id,
                numeric_topic_id,
                partition_id,
                Arc::clone(&consumer_offsets),
            );
            self.shared_consumer_offsets
                .register_consumer_group_offsets(
                    numeric_stream_id,
                    numeric_topic_id,
                    partition_id,
                    Arc::clone(&consumer_group_offsets),
                );

            let ns = IggyNamespace::new(numeric_stream_id, numeric_topic_id, partition_id);
            let shard_id = ShardId::new(calculate_shard_assignment(&ns, shards_count));
            let is_current_shard = self.id == *shard_id;
            let location = PartitionLocation::new(shard_id, LocalIdx::new(0));
            self.insert_shard_table_record(ns, location);

            create_partition_file_hierarchy(
                numeric_stream_id,
                numeric_topic_id,
                partition_id,
                &self.config.system,
            )
            .await?;
            stats.increment_segments_count(1);
            if is_current_shard {
                // Create PartitionData and insert into partition_store before init_log
                let partition_data = PartitionData::new(
                    Default::default(), // Empty SegmentedLog
                    Arc::clone(&partition_meta.offset),
                    Arc::clone(&consumer_offsets),
                    Arc::clone(&consumer_group_offsets),
                    None, // No message deduplicator for now
                    Arc::clone(&stats),
                    partition_meta.created_at,
                    partition_meta
                        .should_increment_offset
                        .load(Ordering::Relaxed),
                );
                self.partition_store
                    .borrow_mut()
                    .insert(ns.clone(), partition_data);
                self.init_log(stream_id, topic_id, partition_id).await?;
            }

            partitions.push(partition_id);
        }
        Ok(partitions)
    }

    pub async fn init_log(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: usize,
    ) -> Result<(), IggyError> {
        let numeric_stream_id = self
            .shared_metadata
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
        let numeric_topic_id = self
            .shared_metadata
            .get_topic_id(numeric_stream_id, topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;

        let start_offset = 0;
        info!(
            "Initializing log for partition ID: {} for topic ID: {} for stream ID: {} with start offset: {}",
            partition_id, numeric_topic_id, numeric_stream_id, start_offset
        );

        let segment = Segment::new(
            start_offset,
            self.config.system.segment.size,
            self.config.system.segment.message_expiry,
        );

        let messages_size = 0;
        let indexes_size = 0;
        let storage = create_segment_storage(
            &self.config.system,
            numeric_stream_id,
            numeric_topic_id,
            partition_id,
            messages_size,
            indexes_size,
            start_offset,
        )
        .await?;

        let ns = IggyNamespace::new(numeric_stream_id, numeric_topic_id, partition_id);
        let partition_rc = self.partition_store.borrow().get_rc(&ns);
        if let Some(partition_data) = partition_rc {
            partition_data
                .borrow_mut()
                .log
                .add_persisted_segment(segment, storage);
        }

        info!(
            "Initialized log for partition ID: {} for topic ID: {} for stream ID: {} with start offset: {}",
            partition_id, numeric_topic_id, numeric_stream_id, start_offset
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
        self.ensure_authenticated(session)?;
        self.ensure_partitions_exist(stream_id, topic_id, partitions_count)?;

        let numeric_stream_id = self
            .shared_metadata
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
        let numeric_topic_id = self
            .shared_metadata
            .get_topic_id(numeric_stream_id, topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;

        self.validate_partition_permissions(
            session,
            numeric_stream_id,
            numeric_topic_id,
            "delete",
        )?;

        // Get partition IDs to delete from SharedMetadata
        let partition_ids = self.shared_metadata.get_partition_ids_to_delete(
            numeric_stream_id,
            numeric_topic_id,
            partitions_count,
        );

        // Get topic stats from SharedStatsStore for updating totals
        let parent_stats = self
            .shared_stats
            .get_topic_stats(numeric_stream_id, numeric_topic_id);

        let mut deleted_ids = Vec::with_capacity(partition_ids.len());
        let mut total_messages_count = 0u64;
        let mut total_segments_count = 0u32;
        let mut total_size_bytes = 0u64;

        for partition_id in partition_ids {
            let ns = IggyNamespace::new(numeric_stream_id, numeric_topic_id, partition_id);
            self.remove_shard_table_record(&ns);

            // Get partition stats before cleanup
            if let Some(stats) = self.shared_stats.get_partition_stats(
                numeric_stream_id,
                numeric_topic_id,
                partition_id,
            ) {
                total_segments_count += stats.segments_count_inconsistent();
                total_messages_count += stats.messages_count_inconsistent();
                total_size_bytes += stats.size_bytes_inconsistent();
            }

            // Clean up partition stats from SharedStatsStore
            self.shared_stats.remove_partition_stats(
                numeric_stream_id,
                numeric_topic_id,
                partition_id,
            );

            // Clean up consumer offsets from SharedConsumerOffsetsStore
            self.shared_consumer_offsets.remove_consumer_offsets(
                numeric_stream_id,
                numeric_topic_id,
                partition_id,
            );
            self.shared_consumer_offsets.remove_consumer_group_offsets(
                numeric_stream_id,
                numeric_topic_id,
                partition_id,
            );

            // Clean up partition_store
            self.partition_store.borrow_mut().remove(&ns);

            self.delete_partition_dir(numeric_stream_id, numeric_topic_id, partition_id)
                .await?;

            deleted_ids.push(partition_id);
        }

        self.metrics.decrement_partitions(deleted_ids.len() as u32);
        self.metrics.decrement_segments(total_segments_count);

        // Update parent topic stats
        if let Some(parent) = parent_stats {
            parent.decrement_messages_count(total_messages_count);
            parent.decrement_size_bytes(total_size_bytes);
            parent.decrement_segments_count(total_segments_count);
        }

        // Delete from SharedMetadata (single source of truth)
        let _ = self
            .shared_metadata
            .delete_partitions(stream_id, topic_id, &deleted_ids);

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
}
