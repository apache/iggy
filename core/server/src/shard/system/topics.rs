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
use crate::io::fs_utils::remove_dir_all;
use crate::metadata::TopicMeta;
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use crate::streaming::stats::TopicStats;
use crate::streaming::topics::storage::create_topic_file_hierarchy;
use err_trail::ErrContext;
use iggy_common::sharding::IggyNamespace;
use iggy_common::{CompressionAlgorithm, Identifier, IggyError, IggyExpiry, MaxTopicSize};
use std::sync::Arc;
use tracing::info;

impl IggyShard {
    /// Create a topic. Returns the topic metadata.
    /// Writes only to SharedMetadata (single source of truth).
    #[allow(clippy::too_many_arguments)]
    pub async fn create_topic(
        &self,
        session: &Session,
        stream_id: &Identifier,
        name: String,
        message_expiry: IggyExpiry,
        compression: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: Option<u8>,
    ) -> Result<TopicMeta, IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_stream_exists(stream_id)?;

        // Get stream ID from SharedMetadata (source of truth)
        let numeric_stream_id = self
            .shared_metadata
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;

        self.permissioner
            .create_topic(session.get_user_id(), numeric_stream_id)
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - permission denied to create topic with name: {name} in stream with ID: {stream_id} for user with ID: {}",
                    session.get_user_id(),
                )
            })?;

        // Check existence in SharedMetadata (source of truth)
        let metadata = self.shared_metadata.load();
        let stream_meta = metadata
            .streams
            .get(&numeric_stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
        if stream_meta.topic_exists(&name) {
            return Err(IggyError::TopicNameAlreadyExists(name, stream_id.clone()));
        }
        drop(metadata);

        let config = &self.config.system;
        let message_expiry = config.resolve_message_expiry(message_expiry);
        info!("Topic message expiry: {}", message_expiry);
        let max_topic_size = config.resolve_max_topic_size(max_topic_size)?;
        let replication = replication_factor.unwrap_or(1);

        // Create in SharedMetadata (single source of truth, auto-generates ID)
        let topic_meta = self.shared_metadata.create_topic(
            stream_id,
            name,
            replication,
            message_expiry,
            compression,
            max_topic_size,
        )?;
        let topic_id = topic_meta.id;

        // Get stream stats from SharedStatsStore to maintain parent chain
        let stream_stats = self
            .shared_stats
            .get_stream_stats(numeric_stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;

        // Create TopicStats with proper parent chain and register in SharedStatsStore
        let stats = Arc::new(TopicStats::new(stream_stats));
        self.shared_stats
            .register_topic_stats(numeric_stream_id, topic_id, Arc::clone(&stats));

        self.metrics.increment_topics(1);

        // Create file hierarchy for the topic
        create_topic_file_hierarchy(numeric_stream_id, topic_id, &self.config.system).await?;
        Ok(topic_meta)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn update_topic(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        name: String,
        message_expiry: IggyExpiry,
        compression_algorithm: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        replication_factor: Option<u8>,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_topic_exists(stream_id, topic_id)?;

        // Get IDs from SharedMetadata (source of truth)
        let numeric_stream_id = self
            .shared_metadata
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
        let numeric_topic_id = self
            .shared_metadata
            .get_topic_id(numeric_stream_id, topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;

        self.permissioner
            .update_topic(session.get_user_id(), numeric_stream_id, numeric_topic_id)
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - permission denied to update topic for user with id: {}, stream ID: {}, topic ID: {}",
                    session.get_user_id(),
                    numeric_stream_id,
                    numeric_topic_id,
                )
            })?;

        // Check if new name already exists (unless it's the same topic)
        let snapshot = self.shared_metadata.load();
        let stream_meta = snapshot
            .streams
            .get(&numeric_stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;

        // Check if there's another topic with the same name
        if let Some(existing_topic_id) = stream_meta.get_topic_id_by_name(&name) {
            if existing_topic_id != numeric_topic_id {
                return Err(IggyError::TopicNameAlreadyExists(name, stream_id.clone()));
            }
        }
        drop(snapshot);

        // Update in SharedMetadata (single source of truth)
        self.shared_metadata.update_topic(
            stream_id,
            topic_id,
            Some(name),
            Some(message_expiry),
            Some(compression_algorithm),
            Some(max_topic_size),
            Some(replication_factor.unwrap_or(1)),
        )?;

        Ok(())
    }

    /// Delete a topic. Returns the deleted topic metadata.
    /// Writes only to SharedMetadata (single source of truth).
    pub async fn delete_topic(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<TopicMeta, IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_topic_exists(stream_id, topic_id)?;

        // Get IDs from SharedMetadata (source of truth)
        let numeric_stream_id = self
            .shared_metadata
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
        let numeric_topic_id = self
            .shared_metadata
            .get_topic_id(numeric_stream_id, topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;

        self.permissioner
            .delete_topic(session.get_user_id(), numeric_stream_id, numeric_topic_id)
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - permission denied to delete topic with ID: {topic_id} in stream with ID: {stream_id} for user with ID: {}",
                    session.get_user_id(),
                )
            })?;

        // Delete from SharedMetadata (single source of truth) - returns the deleted metadata
        let topic_meta = self.shared_metadata.delete_topic(stream_id, topic_id)?;

        // Clean up consumer groups from ClientManager for this topic
        self.client_manager
            .delete_consumer_groups_for_topic(numeric_stream_id, numeric_topic_id);

        // Remove all partition entries from shards_table for this topic
        let namespaces_to_remove: Vec<_> = self
            .shards_table
            .iter()
            .filter_map(|(ns, _)| {
                if ns.stream_id() == numeric_stream_id && ns.topic_id() == numeric_topic_id {
                    Some(ns)
                } else {
                    None
                }
            })
            .collect();

        for ns in namespaces_to_remove {
            // Clean up partition stats from SharedStatsStore
            self.shared_stats.remove_partition_stats(
                ns.stream_id(),
                ns.topic_id(),
                ns.partition_id(),
            );

            // Clean up consumer offsets from SharedConsumerOffsetsStore
            self.shared_consumer_offsets.remove_consumer_offsets(
                ns.stream_id(),
                ns.topic_id(),
                ns.partition_id(),
            );
            self.shared_consumer_offsets.remove_consumer_group_offsets(
                ns.stream_id(),
                ns.topic_id(),
                ns.partition_id(),
            );

            self.remove_shard_table_record(&ns);
        }

        // Clean up topic stats from SharedStatsStore (also decrements stream stats)
        self.shared_stats
            .remove_topic_stats(numeric_stream_id, numeric_topic_id);

        // Clean up partition_store for this topic
        self.partition_store
            .borrow_mut()
            .remove_topic_partitions(numeric_stream_id, numeric_topic_id);

        // Delete topic directory from disk
        let topic_path = self
            .config
            .system
            .get_topic_path(numeric_stream_id, numeric_topic_id);
        if let Err(e) = remove_dir_all(&topic_path).await {
            tracing::warn!("Failed to remove topic directory {}: {}", topic_path, e);
        }

        self.metrics.decrement_topics(1);
        Ok(topic_meta)
    }

    pub async fn purge_topic(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_topic_exists(stream_id, topic_id)?;

        // Get IDs from SharedMetadata (source of truth)
        let numeric_stream_id = self
            .shared_metadata
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
        let numeric_topic_id = self
            .shared_metadata
            .get_topic_id(numeric_stream_id, topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;

        self.permissioner
            .purge_topic(session.get_user_id(), numeric_stream_id, numeric_topic_id)
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - permission denied to purge topic with ID: {topic_id} in stream with ID: {stream_id} for user with ID: {}",
                    session.get_user_id(),
                )
            })?;

        // Collect consumer offset paths to delete from SharedConsumerOffsetsStore
        let snapshot = self.shared_metadata.load();
        let stream_meta = snapshot
            .streams
            .get(&numeric_stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
        let topic_meta = stream_meta
            .get_topic(numeric_topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;
        let partition_ids: Vec<usize> = topic_meta.partitions.keys().copied().collect();
        drop(snapshot);

        // Delete consumer offset files from disk
        for partition_id in &partition_ids {
            // Consumer offsets
            if let Some(offsets) = self.shared_consumer_offsets.get_consumer_offsets(
                numeric_stream_id,
                numeric_topic_id,
                *partition_id,
            ) {
                let guard = offsets.pin();
                let paths: Vec<String> = guard.iter().map(|(_, co)| co.path.clone()).collect();
                drop(guard);
                for path in paths {
                    let _ = self.delete_consumer_offset_from_disk(&path).await;
                }
                // Clear in-memory offsets
                offsets.pin().clear();
            }

            // Consumer group offsets
            if let Some(offsets) = self.shared_consumer_offsets.get_consumer_group_offsets(
                numeric_stream_id,
                numeric_topic_id,
                *partition_id,
            ) {
                let guard = offsets.pin();
                let paths: Vec<String> = guard.iter().map(|(_, co)| co.path.clone()).collect();
                drop(guard);
                for path in paths {
                    let _ = self.delete_consumer_offset_from_disk(&path).await;
                }
                // Clear in-memory offsets
                offsets.pin().clear();
            }
        }

        self.purge_topic_base(stream_id, topic_id).await?;
        Ok(())
    }

    pub(crate) async fn purge_topic_base(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        // Get IDs from SharedMetadata (source of truth)
        let numeric_stream_id = self
            .shared_metadata
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
        let numeric_topic_id = self
            .shared_metadata
            .get_topic_id(numeric_stream_id, topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;

        // Get partition IDs from SharedMetadata
        let snapshot = self.shared_metadata.load();
        let stream_meta = snapshot
            .streams
            .get(&numeric_stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
        let topic_meta = stream_meta
            .get_topic(numeric_topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;
        let partition_ids: Vec<usize> = topic_meta.partitions.keys().copied().collect();
        drop(snapshot);

        // Purge partition_store data (clear logs but keep partitions)
        self.partition_store
            .borrow_mut()
            .purge_topic_partitions(numeric_stream_id, numeric_topic_id);

        // Delete segment files for each partition
        for part_id in partition_ids {
            let ns = IggyNamespace::new(numeric_stream_id, numeric_topic_id, part_id);

            // Only process if this shard owns the partition
            if !self.partition_store.borrow().contains(&ns) {
                continue;
            }

            self.delete_segments_base(stream_id, topic_id, part_id, u32::MAX)
                .await?;
        }

        // Zero out ALL partition stats via SharedStatsStore (cross-shard visibility)
        self.shared_stats
            .zero_out_topic_partition_stats(numeric_stream_id, numeric_topic_id);

        // Zero out partition offsets via SharedMetadata (source of truth for offsets)
        self.shared_metadata
            .zero_out_topic_partition_offsets(numeric_stream_id, numeric_topic_id);

        // Zero out topic stats
        if let Some(topic_stats) = self
            .shared_stats
            .get_topic_stats(numeric_stream_id, numeric_topic_id)
        {
            topic_stats.zero_out_all();
        }

        Ok(())
    }
}
