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
use crate::metadata::StreamMeta;
use crate::shard::IggyShard;
use crate::streaming::session::Session;
use crate::streaming::stats::StreamStats;
use crate::streaming::streams::storage::create_stream_file_hierarchy;
use err_trail::ErrContext;
use iggy_common::{Identifier, IggyError};
use std::sync::Arc;

impl IggyShard {
    /// Create a stream. Returns the stream metadata.
    /// Writes only to SharedMetadata (single source of truth).
    pub async fn create_stream(
        &self,
        session: &Session,
        name: String,
    ) -> Result<StreamMeta, IggyError> {
        self.ensure_authenticated(session)?;
        self.permissioner.create_stream(session.get_user_id())?;

        // Check existence in SharedMetadata (source of truth)
        if self.shared_metadata.stream_exists_by_name(&name) {
            return Err(IggyError::StreamNameAlreadyExists(name));
        }

        // Create in SharedMetadata (single source of truth, auto-generates ID)
        let stream_meta = self.shared_metadata.create_stream(name)?;
        let stream_id = stream_meta.id;

        // Create and register stats in SharedStatsStore for cross-shard visibility
        let stats = Arc::new(StreamStats::default());
        self.shared_stats.register_stream_stats(stream_id, stats);

        self.metrics.increment_streams(1);

        create_stream_file_hierarchy(stream_id, &self.config.system).await?;
        Ok(stream_meta)
    }

    pub fn update_stream(
        &self,
        session: &Session,
        stream_id: &Identifier,
        name: String,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_stream_exists(stream_id)?;

        // Get stream ID from SharedMetadata
        let id = self
            .shared_metadata
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;

        self.permissioner
            .update_stream(session.get_user_id(), id)
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - failed to update stream, user ID: {}, stream ID: {}",
                    session.get_user_id(),
                    stream_id
                )
            })?;
        self.update_stream_base(stream_id, name)?;
        Ok(())
    }

    fn update_stream_base(&self, id: &Identifier, name: String) -> Result<(), IggyError> {
        // Get current name from SharedMetadata
        let snapshot = self.shared_metadata.load();
        let stream_id = self
            .shared_metadata
            .get_stream_id(id)
            .ok_or_else(|| IggyError::StreamIdNotFound(id.clone()))?;
        let stream_meta = snapshot
            .streams
            .get(&stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(id.clone()))?;
        let old_name = stream_meta.name.clone();
        drop(snapshot);

        if old_name == name {
            return Ok(());
        }

        // Check if new name already exists
        if self.shared_metadata.stream_exists_by_name(&name) {
            return Err(IggyError::StreamNameAlreadyExists(name.to_string()));
        }

        // Update in SharedMetadata (single source of truth)
        self.shared_metadata.update_stream(id, name)?;

        Ok(())
    }

    /// Delete a stream. Returns the deleted stream metadata.
    /// Writes only to SharedMetadata (single source of truth).
    pub async fn delete_stream(
        &self,
        session: &Session,
        id: &Identifier,
    ) -> Result<StreamMeta, IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_stream_exists(id)?;

        // Get stream ID from SharedMetadata (source of truth)
        let stream_id_usize = self
            .shared_metadata
            .get_stream_id(id)
            .ok_or_else(|| IggyError::StreamIdNotFound(id.clone()))?;

        self.permissioner
            .delete_stream(session.get_user_id(), stream_id_usize)
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - permission denied to delete stream for user {}, stream ID: {}",
                    session.get_user_id(),
                    stream_id_usize,
                )
            })?;

        // Delete from SharedMetadata (single source of truth) - returns the deleted metadata
        let stream_meta = self.shared_metadata.delete_stream(id)?;

        // Clean up consumer groups from ClientManager for this stream
        self.client_manager
            .delete_consumer_groups_for_stream(stream_id_usize);

        // Remove all entries from shards_table for this stream (all topics and partitions)
        let namespaces_to_remove: Vec<_> = self
            .shards_table
            .iter()
            .filter_map(|(ns, _)| {
                if ns.stream_id() == stream_id_usize {
                    Some(ns)
                } else {
                    None
                }
            })
            .collect();

        // Collect unique topic_ids for topic stats cleanup
        let mut topic_ids_to_remove: Vec<usize> = namespaces_to_remove
            .iter()
            .map(|ns| ns.topic_id())
            .collect();
        topic_ids_to_remove.sort_unstable();
        topic_ids_to_remove.dedup();

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

        // Clean up topic stats from SharedStatsStore
        for topic_id in topic_ids_to_remove {
            self.shared_stats
                .remove_topic_stats(stream_id_usize, topic_id);
        }

        // Clean up stream stats from SharedStatsStore
        self.shared_stats.remove_stream_stats(stream_id_usize);

        // Clean up partition_store for this stream
        self.partition_store
            .borrow_mut()
            .remove_stream_partitions(stream_id_usize);

        // Delete stream directory from disk
        let stream_path = self.config.system.get_stream_path(stream_id_usize);
        if let Err(e) = remove_dir_all(&stream_path).await {
            tracing::warn!("Failed to remove stream directory {}: {}", stream_path, e);
        }

        self.metrics.decrement_streams(1);
        Ok(stream_meta)
    }

    pub async fn purge_stream(
        &self,
        session: &Session,
        stream_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_stream_exists(stream_id)?;

        // Get stream ID from SharedMetadata
        let numeric_stream_id = self
            .shared_metadata
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;

        self.permissioner
            .purge_stream(session.get_user_id(), numeric_stream_id)
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - permission denied to purge stream for user {}, stream ID: {}",
                    session.get_user_id(),
                    numeric_stream_id,
                )
            })?;

        self.purge_stream_base(stream_id).await
    }

    async fn purge_stream_base(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        // Get topic IDs from SharedMetadata
        let numeric_stream_id = self
            .shared_metadata
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;

        let snapshot = self.shared_metadata.load();
        let stream_meta = snapshot
            .streams
            .get(&numeric_stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
        let topic_ids: Vec<usize> = stream_meta.topics.keys().copied().collect();
        drop(snapshot);

        for topic_id in topic_ids {
            let topic_identifier = Identifier::numeric(topic_id as u32).unwrap();
            self.purge_topic_base(stream_id, &topic_identifier).await?;
        }

        Ok(())
    }
}
