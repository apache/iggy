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

use crate::state::system::PartitionState;
use crate::streaming::partitions::COMPONENT;
use crate::streaming::partitions::partition::Partition;
use compio::fs::create_dir_all;
use error_set::ErrContext;
use iggy_common::IggyError;
use std::path::Path;
use std::sync::atomic::Ordering;
use tracing::error;

impl Partition {
    pub async fn load(&mut self, state: PartitionState) -> Result<(), IggyError> {
        let storage = self.storage.clone();
        storage.partition.load(self, state).await
    }

    pub async fn persist(&mut self) -> Result<(), IggyError> {
        let storage = self.storage.clone();
        storage.partition.save(self).await
    }

    pub async fn delete(&mut self) -> Result<(), IggyError> {
        for segment in &mut self.segments {
            segment.delete().await.with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to delete segment: {segment}",)
            })?;
            self.segments_count_of_parent_stream
                .fetch_sub(1, Ordering::SeqCst);
        }
        self.storage.partition.delete(self).await
    }

    pub async fn purge(&mut self) -> Result<(), IggyError> {
        self.current_offset = 0;
        self.unsaved_messages_count = 0;
        self.should_increment_offset = false;
        self.consumer_offsets.clear();
        self.consumer_group_offsets.clear();

        for segment in &mut self.segments {
            segment.delete().await.with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to delete segment: {segment}",)
            })?;
            self.segments_count_of_parent_stream
                .fetch_sub(1, Ordering::SeqCst);
        }
        self.segments.clear();
        self.storage
            .partition
            .delete_consumer_offsets(&self.consumer_offsets_path)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to delete consumer offsets in partition: {self}")
            })?;
        self.storage
            .partition
            .delete_consumer_offsets(&self.consumer_group_offsets_path)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to delete consumer offsets in partition: {self}")
            })?;
        self.add_persisted_segment(0)
            .await
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to add persisted segment in partition: {self}",)
            })?;

        if !Path::new(&self.consumer_offsets_path).exists()
            && create_dir_all(&self.consumer_offsets_path).await.is_err()
        {
            error!(
                "Failed to create consumer offsets directory for partition with ID: {} for stream with ID: {} and topic with ID: {}.",
                self.partition_id, self.stream_id, self.topic_id
            );
            return Err(IggyError::CannotCreateConsumerOffsetsDirectory(
                self.consumer_offsets_path.to_owned(),
            ));
        }

        if !Path::new(&self.consumer_group_offsets_path).exists()
            && create_dir_all(&self.consumer_group_offsets_path)
                .await
                .is_err()
        {
            error!(
                "Failed to create consumer group offsets directory for partition with ID: {} for stream with ID: {} and topic with ID: {}.",
                self.partition_id, self.stream_id, self.topic_id
            );
            return Err(IggyError::CannotCreateConsumerOffsetsDirectory(
                self.consumer_group_offsets_path.to_owned(),
            ));
        }

        Ok(())
    }
}
