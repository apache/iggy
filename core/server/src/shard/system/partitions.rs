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

use std::error;
use std::sync::Arc;

use super::COMPONENT;
use crate::configs::system::SystemConfig;
use crate::shard::IggyShard;
use crate::slab::traits_ext::Delete;
use crate::slab::traits_ext::EntityComponentSystem;
use crate::slab::traits_ext::EntityMarker;
use crate::slab::traits_ext::Insert;
use crate::streaming::deduplication::message_deduplicator::MessageDeduplicator;
use crate::streaming::partitions::partition::Partition;
use crate::streaming::partitions::partition2;
use crate::streaming::partitions::storage2::create_partition_file_hierarchy;
use crate::streaming::session::Session;
use crate::streaming::stats::stats::PartitionStats;
use crate::streaming::stats::stats::TopicStats;
use error_set::ErrContext;
use iggy_common::Identifier;
use iggy_common::IggyError;
use iggy_common::IggyTimestamp;
use iggy_common::delete_partitions;
use iggy_common::locking::IggyRwLockFn;
use serde::de;
use std::sync::atomic::AtomicU64;

impl IggyShard {
    fn validate_partition_permissions(
        &self,
        session: &Session,
        stream_id: u32,
        topic_id: u32,
        operation: &str,
    ) -> Result<(), IggyError> {
        let permissioner = self.permissioner.borrow();
        let result = match operation {
            "create" => permissioner.create_partitions(session.get_user_id(), stream_id, topic_id),
            "delete" => permissioner.delete_partitions(session.get_user_id(), stream_id, topic_id),
            _ => return Err(IggyError::InvalidCommand),
        };

        result.with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - permission denied to {operation} partitions for user {} on stream ID: {}, topic ID: {}",
                session.get_user_id(),
                stream_id,
                topic_id
            )
        })
    }

    pub async fn create_partitions2(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<Vec<partition2::Partition>, IggyError> {
        self.ensure_authenticated(session)?;
        /*
        self.ensure_stream_exists(stream_id)?;
        self.ensure_topic_exists(stream_id, topic_id)?;
        */
        let numeric_stream_id =
            self.streams2
                .with_root_by_id(stream_id, |stream| stream.id()) as u32;
        let numeric_topic_id =
            self.streams2
                .with_topic_root_by_id(stream_id, topic_id, |topic| topic.id()) as u32;

        self.validate_partition_permissions(
            session,
            numeric_stream_id,
            numeric_topic_id,
            "create",
        )?;

        let parent_stats = self.streams2.with_root_by_id(stream_id, |root| {
            root.topics()
                .with_stats_by_id(topic_id, |stats| stats.clone())
        });
        let partitions = self.create_and_insert_partitions_mem(
            stream_id,
            topic_id,
            parent_stats,
            partitions_count,
        );
        for partition_id in partitions.iter().map(|p| p.id()) {
            create_partition_file_hierarchy(
                self.id,
                numeric_stream_id as usize,
                numeric_topic_id as usize,
                partition_id,
                &self.config.system,
            )
            .await?;
        }
        Ok(partitions)
    }

    fn create_and_insert_partitions_mem(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        parent_stats: Arc<TopicStats>,
        partitions_count: u32,
    ) -> Vec<partition2::Partition> {
        fn create_message_deduplicator(config: &SystemConfig) -> Option<MessageDeduplicator> {
            if !config.message_deduplication.enabled {
                return None;
            }

            let max_entries = if config.message_deduplication.max_entries > 0 {
                Some(config.message_deduplication.max_entries)
            } else {
                None
            };

            let expiry = if !config.message_deduplication.expiry.is_zero() {
                Some(config.message_deduplication.expiry)
            } else {
                None
            };
            Some(MessageDeduplicator::new(max_entries, expiry))
        }

        let range = 0..partitions_count as usize;
        let created_at = IggyTimestamp::now();
        range
            .map(|_| {
                // Areczkuuuu.
                let stats = Arc::new(PartitionStats::new(parent_stats.clone()));
                let info = partition2::PartitionRoot::new(created_at, false);
                let deduplicator = create_message_deduplicator(&self.config.system);
                let offset = Arc::new(AtomicU64::new(0));
                let consumer_offset = Arc::new(papaya::HashMap::with_capacity(2137));
                let consumer_group_offset = Arc::new(papaya::HashMap::with_capacity(2137));

                let mut partition = partition2::Partition::new(
                    info,
                    stats,
                    deduplicator,
                    offset,
                    consumer_offset,
                    consumer_group_offset,
                );
                let id = self.insert_partition_mem(stream_id, topic_id, partition.clone());
                partition.update_id(id);
                partition
            })
            .collect()
    }

    fn insert_partition_mem(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition: partition2::Partition,
    ) -> usize {
        self.streams2
            .with_partitions_mut(stream_id, topic_id, |partitions| {
                partitions.insert(partition)
            })
    }

    pub fn create_partitions2_bypass_auth(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions: Vec<partition2::Partition>,
    ) -> Result<(), IggyError> {
        for partition in partitions {
            let actual_id = partition.id();
            let id = self.insert_partition_mem(stream_id, topic_id, partition);
            assert_eq!(
                id, actual_id,
                "create_partitions_bypass_auth: partition mismatch ID, wrong creation order ?!"
            );
        }
        Ok(())
    }

    pub async fn create_partitions(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<Vec<u32>, IggyError> {
        self.ensure_authenticated(session)?;
        {
            let stream = self.get_stream(stream_id).with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - stream not found for stream ID: {stream_id}"
                )
            })?;
            let topic = self.find_topic(session, &stream, topic_id).with_error_context(|error| format!("{COMPONENT} (error: {error}) - topic not found for stream ID: {stream_id}, topic ID: {topic_id}"))?;
            self.permissioner.borrow().create_partitions(
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id,
            ).with_error_context(|error| format!(
                "{COMPONENT} (error: {error}) - permission denied to create partitions for user {} on stream ID: {}, topic ID: {}",
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id
            ))?;
        }

        let partition_ids = {
            let mut stream = self.get_stream_mut(stream_id).with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {stream_id}")
            })?;
            let stream_id = stream.stream_id;
            let topic = stream
            .get_topic_mut(topic_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to get mutable reference to stream with id: {stream_id}"
                )
            })?;
            let partition_ids = topic
            .add_persisted_partitions(partitions_count)
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to add persisted partitions, topic: {topic}")
            })?;
            partition_ids
        };

        let mut stream = self.get_stream_mut(stream_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {stream_id}")
        })?;
        let topic = stream.get_topic_mut(topic_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get topic with ID: {topic_id} in stream with ID: {stream_id}")
        })?;

        topic.reassign_consumer_groups();
        self.metrics.increment_partitions(partitions_count);
        self.metrics.increment_segments(partitions_count);
        Ok(partition_ids)
    }

    pub async fn delete_partitions2(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<Vec<u32>, IggyError> {
        self.ensure_authenticated(session)?;

        let numeric_stream_id =
            self.streams2
                .with_root_by_id(stream_id, |stream| stream.id()) as u32;
        let numeric_topic_id =
            self.streams2
                .with_topic_root_by_id(stream_id, topic_id, |topic| topic.id()) as u32;

        self.validate_partition_permissions(
            session,
            numeric_stream_id,
            numeric_topic_id,
            "delete",
        )?;
        let deleted_partition_ids =
            self.delete_partitions_base2(stream_id, topic_id, partitions_count);

        Ok(deleted_partition_ids)
    }

    fn delete_partitions_base2(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Vec<u32> {
        let deleted_partition_ids =
            self.streams2
                .with_topic_root_by_id_mut(stream_id, topic_id, |topic| {
                    let partitions = topic.partitions_mut();
                    let current_count = partitions.len() as u32;
                    let partitions_to_delete = partitions_count.min(current_count);
                    let start_idx = (current_count - partitions_to_delete) as usize;
                    let mut deleted_ids = Vec::with_capacity(partitions_to_delete as usize);
                    for idx in start_idx..current_count as usize {
                        let partition = topic.partitions_mut().delete(idx);
                        assert_eq!(partition.id(), idx);
                        deleted_ids.push(partition.id() as u32);
                    }
                    deleted_ids
                });
        deleted_partition_ids
    }

    pub fn delete_partitions2_bypass_auth(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
        partition_ids: Vec<u32>,
    ) -> Result<(), IggyError> {
        assert_eq!(partitions_count as usize, partition_ids.len());

        let deleted_partition_ids =
            self.delete_partitions_base2(stream_id, topic_id, partitions_count);
        for (deleted_partition_id, actual_deleted_partition_id) in deleted_partition_ids
            .into_iter()
            .zip(partition_ids.into_iter())
        {
            assert_eq!(
                deleted_partition_id, actual_deleted_partition_id,
                "delete_partitions2_bypass_auth: partition mismatch ID"
            );
        }
        Ok(())
    }

    pub async fn delete_partitions(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<Vec<u32>, IggyError> {
        self.ensure_authenticated(session)?;
        {
            let stream = self.get_stream(stream_id).with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - stream not found for stream ID: {stream_id}"
                )
            })?;
            let topic = self.find_topic(session, &stream, topic_id).with_error_context(|error| format!("{COMPONENT} (error: {error}) - topic not found for stream ID: {stream_id}, topic_id: {topic_id}"))?;
            self.permissioner.borrow().delete_partitions(
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id,
            ).with_error_context(|error| format!(
                "{COMPONENT} (error: {error}) - permission denied to delete partitions for user {} on stream ID: {}, topic ID: {}",
                session.get_user_id(),
                topic.stream_id,
                topic.topic_id
            ))?;
        }

        let partitions = {
            let mut stream = self.get_stream_mut(stream_id).with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {stream_id}")
            })?;
            let topic = stream
            .get_topic_mut(topic_id)
            .with_error_context(|error| {
                format!(
                    "{COMPONENT} (error: {error}) - failed to get mutable reference to stream with id: {stream_id}"
                )
            })?;

            let partitions = topic
            .delete_persisted_partitions(partitions_count)
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to delete persisted partitions for topic: {topic}")
            })?;
            partitions
        };

        let mut segments_count = 0;
        let mut messages_count = 0;
        let mut partition_ids = Vec::with_capacity(partitions.len());
        for partition in &partitions {
            let partition = partition.read().await;
            let partition_id = partition.partition_id;
            let partition_messages_count = partition.get_messages_count();
            segments_count += partition.get_segments_count();
            messages_count += partition_messages_count;
            partition_ids.push(partition_id);
        }

        let mut stream = self.get_stream_mut(stream_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get stream with ID: {stream_id}")
        })?;
        let topic = stream.get_topic_mut(topic_id).with_error_context(|error| {
            format!("{COMPONENT} (error: {error}) - failed to get topic with ID: {topic_id}")
        })?;
        topic.reassign_consumer_groups();
        if partitions.len() > 0 {
            self.metrics.decrement_partitions(partitions_count);
            self.metrics.decrement_segments(segments_count);
            self.metrics.decrement_messages(messages_count);
        }
        Ok(partition_ids)
    }
}
