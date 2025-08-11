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
use crate::slab::traits::Access;
use crate::slab::traits::AccessMut;
use crate::streaming::deduplication::message_deduplicator::MessageDeduplicator;
use crate::streaming::partitions::partition2;
use crate::streaming::partitions::partition2::SharedPartition;
use crate::streaming::partitions::storage2::create_partition_file_hierarchy;
use crate::streaming::session::Session;
use crate::streaming::stats::stats::PartitionStats;
use crate::streaming::stats::stats::TopicStats;
use error_set::ErrContext;
use iggy_common::Identifier;
use iggy_common::IggyError;
use iggy_common::IggyTimestamp;
use iggy_common::locking::IggyRwLockFn;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

impl IggyShard {
    fn create_shared_partition(
        &self,
        created_at: IggyTimestamp,
        topic_stats: Arc<TopicStats>,
    ) -> (partition2::Partition, SharedPartition) {
        let partition = partition2::Partition::new(created_at, false);
        let partition_stats = Arc::new(PartitionStats::new(topic_stats));

        let shared_partition = SharedPartition {
            id: partition.id(),
            stats: partition_stats,
            current_offset: Arc::new(AtomicU64::new(0)),
            consumer_group_offset: Arc::new(()),
            consumer_offset: Arc::new(()),
        };

        (partition, shared_partition)
    }

    fn create_message_deduplicator(&self) -> Option<MessageDeduplicator> {
        if !self.config.system.message_deduplication.enabled {
            return None;
        }

        let max_entries = if self.config.system.message_deduplication.max_entries > 0 {
            Some(self.config.system.message_deduplication.max_entries)
        } else {
            None
        };

        let expiry = if !self.config.system.message_deduplication.expiry.is_zero() {
            Some(self.config.system.message_deduplication.expiry)
        } else {
            None
        };

        Some(MessageDeduplicator::new(max_entries, expiry))
    }

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

    fn insert_partition_resources(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition: partition2::Partition,
        shared_partition: &SharedPartition,
    ) -> usize {
        self.streams2
            .with_topic_by_id_mut(stream_id, topic_id, |topic| {
                let partition_id = topic
                    .partitions_mut()
                    .with_mut(|partitions| partition.insert_into(partitions));

                topic
                    .partitions_mut()
                    .with_stats_mut(|stats| stats.insert(shared_partition.stats.clone()));

                let message_deduplicator = self.create_message_deduplicator();
                topic
                    .partitions_mut()
                    .with_message_deduplicators_mut(|deduplicators| {
                        deduplicators.insert(message_deduplicator);
                    });

                topic
                    .partitions_mut()
                    .with_partition_offsets_mut(|offsets| {
                        offsets.insert(shared_partition.current_offset.clone());
                    });

                partition_id
            })
    }

    pub async fn create_partitions2(
        &self,
        session: &Session,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<(Vec<SharedPartition>, IggyTimestamp), IggyError> {
        self.ensure_authenticated(session)?;
        let numeric_stream_id =
            self.streams2
                .with_stream_by_id(stream_id, |stream| stream.id()) as u32;
        let numeric_topic_id =
            self.streams2
                .with_topic_by_id(stream_id, topic_id, |topic| topic.id()) as u32;

        self.validate_partition_permissions(
            session,
            numeric_stream_id,
            numeric_topic_id,
            "create",
        )?;

        let topic_stats = self.streams2.with_stream_by_id(stream_id, |stream| {
            stream
                .topics()
                .with_topic_stats_by_id(topic_id, |stats| stats)
        });

        let created_at = IggyTimestamp::now();
        let shared_partitions = (0..partitions_count)
            .map(|_| {
                let partition = partition2::Partition::new(created_at, false);
                let partition_stats = Arc::new(PartitionStats::new(topic_stats.clone()));

                let mut shared_partition = SharedPartition {
                    id: partition.id(),
                    stats: partition_stats,
                    current_offset: Arc::new(AtomicU64::new(0)),
                    consumer_group_offset: Arc::new(()),
                    consumer_offset: Arc::new(()),
                };
                let partition_id = self.insert_partition_resources(
                    stream_id,
                    topic_id,
                    partition,
                    &shared_partition,
                );

                shared_partition.id = partition_id;
                shared_partition
            })
            .collect::<Vec<SharedPartition>>();

        for partition_id in shared_partitions.iter().map(|p| p.id) {
            create_partition_file_hierarchy(
                self.id,
                numeric_stream_id as usize,
                numeric_topic_id as usize,
                partition_id,
                &self.config.system,
            )
            .await?;
        }

        Ok((shared_partitions, created_at))
    }

    pub async fn create_partitions2_bypass_auth(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        created_at: IggyTimestamp,
        shared_partitions: &Vec<SharedPartition>,
    ) -> Result<(), IggyError> {
        for shared_partition in shared_partitions {
            let partition = partition2::Partition::new(created_at, false);
            let partition_id =
                self.insert_partition_resources(stream_id, topic_id, partition, shared_partition);

            if partition_id != shared_partition.id {
                return Err(IggyError::PartitionNotFound(
                    partition_id as u32,
                    self.streams2.with_stream_by_id(stream_id, |s| s.id()) as u32,
                    self.streams2
                        .with_topic_by_id(stream_id, topic_id, |t| t.id())
                        as u32,
                ));
            }
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
                .with_stream_by_id(stream_id, |stream| stream.id()) as u32;
        let numeric_topic_id =
            self.streams2
                .with_topic_by_id(stream_id, topic_id, |topic| topic.id()) as u32;

        self.validate_partition_permissions(
            session,
            numeric_stream_id,
            numeric_topic_id,
            "delete",
        )?;

        let deleted_partition_ids =
            self.streams2
                .with_topic_by_id_mut(stream_id, topic_id, |topic| {
                    let current_count = topic.partitions().with(|p| p.len()) as u32;

                    if current_count == 0 || partitions_count == 0 {
                        return Vec::new();
                    }

                    let partitions_to_delete = partitions_count.min(current_count);
                    let start_idx = (current_count - partitions_to_delete) as usize;
                    let mut deleted_ids = Vec::with_capacity(partitions_to_delete as usize);

                    for idx in (start_idx..current_count as usize).rev() {
                        topic
                            .partitions_mut()
                            .with_mut(|partitions| partitions.try_remove(idx));

                        topic
                            .partitions_mut()
                            .with_stats_mut(|stats| stats.try_remove(idx));

                        topic
                            .partitions_mut()
                            .with_message_deduplicators_mut(|deduplicators| {
                                deduplicators.try_remove(idx)
                            });

                        topic
                            .partitions_mut()
                            .with_partition_offsets_mut(|offsets| offsets.try_remove(idx));

                        deleted_ids.push(idx as u32);
                    }

                    deleted_ids
                });

        Ok(deleted_partition_ids)
    }

    pub async fn delete_partitions2_bypass_auth(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_ids: &[u32],
    ) -> Result<(), IggyError> {
        self.streams2
            .with_topic_by_id_mut(stream_id, topic_id, |topic| {
                for &partition_id in partition_ids {
                    let idx = partition_id as usize;

                    topic
                        .partitions_mut()
                        .with_mut(|partitions| partitions.try_remove(idx));

                    topic
                        .partitions_mut()
                        .with_stats_mut(|stats| stats.try_remove(idx));

                    topic
                        .partitions_mut()
                        .with_message_deduplicators_mut(|deduplicators| {
                            deduplicators.try_remove(idx)
                        });

                    topic
                        .partitions_mut()
                        .with_partition_offsets_mut(|offsets| offsets.try_remove(idx));
                }
            });

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
