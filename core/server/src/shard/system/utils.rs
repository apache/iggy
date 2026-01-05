// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use iggy_common::{Consumer, ConsumerKind, Identifier, IggyError};

use crate::{shard::IggyShard, streaming::polling_consumer::PollingConsumer};

impl IggyShard {
    /// Checks if a stream exists in SharedMetadata (source of truth for cross-shard consistency).
    pub fn ensure_stream_exists(&self, stream_id: &Identifier) -> Result<(), IggyError> {
        let metadata = self.shared_metadata.load();
        let exists = metadata.get_stream_id(stream_id).is_some();
        if !exists {
            return Err(IggyError::StreamIdNotFound(stream_id.clone()));
        }
        Ok(())
    }

    /// Checks if a topic exists in SharedMetadata (source of truth for cross-shard consistency).
    pub fn ensure_topic_exists(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
    ) -> Result<(), IggyError> {
        let metadata = self.shared_metadata.load();
        let stream_numeric_id = metadata
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;

        let stream_meta = metadata
            .streams
            .get(&stream_numeric_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;

        let topic_exists = match topic_id.kind {
            iggy_common::IdKind::Numeric => {
                let id = topic_id.get_u32_value().unwrap_or(0) as usize;
                stream_meta.topics.contains_key(&id)
            }
            iggy_common::IdKind::String => {
                if let Ok(name) = topic_id.get_cow_str_value() {
                    stream_meta.topic_index.contains_key(name.as_ref())
                } else {
                    false
                }
            }
        };

        if !topic_exists {
            return Err(IggyError::TopicIdNotFound(
                stream_id.clone(),
                topic_id.clone(),
            ));
        }
        Ok(())
    }

    pub fn ensure_consumer_group_exists(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
    ) -> Result<(), IggyError> {
        self.ensure_stream_exists(stream_id)?;
        self.ensure_topic_exists(stream_id, topic_id)?;

        // Use SharedMetadata for consumer group existence check
        let metadata = self.shared_metadata.load();
        let numeric_stream_id = metadata
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
        let numeric_topic_id = metadata
            .get_topic_id(numeric_stream_id, topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(stream_id.clone(), topic_id.clone()))?;
        let stream_meta = metadata
            .streams
            .get(&numeric_stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
        let topic_meta = stream_meta
            .topics
            .get(&numeric_topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(stream_id.clone(), topic_id.clone()))?;

        let group_exists = match group_id.kind {
            iggy_common::IdKind::Numeric => {
                let id = group_id.get_u32_value().unwrap_or(0) as usize;
                topic_meta.consumer_groups.contains_key(&id)
            }
            iggy_common::IdKind::String => {
                if let Ok(name) = group_id.get_cow_str_value() {
                    topic_meta
                        .get_consumer_group_id_by_name(name.as_ref())
                        .is_some()
                } else {
                    false
                }
            }
        };

        if !group_exists {
            return Err(IggyError::ConsumerGroupIdNotFound(
                group_id.clone(),
                topic_id.clone(),
            ));
        }
        Ok(())
    }

    pub fn ensure_partitions_exist(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partitions_count: u32,
    ) -> Result<(), IggyError> {
        self.ensure_topic_exists(stream_id, topic_id)?;

        // Use SharedMetadata for partition count (source of truth for cross-shard consistency)
        let metadata = self.shared_metadata.load();
        let numeric_stream_id = metadata
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
        let numeric_topic_id = metadata
            .get_topic_id(numeric_stream_id, topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(stream_id.clone(), topic_id.clone()))?;
        let stream_meta = metadata
            .streams
            .get(&numeric_stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
        let topic_meta = stream_meta
            .topics
            .get(&numeric_topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(stream_id.clone(), topic_id.clone()))?;
        let actual_partitions_count = topic_meta.partitions.len() as u32;
        drop(metadata);

        if partitions_count > actual_partitions_count {
            return Err(IggyError::InvalidPartitionsCount);
        }

        Ok(())
    }

    pub fn ensure_partition_exists(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: usize,
    ) -> Result<(), IggyError> {
        self.ensure_topic_exists(stream_id, topic_id)?;

        // Use SharedMetadata for partition existence check (source of truth for cross-shard consistency)
        let metadata = self.shared_metadata.load();
        let numeric_stream_id = metadata
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
        let numeric_topic_id = metadata
            .get_topic_id(numeric_stream_id, topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(stream_id.clone(), topic_id.clone()))?;
        let stream_meta = metadata
            .streams
            .get(&numeric_stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
        let topic_meta = stream_meta
            .topics
            .get(&numeric_topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(stream_id.clone(), topic_id.clone()))?;
        let partition_exists = topic_meta.partitions.contains_key(&partition_id);
        drop(metadata);

        if !partition_exists {
            return Err(IggyError::PartitionNotFound(
                partition_id,
                topic_id.clone(),
                stream_id.clone(),
            ));
        }

        Ok(())
    }

    pub fn resolve_consumer_with_partition_id(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        consumer: &Consumer,
        client_id: u32,
        partition_id: Option<u32>,
        calculate_partition_id: bool,
    ) -> Result<Option<(PollingConsumer, usize)>, IggyError> {
        match consumer.kind {
            ConsumerKind::Consumer => {
                let partition_id = partition_id.unwrap_or(0);
                Ok(Some((
                    PollingConsumer::consumer(&consumer.id, partition_id as usize),
                    partition_id as usize,
                )))
            }
            ConsumerKind::ConsumerGroup => {
                self.ensure_consumer_group_exists(stream_id, topic_id, &consumer.id)?;

                // Use SharedMetadata to get consumer group info
                let metadata = self.shared_metadata.load();
                let numeric_stream_id = metadata
                    .get_stream_id(stream_id)
                    .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
                let numeric_topic_id = metadata
                    .get_topic_id(numeric_stream_id, topic_id)
                    .ok_or_else(|| {
                        IggyError::TopicIdNotFound(stream_id.clone(), topic_id.clone())
                    })?;

                let stream_meta = metadata
                    .streams
                    .get(&numeric_stream_id)
                    .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
                let topic_meta = stream_meta.topics.get(&numeric_topic_id).ok_or_else(|| {
                    IggyError::TopicIdNotFound(stream_id.clone(), topic_id.clone())
                })?;

                // Get consumer group ID
                let cg_id = match consumer.id.kind {
                    iggy_common::IdKind::Numeric => {
                        consumer.id.get_u32_value().unwrap_or(0) as usize
                    }
                    iggy_common::IdKind::String => {
                        let name = consumer.id.get_cow_str_value().map_err(|_| {
                            IggyError::ConsumerGroupIdNotFound(
                                consumer.id.clone(),
                                topic_id.clone(),
                            )
                        })?;
                        topic_meta
                            .get_consumer_group_id_by_name(name.as_ref())
                            .ok_or_else(|| {
                                IggyError::ConsumerGroupNameNotFound(
                                    name.to_string(),
                                    topic_id.clone(),
                                )
                            })?
                    }
                };

                let group = topic_meta.consumer_groups.get(&cg_id).ok_or_else(|| {
                    IggyError::ConsumerGroupIdNotFound(consumer.id.clone(), topic_id.clone())
                })?;

                // Check if member exists
                let member = group.get_member(client_id).ok_or_else(|| {
                    IggyError::ConsumerGroupMemberNotFound(
                        client_id,
                        consumer.id.clone(),
                        topic_id.clone(),
                    )
                })?;

                // Use client_id as member_id for PollingConsumer (consistent identifier)
                let member_id = client_id as usize;

                // If partition_id is explicitly provided, use it
                if let Some(partition_id) = partition_id {
                    return Ok(Some((
                        PollingConsumer::consumer_group(cg_id, member_id),
                        partition_id as usize,
                    )));
                }

                // Get member's assigned partitions (clone to release metadata guard)
                let assigned_partitions = member.partitions.clone();
                if assigned_partitions.is_empty() {
                    return Ok(None);
                }

                // Calculate or get current partition ID
                let key = (numeric_stream_id, numeric_topic_id, cg_id, client_id);
                drop(metadata); // Release the guard before borrowing cg_partition_indices
                let mut indices = self.cg_partition_indices.borrow_mut();

                let partition_id = if calculate_partition_id {
                    // Round-robin: get next partition and increment index
                    let current_idx = indices.entry(key).or_insert(0);
                    let partition_id =
                        assigned_partitions[*current_idx % assigned_partitions.len()];
                    *current_idx = (*current_idx + 1) % assigned_partitions.len();
                    partition_id
                } else {
                    // Get current partition without incrementing
                    let current_idx = *indices.get(&key).unwrap_or(&0);
                    assigned_partitions[current_idx % assigned_partitions.len()]
                };

                Ok(Some((
                    PollingConsumer::consumer_group(cg_id, member_id),
                    partition_id,
                )))
            }
        }
    }
}
