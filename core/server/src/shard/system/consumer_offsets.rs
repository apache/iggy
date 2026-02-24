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
use crate::{
    shard::IggyShard,
    shard::transmission::message::{ResolvedTopic, ShardRequest, ShardRequestPayload},
    streaming::{
        partitions::consumer_offset::ConsumerOffset,
        polling_consumer::{ConsumerGroupId, PollingConsumer},
    },
};
use err_trail::ErrContext;
use iggy_common::{
    Consumer, ConsumerKind, ConsumerOffsetInfo, Identifier, IggyError, sharding::IggyNamespace,
};
use std::sync::atomic::Ordering;

impl IggyShard {
    pub async fn store_consumer_offset(
        &self,
        client_id: u32,
        consumer: Consumer,
        topic: ResolvedTopic,
        partition_id: Option<u32>,
        offset: u64,
    ) -> Result<(PollingConsumer, usize), IggyError> {
        let Some((polling_consumer, partition_id)) = self.resolve_consumer_with_partition_id(
            topic,
            &consumer,
            client_id,
            partition_id,
            false,
        )?
        else {
            return Err(IggyError::NotResolvedConsumer(consumer.id));
        };

        if !self
            .metadata
            .partition_exists(topic.stream_id, topic.topic_id, partition_id)
        {
            return Err(IggyError::PartitionNotFound(
                partition_id,
                Identifier::numeric(topic.topic_id as u32).expect("valid topic id"),
                Identifier::numeric(topic.stream_id as u32).expect("valid stream id"),
            ));
        }

        self.store_consumer_offset_base(
            topic.stream_id,
            topic.topic_id,
            &polling_consumer,
            partition_id,
            offset,
        );
        self.persist_consumer_offset_to_disk(
            topic.stream_id,
            topic.topic_id,
            &polling_consumer,
            partition_id,
        )
        .await?;

        self.maybe_complete_pending_revocation(
            &polling_consumer,
            topic.stream_id,
            topic.topic_id,
            partition_id,
        )
        .await;

        Ok((polling_consumer, partition_id))
    }

    pub async fn get_consumer_offset(
        &self,
        client_id: u32,
        consumer: Consumer,
        topic: ResolvedTopic,
        partition_id: Option<u32>,
    ) -> Result<Option<ConsumerOffsetInfo>, IggyError> {
        let (polling_consumer, partition_id) = match consumer.kind {
            ConsumerKind::Consumer => {
                let Some((polling_consumer, partition_id)) = self
                    .resolve_consumer_with_partition_id(
                        topic,
                        &consumer,
                        client_id,
                        partition_id,
                        false,
                    )?
                else {
                    return Err(IggyError::NotResolvedConsumer(consumer.id.clone()));
                };
                (polling_consumer, partition_id)
            }
            ConsumerKind::ConsumerGroup => {
                // Reading offsets doesn't require group membership — offsets are stored
                // per consumer group (not per member), so any client can query the
                // group's progress. Only store_consumer_offset enforces membership.
                let cg_id = self
                    .metadata
                    .get_consumer_group_id(topic.stream_id, topic.topic_id, &consumer.id)
                    .ok_or_else(|| {
                        IggyError::ConsumerGroupIdNotFound(
                            consumer.id.clone(),
                            Identifier::numeric(topic.topic_id as u32).unwrap(),
                        )
                    })?;
                let partition_id = partition_id.unwrap_or(0) as usize;
                (PollingConsumer::consumer_group(cg_id, 0), partition_id)
            }
        };

        if !self
            .metadata
            .partition_exists(topic.stream_id, topic.topic_id, partition_id)
        {
            return Err(IggyError::PartitionNotFound(
                partition_id,
                Identifier::numeric(topic.topic_id as u32).expect("valid topic id"),
                Identifier::numeric(topic.stream_id as u32).expect("valid stream id"),
            ));
        }

        let ns = IggyNamespace::new(topic.stream_id, topic.topic_id, partition_id);
        let partition_current_offset = self
            .metadata
            .get_partition_stats(&ns)
            .map(|s| s.current_offset())
            .unwrap_or(0);

        let offset = match polling_consumer {
            PollingConsumer::Consumer(id, _) => {
                let offsets = self.metadata.get_partition_consumer_offsets(
                    topic.stream_id,
                    topic.topic_id,
                    partition_id,
                );
                offsets.and_then(|co| {
                    let guard = co.pin();
                    guard.get(&id).map(|item| ConsumerOffsetInfo {
                        partition_id: partition_id as u32,
                        current_offset: partition_current_offset,
                        stored_offset: item.offset.load(Ordering::Relaxed),
                    })
                })
            }
            PollingConsumer::ConsumerGroup(consumer_group_id, _) => {
                let offsets = self.metadata.get_partition_consumer_group_offsets(
                    topic.stream_id,
                    topic.topic_id,
                    partition_id,
                );
                offsets.and_then(|co| {
                    let guard = co.pin();
                    guard
                        .get(&consumer_group_id)
                        .map(|item| ConsumerOffsetInfo {
                            partition_id: partition_id as u32,
                            current_offset: partition_current_offset,
                            stored_offset: item.offset.load(Ordering::Relaxed),
                        })
                })
            }
        };
        Ok(offset)
    }

    pub async fn delete_consumer_offset(
        &self,
        client_id: u32,
        consumer: Consumer,
        topic: ResolvedTopic,
        partition_id: Option<u32>,
    ) -> Result<(PollingConsumer, usize), IggyError> {
        let Some((polling_consumer, partition_id)) = self.resolve_consumer_with_partition_id(
            topic,
            &consumer,
            client_id,
            partition_id,
            false,
        )?
        else {
            return Err(IggyError::NotResolvedConsumer(consumer.id));
        };

        if !self
            .metadata
            .partition_exists(topic.stream_id, topic.topic_id, partition_id)
        {
            return Err(IggyError::PartitionNotFound(
                partition_id,
                Identifier::numeric(topic.topic_id as u32).expect("valid topic id"),
                Identifier::numeric(topic.stream_id as u32).expect("valid stream id"),
            ));
        }

        let path = self.delete_consumer_offset_base(
            topic.stream_id,
            topic.topic_id,
            &polling_consumer,
            partition_id,
        )?;
        self.delete_consumer_offset_from_disk(&path).await?;
        Ok((polling_consumer, partition_id))
    }

    pub async fn delete_consumer_group_offsets(
        &self,
        cg_id: ConsumerGroupId,
        stream_id: usize,
        topic_id: usize,
        partition_ids: &[usize],
    ) -> Result<(), IggyError> {
        for &partition_id in partition_ids {
            if !self
                .metadata
                .partition_exists(stream_id, topic_id, partition_id)
            {
                tracing::trace!(
                    "{COMPONENT} - partition {partition_id} not found in stream {stream_id}/topic {topic_id}, skipping offset cleanup for consumer group {}",
                    cg_id.0
                );
                continue;
            }

            let offsets = self.metadata.get_partition_consumer_group_offsets(
                stream_id,
                topic_id,
                partition_id,
            );

            let Some(offsets) = offsets else {
                continue;
            };

            let path = offsets.pin().remove(&cg_id).map(|item| item.path.clone());

            if let Some(path) = path {
                self.delete_consumer_offset_from_disk(&path)
                    .await
                    .error(|e: &IggyError| {
                        format!(
                            "{COMPONENT} (error: {e}) - failed to delete consumer group offset file for group with ID: {} in partition {} of topic with ID: {} and stream with ID: {}",
                            cg_id, partition_id, topic_id, stream_id
                        )
                    })?;
            }
        }

        Ok(())
    }

    fn store_consumer_offset_base(
        &self,
        stream_id: usize,
        topic_id: usize,
        polling_consumer: &PollingConsumer,
        partition_id: usize,
        offset: u64,
    ) {
        match polling_consumer {
            PollingConsumer::Consumer(id, _) => {
                let Some(offsets) =
                    self.metadata
                        .get_partition_consumer_offsets(stream_id, topic_id, partition_id)
                else {
                    return;
                };

                let guard = offsets.pin();
                let entry = guard.get_or_insert_with(*id, || {
                    let dir_path = self.config.system.get_consumer_offsets_path(
                        stream_id,
                        topic_id,
                        partition_id,
                    );
                    let path = format!("{}/{}", dir_path, id);
                    ConsumerOffset::new(ConsumerKind::Consumer, *id as u32, offset, path)
                });
                entry.offset.store(offset, Ordering::Release);
            }
            PollingConsumer::ConsumerGroup(cg_id, _) => {
                let Some(offsets) = self.metadata.get_partition_consumer_group_offsets(
                    stream_id,
                    topic_id,
                    partition_id,
                ) else {
                    return;
                };

                let guard = offsets.pin();
                let entry = guard.get_or_insert_with(*cg_id, || {
                    let dir_path = self.config.system.get_consumer_group_offsets_path(
                        stream_id,
                        topic_id,
                        partition_id,
                    );
                    let path = format!("{}/{}", dir_path, cg_id.0);
                    ConsumerOffset::new(ConsumerKind::ConsumerGroup, cg_id.0 as u32, offset, path)
                });
                entry.offset.store(offset, Ordering::Release);
            }
        }
    }

    fn delete_consumer_offset_base(
        &self,
        stream_id: usize,
        topic_id: usize,
        polling_consumer: &PollingConsumer,
        partition_id: usize,
    ) -> Result<String, IggyError> {
        match polling_consumer {
            PollingConsumer::Consumer(id, _) => {
                let offsets = self
                    .metadata
                    .get_partition_consumer_offsets(stream_id, topic_id, partition_id)
                    .ok_or_else(|| IggyError::ConsumerOffsetNotFound(*id))?;

                let guard = offsets.pin();
                let offset = guard
                    .remove(id)
                    .ok_or_else(|| IggyError::ConsumerOffsetNotFound(*id))?;
                Ok(offset.path.clone())
            }
            PollingConsumer::ConsumerGroup(cg_id, _) => {
                let offsets = self
                    .metadata
                    .get_partition_consumer_group_offsets(stream_id, topic_id, partition_id)
                    .ok_or_else(|| IggyError::ConsumerOffsetNotFound(cg_id.0))?;

                let guard = offsets.pin();
                let offset = guard
                    .remove(cg_id)
                    .ok_or_else(|| IggyError::ConsumerOffsetNotFound(cg_id.0))?;
                Ok(offset.path.clone())
            }
        }
    }

    async fn persist_consumer_offset_to_disk(
        &self,
        stream_id: usize,
        topic_id: usize,
        polling_consumer: &PollingConsumer,
        partition_id: usize,
    ) -> Result<(), IggyError> {
        use crate::streaming::partitions::storage::persist_offset;

        let (offset_value, path) = match polling_consumer {
            PollingConsumer::Consumer(id, _) => {
                let offsets = self
                    .metadata
                    .get_partition_consumer_offsets(stream_id, topic_id, partition_id)
                    .ok_or_else(|| IggyError::ConsumerOffsetNotFound(*id))?;

                let guard = offsets.pin();
                let item = guard
                    .get(id)
                    .ok_or_else(|| IggyError::ConsumerOffsetNotFound(*id))?;
                (item.offset.load(Ordering::Relaxed), item.path.clone())
            }
            PollingConsumer::ConsumerGroup(cg_id, _) => {
                let offsets = self
                    .metadata
                    .get_partition_consumer_group_offsets(stream_id, topic_id, partition_id)
                    .ok_or_else(|| IggyError::ConsumerOffsetNotFound(cg_id.0))?;

                let guard = offsets.pin();
                let item = guard
                    .get(cg_id)
                    .ok_or_else(|| IggyError::ConsumerOffsetNotFound(cg_id.0))?;
                (item.offset.load(Ordering::Relaxed), item.path.clone())
            }
        };
        persist_offset(&path, offset_value).await
    }

    pub async fn delete_consumer_offset_from_disk(&self, path: &str) -> Result<(), IggyError> {
        crate::streaming::partitions::storage::delete_persisted_offset(path).await
    }

    /// Enumerates and deletes all consumer/group offset files for a partition from disk.
    /// Uses filesystem paths from config rather than in-memory state (which may already be cleared).
    pub async fn delete_all_consumer_offset_files(
        &self,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
    ) -> Result<(), IggyError> {
        let consumers_path =
            self.config
                .system
                .get_consumer_offsets_path(stream_id, topic_id, partition_id);
        let groups_path =
            self.config
                .system
                .get_consumer_group_offsets_path(stream_id, topic_id, partition_id);

        Self::delete_all_files_in_dir(&consumers_path).await?;
        Self::delete_all_files_in_dir(&groups_path).await?;
        Ok(())
    }

    /// Complete a pending partition revocation if this offset commit satisfies it.
    pub(crate) async fn maybe_complete_pending_revocation(
        &self,
        polling_consumer: &PollingConsumer,
        stream_id: usize,
        topic_id: usize,
        partition_id: usize,
    ) {
        let PollingConsumer::ConsumerGroup(group_id, member_id) = polling_consumer else {
            return;
        };

        let completion_info = self.metadata.with_metadata(|m| {
            let topic = m.streams.get(stream_id)?.topics.get(topic_id)?;
            let group = topic.consumer_groups.get(group_id.0)?;
            let member = group.members.get(member_id.0)?;
            if !member
                .pending_revocations
                .iter()
                .any(|revocation| revocation.partition_id == partition_id)
            {
                return None;
            }
            let partition = topic.partitions.get(partition_id)?;
            let last_polled = {
                let guard = partition.last_polled_offsets.pin();
                guard.get(group_id).map(|v| v.load(Ordering::Acquire))
            };
            let can_complete = match last_polled {
                None => true,
                Some(polled) => {
                    let guard = partition.consumer_group_offsets.pin();
                    guard
                        .get(group_id)
                        .map(|co| co.offset.load(Ordering::Acquire))
                        .is_some_and(|c| c >= polled)
                }
            };
            if can_complete { Some(member.id) } else { None }
        });

        if let Some(logical_member_id) = completion_info {
            let request =
                ShardRequest::control_plane(ShardRequestPayload::CompletePartitionRevocation {
                    stream_id,
                    topic_id,
                    group_id: group_id.0,
                    member_slab_id: member_id.0,
                    member_id: logical_member_id,
                    partition_id,
                    timed_out: false,
                });
            let _ = self.send_to_control_plane(request).await;
        }
    }

    async fn delete_all_files_in_dir(dir: &str) -> Result<(), IggyError> {
        let entries = match std::fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => {
                return Err(IggyError::IoError(format!(
                    "Failed to read directory {dir}: {e}"
                )));
            }
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_file() {
                crate::streaming::partitions::storage::delete_persisted_offset(
                    &path.to_string_lossy(),
                )
                .await?;
            }
        }
        Ok(())
    }
}
