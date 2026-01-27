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
    streaming::{
        partitions::consumer_offset::ConsumerOffset,
        polling_consumer::{ConsumerGroupId, PollingConsumer},
    },
};
use err_trail::ErrContext;
use iggy_common::{Consumer, ConsumerKind, ConsumerOffsetInfo, Identifier, IggyError};
use std::sync::atomic::Ordering;

impl IggyShard {
    pub async fn store_consumer_offset(
        &self,
        client_id: u32,
        consumer: Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        offset: u64,
    ) -> Result<(PollingConsumer, usize), IggyError> {
        let (stream, topic) = self.resolve_topic_id(stream_id, topic_id)?;

        let Some((polling_consumer, partition_id)) = self.resolve_consumer_with_partition_id(
            stream_id,
            topic_id,
            &consumer,
            client_id,
            partition_id,
            false,
        )?
        else {
            return Err(IggyError::NotResolvedConsumer(consumer.id));
        };
        self.ensure_partition_exists(stream_id, topic_id, partition_id)?;

        self.store_consumer_offset_base(stream, topic, &polling_consumer, partition_id, offset);
        self.persist_consumer_offset_to_disk(stream, topic, &polling_consumer, partition_id)
            .await?;
        Ok((polling_consumer, partition_id))
    }

    pub async fn get_consumer_offset(
        &self,
        client_id: u32,
        consumer: Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
    ) -> Result<Option<ConsumerOffsetInfo>, IggyError> {
        let (stream, topic) = self.resolve_topic_id(stream_id, topic_id)?;

        let Some((polling_consumer, partition_id)) = self.resolve_consumer_with_partition_id(
            stream_id,
            topic_id,
            &consumer,
            client_id,
            partition_id,
            false,
        )?
        else {
            return Err(IggyError::NotResolvedConsumer(consumer.id));
        };
        self.ensure_partition_exists(stream_id, topic_id, partition_id)?;

        // Get the partition's current offset from stats (messages_count - 1, or 0 if empty)
        use iggy_common::sharding::IggyNamespace;
        let ns = IggyNamespace::new(stream, topic, partition_id);
        let partition_current_offset = self
            .metadata
            .get_partition_stats(&ns)
            .map(|s| {
                let count = s.messages_count_inconsistent();
                if count > 0 { count - 1 } else { 0 }
            })
            .unwrap_or(0);

        let offset = match polling_consumer {
            PollingConsumer::Consumer(id, _) => {
                let offsets =
                    self.metadata
                        .get_partition_consumer_offsets(stream, topic, partition_id);
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
                let offsets =
                    self.metadata
                        .get_partition_consumer_group_offsets(stream, topic, partition_id);
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
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
    ) -> Result<(PollingConsumer, usize), IggyError> {
        let (stream, topic) = self.resolve_topic_id(stream_id, topic_id)?;

        let Some((polling_consumer, partition_id)) = self.resolve_consumer_with_partition_id(
            stream_id,
            topic_id,
            &consumer,
            client_id,
            partition_id,
            false,
        )?
        else {
            return Err(IggyError::NotResolvedConsumer(consumer.id));
        };
        self.ensure_partition_exists(stream_id, topic_id, partition_id)?;

        let path =
            self.delete_consumer_offset_base(stream, topic, &polling_consumer, partition_id)?;
        self.delete_consumer_offset_from_disk(&path).await?;
        Ok((polling_consumer, partition_id))
    }

    pub async fn delete_consumer_group_offsets(
        &self,
        cg_id: ConsumerGroupId,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_ids: &[usize],
    ) -> Result<(), IggyError> {
        let (stream, topic) = self.resolve_topic_id(stream_id, topic_id)?;

        for &partition_id in partition_ids {
            let offsets =
                self.metadata
                    .get_partition_consumer_group_offsets(stream, topic, partition_id);

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
                entry.offset.store(offset, Ordering::Relaxed);
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
                entry.offset.store(offset, Ordering::Relaxed);
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
}
