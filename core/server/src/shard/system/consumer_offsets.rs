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
use crate::streaming::partitions::consumer_offset::ConsumerOffset;
use crate::streaming::partitions::storage;
use crate::streaming::polling_consumer::{ConsumerGroupId, PollingConsumer};
use crate::streaming::session::Session;
use err_trail::ErrContext;
use iggy_common::{Consumer, ConsumerKind, ConsumerOffsetInfo, Identifier, IggyError};
use std::sync::atomic::Ordering;

impl IggyShard {
    pub async fn store_consumer_offset(
        &self,
        session: &Session,
        consumer: Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
        offset: u64,
    ) -> Result<(PollingConsumer, usize), IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_topic_exists(stream_id, topic_id)?;

        // Get numeric IDs from SharedMetadata
        let numeric_stream_id = self
            .shared_metadata
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
        let numeric_topic_id = self
            .shared_metadata
            .get_topic_id(numeric_stream_id, topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;

        self.permissioner
            .store_consumer_offset(session.get_user_id(), numeric_stream_id, numeric_topic_id)
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - permission denied to store consumer offset for user with ID: {}, consumer: {consumer} in topic with ID: {numeric_topic_id} and stream with ID: {numeric_stream_id}",
                    session.get_user_id(),
                )
            })?;

        let Some((polling_consumer, partition_id)) = self.resolve_consumer_with_partition_id(
            stream_id,
            topic_id,
            &consumer,
            session.client_id,
            partition_id,
            false,
        )?
        else {
            return Err(IggyError::NotResolvedConsumer(consumer.id));
        };
        self.ensure_partition_exists(stream_id, topic_id, partition_id)?;

        self.store_consumer_offset_base(
            numeric_stream_id,
            numeric_topic_id,
            &polling_consumer,
            partition_id,
            offset,
        );
        self.persist_consumer_offset_to_disk(
            numeric_stream_id,
            numeric_topic_id,
            &polling_consumer,
            partition_id,
        )
        .await?;
        Ok((polling_consumer, partition_id))
    }

    pub async fn get_consumer_offset(
        &self,
        session: &Session,
        consumer: Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
    ) -> Result<Option<ConsumerOffsetInfo>, IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_topic_exists(stream_id, topic_id)?;

        // Get numeric IDs from SharedMetadata
        let numeric_stream_id = self
            .shared_metadata
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
        let numeric_topic_id = self
            .shared_metadata
            .get_topic_id(numeric_stream_id, topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;

        self.permissioner
            .get_consumer_offset(session.get_user_id(), numeric_stream_id, numeric_topic_id)
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - permission denied to get consumer offset for user with ID: {}, consumer: {consumer} in topic with ID: {numeric_topic_id} and stream with ID: {numeric_stream_id}",
                    session.get_user_id()
                )
            })?;

        let Some((polling_consumer, partition_id)) = self.resolve_consumer_with_partition_id(
            stream_id,
            topic_id,
            &consumer,
            session.client_id,
            partition_id,
            false,
        )?
        else {
            return Err(IggyError::NotResolvedConsumer(consumer.id));
        };
        self.ensure_partition_exists(stream_id, topic_id, partition_id)?;

        // Get the partition's current offset from SharedMetadata
        use iggy_common::sharding::IggyNamespace;
        let ns = IggyNamespace::new(numeric_stream_id, numeric_topic_id, partition_id);
        let partition_current_offset = self
            .shared_metadata
            .get_partition(&ns)
            .map(|p| p.current_offset())
            .unwrap_or(0);

        let offset = match polling_consumer {
            PollingConsumer::Consumer(id, _) => {
                let offsets = self.shared_consumer_offsets.get_consumer_offsets(
                    numeric_stream_id,
                    numeric_topic_id,
                    partition_id,
                );
                offsets.and_then(|co| {
                    let guard = co.pin();
                    guard.get(&id).map(|offset| ConsumerOffsetInfo {
                        partition_id: partition_id as u32,
                        current_offset: partition_current_offset,
                        stored_offset: offset.offset.load(Ordering::Relaxed),
                    })
                })
            }
            PollingConsumer::ConsumerGroup(consumer_group_id, _) => {
                let offsets = self.shared_consumer_offsets.get_consumer_group_offsets(
                    numeric_stream_id,
                    numeric_topic_id,
                    partition_id,
                );
                offsets.and_then(|co| {
                    let guard = co.pin();
                    guard
                        .get(&consumer_group_id)
                        .map(|offset| ConsumerOffsetInfo {
                            partition_id: partition_id as u32,
                            current_offset: partition_current_offset,
                            stored_offset: offset.offset.load(Ordering::Relaxed),
                        })
                })
            }
        };
        Ok(offset)
    }

    pub async fn delete_consumer_offset(
        &self,
        session: &Session,
        consumer: Consumer,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: Option<u32>,
    ) -> Result<(PollingConsumer, usize), IggyError> {
        self.ensure_authenticated(session)?;
        self.ensure_topic_exists(stream_id, topic_id)?;

        // Get numeric IDs from SharedMetadata
        let numeric_stream_id = self
            .shared_metadata
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
        let numeric_topic_id = self
            .shared_metadata
            .get_topic_id(numeric_stream_id, topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;

        self.permissioner
            .delete_consumer_offset(session.get_user_id(), numeric_stream_id, numeric_topic_id)
            .error(|e: &IggyError| {
                format!(
                    "{COMPONENT} (error: {e}) - permission denied to delete consumer offset for user with ID: {}, consumer: {consumer} in topic with ID: {numeric_topic_id} and stream with ID: {numeric_stream_id}",
                    session.get_user_id(),
                )
            })?;

        let Some((polling_consumer, partition_id)) = self.resolve_consumer_with_partition_id(
            stream_id,
            topic_id,
            &consumer,
            session.client_id,
            partition_id,
            false,
        )?
        else {
            return Err(IggyError::NotResolvedConsumer(consumer.id));
        };
        self.ensure_partition_exists(stream_id, topic_id, partition_id)?;

        let path = self.delete_consumer_offset_base(
            numeric_stream_id,
            numeric_topic_id,
            &polling_consumer,
            partition_id,
        )?;
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
        // Get numeric IDs from SharedMetadata
        let numeric_stream_id = self
            .shared_metadata
            .get_stream_id(stream_id)
            .ok_or_else(|| IggyError::StreamIdNotFound(stream_id.clone()))?;
        let numeric_topic_id = self
            .shared_metadata
            .get_topic_id(numeric_stream_id, topic_id)
            .ok_or_else(|| IggyError::TopicIdNotFound(topic_id.clone(), stream_id.clone()))?;

        for &partition_id in partition_ids {
            // Get consumer group offsets
            let offsets = self.shared_consumer_offsets.get_consumer_group_offsets(
                numeric_stream_id,
                numeric_topic_id,
                partition_id,
            );

            // Skip if offset does not exist
            let Some(offsets) = offsets else {
                continue;
            };

            let path = {
                let guard = offsets.pin();
                let item = guard.get(&cg_id);
                if item.is_none() {
                    continue;
                }
                item.unwrap().path.clone()
            };

            // Remove from in-memory store
            offsets.pin().remove(&cg_id);

            self.delete_consumer_offset_from_disk(&path)
                .await
                .error(|e: &IggyError| {
                    format!(
                        "{COMPONENT} (error: {e}) - failed to delete consumer group offset file for group with ID: {:?} in partition {} of topic with ID: {} and stream with ID: {}",
                        cg_id, partition_id, topic_id, stream_id
                    )
                })?;
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
                let offsets = self.shared_consumer_offsets.get_consumer_offsets(
                    stream_id,
                    topic_id,
                    partition_id,
                );

                if let Some(offsets) = offsets {
                    let guard = offsets.pin();
                    if let Some(existing) = guard.get(id) {
                        existing.offset.store(offset, Ordering::Relaxed);
                    } else {
                        let dir_path = self.config.system.get_consumer_offsets_path(
                            stream_id,
                            topic_id,
                            partition_id,
                        );
                        let path = format!("{}/{}", dir_path, id);
                        let consumer_offset =
                            ConsumerOffset::new(ConsumerKind::Consumer, *id as u32, offset, path);
                        drop(guard);
                        offsets.pin().insert(*id, consumer_offset);
                    }
                }
            }
            PollingConsumer::ConsumerGroup(cg_id, _) => {
                let offsets = self.shared_consumer_offsets.get_consumer_group_offsets(
                    stream_id,
                    topic_id,
                    partition_id,
                );

                if let Some(offsets) = offsets {
                    let guard = offsets.pin();
                    if let Some(existing) = guard.get(cg_id) {
                        existing.offset.store(offset, Ordering::Relaxed);
                    } else {
                        let dir_path = self.config.system.get_consumer_group_offsets_path(
                            stream_id,
                            topic_id,
                            partition_id,
                        );
                        let path = format!("{}/{}", dir_path, cg_id.0);
                        let consumer_offset = ConsumerOffset::new(
                            ConsumerKind::ConsumerGroup,
                            cg_id.0 as u32,
                            offset,
                            path,
                        );
                        drop(guard);
                        offsets.pin().insert(*cg_id, consumer_offset);
                    }
                }
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
                    .shared_consumer_offsets
                    .get_consumer_offsets(stream_id, topic_id, partition_id)
                    .ok_or_else(|| IggyError::ConsumerOffsetNotFound(*id))?;

                let guard = offsets.pin();
                let item = guard
                    .get(id)
                    .ok_or_else(|| IggyError::ConsumerOffsetNotFound(*id))?;
                let path = item.path.clone();
                drop(guard);

                offsets.pin().remove(id);
                Ok(path)
            }
            PollingConsumer::ConsumerGroup(cg_id, _) => {
                let offsets = self
                    .shared_consumer_offsets
                    .get_consumer_group_offsets(stream_id, topic_id, partition_id)
                    .ok_or_else(|| IggyError::ConsumerOffsetNotFound(cg_id.0))?;

                let guard = offsets.pin();
                let item = guard
                    .get(cg_id)
                    .ok_or_else(|| IggyError::ConsumerOffsetNotFound(cg_id.0))?;
                let path = item.path.clone();
                drop(guard);

                offsets.pin().remove(cg_id);
                Ok(path)
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
        match polling_consumer {
            PollingConsumer::Consumer(id, _) => {
                let offsets = self.shared_consumer_offsets.get_consumer_offsets(
                    stream_id,
                    topic_id,
                    partition_id,
                );

                if let Some(offsets) = offsets {
                    let guard = offsets.pin();
                    if let Some(item) = guard.get(id) {
                        let offset_value = item.offset.load(Ordering::Relaxed);
                        let path = item.path.clone();
                        drop(guard);
                        return storage::persist_offset(&path, offset_value).await;
                    }
                }
                Err(IggyError::ConsumerOffsetNotFound(*id))
            }
            PollingConsumer::ConsumerGroup(cg_id, _) => {
                let offsets = self.shared_consumer_offsets.get_consumer_group_offsets(
                    stream_id,
                    topic_id,
                    partition_id,
                );

                if let Some(offsets) = offsets {
                    let guard = offsets.pin();
                    if let Some(item) = guard.get(cg_id) {
                        let offset_value = item.offset.load(Ordering::Relaxed);
                        let path = item.path.clone();
                        drop(guard);
                        return storage::persist_offset(&path, offset_value).await;
                    }
                }
                Err(IggyError::ConsumerOffsetNotFound(cg_id.0))
            }
        }
    }

    pub async fn delete_consumer_offset_from_disk(&self, path: &str) -> Result<(), IggyError> {
        storage::delete_persisted_offset(path).await
    }
}
