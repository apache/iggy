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

use crate::shard::task_registry::TaskRegistry;
use crate::streaming::partitions as streaming_partitions;
use crate::streaming::partitions::consumer_offset::ConsumerOffset;
use crate::streaming::stats::StreamStats;
use crate::{
    binary::handlers::messages::poll_messages_handler::IggyPollMetadata,
    configs::{cache_indexes::CacheIndexesConfig, system::SystemConfig},
    shard::{namespace::IggyFullNamespace, system::messages::PollingArgs},
    slab::{
        Keyed,
        consumer_groups::ConsumerGroups,
        helpers,
        partitions::{self, Partitions},
        topics::Topics,
        traits_ext::{
            ComponentsById, DeleteCell, EntityComponentSystem, EntityComponentSystemMutCell,
            InsertCell, InteriorMutability, IntoComponents,
        },
    },
    streaming::{
        partitions::{
            journal::Journal,
            partition::{PartitionRef, PartitionRefMut},
        },
        polling_consumer::PollingConsumer,
        segments::{
            IggyMessagesBatchMut, IggyMessagesBatchSet, Segment, storage::create_segment_storage,
        },
        streams::{
            self,
            stream::{self, StreamRef, StreamRefMut},
        },
        topics::{
            self,
            consumer_group::{ConsumerGroupRef, ConsumerGroupRefMut},
            topic::{TopicRef, TopicRefMut},
        },
        traits::MainOps,
    },
};
use ahash::AHashMap;
use err_trail::ErrContext;
use iggy_common::{Identifier, IggyError, IggyTimestamp, PollingKind};
use slab::Slab;
use std::{
    cell::RefCell,
    rc::Rc,
    sync::{Arc, atomic::Ordering},
};
use tracing::error;

const CAPACITY: usize = 1024;
pub type ContainerId = usize;

#[derive(Debug, Clone)]
pub struct Streams {
    index: RefCell<AHashMap<<stream::StreamRoot as Keyed>::Key, ContainerId>>,
    root: RefCell<Slab<stream::StreamRoot>>,
    stats: RefCell<Slab<Arc<StreamStats>>>,
}

impl Default for Streams {
    fn default() -> Self {
        Self {
            index: RefCell::new(AHashMap::with_capacity(CAPACITY)),
            root: RefCell::new(Slab::with_capacity(CAPACITY)),
            stats: RefCell::new(Slab::with_capacity(CAPACITY)),
        }
    }
}

impl<'a> From<&'a Streams> for stream::StreamRef<'a> {
    fn from(value: &'a Streams) -> Self {
        let root = value.root.borrow();
        let stats = value.stats.borrow();
        stream::StreamRef::new(root, stats)
    }
}

impl<'a> From<&'a Streams> for stream::StreamRefMut<'a> {
    fn from(value: &'a Streams) -> Self {
        let root = value.root.borrow_mut();
        let stats = value.stats.borrow_mut();
        stream::StreamRefMut::new(root, stats)
    }
}

impl InsertCell for Streams {
    type Idx = ContainerId;
    type Item = stream::Stream;

    fn insert(&self, item: Self::Item) -> Self::Idx {
        let (root, stats) = item.into_components();
        let mut root_container = self.root.borrow_mut();
        let mut indexes = self.index.borrow_mut();
        let mut stats_container = self.stats.borrow_mut();

        let key = root.key().clone();
        let entity_id = root_container.insert(root);
        let id = stats_container.insert(stats);
        assert_eq!(
            entity_id, id,
            "stream_insert: id mismatch when inserting stats"
        );
        let root = root_container.get_mut(entity_id).unwrap();
        root.update_id(entity_id);
        indexes.insert(key, entity_id);
        entity_id
    }
}

impl DeleteCell for Streams {
    type Idx = ContainerId;
    type Item = stream::Stream;

    fn delete(&self, id: Self::Idx) -> Self::Item {
        let mut root_container = self.root.borrow_mut();
        let mut indexes = self.index.borrow_mut();
        let mut stats_container = self.stats.borrow_mut();

        let root = root_container.remove(id);
        let stats = stats_container.remove(id);

        // Remove from index
        let key = root.key();
        indexes
            .remove(key)
            .expect("stream_delete: key not found in index");

        stream::Stream::new_with_components(root, stats)
    }
}

impl EntityComponentSystem<InteriorMutability> for Streams {
    type Idx = ContainerId;
    type Entity = stream::Stream;
    type EntityComponents<'a> = stream::StreamRef<'a>;

    fn with_components<O, F>(&self, f: F) -> O
    where
        F: for<'a> FnOnce(Self::EntityComponents<'a>) -> O,
    {
        f(self.into())
    }
}

impl EntityComponentSystemMutCell for Streams {
    type EntityComponentsMut<'a> = stream::StreamRefMut<'a>;

    fn with_components_mut<O, F>(&self, f: F) -> O
    where
        F: for<'a> FnOnce(Self::EntityComponentsMut<'a>) -> O,
    {
        f(self.into())
    }
}

impl MainOps for Streams {
    type Namespace = IggyFullNamespace;
    type PollingArgs = PollingArgs;
    type Consumer = PollingConsumer;
    type In = IggyMessagesBatchMut;
    type Out = (IggyPollMetadata, IggyMessagesBatchSet);
    type Error = IggyError;

    async fn append_messages(
        &self,
        config: &SystemConfig,
        registry: &Rc<TaskRegistry>,
        ns: &Self::Namespace,
        mut input: Self::In,
    ) -> Result<(), Self::Error> {
        let stream_id = ns.stream_id();
        let topic_id = ns.topic_id();
        let partition_id = ns.partition_id();

        let current_offset = self.with_partition_by_id(
            stream_id,
            topic_id,
            partition_id,
            streaming_partitions::helpers::calculate_current_offset(),
        );

        let current_position =
            self.with_partition_by_id(stream_id, topic_id, partition_id, |(.., log)| {
                log.active_segment().current_position
            });
        let (segment_start_offset, message_deduplicator) = self.with_partition_by_id(
            stream_id,
            topic_id,
            partition_id,
            streaming_partitions::helpers::get_segment_start_offset_and_deduplicator(),
        );

        input
            .prepare_for_persistence(
                segment_start_offset,
                current_offset,
                current_position,
                message_deduplicator.as_ref(),
            )
            .await;

        let (journal_messages_count, journal_size) = self.with_partition_by_id_mut(
            stream_id,
            topic_id,
            partition_id,
            streaming_partitions::helpers::append_to_journal(current_offset, input),
        )?;

        let unsaved_messages_count_exceeded =
            journal_messages_count >= config.partition.messages_required_to_save;
        let unsaved_messages_size_exceeded = journal_size
            >= config
                .partition
                .size_of_messages_required_to_save
                .as_bytes_u64() as u32;

        let is_full = self.with_partition_by_id(
            stream_id,
            topic_id,
            partition_id,
            streaming_partitions::helpers::is_segment_full(),
        );

        // Try committing the journal
        if is_full || unsaved_messages_count_exceeded || unsaved_messages_size_exceeded {
            let reason = self.with_partition_by_id(
                stream_id,
                topic_id,
                partition_id,
                streaming_partitions::helpers::persist_reason(
                    unsaved_messages_count_exceeded,
                    unsaved_messages_size_exceeded,
                    journal_messages_count,
                    journal_size,
                    config,
                ),
            );

            let _batch_count = self
                .persist_messages(stream_id, topic_id, partition_id, &reason, config)
                .await?;

            if is_full {
                self.handle_full_segment(registry, stream_id, topic_id, partition_id, config)
                    .await?;
            }
        }
        Ok(())
    }

    async fn poll_messages(
        &self,
        ns: &Self::Namespace,
        consumer: Self::Consumer,
        args: Self::PollingArgs,
    ) -> Result<Self::Out, Self::Error> {
        let stream_id = ns.stream_id();
        let topic_id = ns.topic_id();
        let partition_id = ns.partition_id();
        let current_offset = self.with_partition_by_id(
            stream_id,
            topic_id,
            partition_id,
            |(_, _, _, offset, ..)| offset.load(Ordering::Relaxed),
        );
        let metadata = IggyPollMetadata::new(partition_id as u32, current_offset);
        let count = args.count;
        let strategy = args.strategy;
        let value = strategy.value;
        let batches = match strategy.kind {
            PollingKind::Offset => {
                let offset = value;
                // We have to remember to keep the invariant from the if that is on line 290.
                // Alternatively a better design would be to get rid of that if and move the validations here.
                if offset > current_offset {
                    return Ok((metadata, IggyMessagesBatchSet::default()));
                }

                let batches = self
                    .get_messages_by_offset(stream_id, topic_id, partition_id, offset, count)
                    .await?;
                Ok(batches)
            }
            PollingKind::Timestamp => {
                let timestamp = IggyTimestamp::from(value);
                let timestamp_ts = timestamp.as_micros();
                tracing::trace!(
                    "Getting {count} messages by timestamp: {} for partition: {}...",
                    timestamp_ts,
                    partition_id
                );

                let batches = self
                    .get_messages_by_timestamp(
                        stream_id,
                        topic_id,
                        partition_id,
                        timestamp_ts,
                        count,
                    )
                    .await?;
                Ok(batches)
            }
            PollingKind::First => {
                let first_offset = self.with_partition_by_id(
                    stream_id,
                    topic_id,
                    partition_id,
                    |(_, _, _, _, _, _, log)| {
                        log.segments()
                            .first()
                            .map(|segment| segment.start_offset)
                            .unwrap_or(0)
                    },
                );

                let batches = self
                    .get_messages_by_offset(stream_id, topic_id, partition_id, first_offset, count)
                    .await?;
                Ok(batches)
            }
            PollingKind::Last => {
                let (start_offset, actual_count) = self.with_partition_by_id(
                    stream_id,
                    topic_id,
                    partition_id,
                    |(_, _, _, offset, _, _, _)| {
                        let current_offset = offset.load(Ordering::Relaxed);
                        let mut requested_count = count as u64;
                        if requested_count > current_offset + 1 {
                            requested_count = current_offset + 1
                        }
                        let start_offset = 1 + current_offset - requested_count;
                        (start_offset, requested_count as u32)
                    },
                );

                let batches = self
                    .get_messages_by_offset(
                        stream_id,
                        topic_id,
                        partition_id,
                        start_offset,
                        actual_count,
                    )
                    .await?;
                Ok(batches)
            }
            PollingKind::Next => {
                let consumer_offset = match consumer {
                    PollingConsumer::Consumer(consumer_id, _) => self
                        .with_partition_by_id(
                            stream_id,
                            topic_id,
                            partition_id,
                            streaming_partitions::helpers::get_consumer_offset(consumer_id),
                        )
                        .map(|c_offset| c_offset.stored_offset),
                    PollingConsumer::ConsumerGroup(consumer_group_id, _) => self
                        .with_partition_by_id(
                            stream_id,
                            topic_id,
                            partition_id,
                            streaming_partitions::helpers::get_consumer_group_offset(
                                consumer_group_id,
                            ),
                        )
                        .map(|cg_offset| cg_offset.stored_offset),
                };

                if consumer_offset.is_none() {
                    let batches = self
                        .get_messages_by_offset(stream_id, topic_id, partition_id, 0, count)
                        .await?;
                    Ok(batches)
                } else {
                    let consumer_offset = consumer_offset.unwrap();
                    let offset = consumer_offset + 1;
                    match consumer {
                        PollingConsumer::Consumer(consumer_id, _) => {
                            tracing::trace!(
                                "Getting next messages for consumer id: {} for partition: {} from offset: {}...",
                                consumer_id,
                                partition_id,
                                offset
                            );
                        }
                        PollingConsumer::ConsumerGroup(consumer_group_id, member_id) => {
                            tracing::trace!(
                                "Getting next messages for consumer group: {} member: {} for partition: {} from offset: {}...",
                                consumer_group_id.0,
                                member_id.0,
                                partition_id,
                                offset
                            );
                        }
                    }
                    let batches = self
                        .get_messages_by_offset(stream_id, topic_id, partition_id, offset, count)
                        .await?;
                    Ok(batches)
                }
            }
        }?;
        Ok((metadata, batches))
    }
}

// A mental note:
// I think we can't expose as an access interface methods such as `get_topic_by_id` or `get_partition_by_id` etc..
// In a case of a `Stream` module replacement (with a new implementation), the new implementation might not have a notion of `Topic` or `Partition` at all.
// So we should only expose some generic `get_entity_by_id` methods and rely on it's components accessors to get to the nested entities.
impl Streams {
    pub fn exists(&self, id: &Identifier) -> bool {
        match id.kind {
            iggy_common::IdKind::Numeric => {
                let id = id.get_u32_value().unwrap() as usize;
                self.root.borrow().contains(id)
            }
            iggy_common::IdKind::String => {
                let key = id.get_string_value().unwrap();
                self.index.borrow().contains_key(&key)
            }
        }
    }

    pub fn get_index(&self, id: &Identifier) -> usize {
        match id.kind {
            iggy_common::IdKind::Numeric => id.get_u32_value().unwrap() as usize,
            iggy_common::IdKind::String => {
                let key = id.get_string_value().unwrap();
                *self.index.borrow().get(&key).expect("Stream not found")
            }
        }
    }

    pub fn with_index<T>(
        &self,
        f: impl FnOnce(&AHashMap<<stream::StreamRoot as Keyed>::Key, usize>) -> T,
    ) -> T {
        let index = self.index.borrow();
        f(&index)
    }

    pub fn with_index_mut<T>(
        &self,
        f: impl FnOnce(&mut AHashMap<<stream::StreamRoot as Keyed>::Key, usize>) -> T,
    ) -> T {
        let mut index = self.index.borrow_mut();
        f(&mut index)
    }

    pub fn with_stream_by_id<T>(
        &self,
        id: &Identifier,
        f: impl FnOnce(ComponentsById<StreamRef>) -> T,
    ) -> T {
        let id = self.get_index(id);
        self.with_components_by_id(id, f)
    }

    pub fn with_stream_by_id_mut<T>(
        &self,
        id: &Identifier,
        f: impl FnOnce(ComponentsById<StreamRefMut>) -> T,
    ) -> T {
        let id = self.get_index(id);
        self.with_components_by_id_mut(id, f)
    }

    pub fn with_topics<T>(&self, stream_id: &Identifier, f: impl FnOnce(&Topics) -> T) -> T {
        self.with_stream_by_id(stream_id, helpers::topics(f))
    }

    pub fn with_topics_mut<T>(&self, stream_id: &Identifier, f: impl FnOnce(&Topics) -> T) -> T {
        self.with_stream_by_id(stream_id, helpers::topics_mut(f))
    }

    pub fn with_topic_by_id<T>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        f: impl FnOnce(ComponentsById<TopicRef>) -> T,
    ) -> T {
        self.with_topics(stream_id, |container| {
            container.with_topic_by_id(topic_id, f)
        })
    }

    pub fn with_topic_by_id_mut<T>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        f: impl FnOnce(ComponentsById<TopicRefMut>) -> T,
    ) -> T {
        self.with_topics_mut(stream_id, |container| {
            container.with_topic_by_id_mut(topic_id, f)
        })
    }

    pub fn with_consumer_groups<T>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        f: impl FnOnce(&ConsumerGroups) -> T,
    ) -> T {
        self.with_topics(stream_id, |container| {
            container.with_consumer_groups(topic_id, f)
        })
    }

    pub fn with_consumer_group_by_id<T>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
        f: impl FnOnce(ComponentsById<ConsumerGroupRef>) -> T,
    ) -> T {
        self.with_consumer_groups(stream_id, topic_id, |container| {
            container.with_consumer_group_by_id(group_id, f)
        })
    }

    pub fn with_consumer_group_by_id_mut<T>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
        f: impl FnOnce(ComponentsById<ConsumerGroupRefMut>) -> T,
    ) -> T {
        self.with_consumer_groups_mut(stream_id, topic_id, |container| {
            container.with_consumer_group_by_id_mut(group_id, f)
        })
    }

    pub fn with_consumer_groups_mut<T>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        f: impl FnOnce(&mut ConsumerGroups) -> T,
    ) -> T {
        self.with_topics_mut(stream_id, |container| {
            container.with_consumer_groups_mut(topic_id, f)
        })
    }

    pub fn with_partitions<T>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        f: impl FnOnce(&Partitions) -> T,
    ) -> T {
        self.with_topics(stream_id, |container| {
            container.with_partitions(topic_id, f)
        })
    }

    pub fn with_partitions_mut<T>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        f: impl FnOnce(&mut Partitions) -> T,
    ) -> T {
        self.with_topics_mut(stream_id, |container| {
            container.with_partitions_mut(topic_id, f)
        })
    }

    pub fn with_partition_by_id<T>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        id: partitions::ContainerId,
        f: impl FnOnce(ComponentsById<PartitionRef>) -> T,
    ) -> T {
        self.with_partitions(stream_id, topic_id, |container| {
            container.with_partition_by_id(id, f)
        })
    }

    pub fn with_partition_by_id_mut<T>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        id: partitions::ContainerId,
        f: impl FnOnce(ComponentsById<PartitionRefMut>) -> T,
    ) -> T {
        self.with_partitions_mut(stream_id, topic_id, |container| {
            container.with_partition_by_id_mut(id, f)
        })
    }

    pub async fn get_messages_by_offset(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: partitions::ContainerId,
        offset: u64,
        count: u32,
    ) -> Result<IggyMessagesBatchSet, IggyError> {
        if count == 0 {
            return Ok(IggyMessagesBatchSet::default());
        }

        use crate::streaming::partitions::helpers;
        let range = self.with_partition_by_id(
            stream_id,
            topic_id,
            partition_id,
            helpers::get_segment_range_by_offset(offset),
        );

        let mut remaining_count = count;
        let mut batches = IggyMessagesBatchSet::empty();
        let mut current_offset = offset;

        for idx in range {
            if remaining_count == 0 {
                break;
            }

            let (segment_start_offset, segment_end_offset) = self.with_partition_by_id(
                stream_id,
                topic_id,
                partition_id,
                |(_, _, _, _, _, _, log)| {
                    let segment = &log.segments()[idx];
                    (segment.start_offset, segment.end_offset)
                },
            );

            let offset = if current_offset < segment_start_offset {
                segment_start_offset
            } else {
                current_offset
            };

            let mut end_offset = offset + (remaining_count - 1).max(1) as u64;
            if end_offset > segment_end_offset {
                end_offset = segment_end_offset;
            }

            let messages = self
                .get_messages_by_offset_base(
                    stream_id,
                    topic_id,
                    partition_id,
                    idx,
                    offset,
                    end_offset,
                    remaining_count,
                    segment_start_offset,
                )
                .await?;

            let messages_count = messages.count();
            if messages_count == 0 {
                current_offset = segment_end_offset + 1;
                continue;
            }

            remaining_count = remaining_count.saturating_sub(messages_count);

            if let Some(last_offset) = messages.last_offset() {
                current_offset = last_offset + 1;
            } else if messages_count > 0 {
                current_offset += messages_count as u64;
            }

            batches.add_batch_set(messages);
        }

        Ok(batches)
    }

    #[allow(clippy::too_many_arguments)]
    async fn get_messages_by_offset_base(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: partitions::ContainerId,
        idx: usize,
        offset: u64,
        end_offset: u64,
        count: u32,
        segment_start_offset: u64,
    ) -> Result<IggyMessagesBatchSet, IggyError> {
        let (is_journal_empty, journal_first_offset, journal_last_offset) = self
            .with_partition_by_id(
                stream_id,
                topic_id,
                partition_id,
                |(_, _, _, _, _, _, log)| {
                    let journal = log.journal();
                    (
                        journal.is_empty(),
                        journal.inner().base_offset,
                        journal.inner().current_offset,
                    )
                },
            );

        // Case 0: Accumulator is empty, so all messages have to be on disk
        if is_journal_empty {
            return self
                .load_messages_from_disk_by_offset(
                    stream_id,
                    topic_id,
                    partition_id,
                    idx,
                    offset,
                    count,
                    segment_start_offset,
                )
                .await;
        }

        // Case 1: All messages are in accumulator buffer
        if offset >= journal_first_offset && end_offset <= journal_last_offset {
            let batches = self.with_partition_by_id(
                stream_id,
                topic_id,
                partition_id,
                |(_, _, _, _, _, _, log)| {
                    log.journal()
                        .get(|batches| batches.get_by_offset(offset, count))
                },
            );
            return Ok(batches);
        }

        // Case 2: All messages are on disk
        if end_offset < journal_first_offset {
            return self
                .load_messages_from_disk_by_offset(
                    stream_id,
                    topic_id,
                    partition_id,
                    idx,
                    offset,
                    count,
                    segment_start_offset,
                )
                .await;
        }

        // Case 3: Messages span disk and accumulator buffer boundary
        // Calculate how many messages we need from disk
        let disk_count = if offset < journal_first_offset {
            ((journal_first_offset - offset) as u32).min(count)
        } else {
            0
        };
        let mut combined_batch_set = IggyMessagesBatchSet::empty();

        // Load messages from disk if needed
        if disk_count > 0 {
            let disk_messages = self
                .load_messages_from_disk_by_offset(
                    stream_id,
                    topic_id,
                    partition_id,
                    idx,
                    offset,
                    disk_count,
                    segment_start_offset,
                )
                .await
                .with_error(|error| {
                    format!("Failed to load messages from disk, start offset: {offset}, count: {disk_count}, error: {error}")
                })?;

            if !disk_messages.is_empty() {
                combined_batch_set.add_batch_set(disk_messages);
            }
        }

        // Calculate how many more messages we need from the accumulator
        let remaining_count = count - combined_batch_set.count();

        if remaining_count > 0 {
            let accumulator_start_offset = std::cmp::max(offset, journal_first_offset);
            let journal_messages = self.with_partition_by_id(
                stream_id,
                topic_id,
                partition_id,
                |(_, _, _, _, _, _, log)| {
                    log.journal().get(|batches| {
                        batches.get_by_offset(accumulator_start_offset, remaining_count)
                    })
                },
            );

            if !journal_messages.is_empty() {
                combined_batch_set.add_batch_set(journal_messages);
            }
        }

        Ok(combined_batch_set)
    }

    #[allow(clippy::too_many_arguments)]
    async fn load_messages_from_disk_by_offset(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: partitions::ContainerId,
        idx: usize,
        start_offset: u64,
        count: u32,
        segment_start_offset: u64,
    ) -> Result<IggyMessagesBatchSet, IggyError> {
        let relative_start_offset = (start_offset - segment_start_offset) as u32;

        let (index_reader, messages_reader, indexes) = self.with_partition_by_id(
            stream_id,
            topic_id,
            partition_id,
            |(_, _, _, _, _, _, log)| {
                let index_reader = log.storages()[idx]
                    .index_reader
                    .as_ref()
                    .expect("Index reader not initialized")
                    .clone();
                let message_reader = log.storages()[idx]
                    .messages_reader
                    .as_ref()
                    .expect("Messages reader not initialized")
                    .clone();
                let indexes = log.indexes()[idx].as_ref().map(|indexes| {
                    indexes
                        .slice_by_offset(relative_start_offset, count)
                        .unwrap_or_default()
                });
                (index_reader, message_reader, indexes)
            },
        );

        let indexes_to_read = if let Some(indexes) = indexes {
            if !indexes.is_empty() {
                Some(indexes)
            } else {
                index_reader
                    .as_ref()
                    .load_from_disk_by_offset(relative_start_offset, count)
                    .await?
            }
        } else {
            index_reader
                .as_ref()
                .load_from_disk_by_offset(relative_start_offset, count)
                .await?
        };

        if indexes_to_read.is_none() {
            return Ok(IggyMessagesBatchSet::empty());
        }

        let indexes_to_read = indexes_to_read.unwrap();
        let batch = messages_reader
            .as_ref()
            .load_messages_from_disk(indexes_to_read)
            .await
            .with_error(|error| format!("Failed to load messages from disk: {error}"))?;

        batch
            .validate_checksums_and_offsets(start_offset)
            .with_error(|error| {
                format!("Failed to validate messages read from disk! error: {error}")
            })?;

        Ok(IggyMessagesBatchSet::from(batch))
    }

    pub async fn get_messages_by_timestamp(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: partitions::ContainerId,
        timestamp: u64,
        count: u32,
    ) -> Result<IggyMessagesBatchSet, IggyError> {
        use crate::streaming::partitions::helpers;
        let Ok(range) = self.with_partition_by_id(
            stream_id,
            topic_id,
            partition_id,
            helpers::get_segment_range_by_timestamp(timestamp),
        ) else {
            return Ok(IggyMessagesBatchSet::default());
        };

        let mut remaining_count = count;
        let mut batches = IggyMessagesBatchSet::empty();

        for idx in range {
            if remaining_count == 0 {
                break;
            }

            let segment_end_timestamp = self.with_partition_by_id(
                stream_id,
                topic_id,
                partition_id,
                |(_, _, _, _, _, _, log)| {
                    let segment = &log.segments()[idx];
                    segment.end_timestamp
                },
            );

            if segment_end_timestamp < timestamp {
                continue;
            }

            let messages = self
                .get_messages_by_timestamp_base(
                    stream_id,
                    topic_id,
                    partition_id,
                    idx,
                    timestamp,
                    remaining_count,
                )
                .await?;

            let messages_count = messages.count();
            if messages_count == 0 {
                continue;
            }

            remaining_count = remaining_count.saturating_sub(messages_count);
            batches.add_batch_set(messages);
        }

        Ok(batches)
    }

    async fn get_messages_by_timestamp_base(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: partitions::ContainerId,
        idx: usize,
        timestamp: u64,
        count: u32,
    ) -> Result<IggyMessagesBatchSet, IggyError> {
        if count == 0 {
            return Ok(IggyMessagesBatchSet::default());
        }

        let (is_journal_empty, journal_first_timestamp, journal_last_timestamp) = self
            .with_partition_by_id(
                stream_id,
                topic_id,
                partition_id,
                |(_, _, _, _, _, _, log)| {
                    let journal = log.journal();
                    (
                        journal.is_empty(),
                        journal.inner().first_timestamp,
                        journal.inner().end_timestamp,
                    )
                },
            );

        // Case 0: Accumulator is empty, so all messages have to be on disk
        if is_journal_empty {
            return self
                .load_messages_from_disk_by_timestamp(
                    stream_id,
                    topic_id,
                    partition_id,
                    idx,
                    timestamp,
                    count,
                )
                .await;
        }

        // Case 1: All messages are in accumulator buffer (timestamp is after journal ends)
        if timestamp > journal_last_timestamp {
            return Ok(IggyMessagesBatchSet::empty());
        }

        // Case 1b: Timestamp is within journal range
        if timestamp >= journal_first_timestamp {
            let batches = self.with_partition_by_id(
                stream_id,
                topic_id,
                partition_id,
                |(_, _, _, _, _, _, log)| {
                    log.journal()
                        .get(|batches| batches.get_by_timestamp(timestamp, count))
                },
            );
            return Ok(batches);
        }

        // Case 2: All messages are on disk (timestamp is before journal's first timestamp)
        let disk_messages = self
            .load_messages_from_disk_by_timestamp(
                stream_id,
                topic_id,
                partition_id,
                idx,
                timestamp,
                count,
            )
            .await?;

        if disk_messages.count() >= count {
            return Ok(disk_messages);
        }

        // Case 3: Messages span disk and accumulator buffer boundary
        let remaining_count = count - disk_messages.count();
        let journal_messages = self.with_partition_by_id(
            stream_id,
            topic_id,
            partition_id,
            |(_, _, _, _, _, _, log)| {
                log.journal()
                    .get(|batches| batches.get_by_timestamp(timestamp, remaining_count))
            },
        );

        let mut combined_batch_set = disk_messages;
        if !journal_messages.is_empty() {
            combined_batch_set.add_batch_set(journal_messages);
        }
        Ok(combined_batch_set)
    }

    async fn load_messages_from_disk_by_timestamp(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: partitions::ContainerId,
        idx: usize,
        timestamp: u64,
        count: u32,
    ) -> Result<IggyMessagesBatchSet, IggyError> {
        let (index_reader, messages_reader, indexes) = self.with_partition_by_id(
            stream_id,
            topic_id,
            partition_id,
            |(_, _, _, _, _, _, log)| {
                let index_reader = log.storages()[idx]
                    .index_reader
                    .as_ref()
                    .expect("Index reader not initialized")
                    .clone();
                let messages_reader = log.storages()[idx]
                    .messages_reader
                    .as_ref()
                    .expect("Messages reader not initialized")
                    .clone();
                let indexes = log.indexes()[idx].as_ref().map(|indexes| {
                    indexes
                        .slice_by_timestamp(timestamp, count)
                        .unwrap_or_default()
                });
                (index_reader, messages_reader, indexes)
            },
        );

        let indexes_to_read = if let Some(indexes) = indexes {
            if !indexes.is_empty() {
                Some(indexes)
            } else {
                index_reader
                    .as_ref()
                    .load_from_disk_by_timestamp(timestamp, count)
                    .await?
            }
        } else {
            index_reader
                .as_ref()
                .load_from_disk_by_timestamp(timestamp, count)
                .await?
        };

        if indexes_to_read.is_none() {
            return Ok(IggyMessagesBatchSet::empty());
        }

        let indexes_to_read = indexes_to_read.unwrap();

        let batch = messages_reader
            .as_ref()
            .load_messages_from_disk(indexes_to_read)
            .await
            .with_error(|error| {
                format!("Failed to load messages from disk by timestamp: {error}")
            })?;

        Ok(IggyMessagesBatchSet::from(batch))
    }

    pub async fn handle_full_segment(
        &self,
        registry: &Rc<TaskRegistry>,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: partitions::ContainerId,
        config: &crate::configs::system::SystemConfig,
    ) -> Result<(), IggyError> {
        let numeric_stream_id =
            self.with_stream_by_id(stream_id, streams::helpers::get_stream_id());
        let numeric_topic_id =
            self.with_topic_by_id(stream_id, topic_id, topics::helpers::get_topic_id());

        if config.segment.cache_indexes == CacheIndexesConfig::OpenSegment
            || config.segment.cache_indexes == CacheIndexesConfig::None
        {
            self.with_partition_by_id_mut(stream_id, topic_id, partition_id, |(.., log)| {
                log.clear_active_indexes();
            });
        }

        self.with_partition_by_id_mut(stream_id, topic_id, partition_id, |(.., log)| {
            log.active_segment_mut().sealed = true;
        });
        let (log_writer, index_writer) =
            self.with_partition_by_id_mut(stream_id, topic_id, partition_id, |(.., log)| {
                let (msg, index) = log.active_storage_mut().shutdown();
                (msg.unwrap(), index.unwrap())
            });

        registry
            .oneshot("fsync:segment-close-log")
            .critical(true)
            .run(move |_shutdown| async move {
                match log_writer.fsync().await {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        error!("Failed to fsync log writer on segment close: {}", e);
                        Err(e)
                    }
                }
            })
            .spawn();

        registry
            .oneshot("fsync:segment-close-index")
            .critical(true)
            .run(move |_shutdown| async move {
                match index_writer.fsync().await {
                    Ok(_) => {
                        drop(index_writer);
                        Ok(())
                    }
                    Err(e) => {
                        error!("Failed to fsync index writer on segment close: {}", e);
                        drop(index_writer);
                        Err(e)
                    }
                }
            })
            .spawn();

        let (start_offset, size, end_offset) =
            self.with_partition_by_id(stream_id, topic_id, partition_id, |(.., log)| {
                (
                    log.active_segment().start_offset,
                    log.active_segment().size,
                    log.active_segment().end_offset,
                )
            });

        tracing::info!(
            "Closed segment for stream: {}, topic: {} with start offset: {}, end offset: {}, size: {} for partition with ID: {}.",
            stream_id,
            topic_id,
            start_offset,
            end_offset,
            size,
            partition_id
        );

        let messages_size = 0;
        let indexes_size = 0;
        let segment = Segment::new(
            end_offset + 1,
            config.segment.size,
            config.segment.message_expiry,
        );

        let storage = create_segment_storage(
            config,
            numeric_stream_id,
            numeric_topic_id,
            partition_id,
            messages_size,
            indexes_size,
            end_offset + 1,
        )
        .await?;
        self.with_partition_by_id_mut(stream_id, topic_id, partition_id, |(.., log)| {
            log.add_persisted_segment(segment, storage);
        });

        Ok(())
    }

    pub async fn persist_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: usize,
        reason: &str,
        config: &SystemConfig,
    ) -> Result<u32, IggyError> {
        let is_empty = self.with_partition_by_id(stream_id, topic_id, partition_id, |(.., log)| {
            log.journal().is_empty()
        });
        if is_empty {
            return Ok(0);
        }

        let batches = self.with_partition_by_id_mut(
            stream_id,
            topic_id,
            partition_id,
            streaming_partitions::helpers::commit_journal(),
        );

        tracing::trace!(
            "Persisting messages on disk for stream ID: {}, topic ID: {}, partition ID: {} because {}...",
            stream_id,
            topic_id,
            partition_id,
            reason
        );

        let batch_count = self
            .persist_messages_to_disk(stream_id, topic_id, partition_id, batches, config)
            .await?;

        Ok(batch_count)
    }

    pub async fn persist_messages_to_disk(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: usize,
        batches: IggyMessagesBatchSet,
        config: &SystemConfig,
    ) -> Result<u32, IggyError> {
        let batch_count = batches.count();
        let batch_size = batches.size();

        if batch_count == 0 {
            return Ok(0);
        }

        let has_segments =
            self.with_partition_by_id(stream_id, topic_id, partition_id, |(.., log)| {
                log.has_segments()
            });

        if !has_segments {
            return Ok(0);
        }

        // Extract storage before async operations
        let (messages_writer, index_writer) =
            self.with_partition_by_id(stream_id, topic_id, partition_id, |(.., log)| {
                (
                    log.active_storage()
                        .messages_writer
                        .as_ref()
                        .expect("Messages writer not initialized")
                        .clone(),
                    log.active_storage()
                        .index_writer
                        .as_ref()
                        .expect("Index writer not initialized")
                        .clone(),
                )
            });

        let saved = messages_writer
            .as_ref()
            .save_batch_set(batches)
            .await
            .with_error(|error| {
                format!(
                    "Failed to save batch of {batch_count} messages \
                    ({batch_size} bytes) to stream ID: {stream_id}, topic ID: {topic_id}, partition ID: {partition_id}. {error}",
                )
            })?;

        // Extract unsaved indexes before async operation
        let unsaved_indexes_slice =
            self.with_partition_by_id(stream_id, topic_id, partition_id, |(.., log)| {
                log.active_indexes().unwrap().unsaved_slice()
            });

        let indexes_len = unsaved_indexes_slice.len();
        index_writer
            .as_ref()
            .save_indexes(unsaved_indexes_slice)
            .await
            .with_error(|error| {
                format!("Failed to save index of {indexes_len} indexes to stream ID: {stream_id}, topic ID: {topic_id} {partition_id}. {error}",)
            })?;

        tracing::trace!(
            "Persisted {} messages on disk for stream ID: {}, topic ID: {}, for partition with ID: {}, total bytes written: {}.",
            batch_count,
            stream_id,
            topic_id,
            partition_id,
            saved
        );

        self.with_partition_by_id_mut(
            stream_id,
            topic_id,
            partition_id,
            streaming_partitions::helpers::update_index_and_increment_stats(saved, config),
        );

        Ok(batch_count)
    }

    pub async fn fsync_all_messages(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: usize,
    ) -> Result<(), IggyError> {
        let storage = self.with_partition_by_id(stream_id, topic_id, partition_id, |(.., log)| {
            if !log.has_segments() {
                return None;
            }
            Some(log.active_storage().clone())
        });

        let Some(storage) = storage else {
            return Ok(());
        };

        if storage.messages_writer.is_none() || storage.index_writer.is_none() {
            return Ok(());
        }

        if let Some(ref messages_writer) = storage.messages_writer
            && let Err(e) = messages_writer.fsync().await
        {
            tracing::error!(
                "Failed to fsync messages writer for partition {}: {}",
                partition_id,
                e
            );
            return Err(e);
        }

        if let Some(ref index_writer) = storage.index_writer
            && let Err(e) = index_writer.fsync().await
        {
            tracing::error!(
                "Failed to fsync index writer for partition {}: {}",
                partition_id,
                e
            );
            return Err(e);
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn auto_commit_consumer_offset(
        &self,
        config: &SystemConfig,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: usize,
        consumer: PollingConsumer,
        offset: u64,
    ) -> Result<(), IggyError> {
        let numeric_stream_id =
            self.with_stream_by_id(stream_id, streams::helpers::get_stream_id());
        let numeric_topic_id =
            self.with_topic_by_id(stream_id, topic_id, topics::helpers::get_topic_id());

        tracing::trace!(
            "Last offset: {} will be automatically stored for {}, stream: {}, topic: {}, partition: {}",
            offset,
            consumer,
            numeric_stream_id,
            numeric_topic_id,
            partition_id
        );

        match consumer {
            PollingConsumer::Consumer(consumer_id, _) => {
                tracing::trace!(
                    "Auto-committing offset {} for consumer {} on stream {}, topic {}, partition {}",
                    offset,
                    consumer_id,
                    numeric_stream_id,
                    numeric_topic_id,
                    partition_id
                );
                let (offset_value, path) = self.with_partition_by_id(
                    stream_id,
                    topic_id,
                    partition_id,
                    |(.., offsets, _, _)| {
                        let hdl = offsets.pin();
                        let item = hdl.get_or_insert(
                            consumer_id,
                            crate::streaming::partitions::consumer_offset::ConsumerOffset::default_for_consumer(
                                consumer_id as u32,
                                &config.get_consumer_offsets_path(numeric_stream_id, numeric_topic_id, partition_id),
                            ),
                        );
                        item.offset.store(offset, Ordering::Relaxed);
                        let offset_value = item.offset.load(Ordering::Relaxed);
                        let path = item.path.clone();
                        (offset_value, path)
                    },
                );
                crate::streaming::partitions::storage::persist_offset(&path, offset_value).await?;
            }
            PollingConsumer::ConsumerGroup(consumer_group_id, _) => {
                tracing::trace!(
                    "Auto-committing offset {} for consumer group {} on stream {}, topic {}, partition {}",
                    offset,
                    consumer_group_id.0,
                    numeric_stream_id,
                    numeric_topic_id,
                    partition_id
                );
                let (offset_value, path) = self.with_partition_by_id(
                    stream_id,
                    topic_id,
                    partition_id,
                    |(.., offsets, _)| {
                        let hdl = offsets.pin();
                        let item = hdl.get_or_insert(
                            consumer_group_id,
                            ConsumerOffset::default_for_consumer_group(
                                consumer_group_id,
                                &config.get_consumer_group_offsets_path(
                                    numeric_stream_id,
                                    numeric_topic_id,
                                    partition_id,
                                ),
                            ),
                        );
                        item.offset.store(offset, Ordering::Relaxed);
                        let offset_value = item.offset.load(Ordering::Relaxed);
                        let path = item.path.clone();
                        (offset_value, path)
                    },
                );
                crate::streaming::partitions::storage::persist_offset(&path, offset_value).await?;
            }
        }

        Ok(())
    }
}
