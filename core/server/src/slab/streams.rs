use crate::{
    binary::handlers::messages::poll_messages_handler::IggyPollMetadata,
    configs::{cache_indexes::CacheIndexesConfig, system::SystemConfig},
    shard::{namespace::IggyFullNamespace, system::messages::PollingArgs},
    shard_info,
    slab::{
        consumer_groups::ConsumerGroups, helpers, partitions::{self, Partitions}, topics::Topics, traits_ext::{
            ComponentsById, DeleteCell, EntityComponentSystem, EntityComponentSystemMutCell,
            InsertCell, InteriorMutability, IntoComponents,
        }, Keyed
    },
    streaming::{
        partitions::{journal::Journal, partition2::{PartitionRef, PartitionRefMut}},
        polling_consumer::PollingConsumer,
        segments::{
            storage::create_segment_storage, IggyMessagesBatchMut, IggyMessagesBatchSet, Segment2
        },
        stats::stats::StreamStats,
        streams::{
            self,
            stream2::{self, StreamRef, StreamRefMut},
        },
        topics::{
            self,
            consumer_group2::{ConsumerGroupRef, ConsumerGroupRefMut},
            topic2::{TopicRef, TopicRefMut},
        },
        traits::MainOps,
    },
};
use ahash::AHashMap;
use iggy_common::{Identifier, IggyError, IggyTimestamp, PollingKind};
use slab::Slab;
use std::{
    cell::RefCell,
    sync::{Arc, atomic::Ordering},
};
use tracing::trace;

// Import streaming partitions helpers for the persist_messages method
use crate::streaming::partitions as streaming_partitions;

const CAPACITY: usize = 1024;
pub type ContainerId = usize;

#[derive(Debug, Clone)]
pub struct Streams {
    index: RefCell<AHashMap<<stream2::StreamRoot as Keyed>::Key, ContainerId>>,
    root: RefCell<Slab<stream2::StreamRoot>>,
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

impl<'a> From<&'a Streams> for stream2::StreamRef<'a> {
    fn from(value: &'a Streams) -> Self {
        let root = value.root.borrow();
        let stats = value.stats.borrow();
        stream2::StreamRef::new(root, stats)
    }
}

impl<'a> From<&'a Streams> for stream2::StreamRefMut<'a> {
    fn from(value: &'a Streams) -> Self {
        let root = value.root.borrow_mut();
        let stats = value.stats.borrow_mut();
        stream2::StreamRefMut::new(root, stats)
    }
}

impl InsertCell for Streams {
    type Idx = ContainerId;
    type Item = stream2::Stream;

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
    type Item = stream2::Stream;

    fn delete(&self, _id: Self::Idx) -> Self::Item {
        todo!()
    }
}

impl EntityComponentSystem<InteriorMutability> for Streams {
    type Idx = ContainerId;
    type Entity = stream2::Stream;
    type EntityComponents<'a> = stream2::StreamRef<'a>;

    fn with_components<O, F>(&self, f: F) -> O
    where
        F: for<'a> FnOnce(Self::EntityComponents<'a>) -> O,
    {
        f(self.into())
    }

    fn with_components_async<O, F>(&self, f: F) -> impl Future<Output = O>
    where
        F: for<'a> AsyncFnOnce(Self::EntityComponents<'a>) -> O,
    {
        f(self.into())
    }
}

impl EntityComponentSystemMutCell for Streams {
    type EntityComponentsMut<'a> = stream2::StreamRefMut<'a>;

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
        shard_id: u16,
        config: &SystemConfig,
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

        let current_position = self.with_partition_by_id(stream_id, topic_id, partition_id, |(.., log)| {
            log.journal().inner().size + log.active_segment().size
        });
        self.with_partition_by_id_async(
            stream_id,
            topic_id,
            partition_id,
            streaming_partitions::helpers::deduplicate_messages(current_offset, current_position, &mut input),
        )
        .await;

        let (journal_messages_count, journal_size) = self.with_partition_by_id_mut(
            stream_id,
            topic_id,
            partition_id,
            streaming_partitions::helpers::append_to_journal(shard_id, current_offset, input),
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
            self.persist_messages(
                shard_id,
                stream_id,
                topic_id,
                partition_id,
                unsaved_messages_count_exceeded,
                unsaved_messages_size_exceeded,
                journal_messages_count,
                journal_size,
                config,
            )
            .await?;

            if is_full {
                self.handle_full_segment(shard_id, stream_id, topic_id, partition_id, config)
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
                trace!(
                    "Getting {count} messages by timestamp: {} for partition: {}...",
                    timestamp_ts, partition_id
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
                let (consumer_offset, consumer_id) = match consumer {
                    PollingConsumer::Consumer(consumer_id, _) => (
                        self.with_partition_by_id(
                            stream_id,
                            topic_id,
                            partition_id,
                            streaming_partitions::helpers::get_consumer_offset(consumer_id),
                        )
                        .map(|c_offset| c_offset.stored_offset),
                        consumer_id,
                    ),
                    PollingConsumer::ConsumerGroup(cg_id, _) => (
                        self.with_partition_by_id(
                            stream_id,
                            topic_id,
                            partition_id,
                            streaming_partitions::helpers::get_consumer_group_member_offset(cg_id),
                        )
                        .map(|cg_offset| cg_offset.stored_offset),
                        cg_id,
                    ),
                };

                let Some(consumer_offset) = consumer_offset else {
                    return Err(IggyError::ConsumerOffsetNotFound(consumer_id));
                };
                let offset = consumer_offset + 1;
                trace!(
                    "Getting next messages for consumer id: {} for partition: {} from offset: {}...",
                    consumer_id, partition_id, offset
                );
                let batches = self
                    .get_messages_by_offset(stream_id, topic_id, partition_id, offset, count)
                    .await?;
                Ok(batches)
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
        f: impl FnOnce(&AHashMap<<stream2::StreamRoot as Keyed>::Key, usize>) -> T,
    ) -> T {
        let index = self.index.borrow();
        f(&index)
    }

    pub fn with_index_mut<T>(
        &self,
        f: impl FnOnce(&mut AHashMap<<stream2::StreamRoot as Keyed>::Key, usize>) -> T,
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
        self.with_components_by_id(id, |stream| f(stream))
    }

    pub fn with_stream_by_id_async<T>(
        &self,
        id: &Identifier,
        f: impl AsyncFnOnce(ComponentsById<StreamRef>) -> T,
    ) -> impl Future<Output = T> {
        let id = self.get_index(id);
        self.with_components_by_id_async(id, async |stream| f(stream).await)
    }

    pub fn with_stream_by_id_mut<T>(
        &self,
        id: &Identifier,
        f: impl FnOnce(ComponentsById<StreamRefMut>) -> T,
    ) -> T {
        let id = self.get_index(id);
        self.with_components_by_id_mut(id, |stream| f(stream))
    }

    pub fn with_topics<T>(&self, stream_id: &Identifier, f: impl FnOnce(&Topics) -> T) -> T {
        self.with_stream_by_id(stream_id, helpers::topics(f))
    }

    pub fn with_topics_async<T>(
        &self,
        stream_id: &Identifier,
        f: impl AsyncFnOnce(&Topics) -> T,
    ) -> impl Future<Output = T> {
        self.with_stream_by_id_async(stream_id, helpers::topics_async(f))
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

    pub fn with_topic_by_id_async<T>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        f: impl AsyncFnOnce(ComponentsById<TopicRef>) -> T,
    ) -> impl Future<Output = T> {
        self.with_topics_async(stream_id, async |container| {
            container.with_topic_by_id_async(topic_id, f).await
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

    pub fn with_consumer_groups_async<T>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        f: impl AsyncFnOnce(&ConsumerGroups) -> T,
    ) -> impl Future<Output = T> {
        self.with_topics_async(stream_id, async |container| {
            container.with_consumer_groups_async(topic_id, f).await
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

    pub fn with_consumer_group_by_id_async<T>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        group_id: &Identifier,
        f: impl AsyncFnOnce(ComponentsById<ConsumerGroupRef>) -> T,
    ) -> impl Future<Output = T> {
        self.with_consumer_groups_async(stream_id, topic_id, async move |container| {
            container.with_consumer_group_by_id_async(group_id, f).await
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

    pub fn with_partitions_async<T>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        f: impl AsyncFnOnce(&Partitions) -> T,
    ) -> impl Future<Output = T> {
        self.with_topics_async(stream_id, async |container| {
            container.with_partitions_async(topic_id, f).await
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

    pub fn with_partition_by_id_async<T>(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        id: partitions::ContainerId,
        f: impl AsyncFnOnce(ComponentsById<PartitionRef>) -> T,
    ) -> impl Future<Output = T> {
        self.with_partitions_async(stream_id, topic_id, async move |container| {
            container.with_partition_by_id_async(id, f).await
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

    pub fn len(&self) -> usize {
        self.root.borrow().len()
    }

    pub async fn get_messages_by_offset(
        &self,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: partitions::ContainerId,
        offset: u64,
        count: u32,
    ) -> Result<IggyMessagesBatchSet, IggyError> {
        use crate::streaming::partitions::helpers;
        let range = self.with_partition_by_id(
            stream_id,
            topic_id,
            partition_id,
            helpers::get_segment_range_by_offset(offset),
        );

        self.with_partition_by_id_async(
            stream_id,
            topic_id,
            partition_id,
            helpers::get_messages_by_offset_range(offset, count, range),
        )
        .await
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

        self.with_partition_by_id_async(
            stream_id,
            topic_id,
            partition_id,
            helpers::get_messages_by_timestamp_range(timestamp, count, range),
        )
        .await
    }

    pub async fn handle_full_segment(
        &self,
        shard_id: u16,
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
                log.active_storage_mut().shutdown()
            });

        compio::runtime::spawn(async move {
            let _ = log_writer.fsync().await;
        })
        .detach();
        compio::runtime::spawn(async move {
            let _ = index_writer.fsync().await;
            drop(index_writer)
        })
        .detach();

        let (start_offset, size, end_offset) =
            self.with_partition_by_id(stream_id, topic_id, partition_id, |(.., log)| {
                (
                    log.active_segment().start_offset,
                    log.active_segment().size,
                    log.active_segment().end_offset,
                )
            });

        shard_info!(
            shard_id,
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
        let segment = Segment2::new(
            end_offset + 1,
            config.segment.size,
            config.segment.message_expiry,
        );

        let storage = create_segment_storage(
            &config,
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
        shard_id: u16,
        stream_id: &Identifier,
        topic_id: &Identifier,
        partition_id: usize,
        unsaved_messages_count_exceeded: bool,
        unsaved_messages_size_exceeded: bool,
        journal_messages_count: u32,
        journal_size: u32,
        config: &SystemConfig,
    ) -> Result<(), IggyError> {
        let batches = self.with_partition_by_id_mut(
            stream_id,
            topic_id,
            partition_id,
            streaming_partitions::helpers::commit_journal(),
        );

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
        let (saved, batch_count) = self
            .with_partition_by_id_async(
                stream_id,
                topic_id,
                partition_id,
                streaming_partitions::helpers::persist_batch(
                    shard_id,
                    stream_id,
                    topic_id,
                    partition_id,
                    batches,
                    reason,
                ),
            )
            .await?;

        self.with_partition_by_id_mut(
            stream_id,
            topic_id,
            partition_id,
            streaming_partitions::helpers::update_index_and_increment_stats(
                saved,
                batch_count,
                config,
            ),
        );

        Ok(())
    }
}
