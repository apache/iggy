use crate::{
    configs::{cache_indexes::CacheIndexesConfig, system::SystemConfig},
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
        partitions::partition2::{PartitionRef, PartitionRefMut},
        segments::{IggyMessagesBatchSet, Segment2, storage::create_segment_storage},
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
    },
};
use ahash::AHashMap;
use iggy_common::{Identifier, IggyError};
use slab::Slab;
use std::{cell::RefCell, sync::Arc};

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
        let range = self.with_partition_by_id(
            stream_id,
            topic_id,
            partition_id,
            helpers::get_segment_range_by_timestamp(timestamp),
        );

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

        crate::shard_info!(
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
