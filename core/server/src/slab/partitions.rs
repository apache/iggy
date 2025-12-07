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

use crate::{
    slab::traits_ext::{
        Borrow, ComponentsById, Delete, EntityComponentSystem, EntityComponentSystemMut, Insert,
        IntoComponents,
    },
    streaming::{
        deduplication::message_deduplicator::MessageDeduplicator,
        partitions::{
            journal::MemoryMessageJournal,
            log::SegmentedLog,
            partition::{
                self, ConsumerGroupOffsets, ConsumerOffsets, Partition, PartitionRef,
                PartitionRefMut,
            },
        },
        stats::PartitionStats,
    },
};
use slab::Slab;
use std::sync::{Arc, atomic::AtomicU64};

// TODO: This could be upper limit of partitions per topic, use that value to validate instead of whathever this thing is in `common` crate.
pub const PARTITIONS_CAPACITY: usize = 16384;
pub type ContainerId = usize;

#[derive(Debug)]
pub struct Partitions {
    root: Slab<partition::PartitionRoot>,
    stats: Slab<Arc<PartitionStats>>,
    message_deduplicator: Slab<Option<Arc<MessageDeduplicator>>>,
    offset: Slab<Arc<AtomicU64>>,

    consumer_offset: Slab<Arc<ConsumerOffsets>>,
    consumer_group_offset: Slab<Arc<ConsumerGroupOffsets>>,

    log: Slab<SegmentedLog<MemoryMessageJournal>>,
}

/// Clone implementation for partitions, does not copy the actual logs.
/// Since those are very expensive to clone and we use `Clone` only during initialization
/// in order to streamline broadcasting entity creation event to other shards.
/// A better strategy would be to have an `Pool` of Streams/Topics/Partitions and during event broadcast, grab a new `Default` instance from the pool aka ZII.
impl Clone for Partitions {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            stats: self.stats.clone(),
            message_deduplicator: self.message_deduplicator.clone(),
            offset: self.offset.clone(),
            consumer_offset: self.consumer_offset.clone(),
            consumer_group_offset: self.consumer_group_offset.clone(),
            log: Slab::with_capacity(PARTITIONS_CAPACITY), // Empty log, we don't clone the actual logs.
        }
    }
}

impl Insert for Partitions {
    type Idx = ContainerId;
    type Item = Partition;

    fn insert(&mut self, item: Self::Item) -> Self::Idx {
        let (root, stats, deduplicator, offset, consumer_offset, consumer_group_offset, log) =
            item.into_components();

        let entity_id = self.root.insert(root);
        let id = self.stats.insert(stats);
        assert_eq!(
            entity_id, id,
            "partition_insert: id mismatch when creating stats"
        );
        let id = self.log.insert(log);
        assert_eq!(
            entity_id, id,
            "partition_insert: id mismatch when creating log"
        );
        let id = self.message_deduplicator.insert(deduplicator);
        assert_eq!(
            entity_id, id,
            "partition_insert: id mismatch when creating message_deduplicator"
        );
        let id = self.offset.insert(offset);
        assert_eq!(
            entity_id, id,
            "partition_insert: id mismatch when creating offset"
        );
        let id = self.consumer_offset.insert(consumer_offset);
        assert_eq!(
            entity_id, id,
            "partition_insert: id mismatch when creating consumer_offset"
        );
        let id = self.consumer_group_offset.insert(consumer_group_offset);
        assert_eq!(
            entity_id, id,
            "partition_insert: id mismatch when creating consumer_group_offset"
        );
        let root = self.root.get_mut(entity_id).unwrap();
        root.update_id(entity_id);
        entity_id
    }
}

impl Delete for Partitions {
    type Idx = ContainerId;
    type Item = Partition;

    fn delete(&mut self, id: Self::Idx) -> Self::Item {
        let root = self.root.remove(id);
        let stats = self.stats.remove(id);
        let message_deduplicator = self.message_deduplicator.remove(id);
        let offset = self.offset.remove(id);
        let consumer_offset = self.consumer_offset.remove(id);
        let consumer_group_offset = self.consumer_group_offset.remove(id);
        let log = self.log.remove(id);

        Partition::new_with_components(
            root,
            stats,
            message_deduplicator,
            offset,
            consumer_offset,
            consumer_group_offset,
            log,
        )
    }
}

//TODO: those from impls could use a macro aswell.
impl<'a> From<&'a Partitions> for PartitionRef<'a> {
    fn from(value: &'a Partitions) -> Self {
        PartitionRef::new(
            &value.root,
            &value.stats,
            &value.message_deduplicator,
            &value.offset,
            &value.consumer_offset,
            &value.consumer_group_offset,
            &value.log,
        )
    }
}

impl<'a> From<&'a mut Partitions> for PartitionRefMut<'a> {
    fn from(value: &'a mut Partitions) -> Self {
        PartitionRefMut::new(
            &mut value.root,
            &mut value.stats,
            &mut value.message_deduplicator,
            &mut value.offset,
            &mut value.consumer_offset,
            &mut value.consumer_group_offset,
            &mut value.log,
        )
    }
}

impl EntityComponentSystem<Borrow> for Partitions {
    type Idx = ContainerId;
    type Entity = Partition;
    type EntityComponents<'a> = PartitionRef<'a>;

    fn with_components<O, F>(&self, f: F) -> O
    where
        F: for<'a> FnOnce(Self::EntityComponents<'a>) -> O,
    {
        f(self.into())
    }
}

impl EntityComponentSystemMut for Partitions {
    type EntityComponentsMut<'a> = PartitionRefMut<'a>;

    fn with_components_mut<O, F>(&mut self, f: F) -> O
    where
        F: for<'a> FnOnce(Self::EntityComponentsMut<'a>) -> O,
    {
        f(self.into())
    }
}

impl Default for Partitions {
    fn default() -> Self {
        Self {
            root: Slab::with_capacity(PARTITIONS_CAPACITY),
            stats: Slab::with_capacity(PARTITIONS_CAPACITY),
            log: Slab::with_capacity(PARTITIONS_CAPACITY),
            message_deduplicator: Slab::with_capacity(PARTITIONS_CAPACITY),
            offset: Slab::with_capacity(PARTITIONS_CAPACITY),
            consumer_offset: Slab::with_capacity(PARTITIONS_CAPACITY),
            consumer_group_offset: Slab::with_capacity(PARTITIONS_CAPACITY),
        }
    }
}

impl Partitions {
    pub fn len(&self) -> usize {
        self.root.len()
    }

    pub fn is_empty(&self) -> bool {
        self.root.is_empty()
    }

    pub fn insert_default_log(&mut self) -> ContainerId {
        self.log.insert(Default::default())
    }

    pub fn with_partition_by_id<T>(
        &self,
        id: ContainerId,
        f: impl FnOnce(ComponentsById<PartitionRef>) -> T,
    ) -> T {
        self.with_components_by_id(id, |components| f(components))
    }

    pub fn exists(&self, id: ContainerId) -> bool {
        self.root.contains(id)
    }

    pub fn with_partition_by_id_mut<T>(
        &mut self,
        id: ContainerId,
        f: impl FnOnce(ComponentsById<PartitionRefMut>) -> T,
    ) -> T {
        self.with_components_by_id_mut(id, |components| f(components))
    }
}
