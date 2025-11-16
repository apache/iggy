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
    configs::system::SystemConfig,
    slab::{
        partitions,
        streams::Streams,
        traits_ext::{EntityMarker, IntoComponents, IntoComponentsById},
    },
    streaming::{
        self,
        deduplication::message_deduplicator::MessageDeduplicator,
        partitions::{
            consumer_offset, helpers::create_message_deduplicator, journal::MemoryMessageJournal,
            log::SegmentedLog,
        },
        polling_consumer::ConsumerGroupId,
        stats::{PartitionStats, TopicStats},
    },
};
use iggy_common::{Identifier, IggyTimestamp};
use slab::Slab;
use std::sync::{Arc, atomic::AtomicU64};

#[derive(Debug, Clone)]
pub struct ConsumerOffsets(papaya::HashMap<usize, consumer_offset::ConsumerOffset>);

impl ConsumerOffsets {
    pub fn with_capacity(capacity: usize) -> Self {
        Self(papaya::HashMap::with_capacity(capacity))
    }
}

impl<I> From<I> for ConsumerOffsets
where
    I: IntoIterator<Item = (usize, consumer_offset::ConsumerOffset)>,
{
    fn from(iter: I) -> Self {
        Self(papaya::HashMap::from_iter(iter))
    }
}

impl std::ops::Deref for ConsumerOffsets {
    type Target = papaya::HashMap<usize, consumer_offset::ConsumerOffset>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for ConsumerOffsets {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug, Clone)]
pub struct ConsumerGroupOffsets(papaya::HashMap<ConsumerGroupId, consumer_offset::ConsumerOffset>);

impl ConsumerGroupOffsets {
    pub fn with_capacity(capacity: usize) -> Self {
        Self(papaya::HashMap::with_capacity(capacity))
    }
}

impl<I> From<I> for ConsumerGroupOffsets
where
    I: IntoIterator<Item = (ConsumerGroupId, consumer_offset::ConsumerOffset)>,
{
    fn from(iter: I) -> Self {
        Self(papaya::HashMap::from_iter(iter))
    }
}

impl std::ops::Deref for ConsumerGroupOffsets {
    type Target = papaya::HashMap<ConsumerGroupId, consumer_offset::ConsumerOffset>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for ConsumerGroupOffsets {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[derive(Debug)]
pub struct Partition {
    root: PartitionRoot,
    stats: Arc<PartitionStats>,
    message_deduplicator: Option<Arc<MessageDeduplicator>>,
    offset: Arc<AtomicU64>,
    consumer_offset: Arc<ConsumerOffsets>,
    consumer_group_offset: Arc<ConsumerGroupOffsets>,
    log: SegmentedLog<MemoryMessageJournal>,
}

impl Partition {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        created_at: IggyTimestamp,
        should_increment_offset: bool,
        stats: Arc<PartitionStats>,
        message_deduplicator: Option<MessageDeduplicator>,
        offset: Arc<AtomicU64>,
        consumer_offset: Arc<ConsumerOffsets>,
        consumer_group_offset: Arc<ConsumerGroupOffsets>,
        log: SegmentedLog<MemoryMessageJournal>,
    ) -> Self {
        let root = PartitionRoot::new(created_at, should_increment_offset);
        let message_deduplicator = message_deduplicator.map(Arc::new);
        Self {
            root,
            stats,
            message_deduplicator,
            offset,
            consumer_offset,
            consumer_group_offset,
            log,
        }
    }

    pub fn new_with_components(
        root: PartitionRoot,
        stats: Arc<PartitionStats>,
        message_deduplicator: Option<Arc<MessageDeduplicator>>,
        offset: Arc<AtomicU64>,
        consumer_offset: Arc<ConsumerOffsets>,
        consumer_group_offset: Arc<ConsumerGroupOffsets>,
        log: SegmentedLog<MemoryMessageJournal>,
    ) -> Self {
        Self {
            root,
            stats,
            message_deduplicator,
            offset,
            consumer_offset,
            consumer_group_offset,
            log,
        }
    }

    pub fn stats(&self) -> &PartitionStats {
        &self.stats
    }
}

impl Clone for Partition {
    fn clone(&self) -> Self {
        Self {
            root: self.root.clone(),
            stats: Arc::clone(&self.stats),
            message_deduplicator: self.message_deduplicator.clone(),
            offset: Arc::clone(&self.offset),
            consumer_offset: Arc::clone(&self.consumer_offset),
            consumer_group_offset: Arc::clone(&self.consumer_group_offset),
            log: Default::default(),
        }
    }
}

impl EntityMarker for Partition {
    type Idx = partitions::ContainerId;

    fn id(&self) -> Self::Idx {
        self.root.id
    }

    fn update_id(&mut self, id: Self::Idx) {
        self.root.id = id;
    }
}

impl IntoComponents for Partition {
    type Components = (
        PartitionRoot,
        Arc<PartitionStats>,
        Option<Arc<MessageDeduplicator>>,
        Arc<AtomicU64>,
        Arc<ConsumerOffsets>,
        Arc<ConsumerGroupOffsets>,
        SegmentedLog<MemoryMessageJournal>,
    );

    fn into_components(self) -> Self::Components {
        (
            self.root,
            self.stats,
            self.message_deduplicator,
            self.offset,
            self.consumer_offset,
            self.consumer_group_offset,
            self.log,
        )
    }
}

#[derive(Default, Debug, Clone)]
pub struct PartitionRoot {
    id: usize,
    created_at: IggyTimestamp,
    should_increment_offset: bool,
}

impl PartitionRoot {
    pub fn new(created_at: IggyTimestamp, should_increment_offset: bool) -> Self {
        Self {
            id: 0,
            created_at,
            should_increment_offset,
        }
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn update_id(&mut self, id: usize) {
        self.id = id;
    }

    pub fn created_at(&self) -> IggyTimestamp {
        self.created_at
    }

    pub fn should_increment_offset(&self) -> bool {
        self.should_increment_offset
    }

    pub fn set_should_increment_offset(&mut self, value: bool) {
        self.should_increment_offset = value;
    }
}

pub struct PartitionRef<'a> {
    root: &'a Slab<PartitionRoot>,
    stats: &'a Slab<Arc<PartitionStats>>,
    message_deduplicator: &'a Slab<Option<Arc<MessageDeduplicator>>>,
    offset: &'a Slab<Arc<AtomicU64>>,
    consumer_offset: &'a Slab<Arc<ConsumerOffsets>>,
    consumer_group_offset: &'a Slab<Arc<ConsumerGroupOffsets>>,
    log: &'a Slab<SegmentedLog<MemoryMessageJournal>>,
}

impl<'a> PartitionRef<'a> {
    pub fn new(
        root: &'a Slab<PartitionRoot>,
        stats: &'a Slab<Arc<PartitionStats>>,
        message_deduplicator: &'a Slab<Option<Arc<MessageDeduplicator>>>,
        offset: &'a Slab<Arc<AtomicU64>>,
        consumer_offset: &'a Slab<Arc<ConsumerOffsets>>,
        consumer_group_offset: &'a Slab<Arc<ConsumerGroupOffsets>>,
        log: &'a Slab<SegmentedLog<MemoryMessageJournal>>,
    ) -> Self {
        Self {
            root,
            stats,
            message_deduplicator,
            offset,
            consumer_offset,
            consumer_group_offset,
            log,
        }
    }
}

impl<'a> IntoComponents for PartitionRef<'a> {
    type Components = (
        &'a Slab<PartitionRoot>,
        &'a Slab<Arc<PartitionStats>>,
        &'a Slab<Option<Arc<MessageDeduplicator>>>,
        &'a Slab<Arc<AtomicU64>>,
        &'a Slab<Arc<ConsumerOffsets>>,
        &'a Slab<Arc<ConsumerGroupOffsets>>,
        &'a Slab<SegmentedLog<MemoryMessageJournal>>,
    );

    fn into_components(self) -> Self::Components {
        (
            self.root,
            self.stats,
            self.message_deduplicator,
            self.offset,
            self.consumer_offset,
            self.consumer_group_offset,
            self.log,
        )
    }
}

impl<'a> IntoComponentsById for PartitionRef<'a> {
    type Idx = partitions::ContainerId;
    type Output = (
        &'a PartitionRoot,
        &'a Arc<PartitionStats>,
        &'a Option<Arc<MessageDeduplicator>>,
        &'a Arc<AtomicU64>,
        &'a Arc<ConsumerOffsets>,
        &'a Arc<ConsumerGroupOffsets>,
        &'a SegmentedLog<MemoryMessageJournal>,
    );

    fn into_components_by_id(self, index: Self::Idx) -> Self::Output {
        (
            &self.root[index],
            &self.stats[index],
            &self.message_deduplicator[index],
            &self.offset[index],
            &self.consumer_offset[index],
            &self.consumer_group_offset[index],
            &self.log[index],
        )
    }
}

pub struct PartitionRefMut<'a> {
    root: &'a mut Slab<PartitionRoot>,
    stats: &'a mut Slab<Arc<PartitionStats>>,
    message_deduplicator: &'a mut Slab<Option<Arc<MessageDeduplicator>>>,
    offset: &'a mut Slab<Arc<AtomicU64>>,
    consumer_offset: &'a mut Slab<Arc<ConsumerOffsets>>,
    consumer_group_offset: &'a mut Slab<Arc<ConsumerGroupOffsets>>,
    log: &'a mut Slab<SegmentedLog<MemoryMessageJournal>>,
}

impl<'a> PartitionRefMut<'a> {
    pub fn new(
        root: &'a mut Slab<PartitionRoot>,
        stats: &'a mut Slab<Arc<PartitionStats>>,
        message_deduplicator: &'a mut Slab<Option<Arc<MessageDeduplicator>>>,
        offset: &'a mut Slab<Arc<AtomicU64>>,
        consumer_offset: &'a mut Slab<Arc<ConsumerOffsets>>,
        consumer_group_offset: &'a mut Slab<Arc<ConsumerGroupOffsets>>,
        log: &'a mut Slab<SegmentedLog<MemoryMessageJournal>>,
    ) -> Self {
        Self {
            root,
            stats,
            message_deduplicator,
            offset,
            consumer_offset,
            consumer_group_offset,
            log,
        }
    }
}

impl<'a> IntoComponents for PartitionRefMut<'a> {
    type Components = (
        &'a mut Slab<PartitionRoot>,
        &'a mut Slab<Arc<PartitionStats>>,
        &'a mut Slab<Option<Arc<MessageDeduplicator>>>,
        &'a mut Slab<Arc<AtomicU64>>,
        &'a mut Slab<Arc<ConsumerOffsets>>,
        &'a mut Slab<Arc<ConsumerGroupOffsets>>,
        &'a mut Slab<SegmentedLog<MemoryMessageJournal>>,
    );

    fn into_components(self) -> Self::Components {
        (
            self.root,
            self.stats,
            self.message_deduplicator,
            self.offset,
            self.consumer_offset,
            self.consumer_group_offset,
            self.log,
        )
    }
}

impl<'a> IntoComponentsById for PartitionRefMut<'a> {
    type Idx = partitions::ContainerId;
    type Output = (
        &'a mut PartitionRoot,
        &'a mut Arc<PartitionStats>,
        &'a mut Option<Arc<MessageDeduplicator>>,
        &'a mut Arc<AtomicU64>,
        &'a mut Arc<ConsumerOffsets>,
        &'a mut Arc<ConsumerGroupOffsets>,
        &'a mut SegmentedLog<MemoryMessageJournal>,
    );

    fn into_components_by_id(self, index: Self::Idx) -> Self::Output {
        (
            &mut self.root[index],
            &mut self.stats[index],
            &mut self.message_deduplicator[index],
            &mut self.offset[index],
            &mut self.consumer_offset[index],
            &mut self.consumer_group_offset[index],
            &mut self.log[index],
        )
    }
}

pub fn create_and_insert_partitions_mem(
    streams: &Streams,
    stream_id: &Identifier,
    topic_id: &Identifier,
    parent_stats: Arc<TopicStats>,
    partitions_count: u32,
    config: &SystemConfig,
) -> Vec<Partition> {
    let range = 0..partitions_count as usize;
    let created_at = IggyTimestamp::now();
    range
        .map(|_| {
            // Areczkuuuu.
            let stats = Arc::new(PartitionStats::new(parent_stats.clone()));
            let should_increment_offset = false;
            let deduplicator = create_message_deduplicator(config);
            let offset = Arc::new(AtomicU64::new(0));
            let consumer_offset = Arc::new(ConsumerOffsets::with_capacity(2137));
            let consumer_group_offset = Arc::new(ConsumerGroupOffsets::with_capacity(2137));
            let log = Default::default();

            let mut partition = Partition::new(
                created_at,
                should_increment_offset,
                stats,
                deduplicator,
                offset,
                consumer_offset,
                consumer_group_offset,
                log,
            );
            let id = streams.with_partitions_mut(
                stream_id,
                topic_id,
                streaming::partitions::helpers::insert_partition(partition.clone()),
            );
            partition.update_id(id);
            partition
        })
        .collect()
}
