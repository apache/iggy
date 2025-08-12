use crate::slab::topics;
use crate::slab::traits_ext::{EntityMarker, IndexComponents, IntoComponents};
use crate::state::system::TopicState;
use crate::streaming::stats::stats::{PartitionStats, TopicStats};
use crate::{
    slab::{
        Keyed,
        consumer_groups::ConsumerGroups,
        partitions::{PARTITIONS_CAPACITY, Partitions},
    },
    streaming::partitions::consumer_offset,
};
use iggy_common::{CompressionAlgorithm, IggyExpiry, IggyTimestamp, MaxTopicSize};
use slab::Slab;
use std::cell::Ref;
use std::sync::Arc;

#[derive(Default, Debug)]
pub struct TopicRoot {
    id: usize,
    // TODO: This property should be removed, we won't use it in our clustering impl.
    replication_factor: u8,
    name: String,
    created_at: IggyTimestamp,
    message_expiry: IggyExpiry,
    compression_algorithm: CompressionAlgorithm,
    max_topic_size: MaxTopicSize,

    partitions: Partitions,
    consumer_groups: ConsumerGroups,
}

impl Keyed for TopicRoot {
    type Key = String;

    fn key(&self) -> &Self::Key {
        &self.name
    }
}

#[derive(Debug)]
pub struct Topic {
    root: TopicRoot,
    stats: Arc<TopicStats>,
}

impl Topic {
    pub fn new(
        name: String,
        replication_factor: u8,
        message_expiry: IggyExpiry,
        compression: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
        stats: Arc<TopicStats>,
    ) -> Self {
        let root = TopicRoot::new(
            name,
            replication_factor,
            message_expiry,
            compression,
            max_topic_size,
        );
        Self {
            root,
            stats,
        }
    }
}

impl IntoComponents for Topic{
    type Components = (TopicRoot, Arc<TopicStats>);

    fn into_components(self) -> Self::Components {
        (self.root, self.stats)
    }
}

impl EntityMarker for Topic {}

pub struct TopicRef<'a> {
    root: Ref<'a, Slab<TopicRoot>>,
    stats: Ref<'a, Slab<Arc<TopicStats>>>,
}

impl<'a> TopicRef<'a> {
    pub fn new(root: Ref<'a, Slab<TopicRoot>>, stats: Ref<'a, Slab<Arc<TopicStats>>>) -> Self {
        Self { root, stats }
    }
}

impl<'a> IntoComponents for TopicRef<'a> {
    type Components = (
        Ref<'a, Slab<TopicRoot>>,
        Ref<'a, Slab<Arc<TopicStats>>>,
    );

    fn into_components(self) -> Self::Components {
        (self.root, self.stats)
    }
}


impl<'a> IndexComponents<topics::SlabId> for TopicRef<'a> {
    type Output<'t> = (
        &'t TopicRoot,
        &'t Arc<TopicStats>,
    );

    fn index(&self, index: topics::SlabId) -> Self::Output<'_> {
        (&self.root[index], &self.stats[index])
    }
}

impl TopicRoot {
    pub fn new(
        name: String,
        replication_factor: u8,
        message_expiry: IggyExpiry,
        compression: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
    ) -> Self {
        Self {
            id: 0,
            name,
            created_at: IggyTimestamp::now(),
            replication_factor,
            message_expiry,
            compression_algorithm: compression,
            max_topic_size,
            partitions: Partitions::default(),
            consumer_groups: ConsumerGroups::default(),
        }
    }

    pub fn invoke<T>(&self, f: impl FnOnce(&Self) -> T) -> T {
        f(self)
    }

    pub fn invoke_mut<T>(&mut self, f: impl FnOnce(&mut Self) -> T) -> T {
        f(self)
    }

    pub async fn invoke_async<T>(&self, f: impl AsyncFnOnce(&Self) -> T) -> T {
        f(self).await
    }

    pub fn with_partition_stats_mut<T>(
        &mut self,
        f: impl FnOnce(&mut Slab<Arc<PartitionStats>>) -> T,
    ) -> T {
        self.partitions.with_stats_mut(f)
    }

    pub fn message_expiry(&self) -> IggyExpiry {
        self.message_expiry
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn set_name(&mut self, name: String) {
        self.name = name;
    }

    pub fn set_compression(&mut self, compression: CompressionAlgorithm) {
        self.compression_algorithm = compression;
    }

    pub fn set_message_expiry(&mut self, message_expiry: IggyExpiry) {
        self.message_expiry = message_expiry;
    }

    pub fn set_max_topic_size(&mut self, max_topic_size: MaxTopicSize) {
        self.max_topic_size = max_topic_size;
    }

    pub fn set_replication_factor(&mut self, replication_factor: u8) {
        self.replication_factor = replication_factor;
    }

    pub fn partitions(&self) -> &Partitions {
        &self.partitions
    }

    pub fn partitions_mut(&mut self) -> &mut Partitions {
        &mut self.partitions
    }

    pub fn consumer_groups(&self) -> &ConsumerGroups {
        &self.consumer_groups
    }

    pub fn consumer_groups_mut(&mut self) -> &mut ConsumerGroups {
        &mut self.consumer_groups
    }

    pub fn insert_into(self, container: &mut Slab<Self>) -> usize {
        let idx = container.insert(self);
        let topic = &mut container[idx];
        topic.id = idx;
        idx
    }
}

