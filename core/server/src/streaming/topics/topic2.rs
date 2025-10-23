use crate::slab::streams::Streams;
use crate::slab::topics;
use crate::slab::traits_ext::{EntityMarker, InsertCell, IntoComponents, IntoComponentsById};
use crate::slab::{Keyed, consumer_groups::ConsumerGroups, partitions::Partitions};
use crate::streaming::stats::{StreamStats, TopicStats};
use iggy_common::{CompressionAlgorithm, Identifier, IggyExpiry, IggyTimestamp, MaxTopicSize};
use slab::Slab;
use std::cell::{Ref, RefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Debug, Clone)]
pub struct TopicAuxilary {
    current_partition_id: Arc<AtomicUsize>,
}

impl TopicAuxilary {
    pub fn get_next_partition_id(&self, upperbound: usize) -> usize {
        let mut partition_id = self.current_partition_id.fetch_add(1, Ordering::AcqRel);
        if partition_id >= upperbound {
            partition_id = 0;
            self.current_partition_id
                .swap(partition_id + 1, Ordering::Release);
        }
        tracing::trace!("Next partition ID: {}", partition_id);
        partition_id
    }
}

#[derive(Default, Debug, Clone)]
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

#[derive(Debug, Clone)]
pub struct Topic {
    root: TopicRoot,
    auxilary: TopicAuxilary,
    stats: Arc<TopicStats>,
}

impl Topic {
    pub fn new(
        name: String,
        stats: Arc<TopicStats>,
        created_at: IggyTimestamp,
        replication_factor: u8,
        message_expiry: IggyExpiry,
        compression: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
    ) -> Self {
        let root = TopicRoot::new(
            name,
            created_at,
            replication_factor,
            message_expiry,
            compression,
            max_topic_size,
        );
        let auxilary = TopicAuxilary {
            current_partition_id: Arc::new(AtomicUsize::new(0)),
        };
        Self {
            root,
            auxilary,
            stats,
        }
    }
    pub fn new_with_components(
        root: TopicRoot,
        auxilary: TopicAuxilary,
        stats: Arc<TopicStats>,
    ) -> Self {
        Self {
            root,
            auxilary,
            stats,
        }
    }

    pub fn root(&self) -> &TopicRoot {
        &self.root
    }

    pub fn root_mut(&mut self) -> &mut TopicRoot {
        &mut self.root
    }

    pub fn stats(&self) -> &TopicStats {
        &self.stats
    }
}

impl IntoComponents for Topic {
    type Components = (TopicRoot, TopicAuxilary, Arc<TopicStats>);

    fn into_components(self) -> Self::Components {
        (self.root, self.auxilary, self.stats)
    }
}

impl EntityMarker for Topic {
    type Idx = topics::ContainerId;
    fn id(&self) -> Self::Idx {
        self.root.id
    }

    fn update_id(&mut self, id: Self::Idx) {
        self.root.id = id;
    }
}

// TODO: Create a macro to impl those TopicRef/TopicRefMut structs and it's traits.
pub struct TopicRef<'a> {
    root: Ref<'a, Slab<TopicRoot>>,
    auxilary: Ref<'a, Slab<TopicAuxilary>>,
    stats: Ref<'a, Slab<Arc<TopicStats>>>,
}

impl<'a> TopicRef<'a> {
    pub fn new(
        root: Ref<'a, Slab<TopicRoot>>,
        auxilary: Ref<'a, Slab<TopicAuxilary>>,
        stats: Ref<'a, Slab<Arc<TopicStats>>>,
    ) -> Self {
        Self {
            root,
            auxilary,
            stats,
        }
    }
}

impl<'a> IntoComponents for TopicRef<'a> {
    type Components = (
        Ref<'a, Slab<TopicRoot>>,
        Ref<'a, Slab<TopicAuxilary>>,
        Ref<'a, Slab<Arc<TopicStats>>>,
    );

    fn into_components(self) -> Self::Components {
        (self.root, self.auxilary, self.stats)
    }
}

impl<'a> IntoComponentsById for TopicRef<'a> {
    type Idx = topics::ContainerId;
    type Output = (
        Ref<'a, TopicRoot>,
        Ref<'a, TopicAuxilary>,
        Ref<'a, Arc<TopicStats>>,
    );

    fn into_components_by_id(self, index: Self::Idx) -> Self::Output {
        let root = Ref::map(self.root, |r| &r[index]);
        let auxilary = Ref::map(self.auxilary, |a| &a[index]);
        let stats = Ref::map(self.stats, |s| &s[index]);
        (root, auxilary, stats)
    }
}

pub struct TopicRefMut<'a> {
    root: RefMut<'a, Slab<TopicRoot>>,
    auxilary: RefMut<'a, Slab<TopicAuxilary>>,
    stats: RefMut<'a, Slab<Arc<TopicStats>>>,
}

impl<'a> TopicRefMut<'a> {
    pub fn new(
        root: RefMut<'a, Slab<TopicRoot>>,
        auxilary: RefMut<'a, Slab<TopicAuxilary>>,
        stats: RefMut<'a, Slab<Arc<TopicStats>>>,
    ) -> Self {
        Self {
            root,
            auxilary,
            stats,
        }
    }
}

impl<'a> IntoComponents for TopicRefMut<'a> {
    type Components = (
        RefMut<'a, Slab<TopicRoot>>,
        RefMut<'a, Slab<TopicAuxilary>>,
        RefMut<'a, Slab<Arc<TopicStats>>>,
    );

    fn into_components(self) -> Self::Components {
        (self.root, self.auxilary, self.stats)
    }
}

impl<'a> IntoComponentsById for TopicRefMut<'a> {
    type Idx = topics::ContainerId;
    type Output = (
        RefMut<'a, TopicRoot>,
        RefMut<'a, TopicAuxilary>,
        RefMut<'a, Arc<TopicStats>>,
    );

    fn into_components_by_id(self, index: Self::Idx) -> Self::Output {
        let root = RefMut::map(self.root, |r| &mut r[index]);
        let auxilary = RefMut::map(self.auxilary, |a| &mut a[index]);
        let stats = RefMut::map(self.stats, |s| &mut s[index]);
        (root, auxilary, stats)
    }
}

impl TopicRoot {
    pub fn new(
        name: String,
        created_at: IggyTimestamp,
        replication_factor: u8,
        message_expiry: IggyExpiry,
        compression: CompressionAlgorithm,
        max_topic_size: MaxTopicSize,
    ) -> Self {
        Self {
            id: 0,
            name,
            created_at,
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

    pub fn update_id(&mut self, id: usize) {
        self.id = id;
    }

    pub fn id(&self) -> topics::ContainerId {
        self.id
    }

    pub fn message_expiry(&self) -> IggyExpiry {
        self.message_expiry
    }

    pub fn max_topic_size(&self) -> MaxTopicSize {
        self.max_topic_size
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

    pub fn created_at(&self) -> IggyTimestamp {
        self.created_at
    }

    pub fn compression_algorithm(&self) -> CompressionAlgorithm {
        self.compression_algorithm
    }

    pub fn replication_factor(&self) -> u8 {
        self.replication_factor
    }
}

// TODO: Move to separate module.
#[allow(clippy::too_many_arguments)]
pub fn create_and_insert_topics_mem(
    streams: &Streams,
    stream_id: &Identifier,
    name: String,
    replication_factor: u8,
    message_expiry: IggyExpiry,
    compression: CompressionAlgorithm,
    max_topic_size: MaxTopicSize,
    parent_stats: Arc<StreamStats>,
) -> Topic {
    let stats = Arc::new(TopicStats::new(parent_stats));
    let now = IggyTimestamp::now();
    let mut topic = Topic::new(
        name,
        stats,
        now,
        replication_factor,
        message_expiry,
        compression,
        max_topic_size,
    );

    let id = streams.with_topics(stream_id, |topics| topics.insert(topic.clone()));
    topic.update_id(id);
    topic
}
