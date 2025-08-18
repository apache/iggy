use crate::configs::system::SystemConfig;
use crate::slab::topics;
use crate::slab::traits_ext::{EntityMarker, IntoComponents, IntoComponentsById};
use crate::slab::{Keyed, consumer_groups::ConsumerGroups, partitions::Partitions};
use crate::streaming::stats::stats::TopicStats;
use iggy_common::{CompressionAlgorithm, IggyError, IggyExpiry, IggyTimestamp, MaxTopicSize};
use slab::Slab;
use std::cell::{Ref, RefMut};
use std::sync::Arc;

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
        Self { root, stats }
    }

    pub fn root(&self) -> &TopicRoot {
        &self.root
    }
}

impl IntoComponents for Topic {
    type Components = (TopicRoot, Arc<TopicStats>);

    fn into_components(self) -> Self::Components {
        (self.root, self.stats)
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
    stats: Ref<'a, Slab<Arc<TopicStats>>>,
}

impl<'a> TopicRef<'a> {
    pub fn new(root: Ref<'a, Slab<TopicRoot>>, stats: Ref<'a, Slab<Arc<TopicStats>>>) -> Self {
        Self { root, stats }
    }
}

impl<'a> IntoComponents for TopicRef<'a> {
    type Components = (Ref<'a, Slab<TopicRoot>>, Ref<'a, Slab<Arc<TopicStats>>>);

    fn into_components(self) -> Self::Components {
        (self.root, self.stats)
    }
}

impl<'a> IntoComponentsById for TopicRef<'a> {
    type Idx = topics::ContainerId;
    type Output = (Ref<'a, TopicRoot>, Ref<'a, Arc<TopicStats>>);

    fn into_components_by_id(self, index: Self::Idx) -> Self::Output {
        let root = Ref::map(self.root, |r| &r[index]);
        let stats = Ref::map(self.stats, |s| &s[index]);
        (root, stats)
    }
}

pub struct TopicRefMut<'a> {
    root: RefMut<'a, Slab<TopicRoot>>,
    stats: RefMut<'a, Slab<Arc<TopicStats>>>,
}

impl<'a> TopicRefMut<'a> {
    pub fn new(
        root: RefMut<'a, Slab<TopicRoot>>,
        stats: RefMut<'a, Slab<Arc<TopicStats>>>,
    ) -> Self {
        Self { root, stats }
    }
}

impl<'a> IntoComponents for TopicRefMut<'a> {
    type Components = (
        RefMut<'a, Slab<TopicRoot>>,
        RefMut<'a, Slab<Arc<TopicStats>>>,
    );

    fn into_components(self) -> Self::Components {
        (self.root, self.stats)
    }
}

impl<'a> IntoComponentsById for TopicRefMut<'a> {
    type Idx = topics::ContainerId;
    type Output = (RefMut<'a, TopicRoot>, RefMut<'a, Arc<TopicStats>>);

    fn into_components_by_id(self, index: Self::Idx) -> Self::Output {
        let root = RefMut::map(self.root, |r| &mut r[index]);
        let stats = RefMut::map(self.stats, |s| &mut s[index]);
        (root, stats)
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

    pub async fn invoke_async<T>(&self, f: impl AsyncFnOnce(&Self) -> T) -> T {
        f(self).await
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

pub fn resolve_max_topic_size(
    max_topic_size: MaxTopicSize,
    config: &SystemConfig,
) -> Result<MaxTopicSize, IggyError> {
    match max_topic_size {
        MaxTopicSize::ServerDefault => Ok(config.topic.max_size),
        _ => {
            if max_topic_size.as_bytes_u64() < config.segment.size.as_bytes_u64() {
                Err(IggyError::InvalidTopicSize(
                    max_topic_size,
                    config.segment.size,
                ))
            } else {
                Ok(max_topic_size)
            }
        }
    }
}

pub fn resolve_message_expiry(message_expiry: IggyExpiry, config: &SystemConfig) -> IggyExpiry {
    match message_expiry {
        IggyExpiry::ServerDefault => config.segment.message_expiry,
        _ => message_expiry,
    }
}
