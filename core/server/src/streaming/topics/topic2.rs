use crate::slab::{IndexedSlab, Keyed, consumer_groups::ConsumerGroups, partitions::Partitions};
use iggy_common::{CompressionAlgorithm, IggyExpiry, IggyTimestamp, MaxTopicSize};

#[derive(Default, Debug)]
pub struct Topic {
    partitions: Partitions,
    consumer_groups: ConsumerGroups,

    id: usize,
    // TODO: This property should be removed, we won't use it in our clustering impl.
    replication_factor: u8,
    name: String,
    created_at: IggyTimestamp,
    message_expiry: IggyExpiry,
    compression_algorithm: CompressionAlgorithm,
    max_topic_size: MaxTopicSize,
}

impl Topic {
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
            partitions: Partitions::default(),
            consumer_groups: ConsumerGroups::default(),
            replication_factor,
            message_expiry,
            compression_algorithm: compression,
            max_topic_size,
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

    pub fn insert_into(self, container: &mut IndexedSlab<Self>) -> usize {
        let idx = container.insert(self);
        let topic = &mut container[idx];
        topic.id = idx;
        idx
    }
}

impl Keyed for Topic {
    type Key = String;

    fn key(&self) -> &Self::Key {
        &self.name
    }
}
