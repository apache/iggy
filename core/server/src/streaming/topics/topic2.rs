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

    pub fn with(&self, f: impl FnOnce(&Self)) {
        f(self);
    }

    pub fn with_mut(&mut self, f: impl FnOnce(&mut Self)) {
        f(self);
    }

    pub fn partitions(&self) -> &Partitions {
        &self.partitions
    }

    pub fn partitions_mut(&mut self) -> &mut Partitions {
        &mut self.partitions
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
