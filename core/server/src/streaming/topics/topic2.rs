use crate::slab::{Keyed, consumer_groups::ConsumerGroups, partitions::Partitions};

#[derive(Default)]
pub struct Topic {
    name: String,
    partitions: Partitions,
    consumer_groups: ConsumerGroups,
}

impl Topic {
    pub fn partitions(&self) -> &Partitions {
        &self.partitions
    }
}

impl Keyed for Topic {
    type Key = String;

    fn key(&self) -> &Self::Key {
        &self.name
    }
}
