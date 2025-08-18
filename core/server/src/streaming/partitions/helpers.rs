use crate::{
    slab::{
        partitions::Partitions,
        traits_ext::{Components, EntityComponentSystem, IntoComponents},
    },
    streaming::partitions::partition2::PartitionRef,
};

pub fn get_partition_ids() -> impl FnOnce(&Partitions) -> Vec<usize> {
    |partitions| {
        partitions.with_components(|components| {
            let (root, ..) = components.into_components();
            root.iter()
                .map(|(_, partition)| partition.id())
                .collect::<Vec<_>>()
        })
    }
}
