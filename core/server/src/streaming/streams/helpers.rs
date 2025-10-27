use crate::{
    configs::system::SystemConfig,
    slab::{
        streams,
        traits_ext::{ComponentsById, EntityComponentSystem, IntoComponents},
    },
    streaming::{
        partitions,
        streams::stream::{StreamRef, StreamRefMut},
    },
};
use iggy_common::Identifier;

pub fn get_stream_id() -> impl FnOnce(ComponentsById<StreamRef>) -> streams::ContainerId {
    |(root, _)| root.id()
}

pub fn get_stream_name() -> impl FnOnce(ComponentsById<StreamRef>) -> String {
    |(root, _)| root.name().clone()
}

pub fn update_stream_name(name: String) -> impl FnOnce(ComponentsById<StreamRefMut>) {
    move |(mut root, _)| {
        root.set_name(name);
    }
}

pub fn get_topic_ids() -> impl FnOnce(ComponentsById<StreamRef>) -> Vec<usize> {
    |(root, _)| {
        root.topics().with_components(|components| {
            let (topic_roots, ..) = components.into_components();
            topic_roots
                .iter()
                .map(|(_, topic)| topic.id())
                .collect::<Vec<_>>()
        })
    }
}

pub fn store_consumer_offset(
    consumer_id: usize,
    topic_id: &Identifier,
    partition_id: usize,
    offset: u64,
    config: &SystemConfig,
) -> impl FnOnce(ComponentsById<StreamRef>) {
    move |(root, ..)| {
        let stream_id = root.id();
        root.topics().with_topic_by_id(topic_id, |(root, ..)| {
            let topic_id = root.id();
            root.partitions().with_components_by_id(
                partition_id,
                partitions::helpers::store_consumer_offset(
                    consumer_id,
                    stream_id,
                    topic_id,
                    partition_id,
                    offset,
                    config,
                ),
            )
        })
    }
}

pub fn store_consumer_group_member_offset(
    member_id: usize,
    topic_id: &Identifier,
    partition_id: usize,
    offset: u64,
    config: &SystemConfig,
) -> impl FnOnce(ComponentsById<StreamRef>) {
    move |(root, ..)| {
        let stream_id = root.id();
        root.topics().with_topic_by_id(topic_id, |(root, ..)| {
            let topic_id = root.id();
            root.partitions().with_components_by_id(
                partition_id,
                partitions::helpers::store_consumer_group_member_offset(
                    member_id,
                    stream_id,
                    topic_id,
                    partition_id,
                    offset,
                    config,
                ),
            )
        })
    }
}
