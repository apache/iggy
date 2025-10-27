use crate::{
    slab::{
        consumer_groups::ConsumerGroups, partitions::Partitions, topics::Topics,
        traits_ext::ComponentsById,
    },
    streaming::{
        streams::stream::StreamRef,
        topics::topic::{TopicRef, TopicRefMut},
    },
};

// Helpers
pub fn topics<O, F>(f: F) -> impl FnOnce(ComponentsById<StreamRef>) -> O
where
    F: for<'a> FnOnce(&'a Topics) -> O,
{
    |(root, ..)| f(root.topics())
}

pub fn topics_mut<O, F>(f: F) -> impl FnOnce(ComponentsById<StreamRef>) -> O
where
    F: for<'a> FnOnce(&'a Topics) -> O,
{
    |(root, ..)| f(root.topics())
}

pub fn partitions<O, F>(f: F) -> impl FnOnce(ComponentsById<TopicRef>) -> O
where
    F: for<'a> FnOnce(&'a Partitions) -> O,
{
    |(root, ..)| f(root.partitions())
}

pub fn partitions_mut<O, F>(f: F) -> impl FnOnce(ComponentsById<TopicRefMut>) -> O
where
    F: for<'a> FnOnce(&'a mut Partitions) -> O,
{
    |(mut root, ..)| f(root.partitions_mut())
}

pub fn consumer_groups<O, F>(f: F) -> impl FnOnce(ComponentsById<TopicRef>) -> O
where
    F: for<'a> FnOnce(&'a ConsumerGroups) -> O,
{
    |(root, ..)| f(root.consumer_groups())
}

pub fn consumer_groups_mut<O, F>(f: F) -> impl FnOnce(ComponentsById<TopicRefMut>) -> O
where
    F: for<'a> FnOnce(&'a mut ConsumerGroups) -> O,
{
    |(mut root, ..)| f(root.consumer_groups_mut())
}
