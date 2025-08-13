use crate::{
    slab::{partitions::Partitions, topics::Topics, traits_ext::ComponentsById},
    streaming::{
        streams::stream2::{StreamRef, StreamRefMut},
        topics::topic2::{TopicRef, TopicRefMut},
    },
};

// Helpers
pub fn topics<O, F>(f: F) -> impl FnOnce(ComponentsById<StreamRef>) -> O
where
    F: for<'a> FnOnce(&'a Topics) -> O,
{
    |(root, _)| f(root.topics())
}

pub async fn topics_async<O, F>(f: F) -> impl AsyncFnOnce(ComponentsById<StreamRef>) -> O
where
    F: for<'a> AsyncFnOnce(&'a Topics) -> O,
{
    async |(root, _)| f(root.topics()).await
}

pub fn topics_mut<O, F>(f: F) -> impl FnOnce(ComponentsById<StreamRefMut>) -> O
where
    F: for<'a> FnOnce(&'a mut Topics) -> O,
{
    |(mut root, _)| f(root.topics_mut())
}

pub fn partitions<O, F>(f: F) -> impl FnOnce(ComponentsById<TopicRef>) -> O
where
    F: for<'a> FnOnce(&'a Partitions) -> O,
{
    |(root, _)| f(root.partitions())
}

pub async fn partitions_async<O, F>(f: F) -> impl AsyncFnOnce(ComponentsById<TopicRef>) -> O
where
    F: for<'a> AsyncFnOnce(&'a Partitions) -> O,
{
    async |(root, _)| f(root.partitions()).await
}

pub fn partitions_mut<O, F>(f: F) -> impl FnOnce(ComponentsById<TopicRefMut>) -> O
where
    F: for<'a> FnOnce(&'a mut Partitions) -> O,
{
    |(mut root, _)| f(root.partitions_mut())
}
