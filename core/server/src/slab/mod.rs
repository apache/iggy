pub mod consumer_groups;
/// General rules how to implement `with_*` methods on any slab"
/// 1. When implementing method that accepts closure f, make sure that the caller can supply closure with only 1 depth.
/// for example, observe following code snippet:
/// ```rust
/// let topic_id = self.streams2.with_topic_by_id(stream_id, topic_id, get_topic_id());
/// ```
/// if we would not provide a `with_topic_by_id` method and purely relied only ony `with_topics`, we would have to write:
/// ```rust
/// let topic_id = self.streams2.with_topics(stream_id, get_topic_by_id(topic_id, get_topic_id())); // `get_topic_id` is a closure that retrieves the topic id.
/// ```
/// we need to supply a nested closure to `get_topic_by_id`.
pub mod helpers;
pub mod partitions;
pub mod streams;
pub mod topics;
pub mod traits_ext;

use std::fmt::{Debug, Display};

pub trait Keyed {
    type Key: Eq + std::hash::Hash + Clone + Debug + Display;
    fn key(&self) -> &Self::Key;
}
