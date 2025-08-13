use crate::{
    slab::{
        Keyed,
        topics::{self, Topics},
        traits_ext::{ComponentsById, DeleteCell, EntityMarker},
    },
    streaming::topics::topic2::{Topic, TopicRef, TopicRefMut, TopicRoot},
};
use iggy_common::{CompressionAlgorithm, Identifier, IggyExpiry, MaxTopicSize};

pub fn rename_index(
    old_name: &<TopicRoot as Keyed>::Key,
    new_name: String,
) -> impl FnOnce(&Topics) {
    move |topics| {
        topics.with_index_mut(|index| {
            // Rename the key inside of hashmap
            let idx = index.remove(old_name).expect("Rename key: key not found");
            index.insert(new_name, idx);
        })
    }
}

pub fn get_topic_id() -> impl FnOnce(ComponentsById<TopicRef>) -> topics::ContainerId {
    |(root, _)| root.id()
}

pub fn delete_topic(topic_id: &Identifier) -> impl FnOnce(&Topics) -> Topic {
    move |topics| {
        let id = topics.get_index(topic_id);
        let topic = topics.delete(id);
        assert_eq!(topic.id(), id, "delete_topic: topic ID mismatch");
        topic
    }
}

pub fn exists(identifier: &Identifier) -> impl FnOnce(&Topics) -> bool {
    move |topics| topics.exists(identifier)
}

pub fn update_topic(
    name: String,
    message_expiry: IggyExpiry,
    compression_algorithm: CompressionAlgorithm,
    max_topic_size: MaxTopicSize,
    replication_factor: u8,
) -> impl FnOnce(ComponentsById<TopicRefMut>) -> (String, String) {
    move |(mut root, _)| {
        let old_name = root.name().clone();
        root.set_name(name.clone());
        root.set_message_expiry(message_expiry);
        root.set_compression(compression_algorithm);
        root.set_max_topic_size(max_topic_size);
        root.set_replication_factor(replication_factor);
        (old_name, name)
        // TODO: Set message expiry for all partitions and segments.
    }
}
