use super::COMPONENT;
use compio::fs::create_dir_all;
use iggy_common::IggyError;
use std::{fs::remove_dir_all, path::Path};

use crate::{
    configs::system::SystemConfig,
    shard_info,
    slab::traits_ext::{EntityComponentSystem, IntoComponents},
    streaming::{partitions::storage2::delete_partitions_from_disk, topics::topic2},
};

pub async fn create_topic_file_hierarchy(
    shard_id: u16,
    stream_id: usize,
    topic_id: usize,
    config: &SystemConfig,
) -> Result<(), IggyError> {
    let topic_path = config.get_topic_path(stream_id, topic_id);
    let partitions_path = config.get_partitions_path(stream_id, topic_id);
    if !Path::new(&topic_path).exists() && create_dir_all(&topic_path).await.is_err() {
        return Err(IggyError::CannotCreateTopicDirectory(
            topic_id as u32,
            stream_id as u32,
            topic_path,
        ));
    }
    shard_info!(
        shard_id,
        "Saved topic with ID: {}. for stream with ID: {}",
        topic_id,
        stream_id
    );

    if !Path::new(&partitions_path).exists() && create_dir_all(&partitions_path).await.is_err() {
        return Err(IggyError::CannotCreatePartitionsDirectory(
            stream_id as u32,
            topic_id as u32,
        ));
    }

    /*
    shard_info!(shard_id,
        "Saving {} partition(s) for topic {topic}...",
        topic.partitions.len()
    );
    for (_, partition) in topic.partitions.iter() {
        let mut partition = partition.write().await;
        partition.persist().await.with_error_context(|error| {
            format!(
                "{COMPONENT} (error: {error}) - failed to persist partition, topic: {topic}"
            )
        })?;
    }

    shard_info!(shard_id, "Saved topic {topic}");
    */
    Ok(())
}

pub async fn delete_topic_from_disk(
    shard_id: u16,
    stream_id: usize,
    topic: &mut topic2::Topic,
    config: &SystemConfig,
) -> Result<(), IggyError> {
    let topic_path = config.get_topic_path(stream_id, topic.id());
    let topic_id = topic.id();
    if !Path::new(&topic_path).exists() {
        return Err(IggyError::TopicNotFound(topic_id as u32, stream_id as u32));
    }
    let partitions = topic.root_mut().partitions_mut();
    let segments = partitions.remove_segments();
    // First lets go over the partitions and it's segments and delete them from disk.
    delete_partitions_from_disk(shard_id, stream_id, topic_id, segments, config).await?;
    // Then delete the topic directory itself.
    remove_dir_all(&topic_path).await?;
    shard_info!(
        shard_id,
        "Deleted topic files for topic with ID: {} in stream with ID: {}.",
        topic_id,
        stream_id
    );
    Ok(())
}
