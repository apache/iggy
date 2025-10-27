use compio::fs::create_dir_all;
use iggy_common::IggyError;
use std::path::Path;

use crate::{
    configs::system::SystemConfig,
    io::fs_utils::remove_dir_all,
    slab::traits_ext::{Delete, EntityComponentSystem, EntityMarker, IntoComponents},
    streaming::{partitions::storage::delete_partitions_from_disk, topics::topic},
};

pub async fn create_topic_file_hierarchy(
    stream_id: usize,
    topic_id: usize,
    config: &SystemConfig,
) -> Result<(), IggyError> {
    let topic_path = config.get_topic_path(stream_id, topic_id);
    let partitions_path = config.get_partitions_path(stream_id, topic_id);
    if !Path::new(&topic_path).exists() && create_dir_all(&topic_path).await.is_err() {
        return Err(IggyError::CannotCreateTopicDirectory(
            topic_id, stream_id, topic_path,
        ));
    }
    tracing::info!(
        "Saved topic with ID: {}. for stream with ID: {}",
        topic_id,
        stream_id
    );

    if !Path::new(&partitions_path).exists() && create_dir_all(&partitions_path).await.is_err() {
        return Err(IggyError::CannotCreatePartitionsDirectory(
            stream_id, topic_id,
        ));
    }
    Ok(())
}

pub async fn delete_topic_from_disk(
    stream_id: usize,
    topic: &mut topic::Topic,
    config: &SystemConfig,
) -> Result<(u64, u64, u32), IggyError> {
    let topic_path = config.get_topic_path(stream_id, topic.id());
    let topic_id = topic.id();
    if !Path::new(&topic_path).exists() {
        return Err(IggyError::TopicDirectoryNotFound(topic_path));
    }
    // First lets go over the partitions and it's logs and delete them from disk.
    let ids = topic.root().partitions().with_components(|components| {
        let (root, ..) = components.into_components();
        root.iter().map(|(_, r)| r.id()).collect::<Vec<_>>()
    });
    let mut messages_count = 0;
    let mut size_bytes = 0;
    let mut segments_count = 0;
    let partitions = topic.root_mut().partitions_mut();
    for id in ids {
        let partition = partitions.delete(id);
        let (root, stats, _, _, _, _, _log) = partition.into_components();
        let partition_id = root.id();
        delete_partitions_from_disk(stream_id, topic_id, partition_id, config).await?;
        messages_count += stats.messages_count_inconsistent();
        size_bytes += stats.size_bytes_inconsistent();
        segments_count += stats.segments_count_inconsistent();
    }
    // Then delete the topic directory itself.
    remove_dir_all(&topic_path).await.map_err(|_| {
        IggyError::CannotDeleteTopicDirectory(topic_id as u32, stream_id as u32, topic_path)
    })?;
    tracing::info!(
        "Deleted topic files for topic with ID: {} in stream with ID: {}.",
        topic_id,
        stream_id
    );
    Ok((messages_count, size_bytes, segments_count))
}
