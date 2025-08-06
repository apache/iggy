use crate::{configs::system::SystemConfig, shard_error, shard_info};
use compio::fs::create_dir_all;
use iggy_common::IggyError;
use std::path::Path;

pub async fn create_partition_file_hierarchy(
    shard_id: u16,
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
    config: &SystemConfig,
) -> Result<(), IggyError> {
    let partition_path = config.get_partition_path(stream_id, topic_id, partition_id);
    shard_info!(
        shard_id,
        "Saving partition with ID: {} for stream with ID: {} and topic with ID: {}...",
        partition_id,
        stream_id,
        topic_id
    );
    if !Path::new(&partition_path).exists() && create_dir_all(&partition_path).await.is_err() {
        return Err(IggyError::CannotCreatePartitionDirectory(
            partition_id as u32,
            stream_id as u32,
            topic_id as u32,
        ));
    }

    let offset_path = config.get_offsets_path(stream_id, topic_id, partition_id);
    if !Path::new(&offset_path).exists() && create_dir_all(&offset_path).await.is_err() {
        shard_error!(
            shard_id,
            "Failed to create offsets directory for partition with ID: {} for stream with ID: {} and topic with ID: {}.",
            partition_id,
            stream_id,
            topic_id
        );
        return Err(IggyError::CannotCreatePartition(
            partition_id as u32,
            stream_id as u32,
            topic_id as u32,
        ));
    }

    let consumer_offset_path = config.get_consumer_offsets_path(stream_id, topic_id, partition_id);
    if !Path::new(&consumer_offset_path).exists()
        && create_dir_all(&consumer_offset_path).await.is_err()
    {
        shard_error!(
            shard_id,
            "Failed to create consumer offsets directory for partition with ID: {} for stream with ID: {} and topic with ID: {}.",
            partition_id,
            stream_id,
            topic_id
        );
        return Err(IggyError::CannotCreatePartition(
            partition_id as u32,
            stream_id as u32,
            topic_id as u32,
        ));
    }

    let consumer_group_offsets_path =
        config.get_consumer_group_offsets_path(stream_id, topic_id, partition_id);
    if !Path::new(&consumer_group_offsets_path).exists()
        && create_dir_all(&consumer_group_offsets_path).await.is_err()
    {
        shard_error!(
            shard_id,
            "Failed to create consumer group offsets directory for partition with ID: {} for stream with ID: {} and topic with ID: {}.",
            partition_id,
            stream_id,
            topic_id
        );
        return Err(IggyError::CannotCreatePartition(
            partition_id as u32,
            stream_id as u32,
            topic_id as u32,
        ));
    }

    shard_info!(
        shard_id,
        "Saved partition with start ID: {} for stream with ID: {} and topic with ID: {}, path: {}.",
        partition_id,
        stream_id,
        topic_id,
        partition_path
    );

    Ok(())
}
