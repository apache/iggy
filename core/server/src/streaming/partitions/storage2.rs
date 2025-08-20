use crate::{configs::system::SystemConfig, shard_error, shard_info, shard_trace};
use compio::{
    fs::{self, OpenOptions, create_dir_all},
    io::AsyncWriteAtExt,
};
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

pub async fn delete_persisted_offset(shard_id: u16, path: &str) -> Result<(), IggyError> {
    if !Path::new(path).exists() {
        shard_trace!(shard_id, "Consumer offset file does not exist: {path}.");
        return Ok(());
    }

    if fs::remove_file(path).await.is_err() {
        shard_error!(shard_id, "Cannot delete consumer offset file: {path}.");
        return Err(IggyError::CannotDeleteConsumerOffsetFile(path.to_owned()));
    }
    Ok(())
}

pub async fn persist_offset(shard_id: u16, path: &str, offset: u64) -> Result<(), IggyError> {
    let file = OpenOptions::new().write(true).create(true).open(path).await?;
    let buf = offset.to_le_bytes();
    file.write_all_at(buf, 0).await?;
    shard_trace!(
        shard_id,
        "Stored consumer offset value: {}, path: {}",
        offset,
        path
    );
    Ok(())
}
