use super::COMPONENT;
use crate::{
    configs::system::SystemConfig,
    io::fs_utils::remove_dir_all,
    shard_error, shard_info, shard_trace,
    streaming::{partitions::consumer_offset::ConsumerOffset, segments::Segment2},
};
use compio::{
    fs::{self, OpenOptions, create_dir_all},
    io::AsyncWriteAtExt,
};
use error_set::ErrContext;
use iggy_common::{ConsumerKind, IggyError};
use std::{path::Path, sync::Arc};
use tracing::{error, trace};

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

pub async fn delete_partitions_from_disk(
    shard_id: u16,
    stream_id: usize,
    topic_id: usize,
    segments: Vec<Segment2>,
    config: &SystemConfig,
) -> Result<(), IggyError> {
    for segment in segments.iter_mut() {
        //TODO:
        //segment.close().await;
    }

    for partition_id in segments.iter().map(|s| s.parent_id) {
        let partition_path = config.get_partition_path(stream_id, topic_id, partition_id);
        remove_dir_all(&partition_path).await?;
        shard_info!(
            shard_id,
            "Deleted partition files for partition with ID: {} stream with ID: {} and topic with ID: {}.",
            partition_id,
            stream_id,
            topic_id
        );
    }
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
    let file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(path)
        .await?;
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

pub fn load_consumer_offsets(
    path: &str,
    kind: ConsumerKind,
) -> Result<Vec<ConsumerOffset>, IggyError> {
    trace!("Loading consumer offsets from path: {path}...");
    let dir_entries = std::fs::read_dir(&path);
    if dir_entries.is_err() {
        return Err(IggyError::CannotReadConsumerOffsets(path.to_owned()));
    }

    let mut consumer_offsets = Vec::new();
    let mut dir_entries = dir_entries.unwrap();
    while let Some(dir_entry) = dir_entries.next() {
        let dir_entry = dir_entry.unwrap();
        let metadata = dir_entry.metadata();
        if metadata.is_err() {
            break;
        }

        if metadata.unwrap().is_dir() {
            continue;
        }

        let name = dir_entry.file_name().into_string().unwrap();
        let consumer_id = name.parse::<u32>();
        if consumer_id.is_err() {
            error!("Invalid consumer ID file with name: '{}'.", name);
            continue;
        }

        let path = dir_entry.path();
        let path = path.to_str();
        if path.is_none() {
            error!("Invalid consumer ID path for file with name: '{}'.", name);
            continue;
        }

        let path = path.unwrap().to_string();
        let consumer_id = consumer_id.unwrap();
        let file = std::fs::File::open(&path)
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to open offset file, path: {path}")
            })
            .map_err(|_| IggyError::CannotReadFile)?;
        let mut cursor = std::io::Cursor::new(file);
        let offset = cursor
            .read_u64_le()
            .with_error_context(|error| {
                format!("{COMPONENT} (error: {error}) - failed to read consumer offset from file, path: {path}")
            })
            .map_err(|_| IggyError::CannotReadFile)?;

        consumer_offsets.push(ConsumerOffset {
            kind,
            consumer_id,
            offset,
            path,
        });
    }

    consumer_offsets.sort_by(|a, b| a.consumer_id.cmp(&b.consumer_id));
    Ok(consumer_offsets)
}
