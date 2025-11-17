// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::COMPONENT;
use crate::{
    configs::system::SystemConfig, io::fs_utils::remove_dir_all,
    streaming::partitions::consumer_offset::ConsumerOffset,
    streaming::polling_consumer::ConsumerGroupId,
};
use compio::{
    fs::{self, OpenOptions, create_dir_all},
    io::AsyncWriteAtExt,
};
use err_trail::ErrContext;
use iggy_common::{ConsumerKind, IggyError};
use std::{io::Read, path::Path, sync::atomic::AtomicU64};
use tracing::{error, trace};

pub async fn create_partition_file_hierarchy(
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
    config: &SystemConfig,
) -> Result<(), IggyError> {
    let partition_path = config.get_partition_path(stream_id, topic_id, partition_id);
    tracing::info!(
        "Saving partition with ID: {} for stream with ID: {} and topic with ID: {}...",
        partition_id,
        stream_id,
        topic_id
    );
    if !Path::new(&partition_path).exists() && create_dir_all(&partition_path).await.is_err() {
        return Err(IggyError::CannotCreatePartitionDirectory(
            partition_id,
            stream_id,
            topic_id,
        ));
    }

    let offset_path = config.get_offsets_path(stream_id, topic_id, partition_id);
    if !Path::new(&offset_path).exists() && create_dir_all(&offset_path).await.is_err() {
        tracing::error!(
            "Failed to create offsets directory for partition with ID: {} for stream with ID: {} and topic with ID: {}.",
            partition_id,
            stream_id,
            topic_id
        );
        return Err(IggyError::CannotCreatePartition(
            partition_id,
            stream_id,
            topic_id,
        ));
    }

    let consumer_offset_path = config.get_consumer_offsets_path(stream_id, topic_id, partition_id);
    if !Path::new(&consumer_offset_path).exists()
        && create_dir_all(&consumer_offset_path).await.is_err()
    {
        tracing::error!(
            "Failed to create consumer offsets directory for partition with ID: {} for stream with ID: {} and topic with ID: {}.",
            partition_id,
            stream_id,
            topic_id
        );
        return Err(IggyError::CannotCreatePartition(
            partition_id,
            stream_id,
            topic_id,
        ));
    }

    let consumer_group_offsets_path =
        config.get_consumer_group_offsets_path(stream_id, topic_id, partition_id);
    if !Path::new(&consumer_group_offsets_path).exists()
        && create_dir_all(&consumer_group_offsets_path).await.is_err()
    {
        tracing::error!(
            "Failed to create consumer group offsets directory for partition with ID: {} for stream with ID: {} and topic with ID: {}.",
            partition_id,
            stream_id,
            topic_id
        );
        return Err(IggyError::CannotCreatePartition(
            partition_id,
            stream_id,
            topic_id,
        ));
    }

    tracing::info!(
        "Saved partition with start ID: {} for stream with ID: {} and topic with ID: {}, path: {}.",
        partition_id,
        stream_id,
        topic_id,
        partition_path
    );

    Ok(())
}

pub async fn delete_partitions_from_disk(
    stream_id: usize,
    topic_id: usize,
    partition_id: usize,
    config: &SystemConfig,
) -> Result<(), IggyError> {
    let partition_path = config.get_partition_path(stream_id, topic_id, partition_id);
    remove_dir_all(&partition_path).await.map_err(|_| {
        IggyError::CannotDeletePartitionDirectory(stream_id, topic_id, partition_id)
    })?;
    tracing::info!(
        "Deleted partition files for partition with ID: {} stream with ID: {} and topic with ID: {}.",
        partition_id,
        stream_id,
        topic_id
    );
    Ok(())
}

pub async fn delete_persisted_offset(path: &str) -> Result<(), IggyError> {
    if !Path::new(path).exists() {
        tracing::trace!("Consumer offset file does not exist: {path}.");
        return Ok(());
    }

    if fs::remove_file(path).await.is_err() {
        tracing::error!("Cannot delete consumer offset file: {path}.");
        return Err(IggyError::CannotDeleteConsumerOffsetFile(path.to_owned()));
    }
    Ok(())
}

pub async fn persist_offset(path: &str, offset: u64) -> Result<(), IggyError> {
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .open(path)
        .await
        .map_err(|_| IggyError::CannotOpenConsumerOffsetsFile(path.to_owned()))?;
    let buf = offset.to_le_bytes();
    file.write_all_at(buf, 0)
        .await
        .0
        .map_err(|_| IggyError::CannotWriteToFile)?;
    tracing::trace!("Stored consumer offset value: {}, path: {}", offset, path);
    Ok(())
}

pub fn load_consumer_offsets(path: &str) -> Result<Vec<ConsumerOffset>, IggyError> {
    trace!("Loading consumer offsets from path: {path}...");
    let dir_entries = std::fs::read_dir(path);
    if dir_entries.is_err() {
        return Err(IggyError::CannotReadConsumerOffsets(path.to_owned()));
    }

    let mut consumer_offsets = Vec::new();
    let dir_entries = dir_entries.unwrap();
    for dir_entry in dir_entries {
        let dir_entry = dir_entry.unwrap();
        let metadata = dir_entry.metadata();
        if metadata.is_err() {
            break;
        }

        if metadata.unwrap().is_dir() {
            continue;
        }

        let name = dir_entry.file_name().into_string().unwrap();
        let consumer_id = name.parse::<u32>().unwrap_or_else(|_| {
            panic!("Invalid consumer ID file with name: '{}'.", name);
        });

        let path = dir_entry.path();
        let path = path.to_str();
        if path.is_none() {
            error!("Invalid consumer ID path for file with name: '{}'.", name);
            continue;
        }

        let path = path.unwrap().to_string();
        let file = std::fs::File::open(&path)
            .with_error(|error| {
                format!("{COMPONENT} (error: {error}) - failed to open offset file, path: {path}")
            })
            .map_err(|_| IggyError::CannotReadFile)?;
        let mut cursor = std::io::Cursor::new(file);
        let mut offset = [0; 8];
        cursor
            .get_mut().read_exact(&mut offset)
            .with_error(|error| {
                format!("{COMPONENT} (error: {error}) - failed to read consumer offset from file, path: {path}")
            })
            .map_err(|_| IggyError::CannotReadFile)?;
        let offset = AtomicU64::new(u64::from_le_bytes(offset));

        consumer_offsets.push(ConsumerOffset {
            kind: ConsumerKind::Consumer,
            consumer_id,
            offset,
            path,
        });
    }

    consumer_offsets.sort_by(|a, b| a.consumer_id.cmp(&b.consumer_id));
    Ok(consumer_offsets)
}

pub fn load_consumer_group_offsets(
    path: &str,
) -> Result<Vec<(ConsumerGroupId, ConsumerOffset)>, IggyError> {
    trace!("Loading consumer group offsets from path: {path}...");
    let dir_entries = std::fs::read_dir(path);
    if dir_entries.is_err() {
        return Err(IggyError::CannotReadConsumerOffsets(path.to_owned()));
    }

    let mut consumer_group_offsets = Vec::new();
    let dir_entries = dir_entries.unwrap();
    for dir_entry in dir_entries {
        let dir_entry = dir_entry.unwrap();
        let metadata = dir_entry.metadata();
        if metadata.is_err() {
            break;
        }

        if metadata.unwrap().is_dir() {
            continue;
        }

        let name = dir_entry.file_name().into_string().unwrap();

        let consumer_group_id = name.parse::<u32>().unwrap_or_else(|_| {
            panic!(
                "Invalid consumer group ID in consumer group file with name: '{}'.",
                name
            );
        });
        let consumer_group_id = ConsumerGroupId(consumer_group_id as usize);

        let path = dir_entry.path();
        let path = path.to_str();
        if path.is_none() {
            error!(
                "Invalid consumer group offset path for file with name: '{}'.",
                name
            );
            continue;
        }

        let path = path.unwrap().to_string();
        let file = std::fs::File::open(&path)
            .with_error(|error| {
                format!("{COMPONENT} (error: {error}) - failed to open offset file, path: {path}")
            })
            .map_err(|_| IggyError::CannotReadFile)?;
        let mut cursor = std::io::Cursor::new(file);
        let mut offset = [0; 8];
        cursor
            .get_mut().read_exact(&mut offset)
            .with_error(|error| {
                format!("{COMPONENT} (error: {error}) - failed to read consumer group offset from file, path: {path}")
            })
            .map_err(|_| IggyError::CannotReadFile)?;
        let offset = AtomicU64::new(u64::from_le_bytes(offset));

        let consumer_offset = ConsumerOffset {
            kind: ConsumerKind::ConsumerGroup,
            consumer_id: consumer_group_id.0 as u32,
            offset,
            path,
        };

        consumer_group_offsets.push((consumer_group_id, consumer_offset));
    }

    Ok(consumer_group_offsets)
}
