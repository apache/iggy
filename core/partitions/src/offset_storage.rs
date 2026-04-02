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

use crate::PartitionsConfig;
use compio::{
    fs::{OpenOptions, create_dir_all, remove_file},
    io::AsyncWriteAtExt,
};
use iggy_common::{
    ConsumerGroupId, ConsumerKind, ConsumerOffset, IggyError, sharding::IggyNamespace,
};
use std::{io::Read, path::Path};

pub async fn create_offset_file_hierarchy(
    config: &PartitionsConfig,
    namespace: IggyNamespace,
) -> Result<(), IggyError> {
    let stream_id = namespace.stream_id();
    let topic_id = namespace.topic_id();
    let partition_id = namespace.partition_id();
    let partition_path = config.get_partition_path(stream_id, topic_id, partition_id);

    if !Path::new(&partition_path).exists() {
        create_dir_all(&partition_path).await.map_err(|_| {
            IggyError::CannotCreatePartitionDirectory(partition_id, stream_id, topic_id)
        })?;
    }

    let consumer_offsets_path = config.get_consumer_offsets_path(stream_id, topic_id, partition_id);
    if !Path::new(&consumer_offsets_path).exists() {
        create_dir_all(&consumer_offsets_path).await.map_err(|_| {
            IggyError::CannotCreateConsumerOffsetsDirectory(consumer_offsets_path.clone())
        })?;
    }

    let consumer_group_offsets_path =
        config.get_consumer_group_offsets_path(stream_id, topic_id, partition_id);
    if !Path::new(&consumer_group_offsets_path).exists() {
        create_dir_all(&consumer_group_offsets_path)
            .await
            .map_err(|_| {
                IggyError::CannotCreateConsumerOffsetsDirectory(consumer_group_offsets_path.clone())
            })?;
    }

    Ok(())
}

pub async fn persist_offset(path: &str, offset: u64) -> Result<(), IggyError> {
    if let Some(parent) = Path::new(path).parent()
        && !parent.exists()
    {
        create_dir_all(parent).await.map_err(|_| {
            IggyError::CannotCreateConsumerOffsetsDirectory(parent.display().to_string())
        })?;
    }

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
    Ok(())
}

pub async fn delete_persisted_offset(path: &str) -> Result<(), IggyError> {
    if !Path::new(path).exists() {
        return Ok(());
    }

    remove_file(path)
        .await
        .map_err(|_| IggyError::CannotDeleteConsumerOffsetFile(path.to_owned()))
}

pub fn load_consumer_offsets(path: &str) -> Result<Vec<ConsumerOffset>, IggyError> {
    if !Path::new(path).exists() {
        return Ok(Vec::new());
    }

    let dir_entries = std::fs::read_dir(path)
        .map_err(|_| IggyError::CannotReadConsumerOffsets(path.to_owned()))?;
    let mut consumer_offsets = Vec::new();

    for dir_entry in dir_entries {
        let dir_entry =
            dir_entry.map_err(|_| IggyError::CannotReadConsumerOffsets(path.to_owned()))?;
        let metadata = dir_entry
            .metadata()
            .map_err(|_| IggyError::CannotReadConsumerOffsets(path.to_owned()))?;
        if metadata.is_dir() {
            continue;
        }

        let file_name = dir_entry
            .file_name()
            .into_string()
            .map_err(|_| IggyError::CannotReadConsumerOffsets(path.to_owned()))?;
        let consumer_id = file_name
            .parse::<u32>()
            .map_err(|_| IggyError::CannotReadConsumerOffsets(path.to_owned()))?;
        let offset_path = dir_entry
            .path()
            .to_str()
            .ok_or_else(|| IggyError::CannotReadConsumerOffsets(path.to_owned()))?
            .to_owned();
        let offset = read_offset(&offset_path)?;

        consumer_offsets.push(ConsumerOffset::new(
            ConsumerKind::Consumer,
            consumer_id,
            offset,
            offset_path,
        ));
    }

    consumer_offsets.sort_by_key(|offset| offset.consumer_id);
    Ok(consumer_offsets)
}

pub fn load_consumer_group_offsets(
    path: &str,
) -> Result<Vec<(ConsumerGroupId, ConsumerOffset)>, IggyError> {
    if !Path::new(path).exists() {
        return Ok(Vec::new());
    }

    let dir_entries = std::fs::read_dir(path)
        .map_err(|_| IggyError::CannotReadConsumerOffsets(path.to_owned()))?;
    let mut consumer_group_offsets = Vec::new();

    for dir_entry in dir_entries {
        let dir_entry =
            dir_entry.map_err(|_| IggyError::CannotReadConsumerOffsets(path.to_owned()))?;
        let metadata = dir_entry
            .metadata()
            .map_err(|_| IggyError::CannotReadConsumerOffsets(path.to_owned()))?;
        if metadata.is_dir() {
            continue;
        }

        let file_name = dir_entry
            .file_name()
            .into_string()
            .map_err(|_| IggyError::CannotReadConsumerOffsets(path.to_owned()))?;
        let consumer_group_id = file_name
            .parse::<u32>()
            .map_err(|_| IggyError::CannotReadConsumerOffsets(path.to_owned()))?;
        let offset_path = dir_entry
            .path()
            .to_str()
            .ok_or_else(|| IggyError::CannotReadConsumerOffsets(path.to_owned()))?
            .to_owned();
        let offset = read_offset(&offset_path)?;

        consumer_group_offsets.push((
            ConsumerGroupId(
                usize::try_from(consumer_group_id).expect("u32 group id must fit usize"),
            ),
            ConsumerOffset::new(
                ConsumerKind::ConsumerGroup,
                consumer_group_id,
                offset,
                offset_path,
            ),
        ));
    }

    consumer_group_offsets.sort_by_key(|(group_id, _)| group_id.0);
    Ok(consumer_group_offsets)
}

fn read_offset(path: &str) -> Result<u64, IggyError> {
    let file = std::fs::File::open(path).map_err(|_| IggyError::CannotReadFile)?;
    let mut cursor = std::io::Cursor::new(file);
    let mut offset = [0; 8];
    cursor
        .get_mut()
        .read_exact(&mut offset)
        .map_err(|_| IggyError::CannotReadFile)?;
    Ok(u64::from_le_bytes(offset))
}
