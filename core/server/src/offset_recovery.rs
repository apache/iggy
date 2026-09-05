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

//! Server-owned consumer offset recovery.
//!
//! Forked from `server::streaming::partitions::storage` (the legacy
//! `load_consumer_offsets` / `load_consumer_group_offsets`) so server
//! owns the loaders for the offset files its own persistence path writes,
//! without depending on the legacy `server` crate. One file per consumer (numeric
//! file name = consumer id) holding a little-endian `u64` offset then a checksum over
//! it; see [`partitions::offset_storage`]. The legacy server stays compatible both
//! ways: it reads the first eight bytes and stops, and a file it wrote itself decodes
//! here as unchecksummed.

use iggy_common::{ConsumerGroupId, ConsumerKind, ConsumerOffset, IggyError};
use partitions::offset_storage::{OffsetRecord, decode_offset_record};
use std::sync::atomic::AtomicU64;
use tracing::{error, trace, warn};

const COMPONENT: &str = "STREAMING_PARTITIONS";

pub type RecoveredOffsets<T> = (Vec<T>, Vec<u32>);

pub fn load_consumer_offsets(path: &str) -> Result<RecoveredOffsets<ConsumerOffset>, IggyError> {
    trace!("Loading consumer offsets from path: {path}...");
    let Ok(dir_entries) = std::fs::read_dir(path) else {
        return Err(IggyError::CannotReadConsumerOffsets(path.to_owned()));
    };

    let mut consumer_offsets = Vec::new();
    let mut stranded = Vec::new();
    for dir_entry in dir_entries {
        let dir_entry = match dir_entry {
            Ok(entry) => entry,
            Err(e) => {
                warn!(
                    "Failed to read directory entry in consumer offsets path: {path}, \
                     error: {e}, skipping."
                );
                continue;
            }
        };

        let metadata = match dir_entry.file_type() {
            Ok(m) => m,
            Err(e) => {
                warn!(
                    "Failed to read metadata for entry in consumer offsets path: {path}, \
                     error: {e}, skipping."
                );
                continue;
            }
        };

        if !metadata.is_file() {
            continue;
        }

        let name = dir_entry.file_name().to_string_lossy().to_string();
        let Ok(consumer_id) = name.parse::<u32>() else {
            warn!(
                "Unexpected non-numeric consumer offset file: '{}', skipping.",
                name
            );
            continue;
        };

        let path = dir_entry.path();
        let Some(path) = path.to_str().map(str::to_owned) else {
            error!("Invalid consumer ID path for file with name: '{}'.", name);
            continue;
        };

        let Some(offset) = read_offset_file(&path, "consumer offset") else {
            if std::path::Path::new(&path).is_file() {
                stranded.push(consumer_id);
            }
            continue;
        };

        consumer_offsets.push(ConsumerOffset {
            kind: ConsumerKind::Consumer,
            consumer_id,
            offset,
            path,
        });
    }

    consumer_offsets.sort_by_key(|consumer_offset| consumer_offset.consumer_id);
    Ok((consumer_offsets, stranded))
}

pub fn load_consumer_group_offsets(
    path: &str,
) -> Result<RecoveredOffsets<(ConsumerGroupId, ConsumerOffset)>, IggyError> {
    trace!("Loading consumer group offsets from path: {path}...");
    let Ok(dir_entries) = std::fs::read_dir(path) else {
        return Err(IggyError::CannotReadConsumerOffsets(path.to_owned()));
    };

    let mut consumer_group_offsets = Vec::new();
    let mut stranded = Vec::new();
    for dir_entry in dir_entries {
        let dir_entry = match dir_entry {
            Ok(entry) => entry,
            Err(e) => {
                warn!(
                    "Failed to read directory entry in consumer group offsets path: {path}, \
                     error: {e}, skipping."
                );
                continue;
            }
        };

        let metadata = match dir_entry.file_type() {
            Ok(m) => m,
            Err(e) => {
                warn!(
                    "Failed to read metadata for entry in consumer group offsets path: {path}, \
                     error: {e}, skipping."
                );
                continue;
            }
        };

        if !metadata.is_file() {
            continue;
        }

        let name = dir_entry.file_name().to_string_lossy().to_string();
        let Ok(raw_consumer_group_id) = name.parse::<u32>() else {
            warn!(
                "Unexpected non-numeric consumer group offset file: '{}', skipping.",
                name
            );
            continue;
        };
        let consumer_group_id = ConsumerGroupId(raw_consumer_group_id as usize);

        let path = dir_entry.path();
        let Some(path) = path.to_str().map(str::to_owned) else {
            error!(
                "Invalid consumer group offset path for file with name: '{}'.",
                name
            );
            continue;
        };

        let Some(offset) = read_offset_file(&path, "consumer group offset") else {
            if std::path::Path::new(&path).is_file() {
                stranded.push(raw_consumer_group_id);
            }
            continue;
        };

        let consumer_offset = ConsumerOffset {
            kind: ConsumerKind::ConsumerGroup,
            consumer_id: raw_consumer_group_id,
            offset,
            path,
        };

        consumer_group_offsets.push((consumer_group_id, consumer_offset));
    }

    Ok((consumer_group_offsets, stranded))
}

fn read_offset_file(path: &str, offset_kind: &'static str) -> Option<AtomicU64> {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) => {
            warn!(
                "{COMPONENT} (error: {e}) - failed to read offset file, \
                 path: {path}, skipping."
            );
            return None;
        }
    };
    match decode_offset_record(&bytes) {
        OffsetRecord::Value { offset, .. } => Some(AtomicU64::new(offset)),
        OffsetRecord::Torn => {
            warn!(
                "{COMPONENT} - failed to read {offset_kind} from file (truncated), \
                 path: {path}, skipping."
            );
            None
        }
        // Skipped rather than loaded: resuming from a cursor provably not the one
        // written reads as ordinary redelivery or a gap, never as corruption.
        //
        // And unlinked, not just skipped: the offset map starts cold every boot, so a
        // file left behind is re-read by the first auto-commit and trips the commit
        // path again.
        OffsetRecord::Corrupt {
            offset,
            expected,
            found,
        } => {
            error!(
                "{COMPONENT} - {offset_kind} file failed its checksum \
                 (offset: {offset}, expected: {expected}, found: {found}), \
                 path: {path}, removing it and resuming this consumer from the start."
            );
            if let Err(e) = std::fs::remove_file(path) {
                error!(
                    "{COMPONENT} (error: {e}) - could not remove the corrupt \
                     {offset_kind} file, path: {path}; remove it manually."
                );
            }
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_numeric_directory_and_torn_file_when_loading_should_reserve_only_regular_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("7")).unwrap();
        std::fs::write(dir.path().join("8"), [1, 2]).unwrap();
        std::fs::write(dir.path().join("9"), 12_u64.to_le_bytes()).unwrap();
        let path = dir.path().to_str().unwrap();
        let (consumers, stranded) = load_consumer_offsets(path).unwrap();
        assert_eq!(consumers.len(), 1);
        assert_eq!(consumers[0].consumer_id, 9);
        assert_eq!(stranded, vec![8]);
        let (groups, stranded) = load_consumer_group_offsets(path).unwrap();
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].0, ConsumerGroupId(9));
        assert_eq!(stranded, vec![8]);
    }
}
