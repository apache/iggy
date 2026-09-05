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
use partitions::offset_storage::{OFFSET_REPLACEMENT_SUFFIX, OffsetRecord, decode_offset_record};
use std::sync::atomic::AtomicU64;
use tracing::{error, trace, warn};

const COMPONENT: &str = "STREAMING_PARTITIONS";

pub struct RecoveredOffsets<T> {
    pub entries: Vec<T>,
    pub stranded_ids: Vec<u32>,
}

enum OffsetFileLoad {
    Loaded(AtomicU64),
    Removed,
    Stranded,
}

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
        if name.ends_with(OFFSET_REPLACEMENT_SUFFIX) {
            remove_stale_replacement(&dir_entry.path(), &name);
            continue;
        }
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

        let offset = match read_offset_file(&path, "consumer offset") {
            OffsetFileLoad::Loaded(offset) => offset,
            OffsetFileLoad::Removed => continue,
            OffsetFileLoad::Stranded => {
                stranded.push(consumer_id);
                continue;
            }
        };

        consumer_offsets.push(ConsumerOffset {
            kind: ConsumerKind::Consumer,
            consumer_id,
            offset,
            path,
        });
    }

    consumer_offsets.sort_by_key(|consumer_offset| consumer_offset.consumer_id);
    Ok(RecoveredOffsets {
        entries: consumer_offsets,
        stranded_ids: stranded,
    })
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
        if name.ends_with(OFFSET_REPLACEMENT_SUFFIX) {
            remove_stale_replacement(&dir_entry.path(), &name);
            continue;
        }
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

        let offset = match read_offset_file(&path, "consumer group offset") {
            OffsetFileLoad::Loaded(offset) => offset,
            OffsetFileLoad::Removed => continue,
            OffsetFileLoad::Stranded => {
                stranded.push(raw_consumer_group_id);
                continue;
            }
        };

        let consumer_offset = ConsumerOffset {
            kind: ConsumerKind::ConsumerGroup,
            consumer_id: raw_consumer_group_id,
            offset,
            path,
        };

        consumer_group_offsets.push((consumer_group_id, consumer_offset));
    }

    Ok(RecoveredOffsets {
        entries: consumer_group_offsets,
        stranded_ids: stranded,
    })
}

/// A crashed atomic replacement leaves its sibling behind. The rename never
/// landed, so the sibling carries nothing the numeric file lacks.
fn remove_stale_replacement(path: &std::path::Path, name: &str) {
    match std::fs::remove_file(path) {
        Ok(()) => trace!("Removed stale offset replacement file: '{name}'."),
        Err(e) => warn!(
            "{COMPONENT} (error: {e}) - could not remove stale offset replacement \
             file: '{name}', skipping."
        ),
    }
}

fn read_offset_file(path: &str, offset_kind: &'static str) -> OffsetFileLoad {
    let bytes = match std::fs::read(path) {
        Ok(bytes) => bytes,
        Err(e) => {
            warn!(
                "{COMPONENT} (error: {e}) - failed to read offset file, \
                 path: {path}, skipping."
            );
            return OffsetFileLoad::Stranded;
        }
    };
    match decode_offset_record(&bytes) {
        OffsetRecord::Value { offset, .. } => OffsetFileLoad::Loaded(AtomicU64::new(offset)),
        OffsetRecord::Torn => {
            warn!(
                "{COMPONENT} - failed to read {offset_kind} from file (truncated), \
                 path: {path}, skipping."
            );
            remove_invalid_offset_file(path, offset_kind)
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
            remove_invalid_offset_file(path, offset_kind)
        }
    }
}

fn remove_invalid_offset_file(path: &str, offset_kind: &'static str) -> OffsetFileLoad {
    if let Err(error) = std::fs::remove_file(path) {
        error!(
            "{COMPONENT} (error: {error}) - could not remove the invalid \
             {offset_kind} file, path: {path}; remove it manually."
        );
        return OffsetFileLoad::Stranded;
    }
    let Some(parent) = std::path::Path::new(path).parent() else {
        return OffsetFileLoad::Removed;
    };
    match std::fs::File::open(parent).and_then(|dir| dir.sync_all()) {
        Ok(()) => OffsetFileLoad::Removed,
        Err(error) => {
            error!(
                "{COMPONENT} (error: {error}) - removed invalid {offset_kind} file but \
                 could not sync its directory, path: {path}; retaining its capacity slot."
            );
            OffsetFileLoad::Stranded
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_numeric_directory_and_torn_file_when_loading_should_remove_only_invalid_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("7")).unwrap();
        std::fs::write(dir.path().join("8"), [1, 2]).unwrap();
        std::fs::write(dir.path().join("9"), 12_u64.to_le_bytes()).unwrap();
        std::fs::write(dir.path().join("9.tmp"), [0_u8; 4]).unwrap();
        let path = dir.path().to_str().unwrap();
        let consumers = load_consumer_offsets(path).unwrap();
        assert!(!dir.path().join("9.tmp").exists());
        assert_eq!(consumers.entries.len(), 1);
        assert_eq!(consumers.entries[0].consumer_id, 9);
        assert!(consumers.stranded_ids.is_empty());
        assert!(!dir.path().join("8").exists());
        let groups = load_consumer_group_offsets(path).unwrap();
        assert_eq!(groups.entries.len(), 1);
        assert_eq!(groups.entries[0].0, ConsumerGroupId(9));
        assert!(groups.stranded_ids.is_empty());
    }
}
