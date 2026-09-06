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
use partitions::offset_storage::{OffsetRecord, decode_offset_record, offset_replacement_id};
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

impl<T> Default for RecoveredOffsets<T> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            stranded_ids: Vec::new(),
        }
    }
}

pub async fn load_consumer_offsets(
    path: &str,
) -> Result<RecoveredOffsets<ConsumerOffset>, IggyError> {
    let mut recovered = load_offsets(path, ConsumerKind::Consumer, |offset| offset).await?;
    recovered.entries.sort_by_key(|offset| offset.consumer_id);
    Ok(recovered)
}

pub async fn load_consumer_group_offsets(
    path: &str,
) -> Result<RecoveredOffsets<(ConsumerGroupId, ConsumerOffset)>, IggyError> {
    load_offsets(path, ConsumerKind::ConsumerGroup, |offset| {
        (ConsumerGroupId(offset.consumer_id as usize), offset)
    })
    .await
}

async fn load_offsets<T>(
    path: &str,
    kind: ConsumerKind,
    construct: impl Fn(ConsumerOffset) -> T,
) -> Result<RecoveredOffsets<T>, IggyError> {
    trace!(?kind, path, "loading consumer offsets");
    let dir_entries = std::fs::read_dir(path)
        .map_err(|_| IggyError::CannotReadConsumerOffsets(path.to_owned()))?;
    let mut recovered = RecoveredOffsets::default();
    for dir_entry in dir_entries {
        let dir_entry = match dir_entry {
            Ok(entry) => entry,
            Err(error) => {
                warn!(?kind, path, %error, "failed to read offset directory entry");
                continue;
            }
        };
        let file_type = match dir_entry.file_type() {
            Ok(file_type) => file_type,
            Err(error) => {
                warn!(?kind, path, %error, "failed to read offset entry type");
                continue;
            }
        };
        if !file_type.is_file() {
            continue;
        }
        let name = dir_entry.file_name().to_string_lossy().into_owned();
        if offset_replacement_id(&name).is_some() {
            remove_stale_replacement(&dir_entry.path(), &name).await;
            continue;
        }
        let Ok(consumer_id) = name.parse::<u32>() else {
            warn!(
                ?kind,
                name, "unexpected non-numeric consumer offset file, skipping"
            );
            continue;
        };
        let Some(path) = dir_entry.path().to_str().map(str::to_owned) else {
            error!(?kind, name, "invalid consumer offset path");
            continue;
        };
        let offset = match read_offset_file(&path, "consumer offset").await {
            OffsetFileLoad::Loaded(offset) => offset,
            OffsetFileLoad::Removed => continue,
            OffsetFileLoad::Stranded => {
                recovered.stranded_ids.push(consumer_id);
                continue;
            }
        };
        recovered.entries.push(construct(ConsumerOffset {
            kind,
            consumer_id,
            offset,
            path,
        }));
    }
    Ok(recovered)
}

/// A crashed atomic replacement leaves its sibling behind. The rename never
/// landed, so the sibling is never authoritative. Removal needs no directory
/// sync because a resurrected sibling is still ignored on the next load.
async fn remove_stale_replacement(path: &std::path::Path, name: &str) {
    match compio::fs::remove_file(path).await {
        Ok(()) => trace!("Removed stale offset replacement file: '{name}'."),
        Err(e) => warn!(
            "{COMPONENT} (error: {e}) - could not remove stale offset replacement \
             file: '{name}', skipping."
        ),
    }
}

async fn read_offset_file(path: &str, offset_kind: &'static str) -> OffsetFileLoad {
    let bytes = match compio::fs::read(path).await {
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
                 path: {path}, removing invalid file."
            );
            remove_invalid_offset_file(path, offset_kind).await
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
            remove_invalid_offset_file(path, offset_kind).await
        }
    }
}

async fn remove_invalid_offset_file(path: &str, offset_kind: &'static str) -> OffsetFileLoad {
    if let Err(error) = compio::fs::remove_file(path).await {
        error!(
            "{COMPONENT} (error: {error}) - could not remove the invalid \
             {offset_kind} file, path: {path}; remove it manually."
        );
        return OffsetFileLoad::Stranded;
    }
    let Some(parent) = std::path::Path::new(path).parent() else {
        return OffsetFileLoad::Removed;
    };
    match async {
        let directory = compio::fs::File::open(parent).await?;
        directory.sync_all().await
    }
    .await
    {
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

    #[compio::test]
    async fn given_numeric_directory_and_torn_file_when_loading_should_remove_only_invalid_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("7")).unwrap();
        std::fs::write(dir.path().join("8"), [1, 2]).unwrap();
        std::fs::write(dir.path().join("9"), 12_u64.to_le_bytes()).unwrap();
        std::fs::write(dir.path().join("9.tmp"), [0_u8; 4]).unwrap();
        std::fs::write(dir.path().join("notes.tmp"), b"unrelated").unwrap();
        let path = dir.path().to_str().unwrap();
        let consumers = load_consumer_offsets(path).await.unwrap();
        assert!(!dir.path().join("9.tmp").exists());
        assert!(dir.path().join("notes.tmp").exists());
        assert_eq!(consumers.entries.len(), 1);
        assert_eq!(consumers.entries[0].consumer_id, 9);
        assert!(consumers.stranded_ids.is_empty());
        assert!(!dir.path().join("8").exists());
        let groups = load_consumer_group_offsets(path).await.unwrap();
        assert_eq!(groups.entries.len(), 1);
        assert_eq!(groups.entries[0].0, ConsumerGroupId(9));
        assert!(groups.stranded_ids.is_empty());
    }
}
