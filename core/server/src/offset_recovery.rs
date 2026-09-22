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

use std::path::Path;
use std::sync::atomic::AtomicU64;

use futures::StreamExt;
use iggy_common::{ConsumerGroupId, ConsumerKind, ConsumerOffset, IggyError};
#[cfg(test)]
use journal::durable_storage::DiskStorage;
use journal::durable_storage::{DurableFile, DurableStorage, OpenMode};
use partitions::offset_storage::{OffsetRecord, decode_offset_record, offset_replacement_id};
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

#[cfg(test)]
pub async fn load_consumer_offsets(
    path: &str,
) -> Result<RecoveredOffsets<ConsumerOffset>, IggyError> {
    load_consumer_offsets_with_storage(&DiskStorage, path).await
}

/// Recover consumer records, ordered by consumer ID, from a storage backend.
/// Invalid records are removed when possible. Unreadable records or removals
/// that cannot be made durable retain their IDs in `stranded_ids`.
///
/// # Errors
/// Returns [`IggyError::CannotReadConsumerOffsets`] if the directory cannot be
/// enumerated, including when it is missing.
pub async fn load_consumer_offsets_with_storage<S: DurableStorage>(
    storage: &S,
    path: &str,
) -> Result<RecoveredOffsets<ConsumerOffset>, IggyError> {
    let mut recovered =
        load_offsets(storage, path, ConsumerKind::Consumer, |offset| offset).await?;
    recovered.entries.sort_by_key(|offset| offset.consumer_id);
    Ok(recovered)
}

#[cfg(test)]
pub async fn load_consumer_group_offsets(
    path: &str,
) -> Result<RecoveredOffsets<(ConsumerGroupId, ConsumerOffset)>, IggyError> {
    load_consumer_group_offsets_with_storage(&DiskStorage, path).await
}

/// Recover group records with the same cleanup and stranded file handling as
/// [`load_consumer_offsets_with_storage`].
///
/// # Errors
/// Returns [`IggyError::CannotReadConsumerOffsets`] if the directory cannot be
/// enumerated, including when it is missing.
pub async fn load_consumer_group_offsets_with_storage<S: DurableStorage>(
    storage: &S,
    path: &str,
) -> Result<RecoveredOffsets<(ConsumerGroupId, ConsumerOffset)>, IggyError> {
    load_offsets(storage, path, ConsumerKind::ConsumerGroup, |offset| {
        (ConsumerGroupId(offset.consumer_id as usize), offset)
    })
    .await
}

async fn load_offsets<S: DurableStorage, T>(
    storage: &S,
    path: &str,
    kind: ConsumerKind,
    construct: impl Fn(ConsumerOffset) -> T,
) -> Result<RecoveredOffsets<T>, IggyError> {
    trace!(?kind, path, "loading consumer offsets");
    let mut dir_entries = storage
        .regular_files(Path::new(path))
        .await
        .map_err(|error| {
            warn!(?kind, path, %error, "failed to enumerate offset directory");
            IggyError::CannotReadConsumerOffsets(path.to_owned())
        })?;
    let mut recovered = RecoveredOffsets::default();
    while let Some(entry) = dir_entries.next().await {
        let entry_path = match entry {
            Ok(path) => path,
            Err(error) => {
                warn!(?kind, path, %error, "failed to enumerate offset directory");
                return Err(IggyError::CannotReadConsumerOffsets(path.to_owned()));
            }
        };
        let name = entry_path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned();
        if offset_replacement_id(&name).is_some() {
            remove_stale_replacement(storage, &entry_path, &name).await;
            continue;
        }
        let Ok(consumer_id) = name.parse::<u32>() else {
            warn!(
                ?kind,
                name, "unexpected non-numeric consumer offset file, skipping"
            );
            continue;
        };
        let Some(path) = entry_path.to_str().map(str::to_owned) else {
            error!(?kind, name, "invalid consumer offset path");
            continue;
        };
        let offset = match read_offset_file(storage, &path, offset_kind_label(kind)).await {
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
async fn remove_stale_replacement<S: DurableStorage>(storage: &S, path: &Path, name: &str) {
    match storage.remove_file(path).await {
        Ok(()) => trace!("Removed stale offset replacement file: '{name}'."),
        Err(e) => warn!(
            "{COMPONENT} (error: {e}) - could not remove stale offset replacement \
             file: '{name}', skipping."
        ),
    }
}

const fn offset_kind_label(kind: ConsumerKind) -> &'static str {
    match kind {
        ConsumerKind::Consumer => "consumer offset",
        ConsumerKind::ConsumerGroup => "consumer group offset",
    }
}

async fn read_offset_file<S: DurableStorage>(
    storage: &S,
    path: &str,
    offset_kind: &'static str,
) -> OffsetFileLoad {
    let bytes = match async {
        let file = storage.open(Path::new(path), OpenMode::Read).await?;
        let length = usize::try_from(file.length().await?).map_err(std::io::Error::other)?;
        file.read(0, length).await
    }
    .await
    {
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
            remove_invalid_offset_file(storage, path, offset_kind).await
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
            remove_invalid_offset_file(storage, path, offset_kind).await
        }
    }
}

async fn remove_invalid_offset_file<S: DurableStorage>(
    storage: &S,
    path: &str,
    offset_kind: &'static str,
) -> OffsetFileLoad {
    if let Err(error) = storage.remove_file(Path::new(path)).await {
        error!(
            "{COMPONENT} (error: {error}) - could not remove the invalid \
             {offset_kind} file, path: {path}; remove it manually."
        );
        return OffsetFileLoad::Stranded;
    }
    let Some(parent) = Path::new(path).parent() else {
        return OffsetFileLoad::Removed;
    };
    match storage.sync_directory(parent).await {
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
    async fn given_missing_directory_when_loading_should_report_error_instead_of_empty_state() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing");
        assert!(matches!(
            load_consumer_offsets(missing.to_str().unwrap()).await,
            Err(IggyError::CannotReadConsumerOffsets(_))
        ));
    }

    #[compio::test]
    async fn given_numeric_directory_and_torn_file_when_loading_should_remove_only_invalid_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("7")).unwrap();
        std::fs::write(dir.path().join("8"), [1, 2]).unwrap();
        std::fs::write(dir.path().join("9"), 12_u64.to_le_bytes()).unwrap();
        std::fs::write(dir.path().join("10"), []).unwrap();
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
        assert!(!dir.path().join("10").exists());
        let groups = load_consumer_group_offsets(path).await.unwrap();
        assert_eq!(groups.entries.len(), 1);
        assert_eq!(groups.entries[0].0, ConsumerGroupId(9));
        assert!(groups.stranded_ids.is_empty());
    }
}
