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
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use tokio::sync::{Semaphore, mpsc};
use tracing::{error, trace, warn};

const COMPONENT: &str = "STREAMING_PARTITIONS";
const OFFSET_DIRECTORY_BUFFER: usize = 64;
static OFFSET_DIRECTORY_READERS: Semaphore = Semaphore::const_new(4);
type OffsetDirectoryEntries = mpsc::Receiver<std::io::Result<Option<PathBuf>>>;

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
    let mut dir_entries = offset_directory_entries(path).await?;
    let mut recovered = RecoveredOffsets::default();
    loop {
        let entry_path = match dir_entries.recv().await {
            Some(Ok(Some(path))) => path,
            Some(Ok(None)) => break,
            Some(Err(error)) => {
                warn!(?kind, path, %error, "failed to enumerate offset directory");
                return Err(IggyError::CannotReadConsumerOffsets(path.to_owned()));
            }
            None => return Err(IggyError::CannotReadConsumerOffsets(path.to_owned())),
        };
        let name = entry_path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .into_owned();
        if offset_replacement_id(&name).is_some() {
            remove_stale_replacement(&entry_path, &name).await;
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
        let offset = match read_offset_file(&path, offset_kind_label(kind)).await {
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

async fn offset_directory_entries(path: &str) -> Result<OffsetDirectoryEntries, IggyError> {
    // Compio has no asynchronous directory iterator and the shard's blocking
    // pool is disabled. Bound both OS threads and buffered paths. The worker
    // owns the permit so cancellation cannot exceed the concurrency bound.
    let permit = OFFSET_DIRECTORY_READERS
        .acquire()
        .await
        .map_err(|_| IggyError::CannotReadConsumerOffsets(path.to_owned()))?;
    let (sender, receiver) = mpsc::channel(OFFSET_DIRECTORY_BUFFER);
    let directory = path.to_owned();
    std::thread::Builder::new()
        .name("iggy-offset-recovery".to_owned())
        .spawn(move || {
            let _permit = permit;
            let result = (|| {
                // Only the directory read itself is fatal. One unreadable
                // entry is skipped with a warning, as the on-reactor loader
                // did, so a single bad dirent cannot keep a partition from
                // booting.
                for entry in std::fs::read_dir(&directory)? {
                    let entry = match entry {
                        Ok(entry) => entry,
                        Err(error) => {
                            warn!(path = directory, %error, "failed to read offset directory entry");
                            continue;
                        }
                    };
                    let is_file = match entry.file_type() {
                        Ok(file_type) => file_type.is_file(),
                        Err(error) => {
                            warn!(path = directory, %error, "failed to read offset entry type");
                            continue;
                        }
                    };
                    if is_file && sender.blocking_send(Ok(Some(entry.path()))).is_err() {
                        return Ok(());
                    }
                }
                Ok(())
            })();
            // Explicit completion distinguishes an empty directory from an
            // interrupted worker. Closed receivers simply abandon enumeration.
            let _ = sender.blocking_send(result.map(|()| None));
        })
        .map_err(|error| {
            error!(path, %error, "failed to start offset directory reader");
            IggyError::CannotReadConsumerOffsets(path.to_owned())
        })?;
    Ok(receiver)
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

const fn offset_kind_label(kind: ConsumerKind) -> &'static str {
    match kind {
        ConsumerKind::Consumer => "consumer offset",
        ConsumerKind::ConsumerGroup => "consumer group offset",
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
    async fn given_missing_directory_when_loading_should_report_error_instead_of_empty_state() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("missing");
        assert!(matches!(
            load_consumer_offsets(missing.to_str().unwrap()).await,
            Err(IggyError::CannotReadConsumerOffsets(_))
        ));
    }

    #[compio::test]
    async fn given_full_directory_buffer_when_loader_is_cancelled_should_release_worker_capacity() {
        let dir = tempfile::tempdir().unwrap();
        for id in 0..OFFSET_DIRECTORY_BUFFER * 2 {
            std::fs::write(dir.path().join(id.to_string()), 0_u64.to_le_bytes()).unwrap();
        }
        let path = dir.path().to_str().unwrap();
        for _ in 0..8 {
            let entries = offset_directory_entries(path).await.unwrap();
            drop(entries);
        }
        let loaded = load_consumer_offsets(path).await.unwrap();
        assert_eq!(loaded.entries.len(), OFFSET_DIRECTORY_BUFFER * 2);
    }

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
