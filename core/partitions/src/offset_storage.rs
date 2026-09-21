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

//! Store consumer bookmarks and the partition's applied purge generation.
//!
//! A bookmark records the last consumed offset. The purge marker instead records
//! which reset was applied to this incarnation of the partition. Recovery uses
//! them to restore progress and decide whether a purge must be repeated.
//!
//! Functions ending in `_with_storage` share the persistence sequence between
//! real disk and simulated storage. File sync makes record contents durable;
//! directory sync makes creation, replacement, or deletion durable. Offset callers
//! own that directory sync, while purge marker writes include it before returning.

use std::{io, path::Path};

use compio::{
    fs::{OpenOptions, remove_file, rename},
    io::{AsyncReadAt, AsyncWriteAtExt},
};
use iggy_common::{IggyError, calculate_checksum};
use journal::durable_storage::{DiskStorage, DurableFile, DurableStorage, OpenMode};
use tracing::warn;

const OFFSET_SIZE: usize = core::mem::size_of::<u64>();
const CHECKSUM_SIZE: usize = core::mem::size_of::<u64>();

/// Bytes a consumer-offset file holds: the offset, then a checksum over it.
///
/// The offset is a consumer cursor reloaded unchanged on every restart, so a
/// flipped bit silently rewinds the consumer into redelivery or skips it forward.
pub const OFFSET_RECORD_SIZE: usize = OFFSET_SIZE + CHECKSUM_SIZE;

/// Per-partition file recording the purge generation this replica last applied
/// locally, in the partition dir beside the segments it fences.
///
/// Two LE u64s: the applied generation, then the `created_revision` of the
/// partition incarnation it was applied for.
pub const PURGE_GENERATION_FILE: &str = "purge.gen";

/// Sibling name an atomic offset replacement writes before its rename lands.
const OFFSET_REPLACEMENT_SUFFIX: &str = ".tmp";

/// `[generation][created_revision]`, both LE u64.
const PURGE_GENERATION_RECORD_SIZE: usize = 2 * OFFSET_SIZE;

/// What a consumer-offset file was found to hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetRecord {
    /// A usable offset. `checksummed` is false for a bare offset predating the
    /// checksum, read as-is and upgraded by the next write.
    Value { offset: u64, checksummed: bool },
    /// Shorter than the value: a crash between the truncate and the write of an
    /// in-place update, the default path while `consumer_offset_persisted`
    /// is off.
    Torn,
    /// The checksum does not describe the value stored beside it.
    Corrupt {
        offset: u64,
        expected: u64,
        found: u64,
    },
}

/// Encode a consumer offset for persistence.
#[must_use]
pub fn encode_offset_record(offset: u64) -> [u8; OFFSET_RECORD_SIZE] {
    let mut record = [0u8; OFFSET_RECORD_SIZE];
    record[..OFFSET_SIZE].copy_from_slice(&offset.to_le_bytes());
    let checksum = calculate_checksum(&record[..OFFSET_SIZE]);
    record[OFFSET_SIZE..].copy_from_slice(&checksum.to_le_bytes());
    record
}

/// Decode whatever a consumer-offset file contained.
///
/// A file of exactly one offset predates the checksum and is accepted. A partly
/// written checksum region reads as the bare offset for the same reason: the record
/// is written in one call, so the low bytes are the complete new value.
#[must_use]
pub fn decode_offset_record(bytes: &[u8]) -> OffsetRecord {
    let Some(value) = bytes.first_chunk::<OFFSET_SIZE>() else {
        return OffsetRecord::Torn;
    };
    let offset = u64::from_le_bytes(*value);
    let Some(stored) = bytes
        .get(OFFSET_SIZE..)
        .and_then(<[u8]>::first_chunk::<CHECKSUM_SIZE>)
    else {
        return OffsetRecord::Value {
            offset,
            checksummed: false,
        };
    };
    let found = u64::from_le_bytes(*stored);
    let expected = calculate_checksum(value);
    if found == expected {
        OffsetRecord::Value {
            offset,
            checksummed: true,
        }
    } else {
        OffsetRecord::Corrupt {
            offset,
            expected,
            found,
        }
    }
}

/// Overwrite a consumer-offset file with `offset` and a checksum over it.
///
/// Without `persisted` the file is rewritten in place and no directory is
/// synced. With it, the record goes to a sibling inode, is data-synced and
/// renamed over the prior file, so a failed write leaves the prior cursor
/// intact, and the caller marks the parent directory for a sync on the next
/// commit walk. The replacement is tied to the same knob as the sync: without
/// the sync neither the write nor the rename is ordered against a crash, so
/// the extra inode and rename buy nothing.
///
/// # Errors
/// [`IggyError`] when the directory, file, or write cannot be created or completed.
pub async fn persist_offset(path: &str, offset: u64, persisted: bool) -> Result<(), IggyError> {
    persist_offset_with_storage(&DiskStorage, path, offset, persisted).await
}

/// Persist a consumer offset through the supplied storage backend.
///
/// Uses the same record and replacement policy as [`persist_offset`]. When
/// `persisted` is true, the caller must still sync the parent directory before
/// treating the replacement name as durable. Calls for one path must be serialized.
///
/// # Errors
/// Returns the directory, open, or write error from [`persist_offset`].
pub async fn persist_offset_with_storage<S: DurableStorage>(
    storage: &S,
    path: &str,
    offset: u64,
    persisted: bool,
) -> Result<(), IggyError> {
    let record = encode_offset_record(offset);
    if persisted {
        replace_file(storage, path, record, true, false).await
    } else {
        write_in_place(storage, path, record).await
    }
}

/// Return the write result with its original descriptor so checkpoint observes
/// writeback errors even after an unsuccessful write.
///
/// No barrier runs here. For either offset durability policy, the caller must
/// retain the writer and sync it and its directory before reclaiming the WAL
/// history that protects the update.
///
/// # Errors
/// The outer error reports directory/open failures before writing begins.
pub async fn persist_offset_retained(
    path: &str,
    offset: u64,
    existing: Option<compio::fs::File>,
) -> Result<(io::Result<()>, compio::fs::File), IggyError> {
    let mut file = if let Some(file) = existing {
        file
    } else {
        create_parent_dir(path).await?;
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .await
            .map_err(|_| IggyError::CannotOpenConsumerOffsetsFile(path.to_owned()))?
    };
    let result = file.write_all_at(encode_offset_record(offset), 0).await.0;
    Ok((result, file))
}

async fn write_in_place<S: DurableStorage, const N: usize>(
    storage: &S,
    path: &str,
    record: [u8; N],
) -> Result<(), IggyError> {
    create_parent_dir_with_storage(storage, path).await?;
    let mut file = storage
        .open(Path::new(path), OpenMode::CreateWriteOnly)
        .await
        .map_err(|_| IggyError::CannotOpenConsumerOffsetsFile(path.to_owned()))?;
    file.write(0, record.to_vec())
        .await
        .map_err(|_| IggyError::CannotWriteToFile)
}

async fn create_parent_dir(path: &str) -> Result<(), IggyError> {
    create_parent_dir_with_storage(&DiskStorage, path).await
}

async fn create_parent_dir_with_storage<S: DurableStorage>(
    storage: &S,
    path: &str,
) -> Result<(), IggyError> {
    // Probing with `Path::exists()` blocks the pump before it can submit writes.
    // Creating an existing directory already succeeds without changing it.
    if let Some(parent) = Path::new(path).parent() {
        storage.create_directories(parent).await.map_err(|_| {
            IggyError::CannotCreateConsumerOffsetsDirectory(parent.display().to_string())
        })?;
    }
    Ok(())
}

pub(crate) async fn stage_offset_replacement(path: &str, offset: u64) -> Result<(), IggyError> {
    // Install can remove old files before publishing replacements. Staging
    // must survive a crash regardless of the normal consumer offset durability policy.
    write_replacement(&DiskStorage, path, encode_offset_record(offset), true)
        .await
        .map(|_| ())
}

pub(crate) async fn commit_offset_replacement(path: &str) -> Result<(), IggyError> {
    rename(replacement_path(path), path)
        .await
        .map_err(|_| IggyError::CannotWriteToFile)
}

pub(crate) async fn discard_offset_replacement(path: &str) {
    let _ = remove_file(replacement_path(path)).await;
}

async fn replace_file<S: DurableStorage, const N: usize>(
    storage: &S,
    path: &str,
    record: [u8; N],
    persisted: bool,
    sync_parent: bool,
) -> Result<(), IggyError> {
    let temporary = write_replacement(storage, path, record, persisted).await?;
    if storage
        .rename(Path::new(&temporary), Path::new(path))
        .await
        .is_err()
    {
        let _ = storage.remove_file(Path::new(&temporary)).await;
        return Err(IggyError::CannotWriteToFile);
    }
    if sync_parent && let Some(parent) = Path::new(path).parent() {
        storage
            .sync_directory(parent)
            .await
            .map_err(|_| IggyError::CannotSyncFile)?;
    }

    Ok(())
}

async fn write_replacement<S: DurableStorage, const N: usize>(
    storage: &S,
    path: &str,
    record: [u8; N],
    persisted: bool,
) -> Result<String, IggyError> {
    create_parent_dir_with_storage(storage, path).await?;

    // Keep the previous cursor intact until the complete replacement exists.
    // A failed write after truncation otherwise turns a valid cursor into a torn
    // file that boot discards. The fixed sibling is safe because writes to one
    // consumer key are serialized by the partition pump.
    let temporary = replacement_path(path);
    let mut file = storage
        .open(Path::new(&temporary), OpenMode::CreateWriteOnly)
        .await
        .map_err(|_| IggyError::CannotOpenConsumerOffsetsFile(path.to_owned()))?;
    if file.write(0, record.to_vec()).await.is_err() {
        let _ = storage.remove_file(Path::new(&temporary)).await;
        return Err(IggyError::CannotWriteToFile);
    }

    if persisted && file.sync().await.is_err() {
        let _ = storage.remove_file(Path::new(&temporary)).await;
        return Err(IggyError::CannotWriteToFile);
    }
    drop(file);
    Ok(temporary)
}

fn replacement_path(path: &str) -> String {
    format!("{path}{OFFSET_REPLACEMENT_SUFFIX}")
}

#[must_use]
pub fn offset_replacement_id(name: &str) -> Option<u32> {
    name.strip_suffix(OFFSET_REPLACEMENT_SUFFIX)?.parse().ok()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PersistedOffset {
    pub offset: u64,
    pub written: bool,
}

/// Monotone counterpart of [`persist_offset`] for a server auto-commit op.
///
/// Folds `max(current_on_disk, offset)` and returns the value now on disk, skipping
/// the write when the file already holds it. Disk-tier polls replicate their
/// auto-committed offsets in IO-completion order, so a committed op can carry a lower
/// offset than an earlier one, and a plain overwrite would leave the file rewound for
/// a restart to reload and re-deliver. The on-disk value is committed-only, so the
/// fold is identical on every replica applying the same op order.
///
/// The read makes this the cold-key path only: once the caller's persisted-offset
/// tracker knows the file's value, warm commits persist with a blind
/// [`persist_offset`] and skip covered offsets without reading.
///
/// A file that fails its checksum folds as absent and is overwritten. Its value is
/// untrusted and the boot loader already discarded it, so nothing is preserved by
/// refusing, and refusing is not survivable: the caller reads a failed commit as
/// divergence and aborts the shard, and the key is cold on every boot, so one damaged
/// file aborts every boot. Redelivery is within at-least-once.
///
/// # Errors
/// [`IggyError`] when the file cannot be read or written.
pub async fn persist_offset_max(
    path: &str,
    offset: u64,
    persisted: bool,
) -> Result<PersistedOffset, IggyError> {
    let result = read_offset_max(path, offset).await?;
    if result.written {
        persist_offset(path, result.offset, persisted).await?;
    }
    Ok(result)
}

/// Read the committed maximum without replacing its file.
///
/// # Errors
/// Returns an error if the existing offset cannot be read.
pub async fn read_offset_max(path: &str, offset: u64) -> Result<PersistedOffset, IggyError> {
    let on_disk = match read_offset_record(path).await? {
        Some(OffsetRecord::Value { offset, .. }) => Some(offset),
        Some(OffsetRecord::Corrupt {
            offset: stored,
            expected,
            found,
        }) => {
            tracing::error!(
                path,
                stored,
                expected,
                found,
                fold_to = offset,
                "consumer offset file failed its checksum; overwriting it with the committed \
                 offset. This consumer may see redelivery."
            );
            None
        }
        Some(OffsetRecord::Torn) | None => None,
    };
    let effective = on_disk.map_or(offset, |current| current.max(offset));
    let written = on_disk != Some(effective);
    Ok(PersistedOffset {
        offset: effective,
        written,
    })
}

/// Durably record the purge generation a partition has locally applied, keyed
/// to the incarnation (`created_revision`) it was applied for.
///
/// Atomic replacement like [`persist_offset`] but always synced, regardless of
/// the consumer offset durability policy: purges are rare, the record is 16 bytes, and
/// a generation lost from the page cache in a crash makes the reconciler
/// repeat the purge on restart, wiping messages appended after the purge.
/// The parent directory is synced after the replacement is renamed into place.
/// A failure before rename preserves the previous record. If the directory sync
/// fails, the replacement is visible but its survival across a crash is uncertain.
///
/// # Errors
/// Propagates the underlying open, write, or sync failure.
pub async fn persist_purge_generation(
    path: &str,
    generation: u64,
    created_revision: u64,
) -> Result<(), IggyError> {
    persist_purge_generation_with_storage(&DiskStorage, path, generation, created_revision).await
}

/// Persist a purge completion marker through the supplied storage backend.
///
/// The record includes the partition incarnation. Both the replacement file and
/// its parent directory are synced as in [`persist_purge_generation`]. Calls for
/// one path must be serialized because they share a temporary filename.
///
/// # Errors
/// Returns the directory, open, write, or sync error from [`persist_purge_generation`].
pub async fn persist_purge_generation_with_storage<S: DurableStorage>(
    storage: &S,
    path: &str,
    generation: u64,
    created_revision: u64,
) -> Result<(), IggyError> {
    let mut record = [0u8; PURGE_GENERATION_RECORD_SIZE];
    record[..OFFSET_SIZE].copy_from_slice(&generation.to_le_bytes());
    record[OFFSET_SIZE..].copy_from_slice(&created_revision.to_le_bytes());
    replace_file(storage, path, record, true, true).await
}

/// Read the purge generation this replica applied for the `created_revision`
/// incarnation of the partition through the supplied storage backend.
///
/// Absent and torn files map to `Ok(0)`, which makes the reconciler apply any
/// committed purge again. A failed existence probe is logged and treated as absence.
///
/// A record written for a different incarnation maps to `Ok(0)` too. A failed
/// `delete_partitions_from_disk` leaves the directory (and this file) behind;
/// the recreated topic's generations restart at 0, so hydrating the dead
/// incarnation's generation would swallow every purge of the new topic until
/// the committed counter climbed past it.
///
/// An open or read error propagates. Collapsing it to `0` would purge a
/// partition whose durable generation is intact but momentarily unreadable,
/// destroying every message appended after that purge.
///
/// # Errors
/// Propagates an open or read failure after the existence probe, except a short read.
pub async fn read_purge_generation_with_storage<S: DurableStorage>(
    storage: &S,
    path: &str,
    created_revision: u64,
) -> Result<u64, IggyError> {
    match storage.exists_following_links(Path::new(path)).await {
        Ok(true) => {}
        Ok(false) => return Ok(0),
        Err(error) => {
            warn!(
                target: "iggy.partitions.diag",
                plane = "partitions",
                path,
                %error,
                "failed to check purge generation file, treating it as absent"
            );
            return Ok(0);
        }
    }
    let file = storage
        .open(Path::new(path), OpenMode::Read)
        .await
        .map_err(|_| IggyError::CannotOpenConsumerOffsetsFile(path.to_owned()))?;
    let buf = match file.read(0, PURGE_GENERATION_RECORD_SIZE).await {
        Ok(buf) => buf,
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(0),
        Err(_) => return Err(IggyError::CannotReadConsumerOffsets(path.to_owned())),
    };
    let (generation_bytes, revision_bytes) = buf.split_at(OFFSET_SIZE);
    let generation = u64::from_le_bytes(
        generation_bytes
            .try_into()
            .map_err(|_| IggyError::CannotReadConsumerOffsets(path.to_owned()))?,
    );
    let stored_revision = u64::from_le_bytes(
        revision_bytes
            .try_into()
            .map_err(|_| IggyError::CannotReadConsumerOffsets(path.to_owned()))?,
    );
    if stored_revision != created_revision {
        warn!(
            target: "iggy.partitions.diag",
            plane = "partitions",
            path,
            generation,
            stored_revision,
            created_revision,
            "ignoring a purge generation recorded for another partition incarnation"
        );
        return Ok(0);
    }
    Ok(generation)
}

/// Read whatever a consumer-offset file holds. `None` only when absent; a short file
/// reports [`OffsetRecord::Torn`] and the caller folds it as the boot loader does.
///
/// Real I/O errors propagate: unlike a failed checksum, an unreadable file may still
/// hold an intact cursor, and folding it as absent would rewind the consumer.
async fn read_offset_record(path: &str) -> Result<Option<OffsetRecord>, IggyError> {
    // Absence answered by the open, not a `Path::exists()` probe: that is a BLOCKING
    // stat on the pump before every cold-key commit (see `persist_offset`).
    let file = match OpenOptions::new().read(true).open(path).await {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(IggyError::CannotOpenConsumerOffsetsFile(path.to_owned())),
    };
    // One short read, not `read_exact` twice: `decode_offset_record` classifies any
    // length, so the returned count separates a legacy 8-byte file from a full
    // record. `..read` matters: the zero padding would otherwise decode as a
    // checksum and the legacy file as `Corrupt`.
    let compio::BufResult(read, buf) = file.read_at(vec![0u8; OFFSET_RECORD_SIZE], 0).await;
    let read = read.map_err(|_| IggyError::CannotReadConsumerOffsets(path.to_owned()))?;
    if read == 0 {
        return Ok(None);
    }
    Ok(Some(decode_offset_record(&buf[..read])))
}

/// Unlink a persisted consumer-offset file. A no-op if the file is absent.
/// Returns whether a file was removed. An absent file is `Ok(false)`.
///
/// # Errors
/// Returns [`IggyError::CannotDeleteConsumerOffsetFile`] if the unlink fails.
pub async fn delete_persisted_offset(path: &str) -> Result<bool, IggyError> {
    delete_persisted_offset_with_storage(&DiskStorage, path).await
}

/// Unlink a consumer offset through the supplied storage backend.
///
/// Returns `false` when already absent. No directory sync runs here, so the caller
/// must sync the parent directory before treating a successful removal as durable.
///
/// # Errors
/// Returns [`IggyError::CannotDeleteConsumerOffsetFile`] if unlinking fails.
pub async fn delete_persisted_offset_with_storage<S: DurableStorage>(
    storage: &S,
    path: &str,
) -> Result<bool, IggyError> {
    // NotFound is tolerated on the result instead of probed for: the probe was
    // a blocking stat on the pump before every unlink, and "already gone" is
    // exactly the outcome this wants anyway.
    match storage.remove_file(Path::new(path)).await {
        Ok(()) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(_) => Err(IggyError::CannotDeleteConsumerOffsetFile(path.to_owned())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_temp_dir() -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "iggy-offset-storage-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock after epoch")
                .as_nanos(),
        ));
        std::fs::create_dir_all(&dir).expect("create temp dir");
        dir
    }

    #[test]
    fn offset_record_round_trips() {
        let record = encode_offset_record(114);
        assert_eq!(record.len(), OFFSET_RECORD_SIZE);
        assert_eq!(
            decode_offset_record(&record),
            OffsetRecord::Value {
                offset: 114,
                checksummed: true
            }
        );
    }

    #[test]
    fn offset_record_accepts_a_bare_value_written_before_the_checksum() {
        assert_eq!(
            decode_offset_record(&114u64.to_le_bytes()),
            OffsetRecord::Value {
                offset: 114,
                checksummed: false
            }
        );
    }

    #[test]
    fn offset_record_rejects_either_half_flipped() {
        // The point of the checksum: a flipped bit rewinds a consumer into redelivery
        // or skips it forward, and nothing ever notices.
        let mut value_flipped = encode_offset_record(114);
        value_flipped[0] ^= 0x01;
        assert!(matches!(
            decode_offset_record(&value_flipped),
            OffsetRecord::Corrupt { offset: 115, .. }
        ));

        let mut checksum_flipped = encode_offset_record(114);
        checksum_flipped[OFFSET_SIZE] ^= 0x01;
        assert!(matches!(
            decode_offset_record(&checksum_flipped),
            OffsetRecord::Corrupt { offset: 114, .. }
        ));
    }

    #[test]
    fn offset_record_partly_written_is_torn_below_the_value_and_bare_above_it() {
        assert_eq!(decode_offset_record(&[]), OffsetRecord::Torn);
        assert_eq!(decode_offset_record(&[0xAB; 7]), OffsetRecord::Torn);

        // One `write_all_at` writes the whole record, so a torn tail keeps the value.
        let record = encode_offset_record(114);
        assert_eq!(
            decode_offset_record(&record[..OFFSET_SIZE + 3]),
            OffsetRecord::Value {
                offset: 114,
                checksummed: false
            }
        );
    }

    #[compio::test]
    async fn read_offset_record_reports_a_corrupt_file_as_corrupt() {
        let dir = unique_temp_dir();
        let path = dir.join("42").to_string_lossy().into_owned();

        persist_offset(&path, 114, false).await.expect("persist");
        let mut bytes = std::fs::read(&path).expect("offset file exists");
        bytes[0] ^= 0x01;
        std::fs::write(&path, &bytes).expect("corrupt the file");

        let result = read_offset_record(&path).await;
        assert!(
            matches!(result, Ok(Some(OffsetRecord::Corrupt { .. }))),
            "a corrupt cursor must be distinguishable from an absent one, got {result:?}"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio::test]
    async fn read_offset_record_reads_a_legacy_bare_value() {
        let dir = unique_temp_dir();
        let path = dir.join("42").to_string_lossy().into_owned();
        std::fs::write(&path, 114u64.to_le_bytes()).expect("write legacy file");

        let read = read_offset_record(&path).await.expect("legacy file");
        assert_eq!(
            read,
            Some(OffsetRecord::Value {
                offset: 114,
                checksummed: false
            })
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio::test]
    async fn read_offset_record_absent_file_is_none() {
        let dir = unique_temp_dir();
        let path = dir.join("42").to_string_lossy().into_owned();

        let read = read_offset_record(&path).await.expect("absent file");
        assert_eq!(read, None);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio::test]
    async fn read_offset_record_round_trips_persisted_value() {
        let dir = unique_temp_dir();
        let path = dir.join("42").to_string_lossy().into_owned();

        persist_offset(&path, 114, false).await.expect("persist");
        let read = read_offset_record(&path).await.expect("valid file");
        assert_eq!(
            read,
            Some(OffsetRecord::Value {
                offset: 114,
                checksummed: true
            })
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio::test]
    async fn failed_replacement_keeps_the_previous_offset_file_intact() {
        let dir = unique_temp_dir();
        let path = dir.join("42").to_string_lossy().into_owned();
        persist_offset(&path, 114, true)
            .await
            .expect("initial persist");
        std::fs::create_dir(format!("{path}{OFFSET_REPLACEMENT_SUFFIX}"))
            .expect("block temporary file creation");

        assert!(persist_offset(&path, 115, true).await.is_err());
        let bytes = std::fs::read(&path).expect("previous offset survives");
        assert_eq!(
            decode_offset_record(&bytes),
            OffsetRecord::Value {
                offset: 114,
                checksummed: true,
            }
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio::test]
    async fn read_offset_record_torn_file_is_torn_not_error() {
        let dir = unique_temp_dir();
        let path = dir.join("42").to_string_lossy().into_owned();
        std::fs::write(&path, [0xAB, 0xCD, 0xEF]).expect("write torn file");

        let read = read_offset_record(&path)
            .await
            .expect("torn file must not error the commit path");
        assert_eq!(
            read,
            Some(OffsetRecord::Torn),
            "a short file folds as absent, like the boot loader, but is not silence"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio::test]
    async fn read_offset_record_real_io_error_propagates() {
        // A directory opens read-only but every read fails with EISDIR: a real
        // I/O error, not a short read. It must surface as Err, never as None
        // (a blanket None would silently rewind a valid higher offset).
        let dir = unique_temp_dir();
        let path = dir.to_string_lossy().into_owned();

        let result = read_offset_record(&path).await;
        assert!(
            matches!(result, Err(IggyError::CannotReadConsumerOffsets(_))),
            "real I/O error must propagate, got {result:?}",
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio::test]
    async fn purge_generation_absent_or_torn_is_zero_but_io_error_propagates() {
        let dir = unique_temp_dir();
        let path = dir
            .join(PURGE_GENERATION_FILE)
            .to_string_lossy()
            .into_owned();

        assert_eq!(
            read_purge_generation_with_storage(&DiskStorage, &path, 11)
                .await
                .expect("absent file"),
            0,
            "absent file is 0"
        );

        persist_purge_generation(&path, 3, 11)
            .await
            .expect("persist generation");
        assert_eq!(
            read_purge_generation_with_storage(&DiskStorage, &path, 11)
                .await
                .expect("valid file"),
            3,
            "round-trip"
        );

        std::fs::write(&path, [0xAB, 0xCD]).expect("write torn file");
        assert_eq!(
            read_purge_generation_with_storage(&DiskStorage, &path, 11)
                .await
                .expect("torn file"),
            0,
            "torn file degrades to 0 so the reconciler re-applies the purge"
        );

        // A directory path is a real I/O error, not a short read: it must
        // surface, not collapse to the re-purge sentinel (a silent re-purge
        // would destroy post-purge messages).
        let result =
            read_purge_generation_with_storage(&DiskStorage, &dir.to_string_lossy(), 11).await;
        assert!(
            matches!(result, Err(IggyError::CannotReadConsumerOffsets(_))),
            "real I/O error must propagate, got {result:?}",
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A failed `delete_partitions_from_disk` leaves the directory and this
    /// file behind. The recreated partition's generations restart at 0, so a
    /// record from the DEAD incarnation must not be hydrated: it would swallow
    /// every purge of the new topic until the committed counter climbed past
    /// it.
    #[compio::test]
    async fn purge_generation_from_another_incarnation_reads_as_zero() {
        let dir = unique_temp_dir();
        let path = dir
            .join(PURGE_GENERATION_FILE)
            .to_string_lossy()
            .into_owned();

        persist_purge_generation(&path, 9, 41)
            .await
            .expect("persist generation");

        assert_eq!(
            read_purge_generation_with_storage(&DiskStorage, &path, 41)
                .await
                .expect("same dir"),
            9,
            "the incarnation that wrote it still hydrates it"
        );
        assert_eq!(
            read_purge_generation_with_storage(&DiskStorage, &path, 42)
                .await
                .expect("stale file"),
            0,
            "a record from a dead incarnation must not fence the new one"
        );

        // The new incarnation's own purge re-keys the file.
        persist_purge_generation(&path, 1, 42)
            .await
            .expect("persist generation");
        assert_eq!(
            read_purge_generation_with_storage(&DiskStorage, &path, 42)
                .await
                .expect("rekeyed"),
            1
        );
        assert_eq!(
            read_purge_generation_with_storage(&DiskStorage, &path, 41)
                .await
                .expect("now stale"),
            0
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[compio::test]
    async fn persist_offset_max_recovers_torn_file() {
        let dir = unique_temp_dir();
        let path = dir.join("42").to_string_lossy().into_owned();
        std::fs::write(&path, [0xABu8; 5]).expect("write torn file");

        persist_offset_max(&path, 7, false)
            .await
            .expect("torn file folds as absent");
        let read = read_offset_record(&path).await.expect("repaired file");
        assert_eq!(
            read,
            Some(OffsetRecord::Value {
                offset: 7,
                checksummed: true
            })
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The crash-loop guard: a fold against a failed-checksum file must repair it,
    /// not fail the commit. The caller aborts the shard on a failed commit, and the
    /// key is cold on every boot, so the abort would repeat.
    #[compio::test]
    async fn persist_offset_max_overwrites_a_corrupt_file_instead_of_failing() {
        let dir = unique_temp_dir();
        let path = dir.join("42").to_string_lossy().into_owned();

        persist_offset(&path, 114, false).await.expect("persist");
        let mut bytes = std::fs::read(&path).expect("offset file exists");
        bytes[0] ^= 0x01;
        std::fs::write(&path, &bytes).expect("corrupt the file");

        let folded = persist_offset_max(&path, 7, false)
            .await
            .expect("a corrupt file must not fail the commit");
        assert_eq!(
            folded.offset, 7,
            "the untrusted stored value must not win the fold"
        );

        let read = read_offset_record(&path).await.expect("repaired file");
        assert_eq!(
            read,
            Some(OffsetRecord::Value {
                offset: 7,
                checksummed: true
            }),
            "the corrupt file must be repaired in place, not left to trip the next commit"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
