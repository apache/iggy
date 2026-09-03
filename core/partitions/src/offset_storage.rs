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

use compio::{
    fs::{OpenOptions, create_dir_all, remove_file},
    io::{AsyncReadAt, AsyncReadAtExt, AsyncWriteAtExt},
};
use iggy_common::{IggyError, calculate_checksum};
use std::path::Path;
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

/// `[generation][created_revision]`, both LE u64.
const PURGE_GENERATION_RECORD_SIZE: usize = 2 * OFFSET_SIZE;

/// What a consumer-offset file was found to hold.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OffsetRecord {
    /// A usable offset. `checksummed` is false for a bare offset predating the
    /// checksum, read as-is and upgraded by the next write.
    Value { offset: u64, checksummed: bool },
    /// Shorter than the value: a crash between `persist_offset`'s truncate and write.
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
/// # Errors
/// [`IggyError`] when the directory, file, or write cannot be created or completed.
pub async fn persist_offset(path: &str, offset: u64, enforce_fsync: bool) -> Result<(), IggyError> {
    // No `exists()` probe first: that is a BLOCKING `std::path` stat on the pump
    // in front of every write, which serialises a batched fan-out on stats
    // before it can submit any I/O. `create_dir_all` is already a no-op on an
    // existing directory.
    if let Some(parent) = Path::new(path).parent() {
        create_dir_all(parent).await.map_err(|_| {
            IggyError::CannotCreateConsumerOffsetsDirectory(parent.display().to_string())
        })?;
    }

    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .await
        .map_err(|_| IggyError::CannotOpenConsumerOffsetsFile(path.to_owned()))?;
    file.write_all_at(encode_offset_record(offset), 0)
        .await
        .0
        .map_err(|_| IggyError::CannotWriteToFile)?;

    if enforce_fsync {
        file.sync_data()
            .await
            .map_err(|_| IggyError::CannotWriteToFile)?;
    }

    Ok(())
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
    enforce_fsync: bool,
) -> Result<u64, IggyError> {
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
    if on_disk != Some(effective) {
        persist_offset(path, effective, enforce_fsync).await?;
    }
    Ok(effective)
}

/// Durably record the purge generation a partition has locally applied, keyed
/// to the incarnation (`created_revision`) it was applied for.
///
/// Truncate+write like [`persist_offset`] but ALWAYS data-synced, regardless of
/// the consumer-offset fsync knob: purges are rare, the record is 16 bytes, and
/// a generation lost from the page cache in a crash makes the reconciler
/// re-purge on restart, wiping messages appended after the purge. A failure
/// leaves the previous record on disk so the caller keeps its in-memory
/// applied generation old and retries.
///
/// # Errors
/// Propagates the underlying open/write/sync failure.
pub async fn persist_purge_generation(
    path: &str,
    generation: u64,
    created_revision: u64,
) -> Result<(), IggyError> {
    if let Some(parent) = Path::new(path).parent() {
        create_dir_all(parent).await.map_err(|_| {
            IggyError::CannotCreateConsumerOffsetsDirectory(parent.display().to_string())
        })?;
    }

    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(path)
        .await
        .map_err(|_| IggyError::CannotOpenConsumerOffsetsFile(path.to_owned()))?;
    let mut record = [0u8; PURGE_GENERATION_RECORD_SIZE];
    record[..OFFSET_SIZE].copy_from_slice(&generation.to_le_bytes());
    record[OFFSET_SIZE..].copy_from_slice(&created_revision.to_le_bytes());
    file.write_all_at(record, 0)
        .await
        .0
        .map_err(|_| IggyError::CannotWriteToFile)?;
    file.sync_data()
        .await
        .map_err(|_| IggyError::CannotWriteToFile)?;
    Ok(())
}

/// Read the purge generation this replica applied for the `created_revision`
/// incarnation of the partition.
///
/// Absent and torn files map to `Ok(0)`: both imply a purge died mid-write, and
/// `0` makes the reconciler re-apply the purge, the correct self-healing
/// recovery for an idempotent wipe.
///
/// A record written for a DIFFERENT incarnation maps to `Ok(0)` too. A failed
/// `delete_partitions_from_disk` leaves the directory (and this file) behind;
/// the recreated topic's generations restart at 0, so hydrating the dead
/// incarnation's generation would swallow every purge of the new topic until
/// the committed counter climbed past it.
///
/// A real I/O error propagates instead: collapsing it to `0` would re-purge a
/// partition whose durable generation is intact but momentarily unreadable,
/// destroying every message appended after that purge.
///
/// # Errors
/// Propagates a real open/read failure (anything but absent or short).
pub async fn read_purge_generation(path: &str, created_revision: u64) -> Result<u64, IggyError> {
    if !Path::new(path).exists() {
        return Ok(0);
    }
    let file = OpenOptions::new()
        .read(true)
        .open(path)
        .await
        .map_err(|_| IggyError::CannotOpenConsumerOffsetsFile(path.to_owned()))?;
    let buf = vec![0u8; PURGE_GENERATION_RECORD_SIZE];
    let compio::BufResult(read, buf) = file.read_exact_at(buf, 0).await;
    match read {
        Ok(()) => {}
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(0),
        Err(_) => return Err(IggyError::CannotReadConsumerOffsets(path.to_owned())),
    }
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
///
/// # Errors
/// Returns [`IggyError::CannotDeleteConsumerOffsetFile`] if the unlink fails.
pub async fn delete_persisted_offset(path: &str) -> Result<(), IggyError> {
    // NotFound is tolerated on the result instead of probed for: the probe was
    // a blocking stat on the pump before every unlink, and "already gone" is
    // exactly the outcome this wants anyway.
    match remove_file(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
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
        // A directory is never a readable offset record. Unix opens it
        // read-only and fails every read with EISDIR; Windows refuses the open
        // outright. Either way it is a real I/O error, not a short read, and it
        // must surface as Err, never as None (a blanket None would silently
        // rewind a valid higher offset).
        let dir = unique_temp_dir();
        let path = dir.to_string_lossy().into_owned();

        let result = read_offset_record(&path).await;
        assert!(
            matches!(
                result,
                Err(IggyError::CannotReadConsumerOffsets(_)
                    | IggyError::CannotOpenConsumerOffsetsFile(_))
            ),
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
            read_purge_generation(&path, 11).await.expect("absent file"),
            0,
            "absent file is 0"
        );

        persist_purge_generation(&path, 3, 11)
            .await
            .expect("persist generation");
        assert_eq!(
            read_purge_generation(&path, 11).await.expect("valid file"),
            3,
            "round-trip"
        );

        std::fs::write(&path, [0xAB, 0xCD]).expect("write torn file");
        assert_eq!(
            read_purge_generation(&path, 11).await.expect("torn file"),
            0,
            "torn file degrades to 0 so the reconciler re-applies the purge"
        );

        // A directory path is a real I/O error, not a short read: it must
        // surface, not collapse to the re-purge sentinel (a silent re-purge
        // would destroy post-purge messages). Unix fails at read (EISDIR),
        // Windows at open; both must propagate.
        let result = read_purge_generation(&dir.to_string_lossy(), 11).await;
        assert!(
            matches!(
                result,
                Err(IggyError::CannotReadConsumerOffsets(_)
                    | IggyError::CannotOpenConsumerOffsetsFile(_))
            ),
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
            read_purge_generation(&path, 41).await.expect("same dir"),
            9,
            "the incarnation that wrote it still hydrates it"
        );
        assert_eq!(
            read_purge_generation(&path, 42).await.expect("stale file"),
            0,
            "a record from a dead incarnation must not fence the new one"
        );

        // The new incarnation's own purge re-keys the file.
        persist_purge_generation(&path, 1, 42)
            .await
            .expect("persist generation");
        assert_eq!(read_purge_generation(&path, 42).await.expect("rekeyed"), 1);
        assert_eq!(
            read_purge_generation(&path, 41).await.expect("now stale"),
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
            folded, 7,
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
