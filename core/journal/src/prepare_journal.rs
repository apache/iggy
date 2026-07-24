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

use crate::file_storage::FileStorage;
use crate::{Journal, JournalHandle};
use compio::io::AsyncWriteAtExt;
use iggy_binary_protocol::consensus::{Command2, PrepareHeader};
use server_common::{MESSAGE_ALIGN, Message, iobuf::Owned};
use std::cell::{Cell, OnceCell, Ref, RefCell};
use std::fmt;
use std::io;
use std::ops::RangeInclusive;
use std::path::{Path, PathBuf};

const HEADER_SIZE: usize = size_of::<PrepareHeader>();

/// Maximum allowed size for a single WAL entry (64 MiB).
///
/// A header with `size` exceeding this limit is treated as corrupt. This
/// prevents a bit-flipped size field (e.g. `0xFFFF_FFFF`) from causing a
/// multi-GiB allocation during the WAL scan.
const MAX_ENTRY_SIZE: u64 = 64 * 1024 * 1024;

/// Number of slots in the journal ring buffer.
///
/// Must be larger than the maximum number of entries between consecutive
/// snapshots. If the journal wraps past this window, older un-snapshotted
/// entries are silently evicted from the in-memory index (the WAL file
/// still contains them, but they become unreachable for recovery).
///
/// **NOTE:** This number needs to be chosen in balance between number of
/// entries in [`consensus::PIPELINE_PREPARE_QUEUE_MAX`]. Because this number controls
/// how many committed but not yet snapshotted entries that the buffer can
/// hold. This may need to be tuned properly.
pub(crate) const SLOT_COUNT: usize = 1024;

/// Default in-memory index size, in slots. Overridable per journal via
/// [`PrepareJournal::open_with_slots`] (operator knob: `[metadata]
/// journal_slots` in the server-ng config).
pub const DEFAULT_SLOT_COUNT: usize = SLOT_COUNT;

/// Error type for journal operations.
#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub enum JournalError {
    Io(io::Error),
    /// Journal entered an irrecoverable state after a partial `drain()`
    /// failure: the atomic `rename` succeeded but a subsequent step
    /// (parent-dir fsync or storage reopen) failed, leaving in-memory
    /// state desynced from on-disk state. Every IO entry point refuses
    /// further work until the journal is rebuilt by re-opening the WAL.
    ///
    /// `stage` names which drain step failed (for diagnostics); `source`
    /// is the underlying `io::Error` that caused it, kept so operators
    /// see the original kernel error (`ENOSPC`, `EIO`, ...) rather than
    /// just a generic poison marker.
    Poisoned {
        stage: &'static str,
        source: io::Error,
    },
}

impl fmt::Display for JournalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "journal I/O error: {e}"),
            Self::Poisoned { stage, .. } => write!(f, "journal poisoned at {stage}"),
        }
    }
}

impl std::error::Error for JournalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
            Self::Poisoned { source, .. } => Some(source),
        }
    }
}

impl From<io::Error> for JournalError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

/// Persistent prepare journal backed by an append-only WAL file.
///
/// Each WAL entry is a raw `Message<PrepareHeader>`:
/// `[PrepareHeader: 256 bytes][body: header.size - 256 bytes]`
///
/// The in-memory index is a fixed-size slot array indexed by
/// `op % SLOT_COUNT`.
pub struct PrepareJournal {
    /// File-backed append-only WAL.
    storage: FileStorage,
    /// In-memory slot array of entry headers, indexed by `op % SLOT_COUNT`.
    /// A slot is `None` if no entry occupies it (or it has been drained).
    headers: RefCell<Vec<Option<PrepareHeader>>>,
    /// Byte offset within the WAL file for each slot's entry, mirrors `headers`.
    offsets: RefCell<Vec<Option<u64>>>,
    /// Highest op number appended to the journal, or `None` if empty.
    /// Used to detect forward progress and validate append ordering.
    last_op: Cell<Option<u64>>,
    /// Highest op that has been durably snapshotted. Entries with `op <= snapshot_op`
    /// are safe to evict from the slot array. Appending an entry that would evict
    /// an un-snapshotted entry (op > `snapshot_op`) panics and the upper layer must
    /// take a snapshot before the journal wraps.
    snapshot_op: Cell<u64>,
    /// Populated once `drain()` has progressed past the atomic `rename`
    /// and a subsequent step has failed, meaning the on-disk WAL and the
    /// in-memory index no longer agree. All IO entry points must
    /// short-circuit with `JournalError::Poisoned` to prevent the next
    /// `append` from writing into the orphaned old fd or
    /// `entry`/`entry_at` from serving headers whose offsets reference
    /// the pre-drain layout.
    ///
    /// `OnceCell` (not `Cell<Option<_>>`) because the underlying
    /// `io::Error` is `!Clone` and the journal is dead after the first
    /// set; first-write-wins matches the actual semantics and avoids
    /// `RefCell` borrow-panic risk on the read fast path.
    poisoned: OnceCell<PoisonState>,
    /// True while a `drain()` is rewriting the WAL. Concurrent drains
    /// share the one `wal.tmp` swap and race it (truncated tmp, ENOENT
    /// on the losing rename, reads through a reopened fd), so overlap is
    /// refused up front with `ResourceBusy` instead. The upper layer
    /// serializes checkpoints anyway (metadata `journal_gate`); this is
    /// the journal's own defense so no future caller can reintroduce the
    /// race silently.
    drain_in_flight: Cell<bool>,
    /// Number of slots in the in-memory index; ops map to `op % slot_count`.
    /// [`DEFAULT_SLOT_COUNT`] unless the operator overrode it. Larger values
    /// let more committed-but-unsnapshotted entries accumulate between
    /// checkpoints (more WAL churn headroom, more memory).
    slot_count: usize,
}

/// Captured cause of journal poisoning. `stage` names the drain step
/// that tripped; `source` is the original `io::Error` for forensics.
struct PoisonState {
    stage: &'static str,
    source: io::Error,
}

impl fmt::Debug for PrepareJournal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PrepareJournal")
            .field("write_offset", &self.storage.file_len())
            .field("last_op", &self.last_op.get())
            .field("poisoned", &self.poisoned.get().map(|p| p.stage))
            .finish_non_exhaustive()
    }
}

#[allow(clippy::cast_possible_truncation)]
const fn slot_for_op(op: u64, slot_count: usize) -> usize {
    op as usize % slot_count
}

/// Repair a damaged WAL tail by truncating to `pos`, or surface a loud
/// error when truncation would be unsafe.
///
/// Truncation is only sound for a torn final append, which writes at most
/// one entry's worth of bytes. If more than `MAX_ENTRY_SIZE` bytes follow
/// `pos`, the damage is mid-file: truncating would silently discard every
/// committed entry after it, so this hard-errors instead of repairing.
#[allow(clippy::future_not_send)]
async fn truncate_or_fail(
    storage: &FileStorage,
    pos: u64,
    reason: &'static str,
) -> Result<(), JournalError> {
    let trailing = storage.file_len().saturating_sub(pos);
    // Sound only because this WAL appends exactly one entry per `append`
    // + fsync, so a torn tail is at most one entry wide. A batched-append
    // WAL could leave a torn tail many entries wide, and this
    // `> MAX_ENTRY_SIZE` check would misclassify it as mid-file damage.
    if trailing > MAX_ENTRY_SIZE {
        return Err(JournalError::Io(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "mid-file WAL corruption at pos {pos}: {reason}; {trailing} bytes \
                 follow (> MAX_ENTRY_SIZE {MAX_ENTRY_SIZE}), refusing to truncate \
                 and discard committed entries"
            ),
        )));
    }
    storage.truncate(pos).await?;
    // The repair must be crash-durable. `FileStorage::truncate` is a
    // bare `set_len`; without this fsync a power loss right after the
    // repair re-presents the torn tail on the next boot. Mirrors the
    // write-then-fsync the `append` path already does.
    storage.fsync().await?;
    Ok(())
}

/// Best-effort unlink of the drain temp file on any error path between
/// `File::create(wal.tmp)` and the atomic `rename`. Without this, every
/// failed drain leaks a `wal.tmp` next to the WAL; the next drain
/// truncates it on re-create so safety holds, but operators see the tmp
/// files accumulate across crashed/aborted drains. `defuse` is called
/// after a successful rename so the now-renamed file is not removed.
///
/// `Drop` cannot be async, so the unlink is a blocking `std::fs::remove_file`.
/// This only runs on the drain failure path (already returning an error),
/// so a sync syscall here is acceptable. Errors are swallowed: the file
/// may already be gone (e.g. rename succeeded but a later step failed
/// and we defused too late) and there is no useful recovery from a
/// failed cleanup unlink.
struct TmpFileGuard {
    path: PathBuf,
    armed: bool,
}

impl TmpFileGuard {
    const fn new(path: PathBuf) -> Self {
        Self { path, armed: true }
    }

    fn defuse(mut self) {
        self.armed = false;
    }
}

impl Drop for TmpFileGuard {
    fn drop(&mut self) {
        if self.armed {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// Clears `drain_in_flight` on every exit path of `drain()` — success,
/// `?` error, or caller cancellation at any await. A stuck flag would
/// refuse every future drain and let the journal fill for good.
struct DrainInFlightGuard<'a>(&'a Cell<bool>);

impl Drop for DrainInFlightGuard<'_> {
    fn drop(&mut self) {
        self.0.set(false);
    }
}

#[allow(clippy::cast_possible_truncation)]
impl PrepareJournal {
    /// Open the WAL file in read-write mode, scanning forward to rebuild
    /// the in-memory index.
    ///
    /// `snapshot_op` is the highest op that has been durably snapshotted.
    /// It must be provided so that `append()` can detect slot collisions
    /// that would evict un-snapshotted entries.
    ///
    /// If a truncated entry is found at the tail (crash during write),
    /// the file is truncated to the last complete entry.
    ///
    /// # Errors
    /// Returns `JournalError::Io` if the WAL file cannot be opened or read.
    #[allow(clippy::future_not_send)]
    pub async fn open(path: &Path, snapshot_op: u64) -> Result<Self, JournalError> {
        Self::open_with_slots(path, snapshot_op, DEFAULT_SLOT_COUNT).await
    }

    /// [`Self::open`] with an operator-tuned index size.
    ///
    /// `slot_count` bounds how many committed-but-unsnapshotted entries the
    /// journal holds before a forced checkpoint must reclaim WAL space; the
    /// caller owns keeping it above its checkpoint margin + prepare-queue
    /// depth (validated at config load for the server-ng `[metadata]` knob).
    ///
    /// # Errors
    /// Returns `JournalError::Io` if the WAL file cannot be opened or read,
    /// or (`InvalidInput`) if `slot_count` is zero.
    #[allow(clippy::future_not_send)]
    pub async fn open_with_slots(
        path: &Path,
        snapshot_op: u64,
        slot_count: usize,
    ) -> Result<Self, JournalError> {
        if slot_count == 0 {
            return Err(JournalError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "journal slot_count must be non-zero",
            )));
        }
        let storage = FileStorage::open(path).await?;
        Self::scan(storage, snapshot_op, slot_count).await
    }

    #[allow(clippy::future_not_send)]
    async fn scan(
        storage: FileStorage,
        snapshot_op: u64,
        slot_count: usize,
    ) -> Result<Self, JournalError> {
        let file_len = storage.file_len();
        let mut headers: Vec<Option<PrepareHeader>> = vec![None; slot_count];
        let mut offsets: Vec<Option<u64>> = vec![None; slot_count];
        let mut last_op: Option<u64> = None;
        let mut pos: u64 = 0;
        let mut header_buf = vec![0u8; HEADER_SIZE];
        // Reused 16-aligned scratch (PrepareHeader has u128 fields). Avoids
        // per-iteration 4 KiB-aligned alloc; bytes never become a `Message`.
        let mut aligned = Owned::<16>::zeroed(HEADER_SIZE);

        while pos + HEADER_SIZE as u64 <= file_len {
            // Read the 256-byte header
            header_buf = storage.read_at(pos, header_buf).await?;
            aligned.as_mut_slice().copy_from_slice(&header_buf);
            // `try_from_bytes`: corrupt discriminant on disk must NOT panic;
            // route through the same truncate-here branch as command/size below.
            // Copying into a 16-aligned scratch first keeps the
            // `PrepareHeader` (u128 fields) load aligned for miri's
            // strict-provenance / tree-borrows checks.
            let Ok(header_ref) =
                bytemuck::checked::try_from_bytes::<PrepareHeader>(aligned.as_slice())
            else {
                truncate_or_fail(&storage, pos, "corrupt header (invalid bit pattern)").await?;
                break;
            };
            let header: PrepareHeader = *header_ref;

            // Validate: must be a Prepare command with sane size
            if header.command != Command2::Prepare
                || (header.size as usize) < HEADER_SIZE
                || u64::from(header.size) > MAX_ENTRY_SIZE
            {
                truncate_or_fail(&storage, pos, "corrupt or non-prepare entry").await?;
                break;
            }

            let entry_size = u64::from(header.size);

            // TODO(hubcio): verify `header.checksum` / `header.checksum_body`
            // against the entry body during scan and route a mismatch
            // through `truncate_or_fail`. Blocked on the writer side: the
            // `PrepareHeader` projection in consensus builds prepares with
            // `..Default::default()` so the integrity fields are always 0.
            // Until a producer computes them, verification here would be
            // trivially-passing noise. The per-message receive gate now rejects
            // transit body corruption on a follower before apply, but this
            // recovery scan still does not re-verify body integrity read back
            // from disk, so an at-rest body bit-flip that leaves the header
            // valid is replayed silently. Committed bytes are meant to be
            // byte-identical across
            // replicas (deterministic apply, timestamp replicated not
            // re-projected), so once the producer computes the integrity fields
            // they should agree on every node and this check can be turned on
            // without per-replica false positives.

            // Check if the full entry fits
            if pos + entry_size > file_len {
                truncate_or_fail(&storage, pos, "truncated entry at tail").await?;
                break;
            }

            let slot = slot_for_op(header.op, slot_count);

            // Note: Regarding duplicate op in WAL. We rewrite it with whichever
            // is the latest entry.
            headers[slot] = Some(header);
            offsets[slot] = Some(pos);

            match last_op {
                Some(current) if header.op > current => last_op = Some(header.op),
                None => last_op = Some(header.op),
                _ => {}
            }

            pos += entry_size;
        }

        // If there are leftover bytes less than a header, truncate them
        if pos < storage.file_len() {
            truncate_or_fail(&storage, pos, "leftover bytes shorter than a header").await?;
        }

        Ok(Self {
            storage,
            headers: RefCell::new(headers),
            offsets: RefCell::new(offsets),
            last_op: Cell::new(last_op),
            snapshot_op: Cell::new(snapshot_op),
            poisoned: OnceCell::new(),
            drain_in_flight: Cell::new(false),
            slot_count,
        })
    }

    /// Return headers with `op >= from_op`, sorted by op.
    pub fn iter_headers_from(&self, from_op: u64) -> Vec<PrepareHeader> {
        let headers = self.headers.borrow();
        let mut result: Vec<PrepareHeader> = headers
            .iter()
            .filter_map(|slot| slot.filter(|h| h.op >= from_op))
            .collect();
        result.sort_unstable_by_key(|h| h.op);
        result
    }

    /// Highest op number in the index, or `None` if empty.
    pub const fn last_op(&self) -> Option<u64> {
        self.last_op.get()
    }

    /// Advance the snapshot watermark. The caller must ensure `op` is
    /// monotonically increasing and corresponds to a durable snapshot.
    ///
    /// # Panics
    /// Panics if `op` is less than the current snapshot watermark.
    pub fn set_snapshot_op(&self, op: u64) {
        assert!(
            op >= self.snapshot_op.get(),
            "snapshot_op must be monotonically increasing: {} -> {}",
            self.snapshot_op.get(),
            op
        );
        self.snapshot_op.set(op);
    }

    /// Access the underlying storage (for fsync in tests, etc.).
    pub const fn storage_ref(&self) -> &FileStorage {
        &self.storage
    }

    /// Stage name at which the journal was poisoned, if any. `None`
    /// means healthy. Returned as a `&'static str` so callers in
    /// non-IO paths (diagnostics, `Debug`) can read it without an
    /// `io::Error` clone.
    pub fn poison_reason(&self) -> Option<&'static str> {
        self.poisoned.get().map(|p| p.stage)
    }

    /// Build an `io::Error` representing the current poison state. The
    /// caller is expected to have already established that the journal
    /// is poisoned; the embedded `source.kind()` propagates the original
    /// kernel error (`ENOSPC`, `EIO`, ...) so the operator sees more
    /// than a generic poison marker.
    fn poisoned_io_error(state: &PoisonState) -> io::Error {
        io::Error::new(
            state.source.kind(),
            format!("journal poisoned at {}: {}", state.stage, state.source),
        )
    }

    /// Record the first poison cause (subsequent calls are silently
    /// ignored - the journal is already dead) and build the descriptive
    /// `io::Error` callers return up the stack. Consumes `source` so the
    /// original kernel error is preserved in the cell for forensics
    /// rather than discarded after the immediate return.
    fn poison(&self, stage: &'static str, source: io::Error) -> io::Error {
        let kind = source.kind();
        let msg = format!("journal poisoned at {stage}: {source}");
        let _ = self.poisoned.set(PoisonState { stage, source });
        io::Error::new(kind, msg)
    }

    #[cfg(test)]
    pub(crate) fn force_poison(&self, reason: &'static str) {
        let _ = self.poisoned.set(PoisonState {
            stage: reason,
            source: io::Error::other(reason),
        });
    }

    /// Async entry read for recovery.
    ///
    /// Returns `Ok(None)` if the op is not in the index.
    ///
    /// # Errors
    /// Returns an I/O error if the read fails or the entry is malformed.
    /// Returns an `io::ErrorKind::Other` error if the journal is
    /// poisoned; the in-memory index and the on-disk layout disagree
    /// and the stored offsets cannot be trusted.
    #[allow(clippy::future_not_send)]
    pub async fn entry_at(
        &self,
        header: &PrepareHeader,
    ) -> io::Result<Option<Message<PrepareHeader>>> {
        if let Some(state) = self.poisoned.get() {
            return Err(Self::poisoned_io_error(state));
        }
        let (offset, size) = {
            let headers = self.headers.borrow();
            let offsets = self.offsets.borrow();
            let slot = slot_for_op(header.op, self.slot_count);
            let stored = match headers[slot].as_ref() {
                Some(h) if h.op == header.op => h,
                _ => return Ok(None),
            };
            let Some(offset) = offsets[slot] else {
                return Ok(None);
            };
            (offset, stored.size as usize)
        };
        let buf = vec![0u8; size];
        let buf = self.storage.read_at(offset, buf).await?;
        let msg = Message::try_from(Owned::<MESSAGE_ALIGN>::copy_from_slice(&buf)).map_err(
            |e: iggy_binary_protocol::consensus::ConsensusError| {
                io::Error::new(io::ErrorKind::InvalidData, e.to_string())
            },
        )?;
        Ok(Some(msg))
    }
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::future_not_send
)]
impl Journal<FileStorage> for PrepareJournal {
    type Header = PrepareHeader;
    type Entry = Message<PrepareHeader>;
    type HeaderRef<'a> = Ref<'a, PrepareHeader>;

    fn header(&self, idx: usize) -> Option<Self::HeaderRef<'_>> {
        let headers = self.headers.borrow();
        Ref::filter_map(headers, |h| {
            let slot = slot_for_op(idx as u64, self.slot_count);
            let header = h[slot].as_ref()?;
            if header.op == idx as u64 {
                Some(header)
            } else {
                None
            }
        })
        .ok()
    }

    fn previous_header(&self, header: &Self::Header) -> Option<Self::HeaderRef<'_>> {
        if header.op == 0 {
            return None;
        }
        self.header((header.op - 1) as usize)
    }

    fn remaining_capacity(&self) -> Option<usize> {
        let Some(last) = self.last_op.get() else {
            return Some(self.slot_count);
        };
        let snapshot = self.snapshot_op.get();
        if last <= snapshot {
            return Some(self.slot_count);
        }
        let used = (last - snapshot) as usize;
        Some(self.slot_count.saturating_sub(used))
    }

    /// Remove entries with ops in `ops` from the journal,
    /// returning the removed entries sorted by op.
    ///
    /// Internally advances the snapshot watermark to `end_op` so that
    /// future appends treat drained slots as safe to overwrite. Rewrites
    /// the WAL file keeping only entries outside the drained range.
    async fn drain(&self, ops: RangeInclusive<u64>) -> io::Result<Vec<Self::Entry>> {
        if let Some(state) = self.poisoned.get() {
            return Err(Self::poisoned_io_error(state));
        }
        // Overlapping drains race the `wal.tmp` swap below (truncate each
        // other's tmp mid-write, lose the rename to ENOENT, read stale
        // offsets through the winner's reopened fd). Refuse up front,
        // before any WAL bytes move.
        if self.drain_in_flight.replace(true) {
            return Err(io::Error::new(
                io::ErrorKind::ResourceBusy,
                "drain already in flight: concurrent drains would race the WAL rewrite",
            ));
        }
        let _drain_guard = DrainInFlightGuard(&self.drain_in_flight);
        let end_op = *ops.end();

        // Partition slots into drained and live entries.
        let mut to_drain: Vec<(PrepareHeader, u64)> = Vec::new();
        let mut live: Vec<(PrepareHeader, u64)> = Vec::new();
        {
            let headers = self.headers.borrow();
            let offsets = self.offsets.borrow();
            for slot in 0..self.slot_count {
                if let (Some(h), Some(off)) = (&headers[slot], offsets[slot]) {
                    if ops.contains(&h.op) {
                        to_drain.push((*h, off));
                    } else {
                        live.push((*h, off));
                    }
                }
            }
        }
        to_drain.sort_unstable_by_key(|(h, _)| h.op);
        live.sort_unstable_by_key(|(h, _)| h.op);

        // Read drained entries from disk before rewriting the WAL.
        let mut drained = Vec::with_capacity(to_drain.len());
        for (header, offset) in &to_drain {
            let buf = vec![0u8; header.size as usize];
            let buf = self.storage.read_at(*offset, buf).await?;
            let msg = Message::try_from(Owned::<MESSAGE_ALIGN>::copy_from_slice(&buf)).map_err(
                |e: iggy_binary_protocol::consensus::ConsensusError| {
                    io::Error::new(io::ErrorKind::InvalidData, e.to_string())
                },
            )?;
            drained.push(msg);
        }

        // Write live entries to a temp file.
        let wal_path = self.storage.path();
        let tmp_path = wal_path.with_extension("wal.tmp");
        let tmp_guard = TmpFileGuard::new(tmp_path.clone());
        {
            let mut tmp = compio::fs::File::create(&tmp_path).await?;
            let mut write_pos: u64 = 0;
            for (header, old_offset) in &live {
                let size = header.size as usize;
                let buf = vec![0u8; size];
                let buf = self.storage.read_at(*old_offset, buf).await?;
                let (result, _buf) = tmp.write_all_at(buf, write_pos).await.into();
                result?;
                write_pos += size as u64;
            }
            tmp.sync_all().await?;
        }

        // Atomic replace.
        //
        // COMMIT POINT. From here on the on-disk WAL has been swapped
        // and the in-memory index has not yet been rebuilt for the
        // compacted layout. Every subsequent fallible step poisons the
        // journal before returning so the next `append` cannot write at
        // a stale `write_offset` into the orphaned old fd, and the next
        // `entry`/`entry_at` cannot serve offsets from the pre-drain
        // layout that no longer exist in the new file.
        compio::fs::rename(&tmp_path, wal_path).await?;
        // Rename has consumed `tmp_path`; nothing left to unlink.
        tmp_guard.defuse();

        // Fsync parent directory to make the rename durable. Without
        // this the rename can be lost across a power failure and the
        // pre-drain WAL re-presents on recovery; with the journal
        // poisoned the caller learns the drain is not durable instead
        // of silently proceeding.
        if let Some(parent) = wal_path.parent() {
            let dir = match compio::fs::File::open(parent).await {
                Ok(d) => d,
                Err(e) => {
                    return Err(self.poison("drain: open parent dir for fsync", e));
                }
            };
            if let Err(e) = dir.sync_all().await {
                return Err(self.poison("drain: parent dir fsync", e));
            }
        }

        // Reopen the file descriptor at the same path. A failure here
        // leaves the old fd (now pointing at the orphaned pre-rename
        // inode) live inside `FileStorage` with a stale `write_offset`;
        // poisoning prevents a follow-up `append` from writing bytes
        // into the orphan that disappear on the next process restart.
        if let Err(e) = self.storage.reopen().await {
            return Err(self.poison("drain: storage reopen after rename", e));
        }

        // Advance the snapshot watermark only AFTER the WAL rewrite is
        // durable (tmp create -> write -> fsync -> rename -> fsync parent
        // -> reopen). Advancing earlier would leave `snapshot_op` past
        // entries still present on disk on any `?` failure above, letting
        // a future `append()` pass the slot collision check at
        // `existing.op <= snapshot_op` and silently evict a live entry
        // from the index. The entry would survive on disk but become
        // unreachable, stalling `RetransmitPrepares` until view change.
        if end_op > self.snapshot_op.get() {
            self.snapshot_op.set(end_op);
        }

        // Rebuild offsets for the compacted layout and clear drained slots.
        let mut headers = self.headers.borrow_mut();
        let mut offsets = self.offsets.borrow_mut();
        let mut pos: u64 = 0;
        for (header, _) in &live {
            let slot = slot_for_op(header.op, self.slot_count);
            offsets[slot] = Some(pos);
            pos += u64::from(header.size);
        }
        for slot in 0..self.slot_count {
            if let Some(h) = &headers[slot]
                && ops.contains(&h.op)
            {
                headers[slot] = None;
                offsets[slot] = None;
            }
        }

        Ok(drained)
    }

    async fn append(&self, entry: Self::Entry) -> io::Result<()> {
        if let Some(state) = self.poisoned.get() {
            return Err(Self::poisoned_io_error(state));
        }
        let header = *entry.header();
        let slot = slot_for_op(header.op, self.slot_count);

        // Slot collision must be detected BEFORE `write_append + fsync`:
        // a post-fsync panic would leave bytes durably on disk, and the
        // recovery scan on the next boot would re-hit the same collision,
        // turning a single shard fault into a cluster-wide bootloop.
        {
            let headers = self.headers.borrow();
            if let Some(existing) = &headers[slot]
                && existing.op > self.snapshot_op.get()
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "journal slot collision: appending op {} would evict op {} \
                         which has not been snapshotted (snapshot_op={})",
                        header.op,
                        existing.op,
                        self.snapshot_op.get(),
                    ),
                ));
            }
        }

        // `truncate_or_fail` classifies any post-`pos` trailing region
        // larger than `MAX_ENTRY_SIZE` as mid-file damage rather than a
        // torn tail. That classification is only sound because this WAL
        // appends exactly one entry per `append` + fsync. A
        // batched-append regression that wrote more than one entry's
        // worth of bytes per fsync would let a crash leave a torn tail
        // many entries wide, and `truncate_or_fail` would silently
        // discard committed entries. Pin the invariant here so any such
        // regression trips in debug/test builds before reaching prod.
        debug_assert!(
            u64::from(header.size) <= MAX_ENTRY_SIZE,
            "WAL invariant: append must write at most MAX_ENTRY_SIZE bytes; \
             truncate_or_fail relies on this (got {} bytes)",
            header.size
        );
        // Hand the message's owned aligned buffer straight to compio: avoids a
        // per-append heap alloc + memcpy of up to MAX_ENTRY_SIZE bytes on the
        // consensus replicate hot path. `Owned` already implements `IoBuf`, so
        // `write_append` consumes it without copying, reserving its file
        // offset synchronously and returning it so the index records the
        // exact bytes written even when two appends interleave.
        let offset = self.storage.write_append(entry.into_owned()).await?;
        self.storage.fsync().await?;

        let mut headers = self.headers.borrow_mut();
        let mut offsets = self.offsets.borrow_mut();
        headers[slot] = Some(header);
        offsets[slot] = Some(offset);

        match self.last_op.get() {
            Some(current) if header.op > current => self.last_op.set(Some(header.op)),
            None => self.last_op.set(Some(header.op)),
            _ => {}
        }

        Ok(())
    }

    async fn entry(&self, header: &Self::Header) -> Option<Self::Entry> {
        if self.poisoned.get().is_some() {
            return None;
        }
        let (size, offset) = {
            let headers = self.headers.borrow();
            let offsets = self.offsets.borrow();
            let slot = slot_for_op(header.op, self.slot_count);
            let stored = headers[slot].as_ref()?;
            if stored.op != header.op {
                return None;
            }
            (stored.size as usize, offsets[slot]?)
        };

        let buffer = vec![0u8; size];
        let buffer = self.storage.read_at(offset, buffer).await.ok()?;
        Message::try_from(Owned::<MESSAGE_ALIGN>::copy_from_slice(&buffer)).ok()
    }
}

impl JournalHandle for PrepareJournal {
    type Storage = FileStorage;
    type Target = Self;

    fn handle(&self) -> &Self::Target {
        self
    }
}

#[cfg(test)]
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
mod tests {
    use super::*;
    use iggy_binary_protocol::consensus::Operation;
    use tempfile::tempdir;

    fn make_prepare(op: u64, body_size: usize) -> Message<PrepareHeader> {
        let total_size = HEADER_SIZE + body_size;
        let mut buffer = Owned::<MESSAGE_ALIGN>::zeroed(total_size);

        let header = bytemuck::checked::from_bytes_mut::<PrepareHeader>(
            &mut buffer.as_mut_slice()[..HEADER_SIZE],
        );
        header.size = total_size as u32;
        header.command = Command2::Prepare;
        header.op = op;
        header.operation = Operation::CreateStream;

        // Fill body with recognizable pattern
        for (i, byte) in buffer.as_mut_slice()[HEADER_SIZE..].iter_mut().enumerate() {
            *byte = (op as u8).wrapping_add(i as u8);
        }

        Message::try_from(buffer).unwrap()
    }

    #[compio::test]
    async fn open_empty_wal() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal.wal");
        let journal = PrepareJournal::open(&path, 0).await.unwrap();

        assert!(journal.last_op().is_none());
        assert!(journal.header(0).is_none());
    }

    #[compio::test]
    async fn append_and_read() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal.wal");
        let journal = PrepareJournal::open(&path, 0).await.unwrap();

        let msg1 = make_prepare(1, 64);
        let msg2 = make_prepare(2, 32);

        journal.append(msg1.deep_copy()).await.unwrap();
        journal.append(msg2.deep_copy()).await.unwrap();

        assert_eq!(journal.last_op(), Some(2));
        assert!(journal.header(1).is_some());
        assert!(journal.header(2).is_some());
        assert!(journal.header(3).is_none());

        let entry1 = journal.entry(msg1.header()).await.unwrap();
        assert_eq!(entry1.header().op, 1);
        assert_eq!(entry1.as_slice()[HEADER_SIZE..].len(), 64);

        let entry2 = journal.entry(msg2.header()).await.unwrap();
        assert_eq!(entry2.header().op, 2);
        assert_eq!(entry2.as_slice()[HEADER_SIZE..].len(), 32);
    }

    #[compio::test]
    async fn reopen_rebuilds_index() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal.wal");

        {
            let journal = PrepareJournal::open(&path, 0).await.unwrap();
            journal.append(make_prepare(1, 64)).await.unwrap();
            journal.append(make_prepare(2, 128)).await.unwrap();
            journal.append(make_prepare(3, 32)).await.unwrap();
            journal.storage.fsync().await.unwrap();
        }

        // Reopen and verify index is rebuilt
        let journal = PrepareJournal::open(&path, 0).await.unwrap();
        assert_eq!(journal.last_op(), Some(3));

        for op in 1..=3 {
            let header = *journal.header(op).unwrap();
            assert_eq!(header.op, op as u64);
            let entry = journal.entry_at(&header).await.unwrap().unwrap();
            assert_eq!(entry.header().op, op as u64);
        }
    }

    #[compio::test]
    async fn reopen_restores_write_cursor_so_next_append_does_not_overwrite() {
        // Recovery must restore the write cursor (FileStorage::write_offset)
        // to the end of the scanned entries. If it were left at 0, the first
        // post-boot append would reserve offset 0 and overwrite op 1 -- the
        // recovery-side counterpart of the offset-reservation append fix.
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal.wal");

        {
            let journal = PrepareJournal::open(&path, 0).await.unwrap();
            journal.append(make_prepare(1, 64)).await.unwrap();
            journal.append(make_prepare(2, 128)).await.unwrap();
            journal.storage.fsync().await.unwrap();
        }

        let journal = PrepareJournal::open(&path, 0).await.unwrap();
        let end_after_scan = journal.storage.file_len();
        assert_eq!(end_after_scan, (2 * HEADER_SIZE + 64 + 128) as u64);

        // A fresh append must land AFTER the recovered entries.
        journal.append(make_prepare(3, 32)).await.unwrap();
        assert_eq!(
            journal.storage.file_len(),
            end_after_scan + (HEADER_SIZE + 32) as u64
        );

        // All three entries remain intact and readable at distinct offsets.
        for (op, payload) in [(1u64, 64usize), (2, 128), (3, 32)] {
            let header = *journal.header(op as usize).unwrap();
            let entry = journal.entry(&header).await.unwrap();
            assert_eq!(entry.header().op, op);
            assert_eq!(entry.as_slice()[HEADER_SIZE..].len(), payload);
        }
    }

    #[compio::test]
    async fn corrupt_command_byte_truncates_on_reopen() {
        // Bit-flipped `Command2` discriminant: must truncate, not panic.
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal.wal");

        {
            let journal = PrepareJournal::open(&path, 0).await.unwrap();
            journal.append(make_prepare(1, 64)).await.unwrap();
            journal.append(make_prepare(2, 128)).await.unwrap();
            journal.storage.fsync().await.unwrap();
        }

        // Entry 2 at offset HEADER_SIZE+64=320; `offset_of!` guards against
        // future field reorders silently corrupting an unrelated byte.
        let entry_2_offset = (HEADER_SIZE + 64) as u64;
        let command_byte_offset =
            entry_2_offset + std::mem::offset_of!(PrepareHeader, command) as u64;
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
            file.seek(SeekFrom::Start(command_byte_offset)).unwrap();
            file.write_all(&[99u8]).unwrap(); // out of range for Command2
            file.sync_all().unwrap();
        }

        let journal = PrepareJournal::open(&path, 0).await.unwrap();
        assert_eq!(journal.last_op(), Some(1));
        assert!(journal.header(2).is_none());
        assert_eq!(journal.storage.file_len(), entry_2_offset);
    }

    #[compio::test]
    async fn truncated_entry_on_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal.wal");

        {
            let journal = PrepareJournal::open(&path, 0).await.unwrap();
            journal.append(make_prepare(1, 64)).await.unwrap();
            journal.append(make_prepare(2, 128)).await.unwrap();
            journal.storage.fsync().await.unwrap();
        }

        // Simulate crash: truncate the file to cut the second entry short
        {
            let storage = FileStorage::open(&path).await.unwrap();
            let full_len = storage.file_len();
            // Remove the last 10 bytes (partial second entry)
            storage.truncate(full_len - 10).await.unwrap();
            storage.fsync().await.unwrap();
        }

        // Reopen, should recover only the first entry
        let journal = PrepareJournal::open(&path, 0).await.unwrap();
        assert_eq!(journal.last_op(), Some(1));
        assert!(journal.header(2).is_none());

        let h1 = *journal.header(1).unwrap();
        let entry = journal.entry_at(&h1).await.unwrap().unwrap();
        assert_eq!(entry.header().op, 1);
    }

    #[compio::test]
    async fn truncate_or_fail_durably_repairs_torn_tail() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal.wal");

        let good_len = {
            let journal = PrepareJournal::open(&path, 0).await.unwrap();
            journal.append(make_prepare(1, 64)).await.unwrap();
            journal.append(make_prepare(2, 64)).await.unwrap();
            journal.storage.fsync().await.unwrap();
            journal.storage.file_len()
        };

        // Simulate a writer crash mid-append: junk bytes past the last
        // good entry, shorter than MAX_ENTRY_SIZE so it reads as a torn
        // tail rather than mid-file corruption.
        {
            let storage = FileStorage::open(&path).await.unwrap();
            storage.write_append(vec![0xAB_u8; 16]).await.unwrap();
            storage.fsync().await.unwrap();
        }

        // The recovery repair path truncates back to the last good entry.
        {
            let storage = FileStorage::open(&path).await.unwrap();
            truncate_or_fail(&storage, good_len, "torn tail test")
                .await
                .unwrap();
        }

        // The repair is durable: a fresh open sees the file ending
        // exactly at the last good entry, with no torn tail to re-repair.
        assert_eq!(std::fs::metadata(&path).unwrap().len(), good_len);
        let journal = PrepareJournal::open(&path, 0).await.unwrap();
        assert_eq!(journal.last_op(), Some(2));
        assert!(journal.header(3).is_none());
    }

    #[compio::test]
    async fn open_rejects_mid_file_corruption() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal.wal");

        // A corrupt header followed by more than MAX_ENTRY_SIZE of
        // trailing bytes is mid-file damage, not a torn final append. A
        // read-write open must hard-error instead of silently truncating
        // and discarding every committed entry after the corruption.
        {
            use std::io::Write;
            let mut file = std::fs::File::create(&path).unwrap();
            // All-0xFF is not a valid `Command2`/`Operation` bit pattern,
            // so `try_from_bytes` rejects the header.
            file.write_all(&[0xFF_u8; HEADER_SIZE]).unwrap();
            // Sparse-extend past MAX_ENTRY_SIZE so the corruption at pos 0
            // sits far from EOF without writing 64 MiB.
            file.set_len(MAX_ENTRY_SIZE + HEADER_SIZE as u64).unwrap();
            file.sync_all().unwrap();
        }

        let size_before = std::fs::metadata(&path).unwrap().len();
        let err = PrepareJournal::open(&path, 0).await;
        assert!(
            err.is_err(),
            "mid-file corruption must hard-error, not truncate"
        );
        let size_after = std::fs::metadata(&path).unwrap().len();
        assert_eq!(
            size_before, size_after,
            "a rejected mid-file scan must not truncate the WAL"
        );
    }

    #[compio::test]
    async fn iter_headers_from() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal.wal");
        let journal = PrepareJournal::open(&path, 0).await.unwrap();

        journal.append(make_prepare(1, 32)).await.unwrap();
        journal.append(make_prepare(2, 32)).await.unwrap();
        journal.append(make_prepare(3, 32)).await.unwrap();
        journal.append(make_prepare(5, 32)).await.unwrap();

        let from_2 = journal.iter_headers_from(2);
        assert_eq!(from_2.len(), 3);
        assert_eq!(from_2[0].op, 2);
        assert_eq!(from_2[1].op, 3);
        assert_eq!(from_2[2].op, 5);

        let from_4 = journal.iter_headers_from(4);
        assert_eq!(from_4.len(), 1);
        assert_eq!(from_4[0].op, 5);

        let from_10 = journal.iter_headers_from(10);
        assert!(from_10.is_empty());
    }

    #[compio::test]
    async fn previous_header_navigation() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal.wal");
        let journal = PrepareJournal::open(&path, 0).await.unwrap();

        journal.append(make_prepare(0, 32)).await.unwrap();
        journal.append(make_prepare(1, 32)).await.unwrap();

        let h1 = journal.header(1).unwrap();
        let h0 = journal.previous_header(&h1).unwrap();
        assert_eq!(h0.op, 0);
        assert!(journal.previous_header(&h0).is_none());
    }

    #[compio::test]
    async fn slot_wraparound_evicts_snapshotted_entry() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal.wal");
        let journal = PrepareJournal::open(&path, 0).await.unwrap();

        // Op 3 goes to slot 3
        journal.append(make_prepare(3, 32)).await.unwrap();
        // Mark op 3 as snapshotted - safe to evict
        journal.set_snapshot_op(3);
        // Op 3 + SLOT_COUNT goes to the same slot, evicting op 3
        let wraparound_op = 3 + SLOT_COUNT as u64;
        journal
            .append(make_prepare(wraparound_op, 32))
            .await
            .unwrap();

        // Op 3 is evicted from the index
        assert!(journal.header(3).is_none());
        // The new op is present
        assert!(journal.header(3 + SLOT_COUNT).is_some());
        assert_eq!(journal.last_op(), Some(3 + SLOT_COUNT as u64));
    }

    #[compio::test]
    async fn drain_shrinks_wal_and_preserves_live_entries() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal.wal");
        let journal = PrepareJournal::open(&path, 0).await.unwrap();

        // Append 5 entries
        for op in 1..=5 {
            journal.append(make_prepare(op, 64)).await.unwrap();
        }
        let size_before = journal.storage.file_len();

        // Drain entries 1-3
        let drained = journal.drain(1..=3).await.unwrap();
        assert_eq!(drained.len(), 3);
        assert_eq!(drained[0].header().op, 1);
        assert_eq!(drained[1].header().op, 2);
        assert_eq!(drained[2].header().op, 3);

        let size_after = journal.storage.file_len();
        assert!(
            size_after < size_before,
            "WAL should shrink after drain: {size_before} -> {size_after}"
        );

        // Drained entries are gone from the index
        for op in 1..=3 {
            assert!(
                journal.header(op as usize).is_none(),
                "op {op} should be removed"
            );
        }

        // Live entries are still readable
        for op in 4..=5 {
            let h = *journal.header(op as usize).unwrap();
            assert_eq!(h.op, op);
            let entry = journal.entry_at(&h).await.unwrap().unwrap();
            assert_eq!(entry.header().op, op);
            assert_eq!(entry.as_slice()[HEADER_SIZE..].len(), 64);
        }

        // Reopen and verify the drained WAL is valid
        drop(journal);
        let journal = PrepareJournal::open(&path, 3).await.unwrap();
        assert_eq!(journal.last_op(), Some(5));
        for op in 4..=5 {
            let h = *journal.header(op as usize).unwrap();
            let entry = journal.entry_at(&h).await.unwrap().unwrap();
            assert_eq!(entry.header().op, op);
            assert_eq!(entry.as_slice()[HEADER_SIZE..].len(), 64);
        }
    }

    #[compio::test]
    async fn append_errors_on_evicting_unsnapshotted_entry() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal.wal");
        let journal = PrepareJournal::open(&path, 0).await.unwrap();

        journal.append(make_prepare(3, 32)).await.unwrap();
        let size_after_first = journal.storage.file_len();

        // No snapshot taken: evicting op 3 must return an error WITHOUT
        // persisting any bytes; a post-fsync panic would otherwise wedge
        // recovery into a bootloop on the next open.
        let wraparound_op = 3 + SLOT_COUNT as u64;
        let err = journal
            .append(make_prepare(wraparound_op, 32))
            .await
            .expect_err("slot collision must surface as Err, not panic");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(
            err.to_string().contains("journal slot collision"),
            "unexpected error message: {err}"
        );
        assert_eq!(
            journal.storage.file_len(),
            size_after_first,
            "failed append must not persist bytes"
        );

        // Reopen: prior op still recoverable, no collision residue.
        drop(journal);
        let journal = PrepareJournal::open(&path, 0).await.unwrap();
        assert_eq!(journal.last_op(), Some(3));
    }

    const POISON_REASON: &str = "test: simulated post-rename failure";

    #[compio::test]
    async fn poisoned_journal_rejects_append() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal.wal");
        let journal = PrepareJournal::open(&path, 0).await.unwrap();
        journal.append(make_prepare(1, 32)).await.unwrap();
        let size_before = journal.storage.file_len();

        journal.force_poison(POISON_REASON);
        assert_eq!(journal.poison_reason(), Some(POISON_REASON));

        let err = journal
            .append(make_prepare(2, 32))
            .await
            .expect_err("poisoned journal must reject append");
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert!(
            err.to_string().contains(POISON_REASON),
            "missing poison reason: {err}"
        );
        assert_eq!(
            journal.storage.file_len(),
            size_before,
            "rejected append must not touch storage"
        );
    }

    #[compio::test]
    async fn poisoned_journal_rejects_drain() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal.wal");
        let journal = PrepareJournal::open(&path, 0).await.unwrap();
        for op in 1..=3 {
            journal.append(make_prepare(op, 32)).await.unwrap();
        }
        let size_before = journal.storage.file_len();

        journal.force_poison(POISON_REASON);

        let err = journal
            .drain(1..=2)
            .await
            .expect_err("poisoned journal must reject drain");
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert!(err.to_string().contains(POISON_REASON));
        assert_eq!(
            journal.storage.file_len(),
            size_before,
            "rejected drain must not rewrite WAL"
        );
    }

    #[compio::test]
    async fn poisoned_journal_returns_none_from_entry() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal.wal");
        let journal = PrepareJournal::open(&path, 0).await.unwrap();
        let msg = make_prepare(1, 32);
        journal.append(msg.deep_copy()).await.unwrap();

        journal.force_poison(POISON_REASON);

        let h = *msg.header();
        assert!(
            journal.entry(&h).await.is_none(),
            "poisoned journal must not serve cached on-disk reads"
        );
    }

    #[compio::test]
    async fn poisoned_journal_rejects_entry_at() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal.wal");
        let journal = PrepareJournal::open(&path, 0).await.unwrap();
        let msg = make_prepare(1, 32);
        journal.append(msg.deep_copy()).await.unwrap();

        journal.force_poison(POISON_REASON);

        let err = journal
            .entry_at(msg.header())
            .await
            .expect_err("poisoned entry_at must err");
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert!(err.to_string().contains(POISON_REASON));
    }

    #[compio::test]
    async fn in_memory_accessors_still_work_when_poisoned() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal.wal");
        let journal = PrepareJournal::open(&path, 0).await.unwrap();
        journal.append(make_prepare(1, 32)).await.unwrap();
        journal.append(make_prepare(2, 32)).await.unwrap();

        journal.force_poison(POISON_REASON);

        // In-memory state remains the last-known-good snapshot. Useful
        // for diagnostics/recovery; the IO paths are what would corrupt
        // the WAL, not these lookups.
        assert_eq!(journal.last_op(), Some(2));
        assert!(journal.header(1).is_some());
        assert!(journal.header(2).is_some());
        assert_eq!(journal.iter_headers_from(1).len(), 2);
    }

    #[compio::test]
    async fn concurrent_drains_are_refused_not_raced() {
        // Two drivers racing `drain()` share the one fixed `wal.tmp`:
        // `File::create` truncates the other racer's tmp mid-write, the
        // first rename consumes the path, and the loser surfaces ENOENT
        // (production: `forced checkpoint failed ... snapshot I/O error:
        // No such file or directory`) — or, with luckier timing, both
        // renames "succeed" over each other's bytes. Contract: exactly
        // one drain runs; a concurrent call is refused with
        // `ResourceBusy` before it touches the WAL; the journal stays
        // healthy either way.
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal.wal");
        let journal = PrepareJournal::open(&path, 0).await.unwrap();
        for op in 1..=8 {
            journal.append(make_prepare(op, 64)).await.unwrap();
        }

        let (a, b) = futures::join!(journal.drain(1..=4), journal.drain(1..=4));
        let results = [a.map(|v| v.len()), b.map(|v| v.len())];
        let (winners, losers): (Vec<_>, Vec<_>) =
            results.into_iter().partition(std::result::Result::is_ok);
        assert_eq!(
            winners.len(),
            1,
            "exactly one concurrent drain may perform the WAL rewrite: {winners:?} / {losers:?}"
        );
        assert_eq!(winners[0].as_ref().unwrap(), &4, "winner drains ops 1..=4");
        assert_eq!(
            losers[0].as_ref().unwrap_err().kind(),
            io::ErrorKind::ResourceBusy,
            "loser must be refused up front, not fail mid-flight on the shared tmp: {losers:?}"
        );

        // The journal must remain fully usable after the refused call.
        assert!(journal.poisoned.get().is_none(), "refusal must not poison");
        for op in 5..=8 {
            assert!(journal.header(op).is_some(), "live op {op} lost");
        }
        journal.append(make_prepare(9, 32)).await.unwrap();
        let h = *journal.header(9).unwrap();
        let entry = journal.entry_at(&h).await.unwrap().unwrap();
        assert_eq!(entry.header().op, 9);
    }
}
