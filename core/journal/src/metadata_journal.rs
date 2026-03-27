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
use bytes::Bytes;
use compio::io::AsyncWriteAtExt;
use iggy_binary_protocol::consensus::message::Message;
use iggy_binary_protocol::consensus::{Command2, PrepareHeader};
use std::cell::{Cell, Ref, RefCell};
use std::fmt;
use std::io;
use std::ops::RangeInclusive;
use std::path::Path;

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
/// entries in [`core::consensus::pipeline_prepare_queue_max`]. Because this number controls
/// how many committed but not yet snapshotted entries that the buffer can
/// hold. This may need to be tuned properly.
pub(crate) const SLOT_COUNT: usize = 1024;

/// Error type for journal operations.
#[derive(Debug)]
#[allow(clippy::module_name_repetitions)]
pub enum JournalError {
    Io(io::Error),
}

impl fmt::Display for JournalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(e) => write!(f, "journal I/O error: {e}"),
        }
    }
}

impl std::error::Error for JournalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(e) => Some(e),
        }
    }
}

impl From<io::Error> for JournalError {
    fn from(e: io::Error) -> Self {
        Self::Io(e)
    }
}

/// Persistent metadata journal backed by an append-only WAL file.
///
/// Each WAL entry is a raw `Message<PrepareHeader>`:
/// `[PrepareHeader: 256 bytes][body: header.size - 256 bytes]`
///
/// The in-memory index is a fixed-size slot array indexed by
/// `op % SLOT_COUNT`.
pub struct MetadataJournal {
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
}

impl fmt::Debug for MetadataJournal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetadataJournal")
            .field("write_offset", &self.storage.file_len())
            .field("last_op", &self.last_op.get())
            .finish_non_exhaustive()
    }
}

#[allow(clippy::cast_possible_truncation)]
const fn slot_for_op(op: u64) -> usize {
    op as usize % SLOT_COUNT
}

#[allow(clippy::cast_possible_truncation)]
impl MetadataJournal {
    /// Open the WAL file, scanning forward to rebuild the in-memory index.
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
        let storage = FileStorage::open(path).await?;
        let file_len = storage.file_len();
        let mut headers: Vec<Option<PrepareHeader>> = vec![None; SLOT_COUNT];
        let mut offsets: Vec<Option<u64>> = vec![None; SLOT_COUNT];
        let mut last_op: Option<u64> = None;
        let mut pos: u64 = 0;
        let mut header_buf = vec![0u8; HEADER_SIZE];

        while pos + HEADER_SIZE as u64 <= file_len {
            // Read the 256-byte header
            header_buf = storage.read_at(pos, header_buf).await?;
            let header: PrepareHeader =
                *bytemuck::checked::from_bytes::<PrepareHeader>(&header_buf);

            // Validate: must be a Prepare command with sane size
            if header.command != Command2::Prepare
                || (header.size as usize) < HEADER_SIZE
                || u64::from(header.size) > MAX_ENTRY_SIZE
            {
                // Corrupt or non-prepare entry, truncate here
                storage.truncate(pos).await?;
                break;
            }

            let entry_size = u64::from(header.size);

            // Check if the full entry fits
            if pos + entry_size > file_len {
                // Truncated entry at tail
                // This handles the case where crash happened during write and
                // only header was written and body was not. so we truncate the file to the start of the entry.
                storage.truncate(pos).await?;
                break;
            }

            let slot = slot_for_op(header.op);

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
            storage.truncate(pos).await?;
        }

        Ok(Self {
            storage,
            headers: RefCell::new(headers),
            offsets: RefCell::new(offsets),
            last_op: Cell::new(last_op),
            snapshot_op: Cell::new(snapshot_op),
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

    /// Async entry read for recovery.
    ///
    /// Returns `Ok(None)` if the op is not in the index.
    ///
    /// # Errors
    /// Returns an I/O error if the read fails or the entry is malformed.
    #[allow(clippy::future_not_send)]
    pub async fn entry_at(
        &self,
        header: &PrepareHeader,
    ) -> io::Result<Option<Message<PrepareHeader>>> {
        let (offset, size) = {
            let headers = self.headers.borrow();
            let offsets = self.offsets.borrow();
            let slot = slot_for_op(header.op);
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
        let msg = Message::from_bytes(Bytes::from(buf))
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
        Ok(Some(msg))
    }
}

#[allow(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    clippy::future_not_send
)]
impl Journal<FileStorage> for MetadataJournal {
    type Header = PrepareHeader;
    type Entry = Message<PrepareHeader>;
    type HeaderRef<'a> = Ref<'a, PrepareHeader>;

    fn header(&self, idx: usize) -> Option<Self::HeaderRef<'_>> {
        let headers = self.headers.borrow();
        Ref::filter_map(headers, |h| {
            let slot = slot_for_op(idx as u64);
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
            return Some(SLOT_COUNT);
        };
        let snapshot = self.snapshot_op.get();
        if last <= snapshot {
            return Some(SLOT_COUNT);
        }
        let used = (last - snapshot) as usize;
        Some(SLOT_COUNT.saturating_sub(used))
    }

    /// Remove entries with ops in `ops` from the journal,
    /// returning the removed entries sorted by op.
    ///
    /// Internally advances the snapshot watermark to `end_op` so that
    /// future appends treat drained slots as safe to overwrite. Rewrites
    /// the WAL file keeping only entries outside the drained range.
    async fn drain(&self, ops: RangeInclusive<u64>) -> io::Result<Vec<Self::Entry>> {
        let end_op = *ops.end();

        // Advance the snapshot watermark so future appends treat
        // drained ops as safe to overwrite.
        if end_op > self.snapshot_op.get() {
            self.snapshot_op.set(end_op);
        }

        // Partition slots into drained and live entries.
        let mut to_drain: Vec<(PrepareHeader, u64)> = Vec::new();
        let mut live: Vec<(PrepareHeader, u64)> = Vec::new();
        {
            let headers = self.headers.borrow();
            let offsets = self.offsets.borrow();
            for slot in 0..SLOT_COUNT {
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
            let msg = Message::from_bytes(Bytes::from(buf))
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))?;
            drained.push(msg);
        }

        // Write live entries to a temp file.
        let wal_path = self.storage.path();
        let tmp_path = wal_path.with_extension("wal.tmp");
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
        compio::fs::rename(&tmp_path, wal_path).await?;

        // Fsync parent directory to make the rename durable.
        if let Some(parent) = wal_path.parent() {
            let dir = compio::fs::File::open(parent).await?;
            dir.sync_all().await?;
        }

        // Reopen the file descriptor at the same path.
        self.storage.reopen().await?;

        // Rebuild offsets for the compacted layout and clear drained slots.
        let mut headers = self.headers.borrow_mut();
        let mut offsets = self.offsets.borrow_mut();
        let mut pos: u64 = 0;
        for (header, _) in &live {
            let slot = slot_for_op(header.op);
            offsets[slot] = Some(pos);
            pos += u64::from(header.size);
        }
        for slot in 0..SLOT_COUNT {
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
        let header = *entry.header();
        let offset = self.storage.file_len();

        self.storage.write_append(entry.into_inner()).await?;
        self.storage.fsync().await?;

        let slot = slot_for_op(header.op);
        let mut headers = self.headers.borrow_mut();
        let mut offsets = self.offsets.borrow_mut();

        if let Some(existing) = &headers[slot] {
            assert!(
                existing.op <= self.snapshot_op.get(),
                "journal slot collision: appending op {} would evict op {} \
                 which has not been snapshotted (snapshot_op={})",
                header.op,
                existing.op,
                self.snapshot_op.get(),
            );
        }

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
        let (size, offset) = {
            let headers = self.headers.borrow();
            let offsets = self.offsets.borrow();
            let slot = slot_for_op(header.op);
            let stored = headers[slot].as_ref()?;
            if stored.op != header.op {
                return None;
            }
            (stored.size as usize, offsets[slot]?)
        };

        let buffer = vec![0u8; size];
        let buffer = self.storage.read_at(offset, buffer).await.ok()?;
        Message::from_bytes(Bytes::from(buffer)).ok()
    }
}

impl JournalHandle for MetadataJournal {
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
    use bytes::BytesMut;
    use iggy_binary_protocol::consensus::Operation;
    use tempfile::tempdir;

    fn make_prepare(op: u64, body_size: usize) -> Message<PrepareHeader> {
        let total_size = HEADER_SIZE + body_size;
        let mut buffer = BytesMut::zeroed(total_size);

        let header = bytemuck::checked::from_bytes_mut::<PrepareHeader>(&mut buffer[..HEADER_SIZE]);
        header.size = total_size as u32;
        header.command = Command2::Prepare;
        header.op = op;
        header.operation = Operation::CreateStream;

        // Fill body with recognizable pattern
        for (i, byte) in buffer[HEADER_SIZE..].iter_mut().enumerate() {
            *byte = (op as u8).wrapping_add(i as u8);
        }

        Message::from_bytes(buffer.freeze()).unwrap()
    }

    #[compio::test]
    async fn open_empty_wal() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal.wal");
        let journal = MetadataJournal::open(&path, 0).await.unwrap();

        assert!(journal.last_op().is_none());
        assert!(journal.header(0).is_none());
    }

    #[compio::test]
    async fn append_and_read() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal.wal");
        let journal = MetadataJournal::open(&path, 0).await.unwrap();

        let msg1 = make_prepare(1, 64);
        let msg2 = make_prepare(2, 32);

        journal.append(msg1.clone()).await.unwrap();
        journal.append(msg2.clone()).await.unwrap();

        assert_eq!(journal.last_op(), Some(2));
        assert!(journal.header(1).is_some());
        assert!(journal.header(2).is_some());
        assert!(journal.header(3).is_none());

        let entry1 = journal.entry(msg1.header()).await.unwrap();
        assert_eq!(entry1.header().op, 1);
        assert_eq!(entry1.body().len(), 64);

        let entry2 = journal.entry(msg2.header()).await.unwrap();
        assert_eq!(entry2.header().op, 2);
        assert_eq!(entry2.body().len(), 32);
    }

    #[compio::test]
    async fn reopen_rebuilds_index() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal.wal");

        {
            let journal = MetadataJournal::open(&path, 0).await.unwrap();
            journal.append(make_prepare(1, 64)).await.unwrap();
            journal.append(make_prepare(2, 128)).await.unwrap();
            journal.append(make_prepare(3, 32)).await.unwrap();
            journal.storage.fsync().await.unwrap();
        }

        // Reopen and verify index is rebuilt
        let journal = MetadataJournal::open(&path, 0).await.unwrap();
        assert_eq!(journal.last_op(), Some(3));

        for op in 1..=3 {
            let header = *journal.header(op).unwrap();
            assert_eq!(header.op, op as u64);
            let entry = journal.entry_at(&header).await.unwrap().unwrap();
            assert_eq!(entry.header().op, op as u64);
        }
    }

    #[compio::test]
    async fn truncated_entry_on_reopen() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal.wal");

        {
            let journal = MetadataJournal::open(&path, 0).await.unwrap();
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
        let journal = MetadataJournal::open(&path, 0).await.unwrap();
        assert_eq!(journal.last_op(), Some(1));
        assert!(journal.header(2).is_none());

        let h1 = *journal.header(1).unwrap();
        let entry = journal.entry_at(&h1).await.unwrap().unwrap();
        assert_eq!(entry.header().op, 1);
    }

    #[compio::test]
    async fn iter_headers_from() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal.wal");
        let journal = MetadataJournal::open(&path, 0).await.unwrap();

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
        let journal = MetadataJournal::open(&path, 0).await.unwrap();

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
        let journal = MetadataJournal::open(&path, 0).await.unwrap();

        // Op 3 goes to slot 3
        journal.append(make_prepare(3, 32)).await.unwrap();
        // Mark op 3 as snapshotted — safe to evict
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
        let journal = MetadataJournal::open(&path, 0).await.unwrap();

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
            assert_eq!(entry.body().len(), 64);
        }

        // Reopen and verify the drained WAL is valid
        drop(journal);
        let journal = MetadataJournal::open(&path, 3).await.unwrap();
        assert_eq!(journal.last_op(), Some(5));
        for op in 4..=5 {
            let h = *journal.header(op as usize).unwrap();
            let entry = journal.entry_at(&h).await.unwrap().unwrap();
            assert_eq!(entry.header().op, op);
            assert_eq!(entry.body().len(), 64);
        }
    }

    #[compio::test]
    #[should_panic(expected = "journal slot collision")]
    async fn append_panics_on_evicting_unsnapshotted_entry() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("journal.wal");
        let journal = MetadataJournal::open(&path, 0).await.unwrap();

        journal.append(make_prepare(3, 32)).await.unwrap();
        // No snapshot taken, evicting op 3 must panic
        let wraparound_op = 3 + SLOT_COUNT as u64;
        journal
            .append(make_prepare(wraparound_op, 32))
            .await
            .unwrap();
    }
}
