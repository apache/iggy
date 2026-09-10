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

#![allow(clippy::future_not_send)]

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::io;
use std::path::{Path, PathBuf};

use futures::TryStreamExt;
use iggy_binary_protocol::batch::BATCH_HEADER_SIZE;
use iggy_binary_protocol::{Command, ConsensusHeader, Operation, PrepareHeader};
use server_common::{
    Message,
    iobuf::{Frozen, Owned},
};
use twox_hash::XxHash3_64;

use crate::durable_storage::{DiskStorage, DurableFile, DurableStorage, OpenMode};

mod segments;
pub use segments::SegmentPosition;
use segments::SegmentState;

pub const PARTITION_WAL_BLOCK_SIZE: usize = 4096;
pub const PARTITION_WAL_BYTES_MAX: u64 = 256 * 1024 * 1024;
pub const PARTITION_WAL_CAPACITY_MIN: u64 = 2 * (64 * 1024 * 1024 + 4096);
pub const PARTITION_WAL_CAPACITY_MAX: u64 = 4 * 1024 * 1024 * 1024;
const RECORD_PREFIX: usize = 32;
pub const PREPARE_BYTES_MAX: usize = 64 * 1024 * 1024;
const STATE_MAGIC: &[u8; 8] = b"IGGYWAL1";
const REFERENCE_STATE_MAGIC: &[u8; 8] = b"IGGYWAL2";
const INLINE_RECORD: u32 = 0;
const SEGMENT_RECORD: u32 = 1;
const RECORD_KIND_OFFSET: usize = 4;
const SEGMENT_REFERENCE_BYTES: usize = 4 * size_of::<u64>();
const REFERENCED_PREPARE_BYTES: usize = size_of::<PrepareHeader>() + SEGMENT_REFERENCE_BYTES;

/// A prepare body in a segment inode retained by the WAL.
///
/// The caller assigns a fresh generation whenever an offset-named segment is
/// replaced, and synchronizes the body through its writing handle before
/// submitting this reference. Referenced bytes must remain unchanged until the
/// WAL durably removes their operations.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentReference {
    pub generation: u64,
    pub start_offset: u64,
    pub position: u64,
    pub length: u64,
}

pub trait DurableAppend {
    /// # Errors
    /// Returns an error if persistence fails or the prepare does not extend the journal.
    fn append(&mut self, prepare: Frozen<4096>) -> impl Future<Output = io::Result<()>>;
}

pub struct PartitionPrepareJournal<S: DurableStorage = DiskStorage> {
    directory: PathBuf,
    file: S::File,
    storage: S,
    capacity: u64,
    state: JournalState,
    entries: BTreeMap<u64, StoredPrepare>,
    poisoned: bool,
    durable_head: u64,
    obsolete: VecDeque<PathBuf>,
    cleanup_directory_dirty: bool,
    recovered_prepares: Vec<Message<PrepareHeader>>,
    segment_files: BTreeMap<(u64, u64), S::File>,
    preallocate_segments: bool,
    retained_bytes: u64,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct JournalState {
    group: u64,
    incarnation: u64,
    generation: u64,
    length: u64,
    checkpoint: u64,
    checkpoint_checksum: u128,
    head: u64,
    head_checksum: u128,
    anchor_known: bool,
    checkpoint_prepare: bool,
    certified_log_view: Option<u32>,
    purge_generation: u64,
    purge_floor: u64,
    segment_references: bool,
    segment_storage: Option<SegmentState>,
}

#[derive(Clone, Copy)]
struct StoredPrepare {
    position: u64,
    length: usize,
    checksum: u128,
    reference: Option<SegmentReference>,
    next_offset: Option<u64>,
    retained_bytes: u64,
}

impl PartitionPrepareJournal {
    /// # Errors
    /// Returns an error on I/O failure or invalid durable history.
    pub async fn open(directory: &Path, group: u64, incarnation: u64) -> io::Result<Self> {
        Self::open_with_storage(directory, group, incarnation, DiskStorage).await
    }
}

impl<S: DurableStorage> PartitionPrepareJournal<S> {
    /// Open and verify the durably published partition history.
    /// The caller must first durably materialize the parent directory.
    ///
    /// # Errors
    /// Returns an error on I/O failure, invalid history, or a poisoned journal.
    pub async fn open_with_storage(
        directory: &Path,
        group: u64,
        incarnation: u64,
        storage: S,
    ) -> io::Result<Self> {
        Self::open_with_storage_and_capacity(
            directory,
            group,
            incarnation,
            storage,
            PARTITION_WAL_BYTES_MAX,
            false,
        )
        .await
    }

    /// Open history independently of the current admission capacity.
    ///
    /// # Errors
    /// Returns an error for invalid capacity or unverifiable durable history.
    pub async fn open_with_storage_and_capacity(
        directory: &Path,
        group: u64,
        incarnation: u64,
        storage: S,
        capacity: u64,
        preallocate_segments: bool,
    ) -> io::Result<Self> {
        if !(PARTITION_WAL_CAPACITY_MIN..=PARTITION_WAL_CAPACITY_MAX).contains(&capacity)
            || !capacity.is_multiple_of(PARTITION_WAL_BLOCK_SIZE as u64)
        {
            return Err(invalid(
                "partition WAL capacity is out of bounds or unaligned",
            ));
        }
        let parent = directory
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        if !storage.exists(parent).await? {
            return Err(invalid(
                "partition WAL parent must already be durably materialized",
            ));
        }
        storage.create_directories(directory).await?;
        storage.sync_directory(parent).await?;
        let state_path = directory.join("frontier");
        let existing = match storage.open(&state_path, OpenMode::Read).await {
            Ok(file) => {
                let bytes = file.read(0, PARTITION_WAL_BLOCK_SIZE).await?;
                let state = JournalState::decode(&bytes)?;
                if state.group != group || state.incarnation != incarnation {
                    return Err(invalid("partition WAL identity mismatch"));
                }
                Some(state)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => None,
            Err(error) => return Err(error),
        };
        if existing.is_none() {
            Self::validate_unpublished_history(&storage, directory).await?;
        }
        let state = existing.unwrap_or_else(|| JournalState {
            group,
            incarnation,
            certified_log_view: Some(0),
            ..JournalState::default()
        });
        let mode = if existing.is_none() {
            OpenMode::Create
        } else {
            OpenMode::ReadWrite
        };
        let file = storage
            .open(&data_path(directory, state.generation), mode)
            .await?;
        if file.length().await? < state.length {
            return Err(invalid("partition WAL lost acknowledged bytes"));
        }
        let mut journal = Self {
            directory: directory.to_path_buf(),
            file,
            storage,
            capacity,
            state,
            entries: BTreeMap::new(),
            poisoned: false,
            durable_head: state.head,
            obsolete: VecDeque::new(),
            cleanup_directory_dirty: false,
            recovered_prepares: Vec::new(),
            segment_files: BTreeMap::new(),
            preallocate_segments,
            retained_bytes: 0,
        };
        journal.recover_entries().await?;
        // Only bytes covered by the durable frontier could have released an ack.
        journal.file.truncate(state.length).await?;
        journal.file.sync().await?;
        journal.storage.sync_directory(directory).await?;
        if existing.is_none() {
            journal.publish(state).await?;
        }
        journal.discover_obsolete().await?;
        loop {
            let remaining = journal.obsolete.len();
            journal.cleanup_obsolete().await;
            if journal.obsolete.is_empty() || journal.obsolete.len() == remaining {
                break;
            }
        }
        journal.recover_segment_files().await?;
        journal.migrate_segment_prepares().await?;
        Ok(journal)
    }

    pub const fn certified_log_view(&self) -> Option<u32> {
        self.state.certified_log_view
    }

    /// Publish a complete canonical view only after its required head is durable.
    ///
    /// # Errors
    /// Returns an error if the expected history is absent or persistence fails.
    pub async fn certify_log_view(&mut self, view: u32, op: u64, checksum: u128) -> io::Result<()> {
        self.ensure_healthy()?;
        let matches = if op == self.state.checkpoint {
            (op == 0 && checksum == 0) || self.state.checkpoint_checksum == checksum
        } else {
            self.entries
                .get(&op)
                .is_some_and(|entry| entry.checksum == checksum)
        };
        if op > self.state.head || !matches {
            return Err(invalid("view certificate does not match WAL history"));
        }
        self.poisoned = true;
        self.file.sync().await?;
        let state = JournalState {
            certified_log_view: Some(view),
            ..self.state
        };
        self.publish(state).await?;
        self.state = state;
        self.durable_head = state.head;
        self.poisoned = false;
        Ok(())
    }

    #[must_use]
    pub const fn durable_op(&self) -> u64 {
        self.durable_head
    }

    #[must_use]
    pub const fn head(&self) -> u64 {
        self.state.head
    }

    #[must_use]
    pub const fn checkpoint_op(&self) -> u64 {
        self.state.checkpoint
    }

    #[must_use]
    pub const fn checkpoint_checksum(&self) -> Option<u128> {
        if self.state.anchor_known {
            Some(self.state.checkpoint_checksum)
        } else {
            None
        }
    }

    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.state.generation
    }

    #[must_use]
    pub const fn size_bytes(&self) -> u64 {
        self.state.length
    }

    /// Admission includes retained body bytes, even when the WAL stores references.
    pub const fn retained_bytes(&self) -> u64 {
        self.retained_bytes
    }

    #[must_use]
    pub fn contains(&self, header: &PrepareHeader) -> bool {
        if header.op > self.durable_head {
            return false;
        }
        self.entries
            .get(&header.op)
            .is_some_and(|entry| entry.checksum == header.checksum)
    }

    pub fn take_recovered_prepares(&mut self) -> Vec<Message<PrepareHeader>> {
        std::mem::take(&mut self.recovered_prepares)
    }

    /// Read the retained prepares in operation order.
    ///
    /// # Errors
    /// Returns an error on I/O failure, invalid history, or a poisoned journal.
    pub async fn prepares(&self) -> io::Result<Vec<Message<PrepareHeader>>> {
        let mut prepares = Vec::with_capacity(self.entries.len());
        for entry in self.entries.values() {
            let (_, length, prepare, _) = self.read_record(entry.position).await?;
            if length != entry.length {
                return Err(invalid("partition WAL index length mismatch"));
            }
            prepares.push(prepare);
        }
        Ok(prepares)
    }

    /// Durably replace the uncommitted suffix.
    ///
    /// # Errors
    /// Returns an error on I/O failure, invalid history, or a poisoned journal.
    pub async fn truncate_from(&mut self, from_op: u64) -> io::Result<()> {
        self.ensure_healthy()?;
        if from_op <= self.state.checkpoint {
            return Err(invalid("cannot truncate checkpointed partition operations"));
        }
        if from_op > self.state.head {
            return Ok(());
        }
        self.state.certified_log_view = None;
        self.rewrite(
            self.state.checkpoint,
            self.state.checkpoint_checksum,
            Some(from_op),
            None,
        )
        .await?;
        self.recover_segment_files().await
    }

    /// Synchronize required materialized files before removing their WAL coverage.
    /// Authorized deletions are excluded by the caller. A missing listed path
    /// does not prove deletion was authorized and cannot permit WAL reclamation.
    ///
    /// # Errors
    /// Returns an error if any file or directory barrier fails.
    pub async fn checkpoint_files(
        &mut self,
        through_op: u64,
        files: &[std::path::PathBuf],
        directories: &[std::path::PathBuf],
    ) -> io::Result<()> {
        futures::stream::iter(files.iter().map(Ok::<_, io::Error>))
            .try_for_each_concurrent(16, |path| async {
                self.storage.open(path, OpenMode::Read).await?.sync().await
            })
            .await?;
        for path in directories {
            self.storage.sync_directory(path).await?;
        }
        self.checkpoint(through_op).await
    }

    /// The caller has durably materialized every operation through this point.
    /// Reclaim a prefix already materialized durably by the caller.
    ///
    /// # Errors
    /// Returns an error on I/O failure, invalid history, or a poisoned journal.
    pub async fn checkpoint(&mut self, through_op: u64) -> io::Result<()> {
        self.ensure_healthy()?;
        if through_op <= self.state.checkpoint {
            return Ok(());
        }
        let checksum = self
            .entries
            .get(&through_op)
            .ok_or_else(|| invalid("unknown WAL checkpoint"))?
            .checksum;
        self.state.anchor_known = true;
        self.rewrite(through_op, checksum, None, None).await
    }

    /// Install an already durable replacement state, such as a completed transfer.
    /// Replace the journal with an already durable state-transfer checkpoint.
    ///
    /// # Errors
    /// Returns an error on I/O failure, invalid history, or a poisoned journal.
    pub async fn reset(&mut self, op: u64, checksum: Option<u128>) -> io::Result<()> {
        self.ensure_healthy()?;
        if self.state.segment_storage.is_some() {
            return Err(invalid(
                "segment reset requires the installed body boundary",
            ));
        }
        self.state.anchor_known = checksum.is_some();
        self.state.certified_log_view = None;
        self.rewrite(op, checksum.unwrap_or(0), Some(0), None).await
    }

    /// Install the committed prepare together with its materialized checkpoint.
    ///
    /// # Errors
    /// Returns an error for an invalid checkpoint prepare or a storage failure.
    pub async fn reset_with_prepare(&mut self, prepare: Frozen<4096>) -> io::Result<()> {
        self.ensure_healthy()?;
        if self.state.segment_storage.is_some() {
            return Err(invalid(
                "segment reset requires the installed body boundary",
            ));
        }
        let header = self.validate_checkpoint_prepare(&prepare)?;
        self.state.anchor_known = true;
        self.state.certified_log_view = None;
        self.rewrite(header.op, header.checksum, Some(0), Some((prepare, None)))
            .await
    }

    fn validate_checkpoint_prepare(&self, prepare: &Frozen<4096>) -> io::Result<PrepareHeader> {
        let header = *bytemuck::checked::try_from_bytes::<PrepareHeader>(
            prepare
                .as_slice()
                .get(..size_of::<PrepareHeader>())
                .ok_or_else(|| invalid("short checkpoint prepare"))?,
        )
        .map_err(|_| invalid("invalid checkpoint prepare"))?;
        if header.command != Command::Prepare
            || header.group != self.state.group
            || (header.checksum != 0 && header.identity_checksum() != header.checksum)
            || header.size as usize != prepare.len()
            || (header.checksum_body != 0
                && header.checksum_body
                    != u128::from(XxHash3_64::oneshot(
                        &prepare.as_slice()[size_of::<PrepareHeader>()..],
                    )))
        {
            return Err(invalid("invalid checkpoint prepare identity or checksum"));
        }
        Ok(header)
    }

    #[must_use]
    pub const fn purge_marker(&self) -> (u64, u64) {
        (self.state.purge_generation, self.state.purge_floor)
    }

    /// # Errors
    /// Returns an error if the purge marker cannot be published durably.
    pub async fn mark_purge(&mut self, generation: u64, floor: u64) -> io::Result<()> {
        self.ensure_healthy()?;
        if generation < self.state.purge_generation
            || (generation == self.state.purge_generation && floor <= self.state.purge_floor)
        {
            return Ok(());
        }
        if floor > self.state.head {
            return Err(invalid("purge exceeds WAL history"));
        }
        self.poisoned = true;
        self.file.sync().await?;
        let mut state = JournalState {
            purge_generation: generation,
            purge_floor: floor,
            ..self.state
        };
        if let Some(segments) = &mut state.segment_storage {
            segments.reset_position(SegmentPosition::default())?;
        }
        self.publish(state).await?;
        self.state = state;
        self.durable_head = state.head;
        self.segment_files.clear();
        self.poisoned = false;
        Ok(())
    }

    /// Make every buffered predecessor recoverable with one frontier publication.
    ///
    /// # Errors
    /// Returns an error unless the buffered prefix and its frontier are durable.
    pub async fn sync(&mut self) -> io::Result<()> {
        self.ensure_healthy()?;
        if self.durable_head == self.state.head {
            return Ok(());
        }
        self.poisoned = true;
        self.file.sync().await?;
        self.publish(self.state).await?;
        self.durable_head = self.state.head;
        self.poisoned = false;
        Ok(())
    }

    /// Append a predecessor without releasing a durable acknowledgment.
    ///
    /// # Errors
    /// Returns an error on write failure, capacity exhaustion, or a history conflict.
    pub async fn append_buffered(&mut self, prepare: Frozen<4096>) -> io::Result<()> {
        self.append_batch_buffered(std::slice::from_ref(&prepare))
            .await
    }

    /// Append a contiguous extent, validating every operation before allocation or I/O.
    ///
    /// # Errors
    /// Returns an error on invalid history, capacity exhaustion or failed write.
    pub async fn append_batch_buffered(&mut self, prepares: &[Frozen<4096>]) -> io::Result<()> {
        self.append_batch_inner(prepares, None).await
    }

    /// Retain durable segment bodies and append only their headers and locations.
    /// Non-message operations keep their inline payloads. The caller must first
    /// synchronize every referenced body through the handle that wrote it.
    ///
    /// Segment names in the parent directory remain offset-based. Hard links
    /// owned by this WAL keep referenced inodes alive across retention and purge.
    ///
    /// # Errors
    /// Returns an error for invalid references, history, capacity, or storage.
    pub async fn append_batch_referenced_buffered(
        &mut self,
        prepares: &[Frozen<4096>],
        references: &[Option<SegmentReference>],
    ) -> io::Result<()> {
        if references.len() != prepares.len() {
            return Err(invalid("partition WAL reference count mismatch"));
        }
        self.append_batch_inner(prepares, Some(references)).await
    }

    #[allow(clippy::too_many_lines)]
    async fn append_batch_inner(
        &mut self,
        prepares: &[Frozen<4096>],
        references: Option<&[Option<SegmentReference>]>,
    ) -> io::Result<()> {
        self.ensure_healthy()?;
        self.cleanup_obsolete().await;
        self.recovered_prepares.clear();
        let mut state = self.state;
        let mut retained_bytes = self.retained_bytes;
        let mut records: Vec<(u64, StoredPrepare, usize)> = Vec::with_capacity(prepares.len());
        for (index, prepare) in prepares.iter().enumerate() {
            let header = bytemuck::checked::try_from_bytes::<PrepareHeader>(
                prepare
                    .as_slice()
                    .get(..size_of::<PrepareHeader>())
                    .ok_or_else(|| invalid("short WAL prepare"))?,
            )
            .map_err(|_| invalid("invalid WAL prepare alignment"))?;
            if self
                .entries
                .get(&header.op)
                .is_some_and(|entry| entry.checksum == header.checksum)
                || records.last().is_some_and(|(op, entry, _)| {
                    *op == header.op && entry.checksum == header.checksum
                })
            {
                continue;
            }
            if header.op
                != state
                    .head
                    .checked_add(1)
                    .ok_or_else(|| invalid("WAL op exhausted"))?
                || (state.anchor_known && header.parent != state.head_checksum)
                || header.group != state.group
            {
                return Err(invalid("partition WAL append does not extend its history"));
            }
            let (reference, next_offset) = if let Some(segments) = &mut state.segment_storage {
                if references.is_some() {
                    return Err(invalid(
                        "externally assigned reference in owned segment storage",
                    ));
                }
                segments.reserve(header, prepare.as_slice())?
            } else {
                (references.and_then(|references| references[index]), None)
            };
            if let Some(reference) = reference {
                reference.validate(header, prepare.len())?;
            }
            let length =
                record_length(reference.map_or(prepare.len(), |_| REFERENCED_PREPARE_BYTES))?;
            let body_bytes = record_length(prepare.len())? as u64;
            if retained_bytes.saturating_add(body_bytes) > self.capacity {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "partition WAL requires checkpoint",
                ));
            }
            records.push((
                header.op,
                StoredPrepare {
                    position: state.length,
                    length,
                    checksum: header.checksum,
                    reference,
                    next_offset,
                    retained_bytes: body_bytes,
                },
                index,
            ));
            state.length += length as u64;
            retained_bytes += body_bytes;
            state.head = header.op;
            state.head_checksum = header.checksum;
            state.segment_references |= reference.is_some();
            if state
                .certified_log_view
                .is_some_and(|view| header.view > view)
            {
                state.certified_log_view = None;
            }
            if !state.anchor_known {
                state.checkpoint_checksum = header.parent;
                state.anchor_known = true;
            }
        }
        if records.is_empty() {
            return Ok(());
        }
        let mut extent = Owned::zeroed(
            usize::try_from(state.length - self.state.length)
                .map_err(|_| invalid("WAL extent overflow"))?,
        );
        for (_, record, index) in &records {
            let position = usize::try_from(record.position - self.state.length)
                .map_err(|_| invalid("WAL extent overflow"))?;
            encode_record_into(
                prepares[*index].as_slice(),
                record.reference,
                state.generation,
                &mut extent.as_mut_slice()[position..position + record.length],
            )?;
        }
        self.poisoned = true;
        self.write_segment_bodies(prepares, &records).await?;
        self.retain_segment_inodes(&records).await?;
        self.file.write_aligned(self.state.length, extent).await?;
        for (op, record, _) in records {
            self.entries.insert(op, record);
        }
        self.state = state;
        self.retained_bytes = retained_bytes;
        self.retain_active_segment_file();
        self.poisoned = false;
        Ok(())
    }

    async fn retain_segment_inodes(
        &self,
        records: &[(u64, StoredPrepare, usize)],
    ) -> io::Result<()> {
        let mut linked = false;
        for (_, record, _) in records {
            if let Some(reference) = record.reference {
                let retained = reference.path(&self.directory);
                if !self.storage.exists(&retained).await? {
                    let parent = self
                        .directory
                        .parent()
                        .ok_or_else(|| invalid("referenced segment has no partition directory"))?;
                    let source = parent.join(format!("{:020}.log", reference.start_offset));
                    self.storage.hard_link(&source, &retained).await?;
                    linked = true;
                }
            }
        }
        if linked {
            // A visible frontier must never name an inode whose last durable
            // directory entry a concurrent retention or purge can remove.
            self.storage.sync_directory(&self.directory).await?;
        }
        Ok(())
    }

    async fn recover_entries(&mut self) -> io::Result<()> {
        let state = self.state;
        let mut position = 0;
        let mut previous = state.checkpoint;
        let mut checksum = state.checkpoint_checksum;
        while position < state.length {
            let (header, length, prepare, reference) = self.read_record(position).await?;
            let next_offset = if state.segment_storage.is_some() && reference.is_some() {
                Some(segments::batch_next_offset(prepare.as_slice())?)
            } else {
                None
            };
            self.recovered_prepares.push(prepare);
            let checkpoint_prepare = position == 0 && state.checkpoint_prepare;
            if checkpoint_prepare {
                if header.op != state.checkpoint || header.checksum != state.checkpoint_checksum {
                    return Err(invalid("partition WAL checkpoint prepare mismatch"));
                }
            } else if header.op
                != previous
                    .checked_add(1)
                    .ok_or_else(|| invalid("WAL op overflow"))?
                || header.parent != checksum
            {
                return Err(invalid("partition WAL prepare chain is broken"));
            }
            self.entries.insert(
                header.op,
                StoredPrepare {
                    position,
                    length,
                    checksum: header.checksum,
                    reference,
                    next_offset,
                    retained_bytes: record_length(header.size as usize)? as u64,
                },
            );
            previous = header.op;
            checksum = header.checksum;
            position += length as u64;
        }
        if position != state.length || previous != state.head || checksum != state.head_checksum {
            return Err(invalid("partition WAL frontier disagrees with its data"));
        }
        self.retained_bytes = self
            .entries
            .values()
            .map(|entry| entry.retained_bytes)
            .sum();
        Ok(())
    }

    async fn discover_obsolete(&mut self) -> io::Result<()> {
        let retained = self.retained_segment_paths(&self.entries, self.state.segment_storage);
        for entry in self.storage.entries(&self.directory).await? {
            let Some(name) = entry.name.to_str() else {
                continue;
            };
            let generation = name
                .strip_prefix("prepares-")
                .and_then(|name| name.strip_suffix(".wal"))
                .and_then(|value| value.parse::<u64>().ok());
            if !entry.directory
                && (generation.is_some_and(|generation| generation != self.state.generation)
                    || name == "frontier.tmp"
                    || (is_retained_segment_name(name)
                        && !retained.contains(&self.directory.join(&entry.name))))
            {
                self.obsolete.push_back(self.directory.join(entry.name));
            }
        }
        Ok(())
    }

    async fn cleanup_obsolete(&mut self) {
        let count = self.obsolete.len().min(16);
        for _ in 0..count {
            let Some(path) = self.obsolete.pop_front() else {
                break;
            };
            match self.storage.remove_file(&path).await {
                Ok(()) => self.cleanup_directory_dirty = true,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    self.cleanup_directory_dirty = true;
                }
                Err(error) => {
                    tracing::warn!(%error, path = %path.display(), "cannot remove obsolete partition WAL generation");
                    self.obsolete.push_back(path);
                }
            }
        }
        if self.cleanup_directory_dirty {
            match self.storage.sync_directory(&self.directory).await {
                Ok(()) => self.cleanup_directory_dirty = false,
                Err(error) => tracing::warn!(%error, "cannot synchronize partition WAL cleanup"),
            }
        }
    }

    async fn validate_unpublished_history(storage: &S, directory: &Path) -> io::Result<()> {
        for entry in storage.entries(directory).await? {
            if entry.directory || entry.name == "frontier.tmp" {
                continue;
            }
            // An interrupted first open can leave only its empty generation-zero file.
            // Any other history without its frontier may contain acknowledged data.
            if entry.name != "prepares-0.wal"
                || storage
                    .open(&directory.join(&entry.name), OpenMode::Read)
                    .await?
                    .length()
                    .await?
                    != 0
            {
                return Err(invalid(
                    "partition WAL history exists without its durable frontier",
                ));
            }
        }
        Ok(())
    }

    fn ensure_healthy(&self) -> io::Result<()> {
        if self.poisoned {
            Err(invalid(
                "partition WAL requires recovery after failed mutation",
            ))
        } else {
            Ok(())
        }
    }

    async fn read_record(
        &self,
        position: u64,
    ) -> io::Result<(
        PrepareHeader,
        usize,
        Message<PrepareHeader>,
        Option<SegmentReference>,
    )> {
        let (mut buffer, frame_length, reference) = self.read_encoded_record(position).await?;
        let length = buffer.as_slice().len();
        if let Some(reference) = reference {
            let payload = &buffer.as_slice()[RECORD_PREFIX..RECORD_PREFIX + frame_length];
            let header = bytemuck::checked::try_from_bytes::<PrepareHeader>(
                &payload[..size_of::<PrepareHeader>()],
            )
            .map_err(|_| invalid("invalid referenced prepare header"))?;
            let mut prepare = Owned::zeroed(header.size as usize);
            prepare.as_mut_slice()[..size_of::<PrepareHeader>()]
                .copy_from_slice(&payload[..size_of::<PrepareHeader>()]);
            buffer = self
                .storage
                .open(&reference.path(&self.directory), OpenMode::Read)
                .await?
                .read_aligned_tail(reference.position, prepare, size_of::<PrepareHeader>())
                .await?;
        } else {
            buffer
                .as_mut_slice()
                .copy_within(RECORD_PREFIX..RECORD_PREFIX + frame_length, 0);
            buffer.truncate(frame_length);
        }
        let message = Message::<PrepareHeader>::try_from(buffer)
            .map_err(|_| invalid("invalid partition WAL prepare"))?;
        let header = *message.header();
        if header.command != Command::Prepare
            || header.group != self.state.group
            || header.size as usize != message.as_slice().len()
        {
            return Err(invalid("partition WAL prepare identity mismatch"));
        }
        if reference.is_some()
            && ((header.checksum != 0 && header.identity_checksum() != header.checksum)
                || (header.checksum_body != 0
                    && header.checksum_body
                        != u128::from(XxHash3_64::oneshot(
                            &message.as_slice()[size_of::<PrepareHeader>()..],
                        ))))
        {
            return Err(invalid("referenced segment prepare checksum mismatch"));
        }
        Ok((header, length, message, reference))
    }

    async fn read_encoded_record(
        &self,
        position: u64,
    ) -> io::Result<(Owned<4096>, usize, Option<SegmentReference>)> {
        let prefix = self
            .file
            .read_aligned(position, PARTITION_WAL_BLOCK_SIZE)
            .await?;
        let frame_length = u32::from_le_bytes(
            prefix.as_slice()[..4]
                .try_into()
                .map_err(|_| invalid("invalid WAL prefix"))?,
        ) as usize;
        let length = record_length(frame_length)?;
        if position
            .checked_add(length as u64)
            .is_none_or(|end| end > self.state.length)
        {
            return Err(invalid("partition WAL record crosses durable frontier"));
        }
        let mut buffer = if length == PARTITION_WAL_BLOCK_SIZE {
            prefix
        } else {
            let mut bytes = Owned::zeroed(length);
            bytes.as_mut_slice()[..PARTITION_WAL_BLOCK_SIZE].copy_from_slice(prefix.as_slice());
            self.file
                .read_aligned_tail(
                    position + PARTITION_WAL_BLOCK_SIZE as u64,
                    bytes,
                    PARTITION_WAL_BLOCK_SIZE,
                )
                .await?
        };
        let bytes = buffer.as_mut_slice();
        let stored_hash = u64::from_le_bytes(
            bytes[16..24]
                .try_into()
                .map_err(|_| invalid("invalid WAL checksum"))?,
        );
        bytes[16..24].fill(0);
        if XxHash3_64::oneshot(&bytes[..RECORD_PREFIX + frame_length]) != stored_hash {
            return Err(invalid("partition WAL record checksum mismatch"));
        }
        let generation = u64::from_le_bytes(
            bytes[8..16]
                .try_into()
                .map_err(|_| invalid("invalid WAL generation"))?,
        );
        if generation != self.state.generation {
            return Err(invalid("partition WAL stale record generation"));
        }
        let kind = u32::from_le_bytes(
            bytes[RECORD_KIND_OFFSET..RECORD_KIND_OFFSET + size_of::<u32>()]
                .try_into()
                .map_err(|_| invalid("invalid WAL record kind"))?,
        );
        if kind != INLINE_RECORD && (kind != SEGMENT_RECORD || !self.state.segment_references) {
            return Err(invalid("unknown partition WAL record kind"));
        }
        let header = bytemuck::checked::try_from_bytes::<PrepareHeader>(
            &bytes[RECORD_PREFIX..RECORD_PREFIX + size_of::<PrepareHeader>()],
        )
        .map_err(|_| invalid("invalid partition WAL prepare"))?;
        header
            .validate()
            .map_err(|_| invalid("invalid partition WAL prepare"))?;
        if header.group != self.state.group
            || (kind == INLINE_RECORD && header.size as usize != frame_length)
        {
            return Err(invalid("partition WAL prepare identity mismatch"));
        }
        let reference = if kind == SEGMENT_RECORD {
            if frame_length != REFERENCED_PREPARE_BYTES {
                return Err(invalid("invalid segment reference record size"));
            }
            let reference = SegmentReference::decode(
                &bytes[RECORD_PREFIX + size_of::<PrepareHeader>()..RECORD_PREFIX + frame_length],
            )?;
            reference.validate(header, header.size as usize)?;
            Some(reference)
        } else {
            None
        };
        Ok((buffer, frame_length, reference))
    }

    async fn publish(&self, state: JournalState) -> io::Result<()> {
        let temporary = self.directory.join("frontier.tmp");
        let mut file = self.storage.open(&temporary, OpenMode::Create).await?;
        file.write(0, state.encode()).await?;
        file.sync().await?;
        self.storage
            .rename(&temporary, &self.directory.join("frontier"))
            .await?;
        self.storage.sync_directory(&self.directory).await
    }

    #[allow(clippy::too_many_lines)]
    async fn rewrite(
        &mut self,
        checkpoint: u64,
        checksum: u128,
        truncate: Option<u64>,
        checkpoint_prepare: Option<(Frozen<4096>, Option<SegmentReference>)>,
    ) -> io::Result<()> {
        // A failed publication can leave a newer frontier visible on disk.
        // Poison until reopen instead of overwriting that possibly durable state.
        self.poisoned = true;
        let generation = self
            .state
            .generation
            .checked_add(1)
            .ok_or_else(|| invalid("WAL generation exhausted"))?;
        let mut file = self
            .storage
            .open(&data_path(&self.directory, generation), OpenMode::Create)
            .await?;
        let mut entries = BTreeMap::new();
        let mut state = JournalState {
            generation,
            checkpoint,
            checkpoint_checksum: checksum,
            head: checkpoint,
            head_checksum: checksum,
            length: 0,
            checkpoint_prepare: false,
            ..self.state
        };
        if let Some(segments) = &mut state.segment_storage {
            if checkpoint > self.state.checkpoint {
                segments.checkpoint = self
                    .segment_boundary(checkpoint)
                    .ok_or_else(|| invalid("missing segment checkpoint boundary"))?;
            }
            if let Some(from_op) = truncate.filter(|from_op| *from_op > 0) {
                segments.tail = self
                    .segment_boundary(from_op - 1)
                    .ok_or_else(|| invalid("missing segment rollback boundary"))?;
            }
        }
        if let Some((prepare, reference)) = checkpoint_prepare {
            let length =
                record_length(reference.map_or(prepare.len(), |_| REFERENCED_PREPARE_BYTES))?;
            let mut encoded = Owned::zeroed(length);
            encode_record_into(
                prepare.as_slice(),
                reference,
                generation,
                encoded.as_mut_slice(),
            )?;
            let length = encoded.as_slice().len();
            file.write_aligned(0, encoded).await?;
            entries.insert(
                checkpoint,
                StoredPrepare {
                    position: 0,
                    length,
                    checksum,
                    reference,
                    next_offset: None,
                    retained_bytes: record_length(prepare.len())? as u64,
                },
            );
            state.length = length as u64;
            state.checkpoint_prepare = true;
        }
        let previous_entries: Vec<_> = self
            .entries
            .iter()
            .map(|(&op, &entry)| (op, entry))
            .collect();
        for (op, entry) in previous_entries {
            if op < checkpoint || truncate.is_some_and(|from| op >= from) {
                continue;
            }
            let (record, payload_length, mut reference) =
                self.read_encoded_record(entry.position).await?;
            let payload = &record.as_slice()[RECORD_PREFIX..RECORD_PREFIX + payload_length];
            let mut next_offset = entry.next_offset;
            let mut converted = false;
            if reference.is_none()
                && op > state.purge_floor
                && let Some(segments) = &mut state.segment_storage
            {
                let header = bytemuck::checked::try_from_bytes::<PrepareHeader>(
                    &payload[..size_of::<PrepareHeader>()],
                )
                .map_err(|_| invalid("invalid prepare during segment migration"))?;
                if header.operation == Operation::SendMessages
                    && segments::decode_batch(payload)?.base_offset
                        >= segments.tail.position.next_offset
                {
                    (reference, next_offset) = segments.reserve(header, payload)?;
                    if let Some(reference) = reference {
                        self.write_segment_body(
                            reference,
                            &Frozen::from(Owned::copy_from_slice(payload)),
                        )
                        .await?;
                        converted = true;
                    }
                }
            }
            let encoded_length = if converted {
                REFERENCED_PREPARE_BYTES
            } else {
                payload_length
            };
            let mut encoded = Owned::zeroed(record_length(encoded_length)?);
            if converted {
                encode_record_into(payload, reference, generation, encoded.as_mut_slice())?;
            } else {
                encode_payload_into(
                    payload,
                    if reference.is_some() {
                        SEGMENT_RECORD
                    } else {
                        INLINE_RECORD
                    },
                    generation,
                    encoded.as_mut_slice(),
                )?;
            }
            let length = encoded.as_slice().len();
            file.write_aligned(state.length, encoded).await?;
            entries.insert(
                op,
                StoredPrepare {
                    position: state.length,
                    length,
                    checksum: entry.checksum,
                    reference,
                    next_offset,
                    retained_bytes: entry.retained_bytes,
                },
            );
            state.length += length as u64;
            state.checkpoint_prepare |= op == checkpoint;
            state.head = op;
            state.head_checksum = entry.checksum;
        }
        self.sync_segment_files().await?;
        file.sync().await?;
        self.storage.sync_directory(&self.directory).await?;
        self.publish(state).await?;
        let obsolete = data_path(&self.directory, self.state.generation);
        let retained = self.retained_segment_paths(&entries, state.segment_storage);
        self.obsolete.extend(
            self.retained_segment_paths(&self.entries, self.state.segment_storage)
                .into_iter()
                .filter(|path| !retained.contains(path)),
        );
        self.file = file;
        self.state = state;
        self.durable_head = state.head;
        self.entries = entries;
        self.retained_bytes = self
            .entries
            .values()
            .map(|entry| entry.retained_bytes)
            .sum();
        self.retain_active_segment_file();
        self.poisoned = false;
        self.obsolete.push_back(obsolete);
        self.cleanup_obsolete().await;
        Ok(())
    }
}

impl<S: DurableStorage> DurableAppend for PartitionPrepareJournal<S> {
    async fn append(&mut self, prepare: Frozen<4096>) -> io::Result<()> {
        self.append_buffered(prepare).await?;
        self.sync().await
    }
}

impl JournalState {
    fn encode(self) -> Vec<u8> {
        let mut bytes = vec![0; PARTITION_WAL_BLOCK_SIZE];
        bytes[..8].copy_from_slice(if self.segment_references {
            REFERENCE_STATE_MAGIC
        } else {
            STATE_MAGIC
        });
        for (offset, value) in [
            (16, self.group),
            (24, self.incarnation),
            (32, self.generation),
            (40, self.length),
            (48, self.checkpoint),
            (72, self.head),
        ] {
            bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
        }
        bytes[56..72].copy_from_slice(&self.checkpoint_checksum.to_le_bytes());
        bytes[80..96].copy_from_slice(&self.head_checksum.to_le_bytes());
        bytes[96] = u8::from(self.anchor_known);
        bytes[97] = u8::from(self.checkpoint_prepare);
        bytes[98] = u8::from(self.certified_log_view.is_some());
        bytes[120..124].copy_from_slice(&self.certified_log_view.unwrap_or(0).to_le_bytes());
        bytes[104..112].copy_from_slice(&self.purge_generation.to_le_bytes());
        bytes[112..120].copy_from_slice(&self.purge_floor.to_le_bytes());
        bytes[segments::SEGMENT_STATE_FLAG] = u8::from(self.segment_storage.is_some());
        if let Some(segments) = self.segment_storage {
            segments.encode(
                &mut bytes[segments::SEGMENT_STATE_OFFSET
                    ..segments::SEGMENT_STATE_OFFSET + segments::SEGMENT_STATE_BYTES],
            );
        }
        let checksum = XxHash3_64::oneshot(&bytes[16..]);
        bytes[8..16].copy_from_slice(&checksum.to_le_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() != PARTITION_WAL_BLOCK_SIZE
            || (&bytes[..8] != STATE_MAGIC && &bytes[..8] != REFERENCE_STATE_MAGIC)
        {
            return Err(invalid("unknown partition WAL frontier format"));
        }
        let read_u64 = |offset| -> io::Result<u64> {
            Ok(u64::from_le_bytes(
                bytes[offset..offset + 8]
                    .try_into()
                    .map_err(|_| invalid("invalid WAL frontier field"))?,
            ))
        };
        if read_u64(8)? != XxHash3_64::oneshot(&bytes[16..]) {
            return Err(invalid("partition WAL frontier checksum mismatch"));
        }
        let state = Self {
            segment_references: &bytes[..8] == REFERENCE_STATE_MAGIC,
            segment_storage: match bytes[segments::SEGMENT_STATE_FLAG] {
                0 => None,
                1 if &bytes[..8] == REFERENCE_STATE_MAGIC => Some(SegmentState::decode(
                    &bytes[segments::SEGMENT_STATE_OFFSET
                        ..segments::SEGMENT_STATE_OFFSET + segments::SEGMENT_STATE_BYTES],
                )?),
                _ => return Err(invalid("invalid segment storage flag")),
            },
            group: read_u64(16)?,
            incarnation: read_u64(24)?,
            generation: read_u64(32)?,
            length: read_u64(40)?,
            checkpoint: read_u64(48)?,
            head: read_u64(72)?,
            anchor_known: match bytes[96] {
                0 => false,
                1 => true,
                _ => return Err(invalid("invalid WAL anchor flag")),
            },
            checkpoint_prepare: match bytes[97] {
                0 => false,
                1 => true,
                _ => return Err(invalid("invalid checkpoint prepare flag")),
            },
            certified_log_view: match bytes[98] {
                0 => None,
                1 => Some(u32::from_le_bytes(
                    bytes[120..124]
                        .try_into()
                        .map_err(|_| invalid("invalid certified view"))?,
                )),
                _ => return Err(invalid("invalid certified view flag")),
            },
            purge_generation: read_u64(104)?,
            purge_floor: read_u64(112)?,
            checkpoint_checksum: u128::from_le_bytes(
                bytes[56..72]
                    .try_into()
                    .map_err(|_| invalid("invalid checkpoint checksum"))?,
            ),
            head_checksum: u128::from_le_bytes(
                bytes[80..96]
                    .try_into()
                    .map_err(|_| invalid("invalid head checksum"))?,
            ),
        };
        if state.length > PARTITION_WAL_CAPACITY_MAX
            || !state.length.is_multiple_of(PARTITION_WAL_BLOCK_SIZE as u64)
            || state.head < state.checkpoint
            || (!state.anchor_known && state.head != state.checkpoint)
            || (state.checkpoint_prepare
                && (state.checkpoint == 0 || state.length == 0 || !state.anchor_known))
        {
            return Err(invalid("invalid partition WAL frontier bounds"));
        }
        Ok(state)
    }
}

/// Padded size of a prepare record, including its envelope.
///
/// # Errors
/// Returns an error for a frame outside the protocol size bounds.
pub fn record_length(frame_length: usize) -> io::Result<usize> {
    if !(size_of::<PrepareHeader>()..=PREPARE_BYTES_MAX).contains(&frame_length) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "partition prepare size {frame_length} exceeds or falls below the supported range {}..={PREPARE_BYTES_MAX} bytes",
                size_of::<PrepareHeader>()
            ),
        ));
    }
    Ok((RECORD_PREFIX + frame_length).next_multiple_of(PARTITION_WAL_BLOCK_SIZE))
}

fn encode_record_into(
    prepare: &[u8],
    reference: Option<SegmentReference>,
    generation: u64,
    bytes: &mut [u8],
) -> io::Result<()> {
    if let Some(reference) = reference {
        let mut payload = [0; REFERENCED_PREPARE_BYTES];
        payload[..size_of::<PrepareHeader>()]
            .copy_from_slice(&prepare[..size_of::<PrepareHeader>()]);
        reference.encode(&mut payload[size_of::<PrepareHeader>()..]);
        encode_payload_into(&payload, SEGMENT_RECORD, generation, bytes)
    } else {
        encode_payload_into(prepare, INLINE_RECORD, generation, bytes)
    }
}

fn encode_payload_into(
    payload: &[u8],
    kind: u32,
    generation: u64,
    bytes: &mut [u8],
) -> io::Result<()> {
    let length = u32::try_from(payload.len()).map_err(|_| invalid("oversized prepare"))?;
    bytes[..4].copy_from_slice(&length.to_le_bytes());
    bytes[RECORD_KIND_OFFSET..RECORD_KIND_OFFSET + size_of::<u32>()]
        .copy_from_slice(&kind.to_le_bytes());
    bytes[8..16].copy_from_slice(&generation.to_le_bytes());
    bytes[RECORD_PREFIX..RECORD_PREFIX + payload.len()].copy_from_slice(payload);
    let checksum = XxHash3_64::oneshot(&bytes[..RECORD_PREFIX + payload.len()]);
    bytes[16..24].copy_from_slice(&checksum.to_le_bytes());
    Ok(())
}

impl SegmentReference {
    fn validate(self, header: &PrepareHeader, frame_length: usize) -> io::Result<()> {
        if header.command != Command::Prepare
            || header.operation != Operation::SendMessages
            || header.size as usize != frame_length
            || !(size_of::<PrepareHeader>() + BATCH_HEADER_SIZE..=PREPARE_BYTES_MAX)
                .contains(&frame_length)
            || self.length != (frame_length - size_of::<PrepareHeader>()) as u64
            || self.position.checked_add(self.length).is_none()
        {
            return Err(invalid("invalid partition WAL segment reference"));
        }
        Ok(())
    }

    fn path(self, directory: &Path) -> PathBuf {
        directory.join(format!(
            "segment-{}-{}.log",
            self.generation, self.start_offset
        ))
    }

    fn encode(self, bytes: &mut [u8]) {
        for (field, value) in bytes
            .as_chunks_mut::<{ size_of::<u64>() }>()
            .0
            .iter_mut()
            .zip([
                self.generation,
                self.start_offset,
                self.position,
                self.length,
            ])
        {
            field.copy_from_slice(&value.to_le_bytes());
        }
    }

    fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() != SEGMENT_REFERENCE_BYTES {
            return Err(invalid("invalid partition WAL segment reference size"));
        }
        let mut fields = [0; SEGMENT_REFERENCE_BYTES / size_of::<u64>()];
        for (field, bytes) in fields
            .iter_mut()
            .zip(bytes.as_chunks::<{ size_of::<u64>() }>().0)
        {
            *field = u64::from_le_bytes(*bytes);
        }
        let [generation, start_offset, position, length] = fields;
        Ok(Self {
            generation,
            start_offset,
            position,
            length,
        })
    }
}

fn referenced_segments(
    entries: &BTreeMap<u64, StoredPrepare>,
    directory: &Path,
) -> BTreeSet<PathBuf> {
    entries
        .values()
        .filter_map(|entry| entry.reference.map(|reference| reference.path(directory)))
        .collect()
}

fn is_retained_segment_name(name: &str) -> bool {
    name.strip_prefix("segment-")
        .and_then(|name| name.strip_suffix(".log"))
        .and_then(|name| name.split_once('-'))
        .is_some_and(|(generation, start_offset)| {
            generation.parse::<u64>().is_ok() && start_offset.parse::<u64>().is_ok()
        })
}

fn data_path(directory: &Path, generation: u64) -> PathBuf {
    directory.join(format!("prepares-{generation}.wal"))
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use compio::fs::File;
    use compio::io::AsyncWriteAtExt;
    use iggy_binary_protocol::Operation;
    use iggy_binary_protocol::batch::BatchHeader;
    use tempfile::tempdir;

    #[cfg(target_os = "linux")]
    use std::os::unix::fs::MetadataExt;

    const REFERENCE_TEST_BODY_BYTES: usize = 1024 * 1024;
    #[cfg(target_os = "linux")]
    const FILE_BLOCK_BYTES: u64 = 512;

    #[compio::test]
    async fn referenced_bodies_survive_retention_and_checkpoint_without_wal_copies() {
        let partition = tempdir().unwrap();
        let directory = partition.path().join("prepares-7");
        let mut journal = PartitionPrepareJournal::open(&directory, 42, 7)
            .await
            .unwrap();
        let first = sized_prepare(1, 0, size_of::<PrepareHeader>() + REFERENCE_TEST_BODY_BYTES);
        let second = sized_prepare(
            2,
            first.header().checksum,
            size_of::<PrepareHeader>() + REFERENCE_TEST_BODY_BYTES,
        );
        let first_reference = write_segment(partition.path(), 0, 0, &first).await;
        let second_reference = write_segment(partition.path(), 0, 1, &second).await;
        journal
            .append_batch_referenced_buffered(
                &[first.clone().into_frozen(), second.clone().into_frozen()],
                &[Some(first_reference), Some(second_reference)],
            )
            .await
            .unwrap();
        assert!(!journal.contains(second.header()));
        journal.sync().await.unwrap();
        assert_eq!(journal.size_bytes(), 2 * PARTITION_WAL_BLOCK_SIZE as u64);
        for offset in [0, 1] {
            DiskStorage
                .remove_file(&partition.path().join(format!("{offset:020}.log")))
                .await
                .unwrap();
        }
        DiskStorage.sync_directory(partition.path()).await.unwrap();
        drop(journal);

        let mut journal = PartitionPrepareJournal::open(&directory, 42, 7)
            .await
            .unwrap();
        let recovered = journal.prepares().await.unwrap();
        assert_eq!(recovered[0].as_slice(), first.as_slice());
        assert_eq!(recovered[1].as_slice(), second.as_slice());
        journal.checkpoint(2).await.unwrap();
        assert_eq!(journal.size_bytes(), PARTITION_WAL_BLOCK_SIZE as u64);
        assert!(!first_reference.path(&directory).exists());
        assert!(second_reference.path(&directory).exists());
        drop(journal);

        let journal = PartitionPrepareJournal::open(&directory, 42, 7)
            .await
            .unwrap();
        let recovered = journal.prepares().await.unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].as_slice(), second.as_slice());
    }

    #[compio::test]
    async fn truncating_a_reference_keeps_the_shared_segment_for_its_retained_prefix() {
        let partition = tempdir().unwrap();
        let directory = partition.path().join("prepares-7");
        let mut journal = PartitionPrepareJournal::open(&directory, 42, 7)
            .await
            .unwrap();
        let first = sized_prepare(1, 0, size_of::<PrepareHeader>() + REFERENCE_TEST_BODY_BYTES);
        let second = sized_prepare(
            2,
            first.header().checksum,
            size_of::<PrepareHeader>() + REFERENCE_TEST_BODY_BYTES,
        );
        let first_reference = write_segment(partition.path(), 0, 0, &first).await;
        let mut segment = DiskStorage
            .open(
                &partition.path().join("00000000000000000000.log"),
                OpenMode::ReadWrite,
            )
            .await
            .unwrap();
        DurableFile::write(
            &mut segment,
            first_reference.length,
            second.as_slice()[size_of::<PrepareHeader>()..].to_vec(),
        )
        .await
        .unwrap();
        DurableFile::sync(&segment).await.unwrap();
        let second_reference = SegmentReference {
            position: first_reference.length,
            ..first_reference
        };
        journal
            .append_batch_referenced_buffered(
                &[first.clone().into_frozen(), second.into_frozen()],
                &[Some(first_reference), Some(second_reference)],
            )
            .await
            .unwrap();
        journal.sync().await.unwrap();
        journal.truncate_from(2).await.unwrap();
        assert!(first_reference.path(&directory).exists());
        DurableFile::truncate(&segment, first_reference.length)
            .await
            .unwrap();
        let replacement = sized_prepare(
            2,
            first.header().checksum,
            size_of::<PrepareHeader>() + BATCH_HEADER_SIZE,
        );
        DurableFile::write(
            &mut segment,
            first_reference.length,
            replacement.as_slice()[size_of::<PrepareHeader>()..].to_vec(),
        )
        .await
        .unwrap();
        DurableFile::sync(&segment).await.unwrap();
        journal
            .append_batch_referenced_buffered(
                &[replacement.clone().into_frozen()],
                &[Some(SegmentReference {
                    length: BATCH_HEADER_SIZE as u64,
                    ..second_reference
                })],
            )
            .await
            .unwrap();
        journal.sync().await.unwrap();
        drop(journal);
        let journal = PartitionPrepareJournal::open(&directory, 42, 7)
            .await
            .unwrap();
        let recovered = journal.prepares().await.unwrap();
        assert_eq!(recovered[0].as_slice(), first.as_slice());
        assert_eq!(recovered[1].as_slice(), replacement.as_slice());
    }

    #[compio::test]
    async fn references_coexist_with_legacy_records_and_survive_purge_name_reuse() {
        let partition = tempdir().unwrap();
        let directory = partition.path().join("prepares-7");
        let mut journal = PartitionPrepareJournal::open(&directory, 42, 7)
            .await
            .unwrap();
        let legacy = prepare(1, 0);
        journal.append(legacy.clone().into_frozen()).await.unwrap();
        assert_eq!(
            &std::fs::read(directory.join("frontier")).unwrap()[..8],
            STATE_MAGIC
        );
        let first = sized_prepare(
            2,
            legacy.header().checksum,
            size_of::<PrepareHeader>() + REFERENCE_TEST_BODY_BYTES,
        );
        let first_reference = write_segment(partition.path(), 0, 0, &first).await;
        journal
            .append_batch_referenced_buffered(
                &[first.clone().into_frozen()],
                &[Some(first_reference)],
            )
            .await
            .unwrap();
        journal.sync().await.unwrap();
        assert_eq!(
            &std::fs::read(directory.join("frontier")).unwrap()[..8],
            REFERENCE_STATE_MAGIC
        );
        journal.mark_purge(1, 2).await.unwrap();
        DiskStorage
            .remove_file(&partition.path().join("00000000000000000000.log"))
            .await
            .unwrap();
        let second = sized_prepare(
            3,
            first.header().checksum,
            size_of::<PrepareHeader>() + BATCH_HEADER_SIZE,
        );
        let second_reference = write_segment(partition.path(), 1, 0, &second).await;
        journal
            .append_batch_referenced_buffered(
                &[second.clone().into_frozen()],
                &[Some(second_reference)],
            )
            .await
            .unwrap();
        journal.sync().await.unwrap();
        drop(journal);

        let mut journal = PartitionPrepareJournal::open(&directory, 42, 7)
            .await
            .unwrap();
        let recovered = journal.prepares().await.unwrap();
        assert_eq!(recovered[0].as_slice(), legacy.as_slice());
        assert_eq!(recovered[1].as_slice(), first.as_slice());
        assert_eq!(recovered[2].as_slice(), second.as_slice());
        assert_eq!(journal.purge_marker(), (1, 2));
        journal.truncate_from(3).await.unwrap();
        assert!(first_reference.path(&directory).exists());
        assert!(!second_reference.path(&directory).exists());
        drop(journal);
        let journal = PartitionPrepareJournal::open(&directory, 42, 7)
            .await
            .unwrap();
        assert_eq!(
            journal.prepares().await.unwrap()[1].as_slice(),
            first.as_slice()
        );
    }

    #[compio::test]
    async fn recovery_sweeps_unpublished_references_and_keeps_the_durable_prefix() {
        let partition = tempdir().unwrap();
        let directory = partition.path().join("prepares-7");
        let mut journal = PartitionPrepareJournal::open(&directory, 42, 7)
            .await
            .unwrap();
        let first = prepare(1, 0);
        journal.append(first.clone().into_frozen()).await.unwrap();
        let second = sized_prepare(
            2,
            first.header().checksum,
            size_of::<PrepareHeader>() + REFERENCE_TEST_BODY_BYTES,
        );
        let reference = write_segment(partition.path(), 0, 0, &second).await;
        journal
            .append_batch_referenced_buffered(&[second.into_frozen()], &[Some(reference)])
            .await
            .unwrap();
        assert!(reference.path(&directory).exists());
        drop(journal);

        let journal = PartitionPrepareJournal::open(&directory, 42, 7)
            .await
            .unwrap();
        assert_eq!(journal.head(), 1);
        assert_eq!(
            journal.prepares().await.unwrap()[0].as_slice(),
            first.as_slice()
        );
        assert!(!reference.path(&directory).exists());
    }

    #[compio::test]
    async fn recovery_refuses_missing_torn_or_corrupt_referenced_bodies_without_rewriting_evidence()
    {
        for damage in ["missing", "torn", "corrupt"] {
            let partition = tempdir().unwrap();
            let directory = partition.path().join("prepares-7");
            let mut journal = PartitionPrepareJournal::open(&directory, 42, 7)
                .await
                .unwrap();
            let first = sized_prepare(1, 0, size_of::<PrepareHeader>() + REFERENCE_TEST_BODY_BYTES);
            let reference = write_segment(partition.path(), 0, 0, &first).await;
            journal
                .append_batch_referenced_buffered(&[first.into_frozen()], &[Some(reference)])
                .await
                .unwrap();
            journal.sync().await.unwrap();
            drop(journal);
            let wal = std::fs::read(data_path(&directory, 0)).unwrap();
            let frontier = std::fs::read(directory.join("frontier")).unwrap();
            let retained = reference.path(&directory);
            if damage == "missing" {
                DiskStorage.remove_file(&retained).await.unwrap();
            } else {
                let mut file = DiskStorage
                    .open(&retained, OpenMode::ReadWrite)
                    .await
                    .unwrap();
                if damage == "torn" {
                    DurableFile::truncate(&file, reference.length - 1)
                        .await
                        .unwrap();
                } else {
                    DurableFile::write(&mut file, reference.length - 1, vec![0])
                        .await
                        .unwrap();
                }
                DurableFile::sync(&file).await.unwrap();
            }
            assert!(
                PartitionPrepareJournal::open(&directory, 42, 7)
                    .await
                    .is_err(),
                "{damage}"
            );
            assert_eq!(
                std::fs::read(data_path(&directory, 0)).unwrap(),
                wal,
                "{damage}"
            );
            assert_eq!(
                std::fs::read(directory.join("frontier")).unwrap(),
                frontier,
                "{damage}"
            );
        }
    }

    #[compio::test]
    async fn invalid_references_are_rejected_before_modifying_history() {
        let partition = tempdir().unwrap();
        let directory = partition.path().join("prepares-7");
        let mut journal = PartitionPrepareJournal::open(&directory, 42, 7)
            .await
            .unwrap();
        let first = sized_prepare(1, 0, size_of::<PrepareHeader>() + REFERENCE_TEST_BODY_BYTES)
            .into_frozen();
        let reference = SegmentReference {
            generation: 0,
            start_offset: 0,
            position: 0,
            length: REFERENCE_TEST_BODY_BYTES as u64,
        };
        for invalid_reference in [
            SegmentReference {
                length: reference.length - 1,
                ..reference
            },
            SegmentReference {
                position: u64::MAX,
                ..reference
            },
        ] {
            assert!(
                journal
                    .append_batch_referenced_buffered(
                        std::slice::from_ref(&first),
                        &[Some(invalid_reference)]
                    )
                    .await
                    .is_err()
            );
        }
        assert!(
            journal
                .append_batch_referenced_buffered(&[first], &[])
                .await
                .is_err()
        );
        assert_eq!(journal.head(), 0);
        assert_eq!(journal.size_bytes(), 0);
        assert!(!reference.path(&directory).exists());
        assert!(!journal.poisoned);
    }

    async fn write_segment(
        partition: &Path,
        generation: u64,
        start_offset: u64,
        prepare: &Message<PrepareHeader>,
    ) -> SegmentReference {
        let body = &prepare.as_slice()[size_of::<PrepareHeader>()..];
        let path = partition.join(format!("{start_offset:020}.log"));
        let mut file = DiskStorage.open(&path, OpenMode::Create).await.unwrap();
        DurableFile::write(&mut file, 0, body.to_vec())
            .await
            .unwrap();
        DurableFile::sync(&file).await.unwrap();
        DiskStorage.sync_directory(partition).await.unwrap();
        SegmentReference {
            generation,
            start_offset,
            position: 0,
            length: body.len() as u64,
        }
    }

    #[compio::test]
    async fn durable_frontier_recovers_only_covered_prepares() {
        let directory = tempdir().unwrap();
        let mut journal = PartitionPrepareJournal::open(directory.path(), 42, 7)
            .await
            .unwrap();
        let first = prepare(1, 0);
        let second = prepare(2, first.header().checksum);
        journal.append(first.clone().into_frozen()).await.unwrap();
        journal
            .append_buffered(second.clone().into_frozen())
            .await
            .unwrap();
        assert!(journal.contains(first.header()));
        assert!(!journal.contains(second.header()));
        drop(journal);
        let journal = PartitionPrepareJournal::open(directory.path(), 42, 7)
            .await
            .unwrap();
        assert_eq!(journal.head(), 1);
        assert_eq!(
            journal.prepares().await.unwrap()[0].as_slice(),
            first.as_slice()
        );
        assert_eq!(
            journal.file.metadata().await.unwrap().len(),
            journal.size_bytes()
        );
    }

    #[compio::test]
    async fn durable_append_covers_buffered_predecessors() {
        let directory = tempdir().unwrap();
        let mut journal = PartitionPrepareJournal::open(directory.path(), 42, 7)
            .await
            .unwrap();
        let first = prepare(1, 0);
        let second = prepare(2, first.header().checksum);
        journal
            .append_buffered(first.clone().into_frozen())
            .await
            .unwrap();
        journal.append(second.clone().into_frozen()).await.unwrap();
        assert!(journal.contains(first.header()));
        drop(journal);
        let journal = PartitionPrepareJournal::open(directory.path(), 42, 7)
            .await
            .unwrap();
        let entries = journal.prepares().await.unwrap();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[1].as_slice(), second.as_slice());
    }

    #[compio::test]
    async fn zeroed_interior_record_is_not_an_unwritten_tail() {
        let directory = tempdir().unwrap();
        let mut journal = PartitionPrepareJournal::open(directory.path(), 42, 7)
            .await
            .unwrap();
        let first = prepare(1, 0);
        let second = prepare(2, first.header().checksum);
        journal.append(first.into_frozen()).await.unwrap();
        journal.append(second.into_frozen()).await.unwrap();
        let length = journal.size_bytes();
        let (result, _) = journal
            .file
            .write_all_at(vec![0; PARTITION_WAL_BLOCK_SIZE], 0)
            .await
            .into();
        result.unwrap();
        journal.file.sync_data().await.unwrap();
        drop(journal);
        assert!(
            PartitionPrepareJournal::open(directory.path(), 42, 7)
                .await
                .is_err()
        );
        assert_eq!(
            File::open(data_path(directory.path(), 0))
                .await
                .unwrap()
                .metadata()
                .await
                .unwrap()
                .len(),
            length
        );
    }

    #[compio::test]
    async fn truncate_and_checkpoint_preserve_the_recoverable_history() {
        let directory = tempdir().unwrap();
        let mut journal = PartitionPrepareJournal::open(directory.path(), 42, 7)
            .await
            .unwrap();
        let first = prepare(1, 0);
        let second = prepare(2, first.header().checksum);
        let third = prepare(3, second.header().checksum);
        for entry in [&first, &second, &third] {
            journal.append(entry.clone().into_frozen()).await.unwrap();
        }
        journal.truncate_from(3).await.unwrap();
        journal.checkpoint(1).await.unwrap();
        drop(journal);
        let mut journal = PartitionPrepareJournal::open(directory.path(), 42, 7)
            .await
            .unwrap();
        assert_eq!(journal.checkpoint_op(), 1);
        assert_eq!(journal.head(), 2);
        assert_eq!(journal.prepares().await.unwrap().len(), 2);
        journal.append(third.into_frozen()).await.unwrap();
        assert_eq!(journal.head(), 3);
        assert!(journal.truncate_from(1).await.is_err());
    }

    #[compio::test]
    async fn missing_durable_tail_and_wrong_incarnation_are_rejected() {
        let directory = tempdir().unwrap();
        let mut journal = PartitionPrepareJournal::open(directory.path(), 42, 7)
            .await
            .unwrap();
        journal.append(prepare(1, 0).into_frozen()).await.unwrap();
        assert!(
            PartitionPrepareJournal::open(directory.path(), 42, 8)
                .await
                .is_err()
        );
        journal.file.set_len(0).await.unwrap();
        journal.file.sync_data().await.unwrap();
        drop(journal);
        assert!(
            PartitionPrepareJournal::open(directory.path(), 42, 7)
                .await
                .is_err()
        );
    }

    #[compio::test]
    async fn same_generation_purge_retry_persists_the_larger_floor() {
        let directory = tempdir().unwrap();
        let mut journal = PartitionPrepareJournal::open(directory.path(), 42, 7)
            .await
            .unwrap();
        let first = prepare(1, 0);
        let second = prepare(2, first.header().checksum);
        journal.append(first.into_frozen()).await.unwrap();
        journal.mark_purge(9, 1).await.unwrap();
        journal.append(second.into_frozen()).await.unwrap();
        journal.mark_purge(9, 2).await.unwrap();
        journal.mark_purge(9, 1).await.unwrap();
        drop(journal);
        let journal = PartitionPrepareJournal::open(directory.path(), 42, 7)
            .await
            .unwrap();
        assert_eq!(journal.purge_marker(), (9, 2));
    }

    #[compio::test]
    async fn view_certificate_requires_complete_matching_history() {
        let directory = tempdir().unwrap();
        let mut journal = PartitionPrepareJournal::open(directory.path(), 42, 7)
            .await
            .unwrap();
        let first = prepare(1, 0);
        let checksum = first.header().checksum;
        assert!(journal.certify_log_view(2, 1, checksum).await.is_err());
        journal.append(first.into_frozen()).await.unwrap();
        journal.certify_log_view(2, 1, checksum).await.unwrap();
        drop(journal);
        let mut journal = PartitionPrepareJournal::open(directory.path(), 42, 7)
            .await
            .unwrap();
        assert_eq!(journal.certified_log_view(), Some(2));
        journal.truncate_from(1).await.unwrap();
        drop(journal);
        let journal = PartitionPrepareJournal::open(directory.path(), 42, 7)
            .await
            .unwrap();
        assert_eq!(journal.certified_log_view(), None);
    }

    #[compio::test]
    async fn transferred_checkpoint_retains_its_prepare_after_restart() {
        let directory = tempdir().unwrap();
        let mut journal = PartitionPrepareJournal::open(directory.path(), 42, 7)
            .await
            .unwrap();
        let checkpoint = prepare(7, 1234);
        let checksum = checkpoint.header().checksum;
        let expected = checkpoint.as_slice().to_vec();
        journal
            .reset_with_prepare(checkpoint.into_frozen())
            .await
            .unwrap();
        journal.certify_log_view(2, 7, checksum).await.unwrap();
        drop(journal);
        let mut journal = PartitionPrepareJournal::open(directory.path(), 42, 7)
            .await
            .unwrap();
        assert_eq!(journal.checkpoint_op(), 7);
        assert_eq!(journal.certified_log_view(), Some(2));
        let prepares = journal.take_recovered_prepares();
        assert_eq!(prepares.len(), 1);
        assert_eq!(prepares[0].as_slice(), expected);
        journal
            .append(prepare(8, checksum).into_frozen())
            .await
            .unwrap();
    }

    #[compio::test]
    async fn purge_marker_does_not_promote_the_uncommitted_tail_to_checkpoint() {
        let directory = tempdir().unwrap();
        let mut journal = PartitionPrepareJournal::open(directory.path(), 42, 7)
            .await
            .unwrap();
        journal.append(prepare(1, 0).into_frozen()).await.unwrap();
        journal.mark_purge(9, 1).await.unwrap();
        drop(journal);
        let journal = PartitionPrepareJournal::open(directory.path(), 42, 7)
            .await
            .unwrap();
        assert_eq!(journal.purge_marker(), (9, 1));
        assert_eq!(journal.checkpoint_op(), 0);
        assert_eq!(journal.prepares().await.unwrap().len(), 1);
    }

    #[compio::test]
    async fn transferred_checkpoint_binds_the_next_accepted_parent() {
        let directory = tempdir().unwrap();
        let mut journal = PartitionPrepareJournal::open(directory.path(), 42, 7)
            .await
            .unwrap();
        journal.reset(7, None).await.unwrap();
        let next = prepare(8, 1234);
        journal.append(next.clone().into_frozen()).await.unwrap();
        drop(journal);
        let journal = PartitionPrepareJournal::open(directory.path(), 42, 7)
            .await
            .unwrap();
        assert_eq!(journal.checkpoint_op(), 7);
        assert_eq!(journal.head(), 8);
        assert!(journal.contains(next.header()));
    }

    #[compio::test]
    async fn reducing_capacity_recovers_existing_history_and_backpressures_new_appends() {
        let directory = tempdir().unwrap();
        let mut journal = PartitionPrepareJournal::open(directory.path(), 42, 7)
            .await
            .unwrap();
        let mut buffer = Owned::<4096>::zeroed(PREPARE_BYTES_MAX);
        let template = prepare(1, 0);
        buffer.as_mut_slice()[..size_of::<PrepareHeader>()]
            .copy_from_slice(&template.as_slice()[..size_of::<PrepareHeader>()]);
        let checksum_body = XxHash3_64::oneshot(&buffer.as_slice()[size_of::<PrepareHeader>()..]);
        let header = bytemuck::checked::from_bytes_mut::<PrepareHeader>(
            &mut buffer.as_mut_slice()[..size_of::<PrepareHeader>()],
        );
        header.size = u32::try_from(PREPARE_BYTES_MAX).unwrap();
        header.checksum_body = u128::from(checksum_body);
        header.checksum = header.identity_checksum();
        let first = Message::<PrepareHeader>::try_from(buffer).unwrap();
        let second = sized_prepare(2, first.header().checksum, PREPARE_BYTES_MAX);
        let third = prepare(3, second.header().checksum);
        journal.append_buffered(first.into_frozen()).await.unwrap();
        let fourth = prepare(4, third.header().checksum);
        journal.append_buffered(second.into_frozen()).await.unwrap();
        journal.append(third.into_frozen()).await.unwrap();
        drop(journal);
        let mut journal = PartitionPrepareJournal::open_with_storage_and_capacity(
            directory.path(),
            42,
            7,
            DiskStorage,
            PARTITION_WAL_CAPACITY_MIN,
            false,
        )
        .await
        .unwrap();
        assert_eq!(journal.head(), 3);
        assert!(journal.size_bytes() > PARTITION_WAL_CAPACITY_MIN);
        assert!(journal.append(fourth.clone().into_frozen()).await.is_err());
        journal.checkpoint(3).await.unwrap();
        journal.append(fourth.into_frozen()).await.unwrap();
        assert_eq!(journal.head(), 4);
    }

    #[test]
    fn record_padding_and_state_checksums_cover_the_format() {
        assert_eq!(record_length(256).unwrap(), 4096);
        assert_eq!(record_length(4096).unwrap(), 8192);
        assert!(record_length(PREPARE_BYTES_MAX + 1).is_err());
        let state = JournalState {
            group: 42,
            incarnation: 7,
            ..JournalState::default()
        };
        let mut bytes = state.encode();
        assert_eq!(JournalState::decode(&bytes).unwrap(), state);
        bytes[24] ^= 1;
        assert!(JournalState::decode(&bytes).is_err());
    }

    #[compio::test]
    async fn owned_segments_rollback_physical_tails_without_advancing_the_checkpoint() {
        const BODY_BYTES: usize = 8192;
        let partition = tempdir().unwrap();
        let directory = partition.path().join("prepares-7");
        let mut journal = PartitionPrepareJournal::open(&directory, 42, 7)
            .await
            .unwrap();
        journal
            .enable_segment_storage(SegmentPosition::default(), (2 * BODY_BYTES) as u64)
            .await
            .unwrap();
        let first = segment_prepare(1, 0, 0, BODY_BYTES);
        let second = segment_prepare(2, first.header().checksum, 1, BODY_BYTES);
        let third = segment_prepare(3, second.header().checksum, 2, BODY_BYTES);
        journal
            .append_batch_buffered(&[
                first.clone().into_frozen(),
                second.clone().into_frozen(),
                third.clone().into_frozen(),
            ])
            .await
            .unwrap();
        journal.sync().await.unwrap();
        assert_eq!(journal.size_bytes(), (3 * PARTITION_WAL_BLOCK_SIZE) as u64);
        assert_eq!(
            journal.segment_checkpoint(),
            Some(SegmentPosition::default())
        );
        journal.checkpoint(1).await.unwrap();
        let checkpoint = SegmentPosition {
            start_offset: 0,
            length: BODY_BYTES as u64,
            next_offset: 1,
        };
        assert_eq!(journal.segment_checkpoint(), Some(checkpoint));
        journal.truncate_from(2).await.unwrap();
        assert_eq!(
            std::fs::metadata(partition.path().join("00000000000000000000.log"))
                .unwrap()
                .len(),
            BODY_BYTES as u64
        );
        assert!(!partition.path().join("00000000000000000002.log").exists());
        let replacement = segment_prepare(2, first.header().checksum, 1, BODY_BYTES / 2);
        journal
            .append(replacement.clone().into_frozen())
            .await
            .unwrap();
        drop(journal);
        let journal = PartitionPrepareJournal::open(&directory, 42, 7)
            .await
            .unwrap();
        assert_eq!(journal.segment_checkpoint(), Some(checkpoint));
        let recovered = journal.prepares().await.unwrap();
        assert_eq!(recovered[0].as_slice(), first.as_slice());
        assert_eq!(recovered[1].as_slice(), replacement.as_slice());
    }

    #[compio::test]
    async fn owned_segments_recover_unpublished_bytes_and_keep_purged_prepare_bodies() {
        const BODY_BYTES: usize = 8192;
        let partition = tempdir().unwrap();
        let directory = partition.path().join("prepares-7");
        let mut journal = PartitionPrepareJournal::open(&directory, 42, 7)
            .await
            .unwrap();
        journal
            .enable_segment_storage(SegmentPosition::default(), (4 * BODY_BYTES) as u64)
            .await
            .unwrap();
        let first = segment_prepare(1, 0, 0, BODY_BYTES);
        let second = segment_prepare(2, first.header().checksum, 1, BODY_BYTES);
        journal.append(first.clone().into_frozen()).await.unwrap();
        journal.append_buffered(second.into_frozen()).await.unwrap();
        drop(journal);
        let mut journal = PartitionPrepareJournal::open(&directory, 42, 7)
            .await
            .unwrap();
        assert_eq!(
            std::fs::metadata(partition.path().join("00000000000000000000.log"))
                .unwrap()
                .len(),
            BODY_BYTES as u64
        );
        journal.mark_purge(1, 1).await.unwrap();
        let replacement = segment_prepare(2, first.header().checksum, 0, BODY_BYTES / 2);
        journal
            .append(replacement.clone().into_frozen())
            .await
            .unwrap();
        drop(journal);
        let journal = PartitionPrepareJournal::open(&directory, 42, 7)
            .await
            .unwrap();
        let recovered = journal.prepares().await.unwrap();
        assert_eq!(recovered[0].as_slice(), first.as_slice());
        assert_eq!(recovered[1].as_slice(), replacement.as_slice());
        assert_eq!(
            std::fs::read(partition.path().join("00000000000000000000.log")).unwrap(),
            replacement.as_slice()[size_of::<PrepareHeader>()..]
        );
    }

    #[cfg(target_os = "linux")]
    #[compio::test]
    async fn given_preallocated_segments_when_recovering_and_replacing_should_keep_reservations() {
        const SEGMENT_BYTES: u64 = 1024 * 1024;
        const BODY_BYTES: usize = 4096;

        for preallocate in [false, true] {
            let partition = tempdir().unwrap();
            let directory = partition.path().join("prepares-7");
            let mut journal = PartitionPrepareJournal::open_with_storage_and_capacity(
                &directory,
                42,
                7,
                DiskStorage,
                PARTITION_WAL_BYTES_MAX,
                preallocate,
            )
            .await
            .unwrap();
            journal
                .enable_segment_storage(SegmentPosition::default(), SEGMENT_BYTES)
                .await
                .unwrap();
            let first = segment_prepare(1, 0, 0, usize::try_from(SEGMENT_BYTES).unwrap());
            let second = segment_prepare(2, first.header().checksum, 1, BODY_BYTES);
            journal.append(first.clone().into_frozen()).await.unwrap();
            journal.append(second.clone().into_frozen()).await.unwrap();
            journal.checkpoint(1).await.unwrap();
            drop(journal);

            let active = partition.path().join("00000000000000000001.log");
            std::fs::OpenOptions::new()
                .write(true)
                .open(&active)
                .unwrap()
                .set_len(SEGMENT_BYTES + BODY_BYTES as u64)
                .unwrap();
            let mut journal = PartitionPrepareJournal::open_with_storage_and_capacity(
                &directory,
                42,
                7,
                DiskStorage,
                PARTITION_WAL_BYTES_MAX,
                preallocate,
            )
            .await
            .unwrap();
            let metadata = std::fs::metadata(&active).unwrap();
            assert_eq!(metadata.len(), BODY_BYTES as u64);
            assert_eq!(
                metadata.blocks() * FILE_BLOCK_BYTES >= SEGMENT_BYTES,
                preallocate,
                "recovery must preserve the reservation after trimming unpublished bytes"
            );
            assert_eq!(
                std::fs::read(&active).unwrap(),
                second.as_slice()[size_of::<PrepareHeader>()..]
            );
            let recovered = journal.prepares().await.unwrap();
            assert_eq!(recovered[0].as_slice(), first.as_slice());
            assert_eq!(recovered[1].as_slice(), second.as_slice());

            journal.truncate_from(2).await.unwrap();
            let replacement = segment_prepare(2, first.header().checksum, 1, 2 * BODY_BYTES);
            journal
                .append(replacement.clone().into_frozen())
                .await
                .unwrap();
            let metadata = std::fs::metadata(&active).unwrap();
            assert_eq!(metadata.len(), (2 * BODY_BYTES) as u64);
            assert_eq!(
                metadata.blocks() * FILE_BLOCK_BYTES >= SEGMENT_BYTES,
                preallocate,
                "replacement segment must follow the preallocation policy"
            );
            assert_eq!(
                std::fs::read(&active).unwrap(),
                replacement.as_slice()[size_of::<PrepareHeader>()..]
            );
        }
    }

    #[compio::test]
    async fn owned_segments_install_checkpoint_bodies_with_and_without_public_data() {
        const BODY_BYTES: usize = 8192;
        for materialized in [true, false] {
            let partition = tempdir().unwrap();
            let directory = partition.path().join("prepares-7");
            let mut journal = PartitionPrepareJournal::open_with_storage_and_capacity(
                &directory,
                42,
                7,
                DiskStorage,
                PARTITION_WAL_BYTES_MAX,
                true,
            )
            .await
            .unwrap();
            journal
                .enable_segment_storage(SegmentPosition::default(), (4 * BODY_BYTES) as u64)
                .await
                .unwrap();
            let original = segment_prepare(1, 0, 0, BODY_BYTES);
            journal.append(original.into_frozen()).await.unwrap();
            std::fs::remove_file(partition.path().join("00000000000000000000.log")).unwrap();
            let checkpoint = segment_prepare(7, 0, 5, BODY_BYTES);
            let initial = if materialized {
                write_segment(partition.path(), 0, 5, &checkpoint).await;
                SegmentPosition {
                    start_offset: 5,
                    length: BODY_BYTES as u64,
                    next_offset: 6,
                }
            } else {
                SegmentPosition {
                    start_offset: 6,
                    length: 0,
                    next_offset: 6,
                }
            };
            journal
                .reset_with_segment_checkpoint(
                    7,
                    Some(checkpoint.header().checksum),
                    Some(checkpoint.clone().into_frozen()),
                    initial,
                    (4 * BODY_BYTES) as u64,
                )
                .await
                .unwrap();
            assert_eq!(journal.size_bytes(), PARTITION_WAL_BLOCK_SIZE as u64);
            assert_eq!(journal.segment_checkpoint(), Some(initial));
            #[cfg(target_os = "linux")]
            {
                let installed = std::fs::metadata(
                    partition
                        .path()
                        .join(format!("{:020}.log", initial.start_offset)),
                )
                .unwrap();
                assert_eq!(installed.len(), initial.length);
                assert!(installed.blocks() * FILE_BLOCK_BYTES >= (4 * BODY_BYTES) as u64);
            }
            let checkpoint_reference = journal.segment_reference(checkpoint.header()).unwrap();
            if !materialized {
                assert!(!partition.path().join("00000000000000000005.log").exists());
            }
            drop(journal);
            let mut journal = PartitionPrepareJournal::open(&directory, 42, 7)
                .await
                .unwrap();
            assert_eq!(
                journal.prepares().await.unwrap()[0].as_slice(),
                checkpoint.as_slice()
            );
            assert_eq!(journal.segment_checkpoint(), Some(initial));
            let next = segment_prepare(8, checkpoint.header().checksum, 6, BODY_BYTES);
            journal.append(next.clone().into_frozen()).await.unwrap();
            journal.checkpoint(8).await.unwrap();
            assert_eq!(
                journal.prepares().await.unwrap()[0].as_slice(),
                next.as_slice()
            );
            assert_eq!(checkpoint_reference.path(&directory).exists(), materialized);
            let expected = SegmentPosition {
                length: initial.length + BODY_BYTES as u64,
                next_offset: 7,
                ..initial
            };
            assert_eq!(journal.segment_checkpoint(), Some(expected));
            drop(journal);
            let journal = PartitionPrepareJournal::open(&directory, 42, 7)
                .await
                .unwrap();
            assert_eq!(journal.segment_checkpoint(), Some(expected));
            assert_eq!(
                journal.prepares().await.unwrap()[0].as_slice(),
                next.as_slice()
            );
        }
    }

    #[compio::test]
    async fn owned_segments_preserve_overlapping_names_for_chain_validation() {
        const BODY_BYTES: usize = 8192;
        let partition = tempdir().unwrap();
        let directory = partition.path().join("prepares-7");
        let mut journal = PartitionPrepareJournal::open(&directory, 42, 7)
            .await
            .unwrap();
        journal
            .enable_segment_storage(SegmentPosition::default(), (4 * BODY_BYTES) as u64)
            .await
            .unwrap();
        let first = segment_prepare(1, 0, 0, BODY_BYTES);
        let second = segment_prepare(2, first.header().checksum, 1, BODY_BYTES);
        journal.append(first.into_frozen()).await.unwrap();
        journal.append(second.clone().into_frozen()).await.unwrap();
        journal.checkpoint(2).await.unwrap();
        drop(journal);
        let overlap = partition.path().join("00000000000000000001.log");
        let unpublished = partition.path().join("00000000000000000002.log");
        std::fs::write(&overlap, []).unwrap();
        std::fs::write(&unpublished, b"unpublished suffix").unwrap();
        let journal = PartitionPrepareJournal::open(&directory, 42, 7)
            .await
            .unwrap();
        assert!(
            overlap.exists(),
            "recovery must preserve the overlap for quarantine"
        );
        assert!(!unpublished.exists());
        assert_eq!(
            journal.prepares().await.unwrap()[0].as_slice(),
            second.as_slice()
        );
    }

    #[compio::test]
    async fn owned_segments_migrate_only_unmaterialized_legacy_prepares() {
        const BODY_BYTES: usize = 8192;
        let partition = tempdir().unwrap();
        let directory = partition.path().join("prepares-7");
        let mut journal = PartitionPrepareJournal::open(&directory, 42, 7)
            .await
            .unwrap();
        let first = segment_prepare(1, 0, 0, BODY_BYTES);
        let second = segment_prepare(2, first.header().checksum, 1, BODY_BYTES);
        journal.append(first.clone().into_frozen()).await.unwrap();
        journal.append(second.clone().into_frozen()).await.unwrap();
        write_segment(partition.path(), 0, 0, &first).await;
        let checkpoint = SegmentPosition {
            start_offset: 0,
            length: BODY_BYTES as u64,
            next_offset: 1,
        };
        journal
            .enable_segment_storage(checkpoint, (4 * BODY_BYTES) as u64)
            .await
            .unwrap();
        assert!(journal.segment_reference(first.header()).is_none());
        assert!(journal.segment_reference(second.header()).is_some());
        drop(journal);
        let journal = PartitionPrepareJournal::open(&directory, 42, 7)
            .await
            .unwrap();
        assert_eq!(journal.segment_checkpoint(), Some(checkpoint));
        assert_eq!(
            std::fs::metadata(partition.path().join("00000000000000000000.log"))
                .unwrap()
                .len(),
            (2 * BODY_BYTES) as u64
        );
        let recovered = journal.prepares().await.unwrap();
        assert_eq!(recovered[0].as_slice(), first.as_slice());
        assert_eq!(recovered[1].as_slice(), second.as_slice());
    }

    fn segment_prepare(
        op: u64,
        parent: u128,
        offset: u64,
        body_length: usize,
    ) -> Message<PrepareHeader> {
        let template = sized_prepare(op, parent, size_of::<PrepareHeader>() + body_length);
        let mut bytes = Owned::copy_from_slice(template.as_slice());
        let body = &mut bytes.as_mut_slice()[size_of::<PrepareHeader>()..];
        let mut batch = BatchHeader::new(42, 0, body_length as u64, 1);
        batch.base_offset = offset;
        batch.batch_checksum = batch.checksum_for_blob(&body[BATCH_HEADER_SIZE..]);
        batch.encode_into(body);
        let checksum = XxHash3_64::oneshot(body);
        let header = bytemuck::checked::from_bytes_mut::<PrepareHeader>(
            &mut bytes.as_mut_slice()[..size_of::<PrepareHeader>()],
        );
        header.checksum_body = u128::from(checksum);
        header.checksum = header.identity_checksum();
        Message::try_from(bytes).unwrap()
    }

    fn prepare(op: u64, parent: u128) -> Message<PrepareHeader> {
        sized_prepare(op, parent, size_of::<PrepareHeader>() + 16)
    }

    fn sized_prepare(op: u64, parent: u128, length: usize) -> Message<PrepareHeader> {
        let mut buffer = Owned::<4096>::zeroed(length);
        buffer.as_mut_slice()[size_of::<PrepareHeader>()..].fill(u8::try_from(op).unwrap());
        let checksum_body = XxHash3_64::oneshot(&buffer.as_slice()[size_of::<PrepareHeader>()..]);
        let length = buffer.as_slice().len();
        let header = bytemuck::checked::from_bytes_mut::<PrepareHeader>(
            &mut buffer.as_mut_slice()[..size_of::<PrepareHeader>()],
        );
        header.command = Command::Prepare;
        header.operation = Operation::SendMessages;
        header.group = 42;
        header.op = op;
        header.parent = parent;
        header.size = u32::try_from(length).unwrap();
        header.checksum_body = u128::from(checksum_body);
        header.checksum = header.identity_checksum();
        Message::try_from(buffer).unwrap()
    }
}
