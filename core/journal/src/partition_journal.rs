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

use crate::durable_storage::{DiskStorage, DurableFile, DurableStorage, OpenMode};
use futures::TryStreamExt;
use iggy_binary_protocol::{Command, PrepareHeader};
use server_common::{
    Message,
    iobuf::{Frozen, Owned},
};
use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::path::{Path, PathBuf};
use twox_hash::XxHash3_64;

pub const PARTITION_WAL_BLOCK_SIZE: usize = 4096;
pub const PARTITION_WAL_BYTES_MAX: u64 = 256 * 1024 * 1024;
pub const PARTITION_WAL_CAPACITY_MIN: u64 = 2 * (64 * 1024 * 1024 + 4096);
pub const PARTITION_WAL_CAPACITY_MAX: u64 = 4 * 1024 * 1024 * 1024;
const RECORD_PREFIX: usize = 32;
pub const PREPARE_BYTES_MAX: usize = 64 * 1024 * 1024;
const STATE_MAGIC: &[u8; 8] = b"IGGYWAL1";

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
}

#[derive(Clone, Copy)]
struct StoredPrepare {
    position: u64,
    length: usize,
    checksum: u128,
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
            let (_, length, prepare) = self.read_record(entry.position).await?;
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
        .await
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
        let message =
            Message::<PrepareHeader>::try_from(Owned::copy_from_slice(prepare.as_slice()))
                .map_err(|_| invalid("invalid checkpoint prepare"))?;
        let header = message.header();
        if header.group != self.state.group
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
        self.state.anchor_known = true;
        self.state.certified_log_view = None;
        self.rewrite(header.op, header.checksum, Some(0), Some(prepare))
            .await
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
        let state = JournalState {
            purge_generation: generation,
            purge_floor: floor,
            ..self.state
        };
        self.publish(state).await?;
        self.state = state;
        self.durable_head = state.head;
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
        self.ensure_healthy()?;
        self.cleanup_obsolete().await;
        self.recovered_prepares.clear();
        let mut state = self.state;
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
            let length = record_length(prepare.len())?;
            if state.length.saturating_add(length as u64) > self.capacity {
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
                },
                index,
            ));
            state.length += length as u64;
            state.head = header.op;
            state.head_checksum = header.checksum;
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
                state.generation,
                &mut extent.as_mut_slice()[position..position + record.length],
            )?;
        }
        self.poisoned = true;
        self.file.write_aligned(self.state.length, extent).await?;
        for (op, record, _) in records {
            self.entries.insert(op, record);
        }
        self.state = state;
        self.poisoned = false;
        Ok(())
    }

    async fn recover_entries(&mut self) -> io::Result<()> {
        let state = self.state;
        let mut position = 0;
        let mut previous = state.checkpoint;
        let mut checksum = state.checkpoint_checksum;
        while position < state.length {
            let (header, length, prepare) = self.read_record(position).await?;
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
                },
            );
            previous = header.op;
            checksum = header.checksum;
            position += length as u64;
        }
        if position != state.length || previous != state.head || checksum != state.head_checksum {
            return Err(invalid("partition WAL frontier disagrees with its data"));
        }
        Ok(())
    }

    async fn discover_obsolete(&mut self) -> io::Result<()> {
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
                    || name == "frontier.tmp")
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
    ) -> io::Result<(PrepareHeader, usize, Message<PrepareHeader>)> {
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
        bytes.copy_within(RECORD_PREFIX..RECORD_PREFIX + frame_length, 0);
        buffer.truncate(frame_length);
        let message = Message::<PrepareHeader>::try_from(buffer)
            .map_err(|_| invalid("invalid partition WAL prepare"))?;
        let header = *message.header();
        if header.command != Command::Prepare
            || header.group != self.state.group
            || header.size as usize != frame_length
        {
            return Err(invalid("partition WAL prepare identity mismatch"));
        }
        Ok((header, length, message))
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

    async fn rewrite(
        &mut self,
        checkpoint: u64,
        checksum: u128,
        truncate: Option<u64>,
        checkpoint_prepare: Option<Frozen<4096>>,
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
        if let Some(prepare) = checkpoint_prepare {
            let encoded = encode_record(prepare.as_slice(), generation)?;
            let length = encoded.as_slice().len();
            file.write_aligned(0, encoded).await?;
            entries.insert(
                checkpoint,
                StoredPrepare {
                    position: 0,
                    length,
                    checksum,
                },
            );
            state.length = length as u64;
            state.checkpoint_prepare = true;
        }
        for (&op, entry) in &self.entries {
            if op < checkpoint || truncate.is_some_and(|from| op >= from) {
                continue;
            }
            let prepare = self.read_record(entry.position).await?.2;
            let encoded = encode_record(prepare.as_slice(), generation)?;
            let length = encoded.as_slice().len();
            file.write_aligned(state.length, encoded).await?;
            entries.insert(
                op,
                StoredPrepare {
                    position: state.length,
                    length,
                    checksum: entry.checksum,
                },
            );
            state.length += length as u64;
            state.checkpoint_prepare |= op == checkpoint;
            state.head = op;
            state.head_checksum = entry.checksum;
        }
        file.sync().await?;
        self.storage.sync_directory(&self.directory).await?;
        self.publish(state).await?;
        let obsolete = data_path(&self.directory, self.state.generation);
        self.file = file;
        self.state = state;
        self.durable_head = state.head;
        self.entries = entries;
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
        bytes[..8].copy_from_slice(STATE_MAGIC);
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
        let checksum = XxHash3_64::oneshot(&bytes[16..]);
        bytes[8..16].copy_from_slice(&checksum.to_le_bytes());
        bytes
    }

    fn decode(bytes: &[u8]) -> io::Result<Self> {
        if bytes.len() != PARTITION_WAL_BLOCK_SIZE || &bytes[..8] != STATE_MAGIC {
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

fn encode_record(prepare: &[u8], generation: u64) -> io::Result<Owned<4096>> {
    let mut buffer = Owned::zeroed(record_length(prepare.len())?);
    encode_record_into(prepare, generation, buffer.as_mut_slice())?;
    Ok(buffer)
}

fn encode_record_into(prepare: &[u8], generation: u64, bytes: &mut [u8]) -> io::Result<()> {
    let length = u32::try_from(prepare.len()).map_err(|_| invalid("oversized prepare"))?;
    bytes[..4].copy_from_slice(&length.to_le_bytes());
    bytes[8..16].copy_from_slice(&generation.to_le_bytes());
    bytes[RECORD_PREFIX..RECORD_PREFIX + prepare.len()].copy_from_slice(prepare);
    let checksum = XxHash3_64::oneshot(&bytes[..RECORD_PREFIX + prepare.len()]);
    bytes[16..24].copy_from_slice(&checksum.to_le_bytes());
    Ok(())
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
    use tempfile::tempdir;

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
