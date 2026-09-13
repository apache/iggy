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

use std::collections::{BTreeMap, BTreeSet};
use std::io;
use std::path::{Path, PathBuf};

use iggy_binary_protocol::batch::BatchHeader;
use iggy_binary_protocol::{Operation, PrepareHeader};
use iggy_common::MAX_TOPIC_SEGMENT_SIZE;
use server_common::iobuf::{Frozen, IOV_MAX};

use super::{
    JournalState, PartitionPrepareJournal, SegmentReference, StoredPrepare, invalid, segment_path,
};
use crate::durable_storage::{DurableFile, DurableStorage, OpenMode};

pub(super) const SEGMENT_STATE_FLAG: usize = 99;
pub(super) const SEGMENT_STATE_OFFSET: usize = 128;
pub(super) const SEGMENT_STATE_BYTES: usize = 10 * size_of::<u64>();

const SEGMENT_RECOVERY_EXTENSION: &str = "log.tmp";

/// A whole-batch boundary. `next_offset` is the first offset after this prefix.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SegmentPosition {
    pub start_offset: u64,
    pub length: u64,
    pub next_offset: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct SegmentCursor {
    pub generation: u64,
    pub position: SegmentPosition,
}

/// Recovery materialization ends at `checkpoint`; live polls use the partition's
/// committed segment size, which can advance beyond it within the durable tail.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct SegmentState {
    pub max_size: u64,
    pub next_generation: u64,
    pub tail: SegmentCursor,
    pub checkpoint: SegmentCursor,
}

impl<S: DurableStorage> PartitionPrepareJournal<S> {
    /// Enable segment body ownership after recovering the legacy committed view.
    /// The initial boundary must describe durable materialized messages only.
    ///
    /// # Errors
    /// Returns an error for inconsistent boundaries, a `max_size` differing from an
    /// already enabled layout, or a failed storage barrier.
    pub async fn enable_segment_storage(
        &mut self,
        initial: SegmentPosition,
        max_size: u64,
    ) -> io::Result<()> {
        self.ensure_healthy()?;
        if let Some(segments) = self.state.segment_storage {
            if segments.max_size != max_size {
                return Err(invalid("segment size differs from the durable WAL layout"));
            }
            return Ok(());
        }
        if !valid_segment_size(max_size) || !initial.valid() {
            return Err(invalid("invalid initial segment boundary"));
        }
        let generation = self
            .entries
            .values()
            .filter_map(|entry| entry.reference)
            .map(|reference| reference.generation)
            .max()
            .map_or(Some(0), |generation| generation.checked_add(1))
            .ok_or_else(|| invalid("segment generation exhausted"))?;
        let cursor = SegmentCursor {
            generation,
            position: initial,
        };
        let segments = SegmentState {
            max_size,
            next_generation: generation
                .checked_add(1)
                .ok_or_else(|| invalid("segment generation exhausted"))?,
            tail: cursor,
            checkpoint: cursor,
        };
        let state = JournalState {
            segment_references: true,
            segment_storage: Some(segments),
            ..self.state
        };
        let migrate = self.segment_migration_needed(segments).await?;
        self.poisoned = true;
        if initial.length > 0 {
            self.open_segment_file(
                generation,
                initial.start_offset,
                initial.length,
                self.preallocate_segments.then_some(max_size),
            )
            .await?;
            self.sync_segment_files().await?;
        }
        self.file.sync().await?;
        // Upgrade before any uncommitted body reaches an offset-named file.
        self.publish(state).await?;
        self.state = state;
        self.durable_head = state.head;
        self.poisoned = false;
        if migrate {
            self.rewrite(
                self.state.checkpoint,
                self.state.checkpoint_checksum,
                None,
                None,
            )
            .await?;
        }
        Ok(())
    }

    pub const fn segment_checkpoint(&self) -> Option<SegmentPosition> {
        match self.state.segment_storage {
            Some(segments) => Some(segments.checkpoint.position),
            None => None,
        }
    }

    pub fn segment_reference(&self, header: &PrepareHeader) -> Option<SegmentReference> {
        if !self.contains(header) {
            return None;
        }
        self.entries
            .get(&header.op)
            .and_then(|entry| entry.reference)
    }

    /// Completed body writes at or above `from_op`, in operation order.
    /// These references can precede durable publication.
    pub fn written_segment_references(
        &self,
        from_op: u64,
    ) -> impl Iterator<Item = (u64, SegmentReference)> + '_ {
        self.entries
            .range(from_op..)
            .filter_map(|(&op, entry)| entry.reference.map(|reference| (op, reference)))
    }

    /// Install a transferred checkpoint after all replacement segment files are durable.
    /// The replacement establishes its own segment size; it does not extend the old layout.
    ///
    /// # Errors
    /// Returns an error if the checkpoint contradicts the installed bytes or a barrier fails.
    #[allow(clippy::too_many_lines)]
    pub async fn reset_with_segment_checkpoint(
        &mut self,
        op: u64,
        checksum: Option<u128>,
        prepare: Option<Frozen<4096>>,
        initial: SegmentPosition,
        max_size: u64,
    ) -> io::Result<()> {
        self.ensure_healthy()?;
        if !initial.valid() || !valid_segment_size(max_size) {
            return Err(invalid("invalid installed segment boundary"));
        }
        if let Some(prepare) = &prepare {
            let header = self.validate_checkpoint_prepare(prepare)?;
            if header.op != op || Some(header.checksum) != checksum {
                return Err(invalid("installed checkpoint prepare identity mismatch"));
            }
        }
        let generation = self
            .state
            .segment_storage
            .map_or(0, |segments| segments.next_generation);
        let cursor = SegmentCursor {
            generation,
            position: initial,
        };
        let mut segments = SegmentState {
            max_size,
            next_generation: generation
                .checked_add(1)
                .ok_or_else(|| invalid("segment generation exhausted"))?,
            tail: cursor,
            checkpoint: cursor,
        };
        let public = self
            .segment_directory()?
            .join(format!("{:020}.log", initial.start_offset));
        match self.storage.open(&public, OpenMode::Read).await {
            Ok(file) if file.length().await? != initial.length => {
                return Err(invalid(
                    "installed segment size differs from its checkpoint",
                ));
            }
            Ok(_) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound && initial.length == 0 => {}
            Err(error) => return Err(error),
        }
        self.poisoned = true;
        self.open_segment_file(generation, initial.start_offset, initial.length, None)
            .await?;
        let installed_file = self
            .segment_files
            .get(&(generation, initial.start_offset))
            .ok_or_else(|| invalid("installed segment handle is absent"))?;
        if installed_file.length().await? != initial.length {
            return Err(invalid(
                "installed segment size differs from its checkpoint",
            ));
        }
        if self.preallocate_segments {
            installed_file.preallocate(&cursor.path(&self.directory), max_size);
        }
        let checkpoint_prepare = if let Some(prepare) = prepare {
            let header = self.validate_checkpoint_prepare(&prepare)?;
            let reference = if header.operation == Operation::SendMessages {
                let batch = decode_batch(prepare.as_slice())?;
                if initial.length >= batch.batch_length
                    && batch_next_offset(prepare.as_slice())? == initial.next_offset
                {
                    let reference = SegmentReference {
                        generation,
                        start_offset: initial.start_offset,
                        position: initial.length - batch.batch_length,
                        length: batch.batch_length,
                    };
                    let file = self
                        .segment_files
                        .get(&(generation, initial.start_offset))
                        .ok_or_else(|| invalid("installed segment handle is absent"))?;
                    if file
                        .read(
                            reference.position,
                            prepare.len() - size_of::<PrepareHeader>(),
                        )
                        .await?
                        != prepare.as_slice()[size_of::<PrepareHeader>()..]
                    {
                        return Err(invalid(
                            "checkpoint prepare differs from installed segment bytes",
                        ));
                    }
                    Some(reference)
                } else if let Some(reference) = self
                    .entries
                    .get(&op)
                    .filter(|entry| entry.checksum == header.checksum)
                    .and_then(|entry| entry.reference)
                {
                    Some(reference)
                } else {
                    let retained = segments.allocate(SegmentPosition {
                        start_offset: batch.base_offset,
                        length: batch.batch_length,
                        next_offset: batch_next_offset(prepare.as_slice())?,
                    })?;
                    let reference = SegmentReference {
                        generation: retained.generation,
                        start_offset: batch.base_offset,
                        position: 0,
                        length: batch.batch_length,
                    };
                    let mut file = self
                        .storage
                        .open(&reference.path(&self.directory), OpenMode::Create)
                        .await?;
                    file.write_frozen(0, prepare.slice(size_of::<PrepareHeader>()..))
                        .await?;
                    file.sync().await?;
                    Some(reference)
                }
            } else {
                None
            };
            Some((prepare, reference))
        } else {
            None
        };
        self.sync_segment_files().await?;
        self.storage.sync_directory(&self.directory).await?;
        self.state.segment_references = true;
        self.state.segment_storage = Some(segments);
        self.state.checkpoint = op;
        self.state.anchor_known = checksum.is_some();
        self.state.certified_log_view = None;
        self.rewrite(op, checksum.unwrap_or(0), Some(0), checkpoint_prepare)
            .await
    }

    pub(super) async fn migrate_segment_prepares(&mut self) -> io::Result<()> {
        if let Some(segments) = self.state.segment_storage
            && (self
                .entries
                .range(..=self.state.purge_floor)
                .any(|(_, entry)| entry.reference.is_some())
                || self.segment_migration_needed(segments).await?)
        {
            self.rewrite(
                self.state.checkpoint,
                self.state.checkpoint_checksum,
                None,
                None,
            )
            .await?;
        }
        Ok(())
    }

    async fn segment_migration_needed(&self, mut segments: SegmentState) -> io::Result<bool> {
        let mut convertible = false;
        for (_, entry) in self
            .entries
            .range(self.state.purge_floor.saturating_add(1)..)
        {
            if entry.reference.is_some() {
                continue;
            }
            let (header, _, prepare, _) =
                self.read_record(entry.position, self.state.length).await?;
            if header.operation == Operation::SendMessages
                && decode_batch(prepare.as_slice())?.base_offset
                    >= segments.tail.position.next_offset
            {
                // Validate the complete migration before rewrite can modify files.
                segments.reserve(&header, prepare.as_slice())?;
                convertible = true;
            }
        }
        Ok(convertible)
    }

    pub(super) async fn write_segment_bodies(
        &mut self,
        prepares: &[Frozen<4096>],
        records: &[(u64, StoredPrepare, usize)],
    ) -> io::Result<()> {
        if self.state.segment_storage.is_none() {
            return Ok(());
        }
        let mut bodies = records
            .iter()
            .filter_map(|(_, record, index)| record.reference.map(|reference| (reference, *index)))
            .peekable();
        if bodies.peek().is_none() {
            return Ok(());
        }
        while let Some((first, index)) = bodies.next() {
            let mut buffers = Vec::with_capacity(records.len().min(IOV_MAX));
            buffers.push(prepares[index].slice(size_of::<PrepareHeader>()..));
            let mut end = first.position + first.length;
            while buffers.len() < IOV_MAX {
                let Some(&(reference, index)) = bodies.peek() else {
                    break;
                };
                if reference.generation != first.generation
                    || reference.start_offset != first.start_offset
                    || reference.position != end
                {
                    break;
                }
                end += reference.length;
                buffers.push(prepares[index].slice(size_of::<PrepareHeader>()..));
                bodies.next();
            }
            self.segment_writing_file(first)
                .await?
                .write_frozen_vectored(first.position, buffers)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn write_segment_body(
        &mut self,
        reference: SegmentReference,
        prepare: &Frozen<4096>,
    ) -> io::Result<()> {
        self.segment_writing_file(reference)
            .await?
            .write_frozen(
                reference.position,
                prepare.slice(size_of::<PrepareHeader>()..),
            )
            .await
    }

    async fn segment_writing_file(
        &mut self,
        reference: SegmentReference,
    ) -> io::Result<&mut S::File> {
        let preallocate_size = self
            .state
            .segment_storage
            .filter(|_| self.preallocate_segments)
            .map(|segments| segments.max_size);
        self.open_segment_file(
            reference.generation,
            reference.start_offset,
            reference.position,
            preallocate_size,
        )
        .await?;
        self.segment_files_dirty = true;
        self.segment_files
            .get_mut(&(reference.generation, reference.start_offset))
            .ok_or_else(|| invalid("segment writing handle is absent"))
    }

    /// Barrier over every body and link this journal wrote, leaving the dirty
    /// flags alone. Split out of [`Self::sync_segment_files`] so that
    /// `PartitionPrepareJournal::sync` can overlap it with the WAL file's own
    /// barrier and clear both flags once the pair has completed.
    pub(super) async fn segment_barrier(&self) -> io::Result<()> {
        if self.segment_files_dirty {
            for file in self.segment_files.values() {
                // Keep the writing handle: reopening after an errseq writeback error
                // could turn a failed body barrier into a successful acknowledgment.
                file.sync().await?;
            }
        }
        if self.segment_links_dirty {
            self.storage.sync_directory(&self.directory).await?;
            self.storage
                .sync_directory(self.segment_directory()?)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn sync_segment_files(&mut self) -> io::Result<()> {
        self.segment_barrier().await?;
        self.segment_files_dirty = false;
        self.segment_links_dirty = false;
        Ok(())
    }

    pub(super) fn retain_active_segment_file(&mut self) {
        // Buffered rotations must retain their original writers until publication.
        if let Some(segments) = self.state.segment_storage {
            let active = (
                segments.tail.generation,
                segments.tail.position.start_offset,
            );
            self.segment_files.retain(|key, _| *key == active);
        }
    }

    pub(super) fn retained_segment_paths(
        &self,
        entries: &BTreeMap<u64, StoredPrepare>,
        segments: Option<SegmentState>,
    ) -> BTreeSet<PathBuf> {
        let mut paths = super::referenced_segments(entries, &self.directory);
        if let Some(segments) = segments {
            paths.insert(segments.tail.path(&self.directory));
            paths.insert(segments.checkpoint.path(&self.directory));
        }
        paths
    }

    pub(super) fn segment_boundary(&self, through_op: u64) -> Option<SegmentCursor> {
        let segments = self.state.segment_storage?;
        self.entries
            .range(..=through_op)
            .rev()
            .find_map(|(&op, entry)| {
                let reference = entry.reference?;
                let next_offset = entry.next_offset?;
                (op > self.state.purge_floor
                    && op > self.state.checkpoint
                    && next_offset >= segments.checkpoint.position.next_offset)
                    .then_some(SegmentCursor {
                        generation: reference.generation,
                        position: SegmentPosition {
                            start_offset: reference.start_offset,
                            length: reference.position + reference.length,
                            next_offset,
                        },
                    })
            })
            .or(Some(segments.checkpoint))
    }

    pub(super) async fn recover_segment_files(&mut self) -> io::Result<()> {
        let Some(segments) = self.state.segment_storage else {
            return Ok(());
        };
        self.poisoned = true;
        let parent = self.segment_directory()?.to_path_buf();
        let cursor = segments.tail;
        let public = parent.join(format!("{:020}.log", cursor.position.start_offset));
        // Retention may remove a fully checkpointed sealed segment. Its private
        // checkpoint prepare remains available without restoring polled data.
        let restore = cursor.position.length < segments.max_size
            || cursor.position.next_offset > segments.checkpoint.position.next_offset
            || self.storage.exists(&public).await?;
        if restore {
            self.open_segment_file(
                cursor.generation,
                cursor.position.start_offset,
                cursor.position.length,
                self.preallocate_segments.then_some(segments.max_size),
            )
            .await?;
            let file = self
                .segment_files
                .get(&(cursor.generation, cursor.position.start_offset))
                .ok_or_else(|| invalid("segment rollback handle is absent"))?;
            let length = file.length().await?;
            if length < cursor.position.length {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "segment {} (generation {}, start offset {}) lost durably published bytes: expected at least {}, found {}",
                        cursor.path(&self.directory).display(),
                        cursor.generation,
                        cursor.position.start_offset,
                        cursor.position.length,
                        length,
                    ),
                ));
            }
            if length > cursor.position.length {
                file.truncate(cursor.position.length).await?;
                if self.preallocate_segments {
                    file.preallocate(&cursor.path(&self.directory), segments.max_size);
                }
            }
            self.sync_segment_files().await?;
            // Keep the public name present across recovery crashes. Its absence
            // on a fully checkpointed sealed tail means retention removed it.
            let temporary = public.with_extension(SEGMENT_RECOVERY_EXTENSION);
            self.remove_segment_name(&temporary).await?;
            self.storage
                .hard_link(&cursor.path(&self.directory), &temporary)
                .await?;
            self.storage.rename(&temporary, &public).await?;
            // Renaming two links to the same inode leaves both names intact.
            self.remove_segment_name(&temporary).await?;
        }
        for entry in self.storage.entries(&parent).await? {
            let Some(name) = entry.name.to_str() else {
                continue;
            };
            let offset = name
                .strip_suffix(".log")
                .or_else(|| name.strip_suffix(".index"))
                .and_then(|offset| offset.parse::<u64>().ok());
            if !entry.directory
                && offset.is_some_and(|offset| {
                    offset > cursor.position.start_offset && offset >= cursor.position.next_offset
                })
            {
                self.storage.remove_file(&parent.join(entry.name)).await?;
            }
        }
        self.storage.sync_directory(&parent).await?;
        self.retain_active_segment_file();
        self.poisoned = false;
        Ok(())
    }

    pub(super) async fn truncate_segment_tail(&mut self) -> io::Result<()> {
        let Some(segments) = self.state.segment_storage else {
            return Ok(());
        };
        self.poisoned = true;
        let cursor = segments.tail;
        let key = (cursor.generation, cursor.position.start_offset);
        if !self.segment_files.contains_key(&key) {
            let path = cursor.path(&self.directory);
            match self.storage.open(&path, OpenMode::ReadWrite).await {
                Ok(file) => {
                    self.segment_files.insert(key, file);
                }
                Err(error)
                    if error.kind() == io::ErrorKind::NotFound && cursor.position.length == 0 =>
                {
                    self.poisoned = false;
                    return Ok(());
                }
                Err(error) => return Err(error),
            }
        }
        // Live partitions own the public log/index names and may still hold their
        // descriptors. Namespace reconciliation belongs to startup recovery.
        let file = self
            .segment_files
            .get(&key)
            .ok_or_else(|| invalid("segment rollback handle is absent"))?;
        if file.length().await? < cursor.position.length {
            return Err(invalid("segment lost durably published bytes"));
        }
        file.truncate(cursor.position.length).await?;
        file.sync().await?;
        self.poisoned = false;
        Ok(())
    }

    async fn open_segment_file(
        &mut self,
        generation: u64,
        start_offset: u64,
        length: u64,
        preallocate_size: Option<u64>,
    ) -> io::Result<()> {
        let key = (generation, start_offset);
        if self.segment_files.contains_key(&key) {
            return Ok(());
        }
        let parent = self.segment_directory()?.to_path_buf();
        let retained = segment_path(&self.directory, generation, start_offset);
        let public = parent.join(format!("{start_offset:020}.log"));
        let file = if self.storage.exists(&retained).await? {
            self.storage.open(&retained, OpenMode::ReadWrite).await?
        } else {
            if length == 0
                && self.storage.exists(&public).await?
                && self
                    .storage
                    .open(&public, OpenMode::Read)
                    .await?
                    .length()
                    .await?
                    > 0
            {
                // A purged inode can still be retained by older prepares.
                self.remove_segment_name(&public).await?;
            }
            // Segment roll can create the public name during any await. Both
            // creators must open that inode without truncation, then retain it.
            let mode = if length == 0 {
                OpenMode::CreateOrOpen
            } else {
                OpenMode::ReadWrite
            };
            let file = self.storage.open(&public, mode).await?;
            self.storage.hard_link(&public, &retained).await?;
            if length == 0
                && let Some(size) = preallocate_size
            {
                file.preallocate(&retained, size);
            }
            file
        };
        self.segment_files_dirty = true;
        self.segment_links_dirty = true;
        self.segment_files.insert(key, file);
        Ok(())
    }

    fn segment_directory(&self) -> io::Result<&Path> {
        self.directory
            .parent()
            .ok_or_else(|| invalid("WAL has no segment directory"))
    }

    async fn remove_segment_name(&self, path: &Path) -> io::Result<()> {
        match self.storage.remove_file(path).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(error),
        }
    }
}

impl SegmentPosition {
    const fn valid(self) -> bool {
        self.next_offset >= self.start_offset
            && ((self.length == 0) == (self.next_offset == self.start_offset))
    }
}

impl SegmentCursor {
    fn path(self, directory: &Path) -> PathBuf {
        segment_path(directory, self.generation, self.position.start_offset)
    }
}

impl SegmentState {
    pub(super) fn valid(self) -> bool {
        valid_segment_size(self.max_size)
            && self.tail.generation < self.next_generation
            && self.checkpoint.generation < self.next_generation
            && self.tail.position.valid()
            && self.checkpoint.position.valid()
            && self.tail.position.next_offset >= self.checkpoint.position.next_offset
    }

    pub(super) fn reserve(
        &mut self,
        header: &PrepareHeader,
        prepare: &[u8],
    ) -> io::Result<(Option<SegmentReference>, Option<u64>)> {
        if header.operation != Operation::SendMessages {
            return Ok((None, None));
        }
        let batch = decode_batch(prepare)?;
        if batch.base_offset != self.tail.position.next_offset {
            return Err(invalid(
                "message offsets do not extend the physical segment tail",
            ));
        }
        if self.tail.position.length >= self.max_size {
            self.tail = self.allocate(SegmentPosition {
                start_offset: batch.base_offset,
                length: 0,
                next_offset: batch.base_offset,
            })?;
        }
        let reference = SegmentReference {
            generation: self.tail.generation,
            start_offset: self.tail.position.start_offset,
            position: self.tail.position.length,
            length: batch.batch_length,
        };
        self.tail.position.length = reference
            .position
            .checked_add(reference.length)
            .ok_or_else(|| invalid("segment position exhausted"))?;
        self.tail.position.next_offset = batch_next_offset(prepare)?;
        Ok((Some(reference), Some(self.tail.position.next_offset)))
    }

    pub(super) fn reset_position(&mut self, position: SegmentPosition) -> io::Result<()> {
        if !position.valid() {
            return Err(invalid("invalid replacement segment boundary"));
        }
        self.tail = self.allocate(position)?;
        self.checkpoint = self.tail;
        Ok(())
    }

    fn allocate(&mut self, position: SegmentPosition) -> io::Result<SegmentCursor> {
        let generation = self.next_generation;
        self.next_generation = generation
            .checked_add(1)
            .ok_or_else(|| invalid("segment generation exhausted"))?;
        Ok(SegmentCursor {
            generation,
            position,
        })
    }

    pub(super) fn encode(self, bytes: &mut [u8]) {
        let values = [
            self.max_size,
            self.next_generation,
            self.tail.generation,
            self.tail.position.start_offset,
            self.tail.position.length,
            self.tail.position.next_offset,
            self.checkpoint.generation,
            self.checkpoint.position.start_offset,
            self.checkpoint.position.length,
            self.checkpoint.position.next_offset,
        ];
        for (field, value) in bytes
            .as_chunks_mut::<{ size_of::<u64>() }>()
            .0
            .iter_mut()
            .zip(values)
        {
            *field = value.to_le_bytes();
        }
    }

    pub(super) fn decode(bytes: &[u8]) -> io::Result<Self> {
        let mut values = [0; SEGMENT_STATE_BYTES / size_of::<u64>()];
        for (value, field) in values
            .iter_mut()
            .zip(bytes.as_chunks::<{ size_of::<u64>() }>().0)
        {
            *value = u64::from_le_bytes(*field);
        }
        let [
            max_size,
            next_generation,
            tail_generation,
            tail_start,
            tail_length,
            tail_next,
            checkpoint_generation,
            checkpoint_start,
            checkpoint_length,
            checkpoint_next,
        ] = values;
        let state = Self {
            max_size,
            next_generation,
            tail: SegmentCursor {
                generation: tail_generation,
                position: SegmentPosition {
                    start_offset: tail_start,
                    length: tail_length,
                    next_offset: tail_next,
                },
            },
            checkpoint: SegmentCursor {
                generation: checkpoint_generation,
                position: SegmentPosition {
                    start_offset: checkpoint_start,
                    length: checkpoint_length,
                    next_offset: checkpoint_next,
                },
            },
        };
        if !state.valid() {
            return Err(invalid("invalid durable segment boundaries"));
        }
        Ok(state)
    }
}

fn valid_segment_size(max_size: u64) -> bool {
    // The journal supports small test layouts; real topics validate their lower
    // bound and alignment at admission. Recovery must still cap allocation.
    (1..=MAX_TOPIC_SEGMENT_SIZE).contains(&max_size)
}

pub(super) fn batch_next_offset(prepare: &[u8]) -> io::Result<u64> {
    let batch = decode_batch(prepare)?;
    batch
        .base_offset
        .checked_add(u64::from(batch.message_count))
        .ok_or_else(|| invalid("message offset exhausted"))
}

pub(super) fn decode_batch(prepare: &[u8]) -> io::Result<BatchHeader> {
    let body = prepare
        .get(size_of::<PrepareHeader>()..)
        .ok_or_else(|| invalid("short message prepare"))?;
    let batch = BatchHeader::decode(body).map_err(|_| invalid("invalid segment batch header"))?;
    if batch.batch_length != body.len() as u64 || batch.message_count == 0 {
        return Err(invalid("invalid segment batch bounds"));
    }
    Ok(batch)
}
