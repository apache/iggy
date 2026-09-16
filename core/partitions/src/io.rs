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

use std::cell::{Cell, RefCell};
use std::io::IoSlice;
use std::rc::Rc;
use std::task::{Poll, Waker};
use std::time::Duration;

use compio::fs::File;
use consensus::VsrState;
use iggy_binary_protocol::PrepareHeader;
use iggy_common::MAX_MESSAGE_SIZE_UPPER_BYTES;
use iggy_common::{IggyByteSize, IggyError};
use journal::local_gate::OwnedLocalGateGuard;
use journal::superblock::{PingPongSuperblock, SuperblockStore};
use server_common::SegmentStorage;
use server_common::iobuf::{Frozen, IOV_MAX};
use server_common::poll::PollHistoryId;
use server_common::send_messages::COMMAND_HEADER_SIZE;
use server_common::sharding::IggyNamespace;
use tracing::warn;

use crate::iggy_index::IGGY_INDEX_SIZE;
use crate::offset_storage::{
    OffsetFilePermit, PersistedOffset, delete_persisted_offset, persist_offset,
    persist_offset_retained, read_offset_max,
};
use crate::{IggyIndexWriter, MessagesWriter, Segment};

// Slot cells, Rc headers, the executor task header and completion token storage.
const IO_CONTROL_ALLOCATION_RESERVE: usize = 4096;
// Source/destination siblings, File/OpenOptions paths and driver C strings can coexist.
const FILE_PATH_COPIES_MAX: usize = 8;
// glibc __alloc_dir bounds its filesystem-sized readdir buffer at 1 MiB.
// The control reserve separately covers DIR metadata and Rust iterator ownership.
const DIRECTORY_ITERATION_SCRATCH_MAX: usize = 1024 * 1024;
const PARTITION_IO_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

/// Process-unique identity of one partition owner, preserved across its views.
/// A replacement in the same namespace always has a different identity.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct PartitionIncarnation(PollHistoryId);

/// Captured file work. Execution never accesses installed partition state or
/// advances writer cursors; only the owner can accept the returned result.
pub struct MaterializationIoJob {
    pub(crate) allocation_charge: usize,
    pub(crate) target: MaterializationTarget,
    pub(crate) batches: Vec<Frozen<4096>>,
    pub(crate) indexes: Vec<u8>,
    pub(crate) bodies_written: Option<u64>,
    pub(crate) written: u64,
    pub(crate) completes: bool,
}

pub struct MaterializationIoResult {
    pub(crate) target: MaterializationTarget,
    pub(crate) outcome: Result<(u64, u64), IggyError>,
}

pub enum PartitionIoJob<SB = PingPongSuperblock> {
    Materialize(MaterializationIoJob),
    OffsetWrite(OffsetIoJob),
    OffsetDelete(OffsetDeleteIoJob),
    OffsetDirectories(OffsetDirectoriesIoJob),
    SegmentDirectory(String),
    IndexSync(Rc<IggyIndexWriter>),
    RemoveSegment {
        namespace: IggyNamespace,
        paths: [Option<String>; 3],
        strict: bool,
    },
    EmptySegment(SegmentIoJob),
    PurgeCleanup {
        directory: String,
        bodies: bool,
    },
    PurgeGeneration {
        path: String,
        generation: u64,
        revision: u64,
    },
    Quarantine {
        directory: String,
        revision: u64,
        replicated: bool,
    },
    Superblock(SuperblockIoJob<SB>),
    Transfer(TransferFileJob),
    Rotate {
        target: RotationTarget,
        job: SegmentIoJob,
    },
}

pub enum PartitionIoResult {
    Materialize(MaterializationIoResult),
    OffsetWrite(OffsetIoResult),
    OffsetDelete(Result<bool, IggyError>),
    OffsetDirectories(OffsetDirectoriesIoResult),
    SegmentDirectory(std::io::Result<()>),
    IndexSync {
        writer: Rc<IggyIndexWriter>,
        outcome: Result<(), IggyError>,
    },
    SegmentRemoved(Result<(), IggyError>),
    EmptySegment(Result<InstalledSegment, IggyError>),
    PurgeCleanup(std::io::Result<()>),
    PurgeGeneration(Result<(), IggyError>),
    Quarantine(std::io::Result<String>),
    Superblock(SuperblockIoResult),
    Transfer(TransferFileResult),
    Rotate {
        target: RotationTarget,
        outcome: Result<InstalledSegment, IggyError>,
    },
}

/// Identifies the owner continuation that may accept a completed phase.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartitionIoContinuation {
    Install,
    Commit,
    NoAck,
    Materialization,
    Superblock,
    Checkpoint,
    Retention,
    Purge,
    Quarantine,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PartitionIoIdentity {
    pub namespace: IggyNamespace,
    pub incarnation: PartitionIncarnation,
    pub history: PollHistoryId,
    pub sequence: u64,
    pub continuation: PartitionIoContinuation,
    pub local_order: Option<consensus::LocalRequestOrder>,
}

pub struct CapturedPartitionIo<SB> {
    pub identity: PartitionIoIdentity,
    pub job: PartitionIoJob<SB>,
    pub gate: Option<OwnedLocalGateGuard>,
    pub quiescence: Rc<PartitionIoQuiescence>,
}

/// Captured before tombstoning, so teardown never depends on mounted lookup.
#[derive(Default)]
pub struct PartitionIoQuiescence {
    active: Cell<Option<PartitionIoIdentity>>,
    retiring: Cell<bool>,
    deleting: Cell<bool>,
    interrupted: Cell<bool>,
    waiter: RefCell<Option<Waker>>,
}

pub struct PartitionTeardown {
    pub(crate) io: Rc<PartitionIoQuiescence>,
    pub(crate) persistence: Option<Rc<crate::PartitionPersistence>>,
}

impl PartitionIoQuiescence {
    /// Called only after the matching file future returned and its result was consumed.
    /// A removed owner still needs this settlement before its files can be deleted.
    pub fn settle(&self, identity: PartitionIoIdentity) {
        if !self.interrupted.get() && self.active.get() == Some(identity) {
            self.set(None);
        }
    }

    pub(crate) const fn get(&self) -> Option<PartitionIoIdentity> {
        self.active.get()
    }

    pub(crate) fn set(&self, active: Option<PartitionIoIdentity>) {
        self.active.set(active);
        if active.is_none()
            && let Some(waiter) = self.waiter.borrow_mut().take()
        {
            waiter.wake();
        }
    }

    pub(crate) fn retire(&self) {
        self.retiring.set(true);
    }

    pub(crate) const fn is_retiring(&self) -> bool {
        self.retiring.get()
    }

    pub(crate) fn delete(&self) {
        self.retiring.set(true);
        self.deleting.set(true);
    }

    pub(crate) const fn is_deleting(&self) -> bool {
        self.deleting.get()
    }

    pub(crate) const fn is_interrupted(&self) -> bool {
        self.interrupted.get()
    }

    /// Safe in a dropped task: no allocation, callback or resource release.
    pub fn interrupt(&self) {
        self.interrupted.set(true);
    }

    async fn drain(&self) -> std::io::Result<()> {
        futures::future::poll_fn(|context| {
            if self.interrupted.get() {
                return Poll::Ready(Err(std::io::Error::other(
                    "partition writer was interrupted",
                )));
            }
            if self.active.get().is_none() {
                return Poll::Ready(Ok(()));
            }
            *self.waiter.borrow_mut() = Some(context.waker().clone());
            Poll::Pending
        })
        .await
    }
}

impl PartitionTeardown {
    /// # Errors
    /// A failed or interrupted writer keeps the tombstone and its files intact.
    pub async fn drain(self) -> std::io::Result<()> {
        compio::runtime::time::timeout(PARTITION_IO_DRAIN_TIMEOUT, async {
            self.io.drain().await?;
            if let Some(persistence) = &self.persistence {
                persistence.retire();
                persistence.drain_with_timeout().await?;
            }
            Ok(())
        })
        .await
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "partition I/O drain timed out",
            )
        })?
    }
}

pub type PartitionIoNotifier = Rc<dyn Fn(IggyNamespace, PartitionIncarnation)>;

#[derive(Clone, Copy, Debug)]
pub struct PartitionIoPlan {
    pub continuation: PartitionIoContinuation,
    pub allocation_charge: usize,
}

pub enum PartitionIoStep {
    Progress,
    Pending,
    Ready(PartitionIoPlan),
    Transition(server_common::MessageBag),
    ViewApplied {
        actions: Vec<consensus::VsrAction>,
        peer: Option<u8>,
    },
    PurgeFinished {
        generation: u64,
        outcome: Result<(), crate::PurgeError>,
    },
    QuarantineFinished(std::io::Result<Option<String>>),
    TransferReady,
    InstallFinished {
        peer: u8,
        outcome: Result<
            crate::state_transfer::PartitionInstallOutcome,
            crate::state_transfer::PartitionInstallError,
        >,
    },
}

/// Admission can retain a healthy request without claiming it was sequenced.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PartitionIoVerdict {
    Ready,
    Pending,
    Failed,
}

/// Pins existing resources if execution is interrupted. Such a slot remains
/// charged and its partition stays fenced until runtime teardown.
pub struct PartitionIoResources<SB> {
    superblock: Option<Rc<SB>>,
    messages: Option<Rc<MessagesWriter>>,
    indexes: Option<Rc<IggyIndexWriter>>,
    file: Option<File>,
    permit: Option<Rc<OffsetFilePermit>>,
    buffers: Vec<Frozen<4096>>,
}

pub struct RotationTarget {
    pub(crate) incarnation: PartitionIncarnation,
    pub(crate) poll_history: PollHistoryId,
    pub(crate) segment_start: u64,
    pub(crate) segment_size: u64,
    pub(crate) segment_end: u64,
    pub(crate) messages_writer: Option<Rc<MessagesWriter>>,
    pub(crate) messages_position: Option<u64>,
    pub(crate) index_writer: Option<Rc<IggyIndexWriter>>,
    pub(crate) index_position: Option<u64>,
}

pub struct MaterializationTarget {
    pub(crate) namespace: IggyNamespace,
    pub(crate) incarnation: PartitionIncarnation,
    pub(crate) poll_history: PollHistoryId,
    pub(crate) segment_start: u64,
    pub(crate) segment_size: u64,
    pub(crate) messages_writer: Option<Rc<MessagesWriter>>,
    pub(crate) messages_position: Option<u64>,
    pub(crate) index_writer: Rc<IggyIndexWriter>,
    pub(crate) index_position: u64,
}

pub struct SegmentIoJob {
    pub(crate) messages_path: String,
    pub(crate) index_path: String,
    pub(crate) start_offset: u64,
    pub(crate) segment_size: IggyByteSize,
    pub(crate) persisted: bool,
    pub(crate) preallocate: bool,
    pub(crate) segment_bodies: bool,
    pub(crate) old_index: Option<Rc<IggyIndexWriter>>,
}

pub struct InstalledSegment {
    pub(crate) segment: Segment,
    pub(crate) storage: SegmentStorage,
    pub(crate) messages_writer: Option<Rc<MessagesWriter>>,
    pub(crate) index_writer: Option<Rc<IggyIndexWriter>>,
}

pub enum TransferFileJob {
    StageOffsets(Vec<crate::state_transfer::PlannedOffsetWrite>),
    DiscardOffsets(Vec<crate::state_transfer::PlannedOffsetWrite>),
    Backup {
        directory: String,
        begin: bool,
    },
    Sweep {
        directory: String,
        keep: Vec<std::path::PathBuf>,
        bodies: bool,
    },
    Rename {
        from: std::path::PathBuf,
        to: String,
        directory: Option<File>,
    },
    IndexDirectory(String),
    OpenSegment {
        directory: String,
        meta: crate::state_transfer::StagedSegmentMeta,
        segment_size: IggyByteSize,
        persisted: bool,
        preallocate: bool,
        bodies: bool,
        active: bool,
    },
    CommitOffset(String),
    ClearMissing(String),
    Converge {
        directory: String,
        offset_directories: [Option<String>; 2],
        segment: SegmentIoJob,
    },
}

pub enum TransferFileResult {
    Finished(Result<(), crate::state_transfer::PartitionInstallError>),
    Directory(std::io::Result<File>),
    Opened(Result<InstalledSegment, crate::state_transfer::PartitionInstallError>),
    Converged {
        segment: Result<InstalledSegment, IggyError>,
        stranded: Option<(usize, u32)>,
    },
}

pub struct SuperblockIoJob<SB> {
    pub(crate) superblock: Rc<SB>,
    pub(crate) state: VsrState,
}

pub struct SuperblockIoResult {
    pub(crate) state: VsrState,
    pub(crate) outcome: std::io::Result<()>,
}

#[derive(Clone, Copy)]
pub enum OffsetFileOwner {
    Wal,
    Workerless,
    Uncached,
}

pub struct OffsetIoJob {
    pub(crate) path: String,
    pub(crate) offset: u64,
    pub(crate) fold_max: bool,
    pub(crate) persisted: bool,
    pub(crate) owner: OffsetFileOwner,
    pub(crate) file: Option<File>,
    pub(crate) permit: Option<Rc<OffsetFilePermit>>,
}

pub struct OffsetIoResult {
    pub(crate) path: String,
    pub(crate) owner: OffsetFileOwner,
    pub(crate) file: Option<File>,
    pub(crate) permit: Option<Rc<OffsetFilePermit>>,
    pub(crate) outcome: Result<PersistedOffset, IggyError>,
    pub(crate) write_error: Option<std::io::Error>,
}

pub struct OffsetDeleteIoJob {
    pub(crate) path: String,
}

pub struct OffsetDirectoriesIoJob {
    pub(crate) paths: [Option<String>; 2],
    pub(crate) attempted: [bool; 2],
    #[cfg(test)]
    pub(crate) fault: Option<usize>,
}

pub struct OffsetDirectoriesIoResult {
    pub(crate) attempted: [bool; 2],
    pub(crate) synced: [bool; 2],
    pub(crate) failed: [bool; 2],
}

impl MaterializationIoJob {
    #[must_use]
    pub const fn allocation_charge(&self) -> usize {
        self.allocation_charge
    }

    #[allow(clippy::future_not_send)]
    pub async fn execute(self) -> MaterializationIoResult {
        let Self {
            target,
            mut batches,
            indexes,
            bodies_written,
            written,
            completes,
            ..
        } = self;
        let outcome = if let Some(saved) = bodies_written {
            target
                .index_writer
                .save_indexes_buffered_at(indexes, target.index_position)
                .await
                .map(|saved_indexes| (saved, saved_indexes))
        } else if let (Some(writer), Some(position)) =
            (&target.messages_writer, target.messages_position)
        {
            let Some(position) = position.checked_add(written) else {
                return MaterializationIoResult {
                    target,
                    outcome: Err(IggyError::CannotWriteToFile),
                };
            };
            for batch in &mut batches {
                *batch = batch.slice(size_of::<PrepareHeader>()..);
            }
            // Both halves must settle, including when either one fails.
            let (messages, indexes) = futures::future::join(
                writer.save_frozen_batches_at(&batches, position, completes),
                target
                    .index_writer
                    .save_indexes_at(indexes, target.index_position),
            )
            .await;
            if let (Err(message_error), Err(index_error)) = (&messages, &indexes) {
                warn!(
                    namespace_raw = target.namespace.inner(),
                    %message_error, %index_error,
                    "message and sparse-index writes both failed"
                );
            }
            match (messages, indexes) {
                (Ok(saved), Ok(saved_indexes)) => Ok((saved.as_bytes_u64(), saved_indexes)),
                (Err(error), _) | (Ok(_), Err(error)) => Err(error),
            }
        } else {
            Err(IggyError::CannotWriteToFile)
        };
        let outcome = outcome.and_then(|(saved, indexes)| {
            if completes {
                saved
                    .checked_add(written)
                    .map(|saved| (saved, indexes))
                    .ok_or(IggyError::CannotWriteToFile)
            } else {
                Ok((0, 0))
            }
        });
        MaterializationIoResult { target, outcome }
    }
}

impl<SB: SuperblockStore> PartitionIoJob<SB> {
    #[must_use]
    pub fn retain_resources(&self) -> PartitionIoResources<SB> {
        let mut retained = PartitionIoResources {
            superblock: None,
            messages: None,
            indexes: None,
            file: None,
            permit: None,
            buffers: Vec::new(),
        };
        match self {
            Self::Materialize(job) => {
                retained.messages.clone_from(&job.target.messages_writer);
                retained.indexes = Some(Rc::clone(&job.target.index_writer));
                retained.buffers.clone_from(&job.batches);
            }
            Self::Superblock(job) => retained.superblock = Some(Rc::clone(&job.superblock)),
            Self::OffsetWrite(job) => {
                retained.file.clone_from(&job.file);
                retained.permit.clone_from(&job.permit);
            }
            Self::Rotate { job, .. } => retained.indexes.clone_from(&job.old_index),
            Self::IndexSync(writer) => retained.indexes = Some(Rc::clone(writer)),
            Self::Transfer(TransferFileJob::Rename { directory, .. }) => {
                retained.file.clone_from(directory);
            }
            Self::Transfer(_)
            | Self::OffsetDelete(_)
            | Self::OffsetDirectories(_)
            | Self::SegmentDirectory(_)
            | Self::RemoveSegment { .. }
            | Self::EmptySegment(_)
            | Self::PurgeCleanup { .. }
            | Self::PurgeGeneration { .. }
            | Self::Quarantine { .. } => {}
        }
        retained
    }

    #[allow(clippy::future_not_send)]
    pub async fn execute(self) -> PartitionIoResult {
        match self {
            Self::Materialize(job) => PartitionIoResult::Materialize(job.execute().await),
            Self::OffsetWrite(job) => PartitionIoResult::OffsetWrite(job.execute().await),
            Self::OffsetDelete(job) => PartitionIoResult::OffsetDelete(job.execute().await),
            Self::OffsetDirectories(job) => {
                PartitionIoResult::OffsetDirectories(job.execute().await)
            }
            Self::SegmentDirectory(path) => {
                PartitionIoResult::SegmentDirectory(crate::state_transfer::fsync_dir(&path).await)
            }
            Self::IndexSync(writer) => {
                let outcome = writer.fsync().await;
                PartitionIoResult::IndexSync { writer, outcome }
            }
            Self::RemoveSegment {
                namespace,
                paths,
                strict,
            } => {
                let mut outcome = Ok(());
                for path in paths.into_iter().flatten() {
                    match compio::fs::remove_file(&path).await {
                        Ok(()) => {}
                        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                        Err(error) => {
                            warn!(namespace_raw = namespace.inner(), %path, %error, "failed to unlink segment file during cleanup");
                            if strict {
                                outcome = Err(IggyError::CannotDeleteFile);
                            }
                        }
                    }
                }
                PartitionIoResult::SegmentRemoved(outcome)
            }
            Self::EmptySegment(job) => PartitionIoResult::EmptySegment(job.execute().await),
            Self::PurgeCleanup { directory, bodies } => {
                let outcome = if bodies {
                    crate::state_transfer::remove_public_segment_files(&directory).await
                } else {
                    Ok(())
                };
                if outcome.is_ok() {
                    crate::state_transfer::sweep_staging_except(
                        &directory,
                        &std::collections::HashSet::new(),
                    )
                    .await;
                }
                PartitionIoResult::PurgeCleanup(outcome)
            }
            Self::PurgeGeneration {
                path,
                generation,
                revision,
            } => PartitionIoResult::PurgeGeneration(
                crate::offset_storage::persist_purge_generation(&path, generation, revision).await,
            ),
            Self::Quarantine {
                directory,
                revision,
                replicated,
            } => {
                let outcome = async {
                    if replicated {
                        crate::state_transfer::mark_materialization_missing(&directory, revision)
                            .await?;
                    }
                    crate::state_transfer::quarantine_partition_files(&directory, None).await
                }
                .await;
                PartitionIoResult::Quarantine(outcome)
            }
            Self::Superblock(job) => PartitionIoResult::Superblock(job.execute().await),
            Self::Transfer(job) => PartitionIoResult::Transfer(job.execute().await),
            Self::Rotate { target, job } => PartitionIoResult::Rotate {
                target,
                outcome: job.execute().await,
            },
        }
    }
}

/// The encoded HTTP request is checked against u32 only after JSON expansion
/// and encryption. Its exact-sized backing can exceed the raw ingress cap.
///
/// Recovery and replay use frozen format ceilings, independent of live limits.
#[must_use]
pub fn largest_legal_job_charge() -> Option<usize> {
    const ALIGNED_VEC_GROWTH_FACTOR: usize = 2;
    let framed = usize::try_from(MAX_MESSAGE_SIZE_UPPER_BYTES).ok()?;
    let grown_frame = framed.checked_mul(ALIGNED_VEC_GROWTH_FACTOR)?;
    let recovered = framed
        .checked_add(COMMAND_HEADER_SIZE)?
        .checked_add(size_of::<PrepareHeader>())?;
    let replay =
        journal::partition_journal::record_length(journal::partition_journal::PREPARE_BYTES_MAX)
            .ok()?;
    let capacity = usize::try_from(u32::MAX)
        .ok()?
        .max(grown_frame)
        .max(recovered)
        .max(replay);
    materialization_charge(
        Frozen::<4096>::allocation_size(capacity)?,
        1,
        IGGY_INDEX_SIZE,
    )
}

pub fn materialization_allocation_charge(
    batches: &[Frozen<4096>],
    batch_capacity: usize,
    index_capacity: usize,
) -> Option<usize> {
    let backing = batches.iter().try_fold(0_usize, |total, batch| {
        total.checked_add(Frozen::<4096>::allocation_size(batch.backing_capacity())?)
    })?;
    materialization_charge(backing, batch_capacity, index_capacity)
}

pub fn materialization_charge(backing: usize, buffers: usize, indexes: usize) -> Option<usize> {
    let references = buffers
        .checked_mul(2)?
        .checked_add(buffers.min(IOV_MAX))?
        .checked_mul(size_of::<Frozen<4096>>())?;
    // The driver retains a vectored-control allocation up to the syscall limit.
    let descriptors = IOV_MAX.checked_mul(size_of::<IoSlice<'static>>())?;
    backing
        .checked_add(references)?
        .checked_add(descriptors)?
        .checked_add(indexes)?
        .checked_add(size_of::<MaterializationIoJob>())?
        .checked_add(size_of::<MaterializationIoResult>())?
        .checked_add(size_of::<Vec<Frozen<4096>>>())?
        .checked_add(size_of::<Vec<IoSlice<'static>>>())?
        .checked_add(execution_allocation_charge::<PingPongSuperblock>()?)
}

pub fn file_phase_charge<'path, SB: SuperblockStore>(
    mut paths: impl Iterator<Item = &'path str>,
) -> Option<usize> {
    let paths = paths.try_fold(0_usize, |bytes, path| {
        bytes.checked_add(path.len().checked_mul(FILE_PATH_COPIES_MAX)?)
    })?;
    // Atomic replacement owns both the source value and its aligned write buffer.
    let buffers = Frozen::<4096>::allocation_size(IO_CONTROL_ALLOCATION_RESERVE)?.checked_mul(2)?;
    paths
        .checked_add(buffers)?
        .checked_add(DIRECTORY_ITERATION_SCRATCH_MAX)?
        .checked_add(execution_allocation_charge::<SB>()?)
}

fn execution_allocation_charge<SB: SuperblockStore>() -> Option<usize> {
    fn return_size<Input, Output>(_: impl FnOnce(Input) -> Output) -> usize {
        size_of::<Output>()
    }
    return_size(PartitionIoJob::<SB>::execute)
        .checked_add(size_of::<PartitionIoResult>())?
        .checked_add(size_of::<PartitionIoResources<SB>>())?
        .checked_add(IO_CONTROL_ALLOCATION_RESERVE)
}

impl TransferFileJob {
    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    async fn execute(self) -> TransferFileResult {
        match self {
            Self::StageOffsets(writes) => TransferFileResult::Finished(
                crate::state_transfer::stage_offset_writes(&writes).await,
            ),
            Self::DiscardOffsets(writes) => {
                crate::state_transfer::discard_offset_writes(&writes).await;
                TransferFileResult::Finished(Ok(()))
            }
            Self::Backup { directory, begin } => {
                let path = std::path::Path::new(&directory);
                let outcome = if begin {
                    crate::install_backup::begin(path).await
                } else {
                    crate::install_backup::finish(path).await
                };
                TransferFileResult::Finished(outcome.map_err(|source| {
                    crate::state_transfer::PartitionInstallError::SwapIo {
                        path: directory,
                        source,
                    }
                }))
            }
            Self::Sweep {
                directory,
                keep,
                bodies,
            } => {
                let keep = keep.iter().map(std::path::PathBuf::as_path).collect();
                crate::state_transfer::sweep_staging_except(&directory, &keep).await;
                let outcome = async {
                    if bodies {
                        crate::state_transfer::remove_public_segment_files(&directory).await?;
                    }
                    crate::state_transfer::fsync_dir(&directory).await
                }
                .await;
                TransferFileResult::Finished(outcome.map_err(|source| {
                    crate::state_transfer::PartitionInstallError::SwapIo {
                        path: directory,
                        source,
                    }
                }))
            }
            Self::Rename {
                from,
                to,
                directory,
            } => {
                let outcome = async {
                    compio::fs::rename(from, &to).await?;
                    if let Some(directory) = directory {
                        directory.sync_all().await?;
                    }
                    Ok(())
                }
                .await;
                TransferFileResult::Finished(outcome.map_err(|source| {
                    crate::state_transfer::PartitionInstallError::SwapIo { path: to, source }
                }))
            }
            Self::IndexDirectory(path) => {
                let outcome = async {
                    let directory = File::open(path).await?;
                    directory.sync_all().await?;
                    Ok(directory)
                }
                .await;
                TransferFileResult::Directory(outcome)
            }
            Self::OpenSegment {
                directory,
                meta,
                segment_size,
                persisted,
                preallocate,
                bodies,
                active,
            } => {
                let log_path = format!("{directory}/{:020}.log", meta.start_offset);
                let index_path = format!("{directory}/{:020}.index", meta.start_offset);
                let outcome = async {
                    let open = || async {
                        if bodies {
                            SegmentStorage::with_read_only_messages(
                                &log_path,
                                &index_path,
                                meta.index_size,
                                true,
                                None,
                            )
                            .await
                        } else {
                            SegmentStorage::new(
                                &log_path,
                                &index_path,
                                meta.size,
                                meta.index_size,
                                true,
                            )
                            .await
                        }
                    };
                    let storage = match open().await {
                        Ok(storage) => storage,
                        Err(_) => open().await?,
                    };
                    let messages_writer = if active && !bodies {
                        let counter = storage
                            .messages_writer
                            .as_ref()
                            .ok_or(IggyError::CannotReadFile)?
                            .size_counter();
                        Some(Rc::new(
                            MessagesWriter::new(
                                &log_path,
                                counter,
                                persisted,
                                true,
                                preallocate.then_some(segment_size),
                            )
                            .await?,
                        ))
                    } else {
                        None
                    };
                    let index_writer = if active {
                        let counter = storage
                            .index_writer
                            .as_ref()
                            .ok_or(IggyError::CannotReadFile)?
                            .size_counter();
                        Some(Rc::new(
                            IggyIndexWriter::new(&index_path, counter, persisted, true).await?,
                        ))
                    } else {
                        None
                    };
                    let mut segment = Segment::new(meta.start_offset, segment_size);
                    segment.sealed = !active;
                    segment.start_timestamp = meta.start_timestamp;
                    segment.end_timestamp = meta.end_timestamp;
                    segment.max_timestamp = meta.max_timestamp;
                    segment.end_offset = meta.end_offset;
                    segment.size = IggyByteSize::from(meta.size);
                    segment.current_position = meta.size;
                    Ok(InstalledSegment {
                        segment,
                        storage,
                        messages_writer,
                        index_writer,
                    })
                }
                .await;
                TransferFileResult::Opened(outcome.map_err(|source| {
                    crate::state_transfer::PartitionInstallError::SegmentOpen {
                        path: log_path,
                        source,
                    }
                }))
            }
            Self::CommitOffset(path) => TransferFileResult::Finished(
                crate::offset_storage::commit_offset_replacement(&path)
                    .await
                    .map_err(|source| {
                        crate::state_transfer::PartitionInstallError::OffsetPersistence {
                            path,
                            source,
                        }
                    }),
            ),
            Self::ClearMissing(path) => TransferFileResult::Finished(
                crate::state_transfer::clear_materialization_missing(&path)
                    .await
                    .map_err(
                        |source| crate::state_transfer::PartitionInstallError::SwapIo {
                            path,
                            source,
                        },
                    ),
            ),
            Self::Converge {
                directory,
                offset_directories,
                segment,
            } => {
                let mut stranded = None;
                let outcome = async {
                    for (kind, path) in offset_directories.into_iter().enumerate() {
                        let Some(path) = path else {
                            continue;
                        };
                        let entries = match std::fs::read_dir(&path) {
                            Ok(entries) => entries,
                            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                            Err(_) => return Err(IggyError::CannotReadFile),
                        };
                        for entry in entries {
                            let entry = entry.map_err(|_| IggyError::CannotReadFile)?;
                            let path = entry.path();
                            let Some(path) = path.to_str() else {
                                continue;
                            };
                            if let Some(id) = crate::state_transfer::numeric_offset_id(path) {
                                if let Err(error) =
                                    crate::state_transfer::retry_offset_mutation(|| {
                                        crate::offset_storage::delete_persisted_offset(path)
                                    })
                                    .await
                                {
                                    stranded = Some((kind, id));
                                    return Err(error);
                                }
                            } else if entry.file_name().to_str().is_some_and(|name| {
                                crate::offset_storage::offset_replacement_id(name).is_some()
                            }) {
                                let _ = compio::fs::remove_file(path).await;
                            }
                        }
                        match crate::state_transfer::fsync_dir(&path).await {
                            Ok(()) => {}
                            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                            Err(_) => return Err(IggyError::CannotSyncFile),
                        }
                    }
                    for entry in std::fs::read_dir(&directory)
                        .map_err(|_| IggyError::CannotReadPartitions)?
                    {
                        let entry = entry.map_err(|_| IggyError::CannotReadPartitions)?;
                        let path = entry.path();
                        if path.to_str().is_some_and(|path| {
                            [
                                ".log",
                                ".index",
                                ".staging",
                                crate::segment_anchor::ANCHOR_SUFFIX,
                            ]
                            .iter()
                            .any(|suffix| path.ends_with(suffix))
                        }) {
                            match compio::fs::remove_file(path).await {
                                Ok(()) => {}
                                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                                Err(_) => return Err(IggyError::CannotDeleteFile),
                            }
                        }
                    }
                    crate::state_transfer::fsync_dir(&directory)
                        .await
                        .map_err(|_| IggyError::CannotSyncFile)?;
                    segment.execute().await
                }
                .await;
                TransferFileResult::Converged {
                    segment: outcome,
                    stranded,
                }
            }
        }
    }
}

impl SegmentIoJob {
    #[allow(clippy::future_not_send)]
    pub(crate) async fn execute(self) -> Result<InstalledSegment, IggyError> {
        if let Some(writer) = &self.old_index {
            writer.fsync().await?;
        }
        let Self {
            messages_path,
            index_path,
            start_offset,
            segment_size,
            persisted,
            preallocate,
            segment_bodies,
            ..
        } = self;
        let storage = if segment_bodies {
            SegmentStorage::with_read_only_messages(
                &messages_path,
                &index_path,
                0,
                false,
                preallocate.then_some(segment_size.as_bytes_u64()),
            )
            .await
        } else {
            SegmentStorage::new(&messages_path, &index_path, 0, 0, false).await
        }
        .map_err(|_| IggyError::CannotCreateSegmentLogFile(messages_path.clone()))?;
        let messages_writer = if segment_bodies {
            None
        } else {
            let messages_size_bytes = storage
                .messages_writer
                .as_ref()
                .ok_or_else(|| IggyError::CannotCreateSegmentLogFile(messages_path.clone()))?
                .size_counter();
            Some(Rc::new(
                MessagesWriter::new(
                    &messages_path,
                    messages_size_bytes,
                    persisted,
                    false,
                    preallocate.then_some(segment_size),
                )
                .await
                .map_err(|_| IggyError::CannotCreateSegmentLogFile(messages_path.clone()))?,
            ))
        };
        let index_size_bytes = storage
            .index_writer
            .as_ref()
            .ok_or_else(|| IggyError::CannotCreateSegmentIndexFile(index_path.clone()))?
            .size_counter();
        let index_writer = Rc::new(
            IggyIndexWriter::new(&index_path, index_size_bytes, persisted, false)
                .await
                .map_err(|_| IggyError::CannotCreateSegmentIndexFile(index_path))?,
        );
        Ok(InstalledSegment {
            segment: Segment::new(start_offset, segment_size),
            storage,
            messages_writer,
            index_writer: Some(index_writer),
        })
    }
}

impl<SB: SuperblockStore> SuperblockIoJob<SB> {
    #[allow(clippy::future_not_send)]
    pub(crate) async fn execute(self) -> SuperblockIoResult {
        let outcome = self.superblock.write(&self.state.to_bytes()).await;
        SuperblockIoResult {
            state: self.state,
            outcome,
        }
    }
}

impl OffsetIoJob {
    #[allow(clippy::future_not_send)]
    pub(crate) async fn execute(mut self) -> OffsetIoResult {
        let value = if self.fold_max {
            read_offset_max(&self.path, self.offset).await
        } else {
            Ok(PersistedOffset {
                offset: self.offset,
                written: true,
            })
        };
        let mut write_error = None;
        let outcome = match value {
            Ok(value) if value.written => match self.owner {
                OffsetFileOwner::Wal | OffsetFileOwner::Workerless => {
                    match persist_offset_retained(&self.path, value.offset, self.file.take()).await
                    {
                        Ok((result, file)) => {
                            let result = if matches!(self.owner, OffsetFileOwner::Wal)
                                && self.permit.is_none()
                            {
                                // Checkpoint cannot retain this inode when the handle budget is full.
                                result.and(file.sync_data().await)
                            } else {
                                self.file = Some(file);
                                result
                            };
                            result.map(|()| value).map_err(|error| {
                                write_error = Some(error);
                                IggyError::CannotWriteToFile
                            })
                        }
                        Err(error) => Err(error),
                    }
                }
                OffsetFileOwner::Uncached => {
                    persist_offset(&self.path, value.offset, self.persisted)
                        .await
                        .map(|()| value)
                }
            },
            other => other,
        };
        OffsetIoResult {
            path: self.path,
            owner: self.owner,
            file: self.file,
            permit: self.permit,
            outcome,
            write_error,
        }
    }
}

impl OffsetDeleteIoJob {
    #[allow(clippy::future_not_send)]
    pub(crate) async fn execute(self) -> Result<bool, IggyError> {
        delete_persisted_offset(&self.path).await
    }
}

impl OffsetDirectoriesIoJob {
    #[allow(clippy::future_not_send)]
    pub(crate) async fn execute(self) -> OffsetDirectoriesIoResult {
        let mut result = OffsetDirectoriesIoResult {
            attempted: self.attempted,
            synced: [false; 2],
            failed: [false; 2],
        };
        for (index, path) in self.paths.iter().enumerate() {
            if !self.attempted[index] {
                continue;
            }
            #[cfg(test)]
            if self.fault == Some(index) {
                result.failed[index] = true;
                continue;
            }
            let Some(path) = path else {
                continue;
            };
            match crate::state_transfer::fsync_dir(path).await {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    warn!(%error, path, "consumer offset directory sync failed");
                    result.failed[index] = true;
                    continue;
                }
            }
            if let Some(parent) = std::path::Path::new(path)
                .parent()
                .and_then(std::path::Path::to_str)
                && let Err(error) = crate::state_transfer::fsync_dir(parent).await
            {
                warn!(%error, path = parent, "consumer offset parent directory sync failed");
                result.failed[index] = true;
                continue;
            }
            result.synced[index] = true;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use server_common::iobuf::Owned;

    #[test]
    fn materialization_charges_pinned_allocations_and_vector_capacity() {
        const BACKING_CAPACITY: usize = 16 * 1024;
        const REFERENCES_CAPACITY: usize = 8;
        let mut backing = Owned::<4096>::with_capacity(BACKING_CAPACITY);
        backing.extend_from_slice(b"retained");
        let mut batches = Vec::with_capacity(REFERENCES_CAPACITY);
        batches.push(Frozen::from(backing));
        let full_charge =
            materialization_allocation_charge(&batches, batches.capacity(), IGGY_INDEX_SIZE)
                .unwrap();
        batches[0] = batches[0].slice(..1);
        let sliced_charge =
            materialization_allocation_charge(&batches, batches.capacity(), IGGY_INDEX_SIZE)
                .unwrap();
        assert!(sliced_charge > BACKING_CAPACITY + REFERENCES_CAPACITY * size_of::<Frozen<4096>>());
        assert_eq!(
            sliced_charge, full_charge,
            "a small visible slice cannot bypass the backing allocation budget"
        );
        assert!(materialization_charge(usize::MAX, 1, IGGY_INDEX_SIZE).is_none());
        assert!(materialization_charge(0, usize::MAX, IGGY_INDEX_SIZE).is_none());
    }

    #[test]
    fn legal_job_minimum_covers_serialized_http_and_padded_replay() {
        let minimum = largest_legal_job_charge().unwrap();
        let serialized = usize::try_from(u32::MAX).unwrap();
        assert!(minimum > serialized);
        let replay = journal::partition_journal::record_length(
            journal::partition_journal::PREPARE_BYTES_MAX,
        )
        .unwrap();
        assert!(minimum > Frozen::<4096>::allocation_size(replay).unwrap());
        assert!(minimum > usize::try_from(MAX_MESSAGE_SIZE_UPPER_BYTES).unwrap());
    }

    #[test]
    fn legal_job_minimum_covers_indivisible_install_phases() {
        // Linux include/uapi/linux/limits.h bounds each successfully opened path.
        const PATH_BYTES_MAX: usize = 4096;
        const OFFSET_KIND_DIRECTORIES: usize = 2;
        const PATHS_PER_SEGMENT: usize = 2;
        let path = "x".repeat(PATH_BYTES_MAX);
        let base = file_phase_charge::<PingPongSuperblock>(std::iter::repeat_n(
            path.as_str(),
            OFFSET_KIND_DIRECTORIES + 1,
        ))
        .unwrap();
        let minimum = largest_legal_job_charge().unwrap();
        assert!(minimum >= base * crate::state_transfer::OFFSET_PERSIST_CONCURRENCY);
        let sweep = base
            + consensus::state_manifest::STATE_MANIFEST_ENTRIES_MAX as usize
                * PATHS_PER_SEGMENT
                * (PATH_BYTES_MAX
                    + size_of::<std::path::PathBuf>()
                    + 4 * size_of::<&std::path::Path>()
                    + 4);
        assert!(minimum >= sweep);
    }
}
