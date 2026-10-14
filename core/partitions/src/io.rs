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
use std::collections::HashSet;
use std::io::IoSlice;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::task::{Poll, Waker};
use std::time::Duration;

use compio::fs::File;
use consensus::VsrState;
use futures::{StreamExt, TryStreamExt};
use iggy_binary_protocol::PrepareHeader;
use iggy_common::MAX_MESSAGE_SIZE_UPPER_BYTES;
use iggy_common::{ConsumerKind, IggyByteSize, IggyError};
use journal::durable_storage::{DiskStorage, DurableStorage};
use journal::local_gate::OwnedLocalGateGuard;
use journal::superblock::{PingPongSuperblock, SuperblockStore};
use server_common::SegmentStorage;
use server_common::fatal::NoteDescriptorExhaustion;
use server_common::iobuf::{Frozen, IOV_MAX};
use server_common::poll::PollHistoryId;
use server_common::send_messages::COMMAND_HEADER_SIZE;
use server_common::sharding::IggyNamespace;
use tracing::warn;

use crate::iggy_index::IGGY_INDEX_SIZE;
use crate::offset_storage::{
    OffsetFilePermit, PersistedOffset, delete_persisted_offset,
    delete_persisted_offset_with_storage, persist_offset, persist_offset_retained, read_offset_max,
};
use crate::{IggyIndexWriter, MessagesWriter, Segment};

// Slot cells, Rc headers, the executor task header and completion token storage.
const IO_CONTROL_ALLOCATION_RESERVE: usize = 4096;
// Source/destination siblings, File/OpenOptions paths and driver C strings can coexist.
const FILE_PATH_COPIES_MAX: usize = 8;
// glibc __alloc_dir bounds its filesystem-sized readdir buffer at 1 MiB.
// The control reserve separately covers DIR metadata and Rust iterator ownership.
const DIRECTORY_ITERATION_SCRATCH_MAX: usize = 1024 * 1024;
pub const PARTITION_IO_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

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
    OffsetBatch(Vec<OffsetIoJob>),
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
    OffsetCleanup {
        directory: String,
        known: HashSet<u32>,
        /// Sorted IDs preserved by a state-transfer install.
        retained: Vec<u32>,
    },
    RetryCheckpoint {
        path: PathBuf,
        bytes: Vec<u8>,
    },
    ReclaimRetryCheckpoints {
        current: PathBuf,
        keep_current: bool,
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

// TODO: Preserve typed operation, path, and I/O causes until the reply boundary.
pub enum PartitionIoResult {
    Materialize(MaterializationIoResult),
    OffsetWrite(OffsetIoResult),
    OffsetBatch(Vec<OffsetIoResult>),
    OffsetDelete(Result<bool, IggyError>),
    OffsetDirectories(OffsetDirectoriesIoResult),
    SegmentDirectory(std::io::Result<()>),
    IndexSync {
        writer: Rc<IggyIndexWriter>,
        outcome: Result<(), IggyError>,
    },
    SegmentRemoved(Result<(), IggyError>),
    EmptySegment(Result<InstalledSegment, IggyError>),
    OffsetCleanup(OffsetCleanupIoResult),
    RetryCheckpoint(std::io::Result<()>),
    ReclaimRetryCheckpoints(std::io::Result<()>),
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

#[derive(Clone)]
pub struct PartitionTeardown {
    pub(crate) io: Rc<PartitionIoQuiescence>,
    pub(crate) persistence: Option<Rc<crate::PartitionPersistence>>,
    pub(crate) offset_files: Rc<crate::offset_storage::RetainedOffsetFiles<compio::fs::File>>,
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
    pub(crate) fn retire(&self) {
        self.io.retire();
        self.offset_files.clear();
    }

    pub(crate) fn delete(&self) {
        self.io.delete();
        self.offset_files.clear();
    }

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
    WireActions(Vec<consensus::VsrAction>),
    Progress,
    Pending,
    Ready(PartitionIoPlan),
    Transition(server_common::MessageBag),
    ViewApplied {
        actions: Vec<consensus::VsrAction>,
        peer: Option<u8>,
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
    offset_files: Vec<(Option<File>, Option<Rc<OffsetFilePermit>>)>,
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
        synced_files: std::collections::BTreeSet<std::path::PathBuf>,
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
    CommitOffsets(Vec<String>),
    ClearMissing(String),
    Converge {
        directory: String,
        offset_directories: [Option<String>; ConsumerKind::COUNT],
        // Keep recovery-only snapshots out of every file job's inline state.
        stranded_offsets: Box<[HashSet<u32>; ConsumerKind::COUNT]>,
        segment: SegmentIoJob,
    },
}

pub enum TransferFileResult {
    Finished(Result<(), crate::state_transfer::PartitionInstallError>),
    Directory(std::io::Result<File>),
    Opened(Result<InstalledSegment, crate::state_transfer::PartitionInstallError>),
    Converged {
        segment: Result<InstalledSegment, IggyError>,
        stranded: Option<(ConsumerKind, u32)>,
        released: Vec<(ConsumerKind, u32)>,
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
    pub(crate) paths: [Option<String>; ConsumerKind::COUNT],
    pub(crate) attempted: [bool; ConsumerKind::COUNT],
    #[cfg(test)]
    pub(crate) fault: Option<usize>,
}

pub struct OffsetDirectoriesIoResult {
    pub(crate) attempted: [bool; ConsumerKind::COUNT],
    pub(crate) synced: [bool; ConsumerKind::COUNT],
    pub(crate) failed: [bool; ConsumerKind::COUNT],
}

pub struct OffsetCleanupIoResult {
    /// Consumer ids whose offset files were removed or already absent.
    pub(crate) released: Vec<u32>,
    /// Consumer ids whose offset file could not be removed.
    pub(crate) failed: Vec<u32>,
    pub(crate) scan_error: Option<std::io::Error>,
    pub(crate) changed: bool,
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
            offset_files: Vec::new(),
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
            Self::OffsetBatch(jobs) => {
                retained.offset_files = jobs
                    .iter()
                    .map(|job| (job.file.clone(), job.permit.clone()))
                    .collect();
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
            | Self::OffsetCleanup { .. }
            | Self::RetryCheckpoint { .. }
            | Self::ReclaimRetryCheckpoints { .. }
            | Self::Quarantine { .. } => {}
        }
        retained
    }

    #[allow(clippy::future_not_send)]
    pub async fn execute(self) -> PartitionIoResult {
        match self {
            Self::Materialize(job) => PartitionIoResult::Materialize(job.execute().await),
            Self::OffsetWrite(job) => PartitionIoResult::OffsetWrite(job.execute().await),
            Self::OffsetBatch(jobs) => PartitionIoResult::OffsetBatch(
                futures::future::join_all(jobs.into_iter().map(OffsetIoJob::execute)).await,
            ),
            Self::OffsetDelete(job) => PartitionIoResult::OffsetDelete(job.execute().await),
            Self::OffsetDirectories(job) => {
                PartitionIoResult::OffsetDirectories(job.execute().await)
            }
            Self::SegmentDirectory(path) => PartitionIoResult::SegmentDirectory(
                DiskStorage.sync_directory(Path::new(&path)).await,
            ),
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
            Self::OffsetCleanup {
                directory,
                known,
                retained,
            } => PartitionIoResult::OffsetCleanup(
                remove_offset_files(&DiskStorage, &directory, known, &retained).await,
            ),
            Self::RetryCheckpoint { path, bytes } => PartitionIoResult::RetryCheckpoint(
                crate::state_transfer::write_retry_checkpoint(&path, bytes).await,
            ),
            Self::ReclaimRetryCheckpoints {
                current,
                keep_current,
            } => PartitionIoResult::ReclaimRetryCheckpoints(
                crate::state_transfer::reclaim_retry_checkpoint_files(&current, keep_current).await,
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

/// Sweep the directory, not just the ids the live maps hold, so a file they do
/// not track cannot be hydrated back at boot. The caller syncs the directory.
async fn remove_offset_files<S: DurableStorage>(
    storage: &S,
    directory: &str,
    mut known: HashSet<u32>,
    retained: &[u32],
) -> OffsetCleanupIoResult {
    let mut result = OffsetCleanupIoResult {
        released: Vec::new(),
        failed: Vec::new(),
        scan_error: None,
        changed: false,
    };
    let entries = futures::stream::once(storage.regular_files(Path::new(directory))).try_flatten();
    futures::pin_mut!(entries);
    while let Some(entry) = entries.next().await {
        let path = match entry {
            Ok(path) => path,
            Err(error) => {
                if error.kind() == std::io::ErrorKind::NotFound {
                    continue;
                }
                warn!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    path = directory,
                    %error,
                    "failed to scan consumer offset directory during cleanup"
                );
                result.scan_error = Some(error);
                continue;
            }
        };
        let Some(path) = path.to_str() else {
            continue;
        };
        let Some(consumer_id) = crate::state_transfer::numeric_offset_id(path) else {
            continue;
        };
        let release = known.contains(&consumer_id)
            && Path::new(path) == Path::new(directory).join(consumer_id.to_string());
        if release {
            known.remove(&consumer_id);
        }
        if retained.binary_search(&consumer_id).is_err() {
            result.remove(storage, path, consumer_id, release).await;
        }
    }
    // Live paths must still be removed if enumeration fails or skips an entry.
    for consumer_id in known {
        if retained.binary_search(&consumer_id).is_err() {
            result
                .remove(
                    storage,
                    &format!("{directory}/{consumer_id}"),
                    consumer_id,
                    true,
                )
                .await;
        }
    }
    result
}

impl OffsetCleanupIoResult {
    async fn remove<S: DurableStorage>(
        &mut self,
        storage: &S,
        path: &str,
        consumer_id: u32,
        release: bool,
    ) {
        match delete_persisted_offset_with_storage(storage, path).await {
            Ok(removed) => {
                self.changed |= removed;
                if release {
                    self.released.push(consumer_id);
                }
            }
            Err(error) => {
                warn!(
                    target: "iggy.partitions.diag",
                    plane = "partitions",
                    path,
                    %error,
                    "could not remove a consumer offset file"
                );
                self.failed.push(consumer_id);
            }
        }
    }
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
            Self::Backup {
                directory,
                begin,
                synced_files,
            } => {
                let path = std::path::Path::new(&directory);
                let outcome = if begin {
                    crate::install_backup::begin(path, &synced_files).await
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
                crate::state_transfer::sweep_staging_except(&directory, keep.into_iter().collect())
                    .await;
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
                    let directory = File::open(&path)
                        .await
                        .note_descriptor_exhaustion(|| format!("opening directory {path}"))?;
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
                    let mut storage = match open().await {
                        Ok(storage) => storage,
                        // A transient open failure need not fence durable history
                        // or force a complete pull for a partition without a WAL.
                        Err(_) => open().await?,
                    };
                    // Only the tail takes writes, as after a rotation.
                    if !active {
                        storage.seal();
                    }
                    let messages_writer = if active && !bodies {
                        let counter = storage
                            .messages_size
                            .clone()
                            .ok_or(IggyError::CannotReadFile)?;
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
                            .index_size
                            .clone()
                            .ok_or(IggyError::CannotReadFile)?;
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
            Self::CommitOffsets(paths) => {
                let results = futures::future::join_all(paths.into_iter().map(|path| async move {
                    crate::offset_storage::commit_offset_replacement(&path)
                        .await
                        .map_err(|source| {
                            crate::state_transfer::PartitionInstallError::OffsetPersistence {
                                path,
                                source,
                            }
                        })
                }))
                .await;
                TransferFileResult::Finished(
                    results.into_iter().try_for_each(std::convert::identity),
                )
            }
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
                mut stranded_offsets,
                segment,
            } => {
                let mut stranded = None;
                let mut released =
                    Vec::with_capacity(stranded_offsets.iter().map(HashSet::len).sum());
                let outcome = async {
                    for (kind, path) in offset_directories.into_iter().enumerate() {
                        let Some(path) = path else {
                            continue;
                        };
                        let entries =
                            futures::stream::once(DiskStorage.regular_files(Path::new(&path)))
                                .try_flatten();
                        futures::pin_mut!(entries);
                        while let Some(entry) = entries.next().await {
                            let entry = match entry {
                                Ok(entry) => entry,
                                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                                    continue;
                                }
                                Err(_) => return Err(IggyError::CannotReadFile),
                            };
                            let Some(path) = entry.to_str() else {
                                continue;
                            };
                            if let Some(id) = crate::state_transfer::numeric_offset_id(path) {
                                if let Err(error) =
                                    crate::state_transfer::retry_offset_mutation(|| {
                                        crate::offset_storage::delete_persisted_offset(path)
                                    })
                                    .await
                                {
                                    stranded = Some((ConsumerKind::ALL[kind], id));
                                    return Err(error);
                                }
                                if stranded_offsets[kind].remove(&id) {
                                    released.push((ConsumerKind::ALL[kind], id));
                                }
                            } else if entry
                                .file_name()
                                .and_then(|name| name.to_str())
                                .is_some_and(|name| {
                                    crate::offset_storage::offset_replacement_id(name).is_some()
                                })
                                && let Err(error) = compio::fs::remove_file(path).await
                            {
                                warn!(%path, %error, "cannot remove abandoned offset replacement during convergence");
                            }
                        }
                        match crate::state_transfer::fsync_dir(&path).await {
                            Ok(()) => {}
                            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                            Err(_) => return Err(IggyError::CannotSyncFile),
                        }
                    }
                    // TODO: Bound off-shard segment enumeration while preserving symlink cleanup.
                    for entry in std::fs::read_dir(&directory)
                        .map_err(|_| IggyError::CannotReadPartitions)?
                    {
                        let entry = entry.map_err(|_| IggyError::CannotReadPartitions)?;
                        let path = entry.path();
                        if path.to_str().is_some_and(|path| {
                            [
                                ".log",
                                ".index",
                                crate::state_transfer::STAGING_SUFFIX,
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
                    released,
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
                .messages_size
                .clone()
                .ok_or_else(|| IggyError::CannotCreateSegmentLogFile(messages_path.clone()))?;
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
            .index_size
            .clone()
            .ok_or_else(|| IggyError::CannotCreateSegmentIndexFile(index_path.clone()))?;
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
                                match result {
                                    Ok(()) => file.sync_data().await,
                                    Err(error) => Err(error),
                                }
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
            synced: [false; ConsumerKind::COUNT],
            failed: [false; ConsumerKind::COUNT],
        };
        let mut parents: [Option<&str>; ConsumerKind::COUNT] = [None; ConsumerKind::COUNT];
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
            parents[index] = Path::new(path).parent().and_then(Path::to_str);
            if parents[index].is_none() {
                result.synced[index] = true;
            }
        }
        // The kind directory is itself an entry in offsets/. Its publication
        // must survive before a local persisted offset reply. Every kind shares
        // that parent, so each distinct parent syncs once, after all of them.
        for index in 0..ConsumerKind::COUNT {
            let Some(parent) = parents[index] else {
                continue;
            };
            if parents[..index].contains(&Some(parent)) {
                continue;
            }
            let synced = match crate::state_transfer::fsync_dir(parent).await {
                Ok(()) => true,
                Err(error) => {
                    warn!(%error, path = parent, "consumer offset parent directory sync failed");
                    false
                }
            };
            for (sibling, sibling_parent) in parents.iter().enumerate().skip(index) {
                if *sibling_parent == Some(parent) {
                    result.synced[sibling] = synced;
                    result.failed[sibling] = !synced;
                }
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use journal::durable_storage::{OpenMode, RegularFiles, StorageEntry};
    use server_common::iobuf::Owned;

    struct FailedOffsetScan;

    impl DurableStorage for FailedOffsetScan {
        type File = <DiskStorage as DurableStorage>::File;

        async fn open(&self, path: &Path, mode: OpenMode) -> std::io::Result<Self::File> {
            DiskStorage.open(path, mode).await
        }

        async fn create_directories(&self, path: &Path) -> std::io::Result<()> {
            DiskStorage.create_directories(path).await
        }

        async fn sync_directory(&self, path: &Path) -> std::io::Result<()> {
            DiskStorage.sync_directory(path).await
        }

        async fn rename(&self, source: &Path, target: &Path) -> std::io::Result<()> {
            DiskStorage.rename(source, target).await
        }

        async fn remove_file(&self, path: &Path) -> std::io::Result<()> {
            DiskStorage.remove_file(path).await
        }

        async fn hard_link(&self, source: &Path, target: &Path) -> std::io::Result<()> {
            DiskStorage.hard_link(source, target).await
        }

        async fn exists(&self, path: &Path) -> std::io::Result<bool> {
            DiskStorage.exists(path).await
        }

        async fn entries(&self, path: &Path) -> std::io::Result<Vec<StorageEntry>> {
            DiskStorage.entries(path).await
        }

        fn regular_files(
            &self,
            path: &Path,
        ) -> impl Future<Output = std::io::Result<RegularFiles>> {
            std::future::ready(Ok(Box::pin(futures::stream::iter([
                Ok(path.join("1")),
                Err(std::io::Error::other("directory scan interrupted")),
            ])) as RegularFiles))
        }

        async fn remove_tree(&self, path: &Path) -> std::io::Result<()> {
            DiskStorage.remove_tree(path).await
        }
    }

    #[compio::test]
    async fn given_failed_offset_scan_when_replacing_history_should_remove_known_stale_files() {
        let directory = tempfile::tempdir().unwrap();
        for id in [1, 99] {
            std::fs::write(directory.path().join(id.to_string()), [0; 16]).unwrap();
        }
        let result = remove_offset_files(
            &FailedOffsetScan,
            directory.path().to_str().unwrap(),
            HashSet::from([1, 99]),
            &[1],
        )
        .await;
        assert!(result.scan_error.is_some());
        assert_eq!(result.failed, [] as [u32; 0]);
        assert_eq!(result.released, [99]);
        assert!(directory.path().join("1").is_file());
        assert!(!directory.path().join("99").exists());
    }

    #[cfg(unix)]
    #[compio::test]
    async fn given_non_file_offsets_when_converging_should_remove_only_regular_offsets() {
        const OFFSET_ID: u32 = 1;
        const DIRECTORY_ID: u32 = 2;
        const SYMLINK_ID: u32 = 3;
        let directory = tempfile::tempdir().unwrap();
        let offsets = directory.path().join("consumers");
        std::fs::create_dir(&offsets).unwrap();
        let offset = offsets.join(OFFSET_ID.to_string());
        std::fs::write(&offset, crate::offset_storage::encode_offset_record(0)).unwrap();
        let offset_directory = offsets.join(DIRECTORY_ID.to_string());
        std::fs::create_dir(&offset_directory).unwrap();
        let replacement_directory = offsets.join(format!("{DIRECTORY_ID}.tmp"));
        std::fs::create_dir(&replacement_directory).unwrap();
        let preserved = directory.path().join("preserved");
        std::fs::write(&preserved, b"preserved").unwrap();
        let offset_symlink = offsets.join(SYMLINK_ID.to_string());
        std::os::unix::fs::symlink(&preserved, &offset_symlink).unwrap();
        let replacement_symlink = offsets.join(format!("{SYMLINK_ID}.tmp"));
        std::os::unix::fs::symlink(&preserved, &replacement_symlink).unwrap();
        let stale_segment = directory.path().join("stale.log");
        std::fs::write(&stale_segment, b"stale").unwrap();
        let segment_symlink = directory.path().join("linked.log");
        std::os::unix::fs::symlink(&preserved, &segment_symlink).unwrap();
        let mut offset_directories = std::array::from_fn(|_| None);
        offset_directories[ConsumerKind::Consumer.index()] =
            Some(offsets.to_string_lossy().into_owned());
        offset_directories[ConsumerKind::ConsumerGroup.index()] = Some(
            directory
                .path()
                .join("missing")
                .to_string_lossy()
                .into_owned(),
        );
        let mut stranded_offsets = Box::new(std::array::from_fn(|_| HashSet::new()));
        stranded_offsets[ConsumerKind::Consumer.index()].insert(OFFSET_ID);
        let messages_path = directory.path().join("00000000000000000000.log");
        let index_path = directory.path().join("00000000000000000000.index");
        let result = TransferFileJob::Converge {
            directory: directory.path().to_string_lossy().into_owned(),
            offset_directories,
            stranded_offsets,
            segment: SegmentIoJob {
                messages_path: messages_path.to_string_lossy().into_owned(),
                index_path: index_path.to_string_lossy().into_owned(),
                start_offset: 0,
                segment_size: IggyByteSize::default(),
                persisted: false,
                preallocate: false,
                segment_bodies: false,
                old_index: None,
            },
        }
        .execute()
        .await;
        let TransferFileResult::Converged {
            segment,
            stranded,
            released,
        } = result
        else {
            panic!("convergence returned another result");
        };
        assert!(segment.is_ok(), "convergence failed: {:?}", segment.err());
        assert_eq!(stranded, None);
        assert_eq!(released, [(ConsumerKind::Consumer, OFFSET_ID)]);
        assert!(!offset.exists());
        assert!(offset_directory.is_dir());
        assert!(replacement_directory.is_dir());
        assert!(offset_symlink.is_symlink());
        assert!(replacement_symlink.is_symlink());
        assert_eq!(std::fs::read(&preserved).unwrap(), b"preserved");
        assert!(!stale_segment.exists());
        assert!(!segment_symlink.exists());
        assert!(messages_path.is_file());
        assert!(index_path.is_file());
    }

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
