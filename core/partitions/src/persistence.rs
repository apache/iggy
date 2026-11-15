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

use crate::FILE_SYNC_CONCURRENCY;
use crate::FreeSpaceReserve;
use crate::offset_storage::{OffsetFilePermit, RetainedOffsetFile, RetainedOffsetFiles};
use futures::future::Either;
use futures::{FutureExt, TryStreamExt};
use iggy_binary_protocol::{Operation, PrepareHeader};
use iggy_common::ConsumerKind;
use journal::PartitionPrepareJournal;
use journal::durable_storage::{DurableFile, DurableStorage};
use journal::partition_journal::{
    PARTITION_WAL_BYTES_MAX, SegmentPosition, SegmentReference, WalOpenOptions,
};
use server_common::Message;
use server_common::iobuf::Frozen;
use smallvec::SmallVec;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::time::{Duration, Instant};

// Group commit bounds, not throughput bounds. Every prepare in a group is
// already queued and waiting, so widening the group moves work off the barrier
// and onto a buffered memcpy: one body write and one durability barrier serve
// the whole group instead of each prepare paying its own. The byte budget is
// charged against the padded WAL extent, which is one reference record for a
// message body retained in segment storage.
const APPEND_BATCH_WAL_BYTES_MAX: u64 = 8 * 1024 * 1024;
// Bound segment body work independently of the WAL extent. References make the
// WAL cheap, but do not make copying their bodies into segment storage cheap.
const APPEND_BATCH_SEGMENT_BYTES_MAX: u64 = 8 * 1024 * 1024;
const APPEND_BATCH_OPS_MAX: usize = 256;
const CHECKPOINT_DIRTY_FILES_MAX: usize = 1024;
/// Writer backlog a Replicated commit may run ahead of while no checkpoint is
/// pending. It holds the backlog one checkpoint leaves on a busy partition, so
/// the commits after it do not wait, and it caps the bodies a slow writer pins.
const RUN_AHEAD_BYTES_MAX: u64 = 16 * 1024 * 1024;
/// Mutations a partition may apply before its obsolete files are reclaimed
/// whether or not the queue has drained. Reclaiming only on an idle queue
/// keeps the unlinks off every acknowledgement, but a partition under
/// continuous load never goes idle and would hold its old generations until
/// it did.
const RECLAIM_MUTATIONS_MAX: u32 = 64;
const PERSISTENCE_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

static NEXT_INSTANCE: AtomicU64 = AtomicU64::new(1);

/// Directory of the state-machine log inside a partition's WAL directory.
///
/// Created with the first command, so a partition that never runs one keeps a
/// single log. One that did holds two more descriptors for the life of its
/// incarnation, the log file and its frontier, and its open pays the second
/// log's recovery.
pub const STATE_MACHINE_LOG_DIRECTORY: &str = "state-machines";

/// The two logs of one partition consensus group. They share the group's op
/// sequence, and each op lives in exactly one of them: a state-machine command
/// in `commands`, everything else in `data`.
struct Logs<S: DurableStorage> {
    data: PartitionPrepareJournal<S>,
    commands: Option<PartitionPrepareJournal<S>>,
}

impl<S: DurableStorage> Logs<S> {
    fn each(&self) -> impl Iterator<Item = &PartitionPrepareJournal<S>> {
        std::iter::once(&self.data).chain(self.commands.as_ref())
    }

    /// Every op up to here reached its log. Appends arrive in op order, so the
    /// higher head covers the lower one's gaps.
    fn written_head(&self) -> u64 {
        self.each()
            .map(PartitionPrepareJournal::head)
            .max()
            .unwrap_or(0)
    }

    /// Every op up to here is durable in its log: below the lowest op either
    /// log wrote without a barrier.
    fn durable_head(&self) -> u64 {
        let written = self.written_head();
        self.each()
            .filter(|journal| journal.durable_op() < journal.head())
            .filter_map(|journal| journal.first_op_above(journal.durable_op()))
            .map(|op| op - 1)
            .min()
            .map_or(written, |limit| limit.min(written))
    }

    /// A view certifies the pair only when it certifies both logs.
    fn certified_log_view(&self) -> Option<u32> {
        self.each()
            .map(PartitionPrepareJournal::certified_log_view)
            .try_fold(u32::MAX, |lowest, view| view.map(|view| lowest.min(view)))
    }

    fn size_bytes(&self) -> u64 {
        self.each().map(PartitionPrepareJournal::size_bytes).sum()
    }

    fn retained_bytes(&self) -> u64 {
        self.each()
            .map(PartitionPrepareJournal::retained_bytes)
            .sum()
    }
}

impl<S: DurableStorage + Clone> Logs<S> {
    /// Open the data log, and the state-machine log once a command created it.
    /// Each is sparse only beside the other. Returns the storage that creates
    /// the state-machine log while it does not exist.
    ///
    /// The data log opens strict or sparse, so whether the other log exists
    /// has to be known before either opens, which costs one lookup.
    async fn open(
        directory: &Path,
        group: u64,
        incarnation: u64,
        storage: S,
        capacity: u64,
        preallocate_segments: bool,
    ) -> io::Result<(Self, Option<S>)> {
        let command_directory = directory.join(STATE_MACHINE_LOG_DIRECTORY);
        let sparse = storage.exists(&command_directory).await?;
        let data = PartitionPrepareJournal::open_with_options(
            directory,
            group,
            incarnation,
            storage.clone(),
            WalOpenOptions {
                capacity,
                preallocate_segments,
                sparse,
                new_log_view: 0,
            },
        )
        .await?;
        if !sparse {
            return Ok((
                Self {
                    data,
                    commands: None,
                },
                Some(storage),
            ));
        }
        let commands = PartitionPrepareJournal::open_with_options(
            &command_directory,
            group,
            incarnation,
            storage,
            WalOpenOptions {
                capacity,
                sparse,
                ..WalOpenOptions::default()
            },
        )
        .await?;
        Ok((
            Self {
                data,
                commands: Some(commands),
            },
            None,
        ))
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PersistenceCompletion {
    pub group: u64,
    pub instance: u64,
    pub epoch: u64,
}

#[derive(Default)]
pub struct PersistenceMetrics {
    pub disk_bytes: u64,
    pub retained_bytes: u64,
    pub queued_bytes: u64,
    pub in_flight_bytes: u64,
    pub checkpoints_pending: u64,
    pub completed_batches: u64,
    pub batched_prepares: u64,
    /// Durable groups that took the optional pre-barrier wait. Zero while the
    /// delay is disabled, and zero with it enabled means the arrival gap never
    /// cleared the guard, so a group-size change measured against the delay
    /// alone would be attributing something else.
    pub group_commit_waits: u64,
    pub completed_checkpoints: u64,
    pub failed_writes: u64,
}

pub type PersistenceNotifier = Rc<dyn Fn(PersistenceCompletion)>;

/// A file durability barrier that keeps the original writer alive until sync.
#[must_use]
pub struct FileSyncBarrier {
    path: PathBuf,
    sync: Pin<Box<dyn Future<Output = io::Result<()>> + 'static>>,
}

impl FileSyncBarrier {
    /// Retains a simulator file's original writer until its sync runs.
    #[cfg(feature = "simulator")]
    pub fn from_file<F: DurableFile + 'static>(path: impl Into<PathBuf>, file: F) -> Self {
        Self::from_future(path, async move { file.sync().await })
    }

    pub(crate) fn from_future(
        path: impl Into<PathBuf>,
        sync: impl Future<Output = io::Result<()>> + 'static,
    ) -> Self {
        Self {
            path: path.into(),
            sync: Box::pin(sync),
        }
    }

    pub(crate) fn already_synced(path: impl Into<PathBuf>) -> Self {
        Self::from_future(path, async { Ok(()) })
    }

    async fn run(self) -> io::Result<PathBuf> {
        self.sync.await.map_err(|error| {
            io::Error::new(error.kind(), format!("{}: {error}", self.path.display()))
        })?;
        Ok(self.path)
    }
}

/// Bytes this WAL accepted and has not finished writing. The node's free-space
/// reserve counts them as pending, because the write sites charge the disk
/// usage estimate only when a write starts.
#[derive(Default)]
struct PendingBytes {
    bytes: Cell<u64>,
    reserve: RefCell<Option<Arc<FreeSpaceReserve>>>,
}

impl PendingBytes {
    const fn get(&self) -> u64 {
        self.bytes.get()
    }

    fn set(&self, bytes: u64) {
        let previous = self.bytes.replace(bytes);
        if let Some(reserve) = self.reserve.borrow().as_ref() {
            if bytes > previous {
                reserve.add_pending(bytes - previous);
            } else if bytes < previous {
                reserve.release_pending(previous - bytes);
            }
        }
    }

    fn report_to(&self, reserve: Option<Arc<FreeSpaceReserve>>) {
        if let Some(reserve) = &reserve {
            reserve.add_pending(self.bytes.get());
        }
        if let Some(previous) = self.reserve.replace(reserve) {
            previous.release_pending(self.bytes.get());
        }
    }
}

impl Drop for PendingBytes {
    fn drop(&mut self) {
        self.report_to(None);
    }
}

pub struct PartitionPersistence<S: DurableStorage = crate::WalStorage> {
    group: u64,
    incarnation: u64,
    instance: u64,
    recovered_frontier: bool,
    lease: Option<Arc<WriterLease>>,
    epoch: Cell<u64>,
    journal: RefCell<Option<Logs<S>>>,
    /// The storage to create the state-machine log with when the first
    /// command arrives.
    command_storage: RefCell<Option<S>>,
    /// A command was accepted into this group's history, so the state-machine
    /// log exists or the writer creates it before writing anything later.
    holds_commands: Cell<bool>,
    queue: RefCell<VecDeque<Mutation<S>>>,
    offset_files: RetainedOffsetFiles<S::File>,
    accepted: RefCell<AcceptedPrepares>,
    // Published with written_head so readers never borrow the journal across writer I/O.
    segment_references: RefCell<BTreeMap<u64, SegmentReference>>,
    accepted_head: Cell<u64>,
    written_head: Cell<u64>,
    durable_head: Cell<u64>,
    checkpoint: Cell<u64>,
    checkpoint_checksum: Cell<Option<u128>>,
    certified_log_view: Cell<Option<u32>>,
    requested_log_view: Cell<Option<(u32, u64, u128)>>,
    checkpoint_requested: Cell<u64>,
    checkpoint_running: Cell<bool>,
    checkpoint_needed: Cell<bool>,
    dirty_segments: RefCell<BTreeSet<u64>>,
    dirty_offsets: [RefCell<BTreeSet<u32>>; ConsumerKind::COUNT],
    dirty_offset_directories: [Cell<bool>; ConsumerKind::COUNT],
    capacity: u64,
    disk_bytes: Cell<u64>,
    retained_bytes: Cell<u64>,
    segment_checkpoint: Cell<Option<SegmentPosition>>,
    queued_bytes: PendingBytes,
    in_flight_bytes: PendingBytes,
    waiters: RefCell<Vec<std::task::Waker>>,
    running: Cell<bool>,
    writer_active: Cell<bool>,
    retired: Cell<bool>,
    enqueue_paused: Cell<bool>,
    failure: RefCell<Option<Arc<io::Error>>>,
    failure_operation: Cell<Operation>,
    notifier: RefCell<Option<PersistenceNotifier>>,
    group_commit_delay: Cell<Duration>,
    /// Interval between the two most recent submissions. Decides whether a
    /// group-commit wait would see another prepare before it expires.
    append_gap: Cell<Duration>,
    last_append: Cell<Option<Instant>>,
    completed_batches: Cell<u64>,
    batched_prepares: Cell<u64>,
    group_commit_waits: Cell<u64>,
    completed_checkpoints: Cell<u64>,
    failed_writes: Cell<u64>,
    /// Fails a checkpoint after the data log published it and before the
    /// state-machine log did, as a crash between the two would leave them.
    #[cfg(test)]
    stop_between_checkpoints: Cell<bool>,
}

/// One full-drain observation, including its original worker epoch and deadline.
pub struct PersistenceDrain {
    instance: u64,
    epoch: u64,
    // TODO: Use injected monotonic time so owner-observed drains follow simulated time.
    started: Instant,
}

struct WriterLease {
    key: PathBuf,
    id: u64,
    retired: AtomicBool,
    running: AtomicBool,
    waiters: Mutex<Vec<std::task::Waker>>,
}

struct WriterRegistration {
    id: u64,
    writer: Weak<WriterLease>,
    interrupted: bool,
}

static WRITERS: LazyLock<Mutex<HashMap<PathBuf, WriterRegistration>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

impl WriterLease {
    async fn acquire<S: DurableStorage>(key: PathBuf, storage: &S) -> io::Result<Arc<Self>> {
        loop {
            let previous = {
                let mut writers = WRITERS
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if writers
                    .get(&key)
                    .is_some_and(|registration| registration.interrupted)
                {
                    return Err(io::Error::other(
                        "interrupted partition WAL writer requires process restart",
                    ));
                }
                if let Some(previous) = writers
                    .get(&key)
                    .and_then(|registration| registration.writer.upgrade())
                {
                    drop(writers);
                    Some(previous)
                } else {
                    let lease = Arc::new(Self {
                        key: key.clone(),
                        id: NEXT_INSTANCE.fetch_add(1, Ordering::Relaxed),
                        retired: AtomicBool::new(false),
                        running: AtomicBool::new(false),
                        waiters: Mutex::new(Vec::new()),
                    });
                    writers.insert(
                        key.clone(),
                        WriterRegistration {
                            id: lease.id,
                            writer: Arc::downgrade(&lease),
                            interrupted: false,
                        },
                    );
                    drop(writers);
                    return Ok(lease);
                }
            };
            let Some(previous) = previous else { continue };
            if !previous.retired.load(Ordering::Acquire) {
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "partition WAL already has an active writer",
                ));
            }
            let drained = futures::future::poll_fn(|context| {
                let mut waiters = previous
                    .waiters
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if !previous.running.load(Ordering::Acquire) {
                    return std::task::Poll::Ready(());
                }
                if !waiters
                    .iter()
                    .any(|waiter| waiter.will_wake(context.waker()))
                {
                    waiters.push(context.waker().clone());
                }
                std::task::Poll::Pending
            })
            .fuse();
            let expired = storage.sleep(PERSISTENCE_DRAIN_TIMEOUT).fuse();
            futures::pin_mut!(drained, expired);
            futures::select_biased! {
                () = drained => {}
                () = expired => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "retired partition WAL writer did not stop",
                    ));
                }
            }
            let mut writers = WRITERS
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if writers.get(&key).is_some_and(|registration| {
                registration.id == previous.id && !registration.interrupted
            }) {
                writers.remove(&key);
            }
        }
    }

    fn finish(&self) {
        self.running.store(false, Ordering::Release);
        for waiter in self
            .waiters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .drain(..)
        {
            waiter.wake();
        }
    }
}

impl Drop for WriterLease {
    fn drop(&mut self) {
        let mut writers = WRITERS
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if writers
            .get(&self.key)
            .is_some_and(|registration| registration.id == self.id && !registration.interrupted)
        {
            writers.remove(&self.key);
        }
    }
}

struct WriterTicket<S: DurableStorage> {
    owner: Rc<PartitionPersistence<S>>,
    started: bool,
}

impl<S: DurableStorage> Drop for WriterTicket<S> {
    fn drop(&mut self) {
        if self.started {
            return;
        }
        self.owner.fail(io::Error::new(
            io::ErrorKind::Interrupted,
            "partition WAL writer was dropped before polling",
        ));
        self.owner.running.set(false);
        if let Some(lease) = &self.owner.lease {
            lease.finish();
        }
        for waiter in self.owner.waiters.borrow_mut().drain(..) {
            waiter.wake();
        }
    }
}

struct WriterGuard<'a, S: DurableStorage> {
    owner: &'a PartitionPersistence<S>,
    journal: Option<Logs<S>>,
    complete: bool,
}

impl<S: DurableStorage> Drop for WriterGuard<'_, S> {
    fn drop(&mut self) {
        *self.owner.journal.borrow_mut() = self.journal.take();
        self.owner.in_flight_bytes.set(0);
        self.owner.checkpoint_running.set(false);
        if !self.complete
            && let Some(lease) = &self.owner.lease
        {
            let mut writers = WRITERS
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(registration) = writers.get_mut(&lease.key)
                && registration.id == lease.id
            {
                registration.interrupted = true;
            }
        }
        if !self.complete && self.owner.failure.borrow().is_none() {
            *self.owner.failure.borrow_mut() = Some(Arc::new(io::Error::new(
                io::ErrorKind::Interrupted,
                "partition WAL writer stopped during a mutation",
            )));
        }
        self.owner.writer_active.set(false);
        self.owner.running.set(false);
        if let Some(lease) = &self.owner.lease {
            lease.finish();
        }
        for waiter in self.owner.waiters.borrow_mut().drain(..) {
            waiter.wake();
        }
    }
}

struct AcceptedPrepares {
    base: u64,
    checksums: VecDeque<u128>,
}

impl AcceptedPrepares {
    /// The last accepted op, or the base when none is.
    fn head(&self) -> u64 {
        u64::try_from(self.checksums.len()).map_or(u64::MAX, |count| self.base + count)
    }

    fn checksum(&self, op: u64) -> Option<u128> {
        let index = usize::try_from(op.checked_sub(self.base)?.checked_sub(1)?).ok()?;
        self.checksums.get(index).copied()
    }

    fn truncate_from(&mut self, op: u64) {
        let keep =
            usize::try_from(op.saturating_sub(self.base).saturating_sub(1)).unwrap_or(usize::MAX);
        self.checksums.truncate(keep);
    }

    fn checkpoint(&mut self, op: u64) {
        let count = usize::try_from(op.saturating_sub(self.base))
            .unwrap_or(usize::MAX)
            .min(self.checksums.len());
        self.checksums.drain(..count);
        self.base = op;
    }
}

enum Mutation<S: DurableStorage> {
    EnableSegments {
        epoch: u64,
        initial: SegmentPosition,
        max_size: u64,
        max_age: Option<u64>,
    },
    ReanchorSegments {
        epoch: u64,
        next_offset: u64,
    },
    CertifyView {
        epoch: u64,
        view: u32,
        op: u64,
        checksum: u128,
    },
    /// Create the state-machine log, queued ahead of the first command so
    /// that command's append pays only its own write.
    OpenCommandLog {
        epoch: u64,
    },
    Append {
        epoch: u64,
        /// Read from the header once, at submission.
        op: u64,
        operation: Operation,
        prepare: Frozen<4096>,
        durable: bool,
        retained_bytes: u64,
    },
    Sync {
        epoch: u64,
    },
    Truncate {
        epoch: u64,
        from_op: u64,
    },
    Barrier {
        epoch: u64,
        barriers: Vec<FileSyncBarrier>,
        offset_files: Vec<RetainedOffsetFile<S::File>>,
    },
    Checkpoint {
        epoch: u64,
        through_op: u64,
        /// The op's checksum in the shared sequence, which anchors a log that
        /// does not hold the op itself.
        checksum: Option<u128>,
        files: Vec<PathBuf>,
        directories: Vec<PathBuf>,
        barriers: Vec<FileSyncBarrier>,
        offset_files: Vec<RetainedOffsetFile<S::File>>,
        synced_files: BTreeSet<PathBuf>,
    },
    Reset {
        epoch: u64,
        op: u64,
        checksum: Option<u128>,
        prepare: Option<Frozen<4096>>,
        segments: Option<(SegmentPosition, u64, Option<u64>)>,
    },
}

/// What a checkpoint syncs before the logs drop the history it covers.
type CheckpointFiles<S> = (
    Vec<PathBuf>,
    Vec<PathBuf>,
    Vec<FileSyncBarrier>,
    Vec<RetainedOffsetFile<<S as DurableStorage>::File>>,
);

struct AppendBatchBytes {
    /// Logical capacity in bytes retained until checkpoint, charged as padded inline records.
    retained_capacity: u64,
    /// Padded WAL extent in bytes encoded in the current storage mode.
    wal_extent: u64,
    /// Unpadded message-body bytes copied into segment storage.
    segment_body: u64,
}

impl PartitionPersistence {
    /// Persist the recovery fence before a replacement WAL can publish an
    /// empty frontier that a later process could mistake for intact history.
    ///
    /// # Errors
    /// Returns an error if the prior frontier or recovery fence cannot be read or written.
    pub async fn fence_missing_history(directory: &Path, incarnation: u64) -> io::Result<()> {
        if !PartitionPrepareJournal::has_published_frontier(directory).await? {
            let partition_directory =
                directory.parent().and_then(Path::to_str).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "invalid partition path")
                })?;
            crate::state_transfer::mark_materialization_missing(partition_directory, incarnation)
                .await?;
        }
        Ok(())
    }

    /// # Errors
    /// Returns an error if the partition WAL cannot be recovered.
    pub async fn open(
        directory: &Path,
        group: u64,
        incarnation: u64,
    ) -> io::Result<(Rc<Self>, Vec<Message<PrepareHeader>>)> {
        Self::open_with_storage(directory, group, incarnation, crate::WalStorage::default()).await
    }
}

impl<S: DurableStorage + Clone> PartitionPersistence<S> {
    /// # Errors
    /// Returns an error if persistence fails, capacity is exhausted, or history is invalid.
    pub async fn open_with_storage(
        directory: &Path,
        group: u64,
        incarnation: u64,
        storage: S,
    ) -> io::Result<(Rc<Self>, Vec<Message<PrepareHeader>>)> {
        Self::open_with_capacity(
            directory,
            group,
            incarnation,
            storage,
            PARTITION_WAL_BYTES_MAX,
            false,
        )
        .await
    }

    /// # Errors
    /// Returns an error for invalid capacity or unverifiable durable history.
    pub async fn open_with_capacity(
        directory: &Path,
        group: u64,
        incarnation: u64,
        storage: S,
        capacity: u64,
        preallocate_segments: bool,
    ) -> io::Result<(Rc<Self>, Vec<Message<PrepareHeader>>)> {
        let partition_directory = directory.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "partition WAL has no parent directory",
            )
        })?;
        let lease = if let Some(key) = storage.writer_identity(partition_directory)? {
            Some(WriterLease::acquire(key, &storage).await?)
        } else {
            None
        };
        let (mut logs, command_storage) = Logs::open(
            directory,
            group,
            incarnation,
            storage,
            capacity,
            preallocate_segments,
        )
        .await?;
        let holds_commands = logs.commands.is_some();
        let prepares = merge_recovered_logs(&mut logs).await?;
        let mut accepted = AcceptedPrepares {
            base: logs.data.checkpoint_op(),
            checksums: VecDeque::with_capacity(prepares.len()),
        };
        accepted.checksums.extend(
            prepares
                .iter()
                .map(Message::header)
                .filter(|header| header.op > accepted.base)
                .map(|header| header.checksum),
        );
        let head = accepted.head();
        let journal = &logs.data;
        let recovered_frontier = journal.recovered_frontier();
        let checkpoint = journal.checkpoint_op();
        let checkpoint_checksum = journal.checkpoint_checksum();
        let certified_log_view = logs.certified_log_view();
        let disk_bytes = logs.size_bytes();
        let retained_bytes = logs.retained_bytes();
        let segment_checkpoint = journal.segment_checkpoint();
        let segment_references = journal.written_segment_references(0).collect();
        let persistence = Rc::new(Self {
            group,
            incarnation,
            instance: NEXT_INSTANCE.fetch_add(1, Ordering::Relaxed),
            recovered_frontier,
            lease,
            epoch: Cell::new(0),
            accepted_head: Cell::new(head),
            written_head: Cell::new(head),
            durable_head: Cell::new(head),
            checkpoint: Cell::new(checkpoint),
            checkpoint_checksum: Cell::new(checkpoint_checksum),
            certified_log_view: Cell::new(certified_log_view),
            requested_log_view: Cell::new(None),
            checkpoint_requested: Cell::new(checkpoint),
            checkpoint_running: Cell::new(false),
            checkpoint_needed: Cell::new(false),
            dirty_segments: RefCell::new(BTreeSet::new()),
            dirty_offsets: std::array::from_fn(|_| RefCell::new(BTreeSet::new())),
            // Recovery may have completed a rename or unlink without its final barrier.
            dirty_offset_directories: std::array::from_fn(|_| Cell::new(true)),
            capacity,
            disk_bytes: Cell::new(disk_bytes),
            retained_bytes: Cell::new(retained_bytes),
            segment_checkpoint: Cell::new(segment_checkpoint),
            segment_references: RefCell::new(segment_references),
            holds_commands: Cell::new(holds_commands),
            command_storage: RefCell::new(command_storage),
            journal: RefCell::new(Some(logs)),
            queue: RefCell::new(VecDeque::new()),
            offset_files: RetainedOffsetFiles::default(),
            accepted: RefCell::new(accepted),
            queued_bytes: PendingBytes::default(),
            in_flight_bytes: PendingBytes::default(),
            waiters: RefCell::new(Vec::new()),
            running: Cell::new(false),
            writer_active: Cell::new(false),
            retired: Cell::new(false),
            enqueue_paused: Cell::new(false),
            failure: RefCell::new(None),
            failure_operation: Cell::new(Operation::SendMessages),
            notifier: RefCell::new(None),
            group_commit_delay: Cell::new(Duration::ZERO),
            append_gap: Cell::new(Duration::MAX),
            last_append: Cell::new(None),
            completed_batches: Cell::new(0),
            batched_prepares: Cell::new(0),
            group_commit_waits: Cell::new(0),
            completed_checkpoints: Cell::new(0),
            failed_writes: Cell::new(0),
            #[cfg(test)]
            stop_between_checkpoints: Cell::new(false),
        });
        Ok((persistence, prepares))
    }
}

impl<S: DurableStorage> PartitionPersistence<S> {
    #[must_use]
    pub const fn recovered_frontier(&self) -> bool {
        self.recovered_frontier
    }

    /// Whether the group's history holds a state-machine command, so the
    /// state-machine log exists or is being created.
    #[must_use]
    pub const fn holds_commands(&self) -> bool {
        self.holds_commands.get()
    }

    #[cfg(test)]
    pub(crate) fn exhaust_capacity_for_test(&self) {
        self.disk_bytes.set(self.capacity);
        self.retained_bytes.set(self.capacity);
    }

    #[cfg(test)]
    pub(crate) fn release_capacity_for_test(&self) {
        self.disk_bytes.set(0);
        self.retained_bytes.set(0);
    }

    pub const fn certified_log_view(&self) -> Option<u32> {
        self.certified_log_view.get()
    }

    pub fn certify_log_view(&self, view: u32, op: u64, checksum: u128) -> bool {
        if self.certified_log_view.get() == Some(view) {
            return true;
        }
        if self.retired.get()
            || self.failure.borrow().is_some()
            || op > self.head()
            || !(op == 0 && checksum == 0 || self.checksum(op) == Some(checksum))
        {
            return false;
        }
        let target = (view, op, checksum);
        if self
            .requested_log_view
            .get()
            .is_none_or(|(pending, _, _)| pending != view)
        {
            self.requested_log_view.set(Some(target));
            self.queue.borrow_mut().push_back(Mutation::CertifyView {
                epoch: self.epoch.get(),
                view,
                op,
                checksum,
            });
        }
        false
    }

    /// Bound on the wait the writer may take before a barrier, to let more
    /// prepares join the group. Zero keeps the writer's barrier-paced grouping.
    pub fn set_group_commit_delay(&self, delay: Duration) {
        self.group_commit_delay.set(delay);
    }

    pub fn set_notifier(&self, notifier: PersistenceNotifier) {
        *self.notifier.borrow_mut() = Some(notifier);
    }

    pub const fn accepts_completion(&self, completion: PersistenceCompletion) -> bool {
        completion.instance == self.instance
            && completion.epoch == self.epoch.get()
            && !self.retired.get()
    }

    pub fn is_durable(&self, header: &PrepareHeader) -> bool {
        self.is_written(header) && header.op <= self.durable_head.get()
    }

    pub fn is_written(&self, header: &PrepareHeader) -> bool {
        self.is_written_through(header.op) && self.checksum(header.op) == Some(header.checksum)
    }

    pub fn is_written_through(&self, op: u64) -> bool {
        !self.retired.get() && self.failure.borrow().is_none() && op <= self.written_head.get()
    }

    /// Where the WAL wrote the body of `op` in a segment, while the WAL still
    /// holds the op.
    pub fn segment_reference(&self, op: u64) -> Option<SegmentReference> {
        self.segment_references.borrow().get(&op).copied()
    }

    pub const fn segment_checkpoint(&self) -> Option<SegmentPosition> {
        self.segment_checkpoint.get()
    }

    /// Queue a reserved offset boundary after checkpointing the preceding chain.
    pub fn reanchor_segments(&self, next_offset: u64) {
        self.queue
            .borrow_mut()
            .push_back(Mutation::ReanchorSegments {
                epoch: self.epoch.get(),
                next_offset,
            });
    }

    pub fn enable_segment_storage(
        &self,
        initial: SegmentPosition,
        max_size: u64,
        max_age: Option<u64>,
    ) {
        self.queue.borrow_mut().push_back(Mutation::EnableSegments {
            epoch: self.epoch.get(),
            initial,
            max_size,
            max_age,
        });
    }

    /// Verify the physical prefix before making its indexes and logical sizes visible.
    ///
    /// # Errors
    /// Returns an error for an incomplete write or an unmet durability requirement.
    pub fn validate_segment_prefix(
        &self,
        prepares: &[Frozen<4096>],
        start_offset: u64,
        mut position: u64,
        durable: bool,
    ) -> io::Result<u64> {
        let references = self.segment_references.borrow();
        let initial = position;
        for prepare in prepares {
            let header = prepare_header(prepare)?;
            let reference = references
                .get(&header.op)
                .filter(|_| {
                    if durable {
                        self.is_durable(header)
                    } else {
                        self.is_written(header)
                    }
                })
                .ok_or_else(|| io::Error::other("committed segment body is not ready"))?;
            if reference.start_offset != start_offset
                || reference.position != position
                || reference.length != (prepare.len() - size_of::<PrepareHeader>()) as u64
            {
                return Err(io::Error::other(
                    "committed prepare differs from its segment position",
                ));
            }
            position = position
                .checked_add(reference.length)
                .ok_or_else(|| io::Error::other("segment prefix overflow"))?;
        }
        Ok(position - initial)
    }

    pub const fn written_op(&self) -> u64 {
        self.written_head.get()
    }

    pub const fn durable_op(&self) -> u64 {
        self.durable_head.get()
    }

    pub fn is_durable_through(&self, op: u64) -> bool {
        self.is_written_through(op) && op <= self.durable_head.get()
    }

    /// Reports the queued and in-flight bytes of this WAL to `reserve` as
    /// pending, whatever the role of the replica: a backup writes what it
    /// journals to the same disk.
    pub fn report_pending_bytes_to(&self, reserve: Option<&Arc<FreeSpaceReserve>>) {
        self.queued_bytes.report_to(reserve.cloned());
        self.in_flight_bytes.report_to(reserve.cloned());
    }

    pub fn has_capacity(&self, frame_bytes: usize) -> bool {
        let Ok(bytes) = journal::partition_journal::record_length(frame_bytes) else {
            return false;
        };
        let bytes = bytes as u64;
        !self.retired.get()
            && !self.enqueue_paused.get()
            && self.failure.borrow().is_none()
            && self
                .retained_bytes
                .get()
                .saturating_add(self.queued_bytes.get())
                .saturating_add(self.in_flight_bytes.get())
                .saturating_add(bytes)
                <= self.capacity
    }

    /// # Errors
    /// Returns an error if persistence fails, capacity is exhausted, or history is invalid.
    pub fn append(&self, prepare: Frozen<4096>, durable: bool) -> io::Result<()> {
        let header = prepare_header(&prepare)?;
        let retained_bytes = journal::partition_journal::record_length(prepare.len())? as u64;
        if self.accepted.borrow().checksum(header.op) == Some(header.checksum) {
            return Ok(());
        }
        if !self.has_capacity(prepare.len()) {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "partition WAL capacity exhausted",
            ));
        }
        if header.op != self.accepted_head.get().saturating_add(1) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "partition WAL submission is out of order",
            ));
        }
        let command = header.operation == Operation::StateMachineCommand;
        // Each log checks the links between the ops it holds. One that crosses
        // into the other log can only be checked here.
        if (command || self.holds_commands.get())
            && self
                .checksum(header.op - 1)
                .is_some_and(|parent| parent != header.parent)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "partition WAL submission does not extend its history",
            ));
        }
        if command && !self.holds_commands.replace(true) {
            self.queue.borrow_mut().push_back(Mutation::OpenCommandLog {
                epoch: self.epoch.get(),
            });
        }
        let now = Instant::now();
        self.append_gap.set(
            self.last_append
                .replace(Some(now))
                .map_or(Duration::MAX, |previous| {
                    now.saturating_duration_since(previous)
                }),
        );
        self.queued_bytes
            .set(self.queued_bytes.get() + retained_bytes);
        self.accepted
            .borrow_mut()
            .checksums
            .push_back(header.checksum);
        self.accepted_head.set(header.op);
        let (op, operation) = (header.op, header.operation);
        self.queue.borrow_mut().push_back(Mutation::Append {
            epoch: self.epoch.get(),
            op,
            operation,
            prepare,
            durable,
            retained_bytes,
        });
        Ok(())
    }

    pub fn mark_segment_dirty(&self, start_offset: u64) {
        self.dirty_segments.borrow_mut().insert(start_offset);
    }

    pub fn take_offset_file(&self, path: &str) -> Option<RetainedOffsetFile<S::File>> {
        self.offset_files.take(path)
    }

    /// Retain the original writer or synchronize it before closing at the budget.
    ///
    /// # Errors
    /// Returns a barrier error that the caller must fence like a failed write.
    pub async fn retain_offset_file(&self, path: String, file: S::File) -> io::Result<()> {
        if let Some(permit) = self.offset_files.reserve(&path) {
            self.offset_files.put(&path, file, permit);
            Ok(())
        } else {
            file.sync().await
        }
    }

    /// An injected file has no disk handle to lend, so it is checked out as absent.
    pub(crate) fn checkout_offset_file(
        &self,
        path: &str,
    ) -> Option<(Option<compio::fs::File>, Rc<OffsetFilePermit>)>
    where
        compio::fs::File: TryFrom<S::File>,
    {
        self.offset_files.checkout(path).map(|(file, permit)| {
            (
                file.and_then(|file| compio::fs::File::try_from(file).ok()),
                permit,
            )
        })
    }

    pub(crate) fn return_offset_file(
        &self,
        path: &str,
        file: compio::fs::File,
        permit: Rc<OffsetFilePermit>,
    ) where
        S::File: From<compio::fs::File>,
    {
        self.offset_files.put(path, S::File::from(file), permit);
    }

    pub fn retire_offset_file(&self, path: &str) {
        self.offset_files.retire(path);
    }

    pub fn mark_offset_dirty(&self, kind_index: usize, consumer_id: u32, exists: bool) {
        self.dirty_offset_directories[kind_index].set(true);
        let mut offsets = self.dirty_offsets[kind_index].borrow_mut();
        if exists {
            offsets.insert(consumer_id);
        } else {
            offsets.remove(&consumer_id);
        }
    }

    pub fn retire_offset_files(&self) {
        self.offset_files.retire_all();
    }

    pub fn take_dirty_files(
        &self,
    ) -> (
        BTreeSet<u64>,
        [BTreeSet<u32>; ConsumerKind::COUNT],
        [bool; ConsumerKind::COUNT],
    ) {
        (
            std::mem::take(&mut *self.dirty_segments.borrow_mut()),
            std::array::from_fn(|index| {
                std::mem::take(&mut *self.dirty_offsets[index].borrow_mut())
            }),
            std::array::from_fn(|index| self.dirty_offset_directories[index].replace(false)),
        )
    }

    pub fn checkpoint(&self, through_op: u64) {
        // This shorthand has no materialized paths, so
        // [`FileSyncBarrier`] has no original writer to retain.
        self.checkpoint_files(through_op, Vec::new(), Vec::new(), Vec::new());
    }

    pub fn checkpoint_files(
        &self,
        through_op: u64,
        files: Vec<PathBuf>,
        directories: Vec<PathBuf>,
        barriers: Vec<FileSyncBarrier>,
    ) {
        if through_op <= self.checkpoint_requested.get() {
            return;
        }
        self.checkpoint_requested.set(through_op);
        self.checkpoint_needed.set(false);
        let (offset_files, synced_files) = self.offset_files.take_checkpoint();
        self.queue.borrow_mut().push_back(Mutation::Checkpoint {
            epoch: self.epoch.get(),
            through_op,
            checksum: self.checksum(through_op),
            files,
            directories,
            barriers,
            offset_files,
            synced_files,
        });
    }

    /// Queue a durability barrier through retained original writers without
    /// advancing [`PartitionPersistence::checkpoint_op`]. The returned paths
    /// count as synced only after the queued barrier succeeds.
    pub fn barrier_files(&self, barriers: Vec<FileSyncBarrier>) -> BTreeSet<PathBuf> {
        let (offset_files, mut synced_files) = self.offset_files.take_checkpoint();
        synced_files.extend(barriers.iter().map(|barrier| barrier.path.clone()));
        self.queue.borrow_mut().push_back(Mutation::Barrier {
            epoch: self.epoch.get(),
            barriers,
            offset_files,
        });
        synced_files
    }

    pub const fn checkpoint_pending(&self) -> bool {
        self.checkpoint_running.get() || self.checkpoint_requested.get() > self.checkpoint.get()
    }

    pub fn request_checkpoint(&self) {
        self.checkpoint_needed.set(true);
    }

    pub fn truncate_from(&self, from_op: u64) {
        if from_op > self.accepted_head.get() {
            return;
        }
        self.checkpoint_requested.set(self.checkpoint.get());
        for dirty in &self.dirty_offset_directories {
            dirty.set(true);
        }
        self.certified_log_view.set(None);
        self.requested_log_view.set(None);
        let epoch = self.epoch.get().wrapping_add(1);
        self.epoch.set(epoch);
        // Retained submissions still precede the replacement history.
        self.queue
            .borrow_mut()
            .retain_mut(|mutation| match mutation {
                Mutation::Append {
                    epoch: previous,
                    op,
                    retained_bytes,
                    ..
                } => {
                    if *op < from_op {
                        *previous = epoch;
                        true
                    } else {
                        self.queued_bytes
                            .set(self.queued_bytes.get().saturating_sub(*retained_bytes));
                        false
                    }
                }
                Mutation::OpenCommandLog { epoch: previous } => {
                    *previous = epoch;
                    true
                }
                Mutation::Checkpoint {
                    epoch: previous,
                    through_op,
                    ..
                } if *through_op < from_op => {
                    *previous = epoch;
                    self.checkpoint_requested
                        .set(self.checkpoint_requested.get().max(*through_op));
                    true
                }
                Mutation::Barrier { .. } => true,
                _ => false,
            });
        self.accepted.borrow_mut().truncate_from(from_op);
        self.segment_references.borrow_mut().split_off(&from_op);
        self.accepted_head.set(from_op.saturating_sub(1));
        self.written_head
            .set(self.written_head.get().min(from_op.saturating_sub(1)));
        self.durable_head
            .set(self.durable_head.get().min(from_op.saturating_sub(1)));
        self.queue
            .borrow_mut()
            .push_back(Mutation::Truncate { epoch, from_op });
    }

    pub fn reset(&self, op: u64, checksum: Option<u128>) {
        self.reset_with_prepare(op, checksum, None);
    }

    pub fn reset_with_prepare(
        &self,
        op: u64,
        checksum: Option<u128>,
        prepare: Option<Frozen<4096>>,
    ) {
        self.reset_with_segments(op, checksum, prepare, None);
    }

    pub fn reset_with_segments(
        &self,
        op: u64,
        checksum: Option<u128>,
        prepare: Option<Frozen<4096>>,
        segments: Option<(SegmentPosition, u64, Option<u64>)>,
    ) {
        self.offset_files.clear();
        self.certified_log_view.set(None);
        self.requested_log_view.set(None);
        self.checkpoint_requested.set(op);
        for dirty in &self.dirty_offset_directories {
            dirty.set(true);
        }
        let epoch = self.epoch.get().wrapping_add(1);
        self.epoch.set(epoch);
        self.queue.borrow_mut().clear();
        self.queued_bytes.set(0);
        *self.accepted.borrow_mut() = AcceptedPrepares {
            base: op,
            checksums: VecDeque::new(),
        };
        self.accepted_head.set(op);
        self.written_head.set(0);
        self.durable_head.set(0);
        self.segment_references.borrow_mut().clear();
        self.queue.borrow_mut().push_back(Mutation::Reset {
            epoch,
            op,
            checksum,
            prepare,
            segments,
        });
    }

    pub fn needs_checkpoint(&self) -> bool {
        !self.checkpoint_pending()
            && (self.checkpoint_needed.get()
                || self.retained_bytes.get() + self.queued_bytes.get() + self.in_flight_bytes.get()
                    >= self.capacity / 2
                || self.dirty_segments.borrow().len() * 2
                    + self
                        .dirty_offsets
                        .iter()
                        .map(|offsets| offsets.borrow().len())
                        .sum::<usize>()
                    >= CHECKPOINT_DIRTY_FILES_MAX)
    }

    /// Whether a Replicated commit may pass a body the writer has not written.
    /// A pending checkpoint holds the writer, so only admission bounds the
    /// backlog then. A due checkpoint starts only once the writer reaches
    /// `commit_min`, so commits wait for the writer until it starts.
    pub fn can_run_ahead(&self) -> bool {
        self.checkpoint_pending()
            || (!self.needs_checkpoint()
                && self.queued_bytes.get() + self.in_flight_bytes.get() < RUN_AHEAD_BYTES_MAX)
    }

    pub fn take_metrics(&self) -> PersistenceMetrics {
        PersistenceMetrics {
            disk_bytes: self.disk_bytes.get(),
            retained_bytes: self.retained_bytes.get(),
            queued_bytes: self.queued_bytes.get(),
            in_flight_bytes: self.in_flight_bytes.get(),
            checkpoints_pending: u64::from(self.checkpoint_pending()),
            completed_batches: self.completed_batches.replace(0),
            batched_prepares: self.batched_prepares.replace(0),
            group_commit_waits: self.group_commit_waits.replace(0),
            completed_checkpoints: self.completed_checkpoints.replace(0),
            failed_writes: self.failed_writes.replace(0),
        }
    }

    pub fn fail(&self, error: io::Error) {
        self.fail_operation(error, Operation::SendMessages);
    }

    pub fn fail_operation(&self, error: io::Error, operation: Operation) {
        self.failure_operation.set(operation);
        self.failed_writes.set(self.failed_writes.get() + 1);
        *self.failure.borrow_mut() = Some(Arc::new(error));
        self.notify();
    }

    pub fn failure(&self) -> Option<Arc<io::Error>> {
        self.failure.borrow().clone()
    }

    pub const fn failure_operation(&self) -> Operation {
        self.failure_operation.get()
    }

    pub const fn checkpoint_op(&self) -> u64 {
        self.checkpoint.get()
    }

    pub fn checksum(&self, op: u64) -> Option<u128> {
        if op == self.checkpoint.get() {
            self.checkpoint_checksum.get()
        } else {
            self.accepted.borrow().checksum(op)
        }
    }

    pub const fn head(&self) -> u64 {
        self.accepted_head.get()
    }

    /// The WAL byte budget, `wal_bytes_max` in the server configuration.
    pub const fn capacity(&self) -> u64 {
        self.capacity
    }

    pub fn retire(&self) {
        self.retired.set(true);
        if let Some(lease) = &self.lease {
            lease.retired.store(true, Ordering::Release);
        }
        self.queue.borrow_mut().clear();
        self.queued_bytes.set(0);
    }

    pub(crate) fn begin_drain(&self) -> PersistenceDrain {
        self.enqueue_paused.set(true);
        PersistenceDrain {
            instance: self.instance,
            epoch: self.epoch.get(),
            started: Instant::now(),
        }
    }

    pub fn is_quiescent(&self) -> bool {
        !self.running.get() && !self.writer_active.get() && self.queue.borrow().is_empty()
    }

    /// # Errors
    /// Preserves storage/interruption failures and the original drain deadline.
    pub(crate) fn observe_drain(&self, drain: &PersistenceDrain) -> io::Result<bool> {
        if drain.instance != self.instance || drain.epoch != self.epoch.get() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "partition WAL drain epoch changed",
            ));
        }
        if !self.running.get() && !self.writer_active.get() {
            if let Some(error) = self.failure() {
                return Err(io::Error::new(error.kind(), error));
            }
            if self.queue.borrow().is_empty() {
                return Ok(true);
            }
        }
        if drain.started.elapsed() >= PERSISTENCE_DRAIN_TIMEOUT {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "partition WAL drain timed out",
            ));
        }
        Ok(false)
    }

    pub(crate) fn finish_drain(&self, drain: &PersistenceDrain) {
        if drain.instance == self.instance && drain.epoch == self.epoch.get() {
            self.enqueue_paused.set(false);
        }
    }

    /// # Errors
    /// Returns an error if persistence fails, capacity is exhausted, or history is invalid.
    pub async fn drain(&self) -> io::Result<()> {
        futures::future::poll_fn(|context| {
            if !self.running.get()
                && let Some(error) = self.failure()
            {
                return std::task::Poll::Ready(Err(io::Error::new(error.kind(), error)));
            }
            if !self.running.get() && self.queue.borrow().is_empty() {
                return std::task::Poll::Ready(Ok(()));
            }
            let mut waiters = self.waiters.borrow_mut();
            if !waiters
                .iter()
                .any(|waiter| waiter.will_wake(context.waker()))
            {
                waiters.push(context.waker().clone());
            }
            std::task::Poll::Pending
        })
        .await
    }

    /// Times out on the caller's clock, so a simulated owner drains in virtual time.
    ///
    /// # Errors
    /// Returns a storage failure or a timeout without allowing a second writer.
    pub async fn drain_with_timeout<F>(&self, sleep: impl FnOnce(Duration) -> F) -> io::Result<()>
    where
        F: Future<Output = ()>,
    {
        let drain = std::pin::pin!(self.drain());
        let timeout = std::pin::pin!(sleep(PERSISTENCE_DRAIN_TIMEOUT));
        match futures::future::select(drain, timeout).await {
            Either::Left((result, _)) => result,
            Either::Right(((), _)) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "partition WAL drain timed out",
            )),
        }
    }

    /// Queue a barrier for preceding buffered writes; drain before claiming durability.
    pub fn sync(&self) {
        self.queue.borrow_mut().push_back(Mutation::Sync {
            epoch: self.epoch.get(),
        });
    }

    pub fn start(&self) -> bool {
        let start = !self.retired.get()
            && self.failure.borrow().is_none()
            && !self.queue.borrow().is_empty()
            && !self.running.replace(true);
        if start && let Some(lease) = &self.lease {
            lease.running.store(true, Ordering::Release);
        }
        start
    }

    pub fn run(self: Rc<Self>) -> impl Future<Output = ()> {
        let mut ticket = WriterTicket {
            owner: self,
            started: false,
        };
        async move {
            ticket.started = true;
            Rc::clone(&ticket.owner).run_inner().await;
        }
    }

    async fn run_inner(self: Rc<Self>) {
        if self.writer_active.replace(true) {
            self.fail(io::Error::other("partition WAL writer was started twice"));
            return;
        }
        self.running.set(true);
        if let Some(lease) = &self.lease {
            lease.running.store(true, Ordering::Release);
        }
        let journal = self.journal.borrow_mut().take();
        let mut guard = WriterGuard {
            owner: &self,
            journal,
            complete: false,
        };
        let Some(logs) = guard.journal.as_mut() else {
            self.fail(io::Error::other("partition WAL writer has no journal"));
            guard.complete = true;
            return;
        };
        let mut mutations_since_reclaim = 0u32;
        loop {
            if self.retired.get() {
                break;
            }
            let Some(mutation) = self.queue.borrow_mut().pop_front() else {
                break;
            };
            let (epoch, retained_bytes) = match &mutation {
                Mutation::Append {
                    epoch,
                    retained_bytes,
                    ..
                } => (*epoch, *retained_bytes),
                Mutation::CertifyView { epoch, .. }
                | Mutation::OpenCommandLog { epoch }
                | Mutation::Sync { epoch }
                | Mutation::EnableSegments { epoch, .. }
                | Mutation::ReanchorSegments { epoch, .. }
                | Mutation::Truncate { epoch, .. }
                | Mutation::Barrier { epoch, .. }
                | Mutation::Checkpoint { epoch, .. }
                | Mutation::Reset { epoch, .. } => (*epoch, 0),
            };
            self.queued_bytes
                .set(self.queued_bytes.get().saturating_sub(retained_bytes));
            self.in_flight_bytes.set(retained_bytes);
            let rebuild_references = matches!(
                mutation,
                Mutation::EnableSegments { .. }
                    | Mutation::ReanchorSegments { .. }
                    | Mutation::Truncate { .. }
                    | Mutation::Checkpoint { .. }
                    | Mutation::Reset { .. }
            );
            let result = self
                .apply_mutation(logs, mutation, epoch, retained_bytes)
                .await;
            self.in_flight_bytes.set(0);
            if let Err(error) = result {
                self.failed_writes.set(self.failed_writes.get() + 1);
                *self.failure.borrow_mut() = Some(Arc::new(error));
                self.notify();
                break;
            }
            if epoch == self.epoch.get() && !self.retired.get() {
                self.publish_mutation(logs, rebuild_references);
            }
            // Reclaim after publishing the mutation when the queue is empty.
            // Appends arriving during reclaim still wait for its unlinks and
            // directory barrier.
            // A partition that never drains would then never reclaim, so force
            // a pass every `RECLAIM_MUTATIONS_MAX` mutations and accept that
            // one group's latency.
            mutations_since_reclaim += 1;
            let idle = self.queue.borrow().is_empty();
            if idle || mutations_since_reclaim >= RECLAIM_MUTATIONS_MAX {
                mutations_since_reclaim = 0;
                logs.data.cleanup_obsolete().await;
                if let Some(commands) = logs.commands.as_mut() {
                    commands.cleanup_obsolete().await;
                }
            }
        }
        guard.complete = true;
    }

    /// Republish what the completed mutation moved, and wake the partition when
    /// any of it advanced. Runs only while the writer still owns this epoch: a
    /// retired or re-epoched writer must not overwrite the state its successor
    /// published.
    fn publish_mutation(&self, logs: &Logs<S>, rebuild_references: bool) {
        let journal = &logs.data;
        let mut references = self.segment_references.borrow_mut();
        if rebuild_references {
            references.clear();
            references.extend(journal.written_segment_references(0));
        } else if let Some(from_op) = self.written_head.get().checked_add(1) {
            references.extend(journal.written_segment_references(from_op));
        }
        drop(references);
        self.disk_bytes.set(logs.size_bytes());
        self.retained_bytes.set(logs.retained_bytes());
        self.segment_checkpoint.set(journal.segment_checkpoint());
        let written_head = logs.written_head();
        let durable_head = logs.durable_head();
        let certified_log_view = logs.certified_log_view();
        let advanced = durable_head != self.durable_head.get()
            || journal.checkpoint_op() != self.checkpoint.get()
            || certified_log_view != self.certified_log_view.get()
            || (journal.segment_checkpoint().is_some() && written_head != self.written_head.get());
        self.certified_log_view.set(certified_log_view);
        if self
            .requested_log_view
            .get()
            .is_some_and(|(view, _, _)| Some(view) == self.certified_log_view.get())
        {
            self.requested_log_view.set(None);
        }
        self.written_head.set(written_head);
        self.durable_head.set(durable_head);
        if journal.checkpoint_op() > self.checkpoint.get() {
            self.accepted
                .borrow_mut()
                .checkpoint(journal.checkpoint_op());
        }
        self.checkpoint.set(journal.checkpoint_op());
        self.checkpoint_checksum.set(journal.checkpoint_checksum());
        if advanced {
            self.notify();
        }
    }

    async fn apply_mutation(
        &self,
        logs: &mut Logs<S>,
        mutation: Mutation<S>,
        epoch: u64,
        retained_bytes: u64,
    ) -> io::Result<()> {
        match mutation {
            Mutation::EnableSegments {
                initial,
                max_size,
                max_age,
                ..
            } => {
                logs.data
                    .enable_segment_storage(initial, max_size, max_age)
                    .await
            }
            Mutation::ReanchorSegments { next_offset, .. } => {
                logs.data.reanchor_segment_storage(next_offset).await
            }
            Mutation::CertifyView {
                view, op, checksum, ..
            } => Self::certify_logs(logs, view, op, checksum).await,
            Mutation::OpenCommandLog { .. } => self.command_log(logs).await.map(|_| ()),
            Mutation::Append {
                operation,
                prepare,
                durable,
                ..
            } => {
                if operation == Operation::StateMachineCommand {
                    self.append_commands(logs, prepare, epoch, retained_bytes)
                        .await
                } else {
                    self.append_batch(
                        &mut logs.data,
                        (operation, prepare),
                        durable,
                        epoch,
                        retained_bytes,
                    )
                    .await
                }
            }
            Mutation::Sync { .. } => {
                logs.data.sync().await?;
                if let Some(commands) = logs.commands.as_mut() {
                    commands.sync().await?;
                }
                Ok(())
            }
            Mutation::Truncate { from_op, .. } => {
                logs.data.truncate_from(from_op).await?;
                if let Some(commands) = logs.commands.as_mut() {
                    commands.truncate_from(from_op).await?;
                }
                Ok(())
            }
            Mutation::Barrier {
                barriers,
                offset_files,
                ..
            } => {
                Self::sync_files(offset_files, barriers).await?;
                logs.data.sync().await
            }
            Mutation::Checkpoint {
                through_op,
                checksum,
                files,
                directories,
                barriers,
                offset_files,
                synced_files,
                ..
            } => {
                self.checkpoint_running.set(true);
                let result = self
                    .checkpoint_logs(
                        logs,
                        through_op,
                        checksum,
                        (files, directories, barriers, offset_files),
                        synced_files,
                    )
                    .await;
                self.checkpoint_running.set(false);
                if result.is_ok() {
                    self.completed_checkpoints
                        .set(self.completed_checkpoints.get() + 1);
                }
                result
            }
            Mutation::Reset {
                op,
                checksum,
                prepare,
                segments,
                ..
            } => self.reset_logs(logs, op, checksum, prepare, segments).await,
        }
    }

    /// Certify both logs at once, so a view change waits for one barrier
    /// chain rather than two in a row.
    async fn certify_logs(
        logs: &mut Logs<S>,
        view: u32,
        op: u64,
        checksum: u128,
    ) -> io::Result<()> {
        let Logs { data, commands } = logs;
        let (data, commands) =
            futures::future::join(data.certify_log_view(view, op, checksum), async {
                match commands.as_mut() {
                    Some(commands) => commands.certify_log_view(view, op, checksum).await,
                    None => Ok(()),
                }
            })
            .await;
        data.and(commands)
    }

    async fn checkpoint_logs(
        &self,
        logs: &mut Logs<S>,
        through_op: u64,
        checksum: Option<u128>,
        (files, directories, barriers, offset_files): CheckpointFiles<S>,
        mut synced_files: BTreeSet<PathBuf>,
    ) -> io::Result<()> {
        synced_files.extend(Self::sync_files(offset_files, barriers).await?);
        if let Some(checksum) = checksum {
            logs.data
                .checkpoint_files_at(through_op, checksum, &files, &directories, &synced_files)
                .await?;
        } else {
            logs.data
                .checkpoint_files(through_op, &files, &directories, &synced_files)
                .await?;
        }
        #[cfg(test)]
        if self.stop_between_checkpoints.get() {
            return Err(io::Error::other(
                "stopped between the two partition log checkpoints",
            ));
        }
        // After the data log: until it publishes, its older checkpoint is the
        // replay point, and the commands above that are still here. A log
        // holding nothing above its own checkpoint through `through_op` has
        // nothing to reclaim and keeps its older checkpoint, which recovery
        // tolerates because it drops every command below the data log's.
        if let (Some(commands), Some(checksum)) = (logs.commands.as_mut(), checksum)
            && commands
                .first_op_above(commands.checkpoint_op())
                .is_some_and(|op| op <= through_op)
        {
            commands.checkpoint_at(through_op, checksum).await?;
        }
        Ok(())
    }

    async fn reset_logs(
        &self,
        logs: &mut Logs<S>,
        op: u64,
        checksum: Option<u128>,
        prepare: Option<Frozen<4096>>,
        segments: Option<(SegmentPosition, u64, Option<u64>)>,
    ) -> io::Result<()> {
        let (data_prepare, command_prepare) = match prepare {
            Some(prepare)
                if prepare_header(&prepare)?.operation == Operation::StateMachineCommand =>
            {
                (None, Some(prepare))
            }
            prepare => (prepare, None),
        };
        if let Some((position, max_size, max_age)) = segments {
            logs.data
                .reset_with_segment_checkpoint(
                    op,
                    checksum,
                    data_prepare,
                    position,
                    max_size,
                    max_age,
                )
                .await?;
        } else if let Some(prepare) = data_prepare {
            logs.data.reset_with_prepare(prepare).await?;
        } else {
            logs.data.reset(op, checksum).await?;
        }
        if logs.commands.is_none() && command_prepare.is_none() {
            return Ok(());
        }
        let commands = self.command_log(logs).await?;
        match command_prepare {
            Some(prepare) => commands.reset_with_prepare(prepare).await,
            None => commands.reset(op, checksum).await,
        }
    }

    /// Commands always ask for durability. Queued ones that follow each other
    /// share one write and one barrier, and that barrier covers the data log
    /// too: a command is durable only once every op below it is, and the data
    /// ops a replicated group wrote without a barrier would otherwise hold it,
    /// and every acknowledgment after it, until something else synced them.
    async fn append_commands(
        &self,
        logs: &mut Logs<S>,
        first: Frozen<4096>,
        epoch: u64,
        first_retained_bytes: u64,
    ) -> io::Result<()> {
        self.command_log(logs).await?;
        let mut batch = SmallVec::<[Frozen<4096>; 8]>::new();
        batch.push(first);
        let retained_capacity =
            first_retained_bytes + self.collect_queued_commands(&mut batch, epoch)?;
        self.in_flight_bytes.set(retained_capacity);
        let Logs { data, commands } = logs;
        let commands = commands
            .as_mut()
            .ok_or_else(|| io::Error::other("partition state-machine log is not open"))?;
        commands.append_batch_buffered(&batch).await?;
        let (data_synced, commands_synced) =
            futures::future::join(data.sync(), commands.sync()).await;
        data_synced?;
        commands_synced
    }

    /// Move the commands queued right behind the one being written into
    /// `batch`, returning the capacity they retain.
    fn collect_queued_commands(
        &self,
        batch: &mut SmallVec<[Frozen<4096>; 8]>,
        epoch: u64,
    ) -> io::Result<u64> {
        let mut wal_extent = batch.iter().try_fold(0u64, |extent, prepare| {
            journal::partition_journal::record_length(prepare.len())
                .map(|length| extent + length as u64)
        })?;
        let mut retained_capacity = 0;
        let mut queue = self.queue.borrow_mut();
        while batch.len() < APPEND_BATCH_OPS_MAX {
            let Some(Mutation::Append {
                epoch: next_epoch,
                operation,
                prepare,
                ..
            }) = queue.front()
            else {
                break;
            };
            if *next_epoch != epoch || *operation != Operation::StateMachineCommand {
                break;
            }
            let length = journal::partition_journal::record_length(prepare.len())? as u64;
            if wal_extent.saturating_add(length) > APPEND_BATCH_WAL_BYTES_MAX {
                break;
            }
            let Some(Mutation::Append {
                prepare,
                retained_bytes,
                ..
            }) = queue.pop_front()
            else {
                unreachable!("command prefix was checked");
            };
            wal_extent += length;
            retained_capacity += retained_bytes;
            self.queued_bytes
                .set(self.queued_bytes.get().saturating_sub(retained_bytes));
            batch.push(prepare);
        }
        Ok(retained_capacity)
    }

    /// The state-machine log, created with the first command. The data log
    /// accepts gaps from then on, where the commands sit.
    async fn command_log<'a>(
        &self,
        logs: &'a mut Logs<S>,
    ) -> io::Result<&'a mut PartitionPrepareJournal<S>> {
        if logs.commands.is_none() {
            let storage =
                self.command_storage.borrow_mut().take().ok_or_else(|| {
                    io::Error::other("partition state-machine log storage is gone")
                })?;
            // The pair is certified only at the lower view of the two, so a new
            // log published at view zero would withdraw the data log's
            // certificate mid-view, and a crash before a later certification
            // would boot uncertified. Empty, it matches any view's history at
            // the genesis checkpoint, so its first frontier names the data
            // log's view.
            let commands = PartitionPrepareJournal::open_with_options(
                &logs.data.directory().join(STATE_MACHINE_LOG_DIRECTORY),
                self.group,
                self.incarnation,
                storage,
                WalOpenOptions {
                    capacity: self.capacity,
                    sparse: true,
                    new_log_view: logs.data.certified_log_view().unwrap_or(0),
                    ..WalOpenOptions::default()
                },
            )
            .await?;
            logs.data.allow_gaps();
            logs.commands = Some(commands);
            self.holds_commands.set(true);
        }
        logs.commands
            .as_mut()
            .ok_or_else(|| io::Error::other("partition state-machine log is not open"))
    }

    async fn sync_files(
        offset_files: Vec<RetainedOffsetFile<S::File>>,
        barriers: Vec<FileSyncBarrier>,
    ) -> io::Result<Vec<PathBuf>> {
        futures::stream::iter(offset_files.iter().map(Ok::<_, io::Error>))
            .try_for_each_concurrent(FILE_SYNC_CONCURRENCY, |retained| retained.file.sync())
            .await?;
        futures::future::try_join_all(barriers.into_iter().map(FileSyncBarrier::run)).await
    }

    async fn append_batch(
        &self,
        journal: &mut PartitionPrepareJournal<S>,
        (operation, first): (Operation, Frozen<4096>),
        durable: bool,
        epoch: u64,
        first_retained_bytes: u64,
    ) -> io::Result<()> {
        let (first_wal_bytes, first_segment_body_bytes) =
            journal.append_lengths(operation, first.len())?;
        let mut bytes = AppendBatchBytes {
            retained_capacity: first_retained_bytes,
            wal_extent: first_wal_bytes as u64,
            segment_body: first_segment_body_bytes as u64,
        };
        let mut batch = SmallVec::<[Frozen<4096>; 8]>::new();
        batch.push(first);
        let mut durable = durable;
        self.collect_queued(journal, &mut batch, &mut bytes, &mut durable, epoch)?;
        // Charged before the wait: `collect_queued` took these bytes out of the
        // queued total, and admission and checkpoint pacing sum queued and
        // in-flight bytes against the budget, so a gap here would admit a full
        // group past it.
        self.in_flight_bytes.set(bytes.retained_capacity);
        // The barrier is what groups prepares, so a barrier cheaper than the
        // interval between arrivals groups nothing and every prepare pays its
        // own writes. This wait puts that grouping back under operator control.
        if durable && let Some(delay) = self.group_commit_wait(&batch, &bytes) {
            self.group_commit_waits
                .set(self.group_commit_waits.get() + 1);
            compio::runtime::time::sleep(delay).await;
            self.collect_queued(journal, &mut batch, &mut bytes, &mut durable, epoch)?;
            self.in_flight_bytes.set(bytes.retained_capacity);
        }
        let count = batch.len() as u64;
        journal.append_batch_buffered(&batch).await?;
        if durable {
            journal.sync().await?;
            self.completed_batches.set(self.completed_batches.get() + 1);
            self.batched_prepares
                .set(self.batched_prepares.get() + count);
        }
        Ok(())
    }

    /// Move every queued append that still fits into `batch`.
    fn collect_queued(
        &self,
        journal: &PartitionPrepareJournal<S>,
        batch: &mut SmallVec<[Frozen<4096>; 8]>,
        bytes: &mut AppendBatchBytes,
        durable: &mut bool,
        epoch: u64,
    ) -> io::Result<()> {
        let mut queue = self.queue.borrow_mut();
        while batch.len() < APPEND_BATCH_OPS_MAX {
            let Some(Mutation::Append {
                epoch: next_epoch,
                operation,
                prepare,
                ..
            }) = queue.front()
            else {
                break;
            };
            if *next_epoch != epoch || *operation == Operation::StateMachineCommand {
                break;
            }
            let (next_wal_bytes, next_segment_body_bytes) =
                journal.append_lengths(*operation, prepare.len())?;
            let next_wal_bytes = next_wal_bytes as u64;
            let next_segment_bytes = next_segment_body_bytes as u64;
            // Inline prepares count their full padded WAL extent and zero segment
            // bytes. Segment-backed SendMessages count a padded reference record
            // in the WAL and their unpadded body bytes against the segment limit.
            if bytes.wal_extent.saturating_add(next_wal_bytes) > APPEND_BATCH_WAL_BYTES_MAX
                || bytes.segment_body.saturating_add(next_segment_bytes)
                    > APPEND_BATCH_SEGMENT_BYTES_MAX
            {
                break;
            }
            let Some(Mutation::Append {
                prepare,
                durable: requires_sync,
                retained_bytes: record_retained_bytes,
                ..
            }) = queue.pop_front()
            else {
                unreachable!("append prefix was checked");
            };
            bytes.retained_capacity += record_retained_bytes;
            bytes.wal_extent += next_wal_bytes;
            bytes.segment_body += next_segment_bytes;
            self.queued_bytes.set(
                self.queued_bytes
                    .get()
                    .saturating_sub(record_retained_bytes),
            );
            *durable |= requires_sync;
            batch.push(prepare);
        }
        Ok(())
    }

    /// How long to wait for more prepares before the barrier, if at all.
    ///
    /// `None` for a disabled delay, a group already at its bounds, or arrivals
    /// spaced wider than the delay, where the wait would expire before the next
    /// prepare reached the queue.
    fn group_commit_wait(
        &self,
        batch: &[Frozen<4096>],
        bytes: &AppendBatchBytes,
    ) -> Option<Duration> {
        let delay = self.group_commit_delay.get();
        // Inline prepares are bounded by their full padded WAL extent. Segment-
        // backed SendMessages are bounded by both their reference-record extent
        // and the message bodies copied into segment storage.
        if delay.is_zero()
            || batch.len() >= APPEND_BATCH_OPS_MAX
            || bytes.wal_extent >= APPEND_BATCH_WAL_BYTES_MAX
            || bytes.segment_body >= APPEND_BATCH_SEGMENT_BYTES_MAX
        {
            return None;
        }
        (self.append_gap.get() <= delay).then_some(delay)
    }

    fn notify(&self) {
        if let Some(notifier) = self.notifier.borrow().as_ref() {
            notifier(PersistenceCompletion {
                group: self.group,
                instance: self.instance,
                epoch: self.epoch.get(),
            });
        }
    }
}

fn prepare_header(prepare: &Frozen<4096>) -> io::Result<&PrepareHeader> {
    let bytes = prepare
        .as_slice()
        .get(..size_of::<PrepareHeader>())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "short prepare"))?;
    bytemuck::checked::try_from_bytes(bytes)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid prepare"))
}

/// The prepares both logs recovered, merged back into the group's sequence.
///
/// The data log's checkpoint is the replay point. The machines' snapshot at
/// that op is durable before the data log publishes it, so a command at or
/// below it is already applied and stays out of the replay, apart from the
/// command that is the checkpoint prepare itself.
///
/// Above the checkpoint the sequence must be whole. An op is acknowledged only
/// once both logs hold everything below it, so a hole is a tail one log lost
/// before its barrier, and both logs drop everything from the hole on, keeping
/// their certificates. Two adjacent ops that do not link are damage, and the
/// open fails, as does a command at the checkpoint that is not the prepare the
/// data log checkpointed.
async fn merge_recovered_logs<S: DurableStorage>(
    logs: &mut Logs<S>,
) -> io::Result<Vec<Message<PrepareHeader>>> {
    let data = logs.data.take_recovered_prepares();
    let Some(commands) = logs.commands.as_mut() else {
        return Ok(data);
    };
    let checkpoint = logs.data.checkpoint_op();
    let commands: Vec<_> = commands
        .take_recovered_prepares()
        .into_iter()
        .filter(|prepare| prepare.header().op >= checkpoint)
        .collect();
    let mut merged = Vec::with_capacity(data.len() + commands.len());
    let mut data = data.into_iter().peekable();
    let mut commands = commands.into_iter().peekable();
    loop {
        let take_data = match (data.peek(), commands.peek()) {
            (Some(left), Some(right)) => {
                if left.header().op == right.header().op {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "an op is held by both partition logs",
                    ));
                }
                left.header().op < right.header().op
            }
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => break,
        };
        merged.extend(if take_data {
            data.next()
        } else {
            commands.next()
        });
    }
    let mut previous = checkpoint;
    let mut previous_checksum = logs.data.checkpoint_checksum();
    let mut kept = merged.len();
    for (index, prepare) in merged.iter().enumerate() {
        let header = prepare.header();
        if header.op == checkpoint
            && previous_checksum.is_some_and(|checksum| checksum != header.checksum)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "partition checkpoint prepare differs from the checkpointed op",
            ));
        }
        if header.op <= checkpoint {
            continue;
        }
        if header.op != previous + 1 {
            kept = index;
            break;
        }
        if previous_checksum.is_some_and(|checksum| checksum != header.parent) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "partition log prepare chain is broken across the two logs",
            ));
        }
        previous = header.op;
        previous_checksum = Some(header.checksum);
    }
    if kept < merged.len() {
        merged.truncate(kept);
        logs.data.discard_unacknowledged_tail(previous + 1).await?;
        if let Some(commands) = logs.commands.as_mut() {
            commands.discard_unacknowledged_tail(previous + 1).await?;
        }
    }
    Ok(merged)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::{Command, Operation};
    use journal::DurableAppend;
    use server_common::{Message, iobuf::Owned};
    use tempfile::tempdir;

    /// `io::ErrorKind::StorageFull` is still unstable, so match the raw errno.
    const ENOSPC: i32 = 28;

    /// A full disk is a refused write, not a torn one: the prior bytes are
    /// intact and nothing is undefined. Latching it fences the partition for the
    /// life of the process, and `drive_persistence` escalates that to a node
    /// shutdown. The same function already treats open failures as retriable.
    #[compio::test]
    #[ignore = "PR #4092 review: a refused consumer-offset write latches an unclearable `failure`, retroactively reporting already-acked prepares as unwritten"]
    async fn given_a_full_disk_when_the_offset_write_is_refused_then_persistence_should_not_fence()
    {
        let directory = tempdir().unwrap();
        let (persistence, _) =
            PartitionPersistence::open(&directory.path().join("prepares-7"), 42, 7)
                .await
                .unwrap();
        let first = prepare(1, 0);
        persistence
            .append(first.clone().into_frozen(), true)
            .unwrap();
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        assert!(persistence.is_written(first.header()));

        persistence.fail_operation(
            io::Error::from_raw_os_error(ENOSPC),
            Operation::StoreConsumerOffset,
        );

        assert!(
            persistence.is_written(first.header()),
            "an unrelated offset write reported an already-durable prepare as unwritten"
        );
        assert!(
            persistence.failure().is_none(),
            "ENOSPC on one consumer-offset record latched the partition; nothing clears `failure` (its only `None` is the constructor), so `is_written_through` stays false and `drive_persistence` raises FatalCommit"
        );
        assert!(
            persistence.start(),
            "the writer refuses to start again after a recoverable errno"
        );
    }

    #[compio::test]
    async fn completion_is_generation_scoped_and_buffered_work_does_not_ack_durability() {
        let directory = tempdir().unwrap();
        let wal_directory = directory.path().join("prepares-7");
        let (persistence, _) = PartitionPersistence::open(&wal_directory, 42, 7)
            .await
            .unwrap();
        let notifications = Rc::new(RefCell::new(Vec::new()));
        let captured = Rc::clone(&notifications);
        persistence.set_notifier(Rc::new(move |completion| {
            captured.borrow_mut().push(completion);
        }));
        let first = prepare(1, 0);
        persistence
            .append(first.clone().into_frozen(), false)
            .unwrap();
        assert!(!persistence.is_durable(first.header()));
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        assert!(notifications.borrow().is_empty());
        let next = prepare(2, first.header().checksum);
        persistence
            .append(next.clone().into_frozen(), true)
            .unwrap();
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        assert!(persistence.is_durable(first.header()));
        assert!(persistence.is_durable(next.header()));
        let completion = notifications.borrow()[0];
        assert!(persistence.accepts_completion(completion));
        persistence.truncate_from(2);
        assert!(!persistence.accepts_completion(completion));
        assert!(!persistence.is_durable(next.header()));
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        persistence.drain().await.unwrap();
        assert!(persistence.is_durable(first.header()));
    }

    #[compio::test]
    async fn truncating_beyond_the_head_does_not_create_an_operation_gap() {
        let directory = tempdir().unwrap();
        let wal_directory = directory.path().join("prepares-7");
        let (persistence, _) = PartitionPersistence::open(&wal_directory, 42, 7)
            .await
            .unwrap();
        persistence.truncate_from(8);
        assert_eq!(persistence.head(), 0);
        let first = prepare(1, 0);
        persistence.append(first.into_frozen(), true).unwrap();
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        assert_eq!(persistence.head(), 1);
        assert!(persistence.failure().is_none());
    }

    #[compio::test]
    async fn checkpoint_tracks_only_dirty_files_and_preserves_committed_barriers_on_truncation() {
        let directory = tempdir().unwrap();
        let wal_directory = directory.path().join("prepares-7");
        let (persistence, _) = PartitionPersistence::open(&wal_directory, 42, 7)
            .await
            .unwrap();
        persistence.mark_segment_dirty(0);
        persistence.mark_segment_dirty(0);
        persistence.mark_offset_dirty(0, 7, true);
        persistence.mark_offset_dirty(0, 7, false);
        persistence.mark_offset_dirty(1, 9, true);
        let (segments, offsets, directories) = persistence.take_dirty_files();
        assert_eq!(segments.into_iter().collect::<Vec<_>>(), vec![0]);
        assert!(offsets[0].is_empty());
        assert!(offsets[1].contains(&9));
        assert_eq!(directories, [true; ConsumerKind::COUNT]);
        let (segments, offsets, directories) = persistence.take_dirty_files();
        assert!(segments.is_empty());
        assert!(offsets.iter().all(BTreeSet::is_empty));
        assert_eq!(directories, [false; ConsumerKind::COUNT]);
        let first = prepare(1, 0);
        let checkpoint_header = *first.header();
        let second = prepare(2, first.header().checksum);
        persistence.append(first.into_frozen(), true).unwrap();
        persistence.append(second.into_frozen(), true).unwrap();
        persistence.checkpoint(1);
        persistence.truncate_from(2);
        assert!(persistence.checkpoint_pending());
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        assert!(persistence.failure().is_none());
        assert_eq!(persistence.checkpoint_op(), 1);
        assert!(persistence.is_durable(&checkpoint_header));
        assert!(!persistence.checkpoint_pending());
    }

    #[compio::test]
    async fn dropping_an_unpolled_writer_releases_drain_waiters() {
        let directory = tempdir().unwrap();
        let wal_directory = directory.path().join("prepares-7");
        let (persistence, _) = PartitionPersistence::open(&wal_directory, 42, 7)
            .await
            .unwrap();
        persistence
            .append(prepare(1, 0).into_frozen(), true)
            .unwrap();
        assert!(persistence.start());
        drop(Rc::clone(&persistence).run());
        assert!(!persistence.running.get());
        assert!(persistence.journal.borrow().is_some());
        assert_eq!(
            persistence.drain().await.unwrap_err().kind(),
            io::ErrorKind::Interrupted
        );
    }

    #[compio::test]
    async fn replacement_waits_for_the_retired_writer_and_rejects_a_live_owner() {
        let directory = tempdir().unwrap();
        let wal_directory = directory.path().join("prepares-7");
        let next_incarnation = directory.path().join("prepares-8");
        let (old, _) = PartitionPersistence::open(&wal_directory, 42, 7)
            .await
            .unwrap();
        assert!(
            PartitionPersistence::open(&wal_directory, 42, 7)
                .await
                .is_err()
        );
        assert!(
            PartitionPersistence::open(&next_incarnation, 42, 8)
                .await
                .is_err()
        );
        old.append(prepare(1, 0).into_frozen(), true).unwrap();
        assert!(old.start());
        old.retire();
        let mut replacement = Box::pin(PartitionPersistence::open(&next_incarnation, 42, 8));
        assert!(futures::poll!(&mut replacement).is_pending());
        Rc::clone(&old).run().await;
        let (replacement, _) = replacement.await.unwrap();
        assert_eq!(replacement.head(), 0);
        assert!(replacement.failure().is_none());
    }

    #[compio::test]
    async fn replacement_waiting_on_a_cancelled_writer_preserves_the_restart_fence() {
        let directory = tempdir().unwrap();
        let wal_directory = directory.path().join("prepares-7");
        let next_incarnation = directory.path().join("prepares-8");
        let (old, _) = PartitionPersistence::open(&wal_directory, 42, 7)
            .await
            .unwrap();
        old.append(prepare(1, 0).into_frozen(), true).unwrap();
        assert!(old.start());
        let mut writer = Box::pin(Rc::clone(&old).run());
        assert!(futures::poll!(&mut writer).is_pending());
        old.retire();
        let mut replacement = Box::pin(PartitionPersistence::open(&next_incarnation, 42, 8));
        assert!(futures::poll!(&mut replacement).is_pending());

        drop(writer);

        assert_eq!(old.failure().unwrap().kind(), io::ErrorKind::Interrupted);
        assert!(
            replacement.await.is_err(),
            "cancellation must fence an already-waiting replacement"
        );
        assert!(
            PartitionPersistence::open(&next_incarnation, 42, 8)
                .await
                .is_err()
        );
    }

    #[compio::test]
    async fn dirty_file_count_requests_a_checkpoint_below_the_byte_threshold() {
        let directory = tempdir().unwrap();
        let wal_directory = directory.path().join("prepares-7");
        let (persistence, _) = PartitionPersistence::open(&wal_directory, 42, 7)
            .await
            .unwrap();
        for consumer_id in 0..u32::try_from(CHECKPOINT_DIRTY_FILES_MAX).unwrap() {
            persistence.mark_offset_dirty(0, consumer_id, true);
        }
        assert!(persistence.needs_checkpoint());
        assert_eq!(persistence.disk_bytes.get(), 0);
    }

    #[compio::test]
    async fn run_ahead_stops_at_the_backlog_bound_and_while_a_checkpoint_is_due() {
        let directory = tempdir().unwrap();
        let wal_directory = directory.path().join("prepares-7");
        let (persistence, _) = PartitionPersistence::open(&wal_directory, 42, 7)
            .await
            .unwrap();
        let record_bytes = u64::try_from(
            journal::partition_journal::record_length(size_of::<PrepareHeader>()).unwrap(),
        )
        .unwrap();
        let mut head = 0;
        let mut parent = 0;
        while persistence.can_run_ahead() {
            head += 1;
            let next = prepare(head, parent);
            parent = next.header().checksum;
            persistence.append(next.into_frozen(), false).unwrap();
        }
        assert_eq!(head, RUN_AHEAD_BYTES_MAX.div_ceil(record_bytes));

        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        assert!(persistence.can_run_ahead());

        persistence.request_checkpoint();
        assert!(
            !persistence.can_run_ahead(),
            "a due checkpoint must wait for the writer to reach commit_min"
        );

        persistence.checkpoint(head);
        let bound_ops = head;
        for _ in 0..bound_ops {
            head += 1;
            let next = prepare(head, parent);
            parent = next.header().checksum;
            persistence.append(next.into_frozen(), false).unwrap();
        }
        assert!(
            persistence.can_run_ahead(),
            "a pending checkpoint must leave the backlog to admission"
        );
    }

    /// The barrier is what groups prepares, so a barrier cheaper than the
    /// interval between arrivals leaves every prepare paying its own writes.
    /// The delay restores the grouping without changing what the barrier
    /// covers, and it must not fire on a partition whose arrivals are spaced
    /// wider than the wait.
    #[compio::test]
    async fn a_group_commit_delay_admits_prepares_that_arrive_during_the_wait() {
        const DELAY: Duration = Duration::from_millis(200);
        let directory = tempdir().unwrap();
        let (persistence, _) =
            PartitionPersistence::open(&directory.path().join("prepares-7"), 42, 7)
                .await
                .unwrap();
        persistence.set_group_commit_delay(DELAY);
        let first = prepare(1, 0);
        let second = prepare(2, first.header().checksum);
        let third = prepare(3, second.header().checksum);
        // Two back-to-back submissions put the arrival estimate under the delay.
        persistence.append(first.into_frozen(), true).unwrap();
        persistence.append(second.into_frozen(), true).unwrap();
        assert!(persistence.start());
        let writer = Rc::clone(&persistence);
        let late = async {
            compio::runtime::time::sleep(DELAY / 4).await;
            // Collected bytes stay charged against the budget while the writer
            // waits; they left the queue but have not reached the barrier.
            let waiting = persistence.take_metrics();
            assert_eq!(waiting.queued_bytes, 0);
            assert_eq!(waiting.in_flight_bytes, 2 * 4096);
            persistence.append(third.into_frozen(), true).unwrap();
        };
        futures::future::join(writer.run(), late).await;
        assert!(persistence.failure().is_none());
        assert!(persistence.is_durable_through(3));
        let metrics = persistence.take_metrics();
        assert_eq!(metrics.completed_batches, 1);
        assert_eq!(metrics.batched_prepares, 3);
        assert_eq!(metrics.in_flight_bytes, 0);
    }

    /// A group with no barrier to amortize gains nothing from waiting, and a
    /// wait there would only delay the durable group queued behind it.
    #[compio::test]
    async fn a_group_commit_delay_is_skipped_for_a_group_without_a_barrier() {
        let directory = tempdir().unwrap();
        let (persistence, _) =
            PartitionPersistence::open(&directory.path().join("prepares-7"), 42, 7)
                .await
                .unwrap();
        // Wide apart on purpose: a taken wait is at least the delay, a slow
        // append on a loaded runner is milliseconds, so the bound cannot be
        // crossed by either for the wrong reason.
        persistence.set_group_commit_delay(Duration::from_secs(2));
        let first = prepare(1, 0);
        let second = prepare(2, first.header().checksum);
        persistence.append(first.into_frozen(), false).unwrap();
        persistence.append(second.into_frozen(), false).unwrap();
        assert!(persistence.start());
        let started = Instant::now();
        Rc::clone(&persistence).run().await;
        assert!(started.elapsed() < Duration::from_secs(1));
        assert!(persistence.is_written_through(2));
        assert_eq!(persistence.take_metrics().completed_batches, 0);
    }

    /// A partition whose prepares arrive further apart than the delay would pay
    /// the wait for nothing, so the estimate has to keep it off.
    #[compio::test]
    async fn a_group_commit_delay_is_skipped_when_arrivals_outlast_it() {
        let directory = tempdir().unwrap();
        let (persistence, _) =
            PartitionPersistence::open(&directory.path().join("prepares-7"), 42, 7)
                .await
                .unwrap();
        persistence.set_group_commit_delay(Duration::from_millis(1));
        let first = prepare(1, 0);
        let second = prepare(2, first.header().checksum);
        persistence.append(first.into_frozen(), true).unwrap();
        compio::runtime::time::sleep(Duration::from_millis(20)).await;
        persistence.append(second.into_frozen(), true).unwrap();
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        assert!(persistence.is_durable_through(2));
        // Both were queued before the writer started, so they share one barrier
        // regardless. What matters is that no wait was taken to get there.
        assert_eq!(persistence.take_metrics().completed_batches, 1);
    }

    fn command(op: u64, parent: u128) -> Message<PrepareHeader> {
        prepare(op, parent).transmute_header(|mut header, next: &mut PrepareHeader| {
            header.operation = Operation::StateMachineCommand;
            header.checksum = header.identity_checksum();
            *next = header;
        })
    }

    fn ops(prepares: &[Message<PrepareHeader>]) -> Vec<u64> {
        prepares.iter().map(|prepare| prepare.header().op).collect()
    }

    async fn write(
        persistence: &Rc<PartitionPersistence>,
        prepares: &[(&Message<PrepareHeader>, bool)],
    ) {
        for (prepare, durable) in prepares {
            persistence
                .append((*prepare).clone().into_frozen(), *durable)
                .unwrap();
        }
        assert!(persistence.start());
        Rc::clone(persistence).run().await;
    }

    #[compio::test]
    async fn given_commands_between_data_ops_when_reopened_should_merge_both_logs() {
        let directory = tempdir().unwrap();
        let wal = directory.path().join("prepares-7");
        let first = prepare(1, 0);
        let second = command(2, first.header().checksum);
        let third = prepare(3, second.header().checksum);
        let (persistence, _) = PartitionPersistence::open(&wal, 42, 7).await.unwrap();
        assert!(!persistence.holds_commands());
        write(
            &persistence,
            &[(&first, true), (&second, true), (&third, true)],
        )
        .await;
        assert!(persistence.holds_commands());
        assert!(persistence.is_durable_through(3));
        assert!(wal.join(STATE_MACHINE_LOG_DIRECTORY).is_dir());
        assert!(!persistence.certify_log_view(1, 3, third.header().checksum));
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        assert_eq!(persistence.certified_log_view(), Some(1));
        drop(persistence);

        let (persistence, prepares) = PartitionPersistence::open(&wal, 42, 7).await.unwrap();
        assert!(persistence.holds_commands());
        assert_eq!(ops(&prepares), vec![1, 2, 3]);
        assert_eq!(
            prepares[1].header().operation,
            Operation::StateMachineCommand
        );
        assert_eq!(persistence.head(), 3);
        assert_eq!(persistence.checksum(2), Some(second.header().checksum));
        assert_eq!(persistence.certified_log_view(), Some(1));
    }

    #[compio::test]
    async fn given_a_submission_that_skips_the_command_when_appended_should_refuse_it() {
        let directory = tempdir().unwrap();
        let (persistence, _) =
            PartitionPersistence::open(&directory.path().join("prepares-7"), 42, 7)
                .await
                .unwrap();
        let first = prepare(1, 0);
        let second = command(2, first.header().checksum);
        persistence.append(first.into_frozen(), true).unwrap();
        persistence.append(second.into_frozen(), true).unwrap();
        let unlinked = prepare(3, 0xbad);
        assert_eq!(
            persistence
                .append(unlinked.into_frozen(), true)
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[compio::test]
    async fn given_a_checkpoint_at_a_command_when_reopened_should_replay_from_it() {
        let directory = tempdir().unwrap();
        let wal = directory.path().join("prepares-7");
        let first = prepare(1, 0);
        let second = command(2, first.header().checksum);
        let third = prepare(3, second.header().checksum);
        let fourth = command(4, third.header().checksum);
        let (persistence, _) = PartitionPersistence::open(&wal, 42, 7).await.unwrap();
        write(
            &persistence,
            &[
                (&first, true),
                (&second, true),
                (&third, true),
                (&fourth, true),
            ],
        )
        .await;
        persistence.checkpoint(2);
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        assert_eq!(persistence.checkpoint_op(), 2);
        drop(persistence);

        let (persistence, prepares) = PartitionPersistence::open(&wal, 42, 7).await.unwrap();
        assert_eq!(persistence.checkpoint_op(), 2);
        assert_eq!(persistence.checksum(2), Some(second.header().checksum));
        // The command at the checkpoint is its checkpoint prepare.
        assert_eq!(ops(&prepares), vec![2, 3, 4]);
        assert_eq!(persistence.head(), 4);
    }

    /// A replicated data op is written without a barrier, and a command's
    /// barrier must cover it: the command is durable only once every op below
    /// it is.
    #[compio::test]
    async fn given_unsynced_data_ops_when_commands_are_written_should_make_every_op_durable() {
        let directory = tempdir().unwrap();
        let wal = directory.path().join("prepares-7");
        let first = prepare(1, 0);
        let second = command(2, first.header().checksum);
        let third = prepare(3, second.header().checksum);
        let fourth = command(4, third.header().checksum);
        let fifth = command(5, fourth.header().checksum);
        let (persistence, _) = PartitionPersistence::open(&wal, 42, 7).await.unwrap();
        write(&persistence, &[(&first, false), (&second, true)]).await;
        assert_eq!(persistence.durable_op(), 2);
        write(
            &persistence,
            &[(&third, false), (&fourth, true), (&fifth, true)],
        )
        .await;
        assert_eq!(persistence.durable_op(), 5);
    }

    /// The first command creates the state-machine log mid-view, and the pair
    /// stays certified at the data log's view.
    #[compio::test]
    async fn given_a_certified_data_log_when_the_first_command_arrives_should_keep_the_certificate()
    {
        const VIEW: u32 = 3;
        let directory = tempdir().unwrap();
        let wal = directory.path().join("prepares-7");
        let first = in_view(prepare(1, 0), VIEW);
        let second = in_view(command(2, first.header().checksum), VIEW);
        let (persistence, _) = PartitionPersistence::open(&wal, 42, 7).await.unwrap();
        assert!(!persistence.certify_log_view(VIEW, 0, 0));
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        assert_eq!(persistence.certified_log_view(), Some(VIEW));

        write(&persistence, &[(&first, false), (&second, true)]).await;
        assert!(persistence.holds_commands());
        assert_eq!(persistence.certified_log_view(), Some(VIEW));
        assert!(persistence.is_durable_through(2));
        drop(persistence);
        let (persistence, prepares) = PartitionPersistence::open(&wal, 42, 7).await.unwrap();
        assert_eq!(ops(&prepares), vec![1, 2]);
        assert_eq!(persistence.certified_log_view(), Some(VIEW));
    }

    /// A crash between the two barriers of a command can leave the command
    /// durable above a data op the data log lost. Nothing from the lost op on
    /// was acknowledged, so both logs drop it, and what stays is still the
    /// certified view's history.
    #[compio::test]
    async fn given_a_data_op_lost_below_a_durable_command_when_reopened_should_drop_the_tail_and_keep_the_certificate()
     {
        const VIEW: u32 = 1;
        let directory = tempdir().unwrap();
        let wal = directory.path().join("prepares-7");
        let first = prepare(1, 0);
        let second = command(2, first.header().checksum);
        let third = prepare(3, second.header().checksum);
        let lost = prepare(4, third.header().checksum);
        let durable = command(5, lost.header().checksum);
        let (persistence, _) = PartitionPersistence::open(&wal, 42, 7).await.unwrap();
        write(
            &persistence,
            &[(&first, true), (&second, true), (&third, true)],
        )
        .await;
        persistence.checkpoint(2);
        assert!(!persistence.certify_log_view(VIEW, 3, third.header().checksum));
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        assert_eq!(persistence.checkpoint_op(), 2);
        assert_eq!(persistence.certified_log_view(), Some(VIEW));
        drop(persistence);

        let (mut data, mut commands) = open_raw_logs(&wal).await;
        data.append_buffered(lost.into_frozen()).await.unwrap();
        commands.append(durable.into_frozen()).await.unwrap();
        drop((data, commands));

        for _ in 0..2 {
            let (persistence, prepares) = PartitionPersistence::open(&wal, 42, 7).await.unwrap();
            assert_eq!(ops(&prepares), vec![2, 3]);
            assert_eq!(persistence.head(), 3);
            assert_eq!(persistence.certified_log_view(), Some(VIEW));
        }
    }

    /// The data log publishes its checkpoint first: a crash before the
    /// state-machine log follows leaves commands below the data checkpoint,
    /// which recovery drops, and never a hole above it.
    #[compio::test]
    async fn given_a_crash_between_the_two_checkpoints_when_reopened_should_keep_every_op() {
        let directory = tempdir().unwrap();
        let wal = directory.path().join("prepares-7");
        let first = prepare(1, 0);
        let second = command(2, first.header().checksum);
        let third = prepare(3, second.header().checksum);
        let fourth = command(4, third.header().checksum);
        let fifth = prepare(5, fourth.header().checksum);
        let (persistence, _) = PartitionPersistence::open(&wal, 42, 7).await.unwrap();
        write(
            &persistence,
            &[
                (&first, true),
                (&second, true),
                (&third, true),
                (&fourth, true),
                (&fifth, true),
            ],
        )
        .await;
        persistence.stop_between_checkpoints.set(true);
        persistence.checkpoint(4);
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        assert!(persistence.failure().is_some());
        drop(persistence);

        let (persistence, prepares) = PartitionPersistence::open(&wal, 42, 7).await.unwrap();
        assert_eq!(persistence.checkpoint_op(), 4);
        assert_eq!(ops(&prepares), vec![4, 5]);
        assert_eq!(persistence.head(), 5);
        assert_eq!(persistence.checksum(5), Some(fifth.header().checksum));
    }

    #[compio::test]
    async fn given_a_command_at_the_checkpoint_that_is_not_the_checkpointed_op_when_reopened_should_refuse()
     {
        let directory = tempdir().unwrap();
        let wal = directory.path().join("prepares-7");
        let first = prepare(1, 0);
        let (persistence, _) = PartitionPersistence::open(&wal, 42, 7).await.unwrap();
        write(&persistence, &[(&first, true)]).await;
        drop(persistence);
        let (mut data, mut commands) = open_raw_logs(&wal).await;
        data.checkpoint_at(2, 0xbad).await.unwrap();
        commands
            .append(command(2, first.header().checksum).into_frozen())
            .await
            .unwrap();
        drop((data, commands));

        let error = PartitionPersistence::open(&wal, 42, 7)
            .await
            .map(|_| ())
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    /// A checkpoint that would reclaim nothing from the state-machine log
    /// leaves it alone, and recovery still merges both logs.
    #[compio::test]
    async fn given_no_command_below_a_checkpoint_when_checkpointing_should_leave_the_command_log() {
        let directory = tempdir().unwrap();
        let wal = directory.path().join("prepares-7");
        let first = command(1, 0);
        let second = prepare(2, first.header().checksum);
        let third = prepare(3, second.header().checksum);
        let (persistence, _) = PartitionPersistence::open(&wal, 42, 7).await.unwrap();
        write(
            &persistence,
            &[(&first, true), (&second, true), (&third, true)],
        )
        .await;
        persistence.checkpoint(2);
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        persistence.checkpoint(3);
        assert!(persistence.start());
        Rc::clone(&persistence).run().await;
        assert_eq!(persistence.checkpoint_op(), 3);
        drop(persistence);

        let (data, commands) = open_raw_logs(&wal).await;
        assert_eq!(data.checkpoint_op(), 3);
        assert_eq!(commands.checkpoint_op(), 2, "nothing left to reclaim at 3");
        drop((data, commands));
        let (persistence, prepares) = PartitionPersistence::open(&wal, 42, 7).await.unwrap();
        assert_eq!(ops(&prepares), vec![3]);
        assert_eq!(persistence.head(), 3);
    }

    /// Both logs of the partition WAL at `wal`, opened beside a partition that
    /// is gone, to shape them the way a crash would leave them.
    async fn open_raw_logs(
        wal: &Path,
    ) -> (
        PartitionPrepareJournal<crate::WalStorage>,
        PartitionPrepareJournal<crate::WalStorage>,
    ) {
        (
            open_raw_log(wal).await,
            open_raw_log(&wal.join(STATE_MACHINE_LOG_DIRECTORY)).await,
        )
    }

    async fn open_raw_log(directory: &Path) -> PartitionPrepareJournal<crate::WalStorage> {
        PartitionPrepareJournal::open_with_options(
            directory,
            42,
            7,
            crate::WalStorage::default(),
            WalOpenOptions {
                sparse: true,
                ..WalOpenOptions::default()
            },
        )
        .await
        .unwrap()
    }

    fn in_view(prepare: Message<PrepareHeader>, view: u32) -> Message<PrepareHeader> {
        prepare.transmute_header(|mut header, next: &mut PrepareHeader| {
            header.view = view;
            header.checksum = header.identity_checksum();
            *next = header;
        })
    }

    fn prepare(op: u64, parent: u128) -> Message<PrepareHeader> {
        let mut owned = Owned::<4096>::zeroed(size_of::<PrepareHeader>());
        let header = bytemuck::checked::from_bytes_mut::<PrepareHeader>(owned.as_mut_slice());
        header.command = Command::Prepare;
        header.operation = Operation::StoreConsumerOffset;
        header.group = 42;
        header.op = op;
        header.parent = parent;
        header.size = u32::try_from(size_of::<PrepareHeader>()).unwrap();
        header.checksum = header.identity_checksum();
        Message::try_from(owned).unwrap()
    }
}
