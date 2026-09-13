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

use futures::TryStreamExt;
use iggy_binary_protocol::{Operation, PrepareHeader};
use journal::PartitionPrepareJournal;
use journal::durable_storage::{DiskStorage, DurableFile, DurableStorage};
use journal::partition_journal::{PARTITION_WAL_BYTES_MAX, SegmentPosition, SegmentReference};
use server_common::Message;
use server_common::iobuf::Frozen;
use smallvec::SmallVec;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::io;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex, Weak};
use std::time::{Duration, Instant};

#[cfg(unix)]
use nix::sys::resource::{Resource, getrlimit};

// Group commit bounds, not throughput bounds. Every prepare in a group is
// already queued and waiting, so widening the group moves work off the barrier
// and onto a buffered memcpy: one body write and one durability barrier serve
// the whole group instead of each prepare paying its own. The byte budget is
// charged against the padded BODY size even when the WAL stores a segment
// reference and writes 4096 bytes per record, so a tight budget caps grouping
// far below what the write itself costs.
const APPEND_BATCH_BYTES_MAX: u64 = 8 * 1024 * 1024;
const APPEND_BATCH_OPS_MAX: usize = 256;
const CHECKPOINT_DIRTY_FILES_MAX: usize = 1024;
#[cfg(unix)]
const OFFSET_FILES_TOTAL_MAX: usize = 1024;
const OFFSET_FILES_PER_PARTITION_MAX: usize = 64;
#[cfg(unix)]
const OFFSET_FILE_LIMIT_DIVISOR: u64 = 4;
const PERSISTENCE_DRAIN_TIMEOUT: Duration = Duration::from_secs(30);

static NEXT_INSTANCE: AtomicU64 = AtomicU64::new(1);

static RETAINED_OFFSET_FILES: AtomicUsize = AtomicUsize::new(0);
static OFFSET_FILE_LIMIT: LazyLock<usize> = LazyLock::new(|| {
    // Leave descriptor space for sockets, journals, indexes, and active I/O.
    #[cfg(unix)]
    let limit = getrlimit(Resource::RLIMIT_NOFILE).map_or(0, |(soft, _)| {
        usize::try_from(soft / OFFSET_FILE_LIMIT_DIVISOR)
            .unwrap_or(OFFSET_FILES_TOTAL_MAX)
            .min(OFFSET_FILES_TOTAL_MAX)
    });
    #[cfg(not(unix))]
    let limit = 0;
    limit
});

struct RetainedOffsetFile<F> {
    file: F,
    _permit: OffsetFilePermit,
}

struct OffsetFilePermit;

impl OffsetFilePermit {
    fn acquire() -> Option<Self> {
        RETAINED_OFFSET_FILES
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |count| {
                (count < *OFFSET_FILE_LIMIT).then_some(count + 1)
            })
            .ok()
            .map(|_| Self)
    }
}

impl Drop for OffsetFilePermit {
    fn drop(&mut self) {
        RETAINED_OFFSET_FILES.fetch_sub(1, Ordering::Relaxed);
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
    pub completed_checkpoints: u64,
    pub failed_writes: u64,
}

pub type PersistenceNotifier = Rc<dyn Fn(PersistenceCompletion)>;

pub struct PartitionPersistence<S: DurableStorage = DiskStorage> {
    group: u64,
    instance: u64,
    lease: Option<Arc<WriterLease>>,
    epoch: Cell<u64>,
    journal: RefCell<Option<PartitionPrepareJournal<S>>>,
    queue: RefCell<VecDeque<Mutation<S>>>,
    offset_files: RefCell<HashMap<String, RetainedOffsetFile<S::File>>>,
    retired_offset_files: RefCell<Vec<RetainedOffsetFile<S::File>>>,
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
    dirty_offsets: [RefCell<BTreeSet<u32>>; 2],
    purge_generation: Cell<u64>,
    purge_floor: Cell<u64>,
    capacity: u64,
    disk_bytes: Cell<u64>,
    retained_bytes: Cell<u64>,
    segment_checkpoint: Cell<Option<SegmentPosition>>,
    queued_bytes: Cell<u64>,
    in_flight_bytes: Cell<u64>,
    waiters: RefCell<Vec<std::task::Waker>>,
    running: Cell<bool>,
    writer_active: Cell<bool>,
    retired: Cell<bool>,
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
    completed_checkpoints: Cell<u64>,
    failed_writes: Cell<u64>,
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
    async fn acquire(key: PathBuf) -> io::Result<Arc<Self>> {
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
            compio::runtime::time::timeout(
                PERSISTENCE_DRAIN_TIMEOUT,
                futures::future::poll_fn(|context| {
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
                }),
            )
            .await
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::TimedOut,
                    "retired partition WAL writer did not stop",
                )
            })?;
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
    journal: Option<PartitionPrepareJournal<S>>,
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
    },
    CertifyView {
        epoch: u64,
        view: u32,
        op: u64,
        checksum: u128,
    },
    Purge {
        epoch: u64,
        generation: u64,
        floor: u64,
    },
    Append {
        epoch: u64,
        prepare: Frozen<4096>,
        durable: bool,
        bytes: u64,
    },
    Truncate {
        epoch: u64,
        from_op: u64,
    },
    Checkpoint {
        epoch: u64,
        through_op: u64,
        files: Vec<PathBuf>,
        directories: Vec<PathBuf>,
        offset_files: Vec<RetainedOffsetFile<S::File>>,
        synced_files: BTreeSet<PathBuf>,
    },
    Reset {
        epoch: u64,
        op: u64,
        checksum: Option<u128>,
        prepare: Option<Frozen<4096>>,
        segments: Option<(SegmentPosition, u64)>,
    },
}

impl PartitionPersistence {
    /// # Errors
    /// Returns an error if the partition WAL cannot be recovered.
    pub async fn open(
        directory: &Path,
        group: u64,
        incarnation: u64,
    ) -> io::Result<(Rc<Self>, Vec<Message<PrepareHeader>>)> {
        Self::open_with_storage(directory, group, incarnation, DiskStorage).await
    }
}

impl<S: DurableStorage> PartitionPersistence<S> {
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
            Some(WriterLease::acquire(key).await?)
        } else {
            None
        };
        let mut journal = PartitionPrepareJournal::open_with_storage_and_capacity(
            directory,
            group,
            incarnation,
            storage,
            capacity,
            preallocate_segments,
        )
        .await?;
        let prepares = journal.take_recovered_prepares();
        let mut accepted = AcceptedPrepares {
            base: journal.checkpoint_op(),
            checksums: VecDeque::with_capacity(prepares.len()),
        };
        for prepare in &prepares {
            let header = prepare.header();
            if header.op > journal.checkpoint_op() {
                accepted.checksums.push_back(header.checksum);
            }
        }
        let persistence = Rc::new(Self {
            group,
            instance: NEXT_INSTANCE.fetch_add(1, Ordering::Relaxed),
            lease,
            epoch: Cell::new(0),
            accepted_head: Cell::new(journal.head()),
            written_head: Cell::new(journal.head()),
            durable_head: Cell::new(journal.head()),
            checkpoint: Cell::new(journal.checkpoint_op()),
            checkpoint_checksum: Cell::new(journal.checkpoint_checksum()),
            certified_log_view: Cell::new(journal.certified_log_view()),
            requested_log_view: Cell::new(None),
            checkpoint_requested: Cell::new(journal.checkpoint_op()),
            checkpoint_running: Cell::new(false),
            checkpoint_needed: Cell::new(false),
            dirty_segments: RefCell::new(BTreeSet::new()),
            dirty_offsets: std::array::from_fn(|_| RefCell::new(BTreeSet::new())),
            purge_generation: Cell::new(journal.purge_marker().0),
            purge_floor: Cell::new(journal.purge_marker().1),
            capacity,
            disk_bytes: Cell::new(journal.size_bytes()),
            retained_bytes: Cell::new(journal.retained_bytes()),
            segment_checkpoint: Cell::new(journal.segment_checkpoint()),
            segment_references: RefCell::new(journal.written_segment_references(0).collect()),
            journal: RefCell::new(Some(journal)),
            queue: RefCell::new(VecDeque::new()),
            offset_files: RefCell::new(HashMap::new()),
            retired_offset_files: RefCell::new(Vec::new()),
            accepted: RefCell::new(accepted),
            queued_bytes: Cell::new(0),
            in_flight_bytes: Cell::new(0),
            waiters: RefCell::new(Vec::new()),
            running: Cell::new(false),
            writer_active: Cell::new(false),
            retired: Cell::new(false),
            failure: RefCell::new(None),
            failure_operation: Cell::new(Operation::SendMessages),
            notifier: RefCell::new(None),
            group_commit_delay: Cell::new(Duration::ZERO),
            append_gap: Cell::new(Duration::MAX),
            last_append: Cell::new(None),
            completed_batches: Cell::new(0),
            batched_prepares: Cell::new(0),
            completed_checkpoints: Cell::new(0),
            failed_writes: Cell::new(0),
        });
        Ok((persistence, prepares))
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

    pub const fn segment_checkpoint(&self) -> Option<SegmentPosition> {
        self.segment_checkpoint.get()
    }

    pub fn enable_segment_storage(&self, initial: SegmentPosition, max_size: u64) {
        self.queue.borrow_mut().push_back(Mutation::EnableSegments {
            epoch: self.epoch.get(),
            initial,
            max_size,
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

    pub const fn durable_op(&self) -> u64 {
        self.durable_head.get()
    }

    pub fn is_durable_through(&self, op: u64) -> bool {
        self.is_written_through(op) && op <= self.durable_head.get()
    }

    pub fn has_capacity(&self, frame_bytes: usize) -> bool {
        let Ok(bytes) = journal::partition_journal::record_length(frame_bytes) else {
            return false;
        };
        let bytes = bytes as u64;
        !self.retired.get()
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
        let bytes = journal::partition_journal::record_length(prepare.len())? as u64;
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
        let now = Instant::now();
        self.append_gap.set(
            self.last_append
                .replace(Some(now))
                .map_or(Duration::MAX, |previous| {
                    now.saturating_duration_since(previous)
                }),
        );
        self.queued_bytes.set(self.queued_bytes.get() + bytes);
        self.accepted
            .borrow_mut()
            .checksums
            .push_back(header.checksum);
        self.accepted_head.set(header.op);
        self.queue.borrow_mut().push_back(Mutation::Append {
            epoch: self.epoch.get(),
            prepare,
            durable,
            bytes,
        });
        Ok(())
    }

    pub fn mark_segment_dirty(&self, start_offset: u64) {
        self.dirty_segments.borrow_mut().insert(start_offset);
    }

    pub fn take_offset_file(&self, path: &str) -> Option<S::File> {
        self.offset_files
            .borrow_mut()
            .remove(path)
            .map(|retained| retained.file)
    }

    /// Retain the original writer or synchronize it before closing at the budget.
    ///
    /// # Errors
    /// Returns a barrier error that the caller must fence like a failed write.
    pub async fn retain_offset_file(&self, path: String, file: S::File) -> io::Result<()> {
        let retained_count =
            self.offset_files.borrow().len() + self.retired_offset_files.borrow().len();
        if retained_count >= OFFSET_FILES_PER_PARTITION_MAX {
            return file.sync().await;
        }
        let Some(permit) = OffsetFilePermit::acquire() else {
            return file.sync().await;
        };
        if let Some(previous) = self.offset_files.borrow_mut().insert(
            path,
            RetainedOffsetFile {
                file,
                _permit: permit,
            },
        ) {
            self.retired_offset_files.borrow_mut().push(previous);
        }
        Ok(())
    }

    pub fn retire_offset_file(&self, path: &str) {
        if let Some(file) = self.offset_files.borrow_mut().remove(path) {
            self.retired_offset_files.borrow_mut().push(file);
        }
    }

    pub fn mark_offset_dirty(&self, kind_index: usize, consumer_id: u32, exists: bool) {
        let mut offsets = self.dirty_offsets[kind_index].borrow_mut();
        if exists {
            offsets.insert(consumer_id);
        } else {
            offsets.remove(&consumer_id);
        }
    }

    pub fn retire_offset_files(&self) {
        self.retired_offset_files
            .borrow_mut()
            .extend(self.offset_files.borrow_mut().drain().map(|(_, file)| file));
    }

    pub fn take_dirty_files(&self) -> (BTreeSet<u64>, [BTreeSet<u32>; 2]) {
        (
            std::mem::take(&mut *self.dirty_segments.borrow_mut()),
            std::array::from_fn(|index| {
                std::mem::take(&mut *self.dirty_offsets[index].borrow_mut())
            }),
        )
    }

    pub fn checkpoint(&self, through_op: u64) {
        self.checkpoint_files(through_op, Vec::new(), Vec::new());
    }

    pub fn checkpoint_files(
        &self,
        through_op: u64,
        files: Vec<PathBuf>,
        directories: Vec<PathBuf>,
    ) {
        if through_op <= self.checkpoint_requested.get() {
            return;
        }
        self.checkpoint_requested.set(through_op);
        self.checkpoint_needed.set(false);
        let synced_files = self
            .offset_files
            .borrow()
            .keys()
            .map(PathBuf::from)
            .collect();
        let mut offset_files = std::mem::take(&mut *self.retired_offset_files.borrow_mut());
        offset_files.extend(self.offset_files.borrow_mut().drain().map(|(_, file)| file));
        self.queue.borrow_mut().push_back(Mutation::Checkpoint {
            epoch: self.epoch.get(),
            through_op,
            files,
            directories,
            offset_files,
            synced_files,
        });
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
                    prepare,
                    bytes,
                    ..
                } => {
                    if prepare_header(prepare).is_ok_and(|header| header.op < from_op) {
                        *previous = epoch;
                        true
                    } else {
                        self.queued_bytes
                            .set(self.queued_bytes.get().saturating_sub(*bytes));
                        false
                    }
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
        segments: Option<(SegmentPosition, u64)>,
    ) {
        self.offset_files.borrow_mut().clear();
        self.retired_offset_files.borrow_mut().clear();
        self.certified_log_view.set(None);
        self.requested_log_view.set(None);
        self.checkpoint_requested.set(op);
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

    pub fn mark_purge(&self, generation: u64, floor: u64) {
        self.queue.borrow_mut().push_back(Mutation::Purge {
            epoch: self.epoch.get(),
            generation,
            floor,
        });
    }

    pub const fn purge_marker(&self) -> (u64, u64) {
        (self.purge_generation.get(), self.purge_floor.get())
    }

    pub fn needs_checkpoint(&self) -> bool {
        !self.checkpoint_pending()
            && (self.checkpoint_needed.get()
                || self.retained_bytes.get() + self.queued_bytes.get() + self.in_flight_bytes.get()
                    >= self.capacity / 2
                || self.retired_offset_files.borrow().len() >= CHECKPOINT_DIRTY_FILES_MAX
                || self.dirty_segments.borrow().len() * 2
                    + self
                        .dirty_offsets
                        .iter()
                        .map(|offsets| offsets.borrow().len())
                        .sum::<usize>()
                    >= CHECKPOINT_DIRTY_FILES_MAX)
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

    pub fn retire(&self) {
        self.retired.set(true);
        if let Some(lease) = &self.lease {
            lease.retired.store(true, Ordering::Release);
        }
        self.queue.borrow_mut().clear();
        self.queued_bytes.set(0);
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

    /// # Errors
    /// Returns a storage failure or a timeout without allowing a second writer.
    pub async fn drain_with_timeout(&self) -> io::Result<()> {
        compio::runtime::time::timeout(PERSISTENCE_DRAIN_TIMEOUT, self.drain())
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "partition WAL drain timed out"))?
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
        let Some(journal) = guard.journal.as_mut() else {
            self.fail(io::Error::other("partition WAL writer has no journal"));
            guard.complete = true;
            return;
        };
        loop {
            if self.retired.get() {
                break;
            }
            let Some(mutation) = self.queue.borrow_mut().pop_front() else {
                break;
            };
            let (epoch, bytes) = match &mutation {
                Mutation::Append { epoch, bytes, .. } => (*epoch, *bytes),
                Mutation::CertifyView { epoch, .. }
                | Mutation::EnableSegments { epoch, .. }
                | Mutation::Purge { epoch, .. }
                | Mutation::Truncate { epoch, .. }
                | Mutation::Checkpoint { epoch, .. }
                | Mutation::Reset { epoch, .. } => (*epoch, 0),
            };
            self.queued_bytes
                .set(self.queued_bytes.get().saturating_sub(bytes));
            self.in_flight_bytes.set(bytes);
            let rebuild_references = matches!(
                mutation,
                Mutation::EnableSegments { .. }
                    | Mutation::Purge { .. }
                    | Mutation::Truncate { .. }
                    | Mutation::Checkpoint { .. }
                    | Mutation::Reset { .. }
            );
            let result = self.apply_mutation(journal, mutation, epoch, bytes).await;
            self.in_flight_bytes.set(0);
            if let Err(error) = result {
                self.failed_writes.set(self.failed_writes.get() + 1);
                *self.failure.borrow_mut() = Some(Arc::new(error));
                self.notify();
                break;
            }
            if epoch == self.epoch.get() && !self.retired.get() {
                let mut references = self.segment_references.borrow_mut();
                if rebuild_references {
                    references.clear();
                    references.extend(journal.written_segment_references(0));
                } else if let Some(from_op) = self.written_head.get().checked_add(1) {
                    references.extend(journal.written_segment_references(from_op));
                }
                drop(references);
                self.disk_bytes.set(journal.size_bytes());
                self.retained_bytes.set(journal.retained_bytes());
                self.segment_checkpoint.set(journal.segment_checkpoint());
                let advanced = journal.durable_op() != self.durable_head.get()
                    || journal.checkpoint_op() != self.checkpoint.get()
                    || journal.certified_log_view() != self.certified_log_view.get()
                    || (journal.segment_checkpoint().is_some()
                        && journal.head() != self.written_head.get());
                self.certified_log_view.set(journal.certified_log_view());
                if self
                    .requested_log_view
                    .get()
                    .is_some_and(|(view, _, _)| Some(view) == self.certified_log_view.get())
                {
                    self.requested_log_view.set(None);
                }
                self.written_head.set(journal.head());
                self.durable_head.set(journal.durable_op());
                if journal.checkpoint_op() > self.checkpoint.get() {
                    self.accepted
                        .borrow_mut()
                        .checkpoint(journal.checkpoint_op());
                }
                self.checkpoint.set(journal.checkpoint_op());
                self.checkpoint_checksum.set(journal.checkpoint_checksum());
                self.purge_generation.set(journal.purge_marker().0);
                self.purge_floor.set(journal.purge_marker().1);
                if advanced {
                    self.notify();
                }
            }
        }
        guard.complete = true;
    }

    async fn apply_mutation(
        &self,
        journal: &mut PartitionPrepareJournal<S>,
        mutation: Mutation<S>,
        epoch: u64,
        bytes: u64,
    ) -> io::Result<()> {
        match mutation {
            Mutation::EnableSegments {
                initial, max_size, ..
            } => journal.enable_segment_storage(initial, max_size).await,
            Mutation::CertifyView {
                view, op, checksum, ..
            } => journal.certify_log_view(view, op, checksum).await,
            Mutation::Purge {
                generation, floor, ..
            } => journal.mark_purge(generation, floor).await,
            Mutation::Append {
                prepare, durable, ..
            } => {
                self.append_batch(journal, prepare, durable, epoch, bytes)
                    .await
            }
            Mutation::Truncate { from_op, .. } => journal.truncate_from(from_op).await,
            Mutation::Checkpoint {
                through_op,
                files,
                directories,
                offset_files,
                synced_files,
                ..
            } => {
                self.checkpoint_running.set(true);
                let result = async {
                    futures::stream::iter(offset_files.iter().map(Ok::<_, io::Error>))
                        .try_for_each_concurrent(16, |retained| retained.file.sync())
                        .await?;
                    journal
                        .checkpoint_files(through_op, &files, &directories, &synced_files)
                        .await
                }
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
            } => {
                if let Some((position, max_size)) = segments {
                    journal
                        .reset_with_segment_checkpoint(op, checksum, prepare, position, max_size)
                        .await
                } else if let Some(prepare) = prepare {
                    journal.reset_with_prepare(prepare).await
                } else {
                    journal.reset(op, checksum).await
                }
            }
        }
    }

    async fn append_batch(
        &self,
        journal: &mut PartitionPrepareJournal<S>,
        first: Frozen<4096>,
        durable: bool,
        epoch: u64,
        first_bytes: u64,
    ) -> io::Result<()> {
        let mut batch = SmallVec::<[Frozen<4096>; 8]>::new();
        batch.push(first);
        let mut bytes = first_bytes;
        let mut durable = durable;
        self.collect_queued(&mut batch, &mut bytes, &mut durable, epoch);
        // Charged before the wait: `collect_queued` took these bytes out of the
        // queued total, and admission and checkpoint pacing sum queued and
        // in-flight bytes against the budget, so a gap here would admit a full
        // group past it.
        self.in_flight_bytes.set(bytes);
        // The barrier is what groups prepares, so a barrier cheaper than the
        // interval between arrivals groups nothing and every prepare pays its
        // own writes. This wait puts that grouping back under operator control.
        if durable && let Some(delay) = self.group_commit_wait(&batch, bytes) {
            compio::runtime::time::sleep(delay).await;
            self.collect_queued(&mut batch, &mut bytes, &mut durable, epoch);
            self.in_flight_bytes.set(bytes);
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
        batch: &mut SmallVec<[Frozen<4096>; 8]>,
        bytes: &mut u64,
        durable: &mut bool,
        epoch: u64,
    ) {
        let mut queue = self.queue.borrow_mut();
        while batch.len() < APPEND_BATCH_OPS_MAX {
            let Some(Mutation::Append {
                epoch: next_epoch,
                bytes: next_bytes,
                ..
            }) = queue.front()
            else {
                break;
            };
            if *next_epoch != epoch || bytes.saturating_add(*next_bytes) > APPEND_BATCH_BYTES_MAX {
                break;
            }
            let Some(Mutation::Append {
                prepare,
                durable: requires_sync,
                bytes: record_bytes,
                ..
            }) = queue.pop_front()
            else {
                unreachable!("append prefix was checked");
            };
            *bytes += record_bytes;
            self.queued_bytes
                .set(self.queued_bytes.get().saturating_sub(record_bytes));
            *durable |= requires_sync;
            batch.push(prepare);
        }
    }

    /// How long to wait for more prepares before the barrier, if at all.
    ///
    /// `None` for a disabled delay, a group already at its bounds, or arrivals
    /// spaced wider than the delay, where the wait would expire before the next
    /// prepare reached the queue.
    fn group_commit_wait(&self, batch: &[Frozen<4096>], bytes: u64) -> Option<Duration> {
        let delay = self.group_commit_delay.get();
        if delay.is_zero() || batch.len() >= APPEND_BATCH_OPS_MAX || bytes >= APPEND_BATCH_BYTES_MAX
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

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::{Command, Operation};
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
        let (segments, offsets) = persistence.take_dirty_files();
        assert_eq!(segments.into_iter().collect::<Vec<_>>(), vec![0]);
        assert!(offsets[0].is_empty());
        assert!(offsets[1].contains(&9));
        let (segments, offsets) = persistence.take_dirty_files();
        assert!(segments.is_empty());
        assert!(offsets.iter().all(BTreeSet::is_empty));
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
