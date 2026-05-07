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

use crate::oneshot::{self, Receiver, Sender};
use crate::vsr_timeout::{TimeoutKind, TimeoutManager};
use crate::{
    AckLogEvent, Consensus, ControlActionLogEvent, DvcQuorumArray, IgnoreReason, Pipeline,
    PlaneKind, PrepareLogEvent, Project, ReplicaLogContext, SimEventKind, StoredDvc,
    ViewChangeLogEvent, ViewChangeReason, dvc_count, dvc_max_commit, dvc_quorum_array_empty,
    dvc_record, dvc_reset, dvc_select_winner, emit_replica_event, emit_sim_event,
};
use bit_set::BitSet;
use iggy_binary_protocol::{
    Command2, ConsensusHeader, DoViewChangeHeader, GenericHeader, Message, PrepareHeader,
    PrepareOkHeader, ReplyHeader, RequestHeader, StartViewChangeHeader, StartViewHeader,
};
use message_bus::IggyMessageBus;
use message_bus::MessageBus;
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;

pub trait Sequencer {
    type Sequence;
    /// Get the current sequence number
    fn current_sequence(&self) -> Self::Sequence;

    /// Allocate the next sequence number.
    /// TODO Should this return a Future<Output = u64>? for async case?
    fn next_sequence(&self) -> Self::Sequence;

    /// Update the current sequence number.
    fn set_sequence(&self, sequence: Self::Sequence);
}

#[derive(Debug)]
pub struct LocalSequencer {
    op: Cell<u64>,
}

impl LocalSequencer {
    #[must_use]
    pub const fn new(initial_op: u64) -> Self {
        Self {
            op: Cell::new(initial_op),
        }
    }
}

impl Sequencer for LocalSequencer {
    type Sequence = u64;

    fn current_sequence(&self) -> Self::Sequence {
        self.op.get()
    }

    fn next_sequence(&self) -> Self::Sequence {
        let current = self.current_sequence();
        let next = current.checked_add(1).expect("sequence number overflow");
        self.set_sequence(next);
        next
    }

    fn set_sequence(&self, sequence: Self::Sequence) {
        self.op.set(sequence);
    }
}

/// TODO The below numbers need to be added a consensus config
/// TODO understand how to configure these numbers.
/// Maximum number of prepares that can be in-flight in the pipeline.
pub const PIPELINE_PREPARE_QUEUE_MAX: usize = 8;

/// Max accepted-but-not-yet-prepared requests buffered behind a full
/// prepare queue. Beyond this, requests drop and the client retries.
pub const PIPELINE_REQUEST_QUEUE_MAX: usize = 64;

/// Maximum number of replicas in a cluster.
pub const REPLICAS_MAX: usize = 32;

/// Maximum number of clients tracked in the clients table.
/// When exceeded, the client with the oldest committed request is evicted.
pub const CLIENTS_TABLE_MAX: usize = 8192;

#[derive(Debug)]
pub struct PipelineEntry {
    pub header: PrepareHeader,
    /// Bitmap of replicas that have acknowledged this prepare.
    pub ok_from_replicas: BitSet<u32>,
    /// Whether we've received a quorum of `prepare_ok` messages.
    pub ok_quorum_received: bool,
    /// In-process reply subscriber. `None` = network path (`message_bus`);
    /// `Some` = in-server awaiter. Set by [`Self::with_subscriber`], taken
    /// by commit handler via [`Self::take_reply_sender`]. Drop wakes
    /// receiver with `Canceled` (view-change reset, eviction, commit fail).
    pub(crate) reply_sender: Option<Sender<Message<ReplyHeader>>>,
}

impl PipelineEntry {
    /// Entry without subscriber (network path).
    #[must_use]
    pub fn new(header: PrepareHeader) -> Self {
        Self {
            header,
            ok_from_replicas: BitSet::with_capacity(REPLICAS_MAX),
            ok_quorum_received: false,
            reply_sender: None,
        }
    }

    /// Entry paired with a fresh receiver, wakes when this prepare commits.
    ///
    /// # Returns
    /// `(entry, receiver)`. Receiver resolves with reply, or `Err(Canceled)`
    /// if entry drops before commit.
    #[must_use]
    pub fn with_subscriber(header: PrepareHeader) -> (Self, Receiver<Message<ReplyHeader>>) {
        let (sender, receiver) = oneshot::channel();
        let entry = Self {
            header,
            ok_from_replicas: BitSet::with_capacity(REPLICAS_MAX),
            ok_quorum_received: false,
            reply_sender: Some(sender),
        };
        (entry, receiver)
    }

    /// Take reply sender; caller fires after slot update (slot-first ordering).
    /// Idempotent: subsequent calls return `None`.
    pub const fn take_reply_sender(&mut self) -> Option<Sender<Message<ReplyHeader>>> {
        self.reply_sender.take()
    }

    /// Record a `prepare_ok` from the given replica.
    /// Returns the new count of acknowledgments.
    pub fn add_ack(&mut self, replica: u8) -> usize {
        self.ok_from_replicas.insert(replica as usize);
        self.ok_from_replicas.count()
    }

    /// Check if we have an ack from the given replica.
    #[must_use]
    pub fn has_ack(&self, replica: u8) -> bool {
        self.ok_from_replicas.contains(replica as usize)
    }

    /// Get the number of acks received.
    #[must_use]
    pub fn ack_count(&self) -> usize {
        self.ok_from_replicas.count()
    }
}

/// Accepted request waiting in `request_queue` for a prepare slot.
#[derive(Debug)]
pub struct RequestEntry {
    pub message: Message<RequestHeader>,
    // TODO: populate from monotonic clock at push, promote to `pub` for
    // age-based filtering. Currently `0`; `pub(crate)` blocks sort-on-stub.
    #[allow(dead_code)]
    pub(crate) received_at: i64,
}

impl RequestEntry {
    #[must_use]
    pub const fn new(message: Message<RequestHeader>) -> Self {
        Self {
            message,
            received_at: 0,
        }
    }
}

/// Two-queue pipeline: in-flight prepares + buffered requests.
#[derive(Debug)]
pub struct LocalPipeline {
    /// Uncommitted prepares; cap [`PIPELINE_PREPARE_QUEUE_MAX`].
    prepare_queue: VecDeque<PipelineEntry>,
    /// Requests awaiting a prepare slot; cap [`PIPELINE_REQUEST_QUEUE_MAX`].
    request_queue: VecDeque<RequestEntry>,
}

impl Default for LocalPipeline {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalPipeline {
    #[must_use]
    pub fn new() -> Self {
        Self {
            prepare_queue: VecDeque::with_capacity(PIPELINE_PREPARE_QUEUE_MAX),
            request_queue: VecDeque::with_capacity(PIPELINE_REQUEST_QUEUE_MAX),
        }
    }

    #[must_use]
    pub fn prepare_count(&self) -> usize {
        self.prepare_queue.len()
    }

    #[must_use]
    pub fn prepare_queue_full(&self) -> bool {
        self.prepare_queue.len() >= PIPELINE_PREPARE_QUEUE_MAX
    }

    #[must_use]
    pub fn request_queue_len(&self) -> usize {
        self.request_queue.len()
    }

    #[must_use]
    pub fn request_queue_full(&self) -> bool {
        self.request_queue.len() >= PIPELINE_REQUEST_QUEUE_MAX
    }

    #[must_use]
    pub fn request_queue_is_empty(&self) -> bool {
        self.request_queue.is_empty()
    }

    /// Buffer a request behind a full prepare queue.
    ///
    /// # Errors
    /// `Err(entry)` if request queue also full; caller drops, client retries.
    pub fn push_request(&mut self, entry: RequestEntry) -> Result<(), RequestEntry> {
        if self.request_queue_full() {
            return Err(entry);
        }
        self.request_queue.push_back(entry);
        Ok(())
    }

    /// Pop request-queue head. Called when a prepare commits and frees a slot.
    pub fn pop_request(&mut self) -> Option<RequestEntry> {
        self.request_queue.pop_front()
    }

    /// True iff `prepare_queue` is full (NOT including `request_queue`).
    /// Callers branch on this between direct push and [`Self::push_request`].
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.prepare_queue_full()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.prepare_queue.is_empty() && self.request_queue.is_empty()
    }

    /// Push a new entry to the pipeline.
    ///
    /// # Panics
    /// - If message queue is full.
    /// - If the message doesn't chain correctly to the previous entry.
    pub fn push(&mut self, entry: PipelineEntry) {
        assert!(!self.prepare_queue_full(), "prepare queue is full");

        let header = entry.header;

        if let Some(tail) = self.prepare_queue.back() {
            let tail_header = &tail.header;
            assert_eq!(
                header.op,
                tail_header.op + 1,
                "sequence must be sequential: expected {}, got {}",
                tail_header.op + 1,
                header.op
            );
            assert_eq!(
                header.parent, tail_header.checksum,
                "parent must chain to previous checksum"
            );
            assert!(header.view >= tail_header.view, "view cannot go backwards");
        }

        self.prepare_queue.push_back(entry);
    }

    #[allow(clippy::needless_pass_by_value)]
    pub fn push_message(&mut self, message: Message<PrepareHeader>) {
        self.push(PipelineEntry::new(*message.header()));
    }

    /// Pop the oldest message (after it's been committed).
    ///
    pub fn pop_message(&mut self) -> Option<PipelineEntry> {
        self.prepare_queue.pop_front()
    }

    /// Get the head (oldest) prepare.
    #[must_use]
    pub fn prepare_head(&self) -> Option<&PipelineEntry> {
        self.prepare_queue.front()
    }

    pub fn prepare_head_mut(&mut self) -> Option<&mut PipelineEntry> {
        self.prepare_queue.front_mut()
    }

    /// Get the tail (newest) prepare.
    #[must_use]
    pub fn prepare_tail(&self) -> Option<&PipelineEntry> {
        self.prepare_queue.back()
    }

    /// Find a message by op number and checksum (immutable).
    // Pipeline bounded at PIPELINE_PREPARE_QUEUE_MAX (8) entries; index always fits in usize.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn message_by_op_and_checksum(&self, op: u64, checksum: u128) -> Option<&PipelineEntry> {
        let head_op = self.prepare_queue.front()?.header.op;
        let tail_op = self.prepare_queue.back()?.header.op;

        // Verify consecutive ops invariant
        debug_assert_eq!(
            tail_op,
            head_op + self.prepare_queue.len() as u64 - 1,
            "prepare queue ops not consecutive"
        );

        if op < head_op || op > tail_op {
            return None;
        }

        let index = (op - head_op) as usize;
        let entry = self.prepare_queue.get(index)?;

        debug_assert_eq!(entry.header.op, op);

        if entry.header.checksum == checksum {
            Some(entry)
        } else {
            None
        }
    }

    /// Find a message by op number only.
    // Pipeline bounded at PIPELINE_PREPARE_QUEUE_MAX (8) entries; index always fits in usize.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn message_by_op(&self, op: u64) -> Option<&PipelineEntry> {
        let head_op = self.prepare_queue.front()?.header.op;

        if op < head_op {
            return None;
        }

        let index = (op - head_op) as usize;
        self.prepare_queue.get(index)
    }

    /// Get mutable reference to a message entry by op number.
    /// Returns None if op is not in the pipeline.
    // Pipeline bounded at PIPELINE_PREPARE_QUEUE_MAX (8) entries; index always fits in usize.
    #[allow(clippy::cast_possible_truncation)]
    pub fn message_by_op_mut(&mut self, op: u64) -> Option<&mut PipelineEntry> {
        let head_op = self.prepare_queue.front()?.header.op;
        if op < head_op {
            return None;
        }
        let index = (op - head_op) as usize;
        if index >= self.prepare_queue.len() {
            return None;
        }
        self.prepare_queue.get_mut(index)
    }

    /// Get the entry at the head of the prepare queue (oldest uncommitted).
    #[must_use]
    pub fn head(&self) -> Option<&PipelineEntry> {
        self.prepare_queue.front()
    }

    /// True if either queue holds a message from `client`. Used by preflights
    /// for in-progress dedup; request_queue-only entries still count.
    #[must_use]
    pub fn has_message_from_client(&self, client: u128) -> bool {
        self.prepare_queue.iter().any(|p| p.header.client == client)
            || self
                .request_queue
                .iter()
                .any(|r| r.message.header().client == client)
    }

    /// Verify pipeline invariants.
    ///
    /// # Panics
    /// If any invariant is violated.
    pub fn verify(&self) {
        // Check capacity limits
        assert!(self.prepare_queue.len() <= PIPELINE_PREPARE_QUEUE_MAX);
        assert!(self.request_queue.len() <= PIPELINE_REQUEST_QUEUE_MAX);

        // Verify prepare queue hash chain
        if let Some(head) = self.prepare_queue.front() {
            let mut expected_parent = head.header.parent;

            for (expected_op, entry) in (head.header.op..).zip(self.prepare_queue.iter()) {
                let header = &entry.header;

                assert_eq!(header.op, expected_op, "ops must be sequential");
                assert_eq!(header.parent, expected_parent, "must be hash-chained");

                expected_parent = header.checksum;
            }
        }
    }

    /// Clear both queues at view-change completion. New primary rebuilds
    /// prepares from journal; clients retry dropped requests via read-timeout.
    pub fn clear(&mut self) {
        self.prepare_queue.clear();
        self.request_queue.clear();
    }

    /// Drop reply senders on all prepare entries; receivers wake with
    /// `Canceled`. Prepares survive (DVC log reconciliation), cleared at
    /// view-change *completion*. `request_queue` untouched, see
    /// [`Self::clear_request_queue`].
    pub fn cancel_all_subscribers(&mut self) {
        for entry in &mut self.prepare_queue {
            entry.reply_sender.take();
        }
    }

    /// Drop `request_queue` only; preserve `prepare_queue`. View-change reset.
    ///
    /// # Safety
    /// Without this, stale primary-era requests survive into the next view.
    /// If `drain_request_queue_into_prepares` fires pre-completion, those
    /// requests project via `pipeline_prepare_common`, which asserts
    /// `is_primary() && is_normal()` and panics the shard pump.
    pub fn clear_request_queue(&mut self) {
        self.request_queue.clear();
    }
}

impl Pipeline for LocalPipeline {
    type Entry = PipelineEntry;
    type Request = RequestEntry;

    fn push(&mut self, entry: Self::Entry) {
        Self::push(self, entry);
    }

    fn pop(&mut self) -> Option<Self::Entry> {
        Self::pop_message(self)
    }

    fn clear(&mut self) {
        Self::clear(self);
    }

    fn entry_by_op(&self, op: u64) -> Option<&Self::Entry> {
        Self::message_by_op(self, op)
    }

    fn entry_by_op_mut(&mut self, op: u64) -> Option<&mut Self::Entry> {
        Self::message_by_op_mut(self, op)
    }

    fn entry_by_op_and_checksum(&self, op: u64, checksum: u128) -> Option<&Self::Entry> {
        Self::message_by_op_and_checksum(self, op, checksum)
    }

    fn head(&self) -> Option<&Self::Entry> {
        Self::head(self)
    }

    fn is_full(&self) -> bool {
        Self::is_full(self)
    }

    fn is_empty(&self) -> bool {
        Self::is_empty(self)
    }

    fn len(&self) -> usize {
        self.prepare_count()
    }

    fn verify(&self) {
        Self::verify(self);
    }

    fn has_message_from_client(&self, client_id: u128) -> bool {
        Self::has_message_from_client(self, client_id)
    }

    fn cancel_all_subscribers(&mut self) {
        Self::cancel_all_subscribers(self);
    }

    fn clear_request_queue(&mut self) {
        Self::clear_request_queue(self);
    }

    fn push_request(&mut self, request: Self::Request) -> Result<(), Self::Request> {
        Self::push_request(self, request)
    }

    fn pop_request(&mut self) -> Option<Self::Request> {
        Self::pop_request(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Normal,
    ViewChange,
    Recovering,
}

/// Actions to be taken by the caller after processing a VSR event.
#[derive(Debug, Clone)]
pub enum VsrAction {
    /// Send `StartViewChange` to all replicas.
    SendStartViewChange { view: u32, namespace: u64 },
    /// Send `DoViewChange` to primary.
    SendDoViewChange {
        view: u32,
        target: u8,
        log_view: u32,
        op: u64,
        commit: u64,
        namespace: u64,
    },
    /// Send `StartView` to all backups (as new primary).
    SendStartView {
        view: u32,
        op: u64,
        commit: u64,
        namespace: u64,
    },
    /// Send `PrepareOK` for each op in `[from_op, to_op]` that is present in the WAL.
    ///
    /// The caller MUST verify each op exists in the journal before sending.
    /// Sending `PrepareOk` for a missing op is a safety violation, it can
    /// cause the primary to commit an op without enough replicas holding the data.
    SendPrepareOk {
        view: u32,
        from_op: u64,
        to_op: u64,
        target: u8,
        namespace: u64,
    },
    /// Retransmit uncommitted prepares from the WAL to replicas that haven't acked.
    ///
    /// Emitted when the primary's prepare timeout fires and there are
    /// uncommitted entries in the pipeline. Each entry is a prepare header
    /// (for journal lookup) and the list of replica IDs that need it.
    RetransmitPrepares {
        targets: Vec<(PrepareHeader, Vec<u8>)>,
    },
    /// Rebuild the pipeline from the journal after a view change.
    ///
    /// The new primary must re-populate its pipeline with uncommitted ops
    /// from the WAL so that incoming `PrepareOk` messages can be matched
    /// and commits can proceed.
    RebuildPipeline { from_op: u64, to_op: u64 },
    /// Catch up `commit_min` to `commit_max` by applying committed ops from the
    /// journal. Emitted during view change completion so the new primary
    /// is fully caught up before accepting new requests.
    CommitJournal,
    /// Primary heartbeat: send current commit point to all backups.
    ///
    /// Emitted when the `CommitMessage` timeout fires. Prevents backups
    /// from starting a view change during idle periods.
    SendCommit {
        view: u32,
        commit: u64,
        namespace: u64,
        timestamp_monotonic: u64,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrepareOkOutcome {
    Accepted {
        ack_count: usize,
        quorum_reached: bool,
    },
    Ignored {
        reason: IgnoreReason,
    },
}

impl PrepareOkOutcome {
    #[must_use]
    pub const fn quorum_reached(self) -> bool {
        match self {
            Self::Accepted { quorum_reached, .. } => quorum_reached,
            Self::Ignored { .. } => false,
        }
    }
}

#[allow(unused)]
#[derive(Debug)]
pub struct VsrConsensus<B = IggyMessageBus, P = LocalPipeline>
where
    B: MessageBus,
    P: Pipeline,
{
    cluster: u128,
    replica: u8,
    replica_count: u8,
    namespace: u64,

    view: Cell<u32>,

    // The latest view where
    // - the replica was a primary and acquired a DVC quorum, or
    // - the replica was a backup and processed a SV message.
    // i.e. the latest view in which this replica changed its head message.
    // Initialized from the superblock's VSRState.
    // Invariants:
    // * `replica.log_view ≥ replica.log_view_durable`
    // * `replica.log_view = 0` when replica_count=1.
    log_view: Cell<u32>,
    status: Cell<Status>,

    /// Highest op number that has been locally executed (state machine applied,
    /// client table updated). Advances one-by-one in `commit_journal` (backup)
    /// and `on_ack` (primary). On a normal primary, `commit_min == commit_max`.
    commit_min: Cell<u64>,

    /// Highest op number known to be committed by the cluster. Advances
    /// immediately when the replica learns about commits (from prepare
    /// messages, commit heartbeats, or view change messages).
    commit_max: Cell<u64>,

    sequencer: LocalSequencer,

    last_timestamp: Cell<u64>,
    last_prepare_checksum: Cell<u128>,

    pipeline: RefCell<P>,

    message_bus: B,
    loopback_queue: RefCell<VecDeque<Message<GenericHeader>>>,
    /// Tracks start view change messages received from all replicas (including self)
    start_view_change_from_all_replicas: RefCell<BitSet<u32>>,

    /// Tracks DVC messages received (only used by primary candidate)
    /// Stores metadata; actual log comes from message
    do_view_change_from_all_replicas: RefCell<DvcQuorumArray>,
    /// Whether DVC quorum has been achieved in current view change
    do_view_change_quorum: Cell<bool>,
    /// Whether we've sent our own SVC for current view
    sent_own_start_view_change: Cell<bool>,
    /// Whether we've sent our own DVC for current view
    sent_own_do_view_change: Cell<bool>,

    timeouts: RefCell<TimeoutManager>,

    /// Monotonic timestamp from the most recent accepted commit heartbeat.
    /// Old/replayed commit messages with a lower timestamp are ignored.
    heartbeat_timestamp: Cell<u64>,
}

impl<B: MessageBus, P: Pipeline<Entry = PipelineEntry>> VsrConsensus<B, P> {
    /// # Panics
    /// - If `replica >= replica_count`.
    /// - If `replica_count < 1`.
    pub fn new(
        cluster: u128,
        replica: u8,
        replica_count: u8,
        namespace: u64,
        message_bus: B,
        pipeline: P,
    ) -> Self {
        assert!(
            replica < replica_count,
            "replica index must be < replica_count"
        );
        assert!(replica_count >= 1, "need at least 1 replica");
        // TODO: Verify that XOR-based seeding provides sufficient jitter diversity
        // across groups. Consider using a proper hash (e.g., Murmur3) of
        // (replica_id, namespace) for production.
        let timeout_seed = u128::from(replica) ^ u128::from(namespace);
        Self {
            cluster,
            replica,
            replica_count,
            namespace,
            view: Cell::new(0),
            log_view: Cell::new(0),
            status: Cell::new(Status::Recovering),
            sequencer: LocalSequencer::new(0),
            commit_min: Cell::new(0),
            commit_max: Cell::new(0),
            last_timestamp: Cell::new(0),
            last_prepare_checksum: Cell::new(0),
            pipeline: RefCell::new(pipeline),
            message_bus,
            loopback_queue: RefCell::new(VecDeque::with_capacity(PIPELINE_PREPARE_QUEUE_MAX)),
            start_view_change_from_all_replicas: RefCell::new(BitSet::with_capacity(REPLICAS_MAX)),
            do_view_change_from_all_replicas: RefCell::new(dvc_quorum_array_empty()),
            do_view_change_quorum: Cell::new(false),
            sent_own_start_view_change: Cell::new(false),
            sent_own_do_view_change: Cell::new(false),
            timeouts: RefCell::new(TimeoutManager::new(timeout_seed)),
            heartbeat_timestamp: Cell::new(0),
        }
    }

    pub fn init(&self) {
        self.status.set(Status::Normal);
        let mut timeouts = self.timeouts.borrow_mut();
        if self.is_primary() {
            timeouts.start(TimeoutKind::Prepare);
            timeouts.start(TimeoutKind::CommitMessage);
        } else {
            timeouts.start(TimeoutKind::NormalHeartbeat);
        }
    }

    #[must_use]
    // cast_lossless: `u32::from()` unavailable in const fn.
    // cast_possible_truncation: modulo by replica_count (u8) guarantees result fits in u8.
    #[allow(clippy::cast_lossless, clippy::cast_possible_truncation)]
    pub const fn primary_index(&self, view: u32) -> u8 {
        (view % self.replica_count as u32) as u8
    }

    #[must_use]
    pub const fn is_primary(&self) -> bool {
        self.primary_index(self.view.get()) == self.replica
    }

    /// Advance `commit_max` - the highest op known to be committed by the cluster.
    ///
    /// Called when the replica learns about new commits from the primary
    /// (via prepare messages, commit heartbeats, or view change messages).
    ///
    /// # Panics
    /// If `commit_max` would be less than `commit_min` after the update
    /// (invariant violation).
    pub fn advance_commit_max(&self, commit: u64) {
        if commit > self.commit_max.get() {
            self.commit_max.set(commit);
        }
        assert!(self.commit_max.get() >= self.commit_min.get());
    }

    /// Advance `commit_min` - the highest op locally executed.
    ///
    /// Called after each op is applied through `commit_journal` (backup)
    /// or `on_ack` (primary). Must advance sequentially (by 1).
    ///
    /// # Panics
    /// - If `op` is not exactly `commit_min + 1` (must advance sequentially).
    /// - If `commit_min` would exceed `commit_max` after the update.
    pub fn advance_commit_min(&self, op: u64) {
        assert_eq!(
            op,
            self.commit_min.get() + 1,
            "commit_min must advance sequentially: expected {}, got {op}",
            self.commit_min.get() + 1
        );
        self.commit_min.set(op);
        assert!(self.commit_max.get() >= self.commit_min.get());
    }

    /// Maximum number of faulty replicas that can be tolerated.
    /// For a cluster of 2f+1 replicas, this returns f.
    #[must_use]
    pub const fn max_faulty(&self) -> usize {
        (self.replica_count as usize - 1) / 2
    }

    /// Quorum size = f + 1 = `max_faulty` + 1
    #[must_use]
    pub const fn quorum(&self) -> usize {
        self.max_faulty() + 1
    }

    /// Highest op locally executed (state machine applied, client table updated).
    #[must_use]
    pub const fn commit_min(&self) -> u64 {
        self.commit_min.get()
    }

    /// Highest op known to be committed by the cluster.
    #[must_use]
    pub const fn commit_max(&self) -> u64 {
        self.commit_max.get()
    }

    #[must_use]
    pub const fn replica(&self) -> u8 {
        self.replica
    }

    #[must_use]
    pub const fn sequencer(&self) -> &LocalSequencer {
        &self.sequencer
    }

    #[must_use]
    pub const fn view(&self) -> u32 {
        self.view.get()
    }

    pub fn set_view(&mut self, view: u32) {
        self.view.set(view);
    }

    #[must_use]
    pub const fn status(&self) -> Status {
        self.status.get()
    }

    // TODO(hubcio): returning &RefCell<P> leaks interior mutability - callers
    // could hold a Ref/RefMut across an .await and cause a runtime panic.
    // We had this problem with slab + ECS.
    #[must_use]
    pub const fn pipeline(&self) -> &RefCell<P> {
        &self.pipeline
    }

    #[must_use]
    pub const fn pipeline_mut(&mut self) -> &mut RefCell<P> {
        &mut self.pipeline
    }

    /// Push a pre-built [`PipelineEntry`]; start prepare timeout if idle.
    ///
    /// Shared by [`Consensus::pipeline_message`] (no subscriber) and
    /// [`Self::pipeline_message_with_subscriber`] (in-band receiver). The
    /// only difference is whether the entry carries `reply_sender`;
    /// everything else (sim event, timeout, primary assertion) is here.
    fn push_prepare_entry(
        &self,
        plane: PlaneKind,
        message: &Message<PrepareHeader>,
        entry: PipelineEntry,
    ) {
        assert!(self.is_primary(), "only primary can pipeline messages");

        let mut pipeline = self.pipeline.borrow_mut();
        pipeline.push(entry);
        let pipeline_depth = pipeline.len();
        drop(pipeline);

        let header = message.header();

        // Atomically advance sequencer + last_prepare_checksum with the
        // push. Without this, a sibling on_request that runs while on_replicate
        // awaits journal.append would project a duplicate op + parent.
        // The late set in on_replicate (metadata.rs / iggy_partition.rs) is
        // idempotent on primary and still needed on backup.
        self.sequencer.set_sequence(header.op);
        self.set_last_prepare_checksum(header.checksum);

        emit_sim_event(
            SimEventKind::PrepareQueued,
            &PrepareLogEvent {
                replica: ReplicaLogContext::from_consensus(self, plane),
                op: header.op,
                parent_checksum: header.parent,
                prepare_checksum: header.checksum,
                client_id: header.client,
                request_id: header.request,
                operation: header.operation,
                pipeline_depth,
            },
        );

        // Start (not reset) prepare timeout: an already-ticking timer must not
        // be pushed out by every new request. Drives retransmit on missing acks.
        let mut timeouts = self.timeouts.borrow_mut();
        if !timeouts.is_ticking(TimeoutKind::Prepare) {
            timeouts.start(TimeoutKind::Prepare);
        }
    }

    /// Push `message` with in-band reply subscriber.
    ///
    /// Like [`Consensus::pipeline_message`], but entry is built via
    /// [`PipelineEntry::with_subscriber`]; caller gets a [`Receiver`] that
    /// wakes via `take_reply_sender().send(reply)` from the commit handler
    /// (or `Canceled` on view-change reset / entry drop).
    ///
    /// In-process producers (e.g. `IggyMetadata::submit_register_in_process`)
    /// use this to learn their own prepare's commit without `send_to_client`.
    /// Additive: wire reply still fires (`commit_register`/`commit_reply`),
    /// so wire SDK + in-process awaiter both see the same reply.
    ///
    /// # Panics
    /// If not primary (mirrors [`Consensus::pipeline_message`]).
    pub fn pipeline_message_with_subscriber(
        &self,
        plane: PlaneKind,
        message: &Message<PrepareHeader>,
    ) -> Receiver<Message<ReplyHeader>> {
        let (entry, receiver) = PipelineEntry::with_subscriber(*message.header());
        self.push_prepare_entry(plane, message, entry);
        receiver
    }

    #[must_use]
    pub const fn cluster(&self) -> u128 {
        self.cluster
    }

    #[must_use]
    pub const fn replica_count(&self) -> u8 {
        self.replica_count
    }

    #[must_use]
    pub const fn namespace(&self) -> u64 {
        self.namespace
    }

    #[must_use]
    pub const fn last_prepare_checksum(&self) -> u128 {
        self.last_prepare_checksum.get()
    }

    pub fn set_last_prepare_checksum(&self, checksum: u128) {
        self.last_prepare_checksum.set(checksum);
    }

    #[must_use]
    pub const fn log_view(&self) -> u32 {
        self.log_view.get()
    }

    pub fn set_log_view(&self, log_view: u32) {
        self.log_view.set(log_view);
    }

    #[must_use]
    pub const fn is_primary_for_view(&self, view: u32) -> bool {
        self.primary_index(view) == self.replica
    }

    /// Count SVCs from OTHER replicas (excluding self).
    fn svc_count_excluding_self(&self) -> usize {
        let svc = self.start_view_change_from_all_replicas.borrow();
        let total = svc.count();
        if svc.contains(self.replica as usize) {
            total.saturating_sub(1)
        } else {
            total
        }
    }

    /// Reset SVC quorum tracking.
    fn reset_svc_quorum(&self) {
        self.start_view_change_from_all_replicas
            .borrow_mut()
            .make_empty();
    }

    /// Reset DVC quorum tracking.
    fn reset_dvc_quorum(&self) {
        dvc_reset(&mut self.do_view_change_from_all_replicas.borrow_mut());
        self.do_view_change_quorum.set(false);
    }

    /// Reset view-change state on view transition.
    ///
    /// - Clear loopback (stale `PrepareOks` would no-op).
    /// - Cancel subscribers (awaiters wake with `Canceled`).
    /// - Drop `request_queue` (buffered requests have no DVC role).
    ///
    /// `prepare_queue` survives here for DVC log reconciliation; cleared
    /// at view-change *completion*.
    ///
    /// # Safety
    /// `request_queue` clear required: a future broadening of
    /// `drain_request_queue_into_prepares` could project stale entries
    /// via `pipeline_prepare_common`, which panics on non-normal status.
    pub(crate) fn reset_view_change_state(&self) {
        self.reset_svc_quorum();
        self.reset_dvc_quorum();
        self.sent_own_start_view_change.set(false);
        self.sent_own_do_view_change.set(false);
        self.loopback_queue.borrow_mut().clear();
        let mut pipeline = self.pipeline.borrow_mut();
        pipeline.cancel_all_subscribers();
        pipeline.clear_request_queue();
    }

    /// Process one tick. Call this periodically (e.g., every 10ms).
    ///
    /// Returns a list of actions to take based on fired timeouts.
    /// Empty vec means no actions needed.
    pub fn tick(&self, plane: PlaneKind, current_op: u64, current_commit: u64) -> Vec<VsrAction> {
        let mut actions = Vec::new();
        let mut timeouts = self.timeouts.borrow_mut();

        // Phase 1: Tick all timeouts
        timeouts.tick();

        // Phase 2: Handle fired timeouts
        if timeouts.fired(TimeoutKind::NormalHeartbeat) {
            drop(timeouts);
            actions.extend(self.handle_normal_heartbeat_timeout(plane));
            timeouts = self.timeouts.borrow_mut();
        }

        if timeouts.fired(TimeoutKind::StartViewChangeMessage) {
            drop(timeouts);
            actions.extend(self.handle_start_view_change_message_timeout(plane));
            timeouts = self.timeouts.borrow_mut();
        }

        if timeouts.fired(TimeoutKind::DoViewChangeMessage) {
            drop(timeouts);
            actions.extend(self.handle_do_view_change_message_timeout(
                plane,
                current_op,
                current_commit,
            ));
            timeouts = self.timeouts.borrow_mut();
        }

        if timeouts.fired(TimeoutKind::Prepare) {
            drop(timeouts);
            actions.extend(self.handle_prepare_timeout());
            timeouts = self.timeouts.borrow_mut();
        }

        if timeouts.fired(TimeoutKind::CommitMessage) {
            drop(timeouts);
            actions.extend(self.handle_commit_message_timeout());
            timeouts = self.timeouts.borrow_mut();
        }

        if timeouts.fired(TimeoutKind::ViewChangeStatus) {
            drop(timeouts);
            actions.extend(self.handle_view_change_status_timeout(plane));
            // timeouts = self.timeouts.borrow_mut(); // Not needed if last
        }

        actions
    }

    /// Called when `normal_heartbeat` timeout fires.
    /// Backup hasn't heard from primary - start view change.
    fn handle_normal_heartbeat_timeout(&self, plane: PlaneKind) -> Vec<VsrAction> {
        // Only backups trigger view change on heartbeat timeout
        if self.is_primary() {
            return Vec::new();
        }

        // Already in view change
        if self.status.get() == Status::ViewChange {
            return Vec::new();
        }

        // Advance to new view and transition to view change
        let old_view = self.view.get();
        let new_view = old_view + 1;

        self.view.set(new_view);
        self.status.set(Status::ViewChange);
        self.reset_view_change_state();
        self.sent_own_start_view_change.set(true);
        self.start_view_change_from_all_replicas
            .borrow_mut()
            .insert(self.replica as usize);

        // Update timeouts for view change status
        {
            let mut timeouts = self.timeouts.borrow_mut();
            timeouts.stop(TimeoutKind::NormalHeartbeat);
            timeouts.start(TimeoutKind::StartViewChangeMessage);
            timeouts.start(TimeoutKind::ViewChangeStatus);
        }

        emit_sim_event(
            SimEventKind::ViewChangeStarted,
            &ViewChangeLogEvent {
                replica: ReplicaLogContext::from_consensus(self, plane),
                old_view,
                new_view,
                reason: ViewChangeReason::NormalHeartbeatTimeout,
            },
        );

        let action = VsrAction::SendStartViewChange {
            view: new_view,
            namespace: self.namespace,
        };
        emit_sim_event(
            SimEventKind::ControlMessageScheduled,
            &ControlActionLogEvent::from_vsr_action(
                ReplicaLogContext::from_consensus(self, plane),
                &action,
            ),
        );
        vec![action]
    }

    /// Resend SVC message if we've started view change.
    fn handle_start_view_change_message_timeout(&self, plane: PlaneKind) -> Vec<VsrAction> {
        if !self.sent_own_start_view_change.get() {
            return Vec::new();
        }

        self.timeouts
            .borrow_mut()
            .reset(TimeoutKind::StartViewChangeMessage);

        let action = VsrAction::SendStartViewChange {
            view: self.view.get(),
            namespace: self.namespace,
        };
        emit_sim_event(
            SimEventKind::ControlMessageScheduled,
            &ControlActionLogEvent::from_vsr_action(
                ReplicaLogContext::from_consensus(self, plane),
                &action,
            ),
        );
        vec![action]
    }

    /// Resend DVC message if we've sent one.
    fn handle_do_view_change_message_timeout(
        &self,
        plane: PlaneKind,
        current_op: u64,
        current_commit: u64,
    ) -> Vec<VsrAction> {
        if self.status.get() != Status::ViewChange {
            return Vec::new();
        }

        if !self.sent_own_do_view_change.get() {
            return Vec::new();
        }

        // If we're primary candidate with quorum, don't resend
        if self.is_primary() && self.do_view_change_quorum.get() {
            return Vec::new();
        }

        self.timeouts
            .borrow_mut()
            .reset(TimeoutKind::DoViewChangeMessage);

        let action = VsrAction::SendDoViewChange {
            view: self.view.get(),
            target: self.primary_index(self.view.get()),
            log_view: self.log_view.get(),
            op: current_op,
            commit: current_commit,
            namespace: self.namespace,
        };
        emit_sim_event(
            SimEventKind::ControlMessageScheduled,
            &ControlActionLogEvent::from_vsr_action(
                ReplicaLogContext::from_consensus(self, plane),
                &action,
            ),
        );
        vec![action]
    }

    /// Escalate to next view if stuck in view change.
    fn handle_view_change_status_timeout(&self, plane: PlaneKind) -> Vec<VsrAction> {
        if self.status.get() != Status::ViewChange {
            return Vec::new();
        }

        // Escalate: try next view
        let old_view = self.view.get();
        let next_view = old_view + 1;

        self.view.set(next_view);
        self.reset_view_change_state();
        self.sent_own_start_view_change.set(true);
        self.start_view_change_from_all_replicas
            .borrow_mut()
            .insert(self.replica as usize);

        self.timeouts
            .borrow_mut()
            .reset(TimeoutKind::ViewChangeStatus);

        emit_sim_event(
            SimEventKind::ViewChangeStarted,
            &ViewChangeLogEvent {
                replica: ReplicaLogContext::from_consensus(self, plane),
                old_view,
                new_view: next_view,
                reason: ViewChangeReason::ViewChangeStatusTimeout,
            },
        );

        let action = VsrAction::SendStartViewChange {
            view: next_view,
            namespace: self.namespace,
        };
        emit_sim_event(
            SimEventKind::ControlMessageScheduled,
            &ControlActionLogEvent::from_vsr_action(
                ReplicaLogContext::from_consensus(self, plane),
                &action,
            ),
        );
        vec![action]
    }

    /// Collect uncommitted pipeline entries that should be retransmitted.
    ///
    /// Returns `(PrepareHeader, Vec<u8>)` pairs: each op that hasn't reached
    /// quorum paired with the replica IDs that haven't acked it.
    fn retransmit_targets(&self) -> Vec<(PrepareHeader, Vec<u8>)> {
        let pipeline = self.pipeline.borrow();
        let current_op = self.sequencer.current_sequence();
        let replica_count = self.replica_count;
        let mut targets = Vec::new();

        let mut op = self.commit_max() + 1;
        while op <= current_op {
            if let Some(entry) = pipeline.entry_by_op(op)
                && !entry.ok_quorum_received
            {
                let missing: Vec<u8> = (0..replica_count).filter(|&r| !entry.has_ack(r)).collect();
                if !missing.is_empty() {
                    targets.push((entry.header, missing));
                }
            }
            op += 1;
        }

        targets
    }

    /// Retransmit uncommitted prepares when the prepare timeout fires.
    ///
    /// Only acts on the primary in normal status with a non-empty pipeline.
    /// Resets the timeout with backoff on each firing.
    fn handle_prepare_timeout(&self) -> Vec<VsrAction> {
        if !self.is_primary() || self.status.get() != Status::Normal {
            return Vec::new();
        }

        if self.pipeline.borrow().is_empty() {
            return Vec::new();
        }

        let targets = self.retransmit_targets();
        if targets.is_empty() {
            return Vec::new();
        }

        self.timeouts.borrow_mut().backoff(TimeoutKind::Prepare);

        vec![VsrAction::RetransmitPrepares { targets }]
    }

    /// Primary heartbeat: send commit point to all backups so they know
    /// the primary is alive and can advance their own `commit_max`.
    fn handle_commit_message_timeout(&self) -> Vec<VsrAction> {
        if !self.is_primary() || self.status.get() != Status::Normal {
            return Vec::new();
        }

        self.timeouts.borrow_mut().reset(TimeoutKind::CommitMessage);

        // Don't advertise a commit point we haven't locally executed yet.
        // After view change the new primary may have commit_min < commit_max
        // until commit_journal catches up. Send commit_min (what we've
        // actually applied) so backups don't advance past us.
        let ts = self.heartbeat_timestamp.get() + 1;
        self.heartbeat_timestamp.set(ts);

        vec![VsrAction::SendCommit {
            view: self.view.get(),
            commit: self.commit_min.get(),
            namespace: self.namespace,
            timestamp_monotonic: ts,
        }]
    }

    /// Handle a received `StartViewChange` message.
    ///
    /// "When replica i receives STARTVIEWCHANGE messages for its view-number
    /// from f OTHER replicas, it sends a DOVIEWCHANGE message to the node
    /// that will be the primary in the new view."
    ///
    /// # Panics
    /// If `header.namespace` does not match this replica's namespace.
    pub fn handle_start_view_change(
        &self,
        plane: PlaneKind,
        header: &StartViewChangeHeader,
    ) -> Vec<VsrAction> {
        assert_eq!(
            header.namespace, self.namespace,
            "SVC routed to wrong group"
        );
        let from_replica = header.replica;
        let msg_view = header.view;

        // Ignore SVCs for old views
        if msg_view < self.view.get() {
            return Vec::new();
        }

        let mut actions = Vec::new();

        // If SVC is for a higher view, advance to that view
        if msg_view > self.view.get() {
            let old_view = self.view.get();
            self.view.set(msg_view);
            self.status.set(Status::ViewChange);
            self.reset_view_change_state();
            self.sent_own_start_view_change.set(true);
            self.start_view_change_from_all_replicas
                .borrow_mut()
                .insert(self.replica as usize);

            // Update timeouts
            {
                let mut timeouts = self.timeouts.borrow_mut();
                timeouts.stop(TimeoutKind::NormalHeartbeat);
                timeouts.start(TimeoutKind::StartViewChangeMessage);
                timeouts.start(TimeoutKind::ViewChangeStatus);
            }

            emit_sim_event(
                SimEventKind::ViewChangeStarted,
                &ViewChangeLogEvent {
                    replica: ReplicaLogContext::from_consensus(self, plane),
                    old_view,
                    new_view: msg_view,
                    reason: ViewChangeReason::ReceivedStartViewChange,
                },
            );

            // Send our own SVC
            let action = VsrAction::SendStartViewChange {
                view: msg_view,
                namespace: self.namespace,
            };
            emit_sim_event(
                SimEventKind::ControlMessageScheduled,
                &ControlActionLogEvent::from_vsr_action(
                    ReplicaLogContext::from_consensus(self, plane),
                    &action,
                ),
            );
            actions.push(action);
        }

        // Record the SVC from sender
        self.start_view_change_from_all_replicas
            .borrow_mut()
            .insert(from_replica as usize);

        // Check if we have f SVCs from OTHER replicas
        // We need f SVCs from others to send DVC
        if !self.sent_own_do_view_change.get()
            && self.svc_count_excluding_self() >= self.max_faulty()
        {
            self.sent_own_do_view_change.set(true);

            let primary_candidate = self.primary_index(self.view.get());
            let current_op = self.sequencer.current_sequence();
            // DVC uses commit_min: the replica's actual execution progress.
            let current_commit = self.commit_min.get();

            // Start DVC timeout
            self.timeouts
                .borrow_mut()
                .start(TimeoutKind::DoViewChangeMessage);

            let action = VsrAction::SendDoViewChange {
                view: self.view.get(),
                target: primary_candidate,
                log_view: self.log_view.get(),
                op: current_op,
                commit: current_commit,
                namespace: self.namespace,
            };
            emit_sim_event(
                SimEventKind::ControlMessageScheduled,
                &ControlActionLogEvent::from_vsr_action(
                    ReplicaLogContext::from_consensus(self, plane),
                    &action,
                ),
            );
            actions.push(action);

            // If we are the primary candidate, record our own DVC
            if primary_candidate == self.replica {
                let own_dvc = StoredDvc {
                    replica: self.replica,
                    log_view: self.log_view.get(),
                    op: current_op,
                    commit: current_commit,
                };
                dvc_record(
                    &mut self.do_view_change_from_all_replicas.borrow_mut(),
                    own_dvc,
                );

                // Check if we now have quorum
                if dvc_count(&self.do_view_change_from_all_replicas.borrow()) >= self.quorum() {
                    self.do_view_change_quorum.set(true);
                    actions.extend(self.complete_view_change_as_primary(plane));
                }
            }
        }

        actions
    }

    /// Handle a received `DoViewChange` message (only relevant for primary candidate).
    ///
    /// "When the new primary receives f + 1 DOVIEWCHANGE messages from different
    /// replicas (including itself), it sets its view-number to that in the messages
    /// and selects as the new log the one contained in the message with the largest v'..."
    ///
    /// # Panics
    /// If `header.namespace` does not match this replica's namespace.
    pub fn handle_do_view_change(
        &self,
        plane: PlaneKind,
        header: &DoViewChangeHeader,
    ) -> Vec<VsrAction> {
        assert_eq!(
            header.namespace, self.namespace,
            "DVC routed to wrong group"
        );
        let from_replica = header.replica;
        let msg_view = header.view;
        let msg_log_view = header.log_view;
        let msg_op = header.op;
        let msg_commit = header.commit;

        // Ignore DVCs for old views
        if msg_view < self.view.get() {
            return Vec::new();
        }

        let mut actions = Vec::new();

        // If DVC is for a higher view, advance to that view
        if msg_view > self.view.get() {
            let old_view = self.view.get();
            self.view.set(msg_view);
            self.status.set(Status::ViewChange);
            self.reset_view_change_state();
            self.sent_own_start_view_change.set(true);
            self.start_view_change_from_all_replicas
                .borrow_mut()
                .insert(self.replica as usize);

            // Update timeouts
            {
                let mut timeouts = self.timeouts.borrow_mut();
                timeouts.stop(TimeoutKind::NormalHeartbeat);
                timeouts.start(TimeoutKind::StartViewChangeMessage);
                timeouts.start(TimeoutKind::ViewChangeStatus);
            }

            emit_sim_event(
                SimEventKind::ViewChangeStarted,
                &ViewChangeLogEvent {
                    replica: ReplicaLogContext::from_consensus(self, plane),
                    old_view,
                    new_view: msg_view,
                    reason: ViewChangeReason::ReceivedDoViewChange,
                },
            );

            // Send our own SVC
            let action = VsrAction::SendStartViewChange {
                view: msg_view,
                namespace: self.namespace,
            };
            emit_sim_event(
                SimEventKind::ControlMessageScheduled,
                &ControlActionLogEvent::from_vsr_action(
                    ReplicaLogContext::from_consensus(self, plane),
                    &action,
                ),
            );
            actions.push(action);
        }

        // Only the primary candidate processes DVCs for quorum
        if !self.is_primary_for_view(self.view.get()) {
            return actions;
        }

        // Must be in view change to process DVCs
        if self.status.get() != Status::ViewChange {
            return actions;
        }

        let current_op = self.sequencer.current_sequence();
        // Use commit_min: the replica's actual execution progress.
        let current_commit = self.commit_min.get();

        // If we haven't sent our own DVC yet, record it
        if !self.sent_own_do_view_change.get() {
            self.sent_own_do_view_change.set(true);

            let own_dvc = StoredDvc {
                replica: self.replica,
                log_view: self.log_view.get(),
                op: current_op,
                commit: current_commit,
            };
            dvc_record(
                &mut self.do_view_change_from_all_replicas.borrow_mut(),
                own_dvc,
            );
        }

        // Record the received DVC
        let dvc = StoredDvc {
            replica: from_replica,
            log_view: msg_log_view,
            op: msg_op,
            commit: msg_commit,
        };
        dvc_record(&mut self.do_view_change_from_all_replicas.borrow_mut(), dvc);

        // Check if quorum achieved
        if !self.do_view_change_quorum.get()
            && dvc_count(&self.do_view_change_from_all_replicas.borrow()) >= self.quorum()
        {
            self.do_view_change_quorum.set(true);
            actions.extend(self.complete_view_change_as_primary(plane));
        }

        actions
    }

    /// Handle a received `StartView` message (backups only).
    ///
    /// "When other replicas receive the STARTVIEW message, they replace their log
    /// with the one in the message, set their op-number to that of the latest entry
    /// in the log, set their view-number to the view number in the message, change
    /// their status to normal, and send `PrepareOK` for any uncommitted ops."
    ///
    /// # Panics
    /// If `header.namespace` does not match this replica's namespace.
    /// # Client-table maintenance
    ///
    /// Backups maintain the client-table during normal operation via
    /// `commit_journal` in `on_replicate`, which walks the WAL and updates
    /// the client table for each committed op. The WAL survives view changes,
    /// so the new primary can process any committed op it received.
    ///
    /// Gap: if a backup never received a prepare (lost message),
    /// `commit_journal` stops at the gap. Requires message repair.
    pub fn handle_start_view(&self, plane: PlaneKind, header: &StartViewHeader) -> Vec<VsrAction> {
        assert_eq!(header.namespace, self.namespace, "SV routed to wrong group");
        let from_replica = header.replica;
        let msg_view = header.view;
        let msg_op = header.op;
        let msg_commit = header.commit;

        // Verify sender is the primary for this view
        if self.primary_index(msg_view) != from_replica {
            return Vec::new();
        }

        // Ignore old views
        if msg_view < self.view.get() {
            return Vec::new();
        }

        // Skip equal-view SV with old op.
        // Already in this view; re-running reset_view_change_state would
        // cancel subscribers (waking register awaiters Canceled) and clear
        // pipeline for nothing. log_view (not self.view) tracks last-normal view.
        if msg_view == self.log_view.get() && msg_op < self.sequencer.current_sequence() {
            return Vec::new();
        }

        // We shouldn't process our own StartView
        if from_replica == self.replica {
            return Vec::new();
        }

        // Accept the StartView and transition to normal
        self.view.set(msg_view);
        self.log_view.set(msg_view);
        self.status.set(Status::Normal);
        self.advance_commit_max(msg_commit);
        self.reset_view_change_state();

        // Stale pipeline entries from the old view must be discarded
        self.pipeline.borrow_mut().clear();

        // TODO: StartView should carry uncommitted headers so backup installs
        // into WAL and sets op WAL-verified. Today we trust msg_op, correct
        // for truncation (sequencer > msg_op) but wrong when behind
        // (sequencer < msg_op): gap is unreachable without message repair.
        self.sequencer.set_sequence(msg_op);

        // Update timeouts for normal backup operation
        {
            let mut timeouts = self.timeouts.borrow_mut();
            timeouts.stop(TimeoutKind::ViewChangeStatus);
            timeouts.stop(TimeoutKind::DoViewChangeMessage);
            timeouts.stop(TimeoutKind::RequestStartViewMessage);
            timeouts.start(TimeoutKind::NormalHeartbeat);
        }

        // Send PrepareOK for uncommitted ops that we actually have in the WAL.
        // The caller must verify each op exists before sending.
        emit_replica_event(
            SimEventKind::ReplicaStateChanged,
            &ReplicaLogContext::from_consensus(self, plane),
        );

        // CommitJournal so backup applies inherited ops to client_table now,
        // mirroring `complete_view_change_as_primary`. Without this, the
        // table lags until the next Commit heartbeat / Prepare, a
        // promoted-resigned-re-elected primary running register_preflight
        // in that window observes incomplete state.
        let mut actions = Vec::new();
        actions.push(VsrAction::CommitJournal);

        if msg_commit < msg_op {
            let send_prepare_ok = VsrAction::SendPrepareOk {
                view: msg_view,
                from_op: msg_commit + 1,
                to_op: msg_op,
                target: from_replica,
                namespace: self.namespace,
            };
            emit_sim_event(
                SimEventKind::ControlMessageScheduled,
                &ControlActionLogEvent::from_vsr_action(
                    ReplicaLogContext::from_consensus(self, plane),
                    &send_prepare_ok,
                ),
            );
            actions.push(send_prepare_ok);
        }
        actions
    }

    /// Handle a `Commit` (heartbeat) message from the primary.
    ///
    /// Advances `commit_max` and resets the backup's `NormalHeartbeat` timeout
    /// so it doesn't start a spurious view change. Returns `true` if
    /// `commit_max` advanced, signalling the caller to run `commit_journal`.
    ///
    /// Only accepts heartbeats with a strictly newer monotonic timestamp
    /// to prevent old/replayed messages from suppressing view changes.
    ///
    /// # Panics
    /// If `header.namespace` does not match this replica's namespace.
    pub fn handle_commit(&self, header: &iggy_binary_protocol::CommitHeader) -> bool {
        assert_eq!(
            header.namespace, self.namespace,
            "Commit routed to wrong group"
        );

        if self.is_primary() {
            return false;
        }

        if self.status.get() != Status::Normal {
            return false;
        }

        if header.view != self.view.get() {
            return false;
        }

        // TODO: Once connection-level peer verification is added promote
        // this to an assert, the network layer would guarantee the sender
        // matches header.replica.
        if header.replica != self.primary_index(header.view) {
            return false;
        }

        // Only accept heartbeats with a strictly newer timestamp to prevent
        // old/replayed commit messages from resetting the timeout.
        if self.heartbeat_timestamp.get() < header.timestamp_monotonic {
            self.heartbeat_timestamp.set(header.timestamp_monotonic);
            self.timeouts
                .borrow_mut()
                .reset(TimeoutKind::NormalHeartbeat);
        }

        let old_commit_max = self.commit_max.get();
        self.advance_commit_max(header.commit);
        self.commit_max.get() > old_commit_max
    }

    /// Complete view change as the new primary after collecting DVC quorum.
    ///
    /// # Client-table maintenance
    ///
    /// Backups populate the client-table during normal operation via
    /// `commit_journal` in `on_replicate`. The WAL survives view changes, so
    /// when this replica transitions from backup to primary, its table
    /// contains entries for all committed ops it received.
    ///
    /// Gap: missing prepares (lost messages) require message repair.
    fn complete_view_change_as_primary(&self, plane: PlaneKind) -> Vec<VsrAction> {
        let dvc_array = self.do_view_change_from_all_replicas.borrow();

        let Some(winner) = dvc_select_winner(&dvc_array) else {
            return Vec::new();
        };

        let new_op = winner.op;
        let max_commit = dvc_max_commit(&dvc_array);

        // Update state
        self.log_view.set(self.view.get());
        self.status.set(Status::Normal);
        self.advance_commit_max(max_commit);
        self.sequencer.set_sequence(new_op);

        // Stale pipeline entries are invalid in new view; reconciliation
        // replays from journal.
        //
        // Cancel BEFORE clear: relying on Sender::Drop is correct today
        // (drop → Canceled), but a future refactor that moves senders
        // out-of-band could silently lose the wake-up. Explicit cancel
        // pins the contract.
        {
            let mut pipeline = self.pipeline.borrow_mut();
            pipeline.cancel_all_subscribers();
            pipeline.clear();
        }
        // Stale PrepareOk messages from the old view must not leak into the new view.
        // `reset_view_change_state` handles this for view-number advances (SVC/DVC/SV),
        // but this path fires within the current view after DVC quorum -- so we clear
        // the loopback queue directly.
        self.loopback_queue.borrow_mut().clear();

        // Update timeouts for normal primary operation
        {
            let mut timeouts = self.timeouts.borrow_mut();
            timeouts.stop(TimeoutKind::ViewChangeStatus);
            timeouts.stop(TimeoutKind::DoViewChangeMessage);
            timeouts.stop(TimeoutKind::StartViewChangeMessage);
            timeouts.start(TimeoutKind::CommitMessage);
            // If there are uncommitted ops in the rebuilt pipeline, start the
            // Prepare timeout so that lost PrepareOks trigger retransmission.
            if max_commit < new_op {
                timeouts.start(TimeoutKind::Prepare);
            }
        }

        let state = ReplicaLogContext::from_consensus(self, plane);
        emit_replica_event(SimEventKind::PrimaryElected, &state);
        emit_replica_event(SimEventKind::ReplicaStateChanged, &state);

        let action = VsrAction::SendStartView {
            view: self.view.get(),
            op: new_op,
            commit: max_commit,
            namespace: self.namespace,
        };
        emit_sim_event(
            SimEventKind::ControlMessageScheduled,
            &ControlActionLogEvent::from_vsr_action(
                ReplicaLogContext::from_consensus(self, plane),
                &action,
            ),
        );

        let mut actions = vec![action];
        // Catch up commit_min to commit_max before rebuilding the pipeline.
        // Without this, a behind backup (commit_min < max_commit) that becomes
        // primary would have unapplied committed ops.
        actions.push(VsrAction::CommitJournal);
        // The new primary must rebuild its pipeline from the journal so that
        // incoming PrepareOk messages can be matched and commits can proceed.
        if max_commit < new_op {
            assert!(
                (new_op - max_commit) <= PIPELINE_PREPARE_QUEUE_MAX as u64,
                "view change: uncommitted range {}..={} ({} ops) exceeds pipeline capacity ({}); \
                 DVC winner claims more in-flight ops than the pipeline can hold",
                max_commit + 1,
                new_op,
                new_op - max_commit,
                PIPELINE_PREPARE_QUEUE_MAX,
            );
            actions.push(VsrAction::RebuildPipeline {
                from_op: max_commit + 1,
                to_op: new_op,
            });
        }
        actions
    }

    /// Handle a `PrepareOk` message from a replica.
    ///
    /// Returns rich ack-progress information for structured logging.
    /// Caller (`on_ack`) should validate `is_primary` and status before calling.
    ///
    /// # Panics
    /// - If `header.command` is not `Command2::PrepareOk`.
    /// - If `header.replica >= self.replica_count`.
    pub fn handle_prepare_ok(
        &self,
        plane: PlaneKind,
        header: &PrepareOkHeader,
    ) -> PrepareOkOutcome {
        assert_eq!(header.command, Command2::PrepareOk);
        assert!(
            header.replica < self.replica_count,
            "handle_prepare_ok: invalid replica {}",
            header.replica
        );

        // Ignore if from older view
        if header.view < self.view() {
            return PrepareOkOutcome::Ignored {
                reason: IgnoreReason::OlderView,
            };
        }

        // Ignore if from newer view
        if header.view > self.view() {
            return PrepareOkOutcome::Ignored {
                reason: IgnoreReason::NewerView,
            };
        }

        // Ignore if syncing
        if self.is_syncing() {
            return PrepareOkOutcome::Ignored {
                reason: IgnoreReason::Syncing,
            };
        }

        // Find the prepare in our pipeline
        let mut pipeline = self.pipeline.borrow_mut();

        let Some(entry) = pipeline.entry_by_op_mut(header.op) else {
            // Not in pipeline - could be old/duplicate or already committed
            return PrepareOkOutcome::Ignored {
                reason: IgnoreReason::UnknownPrepare,
            };
        };

        // Verify checksum matches
        if entry.header.checksum != header.prepare_checksum {
            return PrepareOkOutcome::Ignored {
                reason: IgnoreReason::ChecksumMismatch,
            };
        }

        // Check for duplicate ack
        if entry.has_ack(header.replica) {
            return PrepareOkOutcome::Ignored {
                reason: IgnoreReason::DuplicateAck,
            };
        }

        // Record the ack from this replica
        let ack_count = entry.add_ack(header.replica);
        let quorum = self.quorum();
        let quorum_reached = ack_count >= quorum && !entry.ok_quorum_received;

        // Check if we've reached quorum
        if quorum_reached {
            entry.ok_quorum_received = true;
        }

        drop(pipeline);

        emit_sim_event(
            SimEventKind::PrepareAcked,
            &AckLogEvent {
                replica: ReplicaLogContext::from_consensus(self, plane),
                op: header.op,
                prepare_checksum: header.prepare_checksum,
                ack_from_replica: header.replica,
                ack_count,
                quorum,
                quorum_reached,
            },
        );

        PrepareOkOutcome::Accepted {
            ack_count,
            quorum_reached,
        }
    }

    /// Enqueue a self-addressed message for processing in the next loopback drain.
    ///
    /// Currently only `PrepareOk` messages are routed here (via `send_or_loopback`).
    // TODO: Route SVC/DVC self-messages through loopback once VsrAction dispatch is implemented.
    pub(crate) fn push_loopback(&self, message: Message<GenericHeader>) {
        assert!(
            self.loopback_queue.borrow().len() < PIPELINE_PREPARE_QUEUE_MAX,
            "loopback queue overflow: {} items",
            self.loopback_queue.borrow().len()
        );
        self.loopback_queue.borrow_mut().push_back(message);
    }

    /// Drain all pending loopback messages into `buf`, leaving the queue empty.
    ///
    /// The caller must dispatch each drained message to the appropriate handler.
    pub fn drain_loopback_into(&self, buf: &mut Vec<Message<GenericHeader>>) {
        buf.extend(self.loopback_queue.borrow_mut().drain(..));
    }

    /// Send a message to `target`, routing self-addressed messages through the loopback queue.
    // VsrConsensus uses Cell/RefCell for single-threaded compio shards; futures are intentionally !Send.
    #[allow(clippy::future_not_send)]
    pub(crate) async fn send_or_loopback(&self, target: u8, message: Message<GenericHeader>)
    where
        B: MessageBus,
    {
        if target == self.replica {
            self.push_loopback(message);
        } else if let Err(e) = self
            .message_bus
            .send_to_replica(target, message.into_frozen())
            .await
        {
            tracing::warn!(
                replica = self.replica,
                target,
                "send_or_loopback failed: {e}"
            );
        }
    }

    #[must_use]
    pub const fn message_bus(&self) -> &B {
        &self.message_bus
    }
}

impl<B, P> Project<Message<PrepareHeader>, VsrConsensus<B, P>> for Message<RequestHeader>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    type Consensus = VsrConsensus<B, P>;

    fn project(self, consensus: &Self::Consensus) -> Message<PrepareHeader> {
        let op = consensus.sequencer.current_sequence() + 1;

        self.transmute_header(|old, new| {
            *new = PrepareHeader {
                cluster: consensus.cluster,
                size: old.size,
                view: consensus.view.get(),
                release: old.release,
                command: Command2::Prepare,
                replica: consensus.replica,
                client: old.client,
                parent: consensus.last_prepare_checksum(),
                request_checksum: old.request_checksum,
                request: old.request,
                commit: consensus.commit_max.get(),
                op,
                timestamp: 0, // 0 for now. Implement correct way to get timestamp later
                operation: old.operation,
                namespace: old.namespace,
                ..Default::default()
            }
        })
    }
}

impl<B, P> Project<Message<PrepareOkHeader>, VsrConsensus<B, P>> for Message<PrepareHeader>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    type Consensus = VsrConsensus<B, P>;

    fn project(self, consensus: &Self::Consensus) -> Message<PrepareOkHeader> {
        self.transmute_header(|old, new| {
            *new = PrepareOkHeader {
                command: Command2::PrepareOk,
                parent: old.parent,
                prepare_checksum: old.checksum,
                request: old.request,
                cluster: consensus.cluster,
                replica: consensus.replica,
                // It's important to use the view of the replica, not the received prepare!
                view: consensus.view.get(),
                op: old.op,
                commit: consensus.commit_max.get(),
                timestamp: old.timestamp,
                operation: old.operation,
                namespace: old.namespace,
                // PrepareOks are only header no body
                ..Default::default()
            };
        })
    }
}

impl<B, P> Consensus for VsrConsensus<B, P>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    type MessageBus = B;
    #[rustfmt::skip] // Scuffed formatter. TODO: Make the naming less ambiguous for `Message`.
    type Message<H> = Message<H> where H: ConsensusHeader;
    type RequestHeader = RequestHeader;
    type ReplicateHeader = PrepareHeader;
    type AckHeader = PrepareOkHeader;

    type Sequencer = LocalSequencer;
    type Pipeline = P;

    // The primary's self-ack is delivered via the loopback queue
    // (push_loopback / drain_loopback_into) rather than inline here,
    // so that WAL persistence can happen between pipeline insertion
    // and ack recording.
    fn pipeline_message(&self, plane: PlaneKind, message: &Self::Message<Self::ReplicateHeader>) {
        self.push_prepare_entry(plane, message, PipelineEntry::new(*message.header()));
    }

    fn verify_pipeline(&self) {
        let pipeline = self.pipeline.borrow();
        pipeline.verify();
    }

    fn is_follower(&self) -> bool {
        !self.is_primary()
    }

    fn is_normal(&self) -> bool {
        self.status() == Status::Normal
    }

    fn is_syncing(&self) -> bool {
        // TODO: for now return false. we have to add syncing related setup to VsrConsensus to make this work.
        false
    }
}

#[cfg(test)]
mod request_queue_tests {
    use super::*;
    use iggy_binary_protocol::{Command2, Operation};

    fn make_request(client: u128, request_num: u64) -> Message<RequestHeader> {
        let header_size = std::mem::size_of::<RequestHeader>();
        let mut msg = Message::<RequestHeader>::new(header_size);
        let header = bytemuck::checked::try_from_bytes_mut::<RequestHeader>(
            &mut msg.as_mut_slice()[..header_size],
        )
        .expect("zeroed bytes are valid");
        *header = RequestHeader {
            command: Command2::Request,
            client,
            session: 1,
            request: request_num,
            operation: Operation::SendMessages,
            ..RequestHeader::default()
        };
        msg
    }

    #[test]
    fn push_request_buffers_when_prepare_queue_full() {
        let mut pipeline = LocalPipeline::new();
        // Buffer one with empty prepare queue.
        let entry = RequestEntry::new(make_request(1, 1));
        pipeline.push_request(entry).expect("request queue empty");
        assert_eq!(pipeline.request_queue_len(), 1);
        assert!(!pipeline.request_queue_full());
        // Symmetric drain.
        let popped = pipeline.pop_request().expect("just-pushed entry");
        assert_eq!(popped.message.header().client, 1);
        assert_eq!(popped.message.header().request, 1);
        assert_eq!(pipeline.request_queue_len(), 0);
    }

    #[test]
    fn push_request_returns_err_when_queue_full() {
        let mut pipeline = LocalPipeline::new();
        for i in 0..PIPELINE_REQUEST_QUEUE_MAX {
            let entry = RequestEntry::new(make_request(i as u128 + 1, 1));
            pipeline
                .push_request(entry)
                .expect("under capacity must succeed");
        }
        assert!(pipeline.request_queue_full());

        // Over capacity: entry returned as Err.
        let overflow = RequestEntry::new(make_request(0xFFFF, 1));
        let err = pipeline
            .push_request(overflow)
            .expect_err("over capacity must reject");
        assert_eq!(err.message.header().client, 0xFFFF);
    }

    #[test]
    fn has_message_from_client_scans_both_queues() {
        let mut pipeline = LocalPipeline::new();

        // Push only into request queue.
        pipeline
            .push_request(RequestEntry::new(make_request(0xCAFE, 1)))
            .expect("request queue empty");

        // Both queues scanned.
        assert!(pipeline.has_message_from_client(0xCAFE));
        assert!(!pipeline.has_message_from_client(0xBEEF));
    }

    // pipeline.clear() must clear both queues, old-view buffered requests
    // must not leak into new view.
    #[test]
    fn clear_drops_both_queues() {
        let mut pipeline = LocalPipeline::new();
        pipeline
            .push_request(RequestEntry::new(make_request(1, 1)))
            .unwrap();
        pipeline
            .push_request(RequestEntry::new(make_request(2, 1)))
            .unwrap();
        assert_eq!(pipeline.request_queue_len(), 2);

        pipeline.clear();
        assert!(pipeline.request_queue_is_empty());
        assert!(pipeline.is_empty());
    }

    // View-change *reset* drops request_queue, preserves prepare_queue
    // for DVC log reconciliation. Wired in `reset_view_change_state` via
    // `cancel_all_subscribers` + `clear_request_queue`.
    #[test]
    fn clear_request_queue_drops_only_request_queue() {
        let mut pipeline = LocalPipeline::new();

        // Two requests buffered, one prepare in flight.
        pipeline
            .push_request(RequestEntry::new(make_request(1, 1)))
            .unwrap();
        pipeline
            .push_request(RequestEntry::new(make_request(2, 1)))
            .unwrap();
        let prepare_header = PrepareHeader {
            op: 7,
            ..PrepareHeader::default()
        };
        pipeline.push(PipelineEntry::new(prepare_header));
        assert_eq!(pipeline.request_queue_len(), 2);
        assert_eq!(pipeline.prepare_count(), 1);

        pipeline.clear_request_queue();

        assert!(
            pipeline.request_queue_is_empty(),
            "request queue must be drained at view-change reset"
        );
        assert_eq!(
            pipeline.prepare_count(),
            1,
            "prepare queue must survive view-change reset for DVC log reconciliation"
        );
        let head = pipeline
            .prepare_head()
            .expect("prepare must still be there");
        assert_eq!(head.header.op, 7);
    }

    // is_full() tracks ONLY prepare_queue, splits "backpressure" signal
    // from "drop the request" signal.
    #[test]
    fn is_full_tracks_only_prepare_queue() {
        let mut pipeline = LocalPipeline::new();
        // Full request queue must not flip is_full.
        for i in 0..PIPELINE_REQUEST_QUEUE_MAX {
            pipeline
                .push_request(RequestEntry::new(make_request(i as u128 + 1, 1)))
                .unwrap();
        }
        assert!(pipeline.request_queue_full());
        assert!(
            !pipeline.is_full(),
            "request queue full does not imply is_full"
        );
    }
}

#[cfg(test)]
mod pipeline_entry_tests {
    //! Pin `PipelineEntry::reply_sender` lifecycle relied on by metadata +
    //! partition commit handlers.
    //!
    //! Contract: commit caller takes sender after slot update, fires reply.
    //! Reverting to header-destructure (the original bug) would wake every
    //! subscriber `Canceled` even on happy path. Tests pin both halves.

    use super::*;
    use iggy_binary_protocol::{Command2, Message, ReplyHeader};

    fn make_reply(client: u128, request: u64) -> Message<ReplyHeader> {
        let header_size = std::mem::size_of::<ReplyHeader>();
        let mut msg = Message::<ReplyHeader>::new(header_size);
        let header = bytemuck::checked::try_from_bytes_mut::<ReplyHeader>(
            &mut msg.as_mut_slice()[..header_size],
        )
        .expect("zeroed bytes are valid");
        *header = ReplyHeader {
            command: Command2::Reply,
            client,
            request,
            ..ReplyHeader::default()
        };
        msg
    }

    /// Happy path: take sender, fire reply.
    #[test]
    fn with_subscriber_take_and_send_delivers_reply() {
        let header = PrepareHeader::default();
        let (mut entry, receiver) = PipelineEntry::with_subscriber(header);

        let sender = entry
            .take_reply_sender()
            .expect("with_subscriber entry must hold a sender");
        let reply = make_reply(0xCAFE, 7);
        sender.send(reply).ok();

        let delivered = futures::executor::block_on(receiver)
            .expect("receiver must resolve to the reply, not Canceled");
        assert_eq!(delivered.header().client, 0xCAFE);
        assert_eq!(delivered.header().request, 7);
    }

    /// Pre-fix bug: dropping entry without firing sender cancels receiver.
    /// What `for entry in drained { let header = entry.header; ... }` did.
    /// Regression marker for any refactor that loses the explicit fire.
    #[test]
    fn drop_entry_without_take_yields_canceled() {
        let header = PrepareHeader::default();
        let (entry, receiver) = PipelineEntry::with_subscriber(header);

        // Exactly what the buggy commit path did via destructure-with-`..`.
        drop(entry);

        let outcome = futures::executor::block_on(receiver);
        assert!(
            outcome.is_err(),
            "dropped sender must wake receiver Canceled (distinguishes \
             'consensus reset' from 'reply delivered')"
        );
    }

    /// `take_reply_sender` idempotent: later calls return `None`, no panic.
    #[test]
    fn take_reply_sender_is_idempotent() {
        let header = PrepareHeader::default();
        let (mut entry, _receiver) = PipelineEntry::with_subscriber(header);

        assert!(entry.take_reply_sender().is_some(), "first take wins");
        assert!(
            entry.take_reply_sender().is_none(),
            "subsequent takes return None"
        );
    }

    /// `new()` (no subscriber) → `take_reply_sender()` returns `None`.
    /// Commit handler's `if let Some(_) = ...` relies on this.
    #[test]
    fn new_entry_has_no_sender() {
        let header = PrepareHeader::default();
        let mut entry = PipelineEntry::new(header);
        assert!(entry.take_reply_sender().is_none());
    }
}
