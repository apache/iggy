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

use crate::client_table::ClientTable;
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
    PrepareOkHeader, RequestHeader, StartViewChangeHeader, StartViewHeader,
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
}

impl PipelineEntry {
    #[must_use]
    pub fn new(header: PrepareHeader) -> Self {
        Self {
            header,
            ok_from_replicas: BitSet::with_capacity(REPLICAS_MAX),
            ok_quorum_received: false,
        }
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

/// A request message waiting to be prepared.
pub struct RequestEntry {
    pub message: Message<RequestHeader>,
    /// Timestamp when the request was received (for ordering/timeout).
    pub received_at: i64, //TODO figure the correct way to do this
}

impl RequestEntry {
    #[must_use]
    pub const fn new(message: Message<RequestHeader>) -> Self {
        Self {
            message,
            received_at: 0, //TODO figure the correct way to do this
        }
    }
}

#[derive(Debug)]
pub struct LocalPipeline {
    /// Messages being prepared (uncommitted and being replicated).
    prepare_queue: VecDeque<PipelineEntry>,
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

    /// Returns true if prepare queue is full.
    #[must_use]
    pub fn is_full(&self) -> bool {
        self.prepare_queue_full()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.prepare_queue.is_empty()
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

    /// Search `prepare_queue` for a message from the given client.
    ///
    /// If there are multiple messages (possible in `prepare_queue` after view change),
    /// returns the latest one.
    #[must_use]
    pub fn has_message_from_client(&self, client: u128) -> bool {
        self.prepare_queue.iter().any(|p| p.header.client == client)
    }

    /// Verify pipeline invariants.
    ///
    /// # Panics
    /// If any invariant is violated.
    pub fn verify(&self) {
        // Check capacity limits
        assert!(self.prepare_queue.len() <= PIPELINE_PREPARE_QUEUE_MAX);

        // Verify prepare queue hash chain
        if let Some(head) = self.prepare_queue.front() {
            let mut expected_op = head.header.op;
            let mut expected_parent = head.header.parent;

            for entry in &self.prepare_queue {
                let header = &entry.header;

                assert_eq!(header.op, expected_op, "ops must be sequential");
                assert_eq!(header.parent, expected_parent, "must be hash-chained");

                expected_parent = header.checksum;
                expected_op += 1;
            }
        }
    }

    /// Clear prepare queue.
    pub fn clear(&mut self) {
        self.prepare_queue.clear();
    }
}

impl Pipeline for LocalPipeline {
    type Entry = PipelineEntry;

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

    /// VSR client-table for duplicate detection and reply caching.
    client_table: RefCell<ClientTable>,
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
            client_table: RefCell::new(ClientTable::new(CLIENTS_TABLE_MAX)),
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

    #[must_use]
    pub const fn client_table(&self) -> &RefCell<ClientTable> {
        &self.client_table
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

    /// Reset all view change state when transitioning to a new view.
    ///
    /// Clears the loopback queue: stale `PrepareOks` from the old view
    /// reference pipeline entries that no longer exist, so processing
    /// them would be a no-op (`handle_prepare_ok` ignores unknown ops).
    /// The primary does not require its own self-ack for quorum.
    pub(crate) fn reset_view_change_state(&self) {
        self.reset_svc_quorum();
        self.reset_dvc_quorum();
        self.sent_own_start_view_change.set(false);
        self.sent_own_do_view_change.set(false);
        self.loopback_queue.borrow_mut().clear();
        self.client_table.borrow_mut().clear_pending();
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

        // TODO: TigerBeetle's StartView message carries uncommitted op headers,
        // allowing the backup to install them into the WAL and set op to a
        // WAL-verified value. We don't carry headers yet, so we blindly trust
        // msg_op. This is correct for truncation (sequencer > msg_op) but wrong
        // when the backup is behind (sequencer < msg_op) — the gap between the
        // WAL and msg_op becomes unreachable without message repair. Fix by
        // either carrying headers in StartView or implementing message repair.
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

        if msg_commit < msg_op {
            let action = VsrAction::SendPrepareOk {
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
                    &action,
                ),
            );
            vec![action]
        } else {
            Vec::new()
        }
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
        // this to an assert — the network layer would guarantee the sender
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

        // Stale pipeline entries from the old view are invalid in the new view.
        // Log reconciliation replays from the journal, not the pipeline.
        self.pipeline.borrow_mut().clear();
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
        B: MessageBus<Replica = u8, Data = Message<GenericHeader>, Client = u128>,
    {
        if target == self.replica {
            self.push_loopback(message);
        } else {
            // TODO: Propagate send errors instead of panicking; requires bus error design.
            self.message_bus
                .send_to_replica(target, message)
                .await
                .unwrap();
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
        assert!(self.is_primary(), "only primary can pipeline messages");

        let mut pipeline = self.pipeline.borrow_mut();
        pipeline.push(PipelineEntry::new(*message.header()));
        let pipeline_depth = pipeline.len();
        drop(pipeline);

        let header = message.header();
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

        // Start the prepare timeout so the primary retransmits if backups
        // don't ack in time. It is only started (not reset) so that an
        // already-ticking timeout is not pushed out by every new request.
        let mut timeouts = self.timeouts.borrow_mut();
        if !timeouts.is_ticking(TimeoutKind::Prepare) {
            timeouts.start(TimeoutKind::Prepare);
        }
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
