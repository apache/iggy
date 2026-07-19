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
use clock::{Clock, IggySystemClock};
use iggy_binary_protocol::{
    Command2, ConsensusHeader, DoViewChangeHeader, GenericHeader, PrepareHeader, PrepareOkHeader,
    ReplyHeader, RequestHeader, RequestStartViewHeader, StartViewChangeHeader, StartViewHeader,
};
use iggy_common::IggyTimestamp;
use message_bus::IggyMessageBus;
use message_bus::MessageBus;
use server_common::Message;
use server_common::sharding::{IggyNamespace, METADATA_CONSENSUS_NAMESPACE};
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::rc::Rc;

/// Injected time source for primary-stamped prepare timestamps.
///
/// The consensus core never reads the wall clock directly,
/// so a deterministic host (the simulator) can substitute virtual time
/// and make prepare timestamps a pure function of the seed.
/// of the seed. Production defaults to [`IggySystemClock`] through
/// [`VsrConsensus::new`]; only tests and the simulator construct one
/// explicitly via [`VsrConsensus::with_clock`].
///
/// The clock is type-erased behind `Rc<dyn Clock>` deliberately, to avoid
/// threading a clock generic through every `VsrConsensus<B, P>` call site. The
/// one vtable dispatch it costs per stamp is negligible next to the WAL append
/// each prepare already performs.
#[derive(Clone)]
pub struct ConsensusClock(Rc<dyn Clock<Realtime = IggyTimestamp>>);

impl ConsensusClock {
    #[must_use]
    pub fn new(clock: Rc<dyn Clock<Realtime = IggyTimestamp>>) -> Self {
        Self(clock)
    }

    #[must_use]
    pub fn system() -> Self {
        Self(Rc::new(IggySystemClock))
    }

    fn realtime(&self) -> IggyTimestamp {
        self.0.realtime()
    }
}

impl std::fmt::Debug for ConsensusClock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ConsensusClock")
    }
}

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
///
/// Sized to absorb a synchronized client burst (e.g. the 20-way
/// concurrent-creation race tests across TCP/QUIC/WebSocket) without
/// `PipelineFull`-rejecting and disconnecting clients that cannot replay in
/// time. At depth 8 the QUIC burst wedges the metadata consensus even in
/// release. Stays well under the journal's `SLOT_COUNT` (1024) and the inbox
/// capacity headroom.
pub const PIPELINE_PREPARE_QUEUE_MAX: usize = 32;

/// Max accepted-but-not-yet-prepared requests buffered behind a full
/// prepare queue. Beyond this, requests drop and the client retries.
pub const PIPELINE_REQUEST_QUEUE_MAX: usize = 64;

/// Maximum number of replicas in a cluster.
pub const REPLICAS_MAX: usize = 32;

/// Unanswered `RequestStartView` probes tolerated before a recovering
/// replica gives up waiting for a settled primary and falls back to an
/// election (a full-cluster restart leaves nobody able to answer).
pub const PROBE_ATTEMPTS_MAX: u32 = 5;

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

    /// `true` iff the entry still owns a reply sender (in-process awaiter).
    /// Caller checks before [`Self::take_reply_sender`] so it can branch on
    /// the slot's network-vs-in-process role without consuming the sender.
    #[must_use]
    pub const fn has_reply_sender(&self) -> bool {
        self.reply_sender.is_some()
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
    /// Uncommitted prepares; cap [`Self::prepare_queue_max`].
    prepare_queue: VecDeque<PipelineEntry>,
    /// Requests awaiting a prepare slot; cap [`Self::request_queue_max`].
    request_queue: VecDeque<RequestEntry>,
    /// Depth bound for `prepare_queue`; [`PIPELINE_PREPARE_QUEUE_MAX`]
    /// unless the operator overrode it (`[metadata]` in the server-ng
    /// config).
    prepare_queue_max: usize,
    /// Depth bound for `request_queue`; [`PIPELINE_REQUEST_QUEUE_MAX`]
    /// unless overridden alongside `prepare_queue_max`.
    request_queue_max: usize,
}

impl Default for LocalPipeline {
    fn default() -> Self {
        Self::new()
    }
}

impl LocalPipeline {
    #[must_use]
    pub fn new() -> Self {
        Self::with_capacities(PIPELINE_PREPARE_QUEUE_MAX, PIPELINE_REQUEST_QUEUE_MAX)
    }

    /// Pipeline with operator-tuned queue depths.
    ///
    /// Callers wiring this from config must keep the journal's
    /// checkpoint margin >= `prepare_queue_max`: up to a full prepare
    /// queue of already-pipelined ops appends while a forced checkpoint
    /// runs, and the margin is what guarantees them journal room (see
    /// `SnapshotCoordinator` in `core/metadata`).
    ///
    /// # Panics
    /// If a depth is zero — a zero-depth pipeline can never admit an op.
    #[must_use]
    pub fn with_capacities(prepare_queue_max: usize, request_queue_max: usize) -> Self {
        assert!(
            prepare_queue_max > 0 && request_queue_max > 0,
            "pipeline queue depths must be non-zero \
             (prepare={prepare_queue_max}, request={request_queue_max})"
        );
        Self {
            prepare_queue: VecDeque::with_capacity(prepare_queue_max),
            request_queue: VecDeque::with_capacity(request_queue_max),
            prepare_queue_max,
            request_queue_max,
        }
    }

    #[must_use]
    pub fn prepare_count(&self) -> usize {
        self.prepare_queue.len()
    }

    #[must_use]
    pub fn prepare_queue_full(&self) -> bool {
        self.prepare_queue.len() >= self.prepare_queue_max
    }

    #[must_use]
    pub fn request_queue_len(&self) -> usize {
        self.request_queue.len()
    }

    #[must_use]
    pub fn request_queue_full(&self) -> bool {
        self.request_queue.len() >= self.request_queue_max
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

/// What a received `Commit` heartbeat did, so the caller knows whether to
/// drain the journal or correct a stale peer.
#[derive(Debug, Clone, Copy)]
pub enum CommitOutcome {
    /// Nothing to do: the heartbeat was absorbed (or ignored as stale /
    /// foreign / wrong-status) without moving `commit_max`.
    Accepted,
    /// `commit_max` advanced; run `commit_journal`.
    Advanced,
    /// A replica is still heartbeating an older view in which it was
    /// primary; this replica is the current view's primary and should
    /// broadcast `StartView` so the stale replica adopts the view.
    RespondStartView,
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
    /// Broadcast a `RequestStartView` probe (recovering replica asking for
    /// the current view's `StartView`; only that view's primary answers).
    /// Stamped with the prober's view so peers can fence stale duplicates
    /// out of the probed-primary election path.
    SendRequestStartView { view: u32, namespace: u64 },
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
    /// Commit point the recovered WAL suffix must re-reach before admitting
    /// client requests as primary (`0` = no recovered suffix pending).
    recovery_barrier: Cell<u64>,
    /// True while this replica declines the primaryship its (stale) recovered
    /// view assigns it (see `init_as_backup`). `is_primary()` is pure view
    /// math, so without this flag a restarted view-N primary would still pass
    /// the submit gate and advertise itself as the roster leader while never
    /// heartbeating. Cleared as soon as any view transition resolves the role
    /// legitimately (`StartView` adoption, DVC completion).
    ceded_primaryship: Cell<bool>,
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
    /// Consecutive unanswered `RequestStartView` probes while Recovering;
    /// at [`PROBE_ATTEMPTS_MAX`] the replica falls back to an election.
    probe_attempts: Cell<u32>,

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

    /// Time source for [`Self::next_monotonic_timestamp`]; see
    /// [`ConsensusClock`].
    clock: ConsensusClock,
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
        Self::with_clock(
            cluster,
            replica,
            replica_count,
            namespace,
            message_bus,
            pipeline,
            ConsensusClock::system(),
        )
    }

    /// [`Self::new`] with an explicit time source. Simulator and clock
    /// tests only; production wiring stays on the system-clock default.
    ///
    /// # Panics
    /// - If `replica >= replica_count`.
    /// - If `replica_count < 1`.
    pub fn with_clock(
        cluster: u128,
        replica: u8,
        replica_count: u8,
        namespace: u64,
        message_bus: B,
        pipeline: P,
        clock: ConsensusClock,
    ) -> Self {
        assert!(
            replica < replica_count,
            "replica index must be < replica_count"
        );
        assert!(replica_count >= 1, "need at least 1 replica");
        // Consensus-control routing distinguishes metadata frames from
        // partition frames by namespace value: metadata uses the sentinel,
        // partitions use `IggyNamespace::inner()` which lives strictly
        // inside the packed range. A namespace outside both ranges would
        // route to neither and silently warn-drop on every receiving peer.
        debug_assert!(
            namespace == METADATA_CONSENSUS_NAMESPACE || IggyNamespace::is_packable(namespace),
            "VsrConsensus namespace must be METADATA_CONSENSUS_NAMESPACE or a packable \
             IggyNamespace; got {namespace:#x}"
        );
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
            recovery_barrier: Cell::new(0),
            ceded_primaryship: Cell::new(false),
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
            probe_attempts: Cell::new(0),
            do_view_change_from_all_replicas: RefCell::new(dvc_quorum_array_empty()),
            do_view_change_quorum: Cell::new(false),
            sent_own_start_view_change: Cell::new(false),
            sent_own_do_view_change: Cell::new(false),
            timeouts: RefCell::new(TimeoutManager::new(timeout_seed)),
            heartbeat_timestamp: Cell::new(0),
            clock,
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

    /// Initialize a restarted replica as a backup regardless of what its
    /// recovered view says about primaryship. A resumed stale primary races
    /// the peers' election: if they moved on (or move on now), two nodes act
    /// primary for different planes and clients route to the wrong one. Join
    /// as a backup instead; either the peers' heartbeat timeout elects a
    /// primary and its `StartView` brings this replica forward, or this
    /// replica's own silence provokes that election. Unlike
    /// [`Self::init_recovering`] the local journal is intact, so the normal
    /// commit walk applies it -- no commit-floor fast-forward.
    pub fn init_as_backup(&self) {
        self.status.set(Status::Normal);
        self.ceded_primaryship.set(true);
        self.timeouts
            .borrow_mut()
            .start(TimeoutKind::NormalHeartbeat);
    }

    /// See the `ceded_primaryship` field.
    #[must_use]
    pub const fn has_ceded_primaryship(&self) -> bool {
        self.ceded_primaryship.get()
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
            // A prepare just committed. Re-arm the prepare-retransmit timer for
            // the next-oldest pending prepare rather than letting it inherit the
            // previous op's grown backoff: the push-site `start` is a no-op
            // while ticking and nothing else clears `attempts`, so under
            // sustained load the backoff ratchets up to 16x base and the tail
            // op's retransmit fires too rarely to recover a lost backup ack,
            // stalling commit. `start` (not `reset`) forces the timer ticking
            // at the base interval - `reset` alone leaves `ticking` untouched,
            // so a timer stopped earlier would be armed-but-dead and never
            // fire. When nothing is pending the timer self-stops via the
            // empty-pipeline branch in `handle_prepare_timeout`.
            if self.sequencer.current_sequence() > commit {
                self.timeouts.borrow_mut().start(TimeoutKind::Prepare);
            }
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

    /// Restore local commit progress from already-applied state during bootstrap.
    ///
    /// Unlike `advance_commit_min`, this is intended for recovery paths where the
    /// state machine has already been restored up to the supplied commit point.
    ///
    /// # Panics
    /// - If `commit_min > commit_max`.
    /// - If commit progress has already been initialized on this consensus instance.
    pub fn restore_commit_state(&self, commit_min: u64, commit_max: u64) {
        assert!(
            commit_min <= commit_max,
            "commit_min ({commit_min}) must be <= commit_max ({commit_max})"
        );
        assert_eq!(
            self.commit_min.get(),
            0,
            "restore_commit_state must only be used on a fresh consensus instance"
        );
        assert_eq!(
            self.commit_max.get(),
            0,
            "restore_commit_state must only be used on a fresh consensus instance"
        );
        self.commit_max.set(commit_max);
        self.commit_min.set(commit_min);
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

    /// Commit point the recovered WAL suffix must re-reach before this
    /// replica (as primary) admits new client requests; `0` when no suffix
    /// was re-pipelined. See `is_caught_up_primary`.
    pub const fn recovery_barrier(&self) -> u64 {
        self.recovery_barrier.get()
    }

    pub fn set_recovery_barrier(&self, required_commit: u64) {
        self.recovery_barrier.set(required_commit);
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
        // backup-only for the same reason: re-setting on primary would rewind
        // past a sibling prepare pipelined during the append await.
        self.sequencer.set_sequence(header.op);
        self.set_last_prepare_checksum(header.checksum);
        self.observe_prepare_timestamp(header.timestamp);

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

    /// Primary-stamped prepare timestamp, strictly greater than every value
    /// this primary has stamped or observed (see
    /// [`Self::observe_prepare_timestamp`]). Monotonicity guards `created_at`
    /// ordering against an NTP step backwards.
    ///
    /// Lower bound only. The upper bound (clamping the read to a Marzullo
    /// interval over a peer-clock quorum, and abdicating if the primary cannot
    /// synchronize) is not enforced: Iggy has no peer-clock sync, so a runaway
    /// primary clock can stamp a far-future value. Backups lift their floor to it on observe,
    /// before commit, so even a view-change-truncated prepare poisons
    /// `created_at` cluster-wide and survives view changes. Consensus safety is
    /// unaffected (all replicas agree on the value); only wall-clock-derived
    /// semantics (retention, PAT expiry) skew. An upper bound is unenforceable
    /// at observe time (a backup clamping by its local clock would diverge from
    /// peers); the only sound site is mint, pending the unbuilt peer-clock
    /// subsystem.
    pub fn next_monotonic_timestamp(&self) -> u64 {
        let now = self.clock.realtime().as_micros();
        let prev = self.last_timestamp.get();
        let next = now.max(prev.saturating_add(1));
        // Strict monotonicity, except at prev == u64::MAX (saturating add
        // sticks, stamp repeats): reachable only via a malformed peer stamp
        // (none rejected yet) or year ~586_524. Debug-only; release never panics.
        debug_assert!(
            next > prev,
            "prepare timestamp not strictly monotonic (prev at u64::MAX?): prev={prev} next={next}"
        );
        self.last_timestamp.set(next);
        next
    }

    /// Read-only clock read (microseconds since the Unix epoch). Unlike
    /// [`Self::next_monotonic_timestamp`] it does not advance the floor:
    /// snapshots stamp `created_at` from the same seed-derived clock so a
    /// replayed seed reproduces identical bytes, without consuming the
    /// monotonic sequence.
    #[must_use]
    pub fn clock_realtime_micros(&self) -> u64 {
        self.clock.realtime().as_micros()
    }

    /// Lift the monotonic floor to a prepare timestamp observed from the log:
    /// backups per replicated prepare, recovery for the restored head, pipeline
    /// rebuilds per entry. Without it the floor is per-primary in-memory state,
    /// so a new primary whose clock lags its predecessor would stamp below
    /// committed entries after a view change. Monotone max-merge: idempotent,
    /// order-independent.
    ///
    /// Observed at append (in `on_replicate`), before commit, so a truncated
    /// prepare still raises the floor: the cluster-wide `created_at` blast
    /// radius noted on [`Self::next_monotonic_timestamp`]. Deliberate: observing
    /// at assignment is the conservative floor, and the runaway-clock fix is the
    /// upper peer-clock (Marzullo) window, not narrowing this to the commit
    /// path. Revisit only together.
    pub fn observe_prepare_timestamp(&self, timestamp: u64) {
        if timestamp > self.last_timestamp.get() {
            self.last_timestamp.set(timestamp);
        }
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
    pub fn tick(&self, plane: PlaneKind) -> Vec<VsrAction> {
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
            actions.extend(self.handle_do_view_change_message_timeout(plane));
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

        if timeouts.fired(TimeoutKind::RequestStartViewMessage) {
            drop(timeouts);
            // Two probers share this timeout, both asking "resend me the
            // current StartView":
            // - Recovering (boot probe): re-broadcast until the settled
            //   primary answers or an election's StartView adopts us.
            // - ViewChange backup: the election may have concluded with our
            //   copy of the StartView lost; re-requesting it is a
            //   two-message fix, while the ViewChangeStatus escalation
            //   backstop burns a fresh cluster-wide election. The would-be
            //   primary of the view skips the probe (it concludes the view
            //   itself or escalates).
            match self.status.get() {
                Status::Recovering => {
                    // A probe answered by nobody, repeatedly, means nobody is
                    // settled -- the whole cluster restarted together and
                    // every group sits quorum-invisible waiting for a primary
                    // that cannot exist. Fall back to an election: recovered
                    // WALs compete on (log_view, op) in the DVC exchange, so
                    // the best surviving log leads; a group whose members all
                    // rejoined journal-less elects on equal terms and stands
                    // on its recovered durable state. Any still-live settled
                    // primary answers well before the fallback fires.
                    let attempts = self.probe_attempts.get() + 1;
                    self.probe_attempts.set(attempts);
                    if attempts >= PROBE_ATTEMPTS_MAX {
                        self.finish_view_probe();
                        actions.extend(
                            self.start_election(plane, ViewChangeReason::ViewProbeUnanswered),
                        );
                    } else {
                        self.timeouts
                            .borrow_mut()
                            .reset(TimeoutKind::RequestStartViewMessage);
                        actions.push(VsrAction::SendRequestStartView {
                            view: self.view.get(),
                            namespace: self.namespace,
                        });
                    }
                }
                Status::ViewChange if self.primary_index(self.view.get()) != self.replica => {
                    self.timeouts
                        .borrow_mut()
                        .reset(TimeoutKind::RequestStartViewMessage);
                    actions.push(VsrAction::SendRequestStartView {
                        view: self.view.get(),
                        namespace: self.namespace,
                    });
                }
                _ => {
                    // Stale arm (e.g. went Normal without passing an exit
                    // that stops it): silence it instead of refiring every
                    // tick.
                    self.timeouts
                        .borrow_mut()
                        .stop(TimeoutKind::RequestStartViewMessage);
                }
            }
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
        // A recovering replica makes progress through RequestStartView
        // retries, not elections; it is quorum-invisible.
        if self.status.get() == Status::Recovering {
            return Vec::new();
        }

        // Only backups trigger view change on heartbeat timeout. `is_primary`
        // is pure view math though: a replica that booted recovering / with
        // ceded primaryship while sitting at the primary index is a backup by
        // role -- if it early-returned here it would neither heartbeat nor
        // start an election, silently dropping out of quorum until an
        // unrelated view change rescues it. Let it climb StartViewChange like
        // any other backup.
        if self.is_primary() && !self.ceded_primaryship.get() {
            return Vec::new();
        }

        // Already in view change
        if self.status.get() == Status::ViewChange {
            return Vec::new();
        }

        self.start_election(plane, ViewChangeReason::NormalHeartbeatTimeout)
    }

    /// Advance to `view + 1` and start a view change (own SVC counted).
    fn start_election(&self, plane: PlaneKind, reason: ViewChangeReason) -> Vec<VsrAction> {
        let old_view = self.view.get();
        let new_view = old_view + 1;

        self.view.set(new_view);
        self.status.set(Status::ViewChange);
        self.reset_view_change_state();
        self.sent_own_start_view_change.set(true);
        self.start_view_change_from_all_replicas
            .borrow_mut()
            .insert(self.replica as usize);

        {
            let mut timeouts = self.timeouts.borrow_mut();
            timeouts.stop(TimeoutKind::NormalHeartbeat);
            timeouts.start(TimeoutKind::StartViewChangeMessage);
            timeouts.start(TimeoutKind::ViewChangeStatus);
            timeouts.start(TimeoutKind::RequestStartViewMessage);
        }

        emit_sim_event(
            SimEventKind::ViewChangeStarted,
            &ViewChangeLogEvent {
                replica: ReplicaLogContext::from_consensus(self, plane),
                old_view,
                new_view,
                reason,
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
    fn handle_do_view_change_message_timeout(&self, plane: PlaneKind) -> Vec<VsrAction> {
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

        let current_op = self.sequencer.current_sequence();
        let action = VsrAction::SendDoViewChange {
            view: self.view.get(),
            target: self.primary_index(self.view.get()),
            log_view: self.log_view.get(),
            op: current_op,
            // commit_max clamped to op: see `handle_start_view_change`.
            commit: self.commit_max.get().min(current_op),
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
        // TODO(prepare-timeout): tighten the timer lifecycle: disarm in
        // the ack path the moment quorum drains the pipeline and rearm
        // for the next-oldest prepare when one commits with others still
        // pending, giving the invariant "ticking iff pipeline non-empty"
        // and a timeout that always measures the current oldest
        // prepare's age. Ours arms once per idle->busy transition and
        // disarms lazily below, so a prepare pushed late into an armed
        // window can be retransmitted before it is `PREPARE_TICKS` old.
        // Also worth special-casing "all remote acks present, own
        // journal write is the laggard" by retrying the local write
        // instead of retransmitting.
        //
        // Every early return below must stop or back off the timeout.
        // `fired()` stays true until the timer is rearmed, so returning
        // with the fired state intact turns the next pipeline push into
        // an instant spurious retransmit on the following tick (the push
        // sees `is_ticking` and does not restart the timer).
        if !self.is_primary() || self.status.get() != Status::Normal {
            self.timeouts.borrow_mut().stop(TimeoutKind::Prepare);
            return Vec::new();
        }

        if self.pipeline.borrow().is_empty() {
            // Everything committed before the timeout fired; the next
            // push restarts the timer from zero.
            self.timeouts.borrow_mut().stop(TimeoutKind::Prepare);
            return Vec::new();
        }

        let targets = self.retransmit_targets();
        if targets.is_empty() {
            // In-flight ops all have their acks; re-check after backoff.
            self.timeouts.borrow_mut().backoff(TimeoutKind::Prepare);
            return Vec::new();
        }

        tracing::debug!(
            replica = self.replica,
            view = self.view.get(),
            targets = targets.len(),
            first_op = targets.first().map(|(h, _)| h.op),
            "prepare timeout: retransmitting un-acked prepares"
        );
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
        // A recovering replica is quorum-invisible: it lost (or cannot trust)
        // its durable state, so it must not vote history into existence. The
        // election proceeds among the peers; its conclusion reaches this
        // replica via StartView, which recovery accepts.
        if self.status.get() == Status::Recovering {
            return Vec::new();
        }
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
                timeouts.start(TimeoutKind::RequestStartViewMessage);
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
            // DVC carries commit_max (highest known-committed), not commit_min
            // (locally applied). The new primary floors its pipeline rebuild at
            // max(commit) across the quorum; only commit_max bounds that range
            // to pipeline depth (every replica holds op - commit_max <= depth).
            // commit_min can lag far behind and overflow the rebuild. The
            // committed-but-unapplied tail (commit_min..commit_max] is replayed
            // by the new primary's CommitJournal, not the pipeline.
            //
            // Clamp to op: a backup learns commit_max from a heartbeat before
            // receiving the prepares, so commit_max can exceed its op. The wire
            // contract `DoViewChangeHeader::validate` rejects commit > op and
            // drops such a DVC (view-change liveness stall). The clamp is
            // lossless for the rebuild floor: quorum intersection guarantees
            // some sender whose op covers the true commit point carries it, so
            // max(commit) across the quorum is unchanged.
            let commit = self.commit_max.get().min(current_op);

            // Start DVC timeout
            self.timeouts
                .borrow_mut()
                .start(TimeoutKind::DoViewChangeMessage);

            let action = VsrAction::SendDoViewChange {
                view: self.view.get(),
                target: primary_candidate,
                log_view: self.log_view.get(),
                op: current_op,
                commit,
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
                    commit,
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
        // Quorum-invisible while recovering (see handle_start_view_change):
        // a recovering replica must not collect DVCs and crown itself.
        if self.status.get() == Status::Recovering {
            return Vec::new();
        }
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
                timeouts.start(TimeoutKind::RequestStartViewMessage);
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
        // commit_max clamped to op: see `handle_start_view_change`.
        let commit = self.commit_max.get().min(current_op);

        // If we haven't sent our own DVC yet, record it
        if !self.sent_own_do_view_change.get() {
            self.sent_own_do_view_change.set(true);

            let own_dvc = StoredDvc {
                replica: self.replica,
                log_view: self.log_view.get(),
                op: current_op,
                commit,
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

    /// Begin the view probe: broadcast `RequestStartView` and keep
    /// re-broadcasting on `TimeoutKind::RequestStartViewMessage` until the
    /// current view's primary answers with a targeted `StartView` (or an
    /// election's `StartView` adopts this replica first). The replica sits
    /// in `Status::Recovering` meanwhile: it acks nothing, votes in no
    /// election, and initiates nothing.
    /// Returns nothing: the first probe rides the
    /// `RequestStartViewMessage` timeout (~1s after boot), by which point
    /// the replica mesh -- absent entirely at the boot-time call sites --
    /// has formed. Emitting an action here implied a send that never
    /// happened.
    pub fn begin_view_probe(&self) {
        tracing::info!(
            replica = self.replica,
            namespace_raw = self.namespace,
            "beginning view probe"
        );
        self.status.set(Status::Recovering);
        self.probe_attempts.set(0);
        let mut timeouts = self.timeouts.borrow_mut();
        timeouts.stop(TimeoutKind::Prepare);
        timeouts.stop(TimeoutKind::CommitMessage);
        timeouts.stop(TimeoutKind::NormalHeartbeat);
        timeouts.start(TimeoutKind::RequestStartViewMessage);
    }

    /// Peer side of the probe (sent by a Recovering replica at boot, or by
    /// a `ViewChange` backup whose copy of the concluding `StartView` was
    /// lost). Only the current view's PRIMARY answers, with a `StartView`;
    /// backups stay silent and the prober retries. Special case: a probe
    /// FROM the replica that is the current view's primary-by-index proves
    /// that primary cannot lead (a probing replica has either lost its
    /// state or abandoned the view), so a peer receiving it elects
    /// immediately instead of waiting out the heartbeat timeout on a slot
    /// known to be dead.
    ///
    /// # Panics
    /// Panics when the probe is routed to the wrong group.
    pub fn handle_request_start_view(
        &self,
        plane: PlaneKind,
        header: &RequestStartViewHeader,
    ) -> Vec<VsrAction> {
        assert_eq!(
            header.namespace, self.namespace,
            "RequestStartView routed to wrong group"
        );
        if self.status.get() != Status::Normal {
            return Vec::new();
        }
        if header.replica == self.replica {
            return Vec::new();
        }
        if self.primary_index(self.view.get()) == header.replica {
            // Probes are re-broadcast on a timer, so delayed duplicates are
            // the normal case, and `primary_index` is view % replica_count:
            // a stale probe from replica R re-matches every replica_count
            // views. Only a probe stamped with the CURRENT view proves the
            // current primary is the one probing; anything else falls
            // through (a backup answers nothing, and the true primary of a
            // newer view answers with its StartView).
            if header.view == self.view.get() {
                return self.start_election(plane, ViewChangeReason::PrimaryProbedView);
            }
            return Vec::new();
        }
        if !self.is_primary() || self.ceded_primaryship.get() {
            return Vec::new();
        }
        // A primary mid-transition (log_view lagging) has no settled
        // frontier to publish yet.
        if self.log_view.get() != self.view.get() {
            return Vec::new();
        }
        vec![VsrAction::SendStartView {
            view: self.view.get(),
            op: self.sequencer.current_sequence(),
            commit: self.commit_max.get(),
            namespace: self.namespace,
        }]
    }

    /// Set the commit floor after journal repair filled `(floor, commit_max]`:
    /// everything at or below `floor` is represented by this replica's
    /// recovered durable state (segments + offset files), proven by the
    /// serving peer answering `RangeEvicted { retained_from = floor + 1 }`.
    /// Unlike the retired first-commit fast-forward, ops in the repair window
    /// are journaled and WALKED, never skipped.
    ///
    /// # Panics
    /// Panics when `floor` would rewind the already-executed `commit_min`.
    pub fn set_commit_floor(&self, floor: u64) {
        let current = self.commit_min.get();
        assert!(
            current <= floor,
            "commit floor {floor} may not rewind commit_min {current}"
        );
        self.commit_min.set(floor);
    }

    fn finish_view_probe(&self) {
        self.probe_attempts.set(0);
        self.timeouts
            .borrow_mut()
            .stop(TimeoutKind::RequestStartViewMessage);
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
        tracing::info!(
            replica = self.replica,
            old_view = self.view.get(),
            new_view = msg_view,
            op = msg_op,
            commit = msg_commit,
            "adopting view from StartView"
        );
        // A StartView concluding around an in-flight view probe supersedes
        // it: the new primary's numbers are at least as fresh as any probe
        // answer.
        self.finish_view_probe();
        self.view.set(msg_view);
        self.log_view.set(msg_view);
        self.status.set(Status::Normal);
        self.ceded_primaryship.set(false);
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
    pub fn handle_commit(&self, header: &iggy_binary_protocol::CommitHeader) -> CommitOutcome {
        assert_eq!(
            header.namespace, self.namespace,
            "Commit routed to wrong group"
        );

        if self.is_primary() {
            // A heartbeat from the primary of an OLDER view means that
            // replica missed our view change entirely -- typically it
            // restarted while the view advanced and recovered the stale
            // view from its journal (there is no durable view watermark),
            // so the SVC/DVC/SV exchange never reached it. Left alone it
            // wedges: it drops our newer-view traffic as foreign and we
            // drop its stale prepares, while its live heartbeats keep its
            // backups from electing anyone. Re-announcing the current view
            // lets its `handle_start_view` adopt the view and cancel its
            // stale pipeline.
            if self.status.get() == Status::Normal
                && header.view < self.view.get()
                && header.replica == self.primary_index(header.view)
            {
                return CommitOutcome::RespondStartView;
            }
            return CommitOutcome::Accepted;
        }

        if self.status.get() != Status::Normal {
            return CommitOutcome::Accepted;
        }

        if header.view != self.view.get() {
            return CommitOutcome::Accepted;
        }

        // TODO: Once connection-level peer verification is added promote
        // this to an assert, the network layer would guarantee the sender
        // matches header.replica.
        if header.replica != self.primary_index(header.view) {
            return CommitOutcome::Accepted;
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
        if self.commit_max.get() > old_commit_max {
            CommitOutcome::Advanced
        } else {
            CommitOutcome::Accepted
        }
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
        self.ceded_primaryship.set(false);
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
            timeouts.stop(TimeoutKind::RequestStartViewMessage);
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
        // Primary stamps the injected clock once at prepare-build (wall time
        // in production, virtual under the simulator); the value is
        // replicated to every backup so apply() reads the same timestamp
        // across the cluster (deterministic state-machine apply). Monotonic
        // wrapper guards against NTP rewinds; see
        // `VsrConsensus::next_monotonic_timestamp`.
        let timestamp = consensus.next_monotonic_timestamp();

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
                timestamp,
                operation: old.operation,
                namespace: old.namespace,
                // Copied verbatim: carries the stamped acting user for client
                // ops (and the authenticated user on Register), so the in-apply
                // RBAC gate resolves the same identity on every backup.
                user_id: old.user_id,
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

    #[allow(clippy::cast_possible_truncation)]
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
                // PrepareOk is header-only; the frame is exactly the header, so
                // `size` is the header size.
                size: std::mem::size_of::<PrepareOkHeader>() as u32,
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
    use iggy_binary_protocol::{Command2, ReplyHeader};
    use server_common::Message;

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

#[cfg(test)]
mod timestamp_clamp_tests {
    //! Pin the monotonic-floor contract: a new primary must never stamp a
    //! prepare below timestamps already in the replicated log, even when its
    //! wall clock lags the predecessor's.

    use super::*;
    use crate::LocalPipeline;
    use server_common::MESSAGE_ALIGN;
    use server_common::iobuf::Frozen;

    /// Clock frozen at a fixed instant, standing in for a lagging wall
    /// clock on a freshly elected primary.
    struct FixedClock(u64);

    impl clock::Clock for FixedClock {
        type Realtime = IggyTimestamp;

        fn realtime(&self) -> Self::Realtime {
            IggyTimestamp::from(self.0)
        }
    }

    struct NoopBus;

    impl MessageBus for NoopBus {
        async fn send_to_client(
            &self,
            _client_id: u128,
            _data: Frozen<MESSAGE_ALIGN>,
        ) -> Result<(), message_bus::SendError> {
            Ok(())
        }

        async fn send_to_replica(
            &self,
            _replica: u8,
            _data: Frozen<MESSAGE_ALIGN>,
        ) -> Result<(), message_bus::SendError> {
            Ok(())
        }

        fn set_connection_lost_fn(&self, _f: message_bus::ConnectionLostFn) {}
        fn set_replica_forward_fn(&self, _f: message_bus::ReplicaForwardFn) {}
        fn set_client_forward_fn(&self, _f: message_bus::ClientForwardFn) {}
        fn track_background(&self, _handle: message_bus::JoinHandle<()>) {}
    }

    #[test]
    fn observed_log_timestamp_floors_new_primary_stamps() {
        let lagging_clock = ConsensusClock::new(Rc::new(FixedClock(1_000)));
        let consensus = VsrConsensus::with_clock(
            1,
            0,
            1,
            METADATA_CONSENSUS_NAMESPACE,
            NoopBus,
            LocalPipeline::new(),
            lagging_clock,
        );

        // Predecessor primary (fast wall clock) committed up to T=50_000;
        // this replica ingests that head via replication / rebuild.
        consensus.observe_prepare_timestamp(50_000);

        let stamped = consensus.next_monotonic_timestamp();
        assert!(
            stamped > 50_000,
            "stamp {stamped} regressed below the observed log head"
        );

        // Own stamps stay strictly monotonic on top of the lifted floor.
        let second = consensus.next_monotonic_timestamp();
        assert!(second > stamped);

        // Observing an OLDER timestamp never rewinds the floor.
        consensus.observe_prepare_timestamp(10);
        assert!(consensus.next_monotonic_timestamp() > second);
    }

    #[test]
    fn wall_clock_ahead_of_log_still_wins() {
        let leading_clock = ConsensusClock::new(Rc::new(FixedClock(100_000)));
        let consensus = VsrConsensus::with_clock(
            1,
            0,
            1,
            METADATA_CONSENSUS_NAMESPACE,
            NoopBus,
            LocalPipeline::new(),
            leading_clock,
        );
        consensus.observe_prepare_timestamp(50_000);
        assert_eq!(
            consensus.next_monotonic_timestamp(),
            100_000,
            "a clock ahead of the log must stamp real time, not floor + 1"
        );
    }
}
