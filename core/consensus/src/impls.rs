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
    AckLogEvent, Consensus, ControlActionLogEvent, DvcHeaderKind, DvcQuorumArray, DvcSuffix,
    HistoryCertificate, HistoryRecovery, IgnoreReason, MergeOutcome, MergeQuorums, MergedLog,
    Pipeline, PlaneKind, PrepareLogEvent, Project, RepairEvidence, ReplicaLogContext, SimEventKind,
    StoredDvc, ViewChangeLogEvent, ViewChangeReason, VsrState, dvc_count, dvc_header_kind,
    dvc_iter, dvc_quorum_array_empty, dvc_record, dvc_reset, dvc_suffix_decode, emit_replica_event,
    emit_sim_event, merge_view_change, seal_prepare_checksum,
};
use bit_set::BitSet;
use clock::{Clock, IggySystemClock};
use iggy_binary_protocol::{
    CHECKSUM_UNSEALED, Command, ConsensusHeader, DoViewChangeHeader, GenericHeader, Operation,
    PrepareHeader, PrepareOkHeader, ReplyHeader, RequestStartViewHeader, RoutedRequestHeader,
    StartViewChangeHeader, StartViewHeader, frame_body,
};
use iggy_common::IggyTimestamp;
use iggy_common::calculate_checksum;
use message_bus::IggyMessageBus;
use message_bus::MessageBus;
use server_common::Message;
use server_common::poll::AutoCommitReservation;
use server_common::sharding::{IggyNamespace, METADATA_GROUP};
use std::cell::{Cell, RefCell};
use std::collections::{HashSet, VecDeque};
use std::num::NonZeroU64;
use std::rc::Rc;
use std::time::Duration;

#[derive(Clone)]
pub struct LoopbackNotifier(Rc<dyn Fn()>);

impl LoopbackNotifier {
    pub fn new(notify: impl Fn() + 'static) -> Self {
        Self(Rc::new(notify))
    }

    fn notify(&self) {
        self.0();
    }
}

impl std::fmt::Debug for LoopbackNotifier {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("LoopbackNotifier")
    }
}

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

#[cfg(test)]
pub struct FixedClock(pub u64);

#[cfg(test)]
impl Clock for FixedClock {
    type Realtime = IggyTimestamp;

    fn realtime(&self) -> Self::Realtime {
        IggyTimestamp::from(self.0)
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

/// Default in-flight prepare-queue depth.
///
/// [`LocalPipeline::new`] uses it, and the server config default
/// (`DEFAULT_METADATA_PREPARE_QUEUE_DEPTH`) is static-asserted equal to it at
/// bootstrap. Operators raise the running bound via `[metadata]
/// prepare_queue_depth`; the pipeline then carries its own capacity (see
/// [`LocalPipeline::with_capacities`]).
///
/// Sized to absorb a synchronized client burst (the 20-way concurrent-creation
/// race tests across TCP/QUIC/WebSocket) without `PipelineFull`-rejecting
/// clients that cannot replay in time; a depth of 8 wedged the QUIC burst even
/// in release. Stays well under the journal's slot count and the inbox headroom.
pub const PIPELINE_PREPARE_QUEUE_MAX: usize = 32;

/// Max accepted-but-not-yet-prepared requests buffered behind a full
/// prepare queue. Beyond this, requests drop and the client retries.
pub const PIPELINE_REQUEST_QUEUE_MAX: usize = 64;

/// Maximum number of replicas in a cluster.
pub const REPLICAS_MAX: usize = 32;

/// Ceiling on [`VsrConsensus::quorum_replication`].
///
/// Past three acks marginal durability is small and every extra ack sits on the
/// commit path, so wide clusters spend the difference on the view-change quorum.
pub const QUORUM_REPLICATION_MAX: usize = 3;

/// Headers a `DoViewChange` may carry, and so the widest uncommitted suffix a
/// view change can reason about.
///
/// Pinned by the wire: `DoViewChangeHeader`'s nack and present bitsets are one
/// `u128` each, one bit per entry. The suffix spans `commit_max..=op`, bounded by
/// `prepare_queue_max`, so capping that depth here keeps every suffix addressable.
pub const DVC_HEADERS_MAX: usize = 128;

/// Deepest prepare queue any node in the cluster may be configured with.
///
/// One less than [`DVC_HEADERS_MAX`]: the suffix spans `commit..=op` and the head
/// needs the reserved slot. Config ceilings and [`LocalPipeline::with_capacities`]
/// both enforce it, so it holds for a peer as well as for this node.
pub const PREPARE_QUEUE_CEILING: usize = DVC_HEADERS_MAX - 1;

/// Unanswered `RequestStartView` probes tolerated before a recovering
/// replica gives up waiting for a settled primary and falls back to an
/// election (a full-cluster restart leaves nobody able to answer).
pub const PROBE_ATTEMPTS_MAX: u32 = 5;

/// Maximum number of clients tracked in the clients table.
/// New sessions are refused at capacity; ended sessions retain protection until retirement.
pub const CLIENTS_TABLE_MAX: usize = 8192;

/// Default live dedup entries per PARTITION consensus group.
///
/// Far below [`CLIENTS_TABLE_MAX`] because this budget is per group rather than
/// per node: the worst case scales with partition count, so it is sized to the
/// producers one partition sees. Pinned against the
/// `[partition] dedup_clients_max` default by a bootstrap assert.
pub const PARTITION_DEDUP_CLIENTS_MAX: usize = 4096;

/// Admission order on one partition owner. It is never replicated or recovered.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct LocalRequestOrder(pub NonZeroU64);

#[derive(Debug)]
pub struct PipelineEntry {
    pub header: PrepareHeader,
    pub local_order: Option<LocalRequestOrder>,
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
            local_order: None,
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
        (Self::with_sender(header, sender), receiver)
    }

    /// Entry adopting an existing reply sender — used when a request that
    /// carried a subscriber through the request queue is promoted into a
    /// prepare slot, so the original in-process awaiter keeps its receiver.
    #[must_use]
    pub fn with_sender(header: PrepareHeader, sender: Sender<Message<ReplyHeader>>) -> Self {
        Self {
            header,
            local_order: None,
            ok_from_replicas: BitSet::with_capacity(REPLICAS_MAX),
            ok_quorum_received: false,
            reply_sender: Some(sender),
        }
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

/// Capacity held by a pending automatic commit.
#[derive(Debug)]
pub struct AutoCommitRequestContext {
    /// Keeps this request's consumer key occupied through promotion and staging.
    pub reservation: AutoCommitReservation,
}

/// Accepted request waiting in `request_queue` for a prepare slot.
#[derive(Debug)]
pub struct RequestEntry {
    pub local_order: Option<LocalRequestOrder>,
    /// Automatic commit context owned by this entry until promotion or removal.
    auto_commit: Option<AutoCommitRequestContext>,
    pub message: Message<RoutedRequestHeader>,
    /// When the request was parked, in microseconds from the consensus-injected
    /// clock ([`VsrConsensus::clock_realtime_micros`]). `0` until
    /// [`VsrConsensus::push_queued_request`] stamps it, which is the only path
    /// that parks an entry in production.
    ///
    /// Read through [`Self::queue_wait_micros`] at promotion, never by subtracting
    /// directly: the queue wait is what age-based shedding filters on and the
    /// queueing half of end-to-end commit latency.
    ///
    /// Deliberately the plain clock read rather than
    /// [`VsrConsensus::next_monotonic_timestamp`]: this must not consume the
    /// prepare-stamping monotonic sequence, or parking a request would perturb
    /// the timestamps replicated to every backup.
    pub received_at: u64,
    /// In-process reply subscriber, carried through the queue so promotion
    /// can hand it to the pipeline entry (see [`PipelineEntry::with_sender`]).
    /// `None` = network path. Dropping a queued entry (view-change reset,
    /// preflight rejection at promotion) wakes the receiver with `Canceled`.
    pub(crate) reply_sender: Option<Sender<Message<ReplyHeader>>>,
}

impl RequestEntry {
    /// Build an automatic commit entry that owns its capacity guard.
    /// The context must match the consumer key encoded in `message`.
    #[must_use]
    pub fn with_auto_commit(
        message: Message<RoutedRequestHeader>,
        context: AutoCommitRequestContext,
    ) -> Self {
        let mut entry = Self::new(message);
        entry.auto_commit = Some(context);
        entry
    }

    /// Transfer the context without dropping its reservation.
    /// Promotion must retain the returned context through replication staging.
    pub const fn take_auto_commit(&mut self) -> Option<AutoCommitRequestContext> {
        self.auto_commit.take()
    }

    /// Inspect the context without acquiring another reservation.
    #[must_use]
    pub const fn auto_commit(&self) -> Option<&AutoCommitRequestContext> {
        self.auto_commit.as_ref()
    }

    /// Queued request on the network reply path: no in-process subscriber.
    #[must_use]
    pub const fn new(message: Message<RoutedRequestHeader>) -> Self {
        Self::with_sender(message, None)
    }

    /// Queued request paired with a fresh receiver that resolves when the
    /// promoted prepare commits (`Err(Canceled)` if the entry is dropped
    /// first). The in-process absorption path: a submit that arrives while
    /// the primary is mid-commit or the prepare queue is full parks here
    /// instead of being bounced with a transient error.
    #[must_use]
    pub fn with_subscriber(
        message: Message<RoutedRequestHeader>,
    ) -> (Self, Receiver<Message<ReplyHeader>>) {
        let (sender, receiver) = oneshot::channel();
        (Self::with_sender(message, Some(sender)), receiver)
    }

    /// Queued request carrying a sender the caller already owns, for a submit
    /// that parked before reaching a prepare slot. `None` is the network reply
    /// path; the other two constructors are this one with a fixed sender.
    #[must_use]
    pub const fn with_sender(
        message: Message<RoutedRequestHeader>,
        reply_sender: Option<Sender<Message<ReplyHeader>>>,
    ) -> Self {
        Self {
            auto_commit: None,
            local_order: None,
            message,
            received_at: 0,
            reply_sender,
        }
    }

    /// Take the reply sender for hand-off to the promoted pipeline entry.
    pub const fn take_reply_sender(&mut self) -> Option<Sender<Message<ReplyHeader>>> {
        self.reply_sender.take()
    }

    /// How long this request has waited, against a realtime reading taken now
    /// (`VsrConsensus::clock_realtime_micros`).
    ///
    /// Saturating, which is why this exists rather than a documented subtraction:
    /// the realtime clock can step backwards between park and promotion, and on
    /// `u64` a plain `now - received_at` wraps to ~1.8e19 micros, an age a shed would
    /// act on. `0` for an unstamped entry.
    #[must_use]
    pub const fn queue_wait_micros(&self, now: u64) -> u64 {
        now.saturating_sub(self.received_at)
    }
}

impl<B, P> VsrConsensus<B, P>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry, Request = RequestEntry>,
{
    /// Park a request that could not take a prepare slot, stamping its arrival
    /// time so the promotion side can measure how long it waited.
    ///
    /// # Errors
    /// The entry itself, when the request queue is at its depth bound.
    pub fn push_queued_request(&self, mut entry: RequestEntry) -> Result<(), RequestEntry> {
        entry.received_at = self.clock_realtime_micros();
        self.pipeline.borrow_mut().push_request(entry)
    }
}

impl<B, P> VsrConsensus<B, P>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    #[must_use]
    pub fn retry_capacity(&self, table: &crate::ClientTable) -> usize {
        if table.capacity_committed() {
            return table.capacity();
        }
        self.pipeline
            .borrow()
            .head()
            .filter(|entry| entry.header.retry_capacity != 0)
            .map_or_else(
                || table.capacity(),
                |entry| entry.header.retry_capacity as usize,
            )
    }

    /// Both queues own reservations. Clearing or refusing an entry releases it
    /// automatically, and a commit replaces its reservation with live protection.
    #[must_use]
    pub fn has_retry_capacity(&self, table: &crate::ClientTable, client: u128) -> bool {
        if table.contains(client) || message_bus::is_auto_commit_client(client) {
            return true;
        }
        let capacity = self.retry_capacity(table);
        let pipeline = self.pipeline.borrow();
        // Queue lengths bound distinct reservations without scanning either queue.
        if pipeline.len() + pipeline.request_queue_len() < capacity.saturating_sub(table.count()) {
            return true;
        }
        let pending: HashSet<_> = pipeline
            .pending_client_ids()
            .filter(|candidate| {
                *candidate != 0
                    && !message_bus::is_auto_commit_client(*candidate)
                    && !table.contains(*candidate)
            })
            .collect();
        pending.contains(&client) || table.count() + pending.len() < capacity
    }
}

/// Outcome of [`VsrConsensus::rollback_pipelined_prepare`].
///
/// Only [`Self::Unwound`] mutates. Every refusal leaves the sequencer, parent
/// chain, and pipeline as it found them, so a caller can report one without
/// repairing anything.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrepareRollback {
    /// Sequencer, parent chain, and pipeline entry all restored. The op is free
    /// again and the next request reuses it.
    Unwound,
    /// Not primary now, so no pre-advance of this replica's own survives to undo.
    /// Either a backup that never pre-advanced (it advances only AFTER its append
    /// succeeds), or a demoted ex-primary whose claim the view-change reset already
    /// discarded.
    NotPreAdvanced,
    /// Refused: a view change ran while the append was parked, so the state this
    /// prepare pre-advanced is no longer its own. `view` is the current view.
    ///
    /// Still primary, and after a re-election the sequencer can sit at `header.op`
    /// again with a DIFFERENT prepare under it: `start_pending_view` rewinds to the
    /// merged head and the next request re-projects that number. Unwinding would pop
    /// a live entry, rewind beneath an op peers have journaled, and mint the op a
    /// third time. Hence the check ahead of the sequencer compare.
    Superseded { view: u32 },
    /// Refused: `sequence` no longer matches the failed op. Two directions, only
    /// the first with a sibling behind it, neither closable locally:
    ///
    /// - `> header.op`: a sibling is live and already chained to a prepare the WAL
    ///   will never hold, so unwinding would additionally hand out its op number.
    /// - `< header.op`: a state transfer or view change rewound beneath this op;
    ///   the pre-advance is simply gone.
    Overtaken { sequence: u64 },
    /// Refused: the sequencer matches the failed op but the pipeline tail is a
    /// different prepare. The two move together everywhere else, so an invariant has
    /// already broken; release refuses rather than rewinding on numbers it just
    /// proved it cannot trust.
    TailMismatch,
}

/// Two-queue pipeline: in-flight prepares + buffered requests.
#[derive(Debug)]
pub struct LocalPipeline {
    /// Uncommitted prepares; cap [`Self::prepare_queue_max`].
    prepare_queue: VecDeque<PipelineEntry>,
    /// Requests awaiting a prepare slot; cap [`Self::request_queue_max`].
    request_queue: VecDeque<RequestEntry>,
    /// Depth bound for `prepare_queue`; [`PIPELINE_PREPARE_QUEUE_MAX`]
    /// unless the operator overrode it (`[metadata]` in the server
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
    /// If a depth is zero, or if the prepare depth would let the uncommitted
    /// suffix outgrow what a `DoViewChange` can address.
    #[must_use]
    pub fn with_capacities(prepare_queue_max: usize, request_queue_max: usize) -> Self {
        assert!(
            prepare_queue_max > 0 && request_queue_max > 0,
            "pipeline queue depths must be non-zero \
             (prepare={prepare_queue_max}, request={request_queue_max})"
        );
        // Each `DoViewChange` bitset addresses one suffix entry with one bit of a
        // `u128`, and the suffix spans `commit..=op`. Deeper, and the builder clamps
        // its window from below, leaving undecidable ops and a stalled view change.
        // Config ceilings also enforce this; a stall is worth a loud boot.
        assert!(
            prepare_queue_max < DVC_HEADERS_MAX,
            "prepare queue depth {prepare_queue_max} would produce an uncommitted suffix wider \
             than a DoViewChange can address (max {})",
            DVC_HEADERS_MAX - 1,
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
    pub const fn request_queue_capacity(&self) -> usize {
        self.request_queue_max
    }

    /// Rebuilt entries have no local marker and precede new local requests.
    #[must_use]
    pub fn has_request_before(&self, order: LocalRequestOrder) -> bool {
        self.prepare_queue
            .iter()
            .any(|entry| entry.local_order.is_none_or(|admitted| admitted < order))
            || self
                .request_queue
                .iter()
                .any(|entry| entry.local_order.is_none_or(|admitted| admitted < order))
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

    pub fn pending_requests(&self) -> impl Iterator<Item = &RequestEntry> {
        self.request_queue.iter()
    }

    #[must_use]
    pub fn request_head(&self) -> Option<&RequestEntry> {
        self.request_queue.front()
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
            // No view ordering: `view` is outside the prepare identity and a
            // retransmit restamps it, so a chain rebuilt from the journal can step
            // down a view between two linked ops.
            assert_eq!(
                header.parent, tail_header.checksum,
                "parent must chain to previous checksum"
            );
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

    /// Drop the newest prepare when it is `op`, returning it.
    ///
    /// `None` (and no mutation) when the tail is a different op. The queue holds
    /// a consecutive run, so removing anything but the tail would punch a hole in
    /// it and break every `message_by_op` index computation; a caller whose op is
    /// no longer the tail has been overtaken and must not unwind.
    ///
    /// The one caller is the journal-append rollback
    /// ([`VsrConsensus::rollback_pipelined_prepare`]).
    pub fn remove_prepare_tail(&mut self, op: u64, checksum: u128) -> Option<PipelineEntry> {
        let tail = self.prepare_queue.back()?;
        if tail.header.op != op || tail.header.checksum != checksum {
            return None;
        }
        self.prepare_queue.pop_back()
    }

    /// Find a message by op number and checksum (immutable).
    // op - head_op is bounded by the configured prepare-queue depth; index always fits in usize.
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
    // op - head_op is bounded by the configured prepare-queue depth; index always fits in usize.
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
    // op - head_op is bounded by the configured prepare-queue depth; index always fits in usize.
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
        assert!(self.prepare_queue.len() <= self.prepare_queue_max);
        assert!(self.request_queue.len() <= self.request_queue_max);

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

    /// Remove pending requests while preserving the order of those retained.
    /// Removed entries drop their reply senders and automatic commit reservations.
    /// Prepare entries are unaffected.
    pub fn retain_requests(&mut self, mut keep: impl FnMut(&RequestEntry) -> bool) {
        self.request_queue.retain(|request| keep(request));
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
    fn pending_request(
        &self,
        client_id: u128,
    ) -> Option<(u64, u64, iggy_binary_protocol::Operation)> {
        self.prepare_queue
            .iter()
            .find(|entry| entry.header.client == client_id)
            .map(|entry| {
                (
                    entry.header.session,
                    entry.header.request,
                    entry.header.operation,
                )
            })
            .or_else(|| {
                self.request_queue
                    .iter()
                    .find(|entry| entry.message.header().client == client_id)
                    .map(|entry| {
                        let header = entry.message.header();
                        (header.session, header.request, header.operation)
                    })
            })
    }
    type Entry = PipelineEntry;
    type Request = RequestEntry;

    fn pending_client_ids(&self) -> impl Iterator<Item = u128> {
        self.prepare_queue
            .iter()
            .map(|entry| entry.header.client)
            .chain(
                self.request_queue
                    .iter()
                    .map(|entry| entry.message.header().client),
            )
    }

    fn push(&mut self, entry: Self::Entry) {
        Self::push(self, entry);
    }

    fn pop(&mut self) -> Option<Self::Entry> {
        Self::pop_message(self)
    }

    fn remove_tail(&mut self, op: u64, checksum: u128) -> Option<Self::Entry> {
        Self::remove_prepare_tail(self, op, checksum)
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

    fn prepare_queue_max(&self) -> usize {
        self.prepare_queue_max
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

    fn request_queue_len(&self) -> usize {
        Self::request_queue_len(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Normal,
    ViewChange,
    Recovering,
}

/// State-transfer progress, orthogonal to [`Status`].
///
/// A transferring replica stays a normal protocol participant (it adopts
/// views, votes, answers probes) but withholds `PrepareOk` and ignores live
/// prepares (`replicate_preflight` / `send_prepare_ok` gate on
/// [`Consensus::is_transferring`]) -- it must not vouch for state it is
/// about to replace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StateTransferStage {
    /// No transfer in progress.
    Idle,
    /// Restarted into a cluster: waiting for the view probe to find a live
    /// primary to fetch from. The probe-exhausted election fallback proves
    /// full-cluster bootstrap instead and abandons the transfer (local
    /// recovery is then authoritative).
    AwaitingTarget,
    /// Target descriptor accepted; pulling artifact chunks.
    Fetching,
    /// Artifacts verified; installing.
    Installing,
}

impl StateTransferStage {
    /// Legal stage transitions; [`VsrConsensus::set_state_transfer_stage`]
    /// asserts against this so a mis-sequenced handler fails loudly instead
    /// of corrupting the install.
    #[must_use]
    pub const fn valid_transition(from: Self, to: Self) -> bool {
        matches!(
            (from, to),
            (Self::Idle, Self::AwaitingTarget)
                | (Self::AwaitingTarget, Self::Fetching | Self::Idle)
                | (
                    Self::Fetching,
                    Self::Installing | Self::AwaitingTarget | Self::Idle
                )
                | (Self::Installing, Self::Idle)
        )
    }
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
    SendStartViewChange { view: u32, group: u64 },
    /// Send `DoViewChange` to primary.
    SendDoViewChange {
        view: u32,
        target: u8,
        log_view: u32,
        op: u64,
        commit: u64,
        group: u64,
        /// The sender's uncommitted suffix, snapshotted for this view. Carried on
        /// the action rather than re-read by the dispatcher so the wire bytes match
        /// this replica's own `StoredDvc`: a merge seeing two versions of one
        /// sender's suffix could adopt a header no replica holds.
        suffix: DvcSuffix,
        /// Stamped as `DoViewChangeHeader::FLAG_UNTRUSTED_LOG`.
        untrusted_log: bool,
    },
    /// Broadcast a `RequestStartView` probe (recovering replica asking for
    /// the current view's `StartView`; only that view's primary answers).
    /// Stamped with the prober's view so peers can fence stale duplicates
    /// out of the probed-primary election path.
    SendRequestStartView { view: u32, group: u64 },
    /// Send `StartView`, as the view's primary.
    ///
    /// `incarnation` echoes the requester's nonce when this answers a
    /// `RequestStartView` probe (freshness proof), and is `0` on an unsolicited
    /// send at view-change completion (carries no freshness claim).
    ///
    /// `target` is the probing replica for an echo and `None` for an unsolicited
    /// broadcast. An echo must not be broadcast: the nonce it carries is one
    /// replica's freshness proof, and every other peer that is itself recovering
    /// reads a foreign nonce and rejects an otherwise current `StartView`.
    SendStartView {
        view: u32,
        op: u64,
        commit: u64,
        incarnation: u128,
        target: Option<u8>,
        group: u64,
        /// The view's suffix, high-to-low op from `op` down toward `commit`.
        ///
        /// Lets a backup check the head it is told to adopt against real headers,
        /// and gives it canonical checksums to verify repaired bodies against.
        /// Empty on the probe-answer path, where the primary reports its own
        /// frontier rather than concluding a view change; the backup trusts `op`.
        suffix: Vec<PrepareHeader>,
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
        group: u64,
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
    /// Send a commit point to `target`, or to every backup when `None`.
    ///
    /// The `CommitMessage` heartbeat goes to every backup and keeps them from
    /// starting a view change during idle periods. An eager `Commit` goes only
    /// to a backup known to hold the op it announces.
    SendCommit {
        view: u32,
        commit: u64,
        group: u64,
        timestamp_monotonic: u64,
        target: Option<u8>,
    },
}

impl VsrAction {
    /// Whether this is the log-less `DoViewChange` a recovering replica barred
    /// from electing sends. It vouches for no history, so a partition awaiting
    /// a state transfer may still send it.
    #[must_use]
    pub fn is_empty_vote(&self) -> bool {
        matches!(
            self,
            Self::SendDoViewChange {
                log_view: 0,
                op: 0,
                commit: 0,
                untrusted_log: true,
                suffix,
                ..
            } if suffix.is_empty()
        )
    }
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

/// What the primary of the current view knows of each replica's log and has
/// told it of the commit point, so an eager `Commit` reaches a backup only once
/// that backup holds the op it announces.
///
/// Chain replication forwards a prepare to the last backups after the quorum
/// commits it. A `Commit` sent to every backup at once overtakes the prepare
/// there, and the backup repairs an op that is still on its way to it.
#[derive(Debug)]
struct CommitAnnouncements {
    /// Commit point the planes asked the backups to learn without waiting for
    /// the heartbeat; see [`VsrConsensus::request_eager_commit`].
    eager: u64,
    /// Indexed by replica id. Reset on every view change.
    replicas: Vec<ReplicaCommitProgress>,
}

#[derive(Debug, Clone, Copy, Default)]
struct ReplicaCommitProgress {
    /// Highest op the replica acknowledged in this view.
    acked: u64,
    /// Highest commit point sent to the replica in this view.
    sent: u64,
}

impl ReplicaCommitProgress {
    /// Whether the replica acked an op up to `eager` that no `Commit` sent to
    /// it covers.
    fn owes(self, eager: u64) -> bool {
        self.sent < eager.min(self.acked)
    }
}

impl CommitAnnouncements {
    fn new(replica_count: u8) -> Self {
        Self {
            eager: 0,
            replicas: vec![ReplicaCommitProgress::default(); usize::from(replica_count)],
        }
    }

    fn reset(&mut self) {
        self.eager = 0;
        self.replicas.fill(ReplicaCommitProgress::default());
    }

    fn record_ack(&mut self, replica: u8, op: u64) {
        if let Some(progress) = self.replicas.get_mut(usize::from(replica)) {
            progress.acked = progress.acked.max(op);
        }
    }

    fn record_broadcast(&mut self, commit: u64) {
        for progress in &mut self.replicas {
            progress.sent = progress.sent.max(commit);
        }
    }

    fn owes(&self, replica: u8) -> bool {
        self.replicas
            .get(usize::from(replica))
            .is_some_and(|progress| progress.owes(self.eager))
    }

    /// The commit point to send `replica` if it is owed one, recorded as sent.
    ///
    /// Clamped to the replica's acks, so the `Commit` names no op it lacks.
    fn take_owed(&mut self, replica: u8, commit_min: u64) -> Option<u64> {
        let eager = self.eager;
        let progress = self
            .replicas
            .get_mut(usize::from(replica))
            .filter(|progress| progress.owes(eager))?;
        progress.sent = commit_min.min(progress.acked);
        Some(progress.sent)
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
    group: u64,
    /// Metadata creation revision for a partition group; zero for metadata.
    partition_incarnation: Cell<u64>,

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
    /// The `view` last made durable in the superblock. `view_durable <= view`; a
    /// view-scoped message must not leave until they are equal, or a crash could
    /// recover an older view than one this replica already acted in (split brain).
    /// Advanced by [`Self::mark_superblock_durable`].
    view_durable: Cell<u32>,
    /// The `log_view` last made durable in the superblock. Realizes the
    /// long-standing invariant `log_view >= log_view_durable`.
    log_view_durable: Cell<u32>,
    /// Per-boot incarnation nonce, stamped on outbound `RequestStartView` probes
    /// and echoed in the answering `StartView`, so a recovering replica ignores a
    /// `StartView` from a previous incarnation still in flight (see the
    /// [`Self::handle_start_view`] recovering-status guard). `0` means unset
    /// (partition plane, tests), leaving the guard inert. Set by
    /// [`Self::set_incarnation`] at boot, before `init`.
    incarnation: Cell<u128>,
    /// Commit point the recovered WAL suffix must re-reach before admitting
    /// client requests as primary (`0` = no recovered suffix pending).
    recovery_barrier: Cell<u64>,
    /// Wall-clock budget the recovered suffix has to re-commit before a waiter
    /// on [`Self::recovery_barrier`] gives up. Armed together with the barrier;
    /// `ZERO` when no suffix is pending (barrier `0`), which no waiter reads.
    recovery_deadline: Cell<Duration>,
    /// Op head of the last view this replica started as primary, `0` before
    /// the first start. The merged log can hold ops above the merged commit
    /// that an older view committed and acknowledged, so a read of applied
    /// state misses them until `commit_min` reaches this head.
    view_start_head: Cell<u64>,
    /// True while this replica declines the primaryship its (stale) recovered
    /// view assigns it (see `init_as_backup`). `is_primary()` is pure view
    /// math, so without this flag a restarted view-N primary would still pass
    /// the submit gate and advertise itself as the roster leader while never
    /// heartbeating. Cleared as soon as any view transition resolves the role
    /// legitimately (`StartView` adoption, DVC completion).
    ceded_primaryship: Cell<bool>,
    status: Cell<Status>,
    /// See [`StateTransferStage`]. Set to `AwaitingTarget` by a cluster
    /// restart boot (`begin_state_transfer_await`), driven through
    /// `Fetching`/`Installing` by the shard's transfer session, cleared by
    /// the probe-exhausted election fallback (full-cluster bootstrap).
    state_transfer_stage: Cell<StateTransferStage>,

    /// Highest view seen on inbound traffic that this replica could not
    /// process because the view was ahead of its own (a dropped newer-view
    /// prepare or heartbeat). Proof the cluster elected past this replica
    /// rather than that the primary died, which is why the heartbeat-timeout
    /// handler probes to catch up instead of starting a futile election (see
    /// [`Self::handle_normal_heartbeat_timeout`]). Monotone `max`; a value at
    /// or below `view` is stale and inert because the catch-up guard is a
    /// strict `>`.
    observed_newer_view: Cell<u32>,

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
    /// Snapshot of the pipeline's in-flight prepare capacity, taken at
    /// construction. Bounds the loopback queue and the view-change rebuild
    /// range without re-borrowing `pipeline`.
    prepare_queue_max: usize,

    message_bus: B,
    loopback_queue: RefCell<VecDeque<Message<GenericHeader>>>,
    loopback_notifier: RefCell<Option<LoopbackNotifier>>,
    /// Tracks start view change messages received from all replicas (including self)
    start_view_change_from_all_replicas: RefCell<BitSet<u32>>,
    /// Consecutive unanswered `RequestStartView` probes while Recovering;
    /// at the `probe_attempts_max` ceiling the replica falls back to an
    /// election.
    probe_attempts: Cell<u32>,
    /// Probe-attempt ceiling backing the fall-back-to-election decision,
    /// seeded from [`PROBE_ATTEMPTS_MAX`] and overridable by the runtime via
    /// `[cluster] view_probe_attempts_max`. The simulator and tests keep the
    /// built-in default.
    probe_attempts_max: Cell<u32>,
    recovery_election_allowed: Cell<bool>,
    /// False when this replica may acknowledge a prepare before the write that
    /// recovery reads back reaches stable storage, so a restart can come back
    /// with a log shorter than what it acknowledged. See
    /// [`Self::set_recovered_log_complete`].
    recovered_log_complete: Cell<bool>,
    /// This incarnation booted on the log it read back from disk and has not
    /// been `Normal` since. Set by [`Self::join`] with
    /// [`JoinMode::ProbeAsBackup`], cleared on the first `Normal`.
    ///
    /// A view probe begun later, by a replica that never restarted, finds it
    /// clear: that log lost nothing, so an unanswered probe is no reason to
    /// distrust it, and doing so could halt a group one crash short of its
    /// fault budget.
    ///
    /// Adopting a `StartView` and starting a view as primary are the clears
    /// that matter. Those in [`Self::init`] and [`Self::init_as_backup`] are
    /// only a safeguard: each runs at boot, before anything but `join` sets
    /// the flag, and `join` sets it after `init_as_backup` returns.
    rejoined_on_recovered_log: Cell<bool>,
    /// This replica left `Recovering` through the probe-exhaustion election
    /// on a log read back after a restart (see
    /// [`Self::rejoined_on_recovered_log`]) while
    /// [`Self::recovered_log_complete`] was false: its log may lack ops it
    /// acknowledged, and no settled primary has vouched for it since.
    ///
    /// Every `DoViewChange` it sends says so, and no merge counts such a log
    /// toward a trusted quorum: a view starts from a quorum of trusted senders, or from
    /// every replica (see `merge_view_change`). Cleared by adopting a
    /// `StartView` or by starting a view as primary, whose merged log either
    /// rule makes safe.
    untrusted_log: Cell<bool>,
    /// View-change escalations since [`Self::untrusted_log`] was last set, for
    /// rate-limiting the halt warning.
    untrusted_view_change_timeouts: Cell<u32>,

    /// This replica's own uncommitted suffix, with the head, commit point and
    /// mutation count the journal was at when it was read.
    ///
    /// Installed by the shard via [`Self::set_local_dvc_suffix`] before any handler
    /// that could enter a view change. Snapshotted rather than recomputed per send
    /// so a retransmit is byte-identical: a nack is a durable claim about this
    /// replica's log, and silently retracting one lets the new primary assemble a
    /// quorum that never simultaneously existed.
    ///
    /// Tagged by `(op, commit, journal_mutations)`, not by view, because that is
    /// what the suffix describes: a view advance leaves the log alone so the
    /// snapshot survives, while anything moving the head, the commit point or the
    /// journal's contents makes the tag mismatch, which reads as no snapshot at
    /// all.
    local_dvc_suffix: RefCell<Option<(u64, u64, u64, DvcSuffix)>>,

    /// Counts mutations of the journal this replica's suffix is read from.
    ///
    /// `(op, commit)` alone says how far the log reaches, not what it holds. A
    /// backup that adopted a `StartView` sits at the announced head with the
    /// bodies still missing, so repair filling one moves neither number, and the
    /// header-only snapshot taken before it would keep going out. The merge reads
    /// a header no sender can serve as proof that sender never journaled the op,
    /// which is now a nack: a stale snapshot would nack an op this replica holds
    /// and acked. Bumped by every append, truncation, drain and eviction on both
    /// planes.
    journal_mutations: Cell<u64>,

    /// The log a DVC quorum settled on or an adopted `StartView` announced,
    /// parked until this replica's journal can serve all of it.
    ///
    /// Under `do_view_change_quorum` it means "primary-elect, repairing": decided
    /// but not started, so this replica prepares and announces nothing. Otherwise
    /// it is an adopted `StartView` suffix. Cleared by
    /// [`VsrConsensus::start_pending_view`], or by `reset_view_change_state`.
    pending_view_log: RefCell<Option<MergedLog>>,
    /// The log the last adopted `StartView` announced, for this replica's
    /// `DoViewChange` until its journal holds it or a newer view's log replaces
    /// it.
    ///
    /// The parked copy in `pending_view_log` goes at the next view change. Without
    /// this one, a backup that has not repaired the announced ops reads blanks
    /// from its journal in every later view change, and at the commit point a
    /// blank leaves the op undecidable when no other sender reports it. Above the
    /// commit point the merge counts the kept header as a nack, as it would the
    /// blank: the journal holds no copy of the op.
    adopted_view: RefCell<Option<crate::AdoptedView>>,
    adopted_view_dirty: Cell<bool>,
    /// The installed view must prove the local suffix before it can be applied.
    /// Survives election resets until the plane successfully reconciles its journal.
    view_log_unproven: Cell<bool>,
    /// Partition-plane claim while an adopted log is unproven. Metadata uses
    /// `history_certificate`, including its durable WAL binding, instead.
    log_view_proven: Cell<u32>,
    /// The proven journal is a primary-elect's merge while `log_view` still names
    /// an older view. Set when the proof settles, cleared when the view starts.
    /// If the merge cleared the WAL certificate of a partition, a superseding
    /// view change clears it too (`unprove_superseded_merge`).
    view_log_merged: Cell<bool>,
    /// The settled merge cleared the WAL certificate of `log_view`. Only the
    /// partition plane reports it. The proof settling resets it, and nothing
    /// reads it unless `view_log_merged` holds.
    view_log_merge_uncertified: Cell<bool>,
    /// The prefix below a repair gap is unknown; entries above it can be proven.
    view_log_unproven_through: Cell<u64>,
    /// Set only after adoption has removed the superseded tail.
    view_log_missing_op: Cell<Option<u64>>,
    /// METADATA plane: the newest view whose log the WAL holds, which lags
    /// `log_view` until the adopted log is proven. Its binding follows the WAL head
    /// while the view is proven, and only moves down during a rewrite.
    history_certificate: Cell<Option<HistoryCertificate>>,
    /// The certificate the superblock last recorded.
    history_certificate_durable: Cell<Option<HistoryCertificate>>,

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

    /// View and counter of the last emitted or accepted heartbeat. A new
    /// primary's counter can be lower, so freshness is scoped to its view.
    heartbeat_watermark: Cell<(u32, u64)>,
    /// The next tick looks for backups owed an eager `Commit`; see
    /// [`Self::request_eager_commit`].
    eager_commit: Cell<bool>,
    commit_announcements: RefCell<CommitAnnouncements>,
    /// Views whose logs were refused for naming a different prepare at an op
    /// this replica applied; see [`Self::contradicts_applied_log`].
    applied_log_contradictions: Cell<u64>,
    /// Part of `applied_log_contradictions` already handed to a metric.
    applied_log_contradictions_taken: Cell<u64>,
    /// Newest view counted in `applied_log_contradictions`.
    contradiction_reported_view: Cell<Option<u32>>,

    /// Time source for [`Self::next_monotonic_timestamp`]; see
    /// [`ConsensusClock`].
    clock: ConsensusClock,
}

/// What carried a log that contradicts an op this replica applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ContradictionSource {
    StartView,
    Prepare,
}

/// Why [`VsrConsensus::try_handle_start_view`] refused a `StartView`.
#[derive(Debug)]
pub enum StartViewRefusal {
    /// The body is malformed or names another cluster, group or partition
    /// incarnation.
    InvalidEvidence,
    /// The log names a different prepare at an op this replica applied. A plane
    /// that can rebuild the group from its peers reads it for the history both
    /// sides still share.
    ContradictsAppliedLog(MergedLog),
}

/// Boot-time timer set for a consensus group, one struct so every plane's
/// restore path applies the same values in the same place.
#[derive(Debug, Clone, Copy)]
pub struct ConsensusTimers {
    pub normal_heartbeat_ticks: u64,
    pub commit_message_ticks: u64,
    pub prepare_ticks: u64,
    pub view_change_retransmit_ticks: u64,
    pub view_change_status_ticks: u64,
    pub request_start_view_ticks: u64,
    pub probe_attempts_max: u32,
}

/// How a restored replica joins its group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinMode {
    /// Fresh group or solo replica: plain init; the group needs its view-0
    /// primary to exist.
    Init,
    /// Prior life detected: join quorum-invisible and probe for the current
    /// view (`RequestStartView`) instead of resuming a role the cluster may
    /// have elected past.
    ProbeAsBackup {
        /// Also await a state-transfer offer before serving, replacing
        /// snapshot-shaped state from the live primary.
        await_state_transfer: bool,
    },
}

/// Restored state handed to [`VsrConsensus::restored`].
#[derive(Debug, Clone, Copy)]
pub struct VsrRestore<'a> {
    pub timers: &'a ConsensusTimers,
    /// `(view, log_view)` read back from the group's durable superblock.
    pub durable_view: Option<(u32, u32)>,
    /// Known view floor from journaled prepares or committed creation metadata.
    /// Used without a durable record, or to raise its view when `log_view` is zero.
    /// It does not certify `log_view`.
    pub view_fallback: Option<u32>,
    /// Starting `(view, log_view)` for a group with NO history of its own,
    /// consulted only when neither of the two above applies: the view the
    /// group was created in, as recorded by the metadata plane.
    ///
    /// Safe to start `Normal` in without a quorum only because every replica
    /// seeds the same value and the group's real view never drops below it:
    /// an empty log at the creation view is the floor a view-0 start used to
    /// be, never canonical over committed history. Seeded above the group's
    /// real view it would be canonical: the empty log wins the next DVC merge
    /// and wedges the group on its committed ops.
    ///
    /// Sets `log_view` as well as `view`, unlike `view_fallback`: the log is
    /// empty, so there is no history to misattribute to the view, and a
    /// primary whose `log_view` lags its `view` is treated as mid-transition
    /// and answers no `RequestStartView` probe (see
    /// [`VsrConsensus::handle_request_start_view`]).
    ///
    /// Deliberately NOT marked superblock-durable: nothing has been written
    /// yet, and claiming otherwise would let a restart resume a view no record
    /// holds. The group persists on its first tick instead.
    pub seed_view: Option<u32>,
    /// Non-zero boot incarnation; `None` keeps the default.
    pub incarnation: Option<u128>,
    pub join: JoinMode,
}

/// The rungs of the [`VsrConsensus::restore_view`] ladder.
///
/// Each is documented on the [`VsrRestore`] field of the same name. Named
/// rather than positional: `view_fallback` and `seed_view` are both
/// `Option<u32>`, and swapping them would set `log_view` to an uncertified
/// floor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ViewRestore {
    pub durable_view: Option<(u32, u32)>,
    pub view_fallback: Option<u32>,
    pub seed_view: Option<u32>,
}

/// How a group with no consensus state in memory should come up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FreshGroupStart {
    pub join: JoinMode,
    /// Recovery floor without log certification; see [`VsrRestore::view_fallback`].
    pub view_fallback: Option<u32>,
    /// Starting view for a group with no durable record, the last rung of the
    /// [`VsrConsensus::restore_view`] ladder; see [`VsrRestore::seed_view`].
    /// Always `None` when a durable record exists.
    pub seed_view: Option<u32>,
}

/// Decide how a group being materialised should join, and the floor and seed
/// it feeds into the [`VsrConsensus::restore_view`] ladder.
///
/// One function because there are two materialisation paths and they must not
/// drift: `build_partition_fresh` on the server, and the simulator's
/// `init_partition`, which cannot call it (that builder does real filesystem
/// work, and the simulator runs on in-memory storage). Both previously decided
/// this for themselves, which is how the simulator came to exercise neither
/// the seed nor the split it closes.
///
/// `restarted` is the caller's own evidence of a prior life, since the two
/// paths read it differently: a partition directory already on disk for the
/// server, a retained in-memory log for the simulator. `created_view` is the
/// view the metadata plane created the group in; see
/// [`VsrRestore::seed_view`].
#[must_use]
pub const fn fresh_group_start(
    restarted: bool,
    durable_view: Option<(u32, u32)>,
    created_view: u32,
) -> FreshGroupStart {
    let join = if restarted {
        JoinMode::ProbeAsBackup {
            await_state_transfer: false,
        }
    } else {
        JoinMode::Init
    };
    let (view_fallback, seed_view) = match (durable_view, join) {
        (None, JoinMode::Init) => (None, Some(created_view)),
        _ => (Some(created_view), None),
    };
    FreshGroupStart {
        join,
        view_fallback,
        seed_view,
    }
}

impl<B: MessageBus, P: Pipeline<Entry = PipelineEntry>> VsrConsensus<B, P> {
    /// # Panics
    /// - If `replica >= replica_count`.
    /// - If `replica_count < 1`.
    pub fn new(
        cluster: u128,
        replica: u8,
        replica_count: u8,
        group: u64,
        message_bus: B,
        pipeline: P,
    ) -> Self {
        Self::with_clock(
            cluster,
            replica,
            replica_count,
            group,
            message_bus,
            pipeline,
            ConsensusClock::system(),
        )
    }

    /// Restore constructor: the one ordered boot path for every plane, so the
    /// metadata and partition planes cannot diverge in restore order. Timers
    /// first, then the durable view, then role selection - a probe must never
    /// advertise a view older than the recorded one.
    ///
    /// # Panics
    /// - If `replica >= replica_count`.
    /// - If `replica_count < 1`.
    pub fn restored(
        cluster: u128,
        replica: u8,
        replica_count: u8,
        group: u64,
        message_bus: B,
        pipeline: P,
        restore: VsrRestore<'_>,
    ) -> Self {
        let mut consensus = Self::new(
            cluster,
            replica,
            replica_count,
            group,
            message_bus,
            pipeline,
        );
        let timers = restore.timers;
        consensus.set_normal_heartbeat_ticks(timers.normal_heartbeat_ticks);
        consensus.set_commit_message_ticks(timers.commit_message_ticks);
        consensus.set_prepare_ticks(timers.prepare_ticks);
        consensus.set_view_change_retransmit_ticks(timers.view_change_retransmit_ticks);
        consensus.set_view_change_status_ticks(timers.view_change_status_ticks);
        consensus.set_request_start_view_ticks(timers.request_start_view_ticks);
        consensus.set_probe_attempts_max(timers.probe_attempts_max);
        if let Some(incarnation) = restore.incarnation {
            consensus.set_incarnation(incarnation);
        }
        consensus.restore_view(ViewRestore {
            durable_view: restore.durable_view,
            view_fallback: restore.view_fallback,
            seed_view: restore.seed_view,
        });
        consensus.join(restore.join);
        consensus
    }

    /// The view ladder of every boot path: the durable record, else the
    /// uncertified floor, else the seed. See [`VsrRestore`] for each rung.
    ///
    /// Must run before `init` / `init_as_backup`: restoring after either would
    /// advertise a view older than the recorded one.
    pub fn restore_view(&mut self, restore: ViewRestore) {
        let ViewRestore {
            durable_view,
            view_fallback,
            seed_view,
        } = restore;
        let group = self.group;
        if let Some((view, log_view)) = durable_view {
            let installed_view = match view_fallback {
                Some(floor) if log_view == 0 => view.max(floor),
                _ => view,
            };
            // The one line proving the durable record was READ BACK, not merely
            // written: a replica that came back at view 0 is otherwise
            // indistinguishable from one that resumed correctly until it votes.
            // Only the metadata group says so at INFO. Every partition group
            // restores through here too, and one line each floods the boot log.
            if group == METADATA_GROUP {
                tracing::info!(
                    group,
                    view,
                    log_view,
                    installed_view,
                    "restored group view from its superblock"
                );
            } else {
                tracing::debug!(
                    group,
                    view,
                    log_view,
                    installed_view,
                    "restored group view from its superblock"
                );
            }
            self.set_view(installed_view);
            self.set_log_view(log_view);
            self.mark_superblock_durable(view, log_view);
        } else if let Some(view) = view_fallback {
            self.set_view(view);
        } else if let Some(view) = seed_view {
            tracing::info!(
                group,
                view,
                "seeded a group with no history of its own at its creation view"
            );
            self.set_view(view);
            self.set_log_view(view);
        }
    }

    /// Enter the role `mode` selects. Call it after [`Self::restore_view`].
    pub fn join(&self, mode: JoinMode) {
        match mode {
            JoinMode::Init => self.init(),
            JoinMode::ProbeAsBackup {
                await_state_transfer,
            } => {
                self.init_as_backup();
                self.rejoined_on_recovered_log.set(true);
                self.begin_view_probe();
                if await_state_transfer {
                    self.begin_state_transfer_await();
                }
            }
        }
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
        group: u64,
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
        // partition frames by the group id: metadata uses the sentinel,
        // partitions use `IggyNamespace::inner()` which lives strictly
        // inside the packed range. A group outside both ranges would
        // route to neither and silently warn-drop on every receiving peer.
        debug_assert!(
            group == METADATA_GROUP || IggyNamespace::is_packable(group),
            "VsrConsensus group must be METADATA_GROUP or a packable \
             IggyNamespace; got {group:#x}"
        );
        // Jitter only has to desynchronize replicas of one group, whose ids
        // differ, so the XOR cannot collide where it matters; `seed_from_u64`
        // (SplitMix64) decorrelates the streams of nearby seeds.
        let timeout_seed = u128::from(replica) ^ u128::from(group);
        let prepare_queue_max = pipeline.prepare_queue_max();
        // A metadata replica with no history holds view 0's empty log until boot
        // restores what recovery decided. Partitions keep no certificate.
        let empty_history = (group == METADATA_GROUP).then_some(HistoryCertificate::new(0, 0, 0));
        Self {
            cluster,
            replica,
            replica_count,
            group,
            view: Cell::new(0),
            log_view: Cell::new(0),
            view_durable: Cell::new(0),
            log_view_durable: Cell::new(0),
            incarnation: Cell::new(0),
            partition_incarnation: Cell::new(0),
            recovery_barrier: Cell::new(0),
            recovery_deadline: Cell::new(Duration::ZERO),
            view_start_head: Cell::new(0),
            ceded_primaryship: Cell::new(false),
            status: Cell::new(Status::Recovering),
            state_transfer_stage: Cell::new(StateTransferStage::Idle),
            observed_newer_view: Cell::new(0),
            sequencer: LocalSequencer::new(0),
            commit_min: Cell::new(0),
            commit_max: Cell::new(0),
            last_timestamp: Cell::new(0),
            last_prepare_checksum: Cell::new(0),
            pipeline: RefCell::new(pipeline),
            prepare_queue_max,
            message_bus,
            loopback_queue: RefCell::new(VecDeque::with_capacity(prepare_queue_max)),
            loopback_notifier: RefCell::new(None),
            start_view_change_from_all_replicas: RefCell::new(BitSet::with_capacity(REPLICAS_MAX)),
            probe_attempts: Cell::new(0),
            probe_attempts_max: Cell::new(PROBE_ATTEMPTS_MAX),
            recovery_election_allowed: Cell::new(true),
            recovered_log_complete: Cell::new(true),
            rejoined_on_recovered_log: Cell::new(false),
            untrusted_log: Cell::new(false),
            untrusted_view_change_timeouts: Cell::new(0),
            local_dvc_suffix: RefCell::new(None),
            journal_mutations: Cell::new(0),
            pending_view_log: RefCell::new(None),
            adopted_view: RefCell::new(None),
            adopted_view_dirty: Cell::new(false),
            view_log_unproven: Cell::new(false),
            log_view_proven: Cell::new(0),
            view_log_merged: Cell::new(false),
            view_log_merge_uncertified: Cell::new(false),
            view_log_unproven_through: Cell::new(u64::MAX),
            view_log_missing_op: Cell::new(None),
            history_certificate: Cell::new(empty_history),
            history_certificate_durable: Cell::new(empty_history),
            do_view_change_from_all_replicas: RefCell::new(dvc_quorum_array_empty()),
            do_view_change_quorum: Cell::new(false),
            sent_own_start_view_change: Cell::new(false),
            sent_own_do_view_change: Cell::new(false),
            timeouts: RefCell::new(TimeoutManager::new(timeout_seed)),
            heartbeat_watermark: Cell::new((0, 0)),
            eager_commit: Cell::new(false),
            commit_announcements: RefCell::new(CommitAnnouncements::new(replica_count)),
            applied_log_contradictions: Cell::new(0),
            applied_log_contradictions_taken: Cell::new(0),
            contradiction_reported_view: Cell::new(None),
            clock,
        }
    }

    /// Override the normal-heartbeat (primary liveness) window, in consensus
    /// ticks. Sized from `[cluster] heartbeat_timeout` by the runtime. Must
    /// run before `init` / `init_as_backup`: the override discards any
    /// countdown already in flight.
    pub fn set_normal_heartbeat_ticks(&self, ticks: u64) {
        self.timeouts.borrow_mut().set_normal_heartbeat_ticks(ticks);
    }

    /// Override the primary's commit-broadcast interval, in consensus ticks.
    /// Sized from `[cluster] commit_broadcast_interval` by the runtime. Must
    /// run before `init` / `init_as_backup`: the override discards any
    /// countdown already in flight.
    pub fn set_commit_message_ticks(&self, ticks: u64) {
        self.timeouts.borrow_mut().set_commit_message_ticks(ticks);
    }

    /// Override the primary's prepare-retransmit interval, in consensus ticks.
    /// Sized from `[cluster] prepare_retransmit_interval` by the runtime. Must
    /// run before `init` / `init_as_backup`: the override discards any
    /// countdown already in flight.
    pub fn set_prepare_ticks(&self, ticks: u64) {
        self.timeouts.borrow_mut().set_prepare_ticks(ticks);
    }

    /// Override the view-change retransmit interval (`StartViewChange` and
    /// `DoViewChange`, kept equal), in consensus ticks. Sized from `[cluster]
    /// view_change_retransmit_interval` by the runtime. Must run before `init`
    /// / `init_as_backup`: the override discards any countdown already in
    /// flight.
    pub fn set_view_change_retransmit_ticks(&self, ticks: u64) {
        self.timeouts
            .borrow_mut()
            .set_view_change_retransmit_ticks(ticks);
    }

    /// Override the view-change status backstop, in consensus ticks. Sized from
    /// `[cluster] view_change_status_timeout` by the runtime. Must run before
    /// `init` / `init_as_backup`: the override discards any countdown already
    /// in flight.
    pub fn set_view_change_status_ticks(&self, ticks: u64) {
        self.timeouts
            .borrow_mut()
            .set_view_change_status_ticks(ticks);
    }

    /// Override the request-start-view retransmit interval, in consensus ticks.
    /// Sized from `[cluster] request_start_view_retransmit_interval` by the
    /// runtime. Must run before `init` / `init_as_backup`: the override
    /// discards any countdown already in flight.
    pub fn set_request_start_view_ticks(&self, ticks: u64) {
        self.timeouts
            .borrow_mut()
            .set_request_start_view_ticks(ticks);
    }

    /// Override the recovering-replica probe-attempt ceiling before it falls
    /// back to an election. Sized from `[cluster] view_probe_attempts_max` by
    /// the runtime; the simulator and tests keep [`PROBE_ATTEMPTS_MAX`]. Must
    /// run before `init` / `init_as_backup`.
    pub fn set_probe_attempts_max(&self, max: u32) {
        self.probe_attempts_max.set(max);
    }

    pub fn set_recovery_election_allowed(&self, allowed: bool) {
        self.recovery_election_allowed.set(allowed);
    }

    #[must_use]
    pub const fn recovery_election_allowed(&self) -> bool {
        self.recovery_election_allowed.get()
    }

    /// Declare whether a restart recovers every prepare this replica
    /// acknowledged. `true` (the default) when every acknowledgement waits for
    /// stable storage; `false` when some are sent once the write is queued, as
    /// `replicated` partition durability does.
    ///
    /// With `false`, a restarted replica whose boot view probe goes unanswered
    /// still elects, since a full-cluster restart has nobody to answer it, but
    /// its log is untrusted until a settled primary vouches for it: its
    /// `DoViewChange` says so, and no view starts on it short of a trusted
    /// quorum or every replica reporting. See [`Self::has_untrusted_log`].
    pub fn set_recovered_log_complete(&self, complete: bool) {
        self.recovered_log_complete.set(complete);
    }

    #[must_use]
    pub const fn recovered_log_complete(&self) -> bool {
        self.recovered_log_complete.get()
    }

    /// Whether this replica elected on a recovered log that may lack ops it
    /// acknowledged and has not yet been synced by a settled primary.
    #[must_use]
    pub const fn has_untrusted_log(&self) -> bool {
        self.untrusted_log.get()
    }

    pub fn init(&self) {
        self.status.set(Status::Normal);
        self.rejoined_on_recovered_log.set(false);
        let mut timeouts = self.timeouts.borrow_mut();
        if self.is_primary() {
            // Prepare is deliberately NOT armed here: a fresh primary has an
            // empty pipeline and nothing to retransmit, and the timer is owned by
            // the pipeline's edges from this point on ("ticking iff the pipeline
            // is non-empty", see `sync_prepare_timeout`). The first
            // `push_prepare_entry` arms it, timed from that push rather than from
            // boot.
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
    /// replica's own silence provokes that election.
    ///
    /// The local journal is intact here, so the normal commit walk applies it and no
    /// commit-floor fast-forward is needed. Callers pair this with
    /// [`Self::begin_view_probe`], which moves the replica to `Status::Recovering` so
    /// it stays quorum-invisible until a `StartView` answers.
    pub fn init_as_backup(&self) {
        self.status.set(Status::Normal);
        self.rejoined_on_recovered_log.set(false);
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

    /// Normal status can persist while a newer view is being confirmed.
    #[must_use]
    pub const fn has_observed_newer_view(&self) -> bool {
        self.observed_newer_view.get() > self.view.get()
    }

    /// Set the per-boot incarnation nonce, at boot before `init`. Production
    /// supplies a random `u128`; the deterministic simulator supplies a
    /// seed-derived value bumped per restart. See the `incarnation` field.
    pub fn set_incarnation(&self, incarnation: u128) {
        self.incarnation.set(incarnation);
    }

    /// This replica's per-boot incarnation nonce (`0` when unset). Stamped on
    /// outbound `RequestStartView` probes by the shard.
    #[must_use]
    pub const fn incarnation(&self) -> u128 {
        self.incarnation.get()
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

    /// The primary of a Normal view, unless it ceded at boot
    /// (`init_as_backup`): then it is a backup by role however the view math
    /// reads, so it neither retransmits prepares nor announces commits.
    fn is_acting_primary(&self) -> bool {
        self.is_primary() && self.status.get() == Status::Normal && !self.ceded_primaryship.get()
    }

    /// Ask the partition quorum to elect `target` in the next view assigned to it.
    /// A ready backup may initiate this too. Refusal leaves consensus unchanged.
    /// The caller fences the incarnation, drains history work, refreshes the DVC
    /// suffix and persists the new view before dispatching the returned actions.
    pub fn request_primary_transfer(&self, expected_view: u32, target: u8) -> Vec<VsrAction> {
        let view = self.view();
        if self.group == METADATA_GROUP
            || target >= self.replica_count
            || view != expected_view
            || !self.is_normal()
            || self.has_ceded_primaryship()
            || self.is_transferring()
            || self.view_log_unproven()
            || self.observed_newer_view.get() > view
            || self.commit_min() != self.commit_max()
            || self.commit_max() < self.recovery_barrier()
            || !self.applied_view_start()
        {
            return Vec::new();
        }
        let primary = self.primary_index(view);
        if target == primary {
            return Vec::new();
        }
        let replicas = u32::from(self.replica_count);
        let distance = (u32::from(target) + replicas - u32::from(primary)) % replicas;
        let Some(next_view) = view.checked_add(distance) else {
            return Vec::new();
        };
        self.enter_view_change(
            PlaneKind::Partitions,
            next_view,
            ViewChangeReason::PrimaryTransfer,
        )
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

    /// `StartViewChange` votes needed from other replicas before sending `DoViewChange`.
    /// This is f for 2f+1 replicas, not the failure bound for every quorum configuration.
    #[must_use]
    pub const fn max_faulty(&self) -> usize {
        (self.replica_count as usize - 1) / 2
    }

    /// Replicas that must ack before an op is committed.
    ///
    /// Capped at [`QUORUM_REPLICATION_MAX`] to keep a wide cluster's commit path
    /// cheap; the view-change quorum grows so the two still sum above the count.
    #[must_use]
    pub const fn quorum_replication(&self) -> usize {
        if self.replica_count == 2 {
            // =1 would intersect, but =2 keeps a two-replica cluster durable.
            return 2;
        }
        let half_rounded_up = (self.replica_count as usize).div_ceil(2);
        if half_rounded_up < QUORUM_REPLICATION_MAX {
            half_rounded_up
        } else {
            QUORUM_REPLICATION_MAX
        }
    }

    /// Replicas that must send a `DoViewChange` before a view can start.
    ///
    /// Pays for the cheaper replication quorum, which is the far hotter path.
    #[must_use]
    pub const fn quorum_view_change(&self) -> usize {
        if self.replica_count == 2 {
            // Avoids a single-replica view change special case.
            return 2;
        }
        self.replica_count as usize - self.quorum_replication() + 1
    }

    /// Nacks required to prove an op was never committed, so the new primary may
    /// truncate it.
    ///
    /// Sized so a nack quorum and a replication quorum cannot both exist for one
    /// op. This is what makes truncation safe, so it is the one quorum that must
    /// never be loosened.
    #[must_use]
    pub const fn quorum_nack_prepare(&self) -> usize {
        self.replica_count as usize - self.quorum_replication() + 1
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

    /// Re-decide the barrier against a log head the cluster just settled.
    ///
    /// Boot arms it at the recovered journal head: those ops were acked before the
    /// restart, so admitting writes before they re-commit rolls back committed
    /// history. It otherwise clears only by `commit_max` passing it, which never
    /// happens when a view change discards the suffix instead of re-committing it.
    /// The boot re-pipeline already ran, so nothing re-prepares those ops,
    /// `is_caught_up_primary` stays shut, and the primary drops the very requests
    /// that would raise `commit_max`.
    ///
    /// Call this wherever the head is authoritatively re-decided: a merged log at
    /// view start, an adopted `StartView`. `head` lowers the barrier when the view
    /// truncated the suffix, keeps it when the suffix survived.
    ///
    /// Lowered, never cleared. `is_caught_up_primary` reads it against `commit_max`
    /// and a met barrier costs it nothing, but `await_recovery_barrier` reads it
    /// against `commit_min`, and adoption raises `commit_max` before walking the
    /// suffix into the state machine. Zeroing a met barrier would open that read
    /// gate over the unapplied window it exists to hold.
    ///
    /// `head == 0` returns instead of lowering: zero is the DISARMED value, not a
    /// met barrier. `barrier_state` (`server/src/http/reads.rs`) reads zero as
    /// "nothing gated" and skips the `commit_min` comparison, so writing it would
    /// open the gate rather than lower it. Reachable on the wire:
    /// `adopt_start_view_suffix` returns `StartViewHeader.op` verbatim when the
    /// suffix fails to decode, `validate` does not require `op >= 1`, and
    /// `head == 0` passes the committed-prefix guard of `handle_start_view` only
    /// while `commit_max` is 0.
    fn redecide_recovery_barrier(&self, head: u64) {
        let barrier = self.recovery_barrier.get();
        if barrier == 0 || head == 0 {
            return;
        }
        self.recovery_barrier.set(barrier.min(head));
    }

    /// Deadline paired with [`Self::recovery_barrier`]; only meaningful while the
    /// barrier is armed (non-zero).
    #[must_use]
    pub const fn recovery_deadline(&self) -> Duration {
        self.recovery_deadline.get()
    }

    pub fn set_recovery_deadline(&self, deadline: Duration) {
        self.recovery_deadline.set(deadline);
    }

    /// Whether `commit_min` reached the op head of the view this replica last
    /// started as primary. See `view_start_head`.
    #[must_use]
    pub const fn applied_view_start(&self) -> bool {
        self.commit_min.get() >= self.view_start_head.get()
    }

    pub fn set_view(&mut self, view: u32) {
        self.view.set(view);
    }

    #[must_use]
    pub const fn status(&self) -> Status {
        self.status.get()
    }

    /// Run `f` against the pipeline.
    ///
    /// The borrow cannot escape `f`, so it cannot be held across an `.await` and
    /// alias a sibling task's `borrow_mut` into a `BorrowMutError` panic. Same
    /// shape, and the same reason, as `IggyPartitions::with_partition`. Prefer
    /// the named accessors below; this is for the few callers that need several
    /// reads under one borrow.
    pub fn with_pipeline<R>(&self, f: impl FnOnce(&P) -> R) -> R {
        f(&self.pipeline.borrow())
    }

    /// [`Self::with_pipeline`] for a mutating operation.
    ///
    /// Callers that pop or clear should prefer [`Self::pop_committed_prepare`] /
    /// [`Self::clear_pipeline`], which also keep the prepare timeout's
    /// ticking-iff-non-empty invariant. This form does not.
    pub fn with_pipeline_mut<R>(&self, f: impl FnOnce(&mut P) -> R) -> R {
        f(&mut self.pipeline.borrow_mut())
    }

    /// Whether the prepare queue is at its depth bound; callers route to
    /// [`Self::push_queued_request`] on `true`.
    #[must_use]
    pub fn pipeline_is_full(&self) -> bool {
        self.pipeline.borrow().is_full()
    }

    #[must_use]
    pub fn pipeline_is_empty(&self) -> bool {
        self.pipeline.borrow().is_empty()
    }

    /// In-flight prepare count.
    #[must_use]
    pub fn pipeline_len(&self) -> usize {
        self.pipeline.borrow().len()
    }

    /// Requests parked waiting for a prepare slot.
    #[must_use]
    pub fn request_queue_len(&self) -> usize {
        self.pipeline.borrow().request_queue_len()
    }

    /// Whether either queue already carries a request from `client_id`: the
    /// metadata plane's in-flight dedup.
    #[must_use]
    pub fn pipeline_has_message_from_client(&self, client_id: u128) -> bool {
        self.pipeline.borrow().has_message_from_client(client_id)
    }

    #[must_use]
    pub fn pending_request(
        &self,
        client_id: u128,
    ) -> Option<(u64, u64, iggy_binary_protocol::Operation)> {
        self.pipeline.borrow().pending_request(client_id)
    }

    /// Header of the oldest in-flight prepare.
    #[must_use]
    pub fn pipeline_head_header(&self) -> Option<PrepareHeader> {
        self.pipeline.borrow().head().map(|entry| entry.header)
    }

    /// Whether an in-flight prepare matches `(op, checksum)`: the ack paths' test
    /// that the frame they are about to count still describes a live entry.
    #[must_use]
    pub fn pipeline_holds_entry(&self, op: u64, checksum: u128) -> bool {
        self.pipeline
            .borrow()
            .entry_by_op_and_checksum(op, checksum)
            .is_some()
    }

    /// Promote the oldest parked request, if any.
    pub fn pop_queued_request(&self) -> Option<P::Request> {
        self.pipeline.borrow_mut().pop_request()
    }

    /// Pop the pipeline head once its op has committed, keeping the prepare
    /// timeout's lifecycle in step.
    ///
    /// The timeout measures the age of the oldest un-acked prepare, so draining
    /// the head has to either stop it (nothing left to retransmit) or restart it
    /// (the next entry becomes the oldest, and it must be timed from now rather
    /// than inheriting the drained entry's elapsed ticks). Arming happens in
    /// `Self::push_prepare_entry`; between the two the invariant is "ticking
    /// iff the pipeline is non-empty".
    pub fn pop_committed_prepare(&self) -> Option<P::Entry> {
        let popped = self.pipeline.borrow_mut().pop();
        if popped.is_some() {
            self.sync_prepare_timeout();
        }
        popped
    }

    /// Drop every in-flight prepare and parked request, and disarm the prepare
    /// timeout with them (a view change re-prepares from the new primary; there
    /// is nothing left here to retransmit).
    pub fn clear_pipeline(&self) {
        self.pipeline.borrow_mut().clear();
        self.timeouts.borrow_mut().stop(TimeoutKind::Prepare);
    }

    /// Re-establish "prepare timeout ticking iff the pipeline is non-empty".
    ///
    /// Stops the timer on an empty pipeline; otherwise restarts it so it times
    /// the current oldest entry from now. Exposed for the plane-side drains that
    /// pop through [`Pipeline`] directly rather than
    /// through [`Self::pop_committed_prepare`].
    pub fn sync_prepare_timeout(&self) {
        let empty = self.pipeline.borrow().is_empty();
        let mut timeouts = self.timeouts.borrow_mut();
        if empty {
            timeouts.stop(TimeoutKind::Prepare);
        } else {
            // `start`, not `reset`: `reset` leaves `ticking` untouched, so a timer
            // stopped earlier would come back armed-but-dead and never fire. On an
            // already-ticking timer the two are the same call. Same trap
            // `advance_commit_max` documents.
            timeouts.start(TimeoutKind::Prepare);
        }
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

    /// Undo the `Self::push_prepare_entry` pre-advance for a prepare whose
    /// journal append failed, so the op it claimed is handed back.
    ///
    /// The pre-advance runs the sequencer ahead of the WAL on purpose, so that a
    /// sibling `on_request` racing the append await cannot project a duplicate op.
    /// The cost is that a failed append leaves the op claimed with nothing durable
    /// behind it: the next prepare chains off a phantom, the WAL takes a permanent
    /// hole at that op, and the divergence rides the handoff bundle out to peers.
    ///
    /// Rolls the sequencer back to `header.op - 1` and the parent chain to
    /// `header.parent`, both of which the header records from the moment it was
    /// projected, and drops the pipeline entry so the reclaimed op is free rather
    /// than colliding with a live entry. Dropping the entry drops its reply
    /// sender, so a waiting client observes `Canceled` instead of hanging until
    /// the request times out.
    ///
    /// The observed prepare timestamp is deliberately NOT rolled back:
    /// [`Self::next_monotonic_timestamp`] only ever needs a lower bound, and
    /// lowering it back could re-stamp a value a peer already observed.
    ///
    /// Runs after an `.await`, so the state it means to unwind may have been
    /// reassigned meanwhile. Four guards prove the pre-advance is still this
    /// prepare's (primaryship, its view, `Normal` status, and a tail matched on
    /// `(op, checksum)`), and only [`PrepareRollback::Unwound`] mutates.
    ///
    /// See [`PrepareRollback`] for the outcomes.
    pub fn rollback_pipelined_prepare(&self, header: &PrepareHeader) -> PrepareRollback {
        if !self.is_primary() {
            return PrepareRollback::NotPreAdvanced;
        }
        // Ahead of the sequencer compare: a view change concluding under the append
        // leaves it able to match `header.op` with a different prepare beneath it.
        if header.view != self.view.get() || !self.is_normal() {
            return PrepareRollback::Superseded {
                view: self.view.get(),
            };
        }
        let sequence = self.sequencer.current_sequence();
        if sequence != header.op {
            return PrepareRollback::Overtaken { sequence };
        }
        // Matched on `(op, checksum)`: an op number alone is not unique across views,
        // and a same-numbered entry belonging to someone else is the whole hazard.
        // A refusal, not a `debug_assert`: the state is reachable from a race rather
        // than a bug here, and asserting would panic debug builds on exactly what
        // release declines to act on.
        let Some(removed) = self
            .pipeline
            .borrow_mut()
            .remove_tail(header.op, header.checksum)
        else {
            return PrepareRollback::TailMismatch;
        };
        drop(removed);
        self.sequencer.set_sequence(header.op.saturating_sub(1));
        self.set_last_prepare_checksum(header.parent);
        // The pop may have emptied the pipeline; the timeout is owned by its edges.
        self.sync_prepare_timeout();
        PrepareRollback::Unwound
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

    /// [`Self::pipeline_message_with_subscriber`] for a promoted queued
    /// request: adopts the sender the request carried through the request
    /// queue instead of minting a fresh channel, so the awaiter that parked
    /// at enqueue time resolves on this prepare's commit.
    ///
    /// # Panics
    /// If not primary (mirrors [`Consensus::pipeline_message`]).
    pub fn pipeline_message_with_sender(
        &self,
        plane: PlaneKind,
        message: &Message<PrepareHeader>,
        sender: Sender<Message<ReplyHeader>>,
    ) {
        let entry = PipelineEntry::with_sender(*message.header(), sender);
        self.push_prepare_entry(plane, message, entry);
    }

    /// Preserve local admission order when a queued request becomes a prepare.
    ///
    /// # Panics
    /// If not primary, as with other pipeline insertion methods.
    pub fn pipeline_message_with_order(
        &self,
        plane: PlaneKind,
        message: &Message<PrepareHeader>,
        sender: Option<Sender<Message<ReplyHeader>>>,
        order: Option<LocalRequestOrder>,
    ) {
        let mut entry = sender.map_or_else(
            || PipelineEntry::new(*message.header()),
            |sender| PipelineEntry::with_sender(*message.header(), sender),
        );
        entry.local_order = order;
        self.push_prepare_entry(plane, message, entry);
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
    pub const fn group(&self) -> u64 {
        self.group
    }

    /// Set the partition's metadata creation revision before serving its group.
    pub fn set_partition_incarnation(&self, partition_incarnation: u64) {
        self.partition_incarnation.set(partition_incarnation);
    }

    #[must_use]
    pub const fn partition_incarnation(&self) -> u64 {
        self.partition_incarnation.get()
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

    /// Restore or seed `log_view`: a value read back from disk or chosen at
    /// creation, so the journal already backs it.
    pub fn set_log_view(&self, log_view: u32) {
        self.log_view.set(log_view);
        if self.group != METADATA_GROUP {
            self.log_view_proven.set(log_view);
        }
    }

    /// The latest log identity this replica can reproduce after a crash.
    /// Adopted headers name a log independently of the certificate proving its
    /// bodies. The dispatcher persists both claims before any view-scoped send.
    #[must_use]
    pub fn log_view_claim(&self) -> u32 {
        let proven = if self.group == METADATA_GROUP {
            self.history_certificate
                .get()
                .map_or(0, |certificate| certificate.view)
        } else if self.view_log_unproven.get() {
            self.log_view_proven.get()
        } else {
            self.log_view.get()
        };
        self.adopted_view
            .borrow()
            .as_ref()
            .map_or(proven, |adopted| proven.max(adopted.view))
    }

    /// Metadata keeps the adopted view beside its bound certificate; partitions
    /// store their proven claim and bind their history in the partition journal.
    const fn persisted_log_view(&self) -> u32 {
        if self.group == METADATA_GROUP {
            self.log_view.get()
        } else if self.view_log_unproven.get() {
            self.log_view_proven.get()
        } else {
            self.log_view.get()
        }
    }

    /// Snapshot the durable VSR state for the superblock. `view`, `log_view`,
    /// `commit_max`, and the replica identity come from consensus; the caller
    /// supplies the paired checkpoint, which consensus does not own.
    #[must_use]
    pub fn vsr_state(&self, checkpoint_op: u64, checkpoint_checksum: u128) -> VsrState {
        VsrState {
            cluster: self.cluster,
            replica_id: self.replica,
            replica_count: self.replica_count,
            view: self.view.get(),
            log_view: self.persisted_log_view(),
            commit_max: self.commit_max.get(),
            checkpoint_op,
            checkpoint_checksum,
            // Consensus mints no message offsets: the PARTITION plane stamps
            // both of these in before it writes
            // (`IggyPartition::write_superblock`).
            offset_frontier: 0,
            offset_reserved: 0,
            adopted_view: self.adopted_view.borrow().clone(),
        }
        .with_history_certificate(self.history_certificate.get())
    }

    /// METADATA plane: the newest view whose log the WAL holds.
    #[must_use]
    pub const fn history_certificate(&self) -> Option<HistoryCertificate> {
        self.history_certificate.get()
    }

    /// Whether this replica may acknowledge in its `log_view`. On the metadata plane
    /// only once the WAL holds that view's log, since a restart restores the
    /// certified view rather than the adopted one, and an ack the restored view does
    /// not cover is a vote the next view change cannot see.
    #[must_use]
    pub fn history_certified(&self) -> bool {
        self.group != METADATA_GROUP
            || self
                .history_certificate
                .get()
                .is_some_and(|certificate| certificate.view == self.log_view.get())
    }

    /// Certify `log_view`: the WAL holds its log, with `checksum` at head `op`.
    /// A proven log may vote again, so an unproven restart's election ban ends.
    pub fn certify_view_log(&self, op: u64, checksum: u128) {
        self.history_certificate.set(Some(HistoryCertificate::new(
            self.log_view.get(),
            op,
            checksum,
        )));
        self.recovery_election_allowed.set(true);
    }

    /// Follow the WAL head while the certified view is the proven normal one, so a
    /// later view finds the binding above every op this replica acknowledged.
    pub fn extend_history_certificate(&self, op: u64, checksum: u128) {
        if let Some(certificate) = self.history_certificate.get()
            && certificate.view == self.log_view.get()
            && certificate.op < op
            && self.status.get() == Status::Normal
            && !self.view_log_unproven.get()
        {
            self.history_certificate.set(Some(HistoryCertificate::new(
                certificate.view,
                op,
                checksum,
            )));
        }
    }

    /// Bind the certificate to `op` when a rewrite is about to remove the entries
    /// from `from` up and the binding is one of them. The caller persists the
    /// lowered binding before it removes anything.
    pub fn lower_history_certificate(&self, from: u64, op: u64, checksum: u128) {
        if let Some(certificate) = self.history_certificate.get()
            && certificate.op >= from
        {
            self.history_certificate.set(Some(HistoryCertificate::new(
                certificate.view,
                op,
                checksum,
            )));
        }
    }

    /// Record the certificate the superblock now holds, the one the write carried.
    pub fn mark_history_certificate_durable(&self, certificate: Option<HistoryCertificate>) {
        self.history_certificate_durable.set(certificate);
    }

    /// Install what recovery decided about the WAL. `head` is the WAL head after
    /// recovery truncated it, `record` the durable state it decided from.
    ///
    /// An unproven WAL is fenced as a log adopted from a view change is until it is
    /// proven, but with no adopted headers to stand in for it. Without a record the
    /// certificate is derived the same way on every boot, so it counts as durable.
    ///
    /// An unproven replica also stays out of elections when its view probe goes
    /// unanswered. It may hold ops a quorum committed in a view it can no longer
    /// claim, and voting under a lower `log_view` lets a peer's older log replace
    /// them. A `StartView` gives it a log to prove against instead.
    pub fn restore_history(
        &self,
        recovery: HistoryRecovery,
        head: (u64, u128),
        record: Option<&VsrState>,
    ) {
        let certificate = match recovery {
            HistoryRecovery::Certified { view, .. } => {
                // A retained repaired body belongs to the adopted log, not the
                // older certificate's prefix. Only whole-log proof may extend it.
                record
                    .filter(|state| {
                        state
                            .adopted_view
                            .as_ref()
                            .is_some_and(|adopted| adopted.view > view)
                    })
                    .and_then(VsrState::history_certificate)
                    .or_else(|| Some(HistoryCertificate::new(view, head.0, head.1)))
            }
            HistoryRecovery::Unproven => {
                tracing::warn!(
                    replica = self.replica,
                    view = self.view.get(),
                    head = head.0,
                    "metadata WAL has no history certificate that binds it; staying out of \
                     elections until a StartView proves it"
                );
                self.recovery_election_allowed.set(false);
                self.view_log_unproven.set(true);
                self.view_log_unproven_through.set(u64::MAX);
                self.view_log_missing_op.set(None);
                self.invalidate_local_dvc_suffix();
                None
            }
        };
        self.history_certificate.set(certificate);
        self.history_certificate_durable
            .set(record.map_or(certificate, VsrState::history_certificate));
    }

    /// Whether the certificate moved to a view, or down to a binding, the superblock
    /// does not hold yet.
    #[must_use]
    pub const fn history_certificate_needs_persist(&self) -> bool {
        match (
            self.history_certificate.get(),
            self.history_certificate_durable.get(),
        ) {
            (Some(certificate), Some(durable)) => {
                certificate.view != durable.view || certificate.op < durable.op
            }
            (Some(_), None) => true,
            (None, _) => false,
        }
    }

    /// Install this replica's uncommitted-suffix snapshot for the current view.
    ///
    /// Called by the shard, which owns the journal. Consensus keeps the snapshot
    /// rather than deriving it so the copy in this replica's `StoredDvc` and the
    /// copy on the wire are the same bytes: a merge seeing two versions of one
    /// sender's suffix could adopt a header no replica holds.
    ///
    /// Installing twice for one view overwrites: the shard refreshes before each
    /// handler, and a later suffix is at least as complete (repair only adds).
    pub fn set_local_dvc_suffix(&self, suffix: DvcSuffix) {
        let (op, commit, mutations) = self.local_dvc_suffix_tag();
        *self.local_dvc_suffix.borrow_mut() = Some((op, commit, mutations, suffix));
    }

    /// Drop the cached suffix snapshot.
    ///
    /// For a change the journal counter cannot see: `start_pending_view` takes the
    /// parked log the snapshot was stitched over, which alters the suffix while
    /// every journal entry stays put.
    pub fn invalidate_local_dvc_suffix(&self) {
        self.local_dvc_suffix.borrow_mut().take();
    }

    /// Record that the journal backing this replica's suffix changed: an append, a
    /// truncation, a drain, an eviction.
    ///
    /// Call from the mutation site. The next refresh re-reads the journal, and
    /// until it does the snapshot reads as absent rather than as a description of
    /// a log that has moved on. Both directions matter: a removal leaves a
    /// snapshot offering bodies this replica can no longer serve, stranding a peer
    /// that picks it as a repair source, and an append leaves one nacking an op
    /// this replica has since journaled and acked, which is a licence to truncate
    /// committed data.
    pub fn note_journal_mutation(&self) {
        self.journal_mutations
            .set(self.journal_mutations.get().wrapping_add(1));
    }

    /// The `(op, commit, journal_mutations)` a snapshot must match to still
    /// describe this log. `commit` is clamped to `op` exactly as the outgoing DVC
    /// clamps it.
    fn local_dvc_suffix_tag(&self) -> (u64, u64, u64) {
        let op = self.sequencer.current_sequence();
        (
            op,
            self.commit_max.get().min(op),
            self.journal_mutations.get(),
        )
    }

    /// This replica's suffix snapshot, or an empty one when none matches the log's
    /// current head and commit point. Empty nacks nothing and offers no bodies,
    /// but it does not abstain either. From a canonical sender it lets every
    /// lower-`log_view` copy of an op count as a nack (`tally_op`), which can
    /// truncate. So the shard refreshes the snapshot before every vote.
    #[must_use]
    pub fn local_dvc_suffix(&self) -> DvcSuffix {
        let tag = self.local_dvc_suffix_tag();
        match &*self.local_dvc_suffix.borrow() {
            Some((op, commit, mutations, suffix)) if (*op, *commit, *mutations) == tag => {
                suffix.clone()
            }
            _ => DvcSuffix::empty(),
        }
    }

    /// True when no snapshot matches the log's current head and commit point,
    /// so the shard must read one from the journal before this replica votes.
    #[must_use]
    pub fn local_dvc_suffix_stale(&self) -> bool {
        let tag = self.local_dvc_suffix_tag();
        !matches!(
            &*self.local_dvc_suffix.borrow(),
            Some((op, commit, mutations, _)) if (*op, *commit, *mutations) == tag
        )
    }

    /// True when the current `(view, log_view)` is not yet in the superblock, so a
    /// view-scoped send would advertise a view a crash could lose. The split-brain
    /// gate: the dispatcher persists first when this holds. `commit_max` is
    /// excluded deliberately, since it advances on every commit and rides the
    /// checkpoint write rather than each view change.
    ///
    /// A newly certified view counts too, since an ack in it must survive a restart,
    /// and so does a binding lowered for a rewrite, which must not remove the entry
    /// the durable certificate still names.
    #[must_use]
    pub const fn needs_superblock_persist(&self) -> bool {
        self.view.get() != self.view_durable.get()
            || self.persisted_log_view() != self.log_view_durable.get()
            || self.history_certificate_needs_persist()
            || self.adopted_view_dirty.get()
    }

    /// Record that the superblock now durably holds `(view, log_view)`: the exact
    /// values written, NOT a re-read of the current in-memory view.
    ///
    /// The caller passes what it wrote because the in-memory view can advance
    /// across the write's `.await`, when a concurrent checkpoint holds the
    /// superblock lock while the pump adopts a newer view via `handle_start_view`.
    /// Re-reading `self.view` here would mark that newer, unwritten view durable,
    /// and [`Self::needs_superblock_persist`] would wrongly report it safe to
    /// send: the split-brain footgun this signature removes.
    ///
    /// Called after a successful write with the state written, and on boot with
    /// the recovered `(view, log_view)`, durable by definition.
    pub fn mark_superblock_durable(&self, view: u32, log_view: u32) {
        debug_assert!(
            view <= self.view.get() && log_view <= self.log_view.get(),
            "durable (view={view}, log_view={log_view}) cannot exceed in-memory \
             (view={}, log_view={})",
            self.view.get(),
            self.log_view.get(),
        );
        self.view_durable.set(view);
        self.log_view_durable.set(log_view);
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
        // A merge parked for the superseded view may describe a different log, so
        // drop it and let the new attempt re-derive from the DVCs it collects.
        self.pending_view_log.borrow_mut().take();
        self.loopback_queue.borrow_mut().clear();
        self.commit_announcements.borrow_mut().reset();
        let mut pipeline = self.pipeline.borrow_mut();
        pipeline.cancel_all_subscribers();
        pipeline.clear_request_queue();
    }

    /// Process one tick. Call this every [`crate::TICK_INTERVAL`].
    ///
    /// Returns a list of actions to take based on fired timeouts.
    /// Empty vec means no actions needed.
    pub fn tick(&self, plane: PlaneKind) -> Vec<VsrAction> {
        self.tick_with_history_fence(plane, false)
    }

    /// Timers and retransmissions continue while storage leases defer history changes.
    pub fn tick_with_history_fence(
        &self,
        plane: PlaneKind,
        history_fenced: bool,
    ) -> Vec<VsrAction> {
        let mut actions = Vec::new();
        let mut timeouts = self.timeouts.borrow_mut();

        // Phase 1: Tick all timeouts
        timeouts.tick();

        // Phase 2: Handle fired timeouts
        if !history_fenced && timeouts.fired(TimeoutKind::NormalHeartbeat) {
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

        // The heartbeat carries `commit_min` too, so it also answers a pending
        // eager request.
        let eager_commit = self.eager_commit.take();
        if timeouts.fired(TimeoutKind::CommitMessage) {
            drop(timeouts);
            actions.extend(self.handle_commit_message_timeout());
            timeouts = self.timeouts.borrow_mut();
        } else if eager_commit {
            actions.extend(self.eager_commit_actions());
        }

        if !history_fenced && timeouts.fired(TimeoutKind::RequestStartViewMessage) {
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
                    let attempts = self.probe_attempts.get().saturating_add(1);
                    self.probe_attempts.set(attempts);
                    if attempts >= self.probe_attempts_max.get()
                        && self.recovery_election_allowed.get()
                    {
                        // Nobody answered: full-cluster bootstrap, so there
                        // is no live primary to fetch state from. Local
                        // recovery is authoritative; abandon the transfer
                        // and elect on recovered logs.
                        if self.state_transfer_stage.get() != StateTransferStage::Idle {
                            tracing::info!(
                                replica = self.replica,
                                namespace_raw = self.group,
                                "view probe exhausted; abandoning state transfer (cluster bootstrap)"
                            );
                            self.set_state_transfer_stage(StateTransferStage::Idle);
                        }
                        self.finish_view_probe();
                        // "Nobody answered" does not prove nobody is alive: a
                        // survivor mid view change answers no probe. If this log
                        // can lack acknowledged ops, a quorum of restarted
                        // replicas could otherwise elect past a survivor holding
                        // committed ops that only it still has. Only a log read
                        // back after a restart can be short of what it acked.
                        if self.rejoined_on_recovered_log.get()
                            && !self.recovered_log_complete.get()
                        {
                            tracing::info!(
                                replica = self.replica,
                                namespace_raw = self.group,
                                view = self.view.get(),
                                "view probe exhausted on a log that may lack acknowledged ops; \
                                 electing, but marking the log untrusted in every DoViewChange"
                            );
                            self.untrusted_log.set(true);
                            self.untrusted_view_change_timeouts.set(0);
                        }
                        actions.extend(
                            self.start_election(plane, ViewChangeReason::ViewProbeUnanswered),
                        );
                    } else {
                        self.timeouts
                            .borrow_mut()
                            .reset(TimeoutKind::RequestStartViewMessage);
                        actions.push(VsrAction::SendRequestStartView {
                            view: self.view.get(),
                            group: self.group,
                        });
                    }
                }
                Status::ViewChange if self.primary_index(self.view.get()) != self.replica => {
                    self.timeouts
                        .borrow_mut()
                        .reset(TimeoutKind::RequestStartViewMessage);
                    actions.push(VsrAction::SendRequestStartView {
                        view: self.view.get(),
                        group: self.group,
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

        if !history_fenced && timeouts.fired(TimeoutKind::ViewChangeStatus) {
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

        // Distinguish "primary died" from "primary moved to a view I missed".
        // If newer-view traffic has been arriving (a partition that healed
        // across an election, or a fresh/rejoined node that never saw the
        // election), the primary is alive and the timer only fired because a
        // matching-view heartbeat never reset it. Electing would be futile --
        // a lagging replica cannot win -- and would drag a healthy cluster
        // through a needless view change. Probe instead: the current primary
        // answers a `RequestStartView` with its live `StartView` regardless of
        // the probe's view stamp, and adopting it routes into journal repair
        // (and state transfer, if the gap fell below the peer's floor). The
        // probe re-broadcasts on its own timer, so no action is emitted here;
        // `Recovering` also makes this timeout inert until the probe resolves.
        if self.observed_newer_view.get() > self.view.get() {
            tracing::info!(
                replica = self.replica,
                namespace_raw = self.group,
                view = self.view.get(),
                observed_newer_view = self.observed_newer_view.get(),
                "heartbeat timed out behind a newer view; probing to catch up"
            );
            self.begin_view_probe();
            return Vec::new();
        }

        self.start_election(plane, ViewChangeReason::NormalHeartbeatTimeout)
    }

    /// Advance to `view + 1` and start a view change (own SVC counted).
    fn start_election(&self, plane: PlaneKind, reason: ViewChangeReason) -> Vec<VsrAction> {
        self.enter_view_change(plane, self.view.get() + 1, reason)
    }

    /// Enter `Status::ViewChange` at `new_view`: count this replica's own SVC,
    /// arm the view-change timers, and schedule the SVC broadcast.
    ///
    /// The own-SVC bookkeeping is a direct insert rather than an SVC delivered to
    /// self through the loopback. Deciding to change view is a local state
    /// transition, not a message this replica happens to address to itself, and
    /// it has to be atomic with the view/status writes above it: a self-message
    /// would land on a later pump drain, leaving a window where the replica has
    /// entered a view change without counting itself. On a solo group that window
    /// IS the whole quorum. `PrepareOk` loops through the loopback because it
    /// genuinely is a message to a peer that happens to be this replica.
    ///
    /// A timed-out primary candidate can become a backup in the next view,
    /// so every transition must restart its probe for a missing `StartView`.
    fn enter_view_change(
        &self,
        plane: PlaneKind,
        new_view: u32,
        reason: ViewChangeReason,
    ) -> Vec<VsrAction> {
        let old_view = self.view.get();

        self.view.set(new_view);
        self.status.set(Status::ViewChange);
        self.reset_view_change_state();
        self.unprove_superseded_merge();
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
            group: self.group,
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
            group: self.group,
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

        // NOT the snapshot the first send used: `build_do_view_change` re-reads
        // `local_dvc_suffix()`, whose tag can have moved since, in which case it
        // answers EMPTY, retracting every nack and body offer already sent.
        // Survivable only because `dvc_record` drops a duplicate real vote, so the
        // candidate keeps the first vote. Allow a retransmit to replace a seated vote
        // and this must pin the snapshot instead.
        let action = self.build_do_view_change(self.primary_index(self.view.get()));
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
        if self.untrusted_log.get() {
            self.warn_untrusted_halt();
        }

        self.enter_view_change(
            plane,
            self.view.get() + 1,
            ViewChangeReason::ViewChangeStatusTimeout,
        )
    }

    /// No view starts without a quorum of trusted logs or every replica
    /// reporting, which looks like an ordinary stuck view change unless it says
    /// so. Logged on the 1st, 2nd, 4th ... 64th escalation and every 64th after,
    /// so a halt that lasts hours stays visible without a line per timeout.
    fn warn_untrusted_halt(&self) {
        const STEADY_INTERVAL: u32 = 64;
        let timeouts = self.untrusted_view_change_timeouts.get().saturating_add(1);
        self.untrusted_view_change_timeouts.set(timeouts);
        let due = if timeouts <= STEADY_INTERVAL {
            timeouts.is_power_of_two()
        } else {
            timeouts.is_multiple_of(STEADY_INTERVAL)
        };
        if !due {
            return;
        }
        let view = self.view.get();
        let primary = self.primary_index(view);
        let awaited_do_view_change: Vec<u8> = if self.is_primary_for_view(view) {
            let received = self.do_view_change_from_all_replicas.borrow();
            (0..self.replica_count)
                .filter(|replica| {
                    *replica != self.replica && received[usize::from(*replica)].is_none()
                })
                .collect()
        } else {
            Vec::new()
        };
        tracing::warn!(
            replica = self.replica,
            namespace_raw = self.group,
            view,
            primary,
            view_change_timeouts = timeouts,
            ?awaited_do_view_change,
            "view change halted: this replica elected on a log that may lack acknowledged ops, \
             so no view starts until a quorum of replicas with intact logs, or every replica, \
             reports"
        );
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
        // The timer's lifecycle is maintained at the pipeline's own edges
        // (`push_prepare_entry` arms, `sync_prepare_timeout` disarms on empty and
        // restarts on a new head), so by the time this fires the timer is already
        // measuring the current oldest prepare rather than inheriting a drained
        // entry's elapsed ticks. The stop below is now a backstop for a pipeline
        // emptied by a path that skipped that maintenance, not the primary
        // disarm.
        //
        // TODO(prepare-timeout): special-case "all remote acks present, own
        // journal write is the laggard" by retrying the local write instead of
        // retransmitting to peers that already acked.
        //
        // Every early return below must stop or back off the timeout.
        // `fired()` stays true until the timer is rearmed, so returning
        // with the fired state intact turns the next pipeline push into
        // an instant spurious retransmit on the following tick (the push
        // sees `is_ticking` and does not restart the timer).
        if !self.is_acting_primary() {
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

        // A primary-by-index that has seen a newer view is stale: the cluster
        // elected past it while it was gone, and it booted primary-by-index at
        // an old view (a fresh or rejoined node that missed the election). It
        // has no `NormalHeartbeat` timeout to catch this -- it IS the heartbeat
        // sender -- so convert here instead of advertising a commit point no
        // peer will accept. The probe solicits the current `StartView`, which
        // demotes this replica to a backup and routes into repair / state
        // transfer. `begin_view_probe` stops this timer.
        if self.observed_newer_view.get() > self.view.get() {
            tracing::info!(
                replica = self.replica,
                namespace_raw = self.group,
                view = self.view.get(),
                observed_newer_view = self.observed_newer_view.get(),
                "stale primary-by-index behind a newer view; probing to catch up"
            );
            self.begin_view_probe();
            return Vec::new();
        }

        self.timeouts.borrow_mut().reset(TimeoutKind::CommitMessage);

        vec![self.commit_heartbeat()]
    }

    /// Ask the next tick to send `commit_min` to the backups instead of waiting
    /// for the `CommitMessage` heartbeat.
    ///
    /// A backup learns a commit from the next prepare or from that heartbeat,
    /// so on a quiet group it applies a committed op up to a heartbeat interval
    /// late. The tick sends a `Commit` to each backup whose `PrepareOk` shows it
    /// holds the op, and a backup that acks it later gets one on the tick after
    /// its ack. Requests and acks before a tick share one `Commit` per backup,
    /// and none is sent while the newest prepare already carries `commit_min`.
    pub fn request_eager_commit(&self) {
        if self.replica_count > 1 && self.is_acting_primary() {
            self.commit_announcements.borrow_mut().eager = self.commit_min.get();
            self.eager_commit.set(true);
        }
    }

    /// The `Commit`s owed since [`Self::request_eager_commit`], unless the
    /// backups learn `commit_min` from a prepare already sent.
    fn eager_commit_actions(&self) -> Vec<VsrAction> {
        // A stale primary leaves the probe to `handle_commit_message_timeout`.
        if !self.is_acting_primary() || self.has_observed_newer_view() {
            return Vec::new();
        }
        let commit_min = self.commit_min.get();
        let carried = self
            .pipeline
            .borrow()
            .entry_by_op(self.sequencer.current_sequence())
            .is_some_and(|entry| entry.header.commit >= commit_min);
        if carried {
            return Vec::new();
        }
        let mut announcements = self.commit_announcements.borrow_mut();
        (0..self.replica_count)
            .filter(|&replica| replica != self.replica)
            .filter_map(|replica| {
                let commit = announcements.take_owed(replica, commit_min)?;
                Some(self.commit_message(Some(replica), commit))
            })
            .collect()
    }

    fn commit_heartbeat(&self) -> VsrAction {
        // Don't advertise a commit point we haven't locally executed yet.
        // After view change the new primary may have commit_min < commit_max
        // until commit_journal catches up. Send commit_min (what we've
        // actually applied) so backups don't advance past us.
        let commit = self.commit_min.get();
        self.commit_announcements
            .borrow_mut()
            .record_broadcast(commit);
        self.commit_message(None, commit)
    }

    fn commit_message(&self, target: Option<u8>, commit: u64) -> VsrAction {
        let ts = self.heartbeat_watermark.get().1 + 1;
        self.heartbeat_watermark.set((self.view.get(), ts));

        VsrAction::SendCommit {
            view: self.view.get(),
            commit,
            group: self.group,
            timestamp_monotonic: ts,
            target,
        }
    }

    /// Handle a received `StartViewChange` message.
    ///
    /// "When replica i receives STARTVIEWCHANGE messages for its view-number
    /// from f OTHER replicas, it sends a DOVIEWCHANGE message to the node
    /// that will be the primary in the new view."
    ///
    /// # Panics
    /// If `header.group` does not match this replica's namespace.
    pub fn handle_start_view_change(
        &self,
        plane: PlaneKind,
        header: &StartViewChangeHeader,
    ) -> Vec<VsrAction> {
        assert_eq!(header.group, self.group, "SVC routed to wrong group");
        // A recovering replica is quorum-invisible: it lost (or cannot trust)
        // its durable state, so it must not vote history into existence. The
        // election proceeds among the peers; its conclusion reaches this
        // replica via StartView, which recovery accepts. The empty vote
        // carries no history, so it is no exception.
        if self.status.get() == Status::Recovering {
            return self
                .empty_do_view_change(plane, header.view)
                .into_iter()
                .collect();
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
            actions.extend(self.enter_view_change(
                plane,
                msg_view,
                ViewChangeReason::ReceivedStartViewChange,
            ));
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
            let commit = self.dvc_commit();

            // Start DVC timeout
            self.timeouts
                .borrow_mut()
                .start(TimeoutKind::DoViewChangeMessage);

            let action = self.build_do_view_change(primary_candidate);
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
                    log_view: self.log_view_claim(),
                    op: current_op,
                    commit,
                    suffix: self.local_dvc_suffix(),
                    untrusted_log: self.untrusted_log.get(),
                };
                dvc_record(
                    &mut self.do_view_change_from_all_replicas.borrow_mut(),
                    own_dvc,
                );

                // `complete_view_change_as_primary` latches only once the merge
                // decides, so an undecidable quorum stays open to later DVCs.
                if !self.do_view_change_quorum.get()
                    && dvc_count(&self.do_view_change_from_all_replicas.borrow())
                        >= self.quorum_view_change()
                {
                    actions.extend(self.complete_view_change_as_primary(plane));
                }
            }
        }

        actions
    }

    /// The `commit` this replica advertises in a `DoViewChange`, and in any
    /// `StartView` it announces.
    ///
    /// `commit_max`, not `commit_min`: the new primary floors its pipeline rebuild
    /// at `max(commit)` across the quorum, and only `commit_max` bounds that range
    /// to the pipeline depth. `commit_min` can lag far enough to overflow the
    /// rebuild; `CommitJournal` replays the committed-but-unapplied tail instead.
    ///
    /// Clamped to `op`, since a backup learns `commit_max` from a heartbeat before
    /// the prepares and `DoViewChangeHeader::validate` rejects `commit > op`.
    /// Lossless for the rebuild floor: quorum intersection guarantees some sender
    /// whose head covers the true commit point carries it.
    #[must_use]
    pub fn dvc_commit(&self) -> u64 {
        let op = self.sequencer.current_sequence();
        self.commit_max.get().min(op)
    }

    /// Build this replica's `DoViewChange` for the current view.
    fn build_do_view_change(&self, target: u8) -> VsrAction {
        VsrAction::SendDoViewChange {
            view: self.view.get(),
            target,
            log_view: self.log_view_claim(),
            op: self.sequencer.current_sequence(),
            commit: self.dvc_commit(),
            group: self.group,
            suffix: self.local_dvc_suffix(),
            untrusted_log: self.untrusted_log.get(),
        }
    }

    /// The `DoViewChange` a recovering replica barred from electing sends in
    /// answer to a `StartViewChange` for `view`, or `None` when it must stay
    /// silent. Never in answer to a `DoViewChange`: see `handle_do_view_change`.
    ///
    /// Without it a group whose every log is untrusted could never start a
    /// view, since that takes every replica reporting and a barred replica
    /// waits for a `StartView` instead. The vote holds no log: flagged
    /// untrusted, at `log_view` 0 with nothing in it, it nacks nothing, offers
    /// nothing and commits nothing, so it counts toward every replica reporting
    /// and decides nothing else. It also leaves this replica `Recovering` in its
    /// own view and never makes it a candidate: it waits for the `StartView`
    /// and the state transfer it was waiting for already.
    ///
    /// Only where a recovered log can be untrusted. Elsewhere a quorum of
    /// trusted peers never needs it.
    fn empty_do_view_change(&self, plane: PlaneKind, view: u32) -> Option<VsrAction> {
        if self.recovered_log_complete.get()
            || self.recovery_election_allowed.get()
            || view < self.view.get()
            || self.is_primary_for_view(view)
        {
            return None;
        }
        let action = VsrAction::SendDoViewChange {
            view,
            target: self.primary_index(view),
            log_view: 0,
            op: 0,
            commit: 0,
            group: self.group,
            suffix: DvcSuffix::empty(),
            untrusted_log: true,
        };
        emit_sim_event(
            SimEventKind::ControlMessageScheduled,
            &ControlActionLogEvent::from_vsr_action(
                ReplicaLogContext::from_consensus(self, plane),
                &action,
            ),
        );
        Some(action)
    }

    /// Decode a peer's suffix, or `None` to drop the whole `DoViewChange`.
    ///
    /// A suffix that will not decode makes the numbers untrustworthy too. Dropping
    /// the message lets the sender's retransmit try again, rather than seating a
    /// vote whose nacks and offered bodies cannot be placed against an op.
    fn decode_peer_suffix(
        &self,
        header: &DoViewChangeHeader,
        suffix_body: &[u8],
    ) -> Option<DvcSuffix> {
        match dvc_suffix_decode(
            suffix_body,
            header.op,
            header.nack_bitset,
            header.present_bitset,
        ) {
            Ok(suffix) => Some(suffix),
            Err(error) => {
                tracing::warn!(
                    replica = self.replica,
                    from_replica = header.replica,
                    view = header.view,
                    op = header.op,
                    "dropping do_view_change with an unreadable suffix: {error}"
                );
                None
            }
        }
    }

    /// `suffix_body` is the sender's uncommitted-suffix headers. Empty from a peer
    /// unable to snapshot one, which then contributes numbers only.
    ///
    /// # Panics
    /// If `header.group` does not match this replica's namespace.
    pub fn handle_do_view_change(
        &self,
        plane: PlaneKind,
        header: &DoViewChangeHeader,
        suffix_body: &[u8],
    ) -> Vec<VsrAction> {
        assert_eq!(header.group, self.group, "DVC routed to wrong group");
        // Quorum-invisible while recovering (see handle_start_view_change):
        // a recovering replica must not collect DVCs and crown itself. Nor does
        // it answer one with its empty vote: DVCs are broadcast, so two barred
        // replicas would answer each other's empty votes forever. The
        // candidate's SVC retransmits keep soliciting the vote instead.
        if self.status.get() == Status::Recovering {
            return Vec::new();
        }
        let from_replica = header.replica;
        let msg_view = header.view;
        let msg_log_view = header.log_view;
        let msg_op = header.op;
        let msg_commit = header.commit;
        let Some(msg_suffix) = self.decode_peer_suffix(header, suffix_body) else {
            return Vec::new();
        };

        // Ignore DVCs for old views
        if msg_view < self.view.get() {
            return Vec::new();
        }

        let mut actions = Vec::new();

        // If DVC is for a higher view, advance to that view
        if msg_view > self.view.get() {
            actions.extend(self.enter_view_change(
                plane,
                msg_view,
                ViewChangeReason::ReceivedDoViewChange,
            ));
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
                log_view: self.log_view_claim(),
                op: current_op,
                commit,
                suffix: self.local_dvc_suffix(),
                untrusted_log: self.untrusted_log.get(),
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
            suffix: msg_suffix,
            untrusted_log: header.untrusted_log(),
        };
        // A retransmit changes nothing the last merge did not already see.
        if !dvc_record(&mut self.do_view_change_from_all_replicas.borrow_mut(), dvc) {
            return actions;
        }

        // `complete_view_change_as_primary` latches only once the merge decides,
        // so an undecidable quorum re-merges as each further DVC lands.
        if !self.do_view_change_quorum.get()
            && dvc_count(&self.do_view_change_from_all_replicas.borrow())
                >= self.quorum_view_change()
        {
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
            namespace_raw = self.group,
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

    /// Arm state transfer for a cluster restart: the replica will replace
    /// its snapshot-shaped state from the live primary the view probe finds
    /// (the shard's transfer session drives the stage from there). If the
    /// probe exhausts instead -- full-cluster bootstrap, nobody to fetch
    /// from -- the election fallback clears the stage and local recovery
    /// stands.
    pub fn begin_state_transfer_await(&self) {
        self.set_state_transfer_stage(StateTransferStage::AwaitingTarget);
    }

    /// Record that a message stamped with `view` arrived that this replica
    /// could not process because the view is ahead of its own. Monotone: only
    /// a value strictly above the current record moves it. Called from the two
    /// ingress paths that drop newer-view traffic (`handle_commit` and
    /// `replicate_preflight`); read by the heartbeat-timeout handler to choose
    /// catch-up over election.
    pub fn observe_newer_view(&self, view: u32) {
        if view > self.observed_newer_view.get() {
            self.observed_newer_view.set(view);
        }
    }

    #[must_use]
    pub const fn state_transfer_stage(&self) -> StateTransferStage {
        self.state_transfer_stage.get()
    }

    /// # Panics
    /// On an illegal stage transition (see
    /// [`StateTransferStage::valid_transition`]).
    pub fn set_state_transfer_stage(&self, to: StateTransferStage) {
        let from = self.state_transfer_stage.get();
        assert!(
            StateTransferStage::valid_transition(from, to),
            "state transfer stage transition {from:?} -> {to:?} is illegal"
        );
        tracing::info!(
            replica = self.replica,
            namespace_raw = self.group,
            ?from,
            ?to,
            "state transfer stage"
        );
        self.state_transfer_stage.set(to);
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
            header.group, self.group,
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
        // Echo the requester's incarnation so it can prove this StartView
        // post-dates its restart (a probe reply, not a stale in-flight message).
        // Addressed to the requester alone: the nonce proves freshness for that
        // replica only, and a peer recovering at the same time would read it as
        // foreign and reject a StartView that is in fact current.
        vec![VsrAction::SendStartView {
            view: self.view.get(),
            op: self.sequencer.current_sequence(),
            commit: self.dvc_commit(),
            incarnation: header.incarnation,
            target: Some(header.replica),
            // A prober can hold a different entry at an op at or below this
            // commit point, left from a view whose entry the change truncated.
            // Without the canonical headers it cannot know, so it adopts the
            // commit point and applies its own stale entry.
            suffix: self.local_dvc_suffix().headers().to_vec(),
            group: self.group,
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
        // The view-log proof was taken from the old commit point. A missing op the
        // floor now covers is applied state that peers have compacted: every repair
        // window opened at it draws `RangeEvicted`, and the proof it would close
        // keeps the commit walk fenced. Drop it so the plane re-derives the proof
        // from the new floor.
        if self
            .view_log_missing_op
            .get()
            .is_some_and(|missing| missing <= floor)
        {
            self.view_log_missing_op.set(None);
            self.view_log_unproven_through.set(u64::MAX);
            self.invalidate_local_dvc_suffix();
        }
    }

    fn finish_view_probe(&self) {
        self.probe_attempts.set(0);
        self.timeouts
            .borrow_mut()
            .stop(TimeoutKind::RequestStartViewMessage);
    }

    /// Decide which head to adopt from a `StartView`, and record the view's
    /// canonical headers when it carried any.
    ///
    /// Headers go in `pending_view_log`, not the journal: a journal entry is a
    /// header plus its body, and a backup adopting a view usually holds neither.
    /// Keeping them lets the repair ingest reject a body that disagrees with what
    /// the view decided, which is what makes fetching by op number safe.
    ///
    /// Falls back to the announced `op` on an empty body (probe answer, stale-view
    /// correction).
    fn adopt_start_view_suffix(&self, header: &StartViewHeader, suffix: &DvcSuffix) -> u64 {
        let headers = suffix.headers();
        if headers.is_empty() && !self.view_log_unproven() {
            return header.op;
        }
        *self.pending_view_log.borrow_mut() = Some(MergedLog {
            op_head: header.op,
            commit_max: header.commit,
            headers: headers.to_vec(),
            committed_elsewhere: Vec::new(),
        });
        header.op
    }

    /// True when `headers` name a different prepare than this replica applied at
    /// some op at or below `commit_min`. The caller refuses the log that carried
    /// them.
    ///
    /// Quorum intersection puts every committed op in each later view's log, so a
    /// mismatch means a quorum decided a log that contradicts a committed op.
    /// Adopting it, or reading its prepare as an old retransmit, hides the loss.
    ///
    /// `applied_at` answers the header of the prepare this replica applied at an
    /// op, `None` once the journal no longer holds it. Only ops it answers for are
    /// compared, and an unsealed checksum on either side carries no identity, so
    /// a healthy replica never trips this.
    ///
    /// `view` is the view of the message that carried the log. The first
    /// contradiction in each newer view is logged with both checksums and
    /// counted. A redelivered `StartView` or a retransmitted prepare of that view
    /// is refused without either, so a peer that keeps resending cannot flood
    /// the log.
    pub fn contradicts_applied_log<'a>(
        &self,
        plane: PlaneKind,
        source: ContradictionSource,
        view: u32,
        headers: impl IntoIterator<Item = &'a PrepareHeader>,
        applied_at: impl Fn(u64) -> Option<PrepareHeader>,
    ) -> bool {
        let commit_min = self.commit_min();
        let contradiction = headers
            .into_iter()
            .filter(|header| {
                header.op <= commit_min && dvc_header_kind(header) == DvcHeaderKind::Valid
            })
            .find_map(|header| {
                applied_at(header.op)
                    .filter(|applied| {
                        applied.checksum != header.checksum
                            && applied.checksum != CHECKSUM_UNSEALED
                            && header.checksum != CHECKSUM_UNSEALED
                    })
                    .map(|applied| (header, applied))
            });
        let Some((header, applied)) = contradiction else {
            return false;
        };
        if self
            .contradiction_reported_view
            .get()
            .is_some_and(|reported| reported >= view)
        {
            return true;
        }
        self.contradiction_reported_view.set(Some(view));
        self.applied_log_contradictions
            .set(self.applied_log_contradictions.get().saturating_add(1));
        tracing::error!(
            plane = plane.as_str(),
            group = self.group,
            replica = self.replica,
            view = self.view.get(),
            message_view = view,
            ?source,
            op = header.op,
            commit_min,
            applied_checksum = applied.checksum,
            announced_checksum = header.checksum,
            "refusing a log that names a different prepare at op {} than this replica \
             applied: a quorum decided a log that contradicts a committed op",
            header.op
        );
        true
    }

    /// Views whose logs [`Self::contradicts_applied_log`] refused since boot.
    #[must_use]
    pub const fn applied_log_contradictions(&self) -> u64 {
        self.applied_log_contradictions.get()
    }

    /// Views counted by [`Self::applied_log_contradictions`] since the last
    /// call. The total keeps counting, so invariants still read it whole.
    #[must_use]
    pub const fn take_applied_log_contradictions(&self) -> u64 {
        let total = self.applied_log_contradictions.get();
        total - self.applied_log_contradictions_taken.replace(total)
    }

    /// [`Self::handle_start_view_guarded`] for a caller that cannot read back the
    /// prepares it applied.
    ///
    /// # Panics
    /// If `header.group` does not match this replica's namespace.
    pub fn handle_start_view(
        &self,
        plane: PlaneKind,
        header: &StartViewHeader,
        suffix_body: &[u8],
    ) -> Vec<VsrAction> {
        self.handle_start_view_guarded(plane, header, suffix_body, |_| None)
            .unwrap_or_default()
    }

    /// [`Self::try_handle_start_view`] for a caller that only needs to know
    /// whether the `StartView` was refused.
    ///
    /// # Panics
    /// If `header.group` does not match this replica's namespace.
    pub fn handle_start_view_guarded(
        &self,
        plane: PlaneKind,
        header: &StartViewHeader,
        suffix_body: &[u8],
        applied_at: impl Fn(u64) -> Option<PrepareHeader>,
    ) -> Option<Vec<VsrAction>> {
        self.try_handle_start_view(plane, header, suffix_body, applied_at)
            .ok()
    }

    /// Handle a received `StartView` message (backups only).
    ///
    /// "When other replicas receive the STARTVIEW message, they replace their log
    /// with the one in the message, set their op-number to that of the latest entry
    /// in the log, set their view-number to the view number in the message, change
    /// their status to normal, and send `PrepareOK` for any uncommitted ops."
    ///
    /// # Panics
    /// If `header.group` does not match this replica's namespace.
    /// # Client-table maintenance
    ///
    /// Backups maintain the client-table during normal operation via
    /// `commit_journal` in `on_replicate`, which walks the WAL and updates
    /// the client table for each committed op. The WAL survives view changes,
    /// so the new primary can process any committed op it received.
    ///
    /// Gap: if a backup never received a prepare (lost message),
    /// `commit_journal` stops at the gap. Requires message repair.
    /// `suffix_body` is the message body: the view's canonical headers, empty
    /// when the announcement carries numbers only.
    ///
    /// # Errors
    /// Refuses a `StartView` that would be adopted, and nothing changed: its
    /// body is malformed or names another group, or its headers contradict a
    /// prepare `applied_at` reports (see [`Self::contradicts_applied_log`]).
    #[expect(
        clippy::too_many_lines,
        reason = "keep StartView validation and both commit paths together"
    )]
    pub fn try_handle_start_view(
        &self,
        plane: PlaneKind,
        header: &StartViewHeader,
        suffix_body: &[u8],
        applied_at: impl Fn(u64) -> Option<PrepareHeader>,
    ) -> Result<Vec<VsrAction>, StartViewRefusal> {
        assert_eq!(header.group, self.group, "SV routed to wrong group");
        let from_replica = header.replica;
        let msg_view = header.view;
        let msg_op = header.op;
        let msg_commit = header.commit;

        // Verify sender is the primary for this view
        if self.primary_index(msg_view) != from_replica {
            return Ok(Vec::new());
        }

        // Ignore old views
        if msg_view < self.view.get() {
            return Ok(Vec::new());
        }

        // Incarnation guard. While Recovering (probing after a restart), only adopt
        // a StartView that is provably post-restart: a strictly newer view, a head
        // at or past ours, or one echoing our current incarnation (a reply to our
        // own probe). Otherwise it may be addressed to a previous incarnation still
        // in flight, and adopting it could install a stale head and let this replica
        // act in a view it will not remember after another crash. Inert when no
        // incarnation is set (partition plane, tests).
        //
        // A zero `header.incarnation` makes no claim either way: it is what an
        // unsolicited StartView carries.
        // Classifying it stale would have this replica reject a current StartView
        // from a healthy primary purely because that primary is older, so it falls
        // through to the view checks that governed before the field existed.
        let self_incarnation = self.incarnation.get();
        if self.status.get() == Status::Recovering
            && self_incarnation != 0
            && header.incarnation != 0
            && msg_view <= self.view.get()
            && msg_op < self.sequencer.current_sequence()
            && header.incarnation != self_incarnation
        {
            tracing::debug!(
                replica = self.replica,
                view = msg_view,
                op = msg_op,
                incarnation = header.incarnation,
                self_incarnation,
                "ignoring StartView while recovering: stale incarnation"
            );
            return Ok(Vec::new());
        }

        // Within one view the primary's log only grows. A Normal replica already
        // in that log_view must keep its acknowledged head when a StartView arrives
        // late or forwarded prepares overtake it. Only its commit watermark can advance.
        // Normal only: a restarted replica re-derives its head from a journal that
        // can still hold a suffix the view discarded. Skipping on that head would
        // drop the answer to its own probe, and its election would push the
        // discarded suffix back.
        let head = self.sequencer.current_sequence();
        if self.status.get() == Status::Normal
            && !self.view_log_unproven()
            && msg_view == self.log_view.get()
            && msg_op <= head
        {
            if msg_commit > self.commit_max() {
                self.advance_commit_max(msg_commit);
                return Ok(vec![VsrAction::CommitJournal]);
            }
            return Ok(Vec::new());
        }

        // Adoption may discard an uncommitted WAL suffix, never a committed prefix. A
        // newer view's head covers every op committed before that view, the skip
        // above keeps what a Normal replica acked in its view, and this guard keeps
        // what this replica knows is committed. A restarted replica relies on no
        // StartView sent before its restart reaching it; the incarnation guard
        // enforces that for probe answers when incarnations are set.
        //
        // Adoption can leave the WAL above the adopted head. Consensus is sans-io and
        // cannot truncate it; the plane sweeps it on every adoption
        // (`reconcile_{metadata,partition}_view_divergence`, above-head branch)
        // before the primary's next prepare can land on a relic.
        if msg_op < self.commit_max() {
            tracing::warn!(
                group = self.group,
                replica = self.replica,
                current_view = self.view.get(),
                view = msg_view,
                op = msg_op,
                commit_min = self.commit_min(),
                commit_max = self.commit_max(),
                "ignoring StartView that omits known committed entries"
            );
            return Ok(Vec::new());
        }

        // We shouldn't process our own StartView
        if from_replica == self.replica {
            return Ok(Vec::new());
        }

        let suffix = match dvc_suffix_decode(suffix_body, msg_op, 0, 0) {
            Ok(suffix) => suffix,
            Err(error) => {
                tracing::warn!(view = msg_view, op = msg_op, %error, "refusing malformed StartView evidence");
                return Err(StartViewRefusal::InvalidEvidence);
            }
        };
        let adopted = crate::AdoptedView {
            view: msg_view,
            group: self.group,
            partition_incarnation: self.partition_incarnation.get(),
            op_head: msg_op,
            commit_max: msg_commit,
            headers: suffix.headers().to_vec(),
        };
        if let Err(error) = adopted.validate(self.cluster, msg_view) {
            tracing::warn!(view = msg_view, op = msg_op, %error, "refusing mismatched StartView evidence");
            return Err(StartViewRefusal::InvalidEvidence);
        }
        if self.contradicts_applied_log(
            plane,
            ContradictionSource::StartView,
            msg_view,
            suffix.headers(),
            applied_at,
        ) {
            return Err(StartViewRefusal::ContradictsAppliedLog(
                adopted.merged_log(),
            ));
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
        let needs_reconciliation = self.view_log_unproven.get()
            || self.status.get() != Status::Normal
            || self.log_view.get() != msg_view;
        if self.group != METADATA_GROUP && !self.view_log_unproven.get() {
            self.log_view_proven.set(self.log_view.get());
        }
        self.view.set(msg_view);
        self.log_view.set(msg_view);
        self.status.set(Status::Normal);
        self.rejoined_on_recovered_log.set(false);
        self.ceded_primaryship.set(false);
        // The view's log comes from a settled primary, but until repair fills this
        // replica's holes from it the log can still lack acknowledged ops, so a
        // later view change must not count it as trusted.
        if !needs_reconciliation {
            self.untrusted_log.set(false);
        }
        self.advance_commit_max(msg_commit);
        self.reset_view_change_state();

        // Stale pipeline entries from the old view must be discarded
        self.view_log_unproven.set(needs_reconciliation);
        self.view_log_unproven_through.set(u64::MAX);
        self.view_log_missing_op.set(None);
        self.pipeline.borrow_mut().clear();

        // Cross-check the announced head against the headers published with it: a
        // suffix head disagreeing with `header.op` means an inconsistently built
        // frame, and either value leaves this replica chasing an unservable head.
        let announced = self.adopt_start_view_suffix(header, &suffix);
        *self.adopted_view.borrow_mut() = Some(adopted);
        self.adopted_view_dirty.set(true);
        self.sequencer.set_sequence(announced);
        // Settle a gated suffix's fate as a backup too, so a later election inherits
        // a decided barrier rather than a latched one.
        self.redecide_recovery_barrier(announced);

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
                group: self.group,
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
        Ok(actions)
    }

    /// Handle a `Commit` (heartbeat) message from the primary.
    ///
    /// Advances `commit_max` and resets the backup's `NormalHeartbeat` timeout
    /// so it doesn't start a spurious view change. Returns `true` if
    /// `commit_max` advanced, signalling the caller to run `commit_journal`.
    ///
    /// Only refreshes liveness for a newer heartbeat within its view, so
    /// old/replayed messages cannot suppress view changes.
    ///
    /// # Panics
    /// If `header.group` does not match this replica's namespace.
    pub fn handle_commit(&self, header: &iggy_binary_protocol::CommitHeader) -> CommitOutcome {
        assert_eq!(header.group, self.group, "Commit routed to wrong group");

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
            // A "primary" hearing a NEWER view is stale-by-index: the cluster
            // elected past it. Record so its heartbeat-send timer converts to
            // a probe (see `handle_commit_message_timeout`).
            if header.view > self.view.get() {
                self.observe_newer_view(header.view);
            }
            return CommitOutcome::Accepted;
        }

        if self.status.get() != Status::Normal {
            return CommitOutcome::Accepted;
        }

        if header.view != self.view.get() {
            // A heartbeat from a newer view is proof the cluster elected past
            // this replica. Record it so the heartbeat-timeout handler probes
            // to catch up instead of electing (this same heartbeat cannot
            // reset the timer -- the reset below is gated on a matching view).
            if header.view > self.view.get() {
                self.observe_newer_view(header.view);
            }
            return CommitOutcome::Accepted;
        }

        // Tolerant skip, not an assert: the replica handshake proves cluster
        // membership only, so nothing binds `header.replica` to the sender.
        if header.replica != self.primary_index(header.view) {
            return CommitOutcome::Accepted;
        }

        let heartbeat = (header.view, header.timestamp_monotonic);
        if self.heartbeat_watermark.get() < heartbeat {
            self.heartbeat_watermark.set(heartbeat);
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
    ///
    /// Re-entrant, called again for every `DoViewChange` landing while the merge is
    /// undecided. Every non-`Ready` outcome leaves this replica untouched, so a
    /// re-run costs only the merge.
    fn complete_view_change_as_primary(&self, plane: PlaneKind) -> Vec<VsrAction> {
        let merged = {
            let dvc_array = self.do_view_change_from_all_replicas.borrow();
            merge_view_change(&dvc_array, self.merge_quorums())
        };

        let merged = match merged {
            MergeOutcome::Ready(merged) => merged,
            // Every non-ready outcome keeps this replica in `ViewChange` with its
            // log untouched. Picking a winner unconditionally and letting the
            // pipeline rebuild truncate what it cannot find locally discards
            // committed ops; an unavailable cluster that says so is the better
            // failure.
            //
            // None of these latch `do_view_change_quorum`: an undecidable quorum is
            // not a decision, and the replicas still to report are what would
            // settle it. The flag belongs only where the quorum is decidable.
            MergeOutcome::AwaitingQuorum => return Vec::new(),
            MergeOutcome::AwaitingRepair { undecided_op } => {
                tracing::debug!(
                    replica = self.replica,
                    view = self.view.get(),
                    undecided_op,
                    "view change waiting on more DoViewChange messages to decide an op"
                );
                return Vec::new();
            }
            MergeOutcome::Deadlocked { undecided_op } => {
                tracing::error!(
                    replica = self.replica,
                    view = self.view.get(),
                    undecided_op,
                    "view change cannot start: op {undecided_op} is neither recoverable from any \
                     replica nor provably uncommitted"
                );
                return Vec::new();
            }
        };

        // The pipeline must hold the whole uncommitted range, and the merge decides
        // that range against a cluster-wide ceiling, so a node configured shallower
        // than its peers can be handed a range it cannot rebuild.
        //
        // Refuse rather than panic: a further DoViewChange can raise `commit_max`
        // and shrink the range, and otherwise the status timeout escalates. A panic
        // would restart into the same merge.
        if merged.op_head.saturating_sub(merged.commit_max) > self.prepare_queue_max as u64 {
            tracing::error!(
                replica = self.replica,
                view = self.view.get(),
                commit_max = merged.commit_max,
                op_head = merged.op_head,
                prepare_queue_max = self.prepare_queue_max,
                "view change cannot start: the merged log claims {} in-flight ops, more than this \
                 replica's pipeline holds; refusing the view",
                merged.op_head - merged.commit_max,
            );
            return Vec::new();
        }

        // Quorum closed now the merge decided: re-merging after parking could
        // produce a different log than the one already being repaired toward.
        self.do_view_change_quorum.set(true);

        // The merged log is authoritative but this replica may not hold every body
        // yet. Park it, let the shard repair up to it, and `start_pending_view`
        // finishes once the journal covers the range. Until then this replica stays
        // in `ViewChange` and prepares nothing, so no client op is stamped onto an
        // unproven log. `log_view` does NOT advance here; see `start_pending_view`.
        let max_commit = merged.commit_max;
        let new_op = merged.op_head;
        self.advance_commit_max(max_commit);
        *self.pending_view_log.borrow_mut() = Some(merged);
        self.reopen_view_log_proof();

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

        tracing::info!(
            replica = self.replica,
            view = self.view.get(),
            op_head = new_op,
            commit_max = max_commit,
            "view-change quorum merged; repairing up to the merged log before starting the view"
        );
        emit_replica_event(
            SimEventKind::ReplicaStateChanged,
            &ReplicaLogContext::from_consensus(self, plane),
        );

        // No sends yet: `SendStartView` promises this replica can serve every op in
        // the merged log, and a backup adopting the announced head asks it for the
        // bodies behind it.
        Vec::new()
    }

    /// Sizes handed to the DVC merge. `merge_view_change` raises the quorum to
    /// every replica, and drops absence nacks, when too few senders trust their
    /// logs.
    const fn merge_quorums(&self) -> MergeQuorums {
        MergeQuorums {
            view_change: self.quorum_view_change(),
            nack_prepare: self.quorum_nack_prepare(),
            replica_count: self.replica_count as usize,
            // The cluster-wide ceiling, not `self.prepare_queue_max`: this node's
            // config says nothing about how deep a peer's pipeline is.
            prepare_queue_ceiling: PREPARE_QUEUE_CEILING as u64,
            absence_nacks: true,
        }
    }

    /// The merged log this replica is repairing toward, if a view change is
    /// mid-transition. The shard reads it for the op range it must cover before the
    /// view can start, and for which peers offered the bodies.
    ///
    /// Clones two `Vec<PrepareHeader>`. Prefer [`Self::view_log_is_pending`] /
    /// [`Self::with_pending_view_log`]; clone only to hold it across an `.await` or
    /// across [`Self::start_pending_view`], which takes the cell.
    #[must_use]
    pub fn pending_view_log(&self) -> Option<MergedLog> {
        self.pending_view_log.borrow().clone()
    }

    /// The log the last adopted `StartView` announced, while this replica's
    /// journal does not hold it yet. Unlike the parked copy it survives view
    /// changes, and it is never this replica's own merge. A settled merge that
    /// cleared the WAL certificate reads as unproven here
    /// (`uncertified_merge_settled`).
    #[must_use]
    pub fn adopted_view_log(&self) -> Option<MergedLog> {
        (self.view_log_unproven() || self.uncertified_merge_settled())
            .then(|| {
                self.adopted_view
                    .borrow()
                    .as_ref()
                    .map(crate::AdoptedView::merged_log)
            })
            .flatten()
    }

    /// Whether the accepted `StartView` names `header`'s prepare identity.
    #[must_use]
    pub fn adopted_view_vouches_for(&self, header: &PrepareHeader) -> bool {
        self.adopted_view
            .borrow()
            .as_ref()
            .is_some_and(|adopted| adopted.vouches_for(header))
    }

    /// The accepted `StartView`'s view and advertised head, including after repair.
    #[must_use]
    pub fn adopted_view_head(&self) -> Option<(u32, u64)> {
        self.adopted_view
            .borrow()
            .as_ref()
            .map(|adopted| (adopted.view, adopted.op_head))
    }

    /// Mark only the evidence captured by the completed write. Adoption may have
    /// advanced while the writer awaited disk I/O.
    pub fn mark_adopted_view_durable(&self, written: Option<&crate::AdoptedView>) {
        if self.adopted_view.borrow().as_ref() == written {
            self.adopted_view_dirty.set(false);
        }
    }

    /// Load validated evidence before joining the message pump. Body recovery
    /// follows separately, then `resume_adopted_view` installs its repair fence.
    pub fn restore_adopted_view(&self, adopted: Option<&crate::AdoptedView>) {
        *self.adopted_view.borrow_mut() = adopted.cloned();
        self.adopted_view_dirty.set(false);
    }

    /// Re-establish the announced head after recovering the physical body log.
    pub fn resume_adopted_view(&self) {
        let adopted = self.adopted_view.borrow();
        let Some(adopted) = adopted.as_ref() else {
            return;
        };
        let proven = if self.group == METADATA_GROUP {
            self.history_certificate
                .get()
                .map_or(0, |certificate| certificate.view)
        } else {
            self.log_view_proven.get()
        };
        if adopted.view > proven {
            self.log_view.set(adopted.view);
            self.view_log_unproven.set(true);
            self.view_log_unproven_through.set(u64::MAX);
            self.view_log_missing_op.set(None);
            self.sequencer.set_sequence(adopted.op_head);
            *self.pending_view_log.borrow_mut() = Some(adopted.merged_log());
            self.advance_commit_max(adopted.commit_max);
            self.invalidate_local_dvc_suffix();
        }
    }

    #[must_use]
    pub const fn view_log_unproven(&self) -> bool {
        self.view_log_unproven.get()
    }

    /// Whether the proven journal is a primary-elect's merge that `log_view` does
    /// not name yet. A replica superseded before it starts the view keeps the
    /// merge under its older `log_view` until a later proof settles. Only a
    /// partition whose merge cleared the WAL certificate goes back to unproven.
    #[must_use]
    pub const fn view_log_merged(&self) -> bool {
        self.view_log_merged.get()
    }

    pub fn settle_view_log_proof(&self) {
        self.view_log_unproven.set(false);
        self.untrusted_log.set(false);
        self.view_log_merged.set(self.is_primary_elect());
        self.view_log_merge_uncertified.set(false);
        if self.group != METADATA_GROUP {
            self.log_view_proven.set(self.log_view.get());
        }
        self.invalidate_local_dvc_suffix();
    }

    /// The merge that just settled cleared the WAL certificate of `log_view`, by
    /// a truncation or by an entry of a later view. A superseding view change
    /// then makes this replica unproven, so the snapshot follows that rule now.
    pub fn note_uncertified_merge(&self) {
        self.view_log_merge_uncertified.set(true);
        self.invalidate_local_dvc_suffix();
    }

    /// Enter the unproven state, keeping the current `log_view` as the claim when
    /// it was proven.
    fn mark_view_log_unproven(&self) {
        if self.group != METADATA_GROUP && !self.view_log_unproven.get() {
            self.log_view_proven.set(self.log_view.get());
        }
        self.view_log_unproven.set(true);
    }

    /// Unproven above `commit_min`, with no gap named until the shard runs the
    /// proof again.
    fn reopen_view_log_proof(&self) {
        self.mark_view_log_unproven();
        self.view_log_unproven_through.set(u64::MAX);
        self.view_log_missing_op.set(None);
    }

    /// A partition primary-elect whose proven merge cleared the WAL certificate of
    /// `log_view`, before the view starts. Its DVC snapshot is built as if
    /// unproven: a superseding view change makes it unproven and votes with the
    /// snapshot taken before.
    const fn uncertified_merge_settled(&self) -> bool {
        self.group != METADATA_GROUP
            && self.view_log_merged.get()
            && self.view_log_merge_uncertified.get()
            && !self.view_log_unproven.get()
    }

    /// The journal holds a merge that no view started, and the merge cleared the
    /// WAL certificate of `log_view`. The superblock gate then refuses every
    /// write. Go back to the state of an elect superseded before its proof
    /// settled, whose superblock records the older claim with no certificate.
    /// An elect whose WAL still certifies `log_view` stays proven and votes with
    /// its merged journal, which is also what a restart from that WAL restores.
    ///
    /// The snapshot stays. It was built over this same range and stitch
    /// (`uncertified_merge_settled`), and the vote this handler may cast reads it
    /// before the shard can refresh it. An empty suffix from a canonical sender
    /// lets every lower-`log_view` copy count as a nack (`tally_op`).
    fn unprove_superseded_merge(&self) {
        if self.uncertified_merge_settled() {
            self.view_log_merged.set(false);
            self.reopen_view_log_proof();
        }
    }

    pub fn fence_view_log(&self, op: u64, unproven_through: u64) {
        self.mark_view_log_unproven();
        self.view_log_unproven_through.set(unproven_through);
        self.view_log_missing_op.set(Some(op));
        self.invalidate_local_dvc_suffix();
    }

    #[must_use]
    pub const fn view_log_entry_proven(&self, op: u64) -> bool {
        !self.view_log_unproven()
            || op <= self.commit_min()
            || op > self.view_log_unproven_through.get()
    }

    /// Whether `view_log_proof` still has to walk `op` before anything commits,
    /// serves or reports it: unproven, and at or below the head the walk starts at.
    #[must_use]
    pub fn view_log_proof_covers(&self, op: u64) -> bool {
        !self.view_log_entry_proven(op)
            && op
                <= self.pending_view_log.borrow().as_ref().map_or_else(
                    || self.sequencer.current_sequence(),
                    |pending| pending.op_head,
                )
    }

    #[must_use]
    pub const fn view_log_unproven_range(&self) -> std::ops::RangeInclusive<u64> {
        self.commit_min().saturating_add(1)..=if self.view_log_unproven() {
            self.view_log_unproven_through.get()
        } else if self.uncertified_merge_settled() {
            u64::MAX
        } else {
            0
        }
    }

    /// The op the view-log proof is waiting on. A bridged proof lets the
    /// partition commit walk pass a missing body that a live retransmit filled.
    /// Named at or below `commit_min`, that op opens every repair below the
    /// peers' retained floor, and the shard re-runs the proof only once no op is
    /// named. [`Self::set_commit_floor`] clears an op that a floor jump covers.
    #[must_use]
    pub const fn view_log_repair_op(&self) -> Option<u64> {
        match self.view_log_missing_op.get() {
            Some(op) if self.view_log_unproven() && op > self.commit_min() => Some(op),
            _ => None,
        }
    }

    pub fn note_repaired_view_header(&self, header: PrepareHeader, evidence: RepairEvidence) {
        // A body awaiting proof cannot become the checksum that proves itself.
        if evidence != RepairEvidence::Proven {
            return;
        }
        if let Some(pending) = self.pending_view_log.borrow_mut().as_mut()
            && pending.op_head == header.op
            && pending
                .headers
                .first()
                .is_none_or(|head| dvc_header_kind(head) == DvcHeaderKind::Blank)
        {
            if let Some(head) = pending.headers.first_mut() {
                *head = header;
            } else {
                pending.headers.push(header);
            }
            self.invalidate_local_dvc_suffix();
        }
    }

    /// Whether a merge is parked, without cloning it.
    #[must_use]
    pub fn view_log_is_pending(&self) -> bool {
        self.pending_view_log.borrow().is_some()
    }

    /// Whether the parked log is the merge a `DoViewChange` quorum of this view
    /// decided, which this replica starts once its journal covers it.
    ///
    /// Primary by index is not enough. A replica that restarts as primary of its
    /// recovered view parks its adopted `StartView` suffix, and the quorum it may
    /// have collected before the crash is gone.
    #[must_use]
    pub fn is_primary_elect(&self) -> bool {
        self.do_view_change_quorum.get() && self.view_log_is_pending()
    }

    /// Read the parked merge in place. The closure must not re-enter consensus: the
    /// `RefCell` stays borrowed for its whole body.
    pub fn with_pending_view_log<T>(&self, read: impl FnOnce(&MergedLog) -> T) -> Option<T> {
        self.pending_view_log.borrow().as_ref().map(read)
    }

    /// Replicas that offered a body for `op`, most-recent-log_view first.
    ///
    /// Only meaningful while a merge is parked. These peers and nobody else: a
    /// cleared present bit means the body was never held or cannot be read back,
    /// and the view change is blocked on the round-trip. A sender whose header
    /// at `op` differs from the merged one holds another body, so it is skipped.
    #[must_use]
    pub fn pending_view_body_sources(&self, op: u64) -> Vec<u8> {
        let merged_checksum = self
            .with_pending_view_log(|pending| {
                pending
                    .headers
                    .iter()
                    .find(|header| {
                        header.op == op && dvc_header_kind(header) == DvcHeaderKind::Valid
                    })
                    .map(|header| header.checksum)
            })
            .flatten();
        self.pending_view_sources(|dvc| {
            dvc.suffix.index_of(dvc.op, op).is_some_and(|index| {
                dvc.suffix.offers_body(index)
                    && merged_checksum.is_none_or(|checksum| {
                        dvc.suffix
                            .valid_header_at(index)
                            .is_some_and(|header| header.checksum == checksum)
                    })
            })
        })
    }

    /// Recorded `DoViewChange` senders that `serves` accepts, most-recent-`log_view`
    /// first, never this replica.
    ///
    /// Ordering and self-exclusion are the same for every source list; only the
    /// "can this sender serve the op" predicate differs.
    fn pending_view_sources(&self, serves: impl Fn(&StoredDvc) -> bool) -> Vec<u8> {
        let quorum = self.do_view_change_from_all_replicas.borrow();
        let mut sources: Vec<(u32, u8)> = dvc_iter(&quorum)
            .filter(|dvc| dvc.replica != self.replica)
            .filter(|dvc| serves(dvc))
            .map(|dvc| (dvc.log_view, dvc.replica))
            .collect();
        sources.sort_unstable_by_key(|(log_view, _)| std::cmp::Reverse(*log_view));
        sources.into_iter().map(|(_, replica)| replica).collect()
    }

    /// Replicas that committed `op`, most-recent-`log_view` first.
    ///
    /// The fallback for an op below the DVC suffixes. A suffix spans `commit..=op`,
    /// so [`Self::pending_view_body_sources`] answers nothing about the committed
    /// prefix and a merged log whose coverage gap sits there would have no source
    /// at all. A sender that committed the op either still journals it or has
    /// compacted it under a checkpoint, and both answers move the requester
    /// forward: the prepare, or the `RangeEvicted` that says repair cannot close
    /// this gap.
    ///
    /// Presence is not proven the way an offered body is, so prefer
    /// [`Self::pending_view_body_sources`] wherever it returns anything.
    #[must_use]
    pub fn pending_view_commit_sources(&self, op: u64) -> Vec<u8> {
        self.pending_view_sources(|dvc| dvc.commit >= op)
    }

    /// Finish the parked view change: this replica's journal now covers the merged
    /// log, so it can serve any op it is about to announce.
    ///
    /// Called by the shard after repair progress. No-op unless
    /// [`Self::is_primary_elect`].
    ///
    /// # Panics
    /// If the merged uncommitted range exceeds pipeline capacity, which needs a head
    /// more than one pipeline depth above the proven commit point.
    pub fn start_pending_view(&self, plane: PlaneKind) -> Vec<VsrAction> {
        // A parked `StartView` suffix is only what the repair ingest verifies
        // bodies against, also on a replica restarted as primary by index. It must
        // never take this path: starting the view claims the primaryship of a view
        // this replica did not win.
        if !self.is_primary_elect() {
            return Vec::new();
        }
        let Some(merged) = self.pending_view_log.borrow_mut().take() else {
            return Vec::new();
        };
        if self.adopted_view.borrow_mut().take().is_some() {
            self.adopted_view_dirty.set(true);
        }
        let new_op = merged.op_head;
        let max_commit = merged.commit_max;
        // Repair can apply beyond the commit point frozen in the DVC merge.
        let rebuild_after = self.commit_min().max(max_commit);

        // The one view-change exit that skips `reset_view_change_state`, so the DVCs
        // (a suffix `Vec` per sender, per group led) would be held for the whole
        // primaryship. Nothing reads the array after the view starts: a late
        // same-view DVC returns at the status gate, a higher-view one resets first.
        //
        // `dvc_reset`, not `reset_dvc_quorum`: the latter also clears the
        // `do_view_change_quorum` latch, which from here means "log decided" and is
        // what stops `handle_do_view_change_timeout` retransmitting.
        dvc_reset(&mut self.do_view_change_from_all_replicas.borrow_mut());
        self.invalidate_local_dvc_suffix();

        self.status.set(Status::Normal);
        self.rejoined_on_recovered_log.set(false);
        self.ceded_primaryship.set(false);
        // Merged from a trusted quorum or from every replica, so no surviving log
        // was left out.
        self.untrusted_log.set(false);
        self.sequencer.set_sequence(new_op);
        self.view_start_head.set(new_op);
        // Only place a promotion learns what the merge did to a gated suffix.
        self.redecide_recovery_barrier(new_op);
        if let Some(head) = merged.headers.first() {
            // Keep the hash chain continuous: the next prepare must chain onto the
            // head this view adopted, not onto whatever was appended last.
            self.set_last_prepare_checksum(head.checksum);
        }
        for header in &merged.headers {
            self.observe_prepare_timestamp(header.timestamp);
        }
        // Only now, with the merged head installed above. `log_view` claims "my log
        // IS the log this view decided", and it selects the canonical senders of the
        // next view change, whose headers outrank everyone else's.
        //
        // Raising it at merge time breaks that claim for the whole parked window,
        // which can end in supersession or a crash (`log_view` is durable): the
        // replica then votes as canonical carrying its own stale head, and ops the
        // merge decided to keep fall outside the next scan range, discarded with no
        // nack required. Merge-time assignment is only truthful where every merged
        // header is installed there; parking installs nothing and the repair ingest
        // only fills holes, so a parked replica still holds its old view's log.
        self.log_view.set(self.view.get());
        self.view_log_merged.set(false);
        // The view starts only once the merged log is proven against the WAL.
        if self.group == METADATA_GROUP {
            self.certify_view_log(
                new_op,
                merged.headers.first().map_or(0, |head| head.checksum),
            );
        } else {
            self.log_view_proven.set(self.view.get());
        }

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
            if rebuild_after < new_op {
                timeouts.start(TimeoutKind::Prepare);
            }
        }

        let state = ReplicaLogContext::from_consensus(self, plane);
        emit_replica_event(SimEventKind::PrimaryElected, &state);
        emit_replica_event(SimEventKind::ReplicaStateChanged, &state);

        // Unsolicited StartView at view-change completion: no probe to answer,
        // so it carries no incarnation (freshness comes from the newer view) and
        // goes to every backup.
        let action = VsrAction::SendStartView {
            view: self.view.get(),
            op: new_op,
            commit: max_commit,
            incarnation: 0,
            target: None,
            group: self.group,
            // `merged` was taken out of the parked slot, so hand the headers over.
            suffix: merged.headers,
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
        if rebuild_after < new_op {
            // `complete_view_change_as_primary` already refused a non-fitting
            // range. Asserted so the sites cannot drift; this one cannot decline.
            debug_assert!(
                (new_op - rebuild_after) <= self.prepare_queue_max as u64,
                "view change: uncommitted range {}..={} ({} ops) exceeds pipeline capacity ({}); \
                 the merged log claims more in-flight ops than the pipeline can hold",
                rebuild_after + 1,
                new_op,
                new_op - rebuild_after,
                self.prepare_queue_max,
            );
            actions.push(VsrAction::RebuildPipeline {
                from_op: rebuild_after + 1,
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
    /// - If `header.command` is not `Command::PrepareOk`.
    /// - If `header.replica >= self.replica_count`.
    pub fn handle_prepare_ok(
        &self,
        plane: PlaneKind,
        header: &PrepareOkHeader,
    ) -> PrepareOkOutcome {
        assert_eq!(header.command, Command::PrepareOk);
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
        if self.is_transferring() {
            return PrepareOkOutcome::Ignored {
                reason: IgnoreReason::StateTransfer,
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
        let quorum = self.quorum_replication();
        let quorum_reached = ack_count >= quorum && !entry.ok_quorum_received;

        // Check if we've reached quorum
        if quorum_reached {
            entry.ok_quorum_received = true;
        }

        drop(pipeline);
        self.note_backup_ack(header.replica, header.op);

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

    /// Handle a `PrepareOk` for an op this primary already committed.
    ///
    /// It counts toward no quorum, but it shows the backup holds the op, so a
    /// `Commit` withheld from it until then goes out on the next tick. The
    /// caller ran `ack_preflight`; the pipeline no longer holds the op.
    pub fn handle_late_prepare_ok(&self, header: &PrepareOkHeader) {
        if header.view == self.view()
            && header.op <= self.commit_min.get()
            && !self.is_transferring()
        {
            self.note_backup_ack(header.replica, header.op);
        }
    }

    /// Record that `replica` holds `op`, and wake the next tick if that makes
    /// it owed a `Commit`.
    fn note_backup_ack(&self, replica: u8, op: u64) {
        if replica == self.replica {
            return;
        }
        let mut announcements = self.commit_announcements.borrow_mut();
        announcements.record_ack(replica, op);
        if announcements.owes(replica) {
            self.eager_commit.set(true);
        }
    }

    /// Enqueue a self-ack for the next owner service round.
    pub(crate) fn push_loopback(&self, message: Message<GenericHeader>) {
        let notify = {
            let mut queue = self.loopback_queue.borrow_mut();
            assert!(
                queue.len() < self.prepare_queue_max,
                "loopback queue overflow: {} items",
                queue.len()
            );
            let was_empty = queue.is_empty();
            queue.push_back(message);
            was_empty
        };
        if notify {
            let notifier = self.loopback_notifier.borrow().clone();
            if let Some(notifier) = notifier {
                notifier.notify();
            }
        }
    }

    /// Drain all pending loopback messages into `buf`, leaving the queue empty.
    ///
    /// The caller must dispatch each drained message to the appropriate handler.
    pub fn drain_loopback_into(&self, buf: &mut Vec<Message<GenericHeader>>) {
        buf.extend(self.loopback_queue.borrow_mut().drain(..));
    }

    /// Registers existing work as well as future empty-to-nonempty edges.
    pub fn set_loopback_notifier(&self, notifier: Option<LoopbackNotifier>) {
        self.loopback_notifier.borrow_mut().clone_from(&notifier);
        if self.has_loopback()
            && let Some(notifier) = notifier
        {
            notifier.notify();
        }
    }

    #[must_use]
    pub fn has_loopback(&self) -> bool {
        !self.loopback_queue.borrow().is_empty()
    }

    /// Send a message to `target`, routing self-addressed messages through the loopback queue.
    // VsrConsensus uses Cell/RefCell for single-threaded compio shards; futures are intentionally !Send.
    #[allow(clippy::future_not_send)]
    pub(crate) async fn send_or_loopback(&self, target: u8, message: Message<GenericHeader>) -> bool
    where
        B: MessageBus,
    {
        if target == self.replica {
            self.push_loopback(message);
            return true;
        }
        match self
            .message_bus
            .send_to_replica(target, message.into_frozen())
            .await
        {
            Ok(()) => true,
            Err(error) => {
                tracing::warn!(
                    replica = self.replica,
                    target,
                    "send_or_loopback failed: {error}"
                );
                false
            }
        }
    }

    #[must_use]
    pub const fn message_bus(&self) -> &B {
        &self.message_bus
    }
}

impl<B, P> Project<Message<PrepareHeader>, VsrConsensus<B, P>> for Message<RoutedRequestHeader>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    type Consensus = VsrConsensus<B, P>;

    fn project(self, consensus: &Self::Consensus) -> Message<PrepareHeader> {
        let message = if matches!(
            self.header().operation,
            Operation::StoreConsumerOffset | Operation::DeleteConsumerOffset
        ) {
            let header = *self.header();
            let payload_end = header.size as usize;
            let total_size = payload_end
                + iggy_binary_protocol::requests::consumer_offsets::PREPARED_OWNER_GENERATION_SIZE;
            let mut prepared = if message_bus::is_auto_commit_client(header.client)
                && self.as_slice().len() == total_size
            {
                self
            } else {
                let mut prepared = Self::new(total_size);
                prepared.as_mut_slice()[size_of::<RoutedRequestHeader>()..payload_end]
                    .copy_from_slice(self.body());
                prepared
            };
            prepared.as_mut_slice()[payload_end..total_size]
                .copy_from_slice(&header.owner_generation.to_le_bytes());
            prepared.transmute_header(|_, expanded: &mut RoutedRequestHeader| {
                *expanded = header;
                expanded.size =
                    u32::try_from(total_size).expect("admitted offset prepare fits u32");
            })
        } else {
            self
        };
        let op = consensus.sequencer.current_sequence() + 1;
        // Primary stamps the injected clock once at prepare-build (wall time
        // in production, virtual under the simulator); the value is
        // replicated to every backup so apply() reads the same timestamp
        // across the cluster (deterministic state-machine apply). Monotonic
        // wrapper guards against NTP rewinds; see
        // `VsrConsensus::next_monotonic_timestamp`.
        let timestamp = consensus.next_monotonic_timestamp();

        // Seal the body integrity field over the payload past the 256-byte header
        // (the client request bytes, carried through the transmute unchanged).
        // Computed once by the primary and replicated verbatim, so every backup
        // stores the same value and the scan verifies it after a crash. The body is
        // never re-stamped (`restamp_prepare_view` patches only `view`), so this
        // survives view-change retransmits. The header `checksum` and its `parent`
        // chain are sealed too, for both planes, by `seal_prepare_checksum` below;
        // they exclude `view`, which is what lets a restamp leave them valid.
        //
        // A partition produce prepare already carries a verified
        // `batch_checksum` over the same bytes, so a second full-payload pass is pure
        // cost on the produce path, and it would describe the WRONG bytes:
        // `stamp_prepare_for_persistence` rewrites the command header INSIDE this
        // sealed region before the entry is journaled. Leaving those prepares at `0`
        // is the designed "nothing to verify" sentinel for produce. Offset and
        // control bodies include their fencing identities in this checksum.
        //
        // TODO(consensus): a partition prepare's `checksum` covers its header alone,
        // so two at one op with matching header fields are indistinguishable however
        // far their batch bytes diverge. The merge then counts a divergent replica as
        // holding a servable copy, and the partition repair ingest (no merged-log
        // identity gate) short-circuits `verify_prepare_integrity`'s body branch on
        // the zero. Two closures, both larger than they look:
        //
        // 1. The batch checksum, recomputed after `stamp_prepare_for_persistence`.
        //    But stamping runs after this projection and folds `base_offset` in,
        //    so identity would change at stamp time and the journaled entry would
        //    no longer match the pipeline entry `handle_prepare_ok` compares.
        // 2. The stamp-invariant cover: everything past the 256-byte command header,
        //    which stamping never touches. Identical on every replica, safe to seal
        //    here, but costs a produce-path pass and retires the "0 means nothing to
        //    verify" sentinel that lets an existing WAL replay.
        //
        // Bounded by `size`, the range every verifier re-reads; the prepare
        // inherits it verbatim below.
        let checksum_body = if message.header().operation == Operation::SendMessages {
            0
        } else {
            u128::from(calculate_checksum(frame_body(
                message.as_slice(),
                message.header().size,
            )))
        };

        let prepared = message.transmute_header(|old, new| {
            *new = PrepareHeader {
                cluster: consensus.cluster,
                size: old.size,
                view: consensus.view.get(),
                release: old.release,
                command: Command::Prepare,
                replica: consensus.replica,
                client: old.client,
                parent: consensus.last_prepare_checksum(),
                request_checksum: old.request_checksum,
                request: old.request,
                session: old.session,
                commit: consensus.commit_max.get(),
                op,
                timestamp,
                operation: old.operation,
                // The GROUP's own id, never the request's: a routed request
                // header can carry group 0, and journaling that would make
                // the stored prepare route to the wrong plane when repair
                // later ships it verbatim (live replication masked this;
                // repair replay is what broke).
                group: consensus.group,
                checksum_body,
                // Copied verbatim: carries the stamped acting user for client
                // ops (and the authenticated user on Register), so the in-apply
                // RBAC gate resolves the same identity on every backup.
                user_id: old.user_id,
                partition_incarnation: consensus.partition_incarnation(),
                ..Default::default()
            }
        });
        // Last, because the checksum covers every other field. Gives the op the
        // stable identity the view-change merge compares across replicas; `parent`
        // chains it, so the log is hash-linked rather than nominally so.
        seal_prepare_checksum(prepared)
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
                command: Command::PrepareOk,
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
                group: old.group,
                // PrepareOk is header-only; the frame is exactly the header, so
                // `size` is the header size.
                size: std::mem::size_of::<PrepareOkHeader>() as u32,
                ..Default::default()
            };
            new.seal();
        })
    }
}

impl<B, P> Consensus for VsrConsensus<B, P>
where
    B: MessageBus,
    P: Pipeline<Entry = PipelineEntry>,
{
    type MessageBus = B;
    type Message<H>
        = server_common::Message<H>
    where
        H: ConsensusHeader;
    type RoutedRequestHeader = RoutedRequestHeader;
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

    fn is_transferring(&self) -> bool {
        self.state_transfer_stage.get() != StateTransferStage::Idle
    }
}

#[cfg(test)]
mod fresh_group_start_tests {
    use super::{JoinMode, METADATA_GROUP, ViewRestore, VsrConsensus, fresh_group_start};
    use crate::LocalPipeline;
    use crate::test_bus::NoopBus;

    const CREATED_VIEW: u32 = 4;

    /// Fresh replicas must agree on the committed assignment's initial view,
    /// including a chosen view above zero, before the group starts serving.
    #[test]
    fn given_a_fresh_group_when_created_after_an_election_should_seed_the_creation_view() {
        let start = fresh_group_start(false, None, CREATED_VIEW);
        assert_eq!(start.join, JoinMode::Init);
        assert_eq!(start.seed_view, Some(CREATED_VIEW));
        assert_eq!(start.view_fallback, None);
    }

    /// A durable record outranks the seed: it is what this replica actually
    /// promised, and the seed only describes where the group began.
    #[test]
    fn given_a_durable_record_when_seeding_should_prefer_the_record() {
        let start = fresh_group_start(false, Some((7, 7)), CREATED_VIEW);
        assert_eq!(start.seed_view, None);
        assert_eq!(start.view_fallback, Some(CREATED_VIEW));
    }

    /// A probing backup must sit at or below the group's real view for the
    /// primary's `StartView` to move it forward. Seeded above it, the reply
    /// reads as stale and the replica never rejoins.
    #[test]
    fn given_a_prior_life_when_seeding_should_probe_at_the_creation_floor() {
        let start = fresh_group_start(true, None, CREATED_VIEW);
        assert!(matches!(start.join, JoinMode::ProbeAsBackup { .. }));
        assert_eq!(start.seed_view, None);
        assert_eq!(start.view_fallback, Some(CREATED_VIEW));
    }

    /// The floor certifies no log, so it raises only a record that certifies
    /// none either, and a raised view is never marked durable.
    #[test]
    fn given_each_restore_source_when_restoring_view_should_follow_the_ladder() {
        const FLOOR: Option<u32> = Some(CREATED_VIEW);
        for (durable_view, view_fallback, seed_view, expected, needs_persist) in [
            (Some((1, 0)), FLOOR, None, (CREATED_VIEW, 0), true),
            (Some((7, 0)), FLOOR, None, (7, 0), false),
            (Some((1, 1)), FLOOR, None, (1, 1), false),
            (None, FLOOR, None, (CREATED_VIEW, 0), true),
            (None, None, FLOOR, (CREATED_VIEW, CREATED_VIEW), true),
        ] {
            let mut consensus =
                VsrConsensus::new(1, 0, 3, METADATA_GROUP, NoopBus, LocalPipeline::new());
            consensus.restore_view(ViewRestore {
                durable_view,
                view_fallback,
                seed_view,
            });
            assert_eq!(
                (consensus.view(), consensus.log_view()),
                expected,
                "durable {durable_view:?}, floor {view_fallback:?}, seed {seed_view:?}"
            );
            assert_eq!(
                consensus.needs_superblock_persist(),
                needs_persist,
                "durable {durable_view:?}, floor {view_fallback:?}, seed {seed_view:?}"
            );
        }
    }
}

#[cfg(test)]
mod request_queue_tests {
    use super::test_bus::NoopBus;
    use super::*;
    use crate::client_table::{ClientTable, ClientTableMode};
    use iggy_binary_protocol::{Command, Operation};
    use iggy_common::ConsumerKind;
    use server_common::poll::AutoCommitReservationToken;
    use std::cell::Cell;
    use std::rc::Rc;

    fn make_request(client: u128, request_num: u64) -> Message<RoutedRequestHeader> {
        let header_size = std::mem::size_of::<RoutedRequestHeader>();
        let mut msg = Message::<RoutedRequestHeader>::new(header_size);
        let header = bytemuck::checked::try_from_bytes_mut::<RoutedRequestHeader>(
            &mut msg.as_mut_slice()[..header_size],
        )
        .expect("zeroed bytes are valid");
        *header = RoutedRequestHeader {
            command: Command::Request,
            client,
            session: 1,
            request: request_num,
            operation: Operation::SendMessages,
            ..RoutedRequestHeader::default()
        };
        msg
    }

    #[test]
    fn given_pending_clients_when_checking_capacity_should_reserve_distinct_unprotected_clients() {
        const CAPACITY: usize = 3;
        let mut table = ClientTable::with_mode(CAPACITY, ClientTableMode::PartitionSlice);
        table.commit_capacity(CAPACITY).unwrap();
        table.commit_request(1, 1, 1, 1).unwrap();
        let consensus = VsrConsensus::new(1, 0, 1, METADATA_GROUP, NoopBus, LocalPipeline::new());
        assert!(consensus.has_retry_capacity(&table, 2));
        consensus.with_pipeline_mut(|pipeline| {
            pipeline.push(PipelineEntry::new(PrepareHeader {
                client: 2,
                op: 1,
                ..PrepareHeader::default()
            }));
            for client in [0, message_bus::AUTO_COMMIT_CLIENT_ID, 1, 2] {
                pipeline
                    .push_request(RequestEntry::new(make_request(client, 1)))
                    .unwrap();
            }
        });
        assert!(
            consensus.has_retry_capacity(&table, 3),
            "only client 2 reserves a new slot"
        );
        consensus.with_pipeline_mut(|pipeline| {
            pipeline
                .push_request(RequestEntry::new(make_request(3, 1)))
                .unwrap();
        });
        assert!(
            !consensus.has_retry_capacity(&table, 4),
            "both queues must reserve capacity"
        );
        assert!(
            consensus.has_retry_capacity(&table, 1),
            "committed protection is reusable"
        );
        assert!(
            consensus.has_retry_capacity(&table, 2),
            "a pending client already owns its reservation"
        );
        consensus.with_pipeline_mut(LocalPipeline::clear_request_queue);
        assert!(
            consensus.has_retry_capacity(&table, 3),
            "queue reset must release its reservations"
        );
        assert_eq!(
            consensus.pipeline_len(),
            1,
            "the prepare reservation must survive queue reset"
        );
    }

    #[test]
    fn queued_contexts_keep_each_reservation_until_their_own_removal() {
        let consumer_id = 7;
        let client_id = 1;
        let reclaim_epoch = Rc::new(Cell::new(0));
        let active_keys = Rc::new(Cell::new(0));
        let token = Rc::new(AutoCommitReservationToken::new(
            ConsumerKind::Consumer,
            consumer_id,
            reclaim_epoch,
            Rc::clone(&active_keys),
        ));
        let mut pipeline = LocalPipeline::new();

        // Each queued request holds its own guard for the same consumer key.
        // The queue stores these messages without interpreting their payloads.
        for request_number in 1..=2 {
            let context = AutoCommitRequestContext {
                reservation: token.acquire(),
            };
            pipeline
                .push_request(RequestEntry::with_auto_commit(
                    make_request(client_id, request_number),
                    context,
                ))
                .expect("both requests fit in the queue");
        }

        // Removing the first entry transfers its context to the caller, as
        // promotion would. Releasing it must preserve the second reservation.
        let mut first_request = pipeline.pop_request().expect("first queued request");
        let first_context = first_request
            .take_auto_commit()
            .expect("context follows first request");
        assert_eq!(first_request.message.header().request, 1);
        drop(first_context);
        assert_eq!(token.active_count(), 1);
        assert_eq!(
            active_keys.get(),
            1,
            "the queued request still holds the key"
        );

        // Clearing the queue drops the remaining context and frees the key.
        pipeline.clear_request_queue();
        assert_eq!(token.active_count(), 0);
        assert_eq!(active_keys.get(), 0);
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
    use iggy_binary_protocol::{Command, ReplyHeader};
    use server_common::Message;

    fn make_reply(client: u128, request: u64) -> Message<ReplyHeader> {
        let header_size = std::mem::size_of::<ReplyHeader>();
        let mut msg = Message::<ReplyHeader>::new(header_size);
        let header = bytemuck::checked::try_from_bytes_mut::<ReplyHeader>(
            &mut msg.as_mut_slice()[..header_size],
        )
        .expect("zeroed bytes are valid");
        *header = ReplyHeader {
            command: Command::Reply,
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

    /// A pipeline configured deeper than [`PIPELINE_PREPARE_QUEUE_MAX`] must
    /// verify a full queue instead of tripping the capacity assert: the bound
    /// tracks the configured depth, not the default const.
    #[test]
    #[allow(clippy::cast_possible_truncation)]
    fn given_prepare_queue_depth_above_default_when_verify_should_not_panic() {
        let depth = PIPELINE_PREPARE_QUEUE_MAX * 2;
        let mut pipeline = LocalPipeline::with_capacities(depth, depth * 2);

        let mut parent = 0u128;
        for op in 1..=depth as u64 {
            let checksum = u128::from(op);
            let header = PrepareHeader {
                command: Command::Prepare,
                size: std::mem::size_of::<PrepareHeader>() as u32,
                op,
                parent,
                checksum,
                ..Default::default()
            };
            pipeline.push(PipelineEntry::new(header));
            parent = checksum;
        }

        assert!(
            pipeline.prepare_queue_full(),
            "queue filled to the configured depth"
        );
        // Would panic on the old `len() <= PIPELINE_PREPARE_QUEUE_MAX` assert.
        pipeline.verify();
    }
}

/// Fixtures every consensus test module needs.
#[cfg(test)]
pub mod test_bus {
    use super::{Command, METADATA_GROUP, Message, StartViewHeader};
    use iggy_binary_protocol::{CommitHeader, ConsensusHeader};
    use message_bus::{BusMessage, MessageBus};
    use server_common::MESSAGE_ALIGN;
    use server_common::iobuf::Frozen;

    /// A `StartView` at `view` announcing head `op`. `commit == op` is the steady
    /// case (no suffix); a lower `commit` keeps an uncommitted suffix.
    ///
    /// # Panics
    /// Never: a zeroed buffer of the right size is a valid `StartViewHeader`.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn make_start_view(
        view: u32,
        op: u64,
        commit: u64,
        replica: u8,
        incarnation: u128,
    ) -> Message<StartViewHeader> {
        let size = std::mem::size_of::<StartViewHeader>();
        let mut msg = Message::<StartViewHeader>::new(size);
        let header = bytemuck::checked::try_from_bytes_mut::<StartViewHeader>(
            &mut msg.as_mut_slice()[..size],
        )
        .expect("zeroed bytes are a valid StartViewHeader");
        header.command = Command::StartView;
        header.cluster = 1;
        header.view = view;
        header.op = op;
        header.commit = commit;
        header.replica = replica;
        header.incarnation = incarnation;
        header.group = METADATA_GROUP;
        header.size = size as u32;
        msg
    }

    /// A `Commit` heartbeat from `replica` in `view` stamped with `counter`.
    ///
    /// # Panics
    /// Never: the header size fits in `u32`.
    #[must_use]
    pub fn make_commit(view: u32, replica: u8, counter: u64) -> Message<CommitHeader> {
        Message::<CommitHeader>::new(std::mem::size_of::<CommitHeader>()).transmute_header(
            |_, header: &mut CommitHeader| {
                header.command = Command::Commit;
                header.cluster = 1;
                header.replica = replica;
                header.view = view;
                header.group = METADATA_GROUP;
                header.timestamp_monotonic = counter;
                header.size = u32::try_from(std::mem::size_of::<CommitHeader>()).unwrap();
                header.seal();
            },
        )
    }

    /// A [`MessageBus`] that accepts everything and remembers nothing.
    pub struct NoopBus;

    impl MessageBus for NoopBus {
        async fn send_to_client(
            &self,
            _client_id: u128,
            _data: impl Into<BusMessage>,
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
        fn track_background(&self, _handle: message_bus::JoinHandle<()>) {}
    }
}

#[cfg(test)]
mod timestamp_clamp_tests {
    //! Pin the monotonic-floor contract: a new primary must never stamp a
    //! prepare below timestamps already in the replicated log, even when its
    //! wall clock lags the predecessor's.

    use super::*;
    use crate::LocalPipeline;

    use crate::test_bus::{NoopBus, make_commit, make_start_view};
    use aligned_vec::{AVec, ConstAlign};
    use iggy_binary_protocol::CommitHeader;

    #[test]
    fn observed_log_timestamp_floors_new_primary_stamps() {
        let lagging_clock = ConsensusClock::new(Rc::new(FixedClock(1_000)));
        let consensus = VsrConsensus::with_clock(
            1,
            0,
            1,
            METADATA_GROUP,
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
            METADATA_GROUP,
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

    #[test]
    fn given_recovering_replica_when_start_view_incarnation_foreign_should_ignore() {
        // A StartView addressed to a PREVIOUS incarnation, still in flight when the
        // replica crashed, must be ignored after restart, while the reply echoing
        // the CURRENT incarnation is adopted. Otherwise the replica could act in a
        // view it will not remember after another crash.
        const CURRENT: u128 = 0xB;
        const STALE: u128 = 0xA;

        // Replica 0 of 3, recovered at view 1 with head op 5, still Recovering
        // (probing). The primary for view 1 is replica 1 (view % replica_count),
        // and commit_max stays 0 so the committed-prefix guard does not fire.
        let mut consensus = VsrConsensus::with_clock(
            1,
            0,
            3,
            METADATA_GROUP,
            NoopBus,
            LocalPipeline::new(),
            ConsensusClock::system(),
        );
        consensus.set_incarnation(CURRENT);
        consensus.set_view(1);
        consensus.sequencer().set_sequence(5);
        assert_eq!(consensus.status(), Status::Recovering);

        // Same view, head behind ours, foreign incarnation: ignored.
        let stale = make_start_view(1, 4, 4, 1, STALE);
        assert!(
            consensus
                .handle_start_view(PlaneKind::Metadata, stale.header(), &[])
                .is_empty(),
            "a StartView echoing a previous incarnation must be ignored while recovering"
        );
        assert_eq!(
            consensus.status(),
            Status::Recovering,
            "an ignored StartView must not transition status"
        );
        assert_eq!(
            consensus.view(),
            1,
            "an ignored StartView must not change the view"
        );

        // Same view and head but echoing our current incarnation: adopted, since
        // the match proves the reply post-dates our restart.
        let fresh = make_start_view(1, 4, 4, 1, CURRENT);
        assert!(
            !consensus
                .handle_start_view(PlaneKind::Metadata, fresh.header(), &[])
                .is_empty(),
            "a StartView echoing our current incarnation must be adopted"
        );
        assert_eq!(
            consensus.status(),
            Status::Normal,
            "adopting a StartView transitions to Normal"
        );
    }

    #[test]
    fn given_partition_operation_when_projecting_should_seal_controls_but_not_batches() {
        // A partition produce prepare already carries a
        // verified `batch_checksum` over the same bytes, so a second full-payload hash
        // is pure cost on the produce path, and it would describe bytes that never reach
        // the journal: `stamp_prepare_for_persistence` rewrites the command header
        // inside the sealed region. `0` is the designed "nothing to verify" sentinel, so
        // a durable partition journal skips verification rather than reading every entry
        // as corrupt.
        let seal = |namespace: u64, operation: Operation| -> u128 {
            // Fixed clock: `project` stamps the prepare timestamp, and Miri covers this
            // crate, where a real clock read is an unsupported syscall.
            let consensus = VsrConsensus::with_clock(
                1,
                0,
                1,
                namespace,
                NoopBus,
                LocalPipeline::new(),
                ConsensusClock::new(Rc::new(FixedClock(100_000))),
            );
            let header_size = size_of::<RoutedRequestHeader>();
            let body = b"produce payload";
            let mut msg = Message::<RoutedRequestHeader>::new(header_size + body.len());
            msg.as_mut_slice()[header_size..].copy_from_slice(body);
            let header = bytemuck::checked::try_from_bytes_mut::<RoutedRequestHeader>(
                &mut msg.as_mut_slice()[..header_size],
            )
            .expect("zeroed bytes are a valid RoutedRequestHeader");
            header.command = Command::Request;
            header.client = 1;
            header.request = 1;
            header.operation = operation;
            header.size = u32::try_from(header_size + body.len()).expect("fits u32");
            msg.project(&consensus).header().checksum_body
        };

        assert_ne!(
            seal(METADATA_GROUP, Operation::CreateStream),
            0,
            "a metadata prepare must be sealed: the WAL scan verifies it after a crash"
        );
        assert_ne!(seal(1, Operation::InstallConsumerGroupOwner), 0);
        assert_ne!(seal(1, Operation::TransitionPartitionHistory), 0);
        assert_eq!(
            seal(1, Operation::SendMessages),
            0,
            "a produce body checksum stays zero because persistence rewrites the body; \
             its prepare header identity is still sealed"
        );
    }

    #[test]
    fn given_committed_prefix_when_start_view_head_behind_wal_should_preserve_commits() {
        const RESTORED_VIEW: u32 = 7;
        const CHECKPOINT: u64 = 100;
        const COMMIT: u64 = 105;
        const WAL_HEAD: u64 = 120;
        for checkpoint in [COMMIT, CHECKPOINT] {
            for view in [RESTORED_VIEW, RESTORED_VIEW + 1] {
                let mut consensus = VsrConsensus::with_clock(
                    1,
                    0,
                    3,
                    METADATA_GROUP,
                    NoopBus,
                    LocalPipeline::new(),
                    ConsensusClock::system(),
                );
                consensus.set_view(RESTORED_VIEW);
                consensus.set_log_view(RESTORED_VIEW);
                consensus.restore_commit_state(checkpoint, COMMIT);
                consensus.sequencer().set_sequence(WAL_HEAD);
                consensus.init_as_backup();
                consensus.begin_view_probe();
                let primary = u8::try_from(view % 3).unwrap();

                assert!(
                    consensus
                        .handle_start_view(
                            PlaneKind::Metadata,
                            make_start_view(view, COMMIT - 1, COMMIT - 1, primary, 0).header(),
                            &[],
                        )
                        .is_empty(),
                    "StartView in view {view} must preserve commit {COMMIT} above checkpoint {checkpoint}",
                );
                assert_eq!(consensus.view(), RESTORED_VIEW);
                assert_eq!(consensus.sequencer().current_sequence(), WAL_HEAD);
                assert_eq!(consensus.commit_min(), checkpoint);
                assert_eq!(consensus.commit_max(), COMMIT);
                assert_eq!(consensus.status(), Status::Recovering);

                assert!(
                    !consensus
                        .handle_start_view(
                            PlaneKind::Metadata,
                            make_start_view(view, COMMIT, COMMIT, primary, 0).header(),
                            &[],
                        )
                        .is_empty(),
                    "StartView in view {view} may discard an uncommitted WAL suffix",
                );
                assert_eq!(consensus.view(), view);
                assert_eq!(consensus.sequencer().current_sequence(), COMMIT);
                assert_eq!(consensus.commit_min(), checkpoint);
                assert_eq!(consensus.commit_max(), COMMIT);
                assert_eq!(consensus.status(), Status::Normal);
            }
        }
    }

    #[test]
    fn given_normal_backup_when_same_view_start_view_is_behind_head_should_keep_acked_ops() {
        const VIEW: u32 = 4;
        const ADOPTED_HEAD: u64 = 20;
        const COMMIT: u64 = 22;
        const STALE_HEAD: u64 = 23;
        const HEAD: u64 = 25;
        let primary = u8::try_from(VIEW % 3).unwrap();
        // Both probers go Normal on the first StartView, while another of the same
        // view can still be in flight.
        for probing_from_view_change in [false, true] {
            let mut consensus = VsrConsensus::with_clock(
                1,
                0,
                3,
                METADATA_GROUP,
                NoopBus,
                LocalPipeline::new(),
                ConsensusClock::system(),
            );
            if probing_from_view_change {
                consensus.enter_view_change(
                    PlaneKind::Metadata,
                    VIEW,
                    ViewChangeReason::NormalHeartbeatTimeout,
                );
            } else {
                consensus.set_view(VIEW);
                consensus.begin_view_probe();
            }
            let start_view = |op, commit| make_start_view(VIEW, op, commit, primary, 0);
            assert!(
                !consensus
                    .handle_start_view(
                        PlaneKind::Metadata,
                        start_view(ADOPTED_HEAD, ADOPTED_HEAD).header(),
                        &[],
                    )
                    .is_empty()
            );
            assert!(consensus.view_log_unproven());
            consensus.settle_view_log_proof();
            // Journaled and acked above the adopted head, as `on_replicate` does.
            consensus.sequencer().set_sequence(HEAD);
            consensus.advance_commit_max(COMMIT);

            for op in [STALE_HEAD, HEAD] {
                assert!(
                    consensus
                        .handle_start_view(
                            PlaneKind::Metadata,
                            start_view(op, ADOPTED_HEAD).header(),
                            &[],
                        )
                        .is_empty(),
                    "StartView at op {op} must not be re-adopted at head {HEAD}: the plane \
                     would truncate the acked ops above it",
                );
                assert_eq!(consensus.sequencer().current_sequence(), HEAD);
                assert_eq!(consensus.commit_max(), COMMIT);
                assert_eq!(consensus.view(), VIEW);
                assert_eq!(consensus.status(), Status::Normal);
            }

            for commit in [STALE_HEAD, HEAD] {
                let actions = consensus.handle_start_view(
                    PlaneKind::Metadata,
                    start_view(commit, commit).header(),
                    &[],
                );
                assert!(
                    matches!(actions.as_slice(), [VsrAction::CommitJournal]),
                    "a duplicate StartView with a newer commit must apply it without adopting its head"
                );
                assert_eq!(consensus.commit_max(), commit);
                assert!(!consensus.view_log_unproven());
                assert_eq!(consensus.sequencer().current_sequence(), HEAD);
                assert_eq!(consensus.view(), VIEW);
                assert_eq!(consensus.status(), Status::Normal);
            }

            assert!(
                !consensus
                    .handle_start_view(
                        PlaneKind::Metadata,
                        start_view(HEAD + 1, HEAD).header(),
                        &[]
                    )
                    .is_empty(),
                "a backup behind the primary must still adopt its head",
            );
            assert_eq!(consensus.sequencer().current_sequence(), HEAD + 1);
            assert!(!consensus.view_log_unproven());
        }
    }

    #[test]
    fn given_unproven_same_view_when_commit_and_start_view_arrive_should_refresh_proof() {
        const VIEW: u32 = 4;
        const HEAD: u64 = 8;
        let consensus = VsrConsensus::with_clock(
            1,
            0,
            3,
            METADATA_GROUP,
            NoopBus,
            LocalPipeline::new(),
            ConsensusClock::system(),
        );
        let primary = u8::try_from(VIEW % 3).unwrap();
        let start = make_start_view(VIEW, HEAD, 0, primary, 0);
        consensus.handle_start_view(PlaneKind::Metadata, start.header(), &[]);
        consensus.fence_view_log(HEAD - 1, HEAD - 1);
        let commit = make_commit(VIEW, primary, 1).transmute_header(
            |original, header: &mut CommitHeader| {
                *header = original;
                header.commit = HEAD;
                header.seal();
            },
        );
        assert!(matches!(
            consensus.handle_commit(commit.header()),
            CommitOutcome::Advanced
        ));
        assert_eq!(consensus.commit_max(), HEAD);
        assert_eq!(consensus.commit_min(), 0);
        assert!(consensus.view_log_unproven());
        let mut canonical = PrepareHeader {
            command: Command::Prepare,
            cluster: 1,
            group: METADATA_GROUP,
            operation: Operation::CreateStream,
            size: u32::try_from(size_of::<PrepareHeader>()).unwrap(),
            op: HEAD,
            ..PrepareHeader::default()
        };
        canonical.checksum = canonical.identity_checksum();

        let actions = consensus.handle_start_view(
            PlaneKind::Metadata,
            make_start_view(VIEW, HEAD, HEAD, primary, 0).header(),
            bytemuck::bytes_of(&canonical),
        );

        assert!(!actions.is_empty());
        assert_eq!(
            consensus.pending_view_log().unwrap().headers,
            vec![canonical]
        );
        assert!(consensus.view_log_unproven());
        assert!(!consensus.view_log_entry_proven(1));
        assert_eq!(consensus.commit_min(), 0);
    }

    /// A state-transfer install jumps `commit_min` past the op a view-log fence
    /// named. Peers answer a repair for that op with `RangeEvicted` at an op the
    /// receiver already applied, and the proof re-runs only once no repair op is
    /// named, so the receiver withholds acks for good. Here the floor lands on the
    /// missing op itself, the boundary the test below steps over.
    #[test]
    fn given_fenced_missing_op_when_transfer_floor_covers_it_should_stop_naming_it() {
        const MISSING: u64 = 5;
        let consensus = VsrConsensus::with_clock(
            1,
            0,
            3,
            METADATA_GROUP,
            NoopBus,
            LocalPipeline::new(),
            ConsensusClock::system(),
        );
        consensus.fence_view_log(MISSING, MISSING);
        assert_eq!(consensus.view_log_repair_op(), Some(MISSING));

        consensus.set_commit_floor(MISSING);
        assert_eq!(
            consensus.view_log_repair_op(),
            None,
            "an op at the commit floor is applied, so no repair can still need it"
        );
        assert!(
            consensus.view_log_unproven(),
            "the floor jump proves nothing above it; the proof has to re-run"
        );
    }

    /// A state-transfer install jumps `commit_min` past the op the view-log proof
    /// was fetching. Kept, that op opens every tail repair below the peers'
    /// retained floor: they answer `RangeEvicted`, the requester reads that as a
    /// peer behind it, and the session rotates between replicas forever.
    #[test]
    fn given_fenced_view_log_when_commit_floor_covers_missing_op_should_rederive_proof() {
        const VIEW: u32 = 4;
        const HEAD: u64 = 51;
        const FLOOR: u64 = 46;
        let fenced_backup = |missing: u64| {
            let consensus = VsrConsensus::with_clock(
                1,
                0,
                3,
                METADATA_GROUP,
                NoopBus,
                LocalPipeline::new(),
                ConsensusClock::system(),
            );
            let primary = u8::try_from(VIEW % 3).unwrap();
            consensus.handle_start_view(
                PlaneKind::Metadata,
                make_start_view(VIEW, HEAD, HEAD - 1, primary, 0).header(),
                &[],
            );
            consensus.fence_view_log(missing, 0);
            consensus
        };

        let covered = fenced_backup(FLOOR - 1);
        covered.set_commit_floor(FLOOR);
        assert_eq!(
            covered.view_log_repair_op(),
            None,
            "a missing op the floor covers opens repair below every peer's retained floor"
        );
        assert!(
            covered.view_log_unproven(),
            "the ops above the floor still need proving against the view"
        );
        assert!(!covered.view_log_entry_proven(FLOOR + 1));

        let above = fenced_backup(FLOOR + 1);
        above.set_commit_floor(FLOOR);
        assert_eq!(
            above.view_log_repair_op(),
            Some(FLOOR + 1),
            "a missing op above the floor is still the one to fetch"
        );
    }

    /// A bridged proof vouches for every held entry, so the partition commit walk
    /// applies the missing body once a live retransmit fills it, while the fence
    /// still names that op. Named, it opens every repair at an op the peers may
    /// have evicted, and the shard re-runs the proof only once no op is named.
    #[test]
    fn given_bridged_fence_when_commit_walks_past_missing_op_should_stop_naming_it() {
        const BACKUP: u8 = 1;
        const MISSING: u64 = 5;
        // What `view_log_proof` reports for a body behind a canonical header.
        const BRIDGED_UNPROVEN_THROUGH: u64 = 0;
        let consensus = VsrConsensus::with_clock(
            1,
            BACKUP,
            3,
            IggyNamespace::new(0, 0, 0).inner(),
            NoopBus,
            LocalPipeline::new(),
            ConsensusClock::system(),
        );
        consensus.restore_commit_state(MISSING - 1, MISSING);
        consensus.fence_view_log(MISSING, BRIDGED_UNPROVEN_THROUGH);
        assert_eq!(consensus.view_log_repair_op(), Some(MISSING));
        assert!(
            consensus.view_log_entry_proven(MISSING),
            "the commit walk may apply a bridged op as soon as its body arrives"
        );

        consensus.advance_commit_min(MISSING);
        assert_eq!(
            consensus.view_log_repair_op(),
            None,
            "a repair opened at an applied op draws `RangeEvicted` once the peers \
             evict it, and the proof re-runs only once no op is named"
        );
        assert!(consensus.view_log_unproven());
    }

    #[test]
    fn given_unreconciled_start_view_when_head_grows_and_times_out_should_keep_commit_fenced() {
        const VIEW: u32 = 4;
        const FIRST_HEAD: u64 = 20;
        const NEXT_HEAD: u64 = FIRST_HEAD + 1;
        let consensus = VsrConsensus::with_clock(
            1,
            0,
            3,
            METADATA_GROUP,
            NoopBus,
            LocalPipeline::new(),
            ConsensusClock::system(),
        );
        let primary = u8::try_from(VIEW % 3).unwrap();

        for head in [FIRST_HEAD, NEXT_HEAD] {
            let start_view = make_start_view(VIEW, head, head, primary, 0);
            assert!(
                !consensus
                    .handle_start_view(PlaneKind::Metadata, start_view.header(), &[])
                    .is_empty()
            );
            assert!(consensus.view_log_unproven());
            assert_eq!(consensus.commit_max(), head);
        }

        assert!(
            !consensus
                .handle_normal_heartbeat_timeout(PlaneKind::Metadata)
                .is_empty()
        );
        assert_eq!(consensus.status(), Status::ViewChange);
        assert_eq!(consensus.view(), VIEW + 1);
        assert!(consensus.view_log_unproven());
        assert_eq!(consensus.commit_max(), NEXT_HEAD);

        consensus.settle_view_log_proof();
        assert!(!consensus.view_log_unproven());
    }

    #[test]
    fn given_a_new_adoption_during_a_write_when_the_old_write_completes_should_keep_the_gate_closed()
     {
        for group in [METADATA_GROUP, IggyNamespace::new(1, 1, 1).inner()] {
            let consensus = VsrConsensus::new(1, 0, 3, group, NoopBus, LocalPipeline::new());
            let start_view = |head| {
                let message = make_start_view(4, head, 0, 1, 0);
                let original = *message.header();
                message.transmute_header(|_, header: &mut StartViewHeader| {
                    *header = StartViewHeader { group, ..original };
                })
            };
            consensus.handle_start_view(PlaneKind::Metadata, start_view(2).header(), &[]);
            let first = consensus.vsr_state(0, 0);
            consensus.handle_start_view(PlaneKind::Metadata, start_view(3).header(), &[]);
            consensus.mark_superblock_durable(first.view, first.log_view);
            consensus.mark_history_certificate_durable(first.history_certificate());
            consensus.mark_adopted_view_durable(first.adopted_view.as_ref());
            assert!(consensus.needs_superblock_persist());
            let latest = consensus.vsr_state(0, 0);
            consensus.mark_superblock_durable(latest.view, latest.log_view);
            consensus.mark_history_certificate_durable(latest.history_certificate());
            consensus.mark_adopted_view_durable(latest.adopted_view.as_ref());
            assert!(!consensus.needs_superblock_persist());
            assert_eq!(consensus.adopted_view_head(), Some((4, 3)));
            assert!(consensus.view_log_unproven());
            assert!(!consensus.view_log_entry_proven(3));
        }
    }

    #[test]
    fn given_malformed_start_view_evidence_when_adopting_should_leave_the_old_identity_intact() {
        const BODY_ALIGN: usize = align_of::<PrepareHeader>();
        let consensus = VsrConsensus::new(1, 0, 3, METADATA_GROUP, NoopBus, LocalPipeline::new());
        let header = make_start_view(4, 2, 0, 1, 0);
        let mut body: AVec<u8, ConstAlign<BODY_ALIGN>> = AVec::new(BODY_ALIGN);
        body.push(0);
        assert!(
            consensus
                .handle_start_view_guarded(PlaneKind::Metadata, header.header(), &body, |_| None)
                .is_none()
        );
        assert_eq!(consensus.view(), 0);
        assert_eq!(consensus.adopted_view_head(), None);
        assert!(!consensus.needs_superblock_persist());
    }

    /// The split-brain gate's predicate: `view` and `log_view` each independently
    /// make the superblock stale, and only a matching `mark_superblock_durable`
    /// clears it. The simulator proves the withheld-send behavior end to end; this
    /// pins the predicate the dispatch sites and the debug tripwire both read.
    #[test]
    fn given_view_change_when_needs_superblock_persist_should_track_durability() {
        let mut consensus =
            VsrConsensus::new(1, 0, 3, METADATA_GROUP, NoopBus, LocalPipeline::new());
        assert!(
            !consensus.needs_superblock_persist(),
            "fresh replica: view == view_durable == 0"
        );

        consensus.set_view(3);
        assert!(
            consensus.needs_superblock_persist(),
            "view advanced but not yet persisted"
        );

        consensus.mark_superblock_durable(consensus.view(), consensus.log_view());
        assert!(
            !consensus.needs_superblock_persist(),
            "marked durable clears the gate"
        );

        consensus.set_log_view(3);
        assert!(
            consensus.needs_superblock_persist(),
            "log_view advanced but not yet persisted"
        );

        consensus.mark_superblock_durable(consensus.view(), consensus.log_view());
        assert!(!consensus.needs_superblock_persist());
    }
}

#[cfg(test)]
mod vsr_consensus_tests {
    use super::*;
    use futures::FutureExt;
    use message_bus::BusMessage;

    #[test]
    fn stage_transitions_follow_the_machine() {
        use StateTransferStage::{AwaitingTarget, Fetching, Idle, Installing};
        let legal = [
            (Idle, AwaitingTarget),
            (AwaitingTarget, Fetching),
            (AwaitingTarget, Idle),
            (Fetching, Installing),
            (Fetching, AwaitingTarget),
            (Fetching, Idle),
            (Installing, Idle),
        ];
        for (from, to) in legal {
            assert!(
                StateTransferStage::valid_transition(from, to),
                "{from:?} -> {to:?} must be legal"
            );
        }
        let illegal = [
            (Idle, Fetching),
            (Idle, Installing),
            (AwaitingTarget, Installing),
            (Installing, Fetching),
            (Installing, AwaitingTarget),
            (Idle, Idle),
        ];
        for (from, to) in illegal {
            assert!(
                !StateTransferStage::valid_transition(from, to),
                "{from:?} -> {to:?} must be illegal"
            );
        }
    }

    /// Bus stub: stage plumbing never touches the wire.
    struct StageNoopBus;

    impl MessageBus for StageNoopBus {
        async fn send_to_client(
            &self,
            _client_id: u128,
            _data: impl Into<BusMessage>,
        ) -> Result<(), message_bus::SendError> {
            Ok(())
        }

        async fn send_to_replica(
            &self,
            _replica: u8,
            _data: server_common::iobuf::Frozen<{ server_common::MESSAGE_ALIGN }>,
        ) -> Result<(), message_bus::SendError> {
            Ok(())
        }

        fn track_background(&self, _handle: message_bus::JoinHandle<()>) {}
        fn set_connection_lost_fn(&self, _f: message_bus::ConnectionLostFn) {}
        fn set_replica_forward_fn(&self, _f: message_bus::ReplicaForwardFn) {}
    }

    #[test]
    fn is_transferring_tracks_stage() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, StageNoopBus, LocalPipeline::new());
        consensus.init();
        assert!(!consensus.is_transferring());
        consensus.begin_state_transfer_await();
        assert!(consensus.is_transferring());
        consensus.set_state_transfer_stage(StateTransferStage::Fetching);
        consensus.set_state_transfer_stage(StateTransferStage::Installing);
        assert!(consensus.is_transferring());
        consensus.set_state_transfer_stage(StateTransferStage::Idle);
        assert!(!consensus.is_transferring());
    }

    #[test]
    fn given_ready_replica_when_requesting_primary_transfer_should_select_next_target_view() {
        const REPLICAS: u8 = 5;
        const INCARNATION: u64 = 17;
        for (replica, view, target, next_view) in [
            (1, 0, 3, 3),
            (3, 6, 3, 8),
            (1, 6, 4, 9),
            (2, 9, 1, 11),
            (2, 4, 0, 5),
            (2, u32::MAX - 1, 0, u32::MAX),
        ] {
            let mut consensus =
                VsrConsensus::new(1, replica, REPLICAS, 0, StageNoopBus, LocalPipeline::new());
            consensus.set_view(view);
            consensus.set_log_view(view);
            consensus.set_partition_incarnation(INCARNATION);
            consensus.mark_superblock_durable(view, view);
            consensus.init();

            let actions = consensus.request_primary_transfer(view, target);

            assert!(matches!(
                actions.as_slice(),
                [VsrAction::SendStartViewChange { view, group: 0 }] if *view == next_view
            ));
            assert_eq!(consensus.view(), next_view);
            assert_eq!(consensus.primary_index(next_view), target);
            assert_eq!(consensus.status(), Status::ViewChange);
            assert_eq!(consensus.log_view(), view);
            assert_eq!(consensus.partition_incarnation(), INCARNATION);
            assert!(consensus.needs_superblock_persist());
            assert_eq!(consensus.svc_count_excluding_self(), 0);
            assert!(consensus.sent_own_start_view_change.get());
        }
    }

    #[test]
    fn given_backup_transfer_when_primary_receives_svc_should_join_the_target_election() {
        const TARGET: u8 = 2;
        let backup = VsrConsensus::new(1, 1, 3, 0, StageNoopBus, LocalPipeline::new());
        let primary = VsrConsensus::new(1, 0, 3, 0, StageNoopBus, LocalPipeline::new());
        backup.init();
        primary.init();
        assert!(!backup.request_primary_transfer(0, TARGET).is_empty());
        assert_eq!(primary.status(), Status::Normal);

        let message = Message::<StartViewChangeHeader>::new(size_of::<StartViewChangeHeader>())
            .transmute_header(|_, header: &mut StartViewChangeHeader| {
                header.command = Command::StartViewChange;
                header.cluster = 1;
                header.replica = backup.replica();
                header.view = backup.view();
                header.group = backup.group();
                header.size = u32::try_from(size_of::<StartViewChangeHeader>()).unwrap();
                header.seal();
            });
        let actions = primary.handle_start_view_change(PlaneKind::Partitions, message.header());

        assert_eq!(primary.view(), u32::from(TARGET));
        assert_eq!(primary.status(), Status::ViewChange);
        assert_eq!(primary.log_view(), 0);
        assert!(
            actions.iter().any(|action| matches!(
                action,
                VsrAction::SendStartViewChange { view: 2, group: 0 }
            ))
        );
        assert!(actions.iter().any(|action| matches!(
            action,
            VsrAction::SendDoViewChange {
                target: TARGET,
                view: 2,
                ..
            }
        )));
        assert!(primary.pending_view_log().is_none());
    }

    #[test]
    fn given_pending_mutation_when_requesting_primary_transfer_should_preserve_identity_and_wake_waiter()
     {
        const COMMITTED: u64 = 7;
        const INCARNATION: u64 = 17;
        let consensus = VsrConsensus::new(1, 0, 3, 0, StageNoopBus, LocalPipeline::new());
        consensus.init();
        consensus.set_partition_incarnation(INCARNATION);
        consensus.sequencer().set_sequence(COMMITTED);
        consensus.restore_commit_state(COMMITTED, COMMITTED);
        let message = projected_prepare(COMMITTED + 1, 0xfeed).transmute_header(
            |previous, header: &mut PrepareHeader| {
                *header = previous;
                header.operation = Operation::SendMessages;
                header.client = 31;
                header.session = 37;
                header.request = 41;
                header.partition_incarnation = INCARNATION;
            },
        );
        let receiver = consensus.pipeline_message_with_subscriber(PlaneKind::Partitions, &message);
        let pending = *message.header();
        let checksum = consensus.last_prepare_checksum();

        assert!(!consensus.request_primary_transfer(0, 2).is_empty());

        assert_eq!(consensus.commit_min(), COMMITTED);
        assert_eq!(consensus.commit_max(), COMMITTED);
        assert_eq!(consensus.sequencer().current_sequence(), pending.op);
        assert_eq!(consensus.last_prepare_checksum(), checksum);
        consensus.with_pipeline(|pipeline| {
            let entry = pipeline.entry_by_op(pending.op).unwrap();
            assert_eq!(
                bytemuck::bytes_of(&entry.header),
                bytemuck::bytes_of(&pending)
            );
            assert!(!entry.has_reply_sender());
        });
        assert!(matches!(
            receiver.now_or_never(),
            Some(Err(crate::oneshot::Canceled))
        ));
        assert_eq!(consensus.log_view(), 0);
    }

    #[test]
    fn given_unready_or_stale_transfer_when_requested_should_leave_consensus_unchanged() {
        const VIEW: u32 = 6;
        for reason in [
            "stale view",
            "existing primary",
            "unknown target",
            "recovering",
            "view change",
            "ceded",
            "state transfer",
            "unproved history",
            "newer view observed",
            "unapplied commit",
            "recovery barrier",
            "view start head",
        ] {
            let mut consensus = VsrConsensus::new(1, 2, 5, 0, StageNoopBus, LocalPipeline::new());
            consensus.set_view(VIEW);
            consensus.set_log_view(VIEW);
            consensus.mark_superblock_durable(VIEW, VIEW);
            consensus.init();
            let mut expected_view = VIEW;
            let mut target = 3;
            match reason {
                "stale view" => expected_view -= 1,
                "existing primary" => target = consensus.primary_index(VIEW),
                "unknown target" => target = consensus.replica_count(),
                "recovering" => consensus.begin_view_probe(),
                "view change" => {
                    let _ = consensus.enter_view_change(
                        PlaneKind::Partitions,
                        VIEW,
                        ViewChangeReason::ReceivedStartViewChange,
                    );
                }
                "ceded" => consensus.init_as_backup(),
                "state transfer" => consensus.begin_state_transfer_await(),
                "unproved history" => consensus.fence_view_log(1, 1),
                "newer view observed" => consensus.observe_newer_view(VIEW + 1),
                "unapplied commit" => consensus.advance_commit_max(1),
                "recovery barrier" => consensus.set_recovery_barrier(1),
                "view start head" => consensus.view_start_head.set(1),
                _ => unreachable!(),
            }
            let before = (
                consensus.view(),
                consensus.log_view(),
                consensus.status(),
                consensus.commit_min(),
                consensus.commit_max(),
                consensus.sequencer().current_sequence(),
                consensus.svc_count_excluding_self(),
                consensus.sent_own_start_view_change.get(),
                consensus.needs_superblock_persist(),
            );
            let timers = format!("{:?}", consensus.timeouts.borrow());

            assert!(
                consensus
                    .request_primary_transfer(expected_view, target)
                    .is_empty(),
                "accepted {reason}"
            );

            assert_eq!(
                before,
                (
                    consensus.view(),
                    consensus.log_view(),
                    consensus.status(),
                    consensus.commit_min(),
                    consensus.commit_max(),
                    consensus.sequencer().current_sequence(),
                    consensus.svc_count_excluding_self(),
                    consensus.sent_own_start_view_change.get(),
                    consensus.needs_superblock_persist(),
                ),
                "mutated {reason}"
            );
            assert_eq!(
                timers,
                format!("{:?}", consensus.timeouts.borrow()),
                "rearmed {reason}"
            );
        }
    }

    #[test]
    fn given_metadata_or_exhausted_view_when_requesting_primary_transfer_should_refuse() {
        for (group, view) in [(METADATA_GROUP, 0), (0, u32::MAX)] {
            let mut consensus =
                VsrConsensus::new(1, 0, 5, group, StageNoopBus, LocalPipeline::new());
            consensus.set_view(view);
            consensus.set_log_view(view);
            consensus.mark_superblock_durable(view, view);
            consensus.init();

            assert!(consensus.request_primary_transfer(view, 1).is_empty());
            assert_eq!(consensus.view(), view);
            assert_eq!(consensus.log_view(), view);
            assert_eq!(consensus.status(), Status::Normal);
            assert!(!consensus.needs_superblock_persist());
            assert!(!consensus.sent_own_start_view_change.get());
        }
    }

    // A backup left behind on VIEW (partition healed across an election, or a
    // fresh node that never saw it) keeps getting newer-view heartbeats it
    // cannot process -- they never reset its heartbeat timer, so the timer
    // fires. The primary is alive, so it must PROBE to adopt the current view
    // (which routes into repair / state transfer), not elect: a lagging
    // replica cannot win, and electing would drag a healthy cluster through a
    // needless view change.
    #[test]
    fn heartbeat_timeout_behind_a_newer_view_probes_not_elects() {
        // Replica 1 is a backup at view 0 (primary_index(0) == 0).
        let consensus = VsrConsensus::new(1, 1, 3, 0, StageNoopBus, LocalPipeline::new());
        consensus.init();
        assert_eq!(consensus.status(), Status::Normal);
        assert_eq!(consensus.view(), 0);

        // A newer-view frame arrived and was dropped by the ingress path.
        consensus.observe_newer_view(3);

        let actions = consensus.handle_normal_heartbeat_timeout(PlaneKind::Metadata);

        assert_eq!(
            consensus.status(),
            Status::Recovering,
            "must enter the probe (Recovering), not an election"
        );
        assert_eq!(consensus.view(), 0, "probing must not bump the view");
        assert!(
            !actions
                .iter()
                .any(|action| matches!(action, VsrAction::SendStartViewChange { .. })),
            "must not broadcast an election SVC"
        );
    }

    // No newer view seen means the primary is presumed dead, so the timeout
    // must still elect -- the split must not swallow the ordinary case.
    #[test]
    fn heartbeat_timeout_with_no_newer_view_still_elects() {
        let consensus = VsrConsensus::new(1, 1, 3, 0, StageNoopBus, LocalPipeline::new());
        consensus.init();

        let actions = consensus.handle_normal_heartbeat_timeout(PlaneKind::Metadata);

        assert_eq!(
            consensus.status(),
            Status::ViewChange,
            "a silent primary must still trigger an election"
        );
        assert_eq!(consensus.view(), 1, "election advances the view");
        assert!(
            actions
                .iter()
                .any(|action| matches!(action, VsrAction::SendStartViewChange { .. })),
            "election must broadcast its SVC"
        );
    }

    // A recorded view at or below the current one is stale (already caught up)
    // and must not divert a genuine election, since the guard is a strict `>`.
    #[test]
    fn stale_observed_view_does_not_divert_election() {
        let consensus = VsrConsensus::new(1, 1, 3, 0, StageNoopBus, LocalPipeline::new());
        consensus.init();
        consensus.observe_newer_view(0);

        let _ = consensus.handle_normal_heartbeat_timeout(PlaneKind::Metadata);

        assert_eq!(
            consensus.status(),
            Status::ViewChange,
            "a stale observed view must not suppress the election"
        );
    }

    // A fresh or rejoined node that missed an election boots primary-by-index
    // at a stale view (replica 0 is primary_index(0)). It has no heartbeat
    // timeout -- it IS the heartbeat sender -- so its heartbeat-SEND timer must
    // convert to a probe once it observes the newer view, or it wedges forever
    // advertising a commit point no peer accepts.
    #[test]
    fn stale_primary_by_index_probes_on_its_heartbeat_send_timer() {
        // Replica 0 is primary-by-index at view 0.
        let consensus = VsrConsensus::new(1, 0, 3, 0, StageNoopBus, LocalPipeline::new());
        consensus.init();
        assert!(consensus.is_primary());
        assert_eq!(consensus.status(), Status::Normal);

        // A prepare/heartbeat from the real primary at a newer view was seen.
        consensus.observe_newer_view(2);

        let actions = consensus.handle_commit_message_timeout();

        assert_eq!(
            consensus.status(),
            Status::Recovering,
            "a stale primary-by-index must probe, not keep heartbeating"
        );
        assert!(
            !actions
                .iter()
                .any(|action| matches!(action, VsrAction::SendCommit { .. })),
            "a stale primary must not advertise a commit point"
        );
    }

    #[test]
    fn given_a_stalled_candidate_when_it_escalates_should_recover_a_lost_start_view() {
        const REJOINING_REPLICA: u8 = 1;
        const NEXT_PRIMARY: u8 = 2;
        const REPLICA_COUNT: u8 = 3;
        const LOCAL_HEAD: u64 = 20;
        const PRIMARY_HEAD: u64 = LOCAL_HEAD + 1;
        let plane = PlaneKind::Partitions;
        let group = IggyNamespace::new(0, 0, 0).inner();
        let backup = VsrConsensus::new(
            1,
            REJOINING_REPLICA,
            REPLICA_COUNT,
            group,
            StageNoopBus,
            LocalPipeline::new(),
        );
        backup.sequencer().set_sequence(LOCAL_HEAD);
        backup.restore_commit_state(LOCAL_HEAD - 1, LOCAL_HEAD - 1);
        backup.begin_view_probe();
        for _ in 0..TimeoutManager::REQUEST_START_VIEW_MESSAGE_TICKS * u64::from(PROBE_ATTEMPTS_MAX)
        {
            backup.tick(plane);
        }
        assert_eq!(backup.view(), 1);
        assert_eq!(backup.status(), Status::ViewChange);
        assert!(backup.is_primary_for_view(backup.view()));

        for _ in 0..TimeoutManager::VIEW_CHANGE_STATUS_TICKS {
            backup.tick(plane);
        }
        assert_eq!(backup.view(), 2);
        assert_eq!(backup.status(), Status::ViewChange);
        assert!(!backup.is_primary_for_view(backup.view()));

        // Model a settled primary whose initial StartView was withheld by persistence.
        let mut primary = VsrConsensus::new(
            1,
            NEXT_PRIMARY,
            REPLICA_COUNT,
            group,
            StageNoopBus,
            LocalPipeline::new(),
        );
        primary.set_view(backup.view());
        primary.set_log_view(backup.view());
        primary.sequencer().set_sequence(PRIMARY_HEAD);
        primary.restore_commit_state(LOCAL_HEAD, LOCAL_HEAD);
        primary.init();

        let probe = (0..TimeoutManager::REQUEST_START_VIEW_MESSAGE_TICKS)
            .flat_map(|_| backup.tick(plane))
            .find_map(|action| match action {
                VsrAction::SendRequestStartView { view, group } => Some(
                    Message::<RequestStartViewHeader>::new(size_of::<RequestStartViewHeader>())
                        .transmute_header(|_, header: &mut RequestStartViewHeader| {
                            header.command = Command::RequestStartView;
                            header.cluster = 1;
                            header.replica = REJOINING_REPLICA;
                            header.view = view;
                            header.group = group;
                            header.size =
                                u32::try_from(size_of::<RequestStartViewHeader>()).unwrap();
                            header.seal();
                        }),
                ),
                _ => None,
            })
            .expect("an escalated candidate must request the missing StartView as a backup");
        let replies = primary.handle_request_start_view(plane, probe.header());
        let [
            VsrAction::SendStartView {
                view,
                op,
                commit,
                incarnation,
                target,
                group,
                suffix,
            },
        ] = replies.as_slice()
        else {
            panic!("the settled primary must answer the backup's probe: {replies:?}");
        };
        assert_eq!(*target, Some(REJOINING_REPLICA));
        assert_eq!(suffix.as_slice(), []);
        let response = Message::<StartViewHeader>::new(size_of::<StartViewHeader>())
            .transmute_header(|_, header: &mut StartViewHeader| {
                header.command = Command::StartView;
                header.cluster = 1;
                header.replica = NEXT_PRIMARY;
                header.view = *view;
                header.op = *op;
                header.commit = *commit;
                header.incarnation = *incarnation;
                header.group = *group;
                header.size = u32::try_from(size_of::<StartViewHeader>()).unwrap();
                header.seal();
            });
        backup.handle_start_view(plane, response.header(), &[]);
        assert_eq!(backup.status(), Status::Normal);
        assert_eq!(backup.view(), primary.view());
        assert_eq!(backup.log_view(), primary.log_view());
        assert_eq!(backup.sequencer().current_sequence(), PRIMARY_HEAD);
        assert_eq!(backup.commit_max(), LOCAL_HEAD);
        assert_eq!(backup.commit_min(), LOCAL_HEAD - 1);
    }

    // A genuine current primary (no newer view seen) keeps heartbeating.
    #[test]
    fn current_primary_keeps_heartbeating() {
        let consensus = VsrConsensus::new(1, 0, 3, 0, StageNoopBus, LocalPipeline::new());
        consensus.init();

        let actions = consensus.handle_commit_message_timeout();

        assert_eq!(consensus.status(), Status::Normal, "must stay primary");
        assert!(
            actions
                .iter()
                .any(|action| matches!(action, VsrAction::SendCommit { .. })),
            "a healthy primary must heartbeat"
        );
    }

    #[test]
    fn given_a_commit_advance_when_the_next_tick_runs_should_announce_it_once_unless_a_prepare_carries_it()
     {
        let consensus = VsrConsensus::new(1, 0, 3, 0, StageNoopBus, LocalPipeline::new());
        consensus.set_commit_message_ticks(u64::MAX);
        consensus.init();
        let announced = || -> Vec<u64> {
            tick_commits(&consensus)
                .into_iter()
                .map(|(_, commit)| commit)
                .collect()
        };
        let commit_head = |op: u64| commit_acked_head(&consensus, op);
        let stamped = |op: u64, commit: u64| {
            projected_prepare(op, 0).transmute_header(|mut header, stamped: &mut PrepareHeader| {
                header.commit = commit;
                *stamped = header;
            })
        };

        pipeline(&consensus, &projected_prepare(1, 0));
        commit_head(1);
        consensus.request_eager_commit();
        assert_eq!(announced(), [1], "requests before a tick share one Commit");
        assert!(announced().is_empty(), "one Commit per commit advance");

        pipeline(&consensus, &projected_prepare(2, 0));
        commit_head(2);
        // A queued request promoted by the commit, stamped with the new commit point.
        pipeline(&consensus, &stamped(3, 2));
        assert!(
            announced().is_empty(),
            "the promoted prepare already tells the backups"
        );

        commit_head(3);
        assert_eq!(announced(), [3]);

        // Prepared while op 4 was in flight, so it carries the older commit point.
        pipeline(&consensus, &projected_prepare(4, 0));
        pipeline(&consensus, &stamped(5, 3));
        commit_head(4);
        assert_eq!(
            announced(),
            [4],
            "a prepare stamped before the commit does not tell the backups"
        );

        // Op 6 is prepared before the commit of op 5 and op 7 after it.
        pipeline(&consensus, &stamped(6, 4));
        commit_head(5);
        pipeline(&consensus, &stamped(7, 5));
        assert!(
            announced().is_empty(),
            "the newest prepare tells the backups, though an older one does not"
        );
    }

    #[test]
    fn given_a_requested_commit_when_the_heartbeat_fires_on_that_tick_should_send_one_commit() {
        const HEARTBEAT_TICKS: u64 = 2;
        let consensus = VsrConsensus::new(1, 0, 3, 0, StageNoopBus, LocalPipeline::new());
        consensus.set_commit_message_ticks(HEARTBEAT_TICKS);
        consensus.init();
        let commits = || tick_commits(&consensus).len();

        assert_eq!(commits(), 0, "the heartbeat is not due yet");
        pipeline(&consensus, &projected_prepare(1, 0));
        commit_acked_head(&consensus, 1);
        assert_eq!(commits(), 1, "the heartbeat answers the request");
        assert_eq!(commits(), 0, "the heartbeat consumed the request");
    }

    #[test]
    fn given_a_backup_that_acks_a_committed_op_late_when_the_next_tick_runs_should_send_it_a_commit_covering_the_op()
     {
        let consensus =
            VsrConsensus::new(1, 0, 3, METADATA_GROUP, StageNoopBus, LocalPipeline::new());
        consensus.set_commit_message_ticks(u64::MAX);
        consensus.init();
        let late_ack = |op: u64| {
            consensus.handle_late_prepare_ok(&prepare_ok(&consensus, LATE_BACKUP, op));
        };

        pipeline(&consensus, &projected_prepare(1, 0));
        pipeline(&consensus, &projected_prepare(2, 0));
        commit_acked_head(&consensus, 1);
        commit_acked_head(&consensus, 2);
        assert_eq!(
            tick_commits(&consensus),
            [(Some(ACKING_BACKUP), 2)],
            "only a backup that acked the op holds it"
        );

        late_ack(1);
        assert_eq!(
            tick_commits(&consensus),
            [(Some(LATE_BACKUP), 1)],
            "a late ack gets a Commit at once, naming no op the backup lacks"
        );
        late_ack(2);
        assert_eq!(tick_commits(&consensus), [(Some(LATE_BACKUP), 2)]);
        late_ack(2);
        assert!(
            tick_commits(&consensus).is_empty(),
            "a Commit already covers a repeated ack"
        );
    }

    #[test]
    fn given_a_backup_that_never_acks_when_the_heartbeat_fires_should_still_send_it_the_commit() {
        const HEARTBEAT_TICKS: u64 = 4;
        let consensus =
            VsrConsensus::new(1, 0, 3, METADATA_GROUP, StageNoopBus, LocalPipeline::new());
        consensus.set_commit_message_ticks(HEARTBEAT_TICKS);
        consensus.init();

        pipeline(&consensus, &projected_prepare(1, 0));
        commit_acked_head(&consensus, 1);
        assert_eq!(
            tick_commits(&consensus),
            [(Some(ACKING_BACKUP), 1)],
            "the eager Commit skips a backup that never acked"
        );
        let heartbeats: Vec<_> = (1..HEARTBEAT_TICKS)
            .flat_map(|_| tick_commits(&consensus))
            .collect();
        assert_eq!(
            heartbeats,
            [(None, 1)],
            "the heartbeat still goes to every backup"
        );

        consensus.handle_late_prepare_ok(&prepare_ok(&consensus, LATE_BACKUP, 1));
        assert!(
            tick_commits(&consensus).is_empty(),
            "the heartbeat already covered the op"
        );
    }

    #[test]
    fn given_a_requested_commit_when_the_replica_stops_leading_before_the_tick_should_not_announce_it()
     {
        for reason in ["demoted", "view change", "newer view observed"] {
            let consensus =
                VsrConsensus::new(1, 0, 3, METADATA_GROUP, StageNoopBus, LocalPipeline::new());
            consensus.set_commit_message_ticks(u64::MAX);
            consensus.init();
            pipeline(&consensus, &projected_prepare(1, 0));
            commit_acked_head(&consensus, 1);
            match reason {
                "demoted" => {
                    let start_view = test_bus::make_start_view(1, 1, 1, 1, 0);
                    let _ =
                        consensus.handle_start_view(PlaneKind::Metadata, start_view.header(), &[]);
                }
                // Replica 0 leads view 3 too, so only its status holds the Commit back.
                "view change" => {
                    let _ = consensus.enter_view_change(
                        PlaneKind::Metadata,
                        3,
                        ViewChangeReason::ReceivedStartViewChange,
                    );
                }
                "newer view observed" => consensus.observe_newer_view(1),
                _ => unreachable!(),
            }
            assert!(
                !consensus
                    .tick(PlaneKind::Metadata)
                    .iter()
                    .any(|action| matches!(action, VsrAction::SendCommit { .. })),
                "announced a commit after {reason}"
            );
        }
    }

    #[test]
    fn given_an_old_primary_when_the_new_primary_heartbeats_should_remain_in_the_new_view() {
        for enters_election in [false, true] {
            let consensus =
                VsrConsensus::new(1, 0, 3, METADATA_GROUP, StageNoopBus, LocalPipeline::new());
            consensus.init();
            for _ in 0..TimeoutManager::NORMAL_HEARTBEAT_TICKS {
                let _ = consensus.tick(PlaneKind::Metadata);
            }
            assert_eq!(
                consensus.heartbeat_watermark.get(),
                (
                    0,
                    TimeoutManager::NORMAL_HEARTBEAT_TICKS / TimeoutManager::COMMIT_MESSAGE_TICKS
                ),
                "the old primary must have emitted a full timeout's worth of heartbeats"
            );

            if enters_election {
                let _ = consensus.start_election(
                    PlaneKind::Metadata,
                    ViewChangeReason::NormalHeartbeatTimeout,
                );
            }
            let start_view = test_bus::make_start_view(1, 0, 0, 1, 0);
            let _ = consensus.handle_start_view(PlaneKind::Metadata, start_view.header(), &[]);

            for tick in 1..=TimeoutManager::NORMAL_HEARTBEAT_TICKS * 2 {
                if tick % TimeoutManager::COMMIT_MESSAGE_TICKS == 0 {
                    let heartbeat =
                        test_bus::make_commit(1, 1, tick / TimeoutManager::COMMIT_MESSAGE_TICKS);
                    consensus.handle_commit(heartbeat.header());
                }
                let _ = consensus.tick(PlaneKind::Metadata);
                assert_eq!(
                    consensus.view(),
                    1,
                    "fresh heartbeats must keep the adopted view alive at tick {tick}, \
                     enters_election={enters_election}"
                );
            }

            for _ in 0..TimeoutManager::NORMAL_HEARTBEAT_TICKS {
                let _ = consensus.tick(PlaneKind::Metadata);
            }
            assert_eq!(
                consensus.view(),
                2,
                "losing the new primary must still trigger an election"
            );
        }
    }

    #[test]
    fn given_an_adopted_view_when_heartbeats_are_stale_or_foreign_should_time_out() {
        let consensus =
            VsrConsensus::new(1, 0, 3, METADATA_GROUP, StageNoopBus, LocalPipeline::new());
        consensus.init();
        let start_view = test_bus::make_start_view(1, 0, 0, 1, 0);
        let _ = consensus.handle_start_view(PlaneKind::Metadata, start_view.header(), &[]);
        let heartbeat = test_bus::make_commit(1, 1, 1);
        consensus.handle_commit(heartbeat.header());
        // Re-adopting the same view restarts the timer but must keep the
        // watermark; the replayed heartbeat below proves it.
        let _ = consensus.handle_start_view(PlaneKind::Metadata, start_view.header(), &[]);

        let invalid_heartbeats = [
            heartbeat,
            test_bus::make_commit(1, 1, 0),
            test_bus::make_commit(0, 0, u64::MAX),
            test_bus::make_commit(1, 2, u64::MAX),
        ];
        for tick in 1..=TimeoutManager::NORMAL_HEARTBEAT_TICKS {
            if tick % TimeoutManager::COMMIT_MESSAGE_TICKS == 0 {
                for heartbeat in &invalid_heartbeats {
                    consensus.handle_commit(heartbeat.header());
                }
            }
            let _ = consensus.tick(PlaneKind::Metadata);
        }
        assert_eq!(
            consensus.view(),
            2,
            "replayed, older-view, and non-primary heartbeats must not suppress an election"
        );
    }

    // `observe_newer_view` is monotone: an older stamp cannot lower it.
    #[test]
    fn observe_newer_view_keeps_the_max() {
        let consensus = VsrConsensus::new(1, 1, 3, 0, StageNoopBus, LocalPipeline::new());
        consensus.init();
        consensus.observe_newer_view(5);
        consensus.observe_newer_view(2);
        // Timeout at view 0 with the record still 5 must probe.
        let _ = consensus.handle_normal_heartbeat_timeout(PlaneKind::Metadata);
        assert_eq!(consensus.status(), Status::Recovering);
    }

    /// A prepare as the primary projects one: `parent` is the chain value the
    /// sequencer stood on before this op, which is exactly what a rollback restores.
    #[allow(clippy::cast_possible_truncation)]
    fn projected_prepare(op: u64, parent: u128) -> Message<PrepareHeader> {
        Message::<PrepareHeader>::new(size_of::<PrepareHeader>()).transmute_header(|_, new| {
            *new = PrepareHeader {
                command: Command::Prepare,
                size: size_of::<PrepareHeader>() as u32,
                op,
                parent,
                ..Default::default()
            };
        })
    }

    /// [`projected_prepare`] carrying the two fields the rollback guards match on,
    /// so two prepares at the same op stay distinguishable.
    #[allow(clippy::cast_possible_truncation)]
    fn projected_prepare_in_view(
        op: u64,
        parent: u128,
        checksum: u128,
        view: u32,
    ) -> Message<PrepareHeader> {
        Message::<PrepareHeader>::new(size_of::<PrepareHeader>()).transmute_header(|_, new| {
            *new = PrepareHeader {
                command: Command::Prepare,
                size: size_of::<PrepareHeader>() as u32,
                op,
                parent,
                checksum,
                view,
                ..Default::default()
            };
        })
    }

    /// Pipeline a prepare the way `on_request` does, pre-advancing the sequencer
    /// and the parent chain ahead of the journal append.
    fn pipeline(consensus: &VsrConsensus<StageNoopBus>, message: &Message<PrepareHeader>) {
        consensus.pipeline_message(PlaneKind::Metadata, message);
    }

    /// The backup whose ack the eager `Commit` tests record for each op they
    /// commit, in a 3-replica group led by replica 0.
    const ACKING_BACKUP: u8 = 1;
    /// The backup those tests withhold acks from, as the chain tail's arrive
    /// after the quorum.
    const LATE_BACKUP: u8 = 2;

    /// The `PrepareOk` `replica` sends in the current view for an op pipelined
    /// from [`projected_prepare`].
    fn prepare_ok(consensus: &VsrConsensus<StageNoopBus>, replica: u8, op: u64) -> PrepareOkHeader {
        PrepareOkHeader {
            command: Command::PrepareOk,
            replica,
            view: consensus.view(),
            op,
            ..Default::default()
        }
    }

    /// Commit the pipeline head `op` the way the metadata plane does, after an
    /// ack from [`ACKING_BACKUP`].
    fn commit_acked_head(consensus: &VsrConsensus<StageNoopBus>, op: u64) {
        let _ = consensus.handle_prepare_ok(
            PlaneKind::Metadata,
            &prepare_ok(consensus, ACKING_BACKUP, op),
        );
        consensus.advance_commit_max(op);
        assert_eq!(consensus.pop_committed_prepare().unwrap().header.op, op);
        consensus.advance_commit_min(op);
        consensus.request_eager_commit();
    }

    /// The `Commit`s the next tick sends, as `(target, commit)`.
    fn tick_commits(consensus: &VsrConsensus<StageNoopBus>) -> Vec<(Option<u8>, u64)> {
        consensus
            .tick(PlaneKind::Metadata)
            .into_iter()
            .filter_map(|action| match action {
                VsrAction::SendCommit { target, commit, .. } => Some((target, commit)),
                _ => None,
            })
            .collect()
    }

    use iggy_binary_protocol::Operation;

    /// Clock frozen at a fixed instant, so a stamp read off it is assertable.
    struct FrozenClock(u64);

    impl clock::Clock for FrozenClock {
        type Realtime = IggyTimestamp;

        fn realtime(&self) -> Self::Realtime {
            IggyTimestamp::from(self.0)
        }
    }

    /// A client request as the admission path hands it to the request queue.
    #[allow(clippy::cast_possible_truncation)]
    fn client_request(client: u128) -> Message<RoutedRequestHeader> {
        Message::<RoutedRequestHeader>::new(size_of::<RoutedRequestHeader>()).transmute_header(
            |_, new| {
                *new = RoutedRequestHeader {
                    command: Command::Request,
                    size: size_of::<RoutedRequestHeader>() as u32,
                    client,
                    session: 1,
                    request: 1,
                    operation: Operation::CreateStream,
                    ..Default::default()
                };
            },
        )
    }

    /// Whether the prepare retransmit timer is armed.
    fn prepare_ticking(consensus: &VsrConsensus<StageNoopBus>) -> bool {
        consensus.timeouts.borrow().is_ticking(TimeoutKind::Prepare)
    }

    #[test]
    fn prepare_timeout_ticks_exactly_while_the_pipeline_is_non_empty() {
        // The lifecycle invariant: armed by the first push, disarmed the moment
        // the pipeline drains. Previously it armed at `init` on an empty pipeline
        // and only disarmed lazily, when the timeout itself fired and found
        // nothing to retransmit.
        let consensus = VsrConsensus::new(1, 0, 3, 0, StageNoopBus, LocalPipeline::new());
        consensus.init();
        assert!(
            !prepare_ticking(&consensus),
            "a fresh primary has nothing to retransmit"
        );

        let first = projected_prepare(1, 0);
        pipeline(&consensus, &first);
        assert!(prepare_ticking(&consensus), "the first push arms the timer");

        let second = projected_prepare(2, 0);
        pipeline(&consensus, &second);

        // Committing the head leaves op 2 in flight, so the timer stays armed --
        // now measuring op 2 rather than carrying op 1's elapsed ticks.
        consensus.advance_commit_max(1);
        assert_eq!(consensus.pop_committed_prepare().unwrap().header.op, 1);
        consensus.advance_commit_min(1);
        assert!(
            prepare_ticking(&consensus),
            "a remaining prepare keeps the timer armed"
        );

        consensus.advance_commit_max(2);
        assert_eq!(consensus.pop_committed_prepare().unwrap().header.op, 2);
        consensus.advance_commit_min(2);
        assert!(
            !prepare_ticking(&consensus),
            "draining the last prepare disarms the timer without waiting for it to fire"
        );
    }

    #[test]
    fn parking_a_request_stamps_its_arrival_from_the_injected_clock() {
        // The queue wait is `clock_realtime_micros() - received_at` at promotion,
        // so the stamp has to come from the same injected clock (virtual under
        // the simulator) and must not consume the prepare-stamping monotonic
        // sequence.
        const NOW: u64 = 4_242;
        let consensus = VsrConsensus::with_clock(
            1,
            0,
            3,
            0,
            StageNoopBus,
            LocalPipeline::new(),
            ConsensusClock::new(Rc::new(FrozenClock(NOW))),
        );
        consensus.init();
        let before = consensus.next_monotonic_timestamp();

        consensus
            .push_queued_request(RequestEntry::new(client_request(1)))
            .expect("empty request queue accepts one");

        let entry = consensus
            .pop_queued_request()
            .expect("the just-parked request");
        assert_eq!(entry.received_at, NOW, "stamped from the injected clock");
        assert_eq!(
            consensus.next_monotonic_timestamp(),
            before + 1,
            "parking must not consume the prepare-stamping monotonic sequence"
        );
    }

    #[test]
    fn clearing_the_pipeline_disarms_the_prepare_timeout() {
        // A view change re-prepares from the new primary, so nothing left here is
        // worth retransmitting.
        let consensus = VsrConsensus::new(1, 0, 3, 0, StageNoopBus, LocalPipeline::new());
        consensus.init();
        pipeline(&consensus, &projected_prepare(1, 0));
        assert!(prepare_ticking(&consensus));

        consensus.clear_pipeline();
        assert!(consensus.pipeline_is_empty());
        assert!(!prepare_ticking(&consensus));
    }

    #[test]
    fn rollback_hands_back_the_op_a_failed_append_claimed() {
        // Without this, the sequencer keeps claiming op 8 while the WAL stops at 7:
        // the next request projects op 9 over a hole that no repair path refills.
        const PARENT: u128 = 0xfeed;
        let consensus = VsrConsensus::new(1, 0, 3, 0, StageNoopBus, LocalPipeline::new());
        consensus.init();
        consensus.sequencer().set_sequence(7);
        consensus.set_last_prepare_checksum(PARENT);

        let message = projected_prepare(8, PARENT);
        pipeline(&consensus, &message);
        assert_eq!(consensus.sequencer().current_sequence(), 8);

        assert_eq!(
            consensus.rollback_pipelined_prepare(message.header()),
            PrepareRollback::Unwound
        );
        assert_eq!(consensus.sequencer().current_sequence(), 7);
        assert_eq!(consensus.last_prepare_checksum(), PARENT);
        assert!(
            consensus.pipeline_is_empty(),
            "the reclaimed op must not stay live in the pipeline; the next request reuses it"
        );
    }

    #[test]
    fn rollback_is_refused_once_a_sibling_took_the_next_op() {
        // The race the pre-advance exists for: a request pipelined while op 8's
        // append was in flight already chained op 9 off it. Rewinding would hand
        // op 9's number back out while op 9 is still live, so the refusal is the
        // safe answer and the caller escalates.
        let consensus = VsrConsensus::new(1, 0, 3, 0, StageNoopBus, LocalPipeline::new());
        consensus.init();
        consensus.sequencer().set_sequence(7);

        let first = projected_prepare(8, 0);
        pipeline(&consensus, &first);
        let sibling = projected_prepare(9, 0);
        pipeline(&consensus, &sibling);

        assert_eq!(
            consensus.rollback_pipelined_prepare(first.header()),
            PrepareRollback::Overtaken { sequence: 9 }
        );
        assert_eq!(consensus.sequencer().current_sequence(), 9);
        assert_eq!(
            consensus.pipeline_len(),
            2,
            "a refused rollback must not touch the pipeline"
        );
    }

    #[test]
    fn rollback_is_refused_while_a_view_change_is_running() {
        // A view change can conclude under the append's `.await`, after which every
        // number the rollback reads belongs to the new view, including a sequencer
        // back on this op with a different prepare beneath it. Unwinding there pops a
        // live entry and rewinds beneath an op peers have journaled.
        const PARENT: u128 = 0xfeed;
        let consensus = VsrConsensus::new(1, 0, 3, 0, StageNoopBus, LocalPipeline::new());
        consensus.init();
        consensus.sequencer().set_sequence(7);
        consensus.set_last_prepare_checksum(PARENT);

        let message = projected_prepare_in_view(8, PARENT, 0xaaaa, 0);
        pipeline(&consensus, &message);
        assert_eq!(consensus.sequencer().current_sequence(), 8);

        let _ = consensus.enter_view_change(
            PlaneKind::Metadata,
            3,
            ViewChangeReason::NormalHeartbeatTimeout,
        );

        assert_eq!(
            consensus.rollback_pipelined_prepare(message.header()),
            PrepareRollback::Superseded { view: 3 },
            "a prepare from a view this replica has left owns none of this state"
        );
        assert_eq!(
            consensus.sequencer().current_sequence(),
            8,
            "a refused rollback must not move the sequencer"
        );
        assert_eq!(
            consensus.last_prepare_checksum(),
            0xaaaa,
            "a refused rollback must not rewind the parent chain"
        );
    }

    #[test]
    fn rollback_is_refused_when_the_tail_is_a_different_prepare() {
        // Op numbers repeat across views: a rewind to the merged head lets the next
        // request re-project this number with its own checksum. Matching on `op` alone
        // pops that live entry, and the old `debug_assert` let release rewind anyway.
        const PARENT: u128 = 0xfeed;
        const LIVE: u128 = 0xbbbb;
        let consensus = VsrConsensus::new(1, 0, 3, 0, StageNoopBus, LocalPipeline::new());
        consensus.init();
        consensus.sequencer().set_sequence(7);

        // The op-8 entry that is actually live, projected after the rewind.
        let live = projected_prepare_in_view(8, PARENT, LIVE, 0);
        pipeline(&consensus, &live);

        // The op-8 prepare whose append failed: same number, different bytes.
        let failed = projected_prepare_in_view(8, PARENT, 0xaaaa, 0);
        assert_eq!(
            consensus.rollback_pipelined_prepare(failed.header()),
            PrepareRollback::TailMismatch
        );
        assert_eq!(consensus.sequencer().current_sequence(), 8);
        assert_eq!(consensus.last_prepare_checksum(), LIVE);
        assert_eq!(
            consensus.pipeline_len(),
            1,
            "the live entry must survive a refused rollback"
        );
        assert!(
            consensus.pipeline_holds_entry(8, LIVE),
            "and it must still be the same entry"
        );
    }

    #[test]
    fn rollback_disarms_the_prepare_timeout_when_it_empties_the_pipeline() {
        // `Unwound` pops through the pipeline directly, so it owes the same
        // "ticking iff non-empty" maintenance every other drain does.
        let consensus = VsrConsensus::new(1, 0, 3, 0, StageNoopBus, LocalPipeline::new());
        consensus.init();
        consensus.sequencer().set_sequence(7);

        let message = projected_prepare(8, 0);
        pipeline(&consensus, &message);
        assert!(prepare_ticking(&consensus));

        assert_eq!(
            consensus.rollback_pipelined_prepare(message.header()),
            PrepareRollback::Unwound
        );
        assert!(consensus.pipeline_is_empty());
        assert!(
            !prepare_ticking(&consensus),
            "unwinding the sole in-flight prepare leaves nothing to retransmit"
        );
    }

    #[test]
    fn rollback_finds_nothing_to_undo_on_a_backup() {
        // Replica 1 is a backup at view 0, and a backup advances only after its
        // append succeeds, so a failed append left nothing pre-advanced.
        let consensus = VsrConsensus::new(1, 1, 3, 0, StageNoopBus, LocalPipeline::new());
        consensus.init();
        consensus.sequencer().set_sequence(7);

        let message = projected_prepare(8, 0);
        assert_eq!(
            consensus.rollback_pipelined_prepare(message.header()),
            PrepareRollback::NotPreAdvanced
        );
        assert_eq!(consensus.sequencer().current_sequence(), 7);
    }

    #[test]
    fn rollback_cancels_the_client_awaiting_the_dropped_prepare() {
        // The write was never made durable, so the caller must learn it failed
        // instead of parking until its request times out.
        use futures::FutureExt as _;

        let consensus = VsrConsensus::new(1, 0, 3, 0, StageNoopBus, LocalPipeline::new());
        consensus.init();
        consensus.sequencer().set_sequence(7);

        let message = projected_prepare(8, 0);
        let receiver = consensus.pipeline_message_with_subscriber(PlaneKind::Metadata, &message);

        assert_eq!(
            consensus.rollback_pipelined_prepare(message.header()),
            PrepareRollback::Unwound
        );
        assert!(
            matches!(receiver.now_or_never(), Some(Err(crate::oneshot::Canceled))),
            "dropping the entry must cancel its awaiter"
        );
    }
}

#[cfg(test)]
mod quorum_tests {
    //! Pin the three quorum sizes for replica counts 1 through 8. The
    //! intersection asserts are the safety properties: replication and
    //! view-change quorums must overlap, so a committed op is visible to the
    //! next view, and replication and nack quorums must overlap, so an op that
    //! may have committed can never gather a nack quorum.

    use super::*;
    use crate::LocalPipeline;

    use crate::test_bus::NoopBus;

    fn consensus_with_replica_count(replica_count: u8) -> VsrConsensus<NoopBus, LocalPipeline> {
        VsrConsensus::new(
            1,
            0,
            replica_count,
            METADATA_GROUP,
            NoopBus,
            LocalPipeline::new(),
        )
    }

    #[test]
    fn given_any_replica_count_when_sizing_quorums_should_intersect() {
        for replica_count in 1u8..=REPLICAS_MAX_U8 {
            let consensus = consensus_with_replica_count(replica_count);
            let count = usize::from(replica_count);

            assert!(
                consensus.quorum_replication() + consensus.quorum_view_change() > count,
                "replication+view-change must intersect at replica_count={replica_count}"
            );
            assert!(
                consensus.quorum_nack_prepare() + consensus.quorum_replication() > count,
                "nack+replication must intersect at replica_count={replica_count}"
            );
            assert!(consensus.quorum_replication() <= count);
            assert!(consensus.quorum_view_change() <= count);
            assert!(consensus.quorum_nack_prepare() <= count);
        }
    }

    /// `REPLICAS_MAX` as a `u8` for loop bounds.
    const REPLICAS_MAX_U8: u8 = {
        assert!(REPLICAS_MAX <= u8::MAX as usize);
        #[allow(clippy::cast_possible_truncation)]
        {
            REPLICAS_MAX as u8
        }
    };
}

#[cfg(test)]
mod view_source_tests {
    //! Who a primary-elect may ask for an op its merged log names. A `DoViewChange`
    //! suffix spans `commit..=op`, so the two selectors cover disjoint halves of
    //! the merged log and the split is what keeps a coverage gap under the merged
    //! commit point askable at all.

    use super::*;
    use crate::LocalPipeline;
    use crate::view_change_quorum::{DvcSuffix, StoredDvc, dvc_record};

    use crate::test_bus::NoopBus;

    fn consensus() -> VsrConsensus<NoopBus, LocalPipeline> {
        VsrConsensus::new(1, 0, 3, METADATA_GROUP, NoopBus, LocalPipeline::new())
    }

    /// A sender whose suffix runs `commit..=op` with every body offered, which is
    /// the widest window a real `DoViewChange` can carry.
    fn sender(replica: u8, log_view: u32, op: u64, commit: u64) -> StoredDvc {
        let headers: Vec<PrepareHeader> = (commit..=op)
            .rev()
            .map(|op| PrepareHeader {
                command: Command::Prepare,
                op,
                view: log_view,
                ..Default::default()
            })
            .collect();
        let present = (1u128 << headers.len()) - 1;
        StoredDvc {
            replica,
            log_view,
            op,
            commit,
            suffix: DvcSuffix::new(headers, 0, present),
            untrusted_log: false,
        }
    }

    fn record(consensus: &VsrConsensus<NoopBus, LocalPipeline>, senders: [StoredDvc; 2]) {
        let mut quorum = consensus.do_view_change_from_all_replicas.borrow_mut();
        for dvc in senders {
            assert!(dvc_record(&mut quorum, dvc));
        }
    }

    #[test]
    fn given_an_op_inside_the_suffixes_when_selecting_should_return_the_body_offers() {
        let consensus = consensus();
        record(&consensus, [sender(1, 5, 12, 10), sender(2, 4, 12, 10)]);

        assert_eq!(
            consensus.pending_view_body_sources(11),
            vec![1, 2],
            "both senders offer op 11, freshest log_view first"
        );
    }

    #[test]
    fn given_an_op_below_every_commit_point_when_selecting_should_need_the_committers() {
        let consensus = consensus();
        record(&consensus, [sender(1, 5, 12, 10), sender(2, 4, 12, 10)]);

        assert!(
            consensus.pending_view_body_sources(7).is_empty(),
            "a suffix spans commit..=op, so it says nothing about op 7"
        );
        assert_eq!(
            consensus.pending_view_commit_sources(7),
            vec![1, 2],
            "a sender that committed op 7 holds it or compacted it, and either \
             answer moves the requester forward"
        );
    }

    #[test]
    fn given_a_sender_behind_the_op_when_selecting_committers_should_skip_it() {
        let consensus = consensus();
        record(&consensus, [sender(1, 5, 12, 10), sender(2, 6, 6, 5)]);

        assert_eq!(
            consensus.pending_view_commit_sources(7),
            vec![1],
            "replica 2 never committed op 7, so asking it wastes a retry interval \
             on a RangeEvicted it has no standing to send"
        );
    }

    #[test]
    fn given_this_replica_in_the_quorum_when_selecting_should_never_return_self() {
        let consensus = consensus();
        record(&consensus, [sender(0, 5, 12, 10), sender(2, 4, 12, 10)]);

        assert_eq!(
            consensus.pending_view_commit_sources(7),
            vec![2],
            "a replica cannot repair from itself"
        );
    }
}

#[cfg(test)]
mod recovery_barrier_tests {
    //! The gate holding a restarted replica's reads and writes until the recovered
    //! WAL suffix re-commits. Boot arms it at the recovered head; only a view that
    //! settles that suffix's fate may move it, and only downward.

    use super::*;
    use crate::LocalPipeline;
    use crate::test_bus::{NoopBus, make_start_view};

    /// Recovered at head 120, proven committed only through 100, in view 7.
    fn recovered_with_gated_suffix() -> VsrConsensus<NoopBus, LocalPipeline> {
        let mut consensus =
            VsrConsensus::new(1, 0, 3, METADATA_GROUP, NoopBus, LocalPipeline::new());
        consensus.set_view(7);
        consensus.set_log_view(7);
        consensus.sequencer().set_sequence(120);
        consensus.restore_commit_state(100, 100);
        consensus.set_recovery_barrier(120);
        consensus
    }

    /// The `commit_max >= recovery_barrier` clause of `is_caught_up_primary`, read
    /// directly so these tests need not satisfy the primary/status clauses.
    fn write_gate_open<B: MessageBus, P>(consensus: &VsrConsensus<B, P>) -> bool
    where
        P: Pipeline<Entry = PipelineEntry>,
    {
        consensus.commit_max() >= consensus.recovery_barrier()
    }

    /// The wedge `redecide_recovery_barrier` exists for: a view change discards the
    /// recovered suffix, the replica later wins an election, and a barrier pinned
    /// to a head that no longer exists shuts admission.
    #[test]
    fn given_a_discarded_suffix_when_adopting_a_view_should_lower_the_barrier() {
        let consensus = recovered_with_gated_suffix();

        // The view's head is 105: 106..=120 committed nowhere, so the view drops
        // them and nothing re-prepares them.
        assert!(
            !consensus
                .handle_start_view(
                    PlaneKind::Metadata,
                    make_start_view(7, 105, 105, 1, 0).header(),
                    &[]
                )
                .is_empty(),
            "the StartView at the commit floor must be adopted"
        );
        assert_eq!(
            consensus.recovery_barrier(),
            105,
            "the adopted head settled the suffix's fate, so the barrier must fall to \
             it rather than latch at a head the view discarded"
        );
        assert!(
            write_gate_open(&consensus),
            "a barrier the adopted commit point covers must stop gating admission"
        );
        assert!(
            consensus.commit_min() < consensus.recovery_barrier(),
            "adoption raises commit_max before applying, so the local read gate \
             (which reads commit_min) must still hold"
        );
    }

    /// The other half: lowering the barrier to the adopted head must not read as
    /// clearing it while the suffix survives.
    #[test]
    fn given_a_surviving_suffix_when_adopting_a_view_should_keep_the_barrier() {
        let consensus = recovered_with_gated_suffix();

        // Head 120, commit still 100: 101..=120 re-replicate under the new view.
        assert!(
            !consensus
                .handle_start_view(
                    PlaneKind::Metadata,
                    make_start_view(7, 120, 100, 1, 0).header(),
                    &[]
                )
                .is_empty(),
            "the StartView carrying the surviving suffix must be adopted"
        );
        assert_eq!(
            consensus.recovery_barrier(),
            120,
            "a suffix the view kept is still unproven, so its gate must stand"
        );
        assert!(
            !write_gate_open(&consensus),
            "an unproven suffix must keep admission shut"
        );
    }

    #[test]
    fn given_no_recovered_suffix_when_redeciding_should_stay_disarmed() {
        // Nothing gated, so no view change may invent a gate.
        let consensus = VsrConsensus::new(1, 0, 3, METADATA_GROUP, NoopBus, LocalPipeline::new());
        consensus.redecide_recovery_barrier(9);
        assert_eq!(consensus.recovery_barrier(), 0);
    }

    /// Zero is disarmed, not met: `barrier_state` skips the `commit_min` comparison
    /// on it, so writing it opens the HTTP read gate over the window the barrier
    /// holds.
    #[test]
    fn given_a_zero_head_when_redeciding_should_leave_the_barrier_armed() {
        let consensus = recovered_with_gated_suffix();
        consensus.redecide_recovery_barrier(0);
        assert_eq!(
            consensus.recovery_barrier(),
            120,
            "a zero head must not disarm the gate"
        );
    }
}

#[cfg(test)]
mod probe_answer_tests {
    //! What a primary publishes when it answers a `RequestStartView` probe.
    //!
    //! The answer is not a bare frontier report. A prober can hold a different
    //! entry at an op at or below the announced commit point, left from a view
    //! whose entry the change truncated, and the canonical headers are its only
    //! way to find that out before it adopts the commit point.

    use super::*;
    use crate::LocalPipeline;
    use crate::test_bus::NoopBus;
    use crate::view_change_quorum::dvc_blank;

    const PROBER: u8 = 1;

    /// Replica 0 of 3, primary in view 0 with head `head` and commit point
    /// `commit`, carrying a fresh suffix snapshot over `[commit + 1, head]`.
    fn primary_with_suffix(head: u64, commit: u64) -> VsrConsensus<NoopBus, LocalPipeline> {
        let consensus = VsrConsensus::new(1, 0, 3, METADATA_GROUP, NoopBus, LocalPipeline::new());
        consensus.init();
        consensus.sequencer().set_sequence(head);
        consensus.advance_commit_max(commit);
        // Tagged on `(head, commit)` as they stand now, so `local_dvc_suffix`
        // returns it rather than falling back to empty.
        let headers: Vec<PrepareHeader> = (commit + 1..=head).rev().map(dvc_blank).collect();
        let present = (1u128 << headers.len()) - 1;
        consensus.set_local_dvc_suffix(DvcSuffix::new(headers, 0, present));
        consensus
    }

    #[allow(clippy::cast_possible_truncation)]
    fn probe(view: u32) -> RequestStartViewHeader {
        RequestStartViewHeader {
            checksum: 0,
            checksum_body: 0,
            cluster: 1,
            size: size_of::<RequestStartViewHeader>() as u32,
            view,
            release: 0,
            command: Command::RequestStartView,
            replica: PROBER,
            reserved_frame: [0; 66],
            group: METADATA_GROUP,
            partition_incarnation: 0,
            reserved: [0; 96],
            incarnation: 0,
        }
    }

    /// The suffix is the whole point: without it the prober cannot tell its own
    /// op from the view's op at the same number, adopts the commit point, and
    /// commits the stale entry.
    #[test]
    fn given_a_cached_suffix_when_answering_a_probe_should_publish_its_headers() {
        let consensus = primary_with_suffix(7, 5);

        let actions = consensus.handle_request_start_view(PlaneKind::Metadata, &probe(0));

        let [
            VsrAction::SendStartView {
                op,
                commit,
                suffix,
                target,
                ..
            },
        ] = &actions[..]
        else {
            panic!("a probe from a backup must be answered with one StartView: {actions:?}");
        };
        assert_eq!(*op, 7);
        assert_eq!(*commit, 5);
        assert_eq!(*target, Some(PROBER));
        assert_eq!(
            suffix.iter().map(|header| header.op).collect::<Vec<_>>(),
            vec![7, 6],
            "the answer must carry the view's canonical headers, high op first"
        );
    }

    /// `commit_max` legitimately runs ahead of the head, since a replica learns
    /// the commit point before it holds the prepares. `StartViewHeader::validate`
    /// refuses `commit > op`, and the dispatcher turns that refusal into a panic,
    /// so the announcement clamps the way `DoViewChange` already does.
    #[test]
    fn given_a_commit_point_above_the_head_when_answering_a_probe_should_clamp_it() {
        let consensus = primary_with_suffix(7, 5);
        consensus.advance_commit_max(9);
        assert!(consensus.commit_max() > consensus.sequencer().current_sequence());

        let actions = consensus.handle_request_start_view(PlaneKind::Metadata, &probe(0));

        let [VsrAction::SendStartView { op, commit, .. }] = &actions[..] else {
            panic!("expected one StartView: {actions:?}");
        };
        assert!(
            commit <= op,
            "announced commit {commit} exceeds head {op}, which StartViewHeader::validate \
             rejects and the dispatcher panics on"
        );
    }
}

#[cfg(test)]
mod recovery_election_tests {
    //! The probe-exhaustion election of a restarted replica.
    //!
    //! A replica that restarts probes for the current view and, when nobody
    //! answers, elects on its recovered log as though the whole cluster had
    //! restarted. Under `replicated` durability that log can be shorter than what
    //! the replica acked before the crash, so the fallback is safe only when the
    //! quorum it elects with still holds every committed op.

    use super::*;
    use crate::LocalPipeline;
    use crate::dvc_merge::suffix_all_present;
    use crate::test_bus::NoopBus;
    use crate::view_change_quorum::encode_prepare_headers;
    use iggy_binary_protocol::{CommitHeader, HEADER_SIZE, Operation};

    const REPLICA_COUNT: u8 = 3;
    /// Primary of `OLD_VIEW`, crashed holding only part of what it committed.
    const RESTARTED_PRIMARY: u8 = 0;
    /// Crashed earlier, behind the committed tail.
    const LAGGING_BACKUP: u8 = 1;
    /// The only replica that never crashed, and the only one holding the tail.
    const SURVIVOR: u8 = 2;
    const OLD_VIEW: u32 = 3;
    /// `RESTARTED_PRIMARY` committed through here with `SURVIVOR`'s acks.
    const COMMITTED: u64 = 10;
    /// What `RESTARTED_PRIMARY`'s WAL held at the crash: its own appends of
    /// `RECOVERED_HEAD + 1..=COMMITTED` were queued but not yet written, which
    /// `replicated` durability acknowledges.
    const RECOVERED_HEAD: u64 = 7;
    /// `LAGGING_BACKUP` crashed before the tail was prepared.
    const LAGGING_HEAD: u64 = 5;
    /// Enough for several probe exhaustions and status escalations.
    const TICKS: u64 =
        4 * TimeoutManager::REQUEST_START_VIEW_MESSAGE_TICKS * PROBE_ATTEMPTS_MAX as u64;

    fn group() -> u64 {
        IggyNamespace::new(0, 0, 0).inner()
    }

    /// Op `op` of the log every replica shares up to its own head, hash-chained
    /// from op 1 so the merge accepts it.
    fn prepare(op: u64) -> PrepareHeader {
        let mut parent = 0;
        let mut header = PrepareHeader::default();
        for at in 1..=op {
            header = PrepareHeader {
                cluster: 1,
                command: Command::Prepare,
                operation: Operation::SendMessages,
                op: at,
                view: OLD_VIEW,
                parent,
                commit: at - 1,
                timestamp: at,
                ..Default::default()
            };
            header.checksum = header.identity_checksum();
            parent = header.checksum;
        }
        header
    }

    /// A replica rebooted from a WAL ending at `head`, joining as the boot path
    /// does when the WAL recovered a frontier: probing, with the fallback
    /// election allowed. `log_complete` is false under `replicated` durability,
    /// which acknowledges a prepare before its write reaches the disk.
    fn restarted(
        replica: u8,
        head: u64,
        log_complete: bool,
    ) -> VsrConsensus<NoopBus, LocalPipeline> {
        let commit = prepare(head).commit;
        let mut consensus = VsrConsensus::new(
            1,
            replica,
            REPLICA_COUNT,
            group(),
            NoopBus,
            LocalPipeline::new(),
        );
        consensus.restore_view(ViewRestore {
            durable_view: Some((OLD_VIEW, OLD_VIEW)),
            view_fallback: None,
            seed_view: None,
        });
        consensus.sequencer().set_sequence(head);
        consensus.restore_commit_state(commit, commit);
        consensus.set_last_prepare_checksum(prepare(head).checksum);
        consensus.set_local_dvc_suffix(suffix_all_present(
            (commit..=head).rev().map(prepare).collect(),
        ));
        consensus.set_recovery_election_allowed(true);
        consensus.set_recovered_log_complete(log_complete);
        consensus.join(JoinMode::ProbeAsBackup {
            await_state_transfer: false,
        });
        consensus
    }

    /// A backup of `OLD_VIEW` holding the whole committed log, left in the view
    /// change its primary's crash started and escalated while nobody else was up.
    fn survivor_in_view_change(plane: PlaneKind) -> VsrConsensus<NoopBus, LocalPipeline> {
        let mut consensus = VsrConsensus::new(
            1,
            SURVIVOR,
            REPLICA_COUNT,
            group(),
            NoopBus,
            LocalPipeline::new(),
        );
        consensus.set_view(OLD_VIEW);
        consensus.set_log_view(OLD_VIEW);
        consensus.sequencer().set_sequence(COMMITTED);
        consensus.restore_commit_state(COMMITTED - 1, COMMITTED - 1);
        consensus.set_local_dvc_suffix(suffix_all_present(
            (COMMITTED - 1..=COMMITTED).rev().map(prepare).collect(),
        ));
        consensus.init();
        while consensus.view() <= OLD_VIEW + 1 {
            consensus.tick(plane);
        }
        assert_eq!(consensus.status(), Status::ViewChange);
        consensus
    }

    fn message<H: ConsensusHeader>(body: usize, fill: impl FnOnce(&mut H)) -> Message<H> {
        Message::<H>::new(HEADER_SIZE + body).transmute_header(|_, header: &mut H| fill(header))
    }

    /// Messages one `Cluster::deliver` may carry before it counts as a loop:
    /// orders of magnitude above what one tick of three replicas provokes.
    const DELIVERIES_PER_ROUND_MAX: usize = 10_000;

    struct DeliveredDoViewChange {
        from: u8,
        to: u8,
        view: u32,
        untrusted_log: bool,
    }

    struct Cluster {
        replicas: Vec<VsrConsensus<NoopBus, LocalPipeline>>,
        plane: PlaneKind,
        /// Every `DoViewChange` delivered.
        do_view_changes: Vec<DeliveredDoViewChange>,
        /// Deliver each `DoViewChange` to every peer rather than to its target,
        /// as the shard dispatches it.
        broadcast_do_view_changes: bool,
        /// Messages delivered so far.
        delivered: usize,
    }

    impl Cluster {
        fn replica(&self, index: u8) -> &VsrConsensus<NoopBus, LocalPipeline> {
            &self.replicas[usize::from(index)]
        }

        /// Deliver `outbox` and every reply it provokes, dropping whatever
        /// `reachable` rules out, until nothing is left in flight.
        fn deliver(
            &mut self,
            mut outbox: VecDeque<(u8, VsrAction)>,
            reachable: impl Fn(u8, u8) -> bool,
        ) {
            let mut round = 0;
            while let Some((from, action)) = outbox.pop_front() {
                for to in 0..REPLICA_COUNT {
                    if to == from || !reachable(from, to) {
                        continue;
                    }
                    round += 1;
                    assert!(
                        round <= DELIVERIES_PER_ROUND_MAX,
                        "unbounded message loop: {round} messages provoked in one round, \
                         the last {action:?} from replica {from} to {to}"
                    );
                    self.delivered += 1;
                    for reply in self.receive(from, to, &action) {
                        outbox.push_back((to, reply));
                    }
                }
                // The shard starts a parked view once repair covers the merged
                // log. Every op in it is offered by a quorum member here, so the
                // repair is assumed to succeed.
                for index in 0..REPLICA_COUNT {
                    for action in self.replica(index).start_pending_view(self.plane) {
                        outbox.push_back((index, action));
                    }
                }
            }
        }

        #[expect(
            clippy::too_many_lines,
            reason = "one arm per message kind the harness delivers"
        )]
        fn receive(&mut self, from: u8, to: u8, action: &VsrAction) -> Vec<VsrAction> {
            let plane = self.plane;
            let receiver = self.replica(to);
            match action {
                VsrAction::SendStartViewChange { view, group } => {
                    let svc = message(0, |header: &mut StartViewChangeHeader| {
                        header.command = Command::StartViewChange;
                        header.cluster = 1;
                        header.size = u32::try_from(HEADER_SIZE).unwrap();
                        header.replica = from;
                        header.view = *view;
                        header.group = *group;
                    });
                    receiver.handle_start_view_change(plane, svc.header())
                }
                VsrAction::SendDoViewChange {
                    view,
                    target,
                    log_view,
                    op,
                    commit,
                    group,
                    suffix,
                    untrusted_log,
                } if self.broadcast_do_view_changes || *target == to => {
                    let mut dvc =
                        message(suffix.encoded_len(), |header: &mut DoViewChangeHeader| {
                            header.command = Command::DoViewChange;
                            header.cluster = 1;
                            header.size =
                                u32::try_from(HEADER_SIZE + suffix.encoded_len()).unwrap();
                            header.replica = from;
                            header.view = *view;
                            header.log_view = *log_view;
                            header.op = *op;
                            header.commit = *commit;
                            header.group = *group;
                            if *untrusted_log {
                                header.flags = DoViewChangeHeader::FLAG_UNTRUSTED_LOG;
                            }
                            header.nack_bitset = suffix.nack_bitset();
                            header.present_bitset = suffix.present_bitset();
                        });
                    suffix.encode_into(&mut dvc.as_mut_slice()[HEADER_SIZE..]);
                    self.do_view_changes.push(DeliveredDoViewChange {
                        from,
                        to,
                        view: *view,
                        untrusted_log: *untrusted_log,
                    });
                    self.replica(to).handle_do_view_change(
                        plane,
                        dvc.header(),
                        &dvc.as_slice()[HEADER_SIZE..],
                    )
                }
                VsrAction::SendRequestStartView { view, group } => {
                    let probe = message(0, |header: &mut RequestStartViewHeader| {
                        header.command = Command::RequestStartView;
                        header.cluster = 1;
                        header.size = u32::try_from(HEADER_SIZE).unwrap();
                        header.replica = from;
                        header.view = *view;
                        header.group = *group;
                    });
                    receiver.handle_request_start_view(plane, probe.header())
                }
                VsrAction::SendStartView {
                    view,
                    op,
                    commit,
                    incarnation,
                    target,
                    group,
                    suffix,
                } if target.is_none_or(|target| target == to) => {
                    let body = suffix.len() * size_of::<PrepareHeader>();
                    let mut start_view = message(body, |header: &mut StartViewHeader| {
                        header.command = Command::StartView;
                        header.cluster = 1;
                        header.size = u32::try_from(HEADER_SIZE + body).unwrap();
                        header.replica = from;
                        header.view = *view;
                        header.op = *op;
                        header.commit = *commit;
                        header.incarnation = *incarnation;
                        header.group = *group;
                    });
                    encode_prepare_headers(suffix, &mut start_view.as_mut_slice()[HEADER_SIZE..]);
                    let actions = receiver.handle_start_view(
                        plane,
                        start_view.header(),
                        &start_view.as_slice()[HEADER_SIZE..],
                    );
                    // No journal here to repair from: the adopted log stands in
                    // for the bodies a real backup would fetch before it
                    // settles its proof and trusts its log again.
                    receiver.untrusted_log.set(false);
                    actions
                }
                // Heartbeats keep a settled view settled; nothing else in this
                // harness carries them.
                VsrAction::SendCommit {
                    view,
                    commit,
                    group,
                    timestamp_monotonic,
                    target,
                } if target.is_none_or(|target| target == to) => {
                    let heartbeat = message(0, |header: &mut CommitHeader| {
                        header.command = Command::Commit;
                        header.cluster = 1;
                        header.size = u32::try_from(HEADER_SIZE).unwrap();
                        header.replica = from;
                        header.view = *view;
                        header.commit = *commit;
                        header.group = *group;
                        header.timestamp_monotonic = *timestamp_monotonic;
                    });
                    let _ = receiver.handle_commit(heartbeat.header());
                    Vec::new()
                }
                _ => Vec::new(),
            }
        }

        /// The invariant: a replica settled in a view newer than `OLD_VIEW` holds
        /// a log covering every op committed in `OLD_VIEW`.
        fn assert_committed_ops_survive(&self, tick: u64) {
            for replica in &self.replicas {
                if replica.status() != Status::Normal || replica.log_view() <= OLD_VIEW {
                    continue;
                }
                let head = replica.sequencer().current_sequence();
                let voters: Vec<u8> = self
                    .do_view_changes
                    .iter()
                    .filter(|delivered| delivered.view == replica.log_view())
                    .map(|delivered| delivered.from)
                    .chain(std::iter::once(replica.primary_index(replica.log_view())))
                    .collect::<std::collections::BTreeSet<_>>()
                    .into_iter()
                    .collect();
                assert!(
                    head >= COMMITTED,
                    "tick {tick}: replica {} is Normal in view {} with head {head}, dropping \
                     ops {}..={COMMITTED} committed in view {OLD_VIEW}. The view was formed from \
                     the DoViewChanges of replicas {voters:?}; only replica {SURVIVOR} (status \
                     {:?}, view {}) holds those ops",
                    replica.replica(),
                    replica.log_view(),
                    head + 1,
                    self.replica(SURVIVOR).status(),
                    self.replica(SURVIVOR).view(),
                );
            }
        }
    }

    impl Cluster {
        /// Tick every replica `ticks` times, delivering what `reachable` allows
        /// and checking the committed-ops invariant after every tick.
        fn run(&mut self, ticks: u64, reachable: impl Fn(u8, u8) -> bool) {
            for tick in 0..ticks {
                let mut outbox = VecDeque::new();
                for index in 0..REPLICA_COUNT {
                    for action in self.replica(index).tick(self.plane) {
                        outbox.push_back((index, action));
                    }
                }
                self.deliver(outbox, &reachable);
                self.assert_committed_ops_survive(tick);
            }
        }

        /// Every replica settled in one view newer than `OLD_VIEW`, holding at
        /// least `head`, and trusting its log again.
        fn assert_settled(&self, head: u64) {
            let view = self.replica(0).view();
            for replica in &self.replicas {
                assert_eq!(
                    replica.status(),
                    Status::Normal,
                    "replica {} never settled",
                    replica.replica()
                );
                assert_eq!(replica.view(), view, "replicas settled in different views");
                assert!(view > OLD_VIEW);
                assert!(replica.sequencer().current_sequence() >= head);
                assert!(!replica.has_untrusted_log());
            }
        }
    }

    /// Two replicas restart after a double crash, the primary having lost the
    /// tail of what it committed. The survivor holding that tail is mid view
    /// change, so it answers no probe. Its view-change traffic reaches the
    /// restarted pair while they probe (and is ignored) and is then delayed past
    /// their fallback election, which an asynchronous network permits. Whatever
    /// the restarted pair decide without it must still contain the committed ops.
    #[test]
    fn given_survivor_in_view_change_when_restarted_pair_elects_should_keep_committed_ops() {
        let plane = PlaneKind::Partitions;
        let mut cluster = Cluster {
            replicas: vec![
                restarted(RESTARTED_PRIMARY, RECOVERED_HEAD, false),
                restarted(LAGGING_BACKUP, LAGGING_HEAD, false),
                survivor_in_view_change(plane),
            ],
            plane,
            do_view_changes: Vec::new(),
            broadcast_do_view_changes: false,
            delivered: 0,
        };
        assert_eq!(
            cluster.replica(RESTARTED_PRIMARY).status(),
            Status::Recovering
        );
        assert_eq!(cluster.replica(LAGGING_BACKUP).status(), Status::Recovering);

        for tick in 0..TICKS {
            let mut outbox = VecDeque::new();
            for index in 0..REPLICA_COUNT {
                for action in cluster.replica(index).tick(plane) {
                    outbox.push_back((index, action));
                }
            }
            let probing = [RESTARTED_PRIMARY, LAGGING_BACKUP]
                .iter()
                .all(|index| cluster.replica(*index).status() == Status::Recovering);
            cluster.deliver(outbox, |from, _| probing || from != SURVIVOR);
            cluster.assert_committed_ops_survive(tick);
        }
    }

    /// The double crash again, with the survivor's traffic never lost. The
    /// restarted pair may not settle a view without it, and once it reports
    /// the cluster settles holding every committed op.
    #[test]
    fn given_survivor_in_view_change_when_it_stays_reachable_should_settle_with_committed_ops() {
        let plane = PlaneKind::Partitions;
        let mut cluster = Cluster {
            replicas: vec![
                restarted(RESTARTED_PRIMARY, RECOVERED_HEAD, false),
                restarted(LAGGING_BACKUP, LAGGING_HEAD, false),
                survivor_in_view_change(plane),
            ],
            plane,
            do_view_changes: Vec::new(),
            broadcast_do_view_changes: false,
            delivered: 0,
        };

        cluster.run(TICKS, |_, _| true);

        cluster.assert_settled(COMMITTED);
    }

    /// The survivor is cut off for good. The restarted pair, a quorum by
    /// count, must stay unavailable rather than settle on their short logs, and
    /// recover as soon as the survivor is back.
    #[test]
    fn given_survivor_partitioned_when_restarted_pair_elects_should_halt_until_it_returns() {
        let plane = PlaneKind::Partitions;
        let mut cluster = Cluster {
            replicas: vec![
                restarted(RESTARTED_PRIMARY, RECOVERED_HEAD, false),
                restarted(LAGGING_BACKUP, LAGGING_HEAD, false),
                survivor_in_view_change(plane),
            ],
            plane,
            do_view_changes: Vec::new(),
            broadcast_do_view_changes: false,
            delivered: 0,
        };

        cluster.run(TICKS, |from, to| from != SURVIVOR && to != SURVIVOR);

        for index in [RESTARTED_PRIMARY, LAGGING_BACKUP] {
            let replica = cluster.replica(index);
            assert_eq!(replica.status(), Status::ViewChange);
            assert!(
                replica.has_untrusted_log(),
                "the fallback election left replica {index} trusting a log that may lack \
                 acknowledged ops"
            );
        }

        cluster.run(TICKS, |_, _| true);

        cluster.assert_settled(COMMITTED);
    }

    /// Every replica restarted together and nobody is left to answer a probe.
    /// The fallback election must still settle, on the longest log: under
    /// `replicated` durability an op the shorter logs lack may have been
    /// acknowledged by them before their crash.
    #[test]
    fn given_full_cluster_restart_when_every_replica_reports_should_settle_on_the_longest_log() {
        let plane = PlaneKind::Partitions;
        let mut cluster = Cluster {
            replicas: vec![
                restarted(RESTARTED_PRIMARY, COMMITTED, false),
                restarted(LAGGING_BACKUP, LAGGING_HEAD, false),
                restarted(SURVIVOR, RECOVERED_HEAD, false),
            ],
            plane,
            do_view_changes: Vec::new(),
            broadcast_do_view_changes: false,
            delivered: 0,
        };

        cluster.run(TICKS, |_, _| true);

        cluster.assert_settled(COMMITTED);
    }

    /// A full restart where one replica's partition boots without the fallback
    /// election, awaiting a state transfer. Every log is untrusted, so a view
    /// needs every replica, and the barred replica supplies its share as an
    /// empty vote: the view starts on the logs that elected, and the barred
    /// replica joins it through the `StartView`.
    #[test]
    fn given_full_restart_with_one_replica_barred_from_election_when_logs_are_untrusted_should_settle_on_its_empty_vote_without_dropping_ops()
     {
        let plane = PlaneKind::Partitions;
        let barred = restarted(SURVIVOR, COMMITTED, false);
        barred.set_recovery_election_allowed(false);
        let mut cluster = Cluster {
            replicas: vec![
                restarted(RESTARTED_PRIMARY, COMMITTED, false),
                restarted(LAGGING_BACKUP, LAGGING_HEAD, false),
                barred,
            ],
            plane,
            do_view_changes: Vec::new(),
            broadcast_do_view_changes: false,
            delivered: 0,
        };

        cluster.run(TICKS, |_, _| true);

        cluster.assert_settled(COMMITTED);
        let empty_votes = cluster
            .do_view_changes
            .iter()
            .filter(|delivered| delivered.from == SURVIVOR)
            .count();
        assert!(empty_votes > 0, "the barred replica never voted");
        assert!(
            cluster
                .do_view_changes
                .iter()
                .filter(|delivered| delivered.from == SURVIVOR)
                .all(|delivered| delivered.untrusted_log),
            "an empty vote must never read as a trusted log"
        );
    }

    /// The barred replica is the candidate of the first view the others elect.
    /// It may vote but never lead: it stays `Recovering` until a `StartView`
    /// from another primary, so the others move past its view and settle.
    #[test]
    fn given_barred_replica_is_the_next_primary_candidate_when_others_elect_should_never_lead() {
        let plane = PlaneKind::Partitions;
        let barred = restarted(LAGGING_BACKUP, COMMITTED, false);
        barred.set_recovery_election_allowed(false);
        assert_eq!(barred.primary_index(OLD_VIEW + 1), LAGGING_BACKUP);
        let mut cluster = Cluster {
            replicas: vec![
                restarted(RESTARTED_PRIMARY, COMMITTED, false),
                barred,
                restarted(SURVIVOR, RECOVERED_HEAD, false),
            ],
            plane,
            do_view_changes: Vec::new(),
            broadcast_do_view_changes: false,
            delivered: 0,
        };

        for tick in 0..TICKS {
            let mut outbox = VecDeque::new();
            for index in 0..REPLICA_COUNT {
                for action in cluster.replica(index).tick(plane) {
                    outbox.push_back((index, action));
                }
            }
            cluster.deliver(outbox, |_, _| true);
            cluster.assert_committed_ops_survive(tick);
            let barred = cluster.replica(LAGGING_BACKUP);
            assert!(
                barred.status() == Status::Recovering
                    || (barred.status() == Status::Normal && !barred.is_primary()),
                "tick {tick}: the barred replica left Recovering on its own: status {:?}, view {}",
                barred.status(),
                barred.view()
            );
        }

        cluster.assert_settled(COMMITTED);
        assert!(!cluster.replica(LAGGING_BACKUP).is_primary());
    }

    /// Cut off from everyone but the barred replica's own SVC and DVC traffic,
    /// an empty voter stays `Recovering` and in its own view: voting is not
    /// joining.
    #[test]
    fn given_barred_replica_when_it_only_hears_a_view_change_should_stay_recovering() {
        let plane = PlaneKind::Partitions;
        let barred = restarted(SURVIVOR, COMMITTED, false);
        barred.set_recovery_election_allowed(false);
        let mut cluster = Cluster {
            replicas: vec![
                restarted(RESTARTED_PRIMARY, COMMITTED, false),
                restarted(LAGGING_BACKUP, COMMITTED, false),
                barred,
            ],
            plane,
            do_view_changes: Vec::new(),
            broadcast_do_view_changes: false,
            delivered: 0,
        };
        let view = cluster.replica(SURVIVOR).view();

        // The barred replica hears everything, its votes reach nobody.
        cluster.run(TICKS, |from, _| from != SURVIVOR);

        let barred = cluster.replica(SURVIVOR);
        assert_eq!(barred.status(), Status::Recovering);
        assert_eq!(barred.view(), view);
        assert!(
            cluster
                .do_view_changes
                .iter()
                .all(|delivered| delivered.from != SURVIVOR)
        );
        for index in [RESTARTED_PRIMARY, LAGGING_BACKUP] {
            assert_eq!(cluster.replica(index).status(), Status::ViewChange);
            assert!(cluster.replica(index).has_untrusted_log());
        }
    }

    /// A restarted replica still probing may yet elect on its log, which can
    /// hold ops nobody else has. It must not cast an empty vote that would let
    /// the others start a view without that log.
    #[test]
    fn given_full_restart_when_a_replica_still_probes_should_wait_for_its_log() {
        let plane = PlaneKind::Partitions;
        let slow = restarted(SURVIVOR, COMMITTED, false);
        slow.set_probe_attempts_max(4 * PROBE_ATTEMPTS_MAX);
        let mut cluster = Cluster {
            replicas: vec![
                restarted(RESTARTED_PRIMARY, RECOVERED_HEAD, false),
                restarted(LAGGING_BACKUP, LAGGING_HEAD, false),
                slow,
            ],
            plane,
            do_view_changes: Vec::new(),
            broadcast_do_view_changes: false,
            delivered: 0,
        };

        cluster.run(2 * TICKS, |_, _| true);

        cluster.assert_settled(COMMITTED);
    }

    /// Persisted recovery never needs the barred replica, so it stays silent
    /// and the trusted pair settle as a plain quorum.
    #[test]
    fn given_complete_logs_when_a_replica_is_barred_should_send_no_empty_vote() {
        let plane = PlaneKind::Partitions;
        let barred = restarted(SURVIVOR, COMMITTED, true);
        barred.set_recovery_election_allowed(false);
        let mut cluster = Cluster {
            replicas: vec![
                restarted(RESTARTED_PRIMARY, COMMITTED, true),
                restarted(LAGGING_BACKUP, LAGGING_HEAD, true),
                barred,
            ],
            plane,
            do_view_changes: Vec::new(),
            broadcast_do_view_changes: false,
            delivered: 0,
        };

        cluster.run(TICKS, |_, _| true);

        cluster.assert_settled(COMMITTED);
        assert!(
            cluster
                .do_view_changes
                .iter()
                .all(|delivered| delivered.from != SURVIVOR && !delivered.untrusted_log)
        );
    }

    /// `persisted` acknowledges only what reached the disk, so a restarted log
    /// holds every op it acknowledged and the restarted pair keep the fast
    /// fallback: they settle without waiting for the survivor.
    #[test]
    fn given_complete_recovered_logs_when_survivor_is_silent_should_elect_without_it() {
        let plane = PlaneKind::Partitions;
        let mut cluster = Cluster {
            replicas: vec![
                restarted(RESTARTED_PRIMARY, COMMITTED, true),
                restarted(LAGGING_BACKUP, LAGGING_HEAD, true),
                survivor_in_view_change(plane),
            ],
            plane,
            do_view_changes: Vec::new(),
            broadcast_do_view_changes: false,
            delivered: 0,
        };

        cluster.run(TICKS, |from, to| from != SURVIVOR && to != SURVIVOR);

        let settled: Vec<u8> = [RESTARTED_PRIMARY, LAGGING_BACKUP]
            .into_iter()
            .filter(|index| {
                let replica = cluster.replica(*index);
                replica.status() == Status::Normal && replica.log_view() > OLD_VIEW
            })
            .collect();
        assert_eq!(settled, vec![RESTARTED_PRIMARY, LAGGING_BACKUP]);
        assert!(
            cluster
                .replicas
                .iter()
                .all(|replica| !replica.has_untrusted_log())
        );
    }

    /// An untrusted primary candidate holding a quorum of `DoViewChange`s waits
    /// for the last one, and the merge keeps the op only that sender holds even
    /// though both restarted senders lack it.
    #[test]
    fn given_untrusted_primary_candidate_when_a_quorum_reports_should_wait_for_every_replica() {
        let plane = PlaneKind::Partitions;
        let candidate = restarted(LAGGING_BACKUP, LAGGING_HEAD, false);
        while candidate.status() == Status::Recovering {
            candidate.tick(plane);
        }
        let view = candidate.view();
        assert_eq!(candidate.primary_index(view), LAGGING_BACKUP);
        assert!(candidate.has_untrusted_log());
        let mut cluster = Cluster {
            replicas: vec![
                restarted(RESTARTED_PRIMARY, RECOVERED_HEAD, false),
                candidate,
                survivor_in_view_change(plane),
            ],
            plane,
            do_view_changes: Vec::new(),
            broadcast_do_view_changes: false,
            delivered: 0,
        };
        let do_view_change =
            |head: u64, commit: u64, untrusted_log: bool| VsrAction::SendDoViewChange {
                view,
                target: LAGGING_BACKUP,
                log_view: OLD_VIEW,
                op: head,
                commit,
                group: group(),
                suffix: suffix_all_present((commit..=head).rev().map(prepare).collect()),
                untrusted_log,
            };

        cluster.receive(
            RESTARTED_PRIMARY,
            LAGGING_BACKUP,
            &do_view_change(RECOVERED_HEAD, RECOVERED_HEAD - 1, true),
        );
        assert!(
            !cluster.replica(LAGGING_BACKUP).view_log_is_pending(),
            "decided a view from {} of {REPLICA_COUNT} DoViewChanges",
            cluster.replica(LAGGING_BACKUP).quorum_view_change()
        );

        cluster.receive(
            SURVIVOR,
            LAGGING_BACKUP,
            &do_view_change(COMMITTED, COMMITTED - 1, false),
        );
        let candidate = cluster.replica(LAGGING_BACKUP);
        assert_eq!(
            candidate.with_pending_view_log(|log| log.op_head),
            Some(COMMITTED),
            "the merge dropped ops only the survivor holds"
        );
        candidate.start_pending_view(plane);
        assert_eq!(candidate.status(), Status::Normal);
        assert!(!candidate.has_untrusted_log());
    }

    /// The lagging backup never restarted, so it trusts its log and is the
    /// candidate of the next view. The restarted primary's `DoViewChange`
    /// completes a quorum by count while the survivor holding the committed tail
    /// is cut off. Flagged untrusted, it does not complete a trusted one, so the
    /// candidate waits for the survivor rather than dropping the tail.
    #[test]
    fn given_trusted_lagging_primary_when_restarted_replica_completes_its_quorum_should_keep_committed_ops()
     {
        let plane = PlaneKind::Partitions;
        let lagging = {
            let mut consensus = VsrConsensus::new(
                1,
                LAGGING_BACKUP,
                REPLICA_COUNT,
                group(),
                NoopBus,
                LocalPipeline::new(),
            );
            consensus.set_view(OLD_VIEW);
            consensus.set_log_view(OLD_VIEW);
            consensus.sequencer().set_sequence(LAGGING_HEAD);
            consensus.restore_commit_state(LAGGING_HEAD - 1, LAGGING_HEAD - 1);
            consensus.set_local_dvc_suffix(suffix_all_present(
                (LAGGING_HEAD - 1..=LAGGING_HEAD)
                    .rev()
                    .map(prepare)
                    .collect(),
            ));
            consensus.set_recovered_log_complete(false);
            consensus.init();
            consensus
        };
        let mut cluster = Cluster {
            replicas: vec![
                restarted(RESTARTED_PRIMARY, RECOVERED_HEAD, false),
                lagging,
                survivor_in_view_change(plane),
            ],
            plane,
            do_view_changes: Vec::new(),
            broadcast_do_view_changes: false,
            delivered: 0,
        };

        cluster.run(TICKS, |from, to| from != SURVIVOR && to != SURVIVOR);

        assert!(!cluster.replica(LAGGING_BACKUP).has_untrusted_log());
        assert!(cluster.replica(RESTARTED_PRIMARY).has_untrusted_log());
        for delivered in &cluster.do_view_changes {
            assert_eq!(
                delivered.untrusted_log,
                delivered.from == RESTARTED_PRIMARY,
                "replica {} sent a DoViewChange misreporting its log",
                delivered.from
            );
        }
        assert!(
            cluster
                .do_view_changes
                .iter()
                .any(|delivered| delivered.from == RESTARTED_PRIMARY)
        );

        cluster.run(TICKS, |_, _| true);

        cluster.assert_settled(COMMITTED);
    }

    /// A trusted candidate counts its own `DoViewChange` as trusted: with one
    /// trusted peer it has a quorum and merges without the untrusted one, even
    /// though that sender reports the longest log.
    #[test]
    fn given_trusted_candidate_when_one_trusted_peer_reports_should_merge_without_the_untrusted_log()
     {
        let plane = PlaneKind::Partitions;
        let candidate = {
            let mut consensus = VsrConsensus::new(
                1,
                LAGGING_BACKUP,
                REPLICA_COUNT,
                group(),
                NoopBus,
                LocalPipeline::new(),
            );
            consensus.set_view(OLD_VIEW);
            consensus.set_log_view(OLD_VIEW);
            consensus.sequencer().set_sequence(RECOVERED_HEAD);
            consensus.restore_commit_state(LAGGING_HEAD, LAGGING_HEAD);
            consensus.set_local_dvc_suffix(suffix_all_present(
                (LAGGING_HEAD..=RECOVERED_HEAD).rev().map(prepare).collect(),
            ));
            consensus.set_recovered_log_complete(false);
            consensus.init();
            consensus
        };
        let view = OLD_VIEW + 1;
        assert_eq!(candidate.primary_index(view), LAGGING_BACKUP);
        let mut cluster = Cluster {
            replicas: vec![
                restarted(RESTARTED_PRIMARY, COMMITTED, false),
                candidate,
                survivor_in_view_change(plane),
            ],
            plane,
            do_view_changes: Vec::new(),
            broadcast_do_view_changes: false,
            delivered: 0,
        };
        let svc = VsrAction::SendStartViewChange {
            view,
            group: group(),
        };
        cluster.receive(SURVIVOR, LAGGING_BACKUP, &svc);
        let do_view_change =
            |head: u64, commit: u64, untrusted_log: bool| VsrAction::SendDoViewChange {
                view,
                target: LAGGING_BACKUP,
                log_view: OLD_VIEW,
                op: head,
                commit,
                group: group(),
                suffix: suffix_all_present((commit..=head).rev().map(prepare).collect()),
                untrusted_log,
            };

        cluster.receive(
            RESTARTED_PRIMARY,
            LAGGING_BACKUP,
            &do_view_change(COMMITTED, COMMITTED - 1, true),
        );
        assert!(
            !cluster.replica(LAGGING_BACKUP).view_log_is_pending(),
            "an untrusted DoViewChange completed the candidate's quorum"
        );

        cluster.receive(
            SURVIVOR,
            LAGGING_BACKUP,
            &do_view_change(RECOVERED_HEAD + 1, LAGGING_HEAD, false),
        );
        assert_eq!(
            cluster
                .replica(LAGGING_BACKUP)
                .with_pending_view_log(|log| log.op_head),
            Some(RECOVERED_HEAD + 1),
            "the merge took the untrusted sender's longer log"
        );
    }

    fn barred(replica: u8, head: u64) -> VsrConsensus<NoopBus, LocalPipeline> {
        let consensus = restarted(replica, head, false);
        consensus.set_recovery_election_allowed(false);
        consensus
    }

    /// Two replicas come back barred from electing. The shard broadcasts every
    /// `DoViewChange`, so each hears the other's empty vote, and answering one
    /// with its own would bounce empty votes between them forever. While the
    /// third replica's view change cannot complete they stay quiet and
    /// `Recovering`; once it can, the group settles.
    #[test]
    fn given_two_barred_replicas_when_do_view_changes_are_broadcast_should_not_answer_each_other() {
        let plane = PlaneKind::Partitions;
        let mut cluster = Cluster {
            replicas: vec![
                restarted(RESTARTED_PRIMARY, COMMITTED, false),
                barred(LAGGING_BACKUP, LAGGING_HEAD),
                barred(SURVIVOR, RECOVERED_HEAD),
            ],
            plane,
            do_view_changes: Vec::new(),
            broadcast_do_view_changes: true,
            delivered: 0,
        };

        // The electing replica's view-change traffic reaches the barred pair,
        // their votes reach only each other.
        cluster.run(TICKS, |from, to| {
            from == RESTARTED_PRIMARY || to != RESTARTED_PRIMARY
        });

        assert!(
            cluster.do_view_changes.iter().any(|delivered| {
                delivered.from != RESTARTED_PRIMARY && delivered.to != RESTARTED_PRIMARY
            }),
            "no empty vote reached the other barred replica"
        );
        for index in [LAGGING_BACKUP, SURVIVOR] {
            assert_eq!(cluster.replica(index).status(), Status::Recovering);
        }
        let quiet = cluster.delivered;
        assert!(
            quiet <= usize::try_from(TICKS).unwrap() * 8,
            "{quiet} messages in {TICKS} ticks"
        );

        cluster.run(TICKS, |_, _| true);

        cluster.assert_settled(COMMITTED);
    }

    /// Lagging backups that never restarted, then heard a newer view and
    /// probed for it. With the third replica gone nobody answers, and the
    /// fallback election must treat their logs as the intact ones they are:
    /// marking them untrusted would halt a group that lost only one replica.
    #[test]
    fn given_replicas_that_never_restarted_when_a_runtime_probe_exhausts_should_elect_trusted_without_the_third()
     {
        let plane = PlaneKind::Partitions;
        let probing = |replica: u8| {
            let mut consensus = VsrConsensus::new(
                1,
                replica,
                REPLICA_COUNT,
                group(),
                NoopBus,
                LocalPipeline::new(),
            );
            consensus.set_view(OLD_VIEW);
            consensus.set_log_view(OLD_VIEW);
            consensus.sequencer().set_sequence(COMMITTED);
            consensus.restore_commit_state(COMMITTED - 1, COMMITTED - 1);
            consensus.set_last_prepare_checksum(prepare(COMMITTED).checksum);
            consensus.set_local_dvc_suffix(suffix_all_present(
                (COMMITTED - 1..=COMMITTED).rev().map(prepare).collect(),
            ));
            consensus.set_recovered_log_complete(false);
            consensus.init();
            consensus.begin_view_probe();
            consensus
        };
        let mut cluster = Cluster {
            replicas: vec![
                probing(RESTARTED_PRIMARY),
                probing(LAGGING_BACKUP),
                survivor_in_view_change(plane),
            ],
            plane,
            do_view_changes: Vec::new(),
            broadcast_do_view_changes: false,
            delivered: 0,
        };

        cluster.run(TICKS, |from, to| from != SURVIVOR && to != SURVIVOR);

        for index in [RESTARTED_PRIMARY, LAGGING_BACKUP] {
            let replica = cluster.replica(index);
            assert_eq!(replica.status(), Status::Normal, "replica {index} halted");
            assert!(replica.log_view() > OLD_VIEW);
            assert!(!replica.has_untrusted_log());
        }
        assert!(
            cluster
                .do_view_changes
                .iter()
                .all(|delivered| !delivered.untrusted_log)
        );
    }

    /// The two continuing replicas of a settled view probe at runtime with the
    /// third gone. Both restarted earlier on logs that may lack acknowledged
    /// ops, but a settled view vouched for them since, so the election must
    /// treat their logs as trusted rather than halt.
    fn assert_runtime_probe_elects_trusted_without_survivor(cluster: &mut Cluster) {
        let settled_view = cluster.replica(RESTARTED_PRIMARY).view();
        for index in [RESTARTED_PRIMARY, LAGGING_BACKUP] {
            cluster.replica(index).begin_view_probe();
        }
        let elected_from = cluster.do_view_changes.len();

        cluster.run(TICKS, |from, to| from != SURVIVOR && to != SURVIVOR);

        for index in [RESTARTED_PRIMARY, LAGGING_BACKUP] {
            let replica = cluster.replica(index);
            assert_eq!(replica.status(), Status::Normal, "replica {index} halted");
            assert!(replica.log_view() > settled_view);
            assert!(!replica.has_untrusted_log());
        }
        assert!(
            cluster.do_view_changes[elected_from..]
                .iter()
                .all(|delivered| !delivered.untrusted_log),
            "a replica that settled after its restart still marked its log untrusted"
        );
    }

    /// Restarted replicas whose probe a live primary answers adopt its
    /// `StartView`, which vouches for their logs. A later runtime probe that
    /// exhausts with that primary gone must elect them trusted.
    #[test]
    fn given_restarted_replicas_that_adopted_a_start_view_when_a_runtime_probe_exhausts_should_elect_trusted_without_the_third()
     {
        const SETTLED_VIEW: u32 = OLD_VIEW + 2;
        let plane = PlaneKind::Partitions;
        let mut primary = VsrConsensus::new(
            1,
            SURVIVOR,
            REPLICA_COUNT,
            group(),
            NoopBus,
            LocalPipeline::new(),
        );
        primary.set_view(SETTLED_VIEW);
        primary.set_log_view(SETTLED_VIEW);
        primary.sequencer().set_sequence(COMMITTED);
        primary.restore_commit_state(COMMITTED - 1, COMMITTED - 1);
        primary.set_last_prepare_checksum(prepare(COMMITTED).checksum);
        primary.set_local_dvc_suffix(suffix_all_present(
            (COMMITTED - 1..=COMMITTED).rev().map(prepare).collect(),
        ));
        primary.init();
        assert!(primary.is_primary());
        let mut cluster = Cluster {
            replicas: vec![
                restarted(RESTARTED_PRIMARY, COMMITTED, false),
                restarted(LAGGING_BACKUP, COMMITTED, false),
                primary,
            ],
            plane,
            do_view_changes: Vec::new(),
            broadcast_do_view_changes: false,
            delivered: 0,
        };

        cluster.run(TICKS, |_, _| true);

        cluster.assert_settled(COMMITTED);
        assert_eq!(cluster.replica(SURVIVOR).view(), SETTLED_VIEW);
        assert!(
            cluster.do_view_changes.is_empty(),
            "the probe was not answered"
        );
        assert_runtime_probe_elects_trusted_without_survivor(&mut cluster);
    }

    /// A full restart settles on untrusted logs once every replica reports: one
    /// continuing replica starts the view as its primary, the other adopts its
    /// `StartView`. A later runtime probe that exhausts with the third gone must
    /// elect both trusted.
    #[test]
    fn given_full_restart_that_settled_when_a_runtime_probe_exhausts_should_elect_trusted_without_the_third()
     {
        let plane = PlaneKind::Partitions;
        let mut cluster = Cluster {
            replicas: vec![
                restarted(RESTARTED_PRIMARY, COMMITTED, false),
                restarted(LAGGING_BACKUP, COMMITTED, false),
                restarted(SURVIVOR, COMMITTED, false),
            ],
            plane,
            do_view_changes: Vec::new(),
            broadcast_do_view_changes: false,
            delivered: 0,
        };

        cluster.run(TICKS, |_, _| true);

        cluster.assert_settled(COMMITTED);
        assert!(
            cluster
                .do_view_changes
                .iter()
                .any(|delivered| delivered.untrusted_log),
            "the full restart elected without marking its logs"
        );
        assert!(
            [RESTARTED_PRIMARY, LAGGING_BACKUP]
                .iter()
                .any(|index| cluster.replica(*index).is_primary()),
            "the settled primary is the replica the runtime probe leaves out"
        );
        assert_runtime_probe_elects_trusted_without_survivor(&mut cluster);
    }

    /// An untrusted candidate can learn its view from a peer's `DoViewChange`
    /// before enough `StartViewChange`s arrive to send its own. The vote it
    /// records for itself must still say untrusted, so one trusted peer is not
    /// a trusted quorum and it waits for every replica.
    #[test]
    fn given_untrusted_candidate_when_a_peer_do_view_change_brings_it_into_the_view_should_record_its_own_vote_untrusted()
     {
        let plane = PlaneKind::Partitions;
        let candidate = restarted(LAGGING_BACKUP, LAGGING_HEAD, false);
        while candidate.status() == Status::Recovering {
            candidate.tick(plane);
        }
        assert!(candidate.has_untrusted_log());
        let view = (candidate.view() + 1..=candidate.view() + u32::from(REPLICA_COUNT))
            .find(|view| candidate.primary_index(*view) == LAGGING_BACKUP)
            .expect("every replica leads one of any run of replica-count views");
        let mut cluster = Cluster {
            replicas: vec![
                restarted(RESTARTED_PRIMARY, RECOVERED_HEAD, false),
                candidate,
                survivor_in_view_change(plane),
            ],
            plane,
            do_view_changes: Vec::new(),
            broadcast_do_view_changes: false,
            delivered: 0,
        };
        let do_view_change =
            |head: u64, commit: u64, untrusted_log: bool| VsrAction::SendDoViewChange {
                view,
                target: LAGGING_BACKUP,
                log_view: OLD_VIEW,
                op: head,
                commit,
                group: group(),
                suffix: suffix_all_present((commit..=head).rev().map(prepare).collect()),
                untrusted_log,
            };

        cluster.receive(
            SURVIVOR,
            LAGGING_BACKUP,
            &do_view_change(COMMITTED, COMMITTED - 1, false),
        );
        let candidate = cluster.replica(LAGGING_BACKUP);
        assert_eq!(candidate.view(), view);
        assert!(
            candidate.do_view_change_from_all_replicas.borrow()[usize::from(LAGGING_BACKUP)]
                .as_ref()
                .expect("the candidate recorded its own vote")
                .untrusted_log
        );
        assert!(
            !candidate.view_log_is_pending(),
            "one trusted peer completed a quorum with the candidate's untrusted log"
        );

        cluster.receive(
            RESTARTED_PRIMARY,
            LAGGING_BACKUP,
            &do_view_change(RECOVERED_HEAD, RECOVERED_HEAD - 1, true),
        );
        assert_eq!(
            cluster
                .replica(LAGGING_BACKUP)
                .with_pending_view_log(|log| log.op_head),
            Some(COMMITTED)
        );
    }

    /// A barred replica votes only for a view it has not moved past and does
    /// not lead.
    #[test]
    fn given_barred_replica_when_a_view_change_is_older_or_its_own_should_send_no_empty_vote() {
        let plane = PlaneKind::Partitions;
        let barred = barred(SURVIVOR, COMMITTED);
        let older = OLD_VIEW - 2;
        let led = OLD_VIEW + 2;
        assert_ne!(barred.primary_index(older), SURVIVOR);
        assert_eq!(barred.primary_index(led), SURVIVOR);
        for (view, votes) in [
            (older, false),
            (OLD_VIEW, true),
            (OLD_VIEW + 1, true),
            (led, false),
        ] {
            let svc = message(0, |header: &mut StartViewChangeHeader| {
                header.command = Command::StartViewChange;
                header.cluster = 1;
                header.size = u32::try_from(HEADER_SIZE).unwrap();
                header.replica = RESTARTED_PRIMARY;
                header.view = view;
                header.group = group();
            });
            let actions = barred.handle_start_view_change(plane, svc.header());
            assert_eq!(
                actions.iter().any(VsrAction::is_empty_vote),
                votes,
                "view {view}: {actions:?}"
            );
            assert_eq!(barred.status(), Status::Recovering);
            assert_eq!(barred.view(), OLD_VIEW);
        }
    }
}

#[cfg(test)]
mod history_certificate_tests {
    use super::*;
    use crate::LocalPipeline;
    use crate::test_bus::NoopBus;

    #[test]
    fn given_a_metadata_replica_when_its_history_moves_should_gate_acks_and_persistence() {
        const ADOPTED_VIEW: u32 = 2;
        let mut consensus =
            VsrConsensus::new(1, 0, 3, METADATA_GROUP, NoopBus, LocalPipeline::new());
        consensus.init();
        let current = |consensus: &VsrConsensus<NoopBus, LocalPipeline>| {
            consensus.vsr_state(0, 0).history_certificate()
        };
        assert!(consensus.history_certified());
        assert!(!consensus.needs_superblock_persist());

        // Following the WAL head forces no write.
        consensus.extend_history_certificate(5, 55);
        assert_eq!(current(&consensus), Some(HistoryCertificate::new(0, 5, 55)));
        assert!(!consensus.needs_superblock_persist());
        consensus.mark_history_certificate_durable(current(&consensus));

        // An adopted view whose log is unproven: no acks, and nothing it journals
        // moves the binding.
        consensus.set_view(ADOPTED_VIEW);
        consensus.set_log_view(ADOPTED_VIEW);
        consensus.mark_superblock_durable(ADOPTED_VIEW, ADOPTED_VIEW);
        assert!(!consensus.history_certified());
        consensus.extend_history_certificate(6, 66);
        assert_eq!(current(&consensus), Some(HistoryCertificate::new(0, 5, 55)));

        // A rewrite off the bound entry: the lowered binding must be durable before
        // the entry goes.
        consensus.lower_history_certificate(5, 4, 44);
        assert_eq!(current(&consensus), Some(HistoryCertificate::new(0, 4, 44)));
        assert!(consensus.needs_superblock_persist());
        consensus.mark_history_certificate_durable(current(&consensus));
        assert!(!consensus.needs_superblock_persist());
        consensus.lower_history_certificate(6, 3, 33);
        assert_eq!(
            current(&consensus),
            Some(HistoryCertificate::new(0, 4, 44)),
            "a rewrite above the binding leaves it"
        );

        // Proven: the view is certified, and recorded before the first ack.
        consensus.certify_view_log(7, 77);
        assert!(consensus.history_certified());
        assert!(consensus.needs_superblock_persist());
        consensus.mark_history_certificate_durable(current(&consensus));
        consensus.extend_history_certificate(8, 88);
        assert_eq!(
            current(&consensus),
            Some(HistoryCertificate::new(ADOPTED_VIEW, 8, 88))
        );
        assert!(!consensus.needs_superblock_persist());
    }

    #[test]
    fn given_a_restart_with_an_unproven_wal_when_restored_should_fence_it_and_stay_out_of_elections()
     {
        let consensus = VsrConsensus::new(1, 0, 3, METADATA_GROUP, NoopBus, LocalPipeline::new());
        consensus.init_as_backup();
        let legacy = VsrState {
            cluster: 1,
            replica_id: 0,
            replica_count: 3,
            view: 4,
            log_view: 4,
            commit_max: 0,
            checkpoint_op: 0,
            checkpoint_checksum: 0,
            offset_frontier: 0,
            offset_reserved: 0,
            adopted_view: None,
        };
        consensus.restore_history(HistoryRecovery::Unproven, (9, 99), Some(&legacy));
        assert!(consensus.view_log_unproven());
        assert!(!consensus.view_log_entry_proven(consensus.commit_min() + 1));
        assert!(!consensus.history_certified());
        assert!(!consensus.needs_superblock_persist());
        assert!(
            !consensus.recovery_election_allowed(),
            "an unanswered probe must not let an unproven log vote"
        );

        consensus.certify_view_log(9, 99);
        assert!(consensus.history_certified());
        assert!(consensus.recovery_election_allowed());
    }

    #[test]
    fn given_a_partition_group_when_recording_should_keep_its_offset_words() {
        let group = IggyNamespace::new(0, 0, 0).inner();
        let mut consensus = VsrConsensus::new(1, 0, 3, group, NoopBus, LocalPipeline::new());
        consensus.init();
        consensus.set_view(1);
        consensus.set_log_view(1);
        assert!(consensus.history_certified());
        let state = consensus.vsr_state(0, 0);
        assert_eq!((state.offset_frontier, state.offset_reserved), (0, 0));
    }
}

#[cfg(test)]
mod adopted_view_claim_tests {
    //! An adopted claim must retain its announced head and exact identity across
    //! elections and recovery. A body certificate alone does not preserve them;
    //! advertising the adopted view with the shorter WAL head loses committed ops.
    //!
    //! A backup that already applied an op must refuse a `StartView` naming a
    //! different prepare there: adopting it would hide the loss.

    use super::*;
    use crate::LocalPipeline;
    use crate::merge_dvc_quorum;
    use crate::test_bus::{NoopBus, make_start_view};
    use crate::view_change_quorum::{DvcSuffix, StoredDvc, dvc_quorum_array_empty, dvc_record};

    /// The view the cluster committed `HEAD` in.
    const OLD_VIEW: u32 = 10;
    /// The view whose `StartView` announced `HEAD` to the backup.
    const ADOPTED_VIEW: u32 = 11;
    const ADOPTED_VIEW_PRIMARY: u8 = 2;
    const BACKUP: u8 = 0;
    /// The view-10 primary, which committed `HEAD` and then restarted.
    const OLD_PRIMARY: u8 = 1;
    /// Committed in `OLD_VIEW`; the backup never receives its body.
    const HEAD: u64 = 26;
    /// The view whose primary-elect the backup becomes.
    const MERGED_VIEW: u32 = ADOPTED_VIEW + 1;
    /// Every group but metadata's runs the partition rules.
    const PARTITION_GROUP: u64 = 0;

    /// Hash-chained metadata prepare headers for ops `1..=HEAD`, indexed by op.
    fn history() -> Vec<PrepareHeader> {
        history_in(METADATA_GROUP)
    }

    /// [`history`] in `group`.
    fn history_in(group: u64) -> Vec<PrepareHeader> {
        let mut headers = vec![PrepareHeader::default()];
        for op in 1..=HEAD {
            let mut header = PrepareHeader {
                command: Command::Prepare,
                cluster: 1,
                group,
                timestamp: op,
                operation: Operation::CreateStream,
                size: u32::try_from(size_of::<PrepareHeader>()).unwrap(),
                op,
                commit: op - 1,
                parent: headers[usize::try_from(op - 1).unwrap()].checksum,
                view: OLD_VIEW,
                ..PrepareHeader::default()
            };
            header.checksum = header.identity_checksum();
            headers.push(header);
        }
        headers
    }

    fn at(history: &[PrepareHeader], op: u64) -> PrepareHeader {
        history[usize::try_from(op).unwrap()]
    }

    /// A suffix for `low..=high` holding every body, high-to-low.
    fn held(history: &[PrepareHeader], low: u64, high: u64) -> DvcSuffix {
        let headers: Vec<PrepareHeader> = (low..=high).rev().map(|op| at(history, op)).collect();
        let present = (1u128 << headers.len()) - 1;
        DvcSuffix::new(headers, 0, present)
    }

    fn plane_of(group: u64) -> PlaneKind {
        if group == METADATA_GROUP {
            PlaneKind::Metadata
        } else {
            PlaneKind::Partitions
        }
    }

    /// A metadata backup in normal status at `OLD_VIEW` whose journal ends at `head`.
    fn backup_at(log_view: u32, view: u32, head: u64, commit: u64) -> VsrConsensus<NoopBus> {
        backup_in(METADATA_GROUP, log_view, view, head, commit)
    }

    /// [`backup_at`] in `group`.
    fn backup_in(
        group: u64,
        log_view: u32,
        view: u32,
        head: u64,
        commit: u64,
    ) -> VsrConsensus<NoopBus> {
        let mut consensus = VsrConsensus::new(1, BACKUP, 3, group, NoopBus, LocalPipeline::new());
        consensus.restore_view(ViewRestore {
            durable_view: Some((view, log_view)),
            view_fallback: None,
            seed_view: None,
        });
        // A partition binds its history in the partition journal instead.
        if group == METADATA_GROUP {
            consensus.restore_history(
                HistoryRecovery::Certified {
                    view: log_view,
                    truncate_from: None,
                },
                (head, 0),
                None,
            );
        }
        consensus.init_as_backup();
        consensus.sequencer().set_sequence(head);
        consensus.restore_commit_state(commit, commit);
        consensus
    }

    /// Adopt the view-11 `StartView` (head `HEAD`, commit `HEAD - 1`) in the group
    /// of `history`, with the journal still ending at op 23.
    fn adopt_unrepaired(history: &[PrepareHeader]) -> VsrConsensus<NoopBus> {
        let group = at(history, HEAD).group;
        let backup = backup_in(group, OLD_VIEW, OLD_VIEW, 23, 22);
        let suffix = [at(history, HEAD), at(history, HEAD - 1)];
        let start_view = make_start_view(ADOPTED_VIEW, HEAD, HEAD - 1, ADOPTED_VIEW_PRIMARY, 0);
        let original = *start_view.header();
        let start_view = start_view.transmute_header(|_, header: &mut StartViewHeader| {
            *header = StartViewHeader { group, ..original };
        });
        backup.handle_start_view(
            plane_of(group),
            start_view.header(),
            bytemuck::cast_slice(&suffix),
        );
        assert_eq!(backup.log_view(), ADOPTED_VIEW);
        assert!(
            backup.view_log_unproven(),
            "the journal does not hold the head yet"
        );
        backup
    }

    fn dvc_of(consensus: &VsrConsensus<NoopBus>) -> StoredDvc {
        let VsrAction::SendDoViewChange {
            log_view,
            op,
            commit,
            suffix,
            ..
        } = consensus.build_do_view_change(BACKUP)
        else {
            unreachable!("build_do_view_change builds a DoViewChange");
        };
        StoredDvc {
            replica: consensus.replica(),
            log_view,
            op,
            commit,
            suffix,
            untrusted_log: false,
        }
    }

    /// The view-10 primary after its restart: `HEAD` journaled, commit point one
    /// below it because nothing it journaled stamps the commit of `HEAD`.
    fn old_primary(history: &[PrepareHeader]) -> StoredDvc {
        StoredDvc {
            replica: OLD_PRIMARY,
            log_view: OLD_VIEW,
            op: HEAD,
            commit: HEAD - 1,
            suffix: held(history, HEAD - 1, HEAD),
            untrusted_log: false,
        }
    }

    fn merged_head(consensus: &VsrConsensus<NoopBus>, senders: [StoredDvc; 2]) -> u64 {
        let mut quorum = dvc_quorum_array_empty();
        for dvc in senders {
            assert!(dvc_record(&mut quorum, dvc));
        }
        match merge_dvc_quorum(&quorum, consensus.merge_quorums()) {
            MergeOutcome::Ready(merged) => merged.op_head,
            outcome => panic!("the quorum holds every body it needs, got {outcome:?}"),
        }
    }

    #[test]
    fn given_an_unrepaired_start_view_head_when_the_backup_restarts_should_keep_the_committed_head()
    {
        let history = history();
        let adopted = adopt_unrepaired(&history);
        // Persisted before any view-scoped send, so this is what survives the crash.
        let durable = adopted.vsr_state(0, 0);

        // Repair reached op 25 before the crash; op 26 never arrived.
        let claimed_view = durable.history_certificate().unwrap().view;
        let restarted = backup_at(claimed_view, durable.view, HEAD - 1, HEAD - 2);
        restarted.restore_adopted_view(durable.adopted_view.as_ref());
        restarted.resume_adopted_view();
        restarted.fence_view_log(HEAD, 0);
        let adopted_headers = restarted.adopted_view_log().unwrap().headers;
        restarted.set_local_dvc_suffix(DvcSuffix::new(adopted_headers, 0, 0b10));
        assert_eq!(restarted.sequencer().current_sequence(), HEAD);
        assert_eq!(restarted.log_view_claim(), ADOPTED_VIEW);

        assert_eq!(
            merged_head(&restarted, [dvc_of(&restarted), old_primary(&history)]),
            HEAD,
            "the restarted backup claimed log_view {} with head {}, so the merge never \
             scanned op {HEAD}, which view {OLD_VIEW} committed",
            durable.log_view,
            HEAD - 1,
        );
    }

    #[test]
    fn given_repaired_adopted_bodies_when_restoring_should_keep_the_older_certificate_binding() {
        const CERTIFIED_HEAD: u64 = HEAD - 3;
        let history = history();
        let adopted = adopt_unrepaired(&history);
        let certificate = HistoryCertificate::new(
            OLD_VIEW,
            CERTIFIED_HEAD,
            at(&history, CERTIFIED_HEAD).checksum,
        );
        let durable = adopted
            .vsr_state(0, 0)
            .with_history_certificate(Some(certificate));
        let recovery =
            HistoryRecovery::new(Some(&durable), false, |op| Some(at(&history, op).checksum));
        let restarted = backup_at(OLD_VIEW, durable.view, HEAD - 1, HEAD - 2);
        restarted.restore_history(
            recovery,
            (HEAD - 1, at(&history, HEAD - 1).checksum),
            Some(&durable),
        );
        restarted.restore_adopted_view(durable.adopted_view.as_ref());
        restarted.resume_adopted_view();
        assert_eq!(
            restarted.vsr_state(0, 0).history_certificate(),
            Some(certificate)
        );
        assert!(restarted.view_log_unproven());
        assert!(!restarted.history_certified());
    }

    #[test]
    fn given_an_unrepaired_start_view_head_when_a_view_change_drops_the_parked_headers_should_keep_the_committed_head()
     {
        let history = history();
        let backup = adopt_unrepaired(&history);
        // Reconcile found op 26 missing; repair brought ops 24 and 25.
        backup.fence_view_log(HEAD, 0);
        backup.enter_view_change(
            PlaneKind::Metadata,
            ADOPTED_VIEW + 1,
            ViewChangeReason::NormalHeartbeatTimeout,
        );
        assert!(backup.pending_view_log().is_none());
        // The accepted header distinguishes a missing body from a truncated op.
        backup.set_local_dvc_suffix(DvcSuffix::new(
            backup.adopted_view_log().unwrap().headers,
            0,
            0b10,
        ));

        assert_eq!(
            merged_head(&backup, [dvc_of(&backup), old_primary(&history)]),
            HEAD,
            "the backup nacked op {HEAD} as a canonical sender of view {ADOPTED_VIEW}, so the \
             view-{OLD_VIEW} primary's copy counted as a second nack",
        );
    }

    #[test]
    fn given_a_restart_as_primary_of_the_recovered_view_when_the_adopted_log_is_proven_should_not_start_the_view()
     {
        const RECOVERED_VIEW: u32 = ADOPTED_VIEW + 1;
        // Acked `COMMITTED` in `ADOPTED_VIEW`, then never learned its commit.
        const PEER: u8 = 1;
        const COMMITTED: u64 = HEAD + 1;
        let history = history();
        let backup = adopt_unrepaired(&history);
        assert!(backup.is_primary_for_view(RECOVERED_VIEW));

        // The view-11 primary committed op 27 with the peer's ack and crashed.
        let mut committed = at(&history, HEAD);
        committed.op = COMMITTED;
        committed.view = ADOPTED_VIEW;
        committed.parent = at(&history, HEAD).checksum;
        committed.commit = HEAD;
        committed.timestamp = COMMITTED;
        committed.checksum = committed.identity_checksum();
        let peer = StoredDvc {
            replica: PEER,
            log_view: ADOPTED_VIEW,
            op: COMMITTED,
            commit: HEAD,
            suffix: DvcSuffix::new(vec![committed, at(&history, HEAD)], 0, 0b11),
            untrusted_log: false,
        };
        backup.enter_view_change(
            PlaneKind::Metadata,
            RECOVERED_VIEW,
            ViewChangeReason::ReceivedStartViewChange,
        );
        for dvc in [dvc_of(&backup), peer] {
            assert!(dvc_record(
                &mut backup.do_view_change_from_all_replicas.borrow_mut(),
                dvc
            ));
        }
        assert!(
            backup
                .complete_view_change_as_primary(PlaneKind::Metadata)
                .is_empty()
        );
        assert_eq!(
            backup.pending_view_log().map(|merged| merged.op_head),
            Some(COMMITTED)
        );
        assert!(backup.is_primary_elect());

        // Repair brought ops 24 to 26 before the crash. The view never started.
        let durable = backup.vsr_state(0, 0);
        let restarted = backup_at(
            durable.history_certificate().unwrap().view,
            durable.view,
            HEAD,
            HEAD - 1,
        );
        restarted.begin_view_probe();
        restarted.restore_adopted_view(durable.adopted_view.as_ref());
        restarted.resume_adopted_view();
        assert_eq!(
            restarted.with_pending_view_log(|merged| merged.op_head),
            Some(HEAD),
            "the restart did not park the view-{ADOPTED_VIEW} log, so nothing below tests the gate"
        );
        // What the plane's reconcile does once the journal matches the parked headers.
        restarted.settle_view_log_proof();
        assert!(
            !restarted.is_primary_elect(),
            "the planes would drive repair and the view start from the adopted log"
        );

        let actions = restarted.start_pending_view(PlaneKind::Metadata);
        assert!(
            !actions
                .iter()
                .any(|action| matches!(action, VsrAction::SendStartView { .. })),
            "the restarted replica announced view {RECOVERED_VIEW} from the view-{ADOPTED_VIEW} \
             log with no DoViewChange quorum, so its peer adopts head {HEAD} and drops op \
             {COMMITTED}, which view {ADOPTED_VIEW} committed",
        );
        assert_eq!(restarted.status(), Status::Recovering);
    }

    /// The `MERGED_VIEW` primary-elect once its journal holds the merge of its own
    /// and the view-10 primary's logs, in the group of `history`. The view has
    /// not started.
    fn proven_merge_elect(history: &[PrepareHeader]) -> VsrConsensus<NoopBus> {
        let plane = plane_of(at(history, HEAD).group);
        let backup = adopt_unrepaired(history);
        // What the plane's reconcile does once the journal matches the parked headers.
        backup.settle_view_log_proof();
        assert!(
            !backup.view_log_merged(),
            "a backup proves the log its log_view names"
        );
        backup.enter_view_change(
            plane,
            MERGED_VIEW,
            ViewChangeReason::ReceivedStartViewChange,
        );
        let peer = StoredDvc {
            replica: OLD_PRIMARY,
            log_view: ADOPTED_VIEW,
            op: HEAD,
            commit: HEAD - 1,
            suffix: held(history, HEAD - 1, HEAD),
            untrusted_log: false,
        };
        for dvc in [dvc_of(&backup), peer] {
            assert!(dvc_record(
                &mut backup.do_view_change_from_all_replicas.borrow_mut(),
                dvc
            ));
        }
        assert!(backup.complete_view_change_as_primary(plane).is_empty());
        assert!(backup.is_primary_elect());
        backup.settle_view_log_proof();
        assert!(backup.view_log_merged());
        backup
    }

    #[test]
    fn given_a_proven_merge_when_the_elect_is_superseded_should_unprove_only_an_uncertified_partition_merge()
     {
        const SUPERSEDING_VIEW: u32 = MERGED_VIEW + 1;
        let partition = proven_merge_elect(&history_in(PARTITION_GROUP));
        // What the partition reconcile reports once the merge cleared the WAL certificate.
        partition.note_uncertified_merge();
        partition.enter_view_change(
            PlaneKind::Partitions,
            SUPERSEDING_VIEW,
            ViewChangeReason::ReceivedStartViewChange,
        );
        assert!(!partition.is_primary_elect());
        assert!(!partition.view_log_merged());
        assert!(
            partition.view_log_unproven(),
            "log_view {ADOPTED_VIEW} would name a journal that holds the view-{MERGED_VIEW} merge"
        );
        assert_eq!(partition.log_view_claim(), ADOPTED_VIEW);
        assert_eq!(
            partition.view_log_unproven_range(),
            partition.commit_min() + 1..=u64::MAX
        );
        assert_eq!(
            partition.view_log_repair_op(),
            None,
            "the proven merge has no gap to repair"
        );

        // A partition whose WAL still certifies log_view votes with its merged
        // journal, as a restart from that WAL restores it. Metadata claims through
        // its history certificate, whatever the WAL reports.
        for (group, uncertified) in [(PARTITION_GROUP, false), (METADATA_GROUP, true)] {
            let elect = proven_merge_elect(&history_in(group));
            if uncertified {
                elect.note_uncertified_merge();
            }
            elect.enter_view_change(
                plane_of(group),
                SUPERSEDING_VIEW,
                ViewChangeReason::ReceivedStartViewChange,
            );
            assert!(!elect.is_primary_elect());
            assert!(elect.view_log_merged(), "group {group}");
            assert!(!elect.view_log_unproven(), "group {group}");
            assert!(elect.view_log_unproven_range().is_empty(), "group {group}");
        }

        for group in [PARTITION_GROUP, METADATA_GROUP] {
            let started = proven_merge_elect(&history_in(group));
            started.start_pending_view(plane_of(group));
            assert_eq!(started.log_view(), MERGED_VIEW);
            assert!(!started.view_log_merged());
        }
    }

    #[test]
    fn given_an_uncertified_partition_merge_when_a_start_view_change_supersedes_it_should_vote_with_the_snapshot_taken_before()
     {
        const SUPERSEDING_VIEW: u32 = MERGED_VIEW + 1;
        let elect = proven_merge_elect(&history_in(PARTITION_GROUP));
        assert!(
            elect.view_log_unproven_range().is_empty() && elect.adopted_view_log().is_none(),
            "a merge that kept the WAL certificate offers its journal in the snapshot"
        );
        elect.note_uncertified_merge();
        // The shard reads both to snapshot the suffix this replica will vote with.
        assert_eq!(
            elect.view_log_unproven_range(),
            elect.commit_min() + 1..=u64::MAX,
            "the snapshot must not offer the merge under log_view {ADOPTED_VIEW}"
        );
        let stitched = elect
            .adopted_view_log()
            .expect("the snapshot must stitch the adopted log, as once superseded")
            .headers;
        elect.set_local_dvc_suffix(DvcSuffix::new(stitched.clone(), 0, 0));

        let start_view_change =
            Message::<StartViewChangeHeader>::new(size_of::<StartViewChangeHeader>())
                .transmute_header(|_, header: &mut StartViewChangeHeader| {
                    header.command = Command::StartViewChange;
                    header.cluster = 1;
                    header.replica = OLD_PRIMARY;
                    header.view = SUPERSEDING_VIEW;
                    header.group = PARTITION_GROUP;
                    header.size = u32::try_from(size_of::<StartViewChangeHeader>()).unwrap();
                    header.seal();
                });
        let actions =
            elect.handle_start_view_change(PlaneKind::Partitions, start_view_change.header());

        let vote = actions.iter().find_map(|action| match action {
            VsrAction::SendDoViewChange {
                view: SUPERSEDING_VIEW,
                log_view,
                suffix,
                ..
            } => Some((*log_view, suffix.headers().to_vec())),
            _ => None,
        });
        assert_eq!(
            vote,
            Some((ADOPTED_VIEW, stitched)),
            "an empty suffix from a canonical sender lets every lower log_view copy count as a nack"
        );
    }

    #[test]
    fn given_an_unrepaired_start_view_head_when_the_proof_settles_should_persist_the_adopted_log_view()
     {
        let history = history();
        let backup = adopt_unrepaired(&history);
        assert_eq!(
            backup.vsr_state(0, 0).history_certificate().unwrap().view,
            OLD_VIEW,
            "the superblock must not record a view whose log the journal does not hold"
        );
        let durable = backup.vsr_state(0, 0);
        backup.mark_superblock_durable(durable.view, durable.log_view);
        backup.mark_adopted_view_durable(durable.adopted_view.as_ref());
        assert!(!backup.needs_superblock_persist());

        backup.settle_view_log_proof();
        backup.certify_view_log(HEAD, at(&history, HEAD).checksum);
        assert!(
            backup.needs_superblock_persist(),
            "the proven view must reach the superblock before the next view-scoped send"
        );
        assert_eq!(
            backup.vsr_state(0, 0).history_certificate().unwrap().view,
            ADOPTED_VIEW
        );
    }

    /// A backup that applied every op through `HEAD` receives the view-11
    /// `StartView` with `suffix` as its log at `HEAD`.
    fn offer_applied_backup(
        history: &[PrepareHeader],
        suffix: PrepareHeader,
    ) -> (VsrConsensus<NoopBus>, Option<Vec<VsrAction>>) {
        let backup = backup_at(OLD_VIEW, OLD_VIEW, HEAD, HEAD);
        let outcome = backup.handle_start_view_guarded(
            PlaneKind::Metadata,
            make_start_view(ADOPTED_VIEW, HEAD, HEAD, ADOPTED_VIEW_PRIMARY, 0).header(),
            bytemuck::bytes_of(&suffix),
            |op| (op <= HEAD).then(|| at(history, op)),
        );
        (backup, outcome)
    }

    #[test]
    fn given_a_start_view_that_contradicts_an_applied_op_when_adopting_should_refuse_it() {
        let history = history();
        let mut forged = at(&history, HEAD);
        forged.timestamp += 1;
        forged.checksum = forged.identity_checksum();

        let (backup, outcome) = offer_applied_backup(&history, forged);

        assert!(
            outcome.is_none(),
            "a log naming another prepare at applied op {HEAD} must be refused, not adopted"
        );
        assert_eq!(
            (
                backup.view(),
                backup.log_view(),
                backup.sequencer().current_sequence()
            ),
            (OLD_VIEW, OLD_VIEW, HEAD),
            "a refused StartView changes nothing"
        );
        assert_eq!(backup.applied_log_contradictions(), 1);
    }

    #[test]
    fn given_a_start_view_that_matches_the_applied_ops_when_adopting_should_adopt_it() {
        let history = history();

        let (backup, outcome) = offer_applied_backup(&history, at(&history, HEAD));

        assert!(outcome.is_some());
        assert_eq!(backup.view(), ADOPTED_VIEW);
        assert_eq!(backup.applied_log_contradictions(), 0);
    }

    #[test]
    fn given_a_contradicting_start_view_when_redelivered_should_report_it_once_per_view() {
        let history = history();
        let mut forged = at(&history, HEAD);
        forged.timestamp += 1;
        forged.checksum = forged.identity_checksum();
        let (backup, _) = offer_applied_backup(&history, forged);
        let offer = |view| {
            backup.handle_start_view_guarded(
                PlaneKind::Metadata,
                make_start_view(view, HEAD, HEAD, backup.primary_index(view), 0).header(),
                bytemuck::bytes_of(&forged),
                |op| (op <= HEAD).then(|| at(&history, op)),
            )
        };

        assert!(offer(ADOPTED_VIEW).is_none(), "a redelivery is refused too");
        assert_eq!(
            backup.applied_log_contradictions(),
            1,
            "a redelivered StartView must not be reported again"
        );
        let next_view = (ADOPTED_VIEW + 1..=ADOPTED_VIEW + u32::from(backup.replica_count()))
            .find(|view| backup.primary_index(*view) != BACKUP)
            .expect("primaryship rotates through every replica");
        assert!(offer(next_view).is_none());
        assert_eq!(
            backup.applied_log_contradictions(),
            2,
            "a contradiction in a newer view is reported"
        );
    }
}
