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
    AckLogEvent, Consensus, ControlActionLogEvent, DvcQuorumArray, DvcSuffix, IgnoreReason,
    MergeOutcome, MergeQuorums, MergedLog, Pipeline, PlaneKind, PrepareLogEvent, Project,
    ReplicaLogContext, SimEventKind, StoredDvc, ViewChangeLogEvent, ViewChangeReason, VsrState,
    dvc_count, dvc_iter, dvc_quorum_array_empty, dvc_record, dvc_reset, dvc_suffix_decode,
    emit_replica_event, emit_sim_event, merge_dvc_quorum, seal_prepare_checksum,
};
use bit_set::BitSet;
use clock::{Clock, IggySystemClock};
use iggy_binary_protocol::{
    Command, ConsensusHeader, DoViewChangeHeader, GenericHeader, PrepareHeader, PrepareOkHeader,
    ReplyHeader, RequestStartViewHeader, RoutedRequestHeader, StartViewChangeHeader,
    StartViewHeader, frame_body,
};
use iggy_common::IggyTimestamp;
use iggy_common::calculate_checksum;
use message_bus::IggyMessageBus;
use message_bus::MessageBus;
use server_common::Message;
use server_common::sharding::{IggyNamespace, METADATA_GROUP};
use std::cell::{Cell, RefCell};
use std::collections::VecDeque;
use std::rc::Rc;
use std::time::Duration;

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
        (Self::with_sender(header, sender), receiver)
    }

    /// Entry adopting an existing reply sender — used when a request that
    /// carried a subscriber through the request queue is promoted into a
    /// prepare slot, so the original in-process awaiter keeps its receiver.
    #[must_use]
    pub fn with_sender(header: PrepareHeader, sender: Sender<Message<ReplyHeader>>) -> Self {
        Self {
            header,
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

/// Accepted request waiting in `request_queue` for a prepare slot.
#[derive(Debug)]
pub struct RequestEntry {
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
    #[must_use]
    pub const fn new(message: Message<RoutedRequestHeader>) -> Self {
        Self {
            message,
            received_at: 0,
            reply_sender: None,
        }
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
        let entry = Self {
            message,
            received_at: 0,
            reply_sender: Some(sender),
        };
        (entry, receiver)
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
    /// Primary heartbeat: send current commit point to all backups.
    ///
    /// Emitted when the `CommitMessage` timeout fires. Prevents backups
    /// from starting a view change during idle periods.
    SendCommit {
        view: u32,
        commit: u64,
        group: u64,
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
    group: u64,

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

    /// This replica's own uncommitted suffix, with the `(op, commit)` the journal
    /// was at when it was read.
    ///
    /// Installed by the shard via [`Self::set_local_dvc_suffix`] before any handler
    /// that could enter a view change. Snapshotted rather than recomputed per send
    /// so a retransmit is byte-identical: a nack is a durable claim about this
    /// replica's log, and silently retracting one lets the new primary assemble a
    /// quorum that never simultaneously existed.
    ///
    /// Tagged by `(op, commit)`, not by view, because that is what the suffix
    /// describes: a view advance leaves the log alone so the snapshot survives,
    /// while anything moving the head or commit point makes the tag mismatch,
    /// which reads as no snapshot at all.
    local_dvc_suffix: RefCell<Option<(u64, u64, DvcSuffix)>>,

    /// The log a DVC quorum settled on, parked until this replica's journal can
    /// serve all of it.
    ///
    /// Non-`None` means "primary-elect, repairing": decided but not started, so
    /// this replica prepares and announces nothing. Cleared by
    /// [`VsrConsensus::start_pending_view`], or by `reset_view_change_state`.
    pending_view_log: RefCell<Option<MergedLog>>,

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
#[derive(Debug, Clone, Copy)]
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
    /// View inferred from the last journaled prepare, consulted only when no
    /// durable record exists; `log_view` cannot be inferred and stays 0.
    pub view_fallback: Option<u32>,
    /// Non-zero boot incarnation; `None` keeps the default.
    pub incarnation: Option<u128>,
    pub join: JoinMode,
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
        if let Some((view, log_view)) = restore.durable_view {
            // The one line proving the durable record was READ BACK, not merely
            // written: a replica that came back at view 0 is otherwise
            // indistinguishable from one that resumed correctly until it votes.
            tracing::info!(
                group,
                view,
                log_view,
                "restored group view from its superblock"
            );
            consensus.set_view(view);
            consensus.set_log_view(log_view);
            consensus.mark_superblock_durable(view, log_view);
        } else if let Some(view) = restore.view_fallback {
            consensus.set_view(view);
        }
        match restore.join {
            JoinMode::Init => consensus.init(),
            JoinMode::ProbeAsBackup {
                await_state_transfer,
            } => {
                consensus.init_as_backup();
                consensus.begin_view_probe();
                if await_state_transfer {
                    consensus.begin_state_transfer_await();
                }
            }
        }
        consensus
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
            recovery_barrier: Cell::new(0),
            recovery_deadline: Cell::new(Duration::ZERO),
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
            start_view_change_from_all_replicas: RefCell::new(BitSet::with_capacity(REPLICAS_MAX)),
            probe_attempts: Cell::new(0),
            probe_attempts_max: Cell::new(PROBE_ATTEMPTS_MAX),
            local_dvc_suffix: RefCell::new(None),
            pending_view_log: RefCell::new(None),
            do_view_change_from_all_replicas: RefCell::new(dvc_quorum_array_empty()),
            do_view_change_quorum: Cell::new(false),
            sent_own_start_view_change: Cell::new(false),
            sent_own_do_view_change: Cell::new(false),
            timeouts: RefCell::new(TimeoutManager::new(timeout_seed)),
            heartbeat_timestamp: Cell::new(0),
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

    pub fn init(&self) {
        self.status.set(Status::Normal);
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

    /// Deadline paired with [`Self::recovery_barrier`]; only meaningful while the
    /// barrier is armed (non-zero).
    #[must_use]
    pub const fn recovery_deadline(&self) -> Duration {
        self.recovery_deadline.get()
    }

    pub fn set_recovery_deadline(&self, deadline: Duration) {
        self.recovery_deadline.set(deadline);
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
    /// pop through [`Pipeline`] directly (`drain_committable_prefix`) rather than
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

    /// Snapshot the durable VSR state for the superblock. `view`, `log_view`,
    /// `commit_max`, and the replica identity come from consensus; the caller
    /// supplies the paired checkpoint, which consensus does not own.
    #[must_use]
    pub const fn vsr_state(&self, checkpoint_op: u64, checkpoint_checksum: u128) -> VsrState {
        VsrState {
            cluster: self.cluster,
            replica_id: self.replica,
            replica_count: self.replica_count,
            view: self.view.get(),
            log_view: self.log_view.get(),
            commit_max: self.commit_max.get(),
            checkpoint_op,
            checkpoint_checksum,
            // Consensus mints no message offsets: the PARTITION plane stamps
            // this in before it writes (`IggyPartition::write_superblock`).
            offset_frontier: 0,
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
        let (op, commit) = self.local_dvc_suffix_tag();
        *self.local_dvc_suffix.borrow_mut() = Some((op, commit, suffix));
    }

    /// Drop the cached suffix snapshot.
    ///
    /// The `(op, commit)` tag tracks how far the log reaches, not what it still
    /// contains, so a mutation that removes entries without moving either
    /// (truncating a diverging uncommitted range) leaves a snapshot reading as
    /// current while offering bodies this replica can no longer serve. A peer that
    /// picks it as a body source then waits out the whole view change.
    ///
    /// Call from the mutation site. The next refresh re-reads the journal.
    pub fn invalidate_local_dvc_suffix(&self) {
        self.local_dvc_suffix.borrow_mut().take();
    }

    /// The `(op, commit)` a snapshot must match to still describe this log.
    /// `commit` is clamped to `op` exactly as the outgoing DVC clamps it.
    fn local_dvc_suffix_tag(&self) -> (u64, u64) {
        let op = self.sequencer.current_sequence();
        (op, self.commit_max.get().min(op))
    }

    /// This replica's suffix snapshot, or an empty one when none matches the log's
    /// current head and commit point. Empty is the safe direction: it nacks nothing
    /// and offers no bodies, so it can only stall a view change, never authorise a
    /// truncation.
    #[must_use]
    pub fn local_dvc_suffix(&self) -> DvcSuffix {
        let tag = self.local_dvc_suffix_tag();
        match &*self.local_dvc_suffix.borrow() {
            Some((op, commit, suffix)) if (*op, *commit) == tag => suffix.clone(),
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
            Some((op, commit, _)) if (*op, *commit) == tag
        )
    }

    /// True when the current `(view, log_view)` is not yet in the superblock, so a
    /// view-scoped send would advertise a view a crash could lose. The split-brain
    /// gate: the dispatcher persists first when this holds. `commit_max` is
    /// excluded deliberately, since it advances on every commit and rides the
    /// checkpoint write rather than each view change.
    #[must_use]
    pub const fn needs_superblock_persist(&self) -> bool {
        self.view.get() != self.view_durable.get()
            || self.log_view.get() != self.log_view_durable.get()
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
        let mut pipeline = self.pipeline.borrow_mut();
        pipeline.cancel_all_subscribers();
        pipeline.clear_request_queue();
    }

    /// Process one tick. Call this every [`crate::TICK_INTERVAL`].
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
                    if attempts >= self.probe_attempts_max.get() {
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
    /// The three callers that used to inline this sequence were the actual
    /// duplication: an election timeout, an SVC for a higher view, and a DVC for
    /// a higher view, differing only in `reason`.
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
        // `local_dvc_suffix()`, whose `(op, commit)` tag can have moved since, in
        // which case it answers EMPTY, retracting every nack and body offer already
        // sent. Survivable only because `dvc_record` drops a duplicate sender, so the
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
        // A replica that ceded at boot (`init_as_backup`) is a backup by role however
        // the view math reads, so it must not retransmit either.
        if !self.is_primary() || self.status.get() != Status::Normal || self.has_ceded_primaryship()
        {
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

        // Don't advertise a commit point we haven't locally executed yet.
        // After view change the new primary may have commit_min < commit_max
        // until commit_journal catches up. Send commit_min (what we've
        // actually applied) so backups don't advance past us.
        let ts = self.heartbeat_timestamp.get() + 1;
        self.heartbeat_timestamp.set(ts);

        vec![VsrAction::SendCommit {
            view: self.view.get(),
            commit: self.commit_min.get(),
            group: self.group,
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
                    log_view: self.log_view.get(),
                    op: current_op,
                    commit,
                    suffix: self.local_dvc_suffix(),
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

    /// Handle a received `DoViewChange` message (only relevant for primary candidate).
    ///
    /// "When the new primary receives f + 1 DOVIEWCHANGE messages from different
    /// replicas (including itself), it sets its view-number to that in the messages
    /// and selects as the new log the one contained in the message with the largest v'..."
    ///
    /// The `commit` this replica advertises in a `DoViewChange`.
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
    fn dvc_commit(&self) -> u64 {
        let op = self.sequencer.current_sequence();
        self.commit_max.get().min(op)
    }

    /// Build this replica's `DoViewChange` for the current view.
    fn build_do_view_change(&self, target: u8) -> VsrAction {
        VsrAction::SendDoViewChange {
            view: self.view.get(),
            target,
            log_view: self.log_view.get(),
            op: self.sequencer.current_sequence(),
            commit: self.dvc_commit(),
            group: self.group,
            suffix: self.local_dvc_suffix(),
        }
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
        // a recovering replica must not collect DVCs and crown itself.
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
                log_view: self.log_view.get(),
                op: current_op,
                commit,
                suffix: self.local_dvc_suffix(),
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
        };
        dvc_record(&mut self.do_view_change_from_all_replicas.borrow_mut(), dvc);

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
            commit: self.commit_max.get(),
            incarnation: header.incarnation,
            target: Some(header.replica),
            // A probe answer reports this primary's settled frontier, not a
            // freshly merged log, so there is no canonical suffix to publish.
            suffix: Vec::new(),
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
    fn adopt_start_view_suffix(&self, header: &StartViewHeader, suffix_body: &[u8]) -> u64 {
        let suffix = match dvc_suffix_decode(suffix_body, header.op, 0, 0) {
            Ok(suffix) => suffix,
            Err(error) => {
                tracing::warn!(
                    replica = self.replica,
                    from_replica = header.replica,
                    view = header.view,
                    op = header.op,
                    "start_view suffix did not decode, falling back to the announced op: {error}"
                );
                return header.op;
            }
        };
        let headers = suffix.headers();
        if headers.is_empty() {
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
    pub fn handle_start_view(
        &self,
        plane: PlaneKind,
        header: &StartViewHeader,
        suffix_body: &[u8],
    ) -> Vec<VsrAction> {
        assert_eq!(header.group, self.group, "SV routed to wrong group");
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
            return Vec::new();
        }

        // Skip an equal-view StartView whose op is below our COMMITTED floor. Such a
        // message can only be stale: a live primary's head covers every op it ever
        // told us was committed. Already in this view, so re-running
        // reset_view_change_state for one would cancel subscribers (waking register
        // awaiters Canceled) and clear the pipeline for nothing. log_view (not
        // self.view) tracks the last-normal view.
        //
        // The bound is deliberately the commit floor and NOT the sequencer head.
        // Adopting a StartView drops the head (below) without truncating the WAL, so
        // a replica that adopted at head H and then restarted re-derives a LONGER
        // head from its own journal while log_view stays at that same view. Skipping
        // on the head would drop the primary's StartView there -- including the reply
        // echoing this replica's own probe -- leaving it to time the probe out and
        // elect instead, with a DoViewChange of (log_view, discarded_head) that
        // outranks the real primary's and pushes ops that view already discarded back
        // over committed bodies.
        //
        // Re-adoption drops the head again while the WAL still holds the discarded
        // suffix. Consensus is sans-io and cannot truncate it; the plane sweeps it
        // on every adoption (`reconcile_{metadata,partition}_view_divergence`,
        // above-head branch) before the primary's next prepare can collide with a
        // relic in `append`'s slot-collision check.
        if msg_view == self.log_view.get() && msg_op < self.commit_min() {
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

        // Cross-check the announced head against the headers published with it: a
        // suffix head disagreeing with `header.op` means an inconsistently built
        // frame, and either value leaves this replica chasing an unservable head.
        let announced = self.adopt_start_view_suffix(header, suffix_body);
        self.sequencer.set_sequence(announced);

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
    ///
    /// Re-entrant, called again for every `DoViewChange` landing while the merge is
    /// undecided. Every non-`Ready` outcome leaves this replica untouched, so a
    /// re-run costs only the merge.
    fn complete_view_change_as_primary(&self, plane: PlaneKind) -> Vec<VsrAction> {
        let merged = {
            let dvc_array = self.do_view_change_from_all_replicas.borrow();
            merge_dvc_quorum(&dvc_array, self.merge_quorums())
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

    /// Sizes handed to the DVC merge.
    const fn merge_quorums(&self) -> MergeQuorums {
        MergeQuorums {
            view_change: self.quorum_view_change(),
            nack_prepare: self.quorum_nack_prepare(),
            replica_count: self.replica_count as usize,
            // The cluster-wide ceiling, not `self.prepare_queue_max`: this node's
            // config says nothing about how deep a peer's pipeline is.
            prepare_queue_ceiling: PREPARE_QUEUE_CEILING as u64,
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

    /// Whether a merge is parked, without cloning it.
    #[must_use]
    pub fn view_log_is_pending(&self) -> bool {
        self.pending_view_log.borrow().is_some()
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
    /// and the view change is blocked on the round-trip.
    #[must_use]
    pub fn pending_view_body_sources(&self, op: u64) -> Vec<u8> {
        let quorum = self.do_view_change_from_all_replicas.borrow();
        let mut sources: Vec<(u32, u8)> = dvc_iter(&quorum)
            .filter(|dvc| dvc.replica != self.replica)
            .filter_map(|dvc| {
                let index = dvc.suffix.index_of(dvc.op, op)?;
                dvc.suffix
                    .offers_body(index)
                    .then_some((dvc.log_view, dvc.replica))
            })
            .collect();
        sources.sort_unstable_by_key(|(log_view, _)| std::cmp::Reverse(*log_view));
        sources.into_iter().map(|(_, replica)| replica).collect()
    }

    /// Finish the parked view change: this replica's journal now covers the merged
    /// log, so it can serve any op it is about to announce.
    ///
    /// Called by the shard after repair progress. No-op when nothing is parked.
    ///
    /// # Panics
    /// If the merged uncommitted range exceeds pipeline capacity, which needs a head
    /// more than one pipeline depth above the proven commit point.
    pub fn start_pending_view(&self, plane: PlaneKind) -> Vec<VsrAction> {
        // A backup's parked log (the `StartView` suffix) is only what its ingest
        // verifies bodies against. It must never take this path: starting the view
        // claims the primaryship of a view this replica did not win.
        if !self.is_primary_for_view(self.view.get()) {
            return Vec::new();
        }
        let Some(merged) = self.pending_view_log.borrow_mut().take() else {
            return Vec::new();
        };
        let new_op = merged.op_head;
        let max_commit = merged.commit_max;

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
        self.ceded_primaryship.set(false);
        self.sequencer.set_sequence(new_op);
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
        if max_commit < new_op {
            // `complete_view_change_as_primary` already refused a non-fitting
            // range. Asserted so the sites cannot drift; this one cannot decline.
            debug_assert!(
                (new_op - max_commit) <= self.prepare_queue_max as u64,
                "view change: uncommitted range {}..={} ({} ops) exceeds pipeline capacity ({}); \
                 the merged log claims more in-flight ops than the pipeline can hold",
                max_commit + 1,
                new_op,
                new_op - max_commit,
                self.prepare_queue_max,
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
    /// Only `PrepareOk` reaches here (via `send_or_loopback`), and deliberately:
    /// it is a message to a peer that happens to be this replica, so a one-drain
    /// delay costs nothing. A replica's own SVC/DVC is not that shape -- it is the
    /// local decision to change view, recorded synchronously with the view and
    /// status writes in [`Self::enter_view_change`]. Routing it here instead would
    /// leave a window where the replica has entered a view change without counting
    /// itself, which on a solo group is the entire quorum.
    pub(crate) fn push_loopback(&self, message: Message<GenericHeader>) {
        assert!(
            self.loopback_queue.borrow().len() < self.prepare_queue_max,
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

impl<B, P> Project<Message<PrepareHeader>, VsrConsensus<B, P>> for Message<RoutedRequestHeader>
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

        // Seal the body integrity field over the payload past the 256-byte header
        // (the client request bytes, carried through the transmute unchanged).
        // Computed once by the primary and replicated verbatim, so every backup
        // stores the same value and the scan verifies it after a crash. The body is
        // never re-stamped (`restamp_prepare_view` patches only `view`), so this
        // survives view-change retransmits. The header `checksum` and its `parent`
        // chain are sealed too, for both planes, by `seal_prepare_checksum` below;
        // they exclude `view`, which is what lets a restamp leave them valid.
        //
        // Metadata plane only. A partition produce prepare already carries a verified
        // `batch_checksum` over the same bytes, so a second full-payload pass is pure
        // cost on the produce path, and it would describe the WRONG bytes:
        // `stamp_prepare_for_persistence` rewrites the command header INSIDE this
        // sealed region before the entry is journaled. Leaving those prepares at `0`
        // is the designed "nothing to verify" sentinel, so a future durable partition
        // journal skips verification instead of failing every entry as corrupt.
        //
        // TODO(consensus): a partition prepare's `checksum` covers its header alone,
        // so two at one op with matching header fields are indistinguishable however
        // far their batch bytes diverge. The merge then counts a divergent replica as
        // holding a servable copy, and the partition repair ingest (no merged-log
        // identity gate) short-circuits `verify_prepare_integrity`'s body branch on
        // the zero. Two closures, both larger than they look:
        //
        // 1. The batch checksum, recomputed after `stamp_prepare_for_persistence`.
        //    But stamping runs per replica after replication and folds `base_offset`
        //    in, so identity would change at stamp time and the journaled entry would
        //    no longer match the pipeline entry `handle_prepare_ok` compares.
        // 2. The stamp-invariant cover: everything past the 256-byte command header,
        //    which stamping never touches. Identical on every replica, safe to seal
        //    here, but costs a produce-path pass and retires the "0 means nothing to
        //    verify" sentinel that lets an existing WAL replay.
        //
        // Bounded by `size`, the range every verifier re-reads; the prepare
        // inherits it verbatim below.
        let checksum_body = if consensus.group == METADATA_GROUP {
            u128::from(calculate_checksum(frame_body(
                self.as_slice(),
                self.header().size,
            )))
        } else {
            0
        };

        let prepared = self.transmute_header(|old, new| {
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
mod request_queue_tests {
    use super::*;
    use iggy_binary_protocol::{Command, Operation};

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

    #[allow(clippy::cast_possible_truncation)]
    fn make_start_view(
        view: u32,
        op: u64,
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
        header.commit = op;
        header.replica = replica;
        header.incarnation = incarnation;
        header.group = METADATA_GROUP;
        header.size = size as u32;
        msg
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
        // and log_view stays 0 so the equal-view-old-op skip does not fire.
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
        let stale = make_start_view(1, 4, 1, STALE);
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
        let fresh = make_start_view(1, 4, 1, CURRENT);
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
    fn given_partition_namespace_when_projecting_should_leave_the_body_unsealed() {
        // The body seal is metadata-only. A partition produce prepare already carries a
        // verified `batch_checksum` over the same bytes, so a second full-payload hash
        // is pure cost on the produce path, and it would describe bytes that never reach
        // the journal: `stamp_prepare_for_persistence` rewrites the command header
        // inside the sealed region. `0` is the designed "nothing to verify" sentinel, so
        // a durable partition journal skips verification rather than reading every entry
        // as corrupt.
        let seal = |namespace: u64| -> u128 {
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
            header.operation = iggy_binary_protocol::Operation::SendMessages;
            header.size = u32::try_from(header_size + body.len()).expect("fits u32");
            msg.project(&consensus).header().checksum_body
        };

        assert_ne!(
            seal(METADATA_GROUP),
            0,
            "a metadata prepare must be sealed: the WAL scan verifies it after a crash"
        );
        assert_eq!(
            seal(1),
            0,
            "a partition prepare must be left unsealed, since its sealed region is \
             rewritten before it is journaled"
        );
    }

    #[test]
    fn given_restored_log_view_when_start_view_head_behind_wal_should_adopt() {
        // A replica that adopted a StartView at head 105 in view 7, then crashed,
        // recovers log_view = 7 from the superblock but re-derives head 120 from its
        // own WAL: adoption drops the head without truncating the journal. The
        // primary's StartView for view 7 then carries an op BEHIND that head, and
        // skipping it on the head comparison would leave this replica probing until it
        // elected instead, carrying a DoViewChange of (7, 120) that outranks the real
        // primary's (7, 105) and resurrects ops view 7 already discarded.
        //
        // Replica 0 of 3 at view 7, whose primary is replica 1 (7 % 3). Incarnation
        // left at 0 so the recovering-replica guard stays inert and this exercises the
        // equal-view path alone.
        let mut consensus = VsrConsensus::with_clock(
            1,
            0,
            3,
            METADATA_GROUP,
            NoopBus,
            LocalPipeline::new(),
            ConsensusClock::system(),
        );
        consensus.set_view(7);
        consensus.set_log_view(7);
        consensus.restore_commit_state(105, 105);
        consensus.sequencer().set_sequence(120);

        // Below the committed floor: stale by construction, since a live primary's
        // head covers every op it told us was committed.
        assert!(
            consensus
                .handle_start_view(
                    PlaneKind::Metadata,
                    make_start_view(7, 104, 1, 0).header(),
                    &[]
                )
                .is_empty(),
            "an equal-view StartView below the commit floor must be skipped"
        );
        assert_eq!(
            consensus.sequencer().current_sequence(),
            120,
            "a skipped StartView must not move the head"
        );

        // At the committed floor but behind our WAL head: the primary's real head.
        // Adopt it and drop the discarded suffix.
        assert!(
            !consensus
                .handle_start_view(
                    PlaneKind::Metadata,
                    make_start_view(7, 105, 1, 0).header(),
                    &[]
                )
                .is_empty(),
            "an equal-view StartView at or above the commit floor must be adopted, \
             even when its head is behind a WAL suffix the view already discarded"
        );
        assert_eq!(
            consensus.sequencer().current_sequence(),
            105,
            "adoption must drop the head to the primary's, so a later DoViewChange \
             cannot outrank it with a discarded suffix"
        );
        assert_eq!(consensus.status(), Status::Normal);
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
            _data: server_common::iobuf::Frozen<{ server_common::MESSAGE_ALIGN }>,
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
        fn set_client_forward_fn(&self, _f: message_bus::ClientForwardFn) {}
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

    use crate::drain_committable_prefix;
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
        assert_eq!(drain_committable_prefix(&consensus).len(), 1);
        assert!(
            prepare_ticking(&consensus),
            "a remaining prepare keeps the timer armed"
        );

        consensus.advance_commit_max(2);
        assert_eq!(drain_committable_prefix(&consensus).len(), 1);
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
    use server_common::MESSAGE_ALIGN;
    use server_common::iobuf::Frozen;

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
