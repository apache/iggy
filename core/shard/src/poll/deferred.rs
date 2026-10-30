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

//! Deferred reads remain provisional until the owner selects one result.
//! One registry owns readiness deadlines and quotas. Detached readers
//! carry only a request identity and a reservation that survives cancellation.
//! Readiness waits share the registry, because it owns the one poll notifier
//! of each partition.
//!
//! A readiness wait runs on the shard of its connection. That shard keeps a
//! mirror of each partition of another shard that its waits name, and the
//! owner pushes the partition state after every change. So a wait in steady
//! state sends no frame to the owners, and its cancel stays local. The first
//! wait on a partition goes to the owner, which answers it and starts the
//! pushes.

use std::cell::RefCell;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashSet, VecDeque};
use std::ops::Bound::{Excluded, Unbounded};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use consensus::client_table::SessionAttachment;
use consensus::{Consensus, MetadataHandle, PartitionsHandle};
use crossfire::TrySendError;
use iggy_binary_protocol::primitives::partition_history::ConsumerGroupOwner;
use iggy_binary_protocol::responses::messages::poll_messages::POLL_RESPONSE_HEADER_SIZE;
use iggy_common::{DeferredPollOptions, IggyError, PollingKind, PollingStrategy};
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::stream::PollMetadata;
use partitions::{
    DISK_POLL_CHUNK_MAX, IggyPartition, Partition, PollReadResult, PollVisibility, PollingArgs,
    PollingConsumer,
};
use server_common::poll::PollHistoryId;
use server_common::sharding::IggyNamespace;

use crate::config::DeferredPollConfig;
use crate::metrics::DeferredPollMetrics;
use crate::shards_table::ShardsTable;
use crate::{IggyShard, LifecycleFrame, PartitionReadReply, Sender, ShardFrame, TaggedSender};
use prometheus_client::metrics::gauge::Gauge;

const SERVICE_BUDGET: usize = 8;
const READ_BUDGET_PARTS: usize = 3;
/// The smallest part of a disk read reservation. One disk read can take a
/// chunk of up to [`DISK_POLL_CHUNK_MAX`] whatever its reply size, so a
/// smaller part would let polls with a small `max_bytes` start more reads than
/// the budget can hold.
#[allow(clippy::cast_possible_truncation)]
const READ_PART_MIN_BYTES: usize = DISK_POLL_CHUNK_MAX as usize;
const MAINTENANCE_BUDGET: usize = 16;
/// A mirror that no wait named for this long ends its watch at the next push
/// or sweep. The waits of a consumer that keeps polling name their
/// partitions more often.
const MIRROR_IDLE_US: u64 = 1_000_000;

/// The `min_count` of a wait that ends at its first message. An owner
/// evaluator sees only its own targets, so it gets this, and the shard of
/// the wait decides on the sum.
pub const ANY_MESSAGE: u32 = 1;

/// What the `max_bytes` of a read counts besides the batches of
/// [`ReadyTarget::known_bytes`]: the poll response header.
#[allow(clippy::cast_possible_truncation)]
pub const KNOWN_BYTES_SLACK: u32 = POLL_RESPONSE_HEADER_SIZE as u32;

/// The history, the visibility and the view of a partition that a readiness
/// evaluation reads, and whether the partition serves the target now. Only a
/// mirror that lags the install of a group owner does not.
type Reading = (PollHistoryId, PollVisibility, u32, bool);

/// Auxiliary connections are charged to their parent session. HTTP requests
/// without a binary session are charged to their authenticated user.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum PollQuotaIdentity {
    Session(u128),
    User(u32),
}

/// The caller of a deferred request. A readiness wait shares one context
/// across its targets.
#[derive(Debug, Clone)]
pub struct DeferredPollContext {
    pub session: Option<SessionAttachment>,
    pub user_id: u32,
    pub user_created_at: Option<u64>,
    pub quota: PollQuotaIdentity,
    pub primary: bool,
}

/// A deferred poll as the origin resolved it.
///
/// The origin shard sets both deadlines in `partition_read` or
/// `deferred_read` from the bus clock, which every shard of the process
/// shares, before it routes the request to the owner. A request that skips
/// both keeps the zero deadlines of `new` and expires at once.
#[derive(Debug)]
pub struct DeferredPollRequest {
    pub consumer: PollingConsumer,
    pub args: PollingArgs,
    pub metadata: PollMetadata,
    pub context: DeferredPollContext,
    pub options: DeferredPollOptions,
    /// The gate of a committing request that its client can cancel.
    pub commit_gate: Option<CommitGate>,
    /// Shared by the reads of one fetch, so at most one of them returns a
    /// first message larger than its `max_bytes`. A poll alone has none and
    /// always may.
    pub oversized_turn: Option<OversizedTurn>,
    pub(crate) deadline: u64,
    pub(crate) request_deadline: u64,
}

impl DeferredPollRequest {
    #[must_use]
    pub const fn new(
        consumer: PollingConsumer,
        args: PollingArgs,
        metadata: PollMetadata,
        context: DeferredPollContext,
        options: DeferredPollOptions,
    ) -> Self {
        Self {
            consumer,
            args,
            metadata,
            context,
            options,
            commit_gate: None,
            oversized_turn: None,
            deadline: 0,
            request_deadline: 0,
        }
    }
}

/// The one turn of a fetch to return a first message larger than the byte
/// limit of its read, which bounds the reply by one such message.
#[derive(Debug, Clone, Default)]
pub struct OversizedTurn(Arc<AtomicU8>);

impl OversizedTurn {
    /// Takes the turn, unless another read took it.
    #[must_use]
    pub fn take(&self) -> bool {
        self.0
            .compare_exchange(0, 2, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    fn accept_normal(&self) -> bool {
        matches!(
            self.0
                .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst),
            Ok(_) | Err(1)
        )
    }
}

/// The cancel gate of one committing deferred request.
///
/// The origin and the owners of its reads share it. A cancel wins only while
/// no read passed the gate, and a read commits only while no cancel won, so a
/// cancelled request stored nothing and a request whose cancel lost answers as
/// usual.
#[derive(Debug, Clone, Default)]
pub struct CommitGate(Arc<AtomicU64>);

const GATE_CANCELLED: u64 = 1 << 63;

impl CommitGate {
    /// Lets one read commit, unless a cancel won.
    #[must_use]
    pub fn enter(&self) -> bool {
        self.0
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |state| {
                (state & GATE_CANCELLED == 0).then_some(state + 1)
            })
            .is_ok()
    }

    /// Cancels the request, unless a read already passed the gate.
    #[must_use]
    pub fn cancel(&self) -> bool {
        self.0
            .compare_exchange(0, GATE_CANCELLED, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::SeqCst) & GATE_CANCELLED != 0
    }
}

/// One partition of a readiness wait, resolved and authorized by the origin.
#[derive(Debug)]
pub struct ReadinessTarget {
    pub namespace: IggyNamespace,
    pub partition_id: u32,
    pub consumer: PollingConsumer,
    /// `Offset` or `Next`, the start point of a poll with the same strategy.
    pub strategy: PollingStrategy,
    pub metadata: PollMetadata,
}

/// The targets of one readiness wait that a single shard evaluates.
///
/// The evaluator is their owner, or the shard of the wait for the targets
/// that it mirrors. They name one stream and topic, so one check of
/// `context` covers all of them.
#[derive(Debug)]
pub struct ReadinessWait {
    pub context: DeferredPollContext,
    pub targets: Vec<ReadinessTarget>,
    pub deadline: u64,
    /// The evaluator answers once its targets hold this many messages, or
    /// at the deadline with the targets that hold any.
    pub min_count: u32,
}

/// A target that holds messages at its start point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadyTarget {
    pub partition_id: u32,
    /// The offsets from the start through the committed offset. A start
    /// before the offsets that retention kept counts removed ones too, so
    /// this bounds the messages from above.
    pub available: u64,
    /// The bytes with batch framing that a read from the start returns, if
    /// the start was the next offset to commit when the wait parked. A read
    /// whose `max_bytes` and read budget cover them plus
    /// [`KNOWN_BYTES_SLACK`] returns them all.
    pub known_bytes: Option<u64>,
}

/// A shard that watches partitions of another shard.
///
/// Pushes and unwatches carry the sequence of the watch, so a frame from
/// before a later watch of the same partition cannot change that watch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadinessWatch {
    pub shard: u16,
    pub seq: u64,
}

/// The state of a watched partition that its owner pushes. `None` means
/// that the owner does not serve the partition.
#[derive(Debug)]
pub struct WatchedState {
    pub namespace: IggyNamespace,
    pub seq: u64,
    pub state: Option<ServedState>,
}

/// What a readiness evaluation reads from a partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServedState {
    history: PollHistoryId,
    visibility: PollVisibility,
    view: u32,
    status: PartitionStatus,
    /// The installed owner of each group that a watching wait names.
    owners: Vec<(u64, Option<ConsumerGroupOwner>)>,
}

impl ServedState {
    fn check(&self, primary: bool, target: &ReadinessTarget) -> Result<bool, IggyError> {
        self.status.check(
            target.namespace,
            primary,
            &target.metadata,
            target.consumer,
            |group| {
                self.owners
                    .iter()
                    .find(|(id, _)| *id == group)
                    .and_then(|(_, owner)| *owner)
            },
        )
    }
}

/// The conditions on a partition that the checks of a read use.
#[allow(clippy::struct_excessive_bools)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PartitionStatus {
    deleted: bool,
    backup: bool,
    /// A fault, a read history that changes, a state transfer, or a status
    /// other than normal.
    unstable: bool,
    view_started: bool,
    primary: bool,
    /// The incarnation that the owner of the partition serves.
    incarnation: Option<u64>,
}

impl PartitionStatus {
    fn of<B: MessageBus, SB: SuperblockStore>(
        partition: &IggyPartition<B, SB>,
        incarnation: Option<u64>,
    ) -> Self {
        let consensus = partition.consensus();
        Self {
            deleted: partition.history_deleted(),
            backup: consensus.is_normal_backup(),
            unstable: partition.fatal().is_some()
                || partition.read_history_is_changing()
                || partition.requires_state_transfer()
                || !consensus.is_normal()
                || consensus.is_transferring(),
            view_started: consensus.applied_view_start(),
            primary: consensus.is_primary(),
            incarnation,
        }
    }

    /// `owner` looks up the installed owner of a group. `Ok(false)` means
    /// that the partition has not applied the install of the group owner
    /// that `metadata` names yet.
    fn check(
        self,
        namespace: IggyNamespace,
        primary: bool,
        metadata: &PollMetadata,
        consumer: PollingConsumer,
        owner: impl FnOnce(u64) -> Option<ConsumerGroupOwner>,
    ) -> Result<bool, IggyError> {
        if self.deleted {
            return Err(IggyError::HistoryUnavailable);
        }
        if primary && self.backup {
            return Err(IggyError::NotPrimary);
        }
        let installed = match consumer {
            PollingConsumer::ConsumerGroup(group, _) => {
                match super::group_owner_rejection(
                    namespace,
                    group as u64,
                    owner(group as u64),
                    metadata,
                ) {
                    None => true,
                    Some(IggyError::TransientNotAccepted) => false,
                    Some(error) => return Err(error),
                }
            }
            PollingConsumer::Consumer(..) => true,
        };
        if self.unstable
            || (primary && (!self.view_started || !self.primary))
            || !metadata.matches_partition(self.incarnation)
        {
            return Err(IggyError::TransientNotAccepted);
        }
        Ok(installed)
    }
}

/// A target as one readiness evaluation finds it.
#[derive(Debug, Clone, Copy)]
struct Readiness {
    history: PollHistoryId,
    view: u32,
    /// The offsets from the start through the committed offset.
    available: u64,
    committed_bytes: u64,
    /// The start is the next offset to commit, so the bytes that later
    /// commits add are those that a read from the start returns.
    at_end: bool,
}

/// A target of a waiter at admission and at its last evaluation.
#[derive(Debug, Clone, Copy)]
struct TargetReadiness {
    admitted: Readiness,
    last: Readiness,
}

impl TargetReadiness {
    fn ready(&self, partition_id: u32) -> Option<ReadyTarget> {
        (self.last.available > 0).then(|| ReadyTarget {
            partition_id,
            available: self.last.available,
            known_bytes: self.admitted.at_end.then(|| {
                self.last
                    .committed_bytes
                    .saturating_sub(self.admitted.committed_bytes)
            }),
        })
    }
}

/// A readiness waiter answers once. Its targets are sorted by namespace, and
/// `targets` holds the readiness of each. A history or a view that an
/// evaluation finds changed since the admission rejects the waiter.
struct ReadinessWaiter {
    wait: ReadinessWait,
    targets: Vec<TargetReadiness>,
    /// The sum of the available offsets of the targets. It saturates only
    /// above any `min_count`.
    available: u64,
    /// Namespaces that changed since the last probe. A commit makes only its
    /// own target ready, so a probe evaluates these targets.
    dirty: Vec<IggyNamespace>,
    /// Maintenance evaluates every target in turn: `swept` targets of its
    /// current pass are done, and the next probe evaluates `sweep` more.
    swept: usize,
    sweep: usize,
    reply: Sender<PartitionReadReply>,
}

impl ReadinessWaiter {
    fn fires(&self) -> bool {
        self.available >= u64::from(self.wait.min_count.max(ANY_MESSAGE))
    }

    fn ready(&self) -> Vec<ReadyTarget> {
        self.wait
            .targets
            .iter()
            .zip(&self.targets)
            .filter_map(|(target, readiness)| readiness.ready(target.partition_id))
            .collect()
    }
}

struct Waiter {
    namespace: IggyNamespace,
    request: DeferredPollRequest,
    /// Admission turned this `Next` poll into an `Offset` poll that starts
    /// after the stored offset.
    next: bool,
    reply: Sender<PartitionReadReply>,
    history: PollHistoryId,
    view: u32,
    /// The visibility at the last read, `None` before the first read.
    last_visibility: Option<PollVisibility>,
    running: bool,
    terminal: bool,
    /// When a full read budget first refused this waiter. It keeps the
    /// waiter's place in the read queue until one of its reads starts.
    refused_at: Option<u64>,
    /// At most what a read from the start through `last_visibility` selects.
    selected: Selection,
}

impl Waiter {
    /// At most what a read from the start selects at `visibility`. Offsets
    /// are consecutive, so the offsets committed after the last read bound
    /// the messages that a read can add, whatever the polling kind.
    fn selection_at(&self, visibility: PollVisibility) -> Option<Selection> {
        let last = self.last_visibility?;
        let args = &self.request.args;
        let mut from = last.offset.map_or(0, |offset| offset.saturating_add(1));
        if args.kind == PollingKind::Offset {
            from = from.max(args.value);
        }
        let messages = visibility
            .offset
            .map_or(0, |offset| offset.saturating_add(1).saturating_sub(from));
        let bytes = visibility
            .committed_bytes
            .saturating_sub(last.committed_bytes);
        Some(Selection {
            messages: u32::try_from(messages).map_or(u32::MAX, |messages| {
                self.selected.messages.saturating_add(messages)
            }),
            bytes: usize::try_from(bytes).map_or(usize::MAX, |bytes| {
                self.selected.bytes.saturating_add(bytes)
            }),
        })
    }

    /// Whether a probe at `visibility` only parks the waiter again, because
    /// its read runs or because a read from the start could not finish. In
    /// the second case the waiter moves its bound to `visibility`: what was
    /// committed after the last read bounds what a read adds.
    fn parks_at(&mut self, visibility: PollVisibility, max_read_bytes: usize) -> bool {
        if !self.running
            && !self.terminal
            && self.last_visibility != Some(visibility)
            && let Some(selected) = self.selection_at(visibility)
            && !selected.finishes_read(&self.request.options, max_read_bytes)
        {
            self.last_visibility = Some(visibility);
            self.selected = selected;
        }
        self.running || (!self.terminal && self.last_visibility == Some(visibility))
    }
}

#[derive(Clone, Copy, Default)]
struct Selection {
    messages: u32,
    /// Counted as `PollReadResult::limit_bytes` counts them.
    bytes: usize,
}

impl Selection {
    /// The reasons of `accept_deferred_result` that do not depend on time. A
    /// disk read that its walk budget would stop earlier is not predicted, and
    /// it runs when one of these holds or when the wait expires.
    fn finishes_read(self, options: &DeferredPollOptions, max_read_bytes: usize) -> bool {
        self.messages >= options.min_count
            || self.bytes >= reply_byte_limit(options, max_read_bytes)
    }
}

/// The selection bytes that a reply to `options` keeps, counted as
/// `PollReadResult::limit_bytes` counts them.
fn reply_byte_limit(options: &DeferredPollOptions, max_read_bytes: usize) -> usize {
    (options.max_bytes as usize)
        .min(max_read_bytes)
        .saturating_sub(POLL_RESPONSE_HEADER_SIZE)
}

/// The part of a read reservation that its disk selection may hold. Index
/// loading stays under one part (`SEALED_INDEX_RESIDENT_MAX_BYTES`) and the
/// snapshot pins journal buffers that exist anyway, so each keeps one minimum
/// part and the selection gets the rest, at least its third.
fn walk_bytes(reservation: usize) -> usize {
    reservation
        .saturating_sub((READ_BUDGET_PARTS - 1) * READ_PART_MIN_BYTES)
        .max(reservation / READ_BUDGET_PARTS)
}

/// Up to `MAINTENANCE_BUDGET` keys of `map` after `cursor`. After the last
/// key, the sweep starts again at the first.
fn keys_after<K: Ord + Copy, V>(map: &BTreeMap<K, V>, cursor: Option<K>) -> Vec<K> {
    let after = cursor.map_or(Unbounded, Excluded);
    let keys: Vec<_> = map
        .range((after, Unbounded))
        .map(|(key, _)| *key)
        .take(MAINTENANCE_BUDGET)
        .collect();
    if keys.is_empty() {
        return map.keys().copied().take(MAINTENANCE_BUDGET).collect();
    }
    keys
}

/// Only a watched partition has a notifier, so only its commits mark it.
#[derive(Default)]
struct Signals {
    queued: HashSet<IggyNamespace>,
    dirty: VecDeque<IggyNamespace>,
    /// Marked partitions whose status changed, so every parked read of them
    /// checks it, also a poll that a commit alone would leave parked.
    changed: HashSet<IggyNamespace>,
    waker: Option<Waker>,
}

impl Signals {
    fn mark_changed(&mut self, namespace: IggyNamespace) {
        self.changed.insert(namespace);
        self.mark(namespace);
    }

    fn mark(&mut self, namespace: IggyNamespace) {
        if self.queued.insert(namespace) {
            self.dirty.push_back(namespace);
            if let Some(waker) = self.waker.take() {
                waker.wake();
            }
        }
    }
}

/// A partition of this shard that other shards watch.
struct Watch {
    watchers: Vec<ReadinessWatch>,
    /// The groups whose installed owner each push carries.
    groups: BTreeSet<u64>,
    notifier: Rc<dyn Fn()>,
    /// The state of the last push, so that a sweep pushes only a change.
    last: Option<ServedState>,
}

/// The copy of a partition of another shard that local waits evaluate.
struct Mirror {
    owner: u16,
    seq: u64,
    /// `None` until the owner pushes the state of a partition that it serves.
    state: Option<ServedState>,
    /// When a wait last named the partition.
    used: u64,
    /// When this shard applied the last push.
    pushed: u64,
}

impl Mirror {
    /// Whether no wait named the partition for `MIRROR_IDLE_US`. A parked
    /// wait also needs the mirror, and the caller checks that.
    const fn unused(&self, now: u64) -> bool {
        now.saturating_sub(self.used) >= MIRROR_IDLE_US
    }
}

#[derive(Default)]
struct Mirrors {
    next_seq: u64,
    entries: BTreeMap<IggyNamespace, Mirror>,
    cursor: Option<IggyNamespace>,
}

#[derive(Default)]
struct State {
    next_id: u64,
    waiters: BTreeMap<u64, Waiter>,
    readiness: BTreeMap<u64, ReadinessWaiter>,
    partitions: BTreeMap<IggyNamespace, BTreeSet<u64>>,
    deadlines: BTreeSet<(u64, u64)>,
    sessions: BTreeMap<PollQuotaIdentity, usize>,
    ready: VecDeque<u64>,
    queued: BTreeSet<u64>,
    /// Parked waiters whose read the budget refused, by `(refused_at, id)`.
    read_queue: BTreeSet<(u64, u64)>,
    maintenance_cursor: u64,
    watches: BTreeMap<IggyNamespace, Watch>,
    /// The watched partitions to push at the next flush, by watching shard.
    /// A flush reads the current state, so the latest state wins.
    pushes: BTreeMap<u16, BTreeSet<IggyNamespace>>,
    watch_cursor: Option<IggyNamespace>,
    closed: bool,
}

impl State {
    fn enqueue(&mut self, id: u64) {
        if (self.waiters.contains_key(&id) || self.readiness.contains_key(&id))
            && self.queued.insert(id)
        {
            self.ready.push_back(id);
        }
    }

    fn pending(&self) -> usize {
        self.waiters.len() + self.readiness.len()
    }

    /// Returns a new waiter id if the registry and the session quota have room.
    fn admit(
        &mut self,
        quota: PollQuotaIdentity,
        config: &DeferredPollConfig,
    ) -> Result<u64, IggyError> {
        if self.closed {
            return Err(IggyError::TransientNotAccepted);
        }
        if self.pending() >= config.max_pending
            || self.sessions.get(&quota).copied().unwrap_or(0) >= config.max_pending_per_session
        {
            return Err(IggyError::DeferredPollCapacityExceeded);
        }
        let id = self
            .next_id
            .checked_add(1)
            .ok_or(IggyError::TransientNotAccepted)?;
        self.next_id = id;
        Ok(id)
    }

    /// Removes the watchers of `namespace` that `stop` selects. Returns
    /// whether the partition lost its last watcher.
    fn unwatch(
        &mut self,
        namespace: IggyNamespace,
        stop: impl Fn(&ReadinessWatch) -> bool,
    ) -> bool {
        let Some(watch) = self.watches.get_mut(&namespace) else {
            return false;
        };
        watch.watchers.retain(|watcher| !stop(watcher));
        if !watch.watchers.is_empty() {
            return false;
        }
        self.watches.remove(&namespace);
        true
    }

    fn ids_after(&self, cursor: u64) -> Vec<u64> {
        let after = (Excluded(cursor), Unbounded);
        let mut ids: Vec<_> = self
            .waiters
            .range(after)
            .map(|(id, _)| *id)
            .take(MAINTENANCE_BUDGET)
            .chain(
                self.readiness
                    .range(after)
                    .map(|(id, _)| *id)
                    .take(MAINTENANCE_BUDGET),
            )
            .collect();
        ids.sort_unstable();
        ids.truncate(MAINTENANCE_BUDGET);
        ids
    }
}

pub struct DeferredPolls {
    metrics: DeferredPollMetrics,
    config: RefCell<DeferredPollConfig>,
    state: RefCell<State>,
    signals: Rc<RefCell<Signals>>,
    mirrors: RefCell<Mirrors>,
    inflight_bytes: Arc<AtomicUsize>,
    /// The status and view that the last tick saw of each partition with
    /// parked reads or watchers.
    statuses: RefCell<BTreeMap<IggyNamespace, (PartitionStatus, u32)>>,
}

impl DeferredPolls {
    pub(crate) fn new(metrics: DeferredPollMetrics) -> Self {
        Self {
            metrics,
            config: RefCell::new(DeferredPollConfig::default()),
            state: RefCell::new(State::default()),
            signals: Rc::new(RefCell::new(Signals::default())),
            mirrors: RefCell::new(Mirrors::default()),
            inflight_bytes: Arc::new(AtomicUsize::new(0)),
            statuses: RefCell::new(BTreeMap::new()),
        }
    }

    /// The wait a request gets. A client wait only bounds it, so a wait above
    /// this server's limit waits for the limit instead of failing.
    pub(crate) fn wait_us(&self, wait_us: u64) -> u64 {
        wait_us.min(self.config.borrow().max_wait_us)
    }

    /// The notifier that a change of the partition at `namespace` calls.
    fn notifier(&self, namespace: IggyNamespace) -> Rc<dyn Fn()> {
        let signals = Rc::downgrade(&self.signals);
        Rc::new(move || {
            if let Some(signals) = signals.upgrade() {
                signals.borrow_mut().mark(namespace);
            }
        })
    }

    pub(crate) fn earliest_deadline(&self) -> Option<u64> {
        self.state
            .borrow()
            .deadlines
            .first()
            .map(|(deadline, _)| *deadline)
    }

    pub(crate) fn poll_ready(&self, context: &Context<'_>) -> Poll<()> {
        let mut signals = self.signals.borrow_mut();
        let state = self.state.borrow();
        // Freed read room wakes nobody, so a refused read that can start
        // keeps the pump turning.
        if !signals.dirty.is_empty() || !state.ready.is_empty() || self.head_read_fits(&state) {
            Poll::Ready(())
        } else {
            signals.waker = Some(context.waker().clone());
            Poll::Pending
        }
    }

    /// A tick probes polls and readiness targets up to `MAINTENANCE_BUDGET`,
    /// so a large wait spreads over several ticks. The cursor stays before a
    /// wait that the budget covers in part, and the next tick resumes it.
    pub(crate) fn maintenance(&self) {
        let mut state = self.state.borrow_mut();
        let mut ids = state.ids_after(state.maintenance_cursor);
        if ids.is_empty() {
            ids = state.ids_after(0);
        }
        let mut budget = MAINTENANCE_BUDGET;
        for id in ids {
            if budget == 0 {
                break;
            }
            let partial = if let Some(waiter) = state.readiness.get_mut(&id) {
                let remaining = waiter
                    .wait
                    .targets
                    .len()
                    .saturating_sub(waiter.swept + waiter.sweep);
                let sweep = remaining.min(budget);
                waiter.sweep += sweep;
                budget -= sweep;
                sweep < remaining
            } else {
                budget -= 1;
                false
            };
            state.enqueue(id);
            if partial {
                break;
            }
            state.maintenance_cursor = id;
        }
    }

    pub(crate) const fn pump_guard<'a>(
        &'a self,
        shard: u16,
        senders: &'a [TaggedSender],
    ) -> PumpGuard<'a> {
        PumpGuard {
            polls: self,
            shard,
            senders,
        }
    }

    /// Rejects the parked waiters, and tells every watching shard that
    /// `shard` no longer serves the partitions that it watches.
    pub(crate) fn close(&self, shard: u16, senders: &[TaggedSender]) {
        let mut state = self.state.borrow_mut();
        state.closed = true;
        let mut unavailable: BTreeMap<u16, Vec<WatchedState>> = BTreeMap::new();
        for (namespace, watch) in std::mem::take(&mut state.watches) {
            for watcher in watch.watchers {
                unavailable
                    .entry(watcher.shard)
                    .or_default()
                    .push(WatchedState {
                        namespace,
                        seq: watcher.seq,
                        state: None,
                    });
            }
        }
        for (watcher, states) in unavailable {
            if let Some(sender) = senders.get(usize::from(watcher)) {
                let _ = sender.reply_sender().try_send(ShardFrame::lifecycle(
                    LifecycleFrame::ReadinessUpdate {
                        from: shard,
                        states,
                    },
                ));
            }
        }
        state.pushes.clear();
        self.mirrors.borrow_mut().entries.clear();
        self.metrics.readiness_watches.set(0);
        for reply in state
            .waiters
            .values()
            .map(|waiter| &waiter.reply)
            .chain(state.readiness.values().map(|waiter| &waiter.reply))
        {
            let _ = reply.try_send(PartitionReadReply::Rejected(
                IggyError::TransientNotAccepted,
            ));
        }
        state.waiters.clear();
        state.readiness.clear();
        state.partitions.clear();
        state.deadlines.clear();
        state.sessions.clear();
        state.ready.clear();
        state.queued.clear();
        state.read_queue.clear();
        self.metrics.pending.set(0);
        *self.signals.borrow_mut() = Signals::default();
    }

    /// The budget that a disk read with `max_bytes` reserves. Its snapshot,
    /// disk selection and index loading can coexist, so it reserves one part
    /// as large as its reply for each, at least [`READ_PART_MIN_BYTES`], up
    /// to `max_read_bytes` in total. [`walk_bytes`] splits it.
    fn read_bytes(&self, max_bytes: u32) -> usize {
        (max_bytes as usize)
            .max(READ_PART_MIN_BYTES)
            .saturating_mul(READ_BUDGET_PARTS)
            .min(self.config.borrow().max_read_bytes)
    }

    fn has_read_room(&self, bytes: usize) -> bool {
        Self::with_read(
            &self.config.borrow(),
            self.inflight_bytes.load(Ordering::Relaxed),
            bytes,
        )
        .is_some()
    }

    /// Whether the read at the head of the read queue fits the free budget.
    /// Only that read can start, so a large one keeps the smaller reads
    /// behind it waiting. A head without a waiter fits, so that the service
    /// loop drops it instead of stalling the queue.
    fn head_read_fits(&self, state: &State) -> bool {
        state.read_queue.first().is_some_and(|(_, id)| {
            state.waiters.get(id).is_none_or(|waiter| {
                self.has_read_room(self.read_bytes(waiter.request.options.max_bytes))
            })
        })
    }

    /// Takes a waiter out of the read queue for a probe. Only a read that the
    /// budget refuses again puts it back, at the same place.
    fn take_read_place(&self, id: u64, waiter: &mut Waiter) -> Option<u64> {
        let refused_at = waiter.refused_at.take()?;
        self.state.borrow_mut().read_queue.remove(&(refused_at, id));
        Some(refused_at)
    }

    /// Whether a read at `place` that reserves `bytes` can start now. A read
    /// starts only ahead of every refused read, so that a waiter whose read
    /// keeps coming back cannot take the budget from a refused one.
    fn read_turn(&self, place: (u64, u64), bytes: usize) -> bool {
        self.state
            .borrow()
            .read_queue
            .first()
            .is_none_or(|first| place < *first)
            && self.has_read_room(bytes)
    }

    fn reserve_read_in_turn(&self, place: (u64, u64), bytes: usize) -> Option<ReadReservation> {
        self.read_turn(place, bytes)
            .then(|| self.reserve_read(bytes).ok())
            .flatten()
    }

    /// Parks a waiter whose read found no room at `place` in the read queue.
    fn queue_read(&self, id: u64, mut waiter: Waiter, place: (u64, u64)) {
        waiter.refused_at = Some(place.0);
        let mut state = self.state.borrow_mut();
        state.read_queue.insert(place);
        state.waiters.insert(id, waiter);
    }

    fn reserve_read(&self, bytes: usize) -> Result<ReadReservation, IggyError> {
        let config = *self.config.borrow();
        self.inflight_bytes
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |used| {
                Self::with_read(&config, used, bytes)
            })
            .map_err(|_| IggyError::DeferredPollCapacityExceeded)?;
        self.metrics
            .inflight_bytes
            .inc_by(i64::try_from(bytes).unwrap_or(i64::MAX));
        Ok(ReadReservation {
            gauge: self.metrics.inflight_bytes.clone(),
            bytes,
            used: Arc::clone(&self.inflight_bytes),
        })
    }

    /// The in-flight read bytes after one more reservation of `bytes`, if
    /// they fit.
    fn with_read(config: &DeferredPollConfig, used: usize, bytes: usize) -> Option<usize> {
        used.checked_add(bytes)
            .filter(|next| *next <= config.max_inflight_bytes)
    }

    /// Returns the namespaces that no waiter and no watching shard needs any
    /// more.
    fn remove(
        &self,
        id: u64,
        deadlines: &[u64],
        quota: PollQuotaIdentity,
        namespaces: impl IntoIterator<Item = IggyNamespace>,
    ) -> Vec<IggyNamespace> {
        let mut state = self.state.borrow_mut();
        for deadline in deadlines {
            state.deadlines.remove(&(*deadline, id));
        }
        // The pump takes a waiter off `ready` before it finishes it, so only a
        // waiter that a wake queued again needs the scan.
        if state.queued.remove(&id) {
            state.ready.retain(|queued| *queued != id);
        }
        debug_assert!(
            state.read_queue.iter().all(|(_, queued)| *queued != id),
            "a probe takes the read place of a waiter before it removes the waiter"
        );
        self.metrics.pending.dec();
        if let Some(count) = state.sessions.get_mut(&quota) {
            *count -= 1;
            if *count == 0 {
                state.sessions.remove(&quota);
            }
        }
        let mut unwatched = Vec::new();
        for namespace in namespaces {
            let empty = state.partitions.get_mut(&namespace).is_some_and(|ids| {
                ids.remove(&id);
                ids.is_empty()
            });
            if empty {
                state.partitions.remove(&namespace);
                if !state.watches.contains_key(&namespace) {
                    unwatched.push(namespace);
                }
            }
        }
        unwatched
    }
}

pub struct PumpGuard<'a> {
    polls: &'a DeferredPolls,
    shard: u16,
    senders: &'a [TaggedSender],
}

impl Drop for PumpGuard<'_> {
    fn drop(&mut self) {
        self.polls.close(self.shard, self.senders);
    }
}

/// May cross the completion channel; it accounts for running and queued reads.
pub struct ReadReservation {
    gauge: Gauge,
    bytes: usize,
    used: Arc<AtomicUsize>,
}

impl Drop for ReadReservation {
    fn drop(&mut self) {
        self.used.fetch_sub(self.bytes, Ordering::Relaxed);
        self.gauge
            .dec_by(i64::try_from(self.bytes).unwrap_or(i64::MAX));
    }
}

impl<B, MJ, S, M, T, SB> IggyShard<B, MJ, S, M, T, SB>
where
    B: MessageBus + 'static,
    T: ShardsTable,
    M: StreamsFrontend,
    SB: SuperblockStore,
{
    pub fn set_deferred_poll_config(&self, config: DeferredPollConfig) {
        *self.deferred_polls.config.borrow_mut() = config;
    }

    pub fn deferred_wait_us(&self, wait_us: u64) -> u64 {
        self.deferred_polls.wait_us(wait_us)
    }

    /// The deferred poll limits, which every shard shares.
    #[must_use]
    pub fn deferred_poll_config(&self) -> DeferredPollConfig {
        *self.deferred_polls.config.borrow()
    }

    fn validate_deferred_context(
        &self,
        namespace: IggyNamespace,
        request: &DeferredPollRequest,
    ) -> Result<(PollHistoryId, PollVisibility, u32), IggyError> {
        self.validate_caller(namespace, &request.context)?;
        self.validate_partition(
            namespace,
            request.context.primary,
            &request.metadata,
            request.consumer,
        )
    }

    /// The checks of the caller, the same for every partition of one stream
    /// and topic.
    fn validate_caller(
        &self,
        namespace: IggyNamespace,
        context: &DeferredPollContext,
    ) -> Result<(), IggyError> {
        if context
            .session
            .as_ref()
            .is_some_and(|session| !session.is_valid())
        {
            return Err(IggyError::StaleClient);
        }
        if !self
            .plane
            .metadata()
            .applied_frontier()
            .partition_requests_allowed(self.bus.monotonic_now())
        {
            return Err(IggyError::TransientNotAccepted);
        }
        let metadata = &self.plane.metadata().mux_stm;
        if context.user_created_at.is_some_and(|created_at| {
            !metadata
                .users()
                .read(|users| users.is_active_account(context.user_id, created_at))
        }) {
            return Err(IggyError::Unauthenticated);
        }
        metadata.users().authorize(|permissioner| {
            permissioner.poll_messages(context.user_id, namespace.stream_id(), namespace.topic_id())
        })?;
        Ok(())
    }

    /// The checks of one partition, after those of its caller.
    fn validate_partition(
        &self,
        namespace: IggyNamespace,
        primary: bool,
        metadata: &PollMetadata,
        consumer: PollingConsumer,
    ) -> Result<(PollHistoryId, PollVisibility, u32), IggyError> {
        if !metadata.is_valid(self.plane.metadata().mux_stm.streams(), namespace) {
            return Err(IggyError::TransientNotAccepted);
        }
        let incarnation = self.shards_table.epoch_for(namespace);
        self.plane
            .partitions()
            .with_partition(&namespace, |partition| {
                let installed = PartitionStatus::of(partition, incarnation).check(
                    namespace,
                    primary,
                    metadata,
                    consumer,
                    |group| partition.consumer_group_owner(group),
                )?;
                if !installed {
                    return Err(IggyError::TransientNotAccepted);
                }
                let (history, visibility) = partition.poll_visibility();
                Ok((history, visibility, partition.consensus().view()))
            })
            .ok_or(IggyError::TransientNotAccepted)?
    }

    /// The visibility of the partition, while its history and view are those
    /// of the admission. A `Next` poll also needs the stored offset of its
    /// admission: another session can store an offset while it waits, and a
    /// commit of this poll would then pass over a rewind.
    fn validate_waiter(&self, waiter: &Waiter) -> Result<PollVisibility, IggyError> {
        let (history, visibility, view) =
            self.validate_deferred_context(waiter.namespace, &waiter.request)?;
        if history != waiter.history
            || view != waiter.view
            || (waiter.next
                && self.next_offset(waiter.namespace, waiter.request.consumer)?
                    != waiter.request.args.value)
        {
            return Err(IggyError::TransientNotAccepted);
        }
        Ok(visibility)
    }

    /// Where a `Next` poll of `consumer` starts: after its stored offset.
    fn next_offset(
        &self,
        namespace: IggyNamespace,
        consumer: PollingConsumer,
    ) -> Result<u64, IggyError> {
        self.plane
            .partitions()
            .with_partition(&namespace, |partition| {
                partition.get_consumer_offset(consumer)
            })
            .ok_or(IggyError::TransientNotAccepted)?
            .map_or(Ok(0), |offset| {
                offset
                    .checked_add(1)
                    .ok_or(IggyError::InvalidOffset(offset))
            })
    }

    /// The pump services the admitted waiter at the top of its next turn.
    #[allow(clippy::too_many_lines)]
    pub(super) fn admit_deferred_poll(
        &self,
        namespace: IggyNamespace,
        mut request: DeferredPollRequest,
        reply: &Sender<PartitionReadReply>,
    ) {
        if reply.is_disconnected() {
            return;
        }
        let admission = (|| {
            request.options.validate(request.args.count)?;
            let (history, _, view) = self.validate_deferred_context(namespace, &request)?;
            let config = *self.deferred_polls.config.borrow();
            let mut state = self.deferred_polls.state.borrow_mut();
            let id = state.admit(request.context.quota, &config)?;
            let next = request.args.kind == PollingKind::Next;
            if next {
                request.args.kind = PollingKind::Offset;
                request.args.value = self.next_offset(namespace, request.consumer)?;
            }
            self.register_waiter(
                &mut state,
                id,
                (request.context.quota, request.deadline),
                [namespace],
            );
            state.waiters.insert(
                id,
                Waiter {
                    namespace,
                    request,
                    next,
                    reply: reply.clone(),
                    history,
                    view,
                    last_visibility: None,
                    running: false,
                    terminal: false,
                    refused_at: None,
                    selected: Selection::default(),
                },
            );
            state.enqueue(id);
            Ok(())
        })();
        if let Err(error) = admission {
            self.metrics.deferred_polls.rejected.inc();
            let _ = reply.try_send(PartitionReadReply::Rejected(error));
        }
    }

    /// One consensus tick of the deferred polls. A partition that loses its
    /// primary, changes view or starts a state transfer marks nothing, so the
    /// tick compares the status of each partition with parked reads or
    /// watchers to the last tick and wakes those reads at once, instead of
    /// when the sweep reaches them.
    pub(crate) fn tick_deferred_polls(&self) {
        let namespaces: BTreeSet<IggyNamespace> = {
            let state = self.deferred_polls.state.borrow();
            state
                .partitions
                .keys()
                .chain(state.watches.keys())
                .copied()
                .collect()
        };
        {
            let mut statuses = self.deferred_polls.statuses.borrow_mut();
            statuses.retain(|namespace, _| namespaces.contains(namespace));
            for namespace in namespaces {
                let incarnation = self.shards_table.epoch_for(namespace);
                let Some(current) =
                    self.plane
                        .partitions()
                        .with_partition(&namespace, |partition| {
                            (
                                PartitionStatus::of(partition, incarnation),
                                partition.consensus().view(),
                            )
                        })
                else {
                    continue;
                };
                if statuses
                    .insert(namespace, current)
                    .is_some_and(|previous| previous != current)
                {
                    self.deferred_polls
                        .signals
                        .borrow_mut()
                        .mark_changed(namespace);
                }
            }
        }
        self.deferred_polls.maintenance();
    }

    #[allow(clippy::future_not_send)]
    pub(crate) async fn service_deferred_polls(&self) {
        for _ in 0..SERVICE_BUDGET {
            let now = self.bus.monotonic_micros();
            let next = {
                let mut state = self.deferred_polls.state.borrow_mut();
                if let Some(&(deadline, id)) = state.deadlines.first()
                    && deadline <= now
                {
                    Some(id)
                } else if self.deferred_polls.head_read_fits(&state)
                    && let Some((_, id)) = state.read_queue.pop_first()
                {
                    // Freed room goes to the read that the budget refused
                    // first, ahead of the other wakes.
                    Some(id)
                } else {
                    let (namespace, changed) = {
                        let mut signals = self.deferred_polls.signals.borrow_mut();
                        let namespace = signals.dirty.pop_front();
                        let changed = namespace.is_some_and(|namespace| {
                            signals.queued.remove(&namespace);
                            signals.changed.remove(&namespace)
                        });
                        (namespace, changed)
                    };
                    if let Some(namespace) = namespace {
                        // A commit queues a poll only if its history or view
                        // changed or if its probe can read. One read of the
                        // partition decides that for every poll. Each probe still
                        // validates before its read and commit, and maintenance
                        // and the deadline still probe the polls that stay parked.
                        let current =
                            self.plane
                                .partitions()
                                .with_partition(&namespace, |partition| {
                                    let (history, visibility) = partition.poll_visibility();
                                    (history, visibility, partition.consensus().view())
                                });
                        let max_read_bytes = self.deferred_polls.config.borrow().max_read_bytes;
                        let ids: Vec<_> = state
                            .partitions
                            .get(&namespace)
                            .into_iter()
                            .flat_map(|ids| ids.iter().copied())
                            .collect();
                        for id in ids {
                            if !changed
                                && let Some((history, visibility, view)) = current
                                && let Some(waiter) = state.waiters.get_mut(&id)
                                && waiter.history == history
                                && waiter.view == view
                                && waiter.parks_at(visibility, max_read_bytes)
                            {
                                continue;
                            }
                            if let Some(waiter) = state.readiness.get_mut(&id)
                                && !waiter.dirty.contains(&namespace)
                            {
                                waiter.dirty.push(namespace);
                            }
                            state.enqueue(id);
                        }
                        let state = &mut *state;
                        if let Some(watch) = state.watches.get(&namespace) {
                            for watcher in &watch.watchers {
                                state
                                    .pushes
                                    .entry(watcher.shard)
                                    .or_default()
                                    .insert(namespace);
                            }
                        }
                    }
                    let next = state.ready.pop_front();
                    match next {
                        Some(id) => {
                            state.queued.remove(&id);
                        }
                        // A change that woke no waiter, such as one that only
                        // other shards watch, does not end the turn.
                        None if namespace.is_some() => continue,
                        None => {}
                    }
                    next
                }
            };
            let Some(id) = next else { break };
            let waiter = self.deferred_polls.state.borrow_mut().waiters.remove(&id);
            if let Some(waiter) = waiter {
                self.probe_deferred_poll(id, waiter).await;
                continue;
            }
            let waiter = self.deferred_polls.state.borrow_mut().readiness.remove(&id);
            if let Some(waiter) = waiter {
                self.probe_readiness(id, waiter);
            }
        }
    }

    #[allow(clippy::future_not_send)]
    async fn probe_deferred_poll(&self, id: u64, mut waiter: Waiter) {
        let refused_at = self.deferred_polls.take_read_place(id, &mut waiter);
        if waiter.reply.is_disconnected() {
            self.metrics.deferred_polls.cancelled.inc();
            self.remove_deferred_poll(id, &waiter);
            return;
        }
        let visibility = match self.validate_waiter(&waiter) {
            Ok(visibility) => visibility,
            Err(error) => {
                self.reject_deferred_poll(id, &waiter, error);
                return;
            }
        };
        if self.bus.monotonic_micros() >= waiter.request.request_deadline {
            self.reject_deferred_poll(id, &waiter, IggyError::TransientNotCommitted);
            return;
        }
        if self.bus.monotonic_micros() >= waiter.request.deadline {
            waiter.terminal = true;
            let mut state = self.deferred_polls.state.borrow_mut();
            state.deadlines.remove(&(waiter.request.deadline, id));
            state
                .deadlines
                .insert((waiter.request.request_deadline, id));
        }
        if waiter.parks_at(
            visibility,
            self.deferred_polls.config.borrow().max_read_bytes,
        ) {
            self.deferred_polls
                .state
                .borrow_mut()
                .waiters
                .insert(id, waiter);
            return;
        }
        // A busy budget only delays the read. The waiter keeps its last
        // visibility, so that its next probe does not skip the read. A read
        // that the budget refused needed the disk, so until its turn it
        // waits without building another plan.
        let place = (
            refused_at.unwrap_or_else(|| self.bus.monotonic_micros()),
            id,
        );
        let read_bytes = self
            .deferred_polls
            .read_bytes(waiter.request.options.max_bytes);
        if refused_at.is_some() && !self.deferred_polls.read_turn(place, read_bytes) {
            self.deferred_polls.queue_read(id, waiter, place);
            return;
        }
        let Some(plan) = self.plane.partitions().build_poll_snapshot(
            &waiter.namespace,
            waiter.request.consumer,
            &waiter.request.args,
        ) else {
            self.reject_deferred_poll(id, &waiter, IggyError::TransientNotAccepted);
            return;
        };
        if !plan.needs_off_pump_io() {
            waiter.last_visibility = Some(visibility);
            let result = plan.execute_resident();
            self.accept_deferred_result(id, waiter, result).await;
            return;
        }
        let Some(reservation) = self.deferred_polls.reserve_read_in_turn(place, read_bytes) else {
            self.deferred_polls.queue_read(id, waiter, place);
            return;
        };
        let max_bytes = walk_bytes(reservation.bytes);
        let max_selected_bytes = reply_byte_limit(
            &waiter.request.options,
            self.deferred_polls.config.borrow().max_read_bytes,
        );
        let completion =
            match self
                .poll_completions
                .try_reserve_deferred(waiter.namespace, id, reservation)
            {
                Ok(completion) => completion,
                Err(error) => {
                    self.reject_deferred_poll(id, &waiter, error);
                    return;
                }
            };
        waiter.last_visibility = Some(visibility);
        waiter.running = true;
        self.deferred_polls
            .state
            .borrow_mut()
            .waiters
            .insert(id, waiter);
        self.bus.spawn(async move {
            completion.complete(plan.execute_with_limit(max_bytes, max_selected_bytes).await);
        });
    }

    #[allow(clippy::future_not_send)]
    pub(super) async fn on_deferred_poll_completed(&self, id: u64, result: PollReadResult) {
        let waiter = self.deferred_polls.state.borrow_mut().waiters.remove(&id);
        if let Some(mut waiter) = waiter {
            waiter.running = false;
            self.accept_deferred_result(id, waiter, result).await;
        } else {
            self.metrics.deferred_polls.late.inc();
        }
    }

    #[allow(clippy::future_not_send)]
    async fn accept_deferred_result(&self, id: u64, mut waiter: Waiter, result: PollReadResult) {
        if waiter.reply.is_disconnected() {
            self.metrics.deferred_polls.cancelled.inc();
            self.remove_deferred_poll(id, &waiter);
            return;
        }
        if self.bus.monotonic_micros() >= waiter.request.request_deadline {
            self.reject_deferred_poll(id, &waiter, IggyError::TransientNotCommitted);
            return;
        }
        let visibility = match self.validate_waiter(&waiter) {
            Ok(visibility) => visibility,
            Err(error) => {
                self.reject_deferred_poll(id, &waiter, error);
                return;
            }
        };
        let result = match result.checked() {
            Ok(result) => result,
            Err(error) => {
                self.reject_deferred_poll(id, &waiter, error);
                return;
            }
        };
        let max_read_bytes = self.deferred_polls.config.borrow().max_read_bytes;
        let (mut result, byte_limited) = match result.limit_bytes(
            waiter.request.options.max_bytes as usize,
            max_read_bytes,
            || true,
        ) {
            Ok(result) => result,
            Err(error) => {
                self.reject_deferred_poll(id, &waiter, error);
                return;
            }
        };
        if result.message_count() > 0
            && let Some(turn) = &waiter.request.oversized_turn
        {
            let oversized =
                result.encoded_bytes() > reply_byte_limit(&waiter.request.options, max_read_bytes);
            let accepted = if oversized {
                turn.take()
            } else {
                turn.accept_normal()
            };
            if !accepted {
                self.reject_deferred_poll(id, &waiter, IggyError::DeferredPollCapacityExceeded);
                return;
            }
        }
        if result.message_count() >= waiter.request.options.min_count
            || byte_limited
            || result.truncated()
            || waiter.terminal
            || self.bus.monotonic_micros() >= waiter.request.deadline
        {
            // Charge what the reply carries, not what the read walked over. A
            // selection that borrows journal buffers can still pin more than
            // the reservation, so copy it out instead of refusing a serveable
            // poll.
            if result.retained_bytes() > max_read_bytes {
                result = result.compacted();
            }
            self.finish_deferred_poll(id, waiter, result).await;
            return;
        }
        waiter.selected = Selection {
            messages: result.message_count(),
            bytes: result.encoded_bytes(),
        };
        let changed = waiter.last_visibility != Some(visibility);
        let mut state = self.deferred_polls.state.borrow_mut();
        state.waiters.insert(id, waiter);
        if changed {
            state.enqueue(id);
        }
    }

    fn remove_deferred_poll(&self, id: u64, waiter: &Waiter) {
        let request = &waiter.request;
        self.unwatch_partitions(self.deferred_polls.remove(
            id,
            &[request.deadline, request.request_deadline],
            request.context.quota,
            [waiter.namespace],
        ));
    }

    /// Counts the waiter toward its quota and makes a visibility change of
    /// any of `namespaces` wake it.
    fn register_waiter(
        &self,
        state: &mut State,
        id: u64,
        (quota, deadline): (PollQuotaIdentity, u64),
        namespaces: impl IntoIterator<Item = IggyNamespace>,
    ) {
        self.metrics.deferred_polls.pending.inc();
        *state.sessions.entry(quota).or_default() += 1;
        state.deadlines.insert((deadline, id));
        for namespace in namespaces {
            state.partitions.entry(namespace).or_default().insert(id);
            if let Some(partition) = self.plane.partitions().get_mut_by_ns(&namespace) {
                partition.set_poll_notifier(Some(self.deferred_polls.notifier(namespace)));
            }
        }
    }

    fn unwatch_partitions(&self, namespaces: Vec<IggyNamespace>) {
        for namespace in namespaces {
            if let Some(partition) = self.plane.partitions().get_mut_by_ns(&namespace) {
                partition.set_poll_notifier(None);
            }
        }
    }

    fn reject_deferred_poll(&self, id: u64, waiter: &Waiter, error: IggyError) {
        self.metrics.deferred_polls.failed.inc();
        self.remove_deferred_poll(id, waiter);
        let _ = waiter.reply.try_send(PartitionReadReply::Rejected(error));
    }

    #[allow(clippy::future_not_send)]
    async fn finish_deferred_poll(&self, id: u64, waiter: Waiter, result: PollReadResult) {
        if self.bus.monotonic_micros() >= waiter.request.request_deadline {
            self.reject_deferred_poll(id, &waiter, IggyError::TransientNotCommitted);
            return;
        }
        if let Err(error) = self.validate_waiter(&waiter) {
            self.reject_deferred_poll(id, &waiter, error);
            return;
        }
        self.remove_deferred_poll(id, &waiter);
        if waiter.reply.is_disconnected() {
            self.metrics.deferred_polls.cancelled.inc();
            return;
        }
        // A read that stores nothing needs no gate. One that would commit after
        // a cancel won stores nothing either.
        if result.message_count() > 0
            && let Some(gate) = &waiter.request.commit_gate
            && !gate.enter()
        {
            self.metrics.deferred_polls.cancelled.inc();
            let _ = waiter.reply.try_send(PartitionReadReply::Rejected(
                IggyError::TransientNotAccepted,
            ));
            return;
        }
        let before_deadline = self.bus.monotonic_micros() < waiter.request.deadline;
        if before_deadline {
            self.metrics.deferred_polls.filled.inc();
        } else {
            self.metrics.deferred_polls.expired.inc();
        }
        let next_offset = self.next_read_offset(waiter.namespace, &waiter.request.args, &result);
        self.accept_poll_result(waiter.namespace, result, waiter.reply, next_offset)
            .await;
    }

    /// Where the read after `result` starts: after its last message. An empty
    /// `Offset` read moves to the first offset that retention kept, so that a
    /// start in deleted segments moves on. `None` for an empty read of another
    /// kind, whose request names no start.
    fn next_read_offset(
        &self,
        namespace: IggyNamespace,
        args: &PollingArgs,
        result: &PollReadResult,
    ) -> Option<u64> {
        if let Some(last) = result.last_matching_offset() {
            return Some(last.saturating_add(1));
        }
        if args.kind != PollingKind::Offset {
            return None;
        }
        let first = self
            .plane
            .partitions()
            .with_partition(&namespace, |partition| {
                partition
                    .log
                    .segments()
                    .first()
                    .map(|segment| segment.start_offset)
            })
            .flatten();
        Some(first.map_or(args.value, |first| first.max(args.value)))
    }

    /// Answers at once with the ready targets. If none is ready and the wait
    /// did not expire, parks one waiter for all targets and answers with its
    /// id, so that the origin can cancel it. With `watch`, this shard also
    /// starts to push the state of the `Offset` targets to the watching
    /// shard, which then evaluates its waits on them itself.
    pub(crate) fn admit_readiness_wait(
        &self,
        wait: ReadinessWait,
        reply: &Sender<PartitionReadReply>,
        watch: Option<ReadinessWatch>,
    ) {
        if let Some(watch) = watch {
            self.watch_partitions(watch, &wait.targets);
        }
        if reply.is_disconnected() {
            return;
        }
        let answer = self
            .park_readiness_wait(wait, reply)
            .unwrap_or_else(|error| {
                self.metrics.deferred_polls.rejected.inc();
                PartitionReadReply::Rejected(error)
            });
        let _ = reply.try_send(answer);
    }

    pub(crate) fn cancel_readiness_wait(&self, id: u64) {
        let waiter = self.deferred_polls.state.borrow_mut().readiness.remove(&id);
        if let Some(waiter) = waiter {
            self.metrics.deferred_polls.cancelled.inc();
            self.remove_readiness_wait(id, &waiter);
        }
    }

    fn park_readiness_wait(
        &self,
        mut wait: ReadinessWait,
        reply: &Sender<PartitionReadReply>,
    ) -> Result<PartitionReadReply, IggyError> {
        let first = wait
            .targets
            .first()
            .ok_or(IggyError::InvalidCommand)?
            .namespace;
        if wait.targets.iter().any(|target| {
            target.namespace.stream_id() != first.stream_id()
                || target.namespace.topic_id() != first.topic_id()
        }) {
            return Err(IggyError::InvalidCommand);
        }
        self.validate_caller(first, &wait.context)?;
        // A probe finds the target of a changed namespace by binary search.
        wait.targets.sort_unstable_by_key(|target| target.namespace);
        let mut targets = Vec::with_capacity(wait.targets.len());
        let mut available = 0_u64;
        for target in &wait.targets {
            let readiness = self.evaluate_readiness(wait.context.primary, target)?;
            available = available.saturating_add(readiness.available);
            targets.push(TargetReadiness {
                admitted: readiness,
                last: readiness,
            });
        }
        let waiter = ReadinessWaiter {
            wait,
            targets,
            available,
            dirty: Vec::new(),
            swept: 0,
            sweep: 0,
            reply: reply.clone(),
        };
        let fires = waiter.fires();
        if fires || self.bus.monotonic_micros() >= waiter.wait.deadline {
            if fires {
                self.metrics.deferred_polls.filled.inc();
            } else {
                self.metrics.deferred_polls.expired.inc();
            }
            return Ok(PartitionReadReply::Ready {
                shard: self.id,
                parked: None,
                partitions: waiter.ready(),
            });
        }
        let config = *self.deferred_polls.config.borrow();
        let mut state = self.deferred_polls.state.borrow_mut();
        let quota = waiter.wait.context.quota;
        let id = state.admit(quota, &config)?;
        self.register_waiter(
            &mut state,
            id,
            (quota, waiter.wait.deadline),
            waiter.wait.targets.iter().map(|target| target.namespace),
        );
        state.readiness.insert(id, waiter);
        Ok(PartitionReadReply::Ready {
            shard: self.id,
            parked: Some(id),
            partitions: Vec::new(),
        })
    }

    /// The target as a poll with its strategy finds it now. Its available
    /// offsets follow the empty-plan test of `build_poll_plan`. The caller of
    /// the wait is checked before.
    fn evaluate_readiness(
        &self,
        primary: bool,
        target: &ReadinessTarget,
    ) -> Result<Readiness, IggyError> {
        let (history, visibility, view, serves) =
            self.evaluate_mirror(primary, target).unwrap_or_else(|| {
                self.validate_partition(
                    target.namespace,
                    primary,
                    &target.metadata,
                    target.consumer,
                )
                .map(|(history, visibility, view)| (history, visibility, view, true))
            })?;
        let start = match target.strategy.kind {
            PollingKind::Offset => target.strategy.value,
            PollingKind::Next => self.next_offset(target.namespace, target.consumer)?,
            _ => return Err(IggyError::InvalidCommand),
        };
        let available = match visibility.offset {
            Some(committed) if serves && start <= committed => {
                (committed - start).saturating_add(1)
            }
            _ => 0,
        };
        Ok(Readiness {
            history,
            view,
            available,
            committed_bytes: visibility.committed_bytes,
            at_end: visibility
                .offset
                .and_then(|committed| committed.checked_add(1))
                == Some(start),
        })
    }

    /// The reading of a target that this shard mirrors, `None` if it has no
    /// mirror of the target.
    fn evaluate_mirror(
        &self,
        primary: bool,
        target: &ReadinessTarget,
    ) -> Option<Result<Reading, IggyError>> {
        let mirrors = self.deferred_polls.mirrors.borrow();
        let mirror = mirrors.entries.get(&target.namespace)?;
        if !target
            .metadata
            .is_valid(self.plane.metadata().mux_stm.streams(), target.namespace)
        {
            return Some(Err(IggyError::TransientNotAccepted));
        }
        let Some(state) = &mirror.state else {
            return Some(Err(IggyError::TransientNotAccepted));
        };
        Some(
            state
                .check(primary, target)
                .map(|serves| (state.history, state.visibility, state.view, serves)),
        )
    }

    fn probe_readiness(&self, id: u64, mut waiter: ReadinessWaiter) {
        if waiter.reply.is_disconnected() {
            self.metrics.deferred_polls.cancelled.inc();
            self.remove_readiness_wait(id, &waiter);
            return;
        }
        let now = self.bus.monotonic_micros();
        let answer = match self.evaluate_readiness_waiter(&mut waiter) {
            Ok(()) if waiter.fires() => {
                self.metrics.deferred_polls.filled.inc();
                self.record_push_to_fire(&waiter, now);
                Some(PartitionReadReply::Ready {
                    shard: self.id,
                    parked: None,
                    partitions: waiter.ready(),
                })
            }
            // An expired wait reads what it has. The origin answers one
            // without data itself.
            Ok(()) if now >= waiter.wait.deadline => {
                self.metrics.deferred_polls.expired.inc();
                let partitions = waiter.ready();
                (!partitions.is_empty()).then(|| PartitionReadReply::Ready {
                    shard: self.id,
                    parked: None,
                    partitions,
                })
            }
            Ok(()) => {
                self.deferred_polls
                    .state
                    .borrow_mut()
                    .readiness
                    .insert(id, waiter);
                return;
            }
            Err(error) => {
                self.metrics.deferred_polls.failed.inc();
                Some(PartitionReadReply::Rejected(error))
            }
        };
        self.remove_readiness_wait(id, &waiter);
        if let Some(answer) = answer {
            let _ = waiter.reply.try_send(answer);
        }
    }

    /// Ends the waiter `id` that this shard parked for the targets that it
    /// evaluates of a wait that ends here, and returns those that hold
    /// messages, also below `min_count`.
    pub(crate) fn take_readiness_wait(&self, id: u64) -> Result<Vec<ReadyTarget>, IggyError> {
        let Some(mut waiter) = self.deferred_polls.state.borrow_mut().readiness.remove(&id) else {
            return Ok(Vec::new());
        };
        let evaluated = self.evaluate_readiness_waiter(&mut waiter);
        if self.bus.monotonic_micros() >= waiter.wait.deadline {
            self.metrics.deferred_polls.expired.inc();
        } else {
            self.metrics.deferred_polls.cancelled.inc();
        }
        self.remove_readiness_wait(id, &waiter);
        evaluated.map(|()| waiter.ready())
    }

    /// Evaluates the targets of `waiter` that can have changed since its last
    /// evaluation. A commit marks only its own namespace. This evaluates the
    /// targets of marked namespaces, also of those that the pump has not
    /// taken yet, and the targets that maintenance assigned. Maintenance
    /// reaches every other target in turn. A lookup per target keeps the cost
    /// of the marks that the shard still holds within the wait size.
    fn evaluate_readiness_waiter(&self, waiter: &mut ReadinessWaiter) -> Result<(), IggyError> {
        let targets = &waiter.wait.targets;
        let mut evaluated: Vec<usize> = {
            let signals = self.deferred_polls.signals.borrow();
            waiter
                .dirty
                .iter()
                .filter_map(|namespace| {
                    targets
                        .binary_search_by_key(namespace, |target| target.namespace)
                        .ok()
                })
                .chain(
                    targets
                        .iter()
                        .enumerate()
                        .filter(|(_, target)| signals.queued.contains(&target.namespace))
                        .map(|(index, _)| index),
                )
                .chain(waiter.swept..waiter.swept + waiter.sweep)
                .collect()
        };
        evaluated.sort_unstable();
        evaluated.dedup();
        waiter.dirty.clear();
        waiter.swept += waiter.sweep;
        waiter.sweep = 0;
        if waiter.swept >= targets.len() {
            waiter.swept = 0;
        }
        let context = &waiter.wait.context;
        if let Some(first) = targets.first() {
            self.validate_caller(first.namespace, context)?;
        }
        for index in evaluated {
            let (Some(target), Some(readiness)) =
                (targets.get(index), waiter.targets.get_mut(index))
            else {
                continue;
            };
            let last = self.evaluate_readiness(context.primary, target)?;
            if (last.history, last.view) != (readiness.admitted.history, readiness.admitted.view) {
                return Err(IggyError::TransientNotAccepted);
            }
            waiter.available = waiter
                .available
                .saturating_sub(readiness.last.available)
                .saturating_add(last.available);
            readiness.last = last;
        }
        Ok(())
    }

    /// Observes the time from the last push that this shard applied for a
    /// ready target to the fire of `waiter`. Targets of this shard have no
    /// push.
    fn record_push_to_fire(&self, waiter: &ReadinessWaiter, now: u64) {
        let pushed = {
            let mirrors = self.deferred_polls.mirrors.borrow();
            waiter
                .wait
                .targets
                .iter()
                .zip(&waiter.targets)
                .filter(|(_, readiness)| readiness.last.available > 0)
                .filter_map(|(target, _)| mirrors.entries.get(&target.namespace))
                .map(|mirror| mirror.pushed)
                .max()
        };
        if let Some(pushed) = pushed {
            self.metrics
                .record_readiness_push_to_fire(Duration::from_micros(now.saturating_sub(pushed)));
        }
    }

    fn remove_readiness_wait(&self, id: u64, waiter: &ReadinessWaiter) {
        self.unwatch_partitions(self.deferred_polls.remove(
            id,
            &[waiter.wait.deadline],
            waiter.wait.context.quota,
            waiter.wait.targets.iter().map(|target| target.namespace),
        ));
    }

    /// Starts or renews the watch of `watch.shard` on the `Offset` targets,
    /// and queues their state for it.
    fn watch_partitions(&self, watch: ReadinessWatch, targets: &[ReadinessTarget]) {
        let mut state = self.deferred_polls.state.borrow_mut();
        if state.closed {
            return;
        }
        let state = &mut *state;
        for target in targets
            .iter()
            .filter(|target| target.strategy.kind == PollingKind::Offset)
        {
            let namespace = target.namespace;
            let entry = match state.watches.entry(namespace) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    let notifier = self.deferred_polls.notifier(namespace);
                    if let Some(partition) = self.plane.partitions().get_mut_by_ns(&namespace) {
                        partition.set_poll_notifier(Some(Rc::clone(&notifier)));
                    }
                    self.metrics.deferred_polls.readiness_watches.inc();
                    entry.insert(Watch {
                        watchers: Vec::new(),
                        groups: BTreeSet::new(),
                        notifier,
                        last: None,
                    })
                }
            };
            match entry
                .watchers
                .iter_mut()
                .find(|watcher| watcher.shard == watch.shard)
            {
                Some(watcher) => watcher.seq = watch.seq,
                None => entry.watchers.push(watch),
            }
            if let PollingConsumer::ConsumerGroup(group, _) = target.consumer {
                entry.groups.insert(group as u64);
            }
            state
                .pushes
                .entry(watch.shard)
                .or_default()
                .insert(namespace);
        }
    }

    /// The state of a watched partition, `None` if this shard does not serve
    /// it.
    fn served_state(
        &self,
        namespace: IggyNamespace,
        groups: &BTreeSet<u64>,
    ) -> Option<ServedState> {
        let incarnation = self.shards_table.epoch_for(namespace);
        self.plane
            .partitions()
            .with_partition(&namespace, |partition| {
                let (history, visibility) = partition.poll_visibility();
                ServedState {
                    history,
                    visibility,
                    view: partition.consensus().view(),
                    status: PartitionStatus::of(partition, incarnation),
                    owners: groups
                        .iter()
                        .map(|group| (*group, partition.consumer_group_owner(*group)))
                        .collect(),
                }
            })
    }

    /// Sends the queued pushes, one frame per watching shard. They ride the
    /// reply lane, so that consensus traffic cannot hold them back. A full
    /// lane keeps them for the next flush, which reads the state again.
    pub(crate) fn flush_readiness_pushes(&self) {
        let mut idle = Vec::new();
        {
            let mut state = self.deferred_polls.state.borrow_mut();
            let state = &mut *state;
            let mut disconnected = Vec::new();
            state.pushes.retain(|watcher, namespaces| {
                let Some(sender) = self.senders.get(usize::from(*watcher)) else {
                    return false;
                };
                let lane = sender.reply_sender();
                if lane.is_full() {
                    return true;
                }
                let states: Vec<_> = namespaces
                    .iter()
                    .filter_map(|namespace| {
                        let watch = state.watches.get_mut(namespace)?;
                        let seq = watch
                            .watchers
                            .iter()
                            .find(|candidate| candidate.shard == *watcher)?
                            .seq;
                        let served = self.served_state(*namespace, &watch.groups);
                        watch.last.clone_from(&served);
                        Some(WatchedState {
                            namespace: *namespace,
                            seq,
                            state: served,
                        })
                    })
                    .collect();
                if states.is_empty() {
                    return false;
                }
                let pushed = states.len() as u64;
                let update = LifecycleFrame::ReadinessUpdate {
                    from: self.id,
                    states,
                };
                match lane.try_send(ShardFrame::lifecycle(update)) {
                    Ok(()) => {
                        self.metrics.deferred_polls.readiness_pushes.inc_by(pushed);
                        false
                    }
                    Err(TrySendError::Full(_)) => true,
                    Err(TrySendError::Disconnected(_)) => {
                        disconnected.push(*watcher);
                        false
                    }
                }
            });
            for watcher in disconnected {
                let namespaces: Vec<_> = state.watches.keys().copied().collect();
                for namespace in namespaces {
                    if state.unwatch(namespace, |candidate| candidate.shard == watcher) {
                        self.metrics.deferred_polls.readiness_watches.dec();
                        if !state.partitions.contains_key(&namespace) {
                            idle.push(namespace);
                        }
                    }
                }
            }
        }
        self.unwatch_partitions(idle);
    }

    /// A tick sweeps `MAINTENANCE_BUDGET` watched partitions and as many
    /// mirrors. It installs the notifier of a watched partition again,
    /// because a rebuilt partition starts without one, and it queues the
    /// changes that no notifier reports, such as a view change or a group
    /// owner install. It ends the watch of a mirror that no wait needs, also
    /// for a partition without commits, which pushes nothing.
    pub(crate) fn sweep_readiness_watches(&self) {
        let now = self.bus.monotonic_micros();
        let mut unused: BTreeMap<u16, Vec<(IggyNamespace, u64)>> = BTreeMap::new();
        {
            let mut state = self.deferred_polls.state.borrow_mut();
            let state = &mut *state;
            for namespace in keys_after(&state.watches, state.watch_cursor) {
                state.watch_cursor = Some(namespace);
                let Some(watch) = state.watches.get(&namespace) else {
                    continue;
                };
                if let Some(partition) = self.plane.partitions().get_mut_by_ns(&namespace) {
                    partition.set_poll_notifier(Some(Rc::clone(&watch.notifier)));
                }
                if self.served_state(namespace, &watch.groups) != watch.last {
                    for watcher in &watch.watchers {
                        state
                            .pushes
                            .entry(watcher.shard)
                            .or_default()
                            .insert(namespace);
                    }
                }
            }
            let mut mirrors = self.deferred_polls.mirrors.borrow_mut();
            for namespace in keys_after(&mirrors.entries, mirrors.cursor) {
                mirrors.cursor = Some(namespace);
                if let Some(mirror) = mirrors.entries.get(&namespace)
                    && mirror.unused(now)
                    && !state.partitions.contains_key(&namespace)
                {
                    unused
                        .entry(mirror.owner)
                        .or_default()
                        .push((namespace, mirror.seq));
                }
            }
        }
        for (owner, watches) in unused {
            self.unwatch_mirrors(owner, watches);
        }
    }

    /// Ends the watches of `from` whose sequence matches.
    pub(crate) fn unwatch_readiness(&self, from: u16, watches: Vec<(IggyNamespace, u64)>) {
        let mut idle = Vec::new();
        {
            let mut state = self.deferred_polls.state.borrow_mut();
            for (namespace, seq) in watches {
                if state.unwatch(namespace, |watcher| {
                    watcher.shard == from && watcher.seq == seq
                }) {
                    self.metrics.deferred_polls.readiness_watches.dec();
                    if !state.partitions.contains_key(&namespace) {
                        idle.push(namespace);
                    }
                }
            }
        }
        self.unwatch_partitions(idle);
    }

    /// Stores pushed states in their mirrors and marks their partitions, so
    /// that the service loop probes the local waiters on them. A push for a
    /// mirror that no wait needs ends its watch, and so does a push for a
    /// partition without a mirror.
    pub(crate) fn apply_readiness_update(&self, from: u16, states: Vec<WatchedState>) {
        let now = self.bus.monotonic_micros();
        let mut unused = Vec::new();
        {
            let state = self.deferred_polls.state.borrow();
            let mut mirrors = self.deferred_polls.mirrors.borrow_mut();
            let mut signals = self.deferred_polls.signals.borrow_mut();
            for update in states {
                let Some(mirror) = mirrors.entries.get_mut(&update.namespace) else {
                    unused.push((update.namespace, update.seq));
                    continue;
                };
                // A push from before the current watch.
                if mirror.owner != from || mirror.seq != update.seq {
                    continue;
                }
                // The state goes in also if the unwatch is refused, because
                // the next wait can still read the mirror.
                mirror.state = update.state;
                mirror.pushed = now;
                if mirror.unused(now) && !state.partitions.contains_key(&update.namespace) {
                    unused.push((update.namespace, update.seq));
                } else {
                    signals.mark(update.namespace);
                }
            }
        }
        self.unwatch_mirrors(from, unused);
    }

    /// Ends the watches of this shard on `watches` of `owner`. Their mirrors
    /// go only if the inbox of `owner` takes the unwatch, so that the next
    /// sweep or push repeats a refused one.
    fn unwatch_mirrors(&self, owner: u16, watches: Vec<(IggyNamespace, u64)>) {
        if watches.is_empty() {
            return;
        }
        let Some(sender) = self.senders.get(usize::from(owner)) else {
            return;
        };
        let namespaces: Vec<_> = watches.iter().map(|(namespace, _)| *namespace).collect();
        let unwatch = LifecycleFrame::ReadinessUnwatch {
            from: self.id,
            watches,
        };
        if sender.try_send(ShardFrame::lifecycle(unwatch)).is_ok() {
            let mut mirrors = self.deferred_polls.mirrors.borrow_mut();
            for namespace in namespaces {
                mirrors.entries.remove(&namespace);
            }
        }
    }
}

/// The routing of a readiness wait on the shard of its connection.
impl<B, MJ, S, M, T, SB> IggyShard<B, MJ, S, M, T, SB>
where
    B: MessageBus + 'static,
    T: ShardsTable,
    SB: SuperblockStore,
{
    /// The watch that the next frame to an owner asks for.
    pub(crate) fn next_readiness_watch(&self) -> ReadinessWatch {
        let mut mirrors = self.deferred_polls.mirrors.borrow_mut();
        mirrors.next_seq = mirrors.next_seq.wrapping_add(1);
        ReadinessWatch {
            shard: self.id,
            seq: mirrors.next_seq,
        }
    }

    /// Whether this shard evaluates `target` against its mirror. A mirror
    /// that finds an error in the partition, or that lags the install of a
    /// group owner, sends the target to the owner, so that no wait gets a
    /// stale error from a mirror.
    pub(crate) fn mirror_serves(
        &self,
        owner: u16,
        target: &ReadinessTarget,
        primary: bool,
        now: u64,
    ) -> bool {
        let mut mirrors = self.deferred_polls.mirrors.borrow_mut();
        let Some(mirror) = mirrors.entries.get_mut(&target.namespace) else {
            return false;
        };
        mirror.used = now;
        mirror.owner == owner
            && mirror
                .state
                .as_ref()
                .is_some_and(|state| matches!(state.check(primary, target), Ok(true)))
    }

    /// Mirrors `namespaces` under `seq`, after `owner` took the frame that
    /// watches them. A mirror keeps its state until the owner pushes for the
    /// new sequence.
    pub(crate) fn mirror_partitions(
        &self,
        owner: u16,
        seq: u64,
        namespaces: &[IggyNamespace],
        now: u64,
    ) {
        let mut mirrors = self.deferred_polls.mirrors.borrow_mut();
        for namespace in namespaces {
            let mirror = mirrors.entries.entry(*namespace).or_insert(Mirror {
                owner,
                seq,
                state: None,
                used: now,
                pushed: now,
            });
            mirror.owner = owner;
            mirror.seq = seq;
            mirror.used = now;
        }
    }
}

#[cfg(test)]
#[path = "deferred_tests.rs"]
mod tests;
