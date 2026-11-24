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

use crate::commit_batch;
use crate::metrics::{frame_drop_reason, frame_drop_variant};
use crate::shards_table::ShardsTable;
use crate::{
    IggyShard, LifecycleFrame, PARTITION_WALKS_PER_TICK_MAX, Receiver, RestorableMetadataStm,
    ShardFrame,
};
use consensus::{MetadataHandle, PartitionsHandle};
use crossfire::TrySendError;
use futures::FutureExt;
use futures::future::poll_fn;
use iggy_binary_protocol::{
    Command, CommitEntry, CommitHeader, CommitReplyEntry, CommitReplyHeader, ConsensusError,
    GenericHeader, Operation, PrepareHeader,
};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use message_bus::{ConnectionInstaller, MessageBus, ReplicaHandshakeDoneFn, ReplicaLinkId};
use partitions::{FatalCommit, PartitionIncarnation};
use server_common::sharding::{IggyNamespace, METADATA_GROUP};
use server_common::{Message, MessageBag};
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::Poll;

/// How often the shard pump drives `VsrConsensus::tick`.
///
/// Heartbeats, prepare retransmit, and view-change timeouts only advance
/// when the tick runs. Re-exported from `consensus` so the drive cadence and
/// the tick counts it feeds share one unit; public so the simulator can
/// advance its virtual clock in whole tick intervals.
pub use consensus::TICK_INTERVAL as CONSENSUS_TICK_INTERVAL;

pub const COOPERATIVE_EVENT_BUDGET: usize = 64;

/// How many kept metadata frames the pump hands back per main-lane frame.
/// More than one: a metadata frame that arrives behind kept ones is kept too,
/// so one per frame never shortens the queue under a steady stream of them,
/// and the repair of a gap waits behind it.
const GATED_METADATA_FRAMES_PER_MAIN_FRAME: usize = 2;

#[derive(Default)]
pub struct LoopbackRound {
    pub(super) entries: VecDeque<(Option<PartitionIncarnation>, Message<GenericHeader>)>,
    pub(super) scratch: Vec<Message<GenericHeader>>,
}

impl<B: MessageBus, MJ, S, M, T, SB> IggyShard<B, MJ, S, M, T, SB> {
    #[allow(clippy::future_not_send)]
    pub(crate) async fn cooperate(&self) {
        let serviced = self.cooperative_events.get() + 1;
        if serviced < COOPERATIVE_EVENT_BUDGET {
            self.cooperative_events.set(serviced);
            return;
        }
        self.cooperative_events.set(0);
        let mut yielded = false;
        poll_fn(|context| {
            if yielded {
                Poll::Ready(())
            } else {
                yielded = true;
                context.waker().wake_by_ref();
                Poll::Pending
            }
        })
        .await;
    }
}

/// Inter-shard dispatch logic.
///
/// All messages — whether destined for a local or remote shard — are routed
/// through the channel into the target shard's message pump.  This ensures
/// that every mutation on a shard is serialized through a single point (the
/// pump), preventing concurrent access from independent async tasks.
impl<B, MJ, S, M, T, SB> IggyShard<B, MJ, S, M, T, SB>
where
    B: MessageBus + ConnectionInstaller + Clone + 'static,
    T: ShardsTable,
    SB: SuperblockStore,
{
    /// Network-receive entry point. Classifies the raw
    /// `Message<GenericHeader>` and routes it to the owning shard via
    /// `route_typed`.
    ///
    /// The only classify a frame gets: the bag rides
    /// [`ShardFrame::Consensus`] to the owning shard, whose pump matches it
    /// directly. Reading routing off it and then handing the bytes on generic
    /// made every frame pay `bytemuck::checked::try_from_bytes` plus the
    /// header's `validate()` twice.
    pub fn dispatch(&self, message: Message<GenericHeader>) {
        let command = message.header().command;
        let bag = match MessageBag::try_from(message) {
            Ok(bag) => bag,
            Err(ConsensusError::UnsupportedOperation { operation }) => {
                // For a replication frame this is terminal for its consensus
                // group, not a per-frame hiccup: the op is never journaled,
                // never acked, and every later prepare dies on the resulting
                // gap while quorum hides the outage. Repair wraps the same
                // typed header, so it cannot rescue this node either -- only
                // aligning the releases can. A routed request, by contrast, is dropped
                // before journaling and stalls nothing; the consequence in the
                // log follows the frame. Nothing fences the sending peer, so
                // the log and the counter are the whole signal an operator
                // gets.
                self.metrics.record_frame_drop(
                    frame_drop_variant::CONSENSUS,
                    frame_drop_reason::UNSUPPORTED_OPERATION,
                );
                let consequence = match command {
                    Command::Prepare | Command::RepairPrepare | Command::PrepareOk => {
                        "this node cannot journal or ack it, so its consensus group stops \
                         making progress until the releases are aligned"
                    }
                    _ => "the frame is dropped before journaling, with no reply to the sender",
                };
                tracing::error!(
                    shard = self.id,
                    operation = format_args!("{operation:#04x}"),
                    command = ?command,
                    build_release = %iggy_binary_protocol::ProtocolVersion(
                        iggy_binary_protocol::IGGY_PROTOCOL_VERSION
                    ),
                    "frame carries an operation this build does not know; the sender likely \
                     runs another release. {consequence}"
                );
                return;
            }
            Err(e) => {
                self.metrics.record_frame_drop(
                    frame_drop_variant::CONSENSUS,
                    frame_drop_reason::UNPARSABLE,
                );
                tracing::warn!(shard = self.id, error = %e, "dropping unparsable consensus frame");
                return;
            }
        };
        let (operation, namespace) = bag.routing();
        match bag {
            MessageBag::Commit(message) => self.route_commit_batch(message, operation),
            MessageBag::CommitReply(message) => self.route_commit_reply_batch(message, operation),
            bag => self.route_typed(operation, namespace, bag),
        }
    }

    /// Send each entry of a Commit batch to the shard that owns its group. A
    /// body that does not match its checksum drops the whole frame.
    fn route_commit_batch(&self, message: Message<CommitHeader>, operation: Operation) {
        let header = *message.header();
        let Some(entries) =
            commit_batch::verified_entries::<_, CommitEntry>(&message, header.checksum_body)
        else {
            self.drop_corrupt_batch(header.command);
            return;
        };
        self.metrics.record_commit_batch_received(entries.len());
        if let Some(target) = self.single_target(&entries, |entry| entry.group) {
            self.try_send_to_target(target, MessageBag::Commit(message), operation);
            return;
        }
        for (target, entries) in self.entries_by_shard(entries, |entry| entry.group) {
            if let Some(part) =
                commit_batch::encode_commit_batch(header.cluster, header.replica, &entries)
            {
                self.try_send_to_target(target, MessageBag::Commit(part), operation);
            }
        }
    }

    /// Send each entry of a `CommitReply` batch to the shard that owns its
    /// group. A body that does not match its checksum drops the whole frame.
    fn route_commit_reply_batch(&self, message: Message<CommitReplyHeader>, operation: Operation) {
        let header = *message.header();
        let Some(entries) =
            commit_batch::verified_entries::<_, CommitReplyEntry>(&message, header.checksum_body)
        else {
            self.drop_corrupt_batch(header.command);
            return;
        };
        self.metrics
            .record_commit_reply_batch_received(entries.len());
        if let Some(target) = self.single_target(&entries, |entry| entry.group) {
            self.try_send_to_target(target, MessageBag::CommitReply(message), operation);
            return;
        }
        for (target, entries) in self.entries_by_shard(entries, |entry| entry.group) {
            if let Some(part) =
                commit_batch::encode_commit_reply_batch(header.cluster, header.replica, &entries)
            {
                self.try_send_to_target(target, MessageBag::CommitReply(part), operation);
            }
        }
    }

    /// The shard that owns every group of `entries`, or `None` when they span
    /// shards. Nodes with equal shard counts and partition placement hash a
    /// group to the same shard, so a batch between them has one owner.
    fn single_target<E>(&self, entries: &[E], group: impl Fn(&E) -> u64) -> Option<u16> {
        let (first, rest) = entries.split_first()?;
        let target = self.group_shard(group(first));
        rest.iter()
            .all(|entry| self.group_shard(group(entry)) == target)
            .then_some(target)
    }

    fn entries_by_shard<E>(
        &self,
        entries: Vec<E>,
        group: impl Fn(&E) -> u64,
    ) -> BTreeMap<u16, Vec<E>> {
        let mut targets: BTreeMap<u16, Vec<E>> = BTreeMap::new();
        for entry in entries {
            targets
                .entry(self.group_shard(group(&entry)))
                .or_default()
                .push(entry);
        }
        targets
    }

    fn group_shard(&self, group: u64) -> u16 {
        if group == METADATA_GROUP {
            0
        } else {
            self.partition_shards.shard_for_consensus_ns(group)
        }
    }

    fn drop_corrupt_batch(&self, command: Command) {
        self.metrics
            .record_frame_drop(frame_drop_variant::CONSENSUS, frame_drop_reason::UNPARSABLE);
        tracing::warn!(
            shard = self.id,
            ?command,
            "dropping a batch frame whose body does not match its checksum"
        );
    }

    /// Invoke the client-request handler directly, exactly as the client-fd
    /// listener does in production once a connection delivers request bytes.
    /// The simulator's dispatch shell uses this to drive `SimClient` requests
    /// through the real `on_client_request` path (auth, session binding,
    /// consensus submit, reply) instead of the raw `dispatch` routing the
    /// shell-off fast path takes. No production caller: a `-p iggy-server`
    /// build excludes the `simulator` feature and this method.
    #[cfg(any(test, feature = "simulator"))]
    pub fn deliver_client_request(&self, client_id: u128, message: Message<GenericHeader>) {
        self.host.on_client_request(client_id, message);
    }

    /// Route a consensus-control message (`StartViewChange`, `DoViewChange`,
    /// `StartView`, `Commit`) by hashing its raw `u64` consensus namespace
    /// with this node's partition placement, the one the group's partition
    /// lives under.
    pub(crate) fn route_consensus_control(
        &self,
        message: MessageBag,
        namespace_u64: u64,
        operation: Operation,
    ) {
        let target = self.partition_shards.shard_for_consensus_ns(namespace_u64);
        self.try_send_to_target(target, message, operation);
    }

    /// Branch on operation class to pick the right routing rule. Used by
    /// [`Self::dispatch`].
    ///
    /// Three classes:
    /// 1. `is_metadata`: metadata data-plane operation, owned by shard 0.
    /// 2. `is_partition`: partition data-plane operation, owned by the
    ///    shard that the `IggyNamespace -> shard_id` table assigns.
    /// 3. `is_vsr_reserved` (`Reserved` / `Register`): VSR control-plane
    ///    frame (`StartViewChange`, `DoViewChange`, `StartView`, `Commit`)
    ///    or a client `Register` request. The owning consensus group is
    ///    identified by `namespace_u64`:
    ///    - `METADATA_GROUP` -> shard 0.
    ///    - packable `IggyNamespace::inner()` -> the shard owning that
    ///      partition's consensus group.
    fn route_typed(&self, operation: Operation, namespace_u64: u64, generic: MessageBag) {
        if operation.is_metadata() {
            self.try_send_to_target(0, generic, operation);
            return;
        }
        if operation.is_partition() {
            let partition_namespace = IggyNamespace::from_raw(namespace_u64);
            // The shards table is a cache of the deterministic hash
            // assignment (every `InsertOwned`/`InsertRouted` row stores
            // `PartitionShards::shard_for`'s result), seeded asynchronously
            // by each shard's reconciler. A miss therefore means "not seeded
            // yet", not "unroutable": fall back to the hash so frames arriving
            // during the post-commit convergence window still reach the shard
            // that will own the partition. That shard is where earliness is
            // resolved -- it parks the frame until its partition materialises
            // (`park_if_unmaterialised`) and fences a mismatched incarnation
            // (`serves_committed_incarnation`) -- so neither a hit nor a miss
            // here carries any claim about readiness.
            let target = self
                .shards_table
                .shard_for(partition_namespace)
                .unwrap_or_else(|| {
                    tracing::debug!(
                        shard = self.id,
                        stream = partition_namespace.stream_id(),
                        topic = partition_namespace.topic_id(),
                        partition = partition_namespace.partition_id(),
                        "namespace not in shards_table; routing by hash assignment"
                    );
                    self.partition_shards.shard_for(&partition_namespace)
                });
            self.try_send_to_target(target, generic, operation);
            return;
        }
        debug_assert!(
            operation.is_vsr_reserved(),
            "route_typed: operation {operation:?} fell through unclassified; \
             expected is_metadata / is_partition / is_vsr_reserved"
        );
        if namespace_u64 == METADATA_GROUP {
            self.try_send_to_target(0, generic, operation);
            return;
        }
        self.route_consensus_control(generic, namespace_u64, operation);
    }

    /// Send `message` into `senders[target]`. Honors the `io_uring` reactor
    /// constraint: never blocks; drops on `Full` / `Disconnected` and
    /// records the drop in `frame_drops_total`, under `variant=partition` for a
    /// partition-plane operation and `variant=consensus` otherwise -- the two
    /// have different recovery stories, so folding them into one label hides
    /// which one is bleeding. VSR retransmit recovers consensus drops, except
    /// the four register/logout forwarding frames, which no retransmit covers: a
    /// dropped forward or its result surfaces as the origin's forward timeout
    /// plus the SDK's session-operation replay. A
    /// `target` past the end of `senders` (a stored `u16` from `shard_for`, not
    /// a trusted index) is dropped with `reason=unroutable` rather than
    /// panicking. Metadata frames always pass `target = 0` here, since
    /// `is_metadata` operations are owned by shard 0.
    fn try_send_to_target(&self, target: u16, message: MessageBag, operation: Operation) {
        let variant = if operation.is_partition() {
            frame_drop_variant::PARTITION
        } else {
            frame_drop_variant::CONSENSUS
        };
        let Some(sender) = self.senders.get(target as usize) else {
            self.metrics
                .record_frame_drop(variant, frame_drop_reason::UNROUTABLE);
            tracing::error!(
                shard = self.id,
                target,
                ?operation,
                "dispatch: target shard id out of range, message dropped"
            );
            return;
        };
        match sender.try_send(ShardFrame::consensus(target, message)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                self.metrics
                    .record_frame_drop(variant, frame_drop_reason::FULL);
                tracing::warn!(
                    shard = self.id,
                    target,
                    ?operation,
                    "dispatch: shard inbox full, message dropped"
                );
            }
            Err(TrySendError::Disconnected(_)) => {
                self.metrics
                    .record_frame_drop(variant, frame_drop_reason::DISCONNECTED);
                tracing::warn!(
                    shard = self.id,
                    target,
                    ?operation,
                    "dispatch: shard inbox closed, message dropped"
                );
            }
        }
    }

    /// Drain this shard's inbox and process each frame locally until the
    /// `stop` signal fires or the inbox disconnects, then drain any frames
    /// still queued so in-flight requests still get a response.
    ///
    /// Returns the commit fault that ended the pump, if one did. A fault
    /// skips that queued drain: the frames in it are requests for a shard
    /// holding a divergent partition, and answering them means re-entering
    /// the commit path that just failed. The final flush still runs, so
    /// every partition that CAN still reach disk does.
    ///
    /// `shutdown_flag` is the cross-thread server shutdown signal. The pump
    /// flips it as soon as a commit fault is resolved, BEFORE the final
    /// flush: the flush writes to the device whose failure raised the fault,
    /// so it can stall indefinitely, and only the flag arms the watchdog and
    /// the bounded pump drain that turn a stalled flush into a timed-out
    /// non-zero exit instead of a process that reports healthy forever.
    #[allow(clippy::future_not_send)]
    #[allow(
        clippy::too_many_lines,
        reason = "Keep the owner select loop and its shutdown sequence together"
    )]
    pub async fn run_message_pump(
        &self,
        stop: Receiver<()>,
        shutdown_flag: Arc<AtomicBool>,
    ) -> Option<FatalCommit>
    where
        B: MessageBus + 'static,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
        SB: 'static,
    {
        let _completion_pump = self.poll_completions.pump_guard();
        let _deferred_pump = self.deferred_polls.pump_guard(self.id, &self.senders);
        self.plane.partitions().set_io_notifier(
            self.partition_io.notifier(),
            self.partition_io.limits.bytes_max(),
        );
        if let Some(consensus) = self.plane.metadata().consensus.as_ref() {
            consensus.set_loopback_notifier(Some(self.plane.partitions().loopback_wake_notifier()));
        }
        if let Some(sender) = self.senders.get(self.id as usize).cloned() {
            let metrics = self.metrics.clone();
            self.plane
                .partitions()
                .set_persistence_notifier(Rc::new(move |completion| {
                    let frame = LifecycleFrame::PartitionPersistenceCompleted(completion);
                    if let Err(error) = sender.try_send(ShardFrame::lifecycle(frame)) {
                        metrics.record_frame_drop(
                            frame_drop_variant::PARTITION_PERSISTENCE_COMPLETED,
                            crate::coordinator::classify_try_send_err(&error),
                        );
                    }
                }));
        }
        // Reused across every pump iteration; pre-size to skip the
        // first-drain reallocation.
        let mut loopback_buf = LoopbackRound::default();
        let mut namespace_scratch: Vec<IggyNamespace> = Vec::with_capacity(64);
        // Consensus timer driver, folded into the pump: running the tick as a
        // select! arm (not a sibling task) serializes it with frame processing,
        // so `tick_partitions` can no longer hold a partition reference across
        // an `.await` while `apply_reconcile_ops` removes that partition on
        // this same task. The timer is created once and pinned, then
        // re-armed only after it fires, so a busy inbox cannot drop-and-reset
        // it (which would stall heartbeats / prepare retransmit).
        // Single source for the timer, so the re-arm below cannot drift from the
        // initial interval.
        // The tick timer comes from the bus so the environment decides the
        // clock: compio wall time in production, virtual time in the
        // simulator (see `MessageBus::sleep`).
        let rearm_tick = || self.bus.sleep(CONSENSUS_TICK_INTERVAL).fuse();
        let mut consensus_tick = std::pin::pin!(rearm_tick());
        // When the armed tick is due, on the timer's own clock. How far past it
        // the tick arm runs is the time the thread spent on other work, such as
        // a long frame or a tier job that did not yield.
        let tick_interval_micros =
            u64::try_from(CONSENSUS_TICK_INTERVAL.as_micros()).unwrap_or(u64::MAX);
        let tick_due_from_now = || {
            self.bus
                .monotonic_micros()
                .saturating_add(tick_interval_micros)
        };
        let mut tick_due = tick_due_from_now();
        // Keep the receive registered: recreating it repeats Crossfire's initial
        // backoff on every pump turn while the shutdown channel is empty.
        let mut stop_signal = std::pin::pin!(stop.recv().fuse());
        let mut fatal: Option<FatalCommit> = None;
        let deadline_timer = |deadline: Option<u64>| {
            async move {
                if let Some(deadline) = deadline {
                    self.bus
                        .sleep(std::time::Duration::from_micros(
                            deadline.saturating_sub(self.bus.monotonic_micros()),
                        ))
                        .await;
                } else {
                    std::future::pending::<()>().await;
                }
            }
            .fuse()
        };
        let mut armed_deadline = None;
        let mut deferred_deadline = std::pin::pin!(deadline_timer(None));
        // The uploads of fresh objects run on deadlines of their own, far
        // shorter than a tick: producers wait for the open window to close.
        let mut armed_fresh_deadline = None;
        let mut fresh_deadline = std::pin::pin!(deadline_timer(None));
        loop {
            self.service_deferred_polls().await;
            // Here and not at the end of the turn: an idle owner would hold
            // the pushes of this turn until its next event or tick.
            self.flush_readiness_pushes();
            let next_deadline = self.deferred_polls.earliest_deadline();
            if next_deadline != armed_deadline {
                armed_deadline = next_deadline;
                deferred_deadline.set(deadline_timer(next_deadline));
            }
            let next_fresh_deadline = self.fresh_uploads_deadline();
            if next_fresh_deadline != armed_fresh_deadline {
                armed_fresh_deadline = next_fresh_deadline;
                fresh_deadline.set(deadline_timer(next_fresh_deadline));
            }
            // `select_biased!`, not `select!`: the unbiased macro draws its
            // arm order from a process-random thread-local PRNG, which the
            // deterministic simulator cannot seed. The listed order is the
            // intended priority anyway: stop, then tick, redispatch, then
            // newly received frames.
            futures::select_biased! {
                _ = stop_signal.as_mut() => break,
                () = consensus_tick.as_mut() => {
                    self.metrics.record_pump_tick_lateness(std::time::Duration::from_micros(
                        self.bus.monotonic_micros().saturating_sub(tick_due),
                    ));
                    // Sharing the pump task keeps `tick_partitions` borrows
                    // safe, but a tick can wait for a main frame's processing
                    // (replication, journal fsync or rotation, and reply), plus
                    // one reply and one poll completion, including its loopback.
                    // TODO(hubcio): if a load test shows tick starvation,
                    // make `tick_partitions` borrow-free so the tick can be
                    // decoupled from the pump again without reintroducing the
                    // partition-ref-across-`.await` UB this fold closed.
                    self.tick_metadata().await;
                    self.tick_primary_handoff();
                    if let Some(fault) = self.tick_partitions(&mut namespace_scratch).await {
                        fatal = Some(fault);
                        break;
                    }
                    self.tick_fresh_writes().await;
                    self.tick_fresh_sweeps();
                    self.tick_geo_positions();
                    self.tick_long_term_rewrites().await;
                    self.tick_long_term_merges().await;
                    // Runs here, not inside `tick_metadata`: that early-returns
                    // on shards without metadata consensus, and partition-plane
                    // offers live on every shard that hosts a serving group --
                    // parked behind the shard-0 gate they would never expire.
                    self.expire_idle_state_transfer_offers();
                    self.serve_parked_partition_transfer_requests().await;
                    // A drain completes without an event, so a pending
                    // revocation wakes the reconciler each tick. A published
                    // fence wakes it once, and retries of an unpublished one
                    // stay on its periodic pass.
                    let fence_published = self.plane.partitions().take_fence_published();
                    let streams = self.plane.metadata().mux_stm.streams();
                    if fence_published || streams.has_pending_revocations() {
                        self.dispatch_metadata_commit_tick();
                    }
                    // A dropped `ReconcileApply` marker (full inbox at
                    // `enqueue_reconcile_op`) otherwise strands staged ops on
                    // a quiet shard until the next inbound frame's tail
                    // drain; parked partition frames then never re-dispatch.
                    self.apply_reconcile_ops();
                    // After the apply: a build that lands on this tick releases
                    // the held reads whose hold has not ended.
                    self.expire_held_reads();
                    self.tick_deferred_polls();
                    self.sweep_readiness_watches();
                    consensus_tick.set(rearm_tick());
                    tick_due = tick_due_from_now();
                    self.process_loopback(&mut loopback_buf).await;
                }
                () = deferred_deadline.as_mut() => {
                    armed_deadline = None;
                    deferred_deadline.set(deadline_timer(None));
                }
                () = fresh_deadline.as_mut() => {
                    armed_fresh_deadline = None;
                    fresh_deadline.set(deadline_timer(None));
                    self.drive_fresh_uploads();
                }
                frame = poll_fn(|_| {
                    self.pop_redispatched_frame().map_or(Poll::Pending, Poll::Ready)
                }).fuse() => {
                    // One frame per select iteration. Ranking this arm above
                    // the inbox preserves park order without making a full
                    // queue stall ticks and commit broadcasts for other groups.
                    self.dispatch_redispatched_frame(frame).await;
                    self.cooperate().await;
                    // A request handled by a solo primary self-acks here. If
                    // loopback waited for another inbox frame, the request
                    // would remain uncommitted indefinitely on a quiet shard.
                    self.process_loopback(&mut loopback_buf).await;
                    self.apply_reconcile_ops();
                    // Same guaranteed reply-lane service as the inbox arm: this
                    // arm outranks both lanes, so a deep drain would otherwise
                    // starve forwarded client replies for its whole duration.
                    if let Ok(reply) = self.reply_inbox.try_recv()
                        && self.accept_frame_for_self(&reply)
                    {
                        self.process_frame(reply).await;
                    }
                    self.process_one_poll_completion(&mut loopback_buf).await;
                }
                frame = self.inbox.recv().fuse() => {
                    match frame {
                        Ok(frame) => {
                            if self.accept_frame_for_self(&frame) {
                                self.process_frame(frame).await;
                                self.process_loopback(&mut loopback_buf).await;
                                // Tail drain catches reconcile ops whose marker was dropped.
                                // Anything it stages is served by the arm above
                                // on the next pass, before this arm can run again.
                                self.apply_reconcile_ops();
                            }
                            // Guaranteed reply-lane service: `select_biased!`
                            // polls the main lane first, so a saturated main
                            // lane would otherwise starve the reply arm below
                            // indefinitely. Taking at most ONE reply per main
                            // frame makes the worst case a deterministic 1:1
                            // interleave - replies keep flowing under a
                            // consensus storm, and consensus keeps flowing
                            // under a reply flood.
                            if let Ok(reply) = self.reply_inbox.try_recv()
                                && self.accept_frame_for_self(&reply)
                            {
                                self.process_frame(reply).await;
                            }
                            self.process_one_poll_completion(&mut loopback_buf).await;
                            // A bounded turn per main frame, as the reply lane
                            // above: the arm below never runs while the main
                            // lane is busy.
                            for _ in 0..GATED_METADATA_FRAMES_PER_MAIN_FRAME {
                                let Some(frame) = self.pop_gated_metadata_frame() else {
                                    break;
                                };
                                self.replay_gated_metadata_frame(frame, &mut loopback_buf).await;
                            }
                        }
                        Err(_) => break,
                    }
                }
                frame = poll_fn(|_| {
                    self.pop_gated_metadata_frame().map_or(Poll::Pending, Poll::Ready)
                }).fuse() => {
                    // Below the main lane, not with the redispatch arm above
                    // it: a trim can leave a deep queue, and the frames that
                    // arrive meanwhile, such as the replies to this replica's
                    // heartbeats, must not wait for all of it.
                    self.replay_gated_metadata_frame(frame, &mut loopback_buf).await;
                    // As the redispatch arm does: this arm outranks the reply
                    // lane and the poll completions too.
                    if let Ok(reply) = self.reply_inbox.try_recv()
                        && self.accept_frame_for_self(&reply)
                    {
                        self.process_frame(reply).await;
                    }
                    self.process_one_poll_completion(&mut loopback_buf).await;
                }
                frame = self.reply_inbox.recv().fuse() => {
                    // Reached only while the main lane is quiet (biased order):
                    // serve forwarded client replies without waiting for the
                    // next main frame or tick.
                    match frame {
                        Ok(frame) => {
                            if self.accept_frame_for_self(&frame) {
                                self.process_frame(frame).await;
                            }
                            self.process_one_poll_completion(&mut loopback_buf).await;
                        }
                        Err(_) => break,
                    }
                }
                completion = self.partition_io.recv().fuse() => {
                    self.accept_partition_io_completion(completion);
                    self.cooperate().await;
                }
                completion = self.poll_completions.recv().fuse() => {
                    match completion {
                        Ok(completion) => {
                            self.on_poll_completed(*completion).await;
                            self.cooperate().await;
                            self.process_loopback(&mut loopback_buf).await;
                            self.apply_reconcile_ops();
                        }
                        Err(_) => break,
                    }
                }
                () = poll_fn(|context| self.deferred_polls.poll_ready(context)).fuse() => {}
                () = poll_fn(|context| {
                    self.plane.partitions().register_loopback_waker(context.waker());
                    self.partition_io.register_waker(context.waker());
                    self.served_reads.register_waker(context.waker());
                    if self.has_pending_loopback(&loopback_buf)
                        || self.partition_io.has_ready()
                        || self.served_reads.has_landed()
                    {
                        Poll::Ready(())
                    } else {
                        Poll::Pending
                    }
                }).fuse() => {
                    self.process_loopback(&mut loopback_buf).await;
                    self.apply_reconcile_ops();
                }
            }
            self.service_partition_io().await;
            // After every event, as partition I/O, so a busy inbox cannot
            // starve the serves that wait for a disk read.
            self.serve_landed_read().await;
            // After every event too: a commit frees the room that the sends
            // of a stored fresh object wait for, and it can come in any arm.
            self.feed_fresh_sends_awaiting_room().await;
        }

        self.poll_completions.close();
        self.deferred_polls.close(self.id, &self.senders);

        // A stop can win the select immediately after a frame fenced a
        // partition, before the next tick observes it. Preserve that fault so
        // shutdown cannot turn a durability failure into a clean pump exit.
        if fatal.is_none() {
            fatal = self.first_partition_commit_fault();
        }

        // Drain remaining frames so in-flight requests get a response, and
        // the reply lane so already-forwarded replies still reach their
        // clients before the bus tears down. Skipped on a commit fault: a
        // queued Ack or Commit frame for the fenced partition would re-enter
        // the commit path that just failed, and `advance_commit_min` asserts
        // on the gap the fault left. Those requests go unanswered and their
        // clients time out, which is what a node stopping on a durability
        // fault owes them.
        self.shutting_down.set(true);
        self.reject_held_reads();
        let stop_began_micros = self.bus.monotonic_micros();
        if fatal.is_none() {
            fatal = self
                .drain_queued_frames_for_shutdown(&mut loopback_buf)
                .await;
        }

        if fatal.is_some() {
            // Flipped BEFORE the final flush, not after the pump returns: the
            // flush writes to the device whose failure raised the fault and
            // can stall there indefinitely, and the flag is what arms the
            // watchdog and the bounded pump drain. Siblings give up at most
            // the flush window of extra serving.
            shutdown_flag.store(true, Ordering::Relaxed);
        }

        // Final flush: committed messages still resident in the in-memory
        // journal must reach segment storage before the process exits, or a
        // graceful restart recovers consumer offsets ahead of the data. On a
        // fault, healthy partitions still flush. A fenced partition skips the
        // flush, leaving any resident committed data unpersisted at shutdown.
        self.flush_partitions(&mut loopback_buf, stop_began_micros)
            .await;
        self.partition_io.close();

        // A failed flush fences its partition: the data it could not write is
        // cluster-committed and now lives only in this process's memory, so a
        // clean exit here would report a durability loss as a good shutdown.
        if fatal.is_none() {
            fatal = self.first_partition_commit_fault();
        }

        fatal
    }

    /// Drives every partition through its graceful stop. The partitions that
    /// the stop can checkpoint hold before they drain their writers, until the
    /// checkpoint starts or the stop checkpoint window, counted from
    /// `stop_began_micros` on the bus clock, ends.
    #[allow(clippy::future_not_send)]
    async fn flush_partitions(&self, loopback: &mut LoopbackRound, stop_began_micros: u64)
    where
        B: MessageBus + 'static,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
        SB: 'static,
    {
        self.shutting_down.set(true);
        let partitions = self.plane.partitions();
        let window = self.stop_checkpoint_window.get();
        let window_ticks = window.as_micros() / CONSENSUS_TICK_INTERVAL.as_micros().max(1);
        // The free start slots of every tick until the deadline. Checkpoint
        // starts share them with other walks, so the stop can reach fewer.
        let reach = usize::try_from(window_ticks)
            .unwrap_or(usize::MAX)
            .saturating_mul(PARTITION_WALKS_PER_TICK_MAX);
        self.metrics
            .record_stop_checkpoints_missed(partitions.plan_stop_checkpoints(reach));
        let hold_deadline_micros =
            stop_began_micros.saturating_add(u64::try_from(window.as_micros()).unwrap_or(u64::MAX));
        let mut holding = true;
        let namespaces: Vec<_> = partitions.namespaces().copied().collect();
        for namespace in &namespaces {
            if let Some(partition) = partitions.get_io_owner(namespace) {
                partition.begin_shutdown_io();
            }
        }
        let rearm = || self.bus.sleep(CONSENSUS_TICK_INTERVAL).fuse();
        let mut tick = std::pin::pin!(rearm());
        let mut namespace_scratch = Vec::with_capacity(namespaces.len());
        let mut inbox = std::pin::pin!(self.inbox.recv().fuse());
        let mut replies = std::pin::pin!(self.reply_inbox.recv().fuse());
        loop {
            self.process_one_poll_completion(loopback).await;
            self.service_partition_io().await;
            self.process_loopback(loopback).await;
            let finished = !self.partition_io.has_ready()
                && self.partition_io.outstanding() == 0
                && namespaces.iter().all(|namespace| {
                    partitions
                        .get_io_owner(namespace)
                        .is_none_or(|partition| partition.shutdown_io_complete())
                });
            if finished {
                break;
            }
            futures::select_biased! {
                () = tick.as_mut() => {
                    for namespace in &namespaces {
                        if let Some(partition) = partitions.get_io_owner(namespace) {
                            partition.drive_persistence().await;
                            if partition.needs_io_retry() {
                                self.partition_io.retry(*namespace, partition.incarnation());
                            }
                        }
                    }
                    self.tick_metadata().await;
                    let _ = self.tick_partitions(&mut namespace_scratch).await;
                    self.tick_fresh_writes().await;
                    self.tick_long_term_rewrites().await;
                    self.tick_long_term_merges().await;
                    // After the tick: the first tick fires one interval after
                    // the stop began, so the tick at the deadline must still
                    // start its share of the reach.
                    if holding && self.bus.monotonic_micros() >= hold_deadline_micros {
                        holding = false;
                        self.metrics.record_stop_checkpoints_missed(
                            partitions.release_stop_checkpoints(),
                        );
                    }
                    tick.set(rearm());
                }
                completion = self.partition_io.recv().fuse() => {
                    self.accept_partition_io_completion(completion);
                    self.cooperate().await;
                }
                frame = inbox.as_mut() => {
                    if let Ok(frame) = frame {
                        if self.accept_frame_for_self(&frame) {
                            self.process_frame(frame).await;
                        }
                        inbox.set(self.inbox.recv().fuse());
                    }
                }
                frame = replies.as_mut() => {
                    if let Ok(frame) = frame {
                        if self.accept_frame_for_self(&frame) {
                            self.process_frame(frame).await;
                        }
                        replies.set(self.reply_inbox.recv().fuse());
                    }
                }
                () = poll_fn(|context| {
                    self.partition_io.register_waker(context.waker());
                    if self.partition_io.has_ready() || self.has_pending_loopback(loopback) {
                        Poll::Ready(())
                    } else { Poll::Pending }
                }).fuse() => {}
            }
        }
    }

    /// A busy ordinary lane yields one completion per frame. Keeping this
    /// service bounded lets ordinary work progress under a completion flood.
    #[allow(clippy::future_not_send)]
    async fn process_one_poll_completion(&self, loopback_buf: &mut LoopbackRound)
    where
        B: MessageBus + 'static,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        if let Ok(completion) = self.poll_completions.try_recv() {
            self.on_poll_completed(*completion).await;
            self.cooperate().await;
            self.process_loopback(loopback_buf).await;
            self.apply_reconcile_ops();
        }
    }

    /// Hand the plane one metadata frame that waited for the end of a WAL
    /// trim, see `Self::defer_gated_metadata_frame`.
    #[allow(clippy::future_not_send)]
    async fn replay_gated_metadata_frame(&self, frame: MessageBag, loopback_buf: &mut LoopbackRound)
    where
        B: MessageBus + 'static,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        self.deliver_message(frame, None).await;
        self.cooperate().await;
        self.process_loopback(loopback_buf).await;
        self.apply_reconcile_ops();
    }

    /// Process queued work after the select loop has stopped. Redispatch keeps
    /// its live-pump rank over the inbox, and every delivered frame gets its
    /// loopback before another frame can run.
    #[allow(clippy::future_not_send)]
    async fn drain_queued_frames_for_shutdown(
        &self,
        loopback_buf: &mut LoopbackRound,
    ) -> Option<FatalCommit>
    where
        B: MessageBus + 'static,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        loop {
            while let Some(frame) = self.pop_redispatched_frame() {
                self.dispatch_redispatched_frame(frame).await;
                self.cooperate().await;
                self.process_loopback(loopback_buf).await;
                self.apply_reconcile_ops();
                if let Some(fault) = self.first_partition_commit_fault() {
                    return Some(fault);
                }
                self.process_one_poll_completion(loopback_buf).await;
                if let Some(fault) = self.first_partition_commit_fault() {
                    return Some(fault);
                }
            }
            let Ok(frame) = self.inbox.try_recv() else {
                if let Ok(completion) = self.poll_completions.try_recv() {
                    self.on_poll_completed(*completion).await;
                    self.cooperate().await;
                    self.process_loopback(loopback_buf).await;
                    self.apply_reconcile_ops();
                    if let Some(fault) = self.first_partition_commit_fault() {
                        return Some(fault);
                    }
                    // Completion processing can stage ordinary work. Return
                    // to the main drain before accepting another completion.
                    continue;
                }
                if self.has_pending_loopback(loopback_buf) {
                    self.process_loopback(loopback_buf).await;
                    self.apply_reconcile_ops();
                    if let Some(fault) = self.first_partition_commit_fault() {
                        return Some(fault);
                    }
                    continue;
                }
                break;
            };
            if self.accept_frame_for_self(&frame) {
                self.process_frame(frame).await;
                self.process_loopback(loopback_buf).await;
                self.apply_reconcile_ops();
                if let Some(fault) = self.first_partition_commit_fault() {
                    return Some(fault);
                }
            }
            self.process_one_poll_completion(loopback_buf).await;
            if let Some(fault) = self.first_partition_commit_fault() {
                return Some(fault);
            }
        }

        // Retired, not delivered: the pump is going away, so a staged frame has
        // no later arm to reach the plane through. This runs before the reply
        // lane drain, which is what carries client denies out.
        self.retire_redispatched_frames();
        while let Ok(frame) = self.reply_inbox.try_recv() {
            if self.accept_frame_for_self(&frame) {
                self.process_frame(frame).await;
                if let Some(fault) = self.first_partition_commit_fault() {
                    return Some(fault);
                }
            }
        }
        None
    }

    /// First partition commit fault currently fenced on this shard.
    ///
    /// The regular path observes faults in `tick_partitions`. This scan covers
    /// the pump-exit edges where a stop signal wins before that tick, or a
    /// queued frame fails while the pump is draining during shutdown.
    fn first_partition_commit_fault(&self) -> Option<FatalCommit> {
        self.plane.partitions().first_commit_fault()
    }

    /// `first_partition_commit_fault` for the simulator's lost-wakeup
    /// tripwire: a fenced pump and a missed wake both leave frames undrained, and
    /// only the second is a channel bug. Test/simulator only, like `inbox_len`.
    #[cfg(any(test, feature = "simulator"))]
    #[must_use]
    pub fn fenced_partition_fault(&self) -> Option<FatalCommit> {
        self.first_partition_commit_fault()
    }

    /// Sanity check at pump entry: every Consensus frame routed through
    /// [`Self::dispatch`] must land on the shard whose `id` matches the
    /// `target_shard` the sender stamped on the frame. The ctor
    /// `assert_sender_ordering` proves `senders[i].shard_id() == i`, but
    /// only checks the sender vec - it cannot catch a receiver moved to
    /// the wrong runtime. This check closes that gap. Non-Consensus
    /// frames are always accepted; a Consensus frame stamped for a
    /// *different* shard fires a `debug_assert_eq!` in test/debug builds
    /// and is logged + dropped in release. Returns `true` when the frame
    /// may be processed, `false` when it must be dropped.
    fn accept_frame_for_self(&self, frame: &ShardFrame) -> bool {
        let ShardFrame::Consensus { target_shard, .. } = frame else {
            return true;
        };
        if *target_shard == self.id {
            return true;
        }
        debug_assert_eq!(
            *target_shard, self.id,
            "shard {} pump received Consensus frame whose target_shard={target_shard}; \
             senders/runtime incorrectly bound",
            self.id
        );
        self.metrics
            .record_frame_drop(frame_drop_variant::CONSENSUS, frame_drop_reason::MISROUTED);
        tracing::error!(
            shard = self.id,
            target_shard,
            "Consensus frame routed to wrong shard; dropping frame to preserve safety"
        );
        false
    }

    #[allow(clippy::future_not_send)]
    async fn process_frame(&self, frame: ShardFrame)
    where
        B: MessageBus + 'static,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        match frame {
            ShardFrame::Consensus { message, .. } => {
                self.on_message(message).await;
            }
            ShardFrame::Lifecycle(payload) => self.process_lifecycle(payload).await,
        }
        self.cooperate().await;
    }

    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    async fn process_lifecycle(&self, payload: LifecycleFrame)
    where
        B: MessageBus + 'static,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target:
            Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
        M: RestorableMetadataStm,
    {
        match payload {
            LifecycleFrame::ReplicaInboundSetup {
                fd,
                slot,
                link,
                deadline,
            } => {
                tracing::info!(
                    shard = self.id,
                    slot,
                    replica_id = link.replica,
                    link = link.link,
                    epoch = link.epoch,
                    raw_fd = fd.as_raw_fd(),
                    "installing delegated inbound replica link fd"
                );
                let on_done =
                    self.replica_handshake_done_fn(ReplicaHandshakeOutcome::Inbound { slot, link });
                self.bus.install_replica_inbound_fd(
                    fd,
                    link,
                    deadline,
                    self.on_replica_message.clone(),
                    on_done,
                );
            }
            LifecycleFrame::ReplicaOutboundSetup { fd, link } => {
                tracing::info!(
                    shard = self.id,
                    replica_id = link.replica,
                    link = link.link,
                    epoch = link.epoch,
                    raw_fd = fd.as_raw_fd(),
                    "installing delegated outbound replica link fd"
                );
                let on_done =
                    self.replica_handshake_done_fn(ReplicaHandshakeOutcome::Outbound(link));
                self.bus.install_replica_outbound_fd(
                    fd,
                    link,
                    self.on_replica_message.clone(),
                    on_done,
                );
            }
            LifecycleFrame::ReplicaInboundHandshakeDone { slot, link } => {
                // Only shard 0 tracks in-flight handshake slots and
                // bundles; a non-zero owning shard's ack closure addresses
                // senders[0] (shard 0 itself acks inline, no frame).
                debug_assert_eq!(
                    self.id, 0,
                    "ReplicaInboundHandshakeDone routed to shard {}",
                    self.id
                );
                self.bus.replica_inbound_handshake_done(slot, link);
            }
            LifecycleFrame::ReplicaOutboundHandshakeDone { link } => {
                debug_assert_eq!(
                    self.id, 0,
                    "ReplicaOutboundHandshakeDone routed to shard {}",
                    self.id
                );
                self.bus.replica_outbound_handshake_done(link);
            }
            LifecycleFrame::ReplicaLinkClosed { link } => {
                debug_assert_eq!(self.id, 0, "ReplicaLinkClosed routed to shard {}", self.id);
                self.bus.replica_link_closed(link);
            }
            LifecycleFrame::ReplicaLinkClose { link } => {
                self.bus.close_replica_link(link);
            }
            LifecycleFrame::ClientConnectionSetup { fd, meta, permit } => {
                tracing::info!(
                    shard = self.id,
                    client_id = meta.client_id,
                    raw_fd = fd.as_raw_fd(),
                    "installing delegated client fd"
                );
                self.bus
                    .install_client_fd(fd, meta, permit, self.on_client_request.clone());
            }
            LifecycleFrame::ClientWsConnectionSetup { fd, meta, permit } => {
                tracing::info!(
                    shard = self.id,
                    client_id = meta.client_id,
                    raw_fd = fd.as_raw_fd(),
                    "installing delegated WS client fd (pre-upgrade)"
                );
                self.bus
                    .install_client_ws_fd(fd, meta, permit, self.on_client_request.clone());
            }
            LifecycleFrame::ClientTcpTlsConnectionSetup {
                fd,
                meta,
                config,
                permit,
            } => {
                tracing::info!(
                    shard = self.id,
                    client_id = meta.client_id,
                    raw_fd = fd.as_raw_fd(),
                    "installing delegated TCP-TLS client fd (pre-handshake)"
                );
                self.bus.install_client_tcp_tls_fd(
                    fd,
                    meta,
                    config,
                    permit,
                    self.on_client_request.clone(),
                );
            }
            LifecycleFrame::ClientWssConnectionSetup {
                fd,
                meta,
                config,
                permit,
            } => {
                tracing::info!(
                    shard = self.id,
                    client_id = meta.client_id,
                    raw_fd = fd.as_raw_fd(),
                    "installing delegated WSS client fd (pre-handshake)"
                );
                self.bus.install_client_wss_fd(
                    fd,
                    meta,
                    config,
                    permit,
                    self.on_client_request.clone(),
                );
            }
            LifecycleFrame::ForwardReplicaSend { replica_id, msg } => {
                if let Err(e) = self.bus.send_to_replica(replica_id, msg).await {
                    tracing::debug!(
                        shard = self.id,
                        replica_id,
                        error = ?e,
                        "forward-replica-send delivery failed"
                    );
                }
            }
            LifecycleFrame::ForwardClientSend { client_id, msg } => {
                if let Err(e) = self.bus.send_to_client(client_id, msg).await {
                    // The production bus refuses every frame sent by id, so
                    // this is the expected outcome there, not a fault.
                    self.metrics.record_frame_drop(
                        frame_drop_variant::FORWARD_CLIENT_SEND,
                        frame_drop_reason::DELIVERY_FAILED,
                    );
                    tracing::debug!(
                        shard = self.id,
                        client_id,
                        error = ?e,
                        "staged client deny has no route by client id"
                    );
                }
            }
            LifecycleFrame::MetadataSubmit(submit) => {
                // Only shard 0 owns the metadata consensus group, and
                // `forward_metadata_submit` always addresses shard 0, so a
                // non-zero shard here is a routing bug. The handler (wired
                // by the server) replies `None` on the carried sender if it
                // cannot submit, so the awaiting peer never blocks forever.
                debug_assert_eq!(
                    self.id, 0,
                    "MetadataSubmit must only be processed on shard 0"
                );
                self.host.on_metadata_submit(submit);
            }
            LifecycleFrame::ListClients { reply } => {
                // Every shard handles this (not shard-0-only): each replies
                // with the clients whose connections it homes. The handler
                // (wired by the server) reads this shard's `SessionManager`
                // and pushes the answer over `reply`.
                self.host.on_list_clients(reply);
            }
            LifecycleFrame::PartitionRead {
                namespace,
                read,
                reply,
                hold,
            } => {
                self.on_partition_read(namespace, read, reply, hold).await;
            }
            LifecycleFrame::ReadinessWait { wait, reply, watch } => {
                self.admit_readiness_wait(*wait, &reply, watch);
            }
            LifecycleFrame::ReadinessCancel { waiter } => {
                self.cancel_readiness_wait(waiter);
            }
            LifecycleFrame::ReadinessUpdate { from, states } => {
                self.apply_readiness_update(from, states);
            }
            LifecycleFrame::ReadinessUnwatch { from, watches } => {
                self.unwatch_readiness(from, watches);
            }
            LifecycleFrame::ReadinessInterest {
                from,
                watches,
                parked,
            } => {
                self.set_readiness_interest(from, watches, parked);
            }
            LifecycleFrame::PartitionSubmit {
                request,
                reply,
                attachment,
            } => {
                // Addressed to the shard owning the request's namespace (the
                // sender resolved it via the shards table, same fallback as
                // `route_typed`). Every refusal answers on `reply`, so the
                // awaiting shard never waits out its budget on a decision
                // already made.
                self.dispatch_partition_submit(request, reply, None, attachment)
                    .await;
            }
            LifecycleFrame::MetadataCommitTick => {
                // Reconciler may not yet be wired (e.g. mid-bootstrap, or
                // single-shard tests that never enable the reconciler loop).
                // Count the drop so operators can detect a stuck handler
                // slot; the reconciler also runs a periodic tick that
                // recovers any missed wake-up.
                if !self.dispatch_metadata_commit_tick() {
                    self.metrics.record_frame_drop(
                        frame_drop_variant::METADATA_COMMIT_TICK,
                        frame_drop_reason::UNROUTABLE,
                    );
                    tracing::trace!(
                        shard = self.id,
                        "metadata commit tick received before reconciler handler installed; dropping"
                    );
                }
            }
            LifecycleFrame::PartitionPlacementViews(reports, statuses) => {
                if self.id == 0 {
                    self.plane.metadata().observe_partition_views(&reports);
                    self.partition_status_outbox.record(&statuses);
                    self.record_partition_statuses(&statuses);
                } else {
                    self.metrics.record_frame_drop(
                        frame_drop_variant::PARTITION_PLACEMENT_VIEWS,
                        frame_drop_reason::MISROUTED,
                    );
                }
            }
            LifecycleFrame::PrimaryRebalance(frame) => {
                self.on_local_rebalance(*frame).await;
            }
            LifecycleFrame::PartitionPersistenceCompleted(completion) => {
                let namespace = IggyNamespace::from_raw(completion.group);
                let partitions = self.plane.partitions();
                if let Some(partition) = partitions.get_mut_by_ns(&namespace) {
                    partition
                        .on_persistence_completed(completion, partitions.config())
                        .await;
                }
            }
            LifecycleFrame::ReconcileApply => {
                self.apply_reconcile_ops();
            }
            LifecycleFrame::CleanPartition {
                namespace,
                now,
                message_expiry,
                max_bytes,
            } => {
                // Pump-side: the single writer of partition state. The timer
                // task already resolved the retention decision off-pump, so
                // this only mutates, serialized with reads on the same loop.
                if let Some(partition) = self.plane.partitions().get_mut_by_ns(&namespace) {
                    let removal = partition
                        .clean_expired_segments(now, message_expiry, max_bytes)
                        .await;
                    if removal.segments > 0 {
                        // Any unlink invalidates what this shard is SERVING:
                        // the offer names files that are gone and the payload
                        // cache can answer from RAM without touching disk, so a
                        // puller would install deleted messages. Neither cache
                        // can notice on its own -- one is keyed on the
                        // partition's commit_op, which retention does not move,
                        // the other on a checksum over the deleted bytes.
                        self.drop_partition_transfer_state(namespace, partition);
                        tracing::debug!(
                            shard = self.id,
                            namespace_raw = namespace.inner(),
                            segments = removal.segments,
                            messages = removal.messages,
                            "segment cleaner removed sealed segments"
                        );
                    }
                    if removal.budget_spent {
                        // The pass stopped on its per-frame budget with more to
                        // remove. Leaving the rest to the next interval tick
                        // caps reclaim at one budget per interval, which a
                        // producer outruns forever on small segments. Re-stage
                        // as a frame rather than looping, so the pump keeps
                        // interleaving consensus ticks between passes. This
                        // terminates: a pass re-arms only when MORE than a full
                        // budget was removable and it retires exactly the
                        // budget, so the backlog strictly shrinks; a pass held
                        // by the consumer barrier stops at or below the budget
                        // and does not re-arm at all.
                        self.request_clean_partition(namespace, now, message_expiry, max_bytes);
                    }
                }
            }
            LifecycleFrame::EvictPartition {
                namespace,
                now,
                envelope,
            } => {
                // The envelope stays on the partition, so a floor advance that
                // applies before the next tick evicts against it too.
                if let Some(partition) = self.plane.partitions().get_mut_by_ns(&namespace) {
                    partition.set_local_envelope(Some(envelope));
                    partition.stage_eviction(now);
                }
            }
            LifecycleFrame::RetainPartition {
                namespace,
                created_revision,
                now,
                message_expiry,
                max_bytes,
                committed_up_to,
            } => {
                self.on_retain_partition(
                    namespace,
                    created_revision,
                    now,
                    message_expiry,
                    max_bytes,
                    committed_up_to,
                );
            }
            LifecycleFrame::UploadSegments {
                namespace,
                created_revision,
                resend_advance,
                committed_up_to,
            } => {
                self.on_upload_segments(
                    namespace,
                    created_revision,
                    resend_advance,
                    committed_up_to,
                )
                .await;
            }
            LifecycleFrame::SegmentUploadFinished(finished) => {
                self.on_segment_upload_finished(*finished).await;
            }
            LifecycleFrame::DeleteObjects {
                namespace,
                created_revision,
                now,
                committed_up_to,
            } => {
                self.on_delete_objects(namespace, created_revision, now, committed_up_to);
            }
            LifecycleFrame::ObjectDeleteFinished(finished) => {
                self.on_object_delete_finished(*finished);
            }
            LifecycleFrame::PublishManifest {
                namespace,
                created_revision,
                now,
            } => {
                self.on_publish_manifest(namespace, created_revision, now);
            }
            LifecycleFrame::ManifestPublishFinished(finished) => {
                self.on_manifest_publish_finished(&finished);
            }
            LifecycleFrame::FreshUploadFinished(finished) => {
                self.on_fresh_upload_finished(*finished).await;
            }
            LifecycleFrame::RewriteSegments {
                namespace,
                created_revision,
            } => {
                self.on_rewrite_segments(namespace, created_revision).await;
            }
            LifecycleFrame::SegmentRewriteFinished(finished) => {
                self.on_segment_rewrite_finished(*finished).await;
            }
            LifecycleFrame::SweepFreshObjects { now } => {
                self.on_sweep_fresh_objects(now);
            }
            LifecycleFrame::FreshObjectsListed(finished) => {
                self.on_fresh_objects_listed(*finished);
            }
            LifecycleFrame::ReportFreshObjects { sweep } => {
                self.on_report_fresh_objects(sweep);
            }
            LifecycleFrame::FreshObjectsReported(report) => {
                self.on_fresh_objects_reported(*report);
            }
            LifecycleFrame::FreshObjectsDeleted(finished) => {
                self.on_fresh_objects_deleted(*finished);
            }
            LifecycleFrame::CheckTopicObjects { now } => {
                self.on_check_topic_objects(now);
            }
            LifecycleFrame::TopicObjectCheckFinished(finished) => {
                self.on_topic_object_check_finished(&finished);
            }
            LifecycleFrame::PeerManifestsListed {
                origin,
                now,
                manifests,
            } => {
                self.on_peer_manifests_listed(&origin, now, manifests).await;
            }
            LifecycleFrame::PeerManifestRead(finished) => {
                self.on_peer_manifest_read(*finished).await;
            }
            LifecycleFrame::RetainRetiredPeer {
                namespace,
                created_revision,
                now,
                message_expiry,
                max_bytes,
            } => {
                self.on_retain_retired_peer(
                    namespace,
                    created_revision,
                    now,
                    message_expiry,
                    max_bytes,
                )
                .await;
            }
            LifecycleFrame::ReportGeoPositions { round, scope } => {
                self.on_report_geo_positions(round, scope).await;
            }
            LifecycleFrame::ReportGeoPositionsFolded { round, scope } => {
                self.on_report_geo_positions_folded(round, scope.as_deref());
            }
            LifecycleFrame::GeoPositionsReported(report) => {
                self.on_geo_positions_reported(*report);
            }
            LifecycleFrame::GeoPositionsStored(finished) => {
                self.on_positions_stored(*finished);
            }
            LifecycleFrame::PeerPositionsListed(positions) => {
                self.on_peer_positions_listed(*positions).await;
            }
            LifecycleFrame::NoteGeoPositions { namespace } => {
                self.on_note_geo_positions(namespace).await;
            }
            LifecycleFrame::GeoCommitHeld(commit) => {
                self.on_geo_commit_held(*commit);
            }
            LifecycleFrame::GeoCommitsWaiting { topics } => {
                self.on_geo_commits_waiting(topics);
            }
            LifecycleFrame::GeoPositionsPublished { round, stored } => {
                self.on_geo_positions_published(round, stored);
            }
            LifecycleFrame::SegmentMergeFinished(finished) => {
                self.on_segment_merge_finished(*finished).await;
            }
            LifecycleFrame::RetainLongTerm {
                namespace,
                created_revision,
                now,
                message_expiry,
                max_bytes,
            } => {
                self.on_retain_long_term(
                    namespace,
                    created_revision,
                    now,
                    message_expiry,
                    max_bytes,
                )
                .await;
            }
            LifecycleFrame::TruncatePartition {
                namespace,
                up_to_offset,
            } => {
                // Pump-side enforcement of a committed delete watermark. The
                // committed offset is identical on every replica, so the local
                // deletion converges; idempotent if already trimmed past it.
                if let Some(partition) = self.plane.partitions().get_mut_by_ns(&namespace) {
                    let removal = partition.remove_sealed_segments_up_to(up_to_offset).await;
                    if removal.segments > 0 {
                        // See the cleaner arm: a truncate commits on the
                        // METADATA plane, so this partition's commit_op never
                        // moves and the cached offer stays a hit over unlinked
                        // files.
                        self.drop_partition_transfer_state(namespace, partition);
                        tracing::debug!(
                            shard = self.id,
                            namespace_raw = namespace.inner(),
                            segments = removal.segments,
                            messages = removal.messages,
                            up_to_offset,
                            "truncate-partition removed sealed segments"
                        );
                    }
                }
            }
        }
    }

    /// Build the one-shot ack a delegated replica handshake fires when it
    /// finishes, releasing the in-flight cap slot (inbound) or clearing
    /// the pending-dial entry (outbound) and refreshing the bundle of the
    /// peer. When this shard IS shard 0 the closure acks inline on the
    /// local bus (infallible, no frame in flight); otherwise it
    /// `try_send`s the outcome frame to shard 0, where a full or
    /// disconnected inbox is log-only: shard 0's deadline expiry and
    /// reconnect sweep cover the lost ack.
    fn replica_handshake_done_fn(
        &self,
        outcome: ReplicaHandshakeOutcome,
    ) -> ReplicaHandshakeDoneFn {
        if self.id == 0 {
            let bus = self.bus.clone();
            return Box::new(move || match outcome {
                ReplicaHandshakeOutcome::Inbound { slot, link } => {
                    bus.replica_inbound_handshake_done(slot, link);
                }
                ReplicaHandshakeOutcome::Outbound(link) => {
                    bus.replica_outbound_handshake_done(link);
                }
            });
        }
        let to_shard_zero = self.senders[0].clone();
        let metrics = self.metrics.clone();
        let shard = self.id;
        Box::new(move || {
            let frame = match outcome {
                ReplicaHandshakeOutcome::Inbound { slot, link } => {
                    LifecycleFrame::ReplicaInboundHandshakeDone { slot, link }
                }
                ReplicaHandshakeOutcome::Outbound(link) => {
                    LifecycleFrame::ReplicaOutboundHandshakeDone { link }
                }
            };
            if let Err(e) = to_shard_zero.try_send(ShardFrame::lifecycle(frame)) {
                metrics.record_frame_drop(
                    frame_drop_variant::REPLICA_HANDSHAKE_ACK,
                    crate::coordinator::classify_try_send_err(&e),
                );
                tracing::debug!(
                    shard,
                    "replica handshake outcome ack dropped (shard-0 expiry covers): {e:?}"
                );
            }
        })
    }
}

/// Outcome a delegated replica handshake reports back to shard 0:
/// release the in-flight cap slot (inbound) or clear the pending-dial
/// entry (outbound), then refresh the bundle of the peer. Becomes a
/// [`LifecycleFrame`] only on the cross-shard send leg; shard 0 applies
/// it inline.
#[derive(Clone, Copy)]
enum ReplicaHandshakeOutcome {
    Inbound { slot: u64, link: ReplicaLinkId },
    Outbound(ReplicaLinkId),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::host::NoopHost;
    use crate::metrics::ShardMetrics;
    use crate::partition_placement_tests::{PlacementShard, owner};
    use crate::shards_table::PapayaShardsTable;
    use crate::{
        PartitionConsensusConfig, PartitionRead, PartitionReadReply, ReplicaTopology,
        ShardIdentity, TaggedSender, build_deny_reply_from_request_header, shard_channel,
        shard_mesh_channels,
    };
    use consensus::{LocalPipeline, VsrConsensus};
    use iggy_binary_protocol::{
        Command, ConsensusError, GenericHeader, HEADER_SIZE, HeartbeatState, PrepareHeader,
        ReplyHeader, RoutedRequestHeader, prepare_identity_checksum_bytes,
    };
    use iggy_common::{IggyByteSize, IggyError, IggyTimestamp, PartitionStats, variadic};
    use message_bus::IggyMessageBus;
    use metadata::stm::stream::{
        Partition as TopicPartition, Stream, Streams, StreamsInner, Topic,
    };
    use metadata::stm::user::Users;
    use metadata::{IggyMetadata, MuxStateMachine};
    use partitions::{
        CheckpointDelays, IggyIndexWriter, IggyPartition, IggyPartitions, MessagesWriter,
        PartitionPathLayout, PartitionPersistence, PartitionsConfig, Segment,
    };
    use server_common::iobuf::Owned;
    use server_common::send_messages::{
        IggyMessage, IggyMessageHeader, IggyMessages, SendMessagesOwned,
    };
    use server_common::sharding::{PartitionLocation, ShardId};
    use server_common::{MESSAGE_ALIGN, Message, MessageBag, SegmentStorage};
    use std::mem::offset_of;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    /// An `Operation` discriminant this build does not know, arriving on an
    /// otherwise wire-valid consensus frame (correct command, size, checksum:
    /// exactly what a newer release sends after an op addition), must decode to
    /// its own error rather than the `InvalidBitPattern` that random memory
    /// corruption produces.
    ///
    /// The two need different operator actions: version skew is fixed by
    /// upgrading this node, and until it is, the frame's consensus group makes
    /// no progress (the op is never journaled, never acked, and every later
    /// prepare dies on the gap). `dispatch` splits its drop arms on this
    /// distinction; the accounting half is pinned in the server crate by
    /// `given_an_unknown_operation_when_dispatched_should_account_an_upgrade_fence_drop`.
    #[test]
    fn given_an_unknown_operation_when_a_consensus_frame_decodes_should_surface_an_upgrade_fence_signal()
     {
        // Far past every defined Operation discriminant (the highest is 162,
        // `DeleteConsumerOffset`).
        const OPERATION_FROM_A_NEWER_RELEASE: u8 = 0xEE;

        let mut owned = Owned::<MESSAGE_ALIGN>::zeroed(HEADER_SIZE);
        {
            let frame = owned.as_mut_slice();
            let size_offset = offset_of!(PrepareHeader, size);
            let frame_size = u32::try_from(HEADER_SIZE).expect("header size fits in u32");
            frame[size_offset..size_offset + 4].copy_from_slice(&frame_size.to_le_bytes());
            frame[offset_of!(PrepareHeader, command)] = Command::Prepare as u8;
            let client_offset = offset_of!(PrepareHeader, client);
            frame[client_offset..client_offset + 16].copy_from_slice(&0xCAFE_u128.to_le_bytes());
            frame[offset_of!(PrepareHeader, operation)] = OPERATION_FROM_A_NEWER_RELEASE;
            // A prepare's `checksum` carries the identity checksum, computed
            // over the operation byte among others; stamping it the way a real
            // sender does leaves the unknown byte as the frame's only anomaly
            // and is what lets the classifier trust that byte.
            let header: &[u8; HEADER_SIZE] = frame[..HEADER_SIZE]
                .try_into()
                .expect("frame spans a full header");
            let identity = prepare_identity_checksum_bytes(header);
            frame[..size_of::<u128>()].copy_from_slice(&identity.to_le_bytes());
        }

        // The generic view carries no operation field, so the receive path
        // accepts the frame; the unknown byte is first seen by the typed
        // decode inside `MessageBag::try_from`, which is what `dispatch` runs.
        let message = Message::<GenericHeader>::try_from(owned)
            .expect("a sealed Prepare frame is wire-valid in the generic view");

        let Err(error) = MessageBag::try_from(message) else {
            panic!("an operation this build does not know must not decode into a bag");
        };

        assert!(
            matches!(
                error,
                ConsensusError::UnsupportedOperation {
                    operation: OPERATION_FROM_A_NEWER_RELEASE
                }
            ),
            "unknown operation {OPERATION_FROM_A_NEWER_RELEASE:#x} is silently dropped: the \
             typed decode collapses a wire-valid frame from a newer release into the same \
             InvalidBitPattern as corruption, and dispatch drops both with only a warn log, \
             no operator signal and no upgrade fence, so an old node in a mixed cluster \
             gap-stops its consensus group while quorum hides it; got {error:?}"
        );
    }
    #[compio::test]
    async fn given_no_reply_owner_when_forwarded_client_send_arrives_should_not_fire_http_slot() {
        const CLIENT: u128 = 41;
        const REQUEST: u64 = 7;
        let (shard, _) = owner(0);
        shard.bus.clients().insert_in_process(CLIENT).unwrap();
        let (_slot, mut reply) = shard
            .bus
            .clients()
            .install_reply_slot(CLIENT, REQUEST)
            .unwrap();
        let header = RoutedRequestHeader {
            client: CLIENT,
            request: REQUEST,
            ..Default::default()
        };
        let message = build_deny_reply_from_request_header(
            &header,
            IggyError::TransientNotAccepted.as_code(),
        );
        shard
            .process_frame(ShardFrame::lifecycle(LifecycleFrame::ForwardClientSend {
                client_id: CLIENT,
                msg: message.into_generic().into_frozen().into(),
            }))
            .await;
        assert!(
            matches!(reply.try_recv(), Ok(None)),
            "an originless forwarded reply must not fill an HTTP slot"
        );
    }

    #[compio::test]
    async fn given_no_reply_owner_when_shard_denies_tombstoned_partition_should_not_fire_http_slot()
    {
        const CLIENT: u128 = 42;
        const REQUEST: u64 = 8;
        let (shard, _) = owner(0);
        let namespace = IggyNamespace::new(0, 0, 0);
        shard.plane.partitions().tombstone(namespace);
        shard.bus.clients().insert_in_process(CLIENT).unwrap();
        let (_slot, mut reply) = shard
            .bus
            .clients()
            .install_reply_slot(CLIENT, REQUEST)
            .unwrap();
        let request = Message::<RoutedRequestHeader>::new(size_of::<RoutedRequestHeader>())
            .transmute_header(|_, header: &mut RoutedRequestHeader| {
                header.command = Command::Request;
                header.operation = Operation::SendMessages;
                header.client = CLIENT;
                header.session = 1;
                header.request = REQUEST;
                header.group = namespace.inner();
                header.size = u32::try_from(size_of::<RoutedRequestHeader>()).unwrap();
            });
        shard.on_message(MessageBag::Request(request)).await;
        assert!(
            matches!(reply.try_recv(), Ok(None)),
            "a tombstone denial without its owner must not fill an HTTP slot"
        );
    }

    #[compio::test]
    async fn given_no_reply_owner_when_shard_denies_unroutable_request_should_not_fire_http_slot() {
        const CLIENT: u128 = 43;
        const REQUEST: u64 = 9;
        let (shard, _) = owner(0);
        shard.bus.clients().insert_in_process(CLIENT).unwrap();
        let (_slot, mut reply) = shard
            .bus
            .clients()
            .install_reply_slot(CLIENT, REQUEST)
            .unwrap();
        let request = Message::<RoutedRequestHeader>::new(size_of::<RoutedRequestHeader>())
            .transmute_header(|_, header: &mut RoutedRequestHeader| {
                header.command = Command::Request;
                header.operation = Operation::NonReplicated;
                header.client = CLIENT;
                header.request = REQUEST;
                header.size = u32::try_from(size_of::<RoutedRequestHeader>()).unwrap();
            });
        shard.on_message(MessageBag::Request(request)).await;
        assert!(
            matches!(reply.try_recv(), Ok(None)),
            "an unroutable denial without its owner must not fill an HTTP slot"
        );
    }

    #[compio::test]
    async fn given_carried_reply_when_partition_is_tombstoned_or_park_buffer_full_should_answer_owner()
     {
        const CLIENT: u128 = 45;
        const REQUEST: u64 = 11;
        for tombstoned in [false, true] {
            let (shard, _) = owner(0);
            let namespace = IggyNamespace::new(0, 0, 0);
            shard.bus.clients().insert_in_process(CLIENT).unwrap();
            let (_slot, mut unrelated) = shard
                .bus
                .clients()
                .install_reply_slot(CLIENT, REQUEST)
                .unwrap();
            let request = Message::<RoutedRequestHeader>::new(size_of::<RoutedRequestHeader>())
                .transmute_header(|_, header: &mut RoutedRequestHeader| {
                    header.command = Command::Request;
                    header.operation = Operation::SendMessages;
                    header.client = CLIENT;
                    header.session = 1;
                    header.request = REQUEST;
                    header.group = namespace.inner();
                    header.size = u32::try_from(size_of::<RoutedRequestHeader>()).unwrap();
                });
            if tombstoned {
                shard.plane.partitions().tombstone(namespace);
            } else {
                for _ in 0..crate::MAX_PARKED_PER_NAMESPACE {
                    shard.on_message(MessageBag::Request(request.clone())).await;
                }
                assert_eq!(
                    shard.parked_frame_count(namespace),
                    crate::MAX_PARKED_PER_NAMESPACE
                );
            }
            let (reply, receiver) = crate::channel(1);
            shard
                .process_frame(ShardFrame::lifecycle(LifecycleFrame::PartitionSubmit {
                    request,
                    reply,
                    attachment: None,
                }))
                .await;
            let response = receiver
                .try_recv()
                .expect("the carried receiver is answered immediately")
                .expect("a concrete denial")
                .try_into_typed::<iggy_binary_protocol::ReplyHeader>()
                .unwrap();
            assert_eq!(
                response.header().status,
                IggyError::TransientNotAccepted.as_code()
            );
            assert!(
                matches!(unrelated.try_recv(), Ok(None)),
                "the same client/request pair does not confer slot ownership"
            );
        }
    }

    #[compio::test]
    async fn given_no_reply_owner_when_shard_stages_deny_should_not_fire_http_slot() {
        const CLIENT: u128 = 44;
        const REQUEST: u64 = 10;
        let (mut shard, _) = owner(0);
        let (sender, _inbox, replies) = shard_channel(0, 1, 1);
        shard.attach_senders(vec![sender]);
        shard.bus.clients().insert_in_process(CLIENT).unwrap();
        let (_slot, mut reply) = shard
            .bus
            .clients()
            .install_reply_slot(CLIENT, REQUEST)
            .unwrap();
        let header = RoutedRequestHeader {
            client: CLIENT,
            request: REQUEST,
            ..Default::default()
        };
        assert!(shard.stage_transient_deny(&header));
        shard.process_frame(replies.recv().await.unwrap()).await;
        assert!(
            matches!(reply.try_recv(), Ok(None)),
            "a staged denial without its owner must not fill an HTTP slot"
        );
    }

    #[test]
    fn given_commit_batches_when_dispatched_should_send_each_owner_shard_one_frame_of_its_entries()
    {
        const SHARDS: u16 = 2;
        const INBOX_CAPACITY: usize = 4;
        const SENDER: u8 = 1;
        let (mut shard, _) = owner(0);
        let (senders, inboxes, _replies) = shard_mesh_channels(SHARDS, INBOX_CAPACITY, 1);
        shard.attach_senders(senders);
        let inboxes: Vec<_> = inboxes.into_iter().map(Option::unwrap).collect();
        let placement = shard.partition_shards();
        let groups_on = |target| {
            (0..)
                .map(|id| IggyNamespace::new(1, 1, id).inner())
                .filter(move |group| placement.shard_for_consensus_ns(*group) == target)
        };
        let on_zero = groups_on(0).next().unwrap();
        let on_one: Vec<u64> = groups_on(1).take(2).collect();

        let commit = |group| CommitEntry::new(group, 1, 2, 3, 4, HeartbeatState::Busy, 0);
        let one_owner = [commit(METADATA_GROUP), commit(on_zero)];
        let two_owners = [commit(on_zero), commit(on_one[0]), commit(METADATA_GROUP)];
        for entries in [&one_owner[..], &two_owners] {
            let batch = commit_batch::encode_commit_batch(1, SENDER, entries).unwrap();
            shard.dispatch(batch.into_generic());
        }
        assert_eq!(
            routed(&inboxes[0]),
            [
                (SENDER, one_owner.to_vec()),
                (SENDER, vec![commit(on_zero), commit(METADATA_GROUP)]),
            ]
        );
        assert_eq!(routed(&inboxes[1]), [(SENDER, vec![commit(on_one[0])])]);

        let reply = |group| CommitReplyEntry::new(group, 1, 2, 3, 0);
        let one_owner = [reply(on_one[0]), reply(on_one[1])];
        let two_owners = [reply(on_one[1]), reply(METADATA_GROUP), reply(on_one[0])];
        for entries in [&one_owner[..], &two_owners] {
            let batch = commit_batch::encode_commit_reply_batch(1, SENDER, entries).unwrap();
            shard.dispatch(batch.into_generic());
        }
        assert_eq!(routed(&inboxes[0]), [(SENDER, vec![reply(METADATA_GROUP)])]);
        assert_eq!(
            routed(&inboxes[1]),
            [
                (SENDER, one_owner.to_vec()),
                (SENDER, vec![reply(on_one[1]), reply(on_one[0])]),
            ]
        );
    }

    /// The sender node and the entries of every batch frame in `inbox`.
    fn routed<E: bytemuck::Pod>(inbox: &Receiver<ShardFrame>) -> Vec<(u8, Vec<E>)> {
        std::iter::from_fn(|| inbox.try_recv().ok())
            .map(|frame| {
                let decoded = match frame {
                    ShardFrame::Consensus {
                        message: MessageBag::Commit(message),
                        ..
                    } => commit_batch::verified_entries(&message, message.header().checksum_body)
                        .map(|entries| (message.header().replica, entries)),
                    ShardFrame::Consensus {
                        message: MessageBag::CommitReply(message),
                        ..
                    } => commit_batch::verified_entries(&message, message.header().checksum_body)
                        .map(|entries| (message.header().replica, entries)),
                    _ => None,
                };
                decoded.expect("only batch frames with a matching body checksum")
            })
            .collect()
    }

    /// Ops each partition commits before the stop.
    const COMMITTED_OPS: u64 = 2;
    /// One tick: the stop can start [`PARTITION_WALKS_PER_TICK_MAX`] checkpoints.
    const ONE_TICK_STOP_WINDOW: Duration = CONSENSUS_TICK_INTERVAL;
    /// More partitions than [`ONE_TICK_STOP_WINDOW`] reaches.
    const STOP_PARTITIONS: usize = PARTITION_WALKS_PER_TICK_MAX + 2;
    /// Holds every WAL completion and reply of a test without a drop.
    const SHARD_INBOX_CAPACITY: usize = 1024;
    /// The partition revision that the metadata and the routes agree on.
    const ROUTED_REVISION: u64 = 1;
    const STOP_BOUND: Duration = Duration::from_secs(10);
    const TASK_POLL_INTERVAL: Duration = Duration::from_millis(1);
    const STOP_STARTS_SERIES: &str = "partition_wal_checkpoint_starts_total{trigger=\"stop\"}";
    const STOP_MISSED_SERIES: &str = "partition_stop_checkpoints_missed_total";
    const IDLE_STARTS_SERIES: &str = "partition_wal_checkpoint_starts_total{trigger=\"idle\"}";
    const QUEUED_IN_FLIGHT_SERIES: &str = "partition_checkpoints_queued_in_flight";
    const TICK_VISITS_SERIES: &str = "partition_tick_visits_total";
    const SEGMENT_SIZE_BYTES: u64 = 1024 * 1024;
    /// Keeps the committed messages resident until the stop flushes them.
    const MESSAGES_REQUIRED_TO_SAVE: u32 = 100;
    /// Longer than the busy ticks after a commit, so a partition sleeps before
    /// its deadline, and shorter than the consensus timer that wakes it.
    const TEST_IDLE_DELAY: Duration = Duration::from_millis(150);
    const TEST_MAX_DELAY: Duration = Duration::from_secs(1);
    /// The pop runs in the tick after the enqueue at the deadline, and the
    /// start counts in the tick of the pop.
    const WAKE_SLACK_TICKS: u64 = 3;
    /// Ticks in which the test counts the visits to sleeping partitions. Timers
    /// still wake them a few times, but an awake partition takes a visit in
    /// every tick.
    const QUIET_TICKS: u32 = 50;

    /// A single-replica shard whose own frames, such as WAL completions, come
    /// back to its pump as they do in the server. Its metadata and routes hold
    /// `partition_count` partitions of stream 0 and topic 0, so the shard
    /// admits sends to them. Their files go under `root`. Without `delays`,
    /// the checkpoint rule stays off.
    fn wired_owner(
        root: &Path,
        partition_count: usize,
        delays: Option<CheckpointDelays>,
    ) -> (PlacementShard, TaggedSender) {
        let bus = Rc::new(IggyMessageBus::new(0));
        let routes = PapayaShardsTable::new();
        let mut topic = Topic::default();
        for id in 0..partition_count {
            let namespace = IggyNamespace::new(0, 0, id);
            routes.insert(
                namespace,
                PartitionLocation::new(ShardId::new(0), ROUTED_REVISION),
            );
            topic.partitions.push(TopicPartition::new(
                id,
                namespace.inner(),
                IggyTimestamp::default(),
                ROUTED_REVISION,
                0,
            ));
        }
        let mut stream = Stream::default();
        stream.topics.insert(topic);
        let mut streams = StreamsInner::default();
        streams.items.insert(Arc::new(stream));
        let segment_size = IggyByteSize::from(SEGMENT_SIZE_BYTES);
        let config = PartitionsConfig {
            messages_required_to_save: MESSAGES_REQUIRED_TO_SAVE,
            size_of_messages_required_to_save: segment_size,
            validate_checksum: true,
            segment_size,
            preallocate_segments: false,
            key_context: Arc::default(),
            path_layout: PartitionPathLayout {
                streams_root: root.to_string_lossy().into_owned(),
            },
        };
        let metadata = IggyMetadata::new(
            None,
            None,
            None,
            None,
            MuxStateMachine::<variadic!(Users, Streams)>::new((
                Users::default(),
                (streams.into(), ()),
            )),
            None,
        );
        let (sender, inbox, replies) = shard_channel(0, SHARD_INBOX_CAPACITY, SHARD_INBOX_CAPACITY);
        let shard = PlacementShard::new(
            ShardIdentity::new(0, "stop-checkpoint-test".to_owned()),
            bus.clone(),
            Rc::new(NoopHost),
            metadata,
            delays.into_iter().fold(
                IggyPartitions::new(ShardId::new(0), config),
                IggyPartitions::with_checkpoint_delays,
            ),
            vec![sender.clone()],
            inbox,
            replies,
            1,
            None,
            routes,
            PartitionConsensusConfig::new(1, ReplicaTopology::new(0, 1), bus),
            None,
            true,
            ShardMetrics::for_shard(),
        )
        .expect("valid shard wiring");
        (shard, sender)
    }

    /// A single-replica partition with an empty WAL, in the directory that
    /// the partitions config of `shard` names.
    #[allow(clippy::future_not_send)]
    async fn insert_wal_partition(shard: &PlacementShard, id: usize) -> IggyNamespace {
        let namespace = IggyNamespace::new(0, 0, id);
        let consensus = VsrConsensus::new(
            1,
            0,
            1,
            namespace.inner(),
            shard.bus.clone(),
            LocalPipeline::new(),
        );
        consensus.init();
        let partitions = shard.plane.partitions();
        let mut partition = IggyPartition::with_in_memory_storage(
            Arc::new(PartitionStats::default()),
            consensus,
            partitions.config().segment_size,
        );
        let directory = partition_dir(partitions.config(), namespace);
        std::fs::create_dir_all(&directory).unwrap();
        partition.set_partition_dir(directory.to_string_lossy().into_owned());
        Box::pin(attach_segment_files(
            &mut partition,
            partitions.config(),
            namespace,
        ))
        .await;
        Box::pin(partition.open_persistence()).await.unwrap();
        partitions.insert(namespace, partition);
        namespace
    }

    /// Backs the first segment with files, so the stop can flush the
    /// committed messages.
    #[allow(clippy::future_not_send)]
    async fn attach_segment_files(
        partition: &mut IggyPartition<Rc<IggyMessageBus>>,
        config: &PartitionsConfig,
        namespace: IggyNamespace,
    ) {
        let (stream, topic, id) = (
            namespace.stream_id(),
            namespace.topic_id(),
            namespace.partition_id(),
        );
        let log_path = config.get_messages_path(stream, topic, id, 0);
        let index_path = config.get_index_path(stream, topic, id, 0);
        let messages =
            MessagesWriter::new(&log_path, Rc::new(AtomicU64::new(0)), false, false, None)
                .await
                .unwrap();
        let indexes = IggyIndexWriter::new(&index_path, Rc::new(AtomicU64::new(0)), false, false)
            .await
            .unwrap();
        let storage = SegmentStorage::new(&log_path, &index_path, 0, 0, true)
            .await
            .unwrap();
        let segment_size = partition.log.active_segment().max_size;
        partition.log.retire_back();
        partition.log.add_persisted_segment(
            Segment::new(0, segment_size),
            storage,
            Some(Rc::new(messages)),
            Some(Rc::new(indexes)),
        );
    }

    fn partition_dir(config: &PartitionsConfig, namespace: IggyNamespace) -> PathBuf {
        PathBuf::from(config.get_partition_path(
            namespace.stream_id(),
            namespace.topic_id(),
            namespace.partition_id(),
        ))
    }

    /// Commits [`COMMITTED_OPS`] sends on `namespace` through the running
    /// pump, from a tick of its own, so each partition has its own first op
    /// tick.
    #[allow(clippy::future_not_send)]
    async fn commit_sends(shard: &PlacementShard, sender: &TaggedSender, namespace: IggyNamespace) {
        let clock = shard.plane.partitions().tick_clock();
        let tick = clock.now();
        while clock.now() == tick {
            compio::time::sleep(TASK_POLL_INTERVAL).await;
        }
        let pending: Vec<_> = (1..=COMMITTED_OPS)
            .map(|client| {
                let (reply, receiver) = crate::channel(1);
                sender
                    .try_send(ShardFrame::lifecycle(LifecycleFrame::PartitionSubmit {
                        request: send_request(namespace, client),
                        reply,
                        attachment: None,
                    }))
                    .expect("the request fits the inbox");
                receiver
            })
            .collect();
        for receiver in pending {
            let reply: Message<ReplyHeader> = receiver
                .recv()
                .await
                .unwrap()
                .expect("a committed send")
                .try_into_typed()
                .unwrap();
            assert_eq!(reply.header().status, 0);
        }
    }

    /// One message from its own client, because a client has one request in
    /// flight per partition.
    fn send_request(namespace: IggyNamespace, client: u64) -> Message<RoutedRequestHeader> {
        let mut messages = IggyMessages::with_capacity(1);
        messages.push(IggyMessage {
            header: IggyMessageHeader {
                id: u128::from(client),
                payload_length: 1,
                ..Default::default()
            },
            payload: vec![1].into(),
            user_headers: None,
        });
        SendMessagesOwned::from_messages(namespace, &messages)
            .unwrap()
            .encode_request(RoutedRequestHeader {
                command: Command::Request,
                operation: Operation::SendMessages,
                client: u128::from(client),
                session: 1,
                request: 1,
                group: namespace.inner(),
                ..Default::default()
            })
            .unwrap()
    }

    /// Runs the pump and stops it once `exercise` ends. Panics when the stop
    /// does not finish within [`STOP_BOUND`].
    #[allow(clippy::future_not_send)]
    async fn run_until_stopped(
        shard: &PlacementShard,
        exercise: impl std::future::Future<Output = ()>,
    ) {
        let (stop, stopped) = crate::channel(1);
        let pump = shard.run_message_pump(stopped, Arc::new(AtomicBool::new(false)));
        let exercise = async {
            exercise.await;
            stop.try_send(()).unwrap();
        };
        let complete = async { futures::join!(pump, exercise) }.fuse();
        let bound = compio::time::sleep(STOP_BOUND).fuse();
        futures::pin_mut!(complete, bound);
        futures::select_biased! {
            (fault, ()) = complete => assert!(fault.is_none(), "the stop fenced a partition: {fault:?}"),
            () = bound => panic!("the stop did not finish within {STOP_BOUND:?}"),
        }
    }

    fn series_value(scrape: &str, series: &str) -> u64 {
        scrape
            .lines()
            .find_map(|line| line.strip_prefix(series)?.trim().parse().ok())
            .unwrap_or_else(|| panic!("no {series} in the scrape:\n{scrape}"))
    }

    /// The checkpoint op and the ops of the WAL of `namespace` in its
    /// partition directory `directory`, read back as a boot reads them.
    #[allow(clippy::future_not_send)]
    async fn reopened_wal(
        directory: &Path,
        namespace: IggyNamespace,
        revision: u64,
    ) -> (u64, Vec<u64>) {
        let directory = directory.join(format!("prepares-{revision}"));
        let (persistence, prepares) =
            PartitionPersistence::open(&directory, namespace.inner(), revision)
                .await
                .unwrap();
        (
            persistence.checkpoint_op(),
            prepares.iter().map(|prepare| prepare.header().op).collect(),
        )
    }

    /// What a stop left behind.
    struct StopOutcome {
        stop_starts: u64,
        missed: u64,
        /// In first op order: the namespace, its commit point and its WAL as
        /// a boot reads it, see [`reopened_wal`].
        partitions: Vec<(IggyNamespace, u64, (u64, Vec<u64>))>,
    }

    /// Commits [`COMMITTED_OPS`] sends on each of `partition_count` WAL
    /// partitions, against namespace order, then stops the pump with the stop
    /// window `window`. Every partition must complete its stop.
    #[allow(clippy::future_not_send)]
    async fn stop_after_commits(partition_count: usize, window: Duration) -> StopOutcome {
        let root = tempfile::tempdir().unwrap();
        let (shard, sender) = wired_owner(root.path(), partition_count, None);
        let mut namespaces = Vec::with_capacity(partition_count);
        for id in (0..partition_count).rev() {
            namespaces.push(Box::pin(insert_wal_partition(&shard, id)).await);
        }
        shard.set_stop_checkpoint_window(window);

        Box::pin(run_until_stopped(&shard, async {
            for namespace in &namespaces {
                commit_sends(&shard, &sender, *namespace).await;
            }
        }))
        .await;

        let scrape = shard.metrics.scrape();
        let partitions = shard.plane.partitions();
        let stopped: Vec<_> = namespaces
            .iter()
            .map(|namespace| {
                let partition = partitions.get_by_ns(namespace).unwrap();
                assert!(partition.shutdown_io_complete());
                (
                    *namespace,
                    partition_dir(partitions.config(), *namespace),
                    partition.created_revision(),
                    partition.consensus().commit_min(),
                )
            })
            .collect();
        drop(shard);
        let mut outcome = StopOutcome {
            stop_starts: series_value(&scrape, STOP_STARTS_SERIES),
            missed: series_value(&scrape, STOP_MISSED_SERIES),
            partitions: Vec::with_capacity(partition_count),
        };
        for (namespace, directory, revision, commit_min) in stopped {
            let wal = Box::pin(reopened_wal(&directory, namespace, revision)).await;
            outcome.partitions.push((namespace, commit_min, wal));
        }
        outcome
    }

    /// No build lands after a stop, so the pump answers the reads that wait
    /// for one. Committed partitions that the shard never built stand for the
    /// gap after a create.
    #[compio::test]
    async fn given_shutdown_when_reads_are_held_should_answer_all() {
        const HELD_READS: usize = 2;
        let root = tempfile::tempdir().unwrap();
        let (shard, sender) = wired_owner(root.path(), HELD_READS, None);
        let held: Vec<_> = (0..HELD_READS)
            .map(|id| {
                let (reply, replies) = crate::channel(1);
                sender
                    .try_send(ShardFrame::lifecycle(LifecycleFrame::PartitionRead {
                        namespace: IggyNamespace::new(0, 0, id),
                        read: PartitionRead::Primary,
                        reply,
                        hold: None,
                    }))
                    .unwrap();
                replies
            })
            .collect();

        Box::pin(run_until_stopped(&shard, async {
            while shard.held_read_count() < HELD_READS {
                compio::time::sleep(TASK_POLL_INTERVAL).await;
            }
        }))
        .await;

        for replies in &held {
            assert!(matches!(
                replies.try_recv(),
                Ok(PartitionReadReply::Rejected(
                    IggyError::TransientNotAccepted
                ))
            ));
        }
        assert_eq!(shard.held_read_count(), 0);
        let (reply, replies) = crate::channel(1);
        shard
            .on_partition_read(
                IggyNamespace::new(0, 0, 0),
                PartitionRead::Primary,
                reply,
                None,
            )
            .await;
        assert!(
            matches!(replies.try_recv(), Ok(PartitionReadReply::NotFound)),
            "a stopped shard holds no new read"
        );
    }

    /// The stop serves the partitions with the oldest first op up to its
    /// reach, counts the rest as missed, and every partition still completes.
    /// A boot then loads no op of a served partition again.
    #[compio::test]
    async fn given_more_partitions_than_the_stop_reach_when_stopping_should_checkpoint_the_oldest_and_count_the_rest()
     {
        let outcome = Box::pin(stop_after_commits(STOP_PARTITIONS, ONE_TICK_STOP_WINDOW)).await;

        let served = u64::try_from(PARTITION_WALKS_PER_TICK_MAX).unwrap();
        let unserved = u64::try_from(STOP_PARTITIONS).unwrap() - served;
        assert_eq!(
            (outcome.stop_starts, outcome.missed),
            (served, unserved),
            "(stop checkpoint starts, missed partitions)"
        );
        for (index, (namespace, commit_min, wal)) in outcome.partitions.iter().enumerate() {
            assert_eq!(*commit_min, COMMITTED_OPS);
            let expected = if index < PARTITION_WALKS_PER_TICK_MAX {
                (*commit_min, vec![*commit_min])
            } else {
                (0, (1..=*commit_min).collect())
            };
            assert_eq!(*wal, expected, "partition {}", namespace.partition_id());
        }
    }

    /// Idle partitions come due one idle delay after their last op, although
    /// they sleep: the deadline wakes them. Each gets one checkpoint through
    /// the queue, and after it no partition stays awake.
    #[compio::test]
    async fn given_idle_partitions_when_the_idle_delay_passes_should_checkpoint_each_once_and_sleep()
     {
        const PARTITIONS: usize = 4;
        let root = tempfile::tempdir().unwrap();
        let delays = CheckpointDelays::new(TEST_IDLE_DELAY, TEST_MAX_DELAY);
        let (shard, sender) = wired_owner(root.path(), PARTITIONS, Some(delays));
        let mut namespaces = Vec::with_capacity(PARTITIONS);
        for id in 0..PARTITIONS {
            namespaces.push(Box::pin(insert_wal_partition(&shard, id)).await);
        }
        let due = u64::try_from(PARTITIONS).unwrap();
        let idle_ticks =
            u64::try_from(TEST_IDLE_DELAY.as_micros() / CONSENSUS_TICK_INTERVAL.as_micros())
                .unwrap();
        let clock = shard.plane.partitions().tick_clock();
        let mut ticks_to_due = None;
        let mut quiet_visits = None;
        Box::pin(run_until_stopped(&shard, async {
            for namespace in &namespaces {
                commit_sends(&shard, &sender, *namespace).await;
            }
            let committed_at = clock.now();
            while series_value(&shard.metrics.scrape(), IDLE_STARTS_SERIES) < due {
                compio::time::sleep(TASK_POLL_INTERVAL).await;
            }
            ticks_to_due = Some(clock.now() - committed_at);
            while series_value(&shard.metrics.scrape(), QUEUED_IN_FLIGHT_SERIES) > 0 {
                compio::time::sleep(TASK_POLL_INTERVAL).await;
            }
            let partitions = shard.plane.partitions();
            for namespace in &namespaces {
                let partition = partitions.get_by_ns(namespace).unwrap();
                assert_eq!(
                    partition.checkpoint_deadline(delays),
                    None,
                    "a deadline that stays armed wakes partition {} again",
                    namespace.partition_id()
                );
            }
            let before = series_value(&shard.metrics.scrape(), TICK_VISITS_SERIES);
            compio::time::sleep(CONSENSUS_TICK_INTERVAL * QUIET_TICKS).await;
            quiet_visits = Some(series_value(&shard.metrics.scrape(), TICK_VISITS_SERIES) - before);
        }))
        .await;

        let ticks_to_due = ticks_to_due.unwrap();
        assert!(
            ticks_to_due <= idle_ticks + WAKE_SLACK_TICKS,
            "the last idle checkpoint started {ticks_to_due} ticks after the commits"
        );
        let quiet_visits = quiet_visits.unwrap();
        assert!(
            quiet_visits < u64::from(QUIET_TICKS),
            "{quiet_visits} visits in {QUIET_TICKS} ticks: a partition stayed awake"
        );
        let scrape = shard.metrics.scrape();
        assert_eq!(series_value(&scrape, IDLE_STARTS_SERIES), due);
        assert_eq!(series_value(&scrape, STOP_STARTS_SERIES), 0);
        let partitions = shard.plane.partitions();
        let stopped: Vec<_> = namespaces
            .iter()
            .map(|namespace| {
                let partition = partitions.get_by_ns(namespace).unwrap();
                (
                    *namespace,
                    partition_dir(partitions.config(), *namespace),
                    partition.created_revision(),
                )
            })
            .collect();
        drop(shard);
        for (namespace, directory, revision) in stopped {
            let wal = Box::pin(reopened_wal(&directory, namespace, revision)).await;
            assert_eq!(
                wal,
                (COMMITTED_OPS, vec![COMMITTED_OPS]),
                "partition {}",
                namespace.partition_id()
            );
        }
    }
}
