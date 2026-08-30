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

use crate::metrics::{frame_drop_reason, frame_drop_variant};
use crate::shards_table::{
    ShardsTable, calculate_shard_assignment, calculate_shard_from_consensus_ns,
};
use crate::{IggyShard, LifecycleFrame, Receiver, RestorableMetadataStm, ShardFrame};
use consensus::{MetadataHandle, PartitionsHandle};
use crossfire::TrySendError;
use futures::FutureExt;
use iggy_binary_protocol::{Command, ConsensusError, GenericHeader, Operation, PrepareHeader};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use message_bus::{ConnectionInstaller, MessageBus, ReplicaHandshakeDoneFn};
use partitions::FatalCommit;
use server_common::sharding::{IggyNamespace, METADATA_GROUP};
use server_common::{Message, MessageBag};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// How often the shard pump drives `VsrConsensus::tick`.
///
/// Heartbeats, prepare retransmit, and view-change timeouts only advance
/// when the tick runs. Re-exported from `consensus` so the drive cadence and
/// the tick counts it feeds share one unit; public so the simulator can
/// advance its virtual clock in whole tick intervals.
pub use consensus::TICK_INTERVAL as CONSENSUS_TICK_INTERVAL;

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
                // upgrading it can. A routed request, by contrast, is dropped
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
                         making progress until this node is upgraded"
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
                    "frame carries an operation this build does not know; a newer release \
                     likely added it. {consequence}"
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
        self.route_typed(operation, namespace, bag);
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
        (self.on_client_request)(client_id, message);
    }

    /// Route a consensus-control message (`StartViewChange`, `DoViewChange`,
    /// `StartView`, `Commit`) by hashing its raw `u64` consensus namespace.
    /// Every node uses the same hash so the shard owning the consensus
    /// group is deterministic across the cluster.
    pub(crate) fn route_consensus_control(
        &self,
        message: MessageBag,
        namespace_u64: u64,
        operation: Operation,
    ) {
        let target = calculate_shard_from_consensus_ns(namespace_u64, self.shard_count);
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
            // `calculate_shard_assignment`'s result), seeded asynchronously
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
                    calculate_shard_assignment(&partition_namespace, self.shard_count)
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
    {
        // Reused across every pump iteration; pre-size to skip the
        // first-drain reallocation.
        let mut loopback_buf = Vec::with_capacity(64);
        let mut namespace_scratch: Vec<IggyNamespace> = Vec::with_capacity(64);
        // Consensus timer driver, folded into the pump: running the tick as a
        // select! arm (not a sibling task) serializes it with frame processing,
        // so `tick_partitions` can no longer hold a partition reference across
        // an `.await` while `apply_reconcile_ops` reallocates the partitions
        // vec on this same task. The timer is created once and pinned, then
        // re-armed only after it fires, so a busy inbox cannot drop-and-reset
        // it (which would stall heartbeats / prepare retransmit).
        // Single source for the timer, so the re-arm below cannot drift from the
        // initial interval.
        // The tick timer comes from the bus so the environment decides the
        // clock: compio wall time in production, virtual time in the
        // simulator (see `MessageBus::sleep`).
        let rearm_tick = || self.bus.sleep(CONSENSUS_TICK_INTERVAL).fuse();
        let mut consensus_tick = std::pin::pin!(rearm_tick());
        let mut fatal: Option<FatalCommit> = None;
        loop {
            // `select_biased!`, not `select!`: the unbiased macro draws its
            // arm order from a process-random thread-local PRNG, which the
            // deterministic simulator cannot seed. The listed order is the
            // intended priority anyway: stop, then tick, then frames.
            futures::select_biased! {
                _ = stop.recv().fuse() => break,
                () = consensus_tick.as_mut() => {
                    // Sharing the pump task is what keeps `tick_partitions`
                    // borrow-safe, but it bounds the tick's worst-case delay
                    // to one main frame body's longest `.await` (replication
                    // append + commit_journal fsync/rotate + reply) plus the
                    // one reply-lane bus send drained per main frame.
                    // TODO(hubcio): if a load test shows tick starvation,
                    // make `tick_partitions` borrow-free so the tick can be
                    // decoupled from the pump again without reintroducing the
                    // partition-ref-across-`.await` UB this fold closed.
                    self.tick_metadata().await;
                    if let Some(fault) = self.tick_partitions(&mut namespace_scratch).await {
                        fatal = Some(fault);
                        break;
                    }
                    // Runs here, not inside `tick_metadata`: that early-returns
                    // on shards without metadata consensus, and partition-plane
                    // offers live on every shard that hosts a serving group --
                    // parked behind the shard-0 gate they would never expire.
                    self.expire_idle_state_transfer_offers();
                    // While a cooperative revocation is pending, wake the
                    // reconciler each tick so the handoff completes within ~one
                    // tick of the partition draining, not the periodic pass.
                    if self
                        .plane
                        .metadata()
                        .mux_stm
                        .streams()
                        .has_pending_revocations()
                    {
                        self.dispatch_metadata_commit_tick();
                    }
                    // A dropped `ReconcileApply` marker (full inbox at
                    // `enqueue_reconcile_op`) otherwise strands staged ops on
                    // a quiet shard until the next inbound frame's tail
                    // drain; parked partition frames then never re-dispatch.
                    self.apply_reconcile_ops();
                    consensus_tick.set(rearm_tick());
                }
                frame = self.inbox.recv().fuse() => {
                    match frame {
                        Ok(frame) => {
                            if self.accept_frame_for_self(&frame) {
                                self.process_frame(frame).await;
                                self.process_loopback(&mut loopback_buf, &mut namespace_scratch).await;
                                // Tail drain catches reconcile ops whose marker was dropped.
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
                        }
                        Err(_) => break,
                    }
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
                        }
                        Err(_) => break,
                    }
                }
            }
        }

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
        if fatal.is_none() {
            while let Ok(frame) = self.inbox.try_recv() {
                if self.accept_frame_for_self(&frame) {
                    self.process_frame(frame).await;
                    self.process_loopback(&mut loopback_buf, &mut namespace_scratch)
                        .await;
                    self.apply_reconcile_ops();
                    if let Some(fault) = self.first_partition_commit_fault() {
                        fatal = Some(fault);
                        break;
                    }
                }
            }
        }
        if fatal.is_none() {
            while let Ok(frame) = self.reply_inbox.try_recv() {
                if self.accept_frame_for_self(&frame) {
                    self.process_frame(frame).await;
                    if let Some(fault) = self.first_partition_commit_fault() {
                        fatal = Some(fault);
                        break;
                    }
                }
            }
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
        // graceful restart recovers consumer offsets ahead of the data. Runs
        // on a fault too, the fenced partition included: its resident prefix
        // is cluster-committed data, so writing what still reaches disk is
        // strictly better than dropping it, and a second failure of an
        // already-fenced partition is warned rather than propagated.
        self.flush_partitions().await;

        // A failed flush fences its partition: the data it could not write is
        // cluster-committed and now lives only in this process's memory, so a
        // clean exit here would report a durability loss as a good shutdown.
        if fatal.is_none() {
            fatal = self.first_partition_commit_fault();
        }

        fatal
    }

    /// First partition commit fault currently fenced on this shard.
    ///
    /// The regular path observes faults in `tick_partitions`. This scan covers
    /// the pump-exit edges where a stop signal wins before that tick, or a
    /// queued frame fails while the pump is draining during shutdown.
    fn first_partition_commit_fault(&self) -> Option<FatalCommit> {
        let partitions = self.plane.partitions();
        partitions.namespaces().find_map(|namespace| {
            partitions
                .get_by_ns(namespace)
                .and_then(|partition| partition.fatal().cloned())
        })
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
    }

    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    async fn process_lifecycle(&self, payload: LifecycleFrame)
    where
        B: MessageBus + 'static,
    {
        match payload {
            LifecycleFrame::ReplicaInboundSetup { fd, slot } => {
                tracing::info!(
                    shard = self.id,
                    slot,
                    raw_fd = fd.as_raw_fd(),
                    "installing blind-delegated inbound replica fd"
                );
                let on_done =
                    self.replica_handshake_done_fn(ReplicaHandshakeOutcome::ReleaseSlot(slot));
                self.bus
                    .install_replica_inbound_fd(fd, self.on_replica_message.clone(), on_done);
            }
            LifecycleFrame::ReplicaOutboundSetup { fd, replica_id } => {
                tracing::info!(
                    shard = self.id,
                    replica_id,
                    raw_fd = fd.as_raw_fd(),
                    "installing delegated outbound replica fd"
                );
                let on_done =
                    self.replica_handshake_done_fn(ReplicaHandshakeOutcome::ClearDial(replica_id));
                self.bus.install_replica_outbound_fd(
                    fd,
                    replica_id,
                    self.on_replica_message.clone(),
                    on_done,
                );
            }
            LifecycleFrame::ReplicaInboundHandshakeDone { slot } => {
                // Only shard 0 tracks in-flight handshake slots; a
                // non-zero owning shard's ack closure addresses senders[0]
                // (shard 0 itself releases inline, no frame).
                debug_assert_eq!(
                    self.id, 0,
                    "ReplicaInboundHandshakeDone routed to shard {}",
                    self.id
                );
                self.bus.release_replica_handshake_slot(slot);
            }
            LifecycleFrame::ReplicaOutboundHandshakeDone { replica_id } => {
                debug_assert_eq!(
                    self.id, 0,
                    "ReplicaOutboundHandshakeDone routed to shard {}",
                    self.id
                );
                self.bus.clear_replica_dial_pending(replica_id);
            }
            LifecycleFrame::ClientConnectionSetup { fd, meta } => {
                tracing::info!(
                    shard = self.id,
                    client_id = meta.client_id,
                    raw_fd = fd.as_raw_fd(),
                    "installing delegated client fd"
                );
                self.bus
                    .install_client_fd(fd, meta, self.on_client_request.clone());
            }
            LifecycleFrame::ClientWsConnectionSetup { fd, meta } => {
                tracing::info!(
                    shard = self.id,
                    client_id = meta.client_id,
                    raw_fd = fd.as_raw_fd(),
                    "installing delegated WS client fd (pre-upgrade)"
                );
                self.bus
                    .install_client_ws_fd(fd, meta, self.on_client_request.clone());
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
                    // Terminal per `SendError` docs: the client never
                    // receives the reply and request / response semantics
                    // break above the bus. Bump the receiver-side counter
                    // (sender side already bumps in `builder.rs`) and raise
                    // to `warn!` so it surfaces in default operator logs.
                    self.metrics.record_frame_drop(
                        frame_drop_variant::FORWARD_CLIENT_SEND,
                        frame_drop_reason::DELIVERY_FAILED,
                    );
                    tracing::warn!(
                        shard = self.id,
                        client_id,
                        error = ?e,
                        "forward-client-send delivery failed"
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
                (self.on_metadata_submit)(submit);
            }
            LifecycleFrame::ListClients { reply } => {
                // Every shard handles this (not shard-0-only): each replies
                // with the clients whose connections it homes. The handler
                // (wired by the server) reads this shard's `SessionManager`
                // and pushes the list over `reply`.
                (self.on_list_clients)(reply);
            }
            LifecycleFrame::PartitionRead {
                namespace,
                read,
                reply,
            } => {
                // Addressed to the shard owning `namespace` (the sender
                // resolved it via the shards table). The handler (wired by
                // the server) runs the read against this shard's partitions
                // plane and pushes the result over `reply`; a dropped
                // sender means the read is skipped and the gather side
                // times out.
                (self.on_partition_read)(namespace, read, reply);
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
            LifecycleFrame::PurgePartition {
                namespace,
                generation,
            } => {
                // Pump-side enforcement of a committed `PurgeTopic`: reset the
                // partition to empty at offset 0 and clear consumer offsets.
                // Idempotent per generation -- skip if already applied so a
                // redundant reconcile pass does not wipe messages sent since.
                let config = self.plane.partitions().config().clone();
                if let Some(partition) = self.plane.partitions().get_mut_by_ns(&namespace)
                    && partition.applied_purge_generation() < generation
                {
                    match partition.purge(&config, generation).await {
                        Ok(()) => {
                            // The purge unlinked the very bytes this shard is
                            // serving: the cached offer still advertises the
                            // pre-purge manifest and the payload cache can
                            // answer chunk requests for it without touching
                            // disk, so a puller would install purged data. Both
                            // are keyed on pre-purge content, so neither can
                            // notice on its own.
                            self.drop_partition_transfer_state(namespace, partition);
                            tracing::debug!(
                                shard = self.id,
                                namespace_raw = namespace.inner(),
                                generation,
                                "purge-partition reset partition to empty"
                            );
                        }
                        Err(partitions::PurgeError::FrontierNotRecorded) => {
                            // NOT fenced: nothing was mutated, so the chain is
                            // whole and `applied_purge_generation` is unmoved,
                            // which means the reconciler's `committed > applied`
                            // gate still sees this purge as outstanding.
                            // Fencing here would quarantine live data, and the
                            // fence's own frontier write would first stamp the
                            // pre-purge counter the purge was about to reset.
                            //
                            // NOT woken: staging a purge counts as work in the
                            // pass, which keeps the fast-skip disarmed, so the
                            // ordinary periodic pass re-issues until one lands
                            // and stops once `applied` catches `committed`. An
                            // eager wake here closes a loop with no pacing in
                            // it at all -- pass, stage, defer, wake -- and on a
                            // disk that refuses instantly that is a full O(N)
                            // reconcile scan and a real `atomic_replace`
                            // attempt per turn, holding the partition write
                            // lock each time.
                            tracing::warn!(
                                shard = self.id,
                                namespace_raw = namespace.inner(),
                                generation,
                                "purge-partition deferred: could not record the frontier reset; \
                                 the reconciler re-issues it while the generation stays unapplied"
                            );
                        }
                        Err(error @ partitions::PurgeError::GenerationNotRecorded(_)) => {
                            // NOT fenced: the wipe ran and a fresh chain is
                            // planted, so the partition is serviceable; only
                            // the durable generation record failed, which
                            // leaves `applied_purge_generation` unmoved and
                            // the reconciler re-issuing the (now cheap) purge.
                            // Same pacing argument as the frontier deferral
                            // above; the caches already describe wiped bytes.
                            self.drop_partition_transfer_state(namespace, partition);
                            tracing::warn!(
                                shard = self.id,
                                namespace_raw = namespace.inner(),
                                generation,
                                %error,
                                "purge-partition deferred: reset applied but the generation \
                                 record failed; the reconciler re-issues it"
                            );
                        }
                        Err(error @ partitions::PurgeError::Unserviceable(_)) => {
                            // Past the drain, so this group has no serviceable
                            // chain and the next append panics on
                            // `active_segment()`. Fence it for rebuild, exactly
                            // as a failed state-transfer convergence does. The
                            // counters were already reset to 0 before the
                            // fallible plant, so the fence's advancing write
                            // records the post-purge frontier.
                            tracing::error!(
                                shard = self.id,
                                namespace_raw = namespace.inner(),
                                generation,
                                %error,
                                "purge-partition failed to reset partition; fencing it for rebuild"
                            );
                            // Fenced, but the caches still describe the
                            // pre-purge bytes until the rebuild lands.
                            self.drop_partition_transfer_state(namespace, partition);
                            self.fence_partition_for_rebuild(namespace, partition, None)
                                .await;
                        }
                    }
                }
            }
        }
    }

    /// Build the one-shot ack a delegated replica handshake fires when it
    /// finishes, releasing the in-flight cap slot (inbound) or clearing
    /// the pending-dial entry (outbound). When this shard IS shard 0 the
    /// closure releases inline on the local bus (infallible, no frame in
    /// flight); otherwise it `try_send`s the outcome frame to shard 0,
    /// where a full or disconnected inbox is log-only: shard 0's
    /// deadline expiry covers the lost ack.
    fn replica_handshake_done_fn(
        &self,
        outcome: ReplicaHandshakeOutcome,
    ) -> ReplicaHandshakeDoneFn {
        if self.id == 0 {
            let bus = self.bus.clone();
            return Box::new(move || match outcome {
                ReplicaHandshakeOutcome::ReleaseSlot(slot) => {
                    bus.release_replica_handshake_slot(slot);
                }
                ReplicaHandshakeOutcome::ClearDial(replica_id) => {
                    bus.clear_replica_dial_pending(replica_id);
                }
            });
        }
        let to_shard_zero = self.senders[0].clone();
        let metrics = self.metrics.clone();
        let shard = self.id;
        Box::new(move || {
            let frame = match outcome {
                ReplicaHandshakeOutcome::ReleaseSlot(slot) => {
                    LifecycleFrame::ReplicaInboundHandshakeDone { slot }
                }
                ReplicaHandshakeOutcome::ClearDial(replica_id) => {
                    LifecycleFrame::ReplicaOutboundHandshakeDone { replica_id }
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
/// entry (outbound). Becomes a [`LifecycleFrame`] only on the
/// cross-shard send leg; shard 0 applies it inline.
#[derive(Clone, Copy)]
enum ReplicaHandshakeOutcome {
    ReleaseSlot(u64),
    ClearDial(u8),
}

#[cfg(test)]
mod tests {
    use iggy_binary_protocol::{
        Command, ConsensusError, GenericHeader, HEADER_SIZE, PrepareHeader,
        prepare_identity_checksum_bytes,
    };
    use server_common::iobuf::Owned;
    use server_common::{MESSAGE_ALIGN, Message, MessageBag};
    use std::mem::offset_of;

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
}
