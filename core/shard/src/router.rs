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
use crate::{IggyShard, LifecycleFrame, Receiver, ShardFrame};
use consensus::{MetadataHandle, PartitionsHandle};
use crossfire::TrySendError;
use futures::FutureExt;
use iggy_binary_protocol::{ConsensusHeader, GenericHeader, Operation, PrepareHeader};
use journal::{Journal, JournalHandle};
use message_bus::{ConnectionInstaller, MessageBus, ReplicaHandshakeDoneFn};
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::StateMachine;
use server_common::sharding::{IggyNamespace, METADATA_CONSENSUS_NAMESPACE};
use server_common::{Message, MessageBag};

/// How often the shard pump drives `VsrConsensus::tick`: heartbeats, prepare
/// retransmit, and view-change timeouts only advance when the tick runs
/// ("call this periodically, e.g. every 10ms").
const CONSENSUS_TICK_INTERVAL: std::time::Duration = std::time::Duration::from_millis(10);

/// Decompose a [`MessageBag`] into the routing-relevant tuple
/// `(operation, namespace, generic_message)`.
///
/// Single source of truth used by every dispatch entry point so the
/// operation / namespace extraction never drifts between call sites.
fn extract_routing(bag: MessageBag) -> (Operation, u64, Message<GenericHeader>) {
    match bag {
        MessageBag::Request(r) => {
            let h = *r.header();
            (h.operation, h.namespace, r.into_generic())
        }
        MessageBag::Prepare(p) => {
            let h = *p.header();
            (h.operation, h.namespace, p.into_generic())
        }
        MessageBag::PrepareOk(p) => {
            let h = *p.header();
            (h.operation, h.namespace, p.into_generic())
        }
        MessageBag::StartViewChange(m) => {
            let h = *m.header();
            (h.operation(), h.namespace, m.into_generic())
        }
        MessageBag::DoViewChange(m) => {
            let h = *m.header();
            (h.operation(), h.namespace, m.into_generic())
        }
        MessageBag::StartView(m) => {
            let h = *m.header();
            (h.operation(), h.namespace, m.into_generic())
        }
        MessageBag::Commit(m) => {
            let h = *m.header();
            (h.operation(), h.namespace, m.into_generic())
        }
    }
}

/// Inter-shard dispatch logic.
///
/// All messages — whether destined for a local or remote shard — are routed
/// through the channel into the target shard's message pump.  This ensures
/// that every mutation on a shard is serialized through a single point (the
/// pump), preventing concurrent access from independent async tasks.
impl<B, MJ, S, M, T> IggyShard<B, MJ, S, M, T>
where
    B: MessageBus + ConnectionInstaller + Clone + 'static,
    T: ShardsTable,
{
    /// Network-receive entry point. Classifies the raw
    /// `Message<GenericHeader>` and routes it to the owning shard via
    /// `route_typed`.
    //
    // TODO(hubcio): perf - this `MessageBag::try_from` is run twice per
    // consensus frame: once here to extract (operation, namespace) for
    // routing, and a second time on the receiving shard inside `on_message`
    // (lib.rs ~560) to dispatch to the correct on_* handler. The second
    // parse re-runs `bytemuck::checked::try_from_bytes` + per-header
    // `validate()` on bytes already validated upstream. Measured ~50 ns/
    // frame; at 1M ops/sec/shard ~ 50 ms/sec/shard of pure re-validation.
    //
    // Fix: thread the classified bag through `ShardFrame::Consensus` (carry
    // `MessageBag` instead of `Message<GenericHeader>`) so the inbox path
    // matches directly with no second parse. Consensus variant grows from
    // ~24 B to ~32 B, but `ShardFrame` total stays at 160 B (LifecycleFrame
    // drives the union size).
    pub fn dispatch(&self, message: Message<GenericHeader>) {
        let bag = match MessageBag::try_from(message) {
            Ok(bag) => bag,
            Err(e) => {
                tracing::warn!(shard = self.id, error = %e, "dropping message with invalid command");
                return;
            }
        };
        let (operation, namespace, generic) = extract_routing(bag);
        self.route_typed(operation, namespace, generic);
    }

    /// Route a consensus-control message (`StartViewChange`, `DoViewChange`,
    /// `StartView`, `Commit`) by hashing its raw `u64` consensus namespace.
    /// Every node uses the same hash so the shard owning the consensus
    /// group is deterministic across the cluster.
    pub(crate) fn route_consensus_control(
        &self,
        message: Message<GenericHeader>,
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
    ///    - `METADATA_CONSENSUS_NAMESPACE` -> shard 0.
    ///    - packable `IggyNamespace::inner()` -> the shard owning that
    ///      partition's consensus group.
    fn route_typed(
        &self,
        operation: Operation,
        namespace_u64: u64,
        generic: Message<GenericHeader>,
    ) {
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
            // yet", not "unroutable": fall back to the hash so replication
            // frames arriving during the post-commit convergence window are
            // not dropped (the partition plane re-checks materialisation on
            // the owning shard).
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
        if namespace_u64 == METADATA_CONSENSUS_NAMESPACE {
            self.try_send_to_target(0, generic, operation);
            return;
        }
        self.route_consensus_control(generic, namespace_u64, operation);
    }

    /// Send `message` into `senders[target]`. Honors the `io_uring` reactor
    /// constraint: never blocks; drops on `Full` / `Disconnected` and
    /// records the drop in `frame_drops_total{variant=consensus}`. VSR
    /// retransmit recovers consensus drops. A `target` past the end of
    /// `senders` (a stored `u16` from `shard_for`, not a trusted index)
    /// is dropped with `reason=unroutable` rather than panicking.
    /// Metadata frames always pass `target = 0` here, since `is_metadata`
    /// operations are owned by shard 0.
    fn try_send_to_target(
        &self,
        target: u16,
        message: Message<GenericHeader>,
        operation: Operation,
    ) {
        let Some(sender) = self.senders.get(target as usize) else {
            self.metrics
                .record_frame_drop(frame_drop_variant::CONSENSUS, frame_drop_reason::UNROUTABLE);
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
                    .record_frame_drop(frame_drop_variant::CONSENSUS, frame_drop_reason::FULL);
                tracing::warn!(
                    shard = self.id,
                    target,
                    ?operation,
                    "dispatch: shard inbox full, message dropped"
                );
            }
            Err(TrySendError::Disconnected(_)) => {
                self.metrics.record_frame_drop(
                    frame_drop_variant::CONSENSUS,
                    frame_drop_reason::DISCONNECTED,
                );
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
    #[allow(clippy::future_not_send)]
    pub async fn run_message_pump(&self, stop: Receiver<()>)
    where
        B: MessageBus + 'static,
        MJ: JournalHandle,
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: StateMachine<
                Input = Message<PrepareHeader>,
                Output = metadata::stm::result::ApplyReply,
                Error = iggy_common::IggyError,
            > + StreamsFrontend,
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
        let rearm_tick = || compio::time::sleep(CONSENSUS_TICK_INTERVAL).fuse();
        let mut consensus_tick = std::pin::pin!(rearm_tick());
        loop {
            futures::select! {
                _ = stop.recv().fuse() => break,
                () = consensus_tick.as_mut() => {
                    // Sharing the pump task is what keeps `tick_partitions`
                    // borrow-safe, but it bounds the tick's worst-case delay to
                    // one frame body's longest `.await` (replication append +
                    // commit_journal fsync/rotate + reply).
                    // TODO(hubcio): if a load test shows tick starvation,
                    // make `tick_partitions` borrow-free so the tick can be
                    // decoupled from the pump again without reintroducing the
                    // partition-ref-across-`.await` UB this fold closed.
                    self.tick_metadata().await;
                    self.tick_partitions().await;
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
                        }
                        Err(_) => break,
                    }
                }
            }
        }

        // Drain remaining frames so in-flight requests get a response.
        while let Ok(frame) = self.inbox.try_recv() {
            if self.accept_frame_for_self(&frame) {
                self.process_frame(frame).await;
                self.process_loopback(&mut loopback_buf, &mut namespace_scratch)
                    .await;
                self.apply_reconcile_ops();
            }
        }

        // Final flush: committed messages still resident in the in-memory
        // journal must reach segment storage before the process exits, or a
        // graceful restart recovers consumer offsets ahead of the data.
        self.flush_partitions().await;
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
        <MJ as JournalHandle>::Target: Journal<
                <MJ as JournalHandle>::Storage,
                Entry = Message<PrepareHeader>,
                Header = PrepareHeader,
            >,
        M: StateMachine<
                Input = Message<PrepareHeader>,
                Output = metadata::stm::result::ApplyReply,
                Error = iggy_common::IggyError,
            > + StreamsFrontend,
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
                // by server-ng) replies `None` on the carried sender if it
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
                // (wired by server-ng) reads this shard's `SessionManager`
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
                // server-ng) runs the read against this shard's partitions
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
                    let (segments, messages) = partition
                        .clean_expired_segments(now, message_expiry, max_bytes)
                        .await;
                    if segments > 0 {
                        tracing::debug!(
                            shard = self.id,
                            namespace_raw = namespace.inner(),
                            segments,
                            messages,
                            "segment cleaner removed sealed segments"
                        );
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
                    let (segments, messages) =
                        partition.remove_sealed_segments_up_to(up_to_offset).await;
                    if segments > 0 {
                        tracing::debug!(
                            shard = self.id,
                            namespace_raw = namespace.inner(),
                            segments,
                            messages,
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
                        Ok(()) => tracing::debug!(
                            shard = self.id,
                            namespace_raw = namespace.inner(),
                            generation,
                            "purge-partition reset partition to empty"
                        ),
                        Err(error) => tracing::error!(
                            shard = self.id,
                            namespace_raw = namespace.inner(),
                            generation,
                            %error,
                            "purge-partition failed to reset partition"
                        ),
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
