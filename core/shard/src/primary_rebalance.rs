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

//! The metadata primary coordinates one partition view change at a time.
//! Moves require fresh challenges, and a transfer additionally requires
//! the target's applied partition prefix. The grant then goes to the source
//! primary, because only its drain fences the replica where prepares start.
//! The source bounds that drain to `GRANT_TIMEOUT` after receipt. Authority
//! loss after receipt does not revoke an accepted grant, so the worst case is
//! one extra move.

use crate::shards_table::calculate_shard_assignment;
use crate::{
    IggyShard, LifecycleFrame, RestorableMetadataStm, ShardFrame, ShardsTable,
    dispatch_partition_journal_actions, dispatch_partition_wire_actions,
    refresh_partition_dvc_suffix, split_local_actions,
};
use consensus::{
    Consensus, MetadataHandle, PartitionsHandle, QUORUM_REPLICATION_MAX, VsrConsensus,
};
use iggy_binary_protocol::{
    Command, ConsensusHeader, HEADER_SIZE, PrepareHeader, PrimaryRebalanceHeader,
    PrimaryRebalanceKind,
};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use metadata::impls::metadata::PartitionPrimaryTransfer;
use partitions::{IggyPartition, PreparedPrimaryTransfer};
use server_common::Message;
use server_common::MessageBag;
use server_common::sharding::IggyNamespace;
use server_common::sharding::METADATA_GROUP;
use std::time::{Duration, Instant};

pub const PROBE_INTERVAL: Duration = Duration::from_secs(1);
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);
const STABLE_INTERVAL: Duration = Duration::from_secs(5);
const GRANT_TIMEOUT: Duration = Duration::from_secs(1);
const TRANSFER_TIMEOUT: Duration = Duration::from_secs(30);
const PROBES_PER_TICK: usize = 8;

#[cfg(test)]
mod tests;

pub struct PrimaryRebalance {
    view: Option<u32>,
    peers: Vec<Peer>,
    eligible: Vec<bool>,
    stable_since: Instant,
    next_move: Instant,
    cursor: Option<IggyNamespace>,
    pending: Option<PendingMove>,
    /// Identity and local receipt time of the newest accepted metadata probe.
    received_probe: Option<((u32, u64, u128), Instant)>,
    /// When this node's partition admission last opened under this authority.
    /// A probe sent before then carried no fresh quorum evidence.
    admission_opened: Option<Instant>,
}

#[derive(Clone, Default)]
struct Peer {
    nonce: Option<u128>,
    sent_at: Option<Instant>,
    ready_at: Option<Instant>,
    answered_at: Option<Instant>,
    /// The peer answered the newest probe that left this node, so it accepts no
    /// older probe after the next one.
    replied: bool,
}

struct PendingMove {
    plan: PartitionPrimaryTransfer,
    probe: PrimaryRebalanceHeader,
    phase: MovePhase,
    since: Instant,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum MovePhase {
    Capture,
    Probe,
    Transfer,
}

/// Only the metadata owner accepts wire frames. Partition owners receive
/// in-process probes and grants after that owner has checked their authority.
pub enum LocalRebalance {
    Wire(PrimaryRebalanceHeader),
    Capture(PrimaryRebalanceHeader),
    Probe(PrimaryRebalanceHeader),
    Transfer {
        namespace: IggyNamespace,
        request: PreparedPrimaryTransfer,
    },
}

impl PrimaryRebalance {
    fn observe_peer(&mut self, header: &PrimaryRebalanceHeader, now: Instant) {
        if self.view != Some(header.view) {
            return;
        }
        let Some(peer) = self.peers.get_mut(usize::from(header.replica)) else {
            return;
        };
        if peer.nonce != Some(header.nonce)
            || peer
                .sent_at
                .is_none_or(|sent| now.duration_since(sent) > PROBE_TIMEOUT)
        {
            return;
        }
        peer.nonce = None;
        peer.answered_at = peer.sent_at;
        peer.ready_at = (header.ready == 1).then_some(now);
        peer.replied = true;
    }

    /// A probe the bus refused never left this node, so nothing can answer it.
    /// The peer is probed again at the regular cadence, not after the timeout.
    fn forget_unsent(&mut self, replica: u8, nonce: u128) {
        if let Some(peer) = self.peers.get_mut(usize::from(replica))
            && peer.nonce == Some(nonce)
        {
            peer.nonce = None;
        }
    }

    /// A grant the bus refused never reached the source, so no view change can
    /// follow it. The move ends at once instead of holding the next one until
    /// the transfer timeout.
    fn forget_unsent_grant(&mut self, nonce: u128) {
        if self.pending.as_ref().is_some_and(|pending| {
            pending.phase == MovePhase::Transfer && pending.probe.nonce == nonce
        }) {
            self.pending = None;
        }
    }

    pub(crate) fn reset_authority(&mut self) {
        // Only a coordinator holding a view sets the fields cleared here.
        if self.view.is_none() {
            return;
        }
        // Authority can change without a new view. Keep replay protection and
        // the separately published deadline through that transition.
        *self = Self {
            received_probe: self.received_probe,
            ..Self::default()
        };
    }

    fn quorum_evidence(&self, quorum: usize) -> Option<Instant> {
        let required_peers = quorum.checked_sub(1)?;
        let mut newest = [None; QUORUM_REPLICATION_MAX - 1];
        for peer in &self.peers {
            let mut candidate = peer.answered_at;
            for rank in newest.iter_mut().take(required_peers) {
                if candidate > *rank {
                    std::mem::swap(rank, &mut candidate);
                }
            }
        }
        newest
            .get(required_peers.checked_sub(1)?)
            .copied()
            .flatten()
    }

    /// Records a probe newer than every accepted one and returns when the
    /// previous accepted probe of its view arrived. A primary advertises
    /// evidence only once the peer answered its previous probe, so that
    /// receipt precedes the evidence leaving the primary, however long this
    /// probe was in transit.
    fn accept_probe(&mut self, header: &PrimaryRebalanceHeader, now: Instant) -> Option<Instant> {
        // A restarted primary with retained metadata must advance the view.
        // An empty view-0 restart may lower its boot-seeded nonce, but cannot
        // serve a partition until its metadata commit advances.
        let identity = (header.view, header.commit, header.nonce);
        let previous = self.received_probe;
        if previous.is_some_and(|(previous, _)| identity <= previous) {
            return None;
        }
        self.received_probe = Some((identity, now));
        previous.and_then(|((view, ..), received)| (view == header.view).then_some(received))
    }

    fn matches_pending(
        &self,
        header: &PrimaryRebalanceHeader,
        phase: MovePhase,
        now: Instant,
    ) -> bool {
        self.view == Some(header.view)
            && self.pending.as_ref().is_some_and(|pending| {
                pending.phase == phase
                    && now.duration_since(pending.since) <= PROBE_TIMEOUT
                    && pending.probe.nonce == header.nonce
                    && pending.probe.namespace == header.namespace
                    && pending.probe.created_revision == header.created_revision
                    && pending.probe.partition_view == header.partition_view
                    && (phase == MovePhase::Capture || pending.probe.commit == header.commit)
            })
    }
}

impl Default for PrimaryRebalance {
    fn default() -> Self {
        Self {
            view: None,
            peers: Vec::new(),
            eligible: Vec::new(),
            stable_since: Instant::now(),
            next_move: Instant::now(),
            cursor: None,
            pending: None,
            received_probe: None,
            admission_opened: None,
        }
    }
}

impl<B, MJ, S, M, T, SB> IggyShard<B, MJ, S, M, T, SB>
where
    B: MessageBus + 'static,
    SB: SuperblockStore,
    MJ: JournalHandle,
    MJ::Target: Journal<Entry = Message<PrepareHeader>, Header = PrepareHeader>,
    M: RestorableMetadataStm,
    T: ShardsTable,
{
    pub(crate) fn fence_rebalance_authority(&self, message: &MessageBag) {
        if message.routing().1 != METADATA_GROUP {
            return;
        }
        let Some(consensus) = self.plane.metadata().consensus.as_ref() else {
            return;
        };
        // A vote for the current view cannot depose its normal primary. Late
        // votes keep arriving after a view change and must not reset readiness.
        let invalidates = match message {
            MessageBag::StartViewChange(message) => message.header().view > consensus.view(),
            MessageBag::DoViewChange(message) => message.header().view > consensus.view(),
            MessageBag::StartView(message) => message.header().view >= consensus.view(),
            MessageBag::RequestStartView(message) => message.header().view > consensus.view(),
            MessageBag::Commit(message) => message.header().view > consensus.view(),
            MessageBag::Prepare(message) => message.header().view > consensus.view(),
            MessageBag::RepairPrepare(message) => message.header().0.view > consensus.view(),
            _ => false,
        };
        // Drop authority before a metadata handler can suspend on persistence,
        // so no reply handled meanwhile grants a move or credits admission.
        if invalidates {
            self.primary_rebalance.borrow_mut().reset_authority();
        }
    }

    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    pub(crate) async fn tick_primary_rebalance(&self) {
        let metadata = self.plane.metadata();
        let Some(consensus) = metadata.consensus.as_ref() else {
            return;
        };
        if self.id != 0 || !consensus.is_primary() || !consensus_ready(consensus) {
            self.primary_rebalance.borrow_mut().reset_authority();
            metadata.observe_replica_readiness(&[]);
            return;
        }
        let mut outgoing = Vec::new();
        let mut capture = None;
        let now = self.bus.monotonic_now();
        {
            let mut state = self.primary_rebalance.borrow_mut();
            let replicas = usize::from(consensus.replica_count());
            if state.view != Some(consensus.view()) {
                *state = PrimaryRebalance {
                    view: Some(consensus.view()),
                    // Placement keeps a peer eligible until it answers not
                    // ready or its first probe times out.
                    peers: vec![
                        Peer {
                            ready_at: Some(now),
                            ..Peer::default()
                        };
                        replicas
                    ],
                    eligible: vec![false; replicas],
                    stable_since: now,
                    next_move: now,
                    received_probe: state.received_probe,
                    ..PrimaryRebalance::default()
                };
            }
            let mut changed = false;
            let admission_opened = state.admission_opened;
            for replica in 0..consensus.replica_count() {
                let index = usize::from(replica);
                let ready = replica == consensus.replica()
                    || state.peers[index]
                        .ready_at
                        .is_some_and(|ready| now.duration_since(ready) <= PROBE_TIMEOUT);
                if state.eligible[index] != ready {
                    state.eligible[index] = ready;
                    changed = true;
                }
                if replica == consensus.replica() || outgoing.len() == PROBES_PER_TICK {
                    continue;
                }
                let peer = &mut state.peers[index];
                let elapsed = peer.sent_at.map_or(PROBE_TIMEOUT + PROBE_INTERVAL, |sent| {
                    now.duration_since(sent)
                });
                // A backup that answered a probe sent before admission opened
                // would otherwise wait a full interval for its own credit. The
                // re-probe is stamped at or after the opening, so it runs once.
                let missed_opening = peer.nonce.is_none()
                    && admission_opened
                        .is_some_and(|opened| peer.sent_at.is_some_and(|sent| sent < opened));
                if !missed_opening
                    && ((peer.nonce.is_some() && elapsed <= PROBE_TIMEOUT)
                        || elapsed < PROBE_INTERVAL)
                {
                    continue;
                }
                let nonce = self.next_forward_nonce(consensus.replica());
                peer.replied &= peer.nonce.is_none();
                peer.nonce = Some(nonce);
                peer.sent_at = Some(now);
                outgoing.push((replica, probe_header(consensus, nonce)));
            }
            if changed {
                state.stable_since = now;
                state.pending = None;
                metadata.observe_replica_readiness(&state.eligible);
            }
            if let Some(pending) = state.pending.as_ref() {
                let elapsed = now.duration_since(pending.since);
                let current = metadata.partition_placement_view(
                    pending.plan.namespace,
                    pending.plan.created_revision,
                );
                let invalid = current.is_none_or(|current| current.view != pending.plan.view);
                let expired = elapsed
                    > match pending.phase {
                        MovePhase::Capture | MovePhase::Probe => PROBE_TIMEOUT,
                        // The coordinator cannot see whether the source used the
                        // grant. Planning before the new view shows up would
                        // count the moved primary twice.
                        MovePhase::Transfer => TRANSFER_TIMEOUT,
                    };
                if invalid || expired {
                    state.pending = None;
                }
            }
            if state.pending.is_none()
                && now >= state.next_move
                && now.duration_since(state.stable_since) >= STABLE_INTERVAL
            {
                state.next_move = now + PROBE_INTERVAL;
                if let Some(plan) =
                    metadata.primary_rebalance_candidate(&state.eligible, state.cursor)
                {
                    let mut probe =
                        probe_header(consensus, self.next_forward_nonce(consensus.replica()));
                    probe.kind = PrimaryRebalanceKind::SourceProbe;
                    probe.namespace = plan.namespace.inner();
                    probe.created_revision = plan.created_revision;
                    probe.partition_view = plan.view;
                    state.cursor = Some(plan.namespace);
                    state.pending = Some(PendingMove {
                        plan,
                        probe,
                        phase: MovePhase::Capture,
                        since: now,
                    });
                    capture = Some(probe);
                }
            }
        }
        if let Some(probe) = capture {
            self.send_rebalance(consensus.primary_index(probe.partition_view), probe)
                .await;
        }
        for (replica, probe) in outgoing {
            if !self.send_rebalance(replica, probe).await {
                self.primary_rebalance
                    .borrow_mut()
                    .forget_unsent(replica, probe.nonce);
            }
        }
    }

    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    pub(crate) async fn on_primary_rebalance(&self, header: PrimaryRebalanceHeader) {
        let Some(consensus) = self.plane.metadata().consensus.as_ref() else {
            return;
        };
        if self.id != 0
            || header.cluster != consensus.cluster()
            || header.replica >= consensus.replica_count()
            || header.view != consensus.view()
        {
            return;
        }
        let now = self.bus.monotonic_now();
        match header.kind {
            PrimaryRebalanceKind::MetadataProbe
            | PrimaryRebalanceKind::PartitionProbe
            | PrimaryRebalanceKind::SourceProbe => {
                if header.replica != consensus.primary_index(header.view) {
                    return;
                }
                if header.kind != PrimaryRebalanceKind::MetadataProbe && consensus_ready(consensus)
                {
                    self.send_local_rebalance(
                        self.rebalance_partition_shard(header.namespace),
                        if header.kind == PrimaryRebalanceKind::SourceProbe {
                            LocalRebalance::Capture(header)
                        } else {
                            LocalRebalance::Probe(header)
                        },
                    );
                    return;
                }
                if header.kind == PrimaryRebalanceKind::MetadataProbe {
                    self.credit_backup_freshness(consensus, &header, now);
                }
                let mut reply = header;
                if header.kind == PrimaryRebalanceKind::MetadataProbe {
                    reply.commit = 0;
                    reply.quorum_age_ns = PrimaryRebalanceHeader::NO_QUORUM_EVIDENCE;
                }
                reply.replica = consensus.replica();
                reply.kind = match header.kind {
                    PrimaryRebalanceKind::MetadataProbe => PrimaryRebalanceKind::MetadataReply,
                    PrimaryRebalanceKind::SourceProbe => PrimaryRebalanceKind::SourceReply,
                    _ => PrimaryRebalanceKind::PartitionReply,
                };
                reply.ready = u8::from(
                    header.kind == PrimaryRebalanceKind::MetadataProbe
                        && consensus_ready(consensus),
                );
                self.send_rebalance(header.replica, reply).await;
            }
            PrimaryRebalanceKind::MetadataReply => {
                if consensus.is_primary() && consensus_ready(consensus) {
                    let evidence = {
                        let mut state = self.primary_rebalance.borrow_mut();
                        state.observe_peer(&header, now);
                        state.quorum_evidence(consensus.quorum_replication())
                    };
                    if let Some(sent) = evidence
                        && self.credit_partition_admission(
                            consensus,
                            now,
                            now.saturating_duration_since(sent),
                        )
                    {
                        self.primary_rebalance.borrow_mut().admission_opened = Some(now);
                    }
                }
            }
            PrimaryRebalanceKind::SourceReply => {
                if !consensus.is_primary() || !consensus_ready(consensus) {
                    return;
                }
                let outgoing = {
                    let mut state = self.primary_rebalance.borrow_mut();
                    if !state.matches_pending(&header, MovePhase::Capture, now)
                        || header.replica != consensus.primary_index(header.partition_view)
                    {
                        return;
                    }
                    if header.ready == 0 {
                        state.pending = None;
                        return;
                    }
                    let Some(pending) = state.pending.as_mut() else {
                        return;
                    };
                    pending.probe.commit = header.commit;
                    pending.probe.kind = PrimaryRebalanceKind::PartitionProbe;
                    pending.phase = MovePhase::Probe;
                    pending.since = now;
                    (pending.plan.target, pending.probe)
                };
                self.send_rebalance(outgoing.0, outgoing.1).await;
            }
            PrimaryRebalanceKind::PartitionReply => {
                if !consensus.is_primary() || !consensus_ready(consensus) {
                    return;
                }
                let outgoing = {
                    let mut state = self.primary_rebalance.borrow_mut();
                    if !state.matches_pending(&header, MovePhase::Probe, now)
                        || state
                            .pending
                            .as_ref()
                            .is_none_or(|pending| pending.plan.target != header.replica)
                    {
                        return;
                    }
                    if header.ready == 0 {
                        state.pending = None;
                        return;
                    }
                    let Some(pending) = state.pending.as_mut() else {
                        return;
                    };
                    pending.probe.kind = PrimaryRebalanceKind::PrimaryTransfer;
                    pending.probe.target = pending.plan.target;
                    pending.phase = MovePhase::Transfer;
                    pending.since = now;
                    (consensus.primary_index(pending.plan.view), pending.probe)
                };
                if !self.send_rebalance(outgoing.0, outgoing.1).await {
                    self.primary_rebalance
                        .borrow_mut()
                        .forget_unsent_grant(outgoing.1.nonce);
                }
            }
            PrimaryRebalanceKind::PrimaryTransfer => {
                if header.replica != consensus.primary_index(header.view)
                    || !consensus_ready(consensus)
                {
                    return;
                }
                self.send_local_rebalance(
                    self.rebalance_partition_shard(header.namespace),
                    LocalRebalance::Transfer {
                        namespace: IggyNamespace::from_raw(header.namespace),
                        request: PreparedPrimaryTransfer {
                            created_revision: header.created_revision,
                            expected_view: header.partition_view,
                            target: header.target,
                            expires_at: now + GRANT_TIMEOUT,
                        },
                    },
                );
            }
        }
    }

    #[allow(clippy::future_not_send)]
    pub(crate) async fn on_local_rebalance(&self, frame: LocalRebalance) {
        match frame {
            LocalRebalance::Wire(header) => self.on_primary_rebalance(header).await,
            LocalRebalance::Capture(mut header) | LocalRebalance::Probe(mut header) => {
                let capture = matches!(frame, LocalRebalance::Capture(_));
                let namespace = IggyNamespace::from_raw(header.namespace);
                let partition = self.plane.partitions().get_by_ns(&namespace);
                header.ready = u8::from(partition.is_some_and(|partition| {
                    self.rebalance_incarnation_current(namespace, header.created_revision)
                        && if capture {
                            partition_history_ready(partition, &header)
                                && partition.consensus().is_primary()
                        } else {
                            partition_ready(partition, &header)
                        }
                }));
                if let Some(partition) = partition {
                    let coordinator = header.replica;
                    header.replica = partition.consensus().replica();
                    header.kind = if capture {
                        header.commit = partition.consensus().commit_max();
                        PrimaryRebalanceKind::SourceReply
                    } else {
                        PrimaryRebalanceKind::PartitionReply
                    };
                    self.send_rebalance(coordinator, header).await;
                }
            }
            LocalRebalance::Transfer { namespace, request } => {
                if self.rebalance_incarnation_current(namespace, request.created_revision)
                    && let Some(partition) = self.plane.partitions().get_mut_by_ns(&namespace)
                {
                    partition.defer_primary_transfer(request);
                }
            }
        }
    }

    #[allow(clippy::future_not_send)]
    pub(crate) async fn apply_primary_transfer(
        &self,
        namespace: IggyNamespace,
        request: PreparedPrimaryTransfer,
    ) {
        if !self.rebalance_incarnation_current(namespace, request.created_revision) {
            return;
        }
        let Some(partition) = self.plane.partitions().get_mut_by_ns(&namespace) else {
            return;
        };
        if partition.created_revision() != request.created_revision
            || partition.fatal().is_some()
            || partition.requires_state_transfer()
            || partition.history_is_busy()
            || self.bus.monotonic_now() >= request.expires_at
        {
            return;
        }
        refresh_partition_dvc_suffix(partition);
        let consensus = partition.consensus();
        let actions = consensus.request_primary_transfer(request.expected_view, request.target);
        if actions.is_empty() {
            return;
        }
        tracing::info!(
            namespace_raw = namespace.inner(),
            created_revision = request.created_revision,
            previous_view = request.expected_view,
            view = consensus.view(),
            target = request.target,
            "rebalancing partition primary after target readiness confirmation"
        );
        let (local, wire) = split_local_actions(actions);
        dispatch_partition_journal_actions(consensus, partition, &local).await;
        dispatch_partition_wire_actions::<B, _, MJ, _>(consensus, partition, wire).await;
    }

    fn rebalance_incarnation_current(&self, namespace: IggyNamespace, revision: u64) -> bool {
        self.shards_table.epoch_for(namespace) == Some(revision)
            && !self
                .plane
                .metadata()
                .mux_stm
                .streams()
                .lifecycle_blocks(namespace.stream_id(), Some(namespace.topic_id()))
            && self
                .plane
                .metadata()
                .mux_stm
                .streams()
                .created_revision_for_namespace(namespace)
                == Some(revision)
    }

    fn rebalance_partition_shard(&self, raw: u64) -> u16 {
        let namespace = IggyNamespace::from_raw(raw);
        self.shards_table
            .shard_for(namespace)
            .unwrap_or_else(|| calculate_shard_assignment(&namespace, self.shard_count))
    }

    fn send_local_rebalance(&self, shard: u16, frame: LocalRebalance) {
        let Some(sender) = self.senders.get(usize::from(shard)) else {
            self.metrics.record_frame_drop(
                crate::metrics::frame_drop_variant::PRIMARY_REBALANCE,
                crate::metrics::frame_drop_reason::UNROUTABLE,
            );
            return;
        };
        // Every phase is bounded by a coordinator deadline; a full inbox
        // abandons this attempt without retaining another queue of work.
        if let Err(error) = sender.try_send(ShardFrame::lifecycle(
            LifecycleFrame::PrimaryRebalance(Box::new(frame)),
        )) {
            self.metrics.record_frame_drop(
                crate::metrics::frame_drop_variant::PRIMARY_REBALANCE,
                crate::coordinator::classify_try_send_err(&error),
            );
        }
    }

    fn credit_backup_freshness(
        &self,
        consensus: &VsrConsensus<B>,
        header: &PrimaryRebalanceHeader,
        now: Instant,
    ) {
        if header.replica == consensus.replica() {
            return;
        }
        let Some(previous_received) = self
            .primary_rebalance
            .borrow_mut()
            .accept_probe(header, now)
        else {
            return;
        };
        if header.quorum_age_ns == PrimaryRebalanceHeader::NO_QUORUM_EVIDENCE {
            return;
        }
        let floor = match consensus.adopted_view_head() {
            Some((view, head)) if view == header.view => head,
            None if header.view == 0 => 0,
            _ => return,
        };
        if header.commit >= floor && self.plane.metadata().applied_frontier().get() >= header.commit
        {
            self.credit_partition_admission(
                consensus,
                now,
                Duration::from_nanos(header.quorum_age_ns)
                    .saturating_add(now.saturating_duration_since(previous_received)),
            );
        }
    }

    fn credit_partition_admission(
        &self,
        consensus: &VsrConsensus<B>,
        now: Instant,
        evidence_age: Duration,
    ) -> bool {
        let opened = self
            .plane
            .metadata()
            .applied_frontier()
            .credit_partition_freshness(now, evidence_age);
        if opened {
            tracing::info!(
                replica = consensus.replica(),
                view = consensus.view(),
                ?evidence_age,
                "partition admission opened"
            );
        }
        opened
    }

    /// Returns false when the frame did not leave this node.
    #[allow(clippy::future_not_send)]
    async fn send_rebalance(&self, target: u8, mut header: PrimaryRebalanceHeader) -> bool {
        if header.kind == PrimaryRebalanceKind::MetadataProbe {
            let Some(consensus) = self.plane.metadata().consensus.as_ref() else {
                return false;
            };
            if header.view != consensus.view()
                || !consensus.is_primary()
                || !consensus_ready(consensus)
            {
                return false;
            }
            header.commit = self.plane.metadata().applied_frontier().get();
            let evidence = {
                let state = self.primary_rebalance.borrow();
                state
                    .peers
                    .get(usize::from(target))
                    .filter(|peer| peer.replied)
                    .and_then(|_| state.quorum_evidence(consensus.quorum_replication()))
            };
            header.quorum_age_ns =
                evidence.map_or(PrimaryRebalanceHeader::NO_QUORUM_EVIDENCE, |sent| {
                    u64::try_from(
                        self.bus
                            .monotonic_now()
                            .saturating_duration_since(sent)
                            .as_nanos(),
                    )
                    .unwrap_or(PrimaryRebalanceHeader::NO_QUORUM_EVIDENCE)
                });
        }
        if target == header.replica {
            self.send_local_rebalance(0, LocalRebalance::Wire(header));
            return true;
        }
        header.seal();
        let message = Message::<PrimaryRebalanceHeader>::new(HEADER_SIZE)
            .transmute_header(|_, output: &mut PrimaryRebalanceHeader| *output = header);
        self.bus
            .send_to_replica(target, message.into_generic().into_frozen())
            .await
            .is_ok()
    }
}

pub fn consensus_ready<B: MessageBus>(consensus: &VsrConsensus<B>) -> bool {
    consensus.is_normal()
        && !consensus.has_ceded_primaryship()
        && !consensus.has_observed_newer_view()
        && !consensus.is_transferring()
        && !consensus.view_log_unproven()
        && consensus.applied_view_start()
        && consensus.commit_min() >= consensus.recovery_barrier()
}

fn partition_ready<B: MessageBus, SB: SuperblockStore>(
    partition: &IggyPartition<B, SB>,
    probe: &PrimaryRebalanceHeader,
) -> bool {
    partition_history_ready(partition, probe)
        && partition.consensus().commit_min() >= probe.commit
        && partition.consensus().commit_min() == partition.consensus().commit_max()
        && !partition.history_is_busy()
}

fn partition_history_ready<B: MessageBus, SB: SuperblockStore>(
    partition: &IggyPartition<B, SB>,
    probe: &PrimaryRebalanceHeader,
) -> bool {
    partition.created_revision() == probe.created_revision
        && partition.consensus().view() == probe.partition_view
        && consensus_ready(partition.consensus())
        && partition.fatal().is_none()
        && !partition.requires_state_transfer()
        && !partition.read_history_is_changing()
}

#[expect(
    clippy::cast_possible_truncation,
    reason = "HEADER_SIZE is fixed at 256 bytes"
)]
const fn probe_header<B: MessageBus>(
    consensus: &VsrConsensus<B>,
    nonce: u128,
) -> PrimaryRebalanceHeader {
    PrimaryRebalanceHeader {
        checksum: 0,
        checksum_body: 0,
        cluster: consensus.cluster(),
        size: HEADER_SIZE as u32,
        view: consensus.view(),
        release: 0,
        command: Command::PrimaryRebalance,
        replica: consensus.replica(),
        reserved_frame: [0; 66],
        nonce,
        namespace: 0,
        created_revision: 0,
        commit: 0,
        quorum_age_ns: PrimaryRebalanceHeader::NO_QUORUM_EVIDENCE,
        partition_view: 0,
        kind: PrimaryRebalanceKind::MetadataProbe,
        ready: 0,
        target: 0,
        reserved: [0; 73],
    }
}
