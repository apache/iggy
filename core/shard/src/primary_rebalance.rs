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

//! The metadata primary coordinates partition view changes in rounds of up to
//! `REBALANCE_MOVES_INFLIGHT_MAX` moves.
//! Moves require fresh challenges, and a transfer additionally requires
//! the target's applied partition prefix. The grant then goes to the source
//! primary, because only its drain fences the replica where prepares start.
//! The source bounds that drain to `GRANT_TIMEOUT` after receipt. Authority
//! loss after receipt does not revoke an accepted grant, so the worst case is
//! one extra move per grant that left.
//!
//! A node that leaves needs no coordinator: each of its shards hands the
//! partitions it leads to a member that keeps up, through the same drain, and
//! the node refuses every probe so that no primary comes back to it.

use crate::commit_batch;
use crate::shards_table::calculate_shard_assignment;
use crate::{
    IggyShard, LifecycleFrame, RestorableMetadataStm, ShardFrame, ShardsTable,
    dispatch_partition_journal_actions, dispatch_partition_wire_actions,
    refresh_partition_dvc_suffix, split_local_actions,
};
use consensus::{
    Consensus, MetadataHandle, PartitionsHandle, QUORUM_REPLICATION_MAX, Status, VsrConsensus,
};
use iggy_binary_protocol::primitives::partition_status::{
    PartitionStatusEntry, encode_status_entries,
};
use iggy_binary_protocol::{
    Command, CommitEntry, ConsensusHeader, HEADER_SIZE, PrepareHeader, PrimaryRebalanceHeader,
    PrimaryRebalanceKind, StartViewChangeHeader,
};
use journal::superblock::SuperblockStore;
use journal::{Journal, JournalHandle};
use message_bus::MessageBus;
use metadata::impls::metadata::PartitionPrimaryTransfer;
use metadata::stm::stream::{PartitionReplication, PartitionReplicationPass};
use partitions::{IggyPartition, PreparedPrimaryTransfer};
use server_common::Message;
use server_common::MessageBag;
use server_common::sharding::IggyNamespace;
use server_common::sharding::METADATA_GROUP;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::ops::Bound;
use std::time::{Duration, Instant};

pub const PROBE_INTERVAL: Duration = Duration::from_secs(1);
const PROBE_TIMEOUT: Duration = Duration::from_secs(3);
const STABLE_INTERVAL: Duration = Duration::from_secs(5);
const GRANT_TIMEOUT: Duration = Duration::from_secs(1);
const TRANSFER_TIMEOUT: Duration = Duration::from_secs(30);
const PROBES_PER_TICK: usize = 8;
/// A moved primary that keeps its partition this long counts as held. A backup
/// that cannot follow the target loses the view inside it: one view-change
/// status timeout to give up on the view, one more to escalate past it. Sized
/// for the default 5 s timeouts. Also the first wait after a failed move.
const MOVE_HOLD_INTERVAL: Duration = Duration::from_secs(30);
/// Caps the wait after repeated failed moves, so a target that recovered gets
/// primaries back within minutes.
const TARGET_BACKOFF_MAX: Duration = Duration::from_secs(240);
/// Partition reports one metadata probe or reply carries.
const STATUS_ENTRIES_PER_FRAME: usize = 512;
/// Moves the coordinator has planned, probed or granted at one time. A move
/// ends with the report of its new view, and one reply of the target carries
/// `STATUS_ENTRIES_PER_FRAME` reports per probe interval, so a larger round
/// would only wait for its reports.
const REBALANCE_MOVES_INFLIGHT_MAX: usize = STATUS_ENTRIES_PER_FRAME;
/// Directory reports one probe batch visits while skipping those of deleted
/// incarnations.
const STATUS_ENTRIES_SCANNED_PER_FRAME: usize = 4 * STATUS_ENTRIES_PER_FRAME;
/// Nodes the in-sync mask of a partition can name.
const HANDOFF_NODES: usize = u32::BITS as usize;
/// Directory reports one retain pass checks under the directory lock.
const DIRECTORY_RETAIN_CHUNK: usize = 1024;
/// Partitions one tick of the replication count visits, so its read stays
/// short at any cluster size.
const PARTITION_REPLICATION_CHUNK: usize = 1024;

/// Shard 0: this node's own partition reports for its metadata replies. Its
/// shards report each change and retract each partition they stop leading,
/// so a report stays until it changes or a reply carries its retraction.
#[derive(Debug, Default)]
pub struct PartitionStatusOutbox {
    entries: RefCell<BTreeMap<u64, OutboxEntry>>,
    /// Namespaces whose incarnation, view or in-sync nodes changed since a
    /// reply last carried them, oldest first, each once.
    changes: RefCell<VecDeque<u64>>,
    cursor: Cell<Option<u64>>,
}

#[derive(Debug)]
struct OutboxEntry {
    status: PartitionStatusEntry,
    /// Waits in `changes`.
    changed: bool,
}

/// Where the outbox stood before a reply that may not leave this node.
#[derive(Debug, Clone, Copy)]
pub struct OutboxUnsent {
    /// How many changes lead the reply.
    changes_sent: usize,
    cursor: Option<u64>,
}

impl PartitionStatusOutbox {
    pub fn record(&self, statuses: &[PartitionStatusEntry]) {
        let mut entries = self.entries.borrow_mut();
        let mut changes = self.changes.borrow_mut();
        for status in statuses {
            let mut moved = true;
            let entry = entries
                .entry(status.namespace)
                .and_modify(|held| {
                    moved = (
                        held.status.created_revision,
                        held.status.view,
                        held.status.in_sync_nodes,
                    ) != (status.created_revision, status.view, status.in_sync_nodes);
                })
                .or_insert(OutboxEntry {
                    status: *status,
                    changed: false,
                });
            entry.status = *status;
            if moved && !entry.changed {
                entry.changed = true;
                changes.push_back(status.namespace);
            }
        }
    }

    /// Up to `limit` reports: the changes first, oldest first, then the cycle
    /// after the cursor, wrapping, advancing it. A retraction leaves once it
    /// goes out as a change. Returns where the outbox stood before, for a
    /// reply that does not leave this node.
    pub fn next_batch(&self, limit: usize) -> (Vec<PartitionStatusEntry>, OutboxUnsent) {
        let mut entries = self.entries.borrow_mut();
        let mut changes = self.changes.borrow_mut();
        let mut batch = Vec::new();
        while batch.len() < limit
            && let Some(namespace) = changes.pop_front()
        {
            let Some(entry) = entries.get_mut(&namespace) else {
                continue;
            };
            entry.changed = false;
            batch.push(entry.status);
            if entry.status.in_sync_nodes == 0 {
                entries.remove(&namespace);
            }
        }
        let changes_sent = batch.len();
        let after = self.cursor.get();
        let start = after.map_or(Bound::Unbounded, Bound::Excluded);
        let end = after.map_or(Bound::Excluded(0), Bound::Included);
        batch.extend(
            entries
                .range((start, Bound::Unbounded))
                .chain(entries.range((Bound::Unbounded, end)))
                .take((limit - changes_sent).min(entries.len()))
                .map(|(_, entry)| entry.status),
        );
        if batch.len() > changes_sent
            && let Some(last) = batch.last()
        {
            self.cursor.set(Some(last.namespace));
        }
        let unsent = OutboxUnsent {
            changes_sent,
            cursor: after,
        };
        (batch, unsent)
    }

    /// Undo a `batch` that did not leave this node: its changes lead the next
    /// batch again, and the cycle resumes where it stood.
    pub fn forget_unsent(&self, batch: &[PartitionStatusEntry], unsent: OutboxUnsent) {
        let mut entries = self.entries.borrow_mut();
        let mut changes = self.changes.borrow_mut();
        for status in batch.iter().take(unsent.changes_sent).rev() {
            let entry = entries.entry(status.namespace).or_insert(OutboxEntry {
                status: *status,
                changed: false,
            });
            if !entry.changed {
                entry.changed = true;
                changes.push_front(status.namespace);
            }
        }
        self.cursor.set(unsent.cursor);
    }

    /// The partitions this node leads, as its shards last reported them, and
    /// those whose retraction no reply carried yet.
    pub fn namespaces(&self) -> Vec<u64> {
        self.entries.borrow().keys().copied().collect()
    }
}

#[cfg(test)]
mod tests;

pub struct PrimaryRebalance {
    view: Option<u32>,
    peers: Vec<Peer>,
    eligible: Vec<bool>,
    /// Every peer was heard from in this view, so placement got `eligible`
    /// rather than no evidence.
    readiness_known: bool,
    /// The cluster's partitions short of their factor or of a write quorum,
    /// as of the last finished pass.
    replication: PartitionReplication,
    /// The nodes the directory last narrowed in-sync reports to.
    live_nodes: u32,
    replication_pass: PartitionReplicationPass,
    /// When this node started coordinating `view`.
    view_since: Instant,
    next_quorum_scan: Instant,
    stable_since: Instant,
    next_move: Instant,
    cursor: Option<IggyNamespace>,
    /// Moves planned, probed or granted whose new view no report shows yet.
    pending: BTreeMap<IggyNamespace, PendingMove>,
    /// Identity and local receipt time of the newest accepted metadata probe.
    received_probe: Option<((u32, u64, u128), Instant)>,
    /// When this node's partition admission last opened under this authority.
    /// A probe sent before then carried no fresh quorum evidence.
    admission_opened: Option<Instant>,
    /// Moves a source carried out, watched until the target holds the
    /// partition for `MOVE_HOLD_INTERVAL` or loses it to a view change.
    moves: Vec<MovedPrimary>,
    /// A round fills `moves` with thousands of entries, so they are judged
    /// once per probe interval instead of on every tick.
    next_watch: Instant,
    /// Per replica. A target ready for a move can still be one that a backup
    /// cannot hear in time, and only a lost view shows it. Moving to it again
    /// at once would repeat the view changes forever.
    backoffs: Vec<TargetBackoff>,
}

/// How far a node that leaves got in giving up the partitions it leads.
#[derive(Debug, Default)]
pub enum PrimaryHandoff {
    /// The node stays.
    #[default]
    Off,
    /// One pass over the partitions the shard held when the node began to
    /// leave.
    Walking {
        /// Partitions the pass has not visited.
        remaining: Vec<IggyNamespace>,
        /// Partitions whose handoff started and still needs this node, each
        /// with the time after which the pass stops waiting for it.
        handed_off: Vec<(IggyNamespace, Instant)>,
        /// Partitions each node took, so the members of a group share them.
        given: [u32; HANDOFF_NODES],
        /// Peers that hand a partition to this node, one bit per node. They
        /// leave too, so they get none.
        leaving: u32,
    },
    /// The pass visited every partition, and its view changes ended.
    Done,
}

struct MovedPrimary {
    plan: PartitionPrimaryTransfer,
    since: Instant,
}

#[derive(Clone, Copy, Default)]
struct TargetBackoff {
    until: Option<Instant>,
    wait: Duration,
}

impl TargetBackoff {
    fn blocks(self, now: Instant) -> bool {
        self.until.is_some_and(|until| now < until)
    }

    /// Moves watched together often fail together, for example when the target
    /// restarts, so a failure inside a running wait counts once.
    fn record_failure(&mut self, now: Instant) {
        if self.blocks(now) {
            return;
        }
        self.wait = (self.wait * 2).clamp(MOVE_HOLD_INTERVAL, TARGET_BACKOFF_MAX);
        self.until = Some(now + self.wait);
    }
}

#[derive(Clone, Default)]
struct Peer {
    nonce: Option<u128>,
    sent_at: Option<Instant>,
    ready_at: Option<Instant>,
    /// When the peer last reported that it can tier topics.
    tiered_storage_ready_at: Option<Instant>,
    /// When the peer last answered a probe of this view, ready or not.
    live_at: Option<Instant>,
    answered_at: Option<Instant>,
    /// The peer answered a probe of this view, or one timed out or never left
    /// this node, so `ready_at` reflects the peer rather than the view's start.
    heard: bool,
    /// The peer answered the newest probe that left this node, so it accepts no
    /// older probe after the next one.
    replied: bool,
    /// The last partition report sent to this peer.
    status_cursor: Option<u64>,
    /// The last directory change sent to this peer.
    changes_sent: u64,
}

/// Where a peer's reports stood before a probe that may not leave this node.
#[derive(Debug, Clone, Copy, Default)]
struct StatusCursors {
    status_cursor: Option<u64>,
    changes_sent: u64,
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
    /// On the metadata primary's first shard, the cluster's partitions short
    /// of their factor or of a write quorum. Zero elsewhere, and until the
    /// first pass of this view ends, which waits until every peer was heard
    /// from.
    pub(crate) const fn partition_replication(&self) -> PartitionReplication {
        self.replication
    }

    /// Whether `header` answers the peer's outstanding probe.
    fn observe_peer(&mut self, header: &PrimaryRebalanceHeader, now: Instant) -> bool {
        if self.view != Some(header.view) {
            return false;
        }
        let Some(peer) = self.peers.get_mut(usize::from(header.replica)) else {
            return false;
        };
        if peer.nonce != Some(header.nonce)
            || peer
                .sent_at
                .is_none_or(|sent| now.duration_since(sent) > PROBE_TIMEOUT)
        {
            return false;
        }
        peer.nonce = None;
        peer.answered_at = peer.sent_at;
        peer.ready_at = (header.ready == 1).then_some(now);
        peer.tiered_storage_ready_at = (header.tiered_storage_ready == 1).then_some(now);
        peer.live_at = Some(now);
        peer.heard = true;
        peer.replied = true;
        true
    }

    /// A probe the bus refused never left this node, so nothing can answer it.
    /// The peer is probed again at the regular cadence, not after the timeout,
    /// and gets the reports that probe carried in the next one.
    fn forget_unsent(&mut self, replica: u8, nonce: u128, unsent: StatusCursors) {
        if let Some(peer) = self.peers.get_mut(usize::from(replica)) {
            if peer.nonce == Some(nonce) {
                peer.nonce = None;
                peer.heard = true;
            }
            peer.status_cursor = unsent.status_cursor;
            peer.changes_sent = unsent.changes_sent;
        }
    }

    /// A grant the bus refused never reached the source, so no view change can
    /// follow it. The move ends at once instead of holding its place until the
    /// transfer timeout.
    fn forget_unsent_grant(&mut self, namespace: IggyNamespace, nonce: u128) {
        if self.pending.get(&namespace).is_some_and(|pending| {
            pending.phase == MovePhase::Transfer && pending.probe.nonce == nonce
        }) {
            self.pending.remove(&namespace);
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
            && self
                .pending
                .get(&IggyNamespace::from_raw(header.namespace))
                .is_some_and(|pending| {
                    pending.phase == phase
                        && now.duration_since(pending.since) <= PROBE_TIMEOUT
                        && pending.probe.nonce == header.nonce
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
            readiness_known: false,
            replication: PartitionReplication::default(),
            live_nodes: u32::MAX,
            replication_pass: PartitionReplicationPass::default(),
            view_since: Instant::now(),
            next_quorum_scan: Instant::now(),
            stable_since: Instant::now(),
            next_move: Instant::now(),
            cursor: None,
            pending: BTreeMap::new(),
            received_probe: None,
            admission_opened: None,
            moves: Vec::new(),
            next_watch: Instant::now(),
            backoffs: Vec::new(),
        }
    }
}

impl<B, MJ, S, M, T, SB> IggyShard<B, MJ, S, M, T, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    /// A node that leaves gives up the partitions this shard leads before its
    /// bus shuts down, so that no group waits for a heartbeat timeout. The
    /// pump does the work on its ticks, see `tick_primary_handoff`.
    pub fn begin_primary_handoff(&self) {
        let mut handoff = self.primary_handoff.borrow_mut();
        if matches!(*handoff, PrimaryHandoff::Off) {
            *handoff = PrimaryHandoff::Walking {
                remaining: self.plane.partitions().namespaces().copied().collect(),
                handed_off: Vec::new(),
                given: [0; HANDOFF_NODES],
                leaving: 0,
            };
        }
    }

    /// Whether the handoff still has a partition to visit or a view change of
    /// its own to wait for.
    pub fn primary_handoff_pending(&self) -> bool {
        matches!(
            *self.primary_handoff.borrow(),
            PrimaryHandoff::Walking { .. }
        )
    }

    /// A node that leaves takes no primary: not from a move, and not as the
    /// first primary of a new partition.
    fn is_leaving(&self) -> bool {
        !matches!(*self.primary_handoff.borrow(), PrimaryHandoff::Off)
    }

    /// Takes the sender of `vote` for a node that leaves, if it is the primary
    /// of the group and its vote gives the group to this node.
    ///
    /// The coordinator moves no primary to a node that leaves, so a primary
    /// that gives a group to this node during the pass hands it off: that peer
    /// leaves too, and it gets no partition from this node. Without this,
    /// nodes that stop at the same time hand their partitions to each other
    /// until the timeout.
    ///
    /// A move granted just before this node began to leave, a primary that
    /// steps down for another reason, or a view that this replica missed makes
    /// a peer that stays look like one that leaves. If no other target is
    /// left, the partitions of this shard wait for a heartbeat timeout, as
    /// they did before the handoff existed.
    pub(crate) fn note_group_given_up(
        &self,
        consensus: &VsrConsensus<B>,
        vote: &StartViewChangeHeader,
    ) {
        let mut handoff = self.primary_handoff.borrow_mut();
        let PrimaryHandoff::Walking { leaving, .. } = &mut *handoff else {
            return;
        };
        // The first test also proves that `vote.replica` is a member.
        if consensus.primary_index(consensus.log_view()) == vote.replica
            && consensus.is_primary_for_view(vote.view)
        {
            *leaving |= 1_u32
                .checked_shl(u32::from(consensus.member_node(vote.replica)))
                .unwrap_or(0);
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
            MessageBag::Commit(message) => commit_batch::verified_entries::<_, CommitEntry>(
                message,
                message.header().checksum_body,
            )
            .is_some_and(|entries| {
                entries
                    .iter()
                    .any(|entry| entry.group == METADATA_GROUP && entry.view > consensus.view())
            }),
            MessageBag::Prepare(message) => message.header().view > consensus.view(),
            MessageBag::RepairPrepare(message) => message.header().0.view > consensus.view(),
            _ => false,
        };
        // Drop authority before a metadata handler can suspend on persistence,
        // so no reply handled meanwhile grants a move or credits admission.
        if invalidates {
            self.primary_rebalance.borrow_mut().reset_authority();
            self.plane
                .metadata()
                .observe_tiered_storage_readiness(false);
        }
    }

    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    pub(crate) async fn tick_primary_rebalance(&self) {
        let metadata = self.plane.metadata();
        let Some(consensus) = metadata.consensus.as_ref() else {
            return;
        };
        if self.id == 0 {
            self.retain_partition_directories(self.bus.monotonic_now());
        }
        if self.id != 0 || !consensus.is_primary() || !consensus_ready(consensus) {
            let mut state = self.primary_rebalance.borrow_mut();
            state.reset_authority();
            state.live_nodes = u32::MAX;
            state.replication = PartitionReplication::default();
            state.replication_pass = PartitionReplicationPass::default();
            drop(state);
            metadata.observe_replica_readiness(&[]);
            metadata.observe_tiered_storage_readiness(false);
            if self.id == 0 {
                metadata
                    .mux_stm
                    .streams()
                    .set_partition_directory_ready_nodes(u32::MAX);
            }
            return;
        }
        let mut outgoing = Vec::new();
        let mut captures = Vec::new();
        let now = self.bus.monotonic_now();
        {
            let mut state = self.primary_rebalance.borrow_mut();
            let replicas = usize::from(consensus.replica_count());
            if state.view != Some(consensus.view()) {
                let streams = metadata.mux_stm.streams();
                let changes_sent = streams.partition_status_last_change();
                // The failover that made this node metadata primary can also
                // have given it partitions, and its shards may have reported
                // them before this view started. No metadata primary relayed
                // those, so this node's own reports count as changed after it.
                streams.mark_partition_statuses_changed(&self.partition_status_outbox.namespaces());
                *state = PrimaryRebalance {
                    view: Some(consensus.view()),
                    // A partition's members never move, so placement waits to
                    // hear from every peer rather than presume the node whose
                    // failure started this view is ready.
                    // Reports from before this view went out under the last
                    // metadata primary or come round in the cycle. The changes
                    // a failover makes come after it, so they go first.
                    peers: vec![
                        Peer {
                            changes_sent,
                            ..Peer::default()
                        };
                        replicas
                    ],
                    eligible: vec![false; replicas],
                    view_since: now,
                    next_quorum_scan: now,
                    backoffs: vec![TargetBackoff::default(); replicas],
                    stable_since: now,
                    next_move: now,
                    next_watch: now,
                    received_probe: state.received_probe,
                    ..PrimaryRebalance::default()
                };
            }
            let mut changed = false;
            let admission_opened = state.admission_opened;
            for replica in 0..consensus.replica_count() {
                let index = usize::from(replica);
                let ready = if replica == consensus.replica() {
                    !self.is_leaving()
                } else {
                    state.peers[index]
                        .ready_at
                        .is_some_and(|ready| now.duration_since(ready) <= PROBE_TIMEOUT)
                };
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
                if peer.nonce.is_some() && elapsed > PROBE_TIMEOUT {
                    peer.heard = true;
                }
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
            let readiness_known = state
                .peers
                .iter()
                .enumerate()
                .all(|(index, peer)| index == usize::from(consensus.replica()) || peer.heard);
            // Placement readiness drops a node whose metadata consensus has not
            // settled, which says nothing about whether it holds its
            // partitions. Liveness is whether it answers at all.
            let live: Vec<bool> = (0..consensus.replica_count())
                .map(|replica| {
                    replica == consensus.replica()
                        || state.peers[usize::from(replica)]
                            .live_at
                            .is_some_and(|live| now.duration_since(live) <= PROBE_TIMEOUT)
                })
                .collect();
            let live_nodes = if readiness_known {
                node_mask(&live)
            } else {
                u32::MAX
            };
            if changed {
                state.stable_since = now;
                state.pending.clear();
            }
            if changed || readiness_known != state.readiness_known {
                state.readiness_known = readiness_known;
                metadata.observe_replica_readiness(if readiness_known {
                    &state.eligible
                } else {
                    &[]
                });
            }
            // A tiered topic's partitions can land on any member, so one that
            // is unheard in this view or whose report aged out counts against
            // the whole cluster. Published every tick because this node's own
            // flag can change without any peer's report changing.
            let tiered_storage_ready = readiness_known
                && metadata.applied_frontier().tiered_storage_ready()
                && state.peers.iter().enumerate().all(|(index, peer)| {
                    index == usize::from(consensus.replica())
                        || peer
                            .tiered_storage_ready_at
                            .is_some_and(|ready| now.duration_since(ready) <= PROBE_TIMEOUT)
                });
            metadata.observe_tiered_storage_readiness(tiered_storage_ready);
            if live_nodes != state.live_nodes {
                state.live_nodes = live_nodes;
                metadata
                    .mux_stm
                    .streams()
                    .set_partition_directory_ready_nodes(live_nodes);
            }
            // A pass spans ticks and takes each tick's liveness and stale
            // cutoff. Only the gauges read its counts, so churn skews them for
            // one pass at most.
            let continues = state.replication_pass.in_progress();
            if readiness_known && (continues || now >= state.next_quorum_scan) {
                if !continues {
                    state.next_quorum_scan = now + PROBE_INTERVAL;
                }
                let streams = metadata.mux_stm.streams();
                // Reports from before this view came through the last metadata
                // primary, so they get as long as this view's own to come round.
                let stale_before = now
                    .checked_sub(report_stale_after(streams.partition_status_count()))
                    .filter(|cutoff| *cutoff >= state.view_since);
                let PrimaryRebalance {
                    replication,
                    replication_pass,
                    ..
                } = &mut *state;
                if let Some(counts) = streams.partition_replication(
                    &live,
                    now,
                    stale_before,
                    replication_pass,
                    PARTITION_REPLICATION_CHUNK,
                ) {
                    *replication = counts;
                }
            }
            let PrimaryRebalance { pending, moves, .. } = &mut *state;
            pending.retain(|_, pending| {
                let current = metadata.partition_placement_view(
                    pending.plan.namespace,
                    pending.plan.created_revision,
                );
                let invalid = current.is_none_or(|current| current.view != pending.plan.view);
                let expired = now.duration_since(pending.since)
                    > match pending.phase {
                        MovePhase::Capture | MovePhase::Probe => PROBE_TIMEOUT,
                        // The coordinator cannot see whether the source used the
                        // grant. Giving up before the new view shows up would
                        // let a later round move the partition a second time.
                        MovePhase::Transfer => TRANSFER_TIMEOUT,
                    };
                if !invalid && !expired {
                    return true;
                }
                // The planned view ended after the grant, so watch whether
                // the target keeps the partition.
                if invalid && current.is_some() && pending.phase == MovePhase::Transfer {
                    moves.push(MovedPrimary {
                        plan: pending.plan,
                        since: now,
                    });
                }
                false
            });
            if now >= state.next_watch {
                state.next_watch = now + PROBE_INTERVAL;
                let PrimaryRebalance {
                    moves, backoffs, ..
                } = &mut *state;
                moves.retain(|moved| {
                    let target = moved.plan.target;
                    let (Some(current), Some(backoff)) = (
                        metadata.partition_placement_view(
                            moved.plan.namespace,
                            moved.plan.created_revision,
                        ),
                        backoffs.get_mut(usize::from(target)),
                    ) else {
                        return false;
                    };
                    if self.partition_primary_node(moved.plan.namespace.inner(), current.view)
                        != Some(target)
                    {
                        backoff.record_failure(now);
                        return false;
                    }
                    if now.duration_since(moved.since) >= MOVE_HOLD_INTERVAL {
                        *backoff = TargetBackoff::default();
                        return false;
                    }
                    true
                });
            }
            if state.pending.len() < REBALANCE_MOVES_INFLIGHT_MAX
                && now >= state.next_move
                && now.duration_since(state.stable_since) >= STABLE_INTERVAL
            {
                state.next_move = now + PROBE_INTERVAL;
                // After its wait, a backed-off target gets one move at a time
                // until one holds. It also sits out as a source: it lost the
                // primaries moved to it, so it rarely has any to give.
                let mut candidates = state.eligible.clone();
                for ((replica, eligible), backoff) in
                    candidates.iter_mut().enumerate().zip(&state.backoffs)
                {
                    let probing = backoff.wait > Duration::ZERO
                        && state
                            .moves
                            .iter()
                            .map(|moved| moved.plan)
                            .chain(state.pending.values().map(|pending| pending.plan))
                            .any(|plan| usize::from(plan.target) == replica);
                    *eligible &= !backoff.blocks(now) && !probing;
                }
                // Each planned move joins the moves in flight, so the next
                // pick counts it as done and the round ends in balance.
                let mut inflight: BTreeMap<_, _> = state
                    .pending
                    .iter()
                    .map(|(namespace, pending)| (*namespace, pending.plan))
                    .collect();
                while inflight.len() < REBALANCE_MOVES_INFLIGHT_MAX
                    && let Some(plan) =
                        metadata.primary_rebalance_candidate(&candidates, state.cursor, &inflight)
                {
                    if state
                        .backoffs
                        .get(usize::from(plan.target))
                        .is_some_and(|backoff| backoff.wait > Duration::ZERO)
                        && let Some(eligible) = candidates.get_mut(usize::from(plan.target))
                    {
                        *eligible = false;
                    }
                    let mut probe =
                        probe_header(consensus, self.next_forward_nonce(consensus.replica()));
                    probe.kind = PrimaryRebalanceKind::SourceProbe;
                    probe.namespace = plan.namespace.inner();
                    probe.created_revision = plan.created_revision;
                    probe.partition_view = plan.view;
                    state.cursor = Some(plan.namespace);
                    inflight.insert(plan.namespace, plan);
                    state.pending.insert(
                        plan.namespace,
                        PendingMove {
                            plan,
                            probe,
                            phase: MovePhase::Capture,
                            since: now,
                        },
                    );
                    captures.push(probe);
                }
            }
        }
        for probe in captures {
            if let Some(source) = self.partition_primary_node(probe.namespace, probe.partition_view)
            {
                self.send_rebalance(source, probe, &[]).await;
            }
        }
        for (replica, probe) in outgoing {
            let (statuses, unsent) = self.next_status_batch(replica);
            if !self.send_rebalance(replica, probe, &statuses).await {
                self.primary_rebalance
                    .borrow_mut()
                    .forget_unsent(replica, probe.nonce, unsent);
            }
        }
    }

    /// The next run of directory reports for `peer`, advancing its cursors,
    /// with the cursors from before it for a send that fails.
    fn next_status_batch(&self, peer: u8) -> (Vec<PartitionStatusEntry>, StatusCursors) {
        let mut state = self.primary_rebalance.borrow_mut();
        let Some(cursor) = state.peers.get_mut(usize::from(peer)) else {
            return (Vec::new(), StatusCursors::default());
        };
        let before = StatusCursors {
            status_cursor: cursor.status_cursor,
            changes_sent: cursor.changes_sent,
        };
        let streams = self.plane.metadata().mux_stm.streams();
        // Changes first: a new primary reaches every node within a probe, not
        // once the cycle through every partition comes back to it.
        let (mut batch, changes_sent) =
            streams.partition_status_changes(cursor.changes_sent, STATUS_ENTRIES_PER_FRAME);
        cursor.changes_sent = changes_sent;
        let room = STATUS_ENTRIES_PER_FRAME - batch.len();
        if room > 0 {
            let (cycle, last) = streams.partition_status_batch(
                cursor.status_cursor,
                room,
                STATUS_ENTRIES_SCANNED_PER_FRAME,
            );
            cursor.status_cursor = last;
            batch.extend(cycle);
        }
        (batch, before)
    }

    /// Node id of the primary `view` assigns in the committed partition behind
    /// `namespace`. `None` on a shard with no metadata group, so only the
    /// coordinator asks. A shard asks the consensus of its own partition.
    fn partition_primary_node(&self, namespace: u64, view: u32) -> Option<u8> {
        let nodes = self.plane.metadata().consensus.as_ref()?.replica_count();
        let members = self
            .plane
            .metadata()
            .mux_stm
            .streams()
            .members_for_namespace(IggyNamespace::from_raw(namespace))?;
        metadata::stm::stream::primary_of(members, view, nodes)
    }

    /// Once per probe interval, check one chunk of the directory for deleted
    /// partitions. The cross shard directory lock is held for one chunk only.
    ///
    /// The metadata primary also feeds a reply's worth of its own reports into
    /// its directory. It never probes itself, so no reply refreshes them, and
    /// its quiet partitions send no new report: without the feed they would
    /// age out of the directory.
    fn retain_partition_directories(&self, now: Instant) {
        if self
            .partition_directory_retained_at
            .get()
            .is_some_and(|retained| now.duration_since(retained) < PROBE_INTERVAL)
        {
            return;
        }
        self.partition_directory_retained_at.set(Some(now));
        let next = self
            .plane
            .metadata()
            .mux_stm
            .streams()
            .retain_partition_directory(
                self.partition_directory_retain_cursor.get(),
                DIRECTORY_RETAIN_CHUNK,
            );
        self.partition_directory_retain_cursor.set(next);
        // Only a coordinator holds a view. Its start marks the outbox's reports
        // as changed, retractions included, so the feed waits for it.
        if self
            .plane
            .metadata()
            .consensus
            .as_ref()
            .is_some_and(|consensus| self.primary_rebalance.borrow().view == Some(consensus.view()))
        {
            let (own, _) = self.next_reported_statuses();
            self.record_partition_statuses(&own);
        }
    }

    /// Take in partition reports, this node's own or a peer's.
    pub(crate) fn record_partition_statuses(&self, statuses: &[PartitionStatusEntry]) {
        let Some(consensus) = self.plane.metadata().consensus.as_ref() else {
            return;
        };
        if statuses.is_empty() {
            return;
        }
        self.plane
            .metadata()
            .mux_stm
            .streams()
            .record_partition_statuses(
                statuses,
                consensus.node_id(),
                consensus.replica_count(),
                self.bus.monotonic_now(),
            );
    }

    #[allow(clippy::future_not_send, clippy::too_many_lines)]
    pub(crate) async fn on_primary_rebalance(
        &self,
        header: PrimaryRebalanceHeader,
        statuses: Vec<PartitionStatusEntry>,
    ) {
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
                    self.record_partition_statuses(&statuses);
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
                        && consensus_ready(consensus)
                        && !self.is_leaving(),
                );
                reply.tiered_storage_ready = u8::from(
                    header.kind == PrimaryRebalanceKind::MetadataProbe
                        && self
                            .plane
                            .metadata()
                            .applied_frontier()
                            .tiered_storage_ready(),
                );
                if header.kind == PrimaryRebalanceKind::MetadataProbe {
                    let (statuses, unsent) = self.next_reported_statuses();
                    if !self.send_rebalance(header.replica, reply, &statuses).await {
                        self.partition_status_outbox
                            .forget_unsent(&statuses, unsent);
                    }
                } else {
                    self.send_rebalance(header.replica, reply, &[]).await;
                }
            }
            PrimaryRebalanceKind::MetadataReply => {
                if consensus.is_primary() && consensus_ready(consensus) {
                    let (answered, evidence) = {
                        let mut state = self.primary_rebalance.borrow_mut();
                        let answered = state.observe_peer(&header, now);
                        (
                            answered,
                            state.quorum_evidence(consensus.quorum_replication()),
                        )
                    };
                    // The next tick would reach the same verdict, but a create
                    // prepared before it would still be admitted.
                    if answered && header.tiered_storage_ready != 1 {
                        self.plane
                            .metadata()
                            .observe_tiered_storage_readiness(false);
                    }
                    // Only a reply to the outstanding probe carries reports
                    // newer than what the directory already holds.
                    if answered {
                        self.record_partition_statuses(&statuses);
                        let views: Vec<_> = statuses
                            .iter()
                            .map(|status| metadata::impls::metadata::PartitionPlacementView {
                                namespace: IggyNamespace::from_raw(status.namespace),
                                created_revision: status.created_revision,
                                view: status.view,
                            })
                            .collect();
                        self.plane.metadata().observe_partition_views(&views);
                    }
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
                        || Some(header.replica)
                            != self.partition_primary_node(header.namespace, header.partition_view)
                    {
                        return;
                    }
                    let namespace = IggyNamespace::from_raw(header.namespace);
                    if header.ready == 0 {
                        state.pending.remove(&namespace);
                        return;
                    }
                    let Some(pending) = state.pending.get_mut(&namespace) else {
                        return;
                    };
                    pending.probe.commit = header.commit;
                    pending.probe.kind = PrimaryRebalanceKind::PartitionProbe;
                    pending.phase = MovePhase::Probe;
                    pending.since = now;
                    (pending.plan.target, pending.probe)
                };
                self.send_rebalance(outgoing.0, outgoing.1, &[]).await;
            }
            PrimaryRebalanceKind::PartitionReply => {
                if !consensus.is_primary() || !consensus_ready(consensus) {
                    return;
                }
                let outgoing = {
                    let mut state = self.primary_rebalance.borrow_mut();
                    let namespace = IggyNamespace::from_raw(header.namespace);
                    if !state.matches_pending(&header, MovePhase::Probe, now)
                        || state
                            .pending
                            .get(&namespace)
                            .is_none_or(|pending| pending.plan.target != header.replica)
                    {
                        return;
                    }
                    if header.ready == 0 {
                        state.pending.remove(&namespace);
                        return;
                    }
                    let Some(pending) = state.pending.get_mut(&namespace) else {
                        return;
                    };
                    pending.probe.kind = PrimaryRebalanceKind::PrimaryTransfer;
                    pending.probe.target = pending.plan.target;
                    pending.phase = MovePhase::Transfer;
                    pending.since = now;
                    (
                        self.partition_primary_node(pending.probe.namespace, pending.plan.view),
                        pending.probe,
                    )
                };
                let Some(source) = outgoing.0 else {
                    return;
                };
                if !self.send_rebalance(source, outgoing.1, &[]).await {
                    self.primary_rebalance.borrow_mut().forget_unsent_grant(
                        IggyNamespace::from_raw(outgoing.1.namespace),
                        outgoing.1.nonce,
                    );
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
            LocalRebalance::Wire(header) => self.on_primary_rebalance(header, Vec::new()).await,
            LocalRebalance::Capture(mut header) | LocalRebalance::Probe(mut header) => {
                let capture = matches!(frame, LocalRebalance::Capture(_));
                let namespace = IggyNamespace::from_raw(header.namespace);
                let partition = self.plane.partitions().get_by_ns(&namespace);
                header.ready = u8::from(partition.is_some_and(|partition| {
                    !self.is_leaving()
                        && self.rebalance_incarnation_current(namespace, header.created_revision)
                        && if capture {
                            partition_history_ready(partition, &header)
                                && partition.consensus().is_primary()
                        } else {
                            partition_ready(partition, &header)
                        }
                }));
                if let Some(partition) = partition {
                    let coordinator = header.replica;
                    header.replica = partition.consensus().node_id();
                    header.kind = if capture {
                        header.commit = partition.consensus().commit_max();
                        PrimaryRebalanceKind::SourceReply
                    } else {
                        PrimaryRebalanceKind::PartitionReply
                    };
                    self.send_rebalance(coordinator, header, &[]).await;
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

    /// One step of the pass that gives up the partitions this shard leads.
    ///
    /// Each partition goes to the in-sync member that took the fewest so far.
    /// A tie goes to the member that an election would pick. The pass visits a
    /// partition once: peers that leave at the same time hand partitions to
    /// this node, and a second pass would send them around.
    ///
    /// Each handoff is a view change through the partition I/O lane, so the
    /// limits on heartbeat votes pace the pass.
    pub(crate) fn tick_primary_handoff(&self) {
        let mut handoff = self.primary_handoff.borrow_mut();
        let PrimaryHandoff::Walking {
            remaining,
            handed_off,
            given,
            leaving,
        } = &mut *handoff
        else {
            return;
        };
        let partitions = self.plane.partitions();
        let now = self.bus.monotonic_now();
        // The node waits for its own handoffs only. A view change that a peer
        // started ends without it, and one that lost its quorum never ends.
        // A target that cannot lead yet, such as a peer that restarted and
        // still probes for the view, holds its view change until the status
        // timeout. So the wait for each handoff has its own deadline.
        handed_off.retain(|(namespace, wait_until)| {
            now < *wait_until
                && partitions.get_by_ns(namespace).is_some_and(|partition| {
                    let consensus = partition.consensus();
                    // A view change whose next primary went away cannot end
                    // before the status timeout.
                    partition.primary_transfer_pending()
                        || (consensus.status() == Status::ViewChange
                            && self
                                .bus
                                .replica_is_up(consensus.primary_node(consensus.view())))
                })
        });
        // Every view change of the shard loads the I/O lane, whoever started it.
        let mut inflight = partitions
            .namespaces()
            .filter_map(|namespace| partitions.get_by_ns(namespace))
            .filter(|partition| {
                partition.primary_transfer_pending()
                    || partition.consensus().status() == Status::ViewChange
            })
            .count();
        let mut started = 0usize;
        while let Some(&namespace) = remaining.last() {
            if started == crate::PARTITION_VIEW_CHANGE_VOTES_PER_TICK_MAX
                || inflight >= crate::PARTITION_VIEW_CHANGES_INFLIGHT_MAX
            {
                return;
            }
            remaining.pop();
            let Some(partition) = partitions.get_mut_by_ns(&namespace) else {
                continue;
            };
            let consensus = partition.consensus();
            let own = 1_u32
                .checked_shl(u32::from(consensus.node_id()))
                .unwrap_or(0);
            let Some(target) =
                consensus
                    .in_sync_nodes(consensus::MEMBER_LAG_MAX)
                    .and_then(|nodes| {
                        least_given(consensus, self.links_up(nodes & !own & !*leaving), given)
                    })
            else {
                continue;
            };
            let expires_at = now + GRANT_TIMEOUT;
            let request = PreparedPrimaryTransfer {
                created_revision: partition.created_revision(),
                expected_view: consensus.view(),
                target,
                expires_at,
            };
            if self.rebalance_incarnation_current(namespace, request.created_revision)
                && partition.defer_primary_transfer(request)
            {
                given[usize::from(target)] += 1;
                started += 1;
                inflight += 1;
                // The transfer starts before it expires, and its view change
                // gets one more grant to end.
                handed_off.push((namespace, expires_at + GRANT_TIMEOUT));
            }
        }
        if !handed_off.is_empty() {
            return;
        }
        // A request that expires in its drain, or whose target goes away,
        // moves no primary, so this is not the number that moved.
        tracing::info!(
            shard = self.id,
            requested = given.iter().sum::<u32>(),
            "ended the primary handoff of this shard before shutdown"
        );
        *handoff = PrimaryHandoff::Done;
    }

    /// The nodes of `nodes` that frames reach now. The in-sync mask of a quiet
    /// group still names a node that went away after its last heartbeat.
    fn links_up(&self, nodes: u32) -> u32 {
        (0..u8::MAX)
            .take(HANDOFF_NODES)
            .filter(|node| nodes & (1 << node) != 0 && self.bus.replica_is_up(*node))
            .fold(0, |up, node| up | (1 << node))
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

    /// Returns false when the frame did not leave this node. `statuses` rides
    /// only on a metadata probe or reply.
    #[allow(clippy::future_not_send)]
    async fn send_rebalance(
        &self,
        target: u8,
        mut header: PrimaryRebalanceHeader,
        statuses: &[PartitionStatusEntry],
    ) -> bool {
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
        let body = encode_status_entries(statuses);
        let size = HEADER_SIZE + body.len();
        header.size = u32::try_from(size).unwrap_or(u32::MAX);
        header.checksum_body = if body.is_empty() {
            0
        } else {
            u128::from(iggy_common::calculate_checksum(&body))
        };
        header.seal();
        let mut message = Message::<PrimaryRebalanceHeader>::new(size)
            .transmute_header(|_, output: &mut PrimaryRebalanceHeader| *output = header);
        message.as_mut_slice()[HEADER_SIZE..size].copy_from_slice(&body);
        self.bus
            .send_to_replica(target, message.into_generic().into_frozen())
            .await
            .is_ok()
    }

    /// The next run of this node's own partition reports for a metadata reply.
    fn next_reported_statuses(&self) -> (Vec<PartitionStatusEntry>, OutboxUnsent) {
        self.partition_status_outbox
            .next_batch(STATUS_ENTRIES_PER_FRAME)
    }
}

/// How long a partition report goes without a newer copy before its primary
/// counts as no longer leading. A node sends the reports of what it leads one
/// frame per probe and leads at most the `reports` held here, so one it keeps
/// sending comes round within that many frames. Twice that, a probe and a
/// probe timeout cover the changes each frame sends ahead of that cycle and a
/// lost reply.
fn report_stale_after(reports: usize) -> Duration {
    let passes = u32::try_from(reports.div_ceil(STATUS_ENTRIES_PER_FRAME)).unwrap_or(u32::MAX);
    PROBE_INTERVAL
        .saturating_mul(passes.saturating_mul(2).saturating_add(1))
        .saturating_add(PROBE_TIMEOUT)
}

/// `eligible` as a mask of node ids, bit `n` for node `n`.
fn node_mask(eligible: &[bool]) -> u32 {
    eligible
        .iter()
        .enumerate()
        .filter(|(_, ready)| **ready)
        .filter_map(|(node, _)| u32::try_from(node).ok())
        .filter_map(|node| 1_u32.checked_shl(node))
        .fold(0, |mask, bit| mask | bit)
}

/// The node of `nodes` that took the fewest partitions of the pass. A tie goes
/// to the node that leads first after the current view of `consensus`, so a
/// lone group lands where an election would put it.
fn least_given<B: MessageBus>(
    consensus: &VsrConsensus<B>,
    nodes: u32,
    given: &[u32; HANDOFF_NODES],
) -> Option<u8> {
    (1..=u32::from(consensus.replica_count()))
        .filter_map(|ahead| consensus.view().checked_add(ahead))
        .map(|view| consensus.primary_node(view))
        .filter(|&node| {
            1_u32
                .checked_shl(u32::from(node))
                .is_some_and(|bit| nodes & bit != 0)
        })
        .min_by_key(|&node| given[usize::from(node)])
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
        && partition.repair().is_none()
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
        && partition.transfer().is_none()
        && partition.transfer_rearm.is_none()
        && !partition.consensus().has_untrusted_log()
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
        tiered_storage_ready: 0,
        reserved: [0; 72],
    }
}
