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

//! Quiesce-time cross-replica oracle (workload Phase C).
//!
//! The per-tick invariants in [`super::invariants`] catch a regression on a
//! single replica. This adds the post-drain checks. [`assert_converged`]
//! asserts that:
//!
//! - no live replica is ahead of the leader on any namespace (a backup ahead of
//!   the leader is a split-brain / divergence bug),
//! - every live replica agrees with every other on each committed metadata op
//!   they both hold, the real consensus property (see
//!   [`super::state_checker`]),
//! - live replicas that committed the same `SendMessages` prepare hold the same
//!   applied message data for it, which the prepare identity leaves out,
//! - on a serial run, the workload's predicted [`Shadow`] equals the metadata
//!   committed on the leader, the payoff of the name-keyed shadow,
//! - every consumer group owner the leader settled on a workload namespace is
//!   the owner its partition primary installed, and no revocation stays pending
//!   long past its drain deadline.
//!
//! Equality is asserted over the committed PREFIX, not over equal heads: a replica
//! that missed the last commit broadcast, or rejoined recently, may legitimately
//! trail. What it may not do is hold different history at an op it did commit.
//! Requiring equal heads would fail on ordinary lag and say nothing about safety.

use crate::client::SimClient;
use crate::replica::Replica;
use crate::workload::change_log_oracle::{assert_change_log_projection, assert_change_logs_agree};
use crate::workload::invariants::Invariants;
use crate::workload::shadow::Shadow;
use crate::workload::{
    Workload, apply_sim_commands, bind_recovered_client, follow_leader_redirect,
    recover_evicted_clients, resubmit_due, resubmit_due_handshakes, state_checker,
};
use crate::{Simulator, rebalancing_timeout_micros};
use consensus::{Consensus, MetadataHandle, PartitionsHandle, Status};
use iggy_binary_protocol::primitives::partition_history::ConsumerGroupOwner;
use iggy_binary_protocol::requests::partitions::RetireConsumerGroupOwnersRequest;
use iggy_binary_protocol::{Operation, WireDecode};
use iggy_common::{ConsumerKind, ResourceOptions, UserStatus};
use metadata::impls::metadata::StreamsFrontend;
use server_common::sharding::IggyNamespace;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write;

/// Prefix every workload-generated entity name carries (see
/// [`Shadow::fresh_name`]). The entity oracle filters committed state to these
/// so genesis or system entities a replica may hold are not mistaken for a
/// shadow mismatch.
const WORKLOAD_PREFIX: &str = "wl-";

/// Settle window stepped after the last reply, before asserting convergence.
///
/// The primary broadcasts its commit number on a timer (`COMMIT_MESSAGE_TICKS`
/// = 50), so a trailing backup applies the final committed prepare only on the
/// next such broadcast, not at the moment the client reply is sent. The window
/// must span more than one interval; idle heartbeats never break convergence
/// because commit offsets and committed state only advance monotonically.
pub(crate) const QUIESCE_SETTLE_TICKS: u64 = 200;

/// How many rebalancing timeouts a pending revocation may age before the quiesce
/// check fails. The reconciler forces the handoff after one, and the second
/// covers the fence and the completion committing through the view changes that
/// crashes leave behind.
pub(crate) const PENDING_REVOCATION_TIMEOUTS_MAX: u64 = 2;

/// Everything [`assert_committed_metadata_agrees`] compares for one replica.
type ReplicaState = (CommittedMetadata, CommittedSettings);

/// Committed metadata entity sets read from one replica. Stored sorted so
/// equality is independent of slab / hashmap iteration order, which is not
/// stable across replicas.
#[derive(Debug, Default, PartialEq, Eq)]
struct CommittedMetadata {
    streams: BTreeSet<String>,
    topics: BTreeSet<(String, String)>,
    users: BTreeSet<String>,
    consumer_groups: BTreeSet<(String, String, String)>,
}

impl CommittedMetadata {
    /// Restrict to workload-generated entities (see [`WORKLOAD_PREFIX`]), so the
    /// entity oracle compares like with like against the shadow.
    ///
    /// Every level is filtered on its OWN name, not its stream's. The harness seeds
    /// filler topics and partitions (`sim-topic-*`, see `Streams::seed_namespace`)
    /// to keep slab ids dense, and once the workload has created enough streams
    /// those land inside a stream named `wl-...`. Filtering topics by their stream
    /// alone then admits harness state and the shadow is blamed for missing it.
    fn workload_owned(mut self) -> Self {
        self.streams
            .retain(|name| name.starts_with(WORKLOAD_PREFIX));
        self.topics.retain(|(stream, topic)| {
            stream.starts_with(WORKLOAD_PREFIX) && topic.starts_with(WORKLOAD_PREFIX)
        });
        self.users.retain(|name| name.starts_with(WORKLOAD_PREFIX));
        self.consumer_groups.retain(|(stream, topic, group)| {
            stream.starts_with(WORKLOAD_PREFIX)
                && topic.starts_with(WORKLOAD_PREFIX)
                && group.starts_with(WORKLOAD_PREFIX)
        });
        self
    }
}

/// Committed state the name-keyed shadow does not predict, compared only across
/// replicas: every value was resolved before its prepare committed.
#[derive(Debug, PartialEq)]
struct CommittedSettings {
    /// Created op and options per stream name.
    streams: BTreeMap<String, (u64, ResourceOptions)>,
    topics: BTreeMap<(String, String), ResourceOptions>,
    /// Status and options per username.
    users: BTreeMap<String, (UserStatus, ResourceOptions)>,
}

/// Drain the system after the active workload, then settle.
///
/// Stops submitting new requests and steps until no client request is
/// outstanding, then runs a settle window so trailing backups apply the final
/// committed prepares via the primary's commit broadcast.
///
/// Returns `true` once drained, `false` if `max_ticks` elapses with requests
/// still outstanding (a liveness failure the caller should surface).
///
/// `invariants` is the checker the active phase ran, carried in rather than built
/// here. A drain is up to 50,000 ticks of a cluster still repairing itself, so a
/// wedge that forms during it used to surface as nothing more than "did not drain",
/// and a fresh checker would start with an empty commit chain, which is the memory
/// that names a divergence.
///
/// A request that a leader redirect holds goes out once its login lands, so the
/// drain carries the logins on.
#[must_use]
pub fn drive_to_quiesce(
    sim: &mut Simulator,
    workload: &mut Workload,
    clients: &[SimClient],
    max_ticks: u64,
    invariants: &mut Invariants,
) -> bool {
    let mut drained = false;
    for _ in 0..max_ticks {
        // The drain keeps resending: a request lost on the way out is never
        // answered, so without retries the drain would spend its whole budget
        // waiting on a reply that cannot arrive.
        workload.tick();
        resubmit_due(sim, workload);
        resubmit_due_handshakes(sim, workload, clients);
        for (from, reply) in sim.step_with_senders() {
            if bind_recovered_client(sim, workload, clients, from, &reply)
                || workload.is_stale(from, &reply)
                || follow_leader_redirect(sim, workload, clients, &reply)
            {
                continue;
            }
            let cmds = workload.on_reply(&reply);
            apply_sim_commands(sim, &cmds);
        }
        workload.readback.check_served(sim, workload.options.seed);
        recover_evicted_clients(sim, workload, clients);
        invariants.check(sim, workload);
        if workload.total_in_flight() == 0 {
            drained = true;
            break;
        }
    }
    if !drained {
        return false;
    }
    for _ in 0..QUIESCE_SETTLE_TICKS {
        for reply in sim.step() {
            let cmds = workload.on_reply(&reply);
            apply_sim_commands(sim, &cmds);
        }
        invariants.check(sim, workload);
    }
    true
}

/// Why the run did not drain, as a multi-line report.
///
/// A failed drain is either a wedge or a merely slow cluster, and the bare boolean
/// [`drive_to_quiesce`] returns cannot tell them apart. With crashes, restarts and
/// packet loss in play that distinction is the whole diagnosis, so name what is
/// outstanding and what every live replica believes.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn quiesce_failure_report(sim: &Simulator, workload: &Workload) -> String {
    let mut report = format!(
        "did not drain: {} request(s) still outstanding (seed={:#x})\n",
        workload.total_in_flight(),
        workload.options.seed,
    );
    for row in workload.outstanding_summary() {
        let _ = writeln!(
            report,
            "  outstanding client={} request={} action={:?} last_target=replica {} \
             attempts={}",
            row.client, row.request, row.action, row.target, row.attempts,
        );
    }
    let _ = writeln!(report, "  resends issued: {}", workload.resends());
    for replica_idx in 0..sim.replica_count {
        if sim.is_crashed(replica_idx) {
            let _ = writeln!(report, "  replica {replica_idx}: CRASHED");
            continue;
        }
        let _ = write!(report, "  replica {replica_idx}: live");
        // Metadata plane first: a rejoining replica is quorum-invisible until its
        // view probe completes, so its status separates "the cluster is slow" from
        // "the cluster has no quorum despite enough live replicas".
        if let Some(consensus) = sim.replicas[usize::from(replica_idx)].shards[0]
            .plane
            .metadata()
            .consensus
            .as_ref()
        {
            // The three ways `is_caught_up_primary` stays shut on what otherwise
            // reads as a healthy Normal primary, silently dropping every request.
            let _ = write!(
                report,
                " | metadata status={:?} view={} log_view={} commit={}..{} barrier={} \
                 transferring={} primary={}",
                consensus.status(),
                consensus.view(),
                consensus.log_view(),
                consensus.commit_min(),
                consensus.commit_max(),
                consensus.recovery_barrier(),
                consensus.is_transferring(),
                consensus.is_primary(),
            );
            // `commit_min < commit_max` with no session is a backup blocked from
            // arming one; a session whose window the commit point already covers is
            // the stale one holding that gate shut.
            match sim.replicas[usize::from(replica_idx)].shards[0].metadata_repair_window() {
                Some((to_op, peer)) => {
                    let _ = write!(report, " repair=..{to_op}@{peer}");
                }
                None => {
                    let _ = write!(report, " repair=none");
                }
            }
        }
        for &ns in &workload.options.namespaces {
            let view = sim.consensus_view(usize::from(replica_idx), ns);
            let commit = sim
                .offsets(usize::from(replica_idx), ns)
                .map(|offsets| offsets.commit_offset);
            let primary = sim.primary_index(ns);
            let _ = write!(
                report,
                " | ns {ns:?} view={view:?} commit_offset={commit:?} primary={primary:?}",
            );
        }
        report.push('\n');
        let replica = &sim.replicas[usize::from(replica_idx)];
        for intent in replica.shards[0]
            .plane
            .metadata()
            .mux_stm
            .streams()
            .pending_lifecycles()
        {
            let _ = writeln!(report, "    pending lifecycle: {intent:?}");
            for target in intent.partitions {
                let namespace = server_common::sharding::IggyNamespace::new(
                    intent.stream_id as usize,
                    target.topic_id as usize,
                    target.partition_id as usize,
                );
                let partition = replica
                    .partition_shard(namespace)
                    .plane
                    .partitions()
                    .get_by_ns(&namespace);
                let _ = writeln!(
                    report,
                    "      {namespace:?}: {:?}",
                    partition.map(|partition| (
                        partition.created_revision(),
                        partition.history_deleted(),
                        partition.fatal(),
                        partition.consensus().status(),
                        partition.consensus().view(),
                        partition.consensus().commit_min(),
                        partition.consensus().commit_max(),
                    ))
                );
            }
        }
        // Per-client table state. `check_request` admits anything above the
        // watermark, so the watermark says whether an outstanding request is still
        // expected (above it) or already answered and owed a cached-reply replay (at
        // or below it).
        let table = sim.replicas[usize::from(replica_idx)].shards[0]
            .plane
            .metadata()
            .client_table
            .borrow();
        for client_id in table.client_ids() {
            let _ = writeln!(
                report,
                "    client {client_id}: watermark={:?} epoch={:?} cached_reply_request={:?}",
                table.get_watermark(client_id),
                table.get_epoch(client_id),
                table
                    .get_reply(client_id)
                    .map(|reply| reply.header().request),
            );
        }
    }
    report
}

/// Step until every live replica's metadata plane is `Normal` in one shared
/// view, or `max_ticks` elapses.
///
/// Needed before [`assert_converged`], which resolves the leader as "the live
/// replica whose metadata consensus says it is primary". With primaries spared
/// that was always the same replica in view 0. Once a primary can be crashed, live
/// replicas transiently hold different views and there may be no `Normal` primary
/// at all, so the leader lookup fails or names a deposed one: a false failure, not
/// a divergence.
///
/// Returns `false` if the views never converge, which is a real liveness failure
/// the caller should report rather than assert against an unsettled cluster.
///
/// Also steps while an owner install is pending:
/// the reconciler installs an owner on its own schedule, and the owner oracle
/// compares only installed ones.
///
/// Runs `invariants` per tick for the same reason [`drive_to_quiesce`] does: this is
/// another 50,000-tick window, and it is the one a view change wedges in.
#[must_use]
pub fn settle_to_stable_view(
    sim: &mut Simulator,
    workload: &mut Workload,
    max_ticks: u64,
    invariants: &mut Invariants,
) -> bool {
    for _ in 0..max_ticks {
        if views_are_settled(sim, workload) && !owner_install_pending(sim, workload) {
            return true;
        }
        workload.tick();
        resubmit_due(sim, workload);
        for reply in sim.step() {
            let cmds = workload.on_reply(&reply);
            apply_sim_commands(sim, &cmds);
        }
        invariants.check(sim, workload);
    }
    views_are_settled(sim, workload)
}

/// Whether leader metadata holds a pending owner on a workload namespace that is
/// younger than the stale revocation bound. Past the bound,
/// [`assert_no_stale_revocations`] fails it instead.
fn owner_install_pending(sim: &Simulator, workload: &Workload) -> bool {
    let Some(leader) = sim.metadata_leader() else {
        return false;
    };
    let now = sim.realtime_micros();
    let bound = pending_revocation_bound_micros();
    sim.replicas[usize::from(leader)].shards[0]
        .plane
        .metadata()
        .mux_stm
        .streams()
        .consumer_group_pending_revocations()
        .iter()
        .any(|transition| {
            now.saturating_sub(transition.created_at) <= bound
                && workload.options.namespaces.contains(&IggyNamespace::new(
                    transition.stream_id as usize,
                    transition.topic_id as usize,
                    transition.partition_id as usize,
                ))
        })
}

/// Ids that the topic holding `namespace` allocated and no longer holds in the
/// committed metadata of replica `leader`. Group ids are never reused.
fn deleted_group_ids(sim: &Simulator, leader: u8, namespace: IggyNamespace) -> Vec<u64> {
    sim.replicas[usize::from(leader)].shards[0]
        .plane
        .metadata()
        .mux_stm
        .streams()
        .read(|inner| {
            inner
                .items
                .get(namespace.stream_id())
                .and_then(|stream| stream.topics.get(namespace.topic_id()))
                .map(|topic| {
                    (0..topic.next_consumer_group_id)
                        .filter(|group_id| !topic.consumer_groups.contains_key(group_id))
                        .collect()
                })
                .unwrap_or_default()
        })
}

/// Both planes settled: the metadata group and every tracked partition group.
///
/// The partition half matters for the leader-relative check in
/// [`assert_converged`], whose leader comes from `Simulator::primary_index`, a
/// single replica's view of that group's primary. Each partition group runs its own
/// view change, so settling only the metadata plane leaves that check asserting
/// against a deposed leader, and a correctly-ahead new one trips "exceeds leader".
fn views_are_settled(sim: &Simulator, workload: &Workload) -> bool {
    if !metadata_view_is_settled(sim) {
        return false;
    }
    workload
        .options
        .namespaces
        .iter()
        .all(|&ns| partition_view_is_settled(sim, ns))
}

/// True when every live replica's metadata consensus is `Normal` in the same
/// view and exactly one of them claims to be primary.
fn metadata_view_is_settled(sim: &Simulator) -> bool {
    let mut view = None;
    let mut primaries = 0usize;
    let mut live = 0usize;
    for replica_idx in 0..sim.replica_count {
        if sim.is_crashed(replica_idx) {
            continue;
        }
        let Some(consensus) = sim.replicas[usize::from(replica_idx)].shards[0]
            .plane
            .metadata()
            .consensus
            .as_ref()
        else {
            continue;
        };
        live += 1;
        if consensus.status() != Status::Normal {
            return false;
        }
        match view {
            Some(agreed) if agreed != consensus.view() => return false,
            Some(_) => {}
            None => view = Some(consensus.view()),
        }
        if consensus.is_primary() {
            primaries += 1;
        }
    }
    live > 0 && primaries == 1
}

/// True when every live replica hosting `ns` has that group `Normal` in one
/// shared view with exactly one primary.
///
/// A replica not hosting the namespace is skipped rather than counted as
/// disagreement: a group materialises only on its hash-owning shard, and a group
/// whose stream the workload deleted is not re-materialised after a restart.
fn partition_view_is_settled(sim: &Simulator, ns: server_common::sharding::IggyNamespace) -> bool {
    let mut view = None;
    let mut primaries = 0usize;
    let mut hosts = 0usize;
    for replica_idx in 0..sim.replica_count {
        if sim.is_crashed(replica_idx) {
            continue;
        }
        let Some(state) = sim.partition_consensus_state(usize::from(replica_idx), ns) else {
            continue;
        };
        hosts += 1;
        if state.status != Status::Normal {
            return false;
        }
        match view {
            Some(agreed) if agreed != state.view => return false,
            Some(_) => {}
            None => view = Some(state.view),
        }
        if state.is_primary {
            primaries += 1;
        }
    }
    // No live host counts as settled: the offset check has no leader to resolve
    // either, and skips the namespace for the same reason.
    hosts == 0 || primaries == 1
}

/// Post-drain consensus checks.
///
/// Asserts no live replica is ahead of the leader, that every live replica agrees
/// with every other on each committed metadata op they share, and (on a serial
/// run) that the shadow equals the metadata committed on the leader.
///
/// Assumes one stable primary every live replica agrees on, which
/// [`settle_to_stable_view`] establishes and callers should run first once
/// primaries can be crashed. Without it this may pick a deposed leader (a
/// correctly-ahead new primary then trips "exceeds leader") or find no `Normal`
/// primary mid-view-change. Both are false failures.
///
/// # Panics
/// If a replica is ahead of the leader, two replicas disagree on a committed op or
/// on the message data it applied, or the shadow mismatches the leader. The
/// workload seed is in the message so the failing run replays deterministically.
#[allow(clippy::too_many_lines)]
pub fn assert_converged(sim: &Simulator, workload: &mut Workload) -> ConvergenceReport {
    let seed = workload.options.seed;
    let live: Vec<usize> = (0..sim.replica_count)
        .filter(|replica_idx| !sim.is_crashed(*replica_idx))
        .map(usize::from)
        .collect();
    assert!(
        !live.is_empty(),
        "no live replicas at quiesce (seed={seed:#x})"
    );

    // The consensus property proper: replicas compared against each other rather
    // than against the workload's expectations. First, because a genuine divergence
    // explains any leader confusion below it.
    let ops_compared = state_checker::assert_committed_prefixes_agree(sim, seed);
    tracing::info!(
        ops_compared,
        "committed metadata prefixes agree across every live replica"
    );

    let Some(leader) = sim.metadata_leader().map(usize::from) else {
        panic!("no metadata leader live at quiesce (seed={seed:#x})");
    };

    // Settlement treats "no live replica hosts this group" as settled, so losing
    // every instance of a LIVE partition reads as converged. The metadata read
    // separates that from a group whose stream the workload deleted.
    let namespaces_checked = assert_live_namespaces_have_a_primary(sim, workload, leader, seed);
    assert_hosted_incarnations_named(sim, workload, &live, seed);

    // Safety direction: no live replica may have COMMITTED more of a group than
    // its leader has. A backup may trail (no idle catch-up yet, see module docs),
    // but a backup committed past the leader is a divergence.
    //
    // Measured on the group's consensus `commit_min`, not
    // `PartitionOffsets::commit_offset`. The latter is the highest durably persisted
    // message offset, which counts an uncommitted suffix: a backup that persisted op
    // N while the electing view settled on N-1 is ordinary VSR, not divergence. That
    // stayed invisible only while primaries were spared, a never-crashed primary
    // being always furthest ahead; with primary crashes it fires on a correct
    // cluster.
    for &ns in &workload.options.namespaces {
        let Some(partition_leader) = sim.primary_index(ns) else {
            continue;
        };
        let Some(leader_committed) = sim
            .partition_consensus_state(usize::from(partition_leader), ns)
            .map(|state| state.commit_min)
        else {
            continue;
        };
        // Another incarnation of the namespace is another log.
        let incarnation = sim.partition_incarnation(usize::from(partition_leader), ns);
        for &replica_idx in &live {
            if sim.partition_incarnation(replica_idx, ns) != incarnation {
                continue;
            }
            if let Some(committed) = sim
                .partition_consensus_state(replica_idx, ns)
                .map(|state| state.commit_min)
            {
                assert!(
                    committed <= leader_committed,
                    "replica {replica_idx} committed {committed} ops exceeds leader \
                     {partition_leader} ({leader_committed}) on ns {ns:?} at quiesce \
                     (seed={seed:#x})",
                );
            }
        }
    }

    // Partition-plane twin of the walk above: that one bounds how much each
    // replica committed, this compares what they committed.
    let partition_ops_compared =
        state_checker::assert_partition_prefixes_agree(sim, &workload.options.namespaces, seed);
    tracing::info!(
        partition_ops_compared,
        "committed partition entries agree across every live replica"
    );
    let data = state_checker::assert_applied_data_agrees(sim, &workload.options.namespaces, seed);
    workload.readback.assert_served_committed(sim, seed);

    let replicas_compared = assert_committed_metadata_agrees(sim, &live, seed);
    let change_records_compared = assert_change_logs_agree(sim, &live, seed);
    let projection_items_compared = assert_change_log_projection(sim, leader, seed);

    let owners_compared = assert_settled_owners_installed(sim, workload, leader, seed);
    let retirements_compared = assert_retired_owners_removed(sim, workload, leader, &live, seed);
    let offsets_compared_with_shadow = assert_partition_offsets_agree(sim, workload, &live, seed);
    assert_no_stale_revocations(sim, leader, seed);

    let report = ConvergenceReport {
        ops_compared,
        partition_ops_compared,
        data_namespaces_compared: data.namespaces,
        data_records_compared: data.records,
        replicas_compared,
        change_records_compared,
        projection_items_compared,
        namespaces_checked,
        owners_compared,
        offsets_compared_with_shadow,
        retirements_compared,
    };

    // Entity oracle: on a serial run the shadow must equal the committed
    // metadata on the leader, the authoritative holder of the metadata log.
    //
    // Runs even when the oracle is disarmed, because a disarmed oracle is an
    // UNKNOWN and this is the measurement that resolves it. Equal means the
    // forgotten request's effect is accounted for and the oracle re-arms; unequal
    // means the unknown did cost the shadow an effect, which is reported rather than
    // asserted, since a failure there would blame the harness's gap on the cluster.
    if !workload.serial_run() {
        return report;
    }
    let committed = read_committed_metadata(&sim.replicas[leader].shards[0]).workload_owned();
    let shadow = shadow_metadata(&workload.shadow);
    if workload.strict_outcome_oracle() {
        assert_eq!(
            shadow, committed,
            "shadow diverged from leader-committed metadata at quiesce \
             (leader={leader}, seed={seed:#x})",
        );
        return report;
    }
    if shadow == committed {
        workload.rearm_outcome_oracle();
        tracing::info!(
            leader,
            "entity oracle re-armed: the shadow matches leader-committed metadata \
             again despite an earlier eviction"
        );
    } else {
        tracing::warn!(
            leader,
            "entity oracle stayed disarmed: an evicted client's forgotten request \
             left the shadow and leader-committed metadata unequal, so this run \
             proved nothing about entity state"
        );
    }
    report
}

/// What [`assert_converged`] actually managed to compare.
///
/// Counts of exercised comparisons, not of checks attempted: zero exactly when the
/// property was never tested. An oracle that compared nothing passes like one that
/// compared everything, so callers assert on these.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConvergenceReport {
    /// Committed metadata ops witnessed by more than one live replica.
    pub ops_compared: usize,
    /// Committed partition ops witnessed by more than one live replica, summed over
    /// every namespace. Separate from `ops_compared`: a partition-plane run commits
    /// almost no metadata and would otherwise read as having compared nothing.
    pub partition_ops_compared: usize,
    /// Namespaces in which live replicas' applied message data was compared.
    pub data_namespaces_compared: usize,
    /// Committed messages whose applied bytes were compared between live replicas.
    /// Separate from `partition_ops_compared`, which compares prepare identities
    /// and cannot see the message batch.
    pub data_records_compared: usize,
    /// Live replicas whose committed metadata CONTENT was compared against a peer
    /// sharing its commit point. Zero on a solo cluster.
    pub replicas_compared: usize,
    /// Change records compared between live replicas for the operations they
    /// all applied and retain.
    pub change_records_compared: usize,
    /// Current state items of the leader a mirror of its change records had to
    /// reproduce. Zero once the log evicted history the mirror needs.
    pub projection_items_compared: usize,
    /// Namespaces still present in committed metadata that were required to have a
    /// host and exactly one primary.
    pub namespaces_checked: usize,
    /// Consumer group owners settled in leader metadata that were compared with
    /// the owner their partition primary installed.
    pub owners_compared: usize,
    /// Partition offset tables compared with the serial acknowledged shadow.
    pub offsets_compared_with_shadow: usize,
    /// Retired groups, counted per live replica, whose owner and offsets the
    /// replica was required to have removed.
    pub retirements_compared: usize,
}

/// Require a host and exactly one primary for every workload namespace committed
/// metadata still knows about. Returns how many namespaces that covered.
///
/// A namespace the leader no longer holds is skipped: a deleted stream is not
/// re-materialised, and that is the only legitimate reason to have no host.
fn assert_live_namespaces_have_a_primary(
    sim: &Simulator,
    workload: &Workload,
    leader: usize,
    seed: u64,
) -> usize {
    let streams = sim.replicas[leader].shards[0]
        .plane
        .metadata()
        .mux_stm
        .streams();
    let mut checked = 0;
    for &ns in &workload.options.namespaces {
        if streams.created_revision_for_namespace(ns).is_none() {
            continue;
        }
        checked += 1;
        let mut hosts = 0usize;
        let mut primaries = 0usize;
        for replica_idx in 0..sim.replica_count {
            if sim.is_crashed(replica_idx) {
                continue;
            }
            if let Some(state) = sim.partition_consensus_state(usize::from(replica_idx), ns) {
                hosts += 1;
                if state.is_primary {
                    primaries += 1;
                }
            }
        }
        assert!(
            hosts > 0,
            "ns {ns:?} is live on leader {leader} but no live replica hosts it at \
             quiesce: every instance was lost (seed={seed:#x})"
        );
        assert_eq!(
            primaries, 1,
            "ns {ns:?} is live on leader {leader} but {hosts} host(s) report \
             {primaries} primaries at quiesce (seed={seed:#x})"
        );
    }
    checked
}

/// Fail if a live replica hosts a workload namespace at an incarnation that its
/// own metadata does not name.
///
/// The per-tick marks start again on any incarnation change, and the checks of
/// [`assert_converged`] compare replicas only on one incarnation, so a replica
/// that steps back to a deleted incarnation passes all of them. A replica whose
/// metadata names none is skipped: the simulator keeps a deleted topic's
/// partitions hosted until the topic is recreated.
fn assert_hosted_incarnations_named(
    sim: &Simulator,
    workload: &Workload,
    live: &[usize],
    seed: u64,
) {
    for &replica_idx in live {
        let streams = sim.replicas[replica_idx].shards[0]
            .plane
            .metadata()
            .mux_stm
            .streams();
        for &ns in &workload.options.namespaces {
            let (Some(hosted), Some(named)) = (
                sim.partition_incarnation(replica_idx, ns),
                streams.created_revision_for_namespace(ns),
            ) else {
                continue;
            };
            assert_eq!(
                hosted, named,
                "replica {replica_idx} hosts ns {ns:?} at incarnation {hosted}, but its \
                 metadata names {named} at quiesce (seed={seed:#x})",
            );
        }
    }
}

/// Assert live replicas standing at the same committed op hold the same committed
/// metadata. Returns how many replicas were compared against a peer.
///
/// The gap [`state_checker`] leaves by construction: it compares prepare HEADERS, so
/// two replicas whose logs agree op for op pass even when one applied that log onto a
/// different baseline, which is what a botched restart leaves.
///
/// Grouped by commit point because a replica that trails legitimately holds less.
///
/// Every committed entity, the harness fillers (`sim-*`) included: those bypass
/// consensus, so they agree only while the seed runs at the same point on every
/// replica, and a divergence there is the slab-order bug.
fn assert_committed_metadata_agrees(sim: &Simulator, live: &[usize], seed: u64) -> usize {
    let mut by_commit: BTreeMap<u64, (usize, ReplicaState)> = BTreeMap::new();
    let mut compared = 0;
    for &replica_idx in live {
        let Some(consensus) = sim.replicas[replica_idx].shards[0]
            .plane
            .metadata()
            .consensus
            .as_ref()
        else {
            continue;
        };
        let committed = read_replica_state(&sim.replicas[replica_idx].shards[0]);
        match by_commit.get(&consensus.commit_min()) {
            Some((owner, canonical)) => {
                assert_eq!(
                    &committed,
                    canonical,
                    "at quiesce replicas {owner} and {replica_idx} both committed \
                     through metadata op {} but hold different metadata: one applied \
                     that log onto a different baseline (seed={seed:#x})",
                    consensus.commit_min(),
                );
                compared += 1;
            }
            None => {
                by_commit.insert(consensus.commit_min(), (replica_idx, committed));
            }
        }
    }
    compared
}

/// Require the partition primary to hold each owner that leader metadata settled,
/// and equally committed replicas to agree. Returns how many owners it compared.
///
/// Metadata settles an owner only after a partition installed it, so a settled
/// owner the primary lacks was lost or never installed. Metadata names no owner
/// for an unassigned partition, which the partition installs as an owner with no
/// client.
fn assert_settled_owners_installed(
    sim: &Simulator,
    workload: &Workload,
    leader: usize,
    seed: u64,
) -> usize {
    let streams = sim.replicas[leader].shards[0]
        .plane
        .metadata()
        .mux_stm
        .streams();
    let mut compared = 0;
    for &ns in &workload.options.namespaces {
        let Some(primary) = sim.primary_index(ns) else {
            continue;
        };
        let settled: Vec<(u64, Option<ConsumerGroupOwner>)> = streams.read(|inner| {
            let Some(topic) = inner
                .items
                .get(ns.stream_id())
                .and_then(|stream| stream.topics.get(ns.topic_id()))
            else {
                return Vec::new();
            };
            topic
                .consumer_groups
                .iter()
                .filter_map(|(&group_id, group)| {
                    let assignment = group.assignments.get(&ns.partition_id())?;
                    assignment
                        .pending
                        .is_none()
                        .then_some((group_id, assignment.owner))
                })
                .collect()
        });
        for (group_id, owner) in settled {
            let installed = sim.installed_group_owner(usize::from(primary), ns, group_id);
            let agrees = owner.map_or_else(
                || installed.is_none_or(ConsumerGroupOwner::is_unassigned),
                |owner| installed == Some(owner),
            );
            assert!(
                agrees,
                "leader {leader} settled owner {owner:?} of consumer group {group_id} on \
                 ns {ns:?}, but partition primary {primary} installed {installed:?} \
                 (seed={seed:#x})",
            );
            let primary_state = sim.partition_consensus_state(usize::from(primary), ns);
            for replica in 0..sim.replica_count {
                let index = usize::from(replica);
                if sim.is_crashed(replica)
                    || sim.partition_incarnation(index, ns)
                        != sim.partition_incarnation(usize::from(primary), ns)
                    || sim
                        .partition_consensus_state(index, ns)
                        .map(|state| state.commit_min)
                        != primary_state.as_ref().map(|state| state.commit_min)
                {
                    continue;
                }
                assert_eq!(
                    sim.installed_group_owner(index, ns, group_id),
                    installed,
                    "replica {replica} and primary {primary} installed different owners at the same committed prefix \
                     (ns={ns:?}, group={group_id}, seed={seed:#x})",
                );
            }
            compared += 1;
        }
    }
    compared
}

/// Require every live replica that hosts the primary's incarnation of a workload
/// namespace, and committed its retirement operation, to
/// have retired it and to hold neither an owner nor an offset for it.
///
/// A retirement stays in force along the committed prefix, so a replica past that
/// commit point without it applied other state than it committed. Judging each
/// replica by its own retirement would pass one that never applied any.
/// Returns how many retired groups that covered, counted per replica.
fn assert_retired_owners_removed(
    sim: &Simulator,
    workload: &Workload,
    leader: usize,
    live: &[usize],
    seed: u64,
) -> usize {
    let Ok(leader) = u8::try_from(leader) else {
        return 0;
    };
    let mut compared = 0;
    for &namespace in &workload.options.namespaces {
        let Some(primary) = sim.primary_index(namespace) else {
            continue;
        };
        let incarnation = sim.partition_incarnation(usize::from(primary), namespace);
        let hosts: Vec<_> = live
            .iter()
            .filter(|&&replica| sim.partition_incarnation(replica, namespace) == incarnation)
            .filter_map(|&replica| {
                sim.replicas[replica]
                    .partition_shard(namespace)
                    .plane
                    .partitions()
                    .get_by_ns(&namespace)
                    .map(|partition| (replica, partition))
            })
            .collect();
        for group_id in deleted_group_ids(sim, leader, namespace) {
            let Some(retired_at) = hosts
                .iter()
                .filter(|(_, partition)| partition.consumer_group_retired(group_id))
                .filter_map(|(_, partition)| {
                    let journal = &partition.log.journal().inner;
                    journal
                        .repair_headers_in(1..=partition.consensus().commit_min())
                        .into_iter()
                        .find_map(|(op, header)| {
                            if header.operation != Operation::RetireConsumerGroupOwners {
                                return None;
                            }
                            let entry = journal.repair_entry(op)?;
                            let catalog = RetireConsumerGroupOwnersRequest::decode_from(
                                &entry.as_slice()
                                    [size_of::<iggy_binary_protocol::PrepareHeader>()..],
                            )
                            .expect("committed retirement catalog decodes");
                            catalog.retires(group_id).then_some(op)
                        })
                })
                .min()
            else {
                continue;
            };
            for &(replica, partition) in &hosts {
                let committed = partition.consensus().commit_min();
                if committed < retired_at {
                    continue;
                }
                let retired = partition.consumer_group_retired(group_id);
                let owner = partition.consumer_group_owner(group_id);
                let offset = partition
                    .retained_consumer_offsets(ConsumerKind::ConsumerGroup)
                    .into_iter()
                    .find(|&(consumer, _)| u64::from(consumer) == group_id);
                assert!(
                    retired && owner.is_none() && offset.is_none(),
                    "replica {replica} committed ns {namespace:?} through op {committed}, at or \
                     past op {retired_at} of a replica that retired consumer group {group_id}, \
                     but has retired={retired} owner={owner:?} offset={offset:?} (seed={seed:#x})",
                );
                compared += 1;
            }
        }
    }
    compared
}

fn assert_partition_offsets_agree(
    sim: &Simulator,
    workload: &Workload,
    live: &[usize],
    seed: u64,
) -> usize {
    let mut compared_with_shadow = 0;
    for &namespace in &workload.options.namespaces {
        let mut committed = BTreeMap::new();
        for &replica in live {
            let Some(partition) = sim.replicas[replica]
                .partition_shard(namespace)
                .plane
                .partitions()
                .get_by_ns(&namespace)
            else {
                continue;
            };
            let mut offsets = BTreeMap::new();
            for kind in ConsumerKind::ALL {
                for (consumer, offset) in partition.retained_consumer_offsets(kind) {
                    offsets.insert((kind.as_code(), consumer), offset);
                }
            }
            let prefix = (
                partition.created_revision(),
                partition.consensus().commit_min(),
            );
            if let Some((other, expected)) = committed.get(&prefix) {
                assert_eq!(
                    &offsets, expected,
                    "replicas {replica} and {other} have different consumer offsets at committed prefix {prefix:?} \
                     (ns={namespace:?}, seed={seed:#x})"
                );
            }
            if workload.strict_outcome_oracle()
                && workload.options.ack_quorum_ratio >= 1.0
                && sim.primary_index(namespace).map(usize::from) == Some(replica)
            {
                let expected: BTreeMap<_, _> = workload
                    .shadow
                    .consumer_offsets
                    .iter()
                    .filter(|((ns, _, _), _)| *ns == namespace)
                    .map(|((_, kind, consumer), offset)| ((*kind, *consumer), *offset))
                    .collect();
                assert_eq!(
                    offsets, expected,
                    "consumer offsets differ from the serial acknowledged shadow (ns={namespace:?}, seed={seed:#x})"
                );
                compared_with_shadow += 1;
            }
            committed.insert(prefix, (replica, offsets));
        }
    }
    compared_with_shadow
}

/// Fail if a revocation pending in the metadata of replica `leader` aged past
/// [`PENDING_REVOCATION_TIMEOUTS_MAX`] rebalancing timeouts.
pub(crate) fn assert_no_stale_revocations(sim: &Simulator, leader: usize, seed: u64) {
    let now = sim.realtime_micros();
    let bound = pending_revocation_bound_micros();
    for transition in sim.replicas[leader].shards[0]
        .plane
        .metadata()
        .mux_stm
        .streams()
        .consumer_group_pending_revocations()
    {
        let age = now.saturating_sub(transition.created_at);
        assert!(
            age <= bound,
            "pending revocation {transition:?} on leader {leader} is {age} us old, past \
             the {bound} us bound (seed={seed:#x})",
        );
    }
}

/// The age, in microseconds, past which a pending revocation is stale.
fn pending_revocation_bound_micros() -> u64 {
    PENDING_REVOCATION_TIMEOUTS_MAX * rebalancing_timeout_micros()
}

/// Read one replica's committed metadata. `read` enters the committed (left)
/// buffer of the left-right state machine, so uncommitted writes are invisible.
fn read_replica_state(replica: &Replica) -> ReplicaState {
    (
        read_committed_metadata(replica),
        read_committed_settings(replica),
    )
}

fn read_committed_metadata(replica: &Replica) -> CommittedMetadata {
    let stm = &replica.plane.metadata().mux_stm;

    let (streams, topics, consumer_groups) = stm.streams().read(|inner| {
        let mut streams = BTreeSet::new();
        let mut topics = BTreeSet::new();
        let mut consumer_groups = BTreeSet::new();
        for (_, stream) in &inner.items {
            let stream_name = stream.name.to_string();
            for (_, topic) in &stream.topics {
                let topic_name = topic.name.to_string();
                for group in topic.consumer_groups.values() {
                    consumer_groups.insert((
                        stream_name.clone(),
                        topic_name.clone(),
                        group.name.to_string(),
                    ));
                }
                topics.insert((stream_name.clone(), topic_name));
            }
            streams.insert(stream_name);
        }
        (streams, topics, consumer_groups)
    });

    let users = stm.users().read(|inner| {
        inner
            .items
            .iter()
            .map(|(_, user)| user.username.to_string())
            .collect()
    });

    CommittedMetadata {
        streams,
        topics,
        users,
        consumer_groups,
    }
}

fn read_committed_settings(replica: &Replica) -> CommittedSettings {
    let stm = &replica.plane.metadata().mux_stm;
    let (streams, topics) = stm.streams().read(|inner| {
        let mut streams = BTreeMap::new();
        let mut topics = BTreeMap::new();
        for (_, stream) in &inner.items {
            let stream_name = stream.name.to_string();
            for (_, topic) in &stream.topics {
                topics.insert(
                    (stream_name.clone(), topic.name.to_string()),
                    topic.options.clone(),
                );
            }
            streams.insert(stream_name, (stream.created_op, stream.options.clone()));
        }
        (streams, topics)
    });
    let users = stm.users().read(|inner| {
        inner
            .items
            .iter()
            .map(|(_, user)| {
                (
                    user.username.to_string(),
                    (user.status, user.options.clone()),
                )
            })
            .collect()
    });
    CommittedSettings {
        streams,
        topics,
        users,
    }
}

/// Project the shadow's predicted entity sets into the comparable shape. All
/// shadow names carry [`WORKLOAD_PREFIX`], so this lines up with
/// [`CommittedMetadata::workload_owned`].
fn shadow_metadata(shadow: &Shadow) -> CommittedMetadata {
    CommittedMetadata {
        streams: shadow.stream_names.iter().cloned().collect(),
        topics: shadow.topic_names.iter().cloned().collect(),
        users: shadow.user_names.iter().cloned().collect(),
        consumer_groups: shadow.consumer_group_names.iter().cloned().collect(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packet::PacketSimulatorOptions;
    use crate::partition_repair_driver_tests::cluster;
    use crate::tests::{commit_sends_everywhere, complete_metadata_op};
    use crate::workload::actions::Action;
    use crate::workload::effect::Effect;
    use crate::workload::options::{ActionWeights, WorkloadOptions};
    use crate::workload::{create_fixture_groups, run_group_lifecycle};
    use iggy_binary_protocol::batch::{BATCH_HEADER_SIZE, BATCH_MESSAGE_HEADER_SIZE};
    use iggy_binary_protocol::{HEADER_SIZE, Operation};
    use iggy_common::{IggyDuration, IggyExpiry, TopicUpdateOptions};
    use journal::Journal;
    use server_common::iobuf::Owned;
    use std::time::Duration;

    const SEED: u64 = 0x000F_F5E7;
    const CLIENT: u128 = 1;
    const CONSUMER: u32 = 7;
    const OFFSET: u64 = 9;
    /// Batches the data fixture commits, one message each.
    const SENDS: usize = 3;
    /// First payload byte of a `SendMessages` prepare: past the prepare header,
    /// the batch header and the frame header of its first message.
    const FIRST_PAYLOAD_BYTE: usize = HEADER_SIZE + BATCH_HEADER_SIZE + BATCH_MESSAGE_HEADER_SIZE;

    fn namespace() -> IggyNamespace {
        IggyNamespace::new(1, 1, 0)
    }

    fn offset_fixture() -> (Simulator, Workload) {
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });
        let mut sim = Simulator::new(
            3,
            std::iter::once(CLIENT),
            PacketSimulatorOptions {
                node_count: 3,
                client_count: 1,
                seed: SEED,
                ..Default::default()
            },
        );
        sim.init_partition(namespace());
        let workload = Workload::new(WorkloadOptions::new(SEED, 3, vec![namespace()]));
        (sim, workload)
    }

    fn seed_offset(sim: &Simulator, replica: usize) {
        sim.replicas[replica]
            .partition_shard(namespace())
            .plane
            .partitions()
            .get_by_ns(&namespace())
            .unwrap()
            .seed_recovered_consumer_offset(ConsumerKind::Consumer, CONSUMER, OFFSET, OFFSET);
    }

    #[test]
    fn given_committed_setting_changes_when_reading_replica_state_should_see_each() {
        const STREAM: &str = "wl-settings-stream";
        const TOPIC: &str = "wl-settings-topic";
        const USER: &str = "wl-settings-user";
        let (mut sim, client) = cluster(SEED);
        sim.register_client_with_primary(&client);
        let state = |sim: &Simulator| read_replica_state(&sim.replicas[0].shards[0]);
        complete_metadata_op(&mut sim, &client, client.create_stream(STREAM));
        complete_metadata_op(&mut sim, &client, client.create_topic(STREAM, TOPIC, 1));
        complete_metadata_op(
            &mut sim,
            &client,
            client.create_user(USER, "pw-settings", UserStatus::Active.as_code()),
        );
        let created = state(&sim);

        let expiry = IggyDuration::from(Duration::from_secs(60));
        let options = TopicUpdateOptions {
            message_expiry: Some(IggyExpiry::ExpireDuration(expiry)),
            ..TopicUpdateOptions::default()
        };
        complete_metadata_op(
            &mut sim,
            &client,
            client.update_topic(STREAM, TOPIC, &options),
        );
        let expiry_set = state(&sim);
        assert_ne!(
            created, expiry_set,
            "a topic option change must be compared"
        );

        let deactivate = client.update_user(USER, None, Some(UserStatus::Inactive));
        complete_metadata_op(&mut sim, &client, deactivate);
        let deactivated = state(&sim);
        assert_ne!(
            expiry_set, deactivated,
            "a user status change must be compared"
        );

        complete_metadata_op(&mut sim, &client, client.delete_topic(STREAM, TOPIC));
        let first_incarnation = state(&sim);
        complete_metadata_op(&mut sim, &client, client.delete_stream(STREAM));
        complete_metadata_op(&mut sim, &client, client.create_stream(STREAM));
        assert_ne!(
            first_incarnation,
            state(&sim),
            "a recreated stream of the same name must be compared"
        );
    }

    #[test]
    #[should_panic(expected = "different consumer offsets at committed prefix")]
    fn given_a_backup_offset_divergence_when_quiesced_should_fail_the_oracle() {
        let (sim, workload) = offset_fixture();
        seed_offset(&sim, 1);
        assert_partition_offsets_agree(&sim, &workload, &[0, 1, 2], SEED);
    }

    #[test]
    #[should_panic(expected = "consumer offsets differ from the serial acknowledged shadow")]
    fn given_an_unacknowledged_offset_effect_when_quiesced_should_fail_the_shadow_oracle() {
        let (sim, mut workload) = offset_fixture();
        workload.options.ack_quorum_ratio = 1.0;
        for replica in 0..3 {
            seed_offset(&sim, replica);
        }
        assert_partition_offsets_agree(&sim, &workload, &[0, 1, 2], SEED);
    }

    #[test]
    fn given_matching_acknowledged_offsets_when_quiesced_should_pass_the_oracle() {
        let (sim, mut workload) = offset_fixture();
        workload.options.ack_quorum_ratio = 1.0;
        for replica in 0..3 {
            seed_offset(&sim, replica);
        }
        workload.shadow.apply(Effect::OffsetStored {
            key: (namespace(), ConsumerKind::Consumer.as_code(), CONSUMER),
            value: OFFSET,
        });
        assert_partition_offsets_agree(&sim, &workload, &[0, 1, 2], SEED);
    }

    /// [`offset_fixture`] with [`SENDS`] batches committed on every replica.
    fn data_fixture() -> (Simulator, Workload) {
        let (mut sim, workload) = offset_fixture();
        let client = SimClient::new(CLIENT);
        sim.register_client_with_primary(&client);
        commit_sends_everywhere(&mut sim, &client, namespace(), SENDS);
        (sim, workload)
    }

    /// The partition primary and its two backups.
    fn replica_roles(sim: &Simulator) -> (u8, u8, u8) {
        let primary = sim
            .primary_index(namespace())
            .expect("the group has a primary");
        let mut backups = (0..sim.replica_count).filter(|&replica| replica != primary);
        match (backups.next(), backups.next()) {
            (Some(first), Some(second)) => (primary, first, second),
            _ => panic!("a three-replica group has two backups"),
        }
    }

    /// The test-only fault: flip one payload byte of the newest committed batch
    /// `replica` holds. Its header, the partition offsets and every journal index
    /// stay as they were, so only the applied data differs.
    fn flip_applied_payload_byte(sim: &Simulator, replica: u8) {
        let partition = sim.replicas[usize::from(replica)]
            .partition_shard(namespace())
            .plane
            .partitions()
            .get_by_ns(&namespace())
            .expect("the replica hosts the partition");
        let journal = &partition.log.journal().inner;
        let (op, _) = journal
            .repair_headers_in(1..=partition.consensus().commit_min())
            .into_iter()
            .rev()
            .find(|(_, header)| header.operation == Operation::SendMessages)
            .expect("the replica holds a committed batch");
        let mut entry = journal
            .repair_entry(op)
            .expect("a retained header has its entry")
            .as_slice()
            .to_vec();
        entry[FIRST_PAYLOAD_BYTE] ^= u8::MAX;
        assert_eq!(
            futures::executor::block_on(journal.remove_range(op..=op))
                .expect("the in-memory journal removes the entry"),
            1,
            "the flipped batch must replace a resident entry, not shadow a retained one"
        );
        futures::executor::block_on(journal.append(Owned::copy_from_slice(&entry).into()))
            .expect("the in-memory journal appends the entry");
    }

    #[test]
    fn given_equal_applied_data_when_converged_should_compare_every_committed_record() {
        let (sim, mut workload) = data_fixture();

        let report = assert_converged(&sim, &mut workload);

        assert_eq!(report.data_namespaces_compared, 1);
        assert_eq!(report.data_records_compared, SENDS);
    }

    /// The negative control. Every prepare identity and offset still agrees, so
    /// no check but the applied data comparison can fail the run.
    #[test]
    #[should_panic(expected = "different applied message data")]
    fn given_a_flipped_payload_byte_on_a_backup_when_converged_should_fail_the_run() {
        let (sim, mut workload) = data_fixture();
        let (primary, backup, _) = replica_roles(&sim);
        flip_applied_payload_byte(&sim, backup);
        assert_eq!(
            sim.offsets(usize::from(backup), namespace()),
            sim.offsets(usize::from(primary), namespace()),
            "the fault must leave the partition offsets agreeing"
        );
        assert_partition_offsets_agree(&sim, &workload, &[0, 1, 2], SEED);
        assert!(
            state_checker::assert_partition_prefixes_agree(&sim, &[namespace()], SEED) > 0,
            "the fault must leave the prepare identities agreeing"
        );

        let _ = assert_converged(&sim, &mut workload);
    }

    /// A backup restarted from its retained log is compared on the batches it
    /// rebuilt. The other backup is down at the check, so every comparison
    /// involves the rebuilt one.
    #[test]
    fn given_a_rebuilt_backup_when_quiesced_should_compare_its_applied_data() {
        let (mut sim, _) = data_fixture();
        let (_, rebuilt, other) = replica_roles(&sim);
        sim.replica_crash(rebuilt);
        sim.replica_restart(rebuilt);
        sim.replica_crash(other);

        let report = state_checker::assert_applied_data_agrees(&sim, &[namespace()], SEED);

        assert_eq!(
            report,
            state_checker::AppliedDataReport {
                namespaces: 1,
                records: SENDS,
            }
        );
    }

    /// A shell cluster hosting [`namespace`] with one logged-in client, and a
    /// workload drawing `weights`.
    fn shell_fixture(weights: ActionWeights) -> (Simulator, Workload, [SimClient; 1]) {
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });
        let mut sim = Simulator::with_shards_shell(
            3,
            1,
            std::iter::once(CLIENT),
            PacketSimulatorOptions {
                node_count: 3,
                client_count: 1,
                seed: SEED,
                ..Default::default()
            },
        );
        sim.init_partition(namespace());
        sim.seed_stream_topic_partition(namespace());
        let clients = [SimClient::new(CLIENT)];
        sim.shell_login(&clients[0]);
        let mut options = WorkloadOptions::new(SEED, 3, vec![namespace()]);
        options.weights = weights;
        (sim, Workload::new(options), clients)
    }

    /// A poll answered while the drain runs is checked on arrival there, not
    /// passed at quiesce, by when every offset it served has long committed.
    #[test]
    fn given_a_poll_answered_during_the_drain_when_drained_should_check_it_on_arrival() {
        const SEND_TICKS: u64 = 500;
        const DRAIN_TICKS: u64 = 20_000;
        let (mut sim, mut workload, clients) =
            shell_fixture(ActionWeights::new(&[(Action::SendMessages, 100)]));
        let mut invariants = Invariants::new();
        crate::workload::run(
            &mut sim,
            &mut workload,
            &clients,
            SEND_TICKS,
            u64::MAX,
            &mut invariants,
        );
        assert!(drive_to_quiesce(
            &mut sim,
            &mut workload,
            &clients,
            DRAIN_TICKS,
            &mut invariants
        ));
        workload.options.weights = ActionWeights::new(&[(Action::PollMessages, 100)]);
        sim.discover_partition_contexts(&clients[0]);
        let (target, poll) = workload
            .build_request(&clients[0])
            .expect("an idle client polls");
        sim.submit_request(clients[0].client_id(), target, poll.into_generic());

        assert!(drive_to_quiesce(
            &mut sim,
            &mut workload,
            &clients,
            DRAIN_TICKS,
            &mut invariants
        ));

        workload.readback.assert_served_committed(&sim, SEED);
        assert_eq!(
            workload.readback.stats().messages_unverifiable_at_quiesce,
            0
        );
        let (_, backup, _) = replica_roles(&sim);
        let journal = &sim.replicas[usize::from(backup)]
            .partition_shard(namespace())
            .plane
            .partitions()
            .get_by_ns(&namespace())
            .unwrap()
            .log
            .journal()
            .inner;
        let sends: Vec<_> = journal
            .repair_headers_in(1..=u64::MAX)
            .into_iter()
            .filter_map(|(op, header)| (header.operation == Operation::SendMessages).then_some(op))
            .collect();
        assert_ne!(sends, [] as [u64; 0]);
        for op in sends {
            futures::executor::block_on(journal.remove_range(op..=op)).unwrap();
        }
        workload.readback.assert_served_committed(&sim, SEED);
        assert!(
            workload.readback.stats().messages_unverifiable_at_quiesce > 0,
            "a missing retained journal body must not count as a full comparison"
        );

        let polls = workload.readback.stats();
        assert_eq!(polls.polls_served, 1, "the poll must serve a message");
        assert_eq!(
            polls.polls_compared + polls.polls_deferred,
            polls.polls_served,
            "the drain must check the served poll on arrival"
        );
    }

    /// A shell cluster in which the scripted group lifecycle retired its groups on
    /// every replica of [`namespace`].
    fn retirement_fixture() -> (Simulator, Workload) {
        let (mut sim, mut workload, clients) = shell_fixture(ActionWeights::lifecycle());
        create_fixture_groups(&mut sim, &mut workload, &clients[0]);
        assert_eq!(
            run_group_lifecycle(&mut sim, &mut workload, &clients, &mut Invariants::new()),
            Some(true),
            "the scripted group lifecycle must complete"
        );
        (sim, workload)
    }

    fn retired_owners_removed(sim: &Simulator, workload: &Workload) -> usize {
        let leader = sim
            .metadata_leader()
            .expect("the metadata group has a primary");
        assert_retired_owners_removed(sim, workload, usize::from(leader), &[0, 1, 2], SEED)
    }

    #[test]
    fn given_retired_groups_when_each_replica_restarts_should_keep_owners_removed() {
        let (mut sim, workload) = retirement_fixture();
        for replica in 0..sim.replica_count {
            sim.replica_crash(replica);
            sim.replica_restart(replica);
            assert!((0..5_000).any(|_| {
                sim.step();
                sim.metadata_leader().is_some()
            }));
            assert_eq!(
                retired_owners_removed(&sim, &workload),
                crate::workload::LIFECYCLE_GROUP_ROUNDS * usize::from(sim.replica_count),
                "replica {replica} must reopen with its committed retirement"
            );
        }
    }

    #[test]
    #[should_panic(expected = "of a replica that retired consumer group")]
    fn given_backup_at_retirement_op_below_peer_head_when_retirement_is_lost_should_fail() {
        let (sim, workload) = retirement_fixture();
        let (_, backup, _) = replica_roles(&sim);
        let partition = sim.replicas[0]
            .partition_shard(namespace())
            .plane
            .partitions()
            .get_by_ns(&namespace())
            .unwrap();
        let retirement_op = partition
            .log
            .journal()
            .inner
            .repair_headers_in(1..=partition.consensus().commit_min())
            .into_iter()
            .find(|(_, header)| header.operation == Operation::RetireConsumerGroupOwners)
            .map(|(op, _)| op)
            .expect("fixture must commit retirement");
        for replica in &sim.replicas {
            let partition = replica
                .partition_shard(namespace())
                .plane
                .partitions()
                .get_by_ns(&namespace())
                .unwrap();
            let later = partition.consensus().commit_min() + 1;
            partition.consensus().advance_commit_max(later);
            partition.consensus().advance_commit_min(later);
        }
        let replica = &sim.replicas[usize::from(backup)];
        let partitions = replica.partition_shard(namespace()).plane.partitions();
        partitions.remove(&namespace());
        assert!(crate::materialise_partition(
            replica,
            namespace(),
            sim.consumer_offsets_max
        ));
        partitions
            .get_by_ns(&namespace())
            .unwrap()
            .consensus()
            .restore_commit_state(retirement_op, retirement_op);
        retired_owners_removed(&sim, &workload);
    }

    #[test]
    fn given_groups_retired_everywhere_when_quiesced_should_compare_every_replica() {
        let (sim, workload) = retirement_fixture();

        assert_eq!(
            retired_owners_removed(&sim, &workload),
            crate::workload::LIFECYCLE_GROUP_ROUNDS * usize::from(sim.replica_count)
        );
    }

    /// The negative control: a backup whose commit point covers the retirements
    /// while its partition state holds none, as a recovery that restored the
    /// commit point but lost the retirement would leave it.
    #[test]
    #[should_panic(expected = "of a replica that retired consumer group")]
    fn given_a_backup_past_the_retirement_without_it_when_quiesced_should_fail_the_oracle() {
        let (sim, workload) = retirement_fixture();
        // Not replica 0, whose view `primary_index` reads.
        let (_, first, second) = replica_roles(&sim);
        let backup = if first == 0 { second } else { first };
        let committed = (0..sim.replica_count)
            .filter_map(|replica| sim.partition_consensus_state(usize::from(replica), namespace()))
            .map(|state| state.commit_min)
            .max()
            .expect("the group has replicas");
        let replica = &sim.replicas[usize::from(backup)];
        let partitions = replica.partition_shard(namespace()).plane.partitions();
        partitions.remove(&namespace());
        assert!(crate::materialise_partition(
            replica,
            namespace(),
            sim.consumer_offsets_max
        ));
        partitions
            .get_by_ns(&namespace())
            .expect("the backup hosts the rebuilt partition")
            .consensus()
            .restore_commit_state(committed, committed);

        let _ = retired_owners_removed(&sim, &workload);
    }
}
