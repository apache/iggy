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
//! asserts two things that must hold:
//!
//! - no live replica is ahead of the leader on any namespace (a backup ahead of
//!   the leader is a split-brain / divergence bug),
//! - every live replica agrees with every other on each committed metadata op
//!   they both hold, the real consensus property (see
//!   [`super::state_checker`]),
//! - on a serial run, the workload's predicted [`Shadow`] equals the metadata
//!   committed on the leader, the payoff of the name-keyed shadow.
//!
//! Equality is asserted over the committed PREFIX, not over equal heads: a replica
//! that missed the last commit broadcast, or rejoined recently, may legitimately
//! trail. What it may not do is hold different history at an op it did commit.
//! Requiring equal heads would fail on ordinary lag and say nothing about safety.

use crate::Simulator;
use crate::replica::Replica;
use crate::workload::shadow::Shadow;
use crate::workload::{Workload, apply_sim_commands, resubmit_due, state_checker};
use consensus::{MetadataHandle, Status};
use metadata::impls::metadata::StreamsFrontend;
use std::collections::BTreeSet;
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
const QUIESCE_SETTLE_TICKS: u64 = 200;

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

/// Drain the system after the active workload, then settle.
///
/// Stops submitting new requests and steps until no client request is
/// outstanding, then runs a settle window so trailing backups apply the final
/// committed prepares via the primary's commit broadcast.
///
/// Returns `true` once drained, `false` if `max_ticks` elapses with requests
/// still outstanding (a liveness failure the caller should surface).
#[must_use]
pub fn drive_to_quiesce(sim: &mut Simulator, workload: &mut Workload, max_ticks: u64) -> bool {
    let mut drained = false;
    for _ in 0..max_ticks {
        // The drain keeps resending: a request lost on the way out is never
        // answered, so without retries the drain would spend its whole budget
        // waiting on a reply that cannot arrive.
        workload.tick();
        resubmit_due(sim, workload);
        for reply in sim.step() {
            let cmds = workload.on_reply(&reply);
            apply_sim_commands(sim, &cmds);
        }
        // A resend can land on a transport a restart left unbound, which the server
        // refuses with an eviction. No re-login here, unlike the active driver: the
        // drain submits nothing new, and the refused request was rejected before
        // commit, so forgetting it is what "drained" means.
        for client_id in sim.take_evictions() {
            workload.forget_evicted_client(client_id);
        }
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
            let _ = write!(
                report,
                " | metadata status={:?} view={} log_view={} commit={} primary={}",
                consensus.status(),
                consensus.view(),
                consensus.log_view(),
                consensus.commit_min(),
                consensus.is_primary(),
            );
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
        // Per-client table state. `check_request` admits a metadata request only at
        // exactly `watermark + 1`, so the watermark says whether an outstanding
        // request is still expected, already answered (and owed a cached-reply
        // replay), or ahead of what this replica will accept.
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
#[must_use]
pub fn settle_to_stable_view(sim: &mut Simulator, workload: &mut Workload, max_ticks: u64) -> bool {
    for _ in 0..max_ticks {
        if views_are_settled(sim, workload) {
            return true;
        }
        workload.tick();
        resubmit_due(sim, workload);
        for reply in sim.step() {
            let cmds = workload.on_reply(&reply);
            apply_sim_commands(sim, &cmds);
        }
    }
    views_are_settled(sim, workload)
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
/// If a replica is ahead of the leader, two replicas disagree on a committed op,
/// or the shadow mismatches the leader. The workload seed is in the message so the
/// failing run replays deterministically.
pub fn assert_converged(sim: &Simulator, workload: &mut Workload) {
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
        let Some(leader) = sim.primary_index(ns) else {
            continue;
        };
        let Some(leader_committed) = sim
            .partition_consensus_state(usize::from(leader), ns)
            .map(|state| state.commit_min)
        else {
            continue;
        };
        for &replica_idx in &live {
            if let Some(committed) = sim
                .partition_consensus_state(replica_idx, ns)
                .map(|state| state.commit_min)
            {
                assert!(
                    committed <= leader_committed,
                    "replica {replica_idx} committed {committed} ops exceeds leader {leader} \
                     ({leader_committed}) on ns {ns:?} at quiesce (seed={seed:#x})",
                );
            }
        }
    }

    // Entity oracle: on a serial run the shadow must equal the committed
    // metadata on the leader, the authoritative holder of the metadata log.
    //
    // Runs even when the oracle is disarmed, because a disarmed oracle is an
    // UNKNOWN and this is the measurement that resolves it. Equal means the
    // forgotten request's effect is accounted for and the oracle re-arms; unequal
    // means the unknown did cost the shadow an effect, which is reported rather than
    // asserted, since a failure there would blame the harness's gap on the cluster.
    if !workload.serial_run() {
        return;
    }
    let Some(leader) = metadata_leader(sim, &live) else {
        panic!("no metadata leader live at quiesce (seed={seed:#x})");
    };
    let committed = read_committed_metadata(&sim.replicas[leader].shards[0]).workload_owned();
    let shadow = shadow_metadata(&workload.shadow);
    if workload.strict_outcome_oracle() {
        assert_eq!(
            shadow, committed,
            "shadow diverged from leader-committed metadata at quiesce \
             (leader={leader}, seed={seed:#x})",
        );
    } else if shadow == committed {
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
}

/// The live replica whose metadata consensus is the current primary, i.e. the
/// authoritative holder of the committed metadata log.
fn metadata_leader(sim: &Simulator, live: &[usize]) -> Option<usize> {
    live.iter().copied().find(|&replica_idx| {
        sim.replicas[replica_idx].shards[0]
            .plane
            .metadata()
            .consensus
            .as_ref()
            .is_some_and(|consensus| consensus.is_primary() && consensus.status() == Status::Normal)
    })
}

/// Read one replica's committed metadata. `read` enters the committed (left)
/// buffer of the left-right state machine, so uncommitted writes are invisible.
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
