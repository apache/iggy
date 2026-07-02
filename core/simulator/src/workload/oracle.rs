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
//! - on a serial run, the workload's predicted [`Shadow`] equals the metadata
//!   committed on the leader, the payoff of the name-keyed shadow.
//!
//! Full cross-replica EQUALITY (every live replica holding the same committed
//! log) is the real consensus property, but it is not asserted yet. Backups
//! apply prepares in strict order and drop any gap (`op != current_op + 1` in
//! `metadata::on_replicate` / `iggy_partition`), and there is no log repair
//! (no `RequestPrepare` / state transfer) to refetch a missed prepare. Under
//! realistic reorder/drop a behind backup therefore never catches up and
//! legitimately sits behind the leader at quiesce. Re-enable equality once
//! message repair lands (the state-sync workstream).

use crate::Simulator;
use crate::replica::Replica;
use crate::workload::shadow::Shadow;
use crate::workload::{Workload, apply_sim_commands};
use consensus::{MetadataHandle, Status};
use metadata::impls::metadata::StreamsFrontend;
use std::collections::BTreeSet;

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
    fn workload_owned(mut self) -> Self {
        self.streams
            .retain(|name| name.starts_with(WORKLOAD_PREFIX));
        self.topics
            .retain(|(stream, _)| stream.starts_with(WORKLOAD_PREFIX));
        self.users.retain(|name| name.starts_with(WORKLOAD_PREFIX));
        self.consumer_groups
            .retain(|(stream, _, _)| stream.starts_with(WORKLOAD_PREFIX));
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
        for reply in sim.step() {
            let cmds = workload.on_reply(&reply);
            apply_sim_commands(sim, &cmds);
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

/// Post-drain consensus checks that hold today.
///
/// Asserts no live replica is ahead of the leader, and (on a serial run) that
/// the shadow equals the metadata committed on the leader. See the module docs
/// for why full cross-replica equality is deferred.
///
/// Assumes one stable primary that every live replica agrees on: the leader is
/// `Simulator::primary_index` (a single replica's view), and both checks treat
/// it as the authoritative, most-advanced log. Sound today because the driver
/// spares primaries from crashes, so no view change runs mid-test. Once
/// primary-crash injection lands, live replicas can hold different views and
/// this breaks: it may pick a stale or crashed leader (a correctly-ahead new
/// primary then trips "exceeds leader"), or find no `Normal` primary mid-view
/// change (`metadata_leader` returns `None`). Both are false failures. Fix
/// then: resolve the leader by highest `(view, commit_offset)`, or quiesce
/// until live replicas reconverge to one view before asserting. Crash injection
/// already runs but spares primaries (`maybe_inject_crash`); this is deferred
/// until primary-crash injection lands, itself gated on a request-resend path.
///
/// # Panics
/// If a replica is ahead of the leader or the shadow mismatches the leader. The
/// workload seed is in the message so the failing run replays deterministically.
pub fn assert_converged(sim: &Simulator, workload: &Workload) {
    let seed = workload.options.seed;
    let live: Vec<usize> = (0..sim.replica_count)
        .filter(|replica_idx| !sim.is_crashed(*replica_idx))
        .map(usize::from)
        .collect();
    assert!(
        !live.is_empty(),
        "no live replicas at quiesce (seed={seed:#x})"
    );

    // Safety direction: no live replica may be ahead of the leader on any
    // namespace. A backup may trail (no idle catch-up yet, see module docs),
    // but a backup whose commit_offset exceeds the leader's is a divergence.
    for &ns in &workload.options.namespaces {
        let Some(leader) = sim.primary_index(ns) else {
            continue;
        };
        let Some(leader_offset) = sim
            .offsets(usize::from(leader), ns)
            .map(|o| o.commit_offset)
        else {
            continue;
        };
        for &replica_idx in &live {
            if let Some(offset) = sim.offsets(replica_idx, ns).map(|o| o.commit_offset) {
                assert!(
                    offset <= leader_offset,
                    "replica {replica_idx} commit_offset {offset} exceeds leader {leader} \
                     ({leader_offset}) on ns {ns:?} at quiesce (seed={seed:#x})",
                );
            }
        }
    }

    // Entity oracle: on a serial run the shadow must equal the committed
    // metadata on the leader, the authoritative holder of the metadata log.
    if workload.strict_outcome_oracle() {
        let Some(leader) = metadata_leader(sim, &live) else {
            panic!("no metadata leader live at quiesce (seed={seed:#x})");
        };
        let committed = read_committed_metadata(&sim.replicas[leader]).workload_owned();
        assert_eq!(
            shadow_metadata(&workload.shadow),
            committed,
            "shadow diverged from leader-committed metadata at quiesce \
             (leader={leader}, seed={seed:#x})",
        );
    }
}

/// The live replica whose metadata consensus is the current primary, i.e. the
/// authoritative holder of the committed metadata log.
fn metadata_leader(sim: &Simulator, live: &[usize]) -> Option<usize> {
    live.iter().copied().find(|&replica_idx| {
        sim.replicas[replica_idx]
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
