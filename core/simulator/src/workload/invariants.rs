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

//! Cheap per-tick invariants.
//!
//! These run on every driver tick, not only at quiesce, so a regression is
//! caught at the tick it happens rather than masked by later progress. The
//! checks are read-only and draw no PRNG, so enabling them leaves the reply
//! trace and the determinism baseline (`workload_replay_is_deterministic`)
//! unchanged.

use crate::Simulator;
use crate::workload::state_checker::StateChecker;
use crate::workload::{CLIENT_REQUEST_QUEUE_MAX, Workload};
use consensus::{Consensus, MetadataHandle};
use server_common::sharding::IggyNamespace;
use std::collections::HashMap;

/// Ticks a `Normal` metadata primary may sit behind its own recovery barrier
/// before the run is called wedged.
///
/// A primary below the barrier admits nothing. Transient while a resumed primary
/// re-pipelines its suffix, a handful of round trips, so two orders of magnitude
/// of slack: only a barrier nothing will ever lower trips it.
const RECOVERY_BARRIER_WEDGE_TICKS: u32 = 2_000;

/// Per-(replica, namespace) high-water marks carried across ticks so each new
/// reading can be compared against the last.
#[derive(Debug, Default)]
pub struct Invariants {
    commit_offset: HashMap<(u8, IggyNamespace), u64>,
    view: HashMap<(u8, IggyNamespace), u64>,
    /// Consecutive ticks a replica has been a `Normal` metadata primary still
    /// gated by its recovery barrier. Reset as soon as any of that stops holding.
    barrier_gated_ticks: HashMap<u8, u32>,
    /// Cross-replica committed-log agreement. Runs every tick like the rest, so a
    /// divergence is reported where it appears rather than at the next quiesce.
    state_checker: StateChecker,
}

impl Invariants {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Assert every invariant against the simulator's current state.
    ///
    /// Per live `(replica, namespace)`:
    /// - partition `commit_offset` never regresses,
    /// - consensus `view` never regresses (a view change only advances it).
    ///
    /// Globally:
    /// - total in-flight requests stay within the per-client queue ceiling,
    /// - live replicas agree on every committed metadata op they share, and the
    ///   committed chain stays hash-linked (see [`StateChecker`]).
    ///
    /// Crashed replicas are skipped and their last-seen marks retained, which
    /// holds across a restart for both quantities: the superblock carries `view`,
    /// and `Simulator::retain_partition_logs` carries each partition's log so a
    /// rebuilt partition recovers its offsets instead of reporting zero. Without
    /// that the check trips on a discarded log and calls it a regression.
    ///
    /// # Panics
    /// On any regression or in-flight overflow. The workload seed is in the
    /// message so the failing run replays deterministically.
    pub fn check(&mut self, sim: &Simulator, workload: &Workload) {
        let seed = workload.options.seed;

        for replica_idx in 0..sim.replica_count {
            if sim.is_crashed(replica_idx) {
                self.barrier_gated_ticks.remove(&replica_idx);
                continue;
            }
            self.check_recovery_barrier(sim, seed, replica_idx);
            for &ns in &workload.options.namespaces {
                if let Some(offsets) = sim.offsets(usize::from(replica_idx), ns) {
                    let cur = offsets.commit_offset;
                    if let Some(&prev) = self.commit_offset.get(&(replica_idx, ns)) {
                        assert_no_regression(seed, "commit_offset", replica_idx, ns, prev, cur);
                    }
                    self.commit_offset.insert((replica_idx, ns), cur);
                }
                if let Some(view) = sim.consensus_view(usize::from(replica_idx), ns) {
                    if let Some(&prev) = self.view.get(&(replica_idx, ns)) {
                        assert_no_regression(seed, "consensus view", replica_idx, ns, prev, view);
                    }
                    self.view.insert((replica_idx, ns), view);
                }
            }
        }

        let in_flight = workload.total_in_flight();
        let bound = workload.in_flight_bound();
        assert!(
            in_flight <= bound,
            "in-flight requests {in_flight} exceed bound {bound} \
             (client_count={}, queue_max={CLIENT_REQUEST_QUEUE_MAX}) (seed={seed:#x})",
            workload.options.client_count,
        );

        self.state_checker.check(sim, seed);
    }

    /// Catch a metadata primary permanently shut behind its own recovery barrier.
    ///
    /// The shape `VsrConsensus::redecide_recovery_barrier` fixes, caught from the
    /// outside: a primary that can never clear its barrier drops every request as
    /// `NotReady`, which otherwise surfaces only as an unexplained stall.
    ///
    /// # Panics
    /// When a `Normal` metadata primary sits below its barrier for
    /// [`RECOVERY_BARRIER_WEDGE_TICKS`] consecutive ticks.
    fn check_recovery_barrier(&mut self, sim: &Simulator, seed: u64, replica_idx: u8) {
        let Some(consensus) = sim.replicas[usize::from(replica_idx)].shards[0]
            .plane
            .metadata()
            .consensus
            .as_ref()
        else {
            return;
        };
        let gated = consensus.is_primary()
            && !consensus.has_ceded_primaryship()
            && consensus.is_normal()
            && consensus.commit_max() < consensus.recovery_barrier();
        if !gated {
            self.barrier_gated_ticks.remove(&replica_idx);
            return;
        }
        let ticks = self
            .barrier_gated_ticks
            .entry(replica_idx)
            .and_modify(|ticks| *ticks += 1)
            .or_insert(1);
        assert!(
            *ticks < RECOVERY_BARRIER_WEDGE_TICKS,
            "replica {replica_idx} has been a Normal metadata primary gated by its recovery \
             barrier for {ticks} ticks: barrier={} commit={}..{} view={}. Nothing lowers the \
             barrier, so this primary drops every client request from here on (seed={seed:#x})",
            consensus.recovery_barrier(),
            consensus.commit_min(),
            consensus.commit_max(),
            consensus.view(),
        );
    }

    /// The canonical committed chain built so far. Tests read it to prove the
    /// equality check compared replicas against each other rather than passing
    /// over an empty chain.
    #[must_use]
    pub const fn state_checker(&self) -> &StateChecker {
        &self.state_checker
    }
}

/// Panic if `cur < prev`. Pure so the catch logic is unit-testable without a
/// full simulator; shared by the `commit_offset` and `view` checks.
fn assert_no_regression(
    seed: u64,
    metric: &str,
    replica_idx: u8,
    ns: IggyNamespace,
    prev: u64,
    cur: u64,
) {
    assert!(
        cur >= prev,
        "{metric} regressed on replica {replica_idx} ns {ns:?}: {prev} -> {cur} (seed={seed:#x})",
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ns() -> IggyNamespace {
        IggyNamespace::new(1, 1, 0)
    }

    #[test]
    fn assert_no_regression_allows_forward_and_equal() {
        assert_no_regression(0x00C0_FFEE, "commit_offset", 0, ns(), 5, 5);
        assert_no_regression(0x00C0_FFEE, "commit_offset", 0, ns(), 5, 9);
    }

    #[test]
    #[should_panic(expected = "commit_offset regressed")]
    fn assert_no_regression_rejects_backward() {
        assert_no_regression(0xDEAD_BEEF, "commit_offset", 0, ns(), 10, 5);
    }
}
