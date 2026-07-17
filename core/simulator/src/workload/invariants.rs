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
use crate::workload::{CLIENT_REQUEST_QUEUE_MAX, Workload};
use server_common::sharding::IggyNamespace;
use std::collections::HashMap;

/// Per-(replica, namespace) high-water marks carried across ticks so each new
/// reading can be compared against the last.
#[derive(Debug, Default)]
pub struct Invariants {
    commit_offset: HashMap<(u8, IggyNamespace), u64>,
    view: HashMap<(u8, IggyNamespace), u64>,
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
    /// - total in-flight requests stay within the per-client queue ceiling.
    ///
    /// Crashed replicas are skipped: their last-seen marks are retained, which
    /// stays correct because both quantities are monotonic across a restart.
    ///
    /// # Panics
    /// On any regression or in-flight overflow. The workload seed is in the
    /// message so the failing run replays deterministically.
    pub fn check(&mut self, sim: &Simulator, workload: &Workload) {
        let seed = workload.options.seed;

        for replica_idx in 0..sim.replica_count {
            if sim.is_crashed(replica_idx) {
                continue;
            }
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
    }

    /// Number of `(replica, namespace)` pairs observed so far. Used by tests to
    /// prove the checks ran over live state rather than vacuously.
    #[cfg(test)]
    #[must_use]
    pub(crate) fn tracked_pairs(&self) -> usize {
        self.commit_offset.len()
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
