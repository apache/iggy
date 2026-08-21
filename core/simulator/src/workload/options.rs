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

use crate::workload::actions::Action;
use server_common::sharding::IggyNamespace;
use strum::{EnumCount, IntoEnumIterator};

/// Default [`WorkloadOptions::request_timeout_ticks`].
///
/// Four times the primary's commit-broadcast interval
/// (`VsrTimeout::COMMIT_MESSAGE_TICKS`, 50), so a request merely waiting on the next
/// broadcast is never resent, while one lost to a dropped packet or a crashed
/// primary is retried well inside the run budget.
pub const DEFAULT_REQUEST_TIMEOUT_TICKS: u64 = 200;

/// Default [`WorkloadOptions::crash_stability_ticks`]. Long enough that the
/// surviving primary commits past the crashed replica's log, so its rejoin has
/// something to repair.
pub const DEFAULT_CRASH_STABILITY_TICKS: u64 = 300;

/// Default [`WorkloadOptions::restart_stability_ticks`]. Long enough for a
/// rejoined replica to catch up before it becomes a crash candidate again, so a
/// run is not entirely half-repaired replicas.
pub const DEFAULT_RESTART_STABILITY_TICKS: u64 = 500;

/// Per-action sampling weights as percentages. Unlisted variants default
/// to 0 (never picked). Listed weights must sum to 100.
#[derive(Debug, Clone, Copy)]
pub struct ActionWeights {
    weights: [u8; Action::COUNT],
}

impl ActionWeights {
    /// # Panics
    ///
    /// Panics if `entries` contains a duplicate `Action`, or if the
    /// listed weights do not sum to 100. Missing variants implicitly
    /// weight 0.
    #[must_use]
    pub fn new(entries: &[(Action, u8)]) -> Self {
        let mut weights = [0u8; Action::COUNT];
        let mut seen = [false; Action::COUNT];
        for &(action, w) in entries {
            let idx = action as usize;
            assert!(!seen[idx], "duplicate Action {action:?} in ActionWeights");
            seen[idx] = true;
            weights[idx] = w;
        }
        let total: u32 = weights.iter().map(|&w| u32::from(w)).sum();
        assert!(total == 100, "ActionWeights must sum to 100, got {total}");
        Self { weights }
    }

    /// Partition plane only: writes plus consumer-offset traffic, no metadata
    /// mutation. Drains and converges most readily, so it is what a run reaches for
    /// when the question is about replication rather than the state machine.
    #[must_use]
    pub fn partition_only() -> Self {
        Self::new(&[
            (Action::SendMessages, 60),
            (Action::StoreConsumerOffset, 32),
            (Action::DeleteConsumerOffset, 8),
        ])
    }

    /// Metadata plane only, weighted so creates outrun deletes and the shadow keeps
    /// a live population to sample duplicate- and missing-target outcomes against.
    /// `DeleteSegments` is excluded: it resolves against partition state the
    /// partition presets build, so it belongs to a mixed run.
    #[must_use]
    pub fn metadata_only() -> Self {
        Self::new(&[
            (Action::CreateStream, 12),
            (Action::UpdateStream, 6),
            (Action::DeleteStream, 6),
            (Action::PurgeStream, 4),
            (Action::CreateTopic, 12),
            (Action::UpdateTopic, 6),
            (Action::DeleteTopic, 6),
            (Action::PurgeTopic, 4),
            (Action::CreatePartitions, 6),
            (Action::DeletePartitions, 4),
            (Action::CreateConsumerGroup, 6),
            (Action::DeleteConsumerGroup, 4),
            (Action::CreateUser, 6),
            (Action::UpdateUser, 3),
            (Action::DeleteUser, 3),
            (Action::ChangePassword, 3),
            (Action::UpdatePermissions, 3),
            (Action::CreatePersonalAccessToken, 3),
            (Action::DeletePersonalAccessToken, 3),
        ])
    }

    /// Every action equally likely. Widest op coverage per tick, at the cost of a
    /// shallow population per entity kind.
    ///
    /// `Action::COUNT` does not divide 100, so the first `100 % COUNT` actions carry
    /// one extra point. Spread rather than asserting even division, so appending an
    /// `Action` never breaks this preset.
    ///
    /// # Panics
    /// If the spread weights do not sum to 100, which means the remainder
    /// arithmetic is wrong rather than the caller.
    #[must_use]
    pub fn uniform() -> Self {
        let count = u32::try_from(Action::COUNT).expect("Action::COUNT fits u32");
        // Above 100 actions the base weight floors to 0 and the remainder gives the
        // FIRST 100 a weight of 1, leaving the rest at 0: a spread that still sums
        // to 100 while contradicting "every action equally likely". The sum check
        // below cannot see that, so it is caught here.
        assert!(
            count <= 100,
            "uniform() cannot spread 100 points over {count} actions without \
             silently starving the tail; widen the weight scale first"
        );
        let base = 100 / count;
        let remainder = 100 % count;
        let entries: Vec<(Action, u8)> = Action::iter()
            .enumerate()
            .map(|(idx, action)| {
                let extra = u32::try_from(idx).expect("action index fits u32") < remainder;
                let weight = base + u32::from(extra);
                (
                    action,
                    u8::try_from(weight).expect("per-action weight is at most 100"),
                )
            })
            .collect();
        Self::new(&entries)
    }

    #[must_use]
    pub const fn weight(&self, action: Action) -> u8 {
        self.weights[action as usize]
    }
}

impl Default for ActionWeights {
    fn default() -> Self {
        Self::new(&[
            (Action::CreateStream, 5),
            (Action::SendMessages, 70),
            (Action::StoreConsumerOffset, 25),
        ])
    }
}

/// Workload generator knobs. Same `seed` reproduces the same action /
/// payload sequence bit-for-bit.
#[derive(Debug, Clone)]
pub struct WorkloadOptions {
    pub seed: u64,
    pub replica_count: u8,
    /// Number of clients registered with the simulator. Informational;
    /// `Workload` is client-agnostic and tracks in-flight state per
    /// `client_id`. Used by the CLI binary and multi-client tests.
    pub client_count: u8,
    /// Pre-seeded namespaces. Fixture must call `Simulator::init_partition`
    /// for each before driving the workload.
    pub namespaces: Vec<IggyNamespace>,
    pub weights: ActionWeights,
    /// Send-batch size = `batch_size_min + prng.range(batch_size_span)`.
    pub batch_size_min: u32,
    pub batch_size_span: u32,
    /// Probability a `StoreConsumerOffset` request uses `Quorum` vs `NoAck`.
    pub ack_quorum_ratio: f32,
    /// Probability a request targets a non-primary replica (exercises the
    /// redirect / forward path).
    pub target_non_primary_ratio: f32,
    /// Probability that a request is intentionally constructed to fail
    /// validation. Currently unused; reserved.
    pub invalid_request_ratio: f32,
    /// Distinct consumer ids round-robined in `StoreConsumerOffset`.
    pub consumer_pool_size: u32,
    /// Upper bound on offset carried by `StoreConsumerOffset`.
    pub max_offset: u64,
    /// Probability per tick that the driver crashes one eligible replica.
    /// `0.0` disables injection entirely: the fault PRNG draws nothing, so
    /// traffic stays bit-identical.
    pub crash_per_tick_ratio: f32,
    /// Probability per tick that the driver restarts one crashed replica.
    /// Meaningless without `crash_per_tick_ratio`, since nothing is ever down.
    pub restart_per_tick_ratio: f32,
    /// Ticks a replica must stay down before it may be restarted. Keeps a crash
    /// long enough to actually matter: a replica restarted the tick after it
    /// crashed never falls behind, so nothing needs repairing.
    pub crash_stability_ticks: u64,
    /// Ticks a replica must stay up before it may be crashed again. Stops a
    /// single unlucky replica from being crash-looped while its peers never
    /// fail.
    pub restart_stability_ticks: u64,
    /// Leave the primary of every tracked namespace out of the crash pool.
    ///
    /// Defaults to `true`, which is what the driver did unconditionally before
    /// clients could resend: a request lost to a crashed primary was never retried
    /// and stranded the client's only in-flight slot. With resending in place,
    /// `false` puts a view change under live traffic.
    pub spare_primary: bool,
    /// Floor on live replicas the driver will not crash below, preserving a
    /// commit quorum. Defaults to `replica_count / 2 + 1`.
    pub min_survivors: u8,
    /// Ticks a request may stay outstanding before the client resends it.
    ///
    /// Must clear the primary's commit-broadcast interval with room to spare, or the
    /// run spends its budget resending work that was about to be answered, and must
    /// stay well under the tick budget, or a lost request is never retried and its
    /// client's slot strands. `0` disables resending.
    pub request_timeout_ticks: u64,
}

impl WorkloadOptions {
    #[must_use]
    pub fn new(seed: u64, replica_count: u8, namespaces: Vec<IggyNamespace>) -> Self {
        Self {
            seed,
            replica_count,
            client_count: 1,
            namespaces,
            weights: ActionWeights::default(),
            batch_size_min: 1,
            batch_size_span: 4,
            ack_quorum_ratio: 0.5,
            target_non_primary_ratio: 0.0,
            invalid_request_ratio: 0.0,
            consumer_pool_size: 4,
            max_offset: 1_000_000,
            crash_per_tick_ratio: 0.0,
            restart_per_tick_ratio: 0.0,
            crash_stability_ticks: DEFAULT_CRASH_STABILITY_TICKS,
            restart_stability_ticks: DEFAULT_RESTART_STABILITY_TICKS,
            spare_primary: true,
            min_survivors: replica_count / 2 + 1,
            request_timeout_ticks: DEFAULT_REQUEST_TIMEOUT_TICKS,
        }
    }
}
