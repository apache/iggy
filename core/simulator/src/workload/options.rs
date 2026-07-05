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
use strum::EnumCount;

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
            (Action::StoreConsumerOffset2, 25),
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
    /// Probability a `StoreConsumerOffset2` request uses `Quorum` vs `NoAck`.
    pub ack_quorum_ratio: f32,
    /// Probability a request targets a non-primary replica (exercises the
    /// redirect / forward path).
    pub target_non_primary_ratio: f32,
    /// Probability that a request is intentionally constructed to fail
    /// validation. Currently unused; reserved.
    pub invalid_request_ratio: f32,
    /// Distinct consumer ids round-robined in `StoreConsumerOffset2`.
    pub consumer_pool_size: u32,
    /// Upper bound on offset carried by `StoreConsumerOffset2`.
    pub max_offset: u64,
    /// Probability per tick that the driver crashes one live non-primary
    /// replica (crash-only, no restart). `0.0` disables injection: the fault
    /// PRNG draws nothing, so traffic stays bit-identical.
    pub crash_per_tick_ratio: f32,
    /// Floor on live replicas the driver will not crash below, preserving a
    /// commit quorum. Defaults to `replica_count / 2 + 1`.
    pub min_survivors: u8,
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
            min_survivors: replica_count / 2 + 1,
        }
    }
}
