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

//! Deterministic seed-based workload generator.
//!
//! - `actions::Action`: server command variants.
//! - `ops/<name>.rs`: per-op `sample`, `build_message`, `classify_reply`,
//!   `predicted_effect`.
//! - `shadow::Shadow`: predicted server entity state.
//! - `auditor::ServerAuditor`: in-flight expectations and invariants.
//! - `effect::Effect`: predicted shadow mutation per commit.

pub mod actions;
pub mod auditor;
pub mod effect;
pub mod ids;
pub mod ops;
pub mod options;
pub mod shadow;

use std::collections::HashMap;

use iggy_binary_protocol::{Message, ReplyHeader, RequestHeader};
use rand::RngExt;
use rand_xoshiro::Xoshiro256Plus;
use rand_xoshiro::rand_core::SeedableRng;

use crate::Simulator;
use crate::client::SimClient;
use actions::Action;
use auditor::{OnReply, ServerAuditor};
use effect::SimCommand;
use options::WorkloadOptions;
use shadow::Shadow;

use crate::workload::ops::InFlight;

/// Max in-flight requests per client. Must stay under the consensus
/// pipeline's queue limits.
pub const CLIENT_REQUEST_QUEUE_MAX: usize = 1;

pub struct Workload {
    prng: Xoshiro256Plus,
    pub auditor: ServerAuditor,
    pub shadow: Shadow,
    pub options: WorkloadOptions,
    /// Number of in-flight requests per client.
    ///
    /// TODO: reap on client disconnect; bounded today by the fixed
    /// `Simulator::new` set.
    in_flight_per_client: HashMap<u128, usize>,
    /// Debug counter for `sample()` returning `None`. Useful when an
    /// outcome-first generation lands and divergence in `apply` branches
    /// could silently shift the PRNG trace.
    samples_none: u64,
}

impl Workload {
    #[must_use]
    pub fn new(options: WorkloadOptions) -> Self {
        let prng = Xoshiro256Plus::seed_from_u64(options.seed);
        let shadow = Shadow::new(options.namespaces.clone(), ids::IdPermutation::Identity);
        Self {
            prng,
            auditor: ServerAuditor::new(),
            shadow,
            options,
            in_flight_per_client: HashMap::new(),
            samples_none: 0,
        }
    }

    /// True if the client has a free in-flight slot.
    #[must_use]
    pub fn client_idle(&self, client_id: u128) -> bool {
        self.in_flight_per_client
            .get(&client_id)
            .copied()
            .unwrap_or(0)
            < CLIENT_REQUEST_QUEUE_MAX
    }

    /// Build the next request for `client`. Returns the message and target
    /// replica index, or `None` if the client has no idle slot or
    /// `ops::sample` could not synthesize an input.
    ///
    /// Note: `pick_action` and `pick_target_replica` advance the PRNG
    /// even when `sample` returns `None` (e.g. an op needs a live stream
    /// but the shadow is empty). Today this is harmless because every
    /// op classifies as `Outcome::Success`. Once outcome-first generation
    /// lands, a divergence in the Success/Failure split inside `sample`
    /// could shift the PRNG trace; the `samples_none` counter exists to
    /// flag that case during development.
    pub fn build_request(&mut self, client: &SimClient) -> Option<(u8, Message<RequestHeader>)> {
        if !self.client_idle(client.client_id()) {
            return None;
        }

        let action = self.pick_action();
        let target = self.pick_target_replica();

        let Some((input, outcome)) =
            ops::sample(action, &mut self.shadow, &mut self.prng, &self.options)
        else {
            self.samples_none += 1;
            return None;
        };
        let message = ops::build_message(client, &input);

        let header = message.header();
        let key = (client.client_id(), header.request);
        self.auditor.record_in_flight(
            key,
            InFlight {
                action,
                input,
                outcome,
                request_namespace: header.namespace,
            },
        );
        *self
            .in_flight_per_client
            .entry(client.client_id())
            .or_insert(0) += 1;

        Some((target, message))
    }

    /// Validate and apply a reply. Returns [`SimCommand`]s the driver
    /// must run against the simulator (e.g. `init_partition`); the
    /// auditor stays transport-agnostic.
    ///
    /// Returns an empty `Vec` for unknown replies (duplicate or stale
    /// at-least-once) and for `OnReply::NsMismatch`. See
    /// [`auditor::ServerAuditor::on_reply`] for the per-variant contract.
    #[must_use = "returned SimCommands must be applied; call apply_sim_commands or use Workload::run"]
    pub fn on_reply(&mut self, reply: &Message<ReplyHeader>) -> Vec<SimCommand> {
        let header = reply.header();
        let key = (header.client, header.request);
        let entry = match self.auditor.on_reply(key, header) {
            OnReply::Match(entry) => entry,
            OnReply::NsMismatch => {
                // Entry consumed; release slot, skip effects (misrouted).
                self.decrement_in_flight(header.client);
                return Vec::new();
            }
            OnReply::Unknown => return Vec::new(),
        };

        // v2.4: every op currently classifies as `Outcome::Success`
        // because server-ng hardcodes `ReplyHeader.context = 0`. The
        // `debug_assert_eq!` locks the contract from day one: once
        // outcomes split, any classify_reply / sample mismatch will
        // panic in debug builds before silently corrupting the audit.
        let classified = ops::classify_reply(entry.action, header);
        debug_assert_eq!(
            classified, entry.outcome,
            "classify_reply produced a different outcome than sample expected: \
             action={:?} classified={classified:?} expected={:?}",
            entry.action, entry.outcome,
        );

        let effect = ops::predicted_effect(&entry.input, &entry.outcome);
        let result = self.shadow.apply(effect);

        // Skip note_committed on no-op apply (e.g. AddTopic after a
        // concurrent RemoveStream) so commits_per_action tracks shadow.
        if result.applied {
            self.auditor.note_committed(entry.action);
        }

        self.decrement_in_flight(header.client);

        result.sim_commands
    }

    /// Release one in-flight slot. Panics on underflow so a future
    /// double-decrement surfaces instead of being silently clamped.
    ///
    /// # Panics
    /// Panics if no entry exists for `client`, or if the counter is 0.
    fn decrement_in_flight(&mut self, client: u128) {
        let count = self
            .in_flight_per_client
            .get_mut(&client)
            .expect("decrement_in_flight: no entry for client; record_in_flight must precede");
        *count = count
            .checked_sub(1)
            .expect("in_flight underflow: per-client counter went below 0");
    }

    /// Debug counter for `sample()` returning `None`. Surfaces sampling
    /// preconditions that aren't met (e.g. shadow has no live stream
    /// when `DeleteStream` is drawn).
    #[must_use]
    pub const fn samples_none(&self) -> u64 {
        self.samples_none
    }

    fn pick_action(&mut self) -> Action {
        use strum::IntoEnumIterator;

        let r: u32 = self.prng.random_range(0..100);
        let weights = &self.options.weights;
        let mut cum: u32 = 0;
        for action in Action::iter() {
            cum += u32::from(weights.weight(action));
            if r < cum {
                return action;
            }
        }
        unreachable!("ActionWeights sum to 100; r < 100 must hit a bucket")
    }

    fn pick_target_replica(&mut self) -> u8 {
        let f: f32 = self.prng.random();
        if f < self.options.target_non_primary_ratio && self.options.replica_count > 1 {
            self.prng.random_range(1..self.options.replica_count)
        } else {
            0
        }
    }
}

/// Drive the simulator until `tick_budget` elapses or `replies_target`
/// replies are seen. Returns the number of replies seen.
pub fn run(
    sim: &mut Simulator,
    workload: &mut Workload,
    clients: &[SimClient],
    tick_budget: u64,
    replies_target: u64,
) -> u64 {
    let mut replies_seen = 0u64;
    for _ in 0..tick_budget {
        for client in clients {
            if let Some((target, msg)) = workload.build_request(client) {
                sim.submit_request(client.client_id(), target, msg.into_generic());
            }
        }
        for reply in sim.step() {
            let cmds = workload.on_reply(&reply);
            apply_sim_commands(sim, &cmds);
            replies_seen += 1;
        }
        if replies_seen >= replies_target {
            break;
        }
    }
    replies_seen
}

/// Apply `SimCommand`s returned by [`Workload::on_reply`].
///
/// Callers must invoke this (or [`run`]) for every batch of returned
/// commands; the auditor itself stays transport-agnostic so the workload
/// can be reused outside the in-process simulator.
pub fn apply_sim_commands(sim: &mut Simulator, cmds: &[SimCommand]) {
    for cmd in cmds {
        match cmd {
            SimCommand::InitPartition { ns } => sim.init_partition(*ns),
        }
    }
}
