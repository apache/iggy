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
pub mod invariants;
pub mod ops;
pub mod options;
pub mod oracle;
pub mod shadow;

use crate::Simulator;
use crate::client::SimClient;
use crate::workload::ops::InFlight;
use actions::Action;
use auditor::{OnReply, ServerAuditor};
use effect::SimCommand;
use iggy_binary_protocol::{ReplyHeader, RequestHeader, result_code};
use invariants::Invariants;
use metadata::stm::result::result_code_recognized;
use options::WorkloadOptions;
use rand::RngExt;
use rand_xoshiro::Xoshiro256Plus;
use rand_xoshiro::rand_core::SeedableRng;
use server_common::Message;
use shadow::Shadow;
use std::collections::{HashMap, HashSet};

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
    /// Debug counter for `sample()` returning `None` (a targeted outcome whose
    /// shadow precondition is unmet). Flags PRNG-trace drift during development.
    samples_none: u64,
    /// Assert the targeted outcome equals the committed one. Sound only for a
    /// fully serial run (one client, one in-flight slot), where the shadow equals
    /// committed server state at sample time so the target is always realized.
    /// Gated on `client_count == 1 && CLIENT_REQUEST_QUEUE_MAX == 1`.
    strict_outcome_oracle: bool,
}

impl Workload {
    #[must_use]
    pub fn new(options: WorkloadOptions) -> Self {
        let prng = Xoshiro256Plus::seed_from_u64(options.seed);
        let shadow = Shadow::new(options.namespaces.clone(), ids::IdPermutation::Identity);
        // Both halves of the soundness precondition (see the field doc). Coupling
        // to the queue max disarms strict equality if it is raised, rather than
        // letting the assert fire on a legitimately raced outcome (a 2nd in-flight
        // request sampled against the shadow before the 1st commits).
        let strict_outcome_oracle = options.client_count == 1 && CLIENT_REQUEST_QUEUE_MAX == 1;
        Self {
            prng,
            auditor: ServerAuditor::new(),
            shadow,
            options,
            in_flight_per_client: HashMap::new(),
            samples_none: 0,
            strict_outcome_oracle,
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

    /// Total in-flight requests across all clients. Read by the
    /// [`Invariants`]; draws no PRNG.
    #[must_use]
    pub(crate) fn total_in_flight(&self) -> usize {
        self.in_flight_per_client.values().copied().sum()
    }

    /// Aggregate in-flight ceiling: one queue's worth per declared client.
    /// Fixtures set `client_count` to the number of driven clients, the same
    /// coupling `strict_outcome_oracle` relies on.
    #[must_use]
    pub(crate) fn in_flight_bound(&self) -> usize {
        usize::from(self.options.client_count) * CLIENT_REQUEST_QUEUE_MAX
    }

    /// True when the run is fully serial (one client, one in-flight slot), the
    /// regime where the shadow equals committed server state. Gates the
    /// quiesce-time entity oracle the same way it gates the per-op equality
    /// oracle.
    #[must_use]
    pub(crate) const fn strict_outcome_oracle(&self) -> bool {
        self.strict_outcome_oracle
    }

    /// Build the next request for `client`. Returns the message and target
    /// replica index, or `None` if the client has no idle slot or
    /// `ops::sample` could not synthesize an input.
    ///
    /// Note: `pick_action`/`pick_target_replica`/`pick_outcome` draw from the
    /// PRNG before `sample` runs, so they advance the trace even when `sample`
    /// returns `None` (a targeted outcome whose precondition is unmet, e.g. a
    /// duplicate-name target with an empty shadow). `samples_none` counts these.
    pub fn build_request(&mut self, client: &SimClient) -> Option<(u8, Message<RequestHeader>)> {
        if !self.client_idle(client.client_id()) {
            return None;
        }

        let action = self.pick_action();
        let target = self.pick_target_replica();
        let outcome_id = self.pick_outcome(action);

        let Some((input, outcome)) = ops::sample(
            action,
            &mut self.shadow,
            &mut self.prng,
            &self.options,
            outcome_id,
        ) else {
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
    ///
    /// # Panics
    /// If a metadata reply carries a committed result code outside the op's
    /// declared result enum (a server bug).
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

        // Decode the committed result code. Metadata replies carry a TB-style
        // result section (see `ApplyReply::to_reply_body`); partition-plane
        // replies do not, hence the `is_metadata` gate.
        let committed_code = if header.operation.is_metadata() {
            // `size` spans header + body, but `Message::try_from` never gates
            // `size >= size_of::<ReplyHeader>()`, so a short `size` reaches here.
            // Assert it (loud server-bug diagnostic) before the slice below
            // panics with start > end.
            assert!(
                header.size as usize >= size_of::<ReplyHeader>(),
                "metadata op {:?} reply size {} below header size {} (client={}, request={})",
                entry.action,
                header.size,
                size_of::<ReplyHeader>(),
                header.client,
                header.request,
            );
            let body = &reply.as_slice()[size_of::<ReplyHeader>()..header.size as usize];
            // A metadata reply always carries a well-formed result section, so
            // `None` is a truncated/corrupt one: a server bug, not a silent Ok
            // (the rejection->success flip "classify never guesses" forbids).
            let Some(code) = result_code(body) else {
                panic!(
                    "metadata op {:?} reply has a truncated or corrupt result section \
                     (client={}, request={})",
                    entry.action, header.client, header.request,
                );
            };
            // The state machine only commits codes its own result enum declares,
            // so an unrecognized one is a server bug (a race still yields a
            // declared code). TB's "classify never guesses".
            assert!(
                result_code_recognized(header.operation, code),
                "metadata op {:?} returned unrecognized result code {code} \
                 (client={}, request={})",
                entry.action,
                header.client,
                header.request,
            );
            code
        } else {
            0
        };

        // Classify the *actual* committed outcome from the wire result code.
        let classified = ops::classify_reply(entry.action, committed_code);

        // Equality oracle: the targeted outcome must match what committed. Sound
        // only for a fully serial run (see `strict_outcome_oracle`); with several
        // clients a concurrent commit can flip it (a targeted duplicate races a
        // delete), so there the recognized-code check above is the only oracle.
        if self.strict_outcome_oracle {
            assert_eq!(
                classified, entry.outcome,
                "outcome-first oracle: targeted {:?} but committed {classified:?} \
                 (action={:?}, code={committed_code}, client={}, request={})",
                entry.outcome, entry.action, header.client, header.request,
            );
        }

        // Effect-follows-actual: drive the shadow off the committed outcome, never
        // the targeted one. Success mutates; a nonzero code is a committed no-op
        // whose `predicted_effect` is `Effect::None`. Keeps the shadow correct
        // under at-least-once re-execution and races.
        let effect = ops::predicted_effect(&entry.input, &classified);
        if committed_code != 0 {
            self.auditor.note_committed_rejection();
        }
        let result = self.shadow.apply(effect);

        // Count a commit only on a success that mutated the shadow, so
        // `commits_per_action` tracks net shadow state (rejections and no-op
        // applies, e.g. AddTopic after a concurrent RemoveStream, are excluded).
        if committed_code == 0 && result.applied {
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

    /// Pick which declared outcome to target for `action`. Single-outcome ops
    /// (the partition/offset plane: `SendMessages`, `StoreConsumerOffset`, ...)
    /// return 0; multi-outcome ops (most metadata ops) draw one, advancing the
    /// PRNG. Adding an outcome to a single-outcome op, or a weight change to which
    /// ops are sampled, shifts the draw order and reply trace - see the locked
    /// baseline in `workload_replay_is_deterministic`.
    fn pick_outcome(&mut self, action: Action) -> usize {
        let count = ops::outcome_count(action);
        if count <= 1 {
            0
        } else {
            self.prng.random_range(0..count)
        }
    }
}

/// Salt mixed into the workload seed for the fault PRNG, so crash scheduling
/// is reproducible from the seed yet independent of the traffic draw order
/// (the determinism baseline stays valid with injection on).
const FAULT_SEED_SALT: u64 = 0x5A1A_F0E5_FACE_0001;

/// Drive the simulator until `tick_budget` elapses or `replies_target`
/// replies are seen. Returns the number of replies seen.
///
/// The invariants are asserted after every tick, so a consensus or
/// workload regression panics at the tick it occurs (the seed in the message
/// replays it). When `crash_per_tick_ratio > 0` the driver also injects
/// crash-only faults via [`maybe_inject_crash`].
pub fn run(
    sim: &mut Simulator,
    workload: &mut Workload,
    clients: &[SimClient],
    tick_budget: u64,
    replies_target: u64,
) -> u64 {
    let mut invariants = Invariants::new();
    let mut fault_prng = Xoshiro256Plus::seed_from_u64(workload.options.seed ^ FAULT_SEED_SALT);
    let mut replies_seen = 0u64;
    for _ in 0..tick_budget {
        if workload.options.crash_per_tick_ratio > 0.0 {
            maybe_inject_crash(sim, workload, &mut fault_prng);
        }
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
        invariants.check(sim, workload);
        if replies_seen >= replies_target {
            break;
        }
    }
    replies_seen
}

/// With probability `crash_per_tick_ratio`, crash one live non-primary replica,
/// provided doing so leaves at least `min_survivors` live. Crash-only: a
/// crashed replica is never restarted (that needs consensus durability).
///
/// "Non-primary" is partition-plane only: the exclusion set comes from
/// `Simulator::primary_index`, which reads `partitions()`. The metadata-plane
/// primary is not consulted; it is spared only by co-location, since every group
/// starts at view 0 with `primary = view % replica_count` (so replica 0 leads
/// both planes) and `min_survivors` keeps a commit quorum, so no view change
/// moves it. Were the two planes' primaries to diverge, the metadata primary
/// could be crashed.
///
/// Primaries are spared at all because the driver has no request-timeout/resend
/// path: a request lost to a crashed primary would wedge the client's only
/// in-flight slot. Forcing primary crashes (and the view change they trigger)
/// while keeping traffic flowing is future work gated on that resend path.
fn maybe_inject_crash(sim: &mut Simulator, workload: &Workload, prng: &mut Xoshiro256Plus) {
    let live: Vec<u8> = (0..sim.replica_count)
        .filter(|replica_idx| !sim.is_crashed(*replica_idx))
        .collect();
    if live.len() <= usize::from(workload.options.min_survivors) {
        return;
    }
    let roll: f32 = prng.random();
    if roll >= workload.options.crash_per_tick_ratio {
        return;
    }
    let primaries: HashSet<u8> = workload
        .options
        .namespaces
        .iter()
        .filter_map(|ns| sim.primary_index(*ns))
        .collect();
    let eligible: Vec<u8> = live
        .into_iter()
        .filter(|replica_idx| !primaries.contains(replica_idx))
        .collect();
    if eligible.is_empty() {
        return;
    }
    let victim = eligible[prng.random_range(0..eligible.len())];
    sim.replica_crash(victim);
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
