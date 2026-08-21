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
pub mod state_checker;

use crate::Simulator;
use crate::client::SimClient;
use crate::seeds::SimSeeds;
use crate::workload::ops::{InFlight, InFlightOutcome};
use actions::Action;
use auditor::{OnReply, ServerAuditor};
use effect::SimCommand;
use iggy_binary_protocol::{Operation, ReplyHeader, RoutedRequestHeader, result_code};
use iggy_common::IggyError;
use invariants::Invariants;
use metadata::stm::result::result_code_recognized;
use options::WorkloadOptions;
use rand::RngExt;
use rand_xoshiro::Xoshiro256PlusPlus;
use rand_xoshiro::rand_core::SeedableRng;
use server_common::Message;
use shadow::Shadow;
use std::collections::{BTreeMap, HashSet};

/// Max in-flight requests per client. Must stay under the consensus
/// pipeline's queue limits.
pub const CLIENT_REQUEST_QUEUE_MAX: usize = 1;

/// An outstanding request, retained so the client can resend it.
///
/// The encoded message is kept verbatim rather than rebuilt from the sampled
/// `Input`: rebuilding would draw a fresh request id, and a resend must reuse the
/// original, which is what the metadata client table dedups on. A renumbered retry
/// commits a second time instead of returning the cached reply.
struct Outstanding {
    message: Message<RoutedRequestHeader>,
    /// Replica the most recent attempt went to. A resend moves to the next one,
    /// so a client whose primary died eventually finds the new one.
    target: u8,
    /// Tick of the most recent attempt, not of the first.
    attempted_tick: u64,
    attempts: u32,
}

/// One still-unanswered request, as the drain-failure report names it.
///
/// A struct rather than a five-tuple: every field is a bare integer, so a tuple
/// leaves the reader counting positions in the one report read after a failure.
#[derive(Debug, Clone, Copy)]
pub(crate) struct OutstandingRow {
    pub client: u128,
    pub request: u64,
    pub action: Option<Action>,
    /// Replica the most recent attempt went to.
    pub target: u8,
    pub attempts: u32,
}

/// A transport rejection the dispatch path framed into the result section
/// instead of `ReplyHeader::status`.
///
/// `build_result_rejection_reply` uses the result section for the `IggyError`s a
/// client must see typed, stamps the REQUEST's own operation, and leaves `status`
/// at 0. Such a reply is shaped exactly like a commit while carrying a code no op's
/// result enum declares, so it has to be recognized before the committed-code path
/// claims it.
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub(crate) enum TransientRejection {
    /// The request never entered a queue, so it definitely did not commit and is
    /// re-issuable anywhere.
    NotAccepted,
    /// The request may or may not have committed. Only replaying the same request
    /// id settles it.
    NotCommitted,
}

impl TransientRejection {
    /// `RequestAlreadyApplied` is deliberately absent: the op DID commit and only
    /// its reply aged out of the client table's ring, so there is no honest shadow
    /// move to make. Left to the recognized-code assert, the right signal for a
    /// reply ring too small for the retry latency.
    pub(crate) fn from_code(code: u32) -> Option<Self> {
        if code == IggyError::TransientNotAccepted.as_code() {
            Some(Self::NotAccepted)
        } else if code == IggyError::TransientNotCommitted.as_code() {
            Some(Self::NotCommitted)
        } else {
            None
        }
    }
}

pub struct Workload {
    prng: Xoshiro256PlusPlus,
    pub auditor: ServerAuditor,
    pub shadow: Shadow,
    pub options: WorkloadOptions,
    /// Outstanding requests, keyed as the auditor keys its expectations so the two
    /// are removed together.
    ///
    /// A `BTreeMap`, not a `HashMap`: [`Self::due_resends`] walks it and the submit
    /// order is observable, so hash order would make replay diverge from the seed.
    ///
    /// TODO: reap on client disconnect; bounded today by the fixed
    /// `Simulator::new` set.
    outstanding: BTreeMap<(u128, u64), Outstanding>,
    /// Driver tick, advanced by [`Self::tick`]. A driver that never ticks never
    /// resends, which is what the hand-written scenario tests rely on.
    now: u64,
    /// Total resends issued, for the run summary.
    resends: u64,
    /// Client evictions survived, for the run summary. Only the dispatch shell
    /// produces them, and in practice only once replicas restart under it.
    evictions: u64,
    /// Clients whose recovery handshake is in flight, and the tick it last went out
    /// on. Such a client submits nothing: its session is gone, so anything it sent
    /// would be refused and evict it again.
    pending_handshakes: BTreeMap<u128, (u8, u64)>,
    /// Debug counter for `sample()` returning `None` (a targeted outcome whose
    /// shadow precondition is unmet). Flags PRNG-trace drift during development.
    samples_none: u64,
    /// Whether the run is fully serial (one client, one in-flight slot), the
    /// standing precondition for comparing the shadow against committed state.
    /// Fixed at construction, unlike `strict_outcome_oracle`, the arm/disarm bit on
    /// top: an eviction disarms the oracle without making the run concurrent, and
    /// the quiesce comparison still runs to decide whether it can be re-armed.
    serial_run: bool,
    /// Assert the targeted outcome equals the committed one. Sound only on a serial
    /// run, where the shadow equals committed state at sample time so the target is
    /// always realized. Starts at `serial_run`, disarmed by
    /// [`Self::forget_evicted_client`], re-armed by [`Self::rearm_outcome_oracle`].
    strict_outcome_oracle: bool,
}

impl Workload {
    #[must_use]
    pub fn new(options: WorkloadOptions) -> Self {
        let prng = Xoshiro256PlusPlus::seed_from_u64(SimSeeds::derive(options.seed).workload);
        let shadow = Shadow::new(options.namespaces.clone(), ids::IdPermutation::Identity);
        // Both halves of the soundness precondition (see the field doc). Coupling
        // to the queue max disarms strict equality if it is raised, rather than
        // letting the assert fire on a legitimately raced outcome (a 2nd in-flight
        // request sampled against the shadow before the 1st commits).
        let serial_run = options.client_count == 1 && CLIENT_REQUEST_QUEUE_MAX == 1;
        let strict_outcome_oracle = serial_run;
        Self {
            prng,
            auditor: ServerAuditor::new(),
            shadow,
            options,
            outstanding: BTreeMap::new(),
            now: 0,
            resends: 0,
            evictions: 0,
            pending_handshakes: BTreeMap::new(),
            samples_none: 0,
            serial_run,
            strict_outcome_oracle,
        }
    }

    /// True if the client has a free in-flight slot.
    #[must_use]
    pub fn client_idle(&self, client_id: u128) -> bool {
        self.client_in_flight(client_id) < CLIENT_REQUEST_QUEUE_MAX
    }

    /// Total in-flight requests across all clients. Read by the
    /// [`Invariants`]; draws no PRNG.
    #[must_use]
    pub(crate) fn total_in_flight(&self) -> usize {
        self.outstanding.len()
    }

    /// Advance the resend clock by one tick. Called once per driver iteration;
    /// [`Self::due_resends`] measures against it.
    pub const fn tick(&mut self) {
        self.now += 1;
    }

    /// Total resends issued so far.
    #[must_use]
    pub const fn resends(&self) -> u64 {
        self.resends
    }

    /// Total client evictions survived.
    #[must_use]
    pub const fn evictions(&self) -> u64 {
        self.evictions
    }

    /// Forget everything outstanding for a client the cluster evicted.
    ///
    /// An eviction is session-terminal: the server refused the request BEFORE commit
    /// (`Eviction(NoSession)` from an unbound transport, which a replica restart
    /// leaves behind, session bindings living in the per-connection
    /// `SessionManager`). Resending is not an option, the retained message carrying
    /// the old session id; the client logs in again and samples afresh.
    ///
    /// The forgotten request's fate is genuinely unknown, which is why this disarms
    /// the strict outcome oracle. The refusal proves only that the ATTEMPT drawing
    /// it did not commit, and that attempt may have been a resend of a request whose
    /// original committed with its reply lost. The shadow is then missing an effect
    /// that did happen, and every later targeted outcome can disagree with what
    /// commits. Claiming the shadow is still authoritative would turn a known
    /// unknown into a spurious failure.
    ///
    /// Returns how many requests were forgotten.
    pub fn forget_evicted_client(&mut self, client_id: u128) -> usize {
        self.evictions += 1;
        self.strict_outcome_oracle = false;
        self.outstanding.retain(|&(owner, _), _| owner != client_id);
        self.auditor.forget_client(client_id)
    }

    /// Whether this client is waiting on a recovery handshake and so must not be
    /// asked for a request.
    #[must_use]
    pub fn is_recovering(&self, client_id: u128) -> bool {
        self.pending_handshakes.contains_key(&client_id)
    }

    /// Record that a recovery handshake for `client_id` went out to `target`.
    pub fn note_handshake_submitted(&mut self, client_id: u128, target: u8) {
        self.pending_handshakes
            .insert(client_id, (target, self.now));
    }

    /// Handshakes whose reply is overdue, each paired with the replica to retry
    /// against. Rotates targets for the same reason `due_resends` does: the replica
    /// that looked live may be neither reachable nor able to forward.
    ///
    /// The interval is the request timeout rather than something tighter because
    /// every landed register commits and re-fences (see `register_preflight`), so an
    /// eager retry costs the client an extra recovery round trip.
    #[must_use = "returned handshakes must be submitted or the client stays wedged"]
    pub fn due_handshake_resends(&mut self, replica_count: u8) -> Vec<(u128, u8)> {
        let timeout = self.options.request_timeout_ticks;
        if timeout == 0 {
            return Vec::new();
        }
        let now = self.now;
        let replica_count = replica_count.max(1);
        let mut due = Vec::new();
        for (&client_id, (target, submitted)) in &mut self.pending_handshakes {
            if now.saturating_sub(*submitted) < timeout {
                continue;
            }
            *target = (*target + 1) % replica_count;
            *submitted = now;
            due.push((client_id, *target));
        }
        due
    }

    /// Take the pending handshake for the client this reply answers, if it is one.
    ///
    /// Returns the client id so the caller can bind the session; `None` leaves the
    /// reply for [`Self::on_reply`], which is where every non-handshake reply
    /// belongs.
    pub fn take_pending_handshake(&mut self, reply: &Message<ReplyHeader>) -> Option<u128> {
        let header = reply.header();
        if header.operation != Operation::Register {
            return None;
        }
        self.pending_handshakes
            .remove(&header.client)
            .map(|_| header.client)
    }

    /// Requests whose reply has not arrived within
    /// [`WorkloadOptions::request_timeout_ticks`], each paired with the replica
    /// to retry it against. Callers must submit every returned message.
    ///
    /// What a real client's read timeout does, needed for two reasons. A dropped
    /// request or reply otherwise strands the client's only in-flight slot for the
    /// rest of the run, so any packet loss wedges the workload. And a request lost
    /// to a crashed primary can only be answered by the next one, which the client
    /// reaches by rotating its target.
    ///
    /// Safe on both planes but not equally cheap: the metadata plane dedups on the
    /// retained request id and replays the cached reply, while the partition plane
    /// is at-least-once and may commit twice. The shadow models that, `Effect`
    /// application being driven by what committed rather than what was targeted.
    #[must_use = "returned requests must be submitted or the client stays wedged"]
    pub fn due_resends(&mut self) -> Vec<(u8, Message<RoutedRequestHeader>)> {
        let timeout = self.options.request_timeout_ticks;
        if timeout == 0 {
            return Vec::new();
        }
        let replica_count = self.options.replica_count.max(1);
        let now = self.now;
        let mut due = Vec::new();
        for entry in self.outstanding.values_mut() {
            if now.saturating_sub(entry.attempted_tick) < timeout {
                continue;
            }
            entry.target = (entry.target + 1) % replica_count;
            entry.attempted_tick = now;
            entry.attempts += 1;
            due.push((entry.target, entry.message.deep_copy()));
        }
        self.resends += due.len() as u64;
        due
    }

    /// Outstanding requests in key order. Diagnostic only: names what a run was
    /// waiting on when it failed to drain, including which op, since some cannot be
    /// answered twice and a resend of those stalls permanently.
    #[must_use]
    pub(crate) fn outstanding_summary(&self) -> Vec<OutstandingRow> {
        self.outstanding
            .iter()
            .map(|(&key, entry)| OutstandingRow {
                client: key.0,
                request: key.1,
                action: self.auditor.in_flight_action(key),
                target: entry.target,
                attempts: entry.attempts,
            })
            .collect()
    }

    /// In-flight count for one client. Keys are `(client, request)`, so the
    /// client's entries are one contiguous range.
    fn client_in_flight(&self, client_id: u128) -> usize {
        self.outstanding
            .range((client_id, 0)..=(client_id, u64::MAX))
            .count()
    }

    /// Aggregate in-flight ceiling: one queue's worth per declared client.
    /// Fixtures set `client_count` to the number of driven clients, the same
    /// coupling `strict_outcome_oracle` relies on.
    #[must_use]
    pub(crate) fn in_flight_bound(&self) -> usize {
        usize::from(self.options.client_count) * CLIENT_REQUEST_QUEUE_MAX
    }

    /// Whether the entity oracle is currently armed. A driver reports it: a run
    /// whose oracle was disarmed by an eviction and never re-armed asserted nothing
    /// about entity state, and without this looks identical to one that did.
    #[must_use]
    pub const fn strict_outcome_oracle(&self) -> bool {
        self.strict_outcome_oracle
    }

    /// True when the run is fully serial (one client, one in-flight slot), the
    /// regime where the shadow equals committed state. Separate from the arm/disarm
    /// bit above because an eviction disarms the oracle without making the run
    /// concurrent.
    #[must_use]
    pub const fn serial_run(&self) -> bool {
        self.serial_run
    }

    /// Re-arm the outcome oracle after the shadow has been PROVEN to equal
    /// committed state again.
    ///
    /// [`Self::forget_evicted_client`] disarms because a forgotten request's fate is
    /// unknown, and an unknown can leave the shadow missing an effect that happened.
    /// But an unknown is not proof of divergence: once the shadow and the leader's
    /// committed metadata are observed equal at rest, it has resolved in the
    /// shadow's favour.
    ///
    /// Without this, one eviction turns the entity oracle into a no-op for the rest
    /// of the run, and a fault run evicts routinely, so the strongest oracle was
    /// silently off for almost every run that mattered. Callable only from
    /// [`oracle::assert_converged`], which does the comparison this rests on.
    pub(crate) const fn rearm_outcome_oracle(&mut self) {
        self.strict_outcome_oracle = true;
    }

    /// Build the next request for `client`. Returns the message and target
    /// replica index, or `None` if the client has no idle slot or
    /// `ops::sample` could not synthesize an input.
    ///
    /// Note: `pick_action`/`pick_target_replica`/`pick_outcome` draw from the
    /// PRNG before `sample` runs, so they advance the trace even when `sample`
    /// returns `None` (a targeted outcome whose precondition is unmet, e.g. a
    /// duplicate-name target with an empty shadow). `samples_none` counts these.
    pub fn build_request(
        &mut self,
        client: &SimClient,
    ) -> Option<(u8, Message<RoutedRequestHeader>)> {
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
                request_namespace: header.group,
            },
        );
        self.outstanding.insert(
            key,
            Outstanding {
                message: message.deep_copy(),
                target,
                attempted_tick: self.now,
                attempts: 1,
            },
        );

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
                self.release_outstanding(key);
                return Vec::new();
            }
            OnReply::Unknown => return Vec::new(),
        };

        // A pre-commit denial short-circuits everything below. The two channels are
        // mutually exclusive: a reply either commits (status 0, result section
        // present) or is denied before commit (status set, EMPTY body). Reading a
        // result section off a denial finds no bytes, which the metadata branch
        // below would report as a corrupt reply.
        //
        // The op never entered the log, so the shadow must not move and there is
        // nothing to classify. Only the dispatch shell produces these, since
        // authorization runs there, which is why this went unmodelled until the
        // workload ran through the shell.
        if header.status != 0 {
            self.auditor.note_denial(entry.action, header.status);
            self.release_outstanding(key);
            return Vec::new();
        }

        // Decode the committed result code. Metadata replies carry a
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
            // A transport rejection, not a committed outcome. Dispatch refuses a
            // request it cannot place (not the primary, transferring, request queue
            // full, a view change canceled the pending prepare) and answers with
            // the reason in the result section under the request's own operation.
            // Nothing committed, so the committed-code path below must not claim it,
            // and the assert after that would report it as a server bug.
            if let Some(transient) = TransientRejection::from_code(code) {
                self.auditor.note_transient_rejection(entry.action, code);
                match transient {
                    // Never entered a queue, so the shadow must not move and the
                    // client's slot is free for a fresh sample.
                    TransientRejection::NotAccepted => self.release_outstanding(key),
                    // Outcome unknown. Releasing would leave the shadow guessing
                    // whether the op landed, so hold the request outstanding and let
                    // `due_resends` replay the same request id: the primary answers
                    // from its client table if it committed and re-dispatches if it
                    // did not, which turns the unknown into a fact.
                    TransientRejection::NotCommitted => {
                        self.auditor.record_in_flight(key, entry);
                    }
                }
                return Vec::new();
            }

            // The state machine only commits codes its own result enum declares,
            // so an unrecognized one is a server bug (a race still yields a
            // declared code). Classify never guesses.
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

        // `CreatePersonalAccessToken` is the one op whose REPLAY is refused rather
        // than served from the reply cache: the committed secret is unrecoverable,
        // so a re-minted one would not match the stored hash and the metadata plane
        // answers `PersonalAccessTokenAlreadyExists`. On a resend that refusal proves
        // the ORIGINAL attempt committed, so the shadow has to record the token it
        // added or every later name draw is made against a shadow missing one. A
        // FIRST attempt answering the same code is a genuine duplicate, which is why
        // the attempt count is the discriminator.
        let attempts = self.outstanding.get(&key).map_or(1, |entry| entry.attempts);
        let classified = if attempts > 1
            && entry.outcome
                == InFlightOutcome::CreatePersonalAccessToken(
                    ops::create_personal_access_token::Outcome::Ok,
                )
            && classified
                == InFlightOutcome::CreatePersonalAccessToken(
                    ops::create_personal_access_token::Outcome::AlreadyExists,
                ) {
            entry.outcome
        } else {
            classified
        };

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

        self.release_outstanding(key);

        result.sim_commands
    }

    /// Drop a request's retry entry, freeing the client's slot. Paired with the
    /// auditor consuming its expectation for the same key.
    ///
    /// # Panics
    /// If no entry exists for `key`. The auditor reports a match or a namespace
    /// mismatch only for a key it was given, and `build_request` records both sides
    /// together, so a miss means the two drifted.
    fn release_outstanding(&mut self, key: (u128, u64)) {
        assert!(
            self.outstanding.remove(&key).is_some(),
            "no outstanding entry for (client={}, request={}); the auditor \
             matched a key the retry buffer never recorded",
            key.0,
            key.1,
        );
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

/// Drive the simulator until `tick_budget` elapses or `replies_target`
/// replies are seen. Returns the number of replies seen.
///
/// The invariants are asserted after every tick, so a consensus or workload
/// regression panics at the tick it occurs (the seed in the message replays it).
/// Crash and restart injection runs through [`FaultInjector`], idle unless one of
/// the two probabilities is set. Discards the injector; use [`run_with_faults`] to
/// read the crash and restart counts back.
pub fn run(
    sim: &mut Simulator,
    workload: &mut Workload,
    clients: &[SimClient],
    tick_budget: u64,
    replies_target: u64,
) -> u64 {
    let mut injector = FaultInjector::new(workload.options.seed, sim.replica_count);
    run_with_faults(
        sim,
        workload,
        clients,
        tick_budget,
        replies_target,
        &mut injector,
    )
}

/// [`run`] against a caller-owned [`FaultInjector`], so a test can assert what
/// was actually injected instead of trusting the probabilities to have fired.
/// # Panics
/// If `injector` was built for a different replica count than `sim` has.
pub fn run_with_faults(
    sim: &mut Simulator,
    workload: &mut Workload,
    clients: &[SimClient],
    tick_budget: u64,
    replies_target: u64,
    injector: &mut FaultInjector,
) -> u64 {
    // The injector is caller-owned, and it sized `last_transition` from a count
    // nobody has checked against this simulator. Left unchecked the mismatch
    // surfaces as a bare "index out of bounds" from inside `stable_for`, with no
    // seed, replica or injector named, in a harness whose every panic is triaged as
    // "real bug or artifact?".
    assert_eq!(
        injector.replica_count(),
        sim.replica_count,
        "fault injector was built for {} replicas but this simulator has {} \
         (seed={:#x})",
        injector.replica_count(),
        sim.replica_count,
        workload.options.seed,
    );
    let mut invariants = Invariants::new();
    let mut replies_seen = 0u64;
    for _ in 0..tick_budget {
        workload.tick();
        injector.step(sim, workload);
        // Resend before sampling: a timed-out request still holds the client's
        // slot, so `build_request` would decline it anyway.
        resubmit_due(sim, workload);
        resubmit_due_handshakes(sim, workload, clients);
        for client in clients {
            // A client whose session is gone submits nothing: the request would be
            // refused and evict it again, and the eviction would look like a fresh
            // failure rather than the one already being recovered.
            if workload.is_recovering(client.client_id()) {
                continue;
            }
            if let Some((target, msg)) = workload.build_request(client) {
                sim.submit_request(client.client_id(), target, msg.into_generic());
            }
        }
        for reply in sim.step() {
            if bind_recovered_client(sim, workload, clients, &reply) {
                continue;
            }
            let cmds = workload.on_reply(&reply);
            apply_sim_commands(sim, &cmds);
            replies_seen += 1;
        }
        recover_evicted_clients(sim, workload, clients);
        invariants.check(sim, workload);
        if replies_seen >= replies_target {
            break;
        }
    }
    replies_seen
}

/// Crash and restart injection with stability windows: a crash must last a while
/// before it may be repaired, and a repaired replica must run a while before it may
/// fail again.
///
/// Owns the fault PRNG so crash scheduling stays reproducible from the seed yet
/// independent of the traffic draw order. Draws nothing while both probabilities
/// are zero, so a fault-free run replays bit-identically.
pub struct FaultInjector {
    prng: Xoshiro256PlusPlus,
    /// Tick of each replica's last crash or restart, indexed by replica id.
    /// Compared against the stability windows to decide eligibility.
    last_transition: Vec<u64>,
    now: u64,
    crashes: u64,
    restarts: u64,
}

impl FaultInjector {
    #[must_use]
    pub fn new(seed: u64, replica_count: u8) -> Self {
        Self {
            prng: Xoshiro256PlusPlus::seed_from_u64(SimSeeds::derive(seed).faults),
            last_transition: vec![0; usize::from(replica_count)],
            now: 0,
            crashes: 0,
            restarts: 0,
        }
    }

    /// Replicas this injector was built for. A driver checks it against the
    /// simulator it is about to drive; see the assert in [`run_with_faults`].
    #[must_use]
    pub fn replica_count(&self) -> u8 {
        u8::try_from(self.last_transition.len()).unwrap_or(u8::MAX)
    }

    #[must_use]
    pub const fn crashes(&self) -> u64 {
        self.crashes
    }

    #[must_use]
    pub const fn restarts(&self) -> u64 {
        self.restarts
    }

    /// Advance one tick and maybe crash or restart one replica.
    ///
    /// Restart is considered before crash so a single tick never both revives
    /// and kills, which would make the stability windows meaningless.
    pub fn step(&mut self, sim: &mut Simulator, workload: &Workload) {
        self.now += 1;
        self.maybe_restart(sim, workload);
        self.maybe_crash(sim, workload);
    }

    /// With probability `restart_per_tick_ratio`, restart one replica that has
    /// been down at least `crash_stability_ticks`.
    ///
    /// What exercises rejoin: the replica comes back with its durable superblock and
    /// metadata WAL but no volatile consensus state, asks the current view's primary
    /// for a `StartView`, and repairs the log it missed.
    fn maybe_restart(&mut self, sim: &mut Simulator, workload: &Workload) {
        if workload.options.restart_per_tick_ratio <= 0.0 {
            return;
        }
        let eligible: Vec<u8> = (0..sim.replica_count)
            .filter(|replica_idx| sim.is_crashed(*replica_idx))
            .filter(|replica_idx| {
                self.stable_for(*replica_idx) >= workload.options.crash_stability_ticks
            })
            .collect();
        if eligible.is_empty() {
            return;
        }
        let roll: f32 = self.prng.random();
        if roll >= workload.options.restart_per_tick_ratio {
            return;
        }
        let revived = eligible[self.prng.random_range(0..eligible.len())];
        sim.replica_restart(revived);
        self.last_transition[usize::from(revived)] = self.now;
        self.restarts += 1;
    }

    /// With probability `crash_per_tick_ratio`, crash one live replica that has
    /// been up at least `restart_stability_ticks`, provided doing so leaves at
    /// least `min_survivors` live.
    ///
    /// Primaries are excluded unless `spare_primary` is off. The exclusion set comes
    /// from `Simulator::primary_index`, which reads `partitions()`, so it names
    /// partition-plane primaries; the metadata primary is spared only by co-location,
    /// every group starting at view 0 with `primary = view % replica_count`. Once
    /// views diverge across planes it can be crashed even with this on.
    fn maybe_crash(&mut self, sim: &mut Simulator, workload: &Workload) {
        if workload.options.crash_per_tick_ratio <= 0.0 {
            return;
        }
        let live: Vec<u8> = (0..sim.replica_count)
            .filter(|replica_idx| !sim.is_crashed(*replica_idx))
            .collect();
        if live.len() <= usize::from(workload.options.min_survivors) {
            return;
        }
        let roll: f32 = self.prng.random();
        if roll >= workload.options.crash_per_tick_ratio {
            return;
        }
        let primaries: HashSet<u8> = if workload.options.spare_primary {
            workload
                .options
                .namespaces
                .iter()
                .filter_map(|ns| sim.primary_index(*ns))
                .collect()
        } else {
            HashSet::new()
        };
        let eligible: Vec<u8> = live
            .into_iter()
            .filter(|replica_idx| !primaries.contains(replica_idx))
            .filter(|replica_idx| {
                self.stable_for(*replica_idx) >= workload.options.restart_stability_ticks
            })
            .collect();
        if eligible.is_empty() {
            return;
        }
        let victim = eligible[self.prng.random_range(0..eligible.len())];
        sim.replica_crash(victim);
        self.last_transition[usize::from(victim)] = self.now;
        self.crashes += 1;
    }

    /// Ticks since this replica last changed state. A replica that never
    /// transitioned counts from tick 0, so the first crash still has to wait out
    /// `restart_stability_ticks`.
    fn stable_for(&self, replica_idx: u8) -> u64 {
        self.now
            .saturating_sub(self.last_transition[usize::from(replica_idx)])
    }
}

/// Log any evicted client back in, which is what a real client does.
///
/// A replica restart drops its `SessionManager`, bindings being per-connection and
/// volatile, so a client that had a session there is unbound and its next
/// replicated request is refused with `Eviction(NoSession)`. The client table is
/// replicated metadata and survives, so the re-login rebinds the existing entry
/// (bumping its fence epoch) and request numbering continues.
///
/// Outstanding requests are forgotten rather than resent: the retained message
/// carries the old session id, so it would only be refused again. See
/// [`Workload::forget_evicted_client`].
///
/// # Panics
/// If an evicted client id is not one the driver knows about, which would mean the
/// simulator and the driver disagree about who is connected.
fn recover_evicted_clients(sim: &mut Simulator, workload: &mut Workload, clients: &[SimClient]) {
    for client_id in sim.take_evictions() {
        let client = clients
            .iter()
            .find(|client| client.client_id() == client_id)
            .unwrap_or_else(|| {
                panic!("cluster evicted unknown client {client_id}: not one the driver drives")
            });
        workload.forget_evicted_client(client_id);
        // Any live replica: a client dialing a backup is a supported path (the
        // backup forwards the register), and the default target may itself be the
        // replica whose restart caused the eviction, in which case the login just
        // times out.
        let Some(target) = (0..sim.replica_count).find(|idx| !sim.is_crashed(*idx)) else {
            continue;
        };
        // Submitted, not awaited. The blocking helpers step the simulator up to
        // `SETUP_TOTAL_STEPS` times inside this tick, with no `Workload::tick`, no
        // fault injection and no invariant check, which is the per-tick checking
        // `run_with_faults` exists for. The reply is picked up by the driver's loop.
        //
        // `submit_handshake` also picks the frame this simulator's mode can answer.
        // Evictions are not shell-only: `Simulator::step` records them without
        // consulting the mode, and the raw wire ingress sends them for `NoSession`
        // and `SessionTooLow`. A login on the raw path then fails quietly rather
        // than loudly, `SimClient::login` being an `Operation::Register` frame that
        // the raw ingress answers by registering, so the client walks away with a
        // session minted by a path that never read the credentials while
        // `set_shell_wire` ends its coverage of the replicated PAT path.
        sim.submit_handshake(client, target);
        workload.note_handshake_submitted(client_id, target);
    }
}

/// Re-submit any recovery handshake whose reply is overdue.
fn resubmit_due_handshakes(sim: &mut Simulator, workload: &mut Workload, clients: &[SimClient]) {
    for (client_id, target) in workload.due_handshake_resends(sim.replica_count) {
        if let Some(client) = clients
            .iter()
            .find(|client| client.client_id() == client_id)
        {
            sim.submit_handshake(client, target);
        }
    }
}

/// Bind the session a recovery handshake reply carries, returning whether the reply
/// was one. A transient rejection is not an answer, so the handshake stays pending
/// and [`Workload::due_handshake_resends`] retries it.
fn bind_recovered_client(
    sim: &Simulator,
    workload: &mut Workload,
    clients: &[SimClient],
    reply: &Message<ReplyHeader>,
) -> bool {
    let Some(session) = sim.handshake_session(reply) else {
        // Still a handshake reply if it correlates, just not a usable one: leave the
        // entry pending so the retry fires, and swallow it either way, the auditor
        // having no expectation for a handshake.
        return reply.header().operation == Operation::Register
            && workload.is_recovering(reply.header().client);
    };
    let Some(client_id) = workload.take_pending_handshake(reply) else {
        return false;
    };
    if let Some(client) = clients
        .iter()
        .find(|client| client.client_id() == client_id)
    {
        client.bind_session(session);
    }
    true
}

/// Submit every request whose reply is overdue (see [`Workload::due_resends`]).
///
/// The client id rides the retained message's header, so a resend re-enters the
/// network exactly as the original did, only aimed at the next replica.
pub fn resubmit_due(sim: &mut Simulator, workload: &mut Workload) {
    for (target, message) in workload.due_resends() {
        let client_id = message.header().client;
        sim.submit_request(client_id, target, message.into_generic());
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::packet::PacketSimulatorOptions;
    use server_common::sharding::IggyNamespace;

    /// A raw-path eviction is recovered by re-registering, not by a shell login.
    ///
    /// Evictions are not shell-only. `Simulator::step` classifies an `Eviction` frame
    /// without consulting the mode, and the raw wire ingress produces them:
    /// `IggyMetadata::on_request` runs `request_preflight` through
    /// `apply_preflight_consensus_plane`, which sends one for `NoSession` and
    /// `SessionTooLow`.
    ///
    /// The wrong branch fails silently, hence asserting on the wire shape rather
    /// than on a panic: the raw ingress answers `SimClient::login` as a plain
    /// register, so the client ends up with a session minted by a path that never
    /// read the credentials, while `shell_login_via`'s `set_shell_wire` ends its
    /// coverage of the replicated PAT path for good.
    #[test]
    fn a_raw_eviction_is_recovered_by_re_registering() {
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });

        let replica_count: u8 = 3;
        let client_id: u128 = 1;
        let seed = 0xE71C_7100;
        let network_opts = PacketSimulatorOptions {
            node_count: replica_count,
            client_count: 1,
            seed,
            ..PacketSimulatorOptions::default()
        };
        let mut sim = Simulator::new(
            usize::from(replica_count),
            std::iter::once(client_id),
            network_opts,
        );
        assert!(
            !sim.is_shell(),
            "this test covers the raw path; a shell simulator would take the login branch"
        );
        let ns = IggyNamespace::new(1, 1, 0);
        sim.init_partition(ns);

        let client = SimClient::new(client_id);
        sim.register_client_with_primary(&client);

        let options = WorkloadOptions::new(seed, replica_count, vec![ns]);
        let mut workload = Workload::new(options);
        let clients = [client];

        // Stands in for the frame the wire ingress sends: `step` records the id
        // the same way whatever produced it, so the driver sees exactly this.
        sim.evicted.push(client_id);
        recover_evicted_clients(&mut sim, &mut workload, &clients);

        assert!(
            !clients[0].shell_wire(),
            "a raw client was flipped to the client wire shape, so its PAT requests \
             stop exercising the replicated path for the rest of the run"
        );

        // The recovery has to leave a usable session behind. Without a fresh
        // registration the next request is refused with another eviction and
        // nothing commits.
        let replies = run(&mut sim, &mut workload, &clients, 400, u64::MAX);
        assert!(replies > 0, "the recovered client got no replies");
        assert!(
            workload
                .auditor
                .stats()
                .commits_per_action
                .iter()
                .sum::<u64>()
                > 0,
            "the recovered client committed nothing, so its session was not restored"
        );
        assert!(
            sim.take_evictions().is_empty(),
            "the recovered client was evicted again, so the re-registration did not bind"
        );
    }
}
