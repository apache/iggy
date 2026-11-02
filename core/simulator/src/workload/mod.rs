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
pub mod change_log_oracle;
pub mod effect;
pub mod ids;
pub mod invariants;
pub mod ops;
pub mod options;
pub mod oracle;
pub mod readback;
pub mod shadow;
pub mod state_checker;

use crate::Simulator;
use crate::client::SimClient;
use crate::seeds::SimSeeds;
use crate::workload::ops::{
    InFlight, InFlightInput, InFlightOutcome, join_fixture_group, replace_fixture_topic,
};
use actions::Action;
use auditor::{OnReply, ServerAuditor};
use consensus::{MetadataHandle, PartitionsHandle};
use effect::{Effect, SimCommand};
use iggy_binary_protocol::primitives::partition_history::{ConsumerGroupOwner, PartitionContext};
use iggy_binary_protocol::{
    AckLevel, GenericHeader, KIND_CONSUMER_GROUP, Operation, ReplyHeader, RoutedRequestHeader,
    result_code,
};
use iggy_common::IggyError;
use invariants::Invariants;
use metadata::impls::metadata::StreamsFrontend;
use metadata::stm::result::result_code_recognized;
use options::{ActionWeights, WorkloadOptions};
use rand::RngExt;
use rand_xoshiro::Xoshiro256PlusPlus;
use rand_xoshiro::rand_core::SeedableRng;
use readback::PollReadback;
use server_common::Message;
use server_common::sharding::IggyNamespace;
use shadow::{CapturedOwner, Shadow, ToggleFate};
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
    /// Set by a leader redirect. From then on only a reply from `target` answers
    /// the request: the SDK reads an attempt's result only on the link that
    /// carried it.
    redirected: bool,
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
    /// This attempt never entered a queue. An earlier copy may still commit,
    /// so retries keep the same request id. A backup's `NotPrimary` is one.
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
        if code == IggyError::TransientNotAccepted.as_code()
            || code == IggyError::NotPrimary.as_code()
        {
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
    pub readback: PollReadback,
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
    /// Requests moved to the metadata leader after a `NotPrimary`, for the run
    /// summary.
    redirects: u64,
    /// Partition ops of deleted fixture incarnations witnessed by more than one
    /// replica, for the run summary.
    deleted_partition_ops_compared: usize,
    stale_owner_stores: u64,
    owner_fence_refusals: u64,
    /// Group offset stores that the installed owner committed.
    valid_owner_stores: u64,
    /// Clients whose recovery or redirect handshake is in flight, with the replica
    /// and the tick it last went out on. Such a client submits nothing: its session
    /// is gone or moving, so anything it sent would be refused.
    pending_handshakes: BTreeMap<u128, (u8, u64)>,
    /// The replica each shell client last logged in to, which is the only one
    /// holding its session. Absent until a client logs in mid-run, and always on
    /// the raw path, which has no sessions to bind.
    homes: BTreeMap<u128, u8>,
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
            readback: PollReadback::default(),
            outstanding: BTreeMap::new(),
            now: 0,
            resends: 0,
            evictions: 0,
            redirects: 0,
            deleted_partition_ops_compared: 0,
            stale_owner_stores: 0,
            owner_fence_refusals: 0,
            valid_owner_stores: 0,
            pending_handshakes: BTreeMap::new(),
            homes: BTreeMap::new(),
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

    /// Total requests moved to the metadata leader.
    #[must_use]
    pub const fn redirects(&self) -> u64 {
        self.redirects
    }

    /// Partition ops of deleted fixture incarnations that more than one replica
    /// held when the delete was reached.
    #[must_use]
    pub const fn deleted_partition_ops_compared(&self) -> usize {
        self.deleted_partition_ops_compared
    }

    /// Forget outstanding requests after a successful rebind proves their session
    /// epoch is no longer current. A transport eviction alone does not prove that.
    ///
    /// The forgotten request's fate is genuinely unknown, which is why this disarms
    /// the strict outcome oracle. The refusal proves only that the ATTEMPT drawing
    /// it did not commit, and that attempt may have been a resend of a request whose
    /// original committed with its reply lost. The shadow is then missing an effect
    /// that did happen, and every later targeted outcome can disagree with what
    /// commits. Claiming the shadow is still authoritative would turn a known
    /// unknown into a spurious failure. A forgotten fixture toggle is probed again
    /// instead, see [`Shadow::begin_fixture_toggle`].
    ///
    /// Returns how many requests were forgotten.
    pub fn forget_evicted_client(&mut self, client_id: u128) -> usize {
        self.strict_outcome_oracle = false;
        self.outstanding.retain(|&(owner, _), _| owner != client_id);
        self.readback.forget_client(client_id);
        let forgotten = self.auditor.forget_client(client_id);
        for entry in &forgotten {
            self.finish_fixture_request(client_id, entry, ToggleFate::Unknown);
        }
        forgotten.len()
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
    /// Returns the client id and the replica the handshake last went to, so the
    /// caller can bind the session there; `None` leaves the reply for
    /// [`Self::on_reply`], which is where every non-handshake reply belongs.
    pub fn take_pending_handshake(
        &mut self,
        from: u8,
        reply: &Message<ReplyHeader>,
    ) -> Option<(u128, u8)> {
        let header = reply.header();
        if header.operation != Operation::Register
            || self.pending_handshakes.get(&header.client)?.0 != from
        {
            return None;
        }
        self.pending_handshakes
            .remove(&header.client)
            .map(|(target, _)| (header.client, target))
    }

    /// Hold a request that a replica other than `leader` refused with
    /// `NotPrimary`, retargeted at `leader`, as the SDK's leader redirect does.
    /// The caller logs the client in there, because a session is bound to the
    /// replica it logged in to, and [`Self::rehome`] resends the request once the
    /// login lands. The attempt count stays: the refused attempt was never
    /// admitted, and [`Self::is_stale`] drops a late copy of its refusal.
    ///
    /// Returns `false` for any other reply, for a refusal from `leader` itself
    /// and for a client already logging in, which leaves the reply to
    /// [`Self::on_reply`].
    pub fn redirect(&mut self, reply: &Message<ReplyHeader>, leader: u8) -> bool {
        let header = reply.header();
        let not_primary = IggyError::NotPrimary.as_code();
        let refused = header.status == not_primary
            || (header.status == 0
                && header.operation.is_metadata()
                && reply
                    .as_slice()
                    .get(size_of::<ReplyHeader>()..header.size as usize)
                    .and_then(result_code)
                    == Some(not_primary));
        if !refused || self.is_recovering(header.client) {
            return false;
        }
        let key = (header.client, header.request);
        let (Some(action), Some(entry)) = (
            self.auditor.in_flight_action(key),
            self.outstanding.get_mut(&key),
        ) else {
            return false;
        };
        if entry.target == leader {
            return false;
        }
        entry.target = leader;
        entry.attempted_tick = self.now;
        entry.redirected = true;
        self.redirects += 1;
        self.auditor.note_transient_rejection(action, not_primary);
        true
    }

    /// Whether `reply`, sent by replica `from`, answers a redirected request from a
    /// replica that the request no longer goes to. The driver drops such a reply.
    #[must_use]
    pub fn is_stale(&self, from: u8, reply: &Message<ReplyHeader>) -> bool {
        let header = reply.header();
        self.outstanding
            .get(&(header.client, header.request))
            .is_some_and(|entry| entry.redirected && entry.target != from)
    }

    /// Make `home` the replica `client_id` sends to, now that its session is bound
    /// there, and return the requests it holds for a resend there. Shell only.
    #[must_use = "returned requests must be submitted or the client stays wedged"]
    pub fn rehome(&mut self, client_id: u128, home: u8) -> Vec<Message<RoutedRequestHeader>> {
        self.homes.insert(client_id, home);
        let now = self.now;
        self.outstanding
            .range_mut((client_id, 0)..=(client_id, u64::MAX))
            .map(|(_, entry)| {
                entry.target = home;
                entry.attempted_tick = now;
                entry.message.deep_copy()
            })
            .collect()
    }

    /// Requests whose reply has not arrived within
    /// [`WorkloadOptions::request_timeout_ticks`], each paired with the replica
    /// to retry it against. Callers must submit every returned message. A client
    /// whose login is pending sends nothing: its held request waits for
    /// [`Self::rehome`], as the SDK sends one attempt at a time.
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
        for (&(client_id, _), entry) in &mut self.outstanding {
            if self.pending_handshakes.contains_key(&client_id)
                || now.saturating_sub(entry.attempted_tick) < timeout
            {
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
        // Drawn even for a client with a home, so the PRNG trace does not depend on
        // where clients logged in.
        let drawn = self.pick_target_replica();
        let target = self
            .homes
            .get(&client.client_id())
            .copied()
            .unwrap_or(drawn);
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
        if let InFlightInput::StoreStaleOwnerOffset(input) = &input {
            if input.captured.owner.client_id != client.client_id() {
                self.samples_none += 1;
                return None;
            }
            self.stale_owner_stores += 1;
        }
        let message = ops::build_message(client, &input);

        let header = message.header();
        let key = (client.client_id(), header.request);
        let replacements = self
            .shadow
            .replacements(IggyNamespace::from_raw(header.group));
        self.auditor.record_in_flight(
            key,
            InFlight {
                action,
                input,
                outcome,
                request_namespace: header.group,
                replacements,
            },
        );
        if action == Action::PollMessages {
            self.readback.note_built(key);
        }
        self.outstanding.insert(
            key,
            Outstanding {
                message: message.deep_copy(),
                target,
                attempted_tick: self.now,
                attempts: 1,
                redirected: false,
            },
        );

        Some((target, message))
    }

    #[must_use]
    pub const fn stale_owner_stores(&self) -> u64 {
        self.stale_owner_stores
    }

    #[must_use]
    pub const fn owner_fence_refusals(&self) -> u64 {
        self.owner_fence_refusals
    }

    #[must_use]
    pub const fn valid_owner_stores(&self) -> u64 {
        self.valid_owner_stores
    }

    fn observe_fixture_owners(&mut self, sim: &Simulator) {
        if self.options.weights.weight(Action::StoreStaleOwnerOffset) == 0 {
            return;
        }
        for &namespace in &self.options.namespaces {
            let Some(captured) = primary_owner(sim, namespace, u64::from(shadow::FIXTURE_GROUP_ID))
            else {
                continue;
            };
            self.shadow
                .observe_owner(namespace, captured.owner, captured.context);
        }
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
    /// declared result enum, a fixture request is refused with
    /// `HistoryUnavailable` when no replacement of the fixture was unsettled since
    /// it was sampled, a first fixture toggle finds its topic already gone or
    /// already there, or a group offset write commits from a client that never
    /// joined the group (all server bugs).
    #[must_use = "returned SimCommands must be applied; call apply_sim_commands or use Workload::run"]
    #[allow(clippy::too_many_lines)]
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
        if let InFlightInput::PollMessages(input) = &entry.input {
            let input = input.clone();
            self.on_poll_reply(key, entry, &input, reply);
            return Vec::new();
        }
        let namespace = IggyNamespace::from_raw(entry.request_namespace);
        if entry.action == Action::StoreStaleOwnerOffset {
            assert_ne!(
                header.status, 0,
                "a captured stale owner committed an offset: input={:?}, request={}, seed={:#x}",
                entry.input, header.request, self.options.seed,
            );
            if header.status
                == IggyError::ConsumerGroupPartitionNotOwned(header.client, 0).as_code()
            {
                self.owner_fence_refusals += 1;
            }
        }
        let attempts = self.outstanding.get(&key).map_or(1, |entry| entry.attempts);
        // A refusal proves only that its own attempt did not commit.
        let refusal = if attempts > 1 {
            ToggleFate::Unknown
        } else {
            ToggleFate::Refused
        };

        // A transient refusal covers one attempt, not every network copy. Keep
        // the original request until its committed reply settles the outcome.
        if header.status != 0 {
            if TransientRejection::from_code(header.status).is_some() {
                self.auditor
                    .note_transient_rejection(entry.action, header.status);
                self.auditor.record_in_flight(key, entry);
            } else {
                // Only a replacement can take a fixture's history, and only one
                // that had not settled when the request was sampled.
                let replacements = self.shadow.replacements(namespace);
                assert!(
                    header.status != IggyError::HistoryUnavailable.as_code()
                        || !self.options.namespaces.contains(&namespace)
                        || replacements.may_replace(entry.replacements),
                    "{:?} on fixture ns {namespace:?} got HistoryUnavailable \
                     (replacements at sample {:?}, now {replacements:?}, client={}, \
                     request={}, seed={:#x})",
                    entry.action,
                    entry.replacements,
                    header.client,
                    header.request,
                    self.options.seed,
                );
                self.auditor.note_denial(entry.action, header.status);
                self.finish_fixture_request(header.client, &entry, refusal);
                self.release_outstanding(key);
            }
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
            // Dispatch also frames transient replies in the result section. They
            // establish no committed outcome for earlier copies of this request.
            if TransientRejection::from_code(code).is_some() {
                self.auditor.note_transient_rejection(entry.action, code);
                self.auditor.record_in_flight(key, entry);
                return Vec::new();
            }

            if !header.has_committed_result()
                && code == IggyError::PersonalAccessTokensLimitReached(0, 0).as_code()
            {
                self.auditor.note_denial(entry.action, code);
                self.finish_fixture_request(header.client, &entry, refusal);
                self.release_outstanding(key);
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

            // A pending lifecycle refused a gated op in apply: a committed no-op
            // that its result enum does not declare, so the shadow must not move.
            if code == IggyError::LifecycleBusy.as_code() {
                self.auditor.note_committed_rejection();
                self.finish_fixture_request(header.client, &entry, ToggleFate::Refused);
                self.release_outstanding(key);
                return Vec::new();
            }
            code
        } else {
            0
        };

        // Classify the *actual* committed outcome from the wire result code.
        let classified = ops::classify_reply(entry.action, committed_code);
        self.assert_toggle_not_already_reached(&entry, attempts, committed_code, header);

        // `CreatePersonalAccessToken` is the one op whose REPLAY is refused rather
        // than served from the reply cache: the committed secret is unrecoverable,
        // so a re-minted one would not match the stored hash and the metadata plane
        // answers `PersonalAccessTokenAlreadyExists`. On a resend that refusal proves
        // the ORIGINAL attempt committed, so the shadow has to record the token it
        // added or every later name draw is made against a shadow missing one. A
        // FIRST attempt answering the same code is a genuine duplicate, which is why
        // the attempt count is the discriminator.
        let classified = if attempts > 1
            && entry.outcome
                == InFlightOutcome::CreatePersonalAccessToken(
                    ops::create_personal_access_token::Outcome::Ok,
                )
            && classified
                == InFlightOutcome::CreatePersonalAccessToken(
                    ops::create_personal_access_token::Outcome::AlreadyExists,
                ) {
            // Without the commit proof a client cannot settle its uncertain
            // create, and its metadata requests stay blocked behind it.
            assert!(
                header.has_committed_result(),
                "a refused PAT replay must prove that the original create committed \
                 (client={}, request={})",
                header.client,
                header.request,
            );
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
        // under at-least-once re-execution and races. A partition-plane success
        // sampled across a fixture replacement can belong to the deleted
        // incarnation, whose state the shadow dropped with it.
        let current = header.operation.is_metadata()
            || self.shadow.effect_is_current(namespace, entry.replacements);
        let effect = if current {
            ops::predicted_effect(&entry.input, &classified)
        } else {
            Effect::None
        };
        if committed_code != 0 {
            self.auditor.note_committed_rejection();
        }
        let result = self.shadow.apply(effect);
        let fate = if matches!(
            classified,
            InFlightOutcome::ReplaceFixtureTopic(replace_fixture_topic::Outcome::Reached)
                | InFlightOutcome::JoinFixtureGroup(join_fixture_group::Outcome::Reached)
        ) {
            ToggleFate::Reached { op: header.op }
        } else {
            ToggleFate::Refused
        };
        self.finish_fixture_request(header.client, &entry, fate);
        self.assert_group_writer_joined(&entry, header);
        if committed_code == 0
            && matches!(&entry.input, InFlightInput::StoreConsumerOffset(input)
                if input.consumer_kind == KIND_CONSUMER_GROUP)
        {
            self.valid_owner_stores += 1;
        }

        // Count a commit only on a success that mutated the shadow, so
        // `commits_per_action` tracks net shadow state (rejections and no-op
        // applies, e.g. AddTopic after a concurrent RemoveStream, are excluded).
        if committed_code == 0 && result.applied {
            self.auditor.note_committed(entry.action);
        }

        self.release_outstanding(key);

        result.sim_commands
    }

    /// Move the fixture topic that `entry` toggles, or the fixture group that it
    /// joins for `client_id`, by `fate`. No-op for any other request.
    /// A poll moves no shadow state: the readback oracle checks what it served.
    fn on_poll_reply(
        &mut self,
        key: (u128, u64),
        entry: InFlight,
        input: &ops::poll_messages::Input,
        reply: &Message<ReplyHeader>,
    ) {
        let header = reply.header();
        if header.status != 0 {
            if TransientRejection::from_code(header.status).is_some() {
                self.auditor
                    .note_transient_rejection(entry.action, header.status);
                self.auditor.record_in_flight(key, entry);
            } else {
                self.auditor.note_denial(entry.action, header.status);
                self.readback.forget(key);
                self.release_outstanding(key);
            }
            return;
        }
        let body = &reply.as_slice()[size_of::<ReplyHeader>()..header.size as usize];
        self.readback.on_served(
            key,
            input.ns,
            (
                input.offset,
                input.count,
                input.consumer_kind == KIND_CONSUMER_GROUP,
            ),
            body,
            self.options.seed,
        );
        self.release_outstanding(key);
    }

    fn finish_fixture_request(&mut self, client_id: u128, entry: &InFlight, fate: ToggleFate) {
        match &entry.input {
            InFlightInput::ReplaceFixtureTopic(input) => {
                self.shadow.finish_fixture_toggle(input.fixture, fate);
            }
            InFlightInput::JoinFixtureGroup(input) => {
                self.shadow.finish_group_join(input.join, client_id, fate);
            }
            _ => {}
        }
    }

    /// Fail the run if a first fixture toggle found its topic already as it asked.
    /// Only a probe or a resend can meet that, see
    /// [`replace_fixture_topic::already_reached`].
    fn assert_toggle_not_already_reached(
        &self,
        entry: &InFlight,
        attempts: u32,
        code: u32,
        header: &ReplyHeader,
    ) {
        let InFlightInput::ReplaceFixtureTopic(input) = &entry.input else {
            return;
        };
        assert!(
            attempts > 1
                || self.shadow.fixture_probing(input.fixture)
                || !replace_fixture_topic::already_reached(code),
            "a first {:?} of fixture topic {} got result code {code}, so the topic was \
             already as the toggle asked (client={}, request={}, seed={:#x})",
            input.toggle,
            input.topic,
            header.client,
            header.request,
            self.options.seed,
        );
    }

    /// Fail the run if a group offset write committed from a client that never
    /// joined the group on an incarnation the write could reach. Only a member
    /// becomes an owner, and the write binds only an owner of its own client and
    /// session. This is only the membership check; the partition state checker
    /// compares each offset with the preceding owner installation in its log.
    fn assert_group_writer_joined(&self, entry: &InFlight, header: &ReplyHeader) {
        let (namespace, group_id) = match &entry.input {
            InFlightInput::StoreConsumerOffset(input)
                if input.consumer_kind == KIND_CONSUMER_GROUP =>
            {
                (input.ns, input.consumer_id)
            }
            InFlightInput::DeleteConsumerOffset(input)
                if input.consumer_kind == KIND_CONSUMER_GROUP =>
            {
                (input.ns, input.consumer_id)
            }
            _ => return,
        };
        assert!(
            self.shadow
                .may_have_joined(namespace, group_id, header.client, entry.replacements),
            "{:?} of consumer group {group_id} on ns {namespace:?} committed from client {} \
             that never joined the group on an incarnation it could reach (replacements \
             at sample {:?}, now {:?}, request={}, seed={:#x})",
            entry.action,
            header.client,
            entry.replacements,
            self.shadow.replacements(namespace),
            header.request,
            self.options.seed,
        );
    }

    /// Compare the committed partition prefixes of every fixture namespace whose
    /// delete was reached since the last call. The deleted incarnation is gone
    /// once its topic is recreated, and the quiesce oracle sees only the last one.
    pub fn compare_deleted_fixture_logs(&mut self, sim: &Simulator) {
        let namespaces = self.shadow.take_deleted_namespaces();
        self.deleted_partition_ops_compared +=
            state_checker::assert_partition_prefixes_agree(sim, &namespaces, self.options.seed);
    }

    /// Settle every fixture replacement whose recreated incarnation every live
    /// replica hosts.
    pub fn settle_fixture_replacements(&mut self, sim: &Simulator) {
        self.shadow
            .settle_fixtures(|op, namespaces| sim.incarnation_settled(op, namespaces));
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
    invariants: &mut Invariants,
) -> u64 {
    let mut injector = FaultInjector::new(workload.options.seed, sim.replica_count);
    run_with_faults(
        sim,
        workload,
        clients,
        tick_budget,
        replies_target,
        &mut injector,
        invariants,
    )
}

/// [`run`] against a caller-owned [`FaultInjector`], so a test can assert what
/// was actually injected instead of trusting the probabilities to have fired.
///
/// [`Invariants`] is caller-owned for a second reason: the drain that follows this
/// call carries on with the same checker, and its high-water marks and canonical
/// commit chain are what let a regression spanning the two phases be seen at all.
/// # Panics
/// If `injector` was built for a different replica count than `sim` has.
pub fn run_with_faults(
    sim: &mut Simulator,
    workload: &mut Workload,
    clients: &[SimClient],
    tick_budget: u64,
    replies_target: u64,
    injector: &mut FaultInjector,
    invariants: &mut Invariants,
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
    let mut replies_seen = 0u64;
    for _ in 0..tick_budget {
        workload.tick();
        injector.step(sim, workload);
        replies_seen += drive_tick(sim, workload, clients, invariants);
        if replies_seen >= replies_target {
            break;
        }
    }
    replies_seen
}

/// Create the fixture group of every fixture topic through the log.
///
/// Only for a run that joins fixture groups, which leaves the trace of every
/// other run alone, and only on the shell path: the raw path does not attach the
/// member identity that a replicated join needs.
///
/// # Panics
/// If a create does not commit.
pub fn create_fixture_groups(sim: &mut Simulator, workload: &mut Workload, client: &SimClient) {
    if !sim.is_shell() || !workload.options.weights.draws_fixture_groups() {
        return;
    }
    for (stream, topic) in workload.shadow.enable_fixture_groups() {
        let request = client
            .create_consumer_group(&stream, &topic, shadow::FIXTURE_GROUP_NAME)
            .into_generic();
        let reply = sim.await_setup_reply(
            client,
            0,
            &request,
            Operation::CreateConsumerGroup,
            "create_fixture_groups",
        );
        assert_eq!(
            result_code(reply.body()),
            Some(0),
            "the fixture group of {stream}/{topic} did not commit"
        );
    }
}

/// Recreate every deleted fixture topic and its fixture group, and step until
/// every replacement settles. Returns `false` if `max_ticks` elapse first.
///
/// The usual traffic keeps running until a join also committed on every fixture
/// group since its topic was last replaced, so the owner oracle has assignments
/// to compare. Run before the drain, so the quiesce oracle finds every fixture
/// namespace hosted. Starts no fixture delete, and injects no faults, as the
/// drain does not.
#[must_use]
pub fn restore_fixture_topics(
    sim: &mut Simulator,
    workload: &mut Workload,
    clients: &[SimClient],
    max_ticks: u64,
    invariants: &mut Invariants,
) -> bool {
    workload.shadow.close_fixture_deletes();
    for _ in 0..max_ticks {
        if workload.shadow.fixtures_restored() {
            return true;
        }
        workload.tick();
        drive_tick(sim, workload, clients, invariants);
    }
    workload.shadow.fixtures_restored()
}

/// Name of the group that [`run_group_lifecycle`] creates, retires and
/// recreates. Outside the `wl-` prefix, so the entity oracle leaves it out.
const LIFECYCLE_GROUP_NAME: &str = "sim-lifecycle-group";

/// Creates and deletes of the lifecycle group per run. Each round waits for its
/// retirement, so the second create gives the name a new id and its retirement
/// supersedes one already committed.
const LIFECYCLE_GROUP_ROUNDS: usize = 2;

/// Ticks one wait of [`run_group_lifecycle`] may take.
const LIFECYCLE_GROUP_WAIT_TICKS: u64 = 20_000;

/// Sends of one store of [`run_group_lifecycle`] that may end in a transient
/// refusal.
const LIFECYCLE_GROUP_STORE_ATTEMPTS: usize = 8;

/// Ticks between two sends of one store, for a fault behind a transient
/// refusal to clear.
const LIFECYCLE_GROUP_RETRY_TICKS: u64 = 500;

/// Drive one consumer group through its whole life on the first fixture topic.
///
/// Runs after the drain and before the views settle. The first client creates
/// the group, joins it, stores an offset, leaves, joins again, replays the
/// owner token from before the leave, stores again and deletes the group. Then
/// it creates, joins and deletes the group again.
///
/// The partition must refuse the replayed token on its generation alone, since
/// the same client and session own the partition again. Each delete leaves an
/// owner, and the first one an offset, that only the reconciler removes, so the
/// owner retirement runs end to end. Random draws reach neither path in every
/// run: only a client that regains ownership passes the membership fence.
///
/// Returns `None` unless the run joins fixture groups on the shell path, and
/// otherwise whether every step completed. A step that does not stops the
/// schedule.
pub fn run_group_lifecycle(
    sim: &mut Simulator,
    workload: &mut Workload,
    clients: &[SimClient],
    invariants: &mut Invariants,
) -> Option<bool> {
    if !sim.is_shell() || !workload.options.weights.draws_fixture_groups() {
        return None;
    }
    let (Some(client), Some(&namespace)) = (clients.first(), workload.options.namespaces.first())
    else {
        return Some(false);
    };
    // Fixture 0 is the topic of the first workload namespace.
    let (stream, topic) = workload.shadow.fixture_names(0);
    let mut lifecycle = GroupLifecycle {
        sim,
        workload,
        invariants,
        client,
        namespace,
        stream,
        topic,
    };
    let completed = (0..LIFECYCLE_GROUP_ROUNDS).all(|round| lifecycle.round(round == 0).is_some());
    // A round ends once the partition primary retired the group. The settle
    // window lets the backups apply the retirement before the quiesce oracle
    // compares them.
    step_until(
        lifecycle.sim,
        lifecycle.workload,
        lifecycle.invariants,
        oracle::QUIESCE_SETTLE_TICKS,
        |_| false,
    );
    Some(completed)
}

#[derive(Clone, Copy)]
enum GroupStep {
    Create,
    Join,
    Leave,
    Delete,
}

struct GroupLifecycle<'a> {
    sim: &'a mut Simulator,
    workload: &'a mut Workload,
    invariants: &'a mut Invariants,
    client: &'a SimClient,
    namespace: IggyNamespace,
    stream: String,
    topic: String,
}

impl GroupLifecycle<'_> {
    /// One life of the group, from its create to its delete. `None` when a step
    /// does not complete.
    fn round(&mut self, replay: bool) -> Option<()> {
        self.step(GroupStep::Create)?;
        let group_id = lifecycle_group_id(self.sim, self.namespace)?;
        self.step(GroupStep::Join)?;
        let first = self.await_owner(group_id, |owner| !owner.is_unassigned())?;
        if replay {
            self.ensure_messages()?;
            self.store(group_id, None)?;
            self.step(GroupStep::Leave)?;
            self.await_owner(group_id, |owner| {
                owner.is_unassigned() && owner.generation > first.owner.generation
            })?;
            self.step(GroupStep::Join)?;
            self.await_owner(group_id, |owner| {
                !owner.is_unassigned() && owner.generation > first.owner.generation
            })?;
            self.store(group_id, Some(first))?;
            self.store(group_id, None)?;
        }
        self.step(GroupStep::Delete)?;
        let namespace = self.namespace;
        step_until(
            self.sim,
            self.workload,
            self.invariants,
            LIFECYCLE_GROUP_WAIT_TICKS,
            |sim| {
                lifecycle_group_id(sim, namespace).is_none()
                    && group_retired(sim, namespace, group_id)
            },
        )
        .then_some(())
    }

    /// Send one metadata request and require it to commit successfully.
    fn step(&mut self, step: GroupStep) -> Option<()> {
        let (stream, topic) = (self.stream.as_str(), self.topic.as_str());
        let request = match step {
            GroupStep::Create => {
                self.client
                    .create_consumer_group(stream, topic, LIFECYCLE_GROUP_NAME)
            }
            GroupStep::Join => self
                .client
                .join_consumer_group(stream, topic, LIFECYCLE_GROUP_NAME),
            GroupStep::Leave => {
                self.client
                    .leave_consumer_group(stream, topic, LIFECYCLE_GROUP_NAME)
            }
            GroupStep::Delete => {
                self.client
                    .delete_consumer_group(stream, topic, LIFECYCLE_GROUP_NAME)
            }
        };
        let operation = request.header().operation;
        let target = self.sim.metadata_leader().unwrap_or_default();
        let reply = self.send(target, &request.into_generic(), operation)?;
        (result_code(reply.body()) == Some(0)).then_some(())
    }

    /// Await the reply to `request`. The await steps the cluster without the
    /// invariants, so they check what committed meanwhile.
    fn send(
        &mut self,
        target: u8,
        request: &Message<GenericHeader>,
        operation: Operation,
    ) -> Option<Message<ReplyHeader>> {
        let reply = self
            .sim
            .try_await_setup_reply(self.client, target, request, operation);
        self.invariants.check(self.sim, self.workload);
        reply
    }

    /// Wait until the partition installed an owner that `wanted` accepts, and
    /// every live replica committed it in metadata, so the membership fence of
    /// any replica the next request reaches agrees with the partition. A
    /// joined owner is this client's.
    fn await_owner(
        &mut self,
        group_id: u64,
        wanted: impl Fn(ConsumerGroupOwner) -> bool,
    ) -> Option<CapturedOwner> {
        let (namespace, client_id, session) = (
            self.namespace,
            self.client.client_id(),
            self.client.session_id(),
        );
        let mut settled = None;
        step_until(
            self.sim,
            self.workload,
            self.invariants,
            LIFECYCLE_GROUP_WAIT_TICKS,
            |sim| {
                settled = settled_owner(sim, namespace, group_id).filter(|captured| {
                    wanted(captured.owner)
                        && (captured.owner.is_unassigned()
                            || (captured.owner.client_id, captured.owner.session)
                                == (client_id, session))
                });
                settled.is_some()
            },
        );
        settled
    }

    /// A store refuses an empty partition with `InvalidOffset` before its owner
    /// check, so send until one message is acknowledged.
    fn ensure_messages(&mut self) -> Option<()> {
        if self.workload.shadow.sends_committed(self.namespace) > 0 {
            return Some(());
        }
        let weights = std::mem::replace(
            &mut self.workload.options.weights,
            ActionWeights::new(&[(Action::SendMessages, 100)]),
        );
        let clients = std::slice::from_ref(self.client);
        let mut sent = false;
        for _ in 0..LIFECYCLE_GROUP_WAIT_TICKS {
            sent = self.workload.shadow.sends_committed(self.namespace) > 0
                && self.workload.total_in_flight() == 0;
            if sent {
                break;
            }
            self.workload.tick();
            drive_tick(self.sim, self.workload, clients, self.invariants);
        }
        self.workload.options.weights = weights;
        sent.then_some(())
    }

    /// Store the newest acknowledged offset as the installed owner, or with the
    /// token of `stale`, which the partition must refuse. A transient refusal,
    /// such as a backup's `NotPrimary`, comes before any owner check, so the
    /// store is sent again after a pause.
    fn store(&mut self, group_id: u64, stale: Option<CapturedOwner>) -> Option<()> {
        let consumer_id = u32::try_from(group_id).ok()?;
        let offset = self
            .workload
            .shadow
            .sends_committed(self.namespace)
            .checked_sub(1)?;
        let mut status = self.send_store(consumer_id, offset, stale)?;
        for _ in 1..LIFECYCLE_GROUP_STORE_ATTEMPTS {
            if TransientRejection::from_code(status).is_none() {
                break;
            }
            step_until(
                self.sim,
                self.workload,
                self.invariants,
                LIFECYCLE_GROUP_RETRY_TICKS,
                |_| false,
            );
            status = self.send_store(consumer_id, offset, stale)?;
        }
        let Some(captured) = stale else {
            self.workload.valid_owner_stores += u64::from(status == 0);
            return (status == 0).then_some(());
        };
        assert_ne!(
            status, 0,
            "a replayed owner token committed an offset: captured={captured:?}, seed={:#x}",
            self.workload.options.seed,
        );
        self.workload.stale_owner_stores += 1;
        if status == IggyError::ConsumerGroupPartitionNotOwned(0, 0).as_code() {
            self.workload.owner_fence_refusals += 1;
        }
        Some(())
    }

    /// Send one store to the partition primary and return the reply status.
    fn send_store(
        &mut self,
        consumer_id: u32,
        offset: u64,
        stale: Option<CapturedOwner>,
    ) -> Option<u32> {
        let request = if let Some(captured) = stale {
            ops::store_stale_owner_offset::build_message(
                self.client,
                &ops::store_stale_owner_offset::Input {
                    captured,
                    group_id: consumer_id,
                    offset,
                },
            )
        } else {
            self.sim.discover_partition_contexts(self.client);
            self.client.store_consumer_offset(
                self.namespace,
                KIND_CONSUMER_GROUP,
                consumer_id,
                offset,
                AckLevel::Quorum,
            )
        };
        let target = self.sim.primary_index(self.namespace).unwrap_or_default();
        let reply = self.send(
            target,
            &request.into_generic(),
            Operation::StoreConsumerOffset,
        )?;
        Some(reply.header().status)
    }
}

/// Step until `done`, answering every other reply, for at most `max_ticks`.
fn step_until(
    sim: &mut Simulator,
    workload: &mut Workload,
    invariants: &mut Invariants,
    max_ticks: u64,
    mut done: impl FnMut(&Simulator) -> bool,
) -> bool {
    for _ in 0..max_ticks {
        if done(sim) {
            return true;
        }
        workload.tick();
        for reply in sim.step() {
            let cmds = workload.on_reply(&reply);
            apply_sim_commands(sim, &cmds);
        }
        invariants.check(sim, workload);
    }
    done(sim)
}

/// The id of [`LIFECYCLE_GROUP_NAME`] in the metadata of the leader.
fn lifecycle_group_id(sim: &Simulator, namespace: IggyNamespace) -> Option<u64> {
    let leader = sim.metadata_leader()?;
    sim.replicas[usize::from(leader)].shards[0]
        .plane
        .metadata()
        .mux_stm
        .streams()
        .read(|inner| {
            inner
                .items
                .get(namespace.stream_id())?
                .topics
                .get(namespace.topic_id())?
                .consumer_groups
                .iter()
                .find(|(_, group)| group.name.as_ref() == LIFECYCLE_GROUP_NAME)
                .map(|(&group_id, _)| group_id)
        })
}

/// Whether the partition primary of `namespace` retired `group_id`.
fn group_retired(sim: &Simulator, namespace: IggyNamespace, group_id: u64) -> bool {
    sim.primary_index(namespace).is_some_and(|primary| {
        sim.replicas[usize::from(primary)]
            .partition_shard(namespace)
            .plane
            .partitions()
            .get_by_ns(&namespace)
            .is_some_and(|partition| partition.consumer_group_retired(group_id))
    })
}

/// The owner the partition primary installed for `group_id`, once every live
/// replica committed it in metadata with no successor pending.
fn settled_owner(
    sim: &Simulator,
    namespace: IggyNamespace,
    group_id: u64,
) -> Option<CapturedOwner> {
    let captured = primary_owner(sim, namespace, group_id)?;
    let owner = captured.owner;
    (0..sim.replica_count)
        .filter(|&replica| !sim.is_crashed(replica))
        .all(|replica| {
            sim.replicas[usize::from(replica)].shards[0]
                .plane
                .metadata()
                .mux_stm
                .streams()
                .read(|inner| {
                    inner
                        .items
                        .get(namespace.stream_id())
                        .and_then(|stream| stream.topics.get(namespace.topic_id()))
                        .and_then(|topic| topic.consumer_groups.get(&group_id))
                        .and_then(|group| group.assignments.get(&namespace.partition_id()))
                        .is_some_and(|assignment| {
                            assignment.pending.is_none()
                                && assignment.owner.map_or_else(
                                    || owner.is_unassigned(),
                                    |committed| committed == owner,
                                )
                        })
                })
        })
        .then_some(captured)
}

/// The owner the partition primary of `namespace` installed for `group_id`, or
/// the default owner when it installed none, with the context that a store by
/// that owner carries.
fn primary_owner(
    sim: &Simulator,
    namespace: IggyNamespace,
    group_id: u64,
) -> Option<CapturedOwner> {
    let primary = usize::from(sim.primary_index(namespace)?);
    let incarnation = sim.partition_incarnation(primary, namespace)?;
    let owner = sim
        .installed_group_owner(primary, namespace, group_id)
        .unwrap_or_default();
    Some(CapturedOwner {
        namespace,
        owner,
        context: PartitionContext {
            incarnation,
            owner_generation: owner.generation,
            metadata_op: sim.replicas[primary].shards[0]
                .plane
                .metadata()
                .applied_frontier()
                .get(),
        },
    })
}

/// One driver tick after the clock and the fault injector moved: resends, a new
/// request from each idle client, the replies, the logs of deleted fixtures,
/// evicted clients, settled fixture replacements and the invariants. Returns the
/// replies the workload saw.
fn drive_tick(
    sim: &mut Simulator,
    workload: &mut Workload,
    clients: &[SimClient],
    invariants: &mut Invariants,
) -> u64 {
    // Resend before sampling: a timed-out request still holds the client's
    // slot, so `build_request` would decline it anyway.
    resubmit_due(sim, workload);
    resubmit_due_handshakes(sim, workload, clients);
    workload.observe_fixture_owners(sim);
    workload.readback.observe_liveness(sim);
    for client in clients {
        // A client whose session is gone submits nothing: the request would be
        // refused and evict it again, and the eviction would look like a fresh
        // failure rather than the one already being recovered.
        if workload.is_recovering(client.client_id()) {
            continue;
        }
        sim.discover_partition_contexts(client);
        if let Some((target, msg)) = workload.build_request(client) {
            sim.submit_request(client.client_id(), target, msg.into_generic());
        }
    }
    let mut replies_seen = 0;
    for (from, reply) in sim.step_with_senders() {
        if bind_recovered_client(sim, workload, clients, from, &reply) {
            continue;
        }
        replies_seen += 1;
        if workload.is_stale(from, &reply) || follow_leader_redirect(sim, workload, clients, &reply)
        {
            continue;
        }
        let cmds = workload.on_reply(&reply);
        apply_sim_commands(sim, &cmds);
    }
    workload.readback.check_served(sim, workload.options.seed);
    workload.compare_deleted_fixture_logs(sim);
    recover_evicted_clients(sim, workload, clients);
    workload.settle_fixture_replacements(sim);
    invariants.check(sim, workload);
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
/// replicated metadata and survives. Rebinding the same identity and secret keeps
/// its epoch, so requests remain outstanding until the handshake proves whether
/// their retry session survived.
///
/// # Panics
/// If an evicted client id is not one the driver knows about, which would mean the
/// simulator and the driver disagree about who is connected.
fn recover_evicted_clients(sim: &mut Simulator, workload: &mut Workload, clients: &[SimClient]) {
    for (client_id, target) in sim.take_evictions() {
        let client = clients
            .iter()
            .find(|client| client.client_id() == client_id)
            .unwrap_or_else(|| {
                panic!("cluster evicted unknown client {client_id}: not one the driver drives")
            });
        workload.evictions += 1;
        if workload.is_recovering(client_id) {
            continue;
        }
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
///
/// On the shell the client then sends to the replica it logged in to, and a
/// request held by [`follow_leader_redirect`] goes out there.
fn bind_recovered_client(
    sim: &mut Simulator,
    workload: &mut Workload,
    clients: &[SimClient],
    from: u8,
    reply: &Message<ReplyHeader>,
) -> bool {
    let Some(session) = sim.handshake_session(reply) else {
        // Still a handshake reply if it correlates, just not a usable one: leave the
        // entry pending so the retry fires, and swallow it either way, the auditor
        // having no expectation for a handshake.
        return reply.header().operation == Operation::Register
            && workload.is_recovering(reply.header().client);
    };
    let Some((client_id, home)) = workload.take_pending_handshake(from, reply) else {
        return false;
    };
    if let Some(client) = clients
        .iter()
        .find(|client| client.client_id() == client_id)
    {
        if client.session_id() != session {
            workload.forget_evicted_client(client_id);
        }
        client.bind_session(session);
    }
    if sim.is_shell() {
        for message in workload.rehome(client_id, home) {
            sim.submit_request(client_id, home, message.into_generic());
        }
    }
    true
}

/// Answer a `NotPrimary` from a backup the way the SDK does: log in to the
/// metadata leader and resend the request there once the login lands. Returns
/// whether the reply was consumed.
///
/// Shell metadata requests only. A partition's primary is independent, so its
/// retries keep rotating instead of returning to the metadata primary. The raw
/// path retains its existing retry rotation and replay trace.
fn follow_leader_redirect(
    sim: &mut Simulator,
    workload: &mut Workload,
    clients: &[SimClient],
    reply: &Message<ReplyHeader>,
) -> bool {
    if !sim.is_shell() || !reply.header().operation.is_metadata() {
        return false;
    }
    let Some(leader) = sim.metadata_leader() else {
        return false;
    };
    let client_id = reply.header().client;
    let Some(client) = clients
        .iter()
        .find(|client| client.client_id() == client_id)
    else {
        return false;
    };
    if !workload.redirect(reply, leader) {
        return false;
    }
    sim.submit_handshake(client, leader);
    workload.note_handshake_submitted(client_id, leader);
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
    use crate::packet::{Packet, PacketSimulatorOptions, ProcessId};
    use crate::partition_repair_driver_tests::is_commit_for;
    use consensus::{MetadataHandle, StateTransferStage, Status};
    use iggy_binary_protocol::namespace::METADATA_GROUP;
    use iggy_binary_protocol::primitives::partition_history::ConsumerGroupOwner;
    use iggy_binary_protocol::requests::consumer_offsets::StoreConsumerOffsetRequest;
    use iggy_binary_protocol::{Command, WireDecode};
    use metadata::stm::result::{ApplyReply, CreateTopicResult, DeleteTopicResult};
    use server_common::sharding::IggyNamespace;
    use std::rc::Rc;

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
        sim.evicted.push((client_id, 0));
        recover_evicted_clients(&mut sim, &mut workload, &clients);

        assert!(
            !clients[0].shell_wire(),
            "a raw client was flipped to the client wire shape, so its PAT requests \
             stop exercising the replicated path for the rest of the run"
        );

        // The recovery has to leave a usable session behind. Without a fresh
        // registration the next request is refused with another eviction and
        // nothing commits.
        let mut invariants = Invariants::new();
        let replies = run(
            &mut sim,
            &mut workload,
            &clients,
            400,
            u64::MAX,
            &mut invariants,
        );
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

    #[test]
    #[allow(clippy::too_many_lines)]
    fn given_a_transport_eviction_when_rebinding_the_same_session_should_finish_the_held_fixture_toggle()
     {
        const SEED: u64 = 0xE71C_7310;
        const REPLICAS: u8 = 3;
        const CLIENT: u128 = 1;
        const PRIMARY: u8 = 0;
        const FIRST_UNBOUND_BACKUP: u8 = 1;
        const SECOND_UNBOUND_BACKUP: u8 = 2;
        const PHASE_TICKS: u64 = 4_000;
        const ONE_TICK: u64 = 1;
        for draining in [false, true] {
            server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
                enabled: false,
                size: iggy_common::IggyByteSize::from(0u64),
                bucket_capacity: 1,
            });
            let mut sim = Simulator::with_shards_shell(
                usize::from(REPLICAS),
                1,
                [CLIENT].into_iter(),
                PacketSimulatorOptions {
                    node_count: REPLICAS,
                    client_count: 1,
                    seed: SEED,
                    ..PacketSimulatorOptions::default()
                },
            );
            let fixture = IggyNamespace::new(1, 1, 0);
            sim.init_partition(fixture);
            let clients = [SimClient::new(CLIENT)];
            sim.shell_login_via(&clients[0], PRIMARY);
            let original_session = clients[0].session_id();
            let mut workload = Workload::new(WorkloadOptions::new(SEED, REPLICAS, vec![fixture]));
            let mut invariants = Invariants::new();
            let mut original_delete = None;

            for (operation, unbound_backup) in [
                (Operation::DeleteTopic, FIRST_UNBOUND_BACKUP),
                (Operation::CreateTopic, SECOND_UNBOUND_BACKUP),
            ] {
                assert!(workload.rehome(CLIENT, unbound_backup).is_empty());
                let request = sample_toggle(&mut workload, &clients[0]);
                assert_eq!(request.header().operation, operation);
                let request_id = request.header().request;
                if operation == Operation::DeleteTopic {
                    original_delete = Some(request.deep_copy());
                }
                let prior_evictions = workload.evictions();
                sim.submit_request(CLIENT, unbound_backup, request.into_generic());
                let mut observed_eviction = false;
                for _ in 0..PHASE_TICKS {
                    if draining {
                        let _ = oracle::drive_to_quiesce(
                            &mut sim,
                            &mut workload,
                            &clients,
                            ONE_TICK,
                            &mut invariants,
                        );
                    } else {
                        workload.tick();
                        drive_tick(&mut sim, &mut workload, &clients, &mut invariants);
                    }
                    if workload.evictions() > prior_evictions && !observed_eviction {
                        assert!(
                            workload.outstanding.contains_key(&(CLIENT, request_id)),
                            "NoSession from an unbound transport must retain the unresolved toggle",
                        );
                        assert!(
                            workload
                                .shadow
                                .begin_fixture_toggle(&mut workload.prng)
                                .is_none()
                        );
                        observed_eviction = true;
                    }
                    if workload.total_in_flight() == 0 {
                        break;
                    }
                }
                assert!(
                    observed_eviction,
                    "the backup must actually refuse the unbound transport"
                );
                assert_eq!(clients[0].session_id(), original_session);
                assert_eq!(
                    workload.total_in_flight(),
                    0,
                    "the retained toggle never resolved"
                );
                assert!(oracle::drive_to_quiesce(
                    &mut sim,
                    &mut workload,
                    &clients,
                    PHASE_TICKS,
                    &mut invariants,
                ));
                let journal = &sim.replicas[usize::from(PRIMARY)].metadata_journal;
                let copies = (1..=journal.last_op().unwrap())
                    .filter_map(|op| journal.entry_sync(op))
                    .filter(|entry| {
                        entry.header().client == CLIENT && entry.header().request == request_id
                    })
                    .count();
                assert_eq!(
                    copies, 1,
                    "recovery must preserve the original request identity"
                );
            }

            workload.settle_fixture_replacements(&sim);
            assert!(workload.shadow.fixtures_restored());
            let recreated = sim.replicas[usize::from(PRIMARY)].shards[0]
                .plane
                .metadata()
                .mux_stm
                .streams()
                .created_revision_for_namespace(fixture)
                .unwrap();
            let original_delete = original_delete.unwrap();
            let original_request = original_delete.header().request;
            sim.submit_request(CLIENT, PRIMARY, original_delete.into_generic());
            let mut replay_answered = false;
            for _ in 0..PHASE_TICKS {
                if sim.step().iter().any(|reply| {
                    reply.header().client == CLIENT && reply.header().request == original_request
                }) {
                    replay_answered = true;
                    break;
                }
            }
            assert!(replay_answered, "the delayed old delete must be processed");
            assert_eq!(
                sim.replicas[usize::from(PRIMARY)].shards[0]
                    .plane
                    .metadata()
                    .mux_stm
                    .streams()
                    .created_revision_for_namespace(fixture),
                Some(recreated),
                "a delayed original delete must not delete the recreated fixture",
            );
        }
    }

    #[test]
    fn given_different_plane_primaries_when_the_partition_transport_is_unbound_should_recover_there()
     {
        const SEED: u64 = 0xE71C_1270;
        const REPLICAS: u8 = 3;
        const CLIENT: u128 = 1;
        const METADATA_PRIMARY: u8 = 0;
        const SAMPLE_ATTEMPTS: usize = 16;

        fn starve_partition_commits(packet: &Packet) -> bool {
            is_commit_for(packet, IggyNamespace::new(1, 1, 0).inner())
        }

        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });
        let mut sim = Simulator::with_shards_shell(
            usize::from(REPLICAS),
            1,
            [CLIENT].into_iter(),
            PacketSimulatorOptions {
                node_count: REPLICAS,
                client_count: 1,
                seed: SEED,
                ..PacketSimulatorOptions::default()
            },
        );
        let fixture = IggyNamespace::new(1, 1, 0);
        sim.init_partition(fixture);
        let clients = [SimClient::new(CLIENT)];
        sim.shell_login_via(&clients[0], METADATA_PRIMARY);
        let original_session = clients[0].session_id();
        for backup in 1..REPLICAS {
            *sim.network.link_drop_packet_fn(
                ProcessId::Replica(METADATA_PRIMARY),
                ProcessId::Replica(backup),
            ) = Some(starve_partition_commits);
        }
        assert!((0..crate::SETUP_TOTAL_STEPS).any(|_| {
            sim.step();
            (0..REPLICAS).all(|replica| sim.consensus_view(usize::from(replica), fixture) > Some(0))
        }));
        for backup in 1..REPLICAS {
            *sim.network.link_drop_packet_fn(
                ProcessId::Replica(METADATA_PRIMARY),
                ProcessId::Replica(backup),
            ) = None;
        }
        assert!((0..crate::SETUP_TOTAL_STEPS).any(|_| {
            sim.step();
            (0..REPLICAS).all(|replica| {
                sim.partition_consensus_state(usize::from(replica), fixture)
                    .is_some_and(|state| state.status == Status::Normal && state.view == 1)
            })
        }));
        let partition_primary = (0..REPLICAS)
            .find(|replica| {
                sim.partition_consensus_state(usize::from(*replica), fixture)
                    .is_some_and(|state| state.is_primary)
            })
            .unwrap();
        assert_ne!(partition_primary, METADATA_PRIMARY);
        assert_eq!(sim.metadata_leader(), Some(METADATA_PRIMARY));

        let mut options = WorkloadOptions::new(SEED, REPLICAS, vec![fixture]);
        options.weights = ActionWeights::new(&[(Action::SendMessages, 100)]);
        let mut workload = Workload::new(options);
        assert!(workload.rehome(CLIENT, partition_primary).is_empty());
        sim.discover_partition_contexts(&clients[0]);
        let (target, request) = (0..SAMPLE_ATTEMPTS)
            .find_map(|_| workload.build_request(&clients[0]))
            .expect("SendMessages must sample an idle client request");
        assert_eq!(target, partition_primary);
        sim.submit_request(CLIENT, target, request.into_generic());
        let mut invariants = Invariants::new();
        assert!(
            oracle::drive_to_quiesce(
                &mut sim,
                &mut workload,
                &clients,
                u64::from(crate::SETUP_TOTAL_STEPS),
                &mut invariants,
            ),
            "{}",
            oracle::quiesce_failure_report(&sim, &workload),
        );
        assert!(
            workload.evictions() > 0,
            "the partition transport was unbound"
        );
        assert_eq!(workload.auditor.stats().commits(Action::SendMessages), 1);
        assert_eq!(clients[0].session_id(), original_session);
        assert_eq!(sim.metadata_leader(), Some(METADATA_PRIMARY));
        assert!(
            sim.partition_consensus_state(usize::from(partition_primary), fixture)
                .unwrap()
                .is_primary
        );
    }

    #[test]
    fn given_independent_primaries_when_a_partition_request_retries_from_metadata_primary_should_reach_its_own_primary()
     {
        const SEED: u64 = 0xE71C_1400;
        const REPLICAS: u8 = 3;
        const CLIENT: u128 = 1;
        const PARTITION_PRIMARY: u8 = 0;
        const METADATA_PRIMARY: u8 = 1;
        const SAMPLE_ATTEMPTS: usize = 16;

        fn starve_metadata_commits(packet: &Packet) -> bool {
            is_commit_for(packet, METADATA_GROUP)
        }

        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });
        let mut sim = Simulator::with_shards_shell(
            usize::from(REPLICAS),
            1,
            [CLIENT].into_iter(),
            PacketSimulatorOptions {
                node_count: REPLICAS,
                client_count: 1,
                seed: SEED,
                ..PacketSimulatorOptions::default()
            },
        );
        let fixture = IggyNamespace::new(1, 1, 0);
        sim.init_partition(fixture);
        let clients = [SimClient::new(CLIENT)];
        for replica in 0..REPLICAS {
            sim.shell_login_via(&clients[0], replica);
        }
        let original_session = clients[0].session_id();
        for backup in 1..REPLICAS {
            *sim.network.link_drop_packet_fn(
                ProcessId::Replica(PARTITION_PRIMARY),
                ProcessId::Replica(backup),
            ) = Some(starve_metadata_commits);
        }
        assert!((0..crate::SETUP_TOTAL_STEPS).any(|_| {
            sim.step();
            sim.metadata_leader() == Some(METADATA_PRIMARY)
        }));
        for backup in 1..REPLICAS {
            *sim.network.link_drop_packet_fn(
                ProcessId::Replica(PARTITION_PRIMARY),
                ProcessId::Replica(backup),
            ) = None;
        }
        assert!((0..crate::SETUP_TOTAL_STEPS).any(|_| {
            sim.step();
            sim.replicas.iter().all(|replica| {
                let consensus = replica.shards[0]
                    .plane
                    .metadata()
                    .consensus
                    .as_ref()
                    .unwrap();
                consensus.status() == Status::Normal && consensus.view() == 1
            })
        }));
        assert!((0..REPLICAS).all(|replica| {
            sim.partition_consensus_state(usize::from(replica), fixture)
                .is_some_and(|state| state.status == Status::Normal && state.view == 0)
        }));
        assert!(
            sim.partition_consensus_state(usize::from(PARTITION_PRIMARY), fixture)
                .unwrap()
                .is_primary
        );

        let mut options = WorkloadOptions::new(SEED, REPLICAS, vec![fixture]);
        options.weights = ActionWeights::new(&[(Action::SendMessages, 100)]);
        let mut workload = Workload::new(options);
        assert!(workload.rehome(CLIENT, METADATA_PRIMARY).is_empty());
        sim.discover_partition_contexts(&clients[0]);
        let (target, request) = (0..SAMPLE_ATTEMPTS)
            .find_map(|_| workload.build_request(&clients[0]))
            .expect("SendMessages must sample an idle client request");
        assert_eq!(target, METADATA_PRIMARY);
        sim.submit_request(CLIENT, target, request.into_generic());
        let mut invariants = Invariants::new();
        assert!(
            oracle::drive_to_quiesce(
                &mut sim,
                &mut workload,
                &clients,
                u64::from(crate::SETUP_TOTAL_STEPS),
                &mut invariants,
            ),
            "{}",
            oracle::quiesce_failure_report(&sim, &workload),
        );
        assert_eq!(workload.evictions(), 0, "every transport was already bound");
        assert_eq!(workload.auditor.stats().commits(Action::SendMessages), 1);
        assert_eq!(clients[0].session_id(), original_session);
        assert_eq!(sim.metadata_leader(), Some(METADATA_PRIMARY));
        assert!(
            sim.partition_consensus_state(usize::from(PARTITION_PRIMARY), fixture)
                .unwrap()
                .is_primary
        );
    }

    #[test]
    fn given_a_late_handshake_from_another_replica_when_recovering_should_keep_the_actual_target_pending()
     {
        const SEED: u64 = 0xE71C_1271;
        const REPLICAS: u8 = 3;
        const CLIENT: u128 = 1;
        const PRIMARY: u8 = 0;
        const UNBOUND_BACKUP: u8 = 1;
        const SAMPLE_ATTEMPTS: usize = 16;
        const ONE_TICK: u64 = 1;
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });
        let mut sim = Simulator::with_shards_shell(
            usize::from(REPLICAS),
            1,
            [CLIENT].into_iter(),
            PacketSimulatorOptions {
                node_count: REPLICAS,
                client_count: 1,
                seed: SEED,
                ..PacketSimulatorOptions::default()
            },
        );
        let clients = [SimClient::new(CLIENT)];
        sim.shell_login_via(&clients[0], PRIMARY);
        let original_session = clients[0].session_id();
        sim.submit_handshake(&clients[0], PRIMARY);
        let old_reply = (0..crate::SETUP_TOTAL_STEPS)
            .find_map(|_| {
                sim.step_with_senders().into_iter().find(|(from, reply)| {
                    *from == PRIMARY && reply.header().operation == Operation::Register
                })
            })
            .expect("the original endpoint must answer its login");
        assert_eq!(sim.handshake_session(&old_reply.1), Some(original_session));

        let mut options = WorkloadOptions::new(SEED, REPLICAS, vec![]);
        options.weights = ActionWeights::new(&[(Action::CreateStream, 100)]);
        let mut workload = Workload::new(options);
        assert!(workload.rehome(CLIENT, UNBOUND_BACKUP).is_empty());
        let (target, request) = (0..SAMPLE_ATTEMPTS)
            .find_map(|_| workload.build_request(&clients[0]))
            .expect("CreateStream must sample an idle client request");
        let request_id = request.header().request;
        sim.submit_request(CLIENT, target, request.into_generic());
        let mut invariants = Invariants::new();
        assert!((0..crate::SETUP_TOTAL_STEPS).any(|_| {
            let _ = oracle::drive_to_quiesce(
                &mut sim,
                &mut workload,
                &clients,
                ONE_TICK,
                &mut invariants,
            );
            workload.evictions() > 0
        }));
        assert_eq!(workload.pending_handshakes[&CLIENT].0, UNBOUND_BACKUP);
        *sim.network.link_drop_packet_fn(
            ProcessId::Replica(UNBOUND_BACKUP),
            ProcessId::Client(CLIENT),
        ) = Some(|_| true);
        sim.deferred_client_replies.push(old_reply);
        sim.evicted.push((CLIENT, PRIMARY));
        workload.tick();
        drive_tick(&mut sim, &mut workload, &clients, &mut invariants);
        assert_eq!(
            workload
                .pending_handshakes
                .get(&CLIENT)
                .map(|(target, _)| *target),
            Some(UNBOUND_BACKUP),
            "neither an old endpoint reply nor a duplicate eviction completes or redirects this recovery",
        );
        assert!(workload.outstanding.contains_key(&(CLIENT, request_id)));
        assert_eq!(clients[0].session_id(), original_session);
        *sim.network.link_drop_packet_fn(
            ProcessId::Replica(UNBOUND_BACKUP),
            ProcessId::Client(CLIENT),
        ) = None;
        assert!(oracle::drive_to_quiesce(
            &mut sim,
            &mut workload,
            &clients,
            u64::from(crate::SETUP_TOTAL_STEPS),
            &mut invariants,
        ));
        assert_eq!(workload.auditor.stats().commits(Action::CreateStream), 1);
    }

    #[test]
    fn given_a_recovery_handshake_when_the_epoch_changes_should_abandon_the_old_toggle_as_unknown()
    {
        const NEW_SESSION: u64 = 2;
        const ONE_TICK: u64 = 1;
        let fixture = IggyNamespace::new(1, 1, 0);
        let (client, mut workload, request) =
            sample_only(Action::ReplaceFixtureTopic, vec![fixture]);
        let client_id = client.client_id();
        let mut sim = Simulator::new(
            1,
            [client_id].into_iter(),
            PacketSimulatorOptions {
                node_count: 1,
                client_count: 1,
                seed: workload.options.seed,
                ..PacketSimulatorOptions::default()
            },
        );
        sim.init_partition(fixture);
        let login = client.register();
        let reply = reply_to(&login, 0, &[]).transmute_header(|header, reply: &mut ReplyHeader| {
            *reply = header;
            reply.commit = NEW_SESSION;
        });
        workload.note_handshake_submitted(client_id, 0);
        let wrong_identity =
            reply
                .deep_copy()
                .transmute_header(|header, reply: &mut ReplyHeader| {
                    *reply = header;
                    reply.client = client_id + 1;
                });
        sim.deferred_client_replies.push((0, wrong_identity));
        let clients = [client];
        let mut invariants = Invariants::new();
        assert!(!oracle::drive_to_quiesce(
            &mut sim,
            &mut workload,
            &clients,
            ONE_TICK,
            &mut invariants,
        ));
        assert!(workload.is_recovering(client_id));
        assert!(
            workload
                .outstanding
                .contains_key(&(client_id, request.header().request))
        );

        sim.deferred_client_replies.push((0, reply));
        workload.tick();
        drive_tick(&mut sim, &mut workload, &clients, &mut invariants);
        assert!(!workload.is_recovering(client_id));
        assert_eq!(workload.total_in_flight(), 0);
        assert_eq!(clients[0].session_id(), NEW_SESSION);
        let probe = sample_toggle(&mut workload, &clients[0]);
        assert_eq!(probe.header().operation, Operation::DeleteTopic);
        assert!(workload.shadow.fixture_probing(0));
    }

    #[test]
    fn given_a_bound_backup_when_draining_should_follow_the_primary_before_retry_timeout() {
        const SEED: u64 = 0xE71C_7311;
        const REPLICAS: u8 = 3;
        const CLIENT: u128 = 1;
        const PRIMARY: u8 = 0;
        const BACKUP: u8 = 1;
        const SAMPLE_ATTEMPTS: usize = 16;
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });
        let mut sim = Simulator::with_shards_shell(
            usize::from(REPLICAS),
            1,
            [CLIENT].into_iter(),
            PacketSimulatorOptions {
                node_count: REPLICAS,
                client_count: 1,
                seed: SEED,
                ..PacketSimulatorOptions::default()
            },
        );
        let clients = [SimClient::new(CLIENT)];
        sim.shell_login_via(&clients[0], BACKUP);
        let mut options = WorkloadOptions::new(SEED, REPLICAS, vec![]);
        options.weights = ActionWeights::new(&[(Action::CreateStream, 100)]);
        let mut workload = Workload::new(options);
        assert!(workload.rehome(CLIENT, BACKUP).is_empty());
        let (target, request) = (0..SAMPLE_ATTEMPTS)
            .find_map(|_| workload.build_request(&clients[0]))
            .expect("CreateStream must sample an idle client request");
        assert_eq!(target, BACKUP);
        sim.submit_request(CLIENT, target, request.into_generic());
        let mut invariants = Invariants::new();
        let drain_ticks = workload.options.request_timeout_ticks / 2;
        assert!(
            oracle::drive_to_quiesce(
                &mut sim,
                &mut workload,
                &clients,
                drain_ticks,
                &mut invariants,
            ),
            "{}",
            oracle::quiesce_failure_report(&sim, &workload),
        );
        assert_eq!(
            workload.evictions(),
            0,
            "the backup transport was already bound"
        );
        assert_eq!(workload.auditor.stats().commits(Action::CreateStream), 1);
        assert_eq!(workload.redirects(), 1);
        assert_eq!(sim.metadata_leader(), Some(PRIMARY));
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn given_a_duplicate_fixture_delete_when_a_copy_is_refused_should_settle_the_original_before_recreating()
     {
        const SEED: u64 = 0xE71C_5035;
        const CLIENT: u128 = 1;
        const PRIMARY: u8 = 0;
        const REPLICAS: u8 = 1;
        for admitted_before_refusal in [true, false] {
            server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
                enabled: false,
                size: iggy_common::IggyByteSize::from(0u64),
                bucket_capacity: 1,
            });
            let mut sim = Simulator::with_shards_shell(
                usize::from(REPLICAS),
                1,
                [CLIENT].into_iter(),
                PacketSimulatorOptions {
                    node_count: REPLICAS,
                    client_count: 1,
                    seed: SEED,
                    ..PacketSimulatorOptions::default()
                },
            );
            let fixture = IggyNamespace::new(1, 1, 0);
            sim.init_partition(fixture);
            let clients = [SimClient::new(CLIENT)];
            sim.shell_login(&clients[0]);
            let primary = Rc::clone(&sim.replicas[usize::from(PRIMARY)].shards[0]);
            let consensus = primary.plane.metadata().consensus.as_ref().unwrap();
            let mut workload = Workload::new(WorkloadOptions::new(SEED, REPLICAS, vec![fixture]));
            let delete = sample_toggle(&mut workload, &clients[0]);
            let key = (CLIENT, delete.header().request);
            if admitted_before_refusal {
                sim.submit_request(CLIENT, PRIMARY, delete.deep_copy().into_generic());
                let pending = (0..crate::SETUP_TOTAL_STEPS)
                    .find_map(|_| {
                        sim.step().into_iter().find(|reply| {
                            reply.header().client == CLIENT
                                && reply.header().request == key.1
                                && result_code(reply.body())
                                    == Some(IggyError::TransientNotCommitted.as_code())
                        })
                    })
                    .expect("the real delete intent must commit before its lifecycle finishes");
                assert!(workload.on_reply(&pending).is_empty());
            }

            consensus.begin_state_transfer_await();
            // A network duplicate does not increment the driver's attempt count.
            sim.submit_request(CLIENT, PRIMARY, delete.deep_copy().into_generic());
            let refused = (0..crate::SETUP_TOTAL_STEPS)
                .find_map(|_| {
                    sim.step().into_iter().find(|reply| {
                        reply.header().client == CLIENT
                            && reply.header().request == key.1
                            && result_code(reply.body())
                                == Some(IggyError::TransientNotAccepted.as_code())
                    })
                })
                .expect("the transferring primary must refuse this copy before admission");
            consensus.set_state_transfer_stage(StateTransferStage::Idle);
            assert!(workload.on_reply(&refused).is_empty());
            assert_eq!(
                workload.total_in_flight(),
                1,
                "a refusal must not abandon other copies"
            );
            assert_eq!(workload.outstanding[&key].attempts, 1);
            assert!(
                workload.build_request(&bound_client(CLIENT + 1)).is_none(),
                "another client must not probe or recreate while the original delete can still commit"
            );

            sim.submit_request(CLIENT, PRIMARY, delete.deep_copy().into_generic());
            let mut invariants = Invariants::new();
            assert!(oracle::drive_to_quiesce(
                &mut sim,
                &mut workload,
                &clients,
                u64::from(crate::SETUP_TOTAL_STEPS),
                &mut invariants,
            ));
            let create = sample_toggle(&mut workload, &clients[0]);
            assert_eq!(create.header().operation, Operation::CreateTopic);
            sim.submit_request(CLIENT, PRIMARY, create.into_generic());
            assert!(oracle::drive_to_quiesce(
                &mut sim,
                &mut workload,
                &clients,
                u64::from(crate::SETUP_TOTAL_STEPS),
                &mut invariants,
            ));
            workload.settle_fixture_replacements(&sim);
            assert!(workload.shadow.fixtures_restored());
            let recreated = primary
                .plane
                .metadata()
                .mux_stm
                .streams()
                .created_revision_for_namespace(fixture)
                .unwrap();

            sim.submit_request(CLIENT, PRIMARY, delete.into_generic());
            assert!(
                (0..crate::SETUP_TOTAL_STEPS).any(|_| {
                    sim.step().iter().any(|reply| {
                        reply.header().client == CLIENT && reply.header().request == key.1
                    })
                }),
                "the delayed original must receive its cached reply"
            );
            assert_eq!(
                primary
                    .plane
                    .metadata()
                    .mux_stm
                    .streams()
                    .created_revision_for_namespace(fixture),
                Some(recreated)
            );
            let journal = &sim.replicas[usize::from(PRIMARY)].metadata_journal;
            let deletes = (1..=journal.last_op().unwrap())
                .filter_map(|op| journal.entry_sync(op))
                .filter(|entry| entry.header().client == CLIENT && entry.header().request == key.1)
                .count();
            assert_eq!(
                deletes, 1,
                "all copies must resolve through the original request identity"
            );
        }
    }

    #[test]
    fn given_a_token_limit_admission_reply_when_received_should_record_a_denial() {
        let (_, mut workload, request) = sample_only(Action::CreatePersonalAccessToken, Vec::new());
        let error = IggyError::PersonalAccessTokensLimitReached(1, 1);
        let body = ApplyReply::err(error.as_code()).to_reply_body();

        assert!(workload.on_reply(&reply_to(&request, 0, &body)).is_empty());

        assert_eq!(workload.total_in_flight(), 0);
        assert_eq!(
            workload
                .auditor
                .stats()
                .commits(Action::CreatePersonalAccessToken),
            0
        );
    }

    #[test]
    fn given_transient_status_replies_when_received_should_keep_the_original_until_it_commits() {
        for error in [
            IggyError::TransientNotAccepted,
            IggyError::NotPrimary,
            IggyError::TransientNotCommitted,
        ] {
            let (client, mut workload, request) = sample_only(Action::CreateStream, Vec::new());
            assert!(
                workload
                    .on_reply(&reply_to(&request, error.as_code(), &[]))
                    .is_empty()
            );
            assert_eq!(
                workload.total_in_flight(),
                1,
                "{error} abandoned an unresolved request"
            );
            assert!(workload.build_request(&client).is_none());
            assert!(
                workload
                    .on_reply(&reply_to(
                        &request,
                        0,
                        &ApplyReply::default().to_reply_body()
                    ))
                    .is_empty()
            );
            assert_eq!(workload.total_in_flight(), 0);
            assert_eq!(workload.auditor.stats().commits(Action::CreateStream), 1);
        }
    }

    /// A client with a bound session.
    fn bound_client(client_id: u128) -> SimClient {
        const SESSION: u64 = 1;
        let client = SimClient::new(client_id);
        client.bind_session(SESSION);
        client
    }

    /// A bound client, a workload over `fixtures` that draws only `action`, and
    /// its first request.
    fn sample_only(
        action: Action,
        fixtures: Vec<IggyNamespace>,
    ) -> (SimClient, Workload, Message<RoutedRequestHeader>) {
        const SAMPLE_ATTEMPTS: usize = 16;
        let client = bound_client(1);
        let mut options = WorkloadOptions::new(0x4C43_4259, 1, fixtures);
        options.weights = ActionWeights::new(&[(action, 100)]);
        let mut workload = Workload::new(options);
        let (_, request) = (0..SAMPLE_ATTEMPTS)
            .find_map(|_| workload.build_request(&client))
            .unwrap_or_else(|| panic!("the shadow must sample {action:?}"));
        (client, workload, request)
    }

    /// The reply to `request`, with `status` and `body`.
    fn reply_to(
        request: &Message<RoutedRequestHeader>,
        status: u32,
        body: &[u8],
    ) -> Message<ReplyHeader> {
        let size = size_of::<ReplyHeader>() + body.len();
        let mut reply =
            Message::<ReplyHeader>::new(size).transmute_header(|_, header: &mut ReplyHeader| {
                header.command = Command::Reply;
                header.size = u32::try_from(size).unwrap();
                header.status = status;
                header.client = request.header().client;
                header.request = request.header().request;
                header.operation = request.header().operation;
            });
        reply.as_mut_slice()[size_of::<ReplyHeader>()..].copy_from_slice(body);
        reply
    }

    /// [`reply_to`] a request that committed at metadata op `op`.
    fn committed_at(
        request: &Message<RoutedRequestHeader>,
        op: u64,
        body: &[u8],
    ) -> Message<ReplyHeader> {
        reply_to(request, 0, body).transmute_header(|reply, header: &mut ReplyHeader| {
            *header = reply;
            header.op = op;
        })
    }

    /// The next fixture toggle `client` sends.
    fn sample_toggle(workload: &mut Workload, client: &SimClient) -> Message<RoutedRequestHeader> {
        workload.options.weights = ActionWeights::new(&[(Action::ReplaceFixtureTopic, 100)]);
        let (_, request) = workload
            .build_request(client)
            .expect("a fixture topic must take a toggle");
        request
    }

    /// Delete and recreate the fixture topic through `client`, then settle it.
    fn replace_fixture(workload: &mut Workload, client: &SimClient) {
        for operation in [Operation::DeleteTopic, Operation::CreateTopic] {
            let toggle = sample_toggle(workload, client);
            assert_eq!(toggle.header().operation, operation);
            let committed = ApplyReply::default().to_reply_body();
            assert!(
                workload
                    .on_reply(&reply_to(&toggle, 0, &committed))
                    .is_empty()
            );
        }
        workload.shadow.settle_fixtures(|_, _| true);
    }

    /// A workload over `fixture` and its fixture group, with every offset op on
    /// consumer id 0, the id of the fixture group.
    fn fixture_group_workload(fixture: IggyNamespace) -> Workload {
        let mut options = WorkloadOptions::new(0x4C43_4259, 1, vec![fixture]);
        options.consumer_pool_size = 1;
        let mut workload = Workload::new(options);
        workload.shadow.enable_fixture_groups();
        workload
    }

    /// The first group offset store that `client` samples. Every store it samples
    /// before that one commits.
    fn sample_group_store(
        workload: &mut Workload,
        client: &SimClient,
    ) -> Message<RoutedRequestHeader> {
        const SAMPLE_ATTEMPTS: usize = 16;
        workload.options.weights = ActionWeights::new(&[(Action::StoreConsumerOffset, 100)]);
        for _ in 0..SAMPLE_ATTEMPTS {
            let (_, store) = workload
                .build_request(client)
                .expect("the fixture must take a store");
            let (request, _) =
                StoreConsumerOffsetRequest::decode(store.body()).expect("a store must decode");
            if request.consumer.kind == KIND_CONSUMER_GROUP {
                return store;
            }
            let _ = workload.on_reply(&reply_to(&store, 0, &[]));
        }
        panic!("no group store in {SAMPLE_ATTEMPTS} samples");
    }

    /// The replica that the workloads of these tests send every request to, which
    /// plays a backup, and the leader that its refusals name.
    const BACKUP: u8 = 0;
    const LEADER: u8 = 1;

    /// The backup's `NotPrimary` to `request`, in the section its plane uses.
    fn not_primary(request: &Message<RoutedRequestHeader>) -> Message<ReplyHeader> {
        let code = IggyError::NotPrimary.as_code();
        if request.header().operation.is_metadata() {
            reply_to(request, 0, &ApplyReply::err(code).to_reply_body())
        } else {
            reply_to(request, code, &[])
        }
    }

    /// Redirect `request` of `client` to the leader on the backup's refusal, and
    /// resend it there once the login lands, as the driver does. Returns the
    /// refusal.
    fn redirect_to_leader(
        workload: &mut Workload,
        client: &SimClient,
        request: &Message<RoutedRequestHeader>,
    ) -> Message<ReplyHeader> {
        let refusal = not_primary(request);
        assert!(workload.redirect(&refusal, LEADER));
        assert_eq!(workload.rehome(client.client_id(), LEADER).len(), 1);
        refusal
    }

    /// Hand `reply` from replica `from` to `workload`, as the driver does.
    fn deliver(workload: &mut Workload, from: u8, reply: &Message<ReplyHeader>) {
        if !workload.is_stale(from, reply) {
            let _ = workload.on_reply(reply);
        }
    }

    fn redirected_request_with_late_refusal()
    -> (Simulator, Workload, [SimClient; 1], Message<ReplyHeader>) {
        const REPLICAS: u8 = 3;
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });
        let (client, mut workload, request) = sample_only(Action::CreateStream, Vec::new());
        workload.options.replica_count = REPLICAS;
        let mut sim = Simulator::with_shards_shell(
            usize::from(REPLICAS),
            1,
            std::iter::once(client.client_id()),
            PacketSimulatorOptions {
                node_count: REPLICAS,
                client_count: 1,
                seed: workload.options.seed,
                ..PacketSimulatorOptions::default()
            },
        );
        let leader = sim
            .metadata_leader()
            .expect("the cluster has a metadata leader");
        let backup = (leader + 1) % REPLICAS;
        assert_eq!(workload.rehome(client.client_id(), backup).len(), 1);
        let refusal = not_primary(&request);
        assert!(workload.redirect(&refusal, leader));
        assert_eq!(workload.rehome(client.client_id(), leader).len(), 1);
        let late_denial = reply_to(&request, IggyError::Unauthorized.as_code(), &[]);
        sim.deferred_client_replies.push((backup, late_denial));
        let committed = reply_to(&request, 0, &ApplyReply::default().to_reply_body());
        (sim, workload, [client], committed)
    }

    #[test]
    fn given_a_redirected_request_when_the_driver_receives_a_late_refusal_should_wait_for_the_leader()
     {
        let (mut sim, mut workload, clients, committed) = redirected_request_with_late_refusal();
        let mut invariants = Invariants::new();

        drive_tick(&mut sim, &mut workload, &clients, &mut invariants);

        assert_eq!(
            workload.total_in_flight(),
            1,
            "the old refusal released the request"
        );
        assert!(!workload.is_recovering(clients[0].client_id()));
        let leader = sim
            .metadata_leader()
            .expect("the cluster has a metadata leader");
        sim.deferred_client_replies.push((leader, committed));
        drive_tick(&mut sim, &mut workload, &clients, &mut invariants);
        assert_eq!(workload.total_in_flight(), 0);
        assert_eq!(workload.auditor.stats().commits(Action::CreateStream), 1);
    }

    #[test]
    fn given_a_redirected_request_when_the_drain_receives_a_late_refusal_should_wait_for_the_leader()
     {
        const ONE_TICK: u64 = 1;
        let (mut sim, mut workload, clients, committed) = redirected_request_with_late_refusal();
        let mut invariants = Invariants::new();

        assert!(
            !oracle::drive_to_quiesce(&mut sim, &mut workload, &clients, ONE_TICK, &mut invariants,),
            "the old refusal drained the request before its leader replied"
        );

        let leader = sim
            .metadata_leader()
            .expect("the cluster has a metadata leader");
        sim.deferred_client_replies.push((leader, committed));
        assert!(oracle::drive_to_quiesce(
            &mut sim,
            &mut workload,
            &clients,
            ONE_TICK,
            &mut invariants,
        ));
        assert_eq!(workload.auditor.stats().commits(Action::CreateStream), 1);
    }

    #[test]
    fn given_a_retried_raw_request_when_the_original_replica_replies_should_apply_its_commit() {
        let (_client, mut workload, request) = sample_only(Action::CreateStream, Vec::new());
        workload.options.replica_count = 3;
        let original_target = workload.outstanding_summary()[0].target;
        for _ in 0..workload.options.request_timeout_ticks {
            workload.tick();
        }
        let resends = workload.due_resends();
        assert_eq!(resends.len(), 1);
        assert_ne!(resends[0].0, original_target);

        deliver(
            &mut workload,
            original_target,
            &reply_to(&request, 0, &ApplyReply::default().to_reply_body()),
        );

        assert_eq!(
            workload.total_in_flight(),
            0,
            "a delayed commit from the original replica was discarded"
        );
        assert_eq!(workload.auditor.stats().commits(Action::CreateStream), 1);
    }

    /// A pending lifecycle refuses a conflicting metadata op in apply. The refusal
    /// commits `LifecycleBusy`, which no op's result enum declares.
    #[test]
    fn given_lifecycle_busy_reply_when_received_should_release_the_request_without_effect() {
        let (client, mut workload, request) = sample_only(Action::DeleteTopic, Vec::new());
        let busy = ApplyReply::err(IggyError::LifecycleBusy.as_code());

        assert!(
            workload
                .on_reply(&reply_to(&request, 0, &busy.to_reply_body()))
                .is_empty()
        );
        assert_eq!(workload.auditor.stats().committed_rejections, 1);
        assert!(workload.client_idle(client.client_id()));
    }

    /// No lifecycle gate guards a user op, so `LifecycleBusy` there is a server bug.
    #[test]
    #[should_panic(expected = "unrecognized result code 88")]
    fn given_lifecycle_busy_for_a_user_op_when_received_should_fail_the_run() {
        let (_, mut workload, request) = sample_only(Action::CreateUser, Vec::new());
        let busy = ApplyReply::err(IggyError::LifecycleBusy.as_code());
        let _ = workload.on_reply(&reply_to(&request, 0, &busy.to_reply_body()));
    }

    /// With no replacement started, a fixture's history is stable, so
    /// `HistoryUnavailable` there is a server bug, not a denial.
    #[test]
    #[should_panic(expected = "HistoryUnavailable")]
    fn given_history_unavailable_on_a_stable_fixture_send_when_received_should_fail_the_run() {
        let fixture = IggyNamespace::new(1, 1, 0);
        let (_, mut workload, request) = sample_only(Action::SendMessages, vec![fixture]);
        let unavailable = IggyError::HistoryUnavailable.as_code();
        let _ = workload.on_reply(&reply_to(&request, unavailable, &[]));
    }

    /// A send sampled before a fixture delete can reach the fenced history.
    #[test]
    fn given_a_fixture_replacement_in_flight_when_a_send_gets_history_unavailable_should_count_a_denial()
     {
        let fixture = IggyNamespace::new(1, 1, 0);
        let (_, mut workload, send) = sample_only(Action::SendMessages, vec![fixture]);
        let delete = sample_toggle(&mut workload, &bound_client(2));
        assert_eq!(delete.header().operation, Operation::DeleteTopic);

        let unavailable = IggyError::HistoryUnavailable.as_code();
        assert!(
            workload
                .on_reply(&reply_to(&send, unavailable, &[]))
                .is_empty()
        );
        assert_eq!(
            workload.auditor.stats().denials,
            1,
            "the refused send must count as a denial"
        );
    }

    /// Once every replica hosts the recreated incarnation, the fixture's history is
    /// stable again.
    #[test]
    #[should_panic(expected = "HistoryUnavailable")]
    fn given_a_settled_fixture_replacement_when_a_later_send_gets_history_unavailable_should_fail_the_run()
     {
        let fixture = IggyNamespace::new(1, 1, 0);
        let client = bound_client(1);
        let mut options = WorkloadOptions::new(0x4C43_4259, 1, vec![fixture]);
        options.weights = ActionWeights::new(&[(Action::ReplaceFixtureTopic, 100)]);
        let mut workload = Workload::new(options);
        replace_fixture(&mut workload, &client);
        workload.options.weights = ActionWeights::new(&[(Action::SendMessages, 100)]);
        let (_, send) = workload
            .build_request(&client)
            .expect("the recreated fixture must take a send");

        let unavailable = IggyError::HistoryUnavailable.as_code();
        let _ = workload.on_reply(&reply_to(&send, unavailable, &[]));
    }

    /// A delete committed as a no-op replaced nothing, so a send sampled while
    /// it was in flight still meets a stable history.
    #[test]
    #[should_panic(expected = "HistoryUnavailable")]
    fn given_a_refused_fixture_delete_when_a_send_sampled_during_it_gets_history_unavailable_should_fail_the_run()
     {
        let fixture = IggyNamespace::new(1, 1, 0);
        let mut workload = Workload::new(WorkloadOptions::new(0x4C43_4259, 1, vec![fixture]));
        let delete = sample_toggle(&mut workload, &bound_client(1));
        assert_eq!(delete.header().operation, Operation::DeleteTopic);
        workload.options.weights = ActionWeights::new(&[(Action::SendMessages, 100)]);
        let (_, send) = workload
            .build_request(&bound_client(2))
            .expect("the fixture must take a send while its delete is in flight");
        let busy = ApplyReply::err(IggyError::LifecycleBusy.as_code()).to_reply_body();
        assert!(workload.on_reply(&reply_to(&delete, 0, &busy)).is_empty());

        let unavailable = IggyError::HistoryUnavailable.as_code();
        let _ = workload.on_reply(&reply_to(&send, unavailable, &[]));
    }

    #[test]
    fn given_a_send_committed_during_a_fixture_delete_when_delete_refuses_should_keep_its_effect() {
        let fixture = IggyNamespace::new(1, 1, 0);
        let (_, mut workload, send) = sample_only(Action::SendMessages, vec![fixture]);
        let delete = sample_toggle(&mut workload, &bound_client(2));
        let _ = workload.on_reply(&reply_to(&send, 0, &[]));
        let committed_messages = workload.shadow.sends_committed(fixture);
        assert!(
            committed_messages > 0,
            "the pending delete must not erase a committed send"
        );

        let busy = ApplyReply::err(IggyError::LifecycleBusy.as_code());
        let _ = workload.on_reply(&reply_to(&delete, 0, &busy.to_reply_body()));
        assert_eq!(workload.shadow.sends_committed(fixture), committed_messages);
    }

    #[test]
    fn given_a_send_committed_during_a_fixture_delete_when_delete_succeeds_should_clear_its_effect()
    {
        let fixture = IggyNamespace::new(1, 1, 0);
        let (_, mut workload, send) = sample_only(Action::SendMessages, vec![fixture]);
        let delete = sample_toggle(&mut workload, &bound_client(2));
        let _ = workload.on_reply(&reply_to(&send, 0, &[]));
        assert!(workload.shadow.sends_committed(fixture) > 0);

        let _ = workload.on_reply(&reply_to(
            &delete,
            0,
            &ApplyReply::default().to_reply_body(),
        ));
        assert_eq!(workload.shadow.sends_committed(fixture), 0);
    }

    /// A send sampled before a fixture replacement can commit on the deleted
    /// incarnation, so it must not count toward the recreated one.
    #[test]
    fn given_a_send_sampled_before_a_fixture_replacement_when_it_commits_after_should_not_count_it()
    {
        let fixture = IggyNamespace::new(1, 1, 0);
        let (_, mut workload, send) = sample_only(Action::SendMessages, vec![fixture]);
        replace_fixture(&mut workload, &bound_client(2));

        assert!(workload.on_reply(&reply_to(&send, 0, &[])).is_empty());
        assert_eq!(
            workload.shadow.sends_committed(fixture),
            0,
            "a send of the deleted incarnation counted toward the recreated one"
        );
    }

    #[test]
    fn given_a_recreated_fixture_when_offsets_commit_while_settling_should_retain_their_effects() {
        let fixture = IggyNamespace::new(1, 1, 0);
        let toggler = bound_client(1);
        let writer = bound_client(2);
        let mut workload = Workload::new(WorkloadOptions::new(0x4C43_4259, 1, vec![fixture]));
        let delete = sample_toggle(&mut workload, &toggler);
        workload.options.weights = ActionWeights::new(&[(Action::SendMessages, 100)]);
        let (_, old_send) = workload.build_request(&writer).unwrap();
        let committed = ApplyReply::default().to_reply_body();
        assert!(
            workload
                .on_reply(&reply_to(&delete, 0, &committed))
                .is_empty()
        );
        let create = sample_toggle(&mut workload, &toggler);
        assert!(
            workload
                .on_reply(&reply_to(&create, 0, &committed))
                .is_empty()
        );
        assert!(!workload.shadow.fixtures_restored());
        assert!(workload.on_reply(&reply_to(&old_send, 0, &[])).is_empty());
        assert_eq!(workload.shadow.sends_committed(fixture), 0);

        workload.options.weights = ActionWeights::new(&[(Action::JoinFixtureGroup, 100)]);
        workload.shadow.enable_fixture_groups();
        for _ in 0..2 {
            let (_, join) = workload.build_request(&writer).unwrap();
            assert!(
                workload
                    .on_reply(&reply_to(&join, 0, &committed))
                    .is_empty()
            );
        }
        workload.options.weights = ActionWeights::new(&[(Action::StoreConsumerOffset, 100)]);
        let (_, store) = workload.build_request(&writer).unwrap();
        let (request, _) = StoreConsumerOffsetRequest::decode(store.body()).unwrap();
        assert!(workload.on_reply(&reply_to(&store, 0, &[])).is_empty());
        assert_eq!(
            workload.shadow.consumer_offset(
                fixture,
                request.consumer.kind,
                request.consumer.id.as_u32().unwrap()
            ),
            Some(request.offset),
            "a store on the recreated history must survive before every replica settles",
        );
        workload.shadow.settle_fixtures(|_, _| true);
        assert_eq!(
            workload.shadow.consumer_offset(
                fixture,
                request.consumer.kind,
                request.consumer.id.as_u32().unwrap()
            ),
            Some(request.offset),
        );
    }

    /// A send sampled while a fixture delete is in flight can commit on the
    /// deleted incarnation too, even though no replacement starts after it.
    #[test]
    fn given_a_send_sampled_during_a_fixture_delete_when_it_commits_after_the_replacement_should_not_count_it()
     {
        let fixture = IggyNamespace::new(1, 1, 0);
        let toggler = bound_client(1);
        let mut workload = Workload::new(WorkloadOptions::new(0x4C43_4259, 1, vec![fixture]));
        let delete = sample_toggle(&mut workload, &toggler);
        assert_eq!(delete.header().operation, Operation::DeleteTopic);
        workload.options.weights = ActionWeights::new(&[(Action::SendMessages, 100)]);
        let (_, send) = workload
            .build_request(&bound_client(2))
            .expect("the fixture must take a send while its delete is in flight");
        let committed = ApplyReply::default().to_reply_body();
        assert!(
            workload
                .on_reply(&reply_to(&delete, 0, &committed))
                .is_empty()
        );
        let create = sample_toggle(&mut workload, &toggler);
        assert_eq!(create.header().operation, Operation::CreateTopic);
        assert!(
            workload
                .on_reply(&reply_to(&create, 0, &committed))
                .is_empty()
        );
        workload.shadow.settle_fixtures(|_, _| true);

        assert!(workload.on_reply(&reply_to(&send, 0, &[])).is_empty());
        assert_eq!(
            workload.shadow.sends_committed(fixture),
            0,
            "a send of the deleted incarnation counted toward the recreated one"
        );
    }

    /// An evicted client's delete may have committed, so the next toggle asks
    /// again, and a topic already gone proves that it did.
    #[test]
    fn given_a_fixture_delete_of_unknown_fate_when_the_probe_finds_no_topic_should_recreate_it() {
        let fixture = IggyNamespace::new(1, 1, 0);
        let (client, mut workload, _) = sample_only(Action::ReplaceFixtureTopic, vec![fixture]);
        workload.forget_evicted_client(client.client_id());
        let probe = sample_toggle(&mut workload, &client);
        assert_eq!(probe.header().operation, Operation::DeleteTopic);

        let gone = ApplyReply::err(DeleteTopicResult::TopicNotFound).to_reply_body();
        assert!(workload.on_reply(&reply_to(&probe, 0, &gone)).is_empty());
        assert_eq!(
            sample_toggle(&mut workload, &client).header().operation,
            Operation::CreateTopic,
            "the probe must prove that the delete committed"
        );
    }

    /// A first delete of a live fixture must find its topic: gone already, the
    /// server lost it.
    #[test]
    #[should_panic(expected = "already as the toggle asked")]
    fn given_a_first_fixture_delete_when_the_topic_is_gone_should_fail_the_run() {
        let fixture = IggyNamespace::new(1, 1, 0);
        let (_, mut workload, delete) = sample_only(Action::ReplaceFixtureTopic, vec![fixture]);
        assert_eq!(delete.header().operation, Operation::DeleteTopic);
        let gone = ApplyReply::err(DeleteTopicResult::TopicNotFound).to_reply_body();
        let _ = workload.on_reply(&reply_to(&delete, 0, &gone));
    }

    /// A first create after a committed delete must not find its topic: still
    /// there, the delete did not remove it.
    #[test]
    #[should_panic(expected = "already as the toggle asked")]
    fn given_a_committed_fixture_delete_when_the_first_create_finds_the_topic_should_fail_the_run()
    {
        let fixture = IggyNamespace::new(1, 1, 0);
        let (client, mut workload, delete) =
            sample_only(Action::ReplaceFixtureTopic, vec![fixture]);
        let committed = ApplyReply::default().to_reply_body();
        assert!(
            workload
                .on_reply(&reply_to(&delete, 0, &committed))
                .is_empty()
        );
        let create = sample_toggle(&mut workload, &client);
        assert_eq!(create.header().operation, Operation::CreateTopic);
        let exists = ApplyReply::err(CreateTopicResult::NameAlreadyExists).to_reply_body();
        let _ = workload.on_reply(&reply_to(&create, 0, &exists));
    }

    /// A resend can find the topic its own first attempt already removed.
    #[test]
    fn given_a_resent_fixture_delete_when_the_topic_is_gone_should_recreate_it() {
        let fixture = IggyNamespace::new(1, 1, 0);
        let (client, mut workload, delete) =
            sample_only(Action::ReplaceFixtureTopic, vec![fixture]);
        for _ in 0..workload.options.request_timeout_ticks {
            workload.tick();
        }
        assert_eq!(workload.due_resends().len(), 1, "the delete must be resent");

        let gone = ApplyReply::err(DeleteTopicResult::TopicNotFound).to_reply_body();
        assert!(workload.on_reply(&reply_to(&delete, 0, &gone)).is_empty());
        assert_eq!(
            sample_toggle(&mut workload, &client).header().operation,
            Operation::CreateTopic,
            "the resend must prove that the delete committed"
        );
    }

    /// The SDK reads a result only on the link of its attempt, so a late copy of
    /// the backup's refusal does not undo the delete that the leader committed.
    #[test]
    fn given_a_redirected_fixture_delete_when_the_backup_refusal_arrives_again_should_recreate_the_topic()
     {
        let fixture = IggyNamespace::new(1, 1, 0);
        let (client, mut workload, delete) =
            sample_only(Action::ReplaceFixtureTopic, vec![fixture]);
        assert_eq!(delete.header().operation, Operation::DeleteTopic);
        let refusal = redirect_to_leader(&mut workload, &client, &delete);
        deliver(&mut workload, BACKUP, &refusal);
        let committed = ApplyReply::default().to_reply_body();
        deliver(&mut workload, LEADER, &reply_to(&delete, 0, &committed));

        assert_eq!(
            sample_toggle(&mut workload, &client).header().operation,
            Operation::CreateTopic,
            "the leader's commit must prove that the delete committed"
        );
    }

    /// A late copy of the backup's refusal does not take back a delete that the
    /// leader committed, so a send sampled during it may meet the replacement.
    #[test]
    fn given_a_redirected_fixture_delete_when_the_backup_refusal_arrives_again_should_count_a_send_denial()
     {
        let fixture = IggyNamespace::new(1, 1, 0);
        let toggler = bound_client(1);
        let mut workload = Workload::new(WorkloadOptions::new(0x4C43_4259, 1, vec![fixture]));
        let delete = sample_toggle(&mut workload, &toggler);
        assert_eq!(delete.header().operation, Operation::DeleteTopic);
        workload.options.weights = ActionWeights::new(&[(Action::SendMessages, 100)]);
        let (_, send) = workload
            .build_request(&bound_client(2))
            .expect("the fixture must take a send while its delete is in flight");
        let refusal = redirect_to_leader(&mut workload, &toggler, &delete);
        deliver(&mut workload, BACKUP, &refusal);
        let committed = ApplyReply::default().to_reply_body();
        deliver(&mut workload, LEADER, &reply_to(&delete, 0, &committed));

        let unavailable = IggyError::HistoryUnavailable.as_code();
        let _ = workload.on_reply(&reply_to(&send, unavailable, &[]));
        assert_eq!(
            workload.auditor.stats().denials,
            1,
            "the refused send must count as a denial"
        );
    }

    /// A late copy of the backup's refusal does not undo a join that the leader
    /// committed, so the store of the joined client is legitimate.
    #[test]
    fn given_a_redirected_join_when_the_backup_refusal_arrives_again_should_apply_the_group_store()
    {
        let client = bound_client(1);
        let mut workload = fixture_group_workload(IggyNamespace::new(1, 1, 0));
        workload.options.weights = ActionWeights::new(&[(Action::JoinFixtureGroup, 100)]);
        let (_, join) = workload
            .build_request(&client)
            .expect("the fixture group must take a join");
        assert_eq!(join.header().operation, Operation::JoinConsumerGroup);
        let refusal = redirect_to_leader(&mut workload, &client, &join);
        deliver(&mut workload, BACKUP, &refusal);
        let committed = ApplyReply::default().to_reply_body();
        deliver(&mut workload, LEADER, &reply_to(&join, 0, &committed));

        let store = sample_group_store(&mut workload, &client);
        assert!(workload.on_reply(&reply_to(&store, 0, &[])).is_empty());
        assert!(workload.client_idle(client.client_id()));
    }

    /// The SDK sends one attempt at a time, so a request that a redirect holds
    /// waits for its login instead of timing out to another replica.
    #[test]
    fn given_a_redirected_request_when_its_login_is_pending_should_not_resend_it() {
        let (client, mut workload, request) = sample_only(Action::CreateStream, Vec::new());
        assert!(workload.redirect(&not_primary(&request), LEADER));
        workload.note_handshake_submitted(client.client_id(), LEADER);
        for _ in 0..workload.options.request_timeout_ticks {
            workload.tick();
        }

        assert!(
            workload.due_resends().is_empty(),
            "a request went out while its login was pending"
        );
        assert_eq!(workload.rehome(client.client_id(), LEADER).len(), 1);
    }

    /// Fixture replacements under concurrent traffic end with every fixture
    /// hosted again, so the quiesce oracle checks each of them.
    #[test]
    fn given_fixture_replacements_under_traffic_when_restored_should_converge() {
        const SEED: u64 = 0x4C49_4645;
        const REPLICAS: u8 = 3;
        const CLIENTS: u8 = 2;
        const TICKS: u64 = 3_000;
        const PHASE_TICKS: u64 = 50_000;
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });
        let client_ids: Vec<u128> = (1..=u128::from(CLIENTS)).collect();
        let mut sim = Simulator::new(
            usize::from(REPLICAS),
            client_ids.iter().copied(),
            PacketSimulatorOptions {
                node_count: REPLICAS,
                client_count: CLIENTS,
                seed: SEED,
                ..PacketSimulatorOptions::default()
            },
        );
        let fixture = IggyNamespace::new(1, 1, 0);
        sim.init_partition(fixture);
        let clients: Vec<SimClient> = client_ids.into_iter().map(SimClient::new).collect();
        for client in &clients {
            sim.register_client_with_primary(client);
        }
        let mut options = WorkloadOptions::new(SEED, REPLICAS, vec![fixture]);
        options.client_count = CLIENTS;
        options.weights = ActionWeights::lifecycle();
        let mut workload = Workload::new(options);
        let mut invariants = Invariants::new();

        run(
            &mut sim,
            &mut workload,
            &clients,
            TICKS,
            u64::MAX,
            &mut invariants,
        );
        assert!(restore_fixture_topics(
            &mut sim,
            &mut workload,
            &clients,
            PHASE_TICKS,
            &mut invariants,
        ));
        assert!(oracle::drive_to_quiesce(
            &mut sim,
            &mut workload,
            &clients,
            PHASE_TICKS,
            &mut invariants,
        ));
        let convergence = oracle::assert_converged(&sim, &mut workload);

        let replacements = workload.shadow.replacement_totals();
        assert!(replacements.started > 0, "the run replaced no fixture");
        assert_eq!(replacements.settled, replacements.started);
        assert_eq!(convergence.namespaces_checked, 1);
        assert!(
            workload.deleted_partition_ops_compared() > 0,
            "no deleted incarnation was compared across replicas"
        );
    }

    /// A group offset write binds only an installed owner, and only a member of
    /// the group becomes one.
    #[test]
    #[should_panic(expected = "never joined the group")]
    fn given_a_never_joined_client_when_a_group_store_commits_should_fail_the_run() {
        let client = bound_client(1);
        let mut workload = fixture_group_workload(IggyNamespace::new(1, 1, 0));
        let store = sample_group_store(&mut workload, &client);
        let _ = workload.on_reply(&reply_to(&store, 0, &[]));
    }

    /// A topic delete drops its group with every member, so a join on the deleted
    /// incarnation does not make the client a member of the recreated one.
    #[test]
    #[should_panic(expected = "never joined the group")]
    fn given_a_client_that_joined_only_a_deleted_incarnation_when_a_group_store_commits_should_fail_the_run()
     {
        let fixture = IggyNamespace::new(1, 1, 0);
        let client = bound_client(1);
        let mut workload = fixture_group_workload(fixture);
        workload.options.weights = ActionWeights::new(&[(Action::JoinFixtureGroup, 100)]);
        let (_, join) = workload
            .build_request(&client)
            .expect("the fixture group must take a join");
        let committed = ApplyReply::default().to_reply_body();
        assert!(
            workload
                .on_reply(&reply_to(&join, 0, &committed))
                .is_empty()
        );
        replace_fixture(&mut workload, &client);

        let store = sample_group_store(&mut workload, &client);
        let _ = workload.on_reply(&reply_to(&store, 0, &[]));
    }

    /// A join that committed before a fixture delete belongs to the deleted
    /// incarnation, even when its reply arrives after the topic came back.
    #[test]
    #[should_panic(expected = "never joined the group")]
    fn given_a_join_committed_before_a_replacement_when_its_store_commits_after_should_fail_the_run()
     {
        const JOIN_OP: u64 = 10;
        const DELETE_OP: u64 = 20;
        const CREATE_OP: u64 = 30;
        let fixture = IggyNamespace::new(1, 1, 0);
        let client = bound_client(1);
        let toggler = bound_client(2);
        let mut workload = fixture_group_workload(fixture);
        workload.options.weights = ActionWeights::new(&[(Action::JoinFixtureGroup, 100)]);
        let (_, join) = workload
            .build_request(&client)
            .expect("the fixture group must take a join");
        let committed = ApplyReply::default().to_reply_body();
        for (operation, op) in [
            (Operation::DeleteTopic, DELETE_OP),
            (Operation::CreateTopic, CREATE_OP),
        ] {
            let toggle = sample_toggle(&mut workload, &toggler);
            assert_eq!(toggle.header().operation, operation);
            assert!(
                workload
                    .on_reply(&committed_at(&toggle, op, &committed))
                    .is_empty()
            );
        }
        workload.shadow.settle_fixtures(|_, _| true);
        assert!(
            workload
                .on_reply(&committed_at(&join, JOIN_OP, &committed))
                .is_empty()
        );

        let store = sample_group_store(&mut workload, &client);
        let _ = workload.on_reply(&reply_to(&store, 0, &[]));
    }

    /// A join of unknown fate can still commit after a replacement, on the
    /// recreated incarnation.
    #[test]
    fn given_a_join_of_unknown_fate_when_a_store_commits_after_a_replacement_should_apply_it() {
        let fixture = IggyNamespace::new(1, 1, 0);
        let client = bound_client(1);
        let mut workload = fixture_group_workload(fixture);
        workload.options.weights = ActionWeights::new(&[(Action::JoinFixtureGroup, 100)]);
        let (_, join) = workload
            .build_request(&client)
            .expect("the fixture group must take a join");
        assert_eq!(join.header().operation, Operation::JoinConsumerGroup);
        workload.forget_evicted_client(client.client_id());
        replace_fixture(&mut workload, &client);

        let store = sample_group_store(&mut workload, &client);
        assert!(workload.on_reply(&reply_to(&store, 0, &[])).is_empty());
        assert!(workload.client_idle(client.client_id()));
    }

    #[test]
    fn given_a_joined_client_when_a_group_store_commits_should_apply_it() {
        let client = bound_client(1);
        let mut workload = fixture_group_workload(IggyNamespace::new(1, 1, 0));
        workload.options.weights = ActionWeights::new(&[(Action::JoinFixtureGroup, 100)]);
        let (_, join) = workload
            .build_request(&client)
            .expect("the fixture group must take a join");
        assert_eq!(join.header().operation, Operation::JoinConsumerGroup);
        let committed = ApplyReply::default().to_reply_body();
        assert!(
            workload
                .on_reply(&reply_to(&join, 0, &committed))
                .is_empty()
        );

        let store = sample_group_store(&mut workload, &client);
        assert!(workload.on_reply(&reply_to(&store, 0, &[])).is_empty());
        assert!(workload.client_idle(client.client_id()));
    }

    #[test]
    fn given_fixture_group_handoffs_under_traffic_when_quiesced_should_agree_on_owners() {
        const SEED: u64 = 0x4A4F_494E;
        const REPLICAS: u8 = 3;
        const CLIENTS: u8 = 3;
        const TICKS: u64 = 3_000;
        const PHASE_TICKS: u64 = 50_000;
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });
        let client_ids: Vec<u128> = (1..=u128::from(CLIENTS)).collect();
        let mut sim = Simulator::with_shards_shell(
            usize::from(REPLICAS),
            1,
            client_ids.iter().copied(),
            PacketSimulatorOptions {
                node_count: REPLICAS,
                client_count: CLIENTS,
                seed: SEED,
                ..PacketSimulatorOptions::default()
            },
        );
        let fixture = IggyNamespace::new(1, 1, 0);
        sim.init_partition(fixture);
        sim.seed_stream_topic_partition(fixture);
        let clients: Vec<SimClient> = client_ids.into_iter().map(SimClient::new).collect();
        for client in &clients {
            sim.shell_login(client);
        }
        let mut options = WorkloadOptions::new(SEED, REPLICAS, vec![fixture]);
        options.client_count = CLIENTS;
        options.weights = ActionWeights::lifecycle();
        let mut workload = Workload::new(options);
        create_fixture_groups(&mut sim, &mut workload, &clients[0]);
        let mut invariants = Invariants::new();

        run(
            &mut sim,
            &mut workload,
            &clients,
            TICKS,
            u64::MAX,
            &mut invariants,
        );
        assert!(restore_fixture_topics(
            &mut sim,
            &mut workload,
            &clients,
            PHASE_TICKS,
            &mut invariants,
        ));
        assert!(oracle::drive_to_quiesce(
            &mut sim,
            &mut workload,
            &clients,
            PHASE_TICKS,
            &mut invariants,
        ));
        let convergence = oracle::assert_converged(&sim, &mut workload);

        let stats = workload.auditor.stats();
        assert!(
            stats.commits(Action::JoinFixtureGroup) > 0,
            "no client joined a fixture group"
        );
        assert!(
            stats.commits(Action::LeaveFixtureGroup) > 0,
            "no client left a fixture group"
        );
        assert!(
            convergence.owners_compared > 0,
            "no settled owner was compared"
        );
    }

    #[test]
    #[allow(clippy::too_many_lines)]
    fn given_a_recaptured_assignment_when_storing_with_its_old_owner_should_refuse_in_the_workload()
    {
        const SEED: u64 = 0x57A1_E001;
        const REPLICAS: u8 = 3;
        const TRAFFIC_TICKS: u64 = 200;
        let namespace = IggyNamespace::new(1, 1, 0);
        server_common::MemoryPool::init_pool(&server_common::MemoryPoolSettings {
            enabled: false,
            size: iggy_common::IggyByteSize::from(0u64),
            bucket_capacity: 1,
        });
        let mut sim = Simulator::with_shards_shell(
            usize::from(REPLICAS),
            1,
            [1].into_iter(),
            PacketSimulatorOptions {
                node_count: REPLICAS,
                client_count: 1,
                seed: SEED,
                ..Default::default()
            },
        );
        sim.init_partition(namespace);
        sim.seed_stream_topic_partition(namespace);
        let clients = [SimClient::new(1)];
        sim.shell_login(&clients[0]);
        let mut options = WorkloadOptions::new(SEED, REPLICAS, vec![namespace]);
        options.ack_quorum_ratio = 1.0;
        options.weights = ActionWeights::lifecycle();
        let mut workload = Workload::new(options);
        let mut invariants = Invariants::new();
        create_fixture_groups(&mut sim, &mut workload, &clients[0]);
        workload.options.weights = ActionWeights::new(&[(Action::SendMessages, 100)]);
        run(
            &mut sim,
            &mut workload,
            &clients,
            TRAFFIC_TICKS,
            u64::MAX,
            &mut invariants,
        );
        assert!(oracle::drive_to_quiesce(
            &mut sim,
            &mut workload,
            &clients,
            u64::from(crate::SETUP_TOTAL_STEPS),
            &mut invariants,
        ));
        assert!(workload.shadow.sends_committed(namespace) > 0);
        workload.options.weights = ActionWeights::new(&[(Action::StoreStaleOwnerOffset, 100)]);
        let (stream, topic) = workload.shadow.fixture_names(0);
        let mut original = None;
        for join in [true, false, true] {
            let request = if join {
                clients[0].join_consumer_group(&stream, &topic, shadow::FIXTURE_GROUP_NAME)
            } else {
                clients[0].leave_consumer_group(&stream, &topic, shadow::FIXTURE_GROUP_NAME)
            };
            let operation = request.header().operation;
            let reply = sim.await_setup_reply(
                &clients[0],
                0,
                &request.into_generic(),
                operation,
                "owner handoff",
            );
            assert_eq!(result_code(reply.body()), Some(0));
            assert!(
                (0..crate::SETUP_TOTAL_STEPS).any(|_| {
                    sim.step();
                    sim.installed_group_owner(0, namespace, u64::from(shadow::FIXTURE_GROUP_ID))
                        .is_some_and(|owner| {
                            owner.is_unassigned() != join
                                && original.is_none_or(|old: ConsumerGroupOwner| {
                                    owner.generation > old.generation
                                })
                        })
                }),
                "the requested owner handoff must install"
            );
            if original.is_none() {
                original =
                    sim.installed_group_owner(0, namespace, u64::from(shadow::FIXTURE_GROUP_ID));
            }
            workload.observe_fixture_owners(&sim);
        }
        let current = sim
            .installed_group_owner(0, namespace, u64::from(shadow::FIXTURE_GROUP_ID))
            .unwrap();
        let original = original.unwrap();
        assert_eq!(current.client_id, original.client_id);
        assert_eq!(current.session, original.session);
        assert!(current.generation > original.generation);

        run(
            &mut sim,
            &mut workload,
            &clients,
            TRAFFIC_TICKS,
            u64::MAX,
            &mut invariants,
        );
        assert!(oracle::drive_to_quiesce(
            &mut sim,
            &mut workload,
            &clients,
            u64::from(crate::SETUP_TOTAL_STEPS),
            &mut invariants,
        ));
        assert!(
            workload.stale_owner_stores() > 0,
            "no captured stale owner was submitted"
        );
        assert!(
            workload.owner_fence_refusals() > 0,
            "no captured stale owner reached its fence"
        );
        let convergence = oracle::assert_converged(&sim, &mut workload);
        assert!(convergence.owners_compared > 0);
        assert!(convergence.offsets_compared_with_shadow > 0);
        println!(
            "stale owner proof: submitted={}, fence_refusals={}, owners_compared={}, offsets_compared_with_shadow={}",
            workload.stale_owner_stores(),
            workload.owner_fence_refusals(),
            convergence.owners_compared,
            convergence.offsets_compared_with_shadow,
        );
    }
}
