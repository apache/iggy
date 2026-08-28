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

//! Auditor: reply-order and per-client commit invariants, plus the
//! in-flight expectation table.
//!
//! Entity-state tracking lives in [`crate::workload::shadow::Shadow`].
//! Stays transport-agnostic so it can drive an Antithesis-style harness
//! against a real server binary.
//!
//! Committed result codes ride the reply body; [`crate::workload::Workload`]
//! decodes them in `on_reply` and classifies the outcome via
//! [`crate::workload::ops::classify_reply`]. This module stays oblivious to the
//! wire format.

use std::collections::HashMap;

use iggy_binary_protocol::ReplyHeader;
use strum::EnumCount;

use crate::workload::actions::Action;
use crate::workload::ops::InFlight;

/// Outcome of [`ServerAuditor::on_reply`]. See its docs for caller
/// obligations per variant.
#[derive(Debug)]
pub enum OnReply {
    /// Entry consumed; effects should be applied.
    Match(InFlight),
    /// Entry consumed but reply landed in the wrong namespace; caller
    /// must decrement the per-client counter and skip effects.
    NsMismatch,
    /// No entry; caller must not decrement.
    Unknown,
}

#[derive(Debug, Clone)]
pub struct AuditorStats {
    pub replies_seen: u64,
    pub replies_unknown: u64,
    /// Per-action committed counter, indexed by `Action as usize`.
    pub commits_per_action: [u64; Action::COUNT],
    /// Metadata reply with a nonzero committed result code: a business rejection.
    /// Shadow does not mutate; on a serial run `on_reply` asserts it was targeted.
    pub committed_rejections: u64,
    /// Denied before commit: `ReplyHeader::status` set, body EMPTY. The op never
    /// entered the log, so the shadow must not move and there is no result section to
    /// classify. Shell only; the raw path has no denial site.
    pub denials: u64,
    /// Per-action denial count and last status, indexed by `Action as usize`. Splits
    /// two causes: an op the server refuses for this input (an offset the partition
    /// cannot accept yet), versus one dispatch cannot decode at all, a workload bug
    /// showing up as every request for that action denied with the same status.
    pub denials_per_action: [(u64, u32); Action::COUNT],
    /// Result section carrying a transport rejection, not a committed outcome.
    /// `build_result_rejection_reply` frames them under the REQUEST's own operation
    /// with `status` 0, so only the code distinguishes them from a commit.
    pub transient_rejections: u64,
    /// Per-action transient count and last code, indexed by `Action as usize`. Split
    /// from `denials_per_action` because a denial never passes, while a transient is
    /// the cluster mid-view-change or backpressured and clears on retry. An action
    /// that is all transients means the workload outruns the cluster, not a broken op.
    pub transient_rejections_per_action: [(u64, u32); Action::COUNT],
}

impl Default for AuditorStats {
    fn default() -> Self {
        Self {
            replies_seen: 0,
            replies_unknown: 0,
            commits_per_action: [0u64; Action::COUNT],
            committed_rejections: 0,
            denials: 0,
            denials_per_action: [(0, 0); Action::COUNT],
            transient_rejections: 0,
            transient_rejections_per_action: [(0, 0); Action::COUNT],
        }
    }
}

impl AuditorStats {
    #[must_use]
    pub const fn commits(&self, action: Action) -> u64 {
        self.commits_per_action[action as usize]
    }
}

pub struct ServerAuditor {
    in_flight: HashMap<(u128, u64), InFlight>,
    /// High-water mark of `header.commit` per `(client, namespace)`. Not
    /// strictly monotonic: parallel in-flights across namespaces +
    /// at-least-once delivery let replies arrive out of `commit` order.
    /// Each VSR group has its own op counter; this map tracks the
    /// highest seen.
    ///
    /// TODO: reap on client disconnect; bounded today by the fixed set.
    last_commit_watermark_per_client_ns: HashMap<(u128, u64), u64>,
    stats: AuditorStats,
}

impl ServerAuditor {
    #[must_use]
    pub fn new() -> Self {
        Self {
            in_flight: HashMap::new(),
            last_commit_watermark_per_client_ns: HashMap::new(),
            stats: AuditorStats::default(),
        }
    }

    /// Record a new in-flight request keyed by `(client, request)`.
    ///
    /// # Panics
    /// Panics if a request with the same key is already in flight.
    /// `SimClient` must produce strictly monotonic request ids per client.
    pub fn record_in_flight(&mut self, key: (u128, u64), entry: InFlight) {
        let prev = self.in_flight.insert(key, entry);
        assert!(
            prev.is_none(),
            "duplicate in-flight key {key:?}: request ids must be unique per client"
        );
    }

    /// Match a reply to its in-flight entry and update the per-(client,
    /// namespace) last-commit cursor.
    ///
    /// Outcomes:
    /// - [`OnReply::Match`]: entry found, namespace matched. Caller
    ///   classifies, applies effects, decrements the counter.
    /// - [`OnReply::NsMismatch`]: entry consumed but reply namespace
    ///   diverged from the request namespace. Caller decrements but
    ///   skips effects + `note_committed`. Unreachable today (the server
    ///   echoes the request namespace); guards future routing/dedup
    ///   bugs from wedging a client at `CLIENT_REQUEST_QUEUE_MAX = 1`.
    /// - [`OnReply::Unknown`]: no matching entry (duplicate cached
    ///   reply or stale at-least-once re-execution). Caller must not
    ///   decrement.
    ///
    /// The in-flight lookup runs before any watermark update so a stray
    /// reply for an unknown key cannot advance the cursor and mask a
    /// later legitimate regression.
    ///
    /// The previous strict-monotonic assert was removed: with parallel
    /// in-flight requests across namespaces and at-least-once delivery,
    /// replies can legitimately arrive out of `commit` order. The
    /// in-flight cross-check already rejects unknown / misrouted
    /// replies; cross-replica commit-order invariants belong in the
    /// quiesce-time validator (v2.7-base).
    pub fn on_reply(&mut self, key: (u128, u64), header: &ReplyHeader) -> OnReply {
        self.stats.replies_seen += 1;

        // Lookup first so a stray reply cannot advance the watermark.
        let Some(entry) = self.in_flight.remove(&key) else {
            self.stats.replies_unknown += 1;
            return OnReply::Unknown;
        };

        // Replies no longer echo a group id (the client wire has no
        // namespace field at all), so correlation rests entirely on the
        // (client, request) key that fetched `entry`; the group the request
        // was submitted to comes from the sim's own bookkeeping.
        let ns_key = (header.client, entry.request_namespace);
        let last_commit = self
            .last_commit_watermark_per_client_ns
            .entry(ns_key)
            .or_insert(0);
        if header.commit > *last_commit {
            *last_commit = header.commit;
        }

        OnReply::Match(entry)
    }

    /// Increment the per-action committed counter. Called only for a committed
    /// success that mutated the shadow, so it tracks net shadow state (rejections
    /// and no-op applies excluded).
    pub const fn note_committed(&mut self, action: Action) {
        self.stats.commits_per_action[action as usize] += 1;
    }

    /// Record a pre-commit denial (`ReplyHeader::status` nonzero).
    pub const fn note_denial(&mut self, action: Action, status: u32) {
        self.stats.denials += 1;
        let entry = &mut self.stats.denials_per_action[action as usize];
        entry.0 += 1;
        entry.1 = status;
    }

    /// Record a result-framed transport rejection (see
    /// [`AuditorStats::transient_rejections`]). Neither a commit nor a denial:
    /// the shadow does not move, and for `TransientNotCommitted` the request
    /// stays outstanding so a replay can settle what actually happened.
    pub const fn note_transient_rejection(&mut self, action: Action, code: u32) {
        self.stats.transient_rejections += 1;
        let entry = &mut self.stats.transient_rejections_per_action[action as usize];
        entry.0 += 1;
        entry.1 = code;
    }

    /// Record a committed business rejection (nonzero result code). Either
    /// targeted by outcome-first generation (duplicate name, fabricated missing
    /// entity) or produced by a race.
    pub const fn note_committed_rejection(&mut self) {
        self.stats.committed_rejections += 1;
    }

    #[must_use]
    pub const fn stats(&self) -> &AuditorStats {
        &self.stats
    }

    /// Drop every in-flight expectation for `client`, returning how many went.
    ///
    /// For a client the cluster evicted: its session is gone, so nothing it had
    /// outstanding will ever be answered and an expectation left behind would
    /// wait forever. The requests themselves were refused before commit, so
    /// forgetting them loses no committed state.
    pub fn forget_client(&mut self, client: u128) -> usize {
        let before = self.in_flight.len();
        self.in_flight.retain(|&(owner, _), _| owner != client);
        before - self.in_flight.len()
    }

    /// The action of an outstanding request, if one is recorded for `key`.
    /// Diagnostic only: names what a stalled run is waiting on, which the bare
    /// `(client, request)` pair cannot.
    #[must_use]
    pub fn in_flight_action(&self, key: (u128, u64)) -> Option<Action> {
        self.in_flight.get(&key).map(|entry| entry.action)
    }

    #[must_use]
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.len()
    }
}

impl Default for ServerAuditor {
    fn default() -> Self {
        Self::new()
    }
}
