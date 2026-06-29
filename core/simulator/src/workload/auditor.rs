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
    /// Metadata replies carrying a nonzero committed result code (a business
    /// rejection). The shadow does not mutate on these; in a serial run the
    /// `on_reply` equality oracle asserts the rejection was the targeted outcome.
    pub committed_rejections: u64,
}

impl Default for AuditorStats {
    fn default() -> Self {
        Self {
            replies_seen: 0,
            replies_unknown: 0,
            commits_per_action: [0u64; Action::COUNT],
            committed_rejections: 0,
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
    ///   skips effects + `note_committed`. Unreachable today (server-ng
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

        // Reply's namespace must match the namespace the request was
        // submitted to. A mismatch means the reply landed in the wrong
        // VSR group's bookkeeping; refuse to apply effects against the
        // wrong shadow bucket. Entry already consumed.
        if entry.request_namespace != header.namespace {
            self.stats.replies_unknown += 1;
            return OnReply::NsMismatch;
        }

        let ns_key = (header.client, header.namespace);
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
