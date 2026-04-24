/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// `Dedup` is consumed only by the `#[cfg(test)]` tests below today; the
// request dispatcher loop in server-ng is not yet wired up. Once the
// dispatcher lands and calls `lookup` / `mark_in_flight` / `complete` /
// `evict_client`, the `dead_code` warnings disappear without needing
// this allow. See plan F6.
#![allow(dead_code)]

//! At-most-once **per-shard-lifetime** request dedup window (IGGY-112, P0-T3).
//!
//! Indexed by `(client_id, request)` pairs carried in the existing
//! `RequestHeader.client` (u128) and `RequestHeader.request` (u64) fields.
//! No wire-format change on the server-ng client plane was needed: the
//! P0-T1 design note found the two fields already in place.
//!
//! # Lifetime scope (load-bearing)
//!
//! "At-most-once" applies for the lifetime of THIS shard's `Dedup`
//! instance. Process restart drops every entry: a client that retries a
//! request after a server restart re-applies side effects unless the
//! request is idempotent at the handler level. Cross-process persistence
//! would need a checkpoint file backed by the consensus WAL and is
//! deferred. Operators sizing client-side retry policy must treat the
//! dedup window as a *crash-window* mitigation only.
//!
//! # Per-client ring
//!
//! Per-client state is a fixed-capacity ring holding the last N
//! `(request, Entry)` pairs. A ring avoids unbounded growth under a
//! chatty client without requiring TTL-driven purge ticks. Done entries
//! also carry an `inserted_at_ns` timestamp; `lookup` treats entries
//! older than `ttl_ns` as absent so a replay from far in the past sees
//! `Fresh` rather than a stale cache hit.
//!
//! # Threading
//!
//! The store is NOT thread-safe. Each server-ng shard owns a `Dedup` on
//! its single-threaded compio runtime. Cross-shard routing happens above
//! this layer. Caller MUST invoke [`Dedup::evict_client`] on session
//! disconnect / bind eviction so the per-client `AHashMap` entry is
//! released; without that hook the outer map grows unbounded across the
//! shard's session history.

use ahash::AHashMap;
use bytes::Bytes;

/// Per-client ring capacity on the dedup window. Sized for a pipelining
/// client issuing tens of outstanding requests; small enough to cap
/// memory at `clients * 64 * avg_reply_size`.
pub const DEFAULT_PER_CLIENT_CAPACITY: usize = 64;

/// Entry TTL.
///
/// An entry older than this is invisible to `lookup`; a replay past
/// the window is treated as `Fresh`. 30 s matches the replica-plane
/// handshake window (`core/message_bus/src/auth.rs`) so operators see
/// one number in their threat model, not two.
pub const DEFAULT_ENTRY_TTL_NS: u128 = 30_000_000_000;

/// Result of `Dedup::lookup`.
///
/// Returned as a borrowed view into the cache: `Cached` hands the caller
/// a `&Bytes` it can `clone()` (cheap, reference-counted) before
/// releasing the borrow. Avoids copying the reply payload on the
/// fast-path cache hit.
#[derive(Debug)]
pub enum LookupResult<'a> {
    /// No (live) entry for this `(client, request)`. Caller should call
    /// [`Dedup::mark_in_flight`] and run the handler.
    Fresh,
    /// The same request is currently being processed by an earlier
    /// invocation. Caller policy decides: drop (client retry will find
    /// the cached reply once handler completes), or buffer.
    InFlight,
    /// Handler already ran; return the cached reply.
    ///
    /// The borrow ties to `&Dedup`. Callers MUST `clone()` the `Bytes`
    /// (cheap, ref-counted) before invoking any `&mut Dedup` method on
    /// the same instance; the borrow checker enforces this at compile
    /// time but the contract is worth naming explicitly.
    Cached(&'a Bytes),
}

#[derive(Debug)]
enum Entry {
    InFlight,
    Done { reply: Bytes, inserted_at_ns: u128 },
}

#[derive(Debug)]
struct Slot {
    request: u64,
    entry: Entry,
}

#[derive(Debug)]
struct PerClient {
    slots: Vec<Slot>,
    cursor: usize,
    capacity: usize,
}

impl PerClient {
    fn with_capacity(capacity: usize) -> Self {
        // Capacity > 0 is validated at the public entry
        // (`Dedup::with_config`); every callsite here goes through that
        // path, so no second assert is needed.
        debug_assert!(capacity > 0, "dedup ring capacity must be > 0");
        Self {
            slots: Vec::with_capacity(capacity),
            cursor: 0,
            capacity,
        }
    }

    fn find(&self, request: u64) -> Option<&Slot> {
        self.slots.iter().find(|s| s.request == request)
    }

    fn find_mut(&mut self, request: u64) -> Option<&mut Slot> {
        self.slots.iter_mut().find(|s| s.request == request)
    }

    fn push(&mut self, slot: Slot) {
        if self.slots.len() < self.capacity {
            self.slots.push(slot);
        } else {
            self.slots[self.cursor] = slot;
            self.cursor = (self.cursor + 1) % self.capacity;
        }
    }
}

/// At-most-once request dedup window.
///
/// Single-shard, single-threaded. Callers hold a `&mut Dedup` and drive
/// the lookup / mark / complete flow synchronously on the hot path.
#[derive(Debug)]
pub struct Dedup {
    per_client: AHashMap<u128, PerClient>,
    per_client_capacity: usize,
    ttl_ns: u128,
}

impl Dedup {
    /// Build a window with defaults: 64-entry per-client ring, 30 s TTL.
    #[must_use]
    pub fn new() -> Self {
        Self::with_config(DEFAULT_PER_CLIENT_CAPACITY, DEFAULT_ENTRY_TTL_NS)
    }

    /// # Panics
    /// Panics if `per_client_capacity == 0`. Every producing path in
    /// the crate uses [`DEFAULT_PER_CLIENT_CAPACITY`]; the assertion
    /// guards misconfiguration in tests or future callers.
    #[must_use]
    pub fn with_config(per_client_capacity: usize, ttl_ns: u128) -> Self {
        assert!(per_client_capacity > 0, "capacity must be > 0");
        Self {
            per_client: AHashMap::new(),
            per_client_capacity,
            ttl_ns,
        }
    }

    /// Inspect the state for `(client, request)` at `now_ns`.
    ///
    /// Done entries older than the TTL are treated as absent; the window
    /// still holds the slot but does not report it. On eviction by a
    /// later `push`, the slot is overwritten silently.
    #[must_use]
    pub fn lookup(&self, client: u128, request: u64, now_ns: u128) -> LookupResult<'_> {
        let Some(state) = self.per_client.get(&client) else {
            return LookupResult::Fresh;
        };
        let Some(slot) = state.find(request) else {
            return LookupResult::Fresh;
        };
        match &slot.entry {
            Entry::InFlight => LookupResult::InFlight,
            Entry::Done {
                reply,
                inserted_at_ns,
            } => {
                if now_ns.saturating_sub(*inserted_at_ns) > self.ttl_ns {
                    LookupResult::Fresh
                } else {
                    LookupResult::Cached(reply)
                }
            }
        }
    }

    /// Mark the handler as starting. Returns `true` if the slot was
    /// inserted; `false` if an entry for `(client, request)` already
    /// existed (caller must consult [`Self::lookup`] for the specific
    /// state).
    pub fn mark_in_flight(&mut self, client: u128, request: u64) -> bool {
        let state = self
            .per_client
            .entry(client)
            .or_insert_with(|| PerClient::with_capacity(self.per_client_capacity));
        if state.find(request).is_some() {
            return false;
        }
        state.push(Slot {
            request,
            entry: Entry::InFlight,
        });
        true
    }

    /// Record a completed reply. Transitions the slot from `InFlight`
    /// to `Done`. If no slot exists (e.g. TTL-evicted between
    /// `mark_in_flight` and `complete`), a new `Done` slot is appended.
    pub fn complete(&mut self, client: u128, request: u64, reply: Bytes, now_ns: u128) {
        let state = self
            .per_client
            .entry(client)
            .or_insert_with(|| PerClient::with_capacity(self.per_client_capacity));
        if let Some(slot) = state.find_mut(request) {
            slot.entry = Entry::Done {
                reply,
                inserted_at_ns: now_ns,
            };
            return;
        }
        state.push(Slot {
            request,
            entry: Entry::Done {
                reply,
                inserted_at_ns: now_ns,
            },
        });
    }

    /// Drop all state for `client`. Call on disconnect (or on
    /// `bind_session` eviction inside `SessionManager`) so the
    /// per-client ring is released deterministically rather than
    /// waiting for TTL expiry of individual entries.
    pub fn evict_client(&mut self, client: u128) {
        self.per_client.remove(&client);
    }

    /// Current number of tracked clients. Test / metrics helper.
    #[must_use]
    pub fn client_count(&self) -> usize {
        self.per_client.len()
    }
}

impl Default for Dedup {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bytes(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    #[test]
    fn fresh_lookup_on_empty_store() {
        let d = Dedup::new();
        assert!(matches!(d.lookup(1, 1, 0), LookupResult::Fresh));
    }

    #[test]
    fn mark_then_lookup_returns_in_flight() {
        let mut d = Dedup::new();
        assert!(d.mark_in_flight(1, 1));
        assert!(matches!(d.lookup(1, 1, 0), LookupResult::InFlight));
    }

    #[test]
    fn double_mark_returns_false() {
        let mut d = Dedup::new();
        assert!(d.mark_in_flight(1, 1));
        assert!(!d.mark_in_flight(1, 1));
    }

    #[test]
    fn complete_transitions_to_cached() {
        let mut d = Dedup::new();
        d.mark_in_flight(1, 7);
        d.complete(1, 7, bytes("ok"), 1_000);
        match d.lookup(1, 7, 1_000) {
            LookupResult::Cached(reply) => assert_eq!(&reply[..], b"ok"),
            other => panic!("expected Cached, got {other:?}"),
        }
    }

    #[test]
    fn complete_without_mark_inserts_directly() {
        let mut d = Dedup::new();
        d.complete(1, 7, bytes("ok"), 0);
        assert!(matches!(d.lookup(1, 7, 0), LookupResult::Cached(_)));
    }

    #[test]
    fn ttl_expired_entry_reports_fresh() {
        let mut d = Dedup::with_config(8, 1_000);
        d.complete(1, 7, bytes("ok"), 0);
        assert!(matches!(d.lookup(1, 7, 2_000), LookupResult::Fresh));
    }

    #[test]
    fn ttl_at_boundary_still_cached() {
        let mut d = Dedup::with_config(8, 1_000);
        d.complete(1, 7, bytes("ok"), 0);
        assert!(matches!(d.lookup(1, 7, 1_000), LookupResult::Cached(_)));
    }

    #[test]
    fn ring_evicts_oldest_entry() {
        let mut d = Dedup::with_config(3, DEFAULT_ENTRY_TTL_NS);
        d.complete(1, 1, bytes("a"), 10);
        d.complete(1, 2, bytes("b"), 20);
        d.complete(1, 3, bytes("c"), 30);
        d.complete(1, 4, bytes("d"), 40);
        // Slot for request 1 was overwritten by request 4.
        assert!(matches!(d.lookup(1, 1, 50), LookupResult::Fresh));
        assert!(matches!(d.lookup(1, 2, 50), LookupResult::Cached(_)));
        assert!(matches!(d.lookup(1, 3, 50), LookupResult::Cached(_)));
        assert!(matches!(d.lookup(1, 4, 50), LookupResult::Cached(_)));
    }

    #[test]
    fn evict_client_drops_all_entries() {
        let mut d = Dedup::new();
        d.complete(1, 1, bytes("a"), 10);
        d.complete(1, 2, bytes("b"), 20);
        d.evict_client(1);
        assert!(matches!(d.lookup(1, 1, 30), LookupResult::Fresh));
        assert_eq!(d.client_count(), 0);
    }

    #[test]
    fn isolation_between_clients() {
        let mut d = Dedup::new();
        d.complete(1, 5, bytes("a"), 10);
        d.complete(2, 5, bytes("b"), 20);
        match d.lookup(1, 5, 30) {
            LookupResult::Cached(r) => assert_eq!(&r[..], b"a"),
            other => panic!("expected Cached a, got {other:?}"),
        }
        match d.lookup(2, 5, 30) {
            LookupResult::Cached(r) => assert_eq!(&r[..], b"b"),
            other => panic!("expected Cached b, got {other:?}"),
        }
    }

    #[test]
    fn complete_updates_inserted_at_on_refresh() {
        let mut d = Dedup::with_config(8, 100);
        d.complete(1, 1, bytes("v1"), 0);
        // Refresh with new timestamp and payload.
        d.complete(1, 1, bytes("v2"), 50);
        match d.lookup(1, 1, 140) {
            LookupResult::Cached(r) => assert_eq!(&r[..], b"v2"),
            other => panic!("expected Cached v2, got {other:?}"),
        }
        // Would have been TTL-expired if inserted_at had stayed at 0.
    }

    #[test]
    #[should_panic(expected = "capacity must be > 0")]
    fn zero_capacity_rejected() {
        let _ = Dedup::with_config(0, DEFAULT_ENTRY_TTL_NS);
    }
}
