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

//! Per-credential VSR session state: the session entry, the first-use
//! registration barrier, and the pure session-table sweep / forget / lookup
//! helpers the state bridge drives.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::rc::Rc;

use consensus::CLIENTS_TABLE_MAX;
use futures::channel::oneshot;
use message_bus::InstanceToken;
use tokio::sync::Mutex;

/// Hard cap on live per-credential sessions. A leak-guard, not a tuning knob:
/// reaching it means this many distinct live tokens are in flight at once. New
/// sessions past the cap are refused with a transient 503 rather than evicting
/// a live one; the client retries. Expired entries are dropped first, so the
/// cap only bites on live oversubscription.
///
/// Bounded by the shared VSR client table: HTTP sessions and the TCP/QUIC/WS
/// virtual clients all Register into the one [`CLIENTS_TABLE_MAX`]-slot table,
/// which LRU-evicts the oldest client when full. Capping HTTP at half that
/// bound keeps this plane from crowding the others out and keeps the combined
/// steady state under the shared bound, so a live idle HTTP session is not
/// routinely evicted consensus-side. The residual eviction race (both planes
/// busy) degrades gracefully: an evicted session's next control write is
/// classified as an eviction and re-registers (see [`HttpInner::forget_session`]).
pub(in crate::http) const MAX_HTTP_SESSIONS: usize = CLIENTS_TABLE_MAX / 2;

/// First per-session request id the write path hands out. VSR request numbers
/// are 1-based and strictly increasing within a session.
pub(in crate::http) const FIRST_REQUEST_ID: u64 = 1;

/// One VSR session established for a single login credential (a JWT `jti` or a
/// PAT). Shared via `Rc` by every concurrent request bearing that credential,
/// so the session granularity is per-login.
pub(in crate::http) struct HttpSession {
    /// Session-table key this entry lives under (`jwt:{jti}` / `pat:{sha}`).
    /// Held so an eviction observed on the write path can remove exactly this
    /// entry (see [`HttpInner::forget_session`]) without threading the key
    /// through the whole write chain.
    pub(in crate::http) key: String,
    /// Shard-0 client id minted for this credential; its top 16 bits are 0, so
    /// it shares the shard-0 id space with TCP virtual clients without
    /// colliding. Fills `RequestHeader.client` on every write.
    pub(in crate::http) client_id: u128,
    /// Cluster session number returned by the VSR `Register` commit. Fills
    /// `RequestHeader.session` on every write.
    pub(in crate::http) session: u64,
    /// User the credential authenticated as. Consumed by the write path for
    /// authorization.
    pub(in crate::http) user_id: u32,
    /// Credential expiry in unix seconds (`u64::MAX` = never). Drives lazy
    /// eviction of stale table entries.
    pub(in crate::http) expiry: u64,
    /// Serializes this session's writes: the guarded value is the NEXT request
    /// id. A `tokio::sync::Mutex` because the write path holds it across the
    /// submit `.await` so each session's request numbers reach the primary in
    /// order and stay gap-free for the depth-1 consensus dedup.
    pub(in crate::http) gate: Mutex<u64>,
    /// Next data-plane request id. A separate, gate-free counter: partition ops
    /// are at-least-once with no consensus dedup, so the id only correlates the
    /// in-process reply slot and concurrent produces on one session are legal.
    /// A plain `Cell` suffices on single-threaded shard 0; ids are minted
    /// monotonically and never reused, which the slot-guard contract requires.
    pub(in crate::http) data_request: Cell<u64>,
    /// Registry token of this session's lazily-installed in-process reply
    /// target (`None` until the first awaited partition write). Stored so
    /// session eviction can tear the registry entry down fenced by the same
    /// token.
    pub(in crate::http) registry_token: Cell<Option<InstanceToken>>,
    /// Awaited partition writes currently in flight on this session, gated by
    /// [`MAX_IN_FLIGHT_WRITES_PER_SESSION`]. Only [`InFlightWriteGuard`]
    /// touches it, so every admission is paired with exactly one release.
    pub(in crate::http) in_flight_writes: Cell<u32>,
}

impl HttpSession {
    /// Mint the next data-plane request id. Also consumed by the `?ack=none`
    /// path, which installs no slot: sharing one counter keeps a shed reply's
    /// id from ever colliding with a live awaited slot on this session.
    pub(in crate::http) fn next_data_request_id(&self) -> u64 {
        let id = self.data_request.get();
        self.data_request.set(id + 1);
        id
    }
}

/// Serializes first-use VSR registration per credential key so a herd of
/// concurrent first-requests for one token runs exactly one `Register` instead
/// of N that each mint a client id and orphan N-1 slots last-writer-wins.
///
/// Shard 0 is single-threaded, so the whole check-or-claim is a synchronous
/// `RefCell` critical section (no cross-thread machinery): the first caller for
/// a key claims it and gets a [`RegistrationGuard`]; every later caller gets a
/// waiter to park on until the registrant finishes. The guard clears the marker
/// and wakes all waiters on drop - including a cancellation drop, so a
/// disconnected registrant can never wedge its waiters.
#[derive(Default)]
pub(in crate::http) struct RegistrationBarrier {
    /// Key -> waiters parked on the in-flight registration. Dropping a waiter's
    /// sender wakes its receiver; the guard drops the whole vec at once.
    inflight: RefCell<HashMap<String, Vec<oneshot::Sender<()>>>>,
}

/// Outcome of [`RegistrationBarrier::enter`]: lead the registration or wait for
/// the caller that is already leading it.
pub(in crate::http) enum BarrierEntry<'a> {
    Lead(RegistrationGuard<'a>),
    Wait(oneshot::Receiver<()>),
}

/// Held by the sole registrant for a key. On drop it removes the in-flight
/// marker, which drops every parked waiter's sender and so wakes them to
/// re-check the session table.
pub(in crate::http) struct RegistrationGuard<'a> {
    barrier: &'a RegistrationBarrier,
    key: String,
}

impl RegistrationBarrier {
    /// Claim `key` for registration, or return a waiter if another caller
    /// already holds it. Synchronous and borrow-free of any `.await`.
    pub(in crate::http) fn enter(&self, key: &str) -> BarrierEntry<'_> {
        match self.inflight.borrow_mut().entry(key.to_owned()) {
            Entry::Occupied(mut occupied) => {
                let (sender, receiver) = oneshot::channel();
                occupied.get_mut().push(sender);
                BarrierEntry::Wait(receiver)
            }
            Entry::Vacant(vacant) => {
                vacant.insert(Vec::new());
                BarrierEntry::Lead(RegistrationGuard {
                    barrier: self,
                    key: key.to_owned(),
                })
            }
        }
    }
}

impl Drop for RegistrationGuard<'_> {
    fn drop(&mut self) {
        // Dropping the vec of senders wakes every waiter (Canceled), which then
        // re-checks the table for the session this registrant installed.
        self.barrier.inflight.borrow_mut().remove(&self.key);
    }
}

/// Drop every expired entry from the session table, returning the
/// `(client_id, registry token)` of each dropped entry that had installed an
/// in-process reply target so the caller can tear those down outside the
/// borrow. A pure map operation (no shard), so the cap/expiry policy is
/// unit-testable without a live consensus.
pub(in crate::http) fn sweep_expired(
    table: &mut HashMap<String, Rc<HttpSession>>,
    now_secs: u64,
) -> Vec<(u128, InstanceToken)> {
    let mut torn = Vec::new();
    table.retain(|_, session| {
        if session.expiry > now_secs {
            return true;
        }
        if let Some(token) = session.registry_token.get() {
            torn.push((session.client_id, token));
        }
        false
    });
    torn
}

/// Remove `session` from the table only if it is still the current occupant of
/// its key (`Rc::ptr_eq`), returning its reply target to tear down. A stale
/// handle whose key was re-registered to a newer session removes nothing. Pure
/// (no shard) so the eviction-recovery fencing is unit-testable.
pub(in crate::http) fn forget_if_same(
    table: &mut HashMap<String, Rc<HttpSession>>,
    session: &Rc<HttpSession>,
) -> Option<(u128, InstanceToken)> {
    match table.get(&session.key) {
        Some(current) if Rc::ptr_eq(current, session) => {
            let torn = session
                .registry_token
                .get()
                .map(|token| (session.client_id, token));
            table.remove(&session.key);
            torn
        }
        _ => None,
    }
}

/// Borrow-and-clone a live table entry, or `None` if missing or expired. Shared
/// by the fast path and the post-Register re-check so neither leaks a guard.
pub(in crate::http) fn live_entry(
    table: &HashMap<String, Rc<HttpSession>>,
    key: &str,
    now_secs: u64,
) -> Option<Rc<HttpSession>> {
    table
        .get(key)
        .filter(|session| session.expiry > now_secs)
        .map(Rc::clone)
}

#[cfg(test)]
mod tests {
    use super::*;

    use iggy_common::defaults::DEFAULT_ROOT_USER_ID;

    /// Pins the platform contract `submit_committed`'s cancellation safety
    /// rests on: a detached compio task keeps running after the handler side
    /// is gone, the gate advance it performs sticks, and sending the result
    /// into a dropped oneshot receiver is an ignorable `Err`, never a panic.
    /// The full hazard window (client disconnect between the consensus commit
    /// and the id advance) needs a live commit round-trip, so it is covered by
    /// construction plus the live cancellation smoke, not faked here.
    #[compio::test]
    async fn detached_task_advances_gate_and_ignores_dead_receiver() {
        let session = Rc::new(HttpSession {
            key: "jwt:test".to_owned(),
            client_id: 7,
            session: 1,
            user_id: DEFAULT_ROOT_USER_ID,
            expiry: u64::MAX,
            gate: Mutex::new(FIRST_REQUEST_ID),
            data_request: Cell::new(FIRST_REQUEST_ID),
            registry_token: Cell::new(None),
            in_flight_writes: Cell::new(0),
        });
        let (result_slot, committed) = oneshot::channel::<u64>();
        // The handler future dies (client disconnect) before the task runs.
        drop(committed);
        let (done_slot, done) = oneshot::channel::<()>();
        let task_session = Rc::clone(&session);
        compio::runtime::spawn(async move {
            let mut next_request_id = task_session.gate.lock().await;
            *next_request_id += 1;
            let _ = result_slot.send(*next_request_id);
            drop(next_request_id);
            let _ = done_slot.send(());
        })
        .detach();
        done.await.expect("detached task must run to completion");
        assert_eq!(*session.gate.lock().await, FIRST_REQUEST_ID + 1);
    }

    /// `InstanceToken` has no public constructor, so fixtures carry no reply
    /// target; the token-teardown branch of the sweep/forget helpers is
    /// exercised via their `Option` path, not fabricated here.
    fn fake_session(key: &str, client_id: u128, expiry: u64) -> Rc<HttpSession> {
        Rc::new(HttpSession {
            key: key.to_owned(),
            client_id,
            session: 1,
            user_id: DEFAULT_ROOT_USER_ID,
            expiry,
            gate: Mutex::new(FIRST_REQUEST_ID),
            data_request: Cell::new(FIRST_REQUEST_ID),
            registry_token: Cell::new(None),
            in_flight_writes: Cell::new(0),
        })
    }

    // The barrier is what makes a herd of concurrent first-requests for one
    // credential run a single `Register`: only the leader reaches
    // `register_session`; every other caller waits and reuses what it installs.
    // A different credential leads independently, and the key frees on drop.
    #[compio::test]
    async fn registration_barrier_leads_one_caller_and_parks_the_rest() {
        let barrier = RegistrationBarrier::default();

        let BarrierEntry::Lead(leader) = barrier.enter("jwt:a") else {
            panic!("first caller for a key must lead");
        };
        let mut waiters = Vec::new();
        for _ in 0..4 {
            match barrier.enter("jwt:a") {
                BarrierEntry::Wait(waiter) => waiters.push(waiter),
                BarrierEntry::Lead(_) => panic!("a concurrent caller must not lead the same key"),
            }
        }
        assert!(
            matches!(barrier.enter("jwt:b"), BarrierEntry::Lead(_)),
            "a different credential leads independently"
        );

        // Leader finishing wakes every waiter and frees the key.
        drop(leader);
        for waiter in waiters {
            let _ = waiter.await;
        }
        assert!(
            matches!(barrier.enter("jwt:a"), BarrierEntry::Lead(_)),
            "the freed key leads a fresh registration"
        );
    }

    #[test]
    fn sweep_expired_drops_only_expired_entries() {
        let mut table = HashMap::new();
        let live = fake_session("jwt:live", 1, 1_000);
        let stale = fake_session("jwt:stale", 2, 10);
        table.insert(live.key.clone(), Rc::clone(&live));
        table.insert(stale.key.clone(), Rc::clone(&stale));

        // now = 100: live.expiry(1000) > now stays, stale.expiry(10) <= now goes.
        let torn = sweep_expired(&mut table, 100);

        assert!(torn.is_empty(), "fixtures install no reply target");
        assert!(table.contains_key("jwt:live"), "live session retained");
        assert!(!table.contains_key("jwt:stale"), "expired session swept");
    }

    // At the cap the sweep only reclaims expired entries, never a live one, so
    // an all-live table stays full and `resolve_session` refuses (503) rather
    // than evicting a live session.
    #[test]
    fn sweep_never_evicts_live_sessions_so_a_full_table_stays_full() {
        let mut table = HashMap::new();
        for client_id in 0..8u128 {
            let session = fake_session(&format!("jwt:{client_id}"), client_id, 1_000);
            table.insert(session.key.clone(), session);
        }
        let torn = sweep_expired(&mut table, 100);
        assert!(torn.is_empty());
        assert_eq!(table.len(), 8, "no live session is evicted to make room");
    }

    #[test]
    fn http_session_cap_leaves_headroom_below_the_shared_client_table_bound() {
        const {
            assert!(
                MAX_HTTP_SESSIONS < CLIENTS_TABLE_MAX,
                "HTTP must not claim the whole shared VSR client table"
            );
        }
        assert_eq!(MAX_HTTP_SESSIONS, CLIENTS_TABLE_MAX / 2);
    }

    // Eviction recovery: forgetting the evicted session drops exactly its
    // entry, and a stale handle never purges a session that re-registered under
    // the same key in the meantime (the `Rc::ptr_eq` fence).
    #[test]
    fn forget_removes_the_evicted_session_but_spares_a_re_registration() {
        let mut table = HashMap::new();
        let evicted = fake_session("jwt:a", 1, u64::MAX);
        table.insert(evicted.key.clone(), Rc::clone(&evicted));

        assert!(forget_if_same(&mut table, &evicted).is_none());
        assert!(!table.contains_key("jwt:a"), "evicted session removed");

        let replacement = fake_session("jwt:a", 2, u64::MAX);
        table.insert(replacement.key.clone(), Rc::clone(&replacement));
        assert!(
            forget_if_same(&mut table, &evicted).is_none(),
            "a stale handle removes nothing"
        );
        assert!(
            Rc::ptr_eq(
                table.get("jwt:a").expect("replacement present"),
                &replacement
            ),
            "the pointer fence spares the re-registered session"
        );
    }
}
