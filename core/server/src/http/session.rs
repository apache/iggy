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
use std::rc::{Rc, Weak};

use crate::consumer_group::lease::SessionActivity;
use consensus::CLIENTS_TABLE_MAX;
use consensus::client_table::SessionAttachment;
use futures::channel::oneshot;
use message_bus::InstanceToken;
use tokio::sync::Mutex;

/// HTTP's slice of the shared VSR client table: half the configured
/// `[metadata] clients_table_max`. A leak-guard, not a tuning knob: reaching
/// the cap means that many distinct live tokens are in flight at once. New
/// sessions past it are refused with a transient 503 rather than evicting a
/// live one; the client retries. Expired entries are dropped first, so the cap
/// only bites on live oversubscription.
///
/// HTTP and TCP/QUIC/WS sessions share the durable registry. Disconnect drops
/// a transport binding and retains its session. The registry refuses new
/// registrations at capacity; ended sessions keep their slots until ordered
/// retirement crosses the metadata and partition barriers. HTTP reserves its
/// local capacity before Register so refused credentials consume no shared
/// slots. The half cap leaves room for registrations from other transports.
///
/// The half rule lives here so a change to the ratio flows to both the runtime
/// value (threaded through `HttpInner` at boot) and the pinned default.
pub(in crate::http) const fn max_http_sessions(clients_table_max: usize) -> usize {
    clients_table_max / 2
}

/// HTTP session cap at the shipped-default client table ([`CLIENTS_TABLE_MAX`]).
/// The runtime value ([`max_http_sessions`] of the configured
/// `clients_table_max`) equals this on a default deployment; the tests pin it.
pub(in crate::http) const DEFAULT_MAX_HTTP_SESSIONS: usize = max_http_sessions(CLIENTS_TABLE_MAX);

// HTTP must never claim the whole shared VSR client table. Compile-time pin of the
// headroom the half-cap guarantees (config validation floors the table at 2, so
// the runtime cap keeps the same headroom).
const _: () = assert!(DEFAULT_MAX_HTTP_SESSIONS < CLIENTS_TABLE_MAX);

/// Watermark a brand-new client-table entry carries: no application request
/// has committed under it yet. A fresh mint that comes back with anything else
/// bound to an entry that already existed (see `HttpInner::register_session`).
pub(in crate::http) const FRESH_ENTRY_WATERMARK: u64 = 0;

/// First per-session request id the write path hands out. VSR request numbers
/// are 1-based and strictly increasing within a session.
pub(in crate::http) const FIRST_REQUEST_ID: u64 = 1;

const PARTITION_GATE_SWEEP_THRESHOLD: usize = 64;

/// One VSR session established for a single login credential (a JWT `jti` or a
/// PAT). Shared via `Rc` by every concurrent request bearing that credential,
/// so the session granularity is per-login.
pub(in crate::http) struct HttpSession {
    pub(in crate::http) attachment: RefCell<SessionAttachment>,
    pub(in crate::http) activity: Rc<SessionActivity>,
    /// Session-table key this entry lives under (`jwt:{jti}` / `pat:{sha}`).
    /// Held so an eviction observed on the write path can remove exactly this
    /// entry (see [`HttpInner::forget_session`]) without threading the key
    /// through the whole write chain.
    pub(in crate::http) key: String,
    /// Shard-0 client id minted for this credential; its top 16 bits are 0, so
    /// it shares the shard-0 id space with TCP virtual clients without
    /// colliding. Fills `RoutedRequestHeader.client` on every write.
    pub(in crate::http) client_id: u128,
    /// Cluster session number returned by the VSR `Register` commit. Fills
    /// `RoutedRequestHeader.session` on every write.
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
    /// order. Ordering is what matters, not contiguity: the client table dedups
    /// on a watermark (see `submit.rs`), so gaps are free but an id overtaken by
    /// a larger one would arrive at or below the watermark and be refused as a
    /// duplicate.
    pub(in crate::http) gate: Mutex<u64>,
    /// Allocates monotonically increasing partition request ids. Each partition
    /// gate serializes its unresolved writes through the reply wait, while
    /// unrelated partitions can progress independently.
    pub(in crate::http) data_gate: Mutex<u64>,
    pub(in crate::http) partition_gates: RefCell<HashMap<u64, Weak<Mutex<()>>>>,
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
    pub(in crate::http) fn partition_gate(&self, namespace: u64) -> Rc<Mutex<()>> {
        let mut gates = self.partition_gates.borrow_mut();
        if let Some(gate) = gates.get(&namespace).and_then(Weak::upgrade) {
            return gate;
        }
        if gates.len() >= PARTITION_GATE_SWEEP_THRESHOLD {
            gates.retain(|_, gate| gate.strong_count() != 0);
        }
        let gate = Rc::new(Mutex::new(()));
        gates.insert(namespace, Rc::downgrade(&gate));
        gate
    }

    pub(in crate::http) fn reattach(&self, registry: &mut consensus::ClientTable) {
        if self.attachment.borrow().is_valid() {
            return;
        }
        if let Some(attachment) =
            registry.attach_session(self.client_id, self.session, self.user_id)
        {
            *self.attachment.borrow_mut() = attachment;
        }
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

/// Lead a registration, wait for its leader, or refuse a full registration queue.
pub(in crate::http) enum BarrierEntry<'a> {
    Lead(RegistrationGuard<'a>),
    Wait(oneshot::Receiver<()>),
    Full,
}

/// Held by the sole registrant for a key. On drop it removes the in-flight
/// marker, which drops every parked waiter's sender and so wakes them to
/// re-check the session table.
pub(in crate::http) struct RegistrationGuard<'a> {
    barrier: &'a RegistrationBarrier,
    key: String,
}

impl RegistrationBarrier {
    /// Reserve one of the slots not occupied by installed sessions. Pending
    /// registrations count against this bound; callers of an existing key wait.
    /// Synchronous and borrow-free of any `.await`.
    pub(in crate::http) fn enter(&self, key: &str, available_slots: usize) -> BarrierEntry<'_> {
        let mut inflight = self.inflight.borrow_mut();
        let full = inflight.len() >= available_slots;
        match inflight.entry(key.to_owned()) {
            Entry::Occupied(mut occupied) => {
                let (sender, receiver) = oneshot::channel();
                occupied.get_mut().push(sender);
                BarrierEntry::Wait(receiver)
            }
            Entry::Vacant(vacant) => {
                if full {
                    return BarrierEntry::Full;
                }
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
        if session.expiry > now_secs && session.attachment.borrow().is_valid() {
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
        .filter(|session| session.expiry > now_secs && session.attachment.borrow().is_valid())
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
        let (_registry, session) = fake_session("jwt:test", 7, u64::MAX);
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

    #[tokio::test]
    async fn partition_gates_serialize_one_namespace_without_blocking_another() {
        let (_registry, session) = fake_session("jwt:gate", 1, u64::MAX);
        let first = session.partition_gate(1);
        let held = first.lock().await;
        let same = session.partition_gate(1);
        assert!(same.try_lock().is_err());
        let other = session.partition_gate(2);
        assert!(other.try_lock().is_ok());
        drop(held);
        assert!(same.try_lock().is_ok());
        drop(other);
        for namespace in 3..100 {
            let gate = session.partition_gate(namespace);
            assert!(gate.try_lock().is_ok());
            assert!(session.partition_gates.borrow().len() <= PARTITION_GATE_SWEEP_THRESHOLD);
        }
    }

    #[tokio::test]
    async fn registry_replacement_preserves_http_identity_and_request_counters() {
        let (registry, session) = fake_session("jwt:replacement", 1, u64::MAX);
        *session.gate.lock().await = 4;
        *session.data_gate.lock().await = 8;
        let partition_gate = session.partition_gate(1);
        let mut replacement = consensus::ClientTable::decode(&registry.encode()).unwrap();
        drop(registry);
        assert!(!session.attachment.borrow().is_valid());

        session.reattach(&mut replacement);

        assert!(session.attachment.borrow().is_valid());
        assert_eq!((session.client_id, session.session), (1, 1));
        assert_eq!(*session.gate.lock().await, 4);
        assert_eq!(*session.data_gate.lock().await, 8);
        assert!(Rc::ptr_eq(&partition_gate, &session.partition_gate(1)));
        replacement.end_session(1, DEFAULT_ROOT_USER_ID, 1, 2);
        session.reattach(&mut replacement);
        assert!(!session.attachment.borrow().is_valid());
    }

    /// `InstanceToken` has no public constructor, so fixtures carry no reply
    /// target; the token-teardown branch of the sweep/forget helpers is
    /// exercised via their `Option` path, not fabricated here.
    fn fake_session(
        key: &str,
        client_id: u128,
        expiry: u64,
    ) -> (consensus::ClientTable, Rc<HttpSession>) {
        let mut registry = consensus::ClientTable::new(1);
        let reply = server_common::Message::<iggy_binary_protocol::ReplyHeader>::new(
            iggy_binary_protocol::HEADER_SIZE,
        )
        .transmute_header(|_, header: &mut iggy_binary_protocol::ReplyHeader| {
            header.command = iggy_binary_protocol::Command::Reply;
            header.size = u32::try_from(iggy_binary_protocol::HEADER_SIZE).unwrap();
            header.client = client_id;
            header.commit = 1;
            header.operation = iggy_binary_protocol::Operation::Register;
        });
        registry
            .commit_register(client_id, DEFAULT_ROOT_USER_ID, [0x5a; 32], reply)
            .unwrap();
        let attachment = registry
            .attach_session(client_id, 1, DEFAULT_ROOT_USER_ID)
            .unwrap();
        (
            registry,
            Rc::new(HttpSession {
                attachment: RefCell::new(attachment),
                activity: Rc::new(SessionActivity::new(
                    iggy_binary_protocol::ConsumerSession {
                        client_id,
                        session: 1,
                    },
                )),
                key: key.to_owned(),
                client_id,
                session: 1,
                user_id: DEFAULT_ROOT_USER_ID,
                expiry,
                gate: Mutex::new(FIRST_REQUEST_ID),
                data_gate: Mutex::new(FIRST_REQUEST_ID),
                partition_gates: RefCell::default(),
                registry_token: Cell::new(None),
                in_flight_writes: Cell::new(0),
            }),
        )
    }

    // The barrier is what makes a herd of concurrent first-requests for one
    // credential run a single `Register`: only the leader reaches
    // `register_session`; every other caller waits and reuses what it installs.
    // A different credential leads independently, and the key frees on drop.
    #[compio::test]
    async fn registration_barrier_leads_one_caller_and_parks_the_rest() {
        let barrier = RegistrationBarrier::default();

        let BarrierEntry::Lead(leader) = barrier.enter("jwt:a", usize::MAX) else {
            panic!("first caller for a key must lead");
        };
        let mut waiters = Vec::new();
        for _ in 0..4 {
            match barrier.enter("jwt:a", usize::MAX) {
                BarrierEntry::Wait(waiter) => waiters.push(waiter),
                BarrierEntry::Lead(_) => panic!("a concurrent caller must not lead the same key"),
                BarrierEntry::Full => panic!("fixture has unlimited capacity"),
            }
        }
        assert!(
            matches!(barrier.enter("jwt:b", usize::MAX), BarrierEntry::Lead(_)),
            "a different credential leads independently"
        );

        // Leader finishing wakes every waiter and frees the key.
        drop(leader);
        for waiter in waiters {
            let _ = waiter.await;
        }
        assert!(
            matches!(barrier.enter("jwt:a", usize::MAX), BarrierEntry::Lead(_)),
            "the freed key leads a fresh registration"
        );
    }

    #[test]
    fn registration_reservations_bound_pending_keys_and_release_on_cancellation() {
        const AVAILABLE_SLOTS: usize = 1;
        let barrier = RegistrationBarrier::default();
        let BarrierEntry::Lead(reservation) = barrier.enter("jwt:a", AVAILABLE_SLOTS) else {
            panic!("the first credential must reserve the available slot");
        };
        assert!(matches!(
            barrier.enter("jwt:b", AVAILABLE_SLOTS),
            BarrierEntry::Full
        ));
        assert!(matches!(barrier.enter("jwt:a", 0), BarrierEntry::Wait(_)));
        drop(reservation);
        assert!(matches!(
            barrier.enter("jwt:b", AVAILABLE_SLOTS),
            BarrierEntry::Lead(_)
        ));
        assert!(matches!(barrier.enter("jwt:c", 0), BarrierEntry::Full));
    }

    #[test]
    fn sweep_expired_drops_only_expired_entries() {
        let mut table = HashMap::new();
        let (_live_registry, live) = fake_session("jwt:live", 1, 1_000);
        let (_stale_registry, stale) = fake_session("jwt:stale", 2, 10);
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
        let mut registries = Vec::new();
        for client_id in 1..=8u128 {
            let (registry, session) = fake_session(&format!("jwt:{client_id}"), client_id, 1_000);
            registries.push(registry);
            table.insert(session.key.clone(), session);
        }
        let torn = sweep_expired(&mut table, 100);
        assert!(torn.is_empty());
        assert_eq!(table.len(), 8, "no live session is evicted to make room");
        assert_eq!(registries.len(), table.len());
    }

    // Pins the specific half ratio (not just headroom, which the module-level
    // compile assert covers): narrowing the split would silently starve HTTP.
    #[test]
    fn http_session_cap_is_half_the_shared_client_table_bound() {
        assert_eq!(DEFAULT_MAX_HTTP_SESSIONS, CLIENTS_TABLE_MAX / 2);
    }

    // Zero behavior change at defaults: the cap computed at boot from the
    // shipped `[metadata] clients_table_max` equals the historical compile-time
    // default, so surfacing the table size as config leaves a default
    // deployment untouched.
    #[test]
    fn runtime_cap_at_default_config_equals_pinned_default() {
        let configured = configs::metadata::MetadataConfig::default().clients_table_max;
        assert_eq!(max_http_sessions(configured), DEFAULT_MAX_HTTP_SESSIONS);
    }

    #[test]
    fn ended_session_is_not_live_even_with_an_unexpired_token() {
        let (mut registry, session) = fake_session("jwt:ended", 7, u64::MAX);
        let mut table = HashMap::from([(session.key.clone(), Rc::clone(&session))]);
        assert!(live_entry(&table, &session.key, 100).is_some());
        assert!(registry.end_session(session.client_id, session.user_id, session.session, 2));
        assert!(live_entry(&table, &session.key, 100).is_none());
        assert!(sweep_expired(&mut table, 100).is_empty());
        assert!(table.is_empty());
    }

    // Eviction recovery: forgetting the evicted session drops exactly its
    // entry, and a stale handle never purges a session that re-registered under
    // the same key in the meantime (the `Rc::ptr_eq` fence).
    #[test]
    fn forget_removes_the_evicted_session_but_spares_a_re_registration() {
        let mut table = HashMap::new();
        let (_evicted_registry, evicted) = fake_session("jwt:a", 1, u64::MAX);
        table.insert(evicted.key.clone(), Rc::clone(&evicted));

        assert!(forget_if_same(&mut table, &evicted).is_none());
        assert!(!table.contains_key("jwt:a"), "evicted session removed");

        let (_replacement_registry, replacement) = fake_session("jwt:a", 2, u64::MAX);
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
