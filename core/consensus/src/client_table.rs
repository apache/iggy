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

use iggy_binary_protocol::consensus::MESSAGE_ALIGN;
use iggy_binary_protocol::consensus::iobuf::Frozen;
use iggy_binary_protocol::{Message, ReplyHeader};
use std::collections::HashMap;
use std::mem::size_of;
use tracing::trace;

/// Refcounted wrapper around a committed reply.
///
/// Bytes are deterministic across replicas: `build_reply_message` reads
/// only from the prepare header, so a backup-promoted primary replays
/// the exact bytes the original primary produced.
///
/// Immutable by construction: [`Frozen`] has no mutable accessor.
#[derive(Debug, Clone)]
pub struct CachedReply {
    bytes: Frozen<MESSAGE_ALIGN>,
}

impl CachedReply {
    /// Reply header view.
    ///
    /// # Panics
    /// Unreachable: prefix validated by [`Message::try_from`] at construction;
    /// `Frozen` has no mutable accessor.
    #[must_use]
    pub fn header(&self) -> &ReplyHeader {
        bytemuck::checked::try_from_bytes(&self.bytes.as_slice()[..size_of::<ReplyHeader>()])
            .expect("cached reply bytes contain a valid ReplyHeader (validated at storage time)")
    }

    /// Consume into wire-shareable [`Frozen`] buffer.
    ///
    /// `MessageBus::send_to_client` takes `Frozen<MESSAGE_ALIGN>` directly.
    /// To retain the cached entry, `.clone()` (Arc bump) first.
    #[must_use]
    pub fn into_wire_bytes(self) -> Frozen<MESSAGE_ALIGN> {
        self.bytes
    }
}

impl CachedReply {
    /// Freeze owned buffer in place; no alloc. Subsequent `Clone`s are Arc bumps.
    ///
    /// `pub(crate)` so [`Self::header`]'s validity invariant cannot be
    /// bypassed by an unvalidated buffer from outside the crate.
    pub(crate) fn from_message(msg: Message<ReplyHeader>) -> Self {
        Self {
            bytes: msg.into_generic().into_frozen(),
        }
    }
}

/// Reserved request number for [`Operation::Register`](iggy_binary_protocol::Operation::Register).
/// Real requests start at 1 (header validation enforces `request > 0`).
pub const REGISTER_REQUEST_ID: u64 = 0;

/// Per-client entry (VR paper §4, Fig. 2): session + latest committed reply.
///
/// `session` is assigned at registration and fixed for the entry's lifetime.
#[derive(Debug)]
pub struct ClientEntry {
    /// Session number = commit op of the register. Monotonic across
    /// registrations; new register always gets a higher session.
    pub session: u64,
    /// Cached reply for client's latest committed request.
    pub reply: CachedReply,
}

/// Result of checking a request against the client table.
///
/// In-progress dedup is the caller's job, preflights consult
/// `pipeline.has_message_from_client(client_id)`. `ClientTable` only sees
/// committed state.
#[derive(Debug)]
pub enum RequestStatus {
    /// Not seen; proceed with consensus.
    New,
    /// Exact request already committed; re-send cached reply.
    Duplicate(CachedReply),
    /// Older than client's latest committed request; drop silently.
    Stale,
    /// No session for this client; must register first.
    NoSession,
    /// Session number doesn't match the entry.
    SessionMismatch { expected: u64, received: u64 },
    /// Request != `committed + 1`. Skipped numbers would be lost permanently.
    RequestGap { expected: u64, received: u64 },
    /// Client already has a session. From `check_register`.
    AlreadyRegistered {
        session: u64,
        cached_reply: CachedReply,
    },
}

/// VSR client-table: durable per-client session state.
///
/// Fixed-size slot array (source of truth) + `HashMap` index (O(1) lookup).
///
/// ## Plane: metadata-only
///
/// Backs Register session, request contiguity, metadata-retry dedup, and
/// `NoSession`/`SessionTooLow` eviction. Partition plane is at-least-once;
/// `SendMessages` retries can re-commit at a new offset and consumers
/// dedup via message ID (`iggy_common::MessageDeduplicator`).
///
/// Do not add per-partition `ClientTable` or `(client_id, request)` dedup
/// on the partition side, that flips iggy's contract toward at-most-once.
/// See project memory `project_vsr_clients_table_integration`.
///
/// ## Tracking
///
/// Committed state only, latest reply per client. In-flight state
/// (acks, subscribers, in-progress dedup) lives on [`crate::PipelineEntry`].
/// Updated by `commit_reply` / `commit_register`.
///
/// ## Known gaps
///
/// - **Message repair**: gaps stall `commit_journal` until repair lands.
/// - **Checkpoint serialization**: slot layout deterministic, encode/decode TODO.
#[derive(Debug)]
pub struct ClientTable {
    /// `None` = free slot. Deterministic iteration for eviction + serialization.
    slots: Vec<Option<ClientEntry>>,
    /// `client_id` -> slot index. Rebuilt on decode.
    index: HashMap<u128, usize>,
}

impl ClientTable {
    /// `max_clients` caps slots; index pre-sized to avoid rehash storms.
    #[must_use]
    pub fn new(max_clients: usize) -> Self {
        let mut slots = Vec::with_capacity(max_clients);
        slots.resize_with(max_clients, || None);
        Self {
            slots,
            index: HashMap::with_capacity(max_clients),
        }
    }

    /// Check request against table. Session first, then request progression.
    /// For Register, use [`check_register`].
    ///
    /// # Panics
    /// If index points to empty slot (invariant violation).
    #[must_use]
    pub fn check_request(&self, client_id: u128, session: u64, request: u64) -> RequestStatus {
        assert!(client_id != 0, "client_id 0 is reserved for internal use");
        // Header validation guarantees both > 0 at wire layer.
        debug_assert!(session > 0, "check_request: session must be > 0");
        debug_assert!(request > 0, "check_request: request must be > 0");

        // Session check before request: wrong-session must be rejected even if
        // (client_id, request) matches a correct-session pending entry.
        let Some(&slot_idx) = self.index.get(&client_id) else {
            return RequestStatus::NoSession;
        };
        let entry = self.slots[slot_idx].as_ref().expect("index/slot mismatch");

        if session != entry.session {
            return RequestStatus::SessionMismatch {
                expected: entry.session,
                received: session,
            };
        }

        let committed_request = entry.reply.header().request;

        if request < committed_request {
            return RequestStatus::Stale;
        }
        if request == committed_request {
            return RequestStatus::Duplicate(entry.reply.clone());
        }
        if request != committed_request + 1 {
            return RequestStatus::RequestGap {
                expected: committed_request + 1,
                received: request,
            };
        }

        RequestStatus::New
    }

    /// Check register. Valid without existing session; returns
    /// `AlreadyRegistered { session, cached_reply }`.
    ///
    /// Caller does in-flight dedup via `pipeline.has_message_from_client`.
    ///
    /// # Panics
    /// If `client_id == 0` or index points to empty slot.
    #[must_use]
    pub fn check_register(&self, client_id: u128) -> RequestStatus {
        assert!(client_id != 0, "client_id 0 is reserved for internal use");

        let Some(&slot_idx) = self.index.get(&client_id) else {
            return RequestStatus::New;
        };
        let entry = self.slots[slot_idx].as_ref().expect("index/slot mismatch");
        RequestStatus::AlreadyRegistered {
            session: entry.session,
            cached_reply: entry.reply.clone(),
        }
    }

    /// Record committed register; create or update session.
    ///
    /// Session = `reply.header().commit`. Monotonic, deterministic.
    /// Idempotent on same-session WAL replay.
    ///
    /// # Session mismatch (no panic, log + skip)
    ///
    /// - `existing.session > new`: stale WAL replay; newer slot is authoritative.
    /// - `existing.session < new`: duplicate Register at different ops,
    ///   protocol violation; keep existing (other replicas may have agreed on it).
    ///
    /// Was `assert_eq!` pre-fix. `commit_journal` runs without the
    /// `is_caught_up_primary` gate (it's what opens the gate), so a
    /// malformed WAL or capacity-evict-then-reregister race could reach
    /// here and panic the shard pump.
    ///
    /// Full table evicts oldest commit; `in_flight` protects pipeline
    /// holders, see [`Self::evict_oldest`].
    ///
    /// # Panics
    /// If `client_id == 0`, `session == 0`, or `client_id != reply.header().client`.
    /// Session mismatch does NOT panic.
    pub fn commit_register<F>(&mut self, client_id: u128, reply: Message<ReplyHeader>, in_flight: F)
    where
        F: Fn(u128) -> bool,
    {
        assert!(client_id != 0, "client_id 0 is reserved for internal use");
        assert_eq!(
            client_id,
            reply.header().client,
            "commit_register: client_id mismatch (arg={client_id}, header={})",
            reply.header().client
        );

        let session = reply.header().commit;
        assert!(session > 0, "commit_register: session must be > 0");

        let existing = self.index.get(&client_id).copied();

        // Mismatch on re-register: log + skip, not panic. See doc above.
        if let Some(slot_idx) = existing {
            let slot = self.slots[slot_idx].as_ref().expect("index/slot mismatch");
            if slot.session != session {
                tracing::warn!(
                    client_id,
                    existing_session = slot.session,
                    replay_session = session,
                    "commit_register: session mismatch (stale WAL replay or \
                     duplicate Register at different ops); skipping update"
                );
                return;
            }
        }

        // Freeze once; later dedup-hit clones Arc-bump.
        let cached: CachedReply = CachedReply::from_message(reply);

        // Update in place on re-register, else new slot. Reply-delivery
        // channel lives on popped `PipelineEntry`, fired by commit caller
        // after this returns, slot-first ordering, see `commit_reply`.
        if let Some(slot_idx) = existing {
            self.slots[slot_idx]
                .as_mut()
                .expect("index/slot mismatch")
                .reply = cached;
        } else {
            if self.index.len() >= self.slots.len() {
                self.evict_oldest(&in_flight);
            }
            let slot_idx = self.first_free_slot().expect("eviction must free a slot");
            self.slots[slot_idx] = Some(ClientEntry {
                session,
                reply: cached,
            });
            self.index.insert(client_id, slot_idx);
        }
    }

    /// Record committed reply, update in place. Client must be registered.
    ///
    /// `session` is asserted against stored session to guard WAL replay
    /// from clobbering a newer entry.
    ///
    /// Reply delivery is caller's job, `Sender` lives on the popped
    /// `PipelineEntry` ([`crate::PipelineEntry::take_reply_sender`]),
    /// fired AFTER this returns (slot-first ordering).
    ///
    /// **No-op on missing client**: evicted between prepare and commit
    /// (WAL replay or `commit_journal` racing eviction). Wire reply still
    /// ships; cache skipped; client gets `NoSession` next request.
    ///
    /// # Panics
    /// On session mismatch or commit/request regression. Missing client
    /// does NOT panic.
    pub fn commit_reply(&mut self, client_id: u128, session: u64, reply: Message<ReplyHeader>) {
        assert!(client_id != 0, "client_id 0 is reserved for internal use");
        let new_header = reply.header();
        let new_client = new_header.client;
        let new_request = new_header.request;
        let new_commit = new_header.commit;
        assert_eq!(
            client_id, new_client,
            "commit_reply: client_id mismatch (arg={client_id}, header={new_client})",
        );

        let Some(&slot_idx) = self.index.get(&client_id) else {
            // Evicted between prepare and commit (WAL replay or
            // commit_journal racing eviction). Cache no-op; caller still
            // ships wire reply; awaiter still notified via popped
            // PipelineEntry sender.
            trace!(
                client_id,
                new_request,
                "commit_reply: client evicted while being prepared, skipping cache update"
            );
            return;
        };

        let slot = self.slots[slot_idx].as_ref().expect("index/slot mismatch");
        let slot_header = slot.reply.header();
        let slot_commit = slot_header.commit;
        let slot_request = slot_header.request;
        assert_eq!(
            slot.session, session,
            "commit_reply: session mismatch for client {client_id}: \
             entry={}, prepare={session}",
            slot.session
        );
        assert!(
            new_commit >= slot_commit,
            "commit_reply: commit regression for client {client_id}: {slot_commit} -> {new_commit}",
        );
        assert!(
            new_request >= slot_request,
            "commit_reply: request regression for client {client_id}: {slot_request} -> {new_request}",
        );

        // Freeze once; later dedup-hit clones Arc-bump.
        self.slots[slot_idx]
            .as_mut()
            .expect("index/slot mismatch")
            .reply = CachedReply::from_message(reply);
    }

    /// Evict client with oldest commit, preferring no-in-flight.
    ///
    /// Deterministic: fixed-array iteration, ties broken by lowest slot index.
    /// All replicas with same committed state evict the same client.
    ///
    /// `in_flight(client) == true` when pipeline holds an uncommitted
    /// prepare. Skipped in primary pass: evicting would leave the prepare's
    /// commit no-opping the cache while wire reply still ships, dead-sessioning
    /// the client. Fallback (oldest in-flight) fires only if EVERY slot is
    /// in-flight (overload).
    ///
    /// Determinism: pipeline state derives from the agreed log; identical
    /// state -> identical choice. `commit_journal` catch-up has empty pipeline,
    /// so `in_flight` returns `false` everywhere, matches pre-fix policy.
    ///
    /// **Metadata caveat**: pre-checkpoint, eviction breaks at-most-once
    /// for the evicted client, next metadata retry treated as `New`.
    /// Partition plane unaffected (at-least-once, doesn't use this table).
    fn evict_oldest<F>(&mut self, in_flight: &F)
    where
        F: Fn(u128) -> bool,
    {
        let mut evictee: Option<(usize, u64)> = None; // (slot_idx, commit)
        let mut fallback: Option<(usize, u64)> = None; // in-flight clients

        for (idx, slot) in self.slots.iter().enumerate() {
            let Some(entry) = slot else { continue };
            let commit = entry.reply.header().commit;
            let client_id = entry.reply.header().client;
            let target = if in_flight(client_id) {
                &mut fallback
            } else {
                &mut evictee
            };
            let should_pick = match *target {
                None => true,
                Some((_, min_commit)) => commit < min_commit,
            };
            if should_pick {
                *target = Some((idx, commit));
            }
        }

        let pick = evictee.or(fallback);
        if let Some((slot_idx, _)) = pick {
            let entry = self.slots[slot_idx].take().expect("evictee must exist");
            let client_id = entry.reply.header().client;
            self.index.remove(&client_id);
            trace!(client_id, "evict_oldest: removed client from session table");
        }
    }

    fn first_free_slot(&self) -> Option<usize> {
        self.slots.iter().position(Option::is_none)
    }

    /// Cached reply for a client (duplicate re-sends).
    ///
    /// Borrow avoids Arc bump for header-only inspection. Wire-senders
    /// `.clone()` (Arc bump) then `.into_wire_bytes()`.
    #[must_use]
    pub fn get_reply(&self, client_id: u128) -> Option<&CachedReply> {
        let &slot_idx = self.index.get(&client_id)?;
        self.slots[slot_idx].as_ref().map(|entry| &entry.reply)
    }

    /// Session number for a registered client.
    #[must_use]
    pub fn get_session(&self, client_id: u128) -> Option<u64> {
        let &slot_idx = self.index.get(&client_id)?;
        self.slots[slot_idx].as_ref().map(|entry| entry.session)
    }

    /// Active committed entries.
    #[must_use]
    pub fn count(&self) -> usize {
        self.index.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::{Command2, Operation};

    fn make_register_reply(client: u128, commit: u64) -> Message<ReplyHeader> {
        let header_size = std::mem::size_of::<ReplyHeader>();
        let mut msg = Message::<ReplyHeader>::new(header_size);
        let header = bytemuck::checked::try_from_bytes_mut::<ReplyHeader>(
            &mut msg.as_mut_slice()[..header_size],
        )
        .expect("zeroed bytes are valid");
        *header = ReplyHeader {
            client,
            request: REGISTER_REQUEST_ID,
            commit,
            command: Command2::Reply,
            operation: Operation::Register,
            ..ReplyHeader::default()
        };
        msg
    }

    fn make_reply_for(client: u128, request: u64, commit: u64) -> Message<ReplyHeader> {
        let header_size = std::mem::size_of::<ReplyHeader>();
        let mut msg = Message::<ReplyHeader>::new(header_size);
        let header = bytemuck::checked::try_from_bytes_mut::<ReplyHeader>(
            &mut msg.as_mut_slice()[..header_size],
        )
        .expect("zeroed bytes are valid");
        *header = ReplyHeader {
            client,
            request,
            commit,
            command: Command2::Reply,
            operation: Operation::SendMessages,
            ..ReplyHeader::default()
        };
        msg
    }

    /// `in_flight` closure that always returns false, tests don't model pipeline.
    fn no_in_flight() -> impl Fn(u128) -> bool {
        |_| false
    }

    /// Register client 1 at commit 10. Returns (table, session=10).
    fn table_with_client() -> (ClientTable, u64) {
        let mut table = ClientTable::new(10);
        let session = 10;
        table.commit_register(1, make_register_reply(1, session), no_in_flight());
        (table, session)
    }

    // Registration tests

    #[test]
    fn register_creates_session() {
        let mut table = ClientTable::new(10);
        table.commit_register(1, make_register_reply(1, 42), no_in_flight());
        assert_eq!(table.get_session(1), Some(42));
        assert_eq!(table.count(), 1);
    }

    #[test]
    fn check_register_new_client() {
        let table = ClientTable::new(10);
        assert!(matches!(table.check_register(1), RequestStatus::New));
    }

    #[test]
    fn check_register_already_registered() {
        let (table, session) = table_with_client();
        match table.check_register(1) {
            RequestStatus::AlreadyRegistered {
                session: s,
                cached_reply,
            } => {
                assert_eq!(s, session);
                // Cached reply IS the register reply, preflight replays it.
                assert_eq!(cached_reply.header().request, REGISTER_REQUEST_ID);
                assert_eq!(cached_reply.header().commit, session);
            }
            other => panic!("expected AlreadyRegistered, got {other:?}"),
        }
    }

    #[test]
    fn check_register_already_registered_after_progress() {
        let (mut table, session) = table_with_client();
        // Client progresses past registration.
        table.commit_reply(1, 10, make_reply_for(1, 1, 11));
        table.commit_reply(1, 10, make_reply_for(1, 2, 12));
        // Cached reply is now latest app reply; preflight must silent-drop.
        match table.check_register(1) {
            RequestStatus::AlreadyRegistered {
                session: s,
                cached_reply,
            } => {
                assert_eq!(s, session);
                assert_eq!(
                    cached_reply.header().request,
                    2,
                    "cached reply must be the latest app reply, not the register reply"
                );
            }
            other => panic!("expected AlreadyRegistered, got {other:?}"),
        }
    }

    // Session validation tests

    #[test]
    fn check_request_no_session() {
        let table = ClientTable::new(10);
        // Not registered: valid session/request but no entry.
        assert!(matches!(
            table.check_request(1, 99, 1),
            RequestStatus::NoSession
        ));
    }

    #[test]
    fn check_request_session_mismatch() {
        let (table, session) = table_with_client();
        match table.check_request(1, session + 1, 1) {
            RequestStatus::SessionMismatch { expected, received } => {
                assert_eq!(expected, session);
                assert_eq!(received, session + 1);
            }
            other => panic!("expected SessionMismatch, got {other:?}"),
        }
    }

    #[test]
    fn check_request_correct_session_new() {
        let (mut table, session) = table_with_client();
        table.commit_reply(1, 10, make_reply_for(1, 1, 11));
        assert!(matches!(
            table.check_request(1, session, 2),
            RequestStatus::New
        ));
    }

    #[test]
    fn check_request_duplicate_after_commit() {
        let (mut table, session) = table_with_client();
        table.commit_reply(1, 10, make_reply_for(1, 1, 11));
        match table.check_request(1, session, 1) {
            RequestStatus::Duplicate(cached) => assert_eq!(cached.header().request, 1),
            other => panic!("expected Duplicate, got {other:?}"),
        }
    }

    // Dedup across view change. Backup inherits client_table via
    // commit_journal; on failover, retry must return ORIGINAL cached reply
    // (same request, same commit op), no re-execution. Pipeline state is
    // on PipelineEntry, so view-change cleanup doesn't touch slots.
    // Simulator test covers end-to-end; this is the unit invariant.
    #[test]
    fn duplicate_survives_view_change_reset() {
        let (mut table, session) = table_with_client();
        table.commit_reply(1, session, make_reply_for(1, 1, 11));

        match table.check_request(1, session, 1) {
            RequestStatus::Duplicate(cached) => {
                assert_eq!(cached.header().client, 1, "original client_id");
                assert_eq!(cached.header().request, 1, "ORIGINAL request, not re-issue");
                assert_eq!(
                    cached.header().commit,
                    11,
                    "ORIGINAL commit op (no re-exec)"
                );
            }
            other => panic!("expected Duplicate, got {other:?}"),
        }
    }

    #[test]
    fn check_request_stale() {
        let (mut table, session) = table_with_client();
        table.commit_reply(1, 10, make_reply_for(1, 5, 15));
        assert!(matches!(
            table.check_request(1, session, 3),
            RequestStatus::Stale
        ));
    }

    #[test]
    fn check_request_gap_rejected() {
        let (mut table, session) = table_with_client();
        table.commit_reply(1, 10, make_reply_for(1, 1, 11));
        // Skip from 1 to 3, reject.
        match table.check_request(1, session, 3) {
            RequestStatus::RequestGap { expected, received } => {
                assert_eq!(expected, 2);
                assert_eq!(received, 3);
            }
            other => panic!("expected RequestGap, got {other:?}"),
        }
    }

    // Commit tests

    #[test]
    fn commit_caches_reply() {
        let (mut table, _) = table_with_client();
        table.commit_reply(1, 10, make_reply_for(1, 1, 11));
        let cached = table.get_reply(1).expect("should have cached reply");
        assert_eq!(cached.header().request, 1);
    }

    #[test]
    fn commit_updates_preserves_session() {
        let (mut table, session) = table_with_client();
        table.commit_reply(1, 10, make_reply_for(1, 1, 11));
        table.commit_reply(1, 10, make_reply_for(1, 2, 12));
        assert_eq!(table.get_reply(1).unwrap().header().request, 2);
        assert_eq!(table.get_session(1), Some(session));
        assert_eq!(table.count(), 1);
    }

    // Eviction tests

    #[test]
    fn eviction_removes_oldest_commit() {
        let mut table = ClientTable::new(2);
        table.commit_register(100, make_register_reply(100, 10), no_in_flight());
        table.commit_register(200, make_register_reply(200, 20), no_in_flight());
        table.commit_register(300, make_register_reply(300, 30), no_in_flight());
        assert!(table.get_reply(100).is_none());
        assert!(table.get_reply(200).is_some());
        assert!(table.get_reply(300).is_some());
        assert_eq!(table.count(), 2);
    }

    #[test]
    fn eviction_is_deterministic_by_slot_index() {
        let mut table = ClientTable::new(2);
        table.commit_register(100, make_register_reply(100, 10), no_in_flight());
        table.commit_register(200, make_register_reply(200, 10), no_in_flight());
        table.commit_register(300, make_register_reply(300, 30), no_in_flight());
        assert!(table.get_reply(100).is_none());
        assert!(table.get_reply(200).is_some());
        assert!(table.get_reply(300).is_some());
    }

    #[test]
    fn slot_reuse_after_eviction() {
        let mut table = ClientTable::new(1);
        table.commit_register(100, make_register_reply(100, 10), no_in_flight());
        table.commit_register(200, make_register_reply(200, 20), no_in_flight());
        assert!(table.get_reply(100).is_none());
        assert!(table.get_reply(200).is_some());
        assert_eq!(table.count(), 1);
    }

    // Don't evict in-flight client: its commit_reply would no-op cache
    // while wire reply ships, session dies on next request even though
    // THIS one succeeded.
    #[test]
    fn eviction_skips_in_flight_clients() {
        let mut table = ClientTable::new(2);
        table.commit_register(100, make_register_reply(100, 10), no_in_flight());
        table.commit_register(200, make_register_reply(200, 20), no_in_flight());
        // 100 in-flight; eviction must pick 200.
        let in_flight = |c: u128| c == 100;
        table.commit_register(300, make_register_reply(300, 30), in_flight);
        assert!(
            table.get_reply(100).is_some(),
            "in-flight client must survive"
        );
        assert!(
            table.get_reply(200).is_none(),
            "200 evicted as oldest non-in-flight"
        );
        assert!(table.get_reply(300).is_some());
    }

    // All in-flight: pick oldest in-flight (still deterministic, pipeline
    // state is deterministic).
    #[test]
    fn eviction_falls_back_to_oldest_when_all_in_flight() {
        let mut table = ClientTable::new(2);
        table.commit_register(100, make_register_reply(100, 10), no_in_flight());
        table.commit_register(200, make_register_reply(200, 20), no_in_flight());
        let all_in_flight = |_| true;
        table.commit_register(300, make_register_reply(300, 30), all_in_flight);
        assert!(
            table.get_reply(100).is_none(),
            "100 evicted (oldest fallback)"
        );
        assert!(table.get_reply(200).is_some());
        assert!(table.get_reply(300).is_some());
    }

    // Edge cases

    #[test]
    fn commit_register_idempotent_on_replay() {
        let mut table = ClientTable::new(10);
        table.commit_register(1, make_register_reply(1, 10), no_in_flight());
        // Same client_id + session = idempotent (WAL replay).
        table.commit_register(1, make_register_reply(1, 10), no_in_flight());
        assert_eq!(table.get_session(1), Some(10));
        assert_eq!(table.count(), 1);
    }

    // Re-register with mismatched session must not panic shard pump.
    // Stale WAL replay or duplicate Register at different ops; either way
    // log + skip, existing slot stays authoritative.
    #[test]
    fn commit_register_different_session_logs_and_skips() {
        let mut table = ClientTable::new(10);
        table.commit_register(1, make_register_reply(1, 10), no_in_flight());
        // existing=10, replay=20.
        table.commit_register(1, make_register_reply(1, 20), no_in_flight());
        assert_eq!(table.get_session(1), Some(10), "first session stays");
        // Smaller replay session: same skip.
        table.commit_register(1, make_register_reply(1, 5), no_in_flight());
        assert_eq!(table.get_session(1), Some(10));
    }

    // commit_reply for unregistered/evicted client must not panic;
    // wire reply still ships, cache silently skipped.
    #[test]
    fn commit_reply_for_unregistered_client_is_noop() {
        let mut table = ClientTable::new(10);
        // No register: index has no entry.
        table.commit_reply(1, 10, make_reply_for(1, 1, 10));
        assert!(table.get_reply(1).is_none(), "no entry must be created");
        assert_eq!(table.count(), 0);
    }

    #[test]
    #[should_panic(expected = "session mismatch")]
    fn commit_reply_wrong_session_panics() {
        let (mut table, _session) = table_with_client();
        // Registered session=10, commit session=99.
        table.commit_reply(1, 99, make_reply_for(1, 1, 11));
    }

    #[test]
    fn different_clients_independent_sessions() {
        let mut table = ClientTable::new(10);
        table.commit_register(1, make_register_reply(1, 10), no_in_flight());
        table.commit_register(2, make_register_reply(2, 20), no_in_flight());
        assert_eq!(table.get_session(1), Some(10));
        assert_eq!(table.get_session(2), Some(20));
        assert!(matches!(table.check_request(1, 10, 1), RequestStatus::New));
        assert!(matches!(table.check_request(2, 20, 1), RequestStatus::New));
        assert!(matches!(
            table.check_request(1, 20, 1),
            RequestStatus::SessionMismatch { .. }
        ));
    }
}
