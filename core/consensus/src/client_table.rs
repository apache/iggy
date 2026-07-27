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

use iggy_binary_protocol::ReplyHeader;
use server_common::{MESSAGE_ALIGN, Message, iobuf::Frozen};
use std::collections::{HashMap, VecDeque};
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

/// Displaced replies retained per entry for below-watermark duplicate hits.
///
/// The SDK enforces one request in flight per session, so the only reply a
/// live client can be waiting for is its latest (`request == watermark`).
/// The ring answers old retransmits and post-rebind stragglers with the
/// original bytes instead of a bare "already applied"; losing an entry
/// degrades the answer, never correctness. In-memory only: ring contents are
/// refcount bumps and are never persisted or transferred.
const REPLY_RING_CAPACITY: usize = 4;

/// Per-session entry: fence epoch + committed-request watermark + replies.
///
/// The key (`client_id` today, the stable `session_id` once SDK identity
/// stability lands) is client-supplied; `epoch` is the server-minted fence
/// that orders rebinds of that key.
#[derive(Debug)]
pub struct ClientEntry {
    /// Fence epoch: 1 at first register, +1 per committed re-register.
    /// Minted here, in apply order, so every replica derives the same value.
    /// Requests stamped with an older epoch are zombies and get fenced;
    /// a newer epoch than minted is a protocol violation.
    epoch: u64,
    /// Acting user id captured at register (re-register refreshes it: the
    /// rebind re-authenticated). Lets every replica resolve session -> user
    /// without a metadata lookup.
    user_id: u32,
    /// Highest committed request number. `REGISTER_REQUEST_ID` (0) until the
    /// first app op commits. Survives re-register: a resumed session keeps
    /// its dedup history.
    watermark: u64,
    /// `request_checksum` of the watermark request; catches a client reusing
    /// a request id for a different operation. Zero when unstamped (integrity
    /// fields are zeroed on the wire today), which disables the comparison.
    watermark_checksum: u128,
    /// Latest committed reply (register or app op).
    reply: CachedReply,
    /// Displaced app replies, oldest at front, bounded by
    /// [`REPLY_RING_CAPACITY`]. Register replies never enter (their
    /// `request == REGISTER_REQUEST_ID` can never match a lookup).
    ring: VecDeque<CachedReply>,
}

/// Result of checking a request against the client table.
///
/// In-progress dedup is the caller's job, preflights consult
/// `pipeline.has_message_from_client(client_id)`. `ClientTable` only sees
/// committed state.
#[derive(Debug)]
pub enum RequestStatus {
    /// Above the watermark; proceed with consensus. Jumps are allowed: the
    /// watermark records the highest committed request, not a contiguous
    /// sequence, so `watermark + k` for any `k >= 1` is new.
    New,
    /// At or below the watermark with the original reply still cached;
    /// re-send it.
    Duplicate(CachedReply),
    /// At or below the watermark, original reply no longer cached. Applied
    /// once already; must not re-execute, nothing to replay.
    AlreadyApplied { request: u64, watermark: u64 },
    /// Request number matches the watermark but its `request_checksum`
    /// differs: the client reused a request id for a different operation.
    /// Returning the cached reply would answer the wrong request.
    ChecksumMismatch { request: u64 },
    /// No entry for this client; must register first.
    NoSession,
    /// Stamped epoch is older than the entry's: a zombie holdover from
    /// before a re-register. Terminal for that holder.
    Fenced { current: u64, received: u64 },
    /// Stamped epoch is newer than any this table minted: client bug
    /// (epochs are only handed out by register replies).
    EpochAhead { current: u64, received: u64 },
    /// Client already has an entry. From `check_register`.
    AlreadyRegistered {
        epoch: u64,
        cached_reply: CachedReply,
    },
}

/// VSR client table: per-session fence epoch + request-watermark dedup.
///
/// Fixed-size slot array (source of truth) + `HashMap` index (O(1) lookup).
///
/// ## Semantics (v2)
///
/// - **Epoch, not commit.** Session identity is the client-supplied key;
///   the entry's `epoch` is a plain counter minted at `commit_register`
///   (1, then +1 per rebind). No field derives from a commit op number, so
///   the same table logic serves any consensus group.
/// - **Watermark, not contiguity.** A request above the watermark executes
///   (gaps allowed); at or below is a duplicate. There is no `RequestGap`:
///   a client that jumps its counter loses nothing but the skipped ids.
/// - **Replies are volatile.** Latest reply plus a small ring of displaced
///   ones, all in-memory refcounts. A duplicate whose reply aged out is
///   still refused execution ([`RequestStatus::AlreadyApplied`]).
///
/// ## Plane
///
/// Metadata-plane today. The design spans planes (one logical table,
/// group-resident slices); partition-plane integration arrives once
/// partition prepares carry real `(session_id, request)` instead of the
/// transport id (data-plane request numbering, IGGY-137). Until then the
/// partition plane stays at-least-once with no dedup.
///
/// ## Tracking
///
/// Committed state only. In-flight state (acks, subscribers, in-progress
/// dedup) lives on [`crate::PipelineEntry`]. Updated by `commit_reply` /
/// `commit_register` in the apply path, so every replica of the group
/// derives an identical table from the committed log.
///
/// ## Known gaps
///
/// - **Serialization**: encode/decode for rejoin slice-fetch and state
///   transfer TODO (IGGY-137).
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

    /// Check a request against the table. Epoch fence first, then the
    /// watermark. For Register, use [`Self::check_register`].
    ///
    /// `request_checksum` is the request's integrity stamp; zero (unstamped)
    /// disables the reuse check.
    ///
    /// # Panics
    /// If index points to empty slot (invariant violation).
    #[must_use]
    pub fn check_request(
        &self,
        client_id: u128,
        epoch: u64,
        request: u64,
        request_checksum: u128,
    ) -> RequestStatus {
        assert!(client_id != 0, "client_id 0 is reserved for internal use");
        // Header validation guarantees both > 0 at wire layer.
        debug_assert!(epoch > 0, "check_request: epoch must be > 0");
        debug_assert!(request > 0, "check_request: request must be > 0");

        // Epoch check before request: a fenced zombie must be rejected even
        // if its request number would read as a clean duplicate.
        let Some(&slot_idx) = self.index.get(&client_id) else {
            return RequestStatus::NoSession;
        };
        let entry = self.slots[slot_idx].as_ref().expect("index/slot mismatch");

        if epoch < entry.epoch {
            return RequestStatus::Fenced {
                current: entry.epoch,
                received: epoch,
            };
        }
        if epoch > entry.epoch {
            return RequestStatus::EpochAhead {
                current: entry.epoch,
                received: epoch,
            };
        }

        if request > entry.watermark {
            return RequestStatus::New;
        }

        if request == entry.watermark
            && entry.watermark_checksum != 0
            && request_checksum != 0
            && entry.watermark_checksum != request_checksum
        {
            return RequestStatus::ChecksumMismatch { request };
        }

        match entry.find_cached(request) {
            Some(cached) => RequestStatus::Duplicate(cached.clone()),
            None => RequestStatus::AlreadyApplied {
                request,
                watermark: entry.watermark,
            },
        }
    }

    /// Check register. Valid without existing entry; returns
    /// `AlreadyRegistered { epoch, cached_reply }` otherwise.
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
            epoch: entry.epoch,
            cached_reply: entry.reply.clone(),
        }
    }

    /// Record a committed register: create the entry at epoch 1, or bump the
    /// existing entry's epoch (rebind).
    ///
    /// The epoch is minted HERE, in apply order, so it is deterministic
    /// across replicas without reading any commit number. A rebind refreshes
    /// `user_id` (the bind re-authenticated), replaces the latest reply with
    /// the register reply (the displaced app reply moves into the ring), and
    /// preserves the watermark - session resume keeps dedup history.
    ///
    /// Full table evicts oldest commit; `in_flight` protects pipeline
    /// holders, see [`Self::evict_oldest`].
    ///
    /// # Panics
    /// If `client_id == 0` or `client_id != reply.header().client`.
    pub fn commit_register<F>(
        &mut self,
        client_id: u128,
        user_id: u32,
        reply: Message<ReplyHeader>,
        in_flight: F,
    ) where
        F: Fn(u128) -> bool,
    {
        assert!(client_id != 0, "client_id 0 is reserved for internal use");
        assert_eq!(
            client_id,
            reply.header().client,
            "commit_register: client_id mismatch (arg={client_id}, header={})",
            reply.header().client
        );

        // Freeze once; later dedup-hit clones Arc-bump.
        let cached: CachedReply = CachedReply::from_message(reply);

        if let Some(&slot_idx) = self.index.get(&client_id) {
            let entry = self.slots[slot_idx].as_mut().expect("index/slot mismatch");
            entry.epoch += 1;
            entry.user_id = user_id;
            let displaced = std::mem::replace(&mut entry.reply, cached);
            entry.push_ring(displaced);
        } else {
            if self.index.len() >= self.slots.len() {
                self.evict_oldest(&in_flight);
            }
            let slot_idx = self.first_free_slot().expect("eviction must free a slot");
            self.slots[slot_idx] = Some(ClientEntry {
                epoch: 1,
                user_id,
                watermark: REGISTER_REQUEST_ID,
                watermark_checksum: 0,
                reply: cached,
                ring: VecDeque::with_capacity(REPLY_RING_CAPACITY),
            });
            self.index.insert(client_id, slot_idx);
        }
    }

    /// Record a committed reply: advance the watermark, cache the reply,
    /// move the displaced one into the ring.
    ///
    /// `epoch` is asserted against the entry to guard a mis-attributed apply
    /// from clobbering a rebound session's state.
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
    /// On epoch mismatch or commit/watermark regression. Missing client
    /// does NOT panic.
    pub fn commit_reply(&mut self, client_id: u128, epoch: u64, reply: Message<ReplyHeader>) {
        assert!(client_id != 0, "client_id 0 is reserved for internal use");
        let new_header = reply.header();
        let new_client = new_header.client;
        let new_request = new_header.request;
        let new_commit = new_header.commit;
        let new_checksum = new_header.request_checksum;
        assert_eq!(
            client_id, new_client,
            "commit_reply: client_id mismatch (arg={client_id}, header={new_client})",
        );
        debug_assert!(
            new_request > REGISTER_REQUEST_ID,
            "commit_reply: register replies go through commit_register"
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

        let entry = self.slots[slot_idx].as_mut().expect("index/slot mismatch");
        assert_eq!(
            entry.epoch, epoch,
            "commit_reply: epoch mismatch for client {client_id}: \
             entry={}, prepare={epoch}",
            entry.epoch
        );
        let latest_commit = entry.reply.header().commit;
        assert!(
            new_commit >= latest_commit,
            "commit_reply: commit regression for client {client_id}: {latest_commit} -> {new_commit}",
        );
        assert!(
            new_request >= entry.watermark,
            "commit_reply: watermark regression for client {client_id}: {} -> {new_request}",
            entry.watermark
        );

        // Freeze once; later dedup-hit clones Arc-bump.
        let cached = CachedReply::from_message(reply);
        if new_request == entry.watermark {
            // Same request re-committed (WAL replay shape): replace in
            // place, never push the stale twin into the ring - two cached
            // replies for one request number would make lookups ambiguous.
            entry.reply = cached;
        } else {
            let displaced = std::mem::replace(&mut entry.reply, cached);
            entry.push_ring(displaced);
            entry.watermark = new_request;
        }
        entry.watermark_checksum = new_checksum;
    }

    /// Remove a client session and cached replies.
    ///
    /// **LOCAL ONLY -- does NOT replicate.** Two correct call sites:
    ///
    /// 1. **Applying a committed `Operation::Logout`** -- every replica runs
    ///    this from `on_ack` / `commit_journal` during deterministic apply,
    ///    so all replicas drop the slot together. Required-on-every-replica.
    /// 2. **Transport-level disconnect cleanup** -- best-effort capacity
    ///    reclaim. Bounded window of local-vs-cluster divergence until
    ///    `evict_oldest` or a `Logout` commit catches the peer side up.
    ///
    /// **Forbidden:** using this to roll back a cluster-committed
    /// `Operation::Register` -- peers keep the slot, producing divergence
    /// that survives view changes.
    ///
    /// Returns `true` when a slot existed.
    ///
    /// [`Operation::Register`]: iggy_binary_protocol::Operation
    pub fn remove_client(&mut self, client_id: u128) -> bool {
        let Some(slot_idx) = self.index.remove(&client_id) else {
            return false;
        };
        self.slots[slot_idx] = None;
        true
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
    /// **Caveat**: eviction erases the evicted session's watermark, so its
    /// next retry is treated as `New` (re-executes). Bounded by table
    /// capacity; the op-TTL + slice persistence work (IGGY-137) shrinks it.
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

    /// Latest cached reply for a client.
    ///
    /// Borrow avoids Arc bump for header-only inspection. Wire-senders
    /// `.clone()` (Arc bump) then `.into_wire_bytes()`.
    #[must_use]
    pub fn get_reply(&self, client_id: u128) -> Option<&CachedReply> {
        let &slot_idx = self.index.get(&client_id)?;
        self.slots[slot_idx].as_ref().map(|entry| &entry.reply)
    }

    /// Fence epoch for a registered client. This is the u64 the register
    /// reply hands the client and the wire `session` field carries back.
    #[must_use]
    pub fn get_epoch(&self, client_id: u128) -> Option<u64> {
        let &slot_idx = self.index.get(&client_id)?;
        self.slots[slot_idx].as_ref().map(|entry| entry.epoch)
    }

    /// Committed-request watermark for a registered client. A (re)bind reply
    /// surfaces this so a restarted client resumes numbering at
    /// `watermark + 1` instead of silently colliding below it.
    #[must_use]
    pub fn get_watermark(&self, client_id: u128) -> Option<u64> {
        let &slot_idx = self.index.get(&client_id)?;
        self.slots[slot_idx].as_ref().map(|entry| entry.watermark)
    }

    /// Acting user id captured when the client registered.
    #[must_use]
    pub fn get_user_id(&self, client_id: u128) -> Option<u32> {
        let &slot_idx = self.index.get(&client_id)?;
        self.slots[slot_idx].as_ref().map(|entry| entry.user_id)
    }

    /// Active committed entries.
    #[must_use]
    pub fn count(&self) -> usize {
        self.index.len()
    }
}

impl ClientEntry {
    /// Cached reply whose `request` matches, latest first then the ring
    /// (newest displaced entries sit at the back; scan order is irrelevant
    /// because request numbers in the ring are unique).
    fn find_cached(&self, request: u64) -> Option<&CachedReply> {
        if self.reply.header().request == request {
            return Some(&self.reply);
        }
        self.ring
            .iter()
            .find(|cached| cached.header().request == request)
    }

    /// Retain a displaced reply for below-watermark duplicates. Register
    /// replies never enter: `request == REGISTER_REQUEST_ID` can never match
    /// a `check_request` lookup (wire validation enforces `request > 0`).
    fn push_ring(&mut self, displaced: CachedReply) {
        if displaced.header().request == REGISTER_REQUEST_ID {
            return;
        }
        if self.ring.len() == REPLY_RING_CAPACITY {
            self.ring.pop_front();
        }
        self.ring.push_back(displaced);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_binary_protocol::{Command2, Operation};

    /// Arbitrary non-zero user id for register fixtures; most tests don't
    /// assert on it (see `register_stores_user_id` for the accessor check).
    const TEST_USER_ID: u32 = 7;

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
        make_reply_with_checksum(client, request, commit, 0)
    }

    fn make_reply_with_checksum(
        client: u128,
        request: u64,
        commit: u64,
        request_checksum: u128,
    ) -> Message<ReplyHeader> {
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
            request_checksum,
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

    /// Register client 1 (register commit stamped at op 10). Returns
    /// (table, epoch=1).
    fn table_with_client() -> (ClientTable, u64) {
        let mut table = ClientTable::new(10);
        table.commit_register(1, TEST_USER_ID, make_register_reply(1, 10), no_in_flight());
        (table, 1)
    }

    // Registration tests

    #[test]
    fn register_mints_epoch_one() {
        let mut table = ClientTable::new(10);
        table.commit_register(1, TEST_USER_ID, make_register_reply(1, 42), no_in_flight());
        assert_eq!(table.get_epoch(1), Some(1));
        assert_eq!(table.get_watermark(1), Some(0));
        assert_eq!(table.get_user_id(1), Some(TEST_USER_ID));
        assert_eq!(table.count(), 1);
    }

    // Re-register = rebind: epoch bumps, watermark (dedup history) survives.
    #[test]
    fn reregister_bumps_epoch_and_preserves_watermark() {
        let (mut table, epoch) = table_with_client();
        table.commit_reply(1, epoch, make_reply_for(1, 5, 15));
        assert_eq!(table.get_watermark(1), Some(5));

        table.commit_register(1, TEST_USER_ID, make_register_reply(1, 20), no_in_flight());
        assert_eq!(table.get_epoch(1), Some(2), "rebind mints the next epoch");
        assert_eq!(
            table.get_watermark(1),
            Some(5),
            "session resume keeps dedup history"
        );
        assert_eq!(table.count(), 1);

        // The displaced app reply moved into the ring: the watermark request
        // still answers with its original bytes under the new epoch.
        match table.check_request(1, 2, 5, 0) {
            RequestStatus::Duplicate(cached) => assert_eq!(cached.header().request, 5),
            other => panic!("expected Duplicate from ring, got {other:?}"),
        }
    }

    // A rebind re-authenticates; the fresh register's user wins.
    #[test]
    fn reregister_refreshes_user_id() {
        let mut table = ClientTable::new(10);
        table.commit_register(1, 11, make_register_reply(1, 10), no_in_flight());
        table.commit_register(1, 22, make_register_reply(1, 20), no_in_flight());
        assert_eq!(table.get_user_id(1), Some(22));
    }

    // Each entry keeps the user id it registered with; lookups are per-client.
    #[test]
    fn register_stores_user_id() {
        let mut table = ClientTable::new(10);
        table.commit_register(1, 11, make_register_reply(1, 10), no_in_flight());
        table.commit_register(2, 22, make_register_reply(2, 20), no_in_flight());
        assert_eq!(table.get_user_id(1), Some(11));
        assert_eq!(table.get_user_id(2), Some(22));
        assert_eq!(
            table.get_user_id(3),
            None,
            "unregistered client has no user"
        );
    }

    #[test]
    fn check_register_new_client() {
        let table = ClientTable::new(10);
        assert!(matches!(table.check_register(1), RequestStatus::New));
    }

    #[test]
    fn check_register_already_registered() {
        let (table, epoch) = table_with_client();
        match table.check_register(1) {
            RequestStatus::AlreadyRegistered {
                epoch: e,
                cached_reply,
            } => {
                assert_eq!(e, epoch);
                // Cached reply IS the register reply, preflight replays it.
                assert_eq!(cached_reply.header().request, REGISTER_REQUEST_ID);
            }
            other => panic!("expected AlreadyRegistered, got {other:?}"),
        }
    }

    #[test]
    fn check_register_already_registered_after_progress() {
        let (mut table, epoch) = table_with_client();
        // Client progresses past registration.
        table.commit_reply(1, epoch, make_reply_for(1, 1, 11));
        table.commit_reply(1, epoch, make_reply_for(1, 2, 12));
        // Cached reply is now latest app reply; preflight must silent-drop.
        match table.check_register(1) {
            RequestStatus::AlreadyRegistered {
                epoch: e,
                cached_reply,
            } => {
                assert_eq!(e, epoch);
                assert_eq!(
                    cached_reply.header().request,
                    2,
                    "cached reply must be the latest app reply, not the register reply"
                );
            }
            other => panic!("expected AlreadyRegistered, got {other:?}"),
        }
    }

    // Epoch fence tests

    #[test]
    fn check_request_no_session() {
        let table = ClientTable::new(10);
        // Not registered: valid epoch/request but no entry.
        assert!(matches!(
            table.check_request(1, 99, 1, 0),
            RequestStatus::NoSession
        ));
    }

    // Zombie fencing: requests stamped with a pre-rebind epoch are terminal.
    #[test]
    fn check_request_stale_epoch_is_fenced() {
        let (mut table, _) = table_with_client();
        table.commit_register(1, TEST_USER_ID, make_register_reply(1, 20), no_in_flight());
        assert_eq!(table.get_epoch(1), Some(2));
        match table.check_request(1, 1, 1, 0) {
            RequestStatus::Fenced { current, received } => {
                assert_eq!(current, 2);
                assert_eq!(received, 1);
            }
            other => panic!("expected Fenced, got {other:?}"),
        }
    }

    // Epochs are only handed out by register replies; a newer-than-minted
    // epoch is a client bug, distinct from the zombie case.
    #[test]
    fn check_request_future_epoch_is_client_bug() {
        let (table, epoch) = table_with_client();
        match table.check_request(1, epoch + 1, 1, 0) {
            RequestStatus::EpochAhead { current, received } => {
                assert_eq!(current, epoch);
                assert_eq!(received, epoch + 1);
            }
            other => panic!("expected EpochAhead, got {other:?}"),
        }
    }

    // Watermark tests

    #[test]
    fn check_request_above_watermark_is_new() {
        let (mut table, epoch) = table_with_client();
        table.commit_reply(1, epoch, make_reply_for(1, 1, 11));
        assert!(matches!(
            table.check_request(1, epoch, 2, 0),
            RequestStatus::New
        ));
    }

    // No contiguity requirement: a jump past the watermark executes. The
    // watermark records the highest committed request, not a sequence.
    #[test]
    fn check_request_jump_above_watermark_is_new() {
        let (mut table, epoch) = table_with_client();
        table.commit_reply(1, epoch, make_reply_for(1, 1, 11));
        assert!(matches!(
            table.check_request(1, epoch, 9, 0),
            RequestStatus::New
        ));
        // And committing the jump moves the watermark to it.
        table.commit_reply(1, epoch, make_reply_for(1, 9, 12));
        assert_eq!(table.get_watermark(1), Some(9));
    }

    #[test]
    fn check_request_duplicate_at_watermark() {
        let (mut table, epoch) = table_with_client();
        table.commit_reply(1, epoch, make_reply_for(1, 1, 11));
        match table.check_request(1, epoch, 1, 0) {
            RequestStatus::Duplicate(cached) => assert_eq!(cached.header().request, 1),
            other => panic!("expected Duplicate, got {other:?}"),
        }
    }

    // Below-watermark duplicate with the original still in the ring answers
    // with the original bytes.
    #[test]
    fn check_request_below_watermark_hits_ring() {
        let (mut table, epoch) = table_with_client();
        table.commit_reply(1, epoch, make_reply_for(1, 1, 11));
        table.commit_reply(1, epoch, make_reply_for(1, 2, 12));
        match table.check_request(1, epoch, 1, 0) {
            RequestStatus::Duplicate(cached) => {
                assert_eq!(cached.header().request, 1, "original reply, not latest");
                assert_eq!(cached.header().commit, 11, "original commit op");
            }
            other => panic!("expected Duplicate from ring, got {other:?}"),
        }
    }

    // Below-watermark duplicate whose reply aged out of the ring is refused
    // execution with nothing to replay.
    #[test]
    fn check_request_below_watermark_past_ring_is_already_applied() {
        let (mut table, epoch) = table_with_client();
        // Requests 1..=6: request 1's reply is displaced beyond the ring
        // (capacity 4 holds 2,3,4,5 once 6 is latest).
        for request in 1..=6u64 {
            table.commit_reply(1, epoch, make_reply_for(1, request, 10 + request));
        }
        match table.check_request(1, epoch, 1, 0) {
            RequestStatus::AlreadyApplied { request, watermark } => {
                assert_eq!(request, 1);
                assert_eq!(watermark, 6);
            }
            other => panic!("expected AlreadyApplied, got {other:?}"),
        }
        // The oldest retained entry still answers.
        match table.check_request(1, epoch, 2, 0) {
            RequestStatus::Duplicate(cached) => assert_eq!(cached.header().request, 2),
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
        let (mut table, epoch) = table_with_client();
        table.commit_reply(1, epoch, make_reply_for(1, 1, 11));

        match table.check_request(1, epoch, 1, 0) {
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

    // Checksum tests

    // Same request id, different request bytes: returning the cached reply
    // would answer the wrong request. Refused loudly.
    #[test]
    fn check_request_checksum_mismatch_at_watermark() {
        let (mut table, epoch) = table_with_client();
        table.commit_reply(1, epoch, make_reply_with_checksum(1, 1, 11, 0xAA));
        match table.check_request(1, epoch, 1, 0xBB) {
            RequestStatus::ChecksumMismatch { request } => assert_eq!(request, 1),
            other => panic!("expected ChecksumMismatch, got {other:?}"),
        }
        // Matching stamp replays.
        assert!(matches!(
            table.check_request(1, epoch, 1, 0xAA),
            RequestStatus::Duplicate(_)
        ));
    }

    // Integrity fields are zeroed on the wire today; a zero on either side
    // must not trip the mismatch (rollout compatibility).
    #[test]
    fn check_request_zero_checksum_disables_comparison() {
        let (mut table, epoch) = table_with_client();
        table.commit_reply(1, epoch, make_reply_with_checksum(1, 1, 11, 0xAA));
        assert!(matches!(
            table.check_request(1, epoch, 1, 0),
            RequestStatus::Duplicate(_)
        ));

        table.commit_reply(1, epoch, make_reply_for(1, 2, 12)); // stored zero
        assert!(matches!(
            table.check_request(1, epoch, 2, 0xBB),
            RequestStatus::Duplicate(_)
        ));
    }

    // Commit tests

    #[test]
    fn commit_caches_reply() {
        let (mut table, epoch) = table_with_client();
        table.commit_reply(1, epoch, make_reply_for(1, 1, 11));
        let cached = table.get_reply(1).expect("should have cached reply");
        assert_eq!(cached.header().request, 1);
    }

    #[test]
    fn commit_updates_preserves_epoch() {
        let (mut table, epoch) = table_with_client();
        table.commit_reply(1, epoch, make_reply_for(1, 1, 11));
        table.commit_reply(1, epoch, make_reply_for(1, 2, 12));
        assert_eq!(table.get_reply(1).unwrap().header().request, 2);
        assert_eq!(table.get_epoch(1), Some(epoch));
        assert_eq!(table.count(), 1);
    }

    // Same request re-committed (WAL replay shape): replace in place, no
    // ring push - two cached replies for one request number would make
    // duplicate lookups ambiguous.
    #[test]
    fn commit_reply_same_request_replaces_in_place() {
        let (mut table, epoch) = table_with_client();
        table.commit_reply(1, epoch, make_reply_for(1, 1, 11));
        table.commit_reply(1, epoch, make_reply_for(1, 1, 11));
        assert_eq!(table.get_watermark(1), Some(1));
        match table.check_request(1, epoch, 1, 0) {
            RequestStatus::Duplicate(cached) => assert_eq!(cached.header().request, 1),
            other => panic!("expected Duplicate, got {other:?}"),
        }
    }

    // Eviction tests

    #[test]
    fn eviction_removes_oldest_commit() {
        let mut table = ClientTable::new(2);
        table.commit_register(
            100,
            TEST_USER_ID,
            make_register_reply(100, 10),
            no_in_flight(),
        );
        table.commit_register(
            200,
            TEST_USER_ID,
            make_register_reply(200, 20),
            no_in_flight(),
        );
        table.commit_register(
            300,
            TEST_USER_ID,
            make_register_reply(300, 30),
            no_in_flight(),
        );
        assert!(table.get_reply(100).is_none());
        assert!(table.get_reply(200).is_some());
        assert!(table.get_reply(300).is_some());
        assert_eq!(table.count(), 2);
    }

    #[test]
    fn eviction_is_deterministic_by_slot_index() {
        let mut table = ClientTable::new(2);
        table.commit_register(
            100,
            TEST_USER_ID,
            make_register_reply(100, 10),
            no_in_flight(),
        );
        table.commit_register(
            200,
            TEST_USER_ID,
            make_register_reply(200, 10),
            no_in_flight(),
        );
        table.commit_register(
            300,
            TEST_USER_ID,
            make_register_reply(300, 30),
            no_in_flight(),
        );
        assert!(table.get_reply(100).is_none());
        assert!(table.get_reply(200).is_some());
        assert!(table.get_reply(300).is_some());
    }

    #[test]
    fn slot_reuse_after_eviction() {
        let mut table = ClientTable::new(1);
        table.commit_register(
            100,
            TEST_USER_ID,
            make_register_reply(100, 10),
            no_in_flight(),
        );
        table.commit_register(
            200,
            TEST_USER_ID,
            make_register_reply(200, 20),
            no_in_flight(),
        );
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
        table.commit_register(
            100,
            TEST_USER_ID,
            make_register_reply(100, 10),
            no_in_flight(),
        );
        table.commit_register(
            200,
            TEST_USER_ID,
            make_register_reply(200, 20),
            no_in_flight(),
        );
        // 100 in-flight; eviction must pick 200.
        let in_flight = |c: u128| c == 100;
        table.commit_register(300, TEST_USER_ID, make_register_reply(300, 30), in_flight);
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
        table.commit_register(
            100,
            TEST_USER_ID,
            make_register_reply(100, 10),
            no_in_flight(),
        );
        table.commit_register(
            200,
            TEST_USER_ID,
            make_register_reply(200, 20),
            no_in_flight(),
        );
        let all_in_flight = |_| true;
        table.commit_register(
            300,
            TEST_USER_ID,
            make_register_reply(300, 30),
            all_in_flight,
        );
        assert!(
            table.get_reply(100).is_none(),
            "100 evicted (oldest fallback)"
        );
        assert!(table.get_reply(200).is_some());
        assert!(table.get_reply(300).is_some());
    }

    // Edge cases

    // commit_reply for unregistered/evicted client must not panic;
    // wire reply still ships, cache silently skipped.
    #[test]
    fn commit_reply_for_unregistered_client_is_noop() {
        let mut table = ClientTable::new(10);
        // No register: index has no entry.
        table.commit_reply(1, 1, make_reply_for(1, 1, 10));
        assert!(table.get_reply(1).is_none(), "no entry must be created");
        assert_eq!(table.count(), 0);
    }

    #[test]
    #[should_panic(expected = "epoch mismatch")]
    fn commit_reply_wrong_epoch_panics() {
        let (mut table, _epoch) = table_with_client();
        // Entry epoch=1, commit claims epoch=99.
        table.commit_reply(1, 99, make_reply_for(1, 1, 11));
    }

    #[test]
    #[should_panic(expected = "watermark regression")]
    fn commit_reply_watermark_regression_panics() {
        let (mut table, epoch) = table_with_client();
        table.commit_reply(1, epoch, make_reply_for(1, 5, 15));
        table.commit_reply(1, epoch, make_reply_for(1, 3, 16));
    }

    #[test]
    fn different_clients_independent_epochs() {
        let mut table = ClientTable::new(10);
        table.commit_register(1, TEST_USER_ID, make_register_reply(1, 10), no_in_flight());
        table.commit_register(2, TEST_USER_ID, make_register_reply(2, 20), no_in_flight());
        // Rebind client 2 only.
        table.commit_register(2, TEST_USER_ID, make_register_reply(2, 30), no_in_flight());
        assert_eq!(table.get_epoch(1), Some(1));
        assert_eq!(table.get_epoch(2), Some(2));
        assert!(matches!(
            table.check_request(1, 1, 1, 0),
            RequestStatus::New
        ));
        assert!(matches!(
            table.check_request(2, 2, 1, 0),
            RequestStatus::New
        ));
        assert!(matches!(
            table.check_request(2, 1, 1, 0),
            RequestStatus::Fenced { .. }
        ));
    }
}
