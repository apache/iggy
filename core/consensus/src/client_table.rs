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

/// Committed replies retained per entry, newest at the back.
///
/// The back is the latest committed reply and is structurally safe:
/// eviction pops the front, and only pushing a newer reply triggers it.
/// The SDK enforces one request in flight per session, so the only reply a
/// live client can be waiting for is its latest (`request == watermark`).
/// Older entries answer old retransmits and post-rebind stragglers with the
/// original bytes instead of a bare "already applied"; losing one
/// degrades the answer, never correctness. In-memory only: ring contents are
/// refcount bumps and are never persisted or transferred.
const REPLY_RING_CAPACITY: usize = 5;

/// Per-session entry: fence epoch + committed-request watermark + replies.
///
/// The key (`client_id` today, the stable `session_id` once SDK identity
/// stability lands) is client-supplied; `epoch` is the server-minted fence
/// that orders rebinds of that key.
#[derive(Debug)]
struct ClientEntry {
    /// Fence epoch: the commit op of the latest committed register for this
    /// key (see [`ClientTable::commit_register`]). Monotonic across the whole
    /// log, so it never regresses even across entry drop + re-register.
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
    /// Committed replies, oldest at front, latest at back; never empty
    /// (registration seeds the register reply). Bounded by
    /// [`REPLY_RING_CAPACITY`]. Request numbers are unique: same-request
    /// recommits replace in place, and a rebind drops the previous
    /// register reply before pushing the new one.
    ring: VecDeque<CachedReply>,
    /// Owning client id and the commit op of the latest cached reply,
    /// denormalized out of `ring.back()`'s header. Purely to keep
    /// [`ClientTable::evict_oldest`] off the header-cast path: it runs inside
    /// shard 0's no-await commit region and scans every slot, so two
    /// `bytemuck` casts per occupied slot per eviction is real work on the
    /// commit loop. Maintained wherever `ring` is pushed.
    client_id: u128,
    latest_commit: u64,
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
}

/// What [`ClientTable::commit_reply`] did. Diagnostics only: the reply is
/// shipped to the client either way, so a non-`Cached` outcome degrades dedup
/// for one entry rather than failing the commit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitReply {
    /// Reply cached and the watermark advanced (or refreshed in place).
    Cached,
    /// No entry for this client (evicted between prepare and commit).
    NoEntry,
    /// The committed op is older than what the entry already holds, which
    /// replica-local eviction makes reachable on a replay. Skipped.
    SkippedRegression { stored: u64, received: u64 },
}

/// VSR client table: per-session fence epoch + request-watermark dedup.
///
/// Fixed-size slot array (source of truth) + `HashMap` index (O(1) lookup).
///
/// ## Semantics (v2)
///
/// - **Client-supplied key, op-derived fence.** Session identity is the
///   client-supplied key; the entry's `epoch` is the commit op of its latest
///   committed register (`TigerBeetle`'s "the commit number becomes the session
///   number"). Register only commits in the metadata group, so the fence has
///   one minting authority and stays comparable in any consensus group's
///   slice; being log-derived it never regresses, even across entry drop and
///   re-create.
/// - **Watermark, not contiguity.** A request above the watermark executes
///   (gaps allowed); at or below is a duplicate. There is no `RequestGap`:
///   a client that jumps its counter loses nothing but the skipped ids.
/// - **Replies are volatile.** A small per-entry ring of recent replies
///   (latest at the back), all in-memory refcounts. A duplicate whose reply
///   aged out is still refused execution ([`RequestStatus::AlreadyApplied`]).
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

/// Whether two integrity stamps for the same request number disagree.
///
/// Zero means unstamped (the wire integrity fields are zeroed today), and an
/// unstamped side carries no evidence either way, so it never conflicts.
const fn checksums_conflict(stored: u128, received: u128) -> bool {
    stored != 0 && received != 0 && stored != received
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

    /// Resize the table to `max_clients` slots. Boot-only: reallocating a
    /// populated table would silently drop live sessions, so this must run
    /// before any client registers (server-ng bootstrap applies the configured
    /// `[metadata] clients_table_max` here).
    ///
    /// # Panics
    /// If the table already holds a client.
    pub fn set_capacity(&mut self, max_clients: usize) {
        assert!(
            self.index.is_empty(),
            "set_capacity must run before any client registers"
        );
        *self = Self::new(max_clients);
    }

    /// Check a request against the table. Epoch fence first, then the
    /// watermark. Register does not come through here: every bind proposes
    /// unconditionally so its fence actually moves, see
    /// [`Self::commit_register`].
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

        // Watermark first: it is checked even when its reply has aged out of
        // the ring, which is the only request for which no cached header
        // survives to compare against.
        if request == entry.watermark
            && checksums_conflict(entry.watermark_checksum, request_checksum)
        {
            return RequestStatus::ChecksumMismatch { request };
        }

        match entry.find_cached(request) {
            // Every cached reply carries the checksum of the request it
            // answered, so the reuse check covers the whole ring rather than
            // the watermark alone.
            Some(cached)
                if checksums_conflict(cached.header().request_checksum, request_checksum) =>
            {
                RequestStatus::ChecksumMismatch { request }
            }
            Some(cached) => RequestStatus::Duplicate(cached.clone()),
            None => RequestStatus::AlreadyApplied {
                request,
                watermark: entry.watermark,
            },
        }
    }

    /// Record a committed register: create the entry, or rebind the existing
    /// one. Either way the entry's epoch becomes the register's commit op
    /// (`reply.header().commit`, which `build_reply_message` stamps from the
    /// prepare's op).
    ///
    /// Deriving the fence from the op gives it `TigerBeetle`'s property ("the
    /// commit number becomes the session number"): it is deterministic in
    /// apply order, strictly higher on every rebind, and -- unlike a per-entry
    /// counter -- it never regresses when an entry is dropped and re-created,
    /// so a zombie from before a capacity eviction can always be fenced.
    /// Register only commits in the metadata group, so there is exactly one
    /// minting authority and the value compares across planes.
    ///
    /// A rebind refreshes `user_id` (the bind re-authenticated), pushes the
    /// register reply as the latest (cached app replies stay put; the
    /// previous register reply is dropped), and preserves the watermark -
    /// session resume keeps dedup history.
    ///
    /// Full table evicts the oldest commit, see [`Self::evict_oldest`].
    ///
    /// # Panics
    /// If `client_id == 0` or `client_id != reply.header().client`.
    pub fn commit_register(&mut self, client_id: u128, user_id: u32, reply: Message<ReplyHeader>) {
        assert!(client_id != 0, "client_id 0 is reserved for internal use");
        assert_eq!(
            client_id,
            reply.header().client,
            "commit_register: client_id mismatch (arg={client_id}, header={})",
            reply.header().client
        );
        let epoch = reply.header().commit;

        // Freeze once; later dedup-hit clones Arc-bump.
        let cached: CachedReply = CachedReply::from_message(reply);

        if let Some(&slot_idx) = self.index.get(&client_id) {
            let entry = self.slots[slot_idx].as_mut().expect("index/slot mismatch");
            // Commits apply in log order on every replica, so a rebind's op is
            // strictly above the entry's current fence.
            debug_assert!(
                epoch > entry.epoch,
                "commit_register: rebind epoch regression ({} -> {epoch})",
                entry.epoch
            );
            entry.epoch = epoch;
            entry.user_id = user_id;
            // Drop the previous register reply (if still retained) before
            // pushing the new one: only the newest rebind's reply is
            // replayable, and two request-0 entries would break the ring's
            // unique-request invariant.
            entry
                .ring
                .retain(|stored| stored.header().request != REGISTER_REQUEST_ID);
            entry.push_latest(cached);
        } else {
            let freed = if self.index.len() >= self.slots.len() {
                self.evict_oldest()
            } else {
                None
            };
            let slot_idx = freed
                .or_else(|| self.first_free_slot())
                .expect("eviction must free a slot");
            let latest_commit = cached.header().commit;
            let mut ring = VecDeque::with_capacity(REPLY_RING_CAPACITY);
            ring.push_back(cached);
            self.slots[slot_idx] = Some(ClientEntry {
                epoch,
                user_id,
                client_id,
                latest_commit,
                watermark: REGISTER_REQUEST_ID,
                watermark_checksum: 0,
                ring,
            });
            self.index.insert(client_id, slot_idx);
        }
    }

    /// Record a committed reply: advance the watermark, push the reply into
    /// the ring (evicting the oldest when full).
    ///
    /// Reply delivery is caller's job, `Sender` lives on the popped
    /// `PipelineEntry` ([`crate::PipelineEntry::take_reply_sender`]),
    /// fired AFTER this returns (slot-first ordering).
    ///
    /// Best-effort by design: the wire reply ships regardless, so anything
    /// that makes this entry uncacheable is reported as a [`CommitReply`]
    /// variant rather than faulting the commit. Two such cases exist, both
    /// downstream of replica-local eviction (capacity pressure and transport
    /// disconnect are not replicated, so replicas disagree on which sessions
    /// exist): a missing entry, and a committed request older than the stored
    /// watermark. Panicking on either would take down a replica for a state
    /// difference that is expected.
    ///
    /// # Panics
    /// If `client_id == 0` or `client_id != reply.header().client`. Neither is
    /// reachable from a well-formed reply, both indicate a caller bug.
    pub fn commit_reply(&mut self, client_id: u128, reply: Message<ReplyHeader>) -> CommitReply {
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
                new_request, "commit_reply: client evicted while being prepared, skipping cache"
            );
            return CommitReply::NoEntry;
        };

        let entry = self.slots[slot_idx].as_mut().expect("index/slot mismatch");
        // Regression checks are SKIPS, never panics. Both are reachable from
        // the apply path without any local bug: capacity eviction is
        // replica-local and unlogged, so a WAL shaped
        // `Register(X), app(X,req=5), [evict], Register(X), app(X,req<5)`
        // replays on a node that did not evict into a rebind that preserves
        // watermark 5, and then commits a lower request. Panicking there
        // takes down a backup's shard pump (or refuses to boot from an
        // otherwise-intact WAL) over cache bookkeeping that is best-effort
        // by design. `client_id` is caller-supplied on the wire, so this is
        // reachable by untrusted input; a skip degrades dedup for that one
        // entry and nothing else.
        if new_commit < entry.latest_commit {
            return CommitReply::SkippedRegression {
                stored: entry.latest_commit,
                received: new_commit,
            };
        }
        if new_request < entry.watermark {
            return CommitReply::SkippedRegression {
                stored: entry.watermark,
                received: new_request,
            };
        }

        // Freeze once; later dedup-hit clones Arc-bump.
        let cached = CachedReply::from_message(reply);
        if new_request == entry.watermark {
            // Same request re-committed (WAL replay shape): replace in
            // place, never push a stale twin - two cached replies for one
            // request number would make lookups ambiguous.
            if let Some(stored) = entry
                .ring
                .iter_mut()
                .find(|stored| stored.header().request == new_request)
            {
                *stored = cached;
                // The watermark's reply is the ring's back, so replacing it in
                // place moves the latest commit without a push.
                entry.latest_commit = new_commit;
            } else {
                entry.push_latest(cached);
            }
        } else {
            entry.push_latest(cached);
            entry.watermark = new_request;
        }
        entry.watermark_checksum = new_checksum;
        CommitReply::Cached
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

    /// Evict the client whose latest cached reply has the oldest commit.
    ///
    /// Deterministic: fixed-array iteration, ties broken by lowest slot index.
    /// Every replica with the same committed state evicts the same client,
    /// which is the whole requirement -- this runs inside the deterministic
    /// apply path, so any input outside the agreed log would diverge the
    /// table. In particular the victim choice must NOT consult pipeline
    /// state: only the primary pipelines client requests, so a
    /// `has_message_from_client` ranking would make the primary spare a
    /// session that every backup drops.
    ///
    /// A client with an uncommitted prepare is therefore evictable. Its
    /// commit lands as [`CommitReply::NoEntry`] -- the reply still ships, and
    /// the client learns the session is gone on its next request (`NoSession`
    /// -> eviction frame -> re-register).
    ///
    /// **Caveat**: eviction erases the evicted session's watermark, so its
    /// next retry is treated as `New` (re-executes). Bounded by table
    /// capacity; the op-TTL + slice persistence work (IGGY-137) shrinks it.
    ///
    /// Returns the freed slot index so the caller can fill it without a second
    /// walk over the array.
    fn evict_oldest(&mut self) -> Option<usize> {
        let mut evictee: Option<(usize, u64)> = None; // (slot_idx, commit)

        for (idx, slot) in self.slots.iter().enumerate() {
            let Some(entry) = slot else { continue };
            let should_pick = match evictee {
                None => true,
                Some((_, min_commit)) => entry.latest_commit < min_commit,
            };
            if should_pick {
                evictee = Some((idx, entry.latest_commit));
            }
        }

        let (slot_idx, _) = evictee?;
        let entry = self.slots[slot_idx].take().expect("evictee must exist");
        self.index.remove(&entry.client_id);
        trace!(
            client_id = entry.client_id,
            "evict_oldest: removed client from session table"
        );
        Some(slot_idx)
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
        self.slots[slot_idx].as_ref().map(ClientEntry::latest)
    }

    /// Fence epoch for a registered client. This is the u64 the register
    /// reply hands the client and the wire `session` field carries back.
    #[must_use]
    pub fn get_epoch(&self, client_id: u128) -> Option<u64> {
        let &slot_idx = self.index.get(&client_id)?;
        self.slots[slot_idx].as_ref().map(|entry| entry.epoch)
    }

    /// Every registered client id, in slot order.
    ///
    /// Boot-time only: the id minter reseeds above the highest recovered
    /// sequence so a post-restart mint cannot land on a recovered entry.
    pub fn client_ids(&self) -> impl Iterator<Item = u128> + '_ {
        self.slots
            .iter()
            .filter_map(|slot| slot.as_ref().map(|entry| entry.client_id))
    }

    /// Committed-request watermark for a registered client.
    ///
    /// NOT yet surfaced to clients: `LoginRegisterResponse` carries only
    /// `{user_id, session, server_protocol_version, server_version}` and
    /// `ReplyHeader.context` is hardcoded `0`, so there is no channel for it.
    /// Until one exists, a client that restarts and resumes numbering from
    /// below this value has those requests answered as duplicates
    /// ([`RequestStatus::Duplicate`] / [`RequestStatus::AlreadyApplied`])
    /// rather than executed. Returning it on (re)bind is the missing half of
    /// SDK-side resume; used by tests and recovery assertions today.
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
    /// Latest committed reply (register or app op).
    ///
    /// # Panics
    /// Unreachable: registration seeds the ring and pops happen only when
    /// displaced by a newer push.
    fn latest(&self) -> &CachedReply {
        self.ring
            .back()
            .expect("ring is never empty after registration")
    }

    /// Cached reply whose `request` matches (scan order is irrelevant
    /// because request numbers in the ring are unique).
    fn find_cached(&self, request: u64) -> Option<&CachedReply> {
        self.ring
            .iter()
            .find(|cached| cached.header().request == request)
    }

    /// Push the newest committed reply, evicting the oldest when full, and
    /// refresh the denormalized `latest_commit`.
    fn push_latest(&mut self, cached: CachedReply) {
        self.latest_commit = cached.header().commit;
        if self.ring.len() == REPLY_RING_CAPACITY {
            self.ring.pop_front();
        }
        self.ring.push_back(cached);
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

    /// Register client 1 (register commit stamped at op 10). Returns
    /// (table, epoch=1).
    fn table_with_client() -> (ClientTable, u64) {
        let mut table = ClientTable::new(10);
        table.commit_register(1, TEST_USER_ID, make_register_reply(1, 10));
        let epoch = table.get_epoch(1).expect("just registered");
        (table, epoch)
    }

    // Registration tests

    #[test]
    fn register_epoch_is_the_register_commit_op() {
        let mut table = ClientTable::new(10);
        table.commit_register(1, TEST_USER_ID, make_register_reply(1, 42));
        assert_eq!(table.get_epoch(1), Some(42));
        assert_eq!(table.get_watermark(1), Some(0));
        assert_eq!(table.get_user_id(1), Some(TEST_USER_ID));
        assert_eq!(table.count(), 1);
    }

    // Re-register = rebind: epoch bumps, watermark (dedup history) survives.
    #[test]
    fn reregister_bumps_epoch_and_preserves_watermark() {
        let (mut table, _epoch) = table_with_client();
        table.commit_reply(1, make_reply_for(1, 5, 15));
        assert_eq!(table.get_watermark(1), Some(5));

        table.commit_register(1, TEST_USER_ID, make_register_reply(1, 20));
        assert_eq!(
            table.get_epoch(1),
            Some(20),
            "rebind moves the fence to the new register's op"
        );
        assert_eq!(
            table.get_watermark(1),
            Some(5),
            "session resume keeps dedup history"
        );
        assert_eq!(table.count(), 1);

        // The app reply stays cached across the rebind: the watermark
        // request still answers with its original bytes under the new epoch.
        match table.check_request(1, 20, 5, 0) {
            RequestStatus::Duplicate(cached) => assert_eq!(cached.header().request, 5),
            other => panic!("expected Duplicate from ring, got {other:?}"),
        }
    }

    // A rebind re-authenticates; the fresh register's user wins.
    #[test]
    fn reregister_refreshes_user_id() {
        let mut table = ClientTable::new(10);
        table.commit_register(1, 11, make_register_reply(1, 10));
        table.commit_register(1, 22, make_register_reply(1, 20));
        assert_eq!(table.get_user_id(1), Some(22));
    }

    // Each entry keeps the user id it registered with; lookups are per-client.
    #[test]
    fn register_stores_user_id() {
        let mut table = ClientTable::new(10);
        table.commit_register(1, 11, make_register_reply(1, 10));
        table.commit_register(2, 22, make_register_reply(2, 20));
        assert_eq!(table.get_user_id(1), Some(11));
        assert_eq!(table.get_user_id(2), Some(22));
        assert_eq!(
            table.get_user_id(3),
            None,
            "unregistered client has no user"
        );
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
        let (mut table, first_epoch) = table_with_client();
        table.commit_register(1, TEST_USER_ID, make_register_reply(1, 20));
        assert_eq!(table.get_epoch(1), Some(20));
        match table.check_request(1, first_epoch, 1, 0) {
            RequestStatus::Fenced { current, received } => {
                assert_eq!(current, 20);
                assert_eq!(received, first_epoch);
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
        table.commit_reply(1, make_reply_for(1, 1, 11));
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
        table.commit_reply(1, make_reply_for(1, 1, 11));
        assert!(matches!(
            table.check_request(1, epoch, 9, 0),
            RequestStatus::New
        ));
        // And committing the jump moves the watermark to it.
        table.commit_reply(1, make_reply_for(1, 9, 12));
        assert_eq!(table.get_watermark(1), Some(9));
    }

    #[test]
    fn check_request_duplicate_at_watermark() {
        let (mut table, epoch) = table_with_client();
        table.commit_reply(1, make_reply_for(1, 1, 11));
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
        table.commit_reply(1, make_reply_for(1, 1, 11));
        table.commit_reply(1, make_reply_for(1, 2, 12));
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
        // (capacity 5 holds 2..=6 once 6 commits; the register reply and
        // request 1 aged out first).
        for request in 1..=6u64 {
            table.commit_reply(1, make_reply_for(1, request, 10 + request));
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
        table.commit_reply(1, make_reply_for(1, 1, 11));

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
        table.commit_reply(1, make_reply_with_checksum(1, 1, 11, 0xAA));
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
        table.commit_reply(1, make_reply_with_checksum(1, 1, 11, 0xAA));
        assert!(matches!(
            table.check_request(1, epoch, 1, 0),
            RequestStatus::Duplicate(_)
        ));

        table.commit_reply(1, make_reply_for(1, 2, 12)); // stored zero
        assert!(matches!(
            table.check_request(1, epoch, 2, 0xBB),
            RequestStatus::Duplicate(_)
        ));
    }

    // Every ring entry carries the checksum of the request it answered, so id
    // reuse is caught below the watermark too, not only at it.
    #[test]
    fn check_request_detects_reuse_below_watermark() {
        let (mut table, epoch) = table_with_client();
        table.commit_reply(1, make_reply_with_checksum(1, 1, 11, 0xAA));
        table.commit_reply(1, make_reply_with_checksum(1, 2, 12, 0xBB));
        assert!(matches!(
            table.check_request(1, epoch, 1, 0xAA),
            RequestStatus::Duplicate(_)
        ));
        assert!(matches!(
            table.check_request(1, epoch, 1, 0xCC),
            RequestStatus::ChecksumMismatch { request: 1 }
        ));
    }

    // Commit tests

    #[test]
    fn commit_caches_reply() {
        let (mut table, _epoch) = table_with_client();
        table.commit_reply(1, make_reply_for(1, 1, 11));
        let cached = table.get_reply(1).expect("should have cached reply");
        assert_eq!(cached.header().request, 1);
    }

    #[test]
    fn commit_updates_preserves_epoch() {
        let (mut table, epoch) = table_with_client();
        table.commit_reply(1, make_reply_for(1, 1, 11));
        table.commit_reply(1, make_reply_for(1, 2, 12));
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
        table.commit_reply(1, make_reply_for(1, 1, 11));
        table.commit_reply(1, make_reply_for(1, 1, 11));
        assert_eq!(table.get_watermark(1), Some(1));
        match table.check_request(1, epoch, 1, 0) {
            RequestStatus::Duplicate(cached) => assert_eq!(cached.header().request, 1),
            other => panic!("expected Duplicate, got {other:?}"),
        }
    }

    // `latest_commit` is denormalized out of the ring's back to keep eviction
    // off the header-cast path, so every commit path must maintain it --
    // including the in-place replace, which bypasses `push_latest`. A stale
    // value would rank this entry for eviction by an old commit.
    #[test]
    fn in_place_replace_keeps_eviction_ranking_current() {
        let mut table = ClientTable::new(2);
        table.commit_register(1, TEST_USER_ID, make_register_reply(1, 10));
        table.commit_register(2, TEST_USER_ID, make_register_reply(2, 20));
        // Client 1 commits request 1, then the same request re-commits at a
        // higher op (the WAL-replay shape) via the in-place arm.
        table.commit_reply(1, make_reply_for(1, 1, 30));
        table.commit_reply(1, make_reply_for(1, 1, 40));

        // Client 2 is now the oldest (20 < 40) and must be the victim.
        table.commit_register(3, TEST_USER_ID, make_register_reply(3, 50));
        assert!(
            table.get_reply(1).is_some(),
            "client 1's refreshed commit must protect it from eviction"
        );
        assert!(table.get_reply(2).is_none(), "client 2 was the oldest");
        assert!(table.get_reply(3).is_some());
    }

    // Eviction tests

    #[test]
    fn eviction_removes_oldest_commit() {
        let mut table = ClientTable::new(2);
        table.commit_register(100, TEST_USER_ID, make_register_reply(100, 10));
        table.commit_register(200, TEST_USER_ID, make_register_reply(200, 20));
        table.commit_register(300, TEST_USER_ID, make_register_reply(300, 30));
        assert!(table.get_reply(100).is_none());
        assert!(table.get_reply(200).is_some());
        assert!(table.get_reply(300).is_some());
        assert_eq!(table.count(), 2);
    }

    #[test]
    fn eviction_is_deterministic_by_slot_index() {
        let mut table = ClientTable::new(2);
        table.commit_register(100, TEST_USER_ID, make_register_reply(100, 10));
        table.commit_register(200, TEST_USER_ID, make_register_reply(200, 10));
        table.commit_register(300, TEST_USER_ID, make_register_reply(300, 30));
        assert!(table.get_reply(100).is_none());
        assert!(table.get_reply(200).is_some());
        assert!(table.get_reply(300).is_some());
    }

    #[test]
    fn slot_reuse_after_eviction() {
        let mut table = ClientTable::new(1);
        table.commit_register(100, TEST_USER_ID, make_register_reply(100, 10));
        table.commit_register(200, TEST_USER_ID, make_register_reply(200, 20));
        assert!(table.get_reply(100).is_none());
        assert!(table.get_reply(200).is_some());
        assert_eq!(table.count(), 1);
    }

    // Victim choice depends only on committed state, so replicas that agree
    // on the log agree on the victim regardless of local pipeline contents.
    #[test]
    fn eviction_ignores_local_state_and_picks_oldest_commit() {
        let mut table = ClientTable::new(2);
        table.commit_register(100, TEST_USER_ID, make_register_reply(100, 10));
        table.commit_register(200, TEST_USER_ID, make_register_reply(200, 20));
        // A prepare in flight for 100 (primary-only state) must not spare it.
        table.commit_register(300, TEST_USER_ID, make_register_reply(300, 30));
        assert!(
            table.get_reply(100).is_none(),
            "oldest commit is evicted even with a local prepare outstanding"
        );
        assert!(table.get_reply(200).is_some());
        assert!(table.get_reply(300).is_some());
    }

    // Capacity resize (boot-only)

    // Resizing an empty table swaps its slot count in: a smaller cap then
    // evicts once the new bound is reached.
    #[test]
    fn set_capacity_resizes_empty_table() {
        let mut table = ClientTable::new(10);
        table.set_capacity(2);
        table.commit_register(100, TEST_USER_ID, make_register_reply(100, 10));
        table.commit_register(200, TEST_USER_ID, make_register_reply(200, 20));
        table.commit_register(300, TEST_USER_ID, make_register_reply(300, 30));
        assert_eq!(table.count(), 2, "resized cap of 2 evicts the oldest");
        assert!(table.get_reply(100).is_none());
    }

    // The empty-table contract is asserted, not silently honored: resizing a
    // populated table would drop live sessions, so it must panic.
    #[test]
    #[should_panic(expected = "before any client registers")]
    fn set_capacity_rejects_a_populated_table() {
        let (mut table, _session) = table_with_client();
        table.set_capacity(2);
    }

    // Evicting a client mid-prepare is safe: the commit reports NoEntry
    // instead of faulting, and no entry is resurrected.
    #[test]
    fn commit_reply_after_eviction_reports_no_entry() {
        let mut table = ClientTable::new(1);
        table.commit_register(100, TEST_USER_ID, make_register_reply(100, 10));
        table.commit_register(200, TEST_USER_ID, make_register_reply(200, 20));
        let outcome = table.commit_reply(100, make_reply_for(100, 1, 21));
        assert_eq!(outcome, CommitReply::NoEntry);
        assert_eq!(table.count(), 1);
    }

    // Edge cases

    // commit_reply for unregistered/evicted client must not panic;
    // wire reply still ships, cache silently skipped.
    #[test]
    fn commit_reply_for_unregistered_client_is_noop() {
        let mut table = ClientTable::new(10);
        // No register: index has no entry.
        let outcome = table.commit_reply(1, make_reply_for(1, 1, 10));
        assert_eq!(outcome, CommitReply::NoEntry);
        assert!(table.get_reply(1).is_none(), "no entry must be created");
        assert_eq!(table.count(), 0);
    }

    #[test]
    fn commit_reply_watermark_regression_is_skipped() {
        let (mut table, _epoch) = table_with_client();
        assert_eq!(
            table.commit_reply(1, make_reply_for(1, 5, 15)),
            CommitReply::Cached
        );
        assert_eq!(
            table.commit_reply(1, make_reply_for(1, 3, 16)),
            CommitReply::SkippedRegression {
                stored: 5,
                received: 3
            }
        );
        // Watermark holds at the newer request, cache keeps the newer reply.
        assert_eq!(table.get_watermark(1), Some(5));
        assert_eq!(
            table.get_reply(1).map(|reply| reply.header().commit),
            Some(15)
        );
    }

    #[test]
    fn different_clients_independent_epochs() {
        let mut table = ClientTable::new(10);
        table.commit_register(1, TEST_USER_ID, make_register_reply(1, 10));
        table.commit_register(2, TEST_USER_ID, make_register_reply(2, 20));
        // Rebind client 2 only.
        table.commit_register(2, TEST_USER_ID, make_register_reply(2, 30));
        assert_eq!(table.get_epoch(1), Some(10));
        assert_eq!(table.get_epoch(2), Some(30));
        assert!(matches!(
            table.check_request(1, 10, 1, 0),
            RequestStatus::New
        ));
        assert!(matches!(
            table.check_request(2, 30, 1, 0),
            RequestStatus::New
        ));
        // Client 2's pre-rebind epoch is a fenced zombie.
        assert!(matches!(
            table.check_request(2, 20, 1, 0),
            RequestStatus::Fenced { .. }
        ));
    }
}
