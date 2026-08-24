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

use crate::le_cursor::{LeCursor, Truncated, split_verified_trailer};
use iggy_binary_protocol::consensus::ConsensusError;
use iggy_binary_protocol::{GenericHeader, ReplyHeader};
use serde::{Deserialize, Serialize};
use server_common::{
    MESSAGE_ALIGN, Message,
    iobuf::{Frozen, Owned},
};
use std::collections::{HashMap, VecDeque};
use std::fmt;
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

    /// Raw reply bytes for checkpoint serialization, round-tripped through
    /// [`Self::from_message`] on decode.
    fn as_bytes(&self) -> &[u8] {
        self.bytes.as_slice()
    }
}

/// Reserved request number for [`Operation::Register`](iggy_binary_protocol::Operation::Register).
/// Real requests start at 1 (header validation enforces `request > 0`).
pub const REGISTER_REQUEST_ID: u64 = 0;

/// Exclusive ceiling on a checkpointed slot index.
///
/// Bounds the table [`ClientTable::from_snapshot`] allocates from an index it read off
/// disk. Mirrors the config's `MAX_METADATA_CLIENTS_TABLE_MAX`, the largest capacity an
/// operator can configure, so no valid checkpoint can carry an index at or above it.
pub const CLIENTS_TABLE_SLOT_MAX: usize = 1 << 16;

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
pub const REPLY_RING_CAPACITY: usize = 5;

/// What capacity eviction keeps after reclaiming an entry's replies.
///
/// At-most-once needs only the fence: the watermark says which request numbers
/// already committed, and the ring merely supplies the bytes to replay. Dropping
/// the whole entry made an evicted client's resume mint at watermark zero, so the
/// retry of a committed request re-executed. Keeping it lets the resume answer
/// from the fence instead: [`RequestStatus::Duplicate`] when the watermark's
/// reply was still ringed, [`RequestStatus::AlreadyApplied`] when it was not.
///
/// In memory only, like the ring: a fence does not survive a checkpoint or a state
/// transfer, and losing one degrades a resume to the pre-fix behaviour rather than
/// corrupting anything.
#[derive(Debug)]
struct EvictedFence {
    client_id: u128,
    epoch: u64,
    user_id: u32,
    watermark: u64,
    watermark_checksum: u128,
    /// The watermark request's own reply when the ring still held it, so the
    /// retry the resume contract prescribes replays its original bytes. `None`
    /// when it had already aged out, or when a rebind left the register reply
    /// as the newest entry: the resume then answers
    /// [`RequestStatus::AlreadyApplied`], which still never re-executes.
    /// One refcount bump, not a copy.
    latest: Option<CachedReply>,
}

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

/// Serializable form of one occupied slot.
///
/// Folded into the metadata checkpoint (`MetadataSnapshot`) so fence epochs and
/// dedup watermarks survive a restart that drained the WAL prefix they committed
/// in. Carries `client_id` explicitly because the index is rebuilt from it on
/// decode.
///
/// Only the entry's latest reply is carried, not the whole ring: `latest_commit`
/// is re-derived from its header, which is what keeps `evict_oldest` picking the
/// same victim on a checkpoint-restored replica as on a WAL-replayed one. The
/// older ring entries are volatile by design (see [`REPLY_RING_CAPACITY`]), so a
/// retransmit that would have hit them answers
/// [`RequestStatus::AlreadyApplied`] instead of replaying bytes: a worse answer,
/// never a re-execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientEntrySnapshot {
    pub client_id: u128,
    pub epoch: u64,
    pub user_id: u32,
    pub watermark: u64,
    pub watermark_checksum: u128,
    /// Wire bytes of the entry's latest committed reply, round-tripped through
    /// `CachedReply::from_message`. Never empty: registration seeds the ring.
    ///
    /// Serialized as a msgpack `bin` blob, not the integer array a plain `Vec<u8>`
    /// produces, which spends 2 bytes on every byte >= 0x80 and runs a checkpoint's
    /// reply payload up to roughly double on disk.
    #[serde(with = "reply_bytes")]
    pub reply: Vec<u8>,
}

/// Serializable [`ClientTable`]: the occupied slots, each with its index.
///
/// Slot positions are carried explicitly rather than by array position, so the
/// encoded form is proportional to live clients instead of to configured capacity.
/// The alternative (a full `Vec<Option<_>>`) makes the slot count self-perpetuating:
/// [`ClientTable::from_snapshot`] would have to honour the array's length, so
/// lowering `clients_table_max` could never take effect, and every checkpoint would
/// serde-walk `clients_table_max` entries while shard 0 is blocked on fsyncs.
///
/// Deterministic eviction order is unaffected: the index is what places each entry,
/// and it survives here verbatim.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientTableSnapshot {
    pub slots: Vec<(u32, ClientEntrySnapshot)>,
}

/// Serializes reply bytes as a msgpack `bin` blob. See [`ClientEntrySnapshot::reply`].
///
/// `bin` is the only accepted encoding; the `visit_seq` arm that also took the older
/// integer-array form is gone. `SNAPSHOT_FORMAT_VERSION` decides readability in one
/// place, and a second decoder quietly accepting a retired layout makes that stamp a
/// lie.
mod reply_bytes {
    use serde::de::{Error, Visitor};
    use serde::{Deserializer, Serializer};
    use std::fmt;

    pub fn serialize<S: Serializer>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(bytes)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<u8>, D::Error> {
        deserializer.deserialize_byte_buf(BytesVisitor)
    }

    struct BytesVisitor;

    impl Visitor<'_> for BytesVisitor {
        type Value = Vec<u8>;

        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("reply bytes")
        }

        fn visit_bytes<E: Error>(self, bytes: &[u8]) -> Result<Self::Value, E> {
            Ok(bytes.to_vec())
        }

        fn visit_byte_buf<E: Error>(self, bytes: Vec<u8>) -> Result<Self::Value, E> {
            Ok(bytes)
        }
    }
}

/// A [`ClientTableSnapshot`] could not be decoded into a [`ClientTable`], so a
/// corrupt or torn checkpoint refuses boot with a typed error rather than
/// panicking mid-decode.
#[derive(Debug)]
pub enum ClientTableDecodeError {
    /// A slot's serialized reply bytes are not a valid reply message.
    InvalidReply {
        /// Slot whose reply bytes failed to decode.
        slot: usize,
        /// The underlying wire-decode failure.
        source: ConsensusError,
    },
    /// Two occupied slots carry the same `client_id`. Rebuilding the index would
    /// collapse them onto one slot and leave the other occupied but unindexed, so
    /// the decode is rejected.
    DuplicateClientId {
        /// Slot repeating an already-seen `client_id`.
        slot: usize,
        /// Slot that first declared it.
        first_slot: usize,
        /// The duplicated client id.
        client_id: u128,
    },
    /// Two entries claim the same slot index. The second would overwrite the first,
    /// leaving that client indexed onto another's state, so the decode is rejected.
    DuplicateSlot {
        /// The repeated slot index.
        slot: usize,
    },
    /// A slot index is past what any configured capacity can produce, so honouring
    /// it would size the table from a corrupt length.
    SlotOutOfRange {
        /// The out-of-range slot index.
        slot: usize,
        /// Exclusive ceiling on a slot index.
        max: usize,
    },
}

impl fmt::Display for ClientTableDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidReply { slot, source } => write!(
                f,
                "client-table checkpoint slot {slot} holds invalid reply bytes: {source}"
            ),
            Self::DuplicateClientId {
                slot,
                first_slot,
                client_id,
            } => write!(
                f,
                "client-table checkpoint slot {slot} repeats client_id {client_id} already in \
                 slot {first_slot}"
            ),
            Self::DuplicateSlot { slot } => write!(
                f,
                "client-table checkpoint holds two entries for slot {slot}"
            ),
            Self::SlotOutOfRange { slot, max } => write!(
                f,
                "client-table checkpoint slot index {slot} is past the {max}-slot ceiling"
            ),
        }
    }
}

impl std::error::Error for ClientTableDecodeError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::InvalidReply { source, .. } => Some(source),
            Self::DuplicateClientId { .. }
            | Self::DuplicateSlot { .. }
            | Self::SlotOutOfRange { .. } => None,
        }
    }
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
/// ## Durability
///
/// [`Self::to_snapshot`] / [`Self::from_snapshot`] fold the table into the
/// metadata checkpoint, so sessions registered below the snapshot floor survive
/// a restart that drained the WAL prefix they committed in.
///
/// ## Serialization (wire)
///
/// [`Self::encode`] / [`Self::decode`] carry the table across state transfer
/// (a rejoin behind the peers' retained floor replaces its table with the
/// primary's copy). Deterministic: slot-order walk over apply-derived state,
/// so every caught-up replica encodes identical bytes. Distinct from the
/// checkpoint form above: that one is local-recovery durability, this one is
/// the transfer wire format.
#[derive(Debug)]
pub struct ClientTable {
    /// `None` = free slot. Deterministic iteration for eviction + serialization.
    slots: Vec<Option<ClientEntry>>,
    /// `client_id` -> slot index. Rebuilt on decode.
    index: HashMap<u128, usize>,
    /// Fences of clients capacity eviction reclaimed, oldest at the front.
    ///
    /// Bounded by the slot count. A fence is the entry's header fields plus, at
    /// most, the watermark request's own reply, so it costs a fraction of the
    /// entry it replaces. Trimmed oldest-first.
    ///
    /// Replica-local best-effort, NOT replicated state: the bound is
    /// `slots.len()`, which `from_snapshot` and `decode` size per node, and a
    /// state transfer replaces the table wholesale. Losing a fence degrades a
    /// resume to the pre-fence behaviour; it never makes one more permissive.
    evicted_fences: VecDeque<EvictedFence>,
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
            evicted_fences: VecDeque::new(),
        }
    }

    /// Resize the table to `max_clients` slots. Boot-only: reallocating a
    /// populated table would silently drop live sessions, so this must run
    /// before any client registers (the server bootstrap applies the configured
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

    /// Snapshot the table for the metadata checkpoint: every occupied slot with its
    /// index, so positions (and with them deterministic eviction order) survive, plus
    /// each entry's `client_id` so the index rebuilds on decode.
    ///
    /// Walks only occupied slots. This runs on shard 0's checkpoint task, inside the
    /// section where that core is already blocked on the snapshot's fsyncs, so it must
    /// not also allocate and serde-walk one element per configured slot.
    #[must_use]
    pub fn to_snapshot(&self) -> ClientTableSnapshot {
        let slots = self
            .slots
            .iter()
            .enumerate()
            .filter_map(|(slot_idx, slot)| {
                let entry = slot.as_ref()?;
                let slot_idx = u32::try_from(slot_idx).ok()?;
                Some((
                    slot_idx,
                    ClientEntrySnapshot {
                        client_id: entry.client_id,
                        epoch: entry.epoch,
                        user_id: entry.user_id,
                        watermark: entry.watermark,
                        watermark_checksum: entry.watermark_checksum,
                        reply: entry.latest().as_bytes().to_vec(),
                    },
                ))
            })
            .collect();
        ClientTableSnapshot { slots }
    }

    /// Rebuild a table from a checkpoint snapshot: restore each slot in place and
    /// rebuild the client-to-slot index.
    ///
    /// The restored ring holds only the entry's latest reply, so `latest_commit`
    /// (and with it `evict_oldest`'s victim order) is reproduced exactly while
    /// older retransmits degrade from [`RequestStatus::Duplicate`] to
    /// [`RequestStatus::AlreadyApplied`].
    ///
    /// `min_slots` is the configured capacity. The rebuilt table is that, or enough
    /// to hold the highest occupied index when the checkpoint was taken under a
    /// larger capacity: a capacity *lowered* below a live entry's slot cannot be
    /// honoured without dropping a recovered session, so the larger count stands
    /// until those entries drain, and a later checkpoint then rebuilds at the
    /// configured size. Slot positions are preserved either way, so eviction order is
    /// unchanged.
    ///
    /// # Errors
    /// [`ClientTableDecodeError`] if a slot's reply bytes are not a valid reply
    /// message, if two slots share a `client_id`, if two entries claim the same slot,
    /// or if a slot index is past [`CLIENTS_TABLE_SLOT_MAX`] (a corrupt, torn, or
    /// foreign checkpoint). Surfaced rather than panicked so a bad checkpoint refuses
    /// boot instead of unwinding the shard. Callers verify the checkpoint's checksum
    /// against the superblock first, so a correct boot never hits this.
    pub fn from_snapshot(
        snapshot: ClientTableSnapshot,
        min_slots: usize,
    ) -> Result<Self, ClientTableDecodeError> {
        // Bound the capacity on a slot index read off disk before allocating from it,
        // as the superblock and WAL do with their length fields.
        let mut capacity = min_slots;
        for (slot_idx, _) in &snapshot.slots {
            let slot = *slot_idx as usize;
            if slot >= CLIENTS_TABLE_SLOT_MAX {
                return Err(ClientTableDecodeError::SlotOutOfRange {
                    slot,
                    max: CLIENTS_TABLE_SLOT_MAX,
                });
            }
            capacity = capacity.max(slot + 1);
        }

        let mut index = HashMap::with_capacity(snapshot.slots.len());
        let mut slots = Vec::with_capacity(capacity);
        slots.resize_with(capacity, || None);
        for (slot_idx, entry) in snapshot.slots {
            let slot_idx = slot_idx as usize;
            let reply = Message::<ReplyHeader>::try_from(Owned::<MESSAGE_ALIGN>::copy_from_slice(
                &entry.reply,
            ))
            .map_err(|source| ClientTableDecodeError::InvalidReply {
                slot: slot_idx,
                source,
            })?;
            // Reject rather than collapse the index onto one slot, leaving the
            // other occupied but unindexed. Slot `client_id`s are unique in a
            // table this crate produced, so a duplicate means a corrupt or
            // foreign checkpoint.
            if let Some(first_slot) = index.insert(entry.client_id, slot_idx) {
                return Err(ClientTableDecodeError::DuplicateClientId {
                    slot: slot_idx,
                    first_slot,
                    client_id: entry.client_id,
                });
            }
            let latest_commit = reply.header().commit;
            let mut ring = VecDeque::with_capacity(REPLY_RING_CAPACITY);
            ring.push_back(CachedReply::from_message(reply));
            // Two entries claiming one slot would silently drop the first, leaving it
            // indexed but pointing at another client's state.
            if slots[slot_idx].is_some() {
                return Err(ClientTableDecodeError::DuplicateSlot { slot: slot_idx });
            }
            slots[slot_idx] = Some(ClientEntry {
                epoch: entry.epoch,
                user_id: entry.user_id,
                watermark: entry.watermark,
                watermark_checksum: entry.watermark_checksum,
                ring,
                client_id: entry.client_id,
                latest_commit,
            });
        }
        Ok(Self {
            slots,
            index,
            // Fences are in-memory only; a restored table starts with none.
            evicted_fences: VecDeque::new(),
        })
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
    /// Full table evicts the oldest commit, see `Self::evict_oldest`.
    ///
    /// A key this table evicted for capacity re-registers as a fresh entry that
    /// RESTORES the evicted watermark (and the watermark reply when it survived)
    /// from `EvictedFence`, for the same `user_id` only. A committed `Logout`
    /// forgets that fence, so a register after one starts clean.
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
            // A client this table evicted for capacity is resuming, not
            // arriving: its committed request numbers must stay deduped, or the
            // retry the resume contract prescribes re-executes. The watermark's
            // own reply comes back with the fence when the ring still held it,
            // so that retry replays its bytes; every other retry at or below the
            // watermark answers `AlreadyApplied`, which also never re-executes.
            //
            // Same identity only: `client_id` is client-supplied, so a fence
            // must never hand one user another user's dedup history, nor its
            // cached reply bytes, merely because the key was reused.
            let fence = self.take_fence(client_id, user_id);
            let freed = if self.index.len() >= self.slots.len() {
                self.evict_oldest()
            } else {
                None
            };
            let slot_idx = freed
                .or_else(|| self.first_free_slot())
                .expect("eviction must free a slot");
            debug_assert!(
                fence.as_ref().is_none_or(|fence| epoch > fence.epoch),
                "commit_register: revived fence epoch regression"
            );
            let latest_commit = cached.header().commit;
            let mut ring = VecDeque::with_capacity(REPLY_RING_CAPACITY);
            // Oldest at the front: the retained reply committed before this
            // register did, and `latest()` must stay the register's own reply.
            if let Some(replay) = fence.as_ref().and_then(|fence| fence.latest.as_ref()) {
                ring.push_back(replay.clone());
            }
            ring.push_back(cached);
            self.slots[slot_idx] = Some(ClientEntry {
                epoch,
                user_id,
                client_id,
                latest_commit,
                watermark: fence
                    .as_ref()
                    .map_or(REGISTER_REQUEST_ID, |fence| fence.watermark),
                watermark_checksum: fence.as_ref().map_or(0, |fence| fence.watermark_checksum),
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
                // Re-derived, not assigned from `new_commit`: the replaced entry
                // is not necessarily the ring's back. A rebind pushes the
                // register reply last, and a fence-revived entry carries the
                // watermark's reply at the front, so assuming otherwise lets
                // `latest_commit` disagree with what `decode` rebuilds from
                // `ring.back()` -- and it is `evict_oldest`'s only ranking key,
                // so the two would pick different victims from one log.
                entry.latest_commit = entry.latest().header().commit;
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
        // A committed Logout is the explicit end of the session, so it also
        // forgets any fence a capacity eviction left for this key. Otherwise a
        // Logout that commits after the eviction (its prepare predates it, so
        // there is no entry left to drop) would strand a fence, and a later
        // register would revive a watermark the client had already ended.
        self.evicted_fences
            .retain(|fence| fence.client_id != client_id);
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
    /// commit lands as [`CommitReply::NoEntry`] -- the reply still ships, the
    /// client learns the session is gone on its next request (`NoSession` ->
    /// eviction frame -> re-register), and that commit reaches no fence, so a
    /// resume can re-execute exactly that request.
    ///
    /// The evicted session's dedup fence survives via [`Self::remember_fence`]
    /// unless it had committed nothing or the fence is later trimmed, so the
    /// re-registering client is normally answered rather than re-executed.
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
        // Reclaim the replies, keep the fence: the evicted client's own resume
        // must not read as a first-time register, or the retry of a committed
        // request re-executes.
        self.remember_fence(&entry);
        trace!(
            client_id = entry.client_id,
            "evict_oldest: removed client from session table"
        );
        Some(slot_idx)
    }

    /// Record an evicted entry's dedup fence, trimming oldest-first.
    fn remember_fence(&mut self, entry: &ClientEntry) {
        // Nothing committed under this session, so there is nothing to dedup.
        // Worth skipping rather than storing: `evict_oldest` ranks on the oldest
        // `latest_commit`, and a session idle since its register carries its own
        // register op, which makes these the PREFERRED victims -- storing them
        // would crowd real fences out of a store bounded by the slot count.
        if entry.watermark == REGISTER_REQUEST_ID {
            return;
        }
        // One fence per identity: a later eviction supersedes the earlier one,
        // and two fences for one key would let the older (lower) watermark be
        // found first and revive a stale one.
        self.evicted_fences
            .retain(|fence| fence.client_id != entry.client_id || fence.user_id != entry.user_id);
        self.evicted_fences.push_back(EvictedFence {
            client_id: entry.client_id,
            epoch: entry.epoch,
            user_id: entry.user_id,
            watermark: entry.watermark,
            watermark_checksum: entry.watermark_checksum,
            latest: entry.find_cached(entry.watermark).cloned(),
        });
        while self.evicted_fences.len() > self.slots.len() {
            self.evicted_fences.pop_front();
        }
    }

    /// Take back the fence a previous capacity eviction left for this
    /// `(client_id, user_id)` pair. The identity half is the security-relevant
    /// one: `client_id` arrives off the wire.
    ///
    /// Linear because it runs only on a register that missed the index, which is
    /// a consensus commit and already far dearer than a scan of at most
    /// `slots.len()` fences.
    fn take_fence(&mut self, client_id: u128, user_id: u32) -> Option<EvictedFence> {
        // Both fields in the predicate, not a client_id match with the identity
        // checked afterwards: `client_id` is client-supplied, so the store can
        // legitimately hold one fence per user for the same key. Matching on the
        // id alone would let whichever fence sits nearer the front shadow the
        // caller's own, handing it a fresh watermark and re-executing a request
        // it had already committed. It also leaves another user's fence in place
        // rather than consuming it.
        let position = self
            .evicted_fences
            .iter()
            .position(|fence| fence.client_id == client_id && fence.user_id == user_id)?;
        self.evicted_fences.remove(position)
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

/// Failure decoding the state-transfer WIRE encoding of a client table
/// ([`ClientTable::encode`] / [`ClientTable::decode`]).
///
/// Distinct from [`ClientTableDecodeError`], which covers the msgpack
/// CHECKPOINT encoding read off local disk. The two formats validate the same
/// invariants against differently-trusted inputs: a checkpoint is this node's
/// own bytes, while these arrive from a peer.
#[derive(Debug)]
pub enum ClientTableWireError {
    /// Byte stream ended mid-field.
    Truncated,
    /// Leading magic is not [`CLIENT_TABLE_MAGIC`].
    BadMagic,
    /// Trailing hash does not match the content.
    ChecksumMismatch { expected: u64, actual: u64 },
    /// Encoded entry count exceeds [`CLIENTS_TABLE_SLOT_MAX`], the allocation
    /// ceiling no valid table can reach.
    TooManyEntries { count: u32, max: usize },
    /// A cached reply's bytes do not parse as a valid reply message.
    InvalidReply,
    /// An entry carries an empty reply ring (violates the never-empty
    /// invariant registration establishes).
    EmptyRing,
    /// Two entries claim the same `client_id`. Indexing them would leave one
    /// slot occupied but unindexed, which desynchronizes the capacity check in
    /// [`ClientTable::commit_register`] from the actual occupancy.
    DuplicateClientId { slot: usize, client_id: u128 },
    /// A reply ring longer than [`REPLY_RING_CAPACITY`]. `push_latest` only
    /// evicts on equality, so an over-capacity ring grows without bound, and
    /// `encode` writes its length as a `u8`.
    RingTooLong { slot: usize, len: u8, max: usize },
}

impl std::fmt::Display for ClientTableWireError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => write!(f, "encoded client table truncated"),
            Self::BadMagic => write!(f, "encoded client table has wrong magic"),
            Self::ChecksumMismatch { expected, actual } => write!(
                f,
                "client table checksum mismatch: expected {expected:#018x}, actual {actual:#018x}"
            ),
            Self::TooManyEntries { count, max } => {
                write!(f, "encoded client table holds {count} entries, max {max}")
            }
            Self::InvalidReply => write!(f, "encoded client table holds an invalid cached reply"),
            Self::EmptyRing => write!(f, "encoded client table entry has an empty reply ring"),
            Self::DuplicateClientId { slot, client_id } => write!(
                f,
                "encoded client table repeats client {client_id} at entry {slot}"
            ),
            Self::RingTooLong { slot, len, max } => write!(
                f,
                "encoded client table entry {slot} has a {len}-reply ring, max {max}"
            ),
        }
    }
}

impl std::error::Error for ClientTableWireError {}

impl From<Truncated> for ClientTableWireError {
    fn from(_: Truncated) -> Self {
        Self::Truncated
    }
}

/// Format tag for [`ClientTable::encode`]; bump on layout change.
///
/// That includes any `ReplyHeader` layout move -- cached replies are embedded
/// as raw wire bytes, so an artifact written under an older header layout must
/// be refused, not silently misread. `ICT2`: `status` sits at offset 216 (the
/// pre-`ICT2` layout carried a `namespace` word before it).
pub const CLIENT_TABLE_MAGIC: [u8; 4] = *b"ICT2";

/// Per-entry fixed fields in the wire encoding: `client(u128) epoch(u64)
/// user_id(u32) watermark(u64) watermark_checksum(u128) ring_len(u8)`.
const ENCODED_ENTRY_FIXED_LEN: usize = size_of::<u128>()
    + size_of::<u64>()
    + size_of::<u32>()
    + size_of::<u64>()
    + size_of::<u128>()
    + size_of::<u8>();

impl ClientTable {
    /// Encode the table for state transfer.
    ///
    /// Layout (all little-endian): `magic(4) count(u32)` then per entry in
    /// slot order `client(u128) epoch(u64) user_id(u32) watermark(u64)
    /// watermark_checksum(u128) ring_len(u8) [reply_len(u32) reply_bytes]*`,
    /// terminated by an `XxHash3_64(8)` over everything before it.
    ///
    /// Slot order makes the bytes deterministic across caught-up replicas
    /// that reached this state the same way. Not a cross-replica byte
    /// identity: entries compact into `0..count` here, so a replica that
    /// installed a transfer re-slots its clients and later registrations land
    /// elsewhere than on a replica that never did.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn encode(&self) -> Vec<u8> {
        // Size exactly rather than guess: each cached reply is a full wire
        // message, so at the default client cap a guessed reservation is off by
        // orders of magnitude and costs several reallocs of a multi-MB buffer
        // on the serving primary's pump, once per offer build.
        let entries = self.slots.iter().flatten();
        let reserved = CLIENT_TABLE_MAGIC.len()
            + size_of::<u32>()
            + entries
                .map(|entry| {
                    ENCODED_ENTRY_FIXED_LEN
                        + entry
                            .ring
                            .iter()
                            .map(|reply| size_of::<u32>() + reply.bytes.len())
                            .sum::<usize>()
                })
                .sum::<usize>()
            + size_of::<u64>();
        let mut out = Vec::with_capacity(reserved);
        out.extend_from_slice(&CLIENT_TABLE_MAGIC);
        out.extend_from_slice(&(self.index.len() as u32).to_le_bytes());
        for (slot_idx, slot) in self.slots.iter().enumerate() {
            let Some(entry) = slot else { continue };
            debug_assert_eq!(self.index.get(&entry.client_id), Some(&slot_idx));
            out.extend_from_slice(&entry.client_id.to_le_bytes());
            out.extend_from_slice(&entry.epoch.to_le_bytes());
            out.extend_from_slice(&entry.user_id.to_le_bytes());
            out.extend_from_slice(&entry.watermark.to_le_bytes());
            out.extend_from_slice(&entry.watermark_checksum.to_le_bytes());
            out.push(entry.ring.len() as u8);
            for reply in &entry.ring {
                let bytes = reply.bytes.as_slice();
                out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
                out.extend_from_slice(bytes);
            }
        }
        debug_assert_eq!(out.len() + size_of::<u64>(), reserved, "encode reservation");
        let trailer = crate::state_manifest::state_artifact_checksum(&out);
        out.extend_from_slice(&trailer.to_le_bytes());
        out
    }

    /// Decode a table encoded by [`Self::encode`] into a fresh table of at
    /// least `min_slots` capacity, grown to the received entry count.
    ///
    /// Growing mirrors [`Self::from_snapshot`]: a serving primary can
    /// legitimately hold more live sessions than this node's configured cap
    /// (its own table grew from a checkpoint, or it runs a larger cap), and a
    /// cold-boot receiver sits at exactly the raw config value -- rejecting on
    /// the local cap would make such a join fail deterministically.
    ///
    /// The denormalized `latest_commit` is rebuilt from the decoded ring's
    /// back, not trusted from the wire, so it cannot drift from the ring.
    ///
    /// # Errors
    /// [`ClientTableWireError`] on truncation, magic/checksum mismatch, an
    /// entry count past [`CLIENTS_TABLE_SLOT_MAX`], a duplicate `client_id`,
    /// an out-of-range ring length, or an undecodable cached reply.
    ///
    /// # Panics
    /// Unreachable: slice-to-array conversions are length-checked first.
    pub fn decode(bytes: &[u8], min_slots: usize) -> Result<Self, ClientTableWireError> {
        let content = split_verified_trailer(bytes).map_err(|mismatch| match mismatch {
            Some((expected, actual)) => ClientTableWireError::ChecksumMismatch { expected, actual },
            None => ClientTableWireError::Truncated,
        })?;

        let mut reader = LeCursor::new(content);
        if reader.take(CLIENT_TABLE_MAGIC.len())? != CLIENT_TABLE_MAGIC {
            return Err(ClientTableWireError::BadMagic);
        }
        let count = reader.u32()?;
        // Bound the allocation on the same slot ceiling `from_snapshot` uses;
        // `count` is a peer-supplied u32 and this is the only check between it
        // and `Self::new`'s eager Vec resize.
        if count as usize > CLIENTS_TABLE_SLOT_MAX {
            return Err(ClientTableWireError::TooManyEntries {
                count,
                max: CLIENTS_TABLE_SLOT_MAX,
            });
        }

        let mut table = Self::new(min_slots.max(count as usize));
        for slot_idx in 0..count as usize {
            let client_id = reader.u128()?;
            let epoch = reader.u64()?;
            let user_id = reader.u32()?;
            let watermark = reader.u64()?;
            let watermark_checksum = reader.u128()?;
            let ring_len = reader.u8()?;
            if ring_len == 0 {
                return Err(ClientTableWireError::EmptyRing);
            }
            // The artifact checksum only proves the bytes survived transit; it
            // says nothing about the peer that computed them. An over-capacity
            // ring is admitted forever after (`push_latest` evicts only on
            // equality) and eventually wraps `encode`'s `u8` length, making
            // this table permanently un-transferable onward.
            if usize::from(ring_len) > REPLY_RING_CAPACITY {
                return Err(ClientTableWireError::RingTooLong {
                    slot: slot_idx,
                    len: ring_len,
                    max: REPLY_RING_CAPACITY,
                });
            }
            let mut ring = VecDeque::with_capacity(REPLY_RING_CAPACITY);
            for _ in 0..ring_len {
                let reply_len = reader.u32()? as usize;
                let reply_bytes = reader.take(reply_len)?;
                let owned = Owned::<MESSAGE_ALIGN>::copy_from_slice(reply_bytes);
                let message = Message::<GenericHeader>::try_from(owned)
                    .map_err(|_| ClientTableWireError::InvalidReply)?
                    .try_into_typed::<ReplyHeader>()
                    .map_err(|_| ClientTableWireError::InvalidReply)?;
                ring.push_back(CachedReply::from_message(message));
            }
            let latest_commit = ring
                .back()
                .expect("ring_len checked non-zero above")
                .header()
                .commit;
            table.slots[slot_idx] = Some(ClientEntry {
                epoch,
                user_id,
                watermark,
                watermark_checksum,
                ring,
                client_id,
                latest_commit,
            });
            // Reject rather than overwrite, as the checkpoint decoder does. An
            // overwrite leaves the displaced slot occupied but unindexed, and
            // `commit_register` sizes its eviction check off `index.len()`: a
            // full table would then skip eviction, find no free slot, and
            // panic the shard.
            if let Some(first_slot) = table.index.insert(client_id, slot_idx) {
                return Err(ClientTableWireError::DuplicateClientId {
                    slot: first_slot,
                    client_id,
                });
            }
        }
        if !reader.remaining().is_empty() {
            return Err(ClientTableWireError::Truncated);
        }
        Ok(table)
    }

    /// Slot capacity, i.e. the largest table this one can absorb from a peer.
    ///
    /// Sized at construction from the configured cap, then raised by
    /// [`Self::from_snapshot`] to cover any slot the checkpoint holds, so this
    /// can exceed `[metadata] clients_table_max`.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.slots.len()
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
#[allow(clippy::cast_possible_truncation)]
mod tests {
    use super::*;
    use iggy_binary_protocol::{Command, Operation};

    /// Arbitrary non-zero user id for register fixtures; most tests don't
    /// assert on it (see `register_stores_user_id` for the accessor check).
    const TEST_USER_ID: u32 = 7;

    /// Capacity eviction reclaims an entry's replies but must not reset its
    /// dedup fence: the evicted client's own resume is a rebind in everything
    /// but bookkeeping, and the resume contract has it retry the request it
    /// never saw answered.
    #[test]
    fn eviction_keeps_the_fence_so_a_resumed_client_is_not_re_executed() {
        const CLIENT_A: u128 = 0xA11CE;
        const CHURN: [u128; 2] = [0xB0B1, 0xB0B2];

        let mut table = ClientTable::new(2);
        table.commit_register(CLIENT_A, TEST_USER_ID, make_register_reply(CLIENT_A, 1));
        // Request 1 commits for A, so its watermark is 1.
        table.commit_reply(CLIENT_A, make_reply_for(CLIENT_A, 1, 2));

        // Two fresh registers fill the table and evict A (oldest commit).
        for (offset, churn) in CHURN.iter().enumerate() {
            let commit = 3 + offset as u64;
            table.commit_register(*churn, TEST_USER_ID, make_register_reply(*churn, commit));
        }
        assert!(
            table.get_epoch(CLIENT_A).is_none(),
            "the churn must have evicted A for this test to mean anything"
        );

        // A resumes: fresh register under the same id, then retries request 1.
        table.commit_register(CLIENT_A, TEST_USER_ID, make_register_reply(CLIENT_A, 10));
        let resumed_epoch = table.get_epoch(CLIENT_A).expect("resume registered");

        match table.check_request(CLIENT_A, resumed_epoch, 1, 0) {
            RequestStatus::Duplicate(replayed) => {
                assert_eq!(
                    replayed.header().request,
                    1,
                    "the retained reply must be the watermark request's own"
                );
            }
            other => panic!(
                "a committed request retried after capacity eviction must replay its cached \
                 reply, not be executed a second time; got {other:?}"
            ),
        }

        // A request above the restored watermark is still new.
        assert!(matches!(
            table.check_request(CLIENT_A, resumed_epoch, 2, 0),
            RequestStatus::New
        ));
    }

    /// `client_id` is client-supplied, so a fence belongs to the user that
    /// earned it: a register under a different identity must neither inherit the
    /// dedup history (it would be handed another user's cached reply bytes) nor
    /// consume the fence (anyone could then erase another client's history just
    /// by presenting its key).
    #[test]
    fn a_fence_is_neither_inherited_nor_consumed_by_a_different_user() {
        const CLIENT_A: u128 = 0xA11CE;
        const OTHER_USER: u32 = TEST_USER_ID + 1;

        let mut table = ClientTable::new(2);
        table.commit_register(CLIENT_A, TEST_USER_ID, make_register_reply(CLIENT_A, 1));
        table.commit_reply(CLIENT_A, make_reply_for(CLIENT_A, 1, 2));
        for (offset, churn) in [0xB0B1u128, 0xB0B2].iter().enumerate() {
            let commit = 3 + offset as u64;
            table.commit_register(*churn, TEST_USER_ID, make_register_reply(*churn, commit));
        }
        assert!(
            table.get_epoch(CLIENT_A).is_none(),
            "the churn must have evicted A, leaving its fence"
        );

        table.commit_register(CLIENT_A, OTHER_USER, make_register_reply(CLIENT_A, 10));
        let squatter_epoch = table.get_epoch(CLIENT_A).expect("registered");
        assert!(
            matches!(
                table.check_request(CLIENT_A, squatter_epoch, 1, 0),
                RequestStatus::New
            ),
            "a different user must start at a fresh watermark, not inherit the fence"
        );
        assert!(
            table
                .evicted_fences
                .iter()
                .any(|fence| fence.client_id == CLIENT_A && fence.user_id == TEST_USER_ID),
            "the owner's fence must survive a register under another identity"
        );
    }

    /// Two fences can share a `client_id` with different users (the key is
    /// client-supplied). Lookup must find the caller's own fence rather than
    /// stopping at whichever one happens to sit closer to the front.
    #[test]
    fn a_fence_is_found_behind_another_users_fence_for_the_same_client_id() {
        const CLIENT_A: u128 = 0xA11CE;
        const FIRST_USER: u32 = TEST_USER_ID;
        const SECOND_USER: u32 = TEST_USER_ID + 1;

        let mut table = ClientTable::new(2);
        for (user, watermark) in [(FIRST_USER, 1u64), (SECOND_USER, 4u64)] {
            table.evicted_fences.push_back(EvictedFence {
                client_id: CLIENT_A,
                epoch: watermark,
                user_id: user,
                watermark,
                watermark_checksum: 0,
                latest: Some(CachedReply::from_message(make_reply_for(
                    CLIENT_A, watermark, watermark,
                ))),
            });
        }

        let fence = table
            .take_fence(CLIENT_A, SECOND_USER)
            .expect("the second user's own fence must be reachable behind the first user's");
        assert_eq!(fence.watermark, 4);
        assert!(
            table
                .evicted_fences
                .iter()
                .any(|fence| fence.user_id == FIRST_USER),
            "and taking it must leave the other user's fence in place"
        );
        assert!(
            table.take_fence(CLIENT_A, SECOND_USER).is_none(),
            "a fence is consumed on the hit, so a re-minted key cannot revive a \
             stale watermark and swallow a fresh session's requests"
        );
    }

    /// A committed `Logout` ends the session explicitly, so it must not leave a
    /// fence behind for a later register to revive: the client asked to be
    /// forgotten, and a Logout committing after its entry was evicted finds
    /// nothing to drop.
    #[test]
    fn logout_forgets_an_evicted_fence() {
        const CLIENT_A: u128 = 0xA11CE;

        let mut table = ClientTable::new(2);
        table.commit_register(CLIENT_A, TEST_USER_ID, make_register_reply(CLIENT_A, 1));
        table.commit_reply(CLIENT_A, make_reply_for(CLIENT_A, 1, 2));
        for (offset, churn) in [0xB0B1u128, 0xB0B2].iter().enumerate() {
            let commit = 3 + offset as u64;
            table.commit_register(*churn, TEST_USER_ID, make_register_reply(*churn, commit));
        }
        assert!(table.get_epoch(CLIENT_A).is_none(), "A must be evicted");

        table.remove_client(CLIENT_A);
        table.commit_register(CLIENT_A, TEST_USER_ID, make_register_reply(CLIENT_A, 10));
        let epoch = table.get_epoch(CLIENT_A).expect("registered again");
        assert!(
            matches!(
                table.check_request(CLIENT_A, epoch, 1, 0),
                RequestStatus::New
            ),
            "a register after Logout must start fresh, not revive the ended session's watermark"
        );
    }

    /// The fence store is bounded by the slot count, so a churn far longer than
    /// the table cannot grow it without limit.
    #[test]
    fn evicted_fences_stay_bounded_by_the_slot_count() {
        let mut table = ClientTable::new(2);
        // Each client commits an app request, so eviction has a real fence to
        // keep -- a register-only session is skipped on purpose.
        for client in 1..=20u128 {
            let commit = (client * 2) as u64;
            table.commit_register(client, TEST_USER_ID, make_register_reply(client, commit));
            table.commit_reply(client, make_reply_for(client, 1, commit + 1));
        }
        assert_eq!(
            table.evicted_fences.len(),
            table.slots.len(),
            "fences must be trimmed to the slot count"
        );
    }

    #[allow(clippy::cast_possible_truncation)]
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
            // Real size so codec-roundtripped replies re-parse.
            size: header_size as u32,
            command: Command::Reply,
            operation: Operation::Register,
            ..ReplyHeader::default()
        };
        msg
    }

    fn make_reply_for(client: u128, request: u64, commit: u64) -> Message<ReplyHeader> {
        make_reply_with_checksum(client, request, commit, 0)
    }

    #[allow(clippy::cast_possible_truncation)]
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
            // Real size so codec-roundtripped replies re-parse.
            size: header_size as u32,
            command: Command::Reply,
            operation: Operation::SendMessages,
            ..ReplyHeader::default()
        };
        msg
    }

    #[test]
    fn to_from_snapshot_round_trips_epochs_and_watermarks() {
        let mut table = ClientTable::new(8);
        table.commit_register(1, 11, make_register_reply(1, 10));
        table.commit_register(2, 22, make_register_reply(2, 20));
        // Client 1 committed request 5; its reply is the entry's latest.
        table.commit_reply(1, make_reply_with_checksum(1, 5, 30, 0xbeef));

        let restored = ClientTable::from_snapshot(table.to_snapshot(), 0).unwrap();

        // Fences and dedup history survive, and the index is rebuilt (every
        // accessor reads through it).
        assert_eq!(restored.get_epoch(1), Some(10));
        assert_eq!(restored.get_epoch(2), Some(20));
        assert_eq!(restored.get_watermark(1), Some(5));
        assert_eq!(restored.get_watermark(2), Some(0));
        assert_eq!(restored.get_user_id(1), Some(11));
        // Replaying request 5 is a dedup hit, not a re-execution: at-most-once
        // holds across a restart, and the original bytes still answer it.
        match restored.check_request(1, 10, 5, 0xbeef) {
            RequestStatus::Duplicate(cached) => assert_eq!(cached.header().request, 5),
            other => panic!("expected Duplicate, got {other:?}"),
        }
        // The persisted watermark checksum still catches request-id reuse.
        assert!(matches!(
            restored.check_request(1, 10, 5, 0xfeed),
            RequestStatus::ChecksumMismatch { request: 5 }
        ));
        // A zombie holding the pre-restart epoch of a since-rebound client is
        // still fenced, so the fence is not weakened by the round trip.
        assert!(matches!(
            restored.check_request(1, 9, 6, 0),
            RequestStatus::Fenced {
                current: 10,
                received: 9
            }
        ));
        // A client that never registered is still unknown.
        assert!(matches!(
            restored.check_request(3, 1, 1, 0),
            RequestStatus::NoSession
        ));
    }

    // Only the entry's latest reply is persisted, so a retransmit of an older
    // ring entry is refused execution rather than answered from cache.
    #[test]
    fn snapshot_drops_stale_ring_replies_but_keeps_at_most_once() {
        let mut table = ClientTable::new(4);
        table.commit_register(1, TEST_USER_ID, make_register_reply(1, 10));
        table.commit_reply(1, make_reply_for(1, 5, 20));
        table.commit_reply(1, make_reply_for(1, 6, 21));

        let restored = ClientTable::from_snapshot(table.to_snapshot(), 0).unwrap();

        assert!(matches!(
            restored.check_request(1, 10, 5, 0),
            RequestStatus::AlreadyApplied {
                request: 5,
                watermark: 6
            }
        ));
    }

    // Slot positions are preserved, so a checkpoint-restored replica picks the
    // same eviction victim as one that replayed the whole WAL.
    #[test]
    fn snapshot_preserves_slot_order_for_eviction() {
        let mut table = ClientTable::new(2);
        table.commit_register(1, TEST_USER_ID, make_register_reply(1, 10));
        table.commit_register(2, TEST_USER_ID, make_register_reply(2, 20));

        let mut restored = ClientTable::from_snapshot(table.to_snapshot(), 0).unwrap();
        assert_eq!(restored.client_ids().collect::<Vec<_>>(), vec![1, 2]);

        // Full table: the oldest latest_commit (client 1, op 10) is evicted, and
        // latest_commit came back from the persisted reply header.
        restored.commit_register(3, TEST_USER_ID, make_register_reply(3, 30));
        assert_eq!(restored.get_epoch(1), None);
        assert_eq!(restored.get_epoch(2), Some(20));
        assert_eq!(restored.get_epoch(3), Some(30));
    }

    // A LOWERED `clients_table_max` must take effect too. It cannot while the encoded
    // form is one element per configured slot, since the rebuilt table has to be at
    // least as long as that array.
    #[test]
    fn from_snapshot_shrinks_to_the_configured_capacity() {
        let mut table = ClientTable::new(8);
        table.commit_register(1, TEST_USER_ID, make_register_reply(1, 10));

        let mut restored = ClientTable::from_snapshot(table.to_snapshot(), 2).unwrap();

        // Capacity 2: the recovered session plus one, and the third register evicts.
        restored.commit_register(2, TEST_USER_ID, make_register_reply(2, 20));
        restored.commit_register(3, TEST_USER_ID, make_register_reply(3, 30));
        assert_eq!(restored.count(), 2);
        assert_eq!(
            restored.get_epoch(1),
            None,
            "the oldest committed entry is the eviction victim at the lowered capacity"
        );
    }

    // A capacity lowered below a live entry's slot cannot be honoured without dropping
    // a recovered session, so the table keeps room for it.
    #[test]
    fn from_snapshot_keeps_a_slot_above_the_configured_capacity() {
        let mut table = ClientTable::new(8);
        table.commit_register(1, TEST_USER_ID, make_register_reply(1, 10));
        let mut snapshot = table.to_snapshot();
        snapshot.slots[0].0 = 5;

        let restored = ClientTable::from_snapshot(snapshot, 2).unwrap();
        assert_eq!(
            restored.get_epoch(1),
            Some(10),
            "an entry above the configured capacity must survive, not be dropped"
        );
    }

    #[test]
    fn from_snapshot_rejects_two_entries_in_one_slot() {
        let mut table = ClientTable::new(2);
        table.commit_register(1, TEST_USER_ID, make_register_reply(1, 10));
        table.commit_register(2, TEST_USER_ID, make_register_reply(2, 20));
        let mut snapshot = table.to_snapshot();
        snapshot.slots[1].0 = snapshot.slots[0].0;

        assert!(matches!(
            ClientTable::from_snapshot(snapshot, 0),
            Err(ClientTableDecodeError::DuplicateSlot { slot: 0 })
        ));
    }

    #[test]
    fn from_snapshot_rejects_a_slot_index_past_the_ceiling() {
        // The capacity is allocated from this index, so an out-of-range one must be
        // refused before the allocation rather than sized from.
        let mut table = ClientTable::new(2);
        table.commit_register(1, TEST_USER_ID, make_register_reply(1, 10));
        let mut snapshot = table.to_snapshot();
        snapshot.slots[0].0 = u32::MAX;

        assert!(matches!(
            ClientTable::from_snapshot(snapshot, 0),
            Err(ClientTableDecodeError::SlotOutOfRange { .. })
        ));
    }

    // A raised `clients_table_max` must take effect on the next boot rather than
    // staying inert behind the checkpoint's slot count.
    #[test]
    fn from_snapshot_grows_to_the_configured_capacity() {
        let mut table = ClientTable::new(1);
        table.commit_register(1, TEST_USER_ID, make_register_reply(1, 10));

        let mut restored = ClientTable::from_snapshot(table.to_snapshot(), 3).unwrap();

        // Two free slots were padded on, so the next registers land without
        // evicting the recovered session.
        restored.commit_register(2, TEST_USER_ID, make_register_reply(2, 20));
        restored.commit_register(3, TEST_USER_ID, make_register_reply(3, 30));
        assert_eq!(restored.count(), 3);
        assert_eq!(restored.get_epoch(1), Some(10));
    }

    #[test]
    fn from_snapshot_rejects_duplicate_client_ids() {
        let mut table = ClientTable::new(2);
        table.commit_register(1, TEST_USER_ID, make_register_reply(1, 10));
        table.commit_register(2, TEST_USER_ID, make_register_reply(2, 20));
        let mut snapshot = table.to_snapshot();
        snapshot.slots[1].1 = snapshot.slots[0].1.clone();

        assert!(matches!(
            ClientTable::from_snapshot(snapshot, 0),
            Err(ClientTableDecodeError::DuplicateClientId {
                slot: 1,
                first_slot: 0,
                client_id: 1
            })
        ));
    }

    #[test]
    fn from_snapshot_rejects_invalid_reply_bytes() {
        let mut table = ClientTable::new(2);
        table.commit_register(1, TEST_USER_ID, make_register_reply(1, 10));
        let mut snapshot = table.to_snapshot();
        snapshot.slots[0].1.reply = vec![0xff; 8];

        assert!(matches!(
            ClientTable::from_snapshot(snapshot, 0),
            Err(ClientTableDecodeError::InvalidReply { slot: 0, .. })
        ));
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

    // Codec tests

    // State transfer ships the table as bytes; the decoded table must answer
    // every dedup question identically to the original.
    #[test]
    fn encode_decode_roundtrip_preserves_dedup_state() {
        let mut table = ClientTable::new(10);
        table.commit_register(1, 11, make_register_reply(1, 10));
        table.commit_reply(1, make_reply_with_checksum(1, 1, 11, 0xAA));
        table.commit_reply(1, make_reply_for(1, 2, 12));
        // Rebind at op 20: fence moves to 20, watermark preserved.
        table.commit_register(1, 22, make_register_reply(1, 20));
        table.commit_register(2, 33, make_register_reply(2, 30));

        let encoded = table.encode();
        let decoded = ClientTable::decode(&encoded, 10).expect("roundtrip decodes");

        assert_eq!(decoded.count(), 2);
        assert_eq!(decoded.get_epoch(1), Some(20));
        assert_eq!(decoded.get_user_id(1), Some(22));
        assert_eq!(decoded.get_watermark(1), Some(2));
        assert_eq!(decoded.get_epoch(2), Some(30));
        match decoded.check_request(1, 20, 2, 0) {
            RequestStatus::Duplicate(cached) => {
                assert_eq!(cached.header().request, 2, "latest reply survives");
                assert_eq!(cached.header().commit, 12, "original bytes survive");
            }
            other => panic!("expected Duplicate, got {other:?}"),
        }
        assert!(matches!(
            decoded.check_request(1, 20, 1, 0xBB),
            RequestStatus::ChecksumMismatch { request: 1 }
        ));
        assert!(matches!(
            decoded.check_request(1, 10, 1, 0),
            RequestStatus::Fenced { .. }
        ));

        // Deterministic bytes: encoding the decoded table reproduces them.
        assert_eq!(decoded.encode(), encoded);
    }

    // The denormalized `latest_commit` is rebuilt from the ring on decode, so
    // eviction ranks a transferred table exactly like the original.
    #[test]
    fn decode_rebuilds_eviction_ranking() {
        let mut table = ClientTable::new(2);
        table.commit_register(100, TEST_USER_ID, make_register_reply(100, 10));
        table.commit_register(200, TEST_USER_ID, make_register_reply(200, 20));
        table.commit_reply(100, make_reply_for(100, 1, 30));

        let mut decoded = ClientTable::decode(&table.encode(), 2).expect("roundtrip decodes");
        // 200 (latest commit 20) is older than 100 (latest commit 30).
        decoded.commit_register(300, TEST_USER_ID, make_register_reply(300, 40));
        assert!(decoded.get_reply(100).is_some());
        assert!(decoded.get_reply(200).is_none(), "200 was the oldest");
        assert!(decoded.get_reply(300).is_some());
    }

    #[test]
    fn decode_rejects_corruption() {
        let mut table = ClientTable::new(4);
        table.commit_register(1, TEST_USER_ID, make_register_reply(1, 10));
        let encoded = table.encode();

        // Flipped content byte -> checksum mismatch.
        let mut flipped = encoded.clone();
        flipped[8] ^= 0xFF;
        assert!(matches!(
            ClientTable::decode(&flipped, 4),
            Err(ClientTableWireError::ChecksumMismatch { .. })
        ));

        // Truncation.
        assert!(matches!(
            ClientTable::decode(&encoded[..encoded.len() - 1], 4),
            Err(ClientTableWireError::ChecksumMismatch { .. } | ClientTableWireError::Truncated)
        ));

        let empty = ClientTable::new(4).encode();
        assert_eq!(
            ClientTable::decode(&empty, 4)
                .expect("empty table decodes")
                .count(),
            0
        );
    }

    /// Re-stamp a hand-edited body so it passes the trailer check and the
    /// per-field validations are what the decode actually exercises.
    fn reseal(mut content: Vec<u8>) -> Vec<u8> {
        let trailer = crate::state_manifest::state_artifact_checksum(&content);
        content.extend_from_slice(&trailer.to_le_bytes());
        content
    }

    // A duplicate client_id would collapse the index onto one slot, leaving the
    // other occupied but unindexed. `commit_register` sizes its eviction check
    // off `index.len()`, so a full table would then skip eviction, find no free
    // slot, and panic the shard.
    #[test]
    fn decode_rejects_a_duplicate_client_id() {
        let mut table = ClientTable::new(2);
        table.commit_register(7, TEST_USER_ID, make_register_reply(7, 10));
        table.commit_register(9, TEST_USER_ID, make_register_reply(9, 20));
        let encoded = table.encode();

        // Rewrite the second entry's client_id to match the first. Entries are
        // fixed-width up to their ring, and both rings hold one register reply
        // of equal length, so the second entry starts at a computable offset.
        let content = &encoded[..encoded.len() - size_of::<u64>()];
        let header_len = CLIENT_TABLE_MAGIC.len() + size_of::<u32>();
        let reply_len = (content.len() - header_len - 2 * ENCODED_ENTRY_FIXED_LEN) / 2;
        let second = header_len + ENCODED_ENTRY_FIXED_LEN + reply_len;
        let mut duped = content.to_vec();
        duped[second..second + size_of::<u128>()].copy_from_slice(&7u128.to_le_bytes());

        assert!(matches!(
            ClientTable::decode(&reseal(duped), 2),
            Err(ClientTableWireError::DuplicateClientId { client_id: 7, .. })
        ));
    }

    // A serving primary can hold more live sessions than this node's cap: a
    // cold-boot receiver sits at the raw config value, so rejecting on it
    // would make a join under cap reduction fail deterministically. Decode
    // grows to the received count instead, as `from_snapshot` does.
    #[test]
    fn decode_grows_capacity_to_the_received_count() {
        let mut table = ClientTable::new(2);
        table.commit_register(7, TEST_USER_ID, make_register_reply(7, 10));
        table.commit_register(9, TEST_USER_ID, make_register_reply(9, 20));
        let encoded = table.encode();

        let grown = ClientTable::decode(&encoded, 1).expect("decode grows past the local floor");
        assert_eq!(grown.count(), 2);
        assert_eq!(grown.capacity(), 2);

        let floored = ClientTable::decode(&encoded, 8).expect("floor kept when larger");
        assert_eq!(floored.capacity(), 8);
    }

    // The received count is the only bound between a peer-supplied u32 and the
    // eager slot allocation, so it must stop at the same ceiling
    // `from_snapshot` enforces.
    #[test]
    fn decode_rejects_a_count_past_the_slot_ceiling() {
        let mut table = ClientTable::new(1);
        table.commit_register(3, TEST_USER_ID, make_register_reply(3, 10));
        let encoded = table.encode();

        let content = &encoded[..encoded.len() - size_of::<u64>()];
        let mut oversized = content.to_vec();
        let count_at = CLIENT_TABLE_MAGIC.len();
        #[allow(clippy::cast_possible_truncation)]
        {
            oversized[count_at..count_at + size_of::<u32>()]
                .copy_from_slice(&(CLIENTS_TABLE_SLOT_MAX as u32 + 1).to_le_bytes());
        }

        assert!(matches!(
            ClientTable::decode(&reseal(oversized), 1),
            Err(ClientTableWireError::TooManyEntries {
                max: CLIENTS_TABLE_SLOT_MAX,
                ..
            })
        ));
    }

    // `push_latest` evicts only on equality, so an over-capacity ring is
    // admitted permanently and grows on every later reply until `encode`'s u8
    // length wraps and the table stops being transferable at all.
    #[test]
    fn decode_rejects_a_ring_longer_than_capacity() {
        let mut table = ClientTable::new(1);
        table.commit_register(3, TEST_USER_ID, make_register_reply(3, 10));
        let encoded = table.encode();

        let content = &encoded[..encoded.len() - size_of::<u64>()];
        let mut oversized = content.to_vec();
        // ring_len is the last fixed field of the entry.
        let ring_len_at = CLIENT_TABLE_MAGIC.len() + size_of::<u32>() + ENCODED_ENTRY_FIXED_LEN - 1;
        assert_eq!(oversized[ring_len_at], 1, "entry's ring holds one reply");
        #[allow(clippy::cast_possible_truncation)]
        {
            oversized[ring_len_at] = REPLY_RING_CAPACITY as u8 + 1;
        }

        assert!(matches!(
            ClientTable::decode(&reseal(oversized), 1),
            Err(ClientTableWireError::RingTooLong { .. })
        ));
    }
}
