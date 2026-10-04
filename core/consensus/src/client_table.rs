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
use iggy_binary_protocol::{GenericHeader, Operation, ReplyHeader};
use iggy_common::IggyError;
use serde::{Deserialize, Serialize};
use server_common::{
    MESSAGE_ALIGN, Message,
    iobuf::{Frozen, Owned},
};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::mem::size_of;
use std::sync::{Arc, Weak};

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
    /// # Panics
    /// Only if immutable bytes cease to match the validated reply stored here.
    #[must_use]
    pub fn into_message(self) -> Message<ReplyHeader> {
        Message::try_from(Owned::<MESSAGE_ALIGN>::copy_from_slice(
            self.bytes.as_slice(),
        ))
        .expect("immutable cached reply was validated before storage")
    }

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

impl From<Message<ReplyHeader>> for CachedReply {
    fn from(message: Message<ReplyHeader>) -> Self {
        Self {
            bytes: message.into_generic().into_frozen(),
        }
    }
}

impl CachedReply {
    /// Raw reply bytes for checkpoint serialization, validated on decode.
    fn as_bytes(&self) -> &[u8] {
        self.bytes.as_slice()
    }

    /// Wire size of this reply, the unit [`REPLY_RING_RETENTION_BYTES`] budgets.
    fn byte_len(&self) -> usize {
        self.bytes.len()
    }
}

/// Reserved request number for [`Operation::Register`].
/// Real requests start at 1 (header validation enforces `request > 0`).
pub const REGISTER_REQUEST_ID: u64 = 0;

/// Server-originated operations have no session or cached receipt. Wire
/// validation rejects this id, so no external client can claim it.
pub const RESERVED_CLIENT_ID: u128 = 0;

#[must_use]
pub const fn is_partition_receipt_operation(operation: Operation) -> bool {
    matches!(
        operation,
        Operation::SendMessages | Operation::StoreConsumerOffset | Operation::DeleteConsumerOffset
    )
}

pub use iggy_binary_protocol::requests::users::login_register::BIND_SECRET_BYTES;
const BIND_VERIFIER_CONTEXT: &str = "apache.iggy session bind verifier v1";

#[must_use]
pub fn bind_verifier(
    client_id: u128,
    user_id: u32,
    secret: &[u8; BIND_SECRET_BYTES],
) -> [u8; BIND_SECRET_BYTES] {
    let mut hasher = blake3::Hasher::new_derive_key(BIND_VERIFIER_CONTEXT);
    hasher.update(&client_id.to_le_bytes());
    hasher.update(&user_id.to_le_bytes());
    hasher.update(secret);
    hasher.finalize().into()
}

/// Server-owned request number for lease expiry, which has no client-issued ID.
pub const EXPIRED_SESSION_REQUEST_ID: u64 = u64::MAX;

/// Exclusive ceiling on a checkpointed slot index.
///
/// Bounds the table [`ClientTable::from_snapshot`] allocates from an index it read off
/// disk. Mirrors the config's `MAX_METADATA_CLIENTS_TABLE_MAX`, the largest capacity an
/// operator can configure, so no valid checkpoint can carry an index at or above it.
pub const CLIENTS_TABLE_SLOT_MAX: usize = 1 << 16;

/// Minimum reply-cache depth. The latest unresolved result is always retained.
///
/// Metadata checkpoints retain the latest reply, and transfer preserves this
/// floor. Older cached replies may be lost across recovery; their request IDs
/// remain protected and cannot execute again. One unresolved request per group
/// makes the latest reply sufficient for the durable retry contract.
pub const REPLY_RING_CAPACITY: usize = 5;

/// Byte budget for the replies retained past [`REPLY_RING_CAPACITY`].
///
/// Deep retention exists for the slow retrier: a request whose reply aged out
/// can only be answered "already applied, reply gone", which tells the caller
/// its operation succeeded but hands back no result. Budgeting in bytes rather
/// than in replies puts the depth where it is cheapest: a session sending
/// metadata operations keeps a long history, one pulling large batches keeps
/// none past the floor.
///
/// # Depth
///
/// A reply is never shorter than its 256-byte [`ReplyHeader`], so this budget
/// is also the only thing bounding the ring's length: 8 KiB / 256 B = 32
/// replies at the deepest, against a floor of [`REPLY_RING_CAPACITY`].
///
/// The 32 is a chosen bound, not a measured one. The SDK holds one request in
/// flight per session, so what has to fit is the number of newer requests the
/// same session commits between a reply going unacknowledged and its retry
/// landing, and nothing in the tree measures that today.
///
/// # Cost
///
/// Not a wash on the common case. The common metadata reply is header-only, so
/// per-slot retention goes from 5 x 256 B = 1.25 KiB to 8 KiB, a 6.4x rise: a
/// saturated table at the default `clients_table_max` of 8192 goes from 10 MiB
/// to 64 MiB, and from 41k live [`Frozen`] buffers to 262k. At the
/// [`CLIENTS_TABLE_SLOT_MAX`] ceiling it is 512 MiB. Replies carrying a payload
/// exhaust the budget sooner and cost proportionally less; above roughly
/// 1.6 KiB apiece the [`REPLY_RING_CAPACITY`] floor dominates and this budget
/// adds nothing at all.
pub const REPLY_RING_RETENTION_BYTES: usize = 8 * 1024;

/// Registered identity, immutable epoch and principal, and committed retry protection.
#[derive(Debug)]
struct ClientEntry {
    bind_verifier: [u8; 32],
    ended_op: Option<u64>,
    /// Original Register commit. Matching login retries retain this epoch.
    epoch: u64,
    attachment: Option<Arc<()>>,
    /// Principal that owns the registration and its retry protection.
    user_id: u32,
    /// Highest committed request number. `REGISTER_REQUEST_ID` (0) until the
    /// first app op commits. Survives re-register: a resumed session keeps
    /// its dedup history.
    watermark: u64,
    /// Partition-slice only: bit `i` set means request `watermark - i` has
    /// committed, bit 0 being the watermark itself. A request in the retained
    /// window with its bit clear has no commit recorded on this replica and
    /// executes; below the window the outcome is unknown and refused. Zero
    /// on the metadata plane, whose reply ring plays this role.
    committed_window: u128,
    /// Latest reply is protected while live; older cached results are bounded.
    ring: VecDeque<CachedReply>,
    /// Reply identity retained for checkpoint validation without header casts.
    client_id: u128,
    latest_commit: u64,
}

/// A local attachment to one authenticated metadata session.
///
/// It cannot keep the session alive. Logout and table replacement invalidate
/// every attachment, including those held by other shard threads. Matching
/// registration retries preserve existing attachments.
#[derive(Debug, Clone)]
pub struct SessionAttachment {
    session: Weak<()>,
}

impl SessionAttachment {
    #[must_use]
    pub fn is_valid(&self) -> bool {
        self.session.strong_count() != 0
    }
}

/// Serializable form of one occupied slot.
///
/// Folded into the metadata checkpoint (`MetadataSnapshot`) so fence epochs and
/// dedup watermarks survive a restart that drained the WAL prefix they committed
/// in. Carries `client_id` explicitly because the index is rebuilt from it on
/// decode.
///
/// The latest result is sufficient because a session permits one unresolved
/// request per group. Older request numbers remain fenced without cached bytes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientEntrySnapshot {
    pub bind_verifier: [u8; 32],
    pub ended_op: Option<u64>,
    pub client_id: u128,
    pub epoch: u64,
    pub user_id: u32,
    pub watermark: u64,
    /// Wire bytes of the latest committed reply, validated as [`Message<ReplyHeader>`]
    /// on restore. Never empty: registration seeds the ring.
    ///
    /// Serialized as a msgpack `bin` blob, not the integer array a plain `Vec<u8>`
    /// produces, which spends 2 bytes on every byte >= 0x80 and runs a checkpoint's
    /// reply payload up to roughly double on disk.
    #[serde(with = "reply_bytes")]
    pub reply: Vec<u8>,
}

/// Committed capacity and occupied slots. Local configuration cannot shrink
/// recovered protection; sparse slots avoid serializing unused capacity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientTableSnapshot {
    pub capacity: usize,
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
    InvalidEntry {
        slot: usize,
    },
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
            Self::InvalidEntry { slot } => write!(
                f,
                "client-table checkpoint slot {slot} has inconsistent protection"
            ),
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
            | Self::InvalidEntry { .. }
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
    /// The retained receipt belongs to a different operation.
    OperationMismatch { request: u64 },
    /// No entry for this client; must register first.
    NoSession,
    /// Stamped epoch is older than the entry's: a zombie holdover from
    /// before a re-register. Terminal for that holder.
    Fenced { current: u64, received: u64 },
    /// Stamped epoch is newer than any this table minted: client bug
    /// (epochs are only handed out by register replies).
    EpochAhead { current: u64, received: u64 },
}

/// Committed-window verdict for a partition dedup slice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SliceRequestStatus {
    /// No retained commit mark for this request.
    New,
    /// A retained commit mark protects this request from execution.
    Committed,
    /// The request is outside the retained window and its outcome is unknown.
    AgedOut,
}

/// Result of retaining a committed metadata receipt.
///
/// Real client operations fail closed on a non-`Cached` outcome;
/// server-originated operations have no registered entry or receipt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitReply {
    /// Reply cached and the watermark advanced (or refreshed in place).
    Cached,
    /// No registered entry, including for server-originated operations.
    NoEntry,
    /// A replayed op is older than the restored protection frontier.
    SkippedRegression { stored: u64, received: u64 },
}

/// Which of the table's mechanisms an instance runs.
///
/// Metadata registers sessions and keeps a reply ring in preallocated slots.
/// Partition slices grow lazily and retain one session-qualified receipt per client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientTableMode {
    /// Metadata plane: replies cached, epoch fenced, slots preallocated.
    Metadata,
    /// One partition consensus group's receipt protection.
    PartitionSlice,
}

impl ClientTableMode {
    /// Cache metadata replies in a ring. Partition receipts use
    /// [`ClientTable::commit_partition_reply`] independently of this setting.
    #[must_use]
    pub const fn cache_replies(self) -> bool {
        matches!(self, Self::Metadata)
    }

    /// Enforce the metadata registration fence in `check_request`.
    /// Partition admission checks its receipt's epoch independently.
    #[must_use]
    pub const fn fence_epoch(self) -> bool {
        matches!(self, Self::Metadata)
    }

    /// Allocate every slot up front. Off: slots grow to the cap on demand.
    /// Slot assignment is identical either way -- both hand out the lowest free
    /// index -- so slot identities and wire encoding are unchanged.
    #[must_use]
    pub const fn preallocate_slots(self) -> bool {
        matches!(self, Self::Metadata)
    }
}

/// One partition slice entry in its wire and install form.
///
/// Named fields rather than a tuple because `watermark` and `latest_commit`
/// are both `u64` and a positional swap would decode cleanly into the wrong
/// dedup decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DedupWatermark {
    pub client: u128,
    /// Acting user the watermark belongs to. A different user committing under
    /// the same client ID cannot inherit a live session's protection.
    pub user_id: u32,
    /// Highest committed request number.
    pub watermark: u64,
    /// Commit op of the protected result.
    pub latest_commit: u64,
    /// Bit `i` set: request `watermark - i` committed. See
    /// [`COMMITTED_WINDOW_BITS`].
    pub committed_window: u128,
    pub session: u64,
    pub reply: Vec<u8>,
}

/// Width of the per-entry committed-request window below the watermark.
///
/// A client that pipelines writes can see one of them refused transiently and
/// replay it after later ids have committed, so "at or below the watermark"
/// alone would absorb that replay as a duplicate and lose the write. The window
/// records which ids under the watermark this replica saw commit; an unmarked
/// one inside it executes, while one that has aged out below it reads as
/// [`IggyError::RequestTooOld`] because its outcome is unknown.
///
/// The width is in the CLIENT's request-id space, not in this group's writes:
/// `ConsensusSession` mints from one counter across every partition, stream and
/// metadata op, so a slice only ever sees the subset of those ids routed to it.
/// Coverage in a client's own writes to one group is this width divided by the
/// number of groups it interleaves, so 128 ids is around 16 writes per group
/// across 8 partitions, and a replay held back longer than that can be refused
/// with an unknown outcome. A wider bitmap divides by the same fanout: closing
/// the gap needs per-group request numbering, which waits on the clients-table follow-up.
pub const COMMITTED_WINDOW_BITS: u64 = 128;

/// VSR client table: per-session fence epoch + request-watermark dedup.
///
/// Fixed-size slot array (source of truth) + `HashMap` index (O(1) lookup).
///
/// Metadata registration stores an immutable principal, epoch and bind
/// verifier. Matching login retries resolve that same registration. Explicit
/// logout or lease expiry ends it; ordered retirement in every partition group
/// precedes removal. Capacity pressure never evicts a live registration.
///
/// Partition slices retain session-qualified receipts, committed windows
/// and exact results. The first committed prepare fixes the group's capacity.
/// Pending queues reserve slots before admission. Apply, checkpoint recovery and
/// state transfer preserve that committed capacity and all live protection.
///
/// Metadata checkpoints retain the latest result and session tombstones.
/// Partition receipt checkpoints publish protection before WAL reclamation.
/// Missing or malformed referenced protection is refused, never replaced by an
/// empty table beside recovered data.
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
    /// `None` = free slot. Deterministic iteration for serialization.
    ///
    /// Under [`ClientTableMode::preallocate_slots`] this is sized to
    /// `clients_max` at construction; otherwise it grows to that cap on demand.
    /// Every `Some` has exactly one `index` entry, so `index.len()` is the
    /// occupied count.
    slots: Vec<Option<ClientEntry>>,
    /// `client_id` -> slot index. Rebuilt on decode.
    index: HashMap<u128, usize>,
    /// Slot ceiling. Tracked explicitly because `slots.len()` is the allocated
    /// length, which only equals the cap when slots are preallocated.
    clients_max: usize,
    capacity_committed: bool,
    mode: ClientTableMode,
}

impl ClientTable {
    /// `max_clients` caps slots; index pre-sized to avoid rehash storms.
    #[must_use]
    pub fn new(max_clients: usize) -> Self {
        Self::with_mode(max_clients, ClientTableMode::Metadata)
    }

    /// `max_clients` caps slots; `mode` selects which mechanisms run.
    #[must_use]
    pub fn with_mode(max_clients: usize, mode: ClientTableMode) -> Self {
        let (slots, index) = if mode.preallocate_slots() {
            let mut slots = Vec::with_capacity(max_clients);
            slots.resize_with(max_clients, || None);
            (slots, HashMap::with_capacity(max_clients))
        } else {
            (Vec::new(), HashMap::new())
        };
        Self {
            slots,
            index,
            clients_max: max_clients,
            capacity_committed: false,
            mode,
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
        if self.capacity_committed {
            return;
        }
        assert!(
            self.index.is_empty(),
            "set_capacity must run before any client registers"
        );
        *self = Self::with_mode(max_clients, self.mode);
    }

    #[must_use]
    pub const fn capacity_committed(&self) -> bool {
        self.capacity_committed
    }

    #[must_use]
    pub fn contains(&self, client_id: u128) -> bool {
        self.index.contains_key(&client_id)
    }

    /// Verify a binding credential, resolving a zero epoch to its original registration.
    ///
    /// # Errors
    /// Returns `TransientNotAccepted` while an uncertain registration is not visible,
    /// `Unauthenticated` for an absent known session, bad proof or ended session,
    /// and `InvalidSession` for a different nonzero epoch.
    pub fn bind_session(
        &mut self,
        client_id: u128,
        session: u64,
        secret: &[u8; BIND_SECRET_BYTES],
    ) -> Result<(u32, u64, SessionAttachment), IggyError> {
        let missing = if session == 0 {
            IggyError::TransientNotAccepted
        } else {
            IggyError::Unauthenticated
        };
        let user_id = self.get_user_id(client_id).ok_or(missing)?;
        let epoch = self
            .registered_session(
                client_id,
                user_id,
                bind_verifier(client_id, user_id, secret),
            )?
            .ok_or(IggyError::Unauthenticated)?;
        if session != 0 && session != epoch {
            return Err(IggyError::InvalidSession(session));
        }
        let attachment = self
            .attach_session(client_id, epoch, user_id)
            .ok_or(IggyError::Unauthenticated)?;
        Ok((user_id, epoch, attachment))
    }

    /// Resolve an idempotent registration for the same principal and verifier.
    ///
    /// # Errors
    /// Returns `Unauthenticated` when a registered identity has different credentials.
    ///
    /// # Panics
    /// Only if the private index refers to an unoccupied slot.
    pub fn registered_session(
        &self,
        client_id: u128,
        user_id: u32,
        verifier: [u8; BIND_SECRET_BYTES],
    ) -> Result<Option<u64>, IggyError> {
        let Some(&slot) = self.index.get(&client_id) else {
            return Ok(None);
        };
        let entry = self.slots[slot].as_ref().expect("index/slot mismatch");
        if entry.user_id != user_id
            || entry.ended_op.is_some()
            || blake3::Hash::from_bytes(entry.bind_verifier) != blake3::Hash::from_bytes(verifier)
        {
            return Err(IggyError::Unauthenticated);
        }
        Ok(Some(entry.epoch))
    }

    pub fn end_session(
        &mut self,
        client_id: u128,
        user_id: u32,
        session: u64,
        ended_op: u64,
    ) -> bool {
        if let Some(&slot) = self.index.get(&client_id)
            && let Some(entry) = &mut self.slots[slot]
            && entry.user_id == user_id
            && entry.epoch == session
            && entry.ended_op.is_none()
        {
            entry.ended_op = Some(ended_op);
            entry.attachment = None;
            return true;
        }
        false
    }

    /// Retain the exact Logout result before ending its session.
    ///
    /// # Errors
    /// Returns an error if a new Logout receipt cannot be retained.
    ///
    /// # Panics
    /// Only if the private index refers to an unoccupied slot.
    pub fn commit_logout(
        &mut self,
        client_id: u128,
        user_id: u32,
        session: u64,
        reply: Message<ReplyHeader>,
    ) -> Result<bool, ClientTableWireError> {
        let Some(&slot) = self.index.get(&client_id) else {
            return Ok(false);
        };
        let entry = self.slots[slot].as_ref().expect("index/slot mismatch");
        if entry.epoch != session
            || entry.user_id != user_id
            || entry.ended_op.is_some()
            || (reply.header().request <= entry.watermark
                && reply.header().request != EXPIRED_SESSION_REQUEST_ID)
        {
            return Ok(false);
        }
        let ended_op = reply.header().commit;
        if self.commit_reply(client_id, user_id, reply) != CommitReply::Cached {
            return Err(ClientTableWireError::InvalidWatermark { client_id });
        }
        Ok(self.end_session(client_id, user_id, session, ended_op))
    }

    pub fn ended_sessions(
        &self,
    ) -> impl Iterator<Item = iggy_binary_protocol::requests::system::SessionIdentity> + '_ {
        self.slots.iter().flatten().filter_map(|entry| {
            entry.ended_op.map(
                |ended_op| iggy_binary_protocol::requests::system::SessionIdentity {
                    client_id: entry.client_id,
                    session: entry.epoch,
                    metadata_watermark: ended_op,
                },
            )
        })
    }

    pub fn forget_session(&mut self, client_id: u128, session: u64) -> bool {
        let Some(&slot) = self.index.get(&client_id) else {
            return false;
        };
        if self.slots[slot]
            .as_ref()
            .is_none_or(|entry| entry.epoch != session)
        {
            return false;
        }
        self.index.remove(&client_id);
        self.slots[slot] = None;
        true
    }

    pub fn finalize_session(
        &mut self,
        identity: iggy_binary_protocol::requests::system::SessionIdentity,
    ) -> bool {
        if !self.ended_sessions().any(|ended| ended == identity) {
            return false;
        }
        self.forget_session(identity.client_id, identity.session)
    }

    /// Apply the limit carried by the ordered log, independent of local configuration.
    ///
    /// # Errors
    /// Refuses an invalid limit, a changed committed limit, or one below live protection.
    pub fn commit_capacity(&mut self, capacity: usize) -> Result<(), ClientTableWireError> {
        if capacity == 0
            || capacity > CLIENTS_TABLE_SLOT_MAX
            || self.count() > capacity
            || (self.capacity_committed && self.clients_max != capacity)
        {
            return Err(ClientTableWireError::InvalidCapacity { capacity });
        }
        self.clients_max = capacity;
        if self.mode.preallocate_slots() {
            self.slots.resize_with(capacity, || None);
        }
        self.capacity_committed = true;
        Ok(())
    }

    /// Preserve committed capacity, live sessions, tombstones, and the latest result.
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
                        bind_verifier: entry.bind_verifier,
                        ended_op: entry.ended_op,
                        client_id: entry.client_id,
                        epoch: entry.epoch,
                        user_id: entry.user_id,
                        watermark: entry.watermark,
                        reply: entry.latest().as_bytes().to_vec(),
                    },
                ))
            })
            .collect();
        ClientTableSnapshot {
            capacity: self.clients_max,
            slots,
        }
    }

    /// Restore the committed limit without consulting local configuration.
    ///
    /// # Errors
    /// Refuses invalid slots, duplicate identities, and malformed receipts.
    pub fn from_snapshot(snapshot: ClientTableSnapshot) -> Result<Self, ClientTableDecodeError> {
        // Bound the capacity on a slot index read off disk before allocating from it,
        // as the superblock and WAL do with their length fields.
        let capacity = snapshot.capacity;
        if capacity == 0 || capacity > CLIENTS_TABLE_SLOT_MAX {
            return Err(ClientTableDecodeError::SlotOutOfRange {
                slot: capacity,
                max: CLIENTS_TABLE_SLOT_MAX,
            });
        }
        for (slot_idx, _) in &snapshot.slots {
            let slot = *slot_idx as usize;
            if slot >= capacity {
                return Err(ClientTableDecodeError::SlotOutOfRange {
                    slot,
                    max: capacity,
                });
            }
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
            ring.push_back(CachedReply::from(reply));
            // Two entries claiming one slot would silently drop the first, leaving it
            // indexed but pointing at another client's state.
            if slots[slot_idx].is_some() {
                return Err(ClientTableDecodeError::DuplicateSlot { slot: slot_idx });
            }
            let restored = ClientEntry {
                bind_verifier: entry.bind_verifier,
                ended_op: entry.ended_op,
                epoch: entry.epoch,
                attachment: None,
                user_id: entry.user_id,
                watermark: entry.watermark,
                committed_window: 0,
                ring,
                client_id: entry.client_id,
                latest_commit,
            };
            if !restored.valid_metadata_protection() {
                return Err(ClientTableDecodeError::InvalidEntry { slot: slot_idx });
            }
            slots[slot_idx] = Some(restored);
        }
        let clients_max = slots.len();
        let table = Self {
            slots,
            index,
            clients_max,
            capacity_committed: true,
            mode: ClientTableMode::Metadata,
        };
        Ok(table)
    }

    /// Check a request against the table. Epoch fence first, then the
    /// watermark. Register does not come through here: every bind proposes
    /// unconditionally so its fence actually moves, see
    /// [`Self::commit_register`].
    ///
    /// The first committed result wins even when a retry changes its body.
    /// Reusing an ID for another operation cannot replay that result.
    ///
    /// # Panics
    /// If index points to empty slot (invariant violation).
    #[must_use]
    pub fn check_request(
        &self,
        client_id: u128,
        epoch: u64,
        request: u64,
        operation: Operation,
    ) -> RequestStatus {
        assert!(
            client_id != RESERVED_CLIENT_ID,
            "client_id 0 is reserved for internal use"
        );
        // Header validation guarantees both > 0 at wire layer.
        debug_assert!(
            epoch > 0 || !self.mode.fence_epoch(),
            "check_request: epoch must be > 0 when fencing"
        );
        debug_assert!(request > 0, "check_request: request must be > 0");

        // Epoch check before request: a fenced zombie must be rejected even
        // if its request number would read as a clean duplicate.
        let Some(&slot_idx) = self.index.get(&client_id) else {
            return RequestStatus::NoSession;
        };
        let entry = self.slots[slot_idx].as_ref().expect("index/slot mismatch");

        if entry.ended_op.is_some() {
            if epoch == entry.epoch
                && let Some(cached) = entry.find_cached(request)
                && cached.header().operation == iggy_binary_protocol::Operation::Logout
            {
                return if cached.header().operation.request_operation()
                    == operation.request_operation()
                {
                    RequestStatus::Duplicate(cached.clone())
                } else {
                    RequestStatus::OperationMismatch { request }
                };
            }
            return RequestStatus::NoSession;
        }

        // A plane with no register mints no epoch, so there is nothing to
        // fence against and the presented value is ignored.
        if self.mode.fence_epoch() {
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
        }

        if request > entry.watermark {
            return RequestStatus::New;
        }

        match entry.find_cached(request) {
            Some(cached)
                if cached.header().operation.request_operation()
                    != operation.request_operation() =>
            {
                RequestStatus::OperationMismatch { request }
            }
            Some(cached) => RequestStatus::Duplicate(cached.clone()),
            None => RequestStatus::AlreadyApplied {
                request,
                watermark: entry.watermark,
            },
        }
    }

    /// Register once. Matching login retries preserve the original epoch and history.
    ///
    /// # Errors
    /// Refuses conflicting ownership, ended identities, and exhausted capacity.
    pub fn commit_register(
        &mut self,
        client_id: u128,
        user_id: u32,
        bind_verifier: [u8; BIND_SECRET_BYTES],
        reply: Message<ReplyHeader>,
    ) -> Result<(), ClientTableWireError> {
        if client_id == 0
            || client_id != reply.header().client
            || reply.header().commit == 0
            || reply.header().request != REGISTER_REQUEST_ID
            || reply.header().operation != iggy_binary_protocol::Operation::Register
        {
            return Err(ClientTableWireError::InvalidReply);
        }
        if let Some(&slot) = self.index.get(&client_id) {
            let entry = self.slots[slot]
                .as_ref()
                .ok_or(ClientTableWireError::InvalidWatermark { client_id })?;
            return if entry.user_id == user_id
                && entry.bind_verifier == bind_verifier
                && entry.ended_op.is_none()
            {
                Ok(())
            } else {
                Err(ClientTableWireError::InvalidWatermark { client_id })
            };
        }
        if self.index.len() >= self.clients_max {
            return Err(ClientTableWireError::TooManyEntries {
                count: u32::try_from(self.count() + 1).unwrap_or(u32::MAX),
                max: self.clients_max,
            });
        }
        let slot = self
            .first_free_slot()
            .ok_or(ClientTableWireError::InvalidCapacity {
                capacity: self.clients_max,
            })?;
        let epoch = reply.header().commit;
        let mut ring = VecDeque::with_capacity(REPLY_RING_CAPACITY);
        ring.push_back(CachedReply::from(reply));
        self.slots[slot] = Some(ClientEntry {
            bind_verifier,
            ended_op: None,
            epoch,
            attachment: None,
            user_id,
            client_id,
            latest_commit: epoch,
            watermark: REGISTER_REQUEST_ID,
            committed_window: 0,
            ring,
        });
        self.index.insert(client_id, slot);
        Ok(())
    }

    /// Retain a committed metadata result before advancing the applied frontier.
    /// Missing ownership or regressing protection is a commit failure.
    /// Server-originated operations return [`CommitReply::NoEntry`].
    /// # Panics
    /// If the client id differs from the reply or the private index is inconsistent.
    pub fn commit_reply(
        &mut self,
        client_id: u128,
        user_id: u32,
        reply: Message<ReplyHeader>,
    ) -> CommitReply {
        let new_header = reply.header();
        let new_client = new_header.client;
        let new_request = new_header.request;
        let new_commit = new_header.commit;
        assert_eq!(
            client_id, new_client,
            "commit_reply: client_id mismatch (arg={client_id}, header={new_client})",
        );
        // Ahead of the register guard below, which the default `request` 0 of
        // a server-originated header would trip.
        if client_id == RESERVED_CLIENT_ID {
            return CommitReply::NoEntry;
        }
        debug_assert!(
            new_request > REGISTER_REQUEST_ID,
            "commit_reply: register replies go through commit_register"
        );

        let Some(&slot_idx) = self.index.get(&client_id) else {
            return CommitReply::NoEntry;
        };

        let entry = self.slots[slot_idx].as_mut().expect("index/slot mismatch");
        if entry.user_id != user_id {
            return CommitReply::NoEntry;
        }
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
        let cached = CachedReply::from(reply);
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
                entry.latest_commit = entry.latest().header().commit;
            } else {
                entry.push_latest(cached);
            }
        } else {
            entry.push_latest(cached);
            entry.watermark = new_request;
        }
        CommitReply::Cached
    }

    #[must_use]
    /// # Panics
    /// Only if the private index refers to an unoccupied slot.
    pub fn check_partition_request(
        &self,
        client_id: u128,
        user_id: u32,
        session: u64,
        request: u64,
        operation: Operation,
    ) -> RequestStatus {
        let Some(&slot) = self.index.get(&client_id) else {
            return RequestStatus::New;
        };
        let entry = self.slots[slot].as_ref().expect("index/slot mismatch");
        if entry.user_id != user_id || entry.epoch != session {
            return RequestStatus::Fenced {
                current: entry.epoch,
                received: session,
            };
        }
        match entry.check_slice_request(request) {
            SliceRequestStatus::New => RequestStatus::New,
            SliceRequestStatus::Committed => match entry.find_cached(request) {
                Some(reply) if reply.header().operation != operation => {
                    RequestStatus::OperationMismatch { request }
                }
                Some(reply) => RequestStatus::Duplicate(reply.clone()),
                None => RequestStatus::AlreadyApplied {
                    request,
                    watermark: entry.watermark,
                },
            },
            SliceRequestStatus::AgedOut => RequestStatus::AlreadyApplied {
                request,
                watermark: entry.watermark,
            },
        }
    }

    /// Retain a partition receipt and return a shared handle to its first committed result.
    ///
    /// # Errors
    /// Refuses invalid or inconsistent receipts and exhausted protection capacity.
    ///
    /// # Panics
    /// Only if the private index refers to an unoccupied slot.
    pub fn commit_partition_reply(
        &mut self,
        user_id: u32,
        session: u64,
        reply: Message<ReplyHeader>,
    ) -> Result<CachedReply, ClientTableWireError> {
        let header = *reply.header();
        if header.client == 0
            || session == 0
            || header.commit == 0
            || !is_partition_receipt_operation(header.operation)
        {
            return Err(ClientTableWireError::InvalidReply);
        }
        if let Some(&slot) = self.index.get(&header.client)
            && self.slots[slot]
                .as_ref()
                .is_some_and(|entry| entry.epoch != session)
        {
            return Err(ClientTableWireError::InvalidWatermark {
                client_id: header.client,
            });
        }
        self.commit_request(header.client, user_id, header.request, header.commit)?;
        let slot = self.index[&header.client];
        let entry = self.slots[slot].as_mut().expect("index/slot mismatch");
        entry.epoch = session;
        if let Some(cached) = entry.find_cached(header.request) {
            return Ok(cached.clone());
        }
        let cached = CachedReply::from(reply);
        entry.ring.clear();
        entry.latest_commit = cached.header().commit;
        entry.ring.push_back(cached.clone());
        Ok(cached)
    }

    /// Fold a partition receipt's identity into its committed request window.
    ///
    /// Replay is idempotent and order-insensitive within the retained window.
    /// The first committed request fixes its principal. Entries are never
    /// replaced or evicted to admit another client. Explicit writes use
    /// [`Self::commit_partition_reply`] to retain their original result.
    ///
    /// # Panics
    /// If called on a table whose mode caches replies -- that plane must go
    /// through [`Self::commit_reply`] so the ring stays populated.
    /// # Errors
    /// Returns an error for zero identities, a changed principal, or exhausted
    /// committed protection capacity.
    pub(crate) fn commit_request(
        &mut self,
        client_id: u128,
        user_id: u32,
        request: u64,
        commit_op: u64,
    ) -> Result<(), ClientTableWireError> {
        debug_assert!(
            !self.mode.cache_replies(),
            "commit_request: a reply-caching table must use commit_reply"
        );
        if client_id == 0 {
            return Err(ClientTableWireError::InvalidWatermark { client_id });
        }

        if let Some(&slot_idx) = self.index.get(&client_id) {
            let entry = self.slots[slot_idx].as_mut().expect("index/slot mismatch");
            if entry.user_id != user_id {
                return Err(ClientTableWireError::InvalidWatermark { client_id });
            } else if request > entry.watermark {
                let gap = request - entry.watermark;
                entry.committed_window = if gap >= COMMITTED_WINDOW_BITS {
                    1
                } else {
                    (entry.committed_window << gap) | 1
                };
                entry.watermark = request;
                entry.latest_commit = commit_op;
            } else {
                let below = entry.watermark - request;
                if below < COMMITTED_WINDOW_BITS && entry.committed_window & (1 << below) == 0 {
                    entry.committed_window |= 1 << below;
                    // Commits walk in op order, so a newly folded reordered id
                    // is the newest commit unless an install re-walk replays an
                    // older one.
                    entry.latest_commit = entry.latest_commit.max(commit_op);
                }
            }
            return Ok(());
        }

        if self.index.len() >= self.clients_max {
            return Err(ClientTableWireError::TooManyEntries {
                count: u32::try_from(self.index.len() + 1).unwrap_or(u32::MAX),
                max: self.clients_max,
            });
        }
        let Some(slot_idx) = self.first_free_slot() else {
            return Err(ClientTableWireError::InvalidCapacity {
                capacity: self.clients_max,
            });
        };
        self.index.insert(client_id, slot_idx);
        self.slots[slot_idx] = Some(ClientEntry::watermark_only(
            client_id, user_id, request, 1, commit_op,
        ));
        Ok(())
    }

    /// Replace partition protection atomically. Invalid protection leaves the
    /// existing table intact; a smaller local limit cannot discard live receipts.
    ///
    /// # Errors
    /// Refuses duplicate identities, invalid capacity, inconsistent windows, or receipts.
    #[allow(clippy::suspicious_operation_groupings)]
    pub fn install_watermarks(
        &mut self,
        capacity: usize,
        entries: impl IntoIterator<Item = DedupWatermark>,
    ) -> Result<(), ClientTableWireError> {
        debug_assert!(
            !self.mode.cache_replies(),
            "install_watermarks: a reply-caching table installs via decode"
        );
        let mut replacement = Self::with_mode(capacity, self.mode);
        replacement.commit_capacity(capacity)?;
        for entry in entries {
            let slot = replacement.slots.len();
            if slot >= capacity {
                return Err(ClientTableWireError::TooManyEntries {
                    count: u32::try_from(slot + 1).unwrap_or(u32::MAX),
                    max: capacity,
                });
            }
            if entry.client == 0 || entry.session == 0 || entry.committed_window & 1 == 0 {
                return Err(ClientTableWireError::InvalidWatermark {
                    client_id: entry.client,
                });
            }
            if replacement.index.insert(entry.client, slot).is_some() {
                return Err(ClientTableWireError::DuplicateClientId {
                    slot,
                    client_id: entry.client,
                });
            }
            let mut restored = ClientEntry::watermark_only(
                entry.client,
                entry.user_id,
                entry.watermark,
                entry.committed_window,
                entry.latest_commit,
            );
            restored.epoch = entry.session;
            if entry.reply.is_empty() {
                return Err(ClientTableWireError::EmptyRing);
            }
            {
                let reply = Message::<ReplyHeader>::try_from(
                    Owned::<MESSAGE_ALIGN>::copy_from_slice(&entry.reply),
                )
                .map_err(|_| ClientTableWireError::InvalidReply)?;
                let header = reply.header();
                if header.client != entry.client
                    || header.request > entry.watermark
                    || entry.watermark - header.request >= COMMITTED_WINDOW_BITS
                    || entry.committed_window & (1_u128 << (entry.watermark - header.request)) == 0
                    || header.commit != entry.latest_commit
                    || header.commit == 0
                    || header.size as usize != entry.reply.len()
                    || !is_partition_receipt_operation(header.operation)
                {
                    return Err(ClientTableWireError::InvalidReply);
                }
                restored.ring.push_back(CachedReply::from(reply));
            }
            replacement.slots.push(Some(restored));
        }
        *self = replacement;
        Ok(())
    }

    /// Every entry ascending by client: the deterministic form a wire encoding
    /// needs.
    #[must_use]
    pub fn watermarks_sorted(&self) -> Vec<DedupWatermark> {
        let mut entries: Vec<DedupWatermark> = self
            .slots
            .iter()
            .flatten()
            .map(|entry| DedupWatermark {
                client: entry.client_id,
                user_id: entry.user_id,
                watermark: entry.watermark,
                latest_commit: entry.latest_commit,
                committed_window: entry.committed_window,
                session: entry.epoch,
                reply: entry
                    .ring
                    .back()
                    .map_or_else(Vec::new, |reply| reply.as_bytes().to_vec()),
            })
            .collect();
        entries.sort_unstable_by_key(|entry| entry.client);
        entries
    }

    /// Resolve ownership for an exact epoch, including a retained Logout replay.
    #[must_use]
    pub fn user_id_for_session(&self, client_id: u128, session: u64) -> Option<u32> {
        let &slot = self.index.get(&client_id)?;
        let entry = self.slots[slot].as_ref()?;
        (entry.epoch == session).then_some(entry.user_id)
    }

    /// Lowest free slot, growing the array when slots are allocated lazily.
    /// Assignment is identical to the preallocated case: both hand out the
    /// lowest free index, so slot identities and wire encoding do not
    /// depend on the mode.
    ///
    /// The hole scan runs only when a hole exists (`index.len()` is the
    /// occupied count): a lazily grown table below its cap has none, and
    /// scanning it before every push would make the fill quadratic on the
    /// commit path.
    fn first_free_slot(&mut self) -> Option<usize> {
        if self.index.len() < self.slots.len()
            && let Some(index) = self.slots.iter().position(Option::is_none)
        {
            return Some(index);
        }
        (self.slots.len() < self.clients_max).then(|| {
            self.slots.push(None);
            self.slots.len() - 1
        })
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
        self.slots[slot_idx]
            .as_ref()
            .filter(|entry| entry.ended_op.is_none())
            .map(|entry| entry.epoch)
    }

    /// Attach only after the caller has authenticated `user_id` and waited
    /// for the local metadata frontier to cover the requested session.
    /// The registered user owns the session, matching authenticated login
    /// resume. Client ids and epochs are identifiers, not authentication secrets.
    pub fn attach_session(
        &mut self,
        client_id: u128,
        session: u64,
        user_id: u32,
    ) -> Option<SessionAttachment> {
        let &slot_idx = self.index.get(&client_id)?;
        let entry = self.slots[slot_idx].as_mut()?;
        if entry.epoch != session
            || entry.user_id != user_id
            || session == 0
            || entry.ended_op.is_some()
        {
            return None;
        }
        let attachment = entry.attachment.get_or_insert_with(|| Arc::new(()));
        Some(SessionAttachment {
            session: Arc::downgrade(attachment),
        })
    }

    /// Every registered client id, in slot order.
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
    ChecksumMismatch {
        expected: u64,
        actual: u64,
    },
    /// Encoded entry count exceeds [`CLIENTS_TABLE_SLOT_MAX`], the allocation
    /// ceiling no valid table can reach.
    TooManyEntries {
        count: u32,
        max: usize,
    },
    /// A cached reply's bytes do not parse as a valid reply message.
    InvalidReply,
    InvalidWatermark {
        client_id: u128,
    },
    InvalidCapacity {
        capacity: usize,
    },
    /// An entry carries an empty reply ring (violates the never-empty
    /// invariant registration establishes).
    EmptyRing,
    /// Two entries claim the same `client_id`. Indexing them would leave one
    /// slot occupied but unindexed, which desynchronizes the capacity check in
    /// [`ClientTable::commit_register`] from the actual occupancy.
    DuplicateClientId {
        slot: usize,
        client_id: u128,
    },
    /// A reply ring longer than [`REPLY_RING_CAPACITY`], which is every reply
    /// `transferable_replies` ever writes. `encode` writes the length as a
    /// `u8`, and a peer that sends more replies than this crate transfers is
    /// reporting state this one cannot have produced.
    RingTooLong {
        slot: usize,
        len: u8,
        max: usize,
    },
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
            Self::InvalidWatermark { client_id } => {
                write!(f, "invalid committed watermark for client {client_id}")
            }
            Self::InvalidCapacity { capacity } => write!(
                f,
                "invalid or conflicting committed retry capacity {capacity}"
            ),
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
pub const CLIENT_TABLE_MAGIC: [u8; 4] = *b"ICT5";

/// Per-entry fixed fields in the wire encoding: `client(u128) epoch(u64)
/// user_id(u32) watermark(u64) bind_verifier([u8; 32]) ended_op(u64) ring_len(u8)`.
const ENCODED_ENTRY_FIXED_LEN: usize = size_of::<u128>()
    + size_of::<u64>()
    + size_of::<u32>()
    + size_of::<u64>()
    + size_of::<u8>()
    + BIND_SECRET_BYTES
    + size_of::<u64>();

impl ClientTable {
    /// Encode committed capacity and session protection for state transfer.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub fn encode(&self) -> Vec<u8> {
        // Size exactly rather than guess: each cached reply is a full wire
        // message, so at the default client cap a guessed reservation is off by
        // orders of magnitude and costs several reallocs of a multi-MB buffer
        // on the serving primary's pump, once per offer build.
        let entries = self.slots.iter().flatten();
        let reserved = CLIENT_TABLE_MAGIC.len()
            + 2 * size_of::<u32>()
            + entries
                .map(|entry| {
                    ENCODED_ENTRY_FIXED_LEN
                        + entry
                            .transferable_replies()
                            .map(|reply| size_of::<u32>() + reply.bytes.len())
                            .sum::<usize>()
                })
                .sum::<usize>()
            + size_of::<u64>();
        let mut out = Vec::with_capacity(reserved);
        out.extend_from_slice(&CLIENT_TABLE_MAGIC);
        out.extend_from_slice(&(self.clients_max as u32).to_le_bytes());
        out.extend_from_slice(&(self.index.len() as u32).to_le_bytes());
        for (slot_idx, slot) in self.slots.iter().enumerate() {
            let Some(entry) = slot else { continue };
            debug_assert_eq!(self.index.get(&entry.client_id), Some(&slot_idx));
            out.extend_from_slice(&entry.client_id.to_le_bytes());
            out.extend_from_slice(&entry.epoch.to_le_bytes());
            out.extend_from_slice(&entry.user_id.to_le_bytes());
            out.extend_from_slice(&entry.watermark.to_le_bytes());
            out.extend_from_slice(&entry.bind_verifier);
            out.extend_from_slice(&entry.ended_op.unwrap_or(0).to_le_bytes());
            out.push(entry.transferable_replies().count() as u8);
            for reply in entry.transferable_replies() {
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

    /// Decode the current format, retaining its committed capacity.
    ///
    /// # Errors
    /// Refuses corrupt, truncated, unsupported, or internally inconsistent protection.
    pub fn decode(bytes: &[u8]) -> Result<Self, ClientTableWireError> {
        let content = split_verified_trailer(bytes).map_err(|mismatch| match mismatch {
            Some((expected, actual)) => ClientTableWireError::ChecksumMismatch { expected, actual },
            None => ClientTableWireError::Truncated,
        })?;

        let mut reader = LeCursor::new(content);
        let magic = reader.take(CLIENT_TABLE_MAGIC.len())?;
        if magic != CLIENT_TABLE_MAGIC {
            return Err(ClientTableWireError::BadMagic);
        }
        let capacity = reader.u32()? as usize;
        let count = reader.u32()?;
        // Bound the allocation on the same slot ceiling `from_snapshot` uses;
        // `count` is a peer-supplied u32 and this is the only check between it
        // and `Self::new`'s eager Vec resize.
        if count as usize > capacity || capacity > CLIENTS_TABLE_SLOT_MAX {
            return Err(ClientTableWireError::TooManyEntries {
                count,
                max: capacity.min(CLIENTS_TABLE_SLOT_MAX),
            });
        }

        let mut table = Self::new(capacity);
        table.commit_capacity(capacity)?;
        for slot_idx in 0..count as usize {
            let client_id = reader.u128()?;
            let epoch = reader.u64()?;
            let user_id = reader.u32()?;
            let watermark = reader.u64()?;
            let bind_verifier = reader
                .take(BIND_SECRET_BYTES)?
                .try_into()
                .map_err(|_| ClientTableWireError::Truncated)?;
            let ended_op = match reader.u64()? {
                0 => None,
                op => Some(op),
            };
            let ring_len = reader.u8()?;
            if ring_len == 0 {
                return Err(ClientTableWireError::EmptyRing);
            }
            // The artifact checksum only proves the bytes survived transit; it
            // says nothing about the peer that computed them. Bounding at what
            // `transferable_replies` emits keeps an installed ring inside the
            // unconditional floor, so it satisfies `trim_ring`'s byte budget on
            // arrival and needs no trimming of its own.
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
                ring.push_back(CachedReply::from(message));
            }
            let latest_commit = ring
                .back()
                .ok_or(ClientTableWireError::EmptyRing)?
                .header()
                .commit;
            let restored = ClientEntry {
                bind_verifier,
                ended_op,
                epoch,
                attachment: None,
                user_id,
                watermark,
                committed_window: 0,
                ring,
                client_id,
                latest_commit,
            };
            if !restored.valid_metadata_protection() {
                return Err(ClientTableWireError::InvalidWatermark { client_id });
            }
            table.slots[slot_idx] = Some(restored);
            // Reject rather than overwrite, as the checkpoint decoder does. An
            // overwrite would leave an occupied slot outside the index, breaking
            // capacity accounting and session lookup.
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

    /// Protection slots fixed by the first committed prepare's capacity.
    /// Recovery and transfer preserve that limit over local configuration.
    #[must_use]
    pub const fn capacity(&self) -> usize {
        self.clients_max
    }
}

impl ClientEntry {
    /// Partition identity initialized before its session and receipt are installed.
    /// Bit 0 is always set because the watermark itself is committed.
    const fn watermark_only(
        client_id: u128,
        user_id: u32,
        watermark: u64,
        committed_window: u128,
        commit_op: u64,
    ) -> Self {
        Self {
            bind_verifier: [0; 32],
            ended_op: None,
            epoch: 0,
            attachment: None,
            user_id,
            watermark,
            committed_window: committed_window | 1,
            ring: VecDeque::new(),
            client_id,
            latest_commit: commit_op,
        }
    }

    const fn check_slice_request(&self, request: u64) -> SliceRequestStatus {
        if request > self.watermark {
            return SliceRequestStatus::New;
        }
        let below = self.watermark - request;
        if below >= COMMITTED_WINDOW_BITS {
            return SliceRequestStatus::AgedOut;
        }
        if self.committed_window & (1 << below) == 0 {
            SliceRequestStatus::New
        } else {
            SliceRequestStatus::Committed
        }
    }

    fn valid_metadata_protection(&self) -> bool {
        if self.client_id == 0 || self.epoch == 0 || self.latest_commit < self.epoch {
            return false;
        }
        let Some(latest) = self.ring.back() else {
            return false;
        };
        let header = latest.header();
        if header.request != self.watermark
            || header.commit != self.latest_commit
            || self.ended_op.is_some_and(|ended| {
                ended != self.latest_commit
                    || header.operation != iggy_binary_protocol::Operation::Logout
            })
        {
            return false;
        }
        let mut previous = None;
        for receipt in &self.ring {
            let header = receipt.header();
            if header.client != self.client_id
                || header.commit < self.epoch
                || header.request > self.watermark
                || (header.request == REGISTER_REQUEST_ID
                    && (header.operation != iggy_binary_protocol::Operation::Register
                        || header.commit != self.epoch))
                || (header.request != REGISTER_REQUEST_ID
                    && header.operation == iggy_binary_protocol::Operation::Register)
                || previous.is_some_and(|(request, commit)| {
                    header.request <= request || header.commit <= commit
                })
            {
                return false;
            }
            previous = Some((header.request, header.commit));
        }
        true
    }

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

    /// The newest replies a state transfer carries, oldest first.
    ///
    /// Capped at [`REPLY_RING_CAPACITY`] rather than shipping whatever
    /// retention holds locally: an artifact a recovering node has to fetch is
    /// worth keeping small, and the deeper history rebuilds itself from the
    /// receiver's own commits. The cost is a late retrier's result bytes on the
    /// transferred node, never its at-most-once fence, which is the bound
    /// [`REPLY_RING_CAPACITY`] documents.
    fn transferable_replies(&self) -> impl Iterator<Item = &CachedReply> {
        self.ring
            .iter()
            .skip(self.ring.len().saturating_sub(REPLY_RING_CAPACITY))
    }

    /// Cached reply whose `request` matches (scan order is irrelevant
    /// because request numbers in the ring are unique).
    fn find_cached(&self, request: u64) -> Option<&CachedReply> {
        self.ring
            .iter()
            .find(|cached| cached.header().request == request)
    }

    /// Push the newest committed reply, drop the oldest ones retention no
    /// longer covers, and refresh the denormalized `latest_commit`.
    fn push_latest(&mut self, cached: CachedReply) {
        self.latest_commit = cached.header().commit;
        self.ring.push_back(cached);
        self.trim_ring();
    }

    /// Drop the oldest replies once the entry holds more than
    /// [`REPLY_RING_CAPACITY`] and exceeds [`REPLY_RING_RETENTION_BYTES`].
    ///
    /// The total is summed here rather than denormalized onto the entry: no
    /// reply is shorter than a header, so the budget holds the ring to 32
    /// entries and this is a bounded walk of buffer lengths with no header
    /// casts, and it leaves no running total for the sites that write the ring
    /// to drift out of sync with.
    fn trim_ring(&mut self) {
        let mut bytes: usize = self.ring.iter().map(CachedReply::byte_len).sum();
        while self.ring.len() > REPLY_RING_CAPACITY && bytes > REPLY_RING_RETENTION_BYTES {
            let dropped = self.ring.pop_front().expect("length checked above");
            bytes -= dropped.byte_len();
        }
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

    /// A reply heavy enough that two of them exhaust
    /// [`REPLY_RING_RETENTION_BYTES`], so only the floor keeps it cached.
    #[allow(clippy::cast_possible_truncation)]
    fn make_big_reply(client: u128, request: u64, commit: u64) -> Message<ReplyHeader> {
        let header_size = std::mem::size_of::<ReplyHeader>();
        let size = header_size + REPLY_RING_RETENTION_BYTES;
        let mut msg = Message::<ReplyHeader>::new(size);
        let header = bytemuck::checked::try_from_bytes_mut::<ReplyHeader>(
            &mut msg.as_mut_slice()[..header_size],
        )
        .expect("zeroed bytes are valid");
        *header = ReplyHeader {
            client,
            request,
            commit,
            size: size as u32,
            command: Command::Reply,
            operation: Operation::SendMessages,
            ..ReplyHeader::default()
        };
        msg
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
        table
            .commit_register(1, 11, [0x5a; 32], make_register_reply(1, 10))
            .unwrap();
        table
            .commit_register(2, 22, [0x5a; 32], make_register_reply(2, 20))
            .unwrap();
        // Client 1 committed request 5; its reply is the entry's latest.
        table.commit_reply(1, 11, make_reply_with_checksum(1, 5, 30, 0xbeef));

        let restored = ClientTable::from_snapshot(table.to_snapshot()).unwrap();

        // Fences and dedup history survive, and the index is rebuilt (every
        // accessor reads through it).
        assert_eq!(restored.get_epoch(1), Some(10));
        assert_eq!(restored.get_epoch(2), Some(20));
        assert_eq!(restored.get_watermark(1), Some(5));
        assert_eq!(restored.get_watermark(2), Some(0));
        assert_eq!(restored.get_user_id(1), Some(11));
        // Replaying request 5 is a dedup hit, not a re-execution: at-most-once
        // holds across a restart, and the original bytes still answer it.
        match restored.check_request(1, 10, 5, Operation::SendMessages) {
            RequestStatus::Duplicate(cached) => assert_eq!(cached.header().request, 5),
            other => panic!("expected Duplicate, got {other:?}"),
        }
        assert!(matches!(
            restored.check_request(1, 10, 5, Operation::StoreConsumerOffset),
            RequestStatus::OperationMismatch { request: 5 }
        ));
        // A zombie holding the pre-restart epoch of a since-rebound client is
        // still fenced, so the fence is not weakened by the round trip.
        assert!(matches!(
            restored.check_request(1, 9, 6, Operation::SendMessages),
            RequestStatus::Fenced {
                current: 10,
                received: 9
            }
        ));
        // A client that never registered is still unknown.
        assert!(matches!(
            restored.check_request(3, 1, 1, Operation::SendMessages),
            RequestStatus::NoSession
        ));
    }

    // Only the entry's latest reply is persisted, so a retransmit of an older
    // ring entry is refused execution rather than answered from cache.
    #[test]
    fn snapshot_drops_stale_ring_replies_but_keeps_at_most_once() {
        let mut table = ClientTable::new(4);
        table
            .commit_register(1, TEST_USER_ID, [0x5a; 32], make_register_reply(1, 10))
            .unwrap();
        table.commit_reply(1, TEST_USER_ID, make_reply_for(1, 5, 20));
        table.commit_reply(1, TEST_USER_ID, make_reply_for(1, 6, 21));

        let restored = ClientTable::from_snapshot(table.to_snapshot()).unwrap();

        assert!(matches!(
            restored.check_request(1, 10, 5, Operation::SendMessages),
            RequestStatus::AlreadyApplied {
                request: 5,
                watermark: 6
            }
        ));
    }

    #[test]
    fn from_snapshot_preserves_committed_capacity_and_slot_identity() {
        let mut table = ClientTable::new(8);
        table
            .commit_register(1, TEST_USER_ID, [0x5a; 32], make_register_reply(1, 10))
            .unwrap();
        let mut snapshot = table.to_snapshot();
        snapshot.slots[0].0 = 5;

        let restored = ClientTable::from_snapshot(snapshot).unwrap();
        assert_eq!(restored.capacity(), 8);
        assert_eq!(
            restored.get_epoch(1),
            Some(10),
            "the committed slot identity must survive recovery"
        );
    }

    #[test]
    fn from_snapshot_rejects_two_entries_in_one_slot() {
        let mut table = ClientTable::new(2);
        table
            .commit_register(1, TEST_USER_ID, [0x5a; 32], make_register_reply(1, 10))
            .unwrap();
        table
            .commit_register(2, TEST_USER_ID, [0x5a; 32], make_register_reply(2, 20))
            .unwrap();
        let mut snapshot = table.to_snapshot();
        snapshot.slots[1].0 = snapshot.slots[0].0;

        assert!(matches!(
            ClientTable::from_snapshot(snapshot),
            Err(ClientTableDecodeError::DuplicateSlot { slot: 0 })
        ));
    }

    #[test]
    fn from_snapshot_rejects_a_slot_index_past_the_ceiling() {
        // The capacity is allocated from this index, so an out-of-range one must be
        // refused before the allocation rather than sized from.
        let mut table = ClientTable::new(2);
        table
            .commit_register(1, TEST_USER_ID, [0x5a; 32], make_register_reply(1, 10))
            .unwrap();
        let mut snapshot = table.to_snapshot();
        snapshot.slots[0].0 = u32::MAX;

        assert!(matches!(
            ClientTable::from_snapshot(snapshot),
            Err(ClientTableDecodeError::SlotOutOfRange { .. })
        ));
    }

    #[test]
    fn from_snapshot_rejects_duplicate_client_ids() {
        let mut table = ClientTable::new(2);
        table
            .commit_register(1, TEST_USER_ID, [0x5a; 32], make_register_reply(1, 10))
            .unwrap();
        table
            .commit_register(2, TEST_USER_ID, [0x5a; 32], make_register_reply(2, 20))
            .unwrap();
        let mut snapshot = table.to_snapshot();
        snapshot.slots[1].1 = snapshot.slots[0].1.clone();

        assert!(matches!(
            ClientTable::from_snapshot(snapshot),
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
        table
            .commit_register(1, TEST_USER_ID, [0x5a; 32], make_register_reply(1, 10))
            .unwrap();
        let mut snapshot = table.to_snapshot();
        snapshot.slots[0].1.reply = vec![0xff; 8];

        assert!(matches!(
            ClientTable::from_snapshot(snapshot),
            Err(ClientTableDecodeError::InvalidReply { slot: 0, .. })
        ));
    }

    #[test]
    fn recovered_registry_refuses_inconsistent_session_protection() {
        let mut table = ClientTable::new(2);
        table
            .commit_register(
                1,
                TEST_USER_ID,
                [0x5a; BIND_SECRET_BYTES],
                make_register_reply(1, 10),
            )
            .unwrap();
        table.commit_reply(1, TEST_USER_ID, make_reply_with_checksum(1, 5, 30, 0xbeef));
        let valid = table.to_snapshot();
        for corruption in [
            "zero epoch",
            "future epoch",
            "watermark",
            "end marker",
            "foreign receipt",
        ] {
            let mut snapshot = valid.clone();
            let entry = &mut snapshot.slots[0].1;
            match corruption {
                "zero epoch" => entry.epoch = 0,
                "future epoch" => entry.epoch = 31,
                "watermark" => entry.watermark = 6,
                "end marker" => entry.ended_op = Some(30),
                "foreign receipt" => {
                    entry.reply = make_reply_with_checksum(2, 5, 30, 0xbeef)
                        .as_slice()
                        .to_vec();
                }
                _ => unreachable!(),
            }
            assert!(
                matches!(
                    ClientTable::from_snapshot(snapshot),
                    Err(ClientTableDecodeError::InvalidEntry { .. })
                ),
                "accepted registry corruption: {corruption}"
            );
        }
        assert!(ClientTable::from_snapshot(valid).is_ok());
    }

    /// Register client 1 (register commit stamped at op 10). Returns
    /// (table, epoch=1).
    fn table_with_client() -> (ClientTable, u64) {
        let mut table = ClientTable::new(10);
        table
            .commit_register(1, TEST_USER_ID, [0x5a; 32], make_register_reply(1, 10))
            .unwrap();
        let epoch = table.get_epoch(1).expect("just registered");
        (table, epoch)
    }

    // Registration tests

    #[test]
    fn register_epoch_is_the_register_commit_op() {
        let mut table = ClientTable::new(10);
        table
            .commit_register(1, TEST_USER_ID, [0x5a; 32], make_register_reply(1, 42))
            .unwrap();
        assert_eq!(table.get_epoch(1), Some(42));
        assert_eq!(table.get_watermark(1), Some(0));
        assert_eq!(table.get_user_id(1), Some(TEST_USER_ID));
        assert_eq!(table.count(), 1);
    }

    // Each entry keeps the user id it registered with; lookups are per-client.
    #[test]
    fn register_stores_user_id() {
        let mut table = ClientTable::new(10);
        table
            .commit_register(1, 11, [0x5a; 32], make_register_reply(1, 10))
            .unwrap();
        table
            .commit_register(2, 22, [0x5a; 32], make_register_reply(2, 20))
            .unwrap();
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
            table.check_request(1, 99, 1, Operation::SendMessages),
            RequestStatus::NoSession
        ));
    }

    // Epochs are only handed out by register replies; a newer-than-minted
    // epoch is a client bug, distinct from the zombie case.
    #[test]
    fn check_request_future_epoch_is_client_bug() {
        let (table, epoch) = table_with_client();
        match table.check_request(1, epoch + 1, 1, Operation::SendMessages) {
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
        table.commit_reply(1, TEST_USER_ID, make_reply_for(1, 1, 11));
        assert!(matches!(
            table.check_request(1, epoch, 2, Operation::SendMessages),
            RequestStatus::New
        ));
    }

    // No contiguity requirement: a jump past the watermark executes. The
    // watermark records the highest committed request, not a sequence.
    #[test]
    fn check_request_jump_above_watermark_is_new() {
        let (mut table, epoch) = table_with_client();
        table.commit_reply(1, TEST_USER_ID, make_reply_for(1, 1, 11));
        assert!(matches!(
            table.check_request(1, epoch, 9, Operation::SendMessages),
            RequestStatus::New
        ));
        // And committing the jump moves the watermark to it.
        table.commit_reply(1, TEST_USER_ID, make_reply_for(1, 9, 12));
        assert_eq!(table.get_watermark(1), Some(9));
    }

    // The shape a client that spends request ids off the metadata plane
    // produces: partition-plane ids never reach this table, so the next
    // metadata request arrives with a gap under it. It executes, moves the
    // watermark to itself, and its retry still replays the original reply --
    // gaps cost the skipped ids and nothing else.
    #[test]
    fn check_request_dedups_a_metadata_request_that_arrives_after_a_gap() {
        let (mut table, epoch) = table_with_client();
        table.commit_reply(1, TEST_USER_ID, make_reply_for(1, 1, 11));
        // Requests 2..=5 went to the partition plane, which keeps no table.
        assert!(matches!(
            table.check_request(1, epoch, 6, Operation::SendMessages),
            RequestStatus::New
        ));

        table.commit_reply(1, TEST_USER_ID, make_reply_for(1, 6, 12));
        assert_eq!(table.get_watermark(1), Some(6));
        match table.check_request(1, epoch, 6, Operation::SendMessages) {
            RequestStatus::Duplicate(cached) => {
                assert_eq!(cached.header().request, 6);
                assert_eq!(
                    cached.header().commit,
                    12,
                    "the original reply, not a re-run"
                );
            }
            other => panic!("expected the gapped request to dedup, got {other:?}"),
        }
    }

    #[test]
    fn check_request_duplicate_at_watermark() {
        let (mut table, epoch) = table_with_client();
        table.commit_reply(1, TEST_USER_ID, make_reply_for(1, 1, 11));
        match table.check_request(1, epoch, 1, Operation::SendMessages) {
            RequestStatus::Duplicate(cached) => assert_eq!(cached.header().request, 1),
            other => panic!("expected Duplicate, got {other:?}"),
        }
    }

    // Below-watermark duplicate with the original still in the ring answers
    // with the original bytes.
    #[test]
    fn check_request_below_watermark_hits_ring() {
        let (mut table, epoch) = table_with_client();
        table.commit_reply(1, TEST_USER_ID, make_reply_for(1, 1, 11));
        table.commit_reply(1, TEST_USER_ID, make_reply_for(1, 2, 12));
        match table.check_request(1, epoch, 1, Operation::SendMessages) {
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
    fn check_request_below_watermark_past_retention_is_already_applied() {
        let (mut table, epoch) = table_with_client();
        // Enough small replies to exhaust the byte budget several times over,
        // so the oldest are certain to have been dropped.
        let requests = (REPLY_RING_RETENTION_BYTES / size_of::<ReplyHeader>() + 8) as u64;
        for request in 1..=requests {
            table.commit_reply(1, TEST_USER_ID, make_reply_for(1, request, 10 + request));
        }
        match table.check_request(1, epoch, 1, Operation::SendMessages) {
            RequestStatus::AlreadyApplied { request, watermark } => {
                assert_eq!(request, 1);
                assert_eq!(watermark, requests);
            }
            other => panic!("expected AlreadyApplied, got {other:?}"),
        }
        // The newest still answers with its own bytes.
        match table.check_request(1, epoch, requests, Operation::SendMessages) {
            RequestStatus::Duplicate(cached) => assert_eq!(cached.header().request, requests),
            other => panic!("expected Duplicate, got {other:?}"),
        }
    }

    // A retry that arrives after more commits than the floor holds still gets
    // its original bytes back: retention past the floor is budgeted in bytes,
    // and small replies are what a late retrier usually has outstanding.
    #[test]
    fn a_late_retry_replays_while_the_retention_budget_holds_it() {
        let (mut table, epoch) = table_with_client();
        let requests = REPLY_RING_CAPACITY as u64 + 2;
        for request in 1..=requests {
            table.commit_reply(1, TEST_USER_ID, make_reply_for(1, request, 10 + request));
        }

        match table.check_request(1, epoch, 1, Operation::SendMessages) {
            RequestStatus::Duplicate(cached) => {
                assert_eq!(cached.header().request, 1);
                assert_eq!(
                    cached.header().commit,
                    11,
                    "the original reply, not a re-run"
                );
            }
            other => panic!("expected the original reply to replay, got {other:?}"),
        }
    }

    // Heavy replies stay bounded by the floor, so the deeper retention cannot
    // be turned into a memory amplifier by a client polling large batches.
    #[test]
    fn heavy_replies_are_retained_only_to_the_floor() {
        let (mut table, epoch) = table_with_client();
        let requests = REPLY_RING_CAPACITY as u64 + 2;
        for request in 1..=requests {
            table.commit_reply(1, TEST_USER_ID, make_big_reply(1, request, 10 + request));
        }

        // The floor counts the register reply out: it aged out first, leaving
        // the last REPLY_RING_CAPACITY app replies.
        let oldest_retained = requests - REPLY_RING_CAPACITY as u64 + 1;
        assert!(matches!(
            table.check_request(1, epoch, oldest_retained - 1, Operation::SendMessages),
            RequestStatus::AlreadyApplied { .. }
        ));
        assert!(matches!(
            table.check_request(1, epoch, oldest_retained, Operation::SendMessages),
            RequestStatus::Duplicate(_)
        ));
    }

    // Dedup across view change. Backup inherits client_table via
    // commit_journal; on failover, retry must return ORIGINAL cached reply
    // (same request, same commit op), no re-execution. Pipeline state is
    // on PipelineEntry, so view-change cleanup doesn't touch slots.
    // Simulator test covers end-to-end; this is the unit invariant.
    #[test]
    fn duplicate_survives_view_change_reset() {
        let (mut table, epoch) = table_with_client();
        table.commit_reply(1, TEST_USER_ID, make_reply_for(1, 1, 11));

        match table.check_request(1, epoch, 1, Operation::SendMessages) {
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
    fn check_request_rejects_another_operation_at_watermark() {
        let (mut table, epoch) = table_with_client();
        table.commit_reply(1, TEST_USER_ID, make_reply_with_checksum(1, 1, 11, 0xAA));
        match table.check_request(1, epoch, 1, Operation::StoreConsumerOffset) {
            RequestStatus::OperationMismatch { request } => assert_eq!(request, 1),
            other => panic!("expected OperationMismatch, got {other:?}"),
        }
        assert!(matches!(
            table.check_request(1, epoch, 1, Operation::SendMessages),
            RequestStatus::Duplicate(_)
        ));
    }

    #[test]
    fn check_request_rejects_another_operation_below_watermark() {
        let (mut table, epoch) = table_with_client();
        table.commit_reply(1, TEST_USER_ID, make_reply_with_checksum(1, 1, 11, 0xAA));
        table.commit_reply(1, TEST_USER_ID, make_reply_with_checksum(1, 2, 12, 0xBB));
        assert!(matches!(
            table.check_request(1, epoch, 1, Operation::SendMessages),
            RequestStatus::Duplicate(_)
        ));
        assert!(matches!(
            table.check_request(1, epoch, 1, Operation::StoreConsumerOffset),
            RequestStatus::OperationMismatch { request: 1 }
        ));
    }

    #[test]
    fn check_request_replays_projected_operations_after_recovery() {
        for (request_operation, prepared_operation) in [
            (
                Operation::CreateTopic,
                Operation::CreateTopicWithAssignments,
            ),
            (
                Operation::CreatePartitions,
                Operation::CreatePartitionsWithAssignments,
            ),
            (Operation::DeleteSegments, Operation::TruncatePartition),
        ] {
            let (mut table, epoch) = table_with_client();
            let reply = make_reply_for(1, 1, 11).transmute_header(|old, header| {
                *header = old;
                header.operation = prepared_operation;
            });
            table.commit_reply(1, TEST_USER_ID, reply.clone());
            for restored in [
                ClientTable::from_snapshot(table.to_snapshot()).unwrap(),
                ClientTable::decode(&table.encode()).unwrap(),
            ] {
                let RequestStatus::Duplicate(cached) =
                    restored.check_request(1, epoch, 1, request_operation)
                else {
                    panic!("lost the receipt for {request_operation:?}");
                };
                assert_eq!(cached.into_message().as_slice(), reply.as_slice());
                assert!(matches!(
                    restored.check_request(1, epoch, 1, Operation::UpdateStream),
                    RequestStatus::OperationMismatch { .. }
                ));
            }
        }
    }

    // Commit tests

    #[test]
    fn commit_caches_reply() {
        let (mut table, _epoch) = table_with_client();
        table.commit_reply(1, TEST_USER_ID, make_reply_for(1, 1, 11));
        let cached = table.get_reply(1).expect("should have cached reply");
        assert_eq!(cached.header().request, 1);
    }

    #[test]
    fn commit_updates_preserves_epoch() {
        let (mut table, epoch) = table_with_client();
        table.commit_reply(1, TEST_USER_ID, make_reply_for(1, 1, 11));
        table.commit_reply(1, TEST_USER_ID, make_reply_for(1, 2, 12));
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
        table.commit_reply(1, TEST_USER_ID, make_reply_for(1, 1, 11));
        table.commit_reply(1, TEST_USER_ID, make_reply_for(1, 1, 11));
        assert_eq!(table.get_watermark(1), Some(1));
        match table.check_request(1, epoch, 1, Operation::SendMessages) {
            RequestStatus::Duplicate(cached) => assert_eq!(cached.header().request, 1),
            other => panic!("expected Duplicate, got {other:?}"),
        }
    }

    // Eviction tests

    // Capacity resize (boot-only)

    // --- ClientTableMode::PartitionSlice: watermark-only dedup ---
    //
    // One consensus group's slice. No register mints entries here, no reply is
    // cached, and slots grow on demand, so these pin the behaviour the
    // partition plane actually relies on.

    const SLICE_USER: u32 = 3;

    fn slice(clients_max: usize) -> ClientTable {
        ClientTable::with_mode(clients_max, ClientTableMode::PartitionSlice)
    }

    fn watermark(client: u128, watermark: u64, latest_commit: u64) -> DedupWatermark {
        DedupWatermark {
            client,
            user_id: SLICE_USER,
            watermark,
            latest_commit,
            committed_window: 1,
            session: 0,
            reply: Vec::new(),
        }
    }

    fn receipt_watermark(client: u128, request: u64, commit: u64) -> DedupWatermark {
        let mut reply = Message::<ReplyHeader>::new(size_of::<ReplyHeader>() + size_of::<u32>());
        reply = reply.transmute_header(|_, header: &mut ReplyHeader| {
            *header = ReplyHeader {
                client,
                request,
                commit,
                command: Command::Reply,
                operation: iggy_binary_protocol::Operation::StoreConsumerOffset,
                size: (size_of::<ReplyHeader>() + size_of::<u32>()) as u32,
                ..Default::default()
            };
        });
        DedupWatermark {
            session: 42,
            reply: reply.as_slice().to_vec(),
            ..watermark(client, request, commit)
        }
    }

    fn clients_of(table: &ClientTable) -> Vec<u128> {
        table
            .watermarks_sorted()
            .into_iter()
            .map(|entry| entry.client)
            .collect()
    }

    #[test]
    fn given_partition_slice_when_commit_replayed_should_be_idempotent() {
        let mut table = slice(4);
        table.commit_request(7, SLICE_USER, 5, 100).unwrap();
        table.commit_request(7, SLICE_USER, 5, 100).unwrap();
        table.commit_request(7, SLICE_USER, 5, 100).unwrap();

        assert_eq!(table.watermarks_sorted(), vec![watermark(7, 5, 100)]);
    }

    #[test]
    fn given_partition_slice_when_filled_to_cap_should_grow_without_holes() {
        // The lazily grown array must hand out every index once and never
        // rescan for a hole that cannot exist below the cap.
        let mut table = slice(64);
        for client in 1..=64u128 {
            table
                .commit_request(client, SLICE_USER, 1, client as u64)
                .unwrap();
        }

        assert_eq!(table.count(), 64);
        assert_eq!(table.slots.len(), 64);
        assert!(table.slots.iter().all(Option::is_some));
    }

    #[test]
    fn given_partition_slice_when_install_exceeds_local_cap_should_preserve_committed_limit() {
        let mut table = slice(2);
        table
            .install_watermarks(
                3,
                [
                    receipt_watermark(1, 1, 300),
                    receipt_watermark(2, 1, 100),
                    receipt_watermark(3, 1, 200),
                ],
            )
            .unwrap();
        assert_eq!(table.capacity(), 3);
        assert_eq!(clients_of(&table), vec![1, 2, 3]);
    }

    #[test]
    fn given_partition_slice_when_exported_should_sort_ascending_by_client() {
        let mut table = slice(8);
        for (commit_op, client) in [30u128, 10, 20].into_iter().enumerate() {
            table
                .commit_request(client, SLICE_USER, 1, commit_op as u64)
                .unwrap();
        }

        assert_eq!(clients_of(&table), vec![10, 20, 30]);
    }

    #[test]
    fn given_partition_slice_when_client_is_reserved_zero_should_record_nothing() {
        // Zero is reserved cluster-wide and refused at every ingress; a commit
        // or install that still carries it degrades to no entry, not a panic.
        let mut table = slice(4);
        assert!(table.commit_request(0, SLICE_USER, 5, 100).is_err());
        assert!(
            table
                .install_watermarks(
                    4,
                    [receipt_watermark(0, 5, 100), receipt_watermark(1, 1, 101)]
                )
                .is_err()
        );
        assert!(clients_of(&table).is_empty());
    }

    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "a reply-caching table must use commit_reply")]
    fn given_metadata_table_when_commit_request_called_should_panic() {
        ClientTable::new(4)
            .commit_request(7, SLICE_USER, 1, 1)
            .unwrap();
    }

    #[test]
    fn login_retry_and_capacity_pressure_preserve_session_protection() {
        const CLIENT: u128 = 7;
        const EPOCH: u64 = 10;
        let secret = [0x5a; BIND_SECRET_BYTES];
        let verifier = bind_verifier(CLIENT, TEST_USER_ID, &secret);
        let mut table = ClientTable::new(1);
        table.commit_capacity(1).unwrap();
        table
            .commit_register(
                CLIENT,
                TEST_USER_ID,
                verifier,
                make_register_reply(CLIENT, EPOCH),
            )
            .unwrap();
        let (_, discovered, attached) = table.bind_session(CLIENT, 0, &secret).unwrap();
        assert_eq!(discovered, EPOCH);
        let second = table.attach_session(CLIENT, EPOCH, TEST_USER_ID).unwrap();
        table.commit_reply(
            CLIENT,
            TEST_USER_ID,
            make_reply_with_checksum(CLIENT, 1, 11, 0xbeef),
        );
        let original = table.get_reply(CLIENT).unwrap().as_bytes().to_vec();
        table
            .commit_register(
                CLIENT,
                TEST_USER_ID,
                verifier,
                make_register_reply(CLIENT, 20),
            )
            .unwrap();
        assert_eq!(table.get_epoch(CLIENT), Some(EPOCH));
        assert_eq!(table.get_watermark(CLIENT), Some(1));
        assert!(
            table
                .commit_register(
                    CLIENT,
                    TEST_USER_ID + 1,
                    verifier,
                    make_register_reply(CLIENT, 21)
                )
                .is_err()
        );
        assert!(
            table
                .commit_register(
                    CLIENT,
                    TEST_USER_ID,
                    [0x6a; BIND_SECRET_BYTES],
                    make_register_reply(CLIENT, 22)
                )
                .is_err()
        );
        assert!(
            table
                .commit_register(
                    CLIENT + 1,
                    TEST_USER_ID,
                    verifier,
                    make_register_reply(CLIENT + 1, 23)
                )
                .is_err()
        );
        assert!(attached.is_valid() && second.is_valid());
        let RequestStatus::Duplicate(reply) =
            table.check_request(CLIENT, EPOCH, 1, Operation::SendMessages)
        else {
            panic!("capacity pressure lost the committed result");
        };
        assert_eq!(reply.into_message().as_slice(), original);
        assert!(matches!(
            table.check_request(CLIENT, EPOCH, 1, Operation::StoreConsumerOffset),
            RequestStatus::OperationMismatch { .. }
        ));
    }

    #[test]
    fn expired_logout_ends_session_after_client_max_request() {
        let (mut table, epoch) = table_with_client();
        table.commit_reply(1, TEST_USER_ID, make_reply_for(1, u64::MAX, epoch + 1));
        let logout = make_reply_for(1, EXPIRED_SESSION_REQUEST_ID, epoch + 2).transmute_header(
            |old, header| {
                *header = old;
                header.operation = Operation::Logout;
            },
        );
        assert!(table.commit_logout(1, TEST_USER_ID, epoch, logout).unwrap());
        assert_eq!(table.get_epoch(1), None);
        assert_eq!(table.ended_sessions().count(), 1);
    }

    #[test]
    fn stale_logout_preserves_live_session_and_latest_receipt() {
        let (mut table, epoch) = table_with_client();
        let original = make_reply_for(1, 5, epoch + 1);
        table.commit_reply(1, TEST_USER_ID, original.clone());
        let attachment = table.attach_session(1, epoch, TEST_USER_ID).unwrap();
        for request in [1, 5] {
            let logout = make_reply_for(1, request, epoch + 2).transmute_header(|old, header| {
                *header = old;
                header.operation = Operation::Logout;
            });
            assert!(!table.commit_logout(1, TEST_USER_ID, epoch, logout).unwrap());
            assert!(attachment.is_valid());
            assert_eq!(
                table
                    .get_reply(1)
                    .unwrap()
                    .clone()
                    .into_message()
                    .as_slice(),
                original.as_slice()
            );
            assert_eq!(table.ended_sessions().count(), 0);
        }
    }

    #[test]
    fn logout_replay_survives_recovery_until_exact_finalization() {
        const CLIENT: u128 = 7;
        const EPOCH: u64 = 10;
        const END: u64 = 13;
        let secret = [0x5a; BIND_SECRET_BYTES];
        let verifier = bind_verifier(CLIENT, TEST_USER_ID, &secret);
        let mut table = ClientTable::new(1);
        table.commit_capacity(1).unwrap();
        table
            .commit_register(
                CLIENT,
                TEST_USER_ID,
                verifier,
                make_register_reply(CLIENT, EPOCH),
            )
            .unwrap();
        let attached = table.attach_session(CLIENT, EPOCH, TEST_USER_ID).unwrap();
        let reply =
            make_reply_with_checksum(CLIENT, 2, END, 0xbeef).transmute_header(|old, header| {
                *header = old;
                header.operation = iggy_binary_protocol::Operation::Logout;
            });
        assert!(
            !table
                .commit_logout(CLIENT, TEST_USER_ID, EPOCH - 1, reply.clone())
                .unwrap()
        );
        assert!(attached.is_valid());
        assert!(
            table
                .commit_logout(CLIENT, TEST_USER_ID, EPOCH, reply.clone())
                .unwrap()
        );
        assert!(!attached.is_valid());
        assert!(table.bind_session(CLIENT, EPOCH, &secret).is_err());
        assert!(
            table
                .commit_register(
                    CLIENT + 1,
                    TEST_USER_ID,
                    verifier,
                    make_register_reply(CLIENT + 1, END + 1)
                )
                .is_err()
        );
        for mut restored in [
            ClientTable::from_snapshot(table.to_snapshot()).unwrap(),
            ClientTable::decode(&table.encode()).unwrap(),
        ] {
            assert_eq!(restored.capacity(), 1);
            let RequestStatus::Duplicate(cached) =
                restored.check_request(CLIENT, EPOCH, 2, Operation::Logout)
            else {
                panic!("the ended session lost its Logout result");
            };
            assert_eq!(cached.into_message().as_slice(), reply.as_slice());
            let identity = iggy_binary_protocol::requests::system::SessionIdentity {
                client_id: CLIENT,
                session: EPOCH,
                metadata_watermark: END,
            };
            assert!(!restored.finalize_session(
                iggy_binary_protocol::requests::system::SessionIdentity {
                    session: EPOCH - 1,
                    ..identity
                }
            ));
            assert!(!restored.finalize_session(
                iggy_binary_protocol::requests::system::SessionIdentity {
                    metadata_watermark: END + 1,
                    ..identity
                }
            ));
            assert_eq!(restored.count(), 1);
            assert!(restored.finalize_session(identity));
            restored.set_capacity(20);
            assert_eq!(restored.capacity(), 1);
            restored
                .commit_register(
                    CLIENT,
                    TEST_USER_ID,
                    verifier,
                    make_register_reply(CLIENT, END + 2),
                )
                .unwrap();
            assert!(matches!(
                restored.check_request(CLIENT, EPOCH, 2, Operation::SendMessages),
                RequestStatus::Fenced { .. }
            ));
        }
    }

    #[test]
    fn given_partition_receipts_when_requests_commit_should_retain_only_the_latest() {
        const CLIENT: u128 = 7;
        const SESSION: u64 = 42;
        const REQUESTS: u64 = 40;
        let mut table = slice(1);
        for request in 1..=REQUESTS {
            table
                .commit_partition_reply(
                    TEST_USER_ID,
                    SESSION,
                    make_reply_for(CLIENT, request, request),
                )
                .unwrap();
        }
        let entry = table.slots[table.index[&CLIENT]].as_ref().unwrap();
        assert_eq!(
            entry.ring.len(),
            1,
            "only the unresolved result needs bytes"
        );
        assert_eq!(entry.latest().header().request, REQUESTS);
        assert!(matches!(
            table.check_partition_request(
                CLIENT,
                TEST_USER_ID,
                SESSION,
                REQUESTS - 1,
                Operation::SendMessages,
            ),
            RequestStatus::AlreadyApplied { .. }
        ));
        assert!(matches!(
            table.check_partition_request(
                CLIENT,
                TEST_USER_ID,
                SESSION,
                REQUESTS,
                Operation::SendMessages,
            ),
            RequestStatus::Duplicate(_)
        ));
    }

    #[test]
    fn given_missing_session_when_binding_should_distinguish_registration_uncertainty() {
        const CLIENT: u128 = 7;
        const SESSION: u64 = 42;
        let secret = [0x5a; BIND_SECRET_BYTES];
        let mut table = ClientTable::new(1);
        assert!(matches!(
            table.bind_session(CLIENT, SESSION, &secret),
            Err(IggyError::Unauthenticated)
        ));
        assert!(matches!(
            table.bind_session(CLIENT, 0, &secret),
            Err(IggyError::TransientNotAccepted)
        ));
        table
            .commit_register(
                CLIENT,
                TEST_USER_ID,
                bind_verifier(CLIENT, TEST_USER_ID, &secret),
                make_register_reply(CLIENT, SESSION),
            )
            .unwrap();
        assert!(matches!(
            table.bind_session(CLIENT, SESSION, &[0x6a; BIND_SECRET_BYTES]),
            Err(IggyError::Unauthenticated)
        ));
    }

    #[test]
    fn given_partition_receipts_when_request_ids_skip_should_protect_only_committed_ids() {
        const CLIENT: u128 = 7;
        const SESSION: u64 = 42;
        let mut table = slice(1);
        for (request, commit) in [(1, 100), (5, 101)] {
            table
                .commit_partition_reply(
                    TEST_USER_ID,
                    SESSION,
                    make_reply_for(CLIENT, request, commit),
                )
                .unwrap();
        }
        assert!(matches!(
            table.check_partition_request(
                CLIENT,
                TEST_USER_ID,
                SESSION,
                3,
                Operation::SendMessages,
            ),
            RequestStatus::New
        ));
        assert!(matches!(
            table.check_partition_request(
                CLIENT,
                TEST_USER_ID,
                SESSION,
                1,
                Operation::SendMessages,
            ),
            RequestStatus::AlreadyApplied {
                request: 1,
                watermark: 5
            }
        ));
        table
            .commit_partition_reply(
                TEST_USER_ID,
                SESSION,
                make_reply_for(CLIENT, COMMITTED_WINDOW_BITS + 10, 102),
            )
            .unwrap();
        assert!(matches!(
            table.check_partition_request(
                CLIENT,
                TEST_USER_ID,
                SESSION,
                3,
                Operation::SendMessages,
            ),
            RequestStatus::AlreadyApplied { .. }
        ));
        assert!(matches!(
            table.check_partition_request(
                CLIENT,
                TEST_USER_ID + 1,
                SESSION,
                COMMITTED_WINDOW_BITS + 11,
                Operation::SendMessages,
            ),
            RequestStatus::Fenced { .. }
        ));
    }

    #[test]
    fn partition_pressure_and_transfer_preserve_the_exact_receipt() {
        let mut table = slice(1);
        table.commit_capacity(1).unwrap();
        let watermark = receipt_watermark(7, 5, 11);
        let reply =
            Message::<ReplyHeader>::try_from(Owned::copy_from_slice(&watermark.reply)).unwrap();
        let committed = table
            .commit_partition_reply(TEST_USER_ID, watermark.session, reply.clone())
            .unwrap();
        let mut changed = reply.clone();
        changed.as_mut_slice()[size_of::<ReplyHeader>()] ^= 1;
        let replayed = table
            .commit_partition_reply(TEST_USER_ID, watermark.session, changed)
            .unwrap();
        assert!(
            committed
                .into_wire_bytes()
                .shares_allocation(&replayed.into_wire_bytes())
        );
        assert!(
            table
                .commit_partition_reply(TEST_USER_ID + 1, watermark.session, reply.clone())
                .is_err()
        );
        let other = receipt_watermark(8, 1, 12);
        let other_reply =
            Message::<ReplyHeader>::try_from(Owned::copy_from_slice(&other.reply)).unwrap();
        assert!(matches!(
            table.commit_partition_reply(TEST_USER_ID, other.session, other_reply),
            Err(ClientTableWireError::TooManyEntries { .. })
        ));
        let mut restored = slice(20);
        restored
            .install_watermarks(1, table.watermarks_sorted())
            .unwrap();
        assert_eq!(restored.capacity(), 1);
        let RequestStatus::Duplicate(cached) = restored.check_partition_request(
            7,
            TEST_USER_ID,
            watermark.session,
            5,
            Operation::StoreConsumerOffset,
        ) else {
            panic!("a transferred receipt must replay its original bytes");
        };
        assert_eq!(cached.into_message().as_slice(), reply.as_slice());
        assert!(matches!(
            restored.check_partition_request(
                7,
                TEST_USER_ID,
                watermark.session,
                5,
                Operation::SendMessages,
            ),
            RequestStatus::OperationMismatch { request: 5 }
        ));
        assert!(matches!(
            restored.check_partition_request(
                7,
                TEST_USER_ID + 1,
                watermark.session,
                5,
                Operation::StoreConsumerOffset,
            ),
            RequestStatus::Fenced { .. }
        ));
        assert!(!restored.forget_session(7, watermark.session + 1));
        assert_eq!(restored.count(), 1);
        assert!(restored.forget_session(7, watermark.session));
        assert_eq!(restored.count(), 0);
        assert_eq!(restored.capacity(), 1);
    }

    #[test]
    fn set_capacity_resizes_empty_table() {
        let mut table = ClientTable::new(10);
        table.set_capacity(2);
        table
            .commit_register(100, TEST_USER_ID, [0x5a; 32], make_register_reply(100, 10))
            .unwrap();
        table
            .commit_register(200, TEST_USER_ID, [0x5a; 32], make_register_reply(200, 20))
            .unwrap();
        assert!(matches!(
            table.commit_register(300, TEST_USER_ID, [0x5a; 32], make_register_reply(300, 30)),
            Err(ClientTableWireError::TooManyEntries { max: 2, .. })
        ));
        assert_eq!(table.count(), 2);
        assert_eq!(table.get_epoch(100), Some(10));
        assert_eq!(table.get_epoch(200), Some(20));
    }

    // The empty-table contract is asserted, not silently honored: resizing a
    // populated table would drop live sessions, so it must panic.
    #[test]
    #[should_panic(expected = "before any client registers")]
    fn set_capacity_rejects_a_populated_table() {
        let (mut table, _session) = table_with_client();
        table.set_capacity(2);
    }

    // Edge cases

    // commit_reply for unregistered/evicted client must not panic;
    // wire reply still ships, cache silently skipped.
    #[test]
    fn commit_reply_for_unregistered_client_is_noop() {
        let mut table = ClientTable::new(10);
        // No register: index has no entry.
        let outcome = table.commit_reply(1, TEST_USER_ID, make_reply_for(1, 1, 10));
        assert_eq!(outcome, CommitReply::NoEntry);
        assert!(table.get_reply(1).is_none(), "no entry must be created");
        assert_eq!(table.count(), 0);
    }

    #[test]
    fn a_server_originated_commit_under_the_reserved_id_is_not_cached() {
        const NO_USER: u32 = 0;
        const DEFAULT_REQUEST: u64 = 0;
        const COMMIT: u64 = 5;

        let mut table = ClientTable::new(2);
        let outcome = table.commit_reply(
            RESERVED_CLIENT_ID,
            NO_USER,
            make_reply_for(RESERVED_CLIENT_ID, DEFAULT_REQUEST, COMMIT),
        );

        assert_eq!(outcome, CommitReply::NoEntry);
        assert!(
            table.index.is_empty(),
            "the reserved id must not gain an entry"
        );
        assert_eq!(table.count(), 0);
    }

    #[test]
    fn commit_reply_watermark_regression_is_skipped() {
        let (mut table, _epoch) = table_with_client();
        assert_eq!(
            table.commit_reply(1, TEST_USER_ID, make_reply_for(1, 5, 15)),
            CommitReply::Cached
        );
        assert_eq!(
            table.commit_reply(1, TEST_USER_ID, make_reply_for(1, 3, 16)),
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
        table
            .commit_register(1, TEST_USER_ID, [0x5a; 32], make_register_reply(1, 10))
            .unwrap();
        table
            .commit_register(2, TEST_USER_ID, [0x5a; 32], make_register_reply(2, 20))
            .unwrap();
        // Retrying login must not replace either epoch.
        table
            .commit_register(2, TEST_USER_ID, [0x5a; 32], make_register_reply(2, 30))
            .unwrap();
        assert_eq!(table.get_epoch(1), Some(10));
        assert_eq!(table.get_epoch(2), Some(20));
        assert!(matches!(
            table.check_request(1, 10, 1, Operation::SendMessages),
            RequestStatus::New
        ));
        assert!(matches!(
            table.check_request(2, 20, 1, Operation::SendMessages),
            RequestStatus::New
        ));
        // A stale presented epoch remains fenced.
        assert!(matches!(
            table.check_request(2, 19, 1, Operation::SendMessages),
            RequestStatus::Fenced { .. }
        ));
    }

    // Codec tests

    // State transfer ships the table as bytes; the decoded table must answer
    // every dedup question identically to the original.
    #[test]
    fn encode_decode_roundtrip_preserves_dedup_state() {
        let mut table = ClientTable::new(10);
        table
            .commit_register(1, 11, [0x5a; 32], make_register_reply(1, 10))
            .unwrap();
        table.commit_reply(1, 11, make_reply_with_checksum(1, 1, 11, 0xAA));
        table.commit_reply(1, 11, make_reply_for(1, 2, 12));
        // A repeated login preserves the original identity and request history.
        table
            .commit_register(1, 11, [0x5a; 32], make_register_reply(1, 20))
            .unwrap();
        table
            .commit_register(2, 33, [0x5a; 32], make_register_reply(2, 30))
            .unwrap();

        let encoded = table.encode();
        let decoded = ClientTable::decode(&encoded).expect("roundtrip decodes");

        assert_eq!(decoded.count(), 2);
        assert_eq!(decoded.get_epoch(1), Some(10));
        assert_eq!(decoded.get_user_id(1), Some(11));
        assert_eq!(decoded.get_watermark(1), Some(2));
        assert_eq!(decoded.get_epoch(2), Some(30));
        match decoded.check_request(1, 10, 2, Operation::SendMessages) {
            RequestStatus::Duplicate(cached) => {
                assert_eq!(cached.header().request, 2, "latest reply survives");
                assert_eq!(cached.header().commit, 12, "original bytes survive");
            }
            other => panic!("expected Duplicate, got {other:?}"),
        }
        assert!(matches!(
            decoded.check_request(1, 10, 1, Operation::StoreConsumerOffset),
            RequestStatus::OperationMismatch { request: 1 }
        ));
        assert!(matches!(
            decoded.check_request(1, 9, 1, Operation::SendMessages),
            RequestStatus::Fenced { .. }
        ));

        // Deterministic bytes: encoding the decoded table reproduces them.
        assert_eq!(decoded.encode(), encoded);
    }

    // Deep retention is a live-memory property, not a transferred one: the
    // artifact carries the floor, so a retry the serving replica would have
    // replayed loses its bytes on the receiver. The fence still rides along, so
    // the answer degrades and the operation is still never re-executed.
    #[test]
    fn state_transfer_cuts_retention_back_to_the_floor() {
        let (mut table, epoch) = table_with_client();
        let requests = REPLY_RING_CAPACITY as u64 + 3;
        for request in 1..=requests {
            table.commit_reply(1, TEST_USER_ID, make_reply_for(1, request, 10 + request));
        }
        // The whole run is still cached locally: retention is byte-budgeted and
        // these replies are headers.
        assert!(matches!(
            table.check_request(1, epoch, 1, Operation::SendMessages),
            RequestStatus::Duplicate(_)
        ));

        let decoded = ClientTable::decode(&table.encode()).expect("roundtrip decodes");

        assert_eq!(decoded.get_watermark(1), Some(requests));
        let oldest_transferred = requests - REPLY_RING_CAPACITY as u64 + 1;
        match decoded.check_request(1, epoch, oldest_transferred - 1, Operation::SendMessages) {
            RequestStatus::AlreadyApplied { request, watermark } => {
                assert_eq!(request, oldest_transferred - 1);
                assert_eq!(watermark, requests, "the fence survives the transfer");
            }
            other => panic!("expected AlreadyApplied past the transferred floor, got {other:?}"),
        }
        match decoded.check_request(1, epoch, oldest_transferred, Operation::SendMessages) {
            RequestStatus::Duplicate(cached) => {
                assert_eq!(cached.header().request, oldest_transferred);
            }
            other => panic!("expected Duplicate, got {other:?}"),
        }
    }

    #[test]
    fn decode_rejects_corruption() {
        let mut table = ClientTable::new(4);
        table
            .commit_register(1, TEST_USER_ID, [0x5a; 32], make_register_reply(1, 10))
            .unwrap();
        let encoded = table.encode();

        let mut previous_format = encoded[..encoded.len() - size_of::<u64>()].to_vec();
        previous_format[..CLIENT_TABLE_MAGIC.len()].copy_from_slice(b"ICT4");
        assert!(matches!(
            ClientTable::decode(&reseal(previous_format)),
            Err(ClientTableWireError::BadMagic)
        ));

        // Flipped content byte -> checksum mismatch.
        let mut flipped = encoded.clone();
        flipped[8] ^= 0xFF;
        assert!(matches!(
            ClientTable::decode(&flipped),
            Err(ClientTableWireError::ChecksumMismatch { .. })
        ));

        // Truncation.
        assert!(matches!(
            ClientTable::decode(&encoded[..encoded.len() - 1]),
            Err(ClientTableWireError::ChecksumMismatch { .. } | ClientTableWireError::Truncated)
        ));

        let empty = ClientTable::new(4).encode();
        assert_eq!(
            ClientTable::decode(&empty)
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
    // other occupied but unindexed, breaking capacity accounting and lookup.
    #[test]
    fn decode_rejects_a_duplicate_client_id() {
        let mut table = ClientTable::new(2);
        table
            .commit_register(7, TEST_USER_ID, [0x5a; 32], make_register_reply(7, 10))
            .unwrap();
        table
            .commit_register(9, TEST_USER_ID, [0x5a; 32], make_register_reply(9, 20))
            .unwrap();
        let encoded = table.encode();

        // Rewrite the second entry's client_id to match the first. Entries are
        // fixed-width up to their ring, and both rings hold one register reply
        // of equal length, so the second entry starts at a computable offset.
        let content = &encoded[..encoded.len() - size_of::<u64>()];
        let header_len = CLIENT_TABLE_MAGIC.len() + 2 * size_of::<u32>();
        let reply_len = (content.len() - header_len - 2 * ENCODED_ENTRY_FIXED_LEN) / 2;
        let second = header_len + ENCODED_ENTRY_FIXED_LEN + reply_len;
        let mut duped = content.to_vec();
        duped[second..second + size_of::<u128>()].copy_from_slice(&7u128.to_le_bytes());
        let receipt_client = second
            + ENCODED_ENTRY_FIXED_LEN
            + size_of::<u32>()
            + std::mem::offset_of!(ReplyHeader, client);
        duped[receipt_client..receipt_client + size_of::<u128>()]
            .copy_from_slice(&7u128.to_le_bytes());

        assert!(matches!(
            ClientTable::decode(&reseal(duped)),
            Err(ClientTableWireError::DuplicateClientId { client_id: 7, .. })
        ));
    }

    // The received count is the only bound between a peer-supplied u32 and the
    // eager slot allocation, so it must stop at the same ceiling
    // `from_snapshot` enforces.
    #[test]
    fn decode_rejects_a_count_past_the_slot_ceiling() {
        let mut table = ClientTable::new(1);
        table
            .commit_register(3, TEST_USER_ID, [0x5a; 32], make_register_reply(3, 10))
            .unwrap();
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
            ClientTable::decode(&reseal(oversized)),
            Err(ClientTableWireError::TooManyEntries {
                max: CLIENTS_TABLE_SLOT_MAX,
                ..
            })
        ));
    }

    // A ring longer than `transferable_replies` emits comes from a peer this
    // one cannot model; admitting it would eventually wrap `encode`'s u8 length
    // and leave the table untransferable onward.
    #[test]
    fn decode_rejects_a_ring_longer_than_capacity() {
        let mut table = ClientTable::new(1);
        table
            .commit_register(3, TEST_USER_ID, [0x5a; 32], make_register_reply(3, 10))
            .unwrap();
        let encoded = table.encode();

        let content = &encoded[..encoded.len() - size_of::<u64>()];
        let mut oversized = content.to_vec();
        // ring_len is the last fixed field of the entry.
        let ring_len_at =
            CLIENT_TABLE_MAGIC.len() + 2 * size_of::<u32>() + ENCODED_ENTRY_FIXED_LEN - 1;
        assert_eq!(oversized[ring_len_at], 1, "entry's ring holds one reply");
        #[allow(clippy::cast_possible_truncation)]
        {
            oversized[ring_len_at] = REPLY_RING_CAPACITY as u8 + 1;
        }

        assert!(matches!(
            ClientTable::decode(&reseal(oversized)),
            Err(ClientTableWireError::RingTooLong { .. })
        ));
    }
}
