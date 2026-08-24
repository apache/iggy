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

//! All consensus headers are exactly 256 bytes with `#[repr(C)]` layout.
//! Size and field offsets are enforced at compile time. Deserialization
//! is a pointer cast (zero-copy) via `bytemuck::try_from_bytes`.
//!
//! # Wire compatibility
//!
//! The replica-to-replica control headers are a BREAKING, non-negotiable change
//! against any build predating [`ConsensusHeader::FRAME_SEALED`]: `checksum` went
//! from a field nobody wrote to one every receiver verifies, so each side reads the
//! other's frames as corrupt. `release` must be zero on every header, so there is no
//! version channel to gate on and no way for the two to detect each other.
//!
//! Replicas must therefore be upgraded together, with the cluster down. A rolling
//! upgrade does not degrade, it stops the cluster: every control frame between a
//! mixed pair is dropped, so no view change reaches a quorum. Nothing enforces this,
//! because there is nothing left to enforce it with; this note is the declaration.
//!
//! The forwarding commands at discriminants 26 through 29 are same-release
//! replica messages. A build whose command bit-pattern predates them drops the
//! frames as unparsable and never sends a result, so backup-dialed session
//! operations wait for their forward timeout in a mixed fleet. These commands are
//! another reason the release cannot be rolled node by node.
//!
//! `Prepare`, `Request`, `Reply`, and `Eviction` are unaffected. Prepares keep
//! `checksum` as their view-independent identity, and the three client-facing
//! headers are sealed on neither side, so SDKs are untouched.

use super::{Command, ConsensusError, Operation};
use bytemuck::{CheckedBitPattern, NoUninit};
use std::mem::offset_of;

pub const HEADER_SIZE: usize = 256;

/// Length of [`GenericHeader::reserved_command`], the per-command scratch area
/// the replica-auth handshake writes its nonce / MAC / reject-reason into.
pub const RESERVED_COMMAND_LEN: usize = 128;

/// Byte offset of [`GenericHeader::size`] within the on-wire header.
///
/// Single source of truth for transports that decode the size field
/// before constructing the typed header (WS Binary frames, streaming
/// TLS pumps that need the total length to size their accumulator).
/// The `const _: ()` block on [`GenericHeader`] re-asserts the field
/// offset matches this constant, so layout drift trips the build
/// before any caller reads the wrong bytes.
pub const SIZE_FIELD_OFFSET: usize = 48;

/// Read the four-byte little-endian size field at
/// [`SIZE_FIELD_OFFSET`] from a wire header buffer.
///
/// Returns `None` if `header` is shorter than `SIZE_FIELD_OFFSET + 4`;
/// callers that already validated `header.len() >= HEADER_SIZE` are
/// safe but should still propagate the `Option` rather than `unwrap`.
#[inline]
#[must_use]
pub fn read_size_field(header: &[u8]) -> Option<u32> {
    header
        .get(SIZE_FIELD_OFFSET..SIZE_FIELD_OFFSET + 4)
        .and_then(|s| s.try_into().ok())
        .map(u32::from_le_bytes)
}

/// Frame checksum over a raw header: every byte past `checksum` itself.
///
/// Byte-level twin of [`ConsensusHeader::frame_checksum`], which delegates here so
/// the typed and raw seals cannot disagree. For callers that do not know the
/// concrete header type statically, such as a wire-level test fixture.
#[must_use]
pub fn frame_checksum_bytes(header: &[u8; HEADER_SIZE]) -> u128 {
    u128::from(twox_hash::XxHash3_64::oneshot(&header[size_of::<u128>()..]))
}

/// Trait implemented by all consensus header types.
///
/// Every header is exactly [`HEADER_SIZE`] bytes, `#[repr(C)]`, and supports
/// zero-copy deserialization via `bytemuck`.
///
/// # Alignment
///
/// All headers contain `u128` fields → 16-byte alignment required by
/// `bytemuck::checked::try_from_bytes`. Production uses `Owned<MESSAGE_ALIGN>`
/// / `Frozen<MESSAGE_ALIGN>` (4096-aligned). `Vec<u8>` / `bytes::BytesMut`
/// request align=1; they over-align under glibc by accident but fail under
/// strict allocators (Miri, jemalloc, arenas). Use
/// `aligned_vec::AVec<u8, ConstAlign<16>>` for explicit alignment.
pub trait ConsensusHeader: Sized + CheckedBitPattern + NoUninit {
    const COMMAND: Command;

    /// Whether a frame carrying `command` may be typed as this header.
    /// Defaults to an exact match; a header that serves several commands
    /// with one layout (e.g. `RepairDone` / `RangeEvicted`) widens it.
    #[must_use]
    fn accepts(command: Command) -> bool {
        command == Self::COMMAND
    }

    /// Whether this header's `checksum` field seals the frame.
    ///
    /// True for replica-to-replica control frames, whose header carries every
    /// decision field: view number, commit point, and the nack bitset that
    /// authorises truncation. TCP's 16-bit checksum does not reliably catch a
    /// flipped bit on a plaintext replica link.
    ///
    /// False for three groups: [`PrepareHeader`] / [`RepairPrepareHeader`] spend
    /// `checksum` on [`PrepareHeader::identity_checksum`], which excludes `view` so
    /// a re-stamped prepare keeps one identity, and a seal cannot share the field;
    /// [`RequestHeader`] / [`ReplyHeader`] / [`EvictionHeader`] cross the client
    /// boundary, so sealing them is an SDK change on both ends; [`GenericHeader`] is
    /// the type-erased pre-dispatch view and defers to the typed parse, where
    /// [`Self::verify_frame`] runs.
    ///
    /// Required, not defaulted: [`Self::seal`] on an unsealed type overwrites the
    /// identity checksum with a frame checksum, and in release only a `debug_assert`
    /// stands in the way.
    const FRAME_SEALED: bool;

    /// # Errors
    /// Returns `ConsensusError` if the header fields are inconsistent.
    fn validate(&self) -> Result<(), ConsensusError>;
    fn operation(&self) -> Operation;
    fn command(&self) -> Command;
    fn size(&self) -> u32;

    /// The `checksum` field, whatever this header spends it on.
    fn checksum(&self) -> u128;

    /// Overwrite the `checksum` field.
    fn set_checksum(&mut self, checksum: u128);

    /// Checksum over every byte of the header past `checksum` itself.
    ///
    /// `checksum_body` sits inside that range, so sealing the header also
    /// pins the body seal, and the two together cover the whole frame.
    #[must_use]
    fn frame_checksum(&self) -> u128 {
        let bytes: &[u8; HEADER_SIZE] = bytemuck::bytes_of(self)
            .try_into()
            .expect("every consensus header is HEADER_SIZE bytes");
        frame_checksum_bytes(bytes)
    }

    /// Stamp [`Self::frame_checksum`]. Call last when building a frame: it covers
    /// every other field, `checksum_body` included, so later writes are uncovered.
    fn seal(&mut self) {
        debug_assert!(
            Self::FRAME_SEALED,
            "sealing a header whose checksum field means something else",
        );
        let checksum = self.frame_checksum();
        self.set_checksum(checksum);
    }

    /// Reject a frame whose header does not match its own checksum.
    ///
    /// Runs before [`Self::validate`] on every typed parse: a header that did not
    /// arrive intact cannot have any field believed, `validate`'s included.
    ///
    /// # Errors
    /// [`ConsensusError::FrameChecksumMismatch`] on a bad seal. Unsealed header
    /// types return `Ok` unconditionally.
    fn verify_frame(&self) -> Result<(), ConsensusError> {
        if !Self::FRAME_SEALED {
            return Ok(());
        }
        let expected = self.frame_checksum();
        let found = self.checksum();
        if found == expected {
            Ok(())
        } else {
            Err(ConsensusError::FrameChecksumMismatch {
                command: self.command(),
                expected,
                found,
            })
        }
    }
}

// GenericHeader - type-erased dispatch

/// Type-erased 256-byte header for initial message dispatch.
#[repr(C)]
#[derive(Debug, Clone, Copy, CheckedBitPattern, NoUninit)]
pub struct GenericHeader {
    pub checksum: u128,
    pub checksum_body: u128,
    pub cluster: u128,
    pub size: u32,
    pub view: u32,
    pub release: u32,
    pub command: Command,
    pub replica: u8,
    pub reserved_frame: [u8; 66],
    pub reserved_command: [u8; RESERVED_COMMAND_LEN],
}
const _: () = {
    assert!(size_of::<GenericHeader>() == HEADER_SIZE);
    assert!(
        offset_of!(GenericHeader, size) == SIZE_FIELD_OFFSET,
        "GenericHeader.size offset drifted; transports decode wrong bytes",
    );
    assert!(
        offset_of!(GenericHeader, reserved_command)
            == offset_of!(GenericHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
    assert!(
        offset_of!(GenericHeader, reserved_command) + size_of::<[u8; RESERVED_COMMAND_LEN]>()
            == HEADER_SIZE
    );
};

impl ConsensusHeader for GenericHeader {
    const COMMAND: Command = Command::Reserved;
    const FRAME_SEALED: bool = false;

    fn checksum(&self) -> u128 {
        self.checksum
    }

    fn set_checksum(&mut self, checksum: u128) {
        self.checksum = checksum;
    }
    fn operation(&self) -> Operation {
        Operation::Reserved
    }
    fn command(&self) -> Command {
        self.command
    }
    fn validate(&self) -> Result<(), ConsensusError> {
        Ok(())
    }
    fn size(&self) -> u32 {
        self.size
    }
}

// RequestHeader - client -> primary

/// Client -> primary request header. 256 bytes.
#[repr(C)]
#[derive(Debug, Clone, Copy, CheckedBitPattern, NoUninit)]
pub struct RequestHeader {
    pub checksum: u128,
    pub checksum_body: u128,
    pub cluster: u128,
    pub size: u32,
    pub view: u32,
    pub release: u32,
    pub command: Command,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    pub client: u128,
    /// Integrity stamp over the request payload, used by the client table to
    /// catch a `request` number reused for a different operation: a retry that
    /// disagrees with the stamp of the cached reply is refused rather than
    /// answered with the wrong reply. Zero means unstamped, which disables the
    /// comparison; the wire currently sends zero.
    pub request_checksum: u128,
    pub timestamp: u64,
    pub request: u64,
    pub operation: Operation,
    pub operation_padding: [u8; 7],
    /// Session fence epoch: the commit op of the latest committed `Register`
    /// for this `client`. Handed to the client by that register's reply and
    /// echoed on every subsequent request.
    ///
    /// Every bind commits a `Register`, so each rebind of the same `client`
    /// carries a strictly higher value, and a request stamped with an older one
    /// is a zombie from before that rebind and gets fenced. Being a log
    /// position rather than a counter is what makes it non-regressing: it
    /// cannot restart low after the server drops an entry and the client
    /// registers again.
    ///
    /// Zero on `Register` itself (the client has no epoch to echo yet) and on
    /// sessionless ops; header validation enforces both.
    pub session: u64,
    /// Acting user id, stamped by the metadata primary at admission for every
    /// gated client op so the in-apply RBAC gate resolves the same identity on
    /// every replica; on `Register` it carries the freshly authenticated user.
    /// The submitter's wire value is never trusted. Zero for `Logout`,
    /// partition-plane, and server-internal ops.
    pub user_id: u32,
    pub reserved: [u8; 60],
}
const _: () = {
    assert!(size_of::<RequestHeader>() == HEADER_SIZE);
    assert!(
        offset_of!(RequestHeader, client)
            == offset_of!(RequestHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
    assert!(
        offset_of!(RequestHeader, user_id) == offset_of!(RequestHeader, session) + size_of::<u64>()
    );
    assert!(offset_of!(RequestHeader, reserved) + size_of::<[u8; 60]>() == HEADER_SIZE);
};

/// A [`RequestHeader`] AFTER the receiving node resolved its target.
///
/// The server-internal shape a client request travels in between shards
/// (and follower to primary), never on the client wire. `group` carries the
/// resolved consensus group so the owner shard can route, park, and fence
/// WITHOUT re-decoding the payload -- clients no longer send any namespace
/// (it is derived: plane from `operation`, partition group from the body),
/// so this is where the derivation result lives for the internal hop.
///
/// Layout: identical to [`RequestHeader`] with `group` claiming the LAST
/// eight reserved bytes (the tail, bytes 248..256); the leading 52 reserved
/// bytes keep their client-wire meaning, so promotion is a same-size copy.
#[repr(C)]
#[derive(Debug, Clone, Copy, CheckedBitPattern, NoUninit)]
pub struct RoutedRequestHeader {
    pub checksum: u128,
    pub checksum_body: u128,
    pub cluster: u128,
    pub size: u32,
    pub view: u32,
    pub release: u32,
    pub command: Command,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    pub client: u128,
    pub request_checksum: u128,
    pub timestamp: u64,
    pub request: u64,
    pub operation: Operation,
    pub operation_padding: [u8; 7],
    pub session: u64,
    pub user_id: u32,
    /// Same offset and meaning as the leading 52 bytes of
    /// `RequestHeader::reserved` -- this region CARRIES DATA (the
    /// non-replicated op code range), so `group` must not displace it.
    pub reserved: [u8; 52],
    /// The resolved consensus group id (see `binary_protocol::namespace`),
    /// claiming the TAIL of the client header's reserved area.
    pub group: u64,
}
const _: () = {
    assert!(size_of::<RoutedRequestHeader>() == HEADER_SIZE);
    // Every field shared with `RequestHeader` sits at the same offset --
    // including the data-bearing prefix of `reserved` (the non-replicated
    // code range) -- so promotion preserves everything a client sent.
    assert!(offset_of!(RoutedRequestHeader, client) == offset_of!(RequestHeader, client));
    assert!(offset_of!(RoutedRequestHeader, session) == offset_of!(RequestHeader, session));
    assert!(offset_of!(RoutedRequestHeader, user_id) == offset_of!(RequestHeader, user_id));
    assert!(offset_of!(RoutedRequestHeader, reserved) == offset_of!(RequestHeader, reserved));
    assert!(offset_of!(RoutedRequestHeader, group) + size_of::<u64>() == HEADER_SIZE);
};

impl Default for RoutedRequestHeader {
    fn default() -> Self {
        Self {
            checksum: 0,
            checksum_body: 0,
            cluster: 0,
            size: 0,
            view: 0,
            release: 0,
            command: Command::Reserved,
            replica: 0,
            reserved_frame: [0; 66],
            client: 0,
            request_checksum: 0,
            timestamp: 0,
            request: 0,
            operation: Operation::Reserved,
            operation_padding: [0; 7],
            session: 0,
            user_id: 0,
            reserved: [0; 52],
            group: 0,
        }
    }
}

impl Default for RequestHeader {
    fn default() -> Self {
        Self {
            checksum: 0,
            checksum_body: 0,
            cluster: 0,
            size: 0,
            view: 0,
            release: 0,
            command: Command::Reserved,
            replica: 0,
            reserved_frame: [0; 66],
            client: 0,
            request_checksum: 0,
            timestamp: 0,
            request: 0,
            operation: Operation::Reserved,
            operation_padding: [0; 7],
            session: 0,
            user_id: 0,
            reserved: [0; 60],
        }
    }
}

/// Field rules shared by the client-wire [`RequestHeader`] and the
/// server-internal [`RoutedRequestHeader`]. The routed shape is decoded
/// straight off the peer wire (`MessageBag`), so it must reject everything
/// the client boundary rejects; validating only the command byte there
/// would let a peer frame carry `client = 0` into the client table's hard
/// assert, or `operation = Reserved` into a cached-register replay.
fn validate_request_fields(
    client: u128,
    operation: Operation,
    session: u64,
    request: u64,
) -> Result<(), ConsensusError> {
    if client == 0 {
        return Err(ConsensusError::InvalidField(
            "request: client must be != 0".to_string(),
        ));
    }
    // Reserved is the zero value, never a real client op
    // (`is_client_allowed` rejects it). Refusing it here rather than after
    // the dedup preflight matters: a bound client sending
    // `Reserved, request = 0` used to pass validation, reach
    // `request_preflight`, hit its own watermark and get its register
    // reply replayed before the operation gate ever ran.
    if operation == Operation::Reserved {
        return Err(ConsensusError::InvalidField(
            "operation must not be Reserved".to_string(),
        ));
    }
    // Register: session must be 0, request must be 0.
    // NonReplicated: sessionless by design (the `ClientTable` ignores
    // these ops and the server routes/auth-gates them by transport id),
    // so a pre-register client may legitimately send session 0 --
    // ping must work before authentication.
    // Other non-register ops: session must be > 0, request must be > 0.
    if operation == Operation::Register {
        if session != 0 {
            return Err(ConsensusError::InvalidField(
                "register: session must be 0".to_string(),
            ));
        }
        if request != 0 {
            return Err(ConsensusError::InvalidField(
                "register: request must be 0".to_string(),
            ));
        }
    } else if operation != Operation::NonReplicated {
        if session == 0 {
            return Err(ConsensusError::InvalidField(
                "non-register: session must be > 0".to_string(),
            ));
        }
        if request == 0 {
            return Err(ConsensusError::InvalidField(
                "non-register: request must be > 0".to_string(),
            ));
        }
    }
    Ok(())
}

impl ConsensusHeader for RoutedRequestHeader {
    const COMMAND: Command = Command::Request;
    /// The client-wire [`RequestHeader`] this is promoted from is unsealed, and the
    /// promotion copies `checksum` verbatim, so there is nothing here to verify.
    const FRAME_SEALED: bool = false;

    fn checksum(&self) -> u128 {
        self.checksum
    }

    fn set_checksum(&mut self, checksum: u128) {
        self.checksum = checksum;
    }
    fn operation(&self) -> Operation {
        self.operation
    }
    fn command(&self) -> Command {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command::Request {
            return Err(ConsensusError::InvalidCommand {
                expected: Command::Request,
                found: self.command,
            });
        }
        validate_request_fields(self.client, self.operation, self.session, self.request)
    }
}

impl ConsensusHeader for RequestHeader {
    const COMMAND: Command = Command::Request;
    const FRAME_SEALED: bool = false;

    fn checksum(&self) -> u128 {
        self.checksum
    }

    fn set_checksum(&mut self, checksum: u128) {
        self.checksum = checksum;
    }
    fn operation(&self) -> Operation {
        self.operation
    }
    fn command(&self) -> Command {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command::Request {
            return Err(ConsensusError::InvalidCommand {
                expected: Command::Request,
                found: self.command,
            });
        }
        validate_request_fields(self.client, self.operation, self.session, self.request)
    }
}

// ReplyHeader - primary -> client

/// Primary -> client reply header. 256 bytes.
#[repr(C)]
#[derive(Debug, Clone, Copy, CheckedBitPattern, NoUninit)]
pub struct ReplyHeader {
    pub checksum: u128,
    pub checksum_body: u128,
    pub cluster: u128,
    pub size: u32,
    pub view: u32,
    pub release: u32,
    pub command: Command,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    /// Echoed from the request this reply answers; the client table stores it
    /// alongside the cached reply. See `RequestHeader::request_checksum`.
    pub request_checksum: u128,
    pub context: u128,
    pub client: u128,
    pub op: u64,
    pub commit: u64,
    pub timestamp: u64,
    pub request: u64,
    pub operation: Operation,
    pub operation_padding: [u8; 7],
    /// Request-level status: 0 = ok; nonzero = the `IggyError` code for a
    /// failure decided before commit (e.g. a dispatch-time authorization
    /// denial, or the partition primary rejecting a consumer-offset op).
    ///
    /// Contract: this is nonzero ONLY on a pre-commit denial, and a deny
    /// reply always carries an EMPTY body. So this header channel and the
    /// committed per-sub-op results in the metadata result section are mutually
    /// exclusive by construction: a reply either commits (status 0, result
    /// section present) or is denied before commit (status set, no body), and a
    /// consumer never reconciles the two. Carved from `reserved` exactly like
    /// `user_id` in `RequestHeader` / `PrepareHeader`; no existing field offset
    /// moves and `validate` does not inspect it.
    pub status: u32,
    pub reserved: [u8; 36],
}
const _: () = {
    assert!(size_of::<ReplyHeader>() == HEADER_SIZE);
    assert!(
        offset_of!(ReplyHeader, request_checksum)
            == offset_of!(ReplyHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
    assert!(offset_of!(ReplyHeader, reserved) + size_of::<[u8; 36]>() == HEADER_SIZE);
};

impl Default for ReplyHeader {
    fn default() -> Self {
        Self {
            checksum: 0,
            checksum_body: 0,
            cluster: 0,
            size: 0,
            view: 0,
            release: 0,
            command: Command::Reserved,
            replica: 0,
            reserved_frame: [0; 66],
            request_checksum: 0,
            context: 0,
            client: 0,
            op: 0,
            commit: 0,
            timestamp: 0,
            request: 0,
            operation: Operation::Reserved,
            operation_padding: [0; 7],
            status: 0,
            reserved: [0; 36],
        }
    }
}

impl ConsensusHeader for ReplyHeader {
    const COMMAND: Command = Command::Reply;
    const FRAME_SEALED: bool = false;

    fn checksum(&self) -> u128 {
        self.checksum
    }

    fn set_checksum(&mut self, checksum: u128) {
        self.checksum = checksum;
    }
    fn operation(&self) -> Operation {
        self.operation
    }
    fn command(&self) -> Command {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command::Reply {
            return Err(ConsensusError::ReplyInvalidCommand);
        }
        Ok(())
    }
}

// EvictionReason, wire-level reason in EvictionHeader.
//
// Discriminants pinned: any reorder/reuse breaks SDK decoders. New
// variants also break old SDKs (CheckedBitPattern fails). Coordinate
// SDK releases on every extension.

/// Wire reason on [`EvictionHeader`]. Session-terminal; never transient.
/// No `Default`: callers must name reason so `..default()` can't ship
/// `Reserved`.
///
/// **Wire-version pinned.**
#[derive(Debug, Clone, Copy, PartialEq, Eq, NoUninit, CheckedBitPattern)]
#[repr(u8)]
pub enum EvictionReason {
    /// Sentinel; rejected on wire.
    Reserved = 0,

    /// No session for `client_id`.
    NoSession = 1,
    /// Client release < cluster min.
    ClientReleaseTooLow = 2,
    /// Client release > cluster max.
    ClientReleaseTooHigh = 3,
    /// Invalid operation discriminant.
    InvalidRequestOperation = 4,
    /// Body failed state-machine validation.
    InvalidRequestBody = 5,
    /// Body size mismatch.
    InvalidRequestBodySize = 6,
    /// Session < cluster retained minimum.
    SessionTooLow = 7,
    /// Session release ≠ client's current release.
    SessionReleaseMismatch = 8,

    // iggy-specific (9..).
    InvalidCredentials = 9,
    InvalidToken = 10,
    UserInactive = 11,
    SessionError = 12,
    /// Client missed heartbeats past the configured threshold; the server
    /// evicted the session (and dropped it from any consumer groups).
    StaleClient = 13,
    /// Client protocol version outside the server's accepted range; the
    /// header carries `server_protocol_version{,_min}` so the SDK can
    /// report the exact window.
    IncompatibleProtocol = 14,
    /// Login body without a decodable `ClientVersionInfo` prefix.
    MalformedLogin = 15,
}

// EvictionHeader - primary -> client (session-terminal, no body)

/// Primary→client: session-terminal eviction. 256 bytes, header-only.
/// Session-level ("your session is dead, deinit"), no per-request
/// correlation. SDK fires eviction callback and stops. Never transient.
#[repr(C)]
#[derive(Debug, Clone, Copy, CheckedBitPattern, NoUninit)]
pub struct EvictionHeader {
    pub checksum: u128,
    pub checksum_body: u128,
    pub cluster: u128,
    pub size: u32,
    pub view: u32,
    pub release: u32,
    pub command: Command,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    pub client: u128,
    /// Accepted protocol window on `IncompatibleProtocol`; zero otherwise.
    pub server_protocol_version: u32,
    pub server_protocol_version_min: u32,
    pub reserved: [u8; 103],
    pub reason: EvictionReason,
}
const _: () = {
    assert!(size_of::<EvictionHeader>() == HEADER_SIZE);
    assert!(
        offset_of!(EvictionHeader, client)
            == offset_of!(EvictionHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
    assert!(offset_of!(EvictionHeader, server_protocol_version) == 144);
    assert!(offset_of!(EvictionHeader, server_protocol_version_min) == 148);
    assert!(offset_of!(EvictionHeader, reason) + size_of::<EvictionReason>() == HEADER_SIZE);
};

// No `Default`: forces use of [`EvictionHeader::new`] so wire-required
// `reason` can't be filled via `..default()`.

impl EvictionHeader {
    /// Build well-formed header. Wire-required fields set; rest zeroed.
    ///
    /// # Panics (debug)
    /// On `ClientReleaseTooLow`/`ClientReleaseTooHigh`: `release` hardcoded
    /// to 0, those reasons need real bounds. Add `release_min`/`release_max`
    /// params before emitting them. On `IncompatibleProtocol`: needs the
    /// accepted protocol window, use [`Self::incompatible_protocol`] instead.
    ///
    /// # Safety
    /// `client` must be non-zero , `validate` rejects zero so SDKs can
    /// route the frame back to the originating handler.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub const fn new(
        cluster: u128,
        view: u32,
        replica: u8,
        client: u128,
        reason: EvictionReason,
    ) -> Self {
        debug_assert!(
            !matches!(
                reason,
                EvictionReason::ClientReleaseTooLow
                    | EvictionReason::ClientReleaseTooHigh
                    | EvictionReason::IncompatibleProtocol,
            ),
            "EvictionHeader::new: ClientRelease*/IncompatibleProtocol need extra fields",
        );
        // Cap from consensus REPLICAS_MAX=32; literal here to avoid
        // wire-proto crate depending on consensus crate.
        debug_assert!(
            replica < 32,
            "EvictionHeader::new: replica >= REPLICAS_MAX(32)",
        );
        Self {
            checksum: 0,
            checksum_body: 0,
            cluster,
            size: HEADER_SIZE as u32,
            view,
            release: 0,
            command: Command::Eviction,
            replica,
            reserved_frame: [0; 66],
            client,
            server_protocol_version: 0,
            server_protocol_version_min: 0,
            reserved: [0; 103],
            reason,
        }
    }

    /// Protocol-version rejection carrying the accepted window so the SDK
    /// can report `client X, server accepts [min, max]`.
    #[must_use]
    pub const fn incompatible_protocol(
        cluster: u128,
        view: u32,
        replica: u8,
        client: u128,
        server_protocol_version: u32,
        server_protocol_version_min: u32,
    ) -> Self {
        let mut header = Self::new(cluster, view, replica, client, EvictionReason::NoSession);
        header.reason = EvictionReason::IncompatibleProtocol;
        header.server_protocol_version = server_protocol_version;
        header.server_protocol_version_min = server_protocol_version_min;
        header
    }
}

impl ConsensusHeader for EvictionHeader {
    const COMMAND: Command = Command::Eviction;
    const FRAME_SEALED: bool = false;

    fn checksum(&self) -> u128 {
        self.checksum
    }

    fn set_checksum(&mut self, checksum: u128) {
        self.checksum = checksum;
    }
    /// Session-level (not per-op): always `Reserved`.
    fn operation(&self) -> Operation {
        Operation::Reserved
    }
    fn command(&self) -> Command {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    #[allow(clippy::cast_possible_truncation)]
    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command::Eviction {
            return Err(ConsensusError::InvalidCommand {
                expected: Command::Eviction,
                found: self.command,
            });
        }
        if self.size as usize != HEADER_SIZE {
            return Err(ConsensusError::InvalidSize {
                expected: HEADER_SIZE as u32,
                found: self.size,
            });
        }
        // Non-zero client_id so SDK can route back to client handler.
        if self.client == 0 {
            return Err(ConsensusError::InvalidField(
                "eviction: client must be != 0".to_string(),
            ));
        }

        // Validate BOTH reserved regions to block forward-compat smuggling:
        // a future field carved from reserved_frame would be silently zero
        // on old peers. Strict zero-check forces release bump.
        if self.reserved_frame.iter().any(|&b| b != 0) {
            return Err(ConsensusError::InvalidField(
                "eviction: reserved_frame bytes must be zero".to_string(),
            ));
        }
        if self.reserved.iter().any(|&b| b != 0) {
            return Err(ConsensusError::InvalidField(
                "eviction: reserved bytes must be zero".to_string(),
            ));
        }
        // Reserved on wire = sender forgot to set reason.
        if self.reason == EvictionReason::Reserved {
            return Err(ConsensusError::InvalidField(
                "eviction: reason must not be Reserved".to_string(),
            ));
        }
        // Protocol window only travels on IncompatibleProtocol; anywhere
        // else a nonzero value is smuggling (same rule as reserved bytes).
        if self.reason == EvictionReason::IncompatibleProtocol {
            if self.server_protocol_version_min == 0
                || self.server_protocol_version < self.server_protocol_version_min
            {
                return Err(ConsensusError::InvalidField(
                    "eviction: incompatible-protocol window must satisfy 1 <= min <= max"
                        .to_string(),
                ));
            }
        } else if self.server_protocol_version != 0 || self.server_protocol_version_min != 0 {
            return Err(ConsensusError::InvalidField(
                "eviction: protocol window bytes must be zero".to_string(),
            ));
        }
        Ok(())
    }
}

// PrepareHeader - primary -> replicas (replication)

/// Primary -> replicas: replicate this operation.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, CheckedBitPattern, NoUninit)]
pub struct PrepareHeader {
    pub checksum: u128,
    pub checksum_body: u128,
    pub cluster: u128,
    pub size: u32,
    pub view: u32,
    pub release: u32,
    pub command: Command,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    pub client: u128,
    pub parent: u128,
    /// Copied verbatim from the admitted `RequestHeader`; see that field.
    pub request_checksum: u128,
    pub op: u64,
    pub commit: u64,
    pub timestamp: u64,
    pub request: u64,
    pub operation: Operation,
    pub operation_padding: [u8; 7],
    /// Consensus group id: which of the node's multiplexed VSR groups this
    /// frame belongs to. `METADATA_GROUP` (top bit) for the metadata plane,
    /// otherwise the partition's packed stream-topic-partition key. The
    /// demux and repair-replay routing key; see `binary_protocol::namespace`
    /// for the value-space contract.
    pub group: u64,
    /// Acting user id, copied verbatim from the admitted `RequestHeader`; see
    /// that field for the stamping contract.
    pub user_id: u32,
    pub reserved: [u8; 28],
}
const _: () = {
    assert!(size_of::<PrepareHeader>() == HEADER_SIZE);
    assert!(
        offset_of!(PrepareHeader, client)
            == offset_of!(PrepareHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
    assert!(
        offset_of!(PrepareHeader, user_id) == offset_of!(PrepareHeader, group) + size_of::<u64>()
    );
    assert!(offset_of!(PrepareHeader, reserved) + size_of::<[u8; 28]>() == HEADER_SIZE);
};

impl Default for PrepareHeader {
    fn default() -> Self {
        Self {
            checksum: 0,
            checksum_body: 0,
            cluster: 0,
            size: 0,
            view: 0,
            release: 0,
            command: Command::Reserved,
            replica: 0,
            reserved_frame: [0; 66],
            client: 0,
            parent: 0,
            request_checksum: 0,
            op: 0,
            commit: 0,
            timestamp: 0,
            request: 0,
            operation: Operation::Reserved,
            operation_padding: [0; 7],
            group: 0,
            user_id: 0,
            reserved: [0; 28],
        }
    }
}

impl ConsensusHeader for PrepareHeader {
    const COMMAND: Command = Command::Prepare;
    const FRAME_SEALED: bool = false;

    fn checksum(&self) -> u128 {
        self.checksum
    }

    fn set_checksum(&mut self, checksum: u128) {
        self.checksum = checksum;
    }
    fn operation(&self) -> Operation {
        self.operation
    }
    fn command(&self) -> Command {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command::Prepare {
            return Err(ConsensusError::InvalidCommand {
                expected: Command::Prepare,
                found: self.command,
            });
        }
        // Both reserved regions must be zero. They sit inside
        // [`Self::identity_checksum`], so a peer that fills them changes the op's
        // identity while changing nothing the merge can see; and `dvc_blank`
        // classifies a slot by exact struct equality, so a non-zero reserved byte
        // turns a blank into a `Valid` header the merge then indexes.
        if self.reserved_frame.iter().any(|&byte| byte != 0) {
            return Err(ConsensusError::InvalidField(
                "prepare: reserved_frame bytes must be zero".to_string(),
            ));
        }
        if self.reserved.iter().any(|&byte| byte != 0) {
            return Err(ConsensusError::InvalidField(
                "prepare: reserved bytes must be zero".to_string(),
            ));
        }
        Ok(())
    }
}

/// `checksum` of a prepare no producer sealed.
///
/// Written by a build predating the identity seal, or by the partition plane.
/// Verification skips such entries so an older build's WAL still replays.
pub const CHECKSUM_UNSEALED: u128 = 0;

/// The frame's body, bounded by `size`. What `checksum_body` covers.
///
/// Not `&frame[HEADER_SIZE..]`: `Message::try_from` accepts a buffer longer than
/// `size` without trimming, while the WAL scan reads exactly `size`, so slicing to
/// the end makes the two disagree. Empty when `size` overruns the buffer.
#[must_use]
pub fn frame_body(frame: &[u8], size: u32) -> &[u8] {
    let end = size as usize;
    if end <= HEADER_SIZE || end > frame.len() {
        return &[];
    }
    &frame[HEADER_SIZE..end]
}

impl PrepareHeader {
    /// Which prepare this is, independent of which view re-sent it.
    ///
    /// Covers the whole 256-byte header except `checksum` (a field cannot hash
    /// itself) and `view`, so a retransmission that re-stamps `view` stays valid.
    /// The body reaches the value through the covered `checksum_body`.
    ///
    /// Lives here, not in the consensus crate, because the WAL scan verifies it too
    /// and the two must agree byte for byte: it hashes this struct's layout.
    #[must_use]
    pub fn identity_checksum(&self) -> u128 {
        let mut covered = *self;
        covered.checksum = 0;
        covered.view = 0;
        u128::from(twox_hash::XxHash3_64::oneshot(bytemuck::bytes_of(&covered)))
    }
}

// RepairPrepareHeader - repair peer -> recovering replica (journal repair)

/// A stored prepare served for journal repair.
///
/// Byte-identical to [`PrepareHeader`] except the command, so the recovering
/// replica routes it to the fence-free repair ingest instead of live
/// replication. The newtype keeps the frame typed as `RepairPrepare` across
/// every parse; converting to a live `Prepare` happens once, at the apply
/// site.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, CheckedBitPattern, NoUninit)]
pub struct RepairPrepareHeader(pub PrepareHeader);

impl ConsensusHeader for RepairPrepareHeader {
    const COMMAND: Command = Command::RepairPrepare;
    const FRAME_SEALED: bool = false;

    fn checksum(&self) -> u128 {
        self.0.checksum
    }

    fn set_checksum(&mut self, checksum: u128) {
        self.0.checksum = checksum;
    }
    fn operation(&self) -> Operation {
        self.0.operation
    }
    fn command(&self) -> Command {
        self.0.command
    }
    fn size(&self) -> u32 {
        self.0.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.0.command != Command::RepairPrepare {
            return Err(ConsensusError::InvalidCommand {
                expected: Command::RepairPrepare,
                found: self.0.command,
            });
        }
        // Same rule as `PrepareHeader::validate`, same reason: the regions sit inside
        // `identity_checksum`, and a repaired prepare is journaled and later re-read
        // as a DVC suffix entry, where `dvc_blank`'s exact-equality classification is
        // what a dirty byte defeats. Not delegated, so the command check above stays
        // `RepairPrepare`.
        if self.0.reserved_frame.iter().any(|&byte| byte != 0) {
            return Err(ConsensusError::InvalidField(
                "repair_prepare: reserved_frame bytes must be zero".to_string(),
            ));
        }
        if self.0.reserved.iter().any(|&byte| byte != 0) {
            return Err(ConsensusError::InvalidField(
                "repair_prepare: reserved bytes must be zero".to_string(),
            ));
        }
        Ok(())
    }
}

// PrepareOkHeader - replica -> primary (acknowledgement)

/// Replica -> primary: acknowledge a Prepare.
#[repr(C)]
#[derive(Debug, Clone, Copy, CheckedBitPattern, NoUninit)]
pub struct PrepareOkHeader {
    pub checksum: u128,
    pub checksum_body: u128,
    pub cluster: u128,
    pub size: u32,
    pub view: u32,
    pub release: u32,
    pub command: Command,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    pub parent: u128,
    pub prepare_checksum: u128,
    pub op: u64,
    pub commit: u64,
    pub timestamp: u64,
    pub request: u64,
    pub operation: Operation,
    pub operation_padding: [u8; 7],
    pub group: u64,
    pub reserved: [u8; 48],
}
const _: () = {
    assert!(size_of::<PrepareOkHeader>() == HEADER_SIZE);
    assert!(
        offset_of!(PrepareOkHeader, parent)
            == offset_of!(PrepareOkHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
    assert!(offset_of!(PrepareOkHeader, reserved) + size_of::<[u8; 48]>() == HEADER_SIZE);
};

impl Default for PrepareOkHeader {
    fn default() -> Self {
        Self {
            checksum: 0,
            checksum_body: 0,
            cluster: 0,
            size: 0,
            view: 0,
            release: 0,
            command: Command::Reserved,
            replica: 0,
            reserved_frame: [0; 66],
            parent: 0,
            prepare_checksum: 0,
            op: 0,
            commit: 0,
            timestamp: 0,
            request: 0,
            operation: Operation::Reserved,
            operation_padding: [0; 7],
            group: 0,
            reserved: [0; 48],
        }
    }
}

impl ConsensusHeader for PrepareOkHeader {
    const FRAME_SEALED: bool = true;

    const COMMAND: Command = Command::PrepareOk;

    fn checksum(&self) -> u128 {
        self.checksum
    }

    fn set_checksum(&mut self, checksum: u128) {
        self.checksum = checksum;
    }
    fn operation(&self) -> Operation {
        self.operation
    }
    fn command(&self) -> Command {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command::PrepareOk {
            return Err(ConsensusError::InvalidCommand {
                expected: Command::PrepareOk,
                found: self.command,
            });
        }
        Ok(())
    }
}

// CommitHeader - primary -> replicas (commit, header-only)

/// Primary -> replicas: commit up to this op. Header-only (no body).
#[repr(C)]
#[derive(Debug, Clone, Copy, CheckedBitPattern, NoUninit)]
pub struct CommitHeader {
    pub checksum: u128,
    pub checksum_body: u128,
    pub cluster: u128,
    pub size: u32,
    pub view: u32,
    pub release: u32,
    pub command: Command,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    pub commit_checksum: u128,
    pub timestamp_monotonic: u64,
    pub commit: u64,
    pub checkpoint_op: u64,
    pub group: u64,
    pub reserved: [u8; 80],
}
const _: () = {
    assert!(size_of::<CommitHeader>() == HEADER_SIZE);
    assert!(
        offset_of!(CommitHeader, commit_checksum)
            == offset_of!(CommitHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
    assert!(offset_of!(CommitHeader, reserved) + size_of::<[u8; 80]>() == HEADER_SIZE);
};

impl ConsensusHeader for CommitHeader {
    const FRAME_SEALED: bool = true;

    const COMMAND: Command = Command::Commit;

    fn checksum(&self) -> u128 {
        self.checksum
    }

    fn set_checksum(&mut self, checksum: u128) {
        self.checksum = checksum;
    }
    fn operation(&self) -> Operation {
        Operation::Reserved
    }
    fn command(&self) -> Command {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command::Commit {
            return Err(ConsensusError::CommitInvalidCommand);
        }
        if self.size != 256 {
            return Err(ConsensusError::CommitInvalidSize(self.size));
        }
        Ok(())
    }
}

// StartViewChangeHeader - failure detection (header-only)

/// Replica suspects primary failure. Header-only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, CheckedBitPattern, NoUninit)]
#[repr(C)]
pub struct StartViewChangeHeader {
    pub checksum: u128,
    pub checksum_body: u128,
    pub cluster: u128,
    pub size: u32,
    pub view: u32,
    pub release: u32,
    pub command: Command,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    pub group: u64,
    pub reserved: [u8; 120],
}
const _: () = {
    assert!(size_of::<StartViewChangeHeader>() == HEADER_SIZE);
    assert!(
        offset_of!(StartViewChangeHeader, group)
            == offset_of!(StartViewChangeHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
    assert!(offset_of!(StartViewChangeHeader, reserved) + size_of::<[u8; 120]>() == HEADER_SIZE);
};

impl ConsensusHeader for StartViewChangeHeader {
    const FRAME_SEALED: bool = true;

    const COMMAND: Command = Command::StartViewChange;

    fn checksum(&self) -> u128 {
        self.checksum
    }

    fn set_checksum(&mut self, checksum: u128) {
        self.checksum = checksum;
    }
    fn operation(&self) -> Operation {
        Operation::Reserved
    }
    fn command(&self) -> Command {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command::StartViewChange {
            return Err(ConsensusError::InvalidCommand {
                expected: Command::StartViewChange,
                found: self.command,
            });
        }
        if self.release != 0 {
            return Err(ConsensusError::InvalidField("release != 0".to_string()));
        }
        Ok(())
    }
}

// DoViewChangeHeader - view change vote (header-only)

/// Replica -> primary candidate: vote for view change. Header-only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, CheckedBitPattern, NoUninit)]
#[repr(C)]
pub struct DoViewChangeHeader {
    pub checksum: u128,
    pub checksum_body: u128,
    pub cluster: u128,
    pub size: u32,
    pub view: u32,
    pub release: u32,
    pub command: Command,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    /// Highest op-number in this replica's log.
    pub op: u64,
    /// Highest committed op.
    pub commit: u64,
    pub group: u64,
    /// View when status was last normal (key for log selection).
    pub log_view: u32,
    pub reserved: [u8; 68],
    /// Bit `i` set means the sender proves it never prepared suffix entry `i`, so
    /// that entry never reached a replication quorum through this replica. A new
    /// primary may truncate only once `quorum_nack_prepare` senders nack an entry;
    /// short of that it might be committed and must be preserved.
    ///
    /// A corrupt local entry is deliberately NOT nacked: the sender cannot tell a
    /// prepare it never saw from one it saw and lost, and only the former is proof.
    /// Silence costs availability; a false nack costs data.
    ///
    /// Carved from the tail of the former `reserved` region, with `present_bitset`
    /// LAST so both land 16-aligned with no padding and `op`/`commit`/`group`/
    /// `log_view` keep their offsets. A sender with nothing to nack sends zeros,
    /// decoding as "nacks nothing": safe, since that can only slow a view change.
    pub nack_bitset: u128,
    /// Bit `i` set means the sender can serve the BODY of suffix entry `i`, not just
    /// its header. The new primary needs one such sender per surviving entry, since
    /// a header whose body it cannot fetch is an entry it can never commit.
    ///
    /// Zero from a sender offering nothing, reading as "offers no bodies": safe,
    /// since the new primary waits rather than adopting an entry it cannot complete.
    pub present_bitset: u128,
}
const _: () = {
    assert!(size_of::<DoViewChangeHeader>() == HEADER_SIZE);
    assert!(
        offset_of!(DoViewChangeHeader, op)
            == offset_of!(DoViewChangeHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
    // op/commit/group/log_view keep their pre-bitset offsets.
    assert!(offset_of!(DoViewChangeHeader, reserved) == 156);
    // Both bitsets are last and 16-aligned, so the struct has no padding
    // (`NoUninit` would reject any).
    assert!(offset_of!(DoViewChangeHeader, nack_bitset) % 16 == 0);
    assert!(offset_of!(DoViewChangeHeader, present_bitset) % 16 == 0);
    assert!(
        offset_of!(DoViewChangeHeader, nack_bitset)
            == offset_of!(DoViewChangeHeader, reserved) + size_of::<[u8; 68]>()
    );
    assert!(offset_of!(DoViewChangeHeader, present_bitset) + size_of::<u128>() == HEADER_SIZE);
};

/// Suffix headers a `DoViewChange` may carry: one bit per entry in each of the two
/// `u128` bitsets.
///
/// Mirrors `consensus::DVC_HEADERS_MAX` as a literal so this crate need not depend
/// on the consensus crate, as with `REPLICAS_MAX` in [`EvictionHeader::new`].
pub const DVC_HEADERS_MAX: usize = 128;

impl ConsensusHeader for DoViewChangeHeader {
    const FRAME_SEALED: bool = true;

    const COMMAND: Command = Command::DoViewChange;

    fn checksum(&self) -> u128 {
        self.checksum
    }

    fn set_checksum(&mut self, checksum: u128) {
        self.checksum = checksum;
    }
    fn operation(&self) -> Operation {
        Operation::Reserved
    }
    fn command(&self) -> Command {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command::DoViewChange {
            return Err(ConsensusError::InvalidCommand {
                expected: Command::DoViewChange,
                found: self.command,
            });
        }
        if self.release != 0 {
            return Err(ConsensusError::InvalidField(
                "release must be 0".to_string(),
            ));
        }
        if self.log_view > self.view {
            return Err(ConsensusError::InvalidField(
                "log_view cannot exceed view".to_string(),
            ));
        }
        if self.commit > self.op {
            return Err(ConsensusError::InvalidField(
                "commit cannot exceed op".to_string(),
            ));
        }
        let suffix_len = self.suffix_len()?;
        // Bits past the suffix describe entries never sent: unchecked, a peer could
        // smuggle a nack for an op the new primary would then truncate.
        if suffix_len < DVC_HEADERS_MAX {
            let beyond = !((1u128 << suffix_len) - 1);
            if self.nack_bitset & beyond != 0 || self.present_bitset & beyond != 0 {
                return Err(ConsensusError::InvalidField(format!(
                    "do_view_change: bitset bits set past the {suffix_len}-entry suffix"
                )));
            }
        }
        Ok(())
    }
}

impl DoViewChangeHeader {
    /// Number of `PrepareHeader`s in the body.
    ///
    /// Zero is valid and means "no suffix": a replica with nothing uncommitted
    /// contributes numbers only.
    ///
    /// # Errors
    /// [`ConsensusError::InvalidField`] when `size` is short of the header, is not a
    /// whole number of headers, or exceeds what the bitsets can address.
    pub fn suffix_len(&self) -> Result<usize, ConsensusError> {
        suffix_len_of("do_view_change", self.size)
    }
}

/// Body length of a suffix-carrying control frame, in whole [`PrepareHeader`]s.
///
/// Shared by `DoViewChange` and `StartView`: same layout, same `DVC_HEADERS_MAX`
/// bound. `frame` only names the sender in the error text.
///
/// # Errors
/// [`ConsensusError::InvalidField`] when `size` is short of the header, is not a
/// whole number of headers, or exceeds what a view change can address.
fn suffix_len_of(frame: &str, size: u32) -> Result<usize, ConsensusError> {
    let size = size as usize;
    let Some(body_len) = size.checked_sub(HEADER_SIZE) else {
        return Err(ConsensusError::InvalidField(format!(
            "{frame}: size {size} is shorter than the {HEADER_SIZE}-byte header"
        )));
    };
    if body_len % HEADER_SIZE != 0 {
        return Err(ConsensusError::InvalidField(format!(
            "{frame}: body of {body_len} bytes is not a whole number of headers"
        )));
    }
    let suffix_len = body_len / HEADER_SIZE;
    if suffix_len > DVC_HEADERS_MAX {
        return Err(ConsensusError::InvalidField(format!(
            "{frame}: {suffix_len} suffix entries exceeds the maximum {DVC_HEADERS_MAX}"
        )));
    }
    Ok(suffix_len)
}

// StartViewHeader - new view announcement (header-only)

/// New primary -> all replicas: start new view. Header-only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, CheckedBitPattern, NoUninit)]
#[repr(C)]
pub struct StartViewHeader {
    pub checksum: u128,
    pub checksum_body: u128,
    pub cluster: u128,
    pub size: u32,
    pub view: u32,
    pub release: u32,
    pub command: Command,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    /// Highest op in the new primary's log.
    pub op: u64,
    /// max(commit) from all DVCs.
    pub commit: u64,
    pub group: u64,
    pub reserved: [u8; 88],
    /// Sender's incarnation, echoed from the `RequestStartView` this answers so a
    /// recovering replica can prove the reply post-dates its restart (see
    /// `RequestStartViewHeader::incarnation`). `0` on an unsolicited `StartView`
    /// (a normal view-change completion), which carries no freshness claim.
    ///
    /// Carved from the tail of the former `reserved` region and placed LAST so it
    /// lands 16-aligned with no padding WITHOUT moving `op`/`commit`/`group`.
    /// Zero is "no claim", which is what `handle_start_view` keys on and what the
    /// unsolicited completion path sends. NOT mixed-version tolerance: the frame seal
    /// drops a pre-seal peer before any field is read (see this module's header).
    pub incarnation: u128,
}
const _: () = {
    assert!(size_of::<StartViewHeader>() == HEADER_SIZE);
    // op/commit/group keep their pre-incarnation offsets.
    assert!(
        offset_of!(StartViewHeader, op)
            == offset_of!(StartViewHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
    // `incarnation` is last and 16-aligned, so the struct has no padding.
    assert!(offset_of!(StartViewHeader, incarnation) + size_of::<u128>() == HEADER_SIZE);
    assert!(offset_of!(StartViewHeader, incarnation) % 16 == 0);
};

impl ConsensusHeader for StartViewHeader {
    const FRAME_SEALED: bool = true;

    const COMMAND: Command = Command::StartView;

    fn checksum(&self) -> u128 {
        self.checksum
    }

    fn set_checksum(&mut self, checksum: u128) {
        self.checksum = checksum;
    }
    fn operation(&self) -> Operation {
        Operation::Reserved
    }
    fn command(&self) -> Command {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command::StartView {
            return Err(ConsensusError::InvalidCommand {
                expected: Command::StartView,
                found: self.command,
            });
        }
        if self.release != 0 {
            return Err(ConsensusError::InvalidField(
                "release must be 0".to_string(),
            ));
        }
        if self.commit > self.op {
            return Err(ConsensusError::InvalidField(
                "commit cannot exceed op".to_string(),
            ));
        }
        self.suffix_len()?;
        Ok(())
    }
}

impl StartViewHeader {
    /// Number of `PrepareHeader`s in the body: the view's suffix, high-to-low op
    /// from `op` down toward `commit`.
    ///
    /// Zero means numbers only, which is what the probe-answer path sends. A backup
    /// then falls back to trusting `op`.
    ///
    /// # Errors
    /// [`ConsensusError::InvalidField`] when `size` is short of the header, is not a
    /// whole number of headers, or exceeds what a view change can address.
    pub fn suffix_len(&self) -> Result<usize, ConsensusError> {
        suffix_len_of("start_view", self.size)
    }
}

// RequestStartViewHeader - restarted replica asking for the current view

/// Recovering replica -> all replicas: resend me the current `StartView`.
///
/// Header-only; only the current view's primary answers, with a targeted
/// `StartView`. Adoption is fenced by the receiver's view monotonicity and the
/// sender-is-primary check; `incarnation` additionally proves the reply
/// post-dates this replica's restart, so a `StartView` from a previous
/// incarnation still in flight cannot be adopted (see the `handle_start_view`
/// recovering-status guard).
#[derive(Debug, Clone, Copy, PartialEq, Eq, CheckedBitPattern, NoUninit)]
#[repr(C)]
pub struct RequestStartViewHeader {
    pub checksum: u128,
    pub checksum_body: u128,
    pub cluster: u128,
    pub size: u32,
    pub view: u32,
    pub release: u32,
    pub command: Command,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    pub group: u64,
    pub reserved: [u8; 104],
    /// The requester's per-boot incarnation, echoed back in the answering
    /// `StartView` so a reply from a previous incarnation is detectable.
    ///
    /// Carved from the tail of the former `reserved` region and placed LAST so it
    /// lands 16-aligned with no padding WITHOUT moving `group`. Zero is "no claim
    /// to echo"; see [`StartViewHeader::incarnation`] on why that is not
    /// mixed-version tolerance.
    pub incarnation: u128,
}
const _: () = {
    assert!(size_of::<RequestStartViewHeader>() == HEADER_SIZE);
    // group keeps its pre-incarnation offset.
    assert!(
        offset_of!(RequestStartViewHeader, group)
            == offset_of!(RequestStartViewHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
    // `incarnation` is last and 16-aligned, so the struct has no padding.
    assert!(offset_of!(RequestStartViewHeader, incarnation) + size_of::<u128>() == HEADER_SIZE);
    assert!(offset_of!(RequestStartViewHeader, incarnation) % 16 == 0);
};

impl ConsensusHeader for RequestStartViewHeader {
    const FRAME_SEALED: bool = true;

    const COMMAND: Command = Command::RequestStartView;

    fn checksum(&self) -> u128 {
        self.checksum
    }

    fn set_checksum(&mut self, checksum: u128) {
        self.checksum = checksum;
    }
    fn operation(&self) -> Operation {
        Operation::Reserved
    }
    fn command(&self) -> Command {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command::RequestStartView {
            return Err(ConsensusError::InvalidCommand {
                expected: Command::RequestStartView,
                found: self.command,
            });
        }
        if self.release != 0 {
            return Err(ConsensusError::InvalidField(
                "release must be 0".to_string(),
            ));
        }
        Ok(())
    }
}

// RequestPreparesHeader - ask a peer for a range of committed prepares

/// Recovering/holed replica -> a Normal peer: request a repair stream.
///
/// Sent to the primary first. Asks for the journaled prepares in
/// `[from_op, to_op]` for `group`. Header-only. The peer answers with
/// `RepairPrepare` frames in op order, terminated by `RepairDone` or
/// `RangeEvicted`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, CheckedBitPattern, NoUninit)]
#[repr(C)]
pub struct RequestPreparesHeader {
    pub checksum: u128,
    pub checksum_body: u128,
    pub cluster: u128,
    pub size: u32,
    pub view: u32,
    pub release: u32,
    pub command: Command,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    pub nonce: u128,
    pub from_op: u64,
    pub to_op: u64,
    pub group: u64,
    pub reserved: [u8; 88],
}
const _: () = {
    assert!(size_of::<RequestPreparesHeader>() == HEADER_SIZE);
    assert!(
        offset_of!(RequestPreparesHeader, nonce)
            == offset_of!(RequestPreparesHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
    assert!(offset_of!(RequestPreparesHeader, reserved) + size_of::<[u8; 88]>() == HEADER_SIZE);
};

impl ConsensusHeader for RequestPreparesHeader {
    const FRAME_SEALED: bool = true;

    const COMMAND: Command = Command::RequestPrepares;

    fn checksum(&self) -> u128 {
        self.checksum
    }

    fn set_checksum(&mut self, checksum: u128) {
        self.checksum = checksum;
    }
    fn operation(&self) -> Operation {
        Operation::Reserved
    }
    fn command(&self) -> Command {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command::RequestPrepares {
            return Err(ConsensusError::InvalidCommand {
                expected: Command::RequestPrepares,
                found: self.command,
            });
        }
        if self.from_op == 0 || self.from_op > self.to_op {
            return Err(ConsensusError::InvalidField(
                "repair range must be non-empty and 1-based".to_string(),
            ));
        }
        Ok(())
    }
}

// RepairRangeReplyHeader - RepairDone / RangeEvicted terminators

/// Serving peer -> requester: terminates a repair stream.
///
/// As `RepairDone`, `through_op` is the last op served. As `RangeEvicted`,
/// `retained_from` is the peer's oldest retained op -- everything older must
/// come from bulk state sync (phase 3). One layout serves both commands.
#[derive(Debug, Clone, Copy, PartialEq, Eq, CheckedBitPattern, NoUninit)]
#[repr(C)]
pub struct RepairRangeReplyHeader {
    pub checksum: u128,
    pub checksum_body: u128,
    pub cluster: u128,
    pub size: u32,
    pub view: u32,
    pub release: u32,
    pub command: Command,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    pub nonce: u128,
    /// `RepairDone`: last op served. `RangeEvicted`: oldest retained op.
    pub op: u64,
    pub group: u64,
    pub reserved: [u8; 96],
}
const _: () = {
    assert!(size_of::<RepairRangeReplyHeader>() == HEADER_SIZE);
    assert!(
        offset_of!(RepairRangeReplyHeader, nonce)
            == offset_of!(RepairRangeReplyHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
    assert!(offset_of!(RepairRangeReplyHeader, reserved) + size_of::<[u8; 96]>() == HEADER_SIZE);
};

impl ConsensusHeader for RepairRangeReplyHeader {
    const FRAME_SEALED: bool = true;

    const COMMAND: Command = Command::RepairDone;

    fn checksum(&self) -> u128 {
        self.checksum
    }

    fn set_checksum(&mut self, checksum: u128) {
        self.checksum = checksum;
    }
    // One layout, two commands: `RepairDone` terminates a stream,
    // `RangeEvicted` prefixes it. Without this widening, `try_into_typed`
    // rejects `RangeEvicted` frames before `validate` ever sees them.
    fn accepts(command: Command) -> bool {
        command == Command::RepairDone || command == Command::RangeEvicted
    }
    fn operation(&self) -> Operation {
        Operation::Reserved
    }
    fn command(&self) -> Command {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command::RepairDone && self.command != Command::RangeEvicted {
            return Err(ConsensusError::InvalidCommand {
                expected: Command::RepairDone,
                found: self.command,
            });
        }
        Ok(())
    }
}

// State transfer: descriptor + chunk pull frames.
//
// Plane-agnostic: the descriptor's BODY carries a state manifest (see the
// consensus crate's `state_manifest`) listing N artifacts, and the chunk
// frames address bytes by `(manifest index, offset)`. The metadata plane
// ships two artifacts (snapshot + client table); the partition plane ships
// its own set (segment logs, offsets) through the same frames.

// RequestStateTransferHeader - restarted replica -> current primary

/// Ask the current primary for a state-transfer target descriptor.
///
/// Answered with `StateTransferTarget`. Sent by a restarted replica after it
/// adopts a view from a live primary; `nonce` correlates the whole transfer
/// session.
#[derive(Debug, Clone, Copy, PartialEq, Eq, CheckedBitPattern, NoUninit)]
#[repr(C)]
pub struct RequestStateTransferHeader {
    pub checksum: u128,
    pub checksum_body: u128,
    pub cluster: u128,
    pub size: u32,
    pub view: u32,
    pub release: u32,
    pub command: Command,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    pub nonce: u128,
    pub group: u64,
    pub reserved: [u8; 104],
}
const _: () = {
    assert!(size_of::<RequestStateTransferHeader>() == HEADER_SIZE);
    assert!(
        offset_of!(RequestStateTransferHeader, nonce)
            == offset_of!(RequestStateTransferHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
    assert!(
        offset_of!(RequestStateTransferHeader, reserved) + size_of::<[u8; 104]>() == HEADER_SIZE
    );
};

impl ConsensusHeader for RequestStateTransferHeader {
    const FRAME_SEALED: bool = true;

    const COMMAND: Command = Command::RequestStateTransfer;

    fn checksum(&self) -> u128 {
        self.checksum
    }

    fn set_checksum(&mut self, checksum: u128) {
        self.checksum = checksum;
    }
    fn operation(&self) -> Operation {
        Operation::Reserved
    }
    fn command(&self) -> Command {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    #[allow(clippy::cast_possible_truncation)]
    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command::RequestStateTransfer {
            return Err(ConsensusError::InvalidCommand {
                expected: Command::RequestStateTransfer,
                found: self.command,
            });
        }
        // Header-only frame, so the size is fully determined. `EvictionHeader`
        // pins the same way; the generic `Message::try_from` bound makes this
        // safe either way, but a validate that checks what it can keeps the
        // surface uniform across frames.
        if self.size as usize != HEADER_SIZE {
            return Err(ConsensusError::InvalidSize {
                expected: HEADER_SIZE as u32,
                found: self.size,
            });
        }
        Ok(())
    }
}

// StateTransferTargetHeader - serving primary -> requester

/// The transfer target descriptor.
///
/// The artifact list rides the BODY as an encoded state manifest (`size`
/// spans header + manifest); the header carries only the session nonce and
/// the serving peer's applied frontier. `available == 0` means the serving
/// peer cannot serve right now (not a caught-up primary, or it has never
/// checkpointed, so the requester's journal repair can cover the whole gap)
/// and the frame is header-only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, CheckedBitPattern, NoUninit)]
#[repr(C)]
pub struct StateTransferTargetHeader {
    pub checksum: u128,
    pub checksum_body: u128,
    pub cluster: u128,
    pub size: u32,
    pub view: u32,
    pub release: u32,
    pub command: Command,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    pub nonce: u128,
    /// Serving primary's applied frontier (`commit_min`) when the descriptor
    /// was built. The receiver's tail repair targets past this.
    pub commit_op: u64,
    pub group: u64,
    pub available: u8,
    /// Set on an `available == 0` refusal that means "not right now" rather than
    /// "this node is broken".
    ///
    /// PARTITION arm only: it is the only side with a consecutive-failure count
    /// to charge. The requester then re-arms on a flat interval instead of
    /// charging that count, whose exponential backoff climbs to 1024x the retry
    /// interval and is reset only by a completed install. A serving primary
    /// momentarily behind its own frontier is the common case under produce
    /// load.
    ///
    /// This and `commit_max` below claim the HEAD of what used to be the
    /// reserved tail, so every pre-existing field keeps its published offset.
    /// Layout compatibility only: the size assert cannot catch an equal-size
    /// reshuffle, so a mid-struct insertion would silently move every field
    /// after it. It says nothing about the semantics of these two -- an older
    /// peer presents zeros here and serves no partition transfers at all.
    pub unavailable_transient: u8,
    /// Explicit padding so `commit_max` sits 8-aligned without the implicit
    /// padding `NoUninit` forbids.
    pub reserved_alignment: [u8; 6],
    /// Serving replica's `commit_max` when the descriptor was built.
    ///
    /// Read by the PARTITION receiver only; the metadata arm branches on
    /// `available` and falls back to journal repair without a refusal.
    ///
    /// A partition receiver refuses an offer from a replica that knows LESS
    /// than it does:
    /// without this the descriptor carried no proof of the sender's own
    /// progress, and a phantom view-0 primary (a group whose directory vanished
    /// boots `init()` rather than `init_as_backup()`, comes up Normal at view 0,
    /// and an empty log is trivially caught up) could hand a data-holding
    /// rejoiner an empty offer that unlinks its chain.
    pub commit_max: u64,
    pub reserved: [u8; 80],
}
const _: () = {
    assert!(size_of::<StateTransferTargetHeader>() == HEADER_SIZE);
    assert!(
        offset_of!(StateTransferTargetHeader, nonce)
            == offset_of!(StateTransferTargetHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
    // The pre-existing published offsets. New fields grow into the reserved
    // tail only; a change that moves one of these is a wire break.
    assert!(offset_of!(StateTransferTargetHeader, commit_op) == 144);
    assert!(offset_of!(StateTransferTargetHeader, group) == 152);
    assert!(offset_of!(StateTransferTargetHeader, available) == 160);
    assert!(offset_of!(StateTransferTargetHeader, unavailable_transient) == 161);
    assert!(offset_of!(StateTransferTargetHeader, commit_max) == 168);
    assert!(offset_of!(StateTransferTargetHeader, reserved) + size_of::<[u8; 80]>() == HEADER_SIZE);
};

impl ConsensusHeader for StateTransferTargetHeader {
    const FRAME_SEALED: bool = true;

    const COMMAND: Command = Command::StateTransferTarget;

    fn checksum(&self) -> u128 {
        self.checksum
    }

    fn set_checksum(&mut self, checksum: u128) {
        self.checksum = checksum;
    }
    fn operation(&self) -> Operation {
        Operation::Reserved
    }
    fn command(&self) -> Command {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command::StateTransferTarget {
            return Err(ConsensusError::InvalidCommand {
                expected: Command::StateTransferTarget,
                found: self.command,
            });
        }
        if self.available > 1 {
            return Err(ConsensusError::InvalidField(
                "available must be 0 or 1".to_string(),
            ));
        }
        if self.unavailable_transient > 1 {
            return Err(ConsensusError::InvalidField(
                "unavailable_transient must be 0 or 1".to_string(),
            ));
        }
        // The flag qualifies a refusal, so it is meaningless on an offer. Inert
        // today (the receiver reads it only inside the `available == 0` arm),
        // rejected anyway because a self-contradictory descriptor says the
        // sender is not the build this field was designed for.
        if self.available == 1 && self.unavailable_transient == 1 {
            return Err(ConsensusError::InvalidField(
                "unavailable_transient must be 0 on an available offer".to_string(),
            ));
        }
        // Unavailable is a bare refusal; a manifest body on it would be
        // ambiguous (which offer would the chunks belong to?). An
        // `available == 1` body is left unbounded here on purpose: it carries
        // the state manifest, whose entry count and per-artifact/total lengths
        // are bounded where it is decoded (`STATE_MANIFEST_ENTRIES_MAX`, plus
        // the receiver's artifact caps), and the generic `Message::try_from`
        // bound already keeps `size` inside the frame.
        if self.available == 0 && self.size as usize != HEADER_SIZE {
            return Err(ConsensusError::InvalidField(
                "unavailable descriptor must be header-only".to_string(),
            ));
        }
        Ok(())
    }
}

// RequestStateChunkHeader - requester -> serving primary

/// Pull one bounded chunk of an artifact.
///
/// Lockstep per artifact: the requester keeps at most one chunk in flight,
/// so the bounded per-peer bus queue can never drop a burst tail.
#[derive(Debug, Clone, Copy, PartialEq, Eq, CheckedBitPattern, NoUninit)]
#[repr(C)]
pub struct RequestStateChunkHeader {
    pub checksum: u128,
    pub checksum_body: u128,
    pub cluster: u128,
    pub size: u32,
    pub view: u32,
    pub release: u32,
    pub command: Command,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    pub nonce: u128,
    pub offset: u64,
    pub group: u64,
    pub len: u32,
    /// Index into the offered state manifest. Range-checked by the serving
    /// handler against the cached offer (the header cannot know the count).
    pub artifact: u32,
    pub reserved: [u8; 88],
}
const _: () = {
    assert!(size_of::<RequestStateChunkHeader>() == HEADER_SIZE);
    assert!(
        offset_of!(RequestStateChunkHeader, nonce)
            == offset_of!(RequestStateChunkHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
    assert!(offset_of!(RequestStateChunkHeader, reserved) + size_of::<[u8; 88]>() == HEADER_SIZE);
};

impl ConsensusHeader for RequestStateChunkHeader {
    const FRAME_SEALED: bool = true;

    const COMMAND: Command = Command::RequestStateChunk;

    fn checksum(&self) -> u128 {
        self.checksum
    }

    fn set_checksum(&mut self, checksum: u128) {
        self.checksum = checksum;
    }
    fn operation(&self) -> Operation {
        Operation::Reserved
    }
    fn command(&self) -> Command {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    #[allow(clippy::cast_possible_truncation)]
    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command::RequestStateChunk {
            return Err(ConsensusError::InvalidCommand {
                expected: Command::RequestStateChunk,
                found: self.command,
            });
        }
        if self.len == 0 {
            return Err(ConsensusError::InvalidField(
                "chunk len must be non-zero".to_string(),
            ));
        }
        // Header-only frame; the requested `len` describes the REPLY, which the
        // serving side clamps against its own chunk size and the bus ceiling.
        if self.size as usize != HEADER_SIZE {
            return Err(ConsensusError::InvalidSize {
                expected: HEADER_SIZE as u32,
                found: self.size,
            });
        }
        Ok(())
    }
}

// StateChunkHeader - serving primary -> requester (carries payload)

/// One chunk of artifact bytes at `offset`.
///
/// The payload rides the body (`size` spans header + payload). Transit
/// integrity is checked at the artifact level (descriptor checksums), not
/// per chunk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, CheckedBitPattern, NoUninit)]
#[repr(C)]
pub struct StateChunkHeader {
    pub checksum: u128,
    pub checksum_body: u128,
    pub cluster: u128,
    pub size: u32,
    pub view: u32,
    pub release: u32,
    pub command: Command,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    pub nonce: u128,
    pub offset: u64,
    pub group: u64,
    /// Index into the offered state manifest. Range-checked by the receiving
    /// handler against its accepted manifest.
    pub artifact: u32,
    pub reserved: [u8; 92],
}
const _: () = {
    assert!(size_of::<StateChunkHeader>() == HEADER_SIZE);
    assert!(
        offset_of!(StateChunkHeader, nonce)
            == offset_of!(StateChunkHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
    assert!(offset_of!(StateChunkHeader, reserved) + size_of::<[u8; 92]>() == HEADER_SIZE);
};

impl ConsensusHeader for StateChunkHeader {
    const FRAME_SEALED: bool = true;

    const COMMAND: Command = Command::StateChunk;

    fn checksum(&self) -> u128 {
        self.checksum
    }

    fn set_checksum(&mut self, checksum: u128) {
        self.checksum = checksum;
    }
    fn operation(&self) -> Operation {
        Operation::Reserved
    }
    fn command(&self) -> Command {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command::StateChunk {
            return Err(ConsensusError::InvalidCommand {
                expected: Command::StateChunk,
                found: self.command,
            });
        }
        if (self.size as usize) < HEADER_SIZE {
            return Err(ConsensusError::InvalidField(
                "state chunk size below header size".to_string(),
            ));
        }
        Ok(())
    }
}

// ForwardRegisterHeader - backup shard 0 -> primary shard 0

/// A backup relays a login it has already authenticated.
///
/// Credential verification runs on the node the client dialed, against the
/// replicated users table, so neither the client's frame nor its credentials
/// travel: this header carries the VERIFIED identity and nothing else. The
/// backup keeps the session bind, the reply build, and the connection.
///
/// The trust boundary is the replica interconnect's network placement: by
/// default the replica port trusts any peer that reaches it (the PSK
/// handshake and TLS ship disabled and are what upgrade the boundary), and
/// the seal is an unkeyed integrity check, not a MAC. Trusting `user_id`
/// here adds no capability the port did not already expose, since that same
/// peer could inject a `Request` + `Register` directly. Clients cannot
/// reach this command, because every client frame is typed through
/// [`RequestHeader`], whose `validate` rejects any command but
/// [`Command::Request`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, CheckedBitPattern, NoUninit)]
#[repr(C)]
pub struct ForwardRegisterHeader {
    pub checksum: u128,
    pub checksum_body: u128,
    pub cluster: u128,
    pub size: u32,
    pub view: u32,
    pub release: u32,
    pub command: Command,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    /// The login frame's consensus client id, proposed verbatim by the primary.
    pub client: u128,
    /// Correlation minted by the backup, echoed verbatim in the result. Unique
    /// per in-flight forward on the originating node, across its restarts as
    /// well: an answer that outlives the login it was minted for must not match
    /// the one holding that nonce next.
    pub nonce: u128,
    /// The acting user the backup authenticated. The trust payload: the primary
    /// proposes the register under this id without re-verifying credentials.
    pub user_id: u32,
    pub reserved: [u8; 92],
}
const _: () = {
    assert!(size_of::<ForwardRegisterHeader>() == HEADER_SIZE);
    assert!(
        offset_of!(ForwardRegisterHeader, client)
            == offset_of!(ForwardRegisterHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
    assert!(offset_of!(ForwardRegisterHeader, nonce) == 144);
    assert!(offset_of!(ForwardRegisterHeader, user_id) == 160);
    assert!(offset_of!(ForwardRegisterHeader, reserved) + size_of::<[u8; 92]>() == HEADER_SIZE);
};

impl ConsensusHeader for ForwardRegisterHeader {
    // The seal covers `user_id`, which is the whole reason this frame is
    // believed: a flipped bit there would commit a register under another user.
    const FRAME_SEALED: bool = true;

    const COMMAND: Command = Command::ForwardRegister;

    fn checksum(&self) -> u128 {
        self.checksum
    }

    fn set_checksum(&mut self, checksum: u128) {
        self.checksum = checksum;
    }
    fn operation(&self) -> Operation {
        Operation::Reserved
    }
    fn command(&self) -> Command {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command::ForwardRegister {
            return Err(ConsensusError::InvalidCommand {
                expected: Command::ForwardRegister,
                found: self.command,
            });
        }
        validate_forward_register_frame(self.size, self.client, self.nonce, &self.reserved)
    }
}

// ForwardRegisterResultHeader - primary shard 0 -> backup shard 0

/// The primary's verdict on a [`ForwardRegisterHeader`].
///
/// Header-only: the backup owns the client connection and builds the wire
/// reply itself, so only the committed bind (or the refusal) crosses the
/// interconnect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, CheckedBitPattern, NoUninit)]
#[repr(C)]
pub struct ForwardRegisterResultHeader {
    pub checksum: u128,
    pub checksum_body: u128,
    pub cluster: u128,
    pub size: u32,
    pub view: u32,
    pub release: u32,
    pub command: Command,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    /// Echo of [`ForwardRegisterHeader::nonce`]; with `client`, the backup's
    /// routing key.
    pub nonce: u128,
    /// Echo of [`ForwardRegisterHeader::client`]. Half of the routing key, not a
    /// diagnostic: the backup drops an answer whose client disagrees with the
    /// login it parked under `nonce`.
    pub client: u128,
    /// The committed register's op number, which fences the session. Zero
    /// unless `outcome` is [`ForwardRegisterOutcome::Ok`].
    pub epoch: u64,
    /// The client-table entry's highest committed request number, for a caller
    /// that must resume numbering. Zero unless `outcome` is
    /// [`ForwardRegisterOutcome::Ok`].
    pub watermark: u64,
    pub reserved: [u8; 79],
    pub outcome: ForwardRegisterOutcome,
}
const _: () = {
    assert!(size_of::<ForwardRegisterResultHeader>() == HEADER_SIZE);
    assert!(
        offset_of!(ForwardRegisterResultHeader, nonce)
            == offset_of!(ForwardRegisterResultHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
    assert!(offset_of!(ForwardRegisterResultHeader, client) == 144);
    assert!(offset_of!(ForwardRegisterResultHeader, epoch) == 160);
    assert!(offset_of!(ForwardRegisterResultHeader, watermark) == 168);
    assert!(
        offset_of!(ForwardRegisterResultHeader, outcome) + size_of::<ForwardRegisterOutcome>()
            == HEADER_SIZE
    );
};

/// Wire verdict on a forwarded register.
///
/// Mirrors the server-side submit error one-for-one so the backup can surface
/// the primary's exact answer: every variant but
/// [`Self::ClientIdOwnedByAnotherUser`] is transient and replayable.
///
/// **Wire-version pinned**, like [`EvictionReason`]: reordering or reusing a
/// discriminant silently reinterprets a live cluster's frames.
#[derive(Debug, Clone, Copy, PartialEq, Eq, NoUninit, CheckedBitPattern)]
#[repr(u8)]
pub enum ForwardRegisterOutcome {
    /// Committed; `epoch` and `watermark` carry the bind.
    Ok = 0,
    NotPrimary = 1,
    /// Reserved: the register submit parks instead of bouncing when the
    /// primary is not caught up, so nothing produces this today. Wire-pinned,
    /// so it must keep its discriminant either way.
    NotCaughtUp = 2,
    PipelineFull = 3,
    InProgress = 4,
    Canceled = 5,
    /// Terminal: the presented client id belongs to another user.
    ClientIdOwnedByAnotherUser = 6,
}

impl ConsensusHeader for ForwardRegisterResultHeader {
    const FRAME_SEALED: bool = true;

    const COMMAND: Command = Command::ForwardRegisterResult;

    fn checksum(&self) -> u128 {
        self.checksum
    }

    fn set_checksum(&mut self, checksum: u128) {
        self.checksum = checksum;
    }
    fn operation(&self) -> Operation {
        Operation::Reserved
    }
    fn command(&self) -> Command {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command::ForwardRegisterResult {
            return Err(ConsensusError::InvalidCommand {
                expected: Command::ForwardRegisterResult,
                found: self.command,
            });
        }
        validate_forward_register_frame(self.size, self.client, self.nonce, &self.reserved)?;
        if self.outcome != ForwardRegisterOutcome::Ok && (self.epoch != 0 || self.watermark != 0) {
            return Err(ConsensusError::InvalidField(
                "forward register result bind must be zero on failure".to_string(),
            ));
        }
        Ok(())
    }
}

/// Shared field rules of the two register-forwarding headers.
///
/// Reserved bytes are strict-zero rather than ignored: this pair only ever
/// travels between replicas of one release (see the wire-compatibility note at
/// the top of this module), so there is no forward-compatibility to preserve
/// and a nonzero byte can only mean a builder bug or a mangled frame.
fn validate_forward_register_frame(
    size: u32,
    client: u128,
    nonce: u128,
    reserved: &[u8],
) -> Result<(), ConsensusError> {
    validate_forward_frame(size, client, nonce, reserved)
}

// ForwardLogoutHeader - backup shard 0 -> primary shard 0

/// A backup asks the metadata primary to tear down a session it owns.
///
/// The client-facing Logout request stays on the backup. Only the replicated
/// session identity and request number cross the replica interconnect, and the
/// primary's epoch guard makes a delayed forward harmless after a rebind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, CheckedBitPattern, NoUninit)]
#[repr(C)]
pub struct ForwardLogoutHeader {
    pub checksum: u128,
    pub checksum_body: u128,
    pub cluster: u128,
    pub size: u32,
    pub view: u32,
    pub release: u32,
    pub command: Command,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    /// The consensus client id whose table entry should be removed.
    pub client: u128,
    /// Correlation minted by the backup and echoed in the result.
    pub nonce: u128,
    /// The exact register epoch being logged out.
    pub session: u64,
    /// The client's request number, or the server's synthetic disconnect id.
    pub request: u64,
    pub reserved: [u8; 80],
}
const _: () = {
    assert!(size_of::<ForwardLogoutHeader>() == HEADER_SIZE);
    assert!(
        offset_of!(ForwardLogoutHeader, client)
            == offset_of!(ForwardLogoutHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
    assert!(offset_of!(ForwardLogoutHeader, nonce) == 144);
    assert!(offset_of!(ForwardLogoutHeader, session) == 160);
    assert!(offset_of!(ForwardLogoutHeader, request) == 168);
    assert!(offset_of!(ForwardLogoutHeader, reserved) + size_of::<[u8; 80]>() == HEADER_SIZE);
};

impl ConsensusHeader for ForwardLogoutHeader {
    const FRAME_SEALED: bool = true;
    const COMMAND: Command = Command::ForwardLogout;

    fn checksum(&self) -> u128 {
        self.checksum
    }

    fn set_checksum(&mut self, checksum: u128) {
        self.checksum = checksum;
    }

    fn operation(&self) -> Operation {
        Operation::Reserved
    }

    fn command(&self) -> Command {
        self.command
    }

    fn size(&self) -> u32 {
        self.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command::ForwardLogout {
            return Err(ConsensusError::InvalidCommand {
                expected: Command::ForwardLogout,
                found: self.command,
            });
        }
        validate_forward_logout_frame(
            self.size,
            self.client,
            self.nonce,
            self.session,
            self.request,
            &self.reserved,
        )
    }
}

// ForwardLogoutResultHeader - primary shard 0 -> backup shard 0

/// The metadata primary's verdict on a [`ForwardLogoutHeader`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, CheckedBitPattern, NoUninit)]
#[repr(C)]
pub struct ForwardLogoutResultHeader {
    pub checksum: u128,
    pub checksum_body: u128,
    pub cluster: u128,
    pub size: u32,
    pub view: u32,
    pub release: u32,
    pub command: Command,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    /// Echo of [`ForwardLogoutHeader::nonce`].
    pub nonce: u128,
    /// Echo of [`ForwardLogoutHeader::client`].
    pub client: u128,
    /// Committed Logout op, or the current commit for an idempotent no-op.
    /// Zero unless `outcome` is [`ForwardLogoutOutcome::Ok`].
    pub commit: u64,
    pub reserved: [u8; 87],
    pub outcome: ForwardLogoutOutcome,
}
const _: () = {
    assert!(size_of::<ForwardLogoutResultHeader>() == HEADER_SIZE);
    assert!(
        offset_of!(ForwardLogoutResultHeader, nonce)
            == offset_of!(ForwardLogoutResultHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
    assert!(offset_of!(ForwardLogoutResultHeader, client) == 144);
    assert!(offset_of!(ForwardLogoutResultHeader, commit) == 160);
    assert!(
        offset_of!(ForwardLogoutResultHeader, outcome) + size_of::<ForwardLogoutOutcome>()
            == HEADER_SIZE
    );
};

/// Wire verdict on a forwarded logout.
#[derive(Debug, Clone, Copy, PartialEq, Eq, NoUninit, CheckedBitPattern)]
#[repr(u8)]
pub enum ForwardLogoutOutcome {
    Ok = 0,
    NotPrimary = 1,
    PipelineFull = 2,
    InProgress = 3,
    Canceled = 4,
}

impl ConsensusHeader for ForwardLogoutResultHeader {
    const FRAME_SEALED: bool = true;
    const COMMAND: Command = Command::ForwardLogoutResult;

    fn checksum(&self) -> u128 {
        self.checksum
    }

    fn set_checksum(&mut self, checksum: u128) {
        self.checksum = checksum;
    }

    fn operation(&self) -> Operation {
        Operation::Reserved
    }

    fn command(&self) -> Command {
        self.command
    }

    fn size(&self) -> u32 {
        self.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command::ForwardLogoutResult {
            return Err(ConsensusError::InvalidCommand {
                expected: Command::ForwardLogoutResult,
                found: self.command,
            });
        }
        validate_forward_frame(self.size, self.client, self.nonce, &self.reserved)?;
        if self.outcome != ForwardLogoutOutcome::Ok && self.commit != 0 {
            return Err(ConsensusError::InvalidField(
                "forward logout result commit must be zero on failure".to_string(),
            ));
        }
        Ok(())
    }
}

fn validate_forward_logout_frame(
    size: u32,
    client: u128,
    nonce: u128,
    session: u64,
    request: u64,
    reserved: &[u8],
) -> Result<(), ConsensusError> {
    validate_forward_frame(size, client, nonce, reserved)?;
    if session == 0 {
        return Err(ConsensusError::InvalidField(
            "forward logout session must be non-zero".to_string(),
        ));
    }
    if request == 0 {
        return Err(ConsensusError::InvalidField(
            "forward logout request must be non-zero".to_string(),
        ));
    }
    Ok(())
}

#[allow(clippy::cast_possible_truncation)]
fn validate_forward_frame(
    size: u32,
    client: u128,
    nonce: u128,
    reserved: &[u8],
) -> Result<(), ConsensusError> {
    if size as usize != HEADER_SIZE {
        return Err(ConsensusError::InvalidSize {
            expected: HEADER_SIZE as u32,
            found: size,
        });
    }
    if client == 0 {
        return Err(ConsensusError::InvalidField(
            "forward client must be non-zero".to_string(),
        ));
    }
    if nonce == 0 {
        return Err(ConsensusError::InvalidField(
            "forward nonce must be non-zero".to_string(),
        ));
    }
    if reserved.iter().any(|&byte| byte != 0) {
        return Err(ConsensusError::InvalidField(
            "forward reserved bytes must be zero".to_string(),
        ));
    }
    Ok(())
}

// Tests

#[cfg(test)]
mod tests {
    use super::{
        Command, CommitHeader, ConsensusError, ConsensusHeader, DoViewChangeHeader, EvictionHeader,
        EvictionReason, ForwardLogoutHeader, ForwardLogoutOutcome, ForwardLogoutResultHeader,
        ForwardRegisterHeader, ForwardRegisterOutcome, ForwardRegisterResultHeader, GenericHeader,
        HEADER_SIZE, Operation, PrepareHeader, PrepareOkHeader, RepairPrepareHeader,
        RepairRangeReplyHeader, ReplyHeader, RequestHeader, RequestPreparesHeader,
        RequestStartViewHeader, RequestStateChunkHeader, RequestStateTransferHeader,
        RoutedRequestHeader, StartViewChangeHeader, StartViewHeader, StateChunkHeader,
        StateTransferTargetHeader,
    };
    use aligned_vec::{AVec, ConstAlign};

    // bytemuck requires 16-byte alignment (see `ConsensusHeader` trait doc).
    // `BytesMut::zeroed` works on glibc by accident, fails under Miri.
    fn aligned_zeroed(size: usize) -> AVec<u8, ConstAlign<16>> {
        let mut v: AVec<u8, ConstAlign<16>> = AVec::new(16);
        v.resize(size, 0);
        v
    }

    /// A header-sized frame that satisfies `bytemuck`'s 16-byte alignment.
    #[repr(C, align(16))]
    struct AlignedFrame([u8; HEADER_SIZE]);

    /// A minimal well-formed header of type `H`: own command and size, everything
    /// else zero. Enough for the seal, which reads bytes rather than fields.
    fn control_header<H: ConsensusHeader>() -> H {
        const COMMAND_OFF: usize = std::mem::offset_of!(GenericHeader, command);
        const SIZE_OFF: usize = std::mem::offset_of!(GenericHeader, size);

        let frame_len = u32::try_from(HEADER_SIZE).expect("HEADER_SIZE fits u32");
        let mut frame = AlignedFrame([0u8; HEADER_SIZE]);
        frame.0[COMMAND_OFF] = H::COMMAND as u8;
        frame.0[SIZE_OFF..SIZE_OFF + 4].copy_from_slice(&frame_len.to_le_bytes());
        *bytemuck::checked::try_from_bytes::<H>(&frame.0).expect("a zeroed frame is a valid header")
    }

    /// Seal a header, flip one bit at `offset`, and report `verify_frame`'s verdict.
    fn tamper<H: ConsensusHeader>(mut header: H, offset: usize) -> Result<(), ConsensusError> {
        header.seal();
        let mut frame = AlignedFrame([0u8; HEADER_SIZE]);
        frame.0.copy_from_slice(bytemuck::bytes_of(&header));
        frame.0[offset] ^= 0x01;
        let tampered = bytemuck::checked::try_from_bytes::<H>(&frame.0)
            .expect("a single flipped bit stays a valid bit pattern here");
        tampered.verify_frame()
    }

    #[test]
    fn given_a_sealed_control_header_when_verifying_should_accept() {
        macro_rules! seals {
            ($($header:ty),+ $(,)?) => {$({
                assert!(
                    <$header>::FRAME_SEALED,
                    "{} is a replica-to-replica control header and must seal",
                    stringify!($header),
                );
                let mut header = control_header::<$header>();
                header.seal();
                assert_eq!(
                    header.verify_frame(),
                    Ok(()),
                    "{} must accept its own seal",
                    stringify!($header),
                );
            })+};
        }
        seals!(
            PrepareOkHeader,
            CommitHeader,
            StartViewChangeHeader,
            DoViewChangeHeader,
            StartViewHeader,
            RequestStartViewHeader,
            RequestPreparesHeader,
            RepairRangeReplyHeader,
            RequestStateTransferHeader,
            StateTransferTargetHeader,
            RequestStateChunkHeader,
            StateChunkHeader,
            ForwardRegisterHeader,
            ForwardRegisterResultHeader,
            ForwardLogoutHeader,
            ForwardLogoutResultHeader,
        );
    }

    #[test]
    fn given_any_covered_byte_when_flipped_should_reject() {
        // Why this seal exists. A `DoViewChange` nack bitset is a new primary's
        // authority to truncate: two nacks on three replicas reach
        // `quorum_nack_prepare` and discard a committed, client-acked op. The bitsets
        // ride the header, and TCP's checksum will not reliably catch one bit.
        // Every byte past `checksum` is covered, `checksum_body` included.
        for offset in size_of::<u128>()..HEADER_SIZE {
            let header = control_header::<DoViewChangeHeader>();
            // Skip offsets where the flipped bit is an invalid bit pattern, which
            // `try_from_bytes` rejects one layer earlier.
            if offset == std::mem::offset_of!(DoViewChangeHeader, command) {
                continue;
            }
            assert!(
                matches!(
                    tamper(header, offset),
                    Err(ConsensusError::FrameChecksumMismatch { .. })
                ),
                "byte {offset} is inside the seal and must be covered"
            );
        }
    }

    #[test]
    fn given_an_unsealed_control_header_when_verifying_should_reject() {
        // No presence-keying: a zero checksum is a corrupt frame, not an old one.
        // Keying on "does this look sealed" leaves the layer bypassable by zeroing
        // the one field that decides whether anything is checked.
        let header = control_header::<DoViewChangeHeader>();
        assert_eq!(header.checksum, 0);
        assert!(matches!(
            header.verify_frame(),
            Err(ConsensusError::FrameChecksumMismatch { found: 0, .. })
        ));
    }

    #[test]
    fn given_an_identity_or_client_header_when_verifying_should_abstain() {
        // `PrepareHeader` spends `checksum` on `identity_checksum`, which excludes
        // `view` so a re-stamped prepare keeps one identity; the client-facing three
        // are sealed on neither side yet. All must parse unchanged.
        const {
            assert!(!PrepareHeader::FRAME_SEALED);
            assert!(!RepairPrepareHeader::FRAME_SEALED);
            assert!(!RequestHeader::FRAME_SEALED);
            assert!(!ReplyHeader::FRAME_SEALED);
            assert!(!EvictionHeader::FRAME_SEALED);
            assert!(!GenericHeader::FRAME_SEALED);
        }

        let prepare = PrepareHeader {
            command: Command::Prepare,
            checksum: 0xdead_beef,
            ..Default::default()
        };
        assert_eq!(prepare.verify_frame(), Ok(()));
    }

    #[test]
    fn all_headers_are_256_bytes() {
        assert_eq!(size_of::<GenericHeader>(), 256);
        assert_eq!(size_of::<RequestHeader>(), 256);
        assert_eq!(size_of::<ReplyHeader>(), 256);
        assert_eq!(size_of::<PrepareHeader>(), 256);
        assert_eq!(size_of::<PrepareOkHeader>(), 256);
        assert_eq!(size_of::<CommitHeader>(), 256);
        assert_eq!(size_of::<StartViewChangeHeader>(), 256);
        assert_eq!(size_of::<DoViewChangeHeader>(), 256);
        assert_eq!(size_of::<StartViewHeader>(), 256);
    }

    #[test]
    fn generic_header_zero_copy() {
        let buf = aligned_zeroed(256);
        let header: &GenericHeader = bytemuck::checked::try_from_bytes(&buf).unwrap();
        assert_eq!(header.command, Command::Reserved);
        assert_eq!(header.size, 0);
    }

    #[test]
    fn request_header_zero_copy() {
        let mut buf = aligned_zeroed(256);
        buf[60] = Command::Request as u8;
        // client offset = 60 + 1 (replica) + 66 (reserved_frame) = 128.
        // validate rejects client == 0.
        buf[128] = 1;
        // Register (session 0, request 0 are its required values); a zeroed
        // operation is `Reserved`, which validate rejects.
        buf[std::mem::offset_of!(RequestHeader, operation)] = Operation::Register as u8;
        let header: &RequestHeader = bytemuck::checked::try_from_bytes(&buf).unwrap();
        assert_eq!(header.command, Command::Request);
        assert!(header.validate().is_ok());
    }

    #[test]
    fn request_header_wrong_command_fails_validation() {
        let buf = aligned_zeroed(256);
        let header: &RequestHeader = bytemuck::checked::try_from_bytes(&buf).unwrap();
        assert!(header.validate().is_err());
    }

    // `Reserved` is the zero discriminant and never a real client op. Ingress
    // must reject it, since the operation gate that would otherwise catch it
    // runs after the dedup preflight.
    #[test]
    fn request_reserved_operation_rejected() {
        let header = RequestHeader {
            command: Command::Request,
            client: 1,
            operation: Operation::Reserved,
            session: 1,
            request: 1,
            ..RequestHeader::default()
        };
        assert!(header.validate().is_err());
    }

    #[test]
    fn request_register_nonzero_session_rejected() {
        let header = RequestHeader {
            command: Command::Request,
            operation: Operation::Register,
            session: 5,
            request: 0,
            ..RequestHeader::default()
        };
        assert!(header.validate().is_err());
    }

    #[test]
    fn request_register_nonzero_request_rejected() {
        let header = RequestHeader {
            command: Command::Request,
            operation: Operation::Register,
            session: 0,
            request: 1,
            ..RequestHeader::default()
        };
        assert!(header.validate().is_err());
    }

    #[test]
    fn request_non_register_valid() {
        let header = RequestHeader {
            command: Command::Request,
            operation: Operation::SendMessages,
            client: 0xCAFE,
            session: 10,
            request: 1,
            ..RequestHeader::default()
        };
        assert!(header.validate().is_ok());
    }

    #[test]
    fn request_non_register_zero_session_rejected() {
        let header = RequestHeader {
            command: Command::Request,
            operation: Operation::SendMessages,
            session: 0,
            request: 1,
            ..RequestHeader::default()
        };
        assert!(header.validate().is_err());
    }

    #[test]
    fn request_non_register_zero_request_rejected() {
        let header = RequestHeader {
            command: Command::Request,
            operation: Operation::SendMessages,
            session: 10,
            request: 0,
            ..RequestHeader::default()
        };
        assert!(header.validate().is_err());
    }

    #[test]
    fn reply_header_zero_copy() {
        let mut buf = aligned_zeroed(256);
        buf[60] = Command::Reply as u8;
        let header: &ReplyHeader = bytemuck::checked::try_from_bytes(&buf).unwrap();
        assert_eq!(header.command, Command::Reply);
        assert!(header.validate().is_ok());
    }

    // `status` is carved from the reserved tail; the SDK reply funnel peeks it
    // at this offset before any body decode, and four foreign SDKs hardcode
    // it, so a layout drift must trip here.
    #[test]
    fn reply_header_status_offset_and_size_pinned() {
        use std::mem::offset_of;
        assert_eq!(size_of::<ReplyHeader>(), HEADER_SIZE);
        assert_eq!(offset_of!(ReplyHeader, status), 216);
        assert_eq!(
            offset_of!(ReplyHeader, reserved) + size_of::<[u8; 36]>(),
            HEADER_SIZE
        );
    }

    // `group` claims the TAIL of the client header's reserved area; the
    // leading 52 reserved bytes carry data (the non-replicated op code lives
    // in `reserved[0..4]`), so a reshuffle of the carve must trip here rather
    // than silently move the code range.
    #[test]
    fn routed_request_group_claims_reserved_tail() {
        use std::mem::offset_of;
        assert_eq!(
            offset_of!(RoutedRequestHeader, reserved),
            offset_of!(RequestHeader, reserved)
        );
        assert_eq!(
            offset_of!(RoutedRequestHeader, group),
            offset_of!(RequestHeader, reserved) + 52
        );
        assert_eq!(offset_of!(RoutedRequestHeader, group), 248);
        assert_eq!(
            offset_of!(RoutedRequestHeader, group) + size_of::<u64>(),
            HEADER_SIZE
        );
    }

    // The routed shape is decoded straight off the peer wire, so it must
    // enforce the same field rules as the client boundary; a command-only
    // validate lets a forged `client = 0` frame reach the client table's
    // hard assert.
    #[test]
    fn routed_request_zero_client_rejected() {
        let header = RoutedRequestHeader {
            command: Command::Request,
            operation: Operation::SendMessages,
            session: 10,
            request: 1,
            ..RoutedRequestHeader::default()
        };
        assert!(header.validate().is_err());
    }

    #[test]
    fn routed_request_reserved_operation_rejected() {
        let header = RoutedRequestHeader {
            command: Command::Request,
            client: 0xCAFE,
            session: 10,
            request: 1,
            ..RoutedRequestHeader::default()
        };
        assert!(header.validate().is_err());
    }

    #[test]
    fn routed_request_default_fails_validate() {
        assert!(RoutedRequestHeader::default().validate().is_err());
    }

    #[test]
    fn routed_request_valid_fields_accepted() {
        let header = RoutedRequestHeader {
            command: Command::Request,
            operation: Operation::SendMessages,
            client: 0xCAFE,
            session: 10,
            request: 1,
            group: 7,
            ..RoutedRequestHeader::default()
        };
        assert!(header.validate().is_ok());
    }

    // A nonzero status rides the reserved region, which reply `validate` does
    // not inspect: a status-bearing reply stays valid so the SDK can peek it.
    #[test]
    fn reply_header_nonzero_status_still_validates() {
        let header = ReplyHeader {
            command: Command::Reply,
            status: 41,
            ..ReplyHeader::default()
        };
        assert!(header.validate().is_ok());
    }

    // Wire-discriminant pin: any change breaks SDK decoders.
    #[test]
    fn eviction_reason_discriminants_pinned() {
        assert_eq!(EvictionReason::Reserved as u8, 0);
        assert_eq!(EvictionReason::NoSession as u8, 1);
        assert_eq!(EvictionReason::ClientReleaseTooLow as u8, 2);
        assert_eq!(EvictionReason::ClientReleaseTooHigh as u8, 3);
        assert_eq!(EvictionReason::InvalidRequestOperation as u8, 4);
        assert_eq!(EvictionReason::InvalidRequestBody as u8, 5);
        assert_eq!(EvictionReason::InvalidRequestBodySize as u8, 6);
        assert_eq!(EvictionReason::SessionTooLow as u8, 7);
        assert_eq!(EvictionReason::SessionReleaseMismatch as u8, 8);
        assert_eq!(EvictionReason::InvalidCredentials as u8, 9);
        assert_eq!(EvictionReason::InvalidToken as u8, 10);
        assert_eq!(EvictionReason::UserInactive as u8, 11);
        assert_eq!(EvictionReason::SessionError as u8, 12);
        assert_eq!(EvictionReason::StaleClient as u8, 13);
        assert_eq!(EvictionReason::IncompatibleProtocol as u8, 14);
        assert_eq!(EvictionReason::MalformedLogin as u8, 15);
    }

    #[test]
    fn eviction_incompatible_protocol_accepts_valid_window() {
        let header = EvictionHeader::incompatible_protocol(0, 0, 0, 0xCAFE, 2, 1);
        assert!(header.validate().is_ok());
        assert_eq!(header.reason, EvictionReason::IncompatibleProtocol);
        assert_eq!(header.server_protocol_version, 2);
        assert_eq!(header.server_protocol_version_min, 1);
    }

    #[test]
    fn eviction_incompatible_protocol_rejects_inverted_window() {
        let header = EvictionHeader::incompatible_protocol(0, 0, 0, 0xCAFE, 1, 2);
        assert!(header.validate().is_err());
    }

    #[test]
    fn eviction_incompatible_protocol_rejects_zero_min() {
        let header = EvictionHeader::incompatible_protocol(0, 0, 0, 0xCAFE, 1, 0);
        assert!(header.validate().is_err());
    }

    // Protocol window is IncompatibleProtocol-only; nonzero elsewhere is
    // smuggling, same rule as the reserved-byte guards.
    #[test]
    fn eviction_validate_rejects_window_on_other_reason() {
        let mut header = EvictionHeader::new(0, 0, 0, 0xCAFE, EvictionReason::NoSession);
        header.server_protocol_version = 1;
        assert!(header.validate().is_err());
    }

    #[test]
    fn eviction_validate_rejects_zero_client() {
        let header = EvictionHeader::new(0, 0, 0, 0, EvictionReason::NoSession);
        assert!(header.validate().is_err());
    }

    #[test]
    fn eviction_validate_rejects_reserved_reason() {
        let header = EvictionHeader::new(0, 0, 0, 1, EvictionReason::Reserved);
        assert!(header.validate().is_err());
    }

    // Reserved bytes guard: blocks forward-incompat smuggling.
    #[test]
    fn eviction_validate_rejects_nonzero_reserved() {
        let mut header = EvictionHeader::new(0, 0, 0, 1, EvictionReason::NoSession);
        header.reserved[0] = 1;
        assert!(header.validate().is_err());
    }

    #[test]
    fn eviction_validate_accepts_well_formed_frame() {
        let header = EvictionHeader::new(0, 0, 0, 0xCAFE, EvictionReason::NoSession);
        assert!(header.validate().is_ok());
    }

    #[test]
    fn eviction_header_is_256_bytes() {
        assert_eq!(size_of::<EvictionHeader>(), 256);
    }

    /// A sealed, well-formed `ForwardRegister` ready to be mutated per test.
    fn forward_register() -> ForwardRegisterHeader {
        let mut header = ForwardRegisterHeader {
            checksum: 0,
            checksum_body: 0,
            cluster: 7,
            size: u32::try_from(HEADER_SIZE).expect("HEADER_SIZE fits u32"),
            view: 3,
            release: 0,
            command: Command::ForwardRegister,
            replica: 2,
            reserved_frame: [0; 66],
            client: 0xCAFE,
            nonce: 0xF00D,
            user_id: 41,
            reserved: [0; 92],
        };
        header.seal();
        header
    }

    /// A sealed, well-formed `ForwardRegisterResult` ready to be mutated.
    fn forward_register_result(outcome: ForwardRegisterOutcome) -> ForwardRegisterResultHeader {
        let mut header = ForwardRegisterResultHeader {
            checksum: 0,
            checksum_body: 0,
            cluster: 7,
            size: u32::try_from(HEADER_SIZE).expect("HEADER_SIZE fits u32"),
            view: 3,
            release: 0,
            command: Command::ForwardRegisterResult,
            replica: 0,
            reserved_frame: [0; 66],
            nonce: 0xF00D,
            client: 0xCAFE,
            epoch: u64::from(outcome == ForwardRegisterOutcome::Ok) * 91,
            watermark: u64::from(outcome == ForwardRegisterOutcome::Ok) * 12,
            reserved: [0; 79],
            outcome,
        };
        header.seal();
        header
    }

    #[test]
    fn forward_register_round_trips_through_generic_bytes() {
        for (command, bytes) in [
            (
                Command::ForwardRegister,
                bytemuck::bytes_of(&forward_register()).to_vec(),
            ),
            (
                Command::ForwardRegisterResult,
                bytemuck::bytes_of(&forward_register_result(ForwardRegisterOutcome::Ok)).to_vec(),
            ),
        ] {
            // 16-byte alignment: `Vec<u8>` requests align=1 and fails Miri.
            let mut buf = aligned_zeroed(HEADER_SIZE);
            buf.copy_from_slice(&bytes);
            let generic = bytemuck::checked::try_from_bytes::<GenericHeader>(&buf)
                .expect("a forward-register frame is a valid generic header");
            assert_eq!(generic.command, command);
            assert_eq!(generic.size as usize, HEADER_SIZE);
        }

        let buf = {
            let mut buf = aligned_zeroed(HEADER_SIZE);
            buf.copy_from_slice(bytemuck::bytes_of(&forward_register()));
            buf
        };
        let typed = bytemuck::checked::try_from_bytes::<ForwardRegisterHeader>(&buf)
            .expect("round-trips into its own type");
        assert_eq!(typed.verify_frame(), Ok(()));
        assert_eq!(typed.validate(), Ok(()));
        assert_eq!(typed.user_id, 41);
        assert_eq!(typed.nonce, 0xF00D);

        let buf = {
            let mut buf = aligned_zeroed(HEADER_SIZE);
            buf.copy_from_slice(bytemuck::bytes_of(&forward_register_result(
                ForwardRegisterOutcome::ClientIdOwnedByAnotherUser,
            )));
            buf
        };
        let typed = bytemuck::checked::try_from_bytes::<ForwardRegisterResultHeader>(&buf)
            .expect("round-trips into its own type");
        assert_eq!(typed.verify_frame(), Ok(()));
        assert_eq!(typed.validate(), Ok(()));
        assert_eq!(
            typed.outcome,
            ForwardRegisterOutcome::ClientIdOwnedByAnotherUser
        );
    }

    // The seal is the whole trust story for `user_id`: the primary proposes a
    // register under it without re-verifying credentials, so a flipped bit
    // must not reach the proposal.
    #[test]
    fn forward_register_tampered_user_id_fails_the_seal() {
        const USER_ID_OFFSET: usize = std::mem::offset_of!(ForwardRegisterHeader, user_id);

        assert!(matches!(
            tamper(forward_register(), USER_ID_OFFSET),
            Err(ConsensusError::FrameChecksumMismatch { .. })
        ));
    }

    #[test]
    fn forward_register_validate_rejects_malformed_frames() {
        let mut zero_client = forward_register();
        zero_client.client = 0;
        assert!(matches!(
            zero_client.validate(),
            Err(ConsensusError::InvalidField(_))
        ));

        let mut zero_nonce = forward_register();
        zero_nonce.nonce = 0;
        assert!(matches!(
            zero_nonce.validate(),
            Err(ConsensusError::InvalidField(_))
        ));

        let mut dirty_reserved = forward_register();
        dirty_reserved.reserved[0] = 1;
        assert!(matches!(
            dirty_reserved.validate(),
            Err(ConsensusError::InvalidField(_))
        ));

        let mut wrong_size = forward_register();
        wrong_size.size = 512;
        assert!(matches!(
            wrong_size.validate(),
            Err(ConsensusError::InvalidSize { .. })
        ));

        let mut result_zero_nonce = forward_register_result(ForwardRegisterOutcome::Ok);
        result_zero_nonce.nonce = 0;
        assert!(matches!(
            result_zero_nonce.validate(),
            Err(ConsensusError::InvalidField(_))
        ));

        let mut failed_with_bind = forward_register_result(ForwardRegisterOutcome::NotPrimary);
        failed_with_bind.epoch = 91;
        failed_with_bind.watermark = 12;
        assert!(matches!(
            failed_with_bind.validate(),
            Err(ConsensusError::InvalidField(_))
        ));
        failed_with_bind.epoch = 0;
        failed_with_bind.watermark = 0;
        assert_eq!(failed_with_bind.validate(), Ok(()));
    }

    // An unknown outcome is rejected by the bit-pattern check, one layer
    // before `validate` ever runs.
    #[test]
    fn forward_register_result_rejects_unknown_outcome() {
        const OUTCOME_OFFSET: usize = std::mem::offset_of!(ForwardRegisterResultHeader, outcome);

        let mut buf = aligned_zeroed(HEADER_SIZE);
        buf.copy_from_slice(bytemuck::bytes_of(&forward_register_result(
            ForwardRegisterOutcome::Ok,
        )));
        buf[OUTCOME_OFFSET] = 7;
        assert!(bytemuck::checked::try_from_bytes::<ForwardRegisterResultHeader>(&buf).is_err());
    }

    // Wire-discriminant pin: reordering reinterprets a live cluster's frames.
    #[test]
    fn forward_register_outcome_discriminants_pinned() {
        assert_eq!(ForwardRegisterOutcome::Ok as u8, 0);
        assert_eq!(ForwardRegisterOutcome::NotPrimary as u8, 1);
        assert_eq!(ForwardRegisterOutcome::NotCaughtUp as u8, 2);
        assert_eq!(ForwardRegisterOutcome::PipelineFull as u8, 3);
        assert_eq!(ForwardRegisterOutcome::InProgress as u8, 4);
        assert_eq!(ForwardRegisterOutcome::Canceled as u8, 5);
        assert_eq!(ForwardRegisterOutcome::ClientIdOwnedByAnotherUser as u8, 6);
    }

    fn forward_logout() -> ForwardLogoutHeader {
        let mut header = ForwardLogoutHeader {
            checksum: 0,
            checksum_body: 0,
            cluster: 7,
            size: u32::try_from(HEADER_SIZE).expect("HEADER_SIZE fits u32"),
            view: 3,
            release: 0,
            command: Command::ForwardLogout,
            replica: 2,
            reserved_frame: [0; 66],
            client: 0xCAFE,
            nonce: 0xF00D,
            session: 91,
            request: 12,
            reserved: [0; 80],
        };
        header.seal();
        header
    }

    fn forward_logout_result(outcome: ForwardLogoutOutcome) -> ForwardLogoutResultHeader {
        let mut header = ForwardLogoutResultHeader {
            checksum: 0,
            checksum_body: 0,
            cluster: 7,
            size: u32::try_from(HEADER_SIZE).expect("HEADER_SIZE fits u32"),
            view: 3,
            release: 0,
            command: Command::ForwardLogoutResult,
            replica: 0,
            reserved_frame: [0; 66],
            nonce: 0xF00D,
            client: 0xCAFE,
            commit: if outcome == ForwardLogoutOutcome::Ok {
                92
            } else {
                0
            },
            reserved: [0; 87],
            outcome,
        };
        header.seal();
        header
    }

    #[test]
    fn forward_logout_headers_round_trip_and_validate() {
        let forward = forward_logout();
        assert_eq!(forward.verify_frame(), Ok(()));
        assert_eq!(forward.validate(), Ok(()));

        for outcome in [
            ForwardLogoutOutcome::Ok,
            ForwardLogoutOutcome::NotPrimary,
            ForwardLogoutOutcome::PipelineFull,
            ForwardLogoutOutcome::InProgress,
            ForwardLogoutOutcome::Canceled,
        ] {
            let result = forward_logout_result(outcome);
            assert_eq!(result.verify_frame(), Ok(()));
            assert_eq!(result.validate(), Ok(()));
            let mut bytes = aligned_zeroed(HEADER_SIZE);
            bytes.copy_from_slice(bytemuck::bytes_of(&result));
            let generic = bytemuck::checked::try_from_bytes::<GenericHeader>(&bytes)
                .expect("forward logout result is a valid generic header");
            assert_eq!(generic.command, Command::ForwardLogoutResult);
        }
    }

    #[test]
    fn forward_logout_validate_rejects_malformed_frames() {
        let mut header = forward_logout();
        header.client = 0;
        assert!(matches!(
            header.validate(),
            Err(ConsensusError::InvalidField(_))
        ));
        header = forward_logout();
        header.nonce = 0;
        assert!(matches!(
            header.validate(),
            Err(ConsensusError::InvalidField(_))
        ));
        header = forward_logout();
        header.session = 0;
        assert!(matches!(
            header.validate(),
            Err(ConsensusError::InvalidField(_))
        ));
        header = forward_logout();
        header.request = 0;
        assert!(matches!(
            header.validate(),
            Err(ConsensusError::InvalidField(_))
        ));
        header = forward_logout();
        header.reserved[0] = 1;
        assert!(matches!(
            header.validate(),
            Err(ConsensusError::InvalidField(_))
        ));

        let mut result = forward_logout_result(ForwardLogoutOutcome::NotPrimary);
        result.commit = 92;
        assert!(matches!(
            result.validate(),
            Err(ConsensusError::InvalidField(_))
        ));
    }

    #[test]
    fn forward_logout_result_rejects_unknown_outcome() {
        const OUTCOME_OFFSET: usize = std::mem::offset_of!(ForwardLogoutResultHeader, outcome);

        let mut bytes = aligned_zeroed(HEADER_SIZE);
        bytes.copy_from_slice(bytemuck::bytes_of(&forward_logout_result(
            ForwardLogoutOutcome::Ok,
        )));
        bytes[OUTCOME_OFFSET] = 5;
        assert!(bytemuck::checked::try_from_bytes::<ForwardLogoutResultHeader>(&bytes).is_err());
    }

    #[test]
    fn forward_logout_outcome_discriminants_pinned() {
        assert_eq!(ForwardLogoutOutcome::Ok as u8, 0);
        assert_eq!(ForwardLogoutOutcome::NotPrimary as u8, 1);
        assert_eq!(ForwardLogoutOutcome::PipelineFull as u8, 2);
        assert_eq!(ForwardLogoutOutcome::InProgress as u8, 3);
        assert_eq!(ForwardLogoutOutcome::Canceled as u8, 4);
    }
}
