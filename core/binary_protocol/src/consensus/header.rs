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

use super::{Command2, ConsensusError, Operation};
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
    const COMMAND: Command2;

    /// Whether a frame carrying `command` may be typed as this header.
    /// Defaults to an exact match; a header that serves several commands
    /// with one layout (e.g. `RepairDone` / `RangeEvicted`) widens it.
    #[must_use]
    fn accepts(command: Command2) -> bool {
        command == Self::COMMAND
    }

    /// # Errors
    /// Returns `ConsensusError` if the header fields are inconsistent.
    fn validate(&self) -> Result<(), ConsensusError>;
    fn operation(&self) -> Operation;
    fn command(&self) -> Command2;
    fn size(&self) -> u32;
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
    pub command: Command2,
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
    const COMMAND: Command2 = Command2::Reserved;
    fn operation(&self) -> Operation {
        Operation::Reserved
    }
    fn command(&self) -> Command2 {
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
    pub command: Command2,
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
    pub namespace: u64,
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
    pub reserved: [u8; 52],
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
    assert!(offset_of!(RequestHeader, reserved) + size_of::<[u8; 52]>() == HEADER_SIZE);
};

impl Default for RequestHeader {
    fn default() -> Self {
        Self {
            checksum: 0,
            checksum_body: 0,
            cluster: 0,
            size: 0,
            view: 0,
            release: 0,
            command: Command2::Reserved,
            replica: 0,
            reserved_frame: [0; 66],
            client: 0,
            request_checksum: 0,
            timestamp: 0,
            request: 0,
            operation: Operation::Reserved,
            operation_padding: [0; 7],
            namespace: 0,
            session: 0,
            user_id: 0,
            reserved: [0; 52],
        }
    }
}

impl ConsensusHeader for RequestHeader {
    const COMMAND: Command2 = Command2::Request;
    fn operation(&self) -> Operation {
        self.operation
    }
    fn command(&self) -> Command2 {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command2::Request {
            return Err(ConsensusError::InvalidCommand {
                expected: Command2::Request,
                found: self.command,
            });
        }
        if self.client == 0 {
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
        if self.operation == Operation::Reserved {
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
        if self.operation == Operation::Register {
            if self.session != 0 {
                return Err(ConsensusError::InvalidField(
                    "register: session must be 0".to_string(),
                ));
            }
            if self.request != 0 {
                return Err(ConsensusError::InvalidField(
                    "register: request must be 0".to_string(),
                ));
            }
        } else if self.operation != Operation::NonReplicated {
            if self.session == 0 {
                return Err(ConsensusError::InvalidField(
                    "non-register: session must be > 0".to_string(),
                ));
            }
            if self.request == 0 {
                return Err(ConsensusError::InvalidField(
                    "non-register: request must be > 0".to_string(),
                ));
            }
        }
        Ok(())
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
    pub command: Command2,
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
    pub namespace: u64,
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
    pub reserved: [u8; 28],
}
const _: () = {
    assert!(size_of::<ReplyHeader>() == HEADER_SIZE);
    assert!(
        offset_of!(ReplyHeader, request_checksum)
            == offset_of!(ReplyHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
    assert!(
        offset_of!(ReplyHeader, status) == offset_of!(ReplyHeader, namespace) + size_of::<u64>()
    );
    assert!(offset_of!(ReplyHeader, reserved) + size_of::<[u8; 28]>() == HEADER_SIZE);
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
            command: Command2::Reserved,
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
            namespace: 0,
            status: 0,
            reserved: [0; 28],
        }
    }
}

impl ConsensusHeader for ReplyHeader {
    const COMMAND: Command2 = Command2::Reply;
    fn operation(&self) -> Operation {
        self.operation
    }
    fn command(&self) -> Command2 {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command2::Reply {
            return Err(ConsensusError::ReplyInvalidCommand2);
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
    pub command: Command2,
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
            command: Command2::Eviction,
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
    const COMMAND: Command2 = Command2::Eviction;
    /// Session-level (not per-op): always `Reserved`.
    fn operation(&self) -> Operation {
        Operation::Reserved
    }
    fn command(&self) -> Command2 {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    #[allow(clippy::cast_possible_truncation)]
    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command2::Eviction {
            return Err(ConsensusError::InvalidCommand {
                expected: Command2::Eviction,
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
#[derive(Debug, Clone, Copy, CheckedBitPattern, NoUninit)]
pub struct PrepareHeader {
    pub checksum: u128,
    pub checksum_body: u128,
    pub cluster: u128,
    pub size: u32,
    pub view: u32,
    pub release: u32,
    pub command: Command2,
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
    pub namespace: u64,
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
        offset_of!(PrepareHeader, user_id)
            == offset_of!(PrepareHeader, namespace) + size_of::<u64>()
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
            command: Command2::Reserved,
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
            namespace: 0,
            user_id: 0,
            reserved: [0; 28],
        }
    }
}

impl ConsensusHeader for PrepareHeader {
    const COMMAND: Command2 = Command2::Prepare;
    fn operation(&self) -> Operation {
        self.operation
    }
    fn command(&self) -> Command2 {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command2::Prepare {
            return Err(ConsensusError::InvalidCommand {
                expected: Command2::Prepare,
                found: self.command,
            });
        }
        Ok(())
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
    const COMMAND: Command2 = Command2::RepairPrepare;
    fn operation(&self) -> Operation {
        self.0.operation
    }
    fn command(&self) -> Command2 {
        self.0.command
    }
    fn size(&self) -> u32 {
        self.0.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.0.command != Command2::RepairPrepare {
            return Err(ConsensusError::InvalidCommand {
                expected: Command2::RepairPrepare,
                found: self.0.command,
            });
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
    pub command: Command2,
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
    pub namespace: u64,
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
            command: Command2::Reserved,
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
            namespace: 0,
            reserved: [0; 48],
        }
    }
}

impl ConsensusHeader for PrepareOkHeader {
    const COMMAND: Command2 = Command2::PrepareOk;
    fn operation(&self) -> Operation {
        self.operation
    }
    fn command(&self) -> Command2 {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command2::PrepareOk {
            return Err(ConsensusError::InvalidCommand {
                expected: Command2::PrepareOk,
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
    pub command: Command2,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    pub commit_checksum: u128,
    pub timestamp_monotonic: u64,
    pub commit: u64,
    pub checkpoint_op: u64,
    pub namespace: u64,
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
    const COMMAND: Command2 = Command2::Commit;
    fn operation(&self) -> Operation {
        Operation::Reserved
    }
    fn command(&self) -> Command2 {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command2::Commit {
            return Err(ConsensusError::CommitInvalidCommand2);
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
    pub command: Command2,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    pub namespace: u64,
    pub reserved: [u8; 120],
}
const _: () = {
    assert!(size_of::<StartViewChangeHeader>() == HEADER_SIZE);
    assert!(
        offset_of!(StartViewChangeHeader, namespace)
            == offset_of!(StartViewChangeHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
    assert!(offset_of!(StartViewChangeHeader, reserved) + size_of::<[u8; 120]>() == HEADER_SIZE);
};

impl ConsensusHeader for StartViewChangeHeader {
    const COMMAND: Command2 = Command2::StartViewChange;
    fn operation(&self) -> Operation {
        Operation::Reserved
    }
    fn command(&self) -> Command2 {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command2::StartViewChange {
            return Err(ConsensusError::InvalidCommand {
                expected: Command2::StartViewChange,
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
    pub command: Command2,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    /// Highest op-number in this replica's log.
    pub op: u64,
    /// Highest committed op.
    pub commit: u64,
    pub namespace: u64,
    /// View when status was last normal (key for log selection).
    pub log_view: u32,
    pub reserved: [u8; 100],
}
const _: () = {
    assert!(size_of::<DoViewChangeHeader>() == HEADER_SIZE);
    assert!(
        offset_of!(DoViewChangeHeader, op)
            == offset_of!(DoViewChangeHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
    assert!(offset_of!(DoViewChangeHeader, reserved) + size_of::<[u8; 100]>() == HEADER_SIZE);
};

impl ConsensusHeader for DoViewChangeHeader {
    const COMMAND: Command2 = Command2::DoViewChange;
    fn operation(&self) -> Operation {
        Operation::Reserved
    }
    fn command(&self) -> Command2 {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command2::DoViewChange {
            return Err(ConsensusError::InvalidCommand {
                expected: Command2::DoViewChange,
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
        Ok(())
    }
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
    pub command: Command2,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    /// Highest op in the new primary's log.
    pub op: u64,
    /// max(commit) from all DVCs.
    pub commit: u64,
    pub namespace: u64,
    pub reserved: [u8; 88],
    /// Sender's incarnation, echoed from the `RequestStartView` this answers so a
    /// recovering replica can prove the reply post-dates its restart (see
    /// `RequestStartViewHeader::incarnation`). `0` on an unsolicited `StartView`
    /// (a normal view-change completion), which carries no freshness claim.
    ///
    /// Carved from the tail of the former `reserved` region and placed LAST so it
    /// lands 16-aligned with no padding WITHOUT moving `op`/`commit`/`namespace`.
    /// A peer that predates it sends zeros, decoding as `incarnation == 0`, which
    /// the `handle_start_view` guard treats as no claim rather than as a foreign
    /// one, so a mixed-version rolling upgrade is wire-compatible: the pre-upgrade
    /// peer's `StartView` is judged by the view checks alone, as before the field.
    pub incarnation: u128,
}
const _: () = {
    assert!(size_of::<StartViewHeader>() == HEADER_SIZE);
    // op/commit/namespace keep their pre-incarnation offsets.
    assert!(
        offset_of!(StartViewHeader, op)
            == offset_of!(StartViewHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
    // `incarnation` is last and 16-aligned, so the struct has no padding.
    assert!(offset_of!(StartViewHeader, incarnation) + size_of::<u128>() == HEADER_SIZE);
    assert!(offset_of!(StartViewHeader, incarnation) % 16 == 0);
};

impl ConsensusHeader for StartViewHeader {
    const COMMAND: Command2 = Command2::StartView;
    fn operation(&self) -> Operation {
        Operation::Reserved
    }
    fn command(&self) -> Command2 {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command2::StartView {
            return Err(ConsensusError::InvalidCommand {
                expected: Command2::StartView,
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
        Ok(())
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
    pub command: Command2,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    pub namespace: u64,
    pub reserved: [u8; 104],
    /// The requester's per-boot incarnation, echoed back in the answering
    /// `StartView` so a reply from a previous incarnation is detectable.
    ///
    /// Carved from the tail of the former `reserved` region and placed LAST so it
    /// lands 16-aligned with no padding WITHOUT moving `namespace`. A peer that
    /// predates it sends zeros, decoding as `incarnation == 0`, so a mixed-version
    /// rolling upgrade is wire-compatible.
    pub incarnation: u128,
}
const _: () = {
    assert!(size_of::<RequestStartViewHeader>() == HEADER_SIZE);
    // namespace keeps its pre-incarnation offset.
    assert!(
        offset_of!(RequestStartViewHeader, namespace)
            == offset_of!(RequestStartViewHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
    // `incarnation` is last and 16-aligned, so the struct has no padding.
    assert!(offset_of!(RequestStartViewHeader, incarnation) + size_of::<u128>() == HEADER_SIZE);
    assert!(offset_of!(RequestStartViewHeader, incarnation) % 16 == 0);
};

impl ConsensusHeader for RequestStartViewHeader {
    const COMMAND: Command2 = Command2::RequestStartView;
    fn operation(&self) -> Operation {
        Operation::Reserved
    }
    fn command(&self) -> Command2 {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command2::RequestStartView {
            return Err(ConsensusError::InvalidCommand {
                expected: Command2::RequestStartView,
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
/// `[from_op, to_op]` for `namespace`. Header-only. The peer answers with
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
    pub command: Command2,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    pub nonce: u128,
    pub from_op: u64,
    pub to_op: u64,
    pub namespace: u64,
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
    const COMMAND: Command2 = Command2::RequestPrepares;
    fn operation(&self) -> Operation {
        Operation::Reserved
    }
    fn command(&self) -> Command2 {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command2::RequestPrepares {
            return Err(ConsensusError::InvalidCommand {
                expected: Command2::RequestPrepares,
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
    pub command: Command2,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    pub nonce: u128,
    /// `RepairDone`: last op served. `RangeEvicted`: oldest retained op.
    pub op: u64,
    pub namespace: u64,
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
    const COMMAND: Command2 = Command2::RepairDone;
    // One layout, two commands: `RepairDone` terminates a stream,
    // `RangeEvicted` prefixes it. Without this widening, `try_into_typed`
    // rejects `RangeEvicted` frames before `validate` ever sees them.
    fn accepts(command: Command2) -> bool {
        command == Command2::RepairDone || command == Command2::RangeEvicted
    }
    fn operation(&self) -> Operation {
        Operation::Reserved
    }
    fn command(&self) -> Command2 {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command2::RepairDone && self.command != Command2::RangeEvicted {
            return Err(ConsensusError::InvalidCommand {
                expected: Command2::RepairDone,
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
    pub command: Command2,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    pub nonce: u128,
    pub namespace: u64,
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
    const COMMAND: Command2 = Command2::RequestStateTransfer;
    fn operation(&self) -> Operation {
        Operation::Reserved
    }
    fn command(&self) -> Command2 {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    #[allow(clippy::cast_possible_truncation)]
    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command2::RequestStateTransfer {
            return Err(ConsensusError::InvalidCommand {
                expected: Command2::RequestStateTransfer,
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
    pub command: Command2,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    pub nonce: u128,
    /// Serving primary's applied frontier (`commit_min`) when the descriptor
    /// was built. The receiver's tail repair targets past this.
    pub commit_op: u64,
    pub namespace: u64,
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
    assert!(offset_of!(StateTransferTargetHeader, namespace) == 152);
    assert!(offset_of!(StateTransferTargetHeader, available) == 160);
    assert!(offset_of!(StateTransferTargetHeader, unavailable_transient) == 161);
    assert!(offset_of!(StateTransferTargetHeader, commit_max) == 168);
    assert!(offset_of!(StateTransferTargetHeader, reserved) + size_of::<[u8; 80]>() == HEADER_SIZE);
};

impl ConsensusHeader for StateTransferTargetHeader {
    const COMMAND: Command2 = Command2::StateTransferTarget;
    fn operation(&self) -> Operation {
        Operation::Reserved
    }
    fn command(&self) -> Command2 {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command2::StateTransferTarget {
            return Err(ConsensusError::InvalidCommand {
                expected: Command2::StateTransferTarget,
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
    pub command: Command2,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    pub nonce: u128,
    pub offset: u64,
    pub namespace: u64,
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
    const COMMAND: Command2 = Command2::RequestStateChunk;
    fn operation(&self) -> Operation {
        Operation::Reserved
    }
    fn command(&self) -> Command2 {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    #[allow(clippy::cast_possible_truncation)]
    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command2::RequestStateChunk {
            return Err(ConsensusError::InvalidCommand {
                expected: Command2::RequestStateChunk,
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
    pub command: Command2,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    pub nonce: u128,
    pub offset: u64,
    pub namespace: u64,
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
    const COMMAND: Command2 = Command2::StateChunk;
    fn operation(&self) -> Operation {
        Operation::Reserved
    }
    fn command(&self) -> Command2 {
        self.command
    }
    fn size(&self) -> u32 {
        self.size
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command2::StateChunk {
            return Err(ConsensusError::InvalidCommand {
                expected: Command2::StateChunk,
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

// Tests

#[cfg(test)]
mod tests {
    use super::{
        Command2, CommitHeader, ConsensusHeader, DoViewChangeHeader, EvictionHeader,
        EvictionReason, GenericHeader, HEADER_SIZE, Operation, PrepareHeader, PrepareOkHeader,
        ReplyHeader, RequestHeader, StartViewChangeHeader, StartViewHeader,
    };
    use aligned_vec::{AVec, ConstAlign};

    // bytemuck requires 16-byte alignment (see `ConsensusHeader` trait doc).
    // `BytesMut::zeroed` works on glibc by accident, fails under Miri.
    fn aligned_zeroed(size: usize) -> AVec<u8, ConstAlign<16>> {
        let mut v: AVec<u8, ConstAlign<16>> = AVec::new(16);
        v.resize(size, 0);
        v
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
        assert_eq!(header.command, Command2::Reserved);
        assert_eq!(header.size, 0);
    }

    #[test]
    fn request_header_zero_copy() {
        let mut buf = aligned_zeroed(256);
        buf[60] = Command2::Request as u8;
        // client offset = 60 + 1 (replica) + 66 (reserved_frame) = 128.
        // validate rejects client == 0.
        buf[128] = 1;
        // Register (session 0, request 0 are its required values); a zeroed
        // operation is `Reserved`, which validate rejects.
        buf[std::mem::offset_of!(RequestHeader, operation)] = Operation::Register as u8;
        let header: &RequestHeader = bytemuck::checked::try_from_bytes(&buf).unwrap();
        assert_eq!(header.command, Command2::Request);
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
            command: Command2::Request,
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
            command: Command2::Request,
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
            command: Command2::Request,
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
            command: Command2::Request,
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
            command: Command2::Request,
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
            command: Command2::Request,
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
        buf[60] = Command2::Reply as u8;
        let header: &ReplyHeader = bytemuck::checked::try_from_bytes(&buf).unwrap();
        assert_eq!(header.command, Command2::Reply);
        assert!(header.validate().is_ok());
    }

    // `status` is carved from the reserved tail; the SDK reply funnel peeks it
    // at this offset before any body decode, so a layout drift must trip here.
    #[test]
    fn reply_header_status_offset_and_size_pinned() {
        use std::mem::offset_of;
        assert_eq!(size_of::<ReplyHeader>(), HEADER_SIZE);
        assert_eq!(
            offset_of!(ReplyHeader, status),
            offset_of!(ReplyHeader, namespace) + size_of::<u64>()
        );
        assert_eq!(
            offset_of!(ReplyHeader, reserved) + size_of::<[u8; 28]>(),
            HEADER_SIZE
        );
    }

    // A nonzero status rides the reserved region, which reply `validate` does
    // not inspect: a status-bearing reply stays valid so the SDK can peek it.
    #[test]
    fn reply_header_nonzero_status_still_validates() {
        let header = ReplyHeader {
            command: Command2::Reply,
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
}
