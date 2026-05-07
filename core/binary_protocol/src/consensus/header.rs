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
pub trait ConsensusHeader: Sized + CheckedBitPattern + NoUninit {
    const COMMAND: Command2;

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
    pub reserved_command: [u8; 128],
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
    assert!(offset_of!(GenericHeader, reserved_command) + size_of::<[u8; 128]>() == HEADER_SIZE);
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
    pub request_checksum: u128,
    pub timestamp: u64,
    pub request: u64,
    pub operation: Operation,
    pub operation_padding: [u8; 7],
    pub namespace: u64,
    pub session: u64,
    pub reserved: [u8; 56],
}
const _: () = {
    assert!(size_of::<RequestHeader>() == HEADER_SIZE);
    assert!(
        offset_of!(RequestHeader, client)
            == offset_of!(RequestHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
    assert!(offset_of!(RequestHeader, reserved) + size_of::<[u8; 56]>() == HEADER_SIZE);
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
            reserved: [0; 56],
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
        // Register: session must be 0, request must be 0.
        // Non-register: session must be > 0, request must be > 0.
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
        } else if self.operation != Operation::Reserved {
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
    pub reserved: [u8; 32],
}
const _: () = {
    assert!(size_of::<ReplyHeader>() == HEADER_SIZE);
    assert!(
        offset_of!(ReplyHeader, request_checksum)
            == offset_of!(ReplyHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
    assert!(offset_of!(ReplyHeader, reserved) + size_of::<[u8; 32]>() == HEADER_SIZE);
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
            reserved: [0; 32],
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
    pub reserved: [u8; 111],
    pub reason: EvictionReason,
}
const _: () = {
    assert!(size_of::<EvictionHeader>() == HEADER_SIZE);
    assert!(
        offset_of!(EvictionHeader, client)
            == offset_of!(EvictionHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
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
    /// params before emitting them.
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
                EvictionReason::ClientReleaseTooLow | EvictionReason::ClientReleaseTooHigh,
            ),
            "EvictionHeader::new: ClientRelease* needs release_min/release_max",
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
            reserved: [0; 111],
            reason,
        }
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
    pub request_checksum: u128,
    pub op: u64,
    pub commit: u64,
    pub timestamp: u64,
    pub request: u64,
    pub operation: Operation,
    pub operation_padding: [u8; 7],
    pub namespace: u64,
    pub reserved: [u8; 32],
}
const _: () = {
    assert!(size_of::<PrepareHeader>() == HEADER_SIZE);
    assert!(
        offset_of!(PrepareHeader, client)
            == offset_of!(PrepareHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
    assert!(offset_of!(PrepareHeader, reserved) + size_of::<[u8; 32]>() == HEADER_SIZE);
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
            reserved: [0; 32],
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
    pub reserved: [u8; 104],
}
const _: () = {
    assert!(size_of::<StartViewHeader>() == HEADER_SIZE);
    assert!(
        offset_of!(StartViewHeader, op)
            == offset_of!(StartViewHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
    assert!(offset_of!(StartViewHeader, reserved) + size_of::<[u8; 104]>() == HEADER_SIZE);
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

// Tests

#[cfg(test)]
mod tests {
    use super::{
        Command2, CommitHeader, ConsensusHeader, DoViewChangeHeader, EvictionHeader,
        EvictionReason, GenericHeader, Operation, PrepareHeader, PrepareOkHeader, ReplyHeader,
        RequestHeader, StartViewChangeHeader, StartViewHeader,
    };

    fn aligned_zeroed(size: usize) -> bytes::BytesMut {
        bytes::BytesMut::zeroed(size)
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
