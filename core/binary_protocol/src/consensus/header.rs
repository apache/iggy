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

// ---------------------------------------------------------------------------
// GenericHeader - type-erased dispatch
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// RequestHeader - client -> primary
// ---------------------------------------------------------------------------

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
    pub reserved: [u8; 64],
}
const _: () = {
    assert!(size_of::<RequestHeader>() == HEADER_SIZE);
    assert!(
        offset_of!(RequestHeader, client)
            == offset_of!(RequestHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
    assert!(offset_of!(RequestHeader, reserved) + size_of::<[u8; 64]>() == HEADER_SIZE);
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
            reserved: [0; 64],
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
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ReplyHeader - primary -> client
// ---------------------------------------------------------------------------

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
    assert!(size_of::<ReplyHeader>() == HEADER_SIZE);
    assert!(
        offset_of!(ReplyHeader, request_checksum)
            == offset_of!(ReplyHeader, reserved_frame) + size_of::<[u8; 66]>()
    );
    assert!(offset_of!(ReplyHeader, reserved) + size_of::<[u8; 48]>() == HEADER_SIZE);
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

// ---------------------------------------------------------------------------
// PrepareHeader - primary -> replicas (replication)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// PrepareOkHeader - replica -> primary (acknowledgement)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// CommitHeader - primary -> replicas (commit, header-only)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// StartViewChangeHeader - failure detection (header-only)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// DoViewChangeHeader - view change vote (header-only)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// StartViewHeader - new view announcement (header-only)
// ---------------------------------------------------------------------------

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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::{
        Command2, CommitHeader, ConsensusHeader, DoViewChangeHeader, GenericHeader, PrepareHeader,
        PrepareOkHeader, ReplyHeader, RequestHeader, StartViewChangeHeader, StartViewHeader,
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
    fn reply_header_zero_copy() {
        let mut buf = aligned_zeroed(256);
        buf[60] = Command2::Reply as u8;
        let header: &ReplyHeader = bytemuck::checked::try_from_bytes(&buf).unwrap();
        assert_eq!(header.command, Command2::Reply);
        assert!(header.validate().is_ok());
    }
}
