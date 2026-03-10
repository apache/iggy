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

use bytemuck::{CheckedBitPattern, NoUninit, Pod, Zeroable};
use enumset::EnumSetType;
use thiserror::Error;

const HEADER_SIZE: usize = 256;
pub trait ConsensusHeader: Sized + CheckedBitPattern + NoUninit {
    const COMMAND: Command2;

    fn validate(&self) -> Result<(), ConsensusError>;
    fn operation(&self) -> Operation;
    fn command(&self) -> Command2;
    fn size(&self) -> u32;
}

#[derive(Default, Debug, EnumSetType)]
#[repr(u8)]
pub enum Command2 {
    #[default]
    Reserved = 0,

    Ping = 1,
    Pong = 2,
    PingClient = 3,
    PongClient = 4,

    Request = 5,
    Prepare = 6,
    PrepareOk = 7,
    Reply = 8,
    Commit = 9,

    StartViewChange = 10,
    DoViewChange = 11,
    StartView = 12,
}

// SAFETY: Command2 is #[repr(u8)] with no padding bytes.
unsafe impl NoUninit for Command2 {}

// SAFETY: Command2 is #[repr(u8)]; is_valid_bit_pattern matches all defined discriminants.
unsafe impl CheckedBitPattern for Command2 {
    type Bits = u8;

    fn is_valid_bit_pattern(bits: &u8) -> bool {
        *bits <= 12
    }
}

impl TryFrom<u8> for Command2 {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Command2::Reserved),
            1 => Ok(Command2::Ping),
            2 => Ok(Command2::Pong),
            3 => Ok(Command2::PingClient),
            4 => Ok(Command2::PongClient),
            5 => Ok(Command2::Request),
            6 => Ok(Command2::Prepare),
            7 => Ok(Command2::PrepareOk),
            8 => Ok(Command2::Reply),
            9 => Ok(Command2::Commit),
            10 => Ok(Command2::StartViewChange),
            11 => Ok(Command2::DoViewChange),
            12 => Ok(Command2::StartView),
            _ => Err(value),
        }
    }
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum ConsensusError {
    #[error("invalid command: expected {expected:?}, found {found:?}")]
    InvalidCommand { expected: Command2, found: Command2 },

    #[error("invalid command byte: 0x{0:02X} is not a valid Command2 discriminant")]
    InvalidCommandByte(u8),

    #[error("invalid operation byte: 0x{0:02X} is not a valid Operation discriminant")]
    InvalidOperationByte(u8),

    #[error("invalid size: expected {expected:?}, found {found:?}")]
    InvalidSize { expected: u32, found: u32 },

    #[error("invalid checksum")]
    InvalidChecksum,

    #[error("invalid cluster ID")]
    InvalidCluster,

    #[error("invalid field: {0}")]
    InvalidField(String),

    #[error("parent_padding must be 0")]
    PrepareParentPaddingNonZero,

    #[error("request_checksum_padding must be 0")]
    PrepareRequestChecksumPaddingNonZero,

    #[error("command must be Commit")]
    CommitInvalidCommand2,

    #[error("size must be 256, found {0}")]
    CommitInvalidSize(u32),

    // ReplyHeader specific
    #[error("command must be Reply")]
    ReplyInvalidCommand2,

    #[error("request_checksum_padding must be 0")]
    ReplyRequestChecksumPaddingNonZero,

    #[error("context_padding must be 0")]
    ReplyContextPaddingNonZero,

    #[error("invalid bit pattern in header (enum discriminant out of range)")]
    InvalidBitPattern,
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, NoUninit, CheckedBitPattern)]
#[repr(u8)]
pub enum Operation {
    #[default]
    Reserved = 0,
    CreateStream = 128,
    UpdateStream = 129,
    DeleteStream = 130,
    PurgeStream = 131,
    CreateTopic = 132,
    UpdateTopic = 133,
    DeleteTopic = 134,
    PurgeTopic = 135,
    CreatePartitions = 136,
    DeletePartitions = 137,
    DeleteSegments = 138,
    CreateConsumerGroup = 139,
    DeleteConsumerGroup = 140,
    CreateUser = 141,
    UpdateUser = 142,
    DeleteUser = 143,
    ChangePassword = 144,
    UpdatePermissions = 145,
    CreatePersonalAccessToken = 146,
    DeletePersonalAccessToken = 147,

    // Partition operations (replicated via consensus)
    SendMessages = 160,
    StoreConsumerOffset = 161,
}

impl TryFrom<u8> for Operation {
    type Error = u8;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Operation::Reserved),
            128 => Ok(Operation::CreateStream),
            129 => Ok(Operation::UpdateStream),
            130 => Ok(Operation::DeleteStream),
            131 => Ok(Operation::PurgeStream),
            132 => Ok(Operation::CreateTopic),
            133 => Ok(Operation::UpdateTopic),
            134 => Ok(Operation::DeleteTopic),
            135 => Ok(Operation::PurgeTopic),
            136 => Ok(Operation::CreatePartitions),
            137 => Ok(Operation::DeletePartitions),
            138 => Ok(Operation::DeleteSegments),
            139 => Ok(Operation::CreateConsumerGroup),
            140 => Ok(Operation::DeleteConsumerGroup),
            141 => Ok(Operation::CreateUser),
            142 => Ok(Operation::UpdateUser),
            143 => Ok(Operation::DeleteUser),
            144 => Ok(Operation::ChangePassword),
            145 => Ok(Operation::UpdatePermissions),
            146 => Ok(Operation::CreatePersonalAccessToken),
            147 => Ok(Operation::DeletePersonalAccessToken),
            160 => Ok(Operation::SendMessages),
            161 => Ok(Operation::StoreConsumerOffset),
            _ => Err(value),
        }
    }
}

impl Operation {
    /// Returns `true` for metadata / control-plane operations (streams, topics,
    /// users, consumer groups, etc.) that are always handled by shard 0.
    #[inline]
    pub fn is_metadata(&self) -> bool {
        matches!(
            self,
            Operation::CreateStream
                | Operation::UpdateStream
                | Operation::DeleteStream
                | Operation::PurgeStream
                | Operation::CreateTopic
                | Operation::UpdateTopic
                | Operation::DeleteTopic
                | Operation::PurgeTopic
                | Operation::CreatePartitions
                | Operation::DeletePartitions
                | Operation::CreateConsumerGroup
                | Operation::DeleteConsumerGroup
                | Operation::CreateUser
                | Operation::UpdateUser
                | Operation::DeleteUser
                | Operation::ChangePassword
                | Operation::UpdatePermissions
                | Operation::CreatePersonalAccessToken
                | Operation::DeletePersonalAccessToken
        )
    }

    /// Returns `true` for data-plane operations that are routed to the shard
    /// owning the partition identified by the message's namespace.
    #[inline]
    pub fn is_partition(&self) -> bool {
        matches!(
            self,
            Operation::SendMessages | Operation::StoreConsumerOffset | Operation::DeleteSegments
        )
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct GenericHeader {
    pub checksum: u128,
    pub checksum_body: u128,
    pub cluster: u128,
    pub size: u32,
    pub view: u32,
    pub release: u32,
    pub command: u8,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    pub reserved_command: [u8; 128],
}
const _: () = {
    assert!(core::mem::size_of::<GenericHeader>() == HEADER_SIZE);
    // Ensure no implicit padding is inserted between reserved_frame and the body fields.
    assert!(
        core::mem::offset_of!(GenericHeader, reserved_command)
            == core::mem::offset_of!(GenericHeader, reserved_frame)
                + core::mem::size_of::<[u8; 66]>()
    );
    // Ensure no implicit tail padding is inserted after the explicit trailing bytes.
    assert!(
        core::mem::offset_of!(GenericHeader, reserved_command) + core::mem::size_of::<[u8; 128]>()
            == HEADER_SIZE
    );
};

unsafe impl Pod for GenericHeader {}
unsafe impl Zeroable for GenericHeader {}

impl ConsensusHeader for GenericHeader {
    const COMMAND: Command2 = Command2::Reserved;

    fn operation(&self) -> Operation {
        Operation::Reserved
    }

    fn command(&self) -> Command2 {
        Command2::try_from(self.command).unwrap_or(Command2::Reserved)
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        Command2::try_from(self.command).map_err(ConsensusError::InvalidCommandByte)?;
        Ok(())
    }

    fn size(&self) -> u32 {
        self.size
    }
}

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
    assert!(core::mem::size_of::<RequestHeader>() == HEADER_SIZE);
    // Ensure no implicit padding is inserted between reserved_frame and the body fields.
    assert!(
        core::mem::offset_of!(RequestHeader, client)
            == core::mem::offset_of!(RequestHeader, reserved_frame)
                + core::mem::size_of::<[u8; 66]>()
    );
    // Ensure no implicit tail padding is inserted after the explicit trailing bytes.
    assert!(
        core::mem::offset_of!(RequestHeader, reserved) + core::mem::size_of::<[u8; 64]>()
            == HEADER_SIZE
    );
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

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command2::Request {
            return Err(ConsensusError::InvalidCommand {
                expected: Command2::Request,
                found: self.command,
            });
        }
        Ok(())
    }
    fn command(&self) -> Command2 {
        self.command
    }

    fn size(&self) -> u32 {
        self.size
    }
}

// TODO: Manually impl default (and use a const for the `release`)
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
    assert!(core::mem::size_of::<PrepareHeader>() == HEADER_SIZE);
    // Ensure no implicit padding is inserted between reserved_frame and the body fields.
    assert!(
        core::mem::offset_of!(PrepareHeader, client)
            == core::mem::offset_of!(PrepareHeader, reserved_frame)
                + core::mem::size_of::<[u8; 66]>()
    );
    // Ensure no implicit tail padding is inserted after the explicit trailing bytes.
    assert!(
        core::mem::offset_of!(PrepareHeader, reserved) + core::mem::size_of::<[u8; 32]>()
            == HEADER_SIZE
    );
};

impl ConsensusHeader for PrepareHeader {
    const COMMAND: Command2 = Command2::Prepare;

    fn operation(&self) -> Operation {
        self.operation
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
    fn command(&self) -> Command2 {
        self.command
    }

    fn size(&self) -> u32 {
        self.size
    }
}

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

// TODO: Manually impl default (and use a const for the `release`)
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
    assert!(core::mem::size_of::<PrepareOkHeader>() == HEADER_SIZE);
    // Ensure no implicit padding is inserted between reserved_frame and the body fields.
    assert!(
        core::mem::offset_of!(PrepareOkHeader, parent)
            == core::mem::offset_of!(PrepareOkHeader, reserved_frame)
                + core::mem::size_of::<[u8; 66]>()
    );
    // Ensure no implicit tail padding is inserted after the explicit trailing bytes.
    assert!(
        core::mem::offset_of!(PrepareOkHeader, reserved) + core::mem::size_of::<[u8; 48]>()
            == HEADER_SIZE
    );
};

impl ConsensusHeader for PrepareOkHeader {
    const COMMAND: Command2 = Command2::PrepareOk;

    fn operation(&self) -> Operation {
        self.operation
    }
    fn command(&self) -> Command2 {
        self.command
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

    fn size(&self) -> u32 {
        self.size
    }
}

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
    assert!(core::mem::size_of::<CommitHeader>() == HEADER_SIZE);
    // Ensure no implicit padding is inserted between reserved_frame and the body fields.
    assert!(
        core::mem::offset_of!(CommitHeader, commit_checksum)
            == core::mem::offset_of!(CommitHeader, reserved_frame)
                + core::mem::size_of::<[u8; 66]>()
    );
    // Ensure no implicit tail padding is inserted after the explicit trailing bytes.
    assert!(
        core::mem::offset_of!(CommitHeader, reserved) + core::mem::size_of::<[u8; 80]>()
            == HEADER_SIZE
    );
};

impl ConsensusHeader for CommitHeader {
    const COMMAND: Command2 = Command2::Commit;

    fn operation(&self) -> Operation {
        Operation::Reserved
    }
    fn command(&self) -> Command2 {
        self.command
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

    fn size(&self) -> u32 {
        self.size
    }
}

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
    assert!(core::mem::size_of::<ReplyHeader>() == HEADER_SIZE);
    // Ensure no implicit padding is inserted between reserved_frame and the body fields.
    assert!(
        core::mem::offset_of!(ReplyHeader, request_checksum)
            == core::mem::offset_of!(ReplyHeader, reserved_frame)
                + core::mem::size_of::<[u8; 66]>()
    );
    // Ensure no implicit tail padding is inserted after the explicit trailing bytes.
    assert!(
        core::mem::offset_of!(ReplyHeader, reserved) + core::mem::size_of::<[u8; 48]>()
            == HEADER_SIZE
    );
};

impl ConsensusHeader for ReplyHeader {
    const COMMAND: Command2 = Command2::Reply;

    fn operation(&self) -> Operation {
        self.operation
    }
    fn command(&self) -> Command2 {
        self.command
    }

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command2::Reply {
            return Err(ConsensusError::ReplyInvalidCommand2);
        }
        Ok(())
    }

    fn size(&self) -> u32 {
        self.size
    }
}

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

/// StartViewChange message header.
///
/// Sent by a replica when it suspects the primary has failed.
/// This is a header-only message with no body.
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
    assert!(core::mem::size_of::<StartViewChangeHeader>() == HEADER_SIZE);
    // Ensure no implicit padding is inserted between reserved_frame and the body fields.
    assert!(
        core::mem::offset_of!(StartViewChangeHeader, namespace)
            == core::mem::offset_of!(StartViewChangeHeader, reserved_frame)
                + core::mem::size_of::<[u8; 66]>()
    );
    // Ensure no implicit tail padding is inserted after the explicit trailing bytes.
    assert!(
        core::mem::offset_of!(StartViewChangeHeader, reserved) + core::mem::size_of::<[u8; 120]>()
            == HEADER_SIZE
    );
};

impl ConsensusHeader for StartViewChangeHeader {
    const COMMAND: Command2 = Command2::StartViewChange;

    fn operation(&self) -> Operation {
        Operation::Reserved
    }
    fn command(&self) -> Command2 {
        self.command
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

    fn size(&self) -> u32 {
        self.size
    }
}

/// DoViewChange message header.
///
/// Sent by replicas to the primary candidate after collecting a quorum of
/// StartViewChange messages.
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

    /// The highest op-number in this replica's log.
    /// Used to select the most complete log when log_view values are equal.
    pub op: u64,
    /// The replica's commit number (highest committed op).
    /// The new primary sets its commit to max(commit) across all DVCs.
    pub commit: u64,
    pub namespace: u64,
    /// The view number when this replica's status was last normal.
    /// This is the key field for log selection: the replica with the
    /// highest log_view has the most authoritative log.
    pub log_view: u32,
    pub reserved: [u8; 100],
}
const _: () = {
    assert!(core::mem::size_of::<DoViewChangeHeader>() == HEADER_SIZE);
    // Ensure no implicit padding is inserted between reserved_frame and the body fields.
    assert!(
        core::mem::offset_of!(DoViewChangeHeader, op)
            == core::mem::offset_of!(DoViewChangeHeader, reserved_frame)
                + core::mem::size_of::<[u8; 66]>()
    );
    // Ensure no implicit tail padding is inserted after the explicit trailing bytes.
    assert!(
        core::mem::offset_of!(DoViewChangeHeader, reserved) + core::mem::size_of::<[u8; 100]>()
            == HEADER_SIZE
    );
};

impl ConsensusHeader for DoViewChangeHeader {
    const COMMAND: Command2 = Command2::DoViewChange;

    fn operation(&self) -> Operation {
        Operation::Reserved
    }
    fn command(&self) -> Command2 {
        self.command
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

        // log_view must be <= view (can't have been normal in a future view)
        if self.log_view > self.view {
            return Err(ConsensusError::InvalidField(
                "log_view cannot exceed view".to_string(),
            ));
        }

        // commit must be <= op (can't commit what we haven't seen)
        if self.commit > self.op {
            return Err(ConsensusError::InvalidField(
                "commit cannot exceed op".to_string(),
            ));
        }
        Ok(())
    }

    fn size(&self) -> u32 {
        self.size
    }
}

/// StartView message header.
///
/// Sent by the new primary to all replicas after collecting a quorum of
/// DoViewChange messages.
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

    /// The op-number of the highest entry in the new primary's log.
    /// Backups set their op to this value.
    pub op: u64,
    /// The commit number.
    /// This is max(commit) from all DVCs received by the primary.
    /// Backups set their commit to this value.
    pub commit: u64,
    pub namespace: u64,
    pub reserved: [u8; 104],
}
const _: () = {
    assert!(core::mem::size_of::<StartViewHeader>() == HEADER_SIZE);
    // Ensure no implicit padding is inserted between reserved_frame and the body fields.
    assert!(
        core::mem::offset_of!(StartViewHeader, op)
            == core::mem::offset_of!(StartViewHeader, reserved_frame)
                + core::mem::size_of::<[u8; 66]>()
    );
    // Ensure no implicit tail padding is inserted after the explicit trailing bytes.
    assert!(
        core::mem::offset_of!(StartViewHeader, reserved) + core::mem::size_of::<[u8; 104]>()
            == HEADER_SIZE
    );
};

impl ConsensusHeader for StartViewHeader {
    const COMMAND: Command2 = Command2::StartView;

    fn operation(&self) -> Operation {
        Operation::Reserved
    }
    fn command(&self) -> Command2 {
        self.command
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

        // commit must be <= op
        if self.commit > self.op {
            return Err(ConsensusError::InvalidField(
                "commit cannot exceed op".to_string(),
            ));
        }
        Ok(())
    }

    fn size(&self) -> u32 {
        self.size
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command2_try_from_valid() {
        let cases = [
            (0, Command2::Reserved),
            (1, Command2::Ping),
            (2, Command2::Pong),
            (3, Command2::PingClient),
            (4, Command2::PongClient),
            (5, Command2::Request),
            (6, Command2::Prepare),
            (7, Command2::PrepareOk),
            (8, Command2::Reply),
            (9, Command2::Commit),
            (10, Command2::StartViewChange),
            (11, Command2::DoViewChange),
            (12, Command2::StartView),
        ];
        for (byte, expected) in cases {
            assert_eq!(Command2::try_from(byte), Ok(expected), "byte {byte}");
        }
    }

    #[test]
    fn command2_try_from_invalid() {
        for byte in [13, 14, 100, 255] {
            assert_eq!(Command2::try_from(byte), Err(byte), "byte {byte}");
        }
    }

    #[test]
    fn command2_roundtrip() {
        for byte in 0u8..=12 {
            let cmd = Command2::try_from(byte).unwrap();
            assert_eq!(cmd as u8, byte);
        }
    }

    #[test]
    fn operation_try_from_valid() {
        let cases = [
            (0, Operation::Reserved),
            (128, Operation::CreateStream),
            (129, Operation::UpdateStream),
            (130, Operation::DeleteStream),
            (131, Operation::PurgeStream),
            (132, Operation::CreateTopic),
            (133, Operation::UpdateTopic),
            (134, Operation::DeleteTopic),
            (135, Operation::PurgeTopic),
            (136, Operation::CreatePartitions),
            (137, Operation::DeletePartitions),
            (138, Operation::DeleteSegments),
            (139, Operation::CreateConsumerGroup),
            (140, Operation::DeleteConsumerGroup),
            (141, Operation::CreateUser),
            (142, Operation::UpdateUser),
            (143, Operation::DeleteUser),
            (144, Operation::ChangePassword),
            (145, Operation::UpdatePermissions),
            (146, Operation::CreatePersonalAccessToken),
            (147, Operation::DeletePersonalAccessToken),
            (160, Operation::SendMessages),
            (161, Operation::StoreConsumerOffset),
        ];
        for (byte, expected) in cases {
            assert_eq!(Operation::try_from(byte), Ok(expected), "byte {byte}");
        }
    }

    #[test]
    fn operation_try_from_invalid() {
        for byte in [1, 127, 148, 159, 162, 199, 200, 201, 255] {
            assert_eq!(Operation::try_from(byte), Err(byte), "byte {byte}");
        }
    }

    #[test]
    fn operation_roundtrip() {
        let valid_bytes: &[u8] = &[
            0, 128, 129, 130, 131, 132, 133, 134, 135, 136, 137, 138, 139, 140, 141, 142, 143, 144,
            145, 146, 147, 160, 161,
        ];
        for &byte in valid_bytes {
            let op = Operation::try_from(byte).unwrap();
            assert_eq!(op as u8, byte);
        }
    }
}
