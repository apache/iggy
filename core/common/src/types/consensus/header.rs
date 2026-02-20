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

use bytemuck::{Pod, Zeroable};
use thiserror::Error;

const HEADER_SIZE: usize = 256;
pub trait ConsensusHeader: Sized + Pod + Zeroable {
    const COMMAND: Command2;
    // TODO: Trait consts are never evaluated unless explicitly accessed (e.g. `<T as ConsensusHeader>::_SIZE_CHECK`).
    // The size invariant is enforced by repr(C) layout + bytemuck Pod derive; consider adding a static_assert in each impl.
    const _SIZE_CHECK: () = assert!(std::mem::size_of::<Self>() == HEADER_SIZE);

    fn validate(&self) -> Result<(), ConsensusError>;
    fn size(&self) -> u32;
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
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

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum ConsensusError {
    #[error("invalid command: expected {expected:?}, found {found:?}")]
    InvalidCommand { expected: Command2, found: Command2 },

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
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Operation {
    #[default]
    Default = 0,
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

    Reserved = 200,
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
    pub command: Command2,
    pub replica: u8,
    pub reserved_frame: [u8; 66],

    pub reserved_command: [u8; 128],
}

unsafe impl Pod for GenericHeader {}
unsafe impl Zeroable for GenericHeader {}

impl ConsensusHeader for GenericHeader {
    const COMMAND: Command2 = Command2::Reserved;

    fn validate(&self) -> Result<(), ConsensusError> {
        Ok(())
    }

    fn size(&self) -> u32 {
        self.size
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
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

impl Default for RequestHeader {
    fn default() -> Self {
        Self {
            checksum: 0,
            checksum_body: 0,
            cluster: 0,
            size: 0,
            view: 0,
            release: 0,
            command: Default::default(),
            replica: 0,
            reserved_frame: [0; 66],
            client: 0,
            request_checksum: 0,
            timestamp: 0,
            request: 0,
            operation: Default::default(),
            operation_padding: [0; 7],
            namespace: 0,
            reserved: [0; 64],
        }
    }
}

unsafe impl Pod for RequestHeader {}
unsafe impl Zeroable for RequestHeader {}

impl ConsensusHeader for RequestHeader {
    const COMMAND: Command2 = Command2::Request;

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command2::Request {
            return Err(ConsensusError::InvalidCommand {
                expected: Command2::Request,
                found: self.command,
            });
        }
        Ok(())
    }

    fn size(&self) -> u32 {
        self.size
    }
}

// TODO: Manually impl default (and use a const for the `release`)
#[repr(C)]
#[derive(Debug, Clone, Copy)]
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

unsafe impl Pod for PrepareHeader {}
unsafe impl Zeroable for PrepareHeader {}

impl ConsensusHeader for PrepareHeader {
    const COMMAND: Command2 = Command2::Prepare;

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command2::Prepare {
            return Err(ConsensusError::InvalidCommand {
                expected: Command2::Prepare,
                found: self.command,
            });
        }
        Ok(())
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
            command: Default::default(),
            replica: 0,
            reserved_frame: [0; 66],
            client: 0,
            parent: 0,
            request_checksum: 0,
            op: 0,
            commit: 0,
            timestamp: 0,
            request: 0,
            operation: Default::default(),
            operation_padding: [0; 7],
            namespace: 0,
            reserved: [0; 32],
        }
    }
}

// TODO: Manually impl default (and use a const for the `release`)
#[repr(C)]
#[derive(Debug, Clone, Copy)]
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

unsafe impl Pod for PrepareOkHeader {}
unsafe impl Zeroable for PrepareOkHeader {}

impl ConsensusHeader for PrepareOkHeader {
    const COMMAND: Command2 = Command2::PrepareOk;

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
            command: Default::default(),
            replica: 0,
            reserved_frame: [0; 66],
            parent: 0,
            prepare_checksum: 0,
            op: 0,
            commit: 0,
            timestamp: 0,
            request: 0,
            operation: Default::default(),
            operation_padding: [0; 7],
            namespace: 0,
            reserved: [0; 48],
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
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

unsafe impl Pod for CommitHeader {}
unsafe impl Zeroable for CommitHeader {}

impl ConsensusHeader for CommitHeader {
    const COMMAND: Command2 = Command2::Commit;

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
#[derive(Debug, Clone, Copy)]
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
    pub reserved: [u8; 41],
}

unsafe impl Pod for ReplyHeader {}
unsafe impl Zeroable for ReplyHeader {}

impl ConsensusHeader for ReplyHeader {
    const COMMAND: Command2 = Command2::Reply;

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
            command: Default::default(),
            replica: 0,
            reserved_frame: [0; 66],
            request_checksum: 0,
            context: 0,
            op: 0,
            commit: 0,
            timestamp: 0,
            request: 0,
            operation: Default::default(),
            operation_padding: [0; 7],
            namespace: 0,
            reserved: [0; 41],
        }
    }
}

/// StartViewChange message header.
///
/// Sent by a replica when it suspects the primary has failed.
/// This is a header-only message with no body.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

unsafe impl Pod for StartViewChangeHeader {}
unsafe impl Zeroable for StartViewChangeHeader {}

impl ConsensusHeader for StartViewChangeHeader {
    const COMMAND: Command2 = Command2::StartViewChange;

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

unsafe impl Pod for DoViewChangeHeader {}
unsafe impl Zeroable for DoViewChangeHeader {}

impl ConsensusHeader for DoViewChangeHeader {
    const COMMAND: Command2 = Command2::DoViewChange;

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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

unsafe impl Pod for StartViewHeader {}
unsafe impl Zeroable for StartViewHeader {}

impl ConsensusHeader for StartViewHeader {
    const COMMAND: Command2 = Command2::StartView;

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
