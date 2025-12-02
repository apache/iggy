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

pub struct Header {}

pub trait ConsensusHeader: Sized + Pod + Zeroable {
    const COMMAND: Command2;

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
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum ConsensusError {
    #[error("invalid command: expected {expected:?}, found {found:?}")]
    InvalidCommand { expected: Command2, found: Command2 },

    #[error("invalid checksum")]
    InvalidChecksum,

    #[error("invalid cluster ID")]
    InvalidCluster,

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
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct GenericHeader {
    pub checksum: u128,
    pub checksum_body: u128,
    pub cluster: u128,
    pub size: u32,
    pub epoch: u32,
    pub view: u32,
    pub release: u32,
    pub protocol: u16,
    pub command: Command2,
    pub replica: u8,
    pub reserved_frame: [u8; 12],

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
    pub epoch: u32,
    pub view: u32,
    pub release: u32,
    pub protocol: u16,
    pub command: Command2,
    pub replica: u8,
    pub reserved_frame: [u8; 12],

    pub request_checksum: u128,
    pub timestamp: u64,
    pub request: u64,
    pub operation: Operation,
    pub reserved: [u8; 95],
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
#[derive(Default, Debug, Clone, Copy)]
pub struct PrepareHeader {
    pub checksum: u128,
    pub checksum_body: u128,
    pub cluster: u128,
    pub size: u32,
    pub epoch: u32,
    pub view: u32,
    pub release: u32,
    pub protocol: u16,
    pub command: Command2,
    pub replica: u8,
    pub reserved_frame: [u8; 12],

    pub parent: u128,
    pub parent_padding: u128,
    pub request_checksum: u128,
    pub request_checksum_padding: u128,
    pub op: u64,
    pub commit: u64,
    pub timestamp: u64,
    pub request: u64,
    pub operation: Operation,
    pub reserved: [u8; 19],
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
        if self.parent_padding != 0 {
            return Err(ConsensusError::PrepareParentPaddingNonZero);
        }
        if self.request_checksum_padding != 0 {
            return Err(ConsensusError::PrepareRequestChecksumPaddingNonZero);
        }
        Ok(())
    }

    fn size(&self) -> u32 {
        self.size
    }
}

// TODO: Manually impl default (and use a const for the `release`)
#[repr(C)]
#[derive(Default, Debug, Clone, Copy)]
pub struct PrepareOkHeader {
    pub checksum: u128,
    pub checksum_body: u128,
    pub cluster: u128,
    pub size: u32,
    pub epoch: u32,
    pub view: u32,
    pub release: u32,
    pub protocol: u16,
    pub command: Command2,
    pub replica: u8,
    pub reserved_frame: [u8; 12],

    pub parent: u128,
    pub parent_padding: u128,
    pub prepare_checksum: u128,
    pub prepare_checksum_padding: u128,
    pub op: u64,
    pub commit: u64,
    pub timestamp: u64,
    pub request: u64,
    pub operation: Operation,
    pub reserved: [u8; 19],
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
        if self.parent_padding != 0 {
            return Err(ConsensusError::PrepareParentPaddingNonZero);
        }
        if self.prepare_checksum_padding != 0 {
            return Err(ConsensusError::PrepareRequestChecksumPaddingNonZero);
        }
        Ok(())
    }

    fn size(&self) -> u32 {
        self.size
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct CommitHeader {
    pub checksum: u128,
    pub checksum_body: u128,
    pub cluster: u128,
    pub size: u32,
    pub epoch: u32,
    pub view: u32,
    pub release: u32,
    pub protocol: u16,
    pub command: Command2,
    pub replica: u8,
    pub reserved_frame: [u8; 12],

    pub commit_checksum: u128,
    pub timestamp_monotonic: u64,
    pub commit: u64,
    pub checkpoint_op: u64,
    pub reserved: [u8; 96],
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
    pub epoch: u32,
    pub view: u32,
    pub release: u32,
    pub protocol: u16,
    pub command: Command2,
    pub replica: u8,
    pub reserved_frame: [u8; 12],

    pub request_checksum: u128,
    pub request_checksum_padding: u128,
    pub context: u128,
    pub context_padding: u128,
    pub op: u64,
    pub commit: u64,
    pub timestamp: u64,
    pub request: u64,
    pub operation: Operation,
    pub reserved: [u8; 19],
}

unsafe impl Pod for ReplyHeader {}
unsafe impl Zeroable for ReplyHeader {}

impl ConsensusHeader for ReplyHeader {
    const COMMAND: Command2 = Command2::Reply;

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command2::Reply {
            return Err(ConsensusError::ReplyInvalidCommand2);
        }
        if self.request_checksum_padding != 0 {
            return Err(ConsensusError::ReplyRequestChecksumPaddingNonZero);
        }
        if self.context_padding != 0 {
            return Err(ConsensusError::ReplyContextPaddingNonZero);
        }
        Ok(())
    }

    fn size(&self) -> u32 {
        self.size
    }
}
