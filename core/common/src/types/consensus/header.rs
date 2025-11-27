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

#[expect(unused)]
pub struct Header {}

pub trait ConsensusHeader: Sized + Pod + Zeroable {
    const COMMAND: Command;

    fn validate(&self) -> Result<(), ConsensusError>;
    fn size(&self) -> u32;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[expect(unused)]
pub enum Command {
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
#[expect(unused)]
pub enum ConsensusError {
    #[error("invalid command: expected {expected:?}, found {found:?}")]
    InvalidCommand { expected: Command, found: Command },

    #[error("invalid checksum")]
    InvalidChecksum,

    #[error("invalid cluster ID")]
    InvalidCluster,

    #[error("parent_padding must be 0")]
    PrepareParentPaddingNonZero,

    #[error("request_checksum_padding must be 0")]
    PrepareRequestChecksumPaddingNonZero,

    #[error("command must be Commit")]
    CommitInvalidCommand,

    #[error("size must be 256, found {0}")]
    CommitInvalidSize(u32),

    // ReplyHeader specific
    #[error("command must be Reply")]
    ReplyInvalidCommand,

    #[error("request_checksum_padding must be 0")]
    ReplyRequestChecksumPaddingNonZero,

    #[error("context_padding must be 0")]
    ReplyContextPaddingNonZero,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[expect(unused)]
pub enum Operation {
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
    pub command: Command,
    pub replica: u8,
    pub reserved_frame: [u8; 12],

    pub reserved_command: [u8; 128],
}

unsafe impl Pod for GenericHeader {}
unsafe impl Zeroable for GenericHeader {}

impl ConsensusHeader for GenericHeader {
    const COMMAND: Command = Command::Reserved;

    fn validate(&self) -> Result<(), ConsensusError> {
        Ok(())
    }

    fn size(&self) -> u32 {
        self.size
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct PrepareHeader {
    pub checksum: u128,
    pub checksum_body: u128,
    pub cluster: u128,
    pub size: u32,
    pub epoch: u32,
    pub view: u32,
    pub release: u32,
    pub protocol: u16,
    pub command: Command,
    pub replica: u8,
    pub reserved_frame: [u8; 12],

    pub parent: u128,
    pub parent_padding: u128,
    pub request_checksum: u128,
    pub request_checksum_padding: u128,
    pub checkpoint_id: u128,
    pub op: u64,
    pub commit: u64,
    pub timestamp: u64,
    pub request: u64,
    pub operation: Operation,
    pub reserved: [u8; 3],
}

unsafe impl Pod for PrepareHeader {}
unsafe impl Zeroable for PrepareHeader {}

impl ConsensusHeader for PrepareHeader {
    const COMMAND: Command = Command::Prepare;

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command::Prepare {
            return Err(ConsensusError::InvalidCommand {
                expected: Command::Prepare,
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
    pub command: Command,
    pub replica: u8,
    pub reserved_frame: [u8; 12],

    pub commit_checksum: u128,
    pub timestamp_monotonic: u64,
    pub checkpoint_id: u128,
    pub commit: u64,
    pub checkpoint_op: u64,
    pub reserved: [u8; 80],
}

unsafe impl Pod for CommitHeader {}
unsafe impl Zeroable for CommitHeader {}

impl ConsensusHeader for CommitHeader {
    const COMMAND: Command = Command::Commit;

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command::Commit {
            return Err(ConsensusError::CommitInvalidCommand);
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
    pub command: Command,
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
    const COMMAND: Command = Command::Reply;

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Command::Reply {
            return Err(ConsensusError::ReplyInvalidCommand);
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
