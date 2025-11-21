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

#[expect(unused)]
pub struct Header {}

pub trait ConsensusHeader: Sized + Pod + Zeroable {
    const COMMAND: Command;

    fn size(&self) -> u32;
    fn command(&self) -> Command;
    fn checksum(&self) -> u128;
    fn cluster(&self) -> u128;

    fn validate(&self) -> Result<(), &'static str>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
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

    RequstHeads = 11,
    RequestPrepare = 12,
    RequestReply = 13,

    Headers = 14,

    Eviction = 15,

    RequestBlocks = 16,
    Block = 17,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
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

    fn size(&self) -> u32 {
        self.size
    }
    fn command(&self) -> Command {
        self.command
    }
    fn checksum(&self) -> u128 {
        self.checksum
    }
    fn cluster(&self) -> u128 {
        self.cluster
    }

    fn validate(&self) -> Result<(), &'static str> {
        Ok(())
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

    fn size(&self) -> u32 {
        self.size
    }
    fn command(&self) -> Command {
        self.command
    }
    fn checksum(&self) -> u128 {
        self.checksum
    }
    fn cluster(&self) -> u128 {
        self.cluster
    }

    fn validate(&self) -> Result<(), &'static str> {
        if self.command != Command::Prepare {
            return Err("command must be Prepare");
        }
        if self.parent_padding != 0 {
            return Err("parent_padding must be 0");
        }
        if self.request_checksum_padding != 0 {
            return Err("request_checksum_padding must be 0");
        }
        Ok(())
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

    fn size(&self) -> u32 {
        self.size
    }
    fn command(&self) -> Command {
        self.command
    }
    fn checksum(&self) -> u128 {
        self.checksum
    }
    fn cluster(&self) -> u128 {
        self.cluster
    }

    fn validate(&self) -> Result<(), &'static str> {
        if self.command != Command::Commit {
            return Err("command != Commit");
        }
        if self.size != 256 {
            return Err("size != 256");
        }
        Ok(())
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

    fn size(&self) -> u32 {
        self.size
    }
    fn command(&self) -> Command {
        self.command
    }
    fn checksum(&self) -> u128 {
        self.checksum
    }
    fn cluster(&self) -> u128 {
        self.cluster
    }

    fn validate(&self) -> Result<(), &'static str> {
        if self.command != Command::Reply {
            return Err("command != Reply");
        }
        Ok(())
    }
}
