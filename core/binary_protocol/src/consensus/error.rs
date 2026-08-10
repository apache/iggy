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

use super::command::Command2;
use thiserror::Error;

#[derive(Debug, Clone, Error, PartialEq, Eq)]
pub enum ConsensusError {
    #[error("invalid command: expected {expected:?}, found {found:?}")]
    InvalidCommand { expected: Command2, found: Command2 },

    #[error("invalid size: expected {expected:?}, found {found:?}")]
    InvalidSize { expected: u32, found: u32 },

    #[error("invalid checksum")]
    InvalidChecksum,

    #[error(
        "{command:?}: header checksum {found:#034x} does not cover the frame (expected \
         {expected:#034x}){}",
        if *found == 0 {
            ". A zeroed checksum is the signature of a peer predating the frame seal, \
             which is a hard version break: replicas must be upgraded together, with the \
             cluster down"
        } else {
            ""
        }
    )]
    FrameChecksumMismatch {
        command: Command2,
        expected: u128,
        found: u128,
    },

    #[error("invalid cluster ID")]
    InvalidCluster,

    #[error("invalid field: {0}")]
    InvalidField(String),

    // -- Reserved for future consensus validation --
    // These variants exist in the original iggy_common implementation
    // and will be used when the consensus crate migrates to this crate.
    #[error("parent_padding must be 0")]
    PrepareParentPaddingNonZero,

    #[error("request_checksum_padding must be 0")]
    PrepareRequestChecksumPaddingNonZero,

    #[error("command must be Commit")]
    CommitInvalidCommand2,

    #[error("size must be 256, found {0}")]
    CommitInvalidSize(u32),

    #[error("command must be Reply")]
    ReplyInvalidCommand2,

    #[error("request_checksum_padding must be 0")]
    ReplyRequestChecksumPaddingNonZero,

    #[error("context_padding must be 0")]
    ReplyContextPaddingNonZero,

    #[error("invalid bit pattern in header (enum discriminant out of range)")]
    InvalidBitPattern,

    #[error("client-bound command {0:?} cannot be dispatched on inbound path")]
    ClientBoundCommand(Command2),
}
