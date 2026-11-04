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

use super::{Command, ConsensusError, ConsensusHeader, HEADER_SIZE, Operation};
use bytemuck::{CheckedBitPattern, NoUninit};

#[derive(Debug, Clone, Copy, PartialEq, Eq, CheckedBitPattern, NoUninit)]
#[repr(u8)]
pub enum SegmentDeleteKind {
    Query = 0,
    Resolved = 1,
    Retry = 2,
}

/// Resolve a segment count on the partition primary before metadata commits
/// the resulting truncation.
///
/// Replies echo the nonce and history fields;
/// ingress authenticates the replica and the requester correlates the reply.
#[derive(Debug, Clone, Copy, PartialEq, Eq, CheckedBitPattern, NoUninit)]
#[repr(C)]
pub struct SegmentDeleteHeader {
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
    pub namespace: u64,
    pub created_revision: u64,
    pub minimum_metadata_op: u64,
    pub up_to_offset: u64,
    pub segments_count: u32,
    pub kind: SegmentDeleteKind,
    pub reserved: [u8; 75],
}

const _: () = assert!(size_of::<SegmentDeleteHeader>() == HEADER_SIZE);

impl ConsensusHeader for SegmentDeleteHeader {
    const FRAME_SEALED: bool = true;
    const COMMAND: Command = Command::SegmentDelete;

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

    #[expect(clippy::cast_possible_truncation, reason = "HEADER_SIZE is 256 bytes")]
    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Self::COMMAND {
            return Err(ConsensusError::InvalidCommand {
                expected: Self::COMMAND,
                found: self.command,
            });
        }
        if self.size as usize != HEADER_SIZE {
            return Err(ConsensusError::InvalidSize {
                expected: HEADER_SIZE as u32,
                found: self.size,
            });
        }
        if self.checksum_body != 0
            || self.release != 0
            || self.reserved_frame != [0; 66]
            || self.reserved != [0; 75]
        {
            return Err(ConsensusError::InvalidField(
                "segment delete body checksum, release and reserved bytes must be zero".into(),
            ));
        }
        if self.nonce == 0 || self.created_revision == 0 {
            return Err(ConsensusError::InvalidField(
                "segment delete nonce and revision must be nonzero".into(),
            ));
        }
        if self.kind != SegmentDeleteKind::Resolved && self.up_to_offset != 0 {
            return Err(ConsensusError::InvalidField(
                "only a resolved segment delete can carry an offset".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::offset_of;

    fn header() -> SegmentDeleteHeader {
        SegmentDeleteHeader {
            checksum: 0,
            checksum_body: 0,
            cluster: 7,
            size: u32::try_from(HEADER_SIZE).unwrap(),
            view: 0,
            release: 0,
            command: Command::SegmentDelete,
            replica: 2,
            reserved_frame: [0; 66],
            nonce: 19,
            namespace: 23,
            created_revision: 31,
            minimum_metadata_op: 41,
            up_to_offset: 0,
            segments_count: 3,
            kind: SegmentDeleteKind::Query,
            reserved: [0; 75],
        }
    }

    #[test]
    fn given_segment_delete_exchange_when_encoded_should_preserve_history_and_seal() {
        assert_eq!(offset_of!(SegmentDeleteHeader, nonce), 128);
        for kind in [
            SegmentDeleteKind::Query,
            SegmentDeleteKind::Resolved,
            SegmentDeleteKind::Retry,
        ] {
            let mut original = header();
            original.kind = kind;
            original.up_to_offset = if kind == SegmentDeleteKind::Resolved {
                51
            } else {
                0
            };
            original.seal();
            let decoded = bytemuck::checked::try_from_bytes::<SegmentDeleteHeader>(
                bytemuck::bytes_of(&original),
            )
            .unwrap();
            assert_eq!(*decoded, original);
            decoded.validate().unwrap();
            decoded.verify_frame().unwrap();
            let mut corrupted = original;
            corrupted.created_revision += 1;
            assert!(corrupted.verify_frame().is_err());
        }
    }

    #[test]
    fn given_segment_delete_exchange_when_invalid_should_reject_before_use() {
        for change in [
            |value: &mut SegmentDeleteHeader| value.command = Command::Commit,
            |value: &mut SegmentDeleteHeader| value.size += 1,
            |value: &mut SegmentDeleteHeader| value.checksum_body = 1,
            |value: &mut SegmentDeleteHeader| value.release = 1,
            |value: &mut SegmentDeleteHeader| value.reserved_frame[0] = 1,
            |value: &mut SegmentDeleteHeader| value.reserved[0] = 1,
            |value: &mut SegmentDeleteHeader| value.nonce = 0,
            |value: &mut SegmentDeleteHeader| value.created_revision = 0,
            |value: &mut SegmentDeleteHeader| value.up_to_offset = 1,
        ] {
            let mut invalid = header();
            change(&mut invalid);
            assert!(invalid.validate().is_err(), "{invalid:?}");
        }
    }
}
