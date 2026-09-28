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
use crate::codec::{read_u64_le, read_u128_le};
use crate::{WireDecode, WireEncode, WireError};
use bytemuck::{CheckedBitPattern, NoUninit};
use bytes::{BufMut, BytesMut};

pub const MAX_CONSUMER_SESSIONS_PER_HEARTBEAT: usize = 1024;

/// A transport-bound session observed by a replica, independent of client pings.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ConsumerSession {
    pub client_id: u128,
    pub session: u64,
}

impl ConsumerSession {
    pub const ENCODED_SIZE: usize = size_of::<u128>() + size_of::<u64>();
}

impl WireEncode for ConsumerSession {
    fn encoded_size(&self) -> usize {
        Self::ENCODED_SIZE
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u128_le(self.client_id);
        buf.put_u64_le(self.session);
    }
}

impl WireDecode for ConsumerSession {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let client_id = read_u128_le(buf, 0)?;
        let session = read_u64_le(buf, size_of::<u128>())?;
        if client_id == 0 || session == 0 {
            return Err(WireError::Validation(
                "consumer session identity must be nonzero".into(),
            ));
        }
        Ok((Self { client_id, session }, Self::ENCODED_SIZE))
    }
}

/// Same-release replica control frame with bounded, packed [`ConsumerSession`] entries.
///
/// A heartbeat renews only the named epochs;
/// absence from a batch does not authorize eviction. An incomplete gather
/// defers expiry, including when no sessions could be collected.
#[derive(Debug, Clone, Copy, PartialEq, Eq, CheckedBitPattern, NoUninit)]
#[repr(C)]
pub struct ConsumerSessionHeartbeatHeader {
    pub checksum: u128,
    pub checksum_body: u128,
    pub cluster: u128,
    pub size: u32,
    pub view: u32,
    pub release: u32,
    pub command: Command,
    pub replica: u8,
    pub reserved_frame: [u8; 66],
    /// 1 if any local shard failed to report its clients, otherwise 0.
    pub incomplete: u8,
    pub reserved: [u8; 127],
}

const _: () = assert!(size_of::<ConsumerSessionHeartbeatHeader>() == HEADER_SIZE);

impl ConsensusHeader for ConsumerSessionHeartbeatHeader {
    const FRAME_SEALED: bool = true;
    const COMMAND: Command = Command::ConsumerSessionHeartbeat;

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

    fn validate(&self) -> Result<(), ConsensusError> {
        if self.command != Self::COMMAND {
            return Err(ConsensusError::InvalidCommand {
                expected: Self::COMMAND,
                found: self.command,
            });
        }
        let body_size = (self.size as usize).saturating_sub(HEADER_SIZE);
        if (self.size as usize) < HEADER_SIZE {
            return Err(ConsensusError::InvalidField(
                "consumer session heartbeat size is smaller than its header".into(),
            ));
        }
        if body_size > MAX_CONSUMER_SESSIONS_PER_HEARTBEAT * ConsumerSession::ENCODED_SIZE {
            return Err(ConsensusError::InvalidField(
                "consumer session heartbeat exceeds the batch limit".into(),
            ));
        }
        if !body_size.is_multiple_of(ConsumerSession::ENCODED_SIZE) {
            return Err(ConsensusError::InvalidField(
                "consumer session heartbeat contains a truncated session".into(),
            ));
        }
        if self.incomplete > 1 {
            return Err(ConsensusError::InvalidField(
                "consumer session heartbeat incomplete flag must be 0 or 1".into(),
            ));
        }
        if self.release != 0 {
            return Err(ConsensusError::InvalidField(
                "consumer session heartbeat release must be zero".into(),
            ));
        }
        if self.reserved_frame != [0; 66] || self.reserved != [0; 127] {
            return Err(ConsensusError::InvalidField(
                "consumer session heartbeat reserved bytes must be zero".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_round_trip_and_truncation() {
        let session = ConsumerSession {
            client_id: u128::MAX,
            session: u64::MAX,
        };
        let encoded = session.to_bytes();
        assert_eq!(encoded.len(), ConsumerSession::ENCODED_SIZE);
        assert_eq!(
            ConsumerSession::decode(&encoded).unwrap(),
            (session, encoded.len())
        );
        for length in 0..encoded.len() {
            assert!(ConsumerSession::decode(&encoded[..length]).is_err());
        }
        assert!(ConsumerSession::decode(&[0; ConsumerSession::ENCODED_SIZE]).is_err());
    }

    #[test]
    fn heartbeat_rejects_unbounded_or_partial_batches() {
        let mut header = ConsumerSessionHeartbeatHeader {
            checksum: 0,
            checksum_body: 0,
            cluster: 1,
            size: 0,
            view: 0,
            release: 0,
            command: Command::ConsumerSessionHeartbeat,
            replica: 0,
            reserved_frame: [0; 66],
            incomplete: 0,
            reserved: [0; 127],
        };
        for count in [0, 1, MAX_CONSUMER_SESSIONS_PER_HEARTBEAT] {
            header.size =
                u32::try_from(HEADER_SIZE + count * ConsumerSession::ENCODED_SIZE).unwrap();
            assert!(header.validate().is_ok());
            header.seal();
            assert!(header.verify_frame().is_ok());
        }
        for size in [
            0,
            HEADER_SIZE + 1,
            HEADER_SIZE + (MAX_CONSUMER_SESSIONS_PER_HEARTBEAT + 1) * ConsumerSession::ENCODED_SIZE,
        ] {
            header.size = u32::try_from(size).unwrap();
            assert!(header.validate().is_err());
        }
        header.size = u32::try_from(HEADER_SIZE).unwrap();
        header.incomplete = 1;
        assert!(header.validate().is_ok());
        header.incomplete = 2;
        assert!(header.validate().is_err());
        header.incomplete = 0;
        header.release = 1;
        assert!(header.validate().is_err());
        header.release = 0;
        header.reserved[0] = 1;
        assert!(header.validate().is_err());
        header.reserved[0] = 0;
        header.reserved_frame[0] = 1;
        assert!(header.validate().is_err());
    }
}
