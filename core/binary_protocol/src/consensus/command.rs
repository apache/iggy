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

use bytemuck::{CheckedBitPattern, NoUninit};
use enumset::EnumSetType;

/// VSR message type discriminant.
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
    Eviction = 13,

    // Replica-to-replica auth handshake (server-ng consensus plane).
    ReplicaHello = 14,
    ReplicaChallenge = 15,
    ReplicaFinish = 16,

    // Replica recovery: a restarted replica asks for the current view's
    // `StartView` instead of trusting stale local state; only that view's
    // primary answers (with a targeted `StartView`).
    RequestStartView = 17,

    // Journal repair: fetch committed prepares a replica is missing (rejoin
    // window or interior hole). `RepairPrepare` carries a journaled prepare
    // verbatim; `RangeEvicted` is the honest answer when the serving peer no
    // longer retains the front of the range.
    RequestPrepares = 18,
    RepairPrepare = 19,
    RepairDone = 20,
    RangeEvicted = 21,
}

// SAFETY: Command2 is #[repr(u8)] with no padding bytes.
unsafe impl NoUninit for Command2 {}

// SAFETY: Command2 is #[repr(u8)]; is_valid_bit_pattern matches all defined discriminants.
unsafe impl CheckedBitPattern for Command2 {
    type Bits = u8;

    fn is_valid_bit_pattern(bits: &u8) -> bool {
        *bits <= Self::RangeEvicted as u8
    }
}

#[cfg(test)]
mod tests {
    use crate::consensus::GenericHeader;
    use aligned_vec::{AVec, ConstAlign};

    #[test]
    fn invalid_bit_pattern_rejected() {
        // 16-byte aligned (see `ConsensusHeader` doc); `BytesMut` fails Miri.
        let mut buf: AVec<u8, ConstAlign<16>> = AVec::new(16);
        buf.resize(256, 0);
        buf[60] = 99;
        let result = bytemuck::checked::try_from_bytes::<GenericHeader>(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn replica_auth_commands_are_valid_bit_patterns() {
        // Locks the is_valid_bit_pattern bump: 14..=21 parse, 22 still rejects.
        for command in 14u8..=21 {
            let mut buf: AVec<u8, ConstAlign<16>> = AVec::new(16);
            buf.resize(256, 0);
            buf[60] = command;
            assert!(bytemuck::checked::try_from_bytes::<GenericHeader>(&buf).is_ok());
        }
        let mut buf: AVec<u8, ConstAlign<16>> = AVec::new(16);
        buf.resize(256, 0);
        buf[60] = 22;
        assert!(bytemuck::checked::try_from_bytes::<GenericHeader>(&buf).is_err());
    }
}
