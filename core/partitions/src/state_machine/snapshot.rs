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

use std::fmt;

use super::registry::MachineId;

const MAGIC: &[u8; 8] = b"IGGYSMS1";
const FIXED: usize = MAGIC.len() + size_of::<u64>() + size_of::<u32>();
const MACHINE_FIXED: usize = size_of::<u16>() + size_of::<u32>();

/// Largest snapshot a partition writes or accepts from disk or a peer.
/// Machines hold state about the partition, never user data.
pub const SNAPSHOT_BYTES_MAX: usize = 16 * 1024 * 1024;

/// Every machine's state at one op: the state-machine log's replay point and
/// what state transfer carries.
///
/// It travels only inside an artifact that checksums its bytes, so it carries
/// no checksum of its own.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct StateMachineSnapshot {
    /// Every command at or below this op is applied, none above it.
    pub op: u64,
    /// Ordered by id.
    pub machines: Vec<(MachineId, Vec<u8>)>,
}

impl StateMachineSnapshot {
    #[must_use]
    pub fn encoded_size(&self) -> usize {
        FIXED
            + self
                .machines
                .iter()
                .map(|(_, bytes)| MACHINE_FIXED + bytes.len())
                .sum::<usize>()
    }

    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.encoded_size());
        self.encode_into(&mut bytes);
        bytes
    }

    /// Append the encoding to `out`.
    ///
    /// # Panics
    /// When a machine's state exceeds `u32::MAX` bytes or there are more than
    /// `u32::MAX` machines, which a snapshot within [`SNAPSHOT_BYTES_MAX`]
    /// rules out.
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(MAGIC);
        out.extend_from_slice(&self.op.to_le_bytes());
        let count = u32::try_from(self.machines.len()).expect("machine count fits u32");
        out.extend_from_slice(&count.to_le_bytes());
        for (id, state) in &self.machines {
            out.extend_from_slice(&id.0.to_le_bytes());
            let length = u32::try_from(state.len()).expect("machine state fits u32");
            out.extend_from_slice(&length.to_le_bytes());
            out.extend_from_slice(state);
        }
    }

    /// # Errors
    /// Returns an error for bytes that are not a whole snapshot, or that list
    /// machines out of id order.
    pub fn decode(bytes: &[u8]) -> Result<Self, SnapshotDecodeError> {
        if bytes.len() > SNAPSHOT_BYTES_MAX {
            return Err(SnapshotDecodeError::TooLarge {
                length: bytes.len(),
            });
        }
        let mut reader = Reader { bytes };
        if reader.take(MAGIC.len())? != MAGIC {
            return Err(SnapshotDecodeError::Magic);
        }
        let op = reader.u64()?;
        let count = reader.u32()? as usize;
        let mut machines: Vec<(MachineId, Vec<u8>)> =
            Vec::with_capacity(count.min(bytes.len() / MACHINE_FIXED));
        for _ in 0..count {
            let id = MachineId(reader.u16()?);
            if machines.last().is_some_and(|(last, _)| *last >= id) {
                return Err(SnapshotDecodeError::MachineOrder);
            }
            let length = reader.u32()? as usize;
            machines.push((id, reader.take(length)?.to_vec()));
        }
        if !reader.bytes.is_empty() {
            return Err(SnapshotDecodeError::TrailingBytes);
        }
        Ok(Self { op, machines })
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
}

impl<'a> Reader<'a> {
    fn take(&mut self, length: usize) -> Result<&'a [u8], SnapshotDecodeError> {
        let (taken, rest) = self
            .bytes
            .split_at_checked(length)
            .ok_or(SnapshotDecodeError::Truncated)?;
        self.bytes = rest;
        Ok(taken)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], SnapshotDecodeError> {
        self.take(N)?
            .try_into()
            .map_err(|_| SnapshotDecodeError::Truncated)
    }

    fn u16(&mut self) -> Result<u16, SnapshotDecodeError> {
        self.array().map(u16::from_le_bytes)
    }

    fn u32(&mut self) -> Result<u32, SnapshotDecodeError> {
        self.array().map(u32::from_le_bytes)
    }

    fn u64(&mut self) -> Result<u64, SnapshotDecodeError> {
        self.array().map(u64::from_le_bytes)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SnapshotDecodeError {
    TooLarge { length: usize },
    Truncated,
    Magic,
    MachineOrder,
    TrailingBytes,
}

impl fmt::Display for SnapshotDecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooLarge { length } => write!(
                f,
                "state machine snapshot of {length} bytes exceeds {SNAPSHOT_BYTES_MAX}"
            ),
            Self::Truncated => f.write_str("state machine snapshot is truncated"),
            Self::Magic => f.write_str("not a state machine snapshot"),
            Self::MachineOrder => f.write_str("state machine snapshot lists machines out of order"),
            Self::TrailingBytes => f.write_str("state machine snapshot has trailing bytes"),
        }
    }
}

impl std::error::Error for SnapshotDecodeError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> StateMachineSnapshot {
        StateMachineSnapshot {
            op: 42,
            machines: vec![
                (MachineId(1), b"floor".to_vec()),
                (MachineId(7), Vec::new()),
            ],
        }
    }

    #[test]
    fn given_a_snapshot_when_round_tripping_should_decode_what_was_encoded() {
        let snapshot = sample();
        let bytes = snapshot.encode();
        assert_eq!(bytes.len(), snapshot.encoded_size());
        assert_eq!(StateMachineSnapshot::decode(&bytes), Ok(snapshot));

        let empty = StateMachineSnapshot::default();
        assert_eq!(StateMachineSnapshot::decode(&empty.encode()), Ok(empty));
    }

    #[test]
    fn given_truncated_or_garbled_bytes_when_decoding_should_refuse_them() {
        let bytes = sample().encode();
        for length in 0..bytes.len() {
            assert!(StateMachineSnapshot::decode(&bytes[..length]).is_err());
        }
        let mut garbled = bytes.clone();
        garbled[0] ^= 1;
        assert_eq!(
            StateMachineSnapshot::decode(&garbled),
            Err(SnapshotDecodeError::Magic)
        );
        let mut padded = bytes;
        padded.push(0);
        assert_eq!(
            StateMachineSnapshot::decode(&padded),
            Err(SnapshotDecodeError::TrailingBytes)
        );
    }

    #[test]
    fn given_unordered_machines_when_decoding_should_refuse_them() {
        let unordered = StateMachineSnapshot {
            op: 1,
            machines: vec![(MachineId(2), Vec::new()), (MachineId(2), Vec::new())],
        };
        assert_eq!(
            StateMachineSnapshot::decode(&unordered.encode()),
            Err(SnapshotDecodeError::MachineOrder)
        );
    }
}
