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
use std::ops::Range;

use super::registry::MachineId;

const MAGIC: &[u8; 8] = b"IGGYSMS1";
const FIXED: usize = MAGIC.len() + size_of::<u64>() + size_of::<u32>();
const COUNT_AT: usize = MAGIC.len() + size_of::<u64>();
const MACHINE_FIXED: usize = size_of::<u16>() + size_of::<u32>();

/// Largest snapshot a partition writes or accepts from disk or a peer.
/// Machines hold state about the partition, never user data.
pub const SNAPSHOT_BYTES_MAX: usize = 16 * 1024 * 1024;

/// Every machine's state at one op: the state-machine log's replay point and
/// what state transfer carries.
///
/// Holds its own encoding, which machines write their state straight into,
/// so a checkpoint keeps one serialized copy of the machines. It travels only
/// inside an artifact that checksums its bytes, so it carries no checksum of
/// its own.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StateMachineSnapshot {
    /// Every command at or below this op is applied, none above it.
    op: u64,
    /// `magic | op u64 | count u32 | {id u16, length u32, state}*`.
    encoded: Vec<u8>,
    /// Where each machine's state sits in `encoded`, ordered by id.
    parts: Vec<(MachineId, Range<usize>)>,
}

impl Default for StateMachineSnapshot {
    fn default() -> Self {
        SnapshotWriter::new(0).finish()
    }
}

impl StateMachineSnapshot {
    #[must_use]
    pub const fn op(&self) -> u64 {
        self.op
    }

    /// Each machine's id and state, ordered by id.
    pub fn machines(&self) -> impl Iterator<Item = (MachineId, &[u8])> + '_ {
        self.parts
            .iter()
            .map(|(id, range)| (*id, &self.encoded[range.clone()]))
    }

    #[must_use]
    pub const fn encoded_size(&self) -> usize {
        self.encoded.len()
    }

    /// Append the encoding to `out`.
    pub fn encode_into(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.encoded);
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
        let mut reader = Reader { bytes, at: 0 };
        if reader.array::<{ MAGIC.len() }>()? != *MAGIC {
            return Err(SnapshotDecodeError::Magic);
        }
        let op = u64::from_le_bytes(reader.array()?);
        let count = u32::from_le_bytes(reader.array()?) as usize;
        let mut parts: Vec<(MachineId, Range<usize>)> =
            Vec::with_capacity(count.min(bytes.len() / MACHINE_FIXED));
        for _ in 0..count {
            let id = MachineId(u16::from_le_bytes(reader.array()?));
            if parts.last().is_some_and(|(last, _)| *last >= id) {
                return Err(SnapshotDecodeError::MachineOrder);
            }
            let length = u32::from_le_bytes(reader.array()?) as usize;
            parts.push((id, reader.take(length)?));
        }
        if reader.at != bytes.len() {
            return Err(SnapshotDecodeError::TrailingBytes);
        }
        Ok(Self {
            op,
            encoded: bytes.to_vec(),
            parts,
        })
    }

    /// A snapshot of `parts` at `op`, in the given order.
    #[cfg(test)]
    pub(crate) fn from_parts(op: u64, parts: &[(MachineId, &[u8])]) -> Self {
        let mut writer = SnapshotWriter::new(op);
        for (id, state) in parts {
            writer.machine(*id, |out| out.extend_from_slice(state));
        }
        writer.finish()
    }
}

/// Builds a [`StateMachineSnapshot`] one machine at a time, each writing its
/// state into the snapshot's own buffer.
pub struct SnapshotWriter {
    op: u64,
    encoded: Vec<u8>,
    parts: Vec<(MachineId, Range<usize>)>,
}

impl SnapshotWriter {
    pub fn new(op: u64) -> Self {
        let mut encoded = Vec::with_capacity(FIXED);
        encoded.extend_from_slice(MAGIC);
        encoded.extend_from_slice(&op.to_le_bytes());
        encoded.extend_from_slice(&0u32.to_le_bytes());
        Self {
            op,
            encoded,
            parts: Vec::new(),
        }
    }

    /// Add machine `id`, whose state `write` appends.
    ///
    /// A state past `u32::MAX` bytes records that length saturated, which no
    /// reader decodes: such a snapshot is far past [`SNAPSHOT_BYTES_MAX`],
    /// which every snapshot that leaves memory is checked against.
    pub fn machine(&mut self, id: MachineId, write: impl FnOnce(&mut Vec<u8>)) {
        self.encoded.extend_from_slice(&id.0.to_le_bytes());
        let length_at = self.encoded.len();
        self.encoded.extend_from_slice(&0u32.to_le_bytes());
        let start = self.encoded.len();
        write(&mut self.encoded);
        let length = u32::try_from(self.encoded.len() - start).unwrap_or(u32::MAX);
        self.encoded[length_at..start].copy_from_slice(&length.to_le_bytes());
        self.parts.push((id, start..self.encoded.len()));
    }

    pub fn finish(mut self) -> StateMachineSnapshot {
        let count = u32::try_from(self.parts.len()).unwrap_or(u32::MAX);
        self.encoded[COUNT_AT..FIXED].copy_from_slice(&count.to_le_bytes());
        StateMachineSnapshot {
            op: self.op,
            encoded: self.encoded,
            parts: self.parts,
        }
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Reader<'_> {
    /// The range of the next `length` bytes, consumed.
    fn take(&mut self, length: usize) -> Result<Range<usize>, SnapshotDecodeError> {
        let end = self
            .at
            .checked_add(length)
            .filter(|end| *end <= self.bytes.len())
            .ok_or(SnapshotDecodeError::Truncated)?;
        let range = self.at..end;
        self.at = end;
        Ok(range)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], SnapshotDecodeError> {
        let (taken, _) = self.bytes[self.at..]
            .split_first_chunk::<N>()
            .ok_or(SnapshotDecodeError::Truncated)?;
        self.at += N;
        Ok(*taken)
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
        StateMachineSnapshot::from_parts(42, &[(MachineId(1), b"floor"), (MachineId(7), b"")])
    }

    fn encoded(snapshot: &StateMachineSnapshot) -> Vec<u8> {
        let mut bytes = Vec::new();
        snapshot.encode_into(&mut bytes);
        bytes
    }

    #[test]
    fn given_a_snapshot_when_round_tripping_should_decode_what_was_encoded() {
        let snapshot = sample();
        let bytes = encoded(&snapshot);
        assert_eq!(bytes.len(), snapshot.encoded_size());
        let decoded = StateMachineSnapshot::decode(&bytes).unwrap();
        assert_eq!(decoded, snapshot);
        assert_eq!(decoded.op(), 42);
        assert_eq!(
            decoded.machines().collect::<Vec<_>>(),
            vec![(MachineId(1), &b"floor"[..]), (MachineId(7), &b""[..])]
        );

        let empty = StateMachineSnapshot::default();
        assert_eq!(StateMachineSnapshot::decode(&encoded(&empty)), Ok(empty));
    }

    #[test]
    fn given_truncated_or_garbled_bytes_when_decoding_should_refuse_them() {
        let bytes = encoded(&sample());
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
        let unordered =
            StateMachineSnapshot::from_parts(1, &[(MachineId(2), b""), (MachineId(2), b"")]);
        assert_eq!(
            StateMachineSnapshot::decode(&encoded(&unordered)),
            Err(SnapshotDecodeError::MachineOrder)
        );
    }
}
