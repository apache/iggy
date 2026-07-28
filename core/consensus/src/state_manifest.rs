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

//! State-transfer manifest: the artifact list a serving peer offers, carried
//! in the `StateTransferTarget` body.
//!
//! `TigerBeetle` pulls state as content-addressed grid blocks, so a bare
//! `(address, checksum)` names any piece of state and one repair protocol
//! covers manifest, free set, client sessions, and LSM tables alike. Iggy's
//! artifacts (metadata snapshot, client table, partition segments) are not
//! content-addressed, so the manifest supplies the addressing instead: each
//! entry names one artifact, `(manifest index, byte offset)` is the chunk
//! address the `RequestStateChunk`/`StateChunk` frames pull by, and the
//! per-artifact checksum is the integrity stamp (chunks carry none).
//!
//! Plane-agnostic by construction: the metadata plane ships two entries
//! (snapshot + client table); the partition plane ships N (segment logs,
//! consumer offsets, ...) through the same frames by appending [`kind`]
//! values. Entries carry their own length on the wire (`entry_len`), so new
//! per-entry fields can be appended without breaking old decoders.
//!
//! [`kind`]: StateArtifact::kind

use std::hash::Hasher;
use twox_hash::XxHash3_64;

/// Artifact kinds. Wire-pinned: never reorder or reuse.
pub mod artifact_kind {
    /// Metadata plane: `snapshot.bin` bytes verbatim.
    pub const METADATA_SNAPSHOT: u8 = 0;
    /// Metadata plane: [`crate::ClientTable::encode`] bytes.
    pub const CLIENT_TABLE: u8 = 1;
    // Partition plane (reserved, not served yet):
    // SEGMENT_LOG = 2, CONSUMER_OFFSETS = 3.
}

/// One artifact a serving peer offers: what it is, where its receiver-side
/// watermark sits, and how to verify the assembled bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StateArtifact {
    /// [`artifact_kind`] value. Unknown kinds decode fine (forward compat);
    /// the receiving plane decides whether it can install them.
    pub kind: u8,
    /// Kind-specific watermark: the snapshot's `sequence_number`, the client
    /// table's mutation frontier, a segment's base offset, ...
    pub frontier: u64,
    /// Artifact byte length; `(manifest index, offset in 0..len)` addresses
    /// every chunk of it.
    pub len: u64,
    /// `XxHash3_64` over the artifact bytes (artifact-level integrity;
    /// chunks carry none).
    pub checksum: u64,
}

impl StateArtifact {
    /// Entry for `bytes`, stamping length + checksum.
    #[must_use]
    pub fn for_bytes(kind: u8, frontier: u64, bytes: &[u8]) -> Self {
        Self {
            kind,
            frontier,
            len: bytes.len() as u64,
            checksum: state_artifact_checksum(bytes),
        }
    }
}

/// Artifact-level integrity stamp (manifest `checksum` field).
#[must_use]
pub fn state_artifact_checksum(bytes: &[u8]) -> u64 {
    let mut hasher = XxHash3_64::new();
    hasher.write(bytes);
    hasher.finish()
}

/// Failure decoding an encoded manifest.
#[derive(Debug)]
pub enum StateManifestError {
    /// Byte stream ended mid-field.
    Truncated,
    /// Leading magic is not [`STATE_MANIFEST_MAGIC`].
    BadMagic,
    /// Trailing hash does not match the content.
    ChecksumMismatch { expected: u64, actual: u64 },
    /// Entry count exceeds [`STATE_MANIFEST_ENTRIES_MAX`].
    TooManyEntries { count: u32 },
    /// Declared per-entry length is below this version's field set (a
    /// FUTURE-shrunk entry; longer entries are fine, the tail is skipped).
    EntryTooShort { entry_len: u8 },
}

impl std::fmt::Display for StateManifestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncated => write!(f, "state manifest truncated"),
            Self::BadMagic => write!(f, "state manifest has wrong magic"),
            Self::ChecksumMismatch { expected, actual } => write!(
                f,
                "state manifest checksum mismatch: expected {expected:#018x}, actual {actual:#018x}"
            ),
            Self::TooManyEntries { count } => {
                write!(
                    f,
                    "state manifest holds {count} entries, max {STATE_MANIFEST_ENTRIES_MAX}"
                )
            }
            Self::EntryTooShort { entry_len } => write!(
                f,
                "state manifest entry length {entry_len} below this version's {STATE_MANIFEST_ENTRY_LEN}"
            ),
        }
    }
}

impl std::error::Error for StateManifestError {}

/// Format tag for [`encode_state_manifest`]; bump on incompatible change
/// (appending entry fields is compatible, see `entry_len`).
pub const STATE_MANIFEST_MAGIC: [u8; 4] = *b"ISM1";

/// This version's entry size: `kind(1) frontier(8) len(8) checksum(8)`.
pub const STATE_MANIFEST_ENTRY_LEN: u8 = 25;

/// Sanity ceiling on entries per manifest. A partition rejoin lists at most
/// its retained segments plus a handful of trailers; 65k is a corruption
/// guard, not a target.
pub const STATE_MANIFEST_ENTRIES_MAX: u32 = 1 << 16;

/// Encode a manifest.
///
/// Layout (little-endian): `magic(4) count(u32) entry_len(u8)` then per
/// entry `kind(u8) frontier(u64) len(u64) checksum(u64)`, terminated by an
/// `XxHash3_64(8)` over everything before it.
///
/// # Panics
/// If `artifacts.len()` exceeds [`STATE_MANIFEST_ENTRIES_MAX`] (a serving
/// peer offering that many artifacts is a bug, not an input).
#[must_use]
#[allow(clippy::cast_possible_truncation)]
pub fn encode_state_manifest(artifacts: &[StateArtifact]) -> Vec<u8> {
    assert!(
        artifacts.len() <= STATE_MANIFEST_ENTRIES_MAX as usize,
        "state manifest entry count {} exceeds {STATE_MANIFEST_ENTRIES_MAX}",
        artifacts.len()
    );
    let mut out = Vec::with_capacity(9 + artifacts.len() * STATE_MANIFEST_ENTRY_LEN as usize + 8);
    out.extend_from_slice(&STATE_MANIFEST_MAGIC);
    out.extend_from_slice(&(artifacts.len() as u32).to_le_bytes());
    out.push(STATE_MANIFEST_ENTRY_LEN);
    for artifact in artifacts {
        out.push(artifact.kind);
        out.extend_from_slice(&artifact.frontier.to_le_bytes());
        out.extend_from_slice(&artifact.len.to_le_bytes());
        out.extend_from_slice(&artifact.checksum.to_le_bytes());
    }
    let mut hasher = XxHash3_64::new();
    hasher.write(&out);
    out.extend_from_slice(&hasher.finish().to_le_bytes());
    out
}

/// Decode a manifest encoded by [`encode_state_manifest`].
///
/// Entries longer than [`STATE_MANIFEST_ENTRY_LEN`] decode fine: the known
/// prefix is read and the remainder skipped, so a future encoder can append
/// per-entry fields without breaking this decoder.
///
/// # Errors
/// [`StateManifestError`] on truncation, magic/checksum mismatch, an entry
/// count past the ceiling, or a shrunken entry length.
///
/// # Panics
/// Unreachable: slice-to-array conversions are length-checked first.
pub fn decode_state_manifest(bytes: &[u8]) -> Result<Vec<StateArtifact>, StateManifestError> {
    const TRAILER: usize = size_of::<u64>();
    let content_len = bytes
        .len()
        .checked_sub(TRAILER)
        .ok_or(StateManifestError::Truncated)?;
    let (content, trailer) = bytes.split_at(content_len);
    let expected = u64::from_le_bytes(trailer.try_into().expect("trailer is 8 bytes"));
    let mut hasher = XxHash3_64::new();
    hasher.write(content);
    let actual = hasher.finish();
    if expected != actual {
        return Err(StateManifestError::ChecksumMismatch { expected, actual });
    }

    let mut reader = ManifestReader { bytes: content };
    if reader.take(STATE_MANIFEST_MAGIC.len())? != STATE_MANIFEST_MAGIC {
        return Err(StateManifestError::BadMagic);
    }
    let count = reader.u32()?;
    if count > STATE_MANIFEST_ENTRIES_MAX {
        return Err(StateManifestError::TooManyEntries { count });
    }
    let entry_len = reader.u8()?;
    if entry_len < STATE_MANIFEST_ENTRY_LEN {
        return Err(StateManifestError::EntryTooShort { entry_len });
    }

    let mut artifacts = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let entry = reader.take(entry_len as usize)?;
        let mut entry = ManifestReader { bytes: entry };
        artifacts.push(StateArtifact {
            kind: entry.u8()?,
            frontier: entry.u64()?,
            len: entry.u64()?,
            checksum: entry.u64()?,
        });
    }
    if !reader.bytes.is_empty() {
        return Err(StateManifestError::Truncated);
    }
    Ok(artifacts)
}

/// Little-endian cursor over the encoded manifest content.
struct ManifestReader<'a> {
    bytes: &'a [u8],
}

impl<'a> ManifestReader<'a> {
    const fn take(&mut self, len: usize) -> Result<&'a [u8], StateManifestError> {
        if self.bytes.len() < len {
            return Err(StateManifestError::Truncated);
        }
        let (head, tail) = self.bytes.split_at(len);
        self.bytes = tail;
        Ok(head)
    }

    fn u8(&mut self) -> Result<u8, StateManifestError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, StateManifestError> {
        Ok(u32::from_le_bytes(self.take(4)?.try_into().expect("4B")))
    }

    fn u64(&mut self) -> Result<u64, StateManifestError> {
        Ok(u64::from_le_bytes(self.take(8)?.try_into().expect("8B")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Vec<StateArtifact> {
        vec![
            StateArtifact::for_bytes(artifact_kind::METADATA_SNAPSHOT, 192, b"snapshot-bytes"),
            StateArtifact::for_bytes(artifact_kind::CLIENT_TABLE, 195, b"table-bytes"),
        ]
    }

    #[test]
    fn roundtrip_preserves_entries() {
        let artifacts = sample();
        let decoded = decode_state_manifest(&encode_state_manifest(&artifacts)).unwrap();
        assert_eq!(decoded, artifacts);
    }

    #[test]
    fn empty_manifest_roundtrips() {
        assert_eq!(
            decode_state_manifest(&encode_state_manifest(&[])).unwrap(),
            vec![]
        );
    }

    #[test]
    fn corruption_is_rejected() {
        let mut bytes = encode_state_manifest(&sample());
        bytes[6] ^= 0xFF;
        assert!(matches!(
            decode_state_manifest(&bytes),
            Err(StateManifestError::ChecksumMismatch { .. })
        ));
        let encoded = encode_state_manifest(&sample());
        assert!(matches!(
            decode_state_manifest(&encoded[..encoded.len() - 1]),
            Err(StateManifestError::ChecksumMismatch { .. } | StateManifestError::Truncated)
        ));
    }

    // A future encoder may append per-entry fields; this decoder must read
    // the known prefix and skip the tail.
    #[test]
    fn longer_entries_decode_with_tail_skipped() {
        let artifacts = sample();
        // Re-encode by hand with entry_len + 7 trailing bytes per entry.
        let mut out = Vec::new();
        out.extend_from_slice(&STATE_MANIFEST_MAGIC);
        #[allow(clippy::cast_possible_truncation)]
        out.extend_from_slice(&(artifacts.len() as u32).to_le_bytes());
        out.push(STATE_MANIFEST_ENTRY_LEN + 7);
        for artifact in &artifacts {
            out.push(artifact.kind);
            out.extend_from_slice(&artifact.frontier.to_le_bytes());
            out.extend_from_slice(&artifact.len.to_le_bytes());
            out.extend_from_slice(&artifact.checksum.to_le_bytes());
            out.extend_from_slice(&[0xAB; 7]);
        }
        let mut hasher = XxHash3_64::new();
        hasher.write(&out);
        out.extend_from_slice(&hasher.finish().to_le_bytes());

        assert_eq!(decode_state_manifest(&out).unwrap(), artifacts);
    }

    #[test]
    fn shrunken_entries_are_rejected() {
        let mut out = Vec::new();
        out.extend_from_slice(&STATE_MANIFEST_MAGIC);
        out.extend_from_slice(&0u32.to_le_bytes());
        out.push(STATE_MANIFEST_ENTRY_LEN - 1);
        let mut hasher = XxHash3_64::new();
        hasher.write(&out);
        out.extend_from_slice(&hasher.finish().to_le_bytes());
        assert!(matches!(
            decode_state_manifest(&out),
            Err(StateManifestError::EntryTooShort { .. })
        ));
    }
}
