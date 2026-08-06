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

//! The durable VSR state: the consensus numbers a replica must recover from its
//! own disk after a crash, rather than infer from the WAL or relearn from a
//! peer.
//!
//! The payload `journal::superblock` persists. Consensus owns its meaning and
//! byte layout; the superblock adds framing, versioning, and integrity.
//!
//! Fixed little-endian, so it is byte-identical across replicas and stable
//! across restarts. Field order is load-bearing: reorders and width changes are
//! breaking and must ride a superblock version bump.

use std::fmt;

/// Number of bytes [`VsrState::to_bytes`] produces and [`VsrState::try_from`]
/// expects: `cluster`(16) + `replica_id`(1) + `replica_count`(1) + `view`(4)
/// + `log_view`(4) + `commit_max`(8) + `checkpoint_op`(8)
/// + `checkpoint_checksum`(16) + `offset_frontier`(8).
pub const ENCODED_LEN: usize = 66;

/// The durable consensus state of one replica for one consensus group.
///
/// A view a replica acted in must survive a crash, or it can re-participate in
/// an old view and split the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VsrState {
    /// Cluster this superblock belongs to. Recovery rejects a mismatch: a copied or
    /// misplaced data directory. Catches misplacement only, never staleness -- an
    /// older backup of this replica's own directory carries the right identity and
    /// passes.
    pub cluster: u128,
    /// Replica index this superblock belongs to. Recovery rejects a mismatch, with
    /// the same misplacement-only scope as `cluster`.
    pub replica_id: u8,
    /// Cluster size at write time. Recovery rejects a mismatch: quorum size and the
    /// `view % replica_count` primary mapping both derive from it, so booting a
    /// resized cluster without reconfiguration splits the log.
    pub replica_count: u8,
    /// Current view.
    pub view: u32,
    /// Latest view in which this replica changed its head (adopted a `StartView`
    /// as a backup, or completed a DVC quorum as primary). `log_view <= view`.
    pub log_view: u32,
    /// Highest op known committed by the cluster at write time.
    ///
    /// A recovery lower bound and nothing more: recovery reports a gap against what the
    /// WAL can prove, but cannot act on one, since closing it needs state transfer.
    /// Kept in the durable record because that is what state transfer will read, and
    /// adding it later would mean a version bump that invalidates every record.
    pub commit_max: u64,
    /// Op of the paired checkpoint (the metadata snapshot's sequence number).
    pub checkpoint_op: u64,
    /// Integrity tag of the paired checkpoint, detecting a torn
    /// snapshot/superblock pairing across a crash.
    pub checkpoint_checksum: u128,
    /// PARTITION plane: the next message offset this replica will mint, or `0`
    /// for a group whose offset space is still empty.
    ///
    /// A durable LOWER BOUND, not a completeness claim: boot takes the max of
    /// this and whatever the recovered segments prove. It exists because
    /// nothing else durably names the frontier once the segments that carried
    /// it are gone -- a state-transfer install of an all-GC'd origin, a crash
    /// inside the install's swap window, and the fence-and-rebuild path all
    /// leave a replica whose counter would otherwise restart at 0 while the
    /// group is at N. That is not a lag: replicas re-stamp `base_offset` from
    /// this counter and recompute `batch_checksum` over it, so the next
    /// replicated prepare would persist different bytes here than on every
    /// peer, silently.
    ///
    /// Always `0` on the metadata plane, which mints no message offsets.
    pub offset_frontier: u64,
}

impl VsrState {
    /// Encode to the fixed little-endian on-disk layout.
    #[must_use]
    pub fn to_bytes(&self) -> [u8; ENCODED_LEN] {
        let mut out = [0u8; ENCODED_LEN];
        out[0..16].copy_from_slice(&self.cluster.to_le_bytes());
        out[16] = self.replica_id;
        out[17] = self.replica_count;
        out[18..22].copy_from_slice(&self.view.to_le_bytes());
        out[22..26].copy_from_slice(&self.log_view.to_le_bytes());
        out[26..34].copy_from_slice(&self.commit_max.to_le_bytes());
        out[34..42].copy_from_slice(&self.checkpoint_op.to_le_bytes());
        out[42..58].copy_from_slice(&self.checkpoint_checksum.to_le_bytes());
        out[58..66].copy_from_slice(&self.offset_frontier.to_le_bytes());
        out
    }
}

impl TryFrom<&[u8]> for VsrState {
    type Error = VsrStateError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        // One length check up front puts every field slice below in bounds by
        // construction, so the `try_into`s cannot fail.
        let bytes: &[u8; ENCODED_LEN] =
            bytes.try_into().map_err(|_| VsrStateError::WrongLength {
                expected: ENCODED_LEN,
                actual: bytes.len(),
            })?;
        let state = Self {
            cluster: u128::from_le_bytes(field(bytes, 0)),
            replica_id: bytes[16],
            replica_count: bytes[17],
            view: u32::from_le_bytes(field(bytes, 18)),
            log_view: u32::from_le_bytes(field(bytes, 22)),
            commit_max: u64::from_le_bytes(field(bytes, 26)),
            checkpoint_op: u64::from_le_bytes(field(bytes, 34)),
            checkpoint_checksum: u128::from_le_bytes(field(bytes, 42)),
            offset_frontier: u64::from_le_bytes(field(bytes, 58)),
        };
        // A record violating `log_view <= view` decodes into a replica that looks
        // healthy locally while `DoViewChangeHeader::validate` makes every peer drop
        // its DVCs, so it can never conclude a view change. Length validation alone
        // would let corruption inside the checksummed region through as a live
        // consensus state.
        if state.log_view > state.view {
            return Err(VsrStateError::LogViewAheadOfView {
                view: state.view,
                log_view: state.log_view,
            });
        }
        Ok(state)
    }
}

/// Copy a fixed-width field out of the length-validated record. Always in bounds
/// for a `[u8; ENCODED_LEN]`, so the slice conversion is infallible.
fn field<const N: usize>(bytes: &[u8; ENCODED_LEN], start: usize) -> [u8; N] {
    let mut out = [0u8; N];
    out.copy_from_slice(&bytes[start..start + N]);
    out
}

/// Failure decoding a [`VsrState`] from bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VsrStateError {
    /// The byte slice was not exactly [`ENCODED_LEN`] long.
    WrongLength { expected: usize, actual: usize },
    /// The record violates `log_view <= view`, so it cannot be a state any replica
    /// reached.
    LogViewAheadOfView { view: u32, log_view: u32 },
}

impl fmt::Display for VsrStateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::WrongLength { expected, actual } => {
                write!(f, "VsrState needs {expected} bytes, got {actual}")
            }
            Self::LogViewAheadOfView { view, log_view } => write!(
                f,
                "VsrState log_view {log_view} exceeds view {view}, which no replica can reach"
            ),
        }
    }
}

impl std::error::Error for VsrStateError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_distinct_fields_when_to_bytes_should_pin_offsets_and_round_trip() {
        // Guards the on-disk layout: a reorder or width change trips this, forcing a
        // deliberate superblock version bump. Distinct per-field values make a swap
        // between two same-width fields observable.
        let state = VsrState {
            cluster: 1,
            replica_id: 2,
            replica_count: 5,
            // Distinct per-field values make a swap between two same-width fields
            // observable, and `view > log_view` keeps the record decodable.
            view: 4,
            log_view: 3,
            commit_max: 6,
            checkpoint_op: 7,
            checkpoint_checksum: 8,
            offset_frontier: 0,
        };
        let bytes = state.to_bytes();
        assert_eq!(bytes.len(), ENCODED_LEN);
        assert_eq!(bytes[0], 1, "cluster low byte");
        assert_eq!(bytes[16], 2, "replica_id");
        assert_eq!(bytes[17], 5, "replica_count");
        assert_eq!(bytes[18], 4, "view low byte");
        assert_eq!(bytes[22], 3, "log_view low byte");
        assert_eq!(bytes[26], 6, "commit_max low byte");
        assert_eq!(bytes[34], 7, "checkpoint_op low byte");
        assert_eq!(bytes[42], 8, "checkpoint_checksum low byte");

        assert_eq!(VsrState::try_from(&bytes[..]).unwrap(), state);
        assert!(VsrState::try_from(&bytes[..ENCODED_LEN - 1]).is_err());
    }

    #[test]
    fn given_log_view_past_view_when_decoded_should_reject() {
        // Corruption inside the checksummed region can produce a length-valid record
        // that violates `log_view <= view`. Decoding it yields a replica that looks
        // healthy locally while every peer drops its DoViewChange messages
        // (`DoViewChangeHeader::validate`), so it can never conclude a view change.
        let mut bytes = VsrState {
            cluster: 1,
            replica_id: 0,
            replica_count: 3,
            view: 4,
            log_view: 4,
            commit_max: 0,
            checkpoint_op: 0,
            checkpoint_checksum: 0,
            offset_frontier: 0,
        }
        .to_bytes();
        bytes[22] = 5; // log_view = 5, view stays 4

        assert_eq!(
            VsrState::try_from(&bytes[..]),
            Err(VsrStateError::LogViewAheadOfView {
                view: 4,
                log_view: 5
            })
        );
    }
}
