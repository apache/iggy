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

use iggy_binary_protocol::{CHECKSUM_UNSEALED, PrepareHeader};

use crate::{DVC_HEADERS_MAX, DvcHeaderKind, MergedLog, dvc_header_kind, dvc_suffix_decode};

/// Size of the legacy fixed prefix. New records append a versioned recovery
/// extension, keeping adopted log identity separate from body certification.
/// Old 66/74-byte records upgrade without invented adoption evidence. A binary
/// predating the extension refuses the new record; rollback requires a consistent
/// pre-upgrade backup or a deliberate clean rejoin, never partial-file conversion.
pub const ENCODED_LEN: usize = 74;

const RECOVERY_VERSION: u16 = 2;
const RECOVERY_PREFIX_LEN: usize = ENCODED_LEN + 4;
const ADOPTED_FIELDS_LEN: usize = 40;
const ADOPTED_HEADERS_OFFSET: usize = RECOVERY_PREFIX_LEN + ADOPTED_FIELDS_LEN;

/// The layout before `offset_reserved` was appended.
///
/// [`VsrState::try_from`] accepts records of this length. Without it every
/// superblock already on disk decodes as [`VsrStateError::WrongLength`], which
/// the metadata plane treats as a durability violation and refuses the whole
/// node's boot on.
///
/// "Already on disk" is not a clustering concern. `PingPongSuperblock::open`
/// runs unconditionally in metadata recovery, so EVERY server writes one of
/// these into `metadata/superblock.a`, clustered or not, on every view change
/// and checkpoint. Every release from `server-0.9.0-edge.2` (the first that
/// carries this module at all) through `edge.6` wrote exactly 66 bytes,
/// single-node deployments that never enabled clustering included.
///
/// The 58-byte layout that preceded it is deliberately NOT accepted: it left
/// trunk before any release carried it -- `server-0.8.2-edge.1` has no
/// `vsr_state.rs`, and `edge.2` already wrote 66 -- so a record that short is
/// corruption, not history.
///
pub const ENCODED_LEN_WITHOUT_RESERVATION: usize = 66;

/// The durable consensus state of one replica for one consensus group.
///
/// A view a replica acted in must survive a crash, or it can re-participate in
/// an old view and split the log.
#[derive(Debug, Clone, PartialEq, Eq)]
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
    /// The metadata plane mints no message offsets and keeps its
    /// [`HistoryCertificate`] here instead: see [`Self::history_certificate`].
    pub offset_frontier: u64,
    /// PARTITION plane: a monotone CEILING on the offsets this replica may
    /// already have minted, claimed ahead of the counter in blocks so an append
    /// pays one superblock write per block instead of one per batch.
    ///
    /// Never folded into [`Self::offset_frontier`]. The frontier is a claim
    /// about DATA, which state transfer's rewind guard refuses to destroy; a
    /// reservation names no bytes, only "an offset up to here may have reached
    /// a client". Comparing an offer against it refuses every legitimate offer
    /// below the lease headroom, and the replica cycles transfer -> refusal ->
    /// backoff forever. Boot seeds the mint counter from both; the rewind guard
    /// reads the frontier alone.
    ///
    /// The metadata plane keeps its [`HistoryCertificate`] here instead: see
    /// [`Self::history_certificate`].
    pub offset_reserved: u64,
    /// Accepted `StartView` identity, independent of the WAL's body certificate.
    pub adopted_view: Option<AdoptedView>,
}

/// The exact log a primary announced, not a claim that its bodies are local.
/// Persisted with the replica's view before it can vote under this log's view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdoptedView {
    pub view: u32,
    pub group: u64,
    pub partition_incarnation: u64,
    pub op_head: u64,
    pub commit_max: u64,
    pub headers: Vec<PrepareHeader>,
}

impl AdoptedView {
    #[must_use]
    pub fn merged_log(&self) -> MergedLog {
        MergedLog {
            op_head: self.op_head,
            commit_max: self.commit_max,
            headers: self.headers.clone(),
            committed_elsewhere: Vec::new(),
        }
    }

    /// An adopted identity may retain a repaired body without certifying the WAL.
    #[must_use]
    pub fn vouches_for(&self, header: &PrepareHeader) -> bool {
        if header.checksum == CHECKSUM_UNSEALED {
            return false;
        }
        self.headers.iter().any(|adopted| {
            if adopted.op != header.op || dvc_header_kind(adopted) != DvcHeaderKind::Valid {
                return false;
            }
            // Retransmission re-stamps the view without changing prepare identity.
            let mut resident = *header;
            resident.view = adopted.view;
            resident == *adopted
        })
    }

    /// Validate the same header identities and chain accepted on the wire,
    /// together with their durable owner and advertised frontier.
    ///
    /// # Errors
    /// Refuses evidence that could not describe this replica's accepted view.
    pub fn validate(&self, cluster: u128, view: u32) -> Result<(), VsrStateError> {
        if self.view > view || self.commit_max > self.op_head {
            return Err(VsrStateError::InvalidAdoptedView);
        }
        dvc_suffix_decode(bytemuck::cast_slice(&self.headers), self.op_head, 0, 0)
            .map_err(|_| VsrStateError::InvalidAdoptedView)?;
        if self.headers.iter().any(|header| {
            dvc_header_kind(header) == DvcHeaderKind::Valid
                && (header.cluster != cluster
                    || header.group != self.group
                    || header.partition_incarnation != self.partition_incarnation)
        }) {
            return Err(VsrStateError::InvalidAdoptedView);
        }
        Ok(())
    }
}

/// METADATA plane: the newest view whose log this replica's WAL durably holds.
///
/// `log_view` names the view a replica adopted, which can run ahead of its WAL: a
/// backup adopts a `StartView` and only then removes divergent entries and refills
/// the view's log. A crash in that window must not let the WAL be advertised under
/// the adopted view, or a `DoViewChange` presents a stale suffix as canonical.
///
/// Advanced only once a view's log is proven against the WAL, and kept through a
/// rewrite. Entries above `op` are this view's history only while no later view was
/// made durable after the binding: see [`HistoryRecovery`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryCertificate {
    pub view: u32,
    /// The WAL head the certificate is bound to.
    pub op: u64,
    /// The low 32 bits of the checksum of the entry at `op`, zero when no entry is
    /// left above the snapshot floor. The record has room for no more, and they only
    /// guard against a binding the WAL lost to a bug: a rewrite moves the binding
    /// before it removes the entry.
    pub checksum: u32,
}

impl HistoryCertificate {
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub const fn new(view: u32, op: u64, checksum: u128) -> Self {
        Self {
            view,
            op,
            checksum: checksum as u32,
        }
    }

    /// Whether `checksum` is the entry the certificate is bound to.
    #[must_use]
    pub const fn binds(&self, checksum: u128) -> bool {
        Self::new(self.view, self.op, checksum).checksum == self.checksum
    }
}

/// What a restarted METADATA replica may claim about the WAL it recovered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryRecovery {
    /// The original prefix is `view` history. Entries from `truncate_from` up have
    /// neither that proof nor a matching adopted identity and must be dropped.
    Certified {
        view: u32,
        truncate_from: Option<u64>,
    },
    /// Nothing proves which view the WAL holds: the record predates certificates, or
    /// names an entry the WAL lost. Claim no view, fence the whole suffix, and vote in
    /// no election until a `StartView` gives the log a view to be proven against.
    Unproven,
}

impl HistoryRecovery {
    /// Preserve the resident prefix named by a newer durable adoption. A missing
    /// body is not a conflicting entry; replay still stops before any hole.
    /// `headers` must visit resident entries in ascending op order.
    #[must_use]
    pub fn retain_adopted_suffix(
        self,
        adopted: &AdoptedView,
        headers: impl IntoIterator<Item = PrepareHeader>,
    ) -> Self {
        let Self::Certified {
            view,
            truncate_from: Some(from),
        } = self
        else {
            return self;
        };
        if adopted.view <= view {
            return self;
        }
        let mut truncate_from = Some(from);
        for header in headers.into_iter().filter(|header| header.op >= from) {
            if !adopted.vouches_for(&header) {
                truncate_from = Some(header.op);
                break;
            }
            truncate_from = header.op.checked_add(1);
        }
        Self::Certified {
            view,
            truncate_from,
        }
    }

    /// Decide from the durable record and the recovered WAL, whose entry checksums
    /// `checksum_at` reads.
    ///
    /// Without a record nothing was ever made durable, so no view was adopted and
    /// the WAL is view-0 history. A solo replica's WAL is its log by definition.
    #[must_use]
    pub fn new(
        state: Option<&VsrState>,
        solo: bool,
        checksum_at: impl Fn(u64) -> Option<u128>,
    ) -> Self {
        let Some(state) = state else {
            return Self::Certified {
                view: 0,
                truncate_from: None,
            };
        };
        if solo {
            return Self::Certified {
                view: state.log_view,
                truncate_from: None,
            };
        }
        let Some(certificate) = state
            .history_certificate()
            .filter(|certificate| certificate.view <= state.log_view)
        else {
            return Self::Unproven;
        };
        // A rewrite or refill for a later view runs only once that view is durable,
        // so while it is not, every entry is still this view's history.
        if state.view == certificate.view {
            return Self::Certified {
                view: certificate.view,
                truncate_from: None,
            };
        }
        let anchored = certificate.op <= state.checkpoint_op;
        if anchored
            || checksum_at(certificate.op).is_some_and(|checksum| certificate.binds(checksum))
        {
            return Self::Certified {
                view: certificate.view,
                truncate_from: Some(certificate.op.max(state.checkpoint_op).saturating_add(1)),
            };
        }
        Self::Unproven
    }

    /// The `log_view` the replica restores, and advertises in its `DoViewChange`.
    #[must_use]
    pub const fn log_view(&self) -> u32 {
        match *self {
            Self::Certified { view, .. } => view,
            Self::Unproven => 0,
        }
    }
}

impl VsrState {
    /// METADATA plane: the history certificate this record carries, `None` for a
    /// record written before certificates existed.
    ///
    /// Packed into the two offset words, which the metadata plane otherwise leaves
    /// zero: `offset_frontier` holds the bound op, `offset_reserved` the view plus one
    /// in its high half and the bound checksum in its low half. The plus one keeps a
    /// certificate for view 0 apart from an old record's zero.
    #[must_use]
    #[allow(clippy::cast_possible_truncation)]
    pub const fn history_certificate(&self) -> Option<HistoryCertificate> {
        let view = (self.offset_reserved >> 32) as u32;
        if view == 0 {
            return None;
        }
        Some(HistoryCertificate {
            view: view - 1,
            op: self.offset_frontier,
            checksum: self.offset_reserved as u32,
        })
    }

    /// METADATA plane: store `certificate` as [`Self::history_certificate`] reads it.
    /// A view with no room for the plus one is stored as no certificate.
    #[must_use]
    pub const fn with_history_certificate(
        mut self,
        certificate: Option<HistoryCertificate>,
    ) -> Self {
        let (op, packed) = match certificate {
            Some(certificate) => match certificate.view.checked_add(1) {
                Some(view) => (
                    certificate.op,
                    ((view as u64) << 32) | certificate.checksum as u64,
                ),
                None => (0, 0),
            },
            None => (0, 0),
        };
        self.offset_frontier = op;
        self.offset_reserved = packed;
        self
    }

    /// Refuse evidence copied from another group or partition incarnation.
    ///
    /// # Errors
    /// Returns an identity error without installing any recovered state.
    pub fn validate_group(&self, group: u64, incarnation: u64) -> Result<(), VsrStateError> {
        if self.adopted_view.as_ref().is_some_and(|adopted| {
            adopted.group != group || adopted.partition_incarnation != incarnation
        }) {
            return Err(VsrStateError::AdoptedGroupMismatch);
        }
        Ok(())
    }

    /// Encode the legacy prefix and versioned adopted-view extension atomically.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = vec![0u8; RECOVERY_PREFIX_LEN];
        out[0..16].copy_from_slice(&self.cluster.to_le_bytes());
        out[16] = self.replica_id;
        out[17] = self.replica_count;
        out[18..22].copy_from_slice(&self.view.to_le_bytes());
        out[22..26].copy_from_slice(&self.log_view.to_le_bytes());
        out[26..34].copy_from_slice(&self.commit_max.to_le_bytes());
        out[34..42].copy_from_slice(&self.checkpoint_op.to_le_bytes());
        out[42..58].copy_from_slice(&self.checkpoint_checksum.to_le_bytes());
        out[58..66].copy_from_slice(&self.offset_frontier.to_le_bytes());
        out[66..74].copy_from_slice(&self.offset_reserved.to_le_bytes());
        out[74..76].copy_from_slice(&RECOVERY_VERSION.to_le_bytes());
        if let Some(adopted) = &self.adopted_view {
            out[76..78].copy_from_slice(&1u16.to_le_bytes());
            out.extend_from_slice(&adopted.view.to_le_bytes());
            out.extend_from_slice(&adopted.group.to_le_bytes());
            out.extend_from_slice(&adopted.partition_incarnation.to_le_bytes());
            out.extend_from_slice(&adopted.op_head.to_le_bytes());
            out.extend_from_slice(&adopted.commit_max.to_le_bytes());
            let count = u32::try_from(adopted.headers.len()).unwrap_or(u32::MAX);
            out.extend_from_slice(&count.to_le_bytes());
            out.extend_from_slice(bytemuck::cast_slice(&adopted.headers));
        }
        out
    }
}

impl TryFrom<&[u8]> for VsrState {
    type Error = VsrStateError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        // Length-tolerant for ONE legacy layout, the pre-`offset_reserved` record
        // every tagged release wrote. The one length check then puts every field
        // slice below in bounds by construction, so the `try_into`s cannot fail.
        //
        // `offset_reserved` is filled from `offset_frontier`, not zeroed. The two
        // agree on a record written before the reservation existed: the frontier
        // is what that build's data proved, and the write side clamps the
        // reservation up to it anyway, so this is the same value a first write
        // under this build would record. A 0 would instead claim "nothing
        // reserved" for offsets the frontier says exist.
        //
        // It cannot recover what the old build never wrote down -- offsets acked
        // out of RAM above the frontier are gone with the process either way --
        // but refusing the record recovers nothing and costs the node its boot.
        let mut padded = [0u8; ENCODED_LEN];
        let adopted_view = if bytes.len() >= RECOVERY_PREFIX_LEN {
            let version = u16::from_le_bytes([bytes[74], bytes[75]]);
            if version != RECOVERY_VERSION {
                return Err(VsrStateError::UnsupportedVersion { version });
            }
            let present = u16::from_le_bytes([bytes[76], bytes[77]]);
            match present {
                0 if bytes.len() == RECOVERY_PREFIX_LEN => None,
                1 if bytes.len() >= ADOPTED_HEADERS_OFFSET => {
                    let fields = &bytes[RECOVERY_PREFIX_LEN..ADOPTED_HEADERS_OFFSET];
                    let count = u32::from_le_bytes(field(fields, 36)) as usize;
                    if count > DVC_HEADERS_MAX
                        || bytes.len()
                            != ADOPTED_HEADERS_OFFSET + count * size_of::<PrepareHeader>()
                    {
                        return Err(VsrStateError::InvalidAdoptedView);
                    }
                    let mut headers = Vec::with_capacity(count);
                    for chunk in bytes[ADOPTED_HEADERS_OFFSET..]
                        .as_chunks::<{ size_of::<PrepareHeader>() }>()
                        .0
                    {
                        headers.push(
                            bytemuck::checked::try_pod_read_unaligned(chunk)
                                .map_err(|_| VsrStateError::InvalidAdoptedView)?,
                        );
                    }
                    Some(AdoptedView {
                        view: u32::from_le_bytes(field(fields, 0)),
                        group: u64::from_le_bytes(field(fields, 4)),
                        partition_incarnation: u64::from_le_bytes(field(fields, 12)),
                        op_head: u64::from_le_bytes(field(fields, 20)),
                        commit_max: u64::from_le_bytes(field(fields, 28)),
                        headers,
                    })
                }
                _ => return Err(VsrStateError::InvalidAdoptedView),
            }
        } else {
            None
        };
        match bytes.len() {
            length if length >= RECOVERY_PREFIX_LEN => {
                padded.copy_from_slice(&bytes[..ENCODED_LEN]);
            }
            ENCODED_LEN => padded.copy_from_slice(bytes),
            ENCODED_LEN_WITHOUT_RESERVATION => {
                padded[..ENCODED_LEN_WITHOUT_RESERVATION].copy_from_slice(bytes);
                padded[ENCODED_LEN_WITHOUT_RESERVATION..ENCODED_LEN]
                    .copy_from_slice(&bytes[58..66]);
            }
            actual => {
                return Err(VsrStateError::WrongLength {
                    expected: ENCODED_LEN,
                    actual,
                });
            }
        }
        let bytes = &padded;
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
            offset_reserved: u64::from_le_bytes(field(bytes, 66)),
            adopted_view,
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
        if let Some(adopted) = &state.adopted_view {
            adopted.validate(state.cluster, state.view)?;
        }
        Ok(state)
    }
}

/// Copy a fixed-width field out of the length-validated record. Always in bounds
/// after the fixed prefix or extension length checks above.
fn field<const N: usize>(bytes: &[u8], start: usize) -> [u8; N] {
    let mut out = [0u8; N];
    out.copy_from_slice(&bytes[start..start + N]);
    out
}

/// Failure decoding a [`VsrState`] from bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VsrStateError {
    /// The record did not match any supported fixed or extended layout.
    WrongLength {
        expected: usize,
        actual: usize,
    },
    /// The record violates `log_view <= view`, so it cannot be a state any replica
    /// reached.
    LogViewAheadOfView {
        view: u32,
        log_view: u32,
    },
    UnsupportedVersion {
        version: u16,
    },
    InvalidAdoptedView,
    AdoptedGroupMismatch,
}

impl fmt::Display for VsrStateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedVersion { version } => {
                write!(f, "unsupported VsrState recovery version {version}")
            }
            Self::InvalidAdoptedView => write!(f, "invalid durable adopted-view evidence"),
            Self::AdoptedGroupMismatch => write!(
                f,
                "durable adopted view belongs to another group or incarnation"
            ),
            Self::WrongLength { expected, actual } => {
                write!(
                    f,
                    "VsrState needs {expected} bytes (or {ENCODED_LEN_WITHOUT_RESERVATION}, \
                     the layout before the offset reservation), got {actual}"
                )
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
            offset_frontier: 9,
            offset_reserved: 10,
            adopted_view: None,
        };
        let bytes = state.to_bytes();
        assert_eq!(bytes.len(), RECOVERY_PREFIX_LEN);
        assert_eq!(bytes[0], 1, "cluster low byte");
        assert_eq!(bytes[16], 2, "replica_id");
        assert_eq!(bytes[17], 5, "replica_count");
        assert_eq!(bytes[18], 4, "view low byte");
        assert_eq!(bytes[22], 3, "log_view low byte");
        assert_eq!(bytes[26], 6, "commit_max low byte");
        assert_eq!(bytes[34], 7, "checkpoint_op low byte");
        assert_eq!(bytes[42], 8, "checkpoint_checksum low byte");
        assert_eq!(bytes[58], 9, "offset_frontier low byte");
        assert_eq!(bytes[66], 10, "offset_reserved low byte");

        assert_eq!(VsrState::try_from(&bytes[..]).unwrap(), state);
        assert!(VsrState::try_from(&bytes[..ENCODED_LEN - 1]).is_err());
    }

    /// A superblock written before `offset_reserved` existed must still decode:
    /// every release from `server-0.9.0-edge.2` to `edge.6` wrote that layout on
    /// both planes -- the metadata one unconditionally, so single-node
    /// deployments that never enabled clustering have one -- and the metadata
    /// plane refuses the whole node's boot on a record it cannot decode.
    #[test]
    fn given_pre_reservation_record_when_decoded_should_fill_from_the_frontier() {
        let full = VsrState {
            cluster: 3,
            replica_id: 1,
            replica_count: 3,
            view: 9,
            log_view: 8,
            commit_max: 41,
            checkpoint_op: 7,
            checkpoint_checksum: 5,
            offset_frontier: 77,
            offset_reserved: 88,
            adopted_view: None,
        }
        .to_bytes();

        let legacy = &full[..ENCODED_LEN_WITHOUT_RESERVATION];
        let decoded = VsrState::try_from(legacy).expect("a pre-reservation record must decode");
        assert_eq!(decoded.offset_frontier, 77);
        assert_eq!(
            decoded.offset_reserved, 77,
            "the reservation fills from the frontier, not from zero: a 0 would claim \
             nothing was reserved for offsets the frontier says exist"
        );
        assert_eq!(decoded.view, 9);
        assert_eq!(decoded.log_view, 8);
        assert_eq!(decoded.commit_max, 41);
        assert_eq!(decoded.checkpoint_op, 7);
        assert_eq!(decoded.checkpoint_checksum, 5);

        // Anything that is neither layout is still refused, the 58-byte
        // pre-frontier layout included: `server-0.8.2-edge.1` has no
        // `vsr_state.rs` and `edge.2` already wrote 66, so no release ever put a
        // 58-byte record on a disk and one that short is corruption, not
        // history.
        for short in [40, 58] {
            assert!(
                matches!(
                    VsrState::try_from(&full[..short]),
                    Err(VsrStateError::WrongLength { .. })
                ),
                "a {short}-byte record must not decode"
            );
        }
    }

    /// A zero frontier is the shape a record written before either offset field
    /// existed decodes into, and the fill must not turn that into a claim.
    #[test]
    fn given_pre_reservation_record_with_no_frontier_when_decoded_should_reserve_nothing() {
        let full = VsrState {
            cluster: 3,
            replica_id: 1,
            replica_count: 3,
            view: 2,
            log_view: 2,
            commit_max: 0,
            checkpoint_op: 0,
            checkpoint_checksum: 0,
            offset_frontier: 0,
            offset_reserved: 0,
            adopted_view: None,
        }
        .to_bytes();

        let decoded = VsrState::try_from(&full[..ENCODED_LEN_WITHOUT_RESERVATION])
            .expect("a pre-reservation record must decode");
        assert_eq!(decoded.offset_frontier, 0);
        assert_eq!(decoded.offset_reserved, 0);
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
            // Distinct and nonzero: with 0 here a transposed write over the
            // trailing field would still satisfy every assertion below.
            offset_frontier: 9,
            offset_reserved: 11,
            adopted_view: None,
        }
        .to_bytes();
        assert_eq!(bytes[58], 9, "offset_frontier must occupy bytes 58..66");
        assert_eq!(bytes[66], 11, "offset_reserved must occupy bytes 66..74");
        bytes[22] = 5; // log_view = 5, view stays 4

        assert_eq!(
            VsrState::try_from(&bytes[..]),
            Err(VsrStateError::LogViewAheadOfView {
                view: 4,
                log_view: 5
            })
        );
    }

    fn metadata_record(view: u32, log_view: u32, checkpoint_op: u64) -> VsrState {
        VsrState {
            cluster: 1,
            replica_id: 1,
            replica_count: 3,
            view,
            log_view,
            commit_max: 0,
            checkpoint_op,
            checkpoint_checksum: 0,
            offset_frontier: 0,
            offset_reserved: 0,
            adopted_view: None,
        }
    }

    #[test]
    fn given_a_history_certificate_when_recorded_should_round_trip_and_tell_view_zero_from_none() {
        const CHECKSUM: u128 = u128::MAX - 3;
        for certificate in [
            HistoryCertificate::new(0, 0, 0),
            HistoryCertificate::new(7, 41, CHECKSUM),
        ] {
            let state = metadata_record(9, 8, 3).with_history_certificate(Some(certificate));
            let decoded = VsrState::try_from(&state.to_bytes()[..]).unwrap();
            assert_eq!(decoded, state);
            assert_eq!(decoded.history_certificate(), Some(certificate));
        }
        let certificate = HistoryCertificate::new(7, 41, CHECKSUM);
        assert!(certificate.binds(CHECKSUM));
        assert!(!certificate.binds(CHECKSUM ^ 1));

        // Records written before certificates: zero words, or the shorter layout
        // whose reservation fills from a zero frontier.
        let legacy = metadata_record(9, 8, 3);
        assert_eq!(legacy.history_certificate(), None);
        let short = &legacy.to_bytes()[..ENCODED_LEN_WITHOUT_RESERVATION];
        assert_eq!(
            VsrState::try_from(short).unwrap().history_certificate(),
            None
        );
        assert_eq!(legacy.clone().with_history_certificate(None), legacy);
    }

    #[test]
    fn given_recovered_records_when_deciding_history_should_trust_only_a_bound_certificate() {
        const WAL_HEAD: u64 = 12;
        const BINDING: u64 = 10;
        const OLD_VIEW: u32 = 5;
        const ADOPTED_VIEW: u32 = 6;
        // Low bits set, so a binding that kept only the op would not match.
        const CHECKSUM_LOW_BITS: u128 = 0x00C0_FFEE;
        let checksum_of = |op: u64| u128::from(op) << 64 | CHECKSUM_LOW_BITS;
        let checksum_at = |op: u64| (op <= WAL_HEAD).then(|| checksum_of(op));
        let bound = |op: u64| HistoryCertificate::new(OLD_VIEW, op, checksum_of(op));
        let decide = |state: Option<VsrState>, solo: bool| {
            HistoryRecovery::new(state.as_ref(), solo, checksum_at)
        };
        let certified = |view: u32, truncate_from: Option<u64>| HistoryRecovery::Certified {
            view,
            truncate_from,
        };

        // Nothing was ever recorded, so nothing was adopted.
        assert_eq!(decide(None, false), certified(0, None));
        // A solo replica's WAL is its log.
        let legacy = metadata_record(ADOPTED_VIEW, ADOPTED_VIEW, 0);
        assert_eq!(
            decide(Some(legacy.clone()), true),
            certified(ADOPTED_VIEW, None)
        );
        // An earlier build recorded the adopted view with no proof of its log.
        assert_eq!(decide(Some(legacy), false), HistoryRecovery::Unproven);
        assert_eq!(HistoryRecovery::Unproven.log_view(), 0);

        let record = |view, log_view, checkpoint_op, certificate| {
            metadata_record(view, log_view, checkpoint_op)
                .with_history_certificate(Some(certificate))
        };
        // No later view was recorded, so every entry is the certified view's,
        // above the binding too.
        let same_view = record(OLD_VIEW, OLD_VIEW, 0, bound(BINDING));
        assert_eq!(decide(Some(same_view), false), certified(OLD_VIEW, None));
        // A later view was recorded, during its view change or after adopting
        // it: entries above the binding were journaled for it and are dropped.
        for log_view in [OLD_VIEW, ADOPTED_VIEW] {
            let adopted = record(ADOPTED_VIEW, log_view, 0, bound(BINDING));
            let decided = decide(Some(adopted), false);
            assert_eq!(decided, certified(OLD_VIEW, Some(BINDING + 1)));
            assert_eq!(decided.log_view(), OLD_VIEW);
        }
        // A checkpoint published over the binding anchors it, even once its drain
        // removed the bound entry.
        let drained = HistoryCertificate::new(OLD_VIEW, BINDING - 2, 1);
        let checkpointed = record(ADOPTED_VIEW, ADOPTED_VIEW, BINDING, drained);
        assert_eq!(
            decide(Some(checkpointed), false),
            certified(OLD_VIEW, Some(BINDING + 1))
        );
        // A binding the WAL no longer holds proves nothing.
        for lost in [
            bound(WAL_HEAD + 1),
            HistoryCertificate::new(OLD_VIEW, BINDING, 1),
        ] {
            let unbound = record(ADOPTED_VIEW, ADOPTED_VIEW, 0, lost);
            assert_eq!(decide(Some(unbound), false), HistoryRecovery::Unproven);
        }
        // Certified past the recorded log_view: corruption.
        let ahead = record(ADOPTED_VIEW, OLD_VIEW - 1, 0, bound(BINDING));
        assert_eq!(decide(Some(ahead), false), HistoryRecovery::Unproven);
    }
    #[test]
    fn given_adopted_headers_when_persisted_should_round_trip_independently_of_body_proof() {
        let state = adopted_record();
        let bytes = state.to_bytes();
        assert_eq!(VsrState::try_from(bytes.as_slice()).unwrap(), state);
        let legacy = VsrState::try_from(&bytes[..ENCODED_LEN]).unwrap();
        assert!(legacy.adopted_view.is_none());
        assert_eq!(legacy.history_certificate(), state.history_certificate());
        assert_eq!(state.adopted_view.as_ref().unwrap().view, state.view);
        assert!(state.history_certificate().unwrap().view < state.view);
        assert!(state.validate_group(7, 9).is_ok());
        assert_eq!(
            state.validate_group(8, 9),
            Err(VsrStateError::AdoptedGroupMismatch)
        );
        assert_eq!(
            state.validate_group(7, 10),
            Err(VsrStateError::AdoptedGroupMismatch)
        );
    }

    #[test]
    fn given_malformed_adopted_evidence_when_decoding_should_refuse_before_recovery() {
        let original = adopted_record();
        for corrupt in 0..7 {
            let mut state = original.clone();
            let adopted = state.adopted_view.as_mut().unwrap();
            match corrupt {
                0 => adopted.view = state.view + 1,
                1 => adopted.commit_max = adopted.op_head + 1,
                2 => adopted.op_head += 1,
                3 => adopted.headers[0].checksum ^= 1,
                4 => adopted.headers[0].reserved[0] = 1,
                5 => {
                    adopted.headers[0].parent ^= 1;
                    adopted.headers[0].checksum = adopted.headers[0].identity_checksum();
                }
                6 => adopted.group += 1,
                _ => unreachable!(),
            }
            assert_eq!(
                VsrState::try_from(state.to_bytes().as_slice()),
                Err(VsrStateError::InvalidAdoptedView),
                "case {corrupt}"
            );
        }
        let mut bytes = original.to_bytes();
        bytes[ADOPTED_HEADERS_OFFSET - 4..ADOPTED_HEADERS_OFFSET]
            .copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(
            VsrState::try_from(bytes.as_slice()),
            Err(VsrStateError::InvalidAdoptedView)
        );
        let mut bytes = original.to_bytes();
        bytes.pop();
        assert_eq!(
            VsrState::try_from(bytes.as_slice()),
            Err(VsrStateError::InvalidAdoptedView)
        );
        let mut bytes = original.to_bytes();
        bytes[ENCODED_LEN..ENCODED_LEN + 2].copy_from_slice(&(RECOVERY_VERSION + 1).to_le_bytes());
        assert_eq!(
            VsrState::try_from(bytes.as_slice()),
            Err(VsrStateError::UnsupportedVersion {
                version: RECOVERY_VERSION + 1
            })
        );
    }

    #[test]
    fn given_an_unsealed_adopted_header_when_recovering_should_truncate_its_unproven_body() {
        let mut state = adopted_record();
        let adopted = state.adopted_view.as_mut().unwrap();
        let oldest = adopted.headers.last_mut().unwrap();
        oldest.checksum = iggy_binary_protocol::CHECKSUM_UNSEALED;
        let first_unproven = oldest.op;
        let original = HistoryRecovery::Certified {
            view: state.history_certificate().unwrap().view,
            truncate_from: Some(first_unproven),
        };
        let adopted = state.adopted_view.as_ref().unwrap();
        let recovered =
            original.retain_adopted_suffix(adopted, adopted.headers.iter().rev().copied());
        assert_eq!(
            recovered, original,
            "an unsealed header cannot vouch for a repaired body even when its bytes match"
        );
    }

    fn adopted_record() -> VsrState {
        let mut older = PrepareHeader {
            command: iggy_binary_protocol::Command::Prepare,
            operation: iggy_binary_protocol::Operation::CreateStream,
            cluster: 1,
            group: 7,
            partition_incarnation: 9,
            op: 25,
            commit: 24,
            timestamp: 25,
            ..PrepareHeader::default()
        };
        older.checksum = older.identity_checksum();
        let mut newer = PrepareHeader {
            op: 26,
            timestamp: 26,
            parent: older.checksum,
            ..older
        };
        newer.checksum = newer.identity_checksum();
        let mut state = metadata_record(14, 10, 0)
            .with_history_certificate(Some(HistoryCertificate::new(10, 24, 1)));
        state.adopted_view = Some(AdoptedView {
            view: 14,
            group: 7,
            partition_incarnation: 9,
            op_head: 26,
            commit_max: 25,
            headers: vec![newer, older],
        });
        state
    }
}
