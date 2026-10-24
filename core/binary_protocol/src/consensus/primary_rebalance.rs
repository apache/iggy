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
pub enum PrimaryRebalanceKind {
    MetadataProbe = 0,
    MetadataReply = 1,
    PartitionProbe = 2,
    PartitionReply = 3,
    SourceProbe = 4,
    SourceReply = 5,
    PrimaryTransfer = 6,
}

/// Same-release rebalance exchange, always routed through the metadata owner.
///
/// `view` identifies the metadata coordinator's view, while `partition_view`
/// belongs to the probed partition. A ready reply only supplies evidence to the
/// coordinator. A `PrimaryTransfer` from the metadata primary of `view` grants
/// the source primary of `partition_view` a handoff to `target`. Ingress must
/// authenticate `replica`, and the coordinator must correlate the nonce and
/// history fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq, CheckedBitPattern, NoUninit)]
#[repr(C)]
pub struct PrimaryRebalanceHeader {
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
    pub commit: u64,
    /// Metadata primary's quorum-evidence age at send, or `NO_QUORUM_EVIDENCE`.
    pub quorum_age_ns: u64,
    pub partition_view: u32,
    pub kind: PrimaryRebalanceKind,
    pub ready: u8,
    /// The replica a `PrimaryTransfer` hands off to. Zero in every other kind.
    pub target: u8,
    pub reserved: [u8; 73],
}

const _: () = assert!(size_of::<PrimaryRebalanceHeader>() == HEADER_SIZE);

impl PrimaryRebalanceHeader {
    pub const NO_QUORUM_EVIDENCE: u64 = u64::MAX;
}

impl ConsensusHeader for PrimaryRebalanceHeader {
    const FRAME_SEALED: bool = true;
    const COMMAND: Command = Command::PrimaryRebalance;

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

    #[expect(
        clippy::cast_possible_truncation,
        reason = "HEADER_SIZE is fixed at 256 bytes"
    )]
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
        if self.checksum_body != 0 || self.release != 0 {
            return Err(ConsensusError::InvalidField(
                "primary rebalance body checksum and release must be zero".into(),
            ));
        }
        if self.reserved_frame != [0; 66] || self.reserved != [0; 73] {
            return Err(ConsensusError::InvalidField(
                "primary rebalance reserved bytes must be zero".into(),
            ));
        }
        if self.nonce == 0 {
            return Err(ConsensusError::InvalidField(
                "primary rebalance nonce must be nonzero".into(),
            ));
        }
        if self.ready > 1
            || (self.ready != 0
                && matches!(
                    self.kind,
                    PrimaryRebalanceKind::MetadataProbe
                        | PrimaryRebalanceKind::PartitionProbe
                        | PrimaryRebalanceKind::SourceProbe
                        | PrimaryRebalanceKind::PrimaryTransfer
                ))
        {
            return Err(ConsensusError::InvalidField(
                "primary rebalance probes and grants cannot be ready; replies require 0 or 1"
                    .into(),
            ));
        }
        if self.kind != PrimaryRebalanceKind::PrimaryTransfer && self.target != 0 {
            return Err(ConsensusError::InvalidField(
                "only a primary rebalance grant names a target".into(),
            ));
        }
        if self.kind != PrimaryRebalanceKind::MetadataProbe
            && self.quorum_age_ns != Self::NO_QUORUM_EVIDENCE
        {
            return Err(ConsensusError::InvalidField(
                "only a metadata probe can advertise quorum evidence".into(),
            ));
        }
        if self.kind == PrimaryRebalanceKind::MetadataReply && self.commit != 0 {
            return Err(ConsensusError::InvalidField(
                "metadata reply cannot advertise a commit".into(),
            ));
        }
        match self.kind {
            PrimaryRebalanceKind::MetadataProbe | PrimaryRebalanceKind::MetadataReply => {
                if self.namespace != 0 || self.created_revision != 0 || self.partition_view != 0 {
                    return Err(ConsensusError::InvalidField(
                        "primary rebalance metadata exchange cannot carry partition fields".into(),
                    ));
                }
            }
            PrimaryRebalanceKind::PartitionProbe
            | PrimaryRebalanceKind::PartitionReply
            | PrimaryRebalanceKind::SourceProbe
            | PrimaryRebalanceKind::SourceReply
            | PrimaryRebalanceKind::PrimaryTransfer => {
                if self.created_revision == 0 {
                    return Err(ConsensusError::InvalidField(
                        "primary rebalance partition revision must be nonzero".into(),
                    ));
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use aligned_vec::{AVec, ConstAlign};
    use std::mem::offset_of;

    fn header(kind: PrimaryRebalanceKind) -> PrimaryRebalanceHeader {
        let partition = matches!(
            kind,
            PrimaryRebalanceKind::PartitionProbe
                | PrimaryRebalanceKind::PartitionReply
                | PrimaryRebalanceKind::SourceProbe
                | PrimaryRebalanceKind::SourceReply
                | PrimaryRebalanceKind::PrimaryTransfer
        );
        PrimaryRebalanceHeader {
            checksum: 0,
            checksum_body: 0,
            cluster: 7,
            size: u32::try_from(HEADER_SIZE).unwrap(),
            view: 11,
            release: 0,
            command: Command::PrimaryRebalance,
            replica: 2,
            reserved_frame: [0; 66],
            nonce: u128::MAX,
            namespace: if partition { 17 } else { 0 },
            created_revision: u64::from(partition),
            commit: if partition { 23 } else { 0 },
            quorum_age_ns: PrimaryRebalanceHeader::NO_QUORUM_EVIDENCE,
            partition_view: if partition { 5 } else { 0 },
            kind,
            ready: 0,
            target: u8::from(kind == PrimaryRebalanceKind::PrimaryTransfer),
            reserved: [0; 73],
        }
    }

    #[test]
    fn given_rebalance_phases_when_encoded_should_preserve_the_fixed_layout_and_seal() {
        assert_eq!(offset_of!(PrimaryRebalanceHeader, nonce), 128);
        assert_eq!(offset_of!(PrimaryRebalanceHeader, namespace), 144);
        assert_eq!(offset_of!(PrimaryRebalanceHeader, created_revision), 152);
        assert_eq!(offset_of!(PrimaryRebalanceHeader, commit), 160);
        assert_eq!(offset_of!(PrimaryRebalanceHeader, quorum_age_ns), 168);
        assert_eq!(offset_of!(PrimaryRebalanceHeader, partition_view), 176);
        assert_eq!(offset_of!(PrimaryRebalanceHeader, kind), 180);
        assert_eq!(offset_of!(PrimaryRebalanceHeader, ready), 181);
        assert_eq!(offset_of!(PrimaryRebalanceHeader, target), 182);
        assert_eq!(offset_of!(PrimaryRebalanceHeader, reserved), 183);
        for kind in [
            PrimaryRebalanceKind::MetadataProbe,
            PrimaryRebalanceKind::MetadataReply,
            PrimaryRebalanceKind::PartitionProbe,
            PrimaryRebalanceKind::PartitionReply,
            PrimaryRebalanceKind::SourceProbe,
            PrimaryRebalanceKind::SourceReply,
            PrimaryRebalanceKind::PrimaryTransfer,
        ] {
            for ready in 0..=1 {
                let mut original = header(kind);
                original.ready = ready;
                original.seal();
                let decoded = bytemuck::checked::try_from_bytes::<PrimaryRebalanceHeader>(
                    bytemuck::bytes_of(&original),
                )
                .unwrap();
                assert_eq!(*decoded, original);
                decoded.verify_frame().unwrap();
                let is_reply = matches!(
                    kind,
                    PrimaryRebalanceKind::MetadataReply
                        | PrimaryRebalanceKind::PartitionReply
                        | PrimaryRebalanceKind::SourceReply
                );
                assert_eq!(decoded.validate().is_ok(), ready == 0 || is_reply);
            }
        }
    }

    #[test]
    fn given_metadata_quorum_evidence_when_encoded_should_preserve_commit_and_age() {
        for age in [
            0,
            1,
            8_000_000_000,
            PrimaryRebalanceHeader::NO_QUORUM_EVIDENCE,
        ] {
            let mut probe = header(PrimaryRebalanceKind::MetadataProbe);
            probe.commit = 13;
            probe.quorum_age_ns = age;
            probe.seal();
            let decoded = bytemuck::checked::try_from_bytes::<PrimaryRebalanceHeader>(
                bytemuck::bytes_of(&probe),
            )
            .unwrap();
            decoded.validate().unwrap();
            decoded.verify_frame().unwrap();
            assert_eq!(*decoded, probe);
        }
        for kind in [
            PrimaryRebalanceKind::MetadataReply,
            PrimaryRebalanceKind::PartitionProbe,
            PrimaryRebalanceKind::PartitionReply,
            PrimaryRebalanceKind::SourceProbe,
            PrimaryRebalanceKind::SourceReply,
            PrimaryRebalanceKind::PrimaryTransfer,
        ] {
            let mut invalid = header(kind);
            invalid.quorum_age_ns = 0;
            assert!(invalid.validate().is_err(), "{kind:?}");
        }
        let mut invalid = header(PrimaryRebalanceKind::MetadataReply);
        invalid.commit = 1;
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn given_a_rebalance_frame_when_fields_are_corrupt_should_reject_before_use() {
        let mut valid = header(PrimaryRebalanceKind::PartitionReply);
        valid.ready = 1;
        valid.seal();
        let mut bytes: AVec<u8, ConstAlign<16>> = AVec::new(16);
        bytes.extend_from_slice(bytemuck::bytes_of(&valid));
        for offset in [32, 52, 61, 128, 144, 152, 160, 168, 176, 180, 181, 182] {
            bytes[offset] ^= 1;
            let corrupted =
                bytemuck::checked::try_from_bytes::<PrimaryRebalanceHeader>(&bytes).unwrap();
            assert!(corrupted.verify_frame().is_err(), "offset {offset}");
            bytes[offset] ^= 1;
        }
        for kind in 7..=u8::MAX {
            bytes[offset_of!(PrimaryRebalanceHeader, kind)] = kind;
            assert!(bytemuck::checked::try_from_bytes::<PrimaryRebalanceHeader>(&bytes).is_err());
        }
    }

    #[test]
    fn given_a_rebalance_frame_when_envelope_or_phase_fields_are_invalid_should_reject() {
        let valid = header(PrimaryRebalanceKind::PartitionReply);
        for change in [
            |value: &mut PrimaryRebalanceHeader| value.command = Command::Commit,
            |value: &mut PrimaryRebalanceHeader| {
                value.size = u32::try_from(HEADER_SIZE - 1).unwrap();
            },
            |value: &mut PrimaryRebalanceHeader| {
                value.size = u32::try_from(HEADER_SIZE + 1).unwrap();
            },
            |value: &mut PrimaryRebalanceHeader| value.size = u32::MAX,
            |value: &mut PrimaryRebalanceHeader| value.checksum_body = 1,
            |value: &mut PrimaryRebalanceHeader| value.release = 1,
            |value: &mut PrimaryRebalanceHeader| value.reserved_frame[65] = 1,
            |value: &mut PrimaryRebalanceHeader| value.reserved[72] = 1,
            |value: &mut PrimaryRebalanceHeader| value.nonce = 0,
            |value: &mut PrimaryRebalanceHeader| value.ready = 2,
            |value: &mut PrimaryRebalanceHeader| value.created_revision = 0,
            |value: &mut PrimaryRebalanceHeader| value.target = 1,
        ] {
            let mut invalid = valid;
            change(&mut invalid);
            invalid.seal();
            invalid.verify_frame().unwrap();
            assert!(invalid.validate().is_err(), "{invalid:?}");
        }
        for kind in [
            PrimaryRebalanceKind::MetadataProbe,
            PrimaryRebalanceKind::MetadataReply,
        ] {
            for change in [
                |value: &mut PrimaryRebalanceHeader| value.namespace = 1,
                |value: &mut PrimaryRebalanceHeader| value.created_revision = 1,
                |value: &mut PrimaryRebalanceHeader| value.partition_view = 1,
            ] {
                let mut invalid = header(kind);
                change(&mut invalid);
                assert!(invalid.validate().is_err(), "{invalid:?}");
            }
        }
        for kind in [
            PrimaryRebalanceKind::PartitionProbe,
            PrimaryRebalanceKind::PartitionReply,
            PrimaryRebalanceKind::SourceProbe,
            PrimaryRebalanceKind::SourceReply,
            PrimaryRebalanceKind::PrimaryTransfer,
        ] {
            let mut invalid = header(kind);
            invalid.created_revision = 0;
            assert!(invalid.validate().is_err(), "{kind:?}");
        }
    }
}
