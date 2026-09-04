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

//! The record that makes a gap in the segment chain legitimate.
//!
//! Recovery derives each segment's bounds from its own bytes, so a gap has no
//! author: the boot re-anchor's planted gap and a lost segment look identical.
//! The re-anchor writes its intent down instead, and the chain guard admits a
//! forward gap only when the far side carries an anchor naming exactly the near
//! side.
//!
//! Written and directory-fsynced BEFORE that segment is created. A crash in the
//! window then leaves an anchor with no segment, which the boot sweep collects;
//! the other order leaves a planted segment with no anchor, which the guard
//! reads as damage on an intact chain.

use crate::state_transfer::STAGING_SUFFIX;
use compio::io::AsyncWriteAtExt;
use consensus::state_artifact_checksum;
use std::io;

/// File extension for an anchor record, `{start_offset:020}.anchor` beside the
/// `{start_offset:020}.log` it belongs to.
pub const ANCHOR_EXTENSION: &str = "anchor";

/// [`ANCHOR_EXTENSION`] as a filename suffix, for the directory sweeps that
/// match on one.
pub const ANCHOR_SUFFIX: &str = ".anchor";

/// Leading bytes of an anchor record, so a file that is not one (a truncated
/// write, an operator's copy) is refused rather than decoded.
const ANCHOR_MAGIC: u64 = u64::from_le_bytes(*b"IGGYANCH");

/// `magic`(8) + `planted_start`(8) + `sealed_start`(8) + `sealed_end`(8) +
/// `checksum`(8).
pub const ANCHOR_ENCODED_LEN: usize = 40;

/// The gap one planted segment is allowed to leave behind it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentAnchor {
    /// Start offset of the segment this record sits beside, i.e. the FAR side of
    /// the gap.
    ///
    /// Redundant with the file name and deliberately so: the name is not
    /// checksummed, so without this field the payload authenticates the
    /// predecessor bounds while saying nothing about which plant it authorises.
    /// Valid anchor bytes copied beside a later segment would then cover a wider
    /// gap after the same predecessor -- exactly the operator's copy this module
    /// claims to refuse.
    pub planted_start: u64,
    /// Start offset of the segment that was sealed, i.e. the near side of the
    /// gap. Names WHICH segment, so an anchor cannot be satisfied by a
    /// different file that happens to end where this one expects.
    pub sealed_start: u64,
    /// End offset the sealed segment held when it was sealed. The gap runs from
    /// here to the planted segment's own start offset.
    pub sealed_end: u64,
}

impl SegmentAnchor {
    /// Encode to the fixed little-endian on-disk layout.
    #[must_use]
    pub fn to_bytes(&self) -> [u8; ANCHOR_ENCODED_LEN] {
        let mut out = [0u8; ANCHOR_ENCODED_LEN];
        out[0..8].copy_from_slice(&ANCHOR_MAGIC.to_le_bytes());
        out[8..16].copy_from_slice(&self.planted_start.to_le_bytes());
        out[16..24].copy_from_slice(&self.sealed_start.to_le_bytes());
        out[24..32].copy_from_slice(&self.sealed_end.to_le_bytes());
        let checksum = state_artifact_checksum(&out[0..32]);
        out[32..40].copy_from_slice(&checksum.to_le_bytes());
        out
    }

    /// Decode a record, returning `None` for anything this build did not write:
    /// a wrong length, a wrong magic, or a checksum that does not match.
    ///
    /// A `None` is never treated as "no gap was intended". It means the record
    /// proves nothing, so the gap it would have covered stays damage.
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != ANCHOR_ENCODED_LEN {
            return None;
        }
        let field = |at: usize| -> u64 {
            let mut raw = [0u8; 8];
            raw.copy_from_slice(&bytes[at..at + 8]);
            u64::from_le_bytes(raw)
        };
        if field(0) != ANCHOR_MAGIC || field(32) != state_artifact_checksum(&bytes[0..32]) {
            return None;
        }
        Some(Self {
            planted_start: field(8),
            sealed_start: field(16),
            sealed_end: field(24),
        })
    }

    /// Whether this anchor legitimises the gap between the segment starting at
    /// `sealed_start` / ending at `sealed_end` and the segment planted at
    /// `planted_start`.
    ///
    /// All THREE bounds must match. The predecessor pair alone would let an
    /// anchor left by an earlier incarnation of the chain cover a gap it never
    /// saw; the plant alone would let any predecessor satisfy it. Binding the
    /// plant is what stops valid bytes from being copied beside a later segment
    /// to authorise a wider gap after the same predecessor.
    #[must_use]
    pub const fn covers(&self, planted_start: u64, sealed_start: u64, sealed_end: u64) -> bool {
        self.planted_start == planted_start
            && self.sealed_start == sealed_start
            && self.sealed_end == sealed_end
    }
}

/// Path of the anchor record beside the segment starting at `start_offset`.
#[must_use]
pub fn anchor_path(partition_dir: &str, start_offset: u64) -> String {
    format!("{partition_dir}/{start_offset:0>20}.{ANCHOR_EXTENSION}")
}

/// Write the anchor for a segment about to be planted at `start_offset`, then
/// fsync the directory so the record cannot arrive after the segment it
/// describes.
///
/// # Errors
///
/// Any I/O failure. The caller must NOT plant the segment: a planted segment
/// whose anchor is missing reads as damage on the next boot.
pub async fn write_anchor(partition_dir: &str, anchor: SegmentAnchor) -> io::Result<()> {
    // Temp, fsync, rename, fsync the dir, like the superblock in this same
    // directory. There is no second slot to fall back on, so a truncating
    // in-place write would leave a torn record where the guard needs either the
    // old one or the new one.
    //
    // The path comes from the record's own `planted_start` rather than a second
    // parameter: the guard matches the two for equality, so a caller that could
    // pass them separately could write a record that never satisfies anything.
    let path = anchor_path(partition_dir, anchor.planted_start);
    // `STAGING_SUFFIX` rather than a suffix of its own: every sweep already
    // unlinks it unconditionally, boot included, so a torn write leaves nothing
    // a later guard can read.
    let tmp_path = format!("{path}{STAGING_SUFFIX}");
    let mut file = compio::fs::File::create(&tmp_path).await?;
    let (result, _buf) = file
        .write_all_at(anchor.to_bytes().to_vec(), 0)
        .await
        .into();
    result?;
    file.sync_all().await?;
    compio::fs::rename(&tmp_path, &path).await?;
    crate::state_transfer::fsync_dir(partition_dir).await
}

/// Read the anchor beside the segment starting at `start_offset`.
///
/// `Ok(None)` when the file is absent, is not exactly [`ANCHOR_ENCODED_LEN`]
/// bytes, or does not decode; all of them mean the same thing to the guard, so
/// the caller needs no distinction.
///
/// The length is checked from the metadata BEFORE any read, so a corrupt or
/// foreign file left at this path cannot size an allocation on the boot path.
/// `from_bytes` would refuse it either way, but only after reading all of it.
///
/// # Errors
///
/// Any other stat or read failure. NOT folded into `Ok(None)`: an `EACCES` or
/// `EIO` over a healthy planted chain would read as no gap intended, refusing
/// it as damage for as long as the fault lasts.
pub async fn read_anchor(
    partition_dir: &str,
    start_offset: u64,
) -> io::Result<Option<SegmentAnchor>> {
    let path = anchor_path(partition_dir, start_offset);
    let length = match compio::fs::metadata(&path).await {
        Ok(metadata) => metadata.len(),
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if length != ANCHOR_ENCODED_LEN as u64 {
        return Ok(None);
    }
    match compio::fs::read(&path).await {
        Ok(bytes) => Ok(SegmentAnchor::from_bytes(&bytes)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn anchor() -> SegmentAnchor {
        SegmentAnchor {
            planted_start: 8_192,
            sealed_start: 7,
            sealed_end: 4_095,
        }
    }

    #[test]
    fn given_an_anchor_when_round_tripped_should_decode_identically() {
        let bytes = anchor().to_bytes();
        assert_eq!(bytes.len(), ANCHOR_ENCODED_LEN);
        assert_eq!(SegmentAnchor::from_bytes(&bytes), Some(anchor()));
    }

    #[test]
    fn given_a_flipped_bit_when_decoded_should_refuse() {
        // Every byte the checksum covers, so a corrupted record can never read as
        // a legitimate gap.
        for index in 0..32 {
            let mut bytes = anchor().to_bytes();
            bytes[index] ^= 1;
            assert_eq!(
                SegmentAnchor::from_bytes(&bytes),
                None,
                "a record corrupted at byte {index} must not decode"
            );
        }
    }

    #[test]
    fn given_a_wrong_length_when_decoded_should_refuse() {
        let bytes = anchor().to_bytes();
        assert_eq!(SegmentAnchor::from_bytes(&bytes[..39]), None);
        assert_eq!(SegmentAnchor::from_bytes(&[]), None);
    }

    #[test]
    fn given_an_anchor_when_matching_a_different_predecessor_should_not_cover_it() {
        let anchor = SegmentAnchor {
            planted_start: 30,
            sealed_start: 10,
            sealed_end: 20,
        };
        assert!(anchor.covers(30, 10, 20));
        assert!(!anchor.covers(30, 10, 21), "a different end must not match");
        assert!(
            !anchor.covers(30, 0, 20),
            "the same end under a different segment must not match: an anchor left \
             by an earlier chain would otherwise cover a gap it never saw"
        );
    }

    /// The copy this module claims to refuse: valid, checksum-clean bytes moved
    /// beside a LATER segment. Without the plant in the payload the record still
    /// authenticates, still names the same predecessor, and authorises a gap that
    /// is now arbitrarily wide.
    #[test]
    fn given_valid_anchor_bytes_copied_beside_a_later_segment_should_not_cover_the_wider_gap() {
        let planted = SegmentAnchor {
            planted_start: 100,
            sealed_start: 0,
            sealed_end: 99,
        };
        let decoded = SegmentAnchor::from_bytes(&planted.to_bytes())
            .expect("the copied bytes are checksum-clean, which is the premise");

        assert!(
            decoded.covers(100, 0, 99),
            "beside its own segment it holds"
        );
        assert!(
            !decoded.covers(5_000, 0, 99),
            "the same bytes beside a segment planted at 5000 must not authorise \
             the gap 100..5000 after the same predecessor"
        );
    }

    /// A foreign or corrupt file at the anchor path must be refused from its
    /// metadata, not by reading however many bytes it happens to hold.
    #[compio::test]
    async fn given_an_oversized_file_at_the_anchor_path_when_read_should_refuse_it() {
        let dir = tempfile::tempdir().expect("tempdir");
        let partition_dir = dir.path().to_str().expect("utf-8 tempdir");
        let path = anchor_path(partition_dir, 42);

        std::fs::write(&path, vec![0u8; 1 << 20]).expect("plant an oversized file");
        assert_eq!(
            read_anchor(partition_dir, 42)
                .await
                .expect("an oversized file is refused, not an error"),
            None
        );

        std::fs::write(&path, anchor().to_bytes()).expect("plant a real record");
        assert_eq!(
            read_anchor(partition_dir, 42)
                .await
                .expect("a well-formed record reads back"),
            Some(anchor())
        );
    }

    /// The path is derived from the record, so a write always lands where the
    /// guard will look for it.
    #[compio::test]
    async fn given_an_anchor_when_written_should_land_at_its_own_planted_start() {
        let dir = tempfile::tempdir().expect("tempdir");
        let partition_dir = dir.path().to_str().expect("utf-8 tempdir");

        write_anchor(partition_dir, anchor())
            .await
            .expect("write the anchor");

        assert_eq!(
            read_anchor(partition_dir, anchor().planted_start)
                .await
                .expect("read it back"),
            Some(anchor())
        );
    }
}
