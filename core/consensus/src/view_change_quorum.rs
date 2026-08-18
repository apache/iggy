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

use crate::REPLICAS_MAX;
use iggy_binary_protocol::{
    CHECKSUM_UNSEALED, Command, ConsensusHeader, DVC_HEADERS_MAX, Operation, PrepareHeader,
};

/// Write prepare headers into a control-message body, high-to-low op.
///
/// Shared by `DoViewChange` and `StartView`, which both carry a suffix as a plain
/// run of 256-byte headers; stating the layout once keeps them from drifting.
///
/// # Panics
/// When `dst` is not exactly `headers.len()` headers wide.
pub fn encode_prepare_headers(headers: &[PrepareHeader], dst: &mut [u8]) {
    let stride = size_of::<PrepareHeader>();
    assert_eq!(
        dst.len(),
        std::mem::size_of_val(headers),
        "control-message body buffer must fit the headers exactly"
    );
    for (index, header) in headers.iter().enumerate() {
        dst[index * stride..(index + 1) * stride].copy_from_slice(bytemuck::bytes_of(header));
    }
}

/// Placeholder standing in for a suffix entry the sender does not hold.
///
/// The suffix stays consecutive so a `(head_op, op)` pair indexes it arithmetically
/// and one bitset bit lines up with one op. A gap is transmitted, not omitted.
///
/// `Operation::Reserved` is the marker, since no real prepare carries it, and every
/// other field is zero. [`dvc_header_kind`] insists on exactly that, so arbitrary
/// bytes cannot pass as a blank the merge would index.
#[must_use]
pub fn dvc_blank(op: u64) -> PrepareHeader {
    PrepareHeader {
        command: Command::Prepare,
        operation: Operation::Reserved,
        op,
        ..Default::default()
    }
}

/// What a suffix slot says about the sender's log at that op.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DvcHeaderKind {
    /// No header for this op, or one the sender cannot vouch for. The nack bit is
    /// what distinguishes "never prepared" (proof) from "lost it" (no proof).
    Blank,
    /// A real prepare header the sender holds.
    Valid,
}

/// Classify a suffix slot, by exact comparison against the canonical blank.
#[must_use]
pub fn dvc_header_kind(header: &PrepareHeader) -> DvcHeaderKind {
    if *header == dvc_blank(header.op) {
        DvcHeaderKind::Blank
    } else {
        DvcHeaderKind::Valid
    }
}

/// A sender's uncommitted suffix: the headers spanning `commit..=op`, high-to-low,
/// plus one nack bit and one present bit per entry.
///
/// Index 0 is the head (`StoredDvc::op`); index `i` is op `op - i`. Empty when the
/// sender has nothing uncommitted, or could not snapshot a suffix for this log.
#[derive(Debug, Clone, Default)]
pub struct DvcSuffix {
    headers: Vec<PrepareHeader>,
    nack_bitset: u128,
    present_bitset: u128,
}

// These all read through the `Vec`, and neither `Vec::len` nor `Deref` is const on
// the pinned toolchain, so clippy's suggestion does not compile.
#[allow(clippy::missing_const_for_fn)]
impl DvcSuffix {
    /// # Panics
    /// When `headers` exceeds [`DVC_HEADERS_MAX`], or a bitset sets a bit past the
    /// suffix. Sender-side programming errors; the same conditions off the wire go
    /// through `DoViewChangeHeader::validate`.
    #[must_use]
    pub fn new(headers: Vec<PrepareHeader>, nack_bitset: u128, present_bitset: u128) -> Self {
        assert!(
            headers.len() <= DVC_HEADERS_MAX,
            "DVC suffix of {} entries exceeds the addressable maximum {DVC_HEADERS_MAX}",
            headers.len()
        );
        if headers.len() < DVC_HEADERS_MAX {
            let beyond = !((1u128 << headers.len()) - 1);
            assert_eq!(
                nack_bitset & beyond,
                0,
                "nack bit set past the {}-entry suffix",
                headers.len()
            );
            assert_eq!(
                present_bitset & beyond,
                0,
                "present bit set past the {}-entry suffix",
                headers.len()
            );
        }
        Self {
            headers,
            nack_bitset,
            present_bitset,
        }
    }

    /// A sender contributing numbers only: no headers, nacks, or offered bodies.
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.headers.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.headers.is_empty()
    }

    #[must_use]
    pub fn headers(&self) -> &[PrepareHeader] {
        &self.headers
    }

    #[must_use]
    pub const fn nack_bitset(&self) -> u128 {
        self.nack_bitset
    }

    #[must_use]
    pub const fn present_bitset(&self) -> u128 {
        self.present_bitset
    }

    /// Bytes the headers occupy on the wire.
    #[must_use]
    pub fn encoded_len(&self) -> usize {
        self.headers.len() * size_of::<PrepareHeader>()
    }

    /// Write the headers into a `DoViewChange` body, high-to-low op.
    ///
    /// Paired with [`dvc_suffix_decode`], so the wire ordering is stated once.
    ///
    /// # Panics
    /// When `dst` is not exactly [`Self::encoded_len`] bytes.
    pub fn encode_into(&self, dst: &mut [u8]) {
        encode_prepare_headers(&self.headers, dst);
    }

    /// Slot index for `op`. `None` when `op` falls outside this sender's window.
    #[must_use]
    pub fn index_of(&self, head_op: u64, op: u64) -> Option<usize> {
        let distance = usize::try_from(head_op.checked_sub(op)?).ok()?;
        (distance < self.headers.len()).then_some(distance)
    }

    /// The header at `index`, or `None` for a blank or out-of-range slot.
    #[must_use]
    pub fn valid_header_at(&self, index: usize) -> Option<&PrepareHeader> {
        let header = self.headers.get(index)?;
        matches!(dvc_header_kind(header), DvcHeaderKind::Valid).then_some(header)
    }

    /// Whether the sender proves it never prepared the entry at `index`.
    #[must_use]
    pub fn nacks(&self, index: usize) -> bool {
        index < self.headers.len() && self.nack_bitset & (1u128 << index) != 0
    }

    /// Whether the sender can serve the body of the entry at `index`.
    #[must_use]
    pub fn offers_body(&self, index: usize) -> bool {
        index < self.headers.len() && self.present_bitset & (1u128 << index) != 0
    }
}

/// Stored information from a `DoViewChange` message.
#[derive(Debug, Clone)]
pub struct StoredDvc {
    pub replica: u8,
    /// The view when the replica's status was last normal.
    pub log_view: u32,
    pub op: u64,
    pub commit: u64,
    /// The sender's uncommitted suffix. Empty from a silent sender, which counts
    /// toward the quorum and `max(commit)` but neither nacks nor offers bodies.
    pub suffix: DvcSuffix,
}

/// Array type for storing DVC messages from all replicas.
pub type DvcQuorumArray = [Option<StoredDvc>; REPLICAS_MAX];

/// Create an empty DVC quorum array.
#[must_use]
pub fn dvc_quorum_array_empty() -> DvcQuorumArray {
    // `[None; REPLICAS_MAX]` needs `StoredDvc: Copy`, ruled out by the `Vec`.
    std::array::from_fn(|_| None)
}

/// Record a DVC in the array. Returns true if this is a new entry (not duplicate).
pub fn dvc_record(array: &mut DvcQuorumArray, dvc: StoredDvc) -> bool {
    let slot = &mut array[dvc.replica as usize];
    if slot.is_some() {
        return false; // Duplicate
    }
    *slot = Some(dvc);
    true
}

/// Count how many DVCs have been received.
#[must_use]
pub fn dvc_count(array: &DvcQuorumArray) -> usize {
    array.iter().filter(|m| m.is_some()).count()
}

/// Reset the DVC quorum array.
pub fn dvc_reset(array: &mut DvcQuorumArray) {
    *array = dvc_quorum_array_empty();
}

/// Iterator over all stored DVCs.
pub fn dvc_iter(array: &DvcQuorumArray) -> impl Iterator<Item = &StoredDvc> {
    array.iter().filter_map(|m| m.as_ref())
}

/// Why a `DoViewChange` body could not be read as a suffix.
///
/// Dropped whole rather than partially trusted: the merge indexes arithmetically
/// from the head op, so one bad offset misattributes a header, nack, or body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DvcSuffixError {
    /// Body length is not a whole number of headers.
    NotHeaderMultiple { body_len: usize },
    /// More entries than the bitsets can address.
    TooManyEntries { count: usize },
    /// An entry is not a valid `PrepareHeader` bit pattern.
    MalformedHeader { index: usize },
    /// Entries are not consecutive descending from the head op.
    OpOutOfOrder {
        index: usize,
        expected: u64,
        found: u64,
    },
    /// A bitset addresses an entry the body does not contain.
    BitsetBeyondSuffix { count: usize },
    /// An entry's identity checksum does not match its own contents.
    ChecksumMismatch { index: usize },
    /// A lower entry claims a timestamp at or after the entry above it.
    TimestampNotDecreasing { index: usize },
    /// Consecutive entries do not hash-chain.
    ChainBreak { index: usize },
}

impl std::fmt::Display for DvcSuffixError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotHeaderMultiple { body_len } => write!(
                f,
                "do_view_change body of {body_len} bytes is not a whole number of {} -byte headers",
                size_of::<PrepareHeader>()
            ),
            Self::TooManyEntries { count } => write!(
                f,
                "do_view_change suffix of {count} entries exceeds the maximum {DVC_HEADERS_MAX}"
            ),
            Self::MalformedHeader { index } => {
                write!(
                    f,
                    "do_view_change suffix entry {index} is not a prepare header"
                )
            }
            Self::OpOutOfOrder {
                index,
                expected,
                found,
            } => write!(
                f,
                "do_view_change suffix entry {index} carries op {found}, expected {expected}"
            ),
            Self::BitsetBeyondSuffix { count } => write!(
                f,
                "do_view_change bitset addresses an entry past the {count}-entry suffix"
            ),
            Self::ChecksumMismatch { index } => write!(
                f,
                "do_view_change suffix entry {index} does not match its own identity checksum"
            ),
            Self::TimestampNotDecreasing { index } => write!(
                f,
                "do_view_change suffix entry {index} does not predate the entry above it"
            ),
            Self::ChainBreak { index } => write!(
                f,
                "do_view_change suffix entry {index} does not chain to the entry above it"
            ),
        }
    }
}

impl std::error::Error for DvcSuffixError {}

/// Read a suffix out of a `DoViewChange` body.
///
/// `head_op` is the sender's `header.op`; entries run consecutively down from it,
/// blanks included, so slot `i` is unambiguously op `head_op - i`. An empty body
/// yields an empty suffix, which is what a sender with nothing to describe sends.
///
/// `body` must carry [`PrepareHeader`]'s alignment: the cast below is checked, so an
/// unaligned body reports every entry as [`DvcSuffixError::MalformedHeader`] instead
/// of what is actually wrong. Real frames clear this because the body starts a whole
/// number of 256-byte headers into an aligned buffer; the debug assert catches a
/// hand-built one.
///
/// # Errors
/// [`DvcSuffixError`] when the body is not a consecutive run of valid prepare
/// headers descending from `head_op`, or a bitset addresses a missing entry.
pub fn dvc_suffix_decode(
    body: &[u8],
    head_op: u64,
    nack_bitset: u128,
    present_bitset: u128,
) -> Result<DvcSuffix, DvcSuffixError> {
    debug_assert!(
        body.is_empty()
            || body
                .as_ptr()
                .addr()
                .is_multiple_of(align_of::<PrepareHeader>()),
        "suffix body must be aligned for PrepareHeader"
    );
    let header_size = size_of::<PrepareHeader>();
    if !body.len().is_multiple_of(header_size) {
        return Err(DvcSuffixError::NotHeaderMultiple {
            body_len: body.len(),
        });
    }
    let count = body.len() / header_size;
    if count > DVC_HEADERS_MAX {
        return Err(DvcSuffixError::TooManyEntries { count });
    }
    if count < DVC_HEADERS_MAX {
        let beyond = !((1u128 << count) - 1);
        if nack_bitset & beyond != 0 || present_bitset & beyond != 0 {
            return Err(DvcSuffixError::BitsetBeyondSuffix { count });
        }
    }

    let mut headers = Vec::with_capacity(count);
    // The entry above the current one, skipping blanks: high-to-low, so the child.
    let mut child: Option<PrepareHeader> = None;
    for index in 0..count {
        let chunk = &body[index * header_size..(index + 1) * header_size];
        let header = bytemuck::checked::try_from_bytes::<PrepareHeader>(chunk)
            .map_err(|_| DvcSuffixError::MalformedHeader { index })?;
        // The cast proves the enums, not the reserved regions. A dirty reserved byte
        // with `checksum == 0` reads as Valid here (blanks are classified by exact
        // struct equality) AND skips the identity recompute below, so it conflicts
        // with every honest header at that op and no canonical one can be picked.
        header
            .validate()
            .map_err(|_| DvcSuffixError::MalformedHeader { index })?;
        let expected = head_op
            .checked_sub(index as u64)
            .ok_or(DvcSuffixError::OpOutOfOrder {
                index,
                expected: 0,
                found: header.op,
            })?;
        if header.op != expected {
            return Err(DvcSuffixError::OpOutOfOrder {
                index,
                expected,
                found: header.op,
            });
        }

        if matches!(dvc_header_kind(header), DvcHeaderKind::Valid) {
            // Recompute rather than trust the field. Otherwise one bit flipped in
            // transit becomes a canonical header no replica holds, honest senders
            // read as disagreeing, and a corrupted frame turns into a nack quorum
            // against a committed op. The on-disk sentinel is skipped: a suffix is read
            // out of the journal, which may hold entries a pre-seal build wrote, and
            // partition-plane prepares are unsealed by construction.
            if header.checksum != CHECKSUM_UNSEALED && header.identity_checksum() != header.checksum
            {
                return Err(DvcSuffixError::ChecksumMismatch { index });
            }
            if let Some(child) = child {
                // Timestamps never run forwards down the log, and consecutive entries
                // hash-chain. A frame breaking either describes a log that cannot exist.
                //
                // `view` is NOT checked. Monotone along one log, but a suffix is two:
                // `build_dvc_suffix` stitches the journal over the adopted view's
                // headers, and `restamp_prepare_view` rewrites `view` in place, so a
                // held op keeps whichever view delivered it. A hole below one is enough
                // to make an honest sender's suffix look regressed, and rejecting drops
                // its vote forever (the retransmit is byte-identical). Nothing reads a
                // suffix entry's `view`; the merge ranks by the message's `log_view`.
                if header.timestamp >= child.timestamp {
                    return Err(DvcSuffixError::TimestampNotDecreasing { index });
                }
                if header.op + 1 == child.op
                    && header.checksum != CHECKSUM_UNSEALED
                    && child.parent != header.checksum
                {
                    return Err(DvcSuffixError::ChainBreak { index });
                }
            }
            child = Some(*header);
        }
        headers.push(*header);
    }

    Ok(DvcSuffix::new(headers, nack_bitset, present_bitset))
}
