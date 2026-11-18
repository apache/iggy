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

//! The self-describing fresh object: an index that says whose batches the
//! object holds and which earlier batches committed at which offsets, then
//! the messages of its batches.
//!
//! All integers are little endian.
//!
//! ```text
//! [header 64][directory P x 72][batches B x 40][proofs K x 56][checksum 8][data]
//!
//! header:
//! [0..8]    magic                 "IGGYFRS2"
//! [8..24]   object_id             the id that the object's key carries
//! [24..40]  cluster_uid           StoreIdentity::cluster_uid of the writer
//! [40..48]  sealed_at             the writer's wall clock at seal, in
//!                                 microseconds since the Unix epoch
//! [48..52]  partitions            P
//! [52..56]  batches               B
//! [56..60]  proofs                K
//! [60..64]  flags                 bit 0: confirmation only; others zero
//!
//! directory row, one per partition with a batch or a proof in the object,
//! ascending by namespace:
//! [0..8]    namespace             IggyNamespace, raw
//! [8..16]   created_revision      the partition's incarnation
//! [16..20]  view                  the partition's consensus view at seal
//! [20..24]  first_batch           its first row of the batch table
//! [24..28]  batch_count
//! [28..32]  first_proof           its first row of the proof table
//! [32..36]  proof_count
//! [36..40]  messages              the messages of its batches
//! [40..48]  data_position         where its blobs start in the object
//! [48..56]  data_length           the bytes of its blobs
//! [56..64]  claimed_first_offset  the offset the writer expects its first
//!                                 message to commit at
//! [64..72]  proven_below          as in the placements of its batches
//!
//! batch row, one per batch, grouped by directory row in the order the
//! batches came:
//! [0..4]    message_count
//! [4..8]    blob_length
//! [8..16]   origin_timestamp
//! [16..24]  blob_checksum         XXH3 of the blob, as its placement says
//! [24..32]  producer_id           zero without a producer
//! [32..34]  producer_epoch        zero without a producer
//! [34]      producer_flags        bit 0: a producer is present; others zero
//! [35]      zero
//! [36..40]  base_sequence         zero without a producer
//!
//! proof row, grouped by directory row, each row of a partition starting at
//! the offset where the one before it ends:
//! [0..16]   object_id             the object that holds the batch's blob
//! [16..24]  position              where the blob starts in that object
//! [24..32]  base_offset           of the committed batch
//! [32..40]  base_timestamp        of the committed batch
//! [40..44]  message_count
//! [44..48]  zero
//! [48..56]  placed_checksum       the checksum of the committed placed batch
//!
//! checksum: CRC32C of every byte of the index before it, then four zero
//! bytes.
//! data: the blobs of each directory row back to back, rows in order.
//! ```
//!
//! A batch's blob starts at its directory row's `data_position` plus the
//! lengths of the blobs before it in the row.
//!
//! A partition mints the offset and the timestamp of a batch when it
//! prepares the batch, after the object is stored, and can still refuse the
//! batch then. So the claimed offsets of a directory row are a prediction,
//! and only a proof row binds a batch to where it committed. A proof row is
//! written after its batch commits, so it names a batch of an earlier
//! object, never one of its own.
//!
//! An object with no batch confirms proofs alone and holds no data.
//!
//! The checksum is CRC32C, as the manifest's is, so the crate needs no
//! second hash. It takes eight bytes so that the data starts at a multiple
//! of eight.
//!
//! A reader fetches the index with a ranged read of a guessed length, and
//! [`FreshIndex::decode`] says how much more it needs: the header names the
//! length of the whole index. The counts of rows are capped, so an index is
//! never longer than [`FRESH_INDEX_BYTES_MAX`]. A writer keeps the data of an
//! object within [`FRESH_OBJECT_DATA_BYTES_MAX`] unless one batch alone is
//! longer, since a batch is never split. A reader fetches the data by
//! ranges and never holds it whole, so it does not refuse an object for its
//! size.
//!
//! Every byte has one meaning: a reader refuses an index whose rows do not
//! add up, are out of order, or carry bytes where the layout has zeros, so a
//! damaged or foreign index is never read in part.

use std::ops::Range;

use bytes::{BufMut, Bytes, BytesMut};

use crate::fresh::FreshObjectId;

/// Size of the header at the start of the index.
pub const FRESH_HEADER_SIZE: usize = 64;

/// Size of a row of the directory: one per partition.
pub const FRESH_DIRECTORY_ROW_SIZE: usize = 72;

/// Size of a row of the batch table.
pub const FRESH_BATCH_ROW_SIZE: usize = 40;

/// Size of a row of the proof table.
pub const FRESH_PROOF_ROW_SIZE: usize = 56;

/// Size of the checksum at the end of the index.
pub const FRESH_INDEX_CHECKSUM_SIZE: usize = 8;

/// The most batch rows of one object.
pub const FRESH_OBJECT_BATCHES_MAX: u32 = 16_384;

/// The most proof rows of one object.
pub const FRESH_OBJECT_PROOFS_MAX: u32 = 16_384;

/// The most bytes of data of an object with more than one batch.
pub const FRESH_OBJECT_DATA_BYTES_MAX: u64 = 4 * 1024 * 1024;

/// The longest index, which a reader can fetch without knowing its length.
pub const FRESH_INDEX_BYTES_MAX: u64 = 4 * 1024 * 1024;

const _: () = assert!(
    FreshIndex::len_for(
        FRESH_OBJECT_BATCHES_MAX + FRESH_OBJECT_PROOFS_MAX,
        FRESH_OBJECT_BATCHES_MAX,
        FRESH_OBJECT_PROOFS_MAX,
    ) <= FRESH_INDEX_BYTES_MAX
);

pub const MAGIC: &[u8; 8] = b"IGGYFRS2";
const CONFIRMATION_ONLY: u32 = 1;
const PRODUCER_PRESENT: u8 = 1;

/// Who wrote an object, and when.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FreshObjectHeader {
    /// The id that the object's key carries.
    pub object_id: FreshObjectId,
    /// `StoreIdentity::cluster_uid` of the cluster that wrote the object.
    pub cluster_uid: u128,
    /// The writer's wall clock at seal, in microseconds since the Unix epoch.
    pub sealed_at: u64,
}

/// The idempotent producer of a batch, as the batch header carries it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FreshProducer {
    pub producer_id: u64,
    pub epoch: u16,
    pub base_sequence: u32,
}

/// One partition's part of an object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FreshDirectoryRow {
    /// The partition's `IggyNamespace`, raw.
    pub namespace: u64,
    /// The partition's incarnation, as in [`crate::ObjectPrefix`].
    pub created_revision: u64,
    /// The partition's consensus view when the object was sealed.
    pub view: u32,
    /// Its first row of the batch table.
    pub first_batch: u32,
    pub batch_count: u32,
    /// Its first row of the proof table.
    pub first_proof: u32,
    pub proof_count: u32,
    /// The messages of its batches.
    pub messages: u32,
    /// Where its blobs start in the object.
    pub data_position: u64,
    /// The bytes of its blobs.
    pub data_length: u64,
    /// The offset the writer expects the first message of its batches to
    /// commit at. Only recovery and diagnostics read it: a proof row says
    /// where a batch committed.
    pub claimed_first_offset: u64,
    /// As the placements of its batches say: every committed batch of the
    /// partition below it has a proof row in a stored object, or is held by
    /// a long-term segment that the partition committed.
    pub proven_below: u64,
}

/// One batch whose blob the object holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FreshBatchRow {
    pub message_count: u32,
    pub blob_length: u32,
    /// As the batch header carries it.
    pub origin_timestamp: u64,
    /// XXH3 of the blob, as its placement records it.
    pub blob_checksum: u64,
    pub producer: Option<FreshProducer>,
}

/// One committed batch, bound to where it committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct FreshProofRow {
    /// The object that holds the batch's blob, an earlier one.
    pub object_id: FreshObjectId,
    /// Where the blob starts in that object.
    pub position: u64,
    pub base_offset: u64,
    pub base_timestamp: u64,
    pub message_count: u32,
    /// The checksum of the placed batch the partition committed, which the
    /// batch rebuilt from the rows must carry.
    pub placed_checksum: u64,
}

/// A batch that a writer puts in an object, with its blob.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreshBatch<B> {
    pub message_count: u32,
    pub origin_timestamp: u64,
    /// XXH3 of `blob`, which the writer computed for its placement.
    pub blob_checksum: u64,
    pub producer: Option<FreshProducer>,
    pub blob: B,
}

/// What a writer puts in an object for one partition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreshPartition<B> {
    pub namespace: u64,
    pub created_revision: u64,
    pub view: u32,
    pub claimed_first_offset: u64,
    pub proven_below: u64,
    /// In the order they came.
    pub batches: Vec<FreshBatch<B>>,
    /// Ascending, each starting at the offset where the one before it ends.
    pub proofs: Vec<FreshProofRow>,
}

/// The decoded index of an object.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FreshIndex {
    header: FreshObjectHeader,
    directory: Vec<FreshDirectoryRow>,
    batches: Vec<FreshBatchRow>,
    proofs: Vec<FreshProofRow>,
    /// Where the blob of each batch row starts in the object.
    positions: Vec<u64>,
    index_len: u64,
    object_len: u64,
}

/// An object as it goes to the store, and its index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealedFreshObject {
    pub bytes: Bytes,
    /// What a reader decodes of [`Self::bytes`]. Its
    /// [`FreshIndex::positions`] say where each batch's blob starts, in the
    /// order the batches were given.
    pub index: FreshIndex,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FreshObjectError {
    /// Not an error of the object: read the bytes `have..need` of it and
    /// decode again.
    #[error("the index takes the first {need} bytes of the object, and {have} are read")]
    Incomplete { have: u64, need: u64 },
    #[error("the object starts with {0:?}, the magic of no index this build reads")]
    UnknownMagic([u8; 8]),
    #[error("the object sets flags {0:#x}, of which this build does not know some")]
    UnknownFlags(u32),
    #[error(
        "{partitions} partitions, {batches} batches and {proofs} proofs are outside the limits of an object"
    )]
    RowCounts {
        partitions: u32,
        batches: u32,
        proofs: u32,
    },
    #[error("the index checksum is {stored:#018x}, and its bytes hash to {computed:#018x}")]
    ChecksumMismatch { stored: u64, computed: u64 },
    #[error("the object names itself {found:?}, and was read under the key of {expected:?}")]
    WrongObject {
        expected: FreshObjectId,
        found: FreshObjectId,
    },
    #[error(
        "the object's confirmation flag is {confirmation_only}, and it holds {batches} batches"
    )]
    ConfirmationFlag {
        confirmation_only: bool,
        batches: u32,
    },
    #[error("the directory row of namespace {namespace} does not follow the one before it")]
    PartitionOutOfOrder { namespace: u64 },
    #[error("the directory row of namespace {namespace} has neither a batch nor a proof")]
    EmptyPartition { namespace: u64 },
    #[error("the rows of namespace {namespace} do not follow the rows before them")]
    RowRange { namespace: u64 },
    #[error("the data of namespace {namespace} does not match its batches")]
    DataRange { namespace: u64 },
    #[error("the messages of namespace {namespace} do not match its batches or its claim")]
    MessageCount { namespace: u64 },
    #[error("batch row {row} is not a valid batch")]
    InvalidBatch { row: u32 },
    #[error("proof row {row} is not a valid proof")]
    InvalidProof { row: u32 },
    #[error("the proofs of namespace {namespace} skip or repeat offsets at {base_offset}")]
    ProofGap { namespace: u64, base_offset: u64 },
    #[error("a proof of namespace {namespace} names the object that holds it")]
    SelfProof { namespace: u64 },
    #[error("some batch or proof rows belong to no partition")]
    UnclaimedRows,
    #[error("a blob of {length} bytes of namespace {namespace} is too long for a batch row")]
    BlobTooLong { namespace: u64, length: u64 },
    #[error("{length} bytes of data in more than one batch are over the limit of an object")]
    DataTooLong { length: u64 },
}

impl FreshDirectoryRow {
    /// The rows of its batches in the batch table.
    #[must_use]
    pub fn batch_range(&self) -> Range<usize> {
        span(self.first_batch, self.batch_count)
    }

    /// The rows of its proofs in the proof table.
    #[must_use]
    pub fn proof_range(&self) -> Range<usize> {
        span(self.first_proof, self.proof_count)
    }

    /// The offsets the writer expects its batches to commit at.
    #[must_use]
    pub const fn claimed_offsets(&self) -> Range<u64> {
        self.claimed_first_offset
            ..self
                .claimed_first_offset
                .saturating_add(self.messages as u64)
    }

    fn read(fields: &mut Fields<'_>) -> Self {
        Self {
            namespace: fields.u64(),
            created_revision: fields.u64(),
            view: fields.u32(),
            first_batch: fields.u32(),
            batch_count: fields.u32(),
            first_proof: fields.u32(),
            proof_count: fields.u32(),
            messages: fields.u32(),
            data_position: fields.u64(),
            data_length: fields.u64(),
            claimed_first_offset: fields.u64(),
            proven_below: fields.u64(),
        }
    }

    fn write(&self, out: &mut BytesMut) {
        out.put_u64_le(self.namespace);
        out.put_u64_le(self.created_revision);
        out.put_u32_le(self.view);
        out.put_u32_le(self.first_batch);
        out.put_u32_le(self.batch_count);
        out.put_u32_le(self.first_proof);
        out.put_u32_le(self.proof_count);
        out.put_u32_le(self.messages);
        out.put_u64_le(self.data_position);
        out.put_u64_le(self.data_length);
        out.put_u64_le(self.claimed_first_offset);
        out.put_u64_le(self.proven_below);
    }
}

impl FreshProducer {
    /// The bytes of a batch row without a producer.
    const ABSENT: Self = Self {
        producer_id: 0,
        epoch: 0,
        base_sequence: 0,
    };
}

impl FreshBatchRow {
    /// `None` for a producer section that is not one of the two the layout
    /// allows.
    fn read(fields: &mut Fields<'_>) -> Option<Self> {
        let message_count = fields.u32();
        let blob_length = fields.u32();
        let origin_timestamp = fields.u64();
        let blob_checksum = fields.u64();
        let producer_id = fields.u64();
        let epoch = fields.u16();
        let flags = fields.u8();
        let zero = fields.u8();
        let producer = FreshProducer {
            producer_id,
            epoch,
            base_sequence: fields.u32(),
        };
        let producer = match flags {
            PRODUCER_PRESENT => Some(producer),
            0 if producer == FreshProducer::ABSENT => None,
            _ => return None,
        };
        (zero == 0).then_some(Self {
            message_count,
            blob_length,
            origin_timestamp,
            blob_checksum,
            producer,
        })
    }

    fn write(&self, out: &mut BytesMut) {
        out.put_u32_le(self.message_count);
        out.put_u32_le(self.blob_length);
        out.put_u64_le(self.origin_timestamp);
        out.put_u64_le(self.blob_checksum);
        let (producer, flags) = self
            .producer
            .map_or((FreshProducer::ABSENT, 0), |producer| {
                (producer, PRODUCER_PRESENT)
            });
        out.put_u64_le(producer.producer_id);
        out.put_u16_le(producer.epoch);
        out.put_u8(flags);
        out.put_u8(0);
        out.put_u32_le(producer.base_sequence);
    }
}

impl FreshProofRow {
    /// One past the last offset of the batch.
    #[must_use]
    pub const fn end_offset(&self) -> Option<u64> {
        self.base_offset.checked_add(self.message_count as u64)
    }

    /// `None` for bytes where the layout has zeros.
    fn read(fields: &mut Fields<'_>) -> Option<Self> {
        let object_id = FreshObjectId::from_raw(fields.u128());
        let position = fields.u64();
        let base_offset = fields.u64();
        let base_timestamp = fields.u64();
        let message_count = fields.u32();
        let zero = fields.u32();
        let placed_checksum = fields.u64();
        (zero == 0).then_some(Self {
            object_id,
            position,
            base_offset,
            base_timestamp,
            message_count,
            placed_checksum,
        })
    }

    fn write(&self, out: &mut BytesMut) {
        out.put_u128_le(self.object_id.get());
        out.put_u64_le(self.position);
        out.put_u64_le(self.base_offset);
        out.put_u64_le(self.base_timestamp);
        out.put_u32_le(self.message_count);
        out.put_u32_le(0);
        out.put_u64_le(self.placed_checksum);
    }
}

impl FreshIndex {
    /// The length of the index of an object with these counts of rows,
    /// which is where its data starts.
    #[must_use]
    pub const fn len_for(partitions: u32, batches: u32, proofs: u32) -> u64 {
        (FRESH_HEADER_SIZE + FRESH_INDEX_CHECKSUM_SIZE) as u64
            + partitions as u64 * FRESH_DIRECTORY_ROW_SIZE as u64
            + batches as u64 * FRESH_BATCH_ROW_SIZE as u64
            + proofs as u64 * FRESH_PROOF_ROW_SIZE as u64
    }

    /// The index in `prefix`, the first bytes of the object read under the
    /// key of `object_id`. Bytes past the index are ignored.
    ///
    /// # Errors
    /// [`FreshObjectError::Incomplete`] when `prefix` is too short for the
    /// header or, once the header is read, for the whole index: read up to
    /// its `need` and decode again. A wrong magic is refused before the
    /// header is whole. Any other [`FreshObjectError`] refuses the object for
    /// good.
    pub fn decode(prefix: &[u8], object_id: FreshObjectId) -> Result<Self, FreshObjectError> {
        let have = prefix.len() as u64;
        if let Some(magic) = prefix.first_chunk::<8>()
            && magic != MAGIC
        {
            return Err(FreshObjectError::UnknownMagic(*magic));
        }
        let Some(header) = prefix.first_chunk::<FRESH_HEADER_SIZE>() else {
            return Err(FreshObjectError::Incomplete {
                have,
                need: FRESH_HEADER_SIZE as u64,
            });
        };
        let mut fields = Fields(&header[MAGIC.len()..]);
        let header = FreshObjectHeader {
            object_id: FreshObjectId::from_raw(fields.u128()),
            cluster_uid: fields.u128(),
            sealed_at: fields.u64(),
        };
        let (partitions, batches, proofs, flags) =
            (fields.u32(), fields.u32(), fields.u32(), fields.u32());
        if flags & !CONFIRMATION_ONLY != 0 {
            return Err(FreshObjectError::UnknownFlags(flags));
        }
        check_counts(partitions, batches, proofs)?;
        let index_len = Self::len_for(partitions, batches, proofs);
        let Some(index) = usize::try_from(index_len)
            .ok()
            .and_then(|len| prefix.get(..len))
        else {
            return Err(FreshObjectError::Incomplete {
                have,
                need: index_len,
            });
        };
        let (body, checksum) = index.split_at(index.len() - FRESH_INDEX_CHECKSUM_SIZE);
        let stored = Fields(checksum).u64();
        let computed = u64::from(crc32c::crc32c(body));
        if stored != computed {
            return Err(FreshObjectError::ChecksumMismatch { stored, computed });
        }
        if header.object_id != object_id {
            return Err(FreshObjectError::WrongObject {
                expected: object_id,
                found: header.object_id,
            });
        }
        let mut rows = Fields(&body[FRESH_HEADER_SIZE..]);
        let directory = (0..partitions)
            .map(|_| FreshDirectoryRow::read(&mut rows))
            .collect();
        let batches = (0..batches)
            .map(|row| FreshBatchRow::read(&mut rows).ok_or(FreshObjectError::InvalidBatch { row }))
            .collect::<Result<_, _>>()?;
        let proofs = (0..proofs)
            .map(|row| FreshProofRow::read(&mut rows).ok_or(FreshObjectError::InvalidProof { row }))
            .collect::<Result<_, _>>()?;
        Self::assemble(
            header,
            flags & CONFIRMATION_ONLY != 0,
            index_len,
            directory,
            batches,
            proofs,
        )
    }

    #[must_use]
    pub const fn header(&self) -> &FreshObjectHeader {
        &self.header
    }

    /// Whether the object only confirms proofs, with no batch and no data.
    #[must_use]
    pub const fn is_confirmation(&self) -> bool {
        self.batches.is_empty()
    }

    /// Where the data starts.
    #[must_use]
    pub const fn index_len(&self) -> u64 {
        self.index_len
    }

    /// The length of the whole object.
    #[must_use]
    pub const fn object_len(&self) -> u64 {
        self.object_len
    }

    /// Ascending by namespace.
    #[must_use]
    pub fn directory(&self) -> &[FreshDirectoryRow] {
        &self.directory
    }

    #[must_use]
    pub fn batches(&self) -> &[FreshBatchRow] {
        &self.batches
    }

    #[must_use]
    pub fn proofs(&self) -> &[FreshProofRow] {
        &self.proofs
    }

    /// Where the blob of each row of [`Self::batches`] starts.
    #[must_use]
    pub fn positions(&self) -> &[u64] {
        &self.positions
    }

    /// The directory row of `namespace`.
    #[must_use]
    pub fn partition(&self, namespace: u64) -> Option<&FreshDirectoryRow> {
        self.directory
            .binary_search_by_key(&namespace, |row| row.namespace)
            .ok()
            .and_then(|found| self.directory.get(found))
    }

    /// The batches of `row`, each with where its blob starts.
    pub fn batches_of(
        &self,
        row: &FreshDirectoryRow,
    ) -> impl Iterator<Item = (u64, &FreshBatchRow)> {
        let range = row.batch_range();
        let positions = self.positions.get(range.clone()).unwrap_or_default();
        let batches = self.batches.get(range).unwrap_or_default();
        positions.iter().copied().zip(batches)
    }

    #[must_use]
    pub fn proofs_of(&self, row: &FreshDirectoryRow) -> &[FreshProofRow] {
        self.proofs.get(row.proof_range()).unwrap_or_default()
    }

    /// The batch of `namespace` whose blob starts at `position`, with its
    /// partition's directory row.
    #[must_use]
    pub fn batch_at(
        &self,
        namespace: u64,
        position: u64,
    ) -> Option<(&FreshDirectoryRow, &FreshBatchRow)> {
        let row = self.partition(namespace)?;
        let range = row.batch_range();
        let found = self
            .positions
            .get(range.clone())?
            .binary_search(&position)
            .ok()?;
        Some((row, self.batches.get(range.start + found)?))
    }

    /// The index of these rows, once they are found to add up. What the
    /// writer derives is checked as well, so a writer cannot seal what a
    /// reader refuses.
    fn assemble(
        header: FreshObjectHeader,
        confirmation_only: bool,
        index_len: u64,
        directory: Vec<FreshDirectoryRow>,
        batches: Vec<FreshBatchRow>,
        proofs: Vec<FreshProofRow>,
    ) -> Result<Self, FreshObjectError> {
        if confirmation_only != batches.is_empty() {
            return Err(FreshObjectError::ConfirmationFlag {
                confirmation_only,
                batches: count(batches.len()),
            });
        }
        let mut positions = Vec::with_capacity(batches.len());
        let (mut next_batch, mut next_proof, mut next_data) = (0, 0, index_len);
        let mut previous = None;
        for row in &directory {
            let namespace = row.namespace;
            if previous.is_some_and(|previous| previous >= namespace) {
                return Err(FreshObjectError::PartitionOutOfOrder { namespace });
            }
            previous = Some(namespace);
            if row.batch_count == 0 && row.proof_count == 0 {
                return Err(FreshObjectError::EmptyPartition { namespace });
            }
            let (batch_range, proof_range) = (row.batch_range(), row.proof_range());
            let (Some(row_batches), Some(row_proofs)) = (
                batches.get(batch_range.clone()),
                proofs.get(proof_range.clone()),
            ) else {
                return Err(FreshObjectError::RowRange { namespace });
            };
            if batch_range.start != next_batch || proof_range.start != next_proof {
                return Err(FreshObjectError::RowRange { namespace });
            }
            if row.data_position != next_data {
                return Err(FreshObjectError::DataRange { namespace });
            }
            let mut position = row.data_position;
            let mut messages = 0_u32;
            for (row_index, batch) in batch_range.clone().zip(row_batches) {
                if batch.message_count == 0 || batch.blob_length == 0 {
                    return Err(FreshObjectError::InvalidBatch {
                        row: count(row_index),
                    });
                }
                positions.push(position);
                position = position
                    .checked_add(u64::from(batch.blob_length))
                    .ok_or(FreshObjectError::DataRange { namespace })?;
                messages = messages
                    .checked_add(batch.message_count)
                    .ok_or(FreshObjectError::MessageCount { namespace })?;
            }
            if position - row.data_position != row.data_length {
                return Err(FreshObjectError::DataRange { namespace });
            }
            if messages != row.messages
                || row
                    .claimed_first_offset
                    .checked_add(u64::from(messages))
                    .is_none()
            {
                return Err(FreshObjectError::MessageCount { namespace });
            }
            let mut next_offset = None;
            for (row_index, proof) in proof_range.clone().zip(row_proofs) {
                let end_offset = proof
                    .end_offset()
                    .filter(|_| proof.message_count > 0)
                    .ok_or_else(|| FreshObjectError::InvalidProof {
                        row: count(row_index),
                    })?;
                if proof.object_id == header.object_id {
                    return Err(FreshObjectError::SelfProof { namespace });
                }
                if next_offset.is_some_and(|next_offset| next_offset != proof.base_offset) {
                    return Err(FreshObjectError::ProofGap {
                        namespace,
                        base_offset: proof.base_offset,
                    });
                }
                next_offset = Some(end_offset);
            }
            next_batch = batch_range.end;
            next_proof = proof_range.end;
            next_data = position;
        }
        if next_batch != batches.len() || next_proof != proofs.len() {
            return Err(FreshObjectError::UnclaimedRows);
        }
        Ok(Self {
            header,
            directory,
            batches,
            proofs,
            positions,
            index_len,
            object_len: next_data,
        })
    }

    fn write(&self, out: &mut BytesMut) {
        let start = out.len();
        out.put_slice(MAGIC);
        out.put_u128_le(self.header.object_id.get());
        out.put_u128_le(self.header.cluster_uid);
        out.put_u64_le(self.header.sealed_at);
        out.put_u32_le(count(self.directory.len()));
        out.put_u32_le(count(self.batches.len()));
        out.put_u32_le(count(self.proofs.len()));
        out.put_u32_le(if self.is_confirmation() {
            CONFIRMATION_ONLY
        } else {
            0
        });
        for row in &self.directory {
            row.write(out);
        }
        for batch in &self.batches {
            batch.write(out);
        }
        for proof in &self.proofs {
            proof.write(out);
        }
        let checksum = crc32c::crc32c(&out[start..]);
        out.put_u64_le(u64::from(checksum));
    }
}

impl SealedFreshObject {
    /// The object of `partitions` under `header`: its index, then the blobs
    /// copied once, in the order given.
    ///
    /// # Errors
    /// [`FreshObjectError::RowCounts`] for no partition or for more rows than
    /// an object holds, [`FreshObjectError::BlobTooLong`] for a blob whose
    /// length does not fit its row, [`FreshObjectError::DataTooLong`] for
    /// more than [`FRESH_OBJECT_DATA_BYTES_MAX`] bytes of data in more than
    /// one batch, and whatever [`FreshIndex::decode`] would refuse of the
    /// rows: partitions out of order or with neither batch nor proof, a
    /// batch without messages, or proofs that skip offsets or name the
    /// object itself.
    pub fn seal<B: AsRef<[u8]>>(
        header: FreshObjectHeader,
        partitions: &[FreshPartition<B>],
    ) -> Result<Self, FreshObjectError> {
        let batch_count: usize = partitions
            .iter()
            .map(|partition| partition.batches.len())
            .sum();
        let proof_count: usize = partitions
            .iter()
            .map(|partition| partition.proofs.len())
            .sum();
        let counts = (
            count(partitions.len()),
            count(batch_count),
            count(proof_count),
        );
        check_counts(counts.0, counts.1, counts.2)?;
        let index_len = FreshIndex::len_for(counts.0, counts.1, counts.2);
        let mut directory = Vec::with_capacity(partitions.len());
        let mut batches = Vec::with_capacity(batch_count);
        let mut proofs = Vec::with_capacity(proof_count);
        let mut data_position = index_len;
        for partition in partitions {
            let namespace = partition.namespace;
            let first_batch = count(batches.len());
            let mut data_length = 0;
            let mut messages = 0_u32;
            for batch in &partition.batches {
                let length = batch.blob.as_ref().len() as u64;
                let blob_length = u32::try_from(length)
                    .map_err(|_| FreshObjectError::BlobTooLong { namespace, length })?;
                messages = messages
                    .checked_add(batch.message_count)
                    .ok_or(FreshObjectError::MessageCount { namespace })?;
                data_length += length;
                batches.push(FreshBatchRow {
                    message_count: batch.message_count,
                    blob_length,
                    origin_timestamp: batch.origin_timestamp,
                    blob_checksum: batch.blob_checksum,
                    producer: batch.producer,
                });
            }
            let first_proof = count(proofs.len());
            proofs.extend_from_slice(&partition.proofs);
            directory.push(FreshDirectoryRow {
                namespace,
                created_revision: partition.created_revision,
                view: partition.view,
                first_batch,
                batch_count: count(partition.batches.len()),
                first_proof,
                proof_count: count(partition.proofs.len()),
                messages,
                data_position,
                data_length,
                claimed_first_offset: partition.claimed_first_offset,
                proven_below: partition.proven_below,
            });
            data_position += data_length;
        }
        let length = data_position - index_len;
        if length > FRESH_OBJECT_DATA_BYTES_MAX && batch_count > 1 {
            return Err(FreshObjectError::DataTooLong { length });
        }
        let index = FreshIndex::assemble(
            header,
            batch_count == 0,
            index_len,
            directory,
            batches,
            proofs,
        )?;
        let capacity =
            usize::try_from(data_position).map_err(|_| FreshObjectError::DataTooLong { length })?;
        let mut bytes = BytesMut::with_capacity(capacity);
        index.write(&mut bytes);
        for batch in partitions.iter().flat_map(|partition| &partition.batches) {
            bytes.put_slice(batch.blob.as_ref());
        }
        Ok(Self {
            bytes: bytes.freeze(),
            index,
        })
    }
}

fn check_counts(partitions: u32, batches: u32, proofs: u32) -> Result<(), FreshObjectError> {
    // Every directory row has a batch or a proof, so there are never more
    // of them than rows.
    let rows = u64::from(batches) + u64::from(proofs);
    if batches > FRESH_OBJECT_BATCHES_MAX
        || proofs > FRESH_OBJECT_PROOFS_MAX
        || partitions == 0
        || u64::from(partitions) > rows
    {
        return Err(FreshObjectError::RowCounts {
            partitions,
            batches,
            proofs,
        });
    }
    Ok(())
}

/// A count of rows as the index writes it. The caps keep every count far
/// below `u32::MAX`, and one past them is refused all the same.
fn count(rows: usize) -> u32 {
    u32::try_from(rows).unwrap_or(u32::MAX)
}

fn span(first: u32, count: u32) -> Range<usize> {
    let first = usize::try_from(first).unwrap_or(usize::MAX);
    first..first.saturating_add(usize::try_from(count).unwrap_or(usize::MAX))
}

/// Reads fields off the front of bytes whose length the caller checked.
struct Fields<'a>(&'a [u8]);

impl Fields<'_> {
    fn take<const N: usize>(&mut self) -> [u8; N] {
        let bytes = self.0;
        bytes
            .split_first_chunk::<N>()
            .map_or([0; N], |(field, rest)| {
                self.0 = rest;
                *field
            })
    }

    fn u128(&mut self) -> u128 {
        u128::from_le_bytes(self.take())
    }

    fn u64(&mut self) -> u64 {
        u64::from_le_bytes(self.take())
    }

    fn u32(&mut self) -> u32 {
        u32::from_le_bytes(self.take())
    }

    fn u16(&mut self) -> u16 {
        u16::from_le_bytes(self.take())
    }

    fn u8(&mut self) -> u8 {
        u8::from_le_bytes(self.take())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fresh::FreshObject;

    const NOW: u64 = 1_700_000_000_000_000;
    const PRODUCER: FreshProducer = FreshProducer {
        producer_id: 77,
        epoch: 3,
        base_sequence: 1_000,
    };

    fn id(sequence: u64) -> FreshObjectId {
        FreshObjectId::new(1, 2, NOW, sequence).unwrap()
    }

    fn header() -> FreshObjectHeader {
        FreshObjectHeader {
            object_id: id(9),
            cluster_uid: 0x0123_4567_89ab_cdef_0011_2233_4455_6677,
            sealed_at: NOW + 123_456,
        }
    }

    fn batch(
        message_count: u32,
        blob: &'static [u8],
        producer: Option<FreshProducer>,
    ) -> FreshBatch<&'static [u8]> {
        FreshBatch {
            message_count,
            origin_timestamp: NOW + u64::from(message_count),
            blob_checksum: 0x5eed ^ blob.len() as u64,
            producer,
            blob,
        }
    }

    fn proof(object: u64, base_offset: u64, message_count: u32) -> FreshProofRow {
        FreshProofRow {
            object_id: id(object),
            position: 4096 + base_offset,
            base_offset,
            base_timestamp: NOW + base_offset,
            message_count,
            placed_checksum: 0xabcd ^ base_offset,
        }
    }

    fn partition(
        namespace: u64,
        batches: Vec<FreshBatch<&'static [u8]>>,
        proofs: Vec<FreshProofRow>,
    ) -> FreshPartition<&'static [u8]> {
        FreshPartition {
            namespace,
            created_revision: namespace * 100,
            view: 5,
            claimed_first_offset: namespace * 1_000,
            proven_below: namespace * 900,
            batches,
            proofs,
        }
    }

    /// Batches and proofs, batches alone, and proofs alone.
    fn partitions() -> Vec<FreshPartition<&'static [u8]>> {
        vec![
            partition(
                10,
                vec![
                    batch(2, b"first blob", None),
                    batch(1, b"second", Some(PRODUCER)),
                ],
                vec![proof(7, 90, 4), proof(8, 94, 2)],
            ),
            partition(20, vec![batch(3, b"third blob!", None)], Vec::new()),
            partition(30, Vec::new(), vec![proof(8, 500, 1), proof(8, 501, 6)]),
        ]
    }

    fn sealed() -> SealedFreshObject {
        SealedFreshObject::seal(header(), &partitions()).unwrap()
    }

    fn u32_at(bytes: &[u8], at: usize) -> u32 {
        u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap())
    }

    fn index_len_of(bytes: &[u8]) -> usize {
        let len = FreshIndex::len_for(u32_at(bytes, 48), u32_at(bytes, 52), u32_at(bytes, 56));
        usize::try_from(len).unwrap()
    }

    /// `bytes` with `value` at `at` and the index checksum renewed, so that
    /// only the rule under test can refuse them.
    fn tampered(bytes: &[u8], at: usize, value: &[u8]) -> Vec<u8> {
        let mut bytes = bytes.to_vec();
        bytes[at..at + value.len()].copy_from_slice(value);
        let end = index_len_of(&bytes);
        if let Some(index) = bytes.get_mut(..end) {
            let (body, checksum) = index.split_at_mut(end - FRESH_INDEX_CHECKSUM_SIZE);
            checksum.copy_from_slice(&u64::from(crc32c::crc32c(body)).to_le_bytes());
        }
        bytes
    }

    fn directory_row(row: usize) -> usize {
        FRESH_HEADER_SIZE + row * FRESH_DIRECTORY_ROW_SIZE
    }

    /// Where batch row `row` starts in the object of [`partitions`].
    fn batch_row(row: usize) -> usize {
        directory_row(3) + row * FRESH_BATCH_ROW_SIZE
    }

    /// Where proof row `row` starts in the object of [`partitions`].
    fn proof_row(row: usize) -> usize {
        batch_row(3) + row * FRESH_PROOF_ROW_SIZE
    }

    fn decoded(bytes: &[u8]) -> Result<FreshIndex, FreshObjectError> {
        FreshIndex::decode(bytes, id(9))
    }

    #[test]
    fn given_partitions_when_sealing_and_decoding_should_give_the_index_back() {
        let sealed = sealed();

        let index = decoded(&sealed.bytes).unwrap();

        assert_eq!(index, sealed.index);
        assert_eq!(index.header(), &header());
        assert!(!index.is_confirmation());
        assert_eq!(index.index_len(), FreshIndex::len_for(3, 3, 4));
        assert_eq!(index.object_len(), sealed.bytes.len() as u64);
        let blobs: Vec<&[u8]> = partitions()
            .iter()
            .flat_map(|partition| partition.batches.iter().map(|batch| batch.blob))
            .collect();
        assert_eq!(index.positions().len(), blobs.len());
        for (position, blob) in index.positions().iter().zip(&blobs) {
            let start = usize::try_from(*position).unwrap();
            assert_eq!(&sealed.bytes[start..start + blob.len()], *blob);
        }
        for (row, partition) in index.directory().iter().zip(partitions()) {
            assert_eq!(row.namespace, partition.namespace);
            assert_eq!(row.created_revision, partition.created_revision);
            assert_eq!(row.view, partition.view);
            assert_eq!(row.claimed_first_offset, partition.claimed_first_offset);
            assert_eq!(row.proven_below, partition.proven_below);
            assert_eq!(index.proofs_of(row), partition.proofs.as_slice());
            let batches: Vec<_> = index.batches_of(row).map(|(_, batch)| *batch).collect();
            let expected: Vec<_> = partition
                .batches
                .iter()
                .map(|batch| FreshBatchRow {
                    message_count: batch.message_count,
                    blob_length: u32::try_from(batch.blob.len()).unwrap(),
                    origin_timestamp: batch.origin_timestamp,
                    blob_checksum: batch.blob_checksum,
                    producer: batch.producer,
                })
                .collect();
            assert_eq!(batches, expected);
        }
        let first = index.partition(10).unwrap();
        assert_eq!(first.messages, 3);
        assert_eq!(first.claimed_offsets(), 10_000..10_003);
        assert_eq!(
            index.partition(30).unwrap().claimed_offsets(),
            30_000..30_000
        );
    }

    #[test]
    fn given_partitions_when_sealing_should_lay_out_the_bytes_as_the_format_says() {
        let sealed = sealed();
        let bytes = &sealed.bytes;
        let index_len = index_len_of(bytes);

        assert_eq!(&bytes[..8], b"IGGYFRS2");
        assert_eq!(bytes[8..24], id(9).get().to_le_bytes());
        assert_eq!(bytes[24..40], header().cluster_uid.to_le_bytes());
        assert_eq!(bytes[40..48], header().sealed_at.to_le_bytes());
        assert_eq!([48, 52, 56, 60].map(|at| u32_at(bytes, at)), [3, 3, 4, 0]);
        let second = directory_row(1);
        assert_eq!(bytes[second..second + 8], 20_u64.to_le_bytes());
        assert_eq!(u32_at(bytes, second + 20), 2, "first_batch");
        assert_eq!(u32_at(bytes, second + 28), 2, "first_proof");
        let with_producer = batch_row(1);
        assert_eq!(
            bytes[with_producer + 24..with_producer + 32],
            77_u64.to_le_bytes()
        );
        assert_eq!(bytes[with_producer + 34], 1, "producer flags");
        assert_eq!(u32_at(bytes, with_producer + 36), 1_000);
        let last_proof = proof_row(3);
        assert_eq!(
            bytes[last_proof..last_proof + 16],
            id(8).get().to_le_bytes()
        );
        assert_eq!(
            bytes[last_proof + 24..last_proof + 32],
            501_u64.to_le_bytes()
        );
        assert_eq!(proof_row(4) + FRESH_INDEX_CHECKSUM_SIZE, index_len);
        let checksum = u64::from(crc32c::crc32c(&bytes[..index_len - 8]));
        assert_eq!(bytes[index_len - 8..index_len], checksum.to_le_bytes());
        assert_eq!(&bytes[index_len..index_len + 10], b"first blob");
        assert_eq!(&bytes[bytes.len() - 11..], b"third blob!");
    }

    #[test]
    fn given_a_short_prefix_when_decoding_should_say_how_many_bytes_the_index_takes() {
        let bytes = sealed().bytes;
        let index_len = index_len_of(&bytes) as u64;

        for (have, need) in [
            (0, FRESH_HEADER_SIZE as u64),
            (FRESH_HEADER_SIZE as u64 - 1, FRESH_HEADER_SIZE as u64),
            (FRESH_HEADER_SIZE as u64, index_len),
            (index_len - 1, index_len),
        ] {
            let prefix = &bytes[..usize::try_from(have).unwrap()];

            assert_eq!(
                decoded(prefix),
                Err(FreshObjectError::Incomplete { have, need })
            );
        }
        let index = &bytes[..usize::try_from(index_len).unwrap()];
        assert_eq!(decoded(index), Ok(sealed().index));
    }

    #[test]
    fn given_another_magic_when_decoding_should_refuse_it_before_the_header_is_whole() {
        let mut object = FreshObject::new();
        object.push(b"messages");
        let first_version = object.into_bytes();

        assert_eq!(
            decoded(&first_version),
            Err(FreshObjectError::UnknownMagic(*b"IGGYFRS1"))
        );
        assert_eq!(
            decoded(b"IGGYFRS3"),
            Err(FreshObjectError::UnknownMagic(*b"IGGYFRS3"))
        );
        assert_eq!(
            decoded(b"IGGYFRS"),
            Err(FreshObjectError::Incomplete { have: 7, need: 64 })
        );
    }

    #[test]
    fn given_any_flipped_bit_of_the_index_when_decoding_should_refuse_the_object() {
        let bytes = sealed().bytes;

        for byte in 0..index_len_of(&bytes) {
            let mut damaged = bytes.to_vec();
            damaged[byte] ^= 1;
            assert!(
                decoded(&damaged).is_err(),
                "a flipped bit in byte {byte} went unnoticed"
            );
        }
    }

    #[test]
    fn given_an_object_read_under_the_key_of_another_when_decoding_should_refuse_it() {
        let bytes = sealed().bytes;

        assert_eq!(
            FreshIndex::decode(&bytes, id(10)),
            Err(FreshObjectError::WrongObject {
                expected: id(10),
                found: id(9),
            })
        );
    }

    #[test]
    fn given_proofs_alone_when_sealing_should_write_a_confirmation_without_data() {
        let proofs = vec![
            partition(10, Vec::new(), vec![proof(7, 90, 4)]),
            partition(30, Vec::new(), vec![proof(8, 500, 1)]),
        ];

        let sealed = SealedFreshObject::seal(header(), &proofs).unwrap();

        assert_eq!(u32_at(&sealed.bytes, 60), 1, "confirmation flag");
        assert_eq!(sealed.bytes.len() as u64, FreshIndex::len_for(2, 0, 2));
        let index = decoded(&sealed.bytes).unwrap();
        assert!(index.is_confirmation());
        assert_eq!(index.object_len(), index.index_len());
        assert_eq!(index.proofs(), [proof(7, 90, 4), proof(8, 500, 1)]);
    }

    #[test]
    fn given_a_confirmation_flag_that_disagrees_with_the_batches_when_decoding_should_refuse_it() {
        let data = sealed().bytes;
        let confirmation = SealedFreshObject::seal(
            header(),
            &[partition(10, Vec::new(), vec![proof(7, 90, 4)])],
        )
        .unwrap()
        .bytes;

        assert_eq!(
            decoded(&tampered(&data, 60, &1_u32.to_le_bytes())),
            Err(FreshObjectError::ConfirmationFlag {
                confirmation_only: true,
                batches: 3,
            })
        );
        assert_eq!(
            decoded(&tampered(&confirmation, 60, &0_u32.to_le_bytes())),
            Err(FreshObjectError::ConfirmationFlag {
                confirmation_only: false,
                batches: 0,
            })
        );
    }

    #[test]
    fn given_flags_this_build_does_not_know_when_decoding_should_refuse_them() {
        let bytes = sealed().bytes;

        for flags in [2_u32, 1 << 31, 3] {
            assert_eq!(
                decoded(&tampered(&bytes, 60, &flags.to_le_bytes())),
                Err(FreshObjectError::UnknownFlags(flags))
            );
        }
    }

    #[test]
    fn given_row_counts_outside_the_limits_when_decoding_should_refuse_them() {
        let bytes = sealed().bytes;

        for (partitions, batches, proofs) in [
            (3, FRESH_OBJECT_BATCHES_MAX + 1, 4),
            (3, 3, FRESH_OBJECT_PROOFS_MAX + 1),
            (0, 3, 4),
            (8, 3, 4),
        ] {
            let mut counts = Vec::new();
            for count in [partitions, batches, proofs] {
                counts.extend_from_slice(&count.to_le_bytes());
            }

            assert_eq!(
                decoded(&tampered(&bytes, 48, &counts)),
                Err(FreshObjectError::RowCounts {
                    partitions,
                    batches,
                    proofs,
                })
            );
        }
    }

    #[test]
    fn given_rows_at_the_caps_when_sealing_should_take_them_and_refuse_one_more() {
        let batches = |count: u32| {
            vec![partition(
                10,
                (0..count).map(|_| batch(1, b"x", None)).collect(),
                Vec::new(),
            )]
        };
        let proofs = |count: u32| {
            vec![partition(
                10,
                Vec::new(),
                (0..u64::from(count))
                    .map(|offset| proof(7, offset, 1))
                    .collect(),
            )]
        };

        let full = SealedFreshObject::seal(header(), &batches(FRESH_OBJECT_BATCHES_MAX)).unwrap();
        assert_eq!(decoded(&full.bytes), Ok(full.index));
        let full = SealedFreshObject::seal(header(), &proofs(FRESH_OBJECT_PROOFS_MAX)).unwrap();
        assert_eq!(decoded(&full.bytes), Ok(full.index));
        assert_eq!(
            SealedFreshObject::seal(header(), &batches(FRESH_OBJECT_BATCHES_MAX + 1)),
            Err(FreshObjectError::RowCounts {
                partitions: 1,
                batches: FRESH_OBJECT_BATCHES_MAX + 1,
                proofs: 0,
            })
        );
        assert_eq!(
            SealedFreshObject::seal(header(), &proofs(FRESH_OBJECT_PROOFS_MAX + 1)),
            Err(FreshObjectError::RowCounts {
                partitions: 1,
                batches: 0,
                proofs: FRESH_OBJECT_PROOFS_MAX + 1,
            })
        );
        assert_eq!(
            SealedFreshObject::seal::<&[u8]>(header(), &[]),
            Err(FreshObjectError::RowCounts {
                partitions: 0,
                batches: 0,
                proofs: 0,
            })
        );
    }

    /// An object of one partition with a batch of one message for each of
    /// `blobs`.
    fn of_blobs(blobs: Vec<&[u8]>) -> Result<SealedFreshObject, FreshObjectError> {
        let batches = blobs
            .into_iter()
            .map(|blob| FreshBatch {
                message_count: 1,
                origin_timestamp: NOW,
                blob_checksum: 1,
                producer: None,
                blob,
            })
            .collect();
        SealedFreshObject::seal(
            header(),
            &[FreshPartition {
                namespace: 10,
                created_revision: 1,
                view: 1,
                claimed_first_offset: 0,
                proven_below: 0,
                batches,
                proofs: Vec::new(),
            }],
        )
    }

    #[test]
    fn given_more_data_than_an_object_holds_when_sealing_should_refuse_it_unless_one_batch_holds_it()
     {
        let half = usize::try_from(FRESH_OBJECT_DATA_BYTES_MAX / 2).unwrap();
        let blob = vec![7_u8; 2 * half + 1];

        assert!(of_blobs(vec![&blob[..half], &blob[..half]]).is_ok());
        assert_eq!(
            of_blobs(vec![&blob[..half], &blob[..=half]]),
            Err(FreshObjectError::DataTooLong {
                length: FRESH_OBJECT_DATA_BYTES_MAX + 1,
            })
        );
        let alone = of_blobs(vec![&blob]).unwrap();
        assert_eq!(decoded(&alone.bytes), Ok(alone.index));
    }

    #[test]
    fn given_partitions_out_of_order_when_sealing_or_decoding_should_refuse_them() {
        let one = |namespace| partition(namespace, vec![batch(1, b"blob", None)], Vec::new());

        assert_eq!(
            SealedFreshObject::seal(header(), &[one(20), one(10)]),
            Err(FreshObjectError::PartitionOutOfOrder { namespace: 10 })
        );
        assert_eq!(
            SealedFreshObject::seal(header(), &[one(10), one(10)]),
            Err(FreshObjectError::PartitionOutOfOrder { namespace: 10 })
        );
        let bytes = sealed().bytes;
        assert_eq!(
            decoded(&tampered(&bytes, directory_row(1), &10_u64.to_le_bytes())),
            Err(FreshObjectError::PartitionOutOfOrder { namespace: 10 })
        );
    }

    #[test]
    fn given_a_partition_with_neither_batch_nor_proof_when_sealing_or_decoding_should_refuse_it() {
        let rows = [
            partition(
                10,
                vec![batch(1, b"a", None), batch(1, b"b", None)],
                Vec::new(),
            ),
            partition(20, Vec::new(), Vec::new()),
        ];

        assert_eq!(
            SealedFreshObject::seal(header(), &rows),
            Err(FreshObjectError::EmptyPartition { namespace: 20 })
        );
        let bytes = sealed().bytes;
        assert_eq!(
            decoded(&tampered(
                &bytes,
                directory_row(2) + 32,
                &0_u32.to_le_bytes()
            )),
            Err(FreshObjectError::EmptyPartition { namespace: 30 })
        );
    }

    #[test]
    fn given_directory_rows_that_do_not_add_up_when_decoding_should_refuse_them() {
        let bytes = sealed().bytes;
        let first = directory_row(0);
        let second = directory_row(1);
        let third = directory_row(2);
        let index_len = index_len_of(&bytes) as u64;

        for (at, value, refusal) in [
            (
                second + 20,
                1_u32.to_le_bytes().to_vec(),
                FreshObjectError::RowRange { namespace: 20 },
            ),
            (
                second + 24,
                u32::MAX.to_le_bytes().to_vec(),
                FreshObjectError::RowRange { namespace: 20 },
            ),
            (
                third + 28,
                3_u32.to_le_bytes().to_vec(),
                FreshObjectError::RowRange { namespace: 30 },
            ),
            (
                third + 28,
                1_u32.to_le_bytes().to_vec(),
                FreshObjectError::RowRange { namespace: 30 },
            ),
            (
                first + 40,
                (index_len + 1).to_le_bytes().to_vec(),
                FreshObjectError::DataRange { namespace: 10 },
            ),
            (
                first + 48,
                17_u64.to_le_bytes().to_vec(),
                FreshObjectError::DataRange { namespace: 10 },
            ),
            (
                first + 36,
                4_u32.to_le_bytes().to_vec(),
                FreshObjectError::MessageCount { namespace: 10 },
            ),
            (
                first + 56,
                (u64::MAX - 2).to_le_bytes().to_vec(),
                FreshObjectError::MessageCount { namespace: 10 },
            ),
            (
                third + 32,
                1_u32.to_le_bytes().to_vec(),
                FreshObjectError::UnclaimedRows,
            ),
        ] {
            assert_eq!(decoded(&tampered(&bytes, at, &value)), Err(refusal));
        }
    }

    #[test]
    fn given_a_batch_row_that_is_not_a_batch_when_decoding_should_refuse_it() {
        let bytes = sealed().bytes;
        let without_producer = batch_row(0);
        let with_producer = batch_row(1);

        for (at, value, row) in [
            (without_producer, 0_u32.to_le_bytes().to_vec(), 0),
            (without_producer + 4, 0_u32.to_le_bytes().to_vec(), 0),
            (without_producer + 24, 5_u64.to_le_bytes().to_vec(), 0),
            (without_producer + 32, 1_u16.to_le_bytes().to_vec(), 0),
            (without_producer + 36, 1_u32.to_le_bytes().to_vec(), 0),
            (with_producer + 34, vec![3], 1),
            (with_producer + 35, vec![1], 1),
        ] {
            assert_eq!(
                decoded(&tampered(&bytes, at, &value)),
                Err(FreshObjectError::InvalidBatch { row }),
                "byte {at}"
            );
        }
        for refused in [batch(0, b"blob", None), batch(1, b"", None)] {
            assert_eq!(
                SealedFreshObject::seal(header(), &[partition(10, vec![refused], Vec::new())]),
                Err(FreshObjectError::InvalidBatch { row: 0 })
            );
        }
    }

    #[test]
    fn given_a_proof_row_that_is_not_a_proof_when_decoding_should_refuse_it() {
        let bytes = sealed().bytes;
        let last = proof_row(3);

        for (at, value) in [
            (last + 44, 1_u32.to_le_bytes().to_vec()),
            (last + 40, 0_u32.to_le_bytes().to_vec()),
            (last + 24, (u64::MAX - 2).to_le_bytes().to_vec()),
        ] {
            assert_eq!(
                decoded(&tampered(&bytes, at, &value)),
                Err(FreshObjectError::InvalidProof { row: 3 }),
                "byte {at}"
            );
        }
    }

    #[test]
    fn given_proofs_that_skip_or_repeat_offsets_when_sealing_or_decoding_should_refuse_them() {
        let proofs =
            |second: FreshProofRow| [partition(10, Vec::new(), vec![proof(7, 90, 4), second])];

        assert_eq!(
            SealedFreshObject::seal(header(), &proofs(proof(7, 95, 1))),
            Err(FreshObjectError::ProofGap {
                namespace: 10,
                base_offset: 95,
            })
        );
        assert_eq!(
            SealedFreshObject::seal(header(), &proofs(proof(7, 90, 4))),
            Err(FreshObjectError::ProofGap {
                namespace: 10,
                base_offset: 90,
            })
        );
        let bytes = sealed().bytes;
        assert_eq!(
            decoded(&tampered(&bytes, proof_row(1) + 24, &93_u64.to_le_bytes())),
            Err(FreshObjectError::ProofGap {
                namespace: 10,
                base_offset: 93,
            })
        );
    }

    #[test]
    fn given_a_proof_of_a_batch_of_the_object_itself_when_sealing_or_decoding_should_refuse_it() {
        let own = [partition(10, Vec::new(), vec![proof(9, 90, 4)])];

        assert_eq!(
            SealedFreshObject::seal(header(), &own),
            Err(FreshObjectError::SelfProof { namespace: 10 })
        );
        let bytes = sealed().bytes;
        assert_eq!(
            decoded(&tampered(&bytes, proof_row(2), &id(9).get().to_le_bytes())),
            Err(FreshObjectError::SelfProof { namespace: 30 })
        );
    }

    #[test]
    fn given_where_a_blob_starts_when_looking_up_its_batch_should_find_it_in_its_partition_only() {
        let index = sealed().index;
        let second = index.positions()[1];

        let (row, batch) = index.batch_at(10, second).unwrap();

        assert_eq!(row.namespace, 10);
        assert_eq!(batch.producer, Some(PRODUCER));
        assert_eq!(index.batch_at(10, second + 1), None);
        assert_eq!(index.batch_at(20, second), None);
        assert_eq!(index.batch_at(99, second), None);
        assert_eq!(index.batch_at(30, index.index_len()), None);
        assert_eq!(
            index
                .batch_at(20, index.positions()[2])
                .map(|(row, _)| row.namespace),
            Some(20)
        );
    }

    #[test]
    fn given_row_counts_when_measuring_the_index_should_follow_the_layout() {
        assert_eq!(FreshIndex::len_for(1, 1, 0), 64 + 72 + 40 + 8);
        assert_eq!(FreshIndex::len_for(1, 0, 1), 64 + 72 + 56 + 8);
        let largest = FreshIndex::len_for(
            FRESH_OBJECT_BATCHES_MAX + FRESH_OBJECT_PROOFS_MAX,
            FRESH_OBJECT_BATCHES_MAX,
            FRESH_OBJECT_PROOFS_MAX,
        );
        assert!(largest <= FRESH_INDEX_BYTES_MAX, "{largest}");
    }
}
