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

//! The `remote.chain` file of a tiered partition.
//!
//! Every segment a tiered partition seals gets a record of its bounds and the
//! hash of its bytes. Tiered storage appends a marker when it later evicts the
//! segment's `.log`, so the chain is where a replica learns which segments it
//! holds only remotely, and what each one must hash to.
//!
//! The file opens with a header naming the partition's object prefix, then
//! holds fixed-size records appended in order, each with a checksum of its own.
//! A torn last record is a crash mid-append. Any other record that does not
//! decode refuses the whole file: read as absent, it would drop a segment from
//! the chain without a trace.
//!
//! No partition enables a chain yet, so nothing in production writes one.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io;
use std::path::Path;

use compio::io::AsyncWriteAtExt;
use consensus::state_artifact_checksum;
use iggy_common::IggyByteSize;
pub use iggy_object_store::ObjectPrefix;
use iggy_object_store::SEGMENT_HASH_LEN;
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use server_common::fatal::NoteDescriptorExhaustion;
use server_common::sharding::IggyNamespace;
use tracing::warn;

use crate::IggyPartition;
use crate::segment::{Segment, SegmentLocation};
use crate::state_transfer::{
    ChunkHasher, OFFER_HASH_CHUNK_LEN, STAGING_SUFFIX, fsync_dir, hash_segment_range,
};
use crate::tiered_floor::AdvanceTieredFloor;

#[cfg(test)]
mod tests;

/// The chain's file name in the partition directory.
pub const CHAIN_FILE: &str = "remote.chain";

/// Bytes in a segment hash.
pub const SEGMENT_HASH_SIZE: usize = SEGMENT_HASH_LEN;

/// `magic | stream_id | topic_id | partition_id | created_revision |
/// checksum`, all `u64` little-endian.
pub const HEADER_SIZE: usize = 48;

/// `kind u8 | source u8 | 2 zero bytes | crc32c u32 | start_offset | end_offset |
/// start_timestamp | end_timestamp | max_timestamp | size | incarnation | hash
/// | checksum`, the integers `u64` little-endian.
pub const RECORD_SIZE: usize = 104;

/// The longest chain a state transfer carries, one record per segment and its
/// trim point.
pub const CHAIN_ARTIFACT_LEN_MAX: u64 =
    (HEADER_SIZE + (CHAIN_ARTIFACT_SEGMENTS_MAX + 1) * RECORD_SIZE) as u64;

const CHAIN_ARTIFACT_SEGMENTS_MAX: usize = 1 << 20;

const HEADER_MAGIC: u64 = u64::from_le_bytes(*b"IGGYCHN1");
const HEADER_CHECKSUM_AT: usize = HEADER_SIZE - size_of::<u64>();
const RECORD_HASH_AT: usize = 64;
const RECORD_CHECKSUM_AT: usize = RECORD_SIZE - size_of::<u64>();

const SEALED: u8 = 1;
const REMOTE: u8 = 2;
const TRIM_POINT: u8 = 3;
const ADVANCED: u8 = 4;

const COMMIT_PATH: u8 = 1;
const DISK: u8 = 2;

fn encode_header(prefix: &ObjectPrefix) -> [u8; HEADER_SIZE] {
    let mut header = [0; HEADER_SIZE];
    let mut writer = FieldWriter::new(&mut header);
    writer.u64(HEADER_MAGIC);
    writer.u64(prefix.stream_id);
    writer.u64(prefix.topic_id);
    writer.u64(prefix.partition_id);
    writer.u64(prefix.created_revision);
    let checksum = state_artifact_checksum(&header[..HEADER_CHECKSUM_AT]);
    header[HEADER_CHECKSUM_AT..].copy_from_slice(&checksum.to_le_bytes());
    header
}

fn decode_header(header: &[u8; HEADER_SIZE]) -> Option<ObjectPrefix> {
    let mut reader = FieldReader::new(header);
    if reader.u64() != HEADER_MAGIC {
        return None;
    }
    let prefix = ObjectPrefix {
        stream_id: reader.u64(),
        topic_id: reader.u64(),
        partition_id: reader.u64(),
        created_revision: reader.u64(),
    };
    (reader.u64() == state_artifact_checksum(&header[..HEADER_CHECKSUM_AT])).then_some(prefix)
}

/// How a segment's hash was taken.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashSource {
    /// Fed the bytes consensus committed, as they were materialized.
    CommitPath,
    /// Read back from the segment file: a weaker guarantee, since it describes
    /// what the disk returned rather than what consensus agreed on.
    Disk,
}

/// The two hashes of a segment's bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentHashes {
    /// What a replica compares before it trusts a segment it did not write.
    pub blake3: [u8; SEGMENT_HASH_SIZE],
    /// What an upload hands the store, which refuses bytes that hash to
    /// anything else and reports the value back for the object it holds.
    pub crc32c: u32,
}

/// A sealed segment as the chain records it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentRecord {
    pub start_offset: u64,
    pub end_offset: u64,
    pub start_timestamp: u64,
    pub end_timestamp: u64,
    pub max_timestamp: u64,
    pub size: u64,
    /// The partition incarnation that sealed the segment.
    pub incarnation: u64,
    pub hash: [u8; SEGMENT_HASH_SIZE],
    pub crc32c: u32,
    pub source: HashSource,
}

impl SegmentRecord {
    /// The segment this record stands in for once its `.log` left the disk.
    #[must_use]
    pub fn remote_segment(&self, max_size: IggyByteSize) -> Segment {
        let mut segment = Segment::new(self.start_offset, max_size);
        segment.sealed = true;
        segment.location = SegmentLocation::Remote;
        segment.start_timestamp = self.start_timestamp;
        segment.end_timestamp = self.end_timestamp;
        segment.max_timestamp = self.max_timestamp;
        segment.end_offset = self.end_offset;
        segment.size = IggyByteSize::from(self.size);
        segment.current_position = self.size;
        segment
    }

    /// The record of `segment` sealed with `hashes`.
    #[must_use]
    pub fn new(
        segment: &Segment,
        incarnation: u64,
        hashes: SegmentHashes,
        source: HashSource,
    ) -> Self {
        Self {
            start_offset: segment.start_offset,
            end_offset: segment.end_offset,
            start_timestamp: segment.start_timestamp,
            end_timestamp: segment.end_timestamp,
            max_timestamp: segment.max_timestamp,
            size: segment.size.as_bytes_u64(),
            incarnation,
            hash: hashes.blake3,
            crc32c: hashes.crc32c,
            source,
        }
    }
}

/// One entry of the chain.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChainRecord {
    /// A sealed segment and the hash of its bytes.
    Sealed(SegmentRecord),
    /// The segment's `.log` left this replica, and the record stands in for it.
    Remote(SegmentRecord),
    /// The front this replica's own retention had left when tiering was
    /// enabled.
    TrimPoint(u64),
    /// A floor advance this replica applied: the store holds the segment
    /// ending at `up_to`, and its bytes hash to `hash`. The replica evicts its
    /// own copy only once a sealed record matches it.
    Advanced(AdvanceTieredFloor),
}

impl ChainRecord {
    /// The segment a sealed or remote record describes.
    #[must_use]
    pub const fn segment(&self) -> Option<&SegmentRecord> {
        match self {
            Self::Sealed(segment) | Self::Remote(segment) => Some(segment),
            Self::TrimPoint(_) | Self::Advanced(_) => None,
        }
    }

    /// The record's fixed-size encoding, checksum included.
    #[must_use]
    pub fn encode(&self) -> [u8; RECORD_SIZE] {
        let mut out = [0; RECORD_SIZE];
        let mut writer = FieldWriter::new(&mut out);
        match self {
            Self::Sealed(segment) | Self::Remote(segment) => {
                let kind = if matches!(self, Self::Sealed(_)) {
                    SEALED
                } else {
                    REMOTE
                };
                let source = match segment.source {
                    HashSource::CommitPath => COMMIT_PATH,
                    HashSource::Disk => DISK,
                };
                writer.bytes(&[kind, source, 0, 0]);
                writer.bytes(&segment.crc32c.to_le_bytes());
                writer.u64(segment.start_offset);
                writer.u64(segment.end_offset);
                writer.u64(segment.start_timestamp);
                writer.u64(segment.end_timestamp);
                writer.u64(segment.max_timestamp);
                writer.u64(segment.size);
                writer.u64(segment.incarnation);
                writer.bytes(&segment.hash);
            }
            Self::TrimPoint(offset) => {
                writer.bytes(&[TRIM_POINT, 0, 0, 0, 0, 0, 0, 0]);
                writer.u64(*offset);
            }
            Self::Advanced(advance) => {
                writer.bytes(&[ADVANCED, 0, 0, 0, 0, 0, 0, 0]);
                writer.u64(advance.start);
                writer.u64(advance.up_to);
                out[RECORD_HASH_AT..RECORD_CHECKSUM_AT].copy_from_slice(&advance.hash);
            }
        }
        let checksum = state_artifact_checksum(&out[..RECORD_CHECKSUM_AT]);
        out[RECORD_CHECKSUM_AT..].copy_from_slice(&checksum.to_le_bytes());
        out
    }

    /// `None` for anything this build did not write: an unknown kind or hash
    /// source, padding that is not zero, or a checksum that does not match.
    fn decode(bytes: &[u8; RECORD_SIZE]) -> Option<Self> {
        let mut reader = FieldReader::new(bytes);
        let [kind, source, padding @ ..] = reader.array::<4>();
        let crc32c = u32::from_le_bytes(reader.array());
        if padding != [0; 2]
            || u64::from_le_bytes(*bytes.last_chunk()?)
                != state_artifact_checksum(&bytes[..RECORD_CHECKSUM_AT])
        {
            return None;
        }
        if kind == TRIM_POINT {
            let offset = reader.u64();
            let unused = &bytes[size_of::<u64>() * 2..RECORD_CHECKSUM_AT];
            return (source == 0 && crc32c == 0 && unused.iter().all(|byte| *byte == 0))
                .then_some(Self::TrimPoint(offset));
        }
        if kind == ADVANCED {
            let advance = AdvanceTieredFloor {
                start: reader.u64(),
                up_to: reader.u64(),
                hash: bytes[RECORD_HASH_AT..RECORD_CHECKSUM_AT].try_into().ok()?,
            };
            let unused = &bytes[size_of::<u64>() * 3..RECORD_HASH_AT];
            return (source == 0 && crc32c == 0 && unused.iter().all(|byte| *byte == 0))
                .then_some(Self::Advanced(advance));
        }
        let source = match source {
            COMMIT_PATH => HashSource::CommitPath,
            DISK => HashSource::Disk,
            _ => return None,
        };
        let segment = SegmentRecord {
            start_offset: reader.u64(),
            end_offset: reader.u64(),
            start_timestamp: reader.u64(),
            end_timestamp: reader.u64(),
            max_timestamp: reader.u64(),
            size: reader.u64(),
            incarnation: reader.u64(),
            hash: bytes[RECORD_HASH_AT..RECORD_CHECKSUM_AT].try_into().ok()?,
            crc32c,
            source,
        };
        match kind {
            SEALED => Some(Self::Sealed(segment)),
            REMOTE => Some(Self::Remote(segment)),
            _ => None,
        }
    }
}

/// A chain file as read back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chain {
    pub prefix: ObjectPrefix,
    pub records: Vec<ChainRecord>,
    /// The header and the whole records: where the next record goes.
    pub valid_len: u64,
    /// The file ended in a record a crash cut short, which the next append
    /// overwrites.
    pub torn_tail: bool,
}

impl Chain {
    /// The highest trim point the chain records. Retention raises it before it
    /// unlinks anything, so every offset at or below it may be gone.
    #[must_use]
    pub fn trim_point(&self) -> Option<u64> {
        self.records
            .iter()
            .filter_map(|record| match record {
                ChainRecord::TrimPoint(offset) => Some(*offset),
                ChainRecord::Sealed(_) | ChainRecord::Remote(_) | ChainRecord::Advanced(_) => None,
            })
            .max()
    }

    /// The length of [`Self::encode`].
    #[must_use]
    pub const fn encoded_len(&self) -> usize {
        HEADER_SIZE + self.records.len() * RECORD_SIZE
    }

    /// The file bytes of the chain: its header, then every record in order.
    #[must_use]
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.encoded_len());
        bytes.extend_from_slice(&encode_header(&self.prefix));
        for record in &self.records {
            bytes.extend_from_slice(&record.encode());
        }
        bytes
    }

    /// The live segments whose latest record is remote, in offset order.
    #[must_use]
    pub fn remote_segments(&self) -> Vec<SegmentRecord> {
        self.live_segments()
            .into_values()
            .filter_map(|record| match record {
                ChainRecord::Remote(segment) => Some(segment),
                ChainRecord::Sealed(_) | ChainRecord::TrimPoint(_) | ChainRecord::Advanced(_) => {
                    None
                }
            })
            .collect()
    }

    /// The latest record of every segment the trim point has not retired, by
    /// start offset.
    #[must_use]
    pub fn live_segments(&self) -> BTreeMap<u64, ChainRecord> {
        let trim_point = self.trim_point();
        let mut live = BTreeMap::new();
        for record in &self.records {
            if let Some(segment) = record.segment()
                && trim_point.is_none_or(|trim_point| segment.end_offset > trim_point)
            {
                live.insert(segment.start_offset, *record);
            }
        }
        live
    }
}

/// Why a chain file cannot be read.
#[derive(Debug)]
pub enum ChainError {
    Io(io::Error),
    /// The header is cut short or is not one this build writes.
    Header,
    /// A record other than the last does not decode.
    Record {
        index: usize,
    },
    /// The chain names another partition's objects.
    Prefix {
        recorded: ObjectPrefix,
    },
    /// The partition has no directory to keep a chain in.
    NoDirectory,
}

impl fmt::Display for ChainError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "cannot read or write the segment chain: {error}"),
            Self::Header => f.write_str("the segment chain has no valid header"),
            Self::Record { index } => {
                write!(f, "segment chain record {index} does not decode")
            }
            Self::Prefix { recorded } => {
                write!(f, "the segment chain records another prefix: {recorded:?}")
            }
            Self::NoDirectory => f.write_str("the partition has no directory for a chain"),
        }
    }
}

impl std::error::Error for ChainError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for ChainError {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

/// Decode a whole chain file.
///
/// # Errors
/// [`ChainError::Header`] for a header this build did not write, and
/// [`ChainError::Record`] for an undecodable record anywhere but at the end.
pub fn decode_chain(bytes: &[u8]) -> Result<Chain, ChainError> {
    let (header, mut rest) = bytes
        .split_first_chunk::<HEADER_SIZE>()
        .ok_or(ChainError::Header)?;
    let prefix = decode_header(header).ok_or(ChainError::Header)?;
    let mut records = Vec::with_capacity(rest.len() / RECORD_SIZE);
    let mut torn_tail = false;
    while !rest.is_empty() {
        let next = rest.split_first_chunk::<RECORD_SIZE>();
        match next.and_then(|(record, _)| ChainRecord::decode(record)) {
            Some(record) => {
                records.push(record);
                rest = next.map_or(&[], |(_, after)| after);
            }
            None if rest.len() <= RECORD_SIZE => {
                torn_tail = true;
                break;
            }
            None => {
                return Err(ChainError::Record {
                    index: records.len(),
                });
            }
        }
    }
    let valid_len = (HEADER_SIZE + records.len() * RECORD_SIZE) as u64;
    Ok(Chain {
        prefix,
        records,
        valid_len,
        torn_tail,
    })
}

/// Path of the chain file in `partition_dir`.
#[must_use]
pub fn chain_path(partition_dir: &str) -> String {
    format!("{partition_dir}/{CHAIN_FILE}")
}

/// Create the chain of the partition in `partition_dir`, holding only its
/// header, and fsync the directory so the file outlives a crash.
///
/// # Errors
/// Any I/O failure, after which no chain exists under the final name.
pub async fn create_chain(partition_dir: &str, prefix: &ObjectPrefix) -> io::Result<()> {
    replace_chain(partition_dir, encode_header(prefix).to_vec()).await?;
    crate::state_transfer::fsync_dir(partition_dir).await
}

/// Put `bytes` in place as the chain of `partition_dir`, whole or not at all.
/// The caller fsyncs the directory.
///
/// # Errors
/// Any I/O failure, after which the previous chain, if any, is still in place.
pub(crate) async fn replace_chain(partition_dir: &str, bytes: Vec<u8>) -> io::Result<()> {
    let path = chain_path(partition_dir);
    // A staging file every sweep already removes, renamed into place, so a
    // crash never leaves a torn header under the chain's name.
    let staging = format!("{path}{STAGING_SUFFIX}");
    let mut file = compio::fs::File::create(&staging)
        .await
        .note_descriptor_exhaustion(|| format!("creating {staging}"))?;
    file.write_all_at(bytes, 0).await.0?;
    file.sync_all().await?;
    compio::fs::rename(&staging, &path).await
}

/// Read the chain in `partition_dir`, `None` when there is none.
///
/// # Errors
/// An I/O failure other than the file being absent, or a file
/// [`decode_chain`] refuses.
pub async fn read_chain(partition_dir: &str) -> Result<Option<Chain>, ChainError> {
    match compio::fs::read(chain_path(partition_dir)).await {
        Ok(bytes) => decode_chain(&bytes).map(Some),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// Cut the chain at `valid_len`, dropping the record a crash tore.
///
/// # Errors
/// Any I/O failure, after which the torn bytes may remain.
pub async fn truncate_chain(partition_dir: &str, valid_len: u64) -> io::Result<()> {
    let path = chain_path(partition_dir);
    let file = compio::fs::OpenOptions::new()
        .write(true)
        .open(&path)
        .await
        .note_descriptor_exhaustion(|| format!("opening {path}"))?;
    file.set_len(valid_len).await?;
    file.sync_all().await
}

/// Write `record` at `at`, the chain's valid length, and make it durable.
///
/// Writing at the valid length rather than the end of the file overwrites a
/// record a crash cut short, and makes a retried append rewrite the same bytes.
///
/// # Errors
/// Any I/O failure, after which the record may be torn in place.
pub async fn append_record(path: &str, at: u64, record: &ChainRecord) -> io::Result<()> {
    let mut file = compio::fs::OpenOptions::new()
        .write(true)
        .open(path)
        .await
        .note_descriptor_exhaustion(|| format!("opening {path}"))?;
    file.write_all_at(record.encode(), at).await.0?;
    file.sync_all().await
}

/// The hashes of the first `size` bytes of the segment file at `log_path`,
/// read in chunks.
///
/// # Errors
/// Any I/O failure, including a file shorter than `size`.
pub async fn hash_segment_file(log_path: &str, size: u64) -> io::Result<SegmentHashes> {
    let mut hasher = SegmentHasher::default();
    hash_segment_range(log_path, 0, size, &mut hasher, None).await?;
    Ok(hasher.finalize())
}

/// Takes both hashes in one pass over a segment's bytes.
#[derive(Clone, Default)]
pub(crate) struct SegmentHasher {
    blake3: blake3::Hasher,
    crc32c: u32,
}

impl SegmentHasher {
    pub(crate) fn finalize(&self) -> SegmentHashes {
        SegmentHashes {
            blake3: *self.blake3.finalize().as_bytes(),
            crc32c: self.crc32c,
        }
    }
}

impl ChunkHasher for SegmentHasher {
    fn update(&mut self, bytes: &[u8]) {
        self.blake3.update(bytes);
        self.crc32c = crc32c::crc32c_append(self.crc32c, bytes);
    }
}

/// The hashes of the active segment, fed its committed bytes as they are
/// materialized.
#[derive(Clone)]
pub struct SegmentDigest {
    start_offset: u64,
    hashed_len: u64,
    hasher: SegmentHasher,
}

impl SegmentDigest {
    /// A digest of the segment starting at `start_offset`, before its first
    /// byte.
    #[must_use]
    pub fn new(start_offset: u64) -> Self {
        Self {
            start_offset,
            hashed_len: 0,
            hasher: SegmentHasher::default(),
        }
    }

    /// Whether the digest covers exactly the first `size` bytes of the segment
    /// starting at `start_offset`.
    #[must_use]
    pub const fn covers(&self, start_offset: u64, size: u64) -> bool {
        self.start_offset == start_offset && self.hashed_len == size
    }

    #[must_use]
    pub fn finalize(&self) -> SegmentHashes {
        self.hasher.finalize()
    }

    /// Whether the digest holds the first bytes of the segment starting at
    /// `start_offset` and not yet all `size` of them.
    pub(crate) const fn resumes(&self, start_offset: u64, size: u64) -> bool {
        self.start_offset == start_offset && self.hashed_len < size
    }

    /// Whether this is the digest `captured` was before a step of the same
    /// segment's hash.
    pub(crate) const fn continues(&self, captured: &Self) -> bool {
        self.start_offset == captured.start_offset && self.hashed_len > captured.hashed_len
    }

    /// Bytes the next step of a hash from disk reads of a segment of `size`
    /// bytes, which is the buffer the step allocates.
    pub(crate) fn step_len(&self, size: u64) -> usize {
        usize::try_from(size.saturating_sub(self.hashed_len))
            .map_or(HASH_STEP_LEN, |left| left.min(HASH_STEP_LEN))
    }
}

impl ChunkHasher for SegmentDigest {
    fn update(&mut self, bytes: &[u8]) {
        self.hasher.update(bytes);
        self.hashed_len += bytes.len() as u64;
    }
}

/// A chain reduced to its trim point and the latest record of every segment
/// the trim point has not retired. Building it walks every record, so a reader
/// that needs it more than once keeps it.
pub(crate) struct LiveChain {
    prefix: ObjectPrefix,
    valid_len: u64,
    trim_point: Option<u64>,
    sealed: BTreeMap<u64, SegmentRecord>,
    /// The segments in `sealed` whose latest record is remote.
    remote: BTreeSet<u64>,
    /// The hash of every recorded floor advance, by its last offset.
    advances: BTreeMap<u64, [u8; SEGMENT_HASH_SIZE]>,
}

impl LiveChain {
    pub(crate) fn new(chain: &Chain) -> Self {
        let trim_point = chain.trim_point();
        let mut latest: Vec<(&SegmentRecord, bool)> = chain
            .records
            .iter()
            .filter_map(|record| {
                let segment = record.segment()?;
                trim_point
                    .is_none_or(|trim_point| segment.end_offset > trim_point)
                    .then_some((segment, matches!(record, ChainRecord::Remote(_))))
            })
            .collect();
        // Sorted and deduplicated up front, so both collections below are
        // built in one linear pass instead of an insert per record. The sort
        // is stable and the walk is reversed, so the record kept for a start
        // offset is the last the chain holds for it.
        latest.sort_by_key(|(segment, _)| segment.start_offset);
        latest.reverse();
        latest.dedup_by_key(|(segment, _)| segment.start_offset);
        latest.reverse();
        Self {
            prefix: chain.prefix,
            valid_len: chain.valid_len,
            trim_point,
            sealed: latest
                .iter()
                .map(|(segment, _)| (segment.start_offset, **segment))
                .collect(),
            remote: latest
                .iter()
                .filter(|(_, remote)| *remote)
                .map(|(segment, _)| segment.start_offset)
                .collect(),
            advances: chain
                .records
                .iter()
                .filter_map(|record| match record {
                    ChainRecord::Advanced(advance) => Some((advance.up_to, advance.hash)),
                    _ => None,
                })
                .collect(),
        }
    }

    pub(crate) const fn trim_point(&self) -> Option<u64> {
        self.trim_point
    }

    /// The segments whose latest record is remote, in offset order.
    pub(crate) fn remote_segments(&self) -> Vec<SegmentRecord> {
        let mut remote = self.remote.iter().peekable();
        self.sealed
            .iter()
            .filter(|(start_offset, _)| remote.next_if_eq(start_offset).is_some())
            .map(|(_, segment)| *segment)
            .collect()
    }
}

/// What a partition with a chain keeps of it between appends.
pub(crate) struct SegmentChain {
    path: String,
    prefix: ObjectPrefix,
    /// Where the next append writes. Appends run one at a time on the
    /// partition's I/O lane, each at the length it was captured with.
    valid_len: u64,
    /// The latest record of every sealed segment the trim point has not
    /// retired, by start offset.
    sealed: BTreeMap<u64, SegmentRecord>,
    /// The sealed segments with a remote record. Their `.log` is gone, or a
    /// crash cut their eviction short before the unlink.
    remote: BTreeSet<u64>,
    /// The sealed segments whose recorded hash a floor advance matched.
    confirmed: BTreeSet<u64>,
    /// The hash of each recorded advance that no sealed record settled yet, by
    /// the last offset it covers. The segment has not sealed here, or its
    /// record from disk is still to come.
    advances: BTreeMap<u64, [u8; SEGMENT_HASH_SIZE]>,
    /// The advances that this replica's own segment contradicts, by the last
    /// offset they cover: its bytes hash to something else, or it ends
    /// elsewhere. Such a segment never leaves the disk.
    diverged: BTreeSet<u64>,
    trim_point: Option<u64>,
    /// The digest of the active segment, `None` once any of its bytes reached
    /// the segment without passing through it.
    digest: Option<Box<SegmentDigest>>,
}

/// A record the rotation job appends before it creates the next segment, so the
/// sealed segment's hash is durable from the moment it exists.
pub struct ChainAppend {
    pub(crate) path: String,
    pub(crate) at: u64,
    pub(crate) record: ChainRecord,
}

impl ChainAppend {
    pub(crate) async fn execute(&self) -> io::Result<()> {
        append_record(&self.path, self.at, &self.record).await
    }
}

/// The prefix of the partition `namespace` names in the incarnation that
/// `created_revision` identifies.
#[must_use]
pub fn object_prefix(namespace: IggyNamespace, created_revision: u64) -> ObjectPrefix {
    ObjectPrefix {
        stream_id: namespace.stream_id() as u64,
        topic_id: namespace.topic_id() as u64,
        partition_id: namespace.partition_id() as u64,
        created_revision,
    }
}

/// One step of turning tiering on for a partition, or of keeping its chain
/// complete afterwards. Each runs on the partition's I/O lane.
pub enum TieringJob {
    /// Load the chain in the directory, or create it holding `prefix`.
    Open {
        directory: String,
        prefix: ObjectPrefix,
    },
    /// Record the front that retention had left before tiering was enabled,
    /// or a floor advance the replica applied.
    Append(ChainAppend),
    /// Hash the next `HASH_STEP_LEN` bytes of a sealed segment that has no
    /// record from its `.log`, and record the segment once `digest` holds all
    /// of it. `append` holds the record with a zero hash.
    Hash {
        log_path: String,
        append: ChainAppend,
        digest: Box<SegmentDigest>,
    },
}

/// Bytes one step of a hash from disk reads. A step is one job on the
/// partition's I/O lane, so this bounds how long other work waits behind it.
pub(crate) const HASH_STEP_LEN: usize = OFFER_HASH_CHUNK_LEN;

pub enum TieringResult {
    Opened(Result<Chain, ChainError>),
    Appended(io::Result<ChainRecord>),
    /// The digest after a step that left bytes of the segment unread.
    Hashed(Box<SegmentDigest>),
}

impl TieringJob {
    pub(crate) async fn execute(self) -> TieringResult {
        match self {
            Self::Open { directory, prefix } => {
                TieringResult::Opened(open(&directory, prefix).await)
            }
            Self::Append(append) => {
                TieringResult::Appended(append.execute().await.map(|()| append.record))
            }
            Self::Hash {
                log_path,
                append,
                digest,
            } => hash_step(&log_path, append, digest)
                .await
                .unwrap_or_else(|error| TieringResult::Appended(Err(error))),
        }
    }
}

async fn hash_step(
    log_path: &str,
    mut append: ChainAppend,
    mut digest: Box<SegmentDigest>,
) -> io::Result<TieringResult> {
    let ChainRecord::Sealed(segment) = &mut append.record else {
        return Err(io::Error::other(
            "a hash from disk records a sealed segment",
        ));
    };
    let from = digest.hashed_len;
    let to = from + digest.step_len(segment.size) as u64;
    hash_segment_range(log_path, from, to, &mut *digest, None).await?;
    if !digest.covers(segment.start_offset, segment.size) {
        return Ok(TieringResult::Hashed(digest));
    }
    let hashes = digest.finalize();
    segment.hash = hashes.blake3;
    segment.crc32c = hashes.crc32c;
    append.execute().await?;
    Ok(TieringResult::Appended(Ok(append.record)))
}

/// The chain in `directory`, created holding `prefix` when there is none.
async fn open(directory: &str, prefix: ObjectPrefix) -> Result<Chain, ChainError> {
    if let Some(chain) = read_chain(directory).await? {
        if chain.prefix != prefix {
            return Err(ChainError::Prefix {
                recorded: chain.prefix,
            });
        }
        // The chain may be here by a rename no directory sync followed, and
        // a record appended to a name a power loss takes back is lost.
        fsync_dir(directory).await?;
        return Ok(chain);
    }
    create_chain(directory, &prefix).await?;
    Ok(Chain {
        prefix,
        records: Vec::new(),
        valid_len: HEADER_SIZE as u64,
        torn_tail: false,
    })
}

/// Evicts one sealed segment: a durable remote record first, so no crash
/// leaves the `.log` gone without a record standing in for it.
pub struct EvictionJob {
    pub(crate) start_offset: u64,
    /// `None` when the record was already durable before a crash.
    pub(crate) append: Option<ChainAppend>,
    pub(crate) messages_path: Option<String>,
}

pub struct EvictionResult {
    pub(crate) start_offset: u64,
    /// This job made the remote record durable, even if the unlink failed
    /// after it.
    pub(crate) appended: bool,
    pub(crate) outcome: Result<(), EvictionFailure>,
}

/// Which step of an eviction failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EvictionFailure {
    /// The remote record may not be durable, so the `.log` stayed.
    Record,
    /// The record is durable, but the `.log` is still there.
    Unlink,
    /// The `.log` is unlinked, but a power loss may bring it back.
    Sync,
}

impl EvictionJob {
    pub(crate) async fn execute(self) -> EvictionResult {
        let Self {
            start_offset,
            append,
            messages_path,
        } = self;
        if let Some(append) = &append
            && let Err(error) = append.execute().await
        {
            warn!(path = append.path, start_offset, %error, "cannot append the evicted segment's remote record");
            return EvictionResult {
                start_offset,
                appended: false,
                outcome: Err(EvictionFailure::Record),
            };
        }
        let outcome = match &messages_path {
            Some(path) => unlink_log(path, start_offset).await,
            None => Ok(()),
        };
        EvictionResult {
            start_offset,
            appended: append.is_some(),
            outcome,
        }
    }
}

/// Unlink an evicted segment's `.log` and sync its directory before the next
/// eviction may start. Without the sync a power loss could bring this `.log`
/// back while a later one stays gone, and boot refuses a local segment in
/// front of a remote one.
async fn unlink_log(path: &str, start_offset: u64) -> Result<(), EvictionFailure> {
    match compio::fs::remove_file(path).await {
        Ok(()) => {}
        // Gone already, by an unlink no directory sync is known to cover.
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            warn!(%path, start_offset, %error, "cannot unlink the evicted segment's log");
            return Err(EvictionFailure::Unlink);
        }
    }
    let directory = Path::new(path)
        .parent()
        .and_then(Path::to_str)
        .ok_or(EvictionFailure::Sync)?;
    fsync_dir(directory).await.map_err(|error| {
        warn!(%path, start_offset, %error, "cannot sync the directory after unlinking the evicted segment's log");
        EvictionFailure::Sync
    })
}

impl SegmentChain {
    fn load(path: String, chain: LiveChain, active: Option<&Segment>) -> Self {
        let digest = active
            .filter(|active| active.size.as_bytes_u64() == 0)
            .map(|active| Box::new(SegmentDigest::new(active.start_offset)));
        let advances = chain.advances;
        let mut loaded = Self {
            path,
            prefix: chain.prefix,
            valid_len: chain.valid_len,
            sealed: chain.sealed,
            remote: chain.remote,
            confirmed: BTreeSet::new(),
            advances: BTreeMap::new(),
            diverged: BTreeSet::new(),
            trim_point: chain.trim_point,
            digest,
        };
        for (up_to, hash) in advances {
            loaded.take_advance(up_to, hash, &mut Vec::new());
        }
        loaded
    }

    /// Compare the advance ending at `up_to` with this replica's own sealed
    /// record of the segment that holds the offset. A match confirms the
    /// segment, which may then leave the disk, and anything else is pushed to
    /// `diverged`. Returns `false` while that segment has not sealed here.
    fn settle_advance(
        &mut self,
        up_to: u64,
        hash: &[u8; SEGMENT_HASH_SIZE],
        diverged: &mut Vec<u64>,
    ) -> bool {
        if self
            .trim_point
            .is_some_and(|trim_point| up_to <= trim_point)
        {
            return true;
        }
        let Some((start_offset, record)) = self
            .sealed
            .range(..=up_to)
            .next_back()
            .filter(|(_, record)| record.end_offset >= up_to)
        else {
            return false;
        };
        if record.end_offset == up_to && record.hash == *hash {
            self.confirmed.insert(*start_offset);
        } else {
            self.diverged.insert(up_to);
            diverged.push(up_to);
        }
        true
    }

    /// Settle a recorded advance, or keep it until its segment seals.
    fn take_advance(&mut self, up_to: u64, hash: [u8; SEGMENT_HASH_SIZE], diverged: &mut Vec<u64>) {
        if !self.settle_advance(up_to, &hash, diverged) {
            self.advances.insert(up_to, hash);
        }
    }

    /// Settle the advances that waited for the segment `segment` records.
    /// Only those ending inside it can be affected, so a seal costs the same
    /// however many advances wait for later segments.
    fn settle_advances_in(&mut self, segment: &SegmentRecord, diverged: &mut Vec<u64>) {
        let held: Vec<_> = self
            .advances
            .range(segment.start_offset..=segment.end_offset)
            .map(|(up_to, hash)| (*up_to, *hash))
            .collect();
        for (up_to, hash) in held {
            if self.settle_advance(up_to, &hash, diverged) {
                self.advances.remove(&up_to);
            }
        }
    }

    /// Whether `advance` still needs a record: the chain does not hold it and
    /// no segment settled it.
    pub(crate) fn lacks_advance(&self, advance: &AdvanceTieredFloor) -> bool {
        let settled = self.advances.contains_key(&advance.up_to)
            || self.diverged.contains(&advance.up_to)
            || self
                .trim_point
                .is_some_and(|trim_point| advance.up_to <= trim_point)
            || self.sealed.range(..=advance.up_to).next_back().is_some_and(
                |(start_offset, record)| {
                    record.end_offset == advance.up_to && self.confirmed.contains(start_offset)
                },
            );
        !settled
    }

    /// The record of `advance`, `None` when the chain holds it already or a
    /// segment settled it.
    pub(crate) fn record_advance(&self, advance: &AdvanceTieredFloor) -> Option<ChainAppend> {
        self.lacks_advance(advance)
            .then(|| self.append(ChainRecord::Advanced(*advance)))
    }

    /// The last offsets of the advances this replica's segments contradict.
    pub(crate) const fn diverged(&self) -> &BTreeSet<u64> {
        &self.diverged
    }

    /// Take the digest that covers the first `size` bytes of the active segment
    /// starting at `start_offset`, for a materialization to feed. A digest that
    /// does not cover them is dropped: the segment can no longer be hashed on
    /// the commit path.
    pub(crate) fn take_digest(
        &mut self,
        start_offset: u64,
        size: u64,
    ) -> Option<Box<SegmentDigest>> {
        self.digest
            .take()
            .filter(|digest| digest.covers(start_offset, size))
    }

    /// Return the digest a materialization fed.
    pub(crate) fn restore_digest(&mut self, digest: Option<Box<SegmentDigest>>) {
        self.digest = digest;
    }

    /// The record that seals `segment`, when its digest covers every byte.
    pub(crate) fn seal(&self, segment: &Segment, incarnation: u64) -> Option<ChainAppend> {
        let digest = self
            .digest
            .as_ref()
            .filter(|digest| digest.covers(segment.start_offset, segment.size.as_bytes_u64()))?;
        Some(self.append(ChainRecord::Sealed(SegmentRecord::new(
            segment,
            incarnation,
            digest.finalize(),
            HashSource::CommitPath,
        ))))
    }

    /// Account for a seal: the appended record, if there was one, and a fresh
    /// digest for the segment the rotation planted at `next_start`.
    pub(crate) fn sealed(&mut self, appended: Option<&ChainRecord>, next_start: u64) -> Vec<u64> {
        self.digest = Some(Box::new(SegmentDigest::new(next_start)));
        appended.map_or_else(Vec::new, |record| self.note_appended(record))
    }

    /// The chain as a state transfer carries it: the trim point and the latest
    /// record of every live segment, without the records a reader no longer
    /// needs. `applied` are the advances the partition applied, of which the
    /// chain may not hold the latest yet.
    ///
    /// A segment is remote only when `without_log`, ascending, holds its start
    /// offset. The chain's own remote records do not decide it: an eviction cut
    /// short after its record leaves the `.log` in place, and that segment
    /// travels as bytes.
    fn snapshot<'a>(
        &self,
        without_log: &[u64],
        applied: impl Iterator<Item = &'a AdvanceTieredFloor>,
    ) -> Chain {
        let mut records = Vec::with_capacity(self.sealed.len() + 1);
        records.extend(self.trim_point.map(ChainRecord::TrimPoint));
        records.extend(self.sealed.values().map(|segment| {
            if without_log.binary_search(&segment.start_offset).is_ok() {
                ChainRecord::Remote(*segment)
            } else {
                ChainRecord::Sealed(*segment)
            }
        }));
        // The receiver holds the sender's segments, so what an advance proved
        // about them here holds there. Without these it would never evict the
        // segments it received: the advances that covered them are below the
        // floor it starts from, and are not applied again. A remote segment
        // needs none, since nothing is left to evict. Remote here is what the
        // receiver holds no bytes of, as for the records above.
        records.extend(
            self.confirmed
                .iter()
                .filter(|start_offset| without_log.binary_search(start_offset).is_err())
                .filter_map(|start_offset| self.sealed.get(start_offset))
                .map(|segment| {
                    ChainRecord::Advanced(AdvanceTieredFloor {
                        start: segment.start_offset,
                        up_to: segment.end_offset,
                        hash: segment.hash,
                    })
                }),
        );
        // An advance that waits for its segment to seal waits on the receiver
        // too. Only the end and the hash are compared, so the start is not kept.
        records.extend(self.advances.iter().map(|(up_to, hash)| {
            ChainRecord::Advanced(AdvanceTieredFloor {
                start: *up_to,
                up_to: *up_to,
                hash: *hash,
            })
        }));
        // The receiver starts from a floor that covers every applied advance,
        // whether or not its record reached this chain before the capture.
        records.extend(
            applied
                .filter(|advance| self.lacks_advance(advance))
                .map(|advance| ChainRecord::Advanced(*advance)),
        );
        Chain {
            prefix: self.prefix,
            valid_len: (HEADER_SIZE + records.len() * RECORD_SIZE) as u64,
            records,
            torn_tail: false,
        }
    }

    /// The trim point retention appends before it unlinks the segment ending at
    /// `end_offset`, `None` when the chain already holds one at least as high.
    pub(crate) fn trim(&self, end_offset: u64) -> Option<ChainAppend> {
        self.trim_point
            .is_none_or(|trim_point| trim_point < end_offset)
            .then(|| self.append(ChainRecord::TrimPoint(end_offset)))
    }

    /// Whether the segment from `start_offset` to `end_offset` may leave the
    /// local disk: the chain records it and a floor advance matched its hash.
    pub(crate) fn evictable(&self, start_offset: u64, end_offset: u64) -> bool {
        self.confirmed.contains(&start_offset)
            && self
                .sealed
                .get(&start_offset)
                .is_some_and(|record| record.end_offset == end_offset)
    }

    /// The job that evicts the segment starting at `start_offset`. A record
    /// that a crash left durable is not appended twice.
    pub(crate) fn evict(
        &self,
        start_offset: u64,
        messages_path: Option<String>,
    ) -> Option<EvictionJob> {
        let record = self.sealed.get(&start_offset)?;
        let append = (!self.remote.contains(&start_offset))
            .then(|| self.append(ChainRecord::Remote(*record)));
        Some(EvictionJob {
            start_offset,
            append,
            messages_path,
        })
    }

    /// Whether the chain holds neither a record of `segment` nor a trim point
    /// that retires it.
    pub(crate) fn lacks_record(&self, segment: &Segment) -> bool {
        !self.sealed.contains_key(&segment.start_offset)
            && self
                .trim_point
                .is_none_or(|trim_point| segment.end_offset > trim_point)
    }

    /// The record of `segment` hashed from disk, with the hash still to fill
    /// in, `None` when the chain already records the segment.
    pub(crate) fn hash_from_disk(
        &self,
        segment: &Segment,
        incarnation: u64,
    ) -> Option<ChainAppend> {
        if !self.lacks_record(segment) {
            return None;
        }
        Some(self.append(ChainRecord::Sealed(SegmentRecord::new(
            segment,
            incarnation,
            SegmentHashes {
                blake3: [0; SEGMENT_HASH_SIZE],
                crc32c: 0,
            },
            HashSource::Disk,
        ))))
    }

    /// Account for the remote record an eviction appended for the segment
    /// starting at `start_offset`.
    pub(crate) fn note_evicted(&mut self, start_offset: u64) {
        if let Some(segment) = self.sealed.get(&start_offset).copied() {
            self.note_appended(&ChainRecord::Remote(segment));
        }
    }

    /// Account for a record that is durable. Returns the advances that the
    /// record showed this replica's segments to contradict.
    pub(crate) fn note_appended(&mut self, record: &ChainRecord) -> Vec<u64> {
        self.valid_len += RECORD_SIZE as u64;
        let mut diverged = Vec::new();
        match record {
            ChainRecord::Sealed(segment) => {
                self.sealed.insert(segment.start_offset, *segment);
                self.settle_advances_in(segment, &mut diverged);
            }
            ChainRecord::Remote(segment) => {
                self.sealed.insert(segment.start_offset, *segment);
                self.remote.insert(segment.start_offset);
                self.settle_advances_in(segment, &mut diverged);
            }
            ChainRecord::Advanced(advance) => {
                self.take_advance(advance.up_to, advance.hash, &mut diverged);
            }
            ChainRecord::TrimPoint(offset) => {
                let trim_point = self
                    .trim_point
                    .map_or(*offset, |trim_point| trim_point.max(*offset));
                self.trim_point = Some(trim_point);
                self.sealed
                    .retain(|_, segment| segment.end_offset > trim_point);
                let sealed = &self.sealed;
                self.remote
                    .retain(|start_offset| sealed.contains_key(start_offset));
                self.confirmed
                    .retain(|start_offset| sealed.contains_key(start_offset));
                self.advances = trim_point
                    .checked_add(1)
                    .map_or_default(|kept| self.advances.split_off(&kept));
            }
        }
        diverged
    }

    fn append(&self, record: ChainRecord) -> ChainAppend {
        ChainAppend {
            path: self.path.clone(),
            at: self.valid_len,
            record,
        }
    }
}

impl<B, SB> IggyPartition<B, SB>
where
    B: MessageBus,
    SB: SuperblockStore,
{
    /// Start keeping a chain: create it holding `prefix` when the partition
    /// directory has none, or load the one there. The active segment is hashed
    /// on the commit path only when it holds no bytes yet.
    ///
    /// # Errors
    /// Returns an error when the partition has no directory, the chain cannot
    /// be created or read, or it records another prefix.
    pub async fn open_segment_chain(&mut self, prefix: ObjectPrefix) -> Result<(), ChainError> {
        let directory = self.partition_dir.clone().ok_or(ChainError::NoDirectory)?;
        let chain = open(&directory, prefix).await?;
        self.adopt_segment_chain(&chain)
    }

    /// Keep the chain boot recovered beside the segments.
    ///
    /// # Errors
    /// [`ChainError::NoDirectory`] when the partition has no directory, and
    /// [`ChainError::Prefix`] when the chain names another partition or
    /// incarnation.
    pub fn adopt_segment_chain(&mut self, chain: &Chain) -> Result<(), ChainError> {
        self.adopt_live_chain(LiveChain::new(chain))
    }

    /// [`Self::adopt_segment_chain`] of a chain already reduced.
    pub(crate) fn adopt_live_chain(&mut self, chain: LiveChain) -> Result<(), ChainError> {
        let directory = self
            .partition_dir
            .as_deref()
            .ok_or(ChainError::NoDirectory)?;
        if !self.owns_chain_prefix(&chain.prefix) {
            return Err(ChainError::Prefix {
                recorded: chain.prefix,
            });
        }
        let active = self.log.segments().last();
        self.segment_chain = Some(Box::new(SegmentChain::load(
            chain_path(directory),
            chain,
            active,
        )));
        Ok(())
    }

    /// Whether `prefix` names this partition and its incarnation.
    pub(crate) fn owns_chain_prefix(&self, prefix: &ObjectPrefix) -> bool {
        let namespace = self.namespace();
        (prefix.stream_id, prefix.topic_id, prefix.partition_id)
            == (
                namespace.stream_id() as u64,
                namespace.topic_id() as u64,
                namespace.partition_id() as u64,
            )
            && prefix.created_revision == self.created_revision()
    }

    /// Forget the chain: an install is replacing the segments it describes,
    /// and its sweep replaces or removes the file.
    pub(crate) fn drop_segment_chain(&mut self) {
        self.segment_chain = None;
        self.restart_tiering();
    }

    /// What a state transfer carries of the chain, `None` without one.
    /// `without_log` holds the start offsets of the segments the same transfer
    /// sends no bytes for, ascending.
    pub(crate) fn segment_chain_snapshot(&self, without_log: &[u64]) -> Option<Chain> {
        self.segment_chain
            .as_ref()
            .map(|chain| chain.snapshot(without_log, self.pending_advances.iter()))
    }

    /// The object prefix the chain records, `None` without a chain.
    #[must_use]
    pub fn object_prefix(&self) -> Option<ObjectPrefix> {
        self.segment_chain.as_ref().map(|chain| chain.prefix)
    }

    /// The trim point the chain records, `None` without a chain or a trim
    /// point.
    #[must_use]
    pub fn chain_trim_point(&self) -> Option<u64> {
        self.segment_chain
            .as_ref()
            .and_then(|chain| chain.trim_point)
    }

    /// The chain's latest record of the sealed segment starting at
    /// `start_offset`.
    #[must_use]
    pub fn sealed_segment_record(&self, start_offset: u64) -> Option<SegmentRecord> {
        self.segment_chain
            .as_ref()
            .and_then(|chain| chain.sealed.get(&start_offset).copied())
    }

    /// Match a floor advance against this replica's own record of the sealed
    /// segment from `start_offset` to `end_offset`. Only a segment whose hash
    /// matched is ever evicted, so a replica whose bytes diverged keeps them.
    ///
    /// Returns whether the record matched.
    pub fn confirm_segment_hash(
        &mut self,
        start_offset: u64,
        end_offset: u64,
        hash: &[u8; SEGMENT_HASH_SIZE],
    ) -> bool {
        let Some(chain) = self.segment_chain.as_mut() else {
            return false;
        };
        let matched = chain
            .sealed
            .get(&start_offset)
            .is_some_and(|record| record.end_offset == end_offset && record.hash == *hash);
        if matched {
            chain.confirmed.insert(start_offset);
        }
        matched
    }
}

/// Writes fixed-width little-endian fields in order.
struct FieldWriter<'a> {
    out: &'a mut [u8],
    at: usize,
}

impl<'a> FieldWriter<'a> {
    const fn new(out: &'a mut [u8]) -> Self {
        Self { out, at: 0 }
    }

    fn bytes(&mut self, bytes: &[u8]) {
        self.out[self.at..self.at + bytes.len()].copy_from_slice(bytes);
        self.at += bytes.len();
    }

    fn u64(&mut self, value: u64) {
        self.bytes(&value.to_le_bytes());
    }
}

/// Reads fixed-width little-endian fields in order from a buffer known to hold
/// them.
struct FieldReader<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl<'a> FieldReader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, at: 0 }
    }

    fn array<const N: usize>(&mut self) -> [u8; N] {
        let mut out = [0; N];
        out.copy_from_slice(&self.bytes[self.at..self.at + N]);
        self.at += N;
        out
    }

    fn u64(&mut self) -> u64 {
        u64::from_le_bytes(self.array())
    }
}
