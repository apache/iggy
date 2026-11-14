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

use std::collections::BTreeMap;
use std::fmt;
use std::io;

use compio::io::AsyncWriteAtExt;
use consensus::state_artifact_checksum;
pub use iggy_object_store::ObjectPrefix;
use iggy_object_store::SEGMENT_HASH_LEN;
use journal::superblock::SuperblockStore;
use message_bus::MessageBus;
use server_common::fatal::NoteDescriptorExhaustion;

use crate::IggyPartition;
use crate::segment::Segment;
use crate::state_transfer::{ChunkHasher, STAGING_SUFFIX, hash_segment_range};

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

const HEADER_MAGIC: u64 = u64::from_le_bytes(*b"IGGYCHN1");
const HEADER_CHECKSUM_AT: usize = HEADER_SIZE - size_of::<u64>();
const RECORD_HASH_AT: usize = 64;
const RECORD_CHECKSUM_AT: usize = RECORD_SIZE - size_of::<u64>();

const SEALED: u8 = 1;
const REMOTE: u8 = 2;
const TRIM_POINT: u8 = 3;

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
}

impl ChainRecord {
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
    let path = chain_path(partition_dir);
    // A staging file every sweep already removes, renamed into place, so a
    // crash never leaves a torn header under the chain's name.
    let staging = format!("{path}{STAGING_SUFFIX}");
    let mut file = compio::fs::File::create(&staging)
        .await
        .note_descriptor_exhaustion(|| format!("creating {staging}"))?;
    file.write_all_at(encode_header(prefix), 0).await.0?;
    file.sync_all().await?;
    compio::fs::rename(&staging, &path).await?;
    crate::state_transfer::fsync_dir(partition_dir).await
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
struct SegmentHasher {
    blake3: blake3::Hasher,
    crc32c: u32,
}

impl SegmentHasher {
    fn finalize(&self) -> SegmentHashes {
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
}

impl ChunkHasher for SegmentDigest {
    fn update(&mut self, bytes: &[u8]) {
        self.hasher.update(bytes);
        self.hashed_len += bytes.len() as u64;
    }
}

/// What a partition with a chain keeps of it between appends.
pub(crate) struct SegmentChain {
    path: String,
    prefix: ObjectPrefix,
    valid_len: u64,
    /// The latest record of every sealed segment, by start offset.
    sealed: BTreeMap<u64, SegmentRecord>,
    /// The digest of the active segment, `None` once any of its bytes reached
    /// the segment without passing through it.
    digest: Option<Box<SegmentDigest>>,
}

/// A record the rotation job appends before it creates the next segment, so the
/// sealed segment's hash is durable from the moment it exists.
pub(crate) struct ChainAppend {
    pub(crate) path: String,
    pub(crate) at: u64,
    pub(crate) record: ChainRecord,
}

impl ChainAppend {
    pub(crate) async fn execute(&self) -> io::Result<()> {
        append_record(&self.path, self.at, &self.record).await
    }
}

impl SegmentChain {
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
        Some(ChainAppend {
            path: self.path.clone(),
            at: self.valid_len,
            record: ChainRecord::Sealed(SegmentRecord::new(
                segment,
                incarnation,
                digest.finalize(),
                HashSource::CommitPath,
            )),
        })
    }

    /// Account for a seal: the appended record, if there was one, and a fresh
    /// digest for the segment the rotation planted at `next_start`.
    pub(crate) fn sealed(&mut self, appended: Option<&ChainRecord>, next_start: u64) {
        if let Some(record) = appended {
            self.note_appended(record);
        }
        self.digest = Some(Box::new(SegmentDigest::new(next_start)));
    }

    fn note_appended(&mut self, record: &ChainRecord) {
        self.valid_len += RECORD_SIZE as u64;
        if let ChainRecord::Sealed(segment) | ChainRecord::Remote(segment) = record {
            self.sealed.insert(segment.start_offset, *segment);
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
        let chain = if let Some(chain) = read_chain(&directory).await? {
            if chain.prefix != prefix {
                return Err(ChainError::Prefix {
                    recorded: chain.prefix,
                });
            }
            // The chain may be here by a rename no directory sync followed, and
            // a record appended to a name a power loss takes back is lost.
            crate::state_transfer::fsync_dir(&directory).await?;
            chain
        } else {
            create_chain(&directory, &prefix).await?;
            Chain {
                prefix,
                records: Vec::new(),
                valid_len: HEADER_SIZE as u64,
                torn_tail: false,
            }
        };
        let mut sealed = BTreeMap::new();
        for record in &chain.records {
            if let ChainRecord::Sealed(segment) | ChainRecord::Remote(segment) = record {
                sealed.insert(segment.start_offset, *segment);
            }
        }
        let active = self.log.active_segment();
        let digest = (active.size.as_bytes_u64() == 0)
            .then(|| Box::new(SegmentDigest::new(active.start_offset)));
        self.segment_chain = Some(SegmentChain {
            path: chain_path(&directory),
            prefix: chain.prefix,
            valid_len: chain.valid_len,
            sealed,
            digest,
        });
        Ok(())
    }

    /// Stop hashing the active segment on the commit path: bytes reached it
    /// another way, as a state transfer install does.
    pub(crate) fn discard_segment_digest(&mut self) {
        if let Some(chain) = self.segment_chain.as_mut() {
            chain.digest = None;
        }
    }

    /// The object prefix the chain records, `None` without a chain.
    #[must_use]
    pub fn object_prefix(&self) -> Option<ObjectPrefix> {
        self.segment_chain.as_ref().map(|chain| chain.prefix)
    }

    /// The chain's latest record of the sealed segment starting at
    /// `start_offset`.
    #[must_use]
    pub fn sealed_segment_record(&self, start_offset: u64) -> Option<SegmentRecord> {
        self.segment_chain
            .as_ref()
            .and_then(|chain| chain.sealed.get(&start_offset).copied())
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
