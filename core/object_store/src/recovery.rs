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

//! The record that retires a cluster which lost every disk, written by the
//! cluster rebuilt from its store.
//!
//! The rebuilt cluster writes the record under [`KeyLayout::recovery_key`]
//! before it claims the root with its own identity, and never changes it.
//! The record names the retired cluster and its successor, and gives, per
//! partition, the offsets the successor took from the objects of the
//! retired cluster. A reader of the store that follows the retired
//! cluster's fresh objects accepts the batches of those objects only below
//! each partition's cutover, and ignores the rest: from the cutover on, the
//! partition's log is the successor's.
//!
//! All integers are little-endian:
//!
//! ```text
//! magic u64 "IGGYREC1" | retired_cluster_uid u128 | successor_cluster_uid u128
//! recovered_at u64 | flags u32 | partition count u32
//! per partition, ascending by prefix:
//!   stream_id u64 | topic_id u64 | partition_id u64 | created_revision u64
//!   published_end u64 | proven_end u64 | cutover u64
//! crc32c of every byte before it u32
//! ```
//!
//! `flags` bit 0 says that the successor adopted the batches that no proof
//! names; the other bits are zero. Each end is one past the last offset it
//! covers, so zero means none: `published_end` is where the manifest of the
//! retired cluster ends, `proven_end` where the batches its proofs name
//! end, and `cutover` where the batches the successor took end.

use crate::error::ObjectStoreError;
use crate::layout::{KeyLayout, ObjectPrefix};
use crate::store::{ObjectStore, PutBody, PutMode, PutOptions, UserMetadata};

const MAGIC: u64 = u64::from_le_bytes(*b"IGGYREC1");
const ADOPTED_UNPROVEN: u32 = 1;
const HEADER_LEN: usize = 8 + 16 + 16 + 8 + 4 + 4;
const PARTITION_LEN: usize = 7 * 8;
const CHECKSUM_LEN: usize = 4;

/// The most partitions one record lists, which keeps it below 64 MiB.
pub const RECOVERY_PARTITIONS_MAX: usize = 1 << 20;

/// What the successor took of one partition from the retired cluster.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RecoveredCutover {
    pub prefix: ObjectPrefix,
    /// One past the last offset of the retired cluster's manifest.
    pub published_end: u64,
    /// One past the last offset that proofs of the retired cluster name.
    pub proven_end: u64,
    /// One past the last offset the successor took from the retired
    /// cluster's objects. A reader accepts a batch of those objects only
    /// when it ends at or below it.
    pub cutover: u64,
}

/// The retirement of a cluster that lost every disk, see the module docs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryRecord {
    pub retired_cluster_uid: u128,
    /// The cluster that owns the root from the record on.
    pub successor_cluster_uid: u128,
    /// When the successor wrote the record, in microseconds since the Unix
    /// epoch.
    pub recovered_at: u64,
    /// Whether the successor took the batches that no proof names.
    pub adopted_unproven: bool,
    /// Ascending by prefix.
    pub partitions: Vec<RecoveredCutover>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum RecoveryRecordError {
    #[error("the recovery record is {0} bytes, too short for its fields")]
    Truncated(usize),
    #[error("the recovery record starts with {0:#018x}, a version this build does not read")]
    UnsupportedVersion(u64),
    #[error("the recovery record is {len} bytes, and its {partitions} partitions take {expected}")]
    WrongLength {
        len: usize,
        partitions: u32,
        expected: usize,
    },
    #[error(
        "the recovery record's checksum is {stored:#010x}, and its bytes hash to {computed:#010x}"
    )]
    ChecksumMismatch { stored: u32, computed: u32 },
    #[error("the recovery record sets flags {0:#x}, of which this build does not know some")]
    UnknownFlags(u32),
    #[error("the recovery record lists {0} partitions, more than a record holds")]
    TooManyPartitions(usize),
    #[error("the recovery record's partitions are out of order or repeat one")]
    PartitionOrder,
    #[error("the recovery record's ends of {0:?} do not ascend")]
    Ends(ObjectPrefix),
}

/// What a read of a recovery record found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecoveryRead {
    Missing,
    Found(RecoveryRecord),
    Invalid(RecoveryRecordError),
}

impl RecoveryRecord {
    /// The cutover of `prefix`, `None` for a partition the successor did not
    /// take from the retired cluster.
    #[must_use]
    pub fn cutover(&self, prefix: &ObjectPrefix) -> Option<&RecoveredCutover> {
        self.partitions
            .binary_search_by_key(prefix, |partition| partition.prefix)
            .ok()
            .and_then(|found| self.partitions.get(found))
    }

    /// # Errors
    /// What [`Self::decode`] would refuse of these fields.
    pub fn encode(&self) -> Result<Vec<u8>, RecoveryRecordError> {
        self.check()?;
        let count = u32::try_from(self.partitions.len())
            .map_err(|_| RecoveryRecordError::TooManyPartitions(self.partitions.len()))?;
        let mut bytes =
            Vec::with_capacity(HEADER_LEN + self.partitions.len() * PARTITION_LEN + CHECKSUM_LEN);
        bytes.extend_from_slice(&MAGIC.to_le_bytes());
        bytes.extend_from_slice(&self.retired_cluster_uid.to_le_bytes());
        bytes.extend_from_slice(&self.successor_cluster_uid.to_le_bytes());
        bytes.extend_from_slice(&self.recovered_at.to_le_bytes());
        let flags = if self.adopted_unproven {
            ADOPTED_UNPROVEN
        } else {
            0
        };
        bytes.extend_from_slice(&flags.to_le_bytes());
        bytes.extend_from_slice(&count.to_le_bytes());
        for partition in &self.partitions {
            let prefix = partition.prefix;
            for value in [
                prefix.stream_id,
                prefix.topic_id,
                prefix.partition_id,
                prefix.created_revision,
                partition.published_end,
                partition.proven_end,
                partition.cutover,
            ] {
                bytes.extend_from_slice(&value.to_le_bytes());
            }
        }
        let checksum = crc32c::crc32c(&bytes);
        bytes.extend_from_slice(&checksum.to_le_bytes());
        Ok(bytes)
    }

    /// # Errors
    /// See [`RecoveryRecordError`]. A record that fails here must not be used
    /// in part.
    pub fn decode(bytes: &[u8]) -> Result<Self, RecoveryRecordError> {
        let mut fields = Fields { bytes, position: 0 };
        let magic = fields.u64()?;
        if magic != MAGIC {
            return Err(RecoveryRecordError::UnsupportedVersion(magic));
        }
        let retired_cluster_uid = fields.u128()?;
        let successor_cluster_uid = fields.u128()?;
        let recovered_at = fields.u64()?;
        let flags = fields.u32()?;
        let partitions = fields.u32()?;
        let expected = usize::try_from(partitions)
            .ok()
            .and_then(|count| count.checked_mul(PARTITION_LEN))
            .and_then(|length| length.checked_add(HEADER_LEN + CHECKSUM_LEN))
            .unwrap_or(usize::MAX);
        if bytes.len() != expected {
            return Err(RecoveryRecordError::WrongLength {
                len: bytes.len(),
                partitions,
                expected,
            });
        }
        let body = &bytes[..bytes.len() - CHECKSUM_LEN];
        let stored = u32::from_le_bytes(
            bytes[body.len()..]
                .try_into()
                .map_err(|_| RecoveryRecordError::Truncated(bytes.len()))?,
        );
        let computed = crc32c::crc32c(body);
        if stored != computed {
            return Err(RecoveryRecordError::ChecksumMismatch { stored, computed });
        }
        if flags & !ADOPTED_UNPROVEN != 0 {
            return Err(RecoveryRecordError::UnknownFlags(flags));
        }
        let mut cutovers = Vec::with_capacity(expected / PARTITION_LEN);
        for _ in 0..partitions {
            cutovers.push(RecoveredCutover {
                prefix: ObjectPrefix {
                    stream_id: fields.u64()?,
                    topic_id: fields.u64()?,
                    partition_id: fields.u64()?,
                    created_revision: fields.u64()?,
                },
                published_end: fields.u64()?,
                proven_end: fields.u64()?,
                cutover: fields.u64()?,
            });
        }
        let record = Self {
            retired_cluster_uid,
            successor_cluster_uid,
            recovered_at,
            adopted_unproven: flags & ADOPTED_UNPROVEN != 0,
            partitions: cutovers,
        };
        record.check()?;
        Ok(record)
    }

    /// Store the record under the recovery key of its retired cluster,
    /// unless an object is there already. Returns `true` when this call
    /// wrote it, and `false` when it found one, whatever that one holds.
    ///
    /// # Errors
    /// The refusals of [`Self::encode`] as
    /// [`ObjectStoreError::InvalidRequest`], and any other error of the put.
    pub async fn create<S: ObjectStore>(
        &self,
        store: &S,
        layout: &KeyLayout,
    ) -> Result<bool, ObjectStoreError> {
        let body = self
            .encode()
            .map_err(|error| ObjectStoreError::InvalidRequest(error.to_string()))?;
        let options = PutOptions {
            mode: PutMode::IfAbsent,
            metadata: UserMetadata::default(),
            crc32c: Some(crc32c::crc32c(&body)),
            ..PutOptions::default()
        };
        match store
            .put(
                &layout.recovery_key(self.retired_cluster_uid),
                PutBody::from_bytes(body),
                &options,
            )
            .await
        {
            Ok(()) => Ok(true),
            Err(ObjectStoreError::PreconditionFailed { .. }) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Read the record that retired `retired_cluster_uid` under the root of
    /// `layout`.
    ///
    /// # Errors
    /// Any error of the read but [`ObjectStoreError::NotFound`], which is
    /// [`RecoveryRead::Missing`].
    pub async fn read<S: ObjectStore>(
        store: &S,
        layout: &KeyLayout,
        retired_cluster_uid: u128,
    ) -> Result<RecoveryRead, ObjectStoreError> {
        let key = layout.recovery_key(retired_cluster_uid);
        let head = match store.head(&key).await {
            Ok(head) => head,
            Err(ObjectStoreError::NotFound { .. }) => return Ok(RecoveryRead::Missing),
            Err(error) => return Err(error),
        };
        if head.size == 0 {
            return Ok(RecoveryRead::Invalid(RecoveryRecordError::Truncated(0)));
        }
        let range = match store.get_range(&key, 0..head.size).await {
            Ok(range) => range,
            Err(ObjectStoreError::NotFound { .. }) => return Ok(RecoveryRead::Missing),
            Err(error) => return Err(error),
        };
        Ok(match Self::decode(&range.bytes) {
            Ok(record) if record.retired_cluster_uid == retired_cluster_uid => {
                RecoveryRead::Found(record)
            }
            Ok(_) => RecoveryRead::Invalid(RecoveryRecordError::UnsupportedVersion(MAGIC)),
            Err(error) => RecoveryRead::Invalid(error),
        })
    }

    fn check(&self) -> Result<(), RecoveryRecordError> {
        if self.partitions.len() > RECOVERY_PARTITIONS_MAX {
            return Err(RecoveryRecordError::TooManyPartitions(
                self.partitions.len(),
            ));
        }
        if self
            .partitions
            .windows(2)
            .any(|pair| pair[0].prefix >= pair[1].prefix)
        {
            return Err(RecoveryRecordError::PartitionOrder);
        }
        self.partitions
            .iter()
            .find(|partition| {
                partition.published_end > partition.proven_end
                    || partition.proven_end > partition.cutover
            })
            .map_or(Ok(()), |partition| {
                Err(RecoveryRecordError::Ends(partition.prefix))
            })
    }
}

/// Reads fields off the front of the record.
struct Fields<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl Fields<'_> {
    fn take<const N: usize>(&mut self) -> Result<[u8; N], RecoveryRecordError> {
        let field = self
            .bytes
            .get(self.position..self.position + N)
            .and_then(|field| field.try_into().ok())
            .ok_or(RecoveryRecordError::Truncated(self.bytes.len()))?;
        self.position += N;
        Ok(field)
    }

    fn u128(&mut self) -> Result<u128, RecoveryRecordError> {
        self.take().map(u128::from_le_bytes)
    }

    fn u64(&mut self) -> Result<u64, RecoveryRecordError> {
        self.take().map(u64::from_le_bytes)
    }

    fn u32(&mut self) -> Result<u32, RecoveryRecordError> {
        self.take().map(u32::from_le_bytes)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::executor::block_on;

    use super::*;
    use crate::memory::MemoryStore;

    const RETIRED: u128 = 0x0a0a_0a0a_0a0a_0a0a_0a0a_0a0a_0a0a_0a0a;
    const SUCCESSOR: u128 = 0x0b0b_0b0b_0b0b_0b0b_0b0b_0b0b_0b0b_0b0b;

    fn layout() -> KeyLayout {
        KeyLayout::new("tenant/iggy").unwrap()
    }

    fn prefix(partition_id: u64) -> ObjectPrefix {
        ObjectPrefix {
            stream_id: 1,
            topic_id: 2,
            partition_id,
            created_revision: 9,
        }
    }

    fn record() -> RecoveryRecord {
        RecoveryRecord {
            retired_cluster_uid: RETIRED,
            successor_cluster_uid: SUCCESSOR,
            recovered_at: 1_700_000_000_000_000,
            adopted_unproven: true,
            partitions: vec![
                RecoveredCutover {
                    prefix: prefix(0),
                    published_end: 100,
                    proven_end: 140,
                    cutover: 150,
                },
                RecoveredCutover {
                    prefix: prefix(1),
                    published_end: 0,
                    proven_end: 0,
                    cutover: 0,
                },
            ],
        }
    }

    #[test]
    fn given_a_record_when_encoding_should_decode_to_the_same_record() {
        for adopted_unproven in [true, false] {
            let record = RecoveryRecord {
                adopted_unproven,
                ..record()
            };

            let encoded = record.encode().unwrap();

            assert_eq!(encoded.len(), HEADER_LEN + 2 * PARTITION_LEN + CHECKSUM_LEN);
            assert_eq!(RecoveryRecord::decode(&encoded), Ok(record.clone()));
            assert_eq!(record.cutover(&prefix(0)).map(|cut| cut.cutover), Some(150));
            assert_eq!(record.cutover(&prefix(2)), None);
        }
    }

    #[test]
    fn given_damaged_bytes_when_decoding_should_refuse_them() {
        let encoded = record().encode().unwrap();
        for len in [0, 8, HEADER_LEN, encoded.len() - 1] {
            assert!(RecoveryRecord::decode(&encoded[..len]).is_err(), "{len}");
        }
        for at in [0, 9, HEADER_LEN + 3, encoded.len() - 1] {
            let mut damaged = encoded.clone();
            damaged[at] ^= 1;
            assert!(RecoveryRecord::decode(&damaged).is_err(), "byte {at}");
        }
        let mut longer = encoded;
        longer.push(0);
        assert!(matches!(
            RecoveryRecord::decode(&longer),
            Err(RecoveryRecordError::WrongLength { .. })
        ));
    }

    #[test]
    fn given_fields_a_reader_would_refuse_when_encoding_should_refuse_them() {
        let mut unordered = record();
        unordered.partitions.reverse();
        let mut repeated = record();
        repeated.partitions[1].prefix = prefix(0);
        let mut descending = record();
        descending.partitions[0].proven_end = 90;
        let mut past_cutover = record();
        past_cutover.partitions[0].cutover = 120;

        assert_eq!(unordered.encode(), Err(RecoveryRecordError::PartitionOrder));
        assert_eq!(repeated.encode(), Err(RecoveryRecordError::PartitionOrder));
        assert_eq!(
            descending.encode(),
            Err(RecoveryRecordError::Ends(prefix(0)))
        );
        assert_eq!(
            past_cutover.encode(),
            Err(RecoveryRecordError::Ends(prefix(0)))
        );
    }

    #[test]
    fn given_a_free_key_when_creating_twice_should_keep_the_first_record() {
        let store = MemoryStore::new();
        let other = RecoveryRecord {
            successor_cluster_uid: 7,
            ..record()
        };

        assert!(block_on(record().create(&store, &layout())).unwrap());
        assert!(!block_on(other.create(&store, &layout())).unwrap());

        assert_eq!(
            store.keys(),
            vec![format!("tenant/iggy/meta/recovery/{RETIRED:032x}/record")]
        );
        assert_eq!(
            block_on(RecoveryRecord::read(&store, &layout(), RETIRED)).unwrap(),
            RecoveryRead::Found(record())
        );
        assert_eq!(
            block_on(RecoveryRecord::read(&store, &layout(), SUCCESSOR)).unwrap(),
            RecoveryRead::Missing
        );
    }

    #[test]
    fn given_bytes_no_record_wrote_when_reading_should_return_invalid() {
        let store = MemoryStore::new();
        block_on(store.put(
            &layout().recovery_key(RETIRED),
            PutBody::from_bytes(Bytes::from_static(b"not a record")),
            &PutOptions::default(),
        ))
        .unwrap();

        assert!(matches!(
            block_on(RecoveryRecord::read(&store, &layout(), RETIRED)).unwrap(),
            RecoveryRead::Invalid(_)
        ));
    }
}
