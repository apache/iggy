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

//! The positions of the consumer groups of one log, as one node of the
//! cluster that reads the log holds them: what carries consumer positions
//! between datacenters.
//!
//! A log is the messages of a geo topic as its origin wrote them, named by
//! [`GeoLog`]. Each node of each cluster that holds a topic of the log, the
//! geo topic itself or a shadow topic of it, writes one object for it into
//! its own store under [`KeyLayout::positions_key`]. Every other cluster
//! reads those objects and takes, for each group and partition, the highest
//! [`GeoPosition`].
//!
//! All integers are little-endian. A name is its length in one byte and as
//! many bytes of UTF-8, and it is never empty:
//!
//! ```text
//! magic u64 | origin | stream_id u64 | topic_id u64 | created_revision u64
//! replica u8 | published_at u64 | partition_count u32
//! per partition, ids ascending:
//!   partition_id u32 | entry_count u32
//!   per entry, names ascending:
//!     group name | reset u64 | kind u8 | offset u64
//! crc32c of every byte before it u32
//! ```
//!
//! The magic names the version. A reader refuses a magic it does not know.

use std::collections::BTreeMap;

use crate::error::ObjectStoreError;
use crate::layout::KeyLayout;
use crate::store::{ObjectStore, PutBody, PutOptions};

const MAGIC: u64 = u64::from_le_bytes(*b"IGGYPOS1");
const CHECKSUM_LEN: usize = size_of::<u32>();

/// The longest group name, in bytes: the most that its one-byte length says.
pub const MAX_GROUP_NAME_LEN: usize = u8::MAX as usize;

/// The most groups one partition of one object names.
pub const POSITIONS_PER_PARTITION_MAX: usize = 4_096;

/// The most bytes a positions object takes.
pub const MAX_POSITIONS_OBJECT_LEN: usize = 16 * 1024 * 1024;

const KIND_DELETED: u8 = 0;
const KIND_UNSET: u8 = 1;
const KIND_OFFSET: u8 = 2;

/// One log, as its origin names it. The ids and the revision are the
/// origin's, which a shadow topic keeps, so every cluster names a log the
/// same way.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GeoLog {
    /// The `geo.origin` of the cluster that writes the log.
    pub origin: String,
    pub stream_id: u64,
    pub topic_id: u64,
    /// The revision at which the origin created the topic's partitions.
    pub created_revision: u64,
}

/// Where one node keeps its positions of one log.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct PositionsKey {
    pub log: GeoLog,
    /// The replica id of the node that wrote the object.
    pub replica: u8,
}

/// What a group's position on a partition is. The order is the merge order
/// within one reset: a tombstone is below a deleted offset, which is below
/// any offset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum PositionValue {
    /// The group was deleted.
    Deleted,
    /// The group's offset was deleted, so the group starts over.
    Unset,
    /// The last offset the group consumed.
    Offset(u64),
}

/// A group's position on one partition.
///
/// Of two positions the one with the higher reset wins, and within one reset
/// the higher value. A reset counts the times that a cluster moved the group
/// back, so a seek backwards wins over a higher offset that another cluster
/// had before the seek.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GeoPosition {
    pub reset: u64,
    pub value: PositionValue,
}

/// The positions of the groups of one log on one node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PositionsObject {
    pub key: PositionsKey,
    /// When the node built the object, in microseconds since the Unix epoch.
    pub published_at: u64,
    /// By partition id, then by group name.
    pub partitions: BTreeMap<u32, BTreeMap<String, GeoPosition>>,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PositionsObjectError {
    #[error("the positions object is {0} bytes, too short for its fields")]
    Truncated(usize),
    #[error("the positions object starts with {0:#018x}, a version this build does not read")]
    UnsupportedVersion(u64),
    #[error("the positions object is {len} bytes, and its fields take {expected}")]
    WrongLength { len: usize, expected: usize },
    #[error("the positions object is {0} bytes, more than {MAX_POSITIONS_OBJECT_LEN}")]
    TooLarge(usize),
    #[error(
        "the positions object's checksum is {stored:#010x}, and its bytes hash to {computed:#010x}"
    )]
    ChecksumMismatch { stored: u32, computed: u32 },
    #[error("the positions object's {0} is empty, longer than 255 bytes or not UTF-8")]
    InvalidName(&'static str),
    #[error("the positions object's {0} holds a value that this build does not know")]
    InvalidField(&'static str),
    #[error("the positions object lists {0} out of order or twice")]
    Unordered(&'static str),
    #[error("a partition of the positions object names {0} groups, more than the most one holds")]
    TooManyGroups(usize),
    #[error("the positions object under the key of {expected:?} describes {found:?}")]
    KeyMismatch {
        expected: Box<PositionsKey>,
        found: Box<PositionsKey>,
    },
}

impl PositionValue {
    const fn code(self) -> (u8, u64) {
        match self {
            Self::Deleted => (KIND_DELETED, 0),
            Self::Unset => (KIND_UNSET, 0),
            Self::Offset(offset) => (KIND_OFFSET, offset),
        }
    }

    const fn from_code(kind: u8, offset: u64) -> Option<Self> {
        match (kind, offset) {
            (KIND_DELETED, 0) => Some(Self::Deleted),
            (KIND_UNSET, 0) => Some(Self::Unset),
            (KIND_OFFSET, offset) => Some(Self::Offset(offset)),
            _ => None,
        }
    }

    /// The value of a group whose stored offset is `offset`.
    #[must_use]
    pub const fn of_offset(offset: Option<u64>) -> Self {
        match offset {
            Some(offset) => Self::Offset(offset),
            None => Self::Unset,
        }
    }
}

impl PositionsObject {
    /// # Errors
    /// [`PositionsObjectError::InvalidName`] for an empty origin or group
    /// name or one longer than 255 bytes,
    /// [`PositionsObjectError::TooManyGroups`] for a partition with more
    /// than [`POSITIONS_PER_PARTITION_MAX`] groups, and
    /// [`PositionsObjectError::TooLarge`] past
    /// [`MAX_POSITIONS_OBJECT_LEN`].
    pub fn encode(&self) -> Result<Vec<u8>, PositionsObjectError> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&MAGIC.to_le_bytes());
        put_name(&mut bytes, "origin", &self.key.log.origin)?;
        for id in [
            self.key.log.stream_id,
            self.key.log.topic_id,
            self.key.log.created_revision,
        ] {
            bytes.extend_from_slice(&id.to_le_bytes());
        }
        bytes.push(self.key.replica);
        bytes.extend_from_slice(&self.published_at.to_le_bytes());
        let partitions = u32::try_from(self.partitions.len())
            .map_err(|_| PositionsObjectError::TooLarge(usize::MAX))?;
        bytes.extend_from_slice(&partitions.to_le_bytes());
        for (partition_id, groups) in &self.partitions {
            if groups.len() > POSITIONS_PER_PARTITION_MAX {
                return Err(PositionsObjectError::TooManyGroups(groups.len()));
            }
            bytes.extend_from_slice(&partition_id.to_le_bytes());
            let count = u32::try_from(groups.len())
                .map_err(|_| PositionsObjectError::TooManyGroups(groups.len()))?;
            bytes.extend_from_slice(&count.to_le_bytes());
            for (name, position) in groups {
                put_name(&mut bytes, "group name", name)?;
                let (kind, offset) = position.value.code();
                bytes.extend_from_slice(&position.reset.to_le_bytes());
                bytes.push(kind);
                bytes.extend_from_slice(&offset.to_le_bytes());
            }
            if bytes.len() + CHECKSUM_LEN > MAX_POSITIONS_OBJECT_LEN {
                return Err(PositionsObjectError::TooLarge(bytes.len() + CHECKSUM_LEN));
            }
        }
        let checksum = crc32c::crc32c(&bytes);
        bytes.extend_from_slice(&checksum.to_le_bytes());
        Ok(bytes)
    }

    /// # Errors
    /// See [`PositionsObjectError`]. An object that fails here must not be
    /// used in part.
    pub fn decode(bytes: &[u8]) -> Result<Self, PositionsObjectError> {
        if bytes.len() > MAX_POSITIONS_OBJECT_LEN {
            return Err(PositionsObjectError::TooLarge(bytes.len()));
        }
        let body_len = bytes
            .len()
            .checked_sub(CHECKSUM_LEN)
            .ok_or(PositionsObjectError::Truncated(bytes.len()))?;
        let mut reader = Reader::new(&bytes[..body_len]);
        let magic = reader.u64()?;
        if magic != MAGIC {
            return Err(PositionsObjectError::UnsupportedVersion(magic));
        }
        let stored = u32::from_le_bytes(
            bytes[body_len..]
                .try_into()
                .map_err(|_| PositionsObjectError::Truncated(bytes.len()))?,
        );
        let computed = crc32c::crc32c(&bytes[..body_len]);
        if stored != computed {
            return Err(PositionsObjectError::ChecksumMismatch { stored, computed });
        }
        let origin = utf8("origin", reader.name()?)?;
        let stream_id = reader.u64()?;
        let topic_id = reader.u64()?;
        let created_revision = reader.u64()?;
        let replica = reader.u8()?;
        let published_at = reader.u64()?;
        let partition_count = reader.u32()?;
        let mut partitions = BTreeMap::new();
        let mut last_partition = None;
        for _ in 0..partition_count {
            let partition_id = reader.u32()?;
            if last_partition.is_some_and(|last| last >= partition_id) {
                return Err(PositionsObjectError::Unordered("partitions"));
            }
            last_partition = Some(partition_id);
            let count = reader.u32()? as usize;
            if count > POSITIONS_PER_PARTITION_MAX {
                return Err(PositionsObjectError::TooManyGroups(count));
            }
            let mut groups = BTreeMap::new();
            let mut last_name: Option<String> = None;
            for _ in 0..count {
                let name = utf8("group name", reader.name()?)?;
                if last_name.as_ref().is_some_and(|last| *last >= name) {
                    return Err(PositionsObjectError::Unordered("groups"));
                }
                let reset = reader.u64()?;
                let kind = reader.u8()?;
                let offset = reader.u64()?;
                let value = PositionValue::from_code(kind, offset)
                    .ok_or(PositionsObjectError::InvalidField("kind"))?;
                last_name = Some(name.clone());
                groups.insert(name, GeoPosition { reset, value });
            }
            partitions.insert(partition_id, groups);
        }
        if reader.left() != 0 {
            return Err(PositionsObjectError::WrongLength {
                len: bytes.len(),
                expected: bytes.len() - reader.left(),
            });
        }
        Ok(Self {
            key: PositionsKey {
                log: GeoLog {
                    origin,
                    stream_id,
                    topic_id,
                    created_revision,
                },
                replica,
            },
            published_at,
            partitions,
        })
    }

    /// Store `body`, which [`Self::encode`] made of the object under `key`,
    /// in the root of `layout`, over any object there.
    ///
    /// # Errors
    /// Any error of the put.
    pub async fn put_encoded<S: ObjectStore>(
        store: &S,
        layout: &KeyLayout,
        key: &PositionsKey,
        body: Vec<u8>,
    ) -> Result<(), ObjectStoreError> {
        let options = PutOptions {
            crc32c: Some(crc32c::crc32c(&body)),
            ..PutOptions::default()
        };
        store
            .put(
                &layout.positions_key(key),
                PutBody::from_bytes(body),
                &options,
            )
            .await
    }

    /// The object that `bytes`, read under the key of `expected`, holds.
    ///
    /// # Errors
    /// See [`PositionsObjectError`], and
    /// [`PositionsObjectError::KeyMismatch`] for an object that describes
    /// another log or node than its key.
    pub fn decode_under(
        expected: &PositionsKey,
        bytes: &[u8],
    ) -> Result<Self, PositionsObjectError> {
        let object = Self::decode(bytes)?;
        if object.key != *expected {
            return Err(PositionsObjectError::KeyMismatch {
                expected: Box::new(expected.clone()),
                found: Box::new(object.key),
            });
        }
        Ok(object)
    }
}

fn put_name(
    bytes: &mut Vec<u8>,
    field: &'static str,
    name: &str,
) -> Result<(), PositionsObjectError> {
    let len = u8::try_from(name.len())
        .ok()
        .filter(|&len| len > 0)
        .ok_or(PositionsObjectError::InvalidName(field))?;
    bytes.push(len);
    bytes.extend_from_slice(name.as_bytes());
    Ok(())
}

fn utf8(field: &'static str, bytes: &[u8]) -> Result<String, PositionsObjectError> {
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|_| PositionsObjectError::InvalidName(field))
}

/// Reads fields off the front of the bytes. A field past the end is
/// [`PositionsObjectError::Truncated`].
struct Reader<'a> {
    left: &'a [u8],
    len: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self {
            left: bytes,
            len: bytes.len(),
        }
    }

    const fn left(&self) -> usize {
        self.left.len()
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], PositionsObjectError> {
        let (taken, left) = self
            .left
            .split_at_checked(count)
            .ok_or(PositionsObjectError::Truncated(self.len))?;
        self.left = left;
        Ok(taken)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], PositionsObjectError> {
        self.take(N)?
            .try_into()
            .map_err(|_| PositionsObjectError::Truncated(self.len))
    }

    fn u8(&mut self) -> Result<u8, PositionsObjectError> {
        Ok(self.array::<1>()?[0])
    }

    fn u32(&mut self) -> Result<u32, PositionsObjectError> {
        self.array().map(u32::from_le_bytes)
    }

    fn u64(&mut self) -> Result<u64, PositionsObjectError> {
        self.array().map(u64::from_le_bytes)
    }

    fn name(&mut self) -> Result<&'a [u8], PositionsObjectError> {
        let len = self.u8()?;
        if len == 0 {
            return Err(PositionsObjectError::InvalidName("name"));
        }
        self.take(usize::from(len))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn object() -> PositionsObject {
        PositionsObject {
            key: PositionsKey {
                log: GeoLog {
                    origin: "dc-a".to_owned(),
                    stream_id: 3,
                    topic_id: 7,
                    created_revision: 41,
                },
                replica: 2,
            },
            published_at: 1_700_000_000_000_000,
            partitions: BTreeMap::from([
                (
                    0,
                    BTreeMap::from([
                        (
                            "billing".to_owned(),
                            GeoPosition {
                                reset: 1,
                                value: PositionValue::Offset(99),
                            },
                        ),
                        (
                            "gone".to_owned(),
                            GeoPosition {
                                reset: 4,
                                value: PositionValue::Deleted,
                            },
                        ),
                        (
                            "restart".to_owned(),
                            GeoPosition {
                                reset: 2,
                                value: PositionValue::Unset,
                            },
                        ),
                    ]),
                ),
                (5, BTreeMap::new()),
            ]),
        }
    }

    #[test]
    fn given_positions_when_round_tripping_should_keep_every_entry() {
        let object = object();
        let bytes = object.encode().unwrap();

        assert_eq!(PositionsObject::decode(&bytes).unwrap(), object);
        assert_eq!(
            PositionsObject::decode_under(&object.key, &bytes).unwrap(),
            object
        );
    }

    #[test]
    fn given_a_flipped_byte_when_decoding_should_refuse_the_object() {
        let bytes = object().encode().unwrap();
        for at in [9, 20, bytes.len() / 2, bytes.len() - 5] {
            let mut damaged = bytes.clone();
            damaged[at] ^= 0x40;
            assert!(
                matches!(
                    PositionsObject::decode(&damaged),
                    Err(PositionsObjectError::ChecksumMismatch { .. })
                ),
                "byte {at}"
            );
        }
    }

    #[test]
    fn given_cut_or_padded_bytes_when_decoding_should_refuse_them() {
        let bytes = object().encode().unwrap();
        for len in [0, 3, 8, 30, bytes.len() - 1] {
            assert!(PositionsObject::decode(&bytes[..len]).is_err(), "{len}");
        }
        let mut padded = bytes[..bytes.len() - CHECKSUM_LEN].to_vec();
        padded.push(0);
        let checksum = crc32c::crc32c(&padded);
        padded.extend_from_slice(&checksum.to_le_bytes());
        assert!(matches!(
            PositionsObject::decode(&padded),
            Err(PositionsObjectError::WrongLength { .. })
        ));
    }

    #[test]
    fn given_another_magic_when_decoding_should_name_the_version() {
        let mut bytes = object().encode().unwrap();
        bytes[..8].copy_from_slice(b"IGGYPOS2");
        assert!(matches!(
            PositionsObject::decode(&bytes),
            Err(PositionsObjectError::UnsupportedVersion(_))
        ));
    }

    #[test]
    fn given_an_unknown_kind_when_decoding_should_refuse_it() {
        let mut object = object();
        object.partitions.retain(|id, _| *id == 0);
        object
            .partitions
            .get_mut(&0)
            .unwrap()
            .retain(|name, _| name == "billing");
        let mut bytes = object.encode().unwrap();
        let kind_at = bytes.len() - CHECKSUM_LEN - size_of::<u64>() - 1;
        bytes[kind_at] = 9;
        let body_len = bytes.len() - CHECKSUM_LEN;
        let checksum = crc32c::crc32c(&bytes[..body_len]);
        bytes[body_len..].copy_from_slice(&checksum.to_le_bytes());

        assert_eq!(
            PositionsObject::decode(&bytes),
            Err(PositionsObjectError::InvalidField("kind"))
        );
    }

    #[test]
    fn given_an_object_of_another_node_when_decoding_under_a_key_should_refuse_it() {
        let object = object();
        let bytes = object.encode().unwrap();
        let other = PositionsKey {
            replica: 0,
            ..object.key
        };
        assert!(matches!(
            PositionsObject::decode_under(&other, &bytes),
            Err(PositionsObjectError::KeyMismatch { .. })
        ));
    }

    #[test]
    fn given_too_many_groups_when_encoding_should_refuse_the_partition() {
        let mut object = object();
        let groups = (0..=POSITIONS_PER_PARTITION_MAX)
            .map(|index| {
                (
                    format!("group-{index:05}"),
                    GeoPosition {
                        reset: 0,
                        value: PositionValue::Offset(1),
                    },
                )
            })
            .collect();
        object.partitions.insert(9, groups);
        assert_eq!(
            object.encode(),
            Err(PositionsObjectError::TooManyGroups(
                POSITIONS_PER_PARTITION_MAX + 1
            ))
        );
    }

    #[test]
    fn given_positions_when_ordering_should_put_the_reset_before_the_value() {
        let position = |reset, value| GeoPosition { reset, value };
        let ordered = [
            position(0, PositionValue::Deleted),
            position(0, PositionValue::Unset),
            position(0, PositionValue::Offset(0)),
            position(0, PositionValue::Offset(500)),
            position(1, PositionValue::Deleted),
            position(1, PositionValue::Offset(3)),
            position(2, PositionValue::Unset),
        ];
        for pair in ordered.windows(2) {
            assert!(pair[0] < pair[1], "{pair:?}");
        }
    }
}
