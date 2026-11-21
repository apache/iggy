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

//! The object of a geo topic: what another cluster needs to adopt the topic,
//! for a reader that has the bucket and nothing else.
//!
//! The metadata primary of the cluster that owns the topic, its origin,
//! writes the object under [`KeyLayout::topic_key`] after the topic is
//! created. The primary of each of the topic's partitions takes no produce
//! before it reads the object back, so a reader never finds data of a topic
//! that it cannot describe. When the topic is deleted, the origin replaces
//! the object with a tombstone and keeps the topic's objects for a grace
//! period. Then it deletes them, and the tombstone last.
//!
//! All integers are little-endian. A name is its length in one byte and as
//! many bytes of UTF-8, and it is never empty:
//!
//! ```text
//! magic u64 | state u8 | cluster_uid u128 | origin
//! stream_id u64 | stream name | topic_id u64 | topic name
//! created_revision u64 | partition_count u32
//! compression u8 | max_message_size u64
//! message_expiry u64 | max_topic_size u64 | geo_mode u8
//! created_at u64 | published_at u64 | deleted_at u64 | tombstone_grace u64
//! crc32c of every byte before it u32
//! ```
//!
//! The magic names the version. A reader refuses a magic it does not know.

use crate::error::ObjectStoreError;
use crate::layout::{KeyLayout, TopicKey};
use crate::store::{ObjectStore, PutBody, PutOptions};

const MAGIC: u64 = u64::from_le_bytes(*b"IGGYTOP2");
/// The longest name, in bytes: the most that its one-byte length says.
pub const MAX_TOPIC_NAME_LEN: usize = u8::MAX as usize;
/// The bytes of every field but the bytes of the three names.
const FIXED_LEN: usize = 118;
const CHECKSUM_LEN: usize = size_of::<u32>();
/// The most bytes that a topic object takes: three names of the longest
/// length.
pub const MAX_TOPIC_OBJECT_LEN: usize = FIXED_LEN + 3 * MAX_TOPIC_NAME_LEN;

const LIVE: u8 = 1;
const TOMBSTONE: u8 = 2;

/// Whether the topic still exists in its origin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicState {
    Live,
    /// The origin deleted the topic. `deleted_at` is when it wrote the
    /// tombstone, in microseconds since the Unix epoch. The topic's objects
    /// stay in the store for a grace period after that.
    Tombstone {
        deleted_at: u64,
    },
}

/// The codec of the topic's batches. A stored batch carries no codec marker,
/// so a reader needs it. The codes are those of the binary protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicCompression {
    None,
    Gzip,
}

impl TopicCompression {
    #[must_use]
    pub const fn code(self) -> u8 {
        match self {
            Self::None => 1,
            Self::Gzip => 2,
        }
    }

    #[must_use]
    pub const fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::None),
            2 => Some(Self::Gzip),
            _ => None,
        }
    }
}

/// How a geo topic writes its messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicGeoMode {
    /// Upload first: a batch reaches the store before it commits.
    Geo,
    /// Replicate first and upload later, as a tiered topic does.
    GeoTiered,
}

impl TopicGeoMode {
    #[must_use]
    pub const fn code(self) -> u8 {
        match self {
            Self::Geo => 1,
            Self::GeoTiered => 2,
        }
    }

    #[must_use]
    pub const fn from_code(code: u8) -> Option<Self> {
        match code {
            1 => Some(Self::Geo),
            2 => Some(Self::GeoTiered),
            _ => None,
        }
    }
}

/// What a reader of the store learns about one geo topic incarnation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicObject {
    /// The incarnation, which also names the object's key.
    pub key: TopicKey,
    /// The `geo.origin` of the cluster that owns the topic.
    pub origin: String,
    /// The id of the cluster lifetime that owns the store, as the store
    /// identity object names it.
    pub cluster_uid: u128,
    pub stream_name: String,
    pub topic_name: String,
    pub partition_count: u32,
    pub compression: TopicCompression,
    /// The largest wire message that the origin takes, in bytes. No batch
    /// that a reader finds is larger.
    pub max_message_size: u64,
    /// How long a message lives, in microseconds, `u64::MAX` when messages
    /// never expire.
    pub message_expiry: u64,
    /// The most bytes that the topic holds, `u64::MAX` without a limit.
    pub max_topic_size: u64,
    pub geo_mode: TopicGeoMode,
    /// When the origin created the topic, in microseconds since the Unix
    /// epoch, like the next timestamp.
    pub created_at: u64,
    /// When the origin built this object.
    pub published_at: u64,
    pub state: TopicState,
    /// How long the origin keeps the topic's objects after it writes the
    /// tombstone, in microseconds, as the topic sets it. Zero when the topic
    /// sets none: the `[geo] tombstone_grace` of the sweeping node applies.
    /// Readers in other datacenters do not use it.
    pub tombstone_grace: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum TopicObjectError {
    #[error("the topic object is {0} bytes, too short for its fields")]
    Truncated(usize),
    #[error("the topic object starts with {0:#018x}, a version this build does not read")]
    UnsupportedVersion(u64),
    #[error("the topic object is {len} bytes, and its fields take {expected}")]
    WrongLength { len: usize, expected: usize },
    #[error(
        "the topic object's checksum is {stored:#010x}, and its bytes hash to {computed:#010x}"
    )]
    ChecksumMismatch { stored: u32, computed: u32 },
    #[error("the topic object's {0} is empty, longer than 255 bytes or not UTF-8")]
    InvalidName(&'static str),
    #[error("the topic object's {0} holds a value that this build does not know")]
    InvalidField(&'static str),
    #[error("the topic object under the key of {expected:?} describes {found:?}")]
    KeyMismatch { expected: TopicKey, found: TopicKey },
}

/// What a read of a geo topic object found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopicRead {
    /// No object is under the key.
    Missing,
    Found(TopicObject),
    /// Bytes that this build cannot read as the object of the topic under
    /// the key.
    Invalid(TopicObjectError),
}

impl TopicObject {
    /// Whether the origin still has the topic.
    #[must_use]
    pub const fn is_live(&self) -> bool {
        matches!(self.state, TopicState::Live)
    }

    /// This object as the tombstone that the origin writes at `deleted_at`.
    #[must_use]
    pub fn into_tombstone(self, deleted_at: u64) -> Self {
        Self {
            state: TopicState::Tombstone { deleted_at },
            ..self
        }
    }

    /// # Errors
    /// [`TopicObjectError::InvalidName`] for an empty name or one longer than
    /// `MAX_TOPIC_NAME_LEN` bytes.
    pub fn encode(&self) -> Result<Vec<u8>, TopicObjectError> {
        let mut bytes = Vec::with_capacity(
            FIXED_LEN + self.origin.len() + self.stream_name.len() + self.topic_name.len(),
        );
        let (state, deleted_at) = match self.state {
            TopicState::Live => (LIVE, 0),
            TopicState::Tombstone { deleted_at } => (TOMBSTONE, deleted_at),
        };
        bytes.extend_from_slice(&MAGIC.to_le_bytes());
        bytes.push(state);
        bytes.extend_from_slice(&self.cluster_uid.to_le_bytes());
        put_name(&mut bytes, "origin", &self.origin)?;
        bytes.extend_from_slice(&self.key.stream_id.to_le_bytes());
        put_name(&mut bytes, "stream name", &self.stream_name)?;
        bytes.extend_from_slice(&self.key.topic_id.to_le_bytes());
        put_name(&mut bytes, "topic name", &self.topic_name)?;
        bytes.extend_from_slice(&self.key.created_revision.to_le_bytes());
        bytes.extend_from_slice(&self.partition_count.to_le_bytes());
        bytes.push(self.compression.code());
        bytes.extend_from_slice(&self.max_message_size.to_le_bytes());
        bytes.extend_from_slice(&self.message_expiry.to_le_bytes());
        bytes.extend_from_slice(&self.max_topic_size.to_le_bytes());
        bytes.push(self.geo_mode.code());
        for timestamp in [self.created_at, self.published_at, deleted_at] {
            bytes.extend_from_slice(&timestamp.to_le_bytes());
        }
        bytes.extend_from_slice(&self.tombstone_grace.to_le_bytes());
        let checksum = crc32c::crc32c(&bytes);
        bytes.extend_from_slice(&checksum.to_le_bytes());
        Ok(bytes)
    }

    /// # Errors
    /// See [`TopicObjectError`]. An object that fails here must not be used
    /// in part.
    pub fn decode(bytes: &[u8]) -> Result<Self, TopicObjectError> {
        let mut reader = Reader::new(bytes);
        let magic = reader.u64()?;
        if magic != MAGIC {
            return Err(TopicObjectError::UnsupportedVersion(magic));
        }
        let state = reader.u8()?;
        let cluster_uid = reader.u128()?;
        let origin = reader.name()?;
        let stream_id = reader.u64()?;
        let stream_name = reader.name()?;
        let topic_id = reader.u64()?;
        let topic_name = reader.name()?;
        let created_revision = reader.u64()?;
        let partition_count = reader.u32()?;
        let compression = reader.u8()?;
        let max_message_size = reader.u64()?;
        let message_expiry = reader.u64()?;
        let max_topic_size = reader.u64()?;
        let geo_mode = reader.u8()?;
        let created_at = reader.u64()?;
        let published_at = reader.u64()?;
        let deleted_at = reader.u64()?;
        let tombstone_grace = reader.u64()?;
        let body_len = bytes.len() - reader.left();
        if reader.left() != CHECKSUM_LEN {
            return Err(TopicObjectError::WrongLength {
                len: bytes.len(),
                expected: body_len + CHECKSUM_LEN,
            });
        }
        let stored = reader.u32()?;
        let computed = crc32c::crc32c(&bytes[..body_len]);
        if stored != computed {
            return Err(TopicObjectError::ChecksumMismatch { stored, computed });
        }
        let state = match (state, deleted_at) {
            (LIVE, 0) => TopicState::Live,
            (TOMBSTONE, 1..) => TopicState::Tombstone { deleted_at },
            _ => return Err(TopicObjectError::InvalidField("state")),
        };
        Ok(Self {
            key: TopicKey {
                stream_id,
                topic_id,
                created_revision,
            },
            origin: utf8("origin", origin)?,
            cluster_uid,
            stream_name: utf8("stream name", stream_name)?,
            topic_name: utf8("topic name", topic_name)?,
            partition_count,
            compression: TopicCompression::from_code(compression)
                .ok_or(TopicObjectError::InvalidField("compression"))?,
            max_message_size,
            message_expiry,
            max_topic_size,
            geo_mode: TopicGeoMode::from_code(geo_mode)
                .ok_or(TopicObjectError::InvalidField("geo_mode"))?,
            created_at,
            published_at,
            state,
            tombstone_grace,
        })
    }

    /// Read the object of the topic incarnation `key` under the root of
    /// `layout`.
    ///
    /// # Errors
    /// Any error of the read but [`ObjectStoreError::NotFound`], which is
    /// [`TopicRead::Missing`].
    pub async fn read<S: ObjectStore>(
        store: &S,
        layout: &KeyLayout,
        key: &TopicKey,
    ) -> Result<TopicRead, ObjectStoreError> {
        let max_len = MAX_TOPIC_OBJECT_LEN as u64;
        match store.get_range(&layout.topic_key(key), 0..max_len).await {
            Ok(range) if range.size > max_len => {
                Ok(TopicRead::Invalid(TopicObjectError::WrongLength {
                    len: usize::try_from(range.size).unwrap_or(usize::MAX),
                    expected: MAX_TOPIC_OBJECT_LEN,
                }))
            }
            Ok(range) => Ok(match Self::decode(&range.bytes) {
                Ok(object) if object.key == *key => TopicRead::Found(object),
                Ok(object) => TopicRead::Invalid(TopicObjectError::KeyMismatch {
                    expected: *key,
                    found: object.key,
                }),
                Err(error) => TopicRead::Invalid(error),
            }),
            Err(ObjectStoreError::NotFound { .. }) => Ok(TopicRead::Missing),
            // An empty object has no readable range.
            Err(ObjectStoreError::InvalidRange { .. }) => {
                Ok(TopicRead::Invalid(TopicObjectError::Truncated(0)))
            }
            Err(error) => Err(error),
        }
    }

    /// Store this object under its key in the root of `layout`, over any
    /// object there.
    ///
    /// # Errors
    /// [`ObjectStoreError::InvalidRequest`] for an object that
    /// [`Self::encode`] refuses, and any error of the put.
    pub async fn put<S: ObjectStore>(
        &self,
        store: &S,
        layout: &KeyLayout,
    ) -> Result<(), ObjectStoreError> {
        let body = self
            .encode()
            .map_err(|error| ObjectStoreError::InvalidRequest(error.to_string()))?;
        let options = PutOptions {
            crc32c: Some(crc32c::crc32c(&body)),
            ..PutOptions::default()
        };
        store
            .put(
                &layout.topic_key(&self.key),
                PutBody::from_bytes(body),
                &options,
            )
            .await
    }
}

fn put_name(bytes: &mut Vec<u8>, field: &'static str, name: &str) -> Result<(), TopicObjectError> {
    let len = u8::try_from(name.len())
        .ok()
        .filter(|&len| len > 0)
        .ok_or(TopicObjectError::InvalidName(field))?;
    bytes.push(len);
    bytes.extend_from_slice(name.as_bytes());
    Ok(())
}

fn utf8(field: &'static str, bytes: &[u8]) -> Result<String, TopicObjectError> {
    std::str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|_| TopicObjectError::InvalidName(field))
}

/// Reads fields off the front of the bytes. A field past the end is
/// [`TopicObjectError::Truncated`].
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

    fn take(&mut self, count: usize) -> Result<&'a [u8], TopicObjectError> {
        let (field, rest) = self
            .left
            .split_at_checked(count)
            .ok_or(TopicObjectError::Truncated(self.len))?;
        self.left = rest;
        Ok(field)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], TopicObjectError> {
        self.take(N)?
            .try_into()
            .map_err(|_| TopicObjectError::Truncated(self.len))
    }

    fn u8(&mut self) -> Result<u8, TopicObjectError> {
        Ok(self.array::<1>()?[0])
    }

    fn u32(&mut self) -> Result<u32, TopicObjectError> {
        Ok(u32::from_le_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, TopicObjectError> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn u128(&mut self) -> Result<u128, TopicObjectError> {
        Ok(u128::from_le_bytes(self.array()?))
    }

    /// The bytes of a name, which [`utf8`] checks once the checksum holds.
    fn name(&mut self) -> Result<&'a [u8], TopicObjectError> {
        let len = usize::from(self.u8()?);
        self.take(len)
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use futures::executor::block_on;

    use super::*;
    use crate::memory::MemoryStore;

    const KEY: TopicKey = TopicKey {
        stream_id: 1,
        topic_id: 22,
        created_revision: 4096,
    };

    fn layout() -> KeyLayout {
        KeyLayout::new("tenant/iggy").unwrap()
    }

    fn object() -> TopicObject {
        TopicObject {
            key: KEY,
            origin: "dc-a".to_owned(),
            cluster_uid: 0x0123_4567_89ab_cdef_0011_2233_4455_6677,
            stream_name: "orders".to_owned(),
            topic_name: "payments".to_owned(),
            partition_count: 12,
            compression: TopicCompression::Gzip,
            max_message_size: 10 * 1024 * 1024,
            message_expiry: 3_600_000_000,
            max_topic_size: u64::MAX,
            geo_mode: TopicGeoMode::Geo,
            created_at: 1_700_000_000_000_000,
            published_at: 1_700_000_000_500_000,
            state: TopicState::Live,
            tombstone_grace: 3_600_000_000,
        }
    }

    #[test]
    fn given_fixed_len_when_counting_the_fields_should_match_the_layout() {
        // magic, state, cluster_uid, three name lengths, three ids, the
        // partition count, compression, three limits, geo_mode, three
        // timestamps, the tombstone grace and the checksum.
        let fields = [8, 1, 16, 1, 1, 1, 8, 8, 8, 4, 1, 8, 8, 8, 1, 8, 8, 8, 8, 4];
        assert_eq!(fields.iter().sum::<usize>(), FIXED_LEN);
        assert_eq!(MAX_TOPIC_OBJECT_LEN, 883);
    }

    #[test]
    fn given_live_and_tombstone_when_encoding_and_decoding_should_give_them_back() {
        let live = object();
        let tombstone = object().into_tombstone(1_700_000_900_000_000);
        let server_grace = TopicObject {
            tombstone_grace: 0,
            ..object()
        };

        for object in [live, tombstone, server_grace] {
            let encoded = object.encode().unwrap();
            assert_eq!(
                encoded.len(),
                FIXED_LEN
                    + object.origin.len()
                    + object.stream_name.len()
                    + object.topic_name.len()
            );
            assert_eq!(TopicObject::decode(&encoded), Ok(object));
        }
        assert!(object().is_live());
        assert!(!object().into_tombstone(1).is_live());
    }

    #[test]
    fn given_names_of_the_longest_length_when_encoding_should_take_the_most_bytes() {
        let longest = "n".repeat(MAX_TOPIC_NAME_LEN);
        let object = TopicObject {
            origin: longest.clone(),
            stream_name: longest,
            topic_name: "é".repeat(MAX_TOPIC_NAME_LEN / 2) + "n",
            ..object()
        };

        let encoded = object.encode().unwrap();

        assert_eq!(encoded.len(), MAX_TOPIC_OBJECT_LEN);
        assert_eq!(TopicObject::decode(&encoded), Ok(object));
    }

    #[test]
    fn given_an_empty_or_too_long_name_when_encoding_should_refuse_it() {
        let too_long = "n".repeat(MAX_TOPIC_NAME_LEN + 1);
        for (object, field) in [
            (
                TopicObject {
                    origin: String::new(),
                    ..object()
                },
                "origin",
            ),
            (
                TopicObject {
                    stream_name: too_long.clone(),
                    ..object()
                },
                "stream name",
            ),
            (
                TopicObject {
                    topic_name: too_long,
                    ..object()
                },
                "topic name",
            ),
        ] {
            assert_eq!(object.encode(), Err(TopicObjectError::InvalidName(field)));
        }
    }

    #[test]
    fn given_any_flipped_bit_when_decoding_should_refuse_the_object() {
        let encoded = object().into_tombstone(7).encode().unwrap();

        for byte in 0..encoded.len() {
            for bit in 0..8 {
                let mut damaged = encoded.clone();
                damaged[byte] ^= 1 << bit;
                assert!(
                    TopicObject::decode(&damaged).is_err(),
                    "a flipped bit {bit} in byte {byte} went unnoticed"
                );
            }
        }
    }

    #[test]
    fn given_a_cut_or_extended_object_when_decoding_should_refuse_every_length() {
        let encoded = object().encode().unwrap();

        for len in 0..encoded.len() {
            assert!(
                TopicObject::decode(&encoded[..len]).is_err(),
                "an object cut to {len} bytes decoded"
            );
        }
        let mut extended = encoded.clone();
        extended.push(0);
        assert_eq!(
            TopicObject::decode(&extended),
            Err(TopicObjectError::WrongLength {
                len: encoded.len() + 1,
                expected: encoded.len(),
            })
        );
    }

    #[test]
    fn given_another_magic_when_decoding_should_report_an_unsupported_version() {
        let mut encoded = object().encode().unwrap();
        encoded[..8].copy_from_slice(b"IGGYTOP1");

        assert_eq!(
            TopicObject::decode(&encoded),
            Err(TopicObjectError::UnsupportedVersion(u64::from_le_bytes(
                *b"IGGYTOP1"
            )))
        );
    }

    /// The encoding of `object` with the byte at `offset` set to `value` and
    /// the checksum fixed, so only the field check can refuse it.
    fn with_byte(object: &TopicObject, offset: usize, value: u8) -> Vec<u8> {
        let mut encoded = object.encode().unwrap();
        encoded[offset] = value;
        let body_len = encoded.len() - CHECKSUM_LEN;
        let checksum = crc32c::crc32c(&encoded[..body_len]);
        encoded[body_len..].copy_from_slice(&checksum.to_le_bytes());
        encoded
    }

    #[test]
    fn given_unknown_codes_with_a_valid_checksum_when_decoding_should_name_the_field() {
        let object = object();
        let names = object.origin.len() + object.stream_name.len() + object.topic_name.len();
        // Offsets past the three names: the state, compression and geo_mode.
        let compression = 8 + 1 + 16 + 3 + 3 * 8 + 4 + names;
        let geo_mode = compression + 1 + 3 * 8;
        for (offset, field) in [
            (8, "state"),
            (compression, "compression"),
            (geo_mode, "geo_mode"),
        ] {
            assert_eq!(
                TopicObject::decode(&with_byte(&object, offset, 9)),
                Err(TopicObjectError::InvalidField(field)),
                "{field}"
            );
        }
        // A live object with a deletion time, and a tombstone without one.
        assert_eq!(
            TopicObject::decode(&with_byte(&object, 8, TOMBSTONE)),
            Err(TopicObjectError::InvalidField("state"))
        );
        assert_eq!(
            TopicObject::decode(&with_byte(&object.into_tombstone(1), 8, LIVE)),
            Err(TopicObjectError::InvalidField("state"))
        );
    }

    #[test]
    fn given_a_name_that_is_not_utf8_when_decoding_should_refuse_it() {
        let object = object();
        // The first byte of the origin.
        let encoded = with_byte(&object, 8 + 1 + 16 + 1, 0xff);

        assert_eq!(
            TopicObject::decode(&encoded),
            Err(TopicObjectError::InvalidName("origin"))
        );
    }

    #[test]
    fn given_codes_when_round_tripped_should_keep_the_binary_protocol_values() {
        for compression in [TopicCompression::None, TopicCompression::Gzip] {
            assert_eq!(
                TopicCompression::from_code(compression.code()),
                Some(compression)
            );
        }
        assert_eq!(TopicCompression::None.code(), 1);
        assert_eq!(TopicCompression::Gzip.code(), 2);
        for mode in [TopicGeoMode::Geo, TopicGeoMode::GeoTiered] {
            assert_eq!(TopicGeoMode::from_code(mode.code()), Some(mode));
        }
        for code in [0, 3, u8::MAX] {
            assert_eq!(TopicCompression::from_code(code), None);
            assert_eq!(TopicGeoMode::from_code(code), None);
        }
    }

    fn put_raw(store: &MemoryStore, key: &TopicKey, bytes: Vec<u8>) {
        block_on(store.put(
            &layout().topic_key(key),
            PutBody::from_bytes(Bytes::from(bytes)),
            &PutOptions::default(),
        ))
        .unwrap();
    }

    #[test]
    fn given_a_stored_object_when_reading_its_key_should_find_it() {
        let store = MemoryStore::new();
        let layout = layout();
        let tombstone = object().into_tombstone(9);

        assert_eq!(
            block_on(TopicObject::read(&store, &layout, &KEY)).unwrap(),
            TopicRead::Missing
        );
        block_on(object().put(&store, &layout)).unwrap();
        assert_eq!(store.keys(), [layout.topic_key(&KEY)]);
        assert_eq!(
            block_on(TopicObject::read(&store, &layout, &KEY)).unwrap(),
            TopicRead::Found(object())
        );
        block_on(tombstone.put(&store, &layout)).unwrap();
        assert_eq!(
            block_on(TopicObject::read(&store, &layout, &KEY)).unwrap(),
            TopicRead::Found(tombstone)
        );
    }

    #[test]
    fn given_bytes_that_are_not_the_object_of_the_key_when_reading_should_report_them_invalid() {
        let store = MemoryStore::new();
        let layout = layout();
        let other = TopicKey {
            created_revision: 1,
            ..KEY
        };

        put_raw(&store, &KEY, b"not a topic object".to_vec());
        assert!(matches!(
            block_on(TopicObject::read(&store, &layout, &KEY)).unwrap(),
            TopicRead::Invalid(_)
        ));
        put_raw(&store, &KEY, Vec::new());
        assert_eq!(
            block_on(TopicObject::read(&store, &layout, &KEY)).unwrap(),
            TopicRead::Invalid(TopicObjectError::Truncated(0))
        );
        put_raw(&store, &KEY, vec![0; MAX_TOPIC_OBJECT_LEN + 1]);
        assert_eq!(
            block_on(TopicObject::read(&store, &layout, &KEY)).unwrap(),
            TopicRead::Invalid(TopicObjectError::WrongLength {
                len: MAX_TOPIC_OBJECT_LEN + 1,
                expected: MAX_TOPIC_OBJECT_LEN,
            })
        );
        put_raw(&store, &other, object().encode().unwrap());
        assert_eq!(
            block_on(TopicObject::read(&store, &layout, &other)).unwrap(),
            TopicRead::Invalid(TopicObjectError::KeyMismatch {
                expected: other,
                found: KEY,
            })
        );
    }

    #[test]
    fn given_an_object_that_does_not_encode_when_putting_should_store_nothing() {
        let store = MemoryStore::new();
        let object = TopicObject {
            topic_name: String::new(),
            ..object()
        };

        let result = block_on(object.put(&store, &layout()));

        assert!(matches!(result, Err(ObjectStoreError::InvalidRequest(_))));
        assert!(store.is_empty());
    }
}
