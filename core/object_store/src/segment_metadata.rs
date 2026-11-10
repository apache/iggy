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

use crate::store::UserMetadata;

/// Bytes in a segment's BLAKE3 hash.
pub const SEGMENT_HASH_LEN: usize = 32;

const VERSION: &str = "1";

const VERSION_NAME: &str = "metadata-version";
const START_OFFSET: &str = "start-offset";
const END_OFFSET: &str = "end-offset";
const START_TIMESTAMP: &str = "start-timestamp";
const END_TIMESTAMP: &str = "end-timestamp";
const MAX_TIMESTAMP: &str = "max-timestamp";
const SIZE: &str = "size";
const BLAKE3: &str = "blake3";

/// What the user metadata of a segment's `.log` object records, so a listing
/// plus HEAD rebuilds the segment's metadata without a download.
///
/// Every field is one entry: integers in decimal, the hash in lowercase hex,
/// next to a `metadata-version` entry. A reader ignores names it does not
/// know, so a writer can add entries without a new version.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentMetadata {
    pub start_offset: u64,
    /// The offset of the segment's last message.
    pub end_offset: u64,
    /// Microseconds since the Unix epoch, as the segment records it, like the
    /// other two timestamps.
    pub start_timestamp: u64,
    pub end_timestamp: u64,
    pub max_timestamp: u64,
    /// The segment's committed size, which is the object's size.
    pub size: u64,
    /// BLAKE3 of the object's bytes: the segment's commit-path hash, which
    /// the marker in the chain file records too.
    pub hash: [u8; SEGMENT_HASH_LEN],
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SegmentMetadataError {
    #[error("segment metadata has no {0:?} entry")]
    Missing(&'static str),
    #[error("segment metadata entry {name:?} holds {value:?}, not {expected}")]
    Invalid {
        name: &'static str,
        value: String,
        /// The form that the entry takes.
        expected: &'static str,
    },
    #[error("segment metadata version {0:?} is not supported")]
    UnsupportedVersion(String),
    #[error("segment metadata ends at offset {end_offset}, before it starts at {start_offset}")]
    ReversedOffsets { start_offset: u64, end_offset: u64 },
}

impl From<&SegmentMetadata> for UserMetadata {
    fn from(metadata: &SegmentMetadata) -> Self {
        [
            (VERSION_NAME, VERSION.to_owned()),
            (START_OFFSET, metadata.start_offset.to_string()),
            (END_OFFSET, metadata.end_offset.to_string()),
            (START_TIMESTAMP, metadata.start_timestamp.to_string()),
            (END_TIMESTAMP, metadata.end_timestamp.to_string()),
            (MAX_TIMESTAMP, metadata.max_timestamp.to_string()),
            (SIZE, metadata.size.to_string()),
            (BLAKE3, hex::encode(metadata.hash)),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect()
    }
}

impl TryFrom<&UserMetadata> for SegmentMetadata {
    type Error = SegmentMetadataError;

    fn try_from(metadata: &UserMetadata) -> Result<Self, Self::Error> {
        let version = entry(metadata, VERSION_NAME)?;
        if version != VERSION {
            return Err(SegmentMetadataError::UnsupportedVersion(version.to_owned()));
        }
        let decoded = Self {
            start_offset: integer(metadata, START_OFFSET)?,
            end_offset: integer(metadata, END_OFFSET)?,
            start_timestamp: integer(metadata, START_TIMESTAMP)?,
            end_timestamp: integer(metadata, END_TIMESTAMP)?,
            max_timestamp: integer(metadata, MAX_TIMESTAMP)?,
            size: integer(metadata, SIZE)?,
            hash: hash(metadata, BLAKE3)?,
        };
        if decoded.end_offset < decoded.start_offset {
            return Err(SegmentMetadataError::ReversedOffsets {
                start_offset: decoded.start_offset,
                end_offset: decoded.end_offset,
            });
        }
        Ok(decoded)
    }
}

fn entry<'a>(
    metadata: &'a UserMetadata,
    name: &'static str,
) -> Result<&'a str, SegmentMetadataError> {
    metadata
        .get(name)
        .map(String::as_str)
        .ok_or(SegmentMetadataError::Missing(name))
}

fn integer(metadata: &UserMetadata, name: &'static str) -> Result<u64, SegmentMetadataError> {
    let value = entry(metadata, name)?;
    value
        .bytes()
        .all(|byte| byte.is_ascii_digit())
        .then(|| value.parse().ok())
        .flatten()
        .ok_or_else(|| invalid(name, value, "a decimal u64"))
}

fn hash(
    metadata: &UserMetadata,
    name: &'static str,
) -> Result<[u8; SEGMENT_HASH_LEN], SegmentMetadataError> {
    // The expected form below spells out this length.
    const _: () = assert!(SEGMENT_HASH_LEN == 32);
    let value = entry(metadata, name)?;
    let mut hash = [0; SEGMENT_HASH_LEN];
    hex::decode_to_slice(value, &mut hash).map_err(|_| invalid(name, value, "64 hex digits"))?;
    Ok(hash)
}

fn invalid(name: &'static str, value: &str, expected: &'static str) -> SegmentMetadataError {
    SegmentMetadataError::Invalid {
        name,
        value: value.to_owned(),
        expected,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::validate_metadata;

    const SEGMENT: SegmentMetadata = SegmentMetadata {
        start_offset: 1_000,
        end_offset: 1_999,
        start_timestamp: 1_762_000_000_000_000,
        end_timestamp: 1_762_000_360_000_000,
        max_timestamp: 1_762_000_360_000_001,
        size: 1_073_741_824,
        hash: [0xab; SEGMENT_HASH_LEN],
    };

    #[test]
    fn given_segment_when_encoding_should_round_trip() {
        let encoded = UserMetadata::from(&SEGMENT);

        assert_eq!(SegmentMetadata::try_from(&encoded), Ok(SEGMENT));
    }

    #[test]
    fn given_segment_when_encoding_should_write_named_portable_entries() {
        let encoded = UserMetadata::from(&SEGMENT);

        assert!(validate_metadata(&encoded).is_ok());
        assert_eq!(encoded["metadata-version"], "1");
        assert_eq!(encoded["start-offset"], "1000");
        assert_eq!(encoded["end-offset"], "1999");
        assert_eq!(encoded["size"], "1073741824");
        assert_eq!(encoded["blake3"], "ab".repeat(SEGMENT_HASH_LEN));
    }

    #[test]
    fn given_extreme_values_when_encoding_should_fit_and_round_trip() {
        let extreme = SegmentMetadata {
            start_offset: u64::MAX,
            end_offset: u64::MAX,
            start_timestamp: u64::MAX,
            end_timestamp: u64::MAX,
            max_timestamp: u64::MAX,
            size: u64::MAX,
            hash: [0xff; SEGMENT_HASH_LEN],
        };
        let encoded = UserMetadata::from(&extreme);

        assert!(validate_metadata(&encoded).is_ok());
        assert_eq!(SegmentMetadata::try_from(&encoded), Ok(extreme));
    }

    #[test]
    fn given_unknown_entries_when_decoding_should_ignore_them() {
        let mut encoded = UserMetadata::from(&SEGMENT);
        encoded.insert("written-by".to_owned(), "a later release".to_owned());

        assert_eq!(SegmentMetadata::try_from(&encoded), Ok(SEGMENT));
    }

    #[test]
    fn given_missing_entry_when_decoding_should_name_it() {
        for name in [
            "metadata-version",
            "start-offset",
            "end-offset",
            "start-timestamp",
            "end-timestamp",
            "max-timestamp",
            "size",
            "blake3",
        ] {
            let mut encoded = UserMetadata::from(&SEGMENT);
            encoded.remove(name);

            assert_eq!(
                SegmentMetadata::try_from(&encoded),
                Err(SegmentMetadataError::Missing(name))
            );
        }
    }

    #[test]
    fn given_malformed_values_when_decoding_should_refuse() {
        for (name, value) in [
            ("start-offset", ""),
            ("start-offset", "+1000"),
            ("start-offset", "-1"),
            ("start-offset", "1e3"),
            ("size", "18446744073709551616"),
            ("blake3", "ab"),
            ("blake3", &"zz".repeat(SEGMENT_HASH_LEN)),
            ("blake3", &"ab".repeat(SEGMENT_HASH_LEN + 1)),
        ] {
            let mut encoded = UserMetadata::from(&SEGMENT);
            encoded.insert(name.to_owned(), value.to_owned());

            assert!(
                matches!(
                    SegmentMetadata::try_from(&encoded),
                    Err(SegmentMetadataError::Invalid { name: invalid, .. }) if invalid == name
                ),
                "{name} = {value:?}"
            );
        }
    }

    #[test]
    fn given_malformed_values_when_decoding_should_name_expected_form() {
        let decode = |name: &str, value: &str| {
            let mut encoded = UserMetadata::from(&SEGMENT);
            encoded.insert(name.to_owned(), value.to_owned());
            SegmentMetadata::try_from(&encoded).unwrap_err().to_string()
        };

        assert_eq!(
            decode("size", "-1"),
            r#"segment metadata entry "size" holds "-1", not a decimal u64"#
        );
        assert_eq!(
            decode("blake3", "ab"),
            r#"segment metadata entry "blake3" holds "ab", not 64 hex digits"#
        );
    }

    #[test]
    fn given_other_version_when_decoding_should_refuse() {
        let mut encoded = UserMetadata::from(&SEGMENT);
        encoded.insert("metadata-version".to_owned(), "2".to_owned());

        assert_eq!(
            SegmentMetadata::try_from(&encoded),
            Err(SegmentMetadataError::UnsupportedVersion("2".to_owned()))
        );
    }

    #[test]
    fn given_end_before_start_when_decoding_should_refuse() {
        let reversed = SegmentMetadata {
            end_offset: SEGMENT.start_offset - 1,
            ..SEGMENT
        };

        assert_eq!(
            SegmentMetadata::try_from(&UserMetadata::from(&reversed)),
            Err(SegmentMetadataError::ReversedOffsets {
                start_offset: 1_000,
                end_offset: 999,
            })
        );
    }
}
