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

//! The header at the start of every object file.
//!
//! | Bytes  | Field                                                       |
//! | ------ | ----------------------------------------------------------- |
//! | 0..8   | `IGGY-OBJ`                                                  |
//! | 8..12  | format version: 1                                           |
//! | 12..16 | header length in bytes, a multiple of 4096: the body offset |
//! | 16..24 | body length                                                 |
//! | 24..28 | CRC32C of the body                                          |
//! | 28..32 | metadata length                                             |
//! | 32..48 | etag                                                        |
//! | 48..52 | CRC32C of bytes 0..48, then of the metadata                 |
//! | 52..   | metadata: `name=value\n` per entry, by name, then zeros     |
//!
//! Numbers are little-endian.

use std::fmt;

use crate::store::UserMetadata;

/// The longest header. Valid metadata needs at most 4 KiB with its
/// separators, so every header fits in two blocks.
pub(super) const MAX_HEADER_LEN: usize = 2 * BLOCK_LEN;

const MAGIC: [u8; 8] = *b"IGGY-OBJ";
const VERSION: u32 = 1;
/// A header fills whole blocks, so the body starts on a page boundary.
const BLOCK_LEN: usize = 4096;
/// The fields before the metadata.
const FIXED_LEN: usize = 52;
/// The fields that the header checksum covers, before the metadata.
const CHECKED_LEN: usize = 48;

/// A header read from an object file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Header {
    /// The header length: where the body starts.
    pub(super) len: u64,
    pub(super) body_len: u64,
    pub(super) crc32c: u32,
    pub(super) etag: Etag,
    pub(super) metadata: UserMetadata,
}

/// The header of a put whose body is still to come. The metadata alone sets
/// its length, so the body can go in before the header.
#[derive(Debug)]
pub(super) struct PendingHeader {
    metadata: Vec<u8>,
    metadata_len: u32,
    len: u32,
}

/// An object's etag: random for each put.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct Etag(u128);

#[derive(Debug, thiserror::Error)]
pub(super) enum HeaderError {
    #[error("the file ends inside its header")]
    Truncated,
    #[error("the file has no object header")]
    NotAnObject,
    #[error("object format version {0} is unknown")]
    Version(u32),
    #[error("the header length {0} is not valid")]
    Length(u32),
    #[error("the header checksum does not match")]
    Checksum,
    #[error("the metadata is not valid: {0}")]
    Metadata(String),
    #[error("the file holds {file_len} bytes, not the {expected} that its header says")]
    FileLength { file_len: u64, expected: u64 },
}

impl Header {
    /// Checks that a file of `file_len` bytes holds this header and its body,
    /// and nothing more.
    pub(super) const fn check_file_len(&self, file_len: u64) -> Result<(), HeaderError> {
        let expected = self.len.saturating_add(self.body_len);
        if file_len == expected {
            Ok(())
        } else {
            Err(HeaderError::FileLength { file_len, expected })
        }
    }
}

/// Decodes the header at the start of a file. `bytes` hold the file's first
/// [`MAX_HEADER_LEN`] bytes, or all of a shorter file.
impl TryFrom<&[u8]> for Header {
    type Error = HeaderError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        if !bytes.starts_with(&MAGIC) {
            return Err(HeaderError::NotAnObject);
        }
        let fixed: &[u8; FIXED_LEN] = bytes.first_chunk().ok_or(HeaderError::Truncated)?;
        let version = u32::from_le_bytes(field(fixed, 8));
        if version != VERSION {
            return Err(HeaderError::Version(version));
        }
        let len = u32::from_le_bytes(field(fixed, 12));
        let metadata_end = FIXED_LEN + u32::from_le_bytes(field(fixed, 28)) as usize;
        let header_len = len as usize;
        if !header_len.is_multiple_of(BLOCK_LEN)
            || header_len > MAX_HEADER_LEN
            || metadata_end > header_len
        {
            return Err(HeaderError::Length(len));
        }
        let metadata = bytes
            .get(FIXED_LEN..metadata_end)
            .ok_or(HeaderError::Truncated)?;
        let checksum = crc32c::crc32c_append(crc32c::crc32c(&fixed[..CHECKED_LEN]), metadata);
        if checksum != u32::from_le_bytes(field(fixed, CHECKED_LEN)) {
            return Err(HeaderError::Checksum);
        }
        Ok(Self {
            len: u64::from(len),
            body_len: u64::from_le_bytes(field(fixed, 16)),
            crc32c: u32::from_le_bytes(field(fixed, 24)),
            etag: Etag(u128::from_le_bytes(field(fixed, 32))),
            metadata: decode_metadata(metadata)?,
        })
    }
}

impl PendingHeader {
    /// The header of `metadata`, which `validate_metadata` accepted, so the
    /// header fits in [`MAX_HEADER_LEN`] and both lengths fit in a `u32`.
    #[allow(clippy::cast_possible_truncation)]
    pub(super) fn new(metadata: &UserMetadata) -> Self {
        let mut encoded = Vec::new();
        for (name, value) in metadata {
            encoded.extend_from_slice(name.as_bytes());
            encoded.push(b'=');
            encoded.extend_from_slice(value.as_bytes());
            encoded.push(b'\n');
        }
        let len = (FIXED_LEN + encoded.len()).next_multiple_of(BLOCK_LEN);
        Self {
            metadata_len: encoded.len() as u32,
            len: len as u32,
            metadata: encoded,
        }
    }

    /// The header length: where the body starts.
    pub(super) fn len(&self) -> u64 {
        u64::from(self.len)
    }

    /// The whole header of a body of `body_len` bytes.
    pub(super) fn finish(&self, body_len: u64, body_crc32c: u32, etag: Etag) -> Vec<u8> {
        let len = self.len as usize;
        let mut bytes = Vec::with_capacity(len);
        bytes.extend_from_slice(&MAGIC);
        bytes.extend_from_slice(&VERSION.to_le_bytes());
        bytes.extend_from_slice(&self.len.to_le_bytes());
        bytes.extend_from_slice(&body_len.to_le_bytes());
        bytes.extend_from_slice(&body_crc32c.to_le_bytes());
        bytes.extend_from_slice(&self.metadata_len.to_le_bytes());
        bytes.extend_from_slice(&etag.0.to_le_bytes());
        let checksum = crc32c::crc32c_append(crc32c::crc32c(&bytes), &self.metadata);
        bytes.extend_from_slice(&checksum.to_le_bytes());
        bytes.extend_from_slice(&self.metadata);
        bytes.resize(len, 0);
        bytes
    }
}

impl Etag {
    pub(super) fn random() -> Self {
        Self(rand::random())
    }
}

impl fmt::Display for Etag {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{:032x}", self.0)
    }
}

/// The `N` bytes of `fixed` at `at`.
fn field<const N: usize>(fixed: &[u8; FIXED_LEN], at: usize) -> [u8; N] {
    let mut field = [0; N];
    field.copy_from_slice(&fixed[at..at + N]);
    field
}

/// Decodes the entries of format version 1. The header checksum guards them,
/// so the rules for new metadata never apply to a stored object.
fn decode_metadata(bytes: &[u8]) -> Result<UserMetadata, HeaderError> {
    let text = std::str::from_utf8(bytes)
        .map_err(|_| HeaderError::Metadata("it is not UTF-8".to_owned()))?;
    let mut metadata = UserMetadata::new();
    for entry in text.split_terminator('\n') {
        let (name, value) = entry
            .split_once('=')
            .ok_or_else(|| HeaderError::Metadata(format!("entry {entry:?} has no '='")))?;
        if metadata.insert(name.to_owned(), value.to_owned()).is_some() {
            return Err(HeaderError::Metadata(format!("name {name:?} repeats")));
        }
    }
    Ok(metadata)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::validate_metadata;

    const ETAG: Etag = Etag(0x0011_2233_4455_6677_8899_aabb_ccdd_eeff);

    fn metadata(entries: &[(&str, &str)]) -> UserMetadata {
        entries
            .iter()
            .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
            .collect()
    }

    fn encode(entries: &[(&str, &str)], body_len: u64) -> Vec<u8> {
        PendingHeader::new(&metadata(entries)).finish(body_len, 0x0102_0304, ETAG)
    }

    /// The header with a body of `body_len` zeros after it, as a file holds it.
    fn file(entries: &[(&str, &str)], body_len: u64) -> Vec<u8> {
        let mut file = encode(entries, body_len);
        file.resize(file.len() + usize::try_from(body_len).unwrap(), 0);
        file
    }

    #[test]
    fn given_header_when_encoding_should_lay_out_fields_as_documented() {
        let bytes = encode(&[("a", "1")], 5);

        let mut expected = Vec::new();
        expected.extend_from_slice(b"IGGY-OBJ");
        expected.extend_from_slice(&1u32.to_le_bytes());
        expected.extend_from_slice(&4096u32.to_le_bytes());
        expected.extend_from_slice(&5u64.to_le_bytes());
        expected.extend_from_slice(&0x0102_0304u32.to_le_bytes());
        expected.extend_from_slice(&4u32.to_le_bytes());
        expected.extend_from_slice(&ETAG.0.to_le_bytes());
        let checksum = crc32c::crc32c_append(crc32c::crc32c(&expected), b"a=1\n");
        expected.extend_from_slice(&checksum.to_le_bytes());
        expected.extend_from_slice(b"a=1\n");
        assert_eq!(bytes.len(), BLOCK_LEN);
        assert_eq!(bytes[..expected.len()], expected);
        assert!(bytes[expected.len()..].iter().all(|byte| *byte == 0));
        assert_eq!(ETAG.to_string(), "00112233445566778899aabbccddeeff");
    }

    #[test]
    fn given_encoded_header_when_decoding_should_return_every_field() {
        let entries = [("end-offset", "99"), ("empty", ""), ("start-offset", "0")];
        let file = file(&entries, 7);

        let header = Header::try_from(file.as_slice()).unwrap();

        assert_eq!(
            header,
            Header {
                len: 4096,
                body_len: 7,
                crc32c: 0x0102_0304,
                etag: ETAG,
                metadata: metadata(&entries),
            }
        );
        assert!(header.check_file_len(4096 + 7).is_ok());
        assert!(matches!(
            header.check_file_len(4096 + 6),
            Err(HeaderError::FileLength {
                file_len: 4102,
                expected: 4103
            })
        ));
    }

    #[test]
    fn given_largest_valid_metadata_when_encoding_should_fit_in_max_header_len() {
        // The most entries: every name of one, two and some of three bytes,
        // with empty values, so separators take the most room.
        let letters = b'a'..=b'z';
        let tails = (b'a'..=b'z').chain(b'0'..=b'9').chain(*b"-");
        let mut names: Vec<String> = letters
            .clone()
            .map(|letter| char::from(letter).to_string())
            .collect();
        for first in letters {
            for second in tails.clone() {
                names.push(String::from_utf8(vec![first, second]).unwrap());
            }
        }
        let mut longest = UserMetadata::new();
        let mut total = 0;
        for name in names
            .into_iter()
            .chain(tails.map(|tail| String::from_utf8(vec![b'z', b'z', tail]).unwrap()))
        {
            if total + name.len() > crate::MAX_METADATA_LEN {
                break;
            }
            total += name.len();
            longest.insert(name, String::new());
        }
        assert!(validate_metadata(&longest).is_ok());

        let pending = PendingHeader::new(&longest);
        let file = pending.finish(0, 0, ETAG);

        assert_eq!(pending.len(), MAX_HEADER_LEN as u64);
        assert_eq!(Header::try_from(file.as_slice()).unwrap().metadata, longest);
    }

    #[test]
    fn given_damaged_header_when_decoding_should_refuse() {
        let file = file(&[("name", "value")], 3);
        let damaged = |at: usize, value: u8| {
            let mut copy = file.clone();
            copy[at] = value;
            Header::try_from(copy.as_slice())
        };

        assert!(matches!(damaged(0, b'X'), Err(HeaderError::NotAnObject)));
        assert!(matches!(
            Header::try_from(&file[..MAGIC.len() - 1]),
            Err(HeaderError::NotAnObject)
        ));
        assert!(matches!(damaged(8, 2), Err(HeaderError::Version(2))));
        assert!(matches!(damaged(13, 0x11), Err(HeaderError::Length(_))));
        assert!(matches!(damaged(13, 0x30), Err(HeaderError::Length(_))));
        assert!(matches!(damaged(16, 4), Err(HeaderError::Checksum)));
        assert!(matches!(damaged(55, b'X'), Err(HeaderError::Checksum)));
        assert!(matches!(
            Header::try_from(&file[..FIXED_LEN - 1]),
            Err(HeaderError::Truncated)
        ));
        assert!(matches!(
            Header::try_from(&file[..FIXED_LEN + 3]),
            Err(HeaderError::Truncated)
        ));
    }

    #[test]
    fn given_metadata_out_of_format_when_decoding_should_refuse_only_that() {
        let forged = |metadata: &[u8]| {
            let pending = PendingHeader {
                metadata: metadata.to_vec(),
                metadata_len: u32::try_from(metadata.len()).unwrap(),
                len: 4096,
            };
            Header::try_from(pending.finish(0, 0, ETAG).as_slice())
        };

        for metadata in [b"no-separator\n".as_slice(), b"a=1\na=2\n", b"a=\xff\n"] {
            assert!(
                matches!(forged(metadata), Err(HeaderError::Metadata(_))),
                "{:?}",
                String::from_utf8_lossy(metadata)
            );
        }
        let refused_today = forged(b"Upper=1\n").unwrap().metadata;
        assert_eq!(refused_today, metadata(&[("Upper", "1")]));
    }
}
