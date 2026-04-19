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

//! Wire-level primitives for user header TLV validation, iteration, and encoding.
//!
//! User headers are encoded as a sequence of key-value TLV pairs:
//! ```text
//! [key_kind:u8][key_len:u32_le][key_data:N][val_kind:u8][val_len:u32_le][val_data:M] ...
//! ```
//!
//! This module provides structural validation and zero-copy iteration
//! over these bytes without interpreting semantic meaning of kind codes.
//! Domain-level interpretation (mapping kind codes to typed values)
//! happens in `iggy_common`.

use std::borrow::Cow;

use bytes::{BufMut, Bytes, BytesMut};

use crate::WireError;
use crate::codec::{WireEncode, read_u8, read_u32_le};

/// Maximum byte length for a single header key or value on the wire.
const MAX_FIELD_LENGTH: u32 = 255;

/// Opaque header kind tag as transmitted on the wire.
///
/// This is a `u8` newtype - NOT an exhaustive enum. New kind codes can be
/// added without breaking structural validation, which is critical for
/// VSR rolling upgrades where older nodes must forward messages containing
/// kind codes they do not yet understand.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WireHeaderKind(pub u8);

impl WireHeaderKind {
    /// Highest currently defined kind code (Float64 = 15).
    pub const KNOWN_MAX: u8 = 15;

    /// Returns `true` if the kind code is within the currently defined range.
    #[must_use]
    pub fn is_known(self) -> bool {
        (1..=Self::KNOWN_MAX).contains(&self.0)
    }
}

/// A single key-value header entry borrowing the underlying buffer.
#[derive(Debug)]
pub struct WireUserHeaderEntry<'a> {
    pub key_kind: WireHeaderKind,
    pub key: &'a [u8],
    pub value_kind: WireHeaderKind,
    pub value: &'a [u8],
}

/// Validate the structural integrity of a user headers byte buffer.
///
/// Walks TLV pairs and checks:
/// - Every kind byte is non-zero
/// - Every length is in `1..=255`
/// - No buffer overrun
/// - Total consumed bytes equals `buf.len()` (no trailing garbage)
/// - Even number of TLV entries (each pair = key + value)
///
/// Returns the number of complete key-value pairs on success.
///
/// # Errors
///
/// Returns `WireError::UnexpectedEof` if the buffer is truncated mid-entry,
/// or `WireError::Validation` if structural constraints are violated.
pub fn validate_user_headers(buf: &[u8]) -> Result<u32, WireError> {
    if buf.is_empty() {
        return Ok(0);
    }

    let mut pos = 0;
    let mut tlv_count: u32 = 0;

    while pos < buf.len() {
        let kind = read_u8(buf, pos)?;
        if kind == 0 {
            return Err(WireError::Validation(Cow::Owned(format!(
                "header kind is 0 (reserved) at offset {pos}"
            ))));
        }
        pos += 1;

        let length = read_u32_le(buf, pos)?;
        pos += 4;

        if length == 0 || length > MAX_FIELD_LENGTH {
            return Err(WireError::Validation(Cow::Owned(format!(
                "header field length {length} out of range 1..={MAX_FIELD_LENGTH} at offset {}",
                pos - 4
            ))));
        }

        let data_end = pos
            .checked_add(length as usize)
            .ok_or(WireError::Validation(Cow::Borrowed(
                "header field length overflow",
            )))?;
        if data_end > buf.len() {
            return Err(WireError::UnexpectedEof {
                offset: pos,
                need: length as usize,
                have: buf.len() - pos,
            });
        }
        pos = data_end;

        tlv_count = tlv_count
            .checked_add(1)
            .ok_or(WireError::Validation(Cow::Borrowed(
                "header entry count overflow",
            )))?;
    }

    if !tlv_count.is_multiple_of(2) {
        return Err(WireError::Validation(Cow::Owned(format!(
            "odd number of TLV entries ({tlv_count}), expected key-value pairs"
        ))));
    }

    Ok(tlv_count / 2)
}

/// Zero-copy iterator over pre-validated user header bytes.
///
/// Yields one [`WireUserHeaderEntry`] per key-value pair. The buffer
/// **must** have been validated by [`validate_user_headers`] first;
/// iterating an invalid buffer may yield garbage or panic.
pub struct WireUserHeaderIterator<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> WireUserHeaderIterator<'a> {
    /// Create an iterator over a pre-validated user headers buffer.
    #[must_use]
    pub const fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }
}

impl<'a> Iterator for WireUserHeaderIterator<'a> {
    type Item = WireUserHeaderEntry<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos >= self.buf.len() {
            return None;
        }

        // Key TLV
        let key_kind = self.buf[self.pos];
        self.pos += 1;

        let key_len = u32::from_le_bytes(
            self.buf[self.pos..self.pos + 4]
                .try_into()
                .expect("pre-validated buffer"),
        ) as usize;
        self.pos += 4;

        let key = &self.buf[self.pos..self.pos + key_len];
        self.pos += key_len;

        // Value TLV
        let value_kind = self.buf[self.pos];
        self.pos += 1;

        let value_len = u32::from_le_bytes(
            self.buf[self.pos..self.pos + 4]
                .try_into()
                .expect("pre-validated buffer"),
        ) as usize;
        self.pos += 4;

        let value = &self.buf[self.pos..self.pos + value_len];
        self.pos += value_len;

        Some(WireUserHeaderEntry {
            key_kind: WireHeaderKind(key_kind),
            key,
            value_kind: WireHeaderKind(value_kind),
            value,
        })
    }
}

/// Calculate the exact encoded size for a set of header entries.
///
/// Each entry tuple is `(key_kind, key_data, value_kind, value_data)`.
#[must_use]
pub fn user_headers_encoded_size(entries: &[(u8, &[u8], u8, &[u8])]) -> usize {
    entries
        .iter()
        .map(|(_, k, _, v)| 1 + 4 + k.len() + 1 + 4 + v.len())
        .sum()
}

/// Encode header entries into a caller-owned buffer.
///
/// Each entry tuple is `(key_kind, key_data, value_kind, value_data)`.
/// The caller should pre-allocate `buf` with [`user_headers_encoded_size`].
pub fn encode_user_headers(entries: &[(u8, &[u8], u8, &[u8])], buf: &mut BytesMut) {
    for &(key_kind, key_data, value_kind, value_data) in entries {
        buf.put_u8(key_kind);
        #[allow(clippy::cast_possible_truncation)]
        buf.put_u32_le(key_data.len() as u32);
        buf.put_slice(key_data);
        buf.put_u8(value_kind);
        #[allow(clippy::cast_possible_truncation)]
        buf.put_u32_le(value_data.len() as u32);
        buf.put_slice(value_data);
    }
}

/// Pre-validated user headers as a contiguous TLV byte buffer.
///
/// Construction validates structural integrity (no partial TLVs, no zero kinds,
/// no oversized fields, even entry count). The inner bytes are immutable.
///
/// This type is intentionally opaque at the wire layer. Domain-level
/// interpretation of kind codes happens in `iggy_common` via the conversion
/// bridge in `wire_conversions.rs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireUserHeaders(Bytes);

impl WireUserHeaders {
    /// Validate and wrap a borrowed byte slice (copies into new `Bytes`).
    ///
    /// # Errors
    /// Returns `WireError` if the buffer is not structurally valid TLV.
    pub fn from_slice(buf: &[u8]) -> Result<Self, WireError> {
        validate_user_headers(buf)?;
        Ok(Self(Bytes::copy_from_slice(buf)))
    }

    /// Validate and wrap an owned `Bytes` buffer (zero-copy).
    ///
    /// # Errors
    /// Returns `WireError` if the buffer is not structurally valid TLV.
    pub fn from_bytes(buf: Bytes) -> Result<Self, WireError> {
        validate_user_headers(&buf)?;
        Ok(Self(buf))
    }

    /// Wrap pre-validated bytes without re-checking.
    ///
    /// # Safety contract (not `unsafe`, but caller must ensure):
    /// The bytes must be structurally valid TLV as defined by
    /// [`validate_user_headers`]. Iterating invalid bytes will panic.
    pub const fn from_validated(buf: Bytes) -> Self {
        Self(buf)
    }

    /// Empty headers (zero-length buffer).
    #[must_use]
    pub const fn empty() -> Self {
        Self(Bytes::new())
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The raw validated bytes, suitable for wire transmission.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consume into the inner `Bytes` handle (zero-copy).
    #[must_use]
    pub fn into_bytes(self) -> Bytes {
        self.0
    }

    /// Zero-copy iteration over the pre-validated entries.
    #[must_use]
    pub fn iter(&self) -> WireUserHeaderIterator<'_> {
        WireUserHeaderIterator::new(&self.0)
    }
}

impl<'a> IntoIterator for &'a WireUserHeaders {
    type Item = WireUserHeaderEntry<'a>;
    type IntoIter = WireUserHeaderIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl WireEncode for WireUserHeaders {
    fn encoded_size(&self) -> usize {
        self.0.len()
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_slice(&self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encode_test_entries(entries: &[(u8, &[u8], u8, &[u8])]) -> Vec<u8> {
        let size = user_headers_encoded_size(entries);
        let mut buf = BytesMut::with_capacity(size);
        encode_user_headers(entries, &mut buf);
        buf.to_vec()
    }

    #[test]
    fn empty_buffer_is_valid() {
        assert_eq!(validate_user_headers(&[]).unwrap(), 0);
    }

    #[test]
    fn single_pair_round_trip() {
        let entries = [(6u8, b"test" as &[u8], 2u8, b"val" as &[u8])];
        let encoded = encode_test_entries(&entries);

        assert_eq!(validate_user_headers(&encoded).unwrap(), 1);

        let mut iter = WireUserHeaderIterator::new(&encoded);
        let entry = iter.next().unwrap();
        assert_eq!(entry.key_kind, WireHeaderKind(6));
        assert_eq!(entry.key, b"test");
        assert_eq!(entry.value_kind, WireHeaderKind(2));
        assert_eq!(entry.value, b"val");
        assert!(iter.next().is_none());
    }

    #[test]
    fn multiple_pairs() {
        let entries = [
            (1u8, b"k1" as &[u8], 2u8, b"v1" as &[u8]),
            (
                6u8,
                &42i32.to_le_bytes() as &[u8],
                12u8,
                &99u64.to_le_bytes() as &[u8],
            ),
            (3u8, &[1u8] as &[u8], 9u8, &[255u8] as &[u8]),
        ];
        let encoded = encode_test_entries(&entries);

        assert_eq!(validate_user_headers(&encoded).unwrap(), 3);

        let items: Vec<_> = WireUserHeaderIterator::new(&encoded).collect();
        assert_eq!(items.len(), 3);
        assert_eq!(items[0].key, b"k1");
        assert_eq!(items[1].key, &42i32.to_le_bytes());
        assert_eq!(items[2].value, &[255u8]);
    }

    #[test]
    fn unknown_kind_codes_pass_validation() {
        let entries = [(16u8, b"a" as &[u8], 255u8, b"b" as &[u8])];
        let encoded = encode_test_entries(&entries);
        assert_eq!(validate_user_headers(&encoded).unwrap(), 1);

        let entry = WireUserHeaderIterator::new(&encoded).next().unwrap();
        assert_eq!(entry.key_kind, WireHeaderKind(16));
        assert!(!entry.key_kind.is_known());
        assert_eq!(entry.value_kind, WireHeaderKind(255));
        assert!(!entry.value_kind.is_known());
    }

    #[test]
    fn known_kind_codes() {
        for code in 1..=15u8 {
            assert!(WireHeaderKind(code).is_known());
        }
        assert!(!WireHeaderKind(0).is_known());
        assert!(!WireHeaderKind(16).is_known());
    }

    #[test]
    fn truncated_at_kind_byte() {
        let entries = [(1u8, b"k" as &[u8], 2u8, b"v" as &[u8])];
        let encoded = encode_test_entries(&entries);
        // Truncate to just the first kind byte and length, missing data
        let result = validate_user_headers(&encoded[..3]);
        assert!(result.is_err());
    }

    #[test]
    fn truncated_at_length_field() {
        let entries = [(1u8, b"k" as &[u8], 2u8, b"v" as &[u8])];
        let encoded = encode_test_entries(&entries);
        // Only kind byte, partial length
        let result = validate_user_headers(&encoded[..2]);
        assert!(result.is_err());
    }

    #[test]
    fn truncated_at_data() {
        let entries = [(1u8, b"longkey" as &[u8], 2u8, b"v" as &[u8])];
        let encoded = encode_test_entries(&entries);
        // Kind + length present, but data truncated
        let result = validate_user_headers(&encoded[..7]);
        assert!(result.is_err());
    }

    #[test]
    fn zero_length_rejected() {
        // kind=1, length=0
        let buf = [1u8, 0, 0, 0, 0];
        let result = validate_user_headers(&buf);
        assert!(matches!(result, Err(WireError::Validation(_))));
    }

    #[test]
    fn length_exceeds_max_rejected() {
        // kind=1, length=256
        let mut buf = vec![1u8];
        buf.extend_from_slice(&256u32.to_le_bytes());
        buf.extend_from_slice(&[0u8; 256]);
        let result = validate_user_headers(&buf);
        assert!(matches!(result, Err(WireError::Validation(_))));
    }

    #[test]
    fn kind_zero_rejected() {
        // kind=0 is reserved
        let mut buf = vec![0u8];
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.push(42);
        let result = validate_user_headers(&buf);
        assert!(matches!(result, Err(WireError::Validation(_))));
    }

    #[test]
    fn trailing_bytes_rejected() {
        let entries = [(1u8, b"k" as &[u8], 2u8, b"v" as &[u8])];
        let mut encoded = encode_test_entries(&entries);
        encoded.push(0xFF); // trailing garbage
        let result = validate_user_headers(&encoded);
        assert!(result.is_err());
    }

    #[test]
    fn odd_tlv_count_rejected() {
        // One TLV entry (key without value) - odd count
        let mut buf = vec![1u8];
        buf.extend_from_slice(&2u32.to_le_bytes());
        buf.extend_from_slice(b"ab");
        let result = validate_user_headers(&buf);
        assert!(matches!(result, Err(WireError::Validation(_))));
    }

    #[test]
    fn encoded_size_matches_actual() {
        let entries = [
            (1u8, b"key1" as &[u8], 2u8, b"value1" as &[u8]),
            (
                6u8,
                &100i32.to_le_bytes() as &[u8],
                14u8,
                &std::f32::consts::PI.to_le_bytes() as &[u8],
            ),
        ];
        let expected_size = user_headers_encoded_size(&entries);
        let encoded = encode_test_entries(&entries);
        assert_eq!(expected_size, encoded.len());
    }

    #[test]
    fn max_length_field_accepted() {
        let data = [0xAA; 255];
        let entries = [(1u8, &data[..], 1u8, &data[..])];
        let encoded = encode_test_entries(&entries);
        assert_eq!(validate_user_headers(&encoded).unwrap(), 1);
    }

    #[test]
    fn wire_user_headers_empty() {
        let wire = WireUserHeaders::empty();
        assert!(wire.is_empty());
        assert_eq!(wire.as_bytes(), &[]);
        assert_eq!(wire.encoded_size(), 0);
    }

    #[test]
    fn wire_user_headers_from_slice_round_trip() {
        let entries = [(6u8, b"test" as &[u8], 2u8, b"val" as &[u8])];
        let encoded = encode_test_entries(&entries);

        let wire = WireUserHeaders::from_slice(&encoded).unwrap();
        assert!(!wire.is_empty());
        assert_eq!(wire.as_bytes(), &encoded);
        assert_eq!(wire.encoded_size(), encoded.len());

        let entry = wire.iter().next().unwrap();
        assert_eq!(entry.key_kind, WireHeaderKind(6));
        assert_eq!(entry.key, b"test");
    }

    #[test]
    fn wire_user_headers_from_bytes_zero_copy() {
        let entries = [(1u8, b"k" as &[u8], 2u8, b"v" as &[u8])];
        let encoded = Bytes::from(encode_test_entries(&entries));

        let wire = WireUserHeaders::from_bytes(encoded.clone()).unwrap();
        assert_eq!(wire.as_bytes(), &encoded[..]);
        assert_eq!(wire.into_bytes(), encoded);
    }

    #[test]
    fn wire_user_headers_rejects_invalid() {
        let buf = [0u8, 1, 0, 0, 0, 42]; // kind=0 is invalid
        assert!(WireUserHeaders::from_slice(&buf).is_err());
    }

    #[test]
    fn wire_user_headers_encode() {
        let entries = [(1u8, b"k" as &[u8], 2u8, b"v" as &[u8])];
        let encoded = encode_test_entries(&entries);
        let wire = WireUserHeaders::from_slice(&encoded).unwrap();

        let mut buf = BytesMut::with_capacity(wire.encoded_size());
        wire.encode(&mut buf);
        assert_eq!(&buf[..], &encoded);
    }
}
