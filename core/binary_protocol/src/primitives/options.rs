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

//! Key-value options block for `Create*` requests.
//!
//! Options reuse the user-headers TLV encoding and its structural validator
//! wholesale, so unknown value kind codes remain forwardable across
//! mixed-version clusters. On top of the structural walk this layer enforces
//! the stricter contract options need and messages do not: string keys only,
//! no duplicate keys, and hard bounds on entry count, key length, and total
//! block size, all rejected at the wire edge before a request reaches
//! admission.

use std::borrow::Cow;

use bytes::{BufMut, Bytes, BytesMut};

use crate::WireError;
use crate::codec::WireEncode;
use crate::primitives::user_headers::{
    WireHeaderKind, WireUserHeaderEntry, WireUserHeaderIterator, validate_user_headers,
};

/// Maximum number of key-value entries in one options block.
pub const MAX_OPTIONS: u32 = 1024;

/// Maximum total byte length of an encoded options block.
///
/// Mirrors `iggy_common::MAX_USER_HEADERS_SIZE`: options ride the user-headers
/// codec, so they inherit its budget rather than inventing a second one. The
/// constant is duplicated because `iggy_common` depends on this crate, not the
/// other way round; `options_block_budget_matches_user_headers` pins the two
/// together from the side that can see both.
///
/// Sized so [`MAX_OPTIONS`] is actually reachable: the cheapest entry is 12
/// bytes (kind + length + one byte, for key and value each), so 1024 entries
/// need at least 12 KiB. A smaller budget would silently cap the entry count
/// below `MAX_OPTIONS` and make that limit a lie.
pub const MAX_OPTIONS_BYTES: usize = 100 * 1000;

/// Wire kind code for UTF-8 string fields, matching `HeaderKind::String`
/// in `iggy_common`.
const STRING_KIND: WireHeaderKind = WireHeaderKind(2);

/// Validate the structural and semantic integrity of an options byte buffer.
///
/// Runs [`validate_user_headers`] first, then enforces the options contract:
///
/// - Total block size within [`MAX_OPTIONS_BYTES`]
/// - At most [`MAX_OPTIONS`] entries
/// - Every key is a UTF-8 string (length already bounded by the codec)
/// - No duplicate keys
///
/// Value kind codes are deliberately NOT restricted to the currently defined
/// range, mirroring the user-headers forward-compatibility contract.
///
/// Returns the number of key-value pairs on success.
///
/// # Errors
///
/// Returns `WireError::UnexpectedEof` if the buffer is truncated mid-entry,
/// or `WireError::Validation` if any constraint above is violated.
pub fn validate_options(buf: &[u8]) -> Result<u32, WireError> {
    if buf.len() > MAX_OPTIONS_BYTES {
        return Err(WireError::Validation(Cow::Owned(format!(
            "options block is {} bytes, exceeds maximum {MAX_OPTIONS_BYTES}",
            buf.len()
        ))));
    }

    let pairs = validate_user_headers(buf)?;
    if pairs == 0 {
        return Ok(0);
    }
    if pairs > MAX_OPTIONS {
        return Err(WireError::Validation(Cow::Owned(format!(
            "options block has {pairs} entries, exceeds maximum {MAX_OPTIONS}"
        ))));
    }

    let mut keys: Vec<&[u8]> = Vec::with_capacity(pairs as usize);
    for entry in WireUserHeaderIterator::new(buf) {
        if entry.key_kind != STRING_KIND {
            return Err(WireError::Validation(Cow::Owned(format!(
                "option key kind {} is not a string",
                entry.key_kind.0
            ))));
        }
        if std::str::from_utf8(entry.key).is_err() {
            return Err(WireError::Validation(Cow::Borrowed(
                "option key is not valid UTF-8",
            )));
        }
        keys.push(entry.key);
    }

    keys.sort_unstable();
    for pair in keys.windows(2) {
        if pair[0] == pair[1] {
            return Err(WireError::Validation(Cow::Owned(format!(
                "duplicate option key: {}",
                String::from_utf8_lossy(pair[0])
            ))));
        }
    }

    Ok(pairs)
}

/// Encoded size of a `u32`-length-prefixed options block.
///
/// Response elements (topic/stream/user entries in a list) cannot run their
/// options to end-of-payload the way requests do, so they carry the block
/// behind a length prefix.
#[must_use]
pub fn options_prefixed_size(options: &WireOptions) -> usize {
    4 + options.encoded_size()
}

/// Encode a `u32`-length-prefixed options block.
pub fn encode_options_prefixed(options: &WireOptions, buf: &mut BytesMut) {
    #[allow(clippy::cast_possible_truncation)]
    buf.put_u32_le(options.encoded_size() as u32);
    options.encode(buf);
}

/// Decode a `u32`-length-prefixed options block at `pos`.
///
/// Returns the block and the total bytes consumed (prefix included).
///
/// # Errors
///
/// Returns `WireError::UnexpectedEof` on truncation, or
/// `WireError::Validation` when the block violates [`validate_options`].
pub fn decode_options_prefixed(buf: &[u8], pos: usize) -> Result<(WireOptions, usize), WireError> {
    let length = crate::codec::read_u32_le(buf, pos)? as usize;
    let start = pos + 4;
    // `checked_add`: on a 32-bit target a wire-supplied length wraps, the
    // truncation guard below then passes, and the slice panics instead.
    let end = start
        .checked_add(length)
        .ok_or_else(|| WireError::UnexpectedEof {
            offset: start,
            need: length,
            have: buf.len().saturating_sub(start),
        })?;
    if buf.len() < end {
        return Err(WireError::UnexpectedEof {
            offset: start,
            need: length,
            have: buf.len().saturating_sub(start),
        });
    }
    let options = WireOptions::from_slice(&buf[start..end])?;
    Ok((options, 4 + length))
}

/// Pre-validated options as a contiguous TLV byte buffer.
///
/// Construction validates both structural TLV integrity and the options
/// contract via [`validate_options`]. The inner bytes are immutable.
///
/// Like `WireUserHeaders`, this type is opaque at the wire layer. Domain
/// interpretation of value kind codes happens in `iggy_common`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WireOptions(Bytes);

impl WireOptions {
    /// Validate and wrap a borrowed byte slice (copies into new `Bytes`).
    ///
    /// # Errors
    /// Returns `WireError` if the buffer violates [`validate_options`].
    pub fn from_slice(buf: &[u8]) -> Result<Self, WireError> {
        validate_options(buf)?;
        Ok(Self(Bytes::copy_from_slice(buf)))
    }

    /// Validate and wrap an owned `Bytes` buffer (zero-copy).
    ///
    /// # Errors
    /// Returns `WireError` if the buffer violates [`validate_options`].
    pub fn from_bytes(buf: Bytes) -> Result<Self, WireError> {
        validate_options(&buf)?;
        Ok(Self(buf))
    }

    /// Wrap pre-validated bytes without re-checking.
    ///
    /// # Safety contract (not `unsafe`, but caller must ensure):
    /// The bytes must satisfy [`validate_options`]. Iterating invalid bytes
    /// will panic.
    pub const fn from_validated(buf: Bytes) -> Self {
        Self(buf)
    }

    /// Empty options (zero-length buffer).
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

impl<'a> IntoIterator for &'a WireOptions {
    type Item = WireUserHeaderEntry<'a>;
    type IntoIter = WireUserHeaderIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

impl WireEncode for WireOptions {
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
    use crate::primitives::user_headers::encode_user_headers;

    const STRING: u8 = 2;
    const UINT64: u8 = 12;

    fn encode(entries: &[(u8, &[u8], u8, &[u8])]) -> BytesMut {
        let mut buf = BytesMut::new();
        encode_user_headers(entries, &mut buf);
        buf
    }

    #[test]
    fn empty_buffer_is_zero_options() {
        assert_eq!(validate_options(&[]).unwrap(), 0);
        assert!(WireOptions::empty().is_empty());
    }

    #[test]
    fn valid_block_roundtrips() {
        let value = 1_073_741_824u64.to_le_bytes();
        let buf = encode(&[
            (STRING, b"segment_size", UINT64, &value),
            (STRING, b"message_expiry", STRING, b"7 days"),
        ]);
        assert_eq!(validate_options(&buf).unwrap(), 2);

        let options = WireOptions::from_slice(&buf).unwrap();
        let entries: Vec<_> = options.iter().collect();
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].key, b"segment_size");
        assert_eq!(entries[0].value, value);
        assert_eq!(entries[1].key, b"message_expiry");
        assert_eq!(entries[1].value, b"7 days");
    }

    #[test]
    fn unknown_value_kind_is_forwardable() {
        let buf = encode(&[(STRING, b"future_option", 200, b"opaque")]);
        assert_eq!(validate_options(&buf).unwrap(), 1);
    }

    #[test]
    fn duplicate_key_is_rejected() {
        let buf = encode(&[
            (STRING, b"segment_size", STRING, b"1 GiB"),
            (STRING, b"segment_size", STRING, b"2 GiB"),
        ]);
        assert!(matches!(
            validate_options(&buf),
            Err(WireError::Validation(_))
        ));
    }

    #[test]
    fn non_string_key_kind_is_rejected() {
        let buf = encode(&[(UINT64, &1u64.to_le_bytes(), STRING, b"value")]);
        assert!(matches!(
            validate_options(&buf),
            Err(WireError::Validation(_))
        ));
    }

    #[test]
    fn non_utf8_key_is_rejected() {
        let buf = encode(&[(STRING, &[0xFF, 0xFE], STRING, b"value")]);
        assert!(matches!(
            validate_options(&buf),
            Err(WireError::Validation(_))
        ));
    }

    #[test]
    fn key_length_is_bounded_by_the_user_headers_codec() {
        // Options add no key-length rule of their own: every field is already
        // bounded to 1..=255 by `validate_user_headers`, so the codec's limit
        // is the option key's limit.
        let over = [b'k'; 256];
        let buf = encode(&[(STRING, &over, STRING, b"value")]);
        assert!(matches!(
            validate_options(&buf),
            Err(WireError::Validation(_))
        ));

        let at_limit = [b'k'; 255];
        let buf = encode(&[(STRING, &at_limit, STRING, b"value")]);
        assert_eq!(validate_options(&buf).unwrap(), 1);
    }

    #[test]
    fn entry_count_over_limit_is_rejected() {
        let keys: Vec<String> = (0..=MAX_OPTIONS).map(|i| format!("key_{i}")).collect();
        let entries: Vec<(u8, &[u8], u8, &[u8])> = keys
            .iter()
            .map(|key| (STRING, key.as_bytes(), STRING, b"v".as_slice()))
            .collect();
        let buf = encode(&entries);
        assert!(matches!(
            validate_options(&buf),
            Err(WireError::Validation(_))
        ));

        let buf = encode(&entries[..MAX_OPTIONS as usize]);
        assert_eq!(validate_options(&buf).unwrap(), MAX_OPTIONS);
    }

    #[test]
    fn block_over_byte_limit_is_rejected() {
        // 200 maximum-size entries are ~104 KB, over the byte budget while
        // still well under `MAX_OPTIONS`, so this proves the byte cap trips
        // independently of the entry count.
        let value = [b'v'; 255];
        let keys: Vec<String> = (0..200).map(|i| format!("{i:0>255}")).collect();
        let entries: Vec<(u8, &[u8], u8, &[u8])> = keys
            .iter()
            .map(|key| (STRING, key.as_bytes(), STRING, value.as_slice()))
            .collect();
        let buf = encode(&entries);
        assert!(buf.len() > MAX_OPTIONS_BYTES);
        assert!(matches!(
            validate_options(&buf),
            Err(WireError::Validation(_))
        ));
    }

    #[test]
    fn truncated_block_errors() {
        let buf = encode(&[(STRING, b"segment_size", STRING, b"1 GiB")]);
        for i in 1..buf.len() {
            assert!(
                validate_options(&buf[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn wire_options_constructors_validate() {
        let buf = encode(&[
            (STRING, b"enforce_fsync", STRING, b"true"),
            (STRING, b"enforce_fsync", STRING, b"false"),
        ]);
        assert!(WireOptions::from_slice(&buf).is_err());
        assert!(WireOptions::from_bytes(buf.freeze()).is_err());
    }

    #[test]
    fn encoded_size_matches_bytes() {
        let buf = encode(&[(STRING, b"key", STRING, b"value")]);
        let options = WireOptions::from_slice(&buf).unwrap();
        assert_eq!(options.encoded_size(), buf.len());
        assert_eq!(options.as_bytes(), &buf[..]);
    }

    #[test]
    fn prefixed_roundtrip() {
        let buf = encode(&[(STRING, b"key", STRING, b"value")]);
        let options = WireOptions::from_slice(&buf).unwrap();
        let mut encoded = BytesMut::new();
        encode_options_prefixed(&options, &mut encoded);
        assert_eq!(encoded.len(), options_prefixed_size(&options));
        let (decoded, consumed) = decode_options_prefixed(&encoded, 0).unwrap();
        assert_eq!(consumed, encoded.len());
        assert_eq!(decoded, options);
    }

    /// The cross-SDK golden vector for an options block.
    ///
    /// Every SDK writes this block from its own encoder, so "it round-trips
    /// through my own decoder" proves nothing about interoperability. The bytes
    /// here are the contract: `foreign/{node,go,java}` pin the identical vector
    /// in their own unit tests, and a change to the TLV layout has to break all
    /// of them together instead of leaving one SDK talking to itself.
    ///
    /// `enforce_fsync=true` (a `Bool`) and `segment_size=1 GiB` (a `Uint64`)
    /// cover both a one-byte and an eight-byte value, in the sorted key order
    /// every encoder has to produce.
    const GOLDEN_OPTIONS_BLOCK: &[u8] = &[
        2, 13, 0, 0, 0, // key kind String, length 13
        b'e', b'n', b'f', b'o', b'r', b'c', b'e', b'_', b'f', b's', b'y', b'n', b'c', 3, 1, 0, 0,
        0, 1, // value kind Bool, length 1, true
        2, 12, 0, 0, 0, // key kind String, length 12
        b's', b'e', b'g', b'm', b'e', b'n', b't', b'_', b's', b'i', b'z', b'e', 12, 8, 0, 0,
        0, // value kind Uint64, length 8
        0, 0, 0, 64, 0, 0, 0, 0, // 1 GiB little-endian
    ];

    #[test]
    fn golden_options_block_is_byte_stable() {
        let encoded = encode(&[
            (STRING, b"enforce_fsync", 3, &[1]),
            (
                STRING,
                b"segment_size",
                UINT64,
                &1_073_741_824u64.to_le_bytes(),
            ),
        ]);

        assert_eq!(
            &encoded[..],
            GOLDEN_OPTIONS_BLOCK,
            "the options TLV layout changed; update every SDK's copy of this vector"
        );
        assert_eq!(validate_options(GOLDEN_OPTIONS_BLOCK).unwrap(), 2);
    }

    #[test]
    fn prefixed_truncation_errors() {
        let buf = encode(&[(STRING, b"key", STRING, b"value")]);
        let options = WireOptions::from_slice(&buf).unwrap();
        let mut encoded = BytesMut::new();
        encode_options_prefixed(&options, &mut encoded);
        for i in 0..encoded.len() {
            assert!(
                decode_options_prefixed(&encoded[..i], 0).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }
}
