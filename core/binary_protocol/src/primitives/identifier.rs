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

use crate::WireError;
use crate::codec::{WireDecode, WireEncode, read_bytes, read_str, read_u8};
use bytes::{BufMut, BytesMut};
use std::ops::Deref;

// WireName

/// Maximum byte length for a wire name (fits in a u8 length prefix).
pub const MAX_WIRE_NAME_LENGTH: usize = 255;

/// Validated name type used by protocol request/response types.
///
/// Guarantees the inner string is 1-255 bytes, matching the u8
/// length prefix used on the wire.
#[derive(Clone, PartialEq, Eq)]
pub struct WireName(String);

impl WireName {
    /// Create a new `WireName`, validating the length is 1-255 bytes.
    ///
    /// # Errors
    /// Returns `WireError::Validation` if the name is empty or exceeds 255 bytes.
    pub fn new(s: impl Into<String>) -> Result<Self, WireError> {
        let s = s.into();
        if s.is_empty() || s.len() > MAX_WIRE_NAME_LENGTH {
            return Err(WireError::Validation(format!(
                "wire name must be 1-{MAX_WIRE_NAME_LENGTH} bytes, got {}",
                s.len()
            )));
        }
        Ok(Self(s))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    #[must_use]
    pub const fn len(&self) -> usize {
        self.0.len()
    }

    /// Always returns `false` - `WireName` is guaranteed non-empty by construction.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        false
    }
}

impl Deref for WireName {
    type Target = str;

    fn deref(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for WireName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::fmt::Debug for WireName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WireName({:?})", self.0)
    }
}

impl WireEncode for WireName {
    fn encoded_size(&self) -> usize {
        1 + self.0.len()
    }

    fn encode(&self, buf: &mut BytesMut) {
        // Length guaranteed <= 255 by construction, truncation impossible.
        #[allow(clippy::cast_possible_truncation)]
        buf.put_u8(self.0.len() as u8);
        buf.put_slice(self.0.as_bytes());
    }
}

impl WireDecode for WireName {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let name_len = read_u8(buf, 0)? as usize;
        if name_len == 0 || name_len > MAX_WIRE_NAME_LENGTH {
            return Err(WireError::Validation(format!(
                "wire name must be 1-{MAX_WIRE_NAME_LENGTH} bytes, got {name_len}"
            )));
        }
        let name = read_str(buf, 1, name_len)?;
        Ok((Self(name), 1 + name_len))
    }
}

// WireIdentifier

const KIND_NUMERIC: u8 = 1;
const KIND_STRING: u8 = 2;
const NUMERIC_VALUE_LEN: u8 = 4;

/// Protocol-owned identifier type. Polymorphic: numeric (u32) or string.
///
/// Wire format: `[kind:1][length:1][value:N]`
/// - Numeric: kind=1, length=4, value=u32 LE
/// - String:  kind=2, length=1..255, value=UTF-8 bytes
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireIdentifier {
    Numeric(u32),
    String(WireName),
}

impl WireIdentifier {
    #[must_use]
    pub const fn numeric(id: u32) -> Self {
        Self::Numeric(id)
    }

    /// # Errors
    /// Returns `WireError::Validation` if the name is empty or exceeds 255 bytes.
    pub fn named(name: impl Into<String>) -> Result<Self, WireError> {
        let wire_name = WireName::new(name)?;
        Ok(Self::String(wire_name))
    }

    #[must_use]
    pub const fn as_u32(&self) -> Option<u32> {
        match self {
            Self::Numeric(id) => Some(*id),
            Self::String(_) => None,
        }
    }

    #[must_use]
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Numeric(_) => None,
            Self::String(s) => Some(s.as_str()),
        }
    }

    const fn kind_code(&self) -> u8 {
        match self {
            Self::Numeric(_) => KIND_NUMERIC,
            Self::String(_) => KIND_STRING,
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    const fn value_len(&self) -> u8 {
        match self {
            Self::Numeric(_) => NUMERIC_VALUE_LEN,
            // WireName guarantees len <= 255, truncation impossible.
            Self::String(s) => s.len() as u8,
        }
    }
}

impl WireEncode for WireIdentifier {
    fn encoded_size(&self) -> usize {
        2 + self.value_len() as usize
    }

    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(self.kind_code());
        buf.put_u8(self.value_len());
        match self {
            Self::Numeric(id) => buf.put_u32_le(*id),
            Self::String(s) => buf.put_slice(s.as_bytes()),
        }
    }
}

impl WireDecode for WireIdentifier {
    fn decode(buf: &[u8]) -> Result<(Self, usize), WireError> {
        let kind = read_u8(buf, 0)?;
        let length = read_u8(buf, 1)? as usize;
        let value = read_bytes(buf, 2, length)?;
        let consumed = 2 + length;

        match kind {
            KIND_NUMERIC => {
                if length != NUMERIC_VALUE_LEN as usize {
                    return Err(WireError::Validation(format!(
                        "numeric identifier must be {NUMERIC_VALUE_LEN} bytes, got {length}"
                    )));
                }
                let id = u32::from_le_bytes(
                    value
                        .try_into()
                        .expect("length already validated as 4 bytes"),
                );
                Ok((Self::Numeric(id), consumed))
            }
            KIND_STRING => {
                if length == 0 {
                    return Err(WireError::Validation(
                        "string identifier cannot be empty".to_string(),
                    ));
                }
                let s =
                    std::str::from_utf8(value).map_err(|_| WireError::InvalidUtf8 { offset: 2 })?;
                let wire_name = WireName::new(s)?;
                Ok((Self::String(wire_name), consumed))
            }
            _ => Err(WireError::UnknownDiscriminant {
                type_name: "WireIdentifier",
                value: kind,
                offset: 0,
            }),
        }
    }
}

impl std::fmt::Display for WireIdentifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Numeric(id) => write!(f, "{id}"),
            Self::String(s) => write!(f, "{s}"),
        }
    }
}

// Tests

#[cfg(test)]
mod tests {
    use super::{KIND_NUMERIC, KIND_STRING, WireIdentifier, WireName};
    use crate::codec::{WireDecode, WireEncode};

    // -- WireName tests --

    #[test]
    fn wire_name_valid() {
        let name = WireName::new("test").unwrap();
        assert_eq!(name.as_str(), "test");
        assert_eq!(name.len(), 4);
        assert_eq!(&*name, "test");
    }

    #[test]
    fn wire_name_empty_rejected() {
        assert!(WireName::new("").is_err());
    }

    #[test]
    fn wire_name_too_long_rejected() {
        assert!(WireName::new("a".repeat(256)).is_err());
    }

    #[test]
    fn wire_name_max_length_accepted() {
        assert!(WireName::new("a".repeat(255)).is_ok());
    }

    #[test]
    fn wire_name_roundtrip() {
        let name = WireName::new("my-stream").unwrap();
        let bytes = name.to_bytes();
        assert_eq!(bytes.len(), 1 + 9);
        let (decoded, consumed) = WireName::decode(&bytes).unwrap();
        assert_eq!(consumed, 10);
        assert_eq!(decoded, name);
    }

    #[test]
    fn wire_name_display() {
        let name = WireName::new("hello").unwrap();
        assert_eq!(format!("{name}"), "hello");
    }

    #[test]
    fn wire_name_deref_to_str() {
        let name = WireName::new("test").unwrap();
        assert!(name.starts_with("te"));
    }

    #[test]
    fn wire_name_non_ascii_utf8_roundtrip() {
        let name = WireName::new("str\u{00e9}am-\u{00fc}ser").unwrap();
        let bytes = name.to_bytes();
        let (decoded, _) = WireName::decode(&bytes).unwrap();
        assert_eq!(decoded, name);
    }

    // -- WireIdentifier tests --

    #[test]
    fn roundtrip_numeric() {
        let id = WireIdentifier::numeric(42);
        let bytes = id.to_bytes();
        assert_eq!(bytes.len(), 6);
        let (decoded, consumed) = WireIdentifier::decode(&bytes).unwrap();
        assert_eq!(consumed, 6);
        assert_eq!(decoded, id);
        assert_eq!(decoded.as_u32(), Some(42));
    }

    #[test]
    fn roundtrip_string() {
        let id = WireIdentifier::named("my-stream").unwrap();
        let bytes = id.to_bytes();
        assert_eq!(bytes.len(), 2 + 9);
        let (decoded, consumed) = WireIdentifier::decode(&bytes).unwrap();
        assert_eq!(consumed, 11);
        assert_eq!(decoded, id);
        assert_eq!(decoded.as_str(), Some("my-stream"));
    }

    #[test]
    fn empty_string_rejected() {
        assert!(WireIdentifier::named("").is_err());
    }

    #[test]
    fn max_length_string_accepted() {
        let name = "a".repeat(255);
        let id = WireIdentifier::named(&name).unwrap();
        let bytes = id.to_bytes();
        let (decoded, _) = WireIdentifier::decode(&bytes).unwrap();
        assert_eq!(decoded.as_str(), Some(name.as_str()));
    }

    #[test]
    fn too_long_string_rejected() {
        assert!(WireIdentifier::named("a".repeat(256)).is_err());
    }

    #[test]
    fn truncated_buffer_returns_error() {
        let id = WireIdentifier::numeric(1);
        let bytes = id.to_bytes();
        for i in 0..bytes.len() {
            assert!(
                WireIdentifier::decode(&bytes[..i]).is_err(),
                "expected error for truncation at byte {i}"
            );
        }
    }

    #[test]
    fn unknown_kind_returns_error() {
        let buf = [0xFF, 0x04, 0x01, 0x00, 0x00, 0x00];
        assert!(WireIdentifier::decode(&buf).is_err());
    }

    #[test]
    fn numeric_with_wrong_length_returns_error() {
        let buf = [KIND_NUMERIC, 0x03, 0x01, 0x00, 0x00];
        assert!(WireIdentifier::decode(&buf).is_err());
    }

    #[test]
    fn string_with_zero_length_returns_error() {
        let buf = [KIND_STRING, 0x00];
        assert!(WireIdentifier::decode(&buf).is_err());
    }

    #[test]
    fn display_numeric() {
        let id = WireIdentifier::numeric(7);
        assert_eq!(format!("{id}"), "7");
    }

    #[test]
    fn display_string() {
        let id = WireIdentifier::named("test").unwrap();
        assert_eq!(format!("{id}"), "test");
    }

    #[test]
    fn non_ascii_utf8_string_roundtrip() {
        let id = WireIdentifier::named("caf\u{00e9}-latt\u{00e9}").unwrap();
        let bytes = id.to_bytes();
        let (decoded, _) = WireIdentifier::decode(&bytes).unwrap();
        assert_eq!(decoded, id);
    }

    #[test]
    fn invalid_utf8_bytes_rejected() {
        let buf = [KIND_STRING, 0x02, 0xFF, 0xFE];
        assert!(WireIdentifier::decode(&buf).is_err());
    }
}
