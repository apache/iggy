/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::BytesSerializable;
use crate::error::IggyError;
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use serde_with::base64::Base64;
use serde_with::serde_as;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::str::FromStr;

/// Represents a header key with a unique name. The name is case-insensitive and wraps a string.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct HeaderKey(String);

impl HeaderKey {
    pub fn new(key: &str) -> Result<Self, IggyError> {
        if key.is_empty() || key.len() > 255 {
            return Err(IggyError::InvalidHeaderKey);
        }

        Ok(Self(key.to_lowercase().to_string()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Display for HeaderKey {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl Hash for HeaderKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl FromStr for HeaderKey {
    type Err = IggyError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s)
    }
}

impl TryFrom<&str> for HeaderKey {
    type Error = IggyError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

/// Represents a header value of a specific kind.
/// It consists of the following fields:
/// - `kind`: the kind of the header value.
/// - `value`: the value of the header.
#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct HeaderValue {
    /// The kind of the header value.
    pub kind: HeaderKind,
    /// The binary value of the header payload.
    #[serde_as(as = "Base64")]
    pub value: Bytes,
}

/// Represents the kind of a header value.
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum HeaderKind {
    Raw,
    String,
    Bool,
    Int8,
    Int16,
    Int32,
    Int64,
    Int128,
    Uint8,
    Uint16,
    Uint32,
    Uint64,
    Uint128,
    Float32,
    Float64,
}

impl HeaderKind {
    /// Returns the code of the header kind.
    pub fn as_code(&self) -> u8 {
        match self {
            HeaderKind::Raw => 1,
            HeaderKind::String => 2,
            HeaderKind::Bool => 3,
            HeaderKind::Int8 => 4,
            HeaderKind::Int16 => 5,
            HeaderKind::Int32 => 6,
            HeaderKind::Int64 => 7,
            HeaderKind::Int128 => 8,
            HeaderKind::Uint8 => 9,
            HeaderKind::Uint16 => 10,
            HeaderKind::Uint32 => 11,
            HeaderKind::Uint64 => 12,
            HeaderKind::Uint128 => 13,
            HeaderKind::Float32 => 14,
            HeaderKind::Float64 => 15,
        }
    }

    /// Returns the header kind from the code.
    pub fn from_code(code: u8) -> Result<Self, IggyError> {
        match code {
            1 => Ok(HeaderKind::Raw),
            2 => Ok(HeaderKind::String),
            3 => Ok(HeaderKind::Bool),
            4 => Ok(HeaderKind::Int8),
            5 => Ok(HeaderKind::Int16),
            6 => Ok(HeaderKind::Int32),
            7 => Ok(HeaderKind::Int64),
            8 => Ok(HeaderKind::Int128),
            9 => Ok(HeaderKind::Uint8),
            10 => Ok(HeaderKind::Uint16),
            11 => Ok(HeaderKind::Uint32),
            12 => Ok(HeaderKind::Uint64),
            13 => Ok(HeaderKind::Uint128),
            14 => Ok(HeaderKind::Float32),
            15 => Ok(HeaderKind::Float64),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}

impl FromStr for HeaderKind {
    type Err = IggyError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "raw" => Ok(HeaderKind::Raw),
            "string" => Ok(HeaderKind::String),
            "bool" => Ok(HeaderKind::Bool),
            "int8" => Ok(HeaderKind::Int8),
            "int16" => Ok(HeaderKind::Int16),
            "int32" => Ok(HeaderKind::Int32),
            "int64" => Ok(HeaderKind::Int64),
            "int128" => Ok(HeaderKind::Int128),
            "uint8" => Ok(HeaderKind::Uint8),
            "uint16" => Ok(HeaderKind::Uint16),
            "uint32" => Ok(HeaderKind::Uint32),
            "uint64" => Ok(HeaderKind::Uint64),
            "uint128" => Ok(HeaderKind::Uint128),
            "float32" => Ok(HeaderKind::Float32),
            "float64" => Ok(HeaderKind::Float64),
            _ => Err(IggyError::CannotParseHeaderKind(s.to_string())),
        }
    }
}

impl Display for HeaderValue {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: ", self.kind)?;
        write!(f, "{}", self.value_only_to_string())
    }
}

impl Display for HeaderKind {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        match *self {
            HeaderKind::Raw => write!(f, "raw"),
            HeaderKind::String => write!(f, "string"),
            HeaderKind::Bool => write!(f, "bool"),
            HeaderKind::Int8 => write!(f, "int8"),
            HeaderKind::Int16 => write!(f, "int16"),
            HeaderKind::Int32 => write!(f, "int32"),
            HeaderKind::Int64 => write!(f, "int64"),
            HeaderKind::Int128 => write!(f, "int128"),
            HeaderKind::Uint8 => write!(f, "uint8"),
            HeaderKind::Uint16 => write!(f, "uint16"),
            HeaderKind::Uint32 => write!(f, "uint32"),
            HeaderKind::Uint64 => write!(f, "uint64"),
            HeaderKind::Uint128 => write!(f, "uint128"),
            HeaderKind::Float32 => write!(f, "float32"),
            HeaderKind::Float64 => write!(f, "float64"),
        }
    }
}

impl FromStr for HeaderValue {
    type Err = IggyError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::from(HeaderKind::String, s.as_bytes())
    }
}

impl HeaderValue {
    /// Creates a new header value from the specified kind and value.
    /// The kind is parsed from the string representation.
    /// The value is parsed from the string representation.
    pub fn from_kind_str_and_value_str(kind: &str, value: &str) -> Result<Self, IggyError> {
        let kind = HeaderKind::from_str(kind)?;
        Self::from_kind_and_value_str(kind, value)
    }

    /// Creates a new header value from the specified kind and value.
    /// The value is parsed from the string representation.
    pub fn from_kind_and_value_str(kind: HeaderKind, value: &str) -> Result<Self, IggyError> {
        match kind {
            HeaderKind::Raw => Self::from_raw(value.as_bytes()),
            HeaderKind::String => Self::from_str(value),
            HeaderKind::Bool => {
                Self::from_bool(value.parse().map_err(|_| IggyError::InvalidBooleanValue)?)
            }
            HeaderKind::Int8 => {
                Self::from_int8(value.parse().map_err(|_| IggyError::InvalidNumberValue)?)
            }
            HeaderKind::Int16 => {
                Self::from_int16(value.parse().map_err(|_| IggyError::InvalidNumberValue)?)
            }
            HeaderKind::Int32 => {
                Self::from_int32(value.parse().map_err(|_| IggyError::InvalidNumberValue)?)
            }
            HeaderKind::Int64 => {
                Self::from_int64(value.parse().map_err(|_| IggyError::InvalidNumberValue)?)
            }
            HeaderKind::Int128 => {
                Self::from_int128(value.parse().map_err(|_| IggyError::InvalidNumberValue)?)
            }
            HeaderKind::Uint8 => {
                Self::from_uint8(value.parse().map_err(|_| IggyError::InvalidNumberValue)?)
            }
            HeaderKind::Uint16 => {
                Self::from_uint16(value.parse().map_err(|_| IggyError::InvalidNumberValue)?)
            }
            HeaderKind::Uint32 => {
                Self::from_uint32(value.parse().map_err(|_| IggyError::InvalidNumberValue)?)
            }
            HeaderKind::Uint64 => {
                Self::from_uint64(value.parse().map_err(|_| IggyError::InvalidNumberValue)?)
            }
            HeaderKind::Uint128 => {
                Self::from_uint128(value.parse().map_err(|_| IggyError::InvalidNumberValue)?)
            }
            HeaderKind::Float32 => {
                Self::from_float32(value.parse().map_err(|_| IggyError::InvalidNumberValue)?)
            }
            HeaderKind::Float64 => {
                Self::from_float64(value.parse().map_err(|_| IggyError::InvalidNumberValue)?)
            }
        }
    }
    /// Creates a new header value from the specified raw bytes.
    pub fn from_raw(value: &[u8]) -> Result<Self, IggyError> {
        Self::from(HeaderKind::Raw, value)
    }

    /// Returns the raw bytes of the header value.
    pub fn as_raw(&self) -> Result<&[u8], IggyError> {
        if self.kind != HeaderKind::Raw {
            return Err(IggyError::InvalidHeaderValue);
        }

        Ok(&self.value)
    }

    /// Returns the string representation of the header value.
    pub fn as_str(&self) -> Result<&str, IggyError> {
        if self.kind != HeaderKind::String {
            return Err(IggyError::InvalidHeaderValue);
        }

        std::str::from_utf8(&self.value).map_err(|_| IggyError::InvalidUtf8)
    }

    /// Creates a new header value from the specified string.
    pub fn from_bool(value: bool) -> Result<Self, IggyError> {
        Self::from(HeaderKind::Bool, if value { &[1] } else { &[0] })
    }

    /// Returns the boolean representation of the header value.
    pub fn as_bool(&self) -> Result<bool, IggyError> {
        if self.kind != HeaderKind::Bool {
            return Err(IggyError::InvalidHeaderValue);
        }

        match self.value[0] {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(IggyError::InvalidHeaderValue),
        }
    }

    /// Creates a new header value from the specified boolean.
    pub fn from_int8(value: i8) -> Result<Self, IggyError> {
        Self::from(HeaderKind::Int8, &value.to_le_bytes())
    }

    /// Returns the i8 representation of the header value.
    pub fn as_int8(&self) -> Result<i8, IggyError> {
        if self.kind != HeaderKind::Int8 {
            return Err(IggyError::InvalidHeaderValue);
        }

        let value = self.value.to_vec().try_into();
        if value.is_err() {
            return Err(IggyError::InvalidHeaderValue);
        }

        Ok(i8::from_le_bytes(value.unwrap()))
    }

    /// Creates a new header value from the specified i8.
    pub fn from_int16(value: i16) -> Result<Self, IggyError> {
        Self::from(HeaderKind::Int16, &value.to_le_bytes())
    }

    /// Returns the i16 representation of the header value.
    pub fn as_int16(&self) -> Result<i16, IggyError> {
        if self.kind != HeaderKind::Int16 {
            return Err(IggyError::InvalidHeaderValue);
        }

        let value = self.value.to_vec().try_into();
        if value.is_err() {
            return Err(IggyError::InvalidHeaderValue);
        }

        Ok(i16::from_le_bytes(value.unwrap()))
    }

    /// Creates a new header value from the specified i16.
    pub fn from_int32(value: i32) -> Result<Self, IggyError> {
        Self::from(HeaderKind::Int32, &value.to_le_bytes())
    }

    /// Returns the i32 representation of the header value.
    pub fn as_int32(&self) -> Result<i32, IggyError> {
        if self.kind != HeaderKind::Int32 {
            return Err(IggyError::InvalidHeaderValue);
        }

        let value = self.value.to_vec().try_into();
        if value.is_err() {
            return Err(IggyError::InvalidHeaderValue);
        }

        Ok(i32::from_le_bytes(value.unwrap()))
    }

    /// Creates a new header value from the specified i32.
    pub fn from_int64(value: i64) -> Result<Self, IggyError> {
        Self::from(HeaderKind::Int64, &value.to_le_bytes())
    }

    /// Returns the i64 representation of the header value.
    pub fn as_int64(&self) -> Result<i64, IggyError> {
        if self.kind != HeaderKind::Int64 {
            return Err(IggyError::InvalidHeaderValue);
        }

        let value = self.value.to_vec().try_into();
        if value.is_err() {
            return Err(IggyError::InvalidHeaderValue);
        }

        Ok(i64::from_le_bytes(value.unwrap()))
    }

    /// Creates a new header value from the specified i128.
    pub fn from_int128(value: i128) -> Result<Self, IggyError> {
        Self::from(HeaderKind::Int128, &value.to_le_bytes())
    }

    /// Returns the i128 representation of the header value.
    pub fn as_int128(&self) -> Result<i128, IggyError> {
        if self.kind != HeaderKind::Int128 {
            return Err(IggyError::InvalidHeaderValue);
        }

        let value = self.value.to_vec().try_into();
        if value.is_err() {
            return Err(IggyError::InvalidHeaderValue);
        }

        Ok(i128::from_le_bytes(value.unwrap()))
    }

    /// Creates a new header value from the specified u8.
    pub fn from_uint8(value: u8) -> Result<Self, IggyError> {
        Self::from(HeaderKind::Uint8, &value.to_le_bytes())
    }

    /// Returns the u8 representation of the header value.
    pub fn as_uint8(&self) -> Result<u8, IggyError> {
        if self.kind != HeaderKind::Uint8 {
            return Err(IggyError::InvalidHeaderValue);
        }

        Ok(self.value[0])
    }

    /// Creates a new header value from the specified u16.
    pub fn from_uint16(value: u16) -> Result<Self, IggyError> {
        Self::from(HeaderKind::Uint16, &value.to_le_bytes())
    }

    /// Returns the u16 representation of the header value.
    pub fn as_uint16(&self) -> Result<u16, IggyError> {
        if self.kind != HeaderKind::Uint16 {
            return Err(IggyError::InvalidHeaderValue);
        }

        let value = self.value.to_vec().try_into();
        if value.is_err() {
            return Err(IggyError::InvalidHeaderValue);
        }

        Ok(u16::from_le_bytes(value.unwrap()))
    }

    /// Creates a new header value from the specified u32.
    pub fn from_uint32(value: u32) -> Result<Self, IggyError> {
        Self::from(HeaderKind::Uint32, &value.to_le_bytes())
    }

    /// Returns the u32 representation of the header value.
    pub fn as_uint32(&self) -> Result<u32, IggyError> {
        if self.kind != HeaderKind::Uint32 {
            return Err(IggyError::InvalidHeaderValue);
        }

        let value = self.value.to_vec().try_into();
        if value.is_err() {
            return Err(IggyError::InvalidHeaderValue);
        }

        Ok(u32::from_le_bytes(value.unwrap()))
    }

    /// Creates a new header value from the specified u64.
    pub fn from_uint64(value: u64) -> Result<Self, IggyError> {
        Self::from(HeaderKind::Uint64, &value.to_le_bytes())
    }

    /// Returns the u64 representation of the header value.
    pub fn as_uint64(&self) -> Result<u64, IggyError> {
        if self.kind != HeaderKind::Uint64 {
            return Err(IggyError::InvalidHeaderValue);
        }

        let value = self.value.to_vec().try_into();
        if value.is_err() {
            return Err(IggyError::InvalidHeaderValue);
        }

        Ok(u64::from_le_bytes(value.unwrap()))
    }

    /// Creates a new header value from the specified u128.
    pub fn from_uint128(value: u128) -> Result<Self, IggyError> {
        Self::from(HeaderKind::Uint128, &value.to_le_bytes())
    }

    /// Returns the u128 representation of the header value.
    pub fn as_uint128(&self) -> Result<u128, IggyError> {
        if self.kind != HeaderKind::Uint128 {
            return Err(IggyError::InvalidHeaderValue);
        }

        let value = self.value.to_vec().try_into();
        if value.is_err() {
            return Err(IggyError::InvalidHeaderValue);
        }

        Ok(u128::from_le_bytes(value.unwrap()))
    }

    /// Creates a new header value from the specified f32.
    pub fn from_float32(value: f32) -> Result<Self, IggyError> {
        Self::from(HeaderKind::Float32, &value.to_le_bytes())
    }

    /// Returns the f32 representation of the header value.
    pub fn as_float32(&self) -> Result<f32, IggyError> {
        if self.kind != HeaderKind::Float32 {
            return Err(IggyError::InvalidHeaderValue);
        }

        let value = self.value.to_vec().try_into();
        if value.is_err() {
            return Err(IggyError::InvalidHeaderValue);
        }

        Ok(f32::from_le_bytes(value.unwrap()))
    }

    /// Creates a new header value from the specified f64.
    pub fn from_float64(value: f64) -> Result<Self, IggyError> {
        Self::from(HeaderKind::Float64, &value.to_le_bytes())
    }

    /// Returns the f64 representation of the header value.
    pub fn as_float64(&self) -> Result<f64, IggyError> {
        if self.kind != HeaderKind::Float64 {
            return Err(IggyError::InvalidHeaderValue);
        }

        let value = self.value.to_vec().try_into();
        if value.is_err() {
            return Err(IggyError::InvalidHeaderValue);
        }

        Ok(f64::from_le_bytes(value.unwrap()))
    }

    /// Creates a new header value from the specified kind and value.
    fn from(kind: HeaderKind, value: &[u8]) -> Result<Self, IggyError> {
        if value.is_empty() || value.len() > 255 {
            return Err(IggyError::InvalidHeaderValue);
        }

        Ok(Self {
            kind,
            value: Bytes::from(value.to_vec()),
        })
    }

    /// Returns the string representation of the header value without the kind.
    pub fn value_only_to_string(&self) -> String {
        match self.kind {
            HeaderKind::Raw => format!("{:?}", self.value),
            HeaderKind::String => format!("{}", String::from_utf8_lossy(&self.value)),
            HeaderKind::Bool => format!("{}", self.value[0] != 0),
            HeaderKind::Int8 => format!(
                "{}",
                i8::from_le_bytes(self.value.to_vec().try_into().unwrap())
            ),
            HeaderKind::Int16 => format!(
                "{}",
                i16::from_le_bytes(self.value.to_vec().try_into().unwrap())
            ),
            HeaderKind::Int32 => format!(
                "{}",
                i32::from_le_bytes(self.value.to_vec().try_into().unwrap())
            ),
            HeaderKind::Int64 => format!(
                "{}",
                i64::from_le_bytes(self.value.to_vec().try_into().unwrap())
            ),
            HeaderKind::Int128 => format!(
                "{}",
                i128::from_le_bytes(self.value.to_vec().try_into().unwrap())
            ),
            HeaderKind::Uint8 => format!(
                "{}",
                u8::from_le_bytes(self.value.to_vec().try_into().unwrap())
            ),
            HeaderKind::Uint16 => format!(
                "{}",
                u16::from_le_bytes(self.value.to_vec().try_into().unwrap())
            ),
            HeaderKind::Uint32 => format!(
                "{}",
                u32::from_le_bytes(self.value.to_vec().try_into().unwrap())
            ),
            HeaderKind::Uint64 => format!(
                "{}",
                u64::from_le_bytes(self.value.to_vec().try_into().unwrap())
            ),
            HeaderKind::Uint128 => format!(
                "{}",
                u128::from_le_bytes(self.value.to_vec().try_into().unwrap())
            ),
            HeaderKind::Float32 => format!(
                "{}",
                f32::from_le_bytes(self.value.to_vec().try_into().unwrap())
            ),
            HeaderKind::Float64 => format!(
                "{}",
                f64::from_le_bytes(self.value.to_vec().try_into().unwrap())
            ),
        }
    }
}

impl BytesSerializable for HashMap<HeaderKey, HeaderValue> {
    fn to_bytes(&self) -> Bytes {
        if self.is_empty() {
            return Bytes::new();
        }

        let mut bytes = BytesMut::new();
        for (key, value) in self {
            #[allow(clippy::cast_possible_truncation)]
            bytes.put_u32_le(key.0.len() as u32);
            bytes.put_slice(key.0.as_bytes());
            bytes.put_u8(value.kind.as_code());
            #[allow(clippy::cast_possible_truncation)]
            bytes.put_u32_le(value.value.len() as u32);
            bytes.put_slice(&value.value);
        }

        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        if bytes.is_empty() {
            return Ok(Self::new());
        }

        let mut headers = Self::new();
        let mut position = 0;
        while position < bytes.len() {
            let key_length = u32::from_le_bytes(
                bytes[position..position + 4]
                    .try_into()
                    .map_err(|_| IggyError::InvalidNumberEncoding)?,
            ) as usize;
            if key_length == 0 || key_length > 255 {
                tracing::error!("Invalid header key length: {key_length}");
                return Err(IggyError::InvalidHeaderKey);
            }
            position += 4;
            let key = String::from_utf8(bytes[position..position + key_length].to_vec());
            if key.is_err() {
                tracing::error!("Invalid header key: {}", key.unwrap_err());
                return Err(IggyError::InvalidHeaderKey);
            }
            let key = key.unwrap();
            position += key_length;
            let kind = HeaderKind::from_code(bytes[position])?;
            position += 1;
            let value_length = u32::from_le_bytes(
                bytes[position..position + 4]
                    .try_into()
                    .map_err(|_| IggyError::InvalidNumberEncoding)?,
            ) as usize;
            if value_length == 0 || value_length > 255 {
                tracing::error!("Invalid header value length: {value_length}");
                return Err(IggyError::InvalidHeaderValue);
            }
            position += 4;
            let value = bytes[position..position + value_length].to_vec();
            position += value_length;
            headers.insert(
                HeaderKey(key),
                HeaderValue {
                    kind,
                    value: Bytes::from(value),
                },
            );
        }

        Ok(headers)
    }
}

/// Returns the size in bytes of the specified headers.
pub fn get_user_headers_size(headers: &Option<HashMap<HeaderKey, HeaderValue>>) -> Option<u32> {
    let mut size = 0;
    if let Some(headers) = headers {
        for (key, value) in headers {
            size += 4 + key.as_str().len() as u32 + 1 + 4 + value.value.len() as u32;
        }
    }
    Some(size)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_key_should_be_created_for_valid_value() {
        let value = "key-1";
        let header_key = HeaderKey::new(value);
        assert!(header_key.is_ok());
        assert_eq!(header_key.unwrap().0, value);
    }

    #[test]
    fn header_key_should_not_be_created_for_empty_value() {
        let value = "";
        let header_key = HeaderKey::new(value);
        assert!(header_key.is_err());
        let error = header_key.unwrap_err();
        assert_eq!(error.as_code(), IggyError::InvalidHeaderKey.as_code());
    }

    #[test]
    fn header_key_should_not_be_created_for_too_long_value() {
        let value = "a".repeat(256);
        let header_key = HeaderKey::new(&value);
        assert!(header_key.is_err());
        let error = header_key.unwrap_err();
        assert_eq!(error.as_code(), IggyError::InvalidHeaderKey.as_code());
    }

    #[test]
    fn header_value_should_not_be_created_for_empty_value() {
        let header_value = HeaderValue::from(HeaderKind::Raw, &[]);
        assert!(header_value.is_err());
        let error = header_value.unwrap_err();
        assert_eq!(error.as_code(), IggyError::InvalidHeaderValue.as_code());
    }

    #[test]
    fn header_value_should_not_be_created_for_too_long_value() {
        let value = b"a".repeat(256);
        let header_value = HeaderValue::from(HeaderKind::Raw, &value);
        assert!(header_value.is_err());
        let error = header_value.unwrap_err();
        assert_eq!(error.as_code(), IggyError::InvalidHeaderValue.as_code());
    }

    #[test]
    fn header_value_should_be_created_from_raw_bytes() {
        let value = b"Value 1";
        let header_value = HeaderValue::from_raw(value);
        assert!(header_value.is_ok());
        assert_eq!(header_value.unwrap().value.as_ref(), value);
    }

    #[test]
    fn header_value_should_be_created_from_str() {
        let value = "Value 1";
        let header_value = HeaderValue::from_str(value);
        assert!(header_value.is_ok());
        let header_value = header_value.unwrap();
        assert_eq!(header_value.kind, HeaderKind::String);
        assert_eq!(header_value.value, value.as_bytes());
        assert_eq!(header_value.as_str().unwrap(), value);
    }

    #[test]
    fn header_value_should_be_created_from_bool() {
        let value = true;
        let header_value = HeaderValue::from_bool(value);
        assert!(header_value.is_ok());
        let header_value = header_value.unwrap();
        assert_eq!(header_value.kind, HeaderKind::Bool);
        assert_eq!(header_value.value.as_ref(), if value { [1] } else { [0] });
        assert_eq!(header_value.as_bool().unwrap(), value);
    }

    #[test]
    fn header_value_should_be_created_from_int8() {
        let value = 123;
        let header_value = HeaderValue::from_int8(value);
        assert!(header_value.is_ok());
        let header_value = header_value.unwrap();
        assert_eq!(header_value.kind, HeaderKind::Int8);
        assert_eq!(header_value.value.as_ref(), value.to_le_bytes());
        assert_eq!(header_value.as_int8().unwrap(), value);
    }

    #[test]
    fn header_value_should_be_created_from_int16() {
        let value = 12345;
        let header_value = HeaderValue::from_int16(value);
        assert!(header_value.is_ok());
        let header_value = header_value.unwrap();
        assert_eq!(header_value.kind, HeaderKind::Int16);
        assert_eq!(header_value.value.as_ref(), value.to_le_bytes());
        assert_eq!(header_value.as_int16().unwrap(), value);
    }

    #[test]
    fn header_value_should_be_created_from_int32() {
        let value = 123_456;
        let header_value = HeaderValue::from_int32(value);
        assert!(header_value.is_ok());
        let header_value = header_value.unwrap();
        assert_eq!(header_value.kind, HeaderKind::Int32);
        assert_eq!(header_value.value.as_ref(), value.to_le_bytes());
        assert_eq!(header_value.as_int32().unwrap(), value);
    }

    #[test]
    fn header_value_should_be_created_from_int64() {
        let value = 123_4567;
        let header_value = HeaderValue::from_int64(value);
        assert!(header_value.is_ok());
        let header_value = header_value.unwrap();
        assert_eq!(header_value.kind, HeaderKind::Int64);
        assert_eq!(header_value.value.as_ref(), value.to_le_bytes());
        assert_eq!(header_value.as_int64().unwrap(), value);
    }

    #[test]
    fn header_value_should_be_created_from_int128() {
        let value = 1234_5678;
        let header_value = HeaderValue::from_int128(value);
        assert!(header_value.is_ok());
        let header_value = header_value.unwrap();
        assert_eq!(header_value.kind, HeaderKind::Int128);
        assert_eq!(header_value.value.as_ref(), value.to_le_bytes());
        assert_eq!(header_value.as_int128().unwrap(), value);
    }

    #[test]
    fn header_value_should_be_created_from_uint8() {
        let value = 123;
        let header_value = HeaderValue::from_uint8(value);
        assert!(header_value.is_ok());
        let header_value = header_value.unwrap();
        assert_eq!(header_value.kind, HeaderKind::Uint8);
        assert_eq!(header_value.value.as_ref(), value.to_le_bytes());
        assert_eq!(header_value.as_uint8().unwrap(), value);
    }

    #[test]
    fn header_value_should_be_created_from_uint16() {
        let value = 12345;
        let header_value = HeaderValue::from_uint16(value);
        assert!(header_value.is_ok());
        let header_value = header_value.unwrap();
        assert_eq!(header_value.kind, HeaderKind::Uint16);
        assert_eq!(header_value.value.as_ref(), value.to_le_bytes());
        assert_eq!(header_value.as_uint16().unwrap(), value);
    }

    #[test]
    fn header_value_should_be_created_from_uint32() {
        let value = 123_456;
        let header_value = HeaderValue::from_uint32(value);
        assert!(header_value.is_ok());
        let header_value = header_value.unwrap();
        assert_eq!(header_value.kind, HeaderKind::Uint32);
        assert_eq!(header_value.value.as_ref(), value.to_le_bytes());
        assert_eq!(header_value.as_uint32().unwrap(), value);
    }

    #[test]
    fn header_value_should_be_created_from_uint64() {
        let value = 123_4567;
        let header_value = HeaderValue::from_uint64(value);
        assert!(header_value.is_ok());
        let header_value = header_value.unwrap();
        assert_eq!(header_value.kind, HeaderKind::Uint64);
        assert_eq!(header_value.value.as_ref(), value.to_le_bytes());
        assert_eq!(header_value.as_uint64().unwrap(), value);
    }

    #[test]
    fn header_value_should_be_created_from_uint128() {
        let value = 1234_5678;
        let header_value = HeaderValue::from_uint128(value);
        assert!(header_value.is_ok());
        let header_value = header_value.unwrap();
        assert_eq!(header_value.kind, HeaderKind::Uint128);
        assert_eq!(header_value.value.as_ref(), value.to_le_bytes());
        assert_eq!(header_value.as_uint128().unwrap(), value);
    }

    #[test]
    fn header_value_should_be_created_from_float32() {
        let value = 123.01;
        let header_value = HeaderValue::from_float32(value);
        assert!(header_value.is_ok());
        let header_value = header_value.unwrap();
        assert_eq!(header_value.kind, HeaderKind::Float32);
        assert_eq!(header_value.value.as_ref(), value.to_le_bytes());
        assert_eq!(header_value.as_float32().unwrap(), value);
    }

    #[test]
    fn header_value_should_be_created_from_float64() {
        let value = 1234.01234;
        let header_value = HeaderValue::from_float64(value);
        assert!(header_value.is_ok());
        let header_value = header_value.unwrap();
        assert_eq!(header_value.kind, HeaderKind::Float64);
        assert_eq!(header_value.value.as_ref(), value.to_le_bytes());
        assert_eq!(header_value.as_float64().unwrap(), value);
    }

    #[test]
    fn header_value_should_be_created_string_from_kind_and_value_str() {
        let value = "Value 1";
        let header_value = HeaderValue::from_kind_str_and_value_str("string", value);
        assert!(header_value.is_ok());
        let header_value = header_value.unwrap();
        assert_eq!(header_value.kind, HeaderKind::String);
        assert_eq!(header_value.value, value.as_bytes());
        assert_eq!(header_value.as_str().unwrap(), value);
    }

    #[test]
    fn header_value_should_be_created_float_from_kind_and_value_str() {
        let value: f64 = 1234.01234;
        let header_value = HeaderValue::from_kind_str_and_value_str("float64", &value.to_string());
        assert!(header_value.is_ok());
        let header_value = header_value.unwrap();
        assert_eq!(header_value.kind, HeaderKind::Float64);
        assert_eq!(header_value.value.as_ref(), value.to_le_bytes());
        assert_eq!(header_value.as_float64().unwrap(), value);
    }

    #[test]
    fn header_value_should_be_created_raw_from_kind_and_value_str() {
        let value = "Value 1";
        let header_value = HeaderValue::from_kind_str_and_value_str("raw", value);
        assert!(header_value.is_ok());
        let header_value = header_value.unwrap();
        assert_eq!(header_value.kind, HeaderKind::Raw);
        assert_eq!(header_value.value, value.as_bytes());
    }

    #[test]
    fn header_value_should_be_created_bool_from_kind_and_value_str() {
        let value = "true";
        let header_value = HeaderValue::from_kind_str_and_value_str("bool", value);
        assert!(header_value.is_ok());
        let header_value = header_value.unwrap();
        assert_eq!(header_value.kind, HeaderKind::Bool);
        assert_eq!(header_value.value, vec![1]);
        assert!(header_value.as_bool().unwrap());
    }

    #[test]
    fn header_value_should_be_created_int8_from_kind_and_value_str() {
        let value = "123";
        let header_value = HeaderValue::from_kind_str_and_value_str("int8", value);
        assert!(header_value.is_ok());
        let header_value = header_value.unwrap();
        assert_eq!(header_value.kind, HeaderKind::Int8);
        assert_eq!(header_value.value, vec![123]);
        assert_eq!(header_value.as_int8().unwrap(), 123);
    }

    #[test]
    fn header_value_should_be_created_int16_from_kind_and_value_str() {
        let value = "1234";
        let header_value = HeaderValue::from_kind_str_and_value_str("int16", value);
        assert!(header_value.is_ok());
        let header_value = header_value.unwrap();
        assert_eq!(header_value.kind, HeaderKind::Int16);
        assert_eq!(
            header_value.value.as_ref(),
            value.parse::<i16>().unwrap().to_le_bytes()
        );
        assert_eq!(
            header_value.as_int16().unwrap(),
            value.parse::<i16>().unwrap()
        );
    }

    #[test]
    fn header_value_should_be_created_int32_from_kind_and_value_str() {
        let value = "123456";
        let header_value = HeaderValue::from_kind_str_and_value_str("int32", value);
        assert!(header_value.is_ok());
        let header_value = header_value.unwrap();
        assert_eq!(header_value.kind, HeaderKind::Int32);
        assert_eq!(
            header_value.value.as_ref(),
            value.parse::<i32>().unwrap().to_le_bytes()
        );
        assert_eq!(
            header_value.as_int32().unwrap(),
            value.parse::<i32>().unwrap()
        );
    }

    #[test]
    fn header_value_should_be_created_int64_from_kind_and_value_str() {
        let value = "123456789";
        let header_value = HeaderValue::from_kind_str_and_value_str("int64", value);
        assert!(header_value.is_ok());
        let header_value = header_value.unwrap();
        assert_eq!(header_value.kind, HeaderKind::Int64);
        assert_eq!(
            header_value.value.as_ref(),
            value.parse::<i64>().unwrap().to_le_bytes()
        );
        assert_eq!(
            header_value.as_int64().unwrap(),
            value.parse::<i64>().unwrap()
        );
    }

    #[test]
    fn header_value_should_be_created_int128_from_kind_and_value_str() {
        let value = "123456789123456789";
        let header_value = HeaderValue::from_kind_str_and_value_str("int128", value);
        assert!(header_value.is_ok());
        let header_value = header_value.unwrap();
        assert_eq!(header_value.kind, HeaderKind::Int128);
        assert_eq!(
            header_value.value.as_ref(),
            value.parse::<i128>().unwrap().to_le_bytes()
        );
        assert_eq!(
            header_value.as_int128().unwrap(),
            value.parse::<i128>().unwrap()
        );
    }

    #[test]
    fn header_value_should_be_created_uint8_from_kind_and_value_str() {
        let value = "123";
        let header_value = HeaderValue::from_kind_str_and_value_str("uint8", value);
        assert!(header_value.is_ok());
        let header_value = header_value.unwrap();
        assert_eq!(header_value.kind, HeaderKind::Uint8);
        assert_eq!(header_value.value, vec![123]);
        assert_eq!(header_value.as_uint8().unwrap(), 123);
    }

    #[test]
    fn header_value_should_be_created_uint16_from_kind_and_value_str() {
        let value = "12345";
        let header_value = HeaderValue::from_kind_str_and_value_str("uint16", value);
        assert!(header_value.is_ok());
        let header_value = header_value.unwrap();
        assert_eq!(header_value.kind, HeaderKind::Uint16);
        assert_eq!(
            header_value.value.as_ref(),
            value.parse::<u16>().unwrap().to_le_bytes()
        );
        assert_eq!(
            header_value.as_uint16().unwrap(),
            value.parse::<u16>().unwrap()
        );
    }

    #[test]
    fn header_value_should_be_created_uint32_from_kind_and_value_str() {
        let value = "123456";
        let header_value = HeaderValue::from_kind_str_and_value_str("uint32", value);
        assert!(header_value.is_ok());
        let header_value = header_value.unwrap();
        assert_eq!(header_value.kind, HeaderKind::Uint32);
        assert_eq!(
            header_value.value.as_ref(),
            value.parse::<u32>().unwrap().to_le_bytes()
        );
        assert_eq!(
            header_value.as_uint32().unwrap(),
            value.parse::<u32>().unwrap()
        );
    }

    #[test]
    fn header_value_should_be_created_uint64_from_kind_and_value_str() {
        let value = "123456789";
        let header_value = HeaderValue::from_kind_str_and_value_str("uint64", value);
        assert!(header_value.is_ok());
        let header_value = header_value.unwrap();
        assert_eq!(header_value.kind, HeaderKind::Uint64);
        assert_eq!(
            header_value.value.as_ref(),
            value.parse::<u64>().unwrap().to_le_bytes()
        );
        assert_eq!(
            header_value.as_uint64().unwrap(),
            value.parse::<u64>().unwrap()
        );
    }

    #[test]
    fn header_value_should_be_created_uint128_from_kind_and_value_str() {
        let value = "123456789123456789";
        let header_value = HeaderValue::from_kind_str_and_value_str("uint128", value);
        assert!(header_value.is_ok());
        let header_value = header_value.unwrap();
        assert_eq!(header_value.kind, HeaderKind::Uint128);
        assert_eq!(
            header_value.value.as_ref(),
            value.parse::<u128>().unwrap().to_le_bytes()
        );
        assert_eq!(
            header_value.as_uint128().unwrap(),
            value.parse::<u128>().unwrap()
        );
    }

    #[test]
    fn header_value_should_be_created_float32_from_kind_and_value_str() {
        let value = "123.01";
        let header_value = HeaderValue::from_kind_str_and_value_str("float32", value);
        assert!(header_value.is_ok());
        let header_value = header_value.unwrap();
        assert_eq!(header_value.kind, HeaderKind::Float32);
        assert_eq!(
            header_value.value.as_ref(),
            value.parse::<f32>().unwrap().to_le_bytes()
        );
        assert_eq!(
            header_value.as_float32().unwrap(),
            value.parse::<f32>().unwrap()
        );
    }

    #[test]
    fn header_value_should_be_created_float64_from_kind_and_value_str() {
        let value = "1234.01234";
        let header_value = HeaderValue::from_kind_str_and_value_str("float64", value);
        assert!(header_value.is_ok());
        let header_value = header_value.unwrap();
        assert_eq!(header_value.kind, HeaderKind::Float64);
        assert_eq!(
            header_value.value.as_ref(),
            value.parse::<f64>().unwrap().to_le_bytes()
        );
        assert_eq!(
            header_value.as_float64().unwrap(),
            value.parse::<f64>().unwrap()
        );
    }

    #[test]
    fn value_only_to_string_for_string_kind() {
        let header_value = HeaderValue::from_str("Hello").unwrap();
        assert_eq!(header_value.value_only_to_string(), "Hello");
    }

    #[test]
    fn value_only_to_string_for_bool_kind() {
        let header_value = HeaderValue::from_bool(true).unwrap();
        assert_eq!(header_value.value_only_to_string(), "true");
    }

    #[test]
    fn value_only_to_string_for_int8_kind() {
        let header_value = HeaderValue::from_int8(123).unwrap();
        assert_eq!(header_value.value_only_to_string(), "123");
    }

    #[test]
    fn value_only_to_string_for_int16_kind() {
        let header_value = HeaderValue::from_int16(12345).unwrap();
        assert_eq!(header_value.value_only_to_string(), "12345");
    }

    #[test]
    fn value_only_to_string_for_int32_kind() {
        let header_value = HeaderValue::from_int32(123456).unwrap();
        assert_eq!(header_value.value_only_to_string(), "123456");
    }

    #[test]
    fn value_only_to_string_for_int64_kind() {
        let header_value = HeaderValue::from_int64(123456789).unwrap();
        assert_eq!(header_value.value_only_to_string(), "123456789");
    }

    #[test]
    fn value_only_to_string_for_int128_kind() {
        let header_value = HeaderValue::from_int128(123456789123456789).unwrap();
        assert_eq!(header_value.value_only_to_string(), "123456789123456789");
    }

    #[test]
    fn value_only_to_string_for_uint8_kind() {
        let header_value = HeaderValue::from_uint8(123).unwrap();
        assert_eq!(header_value.value_only_to_string(), "123");
    }

    #[test]
    fn value_only_to_string_for_uint16_kind() {
        let header_value = HeaderValue::from_uint16(12345).unwrap();
        assert_eq!(header_value.value_only_to_string(), "12345");
    }

    #[test]
    fn value_only_to_string_for_uint32_kind() {
        let header_value = HeaderValue::from_uint32(123456).unwrap();
        assert_eq!(header_value.value_only_to_string(), "123456");
    }

    #[test]
    fn value_only_to_string_for_uint64_kind() {
        let header_value = HeaderValue::from_uint64(123456789).unwrap();
        assert_eq!(header_value.value_only_to_string(), "123456789");
    }

    #[test]
    fn value_only_to_string_for_uint128_kind() {
        let header_value = HeaderValue::from_uint128(123456789123456789).unwrap();
        assert_eq!(header_value.value_only_to_string(), "123456789123456789");
    }

    #[test]
    fn value_only_to_string_for_float32_kind() {
        let header_value = HeaderValue::from_float32(123.01).unwrap();
        assert_eq!(header_value.value_only_to_string(), "123.01");
    }

    #[test]
    fn value_only_to_string_for_float64_kind() {
        let header_value = HeaderValue::from_float64(1234.01234).unwrap();
        assert_eq!(header_value.value_only_to_string(), "1234.01234");
    }

    #[test]
    fn should_be_serialized_as_bytes() {
        let mut headers = HashMap::new();
        headers.insert(
            HeaderKey::new("key-1").unwrap(),
            HeaderValue::from_str("Value 1").unwrap(),
        );
        headers.insert(
            HeaderKey::new("key 1").unwrap(),
            HeaderValue::from_uint64(12345).unwrap(),
        );
        headers.insert(
            HeaderKey::new("key_3").unwrap(),
            HeaderValue::from_bool(true).unwrap(),
        );

        let bytes = headers.to_bytes();

        let mut position = 0;
        let mut headers_count = 0;
        while position < bytes.len() {
            let key_length =
                u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap()) as usize;
            position += 4;
            let key = String::from_utf8(bytes[position..position + key_length].to_vec()).unwrap();
            position += key_length;
            let kind = HeaderKind::from_code(bytes[position]).unwrap();
            position += 1;
            let value_length =
                u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap()) as usize;
            position += 4;
            let value = bytes[position..position + value_length].to_vec();
            position += value_length;
            let header = headers.get(&HeaderKey::new(&key).unwrap());
            assert!(header.is_some());
            let header = header.unwrap();
            assert_eq!(header.kind, kind);
            assert_eq!(header.value, value);
            headers_count += 1;
        }

        assert_eq!(headers_count, headers.len());
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let mut headers = HashMap::new();
        headers.insert(
            HeaderKey::new("key-1").unwrap(),
            HeaderValue::from_str("Value 1").unwrap(),
        );
        headers.insert(
            HeaderKey::new("key 2").unwrap(),
            HeaderValue::from_uint64(12345).unwrap(),
        );
        headers.insert(
            HeaderKey::new("key_3").unwrap(),
            HeaderValue::from_bool(true).unwrap(),
        );

        let mut bytes = BytesMut::new();
        for (key, value) in &headers {
            bytes.put_u32_le(key.0.len() as u32);
            bytes.put_slice(key.0.as_bytes());
            bytes.put_u8(value.kind.as_code());
            bytes.put_u32_le(value.value.len() as u32);
            bytes.put_slice(&value.value);
        }

        let deserialized_headers = HashMap::<HeaderKey, HeaderValue>::from_bytes(bytes.freeze());

        assert!(deserialized_headers.is_ok());
        let deserialized_headers = deserialized_headers.unwrap();
        assert_eq!(deserialized_headers.len(), headers.len());

        for (key, value) in &headers {
            let deserialized_value = deserialized_headers.get(key);
            assert!(deserialized_value.is_some());
            let deserialized_value = deserialized_value.unwrap();
            assert_eq!(deserialized_value.kind, value.kind);
            assert_eq!(deserialized_value.value, value.value);
        }
    }
}
