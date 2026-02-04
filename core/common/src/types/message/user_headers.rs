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
use std::marker::PhantomData;
use std::str::FromStr;

/// Type alias for header keys in user-defined message headers.
///
/// Header keys can be created from various types using `From`/`TryFrom` traits:
/// - Fixed-size types (infallible): `bool`, `i8`-`i128`, `u8`-`u128`, `f32`, `f64`
/// - Variable-size types (fallible, max 255 bytes): `&str`, `String`, `&[u8]`, `Vec<u8>`
///
/// Values can be extracted back using `TryFrom` or `as_*` methods.
///
/// # Examples
///
/// ```
/// use iggy_common::{HeaderKey, HeaderValue, IggyError};
///
/// // Create from string (most common)
/// let key = HeaderKey::try_from("content-type")?;
///
/// // Create from integer (for numeric keys)
/// let key: HeaderKey = 42u32.into();
///
/// // Extract value back
/// let num: u32 = key.try_into()?;
/// assert_eq!(num, 42);
/// # Ok::<(), IggyError>(())
/// ```
pub type HeaderKey = HeaderField<KeyMarker>;

/// Type alias for header values in user-defined message headers.
///
/// Header values can be created from various types using `From`/`TryFrom` traits:
/// - Fixed-size types (infallible): `bool`, `i8`-`i128`, `u8`-`u128`, `f32`, `f64`
/// - Variable-size types (fallible, max 255 bytes): `&str`, `String`, `&[u8]`, `Vec<u8>`
///
/// Values can be extracted back using `TryFrom` or `as_*` methods.
///
/// # Examples
///
/// ```
/// use iggy_common::{HeaderKey, HeaderValue, IggyError};
///
/// // Create from various types
/// let str_val = HeaderValue::try_from("text/plain")?;
/// let int_val: HeaderValue = 42i32.into();
/// let bool_val: HeaderValue = true.into();
/// let float_val: HeaderValue = 3.14f64.into();
///
/// // Extract values back using TryFrom
/// let num: i32 = int_val.try_into()?;
/// assert_eq!(num, 42);
///
/// // Or use as_* methods
/// let str_val = HeaderValue::try_from("hello")?;
/// assert_eq!(str_val.as_str()?, "hello");
/// # Ok::<(), IggyError>(())
/// ```
pub type HeaderValue = HeaderField<ValueMarker>;

/// Type alias for a collection of user-defined message headers.
pub type UserHeaders = HashMap<HeaderKey, HeaderValue>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct KeyMarker;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ValueMarker;

/// A typed header field that can be used as either a key or value in message headers.
///
/// `HeaderField` is a generic struct parameterized by a marker type (`KeyMarker` or `ValueMarker`)
/// to distinguish between keys and values at the type level. Use the type aliases
/// [`HeaderKey`] and [`HeaderValue`] instead of using this struct directly.
///
/// # Creating Header Fields
///
/// Use `From` trait for fixed-size types (infallible):
/// ```
/// use iggy_common::HeaderValue;
///
/// let val: HeaderValue = 42i32.into();
/// let val: HeaderValue = true.into();
/// let val: HeaderValue = 3.14f64.into();
/// ```
///
/// Use `TryFrom` trait for variable-size types (fallible, max 255 bytes):
/// ```
/// use iggy_common::{HeaderValue, IggyError};
///
/// let val = HeaderValue::try_from("hello")?;
/// let val = HeaderValue::try_from(vec![1u8, 2, 3])?;
/// # Ok::<(), IggyError>(())
/// ```
///
/// # Extracting Values
///
/// Use `TryFrom` to extract values (returns error if kind doesn't match):
/// ```
/// use iggy_common::{HeaderValue, IggyError};
///
/// let val: HeaderValue = 42i32.into();
/// let num: i32 = val.try_into()?;
///
/// // Using reference to avoid consuming the value
/// let val: HeaderValue = 100u64.into();
/// let num: u64 = (&val).try_into()?;
/// // val is still usable here
/// # Ok::<(), IggyError>(())
/// ```
///
/// Or use `as_*` methods:
/// ```
/// use iggy_common::{HeaderValue, IggyError};
///
/// let val: HeaderValue = 42i32.into();
/// assert_eq!(val.as_int32()?, 42);
/// # Ok::<(), IggyError>(())
/// ```
#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct HeaderField<T> {
    kind: HeaderKind,
    #[serde_as(as = "Base64")]
    value: Bytes,
    #[serde(skip)]
    _marker: PhantomData<T>,
}

impl<T> HeaderField<T> {
    /// Returns the kind of this header field.
    pub fn kind(&self) -> HeaderKind {
        self.kind
    }

    /// Returns a clone of the raw bytes value.
    pub fn value(&self) -> Bytes {
        self.value.clone()
    }

    /// Returns a reference to the raw bytes value.
    pub fn as_bytes(&self) -> &[u8] {
        &self.value
    }
}

/// Indicates the type of value stored in a [`HeaderField`].
///
/// This enum is used to track what type of data is stored in the header's raw bytes,
/// enabling proper deserialization and type checking when extracting values.
///
/// # Supported Types
///
/// | Kind | Rust Type | Size (bytes) |
/// |------|-----------|--------------|
/// | `Raw` | `&[u8]` / `Vec<u8>` | 1-255 |
/// | `String` | `&str` / `String` | 1-255 |
/// | `Bool` | `bool` | 1 |
/// | `Int8` | `i8` | 1 |
/// | `Int16` | `i16` | 2 |
/// | `Int32` | `i32` | 4 |
/// | `Int64` | `i64` | 8 |
/// | `Int128` | `i128` | 16 |
/// | `Uint8` | `u8` | 1 |
/// | `Uint16` | `u16` | 2 |
/// | `Uint32` | `u32` | 4 |
/// | `Uint64` | `u64` | 8 |
/// | `Uint128` | `u128` | 16 |
/// | `Float32` | `f32` | 4 |
/// | `Float64` | `f64` | 8 |
#[derive(Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum HeaderKind {
    /// Raw binary data.
    Raw,
    /// UTF-8 encoded string.
    String,
    /// Boolean value.
    Bool,
    /// Signed 8-bit integer.
    Int8,
    /// Signed 16-bit integer.
    Int16,
    /// Signed 32-bit integer.
    Int32,
    /// Signed 64-bit integer.
    Int64,
    /// Signed 128-bit integer.
    Int128,
    /// Unsigned 8-bit integer.
    Uint8,
    /// Unsigned 16-bit integer.
    Uint16,
    /// Unsigned 32-bit integer.
    Uint32,
    /// Unsigned 64-bit integer.
    Uint64,
    /// Unsigned 128-bit integer.
    Uint128,
    /// 32-bit floating point number.
    Float32,
    /// 64-bit floating point number.
    Float64,
}

impl HeaderKind {
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

    fn expected_size(&self) -> Option<usize> {
        match self {
            HeaderKind::Raw | HeaderKind::String => None,
            HeaderKind::Bool | HeaderKind::Int8 | HeaderKind::Uint8 => Some(1),
            HeaderKind::Int16 | HeaderKind::Uint16 => Some(2),
            HeaderKind::Int32 | HeaderKind::Uint32 | HeaderKind::Float32 => Some(4),
            HeaderKind::Int64 | HeaderKind::Uint64 | HeaderKind::Float64 => Some(8),
            HeaderKind::Int128 | HeaderKind::Uint128 => Some(16),
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

impl<T> Display for HeaderField<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: ", self.kind)?;
        write!(f, "{}", self.to_string_value())
    }
}

impl<T> Hash for HeaderField<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.kind.hash(state);
        self.value.hash(state);
    }
}

impl<T> FromStr for HeaderField<T> {
    type Err = IggyError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::try_from(s)
    }
}

impl<T> HeaderField<T> {
    pub fn as_raw(&self) -> Result<&[u8], IggyError> {
        if self.kind != HeaderKind::Raw {
            return Err(IggyError::InvalidHeaderValue);
        }
        Ok(&self.value)
    }

    pub fn as_str(&self) -> Result<&str, IggyError> {
        if self.kind != HeaderKind::String {
            return Err(IggyError::InvalidHeaderValue);
        }
        std::str::from_utf8(&self.value).map_err(|_| IggyError::InvalidUtf8)
    }

    pub fn as_bool(&self) -> Result<bool, IggyError> {
        if self.kind != HeaderKind::Bool {
            return Err(IggyError::InvalidHeaderValue);
        }
        let bytes: [u8; 1] = self
            .value
            .as_ref()
            .try_into()
            .map_err(|_| IggyError::InvalidHeaderValue)?;
        match bytes[0] {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(IggyError::InvalidHeaderValue),
        }
    }

    pub fn as_int8(&self) -> Result<i8, IggyError> {
        if self.kind != HeaderKind::Int8 {
            return Err(IggyError::InvalidHeaderValue);
        }
        self.value
            .as_ref()
            .try_into()
            .map(i8::from_le_bytes)
            .map_err(|_| IggyError::InvalidHeaderValue)
    }

    pub fn as_int16(&self) -> Result<i16, IggyError> {
        if self.kind != HeaderKind::Int16 {
            return Err(IggyError::InvalidHeaderValue);
        }
        self.value
            .as_ref()
            .try_into()
            .map(i16::from_le_bytes)
            .map_err(|_| IggyError::InvalidHeaderValue)
    }

    pub fn as_int32(&self) -> Result<i32, IggyError> {
        if self.kind != HeaderKind::Int32 {
            return Err(IggyError::InvalidHeaderValue);
        }
        self.value
            .as_ref()
            .try_into()
            .map(i32::from_le_bytes)
            .map_err(|_| IggyError::InvalidHeaderValue)
    }

    pub fn as_int64(&self) -> Result<i64, IggyError> {
        if self.kind != HeaderKind::Int64 {
            return Err(IggyError::InvalidHeaderValue);
        }
        self.value
            .as_ref()
            .try_into()
            .map(i64::from_le_bytes)
            .map_err(|_| IggyError::InvalidHeaderValue)
    }

    pub fn as_int128(&self) -> Result<i128, IggyError> {
        if self.kind != HeaderKind::Int128 {
            return Err(IggyError::InvalidHeaderValue);
        }
        self.value
            .as_ref()
            .try_into()
            .map(i128::from_le_bytes)
            .map_err(|_| IggyError::InvalidHeaderValue)
    }

    pub fn as_uint8(&self) -> Result<u8, IggyError> {
        if self.kind != HeaderKind::Uint8 {
            return Err(IggyError::InvalidHeaderValue);
        }
        self.value
            .as_ref()
            .try_into()
            .map(u8::from_le_bytes)
            .map_err(|_| IggyError::InvalidHeaderValue)
    }

    pub fn as_uint16(&self) -> Result<u16, IggyError> {
        if self.kind != HeaderKind::Uint16 {
            return Err(IggyError::InvalidHeaderValue);
        }
        self.value
            .as_ref()
            .try_into()
            .map(u16::from_le_bytes)
            .map_err(|_| IggyError::InvalidHeaderValue)
    }

    pub fn as_uint32(&self) -> Result<u32, IggyError> {
        if self.kind != HeaderKind::Uint32 {
            return Err(IggyError::InvalidHeaderValue);
        }
        self.value
            .as_ref()
            .try_into()
            .map(u32::from_le_bytes)
            .map_err(|_| IggyError::InvalidHeaderValue)
    }

    pub fn as_uint64(&self) -> Result<u64, IggyError> {
        if self.kind != HeaderKind::Uint64 {
            return Err(IggyError::InvalidHeaderValue);
        }
        self.value
            .as_ref()
            .try_into()
            .map(u64::from_le_bytes)
            .map_err(|_| IggyError::InvalidHeaderValue)
    }

    pub fn as_uint128(&self) -> Result<u128, IggyError> {
        if self.kind != HeaderKind::Uint128 {
            return Err(IggyError::InvalidHeaderValue);
        }
        self.value
            .as_ref()
            .try_into()
            .map(u128::from_le_bytes)
            .map_err(|_| IggyError::InvalidHeaderValue)
    }

    pub fn as_float32(&self) -> Result<f32, IggyError> {
        if self.kind != HeaderKind::Float32 {
            return Err(IggyError::InvalidHeaderValue);
        }
        self.value
            .as_ref()
            .try_into()
            .map(f32::from_le_bytes)
            .map_err(|_| IggyError::InvalidHeaderValue)
    }

    pub fn as_float64(&self) -> Result<f64, IggyError> {
        if self.kind != HeaderKind::Float64 {
            return Err(IggyError::InvalidHeaderValue);
        }
        self.value
            .as_ref()
            .try_into()
            .map(f64::from_le_bytes)
            .map_err(|_| IggyError::InvalidHeaderValue)
    }

    pub fn to_string_value(&self) -> String {
        match self.kind {
            HeaderKind::Raw => format!("{:?}", self.value),
            HeaderKind::String => String::from_utf8_lossy(&self.value).to_string(),
            HeaderKind::Bool => {
                if self.value.is_empty() {
                    "<malformed bool>".to_string()
                } else {
                    format!("{}", self.value[0] != 0)
                }
            }
            HeaderKind::Int8 => self
                .value
                .as_ref()
                .try_into()
                .map(|b| i8::from_le_bytes(b).to_string())
                .unwrap_or_else(|_| "<malformed int8>".to_string()),
            HeaderKind::Int16 => self
                .value
                .as_ref()
                .try_into()
                .map(|b| i16::from_le_bytes(b).to_string())
                .unwrap_or_else(|_| "<malformed int16>".to_string()),
            HeaderKind::Int32 => self
                .value
                .as_ref()
                .try_into()
                .map(|b| i32::from_le_bytes(b).to_string())
                .unwrap_or_else(|_| "<malformed int32>".to_string()),
            HeaderKind::Int64 => self
                .value
                .as_ref()
                .try_into()
                .map(|b| i64::from_le_bytes(b).to_string())
                .unwrap_or_else(|_| "<malformed int64>".to_string()),
            HeaderKind::Int128 => self
                .value
                .as_ref()
                .try_into()
                .map(|b| i128::from_le_bytes(b).to_string())
                .unwrap_or_else(|_| "<malformed int128>".to_string()),
            HeaderKind::Uint8 => self
                .value
                .as_ref()
                .try_into()
                .map(|b| u8::from_le_bytes(b).to_string())
                .unwrap_or_else(|_| "<malformed uint8>".to_string()),
            HeaderKind::Uint16 => self
                .value
                .as_ref()
                .try_into()
                .map(|b| u16::from_le_bytes(b).to_string())
                .unwrap_or_else(|_| "<malformed uint16>".to_string()),
            HeaderKind::Uint32 => self
                .value
                .as_ref()
                .try_into()
                .map(|b| u32::from_le_bytes(b).to_string())
                .unwrap_or_else(|_| "<malformed uint32>".to_string()),
            HeaderKind::Uint64 => self
                .value
                .as_ref()
                .try_into()
                .map(|b| u64::from_le_bytes(b).to_string())
                .unwrap_or_else(|_| "<malformed uint64>".to_string()),
            HeaderKind::Uint128 => self
                .value
                .as_ref()
                .try_into()
                .map(|b| u128::from_le_bytes(b).to_string())
                .unwrap_or_else(|_| "<malformed uint128>".to_string()),
            HeaderKind::Float32 => self
                .value
                .as_ref()
                .try_into()
                .map(|b| f32::from_le_bytes(b).to_string())
                .unwrap_or_else(|_| "<malformed float32>".to_string()),
            HeaderKind::Float64 => self
                .value
                .as_ref()
                .try_into()
                .map(|b| f64::from_le_bytes(b).to_string())
                .unwrap_or_else(|_| "<malformed float64>".to_string()),
        }
    }

    fn new_unchecked(kind: HeaderKind, value: &[u8]) -> Self {
        Self {
            kind,
            value: Bytes::from(value.to_vec()),
            _marker: PhantomData,
        }
    }
}

impl<T> TryFrom<&str> for HeaderField<T> {
    type Error = IggyError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        if value.is_empty() || value.len() > 255 {
            return Err(IggyError::InvalidHeaderValue);
        }
        Ok(Self::new_unchecked(HeaderKind::String, value.as_bytes()))
    }
}

impl<T> TryFrom<String> for HeaderField<T> {
    type Error = IggyError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::try_from(value.as_str())
    }
}

impl<T> TryFrom<&[u8]> for HeaderField<T> {
    type Error = IggyError;
    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        if value.is_empty() || value.len() > 255 {
            return Err(IggyError::InvalidHeaderValue);
        }
        Ok(Self::new_unchecked(HeaderKind::Raw, value))
    }
}

impl<T> TryFrom<Vec<u8>> for HeaderField<T> {
    type Error = IggyError;
    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        Self::try_from(value.as_slice())
    }
}

impl<T> From<bool> for HeaderField<T> {
    fn from(value: bool) -> Self {
        Self::new_unchecked(HeaderKind::Bool, if value { &[1] } else { &[0] })
    }
}

impl<T> From<i8> for HeaderField<T> {
    fn from(value: i8) -> Self {
        Self::new_unchecked(HeaderKind::Int8, &value.to_le_bytes())
    }
}

impl<T> From<i16> for HeaderField<T> {
    fn from(value: i16) -> Self {
        Self::new_unchecked(HeaderKind::Int16, &value.to_le_bytes())
    }
}

impl<T> From<i32> for HeaderField<T> {
    fn from(value: i32) -> Self {
        Self::new_unchecked(HeaderKind::Int32, &value.to_le_bytes())
    }
}

impl<T> From<i64> for HeaderField<T> {
    fn from(value: i64) -> Self {
        Self::new_unchecked(HeaderKind::Int64, &value.to_le_bytes())
    }
}

impl<T> From<i128> for HeaderField<T> {
    fn from(value: i128) -> Self {
        Self::new_unchecked(HeaderKind::Int128, &value.to_le_bytes())
    }
}

impl<T> From<u8> for HeaderField<T> {
    fn from(value: u8) -> Self {
        Self::new_unchecked(HeaderKind::Uint8, &value.to_le_bytes())
    }
}

impl<T> From<u16> for HeaderField<T> {
    fn from(value: u16) -> Self {
        Self::new_unchecked(HeaderKind::Uint16, &value.to_le_bytes())
    }
}

impl<T> From<u32> for HeaderField<T> {
    fn from(value: u32) -> Self {
        Self::new_unchecked(HeaderKind::Uint32, &value.to_le_bytes())
    }
}

impl<T> From<u64> for HeaderField<T> {
    fn from(value: u64) -> Self {
        Self::new_unchecked(HeaderKind::Uint64, &value.to_le_bytes())
    }
}

impl<T> From<u128> for HeaderField<T> {
    fn from(value: u128) -> Self {
        Self::new_unchecked(HeaderKind::Uint128, &value.to_le_bytes())
    }
}

impl<T> From<f32> for HeaderField<T> {
    fn from(value: f32) -> Self {
        Self::new_unchecked(HeaderKind::Float32, &value.to_le_bytes())
    }
}

impl<T> From<f64> for HeaderField<T> {
    fn from(value: f64) -> Self {
        Self::new_unchecked(HeaderKind::Float64, &value.to_le_bytes())
    }
}

impl<T> TryFrom<HeaderField<T>> for bool {
    type Error = IggyError;
    fn try_from(field: HeaderField<T>) -> Result<Self, Self::Error> {
        field.as_bool()
    }
}

impl<T> TryFrom<&HeaderField<T>> for bool {
    type Error = IggyError;
    fn try_from(field: &HeaderField<T>) -> Result<Self, Self::Error> {
        field.as_bool()
    }
}

impl<T> TryFrom<HeaderField<T>> for i8 {
    type Error = IggyError;
    fn try_from(field: HeaderField<T>) -> Result<Self, Self::Error> {
        field.as_int8()
    }
}

impl<T> TryFrom<&HeaderField<T>> for i8 {
    type Error = IggyError;
    fn try_from(field: &HeaderField<T>) -> Result<Self, Self::Error> {
        field.as_int8()
    }
}

impl<T> TryFrom<HeaderField<T>> for i16 {
    type Error = IggyError;
    fn try_from(field: HeaderField<T>) -> Result<Self, Self::Error> {
        field.as_int16()
    }
}

impl<T> TryFrom<&HeaderField<T>> for i16 {
    type Error = IggyError;
    fn try_from(field: &HeaderField<T>) -> Result<Self, Self::Error> {
        field.as_int16()
    }
}

impl<T> TryFrom<HeaderField<T>> for i32 {
    type Error = IggyError;
    fn try_from(field: HeaderField<T>) -> Result<Self, Self::Error> {
        field.as_int32()
    }
}

impl<T> TryFrom<&HeaderField<T>> for i32 {
    type Error = IggyError;
    fn try_from(field: &HeaderField<T>) -> Result<Self, Self::Error> {
        field.as_int32()
    }
}

impl<T> TryFrom<HeaderField<T>> for i64 {
    type Error = IggyError;
    fn try_from(field: HeaderField<T>) -> Result<Self, Self::Error> {
        field.as_int64()
    }
}

impl<T> TryFrom<&HeaderField<T>> for i64 {
    type Error = IggyError;
    fn try_from(field: &HeaderField<T>) -> Result<Self, Self::Error> {
        field.as_int64()
    }
}

impl<T> TryFrom<HeaderField<T>> for i128 {
    type Error = IggyError;
    fn try_from(field: HeaderField<T>) -> Result<Self, Self::Error> {
        field.as_int128()
    }
}

impl<T> TryFrom<&HeaderField<T>> for i128 {
    type Error = IggyError;
    fn try_from(field: &HeaderField<T>) -> Result<Self, Self::Error> {
        field.as_int128()
    }
}

impl<T> TryFrom<HeaderField<T>> for u8 {
    type Error = IggyError;
    fn try_from(field: HeaderField<T>) -> Result<Self, Self::Error> {
        field.as_uint8()
    }
}

impl<T> TryFrom<&HeaderField<T>> for u8 {
    type Error = IggyError;
    fn try_from(field: &HeaderField<T>) -> Result<Self, Self::Error> {
        field.as_uint8()
    }
}

impl<T> TryFrom<HeaderField<T>> for u16 {
    type Error = IggyError;
    fn try_from(field: HeaderField<T>) -> Result<Self, Self::Error> {
        field.as_uint16()
    }
}

impl<T> TryFrom<&HeaderField<T>> for u16 {
    type Error = IggyError;
    fn try_from(field: &HeaderField<T>) -> Result<Self, Self::Error> {
        field.as_uint16()
    }
}

impl<T> TryFrom<HeaderField<T>> for u32 {
    type Error = IggyError;
    fn try_from(field: HeaderField<T>) -> Result<Self, Self::Error> {
        field.as_uint32()
    }
}

impl<T> TryFrom<&HeaderField<T>> for u32 {
    type Error = IggyError;
    fn try_from(field: &HeaderField<T>) -> Result<Self, Self::Error> {
        field.as_uint32()
    }
}

impl<T> TryFrom<HeaderField<T>> for u64 {
    type Error = IggyError;
    fn try_from(field: HeaderField<T>) -> Result<Self, Self::Error> {
        field.as_uint64()
    }
}

impl<T> TryFrom<&HeaderField<T>> for u64 {
    type Error = IggyError;
    fn try_from(field: &HeaderField<T>) -> Result<Self, Self::Error> {
        field.as_uint64()
    }
}

impl<T> TryFrom<HeaderField<T>> for u128 {
    type Error = IggyError;
    fn try_from(field: HeaderField<T>) -> Result<Self, Self::Error> {
        field.as_uint128()
    }
}

impl<T> TryFrom<&HeaderField<T>> for u128 {
    type Error = IggyError;
    fn try_from(field: &HeaderField<T>) -> Result<Self, Self::Error> {
        field.as_uint128()
    }
}

impl<T> TryFrom<HeaderField<T>> for f32 {
    type Error = IggyError;
    fn try_from(field: HeaderField<T>) -> Result<Self, Self::Error> {
        field.as_float32()
    }
}

impl<T> TryFrom<&HeaderField<T>> for f32 {
    type Error = IggyError;
    fn try_from(field: &HeaderField<T>) -> Result<Self, Self::Error> {
        field.as_float32()
    }
}

impl<T> TryFrom<HeaderField<T>> for f64 {
    type Error = IggyError;
    fn try_from(field: HeaderField<T>) -> Result<Self, Self::Error> {
        field.as_float64()
    }
}

impl<T> TryFrom<&HeaderField<T>> for f64 {
    type Error = IggyError;
    fn try_from(field: &HeaderField<T>) -> Result<Self, Self::Error> {
        field.as_float64()
    }
}

impl<T> TryFrom<HeaderField<T>> for String {
    type Error = IggyError;
    fn try_from(field: HeaderField<T>) -> Result<Self, Self::Error> {
        field.as_str().map(|s| s.to_owned())
    }
}

impl<T> TryFrom<&HeaderField<T>> for String {
    type Error = IggyError;
    fn try_from(field: &HeaderField<T>) -> Result<Self, Self::Error> {
        field.as_str().map(|s| s.to_owned())
    }
}

impl<T> TryFrom<HeaderField<T>> for Vec<u8> {
    type Error = IggyError;
    fn try_from(field: HeaderField<T>) -> Result<Self, Self::Error> {
        field.as_raw().map(|s| s.to_vec())
    }
}

impl<T> TryFrom<&HeaderField<T>> for Vec<u8> {
    type Error = IggyError;
    fn try_from(field: &HeaderField<T>) -> Result<Self, Self::Error> {
        field.as_raw().map(|s| s.to_vec())
    }
}

impl BytesSerializable for HashMap<HeaderKey, HeaderValue> {
    fn to_bytes(&self) -> Bytes {
        if self.is_empty() {
            return Bytes::new();
        }

        let mut bytes = BytesMut::new();
        for (key, value) in self {
            bytes.put_u8(key.kind().as_code());
            #[allow(clippy::cast_possible_truncation)]
            bytes.put_u32_le(key.as_bytes().len() as u32);
            bytes.put_slice(key.as_bytes());
            bytes.put_u8(value.kind().as_code());
            #[allow(clippy::cast_possible_truncation)]
            bytes.put_u32_le(value.as_bytes().len() as u32);
            bytes.put_slice(value.as_bytes());
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
            let key_kind = HeaderKind::from_code(bytes[position])?;
            position += 1;

            if position + 4 > bytes.len() {
                return Err(IggyError::InvalidHeaderKey);
            }
            let key_length = u32::from_le_bytes(
                bytes[position..position + 4]
                    .try_into()
                    .map_err(|_| IggyError::InvalidNumberEncoding)?,
            ) as usize;
            if key_length == 0 || key_length > 255 {
                return Err(IggyError::InvalidHeaderKey);
            }
            position += 4;

            if position + key_length > bytes.len() {
                return Err(IggyError::InvalidHeaderKey);
            }
            if let Some(expected) = key_kind.expected_size()
                && key_length != expected
            {
                return Err(IggyError::InvalidHeaderKey);
            }
            let key_value = bytes[position..position + key_length].to_vec();
            position += key_length;

            if position >= bytes.len() {
                return Err(IggyError::InvalidHeaderValue);
            }
            let value_kind = HeaderKind::from_code(bytes[position])?;
            position += 1;

            if position + 4 > bytes.len() {
                return Err(IggyError::InvalidHeaderValue);
            }
            let value_length = u32::from_le_bytes(
                bytes[position..position + 4]
                    .try_into()
                    .map_err(|_| IggyError::InvalidNumberEncoding)?,
            ) as usize;
            if value_length == 0 || value_length > 255 {
                return Err(IggyError::InvalidHeaderValue);
            }
            position += 4;

            if position + value_length > bytes.len() {
                return Err(IggyError::InvalidHeaderValue);
            }
            if let Some(expected) = value_kind.expected_size()
                && value_length != expected
            {
                return Err(IggyError::InvalidHeaderValue);
            }
            let value_value = bytes[position..position + value_length].to_vec();
            position += value_length;

            headers.insert(
                HeaderKey::new_unchecked(key_kind, &key_value),
                HeaderValue::new_unchecked(value_kind, &value_value),
            );
        }

        Ok(headers)
    }
}

pub fn get_user_headers_size(headers: &Option<HashMap<HeaderKey, HeaderValue>>) -> Option<u32> {
    let mut size = 0;
    if let Some(headers) = headers {
        for (key, value) in headers {
            size += 1 + 4 + key.as_bytes().len() as u32 + 1 + 4 + value.as_bytes().len() as u32;
        }
    }
    Some(size)
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct HeaderEntry {
    pub key: HeaderKey,
    pub value: HeaderValue,
}

pub fn serialize_headers<S>(headers: &Option<UserHeaders>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::ser::SerializeSeq;

    match headers {
        Some(map) => {
            let mut seq = serializer.serialize_seq(Some(map.len()))?;
            for (key, value) in map {
                seq.serialize_element(&HeaderEntry {
                    key: key.clone(),
                    value: value.clone(),
                })?;
            }
            seq.end()
        }
        None => serializer.serialize_none(),
    }
}

pub fn deserialize_headers<'de, D>(deserializer: D) -> Result<Option<UserHeaders>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let entries: Option<Vec<HeaderEntry>> = Option::deserialize(deserializer)?;
    match entries {
        Some(vec) => {
            let mut map = UserHeaders::new();
            for entry in vec {
                map.insert(entry.key, entry.value);
            }
            Ok(Some(map))
        }
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_key_should_be_created_for_valid_value() {
        let value = "key-1";
        let header_key = HeaderKey::try_from(value);
        assert!(header_key.is_ok());
        let header_key = header_key.unwrap();
        assert_eq!(header_key.kind, HeaderKind::String);
        assert_eq!(header_key.as_str().unwrap(), value);
    }

    #[test]
    fn header_key_should_not_be_created_for_empty_value() {
        let value = "";
        let header_key = HeaderKey::try_from(value);
        assert!(header_key.is_err());
        let error = header_key.unwrap_err();
        assert_eq!(error.as_code(), IggyError::InvalidHeaderValue.as_code());
    }

    #[test]
    fn header_key_should_not_be_created_for_too_long_value() {
        let value = "a".repeat(256);
        let header_key = HeaderKey::try_from(value.as_str());
        assert!(header_key.is_err());
        let error = header_key.unwrap_err();
        assert_eq!(error.as_code(), IggyError::InvalidHeaderValue.as_code());
    }

    #[test]
    fn header_key_should_be_created_from_int32() {
        let value = 12345i32;
        let header_key: HeaderKey = value.into();
        assert_eq!(header_key.kind, HeaderKind::Int32);
        assert_eq!(header_key.as_int32().unwrap(), value);
    }

    #[test]
    fn header_value_should_not_be_created_for_empty_value() {
        let header_value = HeaderValue::try_from([].as_slice());
        assert!(header_value.is_err());
        let error = header_value.unwrap_err();
        assert_eq!(error.as_code(), IggyError::InvalidHeaderValue.as_code());
    }

    #[test]
    fn header_value_should_not_be_created_for_too_long_value() {
        let value = b"a".repeat(256);
        let header_value = HeaderValue::try_from(value.as_slice());
        assert!(header_value.is_err());
        let error = header_value.unwrap_err();
        assert_eq!(error.as_code(), IggyError::InvalidHeaderValue.as_code());
    }

    #[test]
    fn header_value_should_be_created_from_raw_bytes() {
        let value = b"Value 1";
        let header_value = HeaderValue::try_from(value.as_slice());
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
        let header_value: HeaderValue = value.into();
        assert_eq!(header_value.kind, HeaderKind::Bool);
        assert_eq!(header_value.value.as_ref(), if value { [1] } else { [0] });
        assert_eq!(header_value.as_bool().unwrap(), value);
    }

    #[test]
    fn header_value_should_be_created_from_int8() {
        let value: i8 = 123;
        let header_value: HeaderValue = value.into();
        assert_eq!(header_value.kind, HeaderKind::Int8);
        assert_eq!(header_value.value.as_ref(), value.to_le_bytes());
        assert_eq!(header_value.as_int8().unwrap(), value);
    }

    #[test]
    fn header_value_should_be_created_from_int16() {
        let value: i16 = 12345;
        let header_value: HeaderValue = value.into();
        assert_eq!(header_value.kind, HeaderKind::Int16);
        assert_eq!(header_value.value.as_ref(), value.to_le_bytes());
        assert_eq!(header_value.as_int16().unwrap(), value);
    }

    #[test]
    fn header_value_should_be_created_from_int32() {
        let value: i32 = 123_456;
        let header_value: HeaderValue = value.into();
        assert_eq!(header_value.kind, HeaderKind::Int32);
        assert_eq!(header_value.value.as_ref(), value.to_le_bytes());
        assert_eq!(header_value.as_int32().unwrap(), value);
    }

    #[test]
    fn header_value_should_be_created_from_int64() {
        let value: i64 = 123_4567;
        let header_value: HeaderValue = value.into();
        assert_eq!(header_value.kind, HeaderKind::Int64);
        assert_eq!(header_value.value.as_ref(), value.to_le_bytes());
        assert_eq!(header_value.as_int64().unwrap(), value);
    }

    #[test]
    fn header_value_should_be_created_from_int128() {
        let value: i128 = 1234_5678;
        let header_value: HeaderValue = value.into();
        assert_eq!(header_value.kind, HeaderKind::Int128);
        assert_eq!(header_value.value.as_ref(), value.to_le_bytes());
        assert_eq!(header_value.as_int128().unwrap(), value);
    }

    #[test]
    fn header_value_should_be_created_from_uint8() {
        let value: u8 = 123;
        let header_value: HeaderValue = value.into();
        assert_eq!(header_value.kind, HeaderKind::Uint8);
        assert_eq!(header_value.value.as_ref(), value.to_le_bytes());
        assert_eq!(header_value.as_uint8().unwrap(), value);
    }

    #[test]
    fn header_value_should_be_created_from_uint16() {
        let value: u16 = 12345;
        let header_value: HeaderValue = value.into();
        assert_eq!(header_value.kind, HeaderKind::Uint16);
        assert_eq!(header_value.value.as_ref(), value.to_le_bytes());
        assert_eq!(header_value.as_uint16().unwrap(), value);
    }

    #[test]
    fn header_value_should_be_created_from_uint32() {
        let value: u32 = 123_456;
        let header_value: HeaderValue = value.into();
        assert_eq!(header_value.kind, HeaderKind::Uint32);
        assert_eq!(header_value.value.as_ref(), value.to_le_bytes());
        assert_eq!(header_value.as_uint32().unwrap(), value);
    }

    #[test]
    fn header_value_should_be_created_from_uint64() {
        let value: u64 = 123_4567;
        let header_value: HeaderValue = value.into();
        assert_eq!(header_value.kind, HeaderKind::Uint64);
        assert_eq!(header_value.value.as_ref(), value.to_le_bytes());
        assert_eq!(header_value.as_uint64().unwrap(), value);
    }

    #[test]
    fn header_value_should_be_created_from_uint128() {
        let value: u128 = 1234_5678;
        let header_value: HeaderValue = value.into();
        assert_eq!(header_value.kind, HeaderKind::Uint128);
        assert_eq!(header_value.value.as_ref(), value.to_le_bytes());
        assert_eq!(header_value.as_uint128().unwrap(), value);
    }

    #[test]
    fn header_value_should_be_created_from_float32() {
        let value: f32 = 123.01;
        let header_value: HeaderValue = value.into();
        assert_eq!(header_value.kind, HeaderKind::Float32);
        assert_eq!(header_value.value.as_ref(), value.to_le_bytes());
        assert_eq!(header_value.as_float32().unwrap(), value);
    }

    #[test]
    fn header_value_should_be_created_from_float64() {
        let value: f64 = 1234.01234;
        let header_value: HeaderValue = value.into();
        assert_eq!(header_value.kind, HeaderKind::Float64);
        assert_eq!(header_value.value.as_ref(), value.to_le_bytes());
        assert_eq!(header_value.as_float64().unwrap(), value);
    }

    #[test]
    fn to_string_value_for_string_kind() {
        let header_value = HeaderValue::from_str("Hello").unwrap();
        assert_eq!(header_value.to_string_value(), "Hello");
    }

    #[test]
    fn to_string_value_for_bool_kind() {
        let header_value: HeaderValue = true.into();
        assert_eq!(header_value.to_string_value(), "true");
    }

    #[test]
    fn to_string_value_for_int8_kind() {
        let header_value: HeaderValue = 123i8.into();
        assert_eq!(header_value.to_string_value(), "123");
    }

    #[test]
    fn to_string_value_for_int16_kind() {
        let header_value: HeaderValue = 12345i16.into();
        assert_eq!(header_value.to_string_value(), "12345");
    }

    #[test]
    fn to_string_value_for_int32_kind() {
        let header_value: HeaderValue = 123456i32.into();
        assert_eq!(header_value.to_string_value(), "123456");
    }

    #[test]
    fn to_string_value_for_int64_kind() {
        let header_value: HeaderValue = 123456789i64.into();
        assert_eq!(header_value.to_string_value(), "123456789");
    }

    #[test]
    fn to_string_value_for_int128_kind() {
        let header_value: HeaderValue = 123456789123456789i128.into();
        assert_eq!(header_value.to_string_value(), "123456789123456789");
    }

    #[test]
    fn to_string_value_for_uint8_kind() {
        let header_value: HeaderValue = 123u8.into();
        assert_eq!(header_value.to_string_value(), "123");
    }

    #[test]
    fn to_string_value_for_uint16_kind() {
        let header_value: HeaderValue = 12345u16.into();
        assert_eq!(header_value.to_string_value(), "12345");
    }

    #[test]
    fn to_string_value_for_uint32_kind() {
        let header_value: HeaderValue = 123456u32.into();
        assert_eq!(header_value.to_string_value(), "123456");
    }

    #[test]
    fn to_string_value_for_uint64_kind() {
        let header_value: HeaderValue = 123456789u64.into();
        assert_eq!(header_value.to_string_value(), "123456789");
    }

    #[test]
    fn to_string_value_for_uint128_kind() {
        let header_value: HeaderValue = 123456789123456789u128.into();
        assert_eq!(header_value.to_string_value(), "123456789123456789");
    }

    #[test]
    fn to_string_value_for_float32_kind() {
        let header_value: HeaderValue = 123.01f32.into();
        assert_eq!(header_value.to_string_value(), "123.01");
    }

    #[test]
    fn to_string_value_for_float64_kind() {
        let header_value: HeaderValue = 1234.01234f64.into();
        assert_eq!(header_value.to_string_value(), "1234.01234");
    }

    #[test]
    fn should_be_serialized_as_bytes() {
        let mut headers = HashMap::new();
        headers.insert(
            HeaderKey::try_from("key-1").unwrap(),
            HeaderValue::from_str("Value 1").unwrap(),
        );
        headers.insert(HeaderKey::try_from("key 1").unwrap(), 12345u64.into());
        headers.insert(HeaderKey::try_from("key_3").unwrap(), true.into());

        let bytes = headers.to_bytes();

        let mut position = 0;
        let mut headers_count = 0;
        while position < bytes.len() {
            let key_kind = HeaderKind::from_code(bytes[position]).unwrap();
            position += 1;
            let key_length =
                u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap()) as usize;
            position += 4;
            let key_value = bytes[position..position + key_length].to_vec();
            position += key_length;

            let value_kind = HeaderKind::from_code(bytes[position]).unwrap();
            position += 1;
            let value_length =
                u32::from_le_bytes(bytes[position..position + 4].try_into().unwrap()) as usize;
            position += 4;
            let value = bytes[position..position + value_length].to_vec();
            position += value_length;

            let key = HeaderKey {
                kind: key_kind,
                value: Bytes::from(key_value),
                _marker: PhantomData,
            };
            let header = headers.get(&key);
            assert!(header.is_some());
            let header = header.unwrap();
            assert_eq!(header.kind, value_kind);
            assert_eq!(header.value, value);
            headers_count += 1;
        }

        assert_eq!(headers_count, headers.len());
    }

    #[test]
    fn should_be_deserialized_from_bytes() {
        let mut headers = HashMap::new();
        headers.insert(
            HeaderKey::try_from("key-1").unwrap(),
            HeaderValue::from_str("Value 1").unwrap(),
        );
        headers.insert(HeaderKey::try_from("key 2").unwrap(), 12345u64.into());
        headers.insert(HeaderKey::try_from("key_3").unwrap(), true.into());

        let mut bytes = BytesMut::new();
        for (key, value) in &headers {
            bytes.put_u8(key.kind.as_code());
            bytes.put_u32_le(key.value.len() as u32);
            bytes.put_slice(&key.value);
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

    #[test]
    fn should_serialize_and_deserialize_typed_keys() {
        let mut headers = HashMap::new();
        headers.insert(
            123i32.into(),
            HeaderValue::from_str("Value for int key").unwrap(),
        );
        headers.insert(999u64.into(), true.into());

        let bytes = headers.to_bytes();
        let deserialized = HashMap::<HeaderKey, HeaderValue>::from_bytes(bytes).unwrap();

        assert_eq!(deserialized.len(), headers.len());
        for (key, value) in &headers {
            let deserialized_value = deserialized.get(key);
            assert!(deserialized_value.is_some());
            let deserialized_value = deserialized_value.unwrap();
            assert_eq!(deserialized_value.kind, value.kind);
            assert_eq!(deserialized_value.value, value.value);
        }
    }

    #[test]
    fn header_value_should_be_created_from_vec_u8() {
        let value = vec![1u8, 2, 3, 4, 5];
        let header_value = HeaderValue::try_from(value.clone());
        assert!(header_value.is_ok());
        let header_value = header_value.unwrap();
        assert_eq!(header_value.kind, HeaderKind::Raw);
        assert_eq!(header_value.value.as_ref(), value.as_slice());
        assert_eq!(header_value.as_raw().unwrap(), value.as_slice());
    }

    #[test]
    fn header_value_should_not_be_created_from_empty_vec_u8() {
        let value: Vec<u8> = vec![];
        let header_value = HeaderValue::try_from(value);
        assert!(header_value.is_err());
        let error = header_value.unwrap_err();
        assert_eq!(error.as_code(), IggyError::InvalidHeaderValue.as_code());
    }

    #[test]
    fn header_value_should_not_be_created_from_too_long_vec_u8() {
        let value: Vec<u8> = vec![0u8; 256];
        let header_value = HeaderValue::try_from(value);
        assert!(header_value.is_err());
        let error = header_value.unwrap_err();
        assert_eq!(error.as_code(), IggyError::InvalidHeaderValue.as_code());
    }

    #[test]
    fn header_key_should_be_created_from_vec_u8() {
        let value = vec![1u8, 2, 3, 4];
        let header_key = HeaderKey::try_from(value.clone());
        assert!(header_key.is_ok());
        let header_key = header_key.unwrap();
        assert_eq!(header_key.kind, HeaderKind::Raw);
        assert_eq!(header_key.value.as_ref(), value.as_slice());
    }

    #[test]
    fn header_value_should_convert_to_bool() {
        let header_value: HeaderValue = true.into();
        let extracted: bool = header_value.try_into().unwrap();
        assert!(extracted);
    }

    #[test]
    fn header_value_should_convert_to_i8() {
        let header_value: HeaderValue = 42i8.into();
        let extracted: i8 = header_value.try_into().unwrap();
        assert_eq!(extracted, 42);
    }

    #[test]
    fn header_value_should_convert_to_i16() {
        let header_value: HeaderValue = 1234i16.into();
        let extracted: i16 = header_value.try_into().unwrap();
        assert_eq!(extracted, 1234);
    }

    #[test]
    fn header_value_should_convert_to_i32() {
        let header_value: HeaderValue = 123456i32.into();
        let extracted: i32 = header_value.try_into().unwrap();
        assert_eq!(extracted, 123456);
    }

    #[test]
    fn header_value_should_convert_to_i64() {
        let header_value: HeaderValue = 123456789i64.into();
        let extracted: i64 = header_value.try_into().unwrap();
        assert_eq!(extracted, 123456789);
    }

    #[test]
    fn header_value_should_convert_to_i128() {
        let header_value: HeaderValue = 123456789123456789i128.into();
        let extracted: i128 = header_value.try_into().unwrap();
        assert_eq!(extracted, 123456789123456789);
    }

    #[test]
    fn header_value_should_convert_to_u8() {
        let header_value: HeaderValue = 42u8.into();
        let extracted: u8 = header_value.try_into().unwrap();
        assert_eq!(extracted, 42);
    }

    #[test]
    fn header_value_should_convert_to_u16() {
        let header_value: HeaderValue = 1234u16.into();
        let extracted: u16 = header_value.try_into().unwrap();
        assert_eq!(extracted, 1234);
    }

    #[test]
    fn header_value_should_convert_to_u32() {
        let header_value: HeaderValue = 123456u32.into();
        let extracted: u32 = header_value.try_into().unwrap();
        assert_eq!(extracted, 123456);
    }

    #[test]
    fn header_value_should_convert_to_u64() {
        let header_value: HeaderValue = 123456789u64.into();
        let extracted: u64 = header_value.try_into().unwrap();
        assert_eq!(extracted, 123456789);
    }

    #[test]
    fn header_value_should_convert_to_u128() {
        let header_value: HeaderValue = 123456789123456789u128.into();
        let extracted: u128 = header_value.try_into().unwrap();
        assert_eq!(extracted, 123456789123456789);
    }

    #[test]
    fn header_value_should_convert_to_f32() {
        let header_value: HeaderValue = 123.5f32.into();
        let extracted: f32 = header_value.try_into().unwrap();
        assert_eq!(extracted, 123.5);
    }

    #[test]
    fn header_value_should_convert_to_f64() {
        let header_value: HeaderValue = 1234.5678f64.into();
        let extracted: f64 = header_value.try_into().unwrap();
        assert_eq!(extracted, 1234.5678);
    }

    #[test]
    fn header_value_should_convert_to_string() {
        let header_value = HeaderValue::try_from("hello").unwrap();
        let extracted: String = header_value.try_into().unwrap();
        assert_eq!(extracted, "hello");
    }

    #[test]
    fn header_value_should_convert_to_vec_u8() {
        let header_value = HeaderValue::try_from(vec![1u8, 2, 3]).unwrap();
        let extracted: Vec<u8> = header_value.try_into().unwrap();
        assert_eq!(extracted, vec![1u8, 2, 3]);
    }

    #[test]
    fn header_value_should_fail_conversion_with_wrong_kind() {
        let header_value: HeaderValue = 42i32.into();
        let result: Result<u64, _> = header_value.try_into();
        assert!(result.is_err());
    }

    #[test]
    fn header_value_ref_should_convert_to_i32() {
        let value = 123456i32;
        let header_value: HeaderValue = value.into();
        let extracted: i32 = (&header_value).try_into().unwrap();
        assert_eq!(extracted, value);
        assert_eq!(header_value.as_int32().unwrap(), value);
    }

    #[test]
    fn to_string_value_handles_malformed_int32() {
        let field = HeaderField::<ValueMarker> {
            kind: HeaderKind::Int32,
            value: Bytes::from(vec![1, 2, 3]), // 3 bytes, needs 4
            _marker: PhantomData,
        };
        assert_eq!(field.to_string_value(), "<malformed int32>");
    }

    #[test]
    fn as_bool_returns_error_on_empty_value() {
        let field = HeaderField::<ValueMarker> {
            kind: HeaderKind::Bool,
            value: Bytes::new(),
            _marker: PhantomData,
        };
        assert!(field.as_bool().is_err());
    }

    #[test]
    fn as_uint8_returns_error_on_empty_value() {
        let field = HeaderField::<ValueMarker> {
            kind: HeaderKind::Uint8,
            value: Bytes::new(),
            _marker: PhantomData,
        };
        assert!(field.as_uint8().is_err());
    }

    #[test]
    fn from_bytes_returns_error_on_truncated_input() {
        let mut bytes = BytesMut::new();
        bytes.put_u8(2); // key_kind = String
        bytes.put_u32_le(100); // key_len = 100 (lie!)
        bytes.put_slice(b"abc"); // only 3 bytes of key data

        let result = HashMap::<HeaderKey, HeaderValue>::from_bytes(bytes.freeze());
        assert!(result.is_err());
    }

    #[test]
    fn display_handles_malformed_data() {
        let field = HeaderField::<ValueMarker> {
            kind: HeaderKind::Float64,
            value: Bytes::from(vec![1, 2]), // 2 bytes, needs 8
            _marker: PhantomData,
        };
        let display = format!("{}", field);
        assert!(display.contains("<malformed float64>"));
    }

    #[test]
    fn from_bytes_returns_error_on_wrong_size_for_fixed_type() {
        let mut bytes = BytesMut::new();
        // Key: Int32 with correct size
        bytes.put_u8(6); // key_kind = Int32
        bytes.put_u32_le(4); // key_len = 4 (correct)
        bytes.put_slice(&42i32.to_le_bytes());
        // Value: Int16 with wrong size
        bytes.put_u8(5); // value_kind = Int16
        bytes.put_u32_le(4); // value_len = 4 (wrong, should be 2)
        bytes.put_slice(&[1, 2, 3, 4]);

        let result = HashMap::<HeaderKey, HeaderValue>::from_bytes(bytes.freeze());
        assert!(result.is_err());
    }

    #[test]
    fn from_bytes_returns_error_on_truncated_value_length() {
        let mut bytes = BytesMut::new();
        // Key: String
        bytes.put_u8(2); // key_kind = String
        bytes.put_u32_le(3); // key_len = 3
        bytes.put_slice(b"abc");
        // Value kind but no length bytes
        bytes.put_u8(6); // value_kind = Int32
        // Missing value length bytes

        let result = HashMap::<HeaderKey, HeaderValue>::from_bytes(bytes.freeze());
        assert!(result.is_err());
    }

    #[test]
    fn to_string_value_handles_empty_bool() {
        let field = HeaderField::<ValueMarker> {
            kind: HeaderKind::Bool,
            value: Bytes::new(),
            _marker: PhantomData,
        };
        assert_eq!(field.to_string_value(), "<malformed bool>");
    }
}
