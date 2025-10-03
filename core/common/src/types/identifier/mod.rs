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
use crate::Sizeable;
use crate::Validatable;
use crate::error::IggyError;
use crate::utils::byte_size::IggyByteSize;
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use serde_with::base64::Base64;
use serde_with::serde_as;
use std::borrow::Cow;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::str::FromStr;

/// `Identifier` represents the unique identifier of the resources such as stream, topic, partition, user etc.
/// It consists of the following fields:
/// - `kind`: the kind of the identifier.
/// - `length`: the length of the identifier payload.
/// - `value`: the binary value of the identifier payload.
#[serde_as]
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
pub struct Identifier {
    /// The kind of the identifier.
    pub kind: IdKind,
    /// The length of the identifier payload.
    #[serde(skip)]
    pub length: u8,
    /// The binary value of the identifier payload, max length is 255 bytes.
    #[serde_as(as = "Base64")]
    pub value: Vec<u8>,
}

/// `IdKind` represents the kind of the identifier.
#[derive(Debug, Serialize, Deserialize, PartialEq, Default, Copy, Clone, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IdKind {
    /// The identifier is numeric.
    #[default]
    Numeric,
    /// The identifier is string.
    String,
}

impl Default for Identifier {
    fn default() -> Self {
        Self {
            kind: IdKind::default(),
            length: 4,
            value: 0u32.to_le_bytes().to_vec(),
        }
    }
}

impl Validatable<IggyError> for Identifier {
    fn validate(&self) -> Result<(), IggyError> {
        if self.length == 0 {
            return Err(IggyError::InvalidIdentifier);
        }

        if self.value.is_empty() {
            return Err(IggyError::InvalidIdentifier);
        }

        #[allow(clippy::cast_possible_truncation)]
        if self.length != self.value.len() as u8 {
            return Err(IggyError::InvalidIdentifier);
        }

        if self.kind == IdKind::Numeric && self.length != 4 {
            return Err(IggyError::InvalidIdentifier);
        }

        Ok(())
    }
}

impl Identifier {
    /// Returns the numeric value of the identifier.
    pub fn get_u32_value(&self) -> Result<u32, IggyError> {
        if self.kind != IdKind::Numeric {
            return Err(IggyError::InvalidIdentifier);
        }

        if self.length != 4 {
            return Err(IggyError::InvalidIdentifier);
        }

        Ok(u32::from_le_bytes(self.value.clone().try_into().unwrap()))
    }

    /// Returns the string value of the identifier.
    pub fn get_string_value(&self) -> Result<String, IggyError> {
        self.get_cow_str_value().map(|cow| cow.to_string())
    }

    /// Returns the `Cow<str>` value of the identifier.
    pub fn get_cow_str_value(&self) -> Result<Cow<'_, str>, IggyError> {
        if self.kind != IdKind::String {
            return Err(IggyError::InvalidIdentifier);
        }

        Ok(String::from_utf8_lossy(&self.value))
    }

    /// Returns the string representation of the identifier.
    pub fn as_string(&self) -> String {
        self.as_cow_str().to_string()
    }

    // Returns the Cow<str> representation of the identifier.
    pub fn as_cow_str(&self) -> Cow<'_, str> {
        match self.kind {
            IdKind::Numeric => Cow::Owned(self.get_u32_value().unwrap().to_string()),
            IdKind::String => self.get_cow_str_value().unwrap(),
        }
    }

    /// Creates a new identifier from the given identifier.
    pub fn from_identifier(identifier: &Identifier) -> Self {
        Self {
            kind: identifier.kind,
            length: identifier.length,
            value: identifier.value.clone(),
        }
    }

    /// Creates a new identifier from the given string value, either numeric or string.
    pub fn from_str_value(value: &str) -> Result<Self, IggyError> {
        let length = value.len();
        if length == 0 || length > 255 {
            return Err(IggyError::InvalidIdentifier);
        }

        match value.parse::<u32>() {
            Ok(id) => Identifier::numeric(id),
            Err(_) => Identifier::named(value),
        }
    }

    /// Creates a new identifier from the given numeric value.
    pub fn numeric(value: u32) -> Result<Self, IggyError> {
        /*
        if value == 0 {
            return Err(IggyError::InvalidIdentifier);
        }
        */

        Ok(Self {
            kind: IdKind::Numeric,
            length: 4,
            value: value.to_le_bytes().to_vec(),
        })
    }

    /// Creates a new identifier from the given string value.
    pub fn named(value: &str) -> Result<Self, IggyError> {
        let length = value.len();
        if length == 0 || length > 255 {
            return Err(IggyError::InvalidIdentifier);
        }

        Ok(Self {
            kind: IdKind::String,
            #[allow(clippy::cast_possible_truncation)]
            length: length as u8,
            value: value.as_bytes().to_vec(),
        })
    }

    /// Creates identifier from raw bytes
    pub fn from_raw_bytes(bytes: &[u8]) -> Result<Self, IggyError> {
        let kind = IdKind::from_code(bytes[0])?;
        let length = bytes[1];
        let value = bytes[2..2 + length as usize].to_vec();
        if value.len() != length as usize {
            return Err(IggyError::InvalidIdentifier);
        }

        let identifier = Identifier {
            kind,
            length,
            value,
        };
        identifier.validate()?;
        Ok(identifier)
    }

    /// Maximum size of the Identifier struct
    pub const fn maximum_byte_size() -> usize {
        2 + 255
    }
}

impl Sizeable for Identifier {
    fn get_size_bytes(&self) -> IggyByteSize {
        IggyByteSize::from(u64::from(self.length) + 2)
    }
}

impl BytesSerializable for Identifier {
    fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(2 + self.length as usize);
        bytes.put_u8(self.kind.as_code());
        bytes.put_u8(self.length);
        bytes.put_slice(&self.value);
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        if bytes.len() < 3 {
            return Err(IggyError::InvalidIdentifier);
        }

        let kind = IdKind::from_code(bytes[0])?;
        let length = bytes[1];
        let value = bytes[2..2 + length as usize].to_vec();
        if value.len() != length as usize {
            return Err(IggyError::InvalidIdentifier);
        }

        let identifier = Identifier {
            kind,
            length,
            value,
        };
        identifier.validate()?;
        Ok(identifier)
    }

    fn write_to_buffer(&self, bytes: &mut BytesMut) {
        bytes.put_u8(self.kind.as_code());
        bytes.put_u8(self.length);
        bytes.put_slice(&self.value);
    }

    fn get_buffer_size(&self) -> usize {
        2 + self.length as usize
    }
}

impl IdKind {
    /// Returns the code of the identifier kind.
    pub fn as_code(&self) -> u8 {
        match self {
            IdKind::Numeric => 1,
            IdKind::String => 2,
        }
    }

    /// Returns the identifier kind from the code.
    pub fn from_code(code: u8) -> Result<Self, IggyError> {
        match code {
            1 => Ok(IdKind::Numeric),
            2 => Ok(IdKind::String),
            _ => Err(IggyError::InvalidIdentifier),
        }
    }
}

impl FromStr for IdKind {
    type Err = IggyError;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        match input {
            "n" | "numeric" => Ok(IdKind::Numeric),
            "s" | "string" => Ok(IdKind::String),
            _ => Err(IggyError::InvalidIdentifier),
        }
    }
}

impl FromStr for Identifier {
    type Err = IggyError;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if let Ok(value) = input.parse::<u32>() {
            return Identifier::numeric(value);
        }

        let identifier = Identifier::named(input)?;
        identifier.validate()?;
        Ok(identifier)
    }
}

impl TryFrom<u32> for Identifier {
    type Error = IggyError;
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        Identifier::numeric(value)
    }
}

impl TryFrom<String> for Identifier {
    type Error = IggyError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Identifier::from_str(&value)
    }
}

impl TryFrom<&str> for Identifier {
    type Error = IggyError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Identifier::from_str(value)
    }
}

impl Display for Identifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            IdKind::Numeric => write!(
                f,
                "{}",
                u32::from_le_bytes(self.value.as_slice().try_into().unwrap())
            ),
            IdKind::String => write!(f, "{}", String::from_utf8_lossy(&self.value)),
        }
    }
}

impl Display for IdKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IdKind::Numeric => write!(f, "numeric"),
            IdKind::String => write!(f, "string"),
        }
    }
}

impl Hash for Identifier {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self.kind {
            IdKind::Numeric => {
                self.get_u32_value().unwrap().hash(state);
            }
            IdKind::String => {
                self.get_cow_str_value().unwrap().hash(state);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifier_with_a_value_of_greater_than_zero_should_be_valid() {
        assert!(Identifier::numeric(1).is_ok());
    }

    #[test]
    fn identifier_with_a_value_of_non_empty_string_should_be_valid() {
        assert!(Identifier::named("test").is_ok());
    }

    #[test]
    fn identifier_with_a_value_of_empty_string_should_be_invalid() {
        assert!(Identifier::named("").is_err());
    }

    #[test]
    fn identifier_with_a_value_of_string_greater_than_255_chars_should_be_invalid() {
        assert!(Identifier::named(&"a".repeat(256)).is_err());
    }

    #[test]
    fn numeric_id_should_be_converted_into_identifier_using_trait() {
        let id = 1;
        let identifier: Identifier = id.try_into().unwrap();
        assert_eq!(identifier.kind, IdKind::Numeric);
        assert_eq!(identifier.length, 4);
        assert_eq!(identifier.value, id.to_le_bytes().to_vec());
    }

    #[test]
    fn string_id_should_be_converted_into_identifier_using_trait() {
        let id = "test";
        let identifier: Identifier = id.try_into().unwrap();
        assert_eq!(identifier.kind, IdKind::String);
        assert_eq!(identifier.length, 4);
        assert_eq!(identifier.value, id.as_bytes().to_vec());
    }
}
