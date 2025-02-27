use crate::bytes_serializable::BytesSerializable;
use crate::error::IggyError;
use crate::utils::byte_size::IggyByteSize;
use crate::utils::sizeable::Sizeable;
use crate::validatable::Validatable;
use arrayvec::{ArrayString, ArrayVec};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::fmt::Display;
use std::hash::{Hash, Hasher};
use std::str::FromStr;

/// `Identifier` represents the unique identifier of the resources such as stream, topic, partition, user etc.
/// It consists of the following fields:
/// - `kind`: the kind of the identifier.
/// - `length`: the length of the identifier payload.
/// - `value`: the binary value of the identifier payload.
#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Eq)]
pub enum Identifier2<const CAP: usize> {
    Numeric(ArrayVec<u8, 4>),
    String(ArrayString<CAP>),
}

impl<const CAP: usize> Default for Identifier2<CAP> {
    fn default() -> Self {
        Self::Numeric(ArrayVec::from(1u32.to_le_bytes()))
    }
}

impl<const CAP: usize> Validatable<IggyError> for Identifier2<CAP> {
    fn validate(&self) -> Result<(), IggyError> {
        if CAP == 0 {
            return Err(IggyError::InvalidIdentifier);
        }

        if self.is_empty() {
            return Err(IggyError::InvalidIdentifier);
        }

        Ok(())
    }
}

impl<const CAP: usize> Identifier2<CAP> {
    fn is_empty(&self) -> bool {
        match self {
            Identifier2::Numeric(array_vec) => array_vec.is_empty(),
            Identifier2::String(array_string) => array_string.is_empty(),
        }
    }

    /// Returns the numeric value of the identifier.
    pub fn get_u32_value(&self) -> Result<u32, IggyError> {
        match self {
            Identifier2::Numeric(array_vec) => match array_vec.clone().into_inner() {
                Ok(ar) => Ok(u32::from_le_bytes(ar)),
                Err(_) => Err(IggyError::InvalidIdentifier),
            },
            Identifier2::String(_) => Err(IggyError::InvalidIdentifier),
        }
    }

    /// Returns the string value of the identifier.
    pub fn get_string_value(&self) -> Result<String, IggyError> {
        self.get_cow_str_value().map(|cow| cow.to_string())
    }

    /// Returns the Cow<str> value of the identifier.
    pub fn get_cow_str_value(&self) -> Result<Cow<str>, IggyError> {
        match self {
            Identifier2::Numeric(_) => Err(IggyError::InvalidIdentifier),
            Identifier2::String(array_string) => {
                Ok(String::from_utf8_lossy(array_string.as_bytes()))
            }
        }
    }

    /// Returns the string representation of the identifier.
    pub fn as_string(&self) -> String {
        self.as_cow_str().to_string()
    }

    // Returns the Cow<str> representation of the identifier.
    pub fn as_cow_str(&self) -> Cow<str> {
        match self {
            Identifier2::Numeric(_) => Cow::Owned(self.get_u32_value().unwrap().to_string()),
            Identifier2::String(_) => self.get_cow_str_value().unwrap(),
        }
    }

    /// Creates a new identifier from the given identifier.
    pub fn from_identifier(identifier: &Identifier2<CAP>) -> Self {
        match identifier {
            Identifier2::Numeric(array_vec) => Identifier2::Numeric(array_vec.clone()),
            Identifier2::String(array_string) => Identifier2::String(*array_string),
        }
    }

    /// Creates a new identifier from the given string value, either numeric or string.
    pub fn from_str_value(value: &str) -> Result<Self, IggyError> {
        let length = value.len();
        if length == 0 || length > 255 {
            return Err(IggyError::InvalidIdentifier);
        }

        match value.parse::<u32>() {
            Ok(id) => Identifier2::numeric(id),
            Err(_) => Identifier2::named(value),
        }
    }

    /// Creates a new identifier from the given numeric value.
    pub fn numeric(value: u32) -> Result<Self, IggyError> {
        if value == 0 {
            return Err(IggyError::InvalidIdentifier);
        }

        Ok(Identifier2::Numeric(ArrayVec::from(value.to_le_bytes())))
    }

    /// Creates a new identifier from the given string value.
    pub fn named(value: &str) -> Result<Self, IggyError> {
        let length = value.len();
        if length == 0 || length > 255 {
            return Err(IggyError::InvalidIdentifier);
        }

        Ok(Identifier2::String(ArrayString::from(value).unwrap()))
    }

    /// Length of the identifier, for a string it is the capacity of the underlying array
    pub fn length(&self) -> usize {
        match self {
            Identifier2::Numeric(_) => 4,
            Identifier2::String(_) => CAP,
        }
    }

    /// Returns the code of the identifier kind.
    pub fn code(&self) -> u8 {
        match self {
            Identifier2::Numeric(_) => 1,
            Identifier2::String(_) => 2,
        }
    }
}

impl<const CAP: usize> Sizeable for Identifier2<CAP> {
    fn get_size_bytes(&self) -> IggyByteSize {
        IggyByteSize::from(self.length() as u64 + 2)
    }
}

impl<const CAP: usize> BytesSerializable for Identifier2<CAP> {
    fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(2 + self.length());
        bytes.put_u8(self.code());
        bytes.put_u8(self.length() as u8);
        match self {
            Identifier2::Numeric(array_vec) => bytes.put_slice(array_vec),
            Identifier2::String(array_string) => bytes.put_slice(array_string.as_bytes()),
        }
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        if bytes.len() < 3 {
            return Err(IggyError::InvalidIdentifier);
        }

        let kind = bytes[0];
        let length = bytes[1] as usize;

        let id = match kind {
            1 => {
                if length != 4 {
                    Err(IggyError::InvalidIdentifier)
                } else {
                    let mut av = ArrayVec::<u8, 4>::new();
                    av.try_extend_from_slice(&bytes[2..2 + 4]).unwrap();
                    Ok(Identifier2::Numeric(av))
                }
            }
            2 => {
                let value = &bytes[2..2 + length];
                if value.len() != length {
                    Err(IggyError::InvalidIdentifier)
                } else {
                    let str = String::from_utf8_lossy(value);
                    let ar = ArrayString::from(&str).unwrap();
                    Ok(Identifier2::String(ar))
                }
            }
            _ => Err(IggyError::InvalidIdentifier),
        }?;

        id.validate()?;
        Ok(id)
    }
}

impl<const CAP: usize> FromStr for Identifier2<CAP> {
    type Err = IggyError;
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if let Ok(value) = input.parse::<u32>() {
            return Identifier2::numeric(value);
        }

        let identifier = Identifier2::named(input)?;
        identifier.validate()?;
        Ok(identifier)
    }
}

impl<const CAP: usize> TryFrom<u32> for Identifier2<CAP> {
    type Error = IggyError;
    fn try_from(value: u32) -> Result<Self, Self::Error> {
        Identifier2::numeric(value)
    }
}

impl<const CAP: usize> TryFrom<String> for Identifier2<CAP> {
    type Error = IggyError;
    fn try_from(value: String) -> Result<Self, Self::Error> {
        Identifier2::from_str(&value)
    }
}

impl<const CAP: usize> TryFrom<&str> for Identifier2<CAP> {
    type Error = IggyError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        Identifier2::from_str(value)
    }
}

impl<const CAP: usize> Display for Identifier2<CAP> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Identifier2::Numeric(array_vec) => write!(
                f,
                "{}",
                u32::from_le_bytes(array_vec.clone().into_inner().unwrap())
            ),
            Identifier2::String(array_string) => {
                write!(f, "{}", String::from_utf8_lossy(array_string.as_bytes()))
            }
        }
    }
}

impl<const CAP: usize> Hash for Identifier2<CAP> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        match self {
            Identifier2::Numeric(_) => self.get_u32_value().unwrap().hash(state),
            Identifier2::String(_) => self.get_cow_str_value().unwrap().hash(state),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifier_with_a_value_of_greater_than_zero_should_be_valid() {
        assert!(Identifier2::<4>::numeric(1).is_ok());
    }

    #[test]
    fn identifier_with_a_value_of_zero_should_be_invalid() {
        assert!(Identifier2::<4>::numeric(0).is_err());
    }

    #[test]
    fn identifier_with_a_value_of_non_empty_string_should_be_valid() {
        assert!(Identifier2::<4>::named("test").is_ok());
    }

    #[test]
    fn identifier_with_a_value_of_empty_string_should_be_invalid() {
        assert!(Identifier2::<0>::named("").is_err());
    }

    #[test]
    fn identifier_with_a_value_of_string_greater_than_255_chars_should_be_invalid() {
        assert!(Identifier2::<256>::named(&"a".repeat(256)).is_err());
    }

    #[test]
    fn numeric_id_should_be_converted_into_identifier_using_trait() {
        let id = 1;
        if let Identifier2::<4>::Numeric(av) = id.try_into().unwrap() {
            assert_eq!(av.to_vec(), id.to_le_bytes().to_vec());
        } else {
            panic!("identifier not numeric");
        }
    }

    #[test]
    fn string_id_should_be_converted_into_identifier_using_trait() {
        let id = "test";
        if let Identifier2::<4>::String(av) = id.try_into().unwrap() {
            assert_eq!(av.as_bytes(), id.as_bytes());
        } else {
            panic!("identifier not string");
        }
    }
}
