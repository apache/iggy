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

use serde::{
    Deserialize, Serialize, Serializer,
    de::{self, Deserializer, Visitor},
};
use std::{
    fmt::{Display, Formatter},
    str::FromStr,
};

use crate::error::IggyError;

// for now only those, in the future will add snappy, lz4, zstd (same as in confluent kafka) in addition to that
// we should consider brotli as well.
/// Supported compression algorithms
#[derive(Debug, Default, PartialEq, Clone, Copy)]
pub enum CompressionAlgorithm {
    // No compression
    #[default]
    None,
    // Gzip compression algorithm
    Gzip,
}

impl FromStr for CompressionAlgorithm {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "gzip" => Ok(CompressionAlgorithm::Gzip),
            "none" => Ok(CompressionAlgorithm::None),
            _ => Err(format!("Unknown compression type: {s}")),
        }
    }
}

impl CompressionAlgorithm {
    pub fn as_code(&self) -> u8 {
        match self {
            CompressionAlgorithm::None => 1,
            CompressionAlgorithm::Gzip => 2,
        }
    }

    pub fn from_code(code: u8) -> Result<Self, IggyError> {
        match code {
            1 => Ok(CompressionAlgorithm::None),
            2 => Ok(CompressionAlgorithm::Gzip),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}

impl Display for CompressionAlgorithm {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionAlgorithm::None => write!(f, "none"),
            CompressionAlgorithm::Gzip => write!(f, "gzip"),
        }
    }
}

impl Serialize for CompressionAlgorithm {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            CompressionAlgorithm::None => serializer.serialize_str("none"),
            CompressionAlgorithm::Gzip => serializer.serialize_str("gzip"),
        }
    }
}

impl From<CompressionAlgorithm> for String {
    fn from(value: CompressionAlgorithm) -> Self {
        match value {
            CompressionAlgorithm::None => "none".to_string(),
            CompressionAlgorithm::Gzip => "gzip".to_string(),
        }
    }
}
struct CompressionKindVisitor;

impl Visitor<'_> for CompressionKindVisitor {
    type Value = CompressionAlgorithm;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a valid compression type, check documentation for more information.")
    }

    fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        CompressionAlgorithm::from_str(value).map_err(de::Error::custom)
    }
}

impl<'de> Deserialize<'de> for CompressionAlgorithm {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(CompressionKindVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from() {
        let none_alg = CompressionAlgorithm::from_str("none");
        assert!(none_alg.is_ok());
        assert_eq!(none_alg.unwrap(), CompressionAlgorithm::None);

        let none_alg = CompressionAlgorithm::from_str("None");
        assert!(none_alg.is_ok());
        assert_eq!(none_alg.unwrap(), CompressionAlgorithm::None);

        let gzip_alg = CompressionAlgorithm::from_str("gzip");
        assert!(gzip_alg.is_ok());
        assert_eq!(gzip_alg.unwrap(), CompressionAlgorithm::Gzip);

        let gzip_alg = CompressionAlgorithm::from_str("Gzip");
        assert!(gzip_alg.is_ok());
        assert_eq!(gzip_alg.unwrap(), CompressionAlgorithm::Gzip);
    }

    #[test]
    fn test_from_invalid_input() {
        let invalid_compression_kind = CompressionAlgorithm::from_str("invalid");
        assert!(invalid_compression_kind.is_err());

        let invalid_compression_kind = CompressionAlgorithm::from_str("gzipp");
        assert!(invalid_compression_kind.is_err());
    }

    #[test]
    fn test_into() {
        let none: CompressionAlgorithm = CompressionAlgorithm::None;
        let none_string: String = none.into();

        assert_eq!(none_string, "none".to_string());

        let gzip: CompressionAlgorithm = CompressionAlgorithm::Gzip;
        let gzip_string: String = gzip.into();

        assert_eq!(gzip_string, "gzip".to_string());
    }
    #[test]
    fn test_as_code() {
        let none = CompressionAlgorithm::None;
        let none_code = none.as_code();
        assert_eq!(none_code, 1);

        let gzip = CompressionAlgorithm::Gzip;
        let gzip_code = gzip.as_code();
        assert_eq!(gzip_code, 2);
    }
    #[test]
    fn test_from_code() {
        let none = CompressionAlgorithm::from_code(1);
        assert!(none.is_ok());
        assert_eq!(none.unwrap(), CompressionAlgorithm::None);

        let gzip = CompressionAlgorithm::from_code(2);
        assert!(gzip.is_ok());
        assert_eq!(gzip.unwrap(), CompressionAlgorithm::Gzip);
    }
    #[test]
    fn test_from_code_invalid_input() {
        let invalid_compression_kind = CompressionAlgorithm::from_code(0);
        assert!(invalid_compression_kind.is_err());

        let invalid_compression_kind = CompressionAlgorithm::from_code(69);
        assert!(invalid_compression_kind.is_err());

        let invalid_compression_kind = CompressionAlgorithm::from_code(255);
        assert!(invalid_compression_kind.is_err());
    }
}
