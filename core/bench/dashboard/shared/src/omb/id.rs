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

//! The id of an OMB run. An OMB result has no id of its own, so the dashboard makes one from the
//! content of the file.

use std::fmt;
use std::str::FromStr;

use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize, Serializer};
use thiserror::Error;

/// The id has 8 bytes, written as 2 hexadecimal digits each.
const ID_BYTES: usize = 8;
const ID_CHARS: usize = ID_BYTES * 2;

/// The first 8 bytes of the BLAKE3 hash of the result file, written as 16 lowercase hexadecimal
/// digits. A moved or copied file keeps its id, so links stay valid, and two copies of one file are
/// one run. An edited file gets a new id. The id is not a UUID, so an iggy-bench run id never
/// names an OMB run, and the reverse.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct OmbRunId(u64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum OmbRunIdError {
    #[error("an OMB run id has 16 characters, but this one has {0}")]
    WrongLength(usize),
    #[error("an OMB run id holds the lowercase hexadecimal digits 0 to 9 and a to f only")]
    NotHexadecimal,
}

impl OmbRunId {
    /// The id of the result file with the content `bytes`.
    pub fn of_file(bytes: &[u8]) -> Self {
        let hash = blake3::hash(bytes);
        let mut prefix = [0; ID_BYTES];
        prefix.copy_from_slice(&hash.as_bytes()[..ID_BYTES]);
        Self(u64::from_be_bytes(prefix))
    }
}

impl fmt::Display for OmbRunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl FromStr for OmbRunId {
    type Err = OmbRunIdError;

    /// Takes exactly 16 lowercase hexadecimal digits. `u64::from_str_radix` alone would also take
    /// a sign, fewer digits and uppercase digits.
    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let chars = text.chars().count();
        if chars != ID_CHARS {
            return Err(OmbRunIdError::WrongLength(chars));
        }
        if !text
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
        {
            return Err(OmbRunIdError::NotHexadecimal);
        }
        u64::from_str_radix(text, 16)
            .map(Self)
            .map_err(|_| OmbRunIdError::NotHexadecimal)
    }
}

impl Serialize for OmbRunId {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for OmbRunId {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        String::deserialize(deserializer)?
            .parse()
            .map_err(de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::omb::test_fixtures::{ALL, RC1_DEFAULT};

    /// The BLAKE3 hash of no bytes starts with these 8 bytes.
    const EMPTY_INPUT_ID: &str = "af1349b9f5f9a1a6";

    #[test]
    fn given_bytes_when_hashed_should_take_first_eight_bytes_of_blake3() {
        let id = OmbRunId::of_file(b"");

        assert_eq!(id.to_string(), EMPTY_INPUT_ID);
        assert_eq!(id, OmbRunId(0xaf13_49b9_f5f9_a1a6));
    }

    #[test]
    fn given_same_bytes_when_hashed_should_give_same_id() {
        let copy = RC1_DEFAULT.json.to_vec();
        let mut edited = copy.clone();
        edited.push(b'\n');

        assert_eq!(
            OmbRunId::of_file(&copy),
            OmbRunId::of_file(RC1_DEFAULT.json)
        );
        assert_ne!(
            OmbRunId::of_file(&edited),
            OmbRunId::of_file(RC1_DEFAULT.json)
        );
    }

    #[test]
    fn given_fixtures_when_hashed_should_give_distinct_ids_of_sixteen_lowercase_digits() {
        let mut ids: Vec<_> = ALL
            .iter()
            .map(|fixture| OmbRunId::of_file(fixture.json))
            .collect();

        for id in &ids {
            let text = id.to_string();
            assert_eq!(text.len(), 16, "{text}");
            assert!(
                text.bytes()
                    .all(|byte| byte.is_ascii_digit() || byte.is_ascii_lowercase())
            );
            assert_eq!(text.parse::<OmbRunId>(), Ok(*id));
        }
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), ALL.len());
    }

    #[test]
    fn given_short_long_or_uuid_text_when_parsed_should_name_length() {
        assert_eq!(
            "af1349b9f5f9a1a".parse::<OmbRunId>(),
            Err(OmbRunIdError::WrongLength(15))
        );
        assert_eq!(
            "af1349b9f5f9a1a6a".parse::<OmbRunId>(),
            Err(OmbRunIdError::WrongLength(17))
        );
        assert_eq!(
            "b5c560ad-1ad2-49f0-88dd-c29db75126aa".parse::<OmbRunId>(),
            Err(OmbRunIdError::WrongLength(36))
        );
        assert_eq!(
            "b5c560ad1ad249f088ddc29db75126aa".parse::<OmbRunId>(),
            Err(OmbRunIdError::WrongLength(32))
        );
        assert_eq!("".parse::<OmbRunId>(), Err(OmbRunIdError::WrongLength(0)));
    }

    #[test]
    fn given_sixteen_characters_that_are_not_lowercase_hex_when_parsed_should_fail() {
        for text in [
            "AF1349B9F5F9A1A6",
            "+f1349b9f5f9a1a6",
            "af1349b9f5f9a1ag",
            "af1349b9 5f9a1a6",
            "compare000000000",
        ] {
            assert_eq!(
                text.parse::<OmbRunId>(),
                Err(OmbRunIdError::NotHexadecimal),
                "{text}"
            );
        }
        // Multi-byte characters: 16 bytes, but 8 characters, and 16 characters, but 32 bytes.
        assert_eq!(
            "ąąąąąąąą".parse::<OmbRunId>(),
            Err(OmbRunIdError::WrongLength(8))
        );
        assert_eq!(
            "ąąąąąąąąąąąąąąąą".parse::<OmbRunId>(),
            Err(OmbRunIdError::NotHexadecimal)
        );
    }

    #[test]
    fn given_id_when_serialized_should_round_trip_as_string() {
        let id = OmbRunId::of_file(RC1_DEFAULT.json);
        let json = serde_json::to_string(&id).expect("ids serialize");

        assert_eq!(json, format!("\"{id}\""));
        assert_eq!(
            serde_json::from_str::<OmbRunId>(&json).expect("ids deserialize"),
            id
        );
        assert!(
            serde_json::from_str::<OmbRunId>("\"b5c560ad-1ad2-49f0-88dd-c29db75126aa\"").is_err()
        );
        assert!(serde_json::from_str::<OmbRunId>("12").is_err());
    }
}
