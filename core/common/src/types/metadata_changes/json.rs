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

//! JSON forms shared by change records, items and answers.

use serde::{Deserialize, Deserializer, Serializer};

/// Length of a 128-bit value as lowercase hex.
const HEX_U128_LENGTH: usize = 32;

/// A 128-bit value as 32 lowercase hex characters.
pub(crate) mod hex_u128 {
    use super::{Deserialize, Deserializer, Serializer, parse};

    pub(crate) fn serialize<S: Serializer>(value: &u128, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format!("{value:032x}"))
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<u128, D::Error> {
        let text = String::deserialize(deserializer)?;
        parse(&text)
    }
}

/// An optional 128-bit value as 32 lowercase hex characters or null.
pub(crate) mod hex_u128_option {
    use super::{Deserialize, Deserializer, Serializer, parse};

    // serde's `with` hands the field by reference.
    #[allow(clippy::ref_option)]
    pub(crate) fn serialize<S: Serializer>(
        value: &Option<u128>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match value {
            Some(value) => serializer.serialize_str(&format!("{value:032x}")),
            None => serializer.serialize_none(),
        }
    }

    pub(crate) fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<u128>, D::Error> {
        Option::<String>::deserialize(deserializer)?
            .map(|text| parse(&text))
            .transpose()
    }
}

fn parse<E: serde::de::Error>(text: &str) -> Result<u128, E> {
    let canonical = text.len() == HEX_U128_LENGTH
        && text
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte));
    if !canonical {
        return Err(E::custom(format!(
            "expected {HEX_U128_LENGTH} lowercase hex characters, got {text:?}"
        )));
    }
    u128::from_str_radix(text, 16).map_err(E::custom)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Holder {
        #[serde(with = "hex_u128")]
        value: u128,
        #[serde(with = "hex_u128_option")]
        optional: Option<u128>,
    }

    #[test]
    fn given_128_bit_values_when_serializing_should_use_32_lowercase_hex_characters() {
        let holder = Holder {
            value: 0xd4b_8f2a_6e1c_93d7,
            optional: None,
        };
        let text = serde_json::to_string(&holder).unwrap();
        assert_eq!(
            text,
            r#"{"value":"00000000000000000d4b8f2a6e1c93d7","optional":null}"#
        );
        assert_eq!(serde_json::from_str::<Holder>(&text).unwrap(), holder);
    }

    #[test]
    fn given_short_uppercase_or_prefixed_hex_when_deserializing_should_refuse() {
        for value in [
            "d4b",
            "00000000000000000D4B8F2A6E1C93D7",
            "0x000000000000000d4b8f2a6e1c93d7",
        ] {
            let text = format!(r#"{{"value":"{value}","optional":null}}"#);
            assert!(serde_json::from_str::<Holder>(&text).is_err(), "{value}");
        }
    }
}
