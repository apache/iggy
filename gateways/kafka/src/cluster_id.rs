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

//! The Kafka cluster id this gateway reports in Metadata and `DescribeCluster`.

use std::fmt;
use std::str::FromStr;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::{DecodeError, Engine};
use thiserror::Error;
use uuid::Uuid;

/// A Kafka cluster id: 16 random bytes in unpadded URL-safe base64, 22 characters, the form
/// Kafka's own `Uuid.toString` prints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterId(String);

#[derive(Debug, Error, PartialEq, Eq)]
#[error("`{value}` is not a Kafka cluster id: {reason}")]
pub struct InvalidClusterId {
    pub value: String,
    pub reason: &'static str,
}

impl ClusterId {
    /// A new random id, drawn the way Kafka's `Uuid.randomUuid` draws one.
    ///
    /// Kafka redraws an id whose text holds a `-` anywhere, because command line tools read a
    /// value that starts with one as a flag. It also never uses the all-zero id, or
    /// `AAAAAAAAAAAAAAAAAAAAAQ`, which is reserved for its metadata topic.
    #[must_use]
    pub fn random() -> Self {
        loop {
            let id = Self::from_bytes(*Uuid::new_v4().as_bytes());
            if !id.0.contains('-') && !id.is_reserved() {
                return id;
            }
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }

    fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(URL_SAFE_NO_PAD.encode(bytes))
    }

    fn is_reserved(&self) -> bool {
        self.0 == ZERO_ID || self.0 == METADATA_TOPIC_ID
    }
}

/// Kafka's `Uuid.ZERO_UUID`.
const ZERO_ID: &str = "AAAAAAAAAAAAAAAAAAAAAA";
/// Kafka's `Uuid.METADATA_TOPIC_ID`.
const METADATA_TOPIC_ID: &str = "AAAAAAAAAAAAAAAAAAAAAQ";

/// The longest stretch of a rejected value an [`InvalidClusterId`] keeps. The value can be a
/// whole Iggy message, and the error ends up in a log line.
const MAX_SHOWN_LEN: usize = 64;

/// Accepts exactly 22 characters of unpadded URL-safe base64 that decode to 16 bytes, a `-`
/// anywhere included: an id another tool minted is still that cluster's id. Padding is refused,
/// although Kafka's `Uuid.fromString` reads it. So are the two reserved ids and a spelling with
/// non-zero trailing bits, which no encoder prints and would read back as different text.
impl FromStr for ClusterId {
    type Err = InvalidClusterId;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let invalid = |reason| InvalidClusterId {
            value: shown(value),
            reason,
        };
        let bytes = URL_SAFE_NO_PAD.decode(value).map_err(|error| match error {
            DecodeError::InvalidLastSymbol { .. } => {
                invalid("not the canonical spelling of its bytes")
            }
            _ => invalid("not unpadded URL-safe base64"),
        })?;
        let bytes: [u8; 16] = bytes
            .try_into()
            .map_err(|_| invalid("does not decode to 16 bytes"))?;
        let id = Self::from_bytes(bytes);
        if id.is_reserved() {
            return Err(invalid("reserved by Kafka"));
        }
        Ok(id)
    }
}

fn shown(value: &str) -> String {
    match value.char_indices().nth(MAX_SHOWN_LEN) {
        Some((end, _)) => format!("{}...", &value[..end]),
        None => value.to_string(),
    }
}

impl fmt::Display for ClusterId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_a_random_id_when_printed_should_parse_back_to_itself() {
        for _ in 0..1_000 {
            let id = ClusterId::random();
            assert_eq!(id.as_str().len(), 22);
            assert!(!id.as_str().contains('-'));
            assert_eq!(id.as_str().parse::<ClusterId>(), Ok(id));
        }
    }

    #[test]
    fn given_an_id_kafka_printed_when_parsed_should_be_accepted() {
        // From `kafka-storage random-uuid`.
        let id: ClusterId = "MkU3OEVBNTcwNTJENDM2Qg"
            .parse()
            .expect("a Kafka cluster id");
        assert_eq!(id.to_string(), "MkU3OEVBNTcwNTJENDM2Qg");
    }

    /// Kafka's `randomUuid` never mints these, but its `fromString` reads them, and so may an
    /// older Kafka's or another tool's id.
    #[test]
    fn given_an_id_with_a_dash_when_parsed_should_be_accepted() {
        for value in ["-kU3OEVBNTcwNTJENDM2Qg", "MkU3OEVBNTcw-TJENDM2Qg"] {
            let id: ClusterId = value.parse().expect("a Kafka cluster id");
            assert_eq!(id.as_str(), value);
        }
    }

    #[test]
    fn given_text_that_is_not_an_id_when_parsed_should_say_why() {
        let reason = |value: &str| {
            value
                .parse::<ClusterId>()
                .expect_err("must not parse")
                .reason
        };
        assert_eq!(
            reason("MkU3OEVBNTcwNTJENDM2Qh"),
            "not the canonical spelling of its bytes"
        );
        assert_eq!(
            reason("MkU3OEVBNTcwNTJENDM2Q!"),
            "not unpadded URL-safe base64"
        );
        assert_eq!(reason("shortABC"), "does not decode to 16 bytes");
        assert_eq!(reason(ZERO_ID), "reserved by Kafka");
        assert_eq!(reason(METADATA_TOPIC_ID), "reserved by Kafka");
    }

    #[test]
    fn given_a_long_value_when_refused_should_keep_only_its_start() {
        let error = "x"
            .repeat(10_000)
            .parse::<ClusterId>()
            .expect_err("not an id");
        assert_eq!(error.value, format!("{}...", "x".repeat(MAX_SHOWN_LEN)));
    }

    #[test]
    fn given_text_that_is_not_an_id_when_parsed_should_be_rejected() {
        for value in [
            "",
            "short",
            "MkU3OEVBNTcwNTJENDM2Qg==",
            "MkU3OEVBNTcwNTJENDM2Qg+",
            "MkU3OEVBNTcwNTJENDM2Q!",
            "MkU3OEVBNTcwNTJENDM2Qh",
            ZERO_ID,
            METADATA_TOPIC_ID,
        ] {
            assert!(
                value.parse::<ClusterId>().is_err(),
                "`{value}` must not parse"
            );
        }
    }
}
