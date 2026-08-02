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

use iggy_common::{HeaderKey, HeaderValue};
use iggy_connector_sdk::ProducedMessage;
use serde::{Deserialize, Serialize};
use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};
use std::str::FromStr;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Iggy `HeaderValue` rejects values above this size, and forwarded HTTP
/// header values (e.g. `User-Agent`) routinely exceed it.
pub const MAX_HEADER_VALUE_BYTES: usize = 255;

/// Validated secret-path endpoint identifier: exactly 32 lowercase hex
/// characters, giving the URL itself 128 bits of entropy.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct EndpointId(String);

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EndpointIdError {
    InvalidLength(usize),
    InvalidCharacter(char),
}

impl EndpointId {
    pub const LENGTH: usize = 32;
    const LOG_PREFIX_LENGTH: usize = 8;

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// A correlation handle for logs. The full id is the credential for a
    /// secret-path endpoint, so it must never reach a log line.
    pub fn log_prefix(&self) -> String {
        Self::log_prefix_of(&self.0)
    }

    pub fn log_prefix_of(endpoint_id: &str) -> String {
        format!(
            "{}...",
            &endpoint_id[..Self::LOG_PREFIX_LENGTH.min(endpoint_id.len())]
        )
    }
}

impl FromStr for EndpointId {
    type Err = EndpointIdError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.len() != Self::LENGTH {
            return Err(EndpointIdError::InvalidLength(value.len()));
        }
        if let Some(invalid) = value
            .chars()
            .find(|character| !matches!(character, '0'..='9' | 'a'..='f'))
        {
            return Err(EndpointIdError::InvalidCharacter(invalid));
        }
        Ok(Self(value.to_string()))
    }
}

impl TryFrom<String> for EndpointId {
    type Error = EndpointIdError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        value.parse()
    }
}

impl From<EndpointId> for String {
    fn from(endpoint_id: EndpointId) -> Self {
        endpoint_id.0
    }
}

/// Lets `HashMap<EndpointId, _>` be probed with the raw path segment, so
/// request routing never allocates. Sound because the derived `Hash` on a
/// single-field newtype hashes exactly as the inner `String`, which in turn
/// hashes as `str`.
impl Borrow<str> for EndpointId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

impl Display for EndpointId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

impl Display for EndpointIdError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLength(length) => write!(
                formatter,
                "endpoint_id must be exactly {} characters, got {length}",
                EndpointId::LENGTH
            ),
            Self::InvalidCharacter(character) => write!(
                formatter,
                "endpoint_id must be lowercase hex, found: {character}"
            ),
        }
    }
}

impl std::error::Error for EndpointIdError {}

/// Message accepted by an HTTP handler, queued for `poll()` to drain.
#[derive(Debug)]
pub struct QueuedMessage {
    /// Raw HTTP request body bytes, exactly as received.
    pub payload: Vec<u8>,
    /// Already filtered, clamped, and converted by the handler, so draining
    /// the queue cannot fail on a malformed header.
    pub headers: Option<BTreeMap<HeaderKey, HeaderValue>>,
    /// Accept time, kept for a queue-latency metric that does not exist yet.
    /// Never serialized into the message.
    pub received_at: Instant,
}

impl From<QueuedMessage> for ProducedMessage {
    fn from(message: QueuedMessage) -> Self {
        ProducedMessage {
            // Webhook bodies carry no identifier this connector can trust as a
            // dedupe key, and `timestamp` / `checksum` are Iggy's to fill.
            id: None,
            checksum: None,
            timestamp: None,
            origin_timestamp: None,
            headers: message.headers,
            payload: message.payload,
        }
    }
}

/// Wall-clock seconds since the Unix epoch, the unit `expires_at` and
/// `revoked_at` are expressed in. Endpoint expiry is evaluated against the
/// clock of the request that hits it, so no background sweeper is needed.
pub fn unix_now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or_default()
}

/// Clamps a forwarded header value to the Iggy `HeaderValue` limit on a
/// UTF-8 character boundary. Returns `None` for empty values, which Iggy
/// rejects outright.
pub fn clamp_header_value(value: &str) -> Option<&str> {
    if value.is_empty() {
        return None;
    }
    if value.len() <= MAX_HEADER_VALUE_BYTES {
        return Some(value);
    }
    let mut boundary = MAX_HEADER_VALUE_BYTES;
    while !value.is_char_boundary(boundary) {
        boundary -= 1;
    }
    Some(&value[..boundary])
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_ID: &str = "a3f8c2e1b9d04f7a8e6c1d2b3a4f5e6d";

    #[test]
    fn given_valid_lowercase_hex_when_parsed_should_accept() {
        let endpoint_id: EndpointId = VALID_ID.parse().expect("valid id must parse");
        assert_eq!(endpoint_id.as_str(), VALID_ID);
    }

    #[test]
    fn given_wrong_length_when_parsed_should_reject() {
        let result = "a3f8".parse::<EndpointId>();
        assert_eq!(result, Err(EndpointIdError::InvalidLength(4)));
    }

    #[test]
    fn given_uppercase_hex_when_parsed_should_reject() {
        let uppercase = VALID_ID.to_uppercase();
        assert_eq!(
            uppercase.parse::<EndpointId>(),
            Err(EndpointIdError::InvalidCharacter('A'))
        );
    }

    #[test]
    fn given_non_hex_character_when_parsed_should_reject() {
        let with_invalid = format!("g{}", &VALID_ID[1..]);
        assert_eq!(
            with_invalid.parse::<EndpointId>(),
            Err(EndpointIdError::InvalidCharacter('g'))
        );
    }

    #[test]
    fn given_rejected_id_when_displayed_should_name_the_reason() {
        assert_eq!(
            EndpointIdError::InvalidLength(4).to_string(),
            format!(
                "endpoint_id must be exactly {} characters, got 4",
                EndpointId::LENGTH
            )
        );
        assert_eq!(
            EndpointIdError::InvalidCharacter('g').to_string(),
            "endpoint_id must be lowercase hex, found: g"
        );
    }

    #[test]
    fn given_short_value_when_clamped_should_pass_through() {
        assert_eq!(clamp_header_value("api-client/1.0"), Some("api-client/1.0"));
    }

    #[test]
    fn given_empty_value_when_clamped_should_drop() {
        assert_eq!(clamp_header_value(""), None);
    }

    #[test]
    fn given_oversized_value_when_clamped_should_truncate_to_limit() {
        let oversized = "a".repeat(MAX_HEADER_VALUE_BYTES + 100);
        let clamped = clamp_header_value(&oversized).expect("non-empty stays present");
        assert_eq!(clamped.len(), MAX_HEADER_VALUE_BYTES);
    }

    #[test]
    fn given_multibyte_value_when_clamped_should_respect_char_boundary() {
        // 128 two-byte characters = 256 bytes; the clamp must land on the
        // 254-byte boundary, not split a character at 255.
        let multibyte = "é".repeat(128);
        let clamped = clamp_header_value(&multibyte).expect("non-empty stays present");
        assert_eq!(clamped.len(), 254);
        assert!(clamped.chars().all(|character| character == 'é'));
    }
}
