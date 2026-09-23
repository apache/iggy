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

//! `ApiVersions` (API key 18).

use bytes::Bytes;
use kafka_protocol::messages::api_versions_response::ApiVersion;
use kafka_protocol::messages::{ApiVersionsRequest, ApiVersionsResponse};

use crate::error::Result;
use crate::protocol::api::{
    API_KEY_API_VERSIONS, ApiVersionRange, ERROR_INVALID_REQUEST, ERROR_NONE,
    ERROR_UNSUPPORTED_VERSION, GatewayState, HandleOutcome, advertised_min_version,
    is_supported_version, sasl_advertised_ranges, supported_api_ranges,
};
use crate::protocol::bounds_guard::validate_api_versions_shape;
use crate::protocol::handlers::{decode_guarded, encode_message, respond_or_close};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_API_VERSIONS,
    min_version: 0,
    max_version: 3,
};

#[expect(
    clippy::unused_async,
    reason = "the shared handler signature, kept until a handler awaits the bridge"
)]
pub async fn handle(state: &GatewayState, api_version: i16, body: Bytes) -> HandleOutcome {
    if !is_supported_version(API_KEY_API_VERSIONS, api_version) {
        // KIP-511: reply with v0 when the requested version is not understood.
        return respond_or_close(
            encode_response(0, ERROR_UNSUPPORTED_VERSION, state.sasl_enabled),
            "ApiVersions",
        );
    }
    match decode_guarded::<ApiVersionsRequest>(api_version, body, validate_api_versions_shape) {
        Ok(_) => respond_or_close(
            encode_response(api_version, ERROR_NONE, state.sasl_enabled),
            "ApiVersions",
        ),
        Err(error) => {
            // debug!, not warn!: attacker-controlled, not operator-actionable (see the same
            // note on the Produce decode-failure arm).
            tracing::debug!(%error, "failed to decode ApiVersions request");
            respond_or_close(
                encode_response(api_version, ERROR_INVALID_REQUEST, state.sasl_enabled),
                "ApiVersions",
            )
        }
    }
}

/// Advertise exactly what the firewall allows, so a client never negotiates a version this
/// gateway then refuses.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `api_version`.
pub fn encode_response(api_version: i16, error_code: i16, sasl_enabled: bool) -> Result<Bytes> {
    // The SASL keys are advertised only while the feature is on, and are deliberately kept out of
    // `SUPPORTED_RANGES` so dispatch never serves them. With SASL off the state machine still
    // answers them with `ILLEGAL_SASL_STATE` and keeps the connection open.
    let sasl = if sasl_enabled {
        sasl_advertised_ranges()
    } else {
        &[][..]
    };
    // Ascending by api_key, as every real broker emits it. Chaining the SASL rows onto the end
    // would put key 17 after key 19, which no broker does and some clients do not expect.
    let mut rows: Vec<&ApiVersionRange> = supported_api_ranges().iter().chain(sasl).collect();
    rows.sort_unstable_by_key(|r| r.api_key);
    let api_keys = rows
        .into_iter()
        .map(|r| {
            ApiVersion::default()
                .with_api_key(r.api_key)
                .with_min_version(advertised_min_version(r.api_key, r.min_version))
                .with_max_version(r.max_version)
        })
        .collect();
    let resp = ApiVersionsResponse::default()
        .with_error_code(error_code)
        .with_api_keys(api_keys);
    encode_message(&resp, api_version, 128)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::handlers::decode_exhaustive;

    #[test]
    fn decode_v3_requires_software_fields() {
        assert!(decode_exhaustive::<ApiVersionsRequest>(3, Bytes::new()).is_err());
    }

    #[test]
    fn decode_v3_accepts_valid_body() {
        // Hand-encoded rather than round-tripped through `ApiVersionsRequest::encode`: encoding
        // is gated behind the crate's "client" feature, which this broker-only binary doesn't
        // enable.
        let body = Bytes::from_static(&[
            0x0a, b'i', b'g', b'g', b'y', b'-', b't', b'e', b's',
            b't', // compact string (len 9)
            0x06, b'0', b'.', b'1', b'.', b'0', // compact string (len 5)
            0x00, // empty tagged fields
        ]);
        decode_exhaustive::<ApiVersionsRequest>(3, body).unwrap();
    }
}
