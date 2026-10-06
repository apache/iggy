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

//! Heartbeat (API key 12).
//!
//! Refreshes a member's session and is how a follower learns a rebalance started:
//! `REBALANCE_IN_PROGRESS` tells it to send `JoinGroup` again.

use bytes::Bytes;
use kafka_protocol::messages::{HeartbeatRequest, HeartbeatResponse};

use crate::error::Result;
use crate::protocol::api::{
    API_KEY_HEARTBEAT, ApiVersionRange, ERROR_INVALID_REQUEST, ERROR_UNSUPPORTED_VERSION,
    GatewayState, HandleOutcome, is_supported_version,
};
use crate::protocol::bounds_guard::validate_heartbeat_shape;
use crate::protocol::handlers::{
    decode_guarded, encode_message, respond_or_close, unsupported_version_response,
};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_HEARTBEAT,
    min_version: 0,
    max_version: 4,
};

pub async fn handle(state: &GatewayState, api_version: i16, body: Bytes) -> HandleOutcome {
    if !is_supported_version(API_KEY_HEARTBEAT, api_version) {
        return unsupported_version_response(API_KEY_HEARTBEAT, api_version, |version| {
            encode_error_response(version, ERROR_UNSUPPORTED_VERSION)
        });
    }
    let request =
        match decode_guarded::<HeartbeatRequest>(api_version, body, validate_heartbeat_shape) {
            Ok(request) => request,
            Err(error) => {
                // debug!, not warn!: attacker-controlled, not operator-actionable.
                tracing::debug!(%error, api_version, "Failed to decode Heartbeat request");
                return respond_or_close(
                    encode_error_response(api_version, ERROR_INVALID_REQUEST),
                    "Heartbeat",
                );
            }
        };

    let error_code = state
        .groups
        .heartbeat(
            &request.group_id.0,
            request.generation_id,
            &request.member_id,
        )
        .await;
    respond_or_close(encode_error_response(api_version, error_code), "Heartbeat")
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    let response = HeartbeatResponse::default().with_error_code(error_code);
    encode_message(&response, version, 16)
}
