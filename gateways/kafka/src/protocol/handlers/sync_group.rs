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

//! `SyncGroup` (API key 14).
//!
//! The leader ships one opaque blob per member and every member is handed back exactly the bytes
//! the leader filed under its own id, in the same generation. That relay is the whole contract:
//! partitions are disjoint because the leader's assignor made them so, not because this gateway
//! looked inside.

use bytes::Bytes;
use kafka_protocol::messages::{SyncGroupRequest, SyncGroupResponse};

use crate::error::Result;
use crate::group::{SyncRequest, SyncResult};
use crate::protocol::api::{
    API_KEY_SYNC_GROUP, ApiVersionRange, ERROR_INVALID_REQUEST, ERROR_UNSUPPORTED_VERSION,
    GatewayState, HandleOutcome, is_supported_version,
};
use crate::protocol::bounds_guard::validate_sync_group_shape;
use crate::protocol::handlers::{
    decode_guarded, encode_message, respond_or_close, unsupported_version_response,
};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_SYNC_GROUP,
    min_version: 0,
    max_version: 5,
};

pub async fn handle(state: &GatewayState, api_version: i16, body: Bytes) -> HandleOutcome {
    if !is_supported_version(API_KEY_SYNC_GROUP, api_version) {
        return unsupported_version_response(API_KEY_SYNC_GROUP, api_version, |version| {
            encode_error_response(version, ERROR_UNSUPPORTED_VERSION)
        });
    }
    let request = match decode_guarded::<SyncGroupRequest>(api_version, body, |version, body| {
        validate_sync_group_shape(version, body, state.max_frame_size)
    }) {
        Ok(request) => request,
        Err(error) => {
            // debug!, not warn!: attacker-controlled, not operator-actionable.
            tracing::debug!(%error, api_version, "Failed to decode SyncGroup request");
            return respond_or_close(
                encode_error_response(api_version, ERROR_INVALID_REQUEST),
                "SyncGroup",
            );
        }
    };

    let result = state.groups.sync(&SyncRequest::from(&request)).await;
    respond_or_close(encode_response(api_version, &result), "SyncGroup")
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_response(version: i16, result: &SyncResult) -> Result<Bytes> {
    let response = SyncGroupResponse::default()
        .with_error_code(result.error)
        .with_protocol_type(result.protocol_type.clone())
        .with_protocol_name(result.protocol_name.clone())
        .with_assignment(result.assignment.clone());
    encode_message(&response, version, 256)
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    encode_response(version, &SyncResult::error(error_code))
}
