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

//! `JoinGroup` (API key 11).
//!
//! The handler parks for as long as the group's join barrier takes, so it owns no state of its
//! own: everything lives in `crate::group`. Subscription metadata is relayed verbatim to the
//! elected leader and never decoded - which assignor a group runs is the client's business.

use bytes::Bytes;
use kafka_protocol::messages::join_group_response::JoinGroupResponseMember;
use kafka_protocol::messages::{JoinGroupRequest, JoinGroupResponse};
use kafka_protocol::protocol::StrBytes;

use crate::error::Result;
use crate::group::{JoinRequest, JoinResult};
use crate::protocol::api::{
    API_KEY_JOIN_GROUP, ApiVersionRange, ERROR_INVALID_REQUEST, ERROR_UNSUPPORTED_VERSION,
    GatewayState, HandleOutcome, is_supported_version,
};
use crate::protocol::bounds_guard::validate_join_group_shape;
use crate::protocol::handlers::{
    decode_guarded, encode_message, respond_or_close, unsupported_version_response,
};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_JOIN_GROUP,
    min_version: 0,
    max_version: 9,
};

/// `protocol_name` is an `Option` in every version's struct but only nullable on the wire from
/// v7. Below that a `None` encodes as a `-1` length a client reading a non-nullable string
/// rejects, so an error response carries an empty name instead.
const FIRST_NULLABLE_PROTOCOL_NAME_VERSION: i16 = 7;

pub async fn handle(state: &GatewayState, api_version: i16, body: Bytes) -> HandleOutcome {
    if !is_supported_version(API_KEY_JOIN_GROUP, api_version) {
        return unsupported_version_response(API_KEY_JOIN_GROUP, api_version, |version| {
            encode_error_response(version, ERROR_UNSUPPORTED_VERSION)
        });
    }
    let request = match decode_guarded::<JoinGroupRequest>(api_version, body, |version, body| {
        validate_join_group_shape(version, body, state.max_frame_size)
    }) {
        Ok(request) => request,
        Err(error) => {
            // debug!, not warn!: attacker-controlled, not operator-actionable.
            tracing::debug!(%error, api_version, "Failed to decode JoinGroup request");
            return respond_or_close(
                encode_error_response(api_version, ERROR_INVALID_REQUEST),
                "JoinGroup",
            );
        }
    };
    if let Some(reason) = request.reason.as_ref() {
        tracing::debug!(%reason, "JoinGroup rejoin reason");
    }

    let result = state
        .groups
        .join(&JoinRequest::from((api_version, &request)))
        .await;
    respond_or_close(encode_response(api_version, &result), "JoinGroup")
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_response(version: i16, result: &JoinResult) -> Result<Bytes> {
    let members = result
        .members
        .iter()
        .map(|member| {
            JoinGroupResponseMember::default()
                .with_member_id(member.member_id.clone())
                .with_group_instance_id(member.group_instance_id.clone())
                .with_metadata(member.metadata.clone())
        })
        .collect();

    let protocol_name = match (&result.protocol_name, version) {
        (Some(name), _) => Some(name.clone()),
        (None, version) if version >= FIRST_NULLABLE_PROTOCOL_NAME_VERSION => None,
        (None, _) => Some(StrBytes::new()),
    };

    // `skip_assignment` stays at its `false` default: the encoder bails on `true` below v9.
    let response = JoinGroupResponse::default()
        .with_error_code(result.error)
        .with_generation_id(result.generation_id)
        .with_protocol_type(result.protocol_type.clone())
        .with_protocol_name(protocol_name)
        .with_leader(result.leader.clone())
        .with_member_id(result.member_id.clone())
        .with_members(members);
    encode_message(&response, version, 256)
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    encode_response(version, &JoinResult::error(error_code, StrBytes::new()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::api::ERROR_NONE;

    #[test]
    fn error_response_below_v7_carries_an_empty_protocol_name() {
        let body = encode_error_response(6, ERROR_INVALID_REQUEST).unwrap();
        // throttle(4) + error(2) + generation(4), then a compact string.
        assert_eq!(
            body[10], 1,
            "expected a zero-length compact string, not null"
        );
    }

    #[test]
    fn error_response_from_v7_carries_a_null_protocol_name() {
        let body = encode_error_response(7, ERROR_INVALID_REQUEST).unwrap();
        // throttle(4) + error(2) + generation(4) + null protocol_type, then protocol_name.
        assert_eq!(body[10], 0, "expected a null protocol_type");
        assert_eq!(body[11], 0, "expected a null protocol_name");
    }

    #[test]
    fn successful_response_keeps_the_selected_protocol_name() {
        let result = JoinResult {
            error: ERROR_NONE,
            generation_id: 1,
            protocol_type: Some(StrBytes::from_static_str("consumer")),
            protocol_name: Some(StrBytes::from_static_str("range")),
            leader: StrBytes::from_static_str("m-1"),
            member_id: StrBytes::from_static_str("m-1"),
            members: Vec::new(),
        };
        assert!(encode_response(9, &result).is_ok());
    }
}
