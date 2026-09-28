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

//! `LeaveGroup` (API key 13).
//!
//! How a consumer that closes cleanly hands its partitions back without making the rest of its
//! group wait out its session. Never parks: the removal wakes whoever is parked on the group.

use bytes::Bytes;
use kafka_protocol::messages::leave_group_response::MemberResponse;
use kafka_protocol::messages::{LeaveGroupRequest, LeaveGroupResponse};

use crate::error::Result;
use crate::group::{LeaveRequest, LeaveResult};
use crate::protocol::api::{
    API_KEY_LEAVE_GROUP, ApiVersionRange, ERROR_INVALID_REQUEST, ERROR_UNSUPPORTED_VERSION,
    GatewayState, HandleOutcome, is_supported_version,
};
use crate::protocol::bounds_guard::validate_leave_group_shape;
use crate::protocol::handlers::{
    decode_guarded, encode_message, respond_or_close, unsupported_version_response,
};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_LEAVE_GROUP,
    min_version: 0,
    max_version: 5,
};

/// Below this version the response has no `members` array and the encoder refuses one.
const FIRST_BATCHED_VERSION: i16 = 3;
/// Two length prefixes and the error code around each echoed identity, as encoded at v3.
const PER_MEMBER_OVERHEAD: usize = 6;

pub async fn handle(state: &GatewayState, api_version: i16, body: Bytes) -> HandleOutcome {
    if !is_supported_version(API_KEY_LEAVE_GROUP, api_version) {
        return unsupported_version_response(API_KEY_LEAVE_GROUP, api_version, |version| {
            encode_error_response(version, ERROR_UNSUPPORTED_VERSION)
        });
    }
    let request = match decode_guarded::<LeaveGroupRequest>(api_version, body, |version, body| {
        validate_leave_group_shape(version, body, state.max_frame_size)
    }) {
        Ok(request) => request,
        Err(error) => {
            // debug!, not warn!: attacker-controlled, not operator-actionable.
            tracing::debug!(%error, api_version, "Failed to decode LeaveGroup request");
            return respond_or_close(
                encode_error_response(api_version, ERROR_INVALID_REQUEST),
                "LeaveGroup",
            );
        }
    };
    if tracing::enabled!(tracing::Level::DEBUG) {
        for reason in request
            .members
            .iter()
            .filter_map(|member| member.reason.as_ref())
        {
            tracing::debug!(%reason, "LeaveGroup reason");
        }
    }

    let result = state
        .groups
        .leave(&LeaveRequest::from((api_version, &request)))
        .await;
    respond_or_close(encode_response(api_version, &result), "LeaveGroup")
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_response(version: i16, result: &LeaveResult) -> Result<Bytes> {
    let response = if version < FIRST_BATCHED_VERSION {
        LeaveGroupResponse::default().with_error_code(result.top_level_error())
    } else {
        LeaveGroupResponse::default()
            .with_error_code(result.error)
            .with_members(
                result
                    .members
                    .iter()
                    .map(|member| {
                        MemberResponse::default()
                            .with_member_id(member.member_id.clone())
                            .with_group_instance_id(member.group_instance_id.clone())
                            .with_error_code(member.error)
                    })
                    .collect(),
            )
    };
    let echoed: usize = if version < FIRST_BATCHED_VERSION {
        0
    } else {
        result
            .members
            .iter()
            .map(|member| {
                member.member_id.len()
                    + member.group_instance_id.as_ref().map_or(0, |id| id.len())
                    + PER_MEMBER_OVERHEAD
            })
            .sum()
    };
    encode_message(&response, version, 64 + echoed)
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    encode_response(version, &LeaveResult::error(error_code))
}

#[cfg(test)]
mod tests {
    use kafka_protocol::protocol::StrBytes;

    use super::*;
    use crate::group::LeftMember;
    use crate::protocol::api::{ERROR_NONE, ERROR_UNKNOWN_MEMBER_ID};

    fn left(member_id: &'static str, instance_id: Option<&'static str>, error: i16) -> LeftMember {
        LeftMember {
            member_id: StrBytes::from_static_str(member_id),
            group_instance_id: instance_id.map(StrBytes::from_static_str),
            error,
        }
    }

    fn result(members: Vec<LeftMember>) -> LeaveResult {
        LeaveResult {
            error: ERROR_NONE,
            members,
        }
    }

    #[test]
    fn given_a_member_error_at_v0_should_carry_it_at_top_level() {
        let body =
            encode_response(0, &result(vec![left("m-1", None, ERROR_UNKNOWN_MEMBER_ID)])).unwrap();

        assert_eq!(body.as_ref(), &ERROR_UNKNOWN_MEMBER_ID.to_be_bytes());
    }

    /// The encoder refuses `members` below v3, and that refusal closes the connection.
    #[test]
    fn given_member_results_below_v3_should_encode_without_members() {
        let body = encode_response(2, &result(vec![left("m-1", None, ERROR_NONE)])).unwrap();

        assert_eq!(body.as_ref(), &[0, 0, 0, 0, 0, 0]);
    }

    #[test]
    fn given_a_v1_response_should_carry_throttle_time() {
        let body = encode_error_response(1, ERROR_INVALID_REQUEST).unwrap();

        assert_eq!(body.len(), 6);
        assert_eq!(&body[4..], &ERROR_INVALID_REQUEST.to_be_bytes());
    }

    #[test]
    fn given_a_v5_response_should_echo_each_identity_verbatim() {
        let body = encode_response(
            5,
            &result(vec![
                left("m-1", Some("i-1"), ERROR_NONE),
                left("", None, ERROR_UNKNOWN_MEMBER_ID),
            ]),
        )
        .unwrap();

        let expected: &[u8] = &[
            0x00, 0x00, 0x00, 0x00, // throttle_time_ms
            0x00, 0x00, // error_code
            0x03, // members: 2
            0x04, b'm', b'-', b'1', // member_id
            0x04, b'i', b'-', b'1', // group_instance_id
            0x00, 0x00, // error_code
            0x00, // member tagged fields
            0x01, // member_id: empty
            0x00, // group_instance_id: null
            0x00, 0x19, // error_code
            0x00, // member tagged fields
            0x00, // top-level tagged fields
        ];
        assert_eq!(body.as_ref(), expected);
    }

    #[test]
    fn given_a_request_failure_at_v5_should_answer_with_an_empty_members_array() {
        let body = encode_error_response(5, ERROR_INVALID_REQUEST).unwrap();

        assert_eq!(body.as_ref(), &[0, 0, 0, 0, 0, 42, 0x01, 0x00]);
    }
}
