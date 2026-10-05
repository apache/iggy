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

//! `DescribeGroups` (API key 15).
//!
//! Reports the members, assignment and state of groups this coordinator holds. A named group
//! that is not here is `Dead`: v6 also carries `GROUP_ID_NOT_FOUND`, and earlier versions do
//! not, matching Kafka 4.0's `describeGroups`. A v6 client treats error 0 as a real group.
//!
//! `include_authorized_operations` is not answered. The field stays at the omitted sentinel
//! (`i32::MIN`): there is no group ACL bitfield to report, and any other value fails the
//! encoder below v3.

use bytes::Bytes;
use kafka_protocol::messages::describe_groups_response::{DescribedGroup, DescribedGroupMember};
use kafka_protocol::messages::{DescribeGroupsRequest, DescribeGroupsResponse, GroupId};
use kafka_protocol::protocol::StrBytes;

use crate::error::Result;
use crate::group::{GroupDescription, MemberDescription};
use crate::protocol::api::{
    API_KEY_DESCRIBE_GROUPS, ApiVersionRange, ERROR_GROUP_ID_NOT_FOUND, ERROR_UNSUPPORTED_VERSION,
    GatewayState, HandleOutcome, is_supported_version,
};
use crate::protocol::bounds_guard::validate_describe_groups_shape;
use crate::protocol::handlers::{
    decode_guarded, encode_message, respond_or_close, unsupported_version_response,
};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_DESCRIBE_GROUPS,
    min_version: 0,
    max_version: 6,
};

/// Kafka's `Dead` state. This coordinator never stores it: an absent group is reported as one.
const DEAD: &str = "Dead";
/// First version whose response has `error_message`, and the first on which Kafka 4.0 sends
/// `GROUP_ID_NOT_FOUND` for a missing group.
const FIRST_ERROR_MESSAGE_VERSION: i16 = 6;

pub async fn handle(state: &GatewayState, api_version: i16, body: Bytes) -> HandleOutcome {
    if !is_supported_version(API_KEY_DESCRIBE_GROUPS, api_version) {
        return unsupported_version_response(API_KEY_DESCRIBE_GROUPS, api_version, |version| {
            encode_error_response(version, ERROR_UNSUPPORTED_VERSION)
        });
    }
    let request =
        match decode_guarded::<DescribeGroupsRequest>(api_version, body, |version, body| {
            validate_describe_groups_shape(version, body, state.max_frame_size)
        }) {
            Ok(request) => request,
            Err(error) => {
                // debug!, not warn!: attacker-controlled, not operator-actionable.
                // The response has no top-level error field, and a failed decode yields no group
                // id to hang `INVALID_REQUEST` on. An empty group list would look like success.
                tracing::debug!(%error, api_version, "Failed to decode DescribeGroups request");
                return HandleOutcome::Close;
            }
        };

    let group_ids: Vec<StrBytes> = request
        .groups
        .iter()
        .map(|group_id| group_id.0.clone())
        .collect();
    let described = state.groups.describe_groups(&group_ids).await;
    let groups = group_ids
        .iter()
        .zip(described)
        .map(|(group_id, group)| {
            group.map_or_else(|| missing_group(api_version, group_id), described_group)
        })
        .collect();
    respond_or_close(encode_response(api_version, groups), "DescribeGroups")
}

fn described_group(group: GroupDescription) -> DescribedGroup {
    DescribedGroup::default()
        .with_group_id(GroupId(group.group_id))
        .with_group_state(StrBytes::from_static_str(group.state))
        .with_protocol_type(group.protocol_type)
        .with_protocol_data(group.protocol_name.unwrap_or_default())
        .with_members(group.members.into_iter().map(described_member).collect())
}

fn described_member(member: MemberDescription) -> DescribedGroupMember {
    // `client_id` and `client_host` stay empty: the coordinator does not retain the request
    // header's client id, and handlers are not given the connection's peer address.
    DescribedGroupMember::default()
        .with_member_id(member.member_id)
        .with_group_instance_id(member.group_instance_id)
        .with_member_metadata(member.metadata)
        .with_member_assignment(member.assignment)
}

fn missing_group(version: i16, group_id: &StrBytes) -> DescribedGroup {
    let mut group = DescribedGroup::default()
        .with_group_id(GroupId(group_id.clone()))
        .with_group_state(StrBytes::from_static_str(DEAD));
    if version >= FIRST_ERROR_MESSAGE_VERSION {
        group = group
            .with_error_code(ERROR_GROUP_ID_NOT_FOUND)
            .with_error_message(Some(StrBytes::from_string(format!(
                "Group {} not found.",
                group_id.as_str()
            ))));
    }
    group
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_response(version: i16, groups: Vec<DescribedGroup>) -> Result<Bytes> {
    let response = DescribeGroupsResponse::default().with_groups(groups);
    encode_message(&response, version, 64)
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_error_response(version: i16, _error_code: i16) -> Result<Bytes> {
    // No top-level error field. Versions outside 0-6 fail in the encoder, which is what makes
    // `unsupported_version_response` close the connection. A version this encoder accepts is
    // not a version the firewall rejects.
    encode_response(version, Vec::new())
}
