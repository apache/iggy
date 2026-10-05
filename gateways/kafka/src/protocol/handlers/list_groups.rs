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

//! `ListGroups` (API key 16).
//!
//! Lists the groups this coordinator holds. v4+ `states_filter` and v5 `types_filter` keep a
//! group only when the filter is empty or one entry matches, case-insensitively, after trim.
//! A filter value this coordinator does not use matches nothing, so the group is left out and
//! the response error stays 0. Kafka 4.0's `listGroups` does the same ("if invalid, no groups
//! are returned").

use bytes::Bytes;
use kafka_protocol::messages::list_groups_response::ListedGroup;
use kafka_protocol::messages::{GroupId, ListGroupsRequest, ListGroupsResponse};
use kafka_protocol::protocol::StrBytes;

use crate::error::Result;
use crate::group::GroupListing;
use crate::protocol::api::{
    API_KEY_LIST_GROUPS, ApiVersionRange, ERROR_INVALID_REQUEST, ERROR_UNSUPPORTED_VERSION,
    GatewayState, HandleOutcome, is_supported_version,
};
use crate::protocol::bounds_guard::validate_list_groups_shape;
use crate::protocol::handlers::{
    decode_guarded, encode_message, respond_or_close, unsupported_version_response,
};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_LIST_GROUPS,
    min_version: 0,
    max_version: 5,
};

/// `ListGroups` v5 `group_type` for the classic protocol this coordinator runs.
const CLASSIC_GROUP_TYPE: &str = "classic";

pub async fn handle(state: &GatewayState, api_version: i16, body: Bytes) -> HandleOutcome {
    if !is_supported_version(API_KEY_LIST_GROUPS, api_version) {
        return unsupported_version_response(API_KEY_LIST_GROUPS, api_version, |version| {
            encode_error_response(version, ERROR_UNSUPPORTED_VERSION)
        });
    }
    let request = match decode_guarded::<ListGroupsRequest>(api_version, body, |version, body| {
        validate_list_groups_shape(version, body, state.max_frame_size)
    }) {
        Ok(request) => request,
        Err(error) => {
            // debug!, not warn!: attacker-controlled, not operator-actionable.
            tracing::debug!(%error, api_version, "Failed to decode ListGroups request");
            return respond_or_close(
                encode_error_response(api_version, ERROR_INVALID_REQUEST),
                "ListGroups",
            );
        }
    };

    let groups = state
        .groups
        .list_groups()
        .await
        .into_iter()
        .filter(|group| {
            selected(&request.states_filter, group.state)
                && selected(&request.types_filter, CLASSIC_GROUP_TYPE)
        })
        .map(listed_group)
        .collect();
    respond_or_close(encode_response(api_version, groups), "ListGroups")
}

fn selected(filter: &[StrBytes], value: &str) -> bool {
    filter.is_empty()
        || filter
            .iter()
            .any(|wanted| wanted.as_str().trim().eq_ignore_ascii_case(value))
}

fn listed_group(group: GroupListing) -> ListedGroup {
    ListedGroup::default()
        .with_group_id(GroupId(group.group_id))
        .with_protocol_type(group.protocol_type)
        .with_group_state(StrBytes::from_static_str(group.state))
        .with_group_type(StrBytes::from_static_str(CLASSIC_GROUP_TYPE))
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_response(version: i16, groups: Vec<ListedGroup>) -> Result<Bytes> {
    let response = ListGroupsResponse::default().with_groups(groups);
    encode_message(&response, version, 64)
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    let response = ListGroupsResponse::default().with_error_code(error_code);
    encode_message(&response, version, 16)
}
