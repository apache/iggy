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
//! that is not here is `Empty` when it holds offsets in Iggy, as Kafka keeps a group whose
//! consumers left. Otherwise it is `Dead`: v6 also carries `GROUP_ID_NOT_FOUND`, and earlier
//! versions do not, matching Kafka 4.0's `describeGroups`. A v6 client treats error 0 as a real
//! group. While the first scan of the groups that hold offsets runs (`IggyBridge::offset_groups`),
//! a group that is not here gets `COORDINATOR_LOAD_IN_PROGRESS` (14), which clients retry. When no
//! scan could find them, such a group is `Dead`.
//!
//! `include_authorized_operations` is not answered. The field stays at the omitted sentinel
//! (`i32::MIN`): there is no group ACL bitfield to report, and any other value fails the
//! encoder below v3.

use std::sync::Arc;

use bytes::Bytes;
use kafka_protocol::messages::describe_groups_response::{DescribedGroup, DescribedGroupMember};
use kafka_protocol::messages::{DescribeGroupsRequest, DescribeGroupsResponse, GroupId};
use kafka_protocol::protocol::StrBytes;

use crate::bridge::{OffsetGroups, ScannedGroups};
use crate::error::Result;
use crate::group::{DESCRIBE_RESPONSE_TOO_LARGE, GroupDescription, MemberDescription};
use crate::protocol::api::{
    API_KEY_DESCRIBE_GROUPS, ApiVersionRange, ConnectionState, ERROR_COORDINATOR_LOAD_IN_PROGRESS,
    ERROR_GROUP_ID_NOT_FOUND, ERROR_NONE, ERROR_UNKNOWN_SERVER_ERROR, GatewayState, HandleOutcome,
    is_supported_version,
};
use crate::protocol::bounds_guard::validate_describe_groups_shape;
use crate::protocol::dedup::dedup_first_seen;
use crate::protocol::handlers::{
    decode_guarded, encode_message, offset_deadline, respond_or_close, unsupported_version_response,
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

pub async fn handle(
    state: &GatewayState,
    connection: &ConnectionState,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
    if !is_supported_version(API_KEY_DESCRIBE_GROUPS, api_version) {
        // No top-level error field to carry a code on, so this answers with an empty group
        // list rather than a dedicated error response. Versions outside 0-6 fail in the
        // encoder, which is what makes `unsupported_version_response` close the connection
        // instead - a version this encoder accepts is not a version the firewall rejects.
        return unsupported_version_response(API_KEY_DESCRIBE_GROUPS, api_version, |version| {
            encode_response(version, Vec::new())
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

    let requested: Vec<StrBytes> = request
        .groups
        .iter()
        .map(|group_id| group_id.0.clone())
        .collect();
    // One result per distinct id. Repeating one group would otherwise copy its members into the
    // response once per copy, and the shape guard only charges the id strings.
    let group_ids = dedup_first_seen(&requested);
    let described = state
        .groups
        .describe_groups(&group_ids, api_version, state.max_frame_size)
        .await;
    let absent = if described.iter().any(Option::is_none) {
        Absent::look_up(state, connection).await
    } else {
        Absent::Dead
    };
    let groups = group_ids
        .iter()
        .zip(described)
        .map(|(group_id, group)| {
            group.map_or_else(
                || absent.answer(api_version, group_id),
                |group| described_group(api_version, group),
            )
        })
        .collect();
    respond_or_close(encode_response(api_version, groups), "DescribeGroups")
}

/// How one request answers a group that is not on this coordinator.
enum Absent {
    /// Without a bridge no group holds offsets, and with no scan that could find them none is
    /// known to, so each one is `Dead`.
    Dead,
    /// `Empty` for a group that holds offsets, `Dead` for any other.
    Known(Arc<OffsetGroups>),
    /// The first scan of the groups that hold offsets has not ended yet.
    Unknown,
}

impl Absent {
    async fn look_up(state: &GatewayState, connection: &ConnectionState) -> Self {
        let Some(bridge) = &state.bridge else {
            return Self::Dead;
        };
        let deadline = offset_deadline(state, connection).await;
        match bridge.offset_groups(deadline).await {
            ScannedGroups::Found(offset_groups) => Self::Known(offset_groups),
            ScannedGroups::Pending => Self::Unknown,
            ScannedGroups::Failed => Self::Dead,
        }
    }

    fn answer(&self, version: i16, group_id: &StrBytes) -> DescribedGroup {
        match self {
            Self::Known(offset_groups) if offset_groups.contains(group_id.as_str()) => {
                described_group(version, GroupDescription::offsets_only(group_id.clone()))
            }
            Self::Dead | Self::Known(_) => missing_group(version, group_id),
            Self::Unknown => {
                error_group(version, group_id, ERROR_COORDINATOR_LOAD_IN_PROGRESS, None)
            }
        }
    }
}

pub(crate) fn described_group(version: i16, group: GroupDescription) -> DescribedGroup {
    if group.error != ERROR_NONE {
        return error_group(
            version,
            &group.group_id,
            ERROR_UNKNOWN_SERVER_ERROR,
            Some(DESCRIBE_RESPONSE_TOO_LARGE),
        );
    }
    DescribedGroup::default()
        .with_group_id(GroupId(group.group_id))
        .with_group_state(StrBytes::from_static_str(group.state))
        .with_protocol_type(group.protocol_type)
        .with_protocol_data(group.protocol_name.unwrap_or_default())
        .with_members(group.members.into_iter().map(described_member).collect())
}

/// Empty member list and empty strings, matching Kafka's `DescribeGroupsResponse.groupError`.
/// `message` goes out from v6. A group that does not fit the response gets
/// `UNKNOWN_SERVER_ERROR`, which the admin tool does not retry, unlike closing the connection.
/// One that waits for the first scan gets `COORDINATOR_LOAD_IN_PROGRESS`, which it retries.
fn error_group(
    version: i16,
    group_id: &StrBytes,
    code: i16,
    message: Option<&'static str>,
) -> DescribedGroup {
    let group = DescribedGroup::default()
        .with_error_code(code)
        .with_group_id(GroupId(group_id.clone()))
        .with_group_state(StrBytes::from_static_str(""))
        .with_protocol_type(StrBytes::from_static_str(""))
        .with_protocol_data(StrBytes::from_static_str(""));
    if version >= FIRST_ERROR_MESSAGE_VERSION {
        group.with_error_message(message.map(StrBytes::from_static_str))
    } else {
        group
    }
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

pub(crate) fn missing_group(version: i16, group_id: &StrBytes) -> DescribedGroup {
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

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use kafka_protocol::protocol::{Decodable, Encodable};
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::bridge::IggyBridge;
    use crate::group::{GroupCoordinator, GroupCoordinatorConfig};

    /// With Iggy out of reach no scan finds the groups that hold offsets. A group that is not here
    /// is then `Dead`, as without a bridge, and does not wait for a scan that cannot succeed.
    #[tokio::test]
    async fn given_a_scan_that_failed_should_describe_a_missing_group_as_dead() {
        let state = GatewayState::new(
            Some(Arc::new(IggyBridge::offline())),
            8 * 1024 * 1024,
            0,
            GroupCoordinator::new(GroupCoordinatorConfig::default(), CancellationToken::new()),
        );
        let mut body = BytesMut::new();
        DescribeGroupsRequest::default()
            .with_groups(vec![GroupId(StrBytes::from_static_str("orders-app"))])
            .encode(&mut body, 5)
            .unwrap();

        let mut response = handle(&state, &ConnectionState::default(), 5, body.freeze())
            .await
            .expect_response("DescribeGroups answers");
        let response = DescribeGroupsResponse::decode(&mut response, 5).unwrap();

        let group = &response.groups[0];
        assert_eq!(
            (group.error_code, group.group_state.as_str()),
            (ERROR_NONE, DEAD)
        );
    }
}
