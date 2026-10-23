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
//! Lists the groups this coordinator holds, and as `Empty` each group with no members here that
//! holds offsets in Iggy. Kafka keeps a group whose consumers left while it has offsets. v4+
//! `states_filter` and v5 `types_filter` keep a group only when the filter is empty or one entry
//! matches, case-insensitively, after trim. A filter value this coordinator does not use matches
//! nothing, so the group is left out and the response error stays 0. Kafka 4.0's `listGroups`
//! does the same ("if invalid, no groups are returned").
//!
//! The groups that hold offsets come from a scan that costs one Iggy call per topic, and one scan
//! answers for a while (`IggyBridge::offset_groups`). A filter that leaves out `Empty` or
//! `classic` needs no scan. While the first scan runs, the answer is
//! `COORDINATOR_LOAD_IN_PROGRESS` (14), which clients retry. When no scan could find the groups,
//! the answer has only the groups this coordinator holds, as before the gateway knew of them.

use std::collections::HashSet;

use bytes::Bytes;
use kafka_protocol::messages::list_groups_response::ListedGroup;
use kafka_protocol::messages::{GroupId, ListGroupsRequest, ListGroupsResponse};
use kafka_protocol::protocol::StrBytes;

use crate::bridge::{OffsetGroups, ScannedGroups};
use crate::error::Result;
use crate::group::{GROUP_STATE_EMPTY, GroupListing};
use crate::protocol::api::{
    API_KEY_LIST_GROUPS, ApiVersionRange, ConnectionState, ERROR_COORDINATOR_LOAD_IN_PROGRESS,
    ERROR_INVALID_REQUEST, ERROR_UNSUPPORTED_VERSION, GatewayState, HandleOutcome,
    is_supported_version,
};
use crate::protocol::bounds_guard::validate_list_groups_shape;
use crate::protocol::handlers::{
    decode_guarded, encode_message, offset_deadline, respond_or_close, unsupported_version_response,
};
use crate::protocol::header::response_header_version;
use crate::protocol::wire_len::{kafka_compact_prefix, kafka_string_len};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_LIST_GROUPS,
    min_version: 0,
    max_version: 5,
};

/// `ListGroups` v5 `group_type` for the classic protocol this coordinator runs.
const CLASSIC_GROUP_TYPE: &str = "classic";

pub async fn handle(
    state: &GatewayState,
    connection: &ConnectionState,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
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

    let mut listings = state.groups.list_groups().await;
    let lists_offsets_only = selected(&request.states_filter, GROUP_STATE_EMPTY)
        && selected(&request.types_filter, CLASSIC_GROUP_TYPE);
    if lists_offsets_only && let Some(bridge) = &state.bridge {
        let deadline = offset_deadline(state, connection).await;
        match bridge.offset_groups(deadline).await {
            ScannedGroups::Found(offset_groups) => add_offsets_only(&mut listings, &offset_groups),
            ScannedGroups::Pending => {
                tracing::debug!(
                    "ListGroups waits for the first scan of the groups that hold offsets"
                );
                return respond_or_close(
                    encode_error_response(api_version, ERROR_COORDINATOR_LOAD_IN_PROGRESS),
                    "ListGroups",
                );
            }
            // The scan logged why. The groups this coordinator holds still answer.
            ScannedGroups::Failed => {}
        }
    }
    let listings: Vec<GroupListing> = listings
        .into_iter()
        .filter(|group| {
            selected(&request.states_filter, group.state)
                && selected(&request.types_filter, CLASSIC_GROUP_TYPE)
        })
        .collect();
    // `protocol_type` is echoed for every group. The shape guard never sees that list, so a
    // coordinator holding many long types can still outgrow the frame - truncate rather than
    // close, since this is server-sized state, not a request the client chose (same approach as
    // `metadata::truncate_all_topics_to_frame_budget` for the identical problem).
    let listings = truncate_listings_to_frame_budget(api_version, listings, state.max_frame_size);
    let groups = listings.into_iter().map(listed_group).collect();
    respond_or_close(encode_response(api_version, groups), "ListGroups")
}

fn selected(filter: &[StrBytes], value: &str) -> bool {
    filter.is_empty()
        || filter
            .iter()
            .any(|wanted| wanted.as_str().trim().eq_ignore_ascii_case(value))
}

/// Adds each group of `offset_groups` that is not live here, as `Empty`. Keeps the list in group
/// id order.
fn add_offsets_only(listings: &mut Vec<GroupListing>, offset_groups: &OffsetGroups) {
    let live: HashSet<&str> = listings
        .iter()
        .map(|group| group.group_id.as_str())
        .collect();
    let offsets_only: Vec<GroupListing> = offset_groups
        .iter()
        .filter(|group_id| !live.contains(group_id))
        .map(|group_id| GroupListing::offsets_only(StrBytes::from_string(group_id.to_owned())))
        .collect();
    listings.extend(offsets_only);
    listings.sort_by(|left, right| left.group_id.cmp(&right.group_id));
}

/// Keeps as many listed groups as fit `max_frame_size`, dropping the rest rather than closing
/// the connection - same approach as `metadata::truncate_all_topics_to_frame_budget` for the
/// identical problem: this coordinator's own state, not a request the client chose, is what
/// grew the response past the budget.
fn truncate_listings_to_frame_budget(
    version: i16,
    mut listings: Vec<GroupListing>,
    max_frame_size: usize,
) -> Vec<GroupListing> {
    let mut cumulative_bytes = list_response_frame_len(version, &[]);
    let mut keep = listings.len();
    for (index, group) in listings.iter().enumerate() {
        let next = cumulative_bytes + listed_group_len(version, group);
        if next > max_frame_size {
            keep = index;
            break;
        }
        cumulative_bytes = next;
    }
    if keep < listings.len() {
        tracing::warn!(
            total_groups = listings.len(),
            kept_groups = keep,
            max_frame_size,
            "ListGroups response would exceed max_frame_size; truncating rather than closing \
             the connection"
        );
        listings.truncate(keep);
    }
    listings
}

/// Header plus body, the length `send_response` writes and `read_frame` compares to
/// `max_frame_size`.
fn list_response_frame_len(version: i16, groups: &[GroupListing]) -> usize {
    let flexible = version >= 3;
    let mut total = if response_header_version(API_KEY_LIST_GROUPS, version) >= 1 {
        5
    } else {
        4
    };
    if version >= 1 {
        total += 4;
    }
    total += 2;
    total += if flexible {
        kafka_compact_prefix(groups.len())
    } else {
        4
    };
    for group in groups {
        total = total.saturating_add(listed_group_len(version, group));
    }
    if flexible {
        total += 1;
    }
    total
}

fn listed_group_len(version: i16, group: &GroupListing) -> usize {
    let flexible = version >= 3;
    let mut total = kafka_string_len(flexible, group.group_id.len());
    total += kafka_string_len(flexible, group.protocol_type.len());
    if version >= 4 {
        total += kafka_string_len(true, group.state.len());
    }
    if version >= 5 {
        total += kafka_string_len(true, CLASSIC_GROUP_TYPE.len());
    }
    if flexible {
        total += 1;
    }
    total
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::{Bytes, BytesMut};
    use kafka_protocol::messages::{ListGroupsRequest, ListGroupsResponse};
    use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
    use tokio_util::sync::CancellationToken;

    use super::{
        API_KEY_LIST_GROUPS, GroupListing, encode_response, handle, list_response_frame_len,
        listed_group,
    };
    use crate::bridge::IggyBridge;
    use crate::group::{
        CONSUMER_PROTOCOL_TYPE, GroupCoordinator, GroupCoordinatorConfig, JoinRequest,
    };
    use crate::protocol::api::{ConnectionState, ERROR_NONE, GatewayState};
    use crate::protocol::header::response_header_version;

    /// With Iggy out of reach no scan finds the groups that hold offsets. The groups this
    /// coordinator holds still answer, as they did before the gateway knew of the others.
    #[tokio::test]
    async fn given_a_scan_that_failed_should_list_the_groups_of_this_coordinator() {
        let groups = GroupCoordinatorConfig {
            initial_rebalance_delay: Duration::ZERO,
            ..GroupCoordinatorConfig::default()
        };
        let state = GatewayState::new(
            Some(Arc::new(IggyBridge::offline())),
            8 * 1024 * 1024,
            0,
            GroupCoordinator::new(groups, CancellationToken::new()),
        );
        let joined = state
            .groups
            .join(&JoinRequest {
                group_id: StrBytes::from_static_str("orders-app"),
                session_timeout: Duration::from_secs(10),
                rebalance_timeout: Duration::from_secs(10),
                member_id: StrBytes::new(),
                group_instance_id: None,
                protocol_type: StrBytes::from_static_str(CONSUMER_PROTOCOL_TYPE),
                protocols: vec![(StrBytes::from_static_str("range"), Bytes::new())],
                require_known_member_id: false,
                supports_skip_assignment: false,
            })
            .await;
        assert_eq!(joined.error, ERROR_NONE);
        let mut body = BytesMut::new();
        ListGroupsRequest::default().encode(&mut body, 5).unwrap();

        let mut response = handle(&state, &ConnectionState::default(), 5, body.freeze())
            .await
            .expect_response("ListGroups answers");
        let response = ListGroupsResponse::decode(&mut response, 5).unwrap();

        assert_eq!(response.error_code, ERROR_NONE);
        let listed: Vec<&str> = response
            .groups
            .iter()
            .map(|group| group.group_id.0.as_str())
            .collect();
        assert_eq!(listed, ["orders-app"]);
    }

    /// v0-v2 header is 4 bytes, v3+ is 5. v4 adds the state string and v3+ adds a tag byte.
    /// Group id and protocol type sit past the compact-varint boundary so a 1-byte length
    /// prefix cannot accidentally match.
    #[test]
    fn given_listings_when_pricing_v0_through_v5_should_match_the_encoder() {
        let group_id = "g".repeat(200);
        let protocol_type = "p".repeat(200);
        let listings = vec![
            GroupListing {
                group_id: StrBytes::from_string(group_id.clone()),
                protocol_type: StrBytes::from_string(protocol_type.clone()),
                state: "CompletingRebalance",
            },
            GroupListing {
                group_id: StrBytes::from_string(format!("!{group_id}")),
                protocol_type: StrBytes::from_string(protocol_type),
                state: "Empty",
            },
        ];
        for version in 0..=5 {
            let priced = list_response_frame_len(version, &listings);
            let encoded = encode_response(
                version,
                listings.iter().cloned().map(listed_group).collect(),
            )
            .expect("list encodes");
            let header = if response_header_version(API_KEY_LIST_GROUPS, version) >= 1 {
                5
            } else {
                4
            };
            assert_eq!(
                priced,
                header + encoded.len(),
                "version {version} priced {priced} encoded {}",
                header + encoded.len()
            );
        }
    }
}
