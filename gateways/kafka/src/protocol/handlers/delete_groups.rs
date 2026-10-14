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

//! `DeleteGroups` (API key 42).
//!
//! Deletes each named group that has no members here, with its offsets, as
//! `kafka-consumer-groups.sh --delete` asks. Kafka's rules (`ClassicGroup.validateDeleteGroup`):
//! a group with members gets `NON_EMPTY_GROUP` (68), and a group that does not exist gets
//! `GROUP_ID_NOT_FOUND` (69). A group exists here while ids wait to join it, or while it holds
//! offsets in Iggy. The gateway deletes the Iggy group `kafka.cg.<group>` on every Kafka topic,
//! and Iggy deletes the offsets with it (`IggyBridge::delete_offset_groups`).
//!
//! With SASL on, a principal without `manage_topics` gets `GROUP_AUTHORIZATION_FAILED` (30) for
//! every group, and nothing is deleted.
//!
//! An id repeated in one request is answered once, as in `DescribeGroups`. The response has no
//! top-level error, so a request that cannot be decoded closes the connection.

use bytes::Bytes;
use kafka_protocol::messages::delete_groups_response::DeletableGroupResult;
use kafka_protocol::messages::{DeleteGroupsRequest, DeleteGroupsResponse, GroupId};
use kafka_protocol::protocol::StrBytes;

use crate::auth::AuthenticatedPrincipal;
use crate::bridge::{BridgeError, IggyBridge, OffsetCalls};
use crate::error::Result;
use crate::group::{GroupRemoval, is_valid_group_id};
use crate::protocol::api::{
    API_KEY_DELETE_GROUPS, ApiVersionRange, ConnectionState, ERROR_GROUP_AUTHORIZATION_FAILED,
    ERROR_GROUP_ID_NOT_FOUND, ERROR_INVALID_GROUP_ID, ERROR_NON_EMPTY_GROUP, ERROR_NONE,
    ERROR_TOPIC_AUTHORIZATION_FAILED, ERROR_UNKNOWN_SERVER_ERROR, GatewayState, HandleOutcome,
    is_supported_version,
};
use crate::protocol::bounds_guard::validate_delete_groups_shape;
use crate::protocol::dedup::dedup_first_seen;
use crate::protocol::handlers::{
    decode_guarded, encode_message, may_delete_groups, offset_deadline, offset_refusal_cause,
    respond_or_close, unsupported_version_response,
};

/// v3 adds a per-group `error_message`, which `kafka-protocol` cannot encode yet.
pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_DELETE_GROUPS,
    min_version: 0,
    max_version: 2,
};

/// Group id length prefix, error code and tagged fields of one result, past the id itself.
const PER_RESULT_BYTES: usize = 5;

pub async fn handle(
    state: &GatewayState,
    connection: &ConnectionState,
    principal: Option<&AuthenticatedPrincipal>,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
    if !is_supported_version(API_KEY_DELETE_GROUPS, api_version) {
        return unsupported_version_response(API_KEY_DELETE_GROUPS, api_version, |version| {
            encode_response(version, Vec::new())
        });
    }
    let request = match decode_guarded::<DeleteGroupsRequest>(api_version, body, |version, body| {
        validate_delete_groups_shape(version, body, state.max_frame_size)
    }) {
        Ok(request) => request,
        Err(error) => {
            // debug!, not warn!: attacker-controlled, not operator-actionable.
            tracing::debug!(%error, api_version, "Failed to decode DeleteGroups request");
            return HandleOutcome::Close;
        }
    };

    let requested: Vec<StrBytes> = request
        .groups_names
        .iter()
        .map(|group_id| group_id.0.clone())
        .collect();
    let group_ids = dedup_first_seen(&requested);
    let codes = if may_delete_groups(principal, "DeleteGroups") {
        delete(state, connection, &group_ids).await
    } else {
        vec![ERROR_GROUP_AUTHORIZATION_FAILED; group_ids.len()]
    };
    let results = group_ids
        .into_iter()
        .zip(codes)
        .map(|(group_id, code)| {
            DeletableGroupResult::default()
                .with_group_id(GroupId(group_id))
                .with_error_code(code)
        })
        .collect();
    respond_or_close(encode_response(api_version, results), "DeleteGroups")
}

/// One code per group of `group_ids`, in order.
async fn delete(
    state: &GatewayState,
    connection: &ConnectionState,
    group_ids: &[StrBytes],
) -> Vec<i16> {
    let valid: Vec<StrBytes> = group_ids
        .iter()
        .filter(|group_id| is_valid_group_id(group_id))
        .cloned()
        .collect();
    let mut removals = state.groups.remove_empty(&valid).await.into_iter();
    // Per group: its answer, or whether the coordinator held it when its offsets are to go.
    let plans: Vec<core::result::Result<bool, i16>> = group_ids
        .iter()
        .map(|group_id| {
            if !is_valid_group_id(group_id) {
                return Err(ERROR_INVALID_GROUP_ID);
            }
            match removals.next() {
                Some(GroupRemoval::Live) => Err(ERROR_NON_EMPTY_GROUP),
                removal => Ok(removal == Some(GroupRemoval::Removed)),
            }
        })
        .collect();
    let deletable: Vec<&str> = group_ids
        .iter()
        .zip(&plans)
        .filter(|(_, plan)| plan.is_ok())
        .map(|(group_id, _)| group_id.as_str())
        .collect();
    let mut deleted = match &state.bridge {
        Some(bridge) if !deletable.is_empty() => {
            let calls = OffsetCalls::new(offset_deadline(state, connection).await);
            delete_offsets(bridge, &deletable, &calls).await
        }
        _ => Vec::new(),
    }
    .into_iter();
    plans
        .into_iter()
        .map(|plan| match plan {
            Err(code) => code,
            Ok(held) => match deleted.next().unwrap_or(Ok(false)) {
                Ok(found) if found || held => ERROR_NONE,
                Ok(_) => ERROR_GROUP_ID_NOT_FOUND,
                Err(code) => code,
            },
        })
        .collect()
}

/// Deletes the offsets of each group of `groups` in Iggy. Whether any topic held the group, or
/// the code of the failure. A listing failure goes on every group.
async fn delete_offsets(
    bridge: &IggyBridge,
    groups: &[&str],
    calls: &OffsetCalls,
) -> Vec<core::result::Result<bool, i16>> {
    let results = match bridge.delete_offset_groups(groups, calls).await {
        Ok(results) => results,
        Err(error) => {
            log_refusal(groups.first().copied().unwrap_or_default(), &error);
            let code = deletion_error_code(&error);
            return groups.iter().map(|_| Err(code)).collect();
        }
    };
    let mut logged = false;
    groups
        .iter()
        .zip(results)
        .map(|(group, result)| {
            result.map_err(|error| {
                let code = deletion_error_code(&error);
                // One line per request, as in `OffsetCommit`.
                if code == ERROR_UNKNOWN_SERVER_ERROR && !logged {
                    logged = true;
                    log_refusal(group, &error);
                }
                code
            })
        })
        .collect()
}

/// [`BridgeError::to_offset_error_code`], with the group's code where Iggy refused the bridge
/// user: the request deletes a group, not a topic.
const fn deletion_error_code(error: &BridgeError) -> i16 {
    match error.to_offset_error_code() {
        ERROR_TOPIC_AUTHORIZATION_FAILED => ERROR_GROUP_AUTHORIZATION_FAILED,
        code => code,
    }
}

fn log_refusal(group: &str, error: &BridgeError) {
    if error.to_offset_error_code() != ERROR_UNKNOWN_SERVER_ERROR {
        return;
    }
    tracing::error!(
        group,
        %error,
        cause = offset_refusal_cause(error),
        "Iggy refused to delete the offsets of a Kafka group"
    );
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_response(version: i16, results: Vec<DeletableGroupResult>) -> Result<Bytes> {
    let echoed: usize = results
        .iter()
        .map(|result| result.group_id.0.len() + PER_RESULT_BYTES)
        .sum();
    encode_message(
        &DeleteGroupsResponse::default().with_results(results),
        version,
        16 + echoed,
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::BytesMut;
    use kafka_protocol::protocol::{Decodable, Encodable};
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::group::{GroupCoordinator, GroupCoordinatorConfig, JoinRequest};
    use crate::protocol::acl::PrincipalPermissions;
    use crate::protocol::api::ERROR_MEMBER_ID_REQUIRED;

    /// No bridge, and a first join that completes at once.
    fn state() -> GatewayState {
        let groups = GroupCoordinatorConfig {
            initial_rebalance_delay: Duration::ZERO,
            ..GroupCoordinatorConfig::default()
        };
        GatewayState::new(
            None,
            8 * 1024 * 1024,
            0,
            GroupCoordinator::new(groups, CancellationToken::new()),
        )
    }

    fn join_request(group: &str, require_known_member_id: bool) -> JoinRequest {
        JoinRequest {
            group_id: StrBytes::from_string(group.to_owned()),
            session_timeout: Duration::from_secs(10),
            rebalance_timeout: Duration::from_secs(10),
            member_id: StrBytes::new(),
            group_instance_id: None,
            protocol_type: StrBytes::from_static_str("consumer"),
            protocols: vec![(StrBytes::from_static_str("range"), Bytes::new())],
            require_known_member_id,
            supports_skip_assignment: false,
        }
    }

    fn body(version: i16, groups: &[&str]) -> Bytes {
        let request = DeleteGroupsRequest::default().with_groups_names(
            groups
                .iter()
                .map(|group| GroupId(StrBytes::from_string((*group).to_owned())))
                .collect(),
        );
        let mut buf = BytesMut::new();
        request.encode(&mut buf, version).unwrap();
        buf.freeze()
    }

    fn principal(manage_topics: bool, permissions_known: bool) -> AuthenticatedPrincipal {
        AuthenticatedPrincipal {
            username: "alice".to_string(),
            permissions: PrincipalPermissions {
                manage_topics,
                ..PrincipalPermissions::default()
            },
            permissions_known,
        }
    }

    async fn codes(
        state: &GatewayState,
        principal: Option<&AuthenticatedPrincipal>,
        version: i16,
        groups: &[&str],
    ) -> Vec<(String, i16)> {
        let connection = ConnectionState::default();
        let mut response = handle(
            state,
            &connection,
            principal,
            version,
            body(version, groups),
        )
        .await
        .expect_response("DeleteGroups answers");
        let response = DeleteGroupsResponse::decode(&mut response, version).unwrap();
        response
            .results
            .iter()
            .map(|result| (result.group_id.0.to_string(), result.error_code))
            .collect()
    }

    /// Without a bridge no group holds offsets, so only the coordinator's groups exist.
    #[tokio::test]
    async fn given_each_kind_of_group_without_a_bridge_should_answer_as_kafka() {
        for version in 0..=2 {
            let state = state();
            let joined = state.groups.join(&join_request("live", false)).await;
            assert_eq!(joined.error, ERROR_NONE);
            let pending = state.groups.join(&join_request("waiting", true)).await;
            assert_eq!(pending.error, ERROR_MEMBER_ID_REQUIRED);
            let too_long = "g".repeat(crate::group::MAX_GROUP_ID_BYTES + 1);

            assert_eq!(
                codes(
                    &state,
                    None,
                    version,
                    &["live", "waiting", "unknown", "", &too_long, "unknown"],
                )
                .await,
                vec![
                    ("live".to_string(), ERROR_NON_EMPTY_GROUP),
                    ("waiting".to_string(), ERROR_NONE),
                    ("unknown".to_string(), ERROR_GROUP_ID_NOT_FOUND),
                    (String::new(), ERROR_INVALID_GROUP_ID),
                    (too_long.clone(), ERROR_INVALID_GROUP_ID),
                ],
                "v{version}: a repeated id is answered once"
            );
            assert_eq!(
                codes(&state, None, version, &["waiting"]).await,
                vec![("waiting".to_string(), ERROR_GROUP_ID_NOT_FOUND)],
                "v{version}: the deleted group is gone"
            );
        }
    }

    /// Kafka asks for `DELETE` on each group, which needs `manage_topics` here.
    #[tokio::test]
    async fn given_a_principal_without_manage_topics_should_refuse_every_group() {
        let state = state();
        let pending = state.groups.join(&join_request("waiting", true)).await;
        assert_eq!(pending.error, ERROR_MEMBER_ID_REQUIRED);

        for refused in [principal(false, true), principal(true, false)] {
            assert_eq!(
                codes(&state, Some(&refused), 2, &["waiting", ""]).await,
                vec![
                    ("waiting".to_string(), ERROR_GROUP_AUTHORIZATION_FAILED),
                    (String::new(), ERROR_GROUP_AUTHORIZATION_FAILED),
                ],
                "known={}",
                refused.permissions_known
            );
        }
        assert_eq!(
            codes(&state, Some(&principal(true, true)), 2, &["waiting"]).await,
            vec![("waiting".to_string(), ERROR_NONE)],
            "a refused request deletes nothing"
        );
    }

    /// The response has no top-level error, so an unreadable request has no answer.
    #[tokio::test]
    async fn given_an_unreadable_request_should_close_the_connection() {
        let state = state();
        let connection = ConnectionState::default();

        let truncated = body(2, &["orders-app"]).slice(..3);
        assert!(
            handle(&state, &connection, None, 2, truncated)
                .await
                .is_close()
        );
        assert!(
            handle(&state, &connection, None, 3, body(2, &["orders-app"]))
                .await
                .is_close()
        );
    }
}
