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
//! and Iggy deletes the offsets with it (`IggyBridge::delete_offset_groups`). The ids that wait to
//! join a group go only once its offsets did, so a deletion that fails changes nothing, as in
//! Kafka.
//!
//! While the deletion runs, the group takes no join and no commit: both get
//! `COORDINATOR_LOAD_IN_PROGRESS` (14), which clients retry. The deletion waits for each commit
//! that passed its check before the deletion began, and answers 14 when one still runs at the
//! deadline: 25 s on a connection where no member heartbeats, as an admin client's, else the
//! offset deadline. A commit or a deletion runs until none of its Iggy calls can land, also after
//! its request stopped waiting. A deletion that does not end by the deadline answers 14 too. Some
//! topics can then hold no offsets of the group while others still do, and a retry deletes the
//! rest. A retry that finds no offsets of a group whose deletion ended meanwhile answers 0. A
//! member on another gateway is not seen here, so a group whose members all use another gateway
//! is deleted (`docs/OFFSET_STORAGE.md`).
//!
//! When a listener names principals, a principal without a global `manage_topics` or
//! `manage_streams` gets `GROUP_AUTHORIZATION_FAILED` (30) for every group, and nothing is
//! deleted. Each group to delete also gets 30, and nothing is deleted, when the bridge user may
//! not list the Iggy groups of a Kafka topic: a group can hold offsets there.
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
    API_KEY_DELETE_GROUPS, ApiVersionRange, ConnectionState, ERROR_COORDINATOR_LOAD_IN_PROGRESS,
    ERROR_GROUP_AUTHORIZATION_FAILED, ERROR_GROUP_ID_NOT_FOUND, ERROR_INVALID_GROUP_ID,
    ERROR_NON_EMPTY_GROUP, ERROR_NONE, ERROR_TOPIC_AUTHORIZATION_FAILED,
    ERROR_UNKNOWN_SERVER_ERROR, GatewayState, HandleOutcome, is_supported_version,
};
use crate::protocol::bounds_guard::validate_delete_groups_shape;
use crate::protocol::dedup::dedup_first_seen;
use crate::protocol::handlers::{
    decode_guarded, deletion_deadline, encode_message, may_delete_groups, offset_refusal_cause,
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

/// What one group of a request gets before the Iggy step.
#[derive(Clone, Copy)]
enum Plan {
    /// This answer, and nothing to delete.
    Answer(i16),
    /// Its offsets go. `held`: this coordinator holds the group, so it exists even when no topic
    /// holds its offsets.
    Delete { held: bool },
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
    // Until this drops, the groups whose offsets go take no join and no commit.
    let (removals, deletion) = state.groups.group_deletion(&valid).await;
    let mut removals = removals.into_iter();
    let plans: Vec<Plan> = group_ids
        .iter()
        .map(|group_id| {
            if !is_valid_group_id(group_id) {
                return Plan::Answer(ERROR_INVALID_GROUP_ID);
            }
            match removals.next() {
                Some(GroupRemoval::Live) => Plan::Answer(ERROR_NON_EMPTY_GROUP),
                removal => Plan::Delete {
                    held: removal == Some(GroupRemoval::Empty),
                },
            }
        })
        .collect();
    // The index in `group_ids` of each group whose offsets go.
    let deletable: Vec<usize> = plans
        .iter()
        .enumerate()
        .filter(|(_, plan)| matches!(plan, Plan::Delete { .. }))
        .map(|(at, _)| at)
        .collect();
    let names: Vec<&str> = deletable.iter().map(|&at| group_ids[at].as_str()).collect();
    let mut codes: Vec<i16> = plans
        .iter()
        .map(|plan| match plan {
            Plan::Answer(code) => *code,
            Plan::Delete { .. } => ERROR_NONE,
        })
        .collect();
    let found = match &state.bridge {
        Some(bridge) if !names.is_empty() => {
            let deadline = deletion_deadline(state, connection).await;
            if deletion.commits_ended(deadline).await {
                // Each delete keeps the groups fenced until it can no longer land, also past the
                // deadline, so no commit lands while a delete can still land.
                let calls = OffsetCalls::new(deadline).keeping(deletion);
                let found = delete_offsets(bridge, &names, &calls).await;
                // Still fenced by `calls`, so no member joins before the removal.
                remove_emptied(state, group_ids, &plans, &deletable, &found).await;
                found
            } else {
                vec![Err(ERROR_COORDINATOR_LOAD_IN_PROGRESS); names.len()]
            }
        }
        // Without a bridge no group holds offsets.
        _ => {
            let found = vec![Ok(false); names.len()];
            remove_emptied(state, group_ids, &plans, &deletable, &found).await;
            found
        }
    };
    for (&at, found) in deletable.iter().zip(found) {
        let Plan::Delete { held } = plans[at] else {
            continue;
        };
        codes[at] = match found {
            Ok(found) if found || held => ERROR_NONE,
            Ok(_) => ERROR_GROUP_ID_NOT_FOUND,
            Err(code) => code,
        };
    }
    codes
}

/// Removes from the coordinator each group of `deletable` that it holds and whose offsets went,
/// with the ids that wait to join it. `found` has one result per group of `deletable`.
async fn remove_emptied(
    state: &GatewayState,
    group_ids: &[StrBytes],
    plans: &[Plan],
    deletable: &[usize],
    found: &[core::result::Result<bool, i16>],
) {
    let emptied: Vec<StrBytes> = deletable
        .iter()
        .zip(found)
        .filter(|&(&at, found)| matches!(plans[at], Plan::Delete { held: true }) && found.is_ok())
        .map(|(&at, _)| group_ids[at].clone())
        .collect();
    if !emptied.is_empty() {
        state.groups.remove_empty(&emptied).await;
    }
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
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::BytesMut;
    use kafka_protocol::protocol::{Decodable, Encodable};
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::group::{CommitRequest, GroupCoordinator, GroupCoordinatorConfig, JoinRequest};
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

    /// A commit that passed its check before the deletion began must land whole before it. So
    /// the deletion waits for it, and answers 14 while it still runs at the deadline.
    #[tokio::test]
    async fn given_a_commit_that_runs_past_the_deadline_should_answer_coordinator_load_in_progress()
    {
        let groups = GroupCoordinatorConfig {
            deletion_hold: Duration::from_millis(50),
            ..GroupCoordinatorConfig::default()
        };
        let state = GatewayState::new(
            Some(Arc::new(IggyBridge::offline())),
            8 * 1024 * 1024,
            0,
            GroupCoordinator::new(groups, CancellationToken::new()),
        );
        let commit = CommitRequest {
            group_id: StrBytes::from_static_str("orders-app"),
            generation_id: -1,
            member_id: StrBytes::new(),
            group_instance_id: None,
        };
        let _running = state
            .groups
            .validate_commit(&commit)
            .await
            .expect("a commit outside the group protocol passes");

        assert_eq!(
            codes(&state, None, 2, &["orders-app"]).await,
            vec![("orders-app".to_string(), ERROR_COORDINATOR_LOAD_IN_PROGRESS)]
        );
    }

    /// Kafka changes nothing on a `DeleteGroups` that fails. With Iggy out of reach, a group whose
    /// ids wait to join keeps them, so the client that holds one still joins.
    #[tokio::test]
    async fn given_a_deletion_that_fails_should_keep_the_ids_that_wait_to_join() {
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
        let pending = state.groups.join(&join_request("waiting", true)).await;
        assert_eq!(pending.error, ERROR_MEMBER_ID_REQUIRED);

        let answered = codes(&state, None, 2, &["waiting"]).await;
        assert_ne!(answered[0].1, ERROR_NONE, "Iggy is out of reach");

        let claim = JoinRequest {
            member_id: pending.member_id,
            ..join_request("waiting", true)
        };
        assert_eq!(
            state.groups.join(&claim).await.error,
            ERROR_NONE,
            "the id still joins"
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
