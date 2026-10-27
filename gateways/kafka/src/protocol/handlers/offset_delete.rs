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

//! `OffsetDelete` (API key 47).
//!
//! Deletes committed offsets of one group, as `kafka-consumer-groups.sh --delete-offsets` asks.
//! Kafka's rules (`ClassicGroup.validateOffsetDelete`, `OffsetMetadataManager.deleteOffsets`):
//!
//! - A group that has no members here and holds no offsets in Iggy gets `GROUP_ID_NOT_FOUND` (69).
//! - A group with members of a protocol other than `consumer` gets `NON_EMPTY_GROUP` (68).
//! - Each partition of a topic that a member subscribes to gets `GROUP_SUBSCRIBED_TO_TOPIC` (86).
//! - A partition that does not exist gets `UNKNOWN_TOPIC_OR_PARTITION` (3).
//! - Any other partition gets 0, also one that held no offset.
//!
//! A group error leaves the topics out. A deletion that a retry can fix answers
//! `COORDINATOR_LOAD_IN_PROGRESS` (14) for the whole group, which the Java admin client retries.
//! Deleting an offset twice does no harm. v1 is not in a Kafka release, so only v0 is advertised.
//!
//! A group with no members here exists while it holds offsets. Unless the last scan of the groups
//! that hold offsets found it (`IggyBridge::offset_groups`), the gateway asks each requested topic
//! whether it holds the group. That also finds a group that another gateway committed for since
//! the scan. Only a group that no requested topic holds waits for a scan, which tells whether it
//! holds offsets on another topic.
//!
//! While the deletion runs, the group takes no join and no commit: both get 14, which clients
//! retry. So no member subscribes to a topic between the check and the deletions, and a commit
//! lands whole before or after them. The deletions wait for each commit that passed its check
//! before them, and the group gets 14 when one still runs at the deadline, which is as in
//! `DeleteGroups`. A commit or a deletion runs until none of its Iggy calls can land, also after
//! its request stopped waiting. A member on another gateway is not seen here, so its topics count
//! as unsubscribed (`docs/OFFSET_STORAGE.md`).
//!
//! When a listener names principals, a principal without a global `manage_topics` or
//! `manage_streams` gets `GROUP_AUTHORIZATION_FAILED` (30), and nothing is deleted.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use futures::future::join_all;
use kafka_protocol::messages::offset_delete_response::{
    OffsetDeleteResponsePartition, OffsetDeleteResponseTopic,
};
use kafka_protocol::messages::{OffsetDeleteRequest, OffsetDeleteResponse};

use crate::auth::AuthenticatedPrincipal;
use crate::bridge::{BridgeError, IggyBridge, OffsetCalls, ScannedGroups, TopicTarget};
use crate::error::Result;
use crate::group::{OffsetDeletion, Subscribed, is_valid_group_id};
use crate::protocol::api::{
    API_KEY_OFFSET_DELETE, ApiVersionRange, ConnectionState, ERROR_COORDINATOR_LOAD_IN_PROGRESS,
    ERROR_GROUP_AUTHORIZATION_FAILED, ERROR_GROUP_ID_NOT_FOUND, ERROR_GROUP_SUBSCRIBED_TO_TOPIC,
    ERROR_INVALID_GROUP_ID, ERROR_INVALID_REQUEST, ERROR_NON_EMPTY_GROUP, ERROR_NONE,
    ERROR_UNKNOWN_SERVER_ERROR, ERROR_UNKNOWN_TOPIC_OR_PARTITION, ERROR_UNSUPPORTED_VERSION,
    GatewayState, HandleOutcome, is_supported_version,
};
use crate::protocol::bounds_guard::validate_offset_delete_shape;
use crate::protocol::handlers::{
    decode_guarded, deletion_deadline, encode_message, may_delete_groups, offset_refusal_cause,
    respond_or_close, unsupported_version_response,
};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_OFFSET_DELETE,
    min_version: 0,
    max_version: 0,
};

/// Partition index and error code of one echoed partition.
const PER_PARTITION_BYTES: usize = 6;

pub async fn handle(
    state: &GatewayState,
    connection: &ConnectionState,
    principal: Option<&AuthenticatedPrincipal>,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
    if !is_supported_version(API_KEY_OFFSET_DELETE, api_version) {
        return unsupported_version_response(API_KEY_OFFSET_DELETE, api_version, |version| {
            encode_error_response(version, ERROR_UNSUPPORTED_VERSION)
        });
    }
    let request = match decode_guarded::<OffsetDeleteRequest>(api_version, body, |version, body| {
        validate_offset_delete_shape(version, body, state.max_frame_size)
    }) {
        Ok(request) => request,
        Err(error) => {
            // debug!, not warn!: attacker-controlled, not operator-actionable.
            tracing::debug!(%error, api_version, "Failed to decode OffsetDelete request");
            return respond_or_close(
                encode_error_response(api_version, ERROR_INVALID_REQUEST),
                "OffsetDelete",
            );
        }
    };

    if !may_delete_groups(principal, "OffsetDelete") {
        return respond_or_close(
            encode_error_response(api_version, ERROR_GROUP_AUTHORIZATION_FAILED),
            "OffsetDelete",
        );
    }
    let encoded = match delete_offsets(state, connection, &request).await {
        Ok(topics) => encode_response(api_version, topics),
        Err(code) => encode_error_response(api_version, code),
    };
    respond_or_close(encoded, "OffsetDelete")
}

/// The answer for each requested partition, or the group's error.
async fn delete_offsets(
    state: &GatewayState,
    connection: &ConnectionState,
    request: &OffsetDeleteRequest,
) -> core::result::Result<Vec<OffsetDeleteResponseTopic>, i16> {
    let group = &request.group_id.0;
    if !is_valid_group_id(group) {
        return Err(ERROR_INVALID_GROUP_ID);
    }
    // Until this drops, the group takes no join and no commit.
    let (deletable, deletion) = state.groups.offset_deletion(group).await;
    let Some(bridge) = &state.bridge else {
        // No group holds offsets, and no topic exists.
        return match deletable {
            OffsetDeletion::Absent => Err(ERROR_GROUP_ID_NOT_FOUND),
            OffsetDeletion::Refused => Err(ERROR_NON_EMPTY_GROUP),
            OffsetDeletion::Allowed(_) => Ok(same_code(request, ERROR_UNKNOWN_TOPIC_OR_PARTITION)),
        };
    };
    let subscribed = match deletable {
        OffsetDeletion::Refused => return Err(ERROR_NON_EMPTY_GROUP),
        OffsetDeletion::Allowed(subscribed) => Some(subscribed),
        OffsetDeletion::Absent => None,
    };
    let calls = OffsetCalls::new(deletion_deadline(state, connection).await);
    let topics = requested_topics(bridge, request, &calls).await?;
    if !deletion.commits_ended(calls.deadline()).await {
        return Err(ERROR_COORDINATOR_LOAD_IN_PROGRESS);
    }
    let subscribed = match subscribed {
        Some(subscribed) => subscribed,
        // No members here, so the group exists only while it holds offsets.
        None if holds_offsets(bridge, group, &topics, &calls).await? => {
            Subscribed::Topics(HashSet::new())
        }
        None => return Err(ERROR_GROUP_ID_NOT_FOUND),
    };
    // Each delete keeps the group fenced until it can no longer land, also past the deadline, so
    // no commit lands while a delete can still land.
    let calls = calls.keeping(deletion);
    delete_unsubscribed(bridge, request, &topics, &subscribed, &calls).await
}

/// Whether `group`, which has no members here, holds offsets in Iggy. The last scan answers when
/// it found the group. Otherwise each requested topic of `topics` is asked, which also finds
/// offsets that another gateway committed since that scan. A group that none of them holds waits
/// for a scan, which tells whether it holds offsets on another topic.
///
/// # Errors
///
/// `COORDINATOR_LOAD_IN_PROGRESS` when no scan has ended yet, or when a requested topic that did
/// not answer may hold the group.
async fn holds_offsets(
    bridge: &Arc<IggyBridge>,
    group: &str,
    topics: &HashMap<&str, core::result::Result<(TopicTarget, u32), i16>>,
    calls: &OffsetCalls,
) -> core::result::Result<bool, i16> {
    if bridge
        .last_offset_groups()
        .is_some_and(|groups| groups.contains(group))
    {
        return Ok(true);
    }
    let asked = join_all(
        topics
            .values()
            .filter_map(|topic| topic.as_ref().ok())
            .map(|(target, _)| bridge.holds_offset_group(group, target, calls)),
    )
    .await;
    let mut unanswered = false;
    for held in asked {
        match held {
            Ok(true) => return Ok(true),
            Ok(false) => {}
            Err(error) => {
                unanswered |= error.to_offset_error_code() == ERROR_COORDINATOR_LOAD_IN_PROGRESS;
            }
        }
    }
    match bridge.offset_groups(calls.deadline()).await {
        ScannedGroups::Found(groups) if groups.contains(group) => Ok(true),
        ScannedGroups::Pending => Err(ERROR_COORDINATOR_LOAD_IN_PROGRESS),
        _ if unanswered => Err(ERROR_COORDINATOR_LOAD_IN_PROGRESS),
        // No scan found the groups, and no requested topic holds this one.
        ScannedGroups::Found(_) | ScannedGroups::Failed => Ok(false),
    }
}

/// The Iggy topic and the partition count of each requested topic, or the code for each of its
/// partitions. A lookup that a retry can fix fails the group.
async fn requested_topics<'a>(
    bridge: &IggyBridge,
    request: &'a OffsetDeleteRequest,
    calls: &OffsetCalls,
) -> core::result::Result<HashMap<&'a str, core::result::Result<(TopicTarget, u32), i16>>, i16> {
    let mut names: Vec<&str> = request
        .topics
        .iter()
        .map(|topic| topic.name.0.as_str())
        .collect();
    names.sort_unstable();
    names.dedup();
    let counts = join_all(
        names
            .iter()
            .map(|name| bridge.kafka_topic_partitions(name, calls)),
    )
    .await;
    let mut found = HashMap::with_capacity(names.len());
    for (name, count) in names.into_iter().zip(counts) {
        let topic = match count {
            Ok(Some(topic)) => Ok(topic),
            // Kafka answers a topic that it does not know with 3, whatever its name.
            Ok(None) | Err(BridgeError::InvalidKafkaTopicName { .. }) => {
                Err(ERROR_UNKNOWN_TOPIC_OR_PARTITION)
            }
            Err(error) => match error.to_offset_error_code() {
                ERROR_COORDINATOR_LOAD_IN_PROGRESS => {
                    return Err(ERROR_COORDINATOR_LOAD_IN_PROGRESS);
                }
                code => Err(code),
            },
        };
        found.insert(name, topic);
    }
    Ok(found)
}

/// Deletes the offset of each requested partition that exists and whose topic no member
/// subscribes to.
async fn delete_unsubscribed(
    bridge: &IggyBridge,
    request: &OffsetDeleteRequest,
    requested: &HashMap<&str, core::result::Result<(TopicTarget, u32), i16>>,
    subscribed: &Subscribed,
    calls: &OffsetCalls,
) -> core::result::Result<Vec<OffsetDeleteResponseTopic>, i16> {
    let group = request.group_id.0.as_str();
    let mut topics = same_code(request, ERROR_NONE);
    let mut deletions = Vec::new();
    // Where each deletion's code goes: (topic, partition) in `topics`.
    let mut positions = Vec::new();
    for (topic_at, topic) in request.topics.iter().enumerate() {
        let name = topic.name.0.as_str();
        let found = match requested.get(name) {
            Some(Ok(found)) => Ok(found),
            Some(Err(code)) => Err(*code),
            None => Err(ERROR_UNKNOWN_TOPIC_OR_PARTITION),
        };
        for (partition_at, partition) in topic.partitions.iter().enumerate() {
            let code = match (found, u32::try_from(partition.partition_index)) {
                (Err(code), _) => code,
                (Ok((target, count)), Ok(index)) if index < *count => {
                    if subscribed.contains(name) {
                        ERROR_GROUP_SUBSCRIBED_TO_TOPIC
                    } else {
                        deletions.push((target, index, -1));
                        positions.push((topic_at, partition_at));
                        continue;
                    }
                }
                (Ok(_), _) => ERROR_UNKNOWN_TOPIC_OR_PARTITION,
            };
            topics[topic_at].partitions[partition_at].error_code = code;
        }
    }

    let results = bridge.commit_group_offsets(group, &deletions, calls).await;
    let mut first_refusal = None;
    for ((topic_at, partition_at), result) in positions.into_iter().zip(results) {
        let Err(error) = result else {
            continue;
        };
        let code = error.to_offset_error_code();
        if code == ERROR_COORDINATOR_LOAD_IN_PROGRESS {
            return Err(code);
        }
        topics[topic_at].partitions[partition_at].error_code = code;
        if code == ERROR_UNKNOWN_SERVER_ERROR && first_refusal.is_none() {
            first_refusal = Some((topic_at, partition_at, error));
        }
    }
    // One line per request, as in `OffsetCommit`.
    if let Some((topic_at, partition_at, error)) = first_refusal {
        let topic = &topics[topic_at];
        tracing::error!(
            group,
            kafka_topic = topic.name.0.as_str(),
            partition = topic.partitions[partition_at].partition_index,
            %error,
            cause = offset_refusal_cause(&error),
            "Iggy refused to delete Kafka offsets"
        );
    }
    Ok(topics)
}

/// Every requested topic and partition, in request order, with `code`.
fn same_code(request: &OffsetDeleteRequest, code: i16) -> Vec<OffsetDeleteResponseTopic> {
    request
        .topics
        .iter()
        .map(|topic| {
            OffsetDeleteResponseTopic::default()
                .with_name(topic.name.clone())
                .with_partitions(
                    topic
                        .partitions
                        .iter()
                        .map(|partition| {
                            OffsetDeleteResponsePartition::default()
                                .with_partition_index(partition.partition_index)
                                .with_error_code(code)
                        })
                        .collect(),
                )
        })
        .collect()
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_response(version: i16, topics: Vec<OffsetDeleteResponseTopic>) -> Result<Bytes> {
    let echoed: usize = topics
        .iter()
        .map(|topic| topic.name.0.len() + topic.partitions.len() * PER_PARTITION_BYTES)
        .sum();
    encode_message(
        &OffsetDeleteResponse::default().with_topics(topics),
        version,
        16 + echoed,
    )
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    encode_message(
        &OffsetDeleteResponse::default().with_error_code(error_code),
        version,
        16,
    )
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use bytes::BytesMut;
    use kafka_protocol::messages::offset_delete_request::{
        OffsetDeleteRequestPartition, OffsetDeleteRequestTopic,
    };
    use kafka_protocol::messages::{GroupId, TopicName};
    use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};
    use tokio_util::sync::CancellationToken;

    use super::*;
    use crate::group::{GroupCoordinator, GroupCoordinatorConfig, JoinRequest};
    use crate::protocol::acl::PrincipalPermissions;

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

    fn join_request(group: &str, protocol_type: &str) -> JoinRequest {
        JoinRequest {
            group_id: StrBytes::from_string(group.to_owned()),
            session_timeout: Duration::from_secs(10),
            rebalance_timeout: Duration::from_secs(10),
            member_id: StrBytes::new(),
            group_instance_id: None,
            protocol_type: StrBytes::from_string(protocol_type.to_owned()),
            protocols: vec![(StrBytes::from_static_str("range"), Bytes::new())],
            require_known_member_id: false,
            supports_skip_assignment: false,
        }
    }

    fn body(group: &str) -> Bytes {
        let request = OffsetDeleteRequest::default()
            .with_group_id(GroupId(StrBytes::from_string(group.to_owned())))
            .with_topics(vec![
                OffsetDeleteRequestTopic::default()
                    .with_name(TopicName(StrBytes::from_static_str("orders")))
                    .with_partitions(vec![
                        OffsetDeleteRequestPartition::default().with_partition_index(0),
                        OffsetDeleteRequestPartition::default().with_partition_index(1),
                    ]),
            ]);
        let mut buf = BytesMut::new();
        request.encode(&mut buf, 0).unwrap();
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

    /// The top-level error and each partition's `(topic, partition, error)`.
    async fn answer(
        state: &GatewayState,
        principal: Option<&AuthenticatedPrincipal>,
        body: Bytes,
    ) -> (i16, Vec<(String, i32, i16)>) {
        let connection = ConnectionState::default();
        let mut response = handle(state, &connection, principal, 0, body)
            .await
            .expect_response("OffsetDelete answers");
        let response = OffsetDeleteResponse::decode(&mut response, 0).unwrap();
        let partitions = response
            .topics
            .iter()
            .flat_map(|topic| {
                topic.partitions.iter().map(|partition| {
                    (
                        topic.name.0.to_string(),
                        partition.partition_index,
                        partition.error_code,
                    )
                })
            })
            .collect();
        (response.error_code, partitions)
    }

    /// Without a bridge no group holds offsets and no topic exists.
    #[tokio::test]
    async fn given_each_kind_of_group_without_a_bridge_should_answer_as_kafka() {
        let state = state();
        let joined = state
            .groups
            .join(&join_request("consumers", "consumer"))
            .await;
        assert_eq!(joined.error, ERROR_NONE);
        let joined = state.groups.join(&join_request("workers", "connect")).await;
        assert_eq!(joined.error, ERROR_NONE);

        assert_eq!(
            answer(&state, None, body("")).await,
            (ERROR_INVALID_GROUP_ID, vec![])
        );
        assert_eq!(
            answer(&state, None, body("unknown")).await,
            (ERROR_GROUP_ID_NOT_FOUND, vec![])
        );
        assert_eq!(
            answer(&state, None, body("workers")).await,
            (ERROR_NON_EMPTY_GROUP, vec![])
        );
        assert_eq!(
            answer(&state, None, body("consumers")).await,
            (
                ERROR_NONE,
                vec![
                    ("orders".to_string(), 0, ERROR_UNKNOWN_TOPIC_OR_PARTITION),
                    ("orders".to_string(), 1, ERROR_UNKNOWN_TOPIC_OR_PARTITION),
                ]
            )
        );
    }

    /// Kafka asks for `DELETE` on the group, which needs `manage_topics` here.
    #[tokio::test]
    async fn given_a_principal_without_manage_topics_should_refuse_the_group() {
        let state = state();

        for refused in [principal(false, true), principal(true, false)] {
            assert_eq!(
                answer(&state, Some(&refused), body("unknown")).await,
                (ERROR_GROUP_AUTHORIZATION_FAILED, vec![]),
                "known={}",
                refused.permissions_known
            );
        }
        assert_eq!(
            answer(&state, Some(&principal(true, true)), body("unknown")).await,
            (ERROR_GROUP_ID_NOT_FOUND, vec![])
        );
    }

    #[tokio::test]
    async fn given_an_unreadable_request_should_answer_invalid_request() {
        let state = state();

        assert_eq!(
            answer(&state, None, body("consumers").slice(..5)).await,
            (ERROR_INVALID_REQUEST, vec![])
        );
        assert!(
            handle(
                &state,
                &ConnectionState::default(),
                None,
                1,
                body("consumers")
            )
            .await
            .is_close(),
            "v1 is not advertised"
        );
    }
}
