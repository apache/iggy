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

//! `OffsetCommit` (API key 8).
//!
//! Checks the committer against its group, then stores each offset in Iggy
//! (`docs/OFFSET_STORAGE.md`). The response has no top-level error, so a group answer goes on
//! every partition.
//!
//! With the coordinator log, each offset is also appended to the log with its metadata, and the
//! commit counts only once the log holds it. The log answers `OffsetFetch`. A partition that
//! Iggy's change log shows appends at once with the Iggy store, as Kafka checks a commit against
//! its metadata and writes once. Any other appends once Iggy stored it, since that store checks
//! the topic and the partition, and once the change log shows the topic. So each logged offset
//! names its topic's incarnation, and a late deletion of an older topic of that name leaves it.
//!
//! A principal commits only to topics it may read in Iggy (`commit_access`), checked against the
//! change log's ids before any store. Elsewhere the partition gets `GROUP_AUTHORIZATION_FAILED`
//! (30), as Kafka answers a principal without `READ` on the group.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use futures::future::join_all;
use kafka_protocol::messages::offset_commit_response::{
    OffsetCommitResponsePartition, OffsetCommitResponseTopic,
};
use kafka_protocol::messages::{OffsetCommitRequest, OffsetCommitResponse};
use tokio::time::{Instant, timeout_at};

use crate::auth::{AuthenticatedPrincipal, TopicReads};
use crate::bridge::{
    BridgeError, IggyBridge, OffsetCalls, OffsetGroupSource, TopicShape, TopicShapes, TopicTarget,
};
use crate::coordinator::record::CommittedOffset;
use crate::coordinator::{Coordination, unix_millis};
use crate::error::Result;
use crate::group::CommitRequest;
use crate::protocol::api::{
    API_KEY_OFFSET_COMMIT, ApiVersionRange, ConnectionState, ERROR_COORDINATOR_LOAD_IN_PROGRESS,
    ERROR_GROUP_AUTHORIZATION_FAILED, ERROR_NONE, ERROR_OFFSET_METADATA_TOO_LARGE,
    ERROR_UNKNOWN_SERVER_ERROR, ERROR_UNKNOWN_TOPIC_OR_PARTITION, GatewayState, HandleOutcome,
    is_supported_version,
};
use crate::protocol::bounds_guard::validate_offset_commit_shape;
use crate::protocol::handlers::{
    CommitAccess, commit_access, decode_guarded, encode_message, offset_deadline,
    offset_refusal_cause, respond_or_close,
};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_OFFSET_COMMIT,
    min_version: 2,
    max_version: 9,
};

/// Partition index, error code and tagged fields of one echoed partition.
const PER_PARTITION_BYTES: usize = 7;

/// Kafka's default `offset.metadata.max.bytes`. Kafka compares it with the metadata's Java
/// `String.length()`, which counts UTF-16 units, not bytes. Only the coordinator log keeps
/// metadata.
const MAX_OFFSET_METADATA_BYTES: usize = 4096;

/// How often a refused commit warns at most. The others log at debug.
const REFUSAL_WARN_EVERY: Duration = Duration::from_mins(1);

/// When a refused commit last warned, in seconds since the Unix epoch.
static REFUSAL_WARNED_AT: AtomicU64 = AtomicU64::new(0);

pub async fn handle(
    state: &GatewayState,
    connection: &ConnectionState,
    principal: Option<&AuthenticatedPrincipal>,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
    // A client reads a partition left out of the answer as committed, so a request that cannot be
    // answered partition by partition closes the connection instead.
    if !is_supported_version(API_KEY_OFFSET_COMMIT, api_version) {
        return HandleOutcome::Close;
    }
    let request = match decode_guarded::<OffsetCommitRequest>(api_version, body, |version, body| {
        validate_offset_commit_shape(version, body, state.max_frame_size)
    }) {
        Ok(request) => request,
        Err(error) => {
            // debug!, not warn!: attacker-controlled, not operator-actionable.
            tracing::debug!(%error, api_version, "Failed to decode OffsetCommit request");
            return HandleOutcome::Close;
        }
    };

    let validated = state
        .groups
        .validate_commit(&CommitRequest::from(&request))
        .await;
    let topics = match (&state.bridge, validated) {
        (Some(bridge), Ok(commit)) => {
            // The commit stays counted until none of its Iggy calls can land, also past the
            // deadline, so a deletion of the group's offsets waits for them.
            let calls = OffsetCalls::new(offset_deadline(state, connection).await).keeping(commit);
            let coordination = state.coordination.as_deref();
            let targets = targets(bridge, &request);
            let (mut topics, planned) = plan(&request, &targets, coordination.is_some());
            let access = commit_access(principal);
            let planned = admit(bridge, &access, &request, planned, &mut topics, &calls).await;
            if let Some(principal) = principal {
                log_refusal(principal, &request, &topics);
            }
            if let Some(coordination) = coordination {
                commit_logged(bridge, coordination, &request, &planned, topics, &calls).await
            } else {
                store_in_iggy(bridge, &request, &planned, &calls, &mut topics).await;
                topics
            }
        }
        (None, Ok(_)) => same_code(&request, ERROR_COORDINATOR_LOAD_IN_PROGRESS),
        (_, Err(error)) => same_code(&request, error),
    };
    respond_or_close(encode_response(api_version, topics), "OffsetCommit")
}

/// The partitions of `planned` on topics that `access` lets the principal read. Each other one
/// gets `GROUP_AUTHORIZATION_FAILED` (30) in `topics`, or `UNKNOWN_TOPIC_OR_PARTITION` (3), which
/// clients retry, where the topic's ids are not known.
async fn admit<'a>(
    bridge: &IggyBridge,
    access: &CommitAccess,
    request: &OffsetCommitRequest,
    planned: Vec<Planned<'a>>,
    topics: &mut [OffsetCommitResponseTopic],
    calls: &OffsetCalls,
) -> Vec<Planned<'a>> {
    let codes = match access {
        CommitAccess::Any => return planned,
        CommitAccess::Never => vec![ERROR_GROUP_AUTHORIZATION_FAILED; request.topics.len()],
        CommitAccess::Scoped(reads) => {
            let wanted: BTreeSet<usize> = planned.iter().map(|planned| planned.at.0).collect();
            read_codes(bridge, reads, request, &wanted, calls).await
        }
    };
    planned
        .into_iter()
        .filter(|planned| {
            let (topic_at, partition_at) = planned.at;
            let code = codes[topic_at];
            topics[topic_at].partitions[partition_at].error_code = code;
            code == ERROR_NONE
        })
        .collect()
}

/// For each requested topic at `wanted`, positions in the request, whether `reads` cover it:
/// `ERROR_NONE`, or the partition's code. The ids come from the change log, which also keeps the
/// grants, so both are as new. A bridge that does not follow the change log reads them from Iggy.
async fn read_codes(
    bridge: &IggyBridge,
    reads: &TopicReads,
    request: &OffsetCommitRequest,
    wanted: &BTreeSet<usize>,
    calls: &OffsetCalls,
) -> Vec<i16> {
    let mut codes = vec![ERROR_NONE; request.topics.len()];
    let ids: Vec<core::result::Result<Option<(u32, u32)>, BridgeError>> = if bridge
        .offset_group_source()
        == OffsetGroupSource::ChangeLog
    {
        let shapes = request_shapes(request, &bridge.topic_shapes().borrow());
        wanted
            .iter()
            .map(|topic_at| Ok(shapes[*topic_at].map(|shape| (shape.stream_id, shape.topic_id))))
            .collect()
    } else {
        join_all(
            wanted
                .iter()
                .map(|topic_at| bridge.topic_ids(request.topics[*topic_at].name.0.as_str(), calls)),
        )
        .await
    };
    for (topic_at, ids) in wanted.iter().zip(ids) {
        codes[*topic_at] = match ids {
            Ok(Some((stream, topic))) if reads.covers(stream, topic) => ERROR_NONE,
            Ok(Some(_)) => ERROR_GROUP_AUTHORIZATION_FAILED,
            Ok(None) => ERROR_UNKNOWN_TOPIC_OR_PARTITION,
            Err(error) => error.to_offset_error_code(),
        };
    }
    codes
}

/// Logs a request whose partitions `admit` refused: a warning at most once per
/// [`REFUSAL_WARN_EVERY`], since a consumer commits every few seconds, and debug otherwise.
fn log_refusal(
    principal: &AuthenticatedPrincipal,
    request: &OffsetCommitRequest,
    topics: &[OffsetCommitResponseTopic],
) {
    let refused = topics.iter().find(|topic| {
        topic
            .partitions
            .iter()
            .any(|partition| partition.error_code == ERROR_GROUP_AUTHORIZATION_FAILED)
    });
    let Some(refused) = refused else {
        return;
    };
    let cause = if principal.permissions_known {
        "the principal may not read the topic in Iggy; grant it a read or manage permission on \
         topics or streams, on the topic's stream, or on the topic"
    } else {
        "the principal's permissions could not be read at login"
    };
    let (principal, group, kafka_topic) = (
        principal.username.as_str(),
        request.group_id.0.as_str(),
        refused.name.0.as_str(),
    );
    if warn_due(&REFUSAL_WARNED_AT, REFUSAL_WARN_EVERY, unix_secs()) {
        tracing::warn!(
            principal,
            group,
            kafka_topic,
            "OffsetCommit answered GROUP_AUTHORIZATION_FAILED: {cause}"
        );
    } else {
        tracing::debug!(
            principal,
            group,
            kafka_topic,
            "OffsetCommit answered GROUP_AUTHORIZATION_FAILED: {cause}"
        );
    }
}

/// Whether a warning last given at `warned_at` may be given again at `now`, in seconds since the
/// Unix epoch. Moves `warned_at` to `now` when it may.
fn warn_due(warned_at: &AtomicU64, every: Duration, now: u64) -> bool {
    let last = warned_at.load(Ordering::Relaxed);
    now.saturating_sub(last) >= every.as_secs()
        && warned_at
            .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
}

fn unix_secs() -> u64 {
    u64::try_from(unix_millis() / 1000).unwrap_or(0)
}

/// One requested partition that passed the gateway's own checks.
struct Planned<'a> {
    /// Where its code goes: (topic, partition) in the answer.
    at: (usize, usize),
    target: &'a TopicTarget,
    index: u32,
}

/// Stores each offset of `planned` in Iggy and appends it to the coordinator log, with its
/// metadata. A partition gets the log's code when the append fails, since the commit counts only
/// in the log.
async fn commit_logged(
    bridge: &IggyBridge,
    coordination: &Coordination,
    request: &OffsetCommitRequest,
    planned: &[Planned<'_>],
    mut topics: Vec<OffsetCommitResponseTopic>,
    calls: &OffsetCalls,
) -> Vec<OffsetCommitResponseTopic> {
    let group = request.group_id.0.as_str();
    let shapes = request_shapes(request, &bridge.topic_shapes().borrow());
    let (checked, unchecked): (Vec<&Planned<'_>>, Vec<&Planned<'_>>) =
        planned.iter().partition(|planned| {
            shapes[planned.at.0].is_some_and(|shape| planned.index < shape.partitions)
        });
    let commit_timestamp_ms = unix_millis();
    let ((), logged) = tokio::join!(
        store_in_iggy(bridge, request, planned, calls, &mut topics),
        log_offsets(
            coordination,
            group,
            logged_offsets(request, &checked, &shapes, commit_timestamp_ms),
            calls
        ),
    );
    if let Err(code) = logged {
        set_code(&mut topics, &checked, code);
    }
    let stored: Vec<&Planned<'_>> = unchecked
        .into_iter()
        .filter(|planned| code_of(&topics, planned) == ERROR_NONE)
        .collect();
    // A change log that the bridge lost shows no new topic and reports no deletion, so an offset
    // then logs without its topic's incarnation.
    let follows = bridge.offset_group_source() == OffsetGroupSource::ChangeLog;
    let shapes = if follows && !stored.is_empty() {
        let waiting: Vec<usize> = stored.iter().map(|planned| planned.at.0).collect();
        shown_shapes(bridge, request, &waiting, calls.deadline()).await
    } else {
        shapes
    };
    let (shown, unshown): (Vec<&Planned<'_>>, Vec<&Planned<'_>>) = stored
        .into_iter()
        .partition(|planned| !follows || shapes[planned.at.0].is_some());
    set_code(&mut topics, &unshown, ERROR_COORDINATOR_LOAD_IN_PROGRESS);
    let offsets = logged_offsets(request, &shown, &shapes, commit_timestamp_ms);
    if let Err(code) = log_offsets(coordination, group, offsets, calls).await {
        set_code(&mut topics, &shown, code);
    }
    topics
}

/// The change log's shape of each requested topic, in request order.
fn request_shapes(request: &OffsetCommitRequest, shown: &TopicShapes) -> Vec<Option<TopicShape>> {
    request
        .topics
        .iter()
        .map(|topic| shown.get(topic.name.0.as_str()).copied())
        .collect()
}

/// [`request_shapes`] once the change log shows each requested topic at `waiting`, positions in
/// the request, or at `deadline`. Iggy holds those topics, since it stored their offsets, and the
/// change log shows a new topic about a second after Iggy creates it.
async fn shown_shapes(
    bridge: &IggyBridge,
    request: &OffsetCommitRequest,
    waiting: &[usize],
    deadline: Instant,
) -> Vec<Option<TopicShape>> {
    let mut changes = bridge.topic_shapes();
    loop {
        let shapes = request_shapes(request, &changes.borrow_and_update());
        let shown = waiting.iter().all(|topic_at| shapes[*topic_at].is_some());
        if shown || !matches!(timeout_at(deadline, changes.changed()).await, Ok(Ok(()))) {
            return shapes;
        }
    }
}

/// The offset of each of `entries` as the coordinator log keeps it, with the incarnation of its
/// topic from `shapes`.
fn logged_offsets(
    request: &OffsetCommitRequest,
    entries: &[&Planned<'_>],
    shapes: &[Option<TopicShape>],
    commit_timestamp_ms: i64,
) -> Vec<(String, i32, CommittedOffset)> {
    entries
        .iter()
        .map(|planned| {
            let (topic_at, partition_at) = planned.at;
            let topic = &request.topics[topic_at];
            let partition = &topic.partitions[partition_at];
            let committed = CommittedOffset {
                offset: partition.committed_offset,
                leader_epoch: partition.committed_leader_epoch,
                metadata: partition
                    .committed_metadata
                    .as_ref()
                    .map(ToString::to_string),
                commit_timestamp_ms,
                topic_incarnation: shapes[topic_at].map(|shape| shape.incarnation),
            };
            (
                topic.name.0.to_string(),
                partition.partition_index,
                committed,
            )
        })
        .collect()
}

/// The Iggy topic of each requested topic, in request order.
fn targets(
    bridge: &IggyBridge,
    request: &OffsetCommitRequest,
) -> Vec<core::result::Result<TopicTarget, BridgeError>> {
    request
        .topics
        .iter()
        .map(|topic| bridge.topic_target(topic.name.0.as_str()))
        .collect()
}

/// The answer with the code of each partition that fails the gateway's own checks, and the
/// partitions that pass them. With `keeps_metadata`, metadata over Kafka's limit fails.
fn plan<'a>(
    request: &OffsetCommitRequest,
    targets: &'a [core::result::Result<TopicTarget, BridgeError>],
    keeps_metadata: bool,
) -> (Vec<OffsetCommitResponseTopic>, Vec<Planned<'a>>) {
    let mut topics = same_code(request, ERROR_NONE);
    let mut planned = Vec::new();
    for (topic_at, (topic, target)) in request.topics.iter().zip(targets).enumerate() {
        for (partition_at, partition) in topic.partitions.iter().enumerate() {
            let oversized = keeps_metadata
                && partition
                    .committed_metadata
                    .as_ref()
                    .is_some_and(|metadata| {
                        metadata.encode_utf16().count() > MAX_OFFSET_METADATA_BYTES
                    });
            let code = match (target, u32::try_from(partition.partition_index)) {
                _ if oversized => ERROR_OFFSET_METADATA_TOO_LARGE,
                (Err(error), _) => error.to_offset_error_code(),
                (Ok(_), Err(_)) => ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                (Ok(target), Ok(index)) => {
                    planned.push(Planned {
                        at: (topic_at, partition_at),
                        target,
                        index,
                    });
                    continue;
                }
            };
            topics[topic_at].partitions[partition_at].error_code = code;
        }
    }
    (topics, planned)
}

/// Stores the offset of each partition of `planned` in Iggy, and puts each failure's code in
/// `topics`. The slots run at once, each one partition at a time, and a partition that the
/// deadline leaves out answers 14 and makes no call.
async fn store_in_iggy(
    bridge: &IggyBridge,
    request: &OffsetCommitRequest,
    planned: &[Planned<'_>],
    calls: &OffsetCalls,
    topics: &mut [OffsetCommitResponseTopic],
) {
    let group = request.group_id.0.as_str();
    let commits: Vec<_> = planned
        .iter()
        .map(|planned| {
            let (topic_at, partition_at) = planned.at;
            let offset = request.topics[topic_at].partitions[partition_at].committed_offset;
            (planned.target, planned.index, offset)
        })
        .collect();
    let results = bridge.commit_group_offsets(group, &commits, calls).await;
    let mut refused = 0_usize;
    let mut first_refusal = None;
    for (planned, result) in planned.iter().zip(results) {
        let Err(error) = result else {
            continue;
        };
        let (topic_at, partition_at) = planned.at;
        let code = error.to_offset_error_code();
        topics[topic_at].partitions[partition_at].error_code = code;
        if code == ERROR_UNKNOWN_SERVER_ERROR {
            refused += 1;
            if first_refusal.is_none() {
                first_refusal = Some((topic_at, partition_at, error));
            }
        }
    }
    // One line per request: a full partition or an old server refuses each commit, every few
    // seconds.
    if let Some((topic_at, partition_at, error)) = first_refusal {
        let topic = &topics[topic_at];
        tracing::error!(
            group,
            kafka_topic = topic.name.0.as_str(),
            partition = topic.partitions[partition_at].partition_index,
            refused,
            %error,
            cause = offset_refusal_cause(&error),
            "Iggy refused Kafka offset commits"
        );
    }
}

/// Appends `offsets` to the coordinator log, until the deadline of `calls`, as the Iggy stores
/// stop there.
async fn log_offsets(
    coordination: &Coordination,
    group: &str,
    offsets: Vec<(String, i32, CommittedOffset)>,
    calls: &OffsetCalls,
) -> core::result::Result<(), i16> {
    if offsets.is_empty() {
        return Ok(());
    }
    coordination
        .commit_offsets(group, offsets, calls.deadline())
        .await
}

fn code_of(topics: &[OffsetCommitResponseTopic], planned: &Planned<'_>) -> i16 {
    let (topic_at, partition_at) = planned.at;
    topics[topic_at].partitions[partition_at].error_code
}

fn set_code(topics: &mut [OffsetCommitResponseTopic], planned: &[&Planned<'_>], code: i16) {
    for planned in planned {
        let (topic_at, partition_at) = planned.at;
        topics[topic_at].partitions[partition_at].error_code = code;
    }
}

/// Every requested topic and partition, in request order, with `code`.
fn same_code(request: &OffsetCommitRequest, code: i16) -> Vec<OffsetCommitResponseTopic> {
    request
        .topics
        .iter()
        .map(|topic| {
            OffsetCommitResponseTopic::default()
                .with_name(topic.name.clone())
                .with_partitions(
                    topic
                        .partitions
                        .iter()
                        .map(|partition| {
                            OffsetCommitResponsePartition::default()
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
pub fn encode_response(version: i16, topics: Vec<OffsetCommitResponseTopic>) -> Result<Bytes> {
    let echoed: usize = topics
        .iter()
        .map(|topic| topic.name.0.len() + topic.partitions.len() * PER_PARTITION_BYTES)
        .sum();
    encode_message(
        &OffsetCommitResponse::default().with_topics(topics),
        version,
        16 + echoed,
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use bytes::BytesMut;
    use iggy::prelude::{IggyError, Permissions, StreamPermissions};
    use kafka_protocol::messages::offset_commit_request::{
        OffsetCommitRequestPartition, OffsetCommitRequestTopic,
    };
    use kafka_protocol::messages::{GroupId, TopicName};
    use kafka_protocol::protocol::{Decodable, Encodable, StrBytes};

    use super::*;
    use crate::auth::{TopicReads, UserKey};
    use crate::bridge::BridgeError;
    use crate::protocol::acl::PrincipalPermissions;
    use crate::protocol::api::ERROR_UNKNOWN_MEMBER_ID;

    fn request(generation_id: i32, member_id: &'static str) -> OffsetCommitRequest {
        OffsetCommitRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str("g")))
            .with_generation_id_or_member_epoch(generation_id)
            .with_member_id(StrBytes::from_static_str(member_id))
            .with_topics(vec![
                OffsetCommitRequestTopic::default()
                    .with_name(TopicName(StrBytes::from_static_str("orders")))
                    .with_partitions(vec![
                        OffsetCommitRequestPartition::default()
                            .with_partition_index(0)
                            .with_committed_offset(5),
                        OffsetCommitRequestPartition::default()
                            .with_partition_index(1)
                            .with_committed_offset(7),
                    ]),
            ])
    }

    fn body(version: i16, request: &OffsetCommitRequest) -> Bytes {
        let mut buf = BytesMut::new();
        request.encode(&mut buf, version).unwrap();
        buf.freeze()
    }

    async fn codes(version: i16, request: &OffsetCommitRequest) -> Vec<(String, i32, i16)> {
        let state = GatewayState::stub(8 * 1024 * 1024);
        let connection = ConnectionState::default();
        let mut response = handle(&state, &connection, None, version, body(version, request))
            .await
            .expect_response("OffsetCommit answers");
        let response = OffsetCommitResponse::decode(&mut response, version).unwrap();
        response
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
            .collect()
    }

    /// After a gateway restart every group is gone. A member's commit must make it rejoin.
    #[tokio::test]
    async fn given_an_unknown_group_when_a_member_commits_should_answer_unknown_member_id() {
        for version in [2, 8, 9] {
            assert_eq!(
                codes(version, &request(3, "m-1")).await,
                vec![
                    ("orders".to_string(), 0, ERROR_UNKNOWN_MEMBER_ID),
                    ("orders".to_string(), 1, ERROR_UNKNOWN_MEMBER_ID),
                ],
                "v{version}"
            );
        }
    }

    fn principal(read_topics: bool, permissions_known: bool) -> AuthenticatedPrincipal {
        AuthenticatedPrincipal {
            username: "ann".to_string(),
            user: Some(UserKey {
                user_id: 7,
                created_at: 1,
            }),
            permissions: PrincipalPermissions {
                read_topics,
                poll_messages: true,
                ..PrincipalPermissions::default()
            },
            topic_reads: Arc::default(),
            permissions_known,
        }
    }

    /// The kind of [`CommitAccess`] that `principal` gets.
    fn access(principal: Option<&AuthenticatedPrincipal>) -> &'static str {
        match commit_access(principal) {
            CommitAccess::Any => "any",
            CommitAccess::Scoped(_) => "scoped",
            CommitAccess::Never => "never",
        }
    }

    /// `principal(false, true)` with a read grant on every topic of stream 1.
    fn on_stream_1() -> AuthenticatedPrincipal {
        let mut on_stream = principal(false, true);
        on_stream.topic_reads = Arc::new(TopicReads::of(&Permissions {
            streams: Some(BTreeMap::from([(
                1,
                StreamPermissions {
                    read_stream: true,
                    ..StreamPermissions::default()
                },
            )])),
            ..Permissions::default()
        }));
        on_stream
    }

    #[test]
    fn given_each_kind_of_principal_should_commit_only_where_iggy_would_create_its_group() {
        assert_eq!(access(None), "any", "no listener names principals");
        assert_eq!(
            access(Some(&AuthenticatedPrincipal::anonymous())),
            "any",
            "a listener that authenticates no one"
        );
        assert_eq!(access(Some(&principal(true, true))), "any");
        assert_eq!(
            access(Some(&principal(false, true))),
            "never",
            "polling alone does not create a consumer group in Iggy"
        );
        assert_eq!(
            access(Some(&principal(true, false))),
            "never",
            "permissions never read"
        );
        assert_eq!(
            access(Some(&on_stream_1())),
            "scoped",
            "a grant on one stream covers that stream's topics"
        );

        let unread_anonymous = AuthenticatedPrincipal {
            username: AuthenticatedPrincipal::ANONYMOUS.to_string(),
            user: None,
            permissions: PrincipalPermissions::default(),
            topic_reads: Arc::default(),
            permissions_known: false,
        };
        assert_eq!(
            access(Some(&unread_anonymous)),
            "never",
            "an Iggy user named ANONYMOUS whose read failed is not the anonymous principal"
        );
    }

    /// The ids come from the change log, so a scoped check costs no Iggy call. A topic it does
    /// not show answers 3, which clients retry, as for a topic deleted meanwhile.
    #[tokio::test]
    async fn given_a_scoped_principal_should_refuse_each_topic_its_grants_do_not_cover() {
        let bridge = IggyBridge::offline_following();
        let shape = |stream_id| TopicShape {
            incarnation: 1,
            partitions: 1,
            stream_id,
            topic_id: 4,
        };
        bridge.show_topic_shapes(TopicShapes::from([
            ("orders".to_string(), shape(1)),
            ("refunds".to_string(), shape(2)),
        ]));
        let request = request(-1, "").with_topics(
            ["orders", "refunds", "payments"]
                .into_iter()
                .map(|name| {
                    OffsetCommitRequestTopic::default()
                        .with_name(TopicName(StrBytes::from_static_str(name)))
                        .with_partitions(vec![
                            OffsetCommitRequestPartition::default().with_committed_offset(1),
                        ])
                })
                .collect(),
        );
        let targets = targets(&bridge, &request);
        let (mut topics, planned) = plan(&request, &targets, true);
        let calls = OffsetCalls::new(Instant::now() + Duration::from_secs(1));

        let principal = on_stream_1();
        let access = commit_access(Some(&principal));
        let admitted = admit(&bridge, &access, &request, planned, &mut topics, &calls).await;

        assert_eq!(
            admitted
                .iter()
                .map(|planned| planned.at)
                .collect::<Vec<_>>(),
            [(0, 0)]
        );
        let codes: Vec<i16> = topics
            .iter()
            .map(|topic| topic.partitions[0].error_code)
            .collect();
        assert_eq!(
            codes,
            [
                ERROR_NONE,
                ERROR_GROUP_AUTHORIZATION_FAILED,
                ERROR_UNKNOWN_TOPIC_OR_PARTITION
            ]
        );
    }

    #[test]
    fn given_a_refusal_warned_lately_should_not_warn_again_within_the_interval() {
        let warned_at = AtomicU64::new(0);
        let every = Duration::from_secs(60);

        assert!(warn_due(&warned_at, every, 1_000));
        assert!(!warn_due(&warned_at, every, 1_059));
        assert!(warn_due(&warned_at, every, 1_060));
    }

    #[tokio::test]
    async fn given_no_bridge_when_committing_should_answer_retriable_on_every_partition() {
        assert_eq!(
            codes(9, &request(-1, "")).await,
            vec![
                ("orders".to_string(), 0, ERROR_COORDINATOR_LOAD_IN_PROGRESS),
                ("orders".to_string(), 1, ERROR_COORDINATOR_LOAD_IN_PROGRESS),
            ]
        );
    }

    #[test]
    fn given_bridge_errors_when_mapped_for_offsets_should_keep_retriable_failures_retriable() {
        for retriable in [
            BridgeError::Timeout,
            BridgeError::Iggy(IggyError::Disconnected),
            BridgeError::Iggy(IggyError::TransientNotCommitted),
            BridgeError::Iggy(IggyError::RequestTooOld),
            BridgeError::Iggy(IggyError::LaneBlocked(7)),
        ] {
            assert_eq!(
                retriable.to_offset_error_code(),
                ERROR_COORDINATOR_LOAD_IN_PROGRESS
            );
        }
        assert_eq!(
            BridgeError::Iggy(IggyError::TooManyConsumerOffsets).to_offset_error_code(),
            ERROR_UNKNOWN_SERVER_ERROR
        );
        assert_eq!(
            BridgeError::Iggy(IggyError::TopicNameNotFound(
                "orders".to_string(),
                "kafka".to_string(),
            ))
            .to_offset_error_code(),
            ERROR_UNKNOWN_TOPIC_OR_PARTITION
        );
    }

    /// A partition left out of the answer reads as committed, so nothing short of a full answer
    /// may go back.
    #[tokio::test]
    async fn given_an_unreadable_request_should_close_rather_than_answer() {
        let state = GatewayState::stub(8 * 1024 * 1024);
        let connection = ConnectionState::default();

        let truncated = body(9, &request(-1, "")).slice(..5);
        assert!(
            handle(&state, &connection, None, 9, truncated)
                .await
                .is_close()
        );
        assert!(
            handle(&state, &connection, None, 10, body(9, &request(-1, "")))
                .await
                .is_close()
        );
    }

    /// Iggy stored the commit, so it holds the topic, but its change log can show the topic a
    /// moment later. Logged before then, the offset would name no incarnation, and a late
    /// deletion of an older topic of that name would remove it.
    #[tokio::test(start_paused = true)]
    async fn given_a_topic_the_change_log_shows_late_should_wait_for_its_incarnation() {
        let bridge = Arc::new(IggyBridge::offline());
        let request = request(1, "m").with_topics(vec![
            OffsetCommitRequestTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("orders"))),
            OffsetCommitRequestTopic::default()
                .with_name(TopicName(StrBytes::from_static_str("payments"))),
        ]);
        let shape = TopicShape {
            incarnation: 7,
            partitions: 1,
            stream_id: 1,
            topic_id: 1,
        };
        let change_log = Arc::clone(&bridge);
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(300)).await;
            change_log.show_topic_shapes(TopicShapes::from([("orders".to_string(), shape)]));
        });

        let deadline = Instant::now() + Duration::from_secs(5);
        let shapes = shown_shapes(&bridge, &request, &[0], deadline).await;

        assert_eq!(shapes, [Some(shape), None]);
        assert!(Instant::now() < deadline, "it ends once the topic shows");
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_topic_the_change_log_never_shows_should_stop_waiting_at_the_deadline() {
        let bridge = IggyBridge::offline();
        let deadline = Instant::now() + Duration::from_secs(1);

        let shapes = shown_shapes(&bridge, &request(1, "m"), &[0], deadline).await;

        assert_eq!(shapes, [None]);
        assert!(Instant::now() >= deadline);
    }
}
