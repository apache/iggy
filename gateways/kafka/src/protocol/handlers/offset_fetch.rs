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

//! `OffsetFetch` (API key 9).
//!
//! Reads committed offsets from Iggy (`docs/OFFSET_STORAGE.md`). A failure goes at group level
//! from v2: the Java consumer treats most partition errors as fatal, even retriable ones.
//!
//! With the coordinator log, only the coordinator of a group answers for it. The log answers each
//! partition it holds an offset or a deletion for, with the commit metadata. Iggy answers the
//! others, which hold offsets from before the log.

use bytes::Bytes;
use futures::future::join_all;
use kafka_protocol::messages::offset_fetch_response::{
    OffsetFetchResponseGroup, OffsetFetchResponsePartition, OffsetFetchResponsePartitions,
    OffsetFetchResponseTopic, OffsetFetchResponseTopics,
};
use kafka_protocol::messages::{GroupId, OffsetFetchRequest, OffsetFetchResponse, TopicName};
use kafka_protocol::protocol::StrBytes;
use tokio::time::Instant;

use crate::bridge::{BridgeError, IggyBridge, KafkaTopicMetadata, OffsetCalls, TopicTarget};
use crate::coordinator::record::CommittedOffset;
use crate::coordinator::replay::{GroupOffsets, TopicPartition};
use crate::error::Result;
use crate::group::is_valid_group_id;
use crate::protocol::api::{
    API_KEY_OFFSET_FETCH, ApiVersionRange, ConnectionState, ERROR_COORDINATOR_LOAD_IN_PROGRESS,
    ERROR_INVALID_GROUP_ID, ERROR_NONE, ERROR_UNKNOWN_SERVER_ERROR,
    ERROR_UNKNOWN_TOPIC_OR_PARTITION, GatewayState, HandleOutcome, UNKNOWN_OFFSET,
    is_supported_version,
};
use crate::protocol::bounds_guard::validate_offset_fetch_shape;
use crate::protocol::handlers::{
    decode_guarded, encode_message, offset_deadline, offset_refusal_cause, respond_or_close,
};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_OFFSET_FETCH,
    min_version: 1,
    max_version: 9,
};

/// From this version a request names several groups and each answer carries its own error.
const FIRST_BATCHED_VERSION: i16 = 8;
/// Below this version there is no group error field.
const FIRST_GROUP_ERROR_VERSION: i16 = 2;
/// Index, offset, leader epoch, empty metadata, error code and tagged fields of one partition.
const PER_PARTITION_BYTES: usize = 24;

/// One group a request asks about, across wire versions.
#[derive(Debug, Clone)]
struct GroupQuery {
    group_id: StrBytes,
    /// `None` asks for every committed offset of the group.
    topics: Option<Vec<(TopicName, Vec<i32>)>>,
}

impl GroupQuery {
    /// One query per group the request names: one below v8, the `groups` array from v8.
    fn all(version: i16, request: &OffsetFetchRequest) -> Vec<Self> {
        if version >= FIRST_BATCHED_VERSION {
            return request
                .groups
                .iter()
                .map(|group| Self {
                    group_id: group.group_id.0.clone(),
                    topics: group.topics.as_ref().map(|topics| {
                        topics
                            .iter()
                            .map(|topic| (topic.name.clone(), topic.partition_indexes.clone()))
                            .collect()
                    }),
                })
                .collect();
        }
        vec![Self {
            group_id: request.group_id.0.clone(),
            topics: request.topics.as_ref().map(|topics| {
                topics
                    .iter()
                    .map(|topic| (topic.name.clone(), topic.partition_indexes.clone()))
                    .collect()
            }),
        }]
    }
}

/// The answer for one group. A group error carries no topics.
#[derive(Debug)]
struct GroupAnswer {
    error: i16,
    topics: Vec<TopicAnswer>,
}

impl GroupAnswer {
    const fn error(error: i16) -> Self {
        Self {
            error,
            topics: Vec::new(),
        }
    }
}

#[derive(Debug)]
struct TopicAnswer {
    name: TopicName,
    partitions: Vec<PartitionAnswer>,
}

/// `offset` is -1 when nothing is committed. `metadata` is the commit's, which only the
/// coordinator log keeps.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PartitionAnswer {
    index: i32,
    offset: i64,
    error: i16,
    leader_epoch: i32,
    metadata: Option<StrBytes>,
}

impl From<&PartitionAnswer> for OffsetFetchResponsePartitions {
    fn from(partition: &PartitionAnswer) -> Self {
        Self::default()
            .with_partition_index(partition.index)
            .with_committed_offset(partition.offset)
            .with_committed_leader_epoch(partition.leader_epoch)
            .with_metadata(Some(partition.metadata.clone().unwrap_or_default()))
            .with_error_code(partition.error)
    }
}

impl From<&PartitionAnswer> for OffsetFetchResponsePartition {
    fn from(partition: &PartitionAnswer) -> Self {
        Self::default()
            .with_partition_index(partition.index)
            .with_committed_offset(partition.offset)
            .with_committed_leader_epoch(partition.leader_epoch)
            .with_metadata(Some(partition.metadata.clone().unwrap_or_default()))
            .with_error_code(partition.error)
    }
}

impl PartitionAnswer {
    const fn new(index: i32, offset: i64, error: i16) -> Self {
        Self {
            index,
            offset,
            error,
            leader_epoch: -1,
            metadata: None,
        }
    }

    /// The answer the log gives: the commit, or -1 for an offset it deleted.
    fn logged(index: i32, committed: Option<&CommittedOffset>) -> Self {
        committed.map_or_else(
            || Self::new(index, UNKNOWN_OFFSET, ERROR_NONE),
            |committed| Self {
                index,
                offset: committed.offset,
                error: ERROR_NONE,
                leader_epoch: committed.leader_epoch,
                metadata: committed.metadata.clone().map(StrBytes::from_string),
            },
        )
    }
}

pub async fn handle(
    state: &GatewayState,
    connection: &ConnectionState,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
    // A v1 answer has nowhere to put a request error, and a partition left out reads as "nothing
    // committed", so an unreadable request closes the connection at every version.
    if !is_supported_version(API_KEY_OFFSET_FETCH, api_version) {
        return HandleOutcome::Close;
    }
    let request = match decode_guarded::<OffsetFetchRequest>(api_version, body, |version, body| {
        validate_offset_fetch_shape(version, body, state.max_frame_size)
    }) {
        Ok(request) => request,
        Err(error) => {
            // debug!, not warn!: attacker-controlled, not operator-actionable.
            tracing::debug!(%error, api_version, "Failed to decode OffsetFetch request");
            return HandleOutcome::Close;
        }
    };

    let queries = GroupQuery::all(api_version, &request);
    let calls = OffsetCalls::new(offset_deadline(state, connection).await);
    // The groups read at once, so a slow group cannot spend the time of the others.
    let answers = join_all(queries.iter().map(|query| answer(state, query, &calls))).await;
    respond_or_close(
        encode_response(api_version, &queries, answers),
        "OffsetFetch",
    )
}

/// One failed read fails the whole group, so a client never reads a lost answer as "nothing
/// committed" and resets its position.
async fn answer(state: &GatewayState, query: &GroupQuery, calls: &OffsetCalls) -> GroupAnswer {
    if !is_valid_group_id(&query.group_id) {
        return GroupAnswer::error(ERROR_INVALID_GROUP_ID);
    }
    let Some(bridge) = state.bridge.as_deref() else {
        return GroupAnswer::error(ERROR_COORDINATOR_LOAD_IN_PROGRESS);
    };
    let logged = match &state.coordination {
        Some(coordination) => {
            let requested = query.topics.as_deref().map(requested_partitions);
            match coordination
                .offsets(&query.group_id, requested.as_deref())
                .await
            {
                Ok(logged) => logged,
                Err(code) => return GroupAnswer::error(code),
            }
        }
        None => None,
    };
    // The log holds every offset of a group it deleted, so Iggy has nothing to add. Nor does it
    // for a request whose every partition the log answers.
    if let Some(logged) = logged
        .as_ref()
        .filter(|logged| logged.complete || answers_all(logged, query.topics.as_deref()))
    {
        return GroupAnswer {
            error: ERROR_NONE,
            topics: from_log(logged, query.topics.as_deref()),
        };
    }
    // A group the deadline leaves out costs no Iggy call.
    if Instant::now() >= calls.deadline() {
        return GroupAnswer::error(ERROR_COORDINATOR_LOAD_IN_PROGRESS);
    }
    // Each Iggy call stops at the deadline on its own, so none is cut off holding its slot.
    let read = match (&query.topics, &logged) {
        // Iggy reads only the partitions the log holds no entry for.
        (Some(topics), Some(logged)) => {
            read_topics(bridge, &query.group_id, &unlogged(topics, logged), calls)
                .await
                .map(|read| merged(topics, logged, read))
        }
        (Some(topics), None) => read_topics(bridge, &query.group_id, topics, calls).await,
        (None, _) => read_every_topic(bridge, &query.group_id, calls)
            .await
            .map(|mut read| {
                if let Some(logged) = &logged {
                    overlay(&mut read, logged);
                }
                read
            }),
    };
    match read {
        Ok(topics) => GroupAnswer {
            error: ERROR_NONE,
            topics,
        },
        Err(error) => GroupAnswer::error(refusal(&query.group_id, &error)),
    }
}

/// Each partition `topics` asks for, as the log keys it.
fn requested_partitions(topics: &[(TopicName, Vec<i32>)]) -> Vec<TopicPartition> {
    topics
        .iter()
        .flat_map(|(name, indexes)| {
            indexes
                .iter()
                .map(move |index| (name.0.to_string(), *index))
        })
        .collect()
}

/// `topics` without the partitions the log holds an entry for. Each topic stays, so the read
/// answers in the same order.
fn unlogged(topics: &[(TopicName, Vec<i32>)], logged: &GroupOffsets) -> Vec<(TopicName, Vec<i32>)> {
    topics
        .iter()
        .map(|(name, indexes)| {
            let indexes = indexes
                .iter()
                .copied()
                .filter(|index| !logged.entries.contains_key(&(name.0.to_string(), *index)))
                .collect();
            (name.clone(), indexes)
        })
        .collect()
}

/// The answer to `topics`, in request order: the log's for each partition it holds an entry
/// for, else the next of `read`, which answers [`unlogged`] in that order.
fn merged(
    topics: &[(TopicName, Vec<i32>)],
    logged: &GroupOffsets,
    read: Vec<TopicAnswer>,
) -> Vec<TopicAnswer> {
    topics
        .iter()
        .zip(read)
        .map(|((name, indexes), read)| {
            let mut from_iggy = read.partitions.into_iter();
            let partitions = indexes
                .iter()
                .map(|&index| {
                    logged
                        .entries
                        .get(&(name.0.to_string(), index))
                        .map_or_else(
                            || {
                                from_iggy.next().unwrap_or_else(|| {
                                    PartitionAnswer::new(index, UNKNOWN_OFFSET, ERROR_NONE)
                                })
                            },
                            |committed| PartitionAnswer::logged(index, committed.as_ref()),
                        )
                })
                .collect();
            TopicAnswer {
                name: name.clone(),
                partitions,
            }
        })
        .collect()
}

/// Whether the log holds an offset or a deletion for each partition of `topics`. A request with
/// no topic list asks for every topic, which only Iggy can list.
fn answers_all(logged: &GroupOffsets, topics: Option<&[(TopicName, Vec<i32>)]>) -> bool {
    topics.is_some_and(|topics| {
        topics.iter().all(|(name, indexes)| {
            indexes
                .iter()
                .all(|index| logged.entries.contains_key(&(name.0.to_string(), *index)))
        })
    })
}

/// The answers the log alone gives: each requested partition, or with no topic list each
/// partition that holds an offset.
fn from_log(logged: &GroupOffsets, topics: Option<&[(TopicName, Vec<i32>)]>) -> Vec<TopicAnswer> {
    let Some(topics) = topics else {
        let mut answers: Vec<TopicAnswer> = Vec::new();
        for ((topic, index), committed) in &logged.entries {
            let Some(committed) = committed else {
                continue;
            };
            let answer = PartitionAnswer::logged(*index, Some(committed));
            match answers.last_mut() {
                Some(last) if last.name.0.as_str() == topic => last.partitions.push(answer),
                _ => answers.push(TopicAnswer {
                    name: TopicName(StrBytes::from_string(topic.clone())),
                    partitions: vec![answer],
                }),
            }
        }
        return answers;
    };
    topics
        .iter()
        .map(|(name, indexes)| TopicAnswer {
            name: name.clone(),
            partitions: indexes
                .iter()
                .map(|&index| {
                    let key = (name.0.to_string(), index);
                    PartitionAnswer::logged(
                        index,
                        logged.entries.get(&key).and_then(Option::as_ref),
                    )
                })
                .collect(),
        })
        .collect()
}

/// Puts the log's answer on each partition of Iggy's answer to a request with no topic list.
/// The log's offsets that Iggy does not hold join the answer, and its deletions leave it.
fn overlay(topics: &mut Vec<TopicAnswer>, logged: &GroupOffsets) {
    for topic in topics.iter_mut() {
        for partition in &mut topic.partitions {
            let key = (topic.name.0.to_string(), partition.index);
            if let Some(committed) = logged.entries.get(&key) {
                *partition = PartitionAnswer::logged(partition.index, committed.as_ref());
            }
        }
    }
    for topic in topics.iter_mut() {
        topic
            .partitions
            .retain(|partition| partition.offset != UNKNOWN_OFFSET);
    }
    for ((name, index), committed) in &logged.entries {
        let Some(committed) = committed else {
            continue;
        };
        let found = topics
            .iter()
            .position(|topic| topic.name.0.as_str() == name);
        let at = found.unwrap_or_else(|| {
            topics.push(TopicAnswer {
                name: TopicName(StrBytes::from_string(name.clone())),
                partitions: Vec::new(),
            });
            topics.len() - 1
        });
        let partitions = &mut topics[at].partitions;
        if !partitions.iter().any(|partition| partition.index == *index) {
            partitions.push(PartitionAnswer::logged(*index, Some(committed)));
        }
    }
    topics.retain(|topic| !topic.partitions.is_empty());
}

/// The group code for a failed read. A -1 is logged, since the client sees no more.
fn refusal(group: &str, error: &BridgeError) -> i16 {
    let code = error.to_offset_error_code();
    if code == ERROR_UNKNOWN_SERVER_ERROR {
        tracing::error!(
            group,
            %error,
            cause = offset_refusal_cause(error),
            "Iggy refused a Kafka offset read"
        );
    } else {
        tracing::debug!(group, %error, "Iggy refused a Kafka offset read");
    }
    code
}

/// Every requested partition, in request order. A partition with no offset answers -1. Reads on
/// different offset slots run at once.
async fn read_topics(
    bridge: &IggyBridge,
    group: &str,
    topics: &[(TopicName, Vec<i32>)],
    calls: &OffsetCalls,
) -> core::result::Result<Vec<TopicAnswer>, BridgeError> {
    // A name that cannot map to Iggy names no topic, so nothing is committed under it.
    let targets: Vec<Option<TopicTarget>> = topics
        .iter()
        .map(|(name, _)| bridge.topic_target(name.0.as_str()).ok())
        .collect();
    let mut answers: Vec<TopicAnswer> = topics
        .iter()
        .map(|(name, indexes)| TopicAnswer {
            name: name.clone(),
            partitions: indexes
                .iter()
                .map(|&index| PartitionAnswer::new(index, UNKNOWN_OFFSET, ERROR_NONE))
                .collect(),
        })
        .collect();
    let mut reads = Vec::new();
    // Where each read's offset goes: (topic, partition) in `answers`.
    let mut positions = Vec::new();
    for (topic_at, ((_, indexes), target)) in topics.iter().zip(&targets).enumerate() {
        let Some(target) = target else {
            continue;
        };
        for (partition_at, &index) in indexes.iter().enumerate() {
            if let Ok(partition) = u32::try_from(index) {
                reads.push((target, partition));
                positions.push((topic_at, partition_at));
            }
        }
    }
    let offsets = bridge.fetch_group_offsets(group, &reads, calls).await;
    for ((topic_at, partition_at), read) in positions.into_iter().zip(offsets) {
        answers[topic_at].partitions[partition_at].offset = committed(read)?;
    }
    Ok(answers)
}

/// A null topic list, as the Java `Admin.listConsumerGroupOffsets` sends for a whole group: only
/// the partitions with an offset, on every topic that holds the group. The topics read at once,
/// and `calls` keeps the request to one queued call per slot.
async fn read_every_topic(
    bridge: &IggyBridge,
    group: &str,
    calls: &OffsetCalls,
) -> core::result::Result<Vec<TopicAnswer>, BridgeError> {
    let topics = bridge.list_kafka_topics_in_slot(calls).await?;
    let answers = join_all(
        topics
            .into_iter()
            .map(|topic| read_listed_topic(bridge, group, topic, calls)),
    )
    .await;
    answers
        .into_iter()
        .filter_map(core::result::Result::transpose)
        .collect()
}

/// The partitions of `topic` with an offset of `group`. `None` when there are none.
async fn read_listed_topic(
    bridge: &IggyBridge,
    group: &str,
    topic: KafkaTopicMetadata,
    calls: &OffsetCalls,
) -> core::result::Result<Option<TopicAnswer>, BridgeError> {
    let target = bridge.topic_target(&topic.kafka_topic)?;
    match bridge.holds_offset_group(group, &target, calls).await {
        Ok(true) => {}
        Ok(false) => return Ok(None),
        // A topic deleted since the listing holds no offsets.
        Err(error) if error.to_kafka_error_code() == ERROR_UNKNOWN_TOPIC_OR_PARTITION => {
            return Ok(None);
        }
        Err(error) => return Err(error),
    }
    let reads: Vec<_> = (0..topic.partitions_count)
        .map(|partition| (&target, partition))
        .collect();
    let offsets = bridge.fetch_group_offsets(group, &reads, calls).await;
    let mut partitions = Vec::new();
    for (partition, read) in (0..topic.partitions_count).zip(offsets) {
        let offset = committed(read)?;
        if offset != UNKNOWN_OFFSET {
            partitions.push(PartitionAnswer::new(
                i32::try_from(partition).unwrap_or(i32::MAX),
                offset,
                ERROR_NONE,
            ));
        }
    }
    Ok((!partitions.is_empty()).then(|| TopicAnswer {
        name: TopicName(StrBytes::from_string(topic.kafka_topic)),
        partitions,
    }))
}

/// A read as Kafka answers it: -1 for no offset and for a topic or partition that does not exist.
fn committed(
    read: core::result::Result<Option<u64>, BridgeError>,
) -> core::result::Result<i64, BridgeError> {
    match read {
        Ok(Some(offset)) => Ok(i64::try_from(offset).unwrap_or(i64::MAX)),
        Ok(None) => Ok(UNKNOWN_OFFSET),
        Err(error) if error.to_kafka_error_code() == ERROR_UNKNOWN_TOPIC_OR_PARTITION => {
            Ok(UNKNOWN_OFFSET)
        }
        Err(error) => Err(error),
    }
}

/// One entry per query, in request order. Below v8 there is exactly one.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
fn encode_response(
    version: i16,
    queries: &[GroupQuery],
    answers: Vec<GroupAnswer>,
) -> Result<Bytes> {
    let partitions: usize = answers
        .iter()
        .flat_map(|answer| &answer.topics)
        .map(|topic| topic.name.0.len() + topic.partitions.len() * PER_PARTITION_BYTES)
        .sum();
    let groups: usize = queries.iter().map(|query| query.group_id.len() + 8).sum();
    let capacity = 16 + groups + partitions;

    if version >= FIRST_BATCHED_VERSION {
        let groups = queries
            .iter()
            .zip(answers)
            .map(|(query, answer)| {
                OffsetFetchResponseGroup::default()
                    .with_group_id(GroupId(query.group_id.clone()))
                    .with_error_code(answer.error)
                    .with_topics(
                        answer
                            .topics
                            .into_iter()
                            .map(|topic| {
                                OffsetFetchResponseTopics::default()
                                    .with_name(topic.name)
                                    .with_partitions(
                                        topic
                                            .partitions
                                            .iter()
                                            .map(OffsetFetchResponsePartitions::from)
                                            .collect(),
                                    )
                            })
                            .collect(),
                    )
            })
            .collect();
        return encode_message(
            &OffsetFetchResponse::default().with_groups(groups),
            version,
            capacity,
        );
    }

    let (Some(query), Some(GroupAnswer { error, topics })) =
        (queries.first(), answers.into_iter().next())
    else {
        return encode_message(&OffsetFetchResponse::default(), version, capacity);
    };
    let topics = if version < FIRST_GROUP_ERROR_VERSION && error != ERROR_NONE {
        // No group error field yet: the error goes on every partition asked for.
        query
            .topics
            .iter()
            .flatten()
            .map(|(name, indexes)| TopicAnswer {
                name: name.clone(),
                partitions: indexes
                    .iter()
                    .map(|&index| PartitionAnswer::new(index, UNKNOWN_OFFSET, error))
                    .collect(),
            })
            .collect()
    } else {
        topics
    };
    let topics = topics
        .into_iter()
        .map(|topic| {
            OffsetFetchResponseTopic::default()
                .with_name(topic.name)
                .with_partitions(
                    topic
                        .partitions
                        .iter()
                        .map(OffsetFetchResponsePartition::from)
                        .collect(),
                )
        })
        .collect();
    let mut response = OffsetFetchResponse::default().with_topics(topics);
    if version >= FIRST_GROUP_ERROR_VERSION {
        response = response.with_error_code(error);
    }
    encode_message(&response, version, capacity)
}

#[cfg(test)]
mod tests {
    use bytes::BytesMut;
    use iggy::prelude::IggyError;
    use kafka_protocol::messages::offset_fetch_request::{
        OffsetFetchRequestGroup, OffsetFetchRequestTopic, OffsetFetchRequestTopics,
    };
    use kafka_protocol::protocol::{Decodable, Encodable};

    use super::*;

    fn topic(name: &'static str) -> TopicName {
        TopicName(StrBytes::from_static_str(name))
    }

    fn single_group(group_id: &'static str) -> OffsetFetchRequest {
        OffsetFetchRequest::default()
            .with_group_id(GroupId(StrBytes::from_static_str(group_id)))
            .with_topics(Some(vec![
                OffsetFetchRequestTopic::default()
                    .with_name(topic("orders"))
                    .with_partition_indexes(vec![0, 1]),
            ]))
    }

    async fn fetch(version: i16, request: &OffsetFetchRequest) -> OffsetFetchResponse {
        let state = GatewayState::stub(8 * 1024 * 1024);
        let connection = ConnectionState::default();
        let mut body = BytesMut::new();
        request.encode(&mut body, version).unwrap();
        let mut response = handle(&state, &connection, version, body.freeze())
            .await
            .expect_response("OffsetFetch answers");
        OffsetFetchResponse::decode(&mut response, version).unwrap()
    }

    #[tokio::test]
    async fn given_no_bridge_at_v1_should_put_the_error_on_every_partition() {
        let response = fetch(1, &single_group("g")).await;

        let partitions = &response.topics[0].partitions;
        assert_eq!(partitions.len(), 2);
        for partition in partitions {
            assert_eq!(partition.error_code, ERROR_COORDINATOR_LOAD_IN_PROGRESS);
            assert_eq!(partition.committed_offset, UNKNOWN_OFFSET);
        }
    }

    #[tokio::test]
    async fn given_no_bridge_at_v7_should_answer_a_group_error_and_no_topics() {
        let response = fetch(7, &single_group("g")).await;

        assert_eq!(response.error_code, ERROR_COORDINATOR_LOAD_IN_PROGRESS);
        assert_eq!(response.topics, []);
    }

    /// The Java client looks each group up by id in a v8 answer and throws if one is missing.
    #[tokio::test]
    async fn given_several_groups_at_v9_should_echo_each_group_id() {
        let request = OffsetFetchRequest::default().with_groups(vec![
            OffsetFetchRequestGroup::default()
                .with_group_id(GroupId(StrBytes::from_static_str("a")))
                .with_topics(Some(vec![
                    OffsetFetchRequestTopics::default()
                        .with_name(topic("orders"))
                        .with_partition_indexes(vec![0]),
                ])),
            OffsetFetchRequestGroup::default().with_group_id(GroupId(StrBytes::new())),
        ]);

        let response = fetch(9, &request).await;

        let groups: Vec<(&str, i16)> = response
            .groups
            .iter()
            .map(|group| (group.group_id.0.as_str(), group.error_code))
            .collect();
        assert_eq!(
            groups,
            vec![
                ("a", ERROR_COORDINATOR_LOAD_IN_PROGRESS),
                ("", ERROR_INVALID_GROUP_ID)
            ]
        );
    }

    /// Only "there is no such offset" may read as -1. A lost read must fail the group instead,
    /// or the consumer resets its position.
    #[test]
    fn given_bridge_reads_only_a_missing_offset_should_answer_minus_one() {
        assert_eq!(committed(Ok(Some(42))).unwrap(), 42);
        assert_eq!(committed(Ok(None)).unwrap(), UNKNOWN_OFFSET);
        assert_eq!(
            committed(Err(BridgeError::Iggy(IggyError::ResourceNotFound(
                String::new()
            ))))
            .unwrap(),
            UNKNOWN_OFFSET
        );
        let lost = committed(Err(BridgeError::Iggy(IggyError::Disconnected))).unwrap_err();
        assert_eq!(
            lost.to_offset_error_code(),
            ERROR_COORDINATOR_LOAD_IN_PROGRESS
        );
    }

    #[test]
    fn given_a_log_that_holds_some_partitions_should_read_the_rest_from_iggy_in_order() {
        let committed = |offset| CommittedOffset {
            offset,
            leader_epoch: 3,
            metadata: Some("logged".to_string()),
            commit_timestamp_ms: 0,
        };
        let logged = GroupOffsets {
            complete: false,
            entries: [
                (("orders".to_string(), 1), Some(committed(10))),
                (("payments".to_string(), 0), None),
            ]
            .into_iter()
            .collect(),
        };
        let topics = vec![
            (topic("orders"), vec![0, 1, 2]),
            (topic("payments"), vec![0]),
        ];

        let unread = unlogged(&topics, &logged);
        assert_eq!(unread[0].1, [0, 2]);
        assert_eq!(
            unread[1].1,
            Vec::<i32>::new(),
            "a topic the log answers stays"
        );

        let read = vec![
            TopicAnswer {
                name: topic("orders"),
                partitions: vec![
                    PartitionAnswer::new(0, 5, ERROR_NONE),
                    PartitionAnswer::new(2, UNKNOWN_OFFSET, ERROR_NONE),
                ],
            },
            TopicAnswer {
                name: topic("payments"),
                partitions: Vec::new(),
            },
        ];
        let answer = merged(&topics, &logged, read);
        let offsets: Vec<Vec<(i32, i64)>> = answer
            .iter()
            .map(|topic| {
                topic
                    .partitions
                    .iter()
                    .map(|partition| (partition.index, partition.offset))
                    .collect()
            })
            .collect();
        assert_eq!(
            offsets,
            [
                vec![(0, 5), (1, 10), (2, UNKNOWN_OFFSET)],
                vec![(0, UNKNOWN_OFFSET)]
            ]
        );
        assert_eq!(answer[0].partitions[1].leader_epoch, 3, "the log's commit");
    }

    #[tokio::test]
    async fn given_an_unreadable_request_should_close_rather_than_answer() {
        let state = GatewayState::stub(8 * 1024 * 1024);
        let connection = ConnectionState::default();
        let mut body = BytesMut::new();
        single_group("g").encode(&mut body, 6).unwrap();
        let body = body.freeze();

        assert!(
            handle(&state, &connection, 6, body.slice(..3))
                .await
                .is_close()
        );
        assert!(handle(&state, &connection, 10, body).await.is_close());
    }
}
