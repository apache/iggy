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

/// `offset` is -1 when nothing is committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PartitionAnswer {
    index: i32,
    offset: i64,
    error: i16,
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
    let answers = join_all(
        queries
            .iter()
            .map(|query| answer(state.bridge.as_deref(), query, &calls)),
    )
    .await;
    respond_or_close(
        encode_response(api_version, &queries, answers),
        "OffsetFetch",
    )
}

/// One failed read fails the whole group, so a client never reads a lost answer as "nothing
/// committed" and resets its position.
async fn answer(
    bridge: Option<&IggyBridge>,
    query: &GroupQuery,
    calls: &OffsetCalls,
) -> GroupAnswer {
    if !is_valid_group_id(&query.group_id) {
        return GroupAnswer::error(ERROR_INVALID_GROUP_ID);
    }
    let Some(bridge) = bridge else {
        return GroupAnswer::error(ERROR_COORDINATOR_LOAD_IN_PROGRESS);
    };
    // A group the deadline leaves out costs no Iggy call.
    if Instant::now() >= calls.deadline() {
        return GroupAnswer::error(ERROR_COORDINATOR_LOAD_IN_PROGRESS);
    }
    // Each Iggy call stops at the deadline on its own, so none is cut off holding its slot.
    let read = match &query.topics {
        Some(topics) => read_topics(bridge, &query.group_id, topics, calls).await,
        None => read_every_topic(bridge, &query.group_id, calls).await,
    };
    match read {
        Ok(topics) => GroupAnswer {
            error: ERROR_NONE,
            topics,
        },
        Err(error) => GroupAnswer::error(refusal(&query.group_id, &error)),
    }
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
                .map(|&index| PartitionAnswer {
                    index,
                    offset: UNKNOWN_OFFSET,
                    error: ERROR_NONE,
                })
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
            partitions.push(PartitionAnswer {
                index: i32::try_from(partition).unwrap_or(i32::MAX),
                offset,
                error: ERROR_NONE,
            });
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
                                            .map(|partition| {
                                                OffsetFetchResponsePartitions::default()
                                                    .with_partition_index(partition.index)
                                                    .with_committed_offset(partition.offset)
                                                    .with_error_code(partition.error)
                                            })
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
                    .map(|&index| PartitionAnswer {
                        index,
                        offset: UNKNOWN_OFFSET,
                        error,
                    })
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
                        .map(|partition| {
                            OffsetFetchResponsePartition::default()
                                .with_partition_index(partition.index)
                                .with_committed_offset(partition.offset)
                                .with_error_code(partition.error)
                        })
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
    use crate::protocol::api::BrokerAdvertise;

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
        let state = GatewayState::stub(BrokerAdvertise::default(), 8 * 1024 * 1024);
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

    #[tokio::test]
    async fn given_an_unreadable_request_should_close_rather_than_answer() {
        let state = GatewayState::stub(BrokerAdvertise::default(), 8 * 1024 * 1024);
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
