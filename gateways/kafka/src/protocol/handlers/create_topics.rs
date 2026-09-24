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

//! `CreateTopics` (API key 19).

use std::collections::HashSet;
use std::time::Duration;

use bytes::Bytes;
use iggy::prelude::IggyError;
use kafka_protocol::messages::create_topics_request::{CreatableReplicaAssignment, CreatableTopic};
use kafka_protocol::messages::create_topics_response::CreatableTopicResult;
use kafka_protocol::messages::{BrokerId, CreateTopicsRequest, CreateTopicsResponse, TopicName};
use kafka_protocol::protocol::StrBytes;

use crate::bridge::{BridgeError, IggyBridge, TopicCreationOutcome};
use crate::error::Result;
use crate::protocol::api::{
    API_KEY_CREATE_TOPICS, ApiVersionRange, ERROR_INVALID_CONFIG, ERROR_INVALID_PARTITIONS,
    ERROR_INVALID_REPLICA_ASSIGNMENT, ERROR_INVALID_REPLICATION_FACTOR, ERROR_INVALID_REQUEST,
    ERROR_NONE, ERROR_NOT_CONTROLLER, ERROR_REQUEST_TIMED_OUT, ERROR_TOPIC_ALREADY_EXISTS,
    GatewayState, HandleOutcome,
};
use crate::protocol::bounds_guard::validate_create_topics_shape;
use crate::protocol::handlers::{
    decode_guarded, encode_message, handle_versioned_request, is_supported_version,
    respond_or_close, unsupported_version_response,
};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_CREATE_TOPICS,
    min_version: 2,
    max_version: 5,
};

/// KIP-464 `num_partitions = -1` with no manual assignment: the count this bridge creates.
///
/// Matches real Kafka's own out-of-box `num.partitions=1` broker default. Not read from any
/// bridge config - there is no such config surface today.
const DEFAULT_PARTITION_COUNT: u32 = 1;

/// Cap on distinct topic names one `CreateTopics` request may address through the bridge.
///
/// `bounds_guard`'s `MAX_REQUEST_ELEMENTS` (4,096) is a pre-decode `DoS` ceiling, not a usability
/// recommendation: each non-duplicate requested name here costs up to ~4 Iggy round trips
/// (`ensure_stream` + `create_topic`, plus a possible race-retry read on either) against the
/// single lockstep `IggyClient` every Kafka connection on this gateway shares
/// (`bridge/iggy_bridge/mod.rs`'s "Concurrency ceiling"). 100 keeps a worst-case batch's
/// aggregate bridge cost small relative to that shared resource while remaining generous for any
/// real admin batch. Duplicate names never count against this cap - they're rejected by
/// [`find_duplicate_names`] before ever reaching the bridge.
const MAX_BRIDGE_BACKED_TOPICS: usize = 100;

/// Bounds imposed on the request's own `timeout_ms` before it becomes the aggregate bridge-work
/// deadline. That value is client-supplied and otherwise unchecked: `0` or negative would abort
/// every topic on arrival, and an oversized one would tie up the shared `IggyClient` past any
/// reasonable request.
const MIN_REQUEST_TIMEOUT: Duration = Duration::from_millis(1_000);
const MAX_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Clamps the wire's own `timeout_ms` (KIP-4's field for exactly this) into
/// `[MIN_REQUEST_TIMEOUT, MAX_REQUEST_TIMEOUT]` - unlike `ListOffsets`/`Metadata`, `CreateTopics`
/// carries a real client-supplied deadline to honor, not just a fixed internal ceiling.
fn clamp_request_timeout(timeout_ms: i32) -> Duration {
    let requested = Duration::from_millis(u64::try_from(timeout_ms).unwrap_or(0));
    requested.clamp(MIN_REQUEST_TIMEOUT, MAX_REQUEST_TIMEOUT)
}

pub async fn handle(state: &GatewayState, api_version: i16, body: Bytes) -> HandleOutcome {
    let Some(bridge) = &state.bridge else {
        return handle_versioned_request(
            API_KEY_CREATE_TOPICS,
            api_version,
            body,
            |v, b| {
                decode_guarded::<CreateTopicsRequest>(v, b, |v, b| {
                    validate_create_topics_shape(v, b, state.max_frame_size)
                })
            },
            encode_response,
            encode_error_response,
            "CreateTopics",
        );
    };

    if !is_supported_version(API_KEY_CREATE_TOPICS, api_version) {
        return unsupported_version_response(API_KEY_CREATE_TOPICS, api_version, |version| {
            encode_error_response(version, ERROR_INVALID_REQUEST)
        });
    }

    let req = match decode_guarded::<CreateTopicsRequest>(api_version, body, |v, b| {
        validate_create_topics_shape(v, b, state.max_frame_size)
    }) {
        Ok(req) => req,
        Err(error) => {
            // debug!, not warn!: attacker-controlled, not operator-actionable.
            tracing::debug!(%error, "Failed to decode CreateTopics request");
            return respond_or_close(
                encode_error_response(api_version, ERROR_INVALID_REQUEST),
                "CreateTopics",
            );
        }
    };

    let duplicate_names = find_duplicate_names(&req.topics);

    let distinct_bridge_backed: HashSet<&TopicName> = req
        .topics
        .iter()
        .map(|topic| &topic.name)
        .filter(|name| !duplicate_names.contains(*name))
        .collect();
    if distinct_bridge_backed.len() > MAX_BRIDGE_BACKED_TOPICS {
        tracing::warn!(
            distinct_topics = distinct_bridge_backed.len(),
            max = MAX_BRIDGE_BACKED_TOPICS,
            "CreateTopics request addresses too many distinct topics; rejecting"
        );
        let results = req
            .topics
            .iter()
            .map(|topic| {
                CreatableTopicResult::default()
                    .with_name(topic.name.clone())
                    .with_error_code(ERROR_INVALID_REQUEST)
            })
            .collect();
        let resp = CreateTopicsResponse::default().with_topics(results);
        return respond_or_close(encode_message(&resp, api_version, 256), "CreateTopics");
    }

    let deadline = clamp_request_timeout(req.timeout_ms);
    let results = match tokio::time::timeout(
        deadline,
        create_all_topics(
            bridge,
            api_version,
            &req.topics,
            &duplicate_names,
            req.validate_only,
        ),
    )
    .await
    {
        Ok(results) => results,
        Err(_elapsed) => {
            tracing::warn!(
                distinct_topics = distinct_bridge_backed.len(),
                deadline_ms = deadline.as_millis(),
                "CreateTopics request's aggregate bridge work exceeded its deadline; \
                 answering retriable instead of blocking further"
            );
            req.topics
                .iter()
                .map(|topic| {
                    CreatableTopicResult::default()
                        .with_name(topic.name.clone())
                        .with_error_code(ERROR_REQUEST_TIMED_OUT)
                })
                .collect()
        }
    };
    let resp = CreateTopicsResponse::default().with_topics(results);
    respond_or_close(encode_message(&resp, api_version, 256), "CreateTopics")
}

/// Creates (or reports on) every requested topic, skipping the bridge entirely for a duplicate
/// name - real Kafka refuses the whole name, not a first-wins/last-wins split: creating one
/// occurrence and reporting `TOPIC_ALREADY_EXISTS` for the other would let a client observe a
/// create it never got a `NONE` for (`AdminClient` keys its futures by name, so a second
/// per-topic result for the same name is silently discarded client-side regardless of which one
/// this bridge picked).
async fn create_all_topics(
    bridge: &IggyBridge,
    api_version: i16,
    topics: &[CreatableTopic],
    duplicate_names: &HashSet<TopicName>,
    validate_only: bool,
) -> Vec<CreatableTopicResult> {
    let mut results = Vec::with_capacity(topics.len());
    for topic in topics {
        let result = if duplicate_names.contains(&topic.name) {
            CreatableTopicResult::default()
                .with_name(topic.name.clone())
                .with_error_code(ERROR_INVALID_REQUEST)
        } else {
            create_one_topic(bridge, api_version, topic, validate_only).await
        };
        results.push(result);
    }
    results
}

/// Every topic name that appears more than once in `topics` - real Kafka
/// (`ControllerApis.createTopics`) refuses every occurrence of a duplicate name with
/// `INVALID_REQUEST` (42) and creates nothing for it, rather than creating the first occurrence
/// and reporting the rest as already existing.
fn find_duplicate_names(topics: &[CreatableTopic]) -> HashSet<TopicName> {
    let mut seen = HashSet::with_capacity(topics.len());
    let mut duplicates = HashSet::new();
    for topic in topics {
        if !seen.insert(topic.name.clone()) {
            duplicates.insert(topic.name.clone());
        }
    }
    duplicates
}

/// Validates and, when the topic is not rejected outright, provisions one requested topic.
///
/// `configs` is rejected before the shape check - a config-bearing request is rejected the same
/// way regardless of how its partitions/replication are shaped.
///
/// `validate_only` and the real create path diverge deliberately below that point, not just in
/// whether they call the bridge: `validate_only` never mutates anything, so a plain existence
/// read ([`IggyBridge::get_kafka_topic`]) is fine - there's no race to protect against when
/// nothing gets created either way. The real path instead calls
/// [`IggyBridge::create_kafka_topic`], which folds the existence check and the create into one
/// atomic call - a separate read-then-write here would let two concurrent `CreateTopics` for the
/// same new name both observe `Ok(None)` and both receive `NONE`, when Kafka guarantees exactly
/// one caller does.
async fn create_one_topic(
    bridge: &IggyBridge,
    version: i16,
    topic: &CreatableTopic,
    validate_only: bool,
) -> CreatableTopicResult {
    let result = CreatableTopicResult::default().with_name(topic.name.clone());

    if !topic.configs.is_empty() {
        return result
            .with_error_code(ERROR_INVALID_CONFIG)
            .with_error_message(Some(StrBytes::from(
                "per-topic configs are not supported by this bridge".to_string(),
            )));
    }

    let partition_count = match validate_create_topic_shape(version, topic) {
        Ok(count) => count,
        Err(code) => return result.with_error_code(code),
    };

    let kafka_topic = topic.name.as_str();
    let success = || {
        result
            .clone()
            .with_error_code(ERROR_NONE)
            .with_num_partitions(i32::try_from(partition_count).unwrap_or(i32::MAX))
            .with_replication_factor(1)
    };

    if validate_only {
        return match bridge.get_kafka_topic(kafka_topic).await {
            Ok(Some(_existing)) => result.with_error_code(ERROR_TOPIC_ALREADY_EXISTS),
            Ok(None) => success(),
            Err(err) => result
                .with_error_code(err.to_kafka_error_code())
                .with_error_message(Some(StrBytes::from(error_message_for(&err)))),
        };
    }

    match bridge
        .create_kafka_topic(kafka_topic, partition_count)
        .await
    {
        // The second arm: the write committed on its first attempt, the SDK's own reconnect path
        // replayed it, and the server's client-table dedup caught the replay - not a fault.
        // `to_kafka_error_code`'s shared mapping deliberately doesn't special-case this - it's a
        // write-only fact, checked here, at the one write this bridge makes, rather than assumed
        // true for the reads that share that mapping too.
        Ok(TopicCreationOutcome::Created)
        | Err(BridgeError::Iggy(IggyError::RequestAlreadyApplied)) => success(),
        Ok(TopicCreationOutcome::AlreadyExists) => {
            result.with_error_code(ERROR_TOPIC_ALREADY_EXISTS)
        }
        Err(err) => result
            .with_error_code(err.to_kafka_error_code())
            .with_error_message(Some(StrBytes::from(error_message_for(&err)))),
    }
}

/// Error text for `CreatableTopicResult.error_message`, without re-embedding the topic name:
/// `result.name` (`CreatableTopicResult::with_name`) already carries it, so `err.to_string()`'s
/// own embedded copy for these two variants would double the per-topic response cost for a name
/// the client already sent and already has back. Validation runs before any bridge I/O, so this
/// is a purely local, zero-round-trip amplification if left in - 100 topics named with a maximal
/// legal length build a response roughly twice the size the name alone would justify.
fn error_message_for(err: &BridgeError) -> String {
    match err {
        BridgeError::InvalidKafkaTopicName { reason, .. } => reason.clone(),
        BridgeError::InvalidPartitionCount { .. } => {
            "partition count must be at least 1".to_string()
        }
        other => other.to_string(),
    }
}

/// Validates one requested topic's KIP-464 shape and resolves its partition count.
///
/// A manual partition `assignments` list and an explicit `num_partitions`/`replication_factor`
/// are mutually exclusive inputs, not two independently-checked values that happen to agree: real
/// Kafka's own `ReplicationControlManager` rejects a manual assignment unless both are exactly
/// `-1`, regardless of whether an explicit `num_partitions` matches `assignments.len()`. A count
/// that only *disagrees* with the assignment length is not a distinct, more lenient case - the
/// combination itself is what's invalid, so both are `INVALID_REQUEST` (42), never `NONE`.
///
/// An assignment's own partition indices are checked too, not just its length: real Kafka
/// requires the key set to be exactly `0..assignments.len()`, each index appearing once, and
/// rejects anything else - a duplicate or non-consecutive index (`{5: [...], 7: [...]}`) - with
/// `INVALID_REPLICA_ASSIGNMENT` (39). Each entry's own replica list is checked too, not just the
/// index: this gateway advertises exactly one broker (node id 1, `metadata.rs`'s `BrokerId(1)`),
/// so the only legal replica list is `[1]` - never empty, never repeating it, never naming a
/// broker Metadata never advertised. Real Kafka's `ReplicationControlManager` validates the whole
/// binding this way, not just the index.
///
/// With no assignments, `num_partitions = -1` / `replication_factor = -1` mean "use the broker
/// default" from v4+ (pre-v4 requires an explicit positive value for both, since v2/v3 have no
/// broker-default sentinel absent a manual assignment).
fn validate_create_topic_shape(
    version: i16,
    topic: &CreatableTopic,
) -> core::result::Result<u32, i16> {
    if !topic.assignments.is_empty() {
        if topic.num_partitions != -1 || topic.replication_factor != -1 {
            return Err(ERROR_INVALID_REQUEST);
        }
        if !assignment_indices_are_consecutive_from_zero(&topic.assignments)
            || !assignment_replicas_are_valid(&topic.assignments)
        {
            return Err(ERROR_INVALID_REPLICA_ASSIGNMENT);
        }
        return Ok(u32::try_from(topic.assignments.len()).unwrap_or(DEFAULT_PARTITION_COUNT));
    }

    let broker_default_ok = version >= 4;

    let partitions_ok = if broker_default_ok {
        topic.num_partitions == -1 || topic.num_partitions > 0
    } else {
        topic.num_partitions > 0
    };
    if !partitions_ok {
        return Err(ERROR_INVALID_PARTITIONS);
    }

    let replication_ok = if broker_default_ok {
        topic.replication_factor == -1 || topic.replication_factor > 0
    } else {
        topic.replication_factor > 0
    };
    if !replication_ok {
        return Err(ERROR_INVALID_REPLICATION_FACTOR);
    }

    let partition_count = if topic.num_partitions > 0 {
        u32::try_from(topic.num_partitions).unwrap_or(DEFAULT_PARTITION_COUNT)
    } else {
        DEFAULT_PARTITION_COUNT
    };
    Ok(partition_count)
}

/// Real Kafka requires a manual `assignments` list's partition indices to be exactly
/// `0..assignments.len()`, each appearing once - not merely that many entries as there are
/// partitions. `{5: [...], 7: [...]}` has the right length for a 2-partition topic but names
/// neither partition `0` nor `1`.
fn assignment_indices_are_consecutive_from_zero(
    assignments: &[CreatableReplicaAssignment],
) -> bool {
    let Some(last_index) = assignments.len().checked_sub(1) else {
        return false; // empty: the caller never reaches here with an empty list, but no gap math to underflow on.
    };
    let mut indices: Vec<i32> = assignments.iter().map(|a| a.partition_index).collect();
    indices.sort_unstable();
    indices.dedup();
    // Sorted, deduped, and matching the original count rules out both a duplicate and a gap: `n`
    // distinct integers spanning exactly `[0, n-1]` must be all of `0..n`, nothing else fits.
    indices.len() == assignments.len()
        && indices.first().copied() == Some(0)
        && indices.last().copied() == i32::try_from(last_index).ok()
}

/// The only legal replica list for any partition this gateway assigns: this gateway advertises
/// exactly one broker (node id 1, `metadata.rs`'s `BrokerId(1)`), so `[1]` is the sole valid
/// shape - length 1 rules out both empty and a duplicated id, and the value itself rules out a
/// broker Metadata never advertised.
const SINGLE_BROKER_ID: i32 = 1;

fn assignment_replicas_are_valid(assignments: &[CreatableReplicaAssignment]) -> bool {
    assignments
        .iter()
        .all(|assignment| assignment.broker_ids == [BrokerId(SINGLE_BROKER_ID)])
}

/// Well-formed `CreateTopics` response with a single placeholder topic.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    let topics = vec![
        CreatableTopic::default()
            .with_num_partitions(1)
            .with_replication_factor(1),
    ];
    encode_inner(version, &topics, error_code)
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_response(version: i16, req: &CreateTopicsRequest) -> Result<Bytes> {
    encode_inner(version, &req.topics, ERROR_NONE)
}

/// Resolve per-topic `CreateTopics` error for the stub (no-bridge) path.
///
/// KIP-464: `num_partitions = -1` / `replication_factor = -1` mean broker default when either
/// (a) the version is v4+, or (b) the topic carries a manual partition assignment (valid on
/// v2/v3 as well). Otherwise non-positive values are [`ERROR_INVALID_PARTITIONS`] /
/// [`ERROR_INVALID_REPLICATION_FACTOR`]. When validation passes, the stub returns
/// [`ERROR_NOT_CONTROLLER`] so clients do not believe the topic was created.
const fn topic_error(version: i16, topic: &CreatableTopic, forced_error: i16) -> i16 {
    if forced_error != ERROR_NONE {
        return forced_error;
    }

    let broker_default_ok = version >= 4 || !topic.assignments.is_empty();

    let partitions_ok = if broker_default_ok {
        topic.num_partitions == -1 || topic.num_partitions > 0
    } else {
        topic.num_partitions > 0
    };
    if !partitions_ok {
        return ERROR_INVALID_PARTITIONS;
    }

    let replication_ok = if broker_default_ok {
        topic.replication_factor == -1 || topic.replication_factor > 0
    } else {
        topic.replication_factor > 0
    };
    if !replication_ok {
        return ERROR_INVALID_REPLICATION_FACTOR;
    }

    ERROR_NOT_CONTROLLER
}

fn encode_inner(version: i16, topics: &[CreatableTopic], forced_error: i16) -> Result<Bytes> {
    let results = topics
        .iter()
        .map(|topic| {
            CreatableTopicResult::default()
                .with_name(topic.name.clone())
                .with_error_code(topic_error(version, topic, forced_error))
                .with_error_message(None)
                .with_num_partitions(topic.num_partitions)
                .with_replication_factor(topic.replication_factor)
        })
        .collect();
    let resp = CreateTopicsResponse::default().with_topics(results);
    encode_message(&resp, version, 256)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topic_name(name: &str) -> TopicName {
        TopicName(StrBytes::from_string(name.to_string()))
    }

    fn creatable_topic(num_partitions: i32, replication_factor: i16) -> CreatableTopic {
        CreatableTopic::default()
            .with_name(topic_name("orders"))
            .with_num_partitions(num_partitions)
            .with_replication_factor(replication_factor)
    }

    fn assignment(partition_index: i32) -> CreatableReplicaAssignment {
        CreatableReplicaAssignment::default()
            .with_partition_index(partition_index)
            .with_broker_ids(vec![SINGLE_BROKER_ID.into()])
    }

    fn assignment_with_replicas(
        partition_index: i32,
        broker_ids: Vec<i32>,
    ) -> CreatableReplicaAssignment {
        CreatableReplicaAssignment::default()
            .with_partition_index(partition_index)
            .with_broker_ids(broker_ids.into_iter().map(BrokerId).collect())
    }

    #[test]
    fn explicit_partitions_and_replication_resolve_as_given() {
        let topic = creatable_topic(3, 1);
        assert_eq!(validate_create_topic_shape(2, &topic), Ok(3));
    }

    #[test]
    fn v4_plus_accepts_broker_default_sentinel_with_no_assignments() {
        let topic = creatable_topic(-1, -1);
        assert_eq!(
            validate_create_topic_shape(4, &topic),
            Ok(DEFAULT_PARTITION_COUNT)
        );
    }

    #[test]
    fn pre_v4_rejects_broker_default_sentinel_with_no_assignments() {
        let topic = creatable_topic(-1, -1);
        assert_eq!(
            validate_create_topic_shape(2, &topic),
            Err(ERROR_INVALID_PARTITIONS)
        );
    }

    #[test]
    fn pre_v4_accepts_broker_default_sentinel_with_a_manual_assignment() {
        let topic = creatable_topic(-1, -1).with_assignments(vec![assignment(0), assignment(1)]);
        assert_eq!(validate_create_topic_shape(2, &topic), Ok(2));
    }

    #[test]
    fn manual_assignment_with_an_empty_replica_list_is_rejected() {
        let topic =
            creatable_topic(-1, -1).with_assignments(vec![assignment_with_replicas(0, vec![])]);
        assert_eq!(
            validate_create_topic_shape(2, &topic),
            Err(ERROR_INVALID_REPLICA_ASSIGNMENT)
        );
    }

    #[test]
    fn manual_assignment_with_a_duplicated_replica_is_rejected() {
        let topic =
            creatable_topic(-1, -1).with_assignments(vec![assignment_with_replicas(0, vec![1, 1])]);
        assert_eq!(
            validate_create_topic_shape(2, &topic),
            Err(ERROR_INVALID_REPLICA_ASSIGNMENT)
        );
    }

    #[test]
    fn manual_assignment_naming_an_unregistered_broker_is_rejected() {
        // This gateway advertises exactly one broker (node id 1) - naming any other id claims a
        // replica placement on a broker Metadata never advertised.
        let topic =
            creatable_topic(-1, -1).with_assignments(vec![assignment_with_replicas(0, vec![7])]);
        assert_eq!(
            validate_create_topic_shape(2, &topic),
            Err(ERROR_INVALID_REPLICA_ASSIGNMENT)
        );
    }

    #[test]
    fn zero_partitions_is_rejected_regardless_of_version() {
        let topic = creatable_topic(0, 1);
        assert_eq!(
            validate_create_topic_shape(5, &topic),
            Err(ERROR_INVALID_PARTITIONS)
        );
    }

    #[test]
    fn zero_replication_factor_is_rejected_regardless_of_version() {
        let topic = creatable_topic(1, 0);
        assert_eq!(
            validate_create_topic_shape(5, &topic),
            Err(ERROR_INVALID_REPLICATION_FACTOR)
        );
    }

    #[test]
    fn explicit_num_partitions_with_assignments_is_rejected_even_when_it_agrees_with_their_length()
    {
        // Real Kafka rejects an explicit num_partitions alongside a manual assignment outright -
        // a manual assignment requires num_partitions == -1, full stop. Agreeing with
        // assignments.len() does not make the combination valid; there is no wire rule saying
        // which one would win if it did.
        let topic = creatable_topic(2, 1).with_assignments(vec![assignment(0), assignment(1)]);
        assert_eq!(
            validate_create_topic_shape(5, &topic),
            Err(ERROR_INVALID_REQUEST)
        );
    }

    #[test]
    fn explicit_num_partitions_disagreeing_with_assignments_length_is_also_rejected() {
        let topic = creatable_topic(3, 1).with_assignments(vec![assignment(0), assignment(1)]);
        assert_eq!(
            validate_create_topic_shape(5, &topic),
            Err(ERROR_INVALID_REQUEST)
        );
    }

    #[test]
    fn explicit_replication_factor_with_assignments_is_rejected_even_with_num_partitions_at_minus_one()
     {
        let topic = creatable_topic(-1, 1).with_assignments(vec![assignment(0), assignment(1)]);
        assert_eq!(
            validate_create_topic_shape(5, &topic),
            Err(ERROR_INVALID_REQUEST)
        );
    }

    #[test]
    fn find_duplicate_names_finds_a_name_repeated_across_two_requested_topics() {
        let topics = vec![creatable_topic(1, 1), creatable_topic(1, 1)];
        let duplicates = find_duplicate_names(&topics);
        assert_eq!(duplicates, HashSet::from([topic_name("orders")]));
    }

    #[test]
    fn find_duplicate_names_is_empty_when_every_name_is_unique() {
        let topics = vec![
            creatable_topic(1, 1),
            CreatableTopic::default()
                .with_name(topic_name("payments"))
                .with_num_partitions(1)
                .with_replication_factor(1),
        ];
        assert!(find_duplicate_names(&topics).is_empty());
    }

    #[test]
    fn clamp_request_timeout_rejects_a_zero_or_negative_value_up_to_the_floor() {
        assert_eq!(clamp_request_timeout(0), MIN_REQUEST_TIMEOUT);
        assert_eq!(clamp_request_timeout(-1), MIN_REQUEST_TIMEOUT);
        assert_eq!(clamp_request_timeout(i32::MIN), MIN_REQUEST_TIMEOUT);
    }

    #[test]
    fn clamp_request_timeout_caps_an_oversized_value_at_the_ceiling() {
        assert_eq!(clamp_request_timeout(i32::MAX), MAX_REQUEST_TIMEOUT);
    }

    #[test]
    fn clamp_request_timeout_passes_through_a_reasonable_value_unchanged() {
        assert_eq!(clamp_request_timeout(5_000), Duration::from_secs(5));
    }
}
