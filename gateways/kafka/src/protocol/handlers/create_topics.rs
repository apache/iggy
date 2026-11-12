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

use bytes::Bytes;
use iggy::prelude::IggyError;
use iggy_common::MAX_REPLICATION_FACTOR;
use kafka_protocol::messages::create_topics_request::{CreatableReplicaAssignment, CreatableTopic};
use kafka_protocol::messages::create_topics_response::{
    CreatableTopicConfigs, CreatableTopicResult,
};
use kafka_protocol::messages::{BrokerId, CreateTopicsRequest, CreateTopicsResponse};
use kafka_protocol::protocol::StrBytes;

use tokio::sync::Mutex;
use tokio::time::Instant;

use crate::bridge::{
    BridgeError, DEFAULT_TOPIC_PARTITIONS, IggyBridge, KafkaTopicSettings, MAX_TOPIC_PARTITIONS,
    TopicConfigSnapshot, TopicCreationOutcome,
};
use crate::error::Result;
use crate::protocol::api::{
    API_KEY_CREATE_TOPICS, ApiVersionRange, ConnectionState, ERROR_INVALID_CONFIG,
    ERROR_INVALID_PARTITIONS, ERROR_INVALID_REPLICA_ASSIGNMENT, ERROR_INVALID_REPLICATION_FACTOR,
    ERROR_INVALID_REQUEST, ERROR_NONE, ERROR_NOT_CONTROLLER, ERROR_POLICY_VIOLATION,
    ERROR_REQUEST_TIMED_OUT, ERROR_TOPIC_ALREADY_EXISTS, GatewayState, HandleOutcome,
};
use crate::protocol::bounds_guard::validate_create_topics_shape;
use crate::protocol::handlers::topic_config::keys::{ConfigContext, topic_entries};
use crate::protocol::handlers::topic_config::{
    MAX_CONFIG_TOPICS, find_duplicate_names, log_compaction, log_retention_below_segment,
    plan_create, topic_cap_message,
};
use crate::protocol::handlers::{
    another_controller, clamp_request_timeout, decode_guarded, encode_message,
    handle_versioned_request, is_supported_version, respond_or_close, unsupported_version_response,
};

pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_CREATE_TOPICS,
    min_version: 2,
    max_version: 5,
};

pub async fn handle(
    state: &GatewayState,
    connection: &ConnectionState,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
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
    if another_controller(state, &connection.brokers) {
        return respond_or_close(
            encode_inner(api_version, &req.topics, ERROR_NOT_CONTROLLER),
            "CreateTopics",
        );
    }

    let topic_names: Vec<&str> = req.topics.iter().map(|topic| topic.name.as_str()).collect();
    let duplicate_names = find_duplicate_names(topic_names.iter().copied());

    let distinct_bridge_backed: HashSet<&str> = topic_names
        .iter()
        .copied()
        .filter(|name| !duplicate_names.contains(name))
        .collect();
    if distinct_bridge_backed.len() > MAX_CONFIG_TOPICS {
        tracing::warn!(
            distinct_topics = distinct_bridge_backed.len(),
            max = MAX_CONFIG_TOPICS,
            "CreateTopics request addresses too many distinct topics; rejecting"
        );
        // A server-imposed limit, not a malformed request. INVALID_REQUEST would blame the
        // client for a request Kafka itself would accept.
        let message = StrBytes::from(topic_cap_message("CreateTopics"));
        let results = req
            .topics
            .iter()
            .map(|topic| {
                CreatableTopicResult::default()
                    .with_name(topic.name.clone())
                    .with_error_code(ERROR_POLICY_VIOLATION)
                    .with_error_message(Some(message.clone()))
            })
            .collect();
        let resp = CreateTopicsResponse::default().with_topics(results);
        return respond_or_close(encode_message(&resp, api_version, 256), "CreateTopics");
    }

    let deadline = Instant::now() + clamp_request_timeout(req.timeout_ms);
    let request = TopicsRequest {
        api_version,
        validate_only: req.validate_only,
        node_id: state.node_id,
        context: ConfigContext::of(bridge, state.max_frame_size),
    };
    let results = create_all_topics(
        bridge,
        &state.partition_growth,
        &req.topics,
        &duplicate_names,
        &request,
        deadline,
    )
    .await;
    let resp = CreateTopicsResponse::default().with_topics(results);
    respond_or_close(encode_message(&resp, api_version, 256), "CreateTopics")
}

/// Creates (or reports on) every requested topic, skipping the bridge entirely for a duplicate
/// name - real Kafka refuses the whole name, not a first-wins/last-wins split: creating one
/// occurrence and reporting `TOPIC_ALREADY_EXISTS` for the other would let a client observe a
/// create it never got a `NONE` for (`AdminClient` keys its futures by name, so a second
/// per-topic result for the same name is silently discarded client-side regardless of which one
/// this bridge picked).
///
/// `deadline` bounds each topic's own bridge work individually (`timeout_at`), not the whole
/// loop: a single `timeout` around the entire call would discard every already-resolved result
/// the moment one topic's call ran long, answering `REQUEST_TIMED_OUT` even for topics that had
/// already committed.
///
/// `tokio::time::timeout_at` polls the inner future before it ever checks the deadline (see
/// `Timeout::poll`), so once `deadline` has already passed, wrapping a fresh bridge call in it
/// still starts that call - a topic past the deadline would both answer `REQUEST_TIMED_OUT` *and*
/// actually get created. The `Instant::now() >= deadline` check below must run and must skip the
/// call itself, the same pattern `list_offsets.rs` uses, not rely on `timeout_at` to skip it.
async fn create_all_topics(
    bridge: &IggyBridge,
    partition_growth: &Mutex<()>,
    topics: &[CreatableTopic],
    duplicate_names: &HashSet<&str>,
    request: &TopicsRequest<'_>,
    deadline: Instant,
) -> Vec<CreatableTopicResult> {
    let mut results = Vec::with_capacity(topics.len());
    let mut deadline_exceeded = false;
    for topic in topics {
        if duplicate_names.contains(topic.name.as_str()) {
            results.push(
                CreatableTopicResult::default()
                    .with_name(topic.name.clone())
                    .with_error_code(ERROR_INVALID_REQUEST)
                    .with_error_message(None),
            );
            continue;
        }
        if deadline_exceeded || Instant::now() >= deadline {
            if !deadline_exceeded {
                deadline_exceeded = true;
                tracing::warn!(
                    "CreateTopics deadline passed; answering remaining topics retriable \
                     instead of starting new Iggy calls"
                );
            }
            results.push(
                CreatableTopicResult::default()
                    .with_name(topic.name.clone())
                    .with_error_code(ERROR_REQUEST_TIMED_OUT)
                    .with_error_message(None),
            );
            continue;
        }

        let create = create_one_topic(bridge, partition_growth, topic, request);
        let result = match tokio::time::timeout_at(deadline, create).await {
            Ok(result) => result,
            Err(_elapsed) => {
                deadline_exceeded = true;
                tracing::warn!(
                    kafka_topic = topic.name.as_str(),
                    "CreateTopics: this topic's bridge work exceeded the request deadline; \
                     answering retriable instead of blocking further"
                );
                CreatableTopicResult::default()
                    .with_name(topic.name.clone())
                    .with_error_code(ERROR_REQUEST_TIMED_OUT)
                    .with_error_message(None)
            }
        };
        results.push(result);
    }
    results
}

/// Validates and, when the topic is not rejected outright, provisions one requested topic.
///
/// Existence beats a local (config/shape) rejection, not the other way around: real Kafka's
/// controller checks its in-memory topic set before it ever looks at the request's configs or
/// shape, so an existing topic answers `TOPIC_ALREADY_EXISTS` regardless of whether the new
/// request would itself be valid. Kafka Connect's idempotent bootstrap depends on this - it
/// resends `cleanup.policy=compact` against topics it doesn't know already exist, and expects
/// `ALREADY_EXISTS` back, not a config rejection. The existence probe only runs when a local
/// check would otherwise reject, so the common (valid-request) path pays no extra round trip for
/// it.
///
/// `validate_only` and the real create path diverge deliberately past that point, not just in
/// whether they call the bridge: `validate_only` never mutates anything, so a plain existence
/// read ([`IggyBridge::get_kafka_topic`]) is fine - there's no race to protect against when
/// nothing gets created either way. The real path instead calls
/// [`IggyBridge::create_kafka_topic`], which folds the existence check and the create into one
/// atomic call - a separate read-then-write here would let two concurrent `CreateTopics` for the
/// same new name both observe `Ok(None)` and both receive `NONE`, when Kafka guarantees exactly
/// one caller does.
async fn create_one_topic(
    bridge: &IggyBridge,
    partition_growth: &Mutex<()>,
    topic: &CreatableTopic,
    request: &TopicsRequest<'_>,
) -> CreatableTopicResult {
    let result = CreatableTopicResult::default()
        .with_name(topic.name.clone())
        .with_error_message(None);
    let kafka_topic = topic.name.as_str();

    let (partition_count, settings) =
        match local_shape_error(topic, request.node_id, &request.context) {
            Ok(planned) => planned,
            Err((error_code, error_message)) => {
                return match bridge.get_kafka_topic(kafka_topic).await {
                    Ok(Some(_existing)) => result.with_error_code(ERROR_TOPIC_ALREADY_EXISTS),
                    // A failed existence probe doesn't get to mask a real, independently-valid
                    // rejection - the request has a local defect either way.
                    Ok(None) | Err(_) => result
                        .with_error_code(error_code)
                        .with_error_message(error_message),
                };
            }
        };

    // Kafka reports the factor the topic got. A real create gets it from Iggy. A `validate_only`
    // answer and a replayed create take the default the gateway read last for -1, and report -1
    // only before the first read.
    let success = |stored_factor: Option<u32>| {
        let replication_factor = stored_factor
            .and_then(|factor| i16::try_from(factor).ok())
            .unwrap_or(topic.replication_factor);
        // Only v5 carries the configs, so an older answer does not build them.
        let configs = (request.api_version >= 5)
            .then(|| created_configs(&request.context, partition_count, &settings, stored_factor));
        result
            .clone()
            .with_error_code(ERROR_NONE)
            .with_num_partitions(i32::try_from(partition_count).unwrap_or(i32::MAX))
            .with_replication_factor(replication_factor)
            .with_configs(configs)
    };
    let requested_or_default = settings
        .replication_factor
        .or(request.context.default_replication_factor);

    if request.validate_only {
        return match bridge.get_kafka_topic(kafka_topic).await {
            Ok(Some(_existing)) => result.with_error_code(ERROR_TOPIC_ALREADY_EXISTS),
            Ok(None) => beyond_the_cluster(topic.replication_factor, bridge.iggy_node_count())
                .map_or_else(
                    || success(requested_or_default),
                    |reason| {
                        result
                            .clone()
                            .with_error_code(ERROR_INVALID_REPLICATION_FACTOR)
                            .with_error_message(Some(StrBytes::from(reason)))
                    },
                ),
            Err(err) => bridge_error_result(result, kafka_topic, &err, None),
        };
    }

    // `CreatePartitions` reads a topic and then adds to it by name. Holding its lock here keeps
    // a topic created under that name from appearing between its read and its write.
    let _growth = partition_growth.lock().await;
    match bridge
        .create_kafka_topic_with(kafka_topic, partition_count, &settings)
        .await
    {
        // The second arm: the write committed on its first attempt, the SDK's own reconnect path
        // replayed it, and the server's client-table dedup caught the replay - not a fault.
        // `to_kafka_error_code`'s shared mapping deliberately doesn't special-case this - it's a
        // write-only fact, checked here, at the one write this bridge makes, rather than assumed
        // true for the reads that share that mapping too.
        Ok(TopicCreationOutcome::Created { replication_factor }) => {
            log_created(kafka_topic, &settings);
            success(replication_factor)
        }
        Err(BridgeError::Iggy(IggyError::RequestAlreadyApplied)) => {
            log_created(kafka_topic, &settings);
            success(requested_or_default)
        }
        Ok(TopicCreationOutcome::AlreadyExists) => {
            result.with_error_code(ERROR_TOPIC_ALREADY_EXISTS)
        }
        Err(err) => bridge_error_result(
            result,
            kafka_topic,
            &err,
            Some((topic.replication_factor, bridge.iggy_node_count())),
        ),
    }
}

/// What one `CreateTopics` request asks of each of its topics, beside the topic itself.
struct TopicsRequest<'a> {
    api_version: i16,
    validate_only: bool,
    node_id: i32,
    context: ConfigContext<'a>,
}

/// Logs what a created topic does differently from what its request asked.
fn log_created(kafka_topic: &str, settings: &KafkaTopicSettings) {
    log_compaction(kafka_topic, settings.cleanup_policy, true);
    if let Some(below) = settings.retention_below_segment() {
        log_retention_below_segment(kafka_topic, below);
    }
}

/// `configs`-then-shape local validation, bundled so [`create_one_topic`] can probe existence
/// once, only on the rejection path. Returns the partition count and the settings the configs
/// name, or `(error_code, error_message)` rather than a whole [`CreatableTopicResult`] to keep
/// this `Result`'s `Err` arm small (`clippy::result_large_err`); the caller already holds the
/// shared `name`/base fields to rebuild the full result from.
///
/// `min.insync.replicas` is checked against the factor the topic gets: the one it asks for, else
/// the default that the gateway read last.
fn local_shape_error(
    topic: &CreatableTopic,
    node_id: i32,
    context: &ConfigContext<'_>,
) -> core::result::Result<(u32, KafkaTopicSettings), (i16, Option<StrBytes>)> {
    let replication_factor = u32::try_from(topic.replication_factor)
        .ok()
        .filter(|factor| *factor > 0);
    let settings = plan_create(
        topic
            .configs
            .iter()
            .map(|config| (config.name.as_str(), config.value.as_deref())),
        context,
        replication_factor.or(context.default_replication_factor),
    )
    .map_err(|fault| (ERROR_INVALID_CONFIG, Some(StrBytes::from(fault.message()))))?;
    let partition_count = validate_create_topic_shape(topic, node_id).map_err(|code| {
        let reason = (code == ERROR_INVALID_REPLICATION_FACTOR)
            .then(|| StrBytes::from(replication_factor_reason(topic.replication_factor, None)));
        (code, reason)
    })?;
    let settings = KafkaTopicSettings {
        replication_factor,
        ..settings
    };
    Ok((partition_count, settings))
}

/// Kafka's reason for a replication factor it refuses, with Iggy's bounds in place of the broker
/// count: 1 to 32 nodes, and at most every node of the cluster. `node_count` is the cluster size
/// when the gateway knows it.
fn replication_factor_reason(requested: i16, node_count: Option<usize>) -> String {
    if requested < 1 {
        return "Replication factor must be larger than 0, or -1 to use the default value."
            .to_string();
    }
    let unable = format!("Unable to replicate the partition {requested} time(s)");
    if u32::try_from(requested).is_ok_and(|factor| factor > MAX_REPLICATION_FACTOR) {
        return format!(
            "{unable}: Iggy keeps each partition on at most {MAX_REPLICATION_FACTOR} nodes."
        );
    }
    node_count.map_or_else(
        || format!("{unable}: Iggy refused a replication factor above its node count."),
        |count| {
            format!(
                "{unable}: The target replication factor of {requested} cannot be reached \
                 because only {count} Iggy node(s) are in the cluster."
            )
        },
    )
}

/// The reason a replication factor exceeds the `node_count` nodes of the Iggy cluster, or `None`
/// when it fits or the count is unknown. Only `validate_only` asks, because a real create gets the
/// answer from Iggy, which knows a node that joined since the last roster read.
fn beyond_the_cluster(requested: i16, node_count: Option<usize>) -> Option<String> {
    let count = node_count?;
    (usize::try_from(requested).is_ok_and(|factor| factor > count))
        .then(|| replication_factor_reason(requested, Some(count)))
}

/// The configs of the new topic that a v5 response carries, as a describe of it reports them.
/// `replication_factor` is the factor the topic got, when the gateway knows it.
fn created_configs(
    context: &ConfigContext<'_>,
    partition_count: u32,
    settings: &KafkaTopicSettings,
    replication_factor: Option<u32>,
) -> Vec<CreatableTopicConfigs> {
    let options = settings.create_options(partition_count, context.defaults);
    let created = TopicConfigSnapshot {
        replication_factor,
        ..TopicConfigSnapshot::created_with(&options)
    };
    topic_entries(&created, context, None)
        .into_iter()
        .map(|entry| {
            CreatableTopicConfigs::default()
                .with_name(StrBytes::from_static_str(entry.name))
                .with_value(entry.value.map(StrBytes::from))
                .with_read_only(entry.read_only)
                .with_config_source(entry.source)
                .with_is_sensitive(false)
        })
        .collect()
}

/// Maps a bridge failure to a Kafka result, logging the real cause server-side.
///
/// The Kafka client never sees more than the fixed text below: `err.to_string()`'s own embedded
/// detail can be wrong for an error reconstructed from a bare wire status code
/// (`BridgeError::Iggy`'s own doc explains why `IggyError::from_code` fills data fields with
/// defaults on that path), so sending it to the client risks sending a wrong claim rather than no
/// claim. The two client-caused variants are the exception - their text is fixed and always
/// correct, so it's safe to forward and logged at `debug!` (attacker/misuse-controlled, not
/// operator-actionable). `PartitionsLimitReached` carries no data, so its fixed text is correct
/// too, and it is logged at `warn!` for the operator who set the cap. Everything else points at
/// the bridge or Iggy itself and is logged at `error!` (`bridge/error.rs:155`'s own guidance:
/// handlers log the real Iggy error).
fn bridge_error_result(
    result: CreatableTopicResult,
    kafka_topic: &str,
    err: &BridgeError,
    replication: Option<(i16, Option<usize>)>,
) -> CreatableTopicResult {
    let error_code = err.to_kafka_error_code();
    match err {
        BridgeError::Iggy(IggyError::InvalidReplicationFactor) => {
            tracing::debug!(
                kafka_topic,
                "CreateTopics: Iggy refused the replication factor"
            );
            let reason = replication.map_or_else(
                || "Iggy refused the replication factor".to_string(),
                |(requested, node_count)| replication_factor_reason(requested, node_count),
            );
            result
                .with_error_code(error_code)
                .with_error_message(Some(StrBytes::from(reason)))
        }
        BridgeError::InvalidKafkaTopicName { reason, .. } => {
            tracing::debug!(
                kafka_topic,
                reason,
                "CreateTopics rejected an invalid topic name"
            );
            result
                .with_error_code(error_code)
                .with_error_message(Some(StrBytes::from(reason.clone())))
        }
        BridgeError::InvalidPartitionCount { .. } => {
            tracing::debug!(
                kafka_topic,
                "CreateTopics rejected an invalid partition count"
            );
            result
                .with_error_code(error_code)
                .with_error_message(Some(StrBytes::from(
                    "partition count must be at least 1".to_string(),
                )))
        }
        BridgeError::Iggy(IggyError::PartitionsLimitReached) => {
            tracing::warn!(
                kafka_topic,
                "CreateTopics refused: the Iggy node is at metadata.partitions_max"
            );
            result
                .with_error_code(error_code)
                .with_error_message(Some(StrBytes::from(
                    "the Iggy node has reached its partition limit".to_string(),
                )))
        }
        other => {
            tracing::error!(kafka_topic, %other, "CreateTopics failed against the Iggy bridge");
            result
                .with_error_code(error_code)
                .with_error_message(Some(StrBytes::from(
                    "internal error provisioning this topic".to_string(),
                )))
        }
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
/// index: Metadata names this gateway (`node_id`) as the one replica of every partition, so the
/// only legal replica list is `[node_id]` - never empty, never repeating it, never naming another
/// broker. Real Kafka's `ReplicationControlManager` validates the whole
/// binding this way, not just the index.
///
/// With no assignments, `num_partitions = -1` / `replication_factor = -1` mean "use the broker
/// default" at every version. The Java client refuses to send them below v4, but Kafka's
/// controller never checks the version (`ReplicationControlManager.createTopic`), and
/// `KafkaJS` `createTopics()` sends both at v3.
fn validate_create_topic_shape(
    topic: &CreatableTopic,
    node_id: i32,
) -> core::result::Result<u32, i16> {
    if !topic.assignments.is_empty() {
        if topic.num_partitions != -1 || topic.replication_factor != -1 {
            return Err(ERROR_INVALID_REQUEST);
        }
        if !assignment_indices_are_consecutive_from_zero(&topic.assignments)
            || !assignment_replicas_are_valid(&topic.assignments, node_id)
        {
            return Err(ERROR_INVALID_REPLICA_ASSIGNMENT);
        }
        return Ok(u32::try_from(topic.assignments.len()).unwrap_or(DEFAULT_TOPIC_PARTITIONS));
    }

    // Checked ahead of (and separately from) the partition-count check below: that check's own
    // `INVALID_PARTITIONS` (37) means "count is below 1" on the wire (`bridge/error.rs`'s own
    // comment on `IggyError::TooManyPartitions`), the opposite condition from "too many" - reusing
    // it here would send a client-visible message that contradicts the request it just sent. This
    // is the same code the real (non-`validate_only`) path eventually gets back from the bridge
    // once a count this large actually reaches `create_kafka_topic`, so `validate_only` now
    // answers what the real create would.
    if u32::try_from(topic.num_partitions).is_ok_and(|count| count > MAX_TOPIC_PARTITIONS) {
        return Err(ERROR_INVALID_REQUEST);
    }

    if topic.num_partitions != -1 && topic.num_partitions <= 0 {
        return Err(ERROR_INVALID_PARTITIONS);
    }

    // Iggy stores the factor (#4692): 1 to 32 nodes. It refuses a factor above its node count
    // itself, as Kafka's controller refuses one above its broker count.
    if topic.replication_factor != -1
        && !u32::try_from(topic.replication_factor)
            .is_ok_and(|factor| (1..=MAX_REPLICATION_FACTOR).contains(&factor))
    {
        return Err(ERROR_INVALID_REPLICATION_FACTOR);
    }

    let partition_count = if topic.num_partitions > 0 {
        u32::try_from(topic.num_partitions).unwrap_or(DEFAULT_TOPIC_PARTITIONS)
    } else {
        DEFAULT_TOPIC_PARTITIONS
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

/// Whether every partition names `[node_id]`, the one replica Metadata names for each partition:
/// length 1 rules out both empty and a duplicated id, and the value rules out another broker.
/// `CreatePartitions` holds a manual assignment to the same list.
fn assignment_replicas_are_valid(assignments: &[CreatableReplicaAssignment], node_id: i32) -> bool {
    assignments
        .iter()
        .all(|assignment| assignment.broker_ids == [BrokerId(node_id)])
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
/// KIP-464: `num_partitions = -1` / `replication_factor = -1` mean broker default at every
/// version, as in [`validate_create_topic_shape`]. Other non-positive values are
/// [`ERROR_INVALID_PARTITIONS`] / [`ERROR_INVALID_REPLICATION_FACTOR`]. When validation passes,
/// the stub returns [`ERROR_NOT_CONTROLLER`] so clients do not believe the topic was created.
const fn topic_error(topic: &CreatableTopic, forced_error: i16) -> i16 {
    if forced_error != ERROR_NONE {
        return forced_error;
    }
    if topic.num_partitions != -1 && topic.num_partitions <= 0 {
        return ERROR_INVALID_PARTITIONS;
    }
    if topic.replication_factor != -1 && topic.replication_factor <= 0 {
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
                .with_error_code(topic_error(topic, forced_error))
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
    use kafka_protocol::messages::TopicName;
    use kafka_protocol::messages::create_topics_request::CreatableTopicConfig;

    use super::*;
    use crate::bridge::TopicDefaults;

    /// The node id of the gateway under test.
    const NODE_ID: i32 = 1;

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
            .with_broker_ids(vec![BrokerId(NODE_ID)])
    }

    fn assignment_with_replicas(
        partition_index: i32,
        broker_ids: Vec<i32>,
    ) -> CreatableReplicaAssignment {
        CreatableReplicaAssignment::default()
            .with_partition_index(partition_index)
            .with_broker_ids(broker_ids.into_iter().map(BrokerId).collect())
    }

    fn with_config(topic: CreatableTopic, name: &str, value: &str) -> CreatableTopic {
        let config = CreatableTopicConfig::default()
            .with_name(StrBytes::from(name.to_string()))
            .with_value(Some(StrBytes::from(value.to_string())));
        topic.with_configs(vec![config])
    }

    /// The context of a gateway with the default 8 MiB frame and `default_replication_factor`.
    fn context(
        defaults: &TopicDefaults,
        default_replication_factor: Option<u32>,
    ) -> ConfigContext<'_> {
        ConfigContext {
            defaults,
            max_message_bytes: 8 * 1024 * 1024,
            default_replication_factor,
        }
    }

    /// [`local_shape_error`] on a gateway that has not read the default replication factor yet.
    fn shape(
        topic: &CreatableTopic,
    ) -> core::result::Result<(u32, KafkaTopicSettings), (i16, Option<StrBytes>)> {
        local_shape_error(topic, NODE_ID, &context(&TopicDefaults::default(), None))
    }

    #[test]
    fn given_min_insync_replicas_should_check_it_against_the_factor_the_topic_gets() {
        let asked = with_config(creatable_topic(1, 3), "min.insync.replicas", "3");
        let (code, message) = shape(&asked).expect_err("above the majority of 3");
        assert_eq!(code, ERROR_INVALID_CONFIG);
        assert!(
            message
                .as_ref()
                .is_some_and(|text| text.as_str().contains("accepts at most 2")),
            "{message:?}"
        );
        let defaulted = with_config(creatable_topic(1, -1), "min.insync.replicas", "2");
        assert!(shape(&defaulted).is_ok(), "the default factor is not known");
        let defaults = TopicDefaults::default();
        assert!(
            local_shape_error(&defaulted, NODE_ID, &context(&defaults, Some(1))).is_err(),
            "a default factor of 1 has a majority of 1"
        );
    }

    #[test]
    fn given_a_refused_config_and_a_bad_shape_should_answer_the_config_first() {
        let topic = with_config(
            creatable_topic(0, 1),
            "message.timestamp.type",
            "LogAppendTime",
        );
        let (code, message) = shape(&topic).expect_err("refused");
        assert_eq!(code, ERROR_INVALID_CONFIG);
        assert!(
            message
                .as_ref()
                .is_some_and(|text| text.as_str().contains("accepts only CreateTime")),
            "{message:?}"
        );
    }

    #[test]
    fn given_valid_configs_should_plan_the_settings_with_the_partition_count() {
        let topic = with_config(creatable_topic(3, 1), "retention.ms", "60000");
        let (count, settings) = shape(&topic).expect("valid");
        assert_eq!(count, 3);
        assert!(settings.message_expiry.is_some());
        assert_eq!(settings.retention_bytes, None);
    }

    #[test]
    fn explicit_partitions_and_replication_resolve_as_given() {
        let topic = creatable_topic(3, 1);
        assert_eq!(validate_create_topic_shape(&topic, NODE_ID), Ok(3));
    }

    #[test]
    fn broker_default_sentinels_with_no_assignments_resolve_to_the_default_count() {
        assert_eq!(
            validate_create_topic_shape(&creatable_topic(-1, -1), NODE_ID),
            Ok(DEFAULT_TOPIC_PARTITIONS)
        );
        assert_eq!(
            validate_create_topic_shape(&creatable_topic(-1, 1), NODE_ID),
            Ok(DEFAULT_TOPIC_PARTITIONS)
        );
        assert_eq!(
            validate_create_topic_shape(&creatable_topic(4, -1), NODE_ID),
            Ok(4)
        );
    }

    #[test]
    fn broker_default_sentinels_with_a_manual_assignment_take_its_length() {
        let topic = creatable_topic(-1, -1).with_assignments(vec![assignment(0), assignment(1)]);
        assert_eq!(validate_create_topic_shape(&topic, NODE_ID), Ok(2));
    }

    #[test]
    fn given_another_node_id_when_assigning_replicas_should_accept_only_this_gateway() {
        let own =
            creatable_topic(-1, -1).with_assignments(vec![assignment_with_replicas(0, vec![2])]);
        let other =
            creatable_topic(-1, -1).with_assignments(vec![assignment_with_replicas(0, vec![1])]);

        assert_eq!(validate_create_topic_shape(&own, 2), Ok(1));
        assert_eq!(
            validate_create_topic_shape(&other, 2),
            Err(ERROR_INVALID_REPLICA_ASSIGNMENT)
        );
    }

    #[test]
    fn manual_assignment_with_an_empty_replica_list_is_rejected() {
        let topic =
            creatable_topic(-1, -1).with_assignments(vec![assignment_with_replicas(0, vec![])]);
        assert_eq!(
            validate_create_topic_shape(&topic, NODE_ID),
            Err(ERROR_INVALID_REPLICA_ASSIGNMENT)
        );
    }

    #[test]
    fn manual_assignment_with_a_duplicated_replica_is_rejected() {
        let topic =
            creatable_topic(-1, -1).with_assignments(vec![assignment_with_replicas(0, vec![1, 1])]);
        assert_eq!(
            validate_create_topic_shape(&topic, NODE_ID),
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
            validate_create_topic_shape(&topic, NODE_ID),
            Err(ERROR_INVALID_REPLICA_ASSIGNMENT)
        );
    }

    #[test]
    fn zero_partitions_is_rejected_regardless_of_version() {
        let topic = creatable_topic(0, 1);
        assert_eq!(
            validate_create_topic_shape(&topic, NODE_ID),
            Err(ERROR_INVALID_PARTITIONS)
        );
    }

    #[test]
    fn zero_replication_factor_is_rejected_regardless_of_version() {
        let topic = creatable_topic(1, 0);
        assert_eq!(
            validate_create_topic_shape(&topic, NODE_ID),
            Err(ERROR_INVALID_REPLICATION_FACTOR)
        );
    }

    #[test]
    fn given_a_replication_factor_iggy_can_store_should_accept_it() {
        for factor in [1, 2, 3, 32] {
            assert_eq!(
                validate_create_topic_shape(&creatable_topic(1, factor), NODE_ID),
                Ok(1),
                "{factor}"
            );
        }
        assert_eq!(
            validate_create_topic_shape(&creatable_topic(1, 33), NODE_ID),
            Err(ERROR_INVALID_REPLICATION_FACTOR)
        );
    }

    #[test]
    fn given_a_replication_factor_should_plan_it_for_iggy_or_take_the_server_default() {
        let (_, stored) = shape(&creatable_topic(2, 3)).expect("valid");
        assert_eq!(stored.replication_factor, Some(3));
        let (_, default) = shape(&creatable_topic(2, -1)).expect("valid");
        assert_eq!(default.replication_factor, None);
        let (code, reason) = shape(&creatable_topic(2, 40)).expect_err("above 32");
        assert_eq!(code, ERROR_INVALID_REPLICATION_FACTOR);
        assert_eq!(
            reason.as_deref(),
            Some(
                "Unable to replicate the partition 40 time(s): Iggy keeps each partition on at \
                 most 32 nodes."
            )
        );
        let (_, reason) = shape(&creatable_topic(2, 0)).expect_err("zero");
        assert_eq!(
            reason.as_deref(),
            Some("Replication factor must be larger than 0, or -1 to use the default value.")
        );
    }

    #[test]
    fn given_a_replication_factor_above_the_node_count_should_name_the_cluster_size() {
        assert_eq!(beyond_the_cluster(3, Some(3)), None);
        assert_eq!(beyond_the_cluster(-1, Some(1)), None);
        assert_eq!(
            beyond_the_cluster(3, None),
            None,
            "unknown size: Iggy decides"
        );
        assert_eq!(
            beyond_the_cluster(3, Some(1)).as_deref(),
            Some(
                "Unable to replicate the partition 3 time(s): The target replication factor of 3 \
                 cannot be reached because only 1 Iggy node(s) are in the cluster."
            )
        );
    }

    #[test]
    fn replication_factor_of_exactly_one_is_accepted() {
        let topic = creatable_topic(1, 1);
        assert_eq!(validate_create_topic_shape(&topic, NODE_ID), Ok(1));
    }

    #[test]
    fn partition_count_at_the_cap_is_accepted() {
        let topic = creatable_topic(i32::try_from(MAX_TOPIC_PARTITIONS).unwrap(), 1);
        assert_eq!(
            validate_create_topic_shape(&topic, NODE_ID),
            Ok(MAX_TOPIC_PARTITIONS)
        );
    }

    #[test]
    fn partition_count_above_the_cap_is_rejected_with_the_real_creates_own_code() {
        // Not INVALID_PARTITIONS (37): that code's wire text is "below 1", the opposite
        // condition. This is the same code the real (non-validate_only) path eventually gets
        // back from the bridge once IggyError::TooManyPartitions reaches it
        // (`bridge/error.rs::too_many_partitions_maps_to_invalid_request_not_invalid_partitions`).
        let topic = creatable_topic(i32::try_from(MAX_TOPIC_PARTITIONS).unwrap() + 1, 1);
        assert_eq!(
            validate_create_topic_shape(&topic, NODE_ID),
            Err(ERROR_INVALID_REQUEST)
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
            validate_create_topic_shape(&topic, NODE_ID),
            Err(ERROR_INVALID_REQUEST)
        );
    }

    #[test]
    fn explicit_num_partitions_disagreeing_with_assignments_length_is_also_rejected() {
        let topic = creatable_topic(3, 1).with_assignments(vec![assignment(0), assignment(1)]);
        assert_eq!(
            validate_create_topic_shape(&topic, NODE_ID),
            Err(ERROR_INVALID_REQUEST)
        );
    }

    #[test]
    fn explicit_replication_factor_with_assignments_is_rejected_even_with_num_partitions_at_minus_one()
     {
        let topic = creatable_topic(-1, 1).with_assignments(vec![assignment(0), assignment(1)]);
        assert_eq!(
            validate_create_topic_shape(&topic, NODE_ID),
            Err(ERROR_INVALID_REQUEST)
        );
    }

    #[test]
    fn find_duplicate_names_finds_a_name_repeated_across_two_requested_topics() {
        let topics = [creatable_topic(1, 1), creatable_topic(1, 1)];
        let names: Vec<&str> = topics.iter().map(|topic| topic.name.as_str()).collect();
        let duplicates = find_duplicate_names(names);
        assert_eq!(duplicates, HashSet::from(["orders"]));
    }

    #[test]
    fn find_duplicate_names_is_empty_when_every_name_is_unique() {
        let topics = [
            creatable_topic(1, 1),
            CreatableTopic::default()
                .with_name(topic_name("payments"))
                .with_num_partitions(1)
                .with_replication_factor(1),
        ];
        let names: Vec<&str> = topics.iter().map(|topic| topic.name.as_str()).collect();
        assert!(find_duplicate_names(names).is_empty());
    }
}
