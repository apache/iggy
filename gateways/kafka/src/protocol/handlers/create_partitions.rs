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

//! `CreatePartitions` (API key 37).
//!
//! Grows the Iggy topic a Kafka topic name resolves to. Like Kafka, a topic only grows: a
//! `count` at or below the current partition count answers `INVALID_PARTITIONS` (37) and
//! changes nothing. Iggy appends the new partitions after the current last index, so every
//! existing Kafka partition index keeps naming the same Iggy partition and no stored record
//! moves, the same way a Kafka broker grows a topic.
//!
//! Iggy's own `create_partitions` takes the number of partitions to add, with no expected current
//! count, while the Kafka request carries the number to reach. So each topic is read before it is
//! written, and `GatewayState::partition_growth` keeps that read and write together:
//!
//! - Two `CreatePartitions` on this gateway never both read the same count and both add. The
//!   lock is held by a task that outlives a cancelled request, so it is not released while the
//!   write is in flight.
//! - `CreateTopics` and `DeleteTopics` on this gateway take the same lock around their writes,
//!   so nothing on this gateway deletes or recreates the topic between the read and the write.
//! - After the write the topic is read again. The answer is `NONE` only when it is still the
//!   topic that was read (same Iggy id and creation time) and has exactly the requested count.
//! - A write Iggy did not refuse outright (a timeout, `TransientNotCommitted`, a broken
//!   connection, anything else whose outcome is unknown) keeps the lock while the count is
//!   re-read, until `SETTLE_LIMIT` after the write started, so the next request sees the write
//!   if it committed. One still uncommitted after that can commit later, and a retry that read
//!   the old count would then add again.
//!
//! Iggy splits a topic's `max_topic_size` evenly across its partitions, while Kafka's
//! `retention.bytes` caps each partition. So a grow of a topic that has a size cap first raises
//! the cap in proportion, under the same lock, and adds the partitions only once the raise
//! landed. Iggy's cleaner runs on its own clock, so partitions added first would take part of the
//! share of each old partition, and could cost it sealed segments before a later raise.
//!
//! - A raise that Iggy refuses adds nothing. A raise whose outcome is unknown adds nothing
//!   either, and the old cap is written back, so that a retry starts from it.
//! - When the partitions are proven not added, the old cap is written back. When their outcome
//!   is unknown, the raised cap stays: it keeps more of each partition, never less.
//! - A topic that ends with more partitions than requested, because something else grew it too,
//!   gets its cap raised again for every partition it has, from the cap read after the write.
//! - A write of the cap starts only when a whole bridge write fits before `SETTLE_LIMIT`, and is
//!   never cut short, so the lock is not released while it can still land.
//!
//! An alter that writes a size cap takes the same lock, so on one gateway it never runs between
//! the raise and the read that follows the add. Another gateway, or an Iggy client, that changes
//! the cap or the count at the same time can still leave a partition with a smaller share.
//! Closing that needs Iggy to scale the cap in the commit that adds partitions, or to keep a cap
//! for each partition.
//!
//! The lock is held at most `MAX_REQUEST_TIMEOUT` (30 s) plus `SETTLE_LIMIT` (20 s) after the
//! request that took it arrived: the first write never starts past that request's deadline, and
//! everything after it ends by `SETTLE_LIMIT`. A call that waited on the lock gets a retriable
//! `REQUEST_TIMED_OUT`, which the Java admin client does not retry by itself: the caller sees a
//! `TimeoutException` and must retry. 50 s stays under the Java client's 60 s
//! `default.api.timeout.ms`, so a caller that retries at once finds the lock free within the
//! budget it set for the call.
//!
//! The lock is per gateway process. Several gateways in front of one Iggy cluster, or Iggy
//! clients that call `create_partitions` directly, can still both add to the same count. The
//! check after the write then reports the mismatch instead of `NONE`, but the extra partitions
//! stay. Closing that needs Iggy to take an expected count (grow to N) on `create_partitions`.
//!
//! `CreatePartitionsTopicResult.error_message` is encoded at every version this module advertises
//! (v0-v3), unlike `DeleteTopics`, whose equivalent field only reaches the wire at v5+. Rejection
//! texts are the ones Kafka's `KRaft` controller sends, which the Java tools print verbatim.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use iggy::prelude::{
    IggyError, IggyTimestamp, MaxTopicSize, PartitionResizePolicy, TopicCreateOptions, TopicDetails,
};
use kafka_protocol::messages::create_partitions_request::{
    CreatePartitionsAssignment, CreatePartitionsTopic,
};
use kafka_protocol::messages::create_partitions_response::CreatePartitionsTopicResult;
use kafka_protocol::messages::{BrokerId, CreatePartitionsRequest, CreatePartitionsResponse};
use kafka_protocol::protocol::StrBytes;
use tokio::sync::{Mutex, OwnedMutexGuard};
use tokio::time::{Instant, timeout_at};

use crate::auth::AuthenticatedPrincipal;
use crate::bridge::iggy_bridge::{REQUEST_TIMEOUT, in_slot};
use crate::bridge::{
    BridgeError, IggyBridge, MAX_TOPIC_PARTITIONS, RetentionBytes, SettingChange, TopicConfigUpdate,
};
use crate::error::Result;
use crate::protocol::api::{
    API_KEY_CREATE_PARTITIONS, ApiVersionRange, ConnectionState, ERROR_INVALID_PARTITIONS,
    ERROR_INVALID_REPLICA_ASSIGNMENT, ERROR_INVALID_REQUEST, ERROR_NONE, ERROR_NOT_CONTROLLER,
    ERROR_POLICY_VIOLATION, ERROR_REQUEST_TIMED_OUT, ERROR_TOPIC_AUTHORIZATION_FAILED,
    ERROR_UNKNOWN_TOPIC_OR_PARTITION, GatewayState, HandleOutcome,
};
use crate::protocol::bounds_guard::validate_create_partitions_shape;
use crate::protocol::handlers::topic_config::{
    exceeds_topic_cap, find_duplicate_names, topic_cap_message,
};
use crate::protocol::handlers::{
    MAX_REQUEST_TIMEOUT, another_controller, clamp_request_timeout, decode_guarded, encode_message,
    handle_versioned_request, is_supported_version, respond_or_close, unsupported_version_response,
};

/// Every version Kafka itself serves. v0 and v1 are the legacy wire shape, v2 and v3 flexible.
pub const RANGE: ApiVersionRange = ApiVersionRange {
    api_key: API_KEY_CREATE_PARTITIONS,
    min_version: 0,
    max_version: 3,
};

/// How long after a write starts the lock is kept while its outcome is unknown, counting the
/// write itself. Bounds the lock hold together with [`MAX_REQUEST_TIMEOUT`]: 30 s + 20 s stays
/// 10 s under the Java admin client's 60 s `default.api.timeout.ms`.
const SETTLE_LIMIT: Duration = Duration::from_secs(20);

/// The Java admin client's `default.api.timeout.ms`, the most any of its calls waits in all.
const JAVA_DEFAULT_API_TIMEOUT: Duration = Duration::from_secs(60);

const _: () = assert!(
    MAX_REQUEST_TIMEOUT.as_secs() + SETTLE_LIMIT.as_secs() < JAVA_DEFAULT_API_TIMEOUT.as_secs(),
    "the longest lock hold must leave a caller that retries at once inside its call budget"
);

/// The longest one bridge write takes: `with_request_timeout` ends each one.
const WRITE_LIMIT: Duration = REQUEST_TIMEOUT;

const _: () = assert!(
    WRITE_LIMIT.as_secs() < SETTLE_LIMIT.as_secs(),
    "the raise of the size cap, the first write, must end before SETTLE_LIMIT"
);

/// Pause between two re-reads of a write whose outcome is unknown.
const SETTLE_POLL: Duration = Duration::from_millis(250);

/// Kafka's own text for a name repeated in one request (`ControllerApis.createPartitions`).
const DUPLICATE_TOPIC_NAME: &str = "Duplicate topic name.";

const TOPIC_DOES_NOT_EXIST: &str = "this topic does not exist";

const MAY_HAVE_BEEN_ADDED: &str = "timed out waiting for Iggy; the partitions may have been added";

const NOTHING_ADDED: &str = "timed out waiting for Iggy; no partitions were added";

const FIXED_PARTITION_COUNT: &str = "this topic was created with a fixed partition count";

const CAP_IN_DOUBT: &str = "timed out waiting for Iggy to raise the size cap of the topic for \
                            the new partitions; no partitions were added";

/// A rejection or verdict for one topic: the Kafka code and its text.
type Verdict = (i16, Option<StrBytes>);

pub async fn handle(
    state: &GatewayState,
    connection: &ConnectionState,
    principal: Option<&AuthenticatedPrincipal>,
    api_version: i16,
    body: Bytes,
) -> HandleOutcome {
    let Some(bridge) = &state.bridge else {
        return handle_versioned_request(
            API_KEY_CREATE_PARTITIONS,
            api_version,
            body,
            |v, b| {
                decode_guarded::<CreatePartitionsRequest>(v, b, |v, b| {
                    validate_create_partitions_shape(v, b, state.max_frame_size)
                })
            },
            encode_response,
            encode_error_response,
            "CreatePartitions",
        );
    };

    if !is_supported_version(API_KEY_CREATE_PARTITIONS, api_version) {
        return unsupported_version_response(API_KEY_CREATE_PARTITIONS, api_version, |version| {
            encode_error_response(version, ERROR_INVALID_REQUEST)
        });
    }

    let req = match decode_guarded::<CreatePartitionsRequest>(api_version, body, |v, b| {
        validate_create_partitions_shape(v, b, state.max_frame_size)
    }) {
        Ok(req) => req,
        Err(error) => {
            // debug!, not warn!: attacker-controlled, not operator-actionable.
            tracing::debug!(%error, "Failed to decode CreatePartitions request");
            return respond_or_close(
                encode_error_response(api_version, ERROR_INVALID_REQUEST),
                "CreatePartitions",
            );
        }
    };
    if another_controller(state, &connection.brokers) {
        return respond_or_close(
            encode_inner(api_version, &req.topics, ERROR_NOT_CONTROLLER),
            "CreatePartitions",
        );
    }

    // Kafka's `ControllerApis.createPartitions` order: duplicates are answered first, once per
    // name, before authorization looks at the rest.
    let topic_names = req.topics.iter().map(|topic| topic.name.as_str());
    let duplicate_names = find_duplicate_names(topic_names.clone());
    let mut results = duplicate_results(&req.topics, &duplicate_names);
    let topics: Vec<&CreatePartitionsTopic> = req
        .topics
        .iter()
        .filter(|topic| !duplicate_names.contains(topic.name.as_str()))
        .collect();

    if let Some(denied) = authorize(principal, &topics) {
        results.extend(denied);
        return respond(api_version, results);
    }

    if exceeds_topic_cap(topic_names) {
        tracing::warn!(
            requested_topics = req.topics.len(),
            "CreatePartitions request addresses too many distinct topics; rejecting"
        );
        // A server-imposed limit, not a malformed request. INVALID_REQUEST would blame the
        // client for a request Kafka itself would accept.
        let message = StrBytes::from(topic_cap_message("CreatePartitions"));
        results.extend(
            topics
                .iter()
                .map(|topic| topic_result(topic, ERROR_POLICY_VIOLATION, Some(message.clone()))),
        );
        return respond(api_version, results);
    }

    let deadline = Instant::now() + clamp_request_timeout(req.timeout_ms);
    results.extend(
        grow_all_topics(
            bridge,
            &state.partition_growth,
            &topics,
            req.validate_only,
            deadline,
            state.node_id,
        )
        .await,
    );
    respond(api_version, results)
}

fn respond(api_version: i16, results: Vec<CreatePartitionsTopicResult>) -> HandleOutcome {
    let resp = CreatePartitionsResponse::default().with_results(results);
    respond_or_close(encode_message(&resp, api_version, 256), "CreatePartitions")
}

fn topic_result(
    topic: &CreatePartitionsTopic,
    error_code: i16,
    error_message: Option<StrBytes>,
) -> CreatePartitionsTopicResult {
    CreatePartitionsTopicResult::default()
        .with_name(topic.name.clone())
        .with_error_code(error_code)
        .with_error_message(error_message)
}

/// One `INVALID_REQUEST` result per name repeated in the request, in the order each first
/// appears. Nothing is grown for such a name.
fn duplicate_results(
    topics: &[CreatePartitionsTopic],
    duplicate_names: &HashSet<&str>,
) -> Vec<CreatePartitionsTopicResult> {
    let mut reported = HashSet::with_capacity(duplicate_names.len());
    topics
        .iter()
        .filter(|topic| {
            let name = topic.name.as_str();
            duplicate_names.contains(name) && reported.insert(name)
        })
        .map(|topic| {
            topic_result(
                topic,
                ERROR_INVALID_REQUEST,
                Some(StrBytes::from_static_str(DUPLICATE_TOPIC_NAME)),
            )
        })
        .collect()
}

/// Denies every requested topic when `principal` is known but is not allowed to change topics,
/// or when its permissions could not be read. `None` means SASL is off, which this gateway
/// treats as no principal to enforce against - the same choice `DeleteTopics` and
/// `AlterConfigs` make when authentication itself is not configured.
fn authorize(
    principal: Option<&AuthenticatedPrincipal>,
    topics: &[&CreatePartitionsTopic],
) -> Option<Vec<CreatePartitionsTopicResult>> {
    let principal = principal?;
    if principal.permissions_known && principal.permissions.manage_topics {
        return None;
    }
    if !principal.permissions_known {
        // The permission read failed after a successful login, so this connection holds no real
        // answer. Denying is the fail-closed choice: granting would authorize a write off a
        // value nothing ever actually read.
        tracing::warn!(
            principal = %principal.username,
            "CreatePartitions asked on a connection whose permissions were never read; denying"
        );
    }
    let message = StrBytes::from_static_str(
        "the authenticated principal is not authorized to add partitions to topics",
    );
    Some(
        topics
            .iter()
            .map(|topic| {
                topic_result(
                    topic,
                    ERROR_TOPIC_AUTHORIZATION_FAILED,
                    Some(message.clone()),
                )
            })
            .collect(),
    )
}

/// The two Iggy calls a grow makes, kept behind a trait so the timing and error paths of
/// [`grow_locked`] run in unit tests against scripted replies.
pub(crate) trait GrowthBridge: Send + Sync + 'static {
    /// The topic `kafka_topic` resolves to, or `None` when it does not exist.
    fn read_topic(
        &self,
        kafka_topic: &str,
    ) -> impl Future<Output = core::result::Result<Option<TopicState>, BridgeError>> + Send;

    /// Adds `added` partitions after the topic's current last one.
    fn add_partitions(
        &self,
        kafka_topic: &str,
        added: u32,
    ) -> impl Future<Output = core::result::Result<(), BridgeError>> + Send;

    /// Sets the topic's `max_topic_size`. Ends within [`WRITE_LIMIT`].
    fn resize_topic(
        &self,
        kafka_topic: &str,
        max_topic_size: MaxTopicSize,
    ) -> impl Future<Output = core::result::Result<(), BridgeError>> + Send;
}

impl GrowthBridge for IggyBridge {
    async fn read_topic(
        &self,
        kafka_topic: &str,
    ) -> core::result::Result<Option<TopicState>, BridgeError> {
        let details = self.get_kafka_topic(kafka_topic).await?;
        Ok(details.as_ref().map(TopicState::from))
    }

    async fn add_partitions(
        &self,
        kafka_topic: &str,
        added: u32,
    ) -> core::result::Result<(), BridgeError> {
        self.create_kafka_partitions(kafka_topic, added).await
    }

    async fn resize_topic(
        &self,
        kafka_topic: &str,
        max_topic_size: MaxTopicSize,
    ) -> core::result::Result<(), BridgeError> {
        let update = TopicConfigUpdate {
            max_topic_size: Some(SettingChange::Set(max_topic_size)),
            ..TopicConfigUpdate::default()
        };
        self.update_kafka_topic_config(kafka_topic, &update).await
    }
}

/// Grows every requested topic, independently of the others.
///
/// `deadline` bounds each topic's own bridge work individually, not the whole loop, and is
/// checked before each topic's work starts. Same reasoning as `delete_topics::delete_all_topics`:
/// `timeout_at` polls its inner future before it checks the delay, so a deadline that already
/// passed would otherwise still start a real write, answering `REQUEST_TIMED_OUT` for a topic
/// that did grow.
async fn grow_all_topics<B: GrowthBridge>(
    bridge: &Arc<B>,
    partition_growth: &Arc<Mutex<()>>,
    topics: &[&CreatePartitionsTopic],
    validate_only: bool,
    deadline: Instant,
    node_id: i32,
) -> Vec<CreatePartitionsTopicResult> {
    let mut results = Vec::with_capacity(topics.len());
    let mut deadline_exceeded = false;
    for topic in topics {
        if deadline_exceeded || Instant::now() >= deadline {
            if !deadline_exceeded {
                deadline_exceeded = true;
                tracing::warn!(
                    "CreatePartitions deadline passed; answering remaining topics retriable \
                     instead of starting new Iggy calls"
                );
            }
            results.push(topic_result(topic, ERROR_REQUEST_TIMED_OUT, None));
            continue;
        }
        let result = if validate_only {
            validate_one_topic(bridge.as_ref(), topic, deadline, node_id).await
        } else {
            grow_one_topic(bridge, partition_growth, topic, deadline, node_id).await
        };
        results.push(result);
    }
    results
}

/// Runs every check a real grow runs and writes nothing.
///
/// No lock: there is no write to keep consistent with, and taking it would make a validation
/// wait behind a real grow for nothing.
async fn validate_one_topic<B: GrowthBridge>(
    bridge: &B,
    topic: &CreatePartitionsTopic,
    deadline: Instant,
    node_id: i32,
) -> CreatePartitionsTopicResult {
    match timeout_at(deadline, plan_growth(bridge, topic, node_id)).await {
        Ok(Ok(_plan)) => topic_result(topic, ERROR_NONE, None),
        Ok(Err((error_code, error_message))) => {
            topic_result(topic, error_code, Some(error_message))
        }
        Err(_elapsed) => {
            tracing::warn!(
                kafka_topic = topic.name.as_str(),
                "CreatePartitions validation exceeded the request deadline"
            );
            topic_result(topic, ERROR_REQUEST_TIMED_OUT, None)
        }
    }
}

/// Takes [`GatewayState::partition_growth`], then reads, checks and grows one topic in a task
/// that owns the lock.
///
/// The task keeps the lock until the write's outcome is settled, even when this request gives
/// up first at `deadline`, so a cancelled request never frees the lock under a write in flight.
async fn grow_one_topic<B: GrowthBridge>(
    bridge: &Arc<B>,
    partition_growth: &Arc<Mutex<()>>,
    topic: &CreatePartitionsTopic,
    deadline: Instant,
    node_id: i32,
) -> CreatePartitionsTopicResult {
    let kafka_topic = topic.name.as_str();
    let grow = grow_locked(Arc::clone(bridge), topic.clone(), deadline, node_id);
    let Some(guard) = lock_by(partition_growth, deadline).await else {
        tracing::warn!(
            kafka_topic,
            "CreatePartitions waited past the request deadline for another topic change on this \
             gateway; nothing was added"
        );
        return topic_result(
            topic,
            ERROR_REQUEST_TIMED_OUT,
            Some(StrBytes::from_static_str(NOTHING_ADDED)),
        );
    };
    let Some(((error_code, error_message), _)) = in_slot(guard, deadline, grow).await else {
        tracing::warn!(
            kafka_topic,
            "CreatePartitions exceeded the request deadline while adding partitions; the write \
             may still commit"
        );
        return topic_result(
            topic,
            ERROR_REQUEST_TIMED_OUT,
            Some(StrBytes::from_static_str(MAY_HAVE_BEEN_ADDED)),
        );
    };
    topic_result(topic, error_code, error_message)
}

/// The lock, or `None` if it is not free before `deadline`. [`grow_locked`] checks `deadline`
/// again before it writes, so a write never starts late.
async fn lock_by(lock: &Arc<Mutex<()>>, deadline: Instant) -> Option<OwnedMutexGuard<()>> {
    match timeout_at(deadline, Arc::clone(lock).lock_owned()).await {
        Ok(guard) if Instant::now() < deadline => Some(guard),
        _ => None,
    }
}

/// The read, the checks, the raise of the size cap, the write and the check after it, all under
/// the lock.
///
/// Nothing is written once `deadline` has passed, since the caller has answered by then. Every
/// Iggy call after the first write ends by `SETTLE_LIMIT` after it started, which with the
/// deadline bounds how long the lock is held.
async fn grow_locked<B: GrowthBridge>(
    bridge: Arc<B>,
    topic: CreatePartitionsTopic,
    deadline: Instant,
    node_id: i32,
) -> Verdict {
    let kafka_topic = topic.name.as_str();
    let bridge = bridge.as_ref();
    let plan = match timeout_at(deadline, plan_growth(bridge, &topic, node_id)).await {
        Ok(Ok(plan)) => plan,
        Ok(Err((error_code, error_message))) => return (error_code, Some(error_message)),
        Err(_elapsed) => return nothing_added(kafka_topic),
    };
    // The read can finish exactly at the deadline, which `timeout_at` still lets through.
    if Instant::now() >= deadline {
        return nothing_added(kafka_topic);
    }

    let settle_by = Instant::now() + SETTLE_LIMIT;
    if let Some(raised) = plan.raised_size_cap() {
        if let Err(verdict) = raise_size_cap(bridge, kafka_topic, &plan, raised, settle_by).await {
            return verdict;
        }
        // The raise can end past the deadline, and the caller has answered by then.
        if Instant::now() >= deadline {
            restore_size_cap(bridge, kafka_topic, &plan, settle_by).await;
            return nothing_added(kafka_topic);
        }
    }
    let written = timeout_at(settle_by, bridge.add_partitions(kafka_topic, plan.added))
        .await
        .unwrap_or(Err(BridgeError::Timeout));
    match written {
        Ok(()) => match read_by(bridge, kafka_topic, settle_by).await {
            Ok(after) => settle(bridge, kafka_topic, &plan, plan.observe(after), settle_by).await,
            Err(error) => {
                // Iggy acknowledged the write, and the cap was raised before it. Only the identity
                // check could not run.
                tracing::warn!(
                    kafka_topic,
                    %error,
                    "CreatePartitions could not re-read the topic after adding partitions"
                );
                (ERROR_NONE, None)
            }
        },
        Err(BridgeError::Iggy(IggyError::RequestTooOld)) => {
            reread_aged_out(bridge, kafka_topic, &plan, settle_by).await
        }
        Err(error) if refused_before_apply(&error) => {
            restore_size_cap(bridge, kafka_topic, &plan, settle_by).await;
            let (error_code, error_message) = bridge_failure(kafka_topic, &error);
            (error_code, Some(error_message))
        }
        Err(error) => settle_unknown_outcome(bridge, kafka_topic, &plan, &error, settle_by).await,
    }
}

/// Raises the size cap of the topic to `raised` before the write adds partitions, or answers why
/// nothing is added.
///
/// A refusal that proves the cap unchanged answers with that refusal. Any other failure leaves
/// the outcome of the raise unknown. The old cap is then written back, so that a retry sizes the
/// cap from it and not from a raise that landed late.
async fn raise_size_cap<B: GrowthBridge>(
    bridge: &B,
    kafka_topic: &str,
    plan: &Plan,
    raised: MaxTopicSize,
    settle_by: Instant,
) -> core::result::Result<(), Verdict> {
    match write_size_cap(bridge, kafka_topic, raised, settle_by).await {
        Ok(()) => Ok(()),
        Err(error) if refused_before_apply(&error) => {
            tracing::warn!(
                kafka_topic,
                %error,
                "CreatePartitions could not raise the size cap of the topic; nothing was added"
            );
            let (error_code, error_message) = bridge_failure(kafka_topic, &error);
            Err((error_code, Some(error_message)))
        }
        Err(error) => {
            tracing::warn!(
                kafka_topic,
                %error,
                "CreatePartitions does not know whether the size cap of the topic was raised; \
                 nothing was added"
            );
            restore_size_cap(bridge, kafka_topic, plan, settle_by).await;
            Err((
                ERROR_REQUEST_TIMED_OUT,
                Some(StrBytes::from_static_str(CAP_IN_DOUBT)),
            ))
        }
    }
}

/// Writes back the size cap the topic had before the raise, once no partitions were added. When
/// that fails, or a whole write no longer fits before `settle_by`, the raised cap stays: it keeps
/// more of each partition, never less.
async fn restore_size_cap<B: GrowthBridge>(
    bridge: &B,
    kafka_topic: &str,
    plan: &Plan,
    settle_by: Instant,
) {
    if plan.raised_size_cap().is_none() {
        return;
    }
    let before = MaxTopicSize::from(plan.before.max_topic_size);
    if let Err(error) = write_size_cap(bridge, kafka_topic, before, settle_by).await {
        tracing::warn!(
            kafka_topic,
            %error,
            "CreatePartitions added no partitions and could not lower the size cap of the topic \
             back, so each partition keeps more than retention.bytes until it is set again"
        );
    }
}

/// One write of the size cap of the topic. It starts only when a whole bridge write fits before
/// `settle_by`, and it is not cut short by `timeout_at`: a write cut short could still land after
/// the lock is released, over a later change on this gateway.
async fn write_size_cap<B: GrowthBridge>(
    bridge: &B,
    kafka_topic: &str,
    max_topic_size: MaxTopicSize,
    settle_by: Instant,
) -> core::result::Result<(), BridgeError> {
    if Instant::now() + WRITE_LIMIT > settle_by {
        return Err(BridgeError::Timeout);
    }
    match bridge.resize_topic(kafka_topic, max_topic_size).await {
        Ok(()) | Err(BridgeError::Iggy(IggyError::RequestAlreadyApplied)) => Ok(()),
        Err(error) => Err(error),
    }
}

/// [`Plan::verdict`] for what the topic shows after the write. The size cap was raised for the
/// requested count before the write. A topic that has more partitions than that, because
/// something else grew it too, gets the cap raised again, from the cap it shows now, so that each
/// partition keeps its share.
async fn settle<B: GrowthBridge>(
    bridge: &B,
    kafka_topic: &str,
    plan: &Plan,
    observed: Observed,
    settle_by: Instant,
) -> Verdict {
    if let Observed::Count {
        count,
        max_topic_size,
    } = observed
        && let Some(raised) = plan.raised_for(count, max_topic_size)
        && let Err(error) = write_size_cap(bridge, kafka_topic, raised, settle_by).await
    {
        tracing::warn!(
            kafka_topic,
            count,
            %error,
            "CreatePartitions found more partitions than it added and could not raise the size \
             cap of the topic for them, so each partition keeps less than retention.bytes until \
             it is set again"
        );
    }
    plan.verdict(kafka_topic, observed)
}

/// [`GrowthBridge::read_topic`], given up as [`BridgeError::Timeout`] at `by`.
async fn read_by<B: GrowthBridge>(
    bridge: &B,
    kafka_topic: &str,
    by: Instant,
) -> core::result::Result<Option<TopicState>, BridgeError> {
    timeout_at(by, bridge.read_topic(kafka_topic))
        .await
        .unwrap_or(Err(BridgeError::Timeout))
}

fn nothing_added(kafka_topic: &str) -> Verdict {
    tracing::warn!(
        kafka_topic,
        "CreatePartitions reached the request deadline before adding partitions; nothing was \
         written"
    );
    (
        ERROR_REQUEST_TIMED_OUT,
        Some(StrBytes::from_static_str(NOTHING_ADDED)),
    )
}

/// Iggy refusals that prove the write changed nothing, so the lock can go at once. Each is
/// raised before the op is applied, or is the apply step itself refusing without a change
/// (`CreatePartitionsWithAssignmentsRequest::apply`), and an SDK replay of an op that did apply
/// gets its cached reply instead. Every other error leaves the outcome unknown.
const fn refused_before_apply(error: &BridgeError) -> bool {
    matches!(
        error,
        BridgeError::InvalidKafkaTopicName { .. }
            | BridgeError::InvalidPartitionCount { .. }
            | BridgeError::Iggy(
                IggyError::StreamIdNotFound(_)
                    | IggyError::StreamNameNotFound(_)
                    | IggyError::TopicIdNotFound(_, _)
                    | IggyError::TopicNameNotFound(_, _)
                    | IggyError::ResourceNotFound(_)
                    | IggyError::Unauthorized
                    | IggyError::TooManyPartitions
                    | IggyError::PartitionsLimitReached
                    | IggyError::PartitionResizeDisabled
                    | IggyError::PartitionIdSpaceExhausted
                    // The op never entered Iggy's pipeline.
                    | IggyError::TransientNotAccepted
            )
    )
}

/// Answers a write Iggy refused with `RequestTooOld`: the SDK resent a request Iggy had already
/// run, and its reply aged out. The op is behind Iggy's watermark, not in flight, so one re-read
/// tells what it did.
async fn reread_aged_out<B: GrowthBridge>(
    bridge: &B,
    kafka_topic: &str,
    plan: &Plan,
    settle_by: Instant,
) -> Verdict {
    match read_by(bridge, kafka_topic, settle_by).await {
        Ok(after) => match plan.observe(after) {
            Observed::Count { count, .. } if count == plan.before.partitions_count => {
                tracing::warn!(
                    kafka_topic,
                    count,
                    "CreatePartitions write already ran in Iggy and left the count unchanged"
                );
                restore_size_cap(bridge, kafka_topic, plan, settle_by).await;
                (
                    ERROR_REQUEST_TIMED_OUT,
                    Some(StrBytes::from_static_str(NOTHING_ADDED)),
                )
            }
            observed => settle(bridge, kafka_topic, plan, observed, settle_by).await,
        },
        Err(error) => {
            tracing::warn!(
                kafka_topic,
                %error,
                "CreatePartitions could not re-read a write Iggy had already run"
            );
            (
                ERROR_REQUEST_TIMED_OUT,
                Some(StrBytes::from_static_str(MAY_HAVE_BEEN_ADDED)),
            )
        }
    }
}

/// Re-reads the topic after a write whose outcome is unknown until the count moves or
/// `settle_by` passes, so the lock is not released while the write can still be seen to land.
/// The raised size cap stays when the count never moves, since the write can still land.
///
/// A count that moved to anything but the requested one ends the wait too. Another gateway or a
/// direct Iggy client moved it, and this write may still land on top afterwards: the lock is
/// released anyway, since nothing on this gateway can tell the two writes apart.
async fn settle_unknown_outcome<B: GrowthBridge>(
    bridge: &B,
    kafka_topic: &str,
    plan: &Plan,
    error: &BridgeError,
    settle_by: Instant,
) -> Verdict {
    tracing::warn!(
        kafka_topic,
        %error,
        "CreatePartitions write outcome unknown; re-reading the topic before releasing the lock"
    );
    loop {
        if let Ok(after) = read_by(bridge, kafka_topic, settle_by).await {
            match plan.observe(after) {
                Observed::Count { count, .. } if count == plan.before.partitions_count => {}
                observed => return settle(bridge, kafka_topic, plan, observed, settle_by).await,
            }
        }
        if Instant::now() + SETTLE_POLL >= settle_by {
            break;
        }
        tokio::time::sleep(SETTLE_POLL).await;
    }
    tracing::warn!(
        kafka_topic,
        "CreatePartitions write still not visible after re-reading; releasing the lock with the \
         outcome unknown"
    );
    (
        ERROR_REQUEST_TIMED_OUT,
        Some(StrBytes::from_static_str(MAY_HAVE_BEEN_ADDED)),
    )
}

/// What identifies one Iggy topic across a delete and a recreate under the same name, its
/// partition count, and whether it may grow.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TopicState {
    id: u32,
    created_at: IggyTimestamp,
    partitions_count: u32,
    fixed_partition_count: bool,
    /// The topic's `max_topic_size`, in its `u64` form.
    max_topic_size: u64,
}

impl From<&TopicDetails> for TopicState {
    fn from(details: &TopicDetails) -> Self {
        Self {
            id: details.id,
            created_at: details.created_at,
            partitions_count: details.partitions_count,
            fixed_partition_count: TopicCreateOptions::from_resource_options(&details.options)
                .partition_resize_policy
                == Some(PartitionResizePolicy::Fixed),
            max_topic_size: u64::from(details.resolved_options().max_topic_size),
        }
    }
}

/// The topic as read before the write, and how many partitions the write adds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Plan {
    before: TopicState,
    added: u32,
}

/// The topic as read after a write, against [`Plan::before`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Observed {
    /// The same topic, at exactly the requested count.
    Grown,
    /// Gone, or another topic under the same name.
    Replaced,
    /// The same topic at another count, with the size cap read with it, in its `u64` form.
    Count { count: u32, max_topic_size: u64 },
}

impl Plan {
    const fn target(&self) -> u32 {
        self.before.partitions_count + self.added
    }

    /// The size cap that gives each partition of the grown topic the share each one has now, or
    /// `None` when the topic has no cap.
    fn raised_size_cap(&self) -> Option<MaxTopicSize> {
        scaled_size_cap(
            self.before.max_topic_size,
            self.before.partitions_count,
            self.target(),
        )
    }

    /// The size cap that gives each of `count` partitions the share that `max_topic_size`, raised
    /// for the requested count, gives each of those, or `None` when that is no raise.
    fn raised_for(&self, count: u32, max_topic_size: u64) -> Option<MaxTopicSize> {
        scaled_size_cap(max_topic_size, self.target(), count)
    }

    fn observe(&self, after: Option<TopicState>) -> Observed {
        match after {
            Some(after)
                if after.id == self.before.id && after.created_at == self.before.created_at =>
            {
                if after.partitions_count == self.target() {
                    Observed::Grown
                } else {
                    Observed::Count {
                        count: after.partitions_count,
                        max_topic_size: after.max_topic_size,
                    }
                }
            }
            _ => Observed::Replaced,
        }
    }

    fn verdict(&self, kafka_topic: &str, observed: Observed) -> Verdict {
        match observed {
            Observed::Grown => (ERROR_NONE, None),
            // The topic the client meant is gone, and the partitions may sit on whatever topic
            // now has the name. Java's `createPartitions` does not retry this code per topic, so
            // the caller gets a retriable `UnknownTopicOrPartitionException` and decides what to
            // do against the topic there now.
            Observed::Replaced => {
                tracing::warn!(
                    kafka_topic,
                    "CreatePartitions: the topic was deleted or recreated while partitions were \
                     being added"
                );
                (
                    ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                    Some(StrBytes::from_static_str(
                        "this topic was deleted or recreated while partitions were being added to \
                         it; the partitions may have been added to the topic now under this name",
                    )),
                )
            }
            // Something outside this gateway changed the count alongside this write. The count
            // the client asked for no longer holds, which is what INVALID_PARTITIONS reports, and
            // a retry gets the same answer Kafka would give for that count.
            Observed::Count { count, .. } => {
                tracing::warn!(
                    kafka_topic,
                    count,
                    target = self.target(),
                    "CreatePartitions: the partition count changed alongside this write"
                );
                (
                    ERROR_INVALID_PARTITIONS,
                    Some(StrBytes::from(format!(
                        "The topic {kafka_topic} now has {count} partition(s), not the {} \
                         requested; its partition count changed while this request was adding \
                         to it.",
                        self.target()
                    ))),
                )
            }
        }
    }
}

/// The size cap that gives each of `count` partitions the share that `max_topic_size` gives each
/// of `sized_for` partitions, or `None` when the topic has no cap or that is no raise. Never below
/// `max_topic_size`, which Iggy already admitted.
fn scaled_size_cap(max_topic_size: u64, sized_for: u32, count: u32) -> Option<MaxTopicSize> {
    let RetentionBytes::PerPartition(share) =
        RetentionBytes::share_of(MaxTopicSize::from(max_topic_size), sized_for)
    else {
        return None;
    };
    let scaled = RetentionBytes::PerPartition(share).topic_size(count, max_topic_size);
    (u64::from(scaled) > max_topic_size).then_some(scaled)
}

/// Reads one topic and checks the request against it, returning what the write would add.
///
/// The checks run in the order real Kafka's `ReplicationControlManager.createPartitions` runs
/// them: a missing topic first, then a count that is not an increase, then the manual
/// assignment. A fixed partition count comes last, where the write would have met it in Iggy,
/// so `validate_only` answers what the write gets.
async fn plan_growth<B: GrowthBridge>(
    bridge: &B,
    topic: &CreatePartitionsTopic,
    node_id: i32,
) -> core::result::Result<Plan, (i16, StrBytes)> {
    let kafka_topic = topic.name.as_str();
    let before = match bridge.read_topic(kafka_topic).await {
        Ok(Some(before)) => before,
        Ok(None) => {
            tracing::debug!(kafka_topic, "CreatePartitions: topic does not exist");
            return Err((
                ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                StrBytes::from_static_str(TOPIC_DOES_NOT_EXIST),
            ));
        }
        Err(error) => return Err(bridge_failure(kafka_topic, &error)),
    };

    let added = partitions_to_add(kafka_topic, before.partitions_count, topic.count)
        .and_then(|added| {
            validate_assignments(topic.assignments.as_deref(), added, node_id).map(|()| added)
        })
        .and_then(|added| within_per_request_cap(added).map(|()| added))
        .and_then(|added| resizable(&before).map(|()| added))
        .inspect_err(|(error_code, _)| {
            tracing::debug!(
                kafka_topic,
                existing = before.partitions_count,
                requested = topic.count,
                error_code,
                "CreatePartitions rejected a request"
            );
        })?;
    Ok(Plan { before, added })
}

/// Iggy refuses to grow a topic created with a fixed partition count, with
/// `PartitionResizeDisabled`, which maps to `POLICY_VIOLATION` (44).
const fn resizable(topic: &TopicState) -> core::result::Result<(), (i16, StrBytes)> {
    if topic.fixed_partition_count {
        return Err((
            ERROR_POLICY_VIOLATION,
            StrBytes::from_static_str(FIXED_PARTITION_COUNT),
        ));
    }
    Ok(())
}

/// Iggy adds at most [`MAX_TOPIC_PARTITIONS`] partitions to one topic in one call and refuses
/// more with `TooManyPartitions`. Checked here as well, so `validate_only` answers the same
/// rejection the write would get, with the same `INVALID_REQUEST` (42) that error maps to: not
/// `INVALID_PARTITIONS` (37), whose wire text says "below 1", the opposite condition.
fn within_per_request_cap(added: u32) -> core::result::Result<(), (i16, StrBytes)> {
    if added > MAX_TOPIC_PARTITIONS {
        return Err((ERROR_INVALID_REQUEST, too_many_added_message()));
    }
    Ok(())
}

fn too_many_added_message() -> StrBytes {
    StrBytes::from(format!(
        "at most {MAX_TOPIC_PARTITIONS} partitions can be added to one topic in one request"
    ))
}

/// How many partitions reach `requested` from `existing`, or Kafka's refusal when that is not
/// an increase.
///
/// Both refusals are `INVALID_PARTITIONS` (37) with the `KRaft` controller's text. A `requested`
/// at or below zero is just the second case: Kafka compares it against the current count the
/// same way and never checks the sign separately.
fn partitions_to_add(
    kafka_topic: &str,
    existing: u32,
    requested: i32,
) -> core::result::Result<u32, (i16, StrBytes)> {
    let Some(added) = u32::try_from(requested)
        .ok()
        .and_then(|requested| requested.checked_sub(existing))
    else {
        return Err((
            ERROR_INVALID_PARTITIONS,
            StrBytes::from(format!(
                "The topic {kafka_topic} currently has {existing} partition(s); {requested} would \
                 not be an increase."
            )),
        ));
    };
    if added == 0 {
        return Err((
            ERROR_INVALID_PARTITIONS,
            StrBytes::from(format!("Topic already has {existing} partition(s).")),
        ));
    }
    Ok(added)
}

/// Checks a manual `assignments` list against the `added` partitions it must cover.
///
/// A null list means "let the broker place them", which is what every client sends unless the
/// caller named replicas. A present list must hold exactly one entry per new partition, as real
/// Kafka requires, and each entry must name this gateway (`node_id`) exactly once: that is the
/// only replica list Metadata advertises, the same ceiling
/// `create_topics::assignment_replicas_are_valid` holds `CreateTopics` to. Both failures are
/// `INVALID_REPLICA_ASSIGNMENT` (39), real Kafka's code for each.
///
/// Iggy's own `create_partitions` cannot take an assignment, so a valid list changes nothing
/// about the write. It is still checked rather than ignored, so a client that asked for a
/// placement this gateway cannot honor is told so instead of getting a `NONE` for a request
/// that was not carried out as sent.
fn validate_assignments(
    assignments: Option<&[CreatePartitionsAssignment]>,
    added: u32,
    node_id: i32,
) -> core::result::Result<(), (i16, StrBytes)> {
    let Some(assignments) = assignments else {
        return Ok(());
    };
    if assignments.len() != added as usize {
        return Err((
            ERROR_INVALID_REPLICA_ASSIGNMENT,
            StrBytes::from(format!(
                "Attempted to add {added} additional partition(s), but only {} assignment(s) \
                 were specified.",
                assignments.len()
            )),
        ));
    }
    if assignments
        .iter()
        .any(|assignment| assignment.broker_ids != [BrokerId(node_id)])
    {
        return Err((
            ERROR_INVALID_REPLICA_ASSIGNMENT,
            StrBytes::from(format!(
                "each new partition must be assigned to broker {node_id} exactly once, the one \
                 replica this gateway advertises"
            )),
        ));
    }
    Ok(())
}

/// Maps a bridge failure to a Kafka code and text, logging the real cause server-side.
///
/// Reached for the reads and for a write [`refused_before_apply`] accepts as changing nothing.
/// Every other write error is settled by [`settle_unknown_outcome`] instead, so a timeout here
/// comes from a read and never left a write behind.
///
/// Same caution as `create_topics::bridge_error_result` on forwarding `err.to_string()`: a
/// server-side rejection can reconstruct with default-valued fields, so only the client-caused
/// variant (an invalid name) forwards its own text. The refusals Iggy makes for a valid request
/// each get fixed text that names the cause an operator can act on. Everything else gets a fixed
/// message and logs at `error!`.
fn bridge_failure(kafka_topic: &str, err: &BridgeError) -> (i16, StrBytes) {
    let error_code = err.to_kafka_error_code();
    let message = match err {
        BridgeError::InvalidKafkaTopicName { reason, .. } => {
            tracing::debug!(
                kafka_topic,
                reason,
                "CreatePartitions rejected an invalid topic name"
            );
            StrBytes::from(reason.clone())
        }
        BridgeError::Iggy(
            IggyError::StreamIdNotFound(_)
            | IggyError::StreamNameNotFound(_)
            | IggyError::TopicIdNotFound(_, _)
            | IggyError::TopicNameNotFound(_, _)
            | IggyError::ResourceNotFound(_),
        ) => {
            tracing::debug!(kafka_topic, error = %err, "CreatePartitions: topic does not exist");
            StrBytes::from_static_str(TOPIC_DOES_NOT_EXIST)
        }
        BridgeError::Iggy(IggyError::Unauthorized) => {
            tracing::debug!(
                kafka_topic,
                error = %err,
                "CreatePartitions: not authorized to add partitions to this topic"
            );
            StrBytes::from_static_str("not authorized to add partitions to this topic")
        }
        BridgeError::Timeout | BridgeError::Iggy(IggyError::TransientNotCommitted) => {
            tracing::warn!(
                kafka_topic,
                error = %err,
                "CreatePartitions timed out waiting for Iggy before adding partitions"
            );
            StrBytes::from_static_str(NOTHING_ADDED)
        }
        // Unreachable through `grow_locked`, which checks `within_per_request_cap` first, but
        // the bridge method is public and the fixed text is right either way.
        BridgeError::Iggy(IggyError::TooManyPartitions) => {
            tracing::debug!(
                kafka_topic,
                "CreatePartitions refused: more partitions than Iggy adds in one call"
            );
            too_many_added_message()
        }
        BridgeError::Iggy(IggyError::PartitionsLimitReached) => {
            tracing::warn!(
                kafka_topic,
                "CreatePartitions refused: the Iggy node is at metadata.partitions_max"
            );
            StrBytes::from_static_str("the Iggy node has reached its partition limit")
        }
        BridgeError::Iggy(IggyError::PartitionResizeDisabled) => {
            tracing::debug!(
                kafka_topic,
                "CreatePartitions refused: the topic was created with a fixed partition count"
            );
            StrBytes::from_static_str(FIXED_PARTITION_COUNT)
        }
        BridgeError::Iggy(IggyError::PartitionIdSpaceExhausted) => {
            tracing::warn!(
                kafka_topic,
                "CreatePartitions refused: Iggy has no partition ids left for this topic"
            );
            StrBytes::from_static_str("Iggy has no partition ids left for this topic")
        }
        // Iggy refuses writes while a view change or a lifecycle step runs. Routine, and the
        // retriable code already tells the client to try again.
        BridgeError::Iggy(IggyError::TransientNotAccepted) => {
            tracing::debug!(
                kafka_topic,
                "CreatePartitions refused: Iggy is not accepting writes right now"
            );
            StrBytes::from_static_str("Iggy is not accepting writes right now; retry")
        }
        other => {
            tracing::error!(
                kafka_topic,
                %other,
                "CreatePartitions failed against the Iggy bridge"
            );
            StrBytes::from_static_str("internal error adding partitions to this topic")
        }
    };
    (error_code, message)
}

/// Well-formed `CreatePartitions` response with no results.
///
/// There is no top-level error field, so a response that must carry an error without a decoded
/// topic list to echo has nowhere to put it, the same shape `DeleteTopics` answers with.
///
/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_error_response(version: i16, error_code: i16) -> Result<Bytes> {
    encode_inner(version, &[], error_code)
}

/// # Errors
///
/// Returns an error when `kafka_protocol` cannot encode the response at `version`.
pub fn encode_response(version: i16, req: &CreatePartitionsRequest) -> Result<Bytes> {
    encode_inner(version, &req.topics, ERROR_NOT_CONTROLLER)
}

fn encode_inner(
    version: i16,
    topics: &[CreatePartitionsTopic],
    forced_error: i16,
) -> Result<Bytes> {
    let results = topics
        .iter()
        .map(|topic| topic_result(topic, forced_error, None))
        .collect();
    let resp = CreatePartitionsResponse::default().with_results(results);
    encode_message(&resp, version, 256)
}

#[cfg(test)]
mod tests {
    use iggy::prelude::{Identifier, IggyByteSize};
    use kafka_protocol::messages::TopicName;

    use super::*;

    /// The node id of the gateway under test.
    const NODE_ID: i32 = 1;
    use crate::protocol::acl::PrincipalPermissions;
    use crate::protocol::api::{ERROR_NOT_LEADER_OR_FOLLOWER, ERROR_UNKNOWN_SERVER_ERROR};

    fn topic(name: &str, count: i32) -> CreatePartitionsTopic {
        CreatePartitionsTopic::default()
            .with_name(TopicName(StrBytes::from_string(name.to_string())))
            .with_count(count)
            .with_assignments(None)
    }

    fn assignment(broker_ids: &[i32]) -> CreatePartitionsAssignment {
        CreatePartitionsAssignment::default()
            .with_broker_ids(broker_ids.iter().copied().map(BrokerId).collect())
    }

    fn principal(manage_topics: bool, permissions_known: bool) -> AuthenticatedPrincipal {
        AuthenticatedPrincipal {
            username: "alice".to_string(),
            user: None,
            permissions: PrincipalPermissions {
                manage_topics,
                ..PrincipalPermissions::default()
            },
            permissions_known,
        }
    }

    fn topic_state(id: u32, created_at: u64, partitions_count: u32) -> TopicState {
        TopicState {
            id,
            created_at: IggyTimestamp::from(created_at),
            partitions_count,
            fixed_partition_count: false,
            max_topic_size: u64::from(MaxTopicSize::Unlimited),
        }
    }

    /// Topic 7, created at 1000, read at 3 partitions and grown by 2 to 5.
    fn plan() -> Plan {
        Plan {
            before: topic_state(7, 1_000, 3),
            added: 2,
        }
    }

    #[test]
    fn a_larger_count_adds_the_difference() {
        assert_eq!(partitions_to_add("orders", 1, 3), Ok(2));
        assert_eq!(partitions_to_add("orders", 0, 1), Ok(1));
    }

    #[test]
    fn an_equal_count_is_not_an_increase() {
        let (code, message) = partitions_to_add("orders", 3, 3).unwrap_err();
        assert_eq!(code, ERROR_INVALID_PARTITIONS);
        assert_eq!(message.as_str(), "Topic already has 3 partition(s).");
    }

    #[test]
    fn a_smaller_count_is_not_an_increase() {
        let (code, message) = partitions_to_add("orders", 3, 2).unwrap_err();
        assert_eq!(code, ERROR_INVALID_PARTITIONS);
        assert_eq!(
            message.as_str(),
            "The topic orders currently has 3 partition(s); 2 would not be an increase."
        );
    }

    #[test]
    fn a_count_at_or_below_zero_is_not_an_increase() {
        for requested in [0, -1, i32::MIN] {
            let (code, _) = partitions_to_add("orders", 3, requested).unwrap_err();
            assert_eq!(code, ERROR_INVALID_PARTITIONS, "requested {requested}");
        }
    }

    #[test]
    fn adding_up_to_the_per_request_cap_passes_and_one_more_is_invalid_request() {
        assert_eq!(within_per_request_cap(MAX_TOPIC_PARTITIONS), Ok(()));
        let (code, message) = within_per_request_cap(MAX_TOPIC_PARTITIONS + 1).unwrap_err();
        assert_eq!(code, ERROR_INVALID_REQUEST);
        assert_eq!(
            message.as_str(),
            "at most 1000 partitions can be added to one topic in one request"
        );
    }

    #[test]
    fn a_null_assignment_list_passes() {
        assert_eq!(validate_assignments(None, 2, NODE_ID), Ok(()));
    }

    #[test]
    fn an_assignment_per_new_partition_naming_the_single_broker_passes() {
        let assignments = [assignment(&[NODE_ID]), assignment(&[NODE_ID])];
        assert_eq!(validate_assignments(Some(&assignments), 2, NODE_ID), Ok(()));
    }

    #[test]
    fn an_assignment_list_of_the_wrong_length_is_rejected() {
        let assignments = [assignment(&[NODE_ID])];
        let (code, message) = validate_assignments(Some(&assignments), 2, NODE_ID).unwrap_err();
        assert_eq!(code, ERROR_INVALID_REPLICA_ASSIGNMENT);
        assert_eq!(
            message.as_str(),
            "Attempted to add 2 additional partition(s), but only 1 assignment(s) were specified."
        );
    }

    #[test]
    fn an_empty_but_present_assignment_list_is_rejected() {
        // Kafka checks a present list's length even when it is empty: `increaseTo(n)` sends a
        // null list, so an empty one is a client that meant to name replicas and named none.
        let (code, _) = validate_assignments(Some(&[]), 1, NODE_ID).unwrap_err();
        assert_eq!(code, ERROR_INVALID_REPLICA_ASSIGNMENT);
    }

    #[test]
    fn an_assignment_naming_another_broker_or_an_empty_list_is_rejected() {
        for broker_ids in [&[][..], &[7][..], &[NODE_ID, NODE_ID][..]] {
            let assignments = [assignment(broker_ids)];
            let (code, _) = validate_assignments(Some(&assignments), 1, NODE_ID).unwrap_err();
            assert_eq!(code, ERROR_INVALID_REPLICA_ASSIGNMENT, "{broker_ids:?}");
        }
    }

    #[test]
    fn a_duplicated_name_gets_one_result_with_kafkas_text() {
        let topics = [
            topic("orders", 2),
            topic("payments", 2),
            topic("orders", 3),
            topic("orders", 4),
        ];
        let duplicate_names = find_duplicate_names(topics.iter().map(|t| t.name.as_str()));
        let results = duplicate_results(&topics, &duplicate_names);
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].name.as_str(), "orders");
        assert_eq!(results[0].error_code, ERROR_INVALID_REQUEST);
        assert_eq!(
            results[0].error_message.as_deref(),
            Some("Duplicate topic name.")
        );
    }

    #[test]
    fn no_principal_is_not_gated() {
        assert!(authorize(None, &[&topic("orders", 2)]).is_none());
    }

    #[test]
    fn a_principal_with_manage_topics_is_allowed() {
        let principal = principal(true, true);
        assert!(authorize(Some(&principal), &[&topic("orders", 2)]).is_none());
    }

    #[test]
    fn a_principal_without_manage_topics_is_denied_every_topic() {
        let principal = principal(false, true);
        let results = authorize(
            Some(&principal),
            &[&topic("orders", 2), &topic("payments", 3)],
        )
        .expect("denied");
        assert_eq!(results.len(), 2);
        for result in results {
            assert_eq!(result.error_code, ERROR_TOPIC_AUTHORIZATION_FAILED);
            assert!(result.error_message.is_some());
        }
    }

    #[test]
    fn a_principal_whose_permissions_were_never_read_is_denied() {
        let principal = principal(true, false);
        let results = authorize(Some(&principal), &[&topic("orders", 2)]).expect("denied");
        assert_eq!(results[0].error_code, ERROR_TOPIC_AUTHORIZATION_FAILED);
    }

    #[test]
    fn the_same_topic_at_the_requested_count_is_grown() {
        assert_eq!(
            plan().observe(Some(topic_state(7, 1_000, 5))),
            Observed::Grown
        );
        assert_eq!(
            plan().verdict("orders", Observed::Grown),
            (ERROR_NONE, None)
        );
    }

    #[test]
    fn a_missing_or_recreated_topic_is_replaced_and_answers_unknown_topic() {
        let plan = plan();
        for after in [
            None,
            Some(topic_state(8, 1_000, 5)),
            Some(topic_state(7, 2_000, 5)),
        ] {
            assert_eq!(plan.observe(after), Observed::Replaced, "{after:?}");
        }
        let (code, message) = plan.verdict("orders", Observed::Replaced);
        assert_eq!(code, ERROR_UNKNOWN_TOPIC_OR_PARTITION);
        assert!(message.is_some());
    }

    /// What [`Plan::observe`] makes of an uncapped topic at `count`.
    fn uncapped_count(count: u32) -> Observed {
        Observed::Count {
            count,
            max_topic_size: u64::from(MaxTopicSize::Unlimited),
        }
    }

    #[test]
    fn the_same_topic_at_another_count_is_not_answered_none() {
        let plan = plan();
        assert_eq!(
            plan.observe(Some(topic_state(7, 1_000, 7))),
            uncapped_count(7)
        );
        assert_eq!(
            plan.observe(Some(topic_state(7, 1_000, 3))),
            uncapped_count(3)
        );
        let (code, message) = plan.verdict("orders", uncapped_count(7));
        assert_eq!(code, ERROR_INVALID_PARTITIONS);
        assert_eq!(
            message.as_deref(),
            Some(
                "The topic orders now has 7 partition(s), not the 5 requested; its partition \
                 count changed while this request was adding to it."
            )
        );
    }

    #[test]
    fn bridge_failure_names_each_refusal_iggy_makes() {
        let missing_topic = BridgeError::Iggy(IggyError::TopicIdNotFound(
            Identifier::default(),
            Identifier::default(),
        ));
        let cases = [
            (
                &missing_topic,
                ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                "this topic does not exist",
            ),
            (
                &BridgeError::Iggy(IggyError::Unauthorized),
                ERROR_TOPIC_AUTHORIZATION_FAILED,
                "not authorized to add partitions to this topic",
            ),
            (
                &BridgeError::Iggy(IggyError::TooManyPartitions),
                ERROR_INVALID_REQUEST,
                "at most 1000 partitions can be added to one topic in one request",
            ),
            (
                &BridgeError::Iggy(IggyError::PartitionsLimitReached),
                ERROR_POLICY_VIOLATION,
                "the Iggy node has reached its partition limit",
            ),
            (
                &BridgeError::Iggy(IggyError::PartitionResizeDisabled),
                ERROR_POLICY_VIOLATION,
                "this topic was created with a fixed partition count",
            ),
            (
                &BridgeError::Iggy(IggyError::PartitionIdSpaceExhausted),
                ERROR_POLICY_VIOLATION,
                "Iggy has no partition ids left for this topic",
            ),
            (
                &BridgeError::Iggy(IggyError::TransientNotAccepted),
                ERROR_NOT_LEADER_OR_FOLLOWER,
                "Iggy is not accepting writes right now; retry",
            ),
            (
                &BridgeError::Timeout,
                ERROR_REQUEST_TIMED_OUT,
                "timed out waiting for Iggy; no partitions were added",
            ),
            (
                &BridgeError::Iggy(IggyError::TransientNotCommitted),
                ERROR_REQUEST_TIMED_OUT,
                "timed out waiting for Iggy; no partitions were added",
            ),
            (
                &BridgeError::Iggy(IggyError::InvalidCommand),
                ERROR_UNKNOWN_SERVER_ERROR,
                "internal error adding partitions to this topic",
            ),
        ];
        for (err, expected_code, expected_message) in cases {
            let (code, message) = bridge_failure("orders", err);
            assert_eq!(code, expected_code, "{err}");
            assert_eq!(message.as_str(), expected_message, "{err}");
        }
    }

    #[test]
    fn the_stub_response_answers_not_controller_per_topic_at_every_version() {
        let req = CreatePartitionsRequest::default()
            .with_topics(vec![topic("orders", 2), topic("payments", 3)]);
        for version in RANGE.min_version..=RANGE.max_version {
            let body = encode_response(version, &req).expect("encodes");
            assert!(!body.is_empty(), "v{version}");
        }
    }

    /// One scripted Iggy reply: at once, after a delay, or never.
    enum Reply<T> {
        Now(core::result::Result<T, BridgeError>),
        After(Duration, core::result::Result<T, BridgeError>),
        Never,
    }

    impl<T> Reply<T> {
        async fn resolve(self) -> core::result::Result<T, BridgeError> {
            match self {
                Self::Now(result) => result,
                Self::After(delay, result) => {
                    tokio::time::sleep(delay).await;
                    result
                }
                Self::Never => std::future::pending().await,
            }
        }
    }

    type Script<T> = Box<dyn Fn(usize) -> Reply<T> + Send + Sync>;

    /// Builds a fresh reply each call, since a [`BridgeError`] cannot be cloned.
    type Reread = fn() -> Reply<Option<TopicState>>;

    /// One call a [`ScriptedIggy`] took, in the order it took them.
    #[derive(Debug, Clone, Copy, PartialEq)]
    enum Call {
        Read,
        Add,
        Resize(MaxTopicSize),
    }

    /// A [`GrowthBridge`] whose every reply comes from a script indexed by call number. Read 0 is
    /// the read before the write.
    struct ScriptedIggy {
        reads: Script<Option<TopicState>>,
        writes: Script<()>,
        resizes: Script<()>,
        calls: std::sync::Mutex<Vec<Call>>,
    }

    impl ScriptedIggy {
        fn new(
            reads: impl Fn(usize) -> Reply<Option<TopicState>> + Send + Sync + 'static,
            writes: impl Fn(usize) -> Reply<()> + Send + Sync + 'static,
        ) -> Arc<Self> {
            Self::resizing(reads, writes, |_| Reply::Now(Ok(())))
        }

        fn resizing(
            reads: impl Fn(usize) -> Reply<Option<TopicState>> + Send + Sync + 'static,
            writes: impl Fn(usize) -> Reply<()> + Send + Sync + 'static,
            resizes: impl Fn(usize) -> Reply<()> + Send + Sync + 'static,
        ) -> Arc<Self> {
            Arc::new(Self {
                reads: Box::new(reads),
                writes: Box::new(writes),
                resizes: Box::new(resizes),
                calls: std::sync::Mutex::new(Vec::new()),
            })
        }

        /// Records `call`, and returns how many calls of its kind came before it.
        fn take(&self, call: Call) -> usize {
            let mut calls = self.calls.lock().expect("not poisoned");
            let kind = std::mem::discriminant(&call);
            let index = calls
                .iter()
                .filter(|seen| std::mem::discriminant(*seen) == kind)
                .count();
            calls.push(call);
            index
        }

        fn calls(&self) -> Vec<Call> {
            self.calls.lock().expect("not poisoned").clone()
        }

        fn resized_to(&self) -> Vec<MaxTopicSize> {
            self.calls()
                .into_iter()
                .filter_map(|call| match call {
                    Call::Resize(size) => Some(size),
                    Call::Read | Call::Add => None,
                })
                .collect()
        }

        fn reads(&self) -> usize {
            self.calls()
                .iter()
                .filter(|call| **call == Call::Read)
                .count()
        }

        fn writes(&self) -> usize {
            self.calls()
                .iter()
                .filter(|call| **call == Call::Add)
                .count()
        }
    }

    impl GrowthBridge for ScriptedIggy {
        async fn read_topic(
            &self,
            _kafka_topic: &str,
        ) -> core::result::Result<Option<TopicState>, BridgeError> {
            let call = self.take(Call::Read);
            (self.reads)(call).resolve().await
        }

        async fn add_partitions(
            &self,
            _kafka_topic: &str,
            _added: u32,
        ) -> core::result::Result<(), BridgeError> {
            let call = self.take(Call::Add);
            (self.writes)(call).resolve().await
        }

        async fn resize_topic(
            &self,
            _kafka_topic: &str,
            max_topic_size: MaxTopicSize,
        ) -> core::result::Result<(), BridgeError> {
            let call = self.take(Call::Resize(max_topic_size));
            (self.resizes)(call).resolve().await
        }
    }

    const GIB: u64 = 1024 * 1024 * 1024;

    fn gib(count: u64) -> MaxTopicSize {
        MaxTopicSize::Custom(IggyByteSize::from(count * GIB))
    }

    /// Topic 7 of [`plan`] at `count` partitions, with a cap of `cap_gib` GiB in all.
    fn capped(count: u32, cap_gib: u64) -> Reply<Option<TopicState>> {
        Reply::Now(Ok(Some(TopicState {
            max_topic_size: cap_gib * GIB,
            ..topic_state(7, 1_000, count)
        })))
    }

    /// Topic 7 of [`plan`] at `count` partitions, with a cap of 1 GiB for each.
    fn capped_at(count: u32) -> Reply<Option<TopicState>> {
        capped(count, u64::from(count))
    }

    /// Read 0 finds topic 7 at 3 partitions with 1 GiB for each, and every later read `after`.
    fn capped_then(
        after: fn() -> Reply<Option<TopicState>>,
    ) -> impl Fn(usize) -> Reply<Option<TopicState>> + Send + Sync + 'static {
        move |call| if call == 0 { capped_at(3) } else { after() }
    }

    /// Topic 7 of [`plan`] at `count` partitions.
    fn at(count: u32) -> Reply<Option<TopicState>> {
        Reply::Now(Ok(Some(topic_state(7, 1_000, count))))
    }

    /// Unchanged at 3 for `unchanged_reads` reads, then grown to the requested 5.
    fn lands_after(
        unchanged_reads: usize,
    ) -> impl Fn(usize) -> Reply<Option<TopicState>> + Send + Sync + 'static {
        move |call| {
            if call <= unchanged_reads {
                at(3)
            } else {
                at(5)
            }
        }
    }

    fn far_deadline() -> Instant {
        Instant::now() + MAX_REQUEST_TIMEOUT
    }

    /// Grows "orders" from 3 to 5 through `bridge`.
    async fn grow_orders(bridge: &Arc<ScriptedIggy>, deadline: Instant) -> Verdict {
        grow_locked(Arc::clone(bridge), topic("orders", 5), deadline, NODE_ID).await
    }

    fn may_have_been_added() -> Verdict {
        (
            ERROR_REQUEST_TIMED_OUT,
            Some(StrBytes::from_static_str(MAY_HAVE_BEEN_ADDED)),
        )
    }

    fn nothing_was_added() -> Verdict {
        (
            ERROR_REQUEST_TIMED_OUT,
            Some(StrBytes::from_static_str(NOTHING_ADDED)),
        )
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_unknown_write_outcome_when_the_write_shows_later_should_answer_none() {
        let errors: [fn() -> BridgeError; 8] = [
            || BridgeError::Timeout,
            || BridgeError::SendLost(IggyError::Disconnected),
            || BridgeError::Iggy(IggyError::TransientNotCommitted),
            || BridgeError::Iggy(IggyError::Disconnected),
            || BridgeError::Iggy(IggyError::EmptyResponse),
            || BridgeError::Iggy(IggyError::TcpError),
            || BridgeError::Iggy(IggyError::NotConnected),
            || BridgeError::Iggy(IggyError::InvalidCommand),
        ];
        for error in errors {
            let label = error().to_string();
            let bridge = ScriptedIggy::new(lands_after(3), move |_| Reply::Now(Err(error())));
            assert_eq!(
                grow_orders(&bridge, far_deadline()).await,
                (ERROR_NONE, None),
                "{label}"
            );
            assert_eq!(bridge.reads(), 5, "{label}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_unknown_write_outcome_when_rereads_fail_first_should_keep_reading() {
        let bridge = ScriptedIggy::new(
            |call| match call {
                0 => at(3),
                1 | 2 => Reply::Now(Err(BridgeError::Iggy(IggyError::Disconnected))),
                _ => at(5),
            },
            |_| Reply::Now(Err(BridgeError::Timeout)),
        );
        assert_eq!(
            grow_orders(&bridge, far_deadline()).await,
            (ERROR_NONE, None)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_unknown_write_outcome_when_the_count_moves_past_the_target_should_stop() {
        let bridge = ScriptedIggy::new(
            |call| if call == 0 { at(3) } else { at(7) },
            |_| Reply::Now(Err(BridgeError::Timeout)),
        );
        let (code, _) = grow_orders(&bridge, far_deadline()).await;
        assert_eq!(code, ERROR_INVALID_PARTITIONS);
        assert_eq!(bridge.reads(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_write_that_never_shows_should_release_by_the_settle_limit_after_it_started() {
        type Case = (&'static str, fn() -> Reply<()>, Reread);
        let cases: [Case; 3] = [
            (
                "write times out, reads unchanged",
                || Reply::After(Duration::from_secs(15), Err(BridgeError::Timeout)),
                || at(3),
            ),
            (
                "write times out, reads hang",
                || Reply::After(Duration::from_secs(15), Err(BridgeError::Timeout)),
                || Reply::Never,
            ),
            ("write hangs", || Reply::Never, || at(3)),
        ];
        for (label, write, reread) in cases {
            let bridge = ScriptedIggy::new(
                move |call| if call == 0 { at(3) } else { reread() },
                move |_| write(),
            );
            let started = Instant::now();
            let verdict = tokio::time::timeout(
                Duration::from_secs(600),
                grow_orders(&bridge, far_deadline()),
            )
            .await
            .unwrap_or_else(|_| panic!("{label}: the lock was never released"));
            assert_eq!(verdict, may_have_been_added(), "{label}");
            assert!(
                started.elapsed() <= SETTLE_LIMIT,
                "{label}: held for {:?}",
                started.elapsed()
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_request_too_old_should_answer_from_one_reread() {
        let cases: [(Reread, Verdict); 5] = [
            (|| at(5), (ERROR_NONE, None)),
            (|| at(3), nothing_was_added()),
            (
                || Reply::Now(Ok(None)),
                (ERROR_UNKNOWN_TOPIC_OR_PARTITION, None),
            ),
            (|| at(7), (ERROR_INVALID_PARTITIONS, None)),
            (
                || Reply::Now(Err(BridgeError::Iggy(IggyError::Disconnected))),
                may_have_been_added(),
            ),
        ];
        for (reread, (expected_code, expected_message)) in cases {
            let bridge = ScriptedIggy::new(
                move |call| if call == 1 { reread() } else { at(3) },
                |_| Reply::Now(Err(BridgeError::Iggy(IggyError::RequestTooOld))),
            );
            let (code, message) = grow_orders(&bridge, far_deadline()).await;
            assert_eq!(code, expected_code);
            if expected_message.is_some() {
                assert_eq!(message, expected_message);
            }
            assert_eq!(bridge.reads(), 2, "one re-read, no settling");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_acknowledged_write_should_answer_from_the_reread() {
        let cases: [(Reread, i16); 6] = [
            (|| at(5), ERROR_NONE),
            (|| at(7), ERROR_INVALID_PARTITIONS),
            (|| Reply::Now(Ok(None)), ERROR_UNKNOWN_TOPIC_OR_PARTITION),
            (
                || Reply::Now(Ok(Some(topic_state(8, 1_000, 5)))),
                ERROR_UNKNOWN_TOPIC_OR_PARTITION,
            ),
            // Iggy acknowledged the write, so a failed or hung check does not turn it into a 7.
            (|| Reply::Now(Err(BridgeError::Timeout)), ERROR_NONE),
            (|| Reply::Never, ERROR_NONE),
        ];
        for (reread, expected_code) in cases {
            let bridge = ScriptedIggy::new(
                move |call| if call == 0 { at(3) } else { reread() },
                |_| Reply::Now(Ok(())),
            );
            let (code, _) = grow_orders(&bridge, far_deadline()).await;
            assert_eq!(code, expected_code);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_refusal_that_changed_nothing_should_answer_without_rereading() {
        let cases: [(fn() -> BridgeError, i16); 8] = [
            (
                || BridgeError::Iggy(IggyError::PartitionResizeDisabled),
                ERROR_POLICY_VIOLATION,
            ),
            (
                || BridgeError::Iggy(IggyError::PartitionsLimitReached),
                ERROR_POLICY_VIOLATION,
            ),
            (
                || BridgeError::Iggy(IggyError::PartitionIdSpaceExhausted),
                ERROR_POLICY_VIOLATION,
            ),
            (
                || BridgeError::Iggy(IggyError::TooManyPartitions),
                ERROR_INVALID_REQUEST,
            ),
            (
                || BridgeError::Iggy(IggyError::Unauthorized),
                ERROR_TOPIC_AUTHORIZATION_FAILED,
            ),
            (
                || BridgeError::Iggy(IggyError::ResourceNotFound(String::new())),
                ERROR_UNKNOWN_TOPIC_OR_PARTITION,
            ),
            (
                || BridgeError::Iggy(IggyError::TopicNameNotFound(String::new(), String::new())),
                ERROR_UNKNOWN_TOPIC_OR_PARTITION,
            ),
            (
                || BridgeError::Iggy(IggyError::TransientNotAccepted),
                ERROR_NOT_LEADER_OR_FOLLOWER,
            ),
        ];
        for (error, expected_code) in cases {
            let label = error().to_string();
            let bridge = ScriptedIggy::new(lands_after(0), move |_| Reply::Now(Err(error())));
            let (code, _) = grow_orders(&bridge, far_deadline()).await;
            assert_eq!(code, expected_code, "{label}");
            assert_eq!(bridge.reads(), 1, "{label}: no re-read");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_deadline_reached_before_the_write_should_write_nothing() {
        let instant = ScriptedIggy::new(|_| at(3), |_| Reply::Now(Ok(())));
        assert_eq!(
            grow_orders(&instant, Instant::now()).await,
            nothing_was_added()
        );
        assert_eq!(instant.writes(), 0);

        let deadline = Instant::now() + Duration::from_secs(1);
        let at_deadline = ScriptedIggy::new(
            |_| Reply::After(Duration::from_secs(1), Ok(Some(topic_state(7, 1_000, 3)))),
            |_| Reply::Now(Ok(())),
        );
        assert_eq!(
            grow_orders(&at_deadline, deadline).await,
            nothing_was_added()
        );
        assert_eq!(at_deadline.writes(), 0);

        let hung_read = ScriptedIggy::new(|_| Reply::Never, |_| Reply::Now(Ok(())));
        let verdict = tokio::time::timeout(
            Duration::from_secs(600),
            grow_orders(&hung_read, Instant::now() + Duration::from_secs(1)),
        )
        .await
        .expect("a hung read ends at the deadline");
        assert_eq!(verdict, nothing_was_added());
        assert_eq!(hung_read.writes(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_lock_won_at_the_deadline_should_not_take_it() {
        let lock = Arc::new(Mutex::new(()));
        assert!(lock_by(&lock, Instant::now()).await.is_none());

        let bridge = ScriptedIggy::new(|_| at(3), |_| Reply::Now(Ok(())));
        let result =
            grow_one_topic(&bridge, &lock, &topic("orders", 5), Instant::now(), NODE_ID).await;
        assert_eq!(result.error_code, ERROR_REQUEST_TIMED_OUT);
        assert_eq!(result.error_message.as_deref(), Some(NOTHING_ADDED));
        assert_eq!((bridge.reads(), bridge.writes()), (0, 0));
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_request_that_gives_up_mid_write_should_keep_the_lock_until_it_settles() {
        let lock = Arc::new(Mutex::new(()));
        let bridge = ScriptedIggy::new(lands_after(0), |_| {
            Reply::After(Duration::from_secs(5), Ok(()))
        });
        let result = grow_one_topic(
            &bridge,
            &lock,
            &topic("orders", 5),
            Instant::now() + Duration::from_secs(1),
            NODE_ID,
        )
        .await;
        assert_eq!(result.error_code, ERROR_REQUEST_TIMED_OUT);
        assert_eq!(result.error_message.as_deref(), Some(MAY_HAVE_BEEN_ADDED));
        assert!(lock.try_lock().is_err(), "the write is still in flight");

        tokio::time::sleep(Duration::from_secs(5)).await;
        tokio::task::yield_now().await;
        assert!(lock.try_lock().is_ok(), "the write settled");
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_topic_with_a_size_cap_when_it_grows_should_raise_the_cap_before_adding() {
        let bridge = ScriptedIggy::new(capped_then(|| capped(5, 5)), |_| Reply::Now(Ok(())));
        assert_eq!(
            grow_orders(&bridge, far_deadline()).await,
            (ERROR_NONE, None)
        );
        assert_eq!(
            bridge.calls(),
            [Call::Read, Call::Resize(gib(5)), Call::Add, Call::Read],
            "the old partitions keep their share while the new ones appear"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_topic_without_a_size_cap_when_it_grows_should_not_resize_it() {
        let bridge = ScriptedIggy::new(lands_after(0), |_| Reply::Now(Ok(())));
        assert_eq!(
            grow_orders(&bridge, far_deadline()).await,
            (ERROR_NONE, None)
        );
        assert_eq!(bridge.resized_to(), [] as [MaxTopicSize; 0]);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_raise_iggy_refuses_should_add_nothing() {
        let bridge = ScriptedIggy::resizing(
            capped_then(|| capped_at(3)),
            |_| Reply::Now(Ok(())),
            |_| Reply::Now(Err(BridgeError::Iggy(IggyError::Unauthorized))),
        );
        let (code, _) = grow_orders(&bridge, far_deadline()).await;
        assert_eq!(code, ERROR_TOPIC_AUTHORIZATION_FAILED);
        assert_eq!(bridge.calls(), [Call::Read, Call::Resize(gib(5))]);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_raise_of_unknown_outcome_should_add_nothing_and_write_the_old_cap_back() {
        let bridge = ScriptedIggy::resizing(
            capped_then(|| capped_at(3)),
            |_| Reply::Now(Ok(())),
            |call| {
                if call == 0 {
                    Reply::Now(Err(BridgeError::Timeout))
                } else {
                    Reply::Now(Ok(()))
                }
            },
        );
        assert_eq!(
            grow_orders(&bridge, far_deadline()).await,
            (
                ERROR_REQUEST_TIMED_OUT,
                Some(StrBytes::from_static_str(CAP_IN_DOUBT))
            )
        );
        assert_eq!(
            bridge.calls(),
            [Call::Read, Call::Resize(gib(5)), Call::Resize(gib(3))],
            "a retry then sizes the cap from the old one"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_add_iggy_refuses_should_write_the_old_cap_back() {
        let bridge = ScriptedIggy::new(capped_then(|| capped_at(3)), |_| {
            Reply::Now(Err(BridgeError::Iggy(IggyError::PartitionsLimitReached)))
        });
        let (code, _) = grow_orders(&bridge, far_deadline()).await;
        assert_eq!(code, ERROR_POLICY_VIOLATION);
        assert_eq!(
            bridge.calls(),
            [
                Call::Read,
                Call::Resize(gib(5)),
                Call::Add,
                Call::Resize(gib(3))
            ]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_add_whose_reply_aged_out_and_added_nothing_should_write_the_old_cap_back() {
        let bridge = ScriptedIggy::new(capped_then(|| capped(3, 5)), |_| {
            Reply::Now(Err(BridgeError::Iggy(IggyError::RequestTooOld)))
        });
        assert_eq!(
            grow_orders(&bridge, far_deadline()).await,
            nothing_was_added()
        );
        assert_eq!(bridge.resized_to(), [gib(5), gib(3)]);
    }

    #[tokio::test(start_paused = true)]
    async fn given_an_add_of_unknown_outcome_should_keep_the_raised_cap() {
        let bridge = ScriptedIggy::new(capped_then(|| capped(3, 5)), |_| {
            Reply::Now(Err(BridgeError::Timeout))
        });
        assert_eq!(
            grow_orders(&bridge, far_deadline()).await,
            may_have_been_added()
        );
        assert_eq!(
            bridge.resized_to(),
            [gib(5)],
            "the add can still land, and a raised cap keeps more, never less"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_count_grown_past_the_target_should_raise_the_cap_for_every_partition() {
        let bridge = ScriptedIggy::new(capped_then(|| capped(7, 5)), |_| Reply::Now(Ok(())));
        let (code, _) = grow_orders(&bridge, far_deadline()).await;
        assert_eq!(code, ERROR_INVALID_PARTITIONS);
        assert_eq!(
            bridge.resized_to(),
            [gib(5), gib(7)],
            "raised again from the cap read after the write"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_raise_that_ends_past_the_deadline_should_add_nothing_and_write_the_old_cap_back()
     {
        let bridge = ScriptedIggy::resizing(
            capped_then(|| capped(3, 5)),
            |_| Reply::Now(Ok(())),
            |call| {
                if call == 0 {
                    Reply::After(Duration::from_secs(2), Ok(()))
                } else {
                    Reply::Now(Ok(()))
                }
            },
        );
        let deadline = Instant::now() + Duration::from_secs(1);
        assert_eq!(grow_orders(&bridge, deadline).await, nothing_was_added());
        assert_eq!(
            bridge.calls(),
            [Call::Read, Call::Resize(gib(5)), Call::Resize(gib(3))]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_no_room_for_a_whole_write_before_the_settle_limit_should_not_start_one() {
        let bridge = ScriptedIggy::new(capped_then(|| capped_at(3)), |_| {
            Reply::After(
                SETTLE_LIMIT.saturating_sub(WRITE_LIMIT) + Duration::from_secs(1),
                Err(BridgeError::Iggy(IggyError::PartitionsLimitReached)),
            )
        });
        let started = Instant::now();
        let (code, _) = grow_orders(&bridge, far_deadline()).await;
        assert_eq!(code, ERROR_POLICY_VIOLATION);
        assert_eq!(
            bridge.resized_to(),
            [gib(5)],
            "the old cap is not written back when the write could outlast the lock"
        );
        assert!(started.elapsed() <= SETTLE_LIMIT);
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_slow_raise_should_still_release_the_lock_by_the_settle_limit() {
        let bridge = ScriptedIggy::resizing(
            capped_then(|| capped(3, 5)),
            |_| Reply::Never,
            |_| Reply::After(WRITE_LIMIT, Ok(())),
        );
        let started = Instant::now();
        let verdict = tokio::time::timeout(
            Duration::from_secs(600),
            grow_orders(&bridge, far_deadline()),
        )
        .await
        .expect("the lock is released");
        assert_eq!(verdict, may_have_been_added());
        assert!(
            started.elapsed() <= SETTLE_LIMIT,
            "held for {:?}",
            started.elapsed()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_fixed_partition_topic_should_refuse_before_writing() {
        let fixed = TopicState {
            fixed_partition_count: true,
            ..topic_state(7, 1_000, 3)
        };
        let bridge =
            ScriptedIggy::new(move |_| Reply::Now(Ok(Some(fixed))), |_| Reply::Now(Ok(())));
        let expected = (
            ERROR_POLICY_VIOLATION,
            StrBytes::from_static_str(FIXED_PARTITION_COUNT),
        );
        assert_eq!(
            plan_growth(bridge.as_ref(), &topic("orders", 5), NODE_ID).await,
            Err(expected)
        );
        assert_eq!(bridge.writes(), 0);
    }
}
