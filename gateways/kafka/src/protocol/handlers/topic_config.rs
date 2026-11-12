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

//! Topic configuration shared by `CreateTopics`, `DescribeConfigs`, `AlterConfigs` and
//! `IncrementalAlterConfigs`.
//!
//! [`keys`] lists every Kafka topic key and what the gateway does with it. This module turns the
//! configuration a request names into the Iggy settings to create or write, and answers each
//! value it refuses as Kafka does. `crate::bridge::topic_settings` maps the settings onto Iggy
//! topic options.

pub mod keys;

use std::collections::{HashMap, HashSet};

use iggy::prelude::{Durability, IggyError, IggyExpiry, MaxTopicSize};
use iggy_common::{DEFAULT_SEGMENT_SIZE, MAX_TOPIC_SEGMENT_SIZE, MIN_TOPIC_SEGMENT_SIZE};
use kafka_protocol::messages::alter_configs_request::AlterConfigsResource;
use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
use kafka_protocol::messages::incremental_alter_configs_request::AlterConfigsResource as IncrementalAlterConfigsResource;
use kafka_protocol::protocol::StrBytes;
use tokio::sync::Mutex;
use tokio::time::{Instant, timeout_at};

use crate::auth::AuthenticatedPrincipal;
use crate::bridge::topic_settings::expiry_from_retention_ms;
use crate::bridge::{
    BridgeError, CleanupPolicy, IggyBridge, KafkaTopicSettings, RetentionBytes, SettingChange,
    TopicConfigSnapshot, TopicConfigUpdate,
};
use crate::protocol::api::{
    ERROR_INVALID_CONFIG, ERROR_NONE, ERROR_REQUEST_TIMED_OUT, ERROR_UNKNOWN_TOPIC_OR_PARTITION,
};
use keys::{
    CLEANUP_POLICY, ConfigContext, ConfigType, ConfigValue, Refusal, TOPIC_KEYS, TopicKey,
    TopicKeyKind, majority, topic_key,
};

/// Kafka `ConfigResource.Type.TOPIC`.
pub const RESOURCE_TYPE_TOPIC: i8 = 2;

/// Kafka `ConfigResource.Type.BROKER`.
pub const RESOURCE_TYPE_BROKER: i8 = 4;

/// Kafka `ConfigSource.DYNAMIC_TOPIC_CONFIG`: the value was stored on the topic.
pub const CONFIG_SOURCE_TOPIC: i8 = 1;

/// Kafka `ConfigSource.STATIC_BROKER_CONFIG`: the value comes from the configuration of the
/// broker, here a gateway variable.
pub const CONFIG_SOURCE_STATIC_BROKER: i8 = 4;

/// Kafka `ConfigSource.DEFAULT_CONFIG`: the value is the default.
pub const CONFIG_SOURCE_DEFAULT: i8 = 5;

pub const ONLY_TOPIC_RESOURCES: &str = "only topic resources are supported";

/// Distinct topic names one `CreateTopics`, `CreatePartitions`, `DescribeConfigs`, `AlterConfigs`
/// or `IncrementalAlterConfigs` request may address through the bridge.
///
/// `bounds_guard`'s element ceiling is a pre-decode limit. `CreateTopics` can spend several
/// lockstep Iggy calls per name on the shared client (`ensure_stream`, `create_topic`, and a
/// possible race-retry read). 100 keeps that worst case small and is shared with the config
/// APIs. Duplicate names count once toward the cap and are rejected on their own. A larger
/// batch is `POLICY_VIOLATION` on every resource.
pub const MAX_CONFIG_TOPICS: usize = 100;

const NOT_AUTHORIZED_TO_ALTER: &str =
    "the authenticated principal is not authorized to alter topic configuration";

/// Why the configuration of one topic cannot be created or stored. Every variant is
/// `INVALID_CONFIG` (40).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigFault {
    UnknownKey(String),
    /// `CreateTopics` keys without a value, in request order.
    NullValues(Vec<String>),
    Repeated(String),
    InvalidValue {
        name: String,
        value: String,
        reason: String,
    },
    /// Kafka's whole text for a value it refuses without naming the value.
    Message(String),
    /// A key Iggy sets only when it creates a topic, which an alter would reset.
    Unresettable {
        name: &'static str,
        current: u64,
    },
    /// An `APPEND` or `SUBTRACT` on a key that is not a list, in Kafka's own words.
    NotAList {
        operation: ConfigOperation,
        key: String,
    },
    /// A retention that would remove records from a topic that keeps every record, from an alter
    /// that does not name `cleanup.policy`.
    KeepsEveryRecord {
        name: &'static str,
    },
}

impl ConfigFault {
    #[must_use]
    pub fn message(&self) -> String {
        match self {
            Self::UnknownKey(name) => format!("Unknown topic config name: {name}"),
            Self::NullValues(names) => format!(
                "Null value not supported for topic configs: {}",
                names.join(",")
            ),
            Self::Repeated(name) => format!("config key '{name}' is repeated"),
            Self::InvalidValue {
                name,
                value,
                reason,
            } => format!("Invalid value {value} for configuration {name}: {reason}"),
            Self::Message(message) => message.clone(),
            Self::Unresettable { name, current } => format!(
                "{name} cannot go back to its default, because Iggy sets it only when it creates \
                 a topic. This topic has {current}."
            ),
            Self::NotAList { operation, key } => {
                let operation = operation.kafka_name();
                format!("Can't {operation} to key {key} because its type is not LIST.")
            }
            Self::KeepsEveryRecord { name } => format!(
                "This topic keeps every record, as a topic with cleanup.policy=compact does. The \
                 gateway does not store cleanup.policy, so it applies {name} only when the same \
                 request names cleanup.policy."
            ),
        }
    }
}

/// Kafka `AlterConfigOp.OpType`, one per `IncrementalAlterConfigs` entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigOperation {
    Set,
    Delete,
    Append,
    Subtract,
}

impl ConfigOperation {
    /// The spelling Kafka's own error text uses.
    #[must_use]
    pub const fn kafka_name(self) -> &'static str {
        match self {
            Self::Set => "SET",
            Self::Delete => "DELETE",
            Self::Append => "APPEND",
            Self::Subtract => "SUBTRACT",
        }
    }
}

impl TryFrom<i8> for ConfigOperation {
    /// The byte that matched nothing, for the client-facing text.
    type Error = i8;

    fn try_from(operation: i8) -> Result<Self, Self::Error> {
        match operation {
            0 => Ok(Self::Set),
            1 => Ok(Self::Delete),
            2 => Ok(Self::Append),
            3 => Ok(Self::Subtract),
            other => Err(other),
        }
    }
}

/// One `IncrementalAlterConfigs` entry that passed [`incremental_ops`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigOp<'a> {
    pub name: &'a str,
    pub operation: ConfigOperation,
    pub value: Option<&'a str>,
}

/// Why one resource's incremental alter is refused before any Iggy call. Every variant is
/// `INVALID_REQUEST` (42). The duplicate-key and null-value texts are Kafka's own; the
/// unknown-operation text is borrowed from Kafka's broker-resource check, because Kafka's topic
/// path has no well-formed answer for it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequestFault {
    DuplicateKeys,
    NullValues(Vec<String>),
    UnknownOperation(i8),
}

impl RequestFault {
    #[must_use]
    pub fn message(&self) -> String {
        match self {
            Self::DuplicateKeys => "Error due to duplicate config keys".to_string(),
            Self::NullValues(names) => {
                format!("Null value not supported for : {}", names.join(", "))
            }
            Self::UnknownOperation(operation) => format!("Unknown operations type {operation}"),
        }
    }
}

/// Whether `names` contains more than [`MAX_CONFIG_TOPICS`] distinct strings.
#[must_use]
pub fn exceeds_topic_cap<'a>(names: impl IntoIterator<Item = &'a str>) -> bool {
    names.into_iter().collect::<HashSet<_>>().len() > MAX_CONFIG_TOPICS
}

/// Client-facing text for a request over [`MAX_CONFIG_TOPICS`].
#[must_use]
pub fn topic_cap_message(api_name: &str) -> String {
    format!(
        "this gateway addresses at most {MAX_CONFIG_TOPICS} distinct topics per {api_name} request"
    )
}

/// Names in `names` that occur more than once.
///
/// Real Kafka refuses a duplicate resource name with `INVALID_REQUEST` (42) rather than
/// silently picking a winner. Shared by `CreateTopics`, `CreatePartitions`, `DescribeConfigs`
/// and `AlterConfigs`. `CreatePartitions` answers once per repeated name, as Kafka's
/// `ControllerApis.createPartitions` does, the others once per occurrence.
#[must_use]
pub fn find_duplicate_names<'a>(names: impl IntoIterator<Item = &'a str>) -> HashSet<&'a str> {
    let mut seen = HashSet::new();
    let mut duplicates = HashSet::new();
    for name in names {
        if !seen.insert(name) {
            duplicates.insert(name);
        }
    }
    duplicates
}

/// A resource `DescribeConfigs` or `AlterConfigs` can filter down to the ones naming a topic,
/// and check for the cap and for a duplicate name. The two APIs' generated request resource
/// types have no shared trait of their own, so this is the smallest common surface letting
/// [`topic_cap_and_duplicates`] serve both without either handler re-walking its resource list.
pub trait TopicResource {
    fn kafka_resource_type(&self) -> i8;
    fn kafka_resource_name(&self) -> &str;
}

impl TopicResource for AlterConfigsResource {
    fn kafka_resource_type(&self) -> i8 {
        self.resource_type
    }

    fn kafka_resource_name(&self) -> &str {
        self.resource_name.as_str()
    }
}

impl TopicResource for DescribeConfigsResource {
    fn kafka_resource_type(&self) -> i8 {
        self.resource_type
    }

    fn kafka_resource_name(&self) -> &str {
        self.resource_name.as_str()
    }
}

impl TopicResource for IncrementalAlterConfigsResource {
    fn kafka_resource_type(&self) -> i8 {
        self.resource_type
    }

    fn kafka_resource_name(&self) -> &str {
        self.resource_name.as_str()
    }
}

/// The cap/duplicate verdict shared by `DescribeConfigs` and `AlterConfigs`: both filter
/// resources down to [`RESOURCE_TYPE_TOPIC`] before deciding whether the request exceeds
/// [`MAX_CONFIG_TOPICS`] or repeats a name, and the business decision is identical even
/// though the two APIs render it into different response types.
#[must_use]
pub fn topic_cap_and_duplicates<R: TopicResource>(resources: &[R]) -> (bool, HashSet<&str>) {
    let names: Vec<&str> = resources
        .iter()
        .filter(|resource| resource.kafka_resource_type() == RESOURCE_TYPE_TOPIC)
        .map(TopicResource::kafka_resource_name)
        .collect();
    (
        exceeds_topic_cap(names.iter().copied()),
        find_duplicate_names(names.iter().copied()),
    )
}

/// Whether a `DescribeConfigs` request asks for more than [`MAX_CONFIG_TOPICS`] answers: one for
/// each distinct topic, and one for each BROKER resource. The gateway builds each broker answer
/// itself, so a repeated BROKER resource counts each time, and a request of only BROKER resources
/// cannot grow the response without bound.
#[must_use]
pub fn exceeds_describe_cap(resources: &[DescribeConfigsResource]) -> bool {
    let topics: HashSet<&str> = resources
        .iter()
        .filter(|resource| resource.resource_type == RESOURCE_TYPE_TOPIC)
        .map(|resource| resource.resource_name.as_str())
        .collect();
    let brokers = resources
        .iter()
        .filter(|resource| resource.resource_type == RESOURCE_TYPE_BROKER)
        .count();
    topics.len().saturating_add(brokers) > MAX_CONFIG_TOPICS
}

/// Client-facing text for a `DescribeConfigs` request over [`exceeds_describe_cap`].
#[must_use]
pub fn describe_cap_message() -> String {
    format!(
        "this gateway answers at most {MAX_CONFIG_TOPICS} topic and broker resources per \
         DescribeConfigs request"
    )
}

/// The denial every resource gets when `principal` may not alter topic configuration, or
/// `None` when the alter may run.
///
/// No principal means no listener of this gateway names principals, so there is nothing to
/// enforce against. Once one does, a connection that proved no identity carries
/// [`AuthenticatedPrincipal::anonymous`] instead and is denied here. Permissions that were never
/// read deny: granting would authorize a write off a value nothing read.
#[must_use]
pub fn alter_denial(
    principal: Option<&AuthenticatedPrincipal>,
    api_name: &str,
) -> Option<StrBytes> {
    let principal = principal?;
    if principal.permissions_known && principal.permissions.manage_topics {
        return None;
    }
    if !principal.permissions_known {
        tracing::warn!(
            principal = %principal.username,
            "{api_name} asked on a connection whose permissions were never read; denying"
        );
    }
    Some(StrBytes::from_static_str(NOT_AUTHORIZED_TO_ALTER))
}

/// The reason a topic name failed Kafka's own naming rules, or a fixed fallback.
#[must_use]
pub fn name_reason(error: &BridgeError) -> String {
    match error {
        BridgeError::InvalidKafkaTopicName { reason, .. } => reason.clone(),
        _ => "invalid topic name".to_string(),
    }
}

/// Maps a bridge failure to a Kafka error code and client-facing message, logging the real
/// cause server-side.
///
/// `handler` names the API in the log line (`"DescribeConfigs"`/`"AlterConfigs"`); `action` is
/// the present participle of what the bridge call was doing (`"reading"`/`"altering"`), reused
/// in both the timeout log and the generic internal-error message.
///
/// Only [`BridgeError::Timeout`] gets its own arm. `InvalidKafkaTopicName` and `SendLost` are
/// unreachable here: the handler already validated the Kafka name before calling the bridge (so
/// the bridge's own, redundant validation inside `get_kafka_topic`/`update_kafka_topic_config`
/// cannot fail), and `SendLost` is only ever produced by the Produce path (`iggy_bridge/produce.rs`),
/// never by a config read or write. Both fall into the generic arm below rather than keeping a
/// branch no call site here can reach.
#[must_use]
pub fn bridge_failure(error: &BridgeError, handler: &str, action: &str) -> (i16, Option<StrBytes>) {
    let code = error.to_kafka_error_code();
    match error {
        BridgeError::Timeout => {
            tracing::warn!(%error, "{handler} {action} exceeded the bridge deadline");
            (code, None)
        }
        other => {
            tracing::error!(%other, "{handler} failed while {action} topic configuration");
            (
                code,
                Some(StrBytes::from(format!(
                    "internal error {action} topic configuration"
                ))),
            )
        }
    }
}

/// The entries of one `IncrementalAlterConfigs` resource, checked in the order Kafka's broker
/// checks them before it forwards the request: a repeated name, a null value on anything but a
/// delete, then an operation byte it does not know.
///
/// # Errors
///
/// Returns the first [`RequestFault`] in that order.
pub fn incremental_ops<'a>(
    configs: impl IntoIterator<Item = (&'a str, i8, Option<&'a str>)>,
) -> Result<Vec<ConfigOp<'a>>, RequestFault> {
    let configs: Vec<(&str, i8, Option<&str>)> = configs.into_iter().collect();
    let mut seen = HashSet::new();
    if configs.iter().any(|(name, _, _)| !seen.insert(*name)) {
        return Err(RequestFault::DuplicateKeys);
    }
    let null_values: Vec<String> = configs
        .iter()
        .filter(|(_, operation, value)| {
            value.is_none() && ConfigOperation::try_from(*operation) != Ok(ConfigOperation::Delete)
        })
        .map(|(name, _, _)| (*name).to_string())
        .collect();
    if !null_values.is_empty() {
        return Err(RequestFault::NullValues(null_values));
    }
    configs
        .into_iter()
        .map(|(name, operation, value)| {
            let operation =
                ConfigOperation::try_from(operation).map_err(RequestFault::UnknownOperation)?;
            Ok(ConfigOp {
                name,
                operation,
                value,
            })
        })
        .collect()
}

/// `AlterConfigs` null values, in Kafka's words for the request fault (42) it answers before any
/// other check.
#[must_use]
pub fn null_values<'a>(
    configs: impl IntoIterator<Item = (&'a str, Option<&'a str>)>,
) -> Option<RequestFault> {
    let names: Vec<String> = configs
        .into_iter()
        .filter(|(_, value)| value.is_none())
        .map(|(name, _)| name.to_string())
        .collect();
    (!names.is_empty()).then_some(RequestFault::NullValues(names))
}

/// Logs that `kafka_topic` asks for compaction, which Iggy does not do (#4521). `changed` tells
/// whether the request changed the topic. A request that only repeats the policy logs at debug,
/// because tools such as the Strimzi Topic Operator send it again on every pass.
pub fn log_compaction(kafka_topic: &str, policy: CleanupPolicy, changed: bool) {
    if !policy.compact {
        return;
    }
    let effect = if policy.delete {
        "removes old records by retention only"
    } else {
        "keeps every record"
    };
    if changed {
        tracing::warn!(
            kafka_topic,
            cleanup_policy = %policy,
            "the Kafka topic asks for compaction, which Iggy does not do (#4521), so it {effect}"
        );
    } else {
        tracing::debug!(
            kafka_topic,
            cleanup_policy = %policy,
            "the Kafka topic asks for compaction again, so it still {effect}"
        );
    }
}

/// Logs that `kafka_topic` keeps more than its `retention.bytes`, as the server warns for such a
/// cap (`warn_unenforceable_topic_size`). Iggy keeps the newest sealed segment of each partition
/// whatever the size cap.
pub fn log_retention_below_segment(
    kafka_topic: &str,
    (retention_bytes, segment_bytes): (u64, u64),
) {
    tracing::warn!(
        kafka_topic,
        retention_bytes,
        segment_bytes,
        "retention.bytes is below segment.bytes, and Iggy keeps the newest sealed segment of each \
         partition, so each partition keeps more than retention.bytes"
    );
}

/// The answer for one resource of an alter: the Kafka code and its text.
pub type AlterAnswer = (i16, Option<StrBytes>);

/// Writes what an alter planned for `kafka_topic`, and answers it.
///
/// A size cap is the share of each partition times the partition count, and `CreatePartitions`
/// changes that count. So an update that writes a size cap takes `partition_growth`, reads the
/// topic again, and writes what `replan` makes of that read. A grow on this gateway then never
/// runs between the count the cap was sized for and the write. A grow on another gateway still
/// can: closing that needs Iggy to store the share of each partition.
pub async fn write_alter(
    bridge: &IggyBridge,
    partition_growth: &Mutex<()>,
    kafka_topic: &str,
    planned: TopicConfigUpdate,
    replan: impl FnOnce(&TopicConfigSnapshot) -> Result<TopicConfigUpdate, ConfigFault>,
    deadline: Instant,
    api_name: &str,
) -> AlterAnswer {
    if planned.max_topic_size.is_none() {
        return write_update(bridge, kafka_topic, &planned, deadline, api_name).await;
    }
    let Ok(_growth) = timeout_at(deadline, partition_growth.lock()).await else {
        tracing::warn!(
            kafka_topic,
            "{api_name} waited past its deadline for a partition change on this gateway; nothing \
             was written"
        );
        return (ERROR_REQUEST_TIMED_OUT, None);
    };
    let topic = match timeout_at(deadline, bridge.get_kafka_topic(kafka_topic)).await {
        Ok(Ok(Some(details))) => {
            TopicConfigSnapshot::new(&details.options, details.partitions_count)
        }
        Ok(Ok(None)) => return (ERROR_UNKNOWN_TOPIC_OR_PARTITION, None),
        Ok(Err(error)) => return bridge_failure(&error, api_name, "altering"),
        Err(_elapsed) => return (ERROR_REQUEST_TIMED_OUT, None),
    };
    match replan(&topic) {
        Ok(update) => write_update(bridge, kafka_topic, &update, deadline, api_name).await,
        Err(fault) => (ERROR_INVALID_CONFIG, Some(StrBytes::from(fault.message()))),
    }
}

/// Writes `update`. An empty one writes nothing: Kafka answers an empty operation list, and a
/// delete of an override that was never set, the same way.
///
/// The write does not start past `deadline` and is not wrapped in `timeout_at`. Dropping the
/// future does not abort the SDK call, so `UpdateTopic` could commit after the answer, and a
/// short retention would then delete sealed segments. The bridge bounds its own round trip with
/// `with_request_timeout`, so a write can still commit after this answers `REQUEST_TIMED_OUT`.
async fn write_update(
    bridge: &IggyBridge,
    kafka_topic: &str,
    update: &TopicConfigUpdate,
    deadline: Instant,
    api_name: &str,
) -> AlterAnswer {
    if update.is_empty() {
        if let Some(policy) = update.cleanup_policy {
            log_compaction(kafka_topic, policy, false);
        }
        log_kept_segment_size(kafka_topic, update);
        return (ERROR_NONE, None);
    }
    if Instant::now() >= deadline {
        tracing::warn!(
            kafka_topic,
            "{api_name} deadline passed before update_topic; not starting the write"
        );
        return (ERROR_REQUEST_TIMED_OUT, None);
    }
    match bridge.update_kafka_topic_config(kafka_topic, update).await {
        Ok(()) | Err(BridgeError::Iggy(IggyError::RequestAlreadyApplied)) => {
            if let Some(policy) = update.cleanup_policy {
                log_compaction(kafka_topic, policy, true);
            }
            if let Some(below) = update.retention_below_segment {
                log_retention_below_segment(kafka_topic, below);
            }
            log_kept_segment_size(kafka_topic, update);
            (ERROR_NONE, None)
        }
        Err(error) => bridge_failure(&error, api_name, "altering"),
    }
}

/// Logs that an `AlterConfigs` left out the `segment.bytes` of a topic that Iggy created with
/// another size. Kafka would reset it. Iggy cannot, so the topic keeps it.
fn log_kept_segment_size(kafka_topic: &str, update: &TopicConfigUpdate) {
    if let Some(segment_bytes) = update.kept_segment_size {
        tracing::info!(
            kafka_topic,
            segment_bytes,
            "AlterConfigs left segment.bytes out, and Iggy cannot change it after it creates a \
             topic, so the topic keeps it"
        );
    }
}

/// The settings that the configuration of one `CreateTopics` topic names, for a topic that gets
/// `replication_factor` nodes when the gateway knows it.
///
/// A name that repeats keeps its last value, as Kafka's `translateCreationConfigs` does. Each
/// value is checked as Kafka checks it, and then against what the gateway can apply: a fixed key
/// must name the value the gateway reports, `min.insync.replicas` must be at most the majority
/// of the replication factor, and `max.message.bytes` must fit the largest batch. A
/// `retention.bytes` below one segment is accepted: Iggy keeps the newest sealed segment of each
/// partition, which deletes nothing that Kafka would keep.
///
/// # Errors
///
/// Returns every null value at once, as Kafka does, otherwise the first [`ConfigFault`] in
/// request order.
pub fn plan_create<'a>(
    configs: impl IntoIterator<Item = (&'a str, Option<&'a str>)>,
    context: &ConfigContext<'_>,
    replication_factor: Option<u32>,
) -> Result<KafkaTopicSettings, ConfigFault> {
    let named = last_of_each_name(configs);
    let nulls: Vec<String> = named
        .iter()
        .filter(|(_, value)| value.is_none())
        .map(|(name, _)| (*name).to_string())
        .collect();
    if !nulls.is_empty() {
        return Err(ConfigFault::NullValues(nulls));
    }

    let mut settings = KafkaTopicSettings::default();
    for (name, value) in named {
        let key = topic_key(name).ok_or_else(|| ConfigFault::UnknownKey(name.to_string()))?;
        let value = value.unwrap_or_default();
        match key.kind {
            TopicKeyKind::RetentionMs => settings.message_expiry = Some(retention_ms(key, value)?),
            TopicKeyKind::RetentionBytes => {
                settings.retention_bytes = Some(retention_bytes(key, value)?);
            }
            TopicKeyKind::SegmentBytes => settings.segment_size = Some(segment_bytes(key, value)?),
            TopicKeyKind::CleanupPolicy => settings.cleanup_policy = cleanup_policy(key, value)?,
            TopicKeyKind::MinInsyncReplicas => {
                within_majority(key, value, replication_factor)?;
            }
            TopicKeyKind::Flush { every_write } => {
                if flushes_every_write(key, value, every_write)? {
                    settings.durability = Some(Durability::Persisted);
                }
            }
            TopicKeyKind::MaxMessageBytes => within_largest_batch(key, value, context)?,
            TopicKeyKind::Ignored | TopicKeyKind::Fixed { .. } => {
                key.check(value).map_err(|refusal| refused(key, refusal))?;
            }
        }
    }
    Ok(settings)
}

/// Each name of `configs` once, with the value of its last occurrence, in the order of those last
/// occurrences. One pass over the list, so a request of many names costs time in proportion to
/// them.
fn last_of_each_name<'a>(
    configs: impl IntoIterator<Item = (&'a str, Option<&'a str>)>,
) -> Vec<(&'a str, Option<&'a str>)> {
    let configs: Vec<(&str, Option<&str>)> = configs.into_iter().collect();
    let mut last = HashMap::with_capacity(configs.len());
    for (index, (name, _)) in configs.iter().enumerate() {
        last.insert(*name, index);
    }
    configs
        .iter()
        .enumerate()
        .filter(|(index, (name, _))| last.get(name) == Some(index))
        .map(|(_, config)| *config)
        .collect()
}

/// What one `AlterConfigs` resource writes.
///
/// Kafka's `AlterConfigs` replaces the whole configuration of a topic, so each stored key the
/// resource leaves out goes back to its default: the gateway default where one is set, else the
/// Iggy default. A key that a client never set stays as it is. `segment.bytes` cannot go back
/// once Iggy created the topic with another size, so a resource that leaves it out keeps it, and
/// the handler logs that. A `cleanup.policy` without `delete` keeps every record, whatever the
/// retention keys say.
///
/// The gateway does not store `cleanup.policy`. So on a topic that keeps every record, a resource
/// that leaves the policy out keeps every record too: the retention keys it leaves out stay, and
/// a retention it names that would remove records is refused.
///
/// # Errors
///
/// Returns the first [`ConfigFault`]: the named keys in request order, then the keys left out.
pub fn plan_replace<'a>(
    configs: impl IntoIterator<Item = (&'a str, Option<&'a str>)>,
    topic: &TopicConfigSnapshot,
    context: &ConfigContext<'_>,
) -> Result<TopicConfigUpdate, ConfigFault> {
    let configs: Vec<(&str, Option<&str>)> = configs.into_iter().collect();
    let keeps_records =
        topic.keeps_every_record() && !configs.iter().any(|(name, _)| *name == CLEANUP_POLICY);
    let planned = Planned {
        topic,
        context,
        keeps_records,
    };
    let mut named = HashSet::with_capacity(configs.len());
    let mut update = TopicConfigUpdate::default();
    for (name, value) in configs {
        if !named.insert(name) {
            return Err(ConfigFault::Repeated(name.to_string()));
        }
        let key = topic_key(name).ok_or_else(|| ConfigFault::UnknownKey(name.to_string()))?;
        plan_set(&mut update, key, value.unwrap_or_default(), &planned)?;
    }
    for key in TOPIC_KEYS.iter().filter(|key| !named.contains(key.name)) {
        if key.kind == TopicKeyKind::SegmentBytes {
            update.kept_segment_size =
                (topic.segment_size != DEFAULT_SEGMENT_SIZE).then_some(topic.segment_size);
            continue;
        }
        plan_delete(&mut update, key, topic, context, keeps_records)?;
    }
    settle_retention(&mut update, topic);
    Ok(update)
}

/// What one `IncrementalAlterConfigs` resource writes.
///
/// `SET` writes a stored key, and checks any other key as [`plan_create`] does. `DELETE` takes a
/// stored key back to its default, as [`plan_replace`] does for a key left out, and changes
/// nothing for any other key. `APPEND` and `SUBTRACT` apply to a list key from its Kafka default,
/// which for `cleanup.policy` is `delete`, and then check the list as a `SET` does. On any other
/// key they are refused with Kafka's own text, a key Kafka does not know among them.
///
/// The gateway does not store `cleanup.policy`. So on a topic that keeps every record, a resource
/// that does not name the policy keeps every record too: a `DELETE` of a retention key changes
/// nothing, and a retention that would remove records is refused.
///
/// # Errors
///
/// Returns the first [`ConfigFault`] in request order.
pub fn plan_incremental(
    ops: &[ConfigOp<'_>],
    topic: &TopicConfigSnapshot,
    context: &ConfigContext<'_>,
) -> Result<TopicConfigUpdate, ConfigFault> {
    let keeps_records =
        topic.keeps_every_record() && !ops.iter().any(|op| op.name == CLEANUP_POLICY);
    let planned = Planned {
        topic,
        context,
        keeps_records,
    };
    let mut update = TopicConfigUpdate::default();
    for op in ops {
        let key = topic_key(op.name);
        match op.operation {
            ConfigOperation::Set => {
                let key = key.ok_or_else(|| ConfigFault::UnknownKey(op.name.to_string()))?;
                plan_set(&mut update, key, op.value.unwrap_or_default(), &planned)?;
            }
            ConfigOperation::Delete => {
                if let Some(key) = key {
                    plan_delete(&mut update, key, topic, context, keeps_records)?;
                }
            }
            ConfigOperation::Append | ConfigOperation::Subtract => {
                let append = op.operation == ConfigOperation::Append;
                let values = op.value.unwrap_or_default();
                let list = key
                    .filter(|key| key.config_type == ConfigType::List)
                    .and_then(|key| Some((key, key.default_list_after(append, values)?)));
                let Some((key, list)) = list else {
                    return Err(ConfigFault::NotAList {
                        operation: op.operation,
                        key: op.name.to_string(),
                    });
                };
                plan_set(&mut update, key, &list, &planned)?;
            }
        }
    }
    settle_retention(&mut update, topic);
    Ok(update)
}

/// What a `SET` on an existing topic is planned against.
struct Planned<'a> {
    topic: &'a TopicConfigSnapshot,
    context: &'a ConfigContext<'a>,
    /// The topic keeps every record, and the alter does not name `cleanup.policy`.
    keeps_records: bool,
}

/// A `SET` of `key` to `value` on an existing topic.
fn plan_set(
    update: &mut TopicConfigUpdate,
    key: &TopicKey,
    value: &str,
    planned: &Planned<'_>,
) -> Result<(), ConfigFault> {
    let topic = planned.topic;
    match key.kind {
        TopicKeyKind::RetentionMs => {
            let expiry = retention_ms(key, value)?;
            if planned.keeps_records && expiry != IggyExpiry::NeverExpire {
                return Err(ConfigFault::KeepsEveryRecord { name: key.name });
            }
            update.message_expiry = Some(SettingChange::Set(expiry));
        }
        TopicKeyKind::RetentionBytes => {
            let retention = retention_bytes(key, value)?;
            if planned.keeps_records && retention != RetentionBytes::Unlimited {
                return Err(ConfigFault::KeepsEveryRecord { name: key.name });
            }
            update.max_topic_size = Some(SettingChange::Set(
                retention.topic_size(topic.partitions_count, topic.segment_size),
            ));
            update.retention_below_segment = match retention {
                RetentionBytes::PerPartition(bytes) if bytes < topic.segment_size => {
                    Some((bytes, topic.segment_size))
                }
                _ => None,
            };
        }
        TopicKeyKind::SegmentBytes => {
            if segment_bytes(key, value)? != topic.segment_size {
                let reason = format!(
                    "Iggy sets segment.bytes only when it creates a topic, and this topic has {}",
                    topic.segment_size
                );
                return Err(invalid(key, value, reason));
            }
        }
        TopicKeyKind::CleanupPolicy => update.cleanup_policy = Some(cleanup_policy(key, value)?),
        TopicKeyKind::MinInsyncReplicas => {
            within_majority(key, value, planned.context.replication_factor_of(topic))?;
        }
        TopicKeyKind::Flush { every_write } => {
            if flushes_every_write(key, value, every_write)? && !topic.durability.is_persisted() {
                return Err(invalid(
                    key,
                    value.trim(),
                    "Iggy sets durability only when it creates a topic, and this topic does not \
                     flush every write",
                ));
            }
        }
        TopicKeyKind::MaxMessageBytes => within_largest_batch(key, value, planned.context)?,
        TopicKeyKind::Ignored | TopicKeyKind::Fixed { .. } => {
            key.check(value).map_err(|refusal| refused(key, refusal))?;
        }
    }
    Ok(())
}

/// A `DELETE` of `key` on an existing topic: back to the gateway default where one is set, else
/// the Iggy default. Nothing is written when the topic already has that value, or when no client
/// set it. `keeps_records` keeps both retention keys of a topic that keeps every record.
fn plan_delete(
    update: &mut TopicConfigUpdate,
    key: &TopicKey,
    topic: &TopicConfigSnapshot,
    context: &ConfigContext<'_>,
    keeps_records: bool,
) -> Result<(), ConfigFault> {
    let defaults = context.defaults;
    match key.kind {
        TopicKeyKind::RetentionMs | TopicKeyKind::RetentionBytes | TopicKeyKind::CleanupPolicy
            if keeps_records => {}
        TopicKeyKind::RetentionMs => {
            update.message_expiry = back_to_default(
                defaults.expiry(),
                topic.message_expiry,
                topic.message_expiry_explicit,
            );
        }
        TopicKeyKind::RetentionBytes => {
            update.max_topic_size = back_to_default(
                defaults.retention().map(|retention| {
                    retention.topic_size(topic.partitions_count, topic.segment_size)
                }),
                topic.max_topic_size,
                topic.max_topic_size_explicit,
            );
        }
        TopicKeyKind::SegmentBytes => {
            if topic.segment_size != DEFAULT_SEGMENT_SIZE {
                return Err(ConfigFault::Unresettable {
                    name: key.name,
                    current: topic.segment_size,
                });
            }
        }
        TopicKeyKind::CleanupPolicy => update.cleanup_policy = Some(CleanupPolicy::default()),
        TopicKeyKind::MinInsyncReplicas
        | TopicKeyKind::Flush { .. }
        | TopicKeyKind::MaxMessageBytes
        | TopicKeyKind::Ignored
        | TopicKeyKind::Fixed { .. } => {}
    }
    Ok(())
}

/// The retention an alter leaves, once every key of it is planned. A policy without `delete`
/// keeps every record, so it writes no expiry and no size cap in place of what the retention keys
/// say.
fn settle_retention(update: &mut TopicConfigUpdate, topic: &TopicConfigSnapshot) {
    if update
        .cleanup_policy
        .is_some_and(CleanupPolicy::keeps_every_record)
    {
        update.message_expiry = (topic.message_expiry != IggyExpiry::NeverExpire)
            .then_some(SettingChange::Set(IggyExpiry::NeverExpire));
        update.max_topic_size = (topic.max_topic_size != MaxTopicSize::Unlimited)
            .then_some(SettingChange::Set(MaxTopicSize::Unlimited));
        update.retention_below_segment = None;
    }
}

/// The write that takes a value a client set back to `gateway_default`, or to the Iggy default
/// when none is set. `None` when the value is there already, or when no client set it: such a
/// topic has the default it was created with, and Kafka changes nothing for a delete of a key
/// that has no override.
fn back_to_default<T: PartialEq + Copy>(
    gateway_default: Option<T>,
    current: T,
    explicit: bool,
) -> Option<SettingChange<T>> {
    if !explicit {
        return None;
    }
    match gateway_default {
        Some(value) if current == value => None,
        Some(value) => Some(SettingChange::Set(value)),
        None => Some(SettingChange::Reset),
    }
}

/// `retention.ms`: -1 or more, as Kafka's `atLeast(-1)` allows.
fn retention_ms(key: &TopicKey, value: &str) -> Result<IggyExpiry, ConfigFault> {
    match key.config_type.parse(value) {
        Ok(ConfigValue::Long(millis)) if millis >= -1 => Ok(expiry_from_retention_ms(millis)),
        Ok(_) => Err(invalid(key, value, "Value must be at least -1")),
        Err(reason) => Err(invalid(key, value, reason)),
    }
}

/// `retention.bytes`: any count, and a negative one caps nothing, as in Kafka. A count below one
/// segment is stored as one segment ([`RetentionBytes::topic_size`]).
fn retention_bytes(key: &TopicKey, value: &str) -> Result<RetentionBytes, ConfigFault> {
    match key.config_type.parse(value) {
        Ok(ConfigValue::Long(bytes)) => Ok(
            u64::try_from(bytes).map_or(RetentionBytes::Unlimited, RetentionBytes::PerPartition)
        ),
        Ok(_) => Err(invalid(key, value, "Not a number of type LONG")),
        Err(reason) => Err(invalid(key, value, reason)),
    }
}

/// `segment.bytes`: a size Iggy accepts, a multiple of 512 from 1 MiB to 1 GiB. Kafka accepts
/// any size from 1 MiB.
fn segment_bytes(key: &TopicKey, value: &str) -> Result<u64, ConfigFault> {
    let bytes = match key.config_type.parse(value) {
        Ok(ConfigValue::Long(bytes)) => u64::try_from(bytes).unwrap_or(0),
        Ok(_) => 0,
        Err(reason) => return Err(invalid(key, value, reason)),
    };
    if bytes < MIN_TOPIC_SEGMENT_SIZE {
        return Err(invalid(
            key,
            value,
            format!("Value must be at least {MIN_TOPIC_SEGMENT_SIZE}"),
        ));
    }
    if bytes > MAX_TOPIC_SEGMENT_SIZE || !bytes.is_multiple_of(512) {
        return Err(invalid(
            key,
            value,
            format!("Iggy needs a multiple of 512 bytes, at most {MAX_TOPIC_SEGMENT_SIZE}"),
        ));
    }
    Ok(bytes)
}

/// `min.insync.replicas`: as Kafka checks it, and at most the majority of `replication_factor`
/// when the gateway knows it. Iggy commits each write on that majority, so a smaller value gets
/// at least the copies it asks for, and a larger one cannot be applied.
fn within_majority(
    key: &TopicKey,
    value: &str,
    replication_factor: Option<u32>,
) -> Result<(), ConfigFault> {
    let parsed = key.check(value).map_err(|refusal| refused(key, refusal))?;
    if let (ConfigValue::Long(asked), Some(factor)) = (parsed, replication_factor)
        && asked > i64::from(majority(factor))
    {
        let reason = format!(
            "Iggy commits a write on a majority of the {factor} nodes that store each partition, \
             so this gateway accepts at most {}",
            majority(factor)
        );
        return Err(invalid(key, value.trim(), reason));
    }
    Ok(())
}

/// Whether `flush.messages` or `flush.ms` asks for a flush of every write, once it is checked as
/// Kafka checks it.
fn flushes_every_write(key: &TopicKey, value: &str, every_write: i64) -> Result<bool, ConfigFault> {
    let parsed = key.check(value).map_err(|refusal| refused(key, refusal))?;
    Ok(parsed == ConfigValue::Long(every_write))
}

/// `max.message.bytes`: as Kafka checks it, and from Kafka's default up to the largest batch that
/// Produce takes. A smaller value asks the topic to refuse batches that the gateway takes, and a
/// larger one asks it to take batches that the gateway refuses.
fn within_largest_batch(
    key: &TopicKey,
    value: &str,
    context: &ConfigContext<'_>,
) -> Result<(), ConfigFault> {
    let parsed = key.check(value).map_err(|refusal| refused(key, refusal))?;
    let (least, most) = (context.min_message_bytes(), context.max_message_bytes);
    if let ConfigValue::Long(bytes) = parsed
        && !(i64::from(least)..=i64::from(most)).contains(&bytes)
    {
        let reason = format!(
            "this gateway takes record batches of up to {most} bytes in every topic, so it \
             accepts {least} to {most}"
        );
        return Err(invalid(key, value.trim(), reason));
    }
    Ok(())
}

/// `cleanup.policy`: a list of `compact` and `delete`, empty or not, as Kafka checks it.
fn cleanup_policy(key: &TopicKey, value: &str) -> Result<CleanupPolicy, ConfigFault> {
    let ConfigValue::List(items) = key.check(value).map_err(|refusal| refused(key, refusal))?
    else {
        return Ok(CleanupPolicy::default());
    };
    Ok(CleanupPolicy {
        delete: items.iter().any(|item| item == "delete"),
        compact: items.iter().any(|item| item == "compact"),
    })
}

fn refused(key: &TopicKey, refusal: Refusal) -> ConfigFault {
    match refusal {
        Refusal::Value { value, reason } => ConfigFault::InvalidValue {
            name: key.name.to_string(),
            value,
            reason,
        },
        Refusal::Message(message) => ConfigFault::Message(message),
    }
}

fn invalid(key: &TopicKey, value: &str, reason: impl Into<String>) -> ConfigFault {
    ConfigFault::InvalidValue {
        name: key.name.to_string(),
        value: value.to_string(),
        reason: reason.into(),
    }
}

#[cfg(test)]
mod tests {
    use iggy::prelude::{IggyByteSize, MaxTopicSize};

    use super::keys::{
        FLUSH_MESSAGES, FLUSH_MS, MAX_MESSAGE_BYTES, MIN_INSYNC_REPLICAS, RETENTION_BYTES,
        RETENTION_MS, SEGMENT_BYTES,
    };
    use super::*;
    use crate::bridge::TopicDefaults;

    const GIB: u64 = 1024 * 1024 * 1024;
    const MIB: u64 = 1024 * 1024;

    /// The largest batch of a gateway with the default 8 MiB frame.
    const LARGEST_BATCH: i32 = 8 * 1024 * 1024;

    fn topic() -> TopicConfigSnapshot {
        TopicConfigSnapshot {
            partitions_count: 3,
            message_expiry: IggyExpiry::NeverExpire,
            message_expiry_explicit: false,
            max_topic_size: MaxTopicSize::Unlimited,
            max_topic_size_explicit: false,
            segment_size: DEFAULT_SEGMENT_SIZE,
            segment_size_explicit: false,
            replication_factor: None,
            durability: Durability::Replicated,
        }
    }

    /// A topic created with `cleanup.policy=compact`: no expiry and no size cap, both stored.
    fn compacted() -> TopicConfigSnapshot {
        TopicConfigSnapshot {
            message_expiry_explicit: true,
            max_topic_size_explicit: true,
            ..topic()
        }
    }

    fn context(defaults: &TopicDefaults) -> ConfigContext<'_> {
        ConfigContext {
            defaults,
            max_message_bytes: LARGEST_BATCH,
            default_replication_factor: None,
        }
    }

    fn create<'a>(
        configs: impl IntoIterator<Item = (&'a str, Option<&'a str>)>,
    ) -> Result<KafkaTopicSettings, ConfigFault> {
        plan_create(configs, &context(&TopicDefaults::default()), None)
    }

    fn incremental(
        entries: &[(&'static str, i8, Option<&'static str>)],
        topic: &TopicConfigSnapshot,
        defaults: &TopicDefaults,
    ) -> Result<TopicConfigUpdate, ConfigFault> {
        let ops = incremental_ops(entries.iter().copied()).expect("well formed");
        plan_incremental(&ops, topic, &context(defaults))
    }

    fn replace<'a>(
        configs: impl IntoIterator<Item = (&'a str, Option<&'a str>)>,
        topic: &TopicConfigSnapshot,
        defaults: &TopicDefaults,
    ) -> Result<TopicConfigUpdate, ConfigFault> {
        plan_replace(configs, topic, &context(defaults))
    }

    fn size(bytes: u64) -> MaxTopicSize {
        MaxTopicSize::Custom(IggyByteSize::from(bytes))
    }

    const NO_CONFIGS: [(&str, Option<&str>); 0] = [];

    #[test]
    fn given_create_configs_should_map_the_stored_keys_and_accept_reported_defaults() {
        let settings = create([
            (RETENTION_MS, Some("120000")),
            (RETENTION_BYTES, Some("67108864")),
            (SEGMENT_BYTES, Some("67108864")),
            (CLEANUP_POLICY, Some("delete")),
            ("message.timestamp.type", Some("CreateTime")),
            (MIN_INSYNC_REPLICAS, Some("1")),
            (MAX_MESSAGE_BYTES, Some("1048588")),
        ])
        .expect("valid");
        assert_eq!(
            settings.message_expiry,
            Some(expiry_from_retention_ms(120_000))
        );
        assert_eq!(
            settings.retention_bytes,
            Some(RetentionBytes::PerPartition(64 * MIB))
        );
        assert_eq!(settings.segment_size, Some(64 * MIB));
        assert_eq!(settings.durability, None);
        assert_eq!(
            create(NO_CONFIGS).expect("empty"),
            KafkaTopicSettings::default()
        );
    }

    #[test]
    fn given_create_configs_should_accept_every_retention_ms_kafka_accepts() {
        for value in ["-1", "0", "+1", "0001", " 5000 ", "9223372036854775807"] {
            assert!(create([(RETENTION_MS, Some(value))]).is_ok(), "{value}");
        }
        let fault = create([(RETENTION_MS, Some("-2"))]).expect_err("below -1");
        assert_eq!(
            fault.message(),
            "Invalid value -2 for configuration retention.ms: Value must be at least -1"
        );
        let fault = create([(RETENTION_MS, Some("1.5"))]).expect_err("not a long");
        assert_eq!(
            fault.message(),
            "Invalid value 1.5 for configuration retention.ms: Not a number of type LONG"
        );
    }

    #[test]
    fn given_create_configs_should_refuse_only_what_iggy_cannot_apply() {
        for value in ["1048575", "1048577", "2147483647", "2147483648"] {
            assert!(create([(SEGMENT_BYTES, Some(value))]).is_err(), "{value}");
        }
        let fault = create([("message.timestamp.type", Some("LogAppendTime"))]).expect_err("fixed");
        assert_eq!(
            fault.message(),
            "Invalid value LogAppendTime for configuration message.timestamp.type: Iggy keeps the \
             timestamp that the producer sets, so this gateway accepts only CreateTime"
        );
    }

    #[test]
    fn given_a_create_retention_below_one_segment_should_keep_one_segment() {
        let settings = create([(RETENTION_BYTES, Some("104857600"))]).expect("Kafka accepts it");
        assert_eq!(
            settings.retention_bytes,
            Some(RetentionBytes::PerPartition(100 * MIB))
        );
        assert_eq!(settings.retention_below_segment(), Some((100 * MIB, GIB)));
        assert_eq!(
            settings
                .create_options(3, &TopicDefaults::default())
                .max_topic_size,
            Some(size(GIB)),
            "Iggy refuses a topic cap below one segment"
        );
        let zero = create([(RETENTION_BYTES, Some("0"))]).expect("Kafka accepts 0");
        assert_eq!(
            zero.create_options(3, &TopicDefaults::default())
                .max_topic_size,
            Some(size(GIB)),
            "0 keeps the newest segment, and is never no cap"
        );
        assert!(create([(RETENTION_BYTES, Some("-5"))]).is_ok());
        let small_segments = create([
            (RETENTION_BYTES, Some("104857600")),
            (SEGMENT_BYTES, Some("104857600")),
        ])
        .expect("valid");
        assert_eq!(small_segments.retention_below_segment(), None);
    }

    #[test]
    fn given_create_configs_should_accept_keys_iggy_ignores_at_any_value_kafka_accepts() {
        let settings = create([
            (MIN_INSYNC_REPLICAS, Some("2")),
            ("compression.type", Some("zstd")),
            ("segment.ms", Some("3600000")),
            ("min.compaction.lag.ms", Some("86400000")),
            ("unclean.leader.election.enable", Some("true")),
        ])
        .expect("ignored keys");
        assert_eq!(settings, KafkaTopicSettings::default());
        let fault = create([(MIN_INSYNC_REPLICAS, Some("0"))]).expect_err("Kafka refuses 0");
        assert_eq!(
            fault.message(),
            "Invalid value 0 for configuration min.insync.replicas: Value must be at least 1"
        );
    }

    #[test]
    fn given_min_insync_replicas_above_the_majority_should_be_refused() {
        let defaults = TopicDefaults::default();
        let at_factor = |value, factor| {
            plan_create(
                [(MIN_INSYNC_REPLICAS, Some(value))],
                &context(&defaults),
                factor,
            )
        };
        assert!(at_factor("2", Some(3)).is_ok());
        let fault = at_factor(" 3", Some(3)).expect_err("above the majority");
        assert_eq!(
            fault.message(),
            "Invalid value 3 for configuration min.insync.replicas: Iggy commits a write on a \
             majority of the 3 nodes that store each partition, so this gateway accepts at most 2"
        );
        assert!(at_factor("3", None).is_ok(), "the factor is not known");

        let stored = TopicConfigSnapshot {
            replication_factor: Some(5),
            ..topic()
        };
        assert!(incremental(&[(MIN_INSYNC_REPLICAS, 0, Some("3"))], &stored, &defaults).is_ok());
        assert!(matches!(
            incremental(&[(MIN_INSYNC_REPLICAS, 0, Some("4"))], &stored, &defaults),
            Err(ConfigFault::InvalidValue { .. })
        ));
        let one_node = ConfigContext {
            default_replication_factor: Some(1),
            ..context(&defaults)
        };
        let ops = incremental_ops([(MIN_INSYNC_REPLICAS, 0, Some("2"))]).expect("well formed");
        assert!(
            plan_incremental(&ops, &topic(), &one_node).is_err(),
            "a topic without a stored factor has the default"
        );
    }

    #[test]
    fn given_a_flush_of_every_write_should_persist_a_new_topic_and_be_refused_on_an_alter() {
        for (key, value) in [(FLUSH_MESSAGES, "1"), (FLUSH_MS, "0")] {
            let settings = create([(key, Some(value))]).expect("valid");
            assert_eq!(settings.durability, Some(Durability::Persisted), "{key}");
        }
        for (key, value) in [(FLUSH_MESSAGES, "1000"), (FLUSH_MS, "1000")] {
            let settings = create([(key, Some(value))]).expect("valid");
            assert_eq!(settings.durability, None, "{key} is not every write");
        }
        assert!(
            create([(FLUSH_MESSAGES, Some("0"))]).is_err(),
            "Kafka refuses 0"
        );

        let defaults = TopicDefaults::default();
        let fault = incremental(&[(FLUSH_MESSAGES, 0, Some("1"))], &topic(), &defaults)
            .expect_err("a replicated topic");
        assert_eq!(
            fault.message(),
            "Invalid value 1 for configuration flush.messages: Iggy sets durability only when it \
             creates a topic, and this topic does not flush every write"
        );
        let persisted = TopicConfigSnapshot {
            durability: Durability::Persisted,
            ..topic()
        };
        assert!(
            incremental(&[(FLUSH_MS, 0, Some("0"))], &persisted, &defaults)
                .expect("it flushes every write already")
                .is_empty()
        );
        assert!(
            incremental(&[(FLUSH_MS, 0, Some("5000"))], &topic(), &defaults)
                .expect("accepted and not applied")
                .is_empty()
        );
    }

    #[test]
    fn given_max_message_bytes_should_take_kafkas_default_up_to_the_largest_batch() {
        for value in ["1048588", "2097152", "8388608"] {
            assert!(
                create([(MAX_MESSAGE_BYTES, Some(value))]).is_ok(),
                "{value}"
            );
        }
        let fault =
            create([(MAX_MESSAGE_BYTES, Some("8388609"))]).expect_err("above the largest batch");
        assert_eq!(
            fault.message(),
            "Invalid value 8388609 for configuration max.message.bytes: this gateway takes \
             record batches of up to 8388608 bytes in every topic, so it accepts 1048588 to \
             8388608"
        );
        assert!(
            create([(MAX_MESSAGE_BYTES, Some("1000"))]).is_err(),
            "the gateway takes larger batches than that"
        );
        let defaults = TopicDefaults::default();
        assert!(
            incremental(
                &[(MAX_MESSAGE_BYTES, 0, Some("4194304"))],
                &topic(),
                &defaults
            )
            .expect("accepted")
            .is_empty()
        );
    }

    #[test]
    fn given_a_create_policy_without_delete_should_keep_every_record() {
        let compact = create([
            (CLEANUP_POLICY, Some("compact")),
            (RETENTION_MS, Some("1000")),
            (RETENTION_BYTES, Some("1048576")),
        ])
        .expect("retention is ignored");
        assert_eq!(
            compact.cleanup_policy,
            CleanupPolicy {
                delete: false,
                compact: true
            }
        );
        assert!(compact.cleanup_policy.keeps_every_record());
        assert_eq!(compact.retention_below_segment(), None);
        let empty = create([(CLEANUP_POLICY, Some(""))]).expect("Kafka 4.4 accepts it");
        assert!(empty.cleanup_policy.keeps_every_record());
        assert!(!empty.cleanup_policy.compact);

        let both = create([(CLEANUP_POLICY, Some("delete,compact"))]).expect("both");
        assert!(!both.cleanup_policy.keeps_every_record());
        assert!(both.cleanup_policy.compact);
        let small = create([
            (CLEANUP_POLICY, Some("compact,delete")),
            (RETENTION_BYTES, Some("1048576")),
        ])
        .expect("delete applies retention, kept at one segment");
        assert_eq!(small.retention_below_segment(), Some((MIB, GIB)));

        let fault = create([(CLEANUP_POLICY, Some("compact,"))]).expect_err("empty item");
        assert_eq!(
            fault.message(),
            "Configuration 'cleanup.policy' values must not be empty."
        );
        let fault = create([(CLEANUP_POLICY, Some("Delete"))]).expect_err("case");
        assert_eq!(
            fault.message(),
            "Invalid value Delete for configuration cleanup.policy: String must be one of: \
             compact, delete"
        );
    }

    #[test]
    fn given_create_configs_should_name_an_unknown_key_and_every_null_value() {
        let fault = create([("no.such", Some("1"))]).expect_err("unknown");
        assert_eq!(fault.message(), "Unknown topic config name: no.such");
        let fault = create([("message.format.version", Some("3.0"))]).expect_err("removed");
        assert_eq!(
            fault,
            ConfigFault::UnknownKey("message.format.version".to_string())
        );
        let fault = create([
            (RETENTION_MS, None),
            ("no.such", Some("1")),
            (SEGMENT_BYTES, None),
        ])
        .expect_err("nulls first");
        assert_eq!(
            fault.message(),
            "Null value not supported for topic configs: retention.ms,segment.bytes"
        );
    }

    #[test]
    fn given_a_repeated_create_config_should_keep_its_last_value() {
        let settings = create([(RETENTION_MS, Some("-2")), (RETENTION_MS, Some("1000"))])
            .expect("the last value counts");
        assert_eq!(
            settings.message_expiry,
            Some(expiry_from_retention_ms(1_000))
        );
    }

    #[test]
    fn given_repeated_names_should_keep_each_once_where_it_last_appears() {
        assert_eq!(
            last_of_each_name([
                ("a", Some("1")),
                ("b", Some("2")),
                ("a", Some("3")),
                ("c", None),
            ]),
            [("b", Some("2")), ("a", Some("3")), ("c", None)]
        );
        let names: Vec<String> = (0..4096).map(|name| format!("name.{name}")).collect();
        let configs = names
            .iter()
            .map(|name| (name.as_str(), Some("first")))
            .chain(names.iter().map(|name| (name.as_str(), Some("last"))));
        let kept = last_of_each_name(configs);
        assert_eq!(kept.len(), names.len());
        assert!(kept.iter().all(|(_, value)| *value == Some("last")));
        assert_eq!(kept[0].0, "name.0");
    }

    #[test]
    fn given_incremental_sets_should_write_the_stored_keys() {
        let update = incremental(
            &[
                (RETENTION_MS, 0, Some("1500")),
                (RETENTION_BYTES, 0, Some("1073741824")),
            ],
            &topic(),
            &TopicDefaults::default(),
        )
        .expect("valid");
        assert_eq!(
            update.message_expiry,
            Some(SettingChange::Set(expiry_from_retention_ms(1_500)))
        );
        assert_eq!(
            update.max_topic_size,
            Some(SettingChange::Set(size(3 * GIB)))
        );
        assert_eq!(update.retention_below_segment, None);

        let small = incremental(
            &[(RETENTION_BYTES, 0, Some("1048576"))],
            &topic(),
            &TopicDefaults::default(),
        )
        .expect("Kafka accepts a cap below one segment");
        assert_eq!(
            small.max_topic_size,
            Some(SettingChange::Set(size(GIB))),
            "Iggy refuses a topic cap below one segment"
        );
        assert_eq!(small.retention_below_segment, Some((MIB, GIB)));
    }

    #[test]
    fn given_an_incremental_retention_bytes_of_zero_should_keep_one_segment_and_not_lift_the_cap() {
        let zero = incremental(
            &[(RETENTION_BYTES, 0, Some("0"))],
            &topic(),
            &TopicDefaults::default(),
        )
        .expect("Kafka accepts 0");
        assert_eq!(zero.max_topic_size, Some(SettingChange::Set(size(GIB))));
        let replaced = replace(
            [(RETENTION_BYTES, Some("0"))],
            &topic(),
            &TopicDefaults::default(),
        )
        .expect("valid");
        assert_eq!(replaced.max_topic_size, Some(SettingChange::Set(size(GIB))));
    }

    #[test]
    fn given_incremental_sets_of_other_keys_should_write_nothing_or_be_refused() {
        let same = incremental(
            &[
                (SEGMENT_BYTES, 0, Some("1073741824")),
                (CLEANUP_POLICY, 0, Some("delete")),
                (MIN_INSYNC_REPLICAS, 0, Some("2")),
                ("message.timestamp.type", 0, Some("CreateTime")),
            ],
            &topic(),
            &TopicDefaults::default(),
        )
        .expect("no change");
        assert!(same.is_empty());
        assert_eq!(same.cleanup_policy, Some(CleanupPolicy::default()));
        let timestamps = incremental(
            &[("message.timestamp.type", 0, Some("LogAppendTime"))],
            &topic(),
            &TopicDefaults::default(),
        )
        .expect_err("fixed");
        assert!(matches!(timestamps, ConfigFault::InvalidValue { .. }));

        let segment = incremental(
            &[(SEGMENT_BYTES, 0, Some("67108864"))],
            &topic(),
            &TopicDefaults::default(),
        )
        .expect_err("fixed at create");
        assert_eq!(
            segment.message(),
            "Invalid value 67108864 for configuration segment.bytes: Iggy sets segment.bytes only \
             when it creates a topic, and this topic has 1073741824"
        );
        let unknown = incremental(
            &[("no.such", 0, Some("1"))],
            &topic(),
            &TopicDefaults::default(),
        )
        .expect_err("unknown");
        assert_eq!(unknown, ConfigFault::UnknownKey("no.such".to_string()));
    }

    #[test]
    fn given_incremental_deletes_should_go_back_to_the_iggy_default_without_a_gateway_default() {
        let unset = incremental(
            &[
                (RETENTION_MS, 1, None),
                (RETENTION_BYTES, 1, None),
                ("no.such", 1, None),
            ],
            &topic(),
            &TopicDefaults::default(),
        )
        .expect("valid");
        assert!(unset.is_empty(), "nothing was set, so nothing is written");

        let set = TopicConfigSnapshot {
            message_expiry: expiry_from_retention_ms(1_000),
            message_expiry_explicit: true,
            max_topic_size: size(3 * GIB),
            max_topic_size_explicit: true,
            ..topic()
        };
        let reset = incremental(
            &[(RETENTION_MS, 1, Some("")), (RETENTION_BYTES, 1, None)],
            &set,
            &TopicDefaults::default(),
        )
        .expect("valid");
        assert_eq!(reset.message_expiry, Some(SettingChange::Reset));
        assert_eq!(reset.max_topic_size, Some(SettingChange::Reset));
    }

    #[test]
    fn given_incremental_deletes_should_go_back_to_the_gateway_default_only_for_a_set_value() {
        let defaults = TopicDefaults {
            retention_ms: Some(604_800_000),
            retention_bytes: Some(2 * GIB),
            ..TopicDefaults::default()
        };
        let unset = incremental(
            &[(RETENTION_MS, 1, None), (RETENTION_BYTES, 1, None)],
            &topic(),
            &defaults,
        )
        .expect("valid");
        assert!(
            unset.is_empty(),
            "no client set them, and Kafka changes nothing for such a delete"
        );
        assert!(
            replace(NO_CONFIGS, &topic(), &defaults)
                .expect("valid")
                .is_empty(),
            "a replace that leaves them out does not write the default either"
        );

        let set = TopicConfigSnapshot {
            message_expiry: expiry_from_retention_ms(1_000),
            message_expiry_explicit: true,
            max_topic_size: size(3 * GIB),
            max_topic_size_explicit: true,
            ..topic()
        };
        let update = incremental(
            &[(RETENTION_MS, 1, None), (RETENTION_BYTES, 1, None)],
            &set,
            &defaults,
        )
        .expect("valid");
        assert_eq!(
            update.message_expiry,
            Some(SettingChange::Set(expiry_from_retention_ms(604_800_000)))
        );
        assert_eq!(
            update.max_topic_size,
            Some(SettingChange::Set(size(6 * GIB)))
        );

        let at_default = TopicConfigSnapshot {
            message_expiry: expiry_from_retention_ms(604_800_000),
            max_topic_size: size(6 * GIB),
            ..set
        };
        let update = incremental(
            &[(RETENTION_MS, 1, None), (RETENTION_BYTES, 1, None)],
            &at_default,
            &defaults,
        )
        .expect("valid");
        assert!(update.is_empty());
    }

    #[test]
    fn given_a_value_no_client_set_should_not_go_back_to_a_default() {
        assert_eq!(back_to_default(Some(7), 0, false), None);
        assert_eq!(back_to_default(None::<i32>, 0, false), None);
        assert_eq!(back_to_default(Some(7), 7, true), None);
        assert_eq!(
            back_to_default(Some(7), 3, true),
            Some(SettingChange::Set(7))
        );
        assert_eq!(back_to_default(None, 3, true), Some(SettingChange::Reset));
    }

    #[test]
    fn given_a_delete_of_a_segment_size_set_at_create_should_be_refused() {
        let created = TopicConfigSnapshot {
            segment_size: 64 * MIB,
            segment_size_explicit: true,
            ..topic()
        };
        let fault = incremental(
            &[(SEGMENT_BYTES, 1, None)],
            &created,
            &TopicDefaults::default(),
        )
        .expect_err("cannot reset");
        assert_eq!(
            fault.message(),
            "segment.bytes cannot go back to its default, because Iggy sets it only when it \
             creates a topic. This topic has 67108864."
        );
        let at_default = TopicConfigSnapshot {
            segment_size_explicit: true,
            ..topic()
        };
        assert!(
            incremental(
                &[(SEGMENT_BYTES, 1, None)],
                &at_default,
                &TopicDefaults::default()
            )
            .expect("already the default")
            .is_empty()
        );
    }

    #[test]
    fn given_append_and_subtract_should_apply_to_the_default_list_or_be_refused() {
        let expiring = TopicConfigSnapshot {
            message_expiry: expiry_from_retention_ms(1_000),
            message_expiry_explicit: true,
            max_topic_size: size(3 * GIB),
            max_topic_size_explicit: true,
            ..topic()
        };
        let both = incremental(
            &[(CLEANUP_POLICY, 2, Some("compact"))],
            &expiring,
            &TopicDefaults::default(),
        )
        .expect("compact,delete");
        assert!(both.is_empty(), "delete still applies retention");
        assert!(both.cleanup_policy.is_some_and(|policy| policy.compact));
        let emptied = incremental(
            &[(CLEANUP_POLICY, 3, Some("delete"))],
            &expiring,
            &TopicDefaults::default(),
        )
        .expect("an empty list keeps every record");
        assert_eq!(
            emptied.message_expiry,
            Some(SettingChange::Set(IggyExpiry::NeverExpire))
        );
        assert_eq!(
            emptied.max_topic_size,
            Some(SettingChange::Set(MaxTopicSize::Unlimited))
        );
        let throttled = incremental(
            &[("leader.replication.throttled.replicas", 2, Some("0:1"))],
            &topic(),
            &TopicDefaults::default(),
        )
        .expect("ignored");
        assert!(throttled.is_empty());

        let not_a_list = incremental(
            &[(RETENTION_MS, 2, Some("1"))],
            &topic(),
            &TopicDefaults::default(),
        )
        .expect_err("not a list");
        assert_eq!(
            not_a_list.message(),
            "Can't APPEND to key retention.ms because its type is not LIST."
        );
        let unknown = incremental(
            &[("no.such", 3, Some("1"))],
            &topic(),
            &TopicDefaults::default(),
        )
        .expect_err("not a list");
        assert_eq!(
            unknown.message(),
            "Can't SUBTRACT to key no.such because its type is not LIST."
        );
    }

    #[test]
    fn given_a_replace_should_reset_each_stored_key_it_leaves_out() {
        let set = TopicConfigSnapshot {
            message_expiry: expiry_from_retention_ms(1_000),
            message_expiry_explicit: true,
            max_topic_size: size(3 * GIB),
            max_topic_size_explicit: true,
            ..topic()
        };
        let update = replace(
            [(RETENTION_MS, Some("2000"))],
            &set,
            &TopicDefaults::default(),
        )
        .expect("valid");
        assert_eq!(
            update.message_expiry,
            Some(SettingChange::Set(expiry_from_retention_ms(2_000)))
        );
        assert_eq!(update.max_topic_size, Some(SettingChange::Reset));

        let everything =
            replace(NO_CONFIGS, &set, &TopicDefaults::default()).expect("an empty list resets");
        assert_eq!(everything.message_expiry, Some(SettingChange::Reset));
        assert_eq!(everything.max_topic_size, Some(SettingChange::Reset));
        assert_eq!(everything.kept_segment_size, None, "the default size");
    }

    #[test]
    fn given_a_replace_that_leaves_out_a_segment_size_set_at_create_should_keep_it() {
        let created = TopicConfigSnapshot {
            segment_size: 64 * MIB,
            segment_size_explicit: true,
            ..topic()
        };
        let update = replace(
            [(RETENTION_MS, Some("1"))],
            &created,
            &TopicDefaults::default(),
        )
        .expect("Iggy cannot reset the size, so the replace keeps it");
        assert_eq!(update.kept_segment_size, Some(64 * MIB));
        assert_eq!(
            update.message_expiry,
            Some(SettingChange::Set(expiry_from_retention_ms(1)))
        );
        let named = replace(
            [(SEGMENT_BYTES, Some("67108864"))],
            &created,
            &TopicDefaults::default(),
        )
        .expect("valid");
        assert_eq!(named.kept_segment_size, None);
        assert!(
            replace(
                [(SEGMENT_BYTES, Some("1073741824"))],
                &created,
                &TopicDefaults::default()
            )
            .is_err(),
            "a named size still has to be the current one"
        );
    }

    #[test]
    fn given_an_alter_to_compact_should_keep_every_record_over_its_retention_keys() {
        let expiring = TopicConfigSnapshot {
            message_expiry: expiry_from_retention_ms(1_000),
            message_expiry_explicit: true,
            max_topic_size: size(3 * GIB),
            max_topic_size_explicit: true,
            ..topic()
        };
        let incremental_compact = incremental(
            &[
                (RETENTION_MS, 0, Some("5000")),
                (CLEANUP_POLICY, 0, Some("compact")),
                (RETENTION_BYTES, 0, Some("1048576")),
            ],
            &expiring,
            &TopicDefaults::default(),
        )
        .expect("compact wins over the retention keys");
        assert_eq!(
            incremental_compact.message_expiry,
            Some(SettingChange::Set(IggyExpiry::NeverExpire))
        );
        assert_eq!(
            incremental_compact.max_topic_size,
            Some(SettingChange::Set(MaxTopicSize::Unlimited))
        );
        assert_eq!(incremental_compact.retention_below_segment, None);

        let defaults = TopicDefaults {
            retention_ms: Some(604_800_000),
            ..TopicDefaults::default()
        };
        let replace_compact =
            replace([(CLEANUP_POLICY, Some("compact"))], &expiring, &defaults).expect("valid");
        assert_eq!(
            replace_compact.message_expiry,
            Some(SettingChange::Set(IggyExpiry::NeverExpire))
        );
        assert_eq!(
            replace_compact.max_topic_size,
            Some(SettingChange::Set(MaxTopicSize::Unlimited))
        );

        let again =
            replace([(CLEANUP_POLICY, Some("compact"))], &compacted(), &defaults).expect("valid");
        assert!(again.is_empty(), "the topic keeps every record already");
        let back_to_delete =
            replace([(CLEANUP_POLICY, Some("delete"))], &compacted(), &defaults).expect("valid");
        assert_eq!(
            back_to_delete.message_expiry,
            Some(SettingChange::Set(expiry_from_retention_ms(604_800_000))),
            "delete takes the retention the replace leaves, here the gateway default"
        );
    }

    #[test]
    fn given_a_topic_that_keeps_every_record_when_an_alter_leaves_the_policy_out_should_keep_it() {
        let defaults = TopicDefaults {
            retention_ms: Some(604_800_000),
            ..TopicDefaults::default()
        };
        let fault = incremental(&[(RETENTION_MS, 0, Some("1000"))], &compacted(), &defaults)
            .expect_err("it would remove records");
        assert_eq!(fault, ConfigFault::KeepsEveryRecord { name: RETENTION_MS });
        assert_eq!(
            fault.message(),
            "This topic keeps every record, as a topic with cleanup.policy=compact does. The \
             gateway does not store cleanup.policy, so it applies retention.ms only when the \
             same request names cleanup.policy."
        );
        assert!(
            incremental(
                &[(RETENTION_BYTES, 0, Some("1073741824"))],
                &compacted(),
                &defaults
            )
            .is_err()
        );
        assert!(
            incremental(
                &[
                    (RETENTION_MS, 0, Some("-1")),
                    (RETENTION_BYTES, 0, Some("-1"))
                ],
                &compacted(),
                &defaults
            )
            .is_ok(),
            "-1 removes nothing"
        );
        assert!(
            incremental(
                &[(RETENTION_MS, 1, None), (RETENTION_BYTES, 1, None)],
                &compacted(),
                &defaults
            )
            .expect("a reset to the default, as kafka-ui sends")
            .is_empty(),
            "the topic still keeps every record"
        );
        assert!(
            replace(NO_CONFIGS, &compacted(), &defaults)
                .expect("valid")
                .is_empty()
        );
        assert!(replace([(RETENTION_MS, Some("1000"))], &compacted(), &defaults).is_err());

        let named = incremental(
            &[
                (RETENTION_MS, 0, Some("1000")),
                (CLEANUP_POLICY, 0, Some("delete")),
            ],
            &compacted(),
            &defaults,
        )
        .expect("the request names the policy");
        assert_eq!(
            named.message_expiry,
            Some(SettingChange::Set(expiry_from_retention_ms(1_000)))
        );
        let replaced = replace(
            [
                (CLEANUP_POLICY, Some("delete")),
                (RETENTION_MS, Some("1000")),
            ],
            &compacted(),
            &defaults,
        )
        .expect("the request names the policy");
        assert_eq!(
            replaced.message_expiry,
            Some(SettingChange::Set(expiry_from_retention_ms(1_000)))
        );
        assert!(
            incremental(&[(RETENTION_MS, 0, Some("1000"))], &topic(), &defaults).is_ok(),
            "Iggy's own defaults are not a client asking to keep every record"
        );
    }

    #[test]
    fn given_a_replace_should_refuse_a_repeated_or_unknown_key() {
        let fault = replace(
            [(RETENTION_MS, Some("1000")), (RETENTION_MS, Some("1000"))],
            &topic(),
            &TopicDefaults::default(),
        )
        .expect_err("repeated");
        assert_eq!(fault, ConfigFault::Repeated(RETENTION_MS.to_string()));
        let fault = replace(
            [("retention.minutes", Some("2"))],
            &topic(),
            &TopicDefaults::default(),
        )
        .expect_err("not a topic key");
        assert_eq!(
            fault,
            ConfigFault::UnknownKey("retention.minutes".to_string())
        );
    }

    #[test]
    fn given_broker_resources_should_count_toward_the_describe_cap() {
        let resource = |resource_type, name: String| {
            DescribeConfigsResource::default()
                .with_resource_type(resource_type)
                .with_resource_name(StrBytes::from(name))
        };
        let brokers: Vec<DescribeConfigsResource> = (0..=MAX_CONFIG_TOPICS)
            .map(|_| resource(RESOURCE_TYPE_BROKER, "1".to_string()))
            .collect();
        assert!(
            exceeds_describe_cap(&brokers),
            "a repeated broker counts each time"
        );
        assert!(!exceeds_describe_cap(&brokers[1..]));
        let topics: Vec<DescribeConfigsResource> = (0..MAX_CONFIG_TOPICS)
            .map(|topic| resource(RESOURCE_TYPE_TOPIC, format!("topic-{topic}")))
            .chain([resource(RESOURCE_TYPE_TOPIC, "topic-0".to_string())])
            .collect();
        assert!(
            !exceeds_describe_cap(&topics),
            "a repeated topic counts once"
        );
        let mut mixed = topics;
        mixed.push(resource(RESOURCE_TYPE_BROKER, "1".to_string()));
        assert!(exceeds_describe_cap(&mixed));
    }

    fn no_replan(_: &TopicConfigSnapshot) -> Result<TopicConfigUpdate, ConfigFault> {
        panic!("only a size cap is planned again, and only under the lock")
    }

    #[tokio::test]
    async fn given_a_passed_deadline_when_writing_should_answer_timed_out_without_calling_iggy() {
        let bridge = IggyBridge::offline();
        let update = TopicConfigUpdate {
            message_expiry: Some(SettingChange::Set(IggyExpiry::NeverExpire)),
            ..TopicConfigUpdate::default()
        };
        let answer = write_alter(
            &bridge,
            &Mutex::new(()),
            "orders",
            update,
            no_replan,
            Instant::now(),
            "AlterConfigs",
        )
        .await;
        // A write started on the offline bridge fails fast as NOT_LEADER_OR_FOLLOWER instead.
        assert_eq!(answer, (ERROR_REQUEST_TIMED_OUT, None));
    }

    #[tokio::test(start_paused = true)]
    async fn given_a_size_cap_while_a_grow_holds_the_lock_should_write_nothing_by_the_deadline() {
        let bridge = IggyBridge::offline();
        let lock = Mutex::new(());
        let _grow = lock.lock().await;
        let update = TopicConfigUpdate {
            max_topic_size: Some(SettingChange::Set(size(GIB))),
            ..TopicConfigUpdate::default()
        };
        let answer = write_alter(
            &bridge,
            &lock,
            "orders",
            update,
            no_replan,
            Instant::now() + std::time::Duration::from_secs(1),
            "IncrementalAlterConfigs",
        )
        .await;
        assert_eq!(answer, (ERROR_REQUEST_TIMED_OUT, None));
    }

    #[tokio::test]
    async fn given_a_size_cap_should_read_the_topic_again_before_the_write() {
        let bridge = IggyBridge::offline();
        let update = TopicConfigUpdate {
            max_topic_size: Some(SettingChange::Set(size(GIB))),
            ..TopicConfigUpdate::default()
        };
        let (code, _) = write_alter(
            &bridge,
            &Mutex::new(()),
            "orders",
            update,
            no_replan,
            Instant::now() + std::time::Duration::from_secs(5),
            "AlterConfigs",
        )
        .await;
        assert_ne!(
            code, ERROR_NONE,
            "the read fails on the offline bridge, so nothing is written"
        );
    }

    #[test]
    fn given_alter_null_values_should_name_them_as_kafka_does() {
        assert_eq!(null_values([(RETENTION_MS, Some("1"))]), None);
        let fault = null_values([(RETENTION_MS, None), (RETENTION_BYTES, None)]).expect("nulls");
        assert_eq!(
            fault.message(),
            "Null value not supported for : retention.ms, retention.bytes"
        );
    }

    #[test]
    fn find_duplicate_names_finds_a_name_repeated_across_two_resources() {
        let duplicates = find_duplicate_names(["orders", "payments", "orders"]);
        assert_eq!(duplicates, HashSet::from(["orders"]));
    }

    #[test]
    fn find_duplicate_names_is_empty_when_every_name_is_unique() {
        assert!(find_duplicate_names(["orders", "payments"]).is_empty());
    }

    #[test]
    fn incremental_ops_rejects_a_repeated_name_before_a_null_value() {
        let err = incremental_ops([(RETENTION_MS, 0, Some("1")), (RETENTION_MS, 0, None)])
            .expect_err("repeated");
        assert_eq!(err, RequestFault::DuplicateKeys);
        assert_eq!(err.message(), "Error due to duplicate config keys");
    }

    #[test]
    fn incremental_ops_names_every_null_value_except_a_delete() {
        let err = incremental_ops([
            (RETENTION_MS, 0, None),
            (CLEANUP_POLICY, 1, None),
            ("no.such", 9, None),
        ])
        .expect_err("null values");
        assert_eq!(
            err,
            RequestFault::NullValues(vec![RETENTION_MS.to_string(), "no.such".to_string()])
        );
        assert_eq!(
            err.message(),
            "Null value not supported for : retention.ms, no.such"
        );
    }

    #[test]
    fn incremental_ops_rejects_an_unknown_operation_byte() {
        let err = incremental_ops([(RETENTION_MS, 4, Some("1"))]).expect_err("unknown op");
        assert_eq!(err, RequestFault::UnknownOperation(4));
        assert_eq!(err.message(), "Unknown operations type 4");
        let err = incremental_ops([(RETENTION_MS, -1, Some("1"))]).expect_err("negative op");
        assert_eq!(err, RequestFault::UnknownOperation(-1));
        assert_eq!(err.message(), "Unknown operations type -1");
    }

    #[test]
    fn incremental_ops_keeps_request_order_and_a_delete_value() {
        let ops = incremental_ops([(RETENTION_MS, 1, Some("")), ("no.such", 0, Some("1"))])
            .expect("well formed");
        assert_eq!(
            ops,
            vec![
                ConfigOp {
                    name: RETENTION_MS,
                    operation: ConfigOperation::Delete,
                    value: Some(""),
                },
                ConfigOp {
                    name: "no.such",
                    operation: ConfigOperation::Set,
                    value: Some("1"),
                },
            ]
        );
    }
}
