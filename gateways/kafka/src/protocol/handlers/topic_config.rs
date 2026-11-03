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

use std::collections::HashSet;

use iggy::prelude::{IggyExpiry, MaxTopicSize};
use iggy_common::{DEFAULT_SEGMENT_SIZE, MAX_TOPIC_SEGMENT_SIZE, MIN_TOPIC_SEGMENT_SIZE};
use kafka_protocol::messages::alter_configs_request::AlterConfigsResource;
use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
use kafka_protocol::messages::incremental_alter_configs_request::AlterConfigsResource as IncrementalAlterConfigsResource;
use kafka_protocol::protocol::StrBytes;

use crate::auth::AuthenticatedPrincipal;
use crate::bridge::topic_settings::expiry_from_retention_ms;
use crate::bridge::{
    BridgeError, CleanupPolicy, KafkaTopicSettings, RetentionBytes, SettingChange,
    TopicConfigSnapshot, TopicConfigUpdate, TopicDefaults,
};
use keys::{ConfigType, ConfigValue, Refusal, TOPIC_KEYS, TopicKey, TopicKeyKind, topic_key};

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

/// The settings that the configuration of one `CreateTopics` topic names.
///
/// A name that repeats keeps its last value, as Kafka's `translateCreationConfigs` does. Each
/// value is checked as Kafka checks it, and a fixed key must name the value the gateway reports.
/// Unless the policy keeps every record, `retention.bytes` must keep at least one segment in each
/// partition, because Iggy never trims a partition below its newest sealed segment.
///
/// # Errors
///
/// Returns every null value at once, as Kafka does, otherwise the first [`ConfigFault`] in
/// request order.
pub fn plan_create<'a>(
    configs: impl IntoIterator<Item = (&'a str, Option<&'a str>)>,
) -> Result<KafkaTopicSettings, ConfigFault> {
    let mut named: Vec<(&str, Option<&str>)> = Vec::new();
    for (name, value) in configs {
        named.retain(|(seen, _)| *seen != name);
        named.push((name, value));
    }
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
            TopicKeyKind::Ignored | TopicKeyKind::Fixed { .. } => {
                key.check(value).map_err(|refusal| refused(key, refusal))?;
            }
        }
    }
    if !settings.cleanup_policy.keeps_every_record()
        && let Some(retention) = settings.retention_bytes
    {
        keeps_a_segment(
            retention,
            settings.segment_size.unwrap_or(DEFAULT_SEGMENT_SIZE),
        )?;
    }
    Ok(settings)
}

/// What one `AlterConfigs` resource writes.
///
/// Kafka's `AlterConfigs` replaces the whole configuration of a topic, so each stored key the
/// resource leaves out goes back to its default: the gateway default where one is set, else the
/// Iggy default. `segment.bytes` cannot go back once Iggy created the topic with another size. A
/// `cleanup.policy` without `delete` keeps every record, whatever the retention keys say.
///
/// # Errors
///
/// Returns the first [`ConfigFault`]: the named keys in request order, then the keys left out.
pub fn plan_replace<'a>(
    configs: impl IntoIterator<Item = (&'a str, Option<&'a str>)>,
    topic: &TopicConfigSnapshot,
    defaults: &TopicDefaults,
) -> Result<TopicConfigUpdate, ConfigFault> {
    let mut named = HashSet::new();
    let mut update = TopicConfigUpdate::default();
    for (name, value) in configs {
        if !named.insert(name) {
            return Err(ConfigFault::Repeated(name.to_string()));
        }
        let key = topic_key(name).ok_or_else(|| ConfigFault::UnknownKey(name.to_string()))?;
        plan_set(&mut update, key, value.unwrap_or_default(), topic)?;
    }
    for key in TOPIC_KEYS.iter().filter(|key| !named.contains(key.name)) {
        plan_delete(&mut update, key, topic, defaults)?;
    }
    settle_retention(&mut update, topic)?;
    Ok(update)
}

/// What one `IncrementalAlterConfigs` resource writes.
///
/// `SET` writes a stored key, and checks any other key as [`plan_create`] does. `DELETE` takes a
/// stored key back to its default, as [`plan_replace`] does for a key left out, and changes
/// nothing for any other key. `APPEND` and `SUBTRACT` apply to a list key from the value the
/// gateway reports, which for `cleanup.policy` is `delete`, and then check the list as a `SET`
/// does. On any other key they are refused with Kafka's own text, a key Kafka does not know among
/// them.
///
/// The gateway does not store `cleanup.policy`, so a retention change that names no policy
/// applies even to a topic created with `compact`.
///
/// # Errors
///
/// Returns the first [`ConfigFault`] in request order.
pub fn plan_incremental(
    ops: &[ConfigOp<'_>],
    topic: &TopicConfigSnapshot,
    defaults: &TopicDefaults,
) -> Result<TopicConfigUpdate, ConfigFault> {
    let mut update = TopicConfigUpdate::default();
    for op in ops {
        let key = topic_key(op.name);
        match op.operation {
            ConfigOperation::Set => {
                let key = key.ok_or_else(|| ConfigFault::UnknownKey(op.name.to_string()))?;
                plan_set(&mut update, key, op.value.unwrap_or_default(), topic)?;
            }
            ConfigOperation::Delete => {
                if let Some(key) = key {
                    plan_delete(&mut update, key, topic, defaults)?;
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
                plan_set(&mut update, key, &list, topic)?;
            }
        }
    }
    settle_retention(&mut update, topic)?;
    Ok(update)
}

/// A `SET` of `key` to `value` on an existing topic.
fn plan_set(
    update: &mut TopicConfigUpdate,
    key: &TopicKey,
    value: &str,
    topic: &TopicConfigSnapshot,
) -> Result<(), ConfigFault> {
    match key.kind {
        TopicKeyKind::RetentionMs => {
            update.message_expiry = Some(SettingChange::Set(retention_ms(key, value)?));
        }
        TopicKeyKind::RetentionBytes => {
            let retention = retention_bytes(key, value)?;
            update.max_topic_size = Some(SettingChange::Set(
                retention.topic_size(topic.partitions_count),
            ));
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
        TopicKeyKind::Ignored | TopicKeyKind::Fixed { .. } => {
            key.check(value).map_err(|refusal| refused(key, refusal))?;
        }
    }
    Ok(())
}

/// A `DELETE` of `key` on an existing topic: back to the gateway default where one is set, else
/// the Iggy default. Nothing is written when the topic already has that value.
fn plan_delete(
    update: &mut TopicConfigUpdate,
    key: &TopicKey,
    topic: &TopicConfigSnapshot,
    defaults: &TopicDefaults,
) -> Result<(), ConfigFault> {
    match key.kind {
        TopicKeyKind::RetentionMs => {
            update.message_expiry = back_to_default(
                defaults.expiry(),
                topic.message_expiry,
                topic.message_expiry_explicit,
            );
        }
        TopicKeyKind::RetentionBytes => {
            update.max_topic_size = back_to_default(
                defaults
                    .retention()
                    .map(|retention| retention.topic_size(topic.partitions_count)),
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
        TopicKeyKind::Ignored | TopicKeyKind::Fixed { .. } => {}
    }
    Ok(())
}

/// The retention an alter leaves, once every key of it is planned. A policy without `delete`
/// keeps every record, so it writes no expiry and no size cap in place of what the retention keys
/// say. Otherwise a new size cap must keep one segment in each partition.
fn settle_retention(
    update: &mut TopicConfigUpdate,
    topic: &TopicConfigSnapshot,
) -> Result<(), ConfigFault> {
    if update
        .cleanup_policy
        .is_some_and(CleanupPolicy::keeps_every_record)
    {
        update.message_expiry = (topic.message_expiry != IggyExpiry::NeverExpire)
            .then_some(SettingChange::Set(IggyExpiry::NeverExpire));
        update.max_topic_size = (topic.max_topic_size != MaxTopicSize::Unlimited)
            .then_some(SettingChange::Set(MaxTopicSize::Unlimited));
        return Ok(());
    }
    if let Some(SettingChange::Set(size)) = update.max_topic_size {
        keeps_a_segment(
            RetentionBytes::share_of(size, topic.partitions_count),
            topic.segment_size,
        )?;
    }
    Ok(())
}

/// The write that takes a stored value back to `gateway_default`, or to the Iggy default when
/// none is set, or `None` when the value is there already.
fn back_to_default<T: PartialEq + Copy>(
    gateway_default: Option<T>,
    current: T,
    explicit: bool,
) -> Option<SettingChange<T>> {
    match gateway_default {
        Some(value) if explicit && current == value => None,
        Some(value) => Some(SettingChange::Set(value)),
        None => explicit.then_some(SettingChange::Reset),
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

/// `retention.bytes`: any count, and a negative one caps nothing, as in Kafka.
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

/// Iggy keeps the newest sealed segment of a partition whatever its size cap, so a cap below one
/// segment would not hold.
fn keeps_a_segment(retention: RetentionBytes, segment_size: u64) -> Result<(), ConfigFault> {
    match retention {
        RetentionBytes::PerPartition(bytes) if bytes < segment_size => {
            Err(ConfigFault::InvalidValue {
                name: keys::RETENTION_BYTES.to_string(),
                value: bytes.to_string(),
                reason: format!(
                    "Iggy keeps at least one segment of each partition, so retention.bytes must be -1 \
                 or at least segment.bytes ({segment_size})"
                ),
            })
        }
        _ => Ok(()),
    }
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
        CLEANUP_POLICY, MIN_INSYNC_REPLICAS, RETENTION_BYTES, RETENTION_MS, SEGMENT_BYTES,
    };
    use super::*;

    const GIB: u64 = 1024 * 1024 * 1024;
    const MIB: u64 = 1024 * 1024;

    fn topic() -> TopicConfigSnapshot {
        TopicConfigSnapshot {
            partitions_count: 3,
            message_expiry: IggyExpiry::NeverExpire,
            message_expiry_explicit: false,
            max_topic_size: MaxTopicSize::Unlimited,
            max_topic_size_explicit: false,
            segment_size: DEFAULT_SEGMENT_SIZE,
            segment_size_explicit: false,
        }
    }

    fn incremental(
        entries: &[(&'static str, i8, Option<&'static str>)],
        topic: &TopicConfigSnapshot,
        defaults: &TopicDefaults,
    ) -> Result<TopicConfigUpdate, ConfigFault> {
        let ops = incremental_ops(entries.iter().copied()).expect("well formed");
        plan_incremental(&ops, topic, defaults)
    }

    fn size(bytes: u64) -> MaxTopicSize {
        MaxTopicSize::Custom(IggyByteSize::from(bytes))
    }

    #[test]
    fn given_create_configs_should_map_the_stored_keys_and_accept_reported_defaults() {
        let settings = plan_create([
            (RETENTION_MS, Some("120000")),
            (RETENTION_BYTES, Some("67108864")),
            (SEGMENT_BYTES, Some("67108864")),
            (CLEANUP_POLICY, Some("delete")),
            ("message.timestamp.type", Some("CreateTime")),
            ("min.insync.replicas", Some("1")),
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
        assert_eq!(
            plan_create([] as [(&str, Option<&str>); 0]).expect("empty"),
            KafkaTopicSettings::default()
        );
    }

    #[test]
    fn given_create_configs_should_accept_every_retention_ms_kafka_accepts() {
        for value in ["-1", "0", "+1", "0001", " 5000 ", "9223372036854775807"] {
            assert!(
                plan_create([(RETENTION_MS, Some(value))]).is_ok(),
                "{value}"
            );
        }
        let fault = plan_create([(RETENTION_MS, Some("-2"))]).expect_err("below -1");
        assert_eq!(
            fault.message(),
            "Invalid value -2 for configuration retention.ms: Value must be at least -1"
        );
        let fault = plan_create([(RETENTION_MS, Some("1.5"))]).expect_err("not a long");
        assert_eq!(
            fault.message(),
            "Invalid value 1.5 for configuration retention.ms: Not a number of type LONG"
        );
    }

    #[test]
    fn given_create_configs_should_refuse_what_iggy_cannot_hold() {
        let fault = plan_create([(RETENTION_BYTES, Some("104857600"))]).expect_err("floor");
        assert_eq!(
            fault.message(),
            "Invalid value 104857600 for configuration retention.bytes: Iggy keeps at least one \
             segment of each partition, so retention.bytes must be -1 or at least segment.bytes \
             (1073741824)"
        );
        assert!(
            plan_create([
                (RETENTION_BYTES, Some("104857600")),
                (SEGMENT_BYTES, Some("104857600")),
            ])
            .is_ok()
        );
        assert!(plan_create([(RETENTION_BYTES, Some("-5"))]).is_ok());
        assert!(plan_create([(RETENTION_BYTES, Some("0"))]).is_err());
        for value in ["1048575", "1048577", "2147483647", "2147483648"] {
            assert!(
                plan_create([(SEGMENT_BYTES, Some(value))]).is_err(),
                "{value}"
            );
        }
        let fault =
            plan_create([("message.timestamp.type", Some("LogAppendTime"))]).expect_err("fixed");
        assert_eq!(
            fault.message(),
            "Invalid value LogAppendTime for configuration message.timestamp.type: Iggy keeps the \
             timestamp that the producer sets, so this gateway accepts only CreateTime"
        );
        let fault =
            plan_create([("max.message.bytes", Some("2097152"))]).expect_err("fixed size limit");
        assert!(matches!(fault, ConfigFault::InvalidValue { .. }));
    }

    #[test]
    fn given_create_configs_should_accept_keys_iggy_ignores_at_any_value_kafka_accepts() {
        let settings = plan_create([
            (MIN_INSYNC_REPLICAS, Some("2")),
            ("compression.type", Some("zstd")),
            ("segment.ms", Some("3600000")),
            ("min.compaction.lag.ms", Some("86400000")),
            ("unclean.leader.election.enable", Some("true")),
        ])
        .expect("ignored keys");
        assert_eq!(settings, KafkaTopicSettings::default());
        let fault = plan_create([(MIN_INSYNC_REPLICAS, Some("0"))]).expect_err("Kafka refuses 0");
        assert_eq!(
            fault.message(),
            "Invalid value 0 for configuration min.insync.replicas: Value must be at least 1"
        );
    }

    #[test]
    fn given_a_create_policy_without_delete_should_keep_every_record() {
        let compact = plan_create([
            (CLEANUP_POLICY, Some("compact")),
            (RETENTION_MS, Some("1000")),
            (RETENTION_BYTES, Some("1048576")),
        ])
        .expect("retention is ignored, so the floor does not apply");
        assert_eq!(
            compact.cleanup_policy,
            CleanupPolicy {
                delete: false,
                compact: true
            }
        );
        assert!(compact.cleanup_policy.keeps_every_record());
        let empty = plan_create([(CLEANUP_POLICY, Some(""))]).expect("Kafka 4.4 accepts it");
        assert!(empty.cleanup_policy.keeps_every_record());
        assert!(!empty.cleanup_policy.compact);

        let both = plan_create([(CLEANUP_POLICY, Some("delete,compact"))]).expect("both");
        assert!(!both.cleanup_policy.keeps_every_record());
        assert!(both.cleanup_policy.compact);
        assert!(
            plan_create([
                (CLEANUP_POLICY, Some("compact,delete")),
                (RETENTION_BYTES, Some("1048576")),
            ])
            .is_err(),
            "delete applies retention, so the floor does"
        );

        let fault = plan_create([(CLEANUP_POLICY, Some("compact,"))]).expect_err("empty item");
        assert_eq!(
            fault.message(),
            "Configuration 'cleanup.policy' values must not be empty."
        );
        let fault = plan_create([(CLEANUP_POLICY, Some("Delete"))]).expect_err("case");
        assert_eq!(
            fault.message(),
            "Invalid value Delete for configuration cleanup.policy: String must be one of: \
             compact, delete"
        );
    }

    #[test]
    fn given_create_configs_should_name_an_unknown_key_and_every_null_value() {
        let fault = plan_create([("no.such", Some("1"))]).expect_err("unknown");
        assert_eq!(fault.message(), "Unknown topic config name: no.such");
        let fault = plan_create([("message.format.version", Some("3.0"))]).expect_err("removed");
        assert_eq!(
            fault,
            ConfigFault::UnknownKey("message.format.version".to_string())
        );
        let fault = plan_create([
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
        let settings = plan_create([(RETENTION_MS, Some("-2")), (RETENTION_MS, Some("1000"))])
            .expect("the last value counts");
        assert_eq!(
            settings.message_expiry,
            Some(expiry_from_retention_ms(1_000))
        );
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

        let floor = incremental(
            &[(RETENTION_BYTES, 0, Some("1048576"))],
            &topic(),
            &TopicDefaults::default(),
        )
        .expect_err("below one segment");
        assert!(matches!(floor, ConfigFault::InvalidValue { .. }));
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
    fn given_incremental_deletes_should_go_back_to_the_gateway_default() {
        let defaults = TopicDefaults {
            retention_ms: Some(604_800_000),
            retention_bytes: Some(2 * GIB),
            ..TopicDefaults::default()
        };
        let update = incremental(
            &[(RETENTION_MS, 1, None), (RETENTION_BYTES, 1, None)],
            &topic(),
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
            message_expiry_explicit: true,
            max_topic_size: size(6 * GIB),
            max_topic_size_explicit: true,
            ..topic()
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
    fn given_append_and_subtract_should_apply_to_the_reported_list_or_be_refused() {
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
        let update = plan_replace(
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

        let everything = plan_replace(
            [] as [(&str, Option<&str>); 0],
            &set,
            &TopicDefaults::default(),
        )
        .expect("an empty list resets");
        assert_eq!(everything.message_expiry, Some(SettingChange::Reset));
        assert_eq!(everything.max_topic_size, Some(SettingChange::Reset));

        let created = TopicConfigSnapshot {
            segment_size: 64 * MIB,
            segment_size_explicit: true,
            ..topic()
        };
        assert!(matches!(
            plan_replace(
                [(RETENTION_MS, Some("1"))],
                &created,
                &TopicDefaults::default()
            ),
            Err(ConfigFault::Unresettable { .. })
        ));
        assert!(
            plan_replace(
                [(SEGMENT_BYTES, Some("67108864"))],
                &created,
                &TopicDefaults::default()
            )
            .is_ok()
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
        .expect("compact wins, and the floor does not apply");
        assert_eq!(
            incremental_compact.message_expiry,
            Some(SettingChange::Set(IggyExpiry::NeverExpire))
        );
        assert_eq!(
            incremental_compact.max_topic_size,
            Some(SettingChange::Set(MaxTopicSize::Unlimited))
        );

        let defaults = TopicDefaults {
            retention_ms: Some(604_800_000),
            ..TopicDefaults::default()
        };
        let replace_compact =
            plan_replace([(CLEANUP_POLICY, Some("compact"))], &expiring, &defaults).expect("valid");
        assert_eq!(
            replace_compact.message_expiry,
            Some(SettingChange::Set(IggyExpiry::NeverExpire))
        );
        assert_eq!(
            replace_compact.max_topic_size,
            Some(SettingChange::Set(MaxTopicSize::Unlimited))
        );

        let kept = TopicConfigSnapshot {
            message_expiry: IggyExpiry::NeverExpire,
            message_expiry_explicit: true,
            max_topic_size: MaxTopicSize::Unlimited,
            max_topic_size_explicit: true,
            ..topic()
        };
        let again =
            plan_replace([(CLEANUP_POLICY, Some("compact"))], &kept, &defaults).expect("valid");
        assert!(again.is_empty(), "the topic keeps every record already");
        let back_to_delete =
            plan_replace([(CLEANUP_POLICY, Some("delete"))], &kept, &defaults).expect("valid");
        assert_eq!(
            back_to_delete.message_expiry,
            Some(SettingChange::Set(expiry_from_retention_ms(604_800_000))),
            "delete takes the retention the replace leaves, here the gateway default"
        );
    }

    #[test]
    fn given_a_replace_should_refuse_a_repeated_or_unknown_key() {
        let fault = plan_replace(
            [(RETENTION_MS, Some("1000")), (RETENTION_MS, Some("1000"))],
            &topic(),
            &TopicDefaults::default(),
        )
        .expect_err("repeated");
        assert_eq!(fault, ConfigFault::Repeated(RETENTION_MS.to_string()));
        let fault = plan_replace(
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
