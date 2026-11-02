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

//! Topic configuration shared by `DescribeConfigs`, `AlterConfigs` and
//! `IncrementalAlterConfigs`.
//!
//! Iggy stores `message_expiry` as [`IggyExpiry`]. A duration is microseconds
//! (`IggyDuration::from(u64)` / `as_micros`). `u64::MAX` is [`IggyExpiry::NeverExpire`]
//! and `0` is [`IggyExpiry::ServerDefault`], so neither is a usable millisecond expiry.
//! Kafka `retention.ms` is a decimal millisecond count, or `-1` when sealed segments
//! never expire. `retention.minutes` and `retention.hours` are not Kafka topic-level
//! configs - only the broker-level `log.retention.minutes`/`log.retention.hours` exist
//! in real Kafka - so both are rejected the same as any other unknown key.

use std::collections::HashSet;
use std::str::FromStr;

use iggy::prelude::{HeaderKey, IggyDuration, IggyExpiry, ResourceOptions, topic_option_keys};
use kafka_protocol::messages::alter_configs_request::AlterConfigsResource;
use kafka_protocol::messages::describe_configs_request::DescribeConfigsResource;
use kafka_protocol::messages::incremental_alter_configs_request::AlterConfigsResource as IncrementalAlterConfigsResource;
use kafka_protocol::protocol::StrBytes;

use crate::auth::AuthenticatedPrincipal;
use crate::bridge::BridgeError;

/// Kafka `ConfigResource.Type.TOPIC`.
pub const RESOURCE_TYPE_TOPIC: i8 = 2;

/// Kafka `ConfigSource.DYNAMIC_TOPIC_CONFIG`: the value was stored on the topic.
pub const CONFIG_SOURCE_TOPIC: i8 = 1;

/// Kafka `ConfigSource.DEFAULT_CONFIG`: the value is the gateway default.
pub const CONFIG_SOURCE_DEFAULT: i8 = 5;

/// Kafka `ConfigType.LONG`.
pub const CONFIG_TYPE_LONG: i8 = 5;

pub const RETENTION_MS: &str = "retention.ms";
pub const CLEANUP_POLICY: &str = "cleanup.policy";

pub const ONLY_TOPIC_RESOURCES: &str = "only topic resources are supported";

pub const RETENTION_DOC: &str = "How long a sealed segment is kept, in milliseconds. The active segment does not expire. -1 means sealed segments never expire.";

/// Distinct topic names one `CreateTopics`, `CreatePartitions`, `DescribeConfigs`, `AlterConfigs`
/// or `IncrementalAlterConfigs` request may address through the bridge.
///
/// `bounds_guard`'s element ceiling is a pre-decode limit. `CreateTopics` can spend several
/// lockstep Iggy calls per name on the shared client (`ensure_stream`, `create_topic`, and a
/// possible race-retry read). 100 keeps that worst case small and is shared with the config
/// APIs. Duplicate names count once toward the cap and are rejected on their own. A larger
/// batch is `POLICY_VIOLATION` on every resource.
pub const MAX_CONFIG_TOPICS: usize = 100;

const CLEANUP_POLICY_READ_ONLY: &str = "cleanup.policy is read-only";

const NOT_AUTHORIZED_TO_ALTER: &str =
    "the authenticated principal is not authorized to alter topic configuration";

/// One config name a describe request asked for, in request order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListedKey {
    Retention,
    /// An unmodeled name, including `cleanup.policy`, `retention.minutes` and
    /// `retention.hours`. The name itself is never read back out: `DescribeConfigs` omits
    /// every `Unknown` entry from its response, so carrying the name here would only pay a
    /// `String` allocation for a value no caller inspects.
    Unknown,
}

/// Why one resource's alter cannot be stored. Every variant is `INVALID_CONFIG` (40).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigFault {
    UnknownKey(String),
    Repeated(String),
    ReadOnly(&'static str),
    InvalidRetention,
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
            Self::UnknownKey(name) => format!("unknown config key '{name}'"),
            Self::Repeated(name) => format!("config key '{name}' is repeated"),
            Self::ReadOnly(message) => (*message).to_string(),
            Self::InvalidRetention => format!(
                "retention.ms must be -1 or a positive count whose duration is at most {} seconds",
                u32::MAX
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

/// `retention.ms` as Kafka should report it, plus the config source byte.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionMs {
    pub value: String,
    pub source: i8,
}

/// Whether the topic's `message_expiry` option was sent by a client.
///
/// Admission stores the never-expire default with `explicit == false`. An alter
/// of `retention.ms` stores the key with `explicit == true`, including `-1`.
#[must_use]
pub fn message_expiry_is_explicit(options: &ResourceOptions) -> bool {
    let Ok(key) = HeaderKey::from_str(topic_option_keys::MESSAGE_EXPIRY) else {
        return false;
    };
    options.get(&key).is_some_and(|entry| entry.explicit)
}

/// Map an Iggy expiry onto Kafka `retention.ms`.
///
/// A derived never-expire value is [`CONFIG_SOURCE_DEFAULT`]. An explicit one,
/// and any stored duration, is [`CONFIG_SOURCE_TOPIC`]. A duration that is not a
/// positive whole number of milliseconds is refused: reporting a rounded count
/// would describe a different value than Iggy stored.
///
/// # Errors
///
/// Returns `Err` when `expiry` is a duration of `0` microseconds or a duration
/// that is not a whole number of milliseconds.
pub fn retention_ms(expiry: IggyExpiry, explicit: bool) -> Result<RetentionMs, ()> {
    match expiry {
        IggyExpiry::NeverExpire | IggyExpiry::ServerDefault => Ok(RetentionMs {
            value: "-1".to_string(),
            source: if explicit {
                CONFIG_SOURCE_TOPIC
            } else {
                CONFIG_SOURCE_DEFAULT
            },
        }),
        IggyExpiry::ExpireDuration(duration) => {
            let micros = duration.as_micros();
            if micros == 0 || !micros.is_multiple_of(1_000) {
                return Err(());
            }
            Ok(RetentionMs {
                value: (micros / 1_000).to_string(),
                source: CONFIG_SOURCE_TOPIC,
            })
        }
    }
}

/// Parse a Kafka `retention.ms` value into the expiry `update_topic` stores.
///
/// `-1` is [`IggyExpiry::NeverExpire`] (`u64::MAX` on the wire), not
/// [`IggyExpiry::ServerDefault`] (`0`), because the server ignores the `0`
/// sentinel and leaves the previous expiry in place. The text must be the
/// canonical decimal of the parsed integer, so a later describe reports the
/// same characters the alter accepted.
///
/// # Errors
///
/// Returns [`ConfigFault::InvalidRetention`] for any value other than `-1` or a
/// positive millisecond count of at most `u32::MAX` seconds.
pub fn parse_retention_ms(value: &str) -> Result<IggyExpiry, ConfigFault> {
    let Ok(count) = value.parse::<i64>() else {
        return Err(ConfigFault::InvalidRetention);
    };
    if count.to_string() != value {
        return Err(ConfigFault::InvalidRetention);
    }
    if count == -1 {
        return Ok(IggyExpiry::NeverExpire);
    }
    if count <= 0 {
        return Err(ConfigFault::InvalidRetention);
    }
    // `count > 0` was just checked, so this conversion is lossless for every `i64`.
    let millis = u64::try_from(count).map_err(|_| ConfigFault::InvalidRetention)?;
    // `IggyExpiry::from_str` rejects `as_secs() > u32::MAX`. The typed update path
    // does not, but a value the server's own parser refuses is not one this gateway
    // should store.
    if millis / 1_000 > u64::from(u32::MAX) {
        return Err(ConfigFault::InvalidRetention);
    }
    // `millis <= u32::MAX * 1_000` was just proven above, so `millis * 1_000` is far
    // short of `u64::MAX` and cannot overflow.
    Ok(IggyExpiry::ExpireDuration(IggyDuration::from(
        millis * 1_000,
    )))
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
/// the bridge's own, redundant validation inside `get_kafka_topic`/`update_kafka_topic_message_expiry`
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

/// Keys to return for one describe resource.
///
/// `None` and an empty list both mean `retention.ms`. A name repeated in
/// `configuration_keys` is collapsed to one entry, in its first position. Any other name,
/// including `cleanup.policy`, `retention.minutes` and `retention.hours`, is
/// [`ListedKey::Unknown`]. `DescribeConfigs` omits those names from the response and does not
/// fail the resource.
///
/// `cleanup.policy` stays unknown until the gateway stores a policy per topic. Kafka Connect
/// refuses an internal topic that reports any policy other than `compact`, but it starts when
/// the entry is missing (`TopicAdmin.verifyTopicCleanupPolicyOnlyCompact`). A fixed `delete`
/// therefore stops Connect even on topics an operator created for it.
#[must_use]
pub fn listed_keys(configuration_keys: Option<&[StrBytes]>) -> Vec<ListedKey> {
    let Some(keys) = configuration_keys.filter(|keys| !keys.is_empty()) else {
        return vec![ListedKey::Retention];
    };
    let mut seen = HashSet::new();
    keys.iter()
        .filter(|key| seen.insert(key.as_str()))
        .map(|key| match key.as_str() {
            RETENTION_MS => ListedKey::Retention,
            _ => ListedKey::Unknown,
        })
        .collect()
}

/// The expiry to store, or `None` when the resource names no `retention.ms` key.
///
/// One invalid, unknown, or repeated key fails the whole resource and selects nothing.
/// `update_topic` stores the millisecond expiry this returns.
///
/// # Errors
///
/// Returns the first [`ConfigFault`] in request order.
pub fn plan_retention_update<'a>(
    configs: impl IntoIterator<Item = (&'a str, Option<&'a str>)>,
) -> Result<Option<IggyExpiry>, ConfigFault> {
    let mut seen = HashSet::new();
    let mut expiry = None;
    for (name, value) in configs {
        if !seen.insert(name) {
            return Err(ConfigFault::Repeated(name.to_string()));
        }
        match name {
            RETENTION_MS => {
                let Some(value) = value else {
                    return Err(ConfigFault::InvalidRetention);
                };
                expiry = Some(parse_retention_ms(value)?);
            }
            CLEANUP_POLICY => return Err(ConfigFault::ReadOnly(CLEANUP_POLICY_READ_ONLY)),
            other => return Err(ConfigFault::UnknownKey(other.to_string())),
        }
    }
    Ok(expiry)
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

/// The expiry an incremental alter stores, or `None` when no entry changes `retention.ms`.
///
/// `SET` stores the parsed value. `DELETE` stores never-expire, the value a topic has before any
/// alter, and a delete of any other key changes nothing: Kafka removes an override there, and
/// this gateway stored none. `APPEND` and `SUBTRACT` are refused for every key: `cleanup.policy`
/// is a list in Kafka but read-only here, and no other key this gateway knows is a list. Kafka
/// refuses them the same way for a key it does not know.
///
/// # Errors
///
/// Returns the first [`ConfigFault`] in request order.
pub fn plan_incremental_update(ops: &[ConfigOp<'_>]) -> Result<Option<IggyExpiry>, ConfigFault> {
    let mut expiry = None;
    for op in ops {
        match op.operation {
            ConfigOperation::Set => match op.name {
                RETENTION_MS => {
                    let value = op.value.ok_or(ConfigFault::InvalidRetention)?;
                    expiry = Some(parse_retention_ms(value)?);
                }
                CLEANUP_POLICY => return Err(ConfigFault::ReadOnly(CLEANUP_POLICY_READ_ONLY)),
                other => return Err(ConfigFault::UnknownKey(other.to_string())),
            },
            ConfigOperation::Delete => {
                if op.name == RETENTION_MS {
                    expiry = Some(IggyExpiry::NeverExpire);
                }
            }
            ConfigOperation::Append | ConfigOperation::Subtract => {
                if op.name == CLEANUP_POLICY {
                    return Err(ConfigFault::ReadOnly(CLEANUP_POLICY_READ_ONLY));
                }
                return Err(ConfigFault::NotAList {
                    operation: op.operation,
                    key: op.name.to_string(),
                });
            }
        }
    }
    Ok(expiry)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retention_ms_round_trips_a_positive_value_and_never_expire() {
        let expiry = parse_retention_ms("1500").expect("1500 ms");
        let described = retention_ms(expiry, true).expect("representable");
        assert_eq!(described.value, "1500");
        assert_eq!(described.source, CONFIG_SOURCE_TOPIC);
        assert_eq!(
            u64::from(expiry),
            1_500_000,
            "IggyDuration stores microseconds"
        );

        let never = parse_retention_ms("-1").expect("never expire");
        assert_eq!(never, IggyExpiry::NeverExpire);
        let described = retention_ms(never, true).expect("never expire");
        assert_eq!(described.value, "-1");
        assert_eq!(described.source, CONFIG_SOURCE_TOPIC);
        let unset = retention_ms(IggyExpiry::NeverExpire, false).expect("default");
        assert_eq!(unset.value, "-1");
        assert_eq!(unset.source, CONFIG_SOURCE_DEFAULT);
    }

    #[test]
    fn retention_ms_rejects_zero_noncanonical_and_unrepresentable_values() {
        assert!(parse_retention_ms("0").is_err());
        assert!(parse_retention_ms("-2").is_err());
        assert!(parse_retention_ms("0001").is_err());
        assert!(parse_retention_ms("+1").is_err());
        assert!(parse_retention_ms("1.5").is_err());
        assert!(parse_retention_ms("").is_err());
        assert!(parse_retention_ms(&i64::MAX.to_string()).is_err());
        let max_secs_ms = u64::from(u32::MAX) * 1_000;
        let at_cap = parse_retention_ms(&max_secs_ms.to_string()).expect("u32::MAX seconds");
        assert_eq!(u64::from(at_cap), max_secs_ms * 1_000);
        let with_remainder = max_secs_ms + 999;
        assert!(
            parse_retention_ms(&with_remainder.to_string()).is_ok(),
            "a remainder under one second stays within IggyExpiry::from_str"
        );
        assert!(parse_retention_ms(&(max_secs_ms + 1_000).to_string()).is_err());
        assert!(parse_retention_ms(&(u64::MAX / 1_000).to_string()).is_err());

        let sub_ms = IggyExpiry::ExpireDuration(IggyDuration::from(1_500_u64));
        assert!(retention_ms(sub_ms, true).is_err());
    }

    #[test]
    fn listed_keys_defaults_to_retention_and_keeps_an_unknown_name() {
        assert_eq!(listed_keys(None), vec![ListedKey::Retention]);
        assert_eq!(listed_keys(Some(&[])), vec![ListedKey::Retention]);
        let asked = [
            StrBytes::from_static_str(RETENTION_MS),
            StrBytes::from_static_str("no.such"),
        ];
        assert_eq!(
            listed_keys(Some(&asked)),
            vec![ListedKey::Retention, ListedKey::Unknown]
        );
    }

    #[test]
    fn cleanup_policy_is_an_unknown_name_even_when_asked_for() {
        let asked = [StrBytes::from_static_str(CLEANUP_POLICY)];
        assert_eq!(listed_keys(Some(&asked)), vec![ListedKey::Unknown]);
    }

    #[test]
    fn listed_keys_collapses_a_name_repeated_in_the_request() {
        let asked = [
            StrBytes::from_static_str(RETENTION_MS),
            StrBytes::from_static_str(RETENTION_MS),
            StrBytes::from_static_str(CLEANUP_POLICY),
        ];
        assert_eq!(
            listed_keys(Some(&asked)),
            vec![ListedKey::Retention, ListedKey::Unknown]
        );
    }

    #[test]
    fn retention_minutes_and_hours_are_unknown_keys() {
        let asked = [
            StrBytes::from_static_str("retention.minutes"),
            StrBytes::from_static_str("retention.hours"),
        ];
        assert_eq!(
            listed_keys(Some(&asked)),
            vec![ListedKey::Unknown, ListedKey::Unknown]
        );
        let err = plan_retention_update([("retention.minutes", Some("2"))])
            .expect_err("not a real topic config");
        assert_eq!(
            err,
            ConfigFault::UnknownKey("retention.minutes".to_string())
        );
        let err = plan_retention_update([("retention.hours", Some("1"))])
            .expect_err("not a real topic config");
        assert_eq!(err, ConfigFault::UnknownKey("retention.hours".to_string()));
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
    fn one_bad_key_rejects_the_whole_alter_plan() {
        let err = plan_retention_update([(RETENTION_MS, Some("5000")), ("no.such", Some("1"))])
            .expect_err("unknown key");
        assert_eq!(err, ConfigFault::UnknownKey("no.such".to_string()));

        let err = plan_retention_update([(CLEANUP_POLICY, Some("delete"))]).expect_err("read only");
        assert!(matches!(err, ConfigFault::ReadOnly(_)));

        assert_eq!(
            plan_retention_update([(RETENTION_MS, Some("-1"))]).expect("clear"),
            Some(IggyExpiry::NeverExpire)
        );
        assert_eq!(
            plan_retention_update([] as [(&str, Option<&str>); 0]).expect("empty"),
            None
        );
    }

    #[test]
    fn a_repeated_retention_ms_key_fails_the_resource() {
        let err =
            plan_retention_update([(RETENTION_MS, Some("1000")), (RETENTION_MS, Some("1000"))])
                .expect_err("repeated key");
        assert_eq!(err, ConfigFault::Repeated(RETENTION_MS.to_string()));
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

    #[test]
    fn incremental_plan_sets_and_deletes_retention() {
        let set = incremental_ops([(RETENTION_MS, 0, Some("1500"))]).expect("ops");
        assert_eq!(
            plan_incremental_update(&set).expect("set"),
            Some(IggyExpiry::ExpireDuration(IggyDuration::from(
                1_500_000_u64
            )))
        );
        let delete = incremental_ops([(RETENTION_MS, 1, None)]).expect("ops");
        assert_eq!(
            plan_incremental_update(&delete).expect("delete"),
            Some(IggyExpiry::NeverExpire)
        );
    }

    #[test]
    fn incremental_plan_ignores_a_delete_of_a_key_it_never_stored() {
        let ops = incremental_ops([("no.such", 1, None), (CLEANUP_POLICY, 1, None)]).expect("ops");
        assert_eq!(plan_incremental_update(&ops).expect("no-op"), None);
        assert_eq!(plan_incremental_update(&[]).expect("empty"), None);
    }

    #[test]
    fn incremental_plan_refuses_set_of_unknown_read_only_and_invalid_values() {
        let unknown = incremental_ops([("no.such", 0, Some("1"))]).expect("ops");
        assert_eq!(
            plan_incremental_update(&unknown).expect_err("unknown"),
            ConfigFault::UnknownKey("no.such".to_string())
        );
        let read_only = incremental_ops([(CLEANUP_POLICY, 0, Some("compact"))]).expect("ops");
        assert_eq!(
            plan_incremental_update(&read_only).expect_err("read only"),
            ConfigFault::ReadOnly(CLEANUP_POLICY_READ_ONLY)
        );
        let invalid = incremental_ops([(RETENTION_MS, 0, Some("abc"))]).expect("ops");
        assert_eq!(
            plan_incremental_update(&invalid).expect_err("invalid"),
            ConfigFault::InvalidRetention
        );
    }

    #[test]
    fn incremental_plan_refuses_append_and_subtract_on_every_key() {
        let append = incremental_ops([(RETENTION_MS, 2, Some("1"))]).expect("ops");
        let err = plan_incremental_update(&append).expect_err("append");
        assert_eq!(
            err,
            ConfigFault::NotAList {
                operation: ConfigOperation::Append,
                key: RETENTION_MS.to_string(),
            }
        );
        assert_eq!(
            err.message(),
            "Can't APPEND to key retention.ms because its type is not LIST."
        );
        let subtract = incremental_ops([("no.such", 3, Some("1"))]).expect("ops");
        let err = plan_incremental_update(&subtract).expect_err("subtract");
        assert_eq!(
            err.message(),
            "Can't SUBTRACT to key no.such because its type is not LIST."
        );
    }

    #[test]
    fn incremental_plan_refuses_append_and_subtract_on_cleanup_policy_as_read_only() {
        let append = incremental_ops([(CLEANUP_POLICY, 2, Some("compact"))]).expect("ops");
        assert_eq!(
            plan_incremental_update(&append).expect_err("append"),
            ConfigFault::ReadOnly(CLEANUP_POLICY_READ_ONLY)
        );
        let subtract = incremental_ops([(CLEANUP_POLICY, 3, Some("delete"))]).expect("ops");
        assert_eq!(
            plan_incremental_update(&subtract).expect_err("subtract"),
            ConfigFault::ReadOnly(CLEANUP_POLICY_READ_ONLY)
        );
    }
}
