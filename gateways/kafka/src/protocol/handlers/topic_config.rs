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

//! Topic configuration shared by `DescribeConfigs` and `AlterConfigs`.
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
use kafka_protocol::protocol::StrBytes;

use crate::bridge::BridgeError;

/// Kafka `ConfigResource.Type.TOPIC`.
pub const RESOURCE_TYPE_TOPIC: i8 = 2;

/// Kafka `ConfigSource.DYNAMIC_TOPIC_CONFIG`: the value was stored on the topic.
pub const CONFIG_SOURCE_TOPIC: i8 = 1;

/// Kafka `ConfigSource.DEFAULT_CONFIG`: the value is the gateway default.
pub const CONFIG_SOURCE_DEFAULT: i8 = 5;

/// Kafka `ConfigType.STRING`.
pub const CONFIG_TYPE_STRING: i8 = 2;

/// Kafka `ConfigType.LONG`.
pub const CONFIG_TYPE_LONG: i8 = 5;

pub const RETENTION_MS: &str = "retention.ms";
pub const CLEANUP_POLICY: &str = "cleanup.policy";
pub const CLEANUP_POLICY_VALUE: &str = "delete";

pub const ONLY_TOPIC_RESOURCES: &str = "only topic resources are supported";

pub const RETENTION_DOC: &str = "How long a sealed segment is kept, in milliseconds. The active segment does not expire. -1 means sealed segments never expire.";
pub const CLEANUP_DOC: &str = "Iggy deletes expired messages and does not compact a topic.";

/// Distinct topic names one `CreateTopics`, `DescribeConfigs`, or `AlterConfigs` request
/// may address through the bridge.
///
/// `bounds_guard`'s element ceiling is a pre-decode limit. `CreateTopics` can spend several
/// lockstep Iggy calls per name on the shared client (`ensure_stream`, `create_topic`, and a
/// possible race-retry read). 100 keeps that worst case small and is shared with the config
/// APIs. Duplicate names count once toward the cap and are rejected on their own. A larger
/// batch is `POLICY_VIOLATION` on every resource.
pub const MAX_CONFIG_TOPICS: usize = 100;

/// One config name a describe request asked for, in request order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ListedKey {
    Retention,
    Cleanup,
    /// An unmodeled name, including `retention.minutes` and `retention.hours`. The name
    /// itself is never read back out: `DescribeConfigs` omits every `Unknown` entry from
    /// its response, so carrying the name here would only pay a `String` allocation for a
    /// value no caller inspects.
    Unknown,
}

/// Why one resource's alter cannot be stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigFault {
    UnknownKey(String),
    Repeated(String),
    ReadOnly(&'static str),
    InvalidRetention,
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
/// Real Kafka refuses every occurrence of a duplicate resource name with `INVALID_REQUEST`
/// (42) rather than silently picking a winner. Shared by `CreateTopics`, `DescribeConfigs`
/// and `AlterConfigs`, which all make the same choice for a repeated topic name in one batch.
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
/// `None` and an empty list both mean `retention.ms` and `cleanup.policy`. A name repeated
/// in `configuration_keys` is collapsed to one entry, in its first position. Any other name,
/// including `retention.minutes` and `retention.hours`, is [`ListedKey::Unknown`].
/// `DescribeConfigs` omits those names from the response and does not fail the resource.
#[must_use]
pub fn listed_keys(configuration_keys: Option<&[StrBytes]>) -> Vec<ListedKey> {
    let Some(keys) = configuration_keys.filter(|keys| !keys.is_empty()) else {
        return vec![ListedKey::Retention, ListedKey::Cleanup];
    };
    let mut seen = HashSet::new();
    keys.iter()
        .filter(|key| seen.insert(key.as_str()))
        .map(|key| match key.as_str() {
            RETENTION_MS => ListedKey::Retention,
            CLEANUP_POLICY => ListedKey::Cleanup,
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
            CLEANUP_POLICY => return Err(ConfigFault::ReadOnly("cleanup.policy is read-only")),
            other => return Err(ConfigFault::UnknownKey(other.to_string())),
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
    fn listed_keys_defaults_to_both_and_keeps_an_unknown_name() {
        assert_eq!(
            listed_keys(None),
            vec![ListedKey::Retention, ListedKey::Cleanup]
        );
        assert_eq!(
            listed_keys(Some(&[])),
            vec![ListedKey::Retention, ListedKey::Cleanup]
        );
        let asked = [
            StrBytes::from_static_str(CLEANUP_POLICY),
            StrBytes::from_static_str("no.such"),
        ];
        assert_eq!(
            listed_keys(Some(&asked)),
            vec![ListedKey::Cleanup, ListedKey::Unknown]
        );
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
            vec![ListedKey::Retention, ListedKey::Cleanup]
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
}
