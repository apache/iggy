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
//! never expire. `retention.minutes` and `retention.hours` are the same duration,
//! converted to milliseconds before anything is stored. A positive count is capped
//! at `u32::MAX` seconds, matching [`IggyExpiry::from_str`].

use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use iggy::prelude::{HeaderKey, IggyDuration, IggyExpiry, ResourceOptions, topic_option_keys};
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
pub const RETENTION_MINUTES: &str = "retention.minutes";
pub const RETENTION_HOURS: &str = "retention.hours";
pub const MILLIS_PER_MINUTE: u64 = 60_000;
pub const MILLIS_PER_HOUR: u64 = 3_600_000;
pub const CLEANUP_POLICY: &str = "cleanup.policy";
pub const CLEANUP_POLICY_VALUE: &str = "delete";

pub const ONLY_TOPIC_RESOURCES: &str = "only topic resources are supported";

pub const RETENTION_DOC: &str = "How long a sealed segment is kept, in milliseconds. The active segment does not expire. -1 means sealed segments never expire.";
pub const RETENTION_MINUTES_DOC: &str = "How long a sealed segment is kept, in minutes, derived from retention.ms. The count truncates toward zero. -1 means sealed segments never expire.";
pub const RETENTION_HOURS_DOC: &str = "How long a sealed segment is kept, in hours, derived from retention.ms. The count truncates toward zero. -1 means sealed segments never expire.";
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
    Minutes,
    Hours,
    Cleanup,
    Unknown(String),
}

/// Why one resource's alter cannot be stored.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigFault {
    UnknownKey(String),
    Repeated(String),
    ReadOnly(&'static str),
    InvalidRetention,
    ConflictingRetention,
}

impl ConfigFault {
    #[must_use]
    pub fn message(&self) -> String {
        match self {
            Self::UnknownKey(name) => format!("unknown config key '{name}'"),
            Self::Repeated(name) => format!("config key '{name}' is repeated"),
            Self::ReadOnly(message) => (*message).to_string(),
            Self::InvalidRetention => format!(
                "retention.ms, retention.minutes, and retention.hours must be -1 or a positive count whose duration is at most {} seconds",
                u32::MAX
            ),
            Self::ConflictingRetention => "retention.ms, retention.minutes, and retention.hours must describe the same duration".to_string(),
        }
    }
}

/// `retention.minutes` and `retention.hours` used by the last successful alter.
///
/// Process-local, keyed by Iggy stream and topic, and not stored in Iggy. A
/// restart clears it. Deleting the topic outside this gateway does not.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetentionSynonyms {
    pub minutes: bool,
    pub hours: bool,
}

impl RetentionSynonyms {
    #[must_use]
    pub const fn is_empty(self) -> bool {
        !self.minutes && !self.hours
    }
}

/// Iggy stream and topic a Kafka name resolves to. Synonym memory uses this pair,
/// not the Kafka topic name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IggyTopicKey {
    stream: String,
    topic: String,
}

impl IggyTopicKey {
    #[must_use]
    pub fn new(stream: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            stream: stream.into(),
            topic: topic.into(),
        }
    }
}

/// Process memory of [`RetentionSynonyms`] per [`IggyTopicKey`].
#[derive(Debug, Default)]
pub struct RetentionSynonymMemory {
    topics: HashMap<IggyTopicKey, RetentionSynonyms>,
}

impl RetentionSynonymMemory {
    #[must_use]
    pub fn remembered(&self, key: &IggyTopicKey) -> RetentionSynonyms {
        self.topics.get(key).copied().unwrap_or_default()
    }

    /// Remembers `synonyms` after a successful alter that stored an expiry.
    ///
    /// An empty set clears the key. That is an alter which used only
    /// `retention.ms`. A failed alter, and a resource that stored nothing,
    /// must not call this.
    pub fn record_applied(&mut self, key: IggyTopicKey, synonyms: RetentionSynonyms) {
        if synonyms.is_empty() {
            self.topics.remove(&key);
        } else {
            self.topics.insert(key, synonyms);
        }
    }
}

/// One duration to store, plus which synonym names the alter used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlannedRetention {
    pub expiry: IggyExpiry,
    pub synonyms: RetentionSynonyms,
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
#[cfg(test)]
pub fn parse_retention_ms(value: &str) -> Result<IggyExpiry, ConfigFault> {
    parse_scaled_retention(value, 1)
}

/// `retention.ms` text scaled into `unit_ms` units.
///
/// `-1` stays `-1`. Any other count is integer division toward zero, so a stored
/// `90000` reported as minutes is `1` while `retention.ms` stays `90000`.
#[must_use]
pub fn retention_in_unit(millis_text: &str, unit_ms: u64) -> Option<String> {
    let Ok(millis) = millis_text.parse::<i64>() else {
        return None;
    };
    if millis == -1 {
        return Some("-1".to_string());
    }
    if millis < 0 || unit_ms == 0 {
        return None;
    }
    let Ok(millis) = u64::try_from(millis) else {
        return None;
    };
    Some((millis / unit_ms).to_string())
}

/// `count` is one unit of `unit_ms` milliseconds. `1` is `retention.ms`.
fn parse_scaled_retention(value: &str, unit_ms: u64) -> Result<IggyExpiry, ConfigFault> {
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
    let count = u64::try_from(count).map_err(|_| ConfigFault::InvalidRetention)?;
    // Reject a product that does not fit before multiplying into milliseconds.
    if unit_ms == 0 || count > u64::MAX / unit_ms {
        return Err(ConfigFault::InvalidRetention);
    }
    let millis = count * unit_ms;
    // `IggyExpiry::from_str` rejects `as_secs() > u32::MAX`. The typed update path
    // does not, but a value the server's own parser refuses is not one this gateway
    // should store.
    if millis / 1_000 > u64::from(u32::MAX) {
        return Err(ConfigFault::InvalidRetention);
    }
    let Some(micros) = millis.checked_mul(1_000) else {
        return Err(ConfigFault::InvalidRetention);
    };
    Ok(IggyExpiry::ExpireDuration(IggyDuration::from(micros)))
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
/// (42) rather than silently picking a winner - the same choice `CreateTopics`' own
/// `find_duplicate_names` makes for a repeated topic name in one batch.
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

/// The reason a topic name failed Kafka's own naming rules, or a fixed fallback.
#[must_use]
pub fn name_reason(error: &BridgeError) -> String {
    match error {
        BridgeError::InvalidKafkaTopicName { reason, .. } => reason.clone(),
        _ => "invalid topic name".to_string(),
    }
}

#[must_use]
pub const fn static_text(message: &'static str) -> StrBytes {
    StrBytes::from_static_str(message)
}

/// Maps a bridge failure to a Kafka error code and client-facing message, logging the real
/// cause server-side.
///
/// `handler` names the API in the log line (`"DescribeConfigs"`/`"AlterConfigs"`); `action` is
/// the present participle of what the bridge call was doing (`"reading"`/`"altering"`), reused
/// in both the timeout log and the generic internal-error message.
#[must_use]
pub fn bridge_failure(error: &BridgeError, handler: &str, action: &str) -> (i16, Option<StrBytes>) {
    let code = error.to_kafka_error_code();
    match error {
        BridgeError::InvalidKafkaTopicName { reason, .. } => {
            tracing::debug!(reason, "{handler} rejected an invalid topic name");
            (code, Some(StrBytes::from(reason.clone())))
        }
        BridgeError::Timeout | BridgeError::SendLost(_) => {
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
/// `None` and an empty list both mean `retention.ms` and `cleanup.policy`.
/// `retention.minutes` and `retention.hours` are listed only when the client
/// names them. Any other name is [`ListedKey::Unknown`]. `DescribeConfigs` omits
/// those names from the response and does not fail the resource.
#[must_use]
pub fn listed_keys(configuration_keys: Option<&[StrBytes]>) -> Vec<ListedKey> {
    let Some(keys) = configuration_keys.filter(|keys| !keys.is_empty()) else {
        return vec![ListedKey::Retention, ListedKey::Cleanup];
    };
    keys.iter()
        .map(|key| match key.as_str() {
            RETENTION_MS => ListedKey::Retention,
            RETENTION_MINUTES => ListedKey::Minutes,
            RETENTION_HOURS => ListedKey::Hours,
            CLEANUP_POLICY => ListedKey::Cleanup,
            other => ListedKey::Unknown(other.to_string()),
        })
        .collect()
}

/// The expiry to store, or `None` when the resource names no retention key.
///
/// `retention.ms`, `retention.minutes`, and `retention.hours` are one duration.
/// Minutes and hours convert to milliseconds. Two names that convert to
/// different millisecond values fail the resource and select nothing. One
/// expiry is returned when they agree. [`PlannedRetention::synonyms`] is empty
/// when the resource used only `retention.ms`.
///
/// One invalid or repeated key fails the whole resource. Nothing is selected
/// for storage in that case. `update_topic` stores the millisecond expiry, never
/// the minute or hour count.
///
/// # Errors
///
/// Returns the first [`ConfigFault`] in request order.
pub fn plan_retention_update<'a>(
    configs: impl IntoIterator<Item = (&'a str, Option<&'a str>)>,
) -> Result<Option<PlannedRetention>, ConfigFault> {
    let mut seen = HashSet::new();
    let mut expiry = None;
    let mut synonyms = RetentionSynonyms::default();
    for (name, value) in configs {
        if !seen.insert(name) {
            return Err(ConfigFault::Repeated(name.to_string()));
        }
        let unit_ms = retention_scale(name, &mut synonyms)?;
        let Some(value) = value else {
            return Err(ConfigFault::InvalidRetention);
        };
        let parsed = parse_scaled_retention(value, unit_ms)?;
        if let Some(existing) = expiry
            && existing != parsed
        {
            return Err(ConfigFault::ConflictingRetention);
        }
        expiry = Some(parsed);
    }
    Ok(expiry.map(|expiry| PlannedRetention { expiry, synonyms }))
}

fn retention_scale(name: &str, synonyms: &mut RetentionSynonyms) -> Result<u64, ConfigFault> {
    match name {
        RETENTION_MS => Ok(1),
        RETENTION_MINUTES => {
            synonyms.minutes = true;
            Ok(MILLIS_PER_MINUTE)
        }
        RETENTION_HOURS => {
            synonyms.hours = true;
            Ok(MILLIS_PER_HOUR)
        }
        CLEANUP_POLICY => Err(ConfigFault::ReadOnly("cleanup.policy is read-only")),
        other => Err(ConfigFault::UnknownKey(other.to_string())),
    }
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
            vec![
                ListedKey::Cleanup,
                ListedKey::Unknown("no.such".to_string())
            ]
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
    fn one_bad_key_rejects_the_whole_alter_plan() {
        let err = plan_retention_update([(RETENTION_MS, Some("5000")), ("no.such", Some("1"))])
            .expect_err("unknown key");
        assert_eq!(err, ConfigFault::UnknownKey("no.such".to_string()));

        let err = plan_retention_update([(CLEANUP_POLICY, Some("delete"))]).expect_err("read only");
        assert!(matches!(err, ConfigFault::ReadOnly(_)));

        assert_eq!(
            plan_retention_update([(RETENTION_MS, Some("-1"))])
                .expect("clear")
                .expect("stores")
                .expiry,
            IggyExpiry::NeverExpire
        );
        assert_eq!(
            plan_retention_update([] as [(&str, Option<&str>); 0]).expect("empty"),
            None
        );
    }

    #[test]
    fn minutes_and_hours_convert_to_milliseconds() {
        let minutes = plan_retention_update([(RETENTION_MINUTES, Some("2"))])
            .expect("minutes")
            .expect("stores");
        assert_eq!(u64::from(minutes.expiry), 2 * MILLIS_PER_MINUTE * 1_000);
        assert!(minutes.synonyms.minutes);
        assert!(!minutes.synonyms.hours);
        let described = retention_ms(minutes.expiry, true).expect("representable");
        assert_eq!(described.value, "120000");
        assert_eq!(
            retention_in_unit(&described.value, MILLIS_PER_MINUTE).as_deref(),
            Some("2")
        );

        let hours = plan_retention_update([(RETENTION_HOURS, Some("1"))])
            .expect("hours")
            .expect("stores");
        assert_eq!(u64::from(hours.expiry), MILLIS_PER_HOUR * 1_000);
        assert!(hours.synonyms.hours);
        let described = retention_ms(hours.expiry, true).expect("representable");
        assert_eq!(described.value, "3600000");
        assert_eq!(
            retention_in_unit(&described.value, MILLIS_PER_HOUR).as_deref(),
            Some("1")
        );

        let agreed = plan_retention_update([
            (RETENTION_MS, Some("3600000")),
            (RETENTION_MINUTES, Some("60")),
            (RETENTION_HOURS, Some("1")),
        ])
        .expect("same duration")
        .expect("one write");
        assert_eq!(
            retention_ms(agreed.expiry, true)
                .expect("representable")
                .value,
            "3600000"
        );
        assert!(agreed.synonyms.minutes);
        assert!(agreed.synonyms.hours);

        assert_eq!(
            retention_in_unit("90000", MILLIS_PER_MINUTE).as_deref(),
            Some("1"),
            "synonym display truncates toward zero"
        );
        assert_eq!(
            retention_in_unit("-1", MILLIS_PER_HOUR).as_deref(),
            Some("-1")
        );
    }

    #[test]
    fn synonym_memory_tracks_minutes_and_hours_and_clears_on_milliseconds_only() {
        let mut memory = RetentionSynonymMemory::default();
        let orders = IggyTopicKey::new("kafka", "orders");
        let other_stream = IggyTopicKey::new("billing", "orders");
        let minutes = plan_retention_update([(RETENTION_MINUTES, Some("2"))])
            .expect("minutes")
            .expect("stores");
        memory.record_applied(orders.clone(), minutes.synonyms);
        assert!(memory.remembered(&orders).minutes);
        assert!(memory.remembered(&other_stream).is_empty());

        let ms_only = plan_retention_update([(RETENTION_MS, Some("1500"))])
            .expect("ms")
            .expect("stores");
        assert!(ms_only.synonyms.is_empty());
        memory.record_applied(orders.clone(), ms_only.synonyms);
        assert!(memory.remembered(&orders).is_empty());

        assert_eq!(
            listed_keys(None),
            vec![ListedKey::Retention, ListedKey::Cleanup]
        );
        let asked = [StrBytes::from_static_str(RETENTION_MINUTES)];
        assert_eq!(listed_keys(Some(&asked)), vec![ListedKey::Minutes]);
        let asked = [StrBytes::from_static_str(RETENTION_HOURS)];
        assert_eq!(listed_keys(Some(&asked)), vec![ListedKey::Hours]);
    }

    #[test]
    fn disagreeing_retention_units_store_nothing_and_keep_remembered_names() {
        let mut memory = RetentionSynonymMemory::default();
        let orders = IggyTopicKey::new("kafka", "orders");
        memory.record_applied(
            orders.clone(),
            RetentionSynonyms {
                minutes: true,
                hours: false,
            },
        );
        let err =
            plan_retention_update([(RETENTION_MS, Some("90000")), (RETENTION_HOURS, Some("1"))])
                .expect_err("different durations");
        assert_eq!(err, ConfigFault::ConflictingRetention);
        assert!(memory.remembered(&orders).minutes);
        assert!(!memory.remembered(&orders).hours);

        let err = plan_retention_update([("retention.mins", Some("1"))]).expect_err("not a name");
        assert_eq!(err, ConfigFault::UnknownKey("retention.mins".to_string()));
        assert!(memory.remembered(&orders).minutes);
    }

    #[test]
    fn retention_hours_negative_one_stores_never_expire() {
        let planned = plan_retention_update([(RETENTION_HOURS, Some("-1"))])
            .expect("hours")
            .expect("stores");
        assert_eq!(planned.expiry, IggyExpiry::NeverExpire);
        assert!(planned.synonyms.hours);
        assert!(!planned.synonyms.minutes);
        let described = retention_ms(planned.expiry, true).expect("never expire");
        assert_eq!(described.value, "-1");
        assert_eq!(described.source, CONFIG_SOURCE_TOPIC);
    }

    #[test]
    fn retention_unit_overflow_is_invalid_and_not_stored() {
        let mut memory = RetentionSynonymMemory::default();
        let orders = IggyTopicKey::new("kafka", "orders");
        memory.record_applied(
            orders.clone(),
            RetentionSynonyms {
                minutes: false,
                hours: true,
            },
        );

        let overflow_hours = (u64::MAX / MILLIS_PER_HOUR + 1).to_string();
        let err = plan_retention_update([(RETENTION_HOURS, Some(overflow_hours.as_str()))])
            .expect_err("overflow before multiply");
        assert_eq!(err, ConfigFault::InvalidRetention);

        let past_cap_hours = (u64::from(u32::MAX) / 3_600 + 1).to_string();
        let err = plan_retention_update([(RETENTION_HOURS, Some(past_cap_hours.as_str()))])
            .expect_err("seconds above u32::MAX");
        assert_eq!(err, ConfigFault::InvalidRetention);

        let past_cap_minutes = (u64::from(u32::MAX) / 60 + 1).to_string();
        let err = plan_retention_update([(RETENTION_MINUTES, Some(past_cap_minutes.as_str()))])
            .expect_err("seconds above u32::MAX");
        assert_eq!(err, ConfigFault::InvalidRetention);

        assert!(plan_retention_update([(RETENTION_MINUTES, Some("0"))]).is_err());
        assert!(plan_retention_update([(RETENTION_HOURS, Some("0001"))]).is_err());
        assert!(plan_retention_update([(RETENTION_MINUTES, Some("+1"))]).is_err());
        assert!(memory.remembered(&orders).hours);
        assert!(!memory.remembered(&orders).minutes);

        let at_cap_hours = (u64::from(u32::MAX) / 3_600).to_string();
        let stored = plan_retention_update([(RETENTION_HOURS, Some(at_cap_hours.as_str()))])
            .expect("at the second cap")
            .expect("stores");
        let seconds = u64::from(stored.expiry) / 1_000 / 1_000;
        assert!(u32::try_from(seconds).is_ok());
    }
}
