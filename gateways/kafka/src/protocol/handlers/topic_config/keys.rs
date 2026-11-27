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

//! The Kafka configuration keys this gateway answers: every topic key of Kafka 4.4, and the
//! broker keys that tools read.
//!
//! Each value is checked as Kafka 4.4 checks it (`LogConfig`), with Kafka's own text. Past that,
//! the gateway does one of these things with a topic key:
//!
//! - It stores `retention.ms`, `retention.bytes` and `segment.bytes` on the Iggy topic.
//! - It accepts any `cleanup.policy`, and a policy without `delete` keeps every record. The
//!   policy is not stored, because Iggy has no option for it, so a describe leaves it out. Kafka
//!   Connect starts when the entry is missing (`TopicAdmin.verifyTopicCleanupPolicyOnlyCompact`).
//! - It reports `min.insync.replicas` as the majority of the replication factor, which Iggy needs
//!   for every commit, and accepts a value up to that majority.
//! - It maps `flush.messages=1` and `flush.ms=0`, a flush of every write, to `persisted`
//!   durability, which Iggy sets only when it creates a topic. Any other value is accepted and not
//!   applied.
//! - It accepts a `max.message.bytes` from Kafka's default up to the largest batch that Produce
//!   takes, and reports that largest batch.
//! - It accepts any value of a key that changes nothing a client sees, such as a compression
//!   level or a compaction setting, and reports the Kafka default.
//! - It accepts only the Kafka default of `message.timestamp.type`, because another value changes
//!   the timestamps that clients get and Iggy cannot apply it.

use std::cmp::Ordering;
use std::collections::HashSet;

use kafka_protocol::protocol::StrBytes;

use super::{CONFIG_SOURCE_DEFAULT, CONFIG_SOURCE_STATIC_BROKER, CONFIG_SOURCE_TOPIC};
use crate::bridge::topic_settings::retention_ms_of;
use crate::bridge::{DEFAULT_TOPIC_PARTITIONS, IggyBridge, TopicConfigSnapshot, TopicDefaults};

pub const RETENTION_MS: &str = "retention.ms";
pub const RETENTION_BYTES: &str = "retention.bytes";
pub const SEGMENT_BYTES: &str = "segment.bytes";
pub const CLEANUP_POLICY: &str = "cleanup.policy";
pub const MIN_INSYNC_REPLICAS: &str = "min.insync.replicas";
pub const MAX_MESSAGE_BYTES: &str = "max.message.bytes";
pub const FLUSH_MESSAGES: &str = "flush.messages";
pub const FLUSH_MS: &str = "flush.ms";

const RETENTION_MS_DOC: &str = "How long a sealed segment is kept, in milliseconds. The active \
                                segment does not expire. -1 means sealed segments never expire.";
const RETENTION_BYTES_DOC: &str = "The most bytes of sealed segments that each partition keeps. \
                                   Iggy always keeps the newest sealed segment and the active \
                                   one. -1 means no limit.";
const SEGMENT_BYTES_DOC: &str = "The size at which a partition starts a new segment. It is set \
                                 when the topic is created.";
const IGNORED_DOC: &str = "Reported at the Kafka default. This gateway accepts any valid value \
                           and does not apply it.";
const FIXED_DOC: &str = "Reported at the Kafka default. This gateway accepts only this value.";
const MIN_INSYNC_REPLICAS_DOC: &str = "The majority of the replication factor, which Iggy needs \
                                       for every commit. This gateway accepts a value up to it.";
const FLUSH_DOC: &str = "A persisted topic flushes every write. Iggy sets durability only when it \
                         creates a topic, so this gateway accepts other values and does not apply \
                         them.";
const MAX_MESSAGE_BYTES_DOC: &str = "The largest record batch that this gateway takes in every \
                                     topic. It accepts a value from the Kafka default up to it.";
const BROKER_DOC: &str = "Fixed for this gateway, which cannot change broker configuration.";
const RETENTION_MS_DEFAULT_DOC: &str = "retention.ms of each topic the gateway creates without one. Set by \
     IGGY_KAFKA_TOPIC_RETENTION_MS.";
const DEFAULT_REPLICATION_FACTOR_DOC: &str = "The replication factor Iggy gives a topic that names \
                                              none. Read from Iggy. -1 until the gateway reads it.";
const BROKER_MIN_INSYNC_REPLICAS_DOC: &str = "The majority of default.replication.factor, which \
                                              Iggy needs for every commit.";
const BROKER_MESSAGE_MAX_BYTES_DOC: &str = "The largest record batch that this gateway takes.";
const RETENTION_BYTES_DEFAULT_DOC: &str = "retention.bytes of each topic the gateway creates without one. Set by \
     IGGY_KAFKA_TOPIC_RETENTION_BYTES.";

/// Kafka's `max.message.bytes` and `message.max.bytes` default: 1 MiB plus the batch log
/// overhead.
const KAFKA_MAX_MESSAGE_BYTES: i32 = 1_048_588;

/// Kafka `Records.SOFT_MAX_ARRAY_LENGTH`, the default and ceiling of
/// `max.decompressed.message.bytes`.
const SOFT_MAX_ARRAY_LENGTH: i32 = i32::MAX - 8;

/// Kafka `BrokerCompressionType` names, in Kafka's order.
const COMPRESSION_TYPES: &[&str] = &["uncompressed", "zstd", "lz4", "snappy", "gzip", "producer"];

/// Kafka `ConfigType`, the byte `DescribeConfigs` v3+ reports.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigType {
    Boolean,
    String,
    Int,
    Long,
    Double,
    List,
}

impl ConfigType {
    #[must_use]
    pub const fn code(self) -> i8 {
        match self {
            Self::Boolean => 1,
            Self::String => 2,
            Self::Int => 3,
            Self::Long => 5,
            Self::Double => 6,
            Self::List => 7,
        }
    }

    /// Parses `value` as Kafka's `ConfigDef.parseType` does: trimmed, a boolean in any case, and
    /// a list split at commas.
    ///
    /// # Errors
    ///
    /// Kafka's reason when `value` is not of this type.
    pub fn parse(self, value: &str) -> Result<ConfigValue, &'static str> {
        let trimmed = value.trim();
        match self {
            Self::Boolean if trimmed.eq_ignore_ascii_case("true") => Ok(ConfigValue::Boolean(true)),
            Self::Boolean if trimmed.eq_ignore_ascii_case("false") => {
                Ok(ConfigValue::Boolean(false))
            }
            Self::Boolean => Err("Expected value to be either true or false"),
            Self::String => Ok(ConfigValue::String(trimmed.to_string())),
            Self::Int => trimmed
                .parse::<i32>()
                .map(|number| ConfigValue::Long(i64::from(number)))
                .map_err(|_| "Not a number of type INT"),
            Self::Long => trimmed
                .parse::<i64>()
                .map(ConfigValue::Long)
                .map_err(|_| "Not a number of type LONG"),
            Self::Double => trimmed
                .parse::<f64>()
                .map(ConfigValue::Double)
                .map_err(|_| "Not a number of type DOUBLE"),
            Self::List if trimmed.is_empty() => Ok(ConfigValue::List(Vec::new())),
            Self::List => Ok(ConfigValue::List(
                trimmed
                    .split(',')
                    .map(|item| item.trim().to_string())
                    .collect(),
            )),
        }
    }
}

/// A configuration value parsed by its [`ConfigType`]. An `INT` parses to [`Self::Long`].
#[derive(Debug, Clone, PartialEq)]
pub enum ConfigValue {
    Boolean(bool),
    String(String),
    Long(i64),
    Double(f64),
    List(Vec<String>),
}

/// What Kafka 4.4 accepts for a key past its type: the validator `LogConfig` gives it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rule {
    /// Any value of the type.
    Any,
    /// `Range.atLeast(min)`.
    AtLeast(i32),
    /// `Range.between(min, max)`, for an integer or a double.
    Between(i32, i32),
    /// The gzip level: 1 to 9, or -1.
    GzipLevel,
    /// `ValidString.in`: one of these strings, case sensitive.
    OneOf(&'static [&'static str]),
    /// `ValidList.in`: each item one of these strings. The list may be empty, and Kafka drops a
    /// repeated item before it checks the list.
    ListOf(&'static [&'static str]),
    /// `ThrottledReplicaListValidator`: `*`, or `partition:broker` pairs.
    ThrottledReplicas,
}

/// Why Kafka refuses a value, in one of the two forms its `ConfigException` takes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refusal {
    /// "Invalid value {value} for configuration {name}: {reason}". `value` is what Kafka names:
    /// the value, one item of a list, or the list as Java prints it.
    Value { value: String, reason: String },
    /// A whole message that names no value.
    Message(String),
}

impl Refusal {
    fn value(value: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::Value {
            value: value.into(),
            reason: reason.into(),
        }
    }
}

/// What the gateway does with one topic key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopicKeyKind {
    /// Stored as the Iggy topic's `message_expiry`.
    RetentionMs,
    /// Stored as `max_topic_size`, the share of each partition times the partition count.
    RetentionBytes,
    /// Stored as `segment_size`, which Iggy fixes when it creates the topic.
    SegmentBytes,
    /// Accepted at any value, and not reported: Iggy has no option for it.
    CleanupPolicy,
    /// Reported as the majority of the replication factor, which Iggy needs for every commit,
    /// and accepted up to it.
    MinInsyncReplicas,
    /// `flush.messages` or `flush.ms`. `every_write`, a flush of every write, is Iggy's
    /// `persisted` durability. Any other value is accepted and not applied.
    Flush { every_write: i64 },
    /// Accepted from Kafka's default up to the largest batch that Produce takes, and reported as
    /// that largest batch.
    MaxMessageBytes,
    /// Accepted at any value and not applied, because no value changes what a client gets.
    /// Reported at Kafka's default.
    Ignored,
    /// Reported at Kafka's default, and accepted only at that value, for the reason given.
    Fixed { reason: &'static str },
}

/// One Kafka topic configuration key.
#[derive(Debug)]
pub struct TopicKey {
    pub name: &'static str,
    pub config_type: ConfigType,
    /// Kafka 4.4's default, as Kafka reports it. For a fixed key, the only value accepted.
    pub default: &'static str,
    pub kind: TopicKeyKind,
    pub rule: Rule,
}

impl TopicKey {
    /// `value` parsed and checked as Kafka checks this key.
    ///
    /// # Errors
    ///
    /// Kafka's refusal: `value` is not of the key's type, or breaks its rule.
    pub fn validate(&self, value: &str) -> Result<ConfigValue, Refusal> {
        let parsed = self
            .config_type
            .parse(value)
            .map_err(|reason| Refusal::value(value, reason))?;
        let parsed = match (self.rule, parsed) {
            (Rule::ListOf(_), ConfigValue::List(items)) => {
                let mut seen = HashSet::new();
                ConfigValue::List(
                    items
                        .into_iter()
                        .filter(|item| seen.insert(item.clone()))
                        .collect(),
                )
            }
            (_, parsed) => parsed,
        };
        self.check_rule(&parsed)?;
        Ok(parsed)
    }

    /// `value` as [`Self::validate`] checks it, and for a fixed key, the default.
    ///
    /// # Errors
    ///
    /// Kafka's refusal, or the gateway's reason when a fixed key names another value.
    pub fn check(&self, value: &str) -> Result<ConfigValue, Refusal> {
        let parsed = self.validate(value)?;
        if let TopicKeyKind::Fixed { reason } = self.kind
            && self.config_type.parse(self.default).ok().as_ref() != Some(&parsed)
        {
            return Err(Refusal::value(value.trim(), reason));
        }
        Ok(parsed)
    }

    /// The value an `APPEND` (`append`) or a `SUBTRACT` of `values` leaves from this key's
    /// default, as the comma list Kafka then checks, or `None` for a key that is not a list. As in
    /// Kafka, `APPEND` adds the values not yet present, and `SUBTRACT` removes the ones present.
    ///
    /// Each lookup is a set lookup, so a value with many items costs time in proportion to them.
    #[must_use]
    pub fn default_list_after(&self, append: bool, values: &str) -> Option<String> {
        let ConfigValue::List(mut list) = self.config_type.parse(self.default).ok()? else {
            return None;
        };
        let ConfigValue::List(values) = ConfigType::List.parse(values).ok()? else {
            return None;
        };
        if append {
            let mut present: HashSet<String> = list.iter().cloned().collect();
            for value in values {
                if present.insert(value.clone()) {
                    list.push(value);
                }
            }
        } else {
            let removed: HashSet<String> = values.into_iter().collect();
            list.retain(|item| !removed.contains(item));
        }
        Some(list.join(","))
    }

    /// This key's entry for a topic, or `None` for `cleanup.policy`, which is not reported.
    ///
    /// `retention.ms` and `retention.bytes` report a topic override as `DYNAMIC_TOPIC_CONFIG`,
    /// and a value equal to the gateway default as `STATIC_BROKER_CONFIG`, as Kafka reports a
    /// topic that inherits `log.retention.ms`. Iggy marks the stored default as set like any
    /// other, so an override equal to the default reports the same way: Kafka would report
    /// `DYNAMIC_TOPIC_CONFIG` there. `flush.messages` and `flush.ms` of a persisted topic report
    /// the same way against `IGGY_KAFKA_TOPIC_DURABILITY`.
    #[must_use]
    pub fn topic_entry(
        &self,
        topic: &TopicConfigSnapshot,
        context: &ConfigContext<'_>,
    ) -> Option<ConfigEntry> {
        let defaults = context.defaults;
        let entry = |value: Option<String>, read_only: bool, source: i8, documentation| {
            Some(ConfigEntry {
                name: self.name,
                value,
                read_only,
                source,
                config_type: self.config_type,
                documentation,
            })
        };
        match self.kind {
            TopicKeyKind::RetentionMs => {
                let value = retention_ms_of(topic.message_expiry);
                let source = stored_source(
                    topic.message_expiry_explicit,
                    defaults.expiry() == Some(topic.message_expiry),
                );
                entry(
                    value.map(|millis| millis.to_string()),
                    false,
                    source,
                    RETENTION_MS_DOC,
                )
            }
            TopicKeyKind::RetentionBytes => {
                let retention = topic.retention_bytes();
                let source = stored_source(
                    topic.max_topic_size_explicit,
                    defaults.retention() == Some(retention),
                );
                entry(
                    Some(retention.kafka_value().to_string()),
                    false,
                    source,
                    RETENTION_BYTES_DOC,
                )
            }
            TopicKeyKind::SegmentBytes => {
                let source = if topic.segment_size_explicit {
                    CONFIG_SOURCE_TOPIC
                } else {
                    CONFIG_SOURCE_DEFAULT
                };
                entry(
                    Some(topic.segment_size.to_string()),
                    true,
                    source,
                    SEGMENT_BYTES_DOC,
                )
            }
            TopicKeyKind::MinInsyncReplicas => {
                let value = context.replication_factor_of(topic).map_or_else(
                    || self.default.to_string(),
                    |factor| majority(factor).to_string(),
                );
                entry(
                    Some(value),
                    false,
                    CONFIG_SOURCE_DEFAULT,
                    MIN_INSYNC_REPLICAS_DOC,
                )
            }
            TopicKeyKind::Flush { every_write } => {
                let (value, source) = if topic.durability.is_persisted() {
                    let source = stored_source(true, defaults.durability.is_persisted());
                    (every_write.to_string(), source)
                } else {
                    (self.default.to_string(), CONFIG_SOURCE_DEFAULT)
                };
                entry(Some(value), false, source, FLUSH_DOC)
            }
            TopicKeyKind::MaxMessageBytes => entry(
                Some(context.max_message_bytes.to_string()),
                false,
                CONFIG_SOURCE_DEFAULT,
                MAX_MESSAGE_BYTES_DOC,
            ),
            TopicKeyKind::Ignored => entry(
                Some(self.default.to_string()),
                false,
                CONFIG_SOURCE_DEFAULT,
                IGNORED_DOC,
            ),
            TopicKeyKind::Fixed { .. } => entry(
                Some(self.default.to_string()),
                true,
                CONFIG_SOURCE_DEFAULT,
                FIXED_DOC,
            ),
            TopicKeyKind::CleanupPolicy => None,
        }
    }

    /// Checks `value`, already parsed and with repeats dropped, against this key's rule.
    fn check_rule(&self, value: &ConfigValue) -> Result<(), Refusal> {
        let shown = || match value {
            ConfigValue::Boolean(flag) => flag.to_string(),
            ConfigValue::String(text) => text.clone(),
            ConfigValue::Long(number) => number.to_string(),
            ConfigValue::Double(number) => number.to_string(),
            ConfigValue::List(items) => format!("[{}]", items.join(", ")),
        };
        match self.rule {
            Rule::Any => Ok(()),
            Rule::AtLeast(min) => {
                range(value, Some(min), None).map_err(|r| Refusal::value(shown(), r))
            }
            Rule::Between(min, max) => {
                range(value, Some(min), Some(max)).map_err(|r| Refusal::value(shown(), r))
            }
            Rule::GzipLevel => match value {
                ConfigValue::Long(level) if !(1..=9).contains(level) && *level != -1 => Err(
                    Refusal::value(shown(), "Value must be between 1 and 9 or equal to -1"),
                ),
                _ => Ok(()),
            },
            Rule::OneOf(allowed) => match value {
                ConfigValue::String(text) if !allowed.contains(&text.as_str()) => {
                    Err(Refusal::value(
                        text.clone(),
                        format!("String must be one of: {}", allowed.join(", ")),
                    ))
                }
                _ => Ok(()),
            },
            Rule::ListOf(allowed) => {
                let ConfigValue::List(items) = value else {
                    return Ok(());
                };
                for item in items {
                    if item.is_empty() {
                        return Err(Refusal::Message(format!(
                            "Configuration '{}' values must not be empty.",
                            self.name
                        )));
                    }
                    if !allowed.contains(&item.as_str()) {
                        return Err(Refusal::value(
                            item.clone(),
                            format!("String must be one of: {}", allowed.join(", ")),
                        ));
                    }
                }
                Ok(())
            }
            Rule::ThrottledReplicas => {
                let ConfigValue::List(items) = value else {
                    return Ok(());
                };
                let pairs = items
                    .iter()
                    .all(|item| item.is_empty() || is_replica_pair(item));
                if pairs || items.concat() == "*" {
                    Ok(())
                } else {
                    Err(Refusal::value(
                        shown(),
                        format!(
                            "{} must be the literal '*' or a list of replicas in the following \
                             format: [partitionId]:[brokerId],[partitionId]:[brokerId],...",
                            self.name
                        ),
                    ))
                }
            }
        }
    }
}

/// The source of a value a topic stores: `DEFAULT_CONFIG` when no client set it,
/// `STATIC_BROKER_CONFIG` when it equals the gateway default, else `DYNAMIC_TOPIC_CONFIG`.
const fn stored_source(explicit: bool, equals_default: bool) -> i8 {
    if !explicit {
        CONFIG_SOURCE_DEFAULT
    } else if equals_default {
        CONFIG_SOURCE_STATIC_BROKER
    } else {
        CONFIG_SOURCE_TOPIC
    }
}

/// Kafka's `Range` check, with its texts. A double compares with the bounds as doubles.
fn range(value: &ConfigValue, min: Option<i32>, max: Option<i32>) -> Result<(), String> {
    let compare = |bound: i32| match value {
        ConfigValue::Long(number) => number.cmp(&i64::from(bound)),
        ConfigValue::Double(number) => number
            .partial_cmp(&f64::from(bound))
            .unwrap_or(Ordering::Equal),
        _ => Ordering::Equal,
    };
    if let Some(min) = min
        && compare(min).is_lt()
    {
        return Err(format!("Value must be at least {min}"));
    }
    if let Some(max) = max
        && compare(max).is_gt()
    {
        return Err(format!("Value must be no more than {max}"));
    }
    Ok(())
}

/// `[0-9]+:[0-9]+`, the form of one throttled replica.
fn is_replica_pair(item: &str) -> bool {
    let digits = |part: &str| !part.is_empty() && part.bytes().all(|byte| byte.is_ascii_digit());
    item.split_once(':')
        .is_some_and(|(partition, broker)| digits(partition) && digits(broker))
}

const fn key(
    name: &'static str,
    config_type: ConfigType,
    default: &'static str,
    kind: TopicKeyKind,
    rule: Rule,
) -> TopicKey {
    TopicKey {
        name,
        config_type,
        default,
        kind,
        rule,
    }
}

const fn ignored(
    name: &'static str,
    config_type: ConfigType,
    default: &'static str,
    rule: Rule,
) -> TopicKey {
    key(name, config_type, default, TopicKeyKind::Ignored, rule)
}

/// Every topic key of Kafka 4.4 (`LogConfig`), sorted by name for [`topic_key`].
pub static TOPIC_KEYS: [TopicKey; 37] = [
    key(
        CLEANUP_POLICY,
        ConfigType::List,
        "delete",
        TopicKeyKind::CleanupPolicy,
        Rule::ListOf(&["compact", "delete"]),
    ),
    ignored(
        "compression.gzip.level",
        ConfigType::Int,
        "-1",
        Rule::GzipLevel,
    ),
    ignored(
        "compression.lz4.level",
        ConfigType::Int,
        "9",
        Rule::Between(1, 17),
    ),
    ignored(
        "compression.type",
        ConfigType::String,
        "producer",
        Rule::OneOf(COMPRESSION_TYPES),
    ),
    ignored(
        "compression.zstd.level",
        ConfigType::Int,
        "3",
        Rule::Between(-131_072, 22),
    ),
    ignored(
        "delete.retention.ms",
        ConfigType::Long,
        "86400000",
        Rule::AtLeast(0),
    ),
    ignored(
        "errors.deadletterqueue.group.enable",
        ConfigType::Boolean,
        "false",
        Rule::Any,
    ),
    ignored(
        "file.delete.delay.ms",
        ConfigType::Long,
        "60000",
        Rule::AtLeast(0),
    ),
    key(
        FLUSH_MESSAGES,
        ConfigType::Long,
        "9223372036854775807",
        TopicKeyKind::Flush { every_write: 1 },
        Rule::AtLeast(1),
    ),
    key(
        FLUSH_MS,
        ConfigType::Long,
        "9223372036854775807",
        TopicKeyKind::Flush { every_write: 0 },
        Rule::AtLeast(0),
    ),
    ignored(
        "follower.replication.throttled.replicas",
        ConfigType::List,
        "",
        Rule::ThrottledReplicas,
    ),
    ignored(
        "index.interval.bytes",
        ConfigType::Int,
        "4096",
        Rule::AtLeast(0),
    ),
    ignored(
        "leader.replication.throttled.replicas",
        ConfigType::List,
        "",
        Rule::ThrottledReplicas,
    ),
    ignored(
        "local.retention.bytes",
        ConfigType::Long,
        "-2",
        Rule::AtLeast(-2),
    ),
    ignored(
        "local.retention.ms",
        ConfigType::Long,
        "-2",
        Rule::AtLeast(-2),
    ),
    ignored(
        "max.compaction.lag.ms",
        ConfigType::Long,
        "9223372036854775807",
        Rule::AtLeast(1),
    ),
    ignored(
        "max.decompressed.message.bytes",
        ConfigType::Int,
        "2147483639",
        Rule::Between(1, SOFT_MAX_ARRAY_LENGTH),
    ),
    key(
        MAX_MESSAGE_BYTES,
        ConfigType::Int,
        "1048588",
        TopicKeyKind::MaxMessageBytes,
        Rule::AtLeast(0),
    ),
    ignored(
        "message.timestamp.after.max.ms",
        ConfigType::Long,
        "3600000",
        Rule::AtLeast(0),
    ),
    ignored(
        "message.timestamp.before.max.ms",
        ConfigType::Long,
        "9223372036854775807",
        Rule::AtLeast(0),
    ),
    key(
        "message.timestamp.type",
        ConfigType::String,
        "CreateTime",
        TopicKeyKind::Fixed {
            reason: "Iggy keeps the timestamp that the producer sets, so this gateway accepts \
                     only CreateTime",
        },
        Rule::OneOf(&["CreateTime", "LogAppendTime"]),
    ),
    ignored(
        "min.cleanable.dirty.ratio",
        ConfigType::Double,
        "0.5",
        Rule::Between(0, 1),
    ),
    ignored(
        "min.compaction.lag.ms",
        ConfigType::Long,
        "0",
        Rule::AtLeast(0),
    ),
    key(
        MIN_INSYNC_REPLICAS,
        ConfigType::Int,
        "1",
        TopicKeyKind::MinInsyncReplicas,
        Rule::AtLeast(1),
    ),
    ignored("preallocate", ConfigType::Boolean, "false", Rule::Any),
    ignored(
        "remote.copy.lag.bytes",
        ConfigType::Long,
        "-1",
        Rule::AtLeast(-1),
    ),
    ignored(
        "remote.copy.lag.ms",
        ConfigType::Long,
        "0",
        Rule::AtLeast(-1),
    ),
    ignored(
        "remote.log.copy.disable",
        ConfigType::Boolean,
        "false",
        Rule::Any,
    ),
    ignored(
        "remote.log.delete.on.disable",
        ConfigType::Boolean,
        "false",
        Rule::Any,
    ),
    ignored(
        "remote.storage.enable",
        ConfigType::Boolean,
        "false",
        Rule::Any,
    ),
    key(
        RETENTION_BYTES,
        ConfigType::Long,
        "-1",
        TopicKeyKind::RetentionBytes,
        Rule::Any,
    ),
    key(
        RETENTION_MS,
        ConfigType::Long,
        "604800000",
        TopicKeyKind::RetentionMs,
        Rule::AtLeast(-1),
    ),
    key(
        SEGMENT_BYTES,
        ConfigType::Int,
        "1073741824",
        TopicKeyKind::SegmentBytes,
        Rule::AtLeast(1024 * 1024),
    ),
    ignored(
        "segment.index.bytes",
        ConfigType::Int,
        "10485760",
        Rule::AtLeast(4),
    ),
    ignored("segment.jitter.ms", ConfigType::Long, "0", Rule::AtLeast(0)),
    ignored(
        "segment.ms",
        ConfigType::Long,
        "604800000",
        Rule::AtLeast(1),
    ),
    ignored(
        "unclean.leader.election.enable",
        ConfigType::Boolean,
        "false",
        Rule::Any,
    ),
];

/// The topic key named `name`, or `None` for a name Kafka 4.4 does not know.
#[must_use]
pub fn topic_key(name: &str) -> Option<&'static TopicKey> {
    TOPIC_KEYS
        .binary_search_by(|key| key.name.cmp(name))
        .ok()
        .map(|index| &TOPIC_KEYS[index])
}

/// One configuration entry, as `DescribeConfigs` and `CreateTopics` v5 report it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigEntry {
    pub name: &'static str,
    /// `None` when the stored value has no Kafka form.
    pub value: Option<String>,
    pub read_only: bool,
    pub source: i8,
    pub config_type: ConfigType,
    pub documentation: &'static str,
}

/// The majority of `replication_factor` nodes, the count Iggy needs for each commit.
#[must_use]
pub const fn majority(replication_factor: u32) -> u32 {
    replication_factor / 2 + 1
}

/// What some checks and reported values depend on beside the topic itself.
#[derive(Debug, Clone, Copy)]
pub struct ConfigContext<'a> {
    pub defaults: &'a TopicDefaults,
    /// The largest record batch that Produce takes, in bytes: the `max.message.bytes` of every
    /// topic.
    pub max_message_bytes: i32,
    /// The replication factor Iggy gives a topic that names none, when the bridge read it.
    pub default_replication_factor: Option<u32>,
}

impl<'a> ConfigContext<'a> {
    /// The context of `bridge` behind a listener that takes frames of up to `max_frame_size`
    /// bytes. A Produce batch must fit one frame and one Iggy send.
    #[must_use]
    pub fn of(bridge: &'a IggyBridge, max_frame_size: usize) -> Self {
        let frame = u64::try_from(max_frame_size).unwrap_or(u64::MAX);
        let largest = frame.min(bridge.max_send_bytes());
        Self {
            defaults: bridge.topic_defaults(),
            max_message_bytes: i32::try_from(largest).unwrap_or(i32::MAX),
            default_replication_factor: bridge.default_replication_factor(),
        }
    }

    /// The replication factor of `topic`, or the default for a topic created before Iggy stored
    /// one. `None` when neither is known.
    #[must_use]
    pub fn replication_factor_of(&self, topic: &TopicConfigSnapshot) -> Option<u32> {
        topic.replication_factor.or(self.default_replication_factor)
    }

    /// The smallest `max.message.bytes` the gateway accepts: Kafka's default, or the largest
    /// batch when that is smaller. A smaller value asks the topic to refuse batches that Produce
    /// takes.
    #[must_use]
    pub fn min_message_bytes(&self) -> i32 {
        KAFKA_MAX_MESSAGE_BYTES.min(self.max_message_bytes)
    }
}

/// The entries of a topic that `configuration_keys` asks for: every reported key for a null or
/// empty list, otherwise each reported key the list names, in request order and once each. A name
/// the gateway does not report, `cleanup.policy` among them, is left out.
#[must_use]
pub fn topic_entries(
    topic: &TopicConfigSnapshot,
    context: &ConfigContext<'_>,
    configuration_keys: Option<&[StrBytes]>,
) -> Vec<ConfigEntry> {
    configuration_keys
        .filter(|keys| !keys.is_empty())
        .map_or_else(
            || {
                TOPIC_KEYS
                    .iter()
                    .filter_map(|key| key.topic_entry(topic, context))
                    .collect()
            },
            |names| {
                distinct(names)
                    .filter_map(|name| topic_key(name)?.topic_entry(topic, context))
                    .collect()
            },
        )
}

/// What the broker entries report about this gateway.
#[derive(Debug, Clone, Copy)]
pub struct BrokerContext<'a> {
    pub node_id: i32,
    pub auto_create_topics: bool,
    pub config: ConfigContext<'a>,
}

/// The broker entries `configuration_keys` asks for, by the same rule as [`topic_entries`].
///
/// The Strimzi Topic Operator reads `auto.create.topics.enable` and `min.insync.replicas` from
/// every broker at startup and stops when either is missing. kafka-ui and AKHQ show the rest.
/// Every entry is read only: the gateway changes no broker configuration.
#[must_use]
pub fn broker_entries(
    context: &BrokerContext<'_>,
    configuration_keys: Option<&[StrBytes]>,
) -> Vec<ConfigEntry> {
    let all = all_broker_entries(context);
    let Some(names) = configuration_keys.filter(|keys| !keys.is_empty()) else {
        return all;
    };
    distinct(names)
        .filter_map(|name| all.iter().find(|entry| entry.name == name).cloned())
        .collect()
}

/// Every broker entry. A value the gateway has not read yet is reported as Kafka's default from
/// `DEFAULT_CONFIG`, except `default.replication.factor`, which is -1 then. Debezium creates its
/// schema history topic with the factor it reads there, and a factor of -1 takes the Iggy
/// default, where a made-up 1 would keep one copy.
fn all_broker_entries(context: &BrokerContext<'_>) -> Vec<ConfigEntry> {
    let node_id = context.node_id.to_string();
    let defaults = context.config.defaults;
    let default_replication_factor = context.config.default_replication_factor;
    vec![
        ConfigEntry {
            source: if context.auto_create_topics {
                CONFIG_SOURCE_STATIC_BROKER
            } else {
                CONFIG_SOURCE_DEFAULT
            },
            ..broker_entry(
                "auto.create.topics.enable",
                ConfigType::Boolean,
                if context.auto_create_topics {
                    "true"
                } else {
                    "false"
                },
            )
        },
        ConfigEntry {
            source: CONFIG_SOURCE_STATIC_BROKER,
            ..broker_entry("broker.id", ConfigType::Int, &node_id)
        },
        broker_entry("compression.type", ConfigType::String, "producer"),
        configured_entry(
            ("default.replication.factor", ConfigType::Int),
            default_replication_factor.map(|factor| factor.to_string()),
            "-1",
            DEFAULT_REPLICATION_FACTOR_DOC,
        ),
        broker_entry("delete.topic.enable", ConfigType::Boolean, "true"),
        broker_entry("log.cleanup.policy", ConfigType::List, "delete"),
        broker_entry(
            "log.message.timestamp.type",
            ConfigType::String,
            "CreateTime",
        ),
        configured_entry(
            ("log.retention.bytes", ConfigType::Long),
            defaults.retention_bytes.map(|bytes| bytes.to_string()),
            "-1",
            RETENTION_BYTES_DEFAULT_DOC,
        ),
        configured_entry(
            ("log.retention.ms", ConfigType::Long),
            defaults.retention_ms.map(|millis| millis.to_string()),
            "-1",
            RETENTION_MS_DEFAULT_DOC,
        ),
        broker_entry("log.segment.bytes", ConfigType::Int, "1073741824"),
        ConfigEntry {
            documentation: BROKER_MESSAGE_MAX_BYTES_DOC,
            ..broker_entry(
                "message.max.bytes",
                ConfigType::Int,
                &context.config.max_message_bytes.to_string(),
            )
        },
        configured_entry(
            (MIN_INSYNC_REPLICAS, ConfigType::Int),
            default_replication_factor.map(|factor| majority(factor).to_string()),
            "1",
            BROKER_MIN_INSYNC_REPLICAS_DOC,
        ),
        ConfigEntry {
            source: CONFIG_SOURCE_STATIC_BROKER,
            ..broker_entry("node.id", ConfigType::Int, &node_id)
        },
        broker_entry(
            "num.partitions",
            ConfigType::Int,
            &DEFAULT_TOPIC_PARTITIONS.to_string(),
        ),
        broker_entry(
            "unclean.leader.election.enable",
            ConfigType::Boolean,
            "false",
        ),
    ]
}

/// A read-only broker entry with a fixed value, from `DEFAULT_CONFIG`.
fn broker_entry(name: &'static str, config_type: ConfigType, value: &str) -> ConfigEntry {
    ConfigEntry {
        name,
        value: Some(value.to_string()),
        read_only: true,
        source: CONFIG_SOURCE_DEFAULT,
        config_type,
        documentation: BROKER_DOC,
    }
}

/// A read-only broker entry that the gateway learns or an operator sets: `value` from
/// `STATIC_BROKER_CONFIG` when there is one, else `fallback` from `DEFAULT_CONFIG`.
fn configured_entry(
    (name, config_type): (&'static str, ConfigType),
    value: Option<String>,
    fallback: &str,
    documentation: &'static str,
) -> ConfigEntry {
    ConfigEntry {
        name,
        source: if value.is_some() {
            CONFIG_SOURCE_STATIC_BROKER
        } else {
            CONFIG_SOURCE_DEFAULT
        },
        value: Some(value.unwrap_or_else(|| fallback.to_string())),
        read_only: true,
        config_type,
        documentation,
    }
}

/// `names` in request order, each once.
fn distinct(names: &[StrBytes]) -> impl Iterator<Item = &str> {
    let mut seen = HashSet::new();
    names
        .iter()
        .map(StrBytes::as_str)
        .filter(move |name| seen.insert(*name))
}

#[cfg(test)]
mod tests {
    use iggy::prelude::{Durability, IggyByteSize, IggyExpiry, MaxTopicSize};

    use super::*;
    use crate::bridge::topic_settings::expiry_from_retention_ms;

    const GIB: u64 = 1024 * 1024 * 1024;

    /// The largest batch of a gateway with the default 8 MiB frame.
    const LARGEST_BATCH: i32 = 8 * 1024 * 1024;

    fn snapshot() -> TopicConfigSnapshot {
        TopicConfigSnapshot {
            partitions_count: 2,
            message_expiry: IggyExpiry::NeverExpire,
            message_expiry_explicit: false,
            max_topic_size: MaxTopicSize::Unlimited,
            max_topic_size_explicit: false,
            segment_size: GIB,
            segment_size_explicit: false,
            replication_factor: None,
            durability: Durability::Replicated,
        }
    }

    fn context(defaults: &TopicDefaults) -> ConfigContext<'_> {
        ConfigContext {
            defaults,
            max_message_bytes: LARGEST_BATCH,
            default_replication_factor: None,
        }
    }

    fn value_of<'a>(entries: &'a [ConfigEntry], name: &str) -> Option<&'a ConfigEntry> {
        entries.iter().find(|entry| entry.name == name)
    }

    fn known(name: &str) -> &'static TopicKey {
        topic_key(name).expect("a Kafka 4.4 topic key")
    }

    fn refused(name: &str, value: &str) -> Refusal {
        known(name).check(value).expect_err("refused")
    }

    #[test]
    fn given_the_topic_key_table_should_be_sorted_and_unique_for_the_binary_search() {
        for pair in TOPIC_KEYS.windows(2) {
            assert!(
                pair[0].name < pair[1].name,
                "{} >= {}",
                pair[0].name,
                pair[1].name
            );
        }
        for key in &TOPIC_KEYS {
            assert!(std::ptr::eq(topic_key(key.name).expect("found"), key));
            assert!(
                key.check(key.default).is_ok(),
                "{} refuses its own default",
                key.name
            );
        }
        assert!(topic_key("no.such").is_none());
        assert!(topic_key("message.format.version").is_none());
    }

    #[test]
    fn given_a_null_key_list_should_report_every_key_but_cleanup_policy() {
        let defaults = TopicDefaults::default();
        let entries = topic_entries(&snapshot(), &context(&defaults), None);
        assert_eq!(entries.len(), TOPIC_KEYS.len() - 1);
        assert!(value_of(&entries, CLEANUP_POLICY).is_none());
        let min_isr = value_of(&entries, MIN_INSYNC_REPLICAS).expect("reported");
        assert_eq!(
            min_isr.value.as_deref(),
            Some("1"),
            "Kafka's default while the factor is unknown"
        );
        assert_eq!(min_isr.source, CONFIG_SOURCE_DEFAULT);
        assert!(!min_isr.read_only, "accepted up to the majority");
        let largest = value_of(&entries, MAX_MESSAGE_BYTES).expect("reported");
        assert_eq!(largest.value.as_deref(), Some("8388608"));
        assert!(!largest.read_only, "accepted up to the largest batch");
        let flush = value_of(&entries, FLUSH_MESSAGES).expect("reported");
        assert_eq!(flush.value.as_deref(), Some("9223372036854775807"));
        assert_eq!(flush.source, CONFIG_SOURCE_DEFAULT);
        let timestamps = value_of(&entries, "message.timestamp.type").expect("reported");
        assert!(timestamps.read_only, "accepted only at the default");
        let retention = value_of(&entries, RETENTION_MS).expect("reported");
        assert_eq!(retention.value.as_deref(), Some("-1"));
        assert_eq!(retention.source, CONFIG_SOURCE_DEFAULT);
        assert!(!retention.read_only);
        let bytes = value_of(&entries, RETENTION_BYTES).expect("reported");
        assert_eq!(bytes.value.as_deref(), Some("-1"));
        let segment = value_of(&entries, SEGMENT_BYTES).expect("reported");
        assert_eq!(segment.value.as_deref(), Some("1073741824"));
        assert_eq!(segment.source, CONFIG_SOURCE_DEFAULT);
        assert!(segment.read_only);
    }

    #[test]
    fn given_named_keys_should_report_the_known_ones_in_request_order_once() {
        let asked = [
            StrBytes::from_static_str(SEGMENT_BYTES),
            StrBytes::from_static_str("no.such"),
            StrBytes::from_static_str(CLEANUP_POLICY),
            StrBytes::from_static_str(RETENTION_MS),
            StrBytes::from_static_str(SEGMENT_BYTES),
        ];
        let defaults = TopicDefaults::default();
        let entries = topic_entries(&snapshot(), &context(&defaults), Some(&asked));
        let names: Vec<&str> = entries.iter().map(|entry| entry.name).collect();
        assert_eq!(names, [SEGMENT_BYTES, RETENTION_MS]);
    }

    #[test]
    fn given_an_empty_key_list_should_report_every_key_as_a_null_one_does() {
        let defaults = TopicDefaults::default();
        let empty: [StrBytes; 0] = [];
        assert_eq!(
            topic_entries(&snapshot(), &context(&defaults), Some(&empty)),
            topic_entries(&snapshot(), &context(&defaults), None)
        );
        let broker = BrokerContext {
            node_id: 1,
            auto_create_topics: false,
            config: context(&defaults),
        };
        assert_eq!(
            broker_entries(&broker, Some(&empty)),
            broker_entries(&broker, None)
        );
    }

    #[test]
    fn given_a_replication_factor_should_report_its_majority_as_min_insync_replicas() {
        let defaults = TopicDefaults::default();
        let min_isr = |topic: &TopicConfigSnapshot, default_factor: Option<u32>| {
            let context = ConfigContext {
                default_replication_factor: default_factor,
                ..context(&defaults)
            };
            let entries = topic_entries(topic, &context, None);
            value_of(&entries, MIN_INSYNC_REPLICAS)
                .expect("reported")
                .value
                .clone()
        };
        let stored = |factor| TopicConfigSnapshot {
            replication_factor: Some(factor),
            ..snapshot()
        };
        assert_eq!(min_isr(&stored(1), None).as_deref(), Some("1"));
        assert_eq!(min_isr(&stored(2), None).as_deref(), Some("2"));
        assert_eq!(min_isr(&stored(3), Some(5)).as_deref(), Some("2"));
        assert_eq!(
            min_isr(&snapshot(), Some(5)).as_deref(),
            Some("3"),
            "a topic without a stored factor has the default"
        );
        assert_eq!(
            (1..=6).map(majority).collect::<Vec<_>>(),
            [1, 2, 2, 3, 3, 4]
        );
    }

    #[test]
    fn given_a_persisted_topic_should_report_a_flush_of_every_write() {
        let persisted = TopicConfigSnapshot {
            durability: Durability::Persisted,
            ..snapshot()
        };
        let replicated_default = TopicDefaults::default();
        let entries = topic_entries(&persisted, &context(&replicated_default), None);
        let messages = value_of(&entries, FLUSH_MESSAGES).expect("reported");
        assert_eq!(messages.value.as_deref(), Some("1"));
        assert_eq!(messages.source, CONFIG_SOURCE_TOPIC);
        let millis = value_of(&entries, FLUSH_MS).expect("reported");
        assert_eq!(millis.value.as_deref(), Some("0"));

        let persisted_default = TopicDefaults {
            durability: Durability::Persisted,
            ..TopicDefaults::default()
        };
        let entries = topic_entries(&persisted, &context(&persisted_default), None);
        let messages = value_of(&entries, FLUSH_MESSAGES).expect("reported");
        assert_eq!(
            messages.source, CONFIG_SOURCE_STATIC_BROKER,
            "a topic that took the gateway default"
        );
    }

    #[test]
    fn given_the_largest_batch_should_report_it_and_take_kafkas_default_as_the_least() {
        let defaults = TopicDefaults::default();
        let entries = topic_entries(&snapshot(), &context(&defaults), None);
        let largest = value_of(&entries, MAX_MESSAGE_BYTES).expect("reported");
        assert_eq!(largest.value.as_deref(), Some("8388608"));
        assert_eq!(
            context(&defaults).min_message_bytes(),
            KAFKA_MAX_MESSAGE_BYTES
        );
        let small_frames = ConfigContext {
            max_message_bytes: 512 * 1024,
            ..context(&defaults)
        };
        assert_eq!(small_frames.min_message_bytes(), 512 * 1024);
        assert!(matches!(
            refused(MAX_MESSAGE_BYTES, "-1"),
            Refusal::Value { reason, .. } if reason == "Value must be at least 0"
        ));
    }

    #[test]
    fn given_stored_settings_should_report_them_as_topic_overrides() {
        let topic = TopicConfigSnapshot {
            message_expiry: expiry_from_retention_ms(120_000),
            message_expiry_explicit: true,
            max_topic_size: MaxTopicSize::Custom(IggyByteSize::from(6 * GIB)),
            max_topic_size_explicit: true,
            segment_size: 64 * 1024 * 1024,
            segment_size_explicit: true,
            ..snapshot()
        };
        let defaults = TopicDefaults::default();
        let entries = topic_entries(&topic, &context(&defaults), None);
        let retention = value_of(&entries, RETENTION_MS).expect("reported");
        assert_eq!(retention.value.as_deref(), Some("120000"));
        assert_eq!(retention.source, CONFIG_SOURCE_TOPIC);
        let bytes = value_of(&entries, RETENTION_BYTES).expect("reported");
        assert_eq!(bytes.value.as_deref(), Some(&*(3 * GIB).to_string()));
        assert_eq!(bytes.source, CONFIG_SOURCE_TOPIC);
        let segment = value_of(&entries, SEGMENT_BYTES).expect("reported");
        assert_eq!(segment.value.as_deref(), Some("67108864"));
        assert_eq!(segment.source, CONFIG_SOURCE_TOPIC);
    }

    #[test]
    fn given_a_gateway_default_should_report_a_value_equal_to_it_as_a_broker_setting() {
        let defaults = TopicDefaults {
            retention_ms: Some(604_800_000),
            retention_bytes: Some(2 * GIB),
            ..TopicDefaults::default()
        };
        let topic = TopicConfigSnapshot {
            message_expiry: expiry_from_retention_ms(604_800_000),
            message_expiry_explicit: true,
            max_topic_size: MaxTopicSize::Custom(IggyByteSize::from(4 * GIB)),
            max_topic_size_explicit: true,
            ..snapshot()
        };
        let entries = topic_entries(&topic, &context(&defaults), None);
        let retention = value_of(&entries, RETENTION_MS).expect("reported");
        assert_eq!(retention.source, CONFIG_SOURCE_STATIC_BROKER);
        let bytes = value_of(&entries, RETENTION_BYTES).expect("reported");
        assert_eq!(bytes.source, CONFIG_SOURCE_STATIC_BROKER);

        let other = TopicConfigSnapshot {
            message_expiry: expiry_from_retention_ms(1_000),
            ..topic
        };
        let entries = topic_entries(&other, &context(&defaults), None);
        let retention = value_of(&entries, RETENTION_MS).expect("reported");
        assert_eq!(retention.source, CONFIG_SOURCE_TOPIC);
    }

    #[test]
    fn given_an_expiry_without_a_millisecond_form_should_report_no_value() {
        let topic = TopicConfigSnapshot {
            message_expiry: IggyExpiry::from(1_500_u64),
            message_expiry_explicit: true,
            ..snapshot()
        };
        let defaults = TopicDefaults::default();
        let entries = topic_entries(&topic, &context(&defaults), None);
        assert_eq!(
            value_of(&entries, RETENTION_MS).expect("reported").value,
            None
        );
    }

    #[test]
    fn given_kafka_values_should_parse_as_kafka_does() {
        assert_eq!(
            ConfigType::Boolean.parse(" TRUE "),
            Ok(ConfigValue::Boolean(true))
        );
        assert!(ConfigType::Boolean.parse("yes").is_err());
        assert_eq!(ConfigType::Int.parse("+0001"), Ok(ConfigValue::Long(1)));
        assert!(ConfigType::Int.parse("2147483648").is_err());
        assert_eq!(
            ConfigType::Long.parse("9223372036854775807"),
            Ok(ConfigValue::Long(i64::MAX))
        );
        assert_eq!(
            ConfigType::Double.parse("0.50"),
            Ok(ConfigValue::Double(0.5))
        );
        assert_eq!(
            ConfigType::List.parse(" "),
            Ok(ConfigValue::List(Vec::new()))
        );
        assert_eq!(
            ConfigType::List.parse("delete , compact"),
            Ok(ConfigValue::List(vec![
                "delete".to_string(),
                "compact".to_string()
            ]))
        );
    }

    #[test]
    fn given_an_ignored_key_should_accept_any_value_kafka_accepts() {
        assert!(known(MIN_INSYNC_REPLICAS).check("2").is_ok());
        assert!(known("compression.type").check(" lz4 ").is_ok());
        assert!(known("compression.gzip.level").check("-1").is_ok());
        assert!(known("compression.gzip.level").check("9").is_ok());
        assert!(known("compression.zstd.level").check("-131072").is_ok());
        assert!(known("min.cleanable.dirty.ratio").check("1.0").is_ok());
        assert!(
            known("unclean.leader.election.enable")
                .check("TRUE")
                .is_ok()
        );
        assert!(known("segment.ms").check("1").is_ok());
        assert!(known("remote.copy.lag.ms").check("-1").is_ok());
        for throttled in ["*", "0:1, 1:2", "", "0:1,"] {
            assert!(
                known("leader.replication.throttled.replicas")
                    .check(throttled)
                    .is_ok(),
                "{throttled:?}"
            );
        }
    }

    #[test]
    fn given_a_value_kafka_refuses_should_answer_with_kafka_text() {
        let value = |value: &str, reason: &str| Refusal::Value {
            value: value.to_string(),
            reason: reason.to_string(),
        };
        assert_eq!(
            refused(MIN_INSYNC_REPLICAS, "0"),
            value("0", "Value must be at least 1")
        );
        assert_eq!(
            refused(MIN_INSYNC_REPLICAS, "two"),
            value("two", "Not a number of type INT")
        );
        assert_eq!(
            refused("compression.lz4.level", "18"),
            value("18", "Value must be no more than 17")
        );
        assert_eq!(
            refused("compression.gzip.level", "0"),
            value("0", "Value must be between 1 and 9 or equal to -1")
        );
        assert_eq!(
            refused("min.cleanable.dirty.ratio", "1.5"),
            value("1.5", "Value must be no more than 1")
        );
        assert_eq!(
            refused("compression.type", "LZ4"),
            value(
                "LZ4",
                "String must be one of: uncompressed, zstd, lz4, snappy, gzip, producer"
            )
        );
        assert_eq!(
            refused("segment.ms", "0"),
            value("0", "Value must be at least 1")
        );
        assert_eq!(
            refused("leader.replication.throttled.replicas", "*, 0:1"),
            value(
                "[*, 0:1]",
                "leader.replication.throttled.replicas must be the literal '*' or a list of \
                 replicas in the following format: [partitionId]:[brokerId],[partitionId]:\
                 [brokerId],..."
            )
        );
    }

    #[test]
    fn given_a_fixed_key_should_accept_only_its_default() {
        assert!(known("message.timestamp.type").check("CreateTime").is_ok());
        assert_eq!(
            refused("message.timestamp.type", "LogAppendTime"),
            Refusal::Value {
                value: "LogAppendTime".to_string(),
                reason: "Iggy keeps the timestamp that the producer sets, so this gateway \
                         accepts only CreateTime"
                    .to_string(),
            }
        );
        assert!(matches!(
            refused("message.timestamp.type", "createtime"),
            Refusal::Value { reason, .. } if reason.starts_with("String must be one of")
        ));
    }

    #[test]
    fn given_a_cleanup_policy_should_check_it_as_a_kafka_list() {
        let cleanup = known(CLEANUP_POLICY);
        assert_eq!(
            cleanup.check("compact, delete, compact"),
            Ok(ConfigValue::List(vec![
                "compact".to_string(),
                "delete".to_string()
            ]))
        );
        assert_eq!(cleanup.check(""), Ok(ConfigValue::List(Vec::new())));
        assert_eq!(
            cleanup.check("Compact").expect_err("case sensitive"),
            Refusal::Value {
                value: "Compact".to_string(),
                reason: "String must be one of: compact, delete".to_string(),
            }
        );
        assert_eq!(
            cleanup.check("compact,").expect_err("an empty item"),
            Refusal::Message(
                "Configuration 'cleanup.policy' values must not be empty.".to_string()
            )
        );
    }

    #[test]
    fn given_append_and_subtract_should_apply_to_the_default_list() {
        let cleanup = known(CLEANUP_POLICY);
        assert_eq!(
            cleanup.default_list_after(true, "delete").as_deref(),
            Some("delete")
        );
        assert_eq!(
            cleanup.default_list_after(true, "compact").as_deref(),
            Some("delete,compact")
        );
        assert_eq!(
            cleanup.default_list_after(false, "compact").as_deref(),
            Some("delete")
        );
        assert_eq!(
            cleanup.default_list_after(false, "delete").as_deref(),
            Some("")
        );
        assert_eq!(
            known("leader.replication.throttled.replicas")
                .default_list_after(true, "0:1")
                .as_deref(),
            Some("0:1")
        );
        assert_eq!(
            known("leader.replication.throttled.replicas")
                .default_list_after(true, "0:1, 1:2, 0:1, 1:2")
                .as_deref(),
            Some("0:1,1:2"),
            "an item already present is not added again"
        );
        assert!(known(RETENTION_MS).default_list_after(true, "1").is_none());
    }

    #[test]
    fn given_an_append_of_many_items_should_keep_each_once_in_request_order() {
        let items: Vec<String> = (0..20_000).map(|item| format!("{item}:1")).collect();
        let appended = known("follower.replication.throttled.replicas")
            .default_list_after(true, &items.join(","))
            .expect("a list key");
        assert_eq!(appended, items.join(","));
        let doubled = format!("{appended},{appended}");
        let subtracted = known("follower.replication.throttled.replicas")
            .default_list_after(false, &doubled)
            .expect("a list key");
        assert_eq!(subtracted, "");
    }

    #[test]
    fn given_a_broker_should_report_the_keys_strimzi_reads_and_the_gateway_defaults() {
        let defaults = TopicDefaults {
            retention_ms: Some(86_400_000),
            ..TopicDefaults::default()
        };
        let context = BrokerContext {
            node_id: 3,
            auto_create_topics: false,
            config: ConfigContext {
                default_replication_factor: Some(3),
                ..context(&defaults)
            },
        };
        let entries = broker_entries(&context, None);
        for pair in entries.windows(2) {
            assert!(pair[0].name < pair[1].name);
        }
        assert!(entries.iter().all(|entry| entry.read_only));
        let auto_create = value_of(&entries, "auto.create.topics.enable").expect("reported");
        assert_eq!(auto_create.value.as_deref(), Some("false"));
        let min_isr = value_of(&entries, MIN_INSYNC_REPLICAS).expect("reported");
        assert_eq!(
            min_isr.value.as_deref(),
            Some("2"),
            "the majority of the default factor"
        );
        assert_eq!(min_isr.source, CONFIG_SOURCE_STATIC_BROKER);
        let largest = value_of(&entries, "message.max.bytes").expect("reported");
        assert_eq!(largest.value.as_deref(), Some("8388608"));
        let node = value_of(&entries, "node.id").expect("reported");
        assert_eq!(node.value.as_deref(), Some("3"));
        let retention = value_of(&entries, "log.retention.ms").expect("reported");
        assert_eq!(retention.value.as_deref(), Some("86400000"));
        assert_eq!(retention.source, CONFIG_SOURCE_STATIC_BROKER);
        let bytes = value_of(&entries, "log.retention.bytes").expect("reported");
        assert_eq!(bytes.value.as_deref(), Some("-1"));
        assert_eq!(bytes.source, CONFIG_SOURCE_DEFAULT);
        let factor = value_of(&entries, "default.replication.factor").expect("reported");
        assert_eq!(factor.value.as_deref(), Some("3"));
        assert_eq!(factor.source, CONFIG_SOURCE_STATIC_BROKER);

        let asked = [
            StrBytes::from_static_str(MIN_INSYNC_REPLICAS),
            StrBytes::from_static_str("inter.broker.protocol.version"),
        ];
        let entries = broker_entries(&context, Some(&asked));
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, MIN_INSYNC_REPLICAS);
    }

    #[test]
    fn given_a_default_factor_not_read_yet_should_report_minus_one_not_a_made_up_one() {
        let defaults = TopicDefaults::default();
        let context = BrokerContext {
            node_id: 1,
            auto_create_topics: false,
            config: context(&defaults),
        };
        let entries = broker_entries(&context, None);
        let factor = value_of(&entries, "default.replication.factor").expect("reported");
        assert_eq!(
            factor.value.as_deref(),
            Some("-1"),
            "Debezium creates its history topic with the factor it reads here"
        );
        assert_eq!(factor.source, CONFIG_SOURCE_DEFAULT);
        let min_isr = value_of(&entries, MIN_INSYNC_REPLICAS).expect("reported");
        assert_eq!(min_isr.value.as_deref(), Some("1"));
        assert_eq!(min_isr.source, CONFIG_SOURCE_DEFAULT);
    }
}
