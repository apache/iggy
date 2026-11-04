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

//! How Kafka topic settings map onto Iggy topic options.
//!
//! - `retention.ms` is `message_expiry`. Kafka counts milliseconds, Iggy microseconds.
//! - `retention.bytes` caps one partition in Kafka. Iggy's `max_topic_size` caps the whole topic
//!   and gives each partition an equal share, so the gateway stores `retention.bytes` times the
//!   partition count, and reports the share back. Iggy refuses a topic cap below one segment, so a
//!   smaller product is stored as one segment.
//! - `segment.bytes` is `segment_size`, which Iggy fixes when it creates the topic.
//! - `cleanup.policy` decides whether old records go. Kafka deletes them only under `delete`, so
//!   a topic without it gets no expiry and no size cap. Iggy does not compact (#4521), and the
//!   gateway does not store the policy: Iggy has no option for it yet.
//! - `flush.messages=1` and `flush.ms=0` ask for a flush of every write, which a new topic gets as
//!   `persisted` durability.
//! - The Kafka replication factor is Iggy's `replication_factor` (#4692). -1 takes the server
//!   default.

use std::fmt;
use std::str::FromStr;

use iggy::prelude::{
    Durability, HeaderKey, IggyByteSize, IggyDuration, IggyExpiry, MaxTopicSize, OptionKey,
    ResourceOptions, TopicCreateOptions, TopicResolvedOptions, TopicUpdateOptions,
};
use iggy_common::{DEFAULT_SEGMENT_SIZE, MAX_TOPIC_SEGMENT_SIZE};

/// The microseconds Iggy stores for `retention.ms=0`. Iggy reads an expiry of 0 as "use the
/// default", so the shortest expiry it can store stands for 0, and reads back as 0.
const ZERO_RETENTION_MICROS: u64 = 1;

/// The Iggy expiry of a Kafka `retention.ms` of -1 or more.
///
/// -1 never expires. So does a count too large for Iggy's microsecond counter (more than half a
/// million years), which then reads back as -1.
#[must_use]
pub fn expiry_from_retention_ms(retention_ms: i64) -> IggyExpiry {
    match u64::try_from(retention_ms) {
        Err(_) => IggyExpiry::NeverExpire,
        Ok(0) => IggyExpiry::ExpireDuration(IggyDuration::from(ZERO_RETENTION_MICROS)),
        Ok(millis) => millis
            .checked_mul(1_000)
            .filter(|micros| *micros < u64::MAX)
            .map_or(IggyExpiry::NeverExpire, |micros| {
                IggyExpiry::ExpireDuration(IggyDuration::from(micros))
            }),
    }
}

/// The Kafka `retention.ms` of an Iggy expiry, or `None` when the expiry is not a whole number of
/// milliseconds. A rounded count would describe another value than the one Iggy stored.
#[must_use]
pub fn retention_ms_of(expiry: IggyExpiry) -> Option<i64> {
    match expiry {
        IggyExpiry::NeverExpire | IggyExpiry::ServerDefault => Some(-1),
        IggyExpiry::ExpireDuration(duration) => {
            let micros = duration.as_micros();
            if micros == ZERO_RETENTION_MICROS {
                return Some(0);
            }
            if micros == 0 || !micros.is_multiple_of(1_000) {
                return None;
            }
            i64::try_from(micros / 1_000).ok()
        }
    }
}

/// Kafka `retention.bytes`: a cap on each partition, or none.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetentionBytes {
    Unlimited,
    PerPartition(u64),
}

impl RetentionBytes {
    /// The Iggy `max_topic_size` that gives each of `partitions` partitions this cap, and at least
    /// `floor`.
    ///
    /// The floor is the segment size of the topic, because Iggy refuses a topic cap below one
    /// segment. Iggy keeps the newest sealed segment of each partition whatever the cap, so a
    /// raise to the floor deletes nothing that a smaller cap would keep. It also keeps 0 off
    /// Iggy's `ServerDefault` sentinel. A product past Iggy's byte counter caps nothing in
    /// practice, so it is stored as unlimited.
    #[must_use]
    pub fn topic_size(self, partitions: u32, floor: u64) -> MaxTopicSize {
        match self {
            Self::Unlimited => MaxTopicSize::Unlimited,
            Self::PerPartition(bytes) => bytes
                .checked_mul(u64::from(partitions.max(1)))
                .map(|total| total.max(floor).max(1))
                .filter(|total| *total < u64::MAX)
                .map_or(MaxTopicSize::Unlimited, |total| {
                    MaxTopicSize::Custom(IggyByteSize::from(total))
                }),
        }
    }

    /// Each partition's share of `topic_size`, rounded down. Iggy splits the cap evenly.
    #[must_use]
    pub fn share_of(topic_size: MaxTopicSize, partitions: u32) -> Self {
        match topic_size {
            MaxTopicSize::Custom(size) => {
                Self::PerPartition(size.as_bytes_u64() / u64::from(partitions.max(1)))
            }
            MaxTopicSize::Unlimited | MaxTopicSize::ServerDefault => Self::Unlimited,
        }
    }

    /// The value Kafka reports: -1 for no cap.
    #[must_use]
    pub fn kafka_value(self) -> i64 {
        match self {
            Self::Unlimited => -1,
            Self::PerPartition(bytes) => i64::try_from(bytes).unwrap_or(i64::MAX),
        }
    }
}

/// Settings of the topics the gateway creates, for each one a request leaves out: Kafka's broker
/// defaults `log.retention.ms` and `log.retention.bytes`, and the Iggy durability.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TopicDefaults {
    /// `IGGY_KAFKA_TOPIC_RETENTION_MS`, a positive count. `None` never expires.
    pub retention_ms: Option<u64>,
    /// `IGGY_KAFKA_TOPIC_RETENTION_BYTES`, per partition. `None` caps nothing.
    pub retention_bytes: Option<u64>,
    /// `IGGY_KAFKA_TOPIC_DURABILITY`, for the messages and the consumer offsets of a topic.
    pub durability: Durability,
}

impl TopicDefaults {
    /// The `IGGY_KAFKA_*` vars [`Self::from_env`] reads.
    pub const KNOWN_ENV_VARS: &'static [&'static str] = &[
        "IGGY_KAFKA_TOPIC_RETENTION_MS",
        "IGGY_KAFKA_TOPIC_RETENTION_BYTES",
        "IGGY_KAFKA_TOPIC_DURABILITY",
    ];

    /// Reads the defaults from the environment. An unset variable keeps the Iggy default: no
    /// expiry, no size cap, and replicated durability.
    ///
    /// # Errors
    ///
    /// Returns a message naming the variable that does not parse.
    pub fn from_env() -> Result<Self, String> {
        let read = |name: &str| std::env::var(name).ok();
        Self::parse(
            read("IGGY_KAFKA_TOPIC_RETENTION_MS").as_deref(),
            read("IGGY_KAFKA_TOPIC_RETENTION_BYTES").as_deref(),
            read("IGGY_KAFKA_TOPIC_DURABILITY").as_deref(),
        )
    }

    /// [`Self::from_env`] on the raw values.
    ///
    /// A retention of `-1` is the same as none. A byte cap below the largest Iggy segment would
    /// not hold for a topic with larger segments, since Iggy keeps one segment of each partition,
    /// so it is refused here.
    ///
    /// # Errors
    ///
    /// Returns a message naming the variable that does not parse.
    pub fn parse(
        retention_ms: Option<&str>,
        retention_bytes: Option<&str>,
        durability: Option<&str>,
    ) -> Result<Self, String> {
        let retention_ms = match retention_ms {
            None => None,
            Some(raw) => match raw.trim().parse::<i64>() {
                Ok(-1) => None,
                Ok(millis) if millis > 0 => u64::try_from(millis).ok(),
                _ => {
                    return Err(format!(
                        "IGGY_KAFKA_TOPIC_RETENTION_MS '{raw}' is not -1 or a positive number of \
                         milliseconds, for example 604800000"
                    ));
                }
            },
        };
        let retention_bytes = match retention_bytes {
            None => None,
            Some(raw) => match raw.trim().parse::<i64>() {
                Ok(-1) => None,
                Ok(bytes) if u64::try_from(bytes).is_ok_and(|b| b >= MAX_TOPIC_SEGMENT_SIZE) => {
                    u64::try_from(bytes).ok()
                }
                _ => {
                    return Err(format!(
                        "IGGY_KAFKA_TOPIC_RETENTION_BYTES '{raw}' is not -1 or a byte count of at \
                         least {MAX_TOPIC_SEGMENT_SIZE}, the largest Iggy segment"
                    ));
                }
            },
        };
        let durability = match durability {
            None => Durability::default(),
            Some(raw) => Durability::from_str(raw.trim()).map_err(|_| {
                format!("IGGY_KAFKA_TOPIC_DURABILITY '{raw}' is not replicated or persisted")
            })?,
        };
        Ok(Self {
            retention_ms,
            retention_bytes,
            durability,
        })
    }

    /// The expiry a topic gets when its request names no `retention.ms`.
    #[must_use]
    pub fn expiry(&self) -> Option<IggyExpiry> {
        self.retention_ms
            .map(|millis| expiry_from_retention_ms(i64::try_from(millis).unwrap_or(i64::MAX)))
    }

    /// The cap each partition gets when the request names no `retention.bytes`.
    #[must_use]
    pub fn retention(&self) -> Option<RetentionBytes> {
        self.retention_bytes.map(RetentionBytes::PerPartition)
    }
}

/// A Kafka `cleanup.policy`: which of its two values the list names.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CleanupPolicy {
    pub delete: bool,
    pub compact: bool,
}

impl Default for CleanupPolicy {
    /// Kafka's default, `delete`.
    fn default() -> Self {
        Self {
            delete: true,
            compact: false,
        }
    }
}

impl CleanupPolicy {
    /// Whether the topic keeps every record. Kafka removes old records only under `delete`: an
    /// empty list keeps them all, and so does `compact` here, because Iggy does not compact.
    #[must_use]
    pub const fn keeps_every_record(self) -> bool {
        !self.delete
    }
}

impl fmt::Display for CleanupPolicy {
    /// The Kafka list: `delete`, `compact`, `compact,delete`, or nothing for an empty list.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match (self.compact, self.delete) {
            (true, true) => f.write_str("compact,delete"),
            (true, false) => f.write_str("compact"),
            (false, true) => f.write_str("delete"),
            (false, false) => Ok(()),
        }
    }
}

/// The settings a `CreateTopics` request names. `None` takes the gateway default, or for the
/// replication factor the server default.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct KafkaTopicSettings {
    pub message_expiry: Option<IggyExpiry>,
    pub retention_bytes: Option<RetentionBytes>,
    pub segment_size: Option<u64>,
    pub cleanup_policy: CleanupPolicy,
    pub replication_factor: Option<u32>,
    /// The durability of the messages: `Persisted` when `flush.messages=1` or `flush.ms=0` asks
    /// for a flush of every write.
    pub durability: Option<Durability>,
}

impl KafkaTopicSettings {
    /// The Iggy options of a new topic of `partitions` partitions.
    ///
    /// A policy that keeps every record wins over `retention.ms`, `retention.bytes` and the
    /// gateway defaults, as Kafka ignores retention without `delete`. A flush of every write
    /// applies to the messages. The consumer offsets keep the gateway default, as Kafka flushes
    /// `__consumer_offsets` by its own settings.
    #[must_use]
    pub fn create_options(&self, partitions: u32, defaults: &TopicDefaults) -> TopicCreateOptions {
        let segment_size = self.segment_size.unwrap_or(DEFAULT_SEGMENT_SIZE);
        let (message_expiry, max_topic_size) = if self.cleanup_policy.keeps_every_record() {
            (Some(IggyExpiry::NeverExpire), Some(MaxTopicSize::Unlimited))
        } else {
            (
                self.message_expiry.or_else(|| defaults.expiry()),
                self.retention_bytes
                    .or_else(|| defaults.retention())
                    .map(|retention| retention.topic_size(partitions, segment_size)),
            )
        };
        TopicCreateOptions {
            partitions_count: Some(partitions),
            message_expiry,
            max_topic_size,
            segment_size: self.segment_size.map(IggyByteSize::from),
            durability: self.durability.unwrap_or(defaults.durability),
            consumer_offset_durability: defaults.durability,
            replication_factor: self.replication_factor,
            ..TopicCreateOptions::default()
        }
    }

    /// The `retention.bytes` the request names below one segment of the new topic, with the
    /// segment size, or `None`. Iggy keeps the newest sealed segment of each partition, so it
    /// cannot hold such a cap. A policy that keeps every record applies no retention.
    #[must_use]
    pub fn retention_below_segment(&self) -> Option<(u64, u64)> {
        if self.cleanup_policy.keeps_every_record() {
            return None;
        }
        let segment_size = self.segment_size.unwrap_or(DEFAULT_SEGMENT_SIZE);
        match self.retention_bytes? {
            RetentionBytes::PerPartition(bytes) if bytes < segment_size => {
                Some((bytes, segment_size))
            }
            _ => None,
        }
    }
}

/// A topic's Kafka-visible settings, from one Iggy topic read.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TopicConfigSnapshot {
    pub partitions_count: u32,
    pub message_expiry: IggyExpiry,
    /// Whether a client sent the expiry, as opposed to Iggy filling in its default.
    pub message_expiry_explicit: bool,
    pub max_topic_size: MaxTopicSize,
    pub max_topic_size_explicit: bool,
    pub segment_size: u64,
    pub segment_size_explicit: bool,
    /// The replication factor Iggy stored, or `None` for a topic created before Iggy stored one.
    pub replication_factor: Option<u32>,
    /// The durability of the messages.
    pub durability: Durability,
}

impl TopicConfigSnapshot {
    /// The settings of a topic with `options` and `partitions_count` partitions.
    #[must_use]
    pub fn new(options: &ResourceOptions, partitions_count: u32) -> Self {
        let resolved = TopicResolvedOptions::from(options);
        Self {
            partitions_count,
            message_expiry: resolved.message_expiry,
            message_expiry_explicit: is_explicit(options, OptionKey::MessageExpiry),
            max_topic_size: resolved.max_topic_size,
            max_topic_size_explicit: is_explicit(options, OptionKey::MaxTopicSize),
            segment_size: resolved.runtime.effective_segment_size().as_bytes_u64(),
            segment_size_explicit: is_explicit(options, OptionKey::SegmentSize),
            replication_factor: replication_factor_of(options),
            durability: resolved.runtime.durability,
        }
    }

    /// The settings Iggy stores for a topic created with `options`: each key the options carry
    /// is explicit, and each other key takes the Iggy default.
    #[must_use]
    pub fn created_with(options: &TopicCreateOptions) -> Self {
        Self {
            partitions_count: options.partitions_count.unwrap_or(1),
            message_expiry: options.message_expiry.unwrap_or(IggyExpiry::NeverExpire),
            message_expiry_explicit: options.message_expiry.is_some(),
            max_topic_size: options.max_topic_size.unwrap_or(MaxTopicSize::Unlimited),
            max_topic_size_explicit: options.max_topic_size.is_some(),
            segment_size: options
                .segment_size
                .map_or(DEFAULT_SEGMENT_SIZE, |size| size.as_bytes_u64()),
            segment_size_explicit: options.segment_size.is_some(),
            replication_factor: options.replication_factor,
            durability: options.durability,
        }
    }

    /// Each partition's share of the topic's size cap.
    #[must_use]
    pub fn retention_bytes(&self) -> RetentionBytes {
        RetentionBytes::share_of(self.max_topic_size, self.partitions_count)
    }

    /// Whether a client set the topic to keep every record: no expiry and no size cap, both
    /// stored. A topic created or altered with `cleanup.policy=compact` gets both, and so does
    /// one created with `retention.ms=-1` and `retention.bytes=-1`. The gateway does not store
    /// the policy, so it cannot tell the two apart.
    #[must_use]
    pub fn keeps_every_record(&self) -> bool {
        self.message_expiry_explicit
            && self.message_expiry == IggyExpiry::NeverExpire
            && self.max_topic_size_explicit
            && self.max_topic_size == MaxTopicSize::Unlimited
    }
}

fn is_explicit(options: &ResourceOptions, key: OptionKey) -> bool {
    options
        .get(&HeaderKey::from(key))
        .is_some_and(|entry| entry.explicit)
}

/// The replication factor Iggy stored for a topic with `options`, the server default included,
/// or `None` for a topic created before Iggy stored one.
#[must_use]
pub fn replication_factor_of(options: &ResourceOptions) -> Option<u32> {
    options
        .get(&HeaderKey::from(OptionKey::ReplicationFactor))
        .and_then(|entry| entry.value.as_uint32().ok())
}

/// A change to one stored setting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SettingChange<T> {
    Set(T),
    /// Back to the Iggy default, which reads as not set.
    Reset,
}

/// What one alter writes to an Iggy topic. A `None` field keeps the stored value.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct TopicConfigUpdate {
    pub message_expiry: Option<SettingChange<IggyExpiry>>,
    pub max_topic_size: Option<SettingChange<MaxTopicSize>>,
    /// The `cleanup.policy` the alter leaves, when it names one. Iggy stores no policy, so the
    /// fields above already hold all it writes. The handler logs it.
    pub cleanup_policy: Option<CleanupPolicy>,
    /// The `retention.bytes` the alter sets below one segment, with the segment size. Each
    /// partition keeps more than that cap, and the handler logs it.
    pub retention_below_segment: Option<(u64, u64)>,
    /// The `segment.bytes` an `AlterConfigs` left out, which Iggy cannot reset. The handler logs
    /// that the replace kept it.
    pub kept_segment_size: Option<u64>,
}

impl TopicConfigUpdate {
    /// Whether the update writes nothing to Iggy.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.message_expiry.is_none() && self.max_topic_size.is_none()
    }

    /// The Iggy update that writes this.
    #[must_use]
    pub fn options(&self) -> TopicUpdateOptions {
        let mut options = TopicUpdateOptions::default();
        match self.message_expiry {
            Some(SettingChange::Set(expiry)) => options.message_expiry = Some(expiry),
            Some(SettingChange::Reset) => {
                options.reset.insert(OptionKey::MessageExpiry);
            }
            None => {}
        }
        match self.max_topic_size {
            Some(SettingChange::Set(size)) => options.max_topic_size = Some(size),
            Some(SettingChange::Reset) => {
                options.reset.insert(OptionKey::MaxTopicSize);
            }
            None => {}
        }
        options
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const GIB: u64 = 1024 * 1024 * 1024;

    #[test]
    fn given_kafka_retention_ms_should_round_trip_through_iggy_microseconds() {
        let largest = i64::try_from((u64::MAX - 1) / 1_000).expect("fits");
        for millis in [0, 1, 1_500, 604_800_000, largest] {
            assert_eq!(
                retention_ms_of(expiry_from_retention_ms(millis)),
                Some(millis),
                "{millis}"
            );
        }
        assert_eq!(expiry_from_retention_ms(-1), IggyExpiry::NeverExpire);
        assert_eq!(retention_ms_of(IggyExpiry::NeverExpire), Some(-1));
        assert_eq!(retention_ms_of(IggyExpiry::ServerDefault), Some(-1));
    }

    #[test]
    fn given_retention_ms_zero_should_store_one_microsecond_not_the_default_sentinel() {
        let expiry = expiry_from_retention_ms(0);
        assert_eq!(u64::from(expiry), ZERO_RETENTION_MICROS);
        assert_ne!(expiry, IggyExpiry::ServerDefault);
    }

    #[test]
    fn given_retention_ms_past_the_microsecond_counter_should_never_expire() {
        assert_eq!(expiry_from_retention_ms(i64::MAX), IggyExpiry::NeverExpire);
        let largest = i64::try_from((u64::MAX - 1) / 1_000).expect("fits");
        assert_ne!(expiry_from_retention_ms(largest), IggyExpiry::NeverExpire);
        assert_eq!(
            expiry_from_retention_ms(largest + 1),
            IggyExpiry::NeverExpire
        );
    }

    #[test]
    fn given_an_expiry_that_is_not_whole_milliseconds_should_not_report_a_count() {
        let sub_millisecond = IggyExpiry::ExpireDuration(IggyDuration::from(1_500_u64));
        assert_eq!(retention_ms_of(sub_millisecond), None);
    }

    #[test]
    fn given_retention_bytes_should_store_the_share_times_the_partition_count() {
        let retention = RetentionBytes::PerPartition(2 * GIB);
        assert_eq!(
            retention.topic_size(3, GIB),
            MaxTopicSize::Custom(IggyByteSize::from(6 * GIB))
        );
        assert_eq!(
            RetentionBytes::share_of(retention.topic_size(3, GIB), 3),
            retention
        );
        assert_eq!(
            RetentionBytes::Unlimited.topic_size(3, GIB),
            MaxTopicSize::Unlimited
        );
        assert_eq!(
            RetentionBytes::PerPartition(u64::MAX / 2).topic_size(3, GIB),
            MaxTopicSize::Unlimited
        );
        assert_eq!(
            RetentionBytes::share_of(MaxTopicSize::ServerDefault, 3),
            RetentionBytes::Unlimited
        );
        assert_eq!(RetentionBytes::Unlimited.kafka_value(), -1);
    }

    #[test]
    fn given_retention_bytes_below_one_segment_should_store_one_segment_and_never_no_cap() {
        let small = RetentionBytes::PerPartition(1024 * 1024);
        assert_eq!(
            small.topic_size(3, GIB),
            MaxTopicSize::Custom(IggyByteSize::from(GIB)),
            "Iggy refuses a topic cap below one segment"
        );
        assert_eq!(
            RetentionBytes::PerPartition(0).topic_size(3, GIB),
            MaxTopicSize::Custom(IggyByteSize::from(GIB)),
            "0 keeps one segment, and is not Iggy's no-cap or server-default sentinel"
        );
        assert_eq!(
            RetentionBytes::PerPartition(0).topic_size(3, 0),
            MaxTopicSize::Custom(IggyByteSize::from(1))
        );
    }

    #[test]
    fn given_topic_default_variables_should_parse_or_name_the_variable() {
        let defaults =
            TopicDefaults::parse(Some("604800000"), Some(&GIB.to_string()), Some("Persisted"))
                .expect("valid");
        assert_eq!(defaults.retention_ms, Some(604_800_000));
        assert_eq!(defaults.retention_bytes, Some(GIB));
        assert_eq!(defaults.durability, Durability::Persisted);

        let none = TopicDefaults::parse(Some("-1"), Some("-1"), None).expect("valid");
        assert_eq!(none, TopicDefaults::default());

        for (ms, bytes, durability, variable) in [
            (Some("0"), None, None, "IGGY_KAFKA_TOPIC_RETENTION_MS"),
            (Some("7d"), None, None, "IGGY_KAFKA_TOPIC_RETENTION_MS"),
            (
                None,
                Some("1048576"),
                None,
                "IGGY_KAFKA_TOPIC_RETENTION_BYTES",
            ),
            (None, None, Some("fsync"), "IGGY_KAFKA_TOPIC_DURABILITY"),
        ] {
            let error = TopicDefaults::parse(ms, bytes, durability).expect_err("invalid");
            assert!(error.starts_with(variable), "{error}");
        }
    }

    #[test]
    fn given_defaults_should_fill_only_the_settings_a_request_leaves_out() {
        let defaults = TopicDefaults {
            retention_ms: Some(60_000),
            retention_bytes: Some(2 * GIB),
            durability: Durability::Persisted,
        };
        let options = KafkaTopicSettings::default().create_options(2, &defaults);
        assert_eq!(options.partitions_count, Some(2));
        assert_eq!(
            options.message_expiry,
            Some(expiry_from_retention_ms(60_000))
        );
        assert_eq!(
            options.max_topic_size,
            Some(MaxTopicSize::Custom(IggyByteSize::from(4 * GIB)))
        );
        assert_eq!(options.segment_size, None);
        assert_eq!(options.durability, Durability::Persisted);
        assert_eq!(options.consumer_offset_durability, Durability::Persisted);

        let named = KafkaTopicSettings {
            message_expiry: Some(IggyExpiry::NeverExpire),
            retention_bytes: Some(RetentionBytes::Unlimited),
            segment_size: Some(64 * 1024 * 1024),
            replication_factor: Some(3),
            ..KafkaTopicSettings::default()
        }
        .create_options(2, &defaults);
        assert_eq!(named.message_expiry, Some(IggyExpiry::NeverExpire));
        assert_eq!(named.max_topic_size, Some(MaxTopicSize::Unlimited));
        assert_eq!(
            named.segment_size,
            Some(IggyByteSize::from(64 * 1024 * 1024))
        );
        assert_eq!(named.replication_factor, Some(3));

        let bare = KafkaTopicSettings::default().create_options(1, &TopicDefaults::default());
        assert_eq!(bare.message_expiry, None);
        assert_eq!(bare.max_topic_size, None);
        assert_eq!(bare.replication_factor, None);
        assert_eq!(bare.durability, Durability::Replicated);
    }

    #[test]
    fn given_a_flush_of_every_write_should_persist_the_messages_and_keep_the_offset_default() {
        let options = KafkaTopicSettings {
            durability: Some(Durability::Persisted),
            ..KafkaTopicSettings::default()
        }
        .create_options(1, &TopicDefaults::default());
        assert_eq!(options.durability, Durability::Persisted);
        assert_eq!(options.consumer_offset_durability, Durability::Replicated);
        assert_eq!(
            TopicConfigSnapshot::created_with(&options).durability,
            Durability::Persisted
        );
    }

    #[test]
    fn given_a_create_retention_below_one_segment_should_name_it_unless_records_are_kept() {
        let small = KafkaTopicSettings {
            retention_bytes: Some(RetentionBytes::PerPartition(1024)),
            ..KafkaTopicSettings::default()
        };
        assert_eq!(
            small.retention_below_segment(),
            Some((1024, DEFAULT_SEGMENT_SIZE))
        );
        let options = small.create_options(2, &TopicDefaults::default());
        assert_eq!(
            options.max_topic_size,
            Some(MaxTopicSize::Custom(IggyByteSize::from(
                DEFAULT_SEGMENT_SIZE
            )))
        );
        let small_segments = KafkaTopicSettings {
            segment_size: Some(1024 * 1024),
            retention_bytes: Some(RetentionBytes::PerPartition(1024 * 1024)),
            ..KafkaTopicSettings::default()
        };
        assert_eq!(small_segments.retention_below_segment(), None);
        let compact = KafkaTopicSettings {
            cleanup_policy: CleanupPolicy {
                delete: false,
                compact: true,
            },
            ..small
        };
        assert_eq!(compact.retention_below_segment(), None);
    }

    #[test]
    fn given_both_retentions_stored_unlimited_should_keep_every_record() {
        let compact = KafkaTopicSettings {
            cleanup_policy: CleanupPolicy {
                delete: false,
                compact: true,
            },
            ..KafkaTopicSettings::default()
        }
        .create_options(1, &TopicDefaults::default());
        assert!(TopicConfigSnapshot::created_with(&compact).keeps_every_record());
        let bare = KafkaTopicSettings::default().create_options(1, &TopicDefaults::default());
        assert!(
            !TopicConfigSnapshot::created_with(&bare).keeps_every_record(),
            "Iggy's own defaults are not a client asking to keep every record"
        );
        let expiring = KafkaTopicSettings {
            message_expiry: Some(expiry_from_retention_ms(1_000)),
            retention_bytes: Some(RetentionBytes::Unlimited),
            ..KafkaTopicSettings::default()
        }
        .create_options(1, &TopicDefaults::default());
        assert!(!TopicConfigSnapshot::created_with(&expiring).keeps_every_record());
    }

    #[test]
    fn given_a_policy_without_delete_should_keep_every_record_over_retention_and_defaults() {
        let defaults = TopicDefaults {
            retention_ms: Some(60_000),
            retention_bytes: Some(2 * GIB),
            durability: Durability::Replicated,
        };
        for policy in [
            CleanupPolicy {
                delete: false,
                compact: true,
            },
            CleanupPolicy {
                delete: false,
                compact: false,
            },
        ] {
            let options = KafkaTopicSettings {
                message_expiry: Some(expiry_from_retention_ms(1_000)),
                retention_bytes: Some(RetentionBytes::PerPartition(GIB)),
                cleanup_policy: policy,
                ..KafkaTopicSettings::default()
            }
            .create_options(3, &defaults);
            assert_eq!(
                options.message_expiry,
                Some(IggyExpiry::NeverExpire),
                "{policy}"
            );
            assert_eq!(
                options.max_topic_size,
                Some(MaxTopicSize::Unlimited),
                "{policy}"
            );
        }

        let both = KafkaTopicSettings {
            message_expiry: Some(expiry_from_retention_ms(1_000)),
            cleanup_policy: CleanupPolicy {
                delete: true,
                compact: true,
            },
            ..KafkaTopicSettings::default()
        }
        .create_options(3, &defaults);
        assert_eq!(both.message_expiry, Some(expiry_from_retention_ms(1_000)));
        assert_eq!(
            both.max_topic_size,
            Some(MaxTopicSize::Custom(IggyByteSize::from(6 * GIB)))
        );
    }

    #[test]
    fn given_a_cleanup_policy_should_print_the_kafka_list() {
        let policy = |delete, compact| CleanupPolicy { delete, compact };
        assert_eq!(CleanupPolicy::default(), policy(true, false));
        assert_eq!(policy(true, false).to_string(), "delete");
        assert_eq!(policy(false, true).to_string(), "compact");
        assert_eq!(policy(true, true).to_string(), "compact,delete");
        assert_eq!(policy(false, false).to_string(), "");
        assert!(!policy(true, true).keeps_every_record());
        assert!(policy(false, false).keeps_every_record());
    }

    #[test]
    fn given_a_create_should_snapshot_each_sent_key_as_explicit() {
        let options = KafkaTopicSettings {
            message_expiry: Some(expiry_from_retention_ms(1_000)),
            segment_size: Some(64 * 1024 * 1024),
            ..KafkaTopicSettings::default()
        }
        .create_options(4, &TopicDefaults::default());
        let snapshot = TopicConfigSnapshot::created_with(&options);
        assert_eq!(snapshot.partitions_count, 4);
        assert!(snapshot.message_expiry_explicit);
        assert!(!snapshot.max_topic_size_explicit);
        assert_eq!(snapshot.retention_bytes(), RetentionBytes::Unlimited);
        assert_eq!(snapshot.segment_size, 64 * 1024 * 1024);
        assert!(snapshot.segment_size_explicit);
    }

    #[test]
    fn given_an_update_should_set_and_reset_through_the_iggy_patch() {
        let update = TopicConfigUpdate {
            message_expiry: Some(SettingChange::Reset),
            max_topic_size: Some(SettingChange::Set(MaxTopicSize::Custom(
                IggyByteSize::from(GIB),
            ))),
            ..TopicConfigUpdate::default()
        };
        let options = update.options();
        assert_eq!(options.message_expiry, None);
        assert!(options.reset.contains(&OptionKey::MessageExpiry));
        assert_eq!(
            options.max_topic_size,
            Some(MaxTopicSize::Custom(IggyByteSize::from(GIB)))
        );
        assert!(options.to_patch().is_ok());
        assert!(TopicConfigUpdate::default().is_empty());
        let policy_only = TopicConfigUpdate {
            cleanup_policy: Some(CleanupPolicy::default()),
            ..TopicConfigUpdate::default()
        };
        assert!(policy_only.is_empty());
    }
}
