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

//! Identity of a run: where it ran, which build it measured, and which workload it ran.

use std::fmt;
use std::ops::RangeInclusive;

use bench_report::{
    benchmark_kind::BenchmarkKind,
    group_metrics_kind::GroupMetricsKind,
    numeric_parameter::BenchmarkNumericParameter,
    options::{
        BenchmarkClientOptions, BenchmarkConsumerOptions, BenchmarkDurability, BenchmarkOptions,
        BenchmarkTcpOptions, BenchmarkTopicOptions,
    },
    params::{BenchmarkDataVolume, BenchmarkParams},
    transport::BenchmarkTransport,
};
use chrono::DateTime;
use semver::Version;
use serde::{Deserialize, Serialize};

use crate::BenchmarkReportLight;

const RELEASE_TAG_PREFIX: &str = "server-";
const COMMIT_LENGTHS: RangeInclusive<usize> = 7..=40;
const SHORT_COMMIT_LENGTH: usize = 9;
const SINGLE_NODE: usize = 1;

/// Every value that changes what a run measures, so two runs with the same key ran the same
/// workload. A report written before a value was recorded falls back to the bench defaults of
/// that day ([`BenchmarkOptions::legacy`]) and to its completed batches.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkloadKey {
    pub kind: BenchmarkKind,
    pub transport: BenchmarkTransport,
    pub message_size: BenchmarkNumericParameter,
    pub messages_per_batch: BenchmarkNumericParameter,
    pub producers: u32,
    pub consumers: u32,
    pub streams: u32,
    pub partitions: u32,
    pub consumer_groups: u32,
    /// As the bench writes it, for example `800.00 MB`.
    pub rate_limit: Option<String>,
    pub data_volume: DataVolumeKey,
    pub topic: BenchmarkTopicOptions,
    pub consumer: Option<ConsumerOptionsKey>,
    pub client: BenchmarkClientOptions,
    pub tcp: BenchmarkTcpOptions,
    /// `--workload` label.
    pub workload: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataVolumeKey {
    Configured(BenchmarkDataVolume),
    /// Reports without a configured volume: the batches that all actors completed. A consumer group
    /// completes a slightly different number in each run, so those runs rarely share a key.
    CompletedBatches(u64),
}

/// [`BenchmarkConsumerOptions`] with the `f32` as text, so the key implements `Eq` and `Hash`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ConsumerOptionsKey {
    pub consumer_offset_durability: BenchmarkDurability,
    pub max_wait_us: u64,
    pub min_count: u32,
    pub max_bytes: Option<u32>,
    /// The shortest text that parses back to the recorded `f32`, so equal text means an equal
    /// value.
    pub read_amplification: Option<String>,
}

/// One field of a [`WorkloadKey`], with the options broken down to single flags, so two keys can
/// name what differs between them. The variants follow the key's field order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum WorkloadField {
    Kind,
    Transport,
    MessageSize,
    MessagesPerBatch,
    Producers,
    Consumers,
    Streams,
    Partitions,
    ConsumerGroups,
    RateLimit,
    DataVolume,
    Durability,
    MessagesRequiredToSave,
    MaxTopicSize,
    MessageExpiry,
    ConsumerOffsetDurability,
    MaxWait,
    MinCount,
    MaxBytes,
    ReadAmplification,
    WarmupTime,
    HighLevelApi,
    ReuseStreams,
    NoDelay,
    Tls,
    Workload,
}

/// The runs of one workload on one machine and topology. They line up across versions.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct HistoryKey {
    pub workload: WorkloadKey,
    pub machine: String,
    pub nodes: usize,
}

/// Version order of runs: the semantic version, so a pre-release comes before its release, then the
/// commit date, then the start of the run. A value that doesn't parse sorts first.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct VersionOrder {
    pub version: Option<Version>,
    /// `gitref_date` in microseconds since the Unix epoch.
    pub gitref_date_us: Option<i64>,
    /// `timestamp` in microseconds since the Unix epoch.
    pub timestamp_us: Option<i64>,
}

/// Kind of a run for display and comparability. The exact kind is secondary text. The variants
/// follow the plain-language order of the pages, and `Ord` keeps it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum KindFamily {
    Producer,
    Consumer,
    ProducerAndConsumer,
    EndToEnd,
}

/// The build that a run measured, read from `gitref` and the server version.
#[derive(Debug)]
pub(crate) struct RunVersion {
    /// For display, for example `0.10.0-rc.1` or `0.10.0 at 91e020636`.
    pub(crate) text: String,
    pub(crate) semver: Option<Version>,
    pub(crate) commit: Option<String>,
    pub(crate) release: bool,
}

impl From<&BenchmarkParams> for WorkloadKey {
    fn from(params: &BenchmarkParams) -> Self {
        let BenchmarkOptions {
            topic,
            consumer,
            client,
            tcp,
        } = params
            .options
            .clone()
            .unwrap_or_else(|| BenchmarkOptions::legacy(params.benchmark_kind));
        Self {
            kind: params.benchmark_kind,
            transport: params.transport,
            message_size: params.message_size,
            messages_per_batch: params.messages_per_batch,
            producers: params.producers,
            consumers: params.consumers,
            streams: params.streams,
            partitions: params.partitions,
            consumer_groups: params.consumer_groups,
            rate_limit: params.rate_limit.clone(),
            data_volume: params.data_volume.map_or(
                DataVolumeKey::CompletedBatches(params.message_batches),
                DataVolumeKey::Configured,
            ),
            topic,
            consumer: consumer.map(ConsumerOptionsKey::from),
            client,
            tcp,
            workload: params.workload.clone(),
        }
    }
}

impl WorkloadKey {
    /// The fields where `other` differs from this key, in key order. Poll options that only one key
    /// has differ in every poll field. The destructuring makes a new key field a compile error
    /// here, so it can't be left out of the comparison.
    pub fn differences(&self, other: &Self) -> Vec<WorkloadField> {
        let Self {
            kind,
            transport,
            message_size,
            messages_per_batch,
            producers,
            consumers,
            streams,
            partitions,
            consumer_groups,
            rate_limit,
            data_volume,
            topic,
            consumer,
            client,
            tcp,
            workload,
        } = self;
        let BenchmarkTopicOptions {
            durability,
            messages_required_to_save,
            max_topic_size,
            message_expiry,
        } = topic;
        let BenchmarkClientOptions {
            warmup_time_us,
            high_level_api,
            reuse_streams,
        } = client;
        let BenchmarkTcpOptions { nodelay, tls } = tcp;
        let (consumer, poll) = (consumer.as_ref(), other.consumer.as_ref());
        [
            (WorkloadField::Kind, *kind == other.kind),
            (WorkloadField::Transport, *transport == other.transport),
            (
                WorkloadField::MessageSize,
                *message_size == other.message_size,
            ),
            (
                WorkloadField::MessagesPerBatch,
                *messages_per_batch == other.messages_per_batch,
            ),
            (WorkloadField::Producers, *producers == other.producers),
            (WorkloadField::Consumers, *consumers == other.consumers),
            (WorkloadField::Streams, *streams == other.streams),
            (WorkloadField::Partitions, *partitions == other.partitions),
            (
                WorkloadField::ConsumerGroups,
                *consumer_groups == other.consumer_groups,
            ),
            (WorkloadField::RateLimit, *rate_limit == other.rate_limit),
            (WorkloadField::DataVolume, *data_volume == other.data_volume),
            (
                WorkloadField::Durability,
                *durability == other.topic.durability,
            ),
            (
                WorkloadField::MessagesRequiredToSave,
                *messages_required_to_save == other.topic.messages_required_to_save,
            ),
            (
                WorkloadField::MaxTopicSize,
                *max_topic_size == other.topic.max_topic_size,
            ),
            (
                WorkloadField::MessageExpiry,
                *message_expiry == other.topic.message_expiry,
            ),
            (
                WorkloadField::ConsumerOffsetDurability,
                same_poll_option(consumer, poll, |options| {
                    &options.consumer_offset_durability
                }),
            ),
            (
                WorkloadField::MaxWait,
                same_poll_option(consumer, poll, |options| &options.max_wait_us),
            ),
            (
                WorkloadField::MinCount,
                same_poll_option(consumer, poll, |options| &options.min_count),
            ),
            (
                WorkloadField::MaxBytes,
                same_poll_option(consumer, poll, |options| &options.max_bytes),
            ),
            (
                WorkloadField::ReadAmplification,
                same_poll_option(consumer, poll, |options| &options.read_amplification),
            ),
            (
                WorkloadField::WarmupTime,
                *warmup_time_us == other.client.warmup_time_us,
            ),
            (
                WorkloadField::HighLevelApi,
                *high_level_api == other.client.high_level_api,
            ),
            (
                WorkloadField::ReuseStreams,
                *reuse_streams == other.client.reuse_streams,
            ),
            (WorkloadField::NoDelay, *nodelay == other.tcp.nodelay),
            (WorkloadField::Tls, *tls == other.tcp.tls),
            (WorkloadField::Workload, *workload == other.workload),
        ]
        .into_iter()
        .filter_map(|(field, same)| (!same).then_some(field))
        .collect()
    }
}

impl From<BenchmarkConsumerOptions> for ConsumerOptionsKey {
    fn from(options: BenchmarkConsumerOptions) -> Self {
        Self {
            consumer_offset_durability: options.consumer_offset_durability,
            max_wait_us: options.max_wait_us,
            min_count: options.min_count,
            max_bytes: options.max_bytes,
            read_amplification: options.read_amplification.map(|value| value.to_string()),
        }
    }
}

impl VersionOrder {
    pub(crate) fn new(
        version: Option<Version>,
        gitref_date: Option<&str>,
        timestamp: &str,
    ) -> Self {
        Self {
            version,
            gitref_date_us: gitref_date.and_then(unix_micros),
            timestamp_us: unix_micros(timestamp),
        }
    }
}

impl From<BenchmarkKind> for KindFamily {
    fn from(kind: BenchmarkKind) -> Self {
        match kind {
            BenchmarkKind::PinnedProducer | BenchmarkKind::BalancedProducer => Self::Producer,
            BenchmarkKind::PinnedConsumer | BenchmarkKind::BalancedConsumerGroup => Self::Consumer,
            BenchmarkKind::PinnedProducerAndConsumer
            | BenchmarkKind::BalancedProducerAndConsumerGroup => Self::ProducerAndConsumer,
            BenchmarkKind::EndToEndProducingConsumer
            | BenchmarkKind::EndToEndProducingConsumerGroup => Self::EndToEnd,
        }
    }
}

impl KindFamily {
    /// The actor group whose numbers stand for a run on every page: the producers, the
    /// consumers of a consumer-only kind, or the producing consumers of an end-to-end kind. The
    /// "below target rate" badge compares this group with its share of the rate limit.
    pub const fn primary_group(self) -> GroupMetricsKind {
        match self {
            Self::Producer | Self::ProducerAndConsumer => GroupMetricsKind::Producers,
            Self::Consumer => GroupMetricsKind::Consumers,
            Self::EndToEnd => GroupMetricsKind::ProducingConsumers,
        }
    }
}

impl WorkloadField {
    /// Every field, in key order.
    pub const ALL: [Self; 26] = [
        Self::Kind,
        Self::Transport,
        Self::MessageSize,
        Self::MessagesPerBatch,
        Self::Producers,
        Self::Consumers,
        Self::Streams,
        Self::Partitions,
        Self::ConsumerGroups,
        Self::RateLimit,
        Self::DataVolume,
        Self::Durability,
        Self::MessagesRequiredToSave,
        Self::MaxTopicSize,
        Self::MessageExpiry,
        Self::ConsumerOffsetDurability,
        Self::MaxWait,
        Self::MinCount,
        Self::MaxBytes,
        Self::ReadAmplification,
        Self::WarmupTime,
        Self::HighLevelApi,
        Self::ReuseStreams,
        Self::NoDelay,
        Self::Tls,
        Self::Workload,
    ];
}

impl fmt::Display for KindFamily {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Producer => "Producer",
            Self::Consumer => "Consumer",
            Self::ProducerAndConsumer => "Producer + consumer",
            Self::EndToEnd => "End-to-end",
        })
    }
}

impl fmt::Display for WorkloadField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Kind => "Kind",
            Self::Transport => "Transport",
            Self::MessageSize => "Message size",
            Self::MessagesPerBatch => "Messages per batch",
            Self::Producers => "Producers",
            Self::Consumers => "Consumers",
            Self::Streams => "Streams",
            Self::Partitions => "Partitions",
            Self::ConsumerGroups => "Consumer groups",
            Self::RateLimit => "Rate limit",
            Self::DataVolume => "Data volume",
            Self::Durability => "Durability",
            Self::MessagesRequiredToSave => "Messages required to save",
            Self::MaxTopicSize => "Max topic size",
            Self::MessageExpiry => "Message expiry",
            Self::ConsumerOffsetDurability => "Consumer offset durability",
            Self::MaxWait => "Consumer max wait",
            Self::MinCount => "Consumer min count",
            Self::MaxBytes => "Consumer max bytes",
            Self::ReadAmplification => "Read amplification",
            Self::WarmupTime => "Warm-up time",
            Self::HighLevelApi => "High-level API",
            Self::ReuseStreams => "Reuse streams",
            Self::NoDelay => "TCP no delay",
            Self::Tls => "TLS",
            Self::Workload => "Workload label",
        })
    }
}

impl RunVersion {
    /// A `server-X.Y.Z[-pre]` tag wins over the server version, because some tagged builds
    /// report the previous version. A commit, or any other ref, shows next to the server version.
    pub(crate) fn new(gitref: Option<&str>, server_version: &str) -> Self {
        let server_semver = || Version::parse(server_version).ok();
        let Some(gitref) = gitref else {
            return Self {
                text: server_version.to_owned(),
                semver: server_semver(),
                commit: None,
                release: false,
            };
        };
        if let Some(tag) = gitref
            .strip_prefix(RELEASE_TAG_PREFIX)
            .and_then(|version| Version::parse(version).ok())
        {
            return Self {
                text: tag.to_string(),
                release: tag.pre.is_empty() && tag.build.is_empty(),
                semver: Some(tag),
                commit: None,
            };
        }
        if let Ok(version) = Version::parse(gitref) {
            return Self {
                text: version.to_string(),
                semver: Some(version),
                commit: None,
                release: false,
            };
        }
        let commit =
            is_commit(gitref).then(|| gitref[..SHORT_COMMIT_LENGTH.min(gitref.len())].to_owned());
        Self {
            text: format!(
                "{server_version} at {}",
                commit.as_deref().unwrap_or(gitref)
            ),
            semver: server_semver(),
            commit,
            release: false,
        }
    }
}

impl BenchmarkReportLight {
    /// The test environment label: `hardware.identifier`, else the server hostname.
    pub fn machine(&self) -> &str {
        self.hardware
            .identifier
            .as_deref()
            .unwrap_or(&self.server_stats.hostname)
    }

    /// Number of server nodes.
    pub fn nodes(&self) -> usize {
        self.cluster
            .as_ref()
            .map_or(SINGLE_NODE, |cluster| cluster.nodes.len())
    }
}

fn is_commit(gitref: &str) -> bool {
    COMMIT_LENGTHS.contains(&gitref.len()) && gitref.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn same_poll_option<T: PartialEq + ?Sized>(
    left: Option<&ConsumerOptionsKey>,
    right: Option<&ConsumerOptionsKey>,
    option: fn(&ConsumerOptionsKey) -> &T,
) -> bool {
    left.map(option) == right.map(option)
}

fn unix_micros(rfc3339: &str) -> Option<i64> {
    DateTime::parse_from_rfc3339(rfc3339)
        .ok()
        .map(|date| date.timestamp_micros())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_fixtures::{CLUSTER_RUN, PHASE_1A_RUN, report};
    use bench_report::options::{BenchmarkMaxTopicSize, BenchmarkMessageExpiry};

    const COMMIT: &str = "71f29618b";
    const FULL_COMMIT: &str = "91e020636a1b2c3d4e5f60718293a4b5c6d7e8f9";

    /// A named change that must give another workload key.
    type Change<T> = (&'static str, fn(&mut T));
    /// A change to one field, with the field that the key must name as changed.
    type FieldChange<T> = (WorkloadField, fn(&mut T));

    fn params(report_json: &str) -> BenchmarkParams {
        report(report_json).params
    }

    fn order(version: &str, gitref_date: &str, timestamp: &str) -> VersionOrder {
        VersionOrder::new(Version::parse(version).ok(), Some(gitref_date), timestamp)
    }

    #[test]
    fn given_release_tag_when_reading_version_should_mark_release() {
        let version = RunVersion::new(Some("server-0.9.0"), "0.9.0");

        assert_eq!(version.text, "0.9.0");
        assert!(version.release);
        assert_eq!(version.commit, None);
    }

    #[test]
    fn given_other_gitrefs_when_reading_version_should_not_mark_release() {
        for gitref in [
            Some("0.8.0"),
            Some("server-0.10.0-rc.1"),
            Some("0.6.1-edge.4"),
            Some(COMMIT),
            Some(FULL_COMMIT),
            Some("master"),
            None,
        ] {
            assert!(!RunVersion::new(gitref, "0.9.0").release, "{gitref:?}");
        }
    }

    #[test]
    fn given_tag_of_build_reporting_older_version_when_reading_version_should_take_tag() {
        let version = RunVersion::new(Some("server-0.4.202"), "0.4.201");

        assert_eq!(version.text, "0.4.202");
        assert_eq!(version.semver, Version::parse("0.4.202").ok());
    }

    #[test]
    fn given_pre_release_tag_when_reading_version_should_keep_pre_release() {
        let version = RunVersion::new(Some("server-0.10.0-rc.1"), "0.10.0");

        assert_eq!(version.text, "0.10.0-rc.1");
        assert!(!version.release);
    }

    #[test]
    fn given_bare_version_when_reading_version_should_take_it() {
        let version = RunVersion::new(Some("0.6.1-edge.4"), "0.6.1");

        assert_eq!(version.text, "0.6.1-edge.4");
        assert_eq!(version.semver, Version::parse("0.6.1-edge.4").ok());
        assert_eq!(version.commit, None);
    }

    #[test]
    fn given_commit_when_reading_version_should_show_server_version_and_short_commit() {
        let short = RunVersion::new(Some(COMMIT), "0.9.0");
        let full = RunVersion::new(Some(FULL_COMMIT), "0.10.0");

        assert_eq!(short.text, "0.9.0 at 71f29618b");
        assert_eq!(short.commit.as_deref(), Some(COMMIT));
        assert_eq!(short.semver, Version::parse("0.9.0").ok());
        assert_eq!(full.text, "0.10.0 at 91e020636");
        assert_eq!(full.commit.as_deref(), Some("91e020636"));
    }

    #[test]
    fn given_no_gitref_when_reading_version_should_show_server_version() {
        let version = RunVersion::new(None, "0.10.0");

        assert_eq!(version.text, "0.10.0");
        assert_eq!(version.semver, Version::parse("0.10.0").ok());
    }

    #[test]
    fn given_runs_when_sorted_should_follow_version_then_commit_date_then_start() {
        let unknown = VersionOrder::new(None, None, "not a date");
        let candidate = order(
            "0.10.0-rc.1",
            "2026-11-04T10:00:00Z",
            "2026-11-30T10:00:00Z",
        );
        let release = order("0.10.0", "2026-11-20T10:00:00Z", "2026-11-29T10:00:00Z");
        // The same commit date as the next run, written with another offset.
        let first_run = order(
            "0.10.0",
            "2026-11-20T12:00:00+01:00",
            "2026-11-21T10:00:00Z",
        );
        let second_run = order("0.10.0", "2026-11-20T11:00:00Z", "2026-11-22T10:00:00Z");
        let mut runs = vec![
            second_run.clone(),
            release.clone(),
            unknown.clone(),
            first_run.clone(),
            candidate.clone(),
        ];

        runs.sort();

        assert_eq!(runs, [unknown, candidate, release, first_run, second_run]);
    }

    #[test]
    fn given_cluster_report_when_reading_environment_should_take_identifier_and_node_count() {
        let mut cluster = report(CLUSTER_RUN);

        assert_eq!(cluster.machine(), "ip-10-0-0-224");
        assert_eq!(cluster.nodes(), 3);

        cluster.hardware.identifier = None;
        assert_eq!(cluster.machine(), "ip-10-0-0-169");
        assert_eq!(report(PHASE_1A_RUN).nodes(), SINGLE_NODE);
    }

    #[test]
    fn given_runs_of_other_versions_and_remarks_when_keyed_should_share_key() {
        let original = params(PHASE_1A_RUN);
        let mut other = original.clone();
        other.gitref = Some("server-0.10.0".to_owned());
        other.gitref_date = Some("2026-12-01T00:00:00+00:00".to_owned());
        other.remark = Some("pp-0100-aws_i4i@20MB".to_owned());
        other.extra_info = Some("rerun".to_owned());
        other.server_address = "10.0.0.121:8090".to_owned();
        other.pretty_name = "renamed".to_owned();
        other.bench_command = "iggy-bench rebuilt".to_owned();
        other.params_identifier = "other".to_owned();
        other.command_line = Some("iggy-bench typed".to_owned());
        other.suite = Some("i4i-single".to_owned());

        assert_eq!(WorkloadKey::from(&other), WorkloadKey::from(&original));
    }

    #[test]
    fn given_same_configured_volume_when_completed_batches_differ_should_share_key() {
        let mut first = params(PHASE_1A_RUN);
        let mut second = first.clone();
        first.message_batches = 248_001;
        second.message_batches = 248_003;

        assert_eq!(WorkloadKey::from(&first), WorkloadKey::from(&second));
    }

    #[test]
    fn given_report_without_volume_when_completed_batches_differ_should_not_share_key() {
        let mut first = params(CLUSTER_RUN);
        let mut second = first.clone();
        first.message_batches = 248_001;
        second.message_batches = 248_003;

        assert_eq!(
            WorkloadKey::from(&first).data_volume,
            DataVolumeKey::CompletedBatches(248_001)
        );
        assert_ne!(WorkloadKey::from(&first), WorkloadKey::from(&second));
    }

    #[test]
    fn given_change_of_option_rate_limit_or_label_when_keyed_should_change_key() {
        let original = params(PHASE_1A_RUN);
        let changes: [Change<BenchmarkParams>; 15] = [
            ("rate limit", |p| p.rate_limit = Some("40.00 MB".to_owned())),
            ("no rate limit", |p| p.rate_limit = None),
            ("workload label", |p| {
                p.workload = Some("producer-2p-cold@20MB".to_owned())
            }),
            ("data volume", |p| {
                p.data_volume = Some(BenchmarkDataVolume::TotalDataBytes(40_000_000));
            }),
            ("durability", |p| {
                options(p).topic.durability = BenchmarkDurability::Persisted;
            }),
            ("messages required to save", |p| {
                options(p).topic.messages_required_to_save = Some(1);
            }),
            ("max topic size", |p| {
                options(p).topic.max_topic_size = BenchmarkMaxTopicSize::ServerDefault;
            }),
            ("message expiry", |p| {
                options(p).topic.message_expiry = BenchmarkMessageExpiry::AfterUs(1_000_000);
            }),
            ("consumer options", |p| {
                options(p).consumer =
                    BenchmarkOptions::legacy(BenchmarkKind::PinnedConsumer).consumer;
            }),
            ("warmup time", |p| options(p).client.warmup_time_us += 1),
            ("high level api", |p| {
                options(p).client.high_level_api ^= true
            }),
            ("reuse streams", |p| options(p).client.reuse_streams ^= true),
            ("nodelay", |p| options(p).tcp.nodelay ^= true),
            ("tls", |p| options(p).tcp.tls ^= true),
            ("message size", |p| {
                p.message_size = BenchmarkNumericParameter::Range {
                    min: 100,
                    max: 1000,
                };
            }),
        ];

        for (name, change) in changes {
            let mut changed = original.clone();
            change(&mut changed);
            assert_ne!(
                WorkloadKey::from(&changed),
                WorkloadKey::from(&original),
                "{name}"
            );
        }
    }

    #[test]
    fn given_change_of_consumer_option_when_keyed_should_change_key() {
        let mut original = params(PHASE_1A_RUN);
        original.benchmark_kind = BenchmarkKind::PinnedProducerAndConsumer;
        original.options = Some(BenchmarkOptions::legacy(original.benchmark_kind));
        let changes: [Change<BenchmarkConsumerOptions>; 5] = [
            ("offset durability", |c| {
                c.consumer_offset_durability = BenchmarkDurability::Persisted;
            }),
            ("max wait", |c| c.max_wait_us = 250_000),
            ("min count", |c| c.min_count = 10),
            ("max bytes", |c| c.max_bytes = Some(2_097_152)),
            ("read amplification", |c| c.read_amplification = Some(1.5)),
        ];

        for (name, change) in changes {
            let mut changed = original.clone();
            change(
                options(&mut changed)
                    .consumer
                    .as_mut()
                    .expect("producer and consumer kinds poll"),
            );
            assert_ne!(
                WorkloadKey::from(&changed),
                WorkloadKey::from(&original),
                "{name}"
            );
        }
    }

    #[test]
    fn given_report_without_options_when_keyed_should_match_legacy_options() {
        let without = params(CLUSTER_RUN);
        let mut with_legacy = without.clone();
        with_legacy.options = Some(BenchmarkOptions::legacy(without.benchmark_kind));

        assert_eq!(without.options, None);
        assert_eq!(WorkloadKey::from(&without), WorkloadKey::from(&with_legacy));
    }

    #[test]
    fn given_legacy_read_amplification_when_keyed_should_keep_exact_decimal_text() {
        let key = ConsumerOptionsKey::from(
            BenchmarkOptions::legacy(BenchmarkKind::PinnedProducerAndConsumer)
                .consumer
                .expect("producer and consumer kinds poll"),
        );

        assert_eq!(key.read_amplification.as_deref(), Some("1.05"));
    }

    #[test]
    fn given_runs_of_one_workload_when_diffed_should_find_no_field() {
        let original = params(PHASE_1A_RUN);
        let mut rerun = original.clone();
        rerun.remark = Some("rerun".to_owned());
        rerun.gitref = Some("server-0.10.0".to_owned());

        assert_eq!(
            WorkloadKey::from(&original).differences(&WorkloadKey::from(&rerun)),
            []
        );
    }

    #[test]
    fn given_one_changed_field_when_diffed_should_name_it_down_to_single_option() {
        let original = params(PHASE_1A_RUN);
        let changes: [FieldChange<BenchmarkParams>; 21] = [
            (WorkloadField::Kind, |p| {
                p.benchmark_kind = BenchmarkKind::BalancedProducer;
            }),
            (WorkloadField::Transport, |p| {
                p.transport = BenchmarkTransport::Quic;
            }),
            (WorkloadField::MessageSize, |p| {
                p.message_size = BenchmarkNumericParameter::Value(100);
            }),
            (WorkloadField::MessagesPerBatch, |p| {
                p.messages_per_batch = BenchmarkNumericParameter::Range { min: 10, max: 100 };
            }),
            (WorkloadField::Producers, |p| p.producers = 20),
            (WorkloadField::Consumers, |p| p.consumers = 1),
            (WorkloadField::Streams, |p| p.streams = 20),
            (WorkloadField::Partitions, |p| p.partitions = 8),
            (WorkloadField::ConsumerGroups, |p| p.consumer_groups = 1),
            (WorkloadField::RateLimit, |p| p.rate_limit = None),
            (WorkloadField::DataVolume, |p| {
                p.data_volume = Some(BenchmarkDataVolume::TotalDataBytes(40_000_000));
            }),
            (WorkloadField::Durability, |p| {
                options(p).topic.durability = BenchmarkDurability::Persisted;
            }),
            (WorkloadField::MessagesRequiredToSave, |p| {
                options(p).topic.messages_required_to_save = Some(1);
            }),
            (WorkloadField::MaxTopicSize, |p| {
                options(p).topic.max_topic_size = BenchmarkMaxTopicSize::Bytes(1_000_000_000);
            }),
            (WorkloadField::MessageExpiry, |p| {
                options(p).topic.message_expiry = BenchmarkMessageExpiry::ServerDefault;
            }),
            (WorkloadField::WarmupTime, |p| {
                options(p).client.warmup_time_us = 5_000_000;
            }),
            (WorkloadField::HighLevelApi, |p| {
                options(p).client.high_level_api ^= true;
            }),
            (WorkloadField::ReuseStreams, |p| {
                options(p).client.reuse_streams ^= true;
            }),
            (WorkloadField::NoDelay, |p| options(p).tcp.nodelay ^= true),
            (WorkloadField::Tls, |p| options(p).tcp.tls ^= true),
            (WorkloadField::Workload, |p| p.workload = None),
        ];

        for (field, change) in changes {
            let mut changed = original.clone();
            change(&mut changed);

            assert_eq!(
                WorkloadKey::from(&original).differences(&WorkloadKey::from(&changed)),
                [field],
                "{field}"
            );
        }
    }

    #[test]
    fn given_one_changed_poll_option_when_diffed_should_name_that_option() {
        let mut original = params(PHASE_1A_RUN);
        original.benchmark_kind = BenchmarkKind::PinnedProducerAndConsumer;
        original.options = Some(BenchmarkOptions::legacy(original.benchmark_kind));
        let changes: [FieldChange<BenchmarkConsumerOptions>; 5] = [
            (WorkloadField::ConsumerOffsetDurability, |c| {
                c.consumer_offset_durability = BenchmarkDurability::Persisted;
            }),
            (WorkloadField::MaxWait, |c| c.max_wait_us = 250_000),
            (WorkloadField::MinCount, |c| c.min_count = 10),
            (WorkloadField::MaxBytes, |c| c.max_bytes = Some(2_097_152)),
            (WorkloadField::ReadAmplification, |c| {
                c.read_amplification = Some(1.5);
            }),
        ];

        for (field, change) in changes {
            let mut changed = original.clone();
            change(
                options(&mut changed)
                    .consumer
                    .as_mut()
                    .expect("producer and consumer kinds poll"),
            );

            assert_eq!(
                WorkloadKey::from(&original).differences(&WorkloadKey::from(&changed)),
                [field],
                "{field}"
            );
        }
    }

    #[test]
    fn given_several_changed_fields_when_diffed_should_name_each_in_key_order() {
        let original = params(PHASE_1A_RUN);
        let mut changed = original.clone();
        changed.workload = Some("producer-20p-persisted@800MB".to_owned());
        options(&mut changed).topic.durability = BenchmarkDurability::Persisted;
        options(&mut changed).topic.messages_required_to_save = Some(1);
        changed.producers = 20;
        changed.data_volume = Some(BenchmarkDataVolume::TotalDataBytes(800_000_000));

        assert_eq!(
            WorkloadKey::from(&changed).differences(&WorkloadKey::from(&original)),
            [
                WorkloadField::Producers,
                WorkloadField::DataVolume,
                WorkloadField::Durability,
                WorkloadField::MessagesRequiredToSave,
                WorkloadField::Workload,
            ]
        );
    }

    #[test]
    fn given_field_list_when_read_should_hold_every_field_once_in_key_order() {
        let fields = WorkloadField::ALL;

        assert!(fields.windows(2).all(|pair| pair[0] < pair[1]));
        // The label is the last field of the key, so the list ends with it.
        assert_eq!(fields.len(), WorkloadField::Workload as usize + 1);
    }

    #[test]
    fn given_poll_options_on_one_side_only_when_diffed_should_name_every_poll_option() {
        let original = params(PHASE_1A_RUN);
        let mut changed = original.clone();
        options(&mut changed).consumer =
            BenchmarkOptions::legacy(BenchmarkKind::PinnedConsumer).consumer;

        assert_eq!(
            WorkloadKey::from(&original).differences(&WorkloadKey::from(&changed)),
            [
                WorkloadField::ConsumerOffsetDurability,
                WorkloadField::MaxWait,
                WorkloadField::MinCount,
                WorkloadField::MaxBytes,
                WorkloadField::ReadAmplification,
            ]
        );
    }

    fn options(params: &mut BenchmarkParams) -> &mut BenchmarkOptions {
        params
            .options
            .as_mut()
            .expect("the fixture report records its options")
    }
}
