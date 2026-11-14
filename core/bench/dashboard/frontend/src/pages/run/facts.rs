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

//! The facts that a run's page lists: its conditions, command, parameters, options, environment and
//! latency percentiles, as text built from the typed fields of the summary and the report.

use std::{fmt, iter};

use bench_dashboard_shared::BenchmarkReportLight;
use bench_dashboard_shared::delta::Metric;
use bench_dashboard_shared::identity::{DataVolumeKey, KindFamily, WorkloadField, WorkloadKey};
use bench_dashboard_shared::metrics::rate_limit_bytes;
use bench_dashboard_shared::summary::{BenchmarkSummary, actors, kind_name};
use bench_report::group_metrics_summary::BenchmarkGroupMetricsSummary;
use bench_report::hardware::BenchmarkHardware;
use bench_report::numeric_parameter::BenchmarkNumericParameter;
use bench_report::options::{BenchmarkMaxTopicSize, BenchmarkMessageExpiry, BenchmarkOptions};
use bench_report::params::{BenchmarkDataVolume, BenchmarkParams};
use bench_report::server_stats::BenchmarkServerStats;
use bench_report::transport::BenchmarkTransport;

use super::change::metric_name;
use crate::components::run_labels::{
    CONDITION_SEPARATOR, DayLabel, MachineName, batch_size_text, data_volume_text, durability_text,
    memory_text, message_size_text, milliseconds, nodes_text, numeric_text, rate_limit_text,
    rate_text, topic_durability,
};
use crate::format::{format_bytes, group_digits};

pub const NO_OPTIONS_NOTE: &str = "The report doesn't record any options, so the bench defaults \
                                   apply.";
const REUSED_STREAMS_NOTE: &str = "This run reused existing streams, so the topic can still have \
                                   the options of the run that created it.";
const CONSUMER_TOPIC_NOTE: &str = "A consumer-only run polls a topic that an earlier run created, \
                                   so the topic has that run's options.";
const SERVER_DEFAULT: &str = "server default";
const BENCH_DEFAULT: &str = "bench default";
const NO_RATE_LIMIT: &str = "none";
const NO_LABEL: &str = "none";
/// A poll option that the run's kind doesn't use.
const NOT_POLLED: &str = "not used";
const MICROS_PER_SECOND: u64 = 1_000_000;
const MICROS_PER_MILLISECOND: u64 = 1_000;
const SECONDS_PER_MINUTE: u64 = 60;
const SECONDS_PER_HOUR: u64 = 3_600;
const PERCENT: f64 = 100.0;
const BENCH_HOST_FACTS: usize = 5;
pub const MACHINE_LABEL: &str = "Machine";
pub const TOPOLOGY_LABEL: &str = "Topology";
/// The labels of [`bench_host_values`], in its order.
pub const BENCH_HOST_LABELS: [&str; BENCH_HOST_FACTS] =
    ["CPU", "Logical CPUs", "Memory", "OS", "Kernel"];
pub const MIN_LABEL: &str = "Min";
pub const MAX_LABEL: &str = "Max";
pub const AVERAGE_LABEL: &str = "Average";
pub const P9999_LABEL: &str = "P99.99";
/// The ladder's shortest bar, so the smallest value stays visible.
const MIN_BAR_PCT: f64 = 4.0;
const FULL_BAR_PCT: f64 = 100.0;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fact {
    pub label: &'static str,
    pub value: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FactGroup {
    pub title: &'static str,
    pub facts: Vec<Fact>,
}

/// The value of one workload key field. Values sort by amount first, so numbers keep their order,
/// then by text.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct FieldValue {
    /// `None` for a value that isn't an amount, such as a kind or a flag.
    pub size: Option<u64>,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Command {
    pub text: String,
    /// The bench rebuilt the command from its parameters, because the report predates the
    /// recorded command line. It can differ from the command that ran.
    pub rebuilt: bool,
}

/// One rung of the latency ladder, from the fastest batch to the slowest.
#[derive(Debug, Clone, PartialEq)]
pub struct LadderRow {
    pub label: &'static str,
    pub value_ms: f64,
    /// The rung's metric, which can carry a change against the previous version. `None` for a rung
    /// that the delta rule doesn't compare.
    pub metric: Option<Metric>,
    /// The length of the bar on a log scale, in percent of the longest bar.
    pub bar_pct: f64,
}

/// The run's conditions in one line: message size, batch size, data volume, rate limit, transport,
/// durability, machine and topology.
pub fn conditions(summary: &BenchmarkSummary) -> String {
    let key = &summary.workload_key;
    let mut parts = vec![
        message_size_text(key.message_size),
        batch_size_text(key.messages_per_batch),
        data_volume_text(key.data_volume),
        rate_limit_text(key.rate_limit.as_deref()),
        key.transport.to_string(),
    ];
    parts
        .extend(topic_durability(summary).map(|durability| durability_text(durability).to_owned()));
    parts.extend([
        MachineName(&summary.machine).to_string(),
        nodes_text(summary.nodes),
    ]);
    parts.join(CONDITION_SEPARATOR)
}

impl Command {
    /// `None` when the report has neither the recorded nor the rebuilt command.
    pub fn of(params: &BenchmarkParams) -> Option<Self> {
        if let Some(text) = params.command_line.as_ref().filter(|text| !text.is_empty()) {
            return Some(Self {
                text: text.clone(),
                rebuilt: false,
            });
        }
        (!params.bench_command.is_empty()).then(|| Self {
            text: params.bench_command.clone(),
            rebuilt: true,
        })
    }
}

/// The run's parameters. The data volume comes from the summary, which counts the completed batches
/// when a report predates the recorded volume.
pub fn parameters(summary: &BenchmarkSummary, params: &BenchmarkParams) -> Vec<Fact> {
    let mut facts = vec![
        fact("Kind", kind_name(params.benchmark_kind)),
        fact("Actors", actors(params)),
        fact("Transport", params.transport.to_string()),
        fact("Streams", group_digits(params.streams.into())),
        fact("Partitions", group_digits(params.partitions.into())),
        fact("Message size", message_size_value(params.message_size)),
        fact(
            "Messages per batch",
            numeric_text(params.messages_per_batch),
        ),
        fact(
            "Data volume",
            data_volume_text(summary.workload_key.data_volume),
        ),
        fact("Rate limit", rate_limit_value(params.rate_limit.as_deref())),
    ];
    let labels = [
        ("Suite", &params.suite),
        ("Workload", &params.workload),
        ("Remark", &params.remark),
    ];
    facts.extend(
        labels
            .into_iter()
            .filter_map(|(label, value)| value.as_ref().map(|value| fact(label, value.clone()))),
    );
    facts
}

/// The options that the report recorded, in groups. Empty when it recorded none.
pub fn option_groups(params: &BenchmarkParams) -> Vec<FactGroup> {
    let Some(options) = &params.options else {
        return Vec::new();
    };
    let topic = &options.topic;
    let mut groups = vec![FactGroup {
        title: "Topic",
        facts: vec![
            fact("Durability", durability_text(topic.durability)),
            fact(
                "Messages required to save",
                messages_required_to_save_text(topic.messages_required_to_save),
            ),
            fact("Max topic size", max_topic_size_text(topic.max_topic_size)),
            fact("Message expiry", message_expiry_text(topic.message_expiry)),
        ],
    }];
    if let Some(consumer) = &options.consumer {
        let mut facts = vec![
            fact(
                "Offset durability",
                durability_text(consumer.consumer_offset_durability),
            ),
            fact("Max wait", duration_text(consumer.max_wait_us)),
            fact("Min count", group_digits(consumer.min_count.into())),
            fact("Max bytes", max_bytes_text(consumer.max_bytes)),
        ];
        facts.extend(
            consumer
                .read_amplification
                .map(|factor| fact("Read amplification", amplification_text(factor))),
        );
        groups.push(FactGroup {
            title: "Consumer",
            facts,
        });
    }
    groups.push(FactGroup {
        title: "Client",
        facts: vec![
            fact("Warmup", duration_text(options.client.warmup_time_us)),
            fact("High-level API", yes_no(options.client.high_level_api)),
            fact("Reuse streams", yes_no(options.client.reuse_streams)),
        ],
    });
    if params.transport == BenchmarkTransport::Tcp {
        groups.push(FactGroup {
            title: "TCP",
            facts: vec![
                fact("No delay", yes_no(options.tcp.nodelay)),
                fact("TLS", yes_no(options.tcp.tls)),
            ],
        });
    }
    groups
}

/// The value of `field` in `key`, as the Configuration section writes it.
pub fn field_value(field: WorkloadField, key: &WorkloadKey) -> FieldValue {
    let topic = &key.topic;
    let poll = key.consumer.as_ref();
    let amount = |size: u64, text: String| FieldValue {
        size: Some(size),
        text,
    };
    let word = |text: &str| FieldValue {
        size: None,
        text: text.to_owned(),
    };
    let count = |count: u32| amount(count.into(), group_digits(count.into()));
    let flag = |value: bool| word(yes_no(value));
    let not_polled = || word(NOT_POLLED);
    match field {
        WorkloadField::Kind => word(kind_name(key.kind)),
        WorkloadField::Transport => word(&key.transport.to_string()),
        WorkloadField::MessageSize => amount(
            key.message_size.min().into(),
            message_size_value(key.message_size),
        ),
        WorkloadField::MessagesPerBatch => amount(
            key.messages_per_batch.min().into(),
            numeric_text(key.messages_per_batch),
        ),
        WorkloadField::Producers => count(key.producers),
        WorkloadField::Consumers => count(key.consumers),
        WorkloadField::Streams => count(key.streams),
        WorkloadField::Partitions => count(key.partitions),
        WorkloadField::ConsumerGroups => count(key.consumer_groups),
        WorkloadField::RateLimit => FieldValue {
            size: key.rate_limit.as_deref().and_then(rate_limit_bytes),
            text: rate_limit_value(key.rate_limit.as_deref()),
        },
        WorkloadField::DataVolume => amount(
            data_volume_amount(key.data_volume),
            data_volume_text(key.data_volume),
        ),
        WorkloadField::Durability => word(durability_text(topic.durability)),
        WorkloadField::MessagesRequiredToSave => FieldValue {
            size: topic.messages_required_to_save.map(u64::from),
            text: messages_required_to_save_text(topic.messages_required_to_save),
        },
        WorkloadField::MaxTopicSize => FieldValue {
            size: match topic.max_topic_size {
                BenchmarkMaxTopicSize::Bytes(bytes) => Some(bytes),
                BenchmarkMaxTopicSize::ServerDefault | BenchmarkMaxTopicSize::Unlimited => None,
            },
            text: max_topic_size_text(topic.max_topic_size),
        },
        WorkloadField::MessageExpiry => FieldValue {
            size: match topic.message_expiry {
                BenchmarkMessageExpiry::AfterUs(micros) => Some(micros),
                BenchmarkMessageExpiry::ServerDefault | BenchmarkMessageExpiry::NeverExpire => None,
            },
            text: message_expiry_text(topic.message_expiry),
        },
        WorkloadField::ConsumerOffsetDurability => poll.map_or_else(not_polled, |options| {
            word(durability_text(options.consumer_offset_durability))
        }),
        WorkloadField::MaxWait => poll.map_or_else(not_polled, |options| {
            amount(options.max_wait_us, duration_text(options.max_wait_us))
        }),
        WorkloadField::MinCount => poll.map_or_else(not_polled, |options| count(options.min_count)),
        WorkloadField::MaxBytes => poll.map_or_else(not_polled, |options| FieldValue {
            size: options.max_bytes.map(u64::from),
            text: max_bytes_text(options.max_bytes),
        }),
        WorkloadField::ReadAmplification => poll
            .and_then(|options| options.read_amplification.as_deref())
            .map_or_else(not_polled, |factor| word(&amplification_text(factor))),
        WorkloadField::WarmupTime => amount(
            key.client.warmup_time_us,
            duration_text(key.client.warmup_time_us),
        ),
        WorkloadField::HighLevelApi => flag(key.client.high_level_api),
        WorkloadField::ReuseStreams => flag(key.client.reuse_streams),
        WorkloadField::NoDelay => flag(key.tcp.nodelay),
        WorkloadField::Tls => flag(key.tcp.tls),
        WorkloadField::Workload => word(key.workload.as_deref().unwrap_or(NO_LABEL)),
    }
}

/// Why the topic can have different options from the ones the run recorded.
pub fn topic_note(summary: &BenchmarkSummary, options: &BenchmarkOptions) -> Option<&'static str> {
    if options.client.reuse_streams {
        Some(REUSED_STREAMS_NOTE)
    } else if summary.kind_family == KindFamily::Consumer {
        Some(CONSUMER_TOPIC_NOTE)
    } else {
        None
    }
}

/// Where the run ran and what it measured.
pub fn environment(summary: &BenchmarkSummary, report: &BenchmarkReportLight) -> Vec<Fact> {
    let mut facts = vec![
        fact(MACHINE_LABEL, MachineName(&summary.machine).to_string()),
        fact(TOPOLOGY_LABEL, nodes_text(summary.nodes)),
    ];
    let server_version = &report.server_stats.iggy_server_version;
    if !server_version.is_empty() {
        facts.push(fact("Server version", server_version.clone()));
    }
    facts.extend(
        report
            .params
            .gitref
            .clone()
            .map(|gitref| fact("Git ref", gitref)),
    );
    facts.extend(
        summary
            .version_order
            .gitref_date_us
            .and_then(DayLabel::from_unix_micros)
            .map(|date| fact("Commit date", date.day)),
    );
    facts
}

/// The machine that ran the bench. In a cluster run, the server nodes are other machines.
pub fn bench_host(hardware: &BenchmarkHardware) -> Vec<Fact> {
    BENCH_HOST_LABELS
        .into_iter()
        .zip(bench_host_values(hardware))
        .filter_map(|(label, value)| value.map(|value| fact(label, value)))
        .collect()
}

/// Every fact of [`bench_host`] in the order of [`BENCH_HOST_LABELS`], with `None` for a count
/// that an older report leaves at 0.
pub fn bench_host_values(hardware: &BenchmarkHardware) -> [Option<String>; BENCH_HOST_FACTS] {
    [
        Some(hardware.cpu_name.clone()),
        (hardware.cpu_cores > 0).then(|| hardware.cpu_cores.to_string()),
        memory_text(hardware.total_memory_mb),
        Some(hardware.os_name.clone()),
        Some(hardware.os_version.clone()),
    ]
}

/// The stats that the server reported at the end of the run.
pub fn server_facts(stats: &BenchmarkServerStats) -> Vec<FactGroup> {
    let mut groups = vec![
        FactGroup {
            title: "Server process",
            facts: vec![
                fact("Host name", stats.hostname.clone()),
                fact("OS", stats.os_name.clone()),
                fact("OS version", stats.os_version.clone()),
                fact("Kernel", stats.kernel_version.clone()),
                fact("Process ID", stats.process_id.to_string()),
                fact("Run time", run_time_text(stats.run_time)),
            ],
        },
        FactGroup {
            title: "Resources",
            facts: vec![
                fact("Server CPU usage", usage_text(stats.cpu_usage.into())),
                fact("Host CPU usage", usage_text(stats.total_cpu_usage.into())),
                fact("Server memory", format_bytes(stats.memory_usage)),
                fact("Host memory", format_bytes(stats.total_memory)),
                fact("Available memory", format_bytes(stats.available_memory)),
            ],
        },
        FactGroup {
            title: "Storage",
            facts: vec![
                fact("Read", format_bytes(stats.read_bytes)),
                fact("Written", format_bytes(stats.written_bytes)),
                fact("Message data", format_bytes(stats.messages_size_bytes)),
            ],
        },
    ];
    groups.extend(cache_facts(stats));
    groups.push(FactGroup {
        title: "Messaging",
        facts: vec![
            fact("Messages", group_digits(stats.messages_count)),
            fact("Streams", group_digits(stats.streams_count.into())),
            fact("Topics", group_digits(stats.topics_count.into())),
            fact("Partitions", group_digits(stats.partitions_count.into())),
            fact("Segments", group_digits(stats.segments_count.into())),
            fact("Clients", group_digits(stats.clients_count.into())),
            fact(
                "Consumer groups",
                group_digits(stats.consumer_groups_count.into()),
            ),
        ],
    });
    groups
}

/// The caches of all partitions together. `None` for a report without cache metrics, and no hit
/// ratio when nothing accessed the cache.
fn cache_facts(stats: &BenchmarkServerStats) -> Option<FactGroup> {
    if stats.cache_metrics.is_empty() {
        return None;
    }
    let (hits, misses) =
        stats
            .cache_metrics
            .values()
            .fold((0_u64, 0_u64), |(hits, misses), partition| {
                (
                    hits.saturating_add(partition.hits),
                    misses.saturating_add(partition.misses),
                )
            });
    let mut facts = vec![
        fact("Hits", group_digits(hits)),
        fact("Misses", group_digits(misses)),
    ];
    let accesses = hits.saturating_add(misses);
    if accesses > 0 {
        facts.push(fact(
            "Hit ratio",
            usage_text(hits as f64 / accesses as f64 * PERCENT),
        ));
    }
    Some(FactGroup {
        title: "Cache",
        facts,
    })
}

/// The rungs from min to max. The five percentiles that the delta rule compares always show.
pub fn ladder(group: &BenchmarkGroupMetricsSummary) -> Vec<LadderRow> {
    let percentiles = [
        (Metric::P50, group.average_p50_latency_ms),
        (Metric::P90, group.average_p90_latency_ms),
        (Metric::P95, group.average_p95_latency_ms),
        (Metric::P99, group.average_p99_latency_ms),
        (Metric::P999, group.average_p999_latency_ms),
    ]
    .map(|(metric, value)| (metric_name(metric), value, Some(metric)));
    let rungs: Vec<_> = iter::once((MIN_LABEL, group.min_latency_ms, None))
        .chain(percentiles)
        .chain([
            (P9999_LABEL, group.average_p9999_latency_ms, None),
            (MAX_LABEL, group.max_latency_ms, None),
        ])
        .filter(|&(_, value_ms, metric)| metric.is_some() || is_recorded(value_ms))
        .collect();
    let positive = || rungs.iter().map(|rung| rung.1).filter(|value| *value > 0.0);
    let lowest = positive().fold(f64::INFINITY, f64::min);
    let span = (positive().fold(0.0, f64::max) / lowest).ln();
    rungs
        .iter()
        .map(|&(label, value_ms, metric)| LadderRow {
            label,
            value_ms,
            metric,
            bar_pct: if value_ms <= 0.0 {
                MIN_BAR_PCT
            } else if span > 0.0 {
                MIN_BAR_PCT + (FULL_BAR_PCT - MIN_BAR_PCT) * (value_ms / lowest).ln() / span
            } else {
                FULL_BAR_PCT
            },
        })
        .collect()
}

/// The values next to the ladder.
pub fn ladder_side(group: &BenchmarkGroupMetricsSummary) -> Vec<Fact> {
    [
        (AVERAGE_LABEL, group.average_latency_ms),
        ("Std. deviation", group.std_dev_latency_ms),
    ]
    .into_iter()
    .filter(|&(_, value_ms)| is_recorded(value_ms))
    .map(|(label, value_ms)| fact(label, milliseconds(value_ms)))
    .collect()
}

/// Old reports lack min, max and std dev, and serde fills in 0. A real latency or spread over many
/// samples is never 0, so a row without a delta and with 0 has no value to show.
pub fn is_recorded(value_ms: f64) -> bool {
    value_ms > 0.0
}

fn fact(label: &'static str, value: impl Into<String>) -> Fact {
    Fact {
        label,
        value: value.into(),
    }
}

/// For example `1 s`, `250 ms` or `none`.
fn duration_text(micros: u64) -> String {
    if micros == 0 {
        "none".to_owned()
    } else if micros.is_multiple_of(MICROS_PER_SECOND) {
        format!("{} s", group_digits(micros / MICROS_PER_SECOND))
    } else if micros.is_multiple_of(MICROS_PER_MILLISECOND) {
        format!("{} ms", group_digits(micros / MICROS_PER_MILLISECOND))
    } else {
        format!("{} µs", group_digits(micros))
    }
}

/// For example `45 s`, `2 min 14 s` or `1 h 0 min 5 s`, rounded down to the second.
fn run_time_text(micros: u64) -> String {
    let total = micros / MICROS_PER_SECOND;
    let (hours, minutes, seconds) = (
        total / SECONDS_PER_HOUR,
        total % SECONDS_PER_HOUR / SECONDS_PER_MINUTE,
        total % SECONDS_PER_MINUTE,
    );
    match (hours, minutes) {
        (0, 0) => format!("{seconds} s"),
        (0, _) => format!("{minutes} min {seconds} s"),
        _ => format!("{hours} h {minutes} min {seconds} s"),
    }
}

/// For example `12.5%`.
fn usage_text(percent: f64) -> String {
    format!("{percent:.1}%")
}

fn message_size_value(size: BenchmarkNumericParameter) -> String {
    format!("{} B", numeric_text(size))
}

fn rate_limit_value(rate_limit: Option<&str>) -> String {
    rate_limit.map_or_else(|| NO_RATE_LIMIT.to_owned(), rate_text)
}

/// Batches and bytes are different units, so this only orders volumes of the same unit.
fn data_volume_amount(volume: DataVolumeKey) -> u64 {
    match volume {
        DataVolumeKey::Configured(BenchmarkDataVolume::MessageBatchesPerActor(batches)) => {
            batches.into()
        }
        DataVolumeKey::Configured(BenchmarkDataVolume::TotalDataBytes(bytes)) => bytes,
        DataVolumeKey::CompletedBatches(batches) => batches,
    }
}

fn messages_required_to_save_text(count: Option<u32>) -> String {
    count.map_or_else(
        || SERVER_DEFAULT.to_owned(),
        |count| group_digits(count.into()),
    )
}

fn max_topic_size_text(size: BenchmarkMaxTopicSize) -> String {
    match size {
        BenchmarkMaxTopicSize::ServerDefault => SERVER_DEFAULT.to_owned(),
        BenchmarkMaxTopicSize::Unlimited => "unlimited".to_owned(),
        BenchmarkMaxTopicSize::Bytes(bytes) => format_bytes(bytes),
    }
}

fn message_expiry_text(expiry: BenchmarkMessageExpiry) -> String {
    match expiry {
        BenchmarkMessageExpiry::ServerDefault => SERVER_DEFAULT.to_owned(),
        BenchmarkMessageExpiry::NeverExpire => "never".to_owned(),
        BenchmarkMessageExpiry::AfterUs(micros) => duration_text(micros),
    }
}

fn max_bytes_text(bytes: Option<u32>) -> String {
    bytes.map_or_else(
        || BENCH_DEFAULT.to_owned(),
        |bytes| format_bytes(bytes.into()),
    )
}

/// The report holds the factor as a number and the workload key as its exact text.
fn amplification_text(factor: impl fmt::Display) -> String {
    format!("{factor}x")
}

const fn yes_no(value: bool) -> &'static str {
    if value { "yes" } else { "no" }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        RELEASE, consumer, group_summary, params, producer, rate_limited, run, summarize,
    };
    use bench_report::benchmark_kind::BenchmarkKind;
    use bench_report::numeric_parameter::BenchmarkNumericParameter;
    use bench_report::options::BenchmarkDurability;
    use bench_report::params::BenchmarkDataVolume;
    use bench_report::server_stats::{BenchmarkCacheMetrics, BenchmarkCacheMetricsKey};

    const MACHINE: &str = "i4i-cluster3";
    const RECORDED_COMMAND: &str = "iggy-bench --rate-limit 800MB pinned-producer tcp";
    const REBUILT_COMMAND: &str = "iggy-bench -m 1000 -p 1 pinned-producer tcp";

    #[test]
    fn given_each_kind_family_when_writing_conditions_should_list_typed_fields_in_order() {
        let mut producer = rate_limited(producer());
        producer.data_volume = Some(BenchmarkDataVolume::MessageBatchesPerActor(60_000));
        let mut consumer = consumer();
        consumer.data_volume = Some(BenchmarkDataVolume::MessageBatchesPerActor(1));
        let mut mixed = params(BenchmarkKind::PinnedProducerAndConsumer, 8, 8);
        mixed.message_size = BenchmarkNumericParameter::Range { min: 200, max: 500 };
        mixed.data_volume = Some(BenchmarkDataVolume::TotalDataBytes(60_000_000_000));
        let mut end_to_end = params(BenchmarkKind::EndToEndProducingConsumer, 20, 0);
        end_to_end.data_volume = Some(BenchmarkDataVolume::TotalDataBytes(8_000_000_000));
        end_to_end.transport = BenchmarkTransport::Quic;
        let mut cluster_run = run(4, end_to_end, MACHINE, RELEASE);
        cluster_run.nodes = 3;
        let runs = [
            run(1, producer, MACHINE, RELEASE),
            run(2, consumer, MACHINE, RELEASE),
            run(3, mixed, MACHINE, RELEASE),
            cluster_run,
        ];

        let lines: Vec<_> = runs.iter().map(conditions).collect();

        assert_eq!(
            lines,
            [
                "1,000 B messages · 1,000 per batch · 60,000 batches per actor · rate limit \
                 800 MB/s · TCP · replicated · i4i-cluster3 · 1 node",
                "1,000 B messages · 1,000 per batch · 1 batch per actor · no rate limit · TCP · \
                 i4i-cluster3 · 1 node",
                "200 to 500 B messages · 1,000 per batch · 60.00 GB in total · no rate limit · \
                 TCP · replicated · i4i-cluster3 · 1 node",
                "1,000 B messages · 1,000 per batch · 8.00 GB in total · no rate limit · QUIC · \
                 replicated · i4i-cluster3 · 3 nodes",
            ]
        );
    }

    #[test]
    fn given_report_without_command_line_when_reading_command_should_use_rebuilt_one() {
        let mut recorded = producer();
        recorded.command_line = Some(RECORDED_COMMAND.to_owned());
        recorded.bench_command = REBUILT_COMMAND.to_owned();
        let mut old = producer();
        old.bench_command = REBUILT_COMMAND.to_owned();

        assert_eq!(
            Command::of(&recorded),
            Some(Command {
                text: RECORDED_COMMAND.to_owned(),
                rebuilt: false
            })
        );
        assert_eq!(
            Command::of(&old),
            Some(Command {
                text: REBUILT_COMMAND.to_owned(),
                rebuilt: true
            })
        );
        assert_eq!(Command::of(&producer()), None);
    }

    #[test]
    fn given_recorded_options_when_grouping_should_name_defaults_and_reused_topics() {
        let mut params = consumer();
        let mut options = BenchmarkOptions::legacy(BenchmarkKind::PinnedConsumer);
        options.topic.durability = BenchmarkDurability::Persisted;
        options.client.warmup_time_us = 2 * MICROS_PER_SECOND;
        params.options = Some(options.clone());
        let summary = summarize(params.clone());

        let groups = option_groups(&params);

        let titles: Vec<_> = groups.iter().map(|group| group.title).collect();
        assert_eq!(titles, ["Topic", "Consumer", "Client", "TCP"]);
        let values: Vec<_> = groups
            .iter()
            .flat_map(|group| &group.facts)
            .map(|fact| (fact.label, fact.value.as_str()))
            .collect();
        assert_eq!(
            values,
            [
                ("Durability", "persisted"),
                ("Messages required to save", "server default"),
                ("Max topic size", "unlimited"),
                ("Message expiry", "never"),
                ("Offset durability", "replicated"),
                ("Max wait", "1 s"),
                ("Min count", "1"),
                ("Max bytes", "bench default"),
                ("Warmup", "2 s"),
                ("High-level API", "no"),
                ("Reuse streams", "no"),
                ("No delay", "no"),
                ("TLS", "no"),
            ]
        );
        assert!(option_groups(&consumer()).is_empty(), "no recorded options");
        assert_eq!(topic_note(&summary, &options), Some(CONSUMER_TOPIC_NOTE));
        options.client.reuse_streams = true;
        assert_eq!(topic_note(&summary, &options), Some(REUSED_STREAMS_NOTE));
        let fresh = BenchmarkOptions::legacy(BenchmarkKind::PinnedProducer);
        assert_eq!(topic_note(&summarize(producer()), &fresh), None);
    }

    #[test]
    fn given_percentiles_when_building_ladder_should_scale_bars_on_log_axis() {
        let rows = ladder(&group_summary());

        let labels: Vec<_> = rows.iter().map(|row| row.label).collect();
        assert_eq!(
            labels,
            ["Min", "P50", "P90", "P95", "P99", "P99.9", "P99.99", "Max"]
        );
        assert_eq!(rows[0].bar_pct, MIN_BAR_PCT);
        assert_eq!(rows[7].bar_pct, FULL_BAR_PCT);
        let halfway = MIN_BAR_PCT + (FULL_BAR_PCT - MIN_BAR_PCT) / 2.0;
        assert!(
            (rows[1].bar_pct - halfway).abs() < 1e-9,
            "1 ms sits halfway between 0.1 ms and 10 ms on a log axis, got {}",
            rows[1].bar_pct
        );
        assert_eq!(rows[1].metric, Some(Metric::P50));
        assert_eq!(rows[6].metric, None);
        assert_eq!(
            ladder_side(&group_summary()),
            [
                fact("Average", "1.40 ms"),
                fact("Std. deviation", "0.700 ms")
            ]
        );
    }

    #[test]
    fn given_old_report_without_min_max_and_std_dev_when_building_ladder_should_leave_them_out() {
        let mut summary = serde_json::to_value(group_summary()).expect("a summary serializes");
        let fields = summary.as_object_mut().expect("a summary is a JSON object");
        for field in ["min_latency_ms", "max_latency_ms", "std_dev_latency_ms"] {
            fields.remove(field);
        }
        let group: BenchmarkGroupMetricsSummary =
            serde_json::from_value(summary).expect("a summary of an old report parses");

        let labels: Vec<_> = ladder(&group).iter().map(|row| row.label).collect();

        assert_eq!(labels, ["P50", "P90", "P95", "P99", "P99.9", "P99.99"]);
        assert_eq!(ladder_side(&group), [fact("Average", "1.40 ms")]);
    }

    #[test]
    fn given_zero_values_when_building_ladder_should_hide_only_rows_without_delta() {
        let group = BenchmarkGroupMetricsSummary {
            average_p999_latency_ms: 0.0,
            average_p9999_latency_ms: 0.0,
            average_latency_ms: 0.0,
            ..group_summary()
        };

        let labels: Vec<_> = ladder(&group).iter().map(|row| row.label).collect();

        assert_eq!(labels, ["Min", "P50", "P90", "P95", "P99", "P99.9", "Max"]);
        assert_eq!(ladder_side(&group), [fact("Std. deviation", "0.700 ms")]);
    }

    /// The stats of a producer run, with the hits and misses of each partition of `cache`.
    fn server_stats(cache: &[(u64, u64)]) -> BenchmarkServerStats {
        BenchmarkServerStats {
            process_id: 620_591,
            cpu_usage: 570.5,
            total_cpu_usage: 23.84,
            memory_usage: 85_123_072,
            total_memory: 33_547_599_872,
            available_memory: 25_320_759_296,
            run_time: 7_384_250_000,
            written_bytes: 11_727_233_024,
            messages_size_bytes: 11_601_920_000,
            streams_count: 1,
            topics_count: 1,
            partitions_count: 24,
            segments_count: 24,
            messages_count: 80_000_000,
            clients_count: 1,
            hostname: "atlas".to_owned(),
            os_name: "Pop!_OS".to_owned(),
            os_version: "Linux (Pop!_OS 22.04)".to_owned(),
            kernel_version: "6.9.3-76060903-generic".to_owned(),
            cache_metrics: (1_u32..)
                .zip(cache)
                .map(|(partition_id, &(hits, misses))| {
                    let key = BenchmarkCacheMetricsKey {
                        stream_id: 1,
                        topic_id: 1,
                        partition_id,
                    };
                    let metrics = BenchmarkCacheMetrics {
                        hits,
                        misses,
                        hit_ratio: 0.0,
                    };
                    (key, metrics)
                })
                .collect(),
            ..BenchmarkServerStats::default()
        }
    }

    #[test]
    fn given_known_stats_when_listing_server_facts_should_give_expected_texts() {
        let groups = server_facts(&server_stats(&[(300, 100), (600, 0)]));

        let texts: Vec<(&str, Vec<(&str, &str)>)> = groups
            .iter()
            .map(|group| {
                let facts = group
                    .facts
                    .iter()
                    .map(|fact| (fact.label, fact.value.as_str()))
                    .collect();
                (group.title, facts)
            })
            .collect();
        assert_eq!(
            texts,
            [
                (
                    "Server process",
                    vec![
                        ("Host name", "atlas"),
                        ("OS", "Pop!_OS"),
                        ("OS version", "Linux (Pop!_OS 22.04)"),
                        ("Kernel", "6.9.3-76060903-generic"),
                        ("Process ID", "620591"),
                        ("Run time", "2 h 3 min 4 s"),
                    ]
                ),
                (
                    "Resources",
                    vec![
                        ("Server CPU usage", "570.5%"),
                        ("Host CPU usage", "23.8%"),
                        ("Server memory", "85.12 MB"),
                        ("Host memory", "33.55 GB"),
                        ("Available memory", "25.32 GB"),
                    ]
                ),
                (
                    "Storage",
                    vec![
                        ("Read", "0 B"),
                        ("Written", "11.73 GB"),
                        ("Message data", "11.60 GB"),
                    ]
                ),
                (
                    "Cache",
                    vec![("Hits", "900"), ("Misses", "100"), ("Hit ratio", "90.0%")]
                ),
                (
                    "Messaging",
                    vec![
                        ("Messages", "80,000,000"),
                        ("Streams", "1"),
                        ("Topics", "1"),
                        ("Partitions", "24"),
                        ("Segments", "24"),
                        ("Clients", "1"),
                        ("Consumer groups", "0"),
                    ]
                ),
            ]
        );
    }

    #[test]
    fn given_cache_without_accesses_when_listing_server_facts_should_show_no_hit_ratio() {
        let cache = |stats: BenchmarkServerStats| {
            server_facts(&stats)
                .into_iter()
                .find(|group| group.title == "Cache")
        };

        assert_eq!(
            cache(server_stats(&[(0, 0), (0, 0)])),
            Some(FactGroup {
                title: "Cache",
                facts: vec![fact("Hits", "0"), fact("Misses", "0")],
            })
        );
        assert_eq!(
            cache(server_stats(&[])),
            None,
            "a report without cache metrics has no cache group"
        );
    }
}
