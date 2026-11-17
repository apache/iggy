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

//! The `[http.metrics]` scrape surface: the legacy-parity metric registry
//! (entity gauges plus the request counter) and the config gate deciding
//! whether the route is mounted. The scrape handler itself lives with the
//! other route handlers so this leaf never imports the state hub.

use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use configs::http::HttpMetricsConfig;
use iggy_common::{IggyError, stats_rollup_underflows};
use prometheus_client::collector::Collector;
use prometheus_client::encoding::text::{encode_eof, encode_registry};
use prometheus_client::encoding::{DescriptorEncoder, EncodeMetric};
use prometheus_client::metrics::MetricType;
use prometheus_client::metrics::counter::{ConstCounter, Counter};
use prometheus_client::metrics::gauge::{ConstGauge, Gauge};
use prometheus_client::registry::Registry;
use server_common::{DISK_USAGE, DiskUsage};
use tracing::error;

/// Exports the process-wide clamped-rollup count as a counter series, read at
/// encode time rather than mirrored into one.
///
/// Non-zero means a partition, topic or stream total was asked to give back
/// more than it held, so what that scope now reports is low, and stays low
/// until a rebuild or a restart. All three levels clamp and all three feed this
/// one counter, which carries no scope label -- the `warn!` in `iggy_common`
/// names the scope and counter that moved it.
///
/// Not "alert on any increase". A delete, a partition teardown and a
/// snapshot restore each open a window where a retention pass hands back bytes
/// the parents have already given up, and the clamp is the intended outcome
/// there -- `given_a_rolled_back_partition_when_a_late_decrement_arrives_should_leave_siblings_alone`
/// in `core/common/src/types/streaming_stats.rs` drives exactly that. What is
/// worth paging on is a bounded rate OUTSIDE those windows: that is the shape
/// that says the tree is diverging rather than settling.
///
/// A counter, not a gauge: the source only ever climbs within a process, so
/// `rate()` and `increase()` are the queries an operator wants, and both are
/// counter-only. A restart resets the source and the exposition together,
/// which is the counter reset Prometheus expects.
///
/// Read only by the `/metrics` scrape, so it needs `http.enabled` and
/// `http.metrics.enabled` -- as does every other series in this registry,
/// which is the only Prometheus surface the server has. A TCP-only or
/// QUIC-only deployment exports nothing, and the per-scope `warn!` in
/// `iggy_common` is the whole signal there.
#[derive(Debug)]
struct StatsRollupUnderflows;

impl Collector for StatsRollupUnderflows {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        let counter = ConstCounter::new(stats_rollup_underflows());
        let metric_encoder = encoder.encode_descriptor(
            "stats_rollup_underflows",
            "total count of aggregate stats decrements clamped at zero",
            None,
            counter.metric_type(),
        )?;
        counter.encode(metric_encoder)
    }
}

/// Exports the node's free-space reserve, read at encode time. Absent when
/// the reserve is disabled. It describes the host's volume, so it lives in
/// the stats registry next to [`DiskUsageMetrics`].
#[derive(Debug)]
struct DataDiskFreeSpace;

impl Collector for DataDiskFreeSpace {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        let Some(reserve) = crate::sysinfo_probe::free_space_reserve() else {
            return Ok(());
        };
        encode_free_space(&mut encoder, &reserve)
    }
}

fn encode_free_space(
    encoder: &mut DescriptorEncoder,
    reserve: &partitions::FreeSpaceReserve,
) -> Result<(), std::fmt::Error> {
    let gauges = [
        (
            "data_disk_free_bytes",
            "estimated free space on the disk of the data directory in bytes: the last sample minus what Iggy wrote since and what it accepted and has not written yet",
            reserve.free_bytes(),
        ),
        (
            "data_disk_free_space_reserve_bytes",
            "free space the node keeps on the disk of the data directory in bytes",
            reserve.reserve_bytes(),
        ),
        (
            "data_disk_below_free_space_reserve",
            "1 while the data disk is below its reserve and the node refuses new messages, else 0",
            u64::from(reserve.is_short()),
        ),
    ];
    for (name, help, value) in gauges {
        let gauge = ConstGauge::new(value);
        gauge.encode(encoder.encode_descriptor(name, help, None, gauge.metric_type())?)?;
    }
    let refused = ConstCounter::new(reserve.refused_requests());
    refused.encode(encoder.encode_descriptor(
        "messages_refused_for_free_space",
        "total count of message requests refused while the data disk was below its reserve",
        None,
        refused.metric_type(),
    )?)
}

/// One topic's usage as the scrape handler sampled it from the streams STM.
#[derive(Debug)]
pub(in crate::http) struct TopicUsageSample {
    pub(in crate::http) stream: Arc<str>,
    pub(in crate::http) topic: Arc<str>,
    pub(in crate::http) size_bytes: u64,
    pub(in crate::http) messages: u64,
    /// `None` for an uncapped topic, so its series is absent rather than 0 and
    /// `topic_size_bytes / topic_max_size_bytes` never yields `+Inf`.
    pub(in crate::http) max_size_bytes: Option<u64>,
}

/// Per-topic `{stream, topic}` series, replaced wholesale on every scrape so a
/// deleted or renamed topic drops out on the next one.
///
/// The collector cannot read the streams STM itself: it lives on shard 0 and
/// is `!Sync`, while a registered collector must be `Send + Sync`, so the
/// scrape handler hands it a snapshot instead.
#[derive(Debug, Clone, Default)]
pub(in crate::http) struct TopicUsage {
    samples: Arc<Mutex<Vec<TopicUsageSample>>>,
}

impl TopicUsage {
    pub(in crate::http) fn replace(&self, samples: Vec<TopicUsageSample>) {
        *self.samples.lock().unwrap_or_else(PoisonError::into_inner) = samples;
    }
}

impl Collector for TopicUsage {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        // The scrape handler refills the snapshot before every encode, so
        // keeping it past this encode only pins every topic name until the
        // next scrape.
        let samples = mem::take(&mut *self.samples.lock().unwrap_or_else(PoisonError::into_inner));
        encode_topic_gauge(
            &mut encoder,
            "topic_size_bytes",
            "size of the topic's stored messages in bytes",
            &samples,
            |sample| Some(sample.size_bytes),
        )?;
        encode_topic_gauge(
            &mut encoder,
            "topic_messages",
            "count of the topic's stored messages",
            &samples,
            |sample| Some(sample.messages),
        )?;
        encode_topic_gauge(
            &mut encoder,
            "topic_max_size_bytes",
            concat!(
                "configured size cap of the topic in bytes, absent when unlimited; ",
                "retention may keep more",
            ),
            &samples,
            |sample| sample.max_size_bytes,
        )
    }
}

fn encode_topic_gauge(
    encoder: &mut DescriptorEncoder,
    name: &str,
    help: &str,
    samples: &[TopicUsageSample],
    value: impl Fn(&TopicUsageSample) -> Option<u64>,
) -> Result<(), std::fmt::Error> {
    let mut metric_encoder = encoder.encode_descriptor(name, help, None, MetricType::Gauge)?;
    for sample in samples {
        let Some(value) = value(sample) else {
            continue;
        };
        let labels = [("stream", &*sample.stream), ("topic", &*sample.topic)];
        ConstGauge::new(value).encode(metric_encoder.encode_family(&labels)?)?;
    }
    Ok(())
}

/// Exports the disk usage of the volume holding the data directory, read at
/// encode time like [`StatsRollupUnderflows`].
///
/// `volume_used_bytes` is the estimate: the last `statvfs` sample plus what
/// Iggy has written since, so it moves with a burst instead of once a second.
/// It counts every file on the volume, not only Iggy's. It is not exported
/// until the first sample, since the charges alone say nothing about the
/// volume. Used means total minus free blocks (`f_blocks - f_bfree`), as `df`
/// reports it. The free space in `/stats` is `f_bavail`, which leaves out the
/// blocks reserved for root, so the two differ by that reserve.
///
/// `disk_allocated_bytes` counts what the write sites charged since the
/// process started, the part of the estimate that samples never move. Its
/// increase over an interval follows what Iggy allocated in it, with two
/// overcounts: a write that failed stays charged, and so does a reservation
/// that failed or allocated nothing, while the writes into its segment are
/// charged again. A reservation charges up front what it adds to the blocks
/// the file already owns, which is the whole segment only for a new file.
/// Unlike `volume_used_bytes`, it does not move with deletes or with other
/// processes on the volume.
///
/// Both describe the host's volume, which `/stats` shows only to a user its
/// rule admits, so they live in [`HttpMetrics`]' stats registry.
///
/// It holds the estimate it reads, so a test can export one of its own
/// instead of the process-wide one other tests charge concurrently.
#[derive(Debug)]
struct DiskUsageMetrics(&'static DiskUsage);

impl Collector for DiskUsageMetrics {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        if let Some(used_bytes) = self.0.used_bytes() {
            let gauge = ConstGauge::new(used_bytes);
            let metric_encoder = encoder.encode_descriptor(
                "volume_used_bytes",
                "estimated bytes used on the volume holding the data directory, as total minus free blocks like df",
                None,
                gauge.metric_type(),
            )?;
            gauge.encode(metric_encoder)?;
        }
        let counter = ConstCounter::new(self.0.charged_bytes());
        let metric_encoder = encoder.encode_descriptor(
            "disk_allocated_bytes",
            "bytes Iggy allocated on the volume holding the data directory since the process started, failed writes and reservations included",
            None,
            counter.metric_type(),
        )?;
        counter.encode(metric_encoder)
    }
}

/// The legacy server's metric set, registered under the same names and help
/// texts so existing dashboards and alerts keep working unchanged.
///
/// Unlike the legacy server, the entity gauges are not counted at mutation
/// sites: the scrape handler (`http::handlers::get_metrics`) samples the live
/// state on every scrape, so a gauge can never drift from the state it
/// describes.
pub(in crate::http) struct HttpMetrics {
    registry: Registry,
    /// The per-topic and disk series. They name every stream and topic and
    /// describe the host's volume, so a scrape encodes them only for a user
    /// the `/stats` rule admits.
    stats_registry: Registry,
    http_requests: Counter,
    pub(in crate::http) streams: Gauge,
    pub(in crate::http) topics: Gauge,
    pub(in crate::http) partitions: Gauge,
    pub(in crate::http) segments: Gauge,
    pub(in crate::http) messages: Gauge,
    pub(in crate::http) users: Gauge,
    pub(in crate::http) clients: Gauge,
    pub(in crate::http) topic_usage: TopicUsage,
    /// Partition primary lookups on the owning shard for HTTP partition requests.
    pub(in crate::http) partition_primary_probes: Counter,
    /// Attempts to relay an HTTP partition request to another node.
    pub(in crate::http) partition_relays: Counter,
    /// Requests for the metadata primary's commit floor, one per attempt.
    pub(in crate::http) metadata_floor_reads: Counter,
    /// VSR `Register` submits for HTTP sessions, one per minted client id.
    pub(in crate::http) session_registers: Counter,
    last_output_len: AtomicUsize,
}

impl HttpMetrics {
    pub(in crate::http) fn init(shard_metrics_all: &[shard::metrics::ShardMetrics]) -> Self {
        let mut registry = Registry::default();
        let http_requests = Counter::default();
        let streams = Gauge::default();
        let topics = Gauge::default();
        let partitions = Gauge::default();
        let segments = Gauge::default();
        let messages = Gauge::default();
        let users = Gauge::default();
        let clients = Gauge::default();
        registry.register(
            "http_requests",
            "total count of http_requests",
            http_requests.clone(),
        );
        registry.register("streams", "total count of streams", streams.clone());
        registry.register("topics", "total count of topics", topics.clone());
        registry.register(
            "partitions",
            "total count of partitions",
            partitions.clone(),
        );
        registry.register("segments", "total count of segments", segments.clone());
        registry.register("messages", "total count of messages", messages.clone());
        registry.register("users", "total count of users", users.clone());
        registry.register("clients", "total count of clients", clients.clone());
        // Not legacy parity: what routing the HTTP partition requests cost.
        let partition_primary_probes = Counter::default();
        let partition_relays = Counter::default();
        let metadata_floor_reads = Counter::default();
        let session_registers = Counter::default();
        registry.register(
            "http_partition_primary_probes",
            "partition primary lookups on the owning shard for HTTP partition requests",
            partition_primary_probes.clone(),
        );
        registry.register(
            "http_partition_relays",
            "attempts to relay an HTTP partition request to the partition primary on another node",
            partition_relays.clone(),
        );
        registry.register(
            "http_metadata_floor_reads",
            "requests for the metadata primary's commit floor by linearizable HTTP partition reads",
            metadata_floor_reads.clone(),
        );
        registry.register(
            "http_session_registers",
            "VSR Register submits that open an HTTP session, a metadata commit each",
            session_registers.clone(),
        );
        // Not a legacy-parity metric, and not a mirrored one: the source is a
        // process-wide static the scrape reads directly.
        registry.register_collector(Box::new(StatsRollupUnderflows));
        let mut stats_registry = Registry::default();
        stats_registry.register_collector(Box::new(DataDiskFreeSpace));
        let topic_usage = TopicUsage::default();
        stats_registry.register_collector(Box::new(topic_usage.clone()));
        stats_registry.register_collector(Box::new(DiskUsageMetrics(&DISK_USAGE)));
        // Every shard's drop / reconcile / partition counters, one
        // `shard`-labelled sub-registry per shard so series stay per-shard
        // without a `shard_id` label in the counter label sets (see
        // `shard::metrics::FrameDropLabel`). The counters are Arc-backed:
        // each shard bumps its own handle on its own thread, and the scrape
        // on shard 0 reads the shared atomics.
        for (shard_id, shard_metrics) in shard_metrics_all.iter().enumerate() {
            let sub_registry = registry.sub_registry_with_label((
                std::borrow::Cow::Borrowed("shard"),
                std::borrow::Cow::Owned(shard_id.to_string()),
            ));
            shard_metrics.register(sub_registry);
        }
        Self {
            registry,
            stats_registry,
            http_requests,
            streams,
            topics,
            partitions,
            segments,
            messages,
            users,
            clients,
            topic_usage,
            partition_primary_probes,
            partition_relays,
            metadata_floor_reads,
            session_registers,
            last_output_len: AtomicUsize::new(0),
        }
    }

    /// Handle for the router's request-counting layer. The counter is
    /// `Arc`-backed, so bumping the clone bumps the registered metric.
    pub(in crate::http) fn request_counter(&self) -> Counter {
        self.http_requests.clone()
    }

    /// The exposition for one scrape, with the stats registry only when
    /// `exports_stats` says that the `/stats` rule admits the user.
    pub(in crate::http) fn formatted_output(&self, exports_stats: bool) -> String {
        let last = self.last_output_len.load(Ordering::Relaxed);
        let mut buffer = String::with_capacity(last + last / 8);
        if let Err(error) = self.encode(&mut buffer, exports_stats) {
            error!(%error, "failed to encode metrics");
        }
        self.last_output_len.store(buffer.len(), Ordering::Relaxed);
        buffer
    }

    fn encode(&self, buffer: &mut String, exports_stats: bool) -> Result<(), std::fmt::Error> {
        encode_registry(buffer, &self.registry)?;
        if exports_stats {
            encode_registry(buffer, &self.stats_registry)?;
        }
        encode_eof(buffer)
    }
}

/// Resolve the configured scrape path: `None` when `[http.metrics]` is
/// disabled, so the route is never mounted and the endpoint answers 404.
///
/// axum's `Router::route` panics on a path without a leading `/`, so an
/// enabled endpoint missing one is rejected as a configuration error before
/// the router is assembled.
///
/// # Errors
///
/// Returns [`IggyError::InvalidConfiguration`] when metrics are enabled and
/// the endpoint does not start with `/`.
pub(in crate::http) fn validated_endpoint(
    config: &HttpMetricsConfig,
) -> Result<Option<String>, IggyError> {
    if !config.enabled {
        return Ok(None);
    }
    if !config.endpoint.starts_with('/') {
        error!(
            endpoint = %config.endpoint,
            "invalid http.metrics.endpoint: the path must start with '/'"
        );
        return Err(IggyError::InvalidConfiguration);
    }
    Ok(Some(config.endpoint.clone()))
}

/// Clamp a count into the gauge's `i64` domain; only `messages` can pass
/// `i64::MAX` even in theory, the rest are bounded far below it.
pub(in crate::http) fn gauge_value(count: u64) -> i64 {
    i64::try_from(count).unwrap_or(i64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use prometheus_client::encoding::text::encode;
    use shard::metrics::{ShardMetrics, frame_drop_reason, frame_drop_variant};

    const MIB: u64 = 1024 * 1024;

    #[derive(Debug)]
    struct SampledFreeSpace(Arc<partitions::FreeSpaceReserve>);

    impl Collector for SampledFreeSpace {
        fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
            encode_free_space(&mut encoder, &self.0)
        }
    }

    #[test]
    fn given_a_short_data_disk_when_scraping_should_export_the_reserve_and_the_refusals() {
        static USAGE: DiskUsage = DiskUsage::new();
        USAGE.rebase(|| Some(((), 100 * MIB)));
        let reserve = Arc::new(partitions::FreeSpaceReserve::new(MIB, &USAGE));
        reserve.observe(2 * MIB, 100 * MIB);
        // Written since the sample, so the scrape shows it before the next one.
        drop(USAGE.charge(MIB + 1));
        assert!(!reserve.admit(0));
        reserve.count_refusal();
        let mut registry = Registry::default();
        registry.register_collector(Box::new(SampledFreeSpace(Arc::clone(&reserve))));

        let mut short = String::new();
        encode(&mut short, &registry).unwrap();
        USAGE.rebase(|| Some(((), 98 * MIB)));
        reserve.observe(4 * MIB, 98 * MIB);
        let mut recovered = String::new();
        encode(&mut recovered, &registry).unwrap();

        for line in [
            format!("data_disk_free_bytes {}", MIB - 1),
            format!("data_disk_free_space_reserve_bytes {MIB}"),
            "data_disk_below_free_space_reserve 1".to_owned(),
            "messages_refused_for_free_space_total 1".to_owned(),
        ] {
            assert!(short.lines().any(|found| found == line), "{line}\n{short}");
        }
        for line in [
            format!("data_disk_free_bytes {}", 4 * MIB),
            "data_disk_below_free_space_reserve 0".to_owned(),
            "messages_refused_for_free_space_total 1".to_owned(),
        ] {
            assert!(
                recovered.lines().any(|found| found == line),
                "{line}\n{recovered}"
            );
        }
    }

    #[test]
    fn shard_metrics_land_in_the_exposition_under_a_shard_label() {
        let shard_metrics = ShardMetrics::for_shard();
        shard_metrics.record_frame_drop(frame_drop_variant::CONSENSUS, frame_drop_reason::FULL);
        let metrics = HttpMetrics::init(&[shard_metrics]);
        let output = metrics.formatted_output(true);
        assert!(
            output.contains("frame_drops_total"),
            "shard frame-drop counter missing from exposition:\n{output}"
        );
        assert!(
            output.contains(r#"shard="0""#),
            "shard label missing from exposition:\n{output}"
        );
    }

    const PARITY_METRIC_NAMES: [&str; 8] = [
        "http_requests",
        "streams",
        "topics",
        "partitions",
        "segments",
        "messages",
        "users",
        "clients",
    ];

    fn metrics_config(enabled: bool, endpoint: &str) -> HttpMetricsConfig {
        HttpMetricsConfig {
            enabled,
            endpoint: endpoint.to_owned(),
        }
    }

    #[test]
    fn formatted_output_exposes_every_parity_metric() {
        let metrics = HttpMetrics::init(&[]);
        let output = metrics.formatted_output(true);
        for name in PARITY_METRIC_NAMES {
            assert!(
                output.contains(&format!("# TYPE {name} ")),
                "metric {name} missing from exposition:\n{output}"
            );
        }
        assert!(
            output.ends_with("# EOF\n"),
            "missing exposition trailer:\n{output}"
        );
    }

    /// Outside `PARITY_METRIC_NAMES`: the legacy server had no such series, so
    /// it gets its own assertion rather than a row in the parity list.
    ///
    /// The value is a process-wide static shared with every other test in this
    /// binary, so this asserts the series and its type, not a number.
    #[test]
    fn rollup_underflow_collector_lands_in_the_exposition() {
        let metrics = HttpMetrics::init(&[]);
        let output = metrics.formatted_output(true);
        assert!(
            output.contains("# TYPE stats_rollup_underflows counter\n"),
            "expected the collector's series in the exposition:\n{output}"
        );
        assert!(
            output.contains("\nstats_rollup_underflows_total "),
            "expected the collector's sample in the exposition:\n{output}"
        );
    }

    /// The registry reads the process-wide estimate, which other tests in this
    /// binary charge too, so this asserts that both series are registered and
    /// leaves their values to the tests below.
    #[test]
    fn registered_metrics_include_both_disk_usage_series() {
        DISK_USAGE.rebase(|| Some(((), 1)));
        let output = HttpMetrics::init(&[]).formatted_output(true);
        for series in ["volume_used_bytes gauge", "disk_allocated_bytes counter"] {
            assert!(
                output.contains(&format!("# TYPE {series}\n")),
                "expected {series} in the exposition:\n{output}"
            );
        }
    }

    #[test]
    fn given_no_sample_when_encoding_disk_usage_should_export_only_the_allocated_bytes() {
        static USAGE: DiskUsage = DiskUsage::new();
        drop(USAGE.charge(3 * MIB));

        let output = disk_usage_exposition(&USAGE);

        assert!(
            output.contains(&format!("\ndisk_allocated_bytes_total {}\n", 3 * MIB)),
            "expected the allocated bytes in the exposition:\n{output}"
        );
        assert!(
            !output.contains("volume_used_bytes"),
            "the estimate must stay hidden until a sample sets its base:\n{output}"
        );
    }

    #[test]
    fn given_a_sample_and_a_charge_when_encoding_disk_usage_should_export_both_values() {
        static USAGE: DiskUsage = DiskUsage::new();
        USAGE.rebase(|| Some(((), 100 * MIB)));
        let sampled = disk_usage_exposition(&USAGE);

        drop(USAGE.charge(MIB));
        let charged = disk_usage_exposition(&USAGE);

        for (output, used, total) in [(&sampled, 100 * MIB, 0), (&charged, 101 * MIB, MIB)] {
            assert!(
                output.contains(&format!("\nvolume_used_bytes {used}\n")),
                "expected the estimate {used} in the exposition:\n{output}"
            );
            assert!(
                output.contains(&format!("\ndisk_allocated_bytes_total {total}\n")),
                "expected the allocated bytes {total} in the exposition:\n{output}"
            );
        }
    }

    #[test]
    fn given_a_scrape_without_the_stats_permission_when_encoding_should_omit_topic_and_disk_series()
    {
        let metrics = HttpMetrics::init(&[]);
        let stats_series = ["topic_size_bytes", "disk_allocated_bytes"];

        metrics
            .topic_usage
            .replace(vec![topic_sample("orders", "eu", None)]);
        let denied = metrics.formatted_output(false);
        let allowed = metrics.formatted_output(true);

        for name in stats_series {
            assert!(
                !denied.contains(name),
                "a scrape without the permission must not export {name}:\n{denied}"
            );
            assert!(
                allowed.contains(name),
                "a scrape with the permission must export {name}:\n{allowed}"
            );
        }
        assert!(
            denied.contains("\nstreams ") && denied.ends_with("# EOF\n"),
            "a denied scrape still gets the aggregates and the trailer:\n{denied}"
        );
    }

    /// Both disk usage series of `usage`, encoded as `/metrics` would.
    fn disk_usage_exposition(usage: &'static DiskUsage) -> String {
        let mut registry = Registry::default();
        registry.register_collector(Box::new(DiskUsageMetrics(usage)));
        let mut output = String::new();
        encode(&mut output, &registry).expect("encode disk usage metrics");
        output
    }

    #[test]
    fn scraped_values_land_in_the_exposition() {
        let metrics = HttpMetrics::init(&[]);
        metrics.streams.set(1);
        metrics.topics.set(2);
        metrics.partitions.set(3);
        metrics.segments.set(4);
        metrics.messages.set(5);
        metrics.users.set(6);
        metrics.clients.set(7);
        metrics.request_counter().inc();
        let output = metrics.formatted_output(true);
        for line in [
            "streams 1",
            "topics 2",
            "partitions 3",
            "segments 4",
            "messages 5",
            "users 6",
            "clients 7",
            "http_requests_total 1",
        ] {
            assert!(
                output.contains(&format!("\n{line}\n")),
                "expected `{line}` in exposition:\n{output}"
            );
        }
    }

    #[test]
    fn gauge_value_clamps_past_i64_range() {
        assert_eq!(gauge_value(42), 42);
        assert_eq!(gauge_value(u64::MAX), i64::MAX);
    }

    #[test]
    fn validated_endpoint_disabled_yields_none() {
        assert!(matches!(
            validated_endpoint(&metrics_config(false, "/metrics")),
            Ok(None)
        ));
    }

    #[test]
    fn validated_endpoint_returns_enabled_path() {
        let endpoint = validated_endpoint(&metrics_config(true, "/metrics")).unwrap();
        assert_eq!(endpoint.as_deref(), Some("/metrics"));
    }

    fn topic_sample(stream: &str, topic: &str, max_size_bytes: Option<u64>) -> TopicUsageSample {
        TopicUsageSample {
            stream: Arc::from(stream),
            topic: Arc::from(topic),
            size_bytes: 4096,
            messages: 20,
            max_size_bytes,
        }
    }

    #[test]
    fn given_topic_samples_when_encoding_should_label_each_series_by_stream_and_topic() {
        let metrics = HttpMetrics::init(&[]);
        metrics.topic_usage.replace(vec![
            topic_sample("orders", "eu", Some(1_000_000)),
            topic_sample("orders", "us", None),
        ]);
        let output = metrics.formatted_output(true);
        for line in [
            "# TYPE topic_size_bytes gauge",
            "# TYPE topic_messages gauge",
            "# TYPE topic_max_size_bytes gauge",
            r#"topic_size_bytes{stream="orders",topic="eu"} 4096"#,
            r#"topic_size_bytes{stream="orders",topic="us"} 4096"#,
            r#"topic_messages{stream="orders",topic="eu"} 20"#,
            r#"topic_messages{stream="orders",topic="us"} 20"#,
            r#"topic_max_size_bytes{stream="orders",topic="eu"} 1000000"#,
        ] {
            assert!(
                output.contains(&format!("\n{line}\n")),
                "expected `{line}` in exposition:\n{output}"
            );
        }
        assert!(
            !output.contains(r#"topic_max_size_bytes{stream="orders",topic="us"}"#),
            "an uncapped topic must not export a max size:\n{output}"
        );
    }

    #[test]
    fn given_replaced_snapshot_when_encoding_should_drop_topics_no_longer_present() {
        let metrics = HttpMetrics::init(&[]);
        metrics
            .topic_usage
            .replace(vec![topic_sample("orders", "eu", None)]);
        metrics.topic_usage.replace(Vec::new());
        let output = metrics.formatted_output(true);
        assert!(
            !output.contains(r#"topic="eu""#),
            "a deleted topic must not outlive the next scrape:\n{output}"
        );
    }

    #[test]
    fn given_encoded_snapshot_when_scraping_again_without_refill_should_not_retain_it() {
        let metrics = HttpMetrics::init(&[]);
        metrics
            .topic_usage
            .replace(vec![topic_sample("orders", "eu", None)]);
        let first = metrics.formatted_output(true);
        assert!(
            first.contains(r#"topic="eu""#),
            "the snapshot must be encoded before it is released:\n{first}"
        );
        let second = metrics.formatted_output(true);
        assert!(
            !second.contains(r#"topic="eu""#),
            "the snapshot must not outlive the scrape that encoded it:\n{second}"
        );
    }

    #[test]
    fn given_name_with_exposition_syntax_when_encoding_should_escape_it() {
        let metrics = HttpMetrics::init(&[]);
        metrics
            .topic_usage
            .replace(vec![topic_sample("a\"b", "c\\d\ne", None)]);
        let output = metrics.formatted_output(true);
        assert!(
            output.contains(r#"topic_messages{stream="a\"b",topic="c\\d\ne"} 20"#),
            "label values must be escaped:\n{output}"
        );
    }

    #[test]
    fn validated_endpoint_rejects_missing_leading_slash() {
        assert!(matches!(
            validated_endpoint(&metrics_config(true, "metrics")),
            Err(IggyError::InvalidConfiguration)
        ));
    }
}
