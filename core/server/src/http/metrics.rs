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

use std::fmt::Write;
use std::mem;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use configs::http::HttpMetricsConfig;
use iggy_common::{IggyError, stats_rollup_underflows};
use prometheus_client::collector::Collector;
use prometheus_client::encoding::text::encode;
use prometheus_client::encoding::{
    DescriptorEncoder, EncodeLabelValue, EncodeMetric, LabelValueEncoder,
};
use prometheus_client::metrics::MetricType;
use prometheus_client::metrics::counter::{ConstCounter, Counter};
use prometheus_client::metrics::gauge::{ConstGauge, Gauge};
use prometheus_client::registry::Registry;
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

    fn take(&self) -> Vec<TopicUsageSample> {
        mem::take(&mut *self.samples.lock().unwrap_or_else(PoisonError::into_inner))
    }
}

impl Collector for TopicUsage {
    fn encode(&self, mut encoder: DescriptorEncoder) -> Result<(), std::fmt::Error> {
        let samples = self.samples.lock().unwrap_or_else(PoisonError::into_inner);
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
        let labels = [
            ("stream", EscapedLabelValue(&sample.stream)),
            ("topic", EscapedLabelValue(&sample.topic)),
        ];
        ConstGauge::new(value).encode(metric_encoder.encode_family(&labels)?)?;
    }
    Ok(())
}

/// prometheus-client writes label values verbatim, and stream and topic names
/// may contain any character the exposition format treats as syntax.
struct EscapedLabelValue<'a>(&'a str);

impl EncodeLabelValue for EscapedLabelValue<'_> {
    fn encode(&self, encoder: &mut LabelValueEncoder) -> Result<(), std::fmt::Error> {
        if !self.0.contains(['\\', '"', '\n']) {
            return encoder.write_str(self.0);
        }
        for character in self.0.chars() {
            match character {
                '\\' => encoder.write_str("\\\\")?,
                '"' => encoder.write_str("\\\"")?,
                '\n' => encoder.write_str("\\n")?,
                other => encoder.write_char(other)?,
            }
        }
        Ok(())
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
    http_requests: Counter,
    pub(in crate::http) streams: Gauge,
    pub(in crate::http) topics: Gauge,
    pub(in crate::http) partitions: Gauge,
    pub(in crate::http) segments: Gauge,
    pub(in crate::http) messages: Gauge,
    pub(in crate::http) users: Gauge,
    pub(in crate::http) clients: Gauge,
    pub(in crate::http) topic_usage: TopicUsage,
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
        // Not a legacy-parity metric, and not a mirrored one: the source is a
        // process-wide static the scrape reads directly.
        registry.register_collector(Box::new(StatsRollupUnderflows));
        let topic_usage = TopicUsage::default();
        registry.register_collector(Box::new(topic_usage.clone()));
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
            http_requests,
            streams,
            topics,
            partitions,
            segments,
            messages,
            users,
            clients,
            topic_usage,
            last_output_len: AtomicUsize::new(0),
        }
    }

    /// Handle for the router's request-counting layer. The counter is
    /// `Arc`-backed, so bumping the clone bumps the registered metric.
    pub(in crate::http) fn request_counter(&self) -> Counter {
        self.http_requests.clone()
    }

    pub(in crate::http) fn formatted_output(&self) -> String {
        let last = self.last_output_len.load(Ordering::Relaxed);
        let mut buffer = String::with_capacity(last + last / 8);
        if let Err(error) = encode(&mut buffer, &self.registry) {
            error!(%error, "failed to encode metrics");
        }
        // The scrape handler refills the snapshot before every encode, so
        // holding it past this point only pins every topic name until the
        // next scrape.
        drop(self.topic_usage.take());
        self.last_output_len.store(buffer.len(), Ordering::Relaxed);
        buffer
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
    use shard::metrics::{ShardMetrics, frame_drop_reason, frame_drop_variant};

    #[test]
    fn shard_metrics_land_in_the_exposition_under_a_shard_label() {
        let shard_metrics = ShardMetrics::for_shard();
        shard_metrics.record_frame_drop(frame_drop_variant::CONSENSUS, frame_drop_reason::FULL);
        let metrics = HttpMetrics::init(&[shard_metrics]);
        let output = metrics.formatted_output();
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
        let output = metrics.formatted_output();
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
        let output = metrics.formatted_output();
        assert!(
            output.contains("# TYPE stats_rollup_underflows counter\n"),
            "expected the collector's series in the exposition:\n{output}"
        );
        assert!(
            output.contains("\nstats_rollup_underflows_total "),
            "expected the collector's sample in the exposition:\n{output}"
        );
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
        let output = metrics.formatted_output();
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
        let output = metrics.formatted_output();
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
        let output = metrics.formatted_output();
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
        let first = metrics.formatted_output();
        assert!(
            first.contains(r#"topic="eu""#),
            "the snapshot must be encoded before it is released:\n{first}"
        );
        let second = metrics.formatted_output();
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
        let output = metrics.formatted_output();
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
