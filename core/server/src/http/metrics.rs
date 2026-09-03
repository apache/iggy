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

use configs::http::HttpMetricsConfig;
use iggy_common::IggyError;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use tracing::error;

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
    /// Not a legacy-parity metric: the running count of rollup decrements that
    /// had to be clamped at zero. Non-zero means a topic or stream total was
    /// asked to give back more than it held, so the aggregate it now reports is
    /// low. Alert on any increase.
    ///
    /// A `Counter`, not a `Gauge`: it only ever climbs within a process, so
    /// `rate()` and `increase()` are the queries an operator wants, and both are
    /// counter-only. The scrape handler samples the source and advances this by
    /// the difference, which is why the source is monotonic per process.
    stats_rollup_underflows: Counter,
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
        let stats_rollup_underflows = Counter::default();
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
        registry.register(
            "stats_rollup_underflows",
            "total count of aggregate stats decrements clamped at zero",
            stats_rollup_underflows.clone(),
        );
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
            stats_rollup_underflows,
        }
    }

    /// Handle for the router's request-counting layer. The counter is
    /// `Arc`-backed, so bumping the clone bumps the registered metric.
    pub(in crate::http) fn request_counter(&self) -> Counter {
        self.http_requests.clone()
    }

    /// Advance the rollup-underflow counter to `total`, the process-wide count
    /// sampled at scrape time. A counter cannot be set, so this adds the
    /// difference; `total` only climbs within a process, and a restart resets
    /// both sides together, which is the counter reset Prometheus expects.
    pub(in crate::http) fn observe_rollup_underflows(&self, total: u64) {
        let recorded = self.stats_rollup_underflows.get();
        if let Some(delta) = total.checked_sub(recorded) {
            self.stats_rollup_underflows.inc_by(delta);
        }
    }

    pub(in crate::http) fn formatted_output(&self) -> String {
        let mut buffer = String::new();
        if let Err(error) = encode(&mut buffer, &self.registry) {
            error!(%error, "failed to encode metrics");
        }
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

    #[test]
    fn rollup_underflow_total_tracks_the_sampled_count() {
        let metrics = HttpMetrics::init(&[]);
        metrics.observe_rollup_underflows(3);
        metrics.observe_rollup_underflows(5);
        // A restart resets the source; the counter must not run backwards.
        metrics.observe_rollup_underflows(0);
        let output = metrics.formatted_output();
        assert!(
            output.contains("\nstats_rollup_underflows_total 5\n"),
            "expected the sampled total in the exposition:\n{output}"
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

    #[test]
    fn validated_endpoint_rejects_missing_leading_slash() {
        assert!(matches!(
            validated_endpoint(&metrics_config(true, "metrics")),
            Err(IggyError::InvalidConfiguration)
        ));
    }
}
