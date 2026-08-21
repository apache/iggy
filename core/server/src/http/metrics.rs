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
//! (entity gauges plus the request counter), its public scrape handler, and
//! the config gate deciding whether the route is mounted.

use axum::extract::State;
use configs::http::HttpMetricsConfig;
use consensus::MetadataHandle;
use iggy_common::IggyError;
use metadata::impls::metadata::StreamsFrontend;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::registry::Registry;
use send_wrapper::SendWrapper;
use tracing::error;

use crate::http::extractor::Identity;
use crate::http::state::HttpState;

/// The legacy server's metric set, registered under the same names and help
/// texts so existing dashboards and alerts keep working unchanged.
///
/// Unlike the legacy server, the entity gauges are not counted at mutation
/// sites: [`get_metrics`] samples the live state on every scrape, so a gauge
/// can never drift from the state it describes.
pub(in crate::http) struct HttpMetrics {
    registry: Registry,
    http_requests: Counter,
    streams: Gauge,
    topics: Gauge,
    partitions: Gauge,
    segments: Gauge,
    messages: Gauge,
    users: Gauge,
    clients: Gauge,
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
        }
    }

    /// Handle for the router's request-counting layer. The counter is
    /// `Arc`-backed, so bumping the clone bumps the registered metric.
    pub(in crate::http) fn request_counter(&self) -> Counter {
        self.http_requests.clone()
    }

    fn formatted_output(&self) -> String {
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

/// `GET <http.metrics.endpoint>`: the metric set in prometheus text
/// exposition. Auth-only, like `/stats`: the `Identity` extractor rejects a
/// missing or invalid bearer with 401, and any authenticated user may scrape
/// (no RBAC rule guards it). Scrapers present a JWT or a raw PAT the same way
/// every read route accepts them.
///
/// The entity gauges sample the same reads `/stats` serves: the metadata STM
/// stream and user maps plus the stats-registry rollups, whose partition-plane
/// increments are relaxed, so scraped values are approximate while writes are
/// in flight. The clients count scatter-gathers the per-shard session managers
/// exactly like `GET /clients` and turns partial when a shard misses the reply
/// deadline.
pub(in crate::http) async fn get_metrics(
    State(state): State<HttpState>,
    _identity: Identity,
) -> String {
    let (streams_count, topics_count, partitions_count, segments_count, messages_count) = state
        .shard
        .plane
        .metadata()
        .mux_stm
        .streams()
        .read(|streams| {
            let mut topics_count = 0u64;
            let mut partitions_count = 0u64;
            let mut segments_count = 0u64;
            let mut messages_count = 0u64;
            for (_, stream) in &streams.items {
                topics_count = topics_count.saturating_add(stream.topics.len() as u64);
                segments_count = segments_count
                    .saturating_add(u64::from(stream.stats.segments_count_inconsistent()));
                messages_count =
                    messages_count.saturating_add(stream.stats.messages_count_inconsistent());
                for (_, topic) in &stream.topics {
                    partitions_count =
                        partitions_count.saturating_add(topic.partitions.len() as u64);
                }
            }
            (
                streams.items.len() as u64,
                topics_count,
                partitions_count,
                segments_count,
                messages_count,
            )
        });
    let users_count = state
        .shard
        .plane
        .metadata()
        .mux_stm
        .users()
        .read(|users| users.items.len() as u64);
    let clients_count = SendWrapper::new(state.shard.list_all_clients()).await.len() as u64;

    let metrics = &state.metrics;
    metrics.streams.set(gauge_value(streams_count));
    metrics.topics.set(gauge_value(topics_count));
    metrics.partitions.set(gauge_value(partitions_count));
    metrics.segments.set(gauge_value(segments_count));
    metrics.messages.set(gauge_value(messages_count));
    metrics.users.set(gauge_value(users_count));
    metrics.clients.set(gauge_value(clients_count));
    metrics.formatted_output()
}

/// Clamp a count into the gauge's `i64` domain; only `messages` can pass
/// `i64::MAX` even in theory, the rest are bounded far below it.
fn gauge_value(count: u64) -> i64 {
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
