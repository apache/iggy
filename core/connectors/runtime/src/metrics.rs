/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use prometheus_client::encoding::text::encode;
use prometheus_client::encoding::{EncodeLabelSet, EncodeLabelValue};
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;
use std::sync::Arc;
use std::time::Duration;
use tracing::error;

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct ConnectorLabels {
    pub connector_key: String,
    pub connector_type: ConnectorType,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum ConnectorType {
    Source,
    Sink,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
pub struct StageLabels {
    pub connector_key: String,
    pub connector_type: ConnectorType,
    pub stage: Stage,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelValue)]
pub enum Stage {
    Prepare,
    Ffi,
    Decode,
    IggySend,
    StateSave,
    Total,
}

const STAGE_BUCKETS_SECONDS: [f64; 13] = [
    50e-6, 100e-6, 250e-6, 500e-6, 1e-3, 5e-3, 10e-3, 50e-3, 100e-3, 250e-3, 500e-3, 1.0, 5.0,
];

#[derive(Debug, Clone)]
pub struct SinkLabels {
    pub counter: ConnectorLabels,
    pub stage_prepare: StageLabels,
    pub stage_ffi: StageLabels,
    pub stage_total: StageLabels,
}

impl SinkLabels {
    pub fn new(plugin_key: &str) -> Self {
        let key = plugin_key.to_owned();
        Self {
            counter: ConnectorLabels {
                connector_key: key.clone(),
                connector_type: ConnectorType::Sink,
            },
            stage_prepare: StageLabels {
                connector_key: key.clone(),
                connector_type: ConnectorType::Sink,
                stage: Stage::Prepare,
            },
            stage_ffi: StageLabels {
                connector_key: key.clone(),
                connector_type: ConnectorType::Sink,
                stage: Stage::Ffi,
            },
            stage_total: StageLabels {
                connector_key: key,
                connector_type: ConnectorType::Sink,
                stage: Stage::Total,
            },
        }
    }
}

#[derive(Debug, Clone)]
pub struct SourceLabels {
    pub counter: ConnectorLabels,
    pub stage_decode: StageLabels,
    pub stage_prepare: StageLabels,
    pub stage_iggy_send: StageLabels,
    pub stage_state_save: StageLabels,
    pub stage_total: StageLabels,
}

impl SourceLabels {
    pub fn new(plugin_key: &str) -> Self {
        let key = plugin_key.to_owned();
        let stage = |s: Stage| StageLabels {
            connector_key: key.clone(),
            connector_type: ConnectorType::Source,
            stage: s,
        };
        Self {
            counter: ConnectorLabels {
                connector_key: key.clone(),
                connector_type: ConnectorType::Source,
            },
            stage_decode: stage(Stage::Decode),
            stage_prepare: stage(Stage::Prepare),
            stage_iggy_send: stage(Stage::IggySend),
            stage_state_save: stage(Stage::StateSave),
            stage_total: stage(Stage::Total),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Metrics {
    registry: Arc<Registry>,
    sources_total: Gauge,
    sources_running: Gauge,
    sinks_total: Gauge,
    sinks_running: Gauge,
    messages_produced: Family<ConnectorLabels, Counter>,
    messages_sent: Family<ConnectorLabels, Counter>,
    messages_consumed: Family<ConnectorLabels, Counter>,
    messages_processed: Family<ConnectorLabels, Counter>,
    messages_filtered: Family<ConnectorLabels, Counter>,
    errors: Family<ConnectorLabels, Counter>,
    stage_duration_seconds: Family<StageLabels, Histogram, fn() -> Histogram>,
}

impl Metrics {
    pub fn init() -> Self {
        let mut registry = Registry::default();

        let sources_total = Gauge::default();
        let sources_running = Gauge::default();
        let sinks_total = Gauge::default();
        let sinks_running = Gauge::default();
        let messages_produced = Family::<ConnectorLabels, Counter>::default();
        let messages_sent = Family::<ConnectorLabels, Counter>::default();
        let messages_consumed = Family::<ConnectorLabels, Counter>::default();
        let messages_processed = Family::<ConnectorLabels, Counter>::default();
        let messages_filtered = Family::<ConnectorLabels, Counter>::default();
        let errors = Family::<ConnectorLabels, Counter>::default();
        let stage_duration_seconds: Family<StageLabels, Histogram, fn() -> Histogram> =
            Family::new_with_constructor(stage_histogram);

        registry.register(
            "iggy_connectors_sources_total",
            "Total configured source connectors",
            sources_total.clone(),
        );
        registry.register(
            "iggy_connectors_sources_running",
            "Sources in Running status",
            sources_running.clone(),
        );
        registry.register(
            "iggy_connectors_sinks_total",
            "Total configured sink connectors",
            sinks_total.clone(),
        );
        registry.register(
            "iggy_connectors_sinks_running",
            "Sinks in Running status",
            sinks_running.clone(),
        );
        registry.register(
            "iggy_connector_messages_produced_total",
            "Messages received from source plugin poll",
            messages_produced.clone(),
        );
        registry.register(
            "iggy_connector_messages_sent_total",
            "Messages sent to Iggy (source)",
            messages_sent.clone(),
        );
        registry.register(
            "iggy_connector_messages_consumed_total",
            "Messages consumed from Iggy (sink)",
            messages_consumed.clone(),
        );
        registry.register(
            "iggy_connector_messages_processed_total",
            "Messages processed and sent to sink plugin",
            messages_processed.clone(),
        );
        registry.register(
            "iggy_connector_messages_filtered_total",
            "Messages intentionally dropped by transforms returning Ok(None)",
            messages_filtered.clone(),
        );
        registry.register(
            "iggy_connector_errors_total",
            "Errors encountered",
            errors.clone(),
        );
        registry.register(
            "iggy_connector_stage_duration_seconds",
            "Per-batch processing stage duration in seconds",
            stage_duration_seconds.clone(),
        );

        Self {
            registry: Arc::new(registry),
            sources_total,
            sources_running,
            sinks_total,
            sinks_running,
            messages_produced,
            messages_sent,
            messages_consumed,
            messages_processed,
            messages_filtered,
            errors,
            stage_duration_seconds,
        }
    }

    pub fn get_formatted_output(&self) -> String {
        let mut buffer = String::new();
        if let Err(err) = encode(&mut buffer, &self.registry) {
            error!("Failed to encode metrics: {}", err);
        }
        buffer
    }

    pub fn set_sources_total(&self, count: u32) {
        self.sources_total.set(count as i64);
    }

    pub fn set_sinks_total(&self, count: u32) {
        self.sinks_total.set(count as i64);
    }

    pub fn get_sources_total(&self) -> u32 {
        self.sources_total.get() as u32
    }

    pub fn get_sinks_total(&self) -> u32 {
        self.sinks_total.get() as u32
    }

    pub fn get_sources_running(&self) -> u32 {
        self.sources_running.get() as u32
    }

    pub fn get_sinks_running(&self) -> u32 {
        self.sinks_running.get() as u32
    }

    pub fn increment_sources_running(&self) {
        self.sources_running.inc();
    }

    pub fn decrement_sources_running(&self) {
        self.sources_running.dec();
    }

    pub fn increment_sinks_running(&self) {
        self.sinks_running.inc();
    }

    pub fn decrement_sinks_running(&self) {
        self.sinks_running.dec();
    }

    pub fn observe_stage_with_labels(&self, labels: &StageLabels, duration: Duration) {
        self.stage_duration_seconds
            .get_or_create(labels)
            .observe(duration.as_secs_f64());
    }

    #[cfg(test)]
    pub fn increment_messages_produced(&self, key: &str, count: u64) {
        self.inc_messages_produced_with_labels(
            &ConnectorLabels {
                connector_key: key.to_owned(),
                connector_type: ConnectorType::Source,
            },
            count,
        );
    }

    #[cfg(test)]
    pub fn increment_messages_sent(&self, key: &str, count: u64) {
        self.inc_messages_sent_with_labels(
            &ConnectorLabels {
                connector_key: key.to_owned(),
                connector_type: ConnectorType::Source,
            },
            count,
        );
    }

    #[cfg(test)]
    pub fn increment_messages_consumed(&self, key: &str, count: u64) {
        self.inc_messages_consumed_with_labels(
            &ConnectorLabels {
                connector_key: key.to_owned(),
                connector_type: ConnectorType::Sink,
            },
            count,
        );
    }

    #[cfg(test)]
    pub fn increment_messages_processed(&self, key: &str, count: u64) {
        self.inc_messages_processed_with_labels(
            &ConnectorLabels {
                connector_key: key.to_owned(),
                connector_type: ConnectorType::Sink,
            },
            count,
        );
    }

    #[cfg(test)]
    pub fn increment_messages_filtered(
        &self,
        key: &str,
        connector_type: ConnectorType,
        count: u64,
    ) {
        self.inc_messages_filtered_with_labels(
            &ConnectorLabels {
                connector_key: key.to_owned(),
                connector_type,
            },
            count,
        );
    }

    #[cfg(test)]
    pub fn increment_errors(&self, key: &str, connector_type: ConnectorType) {
        self.inc_errors_with_labels(&ConnectorLabels {
            connector_key: key.to_owned(),
            connector_type,
        });
    }

    #[cfg(test)]
    pub fn observe_stage_duration(
        &self,
        key: &str,
        connector_type: ConnectorType,
        stage: Stage,
        duration: Duration,
    ) {
        self.observe_stage_with_labels(
            &StageLabels {
                connector_key: key.to_owned(),
                connector_type,
                stage,
            },
            duration,
        );
    }

    pub fn inc_messages_produced_with_labels(&self, labels: &ConnectorLabels, count: u64) {
        self.messages_produced.get_or_create(labels).inc_by(count);
    }

    pub fn inc_messages_sent_with_labels(&self, labels: &ConnectorLabels, count: u64) {
        self.messages_sent.get_or_create(labels).inc_by(count);
    }

    pub fn inc_messages_consumed_with_labels(&self, labels: &ConnectorLabels, count: u64) {
        self.messages_consumed.get_or_create(labels).inc_by(count);
    }

    pub fn inc_messages_processed_with_labels(&self, labels: &ConnectorLabels, count: u64) {
        self.messages_processed.get_or_create(labels).inc_by(count);
    }

    pub fn inc_messages_filtered_with_labels(&self, labels: &ConnectorLabels, count: u64) {
        self.messages_filtered.get_or_create(labels).inc_by(count);
    }

    pub fn inc_errors_with_labels(&self, labels: &ConnectorLabels) {
        self.errors.get_or_create(labels).inc();
    }

    pub fn get_messages_produced(&self, key: &str) -> u64 {
        self.messages_produced
            .get_or_create(&ConnectorLabels {
                connector_key: key.to_owned(),
                connector_type: ConnectorType::Source,
            })
            .get()
    }

    pub fn get_messages_sent(&self, key: &str) -> u64 {
        self.messages_sent
            .get_or_create(&ConnectorLabels {
                connector_key: key.to_owned(),
                connector_type: ConnectorType::Source,
            })
            .get()
    }

    pub fn get_messages_consumed(&self, key: &str) -> u64 {
        self.messages_consumed
            .get_or_create(&ConnectorLabels {
                connector_key: key.to_owned(),
                connector_type: ConnectorType::Sink,
            })
            .get()
    }

    pub fn get_messages_processed(&self, key: &str) -> u64 {
        self.messages_processed
            .get_or_create(&ConnectorLabels {
                connector_key: key.to_owned(),
                connector_type: ConnectorType::Sink,
            })
            .get()
    }

    pub fn get_errors(&self, key: &str, connector_type: ConnectorType) -> u64 {
        self.errors
            .get_or_create(&ConnectorLabels {
                connector_key: key.to_owned(),
                connector_type,
            })
            .get()
    }

    pub fn get_messages_filtered(&self, key: &str, connector_type: ConnectorType) -> u64 {
        self.messages_filtered
            .get_or_create(&ConnectorLabels {
                connector_key: key.to_owned(),
                connector_type,
            })
            .get()
    }

    #[cfg(test)]
    pub(crate) fn get_stage_duration_sample_count(
        &self,
        key: &str,
        connector_type: ConnectorType,
        stage: Stage,
    ) -> u64 {
        let output = self.get_formatted_output();
        let stage_label = match stage {
            Stage::Prepare => "Prepare",
            Stage::Ffi => "Ffi",
            Stage::Decode => "Decode",
            Stage::IggySend => "IggySend",
            Stage::StateSave => "StateSave",
            Stage::Total => "Total",
        };
        let type_label = match connector_type {
            ConnectorType::Source => "Source",
            ConnectorType::Sink => "Sink",
        };
        let needle = format!(
            "iggy_connector_stage_duration_seconds_count{{connector_key=\"{key}\",connector_type=\"{type_label}\",stage=\"{stage_label}\"}}"
        );
        output
            .lines()
            .find_map(|line| line.strip_prefix(&needle))
            .and_then(|rest| rest.trim().parse::<u64>().ok())
            .unwrap_or(0)
    }
}

fn stage_histogram() -> Histogram {
    Histogram::new(STAGE_BUCKETS_SECONDS.iter().copied())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_init() {
        let metrics = Metrics::init();
        let output = metrics.get_formatted_output();

        assert!(output.contains("iggy_connectors_sources_total"));
        assert!(output.contains("iggy_connectors_sources_running"));
        assert!(output.contains("iggy_connectors_sinks_total"));
        assert!(output.contains("iggy_connectors_sinks_running"));
    }

    #[test]
    fn test_set_sources_total() {
        let metrics = Metrics::init();
        metrics.set_sources_total(5);

        let output = metrics.get_formatted_output();
        assert!(output.contains("iggy_connectors_sources_total 5"));
    }

    #[test]
    fn test_set_sinks_total() {
        let metrics = Metrics::init();
        metrics.set_sinks_total(3);

        let output = metrics.get_formatted_output();
        assert!(output.contains("iggy_connectors_sinks_total 3"));
    }

    #[test]
    fn test_increment_sources_running() {
        let metrics = Metrics::init();
        metrics.increment_sources_running();
        metrics.increment_sources_running();

        let output = metrics.get_formatted_output();
        assert!(output.contains("iggy_connectors_sources_running 2"));
    }

    #[test]
    fn test_decrement_sources_running() {
        let metrics = Metrics::init();
        metrics.increment_sources_running();
        metrics.increment_sources_running();
        metrics.decrement_sources_running();

        let output = metrics.get_formatted_output();
        assert!(output.contains("iggy_connectors_sources_running 1"));
    }

    #[test]
    fn test_increment_sinks_running() {
        let metrics = Metrics::init();
        metrics.increment_sinks_running();

        let output = metrics.get_formatted_output();
        assert!(output.contains("iggy_connectors_sinks_running 1"));
    }

    #[test]
    fn test_decrement_sinks_running() {
        let metrics = Metrics::init();
        metrics.increment_sinks_running();
        metrics.increment_sinks_running();
        metrics.decrement_sinks_running();

        let output = metrics.get_formatted_output();
        assert!(output.contains("iggy_connectors_sinks_running 1"));
    }

    #[test]
    fn test_increment_messages_produced() {
        let metrics = Metrics::init();
        metrics.increment_messages_produced("test-source", 10);

        let output = metrics.get_formatted_output();
        assert!(output.contains("iggy_connector_messages_produced_total"));
        assert!(output.contains("connector_key=\"test-source\""));
        assert!(output.contains("connector_type=\"Source\""));
    }

    #[test]
    fn test_increment_messages_sent() {
        let metrics = Metrics::init();
        metrics.increment_messages_sent("test-source", 5);

        let output = metrics.get_formatted_output();
        assert!(output.contains("iggy_connector_messages_sent_total"));
        assert!(output.contains("connector_key=\"test-source\""));
    }

    #[test]
    fn test_increment_messages_consumed() {
        let metrics = Metrics::init();
        metrics.increment_messages_consumed("test-sink", 20);

        let output = metrics.get_formatted_output();
        assert!(output.contains("iggy_connector_messages_consumed_total"));
        assert!(output.contains("connector_key=\"test-sink\""));
        assert!(output.contains("connector_type=\"Sink\""));
    }

    #[test]
    fn test_increment_messages_processed() {
        let metrics = Metrics::init();
        metrics.increment_messages_processed("test-sink", 15);

        let output = metrics.get_formatted_output();
        assert!(output.contains("iggy_connector_messages_processed_total"));
        assert!(output.contains("connector_key=\"test-sink\""));
    }

    #[test]
    fn test_increment_errors_source() {
        let metrics = Metrics::init();
        metrics.increment_errors("test-source", ConnectorType::Source);

        let output = metrics.get_formatted_output();
        assert!(output.contains("iggy_connector_errors_total"));
        assert!(output.contains("connector_key=\"test-source\""));
        assert!(output.contains("connector_type=\"Source\""));
    }

    #[test]
    fn test_increment_errors_sink() {
        let metrics = Metrics::init();
        metrics.increment_errors("test-sink", ConnectorType::Sink);

        let output = metrics.get_formatted_output();
        assert!(output.contains("iggy_connector_errors_total"));
        assert!(output.contains("connector_key=\"test-sink\""));
        assert!(output.contains("connector_type=\"Sink\""));
    }

    #[test]
    fn test_multiple_connectors() {
        let metrics = Metrics::init();
        metrics.increment_messages_produced("source-1", 10);
        metrics.increment_messages_produced("source-2", 20);
        metrics.increment_messages_consumed("sink-1", 15);

        let output = metrics.get_formatted_output();
        assert!(output.contains("connector_key=\"source-1\""));
        assert!(output.contains("connector_key=\"source-2\""));
        assert!(output.contains("connector_key=\"sink-1\""));
    }

    #[test]
    fn test_cumulative_counts() {
        let metrics = Metrics::init();
        metrics.increment_messages_produced("test-source", 10);
        metrics.increment_messages_produced("test-source", 5);

        let output = metrics.get_formatted_output();
        // Verify cumulative behavior - the same key should accumulate
        assert!(output.contains("iggy_connector_messages_produced_total"));
        assert!(output.contains("connector_key=\"test-source\""));
        // Check the value is 15 (10 + 5)
        assert!(output.contains("} 15"));
    }

    #[test]
    fn test_get_messages_produced() {
        let metrics = Metrics::init();
        metrics.increment_messages_produced("test-source", 10);
        metrics.increment_messages_produced("test-source", 5);

        assert_eq!(metrics.get_messages_produced("test-source"), 15);
        assert_eq!(metrics.get_messages_produced("nonexistent"), 0);
    }

    #[test]
    fn test_get_messages_sent() {
        let metrics = Metrics::init();
        metrics.increment_messages_sent("test-source", 20);

        assert_eq!(metrics.get_messages_sent("test-source"), 20);
    }

    #[test]
    fn test_get_messages_consumed() {
        let metrics = Metrics::init();
        metrics.increment_messages_consumed("test-sink", 30);

        assert_eq!(metrics.get_messages_consumed("test-sink"), 30);
    }

    #[test]
    fn test_get_messages_processed() {
        let metrics = Metrics::init();
        metrics.increment_messages_processed("test-sink", 25);

        assert_eq!(metrics.get_messages_processed("test-sink"), 25);
    }

    #[test]
    fn test_get_errors() {
        let metrics = Metrics::init();
        metrics.increment_errors("test-source", ConnectorType::Source);
        metrics.increment_errors("test-source", ConnectorType::Source);
        metrics.increment_errors("test-sink", ConnectorType::Sink);

        assert_eq!(metrics.get_errors("test-source", ConnectorType::Source), 2);
        assert_eq!(metrics.get_errors("test-sink", ConnectorType::Sink), 1);
        assert_eq!(metrics.get_errors("nonexistent", ConnectorType::Source), 0);
    }

    #[test]
    fn given_filtered_counter_when_incremented_should_register_under_labels() {
        let metrics = Metrics::init();
        metrics.increment_messages_filtered("filter-source", ConnectorType::Source, 7);
        metrics.increment_messages_filtered("filter-sink", ConnectorType::Sink, 3);
        metrics.increment_messages_filtered("filter-source", ConnectorType::Source, 5);

        assert_eq!(
            metrics.get_messages_filtered("filter-source", ConnectorType::Source),
            12
        );
        assert_eq!(
            metrics.get_messages_filtered("filter-sink", ConnectorType::Sink),
            3
        );
        assert_eq!(
            metrics.get_messages_filtered("nonexistent", ConnectorType::Source),
            0
        );

        let output = metrics.get_formatted_output();
        assert!(output.contains("iggy_connector_messages_filtered_total"));
        assert!(output.contains("connector_key=\"filter-source\""));
        assert!(output.contains("connector_key=\"filter-sink\""));
    }

    #[test]
    fn given_filtered_counter_unique_per_connector_type_when_same_key_should_be_separate() {
        let metrics = Metrics::init();
        metrics.increment_messages_filtered("shared", ConnectorType::Source, 2);
        metrics.increment_messages_filtered("shared", ConnectorType::Sink, 4);

        assert_eq!(
            metrics.get_messages_filtered("shared", ConnectorType::Source),
            2
        );
        assert_eq!(
            metrics.get_messages_filtered("shared", ConnectorType::Sink),
            4
        );
    }

    #[test]
    fn given_stage_histogram_when_observations_recorded_should_increment_sample_count() {
        let metrics = Metrics::init();
        metrics.observe_stage_duration(
            "pg",
            ConnectorType::Sink,
            Stage::Prepare,
            Duration::from_micros(120),
        );
        metrics.observe_stage_duration(
            "pg",
            ConnectorType::Sink,
            Stage::Prepare,
            Duration::from_micros(340),
        );
        metrics.observe_stage_duration(
            "pg",
            ConnectorType::Sink,
            Stage::Ffi,
            Duration::from_millis(2),
        );

        assert_eq!(
            metrics.get_stage_duration_sample_count("pg", ConnectorType::Sink, Stage::Prepare),
            2
        );
        assert_eq!(
            metrics.get_stage_duration_sample_count("pg", ConnectorType::Sink, Stage::Ffi),
            1
        );
        assert_eq!(
            metrics.get_stage_duration_sample_count("pg", ConnectorType::Sink, Stage::Total),
            0
        );
    }

    #[test]
    fn given_stage_histogram_when_encoded_should_expose_bucket_metrics() {
        let metrics = Metrics::init();
        metrics.observe_stage_duration(
            "es",
            ConnectorType::Source,
            Stage::IggySend,
            Duration::from_micros(750),
        );
        let output = metrics.get_formatted_output();
        assert!(output.contains("iggy_connector_stage_duration_seconds"));
        assert!(output.contains("connector_key=\"es\""));
        assert!(output.contains("stage=\"IggySend\""));
        assert!(output.contains("iggy_connector_stage_duration_seconds_bucket"));
        assert!(output.contains("iggy_connector_stage_duration_seconds_count"));
        assert!(output.contains("iggy_connector_stage_duration_seconds_sum"));
    }

    #[test]
    fn given_stage_histogram_when_same_label_set_used_twice_should_share_bucket() {
        let metrics = Metrics::init();
        for _ in 0..5 {
            metrics.observe_stage_duration(
                "k",
                ConnectorType::Sink,
                Stage::Total,
                Duration::from_micros(80),
            );
        }
        assert_eq!(
            metrics.get_stage_duration_sample_count("k", ConnectorType::Sink, Stage::Total),
            5
        );
    }

    #[test]
    fn given_source_stages_when_observed_should_each_have_distinct_sample_count() {
        let metrics = Metrics::init();
        let cases: [(Stage, u64); 5] = [
            (Stage::Decode, 1),
            (Stage::Prepare, 2),
            (Stage::IggySend, 3),
            (Stage::StateSave, 4),
            (Stage::Total, 5),
        ];
        for (stage, count) in &cases {
            for _ in 0..*count {
                metrics.observe_stage_duration(
                    "src",
                    ConnectorType::Source,
                    stage.clone(),
                    Duration::from_micros(100),
                );
            }
        }
        for (stage, expected) in &cases {
            assert_eq!(
                metrics.get_stage_duration_sample_count(
                    "src",
                    ConnectorType::Source,
                    stage.clone(),
                ),
                *expected
            );
        }
    }
}
