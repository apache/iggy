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

use async_trait::async_trait;
use iggy::prelude::IggyClient;
use integration::harness::seeds::SeedError;
use integration::harness::{TestBinaryError, TestFixture, TestHarness, seeds};
use integration::iggy_harness;
use std::collections::HashMap;
use std::time::{Duration, Instant};
use tokio::time::sleep;

// Layering: tests in this file rely on TWO independent observability surfaces
// to keep coverage durable against tracing's text-format evolution.
//
//   1. Text-event assertions parse the runtime's stdout/stderr via `parse_event_fields`.
//      Format is tracing-subscriber's default `fmt::layer()` style; parsing is quote-,
//      ANSI-, and embedded-space-aware and covered by `parser_tests` against synthetic
//      inputs so the parser itself is locked.
//   2. Histogram assertions scrape the runtime's Prometheus `/metrics` endpoint and
//      verify `iggy_connector_stage_duration_seconds_count` per (key, type, stage).
//      That path is unaffected by any future tracing formatter change.
//
// If tracing's default format ever changes incompatibly, text-event tests will fail
// before any production regression hits ops - and the histogram tests will still pass,
// pinpointing the issue as test-only formatter coupling rather than a runtime bug.
const BENCHMARK_TARGET: &str = "iggy_connectors::benchmark";
const POLL_INTERVAL: Duration = Duration::from_millis(100);
const POLL_DEADLINE: Duration = Duration::from_secs(10);
const ZERO_EVENT_OBSERVATION_WINDOW: Duration = Duration::from_secs(2);

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/runtime/benchmark.toml")),
    seed = seeds::connector_stream
)]
async fn given_benchmark_enabled_when_batches_flow_should_emit_typed_events(harness: &TestHarness) {
    let (sink_events, source_events) =
        wait_for_both_kinds(harness, "stdout_bench", "random_bench").await;

    let runtime_logs = collect_runtime_logs(harness);
    assert!(
        runtime_logs.contains("Benchmark mode enabled for sink connector")
            && runtime_logs.contains("stdout_bench"),
        "expected sink benchmark startup log in runtime output"
    );
    assert!(
        runtime_logs.contains("Benchmark mode enabled for source connector")
            && runtime_logs.contains("random_bench"),
        "expected source benchmark startup log in runtime output"
    );

    for event in &sink_events {
        assert_eq!(event.get("connector_type"), Some(&"sink".to_string()));
        assert_eq!(
            event.get("connector_key"),
            Some(&"stdout_bench".to_string())
        );
        assert_eq!(event.get("stream"), Some(&"test_stream".to_string()));
        assert_eq!(event.get("topic"), Some(&"test_topic".to_string()));

        let batch_size = parse_u64(event, "batch_size");
        let processed_count = parse_u64(event, "processed_count");
        let prepare_us = parse_u64(event, "prepare_us");
        let ffi_us = parse_u64(event, "ffi_us");
        let total_us = parse_u64(event, "total_us");

        assert!(batch_size > 0, "sink batch_size must be > 0: {event:?}");
        assert!(
            processed_count <= batch_size,
            "sink processed_count must be <= batch_size: {event:?}"
        );
        assert!(total_us > 0, "sink total_us must be > 0: {event:?}");
        assert!(
            prepare_us + ffi_us <= total_us + 100,
            "sink prepare_us + ffi_us must be <= total_us (with slack): {event:?}"
        );
    }

    for event in &source_events {
        assert_eq!(event.get("connector_type"), Some(&"source".to_string()));
        assert_eq!(
            event.get("connector_key"),
            Some(&"random_bench".to_string())
        );
        assert_eq!(event.get("stream"), Some(&"test_stream".to_string()));
        assert_eq!(event.get("topic"), Some(&"test_topic".to_string()));

        let batch_size = parse_u64(event, "batch_size");
        let sent_count = parse_u64(event, "sent_count");
        let total_us = parse_u64(event, "total_us");
        let decode_us = parse_u64(event, "decode_us");
        let prepare_us = parse_u64(event, "prepare_us");
        let iggy_send_us = parse_u64(event, "iggy_send_us");
        let state_save_us = parse_opt_u64(event, "state_save_us");

        assert!(batch_size > 0, "source batch_size must be > 0: {event:?}");
        assert!(
            sent_count <= batch_size,
            "source sent_count must be <= batch_size (no over-counting on transform/encode drops): {event:?}"
        );
        assert!(total_us > 0, "source total_us must be > 0: {event:?}");
        let stage_sum = decode_us + prepare_us + iggy_send_us + state_save_us.unwrap_or(0);
        assert!(
            stage_sum <= total_us + 100,
            "source stage sum (decode+prepare+iggy_send+state_save) must be <= total_us (with slack): {event:?}"
        );
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/runtime/benchmark.toml")),
    seed = seeds::connector_stream
)]
async fn given_benchmark_enabled_when_events_emitted_should_respect_runtime_batch_configuration(
    harness: &TestHarness,
) {
    let (sink_events, source_events) =
        wait_for_both_kinds(harness, "stdout_bench", "random_bench").await;

    let sink_batch_max = sink_events
        .iter()
        .map(|event| parse_u64(event, "batch_size"))
        .max()
        .unwrap_or(0);
    assert!(
        sink_batch_max <= 50,
        "sink batch_size should not exceed configured batch_length=50, saw {sink_batch_max}"
    );

    let source_batch_max = source_events
        .iter()
        .map(|event| parse_u64(event, "batch_size"))
        .max()
        .unwrap_or(0);
    assert!(
        (3..=5).contains(&source_batch_max),
        "source batch_size should fall within configured messages_range=[3,5], saw {source_batch_max}"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/runtime/benchmark_disabled.toml")),
    seed = seeds::connector_stream
)]
async fn given_benchmark_disabled_when_batches_processed_should_emit_no_events(
    harness: &TestHarness,
) {
    sleep(ZERO_EVENT_OBSERVATION_WINDOW).await;

    let runtime_logs = collect_runtime_logs(harness);
    assert!(
        !runtime_logs.is_empty(),
        "runtime should have produced non-benchmark logs even with benchmark disabled"
    );

    let sink_events = parse_benchmark_events(&runtime_logs, "sink");
    let source_events = parse_benchmark_events(&runtime_logs, "source");
    assert!(
        sink_events.is_empty(),
        "expected zero sink benchmark events when benchmark=false, got {} events",
        sink_events.len()
    );
    assert!(
        source_events.is_empty(),
        "expected zero source benchmark events when benchmark=false, got {} events",
        source_events.len()
    );
    assert!(
        !runtime_logs.contains("Benchmark mode enabled for"),
        "expected no benchmark startup log when benchmark=false"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/runtime/benchmark_disabled.toml")),
    seed = seeds::connector_stream
)]
async fn given_benchmark_env_var_override_when_toml_says_false_should_emit_events(
    harness: &TestHarness,
    _fixture: BenchmarkEnvOverrideFixture,
) {
    let (sink_events, source_events) =
        wait_for_both_kinds(harness, "stdout_off", "random_off").await;

    for event in &sink_events {
        assert_eq!(event.get("connector_key"), Some(&"stdout_off".to_string()));
    }
    for event in &source_events {
        assert_eq!(event.get("connector_key"), Some(&"random_off".to_string()));
    }
}

async fn wait_for_both_kinds(
    harness: &TestHarness,
    sink_key: &str,
    source_key: &str,
) -> (Vec<HashMap<String, String>>, Vec<HashMap<String, String>>) {
    let deadline = Instant::now() + POLL_DEADLINE;
    loop {
        let logs = collect_runtime_logs(harness);
        let sink_events = parse_benchmark_events_for_key(&logs, "sink", sink_key);
        let source_events = parse_benchmark_events_for_key(&logs, "source", source_key);
        if !sink_events.is_empty() && !source_events.is_empty() {
            return (sink_events, source_events);
        }
        if Instant::now() >= deadline {
            panic!(
                "timed out waiting for sink ({}) + source ({}) benchmark events. \
                 sink seen: {}, source seen: {}. logs:\n{logs}",
                sink_key,
                source_key,
                sink_events.len(),
                source_events.len(),
            );
        }
        sleep(POLL_INTERVAL).await;
    }
}

fn collect_runtime_logs(harness: &TestHarness) -> String {
    let runtime = harness
        .connectors_runtime()
        .expect("connectors runtime handle should be available");
    let (stdout, stderr) = runtime.collect_logs();
    format!("{stdout}\n{stderr}")
}

fn parse_benchmark_events(logs: &str, connector_type: &str) -> Vec<HashMap<String, String>> {
    logs.lines()
        .filter(|line| line.contains(BENCHMARK_TARGET))
        .filter_map(parse_event_fields)
        .filter(|fields| {
            fields
                .get("connector_type")
                .map(|value| value == connector_type)
                .unwrap_or(false)
        })
        .collect()
}

fn parse_benchmark_events_for_key(
    logs: &str,
    connector_type: &str,
    connector_key: &str,
) -> Vec<HashMap<String, String>> {
    parse_benchmark_events(logs, connector_type)
        .into_iter()
        .filter(|fields| {
            fields
                .get("connector_key")
                .map(|value| value == connector_key)
                .unwrap_or(false)
        })
        .collect()
}

fn parse_event_fields(line: &str) -> Option<HashMap<String, String>> {
    let payload_start = line.find(BENCHMARK_TARGET)? + BENCHMARK_TARGET.len();
    let payload = strip_ansi(&line[payload_start..]);
    let payload = payload.trim_start_matches([':', ' ']);
    let mut fields = HashMap::new();
    let bytes = payload.as_bytes();
    let mut search_from = 0;
    while let Some(rel) = payload[search_from..].find('=') {
        let equals_idx = search_from + rel;
        let key_start = payload[..equals_idx]
            .rfind(|c: char| c.is_whitespace())
            .map(|idx| idx + 1)
            .unwrap_or(0);
        let key = payload[key_start..equals_idx].trim().to_string();
        let value_start = equals_idx + 1;
        let (value, next) = parse_value(bytes, value_start, payload);
        if !key.is_empty() {
            fields.insert(key, value);
        }
        if next <= equals_idx {
            break;
        }
        search_from = next;
    }
    if fields.is_empty() {
        None
    } else {
        Some(fields)
    }
}

fn parse_value(bytes: &[u8], start: usize, payload: &str) -> (String, usize) {
    if start < bytes.len() && bytes[start] == b'"' {
        let mut cursor = start + 1;
        let value_start = cursor;
        while cursor < bytes.len() && bytes[cursor] != b'"' {
            if bytes[cursor] == b'\\' && cursor + 1 < bytes.len() {
                cursor += 2;
            } else {
                cursor += 1;
            }
        }
        let value = payload[value_start..cursor].to_string();
        let next = if cursor < bytes.len() {
            cursor + 1
        } else {
            cursor
        };
        (value, next)
    } else {
        let mut cursor = start;
        while cursor < bytes.len() && !bytes[cursor].is_ascii_whitespace() {
            cursor += 1;
        }
        (payload[start..cursor].to_string(), cursor)
    }
}

fn strip_ansi(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' && chars.peek() == Some(&'[') {
            chars.next();
            for next in chars.by_ref() {
                if next.is_ascii_alphabetic() {
                    break;
                }
            }
        } else {
            out.push(c);
        }
    }
    out
}

fn parse_u64(event: &HashMap<String, String>, key: &str) -> u64 {
    event
        .get(key)
        .unwrap_or_else(|| panic!("missing field {key} in event {event:?}"))
        .parse::<u64>()
        .unwrap_or_else(|error| panic!("field {key} should be u64, got error: {error}: {event:?}"))
}

fn parse_opt_u64(event: &HashMap<String, String>, key: &str) -> Option<u64> {
    let raw = event
        .get(key)
        .unwrap_or_else(|| panic!("missing field {key} in event {event:?}"));
    if raw == "None" {
        return None;
    }
    let inner = raw
        .strip_prefix("Some(")
        .and_then(|s| s.strip_suffix(")"))
        .unwrap_or_else(|| panic!("field {key} should be Option<u64>, got {raw}: {event:?}"));
    Some(
        inner.parse::<u64>().unwrap_or_else(|error| {
            panic!("field {key} should be u64, got error: {error}: {event:?}")
        }),
    )
}

#[cfg(test)]
mod parser_tests {
    use super::*;

    #[test]
    fn given_simple_unquoted_fields_when_parsed_should_extract_all() {
        let line = "...iggy_connectors::benchmark: benchmark partition_id=3 batch_size=42";
        let fields = parse_event_fields(line).expect("fields parsed");
        assert_eq!(fields.get("partition_id"), Some(&"3".to_string()));
        assert_eq!(fields.get("batch_size"), Some(&"42".to_string()));
    }

    #[test]
    fn given_quoted_string_values_when_parsed_should_strip_quotes() {
        let line = "...iggy_connectors::benchmark: benchmark connector_type=\"sink\" connector_key=\"postgres\"";
        let fields = parse_event_fields(line).expect("fields parsed");
        assert_eq!(fields.get("connector_type"), Some(&"sink".to_string()));
        assert_eq!(fields.get("connector_key"), Some(&"postgres".to_string()));
    }

    #[test]
    fn given_quoted_value_with_embedded_space_when_parsed_should_preserve_space() {
        let line = "...iggy_connectors::benchmark: benchmark connector_name=\"my connector\" batch_size=10";
        let fields = parse_event_fields(line).expect("fields parsed");
        assert_eq!(
            fields.get("connector_name"),
            Some(&"my connector".to_string())
        );
        assert_eq!(fields.get("batch_size"), Some(&"10".to_string()));
    }

    #[test]
    fn given_ansi_color_codes_when_parsed_should_be_stripped() {
        let line = "\u{1b}[32miggy_connectors::benchmark\u{1b}[0m: benchmark \u{1b}[3mconnector_type\u{1b}[0m=\"sink\" \u{1b}[3mbatch_size\u{1b}[0m=7";
        let fields = parse_event_fields(line).expect("fields parsed");
        assert_eq!(fields.get("connector_type"), Some(&"sink".to_string()));
        assert_eq!(fields.get("batch_size"), Some(&"7".to_string()));
    }

    #[test]
    fn given_line_without_target_when_parsed_should_return_none() {
        let line = "INFO some_other_target: unrelated message foo=bar";
        assert!(parse_event_fields(line).is_none());
    }

    #[test]
    fn given_logs_with_mixed_targets_when_filtered_should_only_keep_benchmark_events() {
        let logs = "INFO iggy_connectors::sink: regular log foo=1\n\
                    INFO iggy_connectors::benchmark: benchmark connector_type=\"sink\" batch_size=4\n\
                    INFO iggy_connectors::benchmark: benchmark connector_type=\"source\" batch_size=2\n";
        let sink_events = parse_benchmark_events(logs, "sink");
        let source_events = parse_benchmark_events(logs, "source");
        assert_eq!(sink_events.len(), 1);
        assert_eq!(source_events.len(), 1);
        assert_eq!(sink_events[0].get("batch_size"), Some(&"4".to_string()));
        assert_eq!(source_events[0].get("batch_size"), Some(&"2".to_string()));
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/runtime/benchmark.toml")),
    seed = seeds::connector_stream
)]
async fn given_runtime_processing_batches_when_metrics_scraped_should_expose_stage_histograms(
    harness: &TestHarness,
) {
    let (_sink_events, _source_events) =
        wait_for_both_kinds(harness, "stdout_bench", "random_bench").await;

    let metrics_body = scrape_metrics(harness).await;

    assert!(
        metrics_body.contains("iggy_connector_stage_duration_seconds"),
        "expected stage histogram in /metrics, got:\n{metrics_body}"
    );

    let sink_total_count = parse_metric_value(
        &metrics_body,
        "iggy_connector_stage_duration_seconds_count",
        &[
            ("connector_key", "stdout_bench"),
            ("connector_type", "Sink"),
            ("stage", "total"),
        ],
    );
    assert!(
        sink_total_count.is_some_and(|count| count > 0),
        "sink Total histogram count should be > 0, got {sink_total_count:?}"
    );

    let sink_ffi_count = parse_metric_value(
        &metrics_body,
        "iggy_connector_stage_duration_seconds_count",
        &[
            ("connector_key", "stdout_bench"),
            ("connector_type", "Sink"),
            ("stage", "ffi"),
        ],
    );
    assert!(
        sink_ffi_count.is_some_and(|count| count > 0),
        "sink Ffi histogram count should be > 0, got {sink_ffi_count:?}"
    );

    let source_total_count = parse_metric_value(
        &metrics_body,
        "iggy_connector_stage_duration_seconds_count",
        &[
            ("connector_key", "random_bench"),
            ("connector_type", "Source"),
            ("stage", "total"),
        ],
    );
    assert!(
        source_total_count.is_some_and(|count| count > 0),
        "source Total histogram count should be > 0, got {source_total_count:?}"
    );

    let source_send_count = parse_metric_value(
        &metrics_body,
        "iggy_connector_stage_duration_seconds_count",
        &[
            ("connector_key", "random_bench"),
            ("connector_type", "Source"),
            ("stage", "iggy_send"),
        ],
    );
    assert!(
        source_send_count.is_some_and(|count| count > 0),
        "source IggySend histogram count should be > 0, got {source_send_count:?}"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/runtime/benchmark_disabled.toml")),
    seed = seeds::connector_stream
)]
async fn given_benchmark_flag_off_when_metrics_scraped_should_still_expose_histograms(
    harness: &TestHarness,
) {
    sleep(Duration::from_millis(1500)).await;

    let metrics_body = scrape_metrics(harness).await;
    let count = parse_metric_value(
        &metrics_body,
        "iggy_connector_stage_duration_seconds_count",
        &[
            ("connector_key", "stdout_off"),
            ("connector_type", "Sink"),
            ("stage", "total"),
        ],
    );
    assert!(
        count.is_some_and(|count| count > 0),
        "histograms must populate regardless of the benchmark flag (the flag only gates the text event), got {count:?}\n{metrics_body}"
    );
}

async fn scrape_metrics(harness: &TestHarness) -> String {
    let url = format!(
        "{}/metrics",
        harness
            .connectors_runtime()
            .expect("connectors runtime handle should be available")
            .http_url()
    );
    let response = reqwest::Client::new()
        .get(&url)
        .send()
        .await
        .unwrap_or_else(|e| panic!("failed to GET {url}: {e}"));
    assert_eq!(response.status(), 200, "GET {url} returned non-200");
    response
        .text()
        .await
        .unwrap_or_else(|e| panic!("failed to read /metrics body: {e}"))
}

fn parse_metric_value(body: &str, metric: &str, labels: &[(&str, &str)]) -> Option<u64> {
    body.lines()
        .filter(|line| line.starts_with(metric))
        .find_map(|line| {
            let label_section = line.split('{').nth(1)?.split('}').next()?;
            for (key, want) in labels {
                let needle = format!("{key}=\"{want}\"");
                if !label_section.contains(&needle) {
                    return None;
                }
            }
            let value_str = line.rsplit_once('}').and_then(|(_, rest)| {
                let trimmed = rest.trim();
                trimmed.split_whitespace().next()
            })?;
            value_str.parse::<u64>().ok()
        })
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/runtime/benchmark_json.toml")),
    seed = seeds::connector_stream
)]
async fn given_logging_format_json_when_benchmark_events_emitted_should_be_serde_json_parseable(
    harness: &TestHarness,
) {
    let json_events = wait_for_json_benchmark_events(harness, "stdout_json", "random_json").await;

    let sink_event = json_events
        .iter()
        .find(|event| {
            event.get("connector_type").and_then(|v| v.as_str()) == Some("sink")
                && event.get("connector_key").and_then(|v| v.as_str()) == Some("stdout_json")
        })
        .expect("sink benchmark JSON event should be present");
    let fields = sink_event
        .get("fields")
        .or(Some(sink_event))
        .expect("event has fields");
    let target = sink_event
        .get("target")
        .and_then(|v| v.as_str())
        .expect("event has target");
    assert_eq!(target, BENCHMARK_TARGET);
    for key in [
        "connector_type",
        "connector_key",
        "stream",
        "topic",
        "batch_size",
        "processed_count",
        "prepare_us",
        "ffi_us",
        "total_us",
    ] {
        assert!(
            fields.get(key).is_some(),
            "sink benchmark JSON event missing field {key}: {sink_event}"
        );
    }

    let source_event = json_events
        .iter()
        .find(|event| {
            event.get("connector_type").and_then(|v| v.as_str()) == Some("source")
                && event.get("connector_key").and_then(|v| v.as_str()) == Some("random_json")
        })
        .expect("source benchmark JSON event should be present");
    let fields = source_event
        .get("fields")
        .or(Some(source_event))
        .expect("event has fields");
    for key in [
        "connector_type",
        "connector_key",
        "batch_size",
        "sent_count",
        "decode_us",
        "prepare_us",
        "iggy_send_us",
        "state_save_us",
        "total_us",
    ] {
        assert!(
            fields.get(key).is_some(),
            "source benchmark JSON event missing field {key}: {source_event}"
        );
    }
}

async fn wait_for_json_benchmark_events(
    harness: &TestHarness,
    sink_key: &str,
    source_key: &str,
) -> Vec<serde_json::Value> {
    let deadline = Instant::now() + POLL_DEADLINE;
    loop {
        let logs = collect_runtime_logs(harness);
        let mut events = parse_json_benchmark_events(&logs);
        events.retain(|event| {
            let fields = event.get("fields").unwrap_or(event);
            matches!(
                fields.get("connector_key").and_then(|v| v.as_str()),
                Some(k) if k == sink_key || k == source_key
            )
        });
        let has_sink = events.iter().any(|event| {
            event
                .get("fields")
                .unwrap_or(event)
                .get("connector_key")
                .and_then(|v| v.as_str())
                == Some(sink_key)
        });
        let has_source = events.iter().any(|event| {
            event
                .get("fields")
                .unwrap_or(event)
                .get("connector_key")
                .and_then(|v| v.as_str())
                == Some(source_key)
        });
        if has_sink && has_source {
            // Flatten {fields: {...}} envelopes for assertion convenience.
            return events
                .into_iter()
                .map(|event| {
                    if let Some(fields) = event.get("fields").cloned() {
                        let mut merged = fields;
                        if let (Some(target), Some(merged_obj)) =
                            (event.get("target"), merged.as_object_mut())
                        {
                            merged_obj.insert("target".to_string(), target.clone());
                        }
                        merged
                    } else {
                        event
                    }
                })
                .collect();
        }
        if Instant::now() >= deadline {
            panic!(
                "timed out waiting for sink ({sink_key}) + source ({source_key}) JSON benchmark events. \
                 logs:\n{logs}"
            );
        }
        sleep(POLL_INTERVAL).await;
    }
}

fn parse_json_benchmark_events(logs: &str) -> Vec<serde_json::Value> {
    logs.lines()
        .filter_map(|line| serde_json::from_str::<serde_json::Value>(line.trim()).ok())
        .filter(|event| event.get("target").and_then(|v| v.as_str()) == Some(BENCHMARK_TARGET))
        .collect()
}

pub struct BenchmarkEnvOverrideFixture;

#[async_trait]
impl TestFixture for BenchmarkEnvOverrideFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Ok(Self)
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        let mut envs = HashMap::new();
        envs.insert(
            "IGGY_CONNECTORS_SINK_STDOUT_OFF_BENCHMARK".to_string(),
            "true".to_string(),
        );
        envs.insert(
            "IGGY_CONNECTORS_SOURCE_RANDOM_OFF_BENCHMARK".to_string(),
            "true".to_string(),
        );
        envs
    }

    async fn seed(&self, _client: &IggyClient) -> Result<(), SeedError> {
        Ok(())
    }

    fn has_seed(&self) -> bool {
        false
    }
}
