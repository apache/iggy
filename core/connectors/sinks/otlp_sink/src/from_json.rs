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

//! Reconstruct OTLP proto requests from the JSON produced by `otlp_source`'s
//! `convert.rs`. The JSON field names match what `convert.rs` emits; adding a
//! new field there requires a corresponding parse here.

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{
    AnyValue, ArrayValue, KeyValue, KeyValueList, any_value,
};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{
    AggregationTemporality, Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
    metric, number_data_point,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status, span};
use serde_json::Value;
use std::collections::HashMap;
use std::fmt::Write as _;

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

pub fn traces_from_json(messages: &[Value]) -> ExportTraceServiceRequest {
    let groups = group_by_resource(messages.iter().map(|msg| {
        let resource = msg.get("resource").cloned().unwrap_or(Value::Null);
        (resource, json_to_span(msg))
    }));

    ExportTraceServiceRequest {
        resource_spans: groups
            .into_iter()
            .map(|(resource, spans)| ResourceSpans {
                resource: Some(json_to_resource(&resource)),
                scope_spans: vec![ScopeSpans {
                    scope: None,
                    spans,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            })
            .collect(),
    }
}

pub fn metrics_from_json(messages: &[Value]) -> ExportMetricsServiceRequest {
    let groups = group_by_resource(messages.iter().filter_map(|msg| {
        let metric = json_to_metric(msg)?;
        let resource = msg.get("resource").cloned().unwrap_or(Value::Null);
        Some((resource, metric))
    }));

    ExportMetricsServiceRequest {
        resource_metrics: groups
            .into_iter()
            .map(|(resource, metrics)| ResourceMetrics {
                resource: Some(json_to_resource(&resource)),
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            })
            .collect(),
    }
}

pub fn logs_from_json(messages: &[Value]) -> ExportLogsServiceRequest {
    let groups = group_by_resource(messages.iter().map(|msg| {
        let resource = msg.get("resource").cloned().unwrap_or(Value::Null);
        (resource, json_to_log_record(msg))
    }));

    ExportLogsServiceRequest {
        resource_logs: groups
            .into_iter()
            .map(|(resource, log_records)| ResourceLogs {
                resource: Some(json_to_resource(&resource)),
                scope_logs: vec![ScopeLogs {
                    scope: None,
                    log_records,
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            })
            .collect(),
    }
}

/// Group records by full resource identity, not by `service.name` alone.
///
/// Two records can share a `service.name` yet carry different resources
/// (distinct pod, host, or instance attributes). Keying only on the service
/// emitted every such record under the first resource seen, silently
/// corrupting the others' resource attributes. A canonical string of the whole
/// `resource` value is the grouping key instead, so each distinct resource gets
/// its own `Resource{Spans,Metrics,Logs}`. Insertion order is preserved (first
/// occurrence wins its slot) to keep output stable across runs.
fn group_by_resource<T>(items: impl Iterator<Item = (Value, T)>) -> Vec<(Value, Vec<T>)> {
    let mut index: HashMap<String, usize> = HashMap::new();
    let mut groups: Vec<(Value, Vec<T>)> = Vec::new();
    for (resource, item) in items {
        let next = groups.len();
        let slot = *index
            .entry(canonical_resource_key(&resource))
            .or_insert(next);
        if slot == next {
            groups.push((resource, vec![item]));
        } else {
            groups[slot].1.push(item);
        }
    }
    groups
}

/// Stable identity string for a `resource` value.
///
/// `serde_json` is built with `preserve_order` in this workspace, so its `Map`
/// keeps insertion order and `to_string` would render two equivalent resources
/// with differently ordered keys as different strings. Emitting object keys in
/// sorted order at every level makes the key canonical regardless of input
/// ordering.
fn canonical_resource_key(v: &Value) -> String {
    let mut out = String::new();
    write_canonical(v, &mut out);
    out
}

fn write_canonical(v: &Value, out: &mut String) {
    match v {
        Value::Object(map) => {
            out.push('{');
            let mut entries: Vec<(&String, &Value)> = map.iter().collect();
            entries.sort_unstable_by(|a, b| a.0.cmp(b.0));
            for (i, (key, val)) in entries.into_iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                let _ = write!(out, "{key:?}:");
                write_canonical(val, out);
            }
            out.push('}');
        }
        Value::Array(arr) => {
            out.push('[');
            for (i, item) in arr.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        scalar => {
            let _ = write!(out, "{scalar}");
        }
    }
}

// ---------------------------------------------------------------------------
// Per-signal converters
// ---------------------------------------------------------------------------

fn json_to_span(v: &Value) -> Span {
    Span {
        trace_id: hex_to_bytes(v.get("trace_id").and_then(Value::as_str).unwrap_or("")),
        span_id: hex_to_bytes(v.get("span_id").and_then(Value::as_str).unwrap_or("")),
        parent_span_id: hex_to_bytes(
            v.get("parent_span_id")
                .and_then(Value::as_str)
                .unwrap_or(""),
        ),
        name: v
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned(),
        kind: span_kind_from_text(v.get("kind").and_then(Value::as_str).unwrap_or("")),
        start_time_unix_nano: v.get("start_time_ns").and_then(Value::as_u64).unwrap_or(0),
        end_time_unix_nano: v.get("end_time_ns").and_then(Value::as_u64).unwrap_or(0),
        attributes: json_obj_to_kv(v.get("attributes")),
        dropped_attributes_count: 0,
        events: vec![],
        dropped_events_count: 0,
        links: vec![],
        dropped_links_count: 0,
        status: Some(Status {
            code: status_code_from_text(v.get("status").and_then(Value::as_str).unwrap_or("unset")),
            message: v
                .get("status_message")
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_owned(),
        }),
        flags: 0,
        trace_state: String::new(),
    }
}

fn json_to_metric(v: &Value) -> Option<Metric> {
    let name = v.get("name")?.as_str()?.to_owned();
    let unit = v
        .get("unit")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_owned();
    let time_ns = v.get("timestamp_ns").and_then(Value::as_u64).unwrap_or(0);
    let attrs = json_obj_to_kv(v.get("attributes"));
    let metric_type = v.get("type").and_then(Value::as_str).unwrap_or("gauge");

    let value_field = v.get("value");
    let as_double = value_field.and_then(Value::as_f64).unwrap_or(0.0);

    let data = match metric_type {
        "sum" => {
            let dp = NumberDataPoint {
                attributes: attrs,
                time_unix_nano: time_ns,
                value: Some(number_data_point::Value::AsDouble(as_double)),
                ..Default::default()
            };
            Some(metric::Data::Sum(Sum {
                data_points: vec![dp],
                aggregation_temporality: AggregationTemporality::Cumulative as i32,
                is_monotonic: v
                    .get("is_monotonic")
                    .and_then(Value::as_bool)
                    .unwrap_or(true),
            }))
        }
        _ => {
            // Default to gauge for gauge, histogram, summary etc.
            // Histogram detail is lost in convert.rs (only count+sum stored);
            // reconstruct as gauge using count as the value.
            let dp_value = if metric_type == "histogram" || metric_type == "summary" {
                value_field
                    .and_then(|v| v.get("count"))
                    .and_then(Value::as_f64)
                    .unwrap_or(0.0)
            } else {
                as_double
            };
            Some(metric::Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    attributes: attrs,
                    time_unix_nano: time_ns,
                    value: Some(number_data_point::Value::AsDouble(dp_value)),
                    ..Default::default()
                }],
            }))
        }
    };

    Some(Metric {
        name,
        description: String::new(),
        unit,
        data,
        metadata: vec![],
    })
}

fn json_to_log_record(v: &Value) -> LogRecord {
    LogRecord {
        time_unix_nano: v.get("timestamp_ns").and_then(Value::as_u64).unwrap_or(0),
        observed_time_unix_nano: v
            .get("observed_timestamp_ns")
            .and_then(Value::as_u64)
            .unwrap_or(0),
        severity_number: severity_from_text(
            v.get("severity").and_then(Value::as_str).unwrap_or(""),
        ),
        severity_text: v
            .get("severity_text")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned(),
        body: v.get("body").map(json_to_any_value),
        attributes: json_obj_to_kv(v.get("attributes")),
        trace_id: hex_to_bytes(v.get("trace_id").and_then(Value::as_str).unwrap_or("")),
        span_id: hex_to_bytes(v.get("span_id").and_then(Value::as_str).unwrap_or("")),
        dropped_attributes_count: 0,
        flags: 0,
        ..Default::default()
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn json_to_resource(v: &Value) -> Resource {
    Resource {
        attributes: json_obj_to_kv(Some(v)),
        dropped_attributes_count: 0,
        ..Default::default()
    }
}

fn json_obj_to_kv(v: Option<&Value>) -> Vec<KeyValue> {
    let obj = match v.and_then(Value::as_object) {
        Some(o) => o,
        None => return vec![],
    };
    obj.iter()
        .map(|(k, val)| KeyValue {
            key: k.clone(),
            value: Some(json_to_any_value(val)),
            ..Default::default()
        })
        .collect()
}

fn json_to_any_value(v: &Value) -> AnyValue {
    let inner = match v {
        Value::String(s) => any_value::Value::StringValue(s.clone()),
        Value::Bool(b) => any_value::Value::BoolValue(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                any_value::Value::IntValue(i)
            } else {
                any_value::Value::DoubleValue(n.as_f64().unwrap_or(0.0))
            }
        }
        Value::Array(arr) => any_value::Value::ArrayValue(ArrayValue {
            values: arr.iter().map(json_to_any_value).collect(),
        }),
        Value::Object(obj) => any_value::Value::KvlistValue(KeyValueList {
            values: obj
                .iter()
                .map(|(k, val)| KeyValue {
                    key: k.clone(),
                    value: Some(json_to_any_value(val)),
                    ..Default::default()
                })
                .collect(),
        }),
        Value::Null => any_value::Value::StringValue(String::new()),
    };
    AnyValue { value: Some(inner) }
}

fn hex_to_bytes(s: &str) -> Vec<u8> {
    // Iterate raw bytes, not string slices: a multibyte UTF-8 char landing on an
    // even index would make `&s[i..i + 2]` panic on a non-char-boundary before
    // any radix check could reject it. Treating each byte as a candidate hex
    // digit rejects non-ASCII input gracefully (skipped) instead.
    let bytes = s.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return vec![];
    }
    bytes
        .chunks_exact(2)
        .filter_map(|pair| {
            let hi = (pair[0] as char).to_digit(16)?;
            let lo = (pair[1] as char).to_digit(16)?;
            Some((hi * 16 + lo) as u8)
        })
        .collect()
}

fn span_kind_from_text(s: &str) -> i32 {
    match s {
        "internal" => span::SpanKind::Internal as i32,
        "server" => span::SpanKind::Server as i32,
        "client" => span::SpanKind::Client as i32,
        "producer" => span::SpanKind::Producer as i32,
        "consumer" => span::SpanKind::Consumer as i32,
        _ => span::SpanKind::Unspecified as i32,
    }
}

fn status_code_from_text(s: &str) -> i32 {
    match s {
        "ok" => 1,
        "error" => 2,
        _ => 0, // unset
    }
}

fn severity_from_text(s: &str) -> i32 {
    match s.to_ascii_uppercase().as_str() {
        "TRACE" => 1,
        "DEBUG" => 5,
        "INFO" => 9,
        "WARN" | "WARNING" => 13,
        "ERROR" => 17,
        "FATAL" => 21,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn pod_name(res: &Resource) -> Option<&str> {
        res.attributes
            .iter()
            .find(|kv| kv.key == "k8s.pod.name")
            .and_then(|kv| kv.value.as_ref())
            .and_then(|v| match v.value.as_ref() {
                Some(any_value::Value::StringValue(s)) => Some(s.as_str()),
                _ => None,
            })
    }

    #[test]
    fn given_empty_messages_should_return_empty_request() {
        let req = traces_from_json(&[]);
        assert!(req.resource_spans.is_empty());
    }

    #[test]
    fn given_trace_json_should_reconstruct_span() {
        let msg = json!({
            "signal": "trace",
            "trace_id": "0102030405060708090a0b0c0d0e0f10",
            "span_id": "0102030405060708",
            "name": "test-span",
            "kind": "server",
            "start_time_ns": 1_000_000_u64,
            "end_time_ns": 2_000_000_u64,
            "status": "ok",
            "service_name": "svc-a",
            "resource": { "service.name": "svc-a" },
            "attributes": { "http.method": "GET" }
        });
        let req = traces_from_json(&[msg]);
        assert_eq!(req.resource_spans.len(), 1);
        let spans = &req.resource_spans[0].scope_spans[0].spans;
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].name, "test-span");
        assert_eq!(spans[0].kind, span::SpanKind::Server as i32);
        assert_eq!(spans[0].start_time_unix_nano, 1_000_000);
        assert_eq!(spans[0].status.as_ref().unwrap().code, 1); // ok
    }

    #[test]
    fn given_hex_trace_id_should_decode_correctly() {
        let bytes = hex_to_bytes("deadbeef");
        assert_eq!(bytes, vec![0xde, 0xad, 0xbe, 0xef]);
    }

    #[test]
    fn given_empty_hex_should_return_empty_bytes() {
        assert!(hex_to_bytes("").is_empty());
    }

    #[test]
    fn given_odd_length_hex_should_return_empty_bytes() {
        assert!(hex_to_bytes("abc").is_empty());
    }

    #[test]
    fn given_log_json_should_reconstruct_log_record() {
        let msg = json!({
            "signal": "log",
            "timestamp_ns": 999_u64,
            "severity": "WARN",
            "severity_text": "WARNING",
            "body": "something happened",
            "service_name": "svc-b",
            "resource": {},
            "attributes": {}
        });
        let req = logs_from_json(&[msg]);
        assert_eq!(req.resource_logs.len(), 1);
        let records = &req.resource_logs[0].scope_logs[0].log_records;
        assert_eq!(records[0].severity_number, 13); // WARN
    }

    #[test]
    fn given_lowercase_severity_should_map_correctly() {
        assert_eq!(severity_from_text("warn"), 13);
        assert_eq!(severity_from_text("info"), 9);
        assert_eq!(severity_from_text("error"), 17);
        assert_eq!(severity_from_text("debug"), 5);
    }

    #[test]
    fn given_string_log_body_should_not_double_encode() {
        let msg = json!({
            "body": "hello world",
            "service_name": "svc",
            "resource": {}
        });
        let req = logs_from_json(&[msg]);
        let record = &req.resource_logs[0].scope_logs[0].log_records[0];
        let body = record.body.as_ref().unwrap();
        assert_eq!(
            body.value,
            Some(any_value::Value::StringValue("hello world".to_owned()))
        );
    }

    #[test]
    fn given_non_ascii_even_byte_hex_should_not_panic() {
        // "a\u{20ac}" is 4 bytes with a multibyte char straddling an even index;
        // the old `&s[i..i + 2]` slicing panicked on the non-char-boundary.
        assert!(hex_to_bytes("a\u{20ac}").is_empty());
    }

    #[test]
    fn given_same_service_distinct_resources_should_emit_separate_resource_spans() {
        let mk = |pod: &str, span: &str| {
            json!({
                "service_name": "svc",
                "resource": { "service.name": "svc", "k8s.pod.name": pod },
                "name": span,
                "trace_id": "0102030405060708090a0b0c0d0e0f10",
                "span_id": "0102030405060708"
            })
        };
        let req = traces_from_json(&[
            mk("pod-a", "span-a"),
            mk("pod-b", "span-b"),
            mk("pod-a", "span-c"),
        ]);

        assert_eq!(req.resource_spans.len(), 2);

        let first = &req.resource_spans[0];
        assert_eq!(pod_name(first.resource.as_ref().unwrap()), Some("pod-a"));
        let first_names: Vec<&str> = first.scope_spans[0]
            .spans
            .iter()
            .map(|s| s.name.as_str())
            .collect();
        assert_eq!(first_names, vec!["span-a", "span-c"]);

        let second = &req.resource_spans[1];
        assert_eq!(pod_name(second.resource.as_ref().unwrap()), Some("pod-b"));
        assert_eq!(second.scope_spans[0].spans.len(), 1);
        assert_eq!(second.scope_spans[0].spans[0].name, "span-b");
    }

    #[test]
    fn given_same_service_distinct_resources_should_emit_separate_resource_metrics() {
        let mk = |pod: &str, name: &str| {
            json!({
                "service_name": "svc",
                "resource": { "service.name": "svc", "k8s.pod.name": pod },
                "name": name,
                "type": "gauge",
                "value": 1.0
            })
        };
        let req = metrics_from_json(&[mk("pod-a", "m1"), mk("pod-b", "m2")]);

        assert_eq!(req.resource_metrics.len(), 2);
        assert_eq!(
            pod_name(req.resource_metrics[0].resource.as_ref().unwrap()),
            Some("pod-a")
        );
        assert_eq!(
            pod_name(req.resource_metrics[1].resource.as_ref().unwrap()),
            Some("pod-b")
        );
    }

    #[test]
    fn given_same_service_distinct_resources_should_emit_separate_resource_logs() {
        let mk = |pod: &str, body: &str| {
            json!({
                "service_name": "svc",
                "resource": { "service.name": "svc", "k8s.pod.name": pod },
                "body": body
            })
        };
        let req = logs_from_json(&[mk("pod-a", "l1"), mk("pod-b", "l2")]);

        assert_eq!(req.resource_logs.len(), 2);
        assert_eq!(
            pod_name(req.resource_logs[0].resource.as_ref().unwrap()),
            Some("pod-a")
        );
        assert_eq!(
            pod_name(req.resource_logs[1].resource.as_ref().unwrap()),
            Some("pod-b")
        );
    }

    #[test]
    fn given_same_resource_keys_in_different_order_should_group_together() {
        // preserve_order keeps insertion order, so equivalent resources with
        // keys in a different order must still collapse into one group.
        let a = json!({ "resource": { "a": "1", "b": "2" }, "body": "x" });
        let b = json!({ "resource": { "b": "2", "a": "1" }, "body": "y" });
        let req = logs_from_json(&[a, b]);

        assert_eq!(req.resource_logs.len(), 1);
        assert_eq!(req.resource_logs[0].scope_logs[0].log_records.len(), 2);
    }
}
