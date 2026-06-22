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
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
use opentelemetry_proto::tonic::metrics::v1::{
    Gauge, Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum, metric, number_data_point,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status, span};
use serde_json::Value;
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Public entry points
// ---------------------------------------------------------------------------

pub fn traces_from_json(messages: &[Value]) -> ExportTraceServiceRequest {
    // Group spans by service_name so each service gets one ResourceSpans.
    let mut groups: HashMap<String, (Value, Vec<Span>)> = HashMap::new();

    for msg in messages {
        let service = msg
            .get("service_name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        let resource = msg.get("resource").cloned().unwrap_or(Value::Null);
        let span = json_to_span(msg);
        groups
            .entry(service)
            .or_insert_with(|| (resource, Vec::new()))
            .1
            .push(span);
    }

    ExportTraceServiceRequest {
        resource_spans: groups
            .into_values()
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
    let mut groups: HashMap<String, (Value, Vec<Metric>)> = HashMap::new();

    for msg in messages {
        let service = msg
            .get("service_name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        let resource = msg.get("resource").cloned().unwrap_or(Value::Null);
        if let Some(metric) = json_to_metric(msg) {
            groups
                .entry(service)
                .or_insert_with(|| (resource, Vec::new()))
                .1
                .push(metric);
        }
    }

    ExportMetricsServiceRequest {
        resource_metrics: groups
            .into_values()
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
    let mut groups: HashMap<String, (Value, Vec<LogRecord>)> = HashMap::new();

    for msg in messages {
        let service = msg
            .get("service_name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        let resource = msg.get("resource").cloned().unwrap_or(Value::Null);
        let record = json_to_log_record(msg);
        groups
            .entry(service)
            .or_insert_with(|| (resource, Vec::new()))
            .1
            .push(record);
    }

    ExportLogsServiceRequest {
        resource_logs: groups
            .into_values()
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
                aggregation_temporality: 2, // CUMULATIVE
                is_monotonic: false,
            }))
        }
        _ => {
            // Default to gauge for gauge, histogram summary etc.
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
        body: v.get("body").map(|b| AnyValue {
            value: Some(any_value::Value::StringValue(b.to_string())),
        }),
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
        Value::Array(arr) => {
            use opentelemetry_proto::tonic::common::v1::ArrayValue;
            any_value::Value::ArrayValue(ArrayValue {
                values: arr.iter().map(json_to_any_value).collect(),
            })
        }
        Value::Object(obj) => {
            use opentelemetry_proto::tonic::common::v1::KeyValueList;
            any_value::Value::KvlistValue(KeyValueList {
                values: obj
                    .iter()
                    .map(|(k, val)| KeyValue {
                        key: k.clone(),
                        value: Some(json_to_any_value(val)),
                        ..Default::default()
                    })
                    .collect(),
            })
        }
        Value::Null => any_value::Value::StringValue(String::new()),
    };
    AnyValue { value: Some(inner) }
}

fn hex_to_bytes(s: &str) -> Vec<u8> {
    if s.is_empty() {
        return vec![];
    }
    (0..s.len())
        .step_by(2)
        .filter_map(|i| u8::from_str_radix(s.get(i..i + 2)?, 16).ok())
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
    match s {
        "TRACE" => 1,
        "DEBUG" => 5,
        "INFO" => 9,
        "WARN" => 13,
        "ERROR" => 17,
        "FATAL" => 21,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

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
}
