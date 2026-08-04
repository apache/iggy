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

use iggy_connector_sdk::ProducedMessage;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue, any_value};
use opentelemetry_proto::tonic::metrics::v1::{Metric, metric, number_data_point};
use opentelemetry_proto::tonic::resource::v1::Resource;
use serde::Serialize;
use serde_json::{Map, Value, json};
use std::fmt::Write as _;
use tracing::warn;

/// Resource attributes are shared by every record under one `ResourceLogs` /
/// `ResourceSpans` / `ResourceMetrics` envelope. The documents below borrow the
/// map instead of owning it so a batch of N records serializes the same
/// attributes N times without cloning them N times.
#[derive(Serialize)]
struct LogDoc<'a> {
    signal: &'static str,
    timestamp_ns: u64,
    observed_timestamp_ns: u64,
    severity: &'static str,
    #[serde(skip_serializing_if = "str::is_empty")]
    severity_text: &'a str,
    #[serde(skip_serializing_if = "is_absent")]
    body: Option<Value>,
    #[serde(skip_serializing_if = "String::is_empty")]
    trace_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    span_id: String,
    #[serde(skip_serializing_if = "str::is_empty")]
    service_name: &'a str,
    resource: &'a Map<String, Value>,
    attributes: Map<String, Value>,
}

#[derive(Serialize)]
struct SpanDoc<'a> {
    signal: &'static str,
    #[serde(skip_serializing_if = "String::is_empty")]
    trace_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    span_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    parent_span_id: String,
    #[serde(skip_serializing_if = "str::is_empty")]
    name: &'a str,
    kind: &'static str,
    start_time_ns: u64,
    end_time_ns: u64,
    status: &'static str,
    #[serde(skip_serializing_if = "str::is_empty")]
    status_message: &'a str,
    #[serde(skip_serializing_if = "str::is_empty")]
    service_name: &'a str,
    resource: &'a Map<String, Value>,
    attributes: Map<String, Value>,
}

#[derive(Serialize)]
struct MetricDoc<'a> {
    signal: &'static str,
    name: &'a str,
    #[serde(rename = "type")]
    metric_type: &'static str,
    unit: &'a str,
    timestamp_ns: u64,
    value: Value,
    service_name: &'a str,
    resource: &'a Map<String, Value>,
    attributes: Map<String, Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_monotonic: Option<bool>,
}

/// Mirrors the pruning the JSON documents used to do after the fact: a field is
/// omitted when it is absent, JSON null, or an empty string.
fn is_absent(value: &Option<Value>) -> bool {
    match value {
        None | Some(Value::Null) => true,
        Some(Value::String(text)) => text.is_empty(),
        Some(_) => false,
    }
}

pub fn export_logs_to_messages(req: ExportLogsServiceRequest) -> Vec<ProducedMessage> {
    let mut messages = Vec::new();
    for resource_logs in req.resource_logs {
        let resource_attrs = extract_resource_attrs(resource_logs.resource.as_ref());
        let service_name = resource_attrs
            .get("service.name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();

        for scope_logs in resource_logs.scope_logs {
            for record in scope_logs.log_records {
                let doc = LogDoc {
                    signal: "log",
                    timestamp_ns: record.time_unix_nano,
                    observed_timestamp_ns: record.observed_time_unix_nano,
                    severity: severity_number_to_text(record.severity_number),
                    severity_text: &record.severity_text,
                    body: record.body.as_ref().map(any_value_to_json),
                    trace_id: bytes_to_hex(&record.trace_id),
                    span_id: bytes_to_hex(&record.span_id),
                    service_name: &service_name,
                    resource: &resource_attrs,
                    attributes: extract_attrs(&record.attributes),
                };
                match serde_json::to_vec(&doc) {
                    Ok(payload) => messages.push(ProducedMessage {
                        id: None,
                        checksum: None,
                        timestamp: (record.time_unix_nano != 0).then_some(record.time_unix_nano),
                        origin_timestamp: None,
                        headers: None,
                        payload,
                    }),
                    Err(err) => warn!("Failed to serialize log record: {err}"),
                }
            }
        }
    }
    messages
}

pub fn export_metrics_to_messages(req: ExportMetricsServiceRequest) -> Vec<ProducedMessage> {
    let mut messages = Vec::new();
    for resource_metrics in req.resource_metrics {
        let resource_attrs = extract_resource_attrs(resource_metrics.resource.as_ref());
        let service_name = resource_attrs
            .get("service.name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();

        for scope_metrics in resource_metrics.scope_metrics {
            for metric in scope_metrics.metrics {
                let data_points = metric_to_data_points(&metric, &resource_attrs, &service_name);
                messages.extend(data_points);
            }
        }
    }
    messages
}

pub fn export_traces_to_messages(req: ExportTraceServiceRequest) -> Vec<ProducedMessage> {
    let mut messages = Vec::new();
    for resource_spans in req.resource_spans {
        let resource_attrs = extract_resource_attrs(resource_spans.resource.as_ref());
        let service_name = resource_attrs
            .get("service.name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();

        for scope_spans in resource_spans.scope_spans {
            for span in scope_spans.spans {
                let status_code = span
                    .status
                    .as_ref()
                    .map(|s| status_code_to_text(s.code))
                    .unwrap_or("unset");
                let status_message = span
                    .status
                    .as_ref()
                    .map(|s| s.message.as_str())
                    .unwrap_or_default();

                let doc = SpanDoc {
                    signal: "trace",
                    trace_id: bytes_to_hex(&span.trace_id),
                    span_id: bytes_to_hex(&span.span_id),
                    parent_span_id: bytes_to_hex(&span.parent_span_id),
                    name: &span.name,
                    kind: span_kind_to_text(span.kind),
                    start_time_ns: span.start_time_unix_nano,
                    end_time_ns: span.end_time_unix_nano,
                    status: status_code,
                    status_message,
                    service_name: &service_name,
                    resource: &resource_attrs,
                    attributes: extract_attrs(&span.attributes),
                };
                match serde_json::to_vec(&doc) {
                    Ok(payload) => messages.push(ProducedMessage {
                        id: None,
                        checksum: None,
                        timestamp: (span.start_time_unix_nano != 0)
                            .then_some(span.start_time_unix_nano),
                        origin_timestamp: None,
                        headers: None,
                        payload,
                    }),
                    Err(err) => warn!("Failed to serialize span: {err}"),
                }
            }
        }
    }
    messages
}

fn metric_to_data_points(
    metric: &Metric,
    resource_attrs: &Map<String, Value>,
    service_name: &str,
) -> Vec<ProducedMessage> {
    let mut messages = Vec::new();

    let base = |time_ns: u64,
                value: Value,
                attrs: &[KeyValue],
                metric_type: &'static str|
     -> MetricDoc<'_> {
        MetricDoc {
            signal: "metric",
            name: &metric.name,
            metric_type,
            unit: &metric.unit,
            timestamp_ns: time_ns,
            value,
            service_name,
            resource: resource_attrs,
            attributes: extract_attrs(attrs),
            is_monotonic: None,
        }
    };

    match &metric.data {
        Some(metric::Data::Gauge(gauge)) => {
            for dp in &gauge.data_points {
                let value = number_dp_value(&dp.value);
                let doc = base(dp.time_unix_nano, value, &dp.attributes, "gauge");
                push_metric_doc(&doc, dp.time_unix_nano, &mut messages);
            }
        }
        Some(metric::Data::Sum(sum)) => {
            for dp in &sum.data_points {
                let value = number_dp_value(&dp.value);
                let mut doc = base(dp.time_unix_nano, value, &dp.attributes, "sum");
                doc.is_monotonic = Some(sum.is_monotonic);
                push_metric_doc(&doc, dp.time_unix_nano, &mut messages);
            }
        }
        Some(metric::Data::Histogram(hist)) => {
            for dp in &hist.data_points {
                let doc = base(
                    dp.time_unix_nano,
                    json!({ "count": dp.count, "sum": dp.sum }),
                    &dp.attributes,
                    "histogram",
                );
                push_metric_doc(&doc, dp.time_unix_nano, &mut messages);
            }
        }
        Some(metric::Data::ExponentialHistogram(eh)) => {
            for dp in &eh.data_points {
                let doc = base(
                    dp.time_unix_nano,
                    json!({ "count": dp.count, "sum": dp.sum, "scale": dp.scale }),
                    &dp.attributes,
                    "exponential_histogram",
                );
                push_metric_doc(&doc, dp.time_unix_nano, &mut messages);
            }
        }
        Some(metric::Data::Summary(summary)) => {
            for dp in &summary.data_points {
                let doc = base(
                    dp.time_unix_nano,
                    json!({ "count": dp.count, "sum": dp.sum }),
                    &dp.attributes,
                    "summary",
                );
                push_metric_doc(&doc, dp.time_unix_nano, &mut messages);
            }
        }
        None => {}
    }

    messages
}

fn push_metric_doc(doc: &MetricDoc<'_>, time_ns: u64, messages: &mut Vec<ProducedMessage>) {
    match serde_json::to_vec(doc) {
        Ok(payload) => messages.push(ProducedMessage {
            id: None,
            checksum: None,
            timestamp: Some(time_ns),
            origin_timestamp: None,
            headers: None,
            payload,
        }),
        Err(err) => warn!("Failed to serialize metric data point: {err}"),
    }
}

fn number_dp_value(value: &Option<number_data_point::Value>) -> Value {
    match value {
        Some(number_data_point::Value::AsDouble(d)) => json!(d),
        Some(number_data_point::Value::AsInt(i)) => json!(i),
        None => Value::Null,
    }
}

pub fn extract_resource_attrs(resource: Option<&Resource>) -> Map<String, Value> {
    resource
        .map(|r| extract_attrs(&r.attributes))
        .unwrap_or_default()
}

pub fn extract_attrs(attrs: &[KeyValue]) -> Map<String, Value> {
    attrs
        .iter()
        .map(|kv| {
            let value = match kv.value.as_ref() {
                Some(av) if matches!(av.value, Some(any_value::Value::StringValueStrindex(_))) => {
                    warn!(key = %kv.key, "dropping attribute with unrecognized AnyValue variant");
                    Value::Null
                }
                Some(av) => any_value_to_json(av),
                None => Value::Null,
            };
            (kv.key.clone(), value)
        })
        .collect()
}

pub fn any_value_to_json(value: &AnyValue) -> Value {
    match &value.value {
        Some(any_value::Value::StringValue(s)) => Value::String(s.clone()),
        Some(any_value::Value::BoolValue(b)) => Value::Bool(*b),
        Some(any_value::Value::IntValue(i)) => json!(i),
        Some(any_value::Value::DoubleValue(d)) => json!(d),
        Some(any_value::Value::ArrayValue(arr)) => {
            Value::Array(arr.values.iter().map(any_value_to_json).collect())
        }
        Some(any_value::Value::KvlistValue(kvlist)) => Value::Object(extract_attrs(&kvlist.values)),
        Some(any_value::Value::BytesValue(bytes)) => Value::String(bytes_to_hex(bytes)),
        Some(any_value::Value::StringValueStrindex(_)) | None => Value::Null,
    }
}

pub fn bytes_to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

fn severity_number_to_text(number: i32) -> &'static str {
    match number {
        1..=4 => "TRACE",
        5..=8 => "DEBUG",
        9..=12 => "INFO",
        13..=16 => "WARN",
        17..=20 => "ERROR",
        21..=24 => "FATAL",
        _ => "UNSPECIFIED",
    }
}

fn status_code_to_text(code: i32) -> &'static str {
    match code {
        1 => "ok",
        2 => "error",
        _ => "unset",
    }
}

fn span_kind_to_text(kind: i32) -> &'static str {
    match kind {
        1 => "internal",
        2 => "server",
        3 => "client",
        4 => "producer",
        5 => "consumer",
        _ => "unspecified",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::common::v1::{AnyValue, ArrayValue, KeyValueList};
    use opentelemetry_proto::tonic::logs::v1::{LogRecord, ResourceLogs, ScopeLogs};
    use opentelemetry_proto::tonic::metrics::v1::{
        Gauge, Histogram, HistogramDataPoint, Metric, NumberDataPoint, ResourceMetrics,
        ScopeMetrics, Sum,
    };
    use opentelemetry_proto::tonic::trace::v1::{ResourceSpans, ScopeSpans, Span, Status};

    fn string_value(value: &str) -> AnyValue {
        AnyValue {
            value: Some(any_value::Value::StringValue(value.to_owned())),
        }
    }

    fn attribute(key: &str, value: AnyValue) -> KeyValue {
        KeyValue {
            key: key.to_owned(),
            value: Some(value),
            ..Default::default()
        }
    }

    fn service_resource(name: &str) -> Resource {
        Resource {
            attributes: vec![attribute("service.name", string_value(name))],
            ..Default::default()
        }
    }

    fn payload_of(message: &ProducedMessage) -> Value {
        serde_json::from_slice(&message.payload).expect("payload is valid JSON")
    }

    fn logs_request(
        resource: Option<Resource>,
        records: Vec<LogRecord>,
    ) -> ExportLogsServiceRequest {
        ExportLogsServiceRequest {
            resource_logs: vec![ResourceLogs {
                resource,
                scope_logs: vec![ScopeLogs {
                    log_records: records,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn traces_request(resource: Option<Resource>, spans: Vec<Span>) -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource,
                scope_spans: vec![ScopeSpans {
                    spans,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    fn metrics_request(
        resource: Option<Resource>,
        metrics: Vec<Metric>,
    ) -> ExportMetricsServiceRequest {
        ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource,
                scope_metrics: vec![ScopeMetrics {
                    metrics,
                    ..Default::default()
                }],
                ..Default::default()
            }],
        }
    }

    #[test]
    fn given_log_record_should_produce_json_message() {
        let record = LogRecord {
            time_unix_nano: 1_700_000_000_000_000_000,
            observed_time_unix_nano: 1_700_000_000_000_000_001,
            severity_number: 9,
            severity_text: "INFO".to_owned(),
            body: Some(string_value("hello")),
            attributes: vec![attribute("http.method", string_value("GET"))],
            trace_id: vec![0x01, 0x02],
            span_id: vec![0xab],
            ..Default::default()
        };

        let messages =
            export_logs_to_messages(logs_request(Some(service_resource("api")), vec![record]));

        assert_eq!(messages.len(), 1);
        let doc = payload_of(&messages[0]);
        assert_eq!(doc["signal"], "log");
        assert_eq!(doc["timestamp_ns"], 1_700_000_000_000_000_000u64);
        assert_eq!(doc["severity"], "INFO");
        assert_eq!(doc["body"], "hello");
        assert_eq!(doc["trace_id"], "0102");
        assert_eq!(doc["span_id"], "ab");
        assert_eq!(doc["service_name"], "api");
        assert_eq!(doc["resource"]["service.name"], "api");
        assert_eq!(doc["attributes"]["http.method"], "GET");
        assert_eq!(
            messages[0].timestamp,
            Some(1_700_000_000_000_000_000),
            "non-zero record time is carried onto the message"
        );
    }

    #[test]
    fn given_log_record_with_empty_fields_should_prune_them() {
        let record = LogRecord {
            severity_number: 9,
            body: Some(string_value("hello")),
            ..Default::default()
        };

        let messages = export_logs_to_messages(logs_request(None, vec![record]));

        let doc = payload_of(&messages[0]);
        let obj = doc.as_object().expect("document is an object");
        assert!(!obj.contains_key("severity_text"), "empty string is pruned");
        assert!(!obj.contains_key("trace_id"), "empty hex string is pruned");
        assert!(!obj.contains_key("service_name"), "empty string is pruned");
        assert_eq!(doc["body"], "hello");
    }

    #[test]
    fn given_zero_log_timestamp_should_leave_message_timestamp_unset() {
        let record = LogRecord {
            time_unix_nano: 0,
            body: Some(string_value("hello")),
            ..Default::default()
        };

        let messages = export_logs_to_messages(logs_request(None, vec![record]));

        assert_eq!(
            messages[0].timestamp, None,
            "proto default 0 must not become the Unix epoch"
        );
    }

    #[test]
    fn given_shared_resource_should_repeat_it_in_every_record() {
        let record = || LogRecord {
            body: Some(string_value("hello")),
            ..Default::default()
        };

        let messages = export_logs_to_messages(logs_request(
            Some(service_resource("api")),
            vec![record(), record(), record()],
        ));

        assert_eq!(messages.len(), 3);
        for message in &messages {
            assert_eq!(payload_of(message)["resource"]["service.name"], "api");
        }
    }

    #[test]
    fn given_span_should_produce_json_message() {
        let span = Span {
            trace_id: vec![0xde, 0xad],
            span_id: vec![0xbe, 0xef],
            parent_span_id: vec![0x01],
            name: "GET /users".to_owned(),
            kind: 2,
            start_time_unix_nano: 100,
            end_time_unix_nano: 200,
            status: Some(Status {
                code: 2,
                message: "boom".to_owned(),
            }),
            attributes: vec![attribute("http.route", string_value("/users"))],
            ..Default::default()
        };

        let messages =
            export_traces_to_messages(traces_request(Some(service_resource("api")), vec![span]));

        let doc = payload_of(&messages[0]);
        assert_eq!(doc["signal"], "trace");
        assert_eq!(doc["trace_id"], "dead");
        assert_eq!(doc["span_id"], "beef");
        assert_eq!(doc["parent_span_id"], "01");
        assert_eq!(doc["name"], "GET /users");
        assert_eq!(doc["kind"], "server");
        assert_eq!(doc["status"], "error");
        assert_eq!(doc["status_message"], "boom");
        assert_eq!(doc["resource"]["service.name"], "api");
        assert_eq!(doc["attributes"]["http.route"], "/users");
        assert_eq!(messages[0].timestamp, Some(100));
    }

    #[test]
    fn given_span_without_status_should_report_unset() {
        let span = Span {
            name: "work".to_owned(),
            start_time_unix_nano: 1,
            ..Default::default()
        };

        let messages = export_traces_to_messages(traces_request(None, vec![span]));

        assert_eq!(payload_of(&messages[0])["status"], "unset");
    }

    #[test]
    fn given_zero_span_start_time_should_leave_message_timestamp_unset() {
        let span = Span {
            name: "work".to_owned(),
            start_time_unix_nano: 0,
            ..Default::default()
        };

        let messages = export_traces_to_messages(traces_request(None, vec![span]));

        assert_eq!(messages[0].timestamp, None);
    }

    #[test]
    fn given_gauge_metric_should_produce_data_point_message() {
        let metric = Metric {
            name: "cpu.usage".to_owned(),
            unit: "1".to_owned(),
            data: Some(metric::Data::Gauge(Gauge {
                data_points: vec![NumberDataPoint {
                    time_unix_nano: 42,
                    value: Some(number_data_point::Value::AsDouble(0.75)),
                    attributes: vec![attribute("cpu", string_value("0"))],
                    ..Default::default()
                }],
            })),
            ..Default::default()
        };

        let messages = export_metrics_to_messages(metrics_request(
            Some(service_resource("api")),
            vec![metric],
        ));

        assert_eq!(messages.len(), 1);
        let doc = payload_of(&messages[0]);
        assert_eq!(doc["signal"], "metric");
        assert_eq!(doc["name"], "cpu.usage");
        assert_eq!(doc["type"], "gauge");
        assert_eq!(doc["unit"], "1");
        assert_eq!(doc["value"], 0.75);
        assert_eq!(doc["service_name"], "api");
        assert_eq!(doc["resource"]["service.name"], "api");
        assert_eq!(doc["attributes"]["cpu"], "0");
        assert_eq!(messages[0].timestamp, Some(42));
    }

    #[test]
    fn given_sum_metric_should_include_is_monotonic() {
        let metric = Metric {
            name: "requests.total".to_owned(),
            data: Some(metric::Data::Sum(Sum {
                data_points: vec![NumberDataPoint {
                    time_unix_nano: 7,
                    value: Some(number_data_point::Value::AsInt(12)),
                    ..Default::default()
                }],
                is_monotonic: true,
                ..Default::default()
            })),
            ..Default::default()
        };

        let messages = export_metrics_to_messages(metrics_request(None, vec![metric]));

        let doc = payload_of(&messages[0]);
        assert_eq!(doc["type"], "sum");
        assert_eq!(doc["value"], 12);
        assert_eq!(doc["is_monotonic"], true);
    }

    #[test]
    fn given_histogram_metric_should_produce_count_and_sum() {
        let metric = Metric {
            name: "latency".to_owned(),
            data: Some(metric::Data::Histogram(Histogram {
                data_points: vec![HistogramDataPoint {
                    time_unix_nano: 9,
                    count: 3,
                    sum: Some(1.5),
                    ..Default::default()
                }],
                ..Default::default()
            })),
            ..Default::default()
        };

        let messages = export_metrics_to_messages(metrics_request(None, vec![metric]));

        let doc = payload_of(&messages[0]);
        assert_eq!(doc["type"], "histogram");
        assert_eq!(doc["value"]["count"], 3);
        assert_eq!(doc["value"]["sum"], 1.5);
    }

    #[test]
    fn given_metric_without_data_should_produce_no_messages() {
        let metric = Metric {
            name: "empty".to_owned(),
            data: None,
            ..Default::default()
        };

        let messages = export_metrics_to_messages(metrics_request(None, vec![metric]));

        assert!(messages.is_empty());
    }

    #[test]
    fn given_bytes_should_convert_to_lowercase_hex() {
        assert_eq!(bytes_to_hex(&[0x00, 0x0f, 0xff]), "000fff");
        assert_eq!(bytes_to_hex(&[]), "");
    }

    #[test]
    fn given_nested_any_value_should_convert_to_json() {
        let nested = AnyValue {
            value: Some(any_value::Value::KvlistValue(KeyValueList {
                values: vec![attribute("inner", string_value("value"))],
            })),
        };
        let array = AnyValue {
            value: Some(any_value::Value::ArrayValue(ArrayValue {
                values: vec![string_value("a"), string_value("b")],
            })),
        };

        assert_eq!(any_value_to_json(&nested)["inner"], "value");
        assert_eq!(any_value_to_json(&array), json!(["a", "b"]));
    }

    #[test]
    fn given_bytes_any_value_should_convert_to_hex_string() {
        let bytes = AnyValue {
            value: Some(any_value::Value::BytesValue(vec![0xa0, 0x0b])),
        };

        assert_eq!(any_value_to_json(&bytes), "a00b");
    }

    #[test]
    fn given_unrecognized_any_value_variant_should_map_to_null() {
        let unknown = AnyValue {
            value: Some(any_value::Value::StringValueStrindex(3)),
        };

        assert_eq!(any_value_to_json(&unknown), Value::Null);
        assert_eq!(
            extract_attrs(&[attribute("k", unknown)])["k"],
            Value::Null,
            "unknown variants are dropped to null rather than silently skipped"
        );
    }

    #[test]
    fn given_missing_resource_should_extract_empty_attributes() {
        assert!(extract_resource_attrs(None).is_empty());
    }

    #[test]
    fn given_severity_number_should_map_to_text() {
        assert_eq!(severity_number_to_text(1), "TRACE");
        assert_eq!(severity_number_to_text(9), "INFO");
        assert_eq!(severity_number_to_text(17), "ERROR");
        assert_eq!(severity_number_to_text(24), "FATAL");
        assert_eq!(severity_number_to_text(0), "UNSPECIFIED");
        assert_eq!(severity_number_to_text(99), "UNSPECIFIED");
    }

    #[test]
    fn given_span_kind_should_map_to_text() {
        assert_eq!(span_kind_to_text(1), "internal");
        assert_eq!(span_kind_to_text(5), "consumer");
        assert_eq!(span_kind_to_text(0), "unspecified");
    }

    #[test]
    fn given_status_code_should_map_to_text() {
        assert_eq!(status_code_to_text(1), "ok");
        assert_eq!(status_code_to_text(2), "error");
        assert_eq!(status_code_to_text(0), "unset");
    }
}
