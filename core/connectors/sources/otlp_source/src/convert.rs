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
use serde_json::{Map, Value, json};
use std::fmt::Write as _;
use tracing::warn;

pub fn export_logs_to_messages(req: ExportLogsServiceRequest) -> Vec<ProducedMessage> {
    let mut messages = Vec::new();
    for resource_logs in req.resource_logs {
        let resource_attrs = extract_resource_attrs(resource_logs.resource.as_ref());
        let service_name = resource_attrs
            .get("service.name")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();

        let resource_value = Value::Object(resource_attrs);
        for scope_logs in resource_logs.scope_logs {
            for record in scope_logs.log_records {
                let body = record.body.as_ref().map(any_value_to_json);
                let mut doc = json!({
                    "signal": "log",
                    "timestamp_ns": record.time_unix_nano,
                    "observed_timestamp_ns": record.observed_time_unix_nano,
                    "severity": severity_number_to_text(record.severity_number),
                    "severity_text": record.severity_text,
                    "body": body,
                    "trace_id": bytes_to_hex(&record.trace_id),
                    "span_id": bytes_to_hex(&record.span_id),
                    "service_name": service_name,
                    "resource": resource_value.clone(),
                    "attributes": extract_attrs(&record.attributes),
                });
                if let Some(obj) = doc.as_object_mut() {
                    obj.retain(|_, v| !v.is_null() && v != &Value::String(String::new()));
                }
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

        let resource_value = Value::Object(resource_attrs);
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

                let mut doc = json!({
                    "signal": "trace",
                    "trace_id": bytes_to_hex(&span.trace_id),
                    "span_id": bytes_to_hex(&span.span_id),
                    "parent_span_id": bytes_to_hex(&span.parent_span_id),
                    "name": span.name,
                    "kind": span_kind_to_text(span.kind),
                    "start_time_ns": span.start_time_unix_nano,
                    "end_time_ns": span.end_time_unix_nano,
                    "status": status_code,
                    "status_message": status_message,
                    "service_name": service_name,
                    "resource": resource_value.clone(),
                    "attributes": extract_attrs(&span.attributes),
                });
                if let Some(obj) = doc.as_object_mut() {
                    obj.retain(|_, v| !v.is_null() && v != &Value::String(String::new()));
                }
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

    let base = |time_ns: u64, value: Value, attrs: &[KeyValue], metric_type: &str| -> Value {
        json!({
            "signal": "metric",
            "name": metric.name,
            "type": metric_type,
            "unit": metric.unit,
            "timestamp_ns": time_ns,
            "value": value,
            "service_name": service_name,
            "resource": resource_attrs,
            "attributes": extract_attrs(attrs),
        })
    };

    match &metric.data {
        Some(metric::Data::Gauge(gauge)) => {
            for dp in &gauge.data_points {
                let value = number_dp_value(&dp.value);
                let doc = base(dp.time_unix_nano, value, &dp.attributes, "gauge");
                push_metric_doc(doc, dp.time_unix_nano, &mut messages);
            }
        }
        Some(metric::Data::Sum(sum)) => {
            for dp in &sum.data_points {
                let value = number_dp_value(&dp.value);
                let mut doc = base(dp.time_unix_nano, value, &dp.attributes, "sum");
                if let Some(obj) = doc.as_object_mut() {
                    obj.insert("is_monotonic".into(), Value::Bool(sum.is_monotonic));
                }
                push_metric_doc(doc, dp.time_unix_nano, &mut messages);
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
                push_metric_doc(doc, dp.time_unix_nano, &mut messages);
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
                push_metric_doc(doc, dp.time_unix_nano, &mut messages);
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
                push_metric_doc(doc, dp.time_unix_nano, &mut messages);
            }
        }
        None => {}
    }

    messages
}

fn push_metric_doc(doc: Value, time_ns: u64, messages: &mut Vec<ProducedMessage>) {
    match serde_json::to_vec(&doc) {
        Ok(payload) => messages.push(ProducedMessage {
            id: None,
            checksum: None,
            timestamp: (time_ns != 0).then_some(time_ns),
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
