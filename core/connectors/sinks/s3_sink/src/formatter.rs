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

use crate::OutputFormat;
use chrono::{DateTime, Utc};
use iggy_connector_sdk::{ConsumedMessage, MessagesMetadata, Payload, TopicMetadata};
use serde_json::{Map, Value};

pub fn format_message(
    message: &ConsumedMessage,
    topic_metadata: &TopicMetadata,
    messages_metadata: &MessagesMetadata,
    include_metadata: bool,
    include_headers: bool,
    format: OutputFormat,
) -> Vec<u8> {
    match format {
        OutputFormat::JsonLines | OutputFormat::JsonArray => {
            format_json_message(
                message,
                topic_metadata,
                messages_metadata,
                include_metadata,
                include_headers,
            )
        }
        OutputFormat::Raw => format_raw_message(message),
    }
}

fn format_json_message(
    message: &ConsumedMessage,
    topic_metadata: &TopicMetadata,
    messages_metadata: &MessagesMetadata,
    include_metadata: bool,
    include_headers: bool,
) -> Vec<u8> {
    let mut obj = Map::new();

    if include_metadata {
        obj.insert("offset".to_string(), Value::Number(message.offset.into()));
        let ts = timestamp_to_rfc3339(message.timestamp);
        obj.insert("timestamp".to_string(), Value::String(ts));
        obj.insert(
            "stream".to_string(),
            Value::String(topic_metadata.stream.clone()),
        );
        obj.insert(
            "topic".to_string(),
            Value::String(topic_metadata.topic.clone()),
        );
        obj.insert(
            "partition_id".to_string(),
            Value::Number(messages_metadata.partition_id.into()),
        );
    }

    if include_headers
        && let Some(headers) = &message.headers
    {
        let mut headers_obj = Map::new();
        for (key, value) in headers {
            headers_obj.insert(key.to_string(), Value::String(value.to_string()));
        }
        obj.insert("headers".to_string(), Value::Object(headers_obj));
    }

    let payload_value = payload_to_json_value(&message.payload);
    obj.insert("payload".to_string(), payload_value);

    serde_json::to_vec(&Value::Object(obj)).unwrap_or_default()
}

fn format_raw_message(message: &ConsumedMessage) -> Vec<u8> {
    message
        .payload
        .try_to_bytes()
        .unwrap_or_default()
}

fn payload_to_json_value(payload: &Payload) -> Value {
    match payload {
        Payload::Json(value) => {
            let bytes = simd_json::to_vec(value).unwrap_or_default();
            serde_json::from_slice(&bytes).unwrap_or(Value::Null)
        }
        Payload::Text(text) => Value::String(text.clone()),
        Payload::Raw(bytes) => {
            match serde_json::from_slice(bytes) {
                Ok(v) => v,
                Err(_) => Value::String(base64_encode(bytes)),
            }
        }
        Payload::Proto(text) => Value::String(text.clone()),
        Payload::FlatBuffer(bytes) => Value::String(base64_encode(bytes)),
    }
}

fn base64_encode(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn timestamp_to_rfc3339(micros: u64) -> String {
    let secs = (micros / 1_000_000) as i64;
    let nanos = ((micros % 1_000_000) * 1_000) as u32;
    DateTime::<Utc>::from_timestamp(secs, nanos)
        .map(|dt| dt.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string())
}

pub fn finalize_buffer(data: &[Vec<u8>], format: OutputFormat) -> Vec<u8> {
    match format {
        OutputFormat::JsonLines => {
            let mut result = Vec::new();
            for entry in data {
                result.extend_from_slice(entry);
                result.push(b'\n');
            }
            result
        }
        OutputFormat::JsonArray => {
            let entries: Vec<Value> = data
                .iter()
                .filter_map(|bytes| serde_json::from_slice(bytes).ok())
                .collect();
            serde_json::to_vec(&entries).unwrap_or_default()
        }
        OutputFormat::Raw => {
            let mut result = Vec::new();
            for entry in data {
                result.extend_from_slice(entry);
                result.push(b'\n');
            }
            result
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_connector_sdk::Schema;
    use std::collections::BTreeMap;

    fn make_json_payload(json_str: &str) -> Payload {
        let mut bytes = json_str.as_bytes().to_vec();
        let value = simd_json::to_owned_value(&mut bytes).unwrap();
        Payload::Json(value)
    }

    fn make_message(offset: u64, payload: Payload) -> ConsumedMessage {
        ConsumedMessage {
            id: 1,
            offset,
            checksum: 12345,
            timestamp: 1_710_597_751_000_000,
            origin_timestamp: 1_710_597_751_000_000,
            headers: None,
            payload,
        }
    }

    fn make_topic_metadata() -> TopicMetadata {
        TopicMetadata {
            stream: "app_logs".to_string(),
            topic: "api_requests".to_string(),
        }
    }

    fn make_messages_metadata() -> MessagesMetadata {
        MessagesMetadata {
            partition_id: 1,
            current_offset: 42,
            schema: Schema::Json,
        }
    }

    #[test]
    fn json_lines_with_metadata() {
        let payload = make_json_payload(r#"{"method":"GET","status":200}"#);
        let msg = make_message(42, payload);
        let topic = make_topic_metadata();
        let meta = make_messages_metadata();

        let bytes = format_message(&msg, &topic, &meta, true, false, OutputFormat::JsonLines);
        let value: Value = serde_json::from_slice(&bytes).unwrap();

        assert_eq!(value["offset"], 42);
        assert_eq!(value["stream"], "app_logs");
        assert_eq!(value["topic"], "api_requests");
        assert_eq!(value["partition_id"], 1);
        assert_eq!(value["payload"]["method"], "GET");
        assert!(value["timestamp"].is_string());
    }

    #[test]
    fn json_lines_without_metadata() {
        let payload = make_json_payload(r#"{"key":"value"}"#);
        let msg = make_message(10, payload);
        let topic = make_topic_metadata();
        let meta = make_messages_metadata();

        let bytes = format_message(&msg, &topic, &meta, false, false, OutputFormat::JsonLines);
        let value: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(value.get("offset").is_none());
        assert!(value.get("stream").is_none());
        assert_eq!(value["payload"]["key"], "value");
    }

    #[test]
    fn json_lines_with_headers() {
        let payload = make_json_payload(r#"{"data":1}"#);
        let mut msg = make_message(5, payload);

        let mut headers = BTreeMap::new();
        let key = iggy_common::HeaderKey::try_from("content-type").unwrap();
        let value = iggy_common::HeaderValue::try_from("application/json").unwrap();
        headers.insert(key, value);
        msg.headers = Some(headers);

        let topic = make_topic_metadata();
        let meta = make_messages_metadata();

        let bytes = format_message(&msg, &topic, &meta, false, true, OutputFormat::JsonLines);
        let value: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(value["headers"].is_object());
    }

    #[test]
    fn raw_format() {
        let payload = Payload::Text("hello world".to_string());
        let msg = make_message(1, payload);
        let topic = make_topic_metadata();
        let meta = make_messages_metadata();

        let bytes = format_message(&msg, &topic, &meta, true, false, OutputFormat::Raw);
        assert_eq!(bytes, b"hello world");
    }

    #[test]
    fn finalize_json_lines() {
        let entries = vec![
            b"{\"a\":1}".to_vec(),
            b"{\"b\":2}".to_vec(),
        ];
        let result = finalize_buffer(&entries, OutputFormat::JsonLines);
        assert_eq!(result, b"{\"a\":1}\n{\"b\":2}\n");
    }

    #[test]
    fn finalize_json_array() {
        let entries = vec![
            b"{\"a\":1}".to_vec(),
            b"{\"b\":2}".to_vec(),
        ];
        let result = finalize_buffer(&entries, OutputFormat::JsonArray);
        let value: Value = serde_json::from_slice(&result).unwrap();
        assert!(value.is_array());
        assert_eq!(value.as_array().unwrap().len(), 2);
    }

    #[test]
    fn timestamp_conversion() {
        let ts = timestamp_to_rfc3339(1_710_597_751_000_000);
        assert!(ts.starts_with("2024-03-16T"));
        assert!(ts.ends_with('Z'));
    }

    #[test]
    fn timestamp_zero() {
        let ts = timestamp_to_rfc3339(0);
        assert_eq!(ts, "1970-01-01T00:00:00Z");
    }
}
