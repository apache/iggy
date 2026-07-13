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

use crate::OutputFormat;
use chrono::{DateTime, Utc};
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Payload, TopicMetadata, owned_value_to_serde_json,
};
use serde::Serialize;
use serde_json::Value;

#[derive(Serialize)]
struct JsonMessage<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    offset: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    timestamp: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    stream: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    topic: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    partition_id: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    headers: Option<Value>,
    payload: Value,
}

pub(crate) fn format_message(
    message: &ConsumedMessage,
    topic_metadata: &TopicMetadata,
    messages_metadata: &MessagesMetadata,
    include_metadata: bool,
    include_headers: bool,
    format: OutputFormat,
) -> Result<Vec<u8>, Error> {
    match format {
        OutputFormat::JsonLines | OutputFormat::JsonArray => format_json_message(
            message,
            topic_metadata,
            messages_metadata,
            include_metadata,
            include_headers,
        ),
        OutputFormat::Raw => format_raw_message(message),
    }
}

fn format_json_message(
    message: &ConsumedMessage,
    topic_metadata: &TopicMetadata,
    messages_metadata: &MessagesMetadata,
    include_metadata: bool,
    include_headers: bool,
) -> Result<Vec<u8>, Error> {
    let ts_str = if include_metadata {
        Some(timestamp_to_rfc3339(message.timestamp))
    } else {
        None
    };
    let msg = JsonMessage {
        offset: if include_metadata {
            Some(message.offset)
        } else {
            None
        },
        timestamp: ts_str.as_deref(),
        stream: if include_metadata {
            Some(&topic_metadata.stream)
        } else {
            None
        },
        topic: if include_metadata {
            Some(&topic_metadata.topic)
        } else {
            None
        },
        partition_id: if include_metadata {
            Some(messages_metadata.partition_id)
        } else {
            None
        },
        headers: if include_headers {
            message.headers.as_ref().map(serialize_headers)
        } else {
            None
        },
        payload: payload_to_json_value(&message.payload)?,
    };

    serde_json::to_vec(&msg).map_err(|e| {
        Error::CannotStoreData(format!(
            "Failed to serialize message at offset {}: {e}",
            message.offset
        ))
    })
}

fn format_raw_message(message: &ConsumedMessage) -> Result<Vec<u8>, Error> {
    message.payload.try_to_bytes().map_err(|e| {
        Error::CannotStoreData(format!(
            "Failed to extract raw bytes at offset {}: {e}",
            message.offset
        ))
    })
}

fn serialize_headers(
    headers: &std::collections::BTreeMap<iggy_common::HeaderKey, iggy_common::HeaderValue>,
) -> Value {
    use iggy_common::HeaderKind;
    use serde_json::Map;

    let mut obj = Map::new();
    for (key, value) in headers {
        let key_str = key.as_str().unwrap_or("").to_string();
        let json_value = match value.kind() {
            HeaderKind::String => {
                Value::String(String::from_utf8_lossy(&value.value()).into_owned())
            }
            HeaderKind::Raw => Value::String(base64_encode(&value.value())),
            HeaderKind::Bool => {
                let b = !value.value().is_empty() && value.value()[0] != 0;
                Value::Bool(b)
            }
            HeaderKind::Int8 | HeaderKind::Int16 | HeaderKind::Int32 | HeaderKind::Int64 => {
                let s = value.to_string_value();
                s.parse::<i64>()
                    .map(|n| Value::Number(n.into()))
                    .unwrap_or(Value::String(s))
            }
            HeaderKind::Uint8 | HeaderKind::Uint16 | HeaderKind::Uint32 | HeaderKind::Uint64 => {
                let s = value.to_string_value();
                s.parse::<u64>()
                    .map(|n| Value::Number(n.into()))
                    .unwrap_or(Value::String(s))
            }
            HeaderKind::Float32 | HeaderKind::Float64 => {
                let s = value.to_string_value();
                s.parse::<f64>()
                    .ok()
                    .and_then(serde_json::Number::from_f64)
                    .map(Value::Number)
                    .unwrap_or(Value::String(s))
            }
            _ => Value::String(value.to_string_value()),
        };
        obj.insert(key_str, json_value);
    }
    Value::Object(obj)
}

fn payload_to_json_value(payload: &Payload) -> Result<Value, Error> {
    let value = match payload {
        Payload::Json(value) => owned_value_to_serde_json(value),
        Payload::Text(text) => Value::String(text.clone()),
        Payload::Raw(bytes) => match serde_json::from_slice(bytes) {
            Ok(v) => v,
            Err(_) => Value::String(base64_encode(bytes)),
        },
        Payload::Proto(text) => Value::String(text.clone()),
        Payload::FlatBuffer(bytes) => Value::String(base64_encode(bytes)),
        Payload::Avro(bytes) => Value::String(base64_encode(bytes)),
        Payload::Bson(value) => serde_json::to_value(value).map_err(|error| {
            Error::CannotStoreData(format!("Failed to serialize BSON payload: {error}"))
        })?,
    };
    Ok(value)
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

/// Finalize buffer entries into the output byte format.
pub(crate) fn finalize_buffer<'a>(
    entries: impl Iterator<Item = &'a [u8]>,
    format: OutputFormat,
) -> Vec<u8> {
    match format {
        OutputFormat::JsonLines => {
            let mut result = Vec::new();
            for entry in entries {
                result.extend_from_slice(entry);
                result.push(b'\n');
            }
            result
        }
        OutputFormat::Raw => {
            let mut result = Vec::new();
            for entry in entries {
                result.extend_from_slice(entry);
            }
            result
        }
        OutputFormat::JsonArray => {
            let mut result = Vec::new();
            result.push(b'[');
            let mut first = true;
            for entry in entries {
                if !first {
                    result.push(b',');
                }
                result.extend_from_slice(entry);
                first = false;
            }
            result.push(b']');
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

        let bytes =
            format_message(&msg, &topic, &meta, true, false, OutputFormat::JsonLines).unwrap();
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

        let bytes =
            format_message(&msg, &topic, &meta, false, false, OutputFormat::JsonLines).unwrap();
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

        let bytes =
            format_message(&msg, &topic, &meta, false, true, OutputFormat::JsonLines).unwrap();
        let value: Value = serde_json::from_slice(&bytes).unwrap();

        assert!(value["headers"].is_object());
        assert_eq!(value["headers"]["content-type"], "application/json");
    }

    #[test]
    fn raw_format() {
        let payload = Payload::Text("hello world".to_string());
        let msg = make_message(1, payload);
        let topic = make_topic_metadata();
        let meta = make_messages_metadata();

        let bytes = format_message(&msg, &topic, &meta, true, false, OutputFormat::Raw).unwrap();
        assert_eq!(bytes, b"hello world");
    }

    #[test]
    fn finalize_json_lines() {
        let data = b"{\"a\":1}{\"b\":2}";
        let boundaries = [7usize, 14];
        let entries = entries_from_boundaries(data, &boundaries);
        let result = finalize_buffer(entries, OutputFormat::JsonLines);
        assert_eq!(result, b"{\"a\":1}\n{\"b\":2}\n");
    }

    #[test]
    fn finalize_raw_no_delimiter() {
        let data = b"\x00\x01\x02\x0a\xff\xfe";
        let boundaries = [4usize, 6];
        let entries = entries_from_boundaries(data, &boundaries);
        let result = finalize_buffer(entries, OutputFormat::Raw);
        assert_eq!(
            result, data,
            "Raw must concatenate without inserting delimiters"
        );
    }

    #[test]
    fn finalize_json_array() {
        let data = b"{\"a\":1}{\"b\":2}";
        let boundaries = [7usize, 14];
        let entries = entries_from_boundaries(data, &boundaries);
        let result = finalize_buffer(entries, OutputFormat::JsonArray);
        assert_eq!(result, b"[{\"a\":1},{\"b\":2}]");
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

    fn entries_from_boundaries<'a>(
        data: &'a [u8],
        boundaries: &'a [usize],
    ) -> impl Iterator<Item = &'a [u8]> {
        let mut start = 0;
        boundaries.iter().map(move |&end| {
            let s = &data[start..end];
            start = end;
            s
        })
    }
}
