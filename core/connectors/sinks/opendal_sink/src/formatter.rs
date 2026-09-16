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

use base64::Engine;
use chrono::{DateTime, Utc};
use iggy_common::HeaderKind;
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Payload, TopicMetadata, owned_value_to_serde_json,
};
use serde::Serialize;
use serde_json::{Map, Value};

use crate::OutputFormat;

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
        OutputFormat::JsonLines | OutputFormat::JsonArray => format_json(
            message,
            topic_metadata,
            messages_metadata,
            include_metadata,
            include_headers,
            format,
        ),
        OutputFormat::Raw => message.payload.try_to_bytes().map_err(|error| {
            Error::CannotStoreData(format!(
                "Failed to extract raw bytes at offset {}: {error}",
                message.offset
            ))
        }),
    }
}

fn format_json(
    message: &ConsumedMessage,
    topic_metadata: &TopicMetadata,
    messages_metadata: &MessagesMetadata,
    include_metadata: bool,
    include_headers: bool,
    format: OutputFormat,
) -> Result<Vec<u8>, Error> {
    let timestamp = if include_metadata {
        Some(timestamp_to_rfc3339(message.timestamp))
    } else {
        None
    };

    let json_message = JsonMessage {
        offset: if include_metadata {
            Some(message.offset)
        } else {
            None
        },
        timestamp: timestamp.as_deref(),
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
        payload: payload_to_json_value(&message.payload),
    };

    let mut data = serde_json::to_vec(&json_message).map_err(|error| {
        Error::CannotStoreData(format!(
            "Failed to serialize message at offset {}: {error}",
            message.offset
        ))
    })?;

    if format == OutputFormat::JsonLines {
        data.push(b'\n');
    }

    Ok(data)
}

fn serialize_headers(
    headers: &std::collections::BTreeMap<iggy_common::HeaderKey, iggy_common::HeaderValue>,
) -> Value {
    let mut values = Map::new();
    for (key, value) in headers {
        let key = key.as_str().unwrap_or("").to_string();
        let value = match value.kind() {
            HeaderKind::String => {
                Value::String(String::from_utf8_lossy(&value.value()).into_owned())
            }
            HeaderKind::Raw => Value::String(base64_encode(&value.value())),
            HeaderKind::Bool => Value::Bool(!value.value().is_empty() && value.value()[0] != 0),
            HeaderKind::Int8 | HeaderKind::Int16 | HeaderKind::Int32 | HeaderKind::Int64 => {
                let value = value.to_string_value();
                value
                    .parse::<i64>()
                    .map(|number| Value::Number(number.into()))
                    .unwrap_or(Value::String(value))
            }
            HeaderKind::Uint8 | HeaderKind::Uint16 | HeaderKind::Uint32 | HeaderKind::Uint64 => {
                let value = value.to_string_value();
                value
                    .parse::<u64>()
                    .map(|number| Value::Number(number.into()))
                    .unwrap_or(Value::String(value))
            }
            HeaderKind::Float32 | HeaderKind::Float64 => {
                let value = value.to_string_value();
                value
                    .parse::<f64>()
                    .ok()
                    .and_then(serde_json::Number::from_f64)
                    .map(Value::Number)
                    .unwrap_or(Value::String(value))
            }
            _ => Value::String(value.to_string_value()),
        };
        values.insert(key, value);
    }
    Value::Object(values)
}

fn payload_to_json_value(payload: &Payload) -> Value {
    match payload {
        Payload::Json(value) => owned_value_to_serde_json(value),
        Payload::Text(text) | Payload::Proto(text) => Value::String(text.clone()),
        Payload::Raw(bytes) => {
            serde_json::from_slice(bytes).unwrap_or_else(|_| Value::String(base64_encode(bytes)))
        }
        Payload::FlatBuffer(bytes) | Payload::Avro(bytes) => Value::String(base64_encode(bytes)),
    }
}

fn base64_encode(bytes: &[u8]) -> String {
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

fn timestamp_to_rfc3339(micros: u64) -> String {
    let seconds = (micros / 1_000_000) as i64;
    let nanoseconds = ((micros % 1_000_000) * 1_000) as u32;
    DateTime::<Utc>::from_timestamp(seconds, nanoseconds)
        .map(|timestamp| timestamp.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string())
}

pub(crate) fn finalize_buffer<'a>(
    entries: impl Iterator<Item = &'a [u8]>,
    format: OutputFormat,
) -> Vec<u8> {
    let mut output = Vec::new();
    if format == OutputFormat::JsonArray {
        output.push(b'[');
    }

    for (index, entry) in entries.enumerate() {
        if format == OutputFormat::JsonArray && index > 0 {
            output.push(b',');
        }
        output.extend_from_slice(entry);
    }

    if format == OutputFormat::JsonArray {
        output.push(b']');
    }
    output
}
