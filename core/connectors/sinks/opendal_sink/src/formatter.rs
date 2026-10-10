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

use std::collections::BTreeMap;

use base64::Engine;
use chrono::{DateTime, Utc};
use iggy_common::{HeaderKey, HeaderKind, HeaderValue};
use iggy_connector_sdk::{ConsumedMessage, Error, MessagesMetadata, Payload, TopicMetadata};
use serde::{Serialize, Serializer, ser::SerializeMap};
use serde_json::value::RawValue;

use crate::OutputFormat;

#[derive(Debug, Clone, Copy)]
pub(crate) struct Formatter {
    output_format: OutputFormat,
    include_metadata: bool,
    include_headers: bool,
}

impl Formatter {
    pub(crate) const fn new(
        output_format: OutputFormat,
        include_metadata: bool,
        include_headers: bool,
    ) -> Self {
        Self {
            output_format,
            include_metadata,
            include_headers,
        }
    }

    pub(crate) const fn output_format(&self) -> OutputFormat {
        self.output_format
    }

    pub(crate) fn format_batch(
        &self,
        messages: &[ConsumedMessage],
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
    ) -> Result<Vec<u8>, Error> {
        let mut output = Vec::with_capacity(self.batch_capacity(messages, topic_metadata));
        if self.output_format == OutputFormat::JsonArray {
            output.push(b'[');
        }

        for (index, message) in messages.iter().enumerate() {
            if self.output_format == OutputFormat::JsonArray && index > 0 {
                output.push(b',');
            }

            match self.output_format {
                OutputFormat::JsonLines | OutputFormat::JsonArray => self.write_json_message(
                    &mut output,
                    message,
                    topic_metadata,
                    messages_metadata,
                )?,
                OutputFormat::Raw => self.write_raw_payload(&mut output, message)?,
            }

            if self.output_format == OutputFormat::JsonLines {
                output.push(b'\n');
            }
        }

        if self.output_format == OutputFormat::JsonArray {
            output.push(b']');
        }
        Ok(output)
    }

    fn write_json_message(
        &self,
        output: &mut Vec<u8>,
        message: &ConsumedMessage,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
    ) -> Result<(), Error> {
        let timestamp = self
            .include_metadata
            .then(|| timestamp_to_rfc3339(message.timestamp));
        let json_message = JsonMessage {
            offset: self.include_metadata.then_some(message.offset),
            timestamp: timestamp.as_deref(),
            stream: self
                .include_metadata
                .then_some(topic_metadata.stream.as_str()),
            topic: self
                .include_metadata
                .then_some(topic_metadata.topic.as_str()),
            partition_id: self
                .include_metadata
                .then_some(messages_metadata.partition_id),
            headers: if self.include_headers {
                message.headers.as_ref().map(JsonHeaders)
            } else {
                None
            },
            payload: JsonPayload(&message.payload),
        };

        serde_json::to_writer(output, &json_message).map_err(|error| {
            Error::CannotStoreData(format!(
                "Failed to serialize message at offset {}: {error}",
                message.offset
            ))
        })
    }

    fn write_raw_payload(
        &self,
        output: &mut Vec<u8>,
        message: &ConsumedMessage,
    ) -> Result<(), Error> {
        match &message.payload {
            Payload::Json(value) => serde_json::to_writer(output, value).map_err(|error| {
                Error::CannotStoreData(format!(
                    "Failed to serialize raw payload at offset {}: {error}",
                    message.offset
                ))
            }),
            Payload::Raw(bytes) | Payload::FlatBuffer(bytes) | Payload::Avro(bytes) => {
                output.extend_from_slice(bytes);
                Ok(())
            }
            Payload::Text(text) | Payload::Proto(text) => {
                output.extend_from_slice(text.as_bytes());
                Ok(())
            }
        }
    }

    fn batch_capacity(
        &self,
        messages: &[ConsumedMessage],
        topic_metadata: &TopicMetadata,
    ) -> usize {
        let metadata_size = if self.include_metadata {
            topic_metadata
                .stream
                .len()
                .saturating_add(topic_metadata.topic.len())
                .saturating_add(128)
        } else if self.output_format == OutputFormat::Raw {
            0
        } else {
            16
        };

        messages.iter().fold(2, |capacity, message| {
            capacity
                .saturating_add(payload_size_hint(&message.payload))
                .saturating_add(metadata_size)
        })
    }
}

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
    headers: Option<JsonHeaders<'a>>,
    payload: JsonPayload<'a>,
}

struct JsonHeaders<'a>(&'a BTreeMap<HeaderKey, HeaderValue>);

impl Serialize for JsonHeaders<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (key, value) in self.0 {
            serialize_header(&mut map, key.as_str().unwrap_or(""), value)?;
        }
        map.end()
    }
}

struct JsonPayload<'a>(&'a Payload);

impl Serialize for JsonPayload<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self.0 {
            Payload::Json(value) => value.serialize(serializer),
            Payload::Text(text) | Payload::Proto(text) => serializer.serialize_str(text),
            Payload::Raw(bytes) => match serde_json::from_slice::<&RawValue>(bytes) {
                Ok(value) => value.serialize(serializer),
                Err(_) => serializer.serialize_str(&base64_encode(bytes)),
            },
            Payload::FlatBuffer(bytes) | Payload::Avro(bytes) => {
                serializer.serialize_str(&base64_encode(bytes))
            }
        }
    }
}

fn serialize_header<M>(map: &mut M, key: &str, value: &HeaderValue) -> Result<(), M::Error>
where
    M: SerializeMap,
{
    match value.kind() {
        HeaderKind::String => {
            let bytes = value.value();
            map.serialize_entry(key, String::from_utf8_lossy(&bytes).as_ref())
        }
        HeaderKind::Raw => map.serialize_entry(key, &base64_encode(&value.value())),
        HeaderKind::Bool => {
            let bytes = value.value();
            map.serialize_entry(key, &(!bytes.is_empty() && bytes[0] != 0))
        }
        HeaderKind::Int8 | HeaderKind::Int16 | HeaderKind::Int32 | HeaderKind::Int64 => {
            let text = value.to_string_value();
            match text.parse::<i64>() {
                Ok(number) => map.serialize_entry(key, &number),
                Err(_) => map.serialize_entry(key, &text),
            }
        }
        HeaderKind::Uint8 | HeaderKind::Uint16 | HeaderKind::Uint32 | HeaderKind::Uint64 => {
            let text = value.to_string_value();
            match text.parse::<u64>() {
                Ok(number) => map.serialize_entry(key, &number),
                Err(_) => map.serialize_entry(key, &text),
            }
        }
        HeaderKind::Float32 | HeaderKind::Float64 => {
            let text = value.to_string_value();
            match text.parse::<f64>() {
                Ok(number) if number.is_finite() => map.serialize_entry(key, &number),
                _ => map.serialize_entry(key, &text),
            }
        }
        _ => map.serialize_entry(key, &value.to_string_value()),
    }
}

fn payload_size_hint(payload: &Payload) -> usize {
    match payload {
        Payload::Json(_) => 0,
        Payload::Raw(bytes) | Payload::FlatBuffer(bytes) | Payload::Avro(bytes) => bytes.len(),
        Payload::Text(text) | Payload::Proto(text) => text.len(),
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

#[cfg(test)]
mod tests {
    use iggy_connector_sdk::Schema;

    use super::*;

    fn test_message(offset: u64, payload: Payload) -> ConsumedMessage {
        ConsumedMessage {
            id: offset as u128,
            offset,
            checksum: 0,
            timestamp: 0,
            origin_timestamp: 0,
            headers: None,
            payload,
        }
    }

    #[test]
    fn given_all_payload_variants_when_formatting_json_should_preserve_values() {
        let mut json = br#"{"id":1}"#.to_vec();
        let json = simd_json::to_owned_value(&mut json).expect("JSON payload should be valid");
        let messages = vec![
            test_message(0, Payload::Json(json)),
            test_message(1, Payload::Raw(br#"{"raw":true}"#.to_vec())),
            test_message(2, Payload::Raw(vec![0xFF])),
            test_message(3, Payload::Text("text".to_string())),
            test_message(4, Payload::Proto("proto".to_string())),
            test_message(5, Payload::FlatBuffer(vec![1, 2])),
            test_message(6, Payload::Avro(vec![3, 4])),
        ];
        let topic_metadata = TopicMetadata {
            stream: "events".to_string(),
            topic: "orders".to_string(),
        };
        let messages_metadata = MessagesMetadata {
            partition_id: 0,
            current_offset: 6,
            schema: Schema::Json,
        };

        let formatter = Formatter::new(OutputFormat::JsonArray, false, false);
        let output = formatter
            .format_batch(&messages, &topic_metadata, &messages_metadata)
            .expect("payloads should serialize");

        assert_eq!(
            output,
            br#"[{"payload":{"id":1}},{"payload":{"raw":true}},{"payload":"/w=="},{"payload":"text"},{"payload":"proto"},{"payload":"AQI="},{"payload":"AwQ="}]"#
        );
    }
}
