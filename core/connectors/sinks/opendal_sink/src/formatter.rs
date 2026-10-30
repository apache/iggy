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

use std::{
    borrow::Cow,
    collections::{BTreeMap, HashSet},
    io::{self, Write},
};

use base64::{display::Base64Display, engine::general_purpose::STANDARD};
use chrono::{DateTime, Utc};
use iggy_common::{HeaderKey, HeaderKind, HeaderValue};
use iggy_connector_sdk::{ConsumedMessage, Error, MessagesMetadata, Payload, TopicMetadata};
use serde::{Serialize, Serializer, ser::SerializeMap};

use crate::config::OutputFormat;

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

    /// Returns the serialized object bytes and the number of input messages consumed.
    pub(crate) fn format_chunk(
        &self,
        messages: &[ConsumedMessage],
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        max_bytes: usize,
    ) -> Result<(Vec<u8>, usize), Error> {
        if messages.is_empty() {
            return Ok((Vec::new(), 0));
        }

        let mut output = BoundedWriter {
            bytes: Vec::with_capacity(self.chunk_capacity(messages, topic_metadata, max_bytes)),
            limit: if self.output_format == OutputFormat::JsonArray {
                max_bytes.saturating_sub(1)
            } else {
                max_bytes
            },
            exceeded: false,
        };
        if self.output_format == OutputFormat::JsonArray {
            output
                .write_all(b"[")
                .map_err(|_| oversized_message(&messages[0], max_bytes))?;
        }

        let mut consumed = 0;
        for message in messages {
            let checkpoint = output.bytes.len();
            let result = (|| {
                if self.output_format == OutputFormat::JsonArray && consumed > 0 {
                    output.write_all(b",").map_err(serde_json::Error::io)?;
                }

                match self.output_format {
                    OutputFormat::JsonLines | OutputFormat::JsonArray => {
                        self.write_json_message(
                            &mut output,
                            message,
                            topic_metadata,
                            messages_metadata,
                        )?;
                    }
                    OutputFormat::Raw => self.write_raw_payload(&mut output, message)?,
                }

                if self.output_format == OutputFormat::JsonLines {
                    output.write_all(b"\n").map_err(serde_json::Error::io)?;
                }
                Ok::<(), serde_json::Error>(())
            })();

            if let Err(error) = result {
                output.bytes.truncate(checkpoint);
                if consumed > 0 {
                    break;
                }
                return Err(if output.exceeded {
                    oversized_message(message, max_bytes)
                } else {
                    Error::CannotStoreData(format!(
                        "Failed to serialize message at offset {}: {error}",
                        message.offset
                    ))
                });
            }
            consumed += 1;
        }

        if self.output_format == OutputFormat::JsonArray {
            output.bytes.push(b']');
        }
        Ok((output.bytes, consumed))
    }

    fn write_json_message(
        &self,
        output: &mut impl Write,
        message: &ConsumedMessage,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
    ) -> serde_json::Result<()> {
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

        serde_json::to_writer(output, &json_message)
    }

    fn write_raw_payload(
        &self,
        output: &mut impl Write,
        message: &ConsumedMessage,
    ) -> serde_json::Result<()> {
        match &message.payload {
            Payload::Json(value) => serde_json::to_writer(output, value),
            Payload::Proto(text) => match message.payload.json_document() {
                Some(value) => serde_json::to_writer(output, &value),
                None => output
                    .write_all(text.as_bytes())
                    .map_err(serde_json::Error::io),
            },
            Payload::Raw(bytes) | Payload::FlatBuffer(bytes) | Payload::Avro(bytes) => {
                output.write_all(bytes).map_err(serde_json::Error::io)
            }
            Payload::Text(text) => output
                .write_all(text.as_bytes())
                .map_err(serde_json::Error::io),
        }
    }

    fn chunk_capacity(
        &self,
        messages: &[ConsumedMessage],
        topic_metadata: &TopicMetadata,
        max_bytes: usize,
    ) -> usize {
        let metadata_size = if self.output_format == OutputFormat::Raw {
            0
        } else if self.include_metadata {
            topic_metadata
                .stream
                .len()
                .saturating_add(topic_metadata.topic.len())
                .saturating_add(128)
        } else {
            16
        };

        let mut capacity = 2usize;
        for message in messages {
            capacity = capacity
                .saturating_add(payload_size_hint(&message.payload))
                .saturating_add(metadata_size);
            if capacity >= max_bytes {
                return max_bytes;
            }
        }
        capacity.min(max_bytes)
    }
}

struct BoundedWriter {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}

impl Write for BoundedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(io::Error::other("Object size limit exceeded"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn oversized_message(message: &ConsumedMessage, max_bytes: usize) -> Error {
    Error::InvalidRecordValue(format!(
        "Message at offset {} exceeds max_file_size of {max_bytes} bytes",
        message.offset
    ))
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
        // String keys are already unique in the source map; only mixed kinds can collide.
        let mut names = self
            .0
            .keys()
            .any(|key| key.kind() != HeaderKind::String)
            .then(|| HashSet::with_capacity(self.0.len()));
        let mut map = serializer.serialize_map(Some(self.0.len()))?;
        for (key, value) in self.0 {
            let key = match key.kind() {
                HeaderKind::String => {
                    Cow::Borrowed(std::str::from_utf8(key.as_bytes()).map_err(|error| {
                        <S::Error as serde::ser::Error>::custom(format_args!(
                            "String header key is not valid UTF-8: {error}"
                        ))
                    })?)
                }
                _ => Cow::Owned(key.to_string_value()),
            };
            serialize_header(&mut map, key.as_ref(), value)?;
            // format_chunk rolls back this entire record if a duplicate is found.
            if let Some(names) = &mut names
                && let Some(duplicate) = names.replace(key)
            {
                return Err(serde::ser::Error::custom(format_args!(
                    "Duplicate JSON header key name {duplicate:?}"
                )));
            }
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
            Payload::Text(text) => serializer.serialize_str(text),
            Payload::Proto(text) => match self.0.json_document() {
                Some(value) => value.serialize(serializer),
                None => serializer.serialize_str(text),
            },
            Payload::Raw(bytes) | Payload::FlatBuffer(bytes) | Payload::Avro(bytes) => {
                Base64Bytes(bytes).serialize(serializer)
            }
        }
    }
}

struct Base64Bytes<'a>(&'a [u8]);

impl Serialize for Base64Bytes<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.collect_str(&Base64Display::new(self.0, &STANDARD))
    }
}

fn serialize_header<M>(map: &mut M, key: &str, value: &HeaderValue) -> Result<(), M::Error>
where
    M: SerializeMap,
{
    match value.kind() {
        HeaderKind::String => {
            map.serialize_entry(key, String::from_utf8_lossy(value.as_bytes()).as_ref())
        }
        HeaderKind::Raw => map.serialize_entry(key, &Base64Bytes(value.as_bytes())),
        HeaderKind::Bool => {
            let bytes = value.as_bytes();
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

fn timestamp_to_rfc3339(micros: u64) -> String {
    let seconds = (micros / 1_000_000) as i64;
    let nanoseconds = ((micros % 1_000_000) * 1_000) as u32;
    DateTime::<Utc>::from_timestamp(seconds, nanoseconds)
        .map(|timestamp| timestamp.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_else(|| "1970-01-01T00:00:00Z".to_string())
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use base64::Engine;
    use iggy_connector_sdk::Schema;

    use super::*;

    const MAX_CHUNK_BYTES: usize = 4096;

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

    fn test_metadata() -> (TopicMetadata, MessagesMetadata) {
        (
            TopicMetadata {
                stream: "events".to_string(),
                topic: "orders".to_string(),
            },
            MessagesMetadata {
                partition_id: 3,
                current_offset: 9,
                schema: Schema::Json,
            },
        )
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
        let (output, consumed) = formatter
            .format_chunk(
                &messages,
                &topic_metadata,
                &messages_metadata,
                MAX_CHUNK_BYTES,
            )
            .expect("payloads should serialize");

        assert_eq!(consumed, messages.len());
        assert_eq!(
            output,
            br#"[{"payload":{"id":1}},{"payload":"eyJyYXciOnRydWV9"},{"payload":"/w=="},{"payload":"text"},{"payload":"proto"},{"payload":"AQI="},{"payload":"AwQ="}]"#
        );
    }

    #[test]
    fn given_pretty_and_compact_raw_json_when_formatting_should_preserve_distinct_bytes() {
        let payloads = [
            b"{\n  \"value\": 1\n}\n".as_slice(),
            br#"{"value":1}"#.as_slice(),
        ];
        let messages = payloads
            .iter()
            .enumerate()
            .map(|(offset, payload)| test_message(offset as u64, Payload::Raw(payload.to_vec())))
            .collect::<Vec<_>>();
        let (topic_metadata, messages_metadata) = test_metadata();

        for output_format in [OutputFormat::JsonLines, OutputFormat::JsonArray] {
            let (output, consumed) = Formatter::new(output_format, false, false)
                .format_chunk(
                    &messages,
                    &topic_metadata,
                    &messages_metadata,
                    MAX_CHUNK_BYTES,
                )
                .expect("raw payloads should serialize");
            let records = match output_format {
                OutputFormat::JsonLines => std::str::from_utf8(&output)
                    .expect("JSON Lines should be UTF-8")
                    .lines()
                    .map(|line| serde_json::from_str::<serde_json::Value>(line).unwrap())
                    .collect::<Vec<_>>(),
                OutputFormat::JsonArray => serde_json::from_slice(&output).unwrap(),
                OutputFormat::Raw => unreachable!(),
            };

            assert_eq!(consumed, 2);
            assert_eq!(records.len(), 2);
            for (record, expected) in records.iter().zip(payloads) {
                let encoded = record["payload"]
                    .as_str()
                    .expect("raw payloads should be base64 strings");
                assert_eq!(STANDARD.decode(encoded).unwrap(), expected);
            }
            assert_ne!(records[0]["payload"], records[1]["payload"]);
        }
    }

    #[test]
    fn given_proto_json_and_text_when_formatting_should_use_structured_json_or_text() {
        let messages = [
            test_message(
                0,
                Payload::Proto("{\n  \"value\": [true, null]\n}".to_string()),
            ),
            test_message(1, Payload::Proto("not JSON\ntext".to_string())),
        ];
        let (topic_metadata, messages_metadata) = test_metadata();
        let cases = [
            (
                OutputFormat::JsonLines,
                b"{\"payload\":{\"value\":[true,null]}}\n{\"payload\":\"not JSON\\ntext\"}\n"
                    .as_slice(),
            ),
            (
                OutputFormat::JsonArray,
                br#"[{"payload":{"value":[true,null]}},{"payload":"not JSON\ntext"}]"#.as_slice(),
            ),
            (
                OutputFormat::Raw,
                b"{\"value\":[true,null]}not JSON\ntext".as_slice(),
            ),
        ];

        for (output_format, expected) in cases {
            let (output, consumed) = Formatter::new(output_format, false, false)
                .format_chunk(
                    &messages,
                    &topic_metadata,
                    &messages_metadata,
                    MAX_CHUNK_BYTES,
                )
                .expect("Proto payloads should serialize");

            assert_eq!(consumed, 2);
            assert_eq!(output, expected);
        }
    }

    #[test]
    fn given_numeric_header_keys_when_formatting_should_preserve_both_keys() {
        let mut message = test_message(0, Payload::Text("text".to_string()));
        message.headers = Some(BTreeMap::from([
            (
                HeaderKey::from(7u32),
                HeaderValue::from_str("first").unwrap(),
            ),
            (
                HeaderKey::from(11u32),
                HeaderValue::from_str("second").unwrap(),
            ),
        ]));
        let (topic_metadata, messages_metadata) = test_metadata();
        let (output, consumed) = Formatter::new(OutputFormat::JsonArray, false, true)
            .format_chunk(
                &[message],
                &topic_metadata,
                &messages_metadata,
                MAX_CHUNK_BYTES,
            )
            .expect("numeric header keys should serialize");
        let records: serde_json::Value = serde_json::from_slice(&output).unwrap();

        assert_eq!(consumed, 1);
        assert_eq!(
            records[0]["headers"],
            serde_json::json!({"7": "first", "11": "second"})
        );
    }

    #[test]
    fn given_colliding_header_names_when_formatting_should_roll_back_the_invalid_record() {
        let (topic_metadata, messages_metadata) = test_metadata();
        let collisions = [
            (HeaderKey::from_str("1").unwrap(), HeaderKey::from(1i8)),
            (HeaderKey::from(1i8), HeaderKey::from(1u32)),
        ];

        for (first_key, second_key) in collisions {
            let mut first = test_message(7, Payload::Text("first".to_string()));
            first.headers = Some(BTreeMap::from([(
                HeaderKey::from_str("1").unwrap(),
                HeaderValue::from_str("before").unwrap(),
            )]));
            let mut invalid = test_message(8, Payload::Text("invalid".to_string()));
            invalid.headers = Some(BTreeMap::from([
                (first_key, HeaderValue::from_str("first collision").unwrap()),
                (
                    second_key,
                    HeaderValue::from_str("second collision").unwrap(),
                ),
            ]));
            let mut last = test_message(9, Payload::Text("last".to_string()));
            last.headers = Some(BTreeMap::from([(
                HeaderKey::from(1i8),
                HeaderValue::from_str("after").unwrap(),
            )]));
            let messages = [first, invalid, last];
            let cases = [
                (
                    OutputFormat::JsonLines,
                    b"{\"headers\":{\"1\":\"before\"},\"payload\":\"first\"}\n".as_slice(),
                    b"{\"headers\":{\"1\":\"after\"},\"payload\":\"last\"}\n".as_slice(),
                ),
                (
                    OutputFormat::JsonArray,
                    br#"[{"headers":{"1":"before"},"payload":"first"}]"#.as_slice(),
                    br#"[{"headers":{"1":"after"},"payload":"last"}]"#.as_slice(),
                ),
            ];

            for (output_format, expected_prefix, expected_suffix) in cases {
                let formatter = Formatter::new(output_format, false, true);
                let (output, consumed) = formatter
                    .format_chunk(
                        &messages,
                        &topic_metadata,
                        &messages_metadata,
                        MAX_CHUNK_BYTES,
                    )
                    .expect("the valid prefix should survive a later header collision");

                assert_eq!(consumed, 1);
                assert_eq!(output, expected_prefix);
                assert!(matches!(
                    formatter.format_chunk(
                        &messages[consumed..],
                        &topic_metadata,
                        &messages_metadata,
                        MAX_CHUNK_BYTES,
                    ),
                    Err(Error::CannotStoreData(_))
                ));

                let (output, consumed) = formatter
                    .format_chunk(
                        &messages[2..],
                        &topic_metadata,
                        &messages_metadata,
                        MAX_CHUNK_BYTES,
                    )
                    .expect("a bad header should not affect a later valid record");

                assert_eq!(consumed, 1);
                assert_eq!(output, expected_suffix);
            }
        }
    }

    #[test]
    fn given_string_and_mixed_header_keys_when_formatting_should_preserve_names_and_values() {
        let (topic_metadata, messages_metadata) = test_metadata();
        let cases = [
            (
                BTreeMap::from([
                    (
                        HeaderKey::from_str("ordinary").unwrap(),
                        HeaderValue::from_str("text").unwrap(),
                    ),
                    (
                        HeaderKey::from_str("quoted\"key").unwrap(),
                        HeaderValue::from(true),
                    ),
                    (
                        HeaderKey::from_str("path\\key").unwrap(),
                        HeaderValue::from(7i32),
                    ),
                    (
                        HeaderKey::from_str("unicode-\u{03BB}").unwrap(),
                        HeaderValue::from_str("value").unwrap(),
                    ),
                ]),
                serde_json::json!({
                    "ordinary": "text",
                    "quoted\"key": true,
                    "path\\key": 7,
                    "unicode-\u{03BB}": "value",
                }),
            ),
            (
                BTreeMap::from([
                    (
                        HeaderKey::from_str("source").unwrap(),
                        HeaderValue::from_str("checkout").unwrap(),
                    ),
                    (HeaderKey::from(-2i8), HeaderValue::from(42i32)),
                    (HeaderKey::from(11u32), HeaderValue::from(true)),
                    (HeaderKey::from(true), HeaderValue::from(2.5f64)),
                ]),
                serde_json::json!({
                    "source": "checkout",
                    "-2": 42,
                    "11": true,
                    "true": 2.5,
                }),
            ),
        ];

        for (headers, expected) in cases {
            let mut message = test_message(0, Payload::Text("text".to_string()));
            message.headers = Some(headers);

            for output_format in [OutputFormat::JsonLines, OutputFormat::JsonArray] {
                let (output, consumed) = Formatter::new(output_format, false, true)
                    .format_chunk(
                        std::slice::from_ref(&message),
                        &topic_metadata,
                        &messages_metadata,
                        MAX_CHUNK_BYTES,
                    )
                    .expect("distinct header names should serialize");
                let output: serde_json::Value = serde_json::from_slice(&output).unwrap();
                let record = match output_format {
                    OutputFormat::JsonLines => &output,
                    OutputFormat::JsonArray => &output[0],
                    OutputFormat::Raw => unreachable!(),
                };

                assert_eq!(consumed, 1);
                assert_eq!(record["headers"], expected);
            }
        }
    }

    #[test]
    fn given_invalid_utf8_header_key_when_formatting_should_reject_it() {
        let mut message = test_message(0, Payload::Text("text".to_string()));
        message.headers = Some(BTreeMap::from([
            (
                HeaderKey::from_raw(HeaderKind::String, &[0xFF]).unwrap(),
                HeaderValue::from_str("invalid UTF-8").unwrap(),
            ),
            (
                HeaderKey::from_str("\u{FFFD}").unwrap(),
                HeaderValue::from_str("valid replacement character").unwrap(),
            ),
        ]));
        let (topic_metadata, messages_metadata) = test_metadata();

        for output_format in [OutputFormat::JsonLines, OutputFormat::JsonArray] {
            assert!(matches!(
                Formatter::new(output_format, false, true).format_chunk(
                    std::slice::from_ref(&message),
                    &topic_metadata,
                    &messages_metadata,
                    MAX_CHUNK_BYTES,
                ),
                Err(Error::CannotStoreData(_))
            ));
        }
    }

    #[test]
    fn given_chunk_size_boundary_when_formatting_should_preserve_complete_messages() {
        let messages = [
            test_message(7, Payload::Text("one".to_string())),
            test_message(8, Payload::Text("two".to_string())),
            test_message(9, Payload::Text("three".to_string())),
        ];
        let (topic_metadata, messages_metadata) = test_metadata();
        let cases = [
            (
                OutputFormat::JsonLines,
                b"{\"payload\":\"one\"}\n{\"payload\":\"two\"}\n".as_slice(),
                b"{\"payload\":\"three\"}\n".as_slice(),
            ),
            (
                OutputFormat::JsonArray,
                br#"[{"payload":"one"},{"payload":"two"}]"#.as_slice(),
                br#"[{"payload":"three"}]"#.as_slice(),
            ),
            (OutputFormat::Raw, b"onetwo".as_slice(), b"three".as_slice()),
        ];

        for (output_format, expected_first, expected_last) in cases {
            let formatter = Formatter::new(output_format, false, false);
            for max_bytes in [expected_first.len(), expected_first.len() + 1] {
                let (first, consumed) = formatter
                    .format_chunk(&messages, &topic_metadata, &messages_metadata, max_bytes)
                    .expect("the first two messages should fit");
                let (last, remaining) = formatter
                    .format_chunk(
                        &messages[consumed..],
                        &topic_metadata,
                        &messages_metadata,
                        max_bytes,
                    )
                    .expect("the final message should fit");

                assert_eq!(consumed, 2);
                assert_eq!(first, expected_first);
                assert_eq!(remaining, 1);
                assert_eq!(last, expected_last);
            }
        }
    }

    #[test]
    fn given_metadata_and_headers_when_formatting_should_include_them_in_size_limit() {
        let mut message = test_message(7, Payload::Text("one".to_string()));
        message.headers = Some(BTreeMap::from([(
            HeaderKey::from_str("key").unwrap(),
            HeaderValue::from_str("value").unwrap(),
        )]));
        let mut second = test_message(8, Payload::Text("two".to_string()));
        second.headers = message.headers.clone();
        let messages = [message, second];
        let expected = br#"[{"offset":7,"timestamp":"1970-01-01T00:00:00Z","stream":"events","topic":"orders","partition_id":3,"headers":{"key":"value"},"payload":"one"}]"#;
        let (topic_metadata, messages_metadata) = test_metadata();
        let formatter = Formatter::new(OutputFormat::JsonArray, true, true);

        let (output, consumed) = formatter
            .format_chunk(
                &messages,
                &topic_metadata,
                &messages_metadata,
                expected.len(),
            )
            .expect("one complete message should fit at the exact boundary");

        assert_eq!(consumed, 1);
        assert_eq!(output, expected);
        assert!(matches!(
            formatter.format_chunk(
                &messages,
                &topic_metadata,
                &messages_metadata,
                expected.len() - 1,
            ),
            Err(Error::InvalidRecordValue(_))
        ));
    }

    #[test]
    fn given_oversized_first_message_when_formatting_should_reject_it() {
        let (topic_metadata, messages_metadata) = test_metadata();
        let cases = [
            (OutputFormat::JsonLines, b"{\"payload\":\"one\"}\n".len()),
            (OutputFormat::JsonArray, br#"[{"payload":"one"}]"#.len()),
            (OutputFormat::Raw, b"one".len()),
        ];

        for (output_format, message_size) in cases {
            let formatter = Formatter::new(output_format, false, false);
            let message = test_message(7, Payload::Text("one".to_string()));
            for max_bytes in [0, 1, message_size - 1] {
                assert!(matches!(
                    formatter.format_chunk(
                        std::slice::from_ref(&message),
                        &topic_metadata,
                        &messages_metadata,
                        max_bytes,
                    ),
                    Err(Error::InvalidRecordValue(_))
                ));
            }
        }
    }

    #[test]
    fn given_large_binary_message_when_formatting_should_return_the_valid_prefix() {
        let (topic_metadata, messages_metadata) = test_metadata();
        let messages = [
            test_message(7, Payload::Text("first".to_string())),
            test_message(8, Payload::Raw(vec![0xFF; 1024 * 1024])),
        ];

        for output_format in [
            OutputFormat::JsonLines,
            OutputFormat::JsonArray,
            OutputFormat::Raw,
        ] {
            let formatter = Formatter::new(output_format, false, false);
            let (output, consumed) = formatter
                .format_chunk(&messages, &topic_metadata, &messages_metadata, 128)
                .expect("an oversized later message should not discard the valid prefix");

            let expected = match output_format {
                OutputFormat::JsonLines => b"{\"payload\":\"first\"}\n".as_slice(),
                OutputFormat::JsonArray => br#"[{"payload":"first"}]"#.as_slice(),
                OutputFormat::Raw => b"first".as_slice(),
            };
            assert_eq!(consumed, 1);
            assert_eq!(output, expected);
            assert!(matches!(
                formatter.format_chunk(
                    &messages[consumed..],
                    &topic_metadata,
                    &messages_metadata,
                    128,
                ),
                Err(Error::InvalidRecordValue(_))
            ));
        }
    }

    #[test]
    fn given_base64_size_boundary_when_formatting_should_preserve_complete_payloads() {
        let messages = [
            test_message(7, Payload::Raw(vec![0, 1])),
            test_message(8, Payload::Raw(vec![2, 3, 4])),
        ];
        let (topic_metadata, messages_metadata) = test_metadata();
        let cases = [
            (
                OutputFormat::JsonLines,
                b"{\"payload\":\"AAE=\"}\n".as_slice(),
                b"{\"payload\":\"AgME\"}\n".as_slice(),
            ),
            (
                OutputFormat::JsonArray,
                br#"[{"payload":"AAE="}]"#.as_slice(),
                br#"[{"payload":"AgME"}]"#.as_slice(),
            ),
        ];

        for (output_format, expected_first, expected_last) in cases {
            let formatter = Formatter::new(output_format, false, false);
            let max_bytes = expected_first.len();
            let (first, consumed) = formatter
                .format_chunk(&messages, &topic_metadata, &messages_metadata, max_bytes)
                .expect("the first base64 payload should fit exactly");
            let (last, remaining) = formatter
                .format_chunk(
                    &messages[consumed..],
                    &topic_metadata,
                    &messages_metadata,
                    max_bytes,
                )
                .expect("the remaining base64 payload should fit exactly");

            assert_eq!(consumed, 1);
            assert_eq!(first, expected_first);
            assert_eq!(remaining, 1);
            assert_eq!(last, expected_last);
            assert!(matches!(
                formatter.format_chunk(
                    &messages,
                    &topic_metadata,
                    &messages_metadata,
                    max_bytes - 1,
                ),
                Err(Error::InvalidRecordValue(_))
            ));
        }
    }
}
