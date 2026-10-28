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

use std::borrow::Cow;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::{DateTime, SecondsFormat, Utc};
use iggy_connector_sdk::{ConsumedMessage, Error, MessagesMetadata, Payload, TopicMetadata};
use scopedb_client::DataType;
use serde_json::{Map, Value, json};

use crate::schema::Layout;
use crate::{Settings, WriteMode};

pub(crate) fn row(
    message: &ConsumedMessage,
    topic: &TopicMetadata,
    metadata: &MessagesMetadata,
    layout: &Layout,
    settings: &Settings,
) -> Result<Value, Error> {
    let mut row = match settings.mode {
        WriteMode::Mapped => {
            let value = json_payload(&message.payload)?;
            let Value::Object(mut object) = value else {
                return Err(Error::InvalidRecordValue(
                    "mapped payload must be a JSON object".into(),
                ));
            };
            object.retain(|name, _| layout.writable.contains_key(name));
            object
        }
        WriteMode::Raw => {
            let value = match layout.raw_type {
                Some(DataType::Binary) => {
                    Value::String(BASE64.encode(payload_bytes(&message.payload)?.as_ref()))
                }
                Some(DataType::String) => Value::String(
                    String::from_utf8(payload_bytes(&message.payload)?.into_owned())
                        .map_err(|_| Error::InvalidTextPayload)?,
                ),
                Some(DataType::Object) => {
                    let value = raw_json(&message.payload)?;
                    if !value.is_object() {
                        return Err(Error::InvalidRecordValue(
                            "object payload_column requires a JSON object".into(),
                        ));
                    }
                    value
                }
                Some(DataType::Any) => raw_json(&message.payload)?,
                _ => {
                    return Err(Error::SchemaMismatch(
                        "raw layout is not initialized".into(),
                    ));
                }
            };
            let mut object = Map::new();
            object.insert(settings.payload_column.clone(), value);
            object
        }
    };
    if settings.include_metadata {
        let offset = i64::try_from(message.offset).map_err(|_| {
            Error::InvalidRecordValue("iggy_offset exceeds the NDJSON signed integer limit".into())
        })?;
        let micros = i64::try_from(message.timestamp)
            .map_err(|_| Error::InvalidRecordValue("iggy_timestamp is out of range".into()))?;
        let timestamp = DateTime::<Utc>::from_timestamp_micros(micros)
            .ok_or_else(|| Error::InvalidRecordValue("iggy_timestamp is out of range".into()))?;
        row.insert("iggy_stream".into(), Value::String(topic.stream.clone()));
        row.insert("iggy_topic".into(), Value::String(topic.topic.clone()));
        row.insert(
            "iggy_partition_id".into(),
            Value::from(metadata.partition_id),
        );
        row.insert("iggy_offset".into(), Value::from(offset));
        row.insert(
            "iggy_timestamp".into(),
            Value::String(timestamp.to_rfc3339_opts(SecondsFormat::Micros, true)),
        );
        row.insert("iggy_id".into(), Value::String(message.id.to_string()));
    }
    if settings.include_headers {
        let mut headers = Map::new();
        if let Some(values) = &message.headers {
            for (name, value) in values {
                let encoded = match value.as_raw() {
                    Ok(raw) => json!({"data":BASE64.encode(raw), "iggy_header_encoding":"base64"}),
                    Err(_) => Value::String(value.to_string_value()),
                };
                headers.insert(name.to_string_value(), encoded);
            }
        }
        row.insert("iggy_headers".into(), Value::Object(headers));
    }
    let row = Value::Object(row);
    validate_numbers(&row)?;
    let encoded =
        serde_json::to_vec(&row).map_err(|error| Error::Serialization(error.to_string()))?;
    if encoded.len() > settings.max_request_bytes {
        return Err(Error::InvalidRecordValue(
            "row exceeds max_request_bytes".into(),
        ));
    }
    Ok(row)
}

fn payload_bytes(payload: &Payload) -> Result<Cow<'_, [u8]>, Error> {
    match payload {
        Payload::Json(_) => payload.try_to_bytes().map(Cow::Owned),
        Payload::Raw(bytes) | Payload::FlatBuffer(bytes) | Payload::Avro(bytes) => {
            Ok(Cow::Borrowed(bytes))
        }
        Payload::Text(text) | Payload::Proto(text) => Ok(Cow::Borrowed(text.as_bytes())),
    }
}

fn json_payload(payload: &Payload) -> Result<Value, Error> {
    match payload {
        Payload::Json(_) => serde_json::from_slice(payload.try_to_bytes()?.as_ref())
            .map_err(|_| Error::InvalidJsonPayload),
        Payload::Text(text) | Payload::Proto(text) => {
            serde_json::from_str(text).map_err(|_| Error::InvalidJsonPayload)
        }
        _ => Err(Error::InvalidPayloadType),
    }
}

fn raw_json(payload: &Payload) -> Result<Value, Error> {
    match payload {
        Payload::Raw(bytes) => serde_json::from_slice(bytes).map_err(|_| Error::InvalidJsonPayload),
        _ => json_payload(payload),
    }
}

fn validate_numbers(value: &Value) -> Result<(), Error> {
    match value {
        Value::Number(number) if number.as_i64().is_none() => {
            let encoded = number.to_string();
            if !encoded.contains(['.', 'e', 'E']) || number.as_f64().is_none() {
                return Err(Error::InvalidRecordValue(
                    "number exceeds the NDJSON input range".into(),
                ));
            }
        }
        Value::Array(values) => {
            for value in values {
                validate_numbers(value)?;
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                validate_numbers(value)?;
            }
        }
        _ => {}
    }
    Ok(())
}
