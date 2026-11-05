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
use std::io::Write;

use base64::{Engine, engine::general_purpose::STANDARD};

use iggy_common::{HeaderKey, HeaderValue};
use iggy_connector_sdk::{Error, ProducedMessage};
use lapin::{
    message::Delivery,
    types::{AMQPValue, FieldTable},
};
use serde_json::{Map, Value, json};
use tracing::warn;

pub fn encode_delivery(delivery: &Delivery) -> Result<Vec<u8>, serde_json::Error> {
    let mut properties = Map::new();
    for (name, value) in [
        ("message_id", delivery.properties.message_id()),
        ("content_type", delivery.properties.content_type()),
        ("content_encoding", delivery.properties.content_encoding()),
        ("correlation_id", delivery.properties.correlation_id()),
        ("reply_to", delivery.properties.reply_to()),
        ("expiration", delivery.properties.expiration()),
        ("type", delivery.properties.kind()),
        ("user_id", delivery.properties.user_id()),
        ("app_id", delivery.properties.app_id()),
        ("cluster_id", delivery.properties.cluster_id()),
    ] {
        if let Some(value) = value {
            properties.insert(name.to_owned(), Value::String(value.as_str().to_owned()));
        }
    }
    for (name, value) in [
        ("delivery_mode", delivery.properties.delivery_mode()),
        ("priority", delivery.properties.priority()),
    ] {
        if let Some(value) = value {
            properties.insert(name.to_owned(), json!(value));
        }
    }
    if let Some(timestamp) = delivery.properties.timestamp() {
        properties.insert("timestamp".to_owned(), Value::String(timestamp.to_string()));
    }
    let headers = delivery
        .properties
        .headers()
        .as_ref()
        .map(encode_table)
        .unwrap_or_else(|| Value::Object(Map::new()));
    let mut output = Vec::new();
    output.extend_from_slice(b"{\"body_base64\":\"");
    {
        let mut encoder = base64::write::EncoderWriter::new(&mut output, &STANDARD);
        encoder
            .write_all(&delivery.data)
            .map_err(serde_json::Error::io)?;
        encoder.finish().map_err(serde_json::Error::io)?;
    }
    output.extend_from_slice(b"\",\"delivery\":");
    serde_json::to_writer(
        &mut output,
        &json!({
            "exchange": delivery.exchange.as_str(),
            "routing_key": delivery.routing_key.as_str(),
            "redelivered": delivery.redelivered,
        }),
    )?;
    output.extend_from_slice(b",\"headers\":");
    serde_json::to_writer(&mut output, &headers)?;
    output.extend_from_slice(b",\"properties\":");
    serde_json::to_writer(&mut output, &properties)?;
    output.extend_from_slice(b",\"version\":1}");
    Ok(output)
}

pub fn encode_table(table: &FieldTable) -> Value {
    let fields: Map<String, Value> = table
        .inner()
        .iter()
        .map(|(key, value)| (key.as_str().to_owned(), encode_value(value)))
        .collect();
    Value::Object(fields)
}

pub fn encode_value(value: &AMQPValue) -> Value {
    match value {
        AMQPValue::Boolean(value) => json!({"type": "boolean", "value": value}),
        AMQPValue::ShortShortInt(value) => {
            json!({"type": "short_short_int", "value": value.to_string()})
        }
        AMQPValue::ShortShortUInt(value) => {
            json!({"type": "short_short_uint", "value": value.to_string()})
        }
        AMQPValue::ShortInt(value) => json!({"type": "short_int", "value": value.to_string()}),
        AMQPValue::ShortUInt(value) => json!({"type": "short_uint", "value": value.to_string()}),
        AMQPValue::LongInt(value) => json!({"type": "long_int", "value": value.to_string()}),
        AMQPValue::LongUInt(value) => json!({"type": "long_uint", "value": value.to_string()}),
        AMQPValue::LongLongInt(value) => {
            json!({"type": "long_long_int", "value": value.to_string()})
        }
        AMQPValue::Float(value) => {
            json!({"type": "float", "value": if value.is_finite() { value.to_string() } else { format_float(*value as f64) }})
        }
        AMQPValue::Double(value) => json!({"type": "double", "value": format_float(*value)}),
        AMQPValue::DecimalValue(value) => {
            json!({"type": "decimal_value", "value": {"scale": value.scale, "value": value.value.to_string()}})
        }
        AMQPValue::ShortString(value) => json!({"type": "short_string", "value": value.as_str()}),
        AMQPValue::LongString(value) => match std::str::from_utf8(value.as_bytes()) {
            Ok(text) => json!({"type": "long_string", "value": text}),
            Err(_) => {
                json!({"type": "long_string", "value_base64": STANDARD.encode(value.as_bytes())})
            }
        },
        AMQPValue::FieldArray(values) => {
            json!({"type": "field_array", "value": values.as_slice().iter().map(encode_value).collect::<Vec<_>>()})
        }
        AMQPValue::Timestamp(value) => json!({"type": "timestamp", "value": value.to_string()}),
        AMQPValue::FieldTable(value) => {
            json!({"type": "field_table", "value": encode_table(value)})
        }
        AMQPValue::ByteArray(value) => {
            json!({"type": "byte_array", "value_base64": STANDARD.encode(value.as_slice())})
        }
        AMQPValue::Void => json!({"type": "void", "value": null}),
    }
}

fn format_float(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_owned()
    } else if value == f64::INFINITY {
        "Infinity".to_owned()
    } else if value == f64::NEG_INFINITY {
        "-Infinity".to_owned()
    } else {
        value.to_string()
    }
}

const MAX_PAYLOAD_BYTES: usize = 64_000_000;

pub fn prepare_delivery(
    delivery: &mut Delivery,
    include_metadata: bool,
    connector_id: u32,
) -> Result<Option<ProducedMessage>, Error> {
    if unsupported_body(delivery.data.len(), include_metadata) {
        return Ok(None);
    }
    let (payload, headers) = if include_metadata {
        let payload =
            encode_delivery(delivery).map_err(|error| Error::Serialization(error.to_string()))?;
        if exceeds_payload_limit(payload.len()) {
            return Ok(None);
        }
        (payload, None)
    } else {
        let headers = raw_headers(delivery, connector_id)?;
        (std::mem::take(&mut delivery.data), Some(headers))
    };
    Ok(Some(ProducedMessage {
        id: None,
        checksum: None,
        timestamp: None,
        origin_timestamp: None,
        headers,
        payload,
    }))
}

fn unsupported_body(size: usize, include_metadata: bool) -> bool {
    exceeds_payload_limit(size) || (!include_metadata && size == 0)
}

fn exceeds_payload_limit(size: usize) -> bool {
    size > MAX_PAYLOAD_BYTES
}

fn raw_headers(
    delivery: &Delivery,
    connector_id: u32,
) -> Result<BTreeMap<HeaderKey, HeaderValue>, Error> {
    let mut headers = BTreeMap::new();
    for (key, value) in [
        ("amqp.exchange", delivery.exchange.as_str()),
        ("amqp.routing_key", delivery.routing_key.as_str()),
    ] {
        if !value.is_empty() {
            insert_string(&mut headers, key, value)?;
        }
    }
    let key = HeaderKey::try_from("amqp.redelivered")
        .map_err(|error| Error::Serialization(error.to_string()))?;
    headers.insert(key, HeaderValue::from(delivery.redelivered));
    if let Some(value) = delivery.properties.message_id() {
        let key = "amqp.message_id";
        let value = value.as_str();
        if value.is_empty() || value.len() > 255 {
            warn!(
                connector_id,
                header = key,
                "RabbitMQ source omitted a property from Iggy headers"
            );
        } else {
            insert_string(&mut headers, key, value)?;
        }
    }
    Ok(headers)
}

fn insert_string(
    headers: &mut BTreeMap<HeaderKey, HeaderValue>,
    key: &str,
    value: &str,
) -> Result<(), Error> {
    let header_key =
        HeaderKey::try_from(key).map_err(|error| Error::Serialization(error.to_string()))?;
    let header_value =
        HeaderValue::try_from(value).map_err(|error| Error::Serialization(error.to_string()))?;
    headers.insert(header_key, header_value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{MAX_PAYLOAD_BYTES, exceeds_payload_limit, prepare_delivery, unsupported_body};
    use iggy_common::HeaderKey;
    use lapin::{BasicProperties, message::Delivery};

    #[test]
    fn given_raw_delivery_should_preserve_body_and_publisher_message_id() {
        let mut delivery =
            Delivery::mock(1, "exchange".into(), "route".into(), false, vec![0, 255]);
        delivery.properties = BasicProperties::default()
            .with_message_id("a4ed331e-91e8-4c25-9fb3-0b8e3e41a8a2".into())
            .with_content_type("application/json".into())
            .with_content_encoding("gzip".into())
            .with_correlation_id("correlation-1".into())
            .with_reply_to("reply-queue".into());
        let body_ptr = delivery.data.as_ptr();
        let message = prepare_delivery(&mut delivery, false, 1)
            .expect("conversion must succeed")
            .expect("delivery must be supported");
        assert_eq!(message.payload, vec![0, 255]);
        assert_eq!(message.payload.as_ptr(), body_ptr);
        assert!(delivery.data.is_empty());
        assert!(message.id.is_none());
        let headers = message.headers.expect("raw headers must exist");
        let key = HeaderKey::try_from("amqp.exchange").expect("key must be valid");
        assert_eq!(
            headers
                .get(&key)
                .expect("exchange must exist")
                .as_str()
                .expect("value must be a string"),
            "exchange"
        );
        for required in ["amqp.routing_key", "amqp.redelivered"] {
            assert!(
                headers.contains_key(
                    &HeaderKey::try_from(required).expect("header key must be valid")
                ),
                "raw mode must include {required}"
            );
        }
        assert!(headers.contains_key(
            &HeaderKey::try_from("amqp.message_id").expect("header key must be valid")
        ));
        for omitted in [
            "amqp.content_type",
            "amqp.content_encoding",
            "amqp.correlation_id",
            "amqp.reply_to",
        ] {
            assert!(
                !headers
                    .contains_key(&HeaderKey::try_from(omitted).expect("header key must be valid")),
                "raw mode must omit {omitted}"
            );
        }
        assert_eq!(headers.len(), 4);
    }

    #[test]
    fn given_empty_body_should_follow_the_selected_mode() {
        let mut delivery = Delivery::mock(1, "".into(), "".into(), false, Vec::new());
        delivery.properties = BasicProperties::default()
            .with_message_id("00000000-0000-0000-0000-000000000000".into());
        assert!(
            prepare_delivery(&mut delivery, false, 1)
                .expect("conversion must succeed")
                .is_none()
        );
        let message = prepare_delivery(&mut delivery, true, 1)
            .expect("conversion must succeed")
            .expect("envelope must be supported");
        assert!(message.id.is_none());
    }

    #[test]
    fn given_body_size_boundaries_should_apply_the_payload_limit() {
        assert!(!unsupported_body(MAX_PAYLOAD_BYTES, false));
        assert!(!unsupported_body(MAX_PAYLOAD_BYTES, true));
        assert!(unsupported_body(MAX_PAYLOAD_BYTES + 1, false));
        assert!(unsupported_body(MAX_PAYLOAD_BYTES + 1, true));
        assert!(unsupported_body(0, false));
        assert!(!unsupported_body(0, true));
        assert!(!exceeds_payload_limit(MAX_PAYLOAD_BYTES));
        assert!(exceeds_payload_limit(MAX_PAYLOAD_BYTES + 1));
    }

    #[test]
    fn given_message_ids_should_leave_iggy_id_unset_in_both_modes() {
        let valid = "a4ed331e-91e8-4c25-9fb3-0b8e3e41a8a2";
        for id in [
            Some(valid),
            Some("00000000-0000-0000-0000-000000000000"),
            Some("not-a-uuid"),
            None,
        ] {
            for include_metadata in [false, true] {
                let mut delivery = Delivery::mock(1, "".into(), "route".into(), false, vec![1]);
                if let Some(id) = id {
                    delivery.properties = BasicProperties::default().with_message_id(id.into());
                }
                let message = prepare_delivery(&mut delivery, include_metadata, 1)
                    .expect("conversion must succeed")
                    .expect("delivery must be supported");
                assert!(message.id.is_none());
                if include_metadata {
                    let envelope: serde_json::Value = serde_json::from_slice(&message.payload)
                        .expect("metadata payload must be valid JSON");
                    assert_eq!(
                        envelope["properties"]["message_id"].as_str(),
                        id,
                        "metadata mode must preserve the publisher message ID"
                    );
                } else {
                    let headers = message.headers.expect("raw headers must exist");
                    let key = HeaderKey::try_from("amqp.message_id")
                        .expect("message ID header key must be valid");
                    assert_eq!(
                        headers
                            .get(&key)
                            .map(|value| value.as_str().expect("header value must be a string")),
                        id,
                        "raw mode must preserve the publisher message ID"
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod envelope_tests {
    use super::{encode_delivery, encode_value};
    use lapin::{
        BasicProperties,
        message::Delivery,
        types::{AMQPValue, DecimalValue, FieldTable},
    };
    use serde_json::json;

    #[test]
    fn given_binary_delivery_should_encode_the_envelope() {
        let mut headers = FieldTable::default();
        headers.insert(
            "amqp.exchange".into(),
            AMQPValue::LongString(vec![255].into()),
        );
        let mut delivery = Delivery::mock(1, "".into(), "events".into(), false, vec![0, 255]);
        delivery.properties = BasicProperties::default()
            .with_message_id("id-1".into())
            .with_headers(headers);
        let encoded = encode_delivery(&delivery).expect("envelope must serialize");
        let actual: serde_json::Value =
            serde_json::from_slice(&encoded).expect("envelope must be JSON");
        assert_eq!(
            actual,
            json!({
                "version": 1,
                "body_base64": "AP8=",
                "properties": {"message_id": "id-1"},
                "headers": {"amqp.exchange": {"type": "long_string", "value_base64": "/w=="}},
                "delivery": {"exchange": "", "routing_key": "events", "redelivered": false},
            })
        );
    }

    #[test]
    fn given_all_basic_properties_should_preserve_them_in_the_envelope() {
        let mut delivery = Delivery::mock(1, "".into(), "events".into(), false, vec![]);
        delivery.properties = BasicProperties::default()
            .with_content_type("application/json".into())
            .with_content_encoding("utf-8".into())
            .with_delivery_mode(2)
            .with_priority(0)
            .with_correlation_id("correlation".into())
            .with_reply_to("replies".into())
            .with_expiration("1000".into())
            .with_message_id("id-1".into())
            .with_timestamp(u64::MAX)
            .with_type("order.created".into())
            .with_user_id("user".into())
            .with_app_id("app".into())
            .with_cluster_id("cluster".into());
        let encoded = encode_delivery(&delivery).expect("envelope must serialize");
        let actual: serde_json::Value =
            serde_json::from_slice(&encoded).expect("envelope must be JSON");
        assert_eq!(
            actual["properties"],
            json!({
                "content_type": "application/json", "content_encoding": "utf-8",
                "delivery_mode": 2, "priority": 0, "correlation_id": "correlation",
                "reply_to": "replies", "expiration": "1000", "message_id": "id-1",
                "timestamp": "18446744073709551615", "type": "order.created",
                "user_id": "user", "app_id": "app", "cluster_id": "cluster"
            })
        );
        delivery.properties = BasicProperties::default();
        let encoded = encode_delivery(&delivery).expect("envelope must serialize");
        let actual: serde_json::Value =
            serde_json::from_slice(&encoded).expect("envelope must be JSON");
        assert_eq!(actual["properties"], json!({}));
    }

    #[test]
    fn given_amqp_values_should_match_version_one_vectors() {
        let mut table = FieldTable::default();
        table.insert("enabled".into(), AMQPValue::Boolean(true));
        let vectors = vec![
            (
                AMQPValue::Boolean(true),
                json!({"type":"boolean","value":true}),
            ),
            (
                AMQPValue::ShortShortInt(-1),
                json!({"type":"short_short_int","value":"-1"}),
            ),
            (
                AMQPValue::ShortShortUInt(1),
                json!({"type":"short_short_uint","value":"1"}),
            ),
            (
                AMQPValue::ShortInt(-2),
                json!({"type":"short_int","value":"-2"}),
            ),
            (
                AMQPValue::ShortUInt(2),
                json!({"type":"short_uint","value":"2"}),
            ),
            (
                AMQPValue::LongInt(-3),
                json!({"type":"long_int","value":"-3"}),
            ),
            (
                AMQPValue::LongUInt(3),
                json!({"type":"long_uint","value":"3"}),
            ),
            (
                AMQPValue::LongLongInt(-4),
                json!({"type":"long_long_int","value":"-4"}),
            ),
            (AMQPValue::Float(1.5), json!({"type":"float","value":"1.5"})),
            (
                AMQPValue::Double(2.5),
                json!({"type":"double","value":"2.5"}),
            ),
            (
                AMQPValue::DecimalValue(DecimalValue {
                    scale: 2,
                    value: 1234,
                }),
                json!({"type":"decimal_value","value":{"scale":2,"value":"1234"}}),
            ),
            (
                AMQPValue::ShortString("short".into()),
                json!({"type":"short_string","value":"short"}),
            ),
            (
                AMQPValue::LongString("long".into()),
                json!({"type":"long_string","value":"long"}),
            ),
            (
                AMQPValue::LongString(vec![255].into()),
                json!({"type":"long_string","value_base64":"/w=="}),
            ),
            (
                AMQPValue::FieldArray(vec![AMQPValue::Boolean(true)].into()),
                json!({"type":"field_array","value":[{"type":"boolean","value":true}]}),
            ),
            (
                AMQPValue::Timestamp(1234567890),
                json!({"type":"timestamp","value":"1234567890"}),
            ),
            (
                AMQPValue::FieldTable(table),
                json!({"type":"field_table","value":{"enabled":{"type":"boolean","value":true}}}),
            ),
            (
                AMQPValue::ByteArray(vec![0, 255].into()),
                json!({"type":"byte_array","value_base64":"AP8="}),
            ),
            (AMQPValue::Void, json!({"type":"void","value":null})),
        ];
        for (input, expected) in vectors {
            assert_eq!(encode_value(&input), expected);
        }
    }

    #[test]
    fn given_non_finite_floats_should_use_documented_strings() {
        assert_eq!(
            encode_value(&AMQPValue::Float(f32::NAN)),
            json!({"type":"float","value":"NaN"})
        );
        assert_eq!(
            encode_value(&AMQPValue::Double(f64::INFINITY)),
            json!({"type":"double","value":"Infinity"})
        );
        assert_eq!(
            encode_value(&AMQPValue::Double(f64::NEG_INFINITY)),
            json!({"type":"double","value":"-Infinity"})
        );
    }
}
