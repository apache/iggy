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

use base64::{Engine, engine::general_purpose::STANDARD};
use bytes::Bytes;
use iggy_common::{HeaderKey, IggyTimestamp};
use iggy_connector_sdk::{ConsumedMessage, Error, Payload, TopicMetadata};
use rumqttc_v5::mqttbytes::v5::PublishProperties;
use serde::Deserialize;
use std::sync::Arc;

use crate::{MqttProtocol, driver};

/// Header used by the MQTT source to preserve the original broker topic.
const MQTT_TOPIC_HEADER: &str = "mqtt.topic";
pub const DEFAULT_TOPIC_TEMPLATE: &str = "{stream}/{topic}";
const MAX_MQTT_STRING_BYTES: usize = u16::MAX as usize;
const MAX_MQTT_PROPERTIES_BYTES: usize = 268_435_455;
const MICROS_PER_SECOND: u64 = 1_000_000;

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum TopicMode {
    #[default]
    Static,
    Template,
    Dynamic,
}

#[derive(Debug)]
pub struct PreparedPublication {
    pub offset: u64,
    pub topic: PublicationTopic,
    pub payload: Bytes,
    pub packet_size: usize,
    pub properties: Option<PublishProperties>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct PublicationOptions {
    pub(crate) protocol: MqttProtocol,
    pub(crate) qos: u8,
    pub(crate) retain: bool,
}

#[derive(Debug)]
pub enum PublicationTopic {
    Shared(Arc<String>),
    Owned(String),
}

impl PublicationTopic {
    pub fn as_str(&self) -> &str {
        match self {
            Self::Shared(topic) => topic,
            Self::Owned(topic) => topic,
        }
    }

    #[cfg(test)]
    pub fn as_shared(&self) -> Option<&Arc<String>> {
        match self {
            Self::Shared(topic) => Some(topic),
            Self::Owned(_) => None,
        }
    }
}

#[derive(Debug, Deserialize)]
struct MqttEnvelope {
    version: u8,
    topic: Option<String>,
    properties: Option<MqttEnvelopeProperties>,
    payload_base64: String,
}

#[derive(Debug, Deserialize)]
struct MqttEnvelopeProperties {
    payload_format_indicator: Option<u8>,
    message_expiry_interval: Option<u32>,
    response_topic: Option<String>,
    correlation_data_base64: Option<String>,
    #[serde(default)]
    user_properties: Vec<MqttUserProperty>,
    #[serde(default)]
    subscription_identifiers: Vec<usize>,
    content_type: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MqttUserProperty {
    key: String,
    value: String,
}

pub fn validate_topic(topic: &str, field: &str) -> Result<(), Error> {
    validate_publish_topic_value(topic, field).map_err(Error::InvalidConfigValue)
}

pub fn validate_topic_template(template: &str, field: &str) -> Result<(), Error> {
    if template.trim().is_empty() {
        return Err(Error::InvalidConfigValue(format!(
            "{field} must not be empty"
        )));
    }
    if template.starts_with('$') {
        return Err(Error::InvalidConfigValue(format!(
            "{field} must not start with '$'"
        )));
    }
    validate_mqtt_string(template, field).map_err(Error::InvalidConfigValue)?;
    if template
        .as_bytes()
        .iter()
        .any(|character| matches!(*character, b'+' | b'#'))
    {
        return Err(Error::InvalidConfigValue(format!(
            "{field} must not contain MQTT wildcards"
        )));
    }

    let mut remaining = template;
    while let Some(start) = remaining.find('{') {
        if remaining[..start].contains('}') {
            return Err(Error::InvalidConfigValue(format!(
                "{field} contains an unmatched closing brace"
            )));
        }
        let placeholder = &remaining[start + 1..];
        let Some(end) = placeholder.find('}') else {
            return Err(Error::InvalidConfigValue(format!(
                "{field} contains an unterminated placeholder"
            )));
        };
        match &placeholder[..end] {
            "stream" | "topic" => {}
            value => {
                return Err(Error::InvalidConfigValue(format!(
                    "{field} contains unsupported placeholder '{{{value}}}'"
                )));
            }
        }
        remaining = &placeholder[end + 1..];
    }
    if remaining.contains('}') {
        return Err(Error::InvalidConfigValue(format!(
            "{field} contains an unmatched closing brace"
        )));
    }
    Ok(())
}

fn validate_publish_topic_value(topic: &str, field: &str) -> Result<(), String> {
    validate_topic_value(topic, field)?;
    if topic.starts_with('$') {
        return Err(format!("{field} must not start with '$'"));
    }
    Ok(())
}

fn validate_topic_value(topic: &str, field: &str) -> Result<(), String> {
    if topic.trim().is_empty() {
        return Err(format!("{field} must not be empty"));
    }
    validate_mqtt_string(topic, field)?;
    if topic
        .as_bytes()
        .iter()
        .any(|character| matches!(*character, b'+' | b'#'))
    {
        return Err(format!("{field} must not contain MQTT wildcards"));
    }
    Ok(())
}

fn validate_mqtt_string(value: &str, field: &str) -> Result<(), String> {
    if value.len() > MAX_MQTT_STRING_BYTES {
        return Err(format!(
            "{field} exceeds the MQTT UTF-8 string limit of {MAX_MQTT_STRING_BYTES} bytes"
        ));
    }
    if value.contains('\0') {
        return Err(format!("{field} must not contain a null character"));
    }
    if value.chars().any(is_mqtt_noncharacter) {
        return Err(format!("{field} must not contain Unicode noncharacters"));
    }
    Ok(())
}

fn is_mqtt_noncharacter(character: char) -> bool {
    matches!(character as u32, 0xfdd0..=0xfdef)
        || matches!(character as u32, value if value & 0xffff == 0xfffe || value & 0xffff == 0xffff)
}

pub fn prepare_batch_topic(
    metadata: &TopicMetadata,
    mode: TopicMode,
    configured_topic: Option<&str>,
    topic_template: Option<&str>,
) -> Result<Option<Arc<String>>, Error> {
    let template = topic_template.unwrap_or(DEFAULT_TOPIC_TEMPLATE);
    let topic = match mode {
        TopicMode::Static => configured_topic
            .filter(|topic| !topic.trim().is_empty())
            .map(str::to_owned)
            .unwrap_or_else(|| render_template(template, metadata)),
        TopicMode::Template => render_template(template, metadata),
        TopicMode::Dynamic => return Ok(None),
    };
    validate_topic(&topic, "resolved topic")
        .map_err(|error| Error::InvalidRecordValue(error.to_string()))?;
    Ok(Some(Arc::new(topic)))
}

#[cfg(test)]
pub fn prepare_publication(
    message: ConsumedMessage,
    metadata: &TopicMetadata,
    mode: TopicMode,
    configured_topic: Option<&str>,
    topic_template: Option<&str>,
    fallback_topic: Option<&str>,
    protocol: MqttProtocol,
) -> Result<PreparedPublication, Error> {
    let batch_topic = prepare_batch_topic(metadata, mode, configured_topic, topic_template)?;
    prepare_publication_with_batch_topic(
        message,
        metadata,
        mode,
        batch_topic.as_ref(),
        topic_template,
        fallback_topic,
        PublicationOptions {
            protocol,
            qos: 1,
            retain: false,
        },
    )
}

pub fn prepare_publication_with_batch_topic(
    message: ConsumedMessage,
    metadata: &TopicMetadata,
    mode: TopicMode,
    batch_topic: Option<&Arc<String>>,
    topic_template: Option<&str>,
    fallback_topic: Option<&str>,
    options: PublicationOptions,
) -> Result<PreparedPublication, Error> {
    // Routing is explicit rather than a precedence chain: static and template
    // modes treat the Iggy message as opaque, while dynamic mode opts into
    // source-topic/header and MQTT 5 envelope interpretation.
    let template = topic_template.unwrap_or(DEFAULT_TOPIC_TEMPLATE);
    let fallback = fallback_topic.unwrap_or(template);
    let offset = message.offset;
    match mode {
        TopicMode::Static | TopicMode::Template => {
            let topic = batch_topic.ok_or_else(|| {
                Error::InvalidConfigValue("missing static or template batch topic".to_string())
            })?;
            let mut publication = PreparedPublication {
                offset,
                topic: PublicationTopic::Shared(Arc::clone(topic)),
                payload: message.payload.try_into_vec()?.into(),
                packet_size: 0,
                properties: None,
            };
            publication.packet_size = driver::publication_packet_size(
                &publication,
                options.protocol,
                options.qos,
                options.retain,
            );
            Ok(publication)
        }
        TopicMode::Dynamic => {
            let (topic, payload, properties) =
                prepare_dynamic(message, metadata, fallback, options.protocol)?;
            validate_topic(&topic, "resolved topic")
                .map_err(|error| Error::InvalidRecordValue(error.to_string()))?;
            let mut publication = PreparedPublication {
                offset,
                topic: PublicationTopic::Owned(topic),
                payload,
                packet_size: 0,
                properties,
            };
            publication.packet_size = driver::publication_packet_size(
                &publication,
                options.protocol,
                options.qos,
                options.retain,
            );
            Ok(publication)
        }
    }
}

fn prepare_dynamic(
    message: ConsumedMessage,
    metadata: &TopicMetadata,
    fallback: &str,
    protocol: MqttProtocol,
) -> Result<(String, Bytes, Option<PublishProperties>), Error> {
    // Raw messages can carry only the source topic header. JSON messages may
    // additionally contain the versioned MQTT 5 envelope used by mqtt_source.
    let ConsumedMessage {
        headers,
        timestamp,
        payload,
        ..
    } = message;
    match payload {
        payload @ Payload::Raw(_) => {
            let topic = headers
                .as_ref()
                .and_then(|headers| {
                    HeaderKey::try_from(MQTT_TOPIC_HEADER)
                        .ok()
                        .and_then(|key| headers.get(&key))
                })
                .map(|value| value.to_string_value())
                .filter(|topic| !topic.trim().is_empty())
                .unwrap_or_else(|| render_template(fallback, metadata));
            Ok((topic, payload.try_into_vec()?.into(), None))
        }
        Payload::Json(value)
            if matches!(
                &value,
                simd_json::OwnedValue::Object(object)
                    if object.contains_key("version") && object.contains_key("payload_base64")
            ) =>
        {
            let json_value = iggy_connector_sdk::convert::owned_value_into_serde_json(value);
            let envelope = serde_json::from_value::<MqttEnvelope>(json_value).map_err(|error| {
                Error::InvalidRecordValue(format!("invalid MQTT envelope: {error}"))
            })?;
            // A recognized envelope must not be silently downgraded to MQTT
            // 3.1.1, because that would discard its publish properties.
            if protocol == MqttProtocol::Mqtt311 {
                return Err(Error::InvalidRecordValue(
                    "MQTT envelopes require protocol = mqtt5".to_string(),
                ));
            }
            if envelope.version != 1 {
                return Err(Error::InvalidRecordValue(format!(
                    "unsupported MQTT envelope version: {}",
                    envelope.version
                )));
            }
            let topic = envelope
                .topic
                .filter(|topic| !topic.trim().is_empty())
                .unwrap_or_else(|| render_template(fallback, metadata));
            let payload =
                Bytes::from(STANDARD.decode(envelope.payload_base64).map_err(|error| {
                    Error::InvalidRecordValue(format!(
                        "invalid MQTT envelope payload_base64: {error}"
                    ))
                })?);
            let properties = envelope
                .properties
                .map(|properties| prepare_publish_properties(properties, timestamp))
                .transpose()?;
            Ok((topic, payload, properties))
        }
        payload => Ok((
            render_template(fallback, metadata),
            payload.try_into_vec()?.into(),
            None,
        )),
    }
}

fn prepare_publish_properties(
    properties: MqttEnvelopeProperties,
    message_timestamp: u64,
) -> Result<PublishProperties, Error> {
    if let Some(value) = properties.payload_format_indicator
        && !matches!(value, 0 | 1)
    {
        return Err(Error::InvalidRecordValue(
            "MQTT envelope payload_format_indicator must be 0 or 1".to_string(),
        ));
    }
    if !properties.subscription_identifiers.is_empty() {
        return Err(Error::InvalidRecordValue(
            "MQTT envelope subscription_identifiers are not valid for outbound publications"
                .to_string(),
        ));
    }
    if let Some(response_topic) = properties.response_topic.as_deref() {
        validate_topic_value(response_topic, "MQTT envelope response_topic")
            .map_err(Error::InvalidRecordValue)?;
    }
    if let Some(content_type) = properties.content_type.as_deref() {
        validate_mqtt_string(content_type, "MQTT envelope content_type")
            .map_err(Error::InvalidRecordValue)?;
    }
    for property in &properties.user_properties {
        validate_mqtt_string(&property.key, "MQTT envelope user property key")
            .map_err(Error::InvalidRecordValue)?;
        validate_mqtt_string(&property.value, "MQTT envelope user property value")
            .map_err(Error::InvalidRecordValue)?;
    }

    let correlation_data = properties
        .correlation_data_base64
        .as_deref()
        .map(|value| {
            STANDARD.decode(value).map_err(|error| {
                Error::InvalidRecordValue(format!(
                    "invalid MQTT envelope correlation_data_base64: {error}"
                ))
            })
        })
        .transpose()?;
    if correlation_data
        .as_ref()
        .is_some_and(|value| value.len() > MAX_MQTT_STRING_BYTES)
    {
        return Err(Error::InvalidRecordValue(format!(
            "MQTT envelope correlation_data exceeds the MQTT binary data limit of {MAX_MQTT_STRING_BYTES} bytes"
        )));
    }
    let property_length = mqtt_properties_length(&properties, correlation_data.as_deref());
    if property_length > MAX_MQTT_PROPERTIES_BYTES {
        return Err(Error::InvalidRecordValue(format!(
            "MQTT envelope properties exceed the MQTT limit of {MAX_MQTT_PROPERTIES_BYTES} bytes"
        )));
    }

    Ok(PublishProperties {
        payload_format_indicator: properties.payload_format_indicator,
        message_expiry_interval: match properties.message_expiry_interval {
            Some(message_expiry_interval) => remaining_message_expiry_interval(
                Some(message_expiry_interval),
                message_timestamp,
                IggyTimestamp::now().as_micros(),
            )?,
            None => None,
        },
        // Topic aliases are scoped to the MQTT connection that created them.
        // The sink creates a different connection, so it must publish the
        // complete topic instead of forwarding the old alias.
        topic_alias: None,
        response_topic: properties.response_topic,
        correlation_data: correlation_data.map(Into::into),
        user_properties: properties
            .user_properties
            .into_iter()
            .map(|property| (property.key, property.value))
            .collect(),
        subscription_identifiers: properties.subscription_identifiers,
        content_type: properties.content_type,
    })
}

fn remaining_message_expiry_interval(
    message_expiry_interval: Option<u32>,
    message_timestamp: u64,
    now_timestamp: u64,
) -> Result<Option<u32>, Error> {
    let Some(message_expiry_interval) = message_expiry_interval else {
        return Ok(None);
    };

    let elapsed_micros = now_timestamp.saturating_sub(message_timestamp);
    let elapsed_seconds = elapsed_micros / MICROS_PER_SECOND
        + u64::from(!elapsed_micros.is_multiple_of(MICROS_PER_SECOND));
    let remaining = u64::from(message_expiry_interval).saturating_sub(elapsed_seconds);
    if remaining == 0 {
        return Err(Error::InvalidRecordValue(
            "MQTT message expired before sink publication".to_string(),
        ));
    }

    Ok(Some(remaining as u32))
}

fn mqtt_properties_length(
    properties: &MqttEnvelopeProperties,
    correlation_data: Option<&[u8]>,
) -> usize {
    let mut length = 0;
    if properties.payload_format_indicator.is_some() {
        length += 2;
    }
    if properties.message_expiry_interval.is_some() {
        length += 5;
    }
    if let Some(response_topic) = &properties.response_topic {
        length += 3 + response_topic.len();
    }
    if let Some(correlation_data) = correlation_data {
        length += 3 + correlation_data.len();
    }
    for property in &properties.user_properties {
        length += 5 + property.key.len() + property.value.len();
    }
    if let Some(content_type) = &properties.content_type {
        length += 3 + content_type.len();
    }
    length
}

fn render_template(template: &str, metadata: &TopicMetadata) -> String {
    template
        .replace("{stream}", &metadata.stream)
        .replace("{topic}", &metadata.topic)
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_common::{HeaderKey, HeaderValue};
    use std::collections::BTreeMap;

    fn metadata() -> TopicMetadata {
        TopicMetadata {
            stream: "devices".into(),
            topic: "telemetry".into(),
        }
    }

    fn message(payload: Payload) -> ConsumedMessage {
        ConsumedMessage {
            id: 1,
            offset: 1,
            checksum: 1,
            timestamp: 1,
            origin_timestamp: 1,
            headers: None,
            payload,
        }
    }

    #[test]
    fn template_mode_renders_stream_and_topic() {
        let publication = prepare_publication(
            message(Payload::Raw(vec![1, 2])),
            &metadata(),
            TopicMode::Template,
            None,
            Some("iggy/{stream}/{topic}"),
            None,
            MqttProtocol::Mqtt5,
        )
        .unwrap();
        assert_eq!(publication.topic.as_str(), "iggy/devices/telemetry");
    }

    #[test]
    fn static_raw_payload_should_move_storage_into_publication() {
        let payload = vec![1, 2, 3];
        let payload_ptr = payload.as_ptr();

        let publication = prepare_publication(
            message(Payload::Raw(payload)),
            &metadata(),
            TopicMode::Static,
            Some("devices/output"),
            None,
            None,
            MqttProtocol::Mqtt5,
        )
        .unwrap();

        assert_eq!(publication.payload.as_ptr(), payload_ptr);
        assert_eq!(publication.payload.as_ref(), [1, 2, 3]);
    }

    #[test]
    fn dynamic_raw_uses_header() {
        let key = HeaderKey::try_from(MQTT_TOPIC_HEADER).unwrap();
        let mut headers = BTreeMap::new();
        headers.insert(key, HeaderValue::try_from("devices/living-room").unwrap());
        let mut message = message(Payload::Raw(vec![1]));
        message.headers = Some(headers);
        let publication = prepare_publication(
            message,
            &metadata(),
            TopicMode::Dynamic,
            None,
            None,
            Some("fallback/{stream}/{topic}"),
            MqttProtocol::Mqtt5,
        )
        .unwrap();
        assert_eq!(publication.topic.as_str(), "devices/living-room");
    }

    #[test]
    fn given_reserved_dynamic_header_when_preparing_should_return_record_error() {
        let key = HeaderKey::try_from(MQTT_TOPIC_HEADER).unwrap();
        let mut headers = BTreeMap::new();
        headers.insert(key, HeaderValue::try_from("$SYS/broker/uptime").unwrap());
        let mut message = message(Payload::Raw(vec![1]));
        message.headers = Some(headers);

        let error = prepare_publication(
            message,
            &metadata(),
            TopicMode::Dynamic,
            None,
            None,
            Some("fallback/{stream}/{topic}"),
            MqttProtocol::Mqtt5,
        )
        .unwrap_err();

        assert!(
            matches!(error, Error::InvalidRecordValue(reason) if reason.contains("must not start"))
        );
    }

    #[test]
    fn given_wildcard_in_template_value_when_preparing_batch_should_return_record_error() {
        let metadata = TopicMetadata {
            stream: "sensors+".into(),
            topic: "telemetry".into(),
        };

        let error = prepare_batch_topic(
            &metadata,
            TopicMode::Template,
            None,
            Some("{stream}/{topic}"),
        )
        .unwrap_err();

        assert!(matches!(error, Error::InvalidRecordValue(reason) if reason.contains("wildcards")));
    }

    #[test]
    fn dynamic_json_envelope_decodes_payload_and_properties() {
        let payload = serde_json::json!({
            "version": 1,
            "topic": "devices/test/telemetry",
            "properties": {
                "topic_alias": 7,
                "content_type": "application/json",
                "user_properties": [{"key": "device-type", "value": "sensor"}]
            },
            "payload_base64": "AQID"
        });
        let mut bytes = serde_json::to_vec(&payload).unwrap();
        let json = simd_json::to_owned_value(&mut bytes).unwrap();
        let publication = prepare_publication(
            message(Payload::Json(json)),
            &metadata(),
            TopicMode::Dynamic,
            None,
            None,
            None,
            MqttProtocol::Mqtt5,
        )
        .unwrap();
        assert_eq!(publication.topic.as_str(), "devices/test/telemetry");
        assert_eq!(publication.payload.as_ref(), [1, 2, 3]);
        assert_eq!(
            publication.properties.unwrap().user_properties[0],
            ("device-type".into(), "sensor".into())
        );
    }

    #[test]
    fn given_topic_alias_when_preparing_envelope_should_strip_alias() {
        let payload = serde_json::json!({
            "version": 1,
            "payload_base64": "AQID",
            "properties": {"topic_alias": 7}
        });
        let mut bytes = serde_json::to_vec(&payload).unwrap();
        let json = simd_json::to_owned_value(&mut bytes).unwrap();

        let publication = prepare_publication(
            message(Payload::Json(json)),
            &metadata(),
            TopicMode::Dynamic,
            None,
            None,
            None,
            MqttProtocol::Mqtt5,
        )
        .unwrap();

        assert_eq!(publication.properties.unwrap().topic_alias, None);
    }

    #[test]
    fn given_unexpired_message_when_calculating_expiry_should_return_remaining_interval() {
        let now = 100 * MICROS_PER_SECOND;
        let timestamp = 60 * MICROS_PER_SECOND;

        let remaining = remaining_message_expiry_interval(Some(60), timestamp, now).unwrap();

        assert_eq!(remaining, Some(20));
    }

    #[test]
    fn given_expired_message_when_calculating_expiry_should_return_record_error() {
        let now = 100 * MICROS_PER_SECOND;
        let timestamp = 39 * MICROS_PER_SECOND;

        let error = remaining_message_expiry_interval(Some(60), timestamp, now).unwrap_err();

        assert!(matches!(error, Error::InvalidRecordValue(reason) if reason.contains("expired")));
    }

    #[test]
    fn given_expired_envelope_when_preparing_should_return_record_error() {
        let payload = serde_json::json!({
            "version": 1,
            "payload_base64": "AQID",
            "properties": {"message_expiry_interval": 1}
        });
        let mut bytes = serde_json::to_vec(&payload).unwrap();
        let json = simd_json::to_owned_value(&mut bytes).unwrap();
        let mut message = message(Payload::Json(json));
        message.timestamp = 1;

        let error = prepare_publication(
            message,
            &metadata(),
            TopicMode::Dynamic,
            None,
            None,
            None,
            MqttProtocol::Mqtt5,
        )
        .unwrap_err();

        assert!(matches!(error, Error::InvalidRecordValue(reason) if reason.contains("expired")));
    }

    #[test]
    fn dynamic_json_envelope_should_move_topic_storage_into_publication() {
        let payload = serde_json::json!({
            "version": 1,
            "topic": "devices/test/telemetry",
            "payload_base64": "AQID"
        });
        let mut bytes = serde_json::to_vec(&payload).unwrap();
        let json = simd_json::to_owned_value(&mut bytes).unwrap();
        let topic_ptr = match &json {
            simd_json::OwnedValue::Object(object) => match object.get("topic") {
                Some(simd_json::OwnedValue::String(topic)) => topic.as_ptr(),
                _ => panic!("envelope topic must be a string"),
            },
            _ => panic!("envelope must be an object"),
        };

        let publication = prepare_publication(
            message(Payload::Json(json)),
            &metadata(),
            TopicMode::Dynamic,
            None,
            None,
            None,
            MqttProtocol::Mqtt5,
        )
        .unwrap();

        assert!(matches!(&publication.topic, PublicationTopic::Owned(_)));
        assert_eq!(publication.topic.as_str().as_ptr(), topic_ptr);
    }

    #[test]
    fn dynamic_json_envelope_with_invalid_payload_should_return_record_error() {
        let payload = serde_json::json!({
            "version": 1,
            "payload_base64": "not base64"
        });
        let mut bytes = serde_json::to_vec(&payload).unwrap();
        let json = simd_json::to_owned_value(&mut bytes).unwrap();

        let error = prepare_publication(
            message(Payload::Json(json)),
            &metadata(),
            TopicMode::Dynamic,
            None,
            None,
            None,
            MqttProtocol::Mqtt5,
        )
        .unwrap_err();

        assert!(matches!(error, Error::InvalidRecordValue(_)));
    }

    #[test]
    fn dynamic_json_envelope_with_invalid_field_type_should_return_record_error() {
        let payload = serde_json::json!({
            "version": "one",
            "payload_base64": "AQID"
        });
        let mut bytes = serde_json::to_vec(&payload).unwrap();
        let json = simd_json::to_owned_value(&mut bytes).unwrap();

        let error = prepare_publication(
            message(Payload::Json(json)),
            &metadata(),
            TopicMode::Dynamic,
            None,
            None,
            None,
            MqttProtocol::Mqtt5,
        )
        .unwrap_err();

        assert!(
            matches!(error, Error::InvalidRecordValue(reason) if reason.contains("invalid MQTT envelope"))
        );
    }

    #[test]
    fn dynamic_json_without_envelope_markers_should_use_fallback_topic() {
        let payload = serde_json::json!({"topic": "ordinary-json", "value": 1});
        let mut bytes = serde_json::to_vec(&payload).unwrap();
        let json = simd_json::to_owned_value(&mut bytes).unwrap();

        let publication = prepare_publication(
            message(Payload::Json(json)),
            &metadata(),
            TopicMode::Dynamic,
            None,
            None,
            Some("fallback/{stream}/{topic}"),
            MqttProtocol::Mqtt5,
        )
        .unwrap();

        assert_eq!(publication.topic.as_str(), "fallback/devices/telemetry");
        assert!(publication.properties.is_none());
    }

    #[test]
    fn dynamic_json_with_only_version_should_use_fallback_topic() {
        let payload = serde_json::json!({"version": 1, "value": "ordinary-json"});
        let payload_bytes = serde_json::to_vec(&payload).unwrap();
        let mut bytes = payload_bytes.clone();
        let json = simd_json::to_owned_value(&mut bytes).unwrap();

        let publication = prepare_publication(
            message(Payload::Json(json)),
            &metadata(),
            TopicMode::Dynamic,
            None,
            None,
            Some("fallback/{stream}/{topic}"),
            MqttProtocol::Mqtt5,
        )
        .unwrap();

        assert_eq!(publication.topic.as_str(), "fallback/devices/telemetry");
        assert_eq!(publication.payload.as_ref(), payload_bytes);
        assert!(publication.properties.is_none());
    }

    #[test]
    fn dynamic_json_with_only_payload_base64_should_use_fallback_topic() {
        let payload = serde_json::json!({"payload_base64": "AQID", "value": "ordinary-json"});
        let payload_bytes = serde_json::to_vec(&payload).unwrap();
        let mut bytes = payload_bytes.clone();
        let json = simd_json::to_owned_value(&mut bytes).unwrap();

        let publication = prepare_publication(
            message(Payload::Json(json)),
            &metadata(),
            TopicMode::Dynamic,
            None,
            None,
            Some("fallback/{stream}/{topic}"),
            MqttProtocol::Mqtt5,
        )
        .unwrap();

        assert_eq!(publication.topic.as_str(), "fallback/devices/telemetry");
        assert_eq!(publication.payload.as_ref(), payload_bytes);
        assert!(publication.properties.is_none());
    }

    #[test]
    fn topic_with_null_character_should_return_configuration_error() {
        let error = validate_topic("devices\0telemetry", "topic").unwrap_err();

        assert!(matches!(error, Error::InvalidConfigValue(message) if message.contains("null")));
    }

    #[test]
    fn topic_starting_with_dollar_should_return_configuration_error() {
        let error = validate_topic("$SYS/broker/uptime", "topic").unwrap_err();

        assert!(
            matches!(error, Error::InvalidConfigValue(message) if message.contains("must not start"))
        );
    }

    #[test]
    fn topic_template_starting_with_dollar_should_return_configuration_error() {
        let error = validate_topic_template("$SYS/{stream}", "topic_template").unwrap_err();

        assert!(
            matches!(error, Error::InvalidConfigValue(message) if message.contains("must not start"))
        );
    }

    #[test]
    fn topic_exceeding_mqtt_string_limit_should_return_configuration_error() {
        let topic = "a".repeat(MAX_MQTT_STRING_BYTES + 1);
        let error = validate_topic(&topic, "topic").unwrap_err();

        assert!(matches!(error, Error::InvalidConfigValue(message) if message.contains("limit")));
    }

    #[test]
    fn dynamic_json_envelope_with_invalid_properties_should_return_record_error() {
        for properties in [
            serde_json::json!({"payload_format_indicator": 2}),
            serde_json::json!({"response_topic": "devices/#"}),
            serde_json::json!({"subscription_identifiers": [1]}),
        ] {
            let payload = serde_json::json!({
                "version": 1,
                "payload_base64": "AQID",
                "properties": properties,
            });
            let mut bytes = serde_json::to_vec(&payload).unwrap();
            let json = simd_json::to_owned_value(&mut bytes).unwrap();

            let error = prepare_publication(
                message(Payload::Json(json)),
                &metadata(),
                TopicMode::Dynamic,
                None,
                None,
                None,
                MqttProtocol::Mqtt5,
            )
            .unwrap_err();

            assert!(matches!(error, Error::InvalidRecordValue(_)));
        }
    }

    #[test]
    fn dynamic_json_envelope_with_system_response_topic_should_prepare_publication() {
        let payload = serde_json::json!({
            "version": 1,
            "payload_base64": "AQID",
            "properties": {"response_topic": "$reply/device-42"},
        });
        let mut bytes = serde_json::to_vec(&payload).unwrap();
        let json = simd_json::to_owned_value(&mut bytes).unwrap();

        let publication = prepare_publication(
            message(Payload::Json(json)),
            &metadata(),
            TopicMode::Dynamic,
            None,
            None,
            None,
            MqttProtocol::Mqtt5,
        )
        .unwrap();

        assert_eq!(
            publication
                .properties
                .expect("response topic must produce MQTT 5 properties")
                .response_topic
                .as_deref(),
            Some("$reply/device-42")
        );
    }
}
