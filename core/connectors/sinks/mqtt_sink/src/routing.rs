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
use iggy_common::HeaderKey;
use iggy_connector_sdk::{ConsumedMessage, Error, Payload, TopicMetadata};
use rumqttc::v5::mqttbytes::v5::PublishProperties;
use serde::Deserialize;

use crate::MqttProtocol;

/// Header used by the MQTT source to preserve the original broker topic.
const MQTT_TOPIC_HEADER: &str = "mqtt.topic";
const DEFAULT_TOPIC_TEMPLATE: &str = "{stream}/{topic}";

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
    pub topic: String,
    pub payload: Vec<u8>,
    pub properties: Option<PublishProperties>,
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
    topic_alias: Option<u16>,
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
    if topic.trim().is_empty() {
        return Err(Error::InvalidConfigValue(format!(
            "{field} must not be empty"
        )));
    }
    if topic
        .as_bytes()
        .iter()
        .any(|character| matches!(*character, b'+' | b'#'))
    {
        return Err(Error::InvalidConfigValue(format!(
            "{field} must not contain MQTT wildcards"
        )));
    }
    Ok(())
}

pub fn prepare_publication(
    message: &ConsumedMessage,
    metadata: &TopicMetadata,
    mode: TopicMode,
    configured_topic: Option<&str>,
    topic_template: Option<&str>,
    fallback_topic: Option<&str>,
    protocol: MqttProtocol,
) -> Result<PreparedPublication, Error> {
    // Routing is explicit rather than a precedence chain: static and template
    // modes treat the Iggy message as opaque, while dynamic mode opts into
    // source-topic/header and MQTT 5 envelope interpretation.
    let template = topic_template.unwrap_or(DEFAULT_TOPIC_TEMPLATE);
    let fallback = fallback_topic.unwrap_or(template);
    let (topic, payload, properties) = match mode {
        TopicMode::Static => (
            configured_topic
                .filter(|topic| !topic.trim().is_empty())
                .map(str::to_owned)
                .unwrap_or_else(|| render_template(template, metadata)),
            message.payload.try_to_bytes()?,
            None,
        ),
        TopicMode::Template => (
            render_template(template, metadata),
            message.payload.try_to_bytes()?,
            None,
        ),
        TopicMode::Dynamic => prepare_dynamic(message, metadata, fallback, protocol)?,
    };
    validate_topic(&topic, "resolved topic")?;
    Ok(PreparedPublication {
        topic,
        payload,
        properties,
    })
}

fn prepare_dynamic(
    message: &ConsumedMessage,
    metadata: &TopicMetadata,
    fallback: &str,
    protocol: MqttProtocol,
) -> Result<(String, Vec<u8>, Option<PublishProperties>), Error> {
    // Raw messages can carry only the source topic header. JSON messages may
    // additionally contain the versioned MQTT 5 envelope used by mqtt_source.
    if let Payload::Raw(_) = &message.payload {
        let topic = message
            .headers
            .as_ref()
            .and_then(|headers| {
                HeaderKey::try_from(MQTT_TOPIC_HEADER)
                    .ok()
                    .and_then(|key| headers.get(&key))
            })
            .map(|value| value.to_string_value())
            .filter(|topic| !topic.trim().is_empty())
            .unwrap_or_else(|| render_template(fallback, metadata));
        return Ok((topic, message.payload.try_to_bytes()?, None));
    }

    if let Payload::Json(value) = &message.payload {
        let bytes = simd_json::to_vec(value).map_err(|_| Error::InvalidJsonPayload)?;
        if let Ok(envelope) = serde_json::from_slice::<MqttEnvelope>(&bytes) {
            // A recognized envelope must not be silently downgraded to MQTT
            // 3.1.1, because that would discard its publish properties.
            if protocol == MqttProtocol::Mqtt311 {
                return Err(Error::InvalidConfigValue(
                    "MQTT envelopes require protocol = mqtt5".to_string(),
                ));
            }
            if envelope.version != 1 {
                return Err(Error::InvalidConfigValue(format!(
                    "unsupported MQTT envelope version: {}",
                    envelope.version
                )));
            }
            let topic = envelope
                .topic
                .filter(|topic| !topic.trim().is_empty())
                .unwrap_or_else(|| render_template(fallback, metadata));
            let payload = STANDARD.decode(envelope.payload_base64).map_err(|error| {
                Error::InvalidConfigValue(format!("invalid MQTT envelope payload_base64: {error}"))
            })?;
            let properties = envelope
                .properties
                .map(|properties| -> Result<PublishProperties, Error> {
                    let correlation_data = properties
                        .correlation_data_base64
                        .map(|value| {
                            STANDARD.decode(value).map_err(|error| {
                                Error::InvalidConfigValue(format!(
                                    "invalid MQTT envelope correlation_data_base64: {error}"
                                ))
                            })
                        })
                        .transpose()?;
                    Ok(PublishProperties {
                        payload_format_indicator: properties.payload_format_indicator,
                        message_expiry_interval: properties.message_expiry_interval,
                        topic_alias: properties.topic_alias,
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
                })
                .transpose()?;
            return Ok((topic, payload, properties));
        }
    }

    Ok((
        render_template(fallback, metadata),
        message.payload.try_to_bytes()?,
        None,
    ))
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
            &message(Payload::Raw(vec![1, 2])),
            &metadata(),
            TopicMode::Template,
            None,
            Some("iggy/{stream}/{topic}"),
            None,
            MqttProtocol::Mqtt5,
        )
        .unwrap();
        assert_eq!(publication.topic, "iggy/devices/telemetry");
    }

    #[test]
    fn dynamic_raw_uses_header() {
        let key = HeaderKey::try_from(MQTT_TOPIC_HEADER).unwrap();
        let mut headers = BTreeMap::new();
        headers.insert(key, HeaderValue::try_from("devices/living-room").unwrap());
        let mut message = message(Payload::Raw(vec![1]));
        message.headers = Some(headers);
        let publication = prepare_publication(
            &message,
            &metadata(),
            TopicMode::Dynamic,
            None,
            None,
            Some("fallback/{stream}/{topic}"),
            MqttProtocol::Mqtt5,
        )
        .unwrap();
        assert_eq!(publication.topic, "devices/living-room");
    }

    #[test]
    fn dynamic_json_envelope_decodes_payload_and_properties() {
        let payload = serde_json::json!({
            "version": 1,
            "topic": "devices/test/telemetry",
            "properties": {
                "content_type": "application/json",
                "user_properties": [{"key": "device-type", "value": "sensor"}]
            },
            "payload_base64": "AQID"
        });
        let mut bytes = serde_json::to_vec(&payload).unwrap();
        let json = simd_json::to_owned_value(&mut bytes).unwrap();
        let publication = prepare_publication(
            &message(Payload::Json(json)),
            &metadata(),
            TopicMode::Dynamic,
            None,
            None,
            None,
            MqttProtocol::Mqtt5,
        )
        .unwrap();
        assert_eq!(publication.topic, "devices/test/telemetry");
        assert_eq!(publication.payload, vec![1, 2, 3]);
        assert_eq!(
            publication.properties.unwrap().user_properties[0],
            ("device-type".into(), "sensor".into())
        );
    }
}
