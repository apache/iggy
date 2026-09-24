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

use super::{MqttProtocol, MqttSourceConfig, Qos, qos_for_subscription};
use iggy_common::{HeaderKey, HeaderValue};
use rumqttc::v5::{
    AsyncClient as Mqtt5Client, Event as Mqtt5Event, EventLoop as Mqtt5EventLoop,
    Incoming as Mqtt5Incoming, MqttOptions as Mqtt5Options,
    mqttbytes::{QoS as Mqtt5Qos, v5::Publish as Mqtt5Publish},
};
use rumqttc::{
    AsyncClient as Mqtt311Client, Event as Mqtt311Event, EventLoop as Mqtt311EventLoop,
    Incoming as Mqtt311Incoming, MqttOptions as Mqtt311Options,
    mqttbytes::{QoS as Mqtt311Qos, v4::Publish as Mqtt311Publish},
};
use secrecy::ExposeSecret;
use std::{
    collections::{BTreeMap, VecDeque},
    time::Duration,
};
use tokio::time::timeout;
use url::Url;

const ACK_RETRY_ATTEMPTS: usize = 5;
const ACK_RETRY_DELAY: Duration = Duration::from_millis(10);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MqttMessageMetadata {
    pub(crate) qos: Qos,
    pub(crate) packet_id: Option<u16>,
    pub(crate) dup: bool,
    pub(crate) retain: bool,
}

#[derive(Debug)]
pub(crate) struct MqttMessage {
    pub(crate) payload: Vec<u8>,
    pub(crate) topic: String,
    pub(crate) headers: BTreeMap<HeaderKey, HeaderValue>,
    pub(crate) metadata: MqttMessageMetadata,
}

#[derive(Debug)]
pub(crate) struct ReceivedMessage {
    pub(crate) message: MqttMessage,
    pub(crate) ack_token: Option<AckToken>,
}

#[derive(Debug)]
pub(crate) struct AckToken(AckTokenKind);

#[derive(Debug)]
enum AckTokenKind {
    Mqtt311(Mqtt311Publish),
    Mqtt5(Mqtt5Publish),
}

enum MqttConnection {
    Mqtt311 {
        client: Box<Mqtt311Client>,
        event_loop: Box<Mqtt311EventLoop>,
    },
    Mqtt5 {
        client: Box<Mqtt5Client>,
        event_loop: Box<Mqtt5EventLoop>,
    },
}

pub(crate) struct MqttDriver {
    connection: MqttConnection,
    buffered_messages: VecDeque<ReceivedMessage>,
}

impl MqttDriver {
    pub(crate) async fn connect(
        id: u32,
        config: &MqttSourceConfig,
        qos: Qos,
        keep_alive: Duration,
        poll_timeout: Duration,
        request_capacity: usize,
    ) -> Result<Self, iggy_connector_sdk::Error> {
        let broker_url = broker_url_with_client_id(config, id)?;
        match config.protocol {
            MqttProtocol::Mqtt311 => {
                let mut options = Mqtt311Options::parse_url(&broker_url).map_err(|error| {
                    iggy_connector_sdk::Error::InvalidConfigValue(format!("broker_url: {error}"))
                })?;
                options
                    .set_client_id(client_id(config, id))
                    .set_clean_session(config.clean_start)
                    .set_keep_alive(keep_alive)
                    .set_request_channel_capacity(request_capacity)
                    .set_manual_acks(true);
                set_mqtt311_credentials(&mut options, config);

                let (client, mut event_loop) = Mqtt311Client::new(options, request_capacity);
                for topic in &config.subscriptions {
                    let subscription_qos = qos_for_subscription(config, topic, qos)?;
                    client
                        .subscribe(topic, subscription_qos.into())
                        .await
                        .map_err(|error| {
                            iggy_connector_sdk::Error::Connection(error.to_string())
                        })?;
                }
                poll_mqtt311(&mut event_loop, poll_timeout).await?;

                Ok(Self {
                    connection: MqttConnection::Mqtt311 {
                        client: Box::new(client),
                        event_loop: Box::new(event_loop),
                    },
                    buffered_messages: VecDeque::new(),
                })
            }
            MqttProtocol::Mqtt5 => {
                let mut options = Mqtt5Options::parse_url(&broker_url).map_err(|error| {
                    iggy_connector_sdk::Error::InvalidConfigValue(format!("broker_url: {error}"))
                })?;
                options
                    .set_client_id(client_id(config, id))
                    .set_clean_start(config.clean_start)
                    .set_keep_alive(keep_alive)
                    .set_request_channel_capacity(request_capacity)
                    .set_manual_acks(true)
                    .set_session_expiry_interval(config.session_expiry_interval);
                set_mqtt5_credentials(&mut options, config);

                let (client, mut event_loop) = Mqtt5Client::new(options, request_capacity);
                for topic in &config.subscriptions {
                    let subscription_qos = qos_for_subscription(config, topic, qos)?;
                    client
                        .subscribe(topic, subscription_qos.into())
                        .await
                        .map_err(|error| {
                            iggy_connector_sdk::Error::Connection(error.to_string())
                        })?;
                }
                poll_mqtt5(&mut event_loop, poll_timeout).await?;

                Ok(Self {
                    connection: MqttConnection::Mqtt5 {
                        client: Box::new(client),
                        event_loop: Box::new(event_loop),
                    },
                    buffered_messages: VecDeque::new(),
                })
            }
        }
    }

    pub(crate) async fn next_message(
        &mut self,
        poll_timeout: Duration,
    ) -> Result<Option<ReceivedMessage>, iggy_connector_sdk::Error> {
        if let Some(message) = self.buffered_messages.pop_front() {
            return Ok(Some(message));
        }

        let deadline = tokio::time::Instant::now() + poll_timeout;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Ok(None);
            }
            let event = match &mut self.connection {
                MqttConnection::Mqtt311 { event_loop, .. } => {
                    match timeout(remaining, event_loop.poll()).await {
                        Ok(Ok(event)) => match event {
                            Mqtt311Event::Incoming(Mqtt311Incoming::Publish(publish)) => {
                                Some(normalize_mqtt311(publish)?)
                            }
                            Mqtt311Event::Outgoing(_) | Mqtt311Event::Incoming(_) => None,
                        },
                        Ok(Err(error)) => {
                            return Err(iggy_connector_sdk::Error::Connection(error.to_string()));
                        }
                        Err(_) => return Ok(None),
                    }
                }
                MqttConnection::Mqtt5 { event_loop, .. } => {
                    match timeout(remaining, event_loop.poll()).await {
                        Ok(Ok(event)) => match event {
                            Mqtt5Event::Incoming(Mqtt5Incoming::Publish(publish)) => {
                                Some(normalize_mqtt5(publish)?)
                            }
                            Mqtt5Event::Outgoing(_) | Mqtt5Event::Incoming(_) => None,
                        },
                        Ok(Err(error)) => {
                            return Err(iggy_connector_sdk::Error::Connection(error.to_string()));
                        }
                        Err(_) => return Ok(None),
                    }
                }
            };
            if event.is_some() {
                return Ok(event);
            }
        }
    }

    pub(crate) async fn acknowledge_batch(
        &mut self,
        ack_tokens: &mut Vec<AckToken>,
        poll_timeout: Duration,
        max_buffered_messages: usize,
    ) -> Result<(), iggy_connector_sdk::Error> {
        let mut acknowledged = 0;
        let retry_delay = ACK_RETRY_DELAY.min(poll_timeout);
        while acknowledged < ack_tokens.len() {
            let mut last_error = None;
            let mut acknowledged_token = false;
            for attempt in 0..=ACK_RETRY_ATTEMPTS {
                match self.try_acknowledge(&ack_tokens[acknowledged]) {
                    Ok(()) => {
                        acknowledged_token = true;
                        break;
                    }
                    Err(error) => {
                        last_error = Some(error);
                        if attempt == ACK_RETRY_ATTEMPTS {
                            break;
                        }
                        if let Err(error) = self
                            .poll_for_ack_progress(poll_timeout, max_buffered_messages)
                            .await
                        {
                            retain_unacknowledged_tokens(ack_tokens, acknowledged);
                            return Err(error);
                        }
                        if !retry_delay.is_zero() {
                            tokio::time::sleep(retry_delay).await;
                        }
                    }
                }
            }

            if !acknowledged_token {
                retain_unacknowledged_tokens(ack_tokens, acknowledged);
                return Err(last_error.unwrap_or(iggy_connector_sdk::Error::InvalidState));
            }
            acknowledged += 1;
        }
        ack_tokens.clear();
        Ok(())
    }

    fn try_acknowledge(&self, ack_token: &AckToken) -> Result<(), iggy_connector_sdk::Error> {
        match (&self.connection, &ack_token.0) {
            (MqttConnection::Mqtt311 { client, .. }, AckTokenKind::Mqtt311(publish)) => client
                .try_ack(publish)
                .map_err(|error| iggy_connector_sdk::Error::Connection(error.to_string())),
            (MqttConnection::Mqtt5 { client, .. }, AckTokenKind::Mqtt5(publish)) => client
                .try_ack(publish)
                .map_err(|error| iggy_connector_sdk::Error::Connection(error.to_string())),
            _ => Err(iggy_connector_sdk::Error::InvalidState),
        }
    }

    async fn poll_for_ack_progress(
        &mut self,
        poll_timeout: Duration,
        max_buffered_messages: usize,
    ) -> Result<(), iggy_connector_sdk::Error> {
        let received = match &mut self.connection {
            MqttConnection::Mqtt311 { event_loop, .. } => {
                let event = timeout(poll_timeout, event_loop.poll())
                    .await
                    .map_err(|_| {
                        iggy_connector_sdk::Error::Connection(
                            "timed out while flushing MQTT acknowledgements".to_string(),
                        )
                    })?
                    .map_err(|error| iggy_connector_sdk::Error::Connection(error.to_string()))?;
                match event {
                    Mqtt311Event::Incoming(Mqtt311Incoming::Publish(publish)) => {
                        Some(normalize_mqtt311(publish)?)
                    }
                    Mqtt311Event::Outgoing(_) | Mqtt311Event::Incoming(_) => None,
                }
            }
            MqttConnection::Mqtt5 { event_loop, .. } => {
                let event = timeout(poll_timeout, event_loop.poll())
                    .await
                    .map_err(|_| {
                        iggy_connector_sdk::Error::Connection(
                            "timed out while flushing MQTT acknowledgements".to_string(),
                        )
                    })?
                    .map_err(|error| iggy_connector_sdk::Error::Connection(error.to_string()))?;
                match event {
                    Mqtt5Event::Incoming(Mqtt5Incoming::Publish(publish)) => {
                        Some(normalize_mqtt5(publish)?)
                    }
                    Mqtt5Event::Outgoing(_) | Mqtt5Event::Incoming(_) => None,
                }
            }
        };

        if let Some(received) = received {
            if self.buffered_messages.len() >= max_buffered_messages {
                return Err(iggy_connector_sdk::Error::Connection(
                    "MQTT message buffer reached batch_size while flushing acknowledgements"
                        .to_string(),
                ));
            }
            self.buffered_messages.push_back(received);
        }
        Ok(())
    }
}

fn retain_unacknowledged_tokens(ack_tokens: &mut Vec<AckToken>, acknowledged: usize) {
    ack_tokens.drain(..acknowledged);
}

fn broker_url_with_client_id(
    config: &MqttSourceConfig,
    id: u32,
) -> Result<String, iggy_connector_sdk::Error> {
    let mut url = Url::parse(&config.broker_url).map_err(|error| {
        iggy_connector_sdk::Error::InvalidConfigValue(format!("broker_url: {error}"))
    })?;
    let existing_query = url
        .query_pairs()
        .filter(|(key, _)| key != "client_id")
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    let mut query = url::form_urlencoded::Serializer::new(String::new());
    for (key, value) in existing_query {
        query.append_pair(&key, &value);
    }
    query.append_pair("client_id", &client_id(config, id));
    url.set_query(Some(&query.finish()));
    Ok(url.into())
}

fn client_id(config: &MqttSourceConfig, id: u32) -> String {
    config
        .client_id
        .clone()
        .unwrap_or_else(|| format!("iggy-mqtt-source-{id}"))
}

fn set_mqtt311_credentials(options: &mut Mqtt311Options, config: &MqttSourceConfig) {
    if let (Some(username), Some(password)) = (&config.username, &config.password) {
        options.set_credentials(username.clone(), password.expose_secret().to_string());
    }
}

fn set_mqtt5_credentials(options: &mut Mqtt5Options, config: &MqttSourceConfig) {
    if let (Some(username), Some(password)) = (&config.username, &config.password) {
        options.set_credentials(username.clone(), password.expose_secret().to_string());
    }
}

async fn poll_mqtt311(
    event_loop: &mut Mqtt311EventLoop,
    poll_timeout: Duration,
) -> Result<(), iggy_connector_sdk::Error> {
    timeout(poll_timeout, event_loop.poll())
        .await
        .map_err(|_| {
            iggy_connector_sdk::Error::Connection(
                "timed out connecting to MQTT 3.1.1 broker".to_string(),
            )
        })?
        .map_err(|error| iggy_connector_sdk::Error::Connection(error.to_string()))?;
    Ok(())
}

async fn poll_mqtt5(
    event_loop: &mut Mqtt5EventLoop,
    poll_timeout: Duration,
) -> Result<(), iggy_connector_sdk::Error> {
    timeout(poll_timeout, event_loop.poll())
        .await
        .map_err(|_| {
            iggy_connector_sdk::Error::Connection(
                "timed out connecting to MQTT 5 broker".to_string(),
            )
        })?
        .map_err(|error| iggy_connector_sdk::Error::Connection(error.to_string()))?;
    Ok(())
}

fn normalize_mqtt311(
    publish: Mqtt311Publish,
) -> Result<ReceivedMessage, iggy_connector_sdk::Error> {
    let qos = publish.qos.into();
    let metadata = MqttMessageMetadata {
        qos,
        packet_id: (qos != Qos::Zero).then_some(publish.pkid),
        dup: publish.dup,
        retain: publish.retain,
    };
    let headers = metadata_headers("mqtt311", &publish.topic, metadata)?;
    let message = MqttMessage {
        payload: publish.payload.to_vec(),
        topic: publish.topic.clone(),
        headers,
        metadata,
    };
    let ack_token = (metadata.qos != Qos::Zero).then_some(AckToken(AckTokenKind::Mqtt311(publish)));
    Ok(ReceivedMessage { message, ack_token })
}

pub(crate) fn normalize_mqtt5(
    publish: Mqtt5Publish,
) -> Result<ReceivedMessage, iggy_connector_sdk::Error> {
    let qos = publish.qos.into();
    let metadata = MqttMessageMetadata {
        qos,
        packet_id: (qos != Qos::Zero).then_some(publish.pkid),
        dup: publish.dup,
        retain: publish.retain,
    };
    let topic = topic_string(&publish.topic)?;
    let mut headers = metadata_headers("mqtt5", &topic, metadata)?;
    if let Some(properties) = publish.properties.as_ref() {
        insert_mqtt5_properties(&mut headers, properties)?;
    }
    let message = MqttMessage {
        payload: publish.payload.to_vec(),
        topic,
        headers,
        metadata,
    };
    let ack_token = (metadata.qos != Qos::Zero).then_some(AckToken(AckTokenKind::Mqtt5(publish)));
    Ok(ReceivedMessage { message, ack_token })
}

fn metadata_headers(
    protocol: &str,
    topic: &str,
    metadata: MqttMessageMetadata,
) -> Result<BTreeMap<HeaderKey, HeaderValue>, iggy_connector_sdk::Error> {
    let mut headers = BTreeMap::new();
    insert_string_header(&mut headers, "mqtt.protocol", protocol)?;
    insert_string_header(&mut headers, "mqtt.topic", topic)?;
    headers.insert(header_key("mqtt.qos")?, qos_value(metadata.qos).into());
    headers.insert(header_key("mqtt.dup")?, metadata.dup.into());
    headers.insert(header_key("mqtt.retain")?, metadata.retain.into());
    if let Some(packet_id) = metadata.packet_id {
        headers.insert(header_key("mqtt.packet_id")?, packet_id.into());
    }
    Ok(headers)
}

fn insert_mqtt5_properties(
    headers: &mut BTreeMap<HeaderKey, HeaderValue>,
    properties: &rumqttc::v5::mqttbytes::v5::PublishProperties,
) -> Result<(), iggy_connector_sdk::Error> {
    if let Some(value) = properties.payload_format_indicator {
        headers.insert(header_key("mqtt.payload_format_indicator")?, value.into());
    }
    if let Some(value) = properties.message_expiry_interval {
        headers.insert(header_key("mqtt.message_expiry_interval")?, value.into());
    }
    if let Some(value) = properties.topic_alias {
        headers.insert(header_key("mqtt.topic_alias")?, value.into());
    }
    if let Some(value) = properties.response_topic.as_deref() {
        insert_string_header(headers, "mqtt.response_topic", value)?;
    }
    if let Some(value) = properties.correlation_data.as_ref() {
        insert_raw_header(headers, "mqtt.correlation_data", value)?;
    }
    for (index, (key, value)) in properties.user_properties.iter().enumerate() {
        insert_string_header(headers, &format!("mqtt.user_property.{index}.key"), key)?;
        insert_string_header(headers, &format!("mqtt.user_property.{index}.value"), value)?;
    }
    for (index, value) in properties.subscription_identifiers.iter().enumerate() {
        headers.insert(
            header_key(&format!("mqtt.subscription_identifier.{index}"))?,
            (*value as u64).into(),
        );
    }
    if let Some(value) = properties.content_type.as_deref() {
        insert_string_header(headers, "mqtt.content_type", value)?;
    }
    Ok(())
}

fn insert_string_header(
    headers: &mut BTreeMap<HeaderKey, HeaderValue>,
    name: &str,
    value: &str,
) -> Result<(), iggy_connector_sdk::Error> {
    let header_value = HeaderValue::try_from(value).map_err(|error| {
        iggy_connector_sdk::Error::Serialization(format!(
            "invalid MQTT header value for {name}: {error}"
        ))
    })?;
    headers.insert(header_key(name)?, header_value);
    Ok(())
}

fn insert_raw_header(
    headers: &mut BTreeMap<HeaderKey, HeaderValue>,
    name: &str,
    value: &[u8],
) -> Result<(), iggy_connector_sdk::Error> {
    let header_value = HeaderValue::try_from(value.to_vec()).map_err(|error| {
        iggy_connector_sdk::Error::Serialization(format!(
            "invalid MQTT header value for {name}: {error}"
        ))
    })?;
    headers.insert(header_key(name)?, header_value);
    Ok(())
}

fn header_key(name: &str) -> Result<HeaderKey, iggy_connector_sdk::Error> {
    HeaderKey::try_from(name).map_err(|error| {
        iggy_connector_sdk::Error::Serialization(format!("invalid MQTT header key {name}: {error}"))
    })
}

fn qos_value(qos: Qos) -> u8 {
    match qos {
        Qos::Zero => 0,
        Qos::One => 1,
        Qos::Two => 2,
    }
}

fn topic_string(topic: &[u8]) -> Result<String, iggy_connector_sdk::Error> {
    String::from_utf8(topic.to_vec()).map_err(|_| iggy_connector_sdk::Error::InvalidTextPayload)
}

impl From<Qos> for Mqtt311Qos {
    fn from(qos: Qos) -> Self {
        match qos {
            Qos::Zero => Self::AtMostOnce,
            Qos::One => Self::AtLeastOnce,
            Qos::Two => Self::ExactlyOnce,
        }
    }
}

impl From<Qos> for Mqtt5Qos {
    fn from(qos: Qos) -> Self {
        match qos {
            Qos::Zero => Self::AtMostOnce,
            Qos::One => Self::AtLeastOnce,
            Qos::Two => Self::ExactlyOnce,
        }
    }
}

impl From<Mqtt311Qos> for Qos {
    fn from(qos: Mqtt311Qos) -> Self {
        match qos {
            Mqtt311Qos::AtMostOnce => Self::Zero,
            Mqtt311Qos::AtLeastOnce => Self::One,
            Mqtt311Qos::ExactlyOnce => Self::Two,
        }
    }
}

impl From<Mqtt5Qos> for Qos {
    fn from(qos: Mqtt5Qos) -> Self {
        match qos {
            Mqtt5Qos::AtMostOnce => Self::Zero,
            Mqtt5Qos::AtLeastOnce => Self::One,
            Mqtt5Qos::ExactlyOnce => Self::Two,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rumqttc::v5::mqttbytes::v5::PublishProperties;

    #[test]
    fn given_qos_should_convert_to_mqtt311_qos() {
        assert_eq!(Mqtt311Qos::AtMostOnce, Qos::Zero.into());
        assert_eq!(Mqtt311Qos::AtLeastOnce, Qos::One.into());
        assert_eq!(Mqtt311Qos::ExactlyOnce, Qos::Two.into());
    }

    #[test]
    fn given_qos_should_convert_to_mqtt5_qos() {
        assert_eq!(Mqtt5Qos::AtMostOnce, Qos::Zero.into());
        assert_eq!(Mqtt5Qos::AtLeastOnce, Qos::One.into());
        assert_eq!(Mqtt5Qos::ExactlyOnce, Qos::Two.into());
    }

    #[test]
    fn given_mqtt311_qos_zero_should_not_create_ack_token() {
        let publish = Mqtt311Publish::new("devices/test", Mqtt311Qos::AtMostOnce, b"payload");
        let received = normalize_mqtt311(publish).expect("MQTT 3.1.1 publish should normalize");

        assert!(received.ack_token.is_none());
        assert_eq!(received.message.metadata.qos, Qos::Zero);
        assert_eq!(received.message.metadata.packet_id, None);
    }

    #[test]
    fn given_mqtt311_publish_should_map_metadata_to_headers() {
        let mut publish = Mqtt311Publish::new("devices/test", Mqtt311Qos::AtLeastOnce, b"payload");
        publish.dup = true;
        publish.retain = true;

        let received = normalize_mqtt311(publish).expect("MQTT 3.1.1 publish should normalize");
        let headers = &received.message.headers;

        assert_eq!(
            headers[&header_key("mqtt.protocol").unwrap()]
                .as_str()
                .unwrap(),
            "mqtt311"
        );
        assert_eq!(
            headers[&header_key("mqtt.topic").unwrap()]
                .as_str()
                .unwrap(),
            "devices/test"
        );
        assert_eq!(
            headers[&header_key("mqtt.qos").unwrap()]
                .as_uint8()
                .unwrap(),
            1
        );
        assert_eq!(
            headers[&header_key("mqtt.packet_id").unwrap()]
                .as_uint16()
                .unwrap(),
            0
        );
        assert!(headers[&header_key("mqtt.dup").unwrap()].as_bool().unwrap());
        assert!(
            headers[&header_key("mqtt.retain").unwrap()]
                .as_bool()
                .unwrap()
        );
    }

    #[test]
    fn given_mqtt5_qos_two_should_create_deferred_ack_token() {
        let publish = Mqtt5Publish::new(
            "devices/test",
            Mqtt5Qos::ExactlyOnce,
            b"payload".as_slice(),
            None,
        );
        let received = normalize_mqtt5(publish).expect("MQTT 5 publish should normalize");

        assert!(received.ack_token.is_some());
        assert_eq!(received.message.metadata.qos, Qos::Two);
        assert_eq!(received.message.metadata.packet_id, Some(0));
    }

    #[test]
    fn given_mqtt311_qos_one_should_create_deferred_ack_token() {
        let publish = Mqtt311Publish::new("devices/test", Mqtt311Qos::AtLeastOnce, b"payload");
        let received = normalize_mqtt311(publish).expect("MQTT 3.1.1 publish should normalize");

        assert!(received.ack_token.is_some());
        assert_eq!(received.message.metadata.qos, Qos::One);
        assert_eq!(received.message.metadata.packet_id, Some(0));
    }

    #[test]
    fn given_mqtt311_qos_two_should_create_deferred_ack_token() {
        let publish = Mqtt311Publish::new("devices/test", Mqtt311Qos::ExactlyOnce, b"payload");
        let received = normalize_mqtt311(publish).expect("MQTT 3.1.1 publish should normalize");

        assert!(received.ack_token.is_some());
        assert_eq!(received.message.metadata.qos, Qos::Two);
        assert_eq!(received.message.metadata.packet_id, Some(0));
    }

    #[test]
    fn given_mqtt5_qos_zero_should_not_create_ack_token() {
        let publish = Mqtt5Publish::new(
            "devices/test",
            Mqtt5Qos::AtMostOnce,
            b"payload".as_slice(),
            None,
        );
        let received = normalize_mqtt5(publish).expect("MQTT 5 publish should normalize");

        assert!(received.ack_token.is_none());
        assert_eq!(received.message.metadata.qos, Qos::Zero);
        assert_eq!(received.message.metadata.packet_id, None);
    }

    #[test]
    fn given_mqtt5_qos_one_should_create_deferred_ack_token() {
        let publish = Mqtt5Publish::new(
            "devices/test",
            Mqtt5Qos::AtLeastOnce,
            b"payload".as_slice(),
            None,
        );
        let received = normalize_mqtt5(publish).expect("MQTT 5 publish should normalize");

        assert!(received.ack_token.is_some());
        assert_eq!(received.message.metadata.qos, Qos::One);
        assert_eq!(received.message.metadata.packet_id, Some(0));
    }

    #[test]
    fn given_mqtt5_publish_properties_should_map_to_headers() {
        let properties = PublishProperties {
            payload_format_indicator: Some(1),
            message_expiry_interval: Some(30),
            topic_alias: Some(4),
            response_topic: Some("devices/response".to_string()),
            correlation_data: Some(b"correlation".as_slice().into()),
            user_properties: vec![("device_id".to_string(), "device-1".to_string())],
            subscription_identifiers: vec![7, 9],
            content_type: Some("application/json".to_string()),
        };
        let publish = Mqtt5Publish::new(
            "devices/test",
            Mqtt5Qos::AtLeastOnce,
            b"payload".as_slice(),
            Some(properties),
        );

        let received = normalize_mqtt5(publish).expect("MQTT 5 publish should normalize");
        let headers = &received.message.headers;

        assert_eq!(
            headers[&header_key("mqtt.protocol").unwrap()]
                .as_str()
                .unwrap(),
            "mqtt5"
        );
        assert_eq!(
            headers[&header_key("mqtt.payload_format_indicator").unwrap()]
                .as_uint8()
                .unwrap(),
            1
        );
        assert_eq!(
            headers[&header_key("mqtt.message_expiry_interval").unwrap()]
                .as_uint32()
                .unwrap(),
            30
        );
        assert_eq!(
            headers[&header_key("mqtt.topic_alias").unwrap()]
                .as_uint16()
                .unwrap(),
            4
        );
        assert_eq!(
            headers[&header_key("mqtt.response_topic").unwrap()]
                .as_str()
                .unwrap(),
            "devices/response"
        );
        assert_eq!(
            headers[&header_key("mqtt.correlation_data").unwrap()]
                .as_raw()
                .unwrap(),
            b"correlation"
        );
        assert_eq!(
            headers[&header_key("mqtt.user_property.0.key").unwrap()]
                .as_str()
                .unwrap(),
            "device_id"
        );
        assert_eq!(
            headers[&header_key("mqtt.user_property.0.value").unwrap()]
                .as_str()
                .unwrap(),
            "device-1"
        );
        assert_eq!(
            headers[&header_key("mqtt.subscription_identifier.0").unwrap()]
                .as_uint64()
                .unwrap(),
            7
        );
        assert_eq!(
            headers[&header_key("mqtt.subscription_identifier.1").unwrap()]
                .as_uint64()
                .unwrap(),
            9
        );
        assert_eq!(
            headers[&header_key("mqtt.content_type").unwrap()]
                .as_str()
                .unwrap(),
            "application/json"
        );
    }

    #[test]
    fn given_partial_ack_failure_should_retain_unacknowledged_tokens() {
        let first = Mqtt311Publish::new("devices/test", Mqtt311Qos::AtLeastOnce, b"first");
        let second = Mqtt311Publish::new("devices/test", Mqtt311Qos::AtLeastOnce, b"second");
        let third = Mqtt311Publish::new("devices/test", Mqtt311Qos::AtLeastOnce, b"third");
        let mut ack_tokens = vec![
            AckToken(AckTokenKind::Mqtt311(first)),
            AckToken(AckTokenKind::Mqtt311(second)),
            AckToken(AckTokenKind::Mqtt311(third)),
        ];

        retain_unacknowledged_tokens(&mut ack_tokens, 1);

        assert_eq!(ack_tokens.len(), 2);
        let AckToken(AckTokenKind::Mqtt311(publish)) = &ack_tokens[0] else {
            panic!("expected MQTT 3.1.1 acknowledgement token");
        };
        assert_eq!(publish.payload.as_ref(), b"second");
    }

    #[test]
    fn given_invalid_broker_url_should_reject_before_connecting() {
        let config = MqttSourceConfig {
            broker_url: "not a mqtt url".to_string(),
            subscriptions: vec!["devices/test".to_string()],
            subscription_qos: BTreeMap::new(),
            protocol: MqttProtocol::Mqtt5,
            qos: 1,
            client_id: Some("test-source".to_string()),
            username: None,
            password: None,
            clean_start: false,
            session_expiry_interval: None,
            keep_alive: Some("5s".to_string()),
            poll_timeout: Some("100ms".to_string()),
            request_capacity: Some(4),
            batch_size: Some(3),
            batch_timeout: Some("10ms".to_string()),
            verbose_logging: None,
        };
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");

        let result = runtime.block_on(MqttDriver::connect(
            7,
            &config,
            Qos::One,
            Duration::from_secs(5),
            Duration::from_millis(100),
            4,
        ));

        assert!(matches!(
            result,
            Err(iggy_connector_sdk::Error::InvalidConfigValue(message))
                if message.starts_with("broker_url:")
        ));
    }

    #[test]
    fn given_broker_url_without_client_id_should_add_configured_client_id() {
        let config = MqttSourceConfig {
            broker_url: "mqtt://127.0.0.1:1883?keep_alive_secs=30".to_string(),
            subscriptions: vec!["devices/test".to_string()],
            subscription_qos: BTreeMap::new(),
            protocol: MqttProtocol::Mqtt5,
            qos: 1,
            client_id: Some("configured-client".to_string()),
            username: None,
            password: None,
            clean_start: false,
            session_expiry_interval: None,
            keep_alive: Some("30s".to_string()),
            poll_timeout: Some("1s".to_string()),
            request_capacity: Some(4),
            batch_size: Some(3),
            batch_timeout: Some("10ms".to_string()),
            verbose_logging: None,
        };

        let broker_url = broker_url_with_client_id(&config, 7).expect("URL should be valid");
        let parsed = Url::parse(&broker_url).expect("URL should parse");
        let query = parsed.query_pairs().collect::<Vec<_>>();

        assert!(query.contains(&("keep_alive_secs".into(), "30".into())));
        assert!(query.contains(&("client_id".into(), "configured-client".into())));
    }
}
