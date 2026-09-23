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

use super::{MqttProtocol, MqttSourceConfig, Qos};
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
use std::time::Duration;
use tokio::time::timeout;
use url::Url;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MqttMessageMetadata {
    pub(crate) qos: Qos,
    pub(crate) packet_id: Option<u16>,
    pub(crate) retain: bool,
}

#[derive(Debug)]
pub(crate) struct MqttMessage {
    pub(crate) payload: Vec<u8>,
    pub(crate) topic: String,
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
                    client.subscribe(topic, qos.into()).await.map_err(|error| {
                        iggy_connector_sdk::Error::Connection(error.to_string())
                    })?;
                }
                poll_mqtt311(&mut event_loop, poll_timeout).await?;

                Ok(Self {
                    connection: MqttConnection::Mqtt311 {
                        client: Box::new(client),
                        event_loop: Box::new(event_loop),
                    },
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
                    client.subscribe(topic, qos.into()).await.map_err(|error| {
                        iggy_connector_sdk::Error::Connection(error.to_string())
                    })?;
                }
                poll_mqtt5(&mut event_loop, poll_timeout).await?;

                Ok(Self {
                    connection: MqttConnection::Mqtt5 {
                        client: Box::new(client),
                        event_loop: Box::new(event_loop),
                    },
                })
            }
        }
    }

    pub(crate) async fn next_message(
        &mut self,
        poll_timeout: Duration,
    ) -> Result<Option<ReceivedMessage>, iggy_connector_sdk::Error> {
        match &mut self.connection {
            MqttConnection::Mqtt311 { event_loop, .. } => {
                let event = match timeout(poll_timeout, event_loop.poll()).await {
                    Ok(Ok(event)) => event,
                    Ok(Err(error)) => {
                        return Err(iggy_connector_sdk::Error::Connection(error.to_string()));
                    }
                    Err(_) => return Ok(None),
                };
                match event {
                    Mqtt311Event::Incoming(Mqtt311Incoming::Publish(publish)) => {
                        Ok(Some(normalize_mqtt311(publish)?))
                    }
                    Mqtt311Event::Outgoing(_) => Ok(None),
                    Mqtt311Event::Incoming(_) => Ok(None),
                }
            }
            MqttConnection::Mqtt5 { event_loop, .. } => {
                let event = match timeout(poll_timeout, event_loop.poll()).await {
                    Ok(Ok(event)) => event,
                    Ok(Err(error)) => {
                        return Err(iggy_connector_sdk::Error::Connection(error.to_string()));
                    }
                    Err(_) => return Ok(None),
                };
                match event {
                    Mqtt5Event::Incoming(Mqtt5Incoming::Publish(publish)) => {
                        Ok(Some(normalize_mqtt5(publish)?))
                    }
                    Mqtt5Event::Outgoing(_) => Ok(None),
                    Mqtt5Event::Incoming(_) => Ok(None),
                }
            }
        }
    }

    pub(crate) fn acknowledge(
        &self,
        ack_token: &AckToken,
    ) -> Result<(), iggy_connector_sdk::Error> {
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
        retain: publish.retain,
    };
    let message = MqttMessage {
        payload: publish.payload.to_vec(),
        topic: publish.topic.clone(),
        metadata,
    };
    let ack_token = (metadata.qos != Qos::Zero).then_some(AckToken(AckTokenKind::Mqtt311(publish)));
    Ok(ReceivedMessage { message, ack_token })
}

fn normalize_mqtt5(publish: Mqtt5Publish) -> Result<ReceivedMessage, iggy_connector_sdk::Error> {
    let qos = publish.qos.into();
    let metadata = MqttMessageMetadata {
        qos,
        packet_id: (qos != Qos::Zero).then_some(publish.pkid),
        retain: publish.retain,
    };
    let message = MqttMessage {
        payload: publish.payload.to_vec(),
        topic: topic_string(&publish.topic)?,
        metadata,
    };
    let ack_token = (metadata.qos != Qos::Zero).then_some(AckToken(AckTokenKind::Mqtt5(publish)));
    Ok(ReceivedMessage { message, ack_token })
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
    fn given_invalid_broker_url_should_reject_before_connecting() {
        let config = MqttSourceConfig {
            broker_url: "not a mqtt url".to_string(),
            subscriptions: vec!["devices/test".to_string()],
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
            verbose_logging: None,
        };

        let broker_url = broker_url_with_client_id(&config, 7).expect("URL should be valid");
        let parsed = Url::parse(&broker_url).expect("URL should parse");
        let query = parsed.query_pairs().collect::<Vec<_>>();

        assert!(query.contains(&("keep_alive_secs".into(), "30".into())));
        assert!(query.contains(&("client_id".into(), "configured-client".into())));
    }
}
