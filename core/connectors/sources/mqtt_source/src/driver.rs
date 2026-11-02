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
use base64::Engine;
use iggy_common::{HeaderKey, HeaderValue};
use rumqttc_v4::tokio_rustls::rustls::{
    self, ClientConfig, RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer},
};
use rumqttc_v4::{
    AckMode as Mqtt311AckMode, AsyncClient as Mqtt311Client, Event as Mqtt311Event,
    EventLoop as Mqtt311EventLoop, Incoming as Mqtt311Incoming, MqttOptions as Mqtt311Options,
    TlsConfiguration, Transport as Mqtt311Transport,
    mqttbytes::{QoS as MqttQos, v4::Publish as Mqtt311Publish},
};
use rumqttc_v5::{
    AckMode as Mqtt5AckMode, AsyncClient as Mqtt5Client, BrokerSessionResumePolicy,
    Event as Mqtt5Event, EventLoop as Mqtt5EventLoop, Incoming as Mqtt5Incoming,
    MqttOptions as Mqtt5Options, Transport as Mqtt5Transport,
    mqttbytes::v5::Publish as Mqtt5Publish,
};
use rustls_native_certs::load_native_certs;
use rustls_pemfile::{certs, private_key};
use secrecy::ExposeSecret;
use serde::Serialize;
use std::{
    collections::{BTreeMap, VecDeque},
    io::{BufReader, Cursor},
    sync::{Arc, LazyLock},
    time::Duration,
};
use tokio::time::{sleep, timeout};
use tracing::warn;
use url::Url;

const MAX_HEADER_VALUE_LENGTH: usize = 255;
const RECONNECT_RETRY_DELAY: Duration = Duration::from_millis(10);
const MQTTS_DEFAULT_PORT: u16 = 8883;
const MQTT_PROTOCOL_HEADER: &str = "mqtt.protocol";
const MQTT_TOPIC_HEADER: &str = "mqtt.topic";

static MQTT_PROTOCOL_HEADER_KEY: LazyLock<HeaderKey> = LazyLock::new(|| {
    HeaderKey::try_from(MQTT_PROTOCOL_HEADER).expect("MQTT protocol header key is valid")
});
static MQTT_TOPIC_HEADER_KEY: LazyLock<HeaderKey> = LazyLock::new(|| {
    HeaderKey::try_from(MQTT_TOPIC_HEADER).expect("MQTT topic header key is valid")
});
static MQTT_QOS_HEADER_KEY: LazyLock<HeaderKey> =
    LazyLock::new(|| HeaderKey::try_from("mqtt.qos").expect("MQTT QoS header key is valid"));
static MQTT_DUP_HEADER_KEY: LazyLock<HeaderKey> =
    LazyLock::new(|| HeaderKey::try_from("mqtt.dup").expect("MQTT duplicate header key is valid"));
static MQTT_RETAIN_HEADER_KEY: LazyLock<HeaderKey> =
    LazyLock::new(|| HeaderKey::try_from("mqtt.retain").expect("MQTT retain header key is valid"));
#[cfg(test)]
static MQTT_PACKET_ID_HEADER_KEY: LazyLock<HeaderKey> = LazyLock::new(|| {
    HeaderKey::try_from("mqtt.packet_id").expect("MQTT packet ID header key is valid")
});
#[cfg(test)]
static MQTT_RESPONSE_TOPIC_HEADER_KEY: LazyLock<HeaderKey> = LazyLock::new(|| {
    HeaderKey::try_from("mqtt.response_topic").expect("MQTT response topic header key is valid")
});

// Metadata is kept separate from the payload so protocol details can be
// preserved in Iggy headers without changing the application bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct MqttMessageMetadata {
    pub(crate) qos: Qos,
    pub(crate) packet_id: Option<u16>,
    pub(crate) dup: bool,
    pub(crate) retain: bool,
}

/// MQTT payload plus normalized topic metadata ready for Iggy headers.
#[derive(Debug)]
pub(crate) struct MqttMessage {
    pub(crate) payload: Vec<u8>,
    pub(crate) topic: String,
    pub(crate) headers: BTreeMap<HeaderKey, HeaderValue>,
    pub(crate) metadata: MqttMessageMetadata,
    pub(crate) mqtt5_properties: Option<Mqtt5EnvelopeProperties>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct Mqtt5EnvelopeProperties {
    pub(crate) payload_format_indicator: Option<u8>,
    pub(crate) message_expiry_interval: Option<u32>,
    pub(crate) topic_alias: Option<u16>,
    pub(crate) response_topic: Option<String>,
    pub(crate) correlation_data_base64: Option<String>,
    pub(crate) user_properties: Vec<Mqtt5UserProperty>,
    pub(crate) subscription_identifiers: Vec<usize>,
    pub(crate) content_type: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub(crate) struct Mqtt5UserProperty {
    pub(crate) key: String,
    pub(crate) value: String,
}

/// A normalized MQTT message and its optional deferred acknowledgement token.
#[derive(Debug)]
pub(crate) struct ReceivedMessage {
    pub(crate) message: MqttMessage,
    pub(crate) ack_token: Option<AckToken>,
}

#[derive(Debug)]
pub(crate) enum AckToken {
    Mqtt311 { qos: MqttQos, packet_id: u16 },
    Mqtt5 { qos: MqttQos, packet_id: u16 },
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

struct MqttSubscription {
    topic: String,
    qos: Qos,
}

enum PollResult {
    Message(Box<ReceivedMessage>),
    Buffered(VecDeque<ReceivedMessage>),
    None,
}

struct BrokerUrl {
    url: String,
    tls: bool,
}

/// Protocol-specific MQTT client and event loop behind one common driver API.
pub(crate) struct MqttDriver {
    // Messages received while flushing acknowledgements are retained here rather
    // than dropped. The source batch size bounds this queue.
    connection: MqttConnection,
    subscriptions: Vec<MqttSubscription>,
    buffered_messages: VecDeque<ReceivedMessage>,
    // Iggy has accepted these batches, but rumqttc could not yet queue their
    // acknowledgements. Keep them until a later poll can flush them.
    pending_ack_tokens: Vec<AckToken>,
    include_metadata: bool,
    max_buffered_messages: usize,
}

impl MqttDriver {
    pub(crate) async fn connect(
        id: u32,
        config: &MqttSourceConfig,
        qos: Qos,
        keep_alive: Duration,
        poll_timeout: Duration,
        request_capacity: usize,
        max_buffered_messages: usize,
    ) -> Result<Self, iggy_connector_sdk::Error> {
        install_rustls_provider();
        let keep_alive = u16::try_from(keep_alive.as_secs()).map_err(|_| {
            iggy_connector_sdk::Error::InvalidConfigValue(format!(
                "keep_alive must be at most {}s",
                u16::MAX
            ))
        })?;
        let broker_url = broker_url_with_client_id(config, id)?;
        let subscriptions = configured_subscriptions(config, qos)?;
        match config.protocol {
            MqttProtocol::Mqtt311 => {
                let mut options = Mqtt311Options::parse_url(&broker_url.url).map_err(|error| {
                    iggy_connector_sdk::Error::InvalidConfigValue(format!("broker_url: {error}"))
                })?;
                options
                    .set_client_id(client_id(config, id))
                    .set_clean_session(config.clean_start)
                    .set_keep_alive(keep_alive)
                    .set_request_channel_capacity(request_capacity)
                    // Manual acknowledgements let Iggy persistence happen before
                    // PUBACK/PUBREC is sent to the broker.
                    .set_ack_mode(Mqtt311AckMode::Manual);
                set_mqtt311_credentials(&mut options, config);
                if broker_url.tls {
                    options.set_transport(Mqtt311Transport::tls_with_config(tls_configuration(
                        config,
                    )?));
                }

                let (client, mut event_loop) = Mqtt311Client::builder(options)
                    .try_build()
                    .map_err(|error| {
                        iggy_connector_sdk::Error::InvalidConfigValue(error.to_string())
                    })?;
                subscribe_mqtt311(&client, &subscriptions).await?;
                let buffered_messages = poll_mqtt311(
                    &mut event_loop,
                    subscriptions.len(),
                    poll_timeout,
                    max_buffered_messages,
                )
                .await?;

                Ok(Self {
                    connection: MqttConnection::Mqtt311 {
                        client: Box::new(client),
                        event_loop: Box::new(event_loop),
                    },
                    subscriptions,
                    buffered_messages,
                    pending_ack_tokens: Vec::new(),
                    include_metadata: config.include_metadata,
                    max_buffered_messages,
                })
            }
            MqttProtocol::Mqtt5 => {
                let mut options = Mqtt5Options::parse_url(&broker_url.url).map_err(|error| {
                    iggy_connector_sdk::Error::InvalidConfigValue(format!("broker_url: {error}"))
                })?;
                options
                    .set_client_id(client_id(config, id))
                    .set_clean_start(config.clean_start)
                    .set_keep_alive(keep_alive)
                    .set_request_channel_capacity(request_capacity)
                    // MQTT 5 uses the same deferred-acknowledgement lifecycle;
                    // rumqttc maps the token to PUBREC/PUBREL/PUBCOMP internally.
                    .set_ack_mode(Mqtt5AckMode::Manual)
                    // The source keeps no local session store, so after a restart the
                    // broker's session is what redelivers unacknowledged publishes.
                    .set_broker_session_resume_policy(BrokerSessionResumePolicy::AllowBrokerOnly)
                    .set_session_expiry_interval(config.session_expiry_interval);
                set_mqtt5_credentials(&mut options, config);
                if broker_url.tls {
                    options
                        .set_transport(Mqtt5Transport::tls_with_config(tls_configuration(config)?));
                }

                let (client, mut event_loop) =
                    Mqtt5Client::builder(options).try_build().map_err(|error| {
                        iggy_connector_sdk::Error::InvalidConfigValue(error.to_string())
                    })?;
                // A resumed broker session keeps its subscriptions, and broker-only
                // resume refuses new SUBSCRIBE packets, so only a fresh session subscribes.
                let buffered_messages =
                    if mqtt5_session_present(&mut event_loop, poll_timeout).await? {
                        VecDeque::new()
                    } else {
                        subscribe_mqtt5(&client, &subscriptions).await?;
                        poll_mqtt5(
                            &mut event_loop,
                            subscriptions.len(),
                            poll_timeout,
                            config.include_metadata,
                            max_buffered_messages,
                        )
                        .await?
                    };

                Ok(Self {
                    connection: MqttConnection::Mqtt5 {
                        client: Box::new(client),
                        event_loop: Box::new(event_loop),
                    },
                    subscriptions,
                    buffered_messages,
                    pending_ack_tokens: Vec::new(),
                    include_metadata: config.include_metadata,
                    max_buffered_messages,
                })
            }
        }
    }

    pub(crate) async fn next_message(
        &mut self,
        poll_timeout: Duration,
    ) -> Result<Option<ReceivedMessage>, iggy_connector_sdk::Error> {
        // Consume buffered messages first. They arrived while another message's
        // acknowledgement was being flushed and are already valid source data.
        if let Some(message) = self.buffered_messages.pop_front() {
            return Ok(Some(message));
        }

        // Bound each driver poll so the source can flush partial batches and
        // respond to runtime shutdown instead of waiting indefinitely.
        let deadline = tokio::time::Instant::now() + poll_timeout;
        let include_metadata = self.include_metadata;
        let max_buffered_messages = self.max_buffered_messages;
        loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Ok(None);
            }
            let result = match (&mut self.connection, &self.subscriptions) {
                (MqttConnection::Mqtt311 { client, event_loop }, subscriptions) => {
                    match timeout(remaining, event_loop.poll()).await {
                        Ok(Ok(event)) => {
                            handle_mqtt311_event(
                                client,
                                event_loop,
                                event,
                                subscriptions,
                                remaining,
                                max_buffered_messages,
                            )
                            .await?
                        }
                        Ok(Err(error)) => {
                            // rumqttc resets its network state after a
                            // disconnect. Polling it again performs the
                            // reconnect and yields the next ConnAck.
                            warn!(
                                "MQTT 3.1.1 connection interrupted; retrying event loop: {error}"
                            );
                            sleep(RECONNECT_RETRY_DELAY).await;
                            continue;
                        }
                        Err(_) => return Ok(None),
                    }
                }
                (MqttConnection::Mqtt5 { client, event_loop }, subscriptions) => {
                    match timeout(remaining, event_loop.poll()).await {
                        Ok(Ok(event)) => {
                            handle_mqtt5_event(
                                client,
                                event_loop,
                                event,
                                subscriptions,
                                remaining,
                                include_metadata,
                                max_buffered_messages,
                            )
                            .await?
                        }
                        Ok(Err(error)) => {
                            // rumqttc resets its network state after a
                            // disconnect. Polling it again performs the
                            // reconnect and yields the next ConnAck.
                            warn!("MQTT 5 connection interrupted; retrying event loop: {error}");
                            sleep(RECONNECT_RETRY_DELAY).await;
                            continue;
                        }
                        Err(_) => return Ok(None),
                    }
                }
            };
            match result {
                PollResult::Message(message) => return Ok(Some(*message)),
                PollResult::Buffered(mut messages) => {
                    self.buffered_messages.append(&mut messages);
                    if let Some(message) = self.buffered_messages.pop_front() {
                        return Ok(Some(message));
                    }
                }
                PollResult::None => {}
            }
        }
    }

    pub(crate) async fn acknowledge_batch(
        &mut self,
        ack_tokens: &mut Vec<AckToken>,
        poll_timeout: Duration,
        max_retries: u32,
    ) -> Result<(), iggy_connector_sdk::Error> {
        // Process tokens in order. On failure, remove only tokens already
        // acknowledged so the remaining suffix can be retried.
        let mut acknowledged = 0;
        while acknowledged < ack_tokens.len() {
            let mut last_error = None;
            let mut acknowledged_token = false;
            for attempt in 0..=max_retries {
                match self.try_acknowledge(&ack_tokens[acknowledged]) {
                    Ok(()) => {
                        acknowledged_token = true;
                        break;
                    }
                    Err(error) => {
                        let should_poll_for_progress =
                            matches!(error, iggy_connector_sdk::Error::Connection(_));
                        last_error = Some(error);
                        if !should_poll_for_progress {
                            break;
                        }
                        // rumqttc may need event-loop progress before try_ack can
                        // enqueue the acknowledgement. This also lets rumqttc
                        // reconnect before a retained acknowledgement is retried.
                        if let Err(error) = self
                            .poll_for_ack_progress(poll_timeout, self.max_buffered_messages)
                            .await
                        {
                            ack_tokens.drain(..acknowledged);
                            return Err(error);
                        }
                        if attempt == max_retries {
                            break;
                        }
                    }
                }
            }

            if !acknowledged_token {
                ack_tokens.drain(..acknowledged);
                return Err(last_error.unwrap_or(iggy_connector_sdk::Error::InvalidState));
            }
            acknowledged += 1;
        }
        ack_tokens.clear();
        Ok(())
    }

    pub(crate) fn defer_acknowledgements(&mut self, ack_tokens: &mut Vec<AckToken>) {
        retain_acknowledgements(&mut self.pending_ack_tokens, ack_tokens);
    }

    pub(crate) async fn flush_deferred_acknowledgements(
        &mut self,
        poll_timeout: Duration,
        max_retries: u32,
    ) -> Result<(), iggy_connector_sdk::Error> {
        if self.pending_ack_tokens.is_empty() {
            return Ok(());
        }

        let mut ack_tokens = std::mem::take(&mut self.pending_ack_tokens);
        let result = self
            .acknowledge_batch(&mut ack_tokens, poll_timeout, max_retries)
            .await;
        self.pending_ack_tokens = ack_tokens;
        result
    }

    fn try_acknowledge(&self, ack_token: &AckToken) -> Result<(), iggy_connector_sdk::Error> {
        // A token must be acknowledged by the same protocol client that created
        // it. Mixing MQTT 3.1.1 and MQTT 5 tokens is an invalid internal state.
        match (&self.connection, ack_token) {
            (MqttConnection::Mqtt311 { client, .. }, AckToken::Mqtt311 { qos, packet_id }) => {
                // rumqttc derives the MQTT acknowledgement only from QoS and
                // packet ID, so retaining the received payload is unnecessary.
                let mut publish = Mqtt311Publish::new("", *qos, []);
                publish.pkid = *packet_id;
                client
                    .try_ack(&publish)
                    .map_err(|error| iggy_connector_sdk::Error::Connection(error.to_string()))
            }
            (MqttConnection::Mqtt5 { client, .. }, AckToken::Mqtt5 { qos, packet_id }) => {
                let publish = Mqtt5Publish {
                    qos: *qos,
                    pkid: *packet_id,
                    ..Default::default()
                };
                client
                    .try_ack(&publish)
                    .map_err(|error| iggy_connector_sdk::Error::Connection(error.to_string()))
            }
            _ => Err(iggy_connector_sdk::Error::InvalidState),
        }
    }

    async fn poll_for_ack_progress(
        &mut self,
        poll_timeout: Duration,
        max_buffered_messages: usize,
    ) -> Result<(), iggy_connector_sdk::Error> {
        // A publish can arrive while the event loop is being driven for an ACK.
        // Preserve it for the next source poll, subject to the batch bound.
        let deadline = tokio::time::Instant::now() + poll_timeout;
        let result = loop {
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            if remaining.is_zero() {
                return Err(iggy_connector_sdk::Error::Connection(
                    "timed out while flushing MQTT acknowledgements".to_string(),
                ));
            }

            let result = match (&mut self.connection, &self.subscriptions) {
                (MqttConnection::Mqtt311 { client, event_loop }, subscriptions) => {
                    match timeout(remaining, event_loop.poll()).await {
                        Ok(Ok(event)) => {
                            handle_mqtt311_event(
                                client,
                                event_loop,
                                event,
                                subscriptions,
                                remaining,
                                max_buffered_messages,
                            )
                            .await?
                        }
                        Ok(Err(error)) => {
                            warn!(
                                "MQTT 3.1.1 connection interrupted while flushing acknowledgements; retrying event loop: {error}"
                            );
                            sleep(RECONNECT_RETRY_DELAY).await;
                            continue;
                        }
                        Err(_) => {
                            return Err(iggy_connector_sdk::Error::Connection(
                                "timed out while flushing MQTT acknowledgements".to_string(),
                            ));
                        }
                    }
                }
                (MqttConnection::Mqtt5 { client, event_loop }, subscriptions) => {
                    match timeout(remaining, event_loop.poll()).await {
                        Ok(Ok(event)) => {
                            handle_mqtt5_event(
                                client,
                                event_loop,
                                event,
                                subscriptions,
                                remaining,
                                self.include_metadata,
                                max_buffered_messages,
                            )
                            .await?
                        }
                        Ok(Err(error)) => {
                            warn!(
                                "MQTT 5 connection interrupted while flushing acknowledgements; retrying event loop: {error}"
                            );
                            sleep(RECONNECT_RETRY_DELAY).await;
                            continue;
                        }
                        Err(_) => {
                            return Err(iggy_connector_sdk::Error::Connection(
                                "timed out while flushing MQTT acknowledgements".to_string(),
                            ));
                        }
                    }
                }
            };
            break result;
        };

        let mut received = match result {
            PollResult::Message(message) => VecDeque::from([*message]),
            PollResult::Buffered(messages) => messages,
            PollResult::None => VecDeque::new(),
        };
        if !received.is_empty() {
            // The event loop has already handed ownership of this publish to
            // the driver. Buffer it before reporting a full queue so an error
            // does not turn the received message into silent data loss.
            let buffer_full = self.buffered_messages.len() + received.len() > max_buffered_messages;
            self.buffered_messages.append(&mut received);
            if buffer_full {
                return Err(iggy_connector_sdk::Error::Connection(
                    "MQTT message buffer reached batch_size while flushing acknowledgements"
                        .to_string(),
                ));
            }
        }
        Ok(())
    }
}

fn install_rustls_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

fn tls_configuration(
    config: &MqttSourceConfig,
) -> Result<TlsConfiguration, iggy_connector_sdk::Error> {
    // A secure broker URL always gets TLS. The optional tls table supplies a
    // custom CA and client certificate, otherwise the system trust store is used.
    let (ca_file, client_cert_file, client_key_file) = match &config.tls {
        Some(tls) => (&tls.ca_file, &tls.client_cert_file, &tls.client_key_file),
        None => (&None, &None, &None),
    };

    let ca = ca_file
        .as_deref()
        .map(|path| read_certificate_file(path, "tls.ca_file"))
        .transpose()?;
    let client_auth = match (client_cert_file, client_key_file) {
        (Some(cert_path), Some(key_path)) => Some((
            read_certificate_file(cert_path, "tls.client_cert_file")?,
            read_private_key_file(key_path, "tls.client_key_file")?,
        )),
        (None, None) => None,
        _ => unreachable!("TLS client certificate and key validation occurs before connect"),
    };

    Ok(TlsConfiguration::Rustls(Arc::new(build_tls_config(
        ca,
        client_auth,
    )?)))
}

fn read_file(path: &str, field: &str) -> Result<Vec<u8>, iggy_connector_sdk::Error> {
    std::fs::read(path).map_err(|error| {
        iggy_connector_sdk::Error::InvalidConfigValue(format!("{field} could not be read: {error}"))
    })
}

fn read_certificate_file(
    path: &str,
    field: &str,
) -> Result<Vec<CertificateDer<'static>>, iggy_connector_sdk::Error> {
    let contents = read_file(path, field)?;
    let mut reader = BufReader::new(Cursor::new(&contents));
    let certificates = certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| {
            iggy_connector_sdk::Error::InvalidConfigValue(format!("{field} is invalid: {error}"))
        })?;
    if certificates.is_empty() {
        return Err(iggy_connector_sdk::Error::InvalidConfigValue(format!(
            "{field} does not contain a PEM certificate"
        )));
    }
    Ok(certificates)
}

fn read_private_key_file(
    path: &str,
    field: &str,
) -> Result<PrivateKeyDer<'static>, iggy_connector_sdk::Error> {
    let contents = read_file(path, field)?;
    let mut reader = BufReader::new(Cursor::new(&contents));
    private_key(&mut reader)
        .map_err(|error| {
            iggy_connector_sdk::Error::InvalidConfigValue(format!("{field} is invalid: {error}"))
        })?
        .ok_or_else(|| {
            iggy_connector_sdk::Error::InvalidConfigValue(format!(
                "{field} does not contain a PEM private key"
            ))
        })
}

fn system_root_cert_store() -> Result<RootCertStore, iggy_connector_sdk::Error> {
    // This path is used only when TLS is enabled without a custom CA bundle.
    let mut root_cert_store = RootCertStore::empty();
    let native_certificates = load_native_certs();
    for certificate in native_certificates.certs {
        root_cert_store.add(certificate).map_err(|error| {
            iggy_connector_sdk::Error::InvalidConfigValue(format!(
                "system TLS certificate is invalid: {error}"
            ))
        })?;
    }
    if root_cert_store.is_empty() {
        return Err(iggy_connector_sdk::Error::InvalidConfigValue(
            "no system TLS root certificates were found".to_string(),
        ));
    }
    Ok(root_cert_store)
}

fn build_tls_config(
    ca: Option<Vec<CertificateDer<'static>>>,
    client_auth: Option<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)>,
) -> Result<ClientConfig, iggy_connector_sdk::Error> {
    let root_cert_store = match ca {
        Some(ca) => {
            let mut root_cert_store = RootCertStore::empty();
            for certificate in ca {
                root_cert_store.add(certificate).map_err(|error| {
                    iggy_connector_sdk::Error::InvalidConfigValue(format!(
                        "tls.ca_file contains an invalid certificate: {error}"
                    ))
                })?;
            }
            if root_cert_store.is_empty() {
                return Err(iggy_connector_sdk::Error::InvalidConfigValue(
                    "tls.ca_file does not contain a usable certificate".to_string(),
                ));
            }
            root_cert_store
        }
        None => system_root_cert_store()?,
    };
    let client_config = ClientConfig::builder().with_root_certificates(root_cert_store);

    let Some((client_certificates, client_key)) = client_auth else {
        return Ok(client_config.with_no_client_auth());
    };

    client_config
        .with_client_auth_cert(client_certificates, client_key)
        .map_err(|error| {
            iggy_connector_sdk::Error::InvalidConfigValue(format!(
                "TLS client certificate and key do not match: {error}"
            ))
        })
}

fn client_id(config: &MqttSourceConfig, id: u32) -> String {
    config
        .client_id
        .clone()
        .unwrap_or_else(|| format!("iggy-mqtt-source-{id}"))
}

fn broker_url_with_client_id(
    config: &MqttSourceConfig,
    id: u32,
) -> Result<BrokerUrl, iggy_connector_sdk::Error> {
    // rumqttc requires client_id to be present in the URL before parse_url can
    // create MqttOptions. set_client_id below still makes the precedence
    // explicit for both protocol versions.
    let mut url = Url::parse(&config.broker_url).map_err(|error| {
        iggy_connector_sdk::Error::InvalidConfigValue(format!("broker_url: {error}"))
    })?;
    // parse_url refuses secure schemes, so a secure URL is parsed as mqtt:// on
    // its secure port and the caller must set the TLS transport itself.
    let tls = matches!(url.scheme(), "mqtts" | "ssl");
    if tls {
        if url.port().is_none() {
            url.set_port(Some(MQTTS_DEFAULT_PORT)).map_err(|()| {
                iggy_connector_sdk::Error::InvalidConfigValue("broker_url has no host".to_string())
            })?;
        }
        url.set_scheme("mqtt").map_err(|()| {
            iggy_connector_sdk::Error::InvalidConfigValue(
                "broker_url scheme cannot be normalized".to_string(),
            )
        })?;
    }
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
    Ok(BrokerUrl {
        url: url.into(),
        tls,
    })
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

fn retain_acknowledgements(pending_ack_tokens: &mut Vec<AckToken>, ack_tokens: &mut Vec<AckToken>) {
    pending_ack_tokens.append(ack_tokens);
}

fn configured_subscriptions(
    config: &MqttSourceConfig,
    default_qos: Qos,
) -> Result<Vec<MqttSubscription>, iggy_connector_sdk::Error> {
    config
        .subscriptions
        .iter()
        .map(|topic| {
            Ok(MqttSubscription {
                topic: topic.clone(),
                qos: qos_for_subscription(config, topic, default_qos)?,
            })
        })
        .collect()
}

async fn subscribe_mqtt311(
    client: &Mqtt311Client,
    subscriptions: &[MqttSubscription],
) -> Result<(), iggy_connector_sdk::Error> {
    for subscription in subscriptions {
        client
            .subscribe(&subscription.topic, subscription.qos.into())
            .await
            .map_err(|error| iggy_connector_sdk::Error::Connection(error.to_string()))?;
    }
    Ok(())
}

async fn subscribe_mqtt5(
    client: &Mqtt5Client,
    subscriptions: &[MqttSubscription],
) -> Result<(), iggy_connector_sdk::Error> {
    for subscription in subscriptions {
        client
            .subscribe(&subscription.topic, subscription.qos.into())
            .await
            .map_err(|error| iggy_connector_sdk::Error::Connection(error.to_string()))?;
    }
    Ok(())
}

async fn handle_mqtt311_event(
    client: &Mqtt311Client,
    event_loop: &mut Mqtt311EventLoop,
    event: Mqtt311Event,
    subscriptions: &[MqttSubscription],
    poll_timeout: Duration,
    max_buffered_messages: usize,
) -> Result<PollResult, iggy_connector_sdk::Error> {
    match event {
        Mqtt311Event::Incoming(Mqtt311Incoming::Publish(publish)) => {
            Ok(PollResult::Message(Box::new(normalize_mqtt311(publish)?)))
        }
        Mqtt311Event::Incoming(Mqtt311Incoming::ConnAck(connack)) if !connack.session_present => {
            subscribe_mqtt311(client, subscriptions).await?;
            Ok(PollResult::Buffered(
                poll_mqtt311(
                    event_loop,
                    subscriptions.len(),
                    poll_timeout,
                    max_buffered_messages,
                )
                .await?,
            ))
        }
        // Connection and subscription events advance the event loop but are
        // not source messages.
        Mqtt311Event::Outgoing(_) | Mqtt311Event::Incoming(_) => Ok(PollResult::None),
    }
}

async fn handle_mqtt5_event(
    client: &Mqtt5Client,
    event_loop: &mut Mqtt5EventLoop,
    event: Mqtt5Event,
    subscriptions: &[MqttSubscription],
    poll_timeout: Duration,
    include_metadata: bool,
    max_buffered_messages: usize,
) -> Result<PollResult, iggy_connector_sdk::Error> {
    match event {
        Mqtt5Event::Incoming(Mqtt5Incoming::Publish(publish)) => Ok(PollResult::Message(Box::new(
            normalize_mqtt5(publish, include_metadata)?,
        ))),
        Mqtt5Event::Incoming(Mqtt5Incoming::ConnAck(connack)) if !connack.session_present => {
            subscribe_mqtt5(client, subscriptions).await?;
            Ok(PollResult::Buffered(
                poll_mqtt5(
                    event_loop,
                    subscriptions.len(),
                    poll_timeout,
                    include_metadata,
                    max_buffered_messages,
                )
                .await?,
            ))
        }
        Mqtt5Event::Outgoing(_) | Mqtt5Event::Incoming(_) | Mqtt5Event::Auth(_) => {
            Ok(PollResult::None)
        }
    }
}

async fn poll_mqtt311(
    event_loop: &mut Mqtt311EventLoop,
    subscription_count: usize,
    poll_timeout: Duration,
    max_buffered_messages: usize,
) -> Result<VecDeque<ReceivedMessage>, iggy_connector_sdk::Error> {
    let mut acknowledged_subscriptions = 0;
    let mut buffered_messages = VecDeque::new();

    while acknowledged_subscriptions < subscription_count {
        let event = timeout(poll_timeout, event_loop.poll())
            .await
            .map_err(|_| {
                iggy_connector_sdk::Error::Connection(
                    "timed out waiting for MQTT 3.1.1 subscription acknowledgements".to_string(),
                )
            })?
            .map_err(|error| iggy_connector_sdk::Error::Connection(error.to_string()))?;

        match event {
            Mqtt311Event::Incoming(Mqtt311Incoming::SubAck(suback)) => {
                if let Some(reason) = suback.return_codes.iter().find(|reason| {
                    !matches!(
                        reason,
                        rumqttc_v4::mqttbytes::v4::SubscribeReasonCode::Success(_)
                    )
                }) {
                    return Err(iggy_connector_sdk::Error::Connection(format!(
                        "MQTT 3.1.1 broker rejected subscription (packet id {}): {reason:?}",
                        suback.pkid
                    )));
                }
                acknowledged_subscriptions += 1;
            }
            Mqtt311Event::Incoming(Mqtt311Incoming::Publish(publish)) => {
                if buffered_messages.len() >= max_buffered_messages {
                    return Err(iggy_connector_sdk::Error::Connection(
                        "MQTT resubscription buffer reached batch_size before subscription acknowledgements"
                            .to_string(),
                    ));
                }
                buffered_messages.push_back(normalize_mqtt311(publish)?);
            }
            Mqtt311Event::Outgoing(_) | Mqtt311Event::Incoming(_) => {}
        }
    }

    Ok(buffered_messages)
}

async fn mqtt5_session_present(
    event_loop: &mut Mqtt5EventLoop,
    poll_timeout: Duration,
) -> Result<bool, iggy_connector_sdk::Error> {
    loop {
        let event = timeout(poll_timeout, event_loop.poll())
            .await
            .map_err(|_| {
                iggy_connector_sdk::Error::Connection(
                    "timed out waiting for the MQTT 5 connection acknowledgement".to_string(),
                )
            })?
            .map_err(|error| iggy_connector_sdk::Error::Connection(error.to_string()))?;
        if let Mqtt5Event::Incoming(Mqtt5Incoming::ConnAck(connack)) = event {
            return Ok(connack.session_present);
        }
    }
}

async fn poll_mqtt5(
    event_loop: &mut Mqtt5EventLoop,
    subscription_count: usize,
    poll_timeout: Duration,
    include_metadata: bool,
    max_buffered_messages: usize,
) -> Result<VecDeque<ReceivedMessage>, iggy_connector_sdk::Error> {
    let mut acknowledged_subscriptions = 0;
    let mut buffered_messages = VecDeque::new();

    while acknowledged_subscriptions < subscription_count {
        let event = timeout(poll_timeout, event_loop.poll())
            .await
            .map_err(|_| {
                iggy_connector_sdk::Error::Connection(
                    "timed out waiting for MQTT 5 subscription acknowledgements".to_string(),
                )
            })?
            .map_err(|error| iggy_connector_sdk::Error::Connection(error.to_string()))?;

        match event {
            Mqtt5Event::Incoming(Mqtt5Incoming::SubAck(suback)) => {
                if let Some(reason) = suback.return_codes.iter().find(|reason| {
                    !matches!(
                        reason,
                        rumqttc_v5::mqttbytes::v5::SubscribeReasonCode::Success(_)
                    )
                }) {
                    return Err(iggy_connector_sdk::Error::Connection(format!(
                        "MQTT 5 broker rejected subscription (packet id {}): {reason:?}",
                        suback.pkid
                    )));
                }
                acknowledged_subscriptions += 1;
            }
            Mqtt5Event::Incoming(Mqtt5Incoming::Publish(publish)) => {
                if buffered_messages.len() >= max_buffered_messages {
                    return Err(iggy_connector_sdk::Error::Connection(
                        "MQTT resubscription buffer reached batch_size before subscription acknowledgements"
                            .to_string(),
                    ));
                }
                buffered_messages.push_back(normalize_mqtt5(publish, include_metadata)?);
            }
            Mqtt5Event::Outgoing(_) | Mqtt5Event::Incoming(_) | Mqtt5Event::Auth(_) => {}
        }
    }

    Ok(buffered_messages)
}

fn normalize_mqtt311(
    publish: Mqtt311Publish,
) -> Result<ReceivedMessage, iggy_connector_sdk::Error> {
    // Normalize both MQTT protocol versions into one source representation. Only
    // the acknowledgement token type remains protocol-specific.
    let qos = publish.qos.into();
    let metadata = MqttMessageMetadata {
        qos,
        packet_id: (qos != Qos::Zero).then_some(publish.pkid),
        dup: publish.dup,
        retain: publish.retain,
    };
    let topic = topic_string(&publish.topic)?;
    let headers = metadata_headers("mqtt311", &topic, metadata)?;
    let message = MqttMessage {
        payload: publish.payload.to_vec(),
        topic,
        headers,
        metadata,
        mqtt5_properties: None,
    };
    let ack_token = match publish.qos {
        MqttQos::AtMostOnce => None,
        qos => Some(AckToken::Mqtt311 {
            qos,
            packet_id: publish.pkid,
        }),
    };
    Ok(ReceivedMessage { message, ack_token })
}

pub(crate) fn normalize_mqtt5(
    publish: Mqtt5Publish,
    include_metadata: bool,
) -> Result<ReceivedMessage, iggy_connector_sdk::Error> {
    // MQTT 5 properties are needed only for the optional JSON envelope.
    let qos = publish.qos.into();
    let metadata = MqttMessageMetadata {
        qos,
        packet_id: (qos != Qos::Zero).then_some(publish.pkid),
        dup: publish.dup,
        retain: publish.retain,
    };
    let topic = topic_string(&publish.topic)?;
    let headers = metadata_headers("mqtt5", &topic, metadata)?;
    let mqtt5_properties = include_metadata
        .then(|| {
            publish
                .properties
                .as_ref()
                .map(Mqtt5EnvelopeProperties::from)
        })
        .flatten();
    let message = MqttMessage {
        payload: publish.payload.to_vec(),
        topic,
        headers,
        metadata,
        mqtt5_properties,
    };
    let ack_token = match publish.qos {
        MqttQos::AtMostOnce => None,
        qos => Some(AckToken::Mqtt5 {
            qos,
            packet_id: publish.pkid,
        }),
    };
    Ok(ReceivedMessage { message, ack_token })
}

impl From<&rumqttc_v5::mqttbytes::v5::PublishProperties> for Mqtt5EnvelopeProperties {
    fn from(properties: &rumqttc_v5::mqttbytes::v5::PublishProperties) -> Self {
        Self {
            payload_format_indicator: properties.payload_format_indicator,
            message_expiry_interval: properties.message_expiry_interval,
            topic_alias: properties.topic_alias,
            response_topic: properties.response_topic.clone(),
            correlation_data_base64: properties
                .correlation_data
                .as_ref()
                .map(|value| base64::engine::general_purpose::STANDARD.encode(value)),
            user_properties: properties
                .user_properties
                .iter()
                .map(|(key, value)| Mqtt5UserProperty {
                    key: key.clone(),
                    value: value.clone(),
                })
                .collect(),
            subscription_identifiers: properties.subscription_identifiers.clone(),
            content_type: properties.content_type.clone(),
        }
    }
}

fn metadata_headers(
    protocol: &str,
    topic: &str,
    metadata: MqttMessageMetadata,
) -> Result<BTreeMap<HeaderKey, HeaderValue>, iggy_connector_sdk::Error> {
    // Stable lowercase header names let consumers inspect MQTT metadata without
    // decoding the original MQTT packet again.
    let mut headers = BTreeMap::new();
    insert_string_header(
        &mut headers,
        &MQTT_PROTOCOL_HEADER_KEY,
        MQTT_PROTOCOL_HEADER,
        protocol,
    )?;
    insert_string_header(
        &mut headers,
        &MQTT_TOPIC_HEADER_KEY,
        MQTT_TOPIC_HEADER,
        topic,
    )?;
    headers.insert(MQTT_QOS_HEADER_KEY.clone(), qos_value(metadata.qos).into());
    headers.insert(MQTT_DUP_HEADER_KEY.clone(), metadata.dup.into());
    headers.insert(MQTT_RETAIN_HEADER_KEY.clone(), metadata.retain.into());
    Ok(headers)
}

fn insert_string_header(
    headers: &mut BTreeMap<HeaderKey, HeaderValue>,
    key: &HeaderKey,
    name: &str,
    value: &str,
) -> Result<(), iggy_connector_sdk::Error> {
    if value.is_empty() {
        warn_omitted_header(name, "empty_value", value.len());
        return Ok(());
    }
    if value.len() > MAX_HEADER_VALUE_LENGTH {
        warn_omitted_header(name, "value_too_long", value.len());
        return Ok(());
    }

    let header_value = HeaderValue::try_from(value).map_err(|error| {
        iggy_connector_sdk::Error::Serialization(format!(
            "invalid MQTT header value for {name}: {error}"
        ))
    })?;
    headers.insert(key.clone(), header_value);
    Ok(())
}

fn warn_omitted_header(name: &str, reason: &str, value_length: usize) {
    warn!(
        connector = "mqtt",
        header = name,
        reason,
        value_length,
        max_length = MAX_HEADER_VALUE_LENGTH,
        "MQTT metadata header omitted"
    );
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

impl From<Qos> for MqttQos {
    fn from(qos: Qos) -> Self {
        match qos {
            Qos::Zero => Self::AtMostOnce,
            Qos::One => Self::AtLeastOnce,
            Qos::Two => Self::ExactlyOnce,
        }
    }
}

impl From<MqttQos> for Qos {
    fn from(qos: MqttQos) -> Self {
        match qos {
            MqttQos::AtMostOnce => Self::Zero,
            MqttQos::AtLeastOnce => Self::One,
            MqttQos::ExactlyOnce => Self::Two,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rumqttc_v5::mqttbytes::v5::PublishProperties;
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    // First byte of a TLS record carrying a handshake, as opposed to 0x10 for an MQTT CONNECT.
    const TLS_HANDSHAKE_RECORD: u8 = 0x16;

    #[test]
    fn given_qos_should_convert_to_mqtt_qos() {
        assert_eq!(MqttQos::AtMostOnce, Qos::Zero.into());
        assert_eq!(MqttQos::AtLeastOnce, Qos::One.into());
        assert_eq!(MqttQos::ExactlyOnce, Qos::Two.into());
    }

    #[test]
    fn given_mqtt311_qos_zero_should_not_create_ack_token() {
        let publish = Mqtt311Publish::new("devices/test", MqttQos::AtMostOnce, b"payload");
        let received = normalize_mqtt311(publish).expect("MQTT 3.1.1 publish should normalize");

        assert!(received.ack_token.is_none());
        assert_eq!(received.message.metadata.qos, Qos::Zero);
        assert_eq!(received.message.metadata.packet_id, None);
    }

    #[test]
    fn given_mqtt311_publish_should_map_metadata_to_headers() {
        let mut publish = Mqtt311Publish::new("devices/test", MqttQos::AtLeastOnce, b"payload");
        publish.dup = true;
        publish.retain = true;

        let received = normalize_mqtt311(publish).expect("MQTT 3.1.1 publish should normalize");
        let headers = &received.message.headers;

        assert_eq!(
            headers[&MQTT_PROTOCOL_HEADER_KEY].as_str().unwrap(),
            "mqtt311"
        );
        assert_eq!(
            headers[&MQTT_TOPIC_HEADER_KEY].as_str().unwrap(),
            "devices/test"
        );
        assert_eq!(headers[&MQTT_QOS_HEADER_KEY].as_uint8().unwrap(), 1);
        assert!(!headers.contains_key(&MQTT_PACKET_ID_HEADER_KEY));
        assert!(headers[&MQTT_DUP_HEADER_KEY].as_bool().unwrap());
        assert!(headers[&MQTT_RETAIN_HEADER_KEY].as_bool().unwrap());
    }

    #[test]
    fn given_oversized_topic_should_preserve_message_and_omit_topic_header() {
        let topic = "t".repeat(MAX_HEADER_VALUE_LENGTH + 1);
        let publish = Mqtt311Publish::new(&topic, MqttQos::AtMostOnce, b"payload");

        let received = normalize_mqtt311(publish).expect("oversized topic should be tolerated");

        assert_eq!(received.message.payload, b"payload");
        assert!(
            !received
                .message
                .headers
                .contains_key(&MQTT_TOPIC_HEADER_KEY)
        );
        assert!(
            received
                .message
                .headers
                .contains_key(&MQTT_PROTOCOL_HEADER_KEY)
        );
    }

    #[test]
    fn given_empty_mqtt5_property_should_omit_only_that_header() {
        let properties = PublishProperties {
            response_topic: Some(String::new()),
            ..Default::default()
        };
        let publish = Mqtt5Publish::new(
            "devices/test",
            MqttQos::AtMostOnce,
            b"payload".as_slice(),
            Some(properties),
        );

        let received = normalize_mqtt5(publish, true).expect("empty property should be tolerated");

        assert_eq!(received.message.payload, b"payload");
        assert!(
            !received
                .message
                .headers
                .contains_key(&MQTT_RESPONSE_TOPIC_HEADER_KEY)
        );
        assert!(
            received
                .message
                .headers
                .contains_key(&MQTT_TOPIC_HEADER_KEY)
        );
    }

    #[test]
    fn given_mqtt5_qos_two_should_create_deferred_ack_token() {
        let mut publish = Mqtt5Publish::new(
            "devices/test",
            MqttQos::ExactlyOnce,
            b"payload".as_slice(),
            None,
        );
        publish.pkid = 42;
        let received = normalize_mqtt5(publish, false).expect("MQTT 5 publish should normalize");

        assert!(matches!(
            received.ack_token,
            Some(AckToken::Mqtt5 {
                qos: MqttQos::ExactlyOnce,
                packet_id: 42,
            })
        ));
        assert_eq!(received.message.metadata.qos, Qos::Two);
        assert_eq!(received.message.metadata.packet_id, Some(42));
    }

    #[test]
    fn given_mqtt311_qos_one_should_create_deferred_ack_token() {
        let mut publish = Mqtt311Publish::new("devices/test", MqttQos::AtLeastOnce, b"payload");
        publish.pkid = 17;
        let received = normalize_mqtt311(publish).expect("MQTT 3.1.1 publish should normalize");

        assert!(matches!(
            received.ack_token,
            Some(AckToken::Mqtt311 {
                qos: MqttQos::AtLeastOnce,
                packet_id: 17,
            })
        ));
        assert_eq!(received.message.metadata.qos, Qos::One);
        assert_eq!(received.message.metadata.packet_id, Some(17));
    }

    #[test]
    fn given_mqtt311_qos_two_should_create_deferred_ack_token() {
        let publish = Mqtt311Publish::new("devices/test", MqttQos::ExactlyOnce, b"payload");
        let received = normalize_mqtt311(publish).expect("MQTT 3.1.1 publish should normalize");

        assert!(received.ack_token.is_some());
        assert_eq!(received.message.metadata.qos, Qos::Two);
        assert_eq!(received.message.metadata.packet_id, Some(0));
    }

    #[test]
    fn given_unsent_acknowledgements_should_retain_them_in_order() {
        let mut first_publish =
            Mqtt311Publish::new("devices/first", MqttQos::AtLeastOnce, b"first");
        first_publish.pkid = 1;
        let first = normalize_mqtt311(first_publish)
            .expect("first MQTT publish should normalize")
            .ack_token
            .expect("QoS 1 publish should have an acknowledgement token");
        let mut second_publish =
            Mqtt311Publish::new("devices/second", MqttQos::AtLeastOnce, b"second");
        second_publish.pkid = 2;
        let second = normalize_mqtt311(second_publish)
            .expect("second MQTT publish should normalize")
            .ack_token
            .expect("QoS 1 publish should have an acknowledgement token");
        let mut pending_ack_tokens = vec![first];
        let mut unsent_ack_tokens = vec![second];

        retain_acknowledgements(&mut pending_ack_tokens, &mut unsent_ack_tokens);

        assert!(unsent_ack_tokens.is_empty());
        assert_eq!(pending_ack_tokens.len(), 2);
        assert!(matches!(
            pending_ack_tokens[0],
            AckToken::Mqtt311 { packet_id: 1, .. }
        ));
        assert!(matches!(
            pending_ack_tokens[1],
            AckToken::Mqtt311 { packet_id: 2, .. }
        ));
    }

    #[test]
    fn given_mqtt5_qos_zero_should_not_create_ack_token() {
        let publish = Mqtt5Publish::new(
            "devices/test",
            MqttQos::AtMostOnce,
            b"payload".as_slice(),
            None,
        );
        let received = normalize_mqtt5(publish, false).expect("MQTT 5 publish should normalize");

        assert!(received.ack_token.is_none());
        assert_eq!(received.message.metadata.qos, Qos::Zero);
        assert_eq!(received.message.metadata.packet_id, None);
    }

    #[test]
    fn given_mqtt5_qos_one_should_create_deferred_ack_token() {
        let publish = Mqtt5Publish::new(
            "devices/test",
            MqttQos::AtLeastOnce,
            b"payload".as_slice(),
            None,
        );
        let received = normalize_mqtt5(publish, false).expect("MQTT 5 publish should normalize");

        assert!(received.ack_token.is_some());
        assert_eq!(received.message.metadata.qos, Qos::One);
        assert_eq!(received.message.metadata.packet_id, Some(0));
    }

    #[test]
    fn given_mqtt5_publish_properties_should_be_retained_for_envelope() {
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
            MqttQos::AtLeastOnce,
            b"payload".as_slice(),
            Some(properties),
        );

        let received = normalize_mqtt5(publish, true).expect("MQTT 5 publish should normalize");
        let properties = received
            .message
            .mqtt5_properties
            .expect("MQTT 5 properties should be retained");
        assert_eq!(properties.payload_format_indicator, Some(1));
        assert_eq!(properties.message_expiry_interval, Some(30));
        assert_eq!(properties.topic_alias, Some(4));
        assert_eq!(
            properties.response_topic.as_deref(),
            Some("devices/response")
        );
        assert_eq!(
            properties.correlation_data_base64.as_deref(),
            Some("Y29ycmVsYXRpb24=")
        );
        assert_eq!(properties.user_properties[0].key, "device_id");
        assert_eq!(properties.user_properties[0].value, "device-1");
        assert_eq!(properties.subscription_identifiers, vec![7, 9]);
        assert_eq!(properties.content_type.as_deref(), Some("application/json"));
        assert_eq!(received.message.headers.len(), 5);
    }

    #[test]
    fn given_mqtt5_metadata_is_disabled_should_not_convert_properties() {
        let properties = PublishProperties {
            correlation_data: Some(b"correlation".as_slice().into()),
            user_properties: vec![("device_id".to_string(), "device-1".to_string())],
            ..Default::default()
        };
        let publish = Mqtt5Publish::new(
            "devices/test",
            MqttQos::AtLeastOnce,
            b"payload".as_slice(),
            Some(properties),
        );

        let received = normalize_mqtt5(publish, false).expect("MQTT 5 publish should normalize");

        assert!(received.message.mqtt5_properties.is_none());
    }

    #[test]
    fn given_invalid_broker_url_should_reject_before_connecting() {
        let config = MqttSourceConfig {
            broker_url: "not a mqtt url".to_string(),
            subscriptions: vec!["devices/test".to_string()],
            subscription_qos: BTreeMap::new(),
            protocol: MqttProtocol::Mqtt5,
            include_metadata: false,
            qos: 1,
            tls: None,
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
            max_retries: None,
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
            3,
        ));

        assert!(matches!(
            result,
            Err(iggy_connector_sdk::Error::InvalidConfigValue(message))
                if message.starts_with("broker_url:")
        ));
    }

    #[test]
    fn given_missing_tls_ca_file_should_reject_before_connecting() {
        let config = MqttSourceConfig {
            broker_url: "mqtts://127.0.0.1:8883".to_string(),
            subscriptions: vec!["devices/test".to_string()],
            subscription_qos: BTreeMap::new(),
            protocol: MqttProtocol::Mqtt5,
            include_metadata: false,
            qos: 1,
            tls: Some(super::super::MqttTlsConfig {
                ca_file: Some("/path/that/does/not/exist.pem".to_string()),
                client_cert_file: None,
                client_key_file: None,
            }),
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
            max_retries: None,
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
            3,
        ));

        assert!(matches!(
            result,
            Err(iggy_connector_sdk::Error::InvalidConfigValue(message))
                if message.starts_with("tls.ca_file could not be read:")
        ));
    }

    #[test]
    fn given_secure_broker_url_without_tls_table_should_open_with_a_tls_handshake() {
        let runtime = tokio::runtime::Runtime::new().expect("runtime should start");
        runtime.block_on(async {
            let listener = TcpListener::bind("127.0.0.1:0")
                .await
                .expect("listener should bind");
            let port = listener
                .local_addr()
                .expect("listener has an address")
                .port();
            for (protocol, scheme) in [
                (MqttProtocol::Mqtt5, "mqtts"),
                (MqttProtocol::Mqtt311, "ssl"),
            ] {
                let config = MqttSourceConfig {
                    broker_url: format!("{scheme}://127.0.0.1:{port}"),
                    subscriptions: vec!["devices/test".to_string()],
                    subscription_qos: BTreeMap::new(),
                    protocol,
                    include_metadata: false,
                    qos: 1,
                    tls: None,
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
                    max_retries: None,
                    verbose_logging: None,
                };
                let connecting = tokio::spawn(async move {
                    MqttDriver::connect(
                        7,
                        &config,
                        Qos::One,
                        Duration::from_secs(5),
                        Duration::from_millis(100),
                        4,
                        3,
                    )
                    .await
                });
                let (mut stream, _) = timeout(Duration::from_secs(5), listener.accept())
                    .await
                    .expect("the source should dial the broker")
                    .expect("the connection should be accepted");
                let mut first_byte = [0u8; 1];
                stream
                    .read_exact(&mut first_byte)
                    .await
                    .expect("the source should send its first byte");
                connecting.abort();
                assert_eq!(
                    first_byte[0], TLS_HANDSHAKE_RECORD,
                    "{protocol:?} over {scheme}"
                );
            }
        });
    }

    #[test]
    fn given_broker_url_without_client_id_should_add_configured_client_id() {
        let config = MqttSourceConfig {
            broker_url: "mqtt://127.0.0.1:1883?keep_alive_secs=30".to_string(),
            subscriptions: vec!["devices/test".to_string()],
            subscription_qos: BTreeMap::new(),
            protocol: MqttProtocol::Mqtt5,
            include_metadata: false,
            qos: 1,
            tls: None,
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
            max_retries: None,
            verbose_logging: None,
        };

        let broker_url = broker_url_with_client_id(&config, 7).expect("URL should be valid");
        let parsed = Url::parse(&broker_url.url).expect("URL should parse");
        let query = parsed.query_pairs().collect::<Vec<_>>();

        assert!(!broker_url.tls);
        assert!(query.contains(&("keep_alive_secs".into(), "30".into())));
        assert!(query.contains(&("client_id".into(), "configured-client".into())));
    }

    #[test]
    fn given_secure_broker_url_should_parse_as_mqtt_on_its_secure_port_with_tls() {
        for (broker_url, expected_port) in [
            ("mqtts://broker.example", 8883),
            ("ssl://broker.example:9883", 9883),
        ] {
            let config = MqttSourceConfig {
                broker_url: broker_url.to_string(),
                subscriptions: vec!["devices/test".to_string()],
                subscription_qos: BTreeMap::new(),
                protocol: MqttProtocol::Mqtt5,
                include_metadata: false,
                qos: 1,
                tls: None,
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
                max_retries: None,
                verbose_logging: None,
            };

            let normalized = broker_url_with_client_id(&config, 7).expect("URL should be valid");
            let parsed = Url::parse(&normalized.url).expect("URL should parse");

            assert!(normalized.tls, "{broker_url}");
            assert_eq!(
                (parsed.scheme(), parsed.host_str(), parsed.port()),
                ("mqtt", Some("broker.example"), Some(expected_port)),
                "{broker_url}"
            );
        }
    }
}
