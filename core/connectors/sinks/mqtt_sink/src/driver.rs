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

use rumqttc::tokio_rustls::rustls;
use rumqttc::tokio_rustls::rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer},
};
use rumqttc::v5::{
    AsyncClient as Mqtt5Client, ConnectionError as Mqtt5ConnectionError, Event as Mqtt5Event,
    EventLoop as Mqtt5EventLoop, Incoming as Mqtt5Incoming, MqttOptions as Mqtt5Options,
    mqttbytes::v5::{
        ConnectReturnCode as Mqtt5ConnectReturnCode, PubAckReason, PubCompReason, PubRecReason,
    },
};
use rumqttc::{
    AsyncClient as Mqtt311Client, ConnectionError as Mqtt311ConnectionError, Event as Mqtt311Event,
    EventLoop as Mqtt311EventLoop, Incoming as Mqtt311Incoming, MqttOptions as Mqtt311Options,
    Outgoing, TlsConfiguration, Transport,
};
use rustls_native_certs::load_native_certs;
use rustls_pemfile::{certs, private_key};
use secrecy::ExposeSecret;
use serde::Deserialize;
use std::{
    collections::HashMap,
    fs,
    io::{BufReader, Cursor},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::time::timeout;

use crate::{MqttSinkConfig, routing::PreparedPublication};

/// MQTT protocol selection is kept in the plugin because rumqttc exposes
/// separate client/event-loop types for MQTT 3.1.1 and MQTT 5.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum MqttProtocol {
    #[serde(rename = "mqtt311")]
    Mqtt311,
    #[default]
    #[serde(rename = "mqtt5")]
    Mqtt5,
}

pub struct MqttDriver {
    connection: MqttConnection,
}

pub struct PublishWindowError {
    pub confirmed: Vec<usize>,
    pub error: iggy_connector_sdk::Error,
    pub retryable: bool,
}

enum DriverEvent {
    OutgoingPublish(u16),
    PubAck(u16),
    PubComp(u16),
    Mqtt5PubAck {
        packet_id: u16,
        reason: PubAckReason,
    },
    Mqtt5PubRec {
        packet_id: u16,
        reason: PubRecReason,
    },
    Mqtt5PubComp {
        packet_id: u16,
        reason: PubCompReason,
    },
    Other,
}

#[derive(Debug)]
enum PublishWindowFailure {
    BrokerRejected(String),
    Protocol(String),
}

impl PublishWindowFailure {
    fn error(self) -> iggy_connector_sdk::Error {
        match self {
            Self::BrokerRejected(message) => iggy_connector_sdk::Error::CannotStoreData(message),
            Self::Protocol(message) => iggy_connector_sdk::Error::Connection(message),
        }
    }
}

struct PublishWindowState {
    qos: rumqttc::QoS,
    next_submitted_index: usize,
    packet_to_index: HashMap<u16, usize>,
    confirmed: Vec<bool>,
}

impl PublishWindowState {
    fn new(length: usize, qos: rumqttc::QoS) -> Self {
        Self {
            qos,
            next_submitted_index: 0,
            packet_to_index: HashMap::with_capacity(length),
            confirmed: vec![false; length],
        }
    }

    fn is_complete(&self) -> bool {
        self.confirmed.iter().all(|confirmed| *confirmed)
    }

    fn apply(&mut self, event: DriverEvent) -> Result<(), PublishWindowFailure> {
        match event {
            DriverEvent::OutgoingPublish(packet_id) => {
                if self.next_submitted_index == self.confirmed.len() {
                    return Err(PublishWindowFailure::Protocol(
                        "received an unexpected MQTT publish packet".to_string(),
                    ));
                }
                let index = self.next_submitted_index;
                self.next_submitted_index += 1;
                if self.qos == rumqttc::QoS::AtMostOnce {
                    self.confirmed[index] = true;
                } else {
                    self.packet_to_index.insert(packet_id, index);
                }
            }
            DriverEvent::PubAck(packet_id) if self.qos == rumqttc::QoS::AtLeastOnce => {
                if let Some(index) = self.packet_to_index.remove(&packet_id) {
                    self.confirmed[index] = true;
                }
            }
            DriverEvent::PubComp(packet_id) if self.qos == rumqttc::QoS::ExactlyOnce => {
                if let Some(index) = self.packet_to_index.remove(&packet_id) {
                    self.confirmed[index] = true;
                }
            }
            DriverEvent::Mqtt5PubAck { packet_id, reason }
                if self.qos == rumqttc::QoS::AtLeastOnce =>
            {
                if let Some(index) = self.packet_to_index.remove(&packet_id) {
                    if matches!(
                        reason,
                        PubAckReason::Success | PubAckReason::NoMatchingSubscribers
                    ) {
                        self.confirmed[index] = true;
                    } else {
                        return Err(PublishWindowFailure::BrokerRejected(format!(
                            "MQTT 5 PUBACK rejected packet ID {packet_id}: {reason:?}"
                        )));
                    }
                }
            }
            DriverEvent::Mqtt5PubRec { packet_id, reason }
                if self.qos == rumqttc::QoS::ExactlyOnce =>
            {
                if !matches!(
                    reason,
                    PubRecReason::Success | PubRecReason::NoMatchingSubscribers
                ) && self.packet_to_index.remove(&packet_id).is_some()
                {
                    return Err(PublishWindowFailure::BrokerRejected(format!(
                        "MQTT 5 PUBREC rejected packet ID {packet_id}: {reason:?}"
                    )));
                }
            }
            DriverEvent::Mqtt5PubComp { packet_id, reason }
                if self.qos == rumqttc::QoS::ExactlyOnce =>
            {
                if let Some(index) = self.packet_to_index.remove(&packet_id) {
                    if reason == PubCompReason::Success {
                        self.confirmed[index] = true;
                    } else {
                        return Err(PublishWindowFailure::BrokerRejected(format!(
                            "MQTT 5 PUBCOMP rejected packet ID {packet_id}: {reason:?}"
                        )));
                    }
                }
            }
            DriverEvent::PubAck(_)
            | DriverEvent::PubComp(_)
            | DriverEvent::Mqtt5PubAck { .. }
            | DriverEvent::Mqtt5PubRec { .. }
            | DriverEvent::Mqtt5PubComp { .. }
            | DriverEvent::Other => {}
        }
        Ok(())
    }

    fn confirmed_indexes(&self) -> Vec<usize> {
        confirmed_indexes(&self.confirmed)
    }
}

enum MqttConnection {
    Mqtt311 {
        client: Mqtt311Client,
        event_loop: Box<Mqtt311EventLoop>,
    },
    Mqtt5 {
        client: Mqtt5Client,
        event_loop: Box<Mqtt5EventLoop>,
    },
}

impl std::fmt::Debug for MqttConnection {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MqttConnection")
            .field("protocol", &self.protocol())
            .finish_non_exhaustive()
    }
}

impl MqttConnection {
    fn protocol(&self) -> MqttProtocol {
        match self {
            Self::Mqtt311 { .. } => MqttProtocol::Mqtt311,
            Self::Mqtt5 { .. } => MqttProtocol::Mqtt5,
        }
    }
}

impl std::fmt::Debug for MqttDriver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MqttDriver")
            .field("connection", &self.connection)
            .finish()
    }
}

impl MqttDriver {
    pub async fn connect(
        id: u32,
        config: &MqttSinkConfig,
        client_id: String,
        keep_alive: Duration,
        connect_timeout: Duration,
        request_capacity: usize,
        max_in_flight: usize,
    ) -> Result<Self, iggy_connector_sdk::Error> {
        install_rustls_provider();
        if request_capacity == 0 {
            return Err(iggy_connector_sdk::Error::InvalidConfigValue(
                "request_capacity must be greater than zero".to_string(),
            ));
        }

        let max_in_flight = u16::try_from(max_in_flight).map_err(|_| {
            iggy_connector_sdk::Error::InvalidConfigValue(
                "max_in_flight must fit into an MQTT packet identifier window".to_string(),
            )
        })?;

        let connection = match config.protocol {
            MqttProtocol::Mqtt311 => {
                let options_url = broker_url_with_client_id(&config.broker_url, &client_id)?;
                let mut options = Mqtt311Options::parse_url(options_url).map_err(|error| {
                    iggy_connector_sdk::Error::InvalidConfigValue(format!("broker_url: {error}"))
                })?;
                options
                    .set_client_id(client_id)
                    .set_keep_alive(keep_alive)
                    .set_request_channel_capacity(request_capacity)
                    .set_inflight(max_in_flight)
                    .set_clean_session(config.clean_start);
                if let (Some(username), Some(password)) =
                    (config.username.as_deref(), config.password.as_ref())
                {
                    options.set_credentials(username, password.expose_secret());
                }
                if let Some(transport) = tls_transport(config)? {
                    options.set_transport(transport);
                }
                let (client, mut event_loop) = Mqtt311Client::new(options, request_capacity);
                wait_for_connack(&mut event_loop, id, connect_timeout).await?;
                MqttConnection::Mqtt311 {
                    client,
                    event_loop: Box::new(event_loop),
                }
            }
            MqttProtocol::Mqtt5 => {
                let options_url = broker_url_with_client_id(&config.broker_url, &client_id)?;
                let mut options = Mqtt5Options::parse_url(options_url).map_err(|error| {
                    iggy_connector_sdk::Error::InvalidConfigValue(format!("broker_url: {error}"))
                })?;
                options
                    .set_client_id(client_id)
                    .set_keep_alive(keep_alive)
                    .set_request_channel_capacity(request_capacity)
                    .set_outgoing_inflight_upper_limit(max_in_flight)
                    .set_clean_start(config.clean_start)
                    .set_session_expiry_interval(config.session_expiry_interval);
                if let (Some(username), Some(password)) =
                    (config.username.as_deref(), config.password.as_ref())
                {
                    options.set_credentials(username, password.expose_secret());
                }
                if let Some(transport) = tls_transport(config)? {
                    options.set_transport(transport);
                }
                let (client, mut event_loop) = Mqtt5Client::new(options, request_capacity);
                wait_for_connack_v5(&mut event_loop, id, connect_timeout).await?;
                MqttConnection::Mqtt5 {
                    client,
                    event_loop: Box::new(event_loop),
                }
            }
        };

        Ok(Self { connection })
    }

    pub async fn publish_window(
        &mut self,
        publications: &[&PreparedPublication],
        qos: rumqttc::QoS,
        retain: bool,
        publish_timeout: Duration,
    ) -> Result<Vec<usize>, PublishWindowError> {
        if publications.is_empty() {
            return Ok(Vec::new());
        }

        // AsyncClient::publish queues requests in order but does not return
        // packet identifiers. Outgoing::Publish assigns them in that order.
        let mut state = PublishWindowState::new(publications.len(), qos);
        for &publication in publications {
            if let Err(error) = self.enqueue_publication(publication, qos, retain).await {
                return Err(PublishWindowError {
                    confirmed: state.confirmed_indexes(),
                    error,
                    retryable: true,
                });
            }
        }

        let deadline = Instant::now() + publish_timeout;
        while !state.is_complete() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(PublishWindowError {
                    confirmed: state.confirmed_indexes(),
                    error: iggy_connector_sdk::Error::Connection(
                        "timed out waiting for MQTT publication acknowledgements".to_string(),
                    ),
                    retryable: true,
                });
            }
            let event = match timeout(remaining, self.poll_event()).await {
                Ok(Ok(event)) => event,
                Ok(Err(error)) => {
                    return Err(PublishWindowError {
                        confirmed: state.confirmed_indexes(),
                        error,
                        retryable: true,
                    });
                }
                Err(_) => {
                    return Err(PublishWindowError {
                        confirmed: state.confirmed_indexes(),
                        error: iggy_connector_sdk::Error::Connection(
                            "timed out waiting for MQTT publication acknowledgements".to_string(),
                        ),
                        retryable: true,
                    });
                }
            };

            if let Err(error) = state.apply(event) {
                return Err(PublishWindowError {
                    confirmed: state.confirmed_indexes(),
                    error: error.error(),
                    retryable: false,
                });
            }
        }

        Ok(state.confirmed_indexes())
    }

    async fn enqueue_publication(
        &mut self,
        publication: &PreparedPublication,
        qos: rumqttc::QoS,
        retain: bool,
    ) -> Result<(), iggy_connector_sdk::Error> {
        match &mut self.connection {
            MqttConnection::Mqtt311 { client, .. } => client
                .publish(
                    publication.topic.as_str(),
                    qos,
                    retain,
                    publication.payload.clone(),
                )
                .await
                .map_err(|error| iggy_connector_sdk::Error::Connection(error.to_string())),
            MqttConnection::Mqtt5 { client, .. } => {
                let result = if let Some(properties) = publication.properties.clone() {
                    client
                        .publish_with_properties(
                            publication.topic.as_str(),
                            mqtt5_qos(qos),
                            retain,
                            publication.payload.clone(),
                            properties,
                        )
                        .await
                } else {
                    client
                        .publish(
                            publication.topic.as_str(),
                            mqtt5_qos(qos),
                            retain,
                            publication.payload.clone(),
                        )
                        .await
                };
                result.map_err(|error| iggy_connector_sdk::Error::Connection(error.to_string()))
            }
        }
    }

    async fn poll_event(&mut self) -> Result<DriverEvent, iggy_connector_sdk::Error> {
        match &mut self.connection {
            MqttConnection::Mqtt311 { event_loop, .. } => match event_loop
                .poll()
                .await
                .map_err(|error| iggy_connector_sdk::Error::Connection(error.to_string()))?
            {
                Mqtt311Event::Outgoing(Outgoing::Publish(packet_id)) => {
                    Ok(DriverEvent::OutgoingPublish(packet_id))
                }
                Mqtt311Event::Incoming(Mqtt311Incoming::PubAck(ack)) => {
                    Ok(DriverEvent::PubAck(ack.pkid))
                }
                Mqtt311Event::Incoming(Mqtt311Incoming::PubComp(comp)) => {
                    Ok(DriverEvent::PubComp(comp.pkid))
                }
                Mqtt311Event::Incoming(_) | Mqtt311Event::Outgoing(_) => Ok(DriverEvent::Other),
            },
            MqttConnection::Mqtt5 { event_loop, .. } => match event_loop
                .poll()
                .await
                .map_err(|error| iggy_connector_sdk::Error::Connection(error.to_string()))?
            {
                Mqtt5Event::Outgoing(Outgoing::Publish(packet_id)) => {
                    Ok(DriverEvent::OutgoingPublish(packet_id))
                }
                Mqtt5Event::Incoming(Mqtt5Incoming::PubAck(ack)) => Ok(DriverEvent::Mqtt5PubAck {
                    packet_id: ack.pkid,
                    reason: ack.reason,
                }),
                Mqtt5Event::Incoming(Mqtt5Incoming::PubRec(rec)) => Ok(DriverEvent::Mqtt5PubRec {
                    packet_id: rec.pkid,
                    reason: rec.reason,
                }),
                Mqtt5Event::Incoming(Mqtt5Incoming::PubComp(comp)) => {
                    Ok(DriverEvent::Mqtt5PubComp {
                        packet_id: comp.pkid,
                        reason: comp.reason,
                    })
                }
                Mqtt5Event::Incoming(_) | Mqtt5Event::Outgoing(_) => Ok(DriverEvent::Other),
            },
        }
    }

    pub async fn disconnect(
        &mut self,
        disconnect_timeout: Duration,
    ) -> Result<(), iggy_connector_sdk::Error> {
        match &mut self.connection {
            MqttConnection::Mqtt311 { client, event_loop } => {
                client
                    .disconnect()
                    .await
                    .map_err(|error| iggy_connector_sdk::Error::Connection(error.to_string()))?;
                let _ = timeout(disconnect_timeout, event_loop.poll()).await;
            }
            MqttConnection::Mqtt5 { client, event_loop } => {
                client
                    .disconnect()
                    .await
                    .map_err(|error| iggy_connector_sdk::Error::Connection(error.to_string()))?;
                let _ = timeout(disconnect_timeout, event_loop.poll()).await;
            }
        }
        Ok(())
    }
}

pub(crate) fn validate_tls_config(
    config: &MqttSinkConfig,
) -> Result<(), iggy_connector_sdk::Error> {
    let Some(tls) = &config.tls else {
        return Ok(());
    };
    let broker_url = url::Url::parse(&config.broker_url).map_err(|error| {
        iggy_connector_sdk::Error::InvalidConfigValue(format!("broker_url: {error}"))
    })?;
    if !matches!(broker_url.scheme(), "mqtts" | "ssl") {
        return Err(iggy_connector_sdk::Error::InvalidConfigValue(
            "tls requires an mqtts:// or ssl:// broker_url".to_string(),
        ));
    }
    if tls.client_cert_file.is_some() != tls.client_key_file.is_some() {
        return Err(iggy_connector_sdk::Error::InvalidConfigValue(
            "tls client_cert_file and client_key_file must be configured together".to_string(),
        ));
    }
    Ok(())
}

fn tls_transport(config: &MqttSinkConfig) -> Result<Option<Transport>, iggy_connector_sdk::Error> {
    let Some(tls) = &config.tls else {
        return Ok(None);
    };
    let ca = tls
        .ca_file
        .as_deref()
        .map(|path| read_certificate_file(path, "tls.ca_file"))
        .transpose()?;
    let client_auth = match (&tls.client_cert_file, &tls.client_key_file) {
        (Some(certificate_path), Some(key_path)) => Some((
            read_certificate_file(certificate_path, "tls.client_cert_file")?,
            read_private_key_file(key_path, "tls.client_key_file")?,
        )),
        (None, None) => None,
        _ => unreachable!("TLS configuration was validated before loading files"),
    };

    let tls_config = TlsConfiguration::Rustls(Arc::new(build_tls_config(ca, client_auth)?));
    Ok(Some(Transport::tls_with_config(tls_config)))
}

fn broker_url_with_client_id(
    broker_url: &str,
    client_id: &str,
) -> Result<String, iggy_connector_sdk::Error> {
    let mut url = url::Url::parse(broker_url).map_err(|error| {
        iggy_connector_sdk::Error::InvalidConfigValue(format!("broker_url is invalid: {error}"))
    })?;
    let query_pairs = url
        .query_pairs()
        .filter(|(key, _)| key != "client_id")
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    let mut query = url.query_pairs_mut();
    query.clear();
    for (key, value) in &query_pairs {
        query.append_pair(key, value);
    }
    query.append_pair("client_id", client_id);
    drop(query);

    Ok(url.into())
}

fn install_rustls_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

fn read_file(path: &str, field: &str) -> Result<Vec<u8>, iggy_connector_sdk::Error> {
    fs::read(path).map_err(|error| {
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

fn confirmed_indexes(confirmed: &[bool]) -> Vec<usize> {
    confirmed
        .iter()
        .enumerate()
        .filter_map(|(index, confirmed)| confirmed.then_some(index))
        .collect()
}

fn mqtt5_qos(qos: rumqttc::QoS) -> rumqttc::v5::mqttbytes::QoS {
    match qos {
        rumqttc::QoS::AtMostOnce => rumqttc::v5::mqttbytes::QoS::AtMostOnce,
        rumqttc::QoS::AtLeastOnce => rumqttc::v5::mqttbytes::QoS::AtLeastOnce,
        rumqttc::QoS::ExactlyOnce => rumqttc::v5::mqttbytes::QoS::ExactlyOnce,
    }
}

async fn wait_for_connack(
    event_loop: &mut Mqtt311EventLoop,
    id: u32,
    connect_timeout: Duration,
) -> Result<(), iggy_connector_sdk::Error> {
    // Ignore unrelated packets while establishing the session; the first
    // CONNACK is the broker's definitive connection result.
    timeout(connect_timeout, async {
        loop {
            match event_loop.poll().await {
                Ok(Mqtt311Event::Incoming(Mqtt311Incoming::ConnAck(connack))) => {
                    return mqtt311_connack_result(connack.code, id);
                }
                Ok(_) => {}
                Err(error) => {
                    return Err(mqtt311_connection_error(error, id));
                }
            }
        }
    })
    .await
    .map_err(|_| {
        iggy_connector_sdk::Error::Connection(format!(
            "timed out waiting for MQTT 3.1.1 CONNACK for connector ID {id}"
        ))
    })?
}

async fn wait_for_connack_v5(
    event_loop: &mut Mqtt5EventLoop,
    id: u32,
    connect_timeout: Duration,
) -> Result<(), iggy_connector_sdk::Error> {
    timeout(connect_timeout, async {
        loop {
            match event_loop.poll().await {
                Ok(Mqtt5Event::Incoming(Mqtt5Incoming::ConnAck(connack))) => {
                    return mqtt5_connack_result(connack.code, id);
                }
                Ok(_) => {}
                Err(error) => {
                    return Err(mqtt5_connection_error(error, id));
                }
            }
        }
    })
    .await
    .map_err(|_| {
        iggy_connector_sdk::Error::Connection(format!(
            "timed out waiting for MQTT 5 CONNACK for connector ID {id}"
        ))
    })?
}

fn mqtt311_connection_error(error: Mqtt311ConnectionError, id: u32) -> iggy_connector_sdk::Error {
    match error {
        Mqtt311ConnectionError::ConnectionRefused(code) => mqtt311_connack_error(code, id),
        Mqtt311ConnectionError::Tls(error) => mqtt_tls_connection_error(error, id),
        error => iggy_connector_sdk::Error::Connection(format!(
            "MQTT 3.1.1 connection failed for connector ID {id}: {error}"
        )),
    }
}

fn mqtt311_connack_result(
    code: rumqttc::mqttbytes::v4::ConnectReturnCode,
    id: u32,
) -> Result<(), iggy_connector_sdk::Error> {
    if code == rumqttc::mqttbytes::v4::ConnectReturnCode::Success {
        Ok(())
    } else {
        Err(mqtt311_connack_error(code, id))
    }
}

fn mqtt311_connack_error(
    code: rumqttc::mqttbytes::v4::ConnectReturnCode,
    id: u32,
) -> iggy_connector_sdk::Error {
    use rumqttc::mqttbytes::v4::ConnectReturnCode;

    match code {
        ConnectReturnCode::BadUserNamePassword | ConnectReturnCode::NotAuthorized => {
            iggy_connector_sdk::Error::InvalidConfigValue(format!(
                "MQTT 3.1.1 broker rejected authentication for connector ID {id}: {code:?}"
            ))
        }
        ConnectReturnCode::RefusedProtocolVersion | ConnectReturnCode::BadClientId => {
            iggy_connector_sdk::Error::InvalidConfigValue(format!(
                "MQTT 3.1.1 broker rejected connection configuration for connector ID {id}: {code:?}"
            ))
        }
        ConnectReturnCode::ServiceUnavailable => iggy_connector_sdk::Error::Connection(format!(
            "MQTT 3.1.1 broker is unavailable for connector ID {id}"
        )),
        ConnectReturnCode::Success => {
            unreachable!("success is handled before mapping connection failures")
        }
    }
}

fn mqtt5_connection_error(error: Mqtt5ConnectionError, id: u32) -> iggy_connector_sdk::Error {
    match error {
        Mqtt5ConnectionError::ConnectionRefused(code) => mqtt5_connack_error(code, id),
        Mqtt5ConnectionError::Tls(error) => mqtt_tls_connection_error(error, id),
        error => iggy_connector_sdk::Error::Connection(format!(
            "MQTT 5 connection failed for connector ID {id}: {error}"
        )),
    }
}

fn mqtt_tls_connection_error(error: rumqttc::TlsError, id: u32) -> iggy_connector_sdk::Error {
    if matches!(&error, rumqttc::TlsError::Io(_)) {
        return iggy_connector_sdk::Error::Connection(format!(
            "MQTT TLS connection failed for connector ID {id}: {error}"
        ));
    }

    iggy_connector_sdk::Error::InvalidConfigValue(format!(
        "MQTT TLS validation failed for connector ID {id}: {error}"
    ))
}

fn mqtt5_connack_result(
    code: Mqtt5ConnectReturnCode,
    id: u32,
) -> Result<(), iggy_connector_sdk::Error> {
    if code == Mqtt5ConnectReturnCode::Success {
        Ok(())
    } else {
        Err(mqtt5_connack_error(code, id))
    }
}

fn mqtt5_connack_error(code: Mqtt5ConnectReturnCode, id: u32) -> iggy_connector_sdk::Error {
    use Mqtt5ConnectReturnCode::*;

    match code {
        BadUserNamePassword | NotAuthorized | Banned | BadAuthenticationMethod => {
            iggy_connector_sdk::Error::InvalidConfigValue(format!(
                "MQTT 5 broker rejected authentication for connector ID {id}: {code:?}"
            ))
        }
        RefusedProtocolVersion | UnsupportedProtocolVersion | ClientIdentifierNotValid => {
            iggy_connector_sdk::Error::InvalidConfigValue(format!(
                "MQTT 5 broker rejected connection configuration for connector ID {id}: {code:?}"
            ))
        }
        ServerUnavailable
        | ServerBusy
        | UseAnotherServer
        | ServerMoved
        | ConnectionRateExceeded
        | QuotaExceeded => iggy_connector_sdk::Error::Connection(format!(
            "MQTT 5 broker temporarily rejected connection for connector ID {id}: {code:?}"
        )),
        Success => unreachable!("success is handled before mapping connection failures"),
        _ => iggy_connector_sdk::Error::InitError(format!(
            "MQTT 5 broker rejected connection for connector ID {id}: {code:?}"
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use rumqttc::v5::mqttbytes::v5::{
        ConnectReturnCode, PubAckReason, PubCompReason, PubRecReason,
    };

    use super::{
        DriverEvent, Mqtt311Options, PublishWindowFailure, PublishWindowState,
        broker_url_with_client_id, mqtt5_connack_error, mqtt5_connection_error,
        mqtt311_connection_error, read_certificate_file, read_private_key_file,
    };

    #[test]
    fn given_invalid_ca_pem_when_validating_should_return_configuration_error() {
        let path =
            std::env::temp_dir().join(format!("iggy-mqtt-sink-invalid-ca-{}", std::process::id()));
        fs::write(&path, b"not a certificate").expect("invalid CA file must write");
        let error = read_certificate_file(path.to_str().unwrap(), "tls.ca_file")
            .expect_err("invalid PEM must be rejected");
        fs::remove_file(path).expect("invalid CA file must remove");

        assert!(matches!(
            error,
            iggy_connector_sdk::Error::InvalidConfigValue(message)
                if message.contains("tls.ca_file")
        ));
    }

    #[test]
    fn given_invalid_private_key_pem_when_validating_should_return_configuration_error() {
        let path =
            std::env::temp_dir().join(format!("iggy-mqtt-sink-invalid-key-{}", std::process::id()));
        fs::write(&path, b"not a private key").expect("invalid key file must write");
        let error = read_private_key_file(path.to_str().unwrap(), "tls.client_key_file")
            .expect_err("invalid PEM must be rejected");
        fs::remove_file(path).expect("invalid key file must remove");

        assert!(matches!(
            error,
                iggy_connector_sdk::Error::InvalidConfigValue(message)
                if message.contains("tls.client_key_file")
        ));
    }

    #[test]
    fn given_permanent_tls_failure_when_classifying_should_return_configuration_error() {
        let error = mqtt311_connection_error(
            rumqttc::ConnectionError::Tls(rumqttc::TlsError::NoValidCertInChain),
            1,
        );

        assert!(matches!(
            error,
            iggy_connector_sdk::Error::InvalidConfigValue(message)
                if message.contains("TLS validation")
        ));
    }

    #[test]
    fn given_tls_io_failure_when_classifying_should_return_connection_error() {
        let error = mqtt5_connection_error(
            rumqttc::v5::ConnectionError::Tls(rumqttc::TlsError::Io(std::io::Error::other(
                "connection reset",
            ))),
            1,
        );

        assert!(matches!(
            error,
            iggy_connector_sdk::Error::Connection(message)
                if message.contains("MQTT TLS connection failed")
        ));
    }

    #[test]
    fn given_out_of_order_qos_one_acknowledgements_when_applied_should_confirm_matching_publications()
     {
        let mut state = PublishWindowState::new(3, rumqttc::QoS::AtLeastOnce);

        state.apply(DriverEvent::OutgoingPublish(41)).unwrap();
        state.apply(DriverEvent::OutgoingPublish(42)).unwrap();
        state.apply(DriverEvent::OutgoingPublish(43)).unwrap();
        state.apply(DriverEvent::PubAck(42)).unwrap();
        state.apply(DriverEvent::PubAck(41)).unwrap();
        state.apply(DriverEvent::PubAck(43)).unwrap();

        assert_eq!(state.confirmed_indexes(), vec![0, 1, 2]);
    }

    #[test]
    fn given_out_of_order_qos_two_acknowledgements_when_applied_should_confirm_matching_publications()
     {
        let mut state = PublishWindowState::new(2, rumqttc::QoS::ExactlyOnce);

        state.apply(DriverEvent::OutgoingPublish(51)).unwrap();
        state.apply(DriverEvent::OutgoingPublish(52)).unwrap();
        state.apply(DriverEvent::PubComp(52)).unwrap();

        assert_eq!(state.confirmed_indexes(), vec![1]);
        assert!(!state.is_complete());

        state.apply(DriverEvent::PubComp(51)).unwrap();
        assert!(state.is_complete());
    }

    #[test]
    fn given_qos_zero_when_outgoing_publish_is_applied_should_confirm_without_broker_acknowledgement()
     {
        let mut state = PublishWindowState::new(1, rumqttc::QoS::AtMostOnce);

        state.apply(DriverEvent::OutgoingPublish(0)).unwrap();

        assert!(state.is_complete());
    }

    #[test]
    fn given_outgoing_publish_beyond_window_when_applied_should_return_protocol_error() {
        let mut state = PublishWindowState::new(1, rumqttc::QoS::AtLeastOnce);

        state.apply(DriverEvent::OutgoingPublish(41)).unwrap();
        let error = state.apply(DriverEvent::OutgoingPublish(42)).unwrap_err();

        assert!(matches!(
            error,
            PublishWindowFailure::Protocol(message)
                if message.contains("unexpected MQTT publish packet")
        ));
    }

    #[test]
    fn given_stale_acknowledgement_when_new_window_reuses_packet_id_should_not_confirm_publication()
    {
        let mut state = PublishWindowState::new(1, rumqttc::QoS::AtLeastOnce);

        state.apply(DriverEvent::PubAck(71)).unwrap();
        state.apply(DriverEvent::OutgoingPublish(71)).unwrap();

        assert!(!state.is_complete());
    }

    #[test]
    fn given_mqtt_five_no_matching_subscribers_puback_when_applied_should_confirm_publication() {
        let mut state = PublishWindowState::new(1, rumqttc::QoS::AtLeastOnce);
        state.apply(DriverEvent::OutgoingPublish(61)).unwrap();

        state
            .apply(DriverEvent::Mqtt5PubAck {
                packet_id: 61,
                reason: PubAckReason::NoMatchingSubscribers,
            })
            .unwrap();

        assert!(state.is_complete());
    }

    #[test]
    fn given_mqtt_five_rejected_puback_when_applied_should_return_error_without_confirmation() {
        let mut state = PublishWindowState::new(1, rumqttc::QoS::AtLeastOnce);
        state.apply(DriverEvent::OutgoingPublish(62)).unwrap();

        let error = state
            .apply(DriverEvent::Mqtt5PubAck {
                packet_id: 62,
                reason: PubAckReason::NotAuthorized,
            })
            .unwrap_err();

        assert!(matches!(
            &error,
            PublishWindowFailure::BrokerRejected(message)
                if message.contains("PUBACK rejected")
        ));
        assert!(matches!(
            error.error(),
            iggy_connector_sdk::Error::CannotStoreData(message)
                if message.contains("PUBACK rejected")
        ));
        assert!(!state.is_complete());
    }

    #[test]
    fn given_mqtt_five_rejected_pubrec_when_applied_should_return_error_without_confirmation() {
        let mut state = PublishWindowState::new(1, rumqttc::QoS::ExactlyOnce);
        state.apply(DriverEvent::OutgoingPublish(63)).unwrap();

        let error = state
            .apply(DriverEvent::Mqtt5PubRec {
                packet_id: 63,
                reason: PubRecReason::NotAuthorized,
            })
            .unwrap_err();

        assert!(matches!(
            &error,
            PublishWindowFailure::BrokerRejected(message)
                if message.contains("PUBREC rejected")
        ));
        assert!(matches!(
            error.error(),
            iggy_connector_sdk::Error::CannotStoreData(message)
                if message.contains("PUBREC rejected")
        ));
        assert!(!state.is_complete());
    }

    #[test]
    fn given_mqtt_five_rejected_pubcomp_when_applied_should_return_error_without_confirmation() {
        let mut state = PublishWindowState::new(1, rumqttc::QoS::ExactlyOnce);
        state.apply(DriverEvent::OutgoingPublish(63)).unwrap();

        let error = state
            .apply(DriverEvent::Mqtt5PubComp {
                packet_id: 63,
                reason: PubCompReason::PacketIdentifierNotFound,
            })
            .unwrap_err();

        assert!(matches!(
            error,
            PublishWindowFailure::BrokerRejected(message)
                if message.contains("PUBCOMP rejected")
        ));
        assert!(!state.is_complete());
    }

    #[test]
    fn given_mqtt_five_authentication_rejection_should_return_configuration_error() {
        let error = mqtt5_connack_error(ConnectReturnCode::BadUserNamePassword, 1);

        assert!(matches!(
            error,
            iggy_connector_sdk::Error::InvalidConfigValue(message)
                if message.contains("authentication")
        ));
    }

    #[test]
    fn given_mqtt_five_busy_broker_should_return_connection_error() {
        let error = mqtt5_connack_error(ConnectReturnCode::ServerBusy, 1);

        assert!(matches!(
            error,
            iggy_connector_sdk::Error::Connection(message)
                if message.contains("temporarily rejected")
        ));
    }

    #[test]
    fn given_broker_url_without_client_id_when_building_driver_options_should_add_client_id() {
        let broker_url = broker_url_with_client_id("mqtt://localhost:1883", "sink-client")
            .expect("broker URL must accept a client identifier");

        let options =
            Mqtt311Options::parse_url(broker_url).expect("driver options must parse broker URL");

        assert_eq!(options.client_id(), "sink-client");
    }
}
