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

use rumqttc::v5::{
    AsyncClient as Mqtt5Client, ConnectionError as Mqtt5ConnectionError, Event as Mqtt5Event,
    EventLoop as Mqtt5EventLoop, Incoming as Mqtt5Incoming, MqttOptions as Mqtt5Options,
    mqttbytes::v5::{ConnectReturnCode as Mqtt5ConnectReturnCode, PubAckReason, PubCompReason},
};
use rumqttc::{
    AsyncClient as Mqtt311Client, ConnectionError as Mqtt311ConnectionError, Event as Mqtt311Event,
    EventLoop as Mqtt311EventLoop, Incoming as Mqtt311Incoming, MqttOptions as Mqtt311Options,
    Outgoing, Transport,
};
use secrecy::ExposeSecret;
use serde::Deserialize;
use std::{
    collections::{HashMap, VecDeque},
    fs,
    io::{BufReader, Cursor, Read},
    path::Path,
    time::{Duration, Instant},
};
use tokio::time::timeout;

use crate::{MqttSinkConfig, routing::PreparedPublication};

const MAX_TLS_FILE_BYTES: u64 = 1024 * 1024;

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
            Self::BrokerRejected(message) | Self::Protocol(message) => {
                iggy_connector_sdk::Error::Connection(message)
            }
        }
    }
}

struct PublishWindowState {
    qos: rumqttc::QoS,
    submitted: VecDeque<usize>,
    packet_to_index: HashMap<u16, usize>,
    confirmed: Vec<bool>,
}

impl PublishWindowState {
    fn new(length: usize, qos: rumqttc::QoS) -> Self {
        Self {
            qos,
            submitted: VecDeque::with_capacity(length),
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
                let Some(index) = self.submitted.pop_front() else {
                    return Err(PublishWindowFailure::Protocol(
                        "received an unexpected MQTT publish packet".to_string(),
                    ));
                };
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
                let mut options =
                    Mqtt311Options::parse_url(&config.broker_url).map_err(|error| {
                        iggy_connector_sdk::Error::InvalidConfigValue(format!(
                            "broker_url: {error}"
                        ))
                    })?;
                options
                    .set_client_id(client_id)
                    .set_keep_alive(keep_alive)
                    .set_request_channel_capacity(request_capacity)
                    .set_inflight(max_in_flight);
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
                let mut options = Mqtt5Options::parse_url(&config.broker_url).map_err(|error| {
                    iggy_connector_sdk::Error::InvalidConfigValue(format!("broker_url: {error}"))
                })?;
                options
                    .set_client_id(client_id)
                    .set_keep_alive(keep_alive)
                    .set_request_channel_capacity(request_capacity)
                    .set_outgoing_inflight_upper_limit(max_in_flight);
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

        // AsyncClient::publish queues a request but does not return its packet
        // identifier. Keep submitted indexes until the event loop emits the
        // matching Outgoing::Publish event.
        let mut state = PublishWindowState::new(publications.len(), qos);
        for (index, &publication) in publications.iter().enumerate() {
            if let Err(error) = self.enqueue_publication(publication, qos, retain).await {
                return Err(PublishWindowError {
                    confirmed: state.confirmed_indexes(),
                    error,
                    retryable: true,
                });
            }
            state.submitted.push_back(index);
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
                .publish(&publication.topic, qos, retain, publication.payload.clone())
                .await
                .map_err(|error| iggy_connector_sdk::Error::Connection(error.to_string())),
            MqttConnection::Mqtt5 { client, .. } => {
                let result = if let Some(properties) = publication.properties.clone() {
                    client
                        .publish_with_properties(
                            &publication.topic,
                            mqtt5_qos(qos),
                            retain,
                            publication.payload.clone(),
                            properties,
                        )
                        .await
                } else {
                    client
                        .publish(
                            &publication.topic,
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
    let has_ca = config.tls_ca_file.is_some();
    let has_client_cert = config.tls_client_cert_file.is_some();
    let has_client_key = config.tls_client_key_file.is_some();

    for (field, path) in [
        ("tls_ca_file", config.tls_ca_file.as_ref()),
        ("tls_client_cert_file", config.tls_client_cert_file.as_ref()),
        ("tls_client_key_file", config.tls_client_key_file.as_ref()),
    ] {
        if path.is_some_and(|path| path.as_os_str().is_empty()) {
            return Err(iggy_connector_sdk::Error::InvalidConfigValue(format!(
                "{field} must not be empty"
            )));
        }
    }

    if has_client_cert != has_client_key {
        return Err(iggy_connector_sdk::Error::InvalidConfigValue(
            "tls_client_cert_file and tls_client_key_file must be configured together".to_string(),
        ));
    }
    if (has_client_cert || has_client_key) && !has_ca {
        return Err(iggy_connector_sdk::Error::InvalidConfigValue(
            "tls_ca_file is required when mutual TLS is configured".to_string(),
        ));
    }
    if has_ca || has_client_cert || has_client_key {
        let scheme = broker_scheme(&config.broker_url)?;
        if !matches!(scheme.as_str(), "mqtts" | "ssl") {
            return Err(iggy_connector_sdk::Error::InvalidConfigValue(
                "TLS files require a broker_url with the mqtts:// or ssl:// scheme".to_string(),
            ));
        }
    }

    Ok(())
}

fn tls_transport(config: &MqttSinkConfig) -> Result<Option<Transport>, iggy_connector_sdk::Error> {
    validate_tls_config(config)?;

    let Some(ca_path) = config.tls_ca_file.as_deref() else {
        return Ok(None);
    };
    let ca = read_pem_file(ca_path, "tls_ca_file")?;
    validate_certificate_chain(&ca, "tls_ca_file")?;

    let client_auth = match (
        config.tls_client_cert_file.as_deref(),
        config.tls_client_key_file.as_deref(),
    ) {
        (Some(certificate_path), Some(key_path)) => {
            let certificate = read_pem_file(certificate_path, "tls_client_cert_file")?;
            validate_certificate_chain(&certificate, "tls_client_cert_file")?;
            let key = read_pem_file(key_path, "tls_client_key_file")?;
            validate_private_key(&key)?;
            Some((certificate, key))
        }
        (None, None) => None,
        _ => unreachable!("TLS configuration was validated before loading files"),
    };

    Ok(Some(Transport::tls(ca, client_auth, None)))
}

fn broker_scheme(broker_url: &str) -> Result<String, iggy_connector_sdk::Error> {
    let url = url::Url::parse(broker_url).map_err(|error| {
        iggy_connector_sdk::Error::InvalidConfigValue(format!("broker_url is invalid: {error}"))
    })?;
    Ok(url.scheme().to_string())
}

fn read_pem_file(path: &Path, field: &str) -> Result<Vec<u8>, iggy_connector_sdk::Error> {
    let mut file = fs::File::open(path).map_err(|_| {
        iggy_connector_sdk::Error::InvalidConfigValue(format!("cannot read {field}"))
    })?;
    let metadata = file.metadata().map_err(|_| {
        iggy_connector_sdk::Error::InvalidConfigValue(format!("cannot read {field}"))
    })?;
    if !metadata.is_file() {
        return Err(iggy_connector_sdk::Error::InvalidConfigValue(format!(
            "{field} must be a regular file"
        )));
    }
    if metadata.len() > MAX_TLS_FILE_BYTES {
        return Err(iggy_connector_sdk::Error::InvalidConfigValue(format!(
            "{field} exceeds the maximum size of {MAX_TLS_FILE_BYTES} bytes"
        )));
    }

    let mut pem = Vec::with_capacity(metadata.len() as usize);
    file.by_ref()
        .take(MAX_TLS_FILE_BYTES + 1)
        .read_to_end(&mut pem)
        .map_err(|_| {
            iggy_connector_sdk::Error::InvalidConfigValue(format!("cannot read {field}"))
        })?;
    if pem.len() as u64 > MAX_TLS_FILE_BYTES {
        return Err(iggy_connector_sdk::Error::InvalidConfigValue(format!(
            "{field} exceeds the maximum size of {MAX_TLS_FILE_BYTES} bytes"
        )));
    }

    Ok(pem)
}

fn validate_certificate_chain(pem: &[u8], field: &str) -> Result<(), iggy_connector_sdk::Error> {
    let certificates = rustls_pemfile::certs(&mut BufReader::new(Cursor::new(pem)))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| {
            iggy_connector_sdk::Error::InvalidConfigValue(format!("{field} is not valid PEM"))
        })?;
    if certificates.is_empty() {
        return Err(iggy_connector_sdk::Error::InvalidConfigValue(format!(
            "{field} must contain a PEM certificate"
        )));
    }
    Ok(())
}

fn validate_private_key(pem: &[u8]) -> Result<(), iggy_connector_sdk::Error> {
    rustls_pemfile::private_key(&mut BufReader::new(Cursor::new(pem)))
        .map_err(|_| {
            iggy_connector_sdk::Error::InvalidConfigValue(
                "tls_client_key_file is not valid PEM".to_string(),
            )
        })?
        .ok_or_else(|| {
            iggy_connector_sdk::Error::InvalidConfigValue(
                "tls_client_key_file must contain a private key".to_string(),
            )
        })?;
    Ok(())
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

    use rcgen::generate_simple_self_signed;
    use rumqttc::v5::mqttbytes::v5::{ConnectReturnCode, PubAckReason, PubCompReason};

    use super::{
        DriverEvent, MAX_TLS_FILE_BYTES, PublishWindowFailure, PublishWindowState,
        mqtt5_connack_error, mqtt5_connection_error, mqtt311_connection_error, read_pem_file,
        tls_transport, validate_certificate_chain, validate_private_key,
    };
    use crate::{MqttSinkConfig, routing::TopicMode};

    fn test_config() -> MqttSinkConfig {
        MqttSinkConfig {
            broker_url: "mqtts://localhost:8883".to_string(),
            protocol: super::MqttProtocol::Mqtt5,
            client_id: None,
            username: None,
            password: None,
            tls_ca_file: None,
            tls_client_cert_file: None,
            tls_client_key_file: None,
            topic: Some("test/topic".to_string()),
            topic_mode: TopicMode::Static,
            topic_template: None,
            fallback_topic: None,
            qos: 1,
            retain: false,
            keep_alive: None,
            request_capacity: None,
            publish_timeout: None,
            shutdown_timeout: None,
            max_retries: None,
            retry_delay: None,
            max_retry_delay: None,
            max_in_flight: None,
            verbose_logging: None,
        }
    }

    #[test]
    fn given_invalid_ca_pem_when_validating_should_return_configuration_error() {
        let error = validate_certificate_chain(b"not a certificate", "tls_ca_file")
            .expect_err("invalid PEM must be rejected");

        assert!(matches!(
            error,
            iggy_connector_sdk::Error::InvalidConfigValue(message)
                if message.contains("tls_ca_file")
        ));
    }

    #[test]
    fn given_invalid_private_key_pem_when_validating_should_return_configuration_error() {
        let error =
            validate_private_key(b"not a private key").expect_err("invalid PEM must be rejected");

        assert!(matches!(
            error,
            iggy_connector_sdk::Error::InvalidConfigValue(message)
                if message.contains("tls_client_key_file")
        ));
    }

    #[test]
    fn given_tls_ca_file_when_building_transport_should_create_tls_transport() {
        let certificate = generate_simple_self_signed(vec!["localhost".to_string()])
            .expect("test certificate must generate");
        let path = std::env::temp_dir().join(format!(
            "iggy-mqtt-sink-tls-{}-{}.pem",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        fs::write(&path, certificate.cert.pem()).expect("test certificate must write");

        let mut config = test_config();
        config.tls_ca_file = Some(path.clone());
        let transport = tls_transport(&config);

        fs::remove_file(path).expect("test certificate must remove");
        assert!(transport.is_ok_and(|transport| transport.is_some()));
    }

    #[test]
    fn given_tls_directory_when_reading_pem_should_return_configuration_error() {
        let error = read_pem_file(&std::env::temp_dir(), "tls_ca_file")
            .expect_err("a directory must not be accepted as a TLS file");

        assert!(matches!(
            error,
            iggy_connector_sdk::Error::InvalidConfigValue(message)
                if message.contains("regular file")
        ));
    }

    #[test]
    fn given_oversized_tls_file_when_reading_pem_should_return_configuration_error() {
        let path = std::env::temp_dir().join(format!(
            "iggy-mqtt-sink-oversized-tls-{}-{}.pem",
            std::process::id(),
            std::thread::current().name().unwrap_or("test")
        ));
        fs::write(&path, vec![0; MAX_TLS_FILE_BYTES as usize + 1])
            .expect("oversized test file must write");

        let error = read_pem_file(&path, "tls_ca_file")
            .expect_err("an oversized TLS file must be rejected");

        fs::remove_file(path).expect("oversized test file must remove");
        assert!(matches!(
            error,
            iggy_connector_sdk::Error::InvalidConfigValue(message)
                if message.contains("maximum size")
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
        state.submitted.extend([0, 1, 2]);

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
        state.submitted.extend([0, 1]);

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
        state.submitted.push_back(0);

        state.apply(DriverEvent::OutgoingPublish(0)).unwrap();

        assert!(state.is_complete());
    }

    #[test]
    fn given_stale_acknowledgement_when_new_window_reuses_packet_id_should_not_confirm_publication()
    {
        let mut state = PublishWindowState::new(1, rumqttc::QoS::AtLeastOnce);

        state.apply(DriverEvent::PubAck(71)).unwrap();
        state.submitted.push_back(0);
        state.apply(DriverEvent::OutgoingPublish(71)).unwrap();

        assert!(!state.is_complete());
    }

    #[test]
    fn given_mqtt_five_no_matching_subscribers_puback_when_applied_should_confirm_publication() {
        let mut state = PublishWindowState::new(1, rumqttc::QoS::AtLeastOnce);
        state.submitted.push_back(0);
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
        state.submitted.push_back(0);
        state.apply(DriverEvent::OutgoingPublish(62)).unwrap();

        let error = state
            .apply(DriverEvent::Mqtt5PubAck {
                packet_id: 62,
                reason: PubAckReason::NotAuthorized,
            })
            .unwrap_err();

        assert!(matches!(
            error,
            PublishWindowFailure::BrokerRejected(message)
                if message.contains("PUBACK rejected")
        ));
        assert!(!state.is_complete());
    }

    #[test]
    fn given_mqtt_five_rejected_pubcomp_when_applied_should_return_error_without_confirmation() {
        let mut state = PublishWindowState::new(1, rumqttc::QoS::ExactlyOnce);
        state.submitted.push_back(0);
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
}
