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

use rumqttc_v4::tokio_rustls::rustls;
use rumqttc_v4::tokio_rustls::rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer},
};
use rumqttc_v4::{
    AsyncClient as Mqtt311Client, ConnectionError as Mqtt311ConnectionError, Event as Mqtt311Event,
    EventLoop as Mqtt311EventLoop, Incoming as Mqtt311Incoming, MqttOptions as Mqtt311Options,
    Outgoing as Mqtt311Outgoing, PublishOptions as Mqtt311PublishOptions,
    TlsConfiguration as Mqtt311TlsConfiguration, Transport as Mqtt311Transport,
};
use rumqttc_v5::{
    AsyncClient as Mqtt5Client, ConnectionError as Mqtt5ConnectionError, Event as Mqtt5Event,
    EventLoop as Mqtt5EventLoop, Incoming as Mqtt5Incoming, MqttOptions as Mqtt5Options,
    Outgoing as Mqtt5Outgoing, PublishOptions as Mqtt5PublishOptions,
    StateError as Mqtt5StateError,
    mqttbytes::v5::{
        ConnectReturnCode as Mqtt5ConnectReturnCode, DisconnectReasonCode, PubAckReason,
        PubCompReason, PubRecReason,
    },
};
use rustls_native_certs::load_native_certs;
use rustls_pemfile::{certs, private_key};
use secrecy::ExposeSecret;
use serde::Deserialize;
use std::{
    collections::{HashMap, VecDeque},
    fs,
    io::{BufReader, Cursor},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};

use crate::{MqttSinkConfig, routing::PreparedPublication};

mod rumqttc {
    pub use rumqttc_v4::*;
    pub use rumqttc_v5 as v5;
}

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

pub(crate) struct MqttDriver {
    client: MqttClient,
    events: mpsc::Receiver<DriverEvent>,
    commands: mpsc::Sender<DriverCommand>,
    task: Option<JoinHandle<()>>,
    max_packet_size: Arc<AtomicUsize>,
}

pub(crate) struct PublishWindowError {
    pub(crate) error: iggy_connector_sdk::Error,
    pub(crate) retryable: bool,
    pub(crate) requires_clean: bool,
    pub(crate) requires_requeue: bool,
}

pub(crate) enum PublishWindowProgress {
    Complete,
    SessionPresent(bool),
}

enum DriverEvent {
    ConnectionError(iggy_connector_sdk::Error),
    SessionPresent(bool),
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
    TransientBrokerRejected(String),
    Protocol(String),
}

impl PublishWindowFailure {
    fn error(self) -> iggy_connector_sdk::Error {
        match self {
            Self::BrokerRejected(message) => iggy_connector_sdk::Error::CannotStoreData(message),
            Self::TransientBrokerRejected(message) => {
                iggy_connector_sdk::Error::Connection(message)
            }
            Self::Protocol(message) => iggy_connector_sdk::Error::Connection(message),
        }
    }

    fn retryable(&self) -> bool {
        matches!(self, Self::TransientBrokerRejected(_))
    }
}

pub(crate) struct PublishWindowState {
    qos: rumqttc::QoS,
    submitted_indexes: VecDeque<usize>,
    packet_to_index: HashMap<u16, usize>,
    confirmed: Vec<bool>,
    confirmed_count: usize,
}

impl PublishWindowState {
    fn new(length: usize, qos: rumqttc::QoS) -> Self {
        Self {
            qos,
            submitted_indexes: VecDeque::with_capacity(length),
            packet_to_index: HashMap::with_capacity(length),
            confirmed: vec![false; length],
            confirmed_count: 0,
        }
    }

    fn is_complete(&self) -> bool {
        self.confirmed_count == self.confirmed.len()
    }

    fn submitted(&mut self, index: usize) {
        self.submitted_indexes.push_back(index);
    }

    fn confirm(&mut self, index: usize) {
        if !self.confirmed[index] {
            self.confirmed[index] = true;
            self.confirmed_count += 1;
        }
    }

    fn apply(&mut self, event: DriverEvent) -> Result<(), PublishWindowFailure> {
        match event {
            DriverEvent::OutgoingPublish(packet_id) => {
                if self.packet_to_index.contains_key(&packet_id) {
                    return Ok(());
                }
                let Some(index) = self.submitted_indexes.pop_front() else {
                    return Err(PublishWindowFailure::Protocol(
                        "received an unexpected MQTT publish packet".to_string(),
                    ));
                };
                if self.qos == rumqttc::QoS::AtMostOnce {
                    self.confirm(index);
                } else {
                    self.packet_to_index.insert(packet_id, index);
                }
            }
            DriverEvent::PubAck(packet_id) if self.qos == rumqttc::QoS::AtLeastOnce => {
                if let Some(index) = self.packet_to_index.remove(&packet_id) {
                    self.confirm(index);
                }
            }
            DriverEvent::PubComp(packet_id) if self.qos == rumqttc::QoS::ExactlyOnce => {
                if let Some(index) = self.packet_to_index.remove(&packet_id) {
                    self.confirm(index);
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
                        self.confirm(index);
                    } else {
                        return Err(mqtt5_puback_rejection(packet_id, reason));
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
                    return Err(mqtt5_pubrec_rejection(packet_id, reason));
                }
            }
            DriverEvent::Mqtt5PubComp { packet_id, reason }
                if self.qos == rumqttc::QoS::ExactlyOnce =>
            {
                if let Some(index) = self.packet_to_index.remove(&packet_id) {
                    if reason == PubCompReason::Success {
                        self.confirm(index);
                    } else {
                        return Err(PublishWindowFailure::BrokerRejected(format!(
                            "MQTT 5 PUBCOMP rejected packet ID {packet_id}: {reason:?}"
                        )));
                    }
                }
            }
            DriverEvent::ConnectionError(_)
            | DriverEvent::SessionPresent(_)
            | DriverEvent::PubAck(_)
            | DriverEvent::PubComp(_)
            | DriverEvent::Mqtt5PubAck { .. }
            | DriverEvent::Mqtt5PubRec { .. }
            | DriverEvent::Mqtt5PubComp { .. }
            | DriverEvent::Other => {}
        }
        Ok(())
    }

    pub(crate) fn confirmed_indexes(&self) -> Vec<usize> {
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

#[derive(Clone)]
enum MqttClient {
    Mqtt311(Mqtt311Client),
    Mqtt5(Mqtt5Client),
}

enum DriverCommand {
    Clean,
    DiscardQueuedEvents {
        result_sender: oneshot::Sender<()>,
    },
    Shutdown {
        disconnect_timeout: Duration,
        result_sender: oneshot::Sender<Result<(), iggy_connector_sdk::Error>>,
    },
}

struct MqttEventLoopTask {
    connection: MqttConnection,
    id: u32,
    qos: u8,
    retain: bool,
    configured_max_packet_size: usize,
    max_packet_size: Arc<AtomicUsize>,
    event_sender: mpsc::Sender<DriverEvent>,
    commands: mpsc::Receiver<DriverCommand>,
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

    fn client(&self) -> MqttClient {
        match self {
            Self::Mqtt311 { client, .. } => MqttClient::Mqtt311(client.clone()),
            Self::Mqtt5 { client, .. } => MqttClient::Mqtt5(client.clone()),
        }
    }

    fn clean(&mut self) {
        match self {
            Self::Mqtt311 { event_loop, .. } => event_loop.clean(),
            Self::Mqtt5 { event_loop, .. } => event_loop.clean(),
        }
    }

    async fn disconnect(
        &mut self,
        disconnect_timeout: Duration,
    ) -> Result<(), iggy_connector_sdk::Error> {
        match self {
            Self::Mqtt311 { client, event_loop } => {
                client
                    .disconnect()
                    .await
                    .map_err(|error| iggy_connector_sdk::Error::Connection(error.to_string()))?;
                wait_for_mqtt311_disconnect(event_loop, disconnect_timeout).await?;
            }
            Self::Mqtt5 { client, event_loop } => {
                client
                    .disconnect()
                    .await
                    .map_err(|error| iggy_connector_sdk::Error::Connection(error.to_string()))?;
                wait_for_mqtt5_disconnect(event_loop, disconnect_timeout).await?;
            }
        }
        Ok(())
    }
}

impl std::fmt::Debug for MqttDriver {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("MqttDriver")
            .field("protocol", &self.client.protocol())
            .finish()
    }
}

impl MqttClient {
    fn protocol(&self) -> MqttProtocol {
        match self {
            Self::Mqtt311(_) => MqttProtocol::Mqtt311,
            Self::Mqtt5(_) => MqttProtocol::Mqtt5,
        }
    }
}

impl Drop for MqttDriver {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

impl MqttDriver {
    pub(crate) async fn connect(
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
        let keep_alive = u16::try_from(keep_alive.as_secs()).map_err(|_| {
            iggy_connector_sdk::Error::InvalidConfigValue(
                "keep_alive must be at most 65535s".to_string(),
            )
        })?;
        let max_packet_size = config
            .max_packet_size
            .unwrap_or(crate::DEFAULT_MAX_PACKET_SIZE);
        let configured_max_packet_size = max_packet_size;

        let (connection, broker_max_packet_size) = match config.protocol {
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
                    .set_max_packet_size(max_packet_size, max_packet_size)
                    .set_clean_session(config.clean_start);
                if let (Some(username), Some(password)) =
                    (config.username.as_deref(), config.password.as_ref())
                {
                    options.set_credentials(username, password.expose_secret().to_string());
                }
                if let Some(transport) = tls_transport_v4(config)? {
                    options.set_transport(transport);
                }
                let (client, mut event_loop) = Mqtt311Client::builder(options)
                    .try_build()
                    .map_err(|error| {
                        iggy_connector_sdk::Error::InvalidConfigValue(error.to_string())
                    })?;
                wait_for_connack(&mut event_loop, id, connect_timeout).await?;
                (
                    MqttConnection::Mqtt311 {
                        client,
                        event_loop: Box::new(event_loop),
                    },
                    None,
                )
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
                    .set_max_packet_size(Some(u32::try_from(max_packet_size).map_err(|_| {
                        iggy_connector_sdk::Error::InvalidConfigValue(
                            "max_packet_size must fit into an MQTT 5 packet size".to_string(),
                        )
                    })?))
                    .set_clean_start(config.clean_start)
                    .set_session_expiry_interval(config.session_expiry_interval);
                if let (Some(username), Some(password)) =
                    (config.username.as_deref(), config.password.as_ref())
                {
                    options.set_credentials(username, password.expose_secret().to_string());
                }
                if let Some(transport) = tls_transport_v5(config)? {
                    options.set_transport(transport);
                }
                let (client, mut event_loop) =
                    Mqtt5Client::builder(options).try_build().map_err(|error| {
                        iggy_connector_sdk::Error::InvalidConfigValue(error.to_string())
                    })?;
                let broker_max_packet_size = wait_for_connack_v5(
                    &mut event_loop,
                    id,
                    connect_timeout,
                    config.qos,
                    config.retain,
                )
                .await?;
                (
                    MqttConnection::Mqtt5 {
                        client,
                        event_loop: Box::new(event_loop),
                    },
                    broker_max_packet_size,
                )
            }
        };

        let max_packet_size = match config.protocol {
            MqttProtocol::Mqtt311 => max_packet_size,
            MqttProtocol::Mqtt5 => broker_max_packet_size
                .map(|size| max_packet_size.min(size as usize))
                .unwrap_or(max_packet_size),
        };
        let client = connection.client();
        let max_packet_size = Arc::new(AtomicUsize::new(max_packet_size));
        let (event_sender, events) = mpsc::channel(request_capacity);
        let (commands, command_receiver) = mpsc::channel(2);
        let task = tokio::spawn(
            MqttEventLoopTask {
                connection,
                id,
                qos: config.qos,
                retain: config.retain,
                configured_max_packet_size,
                max_packet_size: Arc::clone(&max_packet_size),
                event_sender,
                commands: command_receiver,
            }
            .run(),
        );
        Ok(Self {
            client,
            events,
            commands,
            task: Some(task),
            max_packet_size,
        })
    }

    pub(crate) fn max_packet_size(&self) -> usize {
        self.max_packet_size.load(Ordering::Relaxed)
    }

    pub(crate) fn new_publish_window(
        &self,
        length: usize,
        qos: rumqttc::QoS,
    ) -> PublishWindowState {
        PublishWindowState::new(length, qos)
    }

    pub(crate) async fn enqueue_window(
        &mut self,
        publications: &[&PreparedPublication],
        indexes: &[usize],
        qos: rumqttc::QoS,
        retain: bool,
        state: &mut PublishWindowState,
    ) -> Result<(), iggy_connector_sdk::Error> {
        for (&publication, &index) in publications.iter().zip(indexes) {
            self.enqueue_publication(publication, qos, retain).await?;
            // AsyncClient queues requests in order but does not return packet
            // identifiers. The matching Outgoing::Publish event uses this order.
            state.submitted(index);
        }
        Ok(())
    }

    pub(crate) async fn drive_window(
        &mut self,
        state: &mut PublishWindowState,
        deadline: Instant,
    ) -> Result<PublishWindowProgress, PublishWindowError> {
        while !state.is_complete() {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(PublishWindowError {
                    error: iggy_connector_sdk::Error::Connection(
                        "timed out waiting for MQTT publication acknowledgements".to_string(),
                    ),
                    retryable: true,
                    requires_clean: true,
                    requires_requeue: false,
                });
            }
            let event = match timeout(remaining, self.events.recv()).await {
                Ok(Some(event)) => event,
                Ok(None) => {
                    return Err(PublishWindowError {
                        error: iggy_connector_sdk::Error::Connection(
                            "MQTT event loop stopped unexpectedly".to_string(),
                        ),
                        retryable: true,
                        requires_clean: false,
                        requires_requeue: false,
                    });
                }
                Err(_) => {
                    return Err(PublishWindowError {
                        error: iggy_connector_sdk::Error::Connection(
                            "timed out waiting for MQTT publication acknowledgements".to_string(),
                        ),
                        retryable: true,
                        requires_clean: true,
                        requires_requeue: false,
                    });
                }
            };

            let event = match event {
                DriverEvent::ConnectionError(error) => {
                    return Err(PublishWindowError {
                        retryable: matches!(error, iggy_connector_sdk::Error::Connection(_)),
                        error,
                        requires_clean: false,
                        requires_requeue: false,
                    });
                }
                DriverEvent::SessionPresent(session_present) => {
                    return Ok(PublishWindowProgress::SessionPresent(session_present));
                }
                event => event,
            };

            if let Err(error) = state.apply(event) {
                let retryable = error.retryable();
                return Err(PublishWindowError {
                    error: error.error(),
                    retryable,
                    requires_clean: false,
                    requires_requeue: retryable,
                });
            }
        }

        Ok(PublishWindowProgress::Complete)
    }

    pub(crate) async fn clean(&mut self) -> Result<(), iggy_connector_sdk::Error> {
        self.commands.send(DriverCommand::Clean).await.map_err(|_| {
            iggy_connector_sdk::Error::Connection(
                "MQTT event loop stopped before it could reconnect".to_string(),
            )
        })
    }

    pub(crate) async fn discard_queued_events(&mut self) -> Result<(), iggy_connector_sdk::Error> {
        discard_queued_events(&mut self.events);
        let (result_sender, result_receiver) = oneshot::channel();
        self.commands
            .send(DriverCommand::DiscardQueuedEvents { result_sender })
            .await
            .map_err(|_| {
                iggy_connector_sdk::Error::Connection(
                    "MQTT event loop stopped before it could discard queued events".to_string(),
                )
            })?;
        result_receiver.await.map_err(|_| {
            iggy_connector_sdk::Error::Connection(
                "MQTT event loop stopped before it could discard queued events".to_string(),
            )
        })?;
        discard_queued_events(&mut self.events);
        Ok(())
    }

    async fn enqueue_publication(
        &mut self,
        publication: &PreparedPublication,
        qos: rumqttc::QoS,
        retain: bool,
    ) -> Result<(), iggy_connector_sdk::Error> {
        match &self.client {
            MqttClient::Mqtt311(client) => client
                .publish(
                    publication.topic.as_str(),
                    publication.payload.clone(),
                    Mqtt311PublishOptions::new(qos).retain(retain),
                )
                .await
                .map_err(|error| iggy_connector_sdk::Error::Connection(error.to_string())),
            MqttClient::Mqtt5(client) => {
                let mut options = Mqtt5PublishOptions::new(mqtt5_qos(qos)).retain(retain);
                if let Some(properties) = publication.properties.clone() {
                    options = options.properties(properties);
                }
                client
                    .publish(
                        publication.topic.as_str(),
                        publication.payload.clone(),
                        options,
                    )
                    .await
                    .map_err(|error| iggy_connector_sdk::Error::Connection(error.to_string()))
            }
        }
    }

    pub(crate) async fn disconnect(
        &mut self,
        disconnect_timeout: Duration,
    ) -> Result<(), iggy_connector_sdk::Error> {
        let (result_sender, result_receiver) = oneshot::channel();
        let _ = self
            .commands
            .send(DriverCommand::Shutdown {
                disconnect_timeout,
                result_sender,
            })
            .await;
        let result = timeout(disconnect_timeout, result_receiver)
            .await
            .map_err(|_| {
                iggy_connector_sdk::Error::Connection(
                    "timed out shutting down MQTT event loop".to_string(),
                )
            })?
            .map_err(|_| {
                iggy_connector_sdk::Error::Connection(
                    "MQTT event loop stopped before disconnect completed".to_string(),
                )
            })?;
        if let Some(mut task) = self.task.take()
            && timeout(disconnect_timeout, &mut task).await.is_err()
        {
            task.abort();
            let _ = task.await;
        }
        result
    }
}

impl MqttEventLoopTask {
    async fn run(mut self) {
        let mut pending_event = None;
        loop {
            tokio::select! {
                biased;
                command = self.commands.recv() => match command {
                    Some(DriverCommand::Clean) => self.connection.clean(),
                    Some(DriverCommand::DiscardQueuedEvents { result_sender }) => {
                        pending_event = None;
                        let _ = result_sender.send(());
                    }
                    Some(DriverCommand::Shutdown {
                        disconnect_timeout,
                        result_sender,
                    }) => {
                        let _ = result_sender.send(self.connection.disconnect(disconnect_timeout).await);
                        return;
                    }
                    None => return,
                },
                permit = self.event_sender.reserve(), if pending_event.is_some() => match permit {
                    Ok(permit) => {
                        if let Some(event) = pending_event.take() {
                            permit.send(event);
                        }
                    }
                    Err(_) => return,
                },
                result = poll_connection_event(
                    &mut self.connection,
                    self.id,
                    self.qos,
                    self.retain,
                    self.configured_max_packet_size,
                    &self.max_packet_size,
                ), if pending_event.is_none() => match result {
                    Ok(DriverEvent::Other) => {}
                    Ok(event) => {
                        pending_event = Some(event);
                    }
                    Err(error) => {
                        self.connection.clean();
                        pending_event = Some(DriverEvent::ConnectionError(error));
                    }
                },
            }
        }
    }
}

fn discard_queued_events(events: &mut mpsc::Receiver<DriverEvent>) {
    while events.try_recv().is_ok() {}
}

async fn poll_connection_event(
    connection: &mut MqttConnection,
    id: u32,
    qos: u8,
    retain: bool,
    configured_max_packet_size: usize,
    max_packet_size: &AtomicUsize,
) -> Result<DriverEvent, iggy_connector_sdk::Error> {
    match connection {
        MqttConnection::Mqtt311 { event_loop, .. } => match event_loop
            .poll()
            .await
            .map_err(|error| iggy_connector_sdk::Error::Connection(error.to_string()))?
        {
            Mqtt311Event::Outgoing(Mqtt311Outgoing::Publish(packet_id)) => {
                Ok(DriverEvent::OutgoingPublish(packet_id))
            }
            Mqtt311Event::Incoming(Mqtt311Incoming::ConnAck(connack)) => {
                Ok(DriverEvent::SessionPresent(connack.session_present))
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
            .map_err(|error| mqtt5_connection_error(error, id))?
        {
            Mqtt5Event::Outgoing(Mqtt5Outgoing::Publish(packet_id)) => {
                Ok(DriverEvent::OutgoingPublish(packet_id))
            }
            Mqtt5Event::Incoming(Mqtt5Incoming::ConnAck(connack)) => {
                validate_mqtt5_connack_capabilities(connack.properties.as_ref(), qos, retain, id)?;
                let packet_size = connack
                    .properties
                    .as_ref()
                    .and_then(|properties| properties.max_packet_size)
                    .map(|size| configured_max_packet_size.min(size as usize))
                    .unwrap_or(configured_max_packet_size);
                max_packet_size.store(packet_size, Ordering::Relaxed);
                Ok(DriverEvent::SessionPresent(connack.session_present))
            }
            Mqtt5Event::Incoming(Mqtt5Incoming::PubAck(ack)) => Ok(DriverEvent::Mqtt5PubAck {
                packet_id: ack.pkid,
                reason: ack.reason,
            }),
            Mqtt5Event::Incoming(Mqtt5Incoming::PubRec(rec)) => Ok(DriverEvent::Mqtt5PubRec {
                packet_id: rec.pkid,
                reason: rec.reason,
            }),
            Mqtt5Event::Incoming(Mqtt5Incoming::PubComp(comp)) => Ok(DriverEvent::Mqtt5PubComp {
                packet_id: comp.pkid,
                reason: comp.reason,
            }),
            Mqtt5Event::Auth(_) | Mqtt5Event::Incoming(_) | Mqtt5Event::Outgoing(_) => {
                Ok(DriverEvent::Other)
            }
        },
    }
}

pub(crate) fn publication_packet_size(
    publication: &PreparedPublication,
    protocol: MqttProtocol,
    qos: u8,
    retain: bool,
) -> usize {
    match protocol {
        MqttProtocol::Mqtt311 => {
            let mut publish = rumqttc::mqttbytes::v4::Publish::from_bytes(
                publication.topic.as_str(),
                mqtt311_qos(qos),
                publication.payload.clone(),
            );
            publish.retain = retain;
            if qos > 0 {
                publish.pkid = 1;
            }
            publish.size()
        }
        MqttProtocol::Mqtt5 => {
            let mut publish = rumqttc::v5::mqttbytes::v5::Publish::new(
                publication.topic.as_str(),
                mqtt5_qos(mqtt311_qos(qos)),
                publication.payload.clone(),
                publication.properties.clone(),
            );
            publish.retain = retain;
            if qos > 0 {
                publish.pkid = 1;
            }
            publish.size()
        }
    }
}

fn mqtt311_qos(qos: u8) -> rumqttc::QoS {
    match qos {
        0 => rumqttc::QoS::AtMostOnce,
        1 => rumqttc::QoS::AtLeastOnce,
        2 => rumqttc::QoS::ExactlyOnce,
        _ => unreachable!("MQTT QoS is validated before publication preparation"),
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

fn tls_config(
    config: &MqttSinkConfig,
) -> Result<Option<Arc<ClientConfig>>, iggy_connector_sdk::Error> {
    let broker_url = url::Url::parse(&config.broker_url).map_err(|error| {
        iggy_connector_sdk::Error::InvalidConfigValue(format!("broker_url: {error}"))
    })?;
    if !matches!(broker_url.scheme(), "mqtts" | "ssl") {
        return Ok(None);
    }
    let tls = config.tls.as_ref();
    let ca = tls
        .and_then(|tls| tls.ca_file.as_deref())
        .map(|path| read_certificate_file(path, "tls.ca_file"))
        .transpose()?;
    let client_auth = match tls.map(|tls| (&tls.client_cert_file, &tls.client_key_file)) {
        None => None,
        Some((Some(certificate_path), Some(key_path))) => Some((
            read_certificate_file(certificate_path, "tls.client_cert_file")?,
            read_private_key_file(key_path, "tls.client_key_file")?,
        )),
        Some((None, None)) => None,
        _ => unreachable!("TLS configuration was validated before loading files"),
    };

    Ok(Some(Arc::new(build_tls_config(ca, client_auth)?)))
}

fn tls_transport_v4(
    config: &MqttSinkConfig,
) -> Result<Option<Mqtt311Transport>, iggy_connector_sdk::Error> {
    tls_config(config).map(|config| {
        config.map(|config| {
            Mqtt311Transport::tls_with_config(Mqtt311TlsConfiguration::Rustls(config))
        })
    })
}

fn tls_transport_v5(
    config: &MqttSinkConfig,
) -> Result<Option<rumqttc_v5::Transport>, iggy_connector_sdk::Error> {
    tls_config(config).map(|config| {
        config.map(|config| {
            rumqttc_v5::Transport::tls_with_config(rumqttc_v5::TlsConfiguration::Rustls(config))
        })
    })
}

fn broker_url_with_client_id(
    broker_url: &str,
    client_id: &str,
) -> Result<String, iggy_connector_sdk::Error> {
    let mut url = url::Url::parse(broker_url).map_err(|error| {
        iggy_connector_sdk::Error::InvalidConfigValue(format!("broker_url is invalid: {error}"))
    })?;
    if matches!(url.scheme(), "mqtts" | "ssl") {
        let port = url.port().unwrap_or(8883);
        url.set_scheme("mqtt").map_err(|_| {
            iggy_connector_sdk::Error::InvalidConfigValue(
                "broker_url TLS scheme could not be normalized".to_string(),
            )
        })?;
        url.set_port(Some(port)).map_err(|_| {
            iggy_connector_sdk::Error::InvalidConfigValue(
                "broker_url TLS port could not be preserved".to_string(),
            )
        })?;
    }
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

fn mqtt5_puback_rejection(packet_id: u16, reason: PubAckReason) -> PublishWindowFailure {
    let message = format!("MQTT 5 PUBACK rejected packet ID {packet_id}: {reason:?}");
    match reason {
        PubAckReason::UnspecifiedError
        | PubAckReason::ImplementationSpecificError
        | PubAckReason::QuotaExceeded => PublishWindowFailure::TransientBrokerRejected(message),
        _ => PublishWindowFailure::BrokerRejected(message),
    }
}

fn mqtt5_pubrec_rejection(packet_id: u16, reason: PubRecReason) -> PublishWindowFailure {
    let message = format!("MQTT 5 PUBREC rejected packet ID {packet_id}: {reason:?}");
    match reason {
        PubRecReason::UnspecifiedError
        | PubRecReason::ImplementationSpecificError
        | PubRecReason::QuotaExceeded => PublishWindowFailure::TransientBrokerRejected(message),
        _ => PublishWindowFailure::BrokerRejected(message),
    }
}

async fn wait_for_mqtt311_disconnect(
    event_loop: &mut Mqtt311EventLoop,
    disconnect_timeout: Duration,
) -> Result<(), iggy_connector_sdk::Error> {
    timeout(disconnect_timeout, async {
        loop {
            match event_loop.poll().await {
                Ok(Mqtt311Event::Outgoing(Mqtt311Outgoing::Disconnect)) => return Ok(()),
                Ok(_) => {}
                Err(error) => {
                    return Err(iggy_connector_sdk::Error::Connection(error.to_string()));
                }
            }
        }
    })
    .await
    .map_err(|_| {
        iggy_connector_sdk::Error::Connection(
            "timed out waiting for MQTT 3.1.1 DISCONNECT to flush".to_string(),
        )
    })?
}

async fn wait_for_mqtt5_disconnect(
    event_loop: &mut Mqtt5EventLoop,
    disconnect_timeout: Duration,
) -> Result<(), iggy_connector_sdk::Error> {
    timeout(disconnect_timeout, async {
        loop {
            match event_loop.poll().await {
                Ok(Mqtt5Event::Outgoing(Mqtt5Outgoing::Disconnect)) => return Ok(()),
                Ok(_) => {}
                Err(error) => {
                    return Err(iggy_connector_sdk::Error::Connection(error.to_string()));
                }
            }
        }
    })
    .await
    .map_err(|_| {
        iggy_connector_sdk::Error::Connection(
            "timed out waiting for MQTT 5 DISCONNECT to flush".to_string(),
        )
    })?
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
    qos: u8,
    retain: bool,
) -> Result<Option<u32>, iggy_connector_sdk::Error> {
    timeout(connect_timeout, async {
        loop {
            match event_loop.poll().await {
                Ok(Mqtt5Event::Incoming(Mqtt5Incoming::ConnAck(connack))) => {
                    mqtt5_connack_result(connack.code, id)?;
                    validate_mqtt5_connack_capabilities(
                        connack.properties.as_ref(),
                        qos,
                        retain,
                        id,
                    )?;
                    return Ok(connack
                        .properties
                        .as_ref()
                        .and_then(|properties| properties.max_packet_size));
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
        Mqtt5ConnectionError::MqttState(Mqtt5StateError::ServerDisconnect {
            reason_code:
                DisconnectReasonCode::RetainNotSupported | DisconnectReasonCode::QoSNotSupported,
            reason_string,
        }) => iggy_connector_sdk::Error::InvalidConfigValue(format!(
            "MQTT 5 broker rejected the configured publication policy for connector ID {id}: {reason_string:?}"
        )),
        error => iggy_connector_sdk::Error::Connection(format!(
            "MQTT 5 connection failed for connector ID {id}: {error}"
        )),
    }
}

fn mqtt_tls_connection_error(error: rumqttc::TlsError, id: u32) -> iggy_connector_sdk::Error {
    if let rumqttc::TlsError::Io(io_error) = &error
        && io_error
            .get_ref()
            .and_then(|source| source.downcast_ref::<rustls::Error>())
            .is_none()
    {
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

fn validate_mqtt5_connack_capabilities(
    properties: Option<&rumqttc::v5::mqttbytes::v5::ConnAckProperties>,
    qos: u8,
    retain: bool,
    id: u32,
) -> Result<(), iggy_connector_sdk::Error> {
    validate_mqtt5_broker_capabilities(
        properties.and_then(|properties| properties.max_qos),
        properties.and_then(|properties| properties.retain_available),
        qos,
        retain,
        id,
    )
}

fn validate_mqtt5_broker_capabilities(
    max_qos: Option<u8>,
    retain_available: Option<u8>,
    qos: u8,
    retain: bool,
    id: u32,
) -> Result<(), iggy_connector_sdk::Error> {
    if let Some(max_qos) = max_qos
        && qos > max_qos
    {
        return Err(iggy_connector_sdk::Error::InvalidConfigValue(format!(
            "MQTT 5 broker maximum QoS {max_qos} is lower than configured qos {qos} for connector ID {id}"
        )));
    }
    if retain && retain_available == Some(0) {
        return Err(iggy_connector_sdk::Error::InvalidConfigValue(format!(
            "MQTT 5 broker does not support retained publications for connector ID {id}"
        )));
    }
    Ok(())
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
        ConnectReturnCode, DisconnectReasonCode, PubAckReason, PubCompReason, PubRecReason,
    };

    use super::{
        DriverEvent, Mqtt311Options, PublishWindowFailure, PublishWindowState,
        broker_url_with_client_id, mqtt5_connack_error, mqtt5_connection_error,
        mqtt311_connection_error, read_certificate_file, read_private_key_file, rumqttc,
        validate_mqtt5_broker_capabilities,
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
    fn given_wrapped_tls_certificate_failure_when_classifying_should_return_configuration_error() {
        let certificate_error = rumqttc::tokio_rustls::rustls::Error::InvalidCertificate(
            rumqttc::tokio_rustls::rustls::CertificateError::UnknownIssuer,
        );
        let error = mqtt5_connection_error(
            rumqttc::v5::ConnectionError::Tls(rumqttc::TlsError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                certificate_error,
            ))),
            1,
        );

        assert!(matches!(
            error,
            iggy_connector_sdk::Error::InvalidConfigValue(message)
                if message.contains("TLS validation")
        ));
    }

    #[test]
    fn given_out_of_order_qos_one_acknowledgements_when_applied_should_confirm_matching_publications()
     {
        let mut state = PublishWindowState::new(3, rumqttc::QoS::AtLeastOnce);

        state.submitted(0);
        state.submitted(1);
        state.submitted(2);
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

        state.submitted(0);
        state.submitted(1);
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

        state.submitted(0);
        state.apply(DriverEvent::OutgoingPublish(0)).unwrap();

        assert!(state.is_complete());
    }

    #[test]
    fn given_outgoing_publish_beyond_window_when_applied_should_return_protocol_error() {
        let mut state = PublishWindowState::new(1, rumqttc::QoS::AtLeastOnce);

        state.submitted(0);
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
        state.submitted(0);
        state.apply(DriverEvent::OutgoingPublish(71)).unwrap();

        assert!(!state.is_complete());
    }

    #[test]
    fn given_duplicate_confirmation_should_count_each_publication_once() {
        let mut state = PublishWindowState::new(2, rumqttc::QoS::AtLeastOnce);

        state.confirm(0);
        state.confirm(0);

        assert!(!state.is_complete());

        state.confirm(1);

        assert!(state.is_complete());
    }

    #[test]
    fn given_session_reset_when_rumqttc_replays_publish_should_keep_its_packet_mapping() {
        let mut state = PublishWindowState::new(1, rumqttc::QoS::ExactlyOnce);
        state.submitted(0);
        state.apply(DriverEvent::OutgoingPublish(81)).unwrap();

        state.apply(DriverEvent::OutgoingPublish(81)).unwrap();
        state.apply(DriverEvent::PubComp(81)).unwrap();

        assert!(state.is_complete());
    }

    #[test]
    fn given_mqtt_five_no_matching_subscribers_puback_when_applied_should_confirm_publication() {
        let mut state = PublishWindowState::new(1, rumqttc::QoS::AtLeastOnce);
        state.submitted(0);
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
        state.submitted(0);
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
    fn given_mqtt_five_quota_exceeded_puback_when_applied_should_request_retry() {
        let mut state = PublishWindowState::new(1, rumqttc::QoS::AtLeastOnce);
        state.submitted(0);
        state.apply(DriverEvent::OutgoingPublish(64)).unwrap();

        let error = state
            .apply(DriverEvent::Mqtt5PubAck {
                packet_id: 64,
                reason: PubAckReason::QuotaExceeded,
            })
            .unwrap_err();

        assert!(error.retryable());
        assert!(matches!(
            error.error(),
            iggy_connector_sdk::Error::Connection(message)
                if message.contains("QuotaExceeded")
        ));
    }

    #[test]
    fn given_mqtt_five_rejected_pubrec_when_applied_should_return_error_without_confirmation() {
        let mut state = PublishWindowState::new(1, rumqttc::QoS::ExactlyOnce);
        state.submitted(0);
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
    fn given_mqtt_five_quota_exceeded_pubrec_when_applied_should_request_retry() {
        let mut state = PublishWindowState::new(1, rumqttc::QoS::ExactlyOnce);
        state.submitted(0);
        state.apply(DriverEvent::OutgoingPublish(65)).unwrap();

        let error = state
            .apply(DriverEvent::Mqtt5PubRec {
                packet_id: 65,
                reason: PubRecReason::QuotaExceeded,
            })
            .unwrap_err();

        assert!(error.retryable());
        assert!(matches!(
            error.error(),
            iggy_connector_sdk::Error::Connection(message)
                if message.contains("QuotaExceeded")
        ));
    }

    #[test]
    fn given_mqtt_five_rejected_pubcomp_when_applied_should_return_error_without_confirmation() {
        let mut state = PublishWindowState::new(1, rumqttc::QoS::ExactlyOnce);
        state.submitted(0);
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
    fn given_mqtt_five_broker_qos_limit_below_sink_qos_should_return_configuration_error() {
        let error = validate_mqtt5_broker_capabilities(Some(1), None, 2, false, 1)
            .expect_err("broker QoS limit must reject a higher configured QoS");

        assert!(matches!(
            error,
            iggy_connector_sdk::Error::InvalidConfigValue(message)
                if message.contains("maximum QoS 1")
        ));
    }

    #[test]
    fn given_mqtt_five_broker_without_retain_support_should_return_configuration_error() {
        let error = validate_mqtt5_broker_capabilities(None, Some(0), 1, true, 1)
            .expect_err("broker retain policy must reject retained publications");

        assert!(matches!(
            error,
            iggy_connector_sdk::Error::InvalidConfigValue(message)
                if message.contains("does not support retained")
        ));
    }

    #[test]
    fn given_mqtt_five_policy_disconnect_should_return_configuration_error() {
        let qos_error = mqtt5_connection_error(
            rumqttc::v5::ConnectionError::MqttState(rumqttc::v5::StateError::ServerDisconnect {
                reason_code: DisconnectReasonCode::QoSNotSupported,
                reason_string: None,
            }),
            1,
        );
        let retain_error = mqtt5_connection_error(
            rumqttc::v5::ConnectionError::MqttState(rumqttc::v5::StateError::ServerDisconnect {
                reason_code: DisconnectReasonCode::RetainNotSupported,
                reason_string: None,
            }),
            1,
        );

        assert!(matches!(
            qos_error,
            iggy_connector_sdk::Error::InvalidConfigValue(_)
        ));
        assert!(matches!(
            retain_error,
            iggy_connector_sdk::Error::InvalidConfigValue(_)
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

    #[test]
    fn given_mqtts_broker_url_when_building_driver_options_should_preserve_tls_port() {
        let broker_url = broker_url_with_client_id("mqtts://broker.example", "sink-client")
            .expect("broker URL must accept a client identifier");

        assert_eq!(
            url::Url::parse(&broker_url)
                .expect("broker URL must parse")
                .port(),
            Some(8883)
        );
    }

    #[test]
    fn given_idle_reconnect_events_when_starting_window_should_discard_them() {
        let (sender, mut events) = tokio::sync::mpsc::channel(2);
        sender
            .try_send(DriverEvent::ConnectionError(
                iggy_connector_sdk::Error::Connection("idle reconnect".to_string()),
            ))
            .expect("idle connection error must queue");
        sender
            .try_send(DriverEvent::SessionPresent(false))
            .expect("idle session event must queue");

        super::discard_queued_events(&mut events);

        assert!(matches!(
            events.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Empty)
        ));
    }
}
