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
    AsyncClient as Mqtt5Client, Event as Mqtt5Event, EventLoop as Mqtt5EventLoop,
    Incoming as Mqtt5Incoming, MqttOptions as Mqtt5Options,
    mqttbytes::v5::{PubAckReason, PubCompReason},
};
use rumqttc::{
    AsyncClient as Mqtt311Client, Event as Mqtt311Event, EventLoop as Mqtt311EventLoop,
    Incoming as Mqtt311Incoming, MqttOptions as Mqtt311Options, Outgoing,
};
use secrecy::ExposeSecret;
use serde::Deserialize;
use std::{
    collections::{HashMap, VecDeque},
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

    fn apply(&mut self, event: DriverEvent) -> Result<(), iggy_connector_sdk::Error> {
        match event {
            DriverEvent::OutgoingPublish(packet_id) => {
                let Some(index) = self.submitted.pop_front() else {
                    return Err(iggy_connector_sdk::Error::Connection(
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
                        return Err(iggy_connector_sdk::Error::Connection(format!(
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
                        return Err(iggy_connector_sdk::Error::Connection(format!(
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
                });
            }
            let event = match timeout(remaining, self.poll_event()).await {
                Ok(Ok(event)) => event,
                Ok(Err(error)) => {
                    return Err(PublishWindowError {
                        confirmed: state.confirmed_indexes(),
                        error,
                    });
                }
                Err(_) => {
                    return Err(PublishWindowError {
                        confirmed: state.confirmed_indexes(),
                        error: iggy_connector_sdk::Error::Connection(
                            "timed out waiting for MQTT publication acknowledgements".to_string(),
                        ),
                    });
                }
            };

            if let Err(error) = state.apply(event) {
                return Err(PublishWindowError {
                    confirmed: state.confirmed_indexes(),
                    error,
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

    pub async fn reconnect(&mut self) -> Result<(), iggy_connector_sdk::Error> {
        // rumqttc reconnects when the existing event loop is polled. Keeping the
        // event loop and its pending QoS state is important for duplicate-safe retry.
        match &mut self.connection {
            MqttConnection::Mqtt311 { event_loop, .. } => {
                timeout(Duration::from_secs(30), event_loop.poll())
                    .await
                    .map_err(|_| {
                        iggy_connector_sdk::Error::Connection(
                            "timed out while reconnecting to MQTT broker".to_string(),
                        )
                    })?
                    .map_err(|error| iggy_connector_sdk::Error::Connection(error.to_string()))?;
            }
            MqttConnection::Mqtt5 { event_loop, .. } => {
                timeout(Duration::from_secs(30), event_loop.poll())
                    .await
                    .map_err(|_| {
                        iggy_connector_sdk::Error::Connection(
                            "timed out while reconnecting to MQTT broker".to_string(),
                        )
                    })?
                    .map_err(|error| iggy_connector_sdk::Error::Connection(error.to_string()))?;
            }
        }
        Ok(())
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
                Ok(Mqtt311Event::Incoming(Mqtt311Incoming::ConnAck(_))) => return Ok(()),
                Ok(_) => {}
                Err(error) => {
                    return Err(iggy_connector_sdk::Error::Connection(format!(
                        "MQTT 3.1.1 connection failed for connector ID {id}: {error}"
                    )));
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
                Ok(Mqtt5Event::Incoming(Mqtt5Incoming::ConnAck(_))) => return Ok(()),
                Ok(_) => {}
                Err(error) => {
                    return Err(iggy_connector_sdk::Error::Connection(format!(
                        "MQTT 5 connection failed for connector ID {id}: {error}"
                    )));
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

#[cfg(test)]
mod tests {
    use rumqttc::v5::mqttbytes::v5::{PubAckReason, PubCompReason};

    use super::{DriverEvent, PublishWindowState};

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
            iggy_connector_sdk::Error::Connection(message)
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
            iggy_connector_sdk::Error::Connection(message)
                if message.contains("PUBCOMP rejected")
        ));
        assert!(!state.is_complete());
    }
}
