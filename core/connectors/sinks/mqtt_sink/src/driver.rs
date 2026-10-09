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

use rumqttc::v5::mqttbytes::v5::PublishProperties;
use rumqttc::v5::{
    AsyncClient as Mqtt5Client, Event as Mqtt5Event, EventLoop as Mqtt5EventLoop,
    Incoming as Mqtt5Incoming, MqttOptions as Mqtt5Options,
};
use rumqttc::{
    AsyncClient as Mqtt311Client, Event as Mqtt311Event, EventLoop as Mqtt311EventLoop,
    Incoming as Mqtt311Incoming, MqttOptions as Mqtt311Options, Outgoing,
};
use secrecy::ExposeSecret;
use serde::Deserialize;
use std::time::Duration;
use tokio::time::timeout;

use crate::MqttSinkConfig;

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

    pub async fn publish(
        &mut self,
        topic: &str,
        qos: rumqttc::QoS,
        retain: bool,
        payload: Vec<u8>,
        properties: Option<PublishProperties>,
        publish_timeout: Duration,
    ) -> Result<(), iggy_connector_sdk::Error> {
        // The event loop must be polled until the broker acknowledgement is
        // observed. Calling AsyncClient::publish alone only queues a request.
        match &mut self.connection {
            MqttConnection::Mqtt311 { client, event_loop } => {
                client
                    .publish(topic, qos, retain, payload)
                    .await
                    .map_err(|error| iggy_connector_sdk::Error::Connection(error.to_string()))?;
                wait_for_publish_ack(event_loop, qos, publish_timeout).await
            }
            MqttConnection::Mqtt5 { client, event_loop } => {
                if let Some(properties) = properties {
                    client
                        .publish_with_properties(topic, mqtt5_qos(qos), retain, payload, properties)
                        .await
                        .map_err(|error| {
                            iggy_connector_sdk::Error::Connection(error.to_string())
                        })?;
                } else {
                    client
                        .publish(topic, mqtt5_qos(qos), retain, payload)
                        .await
                        .map_err(|error| {
                            iggy_connector_sdk::Error::Connection(error.to_string())
                        })?;
                }
                wait_for_publish_ack_v5(event_loop, qos, publish_timeout).await
            }
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

async fn wait_for_publish_ack(
    event_loop: &mut Mqtt311EventLoop,
    qos: rumqttc::QoS,
    publish_timeout: Duration,
) -> Result<(), iggy_connector_sdk::Error> {
    // QoS 0 has no broker acknowledgement, but observing the outgoing event
    // confirms that rumqttc accepted the publication for transmission.
    if qos == rumqttc::QoS::AtMostOnce {
        timeout(publish_timeout, wait_for_outgoing_publish(event_loop))
            .await
            .map_err(|_| {
                iggy_connector_sdk::Error::Connection(
                    "timed out while sending MQTT QoS 0 publish".to_string(),
                )
            })??;
        return Ok(());
    }

    let packet_id = timeout(publish_timeout, wait_for_outgoing_publish(event_loop))
        .await
        .map_err(|_| {
            iggy_connector_sdk::Error::Connection(
                "timed out while sending MQTT publish".to_string(),
            )
        })??;
    timeout(publish_timeout, async {
        loop {
            match event_loop.poll().await {
                Ok(Mqtt311Event::Incoming(Mqtt311Incoming::PubAck(ack)))
                    if ack.pkid == packet_id && qos == rumqttc::QoS::AtLeastOnce =>
                {
                    return Ok(());
                }
                Ok(Mqtt311Event::Incoming(Mqtt311Incoming::PubComp(comp)))
                    if comp.pkid == packet_id && qos == rumqttc::QoS::ExactlyOnce =>
                {
                    return Ok(());
                }
                Ok(_) => {}
                Err(error) => {
                    return Err(iggy_connector_sdk::Error::Connection(error.to_string()));
                }
            }
        }
    })
    .await
    .map_err(|_| {
        iggy_connector_sdk::Error::Connection("timed out waiting for MQTT acknowledgement".into())
    })?
}

async fn wait_for_publish_ack_v5(
    event_loop: &mut Mqtt5EventLoop,
    qos: rumqttc::QoS,
    publish_timeout: Duration,
) -> Result<(), iggy_connector_sdk::Error> {
    if qos == rumqttc::QoS::AtMostOnce {
        timeout(publish_timeout, wait_for_outgoing_publish_v5(event_loop))
            .await
            .map_err(|_| {
                iggy_connector_sdk::Error::Connection(
                    "timed out while sending MQTT QoS 0 publish".to_string(),
                )
            })??;
        return Ok(());
    }

    let packet_id = timeout(publish_timeout, wait_for_outgoing_publish_v5(event_loop))
        .await
        .map_err(|_| {
            iggy_connector_sdk::Error::Connection(
                "timed out while sending MQTT publish".to_string(),
            )
        })??;
    timeout(publish_timeout, async {
        loop {
            match event_loop.poll().await {
                Ok(Mqtt5Event::Incoming(Mqtt5Incoming::PubAck(ack)))
                    if ack.pkid == packet_id && qos == rumqttc::QoS::AtLeastOnce =>
                {
                    return Ok(());
                }
                Ok(Mqtt5Event::Incoming(Mqtt5Incoming::PubComp(comp)))
                    if comp.pkid == packet_id && qos == rumqttc::QoS::ExactlyOnce =>
                {
                    return Ok(());
                }
                Ok(_) => {}
                Err(error) => {
                    return Err(iggy_connector_sdk::Error::Connection(error.to_string()));
                }
            }
        }
    })
    .await
    .map_err(|_| {
        iggy_connector_sdk::Error::Connection("timed out waiting for MQTT acknowledgement".into())
    })?
}

async fn wait_for_outgoing_publish(
    event_loop: &mut Mqtt311EventLoop,
) -> Result<u16, iggy_connector_sdk::Error> {
    // Capture the packet identifier before waiting for PUBACK/PUBCOMP so
    // acknowledgements from other in-flight operations are ignored.
    loop {
        match event_loop
            .poll()
            .await
            .map_err(|error| iggy_connector_sdk::Error::Connection(error.to_string()))?
        {
            Mqtt311Event::Outgoing(Outgoing::Publish(packet_id)) => return Ok(packet_id),
            Mqtt311Event::Incoming(_) | Mqtt311Event::Outgoing(_) => {}
        }
    }
}

async fn wait_for_outgoing_publish_v5(
    event_loop: &mut Mqtt5EventLoop,
) -> Result<u16, iggy_connector_sdk::Error> {
    loop {
        match event_loop
            .poll()
            .await
            .map_err(|error| iggy_connector_sdk::Error::Connection(error.to_string()))?
        {
            Mqtt5Event::Outgoing(Outgoing::Publish(packet_id)) => return Ok(packet_id),
            Mqtt5Event::Incoming(_) | Mqtt5Event::Outgoing(_) => {}
        }
    }
}
