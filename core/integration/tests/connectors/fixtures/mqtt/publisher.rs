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

use rumqttc_v4::{
    AsyncClient as Mqtt311Client, Broker as Mqtt311Broker, Event as Mqtt311Event,
    Incoming as Mqtt311Incoming, MqttOptions as Mqtt311Options, Outgoing as Mqtt311Outgoing,
    PublishOptions as Mqtt311PublishOptions, mqttbytes::QoS as MqttQos,
};
use rumqttc_v5::{
    AsyncClient as Mqtt5Client, Broker as Mqtt5Broker, Event as Mqtt5Event,
    Incoming as Mqtt5Incoming, MqttOptions as Mqtt5Options, Outgoing as Mqtt5Outgoing,
    PublishOptions as Mqtt5PublishOptions, mqttbytes::v5::PublishProperties,
};
use std::time::Duration;
use tokio::time::timeout;
use uuid::Uuid;

pub(super) async fn can_connect(broker_url: &str, username: &str, password: &str) -> bool {
    // MQTT 5 is sufficient for readiness. The source protocol matrix is tested
    // by publish(), while this probe only checks broker availability and auth.
    connect_mqtt5(broker_url, username, password).await.is_ok()
}

#[derive(Debug, Clone, Copy)]
pub(super) enum Protocol {
    Mqtt311,
    Mqtt5,
}

pub(super) async fn publish(
    broker_url: &str,
    topic: &str,
    protocol: Protocol,
    qos: u8,
    payload: &[u8],
    username: &str,
    password: &str,
) -> Result<(), String> {
    // A successful return means the publisher observed broker-level completion.
    // It does not mean that Iggy has persisted the message yet.
    match protocol {
        Protocol::Mqtt311 => {
            publish_mqtt311(broker_url, topic, qos, payload, username, password).await
        }
        Protocol::Mqtt5 => publish_mqtt5(broker_url, topic, qos, payload, username, password).await,
    }
}

pub(super) async fn publish_batch(
    broker_url: &str,
    topic: &str,
    protocol: Protocol,
    qos: u8,
    payloads: &[Vec<u8>],
    username: &str,
    password: &str,
) -> Result<(), String> {
    match protocol {
        Protocol::Mqtt311 => {
            let (client, mut event_loop) = connect_mqtt311(broker_url, username, password).await?;
            for payload in payloads {
                client
                    .publish(
                        topic,
                        payload.clone(),
                        Mqtt311PublishOptions::new(mqtt_qos(qos)?),
                    )
                    .await
                    .map_err(|error| {
                        format!("failed to enqueue MQTT 3.1.1 batch publish: {error}")
                    })?;
            }
            wait_for_mqtt311_publishes(&mut event_loop, qos, payloads.len()).await
        }
        Protocol::Mqtt5 => {
            let (client, mut event_loop) = connect_mqtt5(broker_url, username, password).await?;
            for payload in payloads {
                client
                    .publish(
                        topic,
                        payload.clone(),
                        Mqtt5PublishOptions::new(mqtt_qos(qos)?),
                    )
                    .await
                    .map_err(|error| format!("failed to enqueue MQTT 5 batch publish: {error}"))?;
            }
            wait_for_mqtt5_publishes(&mut event_loop, qos, payloads.len()).await
        }
    }
}

pub(super) async fn publish_mixed_batch(
    broker_url: &str,
    topic: &str,
    protocol: Protocol,
    messages: &[(u8, Vec<u8>)],
    username: &str,
    password: &str,
) -> Result<(), String> {
    match protocol {
        Protocol::Mqtt311 => {
            let (client, mut event_loop) = connect_mqtt311(broker_url, username, password).await?;
            for (qos, payload) in messages {
                client
                    .publish(
                        topic,
                        payload.clone(),
                        Mqtt311PublishOptions::new(mqtt_qos(*qos)?),
                    )
                    .await
                    .map_err(|error| {
                        format!("failed to enqueue mixed MQTT 3.1.1 publish: {error}")
                    })?;
            }
            wait_for_mqtt311_mixed_publishes(&mut event_loop, messages).await
        }
        Protocol::Mqtt5 => {
            let (client, mut event_loop) = connect_mqtt5(broker_url, username, password).await?;
            for (qos, payload) in messages {
                client
                    .publish(
                        topic,
                        payload.clone(),
                        Mqtt5PublishOptions::new(mqtt_qos(*qos)?),
                    )
                    .await
                    .map_err(|error| format!("failed to enqueue mixed MQTT 5 publish: {error}"))?;
            }
            wait_for_mqtt5_mixed_publishes(&mut event_loop, messages).await
        }
    }
}

async fn publish_mqtt311(
    broker_url: &str,
    topic: &str,
    qos: u8,
    payload: &[u8],
    username: &str,
    password: &str,
) -> Result<(), String> {
    let (client, mut event_loop) = connect_mqtt311(broker_url, username, password).await?;
    client
        .publish(
            topic,
            payload.to_vec(),
            Mqtt311PublishOptions::new(mqtt_qos(qos)?),
        )
        .await
        .map_err(|error| format!("failed to enqueue MQTT 3.1.1 publish: {error}"))?;

    wait_for_mqtt311_publishes(&mut event_loop, qos, 1).await
}

async fn publish_mqtt5(
    broker_url: &str,
    topic: &str,
    qos: u8,
    payload: &[u8],
    username: &str,
    password: &str,
) -> Result<(), String> {
    let (client, mut event_loop) = connect_mqtt5(broker_url, username, password).await?;
    client
        .publish(
            topic,
            payload.to_vec(),
            Mqtt5PublishOptions::new(mqtt_qos(qos)?),
        )
        .await
        .map_err(|error| format!("failed to enqueue MQTT 5 publish: {error}"))?;

    wait_for_mqtt5_publishes(&mut event_loop, qos, 1).await
}

pub(super) async fn publish_mqtt5_with_properties(
    broker_url: &str,
    topic: &str,
    qos: u8,
    payload: &[u8],
    properties: PublishProperties,
    username: &str,
    password: &str,
) -> Result<(), String> {
    let (client, mut event_loop) = connect_mqtt5(broker_url, username, password).await?;
    client
        .publish(
            topic,
            payload.to_vec(),
            Mqtt5PublishOptions::new(mqtt_qos(qos)?).properties(properties),
        )
        .await
        .map_err(|error| format!("failed to enqueue MQTT 5 publish: {error}"))?;

    wait_for_mqtt5_publishes(&mut event_loop, qos, 1).await
}

fn mqtt_qos(qos: u8) -> Result<MqttQos, String> {
    match qos {
        0 => Ok(MqttQos::AtMostOnce),
        1 => Ok(MqttQos::AtLeastOnce),
        2 => Ok(MqttQos::ExactlyOnce),
        qos => Err(format!("unsupported MQTT QoS: {qos}")),
    }
}

async fn wait_for_mqtt311_publishes(
    event_loop: &mut rumqttc_v4::EventLoop,
    qos: u8,
    expected: usize,
) -> Result<(), String> {
    let mut completed = 0;
    timeout(Duration::from_secs(10), async {
        while completed < expected {
            match event_loop
                .poll()
                .await
                .map_err(|error| format!("failed to publish MQTT 3.1.1 batch: {error}"))?
            {
                Mqtt311Event::Outgoing(Mqtt311Outgoing::Publish(_)) if qos == 0 => completed += 1,
                Mqtt311Event::Incoming(Mqtt311Incoming::PubAck(_)) if qos == 1 => completed += 1,
                Mqtt311Event::Incoming(Mqtt311Incoming::PubComp(_)) if qos == 2 => completed += 1,
                _ => {}
            }
        }
        Ok::<(), String>(())
    })
    .await
    .map_err(|_| format!("timed out waiting for MQTT 3.1.1 batch QoS {qos} completion"))?
}

async fn wait_for_mqtt311_mixed_publishes(
    event_loop: &mut rumqttc_v4::EventLoop,
    messages: &[(u8, Vec<u8>)],
) -> Result<(), String> {
    // A mixed batch has different broker handshakes, so completion is counted
    // separately for each QoS instead of using one aggregate event count.
    let expected_qos0 = messages.iter().filter(|(qos, _)| *qos == 0).count();
    let expected_qos1 = messages.iter().filter(|(qos, _)| *qos == 1).count();
    let expected_qos2 = messages.iter().filter(|(qos, _)| *qos == 2).count();
    let mut completed_qos0 = 0;
    let mut completed_qos1 = 0;
    let mut completed_qos2 = 0;
    timeout(Duration::from_secs(10), async {
        while completed_qos0 < expected_qos0
            || completed_qos1 < expected_qos1
            || completed_qos2 < expected_qos2
        {
            match event_loop
                .poll()
                .await
                .map_err(|error| format!("failed to publish mixed MQTT 3.1.1 batch: {error}"))?
            {
                Mqtt311Event::Outgoing(Mqtt311Outgoing::Publish(_))
                    if completed_qos0 < expected_qos0 =>
                {
                    completed_qos0 += 1
                }
                Mqtt311Event::Incoming(Mqtt311Incoming::PubAck(_))
                    if completed_qos1 < expected_qos1 =>
                {
                    completed_qos1 += 1
                }
                Mqtt311Event::Incoming(Mqtt311Incoming::PubComp(_))
                    if completed_qos2 < expected_qos2 =>
                {
                    completed_qos2 += 1
                }
                _ => {}
            }
        }
        Ok::<(), String>(())
    })
    .await
    .map_err(|_| "timed out waiting for mixed MQTT 3.1.1 publish completion".to_string())?
}

async fn wait_for_mqtt5_publishes(
    event_loop: &mut rumqttc_v5::EventLoop,
    qos: u8,
    expected: usize,
) -> Result<(), String> {
    let mut completed = 0;
    timeout(Duration::from_secs(10), async {
        while completed < expected {
            match event_loop
                .poll()
                .await
                .map_err(|error| format!("failed to publish MQTT 5 batch: {error}"))?
            {
                Mqtt5Event::Outgoing(Mqtt5Outgoing::Publish(_)) if qos == 0 => completed += 1,
                Mqtt5Event::Incoming(Mqtt5Incoming::PubAck(_)) if qos == 1 => completed += 1,
                Mqtt5Event::Incoming(Mqtt5Incoming::PubComp(_)) if qos == 2 => completed += 1,
                _ => {}
            }
        }
        Ok::<(), String>(())
    })
    .await
    .map_err(|_| format!("timed out waiting for MQTT 5 batch QoS {qos} completion"))?
}

async fn wait_for_mqtt5_mixed_publishes(
    event_loop: &mut rumqttc_v5::EventLoop,
    messages: &[(u8, Vec<u8>)],
) -> Result<(), String> {
    // QoS 0, 1, and 2 use different completion packets, so keep separate
    // counters while draining the shared MQTT 5 event loop.
    let expected_qos0 = messages.iter().filter(|(qos, _)| *qos == 0).count();
    let expected_qos1 = messages.iter().filter(|(qos, _)| *qos == 1).count();
    let expected_qos2 = messages.iter().filter(|(qos, _)| *qos == 2).count();
    let mut completed_qos0 = 0;
    let mut completed_qos1 = 0;
    let mut completed_qos2 = 0;
    timeout(Duration::from_secs(10), async {
        while completed_qos0 < expected_qos0
            || completed_qos1 < expected_qos1
            || completed_qos2 < expected_qos2
        {
            match event_loop
                .poll()
                .await
                .map_err(|error| format!("failed to publish mixed MQTT 5 batch: {error}"))?
            {
                Mqtt5Event::Outgoing(Mqtt5Outgoing::Publish(_))
                    if completed_qos0 < expected_qos0 =>
                {
                    completed_qos0 += 1
                }
                Mqtt5Event::Incoming(Mqtt5Incoming::PubAck(_))
                    if completed_qos1 < expected_qos1 =>
                {
                    completed_qos1 += 1
                }
                Mqtt5Event::Incoming(Mqtt5Incoming::PubComp(_))
                    if completed_qos2 < expected_qos2 =>
                {
                    completed_qos2 += 1
                }
                _ => {}
            }
        }
        Ok::<(), String>(())
    })
    .await
    .map_err(|_| "timed out waiting for mixed MQTT 5 publish completion".to_string())?
}

async fn connect_mqtt311(
    broker_url: &str,
    username: &str,
    password: &str,
) -> Result<(Mqtt311Client, rumqttc_v4::EventLoop), String> {
    let url =
        url::Url::parse(broker_url).map_err(|error| format!("invalid MQTT broker URL: {error}"))?;
    let host = url
        .host_str()
        .ok_or_else(|| "MQTT broker URL has no host".to_string())?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| "MQTT broker URL has no port".to_string())?;
    let mut options = Mqtt311Options::new(
        format!("iggy-integration-publisher-{}", Uuid::new_v4().simple()),
        Mqtt311Broker::tcp(host, port),
    );
    options.set_credentials(username, password.to_owned());
    options.set_keep_alive(5);
    let (client, mut event_loop) = Mqtt311Client::builder(options)
        .capacity(10)
        .try_build()
        .map_err(|error| format!("failed to build MQTT client: {error}"))?;

    timeout(Duration::from_secs(10), async {
        loop {
            if let Mqtt311Event::Incoming(Mqtt311Incoming::ConnAck(_)) = event_loop
                .poll()
                .await
                .map_err(|error| format!("failed to connect to MQTT broker: {error}"))?
            {
                return Ok::<(), String>(());
            }
        }
    })
    .await
    .map_err(|_| "timed out waiting for MQTT CONNACK".to_string())??;

    Ok((client, event_loop))
}

async fn connect_mqtt5(
    broker_url: &str,
    username: &str,
    password: &str,
) -> Result<(Mqtt5Client, rumqttc_v5::EventLoop), String> {
    let url =
        url::Url::parse(broker_url).map_err(|error| format!("invalid MQTT broker URL: {error}"))?;
    let host = url
        .host_str()
        .ok_or_else(|| "MQTT broker URL has no host".to_string())?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| "MQTT broker URL has no port".to_string())?;
    let mut options = Mqtt5Options::new(
        format!("iggy-integration-publisher-{}", Uuid::new_v4().simple()),
        Mqtt5Broker::tcp(host, port),
    );
    options.set_credentials(username, password.to_owned());
    options.set_keep_alive(5);
    let (client, mut event_loop) = Mqtt5Client::builder(options)
        .capacity(10)
        .try_build()
        .map_err(|error| format!("failed to build MQTT client: {error}"))?;

    timeout(Duration::from_secs(10), async {
        loop {
            if let Mqtt5Event::Incoming(Mqtt5Incoming::ConnAck(_)) = event_loop
                .poll()
                .await
                .map_err(|error| format!("failed to connect to MQTT broker: {error}"))?
            {
                return Ok::<(), String>(());
            }
        }
    })
    .await
    .map_err(|_| "timed out waiting for MQTT CONNACK".to_string())??;

    Ok((client, event_loop))
}
