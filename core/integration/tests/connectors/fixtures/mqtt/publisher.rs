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

use rumqttc::v5::{AsyncClient, Event, Incoming, MqttOptions, mqttbytes::QoS};
use std::time::Duration;
use tokio::time::timeout;
use uuid::Uuid;

pub(super) async fn can_connect(broker_url: &str) -> bool {
    connect(broker_url).await.is_ok()
}

pub(super) async fn publish_qos_one(
    broker_url: &str,
    topic: &str,
    payload: &[u8],
) -> Result<(), String> {
    let (client, mut event_loop) = connect(broker_url).await?;
    client
        .publish(topic, QoS::AtLeastOnce, false, payload.to_vec())
        .await
        .map_err(|error| format!("failed to enqueue MQTT publish: {error}"))?;

    timeout(Duration::from_secs(10), async {
        loop {
            match event_loop
                .poll()
                .await
                .map_err(|error| format!("failed to publish MQTT message: {error}"))?
            {
                Event::Incoming(Incoming::PubAck(_)) => return Ok::<(), String>(()),
                _ => {}
            }
        }
    })
    .await
    .map_err(|_| "timed out waiting for MQTT PUBACK".to_string())?
}

async fn connect(broker_url: &str) -> Result<(AsyncClient, rumqttc::v5::EventLoop), String> {
    let url =
        url::Url::parse(broker_url).map_err(|error| format!("invalid MQTT broker URL: {error}"))?;
    let host = url
        .host_str()
        .ok_or_else(|| "MQTT broker URL has no host".to_string())?;
    let port = url
        .port_or_known_default()
        .ok_or_else(|| "MQTT broker URL has no port".to_string())?;
    let mut options = MqttOptions::new(
        format!("iggy-integration-publisher-{}", Uuid::new_v4().simple()),
        host,
        port,
    );
    options.set_keep_alive(Duration::from_secs(5));
    let (client, mut event_loop) = AsyncClient::new(options, 10);

    timeout(Duration::from_secs(10), async {
        loop {
            match event_loop
                .poll()
                .await
                .map_err(|error| format!("failed to connect to MQTT broker: {error}"))?
            {
                Event::Incoming(Incoming::ConnAck(_)) => return Ok::<(), String>(()),
                _ => {}
            }
        }
    })
    .await
    .map_err(|_| "timed out waiting for MQTT CONNACK".to_string())??;

    Ok((client, event_loop))
}
