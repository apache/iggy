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

use async_trait::async_trait;
use integration::harness::{TestBinaryError, TestFixture};
use rumqttc::v5::mqttbytes::v5::PublishProperties;
use std::collections::HashMap;

mod container;
mod publisher;

use container::{
    DEFAULT_IGGY_TOPIC, DEFAULT_TEST_STREAM, ENV_SOURCE_BATCH_SIZE, ENV_SOURCE_BATCH_TIMEOUT,
    ENV_SOURCE_BROKER_URL, ENV_SOURCE_CLIENT_ID, ENV_SOURCE_PASSWORD, ENV_SOURCE_PATH,
    ENV_SOURCE_PROTOCOL, ENV_SOURCE_QOS, ENV_SOURCE_SCHEMA, ENV_SOURCE_STREAM, ENV_SOURCE_TOPIC,
    ENV_SOURCE_USERNAME, INVALID_MQTT_PASSWORD, MQTT_PASSWORD, MQTT_USERNAME, MqttBrokerContainer,
};

struct MqttFixture {
    // The publisher and source deliberately share protocol and QoS settings so
    // a test never validates a different broker delivery mode by accident.
    broker: MqttBrokerContainer,
    protocol: publisher::Protocol,
    qos: u8,
    source_username: &'static str,
    source_password: &'static str,
    batch_size: &'static str,
    batch_timeout: &'static str,
}

impl MqttFixture {
    async fn start(
        protocol: publisher::Protocol,
        qos: u8,
        source_username: &'static str,
        source_password: &'static str,
    ) -> Result<Self, TestBinaryError> {
        Self::start_with_batch(
            protocol,
            qos,
            source_username,
            source_password,
            "10",
            "10ms",
        )
        .await
    }

    async fn start_with_batch(
        protocol: publisher::Protocol,
        qos: u8,
        source_username: &'static str,
        source_password: &'static str,
        batch_size: &'static str,
        batch_timeout: &'static str,
    ) -> Result<Self, TestBinaryError> {
        // Batch settings are injected as strings because the runtime receives
        // them through its environment configuration provider.
        Ok(Self {
            broker: MqttBrokerContainer::start().await?,
            protocol,
            qos,
            source_username,
            source_password,
            batch_size,
            batch_timeout,
        })
    }

    async fn restart_broker(&self) -> Result<(), String> {
        self.broker
            .restart()
            .await
            .map_err(|error| error.to_string())
    }

    async fn publish(&self, payload: &[u8]) -> Result<(), String> {
        publisher::publish(
            &self.broker.broker_url,
            container::DEFAULT_TEST_TOPIC,
            self.protocol,
            self.qos,
            payload,
            MQTT_USERNAME,
            MQTT_PASSWORD,
        )
        .await
    }

    async fn publish_batch(&self, payloads: &[Vec<u8>]) -> Result<(), String> {
        publisher::publish_batch(
            &self.broker.broker_url,
            container::DEFAULT_TEST_TOPIC,
            self.protocol,
            self.qos,
            payloads,
            MQTT_USERNAME,
            MQTT_PASSWORD,
        )
        .await
    }

    async fn publish_mixed_batch(&self, messages: &[(u8, Vec<u8>)]) -> Result<(), String> {
        publisher::publish_mixed_batch(
            &self.broker.broker_url,
            container::DEFAULT_TEST_TOPIC,
            self.protocol,
            messages,
            MQTT_USERNAME,
            MQTT_PASSWORD,
        )
        .await
    }

    async fn publish_with_properties(
        &self,
        payload: &[u8],
        properties: PublishProperties,
    ) -> Result<(), String> {
        match self.protocol {
            publisher::Protocol::Mqtt5 => {
                publisher::publish_mqtt5_with_properties(
                    &self.broker.broker_url,
                    container::DEFAULT_TEST_TOPIC,
                    self.qos,
                    payload,
                    properties,
                    MQTT_USERNAME,
                    MQTT_PASSWORD,
                )
                .await
            }
            publisher::Protocol::Mqtt311 => {
                Err("MQTT 5 properties require an MQTT 5 fixture".to_string())
            }
        }
    }

    fn runtime_envs(&self) -> HashMap<String, String> {
        // Keep fixture overrides aligned with production TOML field names. Each
        // test changes only the protocol, QoS, credentials, or batch behavior it
        // is intended to exercise.
        let protocol = match self.protocol {
            publisher::Protocol::Mqtt311 => "mqtt311",
            publisher::Protocol::Mqtt5 => "mqtt5",
        };
        HashMap::from([
            (
                ENV_SOURCE_BROKER_URL.to_string(),
                self.broker.broker_url.clone(),
            ),
            (
                ENV_SOURCE_USERNAME.to_string(),
                self.source_username.to_string(),
            ),
            (
                ENV_SOURCE_PASSWORD.to_string(),
                self.source_password.to_string(),
            ),
            (ENV_SOURCE_PROTOCOL.to_string(), protocol.to_string()),
            (ENV_SOURCE_QOS.to_string(), self.qos.to_string()),
            (
                ENV_SOURCE_BATCH_SIZE.to_string(),
                self.batch_size.to_string(),
            ),
            (
                ENV_SOURCE_BATCH_TIMEOUT.to_string(),
                self.batch_timeout.to_string(),
            ),
            (
                ENV_SOURCE_CLIENT_ID.to_string(),
                "iggy-mqtt-integration-source".to_string(),
            ),
            (
                ENV_SOURCE_STREAM.to_string(),
                DEFAULT_TEST_STREAM.to_string(),
            ),
            (ENV_SOURCE_TOPIC.to_string(), DEFAULT_IGGY_TOPIC.to_string()),
            (ENV_SOURCE_SCHEMA.to_string(), "raw".to_string()),
            (
                ENV_SOURCE_PATH.to_string(),
                "../../target/debug/libiggy_connector_mqtt_source".to_string(),
            ),
        ])
    }
}

macro_rules! define_mqtt_fixture {
    ($name:ident, $protocol:expr, $qos:expr, $username:expr, $password:expr) => {
        pub struct $name(MqttFixture);

        impl $name {
            pub async fn publish(&self, payload: &[u8]) -> Result<(), String> {
                self.0.publish(payload).await
            }
        }

        #[async_trait]
        impl TestFixture for $name {
            async fn setup() -> Result<Self, TestBinaryError> {
                Ok(Self(
                    MqttFixture::start($protocol, $qos, $username, $password).await?,
                ))
            }

            fn connectors_runtime_envs(&self) -> HashMap<String, String> {
                self.0.runtime_envs()
            }
        }
    };
}

pub struct Mqtt5PendingBatchFixture(MqttFixture);

impl Mqtt5PendingBatchFixture {
    pub async fn publish(&self, payload: &[u8]) -> Result<(), String> {
        self.0.publish(payload).await
    }

    pub async fn restart_broker(&self) -> Result<(), String> {
        self.0.restart_broker().await
    }
}

#[async_trait]
impl TestFixture for Mqtt5PendingBatchFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        // A large batch and long timeout leave one QoS 1 publish pending long
        // enough for restart tests to exercise broker redelivery.
        Ok(Self(
            MqttFixture::start_with_batch(
                publisher::Protocol::Mqtt5,
                1,
                MQTT_USERNAME,
                MQTT_PASSWORD,
                "100",
                "5s",
            )
            .await?,
        ))
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.0.runtime_envs()
    }
}

define_mqtt_fixture!(
    Mqtt311Qos0Fixture,
    publisher::Protocol::Mqtt311,
    0,
    MQTT_USERNAME,
    MQTT_PASSWORD
);
define_mqtt_fixture!(
    Mqtt311Qos1Fixture,
    publisher::Protocol::Mqtt311,
    1,
    MQTT_USERNAME,
    MQTT_PASSWORD
);
define_mqtt_fixture!(
    Mqtt311Qos2Fixture,
    publisher::Protocol::Mqtt311,
    2,
    MQTT_USERNAME,
    MQTT_PASSWORD
);
define_mqtt_fixture!(
    Mqtt5Qos0Fixture,
    publisher::Protocol::Mqtt5,
    0,
    MQTT_USERNAME,
    MQTT_PASSWORD
);
define_mqtt_fixture!(
    Mqtt5Qos1Fixture,
    publisher::Protocol::Mqtt5,
    1,
    MQTT_USERNAME,
    MQTT_PASSWORD
);

impl Mqtt5Qos1Fixture {
    pub async fn publish_with_properties(
        &self,
        payload: &[u8],
        properties: PublishProperties,
    ) -> Result<(), String> {
        self.0.publish_with_properties(payload, properties).await
    }
}

impl Mqtt311Qos1Fixture {
    pub async fn publish_batch(&self, payloads: &[Vec<u8>]) -> Result<(), String> {
        self.0.publish_batch(payloads).await
    }
}

impl Mqtt5Qos2Fixture {
    pub async fn publish_mixed_batch(&self, messages: &[(u8, Vec<u8>)]) -> Result<(), String> {
        self.0.publish_mixed_batch(messages).await
    }
}

define_mqtt_fixture!(
    Mqtt5Qos2Fixture,
    publisher::Protocol::Mqtt5,
    2,
    MQTT_USERNAME,
    MQTT_PASSWORD
);
define_mqtt_fixture!(
    Mqtt5InvalidCredentialsFixture,
    publisher::Protocol::Mqtt5,
    1,
    MQTT_USERNAME,
    INVALID_MQTT_PASSWORD
);
define_mqtt_fixture!(
    Mqtt311InvalidCredentialsFixture,
    publisher::Protocol::Mqtt311,
    1,
    MQTT_USERNAME,
    INVALID_MQTT_PASSWORD
);

impl Mqtt5Qos1Fixture {
    pub async fn publish_batch(&self, payloads: &[Vec<u8>]) -> Result<(), String> {
        self.0.publish_batch(payloads).await
    }
}
