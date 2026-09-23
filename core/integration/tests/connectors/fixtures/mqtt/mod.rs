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
use std::collections::HashMap;

mod container;
mod publisher;

use container::{
    DEFAULT_IGGY_TOPIC, DEFAULT_TEST_STREAM, ENV_SOURCE_BROKER_URL, ENV_SOURCE_CLIENT_ID,
    ENV_SOURCE_PASSWORD, ENV_SOURCE_PATH, ENV_SOURCE_PROTOCOL, ENV_SOURCE_QOS, ENV_SOURCE_SCHEMA,
    ENV_SOURCE_STREAM, ENV_SOURCE_TOPIC, ENV_SOURCE_USERNAME, INVALID_MQTT_PASSWORD, MQTT_PASSWORD,
    MQTT_USERNAME, MqttBrokerContainer,
};

struct MqttFixture {
    broker: MqttBrokerContainer,
    protocol: publisher::Protocol,
    qos: u8,
    source_username: &'static str,
    source_password: &'static str,
}

impl MqttFixture {
    async fn start(
        protocol: publisher::Protocol,
        qos: u8,
        source_username: &'static str,
        source_password: &'static str,
    ) -> Result<Self, TestBinaryError> {
        Ok(Self {
            broker: MqttBrokerContainer::start().await?,
            protocol,
            qos,
            source_username,
            source_password,
        })
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

    fn runtime_envs(&self) -> HashMap<String, String> {
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
