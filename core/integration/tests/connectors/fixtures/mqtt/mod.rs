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
    ENV_SOURCE_PATH, ENV_SOURCE_PROTOCOL, ENV_SOURCE_QOS, ENV_SOURCE_SCHEMA, ENV_SOURCE_STREAM,
    ENV_SOURCE_TOPIC, MqttBrokerContainer,
};

pub struct MqttFixture {
    broker: MqttBrokerContainer,
}

impl MqttFixture {
    pub async fn publish_qos_one(&self, payload: &[u8]) -> Result<(), String> {
        publisher::publish_qos_one(
            &self.broker.broker_url,
            container::DEFAULT_TEST_TOPIC,
            payload,
        )
        .await
    }
}

#[async_trait]
impl TestFixture for MqttFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Ok(Self {
            broker: MqttBrokerContainer::start().await?,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        HashMap::from([
            (
                ENV_SOURCE_BROKER_URL.to_string(),
                self.broker.broker_url.clone(),
            ),
            (ENV_SOURCE_PROTOCOL.to_string(), "mqtt5".to_string()),
            (ENV_SOURCE_QOS.to_string(), "1".to_string()),
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
