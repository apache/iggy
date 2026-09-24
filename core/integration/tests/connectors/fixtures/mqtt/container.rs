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

use crate::connectors::fixtures;
use integration::harness::TestBinaryError;
use serde_json::json;
use std::time::Duration;
use testcontainers_modules::testcontainers::core::IntoContainerPort;
use testcontainers_modules::testcontainers::core::WaitFor;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};
use tokio::time::{sleep, timeout};

const EMQX_IMAGE: &str = "docker.io/emqx/emqx";
const EMQX_TAG: &str = "latest";
const EMQX_MQTT_PORT: u16 = 1883;
const EMQX_DASHBOARD_PORT: u16 = 18083;
const EMQX_START_ATTEMPTS: usize = 240;
const EMQX_START_INTERVAL: Duration = Duration::from_millis(250);
pub(super) const MQTT_USERNAME: &str = "iggy-mqtt-user";
pub(super) const MQTT_PASSWORD: &str = "iggy-mqtt-password";
pub(super) const INVALID_MQTT_PASSWORD: &str = "invalid-mqtt-password";

pub(super) const DEFAULT_TEST_TOPIC: &str = "devices/test/telemetry";
pub(super) const DEFAULT_TEST_STREAM: &str = "test_stream";
pub(super) const DEFAULT_IGGY_TOPIC: &str = "test_topic";

pub(super) const ENV_SOURCE_BROKER_URL: &str =
    "IGGY_CONNECTORS_SOURCE_MQTT_PLUGIN_CONFIG_BROKER_URL";
pub(super) const ENV_SOURCE_USERNAME: &str = "IGGY_CONNECTORS_SOURCE_MQTT_PLUGIN_CONFIG_USERNAME";
pub(super) const ENV_SOURCE_PASSWORD: &str = "IGGY_CONNECTORS_SOURCE_MQTT_PLUGIN_CONFIG_PASSWORD";
pub(super) const ENV_SOURCE_PROTOCOL: &str = "IGGY_CONNECTORS_SOURCE_MQTT_PLUGIN_CONFIG_PROTOCOL";
pub(super) const ENV_SOURCE_QOS: &str = "IGGY_CONNECTORS_SOURCE_MQTT_PLUGIN_CONFIG_QOS";
pub(super) const ENV_SOURCE_CLIENT_ID: &str = "IGGY_CONNECTORS_SOURCE_MQTT_PLUGIN_CONFIG_CLIENT_ID";
pub(super) const ENV_SOURCE_BATCH_SIZE: &str =
    "IGGY_CONNECTORS_SOURCE_MQTT_PLUGIN_CONFIG_BATCH_SIZE";
pub(super) const ENV_SOURCE_BATCH_TIMEOUT: &str =
    "IGGY_CONNECTORS_SOURCE_MQTT_PLUGIN_CONFIG_BATCH_TIMEOUT";
pub(super) const ENV_SOURCE_STREAM: &str = "IGGY_CONNECTORS_SOURCE_MQTT_STREAMS_0_STREAM";
pub(super) const ENV_SOURCE_TOPIC: &str = "IGGY_CONNECTORS_SOURCE_MQTT_STREAMS_0_TOPIC";
pub(super) const ENV_SOURCE_SCHEMA: &str = "IGGY_CONNECTORS_SOURCE_MQTT_STREAMS_0_SCHEMA";
pub(super) const ENV_SOURCE_PATH: &str = "IGGY_CONNECTORS_SOURCE_MQTT_PATH";

pub(super) struct MqttBrokerContainer {
    #[allow(dead_code)]
    container: ContainerAsync<GenericImage>,
    pub(super) broker_url: String,
}

impl MqttBrokerContainer {
    pub(super) async fn start() -> Result<Self, TestBinaryError> {
        let container = GenericImage::new(EMQX_IMAGE, EMQX_TAG)
            .with_wait_for(WaitFor::Nothing)
            .with_exposed_port(EMQX_MQTT_PORT.tcp())
            .with_exposed_port(EMQX_DASHBOARD_PORT.tcp())
            .with_mapped_port(0, EMQX_MQTT_PORT.tcp())
            .with_mapped_port(0, EMQX_DASHBOARD_PORT.tcp())
            .with_env_var("EMQX_DASHBOARD__DEFAULT_USERNAME", "admin")
            .with_env_var("EMQX_DASHBOARD__DEFAULT_PASSWORD", "public")
            .with_env_var("EMQX_AUTHENTICATION__1__ENABLE", "true")
            .with_env_var("EMQX_AUTHENTICATION__1__MECHANISM", "password_based")
            .with_env_var("EMQX_AUTHENTICATION__1__BACKEND", "built_in_database")
            .with_env_var("EMQX_AUTHENTICATION__1__USER_ID_TYPE", "username")
            .with_container_name(fixtures::unique_container_name("emqx"))
            .start()
            .await
            .map_err(|error| TestBinaryError::FixtureSetup {
                fixture_type: "MqttBrokerContainer".to_string(),
                message: format!("Failed to start EMQX container: {error}"),
            })?;

        let host_port = container
            .get_host_port_ipv4(EMQX_MQTT_PORT)
            .await
            .map_err(|error| TestBinaryError::FixtureSetup {
                fixture_type: "MqttBrokerContainer".to_string(),
                message: format!("Failed to get EMQX port: {error}"),
            })?;
        let dashboard_port = container
            .get_host_port_ipv4(EMQX_DASHBOARD_PORT)
            .await
            .map_err(|error| TestBinaryError::FixtureSetup {
                fixture_type: "MqttBrokerContainer".to_string(),
                message: format!("Failed to get EMQX dashboard port: {error}"),
            })?;
        let broker_url = format!("mqtt://127.0.0.1:{host_port}");
        let dashboard_url = format!("http://127.0.0.1:{dashboard_port}");

        let token = wait_for_dashboard_token(&dashboard_url).await?;
        create_mqtt_user(&dashboard_url, &token).await?;
        wait_for_mqtt(&broker_url, MQTT_USERNAME, MQTT_PASSWORD).await?;

        Ok(Self {
            container,
            broker_url,
        })
    }

    pub(super) async fn restart(&self) -> Result<(), TestBinaryError> {
        self.container
            .pause()
            .await
            .map_err(|error| TestBinaryError::FixtureSetup {
                fixture_type: "MqttBrokerContainer".to_string(),
                message: format!("Failed to pause EMQX container: {error}"),
            })?;
        sleep(Duration::from_secs(2)).await;
        self.container
            .unpause()
            .await
            .map_err(|error| TestBinaryError::FixtureSetup {
                fixture_type: "MqttBrokerContainer".to_string(),
                message: format!("Failed to resume EMQX container: {error}"),
            })?;

        wait_for_mqtt(&self.broker_url, MQTT_USERNAME, MQTT_PASSWORD).await
    }
}

async fn wait_for_dashboard_token(dashboard_url: &str) -> Result<String, TestBinaryError> {
    let client = reqwest::Client::new();
    let login_url = format!("{dashboard_url}/api/v5/login");
    let token = timeout(EMQX_START_INTERVAL * EMQX_START_ATTEMPTS as u32, async {
        for _ in 0..EMQX_START_ATTEMPTS {
            if let Ok(response) = client
                .post(&login_url)
                .json(&json!({"username": "admin", "password": "public"}))
                .send()
                .await
                && response.status().is_success()
                && let Ok(body) = response.json::<serde_json::Value>().await
                && let Some(token) = body.get("token").and_then(serde_json::Value::as_str)
            {
                return Ok(token.to_string());
            }
            sleep(EMQX_START_INTERVAL).await;
        }
        Err(TestBinaryError::FixtureSetup {
            fixture_type: "MqttBrokerContainer".to_string(),
            message: format!("EMQX dashboard did not accept login requests at {dashboard_url}"),
        })
    })
    .await
    .map_err(|_| TestBinaryError::FixtureSetup {
        fixture_type: "MqttBrokerContainer".to_string(),
        message: format!("Timed out waiting for EMQX dashboard at {dashboard_url}"),
    })??;

    Ok(token)
}

async fn create_mqtt_user(dashboard_url: &str, token: &str) -> Result<(), TestBinaryError> {
    let response = reqwest::Client::new()
        .post(format!(
            "{dashboard_url}/api/v5/authentication/password_based%3Abuilt_in_database/users"
        ))
        .bearer_auth(token)
        .json(&json!({
            "user_id": MQTT_USERNAME,
            "password": MQTT_PASSWORD,
        }))
        .send()
        .await
        .map_err(|error| TestBinaryError::FixtureSetup {
            fixture_type: "MqttBrokerContainer".to_string(),
            message: format!("Failed to create EMQX MQTT test user: {error}"),
        })?;

    if !response.status().is_success() {
        return Err(TestBinaryError::FixtureSetup {
            fixture_type: "MqttBrokerContainer".to_string(),
            message: format!(
                "Failed to create EMQX MQTT test user: HTTP {}",
                response.status()
            ),
        });
    }

    Ok(())
}

async fn wait_for_mqtt(
    broker_url: &str,
    username: &str,
    password: &str,
) -> Result<(), TestBinaryError> {
    let deadline = EMQX_START_INTERVAL * EMQX_START_ATTEMPTS as u32;

    timeout(deadline, async {
        for _ in 0..EMQX_START_ATTEMPTS {
            if super::publisher::can_connect(broker_url, username, password).await {
                return Ok(());
            }
            sleep(EMQX_START_INTERVAL).await;
        }
        Err(TestBinaryError::FixtureSetup {
            fixture_type: "MqttBrokerContainer".to_string(),
            message: format!("EMQX did not accept MQTT connections at {broker_url}"),
        })
    })
    .await
    .map_err(|_| TestBinaryError::FixtureSetup {
        fixture_type: "MqttBrokerContainer".to_string(),
        message: format!("Timed out waiting for EMQX at {broker_url}"),
    })?
}
