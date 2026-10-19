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

use super::container::{
    DEFAULT_IGGY_TOPIC, DEFAULT_TEST_STREAM, INVALID_MQTT_PASSWORD, MQTT_PASSWORD, MQTT_USERNAME,
    MqttBrokerContainer,
};
use async_trait::async_trait;
use iggy_connector_sdk::Schema;
use integration::harness::{TestBinaryError, TestFixture};
use rumqttc::v5::{
    AsyncClient as Mqtt5Client, Event as Mqtt5Event, EventLoop as Mqtt5EventLoop,
    Incoming as Mqtt5Incoming, MqttOptions as Mqtt5Options,
    mqttbytes::{QoS as Mqtt5Qos, v5::PublishProperties},
};
use rumqttc::{
    AsyncClient as Mqtt311Client, Event as Mqtt311Event, EventLoop as Mqtt311EventLoop,
    Incoming as Mqtt311Incoming, MqttOptions as Mqtt311Options, mqttbytes::QoS as Mqtt311Qos,
};
use std::{collections::HashMap, time::Duration};
use tokio::{sync::Mutex, time::timeout};
use url::Url;

const DEFAULT_SINK_TOPIC: &str = "iggy.messages";
const SINK_CLIENT_ID: &str = "iggy-mqtt-integration-sink";
const SUBSCRIBER_CLIENT_ID: &str = "iggy-mqtt-integration-sink-subscriber";
const RECEIVE_TIMEOUT: Duration = Duration::from_secs(10);

const ENV_SINK_BROKER_URL: &str = "IGGY_CONNECTORS_SINK_MQTT_PLUGIN_CONFIG_BROKER_URL";
const ENV_SINK_CLIENT_ID: &str = "IGGY_CONNECTORS_SINK_MQTT_PLUGIN_CONFIG_CLIENT_ID";
const ENV_SINK_MAX_IN_FLIGHT: &str = "IGGY_CONNECTORS_SINK_MQTT_PLUGIN_CONFIG_MAX_IN_FLIGHT";
const ENV_SINK_PASSWORD: &str = "IGGY_CONNECTORS_SINK_MQTT_PLUGIN_CONFIG_PASSWORD";
const ENV_SINK_PROTOCOL: &str = "IGGY_CONNECTORS_SINK_MQTT_PLUGIN_CONFIG_PROTOCOL";
const ENV_SINK_QOS: &str = "IGGY_CONNECTORS_SINK_MQTT_PLUGIN_CONFIG_QOS";
const ENV_SINK_TOPIC: &str = "IGGY_CONNECTORS_SINK_MQTT_PLUGIN_CONFIG_TOPIC";
const ENV_SINK_TOPIC_MODE: &str = "IGGY_CONNECTORS_SINK_MQTT_PLUGIN_CONFIG_TOPIC_MODE";
const ENV_SINK_TOPIC_TEMPLATE: &str = "IGGY_CONNECTORS_SINK_MQTT_PLUGIN_CONFIG_TOPIC_TEMPLATE";
const ENV_SINK_FALLBACK_TOPIC: &str = "IGGY_CONNECTORS_SINK_MQTT_PLUGIN_CONFIG_FALLBACK_TOPIC";
const ENV_SINK_USERNAME: &str = "IGGY_CONNECTORS_SINK_MQTT_PLUGIN_CONFIG_USERNAME";
const ENV_SINK_PATH: &str = "IGGY_CONNECTORS_SINK_MQTT_PATH";
const ENV_SINK_STREAMS_0_CONSUMER_GROUP: &str =
    "IGGY_CONNECTORS_SINK_MQTT_STREAMS_0_CONSUMER_GROUP";
const ENV_SINK_STREAMS_0_SCHEMA: &str = "IGGY_CONNECTORS_SINK_MQTT_STREAMS_0_SCHEMA";
const ENV_SINK_STREAMS_0_STREAM: &str = "IGGY_CONNECTORS_SINK_MQTT_STREAMS_0_STREAM";
const ENV_SINK_STREAMS_0_TOPICS: &str = "IGGY_CONNECTORS_SINK_MQTT_STREAMS_0_TOPICS";

pub struct MqttSinkFixture {
    broker: MqttBrokerContainer,
    subscriber: Mutex<MqttSubscriber>,
    protocol: MqttProtocol,
    qos: u8,
    schema: Schema,
    max_in_flight: usize,
    password: String,
    topic: Option<String>,
    topic_mode: MqttTopicMode,
    topic_template: Option<String>,
    fallback_topic: Option<String>,
}

#[derive(Clone, Copy)]
enum MqttProtocol {
    Mqtt311,
    Mqtt5,
}

#[derive(Clone, Copy)]
enum MqttTopicMode {
    Static,
    Template,
    Dynamic,
}

struct MqttRoutingConfig {
    topic_mode: MqttTopicMode,
    topic: Option<String>,
    topic_template: Option<String>,
    fallback_topic: Option<String>,
    subscription_filters: Vec<String>,
}

impl MqttTopicMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Static => "static",
            Self::Template => "template",
            Self::Dynamic => "dynamic",
        }
    }
}

pub struct ReceivedMqttPublication {
    pub topic: String,
    pub payload: Vec<u8>,
    pub qos: u8,
    pub retain: bool,
    pub properties: Option<PublishProperties>,
}

impl MqttSinkFixture {
    async fn start(
        protocol: MqttProtocol,
        qos: u8,
        schema: Schema,
        max_in_flight: usize,
    ) -> Result<Self, TestBinaryError> {
        Self::start_with_routing(
            protocol,
            qos,
            schema,
            max_in_flight,
            MqttRoutingConfig {
                topic_mode: MqttTopicMode::Static,
                topic: Some(DEFAULT_SINK_TOPIC.to_string()),
                topic_template: None,
                fallback_topic: None,
                subscription_filters: vec![DEFAULT_SINK_TOPIC.to_string()],
            },
        )
        .await
    }

    async fn start_with_routing(
        protocol: MqttProtocol,
        qos: u8,
        schema: Schema,
        max_in_flight: usize,
        routing: MqttRoutingConfig,
    ) -> Result<Self, TestBinaryError> {
        let MqttRoutingConfig {
            topic_mode,
            topic,
            topic_template,
            fallback_topic,
            subscription_filters,
        } = routing;
        let broker = MqttBrokerContainer::start().await?;
        let subscriber =
            MqttSubscriber::connect(&broker.broker_url, protocol, &subscription_filters).await?;
        Ok(Self {
            broker,
            subscriber: Mutex::new(subscriber),
            protocol,
            qos,
            schema,
            max_in_flight,
            password: MQTT_PASSWORD.to_string(),
            topic,
            topic_mode,
            topic_template,
            fallback_topic,
        })
    }

    pub async fn receive_messages(&self, count: usize) -> Result<Vec<Vec<u8>>, TestBinaryError> {
        Ok(self
            .receive_publications(count)
            .await?
            .into_iter()
            .map(|publication| publication.payload)
            .collect())
    }

    pub async fn receive_publications(
        &self,
        count: usize,
    ) -> Result<Vec<ReceivedMqttPublication>, TestBinaryError> {
        self.subscriber
            .lock()
            .await
            .receive_publications(count)
            .await
    }

    pub async fn receive_until_payload(
        &self,
        expected_payload: &[u8],
    ) -> Result<ReceivedMqttPublication, TestBinaryError> {
        self.subscriber
            .lock()
            .await
            .receive_until_payload(expected_payload)
            .await
    }

    pub async fn disconnect_sink(&self) -> Result<(), TestBinaryError> {
        self.broker.disconnect_client(SINK_CLIENT_ID).await
    }

    pub async fn wait_for_sink_reconnect(&self) -> Result<(), TestBinaryError> {
        self.broker.wait_for_client(SINK_CLIENT_ID).await
    }

    pub async fn deny_sink_publications(&self) -> Result<(), TestBinaryError> {
        self.broker
            .deny_publish(MQTT_USERNAME, DEFAULT_SINK_TOPIC)
            .await
    }
}

#[async_trait]
impl TestFixture for MqttSinkFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Self::start(MqttProtocol::Mqtt5, 1, Schema::Json, 1).await
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        HashMap::from([
            (
                ENV_SINK_BROKER_URL.to_string(),
                self.broker.broker_url.clone(),
            ),
            (ENV_SINK_CLIENT_ID.to_string(), SINK_CLIENT_ID.to_string()),
            (ENV_SINK_PASSWORD.to_string(), self.password.clone()),
            (
                ENV_SINK_PROTOCOL.to_string(),
                match self.protocol {
                    MqttProtocol::Mqtt311 => "mqtt311",
                    MqttProtocol::Mqtt5 => "mqtt5",
                }
                .to_string(),
            ),
            (ENV_SINK_QOS.to_string(), self.qos.to_string()),
            (
                ENV_SINK_TOPIC_MODE.to_string(),
                self.topic_mode.as_str().to_string(),
            ),
            (
                ENV_SINK_MAX_IN_FLIGHT.to_string(),
                self.max_in_flight.to_string(),
            ),
            (ENV_SINK_USERNAME.to_string(), MQTT_USERNAME.to_string()),
            (
                ENV_SINK_STREAMS_0_STREAM.to_string(),
                DEFAULT_TEST_STREAM.to_string(),
            ),
            (
                ENV_SINK_STREAMS_0_TOPICS.to_string(),
                format!("[{DEFAULT_IGGY_TOPIC}]"),
            ),
            (
                ENV_SINK_STREAMS_0_SCHEMA.to_string(),
                self.schema.to_string(),
            ),
            (
                ENV_SINK_STREAMS_0_CONSUMER_GROUP.to_string(),
                "mqtt_sink_test_cg".to_string(),
            ),
            (
                ENV_SINK_PATH.to_string(),
                "../../target/debug/libiggy_connector_mqtt_sink".to_string(),
            ),
        ])
        .into_iter()
        .chain(
            self.topic
                .as_ref()
                .map(|topic| (ENV_SINK_TOPIC.to_string(), topic.clone())),
        )
        .chain(
            self.topic_template
                .as_ref()
                .map(|template| (ENV_SINK_TOPIC_TEMPLATE.to_string(), template.clone())),
        )
        .chain(
            self.fallback_topic
                .as_ref()
                .map(|topic| (ENV_SINK_FALLBACK_TOPIC.to_string(), topic.clone())),
        )
        .collect()
    }
}

pub struct MqttSinkQosTwoFixture {
    inner: MqttSinkFixture,
}

impl std::ops::Deref for MqttSinkQosTwoFixture {
    type Target = MqttSinkFixture;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::DerefMut for MqttSinkQosTwoFixture {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[async_trait]
impl TestFixture for MqttSinkQosTwoFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Ok(Self {
            inner: MqttSinkFixture::start(MqttProtocol::Mqtt5, 2, Schema::Json, 1).await?,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.inner.connectors_runtime_envs()
    }
}

pub struct MqttSinkQosZeroFixture {
    inner: MqttSinkFixture,
}

impl std::ops::Deref for MqttSinkQosZeroFixture {
    type Target = MqttSinkFixture;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::DerefMut for MqttSinkQosZeroFixture {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[async_trait]
impl TestFixture for MqttSinkQosZeroFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Ok(Self {
            inner: MqttSinkFixture::start(MqttProtocol::Mqtt5, 0, Schema::Json, 1).await?,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.inner.connectors_runtime_envs()
    }
}

pub struct MqttSinkRawSchemaFixture {
    inner: MqttSinkFixture,
}

impl std::ops::Deref for MqttSinkRawSchemaFixture {
    type Target = MqttSinkFixture;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::DerefMut for MqttSinkRawSchemaFixture {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

#[async_trait]
impl TestFixture for MqttSinkRawSchemaFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Ok(Self {
            inner: MqttSinkFixture::start(MqttProtocol::Mqtt5, 1, Schema::Raw, 1).await?,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.inner.connectors_runtime_envs()
    }
}

macro_rules! mqtt_sink_fixture {
    ($name:ident, $protocol:expr, $qos:expr, $max_in_flight:expr) => {
        pub struct $name {
            inner: MqttSinkFixture,
        }

        impl std::ops::Deref for $name {
            type Target = MqttSinkFixture;

            fn deref(&self) -> &Self::Target {
                &self.inner
            }
        }

        impl std::ops::DerefMut for $name {
            fn deref_mut(&mut self) -> &mut Self::Target {
                &mut self.inner
            }
        }

        #[async_trait]
        impl TestFixture for $name {
            async fn setup() -> Result<Self, TestBinaryError> {
                Ok(Self {
                    inner: MqttSinkFixture::start($protocol, $qos, Schema::Json, $max_in_flight)
                        .await?,
                })
            }

            fn connectors_runtime_envs(&self) -> HashMap<String, String> {
                self.inner.connectors_runtime_envs()
            }
        }
    };
}

mqtt_sink_fixture!(Mqtt311SinkQosZeroFixture, MqttProtocol::Mqtt311, 0, 1);
mqtt_sink_fixture!(Mqtt311SinkQosOneFixture, MqttProtocol::Mqtt311, 1, 1);
mqtt_sink_fixture!(Mqtt311SinkQosTwoFixture, MqttProtocol::Mqtt311, 2, 1);
mqtt_sink_fixture!(MqttSinkPipelinedFixture, MqttProtocol::Mqtt5, 1, 3);

pub struct MqttSinkInvalidCredentialsFixture {
    inner: MqttSinkFixture,
}

impl std::ops::Deref for MqttSinkInvalidCredentialsFixture {
    type Target = MqttSinkFixture;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait]
impl TestFixture for MqttSinkInvalidCredentialsFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        let mut inner = MqttSinkFixture::start(MqttProtocol::Mqtt5, 1, Schema::Json, 1).await?;
        inner.password = INVALID_MQTT_PASSWORD.to_string();
        Ok(Self { inner })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.inner.connectors_runtime_envs()
    }
}

pub struct MqttSinkTemplateTopicFixture {
    inner: MqttSinkFixture,
}

impl std::ops::Deref for MqttSinkTemplateTopicFixture {
    type Target = MqttSinkFixture;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait]
impl TestFixture for MqttSinkTemplateTopicFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Ok(Self {
            inner: MqttSinkFixture::start_with_routing(
                MqttProtocol::Mqtt5,
                1,
                Schema::Json,
                1,
                MqttRoutingConfig {
                    topic_mode: MqttTopicMode::Template,
                    topic: None,
                    topic_template: Some("integration/{stream}/{topic}".to_string()),
                    fallback_topic: None,
                    subscription_filters: vec!["integration/#".to_string()],
                },
            )
            .await?,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.inner.connectors_runtime_envs()
    }
}

pub struct MqttSinkDynamicTopicFixture {
    inner: MqttSinkFixture,
}

impl std::ops::Deref for MqttSinkDynamicTopicFixture {
    type Target = MqttSinkFixture;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait]
impl TestFixture for MqttSinkDynamicTopicFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Ok(Self {
            inner: MqttSinkFixture::start_with_routing(
                MqttProtocol::Mqtt5,
                1,
                Schema::Raw,
                1,
                MqttRoutingConfig {
                    topic_mode: MqttTopicMode::Dynamic,
                    topic: None,
                    topic_template: None,
                    fallback_topic: Some("fallback/{stream}/{topic}".to_string()),
                    subscription_filters: vec![
                        "devices/living-room".to_string(),
                        "fallback/test_stream/test_topic".to_string(),
                    ],
                },
            )
            .await?,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.inner.connectors_runtime_envs()
    }
}

pub struct MqttSinkEnvelopeFixture {
    inner: MqttSinkFixture,
}

impl std::ops::Deref for MqttSinkEnvelopeFixture {
    type Target = MqttSinkFixture;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

#[async_trait]
impl TestFixture for MqttSinkEnvelopeFixture {
    async fn setup() -> Result<Self, TestBinaryError> {
        Ok(Self {
            inner: MqttSinkFixture::start_with_routing(
                MqttProtocol::Mqtt5,
                1,
                Schema::Json,
                1,
                MqttRoutingConfig {
                    topic_mode: MqttTopicMode::Dynamic,
                    topic: None,
                    topic_template: None,
                    fallback_topic: Some("fallback/{stream}/{topic}".to_string()),
                    subscription_filters: vec![
                        "devices/roof/telemetry".to_string(),
                        "devices/valid/one".to_string(),
                        "devices/valid/two".to_string(),
                    ],
                },
            )
            .await?,
        })
    }

    fn connectors_runtime_envs(&self) -> HashMap<String, String> {
        self.inner.connectors_runtime_envs()
    }
}

enum MqttSubscriber {
    Mqtt311 {
        _client: Mqtt311Client,
        event_loop: Box<Mqtt311EventLoop>,
    },
    Mqtt5 {
        _client: Mqtt5Client,
        event_loop: Box<Mqtt5EventLoop>,
    },
}

impl MqttSubscriber {
    async fn connect(
        broker_url: &str,
        protocol: MqttProtocol,
        subscription_filters: &[String],
    ) -> Result<Self, TestBinaryError> {
        match protocol {
            MqttProtocol::Mqtt311 => Self::connect_mqtt311(broker_url, subscription_filters).await,
            MqttProtocol::Mqtt5 => Self::connect_mqtt5(broker_url, subscription_filters).await,
        }
    }

    async fn receive_publications(
        &mut self,
        count: usize,
    ) -> Result<Vec<ReceivedMqttPublication>, TestBinaryError> {
        timeout(RECEIVE_TIMEOUT, async {
            let mut messages = Vec::with_capacity(count);
            while messages.len() < count {
                match self {
                    Self::Mqtt311 { event_loop, .. } => match event_loop.poll().await {
                        Ok(Mqtt311Event::Incoming(Mqtt311Incoming::Publish(publish))) => {
                            messages.push(ReceivedMqttPublication {
                                topic: publish.topic,
                                payload: publish.payload.to_vec(),
                                qos: publish.qos as u8,
                                retain: publish.retain,
                                properties: None,
                            });
                        }
                        Ok(_) => {}
                        Err(error) => {
                            return Err(TestBinaryError::FixtureSetup {
                                fixture_type: "MqttSubscriber".to_string(),
                                message: format!("Failed to receive MQTT publication: {error}"),
                            });
                        }
                    },
                    Self::Mqtt5 { event_loop, .. } => match event_loop.poll().await {
                        Ok(Mqtt5Event::Incoming(Mqtt5Incoming::Publish(publish))) => {
                            messages.push(ReceivedMqttPublication {
                                topic: String::from_utf8_lossy(&publish.topic).into_owned(),
                                payload: publish.payload.to_vec(),
                                qos: publish.qos as u8,
                                retain: publish.retain,
                                properties: publish.properties,
                            });
                        }
                        Ok(_) => {}
                        Err(error) => {
                            return Err(TestBinaryError::FixtureSetup {
                                fixture_type: "MqttSubscriber".to_string(),
                                message: format!("Failed to receive MQTT publication: {error}"),
                            });
                        }
                    },
                }
            }
            Ok(messages)
        })
        .await
        .map_err(|_| TestBinaryError::FixtureSetup {
            fixture_type: "MqttSubscriber".to_string(),
            message: format!("Timed out waiting for {count} MQTT publications"),
        })?
    }

    async fn receive_until_payload(
        &mut self,
        expected_payload: &[u8],
    ) -> Result<ReceivedMqttPublication, TestBinaryError> {
        timeout(RECEIVE_TIMEOUT, async {
            loop {
                let mut publications = self.receive_publications(1).await?;
                let publication = publications
                    .pop()
                    .expect("one requested MQTT publication must be returned");
                if publication.payload == expected_payload {
                    return Ok(publication);
                }
            }
        })
        .await
        .map_err(|_| TestBinaryError::FixtureSetup {
            fixture_type: "MqttSubscriber".to_string(),
            message: "Timed out waiting for the expected MQTT publication".to_string(),
        })?
    }

    async fn connect_mqtt311(
        broker_url: &str,
        subscription_filters: &[String],
    ) -> Result<Self, TestBinaryError> {
        let url = Url::parse(broker_url).map_err(|error| TestBinaryError::FixtureSetup {
            fixture_type: "MqttSubscriber".to_string(),
            message: format!("Failed to parse MQTT broker URL: {error}"),
        })?;
        let host = url
            .host_str()
            .ok_or_else(|| TestBinaryError::FixtureSetup {
                fixture_type: "MqttSubscriber".to_string(),
                message: "MQTT broker URL has no host".to_string(),
            })?;
        let port = url
            .port_or_known_default()
            .ok_or_else(|| TestBinaryError::FixtureSetup {
                fixture_type: "MqttSubscriber".to_string(),
                message: "MQTT broker URL has no port".to_string(),
            })?;
        let mut options = Mqtt311Options::new(SUBSCRIBER_CLIENT_ID, host, port);
        options.set_credentials(MQTT_USERNAME, MQTT_PASSWORD);
        let (client, mut event_loop) = Mqtt311Client::new(options, 16);
        for subscription_filter in subscription_filters {
            client
                .subscribe(subscription_filter, Mqtt311Qos::AtLeastOnce)
                .await
                .map_err(|error| TestBinaryError::FixtureSetup {
                    fixture_type: "MqttSubscriber".to_string(),
                    message: format!("Failed to subscribe to MQTT topic: {error}"),
                })?;
        }

        timeout(RECEIVE_TIMEOUT, async {
            let mut acknowledgements = 0;
            while acknowledgements < subscription_filters.len() {
                match event_loop.poll().await {
                    Ok(Mqtt311Event::Incoming(Mqtt311Incoming::SubAck(_))) => acknowledgements += 1,
                    Ok(_) => {}
                    Err(error) => {
                        return Err(TestBinaryError::FixtureSetup {
                            fixture_type: "MqttSubscriber".to_string(),
                            message: format!("Failed to establish MQTT subscription: {error}"),
                        });
                    }
                }
            }
            Ok(())
        })
        .await
        .map_err(|_| TestBinaryError::FixtureSetup {
            fixture_type: "MqttSubscriber".to_string(),
            message: "Timed out waiting for MQTT subscription acknowledgement".to_string(),
        })??;

        Ok(Self::Mqtt311 {
            _client: client,
            event_loop: Box::new(event_loop),
        })
    }

    async fn connect_mqtt5(
        broker_url: &str,
        subscription_filters: &[String],
    ) -> Result<Self, TestBinaryError> {
        let url = Url::parse(broker_url).map_err(|error| TestBinaryError::FixtureSetup {
            fixture_type: "MqttSubscriber".to_string(),
            message: format!("Failed to parse MQTT broker URL: {error}"),
        })?;
        let host = url
            .host_str()
            .ok_or_else(|| TestBinaryError::FixtureSetup {
                fixture_type: "MqttSubscriber".to_string(),
                message: "MQTT broker URL has no host".to_string(),
            })?;
        let port = url
            .port_or_known_default()
            .ok_or_else(|| TestBinaryError::FixtureSetup {
                fixture_type: "MqttSubscriber".to_string(),
                message: "MQTT broker URL has no port".to_string(),
            })?;
        let mut options = Mqtt5Options::new(SUBSCRIBER_CLIENT_ID, host, port);
        options.set_credentials(MQTT_USERNAME, MQTT_PASSWORD);
        let (client, mut event_loop) = Mqtt5Client::new(options, 16);
        for subscription_filter in subscription_filters {
            client
                .subscribe(subscription_filter, Mqtt5Qos::AtLeastOnce)
                .await
                .map_err(|error| TestBinaryError::FixtureSetup {
                    fixture_type: "MqttSubscriber".to_string(),
                    message: format!("Failed to subscribe to MQTT topic: {error}"),
                })?;
        }

        timeout(RECEIVE_TIMEOUT, async {
            let mut acknowledgements = 0;
            while acknowledgements < subscription_filters.len() {
                match event_loop.poll().await {
                    Ok(Mqtt5Event::Incoming(Mqtt5Incoming::SubAck(_))) => acknowledgements += 1,
                    Ok(_) => {}
                    Err(error) => {
                        return Err(TestBinaryError::FixtureSetup {
                            fixture_type: "MqttSubscriber".to_string(),
                            message: format!("Failed to establish MQTT subscription: {error}"),
                        });
                    }
                }
            }
            Ok(())
        })
        .await
        .map_err(|_| TestBinaryError::FixtureSetup {
            fixture_type: "MqttSubscriber".to_string(),
            message: "Timed out waiting for MQTT subscription acknowledgement".to_string(),
        })??;

        Ok(Self::Mqtt5 {
            _client: client,
            event_loop: Box::new(event_loop),
        })
    }
}
