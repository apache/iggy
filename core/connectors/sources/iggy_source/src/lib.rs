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

use std::{str::FromStr, time::Duration};

use async_trait::async_trait;
use iggy::clients::client::IggyClient;
use iggy_common::{Client, Identifier, MessageClient, PollingStrategy, StreamClient, TopicClient};
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source, source_connector,
};
use serde::{Deserialize, Serialize};
use tokio::{sync::Mutex, time::sleep};
use tracing::{debug, error, info, warn};

source_connector!(IggySource);

const CONNECTOR_NAME: &str = "Iggy source";

#[derive(Debug, Serialize, Deserialize)]
pub struct IggySourceConfig {
    pub server_address: String,
    pub stream_id: String,
    pub topic_id: String,
    pub partition_id: Option<u32>,
    pub batch_size: Option<u32>,
    pub poll_interval: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct State {
    current_offset: u64,
}

#[derive(Debug)]
pub struct IggySource {
    id: u32,
    config: IggySourceConfig,
    poll_interval: Duration,
    batch_size: u32,
    partition_id: u32,
    client: Mutex<Option<IggyClient>>,
    state: Mutex<State>,
}

impl IggySource {
    pub fn new(id: u32, config: IggySourceConfig, state: Option<ConnectorState>) -> Self {
        let interval_str = config.poll_interval.as_deref().unwrap_or("100ms");
        let poll_interval = *humantime::Duration::from_str(interval_str)
            .unwrap_or(humantime::Duration::from_str("100ms").expect("Failed to parse interval"));

        let batch_size = config.batch_size.unwrap_or(100);
        let partition_id = config.partition_id.unwrap_or(1);

        let restored_state = state
            .and_then(|s| s.deserialize::<State>(CONNECTOR_NAME, id))
            .inspect(|s| {
                info!(
                    "Restored state for {CONNECTOR_NAME} connector with ID: {id}. \
                     Current offset: {}",
                    s.current_offset
                );
            });

        IggySource {
            id,
            config,
            poll_interval,
            batch_size,
            partition_id,
            client: Mutex::new(None),
            state: Mutex::new(restored_state.unwrap_or(State { current_offset: 0 })),
        }
    }

    fn serialize_state(&self, state: &State) -> Option<ConnectorState> {
        ConnectorState::serialize(state, CONNECTOR_NAME, self.id)
    }
}

#[async_trait]
impl Source for IggySource {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opening {CONNECTOR_NAME} connector ID: {}, connecting to: {}, stream: {}, topic: {}, partition: {}",
            self.id,
            self.config.server_address,
            self.config.stream_id,
            self.config.topic_id,
            self.partition_id
        );

        let client = IggyClient::from_connection_string(&self.config.server_address)
            .map_err(|e| Error::InitError(format!("Failed to parse server address: {e}")))?;

        client
            .connect()
            .await
            .map_err(|e| Error::Connection(format!("Failed to connect to Iggy server: {e}")))?;

        let stream_id: Identifier = self.config.stream_id.as_str().try_into().map_err(|_| {
            Error::InitError(format!("Invalid stream ID: {}", self.config.stream_id))
        })?;
        let topic_id: Identifier =
            self.config.topic_id.as_str().try_into().map_err(|_| {
                Error::InitError(format!("Invalid topic ID: {}", self.config.topic_id))
            })?;

        let stream = client
            .get_stream(&stream_id)
            .await
            .map_err(|e| Error::Connection(format!("Stream not found: {e}")))?;
        if stream.is_none() {
            return Err(Error::InitError(format!(
                "Stream '{}' does not exist on remote Iggy server",
                self.config.stream_id
            )));
        }

        let topic = client
            .get_topic(&stream_id, &topic_id)
            .await
            .map_err(|e| Error::Connection(format!("Topic not found: {e}")))?;
        if topic.is_none() {
            return Err(Error::InitError(format!(
                "Topic '{}' does not exist on remote Iggy server",
                self.config.topic_id
            )));
        }

        *self.client.lock().await = Some(client);
        info!("Opened {CONNECTOR_NAME} connector ID: {}", self.id);
        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        sleep(self.poll_interval).await;

        let current_offset = {
            let state_guard = self.state.lock().await;
            state_guard.current_offset
        };

        let client_guard = self.client.lock().await;
        let Some(client) = client_guard.as_ref() else {
            error!(
                "{CONNECTOR_NAME} connector ID: {} client not initialized",
                self.id
            );
            return Err(Error::Connection("Client not initialized".to_string()));
        };

        let stream_id: Identifier =
            self.config.stream_id.as_str().try_into().map_err(|_| {
                Error::InitError(format!("Invalid stream: {}", self.config.stream_id))
            })?;
        let topic_id: Identifier =
            self.config.topic_id.as_str().try_into().map_err(|_| {
                Error::InitError(format!("Invalid topic: {}", self.config.topic_id))
            })?;

        let strategy = PollingStrategy::offset(current_offset);
        let consumer = iggy_common::Consumer::default();

        let polled = match client
            .poll_messages(
                &stream_id,
                &topic_id,
                Some(self.partition_id),
                &consumer,
                &strategy,
                self.batch_size,
                false,
            )
            .await
        {
            Ok(polled) => polled,
            Err(e) => {
                warn!(
                    "Transient fetch failure for {CONNECTOR_NAME} ID: {}, error: {e}",
                    self.id
                );
                let state_guard = self.state.lock().await;
                return Ok(ProducedMessages {
                    schema: Schema::Raw,
                    messages: vec![],
                    state: self.serialize_state(&state_guard),
                });
            }
        };

        let mut produced_messages = Vec::with_capacity(polled.messages.len());
        let mut new_offset = current_offset;

        for msg in polled.messages {
            new_offset = msg.header.offset + 1;
            let headers = msg.user_headers_map().ok().flatten();

            produced_messages.push(ProducedMessage {
                id: Some(msg.header.id),
                headers,
                checksum: None,
                timestamp: None,
                origin_timestamp: Some(msg.header.timestamp),
                payload: msg.payload.to_vec(),
            });
        }

        let persisted_state = {
            let mut state_guard = self.state.lock().await;
            state_guard.current_offset = new_offset;
            self.serialize_state(&state_guard)
        };

        debug!(
            "Polled {} messages for {CONNECTOR_NAME} ID: {}, new current_offset: {}",
            produced_messages.len(),
            self.id,
            new_offset
        );

        Ok(ProducedMessages {
            schema: Schema::Raw,
            messages: produced_messages,
            state: persisted_state,
        })
    }

    async fn close(&mut self) -> Result<(), Error> {
        let mut client_guard = self.client.lock().await;
        if let Some(client) = client_guard.take() {
            let _ = client.shutdown().await;
        }
        let state_guard = self.state.lock().await;
        info!(
            "Closed {CONNECTOR_NAME} connector ID: {}, final current_offset: {}",
            self.id, state_guard.current_offset
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> IggySourceConfig {
        IggySourceConfig {
            server_address: "iggy://127.0.0.1:8090".to_string(),
            stream_id: "test_stream".to_string(),
            topic_id: "test_topic".to_string(),
            partition_id: Some(1),
            batch_size: Some(50),
            poll_interval: Some("10ms".to_string()),
        }
    }

    #[test]
    fn given_default_config_should_instantiate_source() {
        let config = IggySourceConfig {
            server_address: "iggy://127.0.0.1:8090".to_string(),
            stream_id: "stream".to_string(),
            topic_id: "topic".to_string(),
            partition_id: None,
            batch_size: None,
            poll_interval: None,
        };
        let src = IggySource::new(1, config, None);
        assert_eq!(src.id, 1);
        assert_eq!(src.batch_size, 100);
        assert_eq!(src.partition_id, 1);
        assert_eq!(src.poll_interval, Duration::from_millis(100));
    }

    #[test]
    fn given_custom_config_should_override_defaults() {
        let src = IggySource::new(2, test_config(), None);
        assert_eq!(src.id, 2);
        assert_eq!(src.batch_size, 50);
        assert_eq!(src.partition_id, 1);
        assert_eq!(src.poll_interval, Duration::from_millis(10));
    }

    #[test]
    fn given_persisted_state_should_restore_current_offset() {
        let state = State {
            current_offset: 1234,
        };
        let serialized = rmp_serde::to_vec(&state).expect("Failed to serialize state");
        let connector_state = ConnectorState(serialized);

        let src = IggySource::new(1, test_config(), Some(connector_state));

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let restored = src.state.lock().await;
            assert_eq!(restored.current_offset, 1234);
        });
    }

    #[test]
    fn given_no_state_should_start_fresh_from_offset_zero() {
        let src = IggySource::new(1, test_config(), None);

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let state = src.state.lock().await;
            assert_eq!(state.current_offset, 0);
        });
    }

    #[test]
    fn given_invalid_state_should_start_fresh() {
        let invalid_state = ConnectorState(b"invalid state bytes".to_vec());
        let src = IggySource::new(1, test_config(), Some(invalid_state));

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let state = src.state.lock().await;
            assert_eq!(state.current_offset, 0);
        });
    }

    #[test]
    fn state_should_be_serializable_and_deserializable() {
        let original = State {
            current_offset: 999,
        };
        let serialized = rmp_serde::to_vec(&original).expect("Failed to serialize");
        let deserialized: State =
            rmp_serde::from_slice(&serialized).expect("Failed to deserialize");
        assert_eq!(original.current_offset, deserialized.current_offset);
    }

    #[test]
    fn serialize_state_helper_should_produce_valid_connector_state() {
        let src = IggySource::new(1, test_config(), None);
        let state = State {
            current_offset: 555,
        };

        let connector_state = src.serialize_state(&state);
        assert!(connector_state.is_some());

        let bytes = connector_state.unwrap().0;
        let restored: State = rmp_serde::from_slice(&bytes).expect("Failed to deserialize state");
        assert_eq!(restored.current_offset, 555);
    }

    #[test]
    fn given_polled_iggy_message_should_convert_to_produced_message() {
        use iggy_common::IggyMessage;

        let original_id = 42u128;
        let payload = vec![1, 2, 3, 4];

        let msg = IggyMessage::builder()
            .id(original_id)
            .payload(bytes::Bytes::from(payload.clone()))
            .build()
            .unwrap();

        let produced = ProducedMessage {
            id: Some(msg.header.id),
            headers: msg.user_headers_map().ok().flatten(),
            checksum: None,
            timestamp: None,
            origin_timestamp: Some(msg.header.timestamp),
            payload: msg.payload.to_vec(),
        };

        assert_eq!(produced.id, Some(original_id));
        assert_eq!(produced.payload, vec![1, 2, 3, 4]);
    }
}
