/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use async_trait::async_trait;
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source, source_connector,
};
use rand::{
    Rng,
    distr::{Alphanumeric, Uniform},
};
use serde::{Deserialize, Serialize};
use std::{str::FromStr, time::Duration};
use tokio::{sync::Mutex, time::sleep};
use tracing::{error, info};
use uuid::Uuid;

source_connector!(RandomSource);

const CONNECTOR_NAME: &str = "Random source";

#[derive(Debug)]
pub struct RandomSource {
    id: u32,
    max_count: Option<usize>,
    interval: Duration,
    messages_range: (u32, u32),
    payload_size: u32,
    state: Mutex<State>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RandomSourceConfig {
    interval: Option<String>,
    max_count: Option<usize>,
    messages_range: Option<(u32, u32)>,
    payload_size: Option<u32>,
}

#[derive(Debug, Serialize, Deserialize)]
struct State {
    messages_produced: usize,
}

impl RandomSource {
    pub fn new(id: u32, config: RandomSourceConfig, state: Option<ConnectorState>) -> Self {
        let interval = config.interval.unwrap_or("1s".to_string());
        let interval = humantime::Duration::from_str(&interval)
            .unwrap_or(humantime::Duration::from_str("1s").expect("Failed to parse interval"));

        let restored_state = state
            .and_then(|s| s.deserialize::<State>(CONNECTOR_NAME, id))
            .inspect(|s| {
                info!(
                    "Restored state for {CONNECTOR_NAME} connector with ID: {id}. \
                     Messages produced: {}",
                    s.messages_produced
                );
            });

        RandomSource {
            id,
            max_count: config.max_count,
            interval: *interval,
            messages_range: config.messages_range.unwrap_or((10, 50)),
            payload_size: config.payload_size.unwrap_or(100),
            state: Mutex::new(restored_state.unwrap_or(State {
                messages_produced: 0,
            })),
        }
    }

    fn serialize_state(&self, state: &State) -> Option<ConnectorState> {
        ConnectorState::serialize(state, CONNECTOR_NAME, self.id)
    }

    fn generate_messages(&self) -> Vec<ProducedMessage> {
        let mut messages = Vec::new();
        let mut rng = rand::rng();
        let messages_count =
            rng.sample(Uniform::new(self.messages_range.0, self.messages_range.1).unwrap());
        for _ in 0..messages_count {
            let record = Record {
                id: Uuid::new_v4(),
                title: "Hello".to_string(),
                name: "World".to_string(),
                text: self.generate_random_text(),
            };
            let Ok(payload) = simd_json::to_vec(&record) else {
                error!(
                    "Failed to serialize record by random source connector with ID: {}",
                    self.id
                );
                continue;
            };

            let message = ProducedMessage {
                id: None,
                headers: None,
                checksum: None,
                timestamp: None,
                origin_timestamp: None,
                payload,
            };
            messages.push(message);
        }
        messages
    }

    fn generate_random_text(&self) -> String {
        let mut rng = rand::rng();
        let text: String = (0..self.payload_size)
            .map(|_| rng.sample(Alphanumeric) as char)
            .collect();
        text
    }
}

#[async_trait]
impl Source for RandomSource {
    async fn open(&mut self) -> Result<(), iggy_connector_sdk::Error> {
        info!(
            "Opened random source connector with ID: {}. Interval: {:#?}, max offset: {:#?}, messages range: {} - {}, payload size: {}",
            self.id,
            self.interval,
            self.max_count,
            self.messages_range.0,
            self.messages_range.1,
            self.payload_size
        );
        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, iggy_connector_sdk::Error> {
        sleep(self.interval).await;
        let mut state = self.state.lock().await;
        if let Some(max_count) = self.max_count
            && state.messages_produced >= max_count
        {
            info!(
                "Reached max number of {max_count} messages for {CONNECTOR_NAME} connector with ID: {}",
                self.id
            );
            return Ok(ProducedMessages {
                schema: Schema::Json,
                messages: vec![],
                state: self.serialize_state(&state),
            });
        }

        let messages = self.generate_messages();
        state.messages_produced += messages.len();
        info!(
            "{CONNECTOR_NAME} connector with ID: {} generated {} messages. Total produced: {}",
            self.id,
            messages.len(),
            state.messages_produced
        );

        let persisted_state = self.serialize_state(&state);

        Ok(ProducedMessages {
            schema: Schema::Json,
            messages,
            state: persisted_state,
        })
    }

    async fn close(&mut self) -> Result<(), Error> {
        let state = self.state.lock().await;
        info!(
            "{CONNECTOR_NAME} connector with ID: {} closed. Total messages produced: {}",
            self.id, state.messages_produced
        );
        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Record {
    id: Uuid,
    title: String,
    name: String,
    text: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> RandomSourceConfig {
        RandomSourceConfig {
            interval: Some("100ms".to_string()),
            max_count: Some(100),
            messages_range: Some((5, 10)),
            payload_size: Some(50),
        }
    }

    #[test]
    fn given_persisted_state_should_restore_messages_produced() {
        let state = State {
            messages_produced: 500,
        };

        let serialized = rmp_serde::to_vec(&state).expect("Failed to serialize state");
        let connector_state = ConnectorState(serialized);

        let src = RandomSource::new(1, test_config(), Some(connector_state));

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let restored = src.state.lock().await;
            assert_eq!(restored.messages_produced, 500);
        });
    }

    #[test]
    fn given_no_state_should_start_fresh() {
        let src = RandomSource::new(1, test_config(), None);

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let state = src.state.lock().await;
            assert_eq!(state.messages_produced, 0);
        });
    }

    #[test]
    fn given_invalid_state_should_start_fresh() {
        let invalid_state = ConnectorState(b"not valid msgpack".to_vec());
        let src = RandomSource::new(1, test_config(), Some(invalid_state));

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let state = src.state.lock().await;
            assert_eq!(state.messages_produced, 0);
        });
    }

    #[test]
    fn state_should_be_serializable_and_deserializable() {
        let original = State {
            messages_produced: 1000,
        };

        let serialized = rmp_serde::to_vec(&original).expect("Failed to serialize");
        let deserialized: State =
            rmp_serde::from_slice(&serialized).expect("Failed to deserialize");

        assert_eq!(original.messages_produced, deserialized.messages_produced);
    }

    #[test]
    fn serialize_state_helper_should_produce_valid_connector_state() {
        let src = RandomSource::new(1, test_config(), None);
        let state = State {
            messages_produced: 42,
        };

        let connector_state = src.serialize_state(&state);
        assert!(connector_state.is_some());

        let bytes = connector_state.unwrap().0;
        let restored: State = rmp_serde::from_slice(&bytes).expect("Failed to deserialize state");
        assert_eq!(restored.messages_produced, 42);
    }
}
