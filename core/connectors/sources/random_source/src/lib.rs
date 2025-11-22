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

#[derive(Debug)]
struct State {
    current_number: usize,
}

impl RandomSource {
    pub fn new(id: u32, config: RandomSourceConfig, state: Option<ConnectorState>) -> Self {
        let interval = config.interval.unwrap_or("1s".to_string());
        let interval = humantime::Duration::from_str(&interval)
            .unwrap_or(humantime::Duration::from_str("1s").expect("Failed to parse interval"));

        let current_number = if let Some(state) = state {
            u64::from_le_bytes(
                state.0[0..8]
                    .try_into()
                    .inspect_err(|error| {
                        error!("Failed to convert state to current number. {error}");
                    })
                    .unwrap_or_default(),
            )
        } else {
            0
        } as usize;

        RandomSource {
            id,
            max_count: config.max_count,
            interval: *interval,
            messages_range: config.messages_range.unwrap_or((10, 50)),
            payload_size: config.payload_size.unwrap_or(100),
            state: Mutex::new(State { current_number }),
        }
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
            && state.current_number >= max_count
        {
            info!(
                "Reached max number of {max_count} messages for random source connector with ID: {}",
                self.id
            );
            return Ok(ProducedMessages {
                schema: Schema::Json,
                messages: vec![],
                state: Some(ConnectorState(state.current_number.to_le_bytes().to_vec())),
            });
        }

        let messages = self.generate_messages();
        state.current_number += messages.len();
        info!(
            "Generated {} messages by random source connector with ID: {}",
            messages.len(),
            self.id
        );
        Ok(ProducedMessages {
            schema: Schema::Json,
            messages,
            state: Some(ConnectorState(state.current_number.to_le_bytes().to_vec())),
        })
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!("Random source connector with ID: {} is closed.", self.id);
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
