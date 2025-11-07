/* Licensed to the Apache Software Foundation (ASF) under one
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
    ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata, sink_connector,
};
use serde::{Deserialize, Serialize};
use tracing::{error, info, warn};

sink_connector!(QuickwitSink);

#[derive(Debug)]
pub struct QuickwitSink {
    id: u32,
    config: QuickwitSinkConfig,
    client: reqwest::Client,
    index_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QuickwitSinkConfig {
    url: String,
    index: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct IndexConfig {
    index_id: String,
}

impl QuickwitSink {
    pub fn new(id: u32, config: QuickwitSinkConfig) -> Self {
        let index_config =
            serde_yaml_ng::from_str::<IndexConfig>(&config.index).expect("Invalid index config.");
        QuickwitSink {
            id,
            config,
            index_id: index_config.index_id,
            client: reqwest::Client::new(),
        }
    }

    async fn has_index(&self) -> Result<bool, Error> {
        let url = format!("{}/api/v1/indexes/{}", self.config.url, self.index_id);
        let response = self.client.get(&url).send().await.map_err(|error| {
            error!(
                "Failed to send HTTP request to check if index with ID: {} exists. {error}",
                self.index_id
            );
            Error::HttpRequestFailed(error.to_string())
        })?;
        let status = response.status();
        if status.is_success() {
            Ok(true)
        } else if status == reqwest::StatusCode::NOT_FOUND {
            Ok(false)
        } else {
            Err(Error::HttpRequestFailed(format!(
                "Unexpected status code: {status}",
            )))
        }
    }

    async fn create_index(&self) -> Result<(), Error> {
        info!("Creating index: {}", self.index_id);
        let url = format!("{}/api/v1/indexes", self.config.url);
        let response = self
            .client
            .post(&url)
            .header("content-type", "application/yaml")
            .body(self.config.index.to_owned())
            .send()
            .await
            .map_err(|error| {
                error!(
                    "Failed to send HTTP request to create index: {}. {error}",
                    self.index_id
                );
                Error::HttpRequestFailed(error.to_string())
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let reason = response.text().await.unwrap_or_default();
            error!(
                "Received an invalid HTTP response when creating index: {}. Status code: {status}, reason: {reason}",
                self.index_id
            );
            return Err(Error::InitError(format!(
                "Failed to create index: {}. {reason}",
                self.index_id
            )));
        }

        info!("Created index: {}", self.index_id);
        Ok(())
    }

    pub async fn ingest(&self, messages: Vec<simd_json::OwnedValue>) -> Result<(), Error> {
        let url = format!(
            "{}/api/v1/{}/ingest?commit=auto",
            self.config.url, self.index_id
        );
        info!("Ingesting messages for index: {}...", self.index_id);
        let messages_count = messages.len();
        let messages = messages
            .into_iter()
            .filter_map(|record| simd_json::to_string(&record).ok())
            .collect::<Vec<_>>()
            .join("\n");

        let response = self
            .client
            .post(&url)
            .body(messages)
            .send()
            .await
            .map_err(|error| {
                error!(
                    "Failed to send HTTP request to ingest messages for index: {}. {error}",
                    self.index_id
                );
                Error::HttpRequestFailed(error.to_string())
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let text = response.text().await.unwrap_or_default();
            error!(
                "Received an invalid HTTP response when ingesting messages for index: {}. Status code: {status}, reason: {text}",
                self.index_id
            );
            return Err(Error::HttpRequestFailed(format!(
                "Status code: {status}, reason: {text}"
            )));
        }

        info!(
            "Ingested {messages_count} messages for index: {}",
            self.index_id
        );
        Ok(())
    }
}

#[async_trait]
impl Sink for QuickwitSink {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opened Quickwit sink connector with ID: {} for URL: {}",
            self.id, self.config.url
        );
        if !self.has_index().await? {
            self.create_index().await?;
        }
        Ok(())
    }

    async fn consume(
        &self,
        _topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        info!(
            "Quickwit sink with ID: {} received: {} messages, format: {}",
            self.id,
            messages.len(),
            messages_metadata.schema
        );

        let mut json_payloads = Vec::with_capacity(messages.len());
        for message in messages {
            match message.payload {
                Payload::Json(value) => json_payloads.push(value),
                _ => {
                    warn!("Unsupported payload format: {}", messages_metadata.schema);
                }
            }
        }

        if json_payloads.is_empty() {
            return Ok(());
        }

        self.ingest(json_payloads).await?;
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!("Quickwit sink connector with ID: {} is closed.", self.id);
        Ok(())
    }
}
