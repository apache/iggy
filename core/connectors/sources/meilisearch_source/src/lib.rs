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
use meilisearch_sdk::{
    client::Client,
    errors::{
        Error as MeilisearchSdkError, ErrorCode as MeilisearchErrorCode,
        ErrorType as MeilisearchErrorType,
    },
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{str::FromStr, time::Duration};
use tokio::{sync::Mutex, time::sleep};
use tracing::info;

source_connector!(MeilisearchSource);

const CONNECTOR_NAME: &str = "Meilisearch source";
const DEFAULT_BATCH_SIZE: usize = 100;
const DEFAULT_POLLING_INTERVAL: &str = "5s";

#[derive(Debug, Serialize, Deserialize)]
pub struct MeilisearchSourceConfig {
    pub url: String,
    pub index: String,
    #[serde(serialize_with = "iggy_common::serde_secret::serialize_optional_secret")]
    pub api_key: Option<SecretString>,
    pub query: Option<String>,
    pub filter: Option<Value>,
    pub sort: Option<Vec<String>>,
    pub batch_size: Option<usize>,
    pub polling_interval: Option<String>,
    pub timeout: Option<String>,
    pub include_metadata: Option<bool>,
}

#[derive(Debug)]
pub struct MeilisearchSource {
    id: u32,
    config: MeilisearchSourceConfig,
    client: Option<Client>,
    batch_size: usize,
    polling_interval: Duration,
    include_metadata: bool,
    state: Mutex<State>,
}

#[derive(Debug, Serialize, Deserialize)]
struct State {
    next_offset: usize,
    documents_produced: usize,
    poll_count: usize,
}

impl MeilisearchSource {
    pub fn new(id: u32, config: MeilisearchSourceConfig, state: Option<ConnectorState>) -> Self {
        let batch_size = config.batch_size.unwrap_or(DEFAULT_BATCH_SIZE).max(1);
        let polling_interval =
            parse_duration(config.polling_interval.as_deref(), DEFAULT_POLLING_INTERVAL);
        let include_metadata = config.include_metadata.unwrap_or(false);
        let restored_state = state
            .and_then(|state| state.deserialize::<State>(CONNECTOR_NAME, id))
            .inspect(|state| {
                info!(
                    "Restored state for {CONNECTOR_NAME} connector with ID: {id}. \
                     Next offset: {}, documents produced: {}, poll count: {}",
                    state.next_offset, state.documents_produced, state.poll_count
                );
            });

        Self {
            id,
            config,
            client: None,
            batch_size,
            polling_interval,
            include_metadata,
            state: Mutex::new(restored_state.unwrap_or(State {
                next_offset: 0,
                documents_produced: 0,
                poll_count: 0,
            })),
        }
    }

    fn serialize_state(&self, state: &State) -> Option<ConnectorState> {
        ConnectorState::serialize(state, CONNECTOR_NAME, self.id)
    }

    fn create_client(&self) -> Result<Client, Error> {
        let host = normalize_host(&self.config.url)?;
        let api_key = self
            .config
            .api_key
            .as_ref()
            .map(|key| key.expose_secret().to_string());

        Client::new(host, api_key).map_err(|error| {
            Error::Connection(format!("Failed to create Meilisearch client: {error}"))
        })
    }

    async fn check_connectivity(&self, client: &Client) -> Result<(), Error> {
        let health = client.health().await.map_err(map_sdk_error)?;
        if health.status == "available" {
            return Ok(());
        }

        Err(Error::Connection(format!(
            "Meilisearch health check returned status '{}'",
            health.status
        )))
    }

    async fn search_documents(&self, client: &Client) -> Result<Vec<ProducedMessage>, Error> {
        let offset = {
            let state = self.state.lock().await;
            state.next_offset
        };
        let filter_expression = self.filter_expression()?;
        let sort_refs = self.sort_refs();
        let index = client.index(&self.config.index);
        let mut query = index.search();
        query
            .with_query(self.config.query.as_deref().unwrap_or_default())
            .with_offset(offset)
            .with_limit(self.batch_size);

        if let Some(filter) = &filter_expression {
            query.with_filter(filter);
        }

        if !sort_refs.is_empty() {
            query.with_sort(&sort_refs);
        }

        let documents = query
            .execute::<Value>()
            .await
            .map_err(map_sdk_error)?
            .hits
            .into_iter()
            .map(|hit| hit.result)
            .collect();
        let messages = self.documents_to_messages(offset, documents)?;

        let mut state = self.state.lock().await;
        state.next_offset += messages.len();
        state.documents_produced += messages.len();
        state.poll_count += 1;

        Ok(messages)
    }

    fn filter_expression(&self) -> Result<Option<String>, Error> {
        let Some(filter) = self.config.filter.as_ref().filter(|value| !value.is_null()) else {
            return Ok(None);
        };

        match filter {
            Value::String(filter) if !filter.is_empty() => Ok(Some(filter.clone())),
            Value::Array(filters) => filters
                .iter()
                .map(|value| {
                    value.as_str().map(str::to_string).ok_or_else(|| {
                        Error::InvalidConfigValue(
                            "Meilisearch filter arrays must contain only strings".to_string(),
                        )
                    })
                })
                .collect::<Result<Vec<_>, _>>()
                .map(|filters| {
                    if filters.is_empty() {
                        None
                    } else {
                        Some(filters.join(" AND "))
                    }
                }),
            _ => Err(Error::InvalidConfigValue(
                "Meilisearch filter must be a string or an array of strings".to_string(),
            )),
        }
    }

    fn sort_refs(&self) -> Vec<&str> {
        self.config
            .sort
            .as_deref()
            .unwrap_or_default()
            .iter()
            .map(String::as_str)
            .collect()
    }

    fn documents_to_messages(
        &self,
        offset: usize,
        documents: Vec<Value>,
    ) -> Result<Vec<ProducedMessage>, Error> {
        documents
            .into_iter()
            .enumerate()
            .map(|(index, document)| {
                let payload = if self.include_metadata {
                    json!({
                        "document": document,
                        "meilisearch": {
                            "index": self.config.index,
                            "offset": offset + index,
                        }
                    })
                } else {
                    document
                };

                serde_json::to_vec(&payload)
                    .map(|payload| ProducedMessage {
                        id: None,
                        checksum: None,
                        timestamp: None,
                        origin_timestamp: None,
                        headers: None,
                        payload,
                    })
                    .map_err(|error| {
                        Error::Serialization(format!(
                            "Failed to serialize Meilisearch document: {error}"
                        ))
                    })
            })
            .collect()
    }
}

#[async_trait]
impl Source for MeilisearchSource {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opening Meilisearch source connector with ID: {} for URL: {}, index: {}",
            self.id, self.config.url, self.config.index
        );

        let client = self.create_client()?;
        self.check_connectivity(&client).await?;
        self.client = Some(client);

        info!(
            "Successfully opened Meilisearch source connector with ID: {}",
            self.id
        );
        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        sleep(self.polling_interval).await;
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::Connection("Meilisearch client not initialized".to_string()))?;
        let messages = self.search_documents(client).await?;
        let persisted_state = {
            let state = self.state.lock().await;
            self.serialize_state(&state)
        };

        Ok(ProducedMessages {
            schema: Schema::Json,
            messages,
            state: persisted_state,
        })
    }

    async fn close(&mut self) -> Result<(), Error> {
        let state = self.state.lock().await;
        info!(
            "Meilisearch source connector with ID: {} is closing. Stats: {} documents produced, {} polls executed",
            self.id, state.documents_produced, state.poll_count
        );
        drop(state);

        self.client = None;
        info!(
            "Meilisearch source connector with ID: {} is closed.",
            self.id
        );
        Ok(())
    }
}

fn parse_duration(value: Option<&str>, default: &str) -> Duration {
    value
        .and_then(|value| humantime::Duration::from_str(value).ok())
        .unwrap_or_else(|| humantime::Duration::from_str(default).expect("valid default duration"))
        .into()
}

fn normalize_host(host: &str) -> Result<String, Error> {
    let trimmed = host.trim();
    if trimmed.is_empty() {
        return Err(Error::Connection(
            "Invalid Meilisearch URL: host cannot be empty".to_string(),
        ));
    }

    let with_scheme = if host.starts_with("http://") || host.starts_with("https://") {
        trimmed.to_string()
    } else {
        format!("http://{trimmed}")
    };
    let mut host = with_scheme;
    while host.ends_with('/') {
        host.pop();
    }
    Ok(host)
}

fn map_sdk_error(error: MeilisearchSdkError) -> Error {
    match error {
        MeilisearchSdkError::Meilisearch(meilisearch_error) => {
            if meilisearch_error.error_type == MeilisearchErrorType::Internal {
                Error::HttpRequestFailed(meilisearch_error.to_string())
            } else if meilisearch_error.error_code == MeilisearchErrorCode::IndexNotFound {
                Error::InvalidConfigValue(meilisearch_error.to_string())
            } else {
                Error::PermanentHttpError(meilisearch_error.to_string())
            }
        }
        MeilisearchSdkError::MeilisearchCommunication(communication_error) => {
            if communication_error.status_code == 429 || communication_error.status_code >= 500 {
                Error::HttpRequestFailed(communication_error.to_string())
            } else {
                Error::PermanentHttpError(communication_error.to_string())
            }
        }
        MeilisearchSdkError::ParseError(error) => {
            Error::Serialization(format!("Invalid Meilisearch response: {error}"))
        }
        MeilisearchSdkError::Timeout => {
            Error::HttpRequestFailed("Meilisearch request timed out".to_string())
        }
        MeilisearchSdkError::HttpError(error) => Error::HttpRequestFailed(error.to_string()),
        other => Error::HttpRequestFailed(other.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> MeilisearchSourceConfig {
        MeilisearchSourceConfig {
            url: "localhost:7700".to_string(),
            index: "iggy_messages".to_string(),
            api_key: None,
            query: None,
            filter: None,
            sort: None,
            batch_size: Some(10),
            polling_interval: Some("1ms".to_string()),
            timeout: Some("1s".to_string()),
            include_metadata: None,
        }
    }

    #[test]
    fn given_host_without_scheme_should_normalize_url() {
        let url = normalize_host("localhost:7700").unwrap();
        assert_eq!(url, "http://localhost:7700");
    }

    #[test]
    fn given_persisted_state_should_restore_next_offset() {
        let state = State {
            next_offset: 42,
            documents_produced: 42,
            poll_count: 7,
        };
        let connector_state = ConnectorState::serialize(&state, CONNECTOR_NAME, 1).unwrap();
        let source = MeilisearchSource::new(1, config(), Some(connector_state));

        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let restored = source.state.lock().await;
            assert_eq!(restored.next_offset, 42);
            assert_eq!(restored.documents_produced, 42);
            assert_eq!(restored.poll_count, 7);
        });
    }

    #[test]
    fn filter_expression_should_accept_string_filter() {
        let mut config = config();
        config.filter = Some(json!("status = active"));
        let source = MeilisearchSource::new(1, config, None);

        assert_eq!(
            source.filter_expression().unwrap(),
            Some("status = active".to_string())
        );
    }

    #[test]
    fn documents_should_be_wrapped_when_metadata_is_enabled() {
        let mut config = config();
        config.include_metadata = Some(true);
        let source = MeilisearchSource::new(1, config, None);
        let messages = source
            .documents_to_messages(3, vec![json!({"id": 1, "title": "hello"})])
            .unwrap();

        let payload: Value = serde_json::from_slice(&messages[0].payload).unwrap();
        assert_eq!(
            payload,
            json!({
                "document": {"id": 1, "title": "hello"},
                "meilisearch": {
                    "index": "iggy_messages",
                    "offset": 3,
                }
            })
        );
    }
}
