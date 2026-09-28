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
use futures::stream::TryStreamExt;
use iggy_common::{DateTime, Utc};
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source,
    source::SourceBatchResult, source_connector,
};
use mongodb::{
    Client, Collection,
    bson::{Bson, Document, doc},
    options::ClientOptions,
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::info;

source_connector!(MongodbSource);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct State {
    last_poll_timestamp: Option<DateTime<Utc>>,
    total_documents_fetched: usize,
    poll_count: usize,
    // `_id` of the document at `last_poll_timestamp`, so documents sharing that
    // millisecond but cut off by `limit` are picked up on the next poll.
    #[serde(default)]
    last_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MongodbSourceConfig {
    #[serde(serialize_with = "iggy_common::serde_secret::serialize_secret")]
    pub connection_uri: SecretString,
    pub database: String,
    pub collection: String,
    pub max_pool_size: Option<u32>,
    pub query: Option<Document>,
    pub timestamp_field: Option<String>,
    pub limit: Option<i64>,
    pub polling_interval: Option<String>,
}

#[derive(Debug)]
pub struct MongodbSource {
    id: u32,
    config: MongodbSourceConfig,
    client: Option<Client>,
    polling_interval: Duration,
    state: Mutex<State>,
    pending_state: Mutex<Option<State>>,
}

const CONNECTOR_NAME: &str = "MongoDB source";

impl MongodbSource {
    pub fn new(id: u32, config: MongodbSourceConfig, state: Option<ConnectorState>) -> Self {
        let polling_interval = config
            .polling_interval
            .as_deref()
            .unwrap_or("10s")
            .parse::<humantime::Duration>()
            .unwrap_or_else(|_| humantime::Duration::from_str("10s").unwrap())
            .into();

        let restored_state = state
            .and_then(|s| s.deserialize::<State>(CONNECTOR_NAME, id))
            .inspect(|s| {
                info!(
                    "Restored state for {CONNECTOR_NAME} connector with ID: {id}. \
                     Documents fetched: {}, poll count: {}",
                    s.total_documents_fetched, s.poll_count
                );
            });

        MongodbSource {
            id,
            config,
            client: None,
            polling_interval,
            state: Mutex::new(restored_state.unwrap_or(State {
                last_poll_timestamp: None,
                total_documents_fetched: 0,
                poll_count: 0,
                last_id: None,
            })),
            pending_state: Mutex::new(None),
        }
    }

    fn serialize_state(&self, state: &State) -> Option<ConnectorState> {
        ConnectorState::serialize(state, CONNECTOR_NAME, self.id)
    }

    async fn create_client(&self) -> Result<Client, Error> {
        let mut client_options: ClientOptions =
            ClientOptions::parse(self.config.connection_uri.expose_secret())
                .await
                .map_err(|e| Error::InitError(format!("Failed to parse connection URI: {e}")))?;
        if let Some(pool_size) = self.config.max_pool_size {
            client_options.max_pool_size = Some(pool_size);
        }
        let client = Client::with_options(client_options)
            .map_err(|e| Error::InitError(format!("Failed to create client: {e}")))?;
        Ok(client)
    }

    /// The cursor only advances on BSON `Date` values, so a string, number or
    /// missing `timestamp_field` would leave the connector silently producing nothing.
    async fn check_timestamp_field(&self, client: &Client) -> Result<(), Error> {
        let Some(timestamp_field) = &self.config.timestamp_field else {
            return Ok(());
        };
        let coll: Collection<Document> = client
            .database(&self.config.database)
            .collection(&self.config.collection);
        let sample = coll
            .find_one(self.config.query.clone().unwrap_or_default())
            .await
            .map_err(|e| Error::InitError(format!("Failed to read a sample document: {e}")))?;
        match sample {
            Some(doc) => validate_timestamp_field(&doc, timestamp_field),
            None => Ok(()),
        }
    }

    async fn search_documents(
        &self,
        client: &Client,
    ) -> Result<(Vec<ProducedMessage>, State), Error> {
        let state = self.state.lock().await.clone();
        let limit = &self.config.limit.unwrap_or(100);

        let coll: Collection<Document> = client
            .database(&self.config.database.to_string())
            .collection(&self.config.collection.to_string());

        let mut cursor = if let Some(timestamp_field) = &self.config.timestamp_field {
            let cursor_filter = cursor_filter(timestamp_field, &state);
            let filter = match &self.config.query {
                Some(query) if !query.is_empty() => doc! { "$and": [query.clone(), cursor_filter] },
                _ => cursor_filter,
            };

            coll.find(filter)
                .limit(*limit)
                .sort(doc! { timestamp_field: 1, "_id": 1 })
                .await
                .map_err(|e| Error::InitError(format!("Failed to execute search: {e}")))?
        } else {
            let filter = &self.config.query.clone().unwrap_or_else(|| doc! {});
            coll.find(filter.clone())
                .await
                .map_err(|e| Error::InitError(format!("Failed to execute search: {e}")))?
        };

        let mut messages = Vec::new();
        let mut latest_position = None;

        while let Some(doc) = cursor
            .try_next()
            .await
            .map_err(|e| Error::InitError(format!("Failed to move cursor {e}")))?
        {
            if let Some(timestamp_field) = &self.config.timestamp_field
                && let Some(timestamp_dt) = doc.get(timestamp_field).and_then(|v| v.as_datetime())
                && let Some(timestamp) =
                    iggy_common::DateTime::<iggy_common::Utc>::from_timestamp_millis(
                        timestamp_dt.timestamp_millis(),
                    )
            {
                // Results are sorted by (timestamp, _id), so the last one seen is the newest.
                latest_position = Some((
                    timestamp,
                    doc.get("_id").and_then(|id| serde_json::to_string(id).ok()),
                ));
            }

            let payload = serde_json::to_vec(&doc).map_err(|e| {
                Error::Serialization(format!("Failed to serialize document: {}", e))
            })?;

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
        let (last_poll_timestamp, last_id) = match latest_position {
            Some((timestamp, id)) => (Some(timestamp), id),
            None => (state.last_poll_timestamp, state.last_id),
        };
        let candidate_state = State {
            last_poll_timestamp,
            total_documents_fetched: state.total_documents_fetched + messages.len(),
            poll_count: state.poll_count + 1,
            last_id,
        };
        Ok((messages, candidate_state))
    }
}

#[async_trait]
impl Source for MongodbSource {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opening Mongodb source connector with ID: {}, collection: {}",
            self.id, self.config.collection
        );

        let client = self.create_client().await?;
        self.check_timestamp_field(&client).await?;
        self.client = Some(client);

        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        let poll_interval = self.polling_interval;
        tokio::time::sleep(poll_interval).await;

        let client = self
            .client
            .as_ref()
            .ok_or_else(|| Error::Storage("Mongodb client not initialized".to_string()))?;

        let (messages, candidate_state) = self.search_documents(client).await?;

        let persisted_state = self.serialize_state(&candidate_state).ok_or_else(|| {
            Error::Serialization("failed to serialize MongoDB source state".to_string())
        })?;
        *self.pending_state.lock().await = Some(candidate_state);

        Ok(ProducedMessages {
            schema: Schema::Json,
            messages,
            state: Some(persisted_state),
        })
    }

    async fn on_batch_result(&self, result: SourceBatchResult) -> Result<(), Error> {
        let candidate_state = self.pending_state.lock().await.take();
        if result == SourceBatchResult::Ack
            && let Some(candidate_state) = candidate_state
        {
            *self.state.lock().await = candidate_state;
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!("Mongodb Connector with ID: {} is closing", self.id);

        let state = self.state.lock().await;

        info!(
            "PostgreSQL source connector ID: {} closed. Total documents processed: {}",
            self.id, state.total_documents_fetched
        );
        Ok(())
    }
}

fn validate_timestamp_field(doc: &Document, timestamp_field: &str) -> Result<(), Error> {
    match doc.get(timestamp_field) {
        Some(Bson::DateTime(_)) => Ok(()),
        Some(value) => Err(Error::InvalidConfigValue(format!(
            "timestamp_field '{timestamp_field}' must hold a BSON Date, found {:?}",
            value.element_type()
        ))),
        None => Err(Error::InvalidConfigValue(format!(
            "timestamp_field '{timestamp_field}' is missing from documents in the collection"
        ))),
    }
}

fn cursor_filter(timestamp_field: &str, state: &State) -> Document {
    let Some(last_timestamp) = state.last_poll_timestamp else {
        return doc! { timestamp_field: { "$type": "date" } };
    };
    let last_timestamp = mongodb::bson::DateTime::from_millis(last_timestamp.timestamp_millis());
    match state
        .last_id
        .as_deref()
        .and_then(|id| serde_json::from_str::<Bson>(id).ok())
    {
        Some(last_id) => doc! {
            "$or": [
                { timestamp_field: { "$gt": last_timestamp } },
                { timestamp_field: last_timestamp, "_id": { "$gt": last_id } },
            ]
        },
        // State saved before `last_id` existed. `$gte` re-delivers the boundary
        // documents once instead of risking skipping them.
        None => doc! { timestamp_field: { "$gte": last_timestamp } },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mongodb::bson::oid::ObjectId;

    fn test_config() -> MongodbSourceConfig {
        MongodbSourceConfig {
            connection_uri: SecretString::from("mongodb://localhost:27017"),
            database: "test_db".to_string(),
            collection: "test_collection".to_string(),
            max_pool_size: None,
            query: None,
            timestamp_field: Some("timestamp".to_string()),
            limit: Some(100),
            polling_interval: Some("100ms".to_string()),
        }
    }

    fn staged_state() -> State {
        State {
            last_poll_timestamp: DateTime::<Utc>::from_timestamp_millis(1_700_000_000_000),
            total_documents_fetched: 10,
            poll_count: 1,
            last_id: Some(serde_json::to_string(&Bson::ObjectId(ObjectId::new())).unwrap()),
        }
    }

    #[tokio::test]
    async fn given_nack_when_batch_is_staged_should_keep_committed_state() {
        let src = MongodbSource::new(1, test_config(), None);
        *src.pending_state.lock().await = Some(staged_state());

        src.on_batch_result(SourceBatchResult::Nack)
            .await
            .expect("NACK should be applied");

        let state = src.state.lock().await;
        assert!(state.last_poll_timestamp.is_none());
        assert_eq!(state.total_documents_fetched, 0);
        assert_eq!(state.poll_count, 0);
        assert!(src.pending_state.lock().await.is_none());
    }

    #[tokio::test]
    async fn given_ack_when_batch_is_staged_should_commit_candidate_state() {
        let src = MongodbSource::new(1, test_config(), None);
        let candidate = staged_state();
        *src.pending_state.lock().await = Some(candidate.clone());

        src.on_batch_result(SourceBatchResult::Ack)
            .await
            .expect("ACK should be applied");

        let state = src.state.lock().await;
        assert_eq!(state.last_poll_timestamp, candidate.last_poll_timestamp);
        assert_eq!(
            state.total_documents_fetched,
            candidate.total_documents_fetched
        );
        assert_eq!(state.poll_count, candidate.poll_count);
        assert_eq!(state.last_id, candidate.last_id);
        assert!(src.pending_state.lock().await.is_none());
    }

    #[test]
    fn given_persisted_state_should_restore_cursor_with_last_id() {
        let original = staged_state();
        let connector_state = ConnectorState::serialize(&original, CONNECTOR_NAME, 1)
            .expect("state should be serializable");

        let restored = connector_state
            .deserialize::<State>(CONNECTOR_NAME, 1)
            .expect("state should be deserializable");

        assert_eq!(restored.last_poll_timestamp, original.last_poll_timestamp);
        assert_eq!(restored.last_id, original.last_id);
    }

    #[test]
    fn given_state_saved_without_last_id_should_restore_with_none() {
        #[derive(Serialize)]
        struct StateWithoutLastId {
            last_poll_timestamp: Option<DateTime<Utc>>,
            total_documents_fetched: usize,
            poll_count: usize,
        }
        let legacy = StateWithoutLastId {
            last_poll_timestamp: DateTime::<Utc>::from_timestamp_millis(1_700_000_000_000),
            total_documents_fetched: 10,
            poll_count: 1,
        };
        let connector_state = ConnectorState::serialize(&legacy, CONNECTOR_NAME, 1)
            .expect("state should be serializable");

        let restored = connector_state
            .deserialize::<State>(CONNECTOR_NAME, 1)
            .expect("state without last_id should still deserialize");

        assert_eq!(restored.last_poll_timestamp, legacy.last_poll_timestamp);
        assert_eq!(restored.total_documents_fetched, 10);
        assert!(restored.last_id.is_none());
    }

    #[test]
    fn given_last_id_when_building_filter_should_break_timestamp_ties_on_id() {
        let state = staged_state();
        let filter = cursor_filter("timestamp", &state);
        let last_timestamp = mongodb::bson::DateTime::from_millis(1_700_000_000_000);

        assert_eq!(
            filter,
            doc! {
                "$or": [
                    { "timestamp": { "$gt": last_timestamp } },
                    { "timestamp": last_timestamp, "_id": { "$gt": serde_json::from_str::<Bson>(&state.last_id.unwrap()).unwrap() } },
                ]
            }
        );
    }

    #[test]
    fn given_no_last_id_when_building_filter_should_include_boundary_timestamp() {
        let state = State {
            last_id: None,
            ..staged_state()
        };
        let filter = cursor_filter("timestamp", &state);

        assert_eq!(
            filter,
            doc! { "timestamp": { "$gte": mongodb::bson::DateTime::from_millis(1_700_000_000_000) } }
        );
    }

    #[test]
    fn given_non_date_timestamp_field_should_reject_config() {
        let date = doc! { "timestamp": mongodb::bson::DateTime::now() };
        assert!(validate_timestamp_field(&date, "timestamp").is_ok());

        for doc in [
            doc! { "timestamp": "2024-01-15T10:30:00Z" },
            doc! { "timestamp": 1_700_000_000_000_i64 },
            doc! { "name": "no timestamp" },
        ] {
            assert!(
                matches!(
                    validate_timestamp_field(&doc, "timestamp"),
                    Err(Error::InvalidConfigValue(_))
                ),
                "{doc:?} should be rejected"
            );
        }
    }
}
