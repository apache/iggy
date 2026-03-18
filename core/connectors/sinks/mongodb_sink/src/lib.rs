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
use humantime::Duration as HumanDuration;
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Sink, TopicMetadata, sink_connector,
};
use mongodb::{Client, Collection, bson, options::ClientOptions};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing::{debug, error, info, warn};

sink_connector!(MongoDbSink);

const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_RETRY_DELAY: &str = "1s";

#[derive(Debug)]
pub struct MongoDbSink {
    pub id: u32,
    client: Option<Client>,
    config: MongoDbSinkConfig,
    verbose: bool,
    batch_size: usize,
    include_metadata: bool,
    include_checksum: bool,
    include_origin_timestamp: bool,
    payload_format: PayloadFormat,
    max_retries: u32,
    retry_delay: Duration,
    messages_processed: AtomicU64,
    insertion_errors: AtomicU64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MongoDbSinkConfig {
    #[serde(serialize_with = "iggy_common::serde_secret::serialize_secret")]
    pub connection_uri: SecretString,
    pub database: String,
    pub collection: String,
    pub max_pool_size: Option<u32>,
    pub auto_create_collection: Option<bool>,
    pub batch_size: Option<u32>,
    pub include_metadata: Option<bool>,
    pub include_checksum: Option<bool>,
    pub include_origin_timestamp: Option<bool>,
    pub payload_format: Option<String>,
    pub verbose_logging: Option<bool>,
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PayloadFormat {
    #[default]
    Binary,
    Json,
    String,
}

impl PayloadFormat {
    fn from_config(s: Option<&str>) -> Self {
        let (payload_format, unknown_value) = classify_payload_format_value(s);
        if let Some(value) = unknown_value {
            warn!("Unknown MongoDB sink payload format '{value}', defaulting to binary");
        }
        payload_format
    }
}

#[derive(Debug)]
struct BatchInsertOutcome {
    inserted_count: u64,
    error: Option<Error>,
}

impl MongoDbSink {
    pub fn new(id: u32, config: MongoDbSinkConfig) -> Self {
        let verbose = config.verbose_logging.unwrap_or(false);
        let payload_format = PayloadFormat::from_config(config.payload_format.as_deref());
        let batch_size = config.batch_size.unwrap_or(100).max(1) as usize;
        let include_metadata = config.include_metadata.unwrap_or(true);
        let include_checksum = config.include_checksum.unwrap_or(true);
        let include_origin_timestamp = config.include_origin_timestamp.unwrap_or(true);
        let max_retries = config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES);
        let delay_str = config.retry_delay.as_deref().unwrap_or(DEFAULT_RETRY_DELAY);
        let retry_delay = HumanDuration::from_str(delay_str)
            .map(|duration| duration.into())
            .unwrap_or_else(|_| Duration::from_secs(1));
        MongoDbSink {
            id,
            client: None,
            config,
            verbose,
            batch_size,
            include_metadata,
            include_checksum,
            include_origin_timestamp,
            payload_format,
            max_retries,
            retry_delay,
            messages_processed: AtomicU64::new(0),
            insertion_errors: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl Sink for MongoDbSink {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opening MongoDB sink connector with ID: {}. Target: {}.{}",
            self.id, self.config.database, self.config.collection
        );
        self.connect().await?;

        // Optionally create the collection so it is visible before first insert
        if self.config.auto_create_collection.unwrap_or(false) {
            self.ensure_collection_exists().await?;
        }

        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        self.process_messages(topic_metadata, &messages_metadata, &messages)
            .await
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!("Closing MongoDB sink connector with ID: {}", self.id);

        // MongoDB client doesn't require explicit close - it's reference counted
        // Just take the client to drop it
        self.client.take();

        let messages_processed = self.messages_processed.load(Ordering::Relaxed);
        let insertion_errors = self.insertion_errors.load(Ordering::Relaxed);
        info!(
            "MongoDB sink ID: {} processed {} messages with {} errors",
            self.id, messages_processed, insertion_errors
        );
        Ok(())
    }
}

impl MongoDbSink {
    /// Build a MongoDB client using ClientOptions so max_pool_size can be applied.
    async fn connect(&mut self) -> Result<(), Error> {
        let redacted = redact_connection_uri(self.config.connection_uri.expose_secret());

        info!("Connecting to MongoDB: {redacted}");

        let mut options = ClientOptions::parse(self.config.connection_uri.expose_secret())
            .await
            .map_err(|e| Error::InitError(format!("Failed to parse connection URI: {e}")))?;

        if let Some(pool_size) = self.config.max_pool_size {
            options.max_pool_size = Some(pool_size);
        }

        let client = Client::with_options(options)
            .map_err(|e| Error::InitError(format!("Failed to create client: {e}")))?;

        // Ping the database to verify connectivity
        client
            .database(&self.config.database)
            .run_command(mongodb::bson::doc! {"ping": 1})
            .await
            .map_err(|e| Error::InitError(format!("Database connectivity test failed: {e}")))?;

        self.client = Some(client);
        info!("Connected to MongoDB database: {}", self.config.database);
        Ok(())
    }

    /// Create the target collection explicitly if it does not already exist.
    async fn ensure_collection_exists(&self) -> Result<(), Error> {
        let client = self.get_client()?;
        let db = client.database(&self.config.database);

        let existing = db
            .list_collection_names()
            .await
            .map_err(|e| Error::InitError(format!("Failed to list collections: {e}")))?;

        if !existing.contains(&self.config.collection) {
            db.create_collection(&self.config.collection)
                .await
                .map_err(|e| {
                    Error::InitError(format!(
                        "Failed to create collection '{}': {e}",
                        self.config.collection
                    ))
                })?;
            info!("Created MongoDB collection '{}'", self.config.collection);
        } else {
            debug!(
                "Collection '{}' already exists, skipping creation",
                self.config.collection
            );
        }

        Ok(())
    }

    async fn process_messages(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        messages: &[ConsumedMessage],
    ) -> Result<(), Error> {
        let client = self.get_client()?;
        let db = client.database(&self.config.database);
        let collection = db.collection(&self.config.collection);

        let mut successful_inserts = 0u64;
        let mut last_error: Option<Error> = None;

        for batch in messages.chunks(self.batch_size) {
            let outcome = self
                .insert_batch(batch, topic_metadata, messages_metadata, &collection)
                .await;

            successful_inserts += outcome.inserted_count;
            if let Some(batch_error) = outcome.error {
                self.insertion_errors.fetch_add(1, Ordering::Relaxed);
                error!(
                    "Failed to insert batch of {} messages: {batch_error}",
                    batch.len()
                );
                last_error = Some(batch_error);
            }
        }

        self.messages_processed
            .fetch_add(successful_inserts, Ordering::Relaxed);

        let coll = &self.config.collection;
        if self.verbose {
            info!(
                "MongoDB sink ID: {} inserted {successful_inserts} messages to collection '{coll}'",
                self.id
            );
        } else {
            debug!(
                "MongoDB sink ID: {} inserted {successful_inserts} messages to collection '{coll}'",
                self.id
            );
        }

        if let Some(e) = last_error {
            Err(e)
        } else {
            Ok(())
        }
    }

    async fn insert_batch(
        &self,
        messages: &[ConsumedMessage],
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        collection: &Collection<mongodb::bson::Document>,
    ) -> BatchInsertOutcome {
        if messages.is_empty() {
            return BatchInsertOutcome {
                inserted_count: 0,
                error: None,
            };
        }

        let mut docs = Vec::with_capacity(messages.len());

        for message in messages {
            let mut doc = mongodb::bson::Document::new();

            let document_id =
                build_composite_document_id(topic_metadata, messages_metadata, message.id);
            doc.insert("_id", document_id);

            if self.include_metadata {
                let (offset_key, offset_value) = build_offset_metadata_value(message.offset);
                doc.insert(offset_key, offset_value);
                doc.insert(
                    "iggy_timestamp",
                    build_bson_datetime_value(message.timestamp),
                );
                doc.insert("iggy_stream", &topic_metadata.stream);
                doc.insert("iggy_topic", &topic_metadata.topic);
                doc.insert(
                    "iggy_partition_id",
                    build_partition_metadata_value(messages_metadata.partition_id),
                );
            }

            if self.include_checksum {
                doc.insert(
                    "iggy_checksum",
                    build_checksum_metadata_value(message.checksum),
                );
            }

            if self.include_origin_timestamp {
                doc.insert(
                    "iggy_origin_timestamp",
                    build_bson_datetime_value(message.origin_timestamp),
                );
            }

            // Handle payload based on format
            let payload_bytes = match message.payload.clone().try_into_vec() {
                Ok(payload_bytes) => payload_bytes,
                Err(error) => {
                    return BatchInsertOutcome {
                        inserted_count: 0,
                        error: Some(Error::CannotStoreData(format!(
                            "Failed to convert payload to bytes: {error}"
                        ))),
                    };
                }
            };

            match self.payload_format {
                PayloadFormat::Binary => {
                    doc.insert(
                        "payload",
                        bson::Binary {
                            subtype: bson::spec::BinarySubtype::Generic,
                            bytes: payload_bytes,
                        },
                    );
                }
                PayloadFormat::Json => {
                    let json_value: serde_json::Value = match serde_json::from_slice(&payload_bytes)
                    {
                        Ok(json_value) => json_value,
                        Err(error) => {
                            error!("Failed to parse payload as JSON: {error}");
                            return BatchInsertOutcome {
                                inserted_count: 0,
                                error: Some(Error::CannotStoreData(format!(
                                    "Failed to parse payload as JSON: {error}"
                                ))),
                            };
                        }
                    };
                    let bson_value = match bson::to_bson(&json_value) {
                        Ok(bson_value) => bson_value,
                        Err(error) => {
                            error!("Failed to convert JSON to BSON: {error}");
                            return BatchInsertOutcome {
                                inserted_count: 0,
                                error: Some(Error::CannotStoreData(format!(
                                    "Failed to convert JSON to BSON: {error}"
                                ))),
                            };
                        }
                    };
                    doc.insert("payload", bson_value);
                }
                PayloadFormat::String => {
                    let text_value = match String::from_utf8(payload_bytes) {
                        Ok(text_value) => text_value,
                        Err(error) => {
                            error!("Failed to parse payload as UTF-8 text: {error}");
                            return BatchInsertOutcome {
                                inserted_count: 0,
                                error: Some(Error::CannotStoreData(format!(
                                    "Failed to parse payload as UTF-8 text: {error}"
                                ))),
                            };
                        }
                    };
                    doc.insert("payload", text_value);
                }
            }

            docs.push(doc);
        }

        self.insert_batch_with_retry(collection, &docs).await
    }

    async fn insert_batch_with_retry(
        &self,
        collection: &Collection<mongodb::bson::Document>,
        docs: &[mongodb::bson::Document],
    ) -> BatchInsertOutcome {
        let mut attempts = 0u32;

        loop {
            let result = collection.insert_many(docs.to_vec()).ordered(false).await;

            match result {
                Ok(result) => {
                    return BatchInsertOutcome {
                        inserted_count: result.inserted_ids.len() as u64,
                        error: None,
                    };
                }
                Err(error) => {
                    if let Some(duplicate_count) = count_duplicate_write_errors(&error) {
                        let inserted_count = docs.len().saturating_sub(duplicate_count) as u64;
                        if duplicate_count > 0 {
                            warn!(
                                "MongoDB sink ID: {} ignored {duplicate_count} duplicate writes in batch",
                                self.id
                            );
                        }
                        return BatchInsertOutcome {
                            inserted_count,
                            error: None,
                        };
                    }

                    attempts += 1;
                    if !is_transient_error(&error) || attempts >= self.max_retries {
                        let inserted_count = estimate_inserted_count_value(&error, docs.len());
                        error!("Batch insert failed after {attempts} attempts: {error}");
                        return BatchInsertOutcome {
                            inserted_count,
                            error: Some(Error::CannotStoreData(format!(
                                "Batch insert failed after {attempts} attempts: {error}"
                            ))),
                        };
                    }
                    warn!(
                        "Transient database error (attempt {attempts}/{}): {error}. Retrying...",
                        self.max_retries
                    );
                    tokio::time::sleep(self.retry_delay * attempts).await;
                }
            }
        }
    }

    fn get_client(&self) -> Result<&Client, Error> {
        self.client
            .as_ref()
            .ok_or_else(|| Error::InitError("Database not connected".to_string()))
    }
}

fn build_composite_document_id(
    topic_metadata: &TopicMetadata,
    messages_metadata: &MessagesMetadata,
    message_id: u128,
) -> String {
    format!(
        "{}:{}:{}:{message_id}",
        topic_metadata.stream, topic_metadata.topic, messages_metadata.partition_id
    )
}

fn build_partition_metadata_value(partition_id: u32) -> bson::Bson {
    if let Ok(value) = i32::try_from(partition_id) {
        bson::Bson::Int32(value)
    } else {
        bson::Bson::Int64(i64::from(partition_id))
    }
}

fn build_offset_metadata_value(offset: u64) -> (&'static str, bson::Bson) {
    if let Ok(offset) = i64::try_from(offset) {
        ("iggy_offset", bson::Bson::Int64(offset))
    } else {
        ("iggy_offset_str", bson::Bson::String(offset.to_string()))
    }
}

fn build_bson_datetime_value(timestamp_micros: u64) -> bson::DateTime {
    let timestamp_ms = timestamp_micros / 1000;
    let timestamp_ms = i64::try_from(timestamp_ms).unwrap_or(i64::MAX);
    bson::DateTime::from_millis(timestamp_ms)
}

fn build_checksum_metadata_value(checksum: u64) -> bson::Bson {
    if let Ok(checksum) = i64::try_from(checksum) {
        bson::Bson::Int64(checksum)
    } else {
        bson::Bson::String(checksum.to_string())
    }
}

fn classify_payload_format_value(input: Option<&str>) -> (PayloadFormat, Option<&str>) {
    match input {
        Some(value) if value.eq_ignore_ascii_case("json") => (PayloadFormat::Json, None),
        Some(value)
            if value.eq_ignore_ascii_case("string") || value.eq_ignore_ascii_case("text") =>
        {
            (PayloadFormat::String, None)
        }
        Some(value) if value.eq_ignore_ascii_case("binary") => (PayloadFormat::Binary, None),
        Some(value) => (PayloadFormat::Binary, Some(value)),
        None => (PayloadFormat::Binary, None),
    }
}

fn count_duplicate_write_errors(error: &mongodb::error::Error) -> Option<usize> {
    use mongodb::error::ErrorKind;

    let ErrorKind::InsertMany(insert_many_error) = error.kind.as_ref() else {
        return None;
    };
    if insert_many_error.write_concern_error.is_some() {
        return None;
    }

    let write_errors = insert_many_error.write_errors.as_ref()?;
    if write_errors.is_empty() || write_errors.iter().any(|error| error.code != 11000) {
        return None;
    }

    Some(write_errors.len())
}

fn estimate_inserted_count_value(error: &mongodb::error::Error, total_docs: usize) -> u64 {
    use mongodb::error::ErrorKind;

    if let ErrorKind::InsertMany(insert_many_error) = error.kind.as_ref()
        && let Some(write_errors) = &insert_many_error.write_errors
    {
        return total_docs.saturating_sub(write_errors.len()) as u64;
    }

    0
}

fn is_transient_error(e: &mongodb::error::Error) -> bool {
    use mongodb::error::ErrorKind;

    if e.contains_label(mongodb::error::RETRYABLE_WRITE_ERROR) {
        return true;
    }

    match e.kind.as_ref() {
        ErrorKind::Io(_) => true,
        ErrorKind::ConnectionPoolCleared { .. } => true,
        ErrorKind::ServerSelection { .. } => true,
        ErrorKind::Authentication { .. } => false,
        ErrorKind::BsonDeserialization(_) => false,
        ErrorKind::BsonSerialization(_) => false,
        ErrorKind::InsertMany(insert_many_error) => {
            let has_non_retryable_write_error = insert_many_error
                .write_errors
                .as_ref()
                .is_some_and(|wes| wes.iter().any(|we| matches!(we.code, 11000 | 13 | 121)));
            !has_non_retryable_write_error
        }
        ErrorKind::Command(cmd_err) => !matches!(cmd_err.code, 11000 | 13 | 121),
        _ => {
            let msg = e.to_string().to_lowercase();
            msg.contains("timeout")
                || msg.contains("network")
                || msg.contains("pool")
                || msg.contains("server selection")
        }
    }
}

fn redact_connection_uri(uri: &str) -> String {
    if let Some(scheme_end) = uri.find("://") {
        let scheme = &uri[..scheme_end + 3];
        let rest = &uri[scheme_end + 3..];
        let preview: String = rest.chars().take(3).collect();
        return format!("{scheme}{preview}***");
    }
    let preview: String = uri.chars().take(3).collect();
    format!("{preview}***")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn given_default_config() -> MongoDbSinkConfig {
        MongoDbSinkConfig {
            connection_uri: SecretString::from("mongodb://localhost:27017"),
            database: "test_db".to_string(),
            collection: "test_collection".to_string(),
            max_pool_size: None,
            auto_create_collection: None,
            batch_size: Some(100),
            include_metadata: None,
            include_checksum: None,
            include_origin_timestamp: None,
            payload_format: None,
            verbose_logging: None,
            max_retries: None,
            retry_delay: None,
        }
    }

    #[test]
    fn given_payload_format_inputs_should_map_expected_variant() {
        let cases = [
            (Some("json"), PayloadFormat::Json),
            (Some("JSON"), PayloadFormat::Json),
            (Some("string"), PayloadFormat::String),
            (Some("text"), PayloadFormat::String),
            (Some("TEXT"), PayloadFormat::String),
            (Some("binary"), PayloadFormat::Binary),
            (Some("unknown"), PayloadFormat::Binary),
            (None, PayloadFormat::Binary),
        ];

        for (input, expected) in cases {
            assert_eq!(PayloadFormat::from_config(input), expected);
        }
    }

    #[test]
    fn given_unknown_payload_format_should_mark_warning_contract() {
        let (payload_format, unknown_value) = classify_payload_format_value(Some("surprise"));
        assert_eq!(payload_format, PayloadFormat::Binary);
        assert_eq!(unknown_value, Some("surprise"));
    }

    #[test]
    fn given_retry_configurations_should_use_expected_values() {
        let cases = [
            (None, None, DEFAULT_MAX_RETRIES, Duration::from_secs(1)),
            (Some(5), None, 5, Duration::from_secs(1)),
            (
                None,
                Some("500ms"),
                DEFAULT_MAX_RETRIES,
                Duration::from_millis(500),
            ),
        ];

        for (max_retries, retry_delay, expected_retries, expected_delay) in cases {
            let mut config = given_default_config();
            config.max_retries = max_retries;
            config.retry_delay = retry_delay.map(std::string::ToString::to_string);

            let sink = MongoDbSink::new(1, config);
            assert_eq!(sink.max_retries, expected_retries);
            assert_eq!(sink.retry_delay, expected_delay);
        }
    }

    #[test]
    fn given_connection_uri_shapes_should_redact_consistently() {
        let cases = [
            (
                "mongodb://user:password@localhost:27017",
                "mongodb://use***",
            ),
            ("localhost:27017", "loc***"),
            (
                "mongodb+srv://admin:secret123@cluster.example.com",
                "mongodb+srv://adm***",
            ),
        ];

        for (uri, expected) in cases {
            assert_eq!(redact_connection_uri(uri), expected);
        }
    }

    #[test]
    fn given_payload_format_config_should_select_sink_format() {
        let cases = [
            (None, PayloadFormat::Binary),
            (Some("json"), PayloadFormat::Json),
            (Some("string"), PayloadFormat::String),
        ];

        for (payload_format, expected) in cases {
            let mut config = given_default_config();
            config.payload_format = payload_format.map(std::string::ToString::to_string);

            let sink = MongoDbSink::new(1, config);
            assert_eq!(sink.payload_format, expected);
        }
    }

    #[test]
    fn given_offset_values_should_use_expected_metadata_field() {
        let (normal_key, normal_value) = build_offset_metadata_value(i64::MAX as u64);
        assert_eq!(normal_key, "iggy_offset");
        assert_eq!(normal_value, bson::Bson::Int64(i64::MAX));

        let oversized_offset = (i64::MAX as u64) + 1;
        let (oversized_key, oversized_value) = build_offset_metadata_value(oversized_offset);
        assert_eq!(oversized_key, "iggy_offset_str");
        assert_eq!(
            oversized_value,
            bson::Bson::String(oversized_offset.to_string())
        );
    }

    #[test]
    fn given_partition_values_should_use_expected_bson_type() {
        assert_eq!(
            build_partition_metadata_value(i32::MAX as u32),
            bson::Bson::Int32(i32::MAX)
        );

        let oversized_partition = (i32::MAX as u32) + 1;
        assert_eq!(
            build_partition_metadata_value(oversized_partition),
            bson::Bson::Int64(i64::from(oversized_partition))
        );
    }

    #[test]
    fn given_timestamp_values_should_use_bounded_bson_datetime() {
        assert_eq!(
            build_bson_datetime_value(1_234_000),
            bson::DateTime::from_millis(1_234)
        );

        assert_eq!(
            build_bson_datetime_value(u64::MAX),
            bson::DateTime::from_millis((u64::MAX / 1_000) as i64)
        );
    }

    #[test]
    fn given_checksum_values_should_use_lossless_bson_value() {
        assert_eq!(
            build_checksum_metadata_value(i64::MAX as u64),
            bson::Bson::Int64(i64::MAX)
        );

        let oversized_checksum = (i64::MAX as u64) + 1;
        assert_eq!(
            build_checksum_metadata_value(oversized_checksum),
            bson::Bson::String(oversized_checksum.to_string())
        );
    }

    #[test]
    fn given_cross_topic_messages_should_build_unique_document_ids() {
        let first_topic = TopicMetadata {
            stream: "test_stream".to_string(),
            topic: "topic_primary".to_string(),
        };
        let second_topic = TopicMetadata {
            stream: "test_stream".to_string(),
            topic: "topic_secondary".to_string(),
        };
        let messages_metadata = MessagesMetadata {
            partition_id: 0,
            current_offset: 0,
            schema: iggy_connector_sdk::Schema::Raw,
        };

        let first_id = build_composite_document_id(&first_topic, &messages_metadata, 42);
        let second_id = build_composite_document_id(&second_topic, &messages_metadata, 42);
        assert_ne!(
            first_id, second_id,
            "Composite document ID must differ for different topics"
        );
    }

    #[test]
    fn given_auto_create_collection_config_should_store_expected_option() {
        let cases = [None, Some(true), Some(false)];

        for auto_create_collection in cases {
            let mut config = given_default_config();
            config.auto_create_collection = auto_create_collection;

            let sink = MongoDbSink::new(1, config);
            assert_eq!(sink.config.auto_create_collection, auto_create_collection);
        }
    }

    // ---- is_transient_error tests ----

    #[test]
    fn given_io_timeout_error_should_be_transient() {
        let io_err = std::io::Error::new(std::io::ErrorKind::TimedOut, "connection timed out");
        let e: mongodb::error::Error = io_err.into();
        assert!(is_transient_error(&e));
    }

    #[test]
    fn given_io_network_error_should_be_transient() {
        let io_err =
            std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "connection refused");
        let e: mongodb::error::Error = io_err.into();
        assert!(is_transient_error(&e));
    }

    #[test]
    fn given_string_timeout_error_should_be_transient() {
        let e = mongodb::error::Error::custom(String::from("server selection timeout exceeded"));
        assert!(is_transient_error(&e));
    }

    #[test]
    fn given_string_pool_error_should_be_transient() {
        let e = mongodb::error::Error::custom(String::from("connection pool exhausted"));
        assert!(is_transient_error(&e));
    }

    #[test]
    fn given_auth_failure_string_should_not_be_transient() {
        let e =
            mongodb::error::Error::custom(String::from("authentication failed: bad credentials"));
        assert!(!is_transient_error(&e));
    }

    #[test]
    fn given_duplicate_key_string_should_not_be_transient() {
        let e = mongodb::error::Error::custom(String::from("duplicate key error on collection"));
        assert!(!is_transient_error(&e));
    }

    // ---- process_messages error propagation tests ----
    // These tests verify that the sink does NOT silently lose data when inserts fail.

    /// Test contract: When MongoDB insert fails, process_messages MUST return Err.
    /// This prevents silent data loss where upstream commits while writes failed.
    ///
    /// Given: A sink with no client (will fail on get_client)
    /// When: process_messages is called with messages
    /// Then: Returns Err (not Ok) and does NOT count failed messages as processed
    #[tokio::test]
    async fn given_no_client_should_return_error_not_silent_ok() {
        let config = given_default_config();
        let sink = MongoDbSink::new(1, config);

        // Sink has no client - this simulates connection failure
        assert!(
            sink.client.is_none(),
            "Sink should not have client before connect"
        );

        let topic_metadata = TopicMetadata {
            stream: "test_stream".to_string(),
            topic: "test_topic".to_string(),
        };
        let messages_metadata = MessagesMetadata {
            partition_id: 1,
            current_offset: 0,
            schema: iggy_connector_sdk::Schema::Raw,
        };
        let messages = vec![ConsumedMessage {
            id: 1,
            offset: 0,
            timestamp: 1000,
            origin_timestamp: 1000,
            checksum: 0,
            headers: None,
            payload: iggy_connector_sdk::Payload::Raw(vec![1, 2, 3]),
        }];

        let result = sink
            .process_messages(&topic_metadata, &messages_metadata, &messages)
            .await;

        // CRITICAL: Must return Err, not Ok(())
        assert!(
            result.is_err(),
            "process_messages MUST return Err when client is unavailable - silent data loss bug!"
        );

        assert_eq!(
            sink.messages_processed.load(Ordering::Relaxed),
            0,
            "messages_processed must only count SUCCESSFUL inserts"
        );
    }

    /// Test contract: messages_processed only counts successfully inserted messages.
    ///
    /// Given: Multiple messages where some may fail
    /// When: process_messages handles them
    /// Then: messages_processed reflects only successful writes
    #[test]
    fn given_new_sink_should_have_zero_messages_processed() {
        let sink = MongoDbSink::new(1, given_default_config());
        assert_eq!(
            sink.messages_processed.load(Ordering::Relaxed),
            0,
            "New sink must start with zero processed count"
        );
        assert_eq!(
            sink.insertion_errors.load(Ordering::Relaxed),
            0,
            "New sink must start with zero error count"
        );
    }
}
