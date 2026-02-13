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

use std::time::Duration;

use crate::clickhouse_client::ClickHouseClient;
use crate::generic_inserter;
use crate::{
    ClickHouseSink, ClickHouseSinkConfig, InsertType, MessageRowWithMetadata,
    MessageRowWithoutMetadata,
};
use async_trait::async_trait;
use clickhouse::error::Error as ChError;
use iggy_connector_sdk::{ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata};
use simd_json::{OwnedValue, base::ValueAsObject};
use tracing::{debug, error, info, warn};

#[async_trait]
impl Sink for ClickHouseSink {
    async fn open(&mut self) -> Result<(), Error> {
        let clickhouse_client = ClickHouseClient::init(self.config.clone())
            .map_err(|e| Error::InitError(e.to_string()))?;
        self.client = Some(clickhouse_client);
        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        self.batch_insert(&messages, topic_metadata, &messages_metadata)
            .await
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!("Closing ClickHouse sink connector with ID: {}", self.id);

        let state = self.state.lock().await;
        info!(
            "ClickHouse sink ID: {} processed {} messages with {} batch attempt failures and with {} batch failures",
            self.id, state.messages_processed, state.insert_batch_failed, state.insert_batch_failed
        );
        Ok(())
    }
}

impl ClickHouseSink {
    pub fn new(id: u32, config: ClickHouseSinkConfig) -> Self {
        let verbose = config.verbose_logging();
        ClickHouseSink {
            id,
            client: None,
            config,
            state: tokio::sync::Mutex::new(Default::default()),
            verbose,
        }
    }

    async fn run_inserter(
        &self,
        messages: &[ConsumedMessage],
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
    ) -> clickhouse::error::Result<()> {
        match self.config.insert_type {
            InsertType::Json => {
                let inserter = self
                    .client
                    .as_ref()
                    .ok_or_else(|| {
                        clickhouse::error::Error::Custom(
                            "ClickHouse client not initialized".to_string(),
                        )
                    })?
                    .create_formatted_inserter();

                generic_inserter::run_inserter(inserter, messages, |msg| {
                    self.prepare_json_data(msg, topic_metadata, messages_metadata)
                })
                .await?;
            }
            InsertType::RowBinary => {
                if self.config.include_metadata() {
                    let inserter = self
                        .client
                        .as_ref()
                        .ok_or_else(|| {
                            clickhouse::error::Error::Custom(
                                "ClickHouse client not initialized".to_string(),
                            )
                        })?
                        .create_typed_inserter::<MessageRowWithMetadata>(&self.config.table);

                    generic_inserter::run_inserter(inserter, messages, |msg| {
                        self.prepare_row_with_metadata(msg, topic_metadata, messages_metadata)
                    })
                    .await?;
                } else {
                    let inserter = self
                        .client
                        .as_ref()
                        .ok_or_else(|| {
                            clickhouse::error::Error::Custom(
                                "ClickHouse client not initialized".to_string(),
                            )
                        })?
                        .create_typed_inserter::<MessageRowWithoutMetadata>(&self.config.table);

                    generic_inserter::run_inserter(inserter, messages, |msg| {
                        self.prepare_row_without_metadata(msg)
                    })
                    .await?;
                }
            }
        }

        Ok(())
    }

    // create the batch and invoke try_insert
    async fn batch_insert(
        &self,
        messages: &[ConsumedMessage],
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
    ) -> Result<(), Error> {
        let batch_size = self.config.max_batch_size() as usize;
        for chunk in messages.chunks(batch_size) {
            self.try_insert(chunk, topic_metadata, messages_metadata)
                .await?;
        }
        Ok(())
    }

    // try insert batch of data with retry
    async fn try_insert(
        &self,
        messages: &[ConsumedMessage],
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
    ) -> Result<(), Error> {
        let retry = self.config.retry();
        let max_retries = self.config.max_retry();
        let mut retry_count = 0;
        let base_delay = Duration::from_millis(self.config.retry_base_delay());

        loop {
            match self
                .run_inserter(messages, topic_metadata, messages_metadata)
                .await
            {
                Ok(_) => {
                    let msg_count = messages.len();
                    let table = &self.config.table;
                    if self.verbose {
                        info!(
                            "ClickHouse sink ID: {} processed {} messages to table '{}' (attempt {})",
                            self.id,
                            msg_count,
                            table,
                            retry_count + 1
                        );
                    } else {
                        debug!(
                            "ClickHouse sink ID: {} processed {} messages to table '{}' (attempt {})",
                            self.id,
                            msg_count,
                            table,
                            retry_count + 1
                        );
                    }

                    let mut state = self.state.lock().await;
                    state.messages_processed += messages.len() as u64;
                    drop(state);

                    return Ok(());
                }
                //TODO: is_retryable check -- done
                Err(ch_err) if retry && retry_count < max_retries && is_retryable(&ch_err) => {
                    let delay = base_delay * 2u32.pow(retry_count);
                    warn!(
                        "Failed to write messages (attempt {}): {}. Retrying in {:?}...",
                        retry_count + 1,
                        ch_err,
                        delay
                    );
                    let mut state = self.state.lock().await;
                    state.insert_attempt_failed += 1;
                    drop(state);
                    retry_count += 1;
                    tokio::time::sleep(delay).await;
                }
                Err(ch_err) => {
                    if retry && retry_count >= max_retries {
                        error!(
                            "Failed to write {} messages after {} attempts: {}",
                            messages.len(),
                            max_retries + 1,
                            ch_err
                        );
                    } else {
                        error!("Non-retryable error, aborting insert: {}", ch_err);
                    }

                    let mut state = self.state.lock().await;
                    state.insert_batch_failed += 1;
                    drop(state);

                    return Err(Error::CannotStoreData(ch_err.to_string()));
                }
            }
        }
    }

    fn prepare_json_data(
        &self,
        message: &ConsumedMessage,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
    ) -> clickhouse::error::Result<Vec<u8>> {
        let mut json_obj = if let Some(field_mappings) = &self.config.field_mappings {
            let fields: Vec<String> = field_mappings.keys().cloned().collect();
            self.get_json_row(message, &fields)
                .map_err(|e| clickhouse::error::Error::Custom(e.to_string()))?
        } else {
            match &message.payload {
                Payload::Json(v) => {
                    if let Some(obj) = v.as_object() {
                        obj.clone()
                    } else {
                        return Err(clickhouse::error::Error::Custom(
                            "Payload is not a JSON object".to_string(),
                        ));
                    }
                }
                _ => {
                    return Err(clickhouse::error::Error::Custom(
                        "Expected JSON payload".to_string(),
                    ));
                }
            }
        };

        if self.config.include_metadata() {
            self.add_metadata_to_json_object(
                &mut json_obj,
                message,
                topic_metadata,
                messages_metadata,
            );
        }

        self.get_bytes_from_json(json_obj)
            .map_err(|e| clickhouse::error::Error::Custom(e.to_string()))
    }

    fn prepare_row_with_metadata(
        &self,
        message: &ConsumedMessage,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
    ) -> clickhouse::error::Result<MessageRowWithMetadata> {
        self.message_to_row_with_metadata(message, topic_metadata, messages_metadata)
            .map_err(|e| clickhouse::error::Error::Custom(e.to_string()))
    }

    fn prepare_row_without_metadata(
        &self,
        message: &ConsumedMessage,
    ) -> clickhouse::error::Result<MessageRowWithoutMetadata> {
        self.message_to_row_without_metadata(message)
            .map_err(|e| clickhouse::error::Error::Custom(e.to_string()))
    }

    /// Extract a value from a nested JSON path like "a.b.c"
    /// Returns None if the path doesn't exist or isn't valid
    fn extract_field<'a>(
        &self,
        value: &'a simd_json::OwnedValue,
        path: &str,
    ) -> Option<&'a simd_json::OwnedValue> {
        if path.is_empty() {
            return Some(value);
        }

        //non-nested
        if !path.contains('.') {
            return value.as_object()?.get(path);
        }

        let mut current = value;
        for segment in path.split('.') {
            current = current.as_object()?.get(segment)?;
        }
        Some(current)
    }

    fn map_field_path_to_column(&self, field_path: &str) -> Result<String, Error> {
        self.config
            .field_mappings
            .as_ref()
            .ok_or(Error::InvalidConfig)?
            .get(field_path)
            .cloned()
            .map_or(Ok(field_path.to_string()), Ok)
    }

    fn get_json_payload<'a>(&self, message: &'a ConsumedMessage) -> Result<&'a OwnedValue, Error> {
        let json_value = match &message.payload {
            Payload::Json(v) => v,
            _ => return Err(Error::InvalidJsonPayload),
        };
        Ok(json_value)
    }

    fn get_json_row(
        &self,
        message: &ConsumedMessage,
        fields: &[String],
    ) -> Result<simd_json::owned::Object, Error> {
        let mut row_obj = simd_json::owned::Object::new();
        let json_value = self.get_json_payload(message)?;
        for field_path in fields {
            if let Some(value) = self.extract_field(json_value, field_path) {
                let column_name = self.map_field_path_to_column(field_path)?;
                row_obj.insert(column_name, value.clone());
            }
        }
        Ok(row_obj)
    }

    fn get_bytes_from_json(&self, row_obj: simd_json::owned::Object) -> Result<Vec<u8>, Error> {
        let row_bytes = simd_json::to_vec(&OwnedValue::Object(Box::new(row_obj)))
            .map_err(|_e| Error::Serialization("bad json body".to_string()))?;
        Ok(row_bytes)
    }

    /// All metadata fields use the `iggy_` prefix
    fn add_metadata_to_json_object(
        &self,
        json_obj: &mut simd_json::owned::Object,
        message: &ConsumedMessage,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
    ) {
        json_obj.insert(
            "iggy_stream".to_string(),
            OwnedValue::from(topic_metadata.stream.clone()),
        );
        json_obj.insert(
            "iggy_topic".to_string(),
            OwnedValue::from(topic_metadata.topic.clone()),
        );
        json_obj.insert(
            "iggy_partition_id".to_string(),
            OwnedValue::from(messages_metadata.partition_id),
        );
        json_obj.insert(
            "iggy_id".to_string(),
            OwnedValue::from(message.id.to_string()),
        );
        json_obj.insert("iggy_offset".to_string(), OwnedValue::from(message.offset));
        json_obj.insert(
            "iggy_checksum".to_string(),
            OwnedValue::from(message.checksum),
        );
        json_obj.insert(
            "iggy_timestamp".to_string(),
            OwnedValue::from(message.timestamp),
        );
        json_obj.insert(
            "iggy_origin_timestamp".to_string(),
            OwnedValue::from(message.origin_timestamp),
        );
    }

    fn message_to_row_without_metadata(
        &self,
        message: &ConsumedMessage,
    ) -> Result<MessageRowWithoutMetadata, Error> {
        let payload_string = match &message.payload {
            Payload::Json(json_val) => simd_json::to_string(json_val).map_err(|e| {
                Error::Serialization(format!("Failed to serialize JSON payload: {}", e))
            })?,
            _ => return Err(Error::InvalidPayloadType),
        };

        Ok(MessageRowWithoutMetadata {
            payload: payload_string,
        })
    }

    fn message_to_row_with_metadata(
        &self,
        message: &ConsumedMessage,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
    ) -> Result<MessageRowWithMetadata, Error> {
        let payload_string = match &message.payload {
            Payload::Json(json_val) => simd_json::to_string(json_val).map_err(|e| {
                Error::Serialization(format!("Failed to serialize JSON payload: {}", e))
            })?,
            _ => return Err(Error::InvalidPayloadType),
        };

        Ok(MessageRowWithMetadata {
            iggy_stream: topic_metadata.stream.clone(),
            iggy_topic: topic_metadata.topic.clone(),
            iggy_partition_id: messages_metadata.partition_id,
            iggy_id: message.id.to_string(),
            iggy_offset: message.offset,
            iggy_checksum: message.checksum,
            iggy_timestamp: message.timestamp,
            iggy_origin_timestamp: message.origin_timestamp,
            payload: payload_string,
        })
    }
}

fn is_retryable(error: &clickhouse::error::Error) -> bool {
    matches!(
        error,
        ChError::Network(_) | ChError::TimedOut | ChError::Other(_)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AuthType, ClickHouseSink, ClickHouseSinkConfig, InsertType};
    use iggy_connector_sdk::Schema;
    use std::collections::HashMap;

    // ── Test fixtures ────────────────────────────────────────────────────────

    fn test_config() -> ClickHouseSinkConfig {
        ClickHouseSinkConfig {
            url: "http://localhost:8123".to_string(),
            compression_enabled: None,
            tls_enabled: None,
            tls_root_ca_cert: None,
            tls_client_cert: None,
            tls_client_key: None,
            auth_type: AuthType::None,
            username: None,
            password: None,
            jwt_token: None,
            role: None,
            table: "test_table".to_string(),
            database: None,
            max_batch_size: None,
            chunk_size: None,
            retry: None,
            max_retry: None,
            base_delay: None,
            insert_type: InsertType::Json,
            payload_data_type: None,
            include_metadata: None,
            field_mappings: None,
            verbose_logging: None,
        }
    }

    fn test_config_with_mappings(mappings: HashMap<String, String>) -> ClickHouseSinkConfig {
        let mut config = test_config();
        config.field_mappings = Some(mappings);
        config
    }

    fn test_sink(config: ClickHouseSinkConfig) -> ClickHouseSink {
        ClickHouseSink::new(1, config)
    }

    fn test_json_message(json: simd_json::OwnedValue) -> ConsumedMessage {
        ConsumedMessage {
            id: 42,
            offset: 100,
            checksum: 999,
            timestamp: 1_700_000_000_000_000,
            origin_timestamp: 1_699_999_999_000_000,
            headers: None,
            payload: Payload::Json(json),
        }
    }

    fn test_topic_metadata() -> TopicMetadata {
        TopicMetadata {
            stream: "test_stream".to_string(),
            topic: "test_topic".to_string(),
        }
    }

    fn test_messages_metadata() -> MessagesMetadata {
        MessagesMetadata {
            partition_id: 1,
            current_offset: 100,
            schema: Schema::Json,
        }
    }

    fn sample_json() -> simd_json::OwnedValue {
        simd_json::json!({
            "name": "Alice",
            "age": 30,
            "address": {
                "city": "Wonderland",
                "zip": "12345"
            }
        })
    }

    // ── extract_field tests ──────────────────────────────────────────────────

    #[test]
    fn given_nested_json_path_should_extract_value() {
        let sink = test_sink(test_config());
        let json = sample_json();

        let result = sink.extract_field(&json, "address.city");
        assert_eq!(result, Some(&OwnedValue::from("Wonderland")));
    }

    #[test]
    fn given_single_level_path_should_extract_value() {
        let sink = test_sink(test_config());
        let json = sample_json();

        let result = sink.extract_field(&json, "name");
        assert_eq!(result, Some(&OwnedValue::from("Alice")));
    }

    #[test]
    fn given_empty_path_should_return_root() {
        let sink = test_sink(test_config());
        let json = sample_json();

        let result = sink.extract_field(&json, "");
        assert_eq!(result, Some(&json));
    }

    #[test]
    fn given_missing_path_should_return_none() {
        let sink = test_sink(test_config());
        let json = sample_json();

        assert_eq!(sink.extract_field(&json, "nonexistent"), None);
        assert_eq!(sink.extract_field(&json, "address.country"), None);
        assert_eq!(sink.extract_field(&json, "a.b.c"), None);
    }

    // ── map_field_path_to_column tests ───────────────────────────────────────

    #[test]
    fn given_field_mappings_should_map_path_to_column() {
        let mut mappings = HashMap::new();
        mappings.insert("user.name".to_string(), "username".to_string());
        let sink = test_sink(test_config_with_mappings(mappings));

        let result = sink.map_field_path_to_column("user.name");
        assert_eq!(result.unwrap(), "username");
    }

    #[test]
    fn given_unmapped_path_should_fall_back_to_path() {
        let mut mappings = HashMap::new();
        mappings.insert("user.name".to_string(), "username".to_string());
        let sink = test_sink(test_config_with_mappings(mappings));

        let result = sink.map_field_path_to_column("user.email");
        assert_eq!(result.unwrap(), "user.email");
    }

    #[test]
    fn given_no_field_mappings_should_return_error() {
        let sink = test_sink(test_config());

        let result = sink.map_field_path_to_column("anything");
        assert!(matches!(result, Err(Error::InvalidConfig)));
    }

    // ── get_json_payload tests ───────────────────────────────────────────────

    #[test]
    fn given_json_payload_should_return_json_value() {
        let sink = test_sink(test_config());
        let json = sample_json();
        let message = test_json_message(json.clone());

        let result = sink.get_json_payload(&message);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), &json);
    }

    #[test]
    fn given_non_json_payload_should_return_error() {
        let sink = test_sink(test_config());
        let message = ConsumedMessage {
            id: 1,
            offset: 0,
            checksum: 0,
            timestamp: 0,
            origin_timestamp: 0,
            headers: None,
            payload: Payload::Raw(vec![1, 2, 3]),
        };

        let result = sink.get_json_payload(&message);
        assert!(matches!(result, Err(Error::InvalidJsonPayload)));
    }

    // ── get_json_row tests ───────────────────────────────────────────────────

    #[test]
    fn given_fields_and_mappings_should_build_json_row() {
        let mut mappings = HashMap::new();
        mappings.insert("name".to_string(), "user_name".to_string());
        mappings.insert("address.city".to_string(), "city".to_string());
        let sink = test_sink(test_config_with_mappings(mappings));

        let message = test_json_message(sample_json());
        let fields = vec!["name".to_string(), "address.city".to_string()];

        let row = sink.get_json_row(&message, &fields).unwrap();
        assert_eq!(row.get("user_name"), Some(&OwnedValue::from("Alice")));
        assert_eq!(row.get("city"), Some(&OwnedValue::from("Wonderland")));
        assert_eq!(row.len(), 2);
    }

    #[test]
    fn given_missing_fields_should_skip_in_json_row() {
        let mut mappings = HashMap::new();
        mappings.insert("name".to_string(), "user_name".to_string());
        mappings.insert("nonexistent".to_string(), "nothing".to_string());
        let sink = test_sink(test_config_with_mappings(mappings));

        let message = test_json_message(sample_json());
        let fields = vec!["name".to_string(), "nonexistent".to_string()];

        let row = sink.get_json_row(&message, &fields).unwrap();
        assert_eq!(row.get("user_name"), Some(&OwnedValue::from("Alice")));
        assert_eq!(row.get("nothing"), None);
        assert_eq!(row.len(), 1);
    }

    // ── get_bytes_from_json tests ────────────────────────────────────────────

    #[test]
    fn given_json_object_should_serialize_to_bytes() {
        let sink = test_sink(test_config());
        let mut obj = simd_json::owned::Object::new();
        obj.insert("key".to_string(), OwnedValue::from("value"));

        let bytes = sink.get_bytes_from_json(obj).unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(parsed["key"], "value");
    }

    // ── add_metadata_to_json_object tests ────────────────────────────────────

    #[test]
    fn given_message_should_add_all_metadata_fields() {
        let sink = test_sink(test_config());
        let message = test_json_message(sample_json());
        let topic_meta = test_topic_metadata();
        let msg_meta = test_messages_metadata();
        let mut obj = simd_json::owned::Object::new();

        sink.add_metadata_to_json_object(&mut obj, &message, &topic_meta, &msg_meta);

        assert_eq!(
            obj.get("iggy_stream"),
            Some(&OwnedValue::from("test_stream"))
        );
        assert_eq!(obj.get("iggy_topic"), Some(&OwnedValue::from("test_topic")));
        assert_eq!(obj.get("iggy_partition_id"), Some(&OwnedValue::from(1u32)));
        assert_eq!(obj.get("iggy_id"), Some(&OwnedValue::from("42")));
        assert_eq!(obj.get("iggy_offset"), Some(&OwnedValue::from(100u64)));
        assert_eq!(obj.get("iggy_checksum"), Some(&OwnedValue::from(999u64)));
        assert_eq!(
            obj.get("iggy_timestamp"),
            Some(&OwnedValue::from(1_700_000_000_000_000u64))
        );
        assert_eq!(
            obj.get("iggy_origin_timestamp"),
            Some(&OwnedValue::from(1_699_999_999_000_000u64))
        );
        assert_eq!(obj.len(), 8);
    }

    // ── message_to_row_with_metadata tests ───────────────────────────────────

    #[test]
    fn given_json_message_should_build_row_with_metadata() {
        let sink = test_sink(test_config());
        let message = test_json_message(simd_json::json!({"key": "value"}));
        let topic_meta = test_topic_metadata();
        let msg_meta = test_messages_metadata();

        let row = sink
            .message_to_row_with_metadata(&message, &topic_meta, &msg_meta)
            .unwrap();

        assert_eq!(row.iggy_stream, "test_stream");
        assert_eq!(row.iggy_topic, "test_topic");
        assert_eq!(row.iggy_partition_id, 1);
        assert_eq!(row.iggy_id, "42");
        assert_eq!(row.iggy_offset, 100);
        assert_eq!(row.iggy_checksum, 999);
        assert_eq!(row.iggy_timestamp, 1_700_000_000_000_000);
        assert_eq!(row.iggy_origin_timestamp, 1_699_999_999_000_000);

        let parsed: serde_json::Value = serde_json::from_str(&row.payload).unwrap();
        assert_eq!(parsed["key"], "value");
    }

    #[test]
    fn given_non_json_message_should_fail_row_with_metadata() {
        let sink = test_sink(test_config());
        let message = ConsumedMessage {
            id: 1,
            offset: 0,
            checksum: 0,
            timestamp: 0,
            origin_timestamp: 0,
            headers: None,
            payload: Payload::Raw(vec![1, 2, 3]),
        };
        let topic_meta = test_topic_metadata();
        let msg_meta = test_messages_metadata();

        let result = sink.message_to_row_with_metadata(&message, &topic_meta, &msg_meta);
        assert!(matches!(result, Err(Error::InvalidPayloadType)));
    }

    // ── message_to_row_without_metadata tests ────────────────────────────────

    #[test]
    fn given_json_message_should_build_row_without_metadata() {
        let sink = test_sink(test_config());
        let message = test_json_message(simd_json::json!({"hello": "world"}));

        let row = sink.message_to_row_without_metadata(&message).unwrap();

        let parsed: serde_json::Value = serde_json::from_str(&row.payload).unwrap();
        assert_eq!(parsed["hello"], "world");
    }

    #[test]
    fn given_non_json_message_should_fail_row_without_metadata() {
        let sink = test_sink(test_config());
        let message = ConsumedMessage {
            id: 1,
            offset: 0,
            checksum: 0,
            timestamp: 0,
            origin_timestamp: 0,
            headers: None,
            payload: Payload::Raw(vec![1, 2, 3]),
        };

        let result = sink.message_to_row_without_metadata(&message);
        assert!(matches!(result, Err(Error::InvalidPayloadType)));
    }
}
