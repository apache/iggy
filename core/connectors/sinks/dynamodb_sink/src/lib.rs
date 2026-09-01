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
use aws_config::BehaviorVersion;
use aws_sdk_dynamodb::Client;
use aws_sdk_dynamodb::config::{Credentials, Region};
use aws_sdk_dynamodb::error::{ProvideErrorMetadata, SdkError};
use aws_sdk_dynamodb::primitives::Blob;
use aws_sdk_dynamodb::types::{
    AttributeValue, KeySchemaElement, KeyType, PutRequest, WriteRequest,
};
use humantime::Duration as HumanDuration;
use iggy_connector_sdk::retry::{exponential_backoff, jitter};
use iggy_connector_sdk::{
    ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata, sink_connector,
};
use secrecy::{ExposeSecret, SecretString};
use serde::Deserialize;
use simd_json::{OwnedValue, StaticNode};
use std::collections::HashMap;
use std::fmt::Write;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing::{debug, error, info, warn};

sink_connector!(DynamoDbSink);

/// `BatchWriteItem` rejects any request carrying more than 25 write requests.
const MAX_BATCH_WRITE_ITEMS: usize = 25;
/// DynamoDB rejects items larger than 400 KB.
const MAX_ITEM_SIZE: usize = 400 * 1024;
const DEFAULT_PARTITION_KEY_FIELD: &str = "iggy_id";
const DEFAULT_MAX_RETRIES: u32 = 3;
const DEFAULT_RETRY_DELAY: &str = "500ms";
const DEFAULT_MAX_RETRY_DELAY: &str = "5s";
const PAYLOAD_FIELD: &str = "payload";
const CREDENTIALS_PROVIDER_NAME: &str = "iggy-dynamodb-sink";

#[derive(Debug)]
pub struct DynamoDbSink {
    pub id: u32,
    client: Option<Client>,
    config: DynamoDbSinkConfig,
    partition_key_field: String,
    sort_key_field: Option<String>,
    batch_size: usize,
    include_metadata: bool,
    include_checksum: bool,
    include_origin_timestamp: bool,
    max_item_size: usize,
    max_retries: u32,
    retry_delay: Duration,
    max_retry_delay: Duration,
    verbose: bool,
    items_written: AtomicU64,
    items_skipped: AtomicU64,
    items_deduplicated: AtomicU64,
    write_errors: AtomicU64,
}

/// Only `Deserialize` - nothing serializes a plugin config back out, and the
/// missing impl is what keeps the credentials unserializable.
#[derive(Debug, Clone, Deserialize)]
pub struct DynamoDbSinkConfig {
    pub table: String,
    pub region: Option<String>,
    pub endpoint: Option<String>,
    pub access_key_id: Option<SecretString>,
    pub secret_access_key: Option<SecretString>,
    pub session_token: Option<SecretString>,
    pub partition_key_field: Option<String>,
    pub sort_key_field: Option<String>,
    pub batch_size: Option<u32>,
    pub include_metadata: Option<bool>,
    pub include_checksum: Option<bool>,
    pub include_origin_timestamp: Option<bool>,
    pub max_item_size: Option<usize>,
    pub max_retries: Option<u32>,
    pub retry_delay: Option<String>,
    pub max_retry_delay: Option<String>,
    pub verbose_logging: Option<bool>,
}

impl DynamoDbSink {
    pub fn new(id: u32, config: DynamoDbSinkConfig) -> Self {
        let partition_key_field = config
            .partition_key_field
            .clone()
            .unwrap_or_else(|| DEFAULT_PARTITION_KEY_FIELD.to_owned());
        let sort_key_field = config.sort_key_field.clone();
        let batch_size = config
            .batch_size
            .unwrap_or(MAX_BATCH_WRITE_ITEMS as u32)
            .clamp(1, MAX_BATCH_WRITE_ITEMS as u32) as usize;
        let include_metadata = config.include_metadata.unwrap_or(true);
        let include_checksum = config.include_checksum.unwrap_or(true);
        let include_origin_timestamp = config.include_origin_timestamp.unwrap_or(true);
        let max_item_size = config
            .max_item_size
            .unwrap_or(MAX_ITEM_SIZE)
            .min(MAX_ITEM_SIZE);
        let max_retries = config.max_retries.unwrap_or(DEFAULT_MAX_RETRIES);
        let retry_delay = parse_duration(config.retry_delay.as_deref(), DEFAULT_RETRY_DELAY);
        let mut max_retry_delay =
            parse_duration(config.max_retry_delay.as_deref(), DEFAULT_MAX_RETRY_DELAY);
        if max_retry_delay < retry_delay {
            warn!(
                "DynamoDB sink ID: {id} has max_retry_delay below retry_delay, raising it to the retry delay"
            );
            max_retry_delay = retry_delay;
        }
        let verbose = config.verbose_logging.unwrap_or(false);

        DynamoDbSink {
            id,
            client: None,
            config,
            partition_key_field,
            sort_key_field,
            batch_size,
            include_metadata,
            include_checksum,
            include_origin_timestamp,
            max_item_size,
            max_retries,
            retry_delay,
            max_retry_delay,
            verbose,
            items_written: AtomicU64::new(0),
            items_skipped: AtomicU64::new(0),
            items_deduplicated: AtomicU64::new(0),
            write_errors: AtomicU64::new(0),
        }
    }
}

#[async_trait]
impl Sink for DynamoDbSink {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opening DynamoDB sink connector with ID: {}, table: {}",
            self.id, self.config.table
        );
        let client = self.build_client().await?;
        let description = client
            .describe_table()
            .table_name(&self.config.table)
            .send()
            .await
            .map_err(|error| {
                Error::InitError(format!(
                    "DynamoDB table '{}' is not reachable, error: {}",
                    self.config.table,
                    describe_sdk_error(&error)
                ))
            })?;
        self.validate_key_schema(
            description
                .table
                .and_then(|table| table.key_schema)
                .unwrap_or_default(),
        )?;

        self.client = Some(client);
        info!(
            "Opened DynamoDB sink connector with ID: {}, table: {}",
            self.id, self.config.table
        );
        Ok(())
    }

    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        self.write_messages(topic_metadata, &messages_metadata, messages)
            .await
    }

    async fn close(&mut self) -> Result<(), Error> {
        info!("Closing DynamoDB sink connector with ID: {}", self.id);
        self.client.take();
        info!(
            "Closed DynamoDB sink connector with ID: {}, written: {}, skipped: {}, deduplicated: {}, errors: {}",
            self.id,
            self.items_written.load(Ordering::Relaxed),
            self.items_skipped.load(Ordering::Relaxed),
            self.items_deduplicated.load(Ordering::Relaxed),
            self.write_errors.load(Ordering::Relaxed)
        );
        Ok(())
    }
}

impl DynamoDbSink {
    async fn build_client(&self) -> Result<Client, Error> {
        if self.config.access_key_id.is_some() != self.config.secret_access_key.is_some() {
            return Err(Error::InvalidConfigValue(
                "Partially configured credentials. You must provide both access_key_id \
                 and secret_access_key, or omit both."
                    .to_owned(),
            ));
        }

        let mut loader = aws_config::defaults(BehaviorVersion::latest());
        if let Some(region) = &self.config.region {
            loader = loader.region(Region::new(region.clone()));
        }
        if let Some(endpoint) = &self.config.endpoint {
            info!("Using custom DynamoDB endpoint: {endpoint}");
            loader = loader.endpoint_url(endpoint);
        }
        if let (Some(access_key_id), Some(secret_access_key)) =
            (&self.config.access_key_id, &self.config.secret_access_key)
        {
            info!(
                "Using explicit DynamoDB credentials for sink ID: {}",
                self.id
            );
            loader = loader.credentials_provider(Credentials::new(
                access_key_id.expose_secret(),
                secret_access_key.expose_secret(),
                self.config
                    .session_token
                    .as_ref()
                    .map(|token| token.expose_secret().to_owned()),
                None,
                CREDENTIALS_PROVIDER_NAME,
            ));
        } else {
            info!(
                "No explicit credentials provided, using the default AWS credential chain for sink ID: {}",
                self.id
            );
        }

        Ok(Client::new(&loader.load().await))
    }

    /// A key field that does not match the table makes DynamoDB reject every
    /// write, and the runtime stops the connector on the first error, so the
    /// mismatch is reported while the sink is still opening.
    fn validate_key_schema(&self, key_schema: Vec<KeySchemaElement>) -> Result<(), Error> {
        let table_key = |key_type: KeyType| {
            key_schema
                .iter()
                .find(|element| element.key_type == key_type)
                .map(|element| element.attribute_name.clone())
        };

        let partition_key = table_key(KeyType::Hash);
        if let Some(partition_key) = &partition_key
            && partition_key != &self.partition_key_field
        {
            return Err(Error::InvalidConfigValue(format!(
                "Table '{}' uses '{partition_key}' as its partition key, but partition_key_field is '{}'",
                self.config.table, self.partition_key_field
            )));
        }

        match (table_key(KeyType::Range), &self.sort_key_field) {
            (Some(sort_key), Some(sort_key_field)) if &sort_key != sort_key_field => {
                Err(Error::InvalidConfigValue(format!(
                    "Table '{}' uses '{sort_key}' as its sort key, but sort_key_field is '{sort_key_field}'",
                    self.config.table
                )))
            }
            (Some(sort_key), None) => Err(Error::InvalidConfigValue(format!(
                "Table '{}' has a sort key '{sort_key}', so sort_key_field must be set",
                self.config.table
            ))),
            (None, Some(sort_key_field)) => Err(Error::InvalidConfigValue(format!(
                "Table '{}' has no sort key, so sort_key_field '{sort_key_field}' must be removed",
                self.config.table
            ))),
            _ => Ok(()),
        }
    }

    async fn write_messages(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        mut messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        let client = self.get_client()?;
        let mut items = Vec::with_capacity(messages.len());
        let mut skipped = 0u64;

        for message in messages.iter_mut() {
            match self.build_item(topic_metadata, messages_metadata, message) {
                Ok(item) => items.push(item),
                Err(reason) => {
                    skipped += 1;
                    warn!(
                        "DynamoDB sink ID: {} skipped message at offset: {}, reason: {reason}",
                        self.id, message.offset
                    );
                }
            }
        }

        let (mut items, duplicates) = self.deduplicate_items(items);
        if duplicates > 0 {
            debug!(
                "DynamoDB sink ID: {} dropped {duplicates} items sharing a primary key",
                self.id
            );
            self.items_deduplicated
                .fetch_add(duplicates, Ordering::Relaxed);
        }
        if skipped > 0 {
            self.items_skipped.fetch_add(skipped, Ordering::Relaxed);
        }

        let mut written = 0u64;
        let mut last_error: Option<Error> = None;
        while !items.is_empty() {
            let chunk_size = self.batch_size.min(items.len());
            let chunk = items.drain(..chunk_size).collect::<Vec<_>>();
            match self.write_chunk(client, chunk).await {
                Ok(count) => written += count,
                Err(error) => {
                    self.write_errors
                        .fetch_add(chunk_size as u64, Ordering::Relaxed);
                    error!(
                        "DynamoDB sink ID: {} failed to write {chunk_size} items to table: {}, error: {error}",
                        self.id, self.config.table
                    );
                    last_error = Some(error);
                }
            }
        }
        self.items_written.fetch_add(written, Ordering::Relaxed);

        let table = &self.config.table;
        if self.verbose {
            info!(
                "DynamoDB sink ID: {} wrote {written} items to table: {table}, current_offset: {}",
                self.id, messages_metadata.current_offset
            );
        } else {
            debug!(
                "DynamoDB sink ID: {} wrote {written} items to table: {table}, current_offset: {}",
                self.id, messages_metadata.current_offset
            );
        }

        match last_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// `BatchWriteItem` rejects a request that carries two writes for the same
    /// primary key, so only the newest item per key survives. DynamoDB would
    /// overwrite the older ones anyway.
    fn deduplicate_items(
        &self,
        items: Vec<HashMap<String, AttributeValue>>,
    ) -> (Vec<HashMap<String, AttributeValue>>, u64) {
        let mut positions: HashMap<String, usize> = HashMap::with_capacity(items.len());
        let mut deduplicated: Vec<HashMap<String, AttributeValue>> =
            Vec::with_capacity(items.len());
        let mut duplicates = 0u64;

        for item in items {
            let key = self.key_signature(&item);
            match positions.get(&key) {
                Some(&position) => {
                    deduplicated[position] = item;
                    duplicates += 1;
                }
                None => {
                    positions.insert(key, deduplicated.len());
                    deduplicated.push(item);
                }
            }
        }

        (deduplicated, duplicates)
    }

    /// `AttributeValue` is not hashable, so the key attributes are rendered
    /// into a string that only lives for the deduplication pass.
    fn key_signature(&self, item: &HashMap<String, AttributeValue>) -> String {
        let mut signature = key_attribute_signature(item.get(&self.partition_key_field));
        if let Some(sort_key_field) = &self.sort_key_field {
            signature.push('|');
            signature.push_str(&key_attribute_signature(item.get(sort_key_field)));
        }
        signature
    }

    /// One `BatchWriteItem` round, retrying both throttled requests and the
    /// `UnprocessedItems` the API returns inside an otherwise successful
    /// response.
    async fn write_chunk(
        &self,
        client: &Client,
        items: Vec<HashMap<String, AttributeValue>>,
    ) -> Result<u64, Error> {
        let total = items.len() as u64;
        let mut pending = Vec::with_capacity(items.len());
        for item in items {
            let put_request =
                PutRequest::builder()
                    .set_item(Some(item))
                    .build()
                    .map_err(|error| {
                        Error::InvalidRecordValue(format!(
                            "Cannot build DynamoDB put request: {error}"
                        ))
                    })?;
            pending.push(WriteRequest::builder().put_request(put_request).build());
        }

        let mut attempt = 0u32;
        loop {
            // `request_items` consumes the batch, and a failed send does not
            // hand it back, so the retry loop keeps its own copy.
            let result = client
                .batch_write_item()
                .request_items(&self.config.table, pending.clone())
                .send()
                .await;

            let unprocessed = match result {
                Ok(output) => output
                    .unprocessed_items
                    .and_then(|mut tables| tables.remove(&self.config.table))
                    .unwrap_or_default(),
                Err(error) => {
                    if !is_transient_error(&error) {
                        return Err(Error::PermanentHttpError(format!(
                            "DynamoDB batch write to table '{}' failed, error: {}",
                            self.config.table,
                            describe_sdk_error(&error)
                        )));
                    }
                    attempt += 1;
                    if attempt > self.max_retries {
                        return Err(Error::CannotStoreData(format!(
                            "DynamoDB batch write to table '{}' failed after {attempt} attempts, error: {}",
                            self.config.table,
                            describe_sdk_error(&error)
                        )));
                    }
                    warn!(
                        "Transient DynamoDB error on attempt {attempt}/{} for sink ID: {}, error: {}",
                        self.max_retries,
                        self.id,
                        describe_sdk_error(&error)
                    );
                    self.backoff(attempt).await;
                    continue;
                }
            };

            if unprocessed.is_empty() {
                return Ok(total);
            }

            attempt += 1;
            if attempt > self.max_retries {
                return Err(Error::CannotStoreData(format!(
                    "DynamoDB left {} of {total} items unprocessed in table '{}' after {attempt} attempts",
                    unprocessed.len(),
                    self.config.table
                )));
            }
            warn!(
                "DynamoDB returned {} unprocessed items on attempt {attempt}/{} for sink ID: {}",
                unprocessed.len(),
                self.max_retries,
                self.id
            );
            self.backoff(attempt).await;
            pending = unprocessed;
        }
    }

    async fn backoff(&self, attempt: u32) {
        let delay = jitter(exponential_backoff(
            self.retry_delay,
            attempt - 1,
            self.max_retry_delay,
        ));
        tokio::time::sleep(delay).await;
    }

    fn build_item(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: &MessagesMetadata,
        message: &mut ConsumedMessage,
    ) -> Result<HashMap<String, AttributeValue>, Error> {
        // The payload is moved out to build the item without copying it, so
        // only the message metadata is read afterwards.
        let payload = std::mem::replace(&mut message.payload, Payload::Raw(Vec::new()));
        let mut item = payload_into_item(payload)?;

        if self.include_metadata {
            item.insert(
                "iggy_stream".to_owned(),
                AttributeValue::S(topic_metadata.stream.clone()),
            );
            item.insert(
                "iggy_topic".to_owned(),
                AttributeValue::S(topic_metadata.topic.clone()),
            );
            item.insert(
                "iggy_partition_id".to_owned(),
                AttributeValue::N(messages_metadata.partition_id.to_string()),
            );
            item.insert(
                "iggy_offset".to_owned(),
                AttributeValue::N(message.offset.to_string()),
            );
            item.insert(
                "iggy_timestamp".to_owned(),
                AttributeValue::N(message.timestamp.to_string()),
            );
        }
        if self.include_checksum {
            item.insert(
                "iggy_checksum".to_owned(),
                AttributeValue::N(message.checksum.to_string()),
            );
        }
        if self.include_origin_timestamp {
            item.insert(
                "iggy_origin_timestamp".to_owned(),
                AttributeValue::N(message.origin_timestamp.to_string()),
            );
        }

        item.entry(self.partition_key_field.clone())
            .or_insert_with(|| {
                AttributeValue::S(build_message_key(
                    topic_metadata,
                    messages_metadata,
                    message.id,
                ))
            });
        if let Some(sort_key_field) = &self.sort_key_field {
            item.entry(sort_key_field.clone())
                .or_insert_with(|| AttributeValue::N(message.offset.to_string()));
        }

        validate_key_attribute(&item, &self.partition_key_field)?;
        if let Some(sort_key_field) = &self.sort_key_field {
            validate_key_attribute(&item, sort_key_field)?;
        }

        let size = estimate_item_size(&item);
        if size > self.max_item_size {
            return Err(Error::InvalidRecordValue(format!(
                "item size of {size} bytes exceeds the limit of {} bytes",
                self.max_item_size
            )));
        }

        Ok(item)
    }

    fn get_client(&self) -> Result<&Client, Error> {
        self.client
            .as_ref()
            .ok_or_else(|| Error::InitError("DynamoDB client is not connected".to_owned()))
    }
}

fn parse_duration(raw: Option<&str>, default: &str) -> Duration {
    let value = raw.unwrap_or(default);
    HumanDuration::from_str(value)
        .map(Duration::from)
        .unwrap_or_else(|_| {
            warn!("Invalid DynamoDB sink duration '{value}', falling back to '{default}'");
            HumanDuration::from_str(default)
                .map(Duration::from)
                .unwrap_or(Duration::from_millis(500))
        })
}

/// DynamoDB items are attribute maps, so anything that is not a JSON object
/// is nested under a single payload attribute.
fn payload_into_item(payload: Payload) -> Result<HashMap<String, AttributeValue>, Error> {
    match payload {
        Payload::Json(value) => Ok(match json_into_attribute_value(value) {
            AttributeValue::M(item) => item,
            other => HashMap::from([(PAYLOAD_FIELD.to_owned(), other)]),
        }),
        Payload::Text(text) => Ok(HashMap::from([(
            PAYLOAD_FIELD.to_owned(),
            AttributeValue::S(text),
        )])),
        Payload::Raw(bytes) => {
            // simd-json unescapes in place, so a failed parse leaves the
            // buffer rewritten and only the copy can be thrown away.
            let mut buffer = bytes.clone();
            let attribute = match simd_json::to_owned_value(&mut buffer) {
                Ok(value) => json_into_attribute_value(value),
                Err(_) => AttributeValue::B(Blob::new(bytes)),
            };
            Ok(match attribute {
                AttributeValue::M(item) => item,
                other => HashMap::from([(PAYLOAD_FIELD.to_owned(), other)]),
            })
        }
        Payload::Proto(_) | Payload::FlatBuffer(_) | Payload::Avro(_) => {
            Err(Error::InvalidPayloadType)
        }
    }
}

fn json_into_attribute_value(value: OwnedValue) -> AttributeValue {
    match value {
        OwnedValue::Static(StaticNode::Null) => AttributeValue::Null(true),
        OwnedValue::Static(StaticNode::Bool(value)) => AttributeValue::Bool(value),
        OwnedValue::Static(StaticNode::I64(value)) => AttributeValue::N(value.to_string()),
        OwnedValue::Static(StaticNode::U64(value)) => AttributeValue::N(value.to_string()),
        // DynamoDB numbers have no NaN or infinity, so those become null
        // instead of a value the API would reject.
        OwnedValue::Static(StaticNode::F64(value)) => {
            if value.is_finite() {
                AttributeValue::N(value.to_string())
            } else {
                AttributeValue::Null(true)
            }
        }
        OwnedValue::String(value) => AttributeValue::S(value),
        OwnedValue::Array(values) => AttributeValue::L(
            values
                .into_iter()
                .map(json_into_attribute_value)
                .collect::<Vec<_>>(),
        ),
        OwnedValue::Object(values) => AttributeValue::M(
            values
                .into_iter()
                .map(|(key, value)| (key, json_into_attribute_value(value)))
                .collect(),
        ),
    }
}

fn key_attribute_signature(value: Option<&AttributeValue>) -> String {
    match value {
        Some(AttributeValue::S(text)) => format!("S:{text}"),
        Some(AttributeValue::N(number)) => format!("N:{number}"),
        Some(AttributeValue::B(blob)) => {
            let mut signature = String::from("B:");
            for byte in blob.as_ref() {
                let _ = write!(signature, "{byte:02x}");
            }
            signature
        }
        _ => String::new(),
    }
}

fn build_message_key(
    topic_metadata: &TopicMetadata,
    messages_metadata: &MessagesMetadata,
    message_id: u128,
) -> String {
    format!(
        "{}:{}:{}:{message_id}",
        topic_metadata.stream, topic_metadata.topic, messages_metadata.partition_id
    )
}

/// DynamoDB key attributes must be a non-empty string, number, or binary
/// value. Anything else fails the whole batch with a validation error, so the
/// offending message is dropped before it is sent.
fn validate_key_attribute(
    item: &HashMap<String, AttributeValue>,
    field: &str,
) -> Result<(), Error> {
    let value = item
        .get(field)
        .ok_or_else(|| Error::InvalidRecordValue(format!("key field '{field}' is missing")))?;
    let valid = match value {
        AttributeValue::S(text) => !text.is_empty(),
        AttributeValue::N(number) => !number.is_empty(),
        AttributeValue::B(blob) => !blob.as_ref().is_empty(),
        _ => false,
    };
    if valid {
        Ok(())
    } else {
        Err(Error::InvalidRecordValue(format!(
            "key field '{field}' must be a non-empty string, number, or binary value"
        )))
    }
}

fn estimate_item_size(item: &HashMap<String, AttributeValue>) -> usize {
    item.iter()
        .map(|(name, value)| name.len() + attribute_value_size(value))
        .sum()
}

fn attribute_value_size(value: &AttributeValue) -> usize {
    match value {
        AttributeValue::S(text) => text.len(),
        AttributeValue::N(number) => number.len(),
        AttributeValue::B(blob) => blob.as_ref().len(),
        AttributeValue::Bool(_) | AttributeValue::Null(_) => 1,
        AttributeValue::Ss(values) => values.iter().map(String::len).sum(),
        AttributeValue::Ns(values) => values.iter().map(String::len).sum(),
        AttributeValue::Bs(values) => values.iter().map(|blob| blob.as_ref().len()).sum(),
        AttributeValue::L(values) => values.iter().map(attribute_value_size).sum(),
        AttributeValue::M(values) => estimate_item_size(values),
        _ => 0,
    }
}

fn is_transient_error<E, R>(error: &SdkError<E, R>) -> bool
where
    E: ProvideErrorMetadata,
{
    match error {
        SdkError::TimeoutError(_) | SdkError::DispatchFailure(_) => true,
        SdkError::ResponseError(_) => true,
        SdkError::ServiceError(service_error) => {
            is_transient_code(service_error.err().code().unwrap_or_default())
        }
        _ => false,
    }
}

fn is_transient_code(code: &str) -> bool {
    matches!(
        code,
        "ProvisionedThroughputExceededException"
            | "RequestLimitExceeded"
            | "ThrottlingException"
            | "InternalServerError"
            | "ServiceUnavailable"
            | "TransactionInProgressException"
    )
}

fn describe_sdk_error<E, R>(error: &SdkError<E, R>) -> String
where
    E: ProvideErrorMetadata,
{
    match error.code() {
        Some(code) => format!("{code}: {}", error.message().unwrap_or("no message")),
        None => error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_connector_sdk::Schema;

    fn given_default_config() -> DynamoDbSinkConfig {
        DynamoDbSinkConfig {
            table: "iggy_messages".to_owned(),
            region: Some("us-east-1".to_owned()),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
            session_token: None,
            partition_key_field: None,
            sort_key_field: None,
            batch_size: None,
            include_metadata: None,
            include_checksum: None,
            include_origin_timestamp: None,
            max_item_size: None,
            max_retries: None,
            retry_delay: None,
            max_retry_delay: None,
            verbose_logging: None,
        }
    }

    fn given_topic_metadata() -> TopicMetadata {
        TopicMetadata {
            stream: "test_stream".to_owned(),
            topic: "test_topic".to_owned(),
        }
    }

    fn given_messages_metadata() -> MessagesMetadata {
        MessagesMetadata {
            partition_id: 1,
            current_offset: 10,
            schema: Schema::Json,
        }
    }

    fn given_message(payload: Payload) -> ConsumedMessage {
        ConsumedMessage {
            id: 42,
            offset: 7,
            checksum: 99,
            timestamp: 1_700_000_000,
            origin_timestamp: 1_600_000_000,
            headers: None,
            payload,
        }
    }

    fn given_json_payload(raw: &str) -> Payload {
        let mut bytes = raw.as_bytes().to_vec();
        Payload::Json(simd_json::to_owned_value(&mut bytes).expect("parse JSON"))
    }

    #[test]
    fn given_no_batch_size_when_created_should_use_dynamodb_limit() {
        let sink = DynamoDbSink::new(1, given_default_config());
        assert_eq!(sink.batch_size, MAX_BATCH_WRITE_ITEMS);
    }

    #[test]
    fn given_oversized_batch_size_when_created_should_clamp_to_dynamodb_limit() {
        let mut config = given_default_config();
        config.batch_size = Some(500);
        let sink = DynamoDbSink::new(1, config);
        assert_eq!(sink.batch_size, MAX_BATCH_WRITE_ITEMS);
    }

    #[test]
    fn given_zero_batch_size_when_created_should_use_single_item_batches() {
        let mut config = given_default_config();
        config.batch_size = Some(0);
        let sink = DynamoDbSink::new(1, config);
        assert_eq!(sink.batch_size, 1);
    }

    #[test]
    fn given_reversed_retry_delays_when_created_should_raise_max_retry_delay() {
        let mut config = given_default_config();
        config.retry_delay = Some("5s".to_owned());
        config.max_retry_delay = Some("1s".to_owned());
        let sink = DynamoDbSink::new(1, config);
        assert_eq!(sink.retry_delay, Duration::from_secs(5));
        assert_eq!(sink.max_retry_delay, Duration::from_secs(5));
    }

    #[test]
    fn given_invalid_duration_when_created_should_use_default() {
        let mut config = given_default_config();
        config.retry_delay = Some("not-a-duration".to_owned());
        let sink = DynamoDbSink::new(1, config);
        assert_eq!(sink.retry_delay, Duration::from_millis(500));
    }

    #[test]
    fn given_oversized_max_item_size_when_created_should_clamp_to_dynamodb_limit() {
        let mut config = given_default_config();
        config.max_item_size = Some(MAX_ITEM_SIZE * 2);
        let sink = DynamoDbSink::new(1, config);
        assert_eq!(sink.max_item_size, MAX_ITEM_SIZE);
    }

    #[test]
    fn given_json_object_payload_when_built_should_map_attributes() {
        let sink = DynamoDbSink::new(1, given_default_config());
        let mut message = given_message(given_json_payload(
            r#"{"name":"first","count":3,"ratio":1.5,"active":true,"tags":["a","b"],"nested":{"key":"value"},"missing":null}"#,
        ));

        let item = sink
            .build_item(
                &given_topic_metadata(),
                &given_messages_metadata(),
                &mut message,
            )
            .expect("build item");

        assert_eq!(item["name"], AttributeValue::S("first".to_owned()));
        assert_eq!(item["count"], AttributeValue::N("3".to_owned()));
        assert_eq!(item["ratio"], AttributeValue::N("1.5".to_owned()));
        assert_eq!(item["active"], AttributeValue::Bool(true));
        assert_eq!(item["missing"], AttributeValue::Null(true));
        assert_eq!(
            item["tags"],
            AttributeValue::L(vec![
                AttributeValue::S("a".to_owned()),
                AttributeValue::S("b".to_owned())
            ])
        );
        assert_eq!(
            item["nested"],
            AttributeValue::M(HashMap::from([(
                "key".to_owned(),
                AttributeValue::S("value".to_owned())
            )]))
        );
    }

    #[test]
    fn given_json_object_payload_when_built_should_add_metadata_attributes() {
        let sink = DynamoDbSink::new(1, given_default_config());
        let mut message = given_message(given_json_payload(r#"{"name":"first"}"#));

        let item = sink
            .build_item(
                &given_topic_metadata(),
                &given_messages_metadata(),
                &mut message,
            )
            .expect("build item");

        assert_eq!(
            item["iggy_stream"],
            AttributeValue::S("test_stream".to_owned())
        );
        assert_eq!(
            item["iggy_topic"],
            AttributeValue::S("test_topic".to_owned())
        );
        assert_eq!(item["iggy_partition_id"], AttributeValue::N("1".to_owned()));
        assert_eq!(item["iggy_offset"], AttributeValue::N("7".to_owned()));
        assert_eq!(item["iggy_checksum"], AttributeValue::N("99".to_owned()));
        assert_eq!(
            item["iggy_origin_timestamp"],
            AttributeValue::N("1600000000".to_owned())
        );
        assert_eq!(
            item[DEFAULT_PARTITION_KEY_FIELD],
            AttributeValue::S("test_stream:test_topic:1:42".to_owned())
        );
    }

    #[test]
    fn given_disabled_metadata_when_built_should_only_keep_payload_and_key() {
        let mut config = given_default_config();
        config.include_metadata = Some(false);
        config.include_checksum = Some(false);
        config.include_origin_timestamp = Some(false);
        let sink = DynamoDbSink::new(1, config);
        let mut message = given_message(given_json_payload(r#"{"name":"first"}"#));

        let item = sink
            .build_item(
                &given_topic_metadata(),
                &given_messages_metadata(),
                &mut message,
            )
            .expect("build item");

        assert_eq!(item.len(), 2);
        assert!(item.contains_key("name"));
        assert!(item.contains_key(DEFAULT_PARTITION_KEY_FIELD));
    }

    #[test]
    fn given_payload_with_partition_key_when_built_should_keep_payload_value() {
        let mut config = given_default_config();
        config.partition_key_field = Some("user_id".to_owned());
        let sink = DynamoDbSink::new(1, config);
        let mut message = given_message(given_json_payload(r#"{"user_id":"u-1"}"#));

        let item = sink
            .build_item(
                &given_topic_metadata(),
                &given_messages_metadata(),
                &mut message,
            )
            .expect("build item");

        assert_eq!(item["user_id"], AttributeValue::S("u-1".to_owned()));
    }

    #[test]
    fn given_missing_sort_key_when_built_should_inject_offset() {
        let mut config = given_default_config();
        config.sort_key_field = Some("event_offset".to_owned());
        let sink = DynamoDbSink::new(1, config);
        let mut message = given_message(given_json_payload(r#"{"name":"first"}"#));

        let item = sink
            .build_item(
                &given_topic_metadata(),
                &given_messages_metadata(),
                &mut message,
            )
            .expect("build item");

        assert_eq!(item["event_offset"], AttributeValue::N("7".to_owned()));
    }

    #[test]
    fn given_invalid_key_type_when_built_should_skip_message() {
        let mut config = given_default_config();
        config.partition_key_field = Some("user_id".to_owned());
        let sink = DynamoDbSink::new(1, config);
        let mut message = given_message(given_json_payload(r#"{"user_id":true}"#));

        let result = sink.build_item(
            &given_topic_metadata(),
            &given_messages_metadata(),
            &mut message,
        );

        assert!(result.is_err());
    }

    #[test]
    fn given_oversized_payload_when_built_should_skip_message() {
        let mut config = given_default_config();
        config.max_item_size = Some(64);
        let sink = DynamoDbSink::new(1, config);
        let mut message = given_message(Payload::Text("x".repeat(1024)));

        let result = sink.build_item(
            &given_topic_metadata(),
            &given_messages_metadata(),
            &mut message,
        );

        assert!(result.is_err());
    }

    #[test]
    fn given_unsupported_schema_payload_when_built_should_skip_message() {
        let sink = DynamoDbSink::new(1, given_default_config());
        let mut message = given_message(Payload::Avro(vec![1, 2, 3]));

        let result = sink.build_item(
            &given_topic_metadata(),
            &given_messages_metadata(),
            &mut message,
        );

        assert!(result.is_err());
    }

    #[test]
    fn given_text_payload_when_built_should_store_payload_attribute() {
        let sink = DynamoDbSink::new(1, given_default_config());
        let mut message = given_message(Payload::Text("hello".to_owned()));

        let item = sink
            .build_item(
                &given_topic_metadata(),
                &given_messages_metadata(),
                &mut message,
            )
            .expect("build item");

        assert_eq!(item[PAYLOAD_FIELD], AttributeValue::S("hello".to_owned()));
    }

    #[test]
    fn given_raw_json_payload_when_built_should_map_attributes() {
        let sink = DynamoDbSink::new(1, given_default_config());
        let mut message = given_message(Payload::Raw(br#"{"name":"first"}"#.to_vec()));

        let item = sink
            .build_item(
                &given_topic_metadata(),
                &given_messages_metadata(),
                &mut message,
            )
            .expect("build item");

        assert_eq!(item["name"], AttributeValue::S("first".to_owned()));
    }

    #[test]
    fn given_raw_binary_payload_when_built_should_store_binary_attribute() {
        let sink = DynamoDbSink::new(1, given_default_config());
        let mut message = given_message(Payload::Raw(vec![0xff, 0xfe, 0xfd]));

        let item = sink
            .build_item(
                &given_topic_metadata(),
                &given_messages_metadata(),
                &mut message,
            )
            .expect("build item");

        assert_eq!(
            item[PAYLOAD_FIELD],
            AttributeValue::B(Blob::new(vec![0xff, 0xfe, 0xfd]))
        );
    }

    #[test]
    fn given_json_array_payload_when_built_should_nest_under_payload_attribute() {
        let sink = DynamoDbSink::new(1, given_default_config());
        let mut message = given_message(given_json_payload(r#"[1,2]"#));

        let item = sink
            .build_item(
                &given_topic_metadata(),
                &given_messages_metadata(),
                &mut message,
            )
            .expect("build item");

        assert_eq!(
            item[PAYLOAD_FIELD],
            AttributeValue::L(vec![
                AttributeValue::N("1".to_owned()),
                AttributeValue::N("2".to_owned())
            ])
        );
    }

    #[test]
    fn given_non_finite_json_number_when_converted_should_become_null() {
        let value = OwnedValue::Static(StaticNode::F64(f64::NAN));
        assert_eq!(json_into_attribute_value(value), AttributeValue::Null(true));
    }

    #[test]
    fn given_same_message_when_built_twice_should_produce_the_same_key() {
        let sink = DynamoDbSink::new(1, given_default_config());
        let topic_metadata = given_topic_metadata();
        let messages_metadata = given_messages_metadata();
        let mut first = given_message(given_json_payload(r#"{"name":"first"}"#));
        let mut second = given_message(given_json_payload(r#"{"name":"first"}"#));

        let first_item = sink
            .build_item(&topic_metadata, &messages_metadata, &mut first)
            .expect("build item");
        let second_item = sink
            .build_item(&topic_metadata, &messages_metadata, &mut second)
            .expect("build item");

        assert_eq!(
            first_item[DEFAULT_PARTITION_KEY_FIELD],
            second_item[DEFAULT_PARTITION_KEY_FIELD]
        );
    }

    #[test]
    fn given_different_topics_when_built_should_produce_different_keys() {
        let messages_metadata = given_messages_metadata();
        let first_topic = given_topic_metadata();
        let second_topic = TopicMetadata {
            stream: "test_stream".to_owned(),
            topic: "other_topic".to_owned(),
        };

        assert_ne!(
            build_message_key(&first_topic, &messages_metadata, 42),
            build_message_key(&second_topic, &messages_metadata, 42)
        );
    }

    #[test]
    fn given_items_sharing_a_key_when_deduplicated_should_keep_the_newest() {
        let mut config = given_default_config();
        config.partition_key_field = Some("user_id".to_owned());
        let sink = DynamoDbSink::new(1, config);
        let items = vec![
            HashMap::from([
                ("user_id".to_owned(), AttributeValue::S("u-1".to_owned())),
                ("name".to_owned(), AttributeValue::S("old".to_owned())),
            ]),
            HashMap::from([
                ("user_id".to_owned(), AttributeValue::S("u-2".to_owned())),
                ("name".to_owned(), AttributeValue::S("other".to_owned())),
            ]),
            HashMap::from([
                ("user_id".to_owned(), AttributeValue::S("u-1".to_owned())),
                ("name".to_owned(), AttributeValue::S("new".to_owned())),
            ]),
        ];

        let (deduplicated, duplicates) = sink.deduplicate_items(items);

        assert_eq!(duplicates, 1);
        assert_eq!(deduplicated.len(), 2);
        assert_eq!(deduplicated[0]["name"], AttributeValue::S("new".to_owned()));
        assert_eq!(
            deduplicated[1]["name"],
            AttributeValue::S("other".to_owned())
        );
    }

    #[test]
    fn given_items_sharing_a_partition_key_when_sort_key_differs_should_keep_both() {
        let mut config = given_default_config();
        config.partition_key_field = Some("user_id".to_owned());
        config.sort_key_field = Some("event_offset".to_owned());
        let sink = DynamoDbSink::new(1, config);
        let items = vec![
            HashMap::from([
                ("user_id".to_owned(), AttributeValue::S("u-1".to_owned())),
                ("event_offset".to_owned(), AttributeValue::N("1".to_owned())),
            ]),
            HashMap::from([
                ("user_id".to_owned(), AttributeValue::S("u-1".to_owned())),
                ("event_offset".to_owned(), AttributeValue::N("2".to_owned())),
            ]),
        ];

        let (deduplicated, duplicates) = sink.deduplicate_items(items);

        assert_eq!(duplicates, 0);
        assert_eq!(deduplicated.len(), 2);
    }

    #[tokio::test]
    async fn given_no_client_when_messages_consumed_should_return_error() {
        let sink = DynamoDbSink::new(1, given_default_config());
        let messages = vec![given_message(given_json_payload(r#"{"name":"first"}"#))];

        let result = sink
            .write_messages(
                &given_topic_metadata(),
                &given_messages_metadata(),
                messages,
            )
            .await;

        assert!(result.is_err());
        assert_eq!(sink.items_written.load(Ordering::Relaxed), 0);
    }

    fn given_key_schema(partition_key: &str, sort_key: Option<&str>) -> Vec<KeySchemaElement> {
        let mut key_schema = vec![
            KeySchemaElement::builder()
                .attribute_name(partition_key)
                .key_type(KeyType::Hash)
                .build()
                .expect("build key schema"),
        ];
        if let Some(sort_key) = sort_key {
            key_schema.push(
                KeySchemaElement::builder()
                    .attribute_name(sort_key)
                    .key_type(KeyType::Range)
                    .build()
                    .expect("build key schema"),
            );
        }
        key_schema
    }

    #[test]
    fn given_matching_key_schema_when_validated_should_accept_the_table() {
        let mut config = given_default_config();
        config.sort_key_field = Some("iggy_offset".to_owned());
        let sink = DynamoDbSink::new(1, config);

        let result = sink.validate_key_schema(given_key_schema(
            DEFAULT_PARTITION_KEY_FIELD,
            Some("iggy_offset"),
        ));

        assert!(result.is_ok());
    }

    #[test]
    fn given_another_partition_key_when_validated_should_reject_the_table() {
        let sink = DynamoDbSink::new(1, given_default_config());

        let result = sink.validate_key_schema(given_key_schema("user_id", None));

        assert!(matches!(result, Err(Error::InvalidConfigValue(_))));
    }

    #[test]
    fn given_table_sort_key_without_configured_field_when_validated_should_reject_the_table() {
        let sink = DynamoDbSink::new(1, given_default_config());

        let result = sink.validate_key_schema(given_key_schema(
            DEFAULT_PARTITION_KEY_FIELD,
            Some("iggy_offset"),
        ));

        assert!(matches!(result, Err(Error::InvalidConfigValue(_))));
    }

    #[test]
    fn given_configured_sort_key_without_table_sort_key_when_validated_should_reject_the_table() {
        let mut config = given_default_config();
        config.sort_key_field = Some("iggy_offset".to_owned());
        let sink = DynamoDbSink::new(1, config);

        let result = sink.validate_key_schema(given_key_schema(DEFAULT_PARTITION_KEY_FIELD, None));

        assert!(matches!(result, Err(Error::InvalidConfigValue(_))));
    }

    #[test]
    fn given_raw_payload_that_is_not_json_when_built_should_store_the_original_bytes() {
        let sink = DynamoDbSink::new(1, given_default_config());
        let raw = br#"{"a":"x\ny"} trailing"#.to_vec();
        let mut message = given_message(Payload::Raw(raw.clone()));

        let item = sink
            .build_item(
                &given_topic_metadata(),
                &given_messages_metadata(),
                &mut message,
            )
            .expect("build item");

        assert_eq!(item[PAYLOAD_FIELD], AttributeValue::B(Blob::new(raw)));
    }

    #[test]
    fn given_service_error_codes_when_checked_should_only_retry_the_transient_ones() {
        assert!(is_transient_code("ThrottlingException"));
        assert!(is_transient_code("ProvisionedThroughputExceededException"));
        assert!(is_transient_code("InternalServerError"));
        assert!(!is_transient_code("ValidationException"));
        assert!(!is_transient_code("ResourceNotFoundException"));
        assert!(!is_transient_code(""));
    }

    #[test]
    fn given_binary_keys_when_signed_should_not_collide() {
        let sink = DynamoDbSink::new(1, given_default_config());
        let first = HashMap::from([(
            DEFAULT_PARTITION_KEY_FIELD.to_owned(),
            AttributeValue::B(Blob::new(vec![0x01, 0x02])),
        )]);
        let second = HashMap::from([(
            DEFAULT_PARTITION_KEY_FIELD.to_owned(),
            AttributeValue::B(Blob::new(vec![0x01, 0x03])),
        )]);

        assert_ne!(sink.key_signature(&first), sink.key_signature(&second));
    }
}
