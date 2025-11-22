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
use base64::{self, Engine};
use decoders::{
    flatbuffer::FlatBufferStreamDecoder, json::JsonStreamDecoder, proto::ProtoStreamDecoder,
    raw::RawStreamDecoder, text::TextStreamDecoder,
};
use encoders::{
    flatbuffer::FlatBufferStreamEncoder, json::JsonStreamEncoder, proto::ProtoStreamEncoder,
    raw::RawStreamEncoder, text::TextStreamEncoder,
};
use iggy::prelude::{HeaderKey, HeaderValue};
use once_cell::sync::OnceCell;
use prost::Message;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use strum_macros::{Display, IntoStaticStr};
use thiserror::Error;
use tokio::runtime::Runtime;

pub mod decoders;
pub mod encoders;
pub mod sink;
pub mod source;
pub mod transforms;

pub use transforms::Transform;

static RUNTIME: OnceCell<Runtime> = OnceCell::new();

pub fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| Runtime::new().expect("Failed to create Tokio runtime"))
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConnectorState(pub Vec<u8>);

/// The Source trait defines the interface for a source connector, responsible for producing the messages to the configured stream and topic.
/// Once the messages are produced (e.g. fetched from an external API), they will be sent further to the specified destination.
#[async_trait]
pub trait Source: Send + Sync {
    /// Invoked when the source is initialized, allowing it to perform any necessary setup.
    async fn open(&mut self) -> Result<(), Error>;

    /// Invoked every time a batch of messages is produced to the configured stream and topic.
    async fn poll(&self) -> Result<ProducedMessages, Error>;

    /// Invoked when the source is closed, allowing it to perform any necessary cleanup.
    async fn close(&mut self) -> Result<(), Error>;
}

/// The Sink trait defines the interface for a sink connector, responsible for consuming the messages from the configured topics.
/// Once the messages are consumed (and optionally transformed before), they should be sent further to the specified destination.
#[async_trait]
pub trait Sink: Send + Sync {
    /// Invoked when the sink is initialized, allowing it to perform any necessary setup.
    async fn open(&mut self) -> Result<(), Error>;

    /// Invoked every time a batch of messages is received from the configured stream(s) and topic(s).
    async fn consume(
        &self,
        topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error>;

    /// Invoked when the sink is closed, allowing it to perform any necessary cleanup.
    async fn close(&mut self) -> Result<(), Error>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Payload {
    Json(simd_json::OwnedValue),
    Raw(Vec<u8>),
    Text(String),
    Proto(String),
    FlatBuffer(Vec<u8>),
}

impl Payload {
    pub fn try_into_vec(self) -> Result<Vec<u8>, Error> {
        match self {
            Payload::Json(value) => {
                Ok(simd_json::to_vec(&value).map_err(|_| Error::InvalidJsonPayload)?)
            }
            Payload::Raw(value) => Ok(value),
            Payload::Text(text) => Ok(text.into_bytes()),
            Payload::Proto(text) => Ok(text.into_bytes()),
            Payload::FlatBuffer(value) => Ok(value),
        }
    }
}

impl std::fmt::Display for Payload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Payload::Json(value) => write!(
                f,
                "Json({})",
                simd_json::to_string_pretty(value).unwrap_or_default()
            ),
            Payload::Raw(value) => write!(f, "Raw({value:#?})"),
            Payload::Text(text) => write!(f, "Text({text})"),
            Payload::Proto(text) => write!(f, "Proto({text})"),
            Payload::FlatBuffer(value) => write!(f, "FlatBuffer({} bytes)", value.len()),
        }
    }
}

#[repr(C)]
#[derive(
    Debug, Default, Copy, Clone, Eq, Hash, PartialEq, Serialize, Deserialize, Display, IntoStaticStr,
)]
#[serde(rename_all = "snake_case")]
pub enum Schema {
    #[default]
    #[strum(to_string = "json")]
    Json,
    #[strum(to_string = "raw")]
    Raw,
    #[strum(to_string = "text")]
    Text,
    #[strum(to_string = "proto")]
    Proto,
    #[strum(to_string = "flatbuffer")]
    FlatBuffer,
}

impl Schema {
    pub fn try_into_payload(self, mut value: Vec<u8>) -> Result<Payload, Error> {
        match self {
            Schema::Json => Ok(Payload::Json(
                simd_json::to_owned_value(&mut value).map_err(|_| Error::InvalidJsonPayload)?,
            )),
            Schema::Raw => Ok(Payload::Raw(value)),
            Schema::Text => Ok(Payload::Text(
                String::from_utf8(value).map_err(|_| Error::InvalidTextPayload)?,
            )),
            Schema::Proto => match prost_types::Any::decode(value.as_slice()) {
                Ok(any) => {
                    let json_value = simd_json::json!({
                        "type_url": any.type_url,
                        "value": base64::engine::general_purpose::STANDARD.encode(&any.value),
                    });
                    Ok(Payload::Json(json_value))
                }
                Err(_) => Ok(Payload::Raw(value)),
            },
            Schema::FlatBuffer => Ok(Payload::FlatBuffer(value)),
        }
    }

    pub fn decoder(self) -> Arc<dyn StreamDecoder> {
        match self {
            Schema::Json => Arc::new(JsonStreamDecoder),
            Schema::Raw => Arc::new(RawStreamDecoder),
            Schema::Text => Arc::new(TextStreamDecoder),
            Schema::Proto => Arc::new(ProtoStreamDecoder::default()),
            Schema::FlatBuffer => Arc::new(FlatBufferStreamDecoder::default()),
        }
    }

    pub fn encoder(self) -> Arc<dyn StreamEncoder> {
        match self {
            Schema::Json => Arc::new(JsonStreamEncoder),
            Schema::Raw => Arc::new(RawStreamEncoder),
            Schema::Text => Arc::new(TextStreamEncoder),
            Schema::Proto => Arc::new(ProtoStreamEncoder::default()),
            Schema::FlatBuffer => Arc::new(FlatBufferStreamEncoder::default()),
        }
    }
}

#[repr(C)]
#[derive(Debug, Serialize, Deserialize)]
pub struct TopicMetadata {
    pub stream: String,
    pub topic: String,
}

#[repr(C)]
#[derive(Debug, Serialize, Deserialize)]
pub struct MessagesMetadata {
    pub partition_id: u32,
    pub current_offset: u64,
    pub schema: Schema,
}

#[repr(C)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReceivedMessage {
    pub id: u128,
    pub offset: u64,
    pub checksum: u64,
    pub timestamp: u64,
    pub origin_timestamp: u64,
    pub headers: Option<HashMap<HeaderKey, HeaderValue>>,
    pub payload: Vec<u8>,
}

#[repr(C)]
#[derive(Debug, Serialize, Deserialize)]
pub struct ProducedMessages {
    pub schema: Schema,
    pub messages: Vec<ProducedMessage>,
    pub state: Option<ConnectorState>,
}

#[repr(C)]
#[derive(Debug, Serialize, Deserialize)]
pub struct ProducedMessage {
    pub id: Option<u128>,
    pub checksum: Option<u64>,
    pub timestamp: Option<u64>,
    pub origin_timestamp: Option<u64>,
    pub headers: Option<HashMap<HeaderKey, HeaderValue>>,
    pub payload: Vec<u8>,
}

#[repr(C)]
#[derive(Debug, Serialize, Deserialize)]
pub struct DecodedMessage {
    pub id: Option<u128>,
    pub offset: Option<u64>,
    pub checksum: Option<u64>,
    pub timestamp: Option<u64>,
    pub origin_timestamp: Option<u64>,
    pub headers: Option<HashMap<HeaderKey, HeaderValue>>,
    pub payload: Payload,
}

#[repr(C)]
#[derive(Debug, Serialize, Deserialize)]
pub struct RawMessages {
    pub schema: Schema,
    pub messages: Vec<RawMessage>,
}

#[repr(C)]
#[derive(Debug, Serialize, Deserialize)]
pub struct RawMessage {
    pub id: u128,
    pub offset: u64,
    pub checksum: u64,
    pub timestamp: u64,
    pub origin_timestamp: u64,
    pub headers: Vec<u8>,
    pub payload: Vec<u8>,
}

#[repr(C)]
#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumedMessage {
    pub id: u128,
    pub offset: u64,
    pub checksum: u64,
    pub timestamp: u64,
    pub origin_timestamp: u64,
    pub headers: Option<HashMap<HeaderKey, HeaderValue>>,
    pub payload: Payload,
}

pub trait StreamDecoder: Send + Sync {
    fn schema(&self) -> Schema;
    fn decode(&self, payload: Vec<u8>) -> Result<Payload, Error>;
}

pub trait StreamEncoder: Send + Sync {
    fn schema(&self) -> Schema;
    fn encode(&self, payload: Payload) -> Result<Vec<u8>, Error>;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Error)]
pub enum Error {
    #[error("Invalid config")]
    InvalidConfig,
    #[error("Invalid record")]
    InvalidRecord,
    #[error("Invalid transformer")]
    InvalidTransformer,
    #[error("HTTP request failed: {0}")]
    HttpRequestFailed(String),
    #[error("Init error: {0}")]
    InitError(String),
    #[error("Invalid payload type")]
    InvalidPayloadType,
    #[error("Invalid JSON payload.")]
    InvalidJsonPayload,
    #[error("Invalid text payload.")]
    InvalidTextPayload,
    #[error("Cannot decode schema {0}")]
    CannotDecode(Schema),
    #[error("Storage error: {0}")]
    Storage(String),
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Invalid protobuf payload.")]
    InvalidProtobufPayload,
    #[error("Cannot open state file")]
    CannotOpenStateFile,
    #[error("Cannot read state file")]
    CannotReadStateFile,
    #[error("Cannot write state file")]
    CannotWriteStateFile,
    #[error("Invalid state")]
    InvalidState,
    #[error("Connection error: {0}")]
    Connection(String),
}
