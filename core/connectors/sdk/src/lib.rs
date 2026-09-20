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
use base64::{self, Engine};
use decoders::{
    avro::AvroStreamDecoder, flatbuffer::FlatBufferStreamDecoder, json::JsonStreamDecoder,
    proto::ProtoStreamDecoder, raw::RawStreamDecoder, text::TextStreamDecoder,
};
use encoders::{
    avro::AvroStreamEncoder, flatbuffer::FlatBufferStreamEncoder, json::JsonStreamEncoder,
    proto::ProtoStreamEncoder, raw::RawStreamEncoder, text::TextStreamEncoder,
};
use iggy::prelude::{HeaderKey, HeaderValue};
use prost::Message;
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};
use strum_macros::{Display, IntoStaticStr};
use thiserror::Error;
use tokio::runtime::Runtime;

#[cfg(feature = "api")]
pub mod api;
pub mod convert;
pub mod decoders;
pub mod encoders;
pub mod log;
pub mod retry;
pub mod sink;
pub mod source;
pub mod transforms;

pub use convert::owned_value_to_serde_json;
pub use log::LogCallback;
pub use transforms::Transform;

#[doc(hidden)]
pub mod connector_macro_support {
    pub use dashmap::DashMap;
}

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

pub fn get_runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| Runtime::new().expect("Failed to create Tokio runtime"))
}

/// Connector state wrapper holding serialized state bytes.
/// The inner bytes are serialized/deserialized using MessagePack via the helper methods.
/// Note: Serialize/Deserialize derives are required for FFI boundary (postcard serialization).
#[derive(Debug, Serialize, Deserialize)]
pub struct ConnectorState(pub Vec<u8>);

impl ConnectorState {
    /// Deserializes the connector state into the specified type using MessagePack.
    /// Returns `None` if deserialization fails, logging the error.
    pub fn deserialize<T: serde::de::DeserializeOwned>(
        self,
        connector_name: &str,
        connector_id: u32,
    ) -> Option<T> {
        rmp_serde::from_slice(&self.0)
            .inspect_err(|error| {
                tracing::warn!(
                    "Failed to deserialize state for {connector_name} connector with ID: {connector_id}. {error}"
                );
            })
            .ok()
    }

    /// Serializes the provided state into a `ConnectorState` using MessagePack.
    /// Returns `None` if serialization fails, logging the error.
    pub fn serialize<T: serde::Serialize>(
        state: &T,
        connector_name: &str,
        connector_id: u32,
    ) -> Option<Self> {
        rmp_serde::to_vec(state)
            .inspect_err(|error| {
                tracing::error!(
                    "Failed to serialize state for {connector_name} connector with ID: {connector_id}. {error}"
                );
            })
            .ok()
            .map(ConnectorState)
    }
}

/// The Source trait defines the interface for a source connector, responsible for producing the messages to the configured stream and topic.
/// Once the messages are produced (e.g. fetched from an external API), they will be sent further to the specified destination.
#[async_trait]
pub trait Source: Send + Sync {
    /// Invoked when the source is initialized, allowing it to perform any necessary setup.
    async fn open(&mut self) -> Result<(), Error>;

    /// Retrieves the next batch for the runtime to process and deliver.
    async fn poll(&self) -> Result<ProducedMessages, Error>;

    /// Invoked after the runtime has finished processing the most recently polled batch.
    ///
    /// Sources that track cursors or perform destructive operations should stage those changes
    /// in [`Source::poll`] and apply them only after receiving [`source::SourceBatchResult::Ack`].
    /// A [`source::SourceBatchResult::Nack`] means the staged changes must be discarded so the
    /// batch can be polled again. The SDK allows only one batch to be in flight at a time and
    /// stops polling if this method returns an error. The default no-op is suitable only for
    /// sources that have no staged cursor changes or destructive work.
    async fn on_batch_result(&self, _result: source::SourceBatchResult) -> Result<(), Error> {
        Ok(())
    }

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
    Avro(Vec<u8>),
}

impl Payload {
    /// The `Schema` describing this payload's variant.
    ///
    /// Not the same thing as `StreamDecoder::schema`, which names the wire
    /// format a decoder reads rather than the variant it hands back: the Avro
    /// and FlatBuffer decoders return `Payload::Json` whenever `extract_as_json`
    /// is set, and the Proto decoder returns `Payload::Json` or `Payload::Raw`
    /// depending on the path it takes. A transform may change the variant again
    /// after that. Anything tagging a payload for transport has to read the tag
    /// off the payload it actually holds.
    pub const fn schema(&self) -> Schema {
        match self {
            Payload::Json(_) => Schema::Json,
            Payload::Raw(_) => Schema::Raw,
            Payload::Text(_) => Schema::Text,
            Payload::Proto(_) => Schema::Proto,
            Payload::FlatBuffer(_) => Schema::FlatBuffer,
            Payload::Avro(_) => Schema::Avro,
        }
    }

    /// Rebuilds a payload from a tag produced by `Payload::schema`.
    ///
    /// The variant-preserving inverse of `Payload::schema`, and not the same
    /// thing as `Schema::try_into_payload`, which reads a tag naming the wire
    /// format a source plugin sent. The two disagree on `Schema::Proto`: it
    /// means protobuf wire bytes to a source and a `Payload::Proto` string
    /// here, so the sink path needs its own inverse to get its variant back.
    pub fn try_from_schema(schema: Schema, mut value: Vec<u8>) -> Result<Self, Error> {
        match schema {
            Schema::Json => Ok(Payload::Json(
                simd_json::to_owned_value(&mut value).map_err(|_| Error::InvalidJsonPayload)?,
            )),
            Schema::Raw => Ok(Payload::Raw(value)),
            Schema::Text => Ok(Payload::Text(
                String::from_utf8(value).map_err(|_| Error::InvalidTextPayload)?,
            )),
            Schema::Proto => Ok(Payload::Proto(
                String::from_utf8(value).map_err(|_| Error::InvalidProtobufPayload)?,
            )),
            Schema::FlatBuffer => Ok(Payload::FlatBuffer(value)),
            Schema::Avro => Ok(Payload::Avro(value)),
        }
    }

    /// The JSON document this payload holds, when it holds one.
    ///
    /// A `Payload::Json` is borrowed as it is. A `Payload::Proto` is parsed,
    /// because `proto_convert` puts the JSON text it was handed there whenever
    /// it has no descriptor or the encode fails, and puts arbitrary text there
    /// on its text and raw paths. The parse can therefore fail by design, and
    /// `None` means the text has to be treated as text. Every other variant is
    /// `None`.
    ///
    /// Proto text is parsed from a copy: simd_json mutates its buffer even when
    /// the parse fails, and the text has to survive for the fallback.
    pub fn json_document(&self) -> Option<Cow<'_, simd_json::OwnedValue>> {
        match self {
            Payload::Json(value) => Some(Cow::Borrowed(value)),
            Payload::Proto(text) => {
                let mut bytes = text.as_bytes().to_vec();
                simd_json::to_owned_value(&mut bytes).ok().map(Cow::Owned)
            }
            _ => None,
        }
    }

    /// Consuming conversion — transfers ownership of inner buffers.
    pub fn try_into_vec(self) -> Result<Vec<u8>, Error> {
        match self {
            Payload::Json(value) => {
                Ok(simd_json::to_vec(&value).map_err(|_| Error::InvalidJsonPayload)?)
            }
            Payload::Raw(value) => Ok(value),
            Payload::Text(text) => Ok(text.into_bytes()),
            Payload::Proto(text) => Ok(text.into_bytes()),
            Payload::FlatBuffer(value) => Ok(value),
            Payload::Avro(value) => Ok(value),
        }
    }

    /// Borrowing serialisation — no clone, no ownership transfer.
    ///
    /// - Json: serialises the `OwnedValue` in place → one allocation
    ///   for the output `Vec<u8>`, zero clone of the value tree.
    /// - Raw: returns a copy of the inner bytes (unavoidable — caller
    ///   needs owned bytes and we only have a reference).
    /// - Text/Proto: copies the string bytes (same reasoning).
    /// - FlatBuffer: copies the buffer bytes.
    ///
    /// For `Json` this replaces a deep clone of the entire `OwnedValue` tree
    /// with a single serialisation pass — O(n) work either way, but the clone
    /// path does O(n) allocation + O(n) serialisation, while this path does
    /// only O(n) serialisation.
    ///
    /// Named `try_to_bytes` (not `try_as_bytes`) because it allocates and
    /// returns an owned `Vec<u8>` — following the Rust API guideline that
    /// `as_` implies a cheap borrowed view while `to_` implies an owned,
    /// potentially-allocating conversion.
    pub fn try_to_bytes(&self) -> Result<Vec<u8>, Error> {
        match self {
            Payload::Json(value) => simd_json::to_vec(value).map_err(|_| Error::InvalidJsonPayload),
            Payload::Raw(value) => Ok(value.clone()),
            Payload::Text(text) => Ok(text.as_bytes().to_vec()),
            Payload::Proto(text) => Ok(text.as_bytes().to_vec()),
            Payload::FlatBuffer(value) => Ok(value.clone()),
            Payload::Avro(value) => Ok(value.clone()),
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
            Payload::Avro(value) => write!(f, "Avro({} bytes)", value.len()),
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
    #[strum(to_string = "avro")]
    Avro,
}

impl Schema {
    /// Rebuilds a payload from a tag naming the wire format its bytes are in.
    ///
    /// Carries the source path, where a plugin sets `ProducedMessages::schema`
    /// to describe the bytes it produced. `Schema::Proto` therefore means
    /// protobuf wire bytes and is read as a `prost_types::Any`. Use
    /// `Payload::try_from_schema` to invert a tag that came from
    /// `Payload::schema` instead.
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
            Schema::Avro => Ok(Payload::Avro(value)),
        }
    }

    pub fn decoder(self) -> Arc<dyn StreamDecoder> {
        match self {
            Schema::Json => Arc::new(JsonStreamDecoder),
            Schema::Raw => Arc::new(RawStreamDecoder),
            Schema::Text => Arc::new(TextStreamDecoder),
            Schema::Proto => Arc::new(ProtoStreamDecoder::default()),
            Schema::FlatBuffer => Arc::new(FlatBufferStreamDecoder::default()),
            Schema::Avro => Arc::new(AvroStreamDecoder::default()),
        }
    }

    pub fn encoder(self) -> Arc<dyn StreamEncoder> {
        match self {
            Schema::Json => Arc::new(JsonStreamEncoder),
            Schema::Raw => Arc::new(RawStreamEncoder),
            Schema::Text => Arc::new(TextStreamEncoder),
            Schema::Proto => Arc::new(ProtoStreamEncoder::default()),
            Schema::FlatBuffer => Arc::new(FlatBufferStreamEncoder::default()),
            Schema::Avro => Arc::new(AvroStreamEncoder::default()),
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
    pub headers: Option<BTreeMap<HeaderKey, HeaderValue>>,
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
    pub headers: Option<BTreeMap<HeaderKey, HeaderValue>>,
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
    pub headers: Option<BTreeMap<HeaderKey, HeaderValue>>,
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
    pub headers: Option<BTreeMap<HeaderKey, HeaderValue>>,
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
    #[error("Invalid config value: {0}")]
    InvalidConfigValue(String),
    #[error("Invalid record")]
    InvalidRecord,
    #[error("Invalid record value: {0}")]
    InvalidRecordValue(String),
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
    #[error("Cannot store data: {0}")]
    CannotStoreData(String),
    /// A non-transient HTTP error (e.g. 400 Bad Request, 422 Unprocessable
    /// Entity) that retrying will not fix. Connectors use this variant to
    /// distinguish permanent data/schema issues from transient connectivity
    /// failures so that circuit breakers are not tripped by bad data.
    #[error("Permanent HTTP error: {0}")]
    PermanentHttpError(String),
    /// The source schema could not be mapped to the destination schema.
    /// Indicates a table definition or configuration problem.
    #[error("Schema mismatch: {0}")]
    SchemaMismatch(String),
    /// An I/O failure while writing data (e.g. Parquet serialization, file
    /// writer close). Distinct from record-level validation errors. May leave
    /// orphaned partial files on the object store; cleanup is caller-side.
    #[error("Write failure: {0}")]
    WriteFailure(String),
    /// In-memory transaction preparation failed (e.g. invalid partition spec,
    /// schema validation). Typically deterministic in the current Iceberg
    /// version; check the underlying Iceberg error to decide retryability.
    #[error("Transaction apply error: {0}")]
    TransactionApplyError(String),
    /// A catalog commit failed. `Transaction::commit()` consumes the
    /// transaction, so retrying requires rebuilding it from new data files.
    /// Retry is not idempotent: callers must verify via the catalog whether
    /// the original commit was applied before retrying, otherwise data may
    /// be duplicated.
    #[error("Catalog commit error: {0}")]
    CatalogCommitError(String),
    /// The state store is temporarily unavailable (5xx, timeout, connect
    /// failure) and bounded retries were exhausted. The operation may succeed
    /// later; the batch-ack path Nacks and the plugin re-polls.
    #[error("Transient state error: {0}")]
    TransientState(String),
    /// The state store rejected the operation in a way retrying cannot fix
    /// (version conflict, revoked authorization, protocol violation). No
    /// write from this process can be expected to succeed again.
    #[error("Permanent state error: {0}")]
    PermanentState(String),
    /// A previous save failed permanently, so the state provider refuses
    /// further saves without touching the network. Fail-fast marker, never
    /// retried.
    #[error("State provider latched after a permanent state error")]
    StateLatched,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_payloads() -> Vec<(Payload, Schema)> {
        vec![
            (Payload::Json(simd_json::json!({"id": 1})), Schema::Json),
            (Payload::Json(simd_json::json!([1, 2, 3])), Schema::Json),
            (Payload::Json(simd_json::json!("scalar")), Schema::Json),
            (Payload::Json(simd_json::json!(null)), Schema::Json),
            (Payload::Raw(vec![1, 2, 3]), Schema::Raw),
            (Payload::Raw(Vec::new()), Schema::Raw),
            (Payload::Text("hello".to_owned()), Schema::Text),
            (Payload::Text(String::new()), Schema::Text),
            (Payload::Proto("proto text".to_owned()), Schema::Proto),
            (Payload::Proto(String::new()), Schema::Proto),
            (Payload::FlatBuffer(vec![4, 5, 6]), Schema::FlatBuffer),
            (Payload::FlatBuffer(Vec::new()), Schema::FlatBuffer),
            (Payload::Avro(vec![7, 8, 9]), Schema::Avro),
            (Payload::Avro(Vec::new()), Schema::Avro),
        ]
    }

    #[test]
    fn given_a_json_payload_when_the_document_is_read_should_borrow_it() {
        let payload = Payload::Json(simd_json::json!({"id": 1}));

        let document = payload
            .json_document()
            .expect("a JSON payload is a document");

        assert!(matches!(document, Cow::Borrowed(_)));
        assert_eq!(*document, simd_json::json!({"id": 1}));
    }

    #[test]
    fn given_proto_text_holding_json_when_the_document_is_read_should_parse_it() {
        let payload = Payload::Proto(r#"{"id": 1, "name": "row-1"}"#.to_owned());

        let document = payload
            .json_document()
            .expect("proto text holding JSON is a document");

        assert!(matches!(document, Cow::Owned(_)));
        assert_eq!(*document, simd_json::json!({"id": 1, "name": "row-1"}));
    }

    #[test]
    fn given_proto_text_that_is_not_json_when_the_document_is_read_should_leave_the_text_intact() {
        let text = r#"binary_data: "AQID""#;
        let payload = Payload::Proto(text.to_owned());

        assert!(payload.json_document().is_none());
        // simd_json overwrites its input buffer on a failed parse, so this
        // only holds because the parse ran on a copy.
        let Payload::Proto(kept) = &payload else {
            panic!("the variant must not change");
        };
        assert_eq!(kept, text);
    }

    #[test]
    fn given_a_payload_that_is_not_json_or_proto_when_the_document_is_read_should_return_none() {
        for payload in [
            Payload::Text(r#"{"id": 1}"#.to_owned()),
            Payload::Raw(br#"{"id": 1}"#.to_vec()),
            Payload::FlatBuffer(vec![1, 2, 3]),
            Payload::Avro(vec![1, 2, 3]),
        ] {
            assert!(
                payload.json_document().is_none(),
                "{payload} must not be read as a document"
            );
        }
    }

    #[test]
    fn given_every_payload_variant_when_schema_is_read_should_name_that_variant() {
        for (payload, expected) in all_payloads() {
            assert_eq!(
                payload.schema(),
                expected,
                "wrong schema for {payload}, expected {expected}"
            );
        }
    }

    #[test]
    fn given_a_payload_when_round_tripped_through_its_own_schema_should_keep_the_variant_and_the_bytes()
     {
        // The JSON rows are small objects, arrays and scalars, so their
        // serialised bytes are stable. A row with many keys would need a value
        // comparison instead, because simd_json objects do not keep key order
        // past their small-map threshold.
        for (payload, schema) in all_payloads() {
            let bytes = payload
                .try_to_bytes()
                .unwrap_or_else(|error| panic!("failed to serialize {schema} payload: {error}"));
            let rebuilt = Payload::try_from_schema(schema, bytes.clone())
                .unwrap_or_else(|error| panic!("failed to rebuild {schema} payload: {error}"));

            assert_eq!(
                rebuilt.schema(),
                schema,
                "round trip through {schema} produced {rebuilt}"
            );
            assert_eq!(
                rebuilt
                    .try_to_bytes()
                    .unwrap_or_else(|error| panic!("failed to serialize {rebuilt}: {error}")),
                bytes,
                "round trip through {schema} changed the payload bytes"
            );
        }
    }

    #[test]
    fn given_proto_text_when_rebuilt_from_a_variant_tag_should_return_the_proto_payload() {
        let payload = Payload::Proto(r#"{"id":1}"#.to_owned());
        let bytes = payload.try_into_vec().expect("failed to serialize");
        let rebuilt = Payload::try_from_schema(Schema::Proto, bytes).expect("failed to rebuild");

        let Payload::Proto(text) = rebuilt else {
            panic!("expected a proto payload, got {rebuilt}");
        };
        assert_eq!(text, r#"{"id":1}"#);
    }

    #[test]
    fn given_non_utf8_bytes_when_rebuilt_as_proto_from_a_variant_tag_should_fail() {
        let error = Payload::try_from_schema(Schema::Proto, vec![0xff, 0xfe])
            .expect_err("non UTF-8 bytes cannot be a proto text payload");

        assert_eq!(error, Error::InvalidProtobufPayload);
    }

    #[test]
    fn given_protobuf_wire_bytes_when_rebuilt_from_a_wire_tag_should_stay_an_any_document() {
        // The source path keeps its own inverse: a plugin tagging
        // `Schema::Proto` really does send protobuf, so those bytes are read as
        // a `prost_types::Any` rather than as text.
        let any = prost_types::Any {
            type_url: "type.googleapis.com/test.Event".to_owned(),
            value: vec![1, 2, 3],
        };
        let rebuilt = Schema::Proto
            .try_into_payload(any.encode_to_vec())
            .expect("failed to rebuild");

        assert_eq!(rebuilt.schema(), Schema::Json);
    }

    #[test]
    fn given_bytes_that_are_not_an_any_when_rebuilt_from_a_wire_tag_should_fall_back_to_raw() {
        // The other half of the source arm. A plugin can tag `Schema::Proto`
        // and send something that is not an `Any`, and those bytes still have
        // to reach the sink rather than being dropped.
        let rebuilt = Schema::Proto
            .try_into_payload(b"not protobuf wire bytes".to_vec())
            .expect("failed to rebuild");

        assert_eq!(rebuilt.schema(), Schema::Raw);
    }
}
