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

use crate::DeltaSink;
use crate::SinkState;
use crate::coercions::{coerce, create_coercion_tree};
use crate::storage::build_storage_options;
use async_trait::async_trait;
use deltalake::writer::{DeltaWriter, JsonWriter};
use iggy_connector_sdk::{ConsumedMessage, Error, MessagesMetadata, Payload, Sink, TopicMetadata};
use tracing::{debug, error, info};

#[async_trait]
impl Sink for DeltaSink {
    async fn open(&mut self) -> Result<(), Error> {
        info!(
            "Opening Delta Lake sink connector with ID: {} for table: {}",
            self.id, self.config.table_uri
        );

        let table_url = url::Url::parse(&self.config.table_uri).map_err(|e| {
            error!("Failed to parse table URI '{}': {e}", self.config.table_uri);
            Error::InitError(format!("Invalid table URI: {e}"))
        })?;

        info!("Parsed table URI: {}", table_url);

        let storage_options = build_storage_options(&self.config).map_err(|e| {
            error!("Invalid storage configuration: {e}");
            Error::InitError(format!("Invalid storage configuration: {e}"))
        })?;

        let table =
            match deltalake::open_table_with_storage_options(table_url, storage_options).await {
                Ok(table) => table,
                Err(e) => {
                    error!("Failed to load Delta table: {e}");
                    return Err(Error::InitError(format!("Failed to load Delta table: {e}")));
                }
            };

        let kernel_schema = table
            .snapshot()
            .map_err(|e| {
                error!("Failed to get table snapshot: {e}");
                Error::InitError(format!("Failed to get table snapshot: {e}"))
            })?
            .schema();
        let coercion_tree = create_coercion_tree(&kernel_schema);

        let writer = JsonWriter::for_table(&table).map_err(|e| {
            error!("Failed to create JsonWriter: {e}");
            Error::InitError(format!("Failed to create JsonWriter: {e}"))
        })?;

        *self.state.lock().await = Some(SinkState {
            table,
            writer,
            coercion_tree,
        });

        info!(
            "Delta Lake sink connector with ID: {} opened successfully.",
            self.id
        );
        Ok(())
    }

    async fn consume(
        &self,
        _topic_metadata: &TopicMetadata,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), Error> {
        debug!(
            "Delta sink with ID: {} received: {} messages, partition: {}, offset: {}",
            self.id,
            messages.len(),
            messages_metadata.partition_id,
            messages_metadata.current_offset,
        );

        // Extract JSON values from consumed messages
        let mut json_values: Vec<serde_json::Value> = Vec::with_capacity(messages.len());
        for msg in &messages {
            match &msg.payload {
                Payload::Json(simd_value) => match owned_value_to_serde_json(simd_value) {
                    Some(v) => json_values.push(v),
                    None => continue,
                },
                other => {
                    error!(
                        "Unsupported payload type: {other}. Delta sink only supports JSON payloads."
                    );
                    return Err(Error::InvalidPayloadType);
                }
            }
        }

        if json_values.is_empty() {
            debug!("No JSON values to write");
            return Ok(());
        }

        let mut state_guard = self.state.lock().await;
        let state = state_guard.as_mut().ok_or_else(|| {
            error!("Delta sink state not initialized — was open() called?");
            Error::InvalidState
        })?;

        // Apply coercions to match Delta table schema
        for value in &mut json_values {
            coerce(value, &state.coercion_tree);
        }

        // Write JSON values to internal Parquet buffers
        if let Err(e) = state.writer.write(json_values).await {
            state.writer.reset();
            error!("Failed to write to Delta writer: {e}");
            return Err(Error::Storage(format!(
                "Failed to write to Delta writer: {e}"
            )));
        }

        // Flush buffers to object store and commit to Delta log
        let version = match state.writer.flush_and_commit(&mut state.table).await {
            Ok(v) => v,
            Err(e) => {
                state.writer.reset();
                error!("Failed to flush and commit to Delta table: {e}");
                return Err(Error::Storage(format!("Failed to flush and commit: {e}")));
            }
        };

        debug!(
            "Delta sink with ID: {} committed version {}",
            self.id, version
        );

        Ok(())
    }

    async fn close(&mut self) -> Result<(), Error> {
        if let Some(mut state) = self.state.lock().await.take()
            && let Err(e) = state.writer.flush_and_commit(&mut state.table).await
        {
            error!(
                "Delta sink with ID: {} failed to flush on close: {e}",
                self.id
            );
            return Err(Error::Storage(format!("Failed to flush on close: {e}")));
        }
        info!("Delta Lake sink connector with ID: {} is closed.", self.id);
        Ok(())
    }
}

// TODO: Similar logic exists in the Elasticsearch sink.
// Refactor to use a common utility when it is implemented.
fn owned_value_to_serde_json(value: &simd_json::OwnedValue) -> Option<serde_json::Value> {
    match value {
        simd_json::OwnedValue::Static(s) => match s {
            simd_json::StaticNode::Null => Some(serde_json::Value::Null),
            simd_json::StaticNode::Bool(b) => Some(serde_json::Value::Bool(*b)),
            simd_json::StaticNode::I64(n) => Some(serde_json::Value::Number((*n).into())),
            simd_json::StaticNode::U64(n) => Some(serde_json::Value::Number((*n).into())),
            simd_json::StaticNode::F64(n) => match serde_json::Number::from_f64(*n) {
                Some(num) => Some(serde_json::Value::Number(num)),
                None => {
                    tracing::warn!("Dropping message containing non-finite float value: {n}");
                    None
                }
            },
        },
        simd_json::OwnedValue::String(s) => Some(serde_json::Value::String(s.to_string())),
        simd_json::OwnedValue::Array(arr) => {
            let converted: Option<Vec<_>> = arr.iter().map(owned_value_to_serde_json).collect();
            converted.map(serde_json::Value::Array)
        }
        simd_json::OwnedValue::Object(obj) => {
            let map: Option<serde_json::Map<String, serde_json::Value>> = obj
                .iter()
                .map(|(k, v)| owned_value_to_serde_json(v).map(|v| (k.to_string(), v)))
                .collect();
            map.map(serde_json::Value::Object)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use simd_json::{OwnedValue, StaticNode};

    #[test]
    fn test_null() {
        assert_eq!(
            owned_value_to_serde_json(&OwnedValue::Static(StaticNode::Null)),
            Some(serde_json::Value::Null)
        );
    }

    #[test]
    fn test_bool() {
        assert_eq!(
            owned_value_to_serde_json(&OwnedValue::Static(StaticNode::Bool(true))),
            Some(serde_json::Value::Bool(true))
        );
        assert_eq!(
            owned_value_to_serde_json(&OwnedValue::Static(StaticNode::Bool(false))),
            Some(serde_json::Value::Bool(false))
        );
    }

    #[test]
    fn test_i64() {
        assert_eq!(
            owned_value_to_serde_json(&OwnedValue::Static(StaticNode::I64(-42))),
            Some(serde_json::json!(-42))
        );
    }

    #[test]
    fn test_u64() {
        assert_eq!(
            owned_value_to_serde_json(&OwnedValue::Static(StaticNode::U64(100))),
            Some(serde_json::json!(100))
        );
    }

    #[test]
    fn test_f64_finite() {
        assert_eq!(
            owned_value_to_serde_json(&OwnedValue::Static(StaticNode::F64(1.5))),
            Some(serde_json::json!(1.5))
        );
    }

    #[test]
    fn test_f64_nan_drops_message() {
        assert!(
            owned_value_to_serde_json(&OwnedValue::Static(StaticNode::F64(f64::NAN))).is_none()
        );
    }

    #[test]
    fn test_f64_infinity_drops_message() {
        assert!(
            owned_value_to_serde_json(&OwnedValue::Static(StaticNode::F64(f64::INFINITY)))
                .is_none()
        );
        assert!(
            owned_value_to_serde_json(&OwnedValue::Static(StaticNode::F64(f64::NEG_INFINITY)))
                .is_none()
        );
    }

    #[test]
    fn test_string() {
        assert_eq!(
            owned_value_to_serde_json(&OwnedValue::String("hello".into())),
            Some(serde_json::Value::String("hello".to_string()))
        );
    }

    #[test]
    fn test_array() {
        let input = OwnedValue::Array(Box::new(vec![
            OwnedValue::Static(StaticNode::Null),
            OwnedValue::Static(StaticNode::Bool(true)),
        ]));
        assert_eq!(
            owned_value_to_serde_json(&input),
            Some(serde_json::json!([null, true]))
        );
    }

    #[test]
    fn test_object() {
        let mut obj = simd_json::owned::Object::new();
        obj.insert("k".into(), OwnedValue::Static(StaticNode::I64(1)));
        let input = OwnedValue::Object(Box::new(obj));
        assert_eq!(
            owned_value_to_serde_json(&input),
            Some(serde_json::json!({"k": 1}))
        );
    }

    #[test]
    fn test_nested_object() {
        let mut inner = simd_json::owned::Object::new();
        inner.insert("b".into(), OwnedValue::String("v".into()));
        let mut outer = simd_json::owned::Object::new();
        outer.insert("a".into(), OwnedValue::Object(Box::new(inner)));
        let input = OwnedValue::Object(Box::new(outer));
        assert_eq!(
            owned_value_to_serde_json(&input),
            Some(serde_json::json!({"a": {"b": "v"}}))
        );
    }
}
