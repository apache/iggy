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

use crate::router::{Router, is_valid_namespaced_table, table_exists, write_data};
use async_trait::async_trait;
use iceberg::Catalog;
use iceberg::table::Table;
use iggy_connector_sdk::{ConsumedMessage, Error, MessagesMetadata, Payload};
use simd_json::base::ValueAsObject;
use std::collections::HashMap;
use tracing::{info, warn};

#[derive(Debug)]
pub struct DynamicRouter {
    catalog: Box<dyn Catalog>,
    route_field: String,
}

pub struct DynamicWriter {
    pub tables_to_write: HashMap<String, Table>,
    pub table_to_message: HashMap<String, Vec<ConsumedMessage>>,
}

impl DynamicWriter {
    pub fn new() -> Self {
        let tables_to_write = HashMap::new();
        let table_to_message = HashMap::new();
        Self {
            tables_to_write,
            table_to_message,
        }
    }

    fn push_to_existing(
        &mut self,
        route_field_val: &str,
        message: ConsumedMessage,
    ) -> Option<ConsumedMessage> {
        if let Some(message_vec) = self.table_to_message.get_mut(route_field_val) {
            message_vec.push(message);
            None
        } else {
            Some(message)
        }
    }

    async fn load_table_if_exists(
        &mut self,
        route_field_val: &str,
        catalog: &dyn Catalog,
    ) -> Result<(), Error> {
        let table = table_exists(route_field_val, catalog)
            .await
            .ok_or(Error::InvalidState)?;

        self.tables_to_write
            .insert(route_field_val.to_string(), table);

        Ok(())
    }
}

impl DynamicRouter {
    pub fn new(catalog: Box<dyn Catalog>, route_field: String) -> Self {
        Self {
            catalog,
            route_field,
        }
    }

    fn extract_route_field(&self, message: &ConsumedMessage) -> Option<String> {
        route_value(&message.payload, &self.route_field)
    }
}

/// The routing value a payload carries under `route_field`, read from the JSON
/// document it holds. Proto text holding JSON is the descriptor-less
/// `proto_convert` fallback and routes like the document it holds; a payload
/// with no document does not route.
fn route_value(payload: &Payload, route_field: &str) -> Option<String> {
    let Some(document) = payload.json_document() else {
        warn!("Unsupported format for iceberg connector");
        return None;
    };
    document
        .as_object()
        .and_then(|obj| obj.get(route_field))
        .map(|val| val.to_string())
}

#[async_trait]
impl Router for DynamicRouter {
    async fn route_data(
        &self,
        messages_metadata: MessagesMetadata,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), crate::Error> {
        let mut writer = DynamicWriter::new();
        for message in messages {
            let route_field_val = match self.extract_route_field(&message) {
                Some(val) => val,
                None => continue,
            };

            let message = match writer.push_to_existing(&route_field_val, message) {
                Some(msg) => msg,
                None => continue,
            };

            if !is_valid_namespaced_table(&route_field_val) {
                warn!(
                    "Found invalid route field name on message: {route_field_val}. Route fields should have at least 1 namespace separated by '.' character before the table"
                );
                continue;
            }

            let route_field_val_cloned = route_field_val.clone();

            if writer
                .load_table_if_exists(&route_field_val_cloned, self.catalog.as_ref())
                .await
                .is_ok()
            {
                if let Some(msgs) = writer.table_to_message.get_mut(&route_field_val_cloned) {
                    msgs.push(message);
                } else {
                    let message_vec: Vec<ConsumedMessage> = vec![message];
                    writer
                        .table_to_message
                        .insert(route_field_val_cloned, message_vec);
                }
            }
        }

        for (table_name, table_obj) in &writer.tables_to_write {
            let batch_messages = match writer.table_to_message.remove(table_name) {
                Some(m) => m,
                None => continue,
            };

            let data: Vec<Payload> = batch_messages.into_iter().map(|m| m.payload).collect();

            write_data(
                &data,
                table_obj,
                self.catalog.as_ref(),
                messages_metadata.schema,
            )
            .await?;
            info!(
                "Dynamically routed {} messages to {} iceberg table",
                data.len(),
                table_name
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::route_value;
    use iggy_connector_sdk::Payload;

    #[test]
    fn given_proto_text_holding_json_should_route_by_the_route_field() {
        let payload = Payload::Proto(r#"{"id": 1, "region": "eu"}"#.to_owned());

        assert_eq!(route_value(&payload, "region").as_deref(), Some("eu"));
    }

    #[test]
    fn given_proto_text_that_is_not_json_should_not_route() {
        let payload = Payload::Proto("region: \"eu\"".to_owned());

        assert_eq!(route_value(&payload, "region"), None);
    }

    #[test]
    fn given_a_json_payload_without_the_route_field_should_not_route() {
        let payload = Payload::Json(simd_json::json!({ "id": 1 }));

        assert_eq!(route_value(&payload, "region"), None);
    }
}
