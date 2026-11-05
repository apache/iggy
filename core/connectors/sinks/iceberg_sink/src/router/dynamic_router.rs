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

use crate::InvalidRows;
use crate::router::{BatchOrigin, Router, is_valid_namespaced_table, table_exists, write_data};
use async_trait::async_trait;
use iceberg::Catalog;
use iceberg::table::Table;
use iggy_connector_sdk::{ConsumedMessage, Payload, Schema};
use simd_json::base::ValueAsObject;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use tracing::{info, warn};

#[derive(Debug)]
pub struct DynamicRouter {
    catalog: Box<dyn Catalog>,
    route_field: String,
    invalid_rows: InvalidRows,
}

impl DynamicRouter {
    pub fn new(catalog: Box<dyn Catalog>, route_field: String, invalid_rows: InvalidRows) -> Self {
        Self {
            catalog,
            route_field,
            invalid_rows,
        }
    }

    fn extract_route_field(&self, message: &ConsumedMessage) -> Option<String> {
        route_value(&message.payload, &self.route_field)
    }
}

/// The routing value a payload carries under `route_field`, read from the JSON
/// document it holds. `route_data` normalises the payload first, so proto text
/// holding JSON arrives here as `Payload::Json`; proto text that is not JSON
/// holds no document and does not route, and neither does any other variant.
fn route_value(payload: &Payload, route_field: &str) -> Option<String> {
    let document = payload.json_document()?;
    document
        .as_object()
        .and_then(|obj| obj.get(route_field))
        .map(|val| val.to_string())
}

#[async_trait]
impl Router for DynamicRouter {
    async fn route_data(
        &self,
        origin: &BatchOrigin<'_>,
        messages_schema: Schema,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), crate::Error> {
        let mut tables: HashMap<String, (Table, Vec<ConsumedMessage>)> = HashMap::new();
        for mut message in messages {
            // Normalised once here so the routing read and the later write both
            // work off the document, rather than parsing the same proto text twice.
            message.payload = message.payload.into_json_document();
            let Some(route_field_val) = self.extract_route_field(&message) else {
                warn!(
                    "Dropping the message at offset {}: it is not a JSON object with the route field: {}",
                    message.offset, self.route_field
                );
                continue;
            };

            let entry = match tables.entry(route_field_val) {
                Entry::Occupied(mut entry) => {
                    entry.get_mut().1.push(message);
                    continue;
                }
                Entry::Vacant(entry) => entry,
            };
            let route_field_val = entry.key();

            if !is_valid_namespaced_table(route_field_val) {
                warn!(
                    "Dropping the message at offset {}: its route field value: {route_field_val} names no table. It needs at least one namespace before the table, separated by '.'",
                    message.offset
                );
                continue;
            }

            let Some(table) = table_exists(route_field_val, self.catalog.as_ref()).await else {
                warn!(
                    "Dropping the message at offset {}: table: {route_field_val} does not load from the catalog",
                    message.offset
                );
                continue;
            };
            entry.insert((table, vec![message]));
        }

        for (table_name, (table, batch_messages)) in tables {
            let committed_rows = write_data(
                origin,
                &batch_messages,
                &table,
                self.catalog.as_ref(),
                messages_schema,
                self.invalid_rows,
            )
            .await?;
            info!(
                "Dynamically routed {} input rows to iceberg table {table_name}, committed {committed_rows}",
                batch_messages.len()
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
