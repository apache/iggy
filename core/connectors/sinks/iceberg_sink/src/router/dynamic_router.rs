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
use crate::router::commit::{is_missing, table_load_error};
use crate::router::redelivery::HeldMessages;
use crate::router::{
    BatchOrigin, Rejection, Router, before, first_rejection, format_error_chain,
    is_valid_namespaced_table, load_table, write_data,
};
use async_trait::async_trait;
use iceberg::Catalog;
use iceberg::table::Table;
use iggy_connector_sdk::retry::RetryPolicy;
use iggy_connector_sdk::{ConsumedMessage, Error, Payload, Schema};
use simd_json::base::ValueAsObject;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use tracing::{info, warn};

#[derive(Debug)]
pub struct DynamicRouter {
    catalog: Box<dyn Catalog>,
    route_field: String,
    invalid_rows: InvalidRows,
    commit_retry: RetryPolicy,
    held: Option<HeldMessages>,
}

impl DynamicRouter {
    pub fn new(
        catalog: Box<dyn Catalog>,
        route_field: String,
        invalid_rows: InvalidRows,
        commit_retry: RetryPolicy,
    ) -> Self {
        Self {
            catalog,
            route_field,
            invalid_rows,
            commit_retry,
            held: None,
        }
    }

    fn extract_route_field(&self, message: &ConsumedMessage) -> Option<String> {
        route_value(&message.payload, &self.route_field)
    }

    /// Drops a message that names no table it can be written to. At least once, it rejects the
    /// message instead, unless `invalid_rows` skips rows, so that the call stops at it.
    fn unroutable(
        &self,
        origin: &BatchOrigin<'_>,
        message: &ConsumedMessage,
        reason: &str,
    ) -> Option<Rejection> {
        if self.held.is_some() && self.invalid_rows == InvalidRows::Fail {
            return Some(Rejection {
                offset: message.offset,
                error: Error::InvalidRecordValue(format!(
                    "The message at offset {} ({origin}) cannot be routed: {reason}",
                    message.offset
                )),
            });
        }
        warn!(
            "Dropping the message at offset {} ({origin}): {reason}",
            message.offset
        );
        None
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
        // Route values that name no table, with the reason, so that a batch loads each once.
        let mut unroutable: HashMap<String, String> = HashMap::new();
        let mut rejection = None;
        for mut message in messages {
            // Normalised once here so the routing read and the later write both
            // work off the document, rather than parsing the same proto text twice.
            message.payload = message.payload.into_json_document();
            let Some(route_field_val) = self.extract_route_field(&message) else {
                let reason = format!(
                    "it is not a JSON object with the route field: {}",
                    self.route_field
                );
                rejection = self.unroutable(origin, &message, &reason);
                if rejection.is_some() {
                    break;
                }
                continue;
            };
            if let Some(reason) = unroutable.get(&route_field_val) {
                rejection = self.unroutable(origin, &message, reason);
                if rejection.is_some() {
                    break;
                }
                continue;
            }

            let entry = match tables.entry(route_field_val) {
                Entry::Occupied(mut entry) => {
                    entry.get_mut().1.push(message);
                    continue;
                }
                Entry::Vacant(entry) => entry,
            };
            let route_field_val = entry.key();

            let reason = if is_valid_namespaced_table(route_field_val) {
                match load_table(route_field_val, self.catalog.as_ref()).await {
                    Ok(table) => {
                        entry.insert((table, vec![message]));
                        continue;
                    }
                    Err(error) if is_missing(&error) => format!(
                        "table: {route_field_val} does not exist in the catalog: {}",
                        format_error_chain(&error)
                    ),
                    // An unreachable catalog or a refused load says nothing about the message,
                    // so at least once the batch fails without skipping it.
                    Err(error) if self.held.is_some() => {
                        return Err(table_load_error(route_field_val, &error));
                    }
                    Err(error) => format!(
                        "table: {route_field_val} does not load from the catalog: {}",
                        format_error_chain(&error)
                    ),
                }
            } else {
                format!(
                    "its route field value: {route_field_val} names no table. It needs at least one namespace before the table, separated by '.'"
                )
            };
            rejection = self.unroutable(origin, &message, &reason);
            if rejection.is_some() {
                break;
            }
            unroutable.insert(entry.into_key(), reason);
        }

        for (table_name, (table, batch_messages)) in tables {
            // Once a message is rejected, the tables still need every message before it.
            let messages = before(&batch_messages, rejection.as_ref());
            let written = write_data(
                origin,
                messages,
                &table,
                Some(&table),
                self.catalog.as_ref(),
                messages_schema,
                self.invalid_rows,
                self.commit_retry,
                self.held.as_ref(),
            )
            .await?;
            info!(
                "Dynamically routed {} input rows to iceberg table {table_name}, committed {}",
                messages.len(),
                written.committed_rows
            );
            rejection = first_rejection(rejection, written.rejection);
        }

        match rejection {
            Some(rejection) => Err(rejection.error),
            None => Ok(()),
        }
    }

    fn enable_at_least_once(&mut self) -> Result<(), Error> {
        self.held = Some(HeldMessages::default());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{NestedField, PrimitiveType, Schema as IcebergSchema, Type};
    use iceberg::{CatalogBuilder, NamespaceIdent, TableCreation, TableIdent};
    use std::time::Duration;

    const ORIGIN: BatchOrigin<'static> = BatchOrigin {
        connector_id: 1,
        stream: "stream",
        topic: "topic",
        partition_id: 0,
    };

    async fn router(invalid_rows: InvalidRows, at_least_once: bool) -> DynamicRouter {
        let catalog = MemoryCatalogBuilder::default()
            .load(
                "memory",
                HashMap::from([(
                    MEMORY_CATALOG_WAREHOUSE.to_owned(),
                    "memory://warehouse".to_owned(),
                )]),
            )
            .await
            .expect("Failed to create catalog");
        let namespace = NamespaceIdent::new("test".to_owned());
        catalog
            .create_namespace(&namespace, HashMap::new())
            .await
            .expect("Failed to create namespace");
        let schema = IcebergSchema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
            ])
            .build()
            .expect("Failed to build schema");
        catalog
            .create_table(
                &namespace,
                TableCreation::builder()
                    .name("events".to_owned())
                    .schema(schema)
                    .build(),
            )
            .await
            .expect("Failed to create table");
        let retry = RetryPolicy {
            max_attempts: 1,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(1),
        };
        let mut router =
            DynamicRouter::new(Box::new(catalog), "table".to_owned(), invalid_rows, retry);
        if at_least_once {
            router
                .enable_at_least_once()
                .expect("the dynamic router supports at least once");
        }
        router
    }

    fn message(payload: simd_json::OwnedValue) -> ConsumedMessage {
        ConsumedMessage {
            id: 1,
            offset: 7,
            checksum: 0,
            timestamp: 0,
            origin_timestamp: 0,
            headers: None,
            payload: Payload::Json(payload),
        }
    }

    /// A message without the route field, one whose value names no namespace, and one routed to
    /// a table the catalog does not hold.
    fn unroutable_messages() -> [ConsumedMessage; 3] {
        [
            message(simd_json::json!({ "id": 1 })),
            message(simd_json::json!({ "table": 5 })),
            message(simd_json::json!({ "table": "test.missing" })),
        ]
    }

    #[tokio::test]
    async fn given_at_least_once_when_a_message_cannot_be_routed_should_fail_permanently() {
        let router = router(InvalidRows::Fail, true).await;
        for message in unroutable_messages() {
            let error = router
                .route_data(&ORIGIN, Schema::Json, vec![message])
                .await
                .expect_err("An unroutable message should fail the batch");

            assert!(error.is_permanent(), "{error}");
            assert!(error.to_string().contains("offset 7"), "{error}");
        }
    }

    #[tokio::test]
    async fn given_invalid_rows_skip_when_a_message_cannot_be_routed_should_drop_it() {
        for at_least_once in [true, false] {
            let router = router(InvalidRows::Skip, at_least_once).await;

            router
                .route_data(&ORIGIN, Schema::Json, unroutable_messages().into())
                .await
                .expect("Skipping should drop the unroutable messages");
        }
    }

    #[tokio::test]
    async fn given_at_most_once_when_a_message_cannot_be_routed_should_drop_it() {
        let router = router(InvalidRows::Fail, false).await;

        router
            .route_data(&ORIGIN, Schema::Json, unroutable_messages().into())
            .await
            .expect("At most once, an unroutable message is dropped");
    }

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

    #[tokio::test]
    async fn given_at_least_once_when_a_message_cannot_be_routed_should_commit_the_messages_before_it()
     {
        let router = router(InvalidRows::Fail, true).await;
        let routed = ConsumedMessage {
            offset: 6,
            ..message(simd_json::json!({ "table": "test.events", "id": 6 }))
        };
        let unroutable = message(simd_json::json!({ "id": 7 }));

        let error = router
            .route_data(&ORIGIN, Schema::Json, vec![routed, unroutable])
            .await
            .expect_err("The unroutable message should stop the batch");

        assert!(error.to_string().contains("offset 7"), "{error}");
        let table = router
            .catalog
            .load_table(&TableIdent::from_strs(["test", "events"]).expect("a valid table name"))
            .await
            .expect("Failed to load table");
        let snapshot = table
            .metadata()
            .current_snapshot()
            .expect("The message before the unroutable one should be committed");
        assert_eq!(
            snapshot
                .summary()
                .additional_properties
                .get("added-records"),
            Some(&"1".to_owned())
        );
    }

    #[tokio::test]
    async fn given_a_catalog_that_refuses_the_load_when_skipping_rows_should_fail_without_skipping()
    {
        let catalog =
            crate::router::commit::tests::rest_catalog_answering(http::StatusCode::FORBIDDEN).await;
        let mut router = DynamicRouter::new(
            Box::new(catalog),
            "table".to_owned(),
            InvalidRows::Skip,
            RetryPolicy {
                max_attempts: 1,
                base_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
            },
        );
        router
            .enable_at_least_once()
            .expect("the dynamic router supports at least once");
        let routed = message(simd_json::json!({ "table": "test.events", "id": 7 }));

        let error = router
            .route_data(&ORIGIN, Schema::Json, vec![routed])
            .await
            .expect_err("A refused load says nothing about the message, so it is not skipped");

        assert!(error.is_permanent(), "{error}");
        assert!(matches!(error, Error::InvalidConfigValue(_)), "{error}");
    }
}
