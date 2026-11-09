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
use crate::router::{
    Appender, BatchOrigin, Rejection, Router, before, first_rejection, format_error_chain,
    is_valid_namespaced_table, load_table,
};
use async_trait::async_trait;
use iceberg::table::Table;
use iggy_connector_sdk::{ConsumedMessage, Error, FlushPartition, Payload, Schema, TopicMetadata};
use simd_json::base::ValueAsObject;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use tracing::{info, warn};

#[derive(Debug)]
pub struct DynamicRouter {
    route_field: String,
    appender: Appender,
}

/// Where the messages carrying one route value go.
enum Destination {
    /// The table at this index of the batch's tables.
    Table(usize),
    /// No table, for this reason.
    Unroutable(String),
}

impl DynamicRouter {
    pub(crate) fn new(appender: Appender, route_field: String) -> Self {
        Self {
            route_field,
            appender,
        }
    }

    fn extract_route_field(&self, message: &ConsumedMessage) -> Option<String> {
        route_value(&message.payload, &self.route_field)
    }

    /// Where the messages carrying `route_field_val` go. Route values that load the same table
    /// share its entry in `tables`, so that each table gets its messages in one ascending run,
    /// which is what tells the messages it holds from the ones it does not.
    async fn destination(
        &self,
        route_field_val: &str,
        tables: &mut Vec<(Table, Vec<ConsumedMessage>)>,
    ) -> Result<Destination, Error> {
        if !is_valid_namespaced_table(route_field_val) {
            return Ok(Destination::Unroutable(format!(
                "its route field value: {route_field_val} names no table. It needs at least one namespace before the table, separated by '.'"
            )));
        }
        let table = match load_table(route_field_val, self.appender.catalog.as_ref()).await {
            Ok(table) => table,
            Err(error) if is_missing(&error) => {
                return Ok(Destination::Unroutable(format!(
                    "table: {route_field_val} does not exist in the catalog: {}",
                    format_error_chain(&error)
                )));
            }
            // An unreachable catalog or a refused load says nothing about the message, so at
            // least once the batch fails without skipping it.
            Err(error) if self.appender.held.is_some() => {
                return Err(table_load_error(route_field_val, &error));
            }
            Err(error) => {
                return Ok(Destination::Unroutable(format!(
                    "table: {route_field_val} does not load from the catalog: {}",
                    format_error_chain(&error)
                )));
            }
        };
        let uuid = table.metadata().uuid();
        let index = match tables
            .iter()
            .position(|(loaded, _)| loaded.metadata().uuid() == uuid)
        {
            Some(index) => index,
            None => {
                tables.push((table, Vec::new()));
                tables.len() - 1
            }
        };
        Ok(Destination::Table(index))
    }

    /// Drops a message that names no table it can be written to. At least once, it rejects the
    /// message instead, unless `invalid_rows` skips rows, so that the call stops at it.
    fn unroutable(
        &self,
        origin: &BatchOrigin<'_>,
        message: &ConsumedMessage,
        reason: &str,
    ) -> Option<Rejection> {
        if self.appender.held.is_some() && self.appender.invalid_rows == InvalidRows::Fail {
            return Some(Rejection {
                offset: message.offset,
                error: Error::InvalidRecordValue(format!(
                    "The message at offset: {} ({origin}) cannot be routed: {reason}",
                    message.offset
                )),
            });
        }
        warn!(
            "Dropping the message at offset: {} ({origin}): {reason}",
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
        let mut tables: Vec<(Table, Vec<ConsumedMessage>)> = Vec::new();
        // Where each route value leads, so that a batch loads each table once.
        let mut destinations: HashMap<String, Destination> = HashMap::new();
        let without_route_field = Destination::Unroutable(format!(
            "it is not a JSON object with the route field: {}",
            self.route_field
        ));
        let mut rejection = None;
        for mut message in messages {
            // Normalised once here so the routing read and the later write both
            // work off the document, rather than parsing the same proto text twice.
            message.payload = message.payload.into_json_document();
            let destination: &Destination = match self.extract_route_field(&message) {
                Some(route_field_val) => match destinations.entry(route_field_val) {
                    Entry::Occupied(entry) => entry.into_mut(),
                    Entry::Vacant(entry) => {
                        let destination = self.destination(entry.key(), &mut tables).await?;
                        entry.insert(destination)
                    }
                },
                None => &without_route_field,
            };
            match destination {
                Destination::Table(index) => tables[*index].1.push(message),
                Destination::Unroutable(reason) => {
                    rejection = self.unroutable(origin, &message, reason);
                    if rejection.is_some() {
                        break;
                    }
                }
            }
        }

        for (table, table_messages) in &tables {
            // Once a message is rejected, the tables still need every message before it.
            let messages = before(table_messages, rejection.as_ref());
            let written = self
                .appender
                .append(origin, table, true, messages_schema, messages)
                .await?;
            info!(
                "Dynamically routed {} input rows to iceberg table {}, committed {}",
                messages.len(),
                table.identifier(),
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
        self.appender.enable_at_least_once();
        Ok(())
    }

    fn flushed(&self, topic_metadata: &TopicMetadata, partitions: &[FlushPartition]) {
        self.appender.flushed(topic_metadata, partitions);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::router::commit::tests::{
        SINGLE_ATTEMPT, catalog_with_table, rest_catalog_answering,
    };
    use iceberg::TableIdent;

    const ORIGIN: BatchOrigin<'static> = BatchOrigin {
        connector_id: 1,
        stream: "stream",
        topic: "topic",
        partition_id: 0,
    };

    async fn router(invalid_rows: InvalidRows, at_least_once: bool) -> DynamicRouter {
        let (catalog, _) = catalog_with_table(HashMap::new()).await;
        let mut router = DynamicRouter::new(
            Appender::new(Box::new(catalog), invalid_rows, SINGLE_ATTEMPT),
            "table".to_owned(),
        );
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
            assert!(error.to_string().contains("offset: 7"), "{error}");
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

    async fn events(router: &DynamicRouter) -> Table {
        router
            .appender
            .catalog
            .load_table(&TableIdent::from_strs(["test", "events"]).expect("a valid table name"))
            .await
            .expect("Failed to load table")
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

        assert!(error.to_string().contains("offset: 7"), "{error}");
        let table = events(&router).await;
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
    async fn given_route_values_that_load_one_table_should_append_their_messages_together() {
        let router = router(InvalidRows::Fail, true).await;
        let table = events(&router).await;
        let alias = TableIdent::from_strs(["test", "alias"]).expect("a valid table name");
        router
            .appender
            .catalog
            .register_table(
                &alias,
                table
                    .metadata_location()
                    .expect("A created table has a metadata location")
                    .to_owned(),
            )
            .await
            .expect("Failed to register the alias");
        let batch = (0..4)
            .map(|offset| ConsumedMessage {
                id: u128::from(offset),
                offset,
                ..message(simd_json::json!({
                    "table": if offset % 2 == 0 { "test.events" } else { "test.alias" },
                    "id": offset,
                }))
            })
            .collect();

        router
            .route_data(&ORIGIN, Schema::Json, batch)
            .await
            .expect("Both route values should load the table");

        let table = events(&router).await;
        let snapshot = table
            .metadata()
            .current_snapshot()
            .expect("The batch should be appended");
        assert_eq!(
            snapshot
                .summary()
                .additional_properties
                .get("added-records"),
            Some(&"4".to_owned()),
            "one append should hold the messages of both route values"
        );
    }

    #[tokio::test]
    async fn given_a_catalog_that_refuses_the_load_when_skipping_rows_should_fail_without_skipping()
    {
        let catalog = rest_catalog_answering(http::StatusCode::FORBIDDEN).await;
        let mut router = DynamicRouter::new(
            Appender::new(Box::new(catalog), InvalidRows::Skip, SINGLE_ATTEMPT),
            "table".to_owned(),
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
        assert!(matches!(error, Error::PermanentHttpError(_)), "{error}");
    }
}
