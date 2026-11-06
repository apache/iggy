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
use crate::router::commit::table_load_error;
use crate::router::redelivery::HeldMessages;
use crate::router::{
    BatchOrigin, Router, before, first_rejection, format_error_chain, is_valid_namespaced_table,
    load_table, write_data,
};
use async_trait::async_trait;
use iceberg::Catalog;
use iceberg::table::Table;
use iggy_connector_sdk::retry::RetryPolicy;
use iggy_connector_sdk::{ConsumedMessage, Error, Schema};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use tracing::{error, info, warn};

/// A table that the configuration declares. A table that does not load is loaded again by the
/// next batch, so that a catalog that was unreachable at startup does not drop it for good.
#[derive(Debug)]
struct DeclaredTable {
    name: String,
    table: Mutex<Option<Arc<Table>>>,
}

impl DeclaredTable {
    fn loaded(&self) -> Option<Arc<Table>> {
        self.lock().clone()
    }

    fn lock(&self) -> MutexGuard<'_, Option<Arc<Table>>> {
        // The guarded value is replaced whole, so a panic while the lock was held cannot leave it
        // half written.
        self.table.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

#[derive(Debug)]
pub(crate) struct StaticRouter {
    tables: Vec<DeclaredTable>,
    /// Declared names that name no table, which no load can fix.
    invalid_names: Vec<String>,
    catalog: Box<dyn Catalog>,
    invalid_rows: InvalidRows,
    commit_retry: RetryPolicy,
    held: Option<HeldMessages>,
}

impl StaticRouter {
    pub async fn new(
        catalog: Box<dyn Catalog>,
        declared_tables: &Vec<String>,
        invalid_rows: InvalidRows,
        commit_retry: RetryPolicy,
    ) -> Result<Self, Error> {
        let mut tables = Vec::with_capacity(declared_tables.len());
        let mut invalid_names = Vec::new();
        for declared_table in declared_tables {
            if !is_valid_namespaced_table(declared_table) {
                error!(
                    "Declared table {} is not valid. It has to include at least one namespace before the table name separated by '.' character",
                    declared_table
                );
                invalid_names.push(declared_table.clone());
                continue;
            }

            let table = match load_table(declared_table, catalog.as_ref()).await {
                Ok(table) => Some(Arc::new(table)),
                Err(err) => {
                    warn!(
                        "Declared table {declared_table} does not load from the configured catalog, so each batch tries again. {}",
                        format_error_chain(&err)
                    );
                    None
                }
            };
            tables.push(DeclaredTable {
                name: declared_table.clone(),
                table: Mutex::new(table),
            });
        }
        let loaded = tables
            .iter()
            .filter(|declared| declared.loaded().is_some())
            .count();
        info!(
            "Static router found {loaded} tables on iceberg catalog from {} tables declared",
            declared_tables.len()
        );
        if loaded == 0 {
            error!("No valid tables found. Can't initiate Iceberg connector");
            return Err(Error::InvalidConfig);
        }
        Ok(StaticRouter {
            tables,
            invalid_names,
            catalog,
            invalid_rows,
            commit_retry,
            held: None,
        })
    }

    /// The table of `declared`, loaded from the catalog when no earlier batch could. At most
    /// once, a table that does not load is skipped for this batch. At least once, it fails the
    /// batch, so that its messages are delivered again instead of lost.
    async fn table(&self, declared: &DeclaredTable) -> Result<Option<Arc<Table>>, Error> {
        if let Some(table) = declared.loaded() {
            return Ok(Some(table));
        }
        match load_table(&declared.name, self.catalog.as_ref()).await {
            Ok(table) => {
                let table = Arc::new(table);
                info!("Declared table {} loaded from the catalog", declared.name);
                *declared.lock() = Some(Arc::clone(&table));
                Ok(Some(table))
            }
            Err(err) if self.held.is_some() => Err(table_load_error(&declared.name, &err)),
            Err(err) => {
                warn!(
                    "Skipping declared table {} for this batch, it does not load from the catalog. {}",
                    declared.name,
                    format_error_chain(&err)
                );
                Ok(None)
            }
        }
    }
}

#[async_trait]
impl Router for StaticRouter {
    async fn route_data(
        &self,
        origin: &BatchOrigin<'_>,
        messages_schema: Schema,
        messages: Vec<ConsumedMessage>,
    ) -> Result<(), crate::Error> {
        let mut rejection = None;
        for declared in &self.tables {
            let Some(table) = self.table(declared).await? else {
                continue;
            };
            // Once a table rejects a message, the others still need every message before it.
            let messages = before(&messages, rejection.as_ref());
            let written = write_data(
                origin,
                messages,
                &table,
                None,
                self.catalog.as_ref(),
                messages_schema,
                self.invalid_rows,
                self.commit_retry,
                self.held.as_ref(),
            )
            .await?;
            info!(
                "Routed {} input rows to iceberg table {}, committed {}",
                messages.len(),
                table.identifier().name(),
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
        if !self.invalid_names.is_empty() {
            return Err(Error::Unsupported(format!(
                "declared tables {:?} are not valid table names, so their messages could never be delivered. Fix or remove them in `tables`",
                self.invalid_names
            )));
        }
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
    use iggy_connector_sdk::Payload;
    use std::collections::HashMap;
    use std::time::Duration;

    const ORIGIN: BatchOrigin<'static> = BatchOrigin {
        connector_id: 1,
        stream: "stream",
        topic: "topic",
        partition_id: 0,
    };

    /// A router over `declared` tables, of which the catalog holds only `test.events`.
    async fn router(declared: &[&str]) -> StaticRouter {
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
        let declared: Vec<String> = declared.iter().map(|name| (*name).to_owned()).collect();
        let retry = RetryPolicy {
            max_attempts: 1,
            base_delay: Duration::from_millis(1),
            max_delay: Duration::from_millis(1),
        };
        StaticRouter::new(Box::new(catalog), &declared, InvalidRows::Fail, retry)
            .await
            .expect("One declared table should load")
    }

    fn messages() -> Vec<ConsumedMessage> {
        vec![ConsumedMessage {
            id: 1,
            offset: 0,
            checksum: 0,
            timestamp: 1,
            origin_timestamp: 0,
            headers: None,
            payload: Payload::Json(simd_json::json!({ "id": 1 })),
        }]
    }

    async fn snapshots(router: &StaticRouter) -> usize {
        router
            .catalog
            .load_table(&TableIdent::from_strs(["test", "events"]).expect("a valid table name"))
            .await
            .expect("Failed to load table")
            .metadata()
            .snapshots()
            .count()
    }

    #[tokio::test]
    async fn given_a_declared_table_that_does_not_load_when_at_most_once_should_skip_it() {
        let router = router(&["test.events", "test.later"]).await;

        router
            .route_data(&ORIGIN, Schema::Json, messages())
            .await
            .expect("At most once, a table that does not load is skipped");

        assert_eq!(snapshots(&router).await, 1);
    }

    #[tokio::test]
    async fn given_a_declared_table_that_does_not_load_when_at_least_once_should_fail_the_batch() {
        let mut router = router(&["test.events", "test.later"]).await;
        router
            .enable_at_least_once()
            .expect("Every declared name is valid");

        let error = router
            .route_data(&ORIGIN, Schema::Json, messages())
            .await
            .expect_err("At least once, a table that does not load fails the batch");

        assert!(
            error.is_permanent(),
            "a missing table stays missing: {error}"
        );
        assert!(error.to_string().contains("test.later"), "{error}");
    }

    #[tokio::test]
    async fn given_a_declared_name_that_names_no_table_should_refuse_at_least_once() {
        let mut router = router(&["test.events", "events"]).await;

        let error = router
            .enable_at_least_once()
            .expect_err("A name without a namespace can never be written");

        assert!(matches!(error, Error::Unsupported(_)), "{error}");
    }
}
