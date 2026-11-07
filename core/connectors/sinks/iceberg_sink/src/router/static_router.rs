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

use crate::router::commit::table_load_error;
use crate::router::{
    Appender, BatchOrigin, Router, before, first_rejection, format_error_chain,
    is_valid_namespaced_table, load_table,
};
use async_trait::async_trait;
use iceberg::table::Table;
use iggy_connector_sdk::{ConsumedMessage, Error, FlushPartition, Schema, TopicMetadata};
use std::sync::OnceLock;
use tracing::{error, info, warn};

/// A table that the configuration declares, set once it loads.
#[derive(Debug)]
struct DeclaredTable {
    name: String,
    table: OnceLock<Table>,
}

#[derive(Debug)]
pub(crate) struct StaticRouter {
    tables: Vec<DeclaredTable>,
    /// Declared names that name no table, which no load can fix.
    invalid_names: Vec<String>,
    appender: Appender,
}

impl StaticRouter {
    pub async fn new(appender: Appender, declared_tables: &Vec<String>) -> Result<Self, Error> {
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

            let table = match load_table(declared_table, appender.catalog.as_ref()).await {
                Ok(table) => OnceLock::from(table),
                Err(err) => {
                    warn!(
                        "Declared table {declared_table} does not load from the configured catalog. At most once it is skipped until the connector restarts, at least once each batch loads it again. {}",
                        format_error_chain(&err)
                    );
                    OnceLock::new()
                }
            };
            tables.push(DeclaredTable {
                name: declared_table.clone(),
                table,
            });
        }
        let loaded = tables
            .iter()
            .filter(|declared| declared.table.get().is_some())
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
            appender,
        })
    }

    /// The table of `declared`. At most once, a table that did not load at startup is skipped,
    /// so that a batch does not pay a catalog call for it. At least once, it is loaded again, and
    /// a failed load fails the batch, so that its messages are delivered again instead of lost.
    async fn table<'d>(&self, declared: &'d DeclaredTable) -> Result<Option<&'d Table>, Error> {
        if let Some(table) = declared.table.get() {
            return Ok(Some(table));
        }
        if self.appender.held.is_none() {
            return Ok(None);
        }
        let table = load_table(&declared.name, self.appender.catalog.as_ref())
            .await
            .map_err(|err| table_load_error(&declared.name, &err))?;
        info!("Declared table {} loaded from the catalog", declared.name);
        Ok(Some(declared.table.get_or_init(|| table)))
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
            let written = self
                .appender
                .append(origin, table, false, messages_schema, messages)
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
    use crate::InvalidRows;
    use crate::router::commit::tests::{SINGLE_ATTEMPT, catalog_with_table};
    use iceberg::{TableCreation, TableIdent};
    use iggy_connector_sdk::Payload;
    use std::collections::HashMap;

    const ORIGIN: BatchOrigin<'static> = BatchOrigin {
        connector_id: 1,
        stream: "stream",
        topic: "topic",
        partition_id: 0,
    };

    /// A router over `declared` tables, of which the catalog holds only `test.events`.
    async fn router(declared: &[&str]) -> StaticRouter {
        let (catalog, _) = catalog_with_table(HashMap::new()).await;
        let declared: Vec<String> = declared.iter().map(|name| (*name).to_owned()).collect();
        let appender = Appender::new(Box::new(catalog), InvalidRows::Fail, SINGLE_ATTEMPT);
        StaticRouter::new(appender, &declared)
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

    async fn snapshots(router: &StaticRouter, table: &str) -> usize {
        router
            .appender
            .catalog
            .load_table(&TableIdent::from_strs(["test", table]).expect("a valid table name"))
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

        assert_eq!(snapshots(&router, "events").await, 1);
    }

    #[tokio::test]
    async fn given_a_declared_table_created_after_startup_when_at_most_once_should_keep_skipping_it()
     {
        let router = router(&["test.events", "test.later"]).await;
        let events = router
            .appender
            .catalog
            .load_table(&TableIdent::from_strs(["test", "events"]).expect("a valid table name"))
            .await
            .expect("Failed to load table");
        router
            .appender
            .catalog
            .create_table(
                events.identifier().namespace(),
                TableCreation::builder()
                    .name("later".to_owned())
                    .schema(events.metadata().current_schema().as_ref().clone())
                    .build(),
            )
            .await
            .expect("Failed to create table");

        router
            .route_data(&ORIGIN, Schema::Json, messages())
            .await
            .expect("At most once, a table that did not load at startup is skipped");

        assert_eq!(snapshots(&router, "later").await, 0);
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
