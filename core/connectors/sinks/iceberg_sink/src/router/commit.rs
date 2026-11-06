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

use crate::router::format_error_chain;
use http::StatusCode;
use iceberg::spec::DataFile;
use iceberg::table::Table;
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::{Catalog, ErrorKind, TableIdent};
use iggy_connector_sdk::Error;
use iggy_connector_sdk::retry::{RetryPolicy, is_transient_status, retry_async};
use std::collections::HashMap;
use tracing::{error, info};
use uuid::Uuid;

/// Snapshot summary property naming the append, so that a retry can tell
/// whether the catalog applied an earlier attempt whose response was lost.
pub(crate) const COMMIT_ID_PROPERTY: &str = "iggy.commit-id";

/// Appends `data_files` to `table` in one snapshot and retries a failed commit
/// with `retry`. Every retry first looks for the snapshot of an earlier attempt.
pub(crate) async fn commit_data_files(
    table: &Table,
    catalog: &dyn Catalog,
    data_files: Vec<DataFile>,
    retry: RetryPolicy,
) -> Result<(), Error> {
    let commit_id = Uuid::new_v4().to_string();
    let transaction = Transaction::new(table);
    // The check reads every manifest of the table at once on each commit, and the data files
    // carry fresh UUID names it could never match.
    let action = transaction
        .fast_append()
        .with_check_duplicate(false)
        .set_snapshot_properties(HashMap::from([(
            COMMIT_ID_PROPERTY.to_owned(),
            commit_id.clone(),
        )]))
        .add_data_files(data_files);
    let transaction = action.apply(transaction).map_err(|err| {
        let chain = format_error_chain(&err);
        error!(
            "Failed to apply transaction on table with UUID: {}, Error: {}",
            table.metadata().uuid(),
            chain
        );
        Error::TransactionApplyError(chain)
    })?;

    // Data files are kept on commit failure: the catalog may have applied the
    // commit before the error surfaced, and deleting files referenced by a
    // committed snapshot would corrupt the table.
    let context = format!("Commit to Iceberg table {}", table.identifier());
    let identifier = table.identifier();
    let transaction = &transaction;
    let commit_id = commit_id.as_str();
    let mut attempted = false;
    retry_async(retry, &context, is_transient, move || {
        let retrying = std::mem::replace(&mut attempted, true);
        commit_once(catalog, identifier, transaction, commit_id, retrying)
    })
    .await
    .map_err(|failure| {
        let chain = format_error_chain(&failure.error);
        error!(
            "Failed to commit transaction on table with UUID: {} after {} attempts, Error: {}",
            table.metadata().uuid(),
            failure.attempts,
            chain
        );
        Error::CatalogCommitError(chain)
    })
}

/// A commit that could not reach the catalog, or that the catalog failed or throttled, can pass
/// on another attempt. A conflict is not retried here: `Transaction::commit` has already retried
/// it as far as the table's `commit.retry.*` properties allow.
fn is_transient(error: &iceberg::Error) -> bool {
    error.kind() == ErrorKind::Unexpected && response_status(error).is_none_or(is_transient_status)
}

/// `iceberg-catalog-rest` keeps the status of an answer it did not expect only in the error's
/// private context, which `Display` renders first: `context: { status: 403 Forbidden, ...` for a
/// catalog call and `context: { code: 401 Unauthorized, ...` for the token endpoint.
fn response_status(error: &iceberg::Error) -> Option<StatusCode> {
    let rendered = error.to_string();
    let (_, context) = rendered.split_once("context: { ")?;
    let status = context
        .strip_prefix("status: ")
        .or_else(|| context.strip_prefix("code: "))?;
    StatusCode::from_bytes(status.get(..3)?.as_bytes()).ok()
}

async fn commit_once(
    catalog: &dyn Catalog,
    identifier: &TableIdent,
    transaction: &Transaction,
    commit_id: &str,
    retrying: bool,
) -> Result<(), iceberg::Error> {
    if retrying {
        let current = catalog.load_table(identifier).await?;
        if holds_commit(&current, commit_id) {
            info!(
                "Iceberg table {identifier} already holds commit {commit_id} from an earlier attempt"
            );
            return Ok(());
        }
    }
    transaction.clone().commit(catalog).await?;
    Ok(())
}

fn holds_commit(table: &Table, commit_id: &str) -> bool {
    table.metadata().snapshots().any(|snapshot| {
        snapshot
            .summary()
            .additional_properties
            .get(COMMIT_ID_PROPERTY)
            .is_some_and(|id| id == commit_id)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, NestedField, PrimitiveType,
        Schema as IcebergSchema, Type,
    };
    use iceberg::{
        CatalogBuilder, MemoryCatalog, Namespace, NamespaceIdent, TableCommit, TableCreation,
    };
    use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, RestCatalog, RestCatalogBuilder};
    use std::collections::VecDeque;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicU32, Ordering};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;

    const FAST_RETRY: RetryPolicy = RetryPolicy {
        max_attempts: 3,
        base_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(1),
    };

    /// What `FlakyCatalog` does with the next `update_table` call.
    #[derive(Debug, Clone, Copy)]
    enum CommitFailure {
        /// The request never reaches the catalog.
        Unreachable,
        /// The catalog applies the commit, and the response is lost.
        ResponseLost,
        /// The catalog refuses the commit for good.
        Refused,
        /// The catalog answers with a status the REST client does not expect.
        Rejected(StatusCode),
        /// Another writer changed the table first.
        Conflict,
    }

    /// A memory catalog whose `update_table` fails as scripted, then succeeds.
    #[derive(Debug)]
    struct FlakyCatalog {
        inner: MemoryCatalog,
        failures: Mutex<VecDeque<CommitFailure>>,
        update_calls: AtomicU32,
    }

    impl FlakyCatalog {
        async fn with_table(failures: &[CommitFailure]) -> (Self, Table) {
            Self::with_table_properties(failures, HashMap::new()).await
        }

        async fn with_table_properties(
            failures: &[CommitFailure],
            properties: HashMap<String, String>,
        ) -> (Self, Table) {
            let inner = MemoryCatalogBuilder::default()
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
            inner
                .create_namespace(&namespace, HashMap::new())
                .await
                .expect("Failed to create namespace");
            let schema = IcebergSchema::builder()
                .with_fields(vec![
                    NestedField::required(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                ])
                .build()
                .expect("Failed to build schema");
            let table = inner
                .create_table(
                    &namespace,
                    TableCreation::builder()
                        .name("events".to_owned())
                        .schema(schema)
                        .properties(properties)
                        .build(),
                )
                .await
                .expect("Failed to create table");
            let catalog = Self {
                inner,
                failures: Mutex::new(failures.iter().copied().collect()),
                update_calls: AtomicU32::new(0),
            };
            (catalog, table)
        }

        fn update_calls(&self) -> u32 {
            self.update_calls.load(Ordering::Relaxed)
        }

        async fn snapshot_count(&self, table: &Table) -> usize {
            self.inner
                .load_table(table.identifier())
                .await
                .expect("Failed to load table")
                .metadata()
                .snapshots()
                .count()
        }
    }

    #[async_trait]
    impl Catalog for FlakyCatalog {
        async fn list_namespaces(
            &self,
            parent: Option<&NamespaceIdent>,
        ) -> iceberg::Result<Vec<NamespaceIdent>> {
            self.inner.list_namespaces(parent).await
        }

        async fn create_namespace(
            &self,
            namespace: &NamespaceIdent,
            properties: HashMap<String, String>,
        ) -> iceberg::Result<Namespace> {
            self.inner.create_namespace(namespace, properties).await
        }

        async fn get_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<Namespace> {
            self.inner.get_namespace(namespace).await
        }

        async fn namespace_exists(&self, namespace: &NamespaceIdent) -> iceberg::Result<bool> {
            self.inner.namespace_exists(namespace).await
        }

        async fn update_namespace(
            &self,
            namespace: &NamespaceIdent,
            properties: HashMap<String, String>,
        ) -> iceberg::Result<()> {
            self.inner.update_namespace(namespace, properties).await
        }

        async fn drop_namespace(&self, namespace: &NamespaceIdent) -> iceberg::Result<()> {
            self.inner.drop_namespace(namespace).await
        }

        async fn list_tables(
            &self,
            namespace: &NamespaceIdent,
        ) -> iceberg::Result<Vec<TableIdent>> {
            self.inner.list_tables(namespace).await
        }

        async fn create_table(
            &self,
            namespace: &NamespaceIdent,
            creation: TableCreation,
        ) -> iceberg::Result<Table> {
            self.inner.create_table(namespace, creation).await
        }

        async fn load_table(&self, table: &TableIdent) -> iceberg::Result<Table> {
            self.inner.load_table(table).await
        }

        async fn drop_table(&self, table: &TableIdent) -> iceberg::Result<()> {
            self.inner.drop_table(table).await
        }

        async fn purge_table(&self, table: &TableIdent) -> iceberg::Result<()> {
            self.inner.purge_table(table).await
        }

        async fn table_exists(&self, table: &TableIdent) -> iceberg::Result<bool> {
            self.inner.table_exists(table).await
        }

        async fn rename_table(&self, src: &TableIdent, dest: &TableIdent) -> iceberg::Result<()> {
            self.inner.rename_table(src, dest).await
        }

        async fn register_table(
            &self,
            table: &TableIdent,
            metadata_location: String,
        ) -> iceberg::Result<Table> {
            self.inner.register_table(table, metadata_location).await
        }

        async fn update_table(&self, commit: TableCommit) -> iceberg::Result<Table> {
            self.update_calls.fetch_add(1, Ordering::Relaxed);
            let failure = self
                .failures
                .lock()
                .expect("Failures lock poisoned")
                .pop_front();
            match failure {
                None => self.inner.update_table(commit).await,
                Some(CommitFailure::Unreachable) => Err(iceberg::Error::new(
                    ErrorKind::Unexpected,
                    "Connection refused",
                )),
                Some(CommitFailure::ResponseLost) => {
                    self.inner.update_table(commit).await?;
                    Err(iceberg::Error::new(
                        ErrorKind::Unexpected,
                        "Gateway timeout, the commit state is unknown",
                    ))
                }
                Some(CommitFailure::Refused) => Err(iceberg::Error::new(
                    ErrorKind::TableNotFound,
                    "Tried to update a table that does not exist",
                )),
                Some(CommitFailure::Rejected(status)) => Err(iceberg::Error::new(
                    ErrorKind::Unexpected,
                    "Received response with unexpected status code",
                )
                .with_context("status", status.to_string())),
                Some(CommitFailure::Conflict) => Err(iceberg::Error::new(
                    ErrorKind::CatalogCommitConflicts,
                    "CatalogCommitConflicts, one or more requirements failed. The client may retry.",
                )
                .with_retryable(true)),
            }
        }
    }

    fn data_file(table: &Table) -> DataFile {
        DataFileBuilder::default()
            .content(DataContentType::Data)
            .file_path(format!(
                "{}/data/{}.parquet",
                table.metadata().location(),
                Uuid::new_v4()
            ))
            .file_format(DataFileFormat::Parquet)
            .record_count(3)
            .file_size_in_bytes(100)
            .build()
            .expect("Failed to build data file")
    }

    #[tokio::test]
    async fn given_an_unreachable_catalog_should_retry_until_the_commit_lands() {
        let (catalog, table) =
            FlakyCatalog::with_table(&[CommitFailure::Unreachable, CommitFailure::Unreachable])
                .await;

        commit_data_files(&table, &catalog, vec![data_file(&table)], FAST_RETRY)
            .await
            .expect("The third attempt should commit");

        assert_eq!(catalog.update_calls(), 3);
        assert_eq!(catalog.snapshot_count(&table).await, 1);
    }

    #[tokio::test]
    async fn given_a_lost_commit_response_should_not_append_the_files_twice() {
        let (catalog, table) = FlakyCatalog::with_table(&[CommitFailure::ResponseLost]).await;

        commit_data_files(&table, &catalog, vec![data_file(&table)], FAST_RETRY)
            .await
            .expect("The retry should find the applied commit");

        assert_eq!(catalog.update_calls(), 1);
        assert_eq!(catalog.snapshot_count(&table).await, 1);
    }

    #[tokio::test]
    async fn given_a_refused_commit_should_fail_without_retrying() {
        let (catalog, table) = FlakyCatalog::with_table(&[CommitFailure::Refused]).await;

        let error = commit_data_files(&table, &catalog, vec![data_file(&table)], FAST_RETRY)
            .await
            .expect_err("A refused commit should fail");

        assert!(matches!(error, Error::CatalogCommitError(_)), "{error}");
        assert_eq!(catalog.update_calls(), 1);
        assert_eq!(catalog.snapshot_count(&table).await, 0);
    }

    #[tokio::test]
    async fn given_a_client_error_status_should_fail_without_retrying() {
        let (catalog, table) =
            FlakyCatalog::with_table(&[CommitFailure::Rejected(StatusCode::FORBIDDEN)]).await;

        let error = commit_data_files(&table, &catalog, vec![data_file(&table)], FAST_RETRY)
            .await
            .expect_err("A forbidden commit should fail");

        assert!(matches!(error, Error::CatalogCommitError(_)), "{error}");
        assert_eq!(catalog.update_calls(), 1);
        assert_eq!(catalog.snapshot_count(&table).await, 0);
    }

    #[tokio::test]
    async fn given_a_throttled_commit_should_retry_until_the_commit_lands() {
        let (catalog, table) =
            FlakyCatalog::with_table(&[CommitFailure::Rejected(StatusCode::TOO_MANY_REQUESTS)])
                .await;

        commit_data_files(&table, &catalog, vec![data_file(&table)], FAST_RETRY)
            .await
            .expect("The retry should commit");

        assert_eq!(catalog.update_calls(), 2);
        assert_eq!(catalog.snapshot_count(&table).await, 1);
    }

    #[tokio::test]
    async fn given_conflicts_that_outlast_the_table_retries_should_fail_without_retrying() {
        let table_retries = HashMap::from([
            ("commit.retry.num-retries".to_owned(), "1".to_owned()),
            ("commit.retry.min-wait-ms".to_owned(), "1".to_owned()),
            ("commit.retry.max-wait-ms".to_owned(), "1".to_owned()),
        ]);
        let (catalog, table) =
            FlakyCatalog::with_table_properties(&[CommitFailure::Conflict; 6], table_retries).await;

        let error = commit_data_files(&table, &catalog, vec![data_file(&table)], FAST_RETRY)
            .await
            .expect_err("The commit should fail once the table retries run out");

        assert!(matches!(error, Error::CatalogCommitError(_)), "{error}");
        assert_eq!(catalog.update_calls(), 2);
        assert_eq!(catalog.snapshot_count(&table).await, 0);
    }

    #[tokio::test]
    async fn given_a_rest_catalog_answering_with_an_unexpected_status_should_keep_it() {
        for (status, transient) in [
            (StatusCode::BAD_REQUEST, false),
            (StatusCode::UNAUTHORIZED, false),
            (StatusCode::FORBIDDEN, false),
            (StatusCode::TOO_MANY_REQUESTS, true),
            (StatusCode::SERVICE_UNAVAILABLE, true),
        ] {
            let catalog = rest_catalog_answering(status).await;
            let error = catalog
                .load_table(&TableIdent::from_strs(["test", "events"]).expect("Invalid identifier"))
                .await
                .expect_err("The catalog should reject the call");

            assert_eq!(response_status(&error), Some(status), "{error}");
            assert_eq!(is_transient(&error), transient, "{error}");
        }
    }

    #[tokio::test]
    async fn given_an_unreachable_catalog_when_the_attempts_run_out_should_fail() {
        let (catalog, table) = FlakyCatalog::with_table(&[CommitFailure::Unreachable; 3]).await;

        let error = commit_data_files(&table, &catalog, vec![data_file(&table)], FAST_RETRY)
            .await
            .expect_err("The commit should fail once the attempts run out");

        assert!(matches!(error, Error::CatalogCommitError(_)), "{error}");
        assert_eq!(catalog.update_calls(), FAST_RETRY.max_attempts);
        assert_eq!(catalog.snapshot_count(&table).await, 0);
    }

    /// A REST catalog whose server answers every request with `status` and no body.
    async fn rest_catalog_answering(status: StatusCode) -> RestCatalog {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("Failed to bind the catalog server");
        let uri = format!(
            "http://{}",
            listener
                .local_addr()
                .expect("Failed to read the catalog address")
        );
        tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut request = [0u8; 4096];
                let _ = stream.read(&mut request).await;
                let response =
                    format!("HTTP/1.1 {status}\r\ncontent-length: 0\r\nconnection: close\r\n\r\n");
                let _ = stream.write_all(response.as_bytes()).await;
            }
        });
        RestCatalogBuilder::default()
            .load(
                "rest",
                HashMap::from([(REST_CATALOG_PROP_URI.to_owned(), uri)]),
            )
            .await
            .expect("Failed to create the REST catalog")
    }
}
