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

/// Appends `data_files` to `table` in one snapshot, with `properties` in its summary, and
/// retries a failed commit with `retry`. Every retry first looks for the snapshot of an earlier
/// attempt.
pub(crate) async fn commit_data_files(
    table: &Table,
    catalog: &dyn Catalog,
    data_files: Vec<DataFile>,
    mut properties: HashMap<String, String>,
    retry: RetryPolicy,
) -> Result<(), Error> {
    let commit_id = Uuid::new_v4().to_string();
    properties.insert(COMMIT_ID_PROPERTY.to_owned(), commit_id.clone());
    let transaction = Transaction::new(table);
    // The check reads every manifest of the table at once on each commit, and the data files
    // carry fresh UUID names it could never match.
    let action = transaction
        .fast_append()
        .with_check_duplicate(false)
        .set_snapshot_properties(properties)
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
        // A conflict outlasted the table's own retries, so a delivery of the batch again rebuilds
        // the transaction on fresh metadata. Any other refusal, such as a denied write, fails the
        // same way every time.
        if is_transient(&failure.error) || failure.error.kind() == ErrorKind::CatalogCommitConflicts
        {
            Error::CatalogCommitError(chain)
        } else {
            Error::PermanentHttpError(format!(
                "The Iceberg catalog refused the commit to table {identifier}: {chain}"
            ))
        }
    })
}

/// A catalog call that could not reach the catalog, or that the catalog failed or throttled, can
/// pass on another attempt. A commit conflict is not retried here: `Transaction::commit` has
/// already retried it as far as the table's `commit.retry.*` properties allow.
pub(crate) fn is_transient(error: &iceberg::Error) -> bool {
    error.kind() == ErrorKind::Unexpected && response_status(error).is_none_or(is_transient_status)
}

/// The error for a table that does not load from the catalog. An unreachable, failing or
/// throttling catalog is transient. Any other refusal, such as a missing table or denied access,
/// fails the same way until the configuration or the catalog changes.
pub(crate) fn table_load_error(table: &str, error: &iceberg::Error) -> Error {
    let reason = format!(
        "Failed to load Iceberg table {table} from the catalog: {}",
        format_error_chain(error)
    );
    if is_transient(error) {
        Error::Connection(reason)
    } else {
        Error::InvalidConfigValue(reason)
    }
}

/// Whether a table load failed because the table or its namespace does not exist.
pub(crate) fn is_missing(error: &iceberg::Error) -> bool {
    matches!(
        error.kind(),
        ErrorKind::TableNotFound | ErrorKind::NamespaceNotFound
    )
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
pub(crate) mod tests {
    use super::*;
    use crate::InvalidRows;
    use crate::router::redelivery::{HeldMessages, LastMessage, snapshot_properties};
    use crate::router::{BatchOrigin, Written, write_data};
    use async_trait::async_trait;
    use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
    use iceberg::spec::{
        DataContentType, DataFileBuilder, DataFileFormat, NestedField, PrimitiveType,
        Schema as IcebergSchema, Type,
    };
    use iceberg::spec::{MAIN_BRANCH, SnapshotReference, SnapshotRetention, TableMetadataBuilder};
    use iceberg::{
        CatalogBuilder, MemoryCatalog, Namespace, NamespaceIdent, TableCommit, TableCreation,
    };
    use iceberg_catalog_rest::{REST_CATALOG_PROP_URI, RestCatalog, RestCatalogBuilder};
    use iggy_connector_sdk::Schema;
    use iggy_connector_sdk::{ConsumedMessage, Payload};
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

        commit_data_files(
            &table,
            &catalog,
            vec![data_file(&table)],
            HashMap::new(),
            FAST_RETRY,
        )
        .await
        .expect("The third attempt should commit");

        assert_eq!(catalog.update_calls(), 3);
        assert_eq!(catalog.snapshot_count(&table).await, 1);
    }

    #[tokio::test]
    async fn given_a_lost_commit_response_should_not_append_the_files_twice() {
        let (catalog, table) = FlakyCatalog::with_table(&[CommitFailure::ResponseLost]).await;

        commit_data_files(
            &table,
            &catalog,
            vec![data_file(&table)],
            HashMap::new(),
            FAST_RETRY,
        )
        .await
        .expect("The retry should find the applied commit");

        assert_eq!(catalog.update_calls(), 1);
        assert_eq!(catalog.snapshot_count(&table).await, 1);
    }

    #[tokio::test]
    async fn given_a_refused_commit_should_fail_without_retrying() {
        let (catalog, table) = FlakyCatalog::with_table(&[CommitFailure::Refused]).await;

        let error = commit_data_files(
            &table,
            &catalog,
            vec![data_file(&table)],
            HashMap::new(),
            FAST_RETRY,
        )
        .await
        .expect_err("A refused commit should fail");

        assert!(
            error.is_permanent(),
            "a refused commit should stop the delivery: {error}"
        );
        assert_eq!(catalog.update_calls(), 1);
        assert_eq!(catalog.snapshot_count(&table).await, 0);
    }

    #[tokio::test]
    async fn given_a_client_error_status_should_fail_without_retrying() {
        let (catalog, table) =
            FlakyCatalog::with_table(&[CommitFailure::Rejected(StatusCode::FORBIDDEN)]).await;

        let error = commit_data_files(
            &table,
            &catalog,
            vec![data_file(&table)],
            HashMap::new(),
            FAST_RETRY,
        )
        .await
        .expect_err("A forbidden commit should fail");

        assert!(
            error.is_permanent(),
            "a refused commit should stop the delivery: {error}"
        );
        assert_eq!(catalog.update_calls(), 1);
        assert_eq!(catalog.snapshot_count(&table).await, 0);
    }

    #[tokio::test]
    async fn given_a_throttled_commit_should_retry_until_the_commit_lands() {
        let (catalog, table) =
            FlakyCatalog::with_table(&[CommitFailure::Rejected(StatusCode::TOO_MANY_REQUESTS)])
                .await;

        commit_data_files(
            &table,
            &catalog,
            vec![data_file(&table)],
            HashMap::new(),
            FAST_RETRY,
        )
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

        let error = commit_data_files(
            &table,
            &catalog,
            vec![data_file(&table)],
            HashMap::new(),
            FAST_RETRY,
        )
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

        let error = commit_data_files(
            &table,
            &catalog,
            vec![data_file(&table)],
            HashMap::new(),
            FAST_RETRY,
        )
        .await
        .expect_err("The commit should fail once the attempts run out");

        assert!(matches!(error, Error::CatalogCommitError(_)), "{error}");
        assert_eq!(catalog.update_calls(), FAST_RETRY.max_attempts);
        assert_eq!(catalog.snapshot_count(&table).await, 0);
    }

    /// A REST catalog whose server answers every request with `status` and no body.
    /// A REST catalog that answers every call with `status`.
    pub(crate) async fn rest_catalog_answering(status: StatusCode) -> RestCatalog {
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

    const ORIGIN: BatchOrigin<'static> = BatchOrigin {
        connector_id: 1,
        stream: "stream",
        topic: "topic",
        partition_id: 2,
    };

    fn message(offset: u64) -> ConsumedMessage {
        ConsumedMessage {
            id: u128::from(offset) + 100,
            offset,
            checksum: 0,
            timestamp: offset + 1_000,
            origin_timestamp: 0,
            headers: None,
            payload: Payload::Raw(Vec::new()),
        }
    }

    fn messages(offsets: std::ops::RangeInclusive<u64>) -> Vec<ConsumedMessage> {
        offsets.map(message).collect()
    }

    fn json_messages(offsets: std::ops::RangeInclusive<u64>) -> Vec<ConsumedMessage> {
        offsets
            .map(|offset| ConsumedMessage {
                payload: Payload::Json(simd_json::json!({ "id": offset })),
                ..message(offset)
            })
            .collect()
    }

    fn offsets(messages: &[ConsumedMessage]) -> Vec<u64> {
        messages.iter().map(|message| message.offset).collect()
    }

    async fn commit_up_to(
        catalog: &FlakyCatalog,
        table: &Table,
        origin: &BatchOrigin<'_>,
        offset: u64,
    ) {
        let properties = snapshot_properties(origin, LastMessage::from(&message(offset)));
        commit_data_files(
            table,
            catalog,
            vec![data_file(table)],
            properties,
            FAST_RETRY,
        )
        .await
        .expect("The append should commit");
    }

    async fn current_table(catalog: &FlakyCatalog, table: &Table) -> Table {
        catalog
            .inner
            .load_table(table.identifier())
            .await
            .expect("Failed to load table")
    }

    #[allow(clippy::too_many_arguments)]
    async fn write(
        catalog: &FlakyCatalog,
        table: &Table,
        batch: &[ConsumedMessage],
        invalid_rows: InvalidRows,
        retry: RetryPolicy,
        held: Option<&HeldMessages>,
    ) -> Result<Written, Error> {
        write_data(
            &ORIGIN,
            batch,
            table,
            None,
            catalog,
            Schema::Json,
            invalid_rows,
            retry,
            held,
        )
        .await
    }

    #[tokio::test]
    async fn given_a_table_holding_part_of_a_batch_should_append_only_the_messages_after_it() {
        let (catalog, table) = FlakyCatalog::with_table(&[]).await;
        commit_up_to(&catalog, &table, &ORIGIN, 4).await;
        let other_partition = BatchOrigin {
            partition_id: 3,
            ..ORIGIN
        };
        commit_up_to(&catalog, &table, &other_partition, 9).await;
        let batch = messages(3..=5);

        let unheld = HeldMessages::default()
            .unheld(&ORIGIN, &table, None, &catalog, &batch)
            .await
            .expect("The table should load");

        assert_eq!(offsets(unheld), [5]);
    }

    #[tokio::test]
    async fn given_a_backlog_batch_carrying_an_older_append_should_skip_up_to_the_newest() {
        let (catalog, table) = FlakyCatalog::with_table(&[]).await;
        for offset in [1, 3, 5] {
            commit_up_to(&catalog, &table, &ORIGIN, offset).await;
        }
        let first_batch = messages(0..=1);
        let longer_batch = messages(0..=6);

        let held = HeldMessages::default();
        let first = held
            .unheld(&ORIGIN, &table, None, &catalog, &first_batch)
            .await
            .expect("The table should load");
        let longer = held
            .unheld(&ORIGIN, &table, None, &catalog, &longer_batch)
            .await
            .expect("The table should load");

        assert!(first.is_empty(), "{:?}", offsets(first));
        assert_eq!(offsets(longer), [6]);
    }

    #[tokio::test]
    async fn given_a_rolled_back_table_should_not_count_the_appends_left_behind() {
        let (catalog, table) = FlakyCatalog::with_table(&[]).await;
        commit_up_to(&catalog, &table, &ORIGIN, 4).await;
        let kept = current_table(&catalog, &table)
            .await
            .metadata()
            .current_snapshot_id()
            .expect("The append should be the current snapshot");
        commit_up_to(&catalog, &table, &ORIGIN, 9).await;
        let current = current_table(&catalog, &table).await;
        let rolled_back = TableMetadataBuilder::new_from_metadata(current.metadata().clone(), None)
            .set_ref(
                MAIN_BRANCH,
                SnapshotReference::new(kept, SnapshotRetention::branch(None, None, None)),
            )
            .expect("The branch should move back")
            .build()
            .expect("The metadata should build")
            .metadata;
        let rolled_back = Table::builder()
            .metadata(rolled_back)
            .identifier(current.identifier().clone())
            .file_io(current.file_io().clone())
            .runtime(iceberg::Runtime::current())
            .build()
            .expect("The table should build");
        let batch = messages(3..=10);

        let unheld = HeldMessages::default()
            .unheld(&ORIGIN, &rolled_back, Some(&rolled_back), &catalog, &batch)
            .await
            .expect("The current table needs no catalog");

        assert_eq!(offsets(unheld), [5, 6, 7, 8, 9, 10]);
    }

    #[tokio::test]
    async fn given_a_commit_this_sink_made_should_skip_it_without_loading_the_table() {
        let (_, table) = FlakyCatalog::with_table(&[]).await;
        let unreachable = rest_catalog_answering(StatusCode::SERVICE_UNAVAILABLE).await;
        let held = HeldMessages::default();
        held.committed(&ORIGIN, &table, LastMessage::from(&message(4)));
        let batch = messages(3..=5);

        let unheld = held
            .unheld(&ORIGIN, &table, None, &unreachable, &batch)
            .await
            .expect("The known commit should need no catalog");

        assert_eq!(offsets(unheld), [5]);
    }

    #[tokio::test]
    async fn given_a_failed_commit_should_read_the_table_again() {
        let (_, table) = FlakyCatalog::with_table(&[]).await;
        let unreachable = rest_catalog_answering(StatusCode::SERVICE_UNAVAILABLE).await;
        let held = HeldMessages::default();
        held.committed(&ORIGIN, &table, LastMessage::from(&message(4)));
        held.forget(&ORIGIN, &table);

        let error = held
            .unheld(&ORIGIN, &table, None, &unreachable, &messages(3..=5))
            .await
            .expect_err("A forgotten commit should be read from the catalog");

        assert!(!error.is_permanent(), "{error}");
    }

    #[tokio::test]
    async fn given_a_catalog_that_cannot_load_the_table_should_fail_by_the_cause() {
        let (_, table) = FlakyCatalog::with_table(&[]).await;
        for (status, permanent) in [
            (StatusCode::SERVICE_UNAVAILABLE, false),
            (StatusCode::TOO_MANY_REQUESTS, false),
            (StatusCode::FORBIDDEN, true),
            (StatusCode::NOT_FOUND, true),
        ] {
            let catalog = rest_catalog_answering(status).await;

            let error = HeldMessages::default()
                .unheld(&ORIGIN, &table, None, &catalog, &messages(3..=5))
                .await
                .expect_err("The table should not load");

            assert_eq!(error.is_permanent(), permanent, "{status}: {error}");
        }
    }

    #[tokio::test]
    async fn given_a_batch_below_the_newest_append_when_recording_should_keep_the_newest() {
        let (_, table) = FlakyCatalog::with_table(&[]).await;
        let held = HeldMessages::default();
        held.committed(&ORIGIN, &table, LastMessage::from(&message(9)));

        let lower = held.last_to_record(&ORIGIN, &table, LastMessage::from(&message(3)));
        let higher = held.last_to_record(&ORIGIN, &table, LastMessage::from(&message(12)));

        assert_eq!(lower, LastMessage::from(&message(9)));
        assert_eq!(higher, LastMessage::from(&message(12)));
    }

    #[tokio::test]
    async fn given_a_commit_that_landed_but_failed_when_delivered_again_should_not_append_it_twice()
    {
        let (catalog, table) = FlakyCatalog::with_table(&[CommitFailure::ResponseLost]).await;
        let single_attempt = RetryPolicy {
            max_attempts: 1,
            ..FAST_RETRY
        };
        let held = HeldMessages::default();
        let batch = json_messages(0..=2);

        write(
            &catalog,
            &table,
            &batch,
            InvalidRows::Fail,
            single_attempt,
            Some(&held),
        )
        .await
        .expect_err("The lost response should fail the commit");
        let again = write(
            &catalog,
            &table,
            &batch,
            InvalidRows::Fail,
            single_attempt,
            Some(&held),
        )
        .await
        .expect("The batch delivered again should succeed");

        assert_eq!(again.committed_rows, 0, "the table already holds the batch");
        assert_eq!(catalog.snapshot_count(&table).await, 1);
    }

    #[tokio::test]
    async fn given_a_row_that_does_not_match_when_failing_rows_should_commit_the_rows_before_it() {
        let (catalog, table) = FlakyCatalog::with_table(&[]).await;
        let mut batch = json_messages(0..=3);
        batch[2].payload = Payload::Json(simd_json::json!({ "id": "two" }));

        let written = write(
            &catalog,
            &table,
            &batch,
            InvalidRows::Fail,
            FAST_RETRY,
            Some(&HeldMessages::default()),
        )
        .await
        .expect("The rows before the rejected one should commit");

        assert_eq!(written.committed_rows, 2);
        let rejection = written.rejection.expect("The row should be rejected");
        assert_eq!(rejection.offset, 2);
        assert!(rejection.error.is_permanent(), "{}", rejection.error);
        assert!(
            rejection.error.to_string().contains("offset: 2"),
            "{}",
            rejection.error
        );
        assert_eq!(catalog.snapshot_count(&table).await, 1);
    }

    #[tokio::test]
    async fn given_a_payload_without_json_should_reject_it_or_skip_it_as_configured() {
        let (catalog, table) = FlakyCatalog::with_table(&[]).await;
        let mut batch = json_messages(0..=3);
        batch[1].payload = Payload::Proto("id: 1".to_owned());

        let failed = write(
            &catalog,
            &table,
            &batch,
            InvalidRows::Fail,
            FAST_RETRY,
            None,
        )
        .await
        .expect("The rows before the rejected one should commit");
        let skipped = write(
            &catalog,
            &table,
            &batch,
            InvalidRows::Skip,
            FAST_RETRY,
            None,
        )
        .await
        .expect("Skipping should write the other rows");

        assert_eq!(failed.committed_rows, 1);
        assert_eq!(failed.rejection.map(|rejection| rejection.offset), Some(1));
        assert_eq!(skipped.committed_rows, 3);
        assert!(skipped.rejection.is_none());
    }

    #[tokio::test]
    async fn given_at_most_once_should_record_no_delivered_messages_in_the_snapshot() {
        let (catalog, table) = FlakyCatalog::with_table(&[]).await;

        write(
            &catalog,
            &table,
            &json_messages(0..=2),
            InvalidRows::Fail,
            FAST_RETRY,
            None,
        )
        .await
        .expect("The batch should commit");

        let current = current_table(&catalog, &table).await;
        let snapshot = current
            .metadata()
            .current_snapshot()
            .expect("The batch should be appended");
        let properties = &snapshot.summary().additional_properties;
        assert!(properties.contains_key(COMMIT_ID_PROPERTY));
        assert!(
            !properties.keys().any(|key| key.starts_with("iggy.last-")),
            "{properties:?}"
        );
    }

    #[tokio::test]
    async fn given_an_empty_call_should_succeed_without_the_catalog() {
        let (_, table) = FlakyCatalog::with_table(&[]).await;
        let unreachable = rest_catalog_answering(StatusCode::SERVICE_UNAVAILABLE).await;

        let written = write_data(
            &ORIGIN,
            &[],
            &table,
            None,
            &unreachable,
            Schema::Json,
            InvalidRows::Fail,
            FAST_RETRY,
            Some(&HeldMessages::default()),
        )
        .await
        .expect("An empty call has nothing to write");

        assert_eq!(written.committed_rows, 0);
    }

    #[test]
    fn given_load_errors_should_tell_missing_tables_and_outages_apart() {
        let missing = iceberg::Error::new(ErrorKind::TableNotFound, "Table does not exist");
        let unreachable = iceberg::Error::new(ErrorKind::Unexpected, "Connection refused");
        let forbidden = iceberg::Error::new(
            ErrorKind::Unexpected,
            "Received response with unexpected status code",
        )
        .with_context("status", StatusCode::FORBIDDEN.to_string());

        assert!(is_missing(&missing));
        assert!(!is_missing(&forbidden));
        assert!(table_load_error("test.events", &missing).is_permanent());
        assert!(!table_load_error("test.events", &unreachable).is_permanent());
        assert!(table_load_error("test.events", &forbidden).is_permanent());
    }
}
