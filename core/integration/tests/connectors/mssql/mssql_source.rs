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

use crate::connectors::fixtures::{
    MSSQL_ORDERS_INSTANCE, MssqlAllTypesFixture, MssqlCdcDisabledFixture,
    MssqlDuplicateTableFixture, MssqlInvalidCredentialsFixture, MssqlLatestStartFixture,
    MssqlMissingTableFixture, MssqlSourceFixture, MssqlSourceManualScanFixture,
    MssqlUncapturedTableFixture, MssqlUnsupportedTypeFixture,
};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use iggy::prelude::IggyClient;
use iggy_common::MessageClient;
use iggy_common::{Consumer, Identifier, IggyError, PollingStrategy};
use iggy_connector_sdk::api::{ConnectorRuntimeStats, ConnectorStats, ConnectorStatus};
use integration::harness::{ConnectorsRuntimeHandle, TestHarness, seeds};
use integration::iggy_harness;
use reqwest::Client;
use serde_json::{Value, json};
use std::collections::{BTreeMap, HashSet};
use std::path::Path;
use std::time::{Duration, SystemTime};
use tokio::time::{Instant, sleep};

const API_KEY: &str = "test-api-key";
const SOURCE_KEY: &str = "mssql";
const POLL_INTERVAL: Duration = Duration::from_millis(200);
const STATUS_TIMEOUT: Duration = Duration::from_secs(60);
const DELIVERY_TIMEOUT: Duration = Duration::from_secs(180);
// Time for a duplicate or a late row to show up after the expected count is reached.
const SETTLE_TIME: Duration = Duration::from_secs(2);
const STATE_FILE: &str = "source_mssql.state";
const DATABASE_CHANGED_MARKER: &str = "restored, re-created or replaced";
const RECONNECT_IDENTITY_MARKER: &str = "but the connection reaches";
const LSN_GAP_MARKER: &str = "were cleaned up";
// The runtime logs this when the source stops itself through on_batch_result.
const RUNTIME_STOP_MARKER: &str = "batch result could not be applied";
const NO_USABLE_INSTANCE_MARKER: &str = "no capture instance of";
const INSTANCE_MISSING_MARKER: &str = "no longer exists";
const LOGIN_FAILED_MARKER: &str = "Login failed";
const TABLE_NOT_FOUND_MARKER: &str = "table dbo.missing does not exist";
const NOT_CAPTURED_MARKER: &str = "has no CDC capture instance";
const DUPLICATE_TABLE_MARKER: &str = "all resolve to";
const UNSUPPORTED_TYPE_MARKER: &str = "columns of unsupported types";
const CDC_NOT_ENABLED_MARKER: &str = "CDC is not enabled";
const STUCK_MARKER: &str = "is stuck at start_lsn";
// The SDK logs this for every poll that returns an error.
const POLL_FAILURE_MARKER: &str = "Failed to poll messages";

#[derive(Debug)]
struct Received {
    id: u128,
    event: Value,
}

impl Received {
    fn table(&self) -> &str {
        self.event["source"]["table"].as_str().unwrap_or_default()
    }

    fn op(&self) -> &str {
        self.event["op"].as_str().unwrap_or_default()
    }
}

// The idle watermark must not skip rows the capture job has not written yet, under concurrent load.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mssql/source.toml")),
    seed = seeds::connector_stream
)]
async fn changes_committed_while_capture_job_runs_are_persisted_once(
    harness: &TestHarness,
    fixture: MssqlSourceFixture,
) {
    const ORDERS: usize = 200;
    const CUSTOMERS: usize = 300;
    const UPDATE_ROUNDS: usize = 5;
    let runtime = harness.connectors_runtime().expect("connectors runtime");
    let http = Client::new();
    wait_for_status(&http, runtime, ConnectorStatus::Running).await;

    let orders = async {
        let mut client = fixture.client().await.expect("connect");
        for id in 0..ORDERS {
            client
                .execute(
                    "INSERT INTO dbo.orders (id, amount) VALUES (@P1, 1.50)",
                    &[&(id as i32)],
                )
                .await
                .expect("insert order");
        }
    };
    let customers = async {
        fixture
            .execute(&format!(
                "WITH numbers AS (SELECT TOP ({CUSTOMERS}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS id \
                 FROM sys.all_objects) INSERT INTO dbo.customers (id, visits) SELECT id, 0 FROM numbers"
            ))
            .await;
        for _ in 0..UPDATE_ROUNDS {
            fixture
                .execute("UPDATE dbo.customers SET visits = visits + 1")
                .await;
        }
    };
    tokio::join!(orders, customers);

    let expected = ORDERS + CUSTOMERS + CUSTOMERS * UPDATE_ROUNDS;
    let client = harness.root_client().await.expect("root client");
    let received = receive(&client, "mssql_visibility", expected).await;

    assert_unique_ids(&received);
    let counts = count_by_table_and_op(&received);
    assert_eq!(
        counts,
        BTreeMap::from([
            (("customers".to_string(), "insert".to_string()), CUSTOMERS),
            (
                ("customers".to_string(), "update".to_string()),
                CUSTOMERS * UPDATE_ROUNDS
            ),
            (("orders".to_string(), "insert".to_string()), ORDERS),
        ]),
        "every change must arrive exactly once"
    );
    wait_for_status(&http, runtime, ConnectorStatus::Running).await;
}

// Cleanup during an Iggy outage deletes undelivered changes, which must stop the source.
#[iggy_harness(
    cluster_nodes = 1,
    server(connectors_runtime(config_path = "tests/connectors/mssql/source.toml")),
    seed = seeds::connector_stream
)]
async fn cleanup_past_undelivered_changes_stops_running_source(
    harness: &mut TestHarness,
    fixture: MssqlSourceFixture,
) {
    const ORDERS: usize = 20;
    const CLEANED_THROUGH: usize = 15;
    harness
        .server_mut()
        .stop_dependents()
        .expect("stop connectors runtime");
    harness
        .server_mut()
        .connectors_runtime_mut()
        .expect("connectors runtime")
        .set_iggy_connection_options("reconnection_retries=0");
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("start connectors runtime");
    wait_until_running(harness).await;

    harness.kill_node(0).expect("kill Iggy server");
    for id in 1..=ORDERS {
        fixture
            .execute(&format!(
                "INSERT INTO dbo.orders (id, amount) VALUES ({id}, 2.00)"
            ))
            .await;
    }
    fixture
        .wait_for_change_rows(MSSQL_ORDERS_INSTANCE, ORDERS)
        .await;
    let low_water_mark = fixture
        .change_lsn(MSSQL_ORDERS_INSTANCE, CLEANED_THROUGH)
        .await;
    fixture
        .cleanup_below(MSSQL_ORDERS_INSTANCE, &low_water_mark)
        .await;

    let runtime = harness.connectors_runtime().expect("connectors runtime");
    wait_for_log(runtime, LSN_GAP_MARKER).await;
    wait_for_log(runtime, RUNTIME_STOP_MARKER).await;
}

// The backup postdates the checkpoint, so only recovery_fork_guid reveals the restore.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mssql/source.toml")),
    seed = seeds::connector_stream
)]
async fn restored_database_fails_source_initialization(
    harness: &mut TestHarness,
    fixture: MssqlSourceFixture,
) {
    deliver_three_orders(harness, &fixture, "mssql_restore").await;
    let fork_before = fixture.recovery_fork_guid().await;

    harness
        .server_mut()
        .stop_dependents()
        .expect("stop connectors runtime");
    fixture.backup_and_restore().await;
    let fork_after = fixture.recovery_fork_guid().await;
    assert_ne!(
        fork_before, fork_after,
        "a restore must start a new recovery fork, or the identity check cannot see it"
    );
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("start connectors runtime");

    let runtime = harness.connectors_runtime().expect("connectors runtime");
    wait_for_status(&Client::new(), runtime, ConnectorStatus::Error).await;
    assert_logs_contain(runtime, DATABASE_CHANGED_MARKER);
}

// A restore under the running connector drops its session. The reconnect must notice the new recovery fork.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mssql/source.toml")),
    seed = seeds::connector_stream
)]
async fn database_restored_while_running_stops_source_after_reconnect(
    harness: &TestHarness,
    fixture: MssqlSourceFixture,
) {
    deliver_three_orders(harness, &fixture, "mssql_restore_running").await;

    fixture.backup_and_restore().await;

    // Only the reconnect's identity check writes this. A poll on the old session would report the checkpoint.
    assert_stops_with(harness, RECONNECT_IDENTITY_MARKER).await;
    let runtime = harness.connectors_runtime().expect("connectors runtime");
    wait_for_log(runtime, RUNTIME_STOP_MARKER).await;
}

// Re-create detection: same name, new database.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mssql/source.toml")),
    seed = seeds::connector_stream
)]
async fn recreated_database_fails_source_initialization(
    harness: &mut TestHarness,
    fixture: MssqlSourceFixture,
) {
    deliver_three_orders(harness, &fixture, "mssql_recreate").await;
    let fork_before = fixture.recovery_fork_guid().await;

    harness
        .server_mut()
        .stop_dependents()
        .expect("stop connectors runtime");
    fixture.recreate_database().await;
    let fork_after = fixture.recovery_fork_guid().await;
    assert_ne!(
        fork_before, fork_after,
        "a re-created database must have a new recovery fork"
    );
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("start connectors runtime");

    let runtime = harness.connectors_runtime().expect("connectors runtime");
    wait_for_status(&Client::new(), runtime, ConnectorStatus::Error).await;
    assert_logs_contain(runtime, DATABASE_CHANGED_MARKER);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mssql/source.toml")),
    seed = seeds::connector_stream
)]
async fn every_supported_type_is_persisted_with_its_json_mapping(
    harness: &TestHarness,
    fixture: MssqlAllTypesFixture,
) {
    wait_until_running(harness).await;
    fixture
        .execute(
            "INSERT INTO dbo.types (id, c_tinyint, c_smallint, c_bigint, c_bit, c_decimal, c_numeric, c_money, \
             c_smallmoney, c_real, c_float, c_char, c_varchar, c_nchar, c_nvarchar, c_text, c_ntext, c_date, \
             c_time, c_datetime, c_datetime2, c_smalldatetime, c_datetimeoffset, c_uniqueidentifier, c_binary, \
             c_varbinary, c_image, c_xml, c_geography, c_geometry, c_hierarchyid) VALUES ( \
             1, 255, -32768, 9223372036854775807, 1, 10.50, 1234567890123456789012345678.0123456789, 12.3456, \
             214748.3647, 1.5, 2.25, 'ab', 'café', N'ab', N'zoë', 'legacy text', N'legacy ntext', '2026-10-20', \
             '23:59:59.9999999', '2026-10-20T10:00:00.997', '2026-10-20T10:00:00.1234567', '2026-10-20T10:00:00', \
             '2026-10-20T10:00:00.123-07:00', '6F9619FF-8B86-D011-B42D-00C04FC964FF', 0x01020304, 0xABABAB, 0xFFFE, \
             N'<a b=\"1\">text</a>', geography::Point(47.65, -122.35, 4326), geometry::Point(1, 2, 0), \
             hierarchyid::Parse('/1/'))",
        )
        .await;

    let received = receive_from(harness, "mssql_types", 1).await;

    assert_eq!(received.len(), 1);
    let after = &received[0].event["after"];
    let expected = json!({
        "id": 1,
        "c_tinyint": 255,
        "c_smallint": -32768,
        "c_bigint": 9223372036854775807_i64,
        "c_bit": true,
        "c_decimal": "10.50",
        "c_numeric": "1234567890123456789012345678.0123456789",
        "c_money": "12.3456",
        "c_smallmoney": "214748.3647",
        "c_real": 1.5,
        "c_float": 2.25,
        "c_char": "ab   ",
        "c_varchar": "café",
        "c_nchar": "ab  ",
        "c_nvarchar": "zoë",
        "c_text": "legacy text",
        "c_ntext": "legacy ntext",
        "c_date": "2026-10-20",
        "c_time": "23:59:59.9999999",
        "c_datetime": "2026-10-20T10:00:00.997",
        "c_datetime2": "2026-10-20T10:00:00.1234567",
        "c_smalldatetime": "2026-10-20T10:00:00",
        "c_datetimeoffset": "2026-10-20T10:00:00.123-07:00",
        "c_uniqueidentifier": "6f9619ff-8b86-d011-b42d-00c04fc964ff",
        "c_binary": "AQIDBA==",
        "c_varbinary": "q6ur",
        "c_image": "//4=",
        "c_xml": "<a b=\"1\">text</a>",
    });
    assert!(
        after.get("c_computed").is_none(),
        "computed columns are always NULL in CDC, so they are left out"
    );
    for (column, value) in expected.as_object().expect("object") {
        assert_eq!(&after[column], value, "column {column}");
    }
    // The serialized forms of these vary with the value, so only their encoding is checked.
    for column in ["c_rowversion", "c_geography", "c_geometry", "c_hierarchyid"] {
        let encoded = after[column].as_str().unwrap_or_default();
        assert!(
            !encoded.is_empty() && BASE64.decode(encoded).is_ok(),
            "column {column} should be base64, got {}",
            after[column]
        );
    }
}

// SQL Server accepts an unpaired UTF-16 surrogate in nvarchar, and the driver refuses to decode it.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mssql/source.toml")),
    seed = seeds::connector_stream
)]
async fn undecodable_value_holds_progress_and_logs_once(
    harness: &TestHarness,
    fixture: MssqlSourceFixture,
) {
    wait_until_running(harness).await;
    fixture
        .execute("INSERT INTO dbo.orders (id, amount, note) VALUES (1, 1.00, NCHAR(0xD800))")
        .await;
    // A source that skipped the bad change would deliver this one.
    fixture
        .execute("INSERT INTO dbo.orders (id, amount) VALUES (2, 2.00)")
        .await;

    let runtime = harness.connectors_runtime().expect("connectors runtime");
    wait_for_log(runtime, STUCK_MARKER).await;
    // Many polls run in this window, at a 100 ms interval.
    sleep(Duration::from_secs(3)).await;

    let (stdout, stderr) = runtime.collect_logs();
    let logs = format!("{stdout}\n{stderr}");
    assert_eq!(logs.matches(STUCK_MARKER).count(), 1, "{logs}");
    assert_eq!(logs.matches(POLL_FAILURE_MARKER).count(), 0, "{logs}");
    let delivered = drain(
        &harness.root_client().await.expect("root client"),
        "mssql_stuck",
    )
    .await;
    assert!(
        delivered.is_empty(),
        "the valid change after the stuck one must not be delivered"
    );
    wait_until_running(harness).await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mssql/source.toml")),
    seed = seeds::connector_stream
)]
async fn insert_update_delete_are_persisted_to_iggy(
    harness: &TestHarness,
    fixture: MssqlSourceFixture,
) {
    wait_until_running(harness).await;
    fixture
        .execute("INSERT INTO dbo.orders (id, amount) VALUES (1, 10.50)")
        .await;
    fixture
        .execute("UPDATE dbo.orders SET amount = 11.00 WHERE id = 1")
        .await;
    fixture.execute("DELETE FROM dbo.orders WHERE id = 1").await;

    let received = receive_from(harness, "mssql_crud", 3).await;

    assert_eq!(received.len(), 3);
    assert_unique_ids(&received);
    let [insert, update, delete] = &received[..] else {
        unreachable!()
    };
    assert_eq!(insert.op(), "insert");
    assert_eq!(insert.event["before"], Value::Null);
    assert_eq!(
        insert.event["after"],
        json!({"id": 1, "amount": "10.50", "note": null})
    );
    assert_eq!(update.op(), "update");
    assert_eq!(update.event["before"]["amount"], json!("10.50"));
    assert_eq!(update.event["after"]["amount"], json!("11.00"));
    assert_eq!(delete.op(), "delete");
    assert_eq!(delete.event["before"]["amount"], json!("11.00"));
    assert_eq!(delete.event["after"], Value::Null);
    for message in &received {
        assert_eq!(message.event["source"]["schema"], json!("dbo"));
        assert_eq!(message.table(), "orders");
        assert!(message.event["commit_time"].is_string());
    }
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mssql/source.toml")),
    seed = seeds::connector_stream
)]
async fn primary_key_update_is_persisted_as_delete_and_insert(
    harness: &TestHarness,
    fixture: MssqlSourceFixture,
) {
    wait_until_running(harness).await;
    fixture
        .execute("INSERT INTO dbo.orders (id, amount) VALUES (1, 5.00)")
        .await;
    fixture
        .execute("UPDATE dbo.orders SET id = 100 WHERE id = 1")
        .await;

    let received = receive_from(harness, "mssql_pk_update", 3).await;

    assert_eq!(received.len(), 3);
    assert_unique_ids(&received);
    assert_eq!(received[1].op(), "delete");
    assert_eq!(received[1].event["before"]["id"], json!(1));
    assert_eq!(received[2].op(), "insert");
    assert_eq!(received[2].event["after"]["id"], json!(100));
    assert_eq!(received[1].event["lsn"], received[2].event["lsn"]);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mssql/source.toml")),
    seed = seeds::connector_stream
)]
async fn cross_table_transaction_is_persisted_in_commit_order(
    harness: &TestHarness,
    fixture: MssqlSourceFixture,
) {
    wait_until_running(harness).await;
    fixture
        .execute(
            "BEGIN TRANSACTION; \
             INSERT INTO dbo.orders (id, amount) VALUES (1, 1.00); \
             INSERT INTO dbo.customers (id, visits) VALUES (1, 1); \
             INSERT INTO dbo.orders (id, amount) VALUES (2, 2.00); \
             COMMIT",
        )
        .await;

    let received = receive_from(harness, "mssql_cross_table", 3).await;

    let order: Vec<(&str, &Value)> = received
        .iter()
        .map(|message| (message.table(), &message.event["after"]["id"]))
        .collect();
    assert_eq!(
        order,
        [
            ("orders", &json!(1)),
            ("customers", &json!(1)),
            ("orders", &json!(2))
        ]
    );
    assert!(
        received
            .iter()
            .all(|message| message.event["lsn"] == received[0].event["lsn"]),
        "one transaction shares one commit LSN"
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mssql/source.toml")),
    seed = seeds::connector_stream
)]
async fn unchanged_lob_is_listed_as_unavailable_in_before_image(
    harness: &TestHarness,
    fixture: MssqlSourceFixture,
) {
    wait_until_running(harness).await;
    fixture
        .execute(
            "INSERT INTO dbo.orders (id, amount, note) VALUES (1, 1.00, REPLICATE(CAST(N'x' AS NVARCHAR(MAX)), 9000))",
        )
        .await;
    fixture
        .execute("UPDATE dbo.orders SET amount = 2.00 WHERE id = 1")
        .await;

    let received = receive_from(harness, "mssql_lob", 2).await;

    assert_eq!(received.len(), 2);
    let update = &received[1];
    assert_eq!(update.op(), "update");
    assert_eq!(update.event["unavailable_columns"], json!(["note"]));
    assert!(update.event["before"].get("note").is_none());
    assert_eq!(
        update.event["after"]["note"].as_str().map(str::len),
        Some(9000)
    );
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mssql/source.toml")),
    seed = seeds::connector_stream
)]
async fn quoted_table_name_is_persisted_to_iggy(
    harness: &TestHarness,
    fixture: MssqlSourceFixture,
) {
    wait_until_running(harness).await;
    fixture
        .execute("INSERT INTO sales.[order items] (line_no, qty) VALUES (1, 3)")
        .await;

    let received = receive_from(harness, "mssql_quoted", 1).await;

    assert_eq!(received.len(), 1);
    assert_eq!(received[0].event["source"]["schema"], json!("sales"));
    assert_eq!(received[0].table(), "order items");
    assert_eq!(received[0].event["after"], json!({"line_no": 1, "qty": 3}));
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mssql/source.toml")),
    seed = seeds::connector_stream
)]
async fn connector_restart_resumes_from_checkpoint_without_duplicates(
    harness: &mut TestHarness,
    fixture: MssqlSourceFixture,
) {
    let mut received = deliver_three_orders(harness, &fixture, "mssql_restart").await;
    harness
        .server_mut()
        .stop_dependents()
        .expect("stop connectors runtime");
    fixture
        .execute("INSERT INTO dbo.orders (id, amount) VALUES (4, 4.00), (5, 5.00)")
        .await;
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("start connectors runtime");
    wait_until_running(harness).await;

    received.extend(receive_from(harness, "mssql_restart", 2).await);

    assert_unique_ids(&received);
    let ids: Vec<&Value> = received
        .iter()
        .map(|message| &message.event["after"]["id"])
        .collect();
    assert_eq!(ids, [&json!(1), &json!(2), &json!(3), &json!(4), &json!(5)]);
}

#[iggy_harness(
    cluster_nodes = 1,
    server(connectors_runtime(config_path = "tests/connectors/mssql/source.toml")),
    seed = seeds::connector_stream
)]
async fn iggy_restart_redelivers_changes_once_without_runtime_restart(
    harness: &mut TestHarness,
    fixture: MssqlSourceFixture,
) {
    harness
        .server_mut()
        .stop_dependents()
        .expect("stop connectors runtime");
    harness
        .server_mut()
        .connectors_runtime_mut()
        .expect("connectors runtime")
        .set_iggy_connection_options("reconnection_retries=0");
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("start connectors runtime");
    wait_until_running(harness).await;
    let http = Client::new();
    let runtime = harness.connectors_runtime().expect("connectors runtime");
    let errors_before = source_stats(&http, runtime)
        .await
        .expect("source stats")
        .errors;

    harness.kill_node(0).expect("kill Iggy server");
    fixture
        .execute("INSERT INTO dbo.orders (id, amount) VALUES (1, 1.00), (2, 2.00), (3, 3.00)")
        .await;
    let runtime = harness.connectors_runtime().expect("connectors runtime");
    wait_for_errors(&http, runtime, errors_before + 2).await;

    // Only the Iggy node restarts. The connectors runtime keeps running and redelivers on its own.
    harness
        .restart_node(0)
        .expect("restart only the Iggy server");

    let received = receive_from(harness, "mssql_iggy_failure", 3).await;

    assert_eq!(
        received.len(),
        3,
        "each NACKed change must arrive exactly once"
    );
    assert_unique_ids(&received);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mssql/source.toml")),
    seed = seeds::connector_stream
)]
async fn forced_disconnect_recovers_without_loss_or_duplicates(
    harness: &TestHarness,
    fixture: MssqlSourceFixture,
) {
    let mut received = deliver_three_orders(harness, &fixture, "mssql_disconnect").await;
    let killed = fixture.kill_other_sessions().await;
    assert!(killed > 0, "the connector's session should be killed");
    fixture
        .execute("INSERT INTO dbo.orders (id, amount) VALUES (4, 4.00), (5, 5.00)")
        .await;

    received.extend(receive_from(harness, "mssql_disconnect", 2).await);

    assert_eq!(received.len(), 5);
    assert_unique_ids(&received);
    wait_until_running(harness).await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mssql/source.toml")),
    seed = seeds::connector_stream
)]
async fn transaction_larger_than_batch_size_is_split_without_loss(
    harness: &TestHarness,
    fixture: MssqlSourceManualScanFixture,
) {
    const ROWS: usize = 35;
    wait_until_running(harness).await;
    fixture
        .execute(&format!(
            "WITH numbers AS (SELECT TOP ({ROWS}) ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS id \
             FROM sys.all_objects) INSERT INTO dbo.orders (id, amount) SELECT id, 1.00 FROM numbers"
        ))
        .await;
    fixture.scan().await;

    let received = receive_from(harness, "mssql_split", ROWS).await;

    assert_eq!(received.len(), ROWS);
    assert_unique_ids(&received);
    // 35 changes at a batch size of 10 need at least four batches.
    let (stdout, _) = harness
        .connectors_runtime()
        .expect("connectors runtime")
        .collect_logs();
    let batches = stdout
        .lines()
        .filter(|line| line.contains(" polled ") && line.contains(" changes"))
        .count();
    assert!(batches >= 4, "expected at least 4 batches, got {batches}");
}

// The documented rotation: create _v2, commit and ACK a change, then drop _v1.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mssql/source.toml")),
    seed = seeds::connector_stream
)]
async fn second_capture_instance_is_read_after_restart_without_loss(
    harness: &mut TestHarness,
    fixture: MssqlSourceFixture,
) {
    let mut received = deliver_three_orders(harness, &fixture, "mssql_rotation").await;
    fixture.add_orders_instance("dbo_orders_v2").await;
    let runtime = harness.connectors_runtime().expect("connectors runtime");
    let saved_before = modified(&runtime.state_path().join(STATE_FILE));
    fixture
        .execute("INSERT INTO dbo.orders (id, amount) VALUES (4, 4.00)")
        .await;
    received.extend(receive_from(harness, "mssql_rotation", 1).await);
    wait_for_checkpoint_save(harness, saved_before).await;

    harness
        .server_mut()
        .stop_dependents()
        .expect("stop connectors runtime");
    fixture.drop_orders_instance(MSSQL_ORDERS_INSTANCE).await;
    fixture
        .execute("INSERT INTO dbo.orders (id, amount) VALUES (5, 5.00)")
        .await;
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("start connectors runtime");
    wait_until_running(harness).await;

    received.extend(receive_from(harness, "mssql_rotation", 1).await);

    assert_eq!(received.len(), 5, "every change must arrive exactly once");
    assert_unique_ids(&received);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mssql/source.toml")),
    seed = seeds::connector_stream
)]
async fn second_capture_instance_above_checkpoint_keeps_recorded_instance(
    harness: &mut TestHarness,
    fixture: MssqlSourceFixture,
) {
    let mut received = deliver_three_orders(harness, &fixture, "mssql_keep_instance").await;
    harness
        .server_mut()
        .stop_dependents()
        .expect("stop connectors runtime");
    fixture.add_orders_instance("dbo_orders_v2").await;
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("start connectors runtime");
    wait_until_running(harness).await;
    fixture
        .execute("INSERT INTO dbo.orders (id, amount) VALUES (4, 4.00)")
        .await;

    received.extend(receive_from(harness, "mssql_keep_instance", 1).await);

    assert_eq!(received.len(), 4);
    assert_unique_ids(&received);
    wait_until_running(harness).await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mssql/source.toml")),
    seed = seeds::connector_stream
)]
async fn dropping_first_instance_before_ack_fails_source_initialization(
    harness: &mut TestHarness,
    fixture: MssqlSourceFixture,
) {
    deliver_three_orders(harness, &fixture, "mssql_early_drop").await;
    harness
        .server_mut()
        .stop_dependents()
        .expect("stop connectors runtime");
    fixture.add_orders_instance("dbo_orders_v2").await;
    fixture.drop_orders_instance(MSSQL_ORDERS_INSTANCE).await;
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("start connectors runtime");

    assert_stops_with(harness, NO_USABLE_INSTANCE_MARKER).await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mssql/source.toml")),
    seed = seeds::connector_stream
)]
async fn stale_checkpoint_fails_source_initialization(
    harness: &mut TestHarness,
    fixture: MssqlSourceFixture,
) {
    deliver_three_orders(harness, &fixture, "mssql_stale").await;
    harness
        .server_mut()
        .stop_dependents()
        .expect("stop connectors runtime");
    fixture
        .execute("INSERT INTO dbo.orders (id, amount) VALUES (4, 4.00), (5, 5.00), (6, 6.00)")
        .await;
    fixture
        .execute("INSERT INTO dbo.orders (id, amount) VALUES (7, 7.00)")
        .await;
    fixture.wait_for_change_rows(MSSQL_ORDERS_INSTANCE, 7).await;
    let low_water_mark = fixture.change_lsn(MSSQL_ORDERS_INSTANCE, 7).await;
    fixture
        .cleanup_below(MSSQL_ORDERS_INSTANCE, &low_water_mark)
        .await;
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("start connectors runtime");

    assert_stops_with(harness, LSN_GAP_MARKER).await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mssql/source.toml")),
    seed = seeds::connector_stream
)]
async fn dropped_capture_instance_stops_running_source(
    harness: &TestHarness,
    fixture: MssqlSourceFixture,
) {
    deliver_three_orders(harness, &fixture, "mssql_instance_dropped").await;
    fixture.drop_orders_instance(MSSQL_ORDERS_INSTANCE).await;

    assert_stops_with(harness, INSTANCE_MISSING_MARKER).await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mssql/source.toml")),
    seed = seeds::connector_stream
)]
async fn cdc_disabled_while_running_stops_source(
    harness: &TestHarness,
    fixture: MssqlSourceFixture,
) {
    deliver_three_orders(harness, &fixture, "mssql_cdc_disabled").await;

    fixture.disable_cdc().await;

    assert_stops_with(harness, CDC_NOT_ENABLED_MARKER).await;
    let runtime = harness.connectors_runtime().expect("connectors runtime");
    wait_for_log(runtime, RUNTIME_STOP_MARKER).await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mssql/source.toml")),
    seed = seeds::connector_stream
)]
async fn database_without_cdc_fails_source_initialization(
    harness: &TestHarness,
    _fixture: MssqlCdcDisabledFixture,
) {
    assert_stops_with(harness, CDC_NOT_ENABLED_MARKER).await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mssql/source.toml")),
    seed = seeds::connector_stream
)]
async fn invalid_credentials_fail_source_initialization(
    harness: &TestHarness,
    _fixture: MssqlInvalidCredentialsFixture,
) {
    assert_stops_with(harness, LOGIN_FAILED_MARKER).await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mssql/source.toml")),
    seed = seeds::connector_stream
)]
async fn missing_table_fails_source_initialization(
    harness: &TestHarness,
    _fixture: MssqlMissingTableFixture,
) {
    assert_stops_with(harness, TABLE_NOT_FOUND_MARKER).await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mssql/source.toml")),
    seed = seeds::connector_stream
)]
async fn table_without_cdc_fails_source_initialization(
    harness: &TestHarness,
    _fixture: MssqlUncapturedTableFixture,
) {
    assert_stops_with(harness, NOT_CAPTURED_MARKER).await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mssql/source.toml")),
    seed = seeds::connector_stream
)]
async fn two_names_for_one_table_fail_source_initialization(
    harness: &TestHarness,
    _fixture: MssqlDuplicateTableFixture,
) {
    assert_stops_with(harness, DUPLICATE_TABLE_MARKER).await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mssql/source.toml")),
    seed = seeds::connector_stream
)]
async fn unsupported_column_type_fails_source_initialization(
    harness: &TestHarness,
    _fixture: MssqlUnsupportedTypeFixture,
) {
    assert_stops_with(harness, UNSUPPORTED_TYPE_MARKER).await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mssql/source.toml")),
    seed = seeds::connector_stream
)]
async fn latest_start_position_skips_changes_committed_before_first_start(
    harness: &TestHarness,
    fixture: MssqlLatestStartFixture,
) {
    wait_until_running(harness).await;
    fixture
        .execute("INSERT INTO dbo.orders (id, amount) VALUES (100, 1.00)")
        .await;

    let received = receive_from(harness, "mssql_latest", 1).await;

    assert_eq!(received.len(), 1, "the seeded orders must be skipped");
    assert_eq!(received[0].event["after"]["id"], json!(100));
}

// The saved checkpoint must win over `latest` on the next start, or changes made while stopped are skipped.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mssql/source.toml")),
    seed = seeds::connector_stream
)]
async fn latest_start_position_is_not_applied_again_after_restart(
    harness: &mut TestHarness,
    fixture: MssqlLatestStartFixture,
) {
    let runtime = harness.connectors_runtime().expect("connectors runtime");
    wait_for_status(&Client::new(), runtime, ConnectorStatus::Running).await;
    let state_file = runtime.state_path().join(STATE_FILE);
    wait_for_checkpoint_save(harness, None).await;
    let saved_before = modified(&state_file);
    fixture
        .execute("INSERT INTO dbo.orders (id, amount) VALUES (100, 1.00)")
        .await;
    let mut received = receive_from(harness, "mssql_latest_restart", 1).await;
    wait_for_checkpoint_save(harness, saved_before).await;

    harness
        .server_mut()
        .stop_dependents()
        .expect("stop connectors runtime");
    fixture
        .execute("INSERT INTO dbo.orders (id, amount) VALUES (101, 1.00)")
        .await;
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("start connectors runtime");
    wait_until_running(harness).await;
    received.extend(receive_from(harness, "mssql_latest_restart", 1).await);

    let ids: Vec<&Value> = received
        .iter()
        .map(|message| &message.event["after"]["id"])
        .collect();
    assert_eq!(ids, [&json!(100), &json!(101)]);
}

// Every row of a multi-row primary key update gets its own seqval, so ids stay distinct.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/mssql/source.toml")),
    seed = seeds::connector_stream
)]
async fn multi_row_primary_key_update_is_persisted_with_distinct_ids(
    harness: &TestHarness,
    fixture: MssqlSourceFixture,
) {
    let mut received = deliver_three_orders(harness, &fixture, "mssql_multi_pk").await;
    fixture.execute("UPDATE dbo.orders SET id = id + 100").await;

    received.extend(receive_from(harness, "mssql_multi_pk", 6).await);

    assert_eq!(received.len(), 9);
    assert_unique_ids(&received);
    let counts = count_by_table_and_op(&received[3..]);
    assert_eq!(
        counts,
        BTreeMap::from([
            (("orders".to_string(), "delete".to_string()), 3),
            (("orders".to_string(), "insert".to_string()), 3),
        ])
    );
}

/// Inserts three orders and waits until they are in Iggy and the checkpoint is saved.
async fn deliver_three_orders(
    harness: &TestHarness,
    fixture: &MssqlSourceFixture,
    consumer: &str,
) -> Vec<Received> {
    let runtime = harness.connectors_runtime().expect("connectors runtime");
    wait_for_status(&Client::new(), runtime, ConnectorStatus::Running).await;
    let state_file = runtime.state_path().join(STATE_FILE);
    // A fresh start saves its open() changes on the first poll, so wait for it before the save below.
    wait_for_checkpoint_save(harness, None).await;
    let saved_before = modified(&state_file);
    fixture
        .execute("INSERT INTO dbo.orders (id, amount) VALUES (1, 1.00), (2, 2.00), (3, 3.00)")
        .await;
    let client = harness.root_client().await.expect("root client");
    let received = receive(&client, consumer, 3).await;
    assert_eq!(received.len(), 3, "three inserts should arrive");
    wait_for_checkpoint_save(harness, saved_before).await;
    received
}

/// The runtime saves state after the send, so a test that stops the runtime waits for the save first.
async fn wait_for_checkpoint_save(harness: &TestHarness, saved_before: Option<SystemTime>) {
    let runtime = harness.connectors_runtime().expect("connectors runtime");
    let state_file = runtime.state_path().join(STATE_FILE);
    let deadline = Instant::now() + STATUS_TIMEOUT;
    while modified(&state_file) == saved_before {
        assert!(Instant::now() < deadline, "the checkpoint was not saved");
        sleep(POLL_INTERVAL).await;
    }
}

fn modified(path: &Path) -> Option<SystemTime> {
    std::fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .ok()
}

async fn receive_from(harness: &TestHarness, consumer: &str, want: usize) -> Vec<Received> {
    let client = harness.root_client().await.expect("root client");
    receive(&client, consumer, want).await
}

async fn wait_until_running(harness: &TestHarness) {
    let runtime = harness.connectors_runtime().expect("connectors runtime");
    wait_for_status(&Client::new(), runtime, ConnectorStatus::Running).await;
}

/// Waits for the source to reach `Error` and checks the runtime logs for the reason.
async fn assert_stops_with(harness: &TestHarness, marker: &str) {
    let runtime = harness.connectors_runtime().expect("connectors runtime");
    wait_for_status(&Client::new(), runtime, ConnectorStatus::Error).await;
    assert_logs_contain(runtime, marker);
}

async fn wait_for_errors(http: &Client, runtime: &ConnectorsRuntimeHandle, minimum: u64) {
    let deadline = Instant::now() + STATUS_TIMEOUT;
    while Instant::now() < deadline {
        if source_stats(http, runtime)
            .await
            .is_some_and(|stats| stats.errors >= minimum)
        {
            return;
        }
        sleep(POLL_INTERVAL).await;
    }
    panic!("MSSQL source did not record {minimum} errors within {STATUS_TIMEOUT:?}");
}

/// Polls until `want` changes arrived, then waits [`SETTLE_TIME`] for anything extra.
async fn receive(client: &IggyClient, consumer: &str, want: usize) -> Vec<Received> {
    let deadline = Instant::now() + DELIVERY_TIMEOUT;
    let mut received = Vec::with_capacity(want);
    while received.len() < want && Instant::now() < deadline {
        match poll_once(client, consumer).await {
            Ok(polled) if !polled.is_empty() => received.extend(polled),
            _ => sleep(POLL_INTERVAL).await,
        }
    }
    assert!(
        received.len() >= want,
        "expected {want} changes, got {} after {DELIVERY_TIMEOUT:?}",
        received.len()
    );
    sleep(SETTLE_TIME).await;
    received.extend(drain(client, consumer).await);
    received
}

/// Reads until an empty poll. A failed poll is retried, so an error cannot end the drain early and hide a duplicate.
async fn drain(client: &IggyClient, consumer: &str) -> Vec<Received> {
    let deadline = Instant::now() + DELIVERY_TIMEOUT;
    let mut received = Vec::new();
    loop {
        match poll_once(client, consumer).await {
            Ok(polled) if polled.is_empty() => return received,
            Ok(polled) => received.extend(polled),
            Err(error) => {
                assert!(
                    Instant::now() < deadline,
                    "polling Iggy kept failing: {error}"
                );
                sleep(POLL_INTERVAL).await;
            }
        }
    }
}

async fn poll_once(client: &IggyClient, consumer: &str) -> Result<Vec<Received>, IggyError> {
    let stream: Identifier = seeds::names::STREAM.try_into().expect("stream id");
    let topic: Identifier = seeds::names::TOPIC.try_into().expect("topic id");
    let consumer: Identifier = consumer.try_into().expect("consumer id");
    let polled = client
        .poll_messages(
            &stream,
            &topic,
            None,
            &Consumer::new(consumer),
            &PollingStrategy::next(),
            500,
            true,
        )
        .await?;
    Ok(polled
        .messages
        .into_iter()
        .map(|message| Received {
            id: message.header.id,
            event: serde_json::from_slice(&message.payload).expect("change event JSON"),
        })
        .collect())
}

async fn source_stats(http: &Client, runtime: &ConnectorsRuntimeHandle) -> Option<ConnectorStats> {
    let stats = http
        .get(format!("{}/stats", runtime.http_url()))
        .header("api-key", API_KEY)
        .send()
        .await
        .ok()?
        .json::<ConnectorRuntimeStats>()
        .await
        .ok()?;
    stats
        .connectors
        .into_iter()
        .find(|connector| connector.key == SOURCE_KEY)
}

async fn wait_for_status(
    http: &Client,
    runtime: &ConnectorsRuntimeHandle,
    expected: ConnectorStatus,
) {
    let deadline = Instant::now() + STATUS_TIMEOUT;
    let mut last = None;
    while Instant::now() < deadline {
        last = source_stats(http, runtime).await.map(|stats| stats.status);
        if last == Some(expected) {
            return;
        }
        sleep(POLL_INTERVAL).await;
    }
    let (stdout, stderr) = runtime.collect_logs();
    panic!(
        "MSSQL source did not reach {expected:?} within {STATUS_TIMEOUT:?}, last status {last:?}\n{stdout}\n{stderr}"
    );
}

async fn wait_for_log(runtime: &ConnectorsRuntimeHandle, marker: &str) {
    let deadline = Instant::now() + STATUS_TIMEOUT;
    while Instant::now() < deadline {
        let (stdout, stderr) = runtime.collect_logs();
        if stdout.contains(marker) || stderr.contains(marker) {
            return;
        }
        sleep(POLL_INTERVAL).await;
    }
    assert_logs_contain(runtime, marker);
}

fn assert_logs_contain(runtime: &ConnectorsRuntimeHandle, marker: &str) {
    let (stdout, stderr) = runtime.collect_logs();
    assert!(
        stdout.contains(marker) || stderr.contains(marker),
        "runtime logs should contain '{marker}'\n{stdout}\n{stderr}"
    );
}

fn assert_unique_ids(received: &[Received]) {
    let mut seen = HashSet::with_capacity(received.len());
    for message in received {
        assert!(
            seen.insert(message.id),
            "duplicate message id {}",
            message.id
        );
    }
}

fn count_by_table_and_op(received: &[Received]) -> BTreeMap<(String, String), usize> {
    let mut counts = BTreeMap::new();
    for message in received {
        *counts
            .entry((message.table().to_string(), message.op().to_string()))
            .or_default() += 1;
    }
    counts
}
