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

use super::{
    API_KEY, DEFAULT_SLOT, POLL_ATTEMPTS, POLL_INTERVAL_MS, SOURCE_KEY, source_stats,
    wait_for_source_errors,
};
use crate::connectors::create_test_messages;
use crate::connectors::fixtures::{PostgresOps, PostgresSourceCdcFixture, PostgresSourceOps};
use iggy::prelude::IggyClient;
use iggy_common::MessageClient;
use iggy_common::{Consumer, Identifier, PollingStrategy};
use iggy_connector_sdk::api::{ConnectorStatus, SourceInfoResponse};
use integration::harness::seeds;
use integration::iggy_harness;
use reqwest::Client;
use serde::Deserialize;
use std::time::Duration;
use tokio::time::sleep;

#[derive(Debug, Deserialize)]
struct CdcRecord {
    table_name: String,
    operation_type: String,
    data: serde_json::Value,
}

async fn poll_cdc_records(
    client: &IggyClient,
    stream_id: &Identifier,
    topic_id: &Identifier,
    consumer_id: &Identifier,
    want: usize,
) -> Vec<CdcRecord> {
    let mut received = Vec::new();
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(polled) = client
            .poll_messages(
                stream_id,
                topic_id,
                None,
                &Consumer::new(consumer_id.clone()),
                &PollingStrategy::next(),
                10,
                true,
            )
            .await
        {
            for msg in polled.messages {
                if let Ok(record) = serde_json::from_slice::<CdcRecord>(&msg.payload) {
                    received.push(record);
                }
            }
            if received.len() >= want {
                break;
            }
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
    received
}

async fn slot_contains_change(pool: &sqlx::PgPool, expected_value: &str) -> bool {
    for attempt in 0..POLL_ATTEMPTS {
        match sqlx::query_scalar::<_, String>(
            "SELECT data FROM pg_logical_slot_peek_changes($1, NULL, NULL)",
        )
        .bind(DEFAULT_SLOT)
        .fetch_all(pool)
        .await
        {
            Ok(changes) => {
                return changes.iter().any(|change| change.contains(expected_value));
            }
            Err(sqlx::Error::Database(ref database_error))
                if attempt + 1 < POLL_ATTEMPTS
                    && database_error.code().as_deref() == Some(PG_OBJECT_IN_USE) =>
            {
                sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
            }
            Err(error) => panic!("CDC replication slot should be readable: {error}"),
        }
    }

    panic!("CDC replication slot remained active after {POLL_ATTEMPTS} attempts");
}

// End-to-end CDC coverage against a real wal_level=logical container:
// INSERT, UPDATE, PK-changing UPDATE, DELETE, a rolled-back transaction
// (must produce nothing), an untracked table (must be filtered out), a
// 25-row sustained batch, and a final INSERT proving the slot/connector
// are still healthy after all of the above.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/postgres/source.toml")),
    seed = seeds::connector_stream
)]
async fn cdc_source_captures_insert_update_delete(
    harness: &TestHarness,
    fixture: PostgresSourceCdcFixture,
) {
    const BATCH_SIZE: usize = 25;

    let client = harness.root_client().await.unwrap();
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.create_table(&pool).await;
    fixture.create_untracked_table(&pool).await;

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "cdc_test_consumer".try_into().unwrap();

    // INSERT: basic capture, full column fidelity (id/name/count/amount/active).
    let [msg] = create_test_messages(1).try_into().unwrap();
    fixture
        .insert_row(
            &pool,
            msg.id as i32,
            &msg.name,
            msg.count as i32,
            msg.amount,
            msg.active,
            msg.timestamp,
        )
        .await;
    let received = poll_cdc_records(&client, &stream_id, &topic_id, &consumer_id, 1).await;
    assert_eq!(received.len(), 1, "Expected 1 INSERT message");
    assert_eq!(received[0].table_name, fixture.table_name());
    assert_eq!(received[0].operation_type, "INSERT");
    assert_eq!(received[0].data["id"], serde_json::json!(msg.id));
    assert_eq!(received[0].data["name"], serde_json::json!(msg.name));
    assert_eq!(received[0].data["count"], serde_json::json!(msg.count));
    assert_eq!(received[0].data["amount"].as_f64(), Some(msg.amount));
    assert_eq!(received[0].data["active"], serde_json::json!(msg.active));

    // UPDATE: non-key column change.
    let updated_count = msg.count + 1;
    fixture
        .update_row(&pool, msg.id as i32, updated_count as i32)
        .await;
    let received = poll_cdc_records(&client, &stream_id, &topic_id, &consumer_id, 1).await;
    assert_eq!(received.len(), 1, "Expected 1 UPDATE message");
    assert_eq!(received[0].operation_type, "UPDATE");
    assert_eq!(received[0].data["id"], serde_json::json!(msg.id));
    assert_eq!(received[0].data["count"], serde_json::json!(updated_count));

    // UPDATE: primary key change - test_decoding emits "old-key: ... new-tuple: ...",
    // the parser must report the new-tuple's id, not the old one.
    let new_id = msg.id as i32 + 1000;
    fixture
        .update_primary_key(&pool, msg.id as i32, new_id)
        .await;
    let received = poll_cdc_records(&client, &stream_id, &topic_id, &consumer_id, 1).await;
    assert_eq!(received.len(), 1, "Expected 1 UPDATE message for PK change");
    assert_eq!(received[0].operation_type, "UPDATE");
    assert_eq!(
        received[0].data["id"],
        serde_json::json!(new_id),
        "PK-changing UPDATE must report the new-tuple id, not the old-key one"
    );
    assert_eq!(received[0].data["name"], serde_json::json!(msg.name));

    // DELETE: only replica-identity columns (here, just id) are present.
    fixture.delete_row(&pool, new_id).await;
    let received = poll_cdc_records(&client, &stream_id, &topic_id, &consumer_id, 1).await;
    assert_eq!(received.len(), 1, "Expected 1 DELETE message");
    assert_eq!(received[0].operation_type, "DELETE");
    assert_eq!(received[0].data["id"], serde_json::json!(new_id));

    // Negative cases, verified together with the batch below: a rolled-back
    // transaction must never reach test_decoding, and a table outside
    // config.tables must be dropped by the client-side filter.
    fixture
        .insert_row_rolled_back(&pool, 9000, "should_not_appear")
        .await;
    fixture.insert_untracked_row(&pool, "not_configured").await;

    // Sustained batch: BATCH_SIZE separate INSERT rows, asserting order and
    // full data fidelity hold under a longer run, and that neither the
    // rolled-back row nor the untracked-table row leaked into the output.
    let batch = create_test_messages(BATCH_SIZE);
    for row in &batch {
        fixture
            .insert_row(
                &pool,
                row.id as i32,
                &row.name,
                row.count as i32,
                row.amount,
                row.active,
                row.timestamp,
            )
            .await;
    }

    let received = poll_cdc_records(&client, &stream_id, &topic_id, &consumer_id, BATCH_SIZE).await;
    assert_eq!(
        received.len(),
        BATCH_SIZE,
        "Expected exactly the {BATCH_SIZE} batch of INSERT rows - the rolled-back row and the \
         untracked-table row must not appear"
    );
    for (i, record) in received.iter().enumerate() {
        assert_eq!(record.operation_type, "INSERT");
        assert_eq!(record.table_name, fixture.table_name());
        assert_eq!(
            record.data["id"],
            serde_json::json!(batch[i].id),
            "Out-of-order or missing row at index {i}"
        );
        assert_eq!(record.data["name"], serde_json::json!(batch[i].name));
    }

    // Final sanity check: the slot/connector must still be healthy after the
    // earlier PK-changing UPDATE and the sustained batch, not left in some
    // degraded state that silently swallows further changes.
    let final_id = new_id + BATCH_SIZE as i32 + 1;
    fixture
        .insert_row(
            &pool,
            final_id,
            "after_pk_change",
            1,
            1.0,
            true,
            msg.timestamp,
        )
        .await;
    let received = poll_cdc_records(&client, &stream_id, &topic_id, &consumer_id, 1).await;
    assert_eq!(
        received.len(),
        1,
        "Expected the final INSERT after the PK change to still be captured"
    );
    assert_eq!(received[0].operation_type, "INSERT");
    assert_eq!(received[0].data["id"], serde_json::json!(final_id));
    assert_eq!(
        received[0].data["name"],
        serde_json::json!("after_pk_change")
    );

    pool.close().await;
}

#[iggy_harness(
    cluster_nodes = 1,
    server(connectors_runtime(config_path = "tests/connectors/postgres/source.toml")),
    seed = seeds::connector_stream
)]
async fn idle_cdc_source_advances_slot_to_current_wal(
    harness: &TestHarness,
    fixture: PostgresSourceCdcFixture,
) {
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.create_table(&pool).await;

    let api_url = harness
        .connectors_runtime()
        .expect("connectors runtime")
        .http_url();
    wait_for_source_status(&Client::new(), &api_url, ConnectorStatus::Running).await;

    sqlx::query("CHECKPOINT")
        .execute(&pool)
        .await
        .expect("Failed to generate WAL without a logical table change");
    let target_lsn: String = sqlx::query_scalar("SELECT pg_current_wal_flush_lsn()::text")
        .fetch_one(&pool)
        .await
        .expect("Failed to read current WAL flush LSN");

    for _ in 0..POLL_ATTEMPTS {
        let reached: bool = sqlx::query_scalar(
            "SELECT confirmed_flush_lsn >= $2::pg_lsn FROM pg_replication_slots WHERE slot_name = $1",
        )
        .bind(DEFAULT_SLOT)
        .bind(&target_lsn)
        .fetch_one(&pool)
        .await
        .expect("Failed to read replication slot position");
        if reached {
            pool.close().await;
            return;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }

    panic!("Idle CDC slot did not advance to WAL flush LSN {target_lsn}");
}

#[iggy_harness(
    cluster_nodes = 1,
    server(connectors_runtime(config_path = "tests/connectors/postgres/source.toml")),
    seed = seeds::connector_stream
)]
async fn given_cdc_change_when_iggy_crashes_should_advance_slot_only_after_redelivery(
    harness: &mut TestHarness,
    fixture: PostgresSourceCdcFixture,
) {
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.create_table(&pool).await;

    harness
        .server_mut()
        .stop_dependents()
        .expect("Failed to stop connectors runtime");
    harness
        .server_mut()
        .connectors_runtime_mut()
        .expect("connectors runtime")
        .set_iggy_connection_options("reconnection_retries=0");
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("Failed to restart connectors runtime");

    let api_url = harness
        .connectors_runtime()
        .expect("connectors runtime")
        .http_url();
    let http = Client::new();
    wait_for_source_status(&http, &api_url, ConnectorStatus::Running).await;
    let errors_before_failure = source_stats(&http, &api_url)
        .await
        .expect("PostgreSQL source stats should be present")
        .errors;
    harness.kill_node(0).expect("Failed to kill Iggy server");

    let [expected] = create_test_messages(1).try_into().unwrap();
    fixture
        .insert_row(
            &pool,
            expected.id as i32,
            &expected.name,
            expected.count as i32,
            expected.amount,
            expected.active,
            expected.timestamp,
        )
        .await;

    wait_for_source_errors(&http, &api_url, errors_before_failure + 2).await;
    assert!(
        slot_contains_change(&pool, &expected.name).await,
        "NACKed CDC change must remain available in the replication slot"
    );

    harness
        .server_mut()
        .stop_dependents()
        .expect("Failed to stop connectors runtime");
    harness
        .restart_node(0)
        .expect("Failed to restart Iggy server");
    harness
        .server_mut()
        .connectors_runtime_mut()
        .expect("connectors runtime")
        .clear_iggy_connection_options();
    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("Failed to restart connectors runtime");

    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "cdc_send_failure_consumer".try_into().unwrap();
    let received = poll_cdc_records(&client, &stream_id, &topic_id, &consumer_id, 1).await;

    assert_eq!(received.len(), 1, "CDC change should be redelivered");
    assert_eq!(received[0].operation_type, "INSERT");
    assert_eq!(received[0].data["id"], serde_json::json!(expected.id));

    let mut change_remains = slot_contains_change(&pool, &expected.name).await;
    for _ in 0..POLL_ATTEMPTS {
        if !change_remains {
            break;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        change_remains = slot_contains_change(&pool, &expected.name).await;
    }
    assert!(
        !change_remains,
        "ACKed CDC change should be consumed from the replication slot"
    );

    pool.close().await;
}

async fn wait_for_source_status(
    http: &Client,
    api_url: &str,
    expected: ConnectorStatus,
) -> SourceInfoResponse {
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(resp) = http
            .get(format!("{api_url}/sources/{SOURCE_KEY}"))
            .header("api-key", API_KEY)
            .send()
            .await
            && let Ok(info) = resp.json::<SourceInfoResponse>().await
            && info.status == expected
        {
            return info;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
    panic!("Source connector did not reach {expected:?} status in time");
}

async fn get_active_source_config(http: &Client, api_url: &str) -> serde_json::Value {
    http.get(format!("{api_url}/sources/{SOURCE_KEY}/configs/active"))
        .header("api-key", API_KEY)
        .send()
        .await
        .expect("Failed to fetch active source config")
        .json()
        .await
        .expect("Failed to parse active source config")
}

// CreateSourceConfig has no key/version/active fields - strip what GET
// .../configs/active added before POSTing the payload back as a new version.
async fn push_source_config(http: &Client, api_url: &str, mut config: serde_json::Value) -> u64 {
    if let Some(obj) = config.as_object_mut() {
        obj.remove("key");
        obj.remove("version");
        obj.remove("active");
    }
    let resp = http
        .post(format!("{api_url}/sources/{SOURCE_KEY}/configs"))
        .header("api-key", API_KEY)
        .json(&config)
        .send()
        .await
        .expect("Failed to call create-config endpoint");
    assert!(
        resp.status().is_success(),
        "Failed to create source config version, got {}",
        resp.status()
    );
    let created: serde_json::Value = resp.json().await.expect("Failed to parse created config");
    created["version"]
        .as_u64()
        .expect("Created config response missing version")
}

async fn activate_source_config(http: &Client, api_url: &str, version: u64) {
    let resp = http
        .put(format!("{api_url}/sources/{SOURCE_KEY}/configs/active"))
        .header("api-key", API_KEY)
        .json(&serde_json::json!({ "version": version }))
        .send()
        .await
        .expect("Failed to call activate-config endpoint");
    assert!(
        resp.status().is_success(),
        "Failed to activate source config version {version}, got {}",
        resp.status()
    );
}

async fn restart_source_expect(http: &Client, api_url: &str, should_succeed: bool, context: &str) {
    let resp = http
        .post(format!("{api_url}/sources/{SOURCE_KEY}/restart"))
        .header("api-key", API_KEY)
        .send()
        .await
        .expect("Failed to call restart endpoint");
    assert_eq!(
        resp.status().is_success(),
        should_succeed,
        "{context}, got {}",
        resp.status()
    );
}

// The local config provider globs every *.toml under config_dir on startup
// (see LocalConnectorsConfigProvider::init), and config_dir here is the real,
// shared postgres_source crate directory - so any version this test pushes
// must be deleted again, or it leaks into every other process that later
// points at that same directory.
async fn delete_source_config_version(http: &Client, api_url: &str, version: u64) {
    let resp = http
        .delete(format!(
            "{api_url}/sources/{SOURCE_KEY}/configs?version={version}"
        ))
        .header("api-key", API_KEY)
        .send()
        .await
        .expect("Failed to call delete-config endpoint");
    assert!(
        resp.status().is_success(),
        "Failed to delete source config version {version}, got {}",
        resp.status()
    );
}

// The connector peeks changes and advances the slot on a fixed poll interval.
// Both operations briefly hold the slot active. A drop landing in that window
// gets ERROR 55006 (slot is active for PID ...), so retry past transient hits
// instead of dropping while the poller is guaranteed stopped.
const PG_OBJECT_IN_USE: &str = "55006";

async fn drop_replication_slot_retrying(pool: &sqlx::PgPool, slot: &str) {
    for attempt in 0..POLL_ATTEMPTS {
        match sqlx::query("SELECT pg_drop_replication_slot($1)")
            .bind(slot)
            .execute(pool)
            .await
        {
            Ok(_) => return,
            Err(sqlx::Error::Database(ref db_err))
                if attempt + 1 < POLL_ATTEMPTS
                    && db_err.code().as_deref() == Some(PG_OBJECT_IN_USE) =>
            {
                sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
            }
            Err(e) => panic!("Failed to drop replication slot {slot}: {e}"),
        }
    }
}

// Three restart scenarios against one container, run in sequence:
// 1. Simulates upgrading from the old broken version, which left a slot
//    behind created with the pgoutput plugin. setup_cdc must refuse to
//    silently reuse a mismatched slot (which used to fail forever on
//    every poll instead) - it should fail loudly at startup, and recover
//    cleanly once the operator drops the bad slot and restarts.
// 2. Invalid plugin_config fields (capture_operations, payload_format).
//    open() must reject each at restart instead of leaving a Running
//    connector that silently drops every change or emits wrong data - the
//    same silent-death shape as the slot mismatch above. Config is fixed
//    one field at a time until restart succeeds and CDC resumes.
// 3. Changes written while the connector is down. The slot retains WAL
//    regardless of consumer state, then the connector peeks and advances
//    it only after Iggy acknowledges the recovered batch.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/postgres/source_cdc_restart.toml")),
    seed = seeds::connector_stream
)]
async fn cdc_source_recovers_from_slot_mismatch_and_restart(
    harness: &mut TestHarness,
    fixture: PostgresSourceCdcFixture,
) {
    let client = harness.root_client().await.unwrap();
    let pool = fixture.create_pool().await.expect("Failed to create pool");
    fixture.create_table(&pool).await;

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let consumer_id: Identifier = "cdc_restart_consumer".try_into().unwrap();

    let api_url = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let http = Client::new();
    wait_for_source_status(&http, &api_url, ConnectorStatus::Running).await;

    drop_replication_slot_retrying(&pool, DEFAULT_SLOT).await;
    sqlx::query("SELECT pg_create_logical_replication_slot($1, 'pgoutput')")
        .bind(DEFAULT_SLOT)
        .execute(&pool)
        .await
        .expect("Failed to create a wrong-plugin slot");

    // Assert on the restart call's own HTTP status, not last_error - the
    // still-running old connector keeps polling in the background and
    // could race a transient poll failure into last_error first.
    let restart_resp = http
        .post(format!("{api_url}/sources/{SOURCE_KEY}/restart"))
        .header("api-key", API_KEY)
        .send()
        .await
        .expect("Failed to call restart endpoint");
    assert!(
        !restart_resp.status().is_success(),
        "Restart with a mismatched slot should not report success, got {}",
        restart_resp.status()
    );

    // Operator fix: drop the bad slot, restart so the connector recreates it.
    sqlx::query("SELECT pg_drop_replication_slot($1)")
        .bind(DEFAULT_SLOT)
        .execute(&pool)
        .await
        .expect("Failed to drop the bad slot");

    let restart_resp = http
        .post(format!("{api_url}/sources/{SOURCE_KEY}/restart"))
        .header("api-key", API_KEY)
        .send()
        .await
        .expect("Failed to call restart endpoint");
    assert!(
        restart_resp.status().is_success(),
        "Restart after fixing the slot should succeed, got {}",
        restart_resp.status()
    );
    wait_for_source_status(&http, &api_url, ConnectorStatus::Running).await;

    let [before] = create_test_messages(1).try_into().unwrap();
    fixture
        .insert_row(
            &pool,
            before.id as i32,
            &before.name,
            before.count as i32,
            before.amount,
            before.active,
            before.timestamp,
        )
        .await;
    let received = poll_cdc_records(&client, &stream_id, &topic_id, &consumer_id, 1).await;
    assert_eq!(
        received.len(),
        1,
        "Expected CDC to resume capturing changes after the slot was fixed"
    );

    // Scenario 2: invalid plugin_config fields must be rejected at restart,
    // one field at a time, until the config is fully valid again. Every
    // pushed version is deleted again at the end (see
    // delete_source_config_version) so nothing outlives this test on disk.
    let baseline_config = get_active_source_config(&http, &api_url).await;
    let mut pushed_versions = Vec::new();

    let mut bad_ops = baseline_config.clone();
    bad_ops["plugin_config"]["capture_operations"] = serde_json::json!(["INSRT"]);
    let version = push_source_config(&http, &api_url, bad_ops).await;
    pushed_versions.push(version);
    activate_source_config(&http, &api_url, version).await;
    restart_source_expect(
        &http,
        &api_url,
        false,
        "Restart with an invalid capture_operations entry should not report success",
    )
    .await;

    let mut bad_format = baseline_config.clone();
    bad_format["plugin_config"]["payload_column"] = serde_json::json!("name");
    bad_format["plugin_config"]["payload_format"] = serde_json::json!("btea");
    let version = push_source_config(&http, &api_url, bad_format).await;
    pushed_versions.push(version);
    activate_source_config(&http, &api_url, version).await;
    restart_source_expect(
        &http,
        &api_url,
        false,
        "Restart with an invalid payload_format should not report success",
    )
    .await;

    let version = push_source_config(&http, &api_url, baseline_config).await;
    pushed_versions.push(version);
    activate_source_config(&http, &api_url, version).await;
    restart_source_expect(
        &http,
        &api_url,
        true,
        "Restart with a valid config should succeed",
    )
    .await;
    wait_for_source_status(&http, &api_url, ConnectorStatus::Running).await;

    let reconfigured_id = before.id as i32 + 1;
    fixture
        .insert_row(
            &pool,
            reconfigured_id,
            "reconfigured_row",
            1,
            1.0,
            true,
            before.timestamp,
        )
        .await;
    let received = poll_cdc_records(&client, &stream_id, &topic_id, &consumer_id, 1).await;
    assert_eq!(
        received.len(),
        1,
        "Expected CDC to resume capturing changes once the config was fixed"
    );

    // Deleting the still-active version last falls back to version 0 (the
    // original config.toml), restoring the pre-test state exactly.
    for version in pushed_versions {
        delete_source_config_version(&http, &api_url, version).await;
    }

    harness
        .server_mut()
        .stop_dependents()
        .expect("Failed to stop connectors");

    // The replication slot retains WAL for changes made while nothing is
    // consuming - this row must still arrive once the connector restarts.
    let after_id = reconfigured_id + 1;
    fixture
        .insert_row(
            &pool,
            after_id,
            "written_while_down",
            1,
            1.0,
            true,
            before.timestamp,
        )
        .await;

    harness
        .server_mut()
        .start_dependents()
        .await
        .expect("Failed to restart connectors");
    let api_url = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    wait_for_source_status(&http, &api_url, ConnectorStatus::Running).await;

    let received = poll_cdc_records(&client, &stream_id, &topic_id, &consumer_id, 1).await;
    assert_eq!(
        received.len(),
        1,
        "Expected the change written while the connector was down to arrive after restart"
    );
    assert_eq!(received[0].operation_type, "INSERT");
    assert_eq!(received[0].data["id"], serde_json::json!(after_id));
    assert_eq!(
        received[0].data["name"],
        serde_json::json!("written_while_down")
    );

    pool.close().await;
}
