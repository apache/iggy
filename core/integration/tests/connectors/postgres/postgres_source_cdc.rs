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

use super::{POLL_ATTEMPTS, POLL_INTERVAL_MS};
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

const API_KEY: &str = "test-api-key";
const SOURCE_KEY: &str = "postgres";
const DEFAULT_SLOT: &str = "iggy_slot";

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

// Two restart scenarios against one container, run in sequence:
// 1. Simulates upgrading from the old broken version, which left a slot
//    behind created with the pgoutput plugin. setup_cdc must refuse to
//    silently reuse a mismatched slot (which used to fail forever on
//    every poll instead) - it should fail loudly at startup, and recover
//    cleanly once the operator drops the bad slot and restarts.
// 2. Changes written while the connector is down (the slot retains WAL
//    regardless of consumer state) - not the at-least-once crash window
//    where the slot has already been consumed but send/state-persist
//    hasn't happened yet. That gap remains open until the slot-peek/LSN
//    work lands.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/postgres/source.toml")),
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

    sqlx::query("SELECT pg_drop_replication_slot($1)")
        .bind(DEFAULT_SLOT)
        .execute(&pool)
        .await
        .expect("Failed to drop the correctly-created slot");
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

    harness
        .server_mut()
        .stop_dependents()
        .expect("Failed to stop connectors");

    // The replication slot retains WAL for changes made while nothing is
    // consuming - this row must still arrive once the connector restarts.
    let after_id = before.id as i32 + 1;
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
