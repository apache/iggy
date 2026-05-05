/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::connectors::create_test_messages;
use crate::connectors::fixtures::{
    DorisOps, DorisSinkColumnsMappingFixture, DorisSinkFixture, DorisSinkMaxFilterRatioFixture,
    DorisSinkPreCreatedFixture,
};
use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_common::Identifier;
use iggy_common::MessageClient;
use integration::harness::seeds;
use integration::iggy_harness;
use serde::{Deserialize, Serialize};

const TEST_DATABASE: &str = "iggy_test_db";
const TEST_TABLE: &str = "test_topic";

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/doris/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_existent_doris_table_should_store(
    harness: &TestHarness,
    fixture: DorisSinkPreCreatedFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let message_count = 10;
    let test_messages = create_test_messages(message_count);
    let payloads: Vec<Bytes> = test_messages
        .iter()
        .map(|m| Bytes::from(serde_json::to_vec(m).expect("serialize")))
        .collect();

    let mut messages: Vec<IggyMessage> = payloads
        .iter()
        .enumerate()
        .map(|(i, p)| {
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(p.clone())
                .build()
                .expect("build message")
        })
        .collect();

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("send messages");

    let count = fixture
        .wait_for_rows(TEST_DATABASE, TEST_TABLE, message_count as i64)
        .await
        .expect("rows");
    assert_eq!(count, message_count as i64);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/doris/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_bulk_message_send_should_store(
    harness: &TestHarness,
    fixture: DorisSinkPreCreatedFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let message_count = 1000;
    let test_messages = create_test_messages(message_count);

    let mut messages: Vec<IggyMessage> = test_messages
        .iter()
        .enumerate()
        .map(|(i, m)| {
            let payload = Bytes::from(serde_json::to_vec(m).expect("serialize"));
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(payload)
                .build()
                .expect("build message")
        })
        .collect();

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("send messages");

    let count = fixture
        .wait_for_rows(TEST_DATABASE, TEST_TABLE, message_count as i64)
        .await
        .expect("rows");
    assert_eq!(count, message_count as i64);
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/doris/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_invalid_messages_should_skip_via_max_filter_ratio(
    harness: &TestHarness,
    fixture: DorisSinkMaxFilterRatioFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    // Two valid messages.
    let valid: Vec<Bytes> = create_test_messages(2)
        .iter()
        .map(|m| Bytes::from(serde_json::to_vec(m).expect("serialize")))
        .collect();

    // One message whose JSON does not match the table columns. Doris will
    // reject it as a "filtered" row; max_filter_ratio = 0.5 covers up to
    // half the batch, so the load still succeeds and the two valid rows land.
    #[derive(Debug, Serialize, Deserialize)]
    struct WrongShape {
        unrelated: f64,
    }
    let invalid =
        Bytes::from(serde_json::to_vec(&WrongShape { unrelated: 1.0 }).expect("serialize"));

    let payloads = [valid[0].clone(), invalid, valid[1].clone()];
    let mut messages: Vec<IggyMessage> = payloads
        .iter()
        .enumerate()
        .map(|(i, p)| {
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(p.clone())
                .build()
                .expect("build message")
        })
        .collect();

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("send messages");

    let count = fixture
        .wait_for_rows(TEST_DATABASE, TEST_TABLE, 2)
        .await
        .expect("rows");
    assert_eq!(count, 2, "only the two valid rows should land");
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/doris/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_replayed_label_should_dedupe(harness: &TestHarness, fixture: DorisSinkFixture) {
    // This fixture variant does NOT pre-create the table; the test creates
    // it explicitly before producing messages so we can drop and recreate
    // between rounds without disturbing the connector runtime's state.
    fixture
        .create_table(TEST_DATABASE, TEST_TABLE)
        .await
        .expect("create table");

    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let message_count = 5;
    let test_messages = create_test_messages(message_count);

    // Round 1: send and observe rows land.
    let mut round1: Vec<IggyMessage> = test_messages
        .iter()
        .enumerate()
        .map(|(i, m)| {
            let payload = Bytes::from(serde_json::to_vec(m).expect("serialize"));
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(payload)
                .build()
                .expect("build message")
        })
        .collect();
    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut round1,
        )
        .await
        .expect("send messages round 1");

    let count = fixture
        .wait_for_rows(TEST_DATABASE, TEST_TABLE, message_count as i64)
        .await
        .expect("rows after round 1");
    assert_eq!(count, message_count as i64);

    // Round 2: send the same payloads again with the same Iggy IDs. The
    // connector generates a deterministic Stream Load label per (stream,
    // topic, partition, first_offset, last_offset). Because Iggy assigns
    // monotonic offsets, the second batch lands at different offsets and
    // gets a different label — so duplicates WOULD land if the replay
    // deduplication only relied on Iggy IDs. The point of this test is the
    // converse case below.
    //
    // Instead we verify the label-dedupe path by issuing a manual Stream
    // Load with the SAME label as the one the connector just used. Doris
    // must respond with "Label Already Exists", which the connector maps
    // to Ok — so the row count must NOT increase.
    // Use the fixture's count helper (raw_sql under the hood) — Doris's
    // MySQL frontend rejects prepared sqlx::query/query_scalar statements.
    let row_before = fixture
        .count_rows(TEST_DATABASE, TEST_TABLE)
        .await
        .expect("count before");
    assert_eq!(row_before, message_count as i64);

    // Replay the same offsets => same label => Doris dedupes server-side.
    let label = format!(
        "iggy_test-{}-{}-0-0-{}",
        sanitize(seeds::names::STREAM),
        sanitize(seeds::names::TOPIC),
        message_count - 1,
    );

    let body = serde_json::to_vec(&test_messages).expect("serialize replay body");

    let client_http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    let url = format!(
        "{}/api/{TEST_DATABASE}/{TEST_TABLE}/_stream_load",
        fixture.container().fe_url()
    );

    // Manually follow the FE -> BE 307 once. Cap iterations so a misbehaving
    // cluster can't hang the test process indefinitely.
    const MAX_REDIRECTS: u8 = 5;
    let mut current_url = url.clone();
    let mut redirects = 0u8;
    let response = loop {
        let resp = client_http
            .put(&current_url)
            .basic_auth("root", Some(""))
            // Doris's FE rejects Stream Load PUTs that don't carry
            // `Expect: 100-continue` (the connector sets this; the manual
            // probe needs the same).
            .header(reqwest::header::EXPECT, "100-continue")
            .header("format", "json")
            .header("strip_outer_array", "true")
            .header("label", &label)
            .body(body.clone())
            .send()
            .await
            .expect("stream load");
        let status = resp.status();
        if status == reqwest::StatusCode::TEMPORARY_REDIRECT
            || status == reqwest::StatusCode::PERMANENT_REDIRECT
        {
            redirects += 1;
            assert!(
                redirects <= MAX_REDIRECTS,
                "exceeded {MAX_REDIRECTS} redirects following Stream Load"
            );
            let loc = resp
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .expect("Location header")
                .to_string();
            current_url = loc;
            continue;
        }
        break resp;
    };
    let body_text = response.text().await.expect("body");
    // Require Doris to report dedupe explicitly. A `"Success"` here would
    // mean the label drifted from what the connector built — which doubles
    // rows and is caught by `row_after == message_count`, but we want the
    // dedupe path itself to be load-bearing in this test.
    assert!(
        body_text.contains("Label Already Exists"),
        "expected Doris to dedupe by label, got: {body_text}"
    );

    let row_after = fixture
        .count_rows(TEST_DATABASE, TEST_TABLE)
        .await
        .expect("count after");
    assert_eq!(
        row_after, message_count as i64,
        "label dedupe must not produce duplicates"
    );
}

/// Connector targets a table that does not exist. Every Stream Load PUT
/// gets a `Status: "Fail"` (or the FE returns a 4xx) — the connector must
/// classify that as `PermanentHttpError`, NOT `CannotStoreData`. The
/// integration assertions here are coarse on purpose (we can't read the
/// connector's internal Result), but they prove three things any future
/// regression would break:
///   1. The connector does not silently auto-create the missing table.
///   2. The connector does not write into any *other* table by mistake.
///   3. The Doris cluster (and our connection pool) survives the failed
///      load attempts intact.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/doris/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_missing_target_table_should_not_create_or_corrupt(
    harness: &TestHarness,
    fixture: DorisSinkFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let test_messages = create_test_messages(5);
    let mut messages: Vec<IggyMessage> = test_messages
        .iter()
        .enumerate()
        .map(|(i, m)| {
            let payload = Bytes::from(serde_json::to_vec(m).expect("serialize"));
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(payload)
                .build()
                .expect("build message")
        })
        .collect();

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("send messages");

    // Give the connector enough time to receive the batch and attempt at
    // least one Stream Load PUT against the missing table.
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // 1. The target table must NOT have been auto-created.
    let exists = fixture
        .table_exists(TEST_DATABASE, TEST_TABLE)
        .await
        .expect("information_schema query");
    assert!(
        !exists,
        "Doris sink must NOT auto-create the target table on a failed load"
    );

    // 2 + 3. The cluster is still healthy and `iggy_test_db` has no rogue
    // tables — proves we didn't write anywhere unexpected and that the
    // connector's HTTP failures didn't take Doris down.
    let pool = fixture.pool().await.expect("pool after failed loads");
    use sqlx::Row;
    let rows = sqlx::raw_sql(
        "SELECT TABLE_NAME FROM information_schema.tables \
         WHERE TABLE_SCHEMA = 'iggy_test_db'",
    )
    .fetch_all(&pool)
    .await
    .expect("list tables in iggy_test_db");
    let tables: Vec<String> = rows
        .iter()
        .filter_map(|r| r.try_get::<String, _>("TABLE_NAME").ok())
        .collect();
    assert!(
        tables.is_empty(),
        "iggy_test_db should be empty after failed loads, found: {tables:?}"
    );
}

/// Verifies the `columns` Stream Load config is wired through end-to-end.
/// The pre-created table has an extra `calculated INT NOT NULL` column that
/// is NOT in the JSON payload; the only way the load succeeds is if the
/// connector forwards the configured `columns` header to Doris so it can
/// derive `calculated = count + 1` server-side.
#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/doris/sink.toml")),
    seed = seeds::connector_stream
)]
async fn given_columns_config_should_apply_derived_expression(
    harness: &TestHarness,
    fixture: DorisSinkColumnsMappingFixture,
) {
    let client = harness.root_client().await.unwrap();
    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();

    let message_count = 5;
    let test_messages = create_test_messages(message_count);
    let mut messages: Vec<IggyMessage> = test_messages
        .iter()
        .enumerate()
        .map(|(i, m)| {
            let payload = Bytes::from(serde_json::to_vec(m).expect("serialize"));
            IggyMessage::builder()
                .id((i + 1) as u128)
                .payload(payload)
                .build()
                .expect("build message")
        })
        .collect();

    client
        .send_messages(
            &stream_id,
            &topic_id,
            &Partitioning::partition_id(0),
            &mut messages,
        )
        .await
        .expect("send messages");

    let count = fixture
        .wait_for_rows(TEST_DATABASE, TEST_TABLE, message_count as i64)
        .await
        .expect("rows");
    assert_eq!(count, message_count as i64);

    // `create_test_messages` produces count = (i - 1) * 10 for i in 1..=N,
    // so SUM(count) = 0 + 10 + 20 + 30 + 40 = 100 and with the derived
    // expression `calculated = count + 1` we expect SUM(calculated - count)
    // to equal exactly the row count (one per row).
    let pool = fixture.pool().await.expect("pool");
    use sqlx::Row;
    let row = sqlx::raw_sql(&format!(
        "SELECT SUM(calculated - `count`) AS delta FROM {TEST_DATABASE}.{TEST_TABLE}"
    ))
    .fetch_one(&pool)
    .await
    .expect("sum query");
    let delta: i64 = row
        .try_get::<i64, _>("delta")
        .expect("delta column decodes as i64");
    assert_eq!(
        delta, message_count as i64,
        "expected calculated = count + 1 per row (delta sum = {message_count}), got {delta}"
    );
}

fn sanitize(value: &str) -> String {
    value
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}
