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

//! Manual-PUT benchmark comparing Doris Stream Load throughput for `format=json`
//! vs `format=csv` over an identical row set. It motivates the connector's opt-in
//! CSV output (`output_format = "csv"`): JSON parsing on the BE is more expensive
//! than CSV, so CSV can load the same data faster.
//!
//! `#[ignore]`d — it boots/reuses a real Doris container and is run on demand:
//!   cargo test -p integration -- --ignored \
//!     connectors::doris::doris_stream_load_format_bench
//!
//! It drives loads with the same manual reqwest PUT the dedupe test uses (so it
//! owns the format headers and compares apples to apples) rather than through
//! the connector. It asserts only that both formats load exactly N rows; the
//! json-vs-csv timing delta is reported, never asserted (it is environment
//! dependent and would flake).

use crate::connectors::fixtures::{DorisOps, DorisSinkFixture};
use crate::connectors::{TestMessage, create_test_messages};
use integration::harness::seeds;
use integration::iggy_harness;
use serde::Deserialize;
use sqlx::MySqlPool;
use std::io::Write as _;
use std::time::Instant;

const BENCH_ROWS: usize = 10_000;
const BENCH_ITERS: usize = 5;
// Mirrors the connector's CSV framing (control-char separators, enclose=", the
// columns in `TEST_TABLE_DDL_TEMPLATE` order).
const CSV_SEPARATOR: u8 = 0x01;
const CSV_LINE_DELIMITER: u8 = 0x02;
const CSV_COLUMNS: &str = "id, name, count, amount, active, timestamp";

/// The subset of Doris's Stream Load response the benchmark reads. `LoadTimeMs`
/// is the server-side load time; `Status` gates correctness.
#[derive(Debug, Deserialize)]
struct StreamLoadResult {
    #[serde(rename = "Status")]
    status: String,
    #[serde(rename = "LoadTimeMs", default)]
    load_time_ms: i64,
}

fn json_body(messages: &[TestMessage]) -> Vec<u8> {
    serde_json::to_vec(messages).expect("serialize json batch")
}

fn csv_body(messages: &[TestMessage]) -> Vec<u8> {
    let mut out = Vec::with_capacity(messages.len() * 64);
    for message in messages {
        let _ = write!(out, "{}", message.id);
        out.push(CSV_SEPARATOR);
        // The generated names are plain ASCII, but enclose them to mirror the
        // connector's encoding so the comparison reflects real CSV output.
        out.push(b'"');
        out.extend_from_slice(message.name.as_bytes());
        out.push(b'"');
        out.push(CSV_SEPARATOR);
        let _ = write!(out, "{}", message.count);
        out.push(CSV_SEPARATOR);
        let _ = write!(out, "{}", message.amount);
        out.push(CSV_SEPARATOR);
        out.extend_from_slice(if message.active { b"true" } else { b"false" });
        out.push(CSV_SEPARATOR);
        let _ = write!(out, "{}", message.timestamp);
        out.push(CSV_LINE_DELIMITER);
    }
    out
}

async fn truncate(pool: &MySqlPool, db: &str, table: &str) {
    sqlx::raw_sql(sqlx::AssertSqlSafe(format!("TRUNCATE TABLE {db}.{table}")))
        .execute(pool)
        .await
        .expect("truncate bench table");
}

#[allow(clippy::too_many_arguments)]
async fn stream_load(
    http: &reqwest::Client,
    fe_url: &str,
    db: &str,
    table: &str,
    label: &str,
    csv: bool,
    body: bytes::Bytes,
) -> StreamLoadResult {
    let mut current = format!("{fe_url}/api/{db}/{table}/_stream_load");
    let mut redirects = 0u8;
    let response = loop {
        let mut request = http
            .put(&current)
            .basic_auth("root", Some(""))
            .header(reqwest::header::EXPECT, "100-continue")
            .header("label", label)
            .body(body.clone());
        request = if csv {
            request
                .header("format", "csv")
                .header("column_separator", "\\x01")
                .header("line_delimiter", "\\x02")
                .header("enclose", "\"")
                .header("escape", "\\")
                .header("columns", CSV_COLUMNS)
        } else {
            request
                .header("format", "json")
                .header("strip_outer_array", "true")
        };
        let resp = request.send().await.expect("stream load PUT");
        let status = resp.status();
        if status == reqwest::StatusCode::TEMPORARY_REDIRECT
            || status == reqwest::StatusCode::PERMANENT_REDIRECT
        {
            redirects += 1;
            assert!(redirects <= 5, "exceeded 5 redirects following Stream Load");
            current = resp
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .expect("Location header")
                .to_string();
            continue;
        }
        break resp;
    };
    let text = response.text().await.expect("stream load body");
    serde_json::from_str(&text)
        .unwrap_or_else(|e| panic!("parse stream load response: {e}; body={text}"))
}

fn median(values: &mut [i64]) -> i64 {
    values.sort_unstable();
    values[values.len() / 2]
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/doris/sink.toml")),
    seed = seeds::connector_stream
)]
#[ignore = "benchmark: run on demand with --ignored; loads a real Doris container"]
async fn bench_json_vs_csv_stream_load(_harness: &TestHarness, fixture: DorisSinkFixture) {
    let db = fixture.database();
    fixture
        .create_table(db, "bench_json")
        .await
        .expect("create bench_json table");
    fixture
        .create_table(db, "bench_csv")
        .await
        .expect("create bench_csv table");

    let messages = create_test_messages(BENCH_ROWS);
    let json = bytes::Bytes::from(json_body(&messages));
    let csv = bytes::Bytes::from(csv_body(&messages));
    let json_bytes = json.len();
    let csv_bytes = csv.len();

    let http = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let fe = fixture.container().fe_url();
    let pool = fixture.pool().await.expect("pool");

    // Discarded warmup so the first cold load doesn't bias either format.
    truncate(&pool, db, "bench_json").await;
    let warmup = stream_load(
        &http,
        &fe,
        db,
        "bench_json",
        "iggy_bench_warmup",
        false,
        json.clone(),
    )
    .await;
    assert_eq!(warmup.status, "Success", "warmup load failed: {warmup:?}");
    fixture
        .wait_for_rows(db, "bench_json", BENCH_ROWS as i64)
        .await
        .expect("warmup rows");

    let mut json_load_ms = Vec::with_capacity(BENCH_ITERS);
    let mut json_wall_ms = Vec::with_capacity(BENCH_ITERS);
    let mut csv_load_ms = Vec::with_capacity(BENCH_ITERS);
    let mut csv_wall_ms = Vec::with_capacity(BENCH_ITERS);

    for iter in 0..BENCH_ITERS {
        // Interleave json then csv each round so neither benefits from warmer
        // cache / compaction state. Distinct tables + labels avoid dedupe and
        // the overshoot guard in wait_for_rows.
        truncate(&pool, db, "bench_json").await;
        let start = Instant::now();
        let r = stream_load(
            &http,
            &fe,
            db,
            "bench_json",
            &format!("iggy_bench_json_{iter}"),
            false,
            json.clone(),
        )
        .await;
        let wall = start.elapsed().as_millis() as i64;
        assert_eq!(r.status, "Success", "json load failed: {r:?}");
        fixture
            .wait_for_rows(db, "bench_json", BENCH_ROWS as i64)
            .await
            .expect("json rows");
        json_load_ms.push(r.load_time_ms);
        json_wall_ms.push(wall);

        truncate(&pool, db, "bench_csv").await;
        let start = Instant::now();
        let r = stream_load(
            &http,
            &fe,
            db,
            "bench_csv",
            &format!("iggy_bench_csv_{iter}"),
            true,
            csv.clone(),
        )
        .await;
        let wall = start.elapsed().as_millis() as i64;
        assert_eq!(r.status, "Success", "csv load failed: {r:?}");
        fixture
            .wait_for_rows(db, "bench_csv", BENCH_ROWS as i64)
            .await
            .expect("csv rows");
        csv_load_ms.push(r.load_time_ms);
        csv_wall_ms.push(wall);
    }

    let json_load = median(&mut json_load_ms);
    let json_wall = median(&mut json_wall_ms);
    let csv_load = median(&mut csv_load_ms);
    let csv_wall = median(&mut csv_wall_ms);

    println!(
        "\n=== Doris Stream Load: JSON vs CSV ({BENCH_ROWS} rows, {BENCH_ITERS} iters, median) ==="
    );
    println!(
        "  JSON: server LoadTimeMs={json_load}  client_wall={json_wall}ms  body={json_bytes} bytes"
    );
    println!(
        "  CSV : server LoadTimeMs={csv_load}  client_wall={csv_wall}ms  body={csv_bytes} bytes"
    );
    let server_delta = json_load - csv_load;
    let server_pct = if json_load > 0 {
        100.0 * server_delta as f64 / json_load as f64
    } else {
        0.0
    };
    println!(
        "  delta: server {server_delta}ms ({server_pct:.0}% {}), wall {}ms\n",
        if server_delta >= 0 {
            "CSV faster"
        } else {
            "JSON faster"
        },
        json_wall - csv_wall,
    );

    // Correctness only — timing is reported, never asserted (it flakes).
    assert_eq!(json_load_ms.len(), BENCH_ITERS);
    assert_eq!(csv_load_ms.len(), BENCH_ITERS);
}
