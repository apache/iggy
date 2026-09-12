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

//! Real-infra failure-state coverage for the OpenSearch sink, mirroring
//! `runtime/error_isolation.rs` but driving the failures through this sink's
//! own code paths against a live OpenSearch server rather than a
//! misconfigured `stdout_sink`.
//!
//! Two failure states, both configured in `failure_states/`, with two
//! structurally different outcomes:
//!
//!   * `opensearch_missing_index` never reaches `Running`. The target index
//!     does not exist and `create_index_if_not_exists = false`, so `open()`
//!     fails with `Error::InitError`. This goes through the sink init path
//!     (`runtime/src/sink.rs::init` / `manager/sink.rs::set_error`), which
//!     genuinely does flip `ConnectorStatus::Error`, observable via
//!     `/sinks`.
//!
//!   * `opensearch_mapping_conflict` reaches `Running`, indexes a first
//!     document successfully, then receives a batch containing one document
//!     that violates its own explicit `count: integer` mapping. OpenSearch
//!     answers that bulk call with a `mapper_parsing_exception`: HTTP 200 at
//!     the top level, with the failure in a per-item `items[]` entry, and
//!     `Sink::consume()` returns `Err`.
//!
//!     That `Err` does not halt the connector. `process_messages` in
//!     `runtime/src/sink.rs` invokes the FFI `consume` callback and discards
//!     its return code:
//!
//!     ```ignore
//!     (consume)(plugin_id, ..., messages.as_ptr(), messages.len());
//!     Ok(SinkBatchTiming { processed_count, decode_elapsed, ffi_elapsed })
//!     ```
//!
//!     `process_messages` always returns `Ok`, so `consume_messages`'s
//!     `if let Err(error) = result { return Err(error); }` fires only on the
//!     runtime's own internal failures (serialization, missing message
//!     fields), never on a plugin-returned `Err`. `ConnectorStatus` never
//!     leaves `Running`, and `/stats` `errors` (fed only by
//!     `process_messages`'s own decode/transform/field-validation failures)
//!     never increments. The malformed document is dropped with no
//!     API-visible trace; the only record is this connector's own `tracing`
//!     output. Subscribes to `test_topic_2`, distinct from the healthy
//!     sink's `test_topic`, so it never receives the healthy sink's
//!     messages or vice versa.
//!
//! `opensearch_healthy` is the control: it subscribes to a different topic
//! and must keep indexing normally throughout, proving the runtime process
//! (`/health`) and sibling connectors are unaffected. The stronger proof
//! point is that `opensearch_mapping_conflict` itself survives its own
//! failure and keeps indexing later messages.

use crate::connectors::fixtures::{OpenSearchFailureFixture, OpenSearchOps};
use bytes::Bytes;
use iggy::prelude::{IggyMessage, Partitioning};
use iggy_common::{Identifier, MessageClient};
use iggy_connector_sdk::api::{ConnectorError, ConnectorStatus, HealthResponse, SinkInfoResponse};
use integration::harness::seeds;
use integration::iggy_harness;
use reqwest::Client;
use std::time::Duration;
use tokio::time::sleep;

const POLL_ATTEMPTS: usize = 50;
const POLL_INTERVAL: Duration = Duration::from_millis(100);

async fn assert_runtime_healthy(http_client: &Client, api_address: &str) {
    let response = http_client
        .get(format!("{api_address}/health"))
        .send()
        .await
        .expect("Failed to query health endpoint");
    assert_eq!(response.status(), 200);
    let health: HealthResponse = response
        .json()
        .await
        .expect("Failed to parse health response");
    assert_eq!(health.status, "healthy");
}

async fn fetch_sinks(http_client: &Client, api_address: &str) -> Vec<SinkInfoResponse> {
    let response = http_client
        .get(format!("{api_address}/sinks"))
        .send()
        .await
        .expect("Failed to query /sinks");
    assert_eq!(response.status(), 200);
    response.json().await.expect("Failed to parse sinks")
}

/// Returns the matching sink's `last_error`, if any, once it reaches `status`.
/// `SinkInfoResponse` is not `Clone`, so only the field this test needs is
/// carried out of the poll loop.
async fn wait_for_status(
    http_client: &Client,
    api_address: &str,
    key: &str,
    status: ConnectorStatus,
) -> Option<ConnectorError> {
    for _ in 0..POLL_ATTEMPTS {
        let sinks = fetch_sinks(http_client, api_address).await;
        if let Some(sink) = sinks.into_iter().find(|sink| sink.key == key)
            && sink.status == status
        {
            return sink.last_error;
        }
        sleep(POLL_INTERVAL).await;
    }
    panic!("Sink '{key}' did not reach status {status:?} within {POLL_ATTEMPTS} attempts");
}

#[iggy_harness(
    server(connectors_runtime(
        config_path = "tests/connectors/opensearch/failure_states.toml"
    )),
    seed = seeds::connector_multi_topic_stream
)]
async fn given_missing_index_and_mapping_conflict_should_isolate_failures_from_healthy_sibling(
    harness: &TestHarness,
    fixture: OpenSearchFailureFixture,
) {
    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let http_client = Client::new();
    let iggy_client = harness.root_client().await.unwrap();

    // opensearch_missing_index never opens successfully: assert this first,
    // since it requires no message traffic at all.
    // `SinkContainer::open` (sdk/src/sink.rs) collapses any `Result<(), Error>`
    // from a plugin's own `open()` to a bare 0/1 at the FFI boundary, and the
    // runtime only ever records the generic "Plugin initialization failed
    // (ID: N)" for that case (runtime/src/sink.rs), so this connector's
    // `Error::InitError("... does not exist ...")` text never crosses the FFI
    // call. Uniform SDK behavior, not fixable per-connector: meilisearch_sink
    // has the identical gap. The only assertion the API can support here is
    // that the connector reached Error with some last_error at all.
    wait_for_status(
        &http_client,
        &api_address,
        "opensearch_missing_index",
        ConnectorStatus::Error,
    )
    .await
    .expect("missing-index sink should expose a last_error");

    // opensearch_healthy and opensearch_mapping_conflict both open fine.
    wait_for_status(
        &http_client,
        &api_address,
        "opensearch_healthy",
        ConnectorStatus::Running,
    )
    .await;
    wait_for_status(
        &http_client,
        &api_address,
        "opensearch_mapping_conflict",
        ConnectorStatus::Running,
    )
    .await;

    let stream_id: Identifier = seeds::names::STREAM.try_into().unwrap();
    let topic_id: Identifier = seeds::names::TOPIC.try_into().unwrap();
    let topic_2_id: Identifier = seeds::names::TOPIC_2.try_into().unwrap();

    let send = |topic_id: Identifier, payload: serde_json::Value, message_id: u128| {
        let stream_id = stream_id.clone();
        let client = &iggy_client;
        async move {
            let mut messages = vec![
                IggyMessage::builder()
                    .id(message_id)
                    .payload(Bytes::from(
                        serde_json::to_vec(&payload).expect("serialize"),
                    ))
                    .build()
                    .expect("build message"),
            ];
            client
                .send_messages(
                    &stream_id,
                    &topic_id,
                    &Partitioning::partition_id(0),
                    &mut messages,
                )
                .await
                .expect("send message");
        }
    };

    // A valid message to the healthy sink's topic, proving it indexes real
    // traffic before the sibling failure is introduced.
    send(
        topic_id.clone(),
        serde_json::json!({ "name": "healthy-before" }),
        1,
    )
    .await;

    // A valid message on the mapping-conflict sink's own topic first, so its
    // Running status and correct indexing are proven before it fails.
    send(
        topic_2_id.clone(),
        serde_json::json!({ "name": "before-failure", "count": 1 }),
        2,
    )
    .await;

    let index = fixture.mapping_conflict_index();
    wait_for_document_count(&fixture, index, 1).await;

    // One valid document plus one that violates the pinned `count: integer`
    // mapping, sent together so both land in the same `_bulk` call.
    let mut mixed_batch = vec![
        IggyMessage::builder()
            .id(3)
            .payload(Bytes::from(
                serde_json::to_vec(&serde_json::json!({ "name": "also-ok", "count": 2 }))
                    .expect("serialize"),
            ))
            .build()
            .expect("build message"),
        IggyMessage::builder()
            .id(4)
            .payload(Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "name": "should-fail",
                    "count": "not-a-number"
                }))
                .expect("serialize"),
            ))
            .build()
            .expect("build message"),
    ];
    iggy_client
        .send_messages(
            &stream_id,
            &topic_2_id,
            &Partitioning::partition_id(0),
            &mut mixed_batch,
        )
        .await
        .expect("send mixed batch");

    // The valid document from the mixed batch still landed: 1 from before
    // the failure + 1 valid document from the failing batch = 2. Proves
    // partial per-item credit against a real server, not just a JSON
    // fixture. The malformed document never appears; it is gone permanently.
    let document_count = wait_for_document_count(&fixture, index, 2).await;
    assert_eq!(
        document_count, 2,
        "the valid document in the mixed batch should still be indexed"
    );

    // The connector itself survives its own consume() failure: no status
    // change, no last_error, no /stats error count. See the module doc.
    let sinks = fetch_sinks(&http_client, &api_address).await;
    let mapping_conflict_sink = sinks
        .iter()
        .find(|sink| sink.key == "opensearch_mapping_conflict")
        .expect("mapping-conflict sink should be reported");
    assert_eq!(
        mapping_conflict_sink.status,
        ConnectorStatus::Running,
        "a consume()-level Err does not flip connector status; it is silently absorbed"
    );
    assert!(
        mapping_conflict_sink.last_error.is_none(),
        "a consume()-level Err never populates last_error"
    );

    // And it keeps indexing normally afterward: a third valid message on the
    // same topic must still land, proving this is not a lingering degraded
    // state: the connector is fully healthy, just missing one document.
    send(
        topic_2_id.clone(),
        serde_json::json!({ "name": "after-failure", "count": 3 }),
        5,
    )
    .await;
    let final_count = wait_for_document_count(&fixture, index, 3).await;
    assert_eq!(
        final_count, 3,
        "the connector must keep indexing later messages after an unnoticed consume() failure"
    );

    // This sink runs batch_size = 2, so these six messages are intended to
    // span three `_bulk` calls with the mapping violation in the first:
    // [bad, ok] [ok, ok] [ok, ok]. Five of the six are indexable, and all
    // five have to land: the runtime discards a consume()-level error and
    // commits the offset regardless, so anything not indexed here is gone
    // with no redelivery.
    //
    // This assertion alone can't distinguish "chunking happened as three
    // separate `_bulk` calls" from "the whole batch went out in one `_bulk`
    // call": OpenSearch's own per-item semantics make the final document
    // count identical either way, since a single call still reports per-item
    // success/failure. The deterministic proof that `index_documents` really
    // does send N separate chunked calls and keeps going after a permanently
    // failing one lives at the unit level, against a mocked server that can
    // assert per-chunk call counts:
    // `given_permanently_failing_chunk_should_not_abandon_later_chunks` in
    // `opensearch_sink/src/lib.rs`. What this test proves that the unit test
    // can't is that real documents genuinely survive in a live index across
    // that chunk boundary.
    let mut chunked_batch = vec![
        IggyMessage::builder()
            .id(6)
            .payload(Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "name": "chunk-poison",
                    "count": "not-a-number"
                }))
                .expect("serialize"),
            ))
            .build()
            .expect("build message"),
    ];
    for index_in_batch in 0..5u128 {
        chunked_batch.push(
            IggyMessage::builder()
                .id(7 + index_in_batch)
                .payload(Bytes::from(
                    serde_json::to_vec(&serde_json::json!({
                        "name": format!("chunk-ok-{index_in_batch}"),
                        "count": 10 + index_in_batch
                    }))
                    .expect("serialize"),
                ))
                .build()
                .expect("build message"),
        );
    }
    iggy_client
        .send_messages(
            &stream_id,
            &topic_2_id,
            &Partitioning::partition_id(0),
            &mut chunked_batch,
        )
        .await
        .expect("send chunked batch");

    let chunked_count = wait_for_document_count(&fixture, index, 8).await;
    assert_eq!(
        chunked_count, 8,
        "every chunk after a failing one must still be indexed"
    );

    // The healthy sibling, on a different topic, never saw the conflicting
    // document, actually indexed the traffic it did receive, and must still
    // be running. Refetched rather than reusing the snapshot taken above:
    // two more sends have happened since then, so a stale snapshot would
    // pass even if the runtime had since flipped its status.
    wait_for_document_count(&fixture, fixture.healthy_index(), 1).await;
    let sinks = fetch_sinks(&http_client, &api_address).await;
    let healthy_sink = sinks
        .iter()
        .find(|sink| sink.key == "opensearch_healthy")
        .expect("healthy sink should be reported");
    assert_eq!(healthy_sink.status, ConnectorStatus::Running);
    assert!(
        healthy_sink.last_error.is_none(),
        "healthy sibling sink should have no last_error"
    );

    // And the runtime process itself is unaffected by either failure.
    assert_runtime_healthy(&http_client, &api_address).await;
}

async fn wait_for_document_count(
    fixture: &OpenSearchFailureFixture,
    index: &str,
    expected: u64,
) -> u64 {
    for _ in 0..POLL_ATTEMPTS {
        if let Ok(count) = fixture.count_documents(index).await
            && count >= expected
        {
            return count;
        }
        sleep(POLL_INTERVAL).await;
    }
    panic!("index '{index}' did not reach {expected} documents within {POLL_ATTEMPTS} attempts");
}
