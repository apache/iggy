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

use crate::connectors::random_source_liveness;
use iggy_connector_sdk::api::{ConnectorRuntimeStats, ConnectorStatus};
use integration::harness::seeds;
use integration::iggy_harness;
use reqwest::Client;
use std::path::Path;
use std::time::Duration;
use tokio::time::{sleep, timeout};

const API_KEY: &str = "test-api-key";
const SOURCE_KEY: &str = "random";
const RETRY_INTERVAL: Duration = Duration::from_millis(100);
/// How long a counter is given to settle after the change that moves it.
/// Shared by the state-file and gauge waits: both are waiting on the same
/// thing, a report that may land just after the poll that preceded it.
const SETTLE_WINDOW: Duration = Duration::from_secs(1);
const WAIT_TIMEOUT: Duration = Duration::from_secs(10);

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/random/source.toml")),
    seed = seeds::connector_stream
)]
async fn random_source_produces_messages(harness: &TestHarness) {
    random_source_liveness::assert_produces_messages(harness).await;
}

#[iggy_harness(
    cluster_nodes = 1,
    server(connectors_runtime(config_path = "tests/connectors/random/source.toml")),
    seed = seeds::connector_stream
)]
async fn state_save_failure_preserves_state_and_source_recovers(harness: &TestHarness) {
    let state_path = harness
        .connectors_runtime()
        .expect("connectors runtime")
        .state_path()
        .join("source_random.state");
    wait_for_state_file(&state_path).await;
    let api_url = harness
        .connectors_runtime()
        .expect("connectors runtime")
        .http_url();
    let http = client();
    let errors_before_failure = source_errors(&http, &api_url).await;
    let state_dir = state_path.parent().expect("source state directory");
    let unavailable_state_dir = state_dir.with_extension("unavailable");
    let unavailable_state_path = unavailable_state_dir.join("source_random.state");

    tokio::fs::rename(state_dir, &unavailable_state_dir)
        .await
        .expect("source state directory should become unavailable");
    let state_before_failure = tokio::fs::read(&unavailable_state_path)
        .await
        .expect("source state should remain readable");
    wait_for_source_error_after(&http, &api_url, errors_before_failure).await;

    sleep(SETTLE_WINDOW).await;
    assert_eq!(
        tokio::fs::read(&unavailable_state_path)
            .await
            .expect("source state should remain readable"),
        state_before_failure,
        "NACKed batches must not advance persisted source state"
    );

    tokio::fs::rename(&unavailable_state_dir, state_dir)
        .await
        .expect("source state directory should become available");
    timeout(WAIT_TIMEOUT, async {
        loop {
            if tokio::fs::read(&state_path)
                .await
                .is_ok_and(|state| state != state_before_failure)
            {
                break;
            }
            sleep(RETRY_INTERVAL).await;
        }
    })
    .await
    .expect("source did not complete another batch after state storage recovery");

    random_source_liveness::assert_produces_messages(harness).await;
}

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/random/source.toml")),
    seed = seeds::connector_stream
)]
async fn sources_running_does_not_climb_across_restarts(harness: &TestHarness) {
    // The gauge counts running instances, and a restart takes one down before
    // bringing one up, so one configured source stays at one however often it
    // is restarted. It used to be reported by two mechanisms and taken back by
    // one.
    let api_url = harness
        .connectors_runtime()
        .expect("connectors runtime")
        .http_url();
    let http = client();

    wait_for_sources_running(&http, &api_url, 1).await;

    for round in 1..=3 {
        let response = http
            .post(format!("{api_url}/sources/{SOURCE_KEY}/restart"))
            .header("api-key", API_KEY)
            .send()
            .await
            .expect("restart request should be sent");
        assert_eq!(
            response.status().as_u16(),
            204,
            "restart {round} should be accepted"
        );

        wait_for_sources_running(&http, &api_url, 1).await;
    }

    // Read it once more after the gauge has settled. A report that lands after
    // the poll above would otherwise go unseen.
    sleep(SETTLE_WINDOW).await;
    assert_eq!(
        sources_running(&http, &api_url).await,
        1,
        "one running source must stay counted once, whatever it took to restart it"
    );
}

/// Every request carries [`WAIT_TIMEOUT`], so none of them can outlive the
/// wait they belong to. Without it a stalled runtime hangs the test rather
/// than failing it, and a hung test reports nothing at all.
fn client() -> Client {
    Client::builder()
        .timeout(WAIT_TIMEOUT)
        .build()
        .expect("the test client must build")
}

/// The one request the stats helpers share. Handing back the `Result` rather
/// than unwrapping it is what lets the retry loops keep treating a failed read
/// as "not yet" while the direct readers keep failing on it.
async fn fetch_stats(http: &Client, api_url: &str) -> reqwest::Result<ConnectorRuntimeStats> {
    http.get(format!("{api_url}/stats"))
        .header("api-key", API_KEY)
        .send()
        .await?
        .json::<ConnectorRuntimeStats>()
        .await
}

async fn sources_running(http: &Client, api_url: &str) -> u32 {
    fetch_stats(http, api_url)
        .await
        .expect("runtime stats should be valid")
        .sources_running
}

async fn wait_for_sources_running(http: &Client, api_url: &str, expected: u32) {
    // The last value the loop actually saw, rather than a fresh read in the
    // failure message. That read was the one request with no budget over it:
    // it only runs once the wait has already timed out, which is exactly when
    // the runtime is stalled, so the test hung instead of failing and reported
    // nothing at all.
    let mut last = None;
    let reached = timeout(WAIT_TIMEOUT, async {
        loop {
            let running = sources_running(http, api_url).await;
            last = Some(running);
            if running == expected {
                return;
            }
            sleep(RETRY_INTERVAL).await;
        }
    })
    .await;
    assert!(
        reached.is_ok(),
        "sources_running never reached {expected}; last read {last:?}"
    );
}

async fn wait_for_state_file(state_path: &Path) {
    timeout(Duration::from_secs(5), async {
        while !state_path.exists() {
            sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("source state file was not created");
}

async fn source_errors(http: &Client, api_url: &str) -> u64 {
    let stats = fetch_stats(http, api_url)
        .await
        .expect("runtime stats should be valid");
    stats
        .connectors
        .iter()
        .find(|connector| connector.key == SOURCE_KEY)
        .expect("random source stats should be present")
        .errors
}

async fn wait_for_source_error_after(http: &Client, api_url: &str, previous_errors: u64) {
    timeout(WAIT_TIMEOUT, async {
        loop {
            if let Ok(stats) = fetch_stats(http, api_url).await
                && let Some(source) = stats
                    .connectors
                    .iter()
                    .find(|connector| connector.key == SOURCE_KEY)
                && source.status == ConnectorStatus::Error
                && source.errors > previous_errors
            {
                break;
            }
            sleep(RETRY_INTERVAL).await;
        }
    })
    .await
    .expect("random source did not report a state-save failure")
}
