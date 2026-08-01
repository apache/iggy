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

use iggy_connector_sdk::api::{ConnectorStatus, SourceInfoResponse};
use integration::harness::seeds;
use integration::iggy_harness;
use reqwest::Client;
use std::time::Duration;
use tokio::time::sleep;

const API_KEY: &str = "test-api-key";

#[iggy_harness(
    server(connectors_runtime(config_path = "tests/connectors/iggy/source.toml")),
    seed = seeds::connector_stream
)]
async fn iggy_source_polls_and_reports_status(harness: &TestHarness) {
    let api_address = harness
        .connectors_runtime()
        .expect("connector runtime should be available")
        .http_url();
    let http_client = Client::new();

    sleep(Duration::from_millis(500)).await;

    let response = http_client
        .get(format!("{}/sources", api_address))
        .header("api-key", API_KEY)
        .send()
        .await
        .expect("Failed to get sources");

    assert_eq!(response.status(), 200);
    let sources: Vec<SourceInfoResponse> = response.json().await.expect("Failed to parse sources");

    assert_eq!(sources.len(), 1);
    assert_eq!(sources[0].key, "iggy");
    assert_eq!(sources[0].status, ConnectorStatus::Running);
    assert!(sources[0].enabled);
}
