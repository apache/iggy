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

mod postgres_sink;
mod postgres_source;
mod postgres_source_cdc;
mod restart;

use std::time::Duration;

use iggy_connector_sdk::api::{ConnectorRuntimeStats, ConnectorStats, ConnectorStatus};
use reqwest::Client;
use serde::Deserialize;
use tokio::time::{sleep, timeout};

use crate::connectors::TestMessage;

const API_KEY: &str = "test-api-key";
const SOURCE_KEY: &str = "postgres";
const DEFAULT_SLOT: &str = "iggy_slot";
const TEST_MESSAGE_COUNT: usize = 3;
const POLL_ATTEMPTS: usize = 100;
const POLL_INTERVAL_MS: u64 = 50;
const SEND_FAILURE_TIMEOUT: Duration = Duration::from_secs(25);

#[derive(Debug, Deserialize)]
struct DatabaseRecord {
    table_name: String,
    operation_type: String,
    data: TestMessage,
}

async fn source_stats(http: &Client, api_url: &str) -> Option<ConnectorStats> {
    let response = http
        .get(format!("{api_url}/stats"))
        .header("api-key", API_KEY)
        .send()
        .await
        .ok()?;
    let stats = response.json::<ConnectorRuntimeStats>().await.ok()?;
    stats
        .connectors
        .into_iter()
        .find(|connector| connector.key == SOURCE_KEY)
}

async fn wait_for_source_errors(http: &Client, api_url: &str, minimum_errors: u64) {
    timeout(SEND_FAILURE_TIMEOUT, async {
        loop {
            if let Some(source) = source_stats(http, api_url).await
                && source.status == ConnectorStatus::Error
                && source.errors >= minimum_errors
            {
                break;
            }
            sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
        }
    })
    .await
    .expect("PostgreSQL source did not retry the NACKed batch");
}
