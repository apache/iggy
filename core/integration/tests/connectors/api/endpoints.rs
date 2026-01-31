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

use crate::connectors::api::setup;
use reqwest::Client;

const API_KEY: &str = "test-api-key";

#[tokio::test]
async fn test_root_endpoint_returns_welcome_message() {
    let runtime = setup().await;
    let api_address = runtime.connectors_api_address().unwrap();
    let client = Client::new();

    let response = client
        .get(format!("{}/", api_address))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body = response.text().await.unwrap();
    assert_eq!(body, "Connector Runtime API");
}

#[tokio::test]
async fn test_health_endpoint_returns_healthy() {
    let runtime = setup().await;
    let api_address = runtime.connectors_api_address().unwrap();
    let client = Client::new();

    let response = client
        .get(format!("{}/health", api_address))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body: serde_json::Value = response.json().await.unwrap();
    assert_eq!(body["status"], "healthy");
}

#[tokio::test]
async fn test_stats_endpoint_returns_runtime_stats() {
    let runtime = setup().await;
    let api_address = runtime.connectors_api_address().unwrap();
    let client = Client::new();

    let response = client
        .get(format!("{}/stats", api_address))
        .header("api-key", API_KEY)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let stats: serde_json::Value = response.json().await.unwrap();

    assert!(stats.get("process_id").is_some());
    assert!(stats.get("cpu_usage").is_some());
    assert!(stats.get("memory_usage").is_some());
    assert!(stats.get("run_time").is_some());
    assert!(stats.get("start_time").is_some());
    assert!(stats.get("sources_total").is_some());
    assert!(stats.get("sources_running").is_some());
    assert!(stats.get("sinks_total").is_some());
    assert!(stats.get("sinks_running").is_some());
    assert!(stats.get("connectors").is_some());

    assert!(stats["connectors"].is_array());
    assert_eq!(stats["sources_total"], 0);
    assert_eq!(stats["sources_running"], 0);
    assert_eq!(stats["sinks_total"], 0);
    assert_eq!(stats["sinks_running"], 0);
}

#[tokio::test]
async fn test_metrics_endpoint_returns_prometheus_format() {
    let runtime = setup().await;
    let api_address = runtime.connectors_api_address().unwrap();
    let client = Client::new();

    let response = client
        .get(format!("{}/metrics", api_address))
        .header("api-key", API_KEY)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let body = response.text().await.unwrap();

    assert!(body.contains("iggy_connectors_sources_total"));
    assert!(body.contains("iggy_connectors_sources_running"));
    assert!(body.contains("iggy_connectors_sinks_total"));
    assert!(body.contains("iggy_connectors_sinks_running"));
}

#[tokio::test]
async fn test_sources_endpoint_returns_list() {
    let runtime = setup().await;
    let api_address = runtime.connectors_api_address().unwrap();
    let client = Client::new();

    let response = client
        .get(format!("{}/sources", api_address))
        .header("api-key", API_KEY)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let sources: serde_json::Value = response.json().await.unwrap();

    assert!(sources.is_array());
    assert_eq!(sources.as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn test_sinks_endpoint_returns_list() {
    let runtime = setup().await;
    let api_address = runtime.connectors_api_address().unwrap();
    let client = Client::new();

    let response = client
        .get(format!("{}/sinks", api_address))
        .header("api-key", API_KEY)
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 200);
    let sinks: serde_json::Value = response.json().await.unwrap();

    assert!(sinks.is_array());
    assert_eq!(sinks.as_array().unwrap().len(), 0);
}

#[tokio::test]
async fn test_api_key_authentication_required() {
    let runtime = setup().await;
    let api_address = runtime.connectors_api_address().unwrap();
    let client = Client::new();

    let response = client
        .get(format!("{}/stats", api_address))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 401);

    let response = client
        .get(format!("{}/metrics", api_address))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 401);

    let response = client
        .get(format!("{}/sources", api_address))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 401);

    let response = client
        .get(format!("{}/sinks", api_address))
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 401);
}

#[tokio::test]
async fn test_api_key_authentication_rejected_with_invalid_key() {
    let runtime = setup().await;
    let api_address = runtime.connectors_api_address().unwrap();
    let client = Client::new();

    let response = client
        .get(format!("{}/stats", api_address))
        .header("api-key", "invalid-key")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 401);

    let response = client
        .get(format!("{}/metrics", api_address))
        .header("api-key", "wrong-api-key")
        .send()
        .await
        .unwrap();

    assert_eq!(response.status(), 401);
}
