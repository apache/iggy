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

use crate::config::get_api_base_url;
use crate::error::{IggyBenchDashboardError, Result};
use bench_dashboard_shared::BenchmarkReportLight;
use bench_dashboard_shared::config::DashboardConfig;
use bench_dashboard_shared::series::{BenchmarkSeries, SeriesMetric, SeriesScope};
use bench_dashboard_shared::summary::BenchmarkSummary;
use gloo::console::log;
use gloo::net::http::Request;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use std::sync::atomic::{AtomicBool, Ordering};
use uuid::Uuid;

static HEALTH_CHECK_DONE: AtomicBool = AtomicBool::new(false);

async fn check_server_health() -> Result<()> {
    if HEALTH_CHECK_DONE.load(Ordering::Relaxed) {
        return Ok(());
    }

    let url = format!("{}/health", get_api_base_url());
    log!(format!("Checking health at: {}", url));

    let resp = Request::get(&url)
        .send()
        .await
        .map_err(|e| IggyBenchDashboardError::HealthCheck(format!("Network error: {e}")))?;

    if !resp.ok() {
        return Err(IggyBenchDashboardError::HealthCheck(format!(
            "Server returned {}",
            resp.status()
        )));
    }

    HEALTH_CHECK_DONE.store(true, Ordering::Relaxed);
    Ok(())
}

pub async fn fetch_benchmark_by_uuid(uuid: &str) -> Result<BenchmarkReportLight> {
    check_server_health().await?;

    log!(format!("Fetching benchmark for UUID: {}", uuid));
    let base_url = get_api_base_url();
    let url = format!("{base_url}/api/benchmark/light/{uuid}");
    let response = Request::get(&url).send().await.map_err(|e| {
        log!(format!("Network error fetching benchmark: {}", e));
        IggyBenchDashboardError::Network(e.to_string())
    })?;

    if response.status() != 200 {
        return Err(IggyBenchDashboardError::Server(format!(
            "Failed to fetch benchmark: {}",
            response.status()
        )));
    }

    let text = response.text().await.map_err(|e| {
        log!(format!("Error getting response text: {}", e));
        IggyBenchDashboardError::Parse(e.to_string())
    })?;

    if text.is_empty() {
        log!("Got empty response body despite 200 status code");
        return Err(IggyBenchDashboardError::Parse(
            "Empty response body".to_string(),
        ));
    }

    serde_json::from_str::<BenchmarkReportLight>(&text).map_err(|e| {
        log!(format!("JSON parse error: {}", e));
        IggyBenchDashboardError::Parse(e.to_string())
    })
}

/// A run's full report as JSON, with every time series.
pub fn report_url(uuid: &Uuid) -> String {
    format!("{}/api/benchmark/full/{uuid}", get_api_base_url())
}

/// A zip of a run's files, including the server logs.
pub fn artifacts_url(uuid: &Uuid) -> String {
    format!("{}/api/artifacts/{uuid}", get_api_base_url())
}

pub async fn fetch_benchmark_summaries() -> Result<Vec<BenchmarkSummary>> {
    get_json("/api/benchmarks").await
}

pub async fn fetch_dashboard_config() -> Result<DashboardConfig> {
    get_json("/api/config").await
}

pub async fn fetch_benchmark_series(
    uuid: Uuid,
    metric: SeriesMetric,
    scope: SeriesScope,
) -> Result<BenchmarkSeries> {
    let (Some(metric_name), Some(scope_name)) = (api_name(metric), api_name(scope)) else {
        return Err(IggyBenchDashboardError::Parse(format!(
            "{metric:?} or {scope:?} has no API name"
        )));
    };
    get_json(&format!(
        "/api/benchmark/series/{uuid}?metric={metric_name}&scope={scope_name}"
    ))
    .await
}

/// The name the JSON API gives a unit variant, so a link spells a value the way the API does.
pub fn api_name(value: impl Serialize) -> Option<String> {
    match serde_json::to_value(value) {
        Ok(Value::String(name)) => Some(name),
        _ => None,
    }
}

pub fn from_api_name<T: DeserializeOwned>(name: &str) -> Option<T> {
    serde_json::from_value(Value::String(name.to_owned())).ok()
}

async fn get_json<T: DeserializeOwned>(path: &str) -> Result<T> {
    let url = format!("{}{path}", get_api_base_url());
    let resp = Request::get(&url)
        .send()
        .await
        .map_err(|e| IggyBenchDashboardError::Network(e.to_string()))?;

    if !resp.ok() {
        return Err(IggyBenchDashboardError::Server(format!(
            "{path} returned {}",
            resp.status()
        )));
    }

    resp.json()
        .await
        .map_err(|e| IggyBenchDashboardError::Parse(e.to_string()))
}
