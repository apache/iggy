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
use bench_report::hardware::BenchmarkHardware;
use bench_report::report::BenchmarkReport;
use gloo::console::log;
use gloo::net::http::Request;
use std::sync::atomic::{AtomicBool, Ordering};
use uuid::Uuid;
use web_sys::window;

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

pub async fn fetch_hardware_configurations() -> Result<Vec<BenchmarkHardware>> {
    check_server_health().await?;

    let url = format!("{}/api/hardware", get_api_base_url());

    let resp = Request::get(&url)
        .send()
        .await
        .map_err(|e| IggyBenchDashboardError::Network(e.to_string()))?;

    if !resp.ok() {
        return Err(IggyBenchDashboardError::Server(format!(
            "Failed to fetch hardware configurations: {}",
            resp.status()
        )));
    }

    resp.json()
        .await
        .map_err(|e| IggyBenchDashboardError::Parse(e.to_string()))
}

pub async fn fetch_gitrefs_for_hardware(hardware: &str) -> Result<Vec<String>> {
    check_server_health().await?;

    let url = format!("{}/api/gitrefs/{}", get_api_base_url(), hardware);

    let resp = Request::get(&url)
        .send()
        .await
        .map_err(|e| IggyBenchDashboardError::Network(e.to_string()))?;

    if !resp.ok() {
        return Err(IggyBenchDashboardError::Server(format!(
            "Failed to fetch git refs: {}",
            resp.status()
        )));
    }

    resp.json()
        .await
        .map_err(|e| IggyBenchDashboardError::Parse(e.to_string()))
}

pub async fn fetch_benchmarks_for_hardware_and_gitref(
    hardware: &str,
    gitref: &str,
) -> Result<Vec<BenchmarkReportLight>> {
    check_server_health().await?;

    let url = format!(
        "{}/api/benchmarks/{}/{}",
        get_api_base_url(),
        hardware,
        gitref
    );

    let resp = Request::get(&url)
        .send()
        .await
        .map_err(|e| IggyBenchDashboardError::Network(e.to_string()))?;

    if !resp.ok() {
        return Err(IggyBenchDashboardError::Server(format!(
            "Failed to fetch benchmarks: {}",
            resp.status()
        )));
    }

    resp.json()
        .await
        .map_err(|e| IggyBenchDashboardError::Parse(e.to_string()))
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

pub async fn fetch_benchmark_report_full(uuid: &Uuid) -> Result<BenchmarkReport> {
    check_server_health().await?;

    let url = format!("{}/api/benchmark/full/{}", get_api_base_url(), uuid);

    let resp = Request::get(&url)
        .send()
        .await
        .map_err(|e| IggyBenchDashboardError::Network(e.to_string()))?;

    if !resp.ok() {
        return Err(IggyBenchDashboardError::Server(format!(
            "Failed to fetch benchmark report: {}",
            resp.status()
        )));
    }

    resp.json()
        .await
        .map_err(|e| IggyBenchDashboardError::Parse(e.to_string()))
}

#[allow(dead_code)]
pub async fn fetch_benchmark_trend(
    hardware: &str,
    params_identifier: &str,
) -> Result<Vec<BenchmarkReportLight>> {
    check_server_health().await?;

    let url = format!(
        "{}/api/benchmark/trend/{}/{}",
        get_api_base_url(),
        hardware,
        params_identifier
    );

    let resp = Request::get(&url)
        .send()
        .await
        .map_err(|e| IggyBenchDashboardError::Network(e.to_string()))?;

    if !resp.ok() {
        return Err(IggyBenchDashboardError::Server(format!(
            "Failed to fetch benchmark trend: {}",
            resp.status()
        )));
    }

    resp.json()
        .await
        .map_err(|e| IggyBenchDashboardError::Parse(e.to_string()))
}

pub fn download_test_artifacts(uuid: &Uuid) {
    let url = format!("{}/api/artifacts/{}", get_api_base_url(), uuid);

    if let Some(window) = window() {
        let _ = window
            .location()
            .set_href(&url)
            .map_err(|_| log!("Failed to initiate download"));
    }
}

pub async fn fetch_recent_benchmarks(limit: Option<u32>) -> Result<Vec<BenchmarkReportLight>> {
    check_server_health().await?;

    let limit_param = limit.unwrap_or(20);
    let url = format!("{}/api/recent/{}", get_api_base_url(), limit_param);

    let resp = Request::get(&url)
        .send()
        .await
        .map_err(|e| IggyBenchDashboardError::Network(e.to_string()))?;

    if !resp.ok() {
        return Err(IggyBenchDashboardError::Server(format!(
            "Failed to fetch recent benchmarks: {}",
            resp.status()
        )));
    }

    resp.json()
        .await
        .map_err(|e| IggyBenchDashboardError::Parse(e.to_string()))
}
