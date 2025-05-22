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

use crate::{cache::BenchmarkCache, error::IggyBenchDashboardServerError};
use actix_web::{HttpRequest, HttpResponse, get, web};
use std::sync::Arc;
use tracing::{info, warn};
use uuid::Uuid;
use walkdir::WalkDir;
use zip::{ZipWriter, write::FileOptions};

type Result<T> = std::result::Result<T, IggyBenchDashboardServerError>;

pub struct AppState {
    pub cache: Arc<BenchmarkCache>,
}

#[get("/health")]
pub async fn health_check(req: HttpRequest) -> Result<HttpResponse> {
    let client_addr = get_client_addr(&req);
    info!("{}: Health check request", client_addr);
    Ok(HttpResponse::Ok().json(serde_json::json!({ "status": "healthy" })))
}

#[get("/api/hardware")]
pub async fn list_hardware(data: web::Data<AppState>, req: HttpRequest) -> Result<HttpResponse> {
    let client_addr = get_client_addr(&req);
    info!("{}: Listing hardware configurations", client_addr);

    let hardware_list = data.cache.get_hardware_configurations();

    info!(
        "{}: Found {} hardware configurations",
        client_addr,
        hardware_list.len()
    );

    Ok(HttpResponse::Ok().json(hardware_list))
}

#[get("/api/gitrefs/{hardware}")]
pub async fn list_gitrefs_for_hardware(
    data: web::Data<AppState>,
    hardware: web::Path<String>,
    req: HttpRequest,
) -> Result<HttpResponse> {
    let client_addr = get_client_addr(&req);
    info!(
        "{}: Listing git refs for hardware '{}'",
        client_addr, hardware
    );

    let gitrefs = data.cache.get_gitrefs_for_hardware(&hardware);

    info!(
        "{}: Found {} git refs for hardware '{}'",
        client_addr,
        gitrefs.len(),
        hardware
    );
    Ok(HttpResponse::Ok().json(gitrefs))
}

#[get("/api/benchmarks/{gitref}")]
pub async fn list_benchmarks_for_gitref(
    data: web::Data<AppState>,
    gitref: web::Path<String>,
    req: HttpRequest,
) -> Result<HttpResponse> {
    let client_addr = get_client_addr(&req);
    let gitref = gitref.into_inner();
    info!(
        "{}: Listing benchmarks for git ref '{}'",
        client_addr, gitref
    );

    let benchmarks = data.cache.get_benchmarks_for_gitref(&gitref);

    info!(
        "{}: Found {} benchmarks for git ref '{}'",
        client_addr,
        benchmarks.len(),
        gitref
    );
    Ok(HttpResponse::Ok().json(benchmarks))
}

#[get("/api/benchmarks/{hardware}/{gitref}")]
pub async fn list_benchmarks_for_hardware_and_gitref(
    data: web::Data<AppState>,
    path: web::Path<(String, String)>,
    req: HttpRequest,
) -> Result<HttpResponse> {
    let client_addr = get_client_addr(&req);
    let (hardware, gitref) = path.into_inner();
    info!(
        "{}: Listing benchmarks for git ref '{}'",
        client_addr, gitref
    );

    let benchmarks = data
        .cache
        .get_benchmarks_for_hardware_and_gitref(&hardware, &gitref);
    info!(
        "{}: Found {} benchmarks for git ref '{}'",
        client_addr,
        benchmarks.len(),
        gitref
    );
    Ok(HttpResponse::Ok().json(benchmarks))
}

#[get("/api/benchmark/full/{unique_id}")]
pub async fn get_benchmark_report_full(
    data: web::Data<AppState>,
    uuid_str: web::Path<String>,
    req: HttpRequest,
) -> Result<HttpResponse> {
    let client_addr = get_client_addr(&req);
    info!(
        "{}: Requesting full benchmark report '{}'",
        client_addr, uuid_str
    );

    let uuid = Uuid::parse_str(&uuid_str).map_err(|_| {
        IggyBenchDashboardServerError::NotFound(format!("Invalid UUID format: '{}'", uuid_str))
    })?;

    let json_path = data.cache.get_benchmark_json_path(&uuid).ok_or_else(|| {
        IggyBenchDashboardServerError::NotFound(format!("Benchmark '{}' not found", uuid_str))
    })?;

    let json_content = std::fs::read_to_string(&json_path).map_err(|e| {
        IggyBenchDashboardServerError::NotFound(format!(
            "Report file not found for '{}': {}",
            json_path.display(),
            e
        ))
    })?;

    info!(
        "{}: Found full benchmark report for uuid '{}' at '{:?}'",
        client_addr, uuid_str, json_path
    );
    Ok(HttpResponse::Ok()
        .content_type("application/json")
        .body(json_content))
}

#[get("/api/benchmark/light/{unique_id}")]
pub async fn get_benchmark_report_light(
    data: web::Data<AppState>,
    uuid_str: web::Path<String>,
    req: HttpRequest,
) -> Result<HttpResponse> {
    let client_addr = get_client_addr(&req);
    info!(
        "{}: Requesting light benchmark report '{}'",
        client_addr, uuid_str
    );

    let uuid = match Uuid::parse_str(&uuid_str) {
        Ok(uuid) => uuid,
        Err(e) => {
            warn!(
                "{client_addr}: Invalid UUID format in light benchmark request: '{uuid_str}', error: {e}",
            );
            return Err(IggyBenchDashboardServerError::InvalidUuid(format!(
                "Invalid UUID format: '{}'",
                uuid_str
            )));
        }
    };

    match data.cache.get_benchmark_report_light(&uuid) {
        Some(report) => {
            info!(
                "{}: Found light benchmark report for uuid '{}'",
                client_addr, uuid_str
            );
            Ok(HttpResponse::Ok().json(report))
        }
        None => {
            warn!(
                "{}: Light benchmark report not found for uuid '{}'",
                client_addr, uuid_str
            );
            Err(IggyBenchDashboardServerError::NotFound(format!(
                "Benchmark '{}' not found",
                uuid_str
            )))
        }
    }
}

#[get("/api/benchmark/trend/{hardware}/{params_identifier}")]
pub async fn get_benchmark_trend(
    data: web::Data<AppState>,
    path: web::Path<(String, String)>,
    req: HttpRequest,
) -> Result<HttpResponse> {
    let (hardware, params_identifier) = path.into_inner();
    let client_addr = get_client_addr(&req);
    info!(
        "{}: Requesting trend data for hardware '{}' with params identifier '{}'",
        client_addr, hardware, params_identifier
    );

    let trend_data = data
        .cache
        .get_benchmark_trend_data(&params_identifier, &hardware)
        .ok_or_else(|| {
            IggyBenchDashboardServerError::NotFound(format!(
                "Trend data not found for hardware '{}' with params identifier '{}'",
                hardware, params_identifier
            ))
        })?;

    info!(
        "{}: Found {} trend data points for hardware '{}' with params identifier '{}'",
        client_addr,
        trend_data.len(),
        hardware,
        params_identifier
    );

    Ok(HttpResponse::Ok().json(trend_data))
}

#[get("/api/artifacts/{uuid}")]
pub async fn get_test_artifacts_zip(
    data: web::Data<AppState>,
    uuid_str: web::Path<String>,
    req: HttpRequest,
) -> Result<HttpResponse> {
    let client_addr = get_client_addr(&req);
    info!(
        "{}: Requesting test artifacts for uuid '{}'",
        client_addr, uuid_str
    );

    let uuid = match Uuid::parse_str(&uuid_str) {
        Ok(uuid) => uuid,
        Err(_) => {
            warn!("{client_addr}: Invalid UUID format in test artifacts request: '{uuid_str}'",);
            return Err(IggyBenchDashboardServerError::InvalidUuid(
                uuid_str.to_string(),
            ));
        }
    };

    // Get the benchmark report to find its directory
    let artifacts_dir = match data.cache.get_benchmark_path(&uuid) {
        Some(path) => path,
        None => {
            warn!(
                "{}: Benchmark not found for uuid '{}'",
                client_addr, uuid_str
            );
            return Err(IggyBenchDashboardServerError::NotFound(format!(
                "Benchmark '{}' not found",
                uuid_str
            )));
        }
    };

    // Create a buffer for the zip file
    let mut zip_buffer = Vec::new();
    {
        let mut zip = ZipWriter::new(std::io::Cursor::new(&mut zip_buffer));
        let options = FileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated)
            .unix_permissions(0o755)
            as FileOptions<zip::write::ExtendedFileOptions>;

        // Walk through all files in the directory
        for entry in WalkDir::new(&artifacts_dir) {
            let entry = entry.map_err(|e| {
                IggyBenchDashboardServerError::InternalError(format!(
                    "Error walking directory: {}",
                    e
                ))
            })?;

            if entry.file_type().is_file() {
                let path = entry.path();
                let relative_path = path.strip_prefix(&artifacts_dir).map_err(|e| {
                    IggyBenchDashboardServerError::InternalError(format!(
                        "Error creating relative path: {}",
                        e
                    ))
                })?;

                zip.start_file(
                    relative_path.to_string_lossy().into_owned(),
                    options.clone(),
                )
                .map_err(|e| {
                    IggyBenchDashboardServerError::InternalError(format!(
                        "Error adding file to zip: {}",
                        e
                    ))
                })?;

                let mut file = std::fs::File::open(path).map_err(|e| {
                    IggyBenchDashboardServerError::InternalError(format!(
                        "Error opening file: {}",
                        e
                    ))
                })?;
                std::io::copy(&mut file, &mut zip).map_err(|e| {
                    IggyBenchDashboardServerError::InternalError(format!(
                        "Error copying file to zip: {}",
                        e
                    ))
                })?;
            }
        }

        zip.finish().map_err(|e| {
            IggyBenchDashboardServerError::InternalError(format!(
                "Error finalizing zip file: {}",
                e
            ))
        })?;
    }

    info!(
        "{}: Successfully created zip archive for test artifacts of uuid '{}'",
        client_addr, uuid_str
    );

    Ok(HttpResponse::Ok()
        .content_type("application/zip")
        .append_header((
            "Content-Disposition",
            format!("attachment; filename=\"test_artifacts_{}.zip\"", uuid_str),
        ))
        .body(zip_buffer))
}

#[get("/api/recent/{limit}")]
pub async fn get_recent_benchmarks(
    data: web::Data<AppState>,
    path: web::Path<usize>,
    req: HttpRequest,
) -> Result<HttpResponse> {
    let client_addr = get_client_addr(&req);
    let limit = path.into_inner();
    info!(
        "{}: Requesting recent benchmarks with limit {}",
        client_addr, limit
    );

    let benchmarks = data.cache.get_recent_benchmarks(limit);

    info!(
        "{}: Found {} recent benchmarks",
        client_addr,
        benchmarks.len()
    );

    Ok(HttpResponse::Ok().json(benchmarks))
}

fn get_client_addr(req: &HttpRequest) -> String {
    req.connection_info()
        .peer_addr()
        .unwrap_or("unknown")
        .to_string()
}
