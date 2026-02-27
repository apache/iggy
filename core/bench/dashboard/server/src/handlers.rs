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

use crate::render::PngRenderPool;
use crate::{cache::BenchmarkCache, error::IggyBenchDashboardServerError};
use actix_web::{HttpRequest, HttpResponse, get, web};
use serde::Deserialize;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{info, warn};
use uuid::Uuid;
use walkdir::WalkDir;
use zip::{ZipWriter, write::FileOptions};

type Result<T> = std::result::Result<T, IggyBenchDashboardServerError>;

pub struct AppState {
    pub cache: Arc<BenchmarkCache>,
    pub render_pool: Arc<PngRenderPool>,
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
        IggyBenchDashboardServerError::NotFound(format!("Invalid UUID format: '{uuid_str}'"))
    })?;

    let json_path = data.cache.get_benchmark_json_path(&uuid).ok_or_else(|| {
        IggyBenchDashboardServerError::NotFound(format!("Benchmark '{uuid_str}' not found"))
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
                "Invalid UUID format: '{uuid_str}'"
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
                "Benchmark '{uuid_str}' not found"
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
                "Trend data not found for hardware '{hardware}' with params identifier '{params_identifier}'"
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
                "Benchmark '{uuid_str}' not found"
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
                    "Error walking directory: {e}"
                ))
            })?;

            if entry.file_type().is_file() {
                let path = entry.path();
                let relative_path = path.strip_prefix(&artifacts_dir).map_err(|e| {
                    IggyBenchDashboardServerError::InternalError(format!(
                        "Error creating relative path: {e}"
                    ))
                })?;

                zip.start_file(
                    relative_path.to_string_lossy().into_owned(),
                    options.clone(),
                )
                .map_err(|e| {
                    IggyBenchDashboardServerError::InternalError(format!(
                        "Error adding file to zip: {e}"
                    ))
                })?;

                let mut file = std::fs::File::open(path).map_err(|e| {
                    IggyBenchDashboardServerError::InternalError(format!("Error opening file: {e}"))
                })?;
                std::io::copy(&mut file, &mut zip).map_err(|e| {
                    IggyBenchDashboardServerError::InternalError(format!(
                        "Error copying file to zip: {e}"
                    ))
                })?;
            }
        }

        zip.finish().map_err(|e| {
            IggyBenchDashboardServerError::InternalError(format!("Error finalizing zip file: {e}"))
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
            format!("attachment; filename=\"test_artifacts_{uuid_str}.zip\""),
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

fn default_chart_type() -> String {
    "latency".to_string()
}

fn default_theme() -> String {
    "dark".to_string()
}

const DEFAULT_PNG_WIDTH: u32 = 1600;
const DEFAULT_PNG_HEIGHT: u32 = 1200;

fn default_png_width() -> u32 {
    DEFAULT_PNG_WIDTH
}

fn default_png_height() -> u32 {
    DEFAULT_PNG_HEIGHT
}

fn default_legend() -> bool {
    true
}

#[derive(Deserialize)]
pub struct EmbedQuery {
    #[serde(rename = "type", default = "default_chart_type")]
    pub chart_type: String,
    #[serde(default = "default_theme")]
    pub theme: String,
    pub action: Option<String>,
}

#[derive(Deserialize)]
pub struct PngQuery {
    #[serde(rename = "type", default = "default_chart_type")]
    pub chart_type: String,
    #[serde(default = "default_theme")]
    pub theme: String,
    #[serde(default = "default_png_width")]
    pub width: u32,
    #[serde(default = "default_png_height")]
    pub height: u32,
    #[serde(default = "default_legend")]
    pub legend: bool,
}

fn png_cache_path(cache_dir: &std::path::Path, uuid: &Uuid, query: &PngQuery) -> PathBuf {
    let legend_suffix = if query.legend { "" } else { "_nolegend" };
    let filename = format!(
        "{uuid}_{chart_type}_{w}x{h}_{theme}{legend_suffix}.png",
        uuid = uuid,
        chart_type = query.chart_type,
        w = query.width,
        h = query.height,
        theme = query.theme,
    );
    cache_dir.join(filename)
}

#[get("/embed/{uuid}/chart.png")]
pub async fn embed_chart_png(
    data: web::Data<AppState>,
    uuid_str: web::Path<String>,
    query: web::Query<PngQuery>,
    req: HttpRequest,
) -> Result<HttpResponse> {
    let client_addr = get_client_addr(&req);
    info!(
        "{}: Embed PNG request for '{}' (type={}, theme={}, {}x{})",
        client_addr, uuid_str, query.chart_type, query.theme, query.width, query.height
    );

    let uuid = Uuid::parse_str(&uuid_str).map_err(|_| {
        IggyBenchDashboardServerError::InvalidUuid(format!("Invalid UUID format: '{uuid_str}'"))
    })?;

    let chart_type = &query.chart_type;
    if chart_type != "latency" && chart_type != "throughput" && chart_type != "distribution" {
        return Err(IggyBenchDashboardServerError::BadRequest(format!(
            "Invalid chart type: '{chart_type}'. Must be 'latency', 'throughput', or 'distribution'"
        )));
    }

    let cache_dir = data.cache.results_dir().join("embed_cache");
    let cached_path = png_cache_path(&cache_dir, &uuid, &query);

    if cached_path.exists() {
        info!("{}: Serving cached PNG for '{}'", client_addr, uuid_str);
        let bytes = std::fs::read(&cached_path).map_err(|e| {
            IggyBenchDashboardServerError::InternalError(format!("Failed to read cached PNG: {e}"))
        })?;
        return Ok(HttpResponse::Ok().content_type("image/png").body(bytes));
    }

    let json_path = data.cache.get_benchmark_json_path(&uuid).ok_or_else(|| {
        IggyBenchDashboardServerError::NotFound(format!("Benchmark '{uuid_str}' not found"))
    })?;

    let json_content = std::fs::read_to_string(&json_path).map_err(|e| {
        IggyBenchDashboardServerError::InternalError(format!(
            "Failed to read report file '{}': {}",
            json_path.display(),
            e
        ))
    })?;

    let report: bench_report::report::BenchmarkReport = serde_json::from_str(&json_content)
        .map_err(|e| {
            IggyBenchDashboardServerError::InternalError(format!(
                "Failed to deserialize report: {e}"
            ))
        })?;

    let theme = &query.theme;
    let is_dark = theme == "dark" || theme == "dark_nobg";
    let transparent = theme == "dark_nobg" || theme == "light_nobg";

    if !matches!(
        theme.as_str(),
        "dark" | "light" | "dark_nobg" | "light_nobg"
    ) {
        return Err(IggyBenchDashboardServerError::BadRequest(format!(
            "Invalid theme: '{theme}'. Must be 'dark', 'light', 'dark_nobg', or 'light_nobg'"
        )));
    }

    let hardware_line = format!(
        "{} @ {} (server {})",
        report
            .hardware
            .identifier
            .as_deref()
            .unwrap_or("identifier"),
        report.hardware.cpu_name,
        report.params.gitref.as_deref().unwrap_or("version")
    );

    let chart = if chart_type == "latency" {
        bench_report::create_latency_chart(&report, is_dark, false)
    } else if chart_type == "throughput" {
        bench_report::create_throughput_chart(&report, is_dark, false)
    } else {
        bench_report::create_latency_distribution_chart(&report, is_dark, false)
    };

    // Strip interactive-only components and adapt layout for static PNG
    let mut chart_value = serde_json::to_value(&chart).map_err(|e| {
        IggyBenchDashboardServerError::InternalError(format!(
            "Failed to serialize chart for PNG: {e}"
        ))
    })?;
    if let Some(obj) = chart_value.as_object_mut() {
        obj.remove("dataZoom");
        obj.insert("toolbox".to_string(), serde_json::json!({ "show": false }));
        let series_count = obj
            .get("series")
            .and_then(|s| s.as_array())
            .map(|s| s.len())
            .unwrap_or(0);
        if transparent {
            obj.insert(
                "backgroundColor".to_string(),
                serde_json::json!("transparent"),
            );
        } else if !is_dark {
            obj.insert("backgroundColor".to_string(), serde_json::json!("#ffffff"));
        }
        // Scale text relative to image dimensions (baseline: 1200x800)
        let scale = (query.width as f64 / 1200.0).min(query.height as f64 / 800.0);
        if let Some(t) = obj
            .get_mut("title")
            .and_then(|t| t.as_array_mut())
            .and_then(|titles| titles.first_mut())
            .and_then(|t| t.as_object_mut())
        {
            // Prepend hardware identifier to existing subtext
            if let Some(existing) = t.get("subtext").and_then(|s| s.as_str()) {
                t.insert(
                    "subtext".to_string(),
                    serde_json::json!(format!("{hardware_line}\n{existing}")),
                );
            }
            t.insert(
                "subtextStyle".to_string(),
                serde_json::json!({
                    "fontSize": (10.0 * scale).round() as u32,
                    "lineHeight": (14.0 * scale).round() as u32,
                }),
            );
            t.insert(
                "textStyle".to_string(),
                serde_json::json!({ "fontSize": (16.0 * scale).round() as u32 }),
            );
        }
        if let Some(g) = obj
            .get_mut("grid")
            .and_then(|g| g.as_array_mut())
            .and_then(|grids| grids.first_mut())
            .and_then(|g| g.as_object_mut())
        {
            g.insert("left".to_string(), serde_json::json!("12%"));
            g.insert("top".to_string(), serde_json::json!("20%"));
            g.insert("bottom".to_string(), serde_json::json!("10%"));
            if !query.legend {
                g.insert("right".to_string(), serde_json::json!("10%"));
            } else if series_count > 20 {
                g.insert("right".to_string(), serde_json::json!("25%"));
            }
        }
        if !query.legend {
            obj.remove("legend");
        } else if let Some(leg) = obj
            .get_mut("legend")
            .and_then(|l| l.as_array_mut())
            .and_then(|legends| legends.first_mut())
            .and_then(|l| l.as_object_mut())
        {
            {
                let available_height = query.height as f64 * 0.75;
                let entry_height = available_height / series_count.max(1) as f64;
                let font_size = (entry_height * 0.55).clamp(6.0, 10.0 * scale).round() as u32;
                let item_gap = (entry_height * 0.2).clamp(1.0, 10.0).round() as u32;
                let item_height = font_size.max(4);
                let item_width = (item_height as f64 * 1.5).round() as u32;

                leg.insert(
                    "textStyle".to_string(),
                    serde_json::json!({ "fontSize": font_size }),
                );
                leg.insert("itemGap".to_string(), serde_json::json!(item_gap));
                leg.insert("itemWidth".to_string(), serde_json::json!(item_width));
                leg.insert("itemHeight".to_string(), serde_json::json!(item_height));
            }
        }
    }
    let chart: charming::Chart = serde_json::from_value(chart_value).map_err(|e| {
        IggyBenchDashboardServerError::InternalError(format!(
            "Failed to rebuild chart for PNG: {e}"
        ))
    })?;

    let png_bytes = data
        .render_pool
        .render_png(chart, query.width, query.height, is_dark)
        .await
        .map_err(IggyBenchDashboardServerError::InternalError)?;

    std::fs::create_dir_all(&cache_dir).map_err(|e| {
        IggyBenchDashboardServerError::InternalError(format!(
            "Failed to create embed cache directory: {e}"
        ))
    })?;
    if let Err(e) = std::fs::write(&cached_path, &png_bytes) {
        warn!("Failed to cache PNG to {}: {}", cached_path.display(), e);
    }

    info!(
        "{}: Generated and cached PNG for '{}' ({}x{})",
        client_addr, uuid_str, query.width, query.height
    );

    Ok(HttpResponse::Ok().content_type("image/png").body(png_bytes))
}

#[get("/embed/{uuid}")]
pub async fn embed_chart(
    data: web::Data<AppState>,
    uuid_str: web::Path<String>,
    query: web::Query<EmbedQuery>,
    req: HttpRequest,
) -> Result<HttpResponse> {
    let client_addr = get_client_addr(&req);
    info!(
        "{}: Embed chart request for '{}' (type={}, theme={})",
        client_addr, uuid_str, query.chart_type, query.theme
    );

    let uuid = Uuid::parse_str(&uuid_str).map_err(|_| {
        IggyBenchDashboardServerError::InvalidUuid(format!("Invalid UUID format: '{uuid_str}'"))
    })?;

    let is_dark = query.theme != "light";
    let chart_type = &query.chart_type;
    if chart_type != "latency" && chart_type != "throughput" && chart_type != "distribution" {
        return Err(IggyBenchDashboardServerError::BadRequest(format!(
            "Invalid chart type: '{chart_type}'. Must be 'latency', 'throughput', or 'distribution'"
        )));
    }

    let json_path = data.cache.get_benchmark_json_path(&uuid).ok_or_else(|| {
        IggyBenchDashboardServerError::NotFound(format!("Benchmark '{uuid_str}' not found"))
    })?;

    let json_content = std::fs::read_to_string(&json_path).map_err(|e| {
        IggyBenchDashboardServerError::InternalError(format!(
            "Failed to read report file '{}': {}",
            json_path.display(),
            e
        ))
    })?;

    let report: bench_report::report::BenchmarkReport = serde_json::from_str(&json_content)
        .map_err(|e| {
            IggyBenchDashboardServerError::InternalError(format!(
                "Failed to deserialize report: {e}"
            ))
        })?;

    let hardware_line = format!(
        "{} @ {} (server {})",
        report
            .hardware
            .identifier
            .as_deref()
            .unwrap_or("identifier"),
        report.hardware.cpu_name,
        report.params.gitref.as_deref().unwrap_or("version")
    );

    let chart = if chart_type == "latency" {
        bench_report::create_latency_chart(&report, is_dark, false)
    } else if chart_type == "throughput" {
        bench_report::create_throughput_chart(&report, is_dark, false)
    } else {
        bench_report::create_latency_distribution_chart(&report, is_dark, false)
    };

    let chart_json = serde_json::to_string(&chart).map_err(|e| {
        IggyBenchDashboardServerError::InternalError(format!(
            "Failed to serialize chart to JSON: {e}"
        ))
    })?;

    let is_download = query.action.as_deref() == Some("download");
    let bg_color = if is_dark { "#070C18" } else { "#ffffff" };

    let hardware_line_js = hardware_line.replace('\\', "\\\\").replace('\'', "\\'");
    let adapt_layout_js = format!(
        r#"
  var hardwareLine = '{hardware_line_js}';
  function adaptLayout() {{
    var w = window.innerWidth;
    var h = window.innerHeight;
    var scale = Math.min(w / 1000, h / 600);

    if (option.title && option.title.length) {{
      var t = option.title[0];
      t.textStyle = Object.assign(t.textStyle || {{}}, {{ fontSize: Math.round(18 * scale) }});
      if (t.subtext && t.subtext.indexOf(hardwareLine) === -1) {{
        t.subtext = hardwareLine + '\n' + t.subtext;
      }}
      if (t.subtext) {{
        t.subtextStyle = Object.assign(t.subtextStyle || {{}}, {{
          fontSize: Math.round(11 * scale),
          lineHeight: Math.round(15 * scale)
        }});
      }}
    }}
    if (option.grid && option.grid.length) {{
      option.grid[0].top = '18%';
      option.grid[0].left = '8%';
      option.grid[0].bottom = '14%';
    }}
    if (option.dataZoom && option.dataZoom.length) {{
      option.dataZoom[0].bottom = '3%';
    }}
    if (option.legend && option.legend.length) {{
      option.legend[0].textStyle = Object.assign(option.legend[0].textStyle || {{}}, {{
        fontSize: Math.round(10 * scale)
      }});
    }}

    chart.setOption(option);
    chart.resize();
  }}"#
    );

    let html = if is_download {
        format!(
            r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Downloading chart...</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5.5.1/dist/echarts.min.js"></script>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ background: transparent; }}
  #chart {{ width: 1200px; height: 800px; }}
  #status {{ position: fixed; top: 50%; left: 50%; transform: translate(-50%, -50%);
    font-family: sans-serif; color: #888; font-size: 16px; }}
</style>
</head>
<body>
<div id="chart"></div>
<div id="status">Generating PNG...</div>
<script>
  var chart = echarts.init(document.getElementById('chart'));
  var option = {chart_json};
  {adapt_layout_js}
  adaptLayout();
  chart.on('finished', function() {{
    var url = chart.getDataURL({{ type: 'png', pixelRatio: 2, backgroundColor: 'rgba(0,0,0,0)' }});
    var a = document.createElement('a');
    a.href = url;
    a.download = '{uuid_str}_{chart_type}.png';
    document.body.appendChild(a);
    a.click();
    document.getElementById('status').textContent = 'Download started. You can close this tab.';
  }});
</script>
</body>
</html>"#
        )
    } else {
        format!(
            r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Iggy Benchmark Chart</title>
<script src="https://cdn.jsdelivr.net/npm/echarts@5.5.1/dist/echarts.min.js"></script>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ background: {bg_color}; overflow: hidden; }}
  #chart {{ width: 100%; height: 100vh; }}
</style>
</head>
<body>
<div id="chart"></div>
<script>
  var chart = echarts.init(document.getElementById('chart'));
  var option = {chart_json};
  {adapt_layout_js}
  adaptLayout();
  window.addEventListener('resize', function() {{ adaptLayout(); }});
</script>
</body>
</html>"#
        )
    };

    Ok(HttpResponse::Ok().content_type("text/html").body(html))
}

fn get_client_addr(req: &HttpRequest) -> String {
    req.connection_info()
        .peer_addr()
        .unwrap_or("unknown")
        .to_string()
}
