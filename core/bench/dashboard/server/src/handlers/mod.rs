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

pub mod omb;

use crate::artifacts;
use crate::cache::{SeriesBuilder, SeriesKey};
use crate::embed_cache::EmbedCache;
use crate::github::StarCount;
use crate::render::{EmbedChart, PngRenderPool, without_raw_mark};
use crate::{cache::BenchmarkCache, error::IggyBenchDashboardServerError};
use actix_files::NamedFile;
use actix_web::http::header::{
    CacheControl, CacheDirective, ContentDisposition, ContentType, DispositionParam,
    DispositionType,
};
use actix_web::{HttpRequest, HttpResponse, get, mime, web};
use bench_dashboard_shared::config::DashboardConfig;
use bench_dashboard_shared::embed::{ChartType, PngSize};
use bench_dashboard_shared::github::GithubStars;
use bench_dashboard_shared::series::{SeriesMetric, SeriesScope};
use charming::Chart;
use serde::Deserialize;
use std::fmt;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Instant;
use tracing::{error, info, warn};
use uuid::Uuid;

type Result<T> = std::result::Result<T, IggyBenchDashboardServerError>;

/// Same-origin copy of the ECharts build that `charming` bundles, placed in the frontend dist
/// by `frontend/scripts/copy_echarts.sh`.
const ECHARTS_SCRIPT_PATH: &str = "/vendor/echarts-5.5.1.min.js";
/// How long a browser or a proxy can keep an iggy-bench PNG. A report seldom changes after upload,
/// and the max age bounds how long a reader sees the old one.
const IGGY_BENCH_PNG_MAX_AGE_SECS: u32 = 86_400;
/// Shorter than the PNG max age, because the page loads `ECHARTS_SCRIPT_PATH`, and an upgrade that
/// changes the ECharts version removes that file. A page kept for longer shows an empty frame.
const IGGY_BENCH_PAGE_MAX_AGE_SECS: u32 = 3600;
const ARTIFACTS_CACHE_DIR: &str = "artifacts";
const UNKNOWN_API_PATH: &str = "Unknown API path";

pub struct AppState {
    pub cache: Arc<BenchmarkCache>,
    pub series_builder: Arc<SeriesBuilder>,
    pub render_pool: Arc<PngRenderPool>,
    pub embed_cache: Arc<EmbedCache>,
    pub cache_dir: PathBuf,
    pub config: Arc<DashboardConfig>,
}

#[get("/health")]
pub async fn health_check(req: HttpRequest) -> Result<HttpResponse> {
    let client_addr = get_client_addr(&req);
    info!("{}: Health check request", client_addr);
    Ok(HttpResponse::Ok().json(serde_json::json!({ "status": "healthy" })))
}

#[get("/api/benchmarks")]
pub async fn list_benchmark_summaries(
    data: web::Data<AppState>,
    req: HttpRequest,
) -> Result<HttpResponse> {
    let client_addr = get_client_addr(&req);
    let summaries = data.cache.get_summaries();
    info!(
        "{}: Listing {} benchmark summaries",
        client_addr,
        summaries.len()
    );
    Ok(HttpResponse::Ok().json(summaries.as_slice()))
}

#[get("/api/config")]
pub async fn get_dashboard_config(
    data: web::Data<AppState>,
    req: HttpRequest,
) -> Result<HttpResponse> {
    info!("{}: Getting dashboard configuration", get_client_addr(&req));
    Ok(HttpResponse::Ok().json(data.config.as_ref()))
}

/// The last star count that the server got from GitHub. The top bar asks once per page load, and
/// the server answers from memory.
#[get("/api/github/stars")]
pub async fn get_github_stars(stars: web::Data<StarCount>) -> HttpResponse {
    HttpResponse::Ok().json(GithubStars { stars: stars.get() })
}

#[get("/api/benchmark/full/{unique_id}")]
pub async fn get_benchmark_report_full(
    data: web::Data<AppState>,
    uuid_str: web::Path<String>,
    req: HttpRequest,
) -> Result<NamedFile> {
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

    let report = NamedFile::open(&json_path).map_err(|e| {
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
    Ok(report)
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

#[derive(Deserialize)]
struct SeriesQuery {
    metric: SeriesMetric,
    scope: SeriesScope,
}

#[get("/api/benchmark/series/{unique_id}")]
pub async fn get_benchmark_series(
    data: web::Data<AppState>,
    uuid_str: web::Path<String>,
    req: HttpRequest,
) -> Result<HttpResponse> {
    let client_addr = get_client_addr(&req);
    info!(
        "{}: Requesting series of '{}' with '{}'",
        client_addr,
        uuid_str,
        req.query_string()
    );

    let uuid = Uuid::parse_str(&uuid_str).map_err(|_| {
        IggyBenchDashboardServerError::InvalidUuid(format!("Invalid UUID format: '{uuid_str}'"))
    })?;
    let query = web::Query::<SeriesQuery>::from_query(req.query_string())
        .map_err(|e| IggyBenchDashboardServerError::BadRequest(e.to_string()))?;
    let key = SeriesKey {
        uuid,
        metric: query.metric,
        scope: query.scope,
    };

    // Taken before the report path, so a reload from here on keeps this build out of the cache.
    let generation = data.cache.series().generation();
    let json_path = data.cache.get_benchmark_json_path(&uuid).ok_or_else(|| {
        IggyBenchDashboardServerError::NotFound(format!("Benchmark '{uuid_str}' not found"))
    })?;

    let json = match data.cache.series().get(&key) {
        Some(json) => {
            info!(
                "{}: Serving {} cached bytes of series of '{}'",
                client_addr,
                json.len(),
                uuid_str
            );
            json
        }
        None => {
            let start = Instant::now();
            let json = data
                .series_builder
                .build(json_path, key, generation)
                .await
                .inspect_err(|e| {
                    error!("{client_addr}: Failed to build series of '{uuid_str}': {e}");
                })?;
            info!(
                "{}: Built {} bytes of series of '{}' in {:.2?}",
                client_addr,
                json.len(),
                uuid_str,
                start.elapsed()
            );
            json
        }
    };

    Ok(HttpResponse::Ok()
        .content_type(ContentType::json())
        .body(json))
}

#[get("/api/artifacts/{uuid}")]
pub async fn get_test_artifacts_zip(
    data: web::Data<AppState>,
    uuid_str: web::Path<String>,
    req: HttpRequest,
) -> Result<NamedFile> {
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

    let zip_path = data
        .cache_dir
        .join(ARTIFACTS_CACHE_DIR)
        .join(format!("{uuid}.zip"));
    let target = zip_path.clone();
    let rebuilt = web::block(move || {
        artifacts::refresh_zip(&artifacts_dir, &target).map_err(|e| {
            IggyBenchDashboardServerError::InternalError(format!(
                "Failed to zip '{}' into '{}': {e}",
                artifacts_dir.display(),
                target.display()
            ))
        })
    })
    .await
    .map_err(|e| {
        IggyBenchDashboardServerError::InternalError(format!("Artifacts zip task failed: {e}"))
    })??;
    if rebuilt {
        info!(
            "{}: Created zip archive for test artifacts of uuid '{}'",
            client_addr, uuid_str
        );
    }

    Ok(
        NamedFile::open(&zip_path)?.set_content_disposition(ContentDisposition {
            disposition: DispositionType::Attachment,
            parameters: vec![DispositionParam::Filename(format!(
                "test_artifacts_{uuid}.zip"
            ))],
        }),
    )
}

fn default_chart_type() -> String {
    "latency".to_string()
}

fn default_theme() -> String {
    "dark".to_string()
}

const IGGY_BENCH_PAGE_TITLE: &str = "Iggy Benchmark Chart";

fn default_png_width() -> u32 {
    PngSize::DEFAULT.width()
}

fn default_png_height() -> u32 {
    PngSize::DEFAULT.height()
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

impl EmbedQuery {
    fn is_download(&self) -> bool {
        self.action.as_deref() == Some("download")
    }
}

impl PngQuery {
    /// The checked theme, size and legend of the query.
    fn variant(&self) -> Result<PngVariant> {
        Ok(PngVariant {
            theme: self.theme.parse()?,
            size: PngSize::try_from((self.width, self.height))?,
            legend: self.legend,
        })
    }
}

/// One PNG of a chart. The server draws only the sizes of [`PngSize::ALL`], so each chart has a
/// fixed set of variants, and the cache holds a fixed number of PNG files per chart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PngVariant {
    theme: PngTheme,
    size: PngSize,
    legend: bool,
}

/// The look of a PNG chart, named as in the `theme` query parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PngTheme {
    dark: bool,
    /// The `_nobg` themes leave the background out.
    transparent: bool,
}

impl PngTheme {
    const ALL: [Self; 4] = [
        Self {
            dark: true,
            transparent: false,
        },
        Self {
            dark: false,
            transparent: false,
        },
        Self {
            dark: true,
            transparent: true,
        },
        Self {
            dark: false,
            transparent: true,
        },
    ];

    const fn name(self) -> &'static str {
        match (self.dark, self.transparent) {
            (true, false) => "dark",
            (false, false) => "light",
            (true, true) => "dark_nobg",
            (false, true) => "light_nobg",
        }
    }
}

impl FromStr for PngTheme {
    type Err = IggyBenchDashboardServerError;

    fn from_str(theme: &str) -> Result<Self> {
        Self::ALL
            .into_iter()
            .find(|png_theme| png_theme.name() == theme)
            .ok_or_else(|| {
                IggyBenchDashboardServerError::BadRequest(format!(
                    "Invalid theme: '{theme}'. Must be 'dark', 'light', 'dark_nobg', or 'light_nobg'"
                ))
            })
    }
}

impl fmt::Display for PngTheme {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// The name of a cached PNG. `run` starts it, so the files of two runs never share a name. The
/// chart type and the variant come from a checked query, so each run has a fixed set of names.
fn png_file_name(
    run: impl fmt::Display,
    chart_type: impl fmt::Display,
    variant: PngVariant,
) -> String {
    let PngVariant {
        theme,
        size,
        legend,
    } = variant;
    let legend_suffix = if legend { "" } else { "_nolegend" };
    format!("{run}_{chart_type}_{size}_{theme}{legend_suffix}.png")
}

/// The name of a cached embed page. A page has a dark and a light look only, so any other theme in
/// the query gives the name of the dark page.
fn page_file_name(
    run: impl fmt::Display,
    chart_type: impl fmt::Display,
    dark: bool,
    download: bool,
) -> String {
    let theme = if dark { "dark" } else { "light" };
    let action = if download { "_download" } else { "" };
    format!("{run}_{chart_type}_{theme}{action}.html")
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
    let chart_type: ChartType = query.chart_type.parse()?;
    let variant = query.variant()?;
    // A cached PNG stays on the disk after its run is gone, so the lookup comes first.
    let EmbedReport {
        json_path,
        file_prefix,
    } = embed_report(&data, &uuid, &uuid_str)?;

    let name = png_file_name(file_prefix, chart_type, variant);
    let runs = Arc::clone(&data.cache);
    let render_pool = Arc::clone(&data.render_pool);
    let build = async move {
        let subtext = embed_subtext(&runs, &uuid)?;
        let chart = render_pool
            .load_chart(json_path, chart_type, subtext, variant.theme.dark)
            .await?;
        render_png(&render_pool, chart, variant).await
    };
    let png = data.embed_cache.get_or_build(&name, build).await?;
    Ok(embed_response(
        mime::IMAGE_PNG,
        png,
        IGGY_BENCH_PNG_MAX_AGE_SECS,
    ))
}

/// What the embeds of a run take from the loaded runs before they read the cache.
struct EmbedReport {
    json_path: PathBuf,
    /// Starts the names of the cached files of the run. The stamp of the report file is in it, so
    /// a report that changes gets new files after the next load.
    file_prefix: String,
}

fn embed_report(data: &AppState, uuid: &Uuid, uuid_str: &str) -> Result<EmbedReport> {
    let (json_path, stamp) = data.cache.get_report_file(uuid).ok_or_else(|| {
        IggyBenchDashboardServerError::NotFound(format!("Benchmark '{uuid_str}' not found"))
    })?;
    Ok(EmbedReport {
        json_path,
        file_prefix: format!("{uuid}_{stamp}"),
    })
}

/// The subtext of the embed charts from the loaded light report, whose throughput lines match the
/// dashboard pages. Only a build needs it, so a request that the cache answers copies no report.
fn embed_subtext(runs: &BenchmarkCache, uuid: &Uuid) -> Result<String> {
    let report = runs.get_benchmark_report_light(uuid).ok_or_else(|| {
        IggyBenchDashboardServerError::NotFound(format!("Benchmark '{uuid}' not found"))
    })?;
    Ok(report.subtext())
}

fn embed_response(content_type: mime::Mime, body: web::Bytes, max_age_secs: u32) -> HttpResponse {
    HttpResponse::Ok()
        .content_type(content_type)
        .insert_header(CacheControl(vec![
            CacheDirective::Public,
            CacheDirective::MaxAge(max_age_secs),
        ]))
        .body(body)
}

/// Renders `chart` as the PNG `variant`.
async fn render_png(
    render_pool: &PngRenderPool,
    chart: EmbedChart,
    variant: PngVariant,
) -> Result<web::Bytes> {
    let PngVariant {
        theme,
        size,
        legend,
    } = variant;
    let chart = png_chart(chart, size.width(), size.height(), legend, theme)?;
    let png = render_pool
        .render_png(chart, size.width(), size.height(), theme.dark)
        .await
        .map_err(IggyBenchDashboardServerError::InternalError)?;
    Ok(web::Bytes::from(png))
}

/// Strips the interactive parts of the chart, puts the hardware line above its subtext, and
/// scales its text and legend to the image size.
fn png_chart(
    chart: EmbedChart,
    width: u32,
    height: u32,
    legend: bool,
    theme: PngTheme,
) -> Result<Chart> {
    let EmbedChart {
        chart,
        hardware_line,
    } = chart;
    let PngTheme {
        dark: is_dark,
        transparent,
    } = theme;

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
        let scale = (width as f64 / 1200.0).min(height as f64 / 800.0);
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
        if let Some(titles) = obj.get_mut("title").and_then(|t| t.as_array_mut()) {
            plain_titles(titles);
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
            if !legend {
                g.insert("right".to_string(), serde_json::json!("10%"));
            } else if series_count > 20 {
                g.insert("right".to_string(), serde_json::json!("25%"));
            }
        }
        if !legend {
            obj.remove("legend");
        } else if let Some(leg) = obj.get_mut("legend").and_then(first_legend) {
            {
                let available_height = height as f64 * 0.75;
                let entry_height = available_height / series_count.max(1) as f64;
                // In a small image the cap falls below the floor, and `clamp` panics on that.
                let font_size = (entry_height * 0.55).min(10.0 * scale).max(6.0).round() as u32;
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
    serde_json::from_value(chart_value).map_err(|e| {
        IggyBenchDashboardServerError::InternalError(format!(
            "Failed to rebuild chart for PNG: {e}"
        ))
    })
}

/// The first legend of a chart option. charming writes a single legend as an object and several
/// legends as an array.
fn first_legend(
    legend: &mut serde_json::Value,
) -> Option<&mut serde_json::Map<String, serde_json::Value>> {
    match legend {
        serde_json::Value::Array(legends) => legends.first_mut()?.as_object_mut(),
        legend => legend.as_object_mut(),
    }
}

/// The renderer writes the chart with `Chart::to_string`, and the titles hold text from the result
/// files and the hardware line, so that text loses charming's raw mark.
fn plain_titles(titles: &mut [serde_json::Value]) {
    for title in titles.iter_mut().filter_map(|title| title.as_object_mut()) {
        for key in ["text", "subtext"] {
            if let Some(value) = title.get_mut(key)
                && let Some(text) = value.as_str()
            {
                *value = serde_json::Value::from(without_raw_mark(text));
            }
        }
    }
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

    let dark = query.theme != "light";
    let chart_type: ChartType = query.chart_type.parse()?;
    let download = query.is_download();
    let EmbedReport {
        json_path,
        file_prefix,
    } = embed_report(&data, &uuid, &uuid_str)?;

    let name = page_file_name(file_prefix, chart_type, dark, download);
    let runs = Arc::clone(&data.cache);
    let render_pool = Arc::clone(&data.render_pool);
    let build = async move {
        let subtext = embed_subtext(&runs, &uuid)?;
        let EmbedChart {
            chart,
            hardware_line,
        } = render_pool
            .load_chart(json_path, chart_type, subtext, dark)
            .await?;
        let option = serde_json::to_string(&chart).map_err(|e| {
            IggyBenchDashboardServerError::InternalError(format!(
                "Failed to serialize chart to JSON: {e}"
            ))
        })?;
        let page = EmbedPage {
            option: &option,
            hardware_line: &hardware_line,
            dark,
            title: IGGY_BENCH_PAGE_TITLE,
            download_name: download.then(|| format!("{uuid}_{chart_type}.png")),
        };
        Ok::<_, IggyBenchDashboardServerError>(web::Bytes::from(page.html()))
    };
    let page = data.embed_cache.get_or_build(&name, build).await?;
    Ok(embed_response(
        mime::TEXT_HTML,
        page,
        IGGY_BENCH_PAGE_MAX_AGE_SECS,
    ))
}

/// A page with one interactive chart, for an iframe, or with `action=download` a page that saves
/// the chart as a PNG file in the browser.
struct EmbedPage<'a> {
    /// The ECharts option as a JavaScript expression.
    option: &'a str,
    /// The page puts it above the chart's subtext.
    hardware_line: &'a str,
    dark: bool,
    title: &'a str,
    /// The name of the PNG file that a download page saves.
    download_name: Option<String>,
}

impl EmbedPage<'_> {
    fn html(&self) -> String {
        let option = script_safe(self.option);
        let hardware_line = script_string(self.hardware_line);
        let title = self.title;
        let bg_color = if self.dark { "#070C18" } else { "#ffffff" };
        // The chart option sets no text colors. Without the theme, ECharts draws the text of a dark
        // page in its dark grey defaults for a white page. The dashboard and the PNG use this theme.
        let echarts_theme = if self.dark { "'dark'" } else { "null" };

        let adapt_layout_js = format!(
            r#"
  var hardwareLine = {hardware_line};
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
    var legend = Array.isArray(option.legend) ? option.legend[0] : option.legend;
    if (legend) {{
      legend.textStyle = Object.assign(legend.textStyle || {{}}, {{
        fontSize: Math.round(10 * scale)
      }});
    }}

    chart.setOption(option);
    chart.resize();
  }}"#
        );

        match &self.download_name {
            Some(download_name) => {
                let download_name = script_string(download_name);
                format!(
                    r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>Downloading chart...</title>
<script src="{ECHARTS_SCRIPT_PATH}"></script>
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
  var chart = echarts.init(document.getElementById('chart'), {echarts_theme});
  var option = {option};
  {adapt_layout_js}
  adaptLayout();
  chart.on('finished', function() {{
    var url = chart.getDataURL({{ type: 'png', pixelRatio: 2, backgroundColor: 'rgba(0,0,0,0)' }});
    var a = document.createElement('a');
    a.href = url;
    a.download = {download_name};
    document.body.appendChild(a);
    a.click();
    document.getElementById('status').textContent = 'Download started. You can close this tab.';
  }});
</script>
</body>
</html>"#
                )
            }
            None => format!(
                r#"<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<script src="{ECHARTS_SCRIPT_PATH}"></script>
<style>
  * {{ margin: 0; padding: 0; box-sizing: border-box; }}
  body {{ background: {bg_color}; overflow: hidden; }}
  #chart {{ width: 100%; height: 100vh; }}
</style>
</head>
<body>
<div id="chart"></div>
<script>
  var chart = echarts.init(document.getElementById('chart'), {echarts_theme});
  var option = {option};
  {adapt_layout_js}
  adaptLayout();
  window.addEventListener('resize', function() {{ adaptLayout(); }});
</script>
</body>
</html>"#
            ),
        }
    }
}

/// `script` with every `</` and `<!--` escaped, so no text in it can end the `<script>` element
/// early. After `<!--<script`, the HTML parser does not take the next `</script>` as the end of
/// the element.
fn script_safe(script: &str) -> String {
    script.replace("</", "<\\/").replace("<!--", "<\\!--")
}

/// `text` as a JavaScript string literal that can stand in a `<script>` element.
fn script_string(text: &str) -> String {
    script_safe(&serde_json::Value::from(text).to_string())
}

/// Default service of the `/api` scope, so an unknown API path returns 404 JSON instead of the
/// frontend page.
pub async fn unknown_api_path(req: HttpRequest) -> Result<HttpResponse> {
    Err(IggyBenchDashboardServerError::NotFound(format!(
        "{UNKNOWN_API_PATH} '{}'",
        req.path()
    )))
}

fn get_client_addr(req: &HttpRequest) -> String {
    req.connection_info()
        .peer_addr()
        .unwrap_or("unknown")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::read_report;
    use crate::config;
    use crate::embed_cache::tests::names_in;
    use crate::embed_cache::{
        BYTES_PER_GIB, DEFAULT_MAX_GIB, EMBED_CACHE_DIR, FileStamp, TEST_BUILD, stamped_name,
    };
    use crate::render::RAW_MARK;
    use actix_web::dev::ServiceResponse;
    use actix_web::http::StatusCode;
    use actix_web::http::header::{CACHE_CONTROL, CONTENT_TYPE};
    use actix_web::{App, test};
    use bench_dashboard_shared::metrics::GroupMetrics;
    use bench_dashboard_shared::series::BenchmarkSeries;
    use bench_dashboard_shared::summary::BenchmarkSummary;
    use bench_report::group_metrics_kind::GroupMetricsKind;
    use charming::component::Title;
    use std::collections::HashSet;
    use std::path::Path;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const FIXTURE_RESULTS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../fixtures/results");
    const SHIPPED_CONFIG: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../dashboard.toml");
    /// An old cluster run left its bench.log in a sibling directory without a report.
    const RUN_DIR_WITHOUT_REPORT: &str = "rc1-cluster3-pinned-producer-20p-800MB-bench-log";
    /// 8 producers, 8 consumers, and the groups of producers, consumers, and both.
    pub(super) const PRODUCER_AND_CONSUMER_DIR: &str = "old-0.4.202-pinned-producer-and-consumer";
    const PRODUCER_AND_CONSUMER_RUN: &str = "6b1e288a-9155-4da3-8d3f-ec4d929de3d3";
    const TWO_PRODUCERS_DIR: &str = "phase1a-pinned-producer-2p-20MB";
    const TWO_PRODUCERS_RUN: &str = "b5c560ad-1ad2-49f0-88dd-c29db75126aa";
    /// 20 producers on 3 nodes. The report sums the actors' own rates to 801.85 MB/s, and the
    /// dashboard measures 784.25 MB/s.
    const CLUSTER_RUN: &str = "48587edc-6d6b-4ab5-8316-c64be4a65135";
    const CLUSTER_RUN_SUMMED_RATE: &str = "801.85";
    const EVERY_CHART_TYPE: [&str; 3] = ["latency", "throughput", "distribution"];
    const RESULTS_DIR: &str = "results";
    /// The fixture reports keep 3 points of every series.
    const FIXTURE_SERIES_POINTS: usize = 3;
    const LATENCY_OF_GROUPS: &str = "metric=latency&scope=group";
    const REPORT_FILE: &str = "report.json";
    const JSON_CONTENT_TYPE: &str = "application/json";

    pub(super) fn app_state() -> AppState {
        app_state_with_results(Path::new(FIXTURE_RESULTS_DIR))
    }

    fn app_state_with_results(results_dir: &Path) -> AppState {
        let config = config::load(Path::new(SHIPPED_CONFIG)).expect("the shipped config is valid");
        let cache = BenchmarkCache::new(results_dir.to_path_buf(), config.thresholds);
        cache.load().expect("the results directory is readable");
        let cache = Arc::new(cache);
        AppState {
            series_builder: Arc::new(
                SeriesBuilder::start(Arc::clone(&cache)).expect("the series build thread starts"),
            ),
            cache,
            render_pool: Arc::new(PngRenderPool::new().expect("the render thread starts")),
            embed_cache: Arc::new(EmbedCache::temporary()),
            cache_dir: std::env::temp_dir(),
            config: Arc::new(config),
        }
    }

    #[actix_web::test]
    async fn given_fixture_runs_when_listing_benchmarks_should_return_every_run_newest_first() {
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_state()))
                .service(list_benchmark_summaries),
        )
        .await;

        let response = test::call_service(
            &app,
            test::TestRequest::get().uri("/api/benchmarks").to_request(),
        )
        .await;

        assert!(response.status().is_success(), "{}", response.status());
        let summaries: Vec<BenchmarkSummary> = test::read_body_json(response).await;
        let runs: Vec<_> = summaries
            .iter()
            .map(|summary| (summary.uuid.to_string(), summary.version.as_str()))
            .collect();
        assert!(
            Path::new(FIXTURE_RESULTS_DIR)
                .join(RUN_DIR_WITHOUT_REPORT)
                .join("bench.log")
                .is_file()
        );
        assert_eq!(
            runs,
            [
                (
                    "b5c560ad-1ad2-49f0-88dd-c29db75126aa".to_owned(),
                    "0.10.0 at 53e3bf1e3"
                ),
                ("ea431315-8646-43cf-a068-926a8bff09d9".to_owned(), "0.10.0"),
                ("48587edc-6d6b-4ab5-8316-c64be4a65135".to_owned(), "0.10.0"),
                (
                    "d7ef0e8c-d152-4c83-8036-4531c17c63ab".to_owned(),
                    "0.9.0 at 71f29618b"
                ),
                ("6b1e288a-9155-4da3-8d3f-ec4d929de3d3".to_owned(), "0.4.202"),
            ]
        );
    }

    #[actix_web::test]
    async fn given_config_file_when_requesting_config_should_return_parsed_file() {
        let state = app_state();
        let expected = state.config.as_ref().clone();
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(state))
                .service(get_dashboard_config),
        )
        .await;

        let response = test::call_service(
            &app,
            test::TestRequest::get().uri("/api/config").to_request(),
        )
        .await;

        assert!(response.status().is_success(), "{}", response.status());
        let config: DashboardConfig = test::read_body_json(response).await;
        assert_eq!(config, expected);
        assert_eq!(config.suites.len(), 3);
        assert_eq!(config.headlines.len(), 4);
    }

    #[actix_web::test]
    async fn given_star_count_when_requesting_stars_should_answer_it_or_null_before_first_answer() {
        let counted = StarCount::default();
        counted.record(Ok::<_, &str>(5123));

        for (count, expected) in [(StarCount::default(), None), (counted, Some(5123))] {
            let app = test::init_service(
                App::new()
                    .app_data(web::Data::new(count))
                    .service(get_github_stars),
            )
            .await;

            let stars: GithubStars = test::call_and_read_body_json(
                &app,
                test::TestRequest::get()
                    .uri("/api/github/stars")
                    .to_request(),
            )
            .await;

            assert_eq!(stars.stars, expected);
        }
    }

    #[actix_web::test]
    async fn given_unknown_api_path_when_requesting_should_answer_not_found_json() {
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_state()))
                .service(get_dashboard_config)
                .service(web::scope("/api").default_service(web::to(unknown_api_path))),
        )
        .await;

        for uri in ["/api/hardware", "/api/does/not/exist"] {
            let response =
                test::call_service(&app, test::TestRequest::get().uri(uri).to_request()).await;

            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{uri}");
            assert_eq!(
                header(&response, CONTENT_TYPE.as_str()),
                Some(JSON_CONTENT_TYPE),
                "{uri}"
            );
            let body: serde_json::Value = test::read_body_json(response).await;
            assert!(
                body["error"]
                    .as_str()
                    .is_some_and(|error| error.contains(uri)),
                "{uri}: {body}"
            );
        }

        // A handler registered before the scope keeps its path.
        let response = test::call_service(
            &app,
            test::TestRequest::get().uri("/api/config").to_request(),
        )
        .await;
        assert!(response.status().is_success(), "{}", response.status());
    }

    fn series_uri(uuid: &str, query: &str) -> String {
        format!("/api/benchmark/series/{uuid}?{query}")
    }

    fn light_uri(uuid: &str) -> String {
        format!("/api/benchmark/light/{uuid}")
    }

    fn copy_fixture_run(run_dir: &str, results_dir: &Path) {
        let target = results_dir.join(run_dir);
        std::fs::create_dir(&target).expect("the run directory is created");
        std::fs::copy(
            Path::new(FIXTURE_RESULTS_DIR)
                .join(run_dir)
                .join(REPORT_FILE),
            target.join(REPORT_FILE),
        )
        .expect("the fixture report is copied");
    }

    fn producer_and_consumer_key(metric: SeriesMetric, scope: SeriesScope) -> SeriesKey {
        SeriesKey {
            uuid: PRODUCER_AND_CONSUMER_RUN
                .parse()
                .expect("the fixture uuid is valid"),
            metric,
            scope,
        }
    }

    #[actix_web::test]
    async fn given_fixture_run_when_requesting_each_series_should_send_every_point_and_cache_it() {
        let state = web::Data::new(app_state());
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .service(get_benchmark_series),
        )
        .await;

        // The group scope leaves out the combined group of producers and consumers.
        for (query, metric, scope, unit, series_count) in [
            (
                LATENCY_OF_GROUPS,
                SeriesMetric::Latency,
                SeriesScope::Group,
                "ms",
                2,
            ),
            (
                "metric=throughput&scope=group",
                SeriesMetric::Throughput,
                SeriesScope::Group,
                "MB/s",
                2,
            ),
            (
                "metric=latency&scope=actors",
                SeriesMetric::Latency,
                SeriesScope::Actors,
                "ms",
                16,
            ),
            (
                "metric=throughput&scope=actors",
                SeriesMetric::Throughput,
                SeriesScope::Actors,
                "MB/s",
                16,
            ),
        ] {
            let response = test::call_service(
                &app,
                test::TestRequest::get()
                    .uri(&series_uri(PRODUCER_AND_CONSUMER_RUN, query))
                    .to_request(),
            )
            .await;

            assert!(
                response.status().is_success(),
                "{query}: {}",
                response.status()
            );
            let series: BenchmarkSeries = test::read_body_json(response).await;
            assert_eq!(series.metric, metric, "{query}");
            assert_eq!(series.unit, unit, "{query}");
            assert_eq!(series.series.len(), series_count, "{query}");
            for line in &series.series {
                assert_eq!(
                    line.points.len(),
                    FIXTURE_SERIES_POINTS,
                    "{query}: {:?}",
                    line.source
                );
            }
            assert!(
                state
                    .cache
                    .series()
                    .get(&producer_and_consumer_key(metric, scope))
                    .is_some(),
                "{query}"
            );
        }
    }

    #[actix_web::test]
    async fn given_unknown_run_when_requesting_series_should_answer_not_found() {
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_state()))
                .service(get_benchmark_series),
        )
        .await;

        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&series_uri(&Uuid::nil().to_string(), LATENCY_OF_GROUPS))
                .to_request(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body: serde_json::Value = test::read_body_json(response).await;
        assert!(body["error"].is_string(), "{body}");
    }

    #[actix_web::test]
    async fn given_unknown_metric_or_scope_when_requesting_series_should_answer_bad_request() {
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_state()))
                .service(get_benchmark_series),
        )
        .await;

        for uri in [
            series_uri(PRODUCER_AND_CONSUMER_RUN, "metric=p99&scope=group"),
            series_uri(PRODUCER_AND_CONSUMER_RUN, "metric=latency&scope=all"),
            series_uri(PRODUCER_AND_CONSUMER_RUN, "metric=latency"),
            series_uri("not-a-uuid", LATENCY_OF_GROUPS),
        ] {
            let response =
                test::call_service(&app, test::TestRequest::get().uri(&uri).to_request()).await;

            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
            let body: serde_json::Value = test::read_body_json(response).await;
            assert!(body["error"].is_string(), "{uri}: {body}");
        }
    }

    #[actix_web::test]
    async fn given_cached_series_when_results_reload_should_drop_it() {
        let state = web::Data::new(app_state());
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .service(get_benchmark_series),
        )
        .await;
        let key = producer_and_consumer_key(SeriesMetric::Latency, SeriesScope::Group);
        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&series_uri(PRODUCER_AND_CONSUMER_RUN, LATENCY_OF_GROUPS))
                .to_request(),
        )
        .await;
        assert!(response.status().is_success(), "{}", response.status());
        assert!(state.cache.series().get(&key).is_some());

        state
            .cache
            .load()
            .expect("the fixture directory is readable");

        assert!(state.cache.series().get(&key).is_none());
    }

    #[actix_web::test]
    async fn given_series_build_started_before_reload_when_it_finishes_should_stay_out_of_cache() {
        let state = app_state();
        let key = producer_and_consumer_key(SeriesMetric::Latency, SeriesScope::Group);
        let generation = state.cache.series().generation();

        state
            .cache
            .load()
            .expect("the fixture directory is readable");
        state
            .cache
            .series()
            .insert(key, web::Bytes::from_static(b"{}"), generation);

        assert!(state.cache.series().get(&key).is_none());
    }

    #[actix_web::test]
    async fn given_loaded_runs_when_reload_fails_should_keep_serving_them() {
        let root = tempfile::tempdir().expect("a temporary directory is created");
        let results_dir = root.path().join(RESULTS_DIR);
        std::fs::create_dir(&results_dir).expect("the results directory is created");
        copy_fixture_run(PRODUCER_AND_CONSUMER_DIR, &results_dir);
        let state = web::Data::new(app_state_with_results(&results_dir));
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .service(list_benchmark_summaries)
                .service(get_benchmark_report_light),
        )
        .await;
        std::fs::remove_dir_all(&results_dir).expect("the results directory is removed");

        assert!(state.cache.load().is_err());

        let summaries: Vec<BenchmarkSummary> = test::call_and_read_body_json(
            &app,
            test::TestRequest::get().uri("/api/benchmarks").to_request(),
        )
        .await;
        let runs: Vec<_> = summaries
            .iter()
            .map(|summary| summary.uuid.to_string())
            .collect();
        assert_eq!(runs, [PRODUCER_AND_CONSUMER_RUN]);
        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&light_uri(PRODUCER_AND_CONSUMER_RUN))
                .to_request(),
        )
        .await;
        assert!(response.status().is_success(), "{}", response.status());
    }

    #[actix_web::test]
    async fn given_run_removed_when_reloading_should_drop_it_from_list_and_lookups() {
        let results_dir = tempfile::tempdir().expect("a temporary directory is created");
        copy_fixture_run(PRODUCER_AND_CONSUMER_DIR, results_dir.path());
        copy_fixture_run(TWO_PRODUCERS_DIR, results_dir.path());
        let state = web::Data::new(app_state_with_results(results_dir.path()));
        let app = test::init_service(
            App::new()
                .app_data(state.clone())
                .service(list_benchmark_summaries)
                .service(get_benchmark_report_light),
        )
        .await;
        std::fs::remove_dir_all(results_dir.path().join(PRODUCER_AND_CONSUMER_DIR))
            .expect("the run directory is removed");

        state
            .cache
            .load()
            .expect("the results directory is readable");

        let summaries: Vec<BenchmarkSummary> = test::call_and_read_body_json(
            &app,
            test::TestRequest::get().uri("/api/benchmarks").to_request(),
        )
        .await;
        let runs: Vec<_> = summaries
            .iter()
            .map(|summary| summary.uuid.to_string())
            .collect();
        assert_eq!(runs, [TWO_PRODUCERS_RUN]);
        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&light_uri(PRODUCER_AND_CONSUMER_RUN))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[actix_web::test]
    async fn given_report_broken_after_load_when_requesting_series_should_answer_internal_error() {
        let results_dir = tempfile::tempdir().expect("a temporary directory is created");
        copy_fixture_run(PRODUCER_AND_CONSUMER_DIR, results_dir.path());
        let report = results_dir
            .path()
            .join(PRODUCER_AND_CONSUMER_DIR)
            .join(REPORT_FILE);
        let state = app_state_with_results(results_dir.path());
        std::fs::write(&report, "{").expect("the report is overwritten");
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(state))
                .service(get_benchmark_series),
        )
        .await;

        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&series_uri(PRODUCER_AND_CONSUMER_RUN, LATENCY_OF_GROUPS))
                .to_request(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body: serde_json::Value = test::read_body_json(response).await;
        assert!(
            body["error"]
                .as_str()
                .is_some_and(|error| error.contains("Failed to parse report file")),
            "{body}"
        );
    }

    pub(super) const CACHED_PNG: &[u8] = b"cached png";
    pub(super) const PNG_CONTENT_TYPE: &str = "image/png";
    pub(super) const HTML_CONTENT_TYPE: &str = "text/html";

    /// `state` with `cache_dir` and an embed cache in it, which takes the files that are there.
    pub(super) fn app_state_with_cache_dir(state: AppState, cache_dir: &Path) -> AppState {
        let embed_cache = EmbedCache::open(
            cache_dir.join(EMBED_CACHE_DIR),
            DEFAULT_MAX_GIB * BYTES_PER_GIB,
            TEST_BUILD,
        )
        .expect("the embed cache opens");
        AppState {
            cache_dir: cache_dir.to_path_buf(),
            embed_cache: Arc::new(embed_cache),
            ..state
        }
    }

    /// The path in the embed directory of [`app_state_with_cache_dir`] of the cached file `name`.
    pub(super) fn cached_path(embed_dir: &Path, name: &str) -> PathBuf {
        embed_dir.join(stamped_name(TEST_BUILD, name))
    }

    /// Starts the names of the cached files of the fixture run `run` in `run_dir`.
    pub(super) fn fixture_file_prefix(run_dir: &str, run: &str) -> String {
        let report = Path::new(FIXTURE_RESULTS_DIR)
            .join(run_dir)
            .join(REPORT_FILE);
        let stamp = FileStamp::of_file(&report).expect("the fixture report is readable");
        format!("{run}_{stamp}")
    }

    /// A render pool whose render thread fails, for a test in which every PNG comes from the cache.
    pub(super) fn render_pool_without_renderer() -> PngRenderPool {
        PngRenderPool::with_renderer(|_, _, _, _| Err("no render expected".to_owned()))
            .expect("the render threads start")
    }

    pub(super) fn header<'a, B>(response: &'a ServiceResponse<B>, name: &str) -> Option<&'a str> {
        response
            .headers()
            .get(name)
            .and_then(|value| value.to_str().ok())
    }

    fn embed_uri(uuid: &str, query: &str) -> String {
        format!("/embed/{uuid}?{query}")
    }

    fn png_uri(uuid: &str, query: &str) -> String {
        format!("/embed/{uuid}/chart.png?{query}")
    }

    pub(super) fn png_query(query: &str) -> PngQuery {
        web::Query::<PngQuery>::from_query(query)
            .expect("the PNG query parses")
            .into_inner()
    }

    /// The name of the cached PNG of `run` for the valid PNG query `query`.
    pub(super) fn png_name(run: impl fmt::Display, query: &str) -> String {
        let query = png_query(query);
        let variant = query.variant().expect("the PNG query is valid");
        png_file_name(run, &query.chart_type, variant)
    }

    #[actix_web::test]
    async fn given_fixture_run_when_requesting_embed_page_should_return_chart_with_hardware_line() {
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_state()))
                .service(embed_chart),
        )
        .await;

        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&embed_uri(
                    PRODUCER_AND_CONSUMER_RUN,
                    "type=throughput&theme=light",
                ))
                .to_request(),
        )
        .await;

        assert!(response.status().is_success(), "{}", response.status());
        assert_eq!(
            header(&response, CONTENT_TYPE.as_str()),
            Some(HTML_CONTENT_TYPE)
        );
        let body = test::read_body(response).await;
        let page = std::str::from_utf8(&body).expect("the page is UTF-8");
        assert!(page.contains("var option = {"), "{page}");
        assert!(
            page.contains(
                r#"var hardwareLine = "atlas @ AMD Ryzen 9 5950X 16-Core Processor (server server-0.4.202)";"#
            ),
            "{page}"
        );
    }

    /// The producers of the cluster run as the dashboard pages show them.
    fn cluster_run_producers(state: &AppState) -> GroupMetrics {
        let uuid = Uuid::parse_str(CLUSTER_RUN).expect("the fixture uuid parses");
        state
            .cache
            .get_summaries()
            .iter()
            .find(|summary| summary.uuid == uuid)
            .and_then(|summary| summary.group(GroupMetricsKind::Producers).cloned())
            .expect("the cluster fixture run has producers")
    }

    fn total_text(group: &GroupMetrics) -> String {
        format!(
            "Total: {:.2} MB/s, {:.0} msg/s",
            group.throughput_megabytes_per_second, group.throughput_messages_per_second
        )
    }

    #[actix_web::test]
    async fn given_actors_finishing_apart_when_requesting_embed_page_should_print_dashboard_rate() {
        let state = app_state();
        let total = total_text(&cluster_run_producers(&state));
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(state))
                .service(embed_chart),
        )
        .await;

        for chart_type in EVERY_CHART_TYPE {
            for action in ["", "&action=download"] {
                let uri = embed_uri(CLUSTER_RUN, &format!("type={chart_type}{action}"));
                let response =
                    test::call_service(&app, test::TestRequest::get().uri(&uri).to_request()).await;

                assert!(
                    response.status().is_success(),
                    "{uri}: {}",
                    response.status()
                );
                let body = test::read_body(response).await;
                let page = std::str::from_utf8(&body).expect("the page is UTF-8");
                assert!(page.contains(&total), "{uri}: {page}");
                assert!(!page.contains(CLUSTER_RUN_SUMMED_RATE), "{uri}: {page}");
            }
        }
    }

    #[actix_web::test]
    async fn given_actors_finishing_apart_when_requesting_png_should_render_dashboard_rate() {
        let rendered = Arc::new(Mutex::new(Vec::new()));
        let charts = Arc::clone(&rendered);
        let render_pool = PngRenderPool::with_renderer(move |chart, _, _, _| {
            let option = serde_json::to_value(chart).map_err(|e| e.to_string())?;
            charts
                .lock()
                .map_err(|e| e.to_string())?
                .push(option["title"][0]["subtext"].clone());
            Ok(b"png".to_vec())
        })
        .expect("the render threads start");
        let cache_dir = tempfile::tempdir().expect("a temporary directory is created");
        let state = AppState {
            render_pool: Arc::new(render_pool),
            ..app_state_with_cache_dir(app_state(), cache_dir.path())
        };
        let total = total_text(&cluster_run_producers(&state));
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(state))
                .service(embed_chart_png),
        )
        .await;

        for chart_type in EVERY_CHART_TYPE {
            let uri = png_uri(CLUSTER_RUN, &format!("type={chart_type}"));
            let response =
                test::call_service(&app, test::TestRequest::get().uri(&uri).to_request()).await;
            assert!(
                response.status().is_success(),
                "{uri}: {}",
                response.status()
            );
        }

        let subtexts = rendered.lock().expect("no render panicked").clone();
        assert_eq!(subtexts.len(), EVERY_CHART_TYPE.len());
        for subtext in subtexts {
            let subtext = subtext.as_str().expect("every chart has a subtext");
            assert!(subtext.contains(&total), "{subtext}");
            assert!(!subtext.contains(CLUSTER_RUN_SUMMED_RATE), "{subtext}");
        }
    }

    #[actix_web::test]
    async fn given_unknown_theme_or_action_when_requesting_embed_page_should_draw_dark_chart_page()
    {
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_state()))
                .service(embed_chart),
        )
        .await;

        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&embed_uri(
                    PRODUCER_AND_CONSUMER_RUN,
                    "theme=dark_nobg&action=view",
                ))
                .to_request(),
        )
        .await;

        assert!(response.status().is_success(), "{}", response.status());
        let body = test::read_body(response).await;
        let page = std::str::from_utf8(&body).expect("the page is UTF-8");
        assert!(page.contains("background: #070C18"), "{page}");
        assert!(!page.contains("a.download"), "{page}");
    }

    #[actix_web::test]
    async fn given_unknown_chart_type_when_requesting_embed_should_answer_bad_request() {
        let cache_dir = tempfile::tempdir().expect("a temporary directory is created");
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_state_with_cache_dir(
                    app_state(),
                    cache_dir.path(),
                )))
                .service(embed_chart_png)
                .service(embed_chart),
        )
        .await;

        for uri in [
            embed_uri(PRODUCER_AND_CONSUMER_RUN, "type=p99"),
            png_uri(PRODUCER_AND_CONSUMER_RUN, "type=p99"),
        ] {
            let response =
                test::call_service(&app, test::TestRequest::get().uri(&uri).to_request()).await;

            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
            let body: serde_json::Value = test::read_body_json(response).await;
            assert!(
                body["error"]
                    .as_str()
                    .is_some_and(|error| error.contains("Invalid chart type")),
                "{uri}: {body}"
            );
        }
    }

    #[actix_web::test]
    async fn given_size_outside_presets_when_requesting_png_should_answer_bad_request_without_caching()
     {
        let cache_dir = tempfile::tempdir().expect("a temporary directory is created");
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_state_with_cache_dir(
                    app_state(),
                    cache_dir.path(),
                )))
                .service(embed_chart_png),
        )
        .await;

        for query in [
            "width=0",
            "height=0",
            "width=800&height=600",
            "width=1920",
            "width=4096&height=4096",
            "width=4294967295&height=4294967295",
        ] {
            let response = test::call_service(
                &app,
                test::TestRequest::get()
                    .uri(&png_uri(PRODUCER_AND_CONSUMER_RUN, query))
                    .to_request(),
            )
            .await;

            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{query}");
            let body: serde_json::Value = test::read_body_json(response).await;
            assert!(
                body["error"]
                    .as_str()
                    .is_some_and(|error| error.contains("Invalid size")),
                "{query}: {body}"
            );
        }
        assert!(!cache_dir.path().join(EMBED_CACHE_DIR).exists());
    }

    #[actix_web::test]
    async fn given_each_png_variant_when_checking_its_query_should_accept_it_and_name_its_own_file()
    {
        let mut names = HashSet::new();
        for theme in PngTheme::ALL {
            for size in PngSize::ALL {
                for legend in [true, false] {
                    let variant = PngVariant {
                        theme,
                        size,
                        legend,
                    };
                    let query = format!(
                        "theme={theme}&width={}&height={}&legend={legend}",
                        size.width(),
                        size.height()
                    );

                    assert_eq!(png_query(&query).variant().ok(), Some(variant), "{query}");
                    names.insert(png_file_name(
                        PRODUCER_AND_CONSUMER_RUN,
                        ChartType::Latency,
                        variant,
                    ));
                }
            }
        }

        assert_eq!(names.len(), PngTheme::ALL.len() * PngSize::ALL.len() * 2);
        assert_eq!(
            png_query("").variant().ok().map(|variant| variant.size),
            Some(PngSize::DEFAULT)
        );
    }

    #[actix_web::test]
    async fn given_cached_png_of_each_size_when_requesting_png_should_serve_it_without_render() {
        let cache_dir = tempfile::tempdir().expect("a temporary directory is created");
        let embed_dir = cache_dir.path().join(EMBED_CACHE_DIR);
        std::fs::create_dir(&embed_dir).expect("the embed directory is created");
        let queries = PngSize::ALL.map(|size| {
            format!(
                "type=latency&width={}&height={}",
                size.width(),
                size.height()
            )
        });
        let file_prefix = fixture_file_prefix(PRODUCER_AND_CONSUMER_DIR, PRODUCER_AND_CONSUMER_RUN);
        // The same report under a run id that the server doesn't list.
        let gone_run_prefix =
            file_prefix.replace(PRODUCER_AND_CONSUMER_RUN, &Uuid::nil().to_string());
        for query in &queries {
            for prefix in [&file_prefix, &gone_run_prefix] {
                std::fs::write(
                    cached_path(&embed_dir, &png_name(prefix, query)),
                    CACHED_PNG,
                )
                .expect("the cached PNG is written");
            }
        }
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(AppState {
                    render_pool: Arc::new(render_pool_without_renderer()),
                    ..app_state_with_cache_dir(app_state(), cache_dir.path())
                }))
                .service(embed_chart_png),
        )
        .await;

        for query in &queries {
            let response = test::call_service(
                &app,
                test::TestRequest::get()
                    .uri(&png_uri(PRODUCER_AND_CONSUMER_RUN, query))
                    .to_request(),
            )
            .await;
            assert!(
                response.status().is_success(),
                "{query}: {}",
                response.status()
            );
            assert_eq!(
                header(&response, CONTENT_TYPE.as_str()),
                Some(PNG_CONTENT_TYPE)
            );
            assert_eq!(test::read_body(response).await, CACHED_PNG, "{query}");

            // The cache keeps the PNG of a run that is gone, and the lookup answers first.
            let response = test::call_service(
                &app,
                test::TestRequest::get()
                    .uri(&png_uri(&Uuid::nil().to_string(), query))
                    .to_request(),
            )
            .await;
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{query}");
        }
    }

    #[actix_web::test]
    async fn given_embed_page_built_when_report_breaks_should_serve_cached_page() {
        let results_dir = tempfile::tempdir().expect("a temporary directory is created");
        let cache_dir = tempfile::tempdir().expect("a temporary directory is created");
        copy_fixture_run(PRODUCER_AND_CONSUMER_DIR, results_dir.path());
        let state =
            app_state_with_cache_dir(app_state_with_results(results_dir.path()), cache_dir.path());
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(state))
                .service(embed_chart),
        )
        .await;
        let uri = embed_uri(PRODUCER_AND_CONSUMER_RUN, "type=throughput&theme=light");
        let first =
            test::call_and_read_body(&app, test::TestRequest::get().uri(&uri).to_request()).await;
        std::fs::write(
            results_dir
                .path()
                .join(PRODUCER_AND_CONSUMER_DIR)
                .join(REPORT_FILE),
            "{",
        )
        .expect("the report is overwritten");

        let response =
            test::call_service(&app, test::TestRequest::get().uri(&uri).to_request()).await;

        assert!(response.status().is_success(), "{}", response.status());
        assert_eq!(test::read_body(response).await, first);
    }

    #[actix_web::test]
    async fn given_report_rewritten_when_runs_reload_should_build_embed_page_and_png_again() {
        const OLD_HARDWARE_LINE: &str =
            "atlas @ AMD Ryzen 9 5950X 16-Core Processor (server server-0.4.202)";
        const NEW_HARDWARE_LINE: &str =
            "rewritten-host @ AMD Ryzen 9 5950X 16-Core Processor (server server-0.4.202)";
        let results_dir = tempfile::tempdir().expect("a temporary directory is created");
        let cache_dir = tempfile::tempdir().expect("a temporary directory is created");
        copy_fixture_run(PRODUCER_AND_CONSUMER_DIR, results_dir.path());
        let renders = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&renders);
        let render_pool = PngRenderPool::with_renderer(move |_, _, _, _| {
            counted.fetch_add(1, Ordering::SeqCst);
            Ok(b"rendered png".to_vec())
        })
        .expect("the render threads start");
        let state = AppState {
            render_pool: Arc::new(render_pool),
            ..app_state_with_cache_dir(app_state_with_results(results_dir.path()), cache_dir.path())
        };
        let runs = Arc::clone(&state.cache);
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(state))
                .service(embed_chart_png)
                .service(embed_chart),
        )
        .await;
        let page_uri = embed_uri(PRODUCER_AND_CONSUMER_RUN, "type=throughput&theme=light");
        let png_uri = png_uri(PRODUCER_AND_CONSUMER_RUN, "type=throughput");
        let get = |uri: &str| {
            test::call_and_read_body(&app, test::TestRequest::get().uri(uri).to_request())
        };
        let first_page = get(&page_uri).await;
        get(&png_uri).await;
        let report_path = results_dir
            .path()
            .join(PRODUCER_AND_CONSUMER_DIR)
            .join(REPORT_FILE);
        let mut report: serde_json::Value =
            serde_json::from_slice(&std::fs::read(&report_path).expect("the report is readable"))
                .expect("the report is JSON");
        report["hardware"]["identifier"] = serde_json::Value::from("rewritten-host");
        std::fs::write(&report_path, report.to_string()).expect("the report is rewritten");
        // Until the reload, the light report and the cached files stay as they were.
        get(&png_uri).await;
        assert_eq!(renders.load(Ordering::SeqCst), 1);

        runs.load().expect("the results directory is readable");
        let second_page = get(&page_uri).await;
        get(&png_uri).await;

        let first_page = std::str::from_utf8(&first_page).expect("the page is UTF-8");
        let second_page = std::str::from_utf8(&second_page).expect("the page is UTF-8");
        assert!(first_page.contains(OLD_HARDWARE_LINE), "{first_page}");
        assert!(second_page.contains(NEW_HARDWARE_LINE), "{second_page}");
        assert_eq!(renders.load(Ordering::SeqCst), 2);
    }

    #[actix_web::test]
    async fn given_page_themes_when_requesting_embed_page_should_cache_one_page_per_look() {
        let cache_dir = tempfile::tempdir().expect("a temporary directory is created");
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_state_with_cache_dir(
                    app_state(),
                    cache_dir.path(),
                )))
                .service(embed_chart),
        )
        .await;

        for query in [
            "theme=dark",
            "theme=dark_nobg",
            "theme=anything&action=view",
            "theme=light",
            "theme=light&action=download",
        ] {
            let uri = embed_uri(PRODUCER_AND_CONSUMER_RUN, query);
            let response =
                test::call_service(&app, test::TestRequest::get().uri(&uri).to_request()).await;
            assert!(
                response.status().is_success(),
                "{uri}: {}",
                response.status()
            );
            assert_eq!(
                header(&response, CACHE_CONTROL.as_str()),
                Some("public, max-age=3600"),
                "{uri}"
            );
        }

        let cached = names_in(&cache_dir.path().join(EMBED_CACHE_DIR));
        let file_prefix = fixture_file_prefix(PRODUCER_AND_CONSUMER_DIR, PRODUCER_AND_CONSUMER_RUN);
        assert_eq!(
            cached,
            ["dark", "light", "light_download"].map(|look| stamped_name(
                TEST_BUILD,
                &format!("{file_prefix}_latency_{look}.html")
            ))
        );
    }

    #[actix_web::test]
    async fn given_unknown_run_when_requesting_embed_should_answer_not_found() {
        let cache_dir = tempfile::tempdir().expect("a temporary directory is created");
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(app_state_with_cache_dir(
                    app_state(),
                    cache_dir.path(),
                )))
                .service(embed_chart_png)
                .service(embed_chart),
        )
        .await;
        let uuid = Uuid::nil().to_string();

        for uri in [embed_uri(&uuid, ""), png_uri(&uuid, "")] {
            let response =
                test::call_service(&app, test::TestRequest::get().uri(&uri).to_request()).await;

            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{uri}");
        }
    }

    #[actix_web::test]
    async fn given_report_broken_after_load_when_requesting_embed_should_answer_internal_error() {
        let results_dir = tempfile::tempdir().expect("a temporary directory is created");
        let cache_dir = tempfile::tempdir().expect("a temporary directory is created");
        copy_fixture_run(PRODUCER_AND_CONSUMER_DIR, results_dir.path());
        let state =
            app_state_with_cache_dir(app_state_with_results(results_dir.path()), cache_dir.path());
        std::fs::write(
            results_dir
                .path()
                .join(PRODUCER_AND_CONSUMER_DIR)
                .join(REPORT_FILE),
            "{",
        )
        .expect("the report is overwritten");
        let app = test::init_service(
            App::new()
                .app_data(web::Data::new(state))
                .service(embed_chart_png)
                .service(embed_chart),
        )
        .await;

        for uri in [
            embed_uri(PRODUCER_AND_CONSUMER_RUN, ""),
            png_uri(PRODUCER_AND_CONSUMER_RUN, ""),
        ] {
            let response =
                test::call_service(&app, test::TestRequest::get().uri(&uri).to_request()).await;

            assert_eq!(
                response.status(),
                StatusCode::INTERNAL_SERVER_ERROR,
                "{uri}"
            );
            let body: serde_json::Value = test::read_body_json(response).await;
            assert!(
                body["error"]
                    .as_str()
                    .is_some_and(|error| error.contains("Failed to parse report file")),
                "{uri}: {body}"
            );
        }
        assert!(!cache_dir.path().join(EMBED_CACHE_DIR).exists());
    }

    #[actix_web::test]
    async fn given_raw_mark_in_title_or_hardware_line_when_preparing_png_should_keep_them_plain_text()
     {
        let embed = EmbedChart {
            chart: Chart::new().title(
                Title::new()
                    .text(format!("{RAW_MARK}a\"+alert(1){RAW_MARK}"))
                    .subtext("Sent 1 Messages"),
            ),
            hardware_line: format!("\"{RAW_MARK}b\"+alert(2){RAW_MARK}"),
        };
        let theme: PngTheme = "dark".parse().expect("dark is a PNG theme");
        let size = PngSize::DEFAULT;

        let chart = png_chart(embed, size.width(), size.height(), true, theme)
            .expect("the PNG chart is prepared");

        let option: serde_json::Value = serde_json::from_str(&chart.to_string())
            .expect("the renderer gets the chart as JSON without script");
        let title = &option["title"][0];
        assert_eq!(title["text"], "a\"+alert(1)");
        assert_eq!(title["subtext"], "\"b\"+alert(2)\nSent 1 Messages");
    }

    fn embed_page_html(dark: bool, download_name: Option<String>) -> String {
        EmbedPage {
            option: "{}",
            hardware_line: "",
            dark,
            title: IGGY_BENCH_PAGE_TITLE,
            download_name,
        }
        .html()
    }

    #[actix_web::test]
    async fn given_dark_embed_page_when_writing_html_should_init_echarts_with_dark_theme() {
        for download_name in [None, Some("chart.png".to_owned())] {
            let html = embed_page_html(true, download_name);

            assert!(
                html.contains("echarts.init(document.getElementById('chart'), 'dark');"),
                "{html}"
            );
        }
    }

    #[actix_web::test]
    async fn given_light_embed_page_when_writing_html_should_init_echarts_without_theme() {
        for download_name in [None, Some("chart.png".to_owned())] {
            let html = embed_page_html(false, download_name);

            assert!(
                html.contains("echarts.init(document.getElementById('chart'), null);"),
                "{html}"
            );
            assert!(!html.contains("'dark'"), "{html}");
        }
    }

    /// charming writes a chart's one legend as an object, which has no `length`.
    #[actix_web::test]
    async fn given_single_legend_when_writing_embed_page_should_scale_its_text() {
        let html = embed_page_html(true, None);

        assert!(
            html.contains(
                "var legend = Array.isArray(option.legend) ? option.legend[0] : option.legend;"
            ),
            "{html}"
        );
        assert!(!html.contains("option.legend.length"), "{html}");
    }

    /// The latency chart of the 2 producer run, as `/embed/{uuid}/chart.png` loads it.
    fn two_producers_latency_chart() -> EmbedChart {
        let path = Path::new(FIXTURE_RESULTS_DIR)
            .join(TWO_PRODUCERS_DIR)
            .join(REPORT_FILE);
        let report = read_report(&path).expect("the fixture report is readable");
        EmbedChart {
            chart: bench_report::create_latency_chart(&report, true, false),
            hardware_line: String::new(),
        }
    }

    fn png_legend_text_size(width: u32, height: u32) -> f64 {
        let theme: PngTheme = "dark".parse().expect("dark is a PNG theme");
        let chart = png_chart(two_producers_latency_chart(), width, height, true, theme)
            .expect("the PNG chart is prepared");
        let option = serde_json::to_value(&chart).expect("the chart serializes");
        option["legend"]["textStyle"]["fontSize"]
            .as_f64()
            .expect("the legend has a text size")
    }

    /// 3 series, so the text size is the cap of 10 px times the scale of the image.
    #[actix_web::test]
    async fn given_single_legend_when_preparing_png_should_scale_its_text() {
        assert_eq!(png_legend_text_size(800, 600), 7.0);
        assert_eq!(png_legend_text_size(1600, 1200), 13.0);
    }

    /// At 300 x 200 the cap of 2.5 px is below the floor of 6 px.
    #[actix_web::test]
    async fn given_small_png_size_when_preparing_png_should_keep_legend_text_at_floor() {
        assert_eq!(png_legend_text_size(300, 200), 6.0);
    }

    #[actix_web::test]
    async fn given_comment_open_in_chart_text_when_writing_embed_page_should_escape_it() {
        let option = r#"{"title":[{"text":"<!--<script>"}]}"#;

        for download_name in [None, Some("<!--<script>.png".to_owned())] {
            let html = EmbedPage {
                option,
                hardware_line: "<!--<script>",
                dark: true,
                title: IGGY_BENCH_PAGE_TITLE,
                download_name,
            }
            .html();

            assert!(!html.contains("<!--"), "{html}");
            assert!(html.contains(r#""text":"<\!--<script>""#), "{html}");
            assert!(
                html.contains(r#"var hardwareLine = "<\!--<script>";"#),
                "{html}"
            );
        }
    }
}
