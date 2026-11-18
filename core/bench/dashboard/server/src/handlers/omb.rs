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

//! The endpoints of the OpenMessaging Benchmark (OMB) runs, and the embeds of their charts. They
//! take OMB run ids only, so an iggy-bench run id gives 400 here.

use super::{
    AppState, EMBED_CACHE_DIR, EmbedPage, EmbedQuery, PngQuery, PngTheme, Result, get_client_addr,
    png_cache_path, png_response, read_cached_png, render_png,
};
use crate::error::IggyBenchDashboardServerError;
use crate::omb::embed::{OmbChartType, setup_line_hash};
use crate::omb::{OmbCache, OmbFileError, OmbRun, OmbRunFiles, read_file};
use crate::render::EmbedChart;
use actix_web::error::BlockingError;
use actix_web::http::header::{ContentDisposition, ContentType};
use actix_web::{HttpRequest, HttpResponse, get, web};
use bench_dashboard_shared::omb::detail::OmbRunDetail;
use bench_dashboard_shared::omb::id::OmbRunId;
use bench_dashboard_shared::omb::result::OmbResult;
use bench_dashboard_shared::omb::setup::OmbSidecar;
use bench_dashboard_shared::omb::summary::{OmbRunList, OmbRunSummary};
use std::io;
use std::path::Path;
use std::str;
use std::str::FromStr;
use std::sync::Arc;
use tracing::{info, warn};

/// The media type of TOML files.
const TOML_CONTENT_TYPE: &str = "application/toml";
/// The page title of the OMB embeds, which tells them apart from the iggy-bench embeds.
const EMBED_PAGE_TITLE: &str = "Iggy OMB Benchmark Chart";
/// Starts the names of the OMB PNG files, in the cache and in a download, so they never share a
/// name with the files of an iggy-bench run.
const PNG_FILE_PREFIX: &str = "omb";

/// The OMB part of the server state.
pub struct OmbState {
    /// `None` without `--omb-results-dir`.
    pub cache: Option<Arc<OmbCache>>,
}

#[get("/api/omb/runs")]
pub async fn list_omb_runs(data: web::Data<OmbState>, req: HttpRequest) -> Result<HttpResponse> {
    let client_addr = get_client_addr(&req);
    let Some(cache) = &data.cache else {
        info!("{client_addr}: Listing OMB runs, but the server has no OMB results directory");
        return Ok(HttpResponse::Ok().json(OmbRunList::default()));
    };
    let list = cache.run_list();
    info!("{}: Listing {} OMB runs", client_addr, list.runs.len());
    Ok(HttpResponse::Ok().json(list.as_ref()))
}

/// Reads the result file again for the series and quantile curves, which the list leaves out.
#[get("/api/omb/runs/{id}")]
pub async fn get_omb_run(
    data: web::Data<OmbState>,
    id: web::Path<String>,
    req: HttpRequest,
) -> Result<HttpResponse> {
    let client_addr = get_client_addr(&req);
    info!("{client_addr}: Requesting OMB run '{id}'");

    let run = data.run(run_id(&id)?)?;
    let detail = web::block(move || read_detail(run))
        .await
        .map_err(blocking_error)??;
    Ok(HttpResponse::Ok().json(detail))
}

#[get("/api/omb/runs/{id}/result.json")]
pub async fn download_omb_result(
    data: web::Data<OmbState>,
    id: web::Path<String>,
    req: HttpRequest,
) -> Result<HttpResponse> {
    let client_addr = get_client_addr(&req);
    info!("{client_addr}: Downloading the result file of OMB run '{id}'");

    let id = run_id(&id)?;
    let files = data.files(id)?;
    let file_name = files.result_file_name();
    let bytes = web::block(move || read_result(id, &files.result))
        .await
        .map_err(blocking_error)??;
    Ok(HttpResponse::Ok()
        .content_type(ContentType::json())
        .insert_header(ContentDisposition::attachment(file_name))
        .body(bytes))
}

/// Sends the sidecar as it is on the disk. The loader keeps a sidecar out of the run files if
/// it has keys that look like secrets, or if it doesn't parse, and the file is checked again
/// before it is sent.
#[get("/api/omb/runs/{id}/setup.toml")]
pub async fn download_omb_setup(
    data: web::Data<OmbState>,
    id: web::Path<String>,
    req: HttpRequest,
) -> Result<HttpResponse> {
    let client_addr = get_client_addr(&req);
    info!("{client_addr}: Downloading the setup file of OMB run '{id}'");

    let id = run_id(&id)?;
    let files = data.files(id)?;
    let file_name = files.setup_file_name();
    let path = files.setup.ok_or_else(|| no_setup(id))?;
    let bytes = web::block(move || read_setup(id, &path))
        .await
        .map_err(blocking_error)??;
    Ok(HttpResponse::Ok()
        .content_type(TOML_CONTENT_TYPE)
        .insert_header(ContentDisposition::attachment(file_name))
        .body(bytes))
}

/// The interactive chart of an OMB run, for an iframe. It takes the query of the iggy-bench embed
/// page, with the chart types of [`OmbChartType`].
#[get("/embed/omb/{id}")]
pub async fn embed_omb_chart(
    data: web::Data<OmbState>,
    id: web::Path<String>,
    query: web::Query<EmbedQuery>,
    req: HttpRequest,
) -> Result<HttpResponse> {
    let client_addr = get_client_addr(&req);
    info!(
        "{client_addr}: Embed chart request for OMB run '{id}' (type={}, theme={})",
        query.chart_type, query.theme
    );

    let id = run_id(&id)?;
    let chart_type: OmbChartType = query.chart_type.parse()?;
    let dark = query.theme.parse::<PageTheme>()? == PageTheme::Dark;
    let action = query
        .action
        .as_deref()
        .map(PageAction::from_str)
        .transpose()?;
    let EmbedChart {
        chart,
        hardware_line,
    } = load_embed_chart(data.run(id)?, chart_type, dark).await?;

    // The quantile axis formats its labels with functions, which only `to_string` writes as
    // JavaScript. serde_json would write them as strings. `to_string` also writes any text between
    // charming's raw marks as JavaScript, so the chart's free text goes through `without_raw_mark`.
    let option = chart.to_string();
    Ok(EmbedPage {
        option: &option,
        hardware_line: &hardware_line,
        dark,
        title: EMBED_PAGE_TITLE,
        download_name: action
            .map(|PageAction::Download| format!("{PNG_FILE_PREFIX}_{id}_{chart_type}.png")),
    }
    .response())
}

/// The chart of an OMB run as a PNG image. It takes the query of the iggy-bench PNG, with the chart
/// types of [`OmbChartType`], and caches the default size the same way.
#[get("/embed/omb/{id}/chart.png")]
pub async fn embed_omb_chart_png(
    app: web::Data<AppState>,
    data: web::Data<OmbState>,
    id: web::Path<String>,
    query: web::Query<PngQuery>,
    req: HttpRequest,
) -> Result<HttpResponse> {
    let client_addr = get_client_addr(&req);
    info!(
        "{client_addr}: Embed PNG request for OMB run '{id}' (type={}, theme={}, {}x{})",
        query.chart_type, query.theme, query.width, query.height
    );

    let id = run_id(&id)?;
    let chart_type: OmbChartType = query.chart_type.parse()?;
    let theme: PngTheme = query.theme.parse()?;
    query.check_size()?;
    // A cached PNG stays on the disk after its run is gone, so the lookup comes first.
    let run = data.run(id)?;

    let embed_dir = app.cache_dir.join(EMBED_CACHE_DIR);
    let cached_path = query
        .is_cached_size()
        .then(|| png_cache_path(&embed_dir, png_cache_name(&run.summary), &query));
    if let Some(png) = read_cached_png(cached_path.as_deref()).await {
        info!("{client_addr}: Serving cached PNG for OMB run '{id}'");
        return Ok(png_response(png));
    }

    let load = load_embed_chart(run, chart_type, theme.dark);
    let png = render_png(&app, load, &query, theme, cached_path).await?;

    info!(
        "{client_addr}: Generated PNG for OMB run '{id}' ({}x{})",
        query.width, query.height
    );
    Ok(png_response(png))
}

impl OmbState {
    fn run(&self, id: OmbRunId) -> Result<OmbRun> {
        self.cache
            .as_ref()
            .and_then(|cache| cache.run(id))
            .ok_or_else(|| not_found(id))
    }

    fn files(&self, id: OmbRunId) -> Result<OmbRunFiles> {
        self.cache
            .as_ref()
            .and_then(|cache| cache.files(id))
            .ok_or_else(|| not_found(id))
    }
}

fn run_id(text: &str) -> Result<OmbRunId> {
    text.parse()
        .map_err(|source| IggyBenchDashboardServerError::InvalidOmbRunId {
            text: text.to_owned(),
            source,
        })
}

/// The look of the OMB embed page, named as in the `theme` query parameter. The `_nobg` themes
/// of the PNG are not page themes. Unlike the iggy-bench page, an unknown theme is a bad request,
/// so a theme added later can't change a URL that works now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PageTheme {
    Dark,
    Light,
}

impl FromStr for PageTheme {
    type Err = IggyBenchDashboardServerError;

    fn from_str(theme: &str) -> Result<Self> {
        match theme {
            "dark" => Ok(Self::Dark),
            "light" => Ok(Self::Light),
            _ => Err(IggyBenchDashboardServerError::BadRequest(format!(
                "Invalid theme: '{theme}'. Must be 'dark' or 'light'"
            ))),
        }
    }
}

/// What the OMB embed page does besides showing the chart, named as in the `action` query
/// parameter. An unknown action is a bad request, as an unknown theme is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PageAction {
    /// Saves the chart as a PNG file in the browser.
    Download,
}

impl FromStr for PageAction {
    type Err = IggyBenchDashboardServerError;

    fn from_str(action: &str) -> Result<Self> {
        match action {
            "download" => Ok(Self::Download),
            _ => Err(IggyBenchDashboardServerError::BadRequest(format!(
                "Invalid action: '{action}'. Must be 'download'"
            ))),
        }
    }
}

/// Names the cached PNG files of a run. The sidecar can change under the same run id, so the hash
/// of the setup line that it gives is in the name too.
fn png_cache_name(summary: &OmbRunSummary) -> String {
    format!(
        "{PNG_FILE_PREFIX}_{}_{}",
        summary.id,
        setup_line_hash(summary)
    )
}

/// Reads the result file again, as [`get_omb_run`] does, and draws the chart from it.
async fn load_embed_chart(run: OmbRun, chart_type: OmbChartType, dark: bool) -> Result<EmbedChart> {
    web::block(move || {
        let id = run.summary.id;
        let detail = read_detail(run)?;
        EmbedChart::of_omb_run(&detail, chart_type, dark).ok_or_else(|| no_chart(id, chart_type))
    })
    .await
    .map_err(blocking_error)?
}

fn read_detail(run: OmbRun) -> Result<OmbRunDetail> {
    let id = run.summary.id;
    let bytes = read_result(id, &run.files.result)?;
    let result = OmbResult::try_from(bytes.as_slice()).map_err(|error| {
        IggyBenchDashboardServerError::InternalError(format!("Can't read OMB run '{id}': {error}"))
    })?;
    Ok(OmbRunDetail::new(run.summary, &result))
}

/// A result file that changed after the last load has another id, so the run `id` is gone until
/// the reload lists the new one.
fn read_result(id: OmbRunId, path: &Path) -> Result<Vec<u8>> {
    let bytes = read_file(path).map_err(|error| unreadable(id, error))?;
    if OmbRunId::of_file(&bytes) != id {
        warn!(
            "The result file '{}' of OMB run '{id}' changed after the last load",
            path.display()
        );
        return Err(not_found(id));
    }
    Ok(bytes)
}

/// A sidecar can change before the next load sees it, so a file that no longer parses, or that
/// now has keys that look like secrets, is not sent.
fn read_setup(id: OmbRunId, path: &Path) -> Result<Vec<u8>> {
    let bytes = read_file(path).map_err(|error| unreadable(id, error))?;
    let sendable = str::from_utf8(&bytes)
        .ok()
        .and_then(|text| text.parse::<OmbSidecar>().ok())
        .is_some_and(|sidecar| !sidecar.setup.file_holds_secrets);
    if !sendable {
        warn!(
            "The setup file '{}' of OMB run '{id}' changed after the last load, so it isn't sent",
            path.display()
        );
        return Err(no_setup(id));
    }
    Ok(bytes)
}

/// The answer names the run only. The log names the path.
fn unreadable(id: OmbRunId, error: OmbFileError) -> IggyBenchDashboardServerError {
    warn!("Can't send a file of OMB run '{id}': {error}");
    match error {
        OmbFileError::Read { source, .. } if source.kind() == io::ErrorKind::NotFound => {
            not_found(id)
        }
        OmbFileError::Read { .. }
        | OmbFileError::TooLarge { .. }
        | OmbFileError::NotRegularFile { .. } => IggyBenchDashboardServerError::InternalError(
            format!("Can't read a file of OMB run '{id}'"),
        ),
    }
}

fn not_found(id: OmbRunId) -> IggyBenchDashboardServerError {
    IggyBenchDashboardServerError::NotFound(format!("OMB run '{id}' not found"))
}

fn no_setup(id: OmbRunId) -> IggyBenchDashboardServerError {
    IggyBenchDashboardServerError::NotFound(format!("OMB run '{id}' has no setup file"))
}

fn no_chart(id: OmbRunId, chart_type: OmbChartType) -> IggyBenchDashboardServerError {
    IggyBenchDashboardServerError::NotFound(format!(
        "OMB run '{id}' has no values for the '{chart_type}' chart"
    ))
}

fn blocking_error(error: BlockingError) -> IggyBenchDashboardServerError {
    IggyBenchDashboardServerError::InternalError(format!("OMB file read task failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::tests::{
        CACHED_PNG, HTML_CONTENT_TYPE, PNG_CONTENT_TYPE, app_state, app_state_with_cache_dir,
        png_query,
    };
    use crate::handlers::{embed_chart, embed_chart_png, unknown_api_path};
    use crate::omb::test_fixtures::{
        FIXTURE_OMB_RESULTS_DIR, FixtureRun, NEWEST_FIRST, NO_CONSUMERS, PUBLISH_ERRORS,
        RC1_DEFAULT, RC1_PERSISTED,
    };
    use crate::render::PngRenderPool;
    use actix_web::body::MessageBody;
    use actix_web::dev::ServiceResponse;
    use actix_web::http::StatusCode;
    use actix_web::http::header::{
        CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_SECURITY_POLICY, CONTENT_TYPE, X_FRAME_OPTIONS,
    };
    use actix_web::{App, test};
    use bench_dashboard_shared::omb::setup::OMB_SETUP_FILE_NAME;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const JSON_CONTENT_TYPE: &str = "application/json";
    /// A valid id that no fixture run has.
    const UNKNOWN_ID: &str = "0000000000000000";
    const UUID: &str = "6b1e288a-9155-4da3-8d3f-ec4d929de3d3";
    const EVERY_CHART_TYPE: [&str; 5] = [
        "latency",
        "end_to_end_latency",
        "throughput",
        "ladder",
        "quantiles",
    ];
    /// The first bytes of every PNG file.
    const PNG_SIGNATURE: &[u8] = b"\x89PNG\r\n\x1a\n";

    fn omb_state(results_dir: Option<&Path>) -> OmbState {
        OmbState {
            cache: results_dir.map(|results_dir| {
                let cache = OmbCache::new(results_dir.to_path_buf());
                cache.load().expect("the OMB results directory is readable");
                Arc::new(cache)
            }),
        }
    }

    /// The OMB services in the order of `main`, before the `/api` fallback scope.
    macro_rules! omb_app {
        ($state:expr) => {
            test::init_service(
                App::new()
                    .app_data(web::Data::new($state))
                    .service(list_omb_runs)
                    .service(get_omb_run)
                    .service(download_omb_result)
                    .service(download_omb_setup)
                    .service(web::scope("/api").default_service(web::to(unknown_api_path))),
            )
            .await
        };
    }

    /// The embed services in the order of `main`, with a cache directory of the test's own, and the
    /// fixture runs or the runs of `$omb_state`.
    macro_rules! embed_app {
        ($cache_dir:expr) => {
            embed_app!(
                $cache_dir,
                omb_state(Some(Path::new(FIXTURE_OMB_RESULTS_DIR)))
            )
        };
        ($cache_dir:expr, $omb_state:expr) => {
            test::init_service(
                App::new()
                    .app_data(web::Data::new(app_state_with_cache_dir(
                        app_state(),
                        $cache_dir,
                    )))
                    .app_data(web::Data::new($omb_state))
                    .service(embed_omb_chart_png)
                    .service(embed_omb_chart)
                    .service(embed_chart_png)
                    .service(embed_chart),
            )
            .await
        };
    }

    fn embed_uri(id: &str, query: &str) -> String {
        format!("/embed/omb/{id}?{query}")
    }

    fn png_uri(id: &str, query: &str) -> String {
        format!("/embed/omb/{id}/chart.png?{query}")
    }

    fn cached_png_path(embed_dir: &Path, summary: &OmbRunSummary, query: &str) -> PathBuf {
        png_cache_path(embed_dir, png_cache_name(summary), &png_query(query))
    }

    /// The summary of `fixture` as the server loads it, with its sidecar.
    fn fixture_summary(fixture: &FixtureRun) -> OmbRunSummary {
        omb_state(Some(Path::new(FIXTURE_OMB_RESULTS_DIR)))
            .run(fixture.id())
            .expect("the fixture run is loaded")
            .summary
    }

    /// The state of the runs in `results_dir`, with its cache, so that a test can load the runs
    /// again after it changes their files.
    fn reloadable_omb_state(results_dir: &Path) -> (OmbState, Arc<OmbCache>) {
        let state = omb_state(Some(results_dir));
        let cache = state.cache.clone().expect("the state has an OMB cache");
        (state, cache)
    }

    async fn page_text(response: ServiceResponse) -> String {
        let body = test::read_body(response).await;
        String::from_utf8(body.to_vec()).expect("the page is UTF-8")
    }

    async fn error_text(response: ServiceResponse) -> String {
        let body: serde_json::Value = test::read_body_json(response).await;
        body["error"].as_str().unwrap_or_default().to_owned()
    }

    fn run_uri(id: &str) -> String {
        format!("/api/omb/runs/{id}")
    }

    fn result_uri(id: &str) -> String {
        format!("/api/omb/runs/{id}/result.json")
    }

    fn setup_uri(id: &str) -> String {
        format!("/api/omb/runs/{id}/setup.toml")
    }

    fn header<'a>(response: &'a ServiceResponse<impl MessageBody>, name: &str) -> Option<&'a str> {
        response
            .headers()
            .get(name)
            .and_then(|value| value.to_str().ok())
    }

    fn attachment(file_name: &str) -> String {
        format!("attachment; filename=\"{file_name}\"")
    }

    fn fixture_id(fixture: &FixtureRun) -> String {
        fixture.id().to_string()
    }

    #[actix_web::test]
    async fn given_fixture_runs_when_listing_omb_runs_should_return_every_run_newest_first() {
        let app = omb_app!(omb_state(Some(Path::new(FIXTURE_OMB_RESULTS_DIR))));

        let response = test::call_service(
            &app,
            test::TestRequest::get().uri("/api/omb/runs").to_request(),
        )
        .await;

        assert!(response.status().is_success(), "{}", response.status());
        assert_eq!(
            header(&response, CONTENT_TYPE.as_str()),
            Some(JSON_CONTENT_TYPE)
        );
        let list: OmbRunList = test::read_body_json(response).await;
        assert!(list.enabled);
        let runs: Vec<_> = list
            .runs
            .iter()
            .map(|summary| (summary.id.to_string(), summary.file_name.as_str()))
            .collect();
        let expected: Vec<_> = NEWEST_FIRST
            .iter()
            .map(|fixture| (fixture_id(fixture), fixture.file_name))
            .collect();
        assert_eq!(runs, expected);
    }

    #[actix_web::test]
    async fn given_no_omb_results_dir_when_listing_omb_runs_should_answer_disabled_empty_list() {
        let app = omb_app!(omb_state(None));

        let response = test::call_service(
            &app,
            test::TestRequest::get().uri("/api/omb/runs").to_request(),
        )
        .await;

        assert!(response.status().is_success(), "{}", response.status());
        let body: serde_json::Value = test::read_body_json(response).await;
        assert_eq!(body, serde_json::json!({ "enabled": false, "runs": [] }));
    }

    #[actix_web::test]
    async fn given_fixture_run_when_requesting_it_should_return_summary_series_and_curves() {
        let app = omb_app!(omb_state(Some(Path::new(FIXTURE_OMB_RESULTS_DIR))));
        let list: OmbRunList = test::call_and_read_body_json(
            &app,
            test::TestRequest::get().uri("/api/omb/runs").to_request(),
        )
        .await;
        let listed = |fixture: &FixtureRun| -> OmbRunSummary {
            list.runs
                .iter()
                .find(|summary| summary.id == fixture.id())
                .cloned()
                .expect("the fixture run is listed")
        };

        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&run_uri(&fixture_id(&RC1_DEFAULT)))
                .to_request(),
        )
        .await;

        assert!(response.status().is_success(), "{}", response.status());
        let detail: OmbRunDetail = test::read_body_json(response).await;
        assert_eq!(detail.summary.id, RC1_DEFAULT.id());
        assert_eq!(detail.summary.file_name, listed(&RC1_DEFAULT).file_name);
        assert_eq!(detail.summary.time, listed(&RC1_DEFAULT).time);
        assert_eq!(detail.summary.setup, listed(&RC1_DEFAULT).setup);
        assert_eq!(detail.series.publish_messages_per_second.len(), 18);
        assert_eq!(detail.series.window_ends().last(), Some(&180));
        assert_eq!(detail.summary.publish_latency_ms.p99, 1.523);
        assert_eq!(
            detail
                .summary
                .end_to_end_latency_ms
                .map(|latency| latency.p99),
            Some(2.0)
        );
        assert!(!detail.quantiles.publish_latency_ms.is_empty());

        let no_consumers: OmbRunDetail = test::call_and_read_body_json(
            &app,
            test::TestRequest::get()
                .uri(&run_uri(&fixture_id(&NO_CONSUMERS)))
                .to_request(),
        )
        .await;
        assert_eq!(no_consumers.summary.end_to_end_latency_ms, None);
        assert_eq!(no_consumers.series.end_to_end_latency_ms, None);
        assert_eq!(no_consumers.quantiles.end_to_end_latency_ms, None);
    }

    #[actix_web::test]
    async fn given_fixture_runs_when_downloading_results_should_send_original_bytes_and_file_name()
    {
        let app = omb_app!(omb_state(Some(Path::new(FIXTURE_OMB_RESULTS_DIR))));

        for fixture in NEWEST_FIRST {
            let response = test::call_service(
                &app,
                test::TestRequest::get()
                    .uri(&result_uri(&fixture_id(fixture)))
                    .to_request(),
            )
            .await;

            assert!(
                response.status().is_success(),
                "{}: {}",
                fixture.file_name,
                response.status()
            );
            assert_eq!(
                header(&response, CONTENT_TYPE.as_str()),
                Some(JSON_CONTENT_TYPE)
            );
            assert_eq!(
                header(&response, CONTENT_DISPOSITION.as_str()),
                Some(attachment(fixture.file_name).as_str())
            );
            assert_eq!(test::read_body(response).await, fixture.json());
        }
    }

    #[actix_web::test]
    async fn given_run_with_sidecar_when_downloading_setup_should_send_original_file_named_after_result()
     {
        let app = omb_app!(omb_state(Some(Path::new(FIXTURE_OMB_RESULTS_DIR))));

        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&setup_uri(&fixture_id(&RC1_DEFAULT)))
                .to_request(),
        )
        .await;

        assert!(response.status().is_success(), "{}", response.status());
        assert_eq!(
            header(&response, CONTENT_TYPE.as_str()),
            Some(TOML_CONTENT_TYPE)
        );
        assert_eq!(
            header(&response, CONTENT_DISPOSITION.as_str()),
            Some(
                attachment("12-topics-1-partition-1kb-150mb-Iggy-2026-11-06-13-26-21.setup.toml")
                    .as_str()
            )
        );
        assert_eq!(
            test::read_body(response).await,
            std::fs::read(RC1_DEFAULT.sidecar_path()).expect("the fixture sidecar is readable")
        );
    }

    #[cfg(unix)]
    #[actix_web::test]
    async fn given_result_name_with_quotes_when_downloading_files_should_escape_both_names_alike() {
        let results_dir = tempfile::tempdir().expect("a temporary directory is created");
        let run_dir = results_dir.path().join("quoted");
        std::fs::create_dir(&run_dir).expect("the run directory is created");
        std::fs::copy(RC1_DEFAULT.result_path(), run_dir.join("run \"one\".json"))
            .expect("the result is copied");
        std::fs::copy(
            RC1_DEFAULT.sidecar_path(),
            run_dir.join(OMB_SETUP_FILE_NAME),
        )
        .expect("the sidecar is copied");
        let app = omb_app!(omb_state(Some(results_dir.path())));
        let id = fixture_id(&RC1_DEFAULT);

        for (uri, expected) in [
            (
                result_uri(&id),
                r#"attachment; filename="run \"one\".json""#,
            ),
            (
                setup_uri(&id),
                r#"attachment; filename="run \"one\".setup.toml""#,
            ),
        ] {
            let response =
                test::call_service(&app, test::TestRequest::get().uri(&uri).to_request()).await;

            assert!(
                response.status().is_success(),
                "{uri}: {}",
                response.status()
            );
            assert_eq!(
                header(&response, CONTENT_DISPOSITION.as_str()),
                Some(expected),
                "{uri}"
            );
        }
    }

    #[actix_web::test]
    async fn given_run_without_sidecar_when_downloading_setup_should_answer_not_found() {
        let app = omb_app!(omb_state(Some(Path::new(FIXTURE_OMB_RESULTS_DIR))));

        for fixture in [&NO_CONSUMERS, &PUBLISH_ERRORS] {
            let response = test::call_service(
                &app,
                test::TestRequest::get()
                    .uri(&setup_uri(&fixture_id(fixture)))
                    .to_request(),
            )
            .await;

            assert_eq!(
                response.status(),
                StatusCode::NOT_FOUND,
                "{}",
                fixture.file_name
            );
            assert_eq!(
                header(&response, CONTENT_TYPE.as_str()),
                Some(JSON_CONTENT_TYPE)
            );
            let body: serde_json::Value = test::read_body_json(response).await;
            assert!(
                body["error"]
                    .as_str()
                    .is_some_and(|error| error.contains("has no setup file")),
                "{body}"
            );
        }
    }

    #[actix_web::test]
    async fn given_id_that_does_not_parse_when_requesting_omb_run_should_answer_bad_request() {
        let app = omb_app!(omb_state(Some(Path::new(FIXTURE_OMB_RESULTS_DIR))));
        let upper_case = fixture_id(&RC1_DEFAULT).to_uppercase();

        for id in ["not-an-id", UUID, upper_case.as_str(), "0123456789abcdeg"] {
            for uri in [run_uri(id), result_uri(id), setup_uri(id)] {
                let response =
                    test::call_service(&app, test::TestRequest::get().uri(&uri).to_request()).await;

                assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
                let body: serde_json::Value = test::read_body_json(response).await;
                assert!(
                    body["error"]
                        .as_str()
                        .is_some_and(|error| error.contains("Invalid OMB run id")),
                    "{uri}: {body}"
                );
            }
        }
    }

    #[actix_web::test]
    async fn given_unknown_id_when_requesting_omb_run_should_answer_not_found() {
        for results_dir in [Some(Path::new(FIXTURE_OMB_RESULTS_DIR)), None] {
            let app = omb_app!(omb_state(results_dir));

            for uri in [
                run_uri(UNKNOWN_ID),
                result_uri(UNKNOWN_ID),
                setup_uri(UNKNOWN_ID),
            ] {
                let response =
                    test::call_service(&app, test::TestRequest::get().uri(&uri).to_request()).await;

                assert_eq!(
                    response.status(),
                    StatusCode::NOT_FOUND,
                    "{uri} with {results_dir:?}"
                );
                let body: serde_json::Value = test::read_body_json(response).await;
                assert!(body["error"].is_string(), "{uri}: {body}");
            }
        }
    }

    #[actix_web::test]
    async fn given_result_changed_or_removed_after_load_when_requesting_should_answer_not_found() {
        let results_dir = tempfile::tempdir().expect("a temporary directory is created");
        let changed_dir = RC1_DEFAULT.copy_into(results_dir.path());
        let removed_dir = RC1_PERSISTED.copy_into(results_dir.path());
        let app = omb_app!(omb_state(Some(results_dir.path())));
        std::fs::write(changed_dir.join(RC1_DEFAULT.file_name), NO_CONSUMERS.json())
            .expect("the result is overwritten");
        std::fs::remove_file(removed_dir.join(RC1_PERSISTED.file_name))
            .expect("the result is removed");

        for fixture in [&RC1_DEFAULT, &RC1_PERSISTED] {
            for uri in [
                run_uri(&fixture_id(fixture)),
                result_uri(&fixture_id(fixture)),
            ] {
                let response =
                    test::call_service(&app, test::TestRequest::get().uri(&uri).to_request()).await;

                assert_eq!(response.status(), StatusCode::NOT_FOUND, "{uri}");
            }
        }
    }

    #[actix_web::test]
    async fn given_sidecar_unreadable_after_load_when_downloading_setup_should_answer_internal_error()
     {
        let results_dir = tempfile::tempdir().expect("a temporary directory is created");
        let run_dir = RC1_DEFAULT.copy_into(results_dir.path());
        let app = omb_app!(omb_state(Some(results_dir.path())));
        let sidecar: PathBuf = run_dir.join(OMB_SETUP_FILE_NAME);
        std::fs::remove_file(&sidecar).expect("the sidecar is removed");
        std::fs::create_dir(&sidecar).expect("a directory takes its place");

        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&setup_uri(&fixture_id(&RC1_DEFAULT)))
                .to_request(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
        let body: serde_json::Value = test::read_body_json(response).await;
        assert!(
            body["error"]
                .as_str()
                .is_some_and(|error| !error.contains(&results_dir.path().display().to_string())),
            "{body}"
        );
    }

    #[actix_web::test]
    async fn given_sidecar_with_secret_written_after_load_when_downloading_setup_should_answer_not_found()
     {
        let results_dir = tempfile::tempdir().expect("a temporary directory is created");
        let run_dir = RC1_DEFAULT.copy_into(results_dir.path());
        let app = omb_app!(omb_state(Some(results_dir.path())));
        std::fs::write(
            run_dir.join(OMB_SETUP_FILE_NAME),
            "[properties.server]\nadmin_token = \"do-not-publish\"\n",
        )
        .expect("the sidecar is overwritten");

        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&setup_uri(&fixture_id(&RC1_DEFAULT)))
                .to_request(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body = test::read_body(response).await;
        assert!(
            !String::from_utf8_lossy(&body).contains("do-not-publish"),
            "{body:?}"
        );
    }

    #[actix_web::test]
    async fn given_unknown_omb_path_when_requesting_should_answer_not_found_json() {
        let app = omb_app!(omb_state(Some(Path::new(FIXTURE_OMB_RESULTS_DIR))));
        let uri = format!("/api/omb/runs/{}/report.html", fixture_id(&RC1_DEFAULT));

        let response =
            test::call_service(&app, test::TestRequest::get().uri(&uri).to_request()).await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(
            header(&response, CONTENT_TYPE.as_str()),
            Some(JSON_CONTENT_TYPE)
        );
    }

    #[actix_web::test]
    async fn given_fixture_run_when_requesting_omb_embed_page_should_return_chart_with_setup_line_and_allow_framing()
     {
        let cache_dir = tempfile::tempdir().expect("a temporary directory is created");
        let app = embed_app!(cache_dir.path());

        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&embed_uri(
                    &fixture_id(&RC1_PERSISTED),
                    "type=quantiles&theme=light",
                ))
                .to_request(),
        )
        .await;

        assert!(response.status().is_success(), "{}", response.status());
        assert_eq!(
            header(&response, CONTENT_TYPE.as_str()),
            Some(HTML_CONTENT_TYPE)
        );
        assert_eq!(header(&response, X_FRAME_OPTIONS.as_str()), None);
        assert_eq!(header(&response, CONTENT_SECURITY_POLICY.as_str()), None);
        let page = page_text(response).await;
        assert!(
            page.contains("<title>Iggy OMB Benchmark Chart</title>"),
            "{page}"
        );
        assert!(
            page.contains(
                r#"var hardwareLine = "AWS i4i.4xlarge, 3 nodes  •  Driver Iggy-persisted  •  Server 0.10.0-rc.1";"#
            ),
            "{page}"
        );
        assert!(page.contains("background: #ffffff"), "{page}");
        assert!(page.contains("function(value) {"), "{page}");
        assert!(!page.contains("#*#*#*#"), "{page}");
    }

    #[actix_web::test]
    async fn given_each_chart_type_when_requesting_omb_embed_page_should_draw_it() {
        let cache_dir = tempfile::tempdir().expect("a temporary directory is created");
        let app = embed_app!(cache_dir.path());
        let id = fixture_id(&RC1_DEFAULT);

        for chart_type in EVERY_CHART_TYPE {
            let response = test::call_service(
                &app,
                test::TestRequest::get()
                    .uri(&embed_uri(&id, &format!("type={chart_type}")))
                    .to_request(),
            )
            .await;

            assert!(response.status().is_success(), "{chart_type}");
            assert!(page_text(response).await.contains("var option = {"));
        }
        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&embed_uri(&id, ""))
                .to_request(),
        )
        .await;
        assert!(response.status().is_success(), "{}", response.status());
        let page = page_text(response).await;
        assert!(
            page.contains(r#""text": "Publish latency - OMB "#),
            "{page}"
        );
        assert!(page.contains("background: #070C18"), "{page}");
    }

    #[actix_web::test]
    async fn given_download_action_when_requesting_omb_embed_should_save_png_named_after_run() {
        let cache_dir = tempfile::tempdir().expect("a temporary directory is created");
        let app = embed_app!(cache_dir.path());
        let id = fixture_id(&RC1_DEFAULT);

        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&embed_uri(&id, "type=ladder&action=download"))
                .to_request(),
        )
        .await;

        assert!(response.status().is_success(), "{}", response.status());
        let page = page_text(response).await;
        assert!(
            page.contains(&format!(r#"a.download = "omb_{id}_ladder.png";"#)),
            "{page}"
        );
    }

    #[actix_web::test]
    async fn given_unknown_theme_or_action_when_requesting_omb_embed_page_should_answer_bad_request()
     {
        let cache_dir = tempfile::tempdir().expect("a temporary directory is created");
        let app = embed_app!(cache_dir.path());
        let id = fixture_id(&RC1_DEFAULT);

        for (query, expected) in [
            ("theme=light_nobg", "Invalid theme"),
            ("theme=dark_nobg", "Invalid theme"),
            ("theme=Light", "Invalid theme"),
            ("theme=blue", "Invalid theme"),
            ("action=view", "Invalid action"),
            ("action=", "Invalid action"),
        ] {
            let uri = embed_uri(&id, &format!("type=ladder&{query}"));
            let response =
                test::call_service(&app, test::TestRequest::get().uri(&uri).to_request()).await;

            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
            let error = error_text(response).await;
            assert!(error.contains(expected), "{uri}: {error}");
        }
    }

    #[actix_web::test]
    async fn given_iggy_bench_or_unknown_chart_type_when_requesting_omb_embed_should_answer_bad_request()
     {
        let cache_dir = tempfile::tempdir().expect("a temporary directory is created");
        let app = embed_app!(cache_dir.path());
        let id = fixture_id(&RC1_DEFAULT);

        for chart_type in ["distribution", "p99"] {
            let query = format!("type={chart_type}");
            for uri in [embed_uri(&id, &query), png_uri(&id, &query)] {
                let response =
                    test::call_service(&app, test::TestRequest::get().uri(&uri).to_request()).await;

                assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{uri}");
                let error = error_text(response).await;
                assert!(error.contains("Invalid chart type"), "{uri}: {error}");
            }
        }
    }

    #[actix_web::test]
    async fn given_iggy_bench_uuid_or_unknown_id_when_requesting_omb_embed_should_answer_bad_request_or_not_found()
     {
        let cache_dir = tempfile::tempdir().expect("a temporary directory is created");
        let app = embed_app!(cache_dir.path());

        for (id, status) in [
            (UUID, StatusCode::BAD_REQUEST),
            (UNKNOWN_ID, StatusCode::NOT_FOUND),
        ] {
            for uri in [embed_uri(id, ""), png_uri(id, "")] {
                let response =
                    test::call_service(&app, test::TestRequest::get().uri(&uri).to_request()).await;

                assert_eq!(response.status(), status, "{uri}");
            }
        }
        assert!(!cache_dir.path().join(EMBED_CACHE_DIR).exists());
    }

    #[actix_web::test]
    async fn given_run_without_consumers_when_requesting_end_to_end_embed_should_answer_not_found()
    {
        let cache_dir = tempfile::tempdir().expect("a temporary directory is created");
        let app = embed_app!(cache_dir.path());
        let id = fixture_id(&NO_CONSUMERS);
        let query = "type=end_to_end_latency";

        for uri in [embed_uri(&id, query), png_uri(&id, query)] {
            let response =
                test::call_service(&app, test::TestRequest::get().uri(&uri).to_request()).await;

            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{uri}");
            let error = error_text(response).await;
            assert!(
                error.contains("has no values for the 'end_to_end_latency' chart"),
                "{uri}: {error}"
            );
        }
        assert!(!cache_dir.path().join(EMBED_CACHE_DIR).exists());
    }

    #[actix_web::test]
    async fn given_bad_theme_or_size_when_requesting_omb_png_should_answer_bad_request_without_caching()
     {
        let cache_dir = tempfile::tempdir().expect("a temporary directory is created");
        let app = embed_app!(cache_dir.path());
        let id = fixture_id(&RC1_DEFAULT);

        for (query, expected) in [
            ("theme=blue", "Invalid theme"),
            ("width=0", "Invalid size"),
            ("height=4097", "Invalid size"),
        ] {
            let response = test::call_service(
                &app,
                test::TestRequest::get()
                    .uri(&png_uri(&id, query))
                    .to_request(),
            )
            .await;

            assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{query}");
            let error = error_text(response).await;
            assert!(error.contains(expected), "{query}: {error}");
        }
        assert!(!cache_dir.path().join(EMBED_CACHE_DIR).exists());
    }

    #[actix_web::test]
    async fn given_cached_omb_png_when_requesting_png_should_serve_it_only_at_default_size() {
        let cache_dir = tempfile::tempdir().expect("a temporary directory is created");
        let embed_dir = cache_dir.path().join(EMBED_CACHE_DIR);
        std::fs::create_dir(&embed_dir).expect("the embed directory is created");
        let summary = fixture_summary(&RC1_DEFAULT);
        let id = summary.id.to_string();
        let default_size = "type=ladder";
        let other_size = "type=ladder&width=800&height=600";
        for query in [default_size, other_size] {
            std::fs::write(cached_png_path(&embed_dir, &summary, query), CACHED_PNG)
                .expect("the cached PNG is written");
        }
        let app = embed_app!(cache_dir.path());

        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&png_uri(&id, default_size))
                .to_request(),
        )
        .await;
        assert!(response.status().is_success(), "{}", response.status());
        assert_eq!(
            header(&response, CONTENT_TYPE.as_str()),
            Some(PNG_CONTENT_TYPE)
        );
        assert_eq!(test::read_body(response).await, CACHED_PNG);

        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&png_uri(&id, other_size))
                .to_request(),
        )
        .await;
        assert!(response.status().is_success(), "{}", response.status());
        let png = test::read_body(response).await;
        assert!(png.starts_with(PNG_SIGNATURE), "{:?}", &png[..8]);
    }

    #[actix_web::test]
    async fn given_cached_png_of_unknown_omb_run_when_requesting_png_should_answer_not_found() {
        let cache_dir = tempfile::tempdir().expect("a temporary directory is created");
        let embed_dir = cache_dir.path().join(EMBED_CACHE_DIR);
        std::fs::create_dir(&embed_dir).expect("the embed directory is created");
        let query = "type=ladder";
        // The name that the run id alone gives, which an unknown run can have in the cache from
        // an older server.
        std::fs::write(
            png_cache_path(
                &embed_dir,
                format!("{PNG_FILE_PREFIX}_{UNKNOWN_ID}"),
                &png_query(query),
            ),
            CACHED_PNG,
        )
        .expect("the cached PNG is written");
        let app = embed_app!(cache_dir.path());

        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&png_uri(UNKNOWN_ID, query))
                .to_request(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[actix_web::test]
    async fn given_run_removed_after_png_was_cached_when_requesting_omb_png_should_answer_not_found()
     {
        let results_dir = tempfile::tempdir().expect("a temporary directory is created");
        let run_dir = RC1_DEFAULT.copy_into(results_dir.path());
        let cache_dir = tempfile::tempdir().expect("a temporary directory is created");
        let (state, omb_cache) = reloadable_omb_state(results_dir.path());
        let app = embed_app!(cache_dir.path(), state);
        let uri = png_uri(&fixture_id(&RC1_DEFAULT), "type=ladder");
        let response =
            test::call_service(&app, test::TestRequest::get().uri(&uri).to_request()).await;
        assert!(response.status().is_success(), "{}", response.status());
        std::fs::remove_dir_all(&run_dir).expect("the run directory is removed");
        omb_cache
            .load()
            .expect("the OMB results directory is readable");

        let response =
            test::call_service(&app, test::TestRequest::get().uri(&uri).to_request()).await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[actix_web::test]
    async fn given_sidecar_changed_after_png_was_cached_when_requesting_omb_png_should_render_it_again()
     {
        let results_dir = tempfile::tempdir().expect("a temporary directory is created");
        let run_dir = RC1_DEFAULT.copy_into(results_dir.path());
        let cache_dir = tempfile::tempdir().expect("a temporary directory is created");
        let (state, omb_cache) = reloadable_omb_state(results_dir.path());
        let app = embed_app!(cache_dir.path(), state);
        let uri = png_uri(&fixture_id(&RC1_DEFAULT), "type=ladder");
        let first =
            test::call_and_read_body(&app, test::TestRequest::get().uri(&uri).to_request()).await;
        assert!(first.starts_with(PNG_SIGNATURE), "{:?}", &first[..8]);
        std::fs::write(
            run_dir.join(OMB_SETUP_FILE_NAME),
            "[environment]\nlabel = \"Another cluster\"\n",
        )
        .expect("the sidecar is overwritten");
        omb_cache
            .load()
            .expect("the OMB results directory is readable");

        let response =
            test::call_service(&app, test::TestRequest::get().uri(&uri).to_request()).await;

        assert!(response.status().is_success(), "{}", response.status());
        let second = test::read_body(response).await;
        assert!(second.starts_with(PNG_SIGNATURE), "{:?}", &second[..8]);
        assert!(second != first, "the PNG with the old setup line came back");
    }

    #[actix_web::test]
    async fn given_directory_in_place_of_cached_png_when_requesting_omb_png_should_render_it() {
        let cache_dir = tempfile::tempdir().expect("a temporary directory is created");
        let embed_dir = cache_dir.path().join(EMBED_CACHE_DIR);
        let summary = fixture_summary(&RC1_DEFAULT);
        let query = "type=ladder";
        std::fs::create_dir_all(cached_png_path(&embed_dir, &summary, query))
            .expect("a directory takes the place of the cached PNG");
        let app = embed_app!(cache_dir.path());

        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&png_uri(&summary.id.to_string(), query))
                .to_request(),
        )
        .await;

        assert!(response.status().is_success(), "{}", response.status());
        let png = test::read_body(response).await;
        assert!(png.starts_with(PNG_SIGNATURE), "{:?}", &png[..8]);
    }

    #[actix_web::test]
    async fn given_fixture_run_when_requesting_omb_png_should_render_it_and_cache_default_size() {
        let cache_dir = tempfile::tempdir().expect("a temporary directory is created");
        let embed_dir = cache_dir.path().join(EMBED_CACHE_DIR);
        let app = embed_app!(cache_dir.path());
        let id = fixture_id(&RC1_PERSISTED);
        let query = "type=quantiles&theme=light_nobg";

        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&png_uri(&id, query))
                .to_request(),
        )
        .await;

        assert!(response.status().is_success(), "{}", response.status());
        assert_eq!(
            header(&response, CONTENT_TYPE.as_str()),
            Some(PNG_CONTENT_TYPE)
        );
        let png = test::read_body(response).await;
        assert!(png.starts_with(PNG_SIGNATURE), "{:?}", &png[..8]);
        assert_eq!(
            std::fs::read(cached_png_path(
                &embed_dir,
                &fixture_summary(&RC1_PERSISTED),
                query
            ))
            .expect("the default size is cached"),
            png
        );
    }

    #[actix_web::test]
    async fn given_concurrent_requests_for_uncached_png_when_rendering_should_render_once() {
        const RENDERED_PNG: &[u8] = b"rendered png";

        for uri in [
            png_uri(&fixture_id(&RC1_DEFAULT), "type=ladder"),
            format!("/embed/{UUID}/chart.png?type=latency"),
        ] {
            let cache_dir = tempfile::tempdir().expect("a temporary directory is created");
            let renders = Arc::new(AtomicUsize::new(0));
            let counted = Arc::clone(&renders);
            let render_pool = PngRenderPool::with_renderer(move |_, _, _, _| {
                counted.fetch_add(1, Ordering::SeqCst);
                Ok(RENDERED_PNG.to_vec())
            })
            .expect("the render threads start");
            let app = test::init_service(
                App::new()
                    .app_data(web::Data::new(AppState {
                        render_pool: Arc::new(render_pool),
                        ..app_state_with_cache_dir(app_state(), cache_dir.path())
                    }))
                    .app_data(web::Data::new(omb_state(Some(Path::new(
                        FIXTURE_OMB_RESULTS_DIR,
                    )))))
                    .service(embed_omb_chart_png)
                    .service(embed_chart_png),
            )
            .await;
            let request =
                || test::call_and_read_body(&app, test::TestRequest::get().uri(&uri).to_request());

            let pngs = tokio::join!(request(), request(), request());

            assert_eq!(
                [pngs.0, pngs.1, pngs.2],
                [RENDERED_PNG, RENDERED_PNG, RENDERED_PNG],
                "{uri}"
            );
            assert_eq!(renders.load(Ordering::SeqCst), 1, "{uri}");
        }
    }

    #[actix_web::test]
    async fn given_embed_page_or_png_when_requesting_should_allow_caching_for_an_hour() {
        let cache_dir = tempfile::tempdir().expect("a temporary directory is created");
        let embed_dir = cache_dir.path().join(EMBED_CACHE_DIR);
        std::fs::create_dir(&embed_dir).expect("the embed directory is created");
        let summary = fixture_summary(&RC1_DEFAULT);
        let id = summary.id.to_string();
        // Cached PNG files, so that no request starts a render.
        for cached_path in [
            cached_png_path(&embed_dir, &summary, "type=ladder"),
            png_cache_path(&embed_dir, UUID, &png_query("type=latency")),
        ] {
            std::fs::write(cached_path, CACHED_PNG).expect("the cached PNG is written");
        }
        let app = embed_app!(cache_dir.path());

        for uri in [
            embed_uri(&id, "type=ladder"),
            png_uri(&id, "type=ladder"),
            format!("/embed/{UUID}?type=latency"),
            format!("/embed/{UUID}/chart.png?type=latency"),
        ] {
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

        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&embed_uri(UNKNOWN_ID, ""))
                .to_request(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        assert_eq!(header(&response, CACHE_CONTROL.as_str()), None);
    }
}
