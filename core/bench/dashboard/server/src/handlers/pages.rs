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

//! The answers for the page paths of [`PAGE_PATHS`]. Each page is the frontend app, and the
//! status tells crawlers whether the page exists. The old link forms move to their pages with a
//! permanent redirect, so search engines move their links too.
//!
//! [`PAGE_PATHS`]: bench_dashboard_shared::pages::PAGE_PATHS

use crate::cache::BenchmarkCache;
use crate::omb::OmbCache;
use crate::public_url::PublicUrl;
use actix_web::body;
use actix_web::http::header::{CacheControl, CacheDirective, ContentType, HeaderName, LOCATION};
use actix_web::http::{Method, StatusCode};
use actix_web::{HttpRequest, HttpResponse, Route, guard, web};
use bench_dashboard_frontend::{START_DATA_ID, StartData, ssr};
use bench_dashboard_shared::config::DashboardConfig;
use bench_dashboard_shared::head::{HEAD_END, HEAD_START, PUBLIC_URL_META, PageHead};
use bench_dashboard_shared::html::{escape, json_script_safe};
use bench_dashboard_shared::omb::id::OmbRunId;
use bench_dashboard_shared::omb::summary::OmbRunList;
use bench_dashboard_shared::pages::{ExploreScope, is_page_path, run_path};
use bench_dashboard_shared::summary::BenchmarkSummary;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::{Instant, timeout_at};
use tracing::{debug, error, warn};
use uuid::Uuid;

pub const X_ROBOTS_TAG: HeaderName = HeaderName::from_static("x-robots-tag");
/// The comments around the app root of `index.html`, which holds the splash. For a page that the
/// server renders, it replaces both and everything between them with the root and the HTML of the
/// page.
const BODY_START: &str = "<!-- page-body-start -->";
const BODY_END: &str = "<!-- page-body-end -->";
/// A render that takes longer answers with the splash, and the browser renders the page.
const RENDER_TIMEOUT: Duration = Duration::from_secs(5);

/// What the page routes, `robots.txt` and the sitemap read.
pub struct SiteState {
    /// The `index.html` of the frontend build. Each answer reads it again, because its script names
    /// change with each build. A page that the server rendered hydrates only when the frontend
    /// build has the components that the server was built with, so the two come from one commit.
    pub index_html: PathBuf,
    /// `None` without `--public-url`.
    pub public_url: Option<PublicUrl>,
    /// The headline workloads of the Overview.
    pub config: Arc<DashboardConfig>,
    pub runs: Arc<BenchmarkCache>,
    /// `None` without `--omb-results-dir`.
    pub omb_runs: Option<Arc<OmbCache>>,
    /// One permit for each render on its way. Every worker of the app shares it, so it bounds the
    /// renders of the whole server.
    pub render_slots: Arc<Semaphore>,
}

/// The render slots of [`SiteState`]: one for each thread of the render pool of Yew, which has a
/// thread for each CPU. A request that finds every slot taken waits for one until the deadline of
/// its render.
pub fn render_slots() -> Arc<Semaphore> {
    let threads = thread::available_parallelism().map_or(1, |threads| threads.get());
    Arc::new(Semaphore::new(threads))
}

/// Registers the page routes. Register them before the static files, because the static files
/// match every path.
pub fn configure(config: &mut web::ServiceConfig) {
    config
        .service(web::resource("/").route(get_or_head().to(overview_page)))
        .service(web::resource("/explore").route(get_or_head().to(explore_page)))
        .service(web::resource("/methodology").route(get_or_head().to(methodology_page)))
        .service(web::resource("/omb").route(get_or_head().to(omb_runs_page)))
        .service(web::resource("/compare").route(get_or_head().to(compare_runs_page)))
        .service(web::resource("/compare/releases").route(get_or_head().to(compare_releases_page)))
        .service(web::resource("/omb/compare").route(get_or_head().to(omb_compare_page)))
        .service(web::resource("/runs/{uuid}").route(get_or_head().to(run_page)))
        .service(web::resource("/omb/runs/{id}").route(get_or_head().to(omb_run_page)))
        .service(web::resource("/benchmarks/{uuid}").route(get_or_head().to(old_run_link)))
        .service(
            web::resource("/compare/{left}/{right}").route(get_or_head().to(old_compare_link)),
        );
}

/// The default service of the app. The static files fall back to it too. A page path with a
/// trailing slash moves to the path without it, and every other path gets the app with 404, which
/// shows its not-found page.
pub async fn not_found(state: web::Data<SiteState>, req: HttpRequest) -> HttpResponse {
    let path = req.path();
    let trimmed = path.trim_end_matches('/');
    let is_read = matches!(*req.method(), Method::GET | Method::HEAD);
    if is_read && trimmed.len() < path.len() && is_page_path(trimmed) {
        return moved_permanently(trimmed, req.query_string());
    }
    app_page(&state, StatusCode::NOT_FOUND, &PageHead::not_found()).await
}

/// GET and HEAD, so that link checkers, which often send HEAD, get the status of the page.
fn get_or_head() -> Route {
    web::route().guard(guard::Any(guard::Get()).or(guard::Head()))
}

async fn overview_page(state: web::Data<SiteState>, req: HttpRequest) -> HttpResponse {
    let summaries = state.runs.get_summaries();
    let head = PageHead::overview(&summaries, &state.config);
    rendered_page(&state, &req, &head, || bench_start(&state, &summaries)).await
}

/// Each scope is a page of its own, with its own head. A search or a filter only narrows the
/// list, so its head is the head of the scope.
async fn explore_page(state: web::Data<SiteState>, req: HttpRequest) -> HttpResponse {
    let pairs = web::Query::<Vec<(String, String)>>::from_query(req.query_string())
        .map(web::Query::into_inner)
        .unwrap_or_default();
    let scope = ExploreScope::of_query(pairs);
    let summaries = state.runs.get_summaries();
    let head = PageHead::explore(&summaries, &state.config, scope);
    rendered_page(&state, &req, &head, || bench_start(&state, &summaries)).await
}

/// The page counts the runs of each suite, so it reads every run.
async fn methodology_page(state: web::Data<SiteState>, req: HttpRequest) -> HttpResponse {
    let summaries = state.runs.get_summaries();
    rendered_page(&state, &req, &PageHead::methodology(), || {
        bench_start(&state, &summaries)
    })
    .await
}

/// The page exists without `--omb-results-dir` too, and says that the server reads no OMB results.
/// The list is then the one that `/api/omb/runs` answers.
async fn omb_runs_page(state: web::Data<SiteState>, req: HttpRequest) -> HttpResponse {
    let list = state.omb_runs.as_ref().map_or_else(
        || Arc::new(OmbRunList::default()),
        |omb_runs| omb_runs.run_list(),
    );
    let head = PageHead::omb_runs(&list);
    let start = || StartData {
        omb_runs: Some(list.as_ref().clone()),
        ..StartData::default()
    };
    rendered_page(&state, &req, &head, start).await
}

async fn compare_runs_page(state: web::Data<SiteState>) -> HttpResponse {
    app_page(&state, StatusCode::OK, &PageHead::compare_runs()).await
}

async fn compare_releases_page(state: web::Data<SiteState>) -> HttpResponse {
    app_page(&state, StatusCode::OK, &PageHead::compare_releases()).await
}

async fn omb_compare_page(state: web::Data<SiteState>) -> HttpResponse {
    app_page(&state, StatusCode::OK, &PageHead::omb_compare()).await
}

/// A run id in another form, for example in uppercase, moves to the hyphenated lowercase form, so
/// that each run has one link. The head and the start data come from one snapshot of the runs, so
/// a run that the watcher removes meanwhile does not give a found head with a missing-run body.
async fn run_page(
    state: web::Data<SiteState>,
    text: web::Path<String>,
    req: HttpRequest,
) -> HttpResponse {
    let summaries = state.runs.get_summaries();
    let run = Uuid::parse_str(&text)
        .ok()
        .and_then(|uuid| summaries.iter().find(|run| run.uuid == uuid));
    match run {
        Some(run) if run.uuid.to_string() == *text => {
            let head = PageHead::run(run);
            let start = || StartData::run_page(run, &summaries, &state.config);
            rendered_page(&state, &req, &head, start).await
        }
        Some(run) => moved_permanently(&run_path(&run.uuid), req.query_string()),
        None => app_page(&state, StatusCode::NOT_FOUND, &PageHead::not_found()).await,
    }
}

/// An OMB run id has one form only, so no link moves. The start data holds the run, so the browser
/// finds it too.
async fn omb_run_page(
    state: web::Data<SiteState>,
    text: web::Path<String>,
    req: HttpRequest,
) -> HttpResponse {
    let list = state.omb_runs.as_ref().map(|omb_runs| omb_runs.run_list());
    let found = list
        .as_deref()
        .zip(text.parse::<OmbRunId>().ok())
        .and_then(|(list, id)| Some((list, list.runs.iter().find(|run| run.id == id)?)));
    match found {
        Some((list, run)) => {
            let head = PageHead::omb_run(run);
            let start = || StartData::omb_run_page(run, list);
            rendered_page(&state, &req, &head, start).await
        }
        None => app_page(&state, StatusCode::NOT_FOUND, &PageHead::not_found()).await,
    }
}

/// The run link from before `/runs`. Blog posts and embed snippets still use it.
async fn old_run_link(
    state: web::Data<SiteState>,
    text: web::Path<String>,
    req: HttpRequest,
) -> HttpResponse {
    match Uuid::parse_str(&text) {
        Ok(uuid) => moved_permanently(&run_path(&uuid), req.query_string()),
        Err(_) => app_page(&state, StatusCode::NOT_FOUND, &PageHead::not_found()).await,
    }
}

/// The compare link from before the `runs` query. The location names the parsed ids only, never
/// the text of the link.
async fn old_compare_link(
    state: web::Data<SiteState>,
    texts: web::Path<(String, String)>,
) -> HttpResponse {
    let (left, right) = texts.into_inner();
    match (Uuid::parse_str(&left), Uuid::parse_str(&right)) {
        (Ok(left), Ok(right)) => moved_permanently(&format!("/compare?runs={left},{right}"), ""),
        _ => app_page(&state, StatusCode::NOT_FOUND, &PageHead::not_found()).await,
    }
}

/// A redirect to a path on this host. The location has no host, so it works without
/// `--public-url`.
fn moved_permanently(path: &str, query: &str) -> HttpResponse {
    let location = if query.is_empty() {
        path.to_owned()
    } else {
        format!("{path}?{query}")
    };
    HttpResponse::MovedPermanently()
        .insert_header((LOCATION, location))
        .finish()
}

/// The frontend app with `status` and the tags of `head`. The browser renders the page.
async fn app_page(state: &SiteState, status: StatusCode, head: &PageHead) -> HttpResponse {
    match read_index(state).await {
        Some(html) => page_response(state, status, head, html, None),
        None => HttpResponse::InternalServerError().finish(),
    }
}

/// The page at the path of `req`, rendered to HTML from `start`, the data that the page reads. The
/// browser hydrates it. If the render fails or takes too long, the answer is the page of
/// [`app_page`]. A HEAD request gets the headers of that page at once, so `start` runs for GET
/// only.
async fn rendered_page(
    state: &SiteState,
    req: &HttpRequest,
    head: &PageHead,
    start: impl FnOnce() -> StartData,
) -> HttpResponse {
    if req.method() == Method::HEAD {
        return without_length(app_page(state, StatusCode::OK, head).await);
    }
    let Some(html) = read_index(state).await else {
        return HttpResponse::InternalServerError().finish();
    };
    let body = render(
        &state.render_slots,
        RENDER_TIMEOUT,
        req.path(),
        req.query_string(),
        start,
    )
    .await;
    page_response(state, StatusCode::OK, head, html, body)
}

/// `response` without its body and its length. The length of the splash page is not the length
/// that a GET answer with the rendered page has, so a HEAD answer must not name it.
fn without_length(response: HttpResponse) -> HttpResponse {
    response.set_body(body::None::new()).map_into_boxed_body()
}

/// The start data of the iggy-bench pages that read every run: `summaries` and the headline
/// configuration.
fn bench_start(state: &SiteState, summaries: &[BenchmarkSummary]) -> StartData {
    StartData {
        summaries: Some(summaries.to_vec()),
        config: Some(state.config.as_ref().clone()),
        ..StartData::default()
    }
}

/// The app HTML at `path` and the start data JSON. `None` when no render slot frees up within
/// `timeout`, or when the render fails or does not end in that time. `start` runs only after the
/// render has a slot.
async fn render(
    slots: &Arc<Semaphore>,
    timeout: Duration,
    path: &str,
    query: &str,
    start: impl FnOnce() -> StartData,
) -> Option<RenderedBody> {
    let deadline = Instant::now() + timeout;
    let Ok(Ok(slot)) = timeout_at(deadline, Arc::clone(slots).acquire_owned()).await else {
        warn!("No render slot was free for '{path}' within {timeout:?}, so the browser renders it");
        return None;
    };
    let start = start();
    let start_json = match serde_json::to_string(&start) {
        Ok(json) => json,
        Err(e) => {
            error!("Can't write the start data of '{path}': {e}");
            return None;
        }
    };
    let render = ssr::render(path.to_owned(), query.to_owned(), start);
    let app = within(deadline, path, slot, render).await?;
    Some(RenderedBody { app, start_json })
}

/// The output of `render`, or `None` after logging why not: a panic, or no output by `deadline`.
/// The render runs as a task of its own, so its panic does not reach the request. The task holds
/// `slot` until the render ends, also after the deadline, because a late render still takes a
/// thread of the render pool.
async fn within(
    deadline: Instant,
    path: &str,
    slot: OwnedSemaphorePermit,
    render: impl Future<Output = String> + Send + 'static,
) -> Option<String> {
    let began = Instant::now();
    let task = tokio::spawn(async move {
        let app = render.await;
        drop(slot);
        app
    });
    match timeout_at(deadline, task).await {
        Ok(Ok(app)) => {
            debug!("Rendered '{path}' in {:.2?}", began.elapsed());
            Some(app)
        }
        Ok(Err(e)) => {
            error!("The render of '{path}' failed, so the browser renders it: {e}");
            None
        }
        Err(_) => {
            warn!(
                "The render of '{path}' did not end by its deadline, {:.2?} after it started, so \
                 the browser renders it",
                began.elapsed()
            );
            None
        }
    }
}

/// The HTML of a rendered page and the data that it was rendered with.
struct RenderedBody {
    app: String,
    start_json: String,
}

/// The frontend build, or `None` after logging why it can't be read. The answer has no validators,
/// so a request with `If-Modified-Since` gets the page and its status too.
async fn read_index(state: &SiteState) -> Option<Vec<u8>> {
    let path = state.index_html.clone();
    match web::block(move || fs::read(&path)).await {
        Ok(Ok(html)) => Some(html),
        Ok(Err(e)) => {
            error!(
                "Can't read the frontend page '{}': {e}",
                state.index_html.display()
            );
            None
        }
        Err(e) => {
            error!("Can't start the read of the frontend page: {e}");
            None
        }
    }
}

/// `html` with the tags of `head`, and with the rendered app of `body` in place of the splash.
fn page_response(
    state: &SiteState,
    status: StatusCode,
    head: &PageHead,
    html: Vec<u8>,
    body: Option<RenderedBody>,
) -> HttpResponse {
    let origin = state.public_url.as_ref().map(PublicUrl::as_str);
    let html = fill_page(html, origin, &head.tags(origin).to_string(), body);
    let mut response = HttpResponse::build(status);
    response
        .insert_header(ContentType::html())
        .insert_header(CacheControl(vec![CacheDirective::NoCache]));
    if let Some(robots) = head.robots {
        response.insert_header((X_ROBOTS_TAG, robots));
    }
    response.body(html)
}

/// `html` with `tags` between [`HEAD_START`] and [`HEAD_END`], and the part from [`BODY_START`] to
/// [`BODY_END`] replaced by the app of `body` and its start data. The head comments stay, because
/// the app finds the tags of the page between them, and `origin` goes in front of them. The start
/// data follows the HTML of the page, so the content arrives first, and the app script, a module,
/// runs after the parser read both. A frontend build without the comments keeps its own head and
/// its splash, and gets no start data, because the browser hydrates a page that has start data.
fn fill_page(
    html: Vec<u8>,
    origin: Option<&str>,
    tags: &str,
    body: Option<RenderedBody>,
) -> Vec<u8> {
    let page = match String::from_utf8(html) {
        Ok(page) => page,
        Err(e) => {
            warn!("The frontend page is not UTF-8, so it keeps its own head: {e}");
            return e.into_bytes();
        }
    };
    let page = match body {
        Some(body) => {
            let root = format!(
                r#"<div id="app">{}</div><script id="{START_DATA_ID}" type="application/json">{}</script>"#,
                body.app,
                json_script_safe(&body.start_json)
            );
            replace_between(&page, BODY_START, BODY_END, &root).unwrap_or_else(|| {
                warn!(
                    "The frontend page has no {BODY_START} and {BODY_END}, so it keeps its splash"
                );
                page
            })
        }
        None => page,
    };
    let origin = origin.map_or_else(String::new, |origin| {
        format!(
            "<meta name=\"{PUBLIC_URL_META}\" content=\"{}\" />\n",
            escape(origin)
        )
    });
    let head = format!("{origin}{HEAD_START}\n{}\n{HEAD_END}", tags.trim_end());
    match replace_between(&page, HEAD_START, HEAD_END, &head) {
        Some(page) => page.into_bytes(),
        None => {
            warn!("The frontend page has no {HEAD_START} and {HEAD_END}, so it keeps its own head");
            page.into_bytes()
        }
    }
}

/// `page` with the part from `start` to `end`, both included, replaced by `replacement`. `None` when
/// the page lacks either comment.
fn replace_between(page: &str, start: &str, end: &str, replacement: &str) -> Option<String> {
    let from = page.find(start)?;
    let to = from + page[from..].find(end)? + end.len();
    let mut replaced = String::with_capacity(page.len() + replacement.len());
    replaced.push_str(&page[..from]);
    replaced.push_str(replacement);
    replaced.push_str(&page[to..]);
    Some(replaced)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::config;
    use crate::omb::test_fixtures::{FIXTURE_OMB_RESULTS_DIR, RC1_DEFAULT};
    use actix_files::Files;
    use actix_web::dev::ServiceResponse;
    use actix_web::http::header::{CACHE_CONTROL, CONTENT_TYPE, IF_MODIFIED_SINCE};
    use actix_web::middleware::Compress;
    use actix_web::{App, HttpServer, test};
    use bench_dashboard_shared::head::NOINDEX_FOLLOW;
    use bench_dashboard_shared::omb::labels::workload_heading;
    use bench_dashboard_shared::pages::{PAGE_PATHS, omb_run_path};
    use std::net::{SocketAddr, TcpListener as StdTcpListener};
    use std::path::Path;
    use tempfile::TempDir;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpStream;
    use tokio::sync::oneshot;
    use url::form_urlencoded;

    const FIXTURE_RESULTS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../fixtures/results");
    pub(crate) const SHIPPED_CONFIG: &str =
        concat!(env!("CARGO_MANIFEST_DIR"), "/../dashboard.toml");
    pub(crate) const CLUSTER_RUN: &str = "48587edc-6d6b-4ab5-8316-c64be4a65135";
    pub(crate) const TWO_PRODUCERS_RUN: &str = "b5c560ad-1ad2-49f0-88dd-c29db75126aa";
    /// No fixture run has it.
    const MISSING_RUN: &str = "00000000-0000-4000-8000-000000000000";
    /// The page up to the head comments, and from them to the end.
    const APP_START: &str = "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\" />";
    const APP_END: &str = "</head><body>app</body></html>";
    const APP_HEAD: &str = "<!-- page-head-start --><title>app</title><!-- page-head-end -->";
    /// The body of a frontend build with the comments around the app root.
    const SPLASH_BODY: &str = "</head><body><!-- page-body-start --><div id=\"app\"><div \
                               class=\"boot-splash\"></div></div><!-- page-body-end --></body></html>";
    const ORIGIN: &str = "https://benchmarks.iggy.apache.org";

    /// A frontend build with only `index.html` and one asset, as `frontend/dist` has them.
    pub(crate) struct FrontendDist {
        dir: TempDir,
    }

    impl FrontendDist {
        pub(crate) fn new() -> Self {
            Self::with_page(&format!("{APP_START}{APP_HEAD}{APP_END}"))
        }

        /// A build with the comments around the app root too, so the server can render into it.
        fn with_app_root() -> Self {
            Self::with_page(&format!("{APP_START}{APP_HEAD}{SPLASH_BODY}"))
        }

        fn with_page(html: &str) -> Self {
            let dir = tempfile::tempdir().expect("create temp dir");
            fs::write(dir.path().join("index.html"), html).expect("write index.html");
            fs::write(dir.path().join("app.css"), "body {}").expect("write app.css");
            Self { dir }
        }

        pub(crate) fn path(&self) -> &Path {
            self.dir.path()
        }
    }

    pub(crate) fn site_state(
        dist: &FrontendDist,
        public_url: Option<&str>,
        with_omb: bool,
    ) -> SiteState {
        let config = config::load(Path::new(SHIPPED_CONFIG)).expect("the shipped config is valid");
        let runs = BenchmarkCache::new(FIXTURE_RESULTS_DIR.into(), config.thresholds);
        runs.load().expect("the fixture results load");
        let omb_runs = with_omb.then(|| {
            let omb_runs = OmbCache::new(FIXTURE_OMB_RESULTS_DIR.into());
            omb_runs.load().expect("the fixture OMB results load");
            Arc::new(omb_runs)
        });
        SiteState {
            index_html: dist.path().join("index.html"),
            public_url: public_url.map(|url| url.parse().expect("a valid public URL")),
            config: Arc::new(config),
            runs: Arc::new(runs),
            omb_runs,
            render_slots: render_slots(),
        }
    }

    /// The page part of the app that `main` builds.
    macro_rules! site {
        ($state:expr, $dist:expr) => {
            test::init_service(
                App::new()
                    .app_data(web::Data::new($state))
                    .configure(configure)
                    .service(Files::new("/", $dist.path()).index_file("index.html"))
                    .default_service(web::to(not_found)),
            )
            .await
        };
    }

    fn request(method: Method, uri: &str) -> test::TestRequest {
        test::TestRequest::default().method(method).uri(uri)
    }

    fn header(response: &ServiceResponse, name: &HeaderName) -> Option<String> {
        response
            .headers()
            .get(name)
            .map(|value| value.to_str().expect("an ASCII header").to_owned())
    }

    /// The app with a head of its own in place of the head comments. Returns that head.
    async fn assert_app_page(response: ServiceResponse, status: StatusCode, uri: &str) -> String {
        assert_eq!(response.status(), status, "{uri}");
        assert_eq!(
            header(&response, &CONTENT_TYPE).as_deref(),
            Some("text/html; charset=utf-8"),
            "{uri}"
        );
        assert_eq!(
            header(&response, &CACHE_CONTROL).as_deref(),
            Some("no-cache"),
            "{uri}"
        );
        let body = String::from_utf8(test::read_body(response).await.to_vec()).expect("UTF-8");
        let head = body
            .strip_prefix(APP_START)
            .and_then(|rest| rest.strip_suffix(APP_END))
            .unwrap_or_else(|| panic!("{uri}: the app around a head: {body}"));
        let head = match head.split_once(&format!("{HEAD_START}\n")) {
            Some((origin, rest)) => {
                assert!(
                    origin.is_empty() || origin.starts_with(r#"<meta name="dashboard-public-url""#),
                    "{uri}: {origin}"
                );
                rest
            }
            None => panic!("{uri}: no {HEAD_START} in {head}"),
        };
        let head = head
            .strip_suffix(&format!("\n{HEAD_END}"))
            .unwrap_or_else(|| panic!("{uri}: no {HEAD_END} after {head}"));
        assert!(head.starts_with("<title>"), "{uri}: {head}");
        assert!(!head.contains("page-head"), "{uri}: {head}");
        head.to_owned()
    }

    fn tags(head: &PageHead, origin: Option<&str>) -> String {
        head.tags(origin).to_string().trim_end().to_owned()
    }

    /// `pattern` with each `:` segment replaced by an id that the fixtures have.
    fn fixture_path(pattern: &str) -> String {
        pattern
            .split('/')
            .map(|segment| match segment {
                ":uuid" | ":left" => CLUSTER_RUN.to_owned(),
                ":right" => TWO_PRODUCERS_RUN.to_owned(),
                ":id" => RC1_DEFAULT.id().to_string(),
                _ => segment.to_owned(),
            })
            .collect::<Vec<_>>()
            .join("/")
    }

    /// The head of each page path, built as the frontend builds it for the same route. `None` for
    /// the old link forms, which move.
    fn expected_head(state: &SiteState, pattern: &str) -> Option<PageHead> {
        let summaries = state.runs.get_summaries();
        let omb_runs = state
            .omb_runs
            .as_ref()
            .expect("the OMB runs load")
            .run_list();
        Some(match pattern {
            "/" => PageHead::overview(&summaries, &state.config),
            "/explore" => PageHead::explore(&summaries, &state.config, ExploreScope::Official),
            "/runs/:uuid" => PageHead::run(
                summaries
                    .iter()
                    .find(|run| run.uuid.to_string() == CLUSTER_RUN)
                    .expect("the fixtures have the cluster run"),
            ),
            "/compare" => PageHead::compare_runs(),
            "/compare/releases" => PageHead::compare_releases(),
            "/omb" => PageHead::omb_runs(&omb_runs),
            "/omb/runs/:id" => PageHead::omb_run(
                omb_runs
                    .runs
                    .iter()
                    .find(|run| run.id == RC1_DEFAULT.id())
                    .expect("the fixtures have the OMB run"),
            ),
            "/omb/compare" => PageHead::omb_compare(),
            "/methodology" => PageHead::methodology(),
            "/404" => PageHead::not_found(),
            _ => return None,
        })
    }

    #[actix_web::test]
    async fn given_each_page_path_when_requested_should_answer_with_head_of_its_page() {
        let dist = FrontendDist::new();
        let state = site_state(&dist, Some(ORIGIN), true);
        let expected: Vec<_> = PAGE_PATHS
            .iter()
            .map(|pattern| (*pattern, expected_head(&state, pattern)))
            .collect();
        let app = site!(state, &dist);

        for (pattern, head) in expected {
            let uri = fixture_path(pattern);

            let response = test::call_service(&app, request(Method::GET, &uri).to_request()).await;

            let Some(head) = head else {
                assert_eq!(response.status(), StatusCode::MOVED_PERMANENTLY, "{uri}");
                continue;
            };
            let status = if pattern == "/404" {
                StatusCode::NOT_FOUND
            } else {
                StatusCode::OK
            };
            let written = assert_app_page(response, status, &uri).await;
            assert_eq!(written, tags(&head, Some(ORIGIN)), "{uri}");
        }
    }

    #[actix_web::test]
    async fn given_public_url_when_requesting_run_page_should_link_page_and_card_absolutely() {
        let dist = FrontendDist::new();
        let app = site!(site_state(&dist, Some(ORIGIN), true), &dist);
        let uri = format!("/runs/{CLUSTER_RUN}");

        let response = test::call_service(&app, request(Method::GET, &uri).to_request()).await;

        let head = assert_app_page(response, StatusCode::OK, &uri).await;
        assert!(head.contains(&format!(
            r#"<link rel="canonical" href="{ORIGIN}/runs/{CLUSTER_RUN}" />"#
        )));
        assert!(head.contains(&format!(
            r#"<meta property="og:image" content="{ORIGIN}/embed/{CLUSTER_RUN}/card.png" />"#
        )));
        assert!(head.contains(r#"<script type="application/ld+json">"#));
    }

    #[actix_web::test]
    async fn given_public_url_or_none_when_requesting_page_should_tell_app_that_address_or_none() {
        for (public_url, meta) in [
            (
                Some(ORIGIN),
                Some(format!(
                    r#"<meta name="{PUBLIC_URL_META}" content="{ORIGIN}" />"#
                )),
            ),
            (None, None),
        ] {
            let dist = FrontendDist::new();
            let app = site!(site_state(&dist, public_url, true), &dist);

            let response = test::call_service(&app, request(Method::GET, "/").to_request()).await;

            let body = String::from_utf8(test::read_body(response).await.to_vec()).expect("UTF-8");
            let expected = match &meta {
                Some(meta) => format!("{APP_START}{meta}\n{HEAD_START}\n"),
                None => format!("{APP_START}{HEAD_START}\n"),
            };
            assert!(body.starts_with(&expected), "{body}");
            assert_eq!(body.contains(PUBLIC_URL_META), meta.is_some(), "{body}");
        }
    }

    #[actix_web::test]
    async fn given_no_public_url_when_requesting_run_page_should_write_no_absolute_link() {
        let dist = FrontendDist::new();
        let app = site!(site_state(&dist, None, true), &dist);
        let uri = format!("/runs/{CLUSTER_RUN}");

        let response = test::call_service(&app, request(Method::GET, &uri).to_request()).await;

        let head = assert_app_page(response, StatusCode::OK, &uri).await;
        for absent in ["canonical", "og:url", "og:image", "ld+json", "https://"] {
            assert!(!head.contains(absent), "{absent} in {head}");
        }
        assert!(head.contains("<title>Pinned producer, 20 producers, 3 nodes, replicated, "));
    }

    #[actix_web::test]
    async fn given_frontend_build_without_head_comments_when_requesting_page_should_keep_its_head()
    {
        let page = "<!doctype html><html><head><title>Own</title></head></html>";
        let dist = FrontendDist::with_page(page);
        let app = site!(site_state(&dist, Some(ORIGIN), true), &dist);

        let response =
            test::call_service(&app, request(Method::GET, "/explore").to_request()).await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(test::read_body(response).await, page);
    }

    #[actix_web::test]
    async fn given_unknown_page_or_run_when_requested_should_answer_404_with_app() {
        let dist = FrontendDist::new();
        let app = site!(site_state(&dist, None, true), &dist);

        for uri in [
            "/no-such-page".to_owned(),
            "/runs".to_owned(),
            format!("/runs/{MISSING_RUN}"),
            "/runs/not-a-uuid".to_owned(),
            "/omb/runs/0000000000000000".to_owned(),
            "/omb/runs/NOT-HEX".to_owned(),
            "/benchmarks/not-a-uuid".to_owned(),
            format!("/compare/{CLUSTER_RUN}/not-a-uuid"),
            "/missing.png".to_owned(),
        ] {
            let response = test::call_service(&app, request(Method::GET, &uri).to_request()).await;

            let head = assert_app_page(response, StatusCode::NOT_FOUND, &uri).await;
            assert_eq!(head, tags(&PageHead::not_found(), None), "{uri}");
        }
    }

    #[actix_web::test]
    async fn given_no_omb_results_when_requesting_omb_run_should_answer_404() {
        let dist = FrontendDist::new();
        let app = site!(site_state(&dist, None, false), &dist);
        let uri = fixture_path("/omb/runs/:id");

        let response = test::call_service(&app, request(Method::GET, &uri).to_request()).await;

        assert_app_page(response, StatusCode::NOT_FOUND, &uri).await;
    }

    #[actix_web::test]
    async fn given_old_links_when_requested_should_move_permanently_to_their_pages() {
        let dist = FrontendDist::new();
        let app = site!(site_state(&dist, None, true), &dist);

        for (uri, location) in [
            (
                format!("/benchmarks/{CLUSTER_RUN}"),
                format!("/runs/{CLUSTER_RUN}"),
            ),
            (
                format!("/benchmarks/{CLUSTER_RUN}?theme=dark"),
                format!("/runs/{CLUSTER_RUN}?theme=dark"),
            ),
            (
                format!("/compare/{CLUSTER_RUN}/{TWO_PRODUCERS_RUN}"),
                format!("/compare?runs={CLUSTER_RUN},{TWO_PRODUCERS_RUN}"),
            ),
            (
                format!("/runs/{}", CLUSTER_RUN.to_uppercase()),
                format!("/runs/{CLUSTER_RUN}"),
            ),
            (
                format!("/runs/{}", CLUSTER_RUN.replace('-', "")),
                format!("/runs/{CLUSTER_RUN}"),
            ),
        ] {
            let response = test::call_service(&app, request(Method::GET, &uri).to_request()).await;

            assert_eq!(response.status(), StatusCode::MOVED_PERMANENTLY, "{uri}");
            assert_eq!(header(&response, &LOCATION), Some(location), "{uri}");
        }
    }

    #[actix_web::test]
    async fn given_page_path_with_trailing_slash_when_requested_should_move_to_path_without_it() {
        let dist = FrontendDist::new();
        let app = site!(site_state(&dist, None, true), &dist);

        for (uri, location) in [
            ("/explore/".to_owned(), "/explore".to_owned()),
            ("/omb/?runs=a".to_owned(), "/omb?runs=a".to_owned()),
            (
                "/compare/releases/".to_owned(),
                "/compare/releases".to_owned(),
            ),
            (
                format!("/runs/{CLUSTER_RUN}/"),
                format!("/runs/{CLUSTER_RUN}"),
            ),
        ] {
            let response = test::call_service(&app, request(Method::GET, &uri).to_request()).await;

            assert_eq!(response.status(), StatusCode::MOVED_PERMANENTLY, "{uri}");
            assert_eq!(header(&response, &LOCATION), Some(location), "{uri}");
        }
    }

    #[actix_web::test]
    async fn given_trailing_slash_on_other_path_when_requested_should_answer_404_without_redirect()
    {
        let dist = FrontendDist::new();
        let app = site!(site_state(&dist, None, true), &dist);

        for uri in ["/no-such-page/", "//evil.example/", "/runs/"] {
            let response = test::call_service(&app, request(Method::GET, uri).to_request()).await;

            assert_eq!(header(&response, &LOCATION), None, "{uri}");
            assert_app_page(response, StatusCode::NOT_FOUND, uri).await;
        }
    }

    #[actix_web::test]
    async fn given_head_request_when_requesting_page_should_answer_with_status_of_get() {
        let dist = FrontendDist::new();
        let app = site!(site_state(&dist, None, true), &dist);

        for (uri, status) in [
            ("/".to_owned(), StatusCode::OK),
            (format!("/runs/{CLUSTER_RUN}"), StatusCode::OK),
            (fixture_path("/omb/runs/:id"), StatusCode::OK),
            (format!("/runs/{MISSING_RUN}"), StatusCode::NOT_FOUND),
            ("/no-such-page".to_owned(), StatusCode::NOT_FOUND),
            (
                format!("/benchmarks/{CLUSTER_RUN}"),
                StatusCode::MOVED_PERMANENTLY,
            ),
        ] {
            let response = test::call_service(&app, request(Method::HEAD, &uri).to_request()).await;

            assert_eq!(response.status(), status, "{uri}");
        }
    }

    #[actix_web::test]
    async fn given_compare_pages_when_requested_should_mark_noindex_follow_and_others_not() {
        let dist = FrontendDist::new();
        let app = site!(site_state(&dist, None, true), &dist);

        for (uri, robots) in [
            ("/compare", Some(NOINDEX_FOLLOW)),
            ("/compare/releases", Some(NOINDEX_FOLLOW)),
            ("/omb/compare", Some(NOINDEX_FOLLOW)),
            ("/", None),
            ("/explore", None),
            ("/methodology", None),
            ("/omb", None),
        ] {
            let response = test::call_service(&app, request(Method::GET, uri).to_request()).await;

            assert_eq!(header(&response, &X_ROBOTS_TAG).as_deref(), robots, "{uri}");
            let head = assert_app_page(response, StatusCode::OK, uri).await;
            let meta = format!(r#"<meta name="robots" content="{NOINDEX_FOLLOW}" />"#);
            assert_eq!(head.contains(&meta), robots.is_some(), "{uri}");
        }
    }

    #[actix_web::test]
    async fn given_conditional_request_when_page_is_missing_should_answer_404_with_app() {
        let dist = FrontendDist::new();
        let app = site!(site_state(&dist, None, true), &dist);
        let uri = format!("/runs/{MISSING_RUN}");

        let response = test::call_service(
            &app,
            test::TestRequest::get()
                .uri(&uri)
                .insert_header((IF_MODIFIED_SINCE, "Sun, 01 Jan 2090 00:00:00 GMT"))
                .to_request(),
        )
        .await;

        assert_app_page(response, StatusCode::NOT_FOUND, &uri).await;
    }

    #[actix_web::test]
    async fn given_missing_frontend_build_when_requesting_page_should_answer_500() {
        let dist = FrontendDist::new();
        let state = site_state(&dist, None, true);
        fs::remove_file(&state.index_html).expect("remove index.html");
        let app = site!(state, &dist);

        let response =
            test::call_service(&app, request(Method::GET, "/explore").to_request()).await;

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }

    /// The app root and the start data of a page answer, or `None` for each that the page lacks.
    async fn rendered_parts(response: ServiceResponse) -> (Option<String>, Option<StartData>) {
        let body = String::from_utf8(test::read_body(response).await.to_vec()).expect("UTF-8");
        let app = body.split_once(r#"<div id="app">"#).map(|(_, rest)| {
            let end = rest
                .find(&format!(r#"</div><script id="{START_DATA_ID}""#))
                .unwrap_or_else(|| rest.len() - "</div></body></html>".len());
            rest[..end].to_owned()
        });
        let start = body
            .split_once(&format!(
                r#"<script id="{START_DATA_ID}" type="application/json">"#
            ))
            .map(|(_, rest)| {
                let json = &rest[..rest.find("</script>").expect("the element ends")];
                serde_json::from_str(json).expect("the start data parses")
            });
        (app, start)
    }

    #[actix_web::test]
    async fn given_rendered_pages_when_requested_should_answer_with_app_html_and_its_start_data() {
        let dist = FrontendDist::with_app_root();
        let state = site_state(&dist, Some(ORIGIN), true);
        let summaries = state.runs.get_summaries();
        let every_run = StartData {
            summaries: Some(summaries.as_ref().clone()),
            config: Some(state.config.as_ref().clone()),
            ..StartData::default()
        };
        let cluster_run = summaries
            .iter()
            .find(|run| run.uuid.to_string() == CLUSTER_RUN)
            .expect("the fixtures have the cluster run");
        // A query goes to the router of the render too, which takes it apart from the path.
        let mut pages = vec![
            ("/".to_owned(), every_run.clone()),
            ("/methodology".to_owned(), every_run.clone()),
            ("/explore".to_owned(), every_run.clone()),
            ("/?utm_source=example".to_owned(), every_run.clone()),
            (
                "/explore?scope=all&q=pinned%20producer".to_owned(),
                every_run.clone(),
            ),
            (
                format!("/runs/{CLUSTER_RUN}?x=1&y=2"),
                StartData::run_page(cluster_run, &summaries, &state.config),
            ),
        ];
        pages.extend(summaries.iter().map(|run| {
            (
                run_path(&run.uuid),
                StartData::run_page(run, &summaries, &state.config),
            )
        }));
        let app = site!(state, &dist);

        for (uri, expected) in pages {
            let response = test::call_service(&app, request(Method::GET, &uri).to_request()).await;

            assert_eq!(response.status(), StatusCode::OK, "{uri}");
            let (app_html, start) = rendered_parts(response).await;
            let app_html = app_html.expect("the page has the app root");
            assert!(app_html.starts_with("<!--<["), "{uri}: {app_html}");
            assert!(app_html.contains(r#"class="page-frame""#), "{uri}");
            assert!(!app_html.contains("boot-splash"), "{uri}");
            assert_eq!(start.as_ref(), Some(&expected), "{uri}");
        }
    }

    /// The HTML of the app at `path`, rendered from `start`.
    async fn rendered_html(path: &str, start: StartData) -> String {
        ssr::render(path.to_owned(), String::new(), start).await
    }

    /// The fixture runs, plus runs around the cluster run: an older version of its workload on its
    /// machine, its workload without the rate limit, and its workload on another machine. The
    /// fixture runs alone share no machine, so their pages have no history and no related runs.
    fn runs_around_cluster_run(state: &SiteState) -> Vec<BenchmarkSummary> {
        let mut summaries = state.runs.get_summaries().as_ref().clone();
        let cluster = summaries
            .iter()
            .find(|run| run.uuid.to_string() == CLUSTER_RUN)
            .expect("the fixtures have the cluster run")
            .clone();
        let mut older = summaries
            .iter()
            .find(|run| run.version_order < cluster.version_order)
            .expect("the fixtures have an older version")
            .clone();
        older.uuid = Uuid::from_u128(1);
        older.machine.clone_from(&cluster.machine);
        older.nodes = cluster.nodes;
        older.workload_key = cluster.workload_key.clone();
        let mut uncapped = cluster.clone();
        uncapped.uuid = Uuid::from_u128(2);
        uncapped.workload_key.rate_limit = None;
        let mut elsewhere = cluster;
        elsewhere.uuid = Uuid::from_u128(3);
        elsewhere.machine = "elsewhere".to_owned();
        summaries.extend([older, uncapped, elsewhere]);
        summaries
    }

    #[actix_web::test]
    async fn given_run_page_when_rendered_from_its_runs_should_give_html_of_render_from_every_run()
    {
        let dist = FrontendDist::new();
        let state = site_state(&dist, None, true);
        let summaries = runs_around_cluster_run(&state);
        let config = state.config.as_ref();
        let every_run = StartData {
            summaries: Some(summaries.clone()),
            config: Some(config.clone()),
            ..StartData::default()
        };

        for run in &summaries {
            let start = StartData::run_page(run, &summaries, config);
            let page_runs: Vec<_> = start
                .run
                .as_ref()
                .expect("the start data has the page runs")
                .runs
                .iter()
                .map(|run| run.uuid)
                .collect();
            if run.uuid.to_string() == CLUSTER_RUN {
                assert_eq!(
                    page_runs,
                    [run.uuid, Uuid::from_u128(1), Uuid::from_u128(2)],
                    "the run, its older version and its run without the rate limit"
                );
            }
            let path = run_path(&run.uuid);

            let from_page_runs = rendered_html(&path, start).await;
            let from_every_run = rendered_html(&path, every_run.clone()).await;

            assert_eq!(from_page_runs, from_every_run, "{path}");
            assert!(!from_page_runs.contains("Run not found"), "{path}");
        }
    }

    #[actix_web::test]
    async fn given_page_runs_of_one_run_when_rendering_another_run_should_wait_for_every_run() {
        let dist = FrontendDist::new();
        let state = site_state(&dist, None, true);
        let summaries = runs_around_cluster_run(&state);
        let cluster = summaries
            .iter()
            .find(|run| run.uuid.to_string() == CLUSTER_RUN)
            .expect("the fixtures have the cluster run");
        let start = StartData::run_page(cluster, &summaries, &state.config);

        for other in [Uuid::from_u128(1), Uuid::from_u128(3)] {
            let html = rendered_html(&run_path(&other), start.clone()).await;

            assert!(!html.contains("Run not found"), "{other}: {html}");
            assert!(html.contains("Loading the runs"), "{other}: {html}");
        }
    }

    #[actix_web::test]
    async fn given_omb_run_page_when_rendered_from_its_runs_should_give_html_of_render_from_every_run()
     {
        let dist = FrontendDist::new();
        let state = site_state(&dist, None, true);
        let list = state
            .omb_runs
            .as_ref()
            .expect("the OMB runs load")
            .run_list();
        let every_run = StartData {
            omb_runs: Some(list.as_ref().clone()),
            ..StartData::default()
        };
        let mut left_out = 0;
        let mut most = 0;

        for run in &list.runs {
            let start = StartData::omb_run_page(run, &list);
            let page_runs = start
                .omb_run
                .as_ref()
                .expect("the start data has the page runs");
            left_out += list.runs.len() - page_runs.runs.runs.len();
            most = most.max(page_runs.runs.runs.len());
            let path = omb_run_path(run.id);

            let from_page_runs = rendered_html(&path, start).await;
            let from_every_run = rendered_html(&path, every_run.clone()).await;

            assert_eq!(from_page_runs, from_every_run, "{path}");
        }
        assert!(left_out > 0, "some OMB run page leaves out a run");
        assert!(most >= 3, "some OMB run page has history and related runs");
    }

    #[actix_web::test]
    async fn given_explore_views_when_requested_should_name_list_of_scope_as_canonical() {
        let dist = FrontendDist::with_app_root();
        let state = site_state(&dist, Some(ORIGIN), true);
        let run_paths: Vec<_> = state
            .runs
            .get_summaries()
            .iter()
            .map(|run| run_path(&run.uuid))
            .collect();
        let app = site!(state, &dist);
        let mut pages = Vec::new();

        for (uri, canonical) in [
            ("/explore", "/explore"),
            ("/explore?kind=producer", "/explore"),
            ("/explore?scope=everything", "/explore"),
            ("/explore?scope=all", "/explore?scope=all"),
            ("/explore?scope=all&kind=producer", "/explore?scope=all"),
        ] {
            let response = test::call_service(&app, request(Method::GET, uri).to_request()).await;

            assert_eq!(response.status(), StatusCode::OK, "{uri}");
            let body = String::from_utf8(test::read_body(response).await.to_vec()).expect("UTF-8");
            assert!(
                body.contains(&format!(
                    r#"<link rel="canonical" href="{ORIGIN}{canonical}" />"#
                )),
                "{uri}"
            );
            pages.push(body);
        }
        // Crawlers reach the runs outside the official suites only through this link.
        assert!(pages[0].contains(r#"href="/explore?scope=all""#));
        for path in &run_paths {
            assert!(pages[3].contains(&format!(r#"href="{path}""#)), "{path}");
        }
    }

    #[actix_web::test]
    async fn given_omb_run_pages_when_requested_should_render_them_with_omb_runs_alone() {
        let dist = FrontendDist::with_app_root();
        let state = site_state(&dist, Some(ORIGIN), true);
        let list = state
            .omb_runs
            .as_ref()
            .expect("the OMB runs load")
            .run_list();
        let app = site!(state, &dist);

        for run in &list.runs {
            let uri = omb_run_path(run.id);

            let response = test::call_service(&app, request(Method::GET, &uri).to_request()).await;

            assert_eq!(response.status(), StatusCode::OK, "{uri}");
            let (app_html, start) = rendered_parts(response).await;
            let app_html = app_html.expect("the page has the app root");
            assert!(!app_html.contains("boot-splash"), "{uri}");
            assert_eq!(
                start.as_ref(),
                Some(&StartData::omb_run_page(run, &list)),
                "{uri}"
            );
            let heading = element(&app_html, "<h1", "</h1>");
            let trail = element(&app_html, r#"<nav aria-label="Breadcrumb""#, "</nav>");
            assert!(
                heading.contains(&workload_heading(&run.workload)),
                "{uri}: {heading}"
            );
            for part in [heading, trail] {
                assert!(!part.contains(&run.workload.name), "{uri}: {part}");
            }
            let query = form_urlencoded::Serializer::new(String::new())
                .append_pair("workload", &run.workload.name)
                .finish();
            let workload_runs = format!(r#"href="/omb?{query}""#);
            assert!(app_html.contains(&workload_runs), "{uri}: {workload_runs}");
        }
    }

    #[actix_web::test]
    async fn given_omb_run_list_when_requested_should_render_it_with_omb_runs_alone() {
        for with_omb in [true, false] {
            let dist = FrontendDist::with_app_root();
            let state = site_state(&dist, Some(ORIGIN), with_omb);
            let list = state
                .omb_runs
                .as_ref()
                .map(|omb_runs| omb_runs.run_list().as_ref().clone())
                .unwrap_or_default();
            let expected = StartData {
                omb_runs: Some(list.clone()),
                ..StartData::default()
            };
            let app = site!(state, &dist);

            for uri in ["/omb", "/omb?q=Iggy&sort=throughput"] {
                let response =
                    test::call_service(&app, request(Method::GET, uri).to_request()).await;

                assert_eq!(response.status(), StatusCode::OK, "{uri}");
                let (app_html, start) = rendered_parts(response).await;
                let app_html = app_html.expect("the page has the app root");
                assert!(!app_html.contains("boot-splash"), "{uri}");
                assert_eq!(start.as_ref(), Some(&expected), "{uri}");
                for run in &list.runs {
                    assert!(
                        uri.contains('?') || app_html.contains(&omb_run_path(run.id)),
                        "{uri}: no link to {}",
                        run.id
                    );
                }
                if !with_omb {
                    assert!(app_html.contains("No OMB results directory"), "{uri}");
                }
            }
        }
    }

    /// The first element of `html` from `open` to `close`, both included.
    fn element<'a>(html: &'a str, open: &str, close: &str) -> &'a str {
        let from = html
            .find(open)
            .unwrap_or_else(|| panic!("no {open} in {html}"));
        let to = from + html[from..].find(close).expect("the element ends") + close.len();
        &html[from..to]
    }

    #[actix_web::test]
    async fn given_pages_that_browser_renders_when_requested_should_keep_splash_and_no_start_data()
    {
        let dist = FrontendDist::with_app_root();
        let app = site!(site_state(&dist, Some(ORIGIN), true), &dist);

        for uri in [
            "/compare".to_owned(),
            "/compare/releases".to_owned(),
            "/omb/compare".to_owned(),
            format!("/runs/{MISSING_RUN}"),
            "/omb/runs/0000000000000000".to_owned(),
        ] {
            let response = test::call_service(&app, request(Method::GET, &uri).to_request()).await;

            let (app_html, start) = rendered_parts(response).await;
            assert!(
                app_html.is_some_and(|html| html.starts_with(r#"<div class="boot-splash">"#)),
                "{uri}"
            );
            assert_eq!(start, None, "{uri}");
        }
    }

    #[actix_web::test]
    async fn given_head_request_when_requesting_rendered_page_should_skip_render_and_keep_get_headers()
     {
        let dist = FrontendDist::with_app_root();
        let app = site!(site_state(&dist, Some(ORIGIN), true), &dist);

        for uri in [
            "/".to_owned(),
            "/explore?scope=all".to_owned(),
            "/methodology".to_owned(),
            format!("/runs/{CLUSTER_RUN}"),
            "/omb".to_owned(),
            fixture_path("/omb/runs/:id"),
        ] {
            let get = test::call_service(&app, request(Method::GET, &uri).to_request()).await;
            let head = test::call_service(&app, request(Method::HEAD, &uri).to_request()).await;

            assert_eq!(head.status(), get.status(), "{uri}");
            for name in [&CONTENT_TYPE, &CACHE_CONTROL, &X_ROBOTS_TAG] {
                assert_eq!(header(&head, name), header(&get, name), "{uri}: {name}");
            }
            let (get_html, _) = rendered_parts(get).await;
            assert!(
                get_html.is_some_and(|html| !html.contains("boot-splash")),
                "{uri}"
            );
            assert!(test::read_body(head).await.is_empty(), "{uri}");
        }
    }

    /// The status line and the headers of the answer to `method` on `uri`, in lowercase, read from a
    /// connection to `address`. The test service of actix skips the encoder, which writes the
    /// `Content-Length` header, so only a connection shows it.
    async fn headers_on_wire(address: SocketAddr, method: &str, uri: &str) -> String {
        let mut stream = TcpStream::connect(address).await.expect("connect");
        let request = format!(
            "{method} {uri} HTTP/1.1\r\nHost: localhost\r\nAccept-Encoding: identity\r\n\
             Connection: close\r\n\r\n"
        );
        stream
            .write_all(request.as_bytes())
            .await
            .expect("send the request");
        let mut answer = Vec::new();
        stream
            .read_to_end(&mut answer)
            .await
            .expect("read the answer");
        let answer = String::from_utf8_lossy(&answer).to_lowercase();
        let (headers, _) = answer.split_once("\r\n\r\n").expect("the headers end");
        headers.to_owned()
    }

    #[actix_web::test]
    async fn given_connection_when_heading_rendered_page_should_name_no_length_and_get_should() {
        let dist = FrontendDist::with_app_root();
        let state = web::Data::new(site_state(&dist, Some(ORIGIN), true));
        let files = dist.path().to_owned();
        let listener = StdTcpListener::bind("127.0.0.1:0").expect("bind a free port");
        let address = listener.local_addr().expect("the bound address");
        let server = HttpServer::new(move || {
            App::new()
                .wrap(Compress::default())
                .app_data(state.clone())
                .configure(configure)
                .service(Files::new("/", &files).index_file("index.html"))
                .default_service(web::to(not_found))
        })
        .workers(1)
        .disable_signals()
        .listen(listener)
        .expect("listen")
        .run();
        let handle = server.handle();
        actix_web::rt::spawn(server);

        for uri in ["/".to_owned(), format!("/runs/{CLUSTER_RUN}")] {
            let get = headers_on_wire(address, "GET", &uri).await;
            let head = headers_on_wire(address, "HEAD", &uri).await;

            assert!(get.starts_with("http/1.1 200"), "{uri}: {get}");
            assert!(get.contains("\r\ncontent-length: "), "{uri}: {get}");
            assert!(head.starts_with("http/1.1 200"), "{uri}: {head}");
            assert!(!head.contains("content-length"), "{uri}: {head}");
            assert!(!head.contains("transfer-encoding"), "{uri}: {head}");
        }
        handle.stop(true).await;
    }

    #[actix_web::test]
    async fn given_frontend_build_without_app_root_comments_when_rendering_should_send_no_start_data()
     {
        let dist = FrontendDist::new();
        let app = site!(site_state(&dist, Some(ORIGIN), true), &dist);

        let response = test::call_service(&app, request(Method::GET, "/").to_request()).await;

        let head = assert_app_page(response, StatusCode::OK, "/").await;
        assert!(!head.contains(START_DATA_ID), "{head}");
    }

    fn soon() -> Instant {
        Instant::now() + Duration::from_millis(50)
    }

    #[actix_web::test]
    async fn given_render_that_panics_or_hangs_when_waiting_should_give_nothing() {
        let slots = Arc::new(Semaphore::new(3));
        let slot = || Arc::clone(&slots).try_acquire_owned().expect("a free slot");

        let finished = within(soon(), "/", slot(), async { "app".to_owned() }).await;
        let panicked = within(soon(), "/", slot(), async { panic!("the render broke") }).await;
        let hung = within(soon(), "/", slot(), std::future::pending::<String>()).await;

        assert_eq!(finished.as_deref(), Some("app"));
        assert_eq!(panicked, None);
        assert_eq!(hung, None);
    }

    #[actix_web::test]
    async fn given_render_past_deadline_when_it_ends_later_should_free_its_slot_only_then() {
        let slots = Arc::new(Semaphore::new(1));
        let slot = Arc::clone(&slots).try_acquire_owned().expect("a free slot");
        let (finish, finished) = oneshot::channel::<()>();
        let render = async move {
            finished.await.ok();
            "app".to_owned()
        };

        let app = within(soon(), "/", slot, render).await;

        assert_eq!(app, None);
        assert_eq!(
            slots.available_permits(),
            0,
            "the late render keeps its slot"
        );
        finish.send(()).expect("the render waits");
        let freed = timeout_at(
            Instant::now() + Duration::from_secs(5),
            Arc::clone(&slots).acquire_owned(),
        )
        .await;
        assert!(freed.is_ok(), "the slot frees once the render ends");
    }

    #[actix_web::test]
    async fn given_every_render_slot_taken_when_rendering_should_give_nothing_without_start_data() {
        let slots = Arc::new(Semaphore::new(1));
        let _taken = Arc::clone(&slots).try_acquire_owned().expect("a free slot");

        let body = render(
            &slots,
            Duration::from_millis(50),
            "/",
            "",
            || -> StartData { unreachable!("the start data waits for a slot") },
        )
        .await;

        assert!(body.is_none());
    }
}
