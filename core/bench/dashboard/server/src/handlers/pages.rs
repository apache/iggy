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
use actix_web::http::header::{CacheControl, CacheDirective, ContentType, HeaderName, LOCATION};
use actix_web::http::{Method, StatusCode};
use actix_web::{HttpRequest, HttpResponse, Route, guard, web};
use bench_dashboard_shared::omb::id::OmbRunId;
use bench_dashboard_shared::pages::is_page_path;
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::error;
use uuid::Uuid;

pub const X_ROBOTS_TAG: HeaderName = HeaderName::from_static("x-robots-tag");
/// For the pages whose query picks the runs. Their links repeat each other, but they link to the
/// run pages.
const NOINDEX_FOLLOW: &str = "noindex, follow";

/// What the page routes, `robots.txt` and the sitemap read.
pub struct SiteState {
    /// The `index.html` of the frontend build. Each answer reads it again, so a new frontend build
    /// serves without a restart. Its script names change with each build.
    pub index_html: PathBuf,
    /// `None` without `--public-url`.
    pub public_url: Option<PublicUrl>,
    pub runs: Arc<BenchmarkCache>,
    /// `None` without `--omb-results-dir`.
    pub omb_runs: Option<Arc<OmbCache>>,
}

/// Registers the page routes. Register them before the static files, because the static files
/// match every path.
pub fn configure(config: &mut web::ServiceConfig) {
    config
        .service(
            web::resource(["/", "/explore", "/methodology", "/omb"])
                .route(get_or_head().to(indexed_page)),
        )
        .service(
            web::resource(["/compare", "/compare/releases", "/omb/compare"])
                .route(get_or_head().to(unindexed_page)),
        )
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
    app_page(&state, StatusCode::NOT_FOUND, None).await
}

/// GET and HEAD, so that link checkers, which often send HEAD, get the status of the page.
fn get_or_head() -> Route {
    web::route().guard(guard::Any(guard::Get()).or(guard::Head()))
}

async fn indexed_page(state: web::Data<SiteState>) -> HttpResponse {
    app_page(&state, StatusCode::OK, None).await
}

async fn unindexed_page(state: web::Data<SiteState>) -> HttpResponse {
    app_page(&state, StatusCode::OK, Some(NOINDEX_FOLLOW)).await
}

/// A run id in another form, for example in uppercase, moves to the hyphenated lowercase form, so
/// that each run has one link.
async fn run_page(
    state: web::Data<SiteState>,
    text: web::Path<String>,
    req: HttpRequest,
) -> HttpResponse {
    match Uuid::parse_str(&text) {
        Ok(uuid) if state.runs.has_run(&uuid) => {
            if uuid.to_string() == *text {
                app_page(&state, StatusCode::OK, None).await
            } else {
                moved_permanently(&run_path(&uuid), req.query_string())
            }
        }
        _ => app_page(&state, StatusCode::NOT_FOUND, None).await,
    }
}

/// An OMB run id has one form only, so no link moves.
async fn omb_run_page(state: web::Data<SiteState>, text: web::Path<String>) -> HttpResponse {
    let exists = text.parse::<OmbRunId>().is_ok_and(|id| {
        state
            .omb_runs
            .as_ref()
            .is_some_and(|omb_runs| omb_runs.has_run(id))
    });
    let status = if exists {
        StatusCode::OK
    } else {
        StatusCode::NOT_FOUND
    };
    app_page(&state, status, None).await
}

/// The run link from before `/runs`. Blog posts and embed snippets still use it.
async fn old_run_link(
    state: web::Data<SiteState>,
    text: web::Path<String>,
    req: HttpRequest,
) -> HttpResponse {
    match Uuid::parse_str(&text) {
        Ok(uuid) => moved_permanently(&run_path(&uuid), req.query_string()),
        Err(_) => app_page(&state, StatusCode::NOT_FOUND, None).await,
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
        _ => app_page(&state, StatusCode::NOT_FOUND, None).await,
    }
}

pub fn run_path(uuid: &Uuid) -> String {
    format!("/runs/{uuid}")
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

/// The frontend app with `status`. The answer has no validators, so a request with
/// `If-Modified-Since` gets the page and its status too.
async fn app_page(
    state: &SiteState,
    status: StatusCode,
    robots: Option<&'static str>,
) -> HttpResponse {
    let path = state.index_html.clone();
    let html = match web::block(move || fs::read(&path)).await {
        Ok(Ok(html)) => html,
        Ok(Err(e)) => {
            error!(
                "Can't read the frontend page '{}': {e}",
                state.index_html.display()
            );
            return HttpResponse::InternalServerError().finish();
        }
        Err(e) => {
            error!("Can't start the read of the frontend page: {e}");
            return HttpResponse::InternalServerError().finish();
        }
    };
    let mut response = HttpResponse::build(status);
    response
        .insert_header(ContentType::html())
        .insert_header(CacheControl(vec![CacheDirective::NoCache]));
    if let Some(robots) = robots {
        response.insert_header((X_ROBOTS_TAG, robots));
    }
    response.body(html)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::config;
    use crate::omb::test_fixtures::{FIXTURE_OMB_RESULTS_DIR, RC1_DEFAULT};
    use actix_files::Files;
    use actix_web::dev::ServiceResponse;
    use actix_web::http::header::{CACHE_CONTROL, CONTENT_TYPE, IF_MODIFIED_SINCE};
    use actix_web::{App, test};
    use bench_dashboard_shared::pages::PAGE_PATHS;
    use std::path::Path;
    use tempfile::TempDir;

    const FIXTURE_RESULTS_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../fixtures/results");
    pub(crate) const SHIPPED_CONFIG: &str =
        concat!(env!("CARGO_MANIFEST_DIR"), "/../dashboard.toml");
    pub(crate) const CLUSTER_RUN: &str = "48587edc-6d6b-4ab5-8316-c64be4a65135";
    pub(crate) const TWO_PRODUCERS_RUN: &str = "b5c560ad-1ad2-49f0-88dd-c29db75126aa";
    /// No fixture run has it.
    const MISSING_RUN: &str = "00000000-0000-4000-8000-000000000000";
    const APP_HTML: &str = "<!doctype html><html lang=\"en\"><title>app</title></html>";

    /// A frontend build with only `index.html` and one asset, as `frontend/dist` has them.
    pub(crate) struct FrontendDist {
        dir: TempDir,
    }

    impl FrontendDist {
        pub(crate) fn new() -> Self {
            let dir = tempfile::tempdir().expect("create temp dir");
            fs::write(dir.path().join("index.html"), APP_HTML).expect("write index.html");
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
            runs: Arc::new(runs),
            omb_runs,
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

    async fn assert_app_page(response: ServiceResponse, status: StatusCode, uri: &str) {
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
        assert_eq!(test::read_body(response).await, APP_HTML, "{uri}");
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

    #[actix_web::test]
    async fn given_each_page_path_when_requested_should_reach_its_page_route() {
        let dist = FrontendDist::new();
        let app = site!(site_state(&dist, None, true), &dist);

        for pattern in PAGE_PATHS {
            let uri = fixture_path(pattern);

            let response = test::call_service(&app, request(Method::GET, &uri).to_request()).await;

            match pattern {
                "/404" => assert_app_page(response, StatusCode::NOT_FOUND, &uri).await,
                "/benchmarks/:uuid" | "/compare/:left/:right" => {
                    assert_eq!(response.status(), StatusCode::MOVED_PERMANENTLY, "{uri}");
                }
                _ => assert_app_page(response, StatusCode::OK, &uri).await,
            }
        }
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

            assert_app_page(response, StatusCode::NOT_FOUND, &uri).await;
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
    async fn given_compare_pages_when_requested_should_send_noindex_follow_and_others_none() {
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

            assert_eq!(response.status(), StatusCode::OK, "{uri}");
            assert_eq!(header(&response, &X_ROBOTS_TAG).as_deref(), robots, "{uri}");
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
}
