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

//! The answers for crawlers: `robots.txt`, the sitemap, and the header that keeps the API and the
//! embed pages out of search results.

use super::pages::{SiteState, X_ROBOTS_TAG, run_path};
use crate::error::IggyBenchDashboardServerError;
use crate::public_url::PublicUrl;
use actix_web::body::MessageBody;
use actix_web::dev::{ServiceRequest, ServiceResponse};
use actix_web::http::header::{CacheControl, CacheDirective, ContentType, HeaderValue};
use actix_web::middleware::Next;
use actix_web::{HttpResponse, route, web};
use bench_dashboard_shared::omb::summary::OmbRunList;
use bench_dashboard_shared::summary::BenchmarkSummary;
use chrono::{DateTime, SecondsFormat, Utc};
use std::fmt::Write;

const SITEMAP_PATH: &str = "/sitemap.xml";
/// A crawler reads the files again after this. The sitemap changes when a run is added.
const CRAWL_FILE_MAX_AGE_SECS: u32 = 3600;
/// The path prefixes that search results must not show. The embed pages are iframes in other
/// sites, and the API serves the data of the app. `robots.txt` doesn't block them, because Google
/// renders the app pages with the same API requests as a browser.
const NOINDEX_PREFIXES: [&str; 2] = ["/api", "/embed"];

#[route("/robots.txt", method = "GET", method = "HEAD")]
pub async fn robots_txt(state: web::Data<SiteState>) -> HttpResponse {
    HttpResponse::Ok()
        .insert_header(ContentType::plaintext())
        .insert_header(crawl_file_cache_control())
        .body(robots_text(state.public_url.as_ref()))
}

/// Every page that search engines can index. The sitemap needs absolute links, so it answers 404
/// without `--public-url`.
#[route("/sitemap.xml", method = "GET", method = "HEAD")]
pub async fn sitemap_xml(
    state: web::Data<SiteState>,
) -> Result<HttpResponse, IggyBenchDashboardServerError> {
    let Some(public_url) = &state.public_url else {
        return Err(IggyBenchDashboardServerError::NotFound(
            "The sitemap needs the --public-url argument of the server".to_owned(),
        ));
    };
    let omb_runs = state.omb_runs.as_ref().map(|omb_runs| omb_runs.run_list());
    let body = sitemap(public_url, &state.runs.get_summaries(), omb_runs.as_deref());
    Ok(HttpResponse::Ok()
        .insert_header(ContentType(mime_xml()))
        .insert_header(crawl_file_cache_control())
        .body(body))
}

/// Adds `X-Robots-Tag: noindex` to every answer under [`NOINDEX_PREFIXES`], errors too.
pub async fn noindex_api_and_embeds(
    req: ServiceRequest,
    next: Next<impl MessageBody>,
) -> Result<ServiceResponse<impl MessageBody>, actix_web::Error> {
    let noindex = NOINDEX_PREFIXES
        .iter()
        .any(|prefix| is_under(req.path(), prefix));
    let mut response = next.call(req).await?;
    if noindex {
        response
            .headers_mut()
            .insert(X_ROBOTS_TAG, HeaderValue::from_static("noindex"));
    }
    Ok(response)
}

fn robots_text(public_url: Option<&PublicUrl>) -> String {
    let mut text = String::from("User-agent: *\nAllow: /\n");
    if let Some(public_url) = public_url {
        let _ = write!(text, "\nSitemap: {}\n", public_url.join(SITEMAP_PATH));
    }
    text
}

/// The pages in the order of the app: the overview, the iggy-bench pages, then the OMB pages. The
/// compare pages are not in it, because their links repeat each other.
fn sitemap(
    public_url: &PublicUrl,
    runs: &[BenchmarkSummary],
    omb_runs: Option<&OmbRunList>,
) -> String {
    let newest_run = runs.iter().filter_map(run_time).max();
    let mut pages = vec![
        ("/".to_owned(), newest_run),
        ("/explore".to_owned(), newest_run),
        ("/methodology".to_owned(), None),
    ];
    pages.extend(runs.iter().map(|run| (run_path(&run.uuid), run_time(run))));
    if let Some(omb_runs) = omb_runs {
        let newest_omb_run = omb_runs.runs.iter().map(|run| run.time.instant).max();
        pages.push(("/omb".to_owned(), newest_omb_run));
        pages.extend(
            omb_runs
                .runs
                .iter()
                .map(|run| (format!("/omb/runs/{}", run.id), Some(run.time.instant))),
        );
    }

    let mut xml = String::from(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <urlset xmlns=\"http://www.sitemaps.org/schemas/sitemap/0.9\">\n",
    );
    for (path, modified) in pages {
        let _ = write!(xml, "<url><loc>{}</loc>", xml_text(&public_url.join(&path)));
        if let Some(modified) = modified {
            let _ = write!(
                xml,
                "<lastmod>{}</lastmod>",
                modified.to_rfc3339_opts(SecondsFormat::Secs, true)
            );
        }
        xml.push_str("</url>\n");
    }
    xml.push_str("</urlset>\n");
    xml
}

/// The start of the run. `None` for a time that is not RFC 3339, so the page has no `lastmod`.
fn run_time(run: &BenchmarkSummary) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(&run.timestamp)
        .ok()
        .map(|time| time.with_timezone(&Utc))
}

fn xml_text(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}

fn is_under(path: &str, prefix: &str) -> bool {
    path.strip_prefix(prefix)
        .is_some_and(|rest| rest.is_empty() || rest.starts_with('/'))
}

fn mime_xml() -> actix_web::mime::Mime {
    "application/xml; charset=utf-8"
        .parse()
        .expect("a valid media type")
}

fn crawl_file_cache_control() -> CacheControl {
    CacheControl(vec![
        CacheDirective::Public,
        CacheDirective::MaxAge(CRAWL_FILE_MAX_AGE_SECS),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::BenchmarkCache;
    use crate::config;
    use crate::handlers::pages::tests::{FrontendDist, SHIPPED_CONFIG, site_state};
    use crate::omb::OmbCache;
    use crate::omb::test_fixtures::NEWEST_FIRST;
    use actix_web::dev::ServiceResponse;
    use actix_web::http::StatusCode;
    use actix_web::http::header::CONTENT_TYPE;
    use actix_web::middleware::from_fn;
    use actix_web::{App, test};
    use bench_dashboard_shared::pages::is_page_path;
    use std::path::Path;
    use std::sync::Arc;

    const PUBLIC_URL: &str = "https://benchmarks.iggy.apache.org";

    macro_rules! crawl_app {
        ($state:expr) => {
            test::init_service(
                App::new()
                    .app_data(web::Data::new($state))
                    .service(robots_txt)
                    .service(sitemap_xml),
            )
            .await
        };
    }

    fn header<B>(response: &ServiceResponse<B>, name: &str) -> Option<String> {
        response
            .headers()
            .get(name)
            .map(|value| value.to_str().expect("an ASCII header").to_owned())
    }

    /// The `loc` values of `xml`, without the public URL.
    fn sitemap_paths(xml: &str) -> Vec<String> {
        xml.split("<loc>")
            .skip(1)
            .map(|rest| {
                let loc = &rest[..rest.find("</loc>").expect("a closed loc")];
                loc.strip_prefix(PUBLIC_URL)
                    .expect("an absolute link")
                    .to_owned()
            })
            .collect()
    }

    #[actix_web::test]
    async fn given_public_url_when_requesting_robots_should_allow_all_and_name_sitemap() {
        let dist = FrontendDist::new();
        let app = crawl_app!(site_state(&dist, Some(PUBLIC_URL), true));

        let response = test::call_service(
            &app,
            test::TestRequest::get().uri("/robots.txt").to_request(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            header(&response, CONTENT_TYPE.as_str()).as_deref(),
            Some("text/plain; charset=utf-8")
        );
        assert_eq!(
            test::read_body(response).await,
            "User-agent: *\nAllow: /\n\nSitemap: https://benchmarks.iggy.apache.org/sitemap.xml\n"
        );
    }

    #[actix_web::test]
    async fn given_no_public_url_when_requesting_crawl_files_should_name_no_sitemap() {
        let dist = FrontendDist::new();
        let app = crawl_app!(site_state(&dist, None, true));

        let robots = test::call_and_read_body(
            &app,
            test::TestRequest::get().uri("/robots.txt").to_request(),
        )
        .await;
        let sitemap = test::call_service(
            &app,
            test::TestRequest::get().uri("/sitemap.xml").to_request(),
        )
        .await;

        assert_eq!(robots, "User-agent: *\nAllow: /\n");
        assert_eq!(sitemap.status(), StatusCode::NOT_FOUND);
    }

    #[actix_web::test]
    async fn given_fixture_runs_when_requesting_sitemap_should_list_every_indexable_page() {
        let dist = FrontendDist::new();
        let state = site_state(&dist, Some(PUBLIC_URL), true);
        let run_paths: Vec<_> = state
            .runs
            .get_summaries()
            .iter()
            .map(|run| format!("/runs/{}", run.uuid))
            .collect();
        let app = crawl_app!(state);

        let response = test::call_service(
            &app,
            test::TestRequest::get().uri("/sitemap.xml").to_request(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            header(&response, CONTENT_TYPE.as_str()).as_deref(),
            Some("application/xml; charset=utf-8")
        );
        let xml = String::from_utf8(test::read_body(response).await.to_vec()).expect("UTF-8");
        let mut expected = vec![
            "/".to_owned(),
            "/explore".to_owned(),
            "/methodology".to_owned(),
        ];
        expected.extend(run_paths);
        expected.push("/omb".to_owned());
        expected.extend(
            NEWEST_FIRST
                .iter()
                .map(|run| format!("/omb/runs/{}", run.id())),
        );
        let paths = sitemap_paths(&xml);
        assert_eq!(paths, expected);
        assert!(paths.iter().all(|path| is_page_path(path)), "{paths:?}");
        assert!(
            xml.contains(&format!(
                "<loc>{PUBLIC_URL}/runs/48587edc-6d6b-4ab5-8316-c64be4a65135</loc><lastmod>"
            )),
            "{xml}"
        );
        assert!(
            xml.contains(&format!("<loc>{PUBLIC_URL}/methodology</loc></url>")),
            "{xml}"
        );
    }

    #[actix_web::test]
    async fn given_no_omb_results_when_requesting_sitemap_should_list_no_omb_page() {
        let dist = FrontendDist::new();
        let app = crawl_app!(site_state(&dist, Some(PUBLIC_URL), false));

        let xml = test::call_and_read_body(
            &app,
            test::TestRequest::get().uri("/sitemap.xml").to_request(),
        )
        .await;

        let xml = String::from_utf8(xml.to_vec()).expect("UTF-8");
        assert!(
            sitemap_paths(&xml)
                .iter()
                .all(|path| !path.starts_with("/omb")),
            "{xml}"
        );
    }

    #[actix_web::test]
    async fn given_run_times_when_building_sitemap_should_write_utc_seconds_or_omit_lastmod() {
        let dist = FrontendDist::new();
        let state = site_state(&dist, Some(PUBLIC_URL), false);
        let mut runs = state.runs.get_summaries().as_ref().clone();
        runs.truncate(2);
        runs[0].timestamp = "2026-11-13T11:16:23.870591+02:00".to_owned();
        runs[1].timestamp = "yesterday".to_owned();
        let public_url = PUBLIC_URL.parse().expect("a valid public URL");

        let xml = sitemap(&public_url, &runs, None);

        let newest = "<lastmod>2026-11-13T09:16:23Z</lastmod>";
        assert!(
            xml.contains(&format!("<loc>{PUBLIC_URL}/</loc>{newest}")),
            "{xml}"
        );
        assert!(
            xml.contains(&format!(
                "<loc>{PUBLIC_URL}/runs/{}</loc>{newest}",
                runs[0].uuid
            )),
            "{xml}"
        );
        assert!(
            xml.contains(&format!(
                "<loc>{PUBLIC_URL}/runs/{}</loc></url>",
                runs[1].uuid
            )),
            "{xml}"
        );
    }

    #[actix_web::test]
    async fn given_empty_results_dirs_when_requesting_sitemap_should_list_fixed_pages_only() {
        let dist = FrontendDist::new();
        let empty = tempfile::tempdir().expect("create temp dir");
        let runs_dir = empty.path().join("results");
        let omb_dir = empty.path().join("omb");
        std::fs::create_dir(&runs_dir).expect("create results dir");
        std::fs::create_dir(&omb_dir).expect("create OMB results dir");
        let mut state = site_state(&dist, Some(PUBLIC_URL), false);
        let config = config::load(Path::new(SHIPPED_CONFIG)).expect("the shipped config is valid");
        let runs = BenchmarkCache::new(runs_dir, config.thresholds);
        runs.load().expect("an empty results dir loads");
        let omb_runs = OmbCache::new(omb_dir);
        omb_runs.load().expect("an empty OMB results dir loads");
        state.runs = Arc::new(runs);
        state.omb_runs = Some(Arc::new(omb_runs));
        let app = crawl_app!(state);

        let xml = test::call_and_read_body(
            &app,
            test::TestRequest::get().uri("/sitemap.xml").to_request(),
        )
        .await;

        let xml = String::from_utf8(xml.to_vec()).expect("UTF-8");
        assert_eq!(
            sitemap_paths(&xml),
            ["/", "/explore", "/methodology", "/omb"]
        );
        assert!(!xml.contains("<lastmod>"), "{xml}");
    }

    #[actix_web::test]
    async fn given_api_and_embed_answers_when_sent_should_carry_noindex_on_errors_too() {
        let app = test::init_service(
            App::new()
                .wrap(from_fn(noindex_api_and_embeds))
                .route(
                    "/api/benchmarks",
                    web::get().to(|| async { HttpResponse::Ok().finish() }),
                )
                .route(
                    "/api/missing",
                    web::get().to(|| async { HttpResponse::NotFound().finish() }),
                )
                .route(
                    "/embed/{uuid}",
                    web::get().to(|| async { HttpResponse::BadRequest().finish() }),
                )
                .route(
                    "/apidocs",
                    web::get().to(|| async { HttpResponse::Ok().finish() }),
                )
                .route(
                    "/explore",
                    web::get().to(|| async { HttpResponse::Ok().finish() }),
                ),
        )
        .await;

        for (uri, robots) in [
            ("/api/benchmarks", Some("noindex")),
            ("/api/missing", Some("noindex")),
            ("/embed/not-a-uuid", Some("noindex")),
            ("/apidocs", None),
            ("/explore", None),
        ] {
            let response =
                test::call_service(&app, test::TestRequest::get().uri(uri).to_request()).await;

            assert_eq!(
                header(&response, X_ROBOTS_TAG.as_str()).as_deref(),
                robots,
                "{uri}"
            );
        }
    }
}
