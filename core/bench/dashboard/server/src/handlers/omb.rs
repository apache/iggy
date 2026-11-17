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

//! The endpoints of the OpenMessaging Benchmark (OMB) runs. They take OMB run ids only, so an
//! iggy-bench run id gives 400 here.

use super::{Result, get_client_addr};
use crate::error::IggyBenchDashboardServerError;
use crate::omb::{OmbCache, OmbFileError, OmbRun, OmbRunFiles, read_file};
use actix_web::error::BlockingError;
use actix_web::http::header::{ContentDisposition, ContentType};
use actix_web::{HttpRequest, HttpResponse, get, web};
use bench_dashboard_shared::omb::detail::OmbRunDetail;
use bench_dashboard_shared::omb::id::OmbRunId;
use bench_dashboard_shared::omb::result::OmbResult;
use bench_dashboard_shared::omb::setup::OmbSidecar;
use bench_dashboard_shared::omb::summary::OmbRunList;
use std::io;
use std::path::Path;
use std::str;
use std::sync::Arc;
use tracing::{info, warn};

/// The media type of TOML files.
const TOML_CONTENT_TYPE: &str = "application/toml";

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

fn blocking_error(error: BlockingError) -> IggyBenchDashboardServerError {
    IggyBenchDashboardServerError::InternalError(format!("OMB file read task failed: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::handlers::unknown_api_path;
    use crate::omb::test_fixtures::{
        FIXTURE_OMB_RESULTS_DIR, FixtureRun, NEWEST_FIRST, NO_CONSUMERS, PUBLISH_ERRORS,
        RC1_DEFAULT, RC1_PERSISTED,
    };
    use actix_web::body::MessageBody;
    use actix_web::dev::ServiceResponse;
    use actix_web::http::StatusCode;
    use actix_web::http::header::{CONTENT_DISPOSITION, CONTENT_TYPE};
    use actix_web::{App, test};
    use bench_dashboard_shared::omb::setup::OMB_SETUP_FILE_NAME;
    use bench_dashboard_shared::omb::summary::OmbRunSummary;
    use std::path::PathBuf;

    const JSON_CONTENT_TYPE: &str = "application/json";
    /// A valid id that no fixture run has.
    const UNKNOWN_ID: &str = "0000000000000000";
    const UUID: &str = "6b1e288a-9155-4da3-8d3f-ec4d929de3d3";

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
}
