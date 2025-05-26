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

mod args;
mod cache;
mod error;
mod github;
mod handlers;

use crate::cache::CacheWatcher;
use actix_cors::Cors;
use actix_files::{self as fs, NamedFile};
use actix_web::{
    App, HttpServer,
    http::header,
    middleware::{Compress, Logger},
    web,
};
use args::{IggyBenchDashboardServerArgs, PollGithub};
use cache::BenchmarkCache;
use github::IggyBenchDashboardGithubPoller;
use handlers::AppState;
use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::{
    EnvFilter,
    fmt::{self, format::Format},
    layer::SubscriberExt,
    util::SubscriberInitExt,
};

#[derive(Clone)]
struct ServerState {
    cache: Arc<BenchmarkCache>,
    _watcher: Arc<CacheWatcher>,
}

async fn index() -> actix_web::Result<NamedFile> {
    Ok(NamedFile::open(
        "core/bench/dashboard/frontend/dist/index.html",
    )?)
}

#[actix_web::main]
async fn main() -> Result<(), std::io::Error> {
    let args = IggyBenchDashboardServerArgs::parse();
    args.validate();

    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&args.log_level));

    tracing_subscriber::registry()
        .with(fmt::layer().event_format(Format::default().with_thread_ids(true)))
        .with(env_filter)
        .try_init()
        .unwrap();

    let results_dir = args.results_dir.clone();
    let addr = args.server_addr();
    let cors_origins = args.cors_origins_list();

    let cache = Arc::new(BenchmarkCache::new(results_dir.clone()).await);
    info!("Starting cache load...");
    let start = std::time::Instant::now();
    if let Err(e) = cache.load().await {
        error!("Failed to load cache: {}", e);
        std::process::exit(1);
    }
    let duration = start.elapsed();
    info!("Cache loaded in {:.2?}", duration);

    let watcher = match CacheWatcher::new(Arc::clone(&cache), results_dir.clone()) {
        Ok(w) => Arc::new(w),
        Err(e) => {
            error!("Failed to initialize file watcher: {}", e);
            std::process::exit(1);
        }
    };

    let poller = if let Some(poller) = args.github {
        match poller {
            PollGithub::PollGithub(args) => {
                info!("Starting GithubPoller for branch {}", args.branch);

                Some(IggyBenchDashboardGithubPoller::start(
                    results_dir.clone(),
                    args.branch,
                    args.interval_seconds,
                    cache.clone(),
                ))
            }
        }
    } else {
        None
    };

    let state = ServerState {
        cache: Arc::clone(&cache),
        _watcher: Arc::clone(&watcher),
    };

    info!("Starting server on {}", addr);
    info!("Results directory: {}", results_dir.display());
    info!("Log level: {}", args.log_level);
    info!("CORS origins: {}", args.cors_origins);

    let server = HttpServer::new(move || {
        let state = state.clone();

        let cors = if cors_origins.contains(&"*".to_string()) {
            Cors::default()
                .allow_any_origin()
                .allowed_methods(vec!["GET"])
                .allowed_headers(vec![header::AUTHORIZATION, header::ACCEPT])
                .allowed_header(header::CONTENT_TYPE)
                .max_age(3600)
        } else {
            let origins = cors_origins.clone();
            Cors::default()
                .allowed_origin_fn(move |origin, _req_head| {
                    origins
                        .iter()
                        .any(|allowed| origin.as_bytes().ends_with(allowed.as_bytes()))
                })
                .allowed_methods(vec!["GET"])
                .allowed_headers(vec![header::AUTHORIZATION, header::ACCEPT])
                .allowed_header(header::CONTENT_TYPE)
                .max_age(3600)
        };

        App::new()
            .wrap(cors)
            .wrap(Logger::new(
                r#"%a "%r" %s %b "%{Referer}i" "%{User-Agent}i" %T"#,
            ))
            .wrap(Compress::default())
            .app_data(web::Data::new(AppState {
                cache: Arc::clone(&state.cache),
            }))
            .service(handlers::health_check)
            .service(handlers::list_hardware)
            .service(handlers::list_gitrefs_for_hardware)
            .service(handlers::get_recent_benchmarks) // Register this before the generic gitref endpoint
            .service(handlers::list_benchmarks_for_gitref)
            .service(handlers::list_benchmarks_for_hardware_and_gitref)
            .service(handlers::get_benchmark_report_full)
            .service(handlers::get_benchmark_report_light)
            .service(handlers::get_benchmark_trend)
            .service(handlers::get_test_artifacts_zip)
            .service(
                fs::Files::new("/", "core/bench/dashboard/frontend/dist")
                    .index_file("index.html")
                    .use_last_modified(true),
            )
            .default_service(web::route().to(index))
    })
    .bind(&addr)?
    .run();

    server.await?;

    if let Some(poller) = poller {
        poller.shutdown().await;
    }

    Ok(())
}
