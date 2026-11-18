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
mod artifacts;
mod cache;
mod config;
mod dedicated_thread;
mod error;
mod github;
mod handlers;
mod omb;
mod render;

use crate::cache::CacheWatcher;
use actix_cors::Cors;
use actix_files::{self as fs, NamedFile};
use actix_web::{
    App, HttpServer,
    http::header,
    middleware::{Compress, Logger},
    web,
};
use args::IggyBenchDashboardServerArgs;
use bench_dashboard_shared::config::DashboardConfig;
use cache::{BenchmarkCache, SeriesBuilder};
use github::StarCount;
use handlers::AppState;
use handlers::omb::OmbState;
use omb::OmbCache;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;
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
    series_builder: Arc<SeriesBuilder>,
    render_pool: Arc<render::PngRenderPool>,
    cache_dir: PathBuf,
    config: Arc<DashboardConfig>,
    omb_cache: Option<Arc<OmbCache>>,
    _omb_watcher: Option<Arc<CacheWatcher>>,
    github_stars: Arc<StarCount>,
}

async fn index() -> actix_web::Result<NamedFile> {
    Ok(NamedFile::open("frontend/dist/index.html")?)
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

    let config = match config::load(&args.config) {
        Ok(config) => Arc::new(config),
        Err(e) => {
            error!("{}", e);
            std::process::exit(1);
        }
    };

    let cache = Arc::new(BenchmarkCache::new(results_dir.clone(), config.thresholds));
    info!("Starting cache load...");
    let start = std::time::Instant::now();
    if let Err(e) = cache.load() {
        error!("Failed to load cache: {}", e);
        std::process::exit(1);
    }
    let duration = start.elapsed();
    info!("Cache loaded in {:.2?}", duration);

    let reload = Arc::clone(&cache);
    let watcher = match CacheWatcher::new(move || reload.load(), results_dir.clone()) {
        Ok(w) => Arc::new(w),
        Err(e) => {
            error!("Failed to initialize file watcher: {}", e);
            std::process::exit(1);
        }
    };

    let (omb_cache, omb_watcher) = args.omb_results_dir.as_deref().map(start_omb_cache).unzip();

    let series_builder = match SeriesBuilder::start(Arc::clone(&cache)) {
        Ok(builder) => Arc::new(builder),
        Err(e) => {
            error!("Failed to start the series build thread: {}", e);
            std::process::exit(1);
        }
    };

    let render_pool = match render::PngRenderPool::default_pool() {
        Ok(pool) => Arc::new(pool),
        Err(e) => {
            error!("Failed to start the PNG render thread: {}", e);
            std::process::exit(1);
        }
    };

    let github_stars = Arc::new(StarCount::default());
    Arc::clone(&github_stars).refresh_hourly();

    let state = ServerState {
        cache: Arc::clone(&cache),
        _watcher: Arc::clone(&watcher),
        series_builder,
        render_pool,
        cache_dir: args.cache_dir.clone(),
        config,
        omb_cache,
        _omb_watcher: omb_watcher,
        github_stars,
    };

    info!("Starting server on {}", addr);
    info!("Results directory: {}", results_dir.display());
    match &args.omb_results_dir {
        Some(omb_results_dir) => info!("OMB results directory: {}", omb_results_dir.display()),
        None => info!("OMB results directory: not set"),
    }
    info!("Cache directory: {}", args.cache_dir.display());
    info!("Config file: {}", args.config.display());
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
                series_builder: Arc::clone(&state.series_builder),
                render_pool: Arc::clone(&state.render_pool),
                cache_dir: state.cache_dir.clone(),
                config: Arc::clone(&state.config),
            }))
            .app_data(web::Data::new(OmbState {
                cache: state.omb_cache.clone(),
            }))
            .app_data(web::Data::from(Arc::clone(&state.github_stars)))
            .service(handlers::health_check)
            .service(handlers::list_benchmark_summaries)
            .service(handlers::get_dashboard_config)
            .service(handlers::get_github_stars)
            .service(handlers::get_benchmark_report_full)
            .service(handlers::get_benchmark_report_light)
            .service(handlers::get_benchmark_series)
            .service(handlers::get_test_artifacts_zip)
            // Only `/embed/omb/chart.png` matches an OMB and an iggy-bench embed route. Both answer
            // 400 for it, and this order names the missing OMB run id in the error.
            .service(handlers::omb::embed_omb_chart_png)
            .service(handlers::omb::embed_omb_chart)
            .service(handlers::embed_chart_png)
            .service(handlers::embed_chart)
            .service(handlers::omb::list_omb_runs)
            .service(handlers::omb::get_omb_run)
            .service(handlers::omb::download_omb_result)
            .service(handlers::omb::download_omb_setup)
            // Keep this after every /api handler: actix matches services in registration order,
            // so the scope hides an /api handler registered after it.
            .service(web::scope("/api").default_service(web::to(handlers::unknown_api_path)))
            .service(
                fs::Files::new("/", "frontend/dist")
                    .index_file("index.html")
                    .use_last_modified(true),
            )
            .default_service(web::route().to(index))
    })
    .bind(&addr)?
    .run();

    server.await?;

    Ok(())
}

/// Loads the OMB runs and watches their directory, or ends the process, as `main` does for the
/// iggy-bench runs.
fn start_omb_cache(omb_results_dir: &Path) -> (Arc<OmbCache>, Arc<CacheWatcher>) {
    let cache = Arc::new(OmbCache::new(omb_results_dir.to_path_buf()));
    info!("Starting OMB run cache load...");
    let start = Instant::now();
    if let Err(e) = cache.load() {
        error!("Failed to load OMB run cache: {}", e);
        std::process::exit(1);
    }
    info!("OMB run cache loaded in {:.2?}", start.elapsed());

    let reload = Arc::clone(&cache);
    match CacheWatcher::new(move || reload.load(), omb_results_dir.to_path_buf()) {
        Ok(watcher) => (cache, Arc::new(watcher)),
        Err(e) => {
            error!("Failed to initialize the OMB file watcher: {}", e);
            std::process::exit(1);
        }
    }
}
