/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::context::RuntimeContext;
use crate::stats;
use auth::resolve_api_key;
use axum::{Json, Router, extract::State, middleware, routing::get};
use axum_server::tls_rustls::RustlsConfig;
use config::{HttpConfig, configure_cors};
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use tokio::spawn;
use tracing::{error, info};

mod auth;
pub mod config;
mod error;
mod models;
mod sink;
mod source;

const NAME: &str = env!("CARGO_PKG_NAME");

pub async fn init(config: &HttpConfig, context: Arc<RuntimeContext>) {
    if !config.enabled {
        info!("{NAME} HTTP API is disabled");
        return;
    }

    let mut system_router = Router::new().route("/stats", get(get_stats));

    if config.metrics.enabled {
        system_router = system_router.route(&config.metrics.endpoint, get(get_metrics));
    }

    let system_router = system_router.with_state(context.clone());

    let mut app = Router::new()
        .route("/", get(|| async { "Connector Runtime API" }))
        .route(
            "/health",
            get(|| async { Json(serde_json::json!({ "status": "healthy" })) }),
        )
        .merge(system_router)
        .merge(sink::router(context.clone()))
        .merge(source::router(context.clone()));

    app = app.layer(middleware::from_fn_with_state(
        context.clone(),
        resolve_api_key,
    ));

    if config.cors.enabled {
        app = app.layer(configure_cors(&config.cors));
    }

    if !config.tls.enabled {
        let listener = tokio::net::TcpListener::bind(&config.address)
            .await
            .unwrap_or_else(|_| panic!("Failed to bind to HTTP address {}", config.address));
        let address = listener
            .local_addr()
            .expect("Failed to get local address for HTTP server");
        info!("Started {NAME} HTTP API on: {address}");
        spawn(async move {
            if let Err(error) = axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            {
                error!("Failed to start {NAME} HTTP API, error: {error}");
            }
        });
        return;
    }

    let tls_config = RustlsConfig::from_pem_file(
        PathBuf::from(&config.tls.cert_file),
        PathBuf::from(&config.tls.key_file),
    )
    .await
    .expect("Failed to load TLS certificate or key file");

    let listener =
        std::net::TcpListener::bind(&config.address).expect("Failed to bind TCP listener");
    let address = listener
        .local_addr()
        .expect("Failed to get local address for HTTPS / TLS server");

    info!("Started {NAME} on: {address}");

    spawn(async move {
        let server = axum_server::from_tcp_rustls(listener, tls_config);
        if let Err(error) = server {
            error!("Failed to start HTTP server, error: {error}");
            return;
        }

        let server = server.unwrap();
        if let Err(error) = server
            .serve(app.into_make_service_with_connect_info::<SocketAddr>())
            .await
        {
            error!("Failed to start {NAME} HTTP API, error: {error}");
        }
    });
}

async fn get_metrics(State(context): State<Arc<RuntimeContext>>) -> String {
    context.metrics.get_formatted_output()
}

async fn get_stats(
    State(context): State<Arc<RuntimeContext>>,
) -> Json<stats::ConnectorRuntimeStats> {
    Json(stats::get_runtime_stats(&context).await)
}
