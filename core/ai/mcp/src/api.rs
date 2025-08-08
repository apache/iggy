/* Licensed to the Apache Software Foundation (ASF) under one
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

use crate::{
    Permissions,
    configs::{HttpApiConfig, configure_cors},
    error::McpRuntimeError,
    service::IggyService,
};
use axum::{Json, Router, routing::get};
use axum_server::tls_rustls::RustlsConfig;
use iggy::prelude::{Consumer, IggyClient};
use rmcp::{
    serde_json,
    transport::{
        StreamableHttpService, streamable_http_server::session::local::LocalSessionManager,
    },
};
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use tokio::spawn;
use tracing::{error, info};

pub async fn init(
    config: HttpApiConfig,
    iggy_client: Arc<IggyClient>,
    iggy_consumer: Arc<Consumer>,
    permissions: Permissions,
) -> Result<(), McpRuntimeError> {
    let service = StreamableHttpService::new(
        move || {
            Ok(IggyService::new(
                iggy_client.clone(),
                iggy_consumer.clone(),
                permissions,
            ))
        },
        LocalSessionManager::default().into(),
        Default::default(),
    );

    if !config.path.starts_with("/") {
        error!("HTTP API path must start with '/'");
        return Err(McpRuntimeError::InvalidApiPath);
    }

    if config.path == "/" {
        error!("HTTP API path cannot be '/'");
        return Err(McpRuntimeError::InvalidApiPath);
    }

    let mut app = Router::new()
        .route("/", get(|| async { "Iggy MCP Server" }))
        .route(
            "/health",
            get(|| async { Json(serde_json::json!({ "status": "healthy" })) }),
        )
        .nest_service(&config.path, service);

    if let Some(cors) = &config.cors
        && cors.enabled
    {
        app = app.layer(configure_cors(cors));
    }

    let tls_enabled = config
        .tls
        .as_ref()
        .map(|tls| tls.enabled)
        .unwrap_or_default();

    if !tls_enabled {
        let listener = tokio::net::TcpListener::bind(&config.address)
            .await
            .map_err(|error| {
                error!("Failed to bind TCP listener: {:?}", error);
                McpRuntimeError::FailedToStartHttpServer
            })?;
        let address = listener
            .local_addr()
            .expect("Failed to get local address for HTTP server");
        info!(
            "HTTP API listening on: {address}, MCP path: {}",
            config.path
        );
        spawn(async move {
            if let Err(error) = axum::serve(
                listener,
                app.into_make_service_with_connect_info::<SocketAddr>(),
            )
            .await
            {
                error!("Failed to start MCP server, error: {error}");
            }
        });
        return Ok(());
    }

    let tls = config.tls.as_ref().expect("TLS configuration is required");
    let tls_config = RustlsConfig::from_pem_file(PathBuf::from(&tls.cert), PathBuf::from(&tls.key))
        .await
        .unwrap();

    let listener = std::net::TcpListener::bind(&config.address).unwrap();
    let address = listener
        .local_addr()
        .expect("Failed to get local address for HTTPS / TLS server");

    info!(
        "HTTP API (TLS) listening on: {address}, MCP path: {}",
        config.path
    );

    spawn(async move {
        if let Err(error) = axum_server::from_tcp_rustls(listener, tls_config)
            .serve(app.into_make_service_with_connect_info::<SocketAddr>())
            .await
        {
            error!("Failed to start MCP server, error: {error}");
        }
    });

    Ok(())
}
