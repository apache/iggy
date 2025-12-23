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

use crate::configs::http::{HttpConfig, HttpCorsConfig};
use crate::http::diagnostics::request_diagnostics;
use crate::http::http_shard_wrapper::HttpSafeShard;
use crate::http::jwt::jwt_manager::JwtManager;
use crate::http::jwt::middleware::jwt_auth;
use crate::http::metrics::metrics;
use crate::http::shared::AppState;
use crate::http::*;
use crate::shard::IggyShard;
use crate::shard::task_registry::ShutdownToken;
use crate::shard::tasks::periodic::spawn_jwt_token_cleaner;
use crate::shard::transmission::event::ShardEvent;
use crate::streaming::persistence::persister::PersisterKind;
use axum::extract::DefaultBodyLimit;
use axum::extract::connect_info::Connected;
use axum::http::Method;
use axum::{Router, middleware};
use axum_server::tls_rustls::RustlsConfig;
use compio_net::TcpListener;
use err_trail::ErrContext;
use iggy_common::IggyError;
use iggy_common::TransportProtocol;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::rc::Rc;
use std::sync::Arc;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing::{error, info};

#[derive(Debug, Clone, Copy)]
pub struct CompioSocketAddr(pub SocketAddr);

impl From<SocketAddr> for CompioSocketAddr {
    fn from(addr: SocketAddr) -> Self {
        CompioSocketAddr(addr)
    }
}

impl From<CompioSocketAddr> for SocketAddr {
    fn from(addr: CompioSocketAddr) -> Self {
        addr.0
    }
}

impl<'a> Connected<cyper_axum::IncomingStream<'a, TcpListener>> for CompioSocketAddr {
    fn connect_info(target: cyper_axum::IncomingStream<'a, TcpListener>) -> Self {
        let addr = *target.remote_addr();
        CompioSocketAddr(addr)
    }
}

/// Starts the HTTP API server.
/// Returns the address the server is listening on.
pub async fn start_http_server(
    config: HttpConfig,
    persister: Arc<PersisterKind>,
    shard: Rc<IggyShard>,
    shutdown: ShutdownToken,
) -> Result<(), IggyError> {
    if shard.id != 0 {
        info!(
            "HTTP server disabled for shard {} (only runs on shard 0)",
            shard.id
        );
        panic!("HTTP server only runs on shard 0");
    }

    let api_name = if config.tls.enabled {
        "HTTP API (TLS)"
    } else {
        "HTTP API"
    };

    let app_state = build_app_state(&config, persister, shard.clone()).await;
    let mut app = Router::new()
        .merge(system::router(app_state.clone(), &config.metrics))
        .merge(personal_access_tokens::router(app_state.clone()))
        .merge(users::router(app_state.clone()))
        .merge(streams::router(app_state.clone()))
        .merge(topics::router(app_state.clone()))
        .merge(consumer_groups::router(app_state.clone()))
        .merge(consumer_offsets::router(app_state.clone()))
        .merge(partitions::router(app_state.clone()))
        .merge(messages::router(app_state.clone()))
        .layer(DefaultBodyLimit::max(
            config.max_request_size.as_bytes_u64() as usize,
        ))
        .layer(middleware::from_fn_with_state(app_state.clone(), jwt_auth));

    if config.cors.enabled {
        app = app.layer(configure_cors(config.cors)?);
    }

    if config.metrics.enabled {
        app = app.layer(middleware::from_fn_with_state(app_state.clone(), metrics));
    }

    spawn_jwt_token_cleaner(shard.clone(), app_state.clone());

    app = app.layer(middleware::from_fn(request_diagnostics));

    #[cfg(feature = "iggy-web")]
    if config.web_ui {
        app = app.merge(web::router());
        info!("Web UI enabled at /ui");
    }

    #[cfg(not(feature = "iggy-web"))]
    if config.web_ui {
        tracing::warn!(
            "Web UI is enabled in configuration (http.web_ui = true) but the server \
             was not compiled with 'iggy-web' feature. The Web UI will not be available. \
             To enable it, rebuild the server with: cargo build --features iggy-web"
        );
    }

    if !config.tls.enabled {
        let listener = TcpListener::bind(config.address.clone())
            .await
            .unwrap_or_else(|_| panic!("Failed to bind to HTTP address {}", config.address));
        let address = listener
            .local_addr()
            .expect("Failed to get local address for HTTP server");
        info!("Started {api_name} on: {address}");

        // Notify shard about the bound address
        let event = ShardEvent::AddressBound {
            protocol: TransportProtocol::Http,
            address,
        };

        crate::shard::handlers::handle_event(&shard, event)
            .await
            .ok();

        let service = app.into_make_service_with_connect_info::<CompioSocketAddr>();

        let shutdown_token = shutdown.clone();
        let result = cyper_axum::serve(listener, service)
            .with_graceful_shutdown(async move { shutdown_token.wait().await })
            .await;

        match result {
            Ok(()) => {
                info!("{api_name} shut down gracefully");
                Ok(())
            }
            Err(error) => {
                error!("{api_name} server error: {}", error);
                Err(IggyError::CannotBindToSocket(format!("HTTP: {}", error)))
            }
        }
    } else {
        let tls_config = RustlsConfig::from_pem_file(
            PathBuf::from(config.tls.cert_file),
            PathBuf::from(config.tls.key_file),
        )
        .await
        .unwrap();

        let listener = std::net::TcpListener::bind(config.address).unwrap();
        listener
            .set_nonblocking(true)
            .expect("Failed to set TLS listener to non-blocking");
        let address = listener
            .local_addr()
            .expect("Failed to get local address for HTTPS / TLS server");

        info!("Started {api_name} on: {address}");

        // Notify shard about the bound address
        use crate::shard::transmission::event::ShardEvent;
        use iggy_common::TransportProtocol;
        let event = ShardEvent::AddressBound {
            protocol: TransportProtocol::Http,
            address,
        };

        crate::shard::handlers::handle_event(&shard, event)
            .await
            .ok();

        let service = app.into_make_service_with_connect_info::<SocketAddr>();
        let handle = axum_server::Handle::new();
        let shutdown_handle = handle.clone();
        let api_name_for_task = api_name;
        shard
            .task_registry
            .oneshot("http_shutdown_listener")
            .critical(false)
            .run(move |shutdown: ShutdownToken| async move {
                shutdown.wait().await;
                info!("Initiating graceful shutdown for {api_name_for_task}");
                shutdown_handle.graceful_shutdown(None);
                Ok(())
            })
            .spawn();

        let server = axum_server::from_tcp_rustls(listener, tls_config)
            .map_err(|err| IggyError::HttpError(err.to_string()))?
            .handle(handle);
        match server.serve(service).await {
            Ok(()) => {
                info!("{api_name} shut down gracefully");
                Ok(())
            }
            Err(error) => {
                error!("Failed to start {api_name} server, error: {}", error);
                Err(IggyError::CannotBindToSocket(format!("HTTPS: {}", error)))
            }
        }
    }
}

async fn build_app_state(
    config: &HttpConfig,
    persister: Arc<PersisterKind>,
    shard: Rc<IggyShard>,
) -> Arc<AppState> {
    let tokens_path;
    {
        tokens_path = shard.config.system.get_state_tokens_path();
    }

    let jwt_manager = JwtManager::from_config(persister, &tokens_path, &config.jwt);
    if let Err(error) = jwt_manager {
        panic!("Failed to initialize JWT manager: {error}");
    }

    let jwt_manager = jwt_manager.unwrap();
    if jwt_manager.load_revoked_tokens().await.is_err() {
        panic!("Failed to load revoked access tokens");
    }

    Arc::new(AppState {
        jwt_manager,
        shard: HttpSafeShard::new(shard),
    })
}

fn configure_cors(config: HttpCorsConfig) -> Result<CorsLayer, IggyError> {
    let allowed_origins = match config.allowed_origins {
        ref origins if origins.is_empty() => AllowOrigin::default(),
        ref origins if origins.first().unwrap() == "*" => AllowOrigin::any(),
        origins => {
            let parsed: Result<Vec<_>, _> = origins
                .iter()
                .filter(|s| !s.trim().is_empty())
                .map(|s| {
                    s.parse()
                        .with_error(|e| format!("Invalid CORS origin '{s}': {e}"))
                        .map_err(|_| IggyError::InvalidConfiguration)
                })
                .collect();
            AllowOrigin::list(parsed?)
        }
    };

    let allowed_headers: Result<Vec<_>, _> = config
        .allowed_headers
        .iter()
        .filter(|s| !s.trim().is_empty())
        .map(|s| {
            s.parse()
                .with_error(|e| format!("Invalid CORS header '{s}': {e}"))
                .map_err(|_| IggyError::InvalidConfiguration)
        })
        .collect();
    let allowed_headers = allowed_headers?;

    let exposed_headers: Result<Vec<_>, _> = config
        .exposed_headers
        .iter()
        .filter(|s| !s.trim().is_empty())
        .map(|s| {
            s.parse()
                .with_error(|e| format!("Invalid CORS exposed header '{s}': {e}"))
                .map_err(|_| IggyError::InvalidConfiguration)
        })
        .collect();
    let exposed_headers = exposed_headers?;

    let allowed_methods: Result<Vec<_>, _> = config
        .allowed_methods
        .iter()
        .filter(|s| !s.trim().is_empty())
        .map(|s| match s.to_uppercase().as_str() {
            "GET" => Ok(Method::GET),
            "POST" => Ok(Method::POST),
            "PUT" => Ok(Method::PUT),
            "DELETE" => Ok(Method::DELETE),
            "HEAD" => Ok(Method::HEAD),
            "OPTIONS" => Ok(Method::OPTIONS),
            "CONNECT" => Ok(Method::CONNECT),
            "PATCH" => Ok(Method::PATCH),
            "TRACE" => Ok(Method::TRACE),
            _ => Err(IggyError::InvalidConfiguration)
                .with_error(|_| format!("Invalid HTTP method in CORS config: '{s}'")),
        })
        .collect();
    let allowed_methods = allowed_methods?;

    Ok(CorsLayer::new()
        .allow_methods(allowed_methods)
        .allow_origin(allowed_origins)
        .allow_headers(allowed_headers)
        .expose_headers(exposed_headers)
        .allow_credentials(config.allow_credentials)
        .allow_private_network(config.allow_private_network))
}
