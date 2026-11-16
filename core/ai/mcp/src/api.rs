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

use crate::{
    Permissions,
    configs::{HttpConfig, configure_cors},
    error::McpRuntimeError,
    service::IggyService,
    stream::resolve_token,
};
use axum::{
    Json, Router,
    extract::{Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::{Next, from_fn_with_state},
    response::{IntoResponse, Response},
    routing::get,
};
use axum_server::tls_rustls::RustlsConfig;
use iggy::prelude::{Consumer, IggyClient};
use rmcp::{
    serde_json,
    transport::{
        StreamableHttpService,
        streamable_http_server::{StreamableHttpServerConfig, session::local::LocalSessionManager},
    },
};
use socket2::{Domain, Protocol, Socket, Type};
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use tokio::spawn;
use tracing::{error, info, warn};

const BEARER_SCHEME: &str = "Bearer";

fn create_reusable_listener(address: SocketAddr) -> Result<std::net::TcpListener, McpRuntimeError> {
    let domain = if address.is_ipv6() {
        Domain::IPV6
    } else {
        Domain::IPV4
    };

    let socket = Socket::new(domain, Type::STREAM, Some(Protocol::TCP)).map_err(|e| {
        error!("Failed to create socket: {e}");
        McpRuntimeError::FailedToStartHttpServer
    })?;

    socket.set_reuse_address(true).map_err(|e| {
        error!("Failed to set SO_REUSEADDR: {e}");
        McpRuntimeError::FailedToStartHttpServer
    })?;

    #[cfg(unix)]
    socket.set_reuse_port(true).map_err(|e| {
        error!("Failed to set SO_REUSEPORT: {e}");
        McpRuntimeError::FailedToStartHttpServer
    })?;

    socket.bind(&address.into()).map_err(|e| {
        error!("Failed to bind to {address}: {e}");
        McpRuntimeError::FailedToStartHttpServer
    })?;

    socket.listen(128).map_err(|e| {
        error!("Failed to listen on {address}: {e}");
        McpRuntimeError::FailedToStartHttpServer
    })?;

    let listener: std::net::TcpListener = socket.into();
    listener.set_nonblocking(true).map_err(|e| {
        error!("Failed to set non-blocking: {e}");
        McpRuntimeError::FailedToStartHttpServer
    })?;

    Ok(listener)
}

pub async fn init(
    config: HttpConfig,
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
        transport(&config),
    );

    if !config.path.starts_with("/") {
        error!("HTTP API path must start with '/'");
        return Err(McpRuntimeError::InvalidApiPath);
    }

    if config.path == "/" {
        error!("HTTP API path cannot be '/'");
        return Err(McpRuntimeError::InvalidApiPath);
    }

    let address: SocketAddr = config.address.parse().map_err(|_| {
        error!("Invalid address: {}", config.address);
        McpRuntimeError::InvalidHttpAddress(config.address.clone())
    })?;
    let auth_token = Some(config.auth_token.trim())
        .filter(|auth_token| !auth_token.is_empty())
        .map(resolve_token)
        .transpose()?
        .map(BearerToken);
    if auth_token.is_none() && !address.ip().is_loopback() {
        error!("HTTP address {address} accepts connections from other hosts without an auth token");
        return Err(McpRuntimeError::MissingHttpAuthToken(address));
    }
    if auth_token.is_none() && config.cors.enabled && config.cors.allows_any_origin() {
        error!("CORS allows every origin without an auth token");
        return Err(McpRuntimeError::UnrestrictedCorsWithoutAuthToken);
    }
    if config.allowed_hosts.is_empty() {
        error!("HTTP allowed hosts cannot be empty");
        return Err(McpRuntimeError::EmptyAllowedHosts);
    }

    let mut app = Router::new()
        .route("/", get(|| async { "Iggy MCP Server" }))
        .route(
            "/health",
            get(|| async { Json(serde_json::json!({ "status": "healthy" })) }),
        )
        .nest_service(&config.path, authenticated(service, auth_token));

    if config.cors.enabled {
        app = app.layer(configure_cors(&config.cors)?);
    }

    if !config.tls.enabled {
        let std_listener = create_reusable_listener(address)?;
        let address = std_listener.local_addr().map_err(|error| {
            error!("Failed to get local address for HTTP server: {error}");
            McpRuntimeError::FailedToStartHttpServer
        })?;
        let listener = tokio::net::TcpListener::from_std(std_listener).map_err(|e| {
            error!("Failed to convert to tokio listener: {e}");
            McpRuntimeError::FailedToStartHttpServer
        })?;
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

    let tls_config = RustlsConfig::from_pem_file(
        PathBuf::from(&config.tls.cert_file),
        PathBuf::from(&config.tls.key_file),
    )
    .await
    .map_err(|error| {
        error!("Failed to load TLS certificate or key file: {error}");
        McpRuntimeError::HttpTls(error)
    })?;

    let listener = create_reusable_listener(address)?;
    let address = listener.local_addr().map_err(|error| {
        error!("Failed to get local address for HTTPS / TLS server: {error}");
        McpRuntimeError::FailedToStartHttpServer
    })?;
    let server = axum_server::from_tcp_rustls(listener, tls_config).map_err(|error| {
        error!("Failed to start HTTP server, error: {error}");
        McpRuntimeError::FailedToStartHttpServer
    })?;

    info!(
        "HTTP API (TLS) listening on: {address}, MCP path: {}",
        config.path
    );

    spawn(async move {
        if let Err(error) = server
            .serve(app.into_make_service_with_connect_info::<SocketAddr>())
            .await
        {
            error!("Failed to start MCP server, error: {error}");
        }
    });

    Ok(())
}

/// The token an MCP client sends as `Authorization: Bearer <token>`.
struct BearerToken(String);

impl BearerToken {
    // A repeated header is refused, so a proxy and this server never read different values.
    fn admits(&self, headers: &HeaderMap) -> bool {
        let mut values = headers.get_all(header::AUTHORIZATION).iter();
        let (Some(value), None) = (values.next(), values.next()) else {
            return false;
        };
        let Some((scheme, token)) = value.to_str().ok().and_then(|value| value.split_once(' '))
        else {
            return false;
        };
        scheme.eq_ignore_ascii_case(BEARER_SCHEME)
            && constant_time_eq(token.as_bytes(), self.0.as_bytes())
    }
}

// rmcp also checks each browser Origin, so a request that skips CORS preflight is still refused.
fn transport(config: &HttpConfig) -> StreamableHttpServerConfig {
    let transport =
        StreamableHttpServerConfig::default().with_allowed_hosts(config.allowed_hosts.clone());
    if !config.cors.enabled {
        return transport.enforce_origin_validation();
    }
    if config.cors.allows_any_origin() {
        return transport.disable_allowed_origins();
    }
    transport
        .with_allowed_origins(config.cors.allowed_origins.clone())
        .enforce_origin_validation()
}

fn authenticated(
    service: StreamableHttpService<IggyService, LocalSessionManager>,
    auth_token: Option<BearerToken>,
) -> Router {
    let router = Router::new().fallback_service(service);
    match auth_token {
        Some(auth_token) => router.layer(from_fn_with_state(Arc::new(auth_token), authenticate)),
        None => router,
    }
}

async fn authenticate(
    State(auth_token): State<Arc<BearerToken>>,
    request: Request,
    next: Next,
) -> Response {
    if auth_token.admits(request.headers()) {
        return next.run(request).await;
    }
    warn!("Refused an MCP request without the configured bearer token.");
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, BEARER_SCHEME)],
    )
        .into_response()
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .fold(0u8, |difference, (left, right)| difference | (left ^ right))
            == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn given_authorization_headers_when_checked_should_admit_only_one_exact_bearer_token() {
        let auth_token = BearerToken("secret-token".to_owned());
        let cases = [
            (vec![], false),
            (vec!["Bearer secret-token"], true),
            (vec!["bearer secret-token"], true),
            (vec!["Bearer secret-tokeN"], false),
            (vec!["Bearer secret-token-longer"], false),
            (vec!["Basic secret-token"], false),
            (vec!["secret-token"], false),
            (vec!["Bearer secret-token", "Bearer secret-token"], false),
            (vec!["Bearer other-token", "Bearer secret-token"], false),
        ];

        for (values, admitted) in cases {
            let mut headers = HeaderMap::new();
            for value in values.iter().copied() {
                headers.append(header::AUTHORIZATION, HeaderValue::from_static(value));
            }
            assert_eq!(auth_token.admits(&headers), admitted, "{values:?}");
        }
    }
}
