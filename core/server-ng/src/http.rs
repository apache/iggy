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

//! Shard-0 HTTP/REST listener. This root binds the listener and assembles the
//! router; the rest is split across submodules: the `state` bridge and axum
//! `State`, the bearer `extractor`, the `jwt` issuer and its `jwks` resolver,
//! the route `handlers`, the `reads` gates, the `submit` write paths, `wire`
//! request mapping, partition-write `admission`, committed-reply `reply`
//! decoding, the rejection `error` types, and per-credential `session` state.

mod admission;
mod error;
mod extractor;
mod handlers;
mod jwks;
mod jwt;
mod reads;
mod reply;
mod session;
mod state;
mod submit;
mod tls;
mod wire;

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::fmt::Display;
use std::net::SocketAddr;
use std::rc::Rc;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use axum::Router;
use axum::extract::{DefaultBodyLimit, Request};
use axum::http::{HeaderName, HeaderValue, Method};
use axum::middleware::{Next, from_fn};
use axum::routing::{delete, get, post, put};
use configs::http::{HttpConfig, HttpCorsConfig};
use configs::ng_cluster::{ClusterConfig, TransportPorts};
use configs::ng_http_admission::HttpAdmissionConfig;
use configs::server_ng::NgSystemConfig;
use iggy_common::IggyError;
use message_bus::client_listener;
use send_wrapper::SendWrapper;
use tower_http::cors::{AllowOrigin, CorsLayer};
use tracing::{error, info};

use crate::bootstrap::ServerNgShard;
use crate::cluster_meta::ClusterRoster;
use crate::http::handlers::{
    change_password, create_cg, create_partitions, create_pat, create_stream, create_topic,
    create_user, delete_cg, delete_consumer_offset, delete_partitions, delete_pat, delete_segments,
    delete_stream, delete_topic, delete_user, get_cg, get_cgs, get_client, get_clients,
    get_cluster_metadata, get_consumer_offset, get_pats, get_snapshot, get_stats, get_stream,
    get_streams, get_topic, get_topics, get_user, get_users, login_user,
    login_with_personal_access_token, logout_user, ping, poll_messages, purge_stream, purge_topic,
    refresh_token, send_messages, store_consumer_offset, update_permissions, update_stream,
    update_topic, update_user,
};
use crate::http::jwt::JwtManager;
use crate::http::session::RegistrationBarrier;
use crate::http::state::{HttpInner, HttpState, insert_view_header};
use crate::server_error::ServerNgError;

/// Bind the shard-0 HTTP listener and spawn the `cyper-axum` serve loop as a
/// background task on shard 0's compio runtime. Serves HTTPS when
/// `http.tls.enabled` (a TLS accept pump feeds handshaken streams to the
/// serve loop, see [`mod@tls`]), plain HTTP otherwise.
///
/// The caller gates this to shard 0 and to `http.enabled`; the listener stops
/// when the bus shutdown token fires.
///
/// # Errors
///
/// Returns [`ServerNgError`] if the JWT manager cannot be built from
/// `http_config.jwt`, the `[http.cors]` config is invalid, the `[http.tls]`
/// credentials cannot be loaded, or the listener cannot bind to `addr`.
pub async fn start(
    shard: &Rc<ServerNgShard>,
    addr: SocketAddr,
    http_config: &HttpConfig,
    admission: &HttpAdmissionConfig,
    cluster: &ClusterConfig,
    system_config: Arc<NgSystemConfig>,
    self_ports: TransportPorts,
) -> Result<(), ServerNgError> {
    let jwt = JwtManager::build(&http_config.jwt)?;
    // Validated before bind so a bad [http.cors] fails boot before the socket
    // opens and the "started" log prints.
    let cors = http_config
        .cors
        .enabled
        .then(|| configure_cors(&http_config.cors))
        .transpose()?;
    let (listener, bound_addr) = client_listener::tcp::bind(addr).await?;

    let state: HttpState = SendWrapper::new(Rc::new(HttpInner {
        shard: Rc::clone(shard),
        jwt,
        system_config,
        sessions: RefCell::new(HashMap::new()),
        registrations: RegistrationBarrier::default(),
        roster: ClusterRoster {
            enabled: cluster.enabled,
            name: cluster.name.clone(),
            nodes: cluster.nodes.clone(),
            self_ip: bound_addr.ip().to_string(),
            // The self node reports the live bound HTTP port; the other client
            // ports arrive resolved from the caller.
            self_ports: TransportPorts {
                http: Some(bound_addr.port()),
                ..self_ports
            },
            // The HTTP listener is shard-0-only, where the live consensus
            // handle supplies the leader; the published-view fallback is
            // never consulted here.
            metadata_view: Arc::new(AtomicU64::new(crate::cluster_meta::METADATA_VIEW_UNKNOWN)),
        },
        max_in_flight_writes: admission.max_in_flight_writes,
        max_in_flight_writes_per_session: admission.max_in_flight_writes_per_session,
        in_flight_writes: Cell::new(0),
    }));
    // Saturating: a configured limit past the pointer width (32-bit target,
    // >4 GiB value) clamps to the largest enforceable cap instead of wrapping.
    let max_request_size =
        usize::try_from(http_config.max_request_size.as_bytes_u64()).unwrap_or(usize::MAX);
    let router = router(state, max_request_size, cors, http_config.web_ui);

    if http_config.tls.enabled {
        let server_config = tls::load_http_tls_server_config(&http_config.tls)?;
        let (connections, pump) =
            tls::spawn_accept_pump(listener, server_config, shard.bus.token());
        shard.bus.track_background(pump);
        info!(address = %bound_addr, "server-ng HTTPS listener started");
        let handle = compio::runtime::spawn(tls::serve(connections, router, shard.bus.token()));
        shard.bus.track_background(handle);
    } else {
        info!(address = %bound_addr, "server-ng HTTP listener started");
        let shutdown = shard.bus.token();
        let handle = compio::runtime::spawn(async move {
            if let Err(error) = cyper_axum::serve(listener, router)
                .with_graceful_shutdown(async move { shutdown.wait().await })
                .await
            {
                error!(%error, "server-ng HTTP listener terminated with error");
            }
        });
        shard.bus.track_background(handle);
    }

    Ok(())
}

/// Health-probe path. Public and pre-auth, and the one success route reached
/// without proving a credential, so the `X-Iggy-View` layer withholds the
/// cluster-internal view number here (see the response layer below).
const PING_PATH: &str = "/ping";

/// Assemble the shard-0 router: unauthenticated health + login routes plus the
/// authenticated REST surface.
///
/// `max_request_size` becomes the router-wide `DefaultBodyLimit` (413 past
/// it), exactly like the legacy server: it bounds the per-request term of the
/// admission math - what one body may cost in bytes and decode CPU - while
/// the in-flight caps bound the multiplier.
///
/// `cors`, present only when `[http.cors]` is enabled, is applied as the
/// outermost layer. This node authenticates per route (the `Authenticated` /
/// `Identity` extractors), so a preflight `OPTIONS` - which carries no
/// `Authorization` header and matches none of the method routes - would 401 or
/// 405 if it reached the router; the outermost `CorsLayer` answers it first
/// instead, and stamps the CORS response headers over every reply, including
/// the inner layer's `x-iggy-view`.
fn router(
    state: HttpState,
    max_request_size: usize,
    cors: Option<CorsLayer>,
    web_ui: bool,
) -> Router {
    // Cloned for the response layer so `X-Iggy-View` reads the live view per
    // response; the original `state` is moved into `with_state` below.
    let view_source = state.clone();
    let router = Router::new()
        .route(PING_PATH, get(ping))
        .route("/users/login", post(login_user))
        .route("/users/refresh-token", post(refresh_token))
        .route(
            "/personal-access-tokens/login",
            post(login_with_personal_access_token),
        )
        .route("/users", get(get_users).post(create_user))
        .route(
            "/users/{user_id}",
            get(get_user).put(update_user).delete(delete_user),
        )
        .route("/users/{user_id}/password", put(change_password))
        .route("/users/{user_id}/permissions", put(update_permissions))
        // Static `logout` outranks the `{user_id}` capture in axum's matcher, so
        // `DELETE /users/logout` never misroutes to `delete_user`.
        .route("/users/logout", delete(logout_user))
        .route("/streams", get(get_streams).post(create_stream))
        .route(
            "/streams/{stream_id}",
            get(get_stream).put(update_stream).delete(delete_stream),
        )
        .route("/streams/{stream_id}/purge", delete(purge_stream))
        .route(
            "/streams/{stream_id}/topics",
            get(get_topics).post(create_topic),
        )
        .route(
            "/streams/{stream_id}/topics/{topic_id}",
            get(get_topic).put(update_topic).delete(delete_topic),
        )
        .route(
            "/streams/{stream_id}/topics/{topic_id}/purge",
            delete(purge_topic),
        )
        .route(
            "/streams/{stream_id}/topics/{topic_id}/partitions",
            post(create_partitions).delete(delete_partitions),
        )
        .route(
            "/streams/{stream_id}/topics/{topic_id}/partitions/{partition_id}",
            delete(delete_segments),
        )
        .route(
            "/streams/{stream_id}/topics/{topic_id}/messages",
            get(poll_messages).post(send_messages),
        )
        .route(
            "/streams/{stream_id}/topics/{topic_id}/consumer-offsets",
            get(get_consumer_offset).put(store_consumer_offset),
        )
        .route(
            "/streams/{stream_id}/topics/{topic_id}/consumer-offsets/{consumer_id}",
            delete(delete_consumer_offset),
        )
        .route(
            "/streams/{stream_id}/topics/{topic_id}/consumer-groups",
            get(get_cgs).post(create_cg),
        )
        .route(
            "/streams/{stream_id}/topics/{topic_id}/consumer-groups/{group_id}",
            get(get_cg).delete(delete_cg),
        )
        .route("/personal-access-tokens", get(get_pats).post(create_pat))
        .route("/personal-access-tokens/{name}", delete(delete_pat))
        .route("/stats", get(get_stats))
        .route("/snapshot", post(get_snapshot))
        .route("/cluster/metadata", get(get_cluster_metadata))
        .route("/clients", get(get_clients))
        .route("/clients/{client_id}", get(get_client))
        .with_state(state)
        .layer(DefaultBodyLimit::max(max_request_size))
        .layer(from_fn(move |request: Request, next: Next| {
            let view_source = view_source.clone();
            // `/ping` is the sole success route reached without proving a
            // credential, so it must not leak the cluster-internal view number
            // (the anon-leak gate). Every other route authenticates before its
            // handler, so a success/redirect there is an authed flow that may
            // carry the header; the login routes prove credentials on success.
            let suppress_view = request.uri().path() == PING_PATH;
            async move {
                let response = next.run(request).await;
                if suppress_view {
                    response
                } else {
                    insert_view_header(&view_source, response)
                }
            }
        }));
    let router = match cors {
        Some(cors) => router.layer(cors),
        None => router,
    };

    merge_web_ui(router, web_ui)
}

/// Merge the unauthenticated `/ui` static-asset surface when `web_ui` is set.
///
/// The caller merges this outermost - after `with_state`, the body limit, the
/// view-header layer, and CORS - mirroring the legacy server. Staying past the
/// view-header layer also keeps the cluster-internal view number off these
/// unauthenticated responses, the same anon-leak gate `/ping` gets above.
///
/// Without the `iggy-web` feature the assets are not compiled in, so an enabled
/// flag only warns instead of serving.
fn merge_web_ui(router: Router, web_ui: bool) -> Router {
    #[cfg(feature = "iggy-web")]
    let router = if web_ui {
        info!("Web UI enabled at /ui");
        router.merge(crate::web::router())
    } else {
        router
    };

    #[cfg(not(feature = "iggy-web"))]
    if web_ui {
        tracing::warn!(
            "Web UI is enabled in configuration (http.web_ui = true) but the server \
             was not compiled with 'iggy-web' feature. The Web UI will not be available. \
             To enable it, rebuild the server with: cargo build --features iggy-web"
        );
    }

    router
}

/// Build the [`CorsLayer`] from `[http.cors]`, porting the legacy server's
/// mapping: an empty origin list yields the tower-http default, a leading `*`
/// allows any origin (later entries are ignored), and anything else is an
/// explicit allow-list. Entries are trimmed and blank ones dropped, so a
/// placeholder like `[""]` maps to "none" rather than a parse error. Methods,
/// allowed headers, and exposed headers map the same way; any unparsable value
/// fails the build.
///
/// Combinations tower-http would reject by panicking (a `*` origin past the
/// first position, or `allow_credentials` with a wildcard origin, header, or
/// exposed-header list) are rejected here as configuration errors instead.
///
/// # Errors
///
/// Returns [`IggyError::InvalidConfiguration`] if an origin or header value is
/// not a valid header token, a method is not one of the standard HTTP verbs,
/// `*` appears past the first origin, or `allow_credentials` is combined with
/// a wildcard origin, header, or exposed-header list.
fn configure_cors(config: &HttpCorsConfig) -> Result<CorsLayer, IggyError> {
    let wildcard_origin = config
        .allowed_origins
        .first()
        .is_some_and(|origin| origin.trim() == "*");
    let allowed_origins = match config.allowed_origins.as_slice() {
        [] => AllowOrigin::default(),
        _ if wildcard_origin => AllowOrigin::any(),
        // `AllowOrigin::list` panics on a wildcard entry, so past the first
        // position `*` is a config mistake, not "any origin".
        origins if origins.iter().any(|origin| origin.trim() == "*") => {
            error!("invalid CORS allowed_origins: \"*\" is honored only as the first entry");
            return Err(IggyError::InvalidConfiguration);
        }
        origins => AllowOrigin::list(parse_cors_values::<HeaderValue>(origins, "origin")?),
    };
    let allowed_headers = parse_cors_values::<HeaderName>(&config.allowed_headers, "header")?;
    let exposed_headers =
        parse_cors_values::<HeaderName>(&config.exposed_headers, "exposed header")?;
    let allowed_methods = parse_cors_methods(&config.allowed_methods)?;

    // tower-http rejects credentials combined with any wildcard by panicking
    // once the layer is applied to the router; catch those combinations here
    // so they fail as configuration errors instead.
    if config.allow_credentials {
        let wildcard_field = if wildcard_origin {
            Some("allowed_origins")
        } else if is_wildcard_header_list(&allowed_headers) {
            Some("allowed_headers")
        } else if is_wildcard_header_list(&exposed_headers) {
            Some("exposed_headers")
        } else {
            None
        };
        if let Some(field) = wildcard_field {
            error!(
                "invalid CORS config: allow_credentials cannot be combined with wildcard {field}"
            );
            return Err(IggyError::InvalidConfiguration);
        }
    }

    Ok(CorsLayer::new()
        .allow_methods(allowed_methods)
        .allow_origin(allowed_origins)
        .allow_headers(allowed_headers)
        .expose_headers(exposed_headers)
        .allow_credentials(config.allow_credentials)
        .allow_private_network(config.allow_private_network))
}

/// Parse a CORS string list into header values or names, trimming entries and
/// skipping blank ones so a placeholder like `[""]` yields an empty set.
/// `label` names the field in the diagnostic log for an unparsable entry.
fn parse_cors_values<T>(values: &[String], label: &str) -> Result<Vec<T>, IggyError>
where
    T: FromStr,
    T::Err: Display,
{
    values
        .iter()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|value| {
            value.parse::<T>().map_err(|error| {
                error!(%value, %error, "invalid CORS {label}");
                IggyError::InvalidConfiguration
            })
        })
        .collect()
}

/// Map the configured method names onto the standard HTTP verbs, trimming
/// entries and skipping blank ones. A name outside the standard set is rejected
/// (rather than accepted as a custom method token) so a typo fails the config
/// loudly.
fn parse_cors_methods(methods: &[String]) -> Result<Vec<Method>, IggyError> {
    methods
        .iter()
        .map(|method| method.trim())
        .filter(|method| !method.is_empty())
        .map(|method| match method.to_uppercase().as_str() {
            "GET" => Ok(Method::GET),
            "POST" => Ok(Method::POST),
            "PUT" => Ok(Method::PUT),
            "DELETE" => Ok(Method::DELETE),
            "HEAD" => Ok(Method::HEAD),
            "OPTIONS" => Ok(Method::OPTIONS),
            "CONNECT" => Ok(Method::CONNECT),
            "PATCH" => Ok(Method::PATCH),
            "TRACE" => Ok(Method::TRACE),
            other => {
                error!(method = %other, "invalid CORS method");
                Err(IggyError::InvalidConfiguration)
            }
        })
        .collect()
}

/// The exact shape tower-http treats as a wildcard header set: a lone `*`.
/// (`["*", "x"]` joins to the literal value `*,x`, which is not a wildcard.)
fn is_wildcard_header_list(headers: &[HeaderName]) -> bool {
    matches!(headers, [only] if only.as_str() == "*")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cors_config() -> HttpCorsConfig {
        HttpCorsConfig {
            enabled: true,
            allowed_methods: vec!["GET".to_owned(), "POST".to_owned()],
            allowed_origins: vec!["*".to_owned()],
            allowed_headers: vec!["content-type".to_owned(), "authorization".to_owned()],
            exposed_headers: vec!["x-iggy-view".to_owned()],
            allow_credentials: false,
            allow_private_network: false,
        }
    }

    #[test]
    fn configure_cors_accepts_wildcard_origin() {
        assert!(configure_cors(&cors_config()).is_ok());
    }

    #[test]
    fn configure_cors_accepts_explicit_origins() {
        let config = HttpCorsConfig {
            allowed_origins: vec![
                "http://localhost:3000".to_owned(),
                "https://app.example.com".to_owned(),
            ],
            ..cors_config()
        };
        assert!(configure_cors(&config).is_ok());
    }

    #[test]
    fn configure_cors_accepts_credentials_with_explicit_origin() {
        let config = HttpCorsConfig {
            allowed_origins: vec!["https://app.example.com".to_owned()],
            allow_credentials: true,
            ..cors_config()
        };
        assert!(configure_cors(&config).is_ok());
    }

    #[test]
    fn configure_cors_skips_blank_entries() {
        // The shipped placeholder shape: a single empty string must map to an
        // empty set, not a parse error.
        let config = HttpCorsConfig {
            allowed_origins: vec!["https://app.example.com".to_owned()],
            exposed_headers: vec![String::new()],
            ..cors_config()
        };
        assert!(configure_cors(&config).is_ok());
    }

    #[test]
    fn configure_cors_accepts_wildcard_headers_without_credentials() {
        let config = HttpCorsConfig {
            allowed_headers: vec!["*".to_owned()],
            exposed_headers: vec!["*".to_owned()],
            ..cors_config()
        };
        assert!(configure_cors(&config).is_ok());
    }

    #[test]
    fn configure_cors_trims_entries() {
        let config = HttpCorsConfig {
            allowed_origins: vec![" https://app.example.com ".to_owned()],
            allowed_headers: vec![" content-type ".to_owned()],
            allowed_methods: vec![" get ".to_owned()],
            ..cors_config()
        };
        assert!(configure_cors(&config).is_ok());
    }

    #[test]
    fn configure_cors_rejects_wildcard_origin_after_first() {
        // tower-http's `AllowOrigin::list` panics on a wildcard entry; the
        // guard must turn it into a clean config error.
        let config = HttpCorsConfig {
            allowed_origins: vec!["https://app.example.com".to_owned(), "*".to_owned()],
            ..cors_config()
        };
        assert!(matches!(
            configure_cors(&config),
            Err(IggyError::InvalidConfiguration)
        ));
    }

    #[test]
    fn configure_cors_rejects_credentials_with_wildcard_origin() {
        // tower-http's `ensure_usable_cors_rules` panics on this combination
        // when the layer is applied; the guard must turn it into a clean
        // config error.
        let config = HttpCorsConfig {
            allow_credentials: true,
            ..cors_config()
        };
        assert!(matches!(
            configure_cors(&config),
            Err(IggyError::InvalidConfiguration)
        ));
    }

    #[test]
    fn configure_cors_rejects_credentials_with_wildcard_headers() {
        let config = HttpCorsConfig {
            allowed_origins: vec!["https://app.example.com".to_owned()],
            allowed_headers: vec!["*".to_owned()],
            allow_credentials: true,
            ..cors_config()
        };
        assert!(matches!(
            configure_cors(&config),
            Err(IggyError::InvalidConfiguration)
        ));
    }

    #[test]
    fn configure_cors_rejects_credentials_with_wildcard_exposed_headers() {
        let config = HttpCorsConfig {
            allowed_origins: vec!["https://app.example.com".to_owned()],
            exposed_headers: vec!["*".to_owned()],
            allow_credentials: true,
            ..cors_config()
        };
        assert!(matches!(
            configure_cors(&config),
            Err(IggyError::InvalidConfiguration)
        ));
    }

    #[test]
    fn configure_cors_rejects_invalid_origin() {
        let config = HttpCorsConfig {
            allowed_origins: vec!["http://bad\norigin".to_owned()],
            ..cors_config()
        };
        assert!(matches!(
            configure_cors(&config),
            Err(IggyError::InvalidConfiguration)
        ));
    }

    #[test]
    fn configure_cors_rejects_invalid_header() {
        let config = HttpCorsConfig {
            allowed_headers: vec!["invalid header".to_owned()],
            ..cors_config()
        };
        assert!(matches!(
            configure_cors(&config),
            Err(IggyError::InvalidConfiguration)
        ));
    }

    #[test]
    fn configure_cors_rejects_unknown_method() {
        let config = HttpCorsConfig {
            allowed_methods: vec!["FOOBAR".to_owned()],
            ..cors_config()
        };
        assert!(matches!(
            configure_cors(&config),
            Err(IggyError::InvalidConfiguration)
        ));
    }
}
