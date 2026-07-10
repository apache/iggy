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
mod wire;

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use axum::Router;
use axum::extract::{DefaultBodyLimit, Request};
use axum::middleware::{Next, from_fn};
use axum::routing::{delete, get, post, put};
use configs::cluster::{ClusterConfig, TransportPorts};
use configs::http::HttpConfig;
use configs::server_ng::NgSystemConfig;
use message_bus::client_listener;
use send_wrapper::SendWrapper;
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
/// background task on shard 0's compio runtime.
///
/// The caller gates this to shard 0 and to `http.enabled`; the listener stops
/// when the bus shutdown token fires.
///
/// # Errors
///
/// Returns [`ServerNgError`] if the JWT manager cannot be built from
/// `http_config.jwt` or the listener cannot bind to `addr`.
pub async fn start(
    shard: &Rc<ServerNgShard>,
    addr: SocketAddr,
    http_config: &HttpConfig,
    cluster: &ClusterConfig,
    system_config: Arc<NgSystemConfig>,
    self_ports: TransportPorts,
) -> Result<(), ServerNgError> {
    let jwt = JwtManager::build(&http_config.jwt)?;
    let (listener, bound_addr) = client_listener::tcp::bind(addr).await?;
    info!(address = %bound_addr, "server-ng HTTP listener started");

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
        in_flight_writes: Cell::new(0),
    }));
    // Saturating: a configured limit past the pointer width (32-bit target,
    // >4 GiB value) clamps to the largest enforceable cap instead of wrapping.
    let max_request_size =
        usize::try_from(http_config.max_request_size.as_bytes_u64()).unwrap_or(usize::MAX);
    let router = router(state, max_request_size);

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
fn router(state: HttpState, max_request_size: usize) -> Router {
    // Cloned for the response layer so `X-Iggy-View` reads the live view per
    // response; the original `state` is moved into `with_state` below.
    let view_source = state.clone();
    Router::new()
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
        }))
}
