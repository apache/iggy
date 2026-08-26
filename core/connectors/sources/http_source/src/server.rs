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

//! The listener every instance of this plugin shares, and the request path.
//!
//! One `.so` is loaded once no matter how many `[[source]]` entries reference
//! it, so the listeners live in a process-global registry keyed by listen
//! address rather than on any one instance. The first `open()` binds; later
//! ones validate their config against the running server and join; the last
//! `close()` releases the ports, which the runtime's stop-then-start restart
//! flow depends on.

use arc_swap::ArcSwap;
use axum::Router;
use axum::body::Bytes;
use axum::extract::rejection::BytesRejection;
use axum::extract::{ConnectInfo, DefaultBodyLimit, Path, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, serve};
use iggy_common::{HeaderKey, HeaderValue};
use iggy_connector_sdk::Error;
use secrecy::SecretString;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use crate::auth::{secrets_match, validate_bearer, validate_hmac};
use crate::metrics::{Metrics, PathKind, UNROUTED};
use crate::routes::{Endpoint, EndpointOrigin, RouteLookup, RouteTable};
use crate::types::{QueuedMessage, clamp_header_value, unix_now_seconds};
use crate::{CONNECTOR_NAME, EndpointAuthType, HttpSourceConfig, SharedState, management};

/// Iggy header carrying the instance an accepted request was routed to.
pub const INSTANCE_HEADER: &str = "iggy_source_instance";
/// Iggy header carrying the peer address the request arrived from.
pub const REMOTE_ADDR_HEADER: &str = "iggy_http_remote_addr";
/// Iggy header carrying accept time, in microseconds since the Unix epoch.
pub const RECEIVED_AT_HEADER: &str = "iggy_http_received_at";

/// How long the last `close()` waits for in-flight requests before abandoning
/// the listener tasks. Bounded so a wedged connection cannot stall shutdown.
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Registers an instance with the listener for its configured address,
/// binding that listener if this is the first instance to ask for it.
pub async fn join(instance: Arc<SharedState>) -> Result<(), Error> {
    let listen_addr = instance.config.listen_addr.clone();
    let mut servers = SERVERS.lock().await;

    if let Some(server) = servers.get_mut(&listen_addr) {
        if server.draining {
            return Err(Error::InitError(format!(
                "The {CONNECTOR_NAME} listener on {listen_addr} is shutting down; retry the open once its port is released"
            )));
        }
        server.ensure_compatible(&instance.config)?;
        let mut instances = server.state.instances();
        // Names address instances in the management API and label every
        // metric series, so a duplicate would silently route registrations to
        // whichever instance happens to sit first in the list. Both are scoped
        // to this listener, which is why the check is too.
        if instances
            .iter()
            .any(|joined| joined.instance_name == instance.instance_name)
        {
            return Err(Error::InvalidConfigValue(format!(
                "instance_name '{}' is already used by another instance on {listen_addr}",
                instance.instance_name
            )));
        }
        instances.push(Arc::clone(&instance));
        server.state.publish(instances)?;
        info!(
            "Joined {CONNECTOR_NAME} connector ID: {} to the listener on {listen_addr}",
            instance.id
        );
        return Ok(());
    }

    // Build the routes before binding: a config that cannot produce a valid
    // table must not leave a port bound behind it.
    let state = Arc::new(ServerState::new(&instance.config));
    state.publish(vec![Arc::clone(&instance)])?;
    let server = SharedServer::start(&instance.config, state).await?;
    servers.insert(listen_addr.clone(), server);
    info!(
        "Bound the {CONNECTOR_NAME} listener on {listen_addr} for connector ID: {}, admin listener on {}",
        instance.id, instance.config.admin_listen_addr
    );
    Ok(())
}

/// Deregisters an instance's routes, and shuts the listeners down once the
/// last instance has left.
/// Deregisters an instance, folding `staged_dropped` into the same shutdown
/// loss metric as whatever is still queued in the bridge.
pub async fn leave(instance: &Arc<SharedState>, staged_dropped: u64) {
    let listen_addr = &instance.config.listen_addr;
    let mut servers = SERVERS.lock().await;
    let Some(server) = servers.get_mut(listen_addr) else {
        return;
    };

    let joined = server.state.instances();
    if !joined.iter().any(|candidate| candidate.id == instance.id) {
        // `SourceContainer::open` keeps the source even when `open()` failed,
        // and stop closes unconditionally, so this runs for instances that
        // never joined. Tearing down on their behalf would rebuild the route
        // table for nothing, log a deregistration that never happened, and,
        // when the failure was the duplicate-name rejection, wipe the metrics
        // of the sibling that legitimately owns that name.
        return;
    }

    let remaining: Vec<Arc<SharedState>> = joined
        .into_iter()
        .filter(|candidate| candidate.id != instance.id)
        .collect();
    let remaining_count = remaining.len();
    if let Err(error) = server.state.publish(remaining) {
        // Dropping an instance cannot introduce a collision, so this is
        // unreachable; serve nothing rather than stale routes if it happens.
        error!(
            "Failed to rebuild {CONNECTOR_NAME} routes after connector ID: {} left. {error}",
            instance.id
        );
        server.state.serve_nothing();
    }
    info!(
        "Deregistered {CONNECTOR_NAME} routes for connector ID: {}, instances left on {listen_addr}: {remaining_count}",
        instance.id
    );

    server
        .state
        .metrics
        .forget_instance(&instance.instance_name);

    // Counted after the routes are gone, which narrows the window but does
    // not close it: a handler that already loaded the old table can still
    // enqueue after this read. The SDK stops the poll task before close(), so
    // whatever is queued here is unreachable either way.
    let dropped = instance.sender.len() as u64 + staged_dropped;
    if dropped > 0 {
        warn!(
            "Dropped {dropped} queued messages closing {CONNECTOR_NAME} connector ID: {}",
            instance.id
        );
        server
            .state
            .metrics
            .record_dropped_on_close(&instance.instance_name, dropped);
    }

    if remaining_count == 0 {
        // Mark the entry draining and take the tasks, but leave the entry in
        // place while releasing the guard. Holding it across shutdown would
        // deadlock against an in-flight management request parked on this
        // lock; removing it outright would let a concurrent `join()` bind a
        // port the listener is still draining. The tombstone does neither.
        server.draining = true;
        let signal = server.shutdown.clone();
        let tasks = std::mem::take(&mut server.tasks);
        drop(servers);

        SharedServer::stop(signal, tasks, listen_addr).await;
        SERVERS.lock().await.remove(listen_addr);
    }
}

/// Every listener this process has bound, keyed by public listen address.
///
/// An async mutex because the guard is held across the bind and across the
/// graceful shutdown await. Both are open/close operations, never requests.
static SERVERS: LazyLock<Mutex<HashMap<String, SharedServer>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

struct SharedServer {
    admin_listen_addr: String,
    max_body_size_bytes: usize,
    state: Arc<ServerState>,
    shutdown: watch::Sender<()>,
    tasks: Vec<JoinHandle<()>>,
    /// Set while the listener is draining. The entry stays in the registry so
    /// a concurrent `join()` cannot bind a port that is still held, but it can
    /// no longer be joined.
    draining: bool,
}

/// The part of a shared server the request handlers see.
#[derive(Debug)]
pub(crate) struct ServerState {
    pub(crate) listen_addr: String,
    /// Taken from the first instance to bind. Every instance joining the same
    /// listener must present the same token, so this is unambiguous.
    pub(crate) management_token: Option<SecretString>,
    pub(crate) metrics: Metrics,
    routes: ArcSwap<RouteTable>,
    instances: ArcSwap<Vec<Arc<SharedState>>>,
    started_at: Instant,
}

impl ServerState {
    pub(crate) fn new(config: &HttpSourceConfig) -> Self {
        ServerState {
            listen_addr: config.listen_addr.clone(),
            management_token: config.management_token.clone(),
            metrics: Metrics::new(),
            routes: ArcSwap::from_pointee(RouteTable::default()),
            instances: ArcSwap::from_pointee(Vec::new()),
            started_at: Instant::now(),
        }
    }

    pub(crate) fn instances(&self) -> Vec<Arc<SharedState>> {
        (**self.instances.load()).clone()
    }

    pub(crate) fn instance(&self, instance_name: &str) -> Option<Arc<SharedState>> {
        self.instances
            .load()
            .iter()
            .find(|instance| instance.instance_name == instance_name)
            .map(Arc::clone)
    }

    /// Serves nothing until the next successful publish. Used when a mutation
    /// removes access but the table cannot be rebuilt: stale routes would keep
    /// honouring a credential the operator believes is gone.
    pub(crate) fn serve_nothing(&self) {
        let dropped = self.routes.load();
        error!(
            "Serving no {CONNECTOR_NAME} routes on {}: dropped {} secret paths and {} named paths across {} instances until the next successful publish",
            self.listen_addr,
            dropped.secret_path_count(),
            dropped.named_path_count(),
            self.instances.load().len()
        );
        self.routes.store(Arc::new(RouteTable::default()));
    }

    /// Swaps in a new instance set and the routes it projects to, or leaves
    /// both untouched if the instances collide on a path.
    fn publish(&self, instances: Vec<Arc<SharedState>>) -> Result<(), Error> {
        let table = RouteTable::build(&instances)
            .map_err(|conflict| Error::InvalidConfigValue(conflict.to_string()))?;
        self.instances.store(Arc::new(instances));
        self.routes.store(Arc::new(table));
        Ok(())
    }
}

/// Reprojects a listener's route table after a management mutation.
///
/// Takes `SERVERS` - the listener map, not the per-instance registry gate,
/// which the caller has already released. A mutation and a join can therefore
/// still interleave; that is safe because the rebuild reprojects from whatever
/// instance set is current rather than patching the previous table.
///
/// Blocking on the registry here is safe because `leave()` releases it before
/// awaiting graceful shutdown; holding it across that await would deadlock,
/// since shutdown waits for the very request that is parked here.
pub(crate) async fn refresh_routes(listen_addr: &str) -> Result<(), Error> {
    let servers = SERVERS.lock().await;
    let Some(server) = servers.get(listen_addr) else {
        return Err(Error::InitError(format!(
            "No {CONNECTOR_NAME} listener is bound to {listen_addr}"
        )));
    };
    server.state.publish(server.state.instances())
}

impl SharedServer {
    async fn start(config: &HttpSourceConfig, state: Arc<ServerState>) -> Result<Self, Error> {
        let public = bind(&config.listen_addr).await?;
        let admin = bind(&config.admin_listen_addr).await?;
        let (shutdown, _) = watch::channel(());

        // The one place this plugin owns background tasks. The runtime cannot
        // drive an HTTP listener for us, so the last close() shuts them down
        // explicitly rather than leaving them to outlive the connector.
        let tasks = vec![
            tokio::spawn(run(
                public,
                public_router(Arc::clone(&state), config.max_body_size_bytes),
                shutdown.subscribe(),
                "public",
            )),
            tokio::spawn(run(
                admin,
                admin_router(state.clone(), config.max_body_size_bytes),
                shutdown.subscribe(),
                "admin",
            )),
        ];

        Ok(SharedServer {
            admin_listen_addr: config.admin_listen_addr.clone(),
            max_body_size_bytes: config.max_body_size_bytes,
            state,
            shutdown,
            tasks,
            draining: false,
        })
    }

    /// Refuses a join whose settings disagree with the running listener.
    ///
    /// Fails closed on purpose: first-instance-wins would leave an operator
    /// with a body limit or admin address their TOML says they do not have.
    fn ensure_compatible(&self, config: &HttpSourceConfig) -> Result<(), Error> {
        if self.admin_listen_addr != config.admin_listen_addr {
            return Err(Error::InvalidConfigValue(format!(
                "admin_listen_addr '{}' does not match '{}' on the listener already bound to {}",
                config.admin_listen_addr, self.admin_listen_addr, config.listen_addr
            )));
        }
        if self.max_body_size_bytes != config.max_body_size_bytes {
            return Err(Error::InvalidConfigValue(format!(
                "max_body_size_bytes {} does not match {} on the listener already bound to {}",
                config.max_body_size_bytes, self.max_body_size_bytes, config.listen_addr
            )));
        }
        // The management API guards one shared listener, so instances cannot
        // hold different opinions about who may call it.
        if !same_token(&self.state.management_token, &config.management_token) {
            return Err(Error::InvalidConfigValue(format!(
                "management_token does not match the one on the listener already bound to {}",
                config.listen_addr
            )));
        }
        Ok(())
    }

    async fn stop(signal: watch::Sender<()>, mut tasks: Vec<JoinHandle<()>>, listen_addr: &str) {
        let _ = signal.send(());

        // One deadline over both listeners: applied per task in a loop, the
        // real bound would be twice SHUTDOWN_TIMEOUT.
        let graceful = tokio::time::timeout(SHUTDOWN_TIMEOUT, async {
            for task in &mut tasks {
                if let Err(error) = task.await {
                    warn!("A {CONNECTOR_NAME} listener task on {listen_addr} failed. {error}");
                }
            }
        })
        .await;

        if graceful.is_err() {
            warn!(
                "A {CONNECTOR_NAME} listener on {listen_addr} did not stop within {SHUTDOWN_TIMEOUT:?}, aborting"
            );
            // `abort()` only schedules cancellation. Awaiting the handle is
            // what guarantees the future - and the TcpListener inside it - has
            // actually been dropped, so the runtime's stop-then-start restart
            // cannot race the bind and fail with EADDRINUSE.
            for task in &mut tasks {
                // Skip the ones the graceful loop already drove to completion:
                // awaiting a finished `JoinHandle` a second time panics, and
                // this runs inside the FFI close of a dlopened plugin.
                if task.is_finished() {
                    continue;
                }
                task.abort();
                match task.await {
                    Err(error) if error.is_cancelled() => {
                        warn!("Aborted a wedged {CONNECTOR_NAME} listener task on {listen_addr}")
                    }
                    Err(error) => {
                        warn!("A {CONNECTOR_NAME} listener task on {listen_addr} panicked. {error}")
                    }
                    Ok(()) => {}
                }
            }
        }
        info!("Released the {CONNECTOR_NAME} listener on {listen_addr}");
    }
}

async fn bind(address: &str) -> Result<TcpListener, Error> {
    TcpListener::bind(address).await.map_err(|error| {
        // Instances are grouped by the `listen_addr` string, so two spellings
        // of the same socket, `0.0.0.0:9090` and `127.0.0.1:9090`, are two
        // groups and the second one tries to bind a port the first already
        // holds. The operator sees a bind failure for a field they believe is
        // shared, so name that possibility rather than only the OS error.
        let hint = if error.kind() == std::io::ErrorKind::AddrInUse {
            format!(
                ". If another {CONNECTOR_NAME} instance is meant to share this listener, its listen_addr must match this one exactly"
            )
        } else {
            String::new()
        };
        Error::InitError(format!(
            "Failed to bind the {CONNECTOR_NAME} listener to {address}. {error}{hint}"
        ))
    })
}

async fn run(
    listener: TcpListener,
    router: Router,
    mut shutdown: watch::Receiver<()>,
    label: &str,
) {
    let service = router.into_make_service_with_connect_info::<SocketAddr>();
    let result = serve(listener, service)
        .with_graceful_shutdown(async move {
            let _ = shutdown.changed().await;
        })
        .await;
    if let Err(error) = result {
        error!("The {CONNECTOR_NAME} {label} listener stopped. {error}");
    }
}

fn public_router(state: Arc<ServerState>, max_body_size_bytes: usize) -> Router {
    Router::new()
        .route("/topics/{topic_path}", post(handle_named_path))
        .route("/e/{endpoint_id}", post(handle_secret_path))
        .route("/health", get(handle_health))
        .layer(DefaultBodyLimit::max(max_body_size_bytes))
        .with_state(state)
}

fn admin_router(state: Arc<ServerState>, max_body_size_bytes: usize) -> Router {
    Router::new()
        .route("/admin/health", get(handle_admin_health))
        .route("/admin/metrics", get(handle_admin_metrics))
        .merge(management::router(Arc::clone(&state)))
        // Without this the admin listener silently used axum's 2 MiB default
        // instead of the operator's cap, so the two listeners disagreed about
        // how large a body they would read.
        .layer(DefaultBodyLimit::max(max_body_size_bytes))
        .with_state(state)
}

fn same_token(left: &Option<SecretString>, right: &Option<SecretString>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => secrets_match(left, right),
        _ => false,
    }
}

async fn handle_named_path(
    State(state): State<Arc<ServerState>>,
    Path(topic_path): Path<String>,
    ConnectInfo(remote_addr): ConnectInfo<SocketAddr>,
    request_headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let started = Instant::now();
    let (instance_name, response) =
        named_path_outcome(&state, &topic_path, remote_addr, &request_headers, body);
    state.metrics.record_request(
        &instance_name,
        PathKind::Named,
        response.status().as_u16(),
        started.elapsed(),
    );
    response
}

async fn handle_secret_path(
    State(state): State<Arc<ServerState>>,
    Path(endpoint_id): Path<String>,
    ConnectInfo(remote_addr): ConnectInfo<SocketAddr>,
    request_headers: HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> Response {
    let started = Instant::now();
    let (instance_name, response) =
        secret_path_outcome(&state, &endpoint_id, remote_addr, &request_headers, body);
    state.metrics.record_request(
        &instance_name,
        PathKind::Secret,
        response.status().as_u16(),
        started.elapsed(),
    );
    response
}

/// Returns the instance the request resolved to alongside the response, so
/// the caller can label the metrics. Requests that never resolved are still
/// counted, under [`UNROUTED`].
fn named_path_outcome(
    state: &ServerState,
    topic_path: &str,
    remote_addr: SocketAddr,
    request_headers: &HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> (String, Response) {
    let routes = state.routes.load();
    let Some(instance) = routes.lookup_named_path(topic_path) else {
        return (
            UNROUTED.to_owned(),
            error_response(StatusCode::NOT_FOUND, "not found"),
        );
    };
    let name = instance.instance_name.clone();
    let body = match body {
        Ok(body) => body,
        Err(rejection) => return (name, rejected_body_response(rejection)),
    };
    if let Some(expected) = &instance.config.auth_bearer_token
        && !validate_bearer(bearer_header(request_headers), expected)
    {
        return (
            name,
            error_response(StatusCode::UNAUTHORIZED, "unauthorized"),
        );
    }
    let response = enqueue(instance, request_headers, remote_addr, body, &state.metrics);
    (name, response)
}

fn secret_path_outcome(
    state: &ServerState,
    endpoint_id: &str,
    remote_addr: SocketAddr,
    request_headers: &HeaderMap,
    body: Result<Bytes, BytesRejection>,
) -> (String, Response) {
    let routes = state.routes.load();
    let entry = match routes.lookup_secret_path(endpoint_id, unix_now_seconds()) {
        RouteLookup::Active(entry) => entry,
        // Revoked endpoints answer as if they never existed, so a leaked URL
        // cannot be used to confirm it was once live. The metric still names
        // the owning instance: only a genuinely unknown path is `unrouted`.
        RouteLookup::Revoked(entry) => {
            return (
                entry.instance.instance_name.clone(),
                error_response(StatusCode::NOT_FOUND, "not found"),
            );
        }
        // Expired answers 404 for the same reason revoked does. A 410 is
        // returned before any credential is checked, so it told anyone holding
        // a leaked or guessed id that the endpoint had once been real.
        RouteLookup::Expired(entry) => {
            return (
                entry.instance.instance_name.clone(),
                error_response(StatusCode::NOT_FOUND, "not found"),
            );
        }
        RouteLookup::Unknown => {
            return (
                UNROUTED.to_owned(),
                error_response(StatusCode::NOT_FOUND, "not found"),
            );
        }
    };
    let name = entry.instance.instance_name.clone();
    let body = match body {
        Ok(body) => body,
        Err(rejection) => return (name, rejected_body_response(rejection)),
    };
    if !authorize(&entry.endpoint, request_headers, &body) {
        return (
            name,
            error_response(StatusCode::UNAUTHORIZED, "unauthorized"),
        );
    }
    let response = enqueue(
        &entry.instance,
        request_headers,
        remote_addr,
        body,
        &state.metrics,
    );
    (name, response)
}

/// Prometheus text format. Unguarded like `/admin/health`: the admin
/// listener defaults to loopback and scrapers do not carry bearer tokens.
async fn handle_admin_metrics(State(state): State<Arc<ServerState>>) -> String {
    state.metrics.encode(&state.instances())
}

/// Readiness for a load balancer: unavailable until an instance is serving.
///
/// A joined instance is not the same as a polling one. The SDK stops the poll
/// task after five consecutive NACKs without calling `close()`, so the instance
/// stays joined and this kept answering 200 to the load balancer while handlers
/// accepted into a bridge nobody drained, until it filled and 429d forever.
/// Readiness needs all three: an instance, a route it can reach, and a poll
/// task still running behind it.
async fn handle_health(State(state): State<Arc<ServerState>>) -> Response {
    let now = unix_now_seconds();
    let ready = !state.routes.load().is_empty()
        && state
            .instances
            .load()
            .iter()
            .any(|instance| instance.poll_is_live(now));
    if !ready {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(StatusResponse {
                status: "unavailable",
            }),
        )
            .into_response();
    }
    Json(StatusResponse { status: "ok" }).into_response()
}

async fn handle_admin_health(State(state): State<Arc<ServerState>>) -> Response {
    let instances = state
        .instances
        .load()
        .iter()
        .map(|instance| {
            let registry = instance.registry();
            let now = unix_now_seconds();
            InstanceHealth {
                instance: instance.instance_name.clone(),
                topic_path: instance.config.topic_path.clone(),
                buffer_used: instance.sender.len(),
                buffer_capacity: instance.config.buffer_capacity,
                endpoints_static: registry.serving_count_by_origin(EndpointOrigin::Static, now),
                endpoints_dynamic: registry.serving_count_by_origin(EndpointOrigin::Dynamic, now),
                endpoints_expired: registry.expired_count(now),
                endpoints_revoked: registry.revoked_count(),
                named_path: instance.config.topic_path.is_some(),
                state_submitted: registry.all_submitted(),
                dropped_headers: state.metrics.headers_dropped(&instance.instance_name),
                clamped_headers: state.metrics.headers_clamped(&instance.instance_name),
            }
        })
        .collect();

    Json(AdminHealth {
        status: "ok",
        instances,
        uptime_secs: state.started_at.elapsed().as_secs(),
    })
    .into_response()
}

/// Hands an accepted request to its instance's bridge, or rejects it.
///
/// Never blocks on a full bridge: waiting would turn a slow Iggy into a pile
/// of held-open connections and, once the sender times out, a retry storm.
fn enqueue(
    instance: &Arc<SharedState>,
    request_headers: &HeaderMap,
    remote_addr: SocketAddr,
    body: Bytes,
    metrics: &Metrics,
) -> Response {
    let (headers, clamped, dropped) = message_headers(instance, request_headers, remote_addr);
    let message = QueuedMessage {
        payload: body.to_vec(),
        headers,
        received_at: Instant::now(),
    };
    if instance.sender.try_send(message).is_err() {
        metrics.record_rejected_full(&instance.instance_name);
        debug!(
            "Rejected a request for {CONNECTOR_NAME} connector ID: {}, bridge is full at {} messages",
            instance.id, instance.config.buffer_capacity
        );
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [(header::RETRY_AFTER, "1")],
            Json(ErrorResponse {
                error: "service temporarily unavailable",
            }),
        )
            .into_response();
    }
    // Only now: a rejected request produced no message, so its header losses
    // would otherwise be counted against messages that never existed.
    metrics.record_headers(&instance.instance_name, clamped, dropped);
    Json(StatusResponse { status: "queued" }).into_response()
}

fn authorize(endpoint: &Endpoint, request_headers: &HeaderMap, body: &[u8]) -> bool {
    match endpoint.auth_type {
        EndpointAuthType::None => true,
        EndpointAuthType::Bearer => endpoint
            .auth_secret
            .as_ref()
            .is_some_and(|secret| validate_bearer(bearer_header(request_headers), secret)),
        EndpointAuthType::HmacSha256 | EndpointAuthType::HmacSha1 => {
            let (Some(secret), Some(algorithm)) = (
                endpoint.auth_secret.as_ref(),
                endpoint.auth_type.hmac_algorithm(),
            ) else {
                return false;
            };
            validate_hmac(
                body,
                header_str(request_headers, &endpoint.hmac_header),
                &endpoint.hmac_prefix,
                secret,
                algorithm,
            )
        }
    }
}

/// Builds the Iggy headers an accepted request rides with.
///
/// Values Iggy would reject are dropped rather than failing the message: a
/// webhook body is worth more than a `User-Agent`.
///
/// Returns the headers to attach plus the number of forwarded values that were
/// truncated and dropped. The counts are returned rather than recorded here so
/// a request that is ultimately rejected does not report losses for a message
/// that was never produced.
fn message_headers(
    instance: &Arc<SharedState>,
    request_headers: &HeaderMap,
    remote_addr: SocketAddr,
) -> (Option<BTreeMap<HeaderKey, HeaderValue>>, u64, u64) {
    let mut headers = BTreeMap::new();
    let mut dropped = 0;
    let mut clamped = 0;
    if instance.config.include_http_metadata {
        insert_header(&mut headers, INSTANCE_HEADER, &instance.instance_name);
        insert_header(
            &mut headers,
            REMOTE_ADDR_HEADER,
            &remote_addr.ip().to_string(),
        );
        insert_header(&mut headers, RECEIVED_AT_HEADER, &received_at_micros());
    }

    for (name, key) in &instance.forward_headers {
        let Some(present) = request_headers.get(name) else {
            continue;
        };
        // Present but unrepresentable is a loss; absent is not. `to_str`
        // rejects any byte outside visible ASCII, which a UTF-8 User-Agent
        // routinely carries.
        let Ok(raw) = present.to_str() else {
            dropped += 1;
            continue;
        };
        let Some(value) = clamp_header_value(raw) else {
            dropped += 1;
            continue;
        };
        if value.len() < raw.len() {
            clamped += 1;
        }
        match HeaderValue::from_str(value) {
            Ok(value) => {
                headers.insert(key.clone(), value);
            }
            Err(_) => dropped += 1,
        }
    }
    ((!headers.is_empty()).then_some(headers), clamped, dropped)
}

fn insert_header(headers: &mut BTreeMap<HeaderKey, HeaderValue>, key: &str, value: &str) {
    let (Ok(key), Ok(value)) = (HeaderKey::try_from(key), HeaderValue::from_str(value)) else {
        return;
    };
    headers.insert(key, value);
}

fn received_at_micros() -> String {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_micros())
        .unwrap_or_default()
        .to_string()
}

fn bearer_header(request_headers: &HeaderMap) -> Option<&str> {
    request_headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
}

fn header_str<'a>(request_headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    request_headers
        .get(name)
        .and_then(|value| value.to_str().ok())
}

fn rejected_body_response(rejection: BytesRejection) -> Response {
    let status = rejection.status();
    let message = if status == StatusCode::PAYLOAD_TOO_LARGE {
        "payload too large"
    } else {
        "bad request"
    };
    error_response(status, message)
}

pub(crate) fn error_response(status: StatusCode, error: &'static str) -> Response {
    (status, Json(ErrorResponse { error })).into_response()
}

#[derive(Serialize)]
struct StatusResponse {
    status: &'static str,
}

#[derive(Serialize)]
struct ErrorResponse {
    error: &'static str,
}

#[derive(Serialize)]
struct AdminHealth {
    status: &'static str,
    instances: Vec<InstanceHealth>,
    uptime_secs: u64,
}

#[derive(Serialize)]
struct InstanceHealth {
    instance: String,
    topic_path: Option<String>,
    buffer_used: usize,
    buffer_capacity: usize,
    endpoints_static: usize,
    endpoints_dynamic: usize,
    endpoints_expired: usize,
    endpoints_revoked: usize,
    named_path: bool,
    state_submitted: bool,
    dropped_headers: u64,
    clamped_headers: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::HttpSource;
    use crate::test_support::{ENDPOINT_ONE, ENDPOINT_TWO, client, free_port};
    use iggy_connector_sdk::Source;
    use ring::hmac;

    const STATIC_SECRET: &str = "whsec_static";

    fn config(public_port: u16, admin_port: u16, endpoints: &[&str]) -> HttpSourceConfig {
        let mut config = crate::test_support::config(Some("github"), endpoints);
        config.listen_addr = format!("127.0.0.1:{public_port}");
        config.admin_listen_addr = format!("127.0.0.1:{admin_port}");
        config
    }

    async fn open(id: u32, config: HttpSourceConfig) -> HttpSource {
        let mut source = HttpSource::new(id, config, None);
        source.open().await.expect("open must succeed");
        source
    }

    fn signature(body: &[u8]) -> String {
        let key = hmac::Key::new(hmac::HMAC_SHA256, STATIC_SECRET.as_bytes());
        format!("sha256={}", hex::encode(hmac::sign(&key, body).as_ref()))
    }

    async fn post_signed(base: &str, endpoint_id: &str, body: &'static str) -> reqwest::Response {
        client()
            .post(format!("{base}/e/{endpoint_id}"))
            .header(crate::DEFAULT_HMAC_HEADER, signature(body.as_bytes()))
            .body(body)
            .send()
            .await
            .expect("the request must reach the listener")
    }

    fn base_url(source: &HttpSource) -> String {
        format!("http://{}", source.shared.config.listen_addr)
    }

    #[tokio::test]
    async fn given_valid_signature_when_posted_to_secret_path_should_queue() {
        let mut source = open(1, config(free_port(), free_port(), &[ENDPOINT_ONE])).await;

        let response = post_signed(&base_url(&source), ENDPOINT_ONE, "{\"event\":\"push\"}").await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(source.shared.sender.len(), 1);
        close(&mut source).await;
    }

    #[tokio::test]
    async fn given_tampered_signature_when_posted_should_answer_unauthorized() {
        let mut source = open(1, config(free_port(), free_port(), &[ENDPOINT_ONE])).await;

        let response = client()
            .post(format!("{}/e/{ENDPOINT_ONE}", base_url(&source)))
            .header(crate::DEFAULT_HMAC_HEADER, signature(b"a different body"))
            .body("{\"event\":\"push\"}")
            .send()
            .await
            .expect("the request must reach the listener");

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(source.shared.sender.len(), 0);
        close(&mut source).await;
    }

    #[tokio::test]
    async fn given_unknown_endpoint_when_posted_should_answer_not_found() {
        let mut source = open(1, config(free_port(), free_port(), &[ENDPOINT_ONE])).await;

        let response = post_signed(&base_url(&source), ENDPOINT_TWO, "{}").await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        close(&mut source).await;
    }

    #[tokio::test]
    async fn given_revoked_endpoint_when_posted_should_answer_not_found() {
        let mut source = open(1, config(free_port(), free_port(), &[ENDPOINT_ONE])).await;
        source
            .shared
            .mutate_registry(|registry| registry.revoke(ENDPOINT_ONE, "compromised".to_string(), 1))
            .await;
        rebuild_routes(&source).await;

        let response = post_signed(&base_url(&source), ENDPOINT_ONE, "{}").await;

        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "a revoked endpoint must not be distinguishable from one that never existed"
        );
        close(&mut source).await;
    }

    #[tokio::test]
    async fn given_expired_endpoint_when_posted_should_answer_gone() {
        let mut source = open(1, config(free_port(), free_port(), &[ENDPOINT_ONE])).await;
        source
            .shared
            .mutate_registry(|registry| {
                registry
                    .endpoint_mut(ENDPOINT_ONE)
                    .expect("the static endpoint is registered")
                    .expires_at = Some(1);
            })
            .await;
        rebuild_routes(&source).await;

        let response = post_signed(&base_url(&source), ENDPOINT_ONE, "{}").await;

        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "expired must not be distinguishable from never-existed by an unauthenticated caller"
        );
        close(&mut source).await;
    }

    #[tokio::test]
    async fn given_oversized_body_when_posted_should_answer_payload_too_large() {
        let mut config = config(free_port(), free_port(), &[ENDPOINT_ONE]);
        config.max_body_size_bytes = 16;
        let mut source = open(1, config).await;

        let response = client()
            .post(format!("{}/e/{ENDPOINT_ONE}", base_url(&source)))
            .body("x".repeat(1024))
            .send()
            .await
            .expect("the request must reach the listener");

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        close(&mut source).await;
    }

    #[tokio::test]
    async fn given_full_bridge_when_posted_should_answer_too_many_requests() {
        let mut config = config(free_port(), free_port(), &[ENDPOINT_ONE]);
        config.buffer_capacity = 1;
        let mut source = open(1, config).await;
        let base = base_url(&source);

        assert_eq!(
            post_signed(&base, ENDPOINT_ONE, "{}").await.status(),
            StatusCode::OK
        );
        let response = post_signed(&base, ENDPOINT_ONE, "{}").await;

        assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(
            response
                .headers()
                .get(header::RETRY_AFTER)
                .and_then(|value| value.to_str().ok()),
            Some("1"),
            "a sender needs to be told when to come back, not just refused"
        );
        close(&mut source).await;
    }

    #[tokio::test]
    async fn given_bearer_token_when_named_path_posted_should_enforce_it() {
        let mut config = config(free_port(), free_port(), &[]);
        config.auth_bearer_token = Some(secrecy::SecretString::from("global-secret"));
        let mut source = open(1, config).await;
        let url = format!("{}/topics/github", base_url(&source));

        let unauthorized = client()
            .post(&url)
            .body("{}")
            .send()
            .await
            .expect("the request must reach the listener");
        let authorized = client()
            .post(&url)
            .header(header::AUTHORIZATION, "Bearer global-secret")
            .body("{}")
            .send()
            .await
            .expect("the request must reach the listener");

        assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(authorized.status(), StatusCode::OK);
        assert_eq!(source.shared.sender.len(), 1);
        close(&mut source).await;
    }

    #[tokio::test]
    async fn given_forwarded_headers_when_posted_should_ride_on_the_message() {
        let mut config = config(free_port(), free_port(), &[ENDPOINT_ONE]);
        config.forward_headers = vec!["x-github-delivery".to_string()];
        let mut source = open(1, config).await;

        client()
            .post(format!("{}/e/{ENDPOINT_ONE}", base_url(&source)))
            .header(crate::DEFAULT_HMAC_HEADER, signature(b"{}"))
            .header("x-github-delivery", "72d3162e-cc78-11e3-81ab-4c9367dc0958")
            .body("{}")
            .send()
            .await
            .expect("the request must reach the listener");

        let message = source
            .receiver
            .lock()
            .await
            .try_recv()
            .expect("the request must have been queued");
        let headers = message.headers.expect("metadata forwarding is on");
        assert_eq!(
            headers
                .get(&HeaderKey::try_from("x-github-delivery").expect("valid key"))
                .map(|value| value.as_str().expect("utf-8 value")),
            Some("72d3162e-cc78-11e3-81ab-4c9367dc0958")
        );
        assert!(
            headers.contains_key(&HeaderKey::try_from(INSTANCE_HEADER).expect("valid key")),
            "instance identity must ride along so a consumer can tell sources apart"
        );
        close(&mut source).await;
    }

    #[tokio::test]
    async fn given_oversized_forwarded_header_when_posted_should_clamp_and_count() {
        let mut config = config(free_port(), free_port(), &[ENDPOINT_ONE]);
        config.forward_headers = vec!["user-agent".to_string()];
        let mut source = open(1, config).await;

        client()
            .post(format!("{}/e/{ENDPOINT_ONE}", base_url(&source)))
            .header(crate::DEFAULT_HMAC_HEADER, signature(b"{}"))
            .header(header::USER_AGENT, "u".repeat(400))
            .body("{}")
            .send()
            .await
            .expect("the request must reach the listener");

        let message = source
            .receiver
            .lock()
            .await
            .try_recv()
            .expect("the request must have been queued");
        let headers = message.headers.expect("metadata forwarding is on");
        let forwarded = headers
            .get(&HeaderKey::try_from("user-agent").expect("valid key"))
            .and_then(|value| value.as_str().ok())
            .expect("an oversized value is clamped, not dropped");
        assert_eq!(forwarded.len(), crate::types::MAX_HEADER_VALUE_BYTES);
        let (clamped, dropped) = metrics_snapshot(&source, &source.shared.instance_name).await;
        assert_eq!(clamped, 1);
        assert_eq!(dropped, 0, "an oversized value is clamped, never dropped");
        close(&mut source).await;
    }

    #[tokio::test]
    async fn given_serving_instance_when_admin_health_read_should_report_counts() {
        let mut config = config(free_port(), free_port(), &[ENDPOINT_ONE]);
        config.instance_name = Some("http_github".to_string());
        config.buffer_capacity = 7;
        let admin = format!("http://{}", config.admin_listen_addr);
        let mut source = open(1, config).await;
        post_signed(&base_url(&source), ENDPOINT_ONE, "{}").await;

        let body: serde_json::Value = client()
            .get(format!("{admin}/admin/health"))
            .send()
            .await
            .expect("the request must reach the admin listener")
            .json()
            .await
            .expect("admin health must be JSON");

        assert_eq!(body["status"], "ok");
        let instance = &body["instances"][0];
        assert_eq!(instance["instance"], "http_github");
        assert_eq!(instance["buffer_used"], 1);
        assert_eq!(instance["buffer_capacity"], 7);
        assert_eq!(instance["endpoints_static"], 1);
        assert_eq!(instance["endpoints_dynamic"], 0);
        assert_eq!(instance["endpoints_revoked"], 0);
        assert_eq!(instance["named_path"], true);
        assert_eq!(instance["state_submitted"], true);
        close(&mut source).await;
    }

    #[tokio::test]
    async fn given_revoked_endpoint_when_admin_health_read_should_report_it_as_revoked() {
        let mut config = config(free_port(), free_port(), &[ENDPOINT_ONE]);
        config.instance_name = Some("http_github".to_string());
        let admin = format!("http://{}", config.admin_listen_addr);
        let mut source = open(1, config).await;
        source
            .shared
            .mutate_registry(|registry| registry.revoke(ENDPOINT_ONE, "compromised".to_string(), 1))
            .await;

        let body: serde_json::Value = client()
            .get(format!("{admin}/admin/health"))
            .send()
            .await
            .expect("the request must reach the admin listener")
            .json()
            .await
            .expect("admin health must be JSON");

        let instance = &body["instances"][0];
        assert_eq!(instance["endpoints_static"], 0);
        assert_eq!(instance["endpoints_revoked"], 1);
        assert_eq!(
            instance["state_submitted"], false,
            "a mutation not yet handed to the runtime must not read as durable"
        );
        close(&mut source).await;
    }

    #[tokio::test]
    async fn given_expired_endpoint_when_admin_health_read_should_not_count_it_as_serving() {
        let mut config = config(free_port(), free_port(), &[ENDPOINT_ONE]);
        config.instance_name = Some("http_github".to_string());
        let admin = format!("http://{}", config.admin_listen_addr);
        let mut source = open(1, config).await;
        source
            .shared
            .mutate_registry(|registry| {
                registry
                    .endpoint_mut(ENDPOINT_ONE)
                    .expect("the static endpoint is registered")
                    .expires_at = Some(1);
            })
            .await;

        let body: serde_json::Value = client()
            .get(format!("{admin}/admin/health"))
            .send()
            .await
            .expect("the request must reach the admin listener")
            .json()
            .await
            .expect("admin health must be JSON");

        let instance = &body["instances"][0];
        assert_eq!(
            instance["endpoints_static"], 0,
            "an endpoint that answers 404 to everything is not serving"
        );
        assert_eq!(instance["endpoints_expired"], 1);
        assert_eq!(instance["endpoints_revoked"], 0);
        close(&mut source).await;
    }

    #[tokio::test]
    async fn given_metadata_enabled_when_posted_should_carry_peer_and_receive_time() {
        let mut source = open(1, config(free_port(), free_port(), &[ENDPOINT_ONE])).await;
        let before = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("the clock is after 1970")
            .as_micros();

        post_signed(&base_url(&source), ENDPOINT_ONE, "{}").await;

        let message = source
            .receiver
            .lock()
            .await
            .try_recv()
            .expect("the request must have been queued");
        let headers = message.headers.expect("metadata forwarding is on");
        assert_eq!(
            headers
                .get(&HeaderKey::try_from(REMOTE_ADDR_HEADER).expect("valid key"))
                .and_then(|value| value.as_str().ok()),
            Some("127.0.0.1")
        );
        let received_at: u128 = headers
            .get(&HeaderKey::try_from(RECEIVED_AT_HEADER).expect("valid key"))
            .and_then(|value| value.as_str().ok())
            .expect("receive time must be present")
            .parse()
            .expect("receive time must be numeric microseconds");
        assert!(
            received_at >= before,
            "receive time must not predate the request"
        );
        close(&mut source).await;
    }

    #[tokio::test]
    async fn given_metadata_disabled_when_posted_should_carry_no_headers() {
        let mut config = config(free_port(), free_port(), &[ENDPOINT_ONE]);
        config.include_http_metadata = false;
        let mut source = open(1, config).await;

        post_signed(&base_url(&source), ENDPOINT_ONE, "{}").await;

        let message = source
            .receiver
            .lock()
            .await
            .try_recv()
            .expect("the request must have been queued");
        assert!(
            message.headers.is_none(),
            "no metadata and no forward_headers must mean no header map at all"
        );
        close(&mut source).await;
    }

    #[tokio::test]
    async fn given_two_instances_when_joined_should_share_one_listener() {
        let public_port = free_port();
        let admin_port = free_port();
        let mut first = open(1, config(public_port, admin_port, &[ENDPOINT_ONE])).await;
        let mut second_config = config(public_port, admin_port, &[ENDPOINT_TWO]);
        second_config.topic_path = Some("stripe".to_string());
        let mut second = open(2, second_config).await;
        let base = base_url(&first);

        assert_eq!(
            post_signed(&base, ENDPOINT_ONE, "{}").await.status(),
            StatusCode::OK
        );
        assert_eq!(
            post_signed(&base, ENDPOINT_TWO, "{}").await.status(),
            StatusCode::OK
        );
        assert_eq!(first.shared.sender.len(), 1);
        assert_eq!(second.shared.sender.len(), 1);

        close(&mut first).await;
        assert_eq!(
            post_signed(&base, ENDPOINT_ONE, "{}").await.status(),
            StatusCode::NOT_FOUND,
            "one instance leaving must not take the listener down with it"
        );
        assert_eq!(
            post_signed(&base, ENDPOINT_TWO, "{}").await.status(),
            StatusCode::OK
        );
        close(&mut second).await;
    }

    #[tokio::test]
    async fn given_mismatched_body_limit_when_joined_should_reject() {
        let public_port = free_port();
        let admin_port = free_port();
        let mut first = open(1, config(public_port, admin_port, &[ENDPOINT_ONE])).await;
        let mut second_config = config(public_port, admin_port, &[ENDPOINT_TWO]);
        second_config.topic_path = Some("stripe".to_string());
        second_config.max_body_size_bytes = 1;

        let mut second = HttpSource::new(2, second_config, None);
        let error = second
            .open()
            .await
            .expect_err("a listener cannot serve two body limits at once");

        assert!(matches!(
            error,
            Error::InvalidConfigValue(message) if message.contains("max_body_size_bytes")
        ));
        close(&mut first).await;
    }

    #[tokio::test]
    async fn given_mismatched_admin_address_when_joined_should_reject() {
        let public_port = free_port();
        let mut first = open(1, config(public_port, free_port(), &[ENDPOINT_ONE])).await;
        let mut second_config = config(public_port, free_port(), &[ENDPOINT_TWO]);
        second_config.topic_path = Some("stripe".to_string());

        let mut second = HttpSource::new(2, second_config, None);
        let error = second
            .open()
            .await
            .expect_err("an instance must not silently get an admin listener it did not configure");

        assert!(matches!(
            error,
            Error::InvalidConfigValue(message) if message.contains("admin_listen_addr")
        ));
        close(&mut first).await;
    }

    #[tokio::test]
    async fn given_mismatched_management_token_when_joined_should_reject() {
        let public_port = free_port();
        let admin_port = free_port();
        let mut first_config = config(public_port, admin_port, &[ENDPOINT_ONE]);
        first_config.management_token = Some(SecretString::from("mgmt-secret"));
        let mut first = open(1, first_config).await;
        let mut second_config = config(public_port, admin_port, &[ENDPOINT_TWO]);
        second_config.topic_path = Some("stripe".to_string());

        let mut second = HttpSource::new(2, second_config, None);
        let error = second
            .open()
            .await
            .expect_err("one listener cannot answer to two management tokens");

        assert!(matches!(
            error,
            Error::InvalidConfigValue(message) if message.contains("management_token")
        ));
        close(&mut first).await;
    }

    #[tokio::test]
    async fn given_colliding_topic_path_when_joined_should_reject() {
        let public_port = free_port();
        let admin_port = free_port();
        let mut first = open(1, config(public_port, admin_port, &[ENDPOINT_ONE])).await;

        let mut second = HttpSource::new(2, config(public_port, admin_port, &[ENDPOINT_TWO]), None);
        let error = second
            .open()
            .await
            .expect_err("two instances cannot claim the same topic path");

        assert!(matches!(
            error,
            Error::InvalidConfigValue(message) if message.contains("topic_path")
        ));
        assert_eq!(
            post_signed(&base_url(&first), ENDPOINT_TWO, "{}")
                .await
                .status(),
            StatusCode::NOT_FOUND,
            "a rejected join must leave no routes behind"
        );
        close(&mut first).await;
    }

    #[tokio::test]
    async fn given_last_instance_when_left_should_release_the_port() {
        let public_port = free_port();
        let mut source = open(1, config(public_port, free_port(), &[ENDPOINT_ONE])).await;
        close(&mut source).await;

        TcpListener::bind(format!("127.0.0.1:{public_port}"))
            .await
            .expect("the last close must release the port for the runtime's restart flow");
    }

    #[tokio::test]
    async fn given_served_traffic_when_metrics_scraped_should_report_it() {
        let mut config = config(free_port(), free_port(), &[ENDPOINT_ONE]);
        config.instance_name = Some("http_github".to_string());
        config.buffer_capacity = 4;
        let admin = format!("http://{}", config.admin_listen_addr);
        let mut source = open(1, config).await;
        let base = base_url(&source);

        post_signed(&base, ENDPOINT_ONE, "{}").await;
        post_signed(&base, ENDPOINT_TWO, "{}").await;

        let scraped = client()
            .get(format!("{admin}/admin/metrics"))
            .send()
            .await
            .expect("the request must reach the admin listener")
            .text()
            .await
            .expect("the scrape must have a body");

        assert!(scraped.contains(
            "http_source_requests_total{instance=\"http_github\",kind=\"secret\",status=\"2xx\"} 1"
        ));
        assert!(
            scraped.contains(
                "http_source_requests_total{instance=\"unrouted\",kind=\"secret\",status=\"4xx\"} 1"
            ),
            "a probe for live endpoint ids must be countable even though it resolves to nothing"
        );
        assert!(scraped.contains("http_source_buffer_used{instance=\"http_github\"} 1"));
        assert!(scraped.contains("http_source_buffer_capacity{instance=\"http_github\"} 4"));
        assert!(
            scraped.contains(
                "http_source_endpoints_active{instance=\"http_github\",kind=\"static\"} 1"
            )
        );
        close(&mut source).await;
    }

    #[tokio::test]
    async fn given_queued_messages_when_instance_closes_should_count_the_loss() {
        let public_port = free_port();
        let admin_port = free_port();
        let mut first_config = config(public_port, admin_port, &[ENDPOINT_ONE]);
        first_config.instance_name = Some("http_github".to_string());
        let admin = format!("http://{}", first_config.admin_listen_addr);
        let mut first = open(1, first_config).await;
        // A sibling keeps the listener alive so the counter survives to be
        // scraped; the last instance leaving takes the whole registry with it.
        let mut second_config = config(public_port, admin_port, &[ENDPOINT_TWO]);
        second_config.topic_path = Some("stripe".to_string());
        second_config.instance_name = Some("http_stripe".to_string());
        let mut second = open(2, second_config).await;

        post_signed(&base_url(&first), ENDPOINT_ONE, "{}").await;
        close(&mut first).await;

        let scraped = client()
            .get(format!("{admin}/admin/metrics"))
            .send()
            .await
            .expect("the request must reach the admin listener")
            .text()
            .await
            .expect("the scrape must have a body");

        assert!(
            scraped.contains("http_source_dropped_on_close_total{instance=\"http_github\"} 1"),
            "messages accepted but never polled are lost, and the loss must be visible"
        );
        close(&mut second).await;
    }

    #[tokio::test]
    async fn given_serving_instance_when_health_checked_should_report_ok() {
        let mut source = open(1, config(free_port(), free_port(), &[])).await;
        // Readiness needs a poll task behind the route, and nothing drives one
        // in these tests, so stand in for the poll that would be in flight.
        let shared = Arc::clone(source.shared());
        let _polling = shared.enter_poll();

        let response = client()
            .get(format!("{}/health", base_url(&source)))
            .send()
            .await
            .expect("the request must reach the listener");

        assert_eq!(response.status(), StatusCode::OK);
        close(&mut source).await;
    }

    #[tokio::test]
    async fn given_no_instances_when_health_checked_should_report_unavailable() {
        let mut source = open(1, config(free_port(), free_port(), &[])).await;
        // The window a load balancer must see: routes are gone but the last
        // instance has not finished releasing the listener yet.
        publish(&source, Vec::new()).await;

        let response = client()
            .get(format!("{}/health", base_url(&source)))
            .send()
            .await
            .expect("the request must reach the listener");

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        close(&mut source).await;
    }

    /// Republishes the instance set so a registry mutation reaches the table.
    /// The management API does this for real; these tests reach for it after
    /// mutating the registry directly.
    async fn rebuild_routes(source: &HttpSource) {
        let instances = {
            let servers = SERVERS.lock().await;
            servers
                .get(&source.shared.config.listen_addr)
                .expect("the instance is joined")
                .state
                .instances()
        };
        publish(source, instances).await;
    }

    async fn publish(source: &HttpSource, instances: Vec<Arc<SharedState>>) {
        let servers = SERVERS.lock().await;
        servers
            .get(&source.shared.config.listen_addr)
            .expect("the instance is joined")
            .state
            .publish(instances)
            .expect("republishing a known-good instance set cannot collide");
    }

    /// The metrics live on the listener, not the instance, so a test has to
    /// go through the registry to read them.
    async fn metrics_snapshot(source: &HttpSource, instance: &str) -> (u64, u64) {
        let servers = SERVERS.lock().await;
        let metrics = &servers
            .get(&source.shared.config.listen_addr)
            .expect("the instance is joined")
            .state
            .metrics;
        (
            metrics.headers_clamped(instance),
            metrics.headers_dropped(instance),
        )
    }

    #[test]
    fn given_populated_routes_when_serving_nothing_should_drop_every_one() {
        let state = ServerState::new(&config(free_port(), free_port(), &[ENDPOINT_ONE]));
        state
            .publish(vec![crate::test_support::instance(
                1,
                Some("github"),
                &[ENDPOINT_ONE],
            )])
            .expect("a single instance cannot collide with itself");
        assert_eq!(state.routes.load().secret_path_count(), 1);
        assert_eq!(state.routes.load().named_path_count(), 1);

        state.serve_nothing();

        assert_eq!(state.routes.load().secret_path_count(), 0);
        assert_eq!(state.routes.load().named_path_count(), 0);
        assert_eq!(
            state.instances().len(),
            1,
            "the instances stay joined; only the routes projecting them stop being served"
        );
    }

    #[tokio::test]
    async fn given_no_bound_listener_when_routes_refreshed_should_name_the_address() {
        let unbound = format!("127.0.0.1:{}", free_port());

        let error = refresh_routes(&unbound)
            .await
            .expect_err("an address nothing is bound to cannot be reprojected");

        assert!(matches!(
            error,
            Error::InitError(message) if message.contains(&unbound)
        ));
    }

    #[tokio::test]
    async fn given_duplicate_instance_name_when_joined_should_reject() {
        let public_port = free_port();
        let admin_port = free_port();
        let mut first_config = config(public_port, admin_port, &[ENDPOINT_ONE]);
        first_config.instance_name = Some("http_github".to_string());
        let mut first = open(1, first_config).await;

        let mut second_config = config(public_port, admin_port, &[ENDPOINT_TWO]);
        second_config.instance_name = Some("http_github".to_string());
        second_config.topic_path = Some("stripe".to_string());
        let mut second = HttpSource::new(2, second_config, None);
        let error = second
            .open()
            .await
            .expect_err("a duplicate name would address two instances at once");

        assert!(matches!(
            error,
            Error::InvalidConfigValue(message) if message.contains("instance_name")
        ));
        assert_eq!(
            post_signed(&base_url(&first), ENDPOINT_TWO, "{}")
                .await
                .status(),
            StatusCode::NOT_FOUND,
            "a rejected join must leave no routes behind"
        );
        close(&mut first).await;
    }

    #[tokio::test]
    async fn given_unknown_named_path_when_posted_should_answer_not_found_as_unrouted() {
        let mut config = config(free_port(), free_port(), &[]);
        config.instance_name = Some("http_github".to_string());
        let admin = format!("http://{}", config.admin_listen_addr);
        let mut source = open(1, config).await;

        let response = client()
            .post(format!("{}/topics/unclaimed", base_url(&source)))
            .body("{}")
            .send()
            .await
            .expect("the request must reach the listener");

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let scraped = client()
            .get(format!("{admin}/admin/metrics"))
            .send()
            .await
            .expect("the request must reach the admin listener")
            .text()
            .await
            .expect("the scrape must have a body");
        assert!(
            scraped.contains(
                "http_source_requests_total{instance=\"unrouted\",kind=\"named\",status=\"4xx\"} 1"
            ),
            "a misconfigured sender posting to the wrong path is the thing an \
             operator needs to see, so it cannot go uncounted: {scraped}"
        );
        close(&mut source).await;
    }

    #[tokio::test]
    async fn given_oversized_body_when_posted_to_a_named_path_should_answer_payload_too_large() {
        let mut config = config(free_port(), free_port(), &[]);
        config.max_body_size_bytes = 16;
        let mut source = open(1, config).await;

        let response = client()
            .post(format!("{}/topics/github", base_url(&source)))
            .body("x".repeat(1024))
            .send()
            .await
            .expect("the request must reach the listener");

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(source.shared.sender.len(), 0);
        close(&mut source).await;
    }

    #[test]
    fn given_unrepresentable_forwarded_values_when_headers_built_should_drop_and_count_them() {
        let mut config = config(free_port(), free_port(), &[]);
        config.include_http_metadata = false;
        config.forward_headers = vec!["x-binary".to_string(), "x-blank".to_string()];
        let source = HttpSource::new(1, config, None);

        let mut request_headers = HeaderMap::new();
        request_headers.insert(
            axum::http::HeaderName::from_static("x-binary"),
            axum::http::HeaderValue::from_bytes(&[0xff])
                .expect("an opaque byte is a legal HTTP header value"),
        );
        request_headers.insert(
            axum::http::HeaderName::from_static("x-blank"),
            axum::http::HeaderValue::from_static(""),
        );

        let (headers, clamped, dropped) = message_headers(
            &source.shared,
            &request_headers,
            "127.0.0.1:4444".parse().expect("a literal address parses"),
        );

        // Both are present but unrepresentable: one is not visible ASCII, the
        // other clamps away to nothing. An absent header is not a loss, so a
        // count above two would be silent over-reporting.
        assert_eq!(dropped, 2);
        assert_eq!(clamped, 0);
        assert!(
            headers.is_none(),
            "with metadata off and both forwarded values dropped there is nothing to attach"
        );
    }

    async fn close(source: &mut HttpSource) {
        source.close().await.expect("close must succeed");
    }
}
