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

use arc_swap::{ArcSwap, Guard};
use axum::Router;
use axum::body::Bytes;
use axum::extract::{ConnectInfo, DefaultBodyLimit, Path, Request, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, serve};
use iggy_common::{HeaderKey, HeaderValue};
use iggy_connector_sdk::Error;
use rand::RngExt;
use ring::hmac;
use secrecy::SecretString;
use serde::Serialize;
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::{Arc, LazyLock};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio::sync::{Mutex, Notify, watch};
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

/// `Retry-After` on a 429, in seconds. Named because a test asserts it and a
/// bare literal in two places drifts.
const RETRY_AFTER_SECONDS: &str = "1";

/// How long a `join()` waits for a draining listener to release its address.
/// Longer than [`SHUTDOWN_TIMEOUT`], since the drain it is waiting on is
/// itself bounded by that.
const DRAIN_WAIT_TIMEOUT: Duration = Duration::from_secs(8);

/// Registers an instance with the listener for its configured address,
/// binding that listener if this is the first instance to ask for it.
pub async fn join(instance: Arc<SharedState>) -> Result<(), Error> {
    let listen_addr = instance.config.listen_addr.clone();
    let mut servers = SERVERS.lock().await;

    if let Some(server) = servers.get_mut(&listen_addr) {
        if server.draining {
            // A sibling is mid-shutdown. Failing here turns two concurrent
            // restarts on one listener into one instance left stopped, so wait
            // for the entry to go and try again. The waiter is enrolled before
            // the guard is released, or the notify could fire into the gap and
            // be missed.
            let drained = Arc::clone(&server.drained);
            let mut waiter = Box::pin(drained.notified());
            waiter.as_mut().enable();
            drop(servers);
            if tokio::time::timeout(DRAIN_WAIT_TIMEOUT, waiter)
                .await
                .is_err()
            {
                return Err(Error::InitError(format!(
                    "The {CONNECTOR_NAME} listener on {listen_addr} was still shutting down after {}s; retry the open once its port is released",
                    DRAIN_WAIT_TIMEOUT.as_secs()
                )));
            }
            return Box::pin(join(instance)).await;
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
///
/// `staged_dropped` is folded into the same shutdown loss metric as whatever
/// is still queued in the bridge.
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
        // never joined. Tearing down on their behalf rebuilds the route table
        // for nothing and logs a deregistration that never happened.
        //
        // Deliberately untested, because the effects are not observable. The
        // republished route set is unchanged, and the sampled gauges are
        // rebuilt from the live instances on every scrape. A test written
        // against those would pass with this guard removed.
        return;
    }

    let remaining: Vec<Arc<SharedState>> = joined
        .into_iter()
        .filter(|candidate| candidate.id != instance.id)
        .collect();
    if let Err(error) = server.state.publish(remaining) {
        // Dropping an instance cannot introduce a collision, so this is
        // unreachable; serve nothing rather than stale routes if it happens.
        error!(
            "Failed to rebuild {CONNECTOR_NAME} routes after connector ID: {} left. {error}",
            instance.id
        );
        server.state.serve_nothing();
    }
    // Read back rather than trusting the vec we hoped to publish, so the count
    // describes what is actually being served. It is not what keeps the
    // listener from leaking: `publish` is all-or-nothing, so on its failure
    // branch the departed instance is still in `instances` either way. What
    // makes that branch unreachable is that `remaining` is a subset of a table
    // that already built, and dropping entries cannot introduce a path
    // collision.
    let remaining_count = server.state.instances().len();
    info!(
        "Deregistered {CONNECTOR_NAME} routes for connector ID: {}, instances left on {listen_addr}: {remaining_count}",
        instance.id
    );

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
        let removed = SERVERS.lock().await.remove(listen_addr);
        // After the removal, so a woken `join()` re-reads a map that no longer
        // holds the drained entry and binds fresh.
        if let Some(removed) = removed {
            removed.drained.notify_waiters();
        }
    }
}

/// Every listener this process has bound, keyed by public listen address.
///
/// An async mutex because the guard is held across the bind and across the
/// graceful shutdown await. Both are open/close operations, never requests.
static SERVERS: LazyLock<Mutex<HashMap<String, SharedServer>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

struct SharedServer {
    state: Arc<ServerState>,
    shutdown: watch::Sender<()>,
    tasks: Vec<JoinHandle<()>>,
    /// Set while the listener is draining. The entry stays in the registry so
    /// a concurrent `join()` cannot bind a port that is still held, but it can
    /// no longer be joined.
    draining: bool,
    /// Fired once the drained entry has been removed from the registry, so a
    /// `join()` that arrived mid-drain can retry instead of failing.
    drained: Arc<Notify>,
}

/// The instance set and the routes derived from it, published as one value so
/// no reader can see them disagree.
#[derive(Debug, Default)]
pub(crate) struct Published {
    pub(crate) instances: Vec<Arc<SharedState>>,
    pub(crate) routes: RouteTable,
}

/// The part of a shared server the request handlers see.
#[derive(Debug)]
pub(crate) struct ServerState {
    pub(crate) listen_addr: String,
    /// Taken from the first instance to bind, like `management_token`.
    /// `ensure_compatible` refuses a join that disagrees, so it is unambiguous.
    pub(crate) admin_listen_addr: String,
    /// Taken from the first instance to bind, like `management_token`.
    /// `ensure_compatible` refuses a join that disagrees, so it is unambiguous.
    /// Held here so a handler can read the body itself, after authorizing,
    /// rather than letting an extractor buffer it first.
    pub(crate) max_body_size_bytes: usize,
    /// Taken from the first instance to bind. Every instance joining the same
    /// listener must present the same token, so this is unambiguous.
    pub(crate) management_token: Option<SecretString>,
    pub(crate) metrics: Metrics,
    /// Instances and the table built from them, swapped together.
    ///
    /// Two `ArcSwap`s let a reader land between the stores and resolve a
    /// request against the old table while holding the new instance set, which
    /// on a departure means routing into a bridge nobody drains. One swap makes
    /// that unrepresentable rather than merely narrow.
    published: ArcSwap<Published>,
    started_at: Instant,
}

impl ServerState {
    pub(crate) fn new(config: &HttpSourceConfig) -> Self {
        ServerState {
            listen_addr: config.listen_addr.clone(),
            admin_listen_addr: config.admin_listen_addr.clone(),
            max_body_size_bytes: config.max_body_size_bytes,
            management_token: config.management_token.clone(),
            metrics: Metrics::new(),
            published: ArcSwap::from_pointee(Published::default()),
            started_at: Instant::now(),
        }
    }

    pub(crate) fn instances(&self) -> Vec<Arc<SharedState>> {
        self.published.load().instances.clone()
    }

    /// One consistent view of both. Callers that need the instance set and the
    /// routes to agree must read them from a single guard, not two loads.
    pub(crate) fn published(&self) -> Guard<Arc<Published>> {
        self.published.load()
    }

    pub(crate) fn instance(&self, instance_name: &str) -> Option<Arc<SharedState>> {
        self.published()
            .instances
            .iter()
            .find(|instance| instance.instance_name == instance_name)
            .map(Arc::clone)
    }

    /// Serves nothing until the next successful publish. Used when a mutation
    /// removes access but the table cannot be rebuilt: stale routes would keep
    /// honouring a credential the operator believes is gone.
    pub(crate) fn serve_nothing(&self) {
        // `rcu`, not load-then-store. A `publish()` from join or leave landing
        // between the two would be discarded, and because this keeps the
        // instance set it would leave the listener bound and serving nothing
        // with no record of the publish that went missing.
        let dropped = self.published.rcu(|current| {
            // Instances are kept: they are still joined and still own bridges,
            // and it is only the routing that is being withdrawn.
            Arc::new(Published {
                instances: current.instances.clone(),
                routes: RouteTable::default(),
            })
        });
        error!(
            "Serving no {CONNECTOR_NAME} routes on {}: dropped {} secret paths and {} named paths across {} instances until the next successful publish",
            self.listen_addr,
            dropped.routes.secret_path_count(),
            dropped.routes.named_path_count(),
            dropped.instances.len()
        );
    }

    /// Swaps in a new instance set and the routes it projects to, or leaves
    /// both untouched if the instances collide on a path.
    fn publish(&self, instances: Vec<Arc<SharedState>>) -> Result<(), Error> {
        let routes = RouteTable::build(&instances)
            .map_err(|conflict| Error::InvalidConfigValue(conflict.to_string()))?;
        self.published
            .store(Arc::new(Published { instances, routes }));
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
        let public = bind("listen_addr", &config.listen_addr).await?;
        let admin = bind("admin_listen_addr", &config.admin_listen_addr).await?;
        let (shutdown, _) = watch::channel(());

        // The one place this plugin owns background tasks. The runtime cannot
        // drive an HTTP listener for us, so the last close() shuts them down
        // explicitly rather than leaving them to outlive the connector.
        let tasks = vec![
            tokio::spawn(run(
                public,
                public_router(Arc::clone(&state)),
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
            state,
            shutdown,
            tasks,
            draining: false,
            drained: Arc::new(Notify::new()),
        })
    }

    /// Refuses a join whose settings disagree with the running listener.
    ///
    /// Fails closed on purpose: first-instance-wins would leave an operator
    /// with a body limit or admin address their TOML says they do not have.
    fn ensure_compatible(&self, config: &HttpSourceConfig) -> Result<(), Error> {
        if self.state.admin_listen_addr != config.admin_listen_addr {
            return Err(Error::InvalidConfigValue(format!(
                "admin_listen_addr '{}' does not match '{}' on the listener already bound to {}",
                config.admin_listen_addr, self.state.admin_listen_addr, config.listen_addr
            )));
        }
        if self.state.max_body_size_bytes != config.max_body_size_bytes {
            return Err(Error::InvalidConfigValue(format!(
                "max_body_size_bytes {} does not match {} on the listener already bound to {}",
                config.max_body_size_bytes, self.state.max_body_size_bytes, config.listen_addr
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

/// Binds one listener, naming the config field it came from.
///
/// Both the public and admin listeners come through here, so a message that
/// always said `listen_addr` sent an operator to the wrong line of TOML when
/// it was the admin port that collided.
async fn bind(field: &str, address: &str) -> Result<TcpListener, Error> {
    TcpListener::bind(address).await.map_err(|error| {
        // Instances are grouped by the `listen_addr` string, so two spellings
        // of the same socket, `0.0.0.0:9090` and `127.0.0.1:9090`, are two
        // groups and the second one tries to bind a port the first already
        // holds. The operator sees a bind failure for a field they believe is
        // shared, so name that possibility rather than only the OS error.
        let hint = if error.kind() == std::io::ErrorKind::AddrInUse {
            format!(
                ". If another {CONNECTOR_NAME} instance is meant to share this listener, its {field} must match this one exactly"
            )
        } else {
            String::new()
        };
        Error::InitError(format!(
            "Failed to bind the {CONNECTOR_NAME} listener for {field} to {address}. {error}{hint}"
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

/// No `DefaultBodyLimit`: both POST handlers take the request whole and pass
/// the operator's cap to `to_bytes` themselves, which is what lets them refuse
/// a request before reading it.
fn public_router(state: Arc<ServerState>) -> Router {
    Router::new()
        .route("/topics/{topic_path}", post(handle_named_path))
        .route("/e/{endpoint_id}", post(handle_secret_path))
        .route("/health", get(handle_health))
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

/// A request that resolved to no instance is still metered, under [`UNROUTED`],
/// which is where a scan for live endpoint ids shows up.
fn instance_label(instance: Option<&Arc<SharedState>>) -> &str {
    instance.map_or(UNROUTED, |instance| instance.instance_name.as_str())
}

fn same_token(left: &Option<SecretString>, right: &Option<SecretString>) -> bool {
    match (left, right) {
        (None, None) => true,
        (Some(left), Some(right)) => secrets_match(left, right),
        _ => false,
    }
}

/// Takes the whole `Request` rather than a `Bytes` extractor, because axum runs
/// extractors before the handler body: with one, an unauthenticated caller who
/// guessed the operator-chosen `topic_path` could make the process buffer
/// `max_body_size_bytes` before receiving its 401. The credential here is a
/// header, so the body is only read once the request has earned it. The secret
/// paths cannot do this for HMAC, whose signature covers the body, and there
/// the endpoint id is itself a 128-bit secret the caller must already hold.
async fn handle_named_path(
    State(state): State<Arc<ServerState>>,
    Path(topic_path): Path<String>,
    ConnectInfo(remote_addr): ConnectInfo<SocketAddr>,
    request: Request,
) -> Response {
    let started = Instant::now();
    let (instance, response) = named_path_outcome(&state, &topic_path, remote_addr, request).await;
    state.metrics.record_request(
        instance_label(instance.as_ref()),
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
    request: Request,
) -> Response {
    let started = Instant::now();
    let (instance, response) =
        secret_path_outcome(&state, &endpoint_id, remote_addr, request).await;
    state.metrics.record_request(
        instance_label(instance.as_ref()),
        PathKind::Secret,
        response.status().as_u16(),
        started.elapsed(),
    );
    response
}

/// The instance the request resolved to, for the caller to label metrics with.
/// Returning the `Arc` the lookup already produced rather than a copy of its
/// name keeps the label allocation-free on the request path; `None` is a
/// request that resolved to no instance, counted under [`UNROUTED`].
async fn named_path_outcome(
    state: &ServerState,
    topic_path: &str,
    remote_addr: SocketAddr,
    request: Request,
) -> (Option<Arc<SharedState>>, Response) {
    // `into_parts` hands them over owned, so nothing is cloned to survive the
    // body being taken.
    let (parts, body) = request.into_parts();
    let request_headers = parts.headers;
    let instance = {
        let routes = &state.published().routes;
        let Some(instance) = routes.lookup_named_path(topic_path) else {
            return (None, error_response(StatusCode::NOT_FOUND, "not found"));
        };
        Arc::clone(instance)
    };

    // Before the body: the whole point of taking the request whole.
    if let Some(expected) = &instance.config.auth_bearer_token
        && !validate_bearer(bearer_header(&request_headers), expected)
    {
        return (
            Some(instance),
            error_response(StatusCode::UNAUTHORIZED, "unauthorized"),
        );
    }

    // `DefaultBodyLimit` guards the extractor, which is no longer in play, so
    // the cap is applied here instead.
    let body = match axum::body::to_bytes(body, state.max_body_size_bytes).await {
        Ok(body) => body,
        Err(error) => return (Some(instance), oversized_body_response(&error)),
    };
    let response = enqueue(
        &instance,
        &request_headers,
        remote_addr,
        body,
        &state.metrics,
    );
    (Some(instance), response)
}

async fn secret_path_outcome(
    state: &ServerState,
    endpoint_id: &str,
    remote_addr: SocketAddr,
    request: Request,
) -> (Option<Arc<SharedState>>, Response) {
    // `into_parts` hands the headers over owned; taking them as an extractor
    // cloned the whole map on every request.
    let (parts, body) = request.into_parts();
    let request_headers = parts.headers;
    let routes = &state.published().routes;
    let entry = match routes.lookup_secret_path(endpoint_id, unix_now_seconds()) {
        RouteLookup::Active(entry) => entry,
        // Both answer as if the endpoint never existed, so a leaked URL cannot
        // be used to confirm it was once live: a 410 for the expired case would
        // be returned before any credential is checked and would say exactly
        // that. The metric still names the owning instance, so only a genuinely
        // unknown path is `unrouted`.
        RouteLookup::Revoked(entry) | RouteLookup::Expired(entry) => {
            return (
                Some(Arc::clone(&entry.instance)),
                error_response(StatusCode::NOT_FOUND, "not found"),
            );
        }
        RouteLookup::Unknown => {
            return (None, error_response(StatusCode::NOT_FOUND, "not found"));
        }
    };
    let instance = Arc::clone(&entry.instance);
    // Only now, once the id resolved to something that is actually serving.
    // An unknown or revoked id used to cost a full body read before its 404,
    // which is free amplification on the public listener. HMAC still needs the
    // body, so the read cannot move any earlier than this.
    let body = match axum::body::to_bytes(body, state.max_body_size_bytes).await {
        Ok(body) => body,
        Err(error) => return (Some(instance), oversized_body_response(&error)),
    };
    if !authorize(
        &entry.endpoint,
        entry.hmac_key.as_ref(),
        &request_headers,
        &body,
    ) {
        return (
            Some(instance),
            error_response(StatusCode::UNAUTHORIZED, "unauthorized"),
        );
    }
    let response = enqueue(
        &entry.instance,
        &request_headers,
        remote_addr,
        body,
        &state.metrics,
    );
    (Some(instance), response)
}

/// OpenMetrics text format. Unguarded like `/admin/health`: the admin
/// listener defaults to loopback and scrapers do not carry bearer tokens.
///
/// The content type is set explicitly because the body carries an `# EOF`
/// trailer, which is OpenMetrics, not the `text/plain` a bare `String` would
/// have been served as.
async fn handle_admin_metrics(State(state): State<Arc<ServerState>>) -> Response {
    (
        [(
            header::CONTENT_TYPE,
            "application/openmetrics-text; version=1.0.0; charset=utf-8",
        )],
        state.metrics.encode(&state.instances()),
    )
        .into_response()
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
    // One guard for both, so readiness cannot be computed from a route table
    // and an instance set that were never published together.
    let published = state.published();
    let instances = &published.instances;
    // Every instance, not any. One load balancer fronts the whole listener, so
    // it can only take all of these in or out together. If one instance's poll
    // task has stopped while a sibling's is alive, `any` keeps the address in
    // rotation and the dead one's webhooks are accepted into a bridge nobody
    // drains, which is the exact failure this gate exists to catch. Shedding
    // the healthy sibling's traffic too costs availability that senders recover
    // by retrying; the alternative loses data that was already answered 200.
    let ready = published.routes.serves_anything(now)
        && !instances.is_empty()
        && instances.iter().all(|instance| instance.poll_is_live(now));
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
        .published()
        .instances
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
                poll_is_live: instance.poll_is_live(now),
                // The registry is handed over whole, so an owed flush is the
                // whole answer. Still not `persisted`: the flag clears when the
                // state leaves the plugin, and the runtime's write landing is
                // something no poll return value reports back.
                state_submitted: !instance.has_pending_state(),
                headers_dropped: state.metrics.headers_dropped(&instance.instance_name),
                headers_clamped: state.metrics.headers_clamped(&instance.instance_name),
            }
        })
        .collect();

    // Derived, not a constant: this answered "ok" while `/health` was
    // answering 503 for the same listener, which is the one moment an operator
    // is looking at both.
    let now = unix_now_seconds();
    let published = state.published();
    let ready = published.routes.serves_anything(now)
        && !published.instances.is_empty()
        && published
            .instances
            .iter()
            .all(|instance| instance.poll_is_live(now));
    Json(AdminHealth {
        status: if ready { "ok" } else { "degraded" },
        instances,
        uptime_secs: state.started_at.elapsed().as_secs(),
    })
    .into_response()
}

/// The 429 a full bridge answers with.
///
/// Shared so the early check and the `try_send` gate cannot drift apart in
/// status, `Retry-After`, or body.
fn bridge_full_response() -> Response {
    (
        StatusCode::TOO_MANY_REQUESTS,
        [(header::RETRY_AFTER, RETRY_AFTER_SECONDS)],
        // Not the 503's wording: an operator grepping logs has to be able to
        // tell a full bridge from a listener that is shutting down.
        Json(ErrorResponse {
            error: "too many requests",
        }),
    )
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
    // Checked before the header map is built and the body copied, since a full
    // bridge throws both away. `is_full` is racy, which is why `try_send`
    // below stays the real gate; this only skips the work when the answer is
    // already known.
    if instance.sender.is_full() {
        metrics.record_rejected_full(&instance.instance_name);
        debug!(
            "Rejected a request for {CONNECTOR_NAME} connector ID: {}, bridge is full at {} messages",
            instance.id, instance.config.buffer_capacity
        );
        return bridge_full_response();
    }
    let (headers, clamped, dropped) = message_headers(instance, request_headers, remote_addr);
    let message = QueuedMessage {
        // Minted here, at accept time, so a replay after a NACK carries the
        // same id rather than a fresh one.
        id: rand::rng().random(),
        // `to_vec`, deliberately, not `Vec::from(body)`. The latter hands back
        // hyper's read buffer, which at typical webhook sizes is several times
        // the body, and the bridge is bounded by message count rather than
        // bytes. Copying the exact length is what keeps `buffer_capacity`
        // meaning what an operator thinks it means.
        payload: body.to_vec(),
        headers,
    };
    if let Err(error) = instance.sender.try_send(message) {
        // A disconnected bridge is not a full one. It cannot happen while the
        // route table holds an `Arc` to this `SharedState`, but reporting it as
        // backpressure would tell a sender to retry into a channel that no
        // longer has a receiver, and would file the loss under the wrong metric.
        if matches!(error, crossfire::TrySendError::Disconnected(_)) {
            metrics.record_rejected_disconnected(&instance.instance_name);
            error!(
                "Rejected a request for {CONNECTOR_NAME} connector ID: {}, its bridge has no receiver",
                instance.id
            );
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: "service unavailable",
                }),
            )
                .into_response();
        }
        metrics.record_rejected_full(&instance.instance_name);
        debug!(
            "Rejected a request for {CONNECTOR_NAME} connector ID: {}, bridge is full at {} messages",
            instance.id, instance.config.buffer_capacity
        );
        return bridge_full_response();
    }
    // Only now: a rejected request produced no message, so its header losses
    // would otherwise be counted against messages that never existed.
    metrics.record_headers(&instance.instance_name, clamped, dropped);
    Json(StatusResponse { status: "queued" }).into_response()
}

fn authorize(
    endpoint: &Endpoint,
    hmac_key: Option<&hmac::Key>,
    request_headers: &HeaderMap,
    body: &[u8],
) -> bool {
    match endpoint.auth_type {
        EndpointAuthType::None => true,
        EndpointAuthType::Bearer => endpoint
            .auth_secret
            .as_ref()
            .is_some_and(|secret| validate_bearer(bearer_header(request_headers), secret)),
        EndpointAuthType::HmacSha256 | EndpointAuthType::HmacSha1 => {
            // Absent only if the endpoint claims HMAC with no secret, which
            // `validate()` and the management API both refuse. Fail closed.
            let Some(key) = hmac_key else {
                return false;
            };
            validate_hmac(
                body,
                header_str(request_headers, &endpoint.hmac_header),
                &endpoint.hmac_prefix,
                key,
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
        insert_header(&mut headers, &INSTANCE_HEADER_KEY, &instance.instance_name);
        insert_header(
            &mut headers,
            &REMOTE_ADDR_HEADER_KEY,
            &remote_addr.ip().to_string(),
        );
        if let Some(received_at) = received_at_micros() {
            insert_header(&mut headers, &RECEIVED_AT_HEADER_KEY, &received_at);
        }
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

/// The three metadata keys, parsed once. They are compile-time constants, so
/// re-parsing them per request repeated validation that can only ever succeed,
/// the same reason `forward_headers` is resolved at construction.
static INSTANCE_HEADER_KEY: LazyLock<Option<HeaderKey>> =
    LazyLock::new(|| HeaderKey::try_from(INSTANCE_HEADER).ok());
static REMOTE_ADDR_HEADER_KEY: LazyLock<Option<HeaderKey>> =
    LazyLock::new(|| HeaderKey::try_from(REMOTE_ADDR_HEADER).ok());
static RECEIVED_AT_HEADER_KEY: LazyLock<Option<HeaderKey>> =
    LazyLock::new(|| HeaderKey::try_from(RECEIVED_AT_HEADER).ok());

fn insert_header(
    headers: &mut BTreeMap<HeaderKey, HeaderValue>,
    key: &Option<HeaderKey>,
    value: &str,
) {
    let (Some(key), Ok(value)) = (key.as_ref(), HeaderValue::from_str(value)) else {
        return;
    };
    headers.insert(key.clone(), value);
}

/// `None` when the clock predates the epoch, which is the only way this fails.
///
/// Defaulting to 0 would stamp every message with the epoch, and the header
/// exists so a consumer can measure queue latency, so a plausible wrong value
/// is worse than an absent one. `unix_now_seconds` saturates instead because
/// its callers gate on expiry, where failing closed is the safe direction.
fn received_at_micros() -> Option<String> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .ok()
        .map(|elapsed| elapsed.as_micros().to_string())
}

pub(crate) fn bearer_header(request_headers: &HeaderMap) -> Option<&str> {
    request_headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
}

fn header_str<'a>(request_headers: &'a HeaderMap, name: &str) -> Option<&'a str> {
    request_headers
        .get(name)
        .and_then(|value| value.to_str().ok())
}

/// `to_bytes` reports the cap and a truncated stream through the same error, so
/// the status mirrors what the `Bytes` extractor would have returned: a body
/// that ran past the limit is 413, anything else is a 400.
fn oversized_body_response(error: &axum::Error) -> Response {
    let too_large = error
        .to_string()
        .to_ascii_lowercase()
        .contains("length limit exceeded");
    if too_large {
        error_response(StatusCode::PAYLOAD_TOO_LARGE, "payload too large")
    } else {
        error_response(StatusCode::BAD_REQUEST, "bad request")
    }
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
    /// Whether a poll has run recently enough to believe one still will.
    ///
    /// `state_submitted` says a change was handed over; this says whether
    /// anything is still there to hand the next one to. The SDK stops the poll
    /// task after five consecutive NACKs without calling `close()`, so the
    /// instance stays registered and keeps accepting mutations that will never
    /// be persisted. See #3941.
    poll_is_live: bool,
    state_submitted: bool,
    /// Named to mirror the metric families exactly, so an operator reading
    /// `/admin/health` and a Prometheus scrape is not looking at two spellings
    /// of one thing.
    headers_dropped: u64,
    headers_clamped: u64,
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

    /// Opens a source, retrying if another test took the port first.
    ///
    /// `free_port` binds, reads the port and releases it, so there is a window
    /// before this bind where anything else on the machine can take it.
    /// nextest runs each test in its own process, so no in-process bookkeeping
    /// can close that window; retrying is what actually removes the flake.
    async fn open(id: u32, mut config: HttpSourceConfig) -> HttpSource {
        const ATTEMPTS: usize = 8;
        for attempt in 1..=ATTEMPTS {
            let mut source = HttpSource::new(id, config.clone(), None);
            match source.open().await {
                Ok(()) => return source,
                Err(error) if attempt < ATTEMPTS && is_address_in_use(&error) => {
                    config.listen_addr = format!("127.0.0.1:{}", free_port());
                    config.admin_listen_addr = format!("127.0.0.1:{}", free_port());
                }
                Err(error) => panic!("open must succeed, got: {error}"),
            }
        }
        unreachable!("the loop returns or panics on the last attempt")
    }

    /// The 413/400 split reads `to_bytes`'s error text, so a wording change
    /// upstream would silently turn every oversized body into a 400. Pinned
    /// here because axum does not expose the limit error as a type to match on.
    #[tokio::test]
    async fn given_an_oversized_body_when_read_should_still_say_length_limit_exceeded() {
        let body = axum::body::Body::from(vec![0u8; 64]);
        let error = axum::body::to_bytes(body, 8)
            .await
            .expect_err("a body past the limit must fail");
        assert!(
            error
                .to_string()
                .to_ascii_lowercase()
                .contains("length limit exceeded"),
            "oversized_body_response splits 413 from 400 on this text, got: {error}"
        );
    }

    fn is_address_in_use(error: &Error) -> bool {
        format!("{error}").contains("Address already in use")
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
    async fn given_oversized_body_to_unknown_endpoint_should_answer_not_found_not_payload_too_large()
     {
        // Guards the ordering against a return to buffering before routing.
        // With the body as an extractor, axum size-checked it before the
        // handler ran, so an unknown id answered 413 having already paid for
        // the read. What this pins is the status an unknown id gets, which is
        // what regresses if the read moves back ahead of the lookup and its
        // failure is surfaced there. It does not, and over HTTP cannot, prove
        // that no bytes were read.
        // A small cap, and a body oversized only relative to it. At the
        // default 1 MiB the request outgrew the socket buffer, so the server's
        // early 404 reached the client mid-write and reqwest surfaced a
        // BrokenPipe instead of the response: the refusal this test wants is
        // exactly what made it flaky. A few KiB completes the write before the
        // server can answer, and the cap is what "oversized" is measured
        // against, so shrinking both pins the same ordering deterministically.
        let mut config = config(free_port(), free_port(), &[ENDPOINT_ONE]);
        config.max_body_size_bytes = 1024;
        let oversized = "x".repeat(config.max_body_size_bytes + 1024);
        let mut source = open(1, config).await;

        let response = client()
            .post(format!("{}/e/{ENDPOINT_TWO}", base_url(&source)))
            .body(oversized)
            .send()
            .await
            .expect("the request must reach the listener");

        assert_eq!(
            response.status(),
            StatusCode::NOT_FOUND,
            "an unknown id must be refused before its body is read"
        );
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
        source.shared.mutate_registry(|registry| {
            registry.revoke(ENDPOINT_ONE, "compromised".to_string(), 1)
        });
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
    async fn given_expired_endpoint_when_posted_should_answer_not_found() {
        let mut source = open(1, config(free_port(), free_port(), &[ENDPOINT_ONE])).await;
        source.shared.mutate_registry(|registry| {
            registry
                .endpoint_mut(ENDPOINT_ONE)
                .expect("the static endpoint is registered")
                .expires_at = Some(1);
            true
        });
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
    async fn given_unauthenticated_oversized_body_should_refuse_before_reading_it() {
        // The status is the whole assertion. axum runs extractors before the
        // handler body, so while the named path took a `Bytes` extractor an
        // unauthenticated caller who guessed `topic_path` could make the
        // process buffer up to `max_body_size_bytes` before its 401. If the
        // body were still read first this would answer 413.
        let mut config = config(free_port(), free_port(), &[]);
        config.auth_bearer_token = Some(SecretString::from("named-path-secret"));
        config.max_body_size_bytes = 1024;
        let mut source = open(1, config).await;

        let response = client()
            .post(format!("{}/topics/github", base_url(&source)))
            .header(header::AUTHORIZATION, "Bearer not-the-token")
            .body("x".repeat(4096))
            .send()
            .await
            .expect("the request must reach the listener");

        assert_eq!(
            response.status(),
            StatusCode::UNAUTHORIZED,
            "an unauthenticated caller must not reach the body reader"
        );
        close(&mut source).await;
    }

    #[tokio::test]
    async fn given_authenticated_oversized_body_should_answer_payload_too_large() {
        // And the cap still applies once the request has earned the read.
        let mut config = config(free_port(), free_port(), &[]);
        config.auth_bearer_token = Some(SecretString::from("named-path-secret"));
        config.max_body_size_bytes = 1024;
        let mut source = open(1, config).await;

        let response = client()
            .post(format!("{}/topics/github", base_url(&source)))
            .header(header::AUTHORIZATION, "Bearer named-path-secret")
            .body("x".repeat(4096))
            .send()
            .await
            .expect("the request must reach the listener");

        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
        close(&mut source).await;
    }

    #[tokio::test]
    async fn given_full_bridge_when_posted_should_answer_too_many_requests() {
        let mut config = config(free_port(), free_port(), &[ENDPOINT_ONE]);
        // Two, not one: at 1 the ring is degenerate, so a test cannot tell a
        // real capacity bound from an off-by-one. `bounded_async` always builds
        // an `Array` whatever the size, so the flavour is not what differs.
        config.buffer_capacity = 2;
        let mut source = open(1, config).await;
        let base = base_url(&source);

        for _ in 0..2 {
            assert_eq!(
                post_signed(&base, ENDPOINT_ONE, "{}").await.status(),
                StatusCode::OK
            );
        }
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

        // Derived from the same readiness `/health` reports, so the two cannot
        // disagree. No poll has run here, which is exactly the state `/health`
        // answers 503 for, so both say the listener is not ready.
        let readiness = client()
            .get(format!("{}/health", base_url(&source)))
            .send()
            .await
            .expect("the request must reach the listener")
            .status();
        assert_eq!(readiness, StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(body["status"], "degraded");
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
        source.shared.mutate_registry(|registry| {
            registry.revoke(ENDPOINT_ONE, "compromised".to_string(), 1)
        });

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
        source.shared.mutate_registry(|registry| {
            registry
                .endpoint_mut(ENDPOINT_ONE)
                .expect("the static endpoint is registered")
                .expires_at = Some(1);
            true
        });

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
    async fn given_revoked_endpoint_when_posted_should_meter_it_against_its_owner() {
        // `build` keeps revoked entries in the table so a leaked id 404s under
        // the instance that owns it. Without that the id would fall through to
        // `unrouted`, which is the endpoint-id-scan signal, and revoking a busy
        // endpoint would look exactly like an attack in progress.
        let mut config = config(free_port(), free_port(), &[ENDPOINT_ONE]);
        config.instance_name = Some("http_github".to_string());
        let admin = format!("http://{}", config.admin_listen_addr);
        let mut source = open(1, config).await;
        let base = base_url(&source);
        let shared = Arc::clone(&source.shared);
        let _polling = shared.enter_poll();

        assert!(shared.mutate_registry(|registry| registry.revoke(
            ENDPOINT_ONE,
            "compromised".to_string(),
            1
        )));
        rebuild_routes(&source).await;

        post_signed(&base, ENDPOINT_ONE, "{}").await;

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
                "http_source_requests_total{instance=\"http_github\",kind=\"secret\",status=\"4xx\"} 1"
            ),
            "a revoked endpoint is still the owning instance's traffic, got: {scraped}"
        );
        assert!(
            !scraped.contains("instance=\"unrouted\""),
            "attributing it to unrouted would forge the scan signal, got: {scraped}"
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

    #[test]
    fn given_constant_metadata_keys_when_parsed_should_all_resolve() {
        // They are parsed once into `Option` rather than `expect`ed, because a
        // panic in a `LazyLock` on the request path unwinds out of the cdylib
        // and aborts the whole connectors process. That safety costs a silent
        // skip if one is ever mistyped, so pin it here instead.
        for (name, key) in [
            (INSTANCE_HEADER, &*INSTANCE_HEADER_KEY),
            (REMOTE_ADDR_HEADER, &*REMOTE_ADDR_HEADER_KEY),
            (RECEIVED_AT_HEADER, &*RECEIVED_AT_HEADER_KEY),
        ] {
            assert!(
                key.is_some(),
                "{name} must be a valid Iggy header key, or it vanishes from every message in silence"
            );
        }
    }

    #[tokio::test]
    async fn given_serving_instance_when_health_checked_should_report_ok() {
        let mut source = open(1, config(free_port(), free_port(), &[])).await;
        // Readiness needs a poll task behind the route, and nothing drives one
        // in these tests, so stand in for the poll that would be in flight.
        let shared = Arc::clone(&source.shared);
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
    async fn given_one_stopped_instance_when_health_checked_should_report_unavailable() {
        // One address fronts both instances, so a sibling whose poll task has
        // stopped would keep receiving traffic into a bridge nothing drains.
        let public_port = free_port();
        let admin_port = free_port();
        let mut first_config = config(public_port, admin_port, &[ENDPOINT_ONE]);
        first_config.instance_name = Some("http_github".to_string());
        let mut first = open(1, first_config).await;

        let mut second_config = config(public_port, admin_port, &[ENDPOINT_TWO]);
        second_config.instance_name = Some("http_partner".to_string());
        second_config.topic_path = Some("stripe".to_string());
        let mut second = open(2, second_config).await;

        let live = Arc::clone(&first.shared);
        let _polling = live.enter_poll();

        let response = client()
            .get(format!("{}/health", base_url(&first)))
            .send()
            .await
            .expect("the request must reach the listener");

        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "one instance polling is not enough when the listener is shared"
        );
        close(&mut second).await;
        close(&mut first).await;
    }

    #[tokio::test]
    async fn given_all_endpoints_revoked_when_health_checked_should_report_unavailable() {
        // `build` inserts revoked endpoints so a leaked id resolves to a 404
        // attributed to its owner rather than to `unrouted`, which means a bare
        // emptiness check on the table would call this instance routable.
        let mut config = config(free_port(), free_port(), &[ENDPOINT_ONE]);
        config.topic_path = None;
        let mut source = open(1, config).await;
        let shared = Arc::clone(&source.shared);
        let _polling = shared.enter_poll();

        assert!(shared.mutate_registry(|registry| registry.revoke(
            ENDPOINT_ONE,
            "compromised".to_string(),
            1
        )));
        rebuild_routes(&source).await;

        let response = client()
            .get(format!("{}/health", base_url(&source)))
            .send()
            .await
            .expect("the request must reach the listener");

        assert_eq!(
            response.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "an instance that would refuse every request is not ready"
        );
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
        assert_eq!(state.published().routes.secret_path_count(), 1);
        assert_eq!(state.published().routes.named_path_count(), 1);

        state.serve_nothing();

        assert_eq!(state.published().routes.secret_path_count(), 0);
        assert_eq!(state.published().routes.named_path_count(), 0);
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
