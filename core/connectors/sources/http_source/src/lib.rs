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

mod auth;
mod management;
mod metrics;
mod routes;
mod server;
mod state;
mod types;

use arc_swap::{ArcSwap, Guard};
use async_trait::async_trait;
use axum::http::HeaderName;
use iggy_common::HeaderKey;
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source, source,
    source_connector,
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Mutex as StdMutex, PoisonError};
use std::time::Duration;
use tokio::sync::{Mutex, Notify};
use tracing::{debug, info, warn};

use crate::auth::HmacAlgorithm;
use crate::server::{INSTANCE_HEADER, RECEIVED_AT_HEADER, REMOTE_ADDR_HEADER};
use crate::state::EndpointRegistry;
use crate::types::{EndpointId, QueuedMessage, unix_now_seconds};

pub const CONNECTOR_NAME: &str = "HTTP source";

pub const DEFAULT_ADMIN_LISTEN_ADDR: &str = "127.0.0.1:9091";
pub const DEFAULT_MAX_BODY_SIZE_BYTES: usize = 1024 * 1024;
pub const DEFAULT_BUFFER_CAPACITY: usize = 10_000;
pub const DEFAULT_MAX_BATCH_SIZE: usize = 500;
/// Ceilings for the three sizing values, mirroring their `> 0` floors.
///
/// `buffer_capacity` is the dangerous one: crossfire asserts `bound <=
/// u32::MAX` while building the ring and allocates every slot eagerly, each
/// holding an inline `MaybeUninit<T>`, so a typo either panics or OOMs. That panic unwinds out of `iggy_source_open`, which is
/// `extern "C"` with no `catch_unwind` around it, and takes down the whole
/// connectors process along with every other plugin loaded into it. The ring
/// is built in `new`, before the SDK ever calls `open`, so `validate` cannot
/// be what protects it.
pub const MAX_BUFFER_CAPACITY: usize = 1_000_000;
/// Bounds the `Vec::with_capacity(max_batch_size)` that every `poll()` builds
/// up front, which is what an oversized value actually costs.
pub const MAX_BATCH_SIZE_LIMIT: usize = 100_000;
pub const MAX_MAX_BODY_SIZE_BYTES: usize = 64 * 1024 * 1024;

pub const DEFAULT_HMAC_HEADER: &str = "X-Hub-Signature-256";
pub const DEFAULT_HMAC_PREFIX: &str = "sha256=";

const IDLE_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// How long after a poll returns the source still counts as live. Generous
/// against the SDK's 30s batch-result timeout, which is the longest a healthy
/// source sits between polls.
const POLL_LIVENESS_SECONDS: u64 = 60;

/// HTTP handler side of an instance's bridge. The pair comes from
/// `bounded_async` because the `poll()` side needs `recv().await`; the handler
/// side only ever `try_send`s.
pub type MessageSender = crossfire::MAsyncTx<crossfire::mpsc::Array<QueuedMessage>>;

/// `poll()` side of an instance's bridge.
pub type MessageReceiver = crossfire::AsyncRx<crossfire::mpsc::Array<QueuedMessage>>;

source_connector!(HttpSource);

/// Webhook gateway source: accepts HTTP POST requests on a listener shared
/// by every instance of this plugin and produces the raw bodies to the
/// instance's configured stream/topic.
#[derive(Debug)]
pub struct HttpSource {
    pub id: u32,
    shared: Arc<SharedState>,
    /// Deliberately not on [`SharedState`]: handlers hold that behind an `Arc`
    /// for as long as the listener serves them, and a receiver reachable from
    /// there would outlive the source it belongs to.
    ///
    /// The mutex only exists to make the single-consumer receiver `Sync`, as
    /// [`Source`] requires. The runtime drives exactly one `poll()` at a time,
    /// so it is never contended despite being held across the wait.
    receiver: Mutex<MessageReceiver>,
    /// Batch already handed to the runtime and not yet acknowledged.
    ///
    /// The runtime NACKs a batch it could not send, expecting the next `poll()`
    /// to produce it again. Draining the bridge is destructive, so without this
    /// the events would exist nowhere else and the NACK would be silent loss.
    /// `std`, not `tokio`: the guard never crosses an `.await`, so the async
    /// lock only added suspension points. One of them mattered - a shutdown
    /// landing inside `stage()` could drop the batch there with nothing
    /// counting it.
    staged: StdMutex<Option<StagedBatch>>,
    /// Why the persisted registry could not be restored, if it could not be.
    ///
    /// Held rather than acted on in `new` so `open` can fail with it, which is
    /// what puts it on the control API as `last_error`.
    restore_error: Option<String>,
}

/// Clears the in-flight marker however `poll()` ends, including cancellation.
/// The SDK drops the poll future when it stops the task, so without a guard a
/// stopped source would look permanently mid-poll.
pub(crate) struct PollGuard<'a>(&'a SharedState);

impl Drop for PollGuard<'_> {
    fn drop(&mut self) {
        // Timestamp first. `poll_is_live` reads the two as an unsynchronised
        // pair, so clearing the flag first leaves a window where a reader sees
        // "not polling" beside the *previous* timestamp. On the first return
        // that previous value is the constructor's 0, which reads as an hour
        // stale, and `/health` answers 503 for a source that is perfectly
        // healthy. Writing this way the pair is only ever seen as live.
        self.0
            .last_poll_at
            .store(unix_now_seconds(), Ordering::Release);
        self.0.poll_active.store(false, Ordering::Release);
    }
}

/// Unacknowledged batch, kept whole so a NACK can be re-polled verbatim.
#[derive(Debug)]
struct StagedBatch {
    messages: Vec<QueuedMessage>,
    /// Consecutive NACKs this batch has taken, for the retry policy below.
    nacks: u32,
}

/// Everything an HTTP handler needs from one instance.
///
/// Handler tasks hold `Arc<SharedState>` and never `Arc<HttpSource>`: the SDK
/// tears a source down with `Arc::try_unwrap`, so a stray clone of the source
/// itself would turn `close()` into a leak.
#[derive(Debug)]
pub struct SharedState {
    pub id: u32,
    pub config: HttpSourceConfig,
    pub sender: MessageSender,
    /// Resolved once from `instance_name`, falling back to the connector ID.
    pub instance_name: String,
    /// Each configured request header paired with the Iggy header key it lands
    /// under, resolved once so the request path neither parses nor validates.
    pub(crate) forward_headers: Vec<(HeaderName, HeaderKey)>,
    registry: ArcSwap<EndpointRegistry>,
    registry_dirty: AtomicBool,
    /// True while a `poll()` is in flight, including the long block on an idle
    /// bridge. Cleared by a guard, so a cancelled poll clears it too, which is
    /// what makes a stopped poll task observable.
    poll_active: AtomicBool,
    /// When the last `poll()` returned. Covers the gap between polls, where
    /// the SDK is awaiting a batch result and nothing is in flight.
    last_poll_at: AtomicU64,
    /// Serializes registry writers, which are all control-plane. The request
    /// path only ever loads the `ArcSwap` and never touches this.
    registry_writer: Mutex<()>,
    /// Wakes `poll()` when a management mutation needs a state flush and no
    /// webhook traffic would otherwise arrive to carry it.
    pub state_flush: Notify,
}

impl SharedState {
    /// Wait-free snapshot of the registry. Requests do not come through here:
    /// they resolve against the prebuilt `RouteTable`, and this serves the
    /// control plane and the scrape path. Cheap rather than free, since
    /// arc-swap's hybrid strategy takes a debt slot and can fall back to a
    /// full clone once a thread exhausts them.
    pub fn registry(&self) -> Guard<Arc<EndpointRegistry>> {
        self.registry.load()
    }

    /// Applies a control-plane mutation, arming the next state flush only if
    /// it changed something.
    ///
    /// The closure reports whether it changed anything, and a `false` costs
    /// nothing beyond the clone. Arming unconditionally let an authenticated
    /// caller turn a stream of no-ops, repeatedly revoking an already-revoked
    /// endpoint, into one registry serialization and one state-store write per
    /// 404, which is a remote write on the HTTP state backend.
    pub async fn mutate_registry(
        &self,
        mutation: impl FnOnce(&mut EndpointRegistry) -> bool,
    ) -> bool {
        {
            let _writer = self.registry_writer.lock().await;
            let mut next = EndpointRegistry::clone(&self.registry.load());
            if !mutation(&mut next) {
                return false;
            }
            self.registry.store(Arc::new(next));
            self.registry_dirty.store(true, Ordering::Release);
        }
        // Notified after the gate is free, so the woken poll finds it open
        // rather than bouncing off `try_lock` and relying on the re-arm there.
        self.state_flush.notify_one();
        true
    }

    /// Whether a mutation is still waiting to be handed to the runtime.
    pub fn has_pending_state(&self) -> bool {
        self.registry_dirty.load(Ordering::Acquire)
    }

    /// Names the auth posture of every path this instance serves.
    ///
    /// A misspelled `auth_bearer_token` key does not fail the config, it just
    /// deserializes to `None`, and the named-path handler skips its whole auth
    /// block when it is `None`. So the difference between "guarded" and "open
    /// to the internet" is one silent typo. Saying it out loud at open is what
    /// makes that visible without turning every env override into an error,
    /// which is what `deny_unknown_fields` here would do.
    pub fn log_auth_posture(&self) {
        if let Some(topic_path) = &self.config.topic_path {
            if self.config.auth_bearer_token.is_some() {
                info!(
                    "Serving POST /topics/{topic_path} with bearer auth for {CONNECTOR_NAME} connector ID: {}",
                    self.id
                );
            } else {
                warn!(
                    "Serving POST /topics/{topic_path} with NO authentication for {CONNECTOR_NAME} connector ID: {}; set auth_bearer_token to guard it",
                    self.id
                );
            }
        }

        let unguarded = self
            .registry()
            .serving_count_without_auth(unix_now_seconds());
        if unguarded > 0 {
            warn!(
                "Serving {unguarded} secret-path endpoints with no second factor for {CONNECTOR_NAME} connector ID: {}; anyone holding the URL can post to them",
                self.id
            );
        }
    }

    /// Whether the poll task still looks alive.
    ///
    /// An in-flight poll counts even when it has been blocked on an empty
    /// bridge for hours, which is the normal state of a quiet gateway. The
    /// timestamp covers the other case, where the SDK is between polls waiting
    /// for a batch result. Neither advances once the poll task has stopped.
    pub fn poll_is_live(&self, now_seconds: u64) -> bool {
        self.poll_active.load(Ordering::Acquire)
            || now_seconds.saturating_sub(self.last_poll_at.load(Ordering::Acquire))
                <= POLL_LIVENESS_SECONDS
    }

    pub(crate) fn enter_poll(&self) -> PollGuard<'_> {
        self.poll_active.store(true, Ordering::Release);
        PollGuard(self)
    }

    /// Re-arms a flush whose state was taken but never persisted.
    ///
    /// `take_dirty_state` clears the flag and marks every endpoint submitted
    /// before the state leaves the plugin, so a caller that later learns the
    /// save did not land has to put the flag back. Without it the mutation is
    /// never retried and nothing on the control API says so.
    pub fn rearm_state_flush(&self) {
        self.registry_dirty.store(true, Ordering::Release);
        self.state_flush.notify_one();
    }

    /// Hands the registry to the runtime for persistence, once per mutation.
    ///
    /// Only ever called for an empty batch. The runtime saves state solely on
    /// the success branch of the Iggy send, so attaching state to a batch of
    /// messages would let a failed send skip the save while this side had
    /// already cleared the flag and marked the registry submitted, losing a
    /// revocation tombstone with no trace. An empty batch cannot fail *for
    /// want of a successful publish*, which is what makes it the safe carrier.
    /// It can still be NACKed: the runtime short-circuits the send stage when
    /// its own state storage is latched or a pending checkpoint will not
    /// resolve, which is why `on_nack` re-arms rather than assuming success.
    ///
    /// Static-only instances never arm the flag, so their polls return
    /// `state: None` and the runtime writes no state file at all.
    pub fn take_dirty_state(&self) -> Option<ConnectorState> {
        // `try_lock`, never `lock().await`. `poll()` calls this holding a
        // batch that has already left the bridge, and the SDK drops the poll
        // future on shutdown; an await here would lose those messages
        // uncounted. Contention comes only from a concurrent management call,
        // and losing that race just leaves the flag armed for the next poll.
        let Ok(_writer) = self.registry_writer.try_lock() else {
            // Mid-mutation. If the permit that woke this poll was theirs we
            // have just consumed it, so re-post rather than sleep on a flush
            // that no further traffic would ever carry. Gated on the flag so a
            // poll that raced a mutation carrying nothing does not wake itself.
            if self.registry_dirty.load(Ordering::Acquire) {
                self.state_flush.notify_one();
            }
            return None;
        };
        if !self.registry_dirty.swap(false, Ordering::AcqRel) {
            return None;
        }
        let snapshot = self.registry.load_full();
        let Some(state) = snapshot.to_connector_state(self.id) else {
            // Serialization logged the cause. The flag goes back so the next
            // poll retries, but deliberately without re-posting the permit:
            // doing that makes the next poll immediate, and since nothing
            // about the registry has changed it fails identically, re-arms,
            // and spins the poll task through a full FFI round trip per
            // iteration with no exit. Traffic or the next mutation carries it.
            self.registry_dirty.store(true, Ordering::Release);
            return None;
        };
        let mut persisted = EndpointRegistry::clone(&snapshot);
        persisted.mark_submitted();
        self.registry.store(Arc::new(persisted));
        Some(state)
    }
}

/// Deliberately not `Serialize`. The runtime keeps plugin configuration as raw
/// JSON and never serializes this struct, so nothing needs it — and without it
/// the compiler guarantees a credential cannot be written out by some future
/// caller. `SecretString` has no `Serialize` impl for exactly that reason.
#[derive(Debug, Clone, Deserialize)]
pub struct HttpSourceConfig {
    /// Public listener shared by all instances of this plugin; every
    /// instance must configure the identical address.
    pub listen_addr: String,
    /// Management + observability listener; never route it through the
    /// public load balancer.
    #[serde(default = "default_admin_listen_addr")]
    pub admin_listen_addr: String,
    #[serde(default = "default_max_body_size_bytes")]
    pub max_body_size_bytes: usize,
    /// Capacity of this instance's HTTP-to-poll bridge, in messages. A full
    /// bridge answers 429.
    #[serde(default = "default_buffer_capacity")]
    pub buffer_capacity: usize,
    /// Maximum messages returned by a single `poll()`.
    #[serde(default = "default_max_batch_size")]
    pub max_batch_size: usize,
    /// Path segment exposed as `POST /topics/{topic_path}`. Unset disables
    /// the named path, leaving only secret-path endpoints.
    #[serde(default)]
    pub topic_path: Option<String>,
    /// Identifies this instance in forwarded message headers and on the admin
    /// listener. Defaults to the connector ID: the runtime keeps the connector
    /// key on its own side of the FFI, so the plugin cannot read it.
    #[serde(default)]
    pub instance_name: Option<String>,
    /// Guards the named topic path; unset leaves that path unauthenticated
    /// (for deployments fronted by an authenticating gateway).
    #[serde(default)]
    pub auth_bearer_token: Option<SecretString>,
    /// Enables `/admin/endpoints`; unset disables dynamic management.
    #[serde(default)]
    pub management_token: Option<SecretString>,
    #[serde(default = "default_true")]
    pub include_http_metadata: bool,
    /// HTTP request headers forwarded as Iggy message headers, clamped to
    /// the 255-byte `HeaderValue` limit.
    #[serde(default)]
    pub forward_headers: Vec<String>,
    /// Statically configured secret-path endpoints.
    #[serde(default)]
    pub endpoints: Vec<StaticEndpointConfig>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StaticEndpointConfig {
    pub endpoint_id: EndpointId,
    #[serde(default)]
    pub auth_type: EndpointAuthType,
    #[serde(default)]
    pub auth_secret: Option<SecretString>,
    #[serde(default = "default_hmac_header")]
    pub hmac_header: String,
    #[serde(default = "default_hmac_prefix")]
    pub hmac_prefix: String,
    /// Unix seconds; requests arriving at or after this answer 404.
    #[serde(default)]
    pub expires_at: Option<u64>,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EndpointAuthType {
    #[default]
    None,
    Bearer,
    HmacSha256,
    HmacSha1,
}

impl EndpointAuthType {
    pub fn hmac_algorithm(self) -> Option<HmacAlgorithm> {
        match self {
            Self::HmacSha256 => Some(HmacAlgorithm::HmacSha256),
            Self::HmacSha1 => Some(HmacAlgorithm::HmacSha1),
            Self::None | Self::Bearer => None,
        }
    }
}

impl HttpSourceConfig {
    fn validate(&self) -> Result<(), Error> {
        self.listen_addr.parse::<SocketAddr>().map_err(|error| {
            Error::InvalidConfigValue(format!("listen_addr '{}': {error}", self.listen_addr))
        })?;
        self.admin_listen_addr
            .parse::<SocketAddr>()
            .map_err(|error| {
                Error::InvalidConfigValue(format!(
                    "admin_listen_addr '{}': {error}",
                    self.admin_listen_addr
                ))
            })?;
        if self.buffer_capacity == 0 || self.buffer_capacity > MAX_BUFFER_CAPACITY {
            return Err(Error::InvalidConfigValue(format!(
                "buffer_capacity {} must be between 1 and {MAX_BUFFER_CAPACITY}",
                self.buffer_capacity
            )));
        }
        if self.max_batch_size == 0 || self.max_batch_size > MAX_BATCH_SIZE_LIMIT {
            return Err(Error::InvalidConfigValue(format!(
                "max_batch_size {} must be between 1 and {MAX_BATCH_SIZE_LIMIT}",
                self.max_batch_size
            )));
        }
        if self.max_body_size_bytes == 0 || self.max_body_size_bytes > MAX_MAX_BODY_SIZE_BYTES {
            return Err(Error::InvalidConfigValue(format!(
                "max_body_size_bytes {} must be between 1 and {MAX_MAX_BODY_SIZE_BYTES}",
                self.max_body_size_bytes
            )));
        }
        if let Some(instance_name) = &self.instance_name {
            if HeaderKey::try_from(instance_name.as_str()).is_err() {
                return Err(Error::InvalidConfigValue(format!(
                    "instance_name '{instance_name}' must be a valid Iggy header key: non-empty and at most 255 bytes"
                )));
            }
            if instance_name == crate::metrics::UNROUTED {
                // Requests to a genuinely unknown path are metered under this
                // name, and that series is what an operator watches for
                // endpoint-id scanning. An instance claiming it would merge
                // real traffic into the signal.
                return Err(Error::InvalidConfigValue(format!(
                    "instance_name '{instance_name}' is reserved for requests that match no route"
                )));
            }
        }
        for header in &self.forward_headers {
            // Forwarding a reusable credential would copy it onto every
            // message and into the log Iggy persists. A per-body signature
            // header is fine; these are not.
            if matches!(
                header.to_ascii_lowercase().as_str(),
                "authorization" | "proxy-authorization" | "cookie"
            ) {
                return Err(Error::InvalidConfigValue(format!(
                    "forward_headers entry '{header}' would copy a credential onto every message"
                )));
            }
            // `message_headers` writes the gateway's own metadata into the
            // same map first, so forwarding a request header under one of
            // these keys lets the sender overwrite a value the pipeline
            // treats as trusted.
            if matches!(
                header.to_ascii_lowercase().as_str(),
                INSTANCE_HEADER | REMOTE_ADDR_HEADER | RECEIVED_AT_HEADER
            ) {
                return Err(Error::InvalidConfigValue(format!(
                    "forward_headers entry '{header}' is reserved for the gateway's own metadata"
                )));
            }
            if HeaderName::from_str(header).is_err() {
                return Err(Error::InvalidConfigValue(format!(
                    "forward_headers entry '{header}' is not a valid HTTP header name"
                )));
            }
            if HeaderKey::try_from(header.as_str()).is_err() {
                return Err(Error::InvalidConfigValue(format!(
                    "forward_headers entry '{header}' is not a valid Iggy header key"
                )));
            }
        }
        if let Some(topic_path) = &self.topic_path
            && (topic_path.is_empty() || topic_path.contains('/'))
        {
            return Err(Error::InvalidConfigValue(format!(
                "topic_path '{topic_path}' must be a single non-empty path segment"
            )));
        }
        for header in [
            ("auth_bearer_token", &self.auth_bearer_token),
            ("management_token", &self.management_token),
        ] {
            if let (field, Some(secret)) = header
                && secret.expose_secret().is_empty()
            {
                return Err(Error::InvalidConfigValue(format!(
                    "{field} must not be empty; omit it entirely to disable that guard"
                )));
            }
        }
        let mut seen: BTreeSet<&EndpointId> = BTreeSet::new();
        for endpoint in &self.endpoints {
            if !seen.insert(&endpoint.endpoint_id) {
                // The registry is a map, so a second block with the same id
                // silently last-wins, and a copy-paste whose second copy
                // carries a weaker auth_type is the one that gets served.
                return Err(Error::InvalidConfigValue(format!(
                    "endpoint {} is declared more than once",
                    endpoint.endpoint_id.log_prefix()
                )));
            }
            if HeaderName::from_str(&endpoint.hmac_header).is_err() {
                // An invalid name makes `HeaderMap::get` return `None`, so
                // every signed request 401s forever with nothing naming why.
                return Err(Error::InvalidConfigValue(format!(
                    "endpoint {} declares hmac_header '{}', which is not a valid HTTP header name",
                    endpoint.endpoint_id.log_prefix(),
                    endpoint.hmac_header
                )));
            }
            if endpoint.auth_type != EndpointAuthType::None
                && !endpoint
                    .auth_secret
                    .as_ref()
                    .is_some_and(|secret| !secret.expose_secret().is_empty())
            {
                // An empty key is perfectly valid for HMAC, so accepting one
                // would leave `auth_type` advertising a second factor that
                // anyone holding the URL can compute.
                // Prefix only: this becomes `last_error`, which the runtime
                // logs and serves over its control API, and the operator will
                // fix the secret while keeping the id.
                return Err(Error::InvalidConfigValue(format!(
                    "endpoint {} declares auth_type {:?} but no non-empty auth_secret",
                    endpoint.endpoint_id.log_prefix(),
                    endpoint.auth_type
                )));
            }
        }
        Ok(())
    }
}

impl HttpSource {
    pub fn new(id: u32, config: HttpSourceConfig, state: Option<ConnectorState>) -> Self {
        // Clamped rather than rejected: `new` cannot fail, and an unchecked
        // value reaches crossfire before `open` runs. `validate` still rejects
        // it, so the operator learns through `last_error` instead of an abort.
        let buffer_capacity = config.buffer_capacity.clamp(1, MAX_BUFFER_CAPACITY);
        if buffer_capacity != config.buffer_capacity {
            warn!(
                "Clamped buffer_capacity {} to {buffer_capacity} for {CONNECTOR_NAME} connector ID: {id}",
                config.buffer_capacity
            );
        }
        // An unreadable registry cannot be served, but `new` has many callers and
        // the SDK builds the source before it calls `open`, so the failure is
        // carried to `open` rather than widening this signature. The empty
        // registry it starts from serves nothing if that ever changed.
        let (registry, restore_error) =
            match EndpointRegistry::restore(&config.endpoints, state, id) {
                Ok(registry) => (registry, None),
                Err(error) => (EndpointRegistry::default(), Some(error.to_string())),
            };
        let (sender, receiver) = crossfire::mpsc::bounded_async(buffer_capacity);
        let instance_name = config
            .instance_name
            .clone()
            .unwrap_or_else(|| id.to_string());
        // Entries that fail to resolve are rejected by `validate()` in
        // `open()`, so the instance never serves traffic with a silent gap.
        let forward_headers = config
            .forward_headers
            .iter()
            .filter_map(|header| {
                let name = HeaderName::from_str(header).ok()?;
                let key = HeaderKey::try_from(header.as_str()).ok()?;
                Some((name, key))
            })
            .collect();
        let shared = SharedState {
            id,
            config,
            sender,
            instance_name,
            forward_headers,
            registry: ArcSwap::from_pointee(registry),
            registry_dirty: AtomicBool::new(false),
            poll_active: AtomicBool::new(false),
            last_poll_at: AtomicU64::new(0),
            registry_writer: Mutex::new(()),
            state_flush: Notify::new(),
        };
        HttpSource {
            id,
            shared: Arc::new(shared),
            receiver: Mutex::new(receiver),
            staged: StdMutex::new(None),
            restore_error,
        }
    }

    /// A poisoned `staged` means a panic while a batch was held. The batch is
    /// still intact, and refusing to look at it would lose it for certain.
    fn lock_staged(&self) -> std::sync::MutexGuard<'_, Option<StagedBatch>> {
        self.staged.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Re-emits the unacknowledged batch, if one is waiting.
    fn staged_batch(&self) -> Option<ProducedMessages> {
        let staged = self.lock_staged();
        let staged = staged.as_ref()?;
        debug!(
            "Replaying {} unacknowledged messages after {} NACK(s) for {CONNECTOR_NAME} connector ID: {}",
            staged.messages.len(),
            staged.nacks,
            self.id
        );
        Some(ProducedMessages {
            schema: Schema::Raw,
            messages: staged.messages.iter().cloned().map(Into::into).collect(),
            // State rides an empty batch only, and a replay is never empty.
            state: None,
        })
    }

    /// Holds `queued` until the runtime acknowledges it and converts it into
    /// the batch to produce. The clone buys the ability to replay.
    fn stage(&self, queued: Vec<QueuedMessage>) -> Vec<ProducedMessage> {
        let messages: Vec<ProducedMessage> = queued.iter().cloned().map(Into::into).collect();
        if !queued.is_empty() {
            *self.lock_staged() = Some(StagedBatch {
                messages: queued,
                nacks: 0,
            });
        }
        messages
    }

    /// Keeps a batch the runtime could not deliver so the next `poll()` replays
    /// it. A batch is never abandoned here.
    ///
    /// Answering 200 told the sender this gateway owns the event, so the only
    /// honest way to shed load is the 429 the handlers return once the bridge
    /// fills, which senders retry. Dropping a staged batch instead would trade
    /// that bounded, visible backpressure for silent loss growing with the
    /// length of the outage, and no downstream can detect it.
    ///
    /// A batch that can never be delivered would replay forever, but this
    /// connector rejects malformed work at the door rather than mid-stream:
    /// oversized bodies get 413 before a handler runs, headers are clamped on
    /// accept, and `Schema::Raw` cannot fail to decode.
    fn on_nack(&self) -> Result<(), Error> {
        let mut staged = self.lock_staged();
        let Some(batch) = staged.as_mut() else {
            drop(staged);
            // Nothing staged means the batch carried no messages, which is
            // usually a state flush the runtime could not persist or would not
            // attempt. It can also be an empty batch that carried no state at
            // all, when the writer gate was contended or nothing was dirty, and
            // re-arming then costs one redundant flush rather than a lost one. `take_dirty_state` already cleared
            // the flag and marked every endpoint submitted, so re-arming here
            // is the only thing that stops a revocation being lost while the
            // API reports it durable. This is load-bearing, not defensive.
            self.shared.rearm_state_flush();
            warn!(
                "Runtime NACKed the state flush for {CONNECTOR_NAME} connector ID: {}, re-arming it",
                self.id
            );
            return Ok(());
        };
        batch.nacks += 1;
        let nacks = batch.nacks;
        let count = batch.messages.len();
        drop(staged);
        warn!(
            "Runtime NACKed {count} messages ({nacks} so far) for {CONNECTOR_NAME} connector ID: {}, replaying them on the next poll",
            self.id
        );
        Ok(())
    }
}

#[async_trait]
impl Source for HttpSource {
    async fn open(&mut self) -> Result<(), Error> {
        if let Some(error) = self.restore_error.take() {
            return Err(Error::InitError(error));
        }
        self.shared.config.validate()?;
        server::join(Arc::clone(&self.shared)).await?;
        self.shared.log_auth_posture();
        info!(
            "Opened {CONNECTOR_NAME} connector ID: {}, listen address: {}, endpoints: {}, named path: {:?}",
            self.id,
            self.shared.config.listen_addr,
            self.shared.registry().serving_count(unix_now_seconds()),
            self.shared.config.topic_path,
        );
        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        let _polling = self.shared.enter_poll();
        // An unacknowledged batch outranks new traffic: replaying it in order
        // is what turns the runtime's NACK into a retry instead of a gap.
        if let Some(staged) = self.staged_batch() {
            return Ok(staged);
        }

        let max_batch_size = self.shared.config.max_batch_size;
        // Empty until traffic actually arrives. Reserving up front cost a
        // `max_batch_size` allocation on every flush-only and idle poll, which
        // at the configurable ceiling is megabytes allocated and freed to carry
        // nothing.
        let mut queued: Vec<QueuedMessage> = Vec::new();
        let receiver = self.receiver.lock().await;
        tokio::select! {
            // The SDK races poll() against its own shutdown watch, so blocking
            // here until traffic arrives is what keeps an idle gateway off the
            // CPU. crossfire documents recv() as cancellation-safe.
            received = receiver.recv() => match received {
                Ok(message) => {
                    queued.push(message);
                    while queued.len() < max_batch_size {
                        let Ok(message) = receiver.try_recv() else {
                            break;
                        };
                        queued.push(message);
                    }
                }
                // Reachable only once every sender is gone. Idle rather than
                // spin the SDK's poll loop.
                Err(_) => tokio::time::sleep(IDLE_POLL_INTERVAL).await,
            },
            _ = self.shared.state_flush.notified() => {}
        }

        // State rides an empty batch and nothing else, so no failed publish
        // can skip its save. Under traffic that means deferring to a later
        // poll; re-arming the notify stops it waiting on traffic to arrive.
        let state = if queued.is_empty() {
            self.shared.take_dirty_state()
        } else {
            if self.shared.has_pending_state() {
                self.shared.state_flush.notify_one();
            }
            None
        };

        Ok(ProducedMessages {
            schema: Schema::Raw,
            messages: self.stage(queued),
            state,
        })
    }

    /// Applies the runtime's verdict on the batch `poll()` last produced.
    ///
    /// An Ack means the batch reached the topic and its state was persisted, so
    /// the staged copy is free. A Nack means neither happened, so the copy has
    /// to outlive it for the next `poll()` to replay.
    async fn on_batch_result(&self, result: source::SourceBatchResult) -> Result<(), Error> {
        match result {
            source::SourceBatchResult::Ack => {
                self.lock_staged().take();
                Ok(())
            }
            source::SourceBatchResult::Nack => self.on_nack(),
        }
    }

    async fn close(&mut self) -> Result<(), Error> {
        // Counted before `leave`, which is what folds it into
        // `http_source_dropped_on_close_total` alongside the bridge. Leaving it
        // to a log line under-reported shutdown loss by up to `max_batch_size`.
        let staged_dropped = match self.lock_staged().take() {
            Some(staged) => {
                warn!(
                    "Dropping {} unacknowledged messages for {CONNECTOR_NAME} connector ID: {}",
                    staged.messages.len(),
                    self.id
                );
                staged.messages.len() as u64
            }
            None => 0,
        };
        // The SDK stops the poll task before calling this, so anything still
        // in the bridge is already unreachable. Deregistering first is what
        // stops new requests from being accepted into a queue nobody drains.
        server::leave(&self.shared, staged_dropped).await;
        info!("Closed {CONNECTOR_NAME} connector ID: {}", self.id);
        Ok(())
    }
}

fn default_admin_listen_addr() -> String {
    DEFAULT_ADMIN_LISTEN_ADDR.to_string()
}

fn default_max_body_size_bytes() -> usize {
    DEFAULT_MAX_BODY_SIZE_BYTES
}

fn default_buffer_capacity() -> usize {
    DEFAULT_BUFFER_CAPACITY
}

fn default_max_batch_size() -> usize {
    DEFAULT_MAX_BATCH_SIZE
}

fn default_hmac_header() -> String {
    DEFAULT_HMAC_HEADER.to_string()
}

fn default_hmac_prefix() -> String {
    DEFAULT_HMAC_PREFIX.to_string()
}

fn default_true() -> bool {
    true
}

/// Fixtures shared by the routing and state test modules.
#[cfg(test)]
pub(crate) mod test_support {
    use super::*;

    pub const ENDPOINT_ONE: &str = "a3f8c2e1b9d04f7a8e6c1d2b3a4f5e6d";
    pub const ENDPOINT_TWO: &str = "b4092d3fa1c85e6b7d0f2a1c3e4b5d6a";

    pub fn endpoint_id(raw_id: &str) -> EndpointId {
        raw_id.parse().expect("test endpoint id must be valid")
    }

    pub fn static_endpoint(raw_id: &str) -> StaticEndpointConfig {
        StaticEndpointConfig {
            endpoint_id: endpoint_id(raw_id),
            auth_type: EndpointAuthType::HmacSha256,
            auth_secret: Some(SecretString::from("whsec_static")),
            hmac_header: DEFAULT_HMAC_HEADER.to_string(),
            hmac_prefix: DEFAULT_HMAC_PREFIX.to_string(),
            expires_at: None,
        }
    }

    pub fn config(topic_path: Option<&str>, endpoint_ids: &[&str]) -> HttpSourceConfig {
        HttpSourceConfig {
            listen_addr: "127.0.0.1:9090".to_string(),
            admin_listen_addr: DEFAULT_ADMIN_LISTEN_ADDR.to_string(),
            max_body_size_bytes: DEFAULT_MAX_BODY_SIZE_BYTES,
            buffer_capacity: DEFAULT_BUFFER_CAPACITY,
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
            topic_path: topic_path.map(str::to_string),
            instance_name: None,
            auth_bearer_token: None,
            management_token: None,
            include_http_metadata: true,
            forward_headers: Vec::new(),
            endpoints: endpoint_ids
                .iter()
                .map(|raw_id| static_endpoint(raw_id))
                .collect(),
        }
    }

    /// Reserves an ephemeral port and hands it back. Every test that binds
    /// needs its own, because the listener registry is process-global and the
    /// test binary runs its cases in parallel.
    pub fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .expect("the loopback interface must offer a port")
            .local_addr()
            .expect("a bound listener has an address")
            .port()
    }

    /// No connection pooling: an idle keep-alive socket would hold graceful
    /// shutdown open until the timeout and slow every teardown to a crawl.
    pub fn client() -> reqwest::Client {
        reqwest::Client::builder()
            .pool_max_idle_per_host(0)
            .build()
            .expect("the test client must build")
    }

    /// Builds one instance's shared state through the real constructor. The
    /// source itself is dropped, taking the bridge receiver with it: routing
    /// and state tests resolve and mutate, they never send.
    pub fn instance(id: u32, topic_path: Option<&str>, endpoint_ids: &[&str]) -> Arc<SharedState> {
        let source = HttpSource::new(id, config(topic_path, endpoint_ids), None);
        Arc::clone(&source.shared)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{ENDPOINT_ONE, ENDPOINT_TWO};
    use iggy_connector_sdk::source::SourceBatchResult;

    fn minimal_config_json() -> &'static str {
        r#"{"listen_addr": "0.0.0.0:9090"}"#
    }

    fn parse(config_json: &str) -> HttpSourceConfig {
        serde_json::from_str(config_json).expect("config must deserialize")
    }

    #[test]
    fn given_minimal_config_when_deserialized_should_apply_defaults() {
        let config = parse(minimal_config_json());
        assert_eq!(config.admin_listen_addr, DEFAULT_ADMIN_LISTEN_ADDR);
        assert_eq!(config.max_body_size_bytes, DEFAULT_MAX_BODY_SIZE_BYTES);
        assert_eq!(config.buffer_capacity, DEFAULT_BUFFER_CAPACITY);
        assert_eq!(config.max_batch_size, DEFAULT_MAX_BATCH_SIZE);
        assert!(config.include_http_metadata);
        assert!(config.topic_path.is_none());
        assert!(config.auth_bearer_token.is_none());
        assert!(config.management_token.is_none());
        assert!(config.forward_headers.is_empty());
        assert!(config.endpoints.is_empty());
    }

    #[test]
    fn given_minimal_config_when_validated_should_accept() {
        assert!(parse(minimal_config_json()).validate().is_ok());
    }

    #[test]
    fn given_unparsable_listen_addr_when_validated_should_reject() {
        let config = parse(r#"{"listen_addr": "not-an-address"}"#);
        assert!(matches!(
            config.validate(),
            Err(Error::InvalidConfigValue(message)) if message.contains("listen_addr")
        ));
    }

    #[test]
    fn given_zero_buffer_capacity_when_validated_should_reject() {
        let config = parse(r#"{"listen_addr": "0.0.0.0:9090", "buffer_capacity": 0}"#);
        assert!(matches!(
            config.validate(),
            Err(Error::InvalidConfigValue(message)) if message.contains("buffer_capacity")
        ));
    }

    #[test]
    fn given_oversized_sizing_values_when_validated_should_reject() {
        for (field, json) in [
            (
                "buffer_capacity",
                r#"{"listen_addr": "0.0.0.0:9090", "buffer_capacity": 1000001}"#,
            ),
            (
                "max_batch_size",
                r#"{"listen_addr": "0.0.0.0:9090", "max_batch_size": 100001}"#,
            ),
            (
                "max_body_size_bytes",
                r#"{"listen_addr": "0.0.0.0:9090", "max_body_size_bytes": 67108865}"#,
            ),
        ] {
            assert!(
                matches!(
                    parse(json).validate(),
                    Err(Error::InvalidConfigValue(message)) if message.contains(field)
                ),
                "{field} had a floor and no ceiling"
            );
        }
    }

    #[test]
    fn given_unbuildable_buffer_capacity_when_constructed_should_clamp_not_panic() {
        let mut config = test_support::config(None, &[]);
        config.buffer_capacity = usize::MAX;

        // `new` builds the crossfire ring, and crossfire asserts on a capacity
        // this large. That panic would unwind out of `iggy_source_open`, which
        // is `extern "C"` with no `catch_unwind`, aborting the whole connectors
        // process. The SDK builds the source before it calls `open`, so
        // `validate` is not what stands between a typo and that abort.
        let source = HttpSource::new(1, config, None);

        assert!(
            source.shared.sender.try_send(queued("one")).is_ok(),
            "the clamped ring must still be usable"
        );
        assert!(
            matches!(
                source.shared.config.validate(),
                Err(Error::InvalidConfigValue(message)) if message.contains("buffer_capacity")
            ),
            "and open() must still report the misconfiguration rather than serve it"
        );
    }

    #[test]
    fn given_reserved_instance_name_when_validated_should_reject() {
        let config = parse(r#"{"listen_addr": "0.0.0.0:9090", "instance_name": "unrouted"}"#);
        assert!(
            matches!(
                config.validate(),
                Err(Error::InvalidConfigValue(message)) if message.contains("reserved")
            ),
            "requests matching no route are metered under this name, and that series is the endpoint-id scan signal"
        );
    }

    #[test]
    fn given_duplicate_endpoint_id_when_validated_should_reject() {
        let config = parse(
            r#"{"listen_addr": "0.0.0.0:9090", "endpoints": [
                {"endpoint_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "auth_type": "none"},
                {"endpoint_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "auth_type": "none"}
            ]}"#,
        );
        assert!(
            matches!(
                config.validate(),
                Err(Error::InvalidConfigValue(message)) if message.contains("more than once")
            ),
            "the registry is a map, so the second block would silently last-win"
        );
    }

    #[test]
    fn given_invalid_hmac_header_when_validated_should_reject() {
        let config = parse(
            r#"{"listen_addr": "0.0.0.0:9090", "endpoints": [
                {"endpoint_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "auth_type": "hmac-sha256",
                 "auth_secret": "whsec", "hmac_header": "not a header"}
            ]}"#,
        );
        assert!(
            matches!(
                config.validate(),
                Err(Error::InvalidConfigValue(message)) if message.contains("hmac_header")
            ),
            "an invalid name makes HeaderMap::get return None, so every signed request 401s forever"
        );
    }

    #[test]
    fn given_topic_path_with_slash_when_validated_should_reject() {
        let config = parse(r#"{"listen_addr": "0.0.0.0:9090", "topic_path": "a/b"}"#);
        assert!(matches!(
            config.validate(),
            Err(Error::InvalidConfigValue(message)) if message.contains("topic_path")
        ));
    }

    #[test]
    fn given_endpoint_with_auth_but_no_secret_when_validated_should_reject() {
        let config = parse(
            r#"{
                "listen_addr": "0.0.0.0:9090",
                "endpoints": [{
                    "endpoint_id": "a3f8c2e1b9d04f7a8e6c1d2b3a4f5e6d",
                    "auth_type": "hmac-sha256"
                }]
            }"#,
        );
        assert!(matches!(
            config.validate(),
            Err(Error::InvalidConfigValue(message)) if message.contains("auth_secret")
        ));
    }

    #[test]
    fn given_endpoint_config_when_deserialized_should_apply_hmac_defaults() {
        let config = parse(
            r#"{
                "listen_addr": "0.0.0.0:9090",
                "endpoints": [{
                    "endpoint_id": "a3f8c2e1b9d04f7a8e6c1d2b3a4f5e6d",
                    "auth_type": "hmac-sha256",
                    "auth_secret": "whsec_test"
                }]
            }"#,
        );
        let endpoint = &config.endpoints[0];
        assert_eq!(endpoint.hmac_header, DEFAULT_HMAC_HEADER);
        assert_eq!(endpoint.hmac_prefix, DEFAULT_HMAC_PREFIX);
        assert_eq!(
            endpoint.auth_type.hmac_algorithm(),
            Some(HmacAlgorithm::HmacSha256)
        );
        assert!(config.validate().is_ok());
    }

    #[test]
    fn given_zero_max_batch_size_when_validated_should_reject() {
        let config = parse(r#"{"listen_addr": "0.0.0.0:9090", "max_batch_size": 0}"#);
        assert!(matches!(
            config.validate(),
            Err(Error::InvalidConfigValue(message)) if message.contains("max_batch_size")
        ));
    }

    #[test]
    fn given_zero_max_body_size_when_validated_should_reject() {
        let config = parse(r#"{"listen_addr": "0.0.0.0:9090", "max_body_size_bytes": 0}"#);
        assert!(matches!(
            config.validate(),
            Err(Error::InvalidConfigValue(message)) if message.contains("max_body_size_bytes")
        ));
    }

    #[test]
    fn given_unparsable_admin_addr_when_validated_should_reject() {
        let config = parse(r#"{"listen_addr": "0.0.0.0:9090", "admin_listen_addr": "nope"}"#);
        assert!(matches!(
            config.validate(),
            Err(Error::InvalidConfigValue(message)) if message.contains("admin_listen_addr")
        ));
    }

    #[test]
    fn given_empty_instance_name_when_validated_should_reject() {
        let config = parse(r#"{"listen_addr": "0.0.0.0:9090", "instance_name": ""}"#);
        assert!(matches!(
            config.validate(),
            Err(Error::InvalidConfigValue(message)) if message.contains("instance_name")
        ));
    }

    #[test]
    fn given_oversized_instance_name_when_validated_should_reject() {
        // It becomes a HeaderKey, and Iggy caps those at 255 bytes. Without
        // this check the identity header would vanish from every message.
        let long_name = "n".repeat(256);
        let config = parse(&format!(
            r#"{{"listen_addr": "0.0.0.0:9090", "instance_name": "{long_name}"}}"#
        ));
        assert!(matches!(
            config.validate(),
            Err(Error::InvalidConfigValue(message)) if message.contains("instance_name")
        ));
    }

    #[test]
    fn given_invalid_forward_header_when_validated_should_reject() {
        // `new()` silently filters entries that fail to resolve, on the stated
        // assumption that validate() rejects them first. That is the test.
        let config =
            parse(r#"{"listen_addr": "0.0.0.0:9090", "forward_headers": ["not a header"]}"#);
        assert!(matches!(
            config.validate(),
            Err(Error::InvalidConfigValue(message)) if message.contains("forward_headers")
        ));
    }

    #[test]
    fn given_empty_endpoint_secret_when_validated_should_reject() {
        // An empty key is valid for HMAC, so accepting one would advertise a
        // second factor that anyone holding the URL can compute.
        let config = parse(
            r#"{
                "listen_addr": "0.0.0.0:9090",
                "endpoints": [{
                    "endpoint_id": "a3f8c2e1b9d04f7a8e6c1d2b3a4f5e6d",
                    "auth_type": "hmac-sha256",
                    "auth_secret": ""
                }]
            }"#,
        );
        assert!(matches!(
            config.validate(),
            Err(Error::InvalidConfigValue(message)) if message.contains("auth_secret")
        ));
    }

    #[test]
    fn given_empty_management_token_when_validated_should_reject() {
        let config = parse(r#"{"listen_addr": "0.0.0.0:9090", "management_token": ""}"#);
        assert!(matches!(
            config.validate(),
            Err(Error::InvalidConfigValue(message)) if message.contains("management_token")
        ));
    }

    #[test]
    fn given_credential_forward_header_when_validated_should_reject() {
        for header in ["Authorization", "cookie", "PROXY-AUTHORIZATION"] {
            let config = parse(&format!(
                r#"{{"listen_addr": "0.0.0.0:9090", "forward_headers": ["{header}"]}}"#
            ));
            assert!(
                matches!(
                    config.validate(),
                    Err(Error::InvalidConfigValue(message)) if message.contains("credential")
                ),
                "{header} would be copied onto every message and persisted in the log"
            );
        }
    }

    #[test]
    fn given_reserved_metadata_forward_header_when_validated_should_reject() {
        // `message_headers` inserts the metadata first and forwarded headers
        // into the same map, so without this the sender picks the value the
        // pipeline trusts. Mixed case because HTTP header names are
        // case-insensitive and the sender chooses the casing.
        for header in [
            INSTANCE_HEADER,
            REMOTE_ADDR_HEADER,
            RECEIVED_AT_HEADER,
            "IGGY_HTTP_REMOTE_ADDR",
        ] {
            let config = parse(&format!(
                r#"{{"listen_addr": "0.0.0.0:9090", "forward_headers": ["{header}"]}}"#
            ));
            assert!(
                matches!(
                    config.validate(),
                    Err(Error::InvalidConfigValue(message)) if message.contains("reserved")
                ),
                "{header} would let the sender overwrite the gateway's own metadata"
            );
        }
    }

    #[test]
    fn given_oversized_forward_header_when_validated_should_reject() {
        // Valid as an HTTP name, too long to become a HeaderKey. Rejected here
        // rather than dropped later, so the operator learns the header they
        // asked to forward would never have ridden along.
        let long_header = "x".repeat(256);
        let config = parse(&format!(
            r#"{{"listen_addr": "0.0.0.0:9090", "forward_headers": ["{long_header}"]}}"#
        ));
        assert!(matches!(
            config.validate(),
            Err(Error::InvalidConfigValue(message)) if message.contains("Iggy header key")
        ));
    }

    #[test]
    fn given_auth_types_when_asked_for_an_algorithm_should_map_each_exactly_once() {
        // A swapped arm would validate SHA-1 signatures with SHA-256 and
        // reject every request the sender signs correctly.
        assert_eq!(
            EndpointAuthType::HmacSha256.hmac_algorithm(),
            Some(HmacAlgorithm::HmacSha256)
        );
        assert_eq!(
            EndpointAuthType::HmacSha1.hmac_algorithm(),
            Some(HmacAlgorithm::HmacSha1)
        );
        assert_eq!(EndpointAuthType::Bearer.hmac_algorithm(), None);
        assert_eq!(EndpointAuthType::None.hmac_algorithm(), None);
    }

    #[test]
    fn given_invalid_endpoint_id_when_deserialized_should_reject() {
        let result = serde_json::from_str::<HttpSourceConfig>(
            r#"{
                "listen_addr": "0.0.0.0:9090",
                "endpoints": [{"endpoint_id": "too-short"}]
            }"#,
        );
        assert!(result.is_err(), "invalid endpoint_id must fail at parse");
    }

    /// The shipped configurations are documentation, and documentation that
    /// no longer deserializes is worse than none.
    #[test]
    fn given_shipped_configs_when_parsed_should_deserialize_and_validate() {
        for (name, raw) in [
            ("config.toml", include_str!("../config.toml")),
            (
                "example_config/http_source_github.toml",
                include_str!("../../../runtime/example_config/connectors/http_source_github.toml"),
            ),
            (
                "example_config/http_source_partner.toml",
                include_str!("../../../runtime/example_config/connectors/http_source_partner.toml"),
            ),
        ] {
            let document: toml::Value =
                toml::from_str(raw).unwrap_or_else(|error| panic!("{name} must parse: {error}"));
            let plugin_config = document
                .get("plugin_config")
                .unwrap_or_else(|| panic!("{name} must carry a plugin_config table"))
                .clone();
            let config: HttpSourceConfig = plugin_config
                .try_into()
                .unwrap_or_else(|error| panic!("{name} plugin_config must deserialize: {error}"));
            config
                .validate()
                .unwrap_or_else(|error| panic!("{name} must pass validation: {error}"));
        }
    }

    fn queued(payload: &str) -> QueuedMessage {
        QueuedMessage {
            payload: payload.as_bytes().to_vec(),
            headers: None,
        }
    }

    #[tokio::test]
    async fn given_persisted_state_when_constructed_should_restore_into_the_shared_registry() {
        let mut persisted = EndpointRegistry::default();
        assert!(persisted.insert(crate::routes::Endpoint {
            endpoint_id: test_support::endpoint_id(ENDPOINT_TWO),
            auth_type: EndpointAuthType::Bearer,
            auth_secret: Some(SecretString::from("whsec_dynamic")),
            hmac_header: DEFAULT_HMAC_HEADER.to_string(),
            hmac_prefix: DEFAULT_HMAC_PREFIX.to_string(),
            expires_at: None,
            origin: crate::routes::EndpointOrigin::Dynamic,
            state: crate::routes::EndpointState::Active,
            submitted: false,
        }));

        let source = HttpSource::new(
            1,
            test_support::config(None, &[ENDPOINT_ONE]),
            persisted.to_connector_state(1),
        );

        let registry = source.shared.registry();
        assert!(
            registry.endpoint(ENDPOINT_TWO).is_some(),
            "the constructor must hand persisted state to the registry, not drop it"
        );
        assert!(registry.endpoint(ENDPOINT_ONE).is_some());
    }

    #[tokio::test]
    async fn given_static_only_instance_when_state_taken_should_stay_none() {
        let source = HttpSource::new(1, test_support::config(None, &[ENDPOINT_ONE]), None);

        assert!(
            source.shared.take_dirty_state().is_none(),
            "a static-only instance must behave like a stateless source"
        );
    }

    #[tokio::test]
    async fn given_registry_mutation_when_state_taken_should_return_it_once() {
        let source = HttpSource::new(1, test_support::config(None, &[ENDPOINT_ONE]), None);
        source
            .shared
            .mutate_registry(|registry| registry.revoke(ENDPOINT_ONE, "rotated".to_string(), 42))
            .await;

        assert!(source.shared.take_dirty_state().is_some());
        assert!(
            source.shared.take_dirty_state().is_none(),
            "an unchanged registry must not be rewritten every poll"
        );
    }

    #[tokio::test]
    async fn given_state_taken_when_registry_read_should_report_endpoints_submitted() {
        let source = HttpSource::new(1, test_support::config(None, &[ENDPOINT_ONE]), None);
        source
            .shared
            .mutate_registry(|registry| registry.revoke(ENDPOINT_ONE, "rotated".to_string(), 42))
            .await;

        assert!(
            !source
                .shared
                .registry()
                .endpoint(ENDPOINT_ONE)
                .expect("endpoint must exist")
                .submitted
        );
        source.shared.take_dirty_state();
        assert!(
            source
                .shared
                .registry()
                .endpoint(ENDPOINT_ONE)
                .expect("endpoint must exist")
                .submitted
        );
    }

    #[tokio::test]
    async fn given_pending_mutation_when_polled_should_flush_state_on_an_empty_batch() {
        let source = HttpSource::new(1, test_support::config(None, &[ENDPOINT_ONE]), None);
        source
            .shared
            .mutate_registry(|registry| registry.revoke(ENDPOINT_ONE, "rotated".to_string(), 42))
            .await;

        let produced = source.poll().await.expect("poll must succeed");

        assert!(produced.messages.is_empty());
        assert!(
            produced.state.is_some(),
            "a mutation must reach the runtime without waiting for traffic"
        );
    }

    #[tokio::test]
    async fn given_pending_mutation_and_traffic_when_polled_should_defer_the_flush() {
        let mut config = test_support::config(None, &[ENDPOINT_ONE]);
        config.max_batch_size = 1;
        let source = HttpSource::new(1, config, None);
        source
            .shared
            .mutate_registry(|registry| registry.revoke(ENDPOINT_ONE, "rotated".to_string(), 42))
            .await;
        source
            .shared
            .sender
            .try_send(queued("one"))
            .expect("bridge must accept");

        // Consume the permit the mutation armed, so `select!` has exactly one
        // ready branch and the batch-carrying poll is deterministic. Asserting
        // across both polls instead would only catch the regression when the
        // random branch order happened to cooperate.
        source.shared.state_flush.notified().await;

        let carried = source.poll().await.expect("poll must succeed");
        assert_eq!(carried.messages.len(), 1);
        assert!(
            carried.state.is_none(),
            "state must never ride a batch whose send can fail, or a failed send loses it silently"
        );
        assert!(
            source.shared.has_pending_state(),
            "and the deferred flush must still be armed"
        );

        source
            .on_batch_result(SourceBatchResult::Ack)
            .await
            .expect("ack must succeed");

        let flushed = source.poll().await.expect("poll must succeed");
        assert!(flushed.messages.is_empty());
        assert!(
            flushed.state.is_some(),
            "the re-arm must carry it without waiting for further traffic"
        );
    }

    #[tokio::test]
    async fn given_queued_messages_when_polled_should_drain_up_to_max_batch_size() {
        let mut config = test_support::config(None, &[]);
        config.max_batch_size = 2;
        let source = HttpSource::new(1, config, None);
        for payload in ["one", "two", "three"] {
            source
                .shared
                .sender
                .try_send(queued(payload))
                .expect("bridge must accept within capacity");
        }

        let first = source.poll().await.expect("poll must succeed");
        // Without the Ack the runtime never confirmed the batch, so the next
        // poll owes a replay rather than fresh traffic.
        source
            .on_batch_result(SourceBatchResult::Ack)
            .await
            .expect("ack must succeed");
        let second = source.poll().await.expect("poll must succeed");

        assert_eq!(first.messages.len(), 2);
        assert_eq!(second.messages.len(), 1);
        assert_eq!(first.messages[0].payload, b"one");
        assert!(matches!(first.schema, Schema::Raw));
        assert!(first.state.is_none());
    }

    #[tokio::test]
    async fn given_nacked_batch_when_polled_should_replay_the_same_messages() {
        let mut config = test_support::config(None, &[]);
        config.max_batch_size = 2;
        let source = HttpSource::new(1, config, None);
        for payload in ["one", "two", "three"] {
            source
                .shared
                .sender
                .try_send(queued(payload))
                .expect("bridge must accept within capacity");
        }

        let first = source.poll().await.expect("poll must succeed");
        source
            .on_batch_result(SourceBatchResult::Nack)
            .await
            .expect("nack must succeed");
        let replayed = source.poll().await.expect("poll must succeed");

        assert_eq!(
            replayed.messages.len(),
            first.messages.len(),
            "the drain is destructive, so a NACK the source cannot replay is lost data"
        );
        assert_eq!(replayed.messages[0].payload, b"one");
        assert_eq!(replayed.messages[1].payload, b"two");
        assert!(
            replayed.state.is_none(),
            "a replay carries messages only, never state"
        );
    }

    #[tokio::test]
    async fn given_many_nacks_when_polled_should_never_abandon_the_batch() {
        let mut config = test_support::config(None, &[]);
        config.max_batch_size = 1;
        let source = HttpSource::new(1, config, None);
        source
            .shared
            .sender
            .try_send(queued("one"))
            .expect("bridge must accept");

        source.poll().await.expect("poll must succeed");
        for _ in 0..20 {
            source
                .on_batch_result(SourceBatchResult::Nack)
                .await
                .expect("nack must succeed");
            let replayed = source.poll().await.expect("poll must succeed");
            assert_eq!(
                replayed.messages[0].payload, b"one",
                "the sender already has a 200 for this event, so dropping it is never an option"
            );
        }
    }

    #[tokio::test]
    async fn given_replayed_batch_when_acked_should_resume_new_traffic() {
        let mut config = test_support::config(None, &[]);
        config.max_batch_size = 2;
        let source = HttpSource::new(1, config, None);
        for payload in ["one", "two", "three"] {
            source
                .shared
                .sender
                .try_send(queued(payload))
                .expect("bridge must accept within capacity");
        }

        source.poll().await.expect("poll must succeed");
        source
            .on_batch_result(SourceBatchResult::Nack)
            .await
            .expect("nack must succeed");
        source.poll().await.expect("poll must succeed");
        source
            .on_batch_result(SourceBatchResult::Ack)
            .await
            .expect("ack must succeed");

        let fresh = source.poll().await.expect("poll must succeed");
        assert_eq!(fresh.messages.len(), 1);
        assert_eq!(
            fresh.messages[0].payload, b"three",
            "the acked replay must release the bridge, not repeat itself"
        );
    }

    #[tokio::test]
    async fn given_contended_writer_when_state_taken_should_not_wake_itself_for_nothing() {
        // The re-post exists so a poll that consumed a mutation's permit does
        // not sleep on a flush nobody will carry. With no flush owed there is
        // nothing to carry, and waking anyway spins the poll task through an
        // FFI round trip per iteration.
        let source = HttpSource::new(1, test_support::config(None, &[]), None);
        let held = source.shared.registry_writer.lock().await;

        assert!(source.shared.take_dirty_state().is_none());
        drop(held);

        assert!(
            tokio::time::timeout(
                Duration::from_millis(50),
                source.shared.state_flush.notified()
            )
            .await
            .is_err(),
            "no flush was owed, so no permit may be waiting"
        );
    }

    #[tokio::test]
    async fn given_state_only_batch_when_nacked_should_rearm_the_flush() {
        let config = test_support::config(None, &[ENDPOINT_ONE]);
        let source = HttpSource::new(1, config, None);
        source
            .shared
            .mutate_registry(|registry| registry.revoke(ENDPOINT_ONE, "rotated".to_string(), 42))
            .await;

        let flushed = source.poll().await.expect("poll must succeed");
        assert!(flushed.messages.is_empty());
        assert!(flushed.state.is_some(), "the mutation must arm a flush");
        assert!(
            !source.shared.has_pending_state(),
            "and handing it out must clear the flag"
        );

        source
            .on_batch_result(SourceBatchResult::Nack)
            .await
            .expect("nack must succeed");

        assert!(
            source.shared.has_pending_state(),
            "a NACKed state flush is a revocation the runtime never persisted, and `take_dirty_state` already marked it submitted, so without the re-arm it is lost for good"
        );

        // Consume the permit the re-arm posted, so `select!` has exactly one
        // ready branch and the traffic poll below is deterministic.
        source.shared.state_flush.notified().await;
        source
            .shared
            .sender
            .try_send(queued("one"))
            .expect("bridge must accept");

        let next = source.poll().await.expect("poll must succeed");
        assert_eq!(
            next.messages[0].payload, b"one",
            "and an empty batch stages nothing, so its NACK must not wedge the poll loop"
        );
    }

    #[test]
    fn given_poll_states_when_liveness_checked_should_separate_running_from_stopped() {
        let source = HttpSource::new(1, test_support::config(None, &[]), None);
        let now = 1_000_000;

        assert!(
            !source.shared.poll_is_live(now),
            "a source that has never polled is not live"
        );

        let polling = source.shared.enter_poll();
        assert!(
            source.shared.poll_is_live(now),
            "an in-flight poll counts even when it has been blocked on an empty bridge for hours"
        );

        drop(polling);
        let returned_at = unix_now_seconds();
        assert!(
            source.shared.poll_is_live(returned_at),
            "and the gap between polls, where the SDK awaits a batch result, still counts"
        );
        assert!(
            !source
                .shared
                .poll_is_live(returned_at + POLL_LIVENESS_SECONDS + 1),
            "but a poll task the SDK stopped after five NACKs eventually goes stale, which is what stops readiness reporting ok"
        );
    }

    #[tokio::test]
    async fn given_unreadable_state_when_opened_should_fail_before_binding() {
        let mut source = HttpSource::new(
            1,
            test_support::config(None, &[ENDPOINT_ONE]),
            Some(ConnectorState(b"not valid msgpack".to_vec())),
        );

        let opened = source.open().await;

        assert!(
            matches!(opened, Err(Error::InitError(_))),
            "an unreadable registry has lost every tombstone, so open must fail rather than serve the static config, and it must fail before the listener binds"
        );
    }

    #[tokio::test]
    async fn given_full_bridge_when_message_sent_should_reject() {
        let mut config = test_support::config(None, &[]);
        // Two, not one: at 1 the ring is degenerate, so a test cannot tell a
        // real capacity bound from an off-by-one. `bounded_async` always builds
        // an `Array` whatever the size, so the flavour is not what differs.
        config.buffer_capacity = 2;
        let source = HttpSource::new(1, config, None);

        assert!(source.shared.sender.try_send(queued("one")).is_ok());
        assert!(source.shared.sender.try_send(queued("two")).is_ok());
        assert!(
            source.shared.sender.try_send(queued("three")).is_err(),
            "a full bridge is what turns into a 429"
        );
    }

    // Shutdown drains whatever the handlers already accepted, and crossfire's
    // docs stop short of promising it.
    #[tokio::test]
    async fn given_dropped_senders_when_received_should_drain_buffered_messages() {
        let (sender, receiver) = crossfire::mpsc::bounded_async::<QueuedMessage>(4);
        sender.try_send(queued("one")).expect("capacity is free");
        sender.try_send(queued("two")).expect("capacity is free");
        drop(sender);

        assert_eq!(receiver.recv().await.expect("buffered").payload, b"one");
        assert_eq!(receiver.recv().await.expect("buffered").payload, b"two");
        assert!(receiver.recv().await.is_err(), "then the bridge is closed");
    }
}
