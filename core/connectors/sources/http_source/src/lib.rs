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

pub mod auth;
pub mod management;
pub mod metrics;
pub mod routes;
pub mod server;
pub mod state;
pub mod types;

use arc_swap::{ArcSwap, Guard};
use async_trait::async_trait;
use axum::http::HeaderName;
use iggy_common::HeaderKey;
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessages, Schema, Source, source_connector,
};
use secrecy::{ExposeSecret, SecretString};
use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::sync::{Mutex, Notify};
use tracing::{debug, info};

use crate::auth::HmacAlgorithm;
use crate::state::EndpointRegistry;
use crate::types::{EndpointId, QueuedMessage, unix_now_seconds};

pub const CONNECTOR_NAME: &str = "HTTP source";

pub const DEFAULT_ADMIN_LISTEN_ADDR: &str = "127.0.0.1:9091";
pub const DEFAULT_MAX_BODY_SIZE_BYTES: usize = 1024 * 1024;
pub const DEFAULT_BUFFER_CAPACITY: usize = 10_000;
pub const DEFAULT_MAX_BATCH_SIZE: usize = 500;
pub const DEFAULT_HMAC_HEADER: &str = "X-Hub-Signature-256";
pub const DEFAULT_HMAC_PREFIX: &str = "sha256=";

const IDLE_POLL_INTERVAL: Duration = Duration::from_millis(100);

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
    /// Serializes registry writers, which are all control-plane. The request
    /// path only ever loads the `ArcSwap` and never touches this.
    registry_writer: Mutex<()>,
    /// Wakes `poll()` when a management mutation needs a state flush and no
    /// webhook traffic would otherwise arrive to carry it.
    pub state_flush: Notify,
}

impl SharedState {
    /// Wait-free snapshot of the registry, one atomic load per request.
    pub fn registry(&self) -> Guard<Arc<EndpointRegistry>> {
        self.registry.load()
    }

    /// Applies a control-plane mutation and arms the next state flush.
    pub async fn mutate_registry<R>(&self, mutation: impl FnOnce(&mut EndpointRegistry) -> R) -> R {
        let outcome = {
            let _writer = self.registry_writer.lock().await;
            let mut next = EndpointRegistry::clone(&self.registry.load());
            let outcome = mutation(&mut next);
            self.registry.store(Arc::new(next));
            self.registry_dirty.store(true, Ordering::Release);
            outcome
        };
        // Notified after the gate is free, so the woken poll finds it open
        // rather than bouncing off `try_lock` and relying on the re-arm there.
        self.state_flush.notify_one();
        outcome
    }

    /// Whether a mutation is still waiting to be handed to the runtime.
    pub fn has_pending_state(&self) -> bool {
        self.registry_dirty.load(Ordering::Acquire)
    }

    /// Hands the registry to the runtime for persistence, once per mutation.
    ///
    /// Only ever called for an empty batch. The runtime saves state solely on
    /// the success branch of the Iggy send, and an empty send always succeeds,
    /// so attaching state to a batch that could fail would let the save be
    /// skipped while this side had already cleared the flag and marked the
    /// registry submitted - losing a revocation tombstone with no trace.
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
            // have just consumed it, so re-arm rather than sleep on a flush
            // that no further traffic would ever carry.
            self.state_flush.notify_one();
            return None;
        };
        if !self.registry_dirty.swap(false, Ordering::AcqRel) {
            return None;
        }
        let snapshot = self.registry.load_full();
        let Some(state) = snapshot.to_connector_state(self.id) else {
            // Serialization logged the cause. Re-arm and re-notify, or the
            // retry this promises would wait for unrelated traffic.
            self.registry_dirty.store(true, Ordering::Release);
            self.state_flush.notify_one();
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
    #[serde(default)]
    pub verbose_logging: Option<bool>,
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
    /// Unix seconds; requests arriving at or after this answer 410 Gone.
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
        if self.buffer_capacity == 0 {
            return Err(Error::InvalidConfigValue(
                "buffer_capacity must be at least 1".to_string(),
            ));
        }
        if self.max_batch_size == 0 {
            return Err(Error::InvalidConfigValue(
                "max_batch_size must be at least 1".to_string(),
            ));
        }
        if self.max_body_size_bytes == 0 {
            return Err(Error::InvalidConfigValue(
                "max_body_size_bytes must be at least 1".to_string(),
            ));
        }
        if let Some(instance_name) = &self.instance_name
            && HeaderKey::try_from(instance_name.as_str()).is_err()
        {
            return Err(Error::InvalidConfigValue(format!(
                "instance_name '{instance_name}' must be a valid Iggy header key: non-empty and at most 255 bytes"
            )));
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
        for endpoint in &self.endpoints {
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
        let registry = EndpointRegistry::restore(&config.endpoints, state, id);
        let (sender, receiver) = crossfire::mpsc::bounded_async(config.buffer_capacity);
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
            registry_writer: Mutex::new(()),
            state_flush: Notify::new(),
        };
        HttpSource {
            id,
            shared: Arc::new(shared),
            receiver: Mutex::new(receiver),
        }
    }

    pub fn shared(&self) -> &Arc<SharedState> {
        &self.shared
    }
}

#[async_trait]
impl Source for HttpSource {
    async fn open(&mut self) -> Result<(), Error> {
        self.shared.config.validate()?;
        server::join(Arc::clone(&self.shared)).await?;
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
        let max_batch_size = self.shared.config.max_batch_size;
        let mut messages = Vec::with_capacity(max_batch_size);
        let receiver = self.receiver.lock().await;
        tokio::select! {
            // The SDK races poll() against its own shutdown watch, so blocking
            // here until traffic arrives is what keeps an idle gateway off the
            // CPU. crossfire documents recv() as cancellation-safe.
            received = receiver.recv() => match received {
                Ok(message) => {
                    messages.push(message.into());
                    while messages.len() < max_batch_size {
                        let Ok(message) = receiver.try_recv() else {
                            break;
                        };
                        messages.push(message.into());
                    }
                }
                // Reachable only once every sender is gone. Idle rather than
                // spin the SDK's poll loop.
                Err(_) => tokio::time::sleep(IDLE_POLL_INTERVAL).await,
            },
            _ = self.shared.state_flush.notified() => {}
        }

        if !messages.is_empty() {
            let count = messages.len();
            if self.shared.config.verbose_logging.unwrap_or(false) {
                info!(
                    "Polled {count} messages for {CONNECTOR_NAME} connector ID: {}",
                    self.id
                );
            } else {
                debug!(
                    "Polled {count} messages for {CONNECTOR_NAME} connector ID: {}",
                    self.id
                );
            }
        }

        // State rides an empty batch and nothing else, so the send it depends
        // on cannot fail. Under traffic that means deferring to a later poll;
        // re-arming the notify is what stops it waiting on traffic to arrive.
        let state = if messages.is_empty() {
            self.shared.take_dirty_state()
        } else {
            if self.shared.has_pending_state() {
                self.shared.state_flush.notify_one();
            }
            None
        };

        Ok(ProducedMessages {
            schema: Schema::Raw,
            messages,
            state,
        })
    }

    async fn close(&mut self) -> Result<(), Error> {
        // The SDK stops the poll task before calling this, so anything still
        // in the bridge is already unreachable. Deregistering first is what
        // stops new requests from being accepted into a queue nobody drains.
        server::leave(&self.shared).await;
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
            verbose_logging: None,
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
        Arc::clone(source.shared())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{ENDPOINT_ONE, ENDPOINT_TWO};
    use std::time::Instant;

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
            received_at: Instant::now(),
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
        let second = source.poll().await.expect("poll must succeed");

        assert_eq!(first.messages.len(), 2);
        assert_eq!(second.messages.len(), 1);
        assert_eq!(first.messages[0].payload, b"one");
        assert!(matches!(first.schema, Schema::Raw));
        assert!(first.state.is_none());
    }

    #[tokio::test]
    async fn given_full_bridge_when_message_sent_should_reject() {
        let mut config = test_support::config(None, &[]);
        config.buffer_capacity = 1;
        let source = HttpSource::new(1, config, None);

        assert!(source.shared.sender.try_send(queued("one")).is_ok());
        assert!(
            source.shared.sender.try_send(queued("two")).is_err(),
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
