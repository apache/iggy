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

use crate::context::RuntimeContext;
use crate::stats;
use auth::resolve_api_key;
use axum::{Json, Router, extract::State, middleware, routing::get};
use axum_server::tls_rustls::RustlsConfig;
use config::{HttpConfig, configure_cors};
use iggy_connector_sdk::api::ConnectorRuntimeStats;
use secrecy::ExposeSecret;
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use tokio::net::lookup_host;
use tokio::spawn;
use tracing::{error, info, warn};

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

    if is_unauthenticated_beyond_loopback(config).await {
        warn!(
            "{NAME} HTTP API is enabled on {} with no api_key configured. Anyone able to reach that address can read or rewrite every connector configuration, credentials included, and restart connectors from it. Set http.api_key, and http.tls unless the key and the responses may cross in cleartext, or bind the API to loopback.",
            config.address
        );
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

/// Whether the API would answer beyond loopback with no key required.
///
/// The configuration endpoints return plugin configuration verbatim and also
/// accept writes, so an unauthenticated listener on a routable address hands
/// out every credential an operator put in their config and lets a caller
/// repoint a connector. Loopback with no key is the shipped default and a
/// defensible posture for an admin API; moving only the address is the
/// combination no other layer catches.
///
/// Resolves rather than parses because `address` is a free-form `String` that
/// takes a hostname, as `[iggy] address` does in the same file. Not because the
/// default needs it: the embedded `config.toml` is the first figment layer, so
/// the effective default is `127.0.0.1:8081` and would parse. An address that
/// cannot resolve counts as exposed, since it is about to fail the bind anyway
/// and staying quiet about one we could not classify is the wrong direction to
/// be wrong in.
///
/// Classification only. Do not bind what this resolves: `TcpListener::bind`
/// walks every resolved address and takes the first that works, so collapsing
/// to one would drop the `localhost` -> `[::1, 127.0.0.1]` fallback on hosts
/// with IPv6 disabled.
async fn is_unauthenticated_beyond_loopback(config: &HttpConfig) -> bool {
    if !config.api_key.expose_secret().is_empty() {
        return false;
    }
    let Ok(resolved) = lookup_host(&config.address).await else {
        return true;
    };
    let addresses: Vec<SocketAddr> = resolved.collect();
    // Empty is reported as exposed rather than confined: `all` over nothing is
    // vacuously true, which would quietly invert the policy above.
    addresses.is_empty() || !addresses.iter().all(|address| address.ip().is_loopback())
}

async fn get_metrics(State(context): State<Arc<RuntimeContext>>) -> String {
    context.metrics.get_formatted_output()
}

async fn get_stats(State(context): State<Arc<RuntimeContext>>) -> Json<ConnectorRuntimeStats> {
    Json(stats::get_runtime_stats(&context).await)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::connectors::create_connectors_config_provider;
    use crate::configs::runtime::{ConnectorsConfig, LocalConnectorsConfig};
    use crate::manager::sink::SinkManager;
    use crate::manager::source::SourceManager;
    use crate::metrics::Metrics;
    use crate::stream::IggyClients;
    use iggy::prelude::IggyClient;
    use iggy_common::IggyTimestamp;
    use secrecy::SecretString;
    use std::sync::Mutex;
    use tempfile::TempDir;
    use tracing::Level;
    use tracing::field::{Field, Visit};
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::Layer as _;
    use tracing_subscriber::filter::LevelFilter;
    use tracing_subscriber::layer::{Context as LayerContext, SubscriberExt};

    /// Reserved for documentation by RFC 5737, so no host routes it and the
    /// bind fails. That is what lets the test reach the warning without
    /// listening anywhere: a non-loopback address that binds successfully would
    /// put a port on every interface for the life of the test binary.
    const UNASSIGNABLE_ROUTABLE_ADDRESS: &str = "192.0.2.1:8081";

    /// Port 0 rather than a port reserved by binding and dropping first. The
    /// tests never need to know which port it lands on, and reserving one is a
    /// race that buys nothing.
    const EPHEMERAL_LOOPBACK_ADDRESS: &str = "127.0.0.1:0";

    type Captured = Arc<Mutex<Vec<(Level, String)>>>;

    fn config(address: &str, api_key: &str) -> HttpConfig {
        HttpConfig {
            address: address.to_owned(),
            api_key: SecretString::from(api_key.to_owned()),
            ..HttpConfig::default()
        }
    }

    /// Captures events for the current thread only, for as long as the returned
    /// guard lives.
    ///
    /// Deliberately not a global subscriber: that slot is process-wide, so
    /// claiming it would break any later test that installs its own, and a
    /// shared buffer would leave every negative assertion hostage to warnings
    /// from elsewhere in the binary.
    ///
    /// `#[tokio::test]` builds a current-thread runtime, so a task spawned by
    /// the test body runs on this thread and sees this subscriber. Under a
    /// multi-thread flavour the capture would come back empty and these
    /// assertions would fail rather than quietly pass.
    fn capture_events() -> (DefaultGuard, Captured) {
        let captured: Captured = Arc::new(Mutex::new(Vec::new()));
        // Filtered rather than checking the level inside `on_event`: a layer
        // with no filter reports no `max_level_hint`, which pushes the global
        // max level to TRACE and stops every callsite in the binary from
        // short-circuiting.
        let layer = CaptureEvents {
            captured: Arc::clone(&captured),
        }
        .with_filter(LevelFilter::INFO);
        let guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
        (guard, captured)
    }

    fn warned_about(captured: &Captured, address: &str) -> bool {
        captured
            .lock()
            .expect("the capture mutex is only held to push a line")
            .iter()
            .any(|(level, message)| *level == Level::WARN && message.contains(address))
    }

    /// Whether `init` got as far as serving. The positive control for tests
    /// whose real assertion is that nothing was warned about.
    fn started_serving(captured: &Captured) -> bool {
        captured
            .lock()
            .expect("the capture mutex is only held to push a line")
            .iter()
            .any(|(level, message)| *level == Level::INFO && message.contains("Started"))
    }

    struct CaptureEvents {
        captured: Captured,
    }

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CaptureEvents {
        fn on_event(&self, event: &tracing::Event<'_>, _context: LayerContext<'_, S>) {
            let mut recorded = Recorded(String::new());
            event.record(&mut recorded);
            self.captured
                .lock()
                .expect("the capture mutex is only held to push a line")
                .push((*event.metadata().level(), recorded.0));
        }
    }

    /// Every field the event carried, rendered into one line.
    ///
    /// Unconditional on purpose. These tests only ask whether an event
    /// mentioned a given string, so singling out the `message` field would add
    /// a branch to the scaffolding whose other side nothing here would ever
    /// take. `record_str` needs no impl either: it forwards here by default,
    /// and a formatted message arrives as `fmt::Arguments` regardless.
    struct Recorded(String);

    impl Visit for Recorded {
        fn record_debug(&mut self, _field: &Field, value: &dyn std::fmt::Debug) {
            self.0.push_str(&format!("{value:?} "));
        }
    }

    /// The cheapest context `init` will accept. Nothing here reaches Iggy: the
    /// clients are never connected, and the warning is decided from the config
    /// alone.
    ///
    /// `api_key` is a parameter rather than always empty because the guard
    /// reads `config.api_key` while the middleware enforces `context.api_key`.
    /// They come from one binding in `main.rs` today, but a test that set only
    /// one of them would be exercising a state the runtime cannot reach.
    async fn context(api_key: &str) -> (Arc<RuntimeContext>, TempDir) {
        let directory = tempfile::tempdir().expect("a temp dir must be available");
        let config_provider =
            create_connectors_config_provider(&ConnectorsConfig::Local(LocalConnectorsConfig {
                config_dir: directory.path().display().to_string(),
            }))
            .await
            .expect("an empty config dir must initialize with no connectors");

        let context = RuntimeContext {
            sinks: SinkManager::new(vec![]),
            sources: SourceManager::new(vec![]),
            api_key: SecretString::from(api_key.to_owned()),
            config_provider: Arc::from(config_provider),
            metrics: Arc::new(Metrics::init()),
            start_time: IggyTimestamp::now(),
            iggy_clients: Arc::new(IggyClients {
                producer: IggyClient::default(),
                consumer: IggyClient::default(),
            }),
            state_path: directory.path().display().to_string(),
        };
        (Arc::new(context), directory)
    }

    #[tokio::test]
    async fn given_loopback_address_and_no_key_when_checked_should_stay_quiet() {
        // The shipped posture. Warning here would train operators to ignore it.
        assert!(!is_unauthenticated_beyond_loopback(&config("127.0.0.1:8081", "")).await);
        assert!(!is_unauthenticated_beyond_loopback(&config("[::1]:8081", "")).await);
        assert!(
            !is_unauthenticated_beyond_loopback(&config("localhost:8081", "")).await,
            "`address` accepts a hostname, and parsing alone would misjudge one"
        );
    }

    #[tokio::test]
    async fn given_routable_address_and_no_key_when_checked_should_report_it() {
        assert!(
            is_unauthenticated_beyond_loopback(&config("0.0.0.0:8081", "")).await,
            "binding every interface to reach the API from outside a container \
             is the case this exists to catch"
        );
        assert!(is_unauthenticated_beyond_loopback(&config("192.0.2.10:8081", "")).await);
    }

    #[tokio::test]
    async fn given_configured_key_when_checked_should_stay_quiet_on_any_address() {
        assert!(!is_unauthenticated_beyond_loopback(&config("0.0.0.0:8081", "secret")).await);
    }

    #[tokio::test]
    async fn given_unresolvable_address_when_checked_should_report_it() {
        // About to fail the bind regardless, so the warning costs nothing and
        // the alternative is silence about an address we cannot classify.
        assert!(is_unauthenticated_beyond_loopback(&config("not a valid address", "")).await);
    }

    #[tokio::test]
    async fn given_no_key_and_a_routable_address_when_initialized_should_warn_before_binding() {
        let (_capture, captured) = capture_events();
        let (context, _directory) = context("").await;
        let config = config(UNASSIGNABLE_ROUTABLE_ADDRESS, "");

        // `init` panics when the bind fails, which is what makes this the
        // ordering test: the warning has to already be out by then, or an
        // operator whose bind fails never learns the API was unauthenticated.
        let bind_failed = tokio::spawn(async move { init(&config, context).await })
            .await
            .is_err();

        assert!(
            bind_failed,
            "a documentation-range address is expected to be unbindable. A host with \
             net.ipv4.ip_nonlocal_bind=1, which keepalived and haproxy boxes set, binds \
             it instead, and this test has then started a listener that outlives the run"
        );
        assert!(
            warned_about(&captured, UNASSIGNABLE_ROUTABLE_ADDRESS),
            "init must consult the guard and name the address it is exposing"
        );
    }

    #[tokio::test]
    async fn given_loopback_address_when_initialized_should_serve_without_warning() {
        let (_capture, captured) = capture_events();
        let (context, _directory) = context("").await;

        init(&config(EPHEMERAL_LOOPBACK_ADDRESS, ""), context).await;

        // Positive control first. Without it the assertion below passes for any
        // reason `init` might return early, including `enabled` defaulting to
        // false, and the only in-`init` loopback coverage would disappear
        // silently.
        assert!(
            started_serving(&captured),
            "init must reach the listener, or the assertion below proves nothing"
        );
        assert!(
            !warned_about(&captured, EPHEMERAL_LOOPBACK_ADDRESS),
            "the shipped posture is loopback with no key; warning about it \
             would teach operators to ignore the one that matters"
        );
    }
}
