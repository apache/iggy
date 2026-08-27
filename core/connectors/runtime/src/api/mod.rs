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
use std::{
    net::{IpAddr, SocketAddr},
    path::PathBuf,
    sync::Arc,
};
use tokio::{net::lookup_host, spawn};
use tracing::{error, info, warn};

mod auth;
pub mod config;
mod error;
mod models;
mod sink;
mod source;

const NAME: &str = env!("CARGO_PKG_NAME");
/// Where the full exposure surface is written down, so the startup warnings
/// can name one thing each instead of restating it. A URL rather than a repo
/// path: the published image carries the binary and its licences, nothing else,
/// so the reader most likely to see these lines has no source tree.
const EXPOSURE_DOC: &str =
    "https://github.com/apache/iggy/blob/master/core/connectors/runtime/README.md";

pub async fn init(config: &HttpConfig, context: Arc<RuntimeContext>) {
    if !config.enabled {
        info!("{NAME} HTTP API is disabled");
        return;
    }

    warn_on_weak_containment(config).await;

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

/// Warns once for each configuration change that leaves this API without a
/// compensating control: no key beyond loopback, no key with an unowned CORS
/// origin, no TLS beyond loopback.
///
/// Separate warnings rather than one, because the three compose independently
/// and an operator who closes one has not necessarily closed the others. Each
/// carries a pointer instead of its own remediation: the README holds the full
/// exposure surface, including the parts no startup warning can help with
/// because they are already true of the shipped defaults.
async fn warn_on_weak_containment(config: &HttpConfig) {
    let unauthenticated = config.api_key.expose_secret().is_empty();
    let beyond_loopback = resolves_beyond_loopback(&config.address).await;

    if unauthenticated && beyond_loopback {
        warn!(
            "{NAME} HTTP API on {} has no api_key: anyone who reaches it can read and rewrite every connector configuration. {EXPOSURE_DOC}",
            config.address
        );
    }

    // Loopback does not contain this one. A browser is a local process and the
    // CORS layer wraps outside authentication, so the shipped
    // `allowed_origins = ["*"]` lets any page the operator visits read these
    // endpoints cross-origin. Pinning origins the operator owns closes that
    // read, and an empty list emits no header at all, so neither is warned
    // about. `null` is not such an origin, so a list holding it still is. No
    // origin setting closes `POST .../restart`, which is CORS-simple and
    // reaches the handler regardless; that one is true of the defaults, so the
    // README carries it rather than a warning.
    if unauthenticated && config.cors.enabled && config.cors.allows_unowned_origin() {
        warn!(
            "{NAME} HTTP API has http.cors open to * or null with no api_key: any page the operator visits can read and rewrite its config. {EXPOSURE_DOC}"
        );
    }

    if beyond_loopback && !config.tls.enabled {
        warn!(
            "{NAME} HTTP API on {} has http.tls disabled: the api-key header and the credentials in its responses cross in cleartext. {EXPOSURE_DOC}",
            config.address
        );
    }
}

/// Whether `address` resolves to anything outside loopback.
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
/// with IPv6 disabled. The cost is resolving twice at startup, which is the
/// trade for keeping that fallback.
async fn resolves_beyond_loopback(address: &str) -> bool {
    let Ok(resolved) = lookup_host(address).await else {
        return true;
    };
    let addresses: Vec<SocketAddr> = resolved.collect();
    // Empty is reported as exposed rather than confined: `all` over nothing is
    // vacuously true, which would quietly invert the policy above.
    addresses.is_empty()
        || !addresses.iter().all(|address| match address.ip() {
            // `Ipv6Addr::is_loopback` matches only `::1`, so an IPv4-mapped
            // `::ffff:127.0.0.1` binds to 127.0.0.1 and would still read as
            // exposed.
            IpAddr::V6(v6) => v6
                .to_ipv4_mapped()
                .map_or(v6.is_loopback(), |v4| v4.is_loopback()),
            ip => ip.is_loopback(),
        })
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
    use crate::state::FileStateFactory;
    use crate::stream::IggyClients;
    use iggy::prelude::IggyClient;
    use iggy_common::IggyTimestamp;
    use secrecy::SecretString;
    use std::fmt::Write as _;
    use std::sync::Mutex;
    use tempfile::TempDir;
    use tracing::Level;
    use tracing::field::{Field, Visit};
    use tracing::subscriber::DefaultGuard;
    use tracing_subscriber::Layer as _;
    use tracing_subscriber::filter::LevelFilter;
    use tracing_subscriber::layer::{Context as LayerContext, SubscriberExt};

    /// Reserved for documentation by RFC 5737, so the bind fails and the tests
    /// that use it reach the warning without listening anywhere. Hosts running
    /// `net.ipv4.ip_nonlocal_bind=1`, which keepalived and haproxy boxes set,
    /// bind it regardless; `routable_address_binds` excuses those.
    const UNASSIGNABLE_ROUTABLE_ADDRESS: &str = "192.0.2.1:8081";
    const EPHEMERAL_LOOPBACK_ADDRESS: &str = "127.0.0.1:0";

    type Captured = Arc<Mutex<Vec<(Level, String)>>>;

    fn config(address: &str, api_key: &str) -> HttpConfig {
        HttpConfig {
            address: address.to_owned(),
            api_key: SecretString::from(api_key.to_owned()),
            ..HttpConfig::default()
        }
    }

    /// Captures events for the current thread only, for as long as the guard
    /// lives. Not a global subscriber: that slot is process-wide, and a shared
    /// buffer would leave negative assertions hostage to the rest of the binary.
    ///
    /// `#[tokio::test]` builds a current-thread runtime, so a task spawned by
    /// the test body sees this subscriber. Under a multi-thread flavour the
    /// capture would come back empty and these tests would fail, not pass.
    ///
    /// Filtered rather than checking the level in `on_event`: a layer with no
    /// filter reports no `max_level_hint`, which pushes the global max level to
    /// TRACE and stops every callsite in the binary short-circuiting.
    fn capture_events() -> (DefaultGuard, Captured) {
        let captured: Captured = Arc::new(Mutex::new(Vec::new()));
        let layer = CaptureEvents {
            captured: Arc::clone(&captured),
        }
        .with_filter(LevelFilter::INFO);
        let guard = tracing::subscriber::set_default(tracing_subscriber::registry().with(layer));
        (guard, captured)
    }

    fn warnings(captured: &Captured) -> Vec<String> {
        captured
            .lock()
            .expect("the capture mutex is only held to push a line")
            .iter()
            .filter(|(level, _)| *level == Level::WARN)
            .map(|(_, message)| message.clone())
            .collect()
    }

    /// Whether `init` got as far as serving. The positive control for tests
    /// whose real assertion is that something was not warned about.
    fn started_serving(captured: &Captured) -> bool {
        logged_info(captured, "Started")
    }

    fn logged_info(captured: &Captured, needle: &str) -> bool {
        captured
            .lock()
            .expect("the capture mutex is only held to push a line")
            .iter()
            .any(|(level, message)| *level == Level::INFO && message.contains(needle))
    }

    /// Whether this host binds an address RFC 5737 reserves for documentation.
    ///
    /// `net.ipv4.ip_nonlocal_bind=1`, which keepalived and haproxy boxes set,
    /// makes it succeed. The two tests that read a failed bind as their
    /// ordering control stand those two assertions down there rather than going
    /// red over a sysctl. They do not skip: a skipped test reports `ok` with no
    /// signal, and the warning itself is required on every host.
    fn routable_address_binds() -> bool {
        std::net::TcpListener::bind(UNASSIGNABLE_ROUTABLE_ADDRESS).is_ok()
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

    /// Unconditional on purpose: singling out the `message` field would add a
    /// branch whose other side nothing here takes.
    struct Recorded(String);

    impl Visit for Recorded {
        fn record_debug(&mut self, _field: &Field, value: &dyn std::fmt::Debug) {
            let _ = write!(self.0, "{value:?} ");
        }
    }

    /// The cheapest context `init` will accept. Nothing here reaches Iggy.
    ///
    /// `api_key` is a parameter because the guard reads `config.api_key` while
    /// the middleware enforces `context.api_key`; a test that set only one
    /// would exercise a state the runtime cannot reach.
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
            state_factory: Arc::new(FileStateFactory::new(
                directory.path().display().to_string(),
            )),
        };
        (Arc::new(context), directory)
    }

    #[tokio::test]
    async fn given_loopback_addresses_when_classified_should_report_contained() {
        assert!(!resolves_beyond_loopback("127.0.0.1:8081").await);
        assert!(!resolves_beyond_loopback("[::1]:8081").await);
        assert!(
            !resolves_beyond_loopback("localhost:8081").await,
            "`address` accepts a hostname, and parsing alone would misjudge one"
        );
        assert!(
            !resolves_beyond_loopback("[::ffff:127.0.0.1]:8081").await,
            "an IPv4-mapped address binds to 127.0.0.1, and `Ipv6Addr::is_loopback` \
             alone matches only `::1`"
        );
    }

    #[tokio::test]
    async fn given_routable_or_unresolvable_addresses_when_classified_should_report_exposed() {
        assert!(
            resolves_beyond_loopback("0.0.0.0:8081").await,
            "binding every interface to reach the API from outside a container \
             is the case this exists to catch"
        );
        assert!(resolves_beyond_loopback("192.0.2.10:8081").await);
        // About to fail the bind regardless, so staying quiet about an address
        // we cannot classify is the wrong direction to be wrong in.
        assert!(resolves_beyond_loopback("not a valid address").await);
    }

    #[tokio::test]
    async fn given_no_key_and_a_routable_address_when_initialized_should_warn_before_binding() {
        let bind_is_refused = !routable_address_binds();
        // Fixture first: `create_connectors_config_provider` logs, and anything
        // it warns about later would otherwise land in this buffer.
        let (context, _directory) = context("").await;
        let (_capture, captured) = capture_events();
        let config = config(UNASSIGNABLE_ROUTABLE_ADDRESS, "");

        // `init` panics when the bind fails, which is what makes this the
        // ordering test: the warning has to already be out by then, or an
        // operator whose bind fails never learns the API was unauthenticated.
        let bind_failed = tokio::spawn(async move { init(&config, context).await })
            .await
            .is_err();

        if bind_is_refused {
            assert!(
                bind_failed,
                "a documentation-range address must not bind here"
            );
            assert!(
                !started_serving(&captured),
                "the bind must not have completed, or this proves nothing about ordering"
            );
        }
        assert!(
            warnings(&captured)
                .iter()
                .any(|warning| warning.contains(UNASSIGNABLE_ROUTABLE_ADDRESS)),
            "init must consult the guard and name the address it is exposing"
        );
        assert!(
            warnings(&captured)
                .iter()
                .all(|warning| warning.contains(EXPOSURE_DOC)),
            "each warning carries the pointer instead of its own remediation: {:?}",
            warnings(&captured)
        );
    }

    #[tokio::test]
    async fn given_loopback_address_when_initialized_should_serve_without_warning() {
        let (context, _directory) = context("").await;
        let (_capture, captured) = capture_events();

        init(&config(EPHEMERAL_LOOPBACK_ADDRESS, ""), context).await;

        // Positive control first: without it the assertion below passes for any
        // reason `init` returns early, including `enabled` ever defaulting to
        // false, and the only in-`init` loopback coverage disappears silently.
        assert!(
            started_serving(&captured),
            "init must reach the listener, or the assertion below proves nothing"
        );
        // Any warning at all, not one matching this address: matching on the
        // address goes vacuous the moment the message is reworded.
        assert!(
            warnings(&captured).is_empty(),
            "the shipped posture is loopback with no key; warning about it would \
             teach operators to ignore the ones that matter: {:?}",
            warnings(&captured)
        );
    }

    #[tokio::test]
    async fn given_wildcard_cors_and_no_key_when_initialized_should_warn_despite_loopback() {
        let (context, _directory) = context("").await;
        let (_capture, captured) = capture_events();
        let mut config = config(EPHEMERAL_LOOPBACK_ADDRESS, "");
        config.cors.enabled = true;
        config.cors.allowed_origins = vec!["*".to_owned()];

        init(&config, context).await;

        assert!(started_serving(&captured));
        assert!(
            warnings(&captured)
                .iter()
                .any(|warning| warning.contains("http.cors")),
            "loopback does not contain CORS: a browser is a local process and the \
             layer wraps outside authentication"
        );
        assert!(
            warnings(&captured)
                .iter()
                .all(|warning| warning.contains(EXPOSURE_DOC)),
            "each warning carries the pointer instead of its own remediation: {:?}",
            warnings(&captured)
        );
    }

    #[tokio::test]
    async fn given_owned_cors_origins_when_initialized_should_not_warn_about_cors() {
        let (context, _directory) = context("").await;
        let (_capture, captured) = capture_events();
        let mut config = config(EPHEMERAL_LOOPBACK_ADDRESS, "");
        config.cors.enabled = true;
        config.cors.allowed_origins = vec!["https://console.example".to_owned()];

        init(&config, context).await;

        assert!(
            started_serving(&captured),
            "init must reach the listener, or the assertion below proves nothing"
        );
        assert!(
            warnings(&captured).is_empty(),
            "an operator who pinned origins they own closed the cross-origin read, \
             and warning at them is what teaches everyone to ignore the rest: {:?}",
            warnings(&captured)
        );
    }

    #[tokio::test]
    async fn given_a_null_cors_origin_when_initialized_should_warn_despite_the_pin() {
        let (context, _directory) = context("").await;
        let (_capture, captured) = capture_events();
        let mut config = config(EPHEMERAL_LOOPBACK_ADDRESS, "");
        config.cors.enabled = true;
        config.cors.allowed_origins = vec!["null".to_owned()];

        init(&config, context).await;

        assert!(started_serving(&captured));
        assert!(
            warnings(&captured)
                .iter()
                .any(|warning| warning.contains("http.cors")),
            "`null` is a pinned origin that nobody owns, so pinning it buys nothing \
             a wildcard would not have given away"
        );
    }

    #[tokio::test]
    async fn given_a_key_but_no_tls_beyond_loopback_when_initialized_should_still_warn() {
        let bind_is_refused = !routable_address_binds();
        let (context, _directory) = context("configured").await;
        let (_capture, captured) = capture_events();
        let config = config(UNASSIGNABLE_ROUTABLE_ADDRESS, "configured");

        let bind_failed = tokio::spawn(async move { init(&config, context).await })
            .await
            .is_err();

        if bind_is_refused {
            assert!(
                bind_failed,
                "a documentation-range address must not bind here"
            );
        }
        let warnings = warnings(&captured);
        assert!(
            warnings.iter().any(|warning| warning.contains("http.tls")),
            "setting a key does not stop the key and the credential-bearing \
             responses crossing the network in cleartext"
        );
        assert_eq!(
            warnings.len(),
            1,
            "the key that was set must not still be reported as missing, and a \
             count cannot go vacuous the way matching on the old wording would: \
             {warnings:?}"
        );
        assert!(
            warnings
                .iter()
                .all(|warning| warning.contains(EXPOSURE_DOC)),
            "each warning carries the pointer instead of its own remediation: {warnings:?}"
        );
    }

    #[tokio::test]
    async fn given_a_disabled_api_when_initialized_should_warn_about_nothing() {
        let (context, _directory) = context("").await;
        let (_capture, captured) = capture_events();
        // Routable, keyless and untrusting in every direction, but switched off.
        let mut config = config(UNASSIGNABLE_ROUTABLE_ADDRESS, "");
        config.enabled = false;
        config.cors.enabled = true;

        init(&config, context).await;

        // Positive control: without it this passes for any reason `init` returns
        // early, including one that never reaches the guard at all.
        assert!(
            logged_info(&captured, "HTTP API is disabled"),
            "init must take the disabled path, or the assertion below proves nothing"
        );
        assert!(
            warnings(&captured).is_empty(),
            "an API that is not listening exposes nothing, and warning about one \
             is the false positive that teaches operators to ignore the rest: {:?}",
            warnings(&captured)
        );
    }
}
