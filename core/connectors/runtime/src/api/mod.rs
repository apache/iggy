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
    net::{SocketAddr, ToSocketAddrs},
    path::PathBuf,
    sync::Arc,
};
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

    if is_unauthenticated_beyond_loopback(config) {
        warn!(
            "{NAME} HTTP API is enabled on {} with no api_key configured. Its configuration endpoints return plugin configuration verbatim, credentials included, so anyone able to reach that address can read every connector secret. Set http.api_key, or bind the API to loopback.",
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
/// The configuration endpoints return plugin configuration verbatim, so an
/// unauthenticated listener on a routable address hands out every credential an
/// operator put in their TOML. Loopback with no key is the shipped default and
/// a defensible posture for an admin API; moving only the address is the
/// combination no other layer catches.
///
/// Resolves rather than parses, because `address` accepts a hostname and the
/// default is `localhost:8081` - which is loopback but is not a `SocketAddr`.
/// The bind that follows resolves the same string, so this classifies what will
/// actually be listened on. An address that cannot resolve counts as exposed:
/// it is about to fail the bind anyway, and staying quiet about an address we
/// could not classify is the wrong direction to be wrong in.
fn is_unauthenticated_beyond_loopback(config: &HttpConfig) -> bool {
    if !config.api_key.expose_secret().is_empty() {
        return false;
    }
    match config.address.to_socket_addrs() {
        Ok(mut resolved) => !resolved.all(|address| address.ip().is_loopback()),
        Err(_) => true,
    }
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
    use std::sync::{Mutex, OnceLock};
    use tempfile::TempDir;
    use tracing::Level;
    use tracing::field::{Field, Visit};
    use tracing_subscriber::layer::{Context as LayerContext, SubscriberExt};

    /// Reserved for documentation (RFC 5737), so it is never assignable on a
    /// real host. Used to reach the warning without binding: any non-loopback
    /// address that binds successfully would expose a port on every interface
    /// for the duration of the test.
    const UNASSIGNABLE_ROUTABLE_ADDRESS: &str = "192.0.2.1:8081";

    fn config(address: &str, api_key: &str) -> HttpConfig {
        HttpConfig {
            address: address.to_owned(),
            api_key: SecretString::from(api_key.to_owned()),
            ..HttpConfig::default()
        }
    }

    fn captured() -> &'static Mutex<Vec<String>> {
        static WARNINGS: OnceLock<Mutex<Vec<String>>> = OnceLock::new();
        WARNINGS.get_or_init(|| Mutex::new(Vec::new()))
    }

    /// Installs the capture once for the whole test binary, since a global
    /// subscriber can only be set once. Every test filters the captured lines
    /// by its own address, so events from tests running in parallel cannot be
    /// mistaken for each other.
    fn capture_warnings() {
        static INSTALLED: OnceLock<()> = OnceLock::new();
        INSTALLED.get_or_init(|| {
            let subscriber = tracing_subscriber::registry().with(CaptureWarnings);
            tracing::subscriber::set_global_default(subscriber)
                .expect("no other test in this binary installs a subscriber");
        });
    }

    fn warned_about(address: &str) -> bool {
        captured()
            .lock()
            .expect("the capture mutex is only held to push a line")
            .iter()
            .any(|warning| warning.contains(address))
    }

    struct CaptureWarnings;

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for CaptureWarnings {
        fn on_event(&self, event: &tracing::Event<'_>, _context: LayerContext<'_, S>) {
            if *event.metadata().level() != Level::WARN {
                return;
            }
            let mut recorded = Recorded(String::new());
            event.record(&mut recorded);
            captured()
                .lock()
                .expect("the capture mutex is only held to push a line")
                .push(recorded.0);
        }
    }

    /// Every field the event carried, rendered into one line.
    ///
    /// Unconditional on purpose. These tests only ask whether a warning
    /// mentioned a given address, so singling out the `message` field would add
    /// a branch to the scaffolding whose other side nothing here would ever
    /// take. `record_str` needs no impl either: it forwards here by default,
    /// and a formatted `warn!` message arrives as `fmt::Arguments` regardless.
    struct Recorded(String);

    impl Visit for Recorded {
        fn record_debug(&mut self, _field: &Field, value: &dyn std::fmt::Debug) {
            self.0.push_str(&format!("{value:?} "));
        }
    }

    /// The cheapest context `init` will accept. Nothing here reaches Iggy: the
    /// clients are never connected, and the warning is decided from the config
    /// alone.
    async fn context() -> (Arc<RuntimeContext>, TempDir) {
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
            api_key: SecretString::from(String::new()),
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

    fn free_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .expect("the loopback interface must offer a port")
            .local_addr()
            .expect("a bound listener has an address")
            .port()
    }

    #[tokio::test]
    async fn given_no_key_and_a_routable_address_when_initialized_should_warn_before_binding() {
        capture_warnings();
        let (context, _directory) = context().await;
        let config = config(UNASSIGNABLE_ROUTABLE_ADDRESS, "");

        // `init` panics when the bind fails, which is what makes this the
        // ordering test: the warning has to already be out by then, or an
        // operator whose bind fails never learns the API was unauthenticated.
        let bind_failed = tokio::spawn(async move { init(&config, context).await })
            .await
            .is_err();

        assert!(
            bind_failed,
            "a documentation-range address must not be bindable, or this test \
             would be exposing a port instead of exercising the warning"
        );
        assert!(
            warned_about(UNASSIGNABLE_ROUTABLE_ADDRESS),
            "init must consult the guard and name the address it is exposing"
        );
    }

    #[tokio::test]
    async fn given_loopback_address_when_initialized_should_not_warn() {
        capture_warnings();
        let address = format!("127.0.0.1:{}", free_port());
        let (context, _directory) = context().await;

        init(&config(&address, ""), context).await;

        assert!(
            !warned_about(&address),
            "the shipped posture is loopback with no key; warning about it \
             would teach operators to ignore the one that matters"
        );
    }

    #[test]
    fn given_loopback_address_and_no_key_when_checked_should_stay_quiet() {
        // The shipped posture. Warning here would train operators to ignore it.
        assert!(!is_unauthenticated_beyond_loopback(&config(
            "127.0.0.1:8081",
            ""
        )));
        assert!(!is_unauthenticated_beyond_loopback(&config(
            "[::1]:8081",
            ""
        )));
        assert!(
            !is_unauthenticated_beyond_loopback(&config("localhost:8081", "")),
            "the default address is a hostname, so parsing alone would misjudge it"
        );
    }

    #[test]
    fn given_routable_address_and_no_key_when_checked_should_report_it() {
        assert!(
            is_unauthenticated_beyond_loopback(&config("0.0.0.0:8081", "")),
            "binding every interface to reach the API from outside a container \
             is the case this exists to catch"
        );
        assert!(is_unauthenticated_beyond_loopback(&config(
            "192.0.2.10:8081",
            ""
        )));
    }

    #[test]
    fn given_configured_key_when_checked_should_stay_quiet_on_any_address() {
        assert!(!is_unauthenticated_beyond_loopback(&config(
            "0.0.0.0:8081",
            "secret"
        )));
    }

    #[test]
    fn given_unresolvable_address_when_checked_should_report_it() {
        // About to fail the bind regardless, so the warning costs nothing and
        // the alternative is silence about an address we cannot classify.
        assert!(is_unauthenticated_beyond_loopback(&config(
            "not a valid address",
            ""
        )));
    }
}
