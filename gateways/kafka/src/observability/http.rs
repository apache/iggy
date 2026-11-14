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

//! The HTTP endpoint at `IGGY_KAFKA_HTTP_ADDR`: `/metrics`, `/ready` and `/live`.
//!
//! No authentication and no TLS, as with a Prometheus exporter. Bind it where only the scraper
//! and the orchestrator reach it.

use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::extract::State;
use axum::http::{StatusCode, header};
use axum::response::IntoResponse;
use axum::routing::get;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::observability::Observability;
use crate::observability::health::ReadyState;

/// The content type of the text that `prometheus_client` encodes.
const OPENMETRICS: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";

/// How long [`HttpEndpoint::stop`] waits for the requests in flight.
const STOP_WAIT: Duration = Duration::from_secs(2);

/// How long [`check`] waits for an answer.
const CHECK_TIMEOUT: Duration = Duration::from_secs(3);

/// The path that a container health check asks: `--check-ready` or `--check-live`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthCheck {
    Ready,
    Live,
}

impl HealthCheck {
    const fn path(self) -> &'static str {
        match self {
            Self::Ready => "/ready",
            Self::Live => "/live",
        }
    }
}

impl FromStr for HealthCheck {
    type Err = String;

    fn from_str(arg: &str) -> Result<Self, Self::Err> {
        match arg {
            "--check-ready" => Ok(Self::Ready),
            "--check-live" => Ok(Self::Live),
            _ => Err(format!(
                "unknown argument `{arg}`: the gateway takes its configuration from IGGY_KAFKA_* \
                 environment variables, and --check-ready and --check-live are its only arguments"
            )),
        }
    }
}

/// A running HTTP endpoint.
pub struct HttpEndpoint {
    local_addr: SocketAddr,
    stop: CancellationToken,
    task: JoinHandle<()>,
}

impl HttpEndpoint {
    /// Binds `addr`, and serves `observability` there until [`Self::stop`].
    ///
    /// # Errors
    ///
    /// The bind error, such as an address already in use.
    pub async fn start(addr: SocketAddr, observability: Arc<Observability>) -> io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        observability.publish();
        let router = Router::new()
            .route("/metrics", get(metrics))
            .route("/ready", get(ready))
            .route("/live", get(live))
            .with_state(observability);
        let stop = CancellationToken::new();
        let stopped = stop.clone();
        let task = tokio::spawn(async move {
            let serve = axum::serve(listener, router)
                .with_graceful_shutdown(async move { stopped.cancelled().await });
            if let Err(error) = serve.await {
                warn!(%error, "the HTTP endpoint stopped");
            }
        });
        info!("HTTP endpoint of metrics, readiness and liveness bound on {local_addr}");
        Ok(Self {
            local_addr,
            stop,
            task,
        })
    }

    /// The address the endpoint is bound to.
    #[must_use]
    pub const fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Stops accepting, and waits up to 2 s for the requests in flight.
    pub async fn stop(self) {
        self.stop.cancel();
        let mut task = self.task;
        if timeout(STOP_WAIT, &mut task).await.is_err() {
            task.abort();
        }
    }
}

async fn metrics(State(observability): State<Arc<Observability>>) -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, OPENMETRICS)],
        observability.render_metrics().await,
    )
}

/// 200 while the gateway should get new clients, else 503 with the reason.
async fn ready(State(observability): State<Arc<Observability>>) -> impl IntoResponse {
    let state = observability.readiness().state();
    let status = if state == ReadyState::Ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (status, format!("{state}\n"))
}

/// 200 while the process answers. Iggy being out of reach is a readiness matter: a restart does
/// not bring Iggy back.
async fn live() -> &'static str {
    "live\n"
}

/// Asks `health` of the endpoint bound to `addr`, for a container health check. An unspecified
/// address, such as `0.0.0.0`, is asked on the loopback address.
///
/// # Errors
///
/// What kept the answer from being 200: no connection, no answer in time, or the status line and
/// the reason the endpoint gave.
pub async fn check(addr: SocketAddr, health: HealthCheck) -> Result<(), String> {
    let addr = reachable(addr);
    let path = health.path();
    let exchange = async {
        let mut stream = TcpStream::connect(addr).await?;
        let request =
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
        stream.write_all(request.as_bytes()).await?;
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await?;
        Ok::<_, io::Error>(response)
    };
    let response = match timeout(CHECK_TIMEOUT, exchange).await {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => return Err(format!("cannot ask {addr}{path}: {error}")),
        Err(_) => {
            return Err(format!(
                "{addr}{path} gave no answer within {CHECK_TIMEOUT:?}"
            ));
        }
    };
    let response = String::from_utf8_lossy(&response);
    let status_line = response.lines().next().unwrap_or_default();
    if status_line.split_whitespace().nth(1) == Some("200") {
        return Ok(());
    }
    let reason = response
        .split_once("\r\n\r\n")
        .map_or("", |(_, body)| body.trim());
    Err(format!("{status_line}: {reason}"))
}

/// `addr`, with an unspecified IP replaced by the loopback address of its family.
const fn reachable(addr: SocketAddr) -> SocketAddr {
    let ip = match addr.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
        ip => ip,
    };
    SocketAddr::new(ip, addr.port())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn get(addr: SocketAddr, path: &str) -> (u16, String) {
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let request =
            format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        let status = response
            .split_whitespace()
            .nth(1)
            .and_then(|code| code.parse().ok())
            .unwrap();
        (status, response)
    }

    #[tokio::test]
    async fn given_a_gateway_that_has_not_started_should_be_live_and_not_ready() {
        let observability = Arc::new(Observability::new());
        let endpoint =
            HttpEndpoint::start("127.0.0.1:0".parse().unwrap(), Arc::clone(&observability))
                .await
                .unwrap();
        let addr = endpoint.local_addr();

        assert_eq!(get(addr, "/live").await.0, 200);
        let (status, body) = get(addr, "/ready").await;
        assert_eq!(status, 503);
        assert!(body.contains("not ready: starting"), "{body}");
        assert!(
            check(addr, HealthCheck::Ready)
                .await
                .unwrap_err()
                .contains("starting")
        );
        check(addr, HealthCheck::Live)
            .await
            .expect("live while it waits for Iggy");

        let (status, body) = get(addr, "/metrics").await;
        assert_eq!(status, 200);
        assert!(
            body.contains("content-type: application/openmetrics-text"),
            "{body}"
        );
        assert!(body.contains("iggy_kafka_gateway_ready 0"), "{body}");
        assert!(body.trim_end().ends_with("# EOF"), "{body}");

        observability.readiness().serving(None);
        assert_eq!(get(addr, "/ready").await.0, 200);
        check(addr, HealthCheck::Ready)
            .await
            .expect("ready once it serves");

        observability.draining();
        assert!(
            check(addr, HealthCheck::Ready)
                .await
                .unwrap_err()
                .contains("shutting down")
        );
        check(addr, HealthCheck::Live)
            .await
            .expect("live while it drains");

        endpoint.stop().await;
        assert!(
            check(addr, HealthCheck::Live).await.is_err(),
            "a stopped endpoint answers nothing"
        );
    }

    #[test]
    fn given_the_health_check_arguments_should_name_their_paths() {
        assert_eq!("--check-ready".parse(), Ok(HealthCheck::Ready));
        assert_eq!("--check-live".parse(), Ok(HealthCheck::Live));
        assert!("--check".parse::<HealthCheck>().is_err());
    }

    #[test]
    fn given_a_wildcard_address_should_ask_the_loopback_address() {
        assert_eq!(
            reachable("0.0.0.0:9096".parse().unwrap()),
            "127.0.0.1:9096".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(
            reachable("[::]:9096".parse().unwrap()),
            "[::1]:9096".parse::<SocketAddr>().unwrap()
        );
        assert_eq!(
            reachable("10.0.0.7:9096".parse().unwrap()),
            "10.0.0.7:9096".parse::<SocketAddr>().unwrap()
        );
    }
}
