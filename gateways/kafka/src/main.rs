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

use std::fmt::Display;
use std::str::FromStr;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::{Semaphore, broadcast};

use iggy_gateway_kafka::server::init_tracing;
use iggy_gateway_kafka::{KafkaServer, ServerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let config = load_config()?;

    let listener = TcpListener::bind(&config.bind_addr)
        .await
        .map_err(|e| format!("failed to bind {}: {e}", config.bind_addr))?;
    let server = KafkaServer::new(config);

    let (tx, rx) = broadcast::channel(1);
    let mut server_task = tokio::spawn(async move { server.run(listener, rx).await });

    tokio::select! {
        result = &mut server_task => {
            return Ok(result??);
        }
        () = shutdown_signal() => {
            let _ = tx.send(());
        }
    }

    server_task.await??;
    Ok(())
}

/// Build [`ServerConfig`] from `IGGY_KAFKA_*` env vars, rejecting values that would silently
/// break the listener (a zero connection cap serves nothing, a zero timeout drops every
/// connection, a connection cap above `Semaphore::MAX_PERMITS` panics at startup).
fn load_config() -> Result<ServerConfig, String> {
    let mut config = ServerConfig::default();

    if let Some(bind_addr) = env_var("IGGY_KAFKA_BIND_ADDR") {
        config.bind_addr = bind_addr;
    }
    if let Some(advertised_host) = env_var("IGGY_KAFKA_ADVERTISED_HOST") {
        config.advertised_host = Some(advertised_host);
    }
    if let Some(raw) = env_var("IGGY_KAFKA_ADVERTISED_PORT") {
        config.advertised_port = Some(parse_positive("IGGY_KAFKA_ADVERTISED_PORT", &raw)?);
    }
    if let Some(raw) = env_var("IGGY_KAFKA_MAX_CONNECTIONS") {
        let max_connections: usize = parse_positive("IGGY_KAFKA_MAX_CONNECTIONS", &raw)?;
        if max_connections > Semaphore::MAX_PERMITS {
            return Err(format!(
                "IGGY_KAFKA_MAX_CONNECTIONS {max_connections} exceeds maximum {}",
                Semaphore::MAX_PERMITS
            ));
        }
        config.max_connections = max_connections;
    }
    if let Some(raw) = env_var("IGGY_KAFKA_MAX_FRAME_SIZE") {
        config.max_frame_size = parse_positive("IGGY_KAFKA_MAX_FRAME_SIZE", &raw)?;
    }
    if let Some(raw) = env_var("IGGY_KAFKA_IDLE_TIMEOUT_SECS") {
        config.idle_timeout =
            Duration::from_secs(parse_positive("IGGY_KAFKA_IDLE_TIMEOUT_SECS", &raw)?);
    }
    if let Some(raw) = env_var("IGGY_KAFKA_READ_TIMEOUT_SECS") {
        config.read_timeout =
            Duration::from_secs(parse_positive("IGGY_KAFKA_READ_TIMEOUT_SECS", &raw)?);
    }
    if let Some(raw) = env_var("IGGY_KAFKA_WRITE_TIMEOUT_SECS") {
        config.write_timeout =
            Duration::from_secs(parse_positive("IGGY_KAFKA_WRITE_TIMEOUT_SECS", &raw)?);
    }
    // Drain of 0 is valid: abandon in-flight connections immediately on shutdown.
    if let Some(raw) = env_var("IGGY_KAFKA_SHUTDOWN_DRAIN_TIMEOUT_SECS") {
        let secs: u64 = raw
            .parse()
            .map_err(|e| format!("invalid IGGY_KAFKA_SHUTDOWN_DRAIN_TIMEOUT_SECS `{raw}`: {e}"))?;
        config.shutdown_drain_timeout = Duration::from_secs(secs);
    }

    Ok(config)
}

fn env_var(key: &str) -> Option<String> {
    std::env::var(key).ok()
}

/// Parse a strictly-positive value, rejecting `0` (which for connection caps and timeouts would
/// silently disable the listener) and unparseable input.
fn parse_positive<T>(key: &str, raw: &str) -> Result<T, String>
where
    T: FromStr + Default + PartialEq,
    T::Err: Display,
{
    let value: T = raw
        .parse()
        .map_err(|e| format!("invalid {key} `{raw}`: {e}"))?;
    if value == T::default() {
        return Err(format!("{key} must be greater than 0"));
    }
    Ok(value)
}

/// Wait for Ctrl-C (SIGINT) or, on Unix, SIGTERM (`docker stop`).
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl-C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {}
        () = terminate => {}
    }
}

#[cfg(test)]
mod tests {
    use super::parse_positive;

    #[test]
    fn parse_positive_rejects_zero() {
        assert!(parse_positive::<usize>("KEY", "0").is_err());
        assert!(parse_positive::<u16>("KEY", "0").is_err());
    }

    #[test]
    fn parse_positive_rejects_non_numeric() {
        assert!(parse_positive::<usize>("KEY", "abc").is_err());
    }

    #[test]
    fn parse_positive_accepts_positive_value() {
        assert_eq!(parse_positive::<u64>("KEY", "42").unwrap(), 42);
        assert_eq!(parse_positive::<u16>("KEY", "9093").unwrap(), 9093);
    }
}
