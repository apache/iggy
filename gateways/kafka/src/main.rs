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

use tokio::net::TcpListener;
use tokio::signal;
use tokio::sync::broadcast;

use iggy_gateway_kafka::server::init_tracing;
use iggy_gateway_kafka::{KafkaServer, ServerConfig};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    init_tracing();

    let mut config = ServerConfig::default();
    if let Ok(bind_addr) = std::env::var("KAFKA_BIND_ADDR") {
        config.bind_addr = bind_addr;
    }
    if let Ok(advertised_host) = std::env::var("KAFKA_ADVERTISED_HOST") {
        config.advertised_host = Some(advertised_host);
    }
    if let Ok(advertised_port) = std::env::var("KAFKA_ADVERTISED_PORT") {
        config.advertised_port = Some(
            advertised_port
                .parse()
                .map_err(|e| format!("invalid KAFKA_ADVERTISED_PORT `{advertised_port}`: {e}"))?,
        );
    }
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
