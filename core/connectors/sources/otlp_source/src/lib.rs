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

use async_trait::async_trait;
use iggy_connector_sdk::{
    ConnectorState, Error, ProducedMessage, ProducedMessages, Schema, Source, source_connector,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, mpsc, oneshot};
use tokio::task::JoinHandle;
use tonic::transport::server::TcpIncoming;
use tracing::info;

pub mod convert;
pub mod server;

source_connector!(OtlpSource);

#[derive(Debug)]
pub struct OtlpSource {
    id: u32,
    config: OtlpSourceConfig,
    rx: Mutex<Option<mpsc::Receiver<ProducedMessage>>>,
    shutdown_tx: Mutex<Option<oneshot::Sender<()>>>,
    server_task: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtlpSourceConfig {
    pub listen_addr: String,
    #[serde(default = "default_channel_capacity")]
    pub channel_capacity: usize,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
}

fn default_channel_capacity() -> usize {
    50_000
}

fn default_batch_size() -> usize {
    1_000
}

impl OtlpSource {
    pub fn new(id: u32, config: OtlpSourceConfig, _state: Option<ConnectorState>) -> Self {
        OtlpSource {
            id,
            config,
            rx: Mutex::new(None),
            shutdown_tx: Mutex::new(None),
            server_task: Mutex::new(None),
        }
    }
}

#[async_trait]
impl Source for OtlpSource {
    async fn open(&mut self) -> Result<(), Error> {
        let addr = self
            .config
            .listen_addr
            .parse()
            .map_err(|err| Error::InitError(format!("Invalid listen address: {err}")))?;

        let incoming = TcpIncoming::bind(addr).map_err(|err| {
            Error::InitError(format!("Failed to bind {}: {err}", self.config.listen_addr))
        })?;

        info!(
            "OTLP source connector with ID: {} listening on {}",
            self.id, self.config.listen_addr
        );

        let (tx, rx) = mpsc::channel(self.config.channel_capacity);
        let (shutdown_tx, shutdown_rx) = oneshot::channel();

        let handle = tokio::spawn(server::run_grpc_server(incoming, tx, shutdown_rx));

        *self.rx.lock().await = Some(rx);
        *self.shutdown_tx.lock().await = Some(shutdown_tx);
        *self.server_task.lock().await = Some(handle);

        Ok(())
    }

    async fn poll(&self) -> Result<ProducedMessages, Error> {
        let mut rx_guard = self.rx.lock().await;
        let rx = rx_guard.as_mut().ok_or_else(|| {
            Error::InitError("OTLP source connector is not initialized".to_string())
        })?;

        let first = match rx.recv().await {
            Some(msg) => msg,
            None => {
                return Ok(ProducedMessages {
                    schema: Schema::Json,
                    messages: vec![],
                    state: None,
                });
            }
        };

        let mut messages = Vec::with_capacity(self.config.batch_size);
        messages.push(first);

        while messages.len() < self.config.batch_size {
            match rx.try_recv() {
                Ok(msg) => messages.push(msg),
                Err(_) => break,
            }
        }

        Ok(ProducedMessages {
            schema: Schema::Json,
            messages,
            state: None,
        })
    }

    async fn close(&mut self) -> Result<(), Error> {
        if let Some(tx) = self.shutdown_tx.lock().await.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.server_task.lock().await.take() {
            task.abort();
        }
        info!("OTLP source connector with ID: {} closed.", self.id);
        Ok(())
    }
}
