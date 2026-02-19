/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
use crate::PLUGIN_ID;
use crate::SinkApi;
use crate::configs::connectors::{ConfigFormat, ConnectorsConfigProvider, SinkConfig};
use crate::error::RuntimeError;
use crate::metrics::Metrics;
use crate::sink;
use dashmap::DashMap;
use dlopen2::wrapper::Container;
use iggy::prelude::IggyClient;
use iggy_connector_sdk::api::{ConnectorError, ConnectorStatus};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;
use tracing::{error, info};

#[derive(Debug)]
pub struct SinkManager {
    sinks: DashMap<String, Arc<Mutex<SinkDetails>>>,
}

impl SinkManager {
    pub fn new(sinks: Vec<SinkDetails>) -> Self {
        Self {
            sinks: DashMap::from_iter(
                sinks
                    .into_iter()
                    .map(|sink| (sink.info.key.to_owned(), Arc::new(Mutex::new(sink))))
                    .collect::<HashMap<_, _>>(),
            ),
        }
    }

    pub async fn get(&self, key: &str) -> Option<Arc<Mutex<SinkDetails>>> {
        self.sinks.get(key).map(|entry| entry.value().clone())
    }

    pub async fn get_config(&self, key: &str) -> Option<SinkConfig> {
        if let Some(sink) = self.sinks.get(key).map(|entry| entry.value().clone()) {
            let sink = sink.lock().await;
            Some(sink.config.clone())
        } else {
            None
        }
    }

    pub async fn get_all(&self) -> Vec<SinkInfo> {
        let sinks = &self.sinks;
        let mut results = Vec::with_capacity(sinks.len());
        for sink in sinks.iter().map(|entry| entry.value().clone()) {
            let sink = sink.lock().await;
            results.push(sink.info.clone());
        }
        results
    }

    pub async fn update_status(
        &self,
        key: &str,
        status: ConnectorStatus,
        metrics: Option<&Arc<Metrics>>,
    ) {
        if let Some(sink) = self.sinks.get(key) {
            let mut sink = sink.lock().await;
            let old_status = sink.info.status;
            sink.info.status = status;
            if matches!(status, ConnectorStatus::Running | ConnectorStatus::Stopped) {
                sink.info.last_error = None;
            }
            if let Some(metrics) = metrics {
                if old_status != ConnectorStatus::Running && status == ConnectorStatus::Running {
                    metrics.increment_sinks_running();
                } else if old_status == ConnectorStatus::Running
                    && status != ConnectorStatus::Running
                {
                    metrics.decrement_sinks_running();
                }
            }
        }
    }

    pub async fn set_error(&self, key: &str, error_message: &str) {
        if let Some(sink) = self.sinks.get(key) {
            let mut sink = sink.lock().await;
            sink.info.status = ConnectorStatus::Error;
            sink.info.last_error = Some(ConnectorError::new(error_message));
        }
    }

    pub async fn stop_connector(
        &self,
        key: &str,
        metrics: &Arc<Metrics>,
    ) -> Result<(), RuntimeError> {
        let details_arc = self
            .sinks
            .get(key)
            .map(|e| e.value().clone())
            .ok_or_else(|| RuntimeError::SinkNotFound(key.to_string()))?;

        let (shutdown_tx, task_handles, plugin_id, container) = {
            let mut details = details_arc.lock().await;
            (
                details.shutdown_tx.take(),
                std::mem::take(&mut details.task_handles),
                details.info.id,
                details.container.clone(),
            )
        };

        if let Some(tx) = shutdown_tx {
            let _ = tx.send(());
        }

        for handle in task_handles {
            let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
        }

        if let Some(container) = &container {
            info!("Closing sink connector with ID: {plugin_id} for plugin: {key}");
            (container.iggy_sink_close)(plugin_id);
            info!("Closed sink connector with ID: {plugin_id} for plugin: {key}");
        }

        {
            let mut details = details_arc.lock().await;
            let old_status = details.info.status;
            details.info.status = ConnectorStatus::Stopped;
            details.info.last_error = None;
            if old_status == ConnectorStatus::Running {
                metrics.decrement_sinks_running();
            }
        }

        Ok(())
    }

    pub async fn start_connector(
        &self,
        key: &str,
        config: &SinkConfig,
        iggy_client: &IggyClient,
        metrics: &Arc<Metrics>,
    ) -> Result<(), RuntimeError> {
        let details_arc = self
            .sinks
            .get(key)
            .map(|e| e.value().clone())
            .ok_or_else(|| RuntimeError::SinkNotFound(key.to_string()))?;

        let container = {
            let details = details_arc.lock().await;
            details.container.clone().ok_or_else(|| {
                RuntimeError::InvalidConfiguration(format!(
                    "No container loaded for sink: {key}"
                ))
            })?
        };

        let plugin_id = PLUGIN_ID.fetch_add(1, Ordering::SeqCst);

        sink::init_sink(
            &container,
            &config.plugin_config.clone().unwrap_or_default(),
            plugin_id,
        )?;
        info!("Sink connector with ID: {plugin_id} for plugin: {key} initialized successfully.");

        let consumers = sink::setup_sink_consumers(key, config, iggy_client).await?;

        let (shutdown_tx, shutdown_rx) = watch::channel(());
        let callback = container.iggy_sink_consume;
        let verbose = config.verbose;
        let mut task_handles = Vec::new();

        for (consumer, decoder, batch_size, transforms) in consumers {
            let plugin_key = key.to_string();
            let metrics_clone = metrics.clone();
            let shutdown_rx = shutdown_rx.clone();

            let handle = tokio::spawn(async move {
                if let Err(error) = sink::consume_messages(
                    plugin_id,
                    decoder,
                    batch_size,
                    callback,
                    transforms,
                    consumer,
                    verbose,
                    &plugin_key,
                    &metrics_clone,
                    shutdown_rx,
                )
                .await
                {
                    error!(
                        "Failed to consume messages for sink connector with ID: {plugin_id}: {error}"
                    );
                }
            });
            task_handles.push(handle);
        }

        {
            let mut details = details_arc.lock().await;
            details.info.id = plugin_id;
            details.info.status = ConnectorStatus::Running;
            details.info.last_error = None;
            details.config = config.clone();
            details.shutdown_tx = Some(shutdown_tx);
            details.task_handles = task_handles;
            metrics.increment_sinks_running();
        }

        Ok(())
    }

    pub async fn restart_connector(
        &self,
        key: &str,
        config_provider: &dyn ConnectorsConfigProvider,
        iggy_client: &IggyClient,
        metrics: &Arc<Metrics>,
    ) -> Result<(), RuntimeError> {
        info!("Restarting sink connector: {key}");
        self.stop_connector(key, metrics).await?;

        let config = config_provider
            .get_sink_config(key, None)
            .await
            .map_err(|e| RuntimeError::InvalidConfiguration(e.to_string()))?
            .ok_or_else(|| RuntimeError::SinkNotFound(key.to_string()))?;

        self.start_connector(key, &config, iggy_client, metrics)
            .await?;
        info!("Sink connector: {key} restarted successfully.");
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SinkInfo {
    pub id: u32,
    pub key: String,
    pub name: String,
    pub path: String,
    pub version: String,
    pub enabled: bool,
    pub status: ConnectorStatus,
    pub last_error: Option<ConnectorError>,
    pub plugin_config_format: Option<ConfigFormat>,
}

pub struct SinkDetails {
    pub info: SinkInfo,
    pub config: SinkConfig,
    pub shutdown_tx: Option<watch::Sender<()>>,
    pub task_handles: Vec<JoinHandle<()>>,
    pub container: Option<Arc<Container<SinkApi>>>,
}

impl fmt::Debug for SinkDetails {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SinkDetails")
            .field("info", &self.info)
            .field("config", &self.config)
            .field("container", &self.container.as_ref().map(|_| "..."))
            .finish()
    }
}
