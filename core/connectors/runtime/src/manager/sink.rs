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
                RuntimeError::InvalidConfiguration(format!("No container loaded for sink: {key}"))
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::connectors::SinkConfig;

    fn create_test_sink_info(key: &str, id: u32) -> SinkInfo {
        SinkInfo {
            id,
            key: key.to_string(),
            name: format!("{key} sink"),
            path: format!("/path/to/{key}"),
            version: "1.0.0".to_string(),
            enabled: true,
            status: ConnectorStatus::Running,
            last_error: None,
            plugin_config_format: None,
        }
    }

    fn create_test_sink_details(key: &str, id: u32) -> SinkDetails {
        SinkDetails {
            info: create_test_sink_info(key, id),
            config: SinkConfig {
                key: key.to_string(),
                enabled: true,
                version: 1,
                name: format!("{key} sink"),
                path: format!("/path/to/{key}"),
                ..Default::default()
            },
            shutdown_tx: None,
            task_handles: vec![],
            container: None,
        }
    }

    #[tokio::test]
    async fn should_create_manager_with_sinks() {
        let manager = SinkManager::new(vec![
            create_test_sink_details("es", 1),
            create_test_sink_details("pg", 2),
        ]);

        let all = manager.get_all().await;
        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn should_get_existing_sink() {
        let manager = SinkManager::new(vec![create_test_sink_details("es", 1)]);

        let sink = manager.get("es").await;
        assert!(sink.is_some());
        let binding = sink.unwrap();
        let details = binding.lock().await;
        assert_eq!(details.info.key, "es");
        assert_eq!(details.info.id, 1);
    }

    #[tokio::test]
    async fn should_return_none_for_unknown_key() {
        let manager = SinkManager::new(vec![create_test_sink_details("es", 1)]);

        assert!(manager.get("nonexistent").await.is_none());
    }

    #[tokio::test]
    async fn should_get_config() {
        let manager = SinkManager::new(vec![create_test_sink_details("es", 1)]);

        let config = manager.get_config("es").await;
        assert!(config.is_some());
        assert_eq!(config.unwrap().key, "es");
    }

    #[tokio::test]
    async fn should_return_none_config_for_unknown_key() {
        let manager = SinkManager::new(vec![]);

        assert!(manager.get_config("nonexistent").await.is_none());
    }

    #[tokio::test]
    async fn should_get_all_sinks() {
        let manager = SinkManager::new(vec![
            create_test_sink_details("es", 1),
            create_test_sink_details("pg", 2),
            create_test_sink_details("stdout", 3),
        ]);

        let all = manager.get_all().await;
        assert_eq!(all.len(), 3);
        let keys: Vec<String> = all.iter().map(|s| s.key.clone()).collect();
        assert!(keys.contains(&"es".to_string()));
        assert!(keys.contains(&"pg".to_string()));
        assert!(keys.contains(&"stdout".to_string()));
    }

    #[tokio::test]
    async fn should_update_status() {
        let manager = SinkManager::new(vec![create_test_sink_details("es", 1)]);

        manager
            .update_status("es", ConnectorStatus::Stopped, None)
            .await;

        let sink = manager.get("es").await.unwrap();
        let details = sink.lock().await;
        assert_eq!(details.info.status, ConnectorStatus::Stopped);
    }

    #[tokio::test]
    async fn should_increment_metrics_when_transitioning_to_running() {
        let metrics = Arc::new(Metrics::init());
        let mut details = create_test_sink_details("es", 1);
        details.info.status = ConnectorStatus::Stopped;
        let manager = SinkManager::new(vec![details]);

        manager
            .update_status("es", ConnectorStatus::Running, Some(&metrics))
            .await;

        assert_eq!(metrics.get_sinks_running(), 1);
    }

    #[tokio::test]
    async fn should_decrement_metrics_when_leaving_running() {
        let metrics = Arc::new(Metrics::init());
        let manager = SinkManager::new(vec![create_test_sink_details("es", 1)]);
        metrics.increment_sinks_running();

        manager
            .update_status("es", ConnectorStatus::Stopped, Some(&metrics))
            .await;

        assert_eq!(metrics.get_sinks_running(), 0);
    }

    #[tokio::test]
    async fn should_clear_error_when_status_becomes_running() {
        let manager = SinkManager::new(vec![create_test_sink_details("es", 1)]);
        manager.set_error("es", "some error").await;

        manager
            .update_status("es", ConnectorStatus::Running, None)
            .await;

        let sink = manager.get("es").await.unwrap();
        let details = sink.lock().await;
        assert!(details.info.last_error.is_none());
    }

    #[tokio::test]
    async fn should_set_error_status_and_message() {
        let manager = SinkManager::new(vec![create_test_sink_details("es", 1)]);

        manager.set_error("es", "connection failed").await;

        let sink = manager.get("es").await.unwrap();
        let details = sink.lock().await;
        assert_eq!(details.info.status, ConnectorStatus::Error);
        assert!(details.info.last_error.is_some());
    }

    #[tokio::test]
    async fn stop_should_return_not_found_for_unknown_key() {
        let metrics = Arc::new(Metrics::init());
        let manager = SinkManager::new(vec![]);

        let result = manager.stop_connector("nonexistent", &metrics).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, RuntimeError::SinkNotFound(_)));
    }

    #[tokio::test]
    async fn stop_should_send_shutdown_signal_and_update_status() {
        let metrics = Arc::new(Metrics::init());
        metrics.increment_sinks_running();
        let (shutdown_tx, mut shutdown_rx) = watch::channel(());
        let handle = tokio::spawn(async move {
            let _ = shutdown_rx.changed().await;
        });
        let mut details = create_test_sink_details("es", 1);
        details.shutdown_tx = Some(shutdown_tx);
        details.task_handles = vec![handle];
        let manager = SinkManager::new(vec![details]);

        let result = manager.stop_connector("es", &metrics).await;
        assert!(result.is_ok());

        let sink = manager.get("es").await.unwrap();
        let details = sink.lock().await;
        assert_eq!(details.info.status, ConnectorStatus::Stopped);
        assert!(details.shutdown_tx.is_none());
        assert!(details.task_handles.is_empty());
    }

    #[tokio::test]
    async fn stop_should_work_without_container() {
        let metrics = Arc::new(Metrics::init());
        let mut details = create_test_sink_details("es", 1);
        details.container = None;
        details.info.status = ConnectorStatus::Stopped;
        let manager = SinkManager::new(vec![details]);

        let result = manager.stop_connector("es", &metrics).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn stop_should_decrement_metrics_from_running() {
        let metrics = Arc::new(Metrics::init());
        metrics.increment_sinks_running();
        let manager = SinkManager::new(vec![create_test_sink_details("es", 1)]);

        manager.stop_connector("es", &metrics).await.unwrap();

        assert_eq!(metrics.get_sinks_running(), 0);
    }

    #[tokio::test]
    async fn should_clear_error_when_status_becomes_stopped() {
        let manager = SinkManager::new(vec![create_test_sink_details("es", 1)]);
        manager.set_error("es", "some error").await;

        manager
            .update_status("es", ConnectorStatus::Stopped, None)
            .await;

        let sink = manager.get("es").await.unwrap();
        let details = sink.lock().await;
        assert_eq!(details.info.status, ConnectorStatus::Stopped);
        assert!(details.info.last_error.is_none());
    }

    #[tokio::test]
    async fn stop_should_clear_last_error() {
        let metrics = Arc::new(Metrics::init());
        let mut details = create_test_sink_details("es", 1);
        details.info.status = ConnectorStatus::Error;
        details.info.last_error = Some(ConnectorError::new("previous error"));
        let manager = SinkManager::new(vec![details]);

        manager.stop_connector("es", &metrics).await.unwrap();

        let sink = manager.get("es").await.unwrap();
        let details = sink.lock().await;
        assert!(details.info.last_error.is_none());
    }

    #[tokio::test]
    async fn stop_should_not_decrement_metrics_from_non_running() {
        let metrics = Arc::new(Metrics::init());
        let mut details = create_test_sink_details("es", 1);
        details.info.status = ConnectorStatus::Stopped;
        let manager = SinkManager::new(vec![details]);

        manager.stop_connector("es", &metrics).await.unwrap();

        assert_eq!(metrics.get_sinks_running(), 0);
    }

    #[tokio::test]
    async fn update_status_should_be_noop_for_unknown_key() {
        let manager = SinkManager::new(vec![]);

        manager
            .update_status("nonexistent", ConnectorStatus::Running, None)
            .await;
    }

    #[tokio::test]
    async fn set_error_should_be_noop_for_unknown_key() {
        let manager = SinkManager::new(vec![]);

        manager.set_error("nonexistent", "some error").await;
    }
}
