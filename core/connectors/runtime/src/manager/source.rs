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
use crate::SourceApi;
use crate::configs::connectors::{ConfigFormat, ConnectorsConfigProvider, SourceConfig};
use crate::context::RuntimeContext;
use crate::error::RuntimeError;
use crate::metrics::Metrics;
use crate::source;
use crate::state::{StateProvider, StateStorage};
use dashmap::DashMap;
use dlopen2::wrapper::Container;
use iggy::prelude::IggyClient;
use iggy_connector_sdk::api::{ConnectorError, ConnectorStatus};
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tracing::info;

#[derive(Debug)]
pub struct SourceManager {
    sources: DashMap<String, Arc<Mutex<SourceDetails>>>,
}

impl SourceManager {
    pub fn new(sources: Vec<SourceDetails>) -> Self {
        Self {
            sources: DashMap::from_iter(
                sources
                    .into_iter()
                    .map(|source| (source.info.key.to_owned(), Arc::new(Mutex::new(source))))
                    .collect::<HashMap<_, _>>(),
            ),
        }
    }

    pub async fn get(&self, key: &str) -> Option<Arc<Mutex<SourceDetails>>> {
        self.sources.get(key).map(|entry| entry.value().clone())
    }

    pub async fn get_config(&self, key: &str) -> Option<SourceConfig> {
        if let Some(source) = self.sources.get(key).map(|entry| entry.value().clone()) {
            let source = source.lock().await;
            Some(source.config.clone())
        } else {
            None
        }
    }

    pub async fn get_all(&self) -> Vec<SourceInfo> {
        let sources = &self.sources;
        let mut results = Vec::with_capacity(sources.len());
        for source in sources.iter().map(|entry| entry.value().clone()) {
            let source = source.lock().await;
            results.push(source.info.clone());
        }
        results
    }

    pub async fn update_status(
        &self,
        key: &str,
        status: ConnectorStatus,
        metrics: Option<&Arc<Metrics>>,
    ) {
        if let Some(source) = self.sources.get(key) {
            let mut source = source.lock().await;
            let old_status = source.info.status;
            source.info.status = status;
            if matches!(status, ConnectorStatus::Running | ConnectorStatus::Stopped) {
                source.info.last_error = None;
            }
            if let Some(metrics) = metrics {
                if old_status != ConnectorStatus::Running && status == ConnectorStatus::Running {
                    metrics.increment_sources_running();
                } else if old_status == ConnectorStatus::Running
                    && status != ConnectorStatus::Running
                {
                    metrics.decrement_sources_running();
                }
            }
        }
    }

    pub async fn set_error(&self, key: &str, error_message: &str) {
        if let Some(source) = self.sources.get(key) {
            let mut source = source.lock().await;
            source.info.status = ConnectorStatus::Error;
            source.info.last_error = Some(ConnectorError::new(error_message));
        }
    }

    pub async fn stop_connector(
        &self,
        key: &str,
        metrics: &Arc<Metrics>,
    ) -> Result<(), RuntimeError> {
        let details_arc = self
            .sources
            .get(key)
            .map(|e| e.value().clone())
            .ok_or_else(|| RuntimeError::SourceNotFound(key.to_string()))?;

        let (task_handles, plugin_id, container) = {
            let mut details = details_arc.lock().await;
            (
                std::mem::take(&mut details.handler_tasks),
                details.info.id,
                details.container.clone(),
            )
        };

        source::cleanup_sender(plugin_id);

        for handle in task_handles {
            let _ = tokio::time::timeout(Duration::from_secs(5), handle).await;
        }

        if let Some(container) = &container {
            info!("Closing source connector with ID: {plugin_id} for plugin: {key}");
            (container.iggy_source_close)(plugin_id);
            info!("Closed source connector with ID: {plugin_id} for plugin: {key}");
        }

        {
            let mut details = details_arc.lock().await;
            let old_status = details.info.status;
            details.info.status = ConnectorStatus::Stopped;
            details.info.last_error = None;
            if old_status == ConnectorStatus::Running {
                metrics.decrement_sources_running();
            }
        }

        Ok(())
    }

    pub async fn start_connector(
        &self,
        key: &str,
        config: &SourceConfig,
        iggy_client: &IggyClient,
        metrics: &Arc<Metrics>,
        state_path: &str,
        context: &Arc<RuntimeContext>,
    ) -> Result<(), RuntimeError> {
        let details_arc = self
            .sources
            .get(key)
            .map(|e| e.value().clone())
            .ok_or_else(|| RuntimeError::SourceNotFound(key.to_string()))?;

        let container = {
            let details = details_arc.lock().await;
            details.container.clone().ok_or_else(|| {
                RuntimeError::InvalidConfiguration(format!("No container loaded for source: {key}"))
            })?
        };

        let plugin_id = PLUGIN_ID.fetch_add(1, Ordering::SeqCst);

        let state_storage = source::get_state_storage(state_path, key);
        let state = match &state_storage {
            StateStorage::File(file) => file.load().await?,
        };

        source::init_source(
            &container,
            &config.plugin_config.clone().unwrap_or_default(),
            plugin_id,
            state,
        )?;
        info!("Source connector with ID: {plugin_id} for plugin: {key} initialized successfully.");

        let (producer, encoder, transforms) =
            source::setup_source_producer(config, iggy_client).await?;

        let (sender, receiver) = flume::unbounded();
        source::SOURCE_SENDERS.insert(plugin_id, sender);

        let callback = container.iggy_source_handle;
        tokio::task::spawn_blocking(move || {
            callback(plugin_id, source::handle_produced_messages);
        });

        let plugin_key = key.to_string();
        let verbose = config.verbose;
        let context_clone = context.clone();
        let handler_task = tokio::spawn(async move {
            source::source_forwarding_loop(
                plugin_id,
                plugin_key,
                verbose,
                producer,
                encoder,
                transforms,
                state_storage,
                receiver,
                context_clone,
            )
            .await;
        });

        {
            let mut details = details_arc.lock().await;
            details.info.id = plugin_id;
            details.info.status = ConnectorStatus::Running;
            details.info.last_error = None;
            details.config = config.clone();
            details.handler_tasks = vec![handler_task];
            metrics.increment_sources_running();
        }

        Ok(())
    }

    pub async fn restart_connector(
        &self,
        key: &str,
        config_provider: &dyn ConnectorsConfigProvider,
        iggy_client: &IggyClient,
        metrics: &Arc<Metrics>,
        state_path: &str,
        context: &Arc<RuntimeContext>,
    ) -> Result<(), RuntimeError> {
        info!("Restarting source connector: {key}");
        self.stop_connector(key, metrics).await?;

        let config = config_provider
            .get_source_config(key, None)
            .await
            .map_err(|e| RuntimeError::InvalidConfiguration(e.to_string()))?
            .ok_or_else(|| RuntimeError::SourceNotFound(key.to_string()))?;

        self.start_connector(key, &config, iggy_client, metrics, state_path, context)
            .await?;
        info!("Source connector: {key} restarted successfully.");
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SourceInfo {
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

pub struct SourceDetails {
    pub info: SourceInfo,
    pub config: SourceConfig,
    pub handler_tasks: Vec<JoinHandle<()>>,
    pub container: Option<Arc<Container<SourceApi>>>,
}

impl fmt::Debug for SourceDetails {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SourceDetails")
            .field("info", &self.info)
            .field("config", &self.config)
            .field("container", &self.container.as_ref().map(|_| "..."))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configs::connectors::SourceConfig;

    fn create_test_source_info(key: &str, id: u32) -> SourceInfo {
        SourceInfo {
            id,
            key: key.to_string(),
            name: format!("{key} source"),
            path: format!("/path/to/{key}"),
            version: "1.0.0".to_string(),
            enabled: true,
            status: ConnectorStatus::Running,
            last_error: None,
            plugin_config_format: None,
        }
    }

    fn create_test_source_details(key: &str, id: u32) -> SourceDetails {
        SourceDetails {
            info: create_test_source_info(key, id),
            config: SourceConfig {
                key: key.to_string(),
                enabled: true,
                version: 1,
                name: format!("{key} source"),
                path: format!("/path/to/{key}"),
                ..Default::default()
            },
            handler_tasks: vec![],
            container: None,
        }
    }

    #[tokio::test]
    async fn should_create_manager_with_sources() {
        let manager = SourceManager::new(vec![
            create_test_source_details("pg", 1),
            create_test_source_details("random", 2),
        ]);

        let all = manager.get_all().await;
        assert_eq!(all.len(), 2);
    }

    #[tokio::test]
    async fn should_get_existing_source() {
        let manager = SourceManager::new(vec![create_test_source_details("pg", 1)]);

        let source = manager.get("pg").await;
        assert!(source.is_some());
        let binding = source.unwrap();
        let details = binding.lock().await;
        assert_eq!(details.info.key, "pg");
        assert_eq!(details.info.id, 1);
    }

    #[tokio::test]
    async fn should_return_none_for_unknown_key() {
        let manager = SourceManager::new(vec![create_test_source_details("pg", 1)]);

        assert!(manager.get("nonexistent").await.is_none());
    }

    #[tokio::test]
    async fn should_get_config() {
        let manager = SourceManager::new(vec![create_test_source_details("pg", 1)]);

        let config = manager.get_config("pg").await;
        assert!(config.is_some());
        assert_eq!(config.unwrap().key, "pg");
    }

    #[tokio::test]
    async fn should_return_none_config_for_unknown_key() {
        let manager = SourceManager::new(vec![]);

        assert!(manager.get_config("nonexistent").await.is_none());
    }

    #[tokio::test]
    async fn should_get_all_sources() {
        let manager = SourceManager::new(vec![
            create_test_source_details("pg", 1),
            create_test_source_details("random", 2),
            create_test_source_details("es", 3),
        ]);

        let all = manager.get_all().await;
        assert_eq!(all.len(), 3);
        let keys: Vec<String> = all.iter().map(|s| s.key.clone()).collect();
        assert!(keys.contains(&"pg".to_string()));
        assert!(keys.contains(&"random".to_string()));
        assert!(keys.contains(&"es".to_string()));
    }

    #[tokio::test]
    async fn should_update_status() {
        let manager = SourceManager::new(vec![create_test_source_details("pg", 1)]);

        manager
            .update_status("pg", ConnectorStatus::Stopped, None)
            .await;

        let source = manager.get("pg").await.unwrap();
        let details = source.lock().await;
        assert_eq!(details.info.status, ConnectorStatus::Stopped);
    }

    #[tokio::test]
    async fn should_increment_metrics_when_transitioning_to_running() {
        let metrics = Arc::new(Metrics::init());
        let mut details = create_test_source_details("pg", 1);
        details.info.status = ConnectorStatus::Stopped;
        let manager = SourceManager::new(vec![details]);

        manager
            .update_status("pg", ConnectorStatus::Running, Some(&metrics))
            .await;

        assert_eq!(metrics.get_sources_running(), 1);
    }

    #[tokio::test]
    async fn should_decrement_metrics_when_leaving_running() {
        let metrics = Arc::new(Metrics::init());
        let manager = SourceManager::new(vec![create_test_source_details("pg", 1)]);
        metrics.increment_sources_running();

        manager
            .update_status("pg", ConnectorStatus::Stopped, Some(&metrics))
            .await;

        assert_eq!(metrics.get_sources_running(), 0);
    }

    #[tokio::test]
    async fn should_clear_error_when_status_becomes_running() {
        let manager = SourceManager::new(vec![create_test_source_details("pg", 1)]);
        manager.set_error("pg", "some error").await;

        manager
            .update_status("pg", ConnectorStatus::Running, None)
            .await;

        let source = manager.get("pg").await.unwrap();
        let details = source.lock().await;
        assert!(details.info.last_error.is_none());
    }

    #[tokio::test]
    async fn should_set_error_status_and_message() {
        let manager = SourceManager::new(vec![create_test_source_details("pg", 1)]);

        manager.set_error("pg", "connection failed").await;

        let source = manager.get("pg").await.unwrap();
        let details = source.lock().await;
        assert_eq!(details.info.status, ConnectorStatus::Error);
        assert!(details.info.last_error.is_some());
    }

    #[tokio::test]
    async fn stop_should_return_not_found_for_unknown_key() {
        let metrics = Arc::new(Metrics::init());
        let manager = SourceManager::new(vec![]);

        let result = manager.stop_connector("nonexistent", &metrics).await;
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(matches!(err, RuntimeError::SourceNotFound(_)));
    }

    #[tokio::test]
    async fn stop_should_drain_tasks_and_update_status() {
        let metrics = Arc::new(Metrics::init());
        metrics.increment_sources_running();
        let handle = tokio::spawn(async {});
        let mut details = create_test_source_details("pg", 1);
        details.handler_tasks = vec![handle];
        let manager = SourceManager::new(vec![details]);

        let result = manager.stop_connector("pg", &metrics).await;
        assert!(result.is_ok());

        let source = manager.get("pg").await.unwrap();
        let details = source.lock().await;
        assert_eq!(details.info.status, ConnectorStatus::Stopped);
        assert!(details.handler_tasks.is_empty());
    }

    #[tokio::test]
    async fn stop_should_work_without_container() {
        let metrics = Arc::new(Metrics::init());
        let mut details = create_test_source_details("pg", 1);
        details.container = None;
        details.info.status = ConnectorStatus::Stopped;
        let manager = SourceManager::new(vec![details]);

        let result = manager.stop_connector("pg", &metrics).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn stop_should_decrement_metrics_from_running() {
        let metrics = Arc::new(Metrics::init());
        metrics.increment_sources_running();
        let manager = SourceManager::new(vec![create_test_source_details("pg", 1)]);

        manager.stop_connector("pg", &metrics).await.unwrap();

        assert_eq!(metrics.get_sources_running(), 0);
    }

    #[tokio::test]
    async fn should_clear_error_when_status_becomes_stopped() {
        let manager = SourceManager::new(vec![create_test_source_details("pg", 1)]);
        manager.set_error("pg", "some error").await;

        manager
            .update_status("pg", ConnectorStatus::Stopped, None)
            .await;

        let source = manager.get("pg").await.unwrap();
        let details = source.lock().await;
        assert_eq!(details.info.status, ConnectorStatus::Stopped);
        assert!(details.info.last_error.is_none());
    }

    #[tokio::test]
    async fn stop_should_clear_last_error() {
        let metrics = Arc::new(Metrics::init());
        let mut details = create_test_source_details("pg", 1);
        details.info.status = ConnectorStatus::Error;
        details.info.last_error = Some(ConnectorError::new("previous error"));
        let manager = SourceManager::new(vec![details]);

        manager.stop_connector("pg", &metrics).await.unwrap();

        let source = manager.get("pg").await.unwrap();
        let details = source.lock().await;
        assert!(details.info.last_error.is_none());
    }

    #[tokio::test]
    async fn stop_should_not_decrement_metrics_from_non_running() {
        let metrics = Arc::new(Metrics::init());
        let mut details = create_test_source_details("pg", 1);
        details.info.status = ConnectorStatus::Stopped;
        let manager = SourceManager::new(vec![details]);

        manager.stop_connector("pg", &metrics).await.unwrap();

        assert_eq!(metrics.get_sources_running(), 0);
    }

    #[tokio::test]
    async fn update_status_should_be_noop_for_unknown_key() {
        let manager = SourceManager::new(vec![]);

        manager
            .update_status("nonexistent", ConnectorStatus::Running, None)
            .await;
    }

    #[tokio::test]
    async fn set_error_should_be_noop_for_unknown_key() {
        let manager = SourceManager::new(vec![]);

        manager.set_error("nonexistent", "some error").await;
    }
}
