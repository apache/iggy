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

use crate::PLUGIN_ID;
use crate::SourceApi;
use crate::configs::connectors::{ConfigFormat, ConnectorsConfigProvider, SourceConfig};
use crate::context::RuntimeContext;
use crate::error::RuntimeError;
use crate::metrics::Metrics;
use crate::source;
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
use tracing::{info, warn};

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
            source.lock().await.apply_status(status, metrics);
        }
    }

    pub async fn set_error(&self, key: &str, error_message: &str, metrics: Option<&Arc<Metrics>>) {
        if let Some(source) = self.sources.get(key) {
            let mut source = source.lock().await;
            // Through the shared transition, so leaving `Running` moves the
            // gauge. Skipping it left an errored instance counted as running,
            // and the loop's later `Stopped` could not correct that either,
            // because by then the old status was `Error` and neither branch
            // fires.
            //
            // The message is assigned after the transition, and that ordering is
            // what preserves it. `Error` being outside the set that clears
            // `last_error` is belt and braces here, not the mechanism.
            source.apply_status(ConnectorStatus::Error, metrics);
            source.info.last_error = Some(ConnectorError::new(error_message));
        }
    }

    pub async fn is_stopping_or_stopped(&self, key: &str) -> bool {
        let Some(source) = self.sources.get(key).map(|entry| entry.value().clone()) else {
            return true;
        };
        let source = source.lock().await;
        matches!(
            source.info.status,
            ConnectorStatus::Stopping | ConnectorStatus::Stopped
        )
    }

    pub async fn stop_connector_with_guard(
        &self,
        key: &str,
        metrics: &Arc<Metrics>,
    ) -> Result<(), RuntimeError> {
        let guard = {
            let details = self
                .sources
                .get(key)
                .map(|e| e.value().clone())
                .ok_or_else(|| RuntimeError::SourceNotFound(key.to_string()))?;
            let details = details.lock().await;
            details.restart_guard.clone()
        };
        let _lock = guard.lock().await;
        self.stop_connector(key, metrics).await
    }

    pub async fn stop_connector(
        &self,
        key: &str,
        metrics: &Arc<Metrics>,
    ) -> Result<(), RuntimeError> {
        let details = self
            .sources
            .get(key)
            .map(|e| e.value().clone())
            .ok_or_else(|| RuntimeError::SourceNotFound(key.to_string()))?;

        self.update_status(key, ConnectorStatus::Stopping, Some(metrics))
            .await;

        let (task_handles, plugin_id, container) = {
            let mut details = details.lock().await;
            (
                std::mem::take(&mut details.handler_tasks),
                details.info.id,
                details.container.clone(),
            )
        };

        // The result callback can race with close, so the forwarding loop
        // treats callback failures during Stopping as expected shutdown.
        if let Some(container) = &container {
            info!("Closing source connector with ID: {plugin_id} for plugin: {key}");
            (container.iggy_source_close)(plugin_id);
            info!("Closed source connector with ID: {plugin_id} for plugin: {key}");
        }

        source::cleanup_sender(plugin_id);

        for mut handle in task_handles {
            if tokio::time::timeout(Duration::from_secs(5), &mut handle)
                .await
                .is_err()
            {
                warn!(
                    "Timed out waiting for source task to finish (plugin_id: {plugin_id}); aborting to prevent stale collisions with the next start."
                );
                handle.abort();
                let _ = handle.await;
            }
        }

        {
            let mut details = details.lock().await;
            details.info.status = ConnectorStatus::Stopped;
            details.info.last_error = None;
        }

        Ok(())
    }

    pub async fn start_connector(
        &self,
        key: &str,
        config: &SourceConfig,
        iggy_client: &IggyClient,
        metrics: &Arc<Metrics>,
        context: &Arc<RuntimeContext>,
    ) -> Result<(), RuntimeError> {
        let details = self
            .sources
            .get(key)
            .map(|e| e.value().clone())
            .ok_or_else(|| RuntimeError::SourceNotFound(key.to_string()))?;

        let container = {
            let details = details.lock().await;
            details.container.clone().ok_or_else(|| {
                RuntimeError::InvalidConfiguration(format!("No container loaded for source: {key}"))
            })?
        };

        let plugin_id = PLUGIN_ID.fetch_add(1, Ordering::SeqCst);

        let state_storage = context.state_factory.storage_for(key)?;
        let state = state_storage
            .load()
            .await
            .map_err(|load_error| match load_error {
                iggy_connector_sdk::Error::TransientState(_)
                | iggy_connector_sdk::Error::PermanentState(_)
                | iggy_connector_sdk::Error::StateLatched => RuntimeError::StateLoadFailed {
                    connector_key: key.to_string(),
                    source: load_error,
                },
                other => RuntimeError::ConnectorSdkError(other),
            })?;

        source::init_source(
            &container,
            &config.plugin_config.clone().unwrap_or_default(),
            plugin_id,
            state,
        )?;
        info!("Source connector with ID: {plugin_id} for plugin: {key} initialized successfully.");
        // Armed from here until the id is recorded below. `SourceInstanceGuard`
        // carries why that window strands the instance.
        let instance_guard =
            source::SourceInstanceGuard::for_container(container.clone(), plugin_id, key);

        let (producer, encoder, transforms) =
            match source::setup_source_producer(key, config, iggy_client).await {
                Ok(parts) => parts,
                Err(error) => {
                    // Closed here rather than left to `drop` so this error
                    // reaches the caller after teardown, not alongside it.
                    // `drop` stays the net for a cancellation, and for any `?`
                    // added inside this window later.
                    instance_guard.close().await;
                    return Err(error);
                }
            };

        let handle_callback = container.iggy_source_handle_v2;
        let batch_result_callback = container.iggy_source_batch_result;

        // The lock is taken before the spawn so nothing can await between
        // registering the tasks and recording the id that reaches them. A
        // cancellation in that gap left the `SOURCE_SENDERS` entry and both
        // spawned tasks behind with no id naming them, and the forwarding loop
        // then ran for the life of the process. The guard closes the plugin
        // instance on that path but cannot reach either of those.
        //
        // The forwarding loop's own first act is to take this lock, so it waits
        // for this block to end rather than racing it.
        {
            let mut details = details.lock().await;
            details.record_started(plugin_id, config, || {
                source::spawn_source_handler(
                    plugin_id,
                    key,
                    config.verbose,
                    config.benchmark,
                    producer,
                    encoder,
                    transforms,
                    state_storage,
                    handle_callback,
                    batch_result_callback,
                    context.clone(),
                )
            });
            // In the same hold as the id record, not after it. Released first,
            // this transition raced the forwarding loop's own report and could
            // overwrite an `Error` the loop had already set.
            details.apply_status(ConnectorStatus::Running, Some(metrics));
        }
        // `details.info.id` now names this instance, so a later stop reaches it.
        instance_guard.disarm();

        Ok(())
    }

    pub async fn restart_connector(
        &self,
        key: &str,
        config_provider: &dyn ConnectorsConfigProvider,
        iggy_client: &IggyClient,
        metrics: &Arc<Metrics>,
        context: &Arc<RuntimeContext>,
    ) -> Result<(), RuntimeError> {
        let guard = {
            let details = self
                .sources
                .get(key)
                .map(|e| e.value().clone())
                .ok_or_else(|| RuntimeError::SourceNotFound(key.to_string()))?;
            let details = details.lock().await;
            details.restart_guard.clone()
        };
        let Ok(_lock) = guard.try_lock() else {
            info!("Restart already in progress for source connector: {key}, skipping.");
            return Ok(());
        };

        info!("Restarting source connector: {key}");
        self.stop_connector(key, metrics).await?;

        let config = config_provider
            .get_source_config(key, None)
            .await
            .map_err(|e| RuntimeError::InvalidConfiguration(e.to_string()))?
            .ok_or_else(|| RuntimeError::SourceNotFound(key.to_string()))?;

        self.start_connector(key, &config, iggy_client, metrics, context)
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
    pub restart_guard: Arc<Mutex<()>>,
}

impl SourceDetails {
    /// Applies a status transition and the gauge move that belongs with it.
    ///
    /// On `&mut self` rather than behind a key, so it can run inside a lock the
    /// caller already holds. `update_status` takes the lock and delegates;
    /// `start_connector` calls it in the same hold as the id record.
    ///
    /// That matters: applied after releasing that lock, the initial `Running`
    /// could land after the forwarding loop had already reported `Running` and
    /// then failed its first batch, overwriting `Error`, clearing `last_error`,
    /// and counting the instance a second time.
    fn apply_status(&mut self, status: ConnectorStatus, metrics: Option<&Arc<Metrics>>) {
        let old_status = self.info.status;
        self.info.status = status;
        if matches!(status, ConnectorStatus::Running | ConnectorStatus::Stopped) {
            self.info.last_error = None;
        }
        let Some(metrics) = metrics else {
            return;
        };
        // Only a real crossing of `Running` moves the gauge, so repeated
        // reports of a status the connector already holds cost nothing.
        if old_status != ConnectorStatus::Running && status == ConnectorStatus::Running {
            metrics.increment_sources_running();
        } else if old_status == ConnectorStatus::Running && status != ConnectorStatus::Running {
            metrics.decrement_sources_running();
        }
    }

    /// Records an instance that has just started, spawning its handlers in the
    /// same breath.
    ///
    /// Deliberately not `async`, and that is the point. The id has to be
    /// recorded under the same lock hold as the spawn: a cancellation between
    /// the two strands the `SOURCE_SENDERS` entry and both tasks with nothing
    /// naming them, which no guard can reach. Taking `spawn` as a closure is
    /// what lets the compiler refuse an await added between them.
    fn record_started(
        &mut self,
        plugin_id: u32,
        config: &SourceConfig,
        spawn: impl FnOnce() -> Vec<JoinHandle<()>>,
    ) {
        self.handler_tasks = spawn();
        self.info.id = plugin_id;
        self.config = config.clone();
    }
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
            restart_guard: Arc::new(Mutex::new(())),
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
    async fn record_started_should_store_the_id_and_the_spawned_tasks() {
        // Both have to land under one lock hold, so they are recorded together
        // and there is nowhere to await between them. A stop reaches the
        // instance through the id and drains it through the tasks, so losing
        // either leaves something behind.
        let mut details = create_test_source_details("pg", 1);
        let config = details.config.clone();

        details.record_started(7, &config, || vec![tokio::spawn(async {})]);

        assert_eq!(
            details.info.id, 7,
            "a later stop closes whatever id this recorded"
        );
        assert_eq!(
            details.handler_tasks.len(),
            1,
            "a stop drains the tasks recorded here, so they cannot be dropped"
        );
    }

    #[tokio::test]
    async fn should_not_double_count_when_an_error_falls_between_two_running_reports() {
        // The interleaving spetz measured on #4064: the forwarding loop reports
        // `Running`, fails its first batch, and a second `Running` report lands
        // afterwards. That second report crosses into `Running` again, so it
        // increments a gauge the error never gave back, and the instance is
        // counted twice.
        //
        // What the consecutive-`Running` test cannot see: there the second
        // report finds the status already `Running`, so no crossing happens and
        // no arithmetic is exercised. The error in the middle is the whole point.
        let metrics = Arc::new(Metrics::init());
        let mut details = create_test_source_details("pg", 1);
        details.info.status = ConnectorStatus::Stopped;
        let manager = SourceManager::new(vec![details]);

        manager
            .update_status("pg", ConnectorStatus::Running, Some(&metrics))
            .await;
        manager
            .set_error("pg", "first batch failed", Some(&metrics))
            .await;
        assert_eq!(
            metrics.get_sources_running(),
            0,
            "an instance that has failed is not running, and the gauge has to say so \
             or nothing later can correct it"
        );

        manager
            .update_status("pg", ConnectorStatus::Running, Some(&metrics))
            .await;

        assert_eq!(
            metrics.get_sources_running(),
            1,
            "one instance, however many times its status crossed Running"
        );
    }

    #[tokio::test]
    async fn should_keep_the_error_message_when_the_status_becomes_error() {
        // `set_error` routes through a transition that clears `last_error` for
        // some statuses, so its observable contract is worth pinning: the
        // status ends `Error` and the message survives.
        //
        // Worth knowing what this does NOT pin. The message is assigned after
        // the transition, so widening the clear to include `Error` leaves this
        // green; checked, and the mutant survives. The ordering is the
        // mechanism, and no unit test can see an ordering inside one lock hold.
        let metrics = Arc::new(Metrics::init());
        let manager = SourceManager::new(vec![create_test_source_details("pg", 1)]);

        manager
            .set_error("pg", "producer setup failed", Some(&metrics))
            .await;

        let source = manager.get("pg").await.expect("source must exist");
        let source = source.lock().await;
        assert_eq!(source.info.status, ConnectorStatus::Error);
        assert_eq!(
            source
                .info
                .last_error
                .as_ref()
                .map(|error| error.message.as_str()),
            Some("producer setup failed"),
            "the transition must not clear the message set right after it"
        );
    }

    #[tokio::test]
    async fn should_increment_metrics_once_when_running_is_reported_twice() {
        // Both a start and the forwarding loop report `Running` for the same
        // instance, so the gauge has to count instances rather than reports.
        let metrics = Arc::new(Metrics::init());
        let mut details = create_test_source_details("pg", 1);
        details.info.status = ConnectorStatus::Stopped;
        let manager = SourceManager::new(vec![details]);

        manager
            .update_status("pg", ConnectorStatus::Running, Some(&metrics))
            .await;
        manager
            .update_status("pg", ConnectorStatus::Running, Some(&metrics))
            .await;

        assert_eq!(
            metrics.get_sources_running(),
            1,
            "a second report of a status the connector already has must not move the gauge"
        );
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
        manager.set_error("pg", "some error", None).await;

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

        manager.set_error("pg", "connection failed", None).await;

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
        manager.set_error("pg", "some error", None).await;

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

        manager.set_error("nonexistent", "some error", None).await;
    }
}
