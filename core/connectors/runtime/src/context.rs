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
use crate::configs::connectors::{ConnectorsConfigProvider, SinkConfig, SourceConfig};
use crate::configs::runtime::ConnectorsRuntimeConfig;
use crate::metrics::Metrics;
use crate::stream::IggyClients;
use crate::{
    FailedPlugin, SinkConnectorWrapper, SourceConnectorWrapper,
    manager::{
        sink::{SinkDetails, SinkInfo, SinkManager},
        source::{SourceDetails, SourceInfo, SourceManager},
    },
};
use iggy_common::IggyTimestamp;
use iggy_connector_sdk::api::ConnectorError;
use iggy_connector_sdk::api::ConnectorStatus;
use secrecy::SecretString;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::error;

pub struct RuntimeContext {
    pub sinks: SinkManager,
    pub sources: SourceManager,
    pub api_key: SecretString,
    pub config_provider: Arc<dyn ConnectorsConfigProvider>,
    pub metrics: Arc<Metrics>,
    pub start_time: IggyTimestamp,
    pub iggy_clients: Arc<IggyClients>,
    pub state_path: String,
}

#[allow(clippy::too_many_arguments)]
pub fn init(
    config: &ConnectorsRuntimeConfig,
    sinks_config: &HashMap<String, SinkConfig>,
    sources_config: &HashMap<String, SourceConfig>,
    sink_wrappers: &[SinkConnectorWrapper],
    source_wrappers: &[SourceConnectorWrapper],
    failed_sinks: &[FailedPlugin],
    failed_sources: &[FailedPlugin],
    config_provider: Box<dyn ConnectorsConfigProvider>,
    iggy_clients: Arc<IggyClients>,
    state_path: String,
) -> RuntimeContext {
    let metrics = Arc::new(Metrics::init());
    let mut sink_details = map_sinks(sinks_config, sink_wrappers);
    sink_details.extend(map_failed_sinks(sinks_config, failed_sinks));
    let mut source_details = map_sources(sources_config, source_wrappers);
    source_details.extend(map_failed_sources(sources_config, failed_sources));

    let sinks = SinkManager::new(sink_details);
    let sources = SourceManager::new(source_details);

    metrics.set_sinks_total(sinks_config.len() as u32);
    metrics.set_sources_total(sources_config.len() as u32);

    RuntimeContext {
        sinks,
        sources,
        api_key: config.http.api_key.clone(),
        config_provider: Arc::from(config_provider),
        metrics,
        start_time: IggyTimestamp::now(),
        iggy_clients,
        state_path,
    }
}

fn map_sinks(
    sinks_config: &HashMap<String, SinkConfig>,
    sink_wrappers: &[SinkConnectorWrapper],
) -> Vec<SinkDetails> {
    let mut sinks = vec![];
    for sink_wrapper in sink_wrappers.iter() {
        for sink_plugin in sink_wrapper.plugins.iter() {
            let Some(sink_config) = sinks_config.get(&sink_plugin.key) else {
                error!("Missing sink config for: {}", sink_plugin.key);
                continue;
            };

            let status = if sink_plugin.error.is_some() {
                ConnectorStatus::Error
            } else if sink_config.enabled {
                ConnectorStatus::Starting
            } else {
                ConnectorStatus::Stopped
            };

            sinks.push(SinkDetails {
                info: SinkInfo {
                    id: sink_plugin.id,
                    key: sink_plugin.key.to_owned(),
                    name: sink_plugin.name.to_owned(),
                    path: sink_plugin.path.to_owned(),
                    version: sink_plugin.version.to_owned(),
                    enabled: sink_config.enabled,
                    status,
                    last_error: sink_plugin
                        .error
                        .as_ref()
                        .map(|err| ConnectorError::new(&err.to_string())),
                    plugin_config_format: sink_plugin.config_format,
                },
                config: sink_config.clone(),
                shutdown_tx: None,
                task_handles: vec![],
                container: None,
                restart_guard: Arc::new(Mutex::new(())),
            });
        }
    }
    sinks
}

fn map_sources(
    sources_config: &HashMap<String, SourceConfig>,
    source_wrappers: &[SourceConnectorWrapper],
) -> Vec<SourceDetails> {
    let mut sources = vec![];
    for source_wrapper in source_wrappers.iter() {
        for source_plugin in source_wrapper.plugins.iter() {
            let Some(source_config) = sources_config.get(&source_plugin.key) else {
                error!("Missing source config for: {}", source_plugin.key);
                continue;
            };

            let status = if source_plugin.error.is_some() {
                ConnectorStatus::Error
            } else if source_config.enabled {
                ConnectorStatus::Starting
            } else {
                ConnectorStatus::Stopped
            };

            sources.push(SourceDetails {
                info: SourceInfo {
                    id: source_plugin.id,
                    key: source_plugin.key.to_owned(),
                    name: source_plugin.name.to_owned(),
                    path: source_plugin.path.to_owned(),
                    version: source_plugin.version.to_owned(),
                    enabled: source_config.enabled,
                    status,
                    last_error: source_plugin
                        .error
                        .as_ref()
                        .map(|err| ConnectorError::new(&err.to_string())),
                    plugin_config_format: source_plugin.config_format,
                },
                config: source_config.clone(),
                handler_tasks: vec![],
                container: None,
                restart_guard: Arc::new(Mutex::new(())),
            });
        }
    }
    sources
}

const UNKNOWN_PLUGIN_VERSION: &str = "unknown";

fn map_failed_sinks(
    sinks_config: &HashMap<String, SinkConfig>,
    failed: &[FailedPlugin],
) -> Vec<SinkDetails> {
    let mut sinks = Vec::with_capacity(failed.len());
    for plugin in failed {
        let Some(config) = sinks_config.get(&plugin.key) else {
            error!("Missing sink config for failed plugin: {}", plugin.key);
            continue;
        };
        sinks.push(SinkDetails {
            info: SinkInfo {
                id: plugin.id,
                key: plugin.key.clone(),
                name: plugin.name.clone(),
                path: plugin.path.clone(),
                version: UNKNOWN_PLUGIN_VERSION.to_owned(),
                enabled: plugin.enabled,
                status: ConnectorStatus::Error,
                last_error: Some(ConnectorError::new(&plugin.error)),
                plugin_config_format: plugin.config_format,
            },
            config: config.clone(),
            shutdown_tx: None,
            task_handles: vec![],
            container: None,
            restart_guard: Arc::new(Mutex::new(())),
        });
    }
    sinks
}

fn map_failed_sources(
    sources_config: &HashMap<String, SourceConfig>,
    failed: &[FailedPlugin],
) -> Vec<SourceDetails> {
    let mut sources = Vec::with_capacity(failed.len());
    for plugin in failed {
        let Some(config) = sources_config.get(&plugin.key) else {
            error!("Missing source config for failed plugin: {}", plugin.key);
            continue;
        };
        sources.push(SourceDetails {
            info: SourceInfo {
                id: plugin.id,
                key: plugin.key.clone(),
                name: plugin.name.clone(),
                path: plugin.path.clone(),
                version: UNKNOWN_PLUGIN_VERSION.to_owned(),
                enabled: plugin.enabled,
                status: ConnectorStatus::Error,
                last_error: Some(ConnectorError::new(&plugin.error)),
                plugin_config_format: plugin.config_format,
            },
            config: config.clone(),
            handler_tasks: vec![],
            container: None,
            restart_guard: Arc::new(Mutex::new(())),
        });
    }
    sources
}
