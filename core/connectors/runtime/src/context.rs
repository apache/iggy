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
use crate::{
    SinkConnectorWrapper, SourceConnectorWrapper,
    manager::{
        sink::{SinkDetails, SinkInfo, SinkManager},
        source::{SourceDetails, SourceInfo, SourceManager},
    },
};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::error;

pub struct RuntimeContext {
    pub sinks: SinkManager,
    pub sources: SourceManager,
    pub api_key: String,
    pub config_provider: Arc<dyn ConnectorsConfigProvider>,
}

pub fn init(
    config: &ConnectorsRuntimeConfig,
    sinks_config: &HashMap<String, SinkConfig>,
    sources_config: &HashMap<String, SourceConfig>,
    sink_wrappers: &[SinkConnectorWrapper],
    source_wrappers: &[SourceConnectorWrapper],
    config_provider: Box<dyn ConnectorsConfigProvider>,
) -> RuntimeContext {
    RuntimeContext {
        sinks: SinkManager::new(map_sinks(sinks_config, sink_wrappers)),
        sources: SourceManager::new(map_sources(sources_config, source_wrappers)),
        api_key: config.http.api_key.to_owned(),
        config_provider: Arc::from(config_provider),
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

            sinks.push(SinkDetails {
                info: SinkInfo {
                    id: sink_plugin.id,
                    key: sink_plugin.key.to_owned(),
                    name: sink_plugin.name.to_owned(),
                    path: sink_plugin.path.to_owned(),
                    enabled: sink_config.enabled,
                    running: sink_config.enabled,
                    plugin_config_format: sink_plugin.config_format,
                },
                config: sink_config.clone(),
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

            sources.push(SourceDetails {
                info: SourceInfo {
                    id: source_plugin.id,
                    key: source_plugin.key.to_owned(),
                    name: source_plugin.name.to_owned(),
                    path: source_plugin.path.to_owned(),
                    enabled: source_config.enabled,
                    running: source_config.enabled,
                    plugin_config_format: source_plugin.config_format,
                },
                config: source_config.clone(),
            });
        }
    }
    sources
}
