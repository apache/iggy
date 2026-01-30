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
use super::status::{ConnectorError, ConnectorStatus};
use crate::configs::connectors::{ConfigFormat, SinkConfig};
use crate::metrics::Metrics;
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

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
}

#[derive(Debug, Clone)]
pub struct SinkInfo {
    pub id: u32,
    pub key: String,
    pub name: String,
    pub path: String,
    pub enabled: bool,
    pub status: ConnectorStatus,
    pub last_error: Option<ConnectorError>,
    pub plugin_config_format: Option<ConfigFormat>,
}

#[derive(Debug)]
pub struct SinkDetails {
    pub info: SinkInfo,
    pub config: SinkConfig,
}
