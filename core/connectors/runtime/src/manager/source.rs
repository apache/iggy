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
use crate::configs::connectors::{ConfigFormat, SourceConfig};
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

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
}

#[derive(Debug, Clone)]
pub struct SourceInfo {
    pub id: u32,
    pub key: String,
    pub name: String,
    pub path: String,
    pub enabled: bool,
    pub running: bool,
    pub plugin_config_format: Option<ConfigFormat>,
}

#[derive(Debug)]
pub struct SourceDetails {
    pub info: SourceInfo,
    pub config: SourceConfig,
}
