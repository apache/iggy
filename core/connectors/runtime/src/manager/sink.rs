/* Licensed to the Apache Software Foundation (ASF) under one
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

use crate::configs::{ConfigFormat, StreamConsumerConfig, TransformsConfig};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::Mutex;

#[derive(Debug)]
pub struct SinkManager {
    sinks: Mutex<HashMap<String, Arc<Mutex<SinkDetails>>>>,
}

impl SinkManager {
    pub fn new(sinks: Vec<SinkDetails>) -> Self {
        Self {
            sinks: Mutex::new(
                sinks
                    .into_iter()
                    .map(|sink| (sink.info.key.to_owned(), Arc::new(Mutex::new(sink))))
                    .collect(),
            ),
        }
    }

    pub async fn get(&self, key: &str) -> Option<Arc<Mutex<SinkDetails>>> {
        let sinks = self.sinks.lock().await;
        sinks.get(key).cloned()
    }

    pub async fn get_all(&self) -> Vec<SinkInfo> {
        let sinks = self.sinks.lock().await;
        let mut results = Vec::with_capacity(sinks.len());
        for sink in sinks.values() {
            let sink = sink.lock().await;
            results.push(sink.info.clone());
        }
        results
    }
}

#[derive(Debug, Clone)]
pub struct SinkInfo {
    pub id: u32,
    pub key: String,
    pub name: String,
    pub path: String,
    pub enabled: bool,
    pub running: bool,
    pub config_format: Option<ConfigFormat>,
}

#[derive(Debug)]
pub struct SinkDetails {
    pub info: SinkInfo,
    pub transforms: Option<TransformsConfig>,
    pub streams: Vec<StreamConsumerConfig>,
    pub config: Option<serde_json::Value>,
}
