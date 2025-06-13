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

use std::{collections::HashMap, sync::Arc};

use crate::configs::{ConfigFormat, StreamConsumerConfig, TransformsConfig};

#[derive(Debug)]
pub struct SinkManager {
    sinks: HashMap<String, SinkDetails>,
}

impl SinkManager {
    pub fn new(sinks: Vec<SinkDetails>) -> Self {
        Self {
            sinks: sinks
                .into_iter()
                .map(|sink| (sink.info.key.to_owned(), sink))
                .collect(),
        }
    }

    pub fn get(&self, key: &str) -> Option<&SinkDetails> {
        self.sinks.get(key)
    }

    pub fn get_all(&self) -> HashMap<&String, &SinkInfo> {
        self.sinks
            .iter()
            .map(|(key, sink)| (key, &sink.info))
            .collect()
    }
}

#[derive(Debug)]
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
    pub transforms: Option<Arc<TransformsConfig>>,
    pub streams: Arc<Vec<StreamConsumerConfig>>,
    pub config: Option<Arc<serde_json::Value>>,
}
