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

use crate::configs::{ConfigFormat, StreamProducerConfig, TransformsConfig};

#[derive(Debug)]
pub struct SourceManager {
    sources: HashMap<String, SourceDetails>,
}

impl SourceManager {
    pub fn new(sources: Vec<SourceDetails>) -> Self {
        Self {
            sources: sources
                .into_iter()
                .map(|source| (source.info.key.to_owned(), source))
                .collect(),
        }
    }

    pub fn get(&self, key: &str) -> Option<&SourceDetails> {
        self.sources.get(key)
    }

    pub fn get_all(&self) -> HashMap<&String, &SourceInfo> {
        self.sources
            .iter()
            .map(|(key, source)| (key, &source.info))
            .collect()
    }
}

#[derive(Debug)]
pub struct SourceInfo {
    pub id: u32,
    pub key: String,
    pub name: String,
    pub path: String,
    pub enabled: bool,
    pub running: bool,
    pub config_format: Option<ConfigFormat>,
}

#[derive(Debug)]
pub struct SourceDetails {
    pub info: SourceInfo,
    pub transforms: Option<Arc<TransformsConfig>>,
    pub streams: Arc<Vec<StreamProducerConfig>>,
    pub config: Option<Arc<serde_json::Value>>,
}
