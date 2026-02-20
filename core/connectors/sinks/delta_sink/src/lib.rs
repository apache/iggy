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

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use deltalake::DeltaTable;
use deltalake::writer::JsonWriter;
use iggy_connector_sdk::sink_connector;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;

mod coercions;
mod sink;
mod storage;
mod utils;

use crate::coercions::CoercionTree;

sink_connector!(DeltaSink);

#[derive(Debug)]
pub struct DeltaSink {
    id: u32,
    config: DeltaSinkConfig,
    state: Mutex<Option<SinkState>>,
}

#[derive(Debug)]
struct SinkState {
    table: DeltaTable,
    writer: JsonWriter,
    coercion_tree: CoercionTree,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeltaSinkConfig {
    pub table_uri: String,
    #[serde(default)]
    pub schema: Vec<String>,
    #[serde(default)]
    pub storage_backend_type: Option<String>,

    // AWS S3
    #[serde(default)]
    pub aws_s3_access_key: Option<String>,
    #[serde(default)]
    pub aws_s3_secret_key: Option<String>,
    #[serde(default)]
    pub aws_s3_region: Option<String>,
    #[serde(default)]
    pub aws_s3_endpoint_url: Option<String>,
    #[serde(default)]
    pub aws_s3_allow_http: Option<bool>,

    // Azure Blob Storage
    #[serde(default)]
    pub azure_storage_account_name: Option<String>,
    #[serde(default)]
    pub azure_storage_account_key: Option<String>,
    #[serde(default)]
    pub azure_storage_sas_token: Option<String>,
    #[serde(default)]
    pub azure_container_name: Option<String>,

    // Google Cloud Storage
    #[serde(default)]
    pub gcs_service_account_key: Option<String>,
    #[serde(default)]
    pub gcs_bucket: Option<String>,
}

impl DeltaSink {
    pub fn new(id: u32, config: DeltaSinkConfig) -> Self {
        DeltaSink {
            id,
            config,
            state: Mutex::new(None),
        }
    }
}
