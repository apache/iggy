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

use crate::router::Router;
use iggy_connector_sdk::{Error, sink_connector};
use serde::{Deserialize, Serialize};
use strum::Display as StrumDisplay;

mod catalog;
mod props;
mod router;
mod sink;

#[derive(Debug, Serialize, Deserialize, StrumDisplay)]
#[serde(rename_all = "lowercase")]
pub enum IcebergSinkTypes {
    REST,
}

#[derive(Debug, Serialize, Deserialize, StrumDisplay)]
#[serde(rename_all = "lowercase")]
pub enum IcebergSinkStoreClass {
    S3,
    FS,
    GCS,
    AZDLS,
    OSS,
}

sink_connector!(IcebergSink);

#[derive(Debug)]
pub struct IcebergSink {
    id: u32,
    config: IcebergSinkConfig,
    router: Option<Box<dyn Router>>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IcebergSinkConfig {
    pub tables: Vec<String>,
    pub catalog_type: IcebergSinkTypes,
    pub warehouse: String,
    pub uri: String,
    pub dynamic_routing: bool,
    pub dynamic_route_field: String,
    pub store_url: String,
    pub store_access_key_id: String,
    pub store_secret_access_key: String,
    pub store_region: String,
    pub store_class: IcebergSinkStoreClass,
}

fn slice_user_table(table: &str) -> Vec<String> {
    table.split('.').map(|s| s.to_string()).collect()
}

impl IcebergSink {
    pub fn new(id: u32, config: IcebergSinkConfig) -> Self {
        let router = None;

        IcebergSink { id, config, router }
    }
}
