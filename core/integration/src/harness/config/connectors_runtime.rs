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

use bon::Builder;
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Debug, Clone, Builder)]
pub struct ConnectorsRuntimeConfig {
    #[builder(into)]
    pub config_path: Option<PathBuf>,
    #[builder(default = true)]
    pub cleanup: bool,
    #[builder(default)]
    pub extra_envs: HashMap<String, String>,
    #[builder(into)]
    pub executable_path: Option<String>,
}

impl Default for ConnectorsRuntimeConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}
