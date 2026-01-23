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

#[derive(Debug, Clone, Builder)]
pub struct McpConfig {
    #[builder(into, default = "mcp".to_string())]
    pub consumer_name: String,
    #[builder(into, default = "/mcp".to_string())]
    pub http_path: String,
    #[builder(default)]
    pub extra_envs: HashMap<String, String>,
    #[builder(into)]
    pub executable_path: Option<String>,
}

impl Default for McpConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mcp_config_builder() {
        let config = McpConfig::builder()
            .consumer_name("custom")
            .http_path("/custom-mcp")
            .build();

        assert_eq!(config.consumer_name, "custom");
        assert_eq!(config.http_path, "/custom-mcp");
    }

    #[test]
    fn test_mcp_config_defaults() {
        let config = McpConfig::default();
        assert_eq!(config.consumer_name, "mcp");
        assert_eq!(config.http_path, "/mcp");
    }
}
