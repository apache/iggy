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

use std::collections::HashMap;

/// Default RESTful URL templates for connector operations
pub struct DefaultUrls;

impl DefaultUrls {
    pub const CREATE_SINK: &'static str = "/sinks/{key}/configs";
    pub const CREATE_SOURCE: &'static str = "/sources/{key}/configs";
    pub const GET_ACTIVE_CONFIGS: &'static str = "/configs/active";
    pub const GET_ACTIVE_VERSIONS: &'static str = "/configs/active/versions";
    pub const SET_ACTIVE_SINK: &'static str = "/sinks/{key}/configs/active";
    pub const SET_ACTIVE_SOURCE: &'static str = "/sources/{key}/configs/active";
    pub const GET_SINK_CONFIGS: &'static str = "/sinks/{key}/configs";
    pub const GET_SINK_CONFIG_BY_VERSION: &'static str = "/sinks/{key}/configs/{version}";
    pub const GET_ACTIVE_SINK_CONFIG: &'static str = "/sinks/{key}/configs/active";
    pub const GET_SOURCE_CONFIGS: &'static str = "/sources/{key}/configs";
    pub const GET_SOURCE_CONFIG_BY_VERSION: &'static str = "/sources/{key}/configs/{version}";
    pub const GET_ACTIVE_SOURCE_CONFIG: &'static str = "/sources/{key}/configs/active";
    pub const DELETE_SINK_CONFIG: &'static str = "/sinks/{key}/configs";
    pub const DELETE_SOURCE_CONFIG: &'static str = "/sources/{key}/configs";
}

/// Template operation names used as keys in the url_templates configuration
pub struct TemplateKeys;

impl TemplateKeys {
    pub const CREATE_SINK: &'static str = "create_sink";
    pub const CREATE_SOURCE: &'static str = "create_source";
    pub const GET_ACTIVE_CONFIGS: &'static str = "get_active_configs";
    pub const GET_ACTIVE_VERSIONS: &'static str = "get_active_versions";
    pub const SET_ACTIVE_SINK: &'static str = "set_active_sink";
    pub const SET_ACTIVE_SOURCE: &'static str = "set_active_source";
    pub const GET_SINK_CONFIGS: &'static str = "get_sink_configs";
    pub const GET_SINK_CONFIG: &'static str = "get_sink_config";
    pub const GET_ACTIVE_SINK_CONFIG: &'static str = "get_active_sink_config";
    pub const GET_SOURCE_CONFIGS: &'static str = "get_source_configs";
    pub const GET_SOURCE_CONFIG: &'static str = "get_source_config";
    pub const GET_ACTIVE_SOURCE_CONFIG: &'static str = "get_active_source_config";
    pub const DELETE_SINK_CONFIG: &'static str = "delete_sink_config";
    pub const DELETE_SOURCE_CONFIG: &'static str = "delete_source_config";
}

/// URL builder for constructing HTTP endpoints from templates
pub struct UrlBuilder {
    base_url: String,
    templates: HashMap<String, String>,
}

impl UrlBuilder {
    /// Creates a new UrlBuilder with optional custom templates
    ///
    /// If templates are not provided, uses RESTful defaults
    pub fn new(base_url: &str, custom_templates: &HashMap<String, String>) -> Self {
        let mut templates = HashMap::new();

        // Initialize with defaults
        templates.insert(
            TemplateKeys::CREATE_SINK.to_string(),
            DefaultUrls::CREATE_SINK.to_string(),
        );
        templates.insert(
            TemplateKeys::CREATE_SOURCE.to_string(),
            DefaultUrls::CREATE_SOURCE.to_string(),
        );
        templates.insert(
            TemplateKeys::GET_ACTIVE_CONFIGS.to_string(),
            DefaultUrls::GET_ACTIVE_CONFIGS.to_string(),
        );
        templates.insert(
            TemplateKeys::GET_ACTIVE_VERSIONS.to_string(),
            DefaultUrls::GET_ACTIVE_VERSIONS.to_string(),
        );
        templates.insert(
            TemplateKeys::SET_ACTIVE_SINK.to_string(),
            DefaultUrls::SET_ACTIVE_SINK.to_string(),
        );
        templates.insert(
            TemplateKeys::SET_ACTIVE_SOURCE.to_string(),
            DefaultUrls::SET_ACTIVE_SOURCE.to_string(),
        );
        templates.insert(
            TemplateKeys::GET_SINK_CONFIGS.to_string(),
            DefaultUrls::GET_SINK_CONFIGS.to_string(),
        );
        templates.insert(
            TemplateKeys::GET_SINK_CONFIG.to_string(),
            DefaultUrls::GET_SINK_CONFIG_BY_VERSION.to_string(),
        );
        templates.insert(
            TemplateKeys::GET_ACTIVE_SINK_CONFIG.to_string(),
            DefaultUrls::GET_ACTIVE_SINK_CONFIG.to_string(),
        );
        templates.insert(
            TemplateKeys::GET_SOURCE_CONFIGS.to_string(),
            DefaultUrls::GET_SOURCE_CONFIGS.to_string(),
        );
        templates.insert(
            TemplateKeys::GET_SOURCE_CONFIG.to_string(),
            DefaultUrls::GET_SOURCE_CONFIG_BY_VERSION.to_string(),
        );
        templates.insert(
            TemplateKeys::GET_ACTIVE_SOURCE_CONFIG.to_string(),
            DefaultUrls::GET_ACTIVE_SOURCE_CONFIG.to_string(),
        );
        templates.insert(
            TemplateKeys::DELETE_SINK_CONFIG.to_string(),
            DefaultUrls::DELETE_SINK_CONFIG.to_string(),
        );
        templates.insert(
            TemplateKeys::DELETE_SOURCE_CONFIG.to_string(),
            DefaultUrls::DELETE_SOURCE_CONFIG.to_string(),
        );

        // Override with custom templates if provided
        for (key, value) in custom_templates.clone().into_iter() {
            templates.insert(key, value);
        }

        Self {
            base_url: base_url.to_owned(),
            templates,
        }
    }

    /// Builds a URL for the specified operation with variable substitution
    pub fn build(&self, operation: &str, vars: &HashMap<&str, &str>) -> String {
        let template = self
            .templates
            .get(operation)
            .map(|s| s.as_str())
            .unwrap_or("");

        let mut url = template.to_string();

        // Replace template variables: {key}, {version}, {type}
        for (var_name, var_value) in vars {
            let placeholder = format!("{{{}}}", var_name);
            url = url.replace(&placeholder, var_value);
        }

        format!("{}{}", self.base_url, url)
    }

    /// Builds a URL with query parameters appended
    pub fn build_with_query(
        &self,
        operation: &str,
        vars: &HashMap<&str, &str>,
        query_params: &HashMap<&str, &str>,
    ) -> String {
        let base = self.build(operation, vars);

        if query_params.is_empty() {
            return base;
        }

        let query_string: Vec<String> = query_params
            .iter()
            .map(|(k, v)| format!("{}={}", k, v))
            .collect();

        format!("{}?{}", base, query_string.join("&"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_templates() {
        let builder = UrlBuilder::new("http://localhost:8080", &HashMap::new());

        let mut vars = HashMap::new();
        vars.insert("key", "my-sink");

        let url = builder.build(TemplateKeys::CREATE_SINK, &vars);
        assert_eq!(url, "http://localhost:8080/sinks/my-sink/configs");
    }

    #[test]
    fn test_custom_template() {
        let mut custom = HashMap::new();
        custom.insert(
            TemplateKeys::CREATE_SINK.to_string(),
            "/api/v2/sinks/{key}/config".to_string(),
        );

        let builder = UrlBuilder::new("http://localhost:8080", &custom);

        let mut vars = HashMap::new();
        vars.insert("key", "my-sink");

        let url = builder.build(TemplateKeys::CREATE_SINK, &vars);
        assert_eq!(url, "http://localhost:8080/api/v2/sinks/my-sink/config");
    }

    #[test]
    fn test_version_substitution() {
        let builder = UrlBuilder::new("http://localhost:8080", &HashMap::new());

        let mut vars = HashMap::new();
        vars.insert("key", "my-sink");
        vars.insert("version", "42");

        let url = builder.build(TemplateKeys::GET_SINK_CONFIG, &vars);
        assert_eq!(url, "http://localhost:8080/sinks/my-sink/configs/42");
    }

    #[test]
    fn test_query_parameters() {
        let builder = UrlBuilder::new("http://localhost:8080", &HashMap::new());

        let mut vars = HashMap::new();
        vars.insert("key", "my-sink");

        let mut query_params = HashMap::new();
        query_params.insert("version", "5");

        let url = builder.build_with_query(TemplateKeys::DELETE_SINK_CONFIG, &vars, &query_params);
        assert_eq!(url, "http://localhost:8080/sinks/my-sink/configs?version=5");
    }

    #[test]
    fn test_query_based_custom_template() {
        let mut custom = HashMap::new();
        custom.insert(
            TemplateKeys::GET_SINK_CONFIG.to_string(),
            "/api?action=get&key={key}&version={version}".to_string(),
        );

        let builder = UrlBuilder::new("http://localhost:8080", &custom);

        let mut vars = HashMap::new();
        vars.insert("key", "my-sink");
        vars.insert("version", "10");

        let url = builder.build(TemplateKeys::GET_SINK_CONFIG, &vars);
        assert_eq!(
            url,
            "http://localhost:8080/api?action=get&key=my-sink&version=10"
        );
    }
}
