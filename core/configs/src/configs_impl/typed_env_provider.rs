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

use super::env_mapping::ConfigEnvMappings;
use super::error::ConfigurationError;
use super::parsing::parse_env_value;
use figment::{
    Profile, Provider,
    value::{Dict, Map as FigmentMap, Tag, Value as FigmentValue},
};
use std::{collections::HashSet, env, marker::PhantomData};
use tracing::{info, warn};

const SECRET_MASK: &str = "******";

/// Controls how env var names are resolved during deserialization.
enum EnvNameResolution<'a> {
    /// Use mapping.env_name directly (for configs with compile-time prefix)
    Direct,
    /// Prepend runtime prefix to mapping.env_name (for plugin configs)
    PrependPrefix(&'a str),
}

/// Controls filtering and messaging for unknown env var warnings.
enum WarningContext<'a> {
    /// Main config: filter IGNORED_ENV_VARS and DELEGATED prefixes
    MainConfig,
    /// Connector config: filter PLUGIN_CONFIG_ prefix
    ConnectorConfig(&'a str),
}

/// Environment variables starting with IGGY_ that are NOT config values.
/// These are used for test control, CI, CLI behavior, config file paths, etc.
const IGNORED_ENV_VARS: &[&str] = &[
    "IGGY_CI_BUILD",
    "IGGY_CONFIG_PATH",
    "IGGY_CONNECTORS_CONFIG_PATH",
    "IGGY_MCP_CONFIG_PATH",
    "IGGY_ROOT_PASSWORD",
    "IGGY_ROOT_USERNAME",
    "IGGY_TEST_VERBOSE",
];

/// Prefixes for env vars handled by separate providers with runtime prefixes.
/// The main config provider skips these; each sub-provider validates its own vars.
const DELEGATED_ENV_VAR_PREFIXES: &[&str] = &["IGGY_CONNECTORS_SINK_", "IGGY_CONNECTORS_SOURCE_"];

type ProfileMap = FigmentMap<Profile, Dict>;

/// Type-safe environment variable provider that uses compile-time generated mappings.
///
/// Unlike runtime schema inference providers, this provider uses the `ConfigEnvMappings`
/// trait to directly look up known env var names, eliminating ambiguity in path resolution.
///
/// # Example
/// ```ignore
/// let provider = TypedEnvProvider::<ServerConfig>::new("IGGY_", &["IGGY_SYSTEM_ENCRYPTION_KEY"]);
/// ```
#[derive(Debug, Clone)]
pub struct TypedEnvProvider<T: ConfigEnvMappings> {
    prefix: String,
    secret_keys: Vec<String>,
    _phantom: PhantomData<T>,
}

impl<T: ConfigEnvMappings> TypedEnvProvider<T> {
    /// Create a new typed environment provider.
    ///
    /// # Arguments
    /// * `prefix` - Environment variable prefix to validate against (e.g., "IGGY_")
    /// * `secret_keys` - Environment variable names that should be masked in logs
    pub fn new(prefix: &str, secret_keys: &[&str]) -> Self {
        Self {
            prefix: prefix.to_string(),
            secret_keys: secret_keys.iter().map(|s| s.to_string()).collect(),
            _phantom: PhantomData,
        }
    }

    /// Create a provider with a runtime-determined prefix.
    ///
    /// Use this when the config struct has no prefix attribute (generates relative mappings)
    /// but you need to apply a prefix at runtime.
    ///
    /// # Example
    /// ```ignore
    /// // SinkConfig has no prefix, generates: ENABLED, NAME, PATH
    /// // We prepend "IGGY_CONNECTORS_SINK_POSTGRES_" at runtime
    /// let provider = TypedEnvProvider::<SinkConfig>::with_runtime_prefix(
    ///     "IGGY_CONNECTORS_SINK_POSTGRES_",
    ///     &[]
    /// );
    /// ```
    pub fn with_runtime_prefix(prefix: &str, secret_keys: &[&str]) -> Self {
        Self::new(prefix, secret_keys)
    }

    /// Create a provider with default prefix "IGGY_" and no secret keys.
    pub fn with_default_prefix() -> Self {
        Self::new("IGGY_", &[])
    }

    /// Create a provider using auto-collected secrets from `#[config_env(secret)]` markers.
    pub fn from_config(prefix: &str) -> Self {
        let secret_keys: Vec<String> = T::secret_env_names()
            .into_iter()
            .map(String::from)
            .collect();
        Self {
            prefix: prefix.to_string(),
            secret_keys,
            _phantom: PhantomData,
        }
    }

    /// Deserialize with runtime prefix prepended to each mapping's env_name.
    ///
    /// Unlike `deserialize()`, this method prepends `self.prefix` to each mapping's
    /// env_name, allowing for dynamic prefix construction at runtime.
    pub fn deserialize_with_runtime_prefix(&self) -> Result<ProfileMap, ConfigurationError> {
        self.warn_unknown_env_vars_inner(WarningContext::ConnectorConfig(&self.prefix));
        self.deserialize_inner(EnvNameResolution::PrependPrefix(&self.prefix))
    }

    /// Deserialize environment variables into a configuration profile map.
    ///
    /// This method:
    /// 1. Validates that all env vars with the prefix are known (warns on unknown)
    /// 2. Iterates over compile-time generated mappings and applies set values
    pub fn deserialize(&self) -> Result<ProfileMap, ConfigurationError> {
        self.warn_unknown_env_vars_inner(WarningContext::MainConfig);
        self.deserialize_inner(EnvNameResolution::Direct)
    }

    fn deserialize_inner(
        &self,
        resolution: EnvNameResolution,
    ) -> Result<ProfileMap, ConfigurationError> {
        let mut root_dict = Dict::new();

        for mapping in T::env_mappings() {
            let env_name = match &resolution {
                EnvNameResolution::Direct => mapping.env_name.to_string(),
                EnvNameResolution::PrependPrefix(prefix) => {
                    format!("{}{}", prefix, mapping.env_name)
                }
            };

            let env_value = match env::var(&env_name) {
                Ok(val) if !val.is_empty() => val,
                _ => continue,
            };

            let is_secret = mapping.is_secret
                || match &resolution {
                    EnvNameResolution::Direct => self.secret_keys.contains(&env_name),
                    EnvNameResolution::PrependPrefix(_) => {
                        self.secret_keys.iter().any(|s| env_name.ends_with(s))
                    }
                };

            let display_value = if is_secret {
                SECRET_MASK.to_string()
            } else {
                env_value.clone()
            };

            info!(
                "{} value changed to: {} from environment variable",
                env_name, display_value
            );

            let parsed_value = parse_env_value(&env_value);
            Self::insert_at_path(&mut root_dict, mapping.config_path, parsed_value);
        }

        let root_dict =
            Self::convert_numeric_dicts_to_arrays(FigmentValue::Dict(Tag::Default, root_dict));
        let root_dict = match root_dict {
            FigmentValue::Dict(_, dict) => dict,
            _ => Dict::new(),
        };

        let mut data = ProfileMap::new();
        data.insert(Profile::default(), root_dict);
        Ok(data)
    }

    /// Recursively convert dicts with all numeric string keys to arrays.
    fn convert_numeric_dicts_to_arrays(value: FigmentValue) -> FigmentValue {
        match value {
            FigmentValue::Dict(tag, dict) => {
                let processed: Dict = dict
                    .into_iter()
                    .map(|(k, v)| (k, Self::convert_numeric_dicts_to_arrays(v)))
                    .collect();

                if Self::is_array_dict(&processed) {
                    Self::dict_to_array(processed)
                } else {
                    FigmentValue::Dict(tag, processed)
                }
            }
            FigmentValue::Array(tag, arr) => {
                let processed: Vec<FigmentValue> = arr
                    .into_iter()
                    .map(Self::convert_numeric_dicts_to_arrays)
                    .collect();
                FigmentValue::Array(tag, processed)
            }
            other => other,
        }
    }

    fn is_array_dict(dict: &Dict) -> bool {
        if dict.is_empty() {
            return false;
        }
        dict.keys().all(|k| k.parse::<usize>().is_ok())
    }

    fn dict_to_array(dict: Dict) -> FigmentValue {
        let mut indexed: Vec<(usize, FigmentValue)> = dict
            .into_iter()
            .filter_map(|(k, v)| k.parse::<usize>().ok().map(|i| (i, v)))
            .collect();

        indexed.sort_by_key(|(i, _)| *i);

        let max_index = indexed.last().map(|(i, _)| *i).unwrap_or(0);
        let mut array = vec![FigmentValue::Dict(Tag::Default, Dict::new()); max_index + 1];

        for (i, v) in indexed {
            array[i] = v;
        }

        FigmentValue::Array(Tag::Default, array)
    }

    fn warn_unknown_env_vars_inner(&self, context: WarningContext) {
        let known_vars: HashSet<String> = match &context {
            WarningContext::MainConfig => T::env_mappings()
                .iter()
                .map(|m| m.env_name.to_string())
                .collect(),
            WarningContext::ConnectorConfig(prefix) => T::env_mappings()
                .iter()
                .map(|m| format!("{}{}", prefix, m.env_name))
                .collect(),
        };
        let known_refs: HashSet<&str> = known_vars.iter().map(|s| s.as_str()).collect();

        for (key, _) in env::vars() {
            if !key.starts_with(&self.prefix) {
                continue;
            }

            if known_vars.contains(&key) {
                continue;
            }

            let should_skip = match &context {
                WarningContext::MainConfig => {
                    IGNORED_ENV_VARS.contains(&key.as_str())
                        || DELEGATED_ENV_VAR_PREFIXES
                            .iter()
                            .any(|p| key.starts_with(p))
                }
                WarningContext::ConnectorConfig(prefix) => {
                    let plugin_config_prefix = format!("{}PLUGIN_CONFIG_", prefix);
                    key.starts_with(&plugin_config_prefix)
                }
            };
            if should_skip {
                continue;
            }

            let suggestions = Self::find_similar_vars(&key, &known_refs);
            let suggestion_hint = if suggestions.is_empty() {
                String::new()
            } else {
                format!(" Similar: {}", suggestions.join(", "))
            };

            let debug_msg = match &context {
                WarningContext::MainConfig => format!(
                    "Unknown IGGY_ env var: '{}'.{}. Add to IGNORED_ENV_VARS if intentional, \
                     or add #[derive(ConfigEnv)] to the config struct.",
                    key, suggestion_hint
                ),
                WarningContext::ConnectorConfig(_) => format!(
                    "Unknown connector env var: '{}'.{} Check field name or add #[config_env(skip)].",
                    key, suggestion_hint
                ),
            };
            debug_assert!(false, "{}", debug_msg);

            Self::warn_unknown_var(&key, &suggestions);
        }
    }

    fn warn_unknown_var(unknown_var: &str, suggestions: &[String]) {
        if suggestions.is_empty() {
            warn!(
                "Unknown environment variable '{}' will be ignored. \
                 Use --list-env-vars to see all valid environment variables.",
                unknown_var
            );
        } else {
            warn!(
                "Unknown environment variable '{}' will be ignored. Similar variables: {}?",
                unknown_var,
                suggestions.join(", ")
            );
        }
    }

    /// Find similar variable names using Levenshtein edit distance.
    /// Returns up to 3 suggestions sorted by similarity.
    pub fn find_similar_vars(
        unknown: &str,
        known: &std::collections::HashSet<&str>,
    ) -> Vec<String> {
        let unknown_lower = unknown.to_lowercase();
        let mut suggestions: Vec<(String, usize)> = known
            .iter()
            .filter_map(|&known_var| {
                let known_lower = known_var.to_lowercase();
                let distance = Self::levenshtein_distance(&unknown_lower, &known_lower);
                let threshold = (unknown.len().max(known_var.len()) * 3) / 10;
                if distance <= threshold.max(3) {
                    Some((known_var.to_string(), distance))
                } else {
                    None
                }
            })
            .collect();

        suggestions.sort_by_key(|(_, dist)| *dist);
        suggestions.truncate(3);
        suggestions.into_iter().map(|(s, _)| s).collect()
    }

    fn levenshtein_distance(a: &str, b: &str) -> usize {
        let a_chars: Vec<char> = a.chars().collect();
        let b_chars: Vec<char> = b.chars().collect();
        let a_len = a_chars.len();
        let b_len = b_chars.len();

        if a_len == 0 {
            return b_len;
        }
        if b_len == 0 {
            return a_len;
        }

        let mut prev_row: Vec<usize> = (0..=b_len).collect();
        let mut curr_row = vec![0; b_len + 1];

        for (i, a_char) in a_chars.iter().enumerate() {
            curr_row[0] = i + 1;

            for (j, b_char) in b_chars.iter().enumerate() {
                let cost = if a_char == b_char { 0 } else { 1 };
                curr_row[j + 1] = (prev_row[j + 1] + 1)
                    .min(curr_row[j] + 1)
                    .min(prev_row[j] + cost);
            }

            std::mem::swap(&mut prev_row, &mut curr_row);
        }

        prev_row[b_len]
    }

    fn insert_at_path(dict: &mut Dict, path: &str, value: FigmentValue) {
        let segments: Vec<&str> = path.split('.').collect();
        Self::insert_at_path_segments(dict, &segments, value);
    }

    fn insert_at_path_segments(dict: &mut Dict, segments: &[&str], value: FigmentValue) {
        if segments.is_empty() {
            return;
        }

        if segments.len() == 1 {
            dict.insert(segments[0].to_string(), value);
            return;
        }

        let key = segments[0].to_string();
        dict.entry(key.clone())
            .or_insert_with(|| FigmentValue::Dict(Tag::Default, Dict::new()));

        if let Some(FigmentValue::Dict(_, inner_dict)) = dict.get_mut(&key) {
            Self::insert_at_path_segments(inner_dict, &segments[1..], value);
        }
    }
}

impl<T: ConfigEnvMappings + Send + Sync> Provider for TypedEnvProvider<T> {
    fn metadata(&self) -> figment::Metadata {
        figment::Metadata::named("typed environment variables")
    }

    fn data(&self) -> Result<ProfileMap, figment::Error> {
        self.deserialize()
            .map_err(|e| figment::Error::from(format!("Failed to deserialize env vars: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ConfigEnv;
    use serde::{Deserialize, Serialize};
    use std::collections::HashSet;

    #[derive(Debug, Default, Clone, Serialize, Deserialize, ConfigEnv)]
    #[config_env(prefix = "TEST_", name = "test-config")]
    struct TestConfig {
        enabled: bool,
        name: String,
        count: u32,
        nested: NestedConfig,
    }

    #[derive(Debug, Default, Clone, Serialize, Deserialize, ConfigEnv)]
    struct NestedConfig {
        value: String,
        flag: bool,
    }

    #[test]
    fn levenshtein_distance_identical_strings() {
        assert_eq!(
            TypedEnvProvider::<TestConfig>::levenshtein_distance("hello", "hello"),
            0
        );
    }

    #[test]
    fn levenshtein_distance_single_char_difference() {
        assert_eq!(
            TypedEnvProvider::<TestConfig>::levenshtein_distance("hello", "hallo"),
            1
        );
        assert_eq!(
            TypedEnvProvider::<TestConfig>::levenshtein_distance("cat", "car"),
            1
        );
    }

    #[test]
    fn levenshtein_distance_empty_strings() {
        assert_eq!(
            TypedEnvProvider::<TestConfig>::levenshtein_distance("", ""),
            0
        );
        assert_eq!(
            TypedEnvProvider::<TestConfig>::levenshtein_distance("abc", ""),
            3
        );
        assert_eq!(
            TypedEnvProvider::<TestConfig>::levenshtein_distance("", "abc"),
            3
        );
    }

    #[test]
    fn levenshtein_distance_insertions_deletions() {
        assert_eq!(
            TypedEnvProvider::<TestConfig>::levenshtein_distance("abc", "ab"),
            1
        );
        assert_eq!(
            TypedEnvProvider::<TestConfig>::levenshtein_distance("ab", "abc"),
            1
        );
    }

    #[test]
    fn find_similar_vars_suggests_typos() {
        let known: HashSet<&str> = ["TEST_ENABLED", "TEST_NAME", "TEST_COUNT"]
            .into_iter()
            .collect();

        let suggestions = TypedEnvProvider::<TestConfig>::find_similar_vars("TEST_ENABELD", &known);
        assert!(suggestions.contains(&"TEST_ENABLED".to_string()));

        let suggestions = TypedEnvProvider::<TestConfig>::find_similar_vars("TEST_NANE", &known);
        assert!(suggestions.contains(&"TEST_NAME".to_string()));
    }

    #[test]
    fn find_similar_vars_returns_empty_for_unrelated() {
        let known: HashSet<&str> = ["TEST_ENABLED", "TEST_NAME"].into_iter().collect();

        let suggestions =
            TypedEnvProvider::<TestConfig>::find_similar_vars("COMPLETELY_DIFFERENT_VAR", &known);
        assert!(suggestions.is_empty());
    }

    #[test]
    fn find_similar_vars_limits_to_three_suggestions() {
        let known: HashSet<&str> = ["VAR_A", "VAR_B", "VAR_C", "VAR_D", "VAR_E"]
            .into_iter()
            .collect();

        let suggestions = TypedEnvProvider::<TestConfig>::find_similar_vars("VAR_X", &known);
        assert!(suggestions.len() <= 3);
    }

    #[test]
    fn test_config_generates_env_mappings() {
        let mappings = TestConfig::env_mappings();
        assert!(!mappings.is_empty());

        assert!(mappings.iter().any(|m| m.env_name == "TEST_ENABLED"));
        assert!(mappings.iter().any(|m| m.env_name == "TEST_NAME"));
        assert!(mappings.iter().any(|m| m.env_name == "TEST_COUNT"));
        assert!(mappings.iter().any(|m| m.env_name == "TEST_NESTED_VALUE"));
        assert!(mappings.iter().any(|m| m.env_name == "TEST_NESTED_FLAG"));
    }

    #[test]
    fn test_config_mappings_have_correct_paths() {
        let mappings = TestConfig::env_mappings();

        let enabled = mappings
            .iter()
            .find(|m| m.env_name == "TEST_ENABLED")
            .unwrap();
        assert_eq!(enabled.config_path, "enabled");

        let nested_value = mappings
            .iter()
            .find(|m| m.env_name == "TEST_NESTED_VALUE")
            .unwrap();
        assert_eq!(nested_value.config_path, "nested.value");
    }

    #[test]
    fn typed_provider_deserializes_env_vars() {
        unsafe {
            env::set_var("TEST_ENABLED", "true");
            env::set_var("TEST_NAME", "test-name");
            env::set_var("TEST_COUNT", "42");
        }

        let provider = TypedEnvProvider::<TestConfig>::new("TEST_", &[]);
        let data = provider.deserialize().expect("deserialize failed");

        unsafe {
            env::remove_var("TEST_ENABLED");
            env::remove_var("TEST_NAME");
            env::remove_var("TEST_COUNT");
        }

        let profile_data = data
            .get(&figment::Profile::default())
            .expect("no default profile");

        let enabled = profile_data.get("enabled").expect("no enabled");
        assert!(matches!(enabled, FigmentValue::Bool(_, true)));

        let name = profile_data.get("name").expect("no name");
        if let FigmentValue::String(_, s) = name {
            assert_eq!(s, "test-name");
        } else {
            panic!("name should be string");
        }

        let count = profile_data.get("count").expect("no count");
        if let FigmentValue::Num(_, figment::value::Num::U64(n)) = count {
            assert_eq!(*n, 42);
        } else {
            panic!("count should be u64");
        }
    }
}
