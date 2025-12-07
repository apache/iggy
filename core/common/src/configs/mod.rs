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

//! Configuration module providing flexible configuration loading with file and environment support.
//!
//! This module provides a trait-based configuration system that supports:
//! - Loading configuration from TOML files
//! - Environment variable overrides with complex path resolution
//! - Array and HashMap field overrides
//! - JSON value field handling
//! - Automatic type conversion and validation

use figment::{
    Figment, Profile, Provider,
    providers::{Data, Format, Toml},
    value::{Dict, Map as FigmentMap, Tag, Value as FigmentValue},
};
use serde::{Serialize, de::DeserializeOwned};
use std::{env, fmt::Display, future::Future, marker::PhantomData, path::Path};
use toml::{Value as TomlValue, map::Map as TomlMap};
use tracing::{error, info, warn};

const SECRET_MASK: &str = "******";
const ARRAY_SEPARATOR: char = '_';
const PATH_SEPARATOR: &str = ".";
const DISPLAY_CONFIG_ENV: &str = "IGGY_DISPLAY_CONFIG";

type ProfileMap = FigmentMap<Profile, Dict>;

/// Type alias for configuration that can be serialized, deserialized, has defaults, and can be displayed
pub trait ConfigurationType: Serialize + DeserializeOwned + Default + Display {}
impl<T: Serialize + DeserializeOwned + Default + Display> ConfigurationType for T {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigurationError {
    CannotLoadConfiguration,
    DefaultSerializationFailed,
    DefaultParsingFailed,
    EnvironmentVariableParsingFailed,
    InvalidConfigurationValue,
}

impl Display for ConfigurationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CannotLoadConfiguration => write!(f, "Cannot load configuration"),
            Self::DefaultSerializationFailed => {
                write!(f, "Failed to serialize default configuration")
            }
            Self::DefaultParsingFailed => write!(f, "Failed to parse default configuration"),
            Self::EnvironmentVariableParsingFailed => {
                write!(f, "Failed to parse environment variables")
            }
            Self::InvalidConfigurationValue => {
                write!(f, "Provided configuration value is invalid")
            }
        }
    }
}

impl std::error::Error for ConfigurationError {}

pub trait ConfigProvider<T: ConfigurationType> {
    fn load_config(&self) -> impl Future<Output = Result<T, ConfigurationError>>;
}

/// File-based configuration provider that combines file, default, and environment configurations
pub struct FileConfigProvider<P> {
    file_path: String,
    default_config: Option<Data<Toml>>,
    env_provider: P,
    display_config: bool,
}

impl<P: Provider> FileConfigProvider<P> {
    /// Create a new file configuration provider
    ///
    /// # Arguments
    /// * `file_path` - Path to the configuration file
    /// * `env_provider` - Environment variable provider
    /// * `display_config` - Whether to display the loaded configuration
    /// * `default_config` - Optional default configuration data
    pub fn new(
        file_path: String,
        env_provider: P,
        display_config: bool,
        default_config: Option<Data<Toml>>,
    ) -> Self {
        Self {
            file_path,
            env_provider,
            default_config,
            display_config,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CustomEnvProvider<T: ConfigurationType> {
    prefix: String,
    secret_keys: Vec<String>,
    _phantom: PhantomData<T>,
}

impl<T: ConfigurationType> CustomEnvProvider<T> {
    /// Create a new custom environment provider
    ///
    /// # Arguments
    /// * `prefix` - Environment variable prefix to filter by
    /// * `secret_keys` - Keys that should be masked in logs
    pub fn new(prefix: &str, secret_keys: &[&str]) -> Self {
        Self {
            prefix: prefix.to_string(),
            secret_keys: secret_keys.iter().map(|s| s.to_string()).collect(),
            _phantom: PhantomData,
        }
    }

    /// Deserialize environment variables into a configuration profile map
    pub fn deserialize(&self) -> Result<ProfileMap, ConfigurationError> {
        let default_config = toml::to_string(&T::default())
            .map_err(|_| ConfigurationError::DefaultSerializationFailed)?;

        let toml_value: TomlValue = toml::from_str(&default_config)
            .map_err(|_| ConfigurationError::DefaultParsingFailed)?;

        let mut source_dict = Dict::new();
        if let TomlValue::Table(table) = toml_value {
            Self::walk_toml_table_to_dict("", table, &mut source_dict);
        }

        let mut target_dict = Dict::new();
        for (key, mut value) in env::vars() {
            let env_key = key.to_uppercase();
            if !env_key.starts_with(&self.prefix) {
                continue;
            }

            let keys: Vec<String> = env_key[self.prefix.len()..]
                .split(ARRAY_SEPARATOR)
                .map(|k| k.to_lowercase())
                .collect();

            let env_var_value = Self::parse_environment_value(&value);

            if self.secret_keys.contains(&env_key) {
                value = SECRET_MASK.to_string();
            }

            info!("{env_key} value changed to: {value} from environment variable");
            Self::insert_environment_override(&source_dict, &mut target_dict, keys, env_var_value);
        }

        let mut data = ProfileMap::new();
        data.insert(Profile::default(), target_dict);
        Ok(data)
    }

    /// Walk through TOML table and convert to dictionary structure
    fn walk_toml_table_to_dict(prefix: &str, table: TomlMap<String, TomlValue>, dict: &mut Dict) {
        for (key, value) in table {
            let new_prefix = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{prefix}{PATH_SEPARATOR}{key}")
            };

            match value {
                TomlValue::Table(inner_table) => {
                    let mut nested_dict = Dict::new();
                    Self::walk_toml_table_to_dict(&new_prefix, inner_table, &mut nested_dict);
                    dict.insert(key, FigmentValue::from(nested_dict));
                }
                _ => {
                    dict.insert(key, Self::toml_to_figment_value(&value));
                }
            }
        }
    }

    /// Insert environment variable override into target dictionary
    fn insert_environment_override(
        source: &Dict,
        target: &mut Dict,
        keys: Vec<String>,
        value: FigmentValue,
    ) {
        if keys.is_empty() {
            return;
        }

        // Detect array patterns (e.g., cluster.nodes.0.id)
        if let Some((idx_pos, array_index)) = Self::find_array_index(&keys) {
            Self::handle_array_override(source, target, &keys, idx_pos, array_index, value);
        } else {
            Self::handle_dictionary_override(source, target, &keys, value);
        }
    }

    /// Find array index in key path
    fn find_array_index(keys: &[String]) -> Option<(usize, usize)> {
        keys.iter()
            .enumerate()
            .find_map(|(i, key)| key.parse::<usize>().ok().map(|idx| (i, idx)))
    }

    /// Handle array field override with complex path resolution
    fn handle_array_override(
        source: &Dict,
        target: &mut Dict,
        keys: &[String],
        idx_pos: usize,
        array_index: usize,
        value: FigmentValue,
    ) {
        let path_to_array = &keys[..idx_pos];
        let remaining_path = &keys[idx_pos + 1..];

        // Navigate to array container
        let current_target = match Self::navigate_to_dictionary(
            target,
            &path_to_array[..path_to_array.len().saturating_sub(1)],
        ) {
            Some(dict) => dict,
            None => return,
        };

        let array_key = match path_to_array.last() {
            Some(key) => key.clone(),
            None => return,
        };

        // Copy existing array from source if needed
        if !current_target.contains_key(&array_key) {
            if let Some(existing_array) = Self::find_source_array(source, path_to_array, &array_key)
            {
                current_target.insert(array_key.clone(), existing_array);
            } else {
                current_target.insert(
                    array_key.clone(),
                    FigmentValue::Array(Tag::Default, Vec::new()),
                );
            }
        }

        // Update array element
        if let Some(FigmentValue::Array(_, arr)) = current_target.get_mut(&array_key) {
            // Ensure array is large enough
            while arr.len() <= array_index {
                arr.push(FigmentValue::Dict(Tag::Default, Dict::new()));
            }

            if remaining_path.is_empty() {
                arr[array_index] = value;
            } else if let FigmentValue::Dict(_, elem_dict) = &mut arr[array_index] {
                // TODO(hubcio): this is workaround done by Claude because this code is overly
                // complicated and I don't want to spend time on it.
                // For nested structures in arrays, check if we need to create intermediate dicts
                // Handle the "ports" case where it should be a nested structure
                if remaining_path.len() >= 2 && remaining_path[0] == "ports" {
                    // Create the ports dict if it doesn't exist
                    elem_dict
                        .entry("ports".to_string())
                        .or_insert_with(|| FigmentValue::Dict(Tag::Default, Dict::new()));

                    if let Some(FigmentValue::Dict(_, ports_dict)) = elem_dict.get_mut("ports") {
                        // Insert the specific port value (tcp, quic, http, websocket)
                        ports_dict.insert(remaining_path[1].clone(), value);
                    }
                } else {
                    // Default behavior for other fields
                    Self::insert_environment_override(
                        &Dict::new(),
                        elem_dict,
                        remaining_path.to_vec(),
                        value,
                    );
                }
            }
        }
    }

    /// Handle dictionary field override with HashMap detection
    fn handle_dictionary_override(
        source: &Dict,
        target: &mut Dict,
        keys: &[String],
        value: FigmentValue,
    ) {
        if keys.is_empty() {
            return;
        }

        // Try to detect HashMap patterns
        if let Some(hashmap_result) =
            Self::try_handle_hashmap_override(source, target, keys, value.clone())
            && hashmap_result
        {
            return;
        }

        // Fallback to original logic for non-HashMap patterns
        let mut current_source = source;
        let mut current_target = target;
        let mut combined_keys = Vec::new();

        for (i, key) in keys.iter().enumerate() {
            combined_keys.push(key.clone());
            let key_to_check = combined_keys.join("_");

            match current_source.get(&key_to_check) {
                Some(FigmentValue::Dict(_, inner_source_dict)) => {
                    current_target
                        .entry(key_to_check.clone())
                        .or_insert_with(|| FigmentValue::Dict(Tag::Default, Dict::new()));

                    if let Some(FigmentValue::Dict(_, inner_target)) =
                        current_target.get_mut(&key_to_check)
                    {
                        current_source = inner_source_dict;
                        current_target = inner_target;
                        combined_keys.clear();
                    } else {
                        return;
                    }
                }
                Some(_) => {
                    current_target.insert(key_to_check, value);
                    return;
                }
                None if i == keys.len() - 1 => {
                    current_target.insert(key_to_check, value);
                    return;
                }
                _ => continue,
            }
        }
    }

    /// Try to handle HashMap override patterns
    fn try_handle_hashmap_override(
        source: &Dict,
        target: &mut Dict,
        keys: &[String],
        value: FigmentValue,
    ) -> Option<bool> {
        if keys.len() < 2 {
            return Some(false);
        }

        let potential_hashmap_key = &keys[0];

        // Check if this is actually a HashMap field in the source
        let source_hashmap = match source.get(potential_hashmap_key) {
            Some(FigmentValue::Dict(_, hashmap_dict)) => {
                // Determine if this is a HashMap or regular struct
                let has_dict_values = hashmap_dict
                    .values()
                    .any(|v| matches!(v, FigmentValue::Dict(_, _)));
                let has_non_dict_values = hashmap_dict
                    .values()
                    .any(|v| !matches!(v, FigmentValue::Dict(_, _)));

                // If it has non-Dict values mixed with Dict values, it's probably a regular struct
                if has_non_dict_values && has_dict_values {
                    return Some(false);
                }

                // If it only has non-Dict values, it's definitely a regular struct
                if has_non_dict_values && !has_dict_values {
                    return Some(false);
                }

                hashmap_dict
            }
            _ => return Some(false),
        };

        // Try to find the best HashMap entry key
        if let Some((entry_key, remaining_keys)) =
            Self::find_optimal_hashmap_split(source_hashmap, &keys[1..])
        {
            return Some(Self::apply_hashmap_override(
                target,
                potential_hashmap_key,
                &entry_key,
                &remaining_keys,
                source_hashmap,
                value,
            ));
        }

        Some(false)
    }

    /// Find optimal HashMap key split for environment variable path
    fn find_optimal_hashmap_split(
        source_hashmap: &Dict,
        keys: &[String],
    ) -> Option<(String, Vec<String>)> {
        if keys.is_empty() {
            return None;
        }

        // First, try existing HashMap entry keys if any exist
        if !source_hashmap.is_empty() {
            for (existing_key, existing_value) in source_hashmap {
                if let FigmentValue::Dict(_, entry_dict) = existing_value {
                    // Try different ways to match this existing key
                    for split_point in 1..=keys.len() {
                        let candidate_key = keys[0..split_point].join("_");

                        if candidate_key == *existing_key {
                            let remaining_keys = keys[split_point..].to_vec();

                            // Validate that the remaining keys form a valid path in the entry
                            if remaining_keys.is_empty()
                                || Self::is_valid_field_path(entry_dict, &remaining_keys)
                            {
                                return Some((existing_key.clone(), remaining_keys));
                            }
                        }
                    }
                }
            }
        }

        // For empty HashMaps or when no existing keys match, use simple split
        let simple_entry_key = keys[0].clone();
        let simple_remaining = keys[1..].to_vec();
        Some((simple_entry_key, simple_remaining))
    }

    /// Check if a field path is valid within a dictionary structure
    fn is_valid_field_path(dict: &Dict, keys: &[String]) -> bool {
        if keys.is_empty() {
            return true;
        }

        // Try different combinations to account for fields with underscores
        for split_point in 1..=keys.len() {
            let field_name = keys[0..split_point].join("_");
            let remaining_keys = &keys[split_point..];

            if let Some(field_value) = dict.get(&field_name) {
                if remaining_keys.is_empty() {
                    return true;
                } else if let FigmentValue::Dict(_, nested_dict) = field_value {
                    return Self::is_valid_field_path(nested_dict, remaining_keys);
                }
            }
        }

        // Also try the original approach with combined keys
        let mut current_dict = dict;
        let mut combined_keys = Vec::new();

        for key in keys {
            combined_keys.push(key.clone());
            let combined_field = combined_keys.join("_");

            if let Some(field_value) = current_dict.get(&combined_field) {
                match field_value {
                    FigmentValue::Dict(_, nested_dict) => {
                        current_dict = nested_dict;
                        combined_keys.clear();
                    }
                    _ => return true,
                }
            }
        }

        true
    }

    /// Apply HashMap override to target configuration
    fn apply_hashmap_override(
        target: &mut Dict,
        hashmap_key: &str,
        entry_key: &str,
        remaining_keys: &[String],
        source_hashmap: &Dict,
        value: FigmentValue,
    ) -> bool {
        // Ensure the HashMap exists in target
        target
            .entry(hashmap_key.to_string())
            .or_insert_with(|| FigmentValue::Dict(Tag::Default, Dict::new()));

        if let Some(FigmentValue::Dict(_, target_hashmap)) = target.get_mut(hashmap_key) {
            // Ensure the specific entry exists in the HashMap
            target_hashmap
                .entry(entry_key.to_string())
                .or_insert_with(|| {
                    if let Some(existing_entry) = source_hashmap.get(entry_key) {
                        existing_entry.clone()
                    } else {
                        FigmentValue::Dict(Tag::Default, Dict::new())
                    }
                });

            if remaining_keys.is_empty() {
                target_hashmap.insert(entry_key.to_string(), value);
            } else if let Some(FigmentValue::Dict(_, entry_dict)) =
                target_hashmap.get_mut(entry_key)
            {
                // Check if we need special handling for JSON value fields
                if let Some((json_field, json_keys)) =
                    Self::find_json_value_field_split(entry_dict, remaining_keys)
                {
                    Self::handle_json_value_override(entry_dict, &json_field, json_keys, value);
                } else {
                    Self::insert_environment_override(
                        &Dict::new(),
                        entry_dict,
                        remaining_keys.to_vec(),
                        value,
                    );
                }
            }
            return true;
        }

        false
    }

    /// Find JSON value field split in remaining keys
    fn find_json_value_field_split(dict: &Dict, keys: &[String]) -> Option<(String, Vec<String>)> {
        if keys.is_empty() {
            return None;
        }

        // Try different split points to find a field that exists
        for split_point in 1..=keys.len() {
            let potential_json_field = keys[0..split_point].join("_");
            let remaining_keys = keys[split_point..].to_vec();

            if dict.contains_key(&potential_json_field) && !remaining_keys.is_empty() {
                return Some((potential_json_field, remaining_keys));
            }
        }

        // If no existing field matches and we have multiple keys, use first key
        if keys.len() > 1 {
            let potential_new_field = keys[0].clone();
            let remaining_keys = keys[1..].to_vec();
            return Some((potential_new_field, remaining_keys));
        }

        None
    }

    /// Handle JSON value field override
    fn handle_json_value_override(
        entry_dict: &mut Dict,
        json_field: &str,
        json_keys: Vec<String>,
        value: FigmentValue,
    ) {
        // Ensure the JSON value field exists
        entry_dict
            .entry(json_field.to_string())
            .or_insert_with(|| FigmentValue::Dict(Tag::Default, Dict::new()));

        // Get or create the JSON field as a Dict
        if let Some(json_value) = entry_dict.get_mut(json_field) {
            let json_dict = match json_value {
                FigmentValue::Dict(_, dict) => dict,
                _ => {
                    *json_value = FigmentValue::Dict(Tag::Default, Dict::new());
                    if let FigmentValue::Dict(_, dict) = json_value {
                        dict
                    } else {
                        return;
                    }
                }
            };

            Self::set_nested_json_field(json_dict, &json_keys, value);
        }
    }

    /// Set nested JSON field value
    fn set_nested_json_field(dict: &mut Dict, keys: &[String], value: FigmentValue) {
        if keys.is_empty() {
            return;
        }

        // Always try the full field name first (with underscores)
        let full_field_name = keys.join("_");
        dict.insert(full_field_name, value);
    }

    /// Navigate to a specific dictionary in the nested structure
    fn navigate_to_dictionary<'a>(target: &'a mut Dict, path: &[String]) -> Option<&'a mut Dict> {
        if path.is_empty() {
            return Some(target);
        }

        let mut current = target;
        let mut combined_keys = Vec::new();

        for key in path {
            combined_keys.push(key.clone());
            let key_to_check = combined_keys.join("_");

            current
                .entry(key_to_check.clone())
                .or_insert_with(|| FigmentValue::Dict(Tag::Default, Dict::new()));

            match current.get_mut(&key_to_check) {
                Some(FigmentValue::Dict(_, inner)) => {
                    current = inner;
                    combined_keys.clear();
                }
                _ => return None,
            }
        }

        Some(current)
    }

    /// Find source array value for copying to target
    fn find_source_array(source: &Dict, path: &[String], array_key: &str) -> Option<FigmentValue> {
        if path.is_empty() {
            return None;
        }

        let mut current = source;
        let mut combined_keys = Vec::new();

        for key in &path[..path.len() - 1] {
            combined_keys.push(key.clone());
            let key_to_check = combined_keys.join("_");

            match current.get(&key_to_check) {
                Some(FigmentValue::Dict(_, inner)) => {
                    current = inner;
                    combined_keys.clear();
                }
                _ => return None,
            }
        }

        current.get(array_key).cloned()
    }

    /// Convert TOML value to Figment value
    fn toml_to_figment_value(toml_value: &TomlValue) -> FigmentValue {
        match toml_value {
            TomlValue::String(s) => FigmentValue::from(s.clone()),
            TomlValue::Integer(i) => FigmentValue::from(*i),
            TomlValue::Float(f) => FigmentValue::from(*f),
            TomlValue::Boolean(b) => FigmentValue::from(*b),
            TomlValue::Array(arr) => {
                let vec: Vec<FigmentValue> = arr.iter().map(Self::toml_to_figment_value).collect();
                FigmentValue::from(vec)
            }
            TomlValue::Table(tbl) => {
                let mut dict = Dict::new();
                for (key, value) in tbl.iter() {
                    dict.insert(key.clone(), Self::toml_to_figment_value(value));
                }
                FigmentValue::from(dict)
            }
            TomlValue::Datetime(_) => {
                // For now, convert datetime to string representation
                FigmentValue::from(toml_value.to_string())
            }
        }
    }

    /// Parse environment variable value with type inference
    fn parse_environment_value(value: &str) -> FigmentValue {
        // Handle array syntax
        if value.starts_with('[') && value.ends_with(']') {
            let inner_value = value.trim_start_matches('[').trim_end_matches(']');
            let values: Vec<FigmentValue> = inner_value
                .split(',')
                .map(|s| Self::parse_environment_value(s.trim()))
                .collect();
            return FigmentValue::from(values);
        }

        // Handle boolean values
        match value.to_lowercase().as_str() {
            "true" => return FigmentValue::from(true),
            "false" => return FigmentValue::from(false),
            _ => {}
        }

        // Try u64 first for most numeric values (ports, byte sizes, etc.)
        if let Ok(uint_val) = value.parse::<u64>() {
            return FigmentValue::from(uint_val);
        }
        // Fall back to i64 for signed integers
        if let Ok(int_val) = value.parse::<i64>() {
            return FigmentValue::from(int_val);
        }

        // Try parsing as float
        if let Ok(float_val) = value.parse::<f64>() {
            return FigmentValue::from(float_val);
        }

        // Default to string
        FigmentValue::from(value)
    }
}

/// Check if a file exists using the same logic as Figment
fn file_exists<P: AsRef<Path>>(path: P) -> bool {
    let path = path.as_ref();

    if path.is_absolute() {
        return path.is_file();
    }

    let cwd = match std::env::current_dir() {
        Ok(dir) => dir,
        Err(_) => return false,
    };

    let mut current_dir = cwd.as_path();
    loop {
        let file_path = current_dir.join(path);
        if file_path.is_file() {
            return true;
        }

        current_dir = match current_dir.parent() {
            Some(parent) => parent,
            None => return false,
        };
    }
}

impl<T: ConfigurationType, P: Provider + Clone> ConfigProvider<T> for FileConfigProvider<P> {
    async fn load_config(&self) -> Result<T, ConfigurationError> {
        info!("Loading config from path: '{}'...", self.file_path);

        // Start with the default configuration if provided
        let mut config_builder = Figment::new();
        let has_default = self.default_config.is_some();
        if let Some(default) = &self.default_config {
            config_builder = config_builder.merge(default);
        } else {
            warn!("No default configuration provided.");
        }

        // If the config file exists, merge it into the configuration
        if file_exists(&self.file_path) {
            info!("Found configuration file at path: '{}'.", self.file_path);
            config_builder = config_builder.merge(Toml::file(&self.file_path));
        } else {
            warn!(
                "Configuration file not found at path: '{}'.",
                self.file_path
            );
            if has_default {
                info!(
                    "Using default configuration embedded into server, as no config file was found."
                );
            }
        }

        // Merge environment variables into the configuration
        config_builder = config_builder.merge(self.env_provider.clone());

        // Finally, attempt to extract the final configuration
        let config_result: Result<T, figment::Error> = config_builder.extract();

        match config_result {
            Ok(config) => {
                info!("Config loaded successfully.");
                let display_config = env::var(DISPLAY_CONFIG_ENV)
                    .map(|val| val == "1" || val.to_lowercase() == "true")
                    .unwrap_or(self.display_config);
                if display_config {
                    info!("Using Config: {config}");
                }
                Ok(config)
            }
            Err(e) => {
                error!("Failed to load config: {e}");
                Err(ConfigurationError::CannotLoadConfiguration)
            }
        }
    }
}
