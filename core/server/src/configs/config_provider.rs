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

use crate::IGGY_ROOT_PASSWORD_ENV;
use crate::configs::server::ServerConfig;
use crate::server_error::ConfigError;
use figment::{
    Error, Figment, Metadata, Profile, Provider,
    providers::{Format, Toml},
    value::{Dict, Map as FigmentMap, Tag, Value as FigmentValue},
};
use std::{env, future::Future, path::Path};
use toml::{Value as TomlValue, map::Map};

const DEFAULT_CONFIG_PROVIDER: &str = "file";
const DEFAULT_CONFIG_PATH: &str = "configs/server.toml";
const SECRET_KEYS: [&str; 6] = [
    IGGY_ROOT_PASSWORD_ENV,
    "IGGY_DATA_MAINTENANCE_ARCHIVER_S3_KEY_SECRET",
    "IGGY_HTTP_JWT_ENCODING_SECRET",
    "IGGY_HTTP_JWT_DECODING_SECRET",
    "IGGY_TCP_TLS_PASSWORD",
    "IGGY_SYSTEM_ENCRYPTION_KEY",
];

pub enum ConfigProviderKind {
    File(FileConfigProvider),
}

impl ConfigProviderKind {
    pub async fn load_config(&self) -> Result<ServerConfig, ConfigError> {
        match self {
            Self::File(p) => p.load_config().await,
        }
    }
}

pub trait ConfigProvider {
    fn load_config(&self) -> impl Future<Output = Result<ServerConfig, ConfigError>>;
}

#[derive(Debug)]
pub struct FileConfigProvider {
    path: String,
}

pub struct CustomEnvProvider {
    prefix: String,
}

impl FileConfigProvider {
    pub fn new(path: String) -> Self {
        Self { path }
    }
}

impl CustomEnvProvider {
    pub fn new(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
        }
    }

    fn walk_toml_table_to_dict(prefix: &str, table: Map<String, TomlValue>, dict: &mut Dict) {
        for (key, value) in table {
            let new_prefix = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{prefix}.{key}")
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

    fn insert_overridden_values_from_env(
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
            Self::handle_dict_override(source, target, &keys, value);
        }
    }

    fn find_array_index(keys: &[String]) -> Option<(usize, usize)> {
        keys.iter()
            .enumerate()
            .find_map(|(i, key)| key.parse::<usize>().ok().map(|idx| (i, idx)))
    }

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
        let current_target = match Self::navigate_to_dict(
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
                Self::insert_overridden_values_from_env(
                    &Dict::new(),
                    elem_dict,
                    remaining_path.to_vec(),
                    value,
                );
            }
        }
    }

    fn handle_dict_override(
        source: &Dict,
        target: &mut Dict,
        keys: &[String],
        value: FigmentValue,
    ) {
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

    fn navigate_to_dict<'a>(target: &'a mut Dict, path: &[String]) -> Option<&'a mut Dict> {
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
                let mut dict = figment::value::Dict::new();
                for (key, value) in tbl.iter() {
                    dict.insert(key.clone(), Self::toml_to_figment_value(value));
                }
                FigmentValue::from(dict)
            }
            TomlValue::Datetime(_) => todo!("not implemented yet!"),
        }
    }

    fn try_parse_value(value: &str) -> FigmentValue {
        if value.starts_with('[') && value.ends_with(']') {
            let value = value.trim_start_matches('[').trim_end_matches(']');
            let values: Vec<FigmentValue> = value.split(',').map(Self::try_parse_value).collect();
            return FigmentValue::from(values);
        }
        if value == "true" {
            return FigmentValue::from(true);
        }
        if value == "false" {
            return FigmentValue::from(false);
        }
        if let Ok(int_val) = value.parse::<i64>() {
            return FigmentValue::from(int_val);
        }
        if let Ok(float_val) = value.parse::<f64>() {
            return FigmentValue::from(float_val);
        }
        FigmentValue::from(value)
    }
}

impl Provider for CustomEnvProvider {
    fn metadata(&self) -> Metadata {
        Metadata::named("iggy-server config")
    }

    fn data(&self) -> Result<FigmentMap<Profile, Dict>, Error> {
        let default_config = toml::to_string(&ServerConfig::default())
            .expect("Cannot serialize default ServerConfig. Something's terribly wrong.");
        let toml_value: TomlValue = toml::from_str(&default_config).unwrap();
        let mut source_dict = Dict::new();
        if let TomlValue::Table(table) = toml_value {
            Self::walk_toml_table_to_dict("", table, &mut source_dict);
        }

        let mut new_dict = Dict::new();
        for (key, mut value) in env::vars() {
            let env_key = key.to_uppercase();
            if !env_key.starts_with(self.prefix.as_str()) {
                continue;
            }
            let keys: Vec<String> = env_key[self.prefix.len()..]
                .split('_')
                .map(|k| k.to_lowercase())
                .collect();
            let env_var_value = Self::try_parse_value(&value);
            if SECRET_KEYS.contains(&env_key.as_str()) {
                value = "******".to_string();
            }

            println!("{env_key} value changed to: {value} from environment variable");
            Self::insert_overridden_values_from_env(
                &source_dict,
                &mut new_dict,
                keys.clone(),
                env_var_value.clone(),
            );
        }
        let mut data = FigmentMap::new();
        data.insert(Profile::default(), new_dict);

        Ok(data)
    }
}

pub fn resolve(config_provider_type: &str) -> Result<ConfigProviderKind, ConfigError> {
    match config_provider_type {
        DEFAULT_CONFIG_PROVIDER => {
            let path =
                env::var("IGGY_CONFIG_PATH").unwrap_or_else(|_| DEFAULT_CONFIG_PATH.to_string());
            Ok(ConfigProviderKind::File(FileConfigProvider::new(path)))
        }
        _ => Err(ConfigError::InvalidConfigurationProvider {
            provider_type: config_provider_type.to_string(),
        }),
    }
}

/// This does exactly the same as Figment does internally.
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

impl ConfigProvider for FileConfigProvider {
    async fn load_config(&self) -> Result<ServerConfig, ConfigError> {
        println!("Loading config from path: '{}'...", self.path);

        // Include the default configuration from server.toml
        let embedded_default_config = Toml::string(include_str!("../../../configs/server.toml"));

        // Start with the default configuration
        let mut config_builder = Figment::new().merge(embedded_default_config);

        // If the server.toml file exists, merge it into the configuration
        if file_exists(&self.path) {
            println!("Found configuration file at path: '{}'.", self.path);
            config_builder = config_builder.merge(Toml::file(&self.path));
        } else {
            println!(
                "Configuration file not found at path: '{}'. Using default configuration from embedded server.toml.",
                self.path
            );
        }

        // Merge environment variables into the configuration
        config_builder = config_builder.merge(CustomEnvProvider::new("IGGY_"));

        // Finally, attempt to extract the final configuration
        let config_result: Result<ServerConfig, figment::Error> = config_builder.extract();

        match config_result {
            Ok(config) => {
                println!("Config loaded successfully.");
                println!("Using Config: {config}");
                Ok(config)
            }
            Err(e) => {
                println!("Failed to load config: {e}");
                Err(ConfigError::CannotLoadConfiguration)
            }
        }
    }
}
