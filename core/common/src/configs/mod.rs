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

use figment::{
    Figment, Profile, Provider,
    providers::{Data, Format, Toml},
    value::{Dict, Map as FigmentMap, Tag, Value as FigmentValue},
};
use serde::{Serialize, de::DeserializeOwned};
use std::{env, fmt::Display, future::Future, marker::PhantomData, path::Path};
use toml::{Value as TomlValue, map::Map};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigurationError {
    CannotLoadConfiguration,
}

pub trait ConfigProvider<T: Serialize + DeserializeOwned + Default + Display> {
    fn load_config(self) -> impl Future<Output = Result<T, ConfigurationError>>;
}

pub struct FileConfigProvider<T: Provider> {
    path: String,
    default_config: Data<Toml>,
    env_provider: T,
}

impl<T: Provider> FileConfigProvider<T> {
    pub fn new(path: String, default_config: Data<Toml>, env_provider: T) -> Self {
        Self {
            path,
            default_config,
            env_provider,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CustomEnvProvider<T: Serialize + DeserializeOwned + Default + Display> {
    prefix: String,
    secret_keys: Vec<String>,
    _data: PhantomData<T>,
}

impl<T: Serialize + DeserializeOwned + Default + Display> CustomEnvProvider<T> {
    pub fn new(prefix: &str, secret_keys: &[&str]) -> Self {
        Self {
            prefix: prefix.to_string(),
            secret_keys: secret_keys.iter().map(|s| s.to_string()).collect(),
            _data: PhantomData,
        }
    }

    pub fn deserialize(&self) -> Result<FigmentMap<Profile, Dict>, ConfigurationError> {
        let default_config = toml::to_string(&T::default())
            .expect("Cannot serialize default Config. Something's terribly wrong.");
        let toml_value: TomlValue = toml::from_str(&default_config)
            .expect("Cannot parse default Config. Something's terribly wrong.");
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
            if self.secret_keys.contains(&env_key) {
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
                // For it to be a HashMap, it should either:
                // 1. Have Dict values (actual HashMap entries)
                // 2. Be empty (empty HashMap)
                // 3. But NOT have regular struct fields

                let has_dict_values = hashmap_dict
                    .values()
                    .any(|v| matches!(v, FigmentValue::Dict(_, _)));
                let has_non_dict_values = hashmap_dict
                    .values()
                    .any(|v| !matches!(v, FigmentValue::Dict(_, _)));

                // If it has non-Dict values mixed with Dict values, it's probably a regular struct, not a HashMap
                if has_non_dict_values && has_dict_values {
                    return Some(false);
                }

                // If it only has non-Dict values, it's definitely a regular struct
                if has_non_dict_values && !has_dict_values {
                    return Some(false);
                }

                // If it's empty or only has Dict values, it could be a HashMap
                hashmap_dict
            }
            _ => return Some(false),
        };

        // Try to find the best HashMap entry key
        if let Some((entry_key, remaining_keys)) =
            Self::find_best_hashmap_split(source_hashmap, &keys[1..])
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

    fn find_best_hashmap_split(
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
                // Check if we need special handling for serde_json::Value fields
                if let Some((json_field, json_keys)) =
                    Self::find_json_value_field_split(entry_dict, remaining_keys)
                {
                    Self::handle_json_value_override(entry_dict, &json_field, json_keys, value);
                } else {
                    Self::insert_overridden_values_from_env(
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

    fn set_nested_json_field(dict: &mut Dict, keys: &[String], value: FigmentValue) {
        if keys.is_empty() {
            return;
        }

        // Always try the full field name first (with underscores)
        let full_field_name = keys.join("_");
        dict.insert(full_field_name, value);
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

impl<T: Serialize + DeserializeOwned + Default + Display, P: Provider + Clone> ConfigProvider<T>
    for FileConfigProvider<P>
{
    async fn load_config(self) -> Result<T, ConfigurationError> {
        println!("Loading config from path: '{}'...", self.path);

        // Start with the default configuration
        let mut config_builder = Figment::new().merge(self.default_config);

        // If the config file exists, merge it into the configuration
        if file_exists(&self.path) {
            println!("Found configuration file at path: '{}'.", self.path);
            config_builder = config_builder.merge(Toml::file(&self.path));
        } else {
            println!(
                "Configuration file not found at path: '{}'. Using default configuration from embedded config",
                self.path
            );
        }

        // Merge environment variables into the configuration
        config_builder = config_builder.merge(self.env_provider);

        // Finally, attempt to extract the final configuration
        let config_result: Result<T, figment::Error> = config_builder.extract();

        match config_result {
            Ok(config) => {
                println!("Config loaded successfully.");
                println!("Using Config: {config}");
                Ok(config)
            }
            Err(e) => {
                println!("Failed to load config: {e}");
                Err(ConfigurationError::CannotLoadConfiguration)
            }
        }
    }
}
