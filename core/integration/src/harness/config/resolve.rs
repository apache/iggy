// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Runtime validation and resolution of config paths to environment variables.

use configs::server::ServerConfig;
use configs::{ConfigEnvMappings, EnvVarMapping};
use std::collections::HashMap;

/// `IGGY_`-prefixed variables the server reads outside the config struct, so
/// `ServerConfig::all_env_var_names` cannot know them. `IGGY_CONFIG_PATH`
/// selects the config file itself and the root credentials are consumed by
/// `args.rs` before the config loads; `IGGY_TEST_VERBOSE` is harness-only.
pub const NON_CONFIG_ENV_VARS: [&str; 4] = [
    "IGGY_CONFIG_PATH",
    "IGGY_ROOT_USERNAME",
    "IGGY_ROOT_PASSWORD",
    "IGGY_TEST_VERBOSE",
];

/// Resolve config paths to environment variable names.
///
/// Takes a map of dot-notation config paths (e.g., "partition.validate_checksum") and their values,
/// validates them against the `ServerConfig` mappings, and returns the
/// corresponding environment variable names with values.
///
/// # Implicit defaults
///
/// - `encryption` is shorthand for `system.encryption.key`.
/// - Setting that key also turns `system.encryption.enabled` on, unless the
///   caller passed it explicitly.
///
/// # Errors
///
/// Returns an error message if any path is invalid, including suggestions for similar paths.
pub fn resolve_config_paths(
    overrides: &HashMap<String, String>,
) -> Result<HashMap<String, String>, String> {
    let mut env_vars = HashMap::new();
    let mut needs_encryption_enabled = false;

    for (path, value) in overrides {
        // Special shorthand: "encryption" maps to "system.encryption.key"
        let resolved_path = if path == "encryption" {
            "system.encryption.key"
        } else {
            path.as_str()
        };

        let mapping = find_mapping(resolved_path)
            .or_else(|| find_mapping(&format!("system.{}", resolved_path)));

        match mapping {
            Some(m) => {
                env_vars.insert(m.env_name.to_string(), value.clone());

                // Track if encryption key is set (auto-enable encryption)
                if path == "encryption"
                    || path == "encryption.key"
                    || path == "system.encryption.key"
                {
                    needs_encryption_enabled = true;
                }
            }
            None => {
                let suggestions = find_similar_paths(path);

                let suggestions_str = if suggestions.is_empty() {
                    String::new()
                } else {
                    format!("\nDid you mean: {:?}?", suggestions)
                };

                return Err(format!(
                    "Unknown config path: '{}'{}",
                    path, suggestions_str
                ));
            }
        }
    }

    // Auto-enable encryption when key is set
    if needs_encryption_enabled && let Some(m) = find_mapping("system.encryption.enabled") {
        env_vars
            .entry(m.env_name.to_string())
            .or_insert_with(|| "true".to_string());
    }

    Ok(env_vars)
}

fn find_mapping(path: &str) -> Option<&'static EnvVarMapping> {
    ServerConfig::find_by_config_path(path)
}

/// Reject `IGGY_*` names that no config leaf reads.
///
/// `extra_envs` is a raw env map with no schema behind it, so a name that was
/// valid before a config key moved or was deleted keeps being set and simply
/// stops doing anything. That failure mode is silent and expensive: the server
/// boots on defaults, the test appears to configure something it does not, and
/// the resulting behavior change surfaces somewhere unrelated. Attribute
/// overrides never had this problem (`resolve_config_paths` validates them);
/// this closes the same gap for the direct path.
///
/// Names outside the `IGGY_` prefix are left alone: those address the process
/// environment (`RUST_LOG`, test scaffolding), not the config schema.
/// `NON_CONFIG_ENV_VARS` carries the `IGGY_`-prefixed names the server reads
/// outside the config struct.
///
/// # Errors
///
/// Returns a message naming every unknown variable, with near-miss
/// suggestions drawn from the live mapping table.
pub fn validate_env_var_names(envs: &HashMap<String, String>) -> Result<(), String> {
    let known: Vec<&'static str> = ServerConfig::all_env_var_names();
    let mut unknown: Vec<&String> = envs
        .keys()
        .filter(|name| {
            name.starts_with("IGGY_")
                && !known.contains(&name.as_str())
                && !NON_CONFIG_ENV_VARS.contains(&name.as_str())
        })
        .collect();
    if unknown.is_empty() {
        return Ok(());
    }
    unknown.sort();
    let mut report = String::new();
    for name in unknown {
        report.push_str(&format!("  unknown config env var: {name}\n"));
        let lowered = name.trim_start_matches("IGGY_").to_ascii_lowercase();
        let mut near: Vec<&&str> = known
            .iter()
            .filter(|candidate| {
                let candidate = candidate.trim_start_matches("IGGY_").to_ascii_lowercase();
                candidate.ends_with(&lowered)
                    || lowered.ends_with(&candidate)
                    || levenshtein(&candidate, &lowered) <= 4
            })
            .collect();
        near.sort();
        near.truncate(5);
        if !near.is_empty() {
            report.push_str("    did you mean: ");
            report.push_str(
                &near
                    .iter()
                    .map(std::string::ToString::to_string)
                    .collect::<Vec<_>>()
                    .join(", "),
            );
            report.push('\n');
        }
    }
    Err(report)
}

fn levenshtein(a: &str, b: &str) -> usize {
    let a_len = a.len();
    let b_len = b.len();

    if a_len == 0 {
        return b_len;
    }
    if b_len == 0 {
        return a_len;
    }

    let mut prev_row: Vec<usize> = (0..=b_len).collect();
    let mut curr_row: Vec<usize> = vec![0; b_len + 1];

    for (i, ca) in a.chars().enumerate() {
        curr_row[0] = i + 1;

        for (j, cb) in b.chars().enumerate() {
            let cost = if ca == cb { 0 } else { 1 };
            curr_row[j + 1] = (prev_row[j + 1] + 1)
                .min(curr_row[j] + 1)
                .min(prev_row[j] + cost);
        }

        std::mem::swap(&mut prev_row, &mut curr_row);
    }

    prev_row[b_len]
}

fn find_similar_paths(unknown: &str) -> Vec<String> {
    let normalized = unknown.replace('_', ".");

    let mut candidates: Vec<_> = ServerConfig::env_mappings()
        .iter()
        .filter_map(|m| {
            let path = m.config_path;

            if path.ends_with(&normalized) || path.ends_with(unknown) {
                return Some((path, 0)); // Perfect suffix match
            }

            let last_segment = path.rsplit('.').next().unwrap_or(path);
            let unknown_last = normalized.rsplit('.').next().unwrap_or(&normalized);
            if last_segment == unknown_last || last_segment == unknown {
                return Some((path, 1)); // Segment match
            }

            let segment_dist = levenshtein(unknown_last, last_segment);
            if segment_dist <= 3 {
                return Some((path, segment_dist + 2));
            }

            let full_dist = levenshtein(&normalized, path);
            if full_dist <= 8 {
                return Some((path, full_dist + 5));
            }

            None
        })
        .collect();

    candidates.sort_by_key(|(path, score)| (*score, *path));

    candidates
        .into_iter()
        .take(3)
        .map(|(path, _)| path.to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_valid_path() {
        let mut overrides = HashMap::new();
        overrides.insert(
            "system.partition.validate_checksum".to_string(),
            "false".to_string(),
        );

        let result = resolve_config_paths(&overrides);
        assert!(result.is_ok());
        let env_vars = result.unwrap();
        assert!(env_vars.contains_key("IGGY_SYSTEM_PARTITION_VALIDATE_CHECKSUM"));
        assert_eq!(
            env_vars.get("IGGY_SYSTEM_PARTITION_VALIDATE_CHECKSUM"),
            Some(&"false".to_string())
        );
    }

    #[test]
    fn resolve_with_implicit_system_prefix() {
        let mut overrides = HashMap::new();
        overrides.insert(
            "partition.validate_checksum".to_string(),
            "false".to_string(),
        );

        let result = resolve_config_paths(&overrides);
        assert!(result.is_ok());
        let env_vars = result.unwrap();
        assert!(env_vars.contains_key("IGGY_SYSTEM_PARTITION_VALIDATE_CHECKSUM"));
    }

    #[test]
    fn validate_env_var_names_accepts_live_names_and_passes_through_non_iggy() {
        let envs = HashMap::from([
            (
                "IGGY_SYSTEM_PARTITION_VALIDATE_CHECKSUM".to_string(),
                "false".to_string(),
            ),
            ("RUST_LOG".to_string(), "debug".to_string()),
        ]);
        assert!(validate_env_var_names(&envs).is_ok());
    }

    #[test]
    fn validate_env_var_names_accepts_the_non_config_variables() {
        let envs: HashMap<String, String> = NON_CONFIG_ENV_VARS
            .iter()
            .map(|name| ((*name).to_string(), "value".to_string()))
            .collect();
        assert!(
            validate_env_var_names(&envs).is_ok(),
            "variables the server reads outside the config struct must pass"
        );
    }

    #[test]
    fn validate_env_var_names_rejects_a_name_no_config_leaf_reads() {
        // The exact shape that went silent when these keys moved to per-topic
        // options: a name that was valid before and now does nothing.
        let envs = HashMap::from([("IGGY_SYSTEM_SEGMENT_SIZE".to_string(), "1MiB".to_string())]);
        let error = validate_env_var_names(&envs).expect_err("deleted key must be rejected");
        assert!(
            error.contains("IGGY_SYSTEM_SEGMENT_SIZE"),
            "the report must name the offending variable, got: {error}"
        );
    }

    #[test]
    fn resolve_invalid_path() {
        let mut overrides = HashMap::new();
        overrides.insert("segmant.size".to_string(), "1MiB".to_string());

        let result = resolve_config_paths(&overrides);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.contains("Unknown config path: 'segmant.size'"));
    }

    #[test]
    fn resolve_encryption_shorthand_auto_enables() {
        let mut overrides = HashMap::new();
        overrides.insert(
            "encryption".to_string(),
            "/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=".to_string(),
        );

        let result = resolve_config_paths(&overrides);
        assert!(result.is_ok());
        let env_vars = result.unwrap();
        assert_eq!(
            env_vars.get("IGGY_SYSTEM_ENCRYPTION_KEY"),
            Some(&"/rvT1xP4V8u1EAhk4xDdqzqM2UOPXyy9XYkl4uRShgE=".to_string())
        );
        assert_eq!(
            env_vars.get("IGGY_SYSTEM_ENCRYPTION_ENABLED"),
            Some(&"true".to_string())
        );
    }

    #[test]
    fn levenshtein_identical() {
        assert_eq!(levenshtein("hello", "hello"), 0);
    }

    #[test]
    fn levenshtein_one_char_diff() {
        assert_eq!(levenshtein("hello", "hallo"), 1);
    }

    #[test]
    fn levenshtein_empty() {
        assert_eq!(levenshtein("", "hello"), 5);
        assert_eq!(levenshtein("hello", ""), 5);
    }
}
