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

//! Runtime validation and resolution of config paths to environment variables.

use configs::ConfigEnvMappings;
use server::configs::server::ServerConfig;
use std::collections::HashMap;

/// Resolve config paths to environment variable names.
///
/// Takes a map of dot-notation config paths (e.g., "segment.size") and their values,
/// validates them against the `ServerConfig` mappings, and returns the corresponding
/// environment variable names with values.
///
/// # Implicit defaults
///
/// - Setting any `tcp.socket.*` option automatically enables `tcp.socket.override_defaults`
/// - Setting any `quic.socket.*` option automatically enables `quic.socket.override_defaults`
///
/// # Errors
///
/// Returns an error message if any path is invalid, including suggestions for similar paths.
pub fn resolve_config_paths(
    overrides: &HashMap<String, String>,
) -> Result<HashMap<String, String>, String> {
    let mut env_vars = HashMap::new();
    let mut needs_tcp_socket_override = false;
    let mut needs_quic_socket_override = false;
    let mut needs_encryption_enabled = false;

    for (path, value) in overrides {
        // Special shorthand: "encryption" maps to "system.encryption.key"
        let resolved_path = if path == "encryption" {
            "system.encryption.key"
        } else {
            path.as_str()
        };

        let mapping = ServerConfig::find_by_config_path(resolved_path)
            .or_else(|| ServerConfig::find_by_config_path(&format!("system.{}", resolved_path)));

        match mapping {
            Some(m) => {
                env_vars.insert(m.env_name.to_string(), value.clone());

                // Track if we need to enable socket override_defaults
                if path.starts_with("tcp.socket.") && path != "tcp.socket.override_defaults" {
                    needs_tcp_socket_override = true;
                }
                if path.starts_with("quic.socket.") && path != "quic.socket.override_defaults" {
                    needs_quic_socket_override = true;
                }
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

    // Auto-enable override_defaults when socket settings are customized
    if needs_tcp_socket_override
        && let Some(m) = ServerConfig::find_by_config_path("tcp.socket.override_defaults")
    {
        env_vars
            .entry(m.env_name.to_string())
            .or_insert_with(|| "true".to_string());
    }
    if needs_quic_socket_override
        && let Some(m) = ServerConfig::find_by_config_path("quic.socket.override_defaults")
    {
        env_vars
            .entry(m.env_name.to_string())
            .or_insert_with(|| "true".to_string());
    }
    // Auto-enable encryption when key is set
    if needs_encryption_enabled
        && let Some(m) = ServerConfig::find_by_config_path("system.encryption.enabled")
    {
        env_vars
            .entry(m.env_name.to_string())
            .or_insert_with(|| "true".to_string());
    }

    Ok(env_vars)
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

    candidates.sort_by_key(|(_, score)| *score);

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
        overrides.insert("system.segment.size".to_string(), "1MiB".to_string());

        let result = resolve_config_paths(&overrides);
        assert!(result.is_ok());
        let env_vars = result.unwrap();
        assert!(env_vars.contains_key("IGGY_SYSTEM_SEGMENT_SIZE"));
        assert_eq!(
            env_vars.get("IGGY_SYSTEM_SEGMENT_SIZE"),
            Some(&"1MiB".to_string())
        );
    }

    #[test]
    fn resolve_with_implicit_system_prefix() {
        let mut overrides = HashMap::new();
        overrides.insert("segment.size".to_string(), "1MiB".to_string());

        let result = resolve_config_paths(&overrides);
        assert!(result.is_ok());
        let env_vars = result.unwrap();
        assert!(env_vars.contains_key("IGGY_SYSTEM_SEGMENT_SIZE"));
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
