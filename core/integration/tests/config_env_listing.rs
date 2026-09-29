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

use assert_cmd::Command;
use std::collections::HashSet;

#[test]
fn config_env_listing_exits_before_startup_for_each_binary() {
    for (binary, config_env, dotenv_env) in [
        ("iggy-server", "IGGY_CONFIG_PATH", "IGGY_ENV_PATH"),
        (
            "iggy-connectors",
            "IGGY_CONNECTORS_CONFIG_PATH",
            "IGGY_CONNECTORS_ENV_PATH",
        ),
        ("iggy-mcp", "IGGY_MCP_CONFIG_PATH", "IGGY_MCP_ENV_PATH"),
    ] {
        let directory = tempfile::tempdir().expect("temporary directory");
        let output = Command::cargo_bin(binary)
            .expect("binary should be built")
            .current_dir(directory.path())
            .arg("--list-config-env-vars")
            .env(config_env, directory.path().join("missing-config.toml"))
            .env(dotenv_env, directory.path().join("missing.env"))
            .output()
            .expect("listing command should run");

        assert!(output.status.success(), "{binary} failed");
        assert!(output.stderr.is_empty(), "{binary} wrote to stderr");

        let stdout = String::from_utf8(output.stdout).expect("UTF-8 output");
        let names: Vec<_> = stdout.lines().collect();
        assert!(!names.is_empty(), "{binary} returned no variables");
        assert!(
            names.windows(2).all(|pair| pair[0] < pair[1]),
            "{binary} output must be sorted and deduplicated"
        );
        assert!(names.iter().all(|name| name.starts_with("IGGY_")));
    }
}

#[test]
fn config_env_listing_includes_vector_index_templates() {
    // Verify that vector fields are correctly represented with <N> placeholder
    let output = Command::cargo_bin("iggy-server")
        .expect("binary should be built")
        .arg("--list-config-env-vars")
        .output()
        .expect("listing command should run");

    let stdout = String::from_utf8(output.stdout).expect("UTF-8 output");
    let names: HashSet<_> = stdout.lines().collect();

    // Cluster nodes should have indexed templates
    assert!(
        names
            .iter()
            .any(|name| name.contains("IGGY_CLUSTER_NODES_<N>_")),
        "server should list cluster node index templates"
    );

    // Verify nested vector expansion (nested <N> placeholders)
    assert!(
        names
            .iter()
            .any(|name| name.contains("IGGY_CLUSTER_NODES_<N>_ADVERTISED_ADDRESSES_<N>_")),
        "server should list nested vector templates"
    );
}

#[test]
fn config_env_listing_includes_connector_templates() {
    // Verify that connector SINK/SOURCE templates are correctly formatted
    let output = Command::cargo_bin("iggy-connectors")
        .expect("binary should be built")
        .arg("--list-config-env-vars")
        .output()
        .expect("listing command should run");

    let stdout = String::from_utf8(output.stdout).expect("UTF-8 output");
    let names: HashSet<_> = stdout.lines().collect();

    // Both SINK and SOURCE templates should be present
    assert!(
        names
            .iter()
            .any(|name| name.contains("IGGY_CONNECTORS_SINK_<KEY>_")),
        "connectors should list SINK templates with <KEY> placeholder"
    );
    assert!(
        names
            .iter()
            .any(|name| name.contains("IGGY_CONNECTORS_SOURCE_<KEY>_")),
        "connectors should list SOURCE templates with <KEY> placeholder"
    );

    // Plugin config templates should be present
    assert!(
        names
            .iter()
            .any(|name| name.contains("IGGY_CONNECTORS_SINK_<KEY>_PLUGIN_CONFIG_<FIELD>")),
        "connectors should list SINK plugin config templates"
    );
    assert!(
        names
            .iter()
            .any(|name| name.contains("IGGY_CONNECTORS_SOURCE_<KEY>_PLUGIN_CONFIG_<FIELD>")),
        "connectors should list SOURCE plugin config templates"
    );
}

#[test]
fn config_env_listing_deduplicates_output() {
    // Verify no duplicates in output across multiple runs
    for binary in ["iggy-server", "iggy-connectors", "iggy-mcp"] {
        let output = Command::cargo_bin(binary)
            .expect("binary should be built")
            .arg("--list-config-env-vars")
            .output()
            .expect("listing command should run");

        let stdout = String::from_utf8(output.stdout).expect("UTF-8 output");
        let names: Vec<_> = stdout.lines().collect();
        let unique_names: HashSet<_> = names.iter().collect();

        assert_eq!(
            names.len(),
            unique_names.len(),
            "{binary} output contains duplicates"
        );
    }
}

#[test]
fn config_env_listing_exits_before_runtime_with_invalid_env_values() {
    // Verify that invalid environment values don't cause failures
    let output = Command::cargo_bin("iggy-server")
        .expect("binary should be built")
        .arg("--list-config-env-vars")
        .env("IGGY_TCP_PORT", "not_a_number") // Invalid port value
        .env("IGGY_HTTP_ADDRESS", "invalid::address") // Invalid address
        .output()
        .expect("listing command should run");

    assert!(
        output.status.success(),
        "listing should exit before validating environment values"
    );
    assert!(output.stderr.is_empty(), "should not log validation errors");
}
