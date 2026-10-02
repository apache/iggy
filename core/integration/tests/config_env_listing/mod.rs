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
use std::time::Duration;

const LIST_ENV_VARS_TIMEOUT: Duration = Duration::from_secs(5);

/// Runs `binary --list-config-env-vars` (plus `extra_args`) with `env` applied,
/// isolated to `directory`. Shared by every test below so each only states
/// what differs: the binary, the env it sets, and any extra flags.
fn run_list_config_env_vars_in<I, K, V>(
    binary: &str,
    directory: &std::path::Path,
    env: I,
    extra_args: &[&str],
) -> std::process::Output
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<std::ffi::OsStr>,
    V: AsRef<std::ffi::OsStr>,
{
    let mut cmd = Command::cargo_bin(binary).expect("binary should be built");
    cmd.current_dir(directory)
        .arg("--list-config-env-vars")
        .timeout(LIST_ENV_VARS_TIMEOUT);

    for (key, value) in env {
        cmd.env(key, value);
    }

    for arg in extra_args {
        cmd.arg(arg);
    }

    cmd.output().expect("listing command should run")
}

/// Convenience wrapper over [`run_list_config_env_vars_in`] for tests that
/// don't need to inspect the working directory afterward.
fn run_list_config_env_vars<I, K, V>(
    binary: &str,
    env: I,
    extra_args: &[&str],
) -> std::process::Output
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<std::ffi::OsStr>,
    V: AsRef<std::ffi::OsStr>,
{
    let directory = tempfile::tempdir().expect("temporary directory");
    run_list_config_env_vars_in(binary, directory.path(), env, extra_args)
}

/// Convenience wrapper over [`run_list_config_env_vars`] for tests that don't
/// need to set any environment variables.
fn run_list_config_env_vars_plain(binary: &str, extra_args: &[&str]) -> std::process::Output {
    run_list_config_env_vars(binary, std::iter::empty::<(&str, &str)>(), extra_args)
}

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
        let output = run_list_config_env_vars_in(
            binary,
            directory.path(),
            [
                (config_env, directory.path().join("missing-config.toml")),
                (dotenv_env, directory.path().join("missing.env")),
            ],
            &[],
        );

        assert!(
            output.status.success(),
            "{binary} failed: {:?}\nstderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stderr.is_empty(), "{binary} wrote to stderr");

        let stdout = String::from_utf8(output.stdout).expect("UTF-8 output");
        let names: Vec<_> = stdout.lines().collect();
        assert!(!names.is_empty(), "{binary} returned no variables");
        assert!(
            names.windows(2).all(|pair| pair[0] < pair[1]),
            "{binary} output must be sorted and deduplicated"
        );
        assert!(
            names.iter().all(|name| name.starts_with("IGGY_")),
            "{binary} output contained non-IGGY_ prefixed names"
        );
    }
}

#[test]
fn config_env_listing_includes_vector_index_templates() {
    // Verify that vector fields are correctly represented with <N> placeholder
    let output = run_list_config_env_vars_plain("iggy-server", &[]);

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
    let output = run_list_config_env_vars_plain("iggy-connectors", &[]);

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

    // Verify SINK and SOURCE ENABLED fields are present
    assert!(
        names
            .iter()
            .any(|name| name.contains("IGGY_CONNECTORS_SINK_<KEY>_ENABLED")),
        "connectors should list SINK_<KEY>_ENABLED"
    );
    assert!(
        names
            .iter()
            .any(|name| name.contains("IGGY_CONNECTORS_SOURCE_<KEY>_ENABLED")),
        "connectors should list SOURCE_<KEY>_ENABLED"
    );
}

#[test]
fn config_env_listing_exits_before_runtime_with_invalid_env_values() {
    // Verify that invalid environment values don't cause failures
    let output = run_list_config_env_vars(
        "iggy-server",
        [("IGGY_HTTP_ADDRESS", "invalid::address")], // Invalid address
        &[],
    );

    assert!(
        output.status.success(),
        "listing should exit before validating environment values: {:?}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty(), "should not log validation errors");
}

#[test]
fn config_env_listing_with_fresh_does_not_wipe_data_dir() {
    // Verify that --fresh does not wipe data when used with --list-config-env-vars
    // Create a tempdir with a sentinel file
    let directory = tempfile::tempdir().expect("temporary directory");
    let data_dir = directory.path().join("data");
    std::fs::create_dir(&data_dir).expect("create data directory");
    let sentinel = data_dir.join("sentinel.txt");
    std::fs::write(&sentinel, "sentinel content").expect("write sentinel file");

    // Run with both --fresh and --list-config-env-vars
    let output = run_list_config_env_vars_in(
        "iggy-server",
        directory.path(),
        [("IGGY_PATH", data_dir.to_string_lossy().into_owned())],
        &["--fresh"],
    );

    assert!(
        output.status.success(),
        "command failed: {:?}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        sentinel.exists(),
        "sentinel file should not be deleted by early exit"
    );
    assert_eq!(
        std::fs::read_to_string(&sentinel).expect("read sentinel"),
        "sentinel content",
        "sentinel file should not be modified"
    );

    // Verify the output is the same as without --fresh
    let output_with_fresh = String::from_utf8(output.stdout).expect("UTF-8 output");

    let output_without_fresh = run_list_config_env_vars_plain("iggy-server", &[]);

    let output_without_fresh =
        String::from_utf8(output_without_fresh.stdout).expect("UTF-8 output");
    assert_eq!(
        output_with_fresh, output_without_fresh,
        "output should be identical with and without --fresh"
    );
}
