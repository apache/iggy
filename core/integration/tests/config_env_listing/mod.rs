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

fn list_config_env_vars_in<I, K, V>(
    binary: &str,
    directory: &std::path::Path,
    env: I,
    extra_args: &[&str],
) -> Vec<String>
where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<std::ffi::OsStr>,
    V: AsRef<std::ffi::OsStr>,
{
    let mut cmd = Command::cargo_bin(binary).expect("binary should be built");
    cmd.current_dir(directory)
        .arg("--list-config-env-vars")
        .envs(env)
        .args(extra_args)
        .timeout(LIST_ENV_VARS_TIMEOUT);

    let output = cmd.output().expect("listing command should run");
    assert!(
        output.status.success(),
        "{binary} failed: {:?}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty(), "{binary} wrote to stderr");
    let stdout = String::from_utf8(output.stdout).expect("UTF-8 output");
    let names = stdout.lines().map(str::to_owned).collect::<Vec<_>>();
    assert!(!names.is_empty(), "{binary} returned no variables");
    names
}

fn list_config_env_vars(binary: &str) -> Vec<String> {
    let directory = tempfile::tempdir().expect("temporary directory");
    list_config_env_vars_in(
        binary,
        directory.path(),
        std::iter::empty::<(&str, &str)>(),
        &[],
    )
}

#[test]
fn config_env_listing_exits_before_startup_for_each_binary() {
    for (binary, config_env, dotenv_env, invalid_env) in [
        (
            "iggy-server",
            "IGGY_CONFIG_PATH",
            "IGGY_ENV_PATH",
            "IGGY_HTTP_ADDRESS",
        ),
        (
            "iggy-connectors",
            "IGGY_CONNECTORS_CONFIG_PATH",
            "IGGY_CONNECTORS_ENV_PATH",
            "IGGY_CONNECTORS_HTTP_ADDRESS",
        ),
        (
            "iggy-mcp",
            "IGGY_MCP_CONFIG_PATH",
            "IGGY_MCP_ENV_PATH",
            "IGGY_MCP_HTTP_ADDRESS",
        ),
    ] {
        let directory = tempfile::tempdir().expect("temporary directory");
        let config_path = directory.path().join("invalid-config.toml");
        let dotenv_path = directory.path().join("invalid.env");
        std::fs::write(&config_path, "not valid toml = [").expect("write invalid config");
        std::fs::write(&dotenv_path, "not a dotenv assignment").expect("write invalid dotenv");

        let entries_before: HashSet<_> = std::fs::read_dir(directory.path())
            .expect("read temporary directory")
            .map(|entry| entry.expect("directory entry").file_name())
            .collect();
        let names = list_config_env_vars_in(
            binary,
            directory.path(),
            [
                (config_env, config_path.into_os_string()),
                (dotenv_env, dotenv_path.into_os_string()),
                (invalid_env, "invalid-value".into()),
            ],
            &[],
        );

        let baseline = list_config_env_vars(binary);
        assert_eq!(
            names, baseline,
            "{binary} output changed with invalid config, dotenv or env values"
        );
        let entries_after: HashSet<_> = std::fs::read_dir(directory.path())
            .expect("read temporary directory")
            .map(|entry| entry.expect("directory entry").file_name())
            .collect();
        assert_eq!(
            entries_after, entries_before,
            "{binary} created startup side effects"
        );

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
    let names = list_config_env_vars("iggy-server");
    let names: HashSet<_> = names.iter().map(String::as_str).collect();

    assert!(
        names
            .iter()
            .any(|name| name.contains("IGGY_CLUSTER_NODES_<N>_ADVERTISED_ADDRESSES_<N>_")),
        "server should list nested vector templates"
    );
}

#[test]
fn config_env_listing_includes_connector_templates() {
    let names = list_config_env_vars("iggy-connectors");
    let names: HashSet<_> = names.iter().map(String::as_str).collect();

    assert!(
        names.contains("IGGY_CONNECTORS_SINK_<KEY>_PLUGIN_CONFIG_<FIELD>"),
        "connectors should list SINK plugin config templates"
    );
    assert!(
        names.contains("IGGY_CONNECTORS_SOURCE_<KEY>_PLUGIN_CONFIG_<FIELD>"),
        "connectors should list SOURCE plugin config templates"
    );
    assert!(
        names.contains("IGGY_CONNECTORS_SINK_<KEY>_PLUGIN_CONFIG_FORMAT"),
        "connectors should list the typed SINK plugin config format"
    );
    assert!(
        names.contains("IGGY_CONNECTORS_SOURCE_<KEY>_PLUGIN_CONFIG_FORMAT"),
        "connectors should list the typed SOURCE plugin config format"
    );

    assert!(
        names.contains("IGGY_CONNECTORS_SINK_<KEY>_ENABLED"),
        "connectors should list SINK_<KEY>_ENABLED"
    );
    assert!(
        names.contains("IGGY_CONNECTORS_SOURCE_<KEY>_ENABLED"),
        "connectors should list SOURCE_<KEY>_ENABLED"
    );
    for excluded in [
        "IGGY_CONNECTORS_SINK_<KEY>_KEY",
        "IGGY_CONNECTORS_SINK_<KEY>_VERSION",
        "IGGY_CONNECTORS_SOURCE_<KEY>_KEY",
        "IGGY_CONNECTORS_SOURCE_<KEY>_VERSION",
    ] {
        assert!(!names.contains(excluded), "connectors listed {excluded}");
    }

    assert_eq!(
        names
            .iter()
            .filter(|name| **name == "IGGY_CONNECTORS_CONNECTORS_CONFIG_TYPE")
            .count(),
        1,
        "enum variants should produce one deduplicated tag name"
    );
}

#[test]
fn mcp_rejects_unknown_arguments() {
    let mut command = Command::cargo_bin("iggy-mcp").expect("binary should be built");
    let output = command
        .arg("--unknown")
        .timeout(LIST_ENV_VARS_TIMEOUT)
        .output()
        .expect("MCP command should run");

    assert_eq!(output.status.code(), Some(2));
    assert!(output.stdout.is_empty(), "MCP wrote to stdout");
    assert!(
        String::from_utf8(output.stderr)
            .expect("UTF-8 stderr")
            .contains("unexpected argument '--unknown'"),
        "MCP should explain the rejected argument"
    );
}

#[test]
fn config_env_listing_excludes_other_processes_variables() {
    let server = list_config_env_vars("iggy-server");
    let server_names: HashSet<_> = server.iter().map(String::as_str).collect();
    for excluded in [
        "IGGY_CI_BUILD",
        "IGGY_HOME",
        "IGGY_PASSWORD",
        "IGGY_TEST_CLEANUP_DISABLED",
        "IGGY_TEST_CLUSTER_NODES",
        "IGGY_TEST_VERBOSE",
        "IGGY_USERNAME",
    ] {
        assert!(!server_names.contains(excluded), "server listed {excluded}");
    }
    assert!(
        server_names
            .iter()
            .all(|name| !name.starts_with("IGGY_CONNECTORS_")
                && !name.starts_with("IGGY_KAFKA_")
                && !name.starts_with("IGGY_MCP_")),
        "server listed a sibling binary's variables"
    );

    for (binary, prefix) in [
        ("iggy-connectors", "IGGY_CONNECTORS_"),
        ("iggy-mcp", "IGGY_MCP_"),
    ] {
        let names = list_config_env_vars(binary);
        assert!(
            names
                .iter()
                .all(|name| name == "IGGY_DISPLAY_CONFIG" || name.starts_with(prefix)),
            "{binary} listed a variable outside {prefix}"
        );
    }
}

#[test]
fn config_env_listing_includes_each_process_runtime_variables() {
    for (binary, expected) in [
        (
            "iggy-server",
            &["IGGY_ROOT_PASSWORD", "IGGY_SHARD_RUNTIME_CAPACITY"][..],
        ),
        (
            "iggy-connectors",
            &["IGGY_CONNECTORS_CONFIG_PATH", "IGGY_CONNECTORS_ENV_PATH"][..],
        ),
        (
            "iggy-mcp",
            &["IGGY_MCP_CONFIG_PATH", "IGGY_MCP_ENV_PATH"][..],
        ),
    ] {
        let names = list_config_env_vars(binary);
        for expected_name in expected {
            assert!(
                names.iter().any(|name| name == expected_name),
                "{binary} did not list {expected_name}"
            );
        }
    }
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
    let output_with_fresh = list_config_env_vars_in(
        "iggy-server",
        directory.path(),
        [("IGGY_PATH", data_dir.to_string_lossy().into_owned())],
        &["--fresh"],
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

    let output_without_fresh = list_config_env_vars("iggy-server");
    assert_eq!(
        output_with_fresh, output_without_fresh,
        "output should be identical with and without --fresh"
    );
}
