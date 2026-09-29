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
