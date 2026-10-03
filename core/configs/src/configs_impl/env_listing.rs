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

/// Writes each name in `names`, sorted and deduplicated, to the provided writer — one
/// call, one error policy. A closed writer (e.g. `iggy-server --list-config-env-vars | head -1`)
/// is expected, not a failure: on `BrokenPipe` this stops writing and returns `Ok(())`. Any
/// other I/O error is real and is propagated so the caller exits non-zero.
pub fn print_env_var_names<I, S, W>(names: I, writer: &mut W) -> std::io::Result<()>
where
    I: IntoIterator<Item = S>,
    S: Into<String>,
    W: std::io::Write,
{
    let mut names: Vec<String> = names.into_iter().map(Into::into).collect();
    names.sort_unstable();
    names.dedup();

    for name in names {
        if let Err(err) = writeln!(writer, "{name}") {
            return if err.kind() == std::io::ErrorKind::BrokenPipe {
                Ok(())
            } else {
                Err(err)
            };
        }
    }
    Ok(())
}

/// Env vars `iggy-mcp --list-config-env-vars` advertises beyond the derived
/// `McpServerConfig` templates.
pub const MCP_CONFIG_PATH_ENV: &str = "IGGY_MCP_CONFIG_PATH";
pub const MCP_ENV_PATH_ENV: &str = "IGGY_MCP_ENV_PATH";
pub const MCP_RUNTIME_ENV_VARS: &[&str] =
    &["IGGY_DISPLAY_CONFIG", MCP_CONFIG_PATH_ENV, MCP_ENV_PATH_ENV];

/// Env vars `iggy-connectors --list-config-env-vars` advertises beyond the
/// derived `ConnectorsRuntimeConfig` templates.
pub const CONNECTORS_CONFIG_PATH_ENV: &str = "IGGY_CONNECTORS_CONFIG_PATH";
pub const CONNECTORS_ENV_PATH_ENV: &str = "IGGY_CONNECTORS_ENV_PATH";
pub const CONNECTORS_RUNTIME_ENV_VARS: &[&str] = &[
    CONNECTORS_CONFIG_PATH_ENV,
    CONNECTORS_ENV_PATH_ENV,
    "IGGY_DISPLAY_CONFIG",
];
