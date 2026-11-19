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

use std::io::{self, Write};

use anyhow::{Error, Result};
use async_trait::async_trait;
use iggy::clients::client::IggyClient;
use iggy_common::Client;
use serde::Serialize;

pub static PRINT_TARGET: &str = "iggy::cli::output";

/// Diagnostics that must not land on [`PRINT_TARGET`]: that channel is the
/// command's result, and callers pipe it. Routed to stderr instead, so a
/// degraded-but-working run (no keyring, for one) stays visible without
/// corrupting parseable output.
pub static DIAGNOSTIC_TARGET: &str = "iggy::cli::diagnostic";

#[async_trait]
/// `main` awaits `execute_with_iggy_client` through `Box<dyn CliCommand>`, and the
/// `async_trait` future of that default method needs `Self: Send`.
pub trait CliCommand: Send {
    fn explain(&self) -> String;
    fn use_tracing(&self) -> bool {
        true
    }
    /// Whether stdout carries only the command's data, so the banner goes to stderr.
    fn stdout_is_data(&self) -> bool {
        false
    }
    fn login_required(&self) -> bool {
        true
    }
    fn connection_required(&self) -> bool {
        true
    }
    /// Whether explicitly supplied credentials take precedence over a cached
    /// login-session token. `iggy login` (re)establishes that session, so it
    /// must authenticate with the provided username/password and can recreate
    /// the session even after the cached token expired. Every other command
    /// keeps reusing the cached token so users need not re-enter credentials.
    fn prefer_explicit_credentials(&self) -> bool {
        false
    }
    async fn execute_cmd(&mut self, client: &dyn Client) -> Result<(), Error>;
    async fn execute_with_iggy_client(&mut self, client: &IggyClient) -> Result<(), Error> {
        self.execute_cmd(client).await
    }
}

/// Print `value` as pretty JSON to stdout.
///
/// Bypasses [`PRINT_TARGET`] on purpose: JSON was asked for explicitly, so
/// `--quiet` must not hide it.
pub(crate) fn print_json<T: Serialize + ?Sized>(value: &T) -> Result<()> {
    let stdout = io::stdout();
    writeln!(stdout.lock(), "{}", serde_json::to_string_pretty(value)?)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::print_json;

    #[test]
    fn given_non_string_map_keys_when_printing_json_should_return_serialization_error() {
        let value = BTreeMap::from([([1, 2], "value")]);

        let error = print_json(&value).unwrap_err();

        assert!(error.is::<serde_json::Error>(), "{error:#}");
    }
}
