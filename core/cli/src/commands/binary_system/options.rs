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

use crate::commands::cli_command::{CliCommand, PRINT_TARGET};
use anyhow::Context;
use async_trait::async_trait;
use comfy_table::Table;
use iggy_common::{Client, HeaderValue, OptionSpec, OptionsScope};
use tracing::{Level, event};

/// `iggy options <scope>`: print the server's option catalog.
///
/// This is the discovery surface for `--set` and `--reset`. A key outside the
/// catalog is refused, and the binary transports carry only the error code
/// back, so without this command a CLI user has no way to see which keys their
/// server knows or what a client may do with each one.
pub struct DescribeOptionsCmd {
    scope: OptionsScope,
}

impl DescribeOptionsCmd {
    pub fn new(scope: OptionsScope) -> Self {
        Self { scope }
    }
}

#[async_trait]
impl CliCommand for DescribeOptionsCmd {
    fn explain(&self) -> String {
        format!("describe {} options", self.scope)
    }

    async fn execute_cmd(&mut self, client: &dyn Client) -> anyhow::Result<(), anyhow::Error> {
        let specs = client
            .describe_options(self.scope)
            .await
            .with_context(|| "Problem sending describe_options command".to_owned())?;

        if specs.is_empty() {
            event!(target: PRINT_TARGET, Level::INFO,
                "{} resources accept no options", self.scope);
            return Ok(());
        }

        let mut table = Table::new();
        table.set_header(vec!["Key", "Kind", "Default", "Mutability", "Description"]);
        for spec in &specs {
            table.add_row(vec![
                spec.key.as_str(),
                spec.kind.to_string().as_str(),
                render_default(spec).as_str(),
                render_mutability(spec).as_str(),
                spec.description.as_str(),
            ]);
        }
        event!(target: PRINT_TARGET, Level::INFO, "{table}");

        Ok(())
    }
}

/// What a client may do with the key after the resource exists.
fn render_mutability(spec: &OptionSpec) -> String {
    let mutability = if spec.read_only {
        "read only"
    } else if spec.updatable && spec.resettable {
        "updatable, resettable"
    } else if spec.updatable {
        "updatable"
    } else {
        "create only"
    };
    let flags = [(spec.secret, "secret"), (spec.critical, "critical")];
    flags
        .into_iter()
        .filter(|(set, _)| *set)
        .fold(mutability.to_owned(), |text, (_, flag)| {
            format!("{text}, {flag}")
        })
}

/// The catalog's default in the string form `--set` takes, so the printed value
/// can be handed straight back to a create.
fn render_default(spec: &OptionSpec) -> String {
    if spec.default_value.is_empty() {
        return String::new();
    }
    match HeaderValue::from_raw(spec.kind, &spec.default_value) {
        Ok(value) => value.to_string_value(),
        // A kind this build cannot name still has a readable byte count.
        Err(_) => format!("<{} bytes>", spec.default_value.len()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_common::HeaderKind;

    #[test]
    fn given_critical_spec_when_rendering_mutability_should_print_critical() {
        let spec = OptionSpec {
            key: "critical_keys".to_owned(),
            kind: HeaderKind::String,
            updatable: false,
            resettable: false,
            secret: false,
            read_only: true,
            critical: true,
            default_value: Vec::new(),
            description: String::new(),
        };

        assert_eq!(render_mutability(&spec), "read only, critical");
    }
}
