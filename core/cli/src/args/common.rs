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

use clap::ValueEnum;
use iggy_cli::commands::binary_client::get_clients::GetClientsOutput;
use iggy_cli::commands::binary_cluster::get_cluster_metadata::GetClusterMetadataOutput;
use iggy_cli::commands::binary_consumer_groups::get_consumer_groups::GetConsumerGroupsOutput;
use iggy_cli::commands::binary_context::get_contexts::GetContextsOutput;
use iggy_cli::commands::binary_personal_access_tokens::get_personal_access_tokens::GetPersonalAccessTokensOutput;
use iggy_cli::commands::binary_streams::get_streams::GetStreamsOutput;
use iggy_cli::commands::binary_system::stats::GetStatsOutput;
use iggy_cli::commands::binary_topics::get_topics::GetTopicsOutput;
use iggy_cli::commands::binary_users::get_users::GetUsersOutput;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum ListMode {
    Table,
    List,
}

impl From<ListMode> for GetClusterMetadataOutput {
    fn from(mode: ListMode) -> Self {
        match mode {
            ListMode::Table => GetClusterMetadataOutput::Table,
            ListMode::List => GetClusterMetadataOutput::List,
        }
    }
}

impl From<ListMode> for GetStreamsOutput {
    fn from(mode: ListMode) -> Self {
        match mode {
            ListMode::Table => GetStreamsOutput::Table,
            ListMode::List => GetStreamsOutput::List,
        }
    }
}

impl From<ListMode> for GetTopicsOutput {
    fn from(mode: ListMode) -> Self {
        match mode {
            ListMode::Table => GetTopicsOutput::Table,
            ListMode::List => GetTopicsOutput::List,
        }
    }
}

impl From<ListMode> for GetPersonalAccessTokensOutput {
    fn from(mode: ListMode) -> Self {
        match mode {
            ListMode::Table => GetPersonalAccessTokensOutput::Table,
            ListMode::List => GetPersonalAccessTokensOutput::List,
        }
    }
}

impl From<ListMode> for GetUsersOutput {
    fn from(mode: ListMode) -> Self {
        match mode {
            ListMode::Table => GetUsersOutput::Table,
            ListMode::List => GetUsersOutput::List,
        }
    }
}

impl From<ListMode> for GetClientsOutput {
    fn from(mode: ListMode) -> Self {
        match mode {
            ListMode::Table => GetClientsOutput::Table,
            ListMode::List => GetClientsOutput::List,
        }
    }
}

impl From<ListMode> for GetConsumerGroupsOutput {
    fn from(mode: ListMode) -> Self {
        match mode {
            ListMode::Table => GetConsumerGroupsOutput::Table,
            ListMode::List => GetConsumerGroupsOutput::List,
        }
    }
}

impl From<ListMode> for GetContextsOutput {
    fn from(mode: ListMode) -> Self {
        match mode {
            ListMode::Table => GetContextsOutput::Table,
            ListMode::List => GetContextsOutput::List,
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub(crate) enum ListModeExt {
    Table,
    List,
    Json,
    Toml,
}

impl From<ListModeExt> for GetStatsOutput {
    fn from(mode: ListModeExt) -> Self {
        match mode {
            ListModeExt::Table => GetStatsOutput::Table,
            ListModeExt::List => GetStatsOutput::List,
            ListModeExt::Json => GetStatsOutput::Json,
            ListModeExt::Toml => GetStatsOutput::Toml,
        }
    }
}

/// Longest an option key or value may be, matching the header-field codec the
/// options block rides. Enforced here so `--set` reports the offending pair
/// with clap's own error rather than failing at encode time.
const MAX_OPTION_FIELD_LEN: usize = 255;

/// Parse a repeatable `--set KEY=VALUE` pair.
///
/// Shared by every resource's option args: divergent copies disagreed on
/// whether an empty value was legal, which decided whether a pair reached the
/// encoder at all.
pub(crate) fn parse_key_value(raw: &str) -> Result<(String, String), String> {
    let (key, value) = raw
        .split_once('=')
        .ok_or_else(|| format!("expected KEY=VALUE, got: {raw}"))?;
    for (label, field) in [("key", key), ("value", value)] {
        if field.is_empty() {
            return Err(format!(
                "expected non-empty {label} in KEY=VALUE, got: {raw}"
            ));
        }
        if field.len() > MAX_OPTION_FIELD_LEN {
            return Err(format!(
                "option {label} is {} bytes, maximum {MAX_OPTION_FIELD_LEN}",
                field.len()
            ));
        }
    }
    Ok((key.to_string(), value.to_string()))
}
