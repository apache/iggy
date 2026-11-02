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

use crate::args::common::{ListMode, parse_key_value, split_reset};
use clap::{Args, Subcommand};
use iggy::prelude::{Identifier, StreamUpdateOptions};
use std::path::PathBuf;

#[derive(Debug, Clone, Subcommand)]
pub(crate) enum StreamAction {
    /// Create stream with given name
    ///
    /// The server assigns the stream ID. The legacy --stream-id flag is ignored.
    ///
    /// Examples:
    ///  iggy stream create prod
    ///  iggy stream create -s 1 test
    ///  iggy stream create --stream-key-file stream.key payments
    #[clap(verbatim_doc_comment, visible_alias = "c")]
    Create(StreamCreateArgs),
    /// Delete stream with given ID
    ///
    /// Stream ID can be specified as a stream name or ID
    ///
    /// Examples:
    ///  iggy stream delete 1
    ///  iggy stream delete test
    #[clap(verbatim_doc_comment, visible_alias = "d")]
    Delete(StreamDeleteArgs),
    /// Update stream name or options for given stream ID
    ///
    /// Stream ID can be specified as a stream name or ID
    /// Only the settings passed change, every other setting keeps its value.
    ///
    /// Examples:
    ///  iggy stream update 1 --name production
    ///  iggy stream update test --name development
    #[clap(verbatim_doc_comment, visible_alias = "u")]
    Update(StreamUpdateArgs),
    /// Get details of a single stream with given ID
    ///
    /// Stream ID can be specified as a stream name or ID
    ///
    /// Examples:
    ///  iggy stream get 1
    ///  iggy stream get test
    #[clap(verbatim_doc_comment, visible_alias = "g")]
    Get(StreamGetArgs),
    /// List all streams
    ///
    /// Examples:
    ///  iggy stream list
    ///  iggy stream list --list-mode table
    ///  iggy stream list -l table
    #[clap(verbatim_doc_comment, visible_alias = "l")]
    List(StreamListArgs),
}

#[derive(Debug, Clone, Args)]
pub(crate) struct StreamCreateArgs {
    /// Legacy stream ID flag (ignored)
    #[clap(short, long)]
    pub(crate) stream_id: Option<u32>,
    /// File holding the stream's own at-rest encryption key
    ///
    /// Standard base64 of exactly 32 random bytes from a secure source, such
    /// as "head -c 32 /dev/urandom | base64". Use a new key for every stream.
    /// Without this flag the server encryption setting applies. The server
    /// stores the key wrapped and reads report only its fingerprint. Unrelated
    /// to --encryption-key, which encrypts message payloads on the client.
    #[clap(long, value_name = "PATH", verbatim_doc_comment)]
    pub(crate) stream_key_file: Option<PathBuf>,
    /// Name of the stream
    pub(crate) name: String,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct StreamDeleteArgs {
    /// Stream ID to delete
    ///
    /// Stream ID can be specified as a stream name or ID
    pub(crate) stream_id: Identifier,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct StreamUpdateArgs {
    /// Stream ID to update
    ///
    /// Stream ID can be specified as a stream name or ID
    pub(crate) stream_id: Identifier,
    /// New name for the stream
    #[arg(short, long)]
    pub(crate) name: Option<String>,
    /// Updatable stream option as key=value, repeatable
    ///
    /// Values are sent as strings and parsed server-side.
    /// Run "iggy options stream" to list the keys and which ones can change.
    #[arg(long = "set", value_name = "KEY=VALUE", value_parser = parse_key_value, verbatim_doc_comment)]
    pub(crate) set: Vec<(String, String)>,
    /// Setting to return to its default, repeatable
    ///
    /// The server refuses a key that cannot be reset.
    #[arg(short, long, verbatim_doc_comment)]
    pub(crate) reset: Vec<String>,
}

impl From<&StreamUpdateArgs> for StreamUpdateOptions {
    fn from(args: &StreamUpdateArgs) -> Self {
        let (reset, raw_reset) = split_reset(&args.reset);
        StreamUpdateOptions {
            name: args.name.clone(),
            reset,
            raw_reset,
            raw: args.set.iter().cloned().collect(),
        }
    }
}

#[derive(Debug, Clone, Args)]
pub(crate) struct StreamGetArgs {
    /// Stream ID to get
    ///
    /// Stream ID can be specified as a stream name or ID
    pub(crate) stream_id: Identifier,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct StreamListArgs {
    /// List mode (table or list)
    #[clap(short, long, value_enum, default_value_t = ListMode::Table)]
    pub(crate) list_mode: ListMode,
}

#[cfg(test)]
mod tests {
    use super::StreamUpdateArgs;
    use clap::Parser;
    use iggy::prelude::StreamUpdateOptions;
    use std::collections::{BTreeMap, BTreeSet};

    #[derive(Parser)]
    struct Update {
        #[command(flatten)]
        args: StreamUpdateArgs,
    }

    #[test]
    fn given_stream_update_without_name_when_parsing_should_accept_reset_and_set() {
        let parsed = Update::try_parse_from([
            "iggy",
            "orders",
            "--reset",
            "future_key",
            "--set",
            "newer_key=1",
        ])
        .expect("an update without a name parses");

        assert_eq!(
            StreamUpdateOptions::from(&parsed.args),
            StreamUpdateOptions {
                raw_reset: BTreeSet::from(["future_key".to_owned()]),
                raw: BTreeMap::from([("newer_key".to_owned(), "1".to_owned())]),
                ..StreamUpdateOptions::default()
            }
        );
    }
}
