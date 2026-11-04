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

use clap::{Args, Subcommand, ValueEnum};
use iggy::prelude::{
    IggyDuration, IggyTimestamp, MetadataChangeCursor, MetadataChangeGroup, MetadataChangeStart,
    MetadataChangesQuery,
};

#[derive(Debug, Clone, Subcommand)]
pub(crate) enum MetadataAction {
    /// read the metadata change log
    ///
    /// Prints every answered page as one JSON object per line: the change
    /// records or current state items, the status and the cursor to continue
    /// from. Requires root or the global manage_servers permission.
    ///
    /// Examples
    ///  iggy metadata changes
    ///  iggy metadata changes --from latest
    ///  iggy metadata changes --after mc1_... --groups streams,topics
    ///  iggy metadata changes --from current --groups streams,topics --follow
    #[clap(verbatim_doc_comment, visible_alias = "c")]
    Changes(MetadataChangesArgs),
}

#[derive(Debug, Clone, Args)]
pub(crate) struct MetadataChangesArgs {
    /// Where to start reading
    ///
    /// earliest: the oldest operation the log still holds completely,
    /// latest: no records, only the cursor at the newest record,
    /// current: the current state, page by page, then its catch-up cursor.
    #[arg(verbatim_doc_comment, long, value_enum, default_value_t = MetadataChangesFrom::Earliest, conflicts_with = "after")]
    pub(crate) from: MetadataChangesFrom,

    /// Continue after this cursor, the `next` value of an earlier page
    #[arg(long)]
    pub(crate) after: Option<MetadataChangeCursor>,

    /// Groups to read, comma separated, every group when omitted
    ///
    /// streams, topics, partitions, consumer_groups, consumer_group_members,
    /// users, personal_access_tokens, sessions
    #[arg(verbatim_doc_comment, long, value_delimiter = ',')]
    pub(crate) groups: Vec<MetadataChangeGroup>,

    /// Oldest prepare timestamp to include, in microseconds since the epoch
    #[arg(long, value_parser = parse_micros)]
    pub(crate) since: Option<IggyTimestamp>,

    /// Newest prepare timestamp to include, in microseconds since the epoch
    #[arg(long, value_parser = parse_micros)]
    pub(crate) until: Option<IggyTimestamp>,

    /// Records or items per page, 1 to 1000
    #[arg(short, long, default_value_t = 100)]
    pub(crate) limit: u32,

    /// Keep reading new changes once the log has no more
    ///
    /// When the cursor's history is no longer retained, the command rebuilds
    /// from the current state and goes on following.
    #[arg(verbatim_doc_comment, short, long, default_value_t = false)]
    pub(crate) follow: bool,

    /// Pause between pages while following
    #[arg(long, default_value = "1s")]
    pub(crate) interval: IggyDuration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum MetadataChangesFrom {
    Earliest,
    Latest,
    Current,
}

impl From<&MetadataChangesArgs> for MetadataChangesQuery {
    fn from(args: &MetadataChangesArgs) -> Self {
        let start = match (args.after, args.from) {
            (Some(cursor), _) => MetadataChangeStart::After(cursor),
            (None, MetadataChangesFrom::Earliest) => MetadataChangeStart::Earliest,
            (None, MetadataChangesFrom::Latest) => MetadataChangeStart::Latest,
            (None, MetadataChangesFrom::Current) => MetadataChangeStart::Current(None),
        };
        Self {
            start,
            groups: args.groups.clone(),
            since: args.since,
            until: args.until,
            limit: args.limit,
        }
    }
}

fn parse_micros(raw: &str) -> Result<IggyTimestamp, String> {
    let micros: u64 = raw
        .parse()
        .map_err(|error| format!("expected microseconds since the epoch, got {raw}: {error}"))?;
    IggyTimestamp::checked_from_micros(micros)
        .ok_or_else(|| format!("{micros} microseconds is past what this platform's clock holds"))
}
