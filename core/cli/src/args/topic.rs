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
use iggy::prelude::{
    CompressionAlgorithm, Identifier, IggyExpiry, MaxTopicSize, TopicUpdateOptions,
};

#[derive(Debug, Clone, Subcommand)]
pub(crate) enum TopicAction {
    /// Create topic with given name, number of partitions, compression algorithm and expiry time for given stream ID
    ///
    /// Stream ID can be specified as a stream name or ID
    /// The server assigns the topic ID. The legacy --topic-id flag is ignored.
    ///
    /// Examples
    ///  iggy topic create 1 sensor1 2 gzip 15days
    ///  iggy topic create prod sensor2 2 none
    ///  iggy topic create test debugs 2 gzip 1day 1hour 1min 1sec
    ///  iggy topic create -t 3 1 sensor3 2 none unlimited
    #[clap(verbatim_doc_comment, visible_alias = "c")]
    Create(TopicCreateArgs),
    /// Delete topic with given ID in given stream ID
    ///
    /// Stream ID can be specified as a stream name or ID
    /// Topic ID can be specified as a topic name or ID
    ///
    /// Examples
    ///  iggy topic delete 1 1
    ///  iggy topic delete prod 2
    ///  iggy topic delete test debugs
    ///  iggy topic delete 2 debugs
    #[clap(verbatim_doc_comment, visible_alias = "d")]
    Delete(TopicDeleteArgs),
    /// Update topic name, message expiry or max topic size for given topic ID in given stream ID
    ///
    /// Stream ID can be specified as a stream name or ID
    /// Topic ID can be specified as a topic name or ID
    /// Only the settings passed change, every other setting keeps its value.
    /// Compression, segment size, durability and the other create-time settings cannot change.
    ///
    /// Examples
    ///  iggy topic update 1 1 --name sensor3
    ///  iggy topic update prod sensor3 --name old-sensor --message-expiry 15days
    ///  iggy topic update test debugs --max-topic-size 15GB
    ///  iggy topic update 1 2 --message-expiry 1day 1hour 1min 1sec
    ///  iggy topic update 1 2 --reset message_expiry --reset max_topic_size
    #[clap(verbatim_doc_comment, visible_alias = "u")]
    Update(TopicUpdateArgs),
    /// Get topic detail for given topic ID and stream ID
    ///
    /// Stream ID can be specified as a stream name or ID
    /// Topic ID can be specified as a topic name or ID
    ///
    /// Examples
    ///  iggy topic get 1 1
    ///  iggy topic get prod 2
    ///  iggy topic get test debugs
    ///  iggy topic get 2 debugs
    #[clap(verbatim_doc_comment, visible_alias = "g")]
    Get(TopicGetArgs),
    /// List all topics in given stream ID
    ///
    /// Stream ID can be specified as a stream name or ID
    ///
    /// Examples
    ///  iggy topic list 1
    ///  iggy topic list prod
    #[clap(verbatim_doc_comment, visible_alias = "l")]
    List(TopicListArgs),
}

#[derive(Debug, Clone, Args)]
pub(crate) struct TopicCreateArgs {
    /// Stream ID to create topic
    ///
    /// Stream ID can be specified as a stream name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) stream_id: Identifier,
    /// Name of the topic
    pub(crate) name: String,
    /// Legacy topic ID flag (ignored)
    #[clap(short, long)]
    pub(crate) topic_id: Option<u32>,
    /// Number of partitions inside the topic
    pub(crate) partitions_count: u32,
    /// Compression metadata (none or gzip). Payload compression is not implemented
    #[arg(value_parser = clap::value_parser!(CompressionAlgorithm), verbatim_doc_comment)]
    pub(crate) compression_algorithm: CompressionAlgorithm,
    /// Max topic size in human-readable format like "unlimited" or "15GB"
    ///
    /// Skipping this parameter or using "server_default" creates a topic with unlimited size.
    /// A finite size cannot be lower than the topic segment size.
    #[arg(short, long, default_value = "server_default", verbatim_doc_comment)]
    pub(crate) max_topic_size: MaxTopicSize,
    /// Message expiry time in human-readable format like "unlimited" or "15days 2min 2s"
    ///
    /// Skipping this parameter or using "server_default" creates a topic with no message expiry.
    #[arg(default_value = "server_default", value_parser = clap::value_parser!(IggyExpiry), verbatim_doc_comment)]
    pub(crate) message_expiry: Vec<IggyExpiry>,
    /// Message completion policy: replicated or persisted. Both policies store messages on disk.
    #[arg(long, default_value_t = iggy_common::Durability::Replicated)]
    pub(crate) durability: iggy_common::Durability,
    /// Offset completion policy: replicated or persisted. Independent of message durability.
    #[arg(long, default_value_t = iggy_common::Durability::Replicated)]
    pub(crate) consumer_offset_durability: iggy_common::Durability,
    /// Additional topic option as key=value, repeatable
    ///
    /// Values are sent as strings and parsed server-side through each option's
    /// own FromStr (e.g. --set segment_size=128MiB). The server rejects keys it
    /// does not support; run "iggy options topic" to list the ones it accepts.
    #[arg(long = "set", value_name = "KEY=VALUE", value_parser = parse_key_value, verbatim_doc_comment)]
    pub(crate) set: Vec<(String, String)>,
}

/// Parse one `--set key=value` occurrence.
#[derive(Debug, Clone, Args)]
pub(crate) struct TopicDeleteArgs {
    /// Stream ID to delete topic
    ///
    /// Stream ID can be specified as a stream name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) stream_id: Identifier,
    /// Topic ID to delete
    ///
    /// Topic ID can be specified as a topic name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) topic_id: Identifier,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct TopicUpdateArgs {
    /// Stream ID to update topic
    ///
    /// Stream ID can be specified as a stream name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) stream_id: Identifier,
    /// Topic ID to update
    ///
    /// Topic ID can be specified as a topic name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) topic_id: Identifier,
    /// New name for the topic
    #[arg(short, long)]
    pub(crate) name: Option<String>,
    /// New max topic size in human-readable format like "unlimited" or "15GB"
    ///
    /// A finite size cannot be lower than the topic segment size.
    #[arg(short, long, verbatim_doc_comment)]
    pub(crate) max_topic_size: Option<MaxTopicSize>,
    /// New message expiry time in human-readable format like "unlimited" or "15days 2min 2s"
    #[arg(short = 'e', long, num_args = 1.., value_parser = clap::value_parser!(IggyExpiry), verbatim_doc_comment)]
    pub(crate) message_expiry: Option<Vec<IggyExpiry>>,
    /// Updatable topic option as key=value, repeatable
    ///
    /// Values are sent as strings and parsed server-side.
    /// Run "iggy options topic" to list the keys and which ones can change.
    #[arg(long = "set", value_name = "KEY=VALUE", value_parser = parse_key_value, verbatim_doc_comment)]
    pub(crate) set: Vec<(String, String)>,
    /// Setting to return to its default, like message_expiry or max_topic_size
    ///
    /// Repeat the flag to reset several settings.
    /// The server refuses a key that cannot be reset.
    #[arg(short, long, verbatim_doc_comment)]
    pub(crate) reset: Vec<String>,
}

impl From<&TopicUpdateArgs> for TopicUpdateOptions {
    fn from(args: &TopicUpdateArgs) -> Self {
        let (reset, raw_reset) = split_reset(&args.reset);
        TopicUpdateOptions {
            name: args.name.clone(),
            message_expiry: args.message_expiry.clone().map(IggyExpiry::from),
            max_topic_size: args.max_topic_size,
            reset,
            raw_reset,
            raw: args.set.iter().cloned().collect(),
        }
    }
}

#[derive(Debug, Clone, Args)]
pub(crate) struct TopicGetArgs {
    /// Stream ID to get topic
    ///
    /// Stream ID can be specified as a stream name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) stream_id: Identifier,
    /// Topic ID to get
    ///
    /// Topic ID can be specified as a topic name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) topic_id: Identifier,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct TopicListArgs {
    /// Stream ID to list topics
    ///
    /// Stream ID can be specified as a stream name or ID
    #[arg(value_parser = clap::value_parser!(Identifier))]
    pub(crate) stream_id: Identifier,

    /// List mode (table or list)
    #[clap(short, long, value_enum, default_value_t = ListMode::Table)]
    pub(crate) list_mode: ListMode,
}

#[cfg(test)]
mod update_tests {
    use super::TopicUpdateArgs;
    use clap::Parser;
    use iggy::prelude::{OptionKey, TopicUpdateOptions};
    use std::collections::{BTreeMap, BTreeSet};

    #[derive(Parser)]
    struct Update {
        #[command(flatten)]
        args: TopicUpdateArgs,
    }

    #[test]
    fn given_reset_name_newer_than_build_when_parsing_topic_update_should_send_it_raw() {
        let parsed = Update::try_parse_from([
            "iggy",
            "1",
            "2",
            "--reset",
            "message_expiry",
            "--reset",
            "future_key",
            "--set",
            "newer_key=1",
        ])
        .expect("a reset of a newer key parses");

        assert_eq!(
            TopicUpdateOptions::from(&parsed.args),
            TopicUpdateOptions {
                reset: BTreeSet::from([OptionKey::MessageExpiry]),
                raw_reset: BTreeSet::from(["future_key".to_owned()]),
                raw: BTreeMap::from([("newer_key".to_owned(), "1".to_owned())]),
                ..TopicUpdateOptions::default()
            }
        );
    }
}

#[cfg(test)]
mod durability_tests {
    use super::TopicCreateArgs;
    use clap::Parser;
    use iggy_common::Durability;

    #[derive(Parser)]
    struct Create {
        #[command(flatten)]
        args: TopicCreateArgs,
    }

    #[test]
    fn topic_durability_defaults_are_independent() {
        let parsed = Create::try_parse_from([
            "iggy",
            "stream",
            "topic",
            "1",
            "none",
            "--durability",
            "persisted",
        ])
        .unwrap();
        assert_eq!(parsed.args.durability, Durability::Persisted);
        assert_eq!(
            parsed.args.consumer_offset_durability,
            Durability::Replicated
        );
        let parsed = Create::try_parse_from([
            "iggy",
            "stream",
            "topic",
            "1",
            "none",
            "--consumer-offset-durability",
            "persisted",
        ])
        .unwrap();
        assert_eq!(parsed.args.durability, Durability::Replicated);
        assert_eq!(
            parsed.args.consumer_offset_durability,
            Durability::Persisted
        );
    }

    #[test]
    fn unknown_durability_is_rejected() {
        assert!(
            Create::try_parse_from([
                "iggy",
                "stream",
                "topic",
                "1",
                "none",
                "--durability",
                "memory"
            ])
            .is_err()
        );
    }
}
