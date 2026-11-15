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

use bench_report::{
    benchmark_kind::BenchmarkKind,
    individual_metrics::BenchmarkIndividualMetrics,
    numeric_parameter::BenchmarkNumericParameter,
    options::{
        BenchmarkClientOptions, BenchmarkConsumerOptions, BenchmarkDurability,
        BenchmarkMaxTopicSize, BenchmarkMessageExpiry, BenchmarkOptions, BenchmarkTcpOptions,
        BenchmarkTopicOptions,
    },
    params::{BenchmarkDataVolume, BenchmarkParams},
    transport::BenchmarkTransport,
};
use configs::{ConfigEnvMappings, server::ServerConfig};
use iggy::prelude::*;
use std::{
    fs, io,
    num::NonZeroU32,
    path::{Path, PathBuf},
};
use tracing::{error, info};

use crate::args::{
    common::IggyBenchArgs,
    defaults::{
        DEFAULT_BALANCED_NUMBER_OF_PARTITIONS, DEFAULT_BALANCED_NUMBER_OF_STREAMS,
        DEFAULT_HTTP_SERVER_ADDRESS, DEFAULT_MESSAGE_BATCHES, DEFAULT_MESSAGE_SIZE,
        DEFAULT_MESSAGES_PER_BATCH, DEFAULT_NUMBER_OF_CONSUMER_GROUPS, DEFAULT_NUMBER_OF_CONSUMERS,
        DEFAULT_NUMBER_OF_PRODUCERS, DEFAULT_PINNED_NUMBER_OF_PARTITIONS,
        DEFAULT_PINNED_NUMBER_OF_STREAMS, DEFAULT_QUIC_SERVER_ADDRESS, DEFAULT_TCP_SERVER_ADDRESS,
        DEFAULT_TOTAL_MESSAGES_SIZE, DEFAULT_WARMUP_TIME, DEFAULT_WEBSOCKET_SERVER_ADDRESS,
    },
    transport::BenchmarkTransportCommand,
};

pub use client_factory::ClientFactory;

pub mod batch_generator;
pub mod client_factory;
pub mod command_line;
pub mod cpu_name;
pub mod finish_condition;
pub mod rate_limiter;

/// Runs of one command that start within the same second get the suffixes `_2` up to this.
const MAX_RUN_DIR_ATTEMPTS: u32 = 100;

const BENCHMARK_SERVER_ENV_VARS: &[&str] = &[
    "IGGY_CONFIG_PATH",
    "IGGY_ENV_PATH",
    "IGGY_SHARD_RUNTIME_CAPACITY",
    "IGGY_SHARD_EVENT_INTERVAL",
];

pub fn batch_total_size_bytes(polled_messages: &PolledMessages) -> u64 {
    polled_messages
        .messages
        .iter()
        .map(|m| m.get_size_bytes().as_bytes_u64())
        .sum()
}

pub fn batch_user_size_bytes(polled_messages: &PolledMessages) -> u64 {
    polled_messages
        .messages
        .iter()
        .map(|m| m.payload.len() as u64)
        .sum()
}

pub async fn get_server_stats(client: &IggyClient) -> Result<Stats, IggyError> {
    client.get_stats().await
}

pub async fn collect_server_logs_and_save_to_file(
    client: &IggyClient,
    output_dir: &Path,
) -> Result<(), IggyError> {
    let snapshot = client
        .snapshot(
            SnapshotCompression::Deflated,
            vec![SystemSnapshotType::ServerLogs],
        )
        .await?
        .0;

    fs::write(output_dir.join("server_logs.zip"), snapshot).map_err(|e| {
        error!("Failed to write server logs to file: {:?}", e);
        IggyError::CannotWriteToFile
    })
}

/// Creates the directory for one run. It never reuses an existing directory, so no run replaces
/// another's results.
pub fn create_run_dir(output_dir: &Path, dir_name: &str) -> io::Result<PathBuf> {
    fs::create_dir_all(output_dir)?;
    for attempt in 1..=MAX_RUN_DIR_ATTEMPTS {
        let path = match attempt {
            1 => output_dir.join(dir_name),
            _ => output_dir.join(format!("{dir_name}_{attempt}")),
        };
        match fs::create_dir(&path) {
            Ok(()) => return Ok(path),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        format!("{dir_name} exists with every suffix up to _{MAX_RUN_DIR_ATTEMPTS}"),
    ))
}

fn message_batches_from_metrics(individual_metrics: &[BenchmarkIndividualMetrics]) -> u64 {
    individual_metrics
        .iter()
        .map(|s| s.summary.total_message_batches)
        .sum()
}

pub fn params_from_args_and_metrics(
    args: &IggyBenchArgs,
    metrics: &[BenchmarkIndividualMetrics],
) -> BenchmarkParams {
    let benchmark_kind = args.benchmark_kind.as_simple_kind();

    // Ugly conversion but let it stay here to have `bench-report` not depend on `iggy` or `integration`
    let transport = match args.transport() {
        TransportProtocol::Tcp => BenchmarkTransport::Tcp,
        TransportProtocol::Quic => BenchmarkTransport::Quic,
        TransportProtocol::Http => BenchmarkTransport::Http,
        TransportProtocol::WebSocket => BenchmarkTransport::WebSocket,
    };
    let server_address = args.server_address().to_string();
    let remark = args.remark();
    let extra_info = args.extra_info();
    let gitref = args.gitref();
    let gitref_date = args.gitref_date();
    let messages_per_batch = args.messages_per_batch();
    let message_size = args.message_size();
    let message_batches = message_batches_from_metrics(metrics);
    let producers = args.producers();
    let consumers = args.consumers();
    let streams = args.streams();
    let partitions = args.number_of_partitions();
    let consumer_groups = args.number_of_consumer_groups();
    let rate_limit = args.rate_limit().map(|limit| limit.to_string());
    let pretty_name = args.generate_pretty_name();
    let bench_command = recreate_bench_command(args);

    let remark_for_identifier = remark
        .clone()
        .unwrap_or_else(|| "no_remark".to_string())
        .replace(' ', "_");

    let data_volume_identifier = args.data_volume_identifier();

    let params_identifier = vec![
        benchmark_kind.to_string(),
        transport.to_string(),
        remark_for_identifier,
        messages_per_batch.to_string(),
        data_volume_identifier,
        message_size.to_string(),
        producers.to_string(),
        consumers.to_string(),
        streams.to_string(),
        partitions.to_string(),
        consumer_groups.to_string(),
    ];

    let params_identifier = params_identifier.join("_");

    let data_volume = args
        .total_data()
        .map(|total_data| BenchmarkDataVolume::TotalDataBytes(total_data.as_bytes_u64()))
        .or_else(|| {
            args.message_batches()
                .map(|batches| BenchmarkDataVolume::MessageBatchesPerActor(batches.get()))
        });

    BenchmarkParams {
        benchmark_kind,
        transport,
        server_address,
        remark,
        extra_info,
        gitref,
        gitref_date,
        messages_per_batch,
        message_batches,
        message_size,
        producers,
        consumers,
        streams,
        partitions,
        consumer_groups,
        rate_limit,
        pretty_name,
        bench_command,
        params_identifier,
        options: Some(options_from_args(args)),
        data_volume,
        suite: args.suite(),
        workload: args.workload(),
        command_line: Some(command_line::from_argv(std::env::args_os())),
    }
}

/// Every flag that changes a result, with the bench defaults applied.
pub fn options_from_args(args: &IggyBenchArgs) -> BenchmarkOptions {
    let tcp = match args.transport_command() {
        BenchmarkTransportCommand::Tcp(tcp) => BenchmarkTcpOptions {
            nodelay: tcp.nodelay,
            tls: tcp.tls,
        },
        BenchmarkTransportCommand::Http(_)
        | BenchmarkTransportCommand::Quic(_)
        | BenchmarkTransportCommand::WebSocket(_) => BenchmarkTcpOptions {
            nodelay: false,
            tls: false,
        },
    };
    BenchmarkOptions {
        topic: BenchmarkTopicOptions {
            durability: benchmark_durability(args.durability()),
            messages_required_to_save: args.messages_required_to_save().map(NonZeroU32::get),
            max_topic_size: benchmark_max_topic_size(args.new_topic_max_size()),
            message_expiry: benchmark_message_expiry(args.message_expiry()),
        },
        consumer: args.polls().then(|| BenchmarkConsumerOptions {
            consumer_offset_durability: benchmark_durability(args.consumer_offset_durability()),
            max_wait_us: args.max_wait.as_micros(),
            min_count: args.min_count,
            max_bytes: args.max_bytes,
            read_amplification: args.read_amplification(),
        }),
        client: BenchmarkClientOptions {
            warmup_time_us: args.warmup_time().as_micros(),
            high_level_api: args.high_level_api(),
            reuse_streams: args.reuse_streams(),
        },
        tcp,
    }
}

const fn benchmark_durability(durability: Durability) -> BenchmarkDurability {
    match durability {
        Durability::Replicated => BenchmarkDurability::Replicated,
        Durability::Persisted => BenchmarkDurability::Persisted,
    }
}

/// The server reads the size as a number, where 0 is its default and `u64::MAX` is
/// unlimited. `--max-topic-size 0` and `--max-topic-size unlimited` both send 0.
fn benchmark_max_topic_size(max_topic_size: MaxTopicSize) -> BenchmarkMaxTopicSize {
    match MaxTopicSize::from(u64::from(max_topic_size)) {
        MaxTopicSize::ServerDefault => BenchmarkMaxTopicSize::ServerDefault,
        MaxTopicSize::Unlimited => BenchmarkMaxTopicSize::Unlimited,
        MaxTopicSize::Custom(size) => BenchmarkMaxTopicSize::Bytes(size.as_bytes_u64()),
    }
}

/// The server reads the expiry as microseconds, where 0, sent by `--message-expiry 0s`, is
/// its default.
fn benchmark_message_expiry(message_expiry: IggyExpiry) -> BenchmarkMessageExpiry {
    match IggyExpiry::from(u64::from(message_expiry)) {
        IggyExpiry::ServerDefault => BenchmarkMessageExpiry::ServerDefault,
        IggyExpiry::NeverExpire => BenchmarkMessageExpiry::NeverExpire,
        IggyExpiry::ExpireDuration(duration) => {
            BenchmarkMessageExpiry::AfterUs(duration.as_micros())
        }
    }
}

fn recreate_bench_command(args: &IggyBenchArgs) -> String {
    let mut parts = Vec::new();

    add_environment_variables(&mut parts, args.server_address());
    parts.push("iggy-bench".to_string());

    add_basic_arguments(&mut parts, args);
    add_benchmark_kind_arguments(&mut parts, args);
    add_infrastructure_arguments(&mut parts, args);
    add_output_arguments(&mut parts, args);

    parts.join(" ")
}

fn add_environment_variables(parts: &mut Vec<String>, server_address: &str) {
    let is_localhost = server_address
        .split(':')
        .next()
        .is_some_and(|host| host == "localhost" || host == "127.0.0.1");

    if is_localhost {
        let iggy_vars: Vec<_> = std::env::vars()
            .filter(|(name, _)| {
                BENCHMARK_SERVER_ENV_VARS.contains(&name.as_str())
                    || ServerConfig::find_by_env_name(name)
                        .is_some_and(|mapping| !mapping.is_secret)
            })
            .collect();

        if !iggy_vars.is_empty() {
            info!("Found env vars starting with IGGY_: {:?}", iggy_vars);
            parts.extend(iggy_vars.into_iter().map(|(k, v)| format!("{k}={v}")));
        }
    }
}

fn add_basic_arguments(parts: &mut Vec<String>, args: &IggyBenchArgs) {
    parts.push(format!("--durability {}", args.durability()));
    parts.push(format!(
        "--consumer-offset-durability {}",
        args.consumer_offset_durability()
    ));
    if let Some(threshold) = args.messages_required_to_save() {
        parts.push(format!("--messages-required-to-save {threshold}"));
    }

    let messages_per_batch = args.messages_per_batch();
    if messages_per_batch != BenchmarkNumericParameter::Value(DEFAULT_MESSAGES_PER_BATCH.get()) {
        parts.push(format!("--messages-per-batch {messages_per_batch}"));
    }

    if let Some(message_batches) = args.message_batches()
        && message_batches != DEFAULT_MESSAGE_BATCHES
    {
        parts.push(format!("--message-batches {message_batches}"));
    }

    if let Some(total_messages_size) = args.total_data()
        && total_messages_size != DEFAULT_TOTAL_MESSAGES_SIZE
    {
        parts.push(format!("--total-data {total_messages_size}"));
    }

    let message_size = args.message_size();
    if message_size != BenchmarkNumericParameter::Value(DEFAULT_MESSAGE_SIZE.get()) {
        parts.push(format!("--message-size {message_size}"));
    }

    if let Some(rate_limit) = args.rate_limit() {
        parts.push(format!("--rate-limit \'{rate_limit}\'"));
    }

    if args.warmup_time().to_string() != DEFAULT_WARMUP_TIME {
        parts.push(format!("--warmup-time \'{}\'", args.warmup_time()));
    }

    let poll_defaults = PollOptions::wait(IggyDuration::ONE_SECOND);
    if args.max_wait != poll_defaults.max_wait {
        parts.push(format!("--max-wait \'{}\'", args.max_wait));
    }
    if args.min_count != poll_defaults.min_count {
        parts.push(format!("--min-count {}", args.min_count));
    }
    if let Some(max_bytes) = args.max_bytes {
        parts.push(format!("--max-bytes {max_bytes}"));
    }
}

fn add_benchmark_kind_arguments(parts: &mut Vec<String>, args: &IggyBenchArgs) {
    let kind_str = match args.benchmark_kind.as_simple_kind() {
        BenchmarkKind::PinnedProducer => "pinned-producer",
        BenchmarkKind::PinnedConsumer => "pinned-consumer",
        BenchmarkKind::PinnedProducerAndConsumer => "pinned-producer-and-consumer",
        BenchmarkKind::BalancedProducer => "balanced-producer",
        BenchmarkKind::BalancedConsumerGroup => "balanced-consumer-group",
        BenchmarkKind::BalancedProducerAndConsumerGroup => "balanced-producer-and-consumer-group",
        BenchmarkKind::EndToEndProducingConsumer => "end-to-end-producing-consumer",
        BenchmarkKind::EndToEndProducingConsumerGroup => "end-to-end-producing-consumer-group",
    };
    parts.push(kind_str.to_string());

    add_actor_arguments(parts, args);
}

fn add_actor_arguments(parts: &mut Vec<String>, args: &IggyBenchArgs) {
    let producers = args.producers();
    let consumers = args.consumers();
    let number_of_consumer_groups = args.number_of_consumer_groups();

    match args.benchmark_kind.as_simple_kind() {
        BenchmarkKind::PinnedProducer
        | BenchmarkKind::BalancedProducer
        | BenchmarkKind::EndToEndProducingConsumer => {
            if producers != DEFAULT_NUMBER_OF_PRODUCERS.get() {
                parts.push(format!("--producers {producers}"));
            }
        }
        BenchmarkKind::PinnedConsumer | BenchmarkKind::BalancedConsumerGroup => {
            if consumers != DEFAULT_NUMBER_OF_CONSUMERS.get() {
                parts.push(format!("--consumers {consumers}"));
            }
        }
        BenchmarkKind::PinnedProducerAndConsumer
        | BenchmarkKind::BalancedProducerAndConsumerGroup => {
            if producers != DEFAULT_NUMBER_OF_PRODUCERS.get() {
                parts.push(format!("--producers {producers}"));
            }
            if consumers != DEFAULT_NUMBER_OF_CONSUMERS.get() {
                parts.push(format!("--consumers {consumers}"));
            }
        }
        BenchmarkKind::EndToEndProducingConsumerGroup => {
            if producers != DEFAULT_NUMBER_OF_PRODUCERS.get() {
                parts.push(format!("--producers {producers}"));
            }
            if consumers != DEFAULT_NUMBER_OF_CONSUMERS.get() {
                parts.push(format!("--consumers {consumers}"));
            }
            if number_of_consumer_groups != DEFAULT_NUMBER_OF_CONSUMER_GROUPS.get() {
                parts.push(format!("--consumer-groups {number_of_consumer_groups}"));
            }
        }
    }
}

fn add_infrastructure_arguments(parts: &mut Vec<String>, args: &IggyBenchArgs) {
    let streams = args.streams();
    let default_streams = match args.benchmark_kind.as_simple_kind() {
        BenchmarkKind::BalancedProducerAndConsumerGroup
        | BenchmarkKind::BalancedConsumerGroup
        | BenchmarkKind::BalancedProducer
        | BenchmarkKind::EndToEndProducingConsumerGroup => DEFAULT_BALANCED_NUMBER_OF_STREAMS.get(),
        _ => DEFAULT_PINNED_NUMBER_OF_STREAMS.get(),
    };
    if streams != default_streams {
        parts.push(format!("--streams {streams}"));
    }

    let partitions = args.number_of_partitions();
    let default_partitions = match args.benchmark_kind.as_simple_kind() {
        BenchmarkKind::BalancedProducerAndConsumerGroup
        | BenchmarkKind::BalancedConsumerGroup
        | BenchmarkKind::BalancedProducer
        | BenchmarkKind::EndToEndProducingConsumerGroup => {
            DEFAULT_BALANCED_NUMBER_OF_PARTITIONS.get()
        }
        _ => DEFAULT_PINNED_NUMBER_OF_PARTITIONS.get(),
    };
    if partitions != 0 && partitions != default_partitions {
        parts.push(format!("--partitions {partitions}"));
    }

    let consumer_groups = args.number_of_consumer_groups();
    if (args.benchmark_kind.as_simple_kind() == BenchmarkKind::BalancedConsumerGroup
        || args.benchmark_kind.as_simple_kind() == BenchmarkKind::BalancedProducerAndConsumerGroup)
        && consumer_groups != DEFAULT_NUMBER_OF_CONSUMER_GROUPS.get()
    {
        parts.push(format!("--consumer-groups {consumer_groups}"));
    }

    if let Some(max_topic_size) = args.max_topic_size() {
        parts.push(format!("--max-topic-size \'{max_topic_size}\'"));
    }

    let transport = args.transport_command().as_str();
    parts.push(transport.to_owned());

    let server_address = args.server_address();
    let default_address = match transport {
        "tcp" => DEFAULT_TCP_SERVER_ADDRESS,
        "quic" => DEFAULT_QUIC_SERVER_ADDRESS,
        "http" => DEFAULT_HTTP_SERVER_ADDRESS,
        "websocket" => DEFAULT_WEBSOCKET_SERVER_ADDRESS,
        _ => "",
    };

    if server_address != default_address {
        parts.push(format!("--server-address {server_address}"));
    }
}

fn add_output_arguments(parts: &mut Vec<String>, args: &IggyBenchArgs) {
    parts.push("output".to_string());
    parts.push("-o performance_results".to_string());

    if let Some(remark) = args.remark() {
        parts.push(format!("--remark \'{remark}\'"));
    }
}

#[cfg(test)]
mod tests {
    use super::{MAX_RUN_DIR_ATTEMPTS, create_run_dir, options_from_args, recreate_bench_command};
    use crate::args::common::IggyBenchArgs;
    use bench_report::options::{
        BenchmarkClientOptions, BenchmarkConsumerOptions, BenchmarkDurability,
        BenchmarkMaxTopicSize, BenchmarkMessageExpiry, BenchmarkOptions, BenchmarkTcpOptions,
        BenchmarkTopicOptions,
    };
    use clap::{Command, CommandFactory, Parser};
    use std::io;

    /// Args that the report holds as typed params.
    const TYPED_PARAM_ARGS: &[&str] = &[
        "message_size",
        "messages_per_batch",
        "message_batches",
        "total_data",
        "rate_limit",
        "streams",
        "partitions",
        "producers",
        "producing_consumers",
        "consumers",
        "consumer_groups",
    ];

    /// Args that `options_from_args` records.
    const RECORDED_OPTION_ARGS: &[&str] = &[
        "durability",
        "messages_required_to_save",
        "max_topic_size",
        "message_expiry",
        "consumer_offset_durability",
        "max_wait",
        "min_count",
        "max_bytes",
        "read_amplification",
        "warmup_time",
        "high_level_api",
        "reuse_streams",
        "nodelay",
        "tls",
    ];

    /// Args that don't change a result: addresses, certificates, credentials, output and labels.
    const NEUTRAL_ARGS: &[&str] = &[
        "server_address",
        "client_address",
        "server_name",
        "validate_certificate",
        "tls_domain",
        "tls_validate_certificate",
        "tls_ca_file",
        "username",
        "password",
        "output_dir",
        "identifier",
        "remark",
        "extra_info",
        "gitref",
        "gitref_date",
        "open_charts",
        "suite",
        "workload",
        "sampling_time",
        "moving_average_window",
        "pretty",
    ];

    const EXAMPLES_SUBCOMMAND: &str = "examples";

    #[test]
    fn reproduced_commands_preserve_consumer_only_and_end_to_end_topologies() {
        for arguments in [
            vec!["iggy-bench", "pinned-consumer", "tcp"],
            vec!["iggy-bench", "balanced-consumer-group", "tcp"],
            vec!["iggy-bench", "end-to-end-producing-consumer-group", "tcp"],
            vec![
                "iggy-bench",
                "end-to-end-producing-consumer-group",
                "--streams",
                "6",
                "--consumer-groups",
                "6",
                "--partitions",
                "1",
                "tcp",
            ],
        ] {
            let mut original = IggyBenchArgs::try_parse_from(arguments).unwrap();
            original.validate();
            let command = recreate_bench_command(&original);
            let arguments = command
                .split_ascii_whitespace()
                .skip_while(|argument| *argument != "iggy-bench");
            let mut reproduced = IggyBenchArgs::try_parse_from(arguments).unwrap();
            reproduced.validate();
            assert_eq!(reproduced.streams(), original.streams(), "{command}");
            assert_eq!(
                reproduced.number_of_partitions(),
                original.number_of_partitions(),
                "{command}"
            );
            assert_eq!(
                reproduced.number_of_consumer_groups(),
                original.number_of_consumer_groups(),
                "{command}"
            );
        }
    }

    #[test]
    fn reproduced_commands_preserve_poll_options() {
        let original = IggyBenchArgs::try_parse_from([
            "iggy-bench",
            "--max-wait",
            "1m 30s",
            "--min-count",
            "100",
            "--max-bytes",
            "65536",
            "pinned-consumer",
            "tcp",
        ])
        .unwrap();
        let command = recreate_bench_command(&original);
        let reproduced = IggyBenchArgs::try_parse_from(shell_words(&command)).unwrap();
        assert_eq!(
            reproduced.poll_options(),
            original.poll_options(),
            "{command}"
        );
    }

    /// Splits on spaces outside single quotes, as a shell does for these commands.
    fn shell_words(command: &str) -> Vec<String> {
        let mut words = vec![String::new()];
        let mut quoted = false;
        for character in command.chars() {
            match character {
                '\'' => quoted = !quoted,
                ' ' if !quoted => words.push(String::new()),
                character => words.last_mut().unwrap().push(character),
            }
        }
        words
            .into_iter()
            .skip_while(|word| word != "iggy-bench")
            .collect()
    }

    #[test]
    fn reproduced_websocket_commands_preserve_independent_topic_policies() {
        for messages in ["replicated", "persisted"] {
            for offsets in ["replicated", "persisted"] {
                let original = IggyBenchArgs::try_parse_from([
                    "iggy-bench",
                    "--durability",
                    messages,
                    "--consumer-offset-durability",
                    offsets,
                    "--messages-required-to-save",
                    "128",
                    "pinned-producer",
                    "ws",
                ])
                .unwrap();
                let command = recreate_bench_command(&original);
                let arguments = command
                    .split_ascii_whitespace()
                    .skip_while(|argument| *argument != "iggy-bench");
                let reproduced = IggyBenchArgs::try_parse_from(arguments).unwrap();
                assert!(
                    command
                        .split_ascii_whitespace()
                        .any(|argument| argument == "websocket")
                );
                assert_eq!(reproduced.durability(), original.durability());
                assert_eq!(
                    reproduced.consumer_offset_durability(),
                    original.consumer_offset_durability()
                );
                assert_eq!(
                    reproduced.messages_required_to_save(),
                    original.messages_required_to_save()
                );
            }
        }
    }

    #[test]
    fn every_bench_arg_should_be_a_typed_param_a_recorded_option_or_neutral() {
        let mut args = Vec::new();
        collect_arg_ids(&IggyBenchArgs::command(), "iggy-bench", &mut args);
        let listed = [TYPED_PARAM_ARGS, RECORDED_OPTION_ARGS, NEUTRAL_ARGS].concat();

        let unlisted: Vec<String> = args
            .iter()
            .filter(|(_, id)| !listed.contains(&id.as_str()))
            .map(|(path, id)| format!("{path} / {id}"))
            .collect();
        let stale: Vec<&str> = listed
            .iter()
            .copied()
            .filter(|listed_id| args.iter().all(|(_, id)| id != listed_id))
            .collect();

        assert!(
            unlisted.is_empty(),
            "add each arg to one list, and record it if it changes a result: {unlisted:#?}"
        );
        assert!(
            stale.is_empty(),
            "listed args that no longer exist: {stale:?}"
        );
    }

    fn collect_arg_ids(command: &Command, path: &str, args: &mut Vec<(String, String)>) {
        for arg in command.get_arguments() {
            args.push((path.to_owned(), arg.get_id().to_string()));
        }
        for subcommand in command.get_subcommands() {
            collect_arg_ids(
                subcommand,
                &format!("{path} {}", subcommand.get_name()),
                args,
            );
        }
    }

    #[test]
    fn given_non_default_recorded_flags_when_building_options_should_record_each_one() {
        let args = IggyBenchArgs::try_parse_from([
            "iggy-bench",
            "--durability",
            "persisted",
            "--messages-required-to-save",
            "128",
            "--consumer-offset-durability",
            "persisted",
            "--max-wait",
            "250ms",
            "--min-count",
            "10",
            "--max-bytes",
            "2097152",
            "--warmup-time",
            "5s",
            "--high-level-api",
            "--reuse-streams",
            "pinned-producer-and-consumer",
            "--max-topic-size",
            "1GiB",
            "--message-expiry",
            "1h",
            "--read-amplification",
            "1.5",
            "tcp",
            "--nodelay",
            "--tls",
        ])
        .unwrap();

        assert_eq!(
            options_from_args(&args),
            BenchmarkOptions {
                topic: BenchmarkTopicOptions {
                    durability: BenchmarkDurability::Persisted,
                    messages_required_to_save: Some(128),
                    max_topic_size: BenchmarkMaxTopicSize::Bytes(1_073_741_824),
                    message_expiry: BenchmarkMessageExpiry::AfterUs(3_600_000_000),
                },
                consumer: Some(BenchmarkConsumerOptions {
                    consumer_offset_durability: BenchmarkDurability::Persisted,
                    max_wait_us: 250_000,
                    min_count: 10,
                    max_bytes: Some(2_097_152),
                    read_amplification: Some(1.5),
                }),
                client: BenchmarkClientOptions {
                    warmup_time_us: 5_000_000,
                    high_level_api: true,
                    reuse_streams: true,
                },
                tcp: BenchmarkTcpOptions {
                    nodelay: true,
                    tls: true,
                },
            }
        );
    }

    #[test]
    fn given_default_flags_when_building_options_should_match_legacy_options_of_each_kind() {
        let command = IggyBenchArgs::command();
        let kinds = command
            .get_subcommands()
            .filter(|kind| kind.get_name() != EXAMPLES_SUBCOMMAND);
        for kind in kinds {
            let args =
                IggyBenchArgs::try_parse_from(["iggy-bench", kind.get_name(), "tcp"]).unwrap();
            assert_eq!(
                options_from_args(&args),
                BenchmarkOptions::legacy(args.kind()),
                "{}",
                kind.get_name()
            );
        }
    }

    #[test]
    fn given_topic_limit_spellings_when_building_options_should_record_what_the_server_applies() {
        let cases: &[(&[&str], BenchmarkMaxTopicSize, BenchmarkMessageExpiry)] = &[
            (
                &[],
                BenchmarkMaxTopicSize::Unlimited,
                BenchmarkMessageExpiry::NeverExpire,
            ),
            (
                &["-T", "0"],
                BenchmarkMaxTopicSize::ServerDefault,
                BenchmarkMessageExpiry::NeverExpire,
            ),
            // `unlimited` parses to 0 bytes, which the server reads as its default.
            (
                &["-T", "unlimited"],
                BenchmarkMaxTopicSize::ServerDefault,
                BenchmarkMessageExpiry::NeverExpire,
            ),
            (
                &["-e", "server_default"],
                BenchmarkMaxTopicSize::Unlimited,
                BenchmarkMessageExpiry::ServerDefault,
            ),
            (
                &["-e", "0s"],
                BenchmarkMaxTopicSize::Unlimited,
                BenchmarkMessageExpiry::ServerDefault,
            ),
            (
                &["-e", "never_expire"],
                BenchmarkMaxTopicSize::Unlimited,
                BenchmarkMessageExpiry::NeverExpire,
            ),
        ];
        for &(flags, max_topic_size, message_expiry) in cases {
            let arguments = ["iggy-bench", "pinned-producer"]
                .into_iter()
                .chain(flags.iter().copied())
                .chain(["tcp"]);
            let topic = options_from_args(&IggyBenchArgs::try_parse_from(arguments).unwrap()).topic;
            assert_eq!(topic.max_topic_size, max_topic_size, "{flags:?}");
            assert_eq!(topic.message_expiry, message_expiry, "{flags:?}");
        }
    }

    #[test]
    fn given_existing_run_dirs_when_creating_another_should_add_a_suffix_up_to_the_bound() {
        let root = tempfile::tempdir().unwrap();
        let output_dir = root.path().join("results");

        let first = create_run_dir(&output_dir, "run").unwrap();
        let second = create_run_dir(&output_dir, "run").unwrap();
        assert_eq!(first, output_dir.join("run"));
        assert_eq!(second, output_dir.join("run_2"));

        for _ in 3..=MAX_RUN_DIR_ATTEMPTS {
            create_run_dir(&output_dir, "run").unwrap();
        }
        let error = create_run_dir(&output_dir, "run").unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
    }
}
