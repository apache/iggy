/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use super::kind::BenchmarkKindCommand;
use super::output::BenchmarkOutputCommand;
use super::props::{BenchmarkKindProps, BenchmarkTransportProps};
use super::{defaults::*, transport::BenchmarkTransportCommand};
use clap::error::ErrorKind;
use clap::{CommandFactory, Parser};
use iggy::utils::byte_size::IggyByteSize;
use iggy::utils::duration::IggyDuration;
use iggy_bench_report::benchmark_kind::BenchmarkKind;
use iggy_bench_report::params::BenchmarkParams;
use iggy_bench_report::transport::BenchmarkTransport;
use integration::test_server::Transport;
use std::net::SocketAddr;
use std::num::NonZeroU32;
use std::path::Path;
use std::str::FromStr;
use tracing::info;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct IggyBenchArgs {
    /// Benchmark kind
    #[command(subcommand)]
    pub benchmark_kind: BenchmarkKindCommand,

    /// Number of messages per batch
    #[arg(long, short = 'p', default_value_t = DEFAULT_MESSAGES_PER_BATCH)]
    pub messages_per_batch: NonZeroU32,

    /// Number of message batches
    #[arg(long, short = 'b', default_value_t = DEFAULT_MESSAGE_BATCHES)]
    pub message_batches: NonZeroU32,

    /// Message size in bytes
    #[arg(long, short = 'm', default_value_t = DEFAULT_MESSAGE_SIZE)]
    pub message_size: NonZeroU32,

    /// Start stream id
    #[arg(long, short = 'S', default_value_t = DEFAULT_START_STREAM_ID)]
    pub start_stream_id: NonZeroU32,

    /// Optional rate limit per individual producer in bytes per second (not aggregate).
    /// Accepts human-readable formats like "50KB", "10MB", or "1GB"
    #[arg(long, short = 'r', verbatim_doc_comment)]
    pub rate_limit: Option<IggyByteSize>,

    /// Warmup time in human readable format, e.g. "1s", "2m", "3h"
    #[arg(long, short = 'w', default_value_t = IggyDuration::from_str(DEFAULT_WARMUP_TIME).unwrap())]
    pub warmup_time: IggyDuration,

    /// Server stdout visibility
    #[arg(long, short = 'v', default_value_t = DEFAULT_SERVER_STDOUT_VISIBILITY)]
    pub verbose: bool,

    /// Sampling time for metrics collection. It is also used as bucket size for time series calculations.
    #[arg(long, short = 't', default_value_t = IggyDuration::from_str(DEFAULT_SAMPLING_TIME).unwrap(), value_parser = IggyDuration::from_str)]
    pub sampling_time: IggyDuration,

    /// Window size for moving average calculations in time series data
    #[arg(long, short = 'W', default_value_t = DEFAULT_MOVING_AVERAGE_WINDOW)]
    pub moving_average_window: u32,

    /// Shutdown iggy-server and remove server local_data directory after the benchmark is finished.
    /// Only applicable to local benchmarks.
    #[arg(long, default_value_t = DEFAULT_PERFORM_CLEANUP, verbatim_doc_comment)]
    pub cleanup: bool,

    /// iggy-server executable path.
    /// Only applicable to local benchmarks.
    #[arg(long, short='e', default_value = None, value_parser = validate_server_executable_path)]
    pub server_executable_path: Option<String>,

    /// Skip server start.
    /// Only applicable to local benchmarks.
    #[arg(long, short = 'k', default_value_t = DEFAULT_SKIP_SERVER_START, verbatim_doc_comment)]
    pub skip_server_start: bool,
}

fn validate_server_executable_path(v: &str) -> Result<String, String> {
    if Path::new(v).exists() {
        Ok(v.to_owned())
    } else {
        Err(format!("Provided server executable '{v}' does not exist."))
    }
}

impl IggyBenchArgs {
    pub fn transport_command(&self) -> &BenchmarkTransportCommand {
        self.benchmark_kind.transport_command()
    }

    pub fn transport(&self) -> &Transport {
        self.benchmark_kind.transport_command().transport()
    }

    pub fn nodelay(&self) -> bool {
        self.benchmark_kind.transport_command().nodelay()
    }

    pub fn server_address(&self) -> &str {
        self.benchmark_kind
            .inner()
            .transport_command()
            .server_address()
    }

    pub fn start_stream_id(&self) -> u32 {
        self.start_stream_id.get()
    }

    pub fn validate(&self) {
        let server_address = self.server_address().parse::<SocketAddr>().unwrap();
        if (self.cleanup || self.verbose) && !server_address.ip().is_loopback() {
            IggyBenchArgs::command()
                .error(
                    ErrorKind::ArgumentConflict,
                    format!(
                        "Cannot use cleanup or verbose flags with a non-loopback server address: {}",
                        self.server_address()
                    ),
                )
                .exit();
        }

        if self.output_dir().is_none()
            && (self.gitref().is_some()
                || self.identifier().is_some()
                || self.remark().is_some()
                || self.extra_info().is_some()
                || self.gitref_date().is_some())
        {
            IggyBenchArgs::command()
                .error(
                    ErrorKind::ArgumentConflict,
                    "--git-ref, --git-ref-date, --identifier, --remark, --extra-info can only be used with --output-dir",
                )
                .exit();
        }

        self.benchmark_kind.inner().validate()
    }

    pub fn messages_per_batch(&self) -> u32 {
        self.messages_per_batch.get()
    }

    pub fn message_batches(&self) -> u32 {
        self.message_batches.get()
    }

    pub fn message_size(&self) -> u32 {
        self.message_size.get()
    }

    pub fn streams(&self) -> u32 {
        self.benchmark_kind.inner().streams()
    }

    pub fn number_of_partitions(&self) -> u32 {
        self.benchmark_kind.inner().partitions()
    }

    pub fn consumers(&self) -> u32 {
        self.benchmark_kind.inner().consumers()
    }

    pub fn producers(&self) -> u32 {
        self.benchmark_kind.inner().producers()
    }

    pub fn kind(&self) -> BenchmarkKind {
        self.benchmark_kind.as_simple_kind()
    }

    pub fn number_of_consumer_groups(&self) -> u32 {
        self.benchmark_kind.inner().number_of_consumer_groups()
    }

    pub fn warmup_time(&self) -> IggyDuration {
        self.warmup_time
    }

    pub fn sampling_time(&self) -> IggyDuration {
        self.sampling_time
    }

    pub fn moving_average_window(&self) -> u32 {
        self.moving_average_window
    }

    pub fn rate_limit(&self) -> Option<IggyByteSize> {
        self.rate_limit
    }

    pub fn output_dir(&self) -> Option<String> {
        self.benchmark_kind
            .inner()
            .transport_command()
            .output_command()
            .as_ref()
            .and_then(|cmd| match cmd {
                BenchmarkOutputCommand::Output(args) => args.output_dir.clone(),
            })
    }

    pub fn identifier(&self) -> Option<String> {
        self.benchmark_kind
            .inner()
            .transport_command()
            .output_command()
            .as_ref()
            .map(|cmd| match cmd {
                BenchmarkOutputCommand::Output(args) => args.identifier.clone(),
            })
    }

    pub fn remark(&self) -> Option<String> {
        self.benchmark_kind
            .inner()
            .transport_command()
            .output_command()
            .as_ref()
            .and_then(|cmd| match cmd {
                BenchmarkOutputCommand::Output(args) => args.remark.clone(),
            })
    }

    pub fn extra_info(&self) -> Option<String> {
        self.benchmark_kind
            .inner()
            .transport_command()
            .output_command()
            .as_ref()
            .and_then(|cmd| match cmd {
                BenchmarkOutputCommand::Output(args) => args.extra_info.clone(),
            })
    }

    pub fn gitref(&self) -> Option<String> {
        self.benchmark_kind
            .inner()
            .transport_command()
            .output_command()
            .as_ref()
            .and_then(|cmd| match cmd {
                BenchmarkOutputCommand::Output(args) => args.gitref.clone(),
            })
    }

    pub fn gitref_date(&self) -> Option<String> {
        self.benchmark_kind
            .inner()
            .transport_command()
            .output_command()
            .as_ref()
            .and_then(|cmd| match cmd {
                BenchmarkOutputCommand::Output(args) => args.gitref_date.clone(),
            })
    }

    pub fn open_charts(&self) -> bool {
        self.benchmark_kind
            .inner()
            .transport_command()
            .output_command()
            .as_ref()
            .is_some_and(|cmd| match cmd {
                BenchmarkOutputCommand::Output(args) => args.open_charts,
            })
    }

    pub fn max_topic_size(&self) -> Option<IggyByteSize> {
        self.benchmark_kind.inner().max_topic_size()
    }

    /// Generates the output directory name based on benchmark parameters.
    pub fn generate_dir_name(&self) -> String {
        let benchmark_kind = match &self.benchmark_kind {
            BenchmarkKindCommand::PinnedProducer(_) => "pinned_producer",
            BenchmarkKindCommand::PinnedConsumer(_) => "pinned_consumer",
            BenchmarkKindCommand::PinnedProducerAndConsumer(_) => "pinned_producer_and_consumer",
            BenchmarkKindCommand::BalancedProducer(_) => "balanced_producer",
            BenchmarkKindCommand::BalancedConsumerGroup(_) => "balanced_consumer_group",
            BenchmarkKindCommand::BalancedProducerAndConsumerGroup(_) => {
                "balanced_producer_and_consumer"
            }
            BenchmarkKindCommand::EndToEndProducingConsumer(_) => "end_to_end_producing_consumer",
            BenchmarkKindCommand::EndToEndProducingConsumerGroup(_) => {
                "end_to_end_producing_consumer_group"
            }
            BenchmarkKindCommand::Examples => unreachable!(),
        };

        let transport = match self.transport_command() {
            BenchmarkTransportCommand::Tcp(_) => "tcp",
            BenchmarkTransportCommand::Quic(_) => "quic",
            BenchmarkTransportCommand::Http(_) => "http",
        };

        let actors = match &self.benchmark_kind {
            BenchmarkKindCommand::PinnedProducer(_) => self.producers(),
            BenchmarkKindCommand::PinnedConsumer(_) => self.consumers(),
            BenchmarkKindCommand::PinnedProducerAndConsumer(_) => {
                self.producers() + self.consumers()
            }
            BenchmarkKindCommand::BalancedProducer(_) => self.producers(),
            BenchmarkKindCommand::BalancedConsumerGroup(_) => self.consumers(),
            BenchmarkKindCommand::BalancedProducerAndConsumerGroup(_) => {
                self.producers() + self.consumers()
            }
            BenchmarkKindCommand::EndToEndProducingConsumer(_) => self.producers(),
            BenchmarkKindCommand::EndToEndProducingConsumerGroup(_) => self.producers(),
            BenchmarkKindCommand::Examples => unreachable!(),
        };

        let mut parts = vec![
            benchmark_kind.to_string(),
            actors.to_string(),
            self.message_size().to_string(),
            self.messages_per_batch().to_string(),
            self.message_batches().to_string(),
            transport.to_string(),
        ];

        if let Some(remark) = &self.remark() {
            parts.push(remark.to_string());
        }

        if let Some(gitref) = &self.gitref() {
            parts.push(gitref.to_string());
        }

        if let Some(identifier) = &self.identifier() {
            parts.push(identifier.to_string());
        }

        parts.join("_")
    }

    /// Generates a human-readable pretty name for the benchmark
    pub fn generate_pretty_name(&self) -> String {
        let consumer_or_producer = match &self.benchmark_kind {
            BenchmarkKindCommand::PinnedProducer(_) | BenchmarkKindCommand::BalancedProducer(_) => {
                format!("{} producers", self.producers())
            }
            BenchmarkKindCommand::PinnedConsumer(_)
            | BenchmarkKindCommand::BalancedConsumerGroup(_) => {
                format!("{} consumers", self.consumers())
            }
            BenchmarkKindCommand::PinnedProducerAndConsumer(_)
            | BenchmarkKindCommand::BalancedProducerAndConsumerGroup(_) => format!(
                "{} producers/{} consumers",
                self.producers(),
                self.consumers()
            ),
            BenchmarkKindCommand::EndToEndProducingConsumer(_) => {
                format!("{} producing consumers", self.producers(),)
            }
            BenchmarkKindCommand::EndToEndProducingConsumerGroup(_) => {
                format!(
                    "{} producing consumers/{} consumer groups",
                    self.producers(),
                    self.consumers()
                )
            }
            BenchmarkKindCommand::Examples => unreachable!(),
        };

        let mut name = format!(
            "{}, {}B msgs, {} msgs/batch",
            consumer_or_producer,
            self.message_size(),
            self.messages_per_batch(),
        );

        if let Some(remark) = &self.remark() {
            name.push_str(&format!(" ({})", remark));
        }

        name
    }
}

fn recreate_bench_command(args: &IggyBenchArgs) -> String {
    let mut parts = Vec::new();

    // If using localhost, add env vars
    let server_address = args.server_address();
    let is_localhost = server_address
        .split(':')
        .next()
        .map(|host| host == "localhost" || host == "127.0.0.1")
        .unwrap_or(false);

    if is_localhost {
        // Get all env vars starting with IGGY_
        let iggy_vars: Vec<_> = std::env::vars()
            .filter(|(k, _)| k.starts_with("IGGY_"))
            .collect();

        if !iggy_vars.is_empty() {
            info!("Found env vars starting with IGGY_: {:?}", iggy_vars);
            parts.extend(iggy_vars.into_iter().map(|(k, v)| format!("{}={}", k, v)));
        }
    }

    parts.push("iggy-bench".to_string());

    let messages_per_batch = args.messages_per_batch();
    if messages_per_batch != DEFAULT_MESSAGES_PER_BATCH.get() {
        parts.push(format!("--messages-per-batch {}", messages_per_batch));
    }

    let message_batches = args.message_batches();
    if message_batches != DEFAULT_MESSAGE_BATCHES.get() {
        parts.push(format!("--message-batches {}", message_batches));
    }

    let message_size = args.message_size();
    if message_size != DEFAULT_MESSAGE_SIZE.get() {
        parts.push(format!("--message-size {}", message_size));
    }

    if let Some(rate_limit) = args.rate_limit() {
        parts.push(format!("--rate-limit \'{}\'", rate_limit));
    }

    if args.warmup_time().to_string() != DEFAULT_WARMUP_TIME {
        parts.push(format!("--warmup-time \'{}\'", args.warmup_time()));
    }

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

    // Add benchmark params, skipping defaults
    let producers = args.producers();
    let consumers = args.consumers();
    let number_of_consumer_groups = args.number_of_consumer_groups();

    match args.benchmark_kind.as_simple_kind() {
        BenchmarkKind::PinnedProducer
        | BenchmarkKind::BalancedProducer
        | BenchmarkKind::EndToEndProducingConsumer => {
            if producers != DEFAULT_NUMBER_OF_PRODUCERS.get() {
                parts.push(format!("--producers {}", producers));
            }
        }
        BenchmarkKind::PinnedConsumer | BenchmarkKind::BalancedConsumerGroup => {
            if consumers != DEFAULT_NUMBER_OF_CONSUMERS.get() {
                parts.push(format!("--consumers {}", consumers));
            }
        }
        BenchmarkKind::PinnedProducerAndConsumer
        | BenchmarkKind::BalancedProducerAndConsumerGroup => {
            if producers != DEFAULT_NUMBER_OF_PRODUCERS.get() {
                parts.push(format!("--producers {}", producers));
            }
            if consumers != DEFAULT_NUMBER_OF_CONSUMERS.get() {
                parts.push(format!("--consumers {}", consumers));
            }
        }
        BenchmarkKind::EndToEndProducingConsumerGroup => {
            if producers != DEFAULT_NUMBER_OF_PRODUCERS.get() {
                parts.push(format!("--producers {}", producers));
            }
            if consumers != DEFAULT_NUMBER_OF_CONSUMERS.get() {
                parts.push(format!("--consumers {}", consumers));
            }
            if number_of_consumer_groups != DEFAULT_NUMBER_OF_CONSUMER_GROUPS.get() {
                parts.push(format!("--consumer-groups {}", number_of_consumer_groups));
            }
        }
    }

    let streams = args.streams();
    let default_streams = match args.benchmark_kind.as_simple_kind() {
        BenchmarkKind::BalancedProducerAndConsumerGroup
        | BenchmarkKind::BalancedConsumerGroup
        | BenchmarkKind::BalancedProducer => DEFAULT_BALANCED_NUMBER_OF_STREAMS.get(),
        _ => DEFAULT_PINNED_NUMBER_OF_STREAMS.get(),
    };
    if streams != default_streams {
        parts.push(format!("--streams {}", streams));
    }

    let partitions = args.number_of_partitions();
    let default_partitions = match args.benchmark_kind.as_simple_kind() {
        BenchmarkKind::BalancedProducerAndConsumerGroup
        | BenchmarkKind::BalancedConsumerGroup
        | BenchmarkKind::BalancedProducer => DEFAULT_BALANCED_NUMBER_OF_PARTITIONS.get(),
        _ => DEFAULT_PINNED_NUMBER_OF_PARTITIONS.get(),
    };
    if partitions != default_partitions {
        parts.push(format!("--partitions {}", partitions));
    }

    let consumer_groups = args.number_of_consumer_groups();
    if args.benchmark_kind.as_simple_kind() == BenchmarkKind::BalancedConsumerGroup
        || args.benchmark_kind.as_simple_kind() == BenchmarkKind::BalancedProducerAndConsumerGroup
            && consumer_groups != DEFAULT_NUMBER_OF_CONSUMER_GROUPS.get()
    {
        parts.push(format!("--consumer-groups {}", consumer_groups));
    }

    if let Some(max_topic_size) = args.max_topic_size() {
        parts.push(format!("--max-topic-size \'{}\'", max_topic_size));
    }

    // Add transport and server address, skipping if default
    let transport = args.transport().to_string().to_lowercase();
    parts.push(transport.clone());

    let default_address = match transport.as_str() {
        "tcp" => DEFAULT_TCP_SERVER_ADDRESS,
        "quic" => DEFAULT_QUIC_SERVER_ADDRESS,
        "http" => DEFAULT_HTTP_SERVER_ADDRESS,
        _ => "",
    };

    if server_address != default_address {
        parts.push(format!("--server-address {}", server_address));
    }

    parts.push("output".to_string());

    parts.push("-o performance_results".to_string());

    let remark = args.remark();
    if let Some(remark) = remark {
        parts.push(format!("--remark \'{}\'", remark));
    }
    parts.join(" ")
}

impl From<&IggyBenchArgs> for BenchmarkParams {
    fn from(args: &IggyBenchArgs) -> Self {
        let benchmark_kind = args.benchmark_kind.as_simple_kind();

        // Ugly conversion but let it stay here to have `iggy-bench-report` not depend on `iggy` or `integration`
        let transport = match args.transport() {
            Transport::Tcp => BenchmarkTransport::Tcp,
            Transport::Quic => BenchmarkTransport::Quic,
            Transport::Http => BenchmarkTransport::Http,
        };
        let server_address = args.server_address().to_string();
        let remark = args.remark();
        let extra_info = args.extra_info();
        let gitref = args.gitref();
        let gitref_date = args.gitref_date();
        let messages_per_batch = args.messages_per_batch();
        let message_batches = args.message_batches();
        let message_size = args.message_size();
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
            .unwrap_or("no_remark".to_string())
            .replace(' ', "_");

        let params_identifier = vec![
            benchmark_kind.to_string(),
            transport.to_string(),
            remark_for_identifier,
            messages_per_batch.to_string(),
            message_batches.to_string(),
            message_size.to_string(),
            producers.to_string(),
            consumers.to_string(),
            streams.to_string(),
            partitions.to_string(),
            consumer_groups.to_string(),
        ];

        let params_identifier = params_identifier.join("_");

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
        }
    }
}
