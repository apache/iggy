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

use clap::{Parser, Subcommand, ValueEnum};
use iggy_common::IggyDuration;
use std::path::PathBuf;
use std::str::FromStr;

/// Default message payload size in bytes.
const DEFAULT_MESSAGE_SIZE: u32 = 256;

#[derive(Parser)]
#[command(
    name = "iggy-chaos",
    about = "Chaos stress testing CLI for Iggy message streaming platform"
)]
pub struct IggyLabArgs {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand)]
pub enum Command {
    /// Run a chaos scenario against a live Iggy server
    Run(RunArgs),
    /// Replay a recorded trace against a live Iggy server
    Replay(ReplayArgs),
    /// List available scenarios
    List,
    /// Show detailed description of a scenario
    Explain {
        /// Scenario name to explain
        scenario: ScenarioName,
    },
    /// Convert trace.jsonl to Perfetto trace format for visualization
    Viz(VizArgs),
}

#[derive(Parser)]
pub struct RunArgs {
    /// Which scenario to run
    pub scenario: ScenarioName,

    /// Server address (host:port)
    #[arg(long, default_value = "127.0.0.1:8090")]
    pub server_address: String,

    /// Transport protocol
    #[arg(long, default_value = "tcp")]
    pub transport: Transport,

    /// PRNG seed for reproducibility (random if omitted)
    #[arg(long)]
    pub seed: Option<u64>,

    /// How long to run the chaos phase
    #[arg(long, default_value = "30s", value_parser = IggyDuration::from_str)]
    pub duration: IggyDuration,

    /// Stop after this many total ops (across all workers). Duration still acts as safety timeout.
    #[arg(long)]
    pub ops: Option<u64>,

    /// Resource name prefix for safety isolation.
    /// With `--prefix lab-`, stable streams use `lab-s-` and churn streams use `lab-c-`.
    #[arg(long, default_value = "lab-")]
    pub prefix: String,

    /// Directory for trace artifacts (stdout-only if omitted)
    #[arg(long)]
    pub output_dir: Option<PathBuf>,

    /// Number of concurrent worker tasks
    #[arg(long, default_value = "8", value_parser = clap::value_parser!(u32).range(1..))]
    pub workers: u32,

    /// Payload size in bytes per message
    #[arg(long, default_value = "256", value_parser = clap::value_parser!(u32).range(1..))]
    pub message_size: u32,

    /// Messages per send batch
    #[arg(long, default_value = "10")]
    pub messages_per_batch: u32,

    /// Continue running after ServerBug classification instead of stopping
    #[arg(long)]
    pub no_fail_fast: bool,

    /// Delete stale lab-prefixed resources before starting
    #[arg(long)]
    pub force_cleanup: bool,

    /// Skip post-run resource deletion
    #[arg(long)]
    pub no_cleanup: bool,

    /// Skip post-run invariant verification
    #[arg(long)]
    pub skip_post_run_verify: bool,
}

#[derive(Parser)]
pub struct ReplayArgs {
    /// Directory containing trace-worker-*.jsonl files
    pub trace_dir: PathBuf,

    /// Server address (host:port)
    #[arg(long, default_value = "127.0.0.1:8090")]
    pub server_address: String,

    /// Transport protocol
    #[arg(long, default_value = "tcp")]
    pub transport: Transport,

    /// Payload size in bytes per message
    #[arg(long, default_value_t = DEFAULT_MESSAGE_SIZE)]
    pub message_size: u32,

    /// Resource name prefix for cleanup
    #[arg(long, default_value = "lab-")]
    pub prefix: String,

    /// Stop on first concerning divergence
    #[arg(long)]
    pub fail_fast: bool,

    /// Delete stale lab-prefixed resources before replaying
    #[arg(long)]
    pub force_cleanup: bool,
}

#[derive(Parser)]
pub struct VizArgs {
    /// Bundle directory containing trace.jsonl
    pub bundle_dir: PathBuf,
}

/// Maps to TransportProtocol but implements clap::ValueEnum
#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Transport {
    Tcp,
    Quic,
    Http,
    #[value(alias = "ws")]
    WebSocket,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum ScenarioName {
    ConcurrentCrud,
    SegmentRotation,
    MixedWorkload,
    StaleConsumers,
    ConsumerGroupChurn,
}

impl std::fmt::Display for Transport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Tcp => write!(f, "tcp"),
            Self::Quic => write!(f, "quic"),
            Self::Http => write!(f, "http"),
            Self::WebSocket => write!(f, "websocket"),
        }
    }
}

impl std::fmt::Display for ScenarioName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConcurrentCrud => write!(f, "concurrent-crud"),
            Self::SegmentRotation => write!(f, "segment-rotation"),
            Self::MixedWorkload => write!(f, "mixed-workload"),
            Self::StaleConsumers => write!(f, "stale-consumers"),
            Self::ConsumerGroupChurn => write!(f, "consumer-group-churn"),
        }
    }
}
