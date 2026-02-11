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

use crate::args::{props::BenchmarkKindProps, transport::BenchmarkTransportCommand};
use clap::{CommandFactory, Parser, ValueEnum, error::ErrorKind};
use iggy::prelude::{IggyByteSize, IggyDuration, IggyExpiry};
use std::num::NonZeroU32;
use std::str::FromStr;

const DEFAULT_PRODUCERS: NonZeroU32 = nonzero_lit::u32!(4);
const DEFAULT_CONSUMERS: NonZeroU32 = nonzero_lit::u32!(4);
const DEFAULT_CHURN_CONCURRENCY: NonZeroU32 = nonzero_lit::u32!(1);
const DEFAULT_STREAMS: u32 = 2;
const DEFAULT_PARTITIONS: u32 = 4;
const DEFAULT_CONSUMER_GROUPS: u32 = 2;

/// Determines the mix of API operations exercised during the stress test.
#[derive(Debug, Clone, Copy, ValueEnum, Default)]
pub enum ApiMix {
    /// Data-plane + control-plane CRUD + admin operations
    #[default]
    Mixed,
    /// Only `send_messages` and `poll_messages`
    DataPlaneOnly,
    /// Heavy CRUD churn with minimal data-plane
    ControlPlaneHeavy,
    /// All available APIs including admin operations
    All,
}

#[derive(Parser, Debug, Clone)]
pub struct StressArgs {
    #[command(subcommand)]
    pub transport: BenchmarkTransportCommand,

    /// Total test duration (e.g. "2m", "10m", "1h")
    #[arg(long, short = 'd', value_parser = IggyDuration::from_str)]
    pub duration: IggyDuration,

    /// Number of data-plane producer actors
    #[arg(long, short = 'p', default_value_t = DEFAULT_PRODUCERS)]
    pub producers: NonZeroU32,

    /// Number of data-plane consumer actors
    #[arg(long, short = 'c', default_value_t = DEFAULT_CONSUMERS)]
    pub consumers: NonZeroU32,

    /// Number of control-plane churner actors
    #[arg(long, default_value_t = DEFAULT_CHURN_CONCURRENCY)]
    pub churn_concurrency: NonZeroU32,

    /// Interval between CRUD churn operations (e.g. "3s", "10s")
    #[arg(long, default_value = "3s", value_parser = IggyDuration::from_str)]
    pub churn_interval: IggyDuration,

    /// Max topic size to bound disk usage. For maximum race density, also run
    /// the server with `IGGY_SYSTEM_SEGMENT_SIZE="1MiB"`.
    #[arg(long, default_value = "500MiB")]
    pub max_topic_size: IggyByteSize,

    /// Message TTL for automatic cleanup
    #[arg(long, default_value = "30s", value_parser = IggyExpiry::from_str)]
    pub message_expiry: IggyExpiry,

    /// API operation mix
    #[arg(long, value_enum, default_value_t = ApiMix::All)]
    pub api_mix: ApiMix,

    /// RNG seed for reproducible chaos operations
    #[arg(long)]
    pub chaos_seed: Option<u64>,
}

impl BenchmarkKindProps for StressArgs {
    fn streams(&self) -> u32 {
        DEFAULT_STREAMS
    }

    fn partitions(&self) -> u32 {
        DEFAULT_PARTITIONS
    }

    fn consumers(&self) -> u32 {
        self.consumers.get()
    }

    fn producers(&self) -> u32 {
        self.producers.get()
    }

    fn transport_command(&self) -> &BenchmarkTransportCommand {
        &self.transport
    }

    fn number_of_consumer_groups(&self) -> u32 {
        DEFAULT_CONSUMER_GROUPS
    }

    fn max_topic_size(&self) -> Option<IggyByteSize> {
        Some(self.max_topic_size)
    }

    fn message_expiry(&self) -> IggyExpiry {
        self.message_expiry
    }

    fn validate(&self) {
        if self.duration.as_secs() < 10 {
            crate::args::common::IggyBenchArgs::command()
                .error(
                    ErrorKind::ValueValidation,
                    "Stress test duration must be at least 10 seconds",
                )
                .exit();
        }
    }
}

impl StressArgs {
    pub const fn duration(&self) -> IggyDuration {
        self.duration
    }

    pub const fn churn_concurrency(&self) -> NonZeroU32 {
        self.churn_concurrency
    }

    pub const fn churn_interval(&self) -> IggyDuration {
        self.churn_interval
    }

    pub const fn api_mix(&self) -> ApiMix {
        self.api_mix
    }

    #[allow(clippy::cast_possible_truncation)]
    pub fn chaos_seed(&self) -> u64 {
        self.chaos_seed.unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("system clock before epoch")
                .as_nanos() as u64
        })
    }
}
