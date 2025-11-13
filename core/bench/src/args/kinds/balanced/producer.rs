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

use crate::args::{
    common::IggyBenchArgs,
    defaults::{
        DEFAULT_BALANCED_NUMBER_OF_PARTITIONS, DEFAULT_BALANCED_NUMBER_OF_STREAMS,
        DEFAULT_NUMBER_OF_PRODUCERS,
    },
    props::BenchmarkKindProps,
    transport::BenchmarkTransportCommand,
};
use clap::{CommandFactory, Parser, error::ErrorKind};
use iggy::prelude::IggyByteSize;
use std::num::NonZeroU32;

/// N producers sending to N separated stream-topic with single partition (one stream per one producer)
#[derive(Parser, Debug, Clone)]
pub struct BalancedProducerArgs {
    #[command(subcommand)]
    pub transport: BenchmarkTransportCommand,

    /// Number of streams
    #[arg(long, short = 's', default_value_t = DEFAULT_BALANCED_NUMBER_OF_STREAMS)]
    pub streams: NonZeroU32,

    /// Number of partitions
    #[arg(long, short = 'a',default_value_t = DEFAULT_BALANCED_NUMBER_OF_PARTITIONS)]
    pub partitions: NonZeroU32,

    /// Number of producers
    #[arg(long, short = 'p', default_value_t = DEFAULT_NUMBER_OF_PRODUCERS)]
    pub producers: NonZeroU32,

    /// Max topic size in human readable format, e.g. "1GiB", "2MB", "1GiB". If not provided then the server default will be used.
    #[arg(long, short = 'T')]
    pub max_topic_size: Option<IggyByteSize>,
}

impl BenchmarkKindProps for BalancedProducerArgs {
    fn streams(&self) -> u32 {
        self.streams.get()
    }

    fn partitions(&self) -> u32 {
        self.partitions.get()
    }

    fn consumers(&self) -> u32 {
        0
    }

    fn producers(&self) -> u32 {
        self.producers.get()
    }

    fn transport_command(&self) -> &BenchmarkTransportCommand {
        &self.transport
    }

    fn number_of_consumer_groups(&self) -> u32 {
        0
    }

    fn max_topic_size(&self) -> Option<IggyByteSize> {
        self.max_topic_size
    }

    fn validate(&self) {
        let partitions = self.partitions();
        let mut cmd = IggyBenchArgs::command();

        if partitions < 2 {
            cmd.error(
                ErrorKind::ArgumentConflict,
                format!("For balanced producer, number of partitions must be at least 2, got {partitions}"),
            )
            .exit();
        }
    }
}
