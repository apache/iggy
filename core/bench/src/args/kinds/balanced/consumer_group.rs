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
        DEFAULT_BALANCED_NUMBER_OF_STREAMS, DEFAULT_NUMBER_OF_CONSUMER_GROUPS,
        DEFAULT_NUMBER_OF_CONSUMERS,
    },
    props::BenchmarkKindProps,
    transport::BenchmarkTransportCommand,
};
use clap::{CommandFactory, Parser, error::ErrorKind};
use iggy::prelude::IggyByteSize;
use std::num::NonZeroU32;

/// Polling benchmark with consumer group
#[derive(Parser, Debug, Clone)]
pub struct BalancedConsumerGroupArgs {
    #[command(subcommand)]
    pub transport: BenchmarkTransportCommand,

    /// Number of streams
    #[arg(long, short = 's', default_value_t = DEFAULT_BALANCED_NUMBER_OF_STREAMS)]
    pub streams: NonZeroU32,

    /// Number of consumers
    #[arg(long, short = 'c', default_value_t = DEFAULT_NUMBER_OF_CONSUMERS)]
    pub consumers: NonZeroU32,

    /// Number of consumer groups
    #[arg(long, short = 'g', default_value_t = DEFAULT_NUMBER_OF_CONSUMER_GROUPS)]
    pub consumer_groups: NonZeroU32,
}

impl BenchmarkKindProps for BalancedConsumerGroupArgs {
    fn streams(&self) -> u32 {
        self.streams.get()
    }

    fn partitions(&self) -> u32 {
        0
    }

    fn consumers(&self) -> u32 {
        self.consumers.get()
    }

    fn producers(&self) -> u32 {
        0
    }

    fn transport_command(&self) -> &BenchmarkTransportCommand {
        &self.transport
    }

    fn number_of_consumer_groups(&self) -> u32 {
        self.consumer_groups.get()
    }

    fn validate(&self) {
        let cg_number = self.number_of_consumer_groups();
        let streams = self.streams();
        let mut cmd = IggyBenchArgs::command();

        if cg_number < streams {
            cmd.error(
                ErrorKind::ArgumentConflict,
                format!(
                    "In balanced consumer group, consumer groups number ({cg_number}) must be less than the number of streams ({streams})"
                ),
            )
            .exit();
        }
    }

    fn max_topic_size(&self) -> Option<IggyByteSize> {
        None
    }
}
