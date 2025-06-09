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
    common::IggyBenchArgs, defaults::DEFAULT_NUMBER_OF_PRODUCERS, props::BenchmarkKindProps,
    transport::BenchmarkTransportCommand,
};
use clap::{CommandFactory, Parser, error::ErrorKind};
use iggy::prelude::IggyByteSize;
use std::num::NonZeroU32;

#[derive(Parser, Debug, Clone)]
pub struct PinnedConsumerArgs {
    #[command(subcommand)]
    pub transport: BenchmarkTransportCommand,

    /// Number of streams
    /// If not provided then number of streams will be equal to number of consumers.
    #[arg(long, short = 's')]
    pub streams: Option<NonZeroU32>,

    /// Number of consumers
    #[arg(long, short = 'c', default_value_t = DEFAULT_NUMBER_OF_PRODUCERS)]
    pub consumers: NonZeroU32,
}

impl BenchmarkKindProps for PinnedConsumerArgs {
    fn streams(&self) -> u32 {
        self.streams.unwrap_or(self.consumers).get()
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
        0
    }

    fn max_topic_size(&self) -> Option<IggyByteSize> {
        None
    }

    fn validate(&self) {
        let mut cmd = IggyBenchArgs::command();
        let streams = self.streams();
        let consumers = self.consumers();
        if streams > consumers {
            cmd.error(
                ErrorKind::ArgumentConflict,
                format!("For pinned consumer, number of streams ({streams}) must be equal to the number of consumers ({consumers}).",
            ))
            .exit();
        }
    }
}
