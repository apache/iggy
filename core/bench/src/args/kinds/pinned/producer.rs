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
use std::num::NonZeroUsize;

#[derive(Parser, Debug, Clone)]
pub struct PinnedProducerArgs {
    #[command(subcommand)]
    pub transport: BenchmarkTransportCommand,

    /// Number of streams
    /// If not provided then number of streams will be equal to number of producers.
    #[arg(long, short = 's')]
    pub streams: Option<NonZeroUsize>,

    /// Number of producers
    #[arg(long, short = 'p', default_value_t = DEFAULT_NUMBER_OF_PRODUCERS)]
    pub producers: NonZeroUsize,

    /// Max topic size in human readable format, e.g. "1GiB", "2MiB", "1GiB". If not provided then the server default will be used.
    #[arg(long, short = 'T')]
    pub max_topic_size: Option<IggyByteSize>,
}

impl BenchmarkKindProps for PinnedProducerArgs {
    fn streams(&self) -> usize {
        self.streams.unwrap_or(self.producers).get()
    }

    fn partitions(&self) -> usize {
        1
    }

    fn consumers(&self) -> usize {
        0
    }

    fn producers(&self) -> usize {
        self.producers.get()
    }

    fn transport_command(&self) -> &BenchmarkTransportCommand {
        &self.transport
    }

    fn number_of_consumer_groups(&self) -> usize {
        0
    }

    fn max_topic_size(&self) -> Option<IggyByteSize> {
        self.max_topic_size
    }

    fn validate(&self) {
        let mut cmd = IggyBenchArgs::command();
        let streams = self.streams();
        let producers = self.producers();
        if streams != producers {
            cmd.error(
                ErrorKind::ArgumentConflict,
                format!("For pinned producer, number of streams ({streams}) must be equal to the number of producers ({producers}).",
            ))
            .exit();
        }
    }
}
