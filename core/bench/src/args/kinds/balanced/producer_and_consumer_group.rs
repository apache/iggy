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
        DEFAULT_NUMBER_OF_CONSUMER_GROUPS, DEFAULT_NUMBER_OF_CONSUMERS,
        DEFAULT_NUMBER_OF_PRODUCERS,
    },
    props::BenchmarkKindProps,
    transport::BenchmarkTransportCommand,
};
use clap::{CommandFactory, Parser, error::ErrorKind};
use iggy::prelude::IggyByteSize;
use std::num::{NonZeroU32, NonZeroUsize};

/// Polling benchmark with consumer group
#[derive(Parser, Debug, Clone)]
pub struct BalancedProducerAndConsumerGroupArgs {
    #[command(subcommand)]
    pub transport: BenchmarkTransportCommand,

    /// Number of streams
    #[arg(long, short = 's', default_value_t = DEFAULT_BALANCED_NUMBER_OF_STREAMS)]
    pub streams: NonZeroUsize,

    /// Number of partitions
    #[arg(long, short = 'a', default_value_t = DEFAULT_BALANCED_NUMBER_OF_PARTITIONS)]
    pub partitions: NonZeroUsize,

    /// Number of producers
    #[arg(long, short = 'p', default_value_t = DEFAULT_NUMBER_OF_PRODUCERS)]
    pub producers: NonZeroUsize,

    /// Number of consumers
    #[arg(long, short = 'c', default_value_t = DEFAULT_NUMBER_OF_CONSUMERS)]
    pub consumers: NonZeroUsize,

    /// Number of consumer groups
    #[arg(long, short = 'g', default_value_t = DEFAULT_NUMBER_OF_CONSUMER_GROUPS)]
    pub consumer_groups: NonZeroUsize,

    /// Max topic size in human readable format, e.g. "1GiB", "2MiB", "1GiB". If not provided then the server default will be used.
    #[arg(long, short = 'T')]
    pub max_topic_size: Option<IggyByteSize>,
}

impl BenchmarkKindProps for BalancedProducerAndConsumerGroupArgs {
    fn streams(&self) -> usize {
        self.streams.get()
    }

    fn partitions(&self) -> usize {
        self.partitions.get()
    }

    fn consumers(&self) -> usize {
        self.consumers.get()
    }

    fn producers(&self) -> usize {
        self.producers.get()
    }

    fn transport_command(&self) -> &BenchmarkTransportCommand {
        &self.transport
    }

    fn number_of_consumer_groups(&self) -> usize {
        self.consumer_groups.get()
    }

    fn validate(&self) {
        let cg_number = self.consumer_groups.get();
        let streams = self.streams.get();
        let mut cmd = IggyBenchArgs::command();

        if cg_number < streams {
            cmd.error(
                ErrorKind::ArgumentConflict,
                "Consumer groups number must be less than or equal to the number of streams.",
            )
            .exit();
        }

        let partitions = self.partitions.get();
        let consumers = self.consumers.get();

        if partitions < consumers {
            cmd.error(
                ErrorKind::ArgumentConflict,
                "Consumer number must be greater than the number of partitions.",
            )
            .exit();
        }
    }

    fn max_topic_size(&self) -> Option<IggyByteSize> {
        self.max_topic_size
    }
}
