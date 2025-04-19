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

use super::examples::print_examples;
use super::kinds::balanced::producer::BalancedProducerArgs;
use super::kinds::balanced::producer_and_consumer_group::BalancedProducerAndConsumerGroupArgs;
use super::kinds::end_to_end::producing_consumer::EndToEndProducingConsumerArgs;
use super::kinds::end_to_end::producing_consumer_group::EndToEndProducingConsumerGroupArgs;
use super::props::BenchmarkKindProps;
use super::transport::BenchmarkTransportCommand;
use crate::args::kinds::balanced::consumer_group::BalancedConsumerGroupArgs;
use crate::args::kinds::pinned::consumer::PinnedConsumerArgs;
use crate::args::kinds::pinned::producer::PinnedProducerArgs;
use crate::args::kinds::pinned::producer_and_consumer::PinnedProducerAndConsumerArgs;
use clap::Subcommand;
use iggy::utils::byte_size::IggyByteSize;
use iggy_bench_report::benchmark_kind::BenchmarkKind;

#[derive(Subcommand, Debug)]
pub enum BenchmarkKindCommand {
    #[command(
        about = "Pinned producer benchmark",
        long_about = "N producers sending to N separated stream-topic with single partition (one stream per one producer)",
        visible_alias = "pp",
        verbatim_doc_comment
    )]
    PinnedProducer(PinnedProducerArgs),

    #[command(
        about = "Pinned consumer benchmark",
        visible_alias = "pc",
        verbatim_doc_comment
    )]
    PinnedConsumer(PinnedConsumerArgs),

    #[command(
        about = "Pinned producer and consumer benchmark",
        long_about = "N consumers polling from N separated stream-topic with single partition (one stream per one consumer)",
        visible_alias = "ppc",
        verbatim_doc_comment
    )]
    PinnedProducerAndConsumer(PinnedProducerAndConsumerArgs),

    #[command(
        about = "Balanced producer benchmark",
        long_about = "N producers sending to M partitions in K streams with balanced partitioning kind",
        visible_alias = "bp",
        verbatim_doc_comment
    )]
    BalancedProducer(BalancedProducerArgs),

    #[command(
        about = "Balanced consumer group benchmark",
        long_about = "N consumers assigned to M consumer groups polling from K partitions in L streams",
        visible_alias = "bcg",
        verbatim_doc_comment
    )]
    BalancedConsumerGroup(BalancedConsumerGroupArgs),

    #[command(
        about = "N producers sending to M partitions in K streams, L consumers polling from P consumer groups",
        visible_alias = "bpcg",
        verbatim_doc_comment
    )]
    BalancedProducerAndConsumerGroup(BalancedProducerAndConsumerGroupArgs),

    #[command(
        about = "N producing consumers sending and polling to/from M streams",
        visible_alias = "e2e",
        verbatim_doc_comment
    )]
    EndToEndProducingConsumer(EndToEndProducingConsumerArgs),

    #[command(
        about = "N producing consumers assigned to M consumer groups sending and polling to/from K streams",
        visible_alias = "e2ecg",
        verbatim_doc_comment
    )]
    EndToEndProducingConsumerGroup(EndToEndProducingConsumerGroupArgs),

    #[command(about = "Print examples", visible_alias = "e", verbatim_doc_comment)]
    Examples,
}

impl BenchmarkKindCommand {
    pub fn as_simple_kind(&self) -> BenchmarkKind {
        match self {
            BenchmarkKindCommand::PinnedProducer(_) => BenchmarkKind::PinnedProducer,
            BenchmarkKindCommand::PinnedConsumer(_) => BenchmarkKind::PinnedConsumer,
            BenchmarkKindCommand::PinnedProducerAndConsumer(_) => {
                BenchmarkKind::PinnedProducerAndConsumer
            }
            BenchmarkKindCommand::BalancedProducer(_) => BenchmarkKind::BalancedProducer,
            BenchmarkKindCommand::BalancedConsumerGroup(_) => BenchmarkKind::BalancedConsumerGroup,
            BenchmarkKindCommand::BalancedProducerAndConsumerGroup(_) => {
                BenchmarkKind::BalancedProducerAndConsumerGroup
            }
            BenchmarkKindCommand::EndToEndProducingConsumer(_) => {
                BenchmarkKind::EndToEndProducingConsumer
            }
            BenchmarkKindCommand::EndToEndProducingConsumerGroup(_) => {
                BenchmarkKind::EndToEndProducingConsumerGroup
            }
            BenchmarkKindCommand::Examples => {
                print_examples();
                std::process::exit(0);
            }
        }
    }
}

impl BenchmarkKindProps for BenchmarkKindCommand {
    fn streams(&self) -> u32 {
        self.inner().streams()
    }

    fn partitions(&self) -> u32 {
        self.inner().partitions()
    }

    fn consumers(&self) -> u32 {
        self.inner().consumers()
    }

    fn producers(&self) -> u32 {
        self.inner().producers()
    }

    fn transport_command(&self) -> &BenchmarkTransportCommand {
        self.inner().transport_command()
    }

    fn number_of_consumer_groups(&self) -> u32 {
        self.inner().number_of_consumer_groups()
    }

    fn max_topic_size(&self) -> Option<IggyByteSize> {
        self.inner().max_topic_size()
    }

    fn inner(&self) -> &dyn BenchmarkKindProps {
        match self {
            BenchmarkKindCommand::PinnedProducer(args) => args,
            BenchmarkKindCommand::PinnedConsumer(args) => args,
            BenchmarkKindCommand::PinnedProducerAndConsumer(args) => args,
            BenchmarkKindCommand::BalancedProducer(args) => args,
            BenchmarkKindCommand::BalancedConsumerGroup(args) => args,
            BenchmarkKindCommand::BalancedProducerAndConsumerGroup(args) => args,
            BenchmarkKindCommand::EndToEndProducingConsumer(args) => args,
            BenchmarkKindCommand::EndToEndProducingConsumerGroup(args) => args,
            BenchmarkKindCommand::Examples => {
                print_examples();
                std::process::exit(0);
            }
        }
    }

    fn validate(&self) {
        self.inner().validate()
    }
}
