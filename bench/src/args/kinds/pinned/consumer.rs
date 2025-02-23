use crate::args::{
    common::IggyBenchArgs, defaults::*, props::BenchmarkKindProps,
    transport::BenchmarkTransportCommand,
};
use clap::{error::ErrorKind, CommandFactory, Parser};
use iggy::utils::byte_size::IggyByteSize;
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
