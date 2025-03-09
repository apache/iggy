use super::{
    benchmark_kind::BenchmarkKind, numeric_parameter::IggyBenchNumericParameter,
    transport::BenchmarkTransport,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
pub struct BenchmarkParams {
    pub benchmark_kind: BenchmarkKind,
    pub transport: BenchmarkTransport,
    pub server_address: String,
    pub remark: Option<String>,
    pub extra_info: Option<String>,
    pub gitref: Option<String>,
    pub gitref_date: Option<String>,
    pub messages_per_batch: IggyBenchNumericParameter,
    pub message_batches: u64,
    pub message_size: IggyBenchNumericParameter,
    pub producers: u32,
    pub consumers: u32,
    pub streams: u32,
    pub partitions: u32,
    pub consumer_groups: u32,
    pub rate_limit: Option<String>,
    pub pretty_name: String,
    pub bench_command: String,
    pub params_identifier: String,
}

impl BenchmarkParams {
    pub fn format_actors_info(&self) -> String {
        match self.benchmark_kind {
            BenchmarkKind::PinnedProducer => format!("{} Producers", self.producers),
            BenchmarkKind::PinnedConsumer => format!("{} Consumers", self.consumers),
            BenchmarkKind::PinnedProducerAndConsumer => {
                format!("{} Producers/{} Consumers", self.producers, self.consumers)
            }
            BenchmarkKind::BalancedProducer => format!("{} Producers", self.producers),
            BenchmarkKind::BalancedConsumerGroup => format!(
                "{} Consumers/{} Consumer Groups",
                self.consumers, self.consumer_groups
            ),
            BenchmarkKind::BalancedProducerAndConsumerGroup => {
                format!(
                    "{} Producers/{} Consumer Groups",
                    self.producers, self.consumer_groups
                )
            }
            BenchmarkKind::EndToEndProducingConsumer => {
                format!("{} Producing Consumers", self.producers)
            }
            BenchmarkKind::EndToEndProducingConsumerGroup => {
                format!(
                    "{} Producing Consumers/{} Consumer Groups",
                    self.producers, self.consumer_groups
                )
            }
        }
    }
}
