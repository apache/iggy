// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::{
    benchmark_kind::BenchmarkKind, numeric_parameter::BenchmarkNumericParameter,
    options::BenchmarkOptions, transport::BenchmarkTransport,
};
use serde::{Deserialize, Serialize};

/// Fields after `params_identifier` are `None` in reports written before they existed.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Default)]
pub struct BenchmarkParams {
    pub benchmark_kind: BenchmarkKind,
    pub transport: BenchmarkTransport,
    pub server_address: String,
    pub remark: Option<String>,
    pub extra_info: Option<String>,
    pub gitref: Option<String>,
    pub gitref_date: Option<String>,
    pub messages_per_batch: BenchmarkNumericParameter,
    /// Batches that completed, summed over all actors. `data_volume` holds the configured
    /// amount, which is the same for every run of one workload.
    pub message_batches: u64,
    pub message_size: BenchmarkNumericParameter,
    pub producers: u32,
    pub consumers: u32,
    pub streams: u32,
    pub partitions: u32,
    pub consumer_groups: u32,
    pub rate_limit: Option<String>,
    pub pretty_name: String,
    pub bench_command: String,
    pub params_identifier: String,
    #[serde(default)]
    pub options: Option<BenchmarkOptions>,
    #[serde(default)]
    pub data_volume: Option<BenchmarkDataVolume>,
    /// `--suite` label.
    #[serde(default)]
    pub suite: Option<String>,
    /// `--workload` label. Inside one suite, one label names one workload.
    #[serde(default)]
    pub workload: Option<String>,
    /// The arguments as typed, quoted for a POSIX shell, with the password value replaced
    /// by `***`.
    #[serde(default)]
    pub command_line: Option<String>,
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

/// Configured stop condition of a run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BenchmarkDataVolume {
    /// `--message-batches`, for each actor.
    MessageBatchesPerActor(u32),
    /// `--total-data`, for all actors together.
    TotalDataBytes(u64),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::{
        BenchmarkClientOptions, BenchmarkConsumerOptions, BenchmarkDurability,
        BenchmarkMaxTopicSize, BenchmarkMessageExpiry, BenchmarkTcpOptions, BenchmarkTopicOptions,
    };
    use serde_json::{Value, json};

    /// `params` of a live 0.9.0 cloud-bench report, written before the options existed.
    const PARAMS_BEFORE_OPTIONS: &str = r#"{
        "benchmark_kind": "balanced_producer",
        "transport": "tcp",
        "server_address": "10.0.0.121:8090",
        "remark": "bp-090-71f29618b-aws_i4i-cluster3@800MB",
        "extra_info": null,
        "gitref": "71f29618b",
        "gitref_date": "2026-09-18T21:49:29.135974+00:00",
        "messages_per_batch": 250,
        "message_batches": 248004,
        "message_size": 1000,
        "producers": 5,
        "consumers": 0,
        "streams": 1,
        "partitions": 20,
        "consumer_groups": 0,
        "rate_limit": "800.00 MB",
        "pretty_name": "5 producers, 1000B msgs, 250 msgs/batch (bp-090-71f29618b-aws_i4i-cluster3@800MB)",
        "bench_command": "iggy-bench --durability replicated --consumer-offset-durability replicated --messages-per-batch 250 --message-batches 49600 --rate-limit '800.00 MB' balanced-producer --producers 5 --partitions 20 tcp --server-address 10.0.0.121:8090 output -o performance_results --remark 'bp-090-71f29618b-aws_i4i-cluster3@800MB'",
        "params_identifier": "Balanced Producer_TCP_bp-090-71f29618b-aws_i4i-cluster3@800MB_250_49600_1000_5_0_1_20_0_cluster3"
    }"#;

    #[test]
    fn given_params_written_before_options_when_parsed_should_leave_new_fields_empty() {
        let params: BenchmarkParams = serde_json::from_str(PARAMS_BEFORE_OPTIONS).unwrap();

        assert_eq!(params.benchmark_kind, BenchmarkKind::BalancedProducer);
        assert_eq!(params.message_batches, 248_004);
        assert_eq!(params.options, None);
        assert_eq!(params.data_volume, None);
        assert_eq!(params.suite, None);
        assert_eq!(params.workload, None);
        assert_eq!(params.command_line, None);
    }

    #[test]
    fn given_every_new_field_set_when_round_tripped_should_keep_values_and_json_shape() {
        let params = BenchmarkParams {
            options: Some(BenchmarkOptions {
                topic: BenchmarkTopicOptions {
                    durability: BenchmarkDurability::Persisted,
                    messages_required_to_save: Some(128),
                    max_topic_size: BenchmarkMaxTopicSize::Bytes(1_073_741_824),
                    message_expiry: BenchmarkMessageExpiry::AfterUs(3_600_000_000),
                },
                consumer: Some(BenchmarkConsumerOptions {
                    consumer_offset_durability: BenchmarkDurability::Persisted,
                    max_wait_us: 250_000,
                    min_count: 10,
                    max_bytes: Some(2_097_152),
                    read_amplification: Some(1.05),
                }),
                client: BenchmarkClientOptions {
                    warmup_time_us: 5_000_000,
                    high_level_api: true,
                    reuse_streams: true,
                },
                tcp: BenchmarkTcpOptions {
                    nodelay: true,
                    tls: true,
                },
            }),
            data_volume: Some(BenchmarkDataVolume::TotalDataBytes(1_048_576)),
            suite: Some("i4i-single".to_owned()),
            workload: Some("producer-20p-persisted@800MB".to_owned()),
            command_line: Some("iggy-bench --password '***' pinned-producer tcp".to_owned()),
            ..BenchmarkParams::default()
        };

        // Reports are written as text, which keeps `1.05_f32` exact, unlike `to_value`.
        let text = serde_json::to_string(&params).unwrap();
        let json: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(
            json["options"],
            json!({
                "topic": {
                    "durability": "persisted",
                    "messages_required_to_save": 128,
                    "max_topic_size": {"bytes": 1_073_741_824_u64},
                    "message_expiry": {"after_us": 3_600_000_000_u64}
                },
                "consumer": {
                    "consumer_offset_durability": "persisted",
                    "max_wait_us": 250_000,
                    "min_count": 10,
                    "max_bytes": 2_097_152,
                    "read_amplification": 1.05
                },
                "client": {"warmup_time_us": 5_000_000, "high_level_api": true, "reuse_streams": true},
                "tcp": {"nodelay": true, "tls": true}
            })
        );
        assert_eq!(json["data_volume"], json!({"total_data_bytes": 1_048_576}));

        let decoded: BenchmarkParams = serde_json::from_str(&text).unwrap();
        assert_eq!(decoded, params);
    }

    #[test]
    fn given_legacy_producer_options_when_serialized_should_write_unit_variants_as_strings() {
        let options = BenchmarkOptions::legacy(BenchmarkKind::PinnedProducer);

        let json: Value = serde_json::from_str(&serde_json::to_string(&options).unwrap()).unwrap();
        assert_eq!(json["topic"]["max_topic_size"], "unlimited");
        assert_eq!(json["topic"]["message_expiry"], "never_expire");
        assert_eq!(json["consumer"], Value::Null);
    }
}
