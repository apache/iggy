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

use super::{
    benchmark_kind::BenchmarkKind, numeric_parameter::BenchmarkNumericParameter,
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
    pub messages_per_batch: BenchmarkNumericParameter,
    pub message_batches: usize,
    pub message_size: BenchmarkNumericParameter,
    pub producers: usize,
    pub consumers: usize,
    pub streams: usize,
    pub partitions: usize,
    pub consumer_groups: usize,
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
