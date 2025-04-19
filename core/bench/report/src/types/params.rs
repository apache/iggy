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

use super::{benchmark_kind::BenchmarkKind, transport::BenchmarkTransport};
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
    pub messages_per_batch: u32,
    pub message_batches: u32,
    pub message_size: u32,
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
            BenchmarkKind::PinnedProducer => format!("{} producers", self.producers),
            BenchmarkKind::PinnedConsumer => format!("{} consumers", self.consumers),
            BenchmarkKind::PinnedProducerAndConsumer => {
                format!("{} producers/{} consumers", self.producers, self.consumers)
            }
            BenchmarkKind::BalancedProducer => format!("{} producers", self.producers),
            BenchmarkKind::BalancedConsumerGroup => format!(
                "{} consumers/{} consumer groups",
                self.consumers, self.consumer_groups
            ),
            BenchmarkKind::BalancedProducerAndConsumerGroup => {
                format!("{} producers/{} consumers", self.producers, self.consumers)
            }
            BenchmarkKind::EndToEndProducingConsumer => {
                format!("{} producing consumers", self.producers)
            }
            BenchmarkKind::EndToEndProducingConsumerGroup => {
                format!(
                    "{} producing consumers/{} consumer groups",
                    self.producers, self.consumer_groups
                )
            }
        }
    }
}
