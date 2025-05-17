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

use derive_more::Display;
use serde::{Deserialize, Serialize};

#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    Hash,
    Display,
    Serialize,
    Deserialize,
    Default,
    PartialOrd,
    Ord,
)]
pub enum BenchmarkKind {
    #[display("Pinned Producer")]
    #[serde(rename = "pinned_producer")]
    PinnedProducer,
    #[display("Pinned Consumer")]
    #[serde(rename = "pinned_consumer")]
    #[default]
    PinnedConsumer,
    #[display("Pinned Producer And Consumer")]
    #[serde(rename = "pinned_producer_and_consumer")]
    PinnedProducerAndConsumer,
    #[display("Balanced Producer")]
    #[serde(rename = "balanced_producer")]
    BalancedProducer,
    #[display("Balanced Consumer Group")]
    #[serde(rename = "balanced_consumer_group")]
    BalancedConsumerGroup,
    #[display("Balanced Producer And Consumer Group")]
    #[serde(rename = "balanced_producer_and_consumer_group")]
    BalancedProducerAndConsumerGroup,
    #[display("End To End Producing Consumer")]
    #[serde(rename = "end_to_end_producing_consumer")]
    EndToEndProducingConsumer,
    #[display("End To End Producing Consumer Group")]
    #[serde(rename = "end_to_end_producing_consumer_group")]
    EndToEndProducingConsumerGroup,
}
