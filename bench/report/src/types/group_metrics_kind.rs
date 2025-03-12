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

use derive_more::derive::Display;
use serde::{Deserialize, Serialize};

/// The kind of group metrics to be displayed
#[derive(Debug, Copy, Clone, Eq, Hash, PartialEq, Serialize, Deserialize, Display)]
pub enum GroupMetricsKind {
    #[display("Producers")]
    #[serde(rename = "producers")]
    Producers,
    #[display("Consumers")]
    #[serde(rename = "consumers")]
    Consumers,
    #[display("Producers and Consumers")]
    #[serde(rename = "producers_and_consumers")]
    ProducersAndConsumers,
    #[display("Producing Consumers")]
    #[serde(rename = "producing_consumers")]
    ProducingConsumers,
}

impl GroupMetricsKind {
    pub fn actor(&self) -> &str {
        match self {
            GroupMetricsKind::Producers => "Producer",
            GroupMetricsKind::Consumers => "Consumer",
            GroupMetricsKind::ProducersAndConsumers => "Actor",
            GroupMetricsKind::ProducingConsumers => "Producing Consumer",
        }
    }
}
