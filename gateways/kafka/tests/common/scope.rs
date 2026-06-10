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

//! Shared scope constants — compiled into each integration test binary via `#[path]`.
#![allow(dead_code)]

use iggy_gateway_kafka::protocol::api::BrokerAdvertise;

/// Scoped API keys exercised by the #3421 regression suite.
pub const SCOPED_API_KEYS: &[(i16, &str, i16, i16)] = &[
    (0, "Produce", 3, 9),
    (1, "Fetch", 4, 12),
    (2, "ListOffsets", 1, 6),
    (3, "Metadata", 0, 9),
    (18, "ApiVersions", 0, 3),
    (19, "CreateTopics", 2, 5),
];

pub fn default_broker() -> BrokerAdvertise {
    BrokerAdvertise::default()
}
