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

//! Iggy SDK integration layer (`#3533`).
//!
//! Maps Kafka topics to Iggy streams/topics, exposes create-if-missing provisioning and
//! high-watermark lookups, and translates Iggy errors to Kafka wire error codes. Not yet wired
//! into the live Produce/Fetch dispatch path - that lands with `#3535`/`#3536`.

pub mod config;
pub mod error;
pub mod iggy_bridge;
pub mod topic_map;

pub use config::IggyBridgeConfig;
pub use error::BridgeError;
pub use iggy_bridge::IggyBridge;
pub use topic_map::{TopicMapping, TopicOverride};
