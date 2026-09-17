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

//! Iggy SDK integration layer.
//!
//! Maps Kafka topics to Iggy streams/topics, exposes create-if-missing provisioning and
//! high-watermark lookups, and translates Iggy errors to Kafka wire error codes. Wired into
//! Metadata (`#3534`), `CreateTopics` (`#3538`) and `ListOffsets` (`#3537`); Produce/Fetch
//! dispatch is still a separate, later change (`#3535`/`#3536`).

pub mod config;
pub mod error;
pub mod iggy_bridge;
pub mod topic_map;

use async_trait::async_trait;

pub use config::IggyBridgeConfig;
pub use error::BridgeError;
pub use iggy_bridge::{IggyBridge, KafkaTopicMetadata};
pub use topic_map::{TopicMapping, TopicOverride};

/// Bridge operations the Kafka protocol handlers (`protocol::api`) call.
///
/// A trait seam, not a direct `IggyBridge` parameter: `IggyBridge` wraps a concrete `IggyClient`
/// with no test double of its own (its own `ensure_topic` doc explains why - no seam for a fake
/// that returns a specific `IggyError` on demand), so a handler written against the concrete type
/// could only ever be tested against a real, spawned `iggy-server`. Protocol-level tests (decode
/// edge cases, version firewall, wire-shape assertions) need to stay fast, in-process unit tests;
/// only the handful of tests that actually exercise bridge *behavior* need the real thing. Method
/// signatures mirror `IggyBridge`'s own inherent methods exactly, so `impl TopicCatalog for
/// IggyBridge` below is pure delegation - production code never goes through a vtable indirection
/// it didn't already pay for by definition (any real `IggyBridge` call round-trips over TCP).
#[async_trait]
pub trait TopicCatalog: Send + Sync {
    /// See [`IggyBridge::ensure_stream_and_topic`].
    async fn ensure_stream_and_topic(
        &self,
        kafka_topic: &str,
        partition_count: u32,
    ) -> Result<(), BridgeError>;

    /// See [`IggyBridge::high_watermarks`].
    async fn high_watermarks(
        &self,
        kafka_topic: &str,
        partitions: &[u32],
    ) -> Result<Vec<(u32, Result<i64, BridgeError>)>, BridgeError>;

    /// See [`IggyBridge::get_kafka_topic`].
    async fn get_kafka_topic(
        &self,
        kafka_topic: &str,
    ) -> Result<Option<KafkaTopicMetadata>, BridgeError>;

    /// See [`IggyBridge::list_kafka_topics`].
    async fn list_kafka_topics(&self) -> Result<Vec<KafkaTopicMetadata>, BridgeError>;
}

#[async_trait]
impl TopicCatalog for IggyBridge {
    async fn ensure_stream_and_topic(
        &self,
        kafka_topic: &str,
        partition_count: u32,
    ) -> Result<(), BridgeError> {
        Self::ensure_stream_and_topic(self, kafka_topic, partition_count).await
    }

    async fn high_watermarks(
        &self,
        kafka_topic: &str,
        partitions: &[u32],
    ) -> Result<Vec<(u32, Result<i64, BridgeError>)>, BridgeError> {
        Self::high_watermarks(self, kafka_topic, partitions).await
    }

    async fn get_kafka_topic(
        &self,
        kafka_topic: &str,
    ) -> Result<Option<KafkaTopicMetadata>, BridgeError> {
        Self::get_kafka_topic(self, kafka_topic).await
    }

    async fn list_kafka_topics(&self) -> Result<Vec<KafkaTopicMetadata>, BridgeError> {
        Self::list_kafka_topics(self).await
    }
}
