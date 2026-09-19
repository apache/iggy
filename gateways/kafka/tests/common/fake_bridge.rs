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

//! In-memory [`TopicCatalog`] for protocol-level tests (decode edge cases, version firewall,
//! wire-shape assertions) that don't need a real `iggy-server` - only the dispatch layer is
//! under test in those files, not bridge behavior itself. `bridge_iggy_integration_tests.rs` and
//! `gateway_bridge_e2e_tests.rs` exercise real `IggyBridge` behavior against a spawned server.

#![allow(dead_code)] // Not every test file that includes this module uses every method.

use std::collections::HashMap;
use std::sync::Mutex;

use async_trait::async_trait;
use iggy::prelude::IggyError;
use iggy_gateway_kafka::bridge::{BridgeError, KafkaTopicMetadata, TopicCatalog};

/// One topic's fake state: partition count, plus a single watermark applied to every partition
/// (real `IggyBridge` tracks watermarks per-partition, but no test in this suite needs more than
/// one distinguishable value per topic to tell LATEST from EARLIEST).
#[derive(Clone, Copy)]
struct TopicState {
    partitions_count: u32,
    /// What `high_watermarks` reports for every partition of this topic. Distinct from `0` by
    /// default specifically so a test seeding a nonzero watermark can tell "the handler read the
    /// real watermark" apart from "the handler (or this fake) always answers 0 regardless" - a
    /// fake that only ever returns `Ok(0)` (this type's original shape) makes every EARLIEST/
    /// LATEST assertion pass identically whether the underlying logic is correct or not.
    watermark: i64,
}

/// Topics this fake already knows about, keyed by Kafka topic name. Seeded via
/// [`FakeBridge::with_topic`]/[`FakeBridge::with_topic_and_watermark`]; `ensure_stream_and_topic`
/// also inserts into it (watermark `0`), mirroring `IggyBridge`'s own create-if-missing contract
/// closely enough for wire-level assertions.
#[derive(Default)]
pub struct FakeBridge {
    topics: Mutex<HashMap<String, TopicState>>,
}

impl FakeBridge {
    pub fn new() -> Self {
        Self::default()
    }

    /// Seeds a topic with watermark `0` for every partition - equivalent to
    /// `with_topic_and_watermark(kafka_topic, partitions_count, 0)`. Kept as its own method: most
    /// callers only care about partition count/existence, not a specific watermark value.
    #[must_use]
    pub fn with_topic(self, kafka_topic: &str, partitions_count: u32) -> Self {
        self.with_topic_and_watermark(kafka_topic, partitions_count, 0)
    }

    #[must_use]
    pub fn with_topic_and_watermark(
        self,
        kafka_topic: &str,
        partitions_count: u32,
        watermark: i64,
    ) -> Self {
        self.topics
            .lock()
            .expect("fake bridge mutex poisoned")
            .insert(
                kafka_topic.to_string(),
                TopicState {
                    partitions_count,
                    watermark,
                },
            );
        self
    }
}

#[async_trait]
impl TopicCatalog for FakeBridge {
    async fn ensure_stream_and_topic(
        &self,
        kafka_topic: &str,
        partition_count: u32,
    ) -> Result<(), BridgeError> {
        let existing = {
            let mut topics = self.topics.lock().expect("fake bridge mutex poisoned");
            let existing = topics.get(kafka_topic).map(|state| state.partitions_count);
            if existing.is_none() {
                topics.insert(
                    kafka_topic.to_string(),
                    TopicState {
                        partitions_count: partition_count,
                        watermark: 0,
                    },
                );
            }
            existing
        };
        match existing {
            Some(existing) if existing != partition_count => {
                Err(BridgeError::PartitionCountMismatch {
                    topic: kafka_topic.to_string(),
                    existing,
                    requested: partition_count,
                })
            }
            _ => Ok(()),
        }
    }

    async fn high_watermarks(
        &self,
        kafka_topic: &str,
        partitions: &[u32],
    ) -> Result<Vec<(u32, Result<i64, BridgeError>)>, BridgeError> {
        let state = {
            let topics = self.topics.lock().expect("fake bridge mutex poisoned");
            topics.get(kafka_topic).copied()
        };
        let Some(state) = state else {
            return Err(BridgeError::Iggy(IggyError::TopicNameNotFound(
                kafka_topic.to_string(),
                "kafka".to_string(),
            )));
        };
        Ok(partitions
            .iter()
            .map(|&partition| {
                let result = if partition < state.partitions_count {
                    Ok(state.watermark)
                } else {
                    Err(BridgeError::PartitionOutOfRange {
                        topic: kafka_topic.to_string(),
                        partition,
                        partitions_count: state.partitions_count,
                    })
                };
                (partition, result)
            })
            .collect())
    }

    async fn get_kafka_topic(
        &self,
        kafka_topic: &str,
    ) -> Result<Option<KafkaTopicMetadata>, BridgeError> {
        let partitions_count = {
            let topics = self.topics.lock().expect("fake bridge mutex poisoned");
            topics.get(kafka_topic).map(|state| state.partitions_count)
        };
        Ok(partitions_count.map(|partitions_count| KafkaTopicMetadata {
            kafka_topic: kafka_topic.to_string(),
            partitions_count,
        }))
    }

    async fn list_kafka_topics(&self) -> Result<Vec<KafkaTopicMetadata>, BridgeError> {
        let snapshot: Vec<(String, u32)> = {
            let topics = self.topics.lock().expect("fake bridge mutex poisoned");
            topics
                .iter()
                .map(|(kafka_topic, state)| (kafka_topic.clone(), state.partitions_count))
                .collect()
        };
        Ok(snapshot
            .into_iter()
            .map(|(kafka_topic, partitions_count)| KafkaTopicMetadata {
                kafka_topic,
                partitions_count,
            })
            .collect())
    }
}
