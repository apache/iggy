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

/// Topics this fake already knows about, keyed by Kafka topic name. Seeded via
/// [`FakeBridge::with_topic`]; `ensure_stream_and_topic` also inserts into it, mirroring
/// `IggyBridge`'s own create-if-missing contract closely enough for wire-level assertions.
#[derive(Default)]
pub struct FakeBridge {
    topics: Mutex<HashMap<String, u32>>,
}

impl FakeBridge {
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_topic(self, kafka_topic: &str, partitions_count: u32) -> Self {
        self.topics
            .lock()
            .expect("fake bridge mutex poisoned")
            .insert(kafka_topic.to_string(), partitions_count);
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
            let existing = topics.get(kafka_topic).copied();
            if existing.is_none() {
                topics.insert(kafka_topic.to_string(), partition_count);
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
        let partitions_count = {
            let topics = self.topics.lock().expect("fake bridge mutex poisoned");
            topics.get(kafka_topic).copied()
        };
        let Some(partitions_count) = partitions_count else {
            return Err(BridgeError::Iggy(IggyError::TopicNameNotFound(
                kafka_topic.to_string(),
                "kafka".to_string(),
            )));
        };
        Ok(partitions
            .iter()
            .map(|&partition| {
                let result = if partition < partitions_count {
                    Ok(0)
                } else {
                    Err(BridgeError::PartitionOutOfRange {
                        topic: kafka_topic.to_string(),
                        partition,
                        partitions_count,
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
            topics.get(kafka_topic).copied()
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
                .map(|(kafka_topic, &partitions_count)| (kafka_topic.clone(), partitions_count))
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
