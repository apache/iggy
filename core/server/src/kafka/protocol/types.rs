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

use super::KafkaCodecError;
use super::codec::*;

// ---- ApiVersions (key=18) v0 ----

pub struct ApiVersionsRequest;

impl ApiVersionsRequest {
    pub fn decode(_buf: &[u8], _pos: &mut usize) -> Result<Self, KafkaCodecError> {
        Ok(Self)
    }
}

#[derive(Clone, Copy)]
pub struct ApiKeyVersion {
    pub api_key: i16,
    pub min_version: i16,
    pub max_version: i16,
}

pub struct ApiVersionsResponse {
    pub error_code: i16,
    pub api_keys: Vec<ApiKeyVersion>,
}

impl ApiVersionsResponse {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        write_i16(&mut buf, self.error_code);
        write_i32(&mut buf, self.api_keys.len() as i32);
        for k in &self.api_keys {
            write_i16(&mut buf, k.api_key);
            write_i16(&mut buf, k.min_version);
            write_i16(&mut buf, k.max_version);
        }
        buf
    }
}

// ---- Metadata (key=3) v1 ----

pub struct MetadataRequest {
    pub topics: Option<Vec<String>>,
}

impl MetadataRequest {
    pub fn decode(buf: &[u8], pos: &mut usize) -> Result<Self, KafkaCodecError> {
        let count = read_array_len(buf, pos)?;
        let topics = if count < 0 {
            None // null array = all topics
        } else {
            let mut v = Vec::with_capacity(safe_capacity(count, buf.len() - *pos));
            for _ in 0..count {
                v.push(read_string(buf, pos)?);
            }
            Some(v)
        };
        Ok(Self { topics })
    }
}

pub struct Broker {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}

pub struct PartitionMetadata {
    pub error_code: i16,
    pub partition_index: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}

pub struct TopicMetadata {
    pub error_code: i16,
    pub name: String,
    pub is_internal: bool,
    pub partitions: Vec<PartitionMetadata>,
}

pub struct MetadataResponse {
    pub brokers: Vec<Broker>,
    pub controller_id: i32,
    pub topics: Vec<TopicMetadata>,
}

impl MetadataResponse {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        write_i32(&mut buf, self.brokers.len() as i32);
        for b in &self.brokers {
            write_i32(&mut buf, b.node_id);
            write_string(&mut buf, &b.host);
            write_i32(&mut buf, b.port);
            write_nullable_string(&mut buf, b.rack.as_deref());
        }
        write_i32(&mut buf, self.controller_id);
        write_i32(&mut buf, self.topics.len() as i32);
        for t in &self.topics {
            write_i16(&mut buf, t.error_code);
            write_string(&mut buf, &t.name);
            write_i8(&mut buf, t.is_internal as i8);
            write_i32(&mut buf, t.partitions.len() as i32);
            for p in &t.partitions {
                write_i16(&mut buf, p.error_code);
                write_i32(&mut buf, p.partition_index);
                write_i32(&mut buf, p.leader);
                write_i32(&mut buf, p.replicas.len() as i32);
                for &r in &p.replicas {
                    write_i32(&mut buf, r);
                }
                write_i32(&mut buf, p.isr.len() as i32);
                for &i in &p.isr {
                    write_i32(&mut buf, i);
                }
            }
        }
        buf
    }
}

// ---- Produce (key=0) v3 ----

pub struct ProducePartitionData {
    pub index: i32,
    pub records: Option<Vec<u8>>,
}

pub struct ProduceTopicData {
    pub name: String,
    pub partition_data: Vec<ProducePartitionData>,
}

pub struct ProduceRequest {
    pub transactional_id: Option<String>,
    pub acks: i16,
    pub timeout_ms: i32,
    pub topic_data: Vec<ProduceTopicData>,
}

impl ProduceRequest {
    pub fn decode(buf: &[u8], pos: &mut usize) -> Result<Self, KafkaCodecError> {
        let transactional_id = read_nullable_string(buf, pos)?;
        let acks = read_i16(buf, pos)?;
        let timeout_ms = read_i32(buf, pos)?;
        let topic_count = read_array_len(buf, pos)?;
        let mut topic_data = Vec::with_capacity(safe_capacity(topic_count, buf.len() - *pos));
        for _ in 0..topic_count {
            let name = read_string(buf, pos)?;
            let part_count = read_array_len(buf, pos)?;
            let mut partition_data =
                Vec::with_capacity(safe_capacity(part_count, buf.len() - *pos));
            for _ in 0..part_count {
                let index = read_i32(buf, pos)?;
                let records = read_nullable_bytes(buf, pos)?;
                partition_data.push(ProducePartitionData { index, records });
            }
            topic_data.push(ProduceTopicData {
                name,
                partition_data,
            });
        }
        Ok(Self {
            transactional_id,
            acks,
            timeout_ms,
            topic_data,
        })
    }
}

pub struct ProducePartitionResponse {
    pub index: i32,
    pub error_code: i16,
    pub base_offset: i64,
    pub log_append_time_ms: i64,
}

pub struct ProduceTopicResponse {
    pub name: String,
    pub partition_responses: Vec<ProducePartitionResponse>,
}

pub struct ProduceResponse {
    pub responses: Vec<ProduceTopicResponse>,
    pub throttle_time_ms: i32,
}

impl ProduceResponse {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        write_i32(&mut buf, self.responses.len() as i32);
        for t in &self.responses {
            write_string(&mut buf, &t.name);
            write_i32(&mut buf, t.partition_responses.len() as i32);
            for p in &t.partition_responses {
                write_i32(&mut buf, p.index);
                write_i16(&mut buf, p.error_code);
                write_i64(&mut buf, p.base_offset);
                write_i64(&mut buf, p.log_append_time_ms);
            }
        }
        write_i32(&mut buf, self.throttle_time_ms);
        buf
    }
}

// ---- Fetch (key=1) v4 ----

pub struct FetchPartition {
    pub partition_index: i32,
    pub fetch_offset: i64,
    pub partition_max_bytes: i32,
}

pub struct FetchTopic {
    pub name: String,
    pub partitions: Vec<FetchPartition>,
}

pub struct FetchRequest {
    pub replica_id: i32,
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub isolation_level: i8,
    pub topics: Vec<FetchTopic>,
}

impl FetchRequest {
    pub fn decode(buf: &[u8], pos: &mut usize) -> Result<Self, KafkaCodecError> {
        let replica_id = read_i32(buf, pos)?;
        let max_wait_ms = read_i32(buf, pos)?;
        let min_bytes = read_i32(buf, pos)?;
        let max_bytes = read_i32(buf, pos)?;
        let isolation_level = read_i8(buf, pos)?;
        let topic_count = read_array_len(buf, pos)?;
        let mut topics = Vec::with_capacity(safe_capacity(topic_count, buf.len() - *pos));
        for _ in 0..topic_count {
            let name = read_string(buf, pos)?;
            let part_count = read_array_len(buf, pos)?;
            let mut partitions = Vec::with_capacity(safe_capacity(part_count, buf.len() - *pos));
            for _ in 0..part_count {
                partitions.push(FetchPartition {
                    partition_index: read_i32(buf, pos)?,
                    fetch_offset: read_i64(buf, pos)?,
                    partition_max_bytes: read_i32(buf, pos)?,
                });
            }
            topics.push(FetchTopic { name, partitions });
        }
        Ok(Self {
            replica_id,
            max_wait_ms,
            min_bytes,
            max_bytes,
            isolation_level,
            topics,
        })
    }
}

pub struct FetchPartitionResponse {
    pub partition_index: i32,
    pub error_code: i16,
    pub high_watermark: i64,
    pub last_stable_offset: i64,
    pub aborted_transactions: Option<Vec<AbortedTransaction>>,
    pub records: Option<Vec<u8>>,
}

pub struct AbortedTransaction {
    pub producer_id: i64,
    pub first_offset: i64,
}

pub struct FetchTopicResponse {
    pub name: String,
    pub partitions: Vec<FetchPartitionResponse>,
}

pub struct FetchResponse {
    pub throttle_time_ms: i32,
    pub responses: Vec<FetchTopicResponse>,
}

impl FetchResponse {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        write_i32(&mut buf, self.throttle_time_ms);
        write_i32(&mut buf, self.responses.len() as i32);
        for t in &self.responses {
            write_string(&mut buf, &t.name);
            write_i32(&mut buf, t.partitions.len() as i32);
            for p in &t.partitions {
                write_i32(&mut buf, p.partition_index);
                write_i16(&mut buf, p.error_code);
                write_i64(&mut buf, p.high_watermark);
                write_i64(&mut buf, p.last_stable_offset);
                match &p.aborted_transactions {
                    None => write_i32(&mut buf, -1),
                    Some(txns) => {
                        write_i32(&mut buf, txns.len() as i32);
                        for t in txns {
                            write_i64(&mut buf, t.producer_id);
                            write_i64(&mut buf, t.first_offset);
                        }
                    }
                }
                write_nullable_bytes(&mut buf, p.records.as_deref());
            }
        }
        buf
    }
}

// ---- ListOffsets (key=2) v1 ----

pub struct ListOffsetsPartition {
    pub partition_index: i32,
    pub timestamp: i64,
}

pub struct ListOffsetsTopic {
    pub name: String,
    pub partitions: Vec<ListOffsetsPartition>,
}

pub struct ListOffsetsRequest {
    pub replica_id: i32,
    pub topics: Vec<ListOffsetsTopic>,
}

impl ListOffsetsRequest {
    pub fn decode(buf: &[u8], pos: &mut usize) -> Result<Self, KafkaCodecError> {
        let replica_id = read_i32(buf, pos)?;
        let topic_count = read_array_len(buf, pos)?;
        let mut topics = Vec::with_capacity(safe_capacity(topic_count, buf.len() - *pos));
        for _ in 0..topic_count {
            let name = read_string(buf, pos)?;
            let part_count = read_array_len(buf, pos)?;
            let mut partitions = Vec::with_capacity(safe_capacity(part_count, buf.len() - *pos));
            for _ in 0..part_count {
                partitions.push(ListOffsetsPartition {
                    partition_index: read_i32(buf, pos)?,
                    timestamp: read_i64(buf, pos)?,
                });
            }
            topics.push(ListOffsetsTopic { name, partitions });
        }
        Ok(Self { replica_id, topics })
    }
}

pub struct ListOffsetsPartitionResponse {
    pub partition_index: i32,
    pub error_code: i16,
    pub timestamp: i64,
    pub offset: i64,
}

pub struct ListOffsetsTopicResponse {
    pub name: String,
    pub partitions: Vec<ListOffsetsPartitionResponse>,
}

pub struct ListOffsetsResponse {
    pub topics: Vec<ListOffsetsTopicResponse>,
}

impl ListOffsetsResponse {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        write_i32(&mut buf, self.topics.len() as i32);
        for t in &self.topics {
            write_string(&mut buf, &t.name);
            write_i32(&mut buf, t.partitions.len() as i32);
            for p in &t.partitions {
                write_i32(&mut buf, p.partition_index);
                write_i16(&mut buf, p.error_code);
                write_i64(&mut buf, p.timestamp);
                write_i64(&mut buf, p.offset);
            }
        }
        buf
    }
}

// ---- FindCoordinator (key=10) v0 ----

pub struct FindCoordinatorRequest {
    pub key: String,
}

impl FindCoordinatorRequest {
    pub fn decode(buf: &[u8], pos: &mut usize) -> Result<Self, KafkaCodecError> {
        Ok(Self {
            key: read_string(buf, pos)?,
        })
    }
}

pub struct FindCoordinatorResponse {
    pub error_code: i16,
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

impl FindCoordinatorResponse {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        write_i16(&mut buf, self.error_code);
        write_i32(&mut buf, self.node_id);
        write_string(&mut buf, &self.host);
        write_i32(&mut buf, self.port);
        buf
    }
}

// ---- CreateTopics (key=19) v0 ----

pub struct CreateTopicRequest {
    pub name: String,
    pub num_partitions: i32,
    pub replication_factor: i16,
}

pub struct CreateTopicsRequest {
    pub topics: Vec<CreateTopicRequest>,
    pub timeout_ms: i32,
}

impl CreateTopicsRequest {
    pub fn decode(buf: &[u8], pos: &mut usize) -> Result<Self, KafkaCodecError> {
        let count = read_array_len(buf, pos)?;
        let mut topics = Vec::with_capacity(safe_capacity(count, buf.len() - *pos));
        for _ in 0..count {
            let name = read_string(buf, pos)?;
            let num_partitions = read_i32(buf, pos)?;
            let replication_factor = read_i16(buf, pos)?;
            // replica_assignment array — skip
            let assign_count = read_array_len(buf, pos)?;
            for _ in 0..assign_count {
                let _partition_index = read_i32(buf, pos)?;
                let replica_count = read_array_len(buf, pos)?;
                for _ in 0..replica_count {
                    let _replica = read_i32(buf, pos)?;
                }
            }
            // config_entries array — skip
            let config_count = read_array_len(buf, pos)?;
            for _ in 0..config_count {
                let _key = read_string(buf, pos)?;
                let _val = read_nullable_string(buf, pos)?;
            }
            topics.push(CreateTopicRequest {
                name,
                num_partitions,
                replication_factor,
            });
        }
        let timeout_ms = read_i32(buf, pos)?;
        Ok(Self { topics, timeout_ms })
    }
}

pub struct CreateTopicResult {
    pub name: String,
    pub error_code: i16,
}

pub struct CreateTopicsResponse {
    pub topics: Vec<CreateTopicResult>,
}

impl CreateTopicsResponse {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        write_i32(&mut buf, self.topics.len() as i32);
        for t in &self.topics {
            write_string(&mut buf, &t.name);
            write_i16(&mut buf, t.error_code);
        }
        buf
    }
}

// ---- DeleteTopics (key=20) v0 ----

pub struct DeleteTopicsRequest {
    pub topic_names: Vec<String>,
    pub timeout_ms: i32,
}

impl DeleteTopicsRequest {
    pub fn decode(buf: &[u8], pos: &mut usize) -> Result<Self, KafkaCodecError> {
        let count = read_array_len(buf, pos)?;
        let mut topic_names = Vec::with_capacity(safe_capacity(count, buf.len() - *pos));
        for _ in 0..count {
            topic_names.push(read_string(buf, pos)?);
        }
        let timeout_ms = read_i32(buf, pos)?;
        Ok(Self {
            topic_names,
            timeout_ms,
        })
    }
}

pub struct DeleteTopicResult {
    pub name: String,
    pub error_code: i16,
}

pub struct DeleteTopicsResponse {
    pub topics: Vec<DeleteTopicResult>,
}

impl DeleteTopicsResponse {
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        write_i32(&mut buf, self.topics.len() as i32);
        for t in &self.topics {
            write_string(&mut buf, &t.name);
            write_i16(&mut buf, t.error_code);
        }
        buf
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_api_versions_round_trip() {
        let resp = ApiVersionsResponse {
            error_code: 0,
            api_keys: vec![
                ApiKeyVersion {
                    api_key: 0,
                    min_version: 0,
                    max_version: 3,
                },
                ApiKeyVersion {
                    api_key: 18,
                    min_version: 0,
                    max_version: 3,
                },
            ],
        };
        let encoded = resp.encode();
        let mut pos = 0;
        assert_eq!(read_i16(&encoded, &mut pos).unwrap(), 0); // error_code
        let count = read_i32(&encoded, &mut pos).unwrap();
        assert_eq!(count, 2);
        assert_eq!(read_i16(&encoded, &mut pos).unwrap(), 0); // api_key
        assert_eq!(read_i16(&encoded, &mut pos).unwrap(), 0); // min
        assert_eq!(read_i16(&encoded, &mut pos).unwrap(), 3); // max
        assert_eq!(read_i16(&encoded, &mut pos).unwrap(), 18);
        assert_eq!(read_i16(&encoded, &mut pos).unwrap(), 0);
        assert_eq!(read_i16(&encoded, &mut pos).unwrap(), 3);
        assert_eq!(pos, encoded.len());
    }

    #[test]
    fn test_metadata_request_decode() {
        let mut buf = Vec::new();
        write_i32(&mut buf, 2); // 2 topics
        write_string(&mut buf, "topic-a");
        write_string(&mut buf, "topic-b");

        let mut pos = 0;
        let req = MetadataRequest::decode(&buf, &mut pos).unwrap();
        let topics = req.topics.unwrap();
        assert_eq!(topics.len(), 2);
        assert_eq!(topics[0], "topic-a");
        assert_eq!(topics[1], "topic-b");
    }

    #[test]
    fn test_metadata_request_null_topics() {
        let mut buf = Vec::new();
        write_i32(&mut buf, -1); // null = all topics

        let mut pos = 0;
        let req = MetadataRequest::decode(&buf, &mut pos).unwrap();
        assert!(req.topics.is_none());
    }

    #[test]
    fn test_find_coordinator_round_trip() {
        // Decode request
        let mut buf = Vec::new();
        write_string(&mut buf, "my-group");
        let mut pos = 0;
        let req = FindCoordinatorRequest::decode(&buf, &mut pos).unwrap();
        assert_eq!(req.key, "my-group");

        // Encode response
        let resp = FindCoordinatorResponse {
            error_code: 0,
            node_id: 0,
            host: "127.0.0.1".to_string(),
            port: 9092,
        };
        let encoded = resp.encode();
        let mut pos = 0;
        assert_eq!(read_i16(&encoded, &mut pos).unwrap(), 0);
        assert_eq!(read_i32(&encoded, &mut pos).unwrap(), 0);
        assert_eq!(read_string(&encoded, &mut pos).unwrap(), "127.0.0.1");
        assert_eq!(read_i32(&encoded, &mut pos).unwrap(), 9092);
    }

    #[test]
    fn test_delete_topics_round_trip() {
        let mut buf = Vec::new();
        write_i32(&mut buf, 2);
        write_string(&mut buf, "t1");
        write_string(&mut buf, "t2");
        write_i32(&mut buf, 5000); // timeout_ms

        let mut pos = 0;
        let req = DeleteTopicsRequest::decode(&buf, &mut pos).unwrap();
        assert_eq!(req.topic_names, vec!["t1", "t2"]);
        assert_eq!(req.timeout_ms, 5000);

        let resp = DeleteTopicsResponse {
            topics: vec![
                DeleteTopicResult {
                    name: "t1".into(),
                    error_code: 0,
                },
                DeleteTopicResult {
                    name: "t2".into(),
                    error_code: 0,
                },
            ],
        };
        let encoded = resp.encode();
        let mut pos = 0;
        assert_eq!(read_i32(&encoded, &mut pos).unwrap(), 2);
        assert_eq!(read_string(&encoded, &mut pos).unwrap(), "t1");
        assert_eq!(read_i16(&encoded, &mut pos).unwrap(), 0);
    }
}
