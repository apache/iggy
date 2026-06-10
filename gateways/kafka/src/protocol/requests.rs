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

//! Kafka request decoders for critical API keys

#![allow(clippy::pedantic)]

use crate::error::{KafkaProtocolError, Result};
use crate::protocol::codec::Decoder;
use bytes::Bytes;

/// Produce Request (API Key 0)
#[derive(Debug, Clone)]
pub struct ProduceRequest {
    pub transactional_id: Option<String>,
    pub acks: i16,
    pub timeout_ms: i32,
    pub topics: Vec<ProduceTopicData>,
}

#[derive(Debug, Clone)]
pub struct ProduceTopicData {
    pub topic: String,
    pub partitions: Vec<ProducePartitionData>,
}

#[derive(Debug, Clone)]
pub struct ProducePartitionData {
    pub partition: i32,
    pub records: Option<Bytes>, // Raw RecordBatch bytes
}

pub fn decode_produce_request(version: i16, body: Bytes) -> Result<ProduceRequest> {
    let mut d = Decoder::new(body);
    let flexible = version >= 9;

    // transactional_id (v3+)
    let transactional_id = if version >= 3 {
        if flexible {
            d.read_compact_nullable_string()?
        } else {
            d.read_nullable_string()?
        }
    } else {
        None
    };

    let acks = d.read_i16()?;
    let timeout_ms = d.read_i32()?;

    // topics array
    let topics_count = if flexible {
        d.read_compact_array_count()?
    } else {
        d.read_i32_array_count()?
    };

    let mut topics = Vec::with_capacity(topics_count);
    for _ in 0..topics_count {
        let topic = if flexible {
            d.read_compact_nullable_string()?
                .ok_or(KafkaProtocolError::NullTopicName)?
        } else {
            d.read_nullable_string()?
                .ok_or(KafkaProtocolError::NullTopicName)?
        };

        let partitions_count = if flexible {
            d.read_compact_array_count()?
        } else {
            d.read_i32_array_count()?
        };

        let mut partitions = Vec::with_capacity(partitions_count);
        for _ in 0..partitions_count {
            let partition = d.read_i32()?;
            let records = if flexible {
                d.read_compact_nullable_bytes()?
            } else {
                d.read_nullable_bytes()?
            };
            partitions.push(ProducePartitionData { partition, records });
            if flexible {
                d.read_tagged_fields()?;
            }
        }

        topics.push(ProduceTopicData { topic, partitions });
        if flexible {
            d.read_tagged_fields()?;
        }
    }

    if flexible {
        d.read_tagged_fields()?;
    }

    Ok(ProduceRequest {
        transactional_id,
        acks,
        timeout_ms,
        topics,
    })
}

/// Fetch Request (API Key 1)
#[derive(Debug, Clone)]
pub struct FetchRequest {
    pub max_wait_ms: i32,
    pub min_bytes: i32,
    pub max_bytes: i32,
    pub isolation_level: i8,
    pub topics: Vec<FetchTopic>,
}

#[derive(Debug, Clone)]
pub struct FetchTopic {
    pub topic: String,
    pub partitions: Vec<FetchPartition>,
}

#[derive(Debug, Clone)]
pub struct FetchPartition {
    pub partition: i32,
    pub fetch_offset: i64,
    pub partition_max_bytes: i32,
}

pub fn decode_fetch_request(version: i16, body: Bytes) -> Result<FetchRequest> {
    let mut d = Decoder::new(body);
    let flexible = version >= 12;

    let _replica_id = d.read_i32()?;
    let max_wait_ms = d.read_i32()?;
    let min_bytes = d.read_i32()?;

    let max_bytes = if version >= 3 {
        d.read_i32()?
    } else {
        52_428_800 // default 50MB
    };

    let isolation_level = if version >= 4 { d.read_i8()? } else { 0 };

    // session_id and session_epoch (v7+) — read and discard (stub path)
    if version >= 7 {
        d.read_i32()?; // session_id
        d.read_i32()?; // session_epoch
    }

    // topics array
    let topics_count = if flexible {
        d.read_compact_array_count()?
    } else {
        d.read_i32_array_count()?
    };

    let mut topics = Vec::with_capacity(topics_count);
    for _ in 0..topics_count {
        let topic = if flexible {
            d.read_compact_nullable_string()?
                .ok_or(KafkaProtocolError::NullTopicName)?
        } else {
            d.read_nullable_string()?
                .ok_or(KafkaProtocolError::NullTopicName)?
        };

        let partitions_count = if flexible {
            d.read_compact_array_count()?
        } else {
            d.read_i32_array_count()?
        };

        let mut partitions = Vec::with_capacity(partitions_count);
        for _ in 0..partitions_count {
            let partition = d.read_i32()?;

            if version >= 9 {
                d.read_i32()?; // current_leader_epoch
            }

            let fetch_offset = d.read_i64()?;

            if version >= 12 {
                d.read_i32()?; // last_fetched_epoch
            }

            if version >= 5 {
                d.read_i64()?; // log_start_offset
            }

            let partition_max_bytes = d.read_i32()?;

            partitions.push(FetchPartition {
                partition,
                fetch_offset,
                partition_max_bytes,
            });

            if flexible {
                d.read_tagged_fields()?;
            }
        }

        topics.push(FetchTopic { topic, partitions });
        if flexible {
            d.read_tagged_fields()?;
        }
    }

    // forgotten_topics_data (v7+) — skip
    if version >= 7 {
        let forgotten_count = if flexible {
            d.read_compact_array_count()?
        } else {
            d.read_i32_array_count()?
        };
        for _ in 0..forgotten_count {
            if flexible {
                d.read_compact_nullable_string()?;
                let partitions_count = d.read_compact_array_count()?;
                for _ in 0..partitions_count {
                    d.read_i32()?;
                }
                d.read_tagged_fields()?;
            } else {
                d.read_nullable_string()?;
                let partitions_count = d.read_i32_array_count()?;
                for _ in 0..partitions_count {
                    d.read_i32()?;
                }
            }
        }
    }

    // rack_id (v11+)
    if version >= 11 {
        if flexible {
            d.read_compact_nullable_string()?;
        } else {
            d.read_nullable_string()?;
        }
    }

    if flexible {
        d.read_tagged_fields()?;
    }

    Ok(FetchRequest {
        max_wait_ms,
        min_bytes,
        max_bytes,
        isolation_level,
        topics,
    })
}

/// ListOffsets Request (API Key 2)
#[derive(Debug, Clone)]
pub struct ListOffsetsRequest {
    pub isolation_level: i8,
    pub topics: Vec<ListOffsetsTopic>,
}

#[derive(Debug, Clone)]
pub struct ListOffsetsTopic {
    pub topic: String,
    pub partitions: Vec<ListOffsetsPartition>,
}

#[derive(Debug, Clone)]
pub struct ListOffsetsPartition {
    pub partition: i32,
    pub timestamp: i64, // -2 = earliest, -1 = latest
}

pub fn decode_list_offsets_request(version: i16, body: Bytes) -> Result<ListOffsetsRequest> {
    let mut d = Decoder::new(body);
    let flexible = version >= 6;

    let _replica_id = d.read_i32()?;

    let isolation_level = if version >= 2 { d.read_i8()? } else { 0 };

    let topics_count = if flexible {
        d.read_compact_array_count()?
    } else {
        d.read_i32_array_count()?
    };

    let mut topics = Vec::with_capacity(topics_count);
    for _ in 0..topics_count {
        let topic = if flexible {
            d.read_compact_nullable_string()?
                .ok_or(KafkaProtocolError::NullTopicName)?
        } else {
            d.read_nullable_string()?
                .ok_or(KafkaProtocolError::NullTopicName)?
        };

        let partitions_count = if flexible {
            d.read_compact_array_count()?
        } else {
            d.read_i32_array_count()?
        };

        let mut partitions = Vec::with_capacity(partitions_count);
        for _ in 0..partitions_count {
            let partition = d.read_i32()?;

            if version >= 4 {
                d.read_i32()?; // current_leader_epoch
            }

            let timestamp = d.read_i64()?;

            if version == 0 {
                d.read_i32()?; // max_num_offsets (deprecated)
            }

            partitions.push(ListOffsetsPartition {
                partition,
                timestamp,
            });

            if flexible {
                d.read_tagged_fields()?;
            }
        }

        topics.push(ListOffsetsTopic { topic, partitions });
        if flexible {
            d.read_tagged_fields()?;
        }
    }

    if flexible {
        d.read_tagged_fields()?;
    }

    Ok(ListOffsetsRequest {
        isolation_level,
        topics,
    })
}

/// CreateTopics Request (API Key 19)
#[derive(Debug, Clone)]
pub struct CreateTopicsRequest {
    pub topics: Vec<CreatableTopic>,
    pub timeout_ms: i32,
    pub validate_only: bool,
}

#[derive(Debug, Clone)]
pub struct CreatableTopic {
    pub name: String,
    pub num_partitions: i32,
    pub replication_factor: i16,
}

pub fn decode_create_topics_request(version: i16, body: Bytes) -> Result<CreateTopicsRequest> {
    let mut d = Decoder::new(body);
    let flexible = version >= 5;

    let topics_count = if flexible {
        d.read_compact_array_count()?
    } else {
        d.read_i32_array_count()?
    };

    let mut topics = Vec::with_capacity(topics_count);
    for _ in 0..topics_count {
        let name = if flexible {
            d.read_compact_nullable_string()?
                .ok_or(KafkaProtocolError::NullTopicName)?
        } else {
            d.read_nullable_string()?
                .ok_or(KafkaProtocolError::NullTopicName)?
        };

        let num_partitions = d.read_i32()?;
        let replication_factor = d.read_i16()?;

        // assignments (COMPACT_ARRAY or ARRAY) — skip
        let assignments_count = if flexible {
            d.read_compact_array_count()?
        } else {
            d.read_i32_array_count()?
        };
        for _ in 0..assignments_count {
            d.read_i32()?; // partition_index
            let replicas_count = if flexible {
                d.read_compact_array_count()?
            } else {
                d.read_i32_array_count()?
            };
            for _ in 0..replicas_count {
                d.read_i32()?; // broker_id
            }
            if flexible {
                d.read_tagged_fields()?;
            }
        }

        // configs (COMPACT_ARRAY or ARRAY) — skip
        let configs_count = if flexible {
            d.read_compact_array_count()?
        } else {
            d.read_i32_array_count()?
        };
        for _ in 0..configs_count {
            if flexible {
                d.read_compact_nullable_string()?; // name
                d.read_compact_nullable_string()?; // value
                d.read_tagged_fields()?;
            } else {
                d.read_nullable_string()?;
                d.read_nullable_string()?;
            }
        }

        topics.push(CreatableTopic {
            name,
            num_partitions,
            replication_factor,
        });

        if flexible {
            d.read_tagged_fields()?;
        }
    }

    let timeout_ms = d.read_i32()?;
    let validate_only = if version >= 1 { d.read_bool()? } else { false };

    if flexible {
        d.read_tagged_fields()?;
    }

    Ok(CreateTopicsRequest {
        topics,
        timeout_ms,
        validate_only,
    })
}
