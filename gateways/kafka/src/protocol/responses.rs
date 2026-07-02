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

//! Kafka response encoders (stub implementations).

#![allow(clippy::pedantic)]

use crate::protocol::api::{ERROR_INVALID_PARTITIONS, ERROR_NONE};
use crate::protocol::codec::Encoder;
use crate::protocol::requests::{
    CreateTopicsRequest, FetchRequest, ListOffsetsRequest, ProducePartitionData, ProduceRequest,
    ProduceTopicData,
};
use bytes::Bytes;

/// Well-formed Produce response with a single placeholder topic/partition.
pub fn encode_produce_error_response(version: i16, error_code: i16) -> Bytes {
    let topics = vec![ProduceTopicData {
        topic: String::new(), // TODO topic name will be populated in the end to end functional completion
        partitions: vec![ProducePartitionData {
            partition: 0,
            records: None,
        }],
    }];
    encode_produce_response_inner(version, &topics, error_code)
}

pub fn encode_produce_response(version: i16, req: &ProduceRequest) -> Bytes {
    encode_produce_response_inner(version, &req.topics, ERROR_NONE)
}

fn encode_produce_response_inner(
    version: i16,
    topics: &[ProduceTopicData],
    partition_error: i16,
) -> Bytes {
    let flexible = version >= 9;
    let mut e = Encoder::with_capacity(512);

    if flexible {
        e.write_varint((topics.len() + 1) as u64);
    } else {
        e.write_i32(i32::try_from(topics.len()).expect("topic count bounded"));
    }

    for topic in topics {
        if flexible {
            e.write_compact_nullable_string(Some(&topic.topic));
        } else {
            e.write_nullable_string_unchecked(Some(&topic.topic));
        }

        if flexible {
            e.write_varint((topic.partitions.len() + 1) as u64);
        } else {
            e.write_i32(i32::try_from(topic.partitions.len()).expect("partition count bounded"));
        }

        for p in &topic.partitions {
            e.write_i32(p.partition);
            e.write_i16(partition_error);
            e.write_i64(0);
            if version >= 2 {
                e.write_i64(-1);
            }
            if version >= 5 {
                e.write_i64(0);
            }
            if version >= 8 {
                if flexible {
                    e.write_varint(1);
                    e.write_compact_nullable_string(None);
                } else {
                    e.write_i32(0);
                    e.write_nullable_string_unchecked(None);
                }
            }
            if flexible {
                e.write_empty_tagged_fields();
            }
        }

        if flexible {
            e.write_empty_tagged_fields();
        }
    }

    if version >= 1 {
        e.write_i32(0);
    }
    if flexible {
        e.write_empty_tagged_fields();
    }

    e.freeze()
}

/// Well-formed Fetch response. Uses top-level `error_code` at v7+, or a single
/// placeholder topic/partition with per-partition `error_code` below v7.
pub fn encode_fetch_error_response(version: i16, error_code: i16) -> Bytes {
    use crate::protocol::requests::{FetchPartition, FetchTopic};

    if version >= 7 {
        return encode_fetch_response_inner(version, &[], Some(error_code), error_code);
    }

    let topics = vec![FetchTopic {
        topic: String::new(),
        partitions: vec![FetchPartition {
            partition: 0,
            fetch_offset: 0,
            partition_max_bytes: 1,
        }],
    }];
    encode_fetch_response_inner(version, &topics, Some(ERROR_NONE), error_code)
}

pub fn encode_fetch_response(version: i16, req: &FetchRequest) -> Bytes {
    encode_fetch_response_inner(version, &req.topics, Some(ERROR_NONE), ERROR_NONE)
}

fn encode_fetch_response_inner(
    version: i16,
    topics: &[crate::protocol::requests::FetchTopic],
    top_level_error: Option<i16>,
    partition_error: i16,
) -> Bytes {
    let flexible = version >= 12;
    let mut e = Encoder::with_capacity(512);

    if version >= 1 {
        e.write_i32(0);
    }
    if version >= 7 {
        e.write_i16(top_level_error.unwrap_or(ERROR_NONE));
        e.write_i32(0);
    }

    if flexible {
        e.write_varint((topics.len() + 1) as u64);
    } else {
        e.write_i32(i32::try_from(topics.len()).expect("topic count bounded"));
    }

    for topic in topics {
        if flexible {
            e.write_compact_nullable_string(Some(&topic.topic));
        } else {
            e.write_nullable_string_unchecked(Some(&topic.topic));
        }

        if flexible {
            e.write_varint((topic.partitions.len() + 1) as u64);
        } else {
            e.write_i32(i32::try_from(topic.partitions.len()).expect("partition count bounded"));
        }

        for partition in &topic.partitions {
            e.write_i32(partition.partition);
            e.write_i16(partition_error);
            e.write_i64(0); // high_watermark
            if version >= 4 {
                e.write_i64(0); // last_stable_offset
            }
            if version >= 5 {
                e.write_i64(0); // log_start_offset
            }
            if version >= 4 {
                if flexible {
                    e.write_varint(1); // empty aborted_transactions
                } else {
                    e.write_i32(0); // empty aborted_transactions
                }
            }
            if version >= 11 {
                e.write_i32(-1); // preferred_read_replica
            }
            if flexible {
                e.write_compact_nullable_bytes(None);
            } else {
                e.write_null_bytes();
            }
            if flexible {
                e.write_empty_tagged_fields();
            }
        }

        if flexible {
            e.write_empty_tagged_fields();
        }
    }

    if flexible {
        e.write_empty_tagged_fields();
    }

    e.freeze()
}

/// Well-formed ListOffsets response with a single placeholder topic/partition.
pub fn encode_list_offsets_error_response(version: i16, error_code: i16) -> Bytes {
    use crate::protocol::requests::{ListOffsetsPartition, ListOffsetsTopic};

    let topics = vec![ListOffsetsTopic {
        topic: String::new(),
        partitions: vec![ListOffsetsPartition {
            partition: 0,
            timestamp: -1,
        }],
    }];
    encode_list_offsets_response_inner(version, &topics, error_code)
}

pub fn encode_list_offsets_response(version: i16, req: &ListOffsetsRequest) -> Bytes {
    encode_list_offsets_response_inner(version, &req.topics, ERROR_NONE)
}

fn encode_list_offsets_response_inner(
    version: i16,
    topics: &[crate::protocol::requests::ListOffsetsTopic],
    partition_error: i16,
) -> Bytes {
    let flexible = version >= 6;
    let mut e = Encoder::with_capacity(256);

    if version >= 2 {
        e.write_i32(0);
    }

    if flexible {
        e.write_varint((topics.len() + 1) as u64);
    } else {
        e.write_i32(i32::try_from(topics.len()).expect("topic count bounded"));
    }

    for topic in topics {
        if flexible {
            e.write_compact_nullable_string(Some(&topic.topic));
        } else {
            e.write_nullable_string_unchecked(Some(&topic.topic));
        }

        if flexible {
            e.write_varint((topic.partitions.len() + 1) as u64);
        } else {
            e.write_i32(i32::try_from(topic.partitions.len()).expect("partition count bounded"));
        }

        for partition in &topic.partitions {
            e.write_i32(partition.partition);
            e.write_i16(partition_error);

            let offset = 0i64;
            if version >= 1 {
                e.write_i64(-1); // -1 = timestamp not available (Kafka sentinel)
            }
            e.write_i64(offset);
            if version >= 4 {
                e.write_i32(-1);
            }
            if flexible {
                e.write_empty_tagged_fields();
            }
        }

        if flexible {
            e.write_empty_tagged_fields();
        }
    }

    if flexible {
        e.write_empty_tagged_fields();
    }

    e.freeze()
}

/// Well-formed CreateTopics response with a single placeholder topic.
pub fn encode_create_topics_error_response(version: i16, error_code: i16) -> Bytes {
    use crate::protocol::requests::CreatableTopic;

    let topics = vec![CreatableTopic {
        name: String::new(),
        num_partitions: 1,
        replication_factor: 1,
    }];
    encode_create_topics_response_inner(version, &topics, error_code)
}

pub fn encode_create_topics_response(version: i16, req: &CreateTopicsRequest) -> Bytes {
    encode_create_topics_response_inner(version, &req.topics, ERROR_NONE)
}

fn encode_create_topics_response_inner(
    version: i16,
    topics: &[crate::protocol::requests::CreatableTopic],
    topic_error: i16,
) -> Bytes {
    let flexible = version >= 5;
    let mut e = Encoder::with_capacity(256);

    if version >= 2 {
        e.write_i32(0);
    }

    if flexible {
        e.write_varint((topics.len() + 1) as u64);
    } else {
        e.write_i32(i32::try_from(topics.len()).expect("topic count bounded"));
    }

    for topic in topics {
        if flexible {
            e.write_compact_nullable_string(Some(&topic.name));
        } else {
            e.write_nullable_string_unchecked(Some(&topic.name));
        }

        let error_code = if topic_error != ERROR_NONE {
            topic_error
        } else if topic.num_partitions <= 0 {
            ERROR_INVALID_PARTITIONS
        } else {
            ERROR_NONE
        };
        e.write_i16(error_code);

        if version >= 1 {
            if flexible {
                e.write_compact_nullable_string(None);
            } else {
                e.write_nullable_string_unchecked(None);
            }
        }

        if version >= 5 {
            e.write_i32(topic.num_partitions);
            e.write_i16(topic.replication_factor);
            e.write_varint(1);
        }

        if flexible {
            e.write_empty_tagged_fields();
        }
    }

    if flexible {
        e.write_empty_tagged_fields();
    }

    e.freeze()
}
