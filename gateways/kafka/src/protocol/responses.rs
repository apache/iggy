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
    CreateTopicsRequest, FetchRequest, ListOffsetsRequest, ProduceRequest,
};
use bytes::Bytes;

pub fn encode_produce_response(version: i16, req: &ProduceRequest) -> Bytes {
    let flexible = version >= 9;
    let mut e = Encoder::with_capacity(512);

    if flexible {
        e.write_varint((req.topics.len() + 1) as u64);
    } else {
        e.write_i32(i32::try_from(req.topics.len()).expect("topic count bounded"));
    }

    for topic in &req.topics {
        if flexible {
            e.write_compact_nullable_string(Some(&topic.topic));
        } else {
            let _ = e.write_nullable_string(Some(&topic.topic));
        }

        if flexible {
            e.write_varint((topic.partitions.len() + 1) as u64);
        } else {
            e.write_i32(i32::try_from(topic.partitions.len()).expect("partition count bounded"));
        }

        for p in &topic.partitions {
            e.write_i32(p.partition);
            e.write_i16(ERROR_NONE);
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
                    let _ = e.write_nullable_string(None);
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

pub fn encode_fetch_response(version: i16, req: &FetchRequest) -> Bytes {
    let flexible = version >= 12;
    let mut e = Encoder::with_capacity(512);

    if version >= 1 {
        e.write_i32(0);
    }
    if version >= 7 {
        e.write_i16(ERROR_NONE);
        e.write_i32(0);
    }

    if flexible {
        e.write_varint((req.topics.len() + 1) as u64);
    } else {
        e.write_i32(i32::try_from(req.topics.len()).expect("topic count bounded"));
    }

    for topic in &req.topics {
        if flexible {
            e.write_compact_nullable_string(Some(&topic.topic));
        } else {
            let _ = e.write_nullable_string(Some(&topic.topic));
        }

        if flexible {
            e.write_varint((topic.partitions.len() + 1) as u64);
        } else {
            e.write_i32(i32::try_from(topic.partitions.len()).expect("partition count bounded"));
        }

        for partition in &topic.partitions {
            e.write_i32(partition.partition);
            e.write_i16(ERROR_NONE);
            e.write_i64(0);
            if version >= 4 {
                e.write_i64(0);
            }
            if version >= 5 {
                e.write_i64(0);
            }
            if version >= 4 {
                if flexible {
                    e.write_varint(1);
                } else {
                    e.write_i32(0);
                }
            }
            if version >= 11 {
                e.write_i32(-1);
            }
            if flexible {
                e.write_compact_nullable_bytes(None);
            } else {
                e.write_nullable_bytes(None);
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

pub fn encode_list_offsets_response(version: i16, req: &ListOffsetsRequest) -> Bytes {
    let flexible = version >= 6;
    let mut e = Encoder::with_capacity(256);

    if version >= 2 {
        e.write_i32(0);
    }

    if flexible {
        e.write_varint((req.topics.len() + 1) as u64);
    } else {
        e.write_i32(i32::try_from(req.topics.len()).expect("topic count bounded"));
    }

    for topic in &req.topics {
        if flexible {
            e.write_compact_nullable_string(Some(&topic.topic));
        } else {
            let _ = e.write_nullable_string(Some(&topic.topic));
        }

        if flexible {
            e.write_varint((topic.partitions.len() + 1) as u64);
        } else {
            e.write_i32(i32::try_from(topic.partitions.len()).expect("partition count bounded"));
        }

        for partition in &topic.partitions {
            e.write_i32(partition.partition);
            e.write_i16(ERROR_NONE);

            let offset = 0i64;
            if version >= 1 {
                e.write_i64(1_700_000_000_000);
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

pub fn encode_create_topics_response(version: i16, req: &CreateTopicsRequest) -> Bytes {
    let flexible = version >= 5;
    let mut e = Encoder::with_capacity(256);

    if version >= 2 {
        e.write_i32(0);
    }

    if flexible {
        e.write_varint((req.topics.len() + 1) as u64);
    } else {
        e.write_i32(i32::try_from(req.topics.len()).expect("topic count bounded"));
    }

    for topic in &req.topics {
        if flexible {
            e.write_compact_nullable_string(Some(&topic.name));
        } else {
            let _ = e.write_nullable_string(Some(&topic.name));
        }

        let error_code = if topic.num_partitions <= 0 {
            ERROR_INVALID_PARTITIONS
        } else {
            ERROR_NONE
        };
        e.write_i16(error_code);

        if version >= 1 {
            if flexible {
                e.write_compact_nullable_string(None);
            } else {
                let _ = e.write_nullable_string(None);
            }
        }

        if version >= 5 {
            e.write_i16(ERROR_NONE);
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
