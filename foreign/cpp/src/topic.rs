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

use crate::ffi;
use iggy::prelude::Topic as RustTopic;

impl From<RustTopic> for ffi::Topic {
    fn from(topic: RustTopic) -> Self {
        ffi::Topic {
            id: topic.id,
            created_at: topic.created_at.as_micros(),
            name: topic.name,
            size_bytes: topic.size.as_bytes_u64(),
            message_expiry: u64::from(topic.message_expiry),
            compression_algorithm: topic.compression_algorithm.to_string(),
            max_topic_size: u64::from(topic.max_topic_size),
            replication_factor: topic.replication_factor,
            messages_count: topic.messages_count,
            partitions_count: topic.partitions_count,
        }
    }
}
