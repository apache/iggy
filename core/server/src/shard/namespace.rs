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

use std::hash::Hasher as _;
use iggy_common::Identifier;
use hash32::{Hasher, Murmur3Hasher};

//TODO: Will probably want to move it to separate crate so we can share it with sdk.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct IggyNamespace {
    pub(crate) stream_id: Identifier,
    pub(crate) topic_id: Identifier,
    pub(crate) partition_id: u32,
}

impl IggyNamespace {
    pub fn new(stream_id: Identifier, topic_id: Identifier, partition_id: u32) -> Self {
        Self {
            stream_id,
            topic_id,
            partition_id,
        }
    }

    pub fn generate_hash(&self) -> u32 {
        let mut hasher = Murmur3Hasher::default();
        hasher.write(&self.stream_id.value);
        hasher.write(&self.topic_id.value);
        hasher.write_u32(self.partition_id);
        hasher.finish32()
    }
}
