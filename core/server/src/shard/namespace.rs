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

use crate::slab::partitions;
use iggy_common::Identifier;

// Packed namespace layout (works only on 64bit platforms, but we won't support 32bit anyway)
// +----------------+----------------+----------------+----------------+
// |    stream_id   |    topic_id    |  partition_id  |     unused     |
// |    STREAM_BITS |    TOPIC_BITS  | PARTITION_BITS |  (64 - total)  |
// +----------------+----------------+----------------+----------------+

// TODO Use consts from the `slab` module.
pub const MAX_STREAMS: usize = 4096;
pub const MAX_TOPICS: usize = 4096;
pub const MAX_PARTITIONS: usize = 1_000_000;

const fn bits_required(mut n: u64) -> u32 {
    if n == 0 {
        return 1;
    }
    let mut b = 0;
    while n > 0 {
        b += 1;
        n >>= 1;
    }
    b
}

pub const STREAM_BITS: u32 = bits_required((MAX_STREAMS - 1) as u64);
pub const TOPIC_BITS: u32 = bits_required((MAX_TOPICS - 1) as u64);
pub const PARTITION_BITS: u32 = bits_required((MAX_PARTITIONS - 1) as u64);

pub const PARTITION_SHIFT: u32 = 0;
pub const TOPIC_SHIFT: u32 = PARTITION_SHIFT + PARTITION_BITS;
pub const STREAM_SHIFT: u32 = TOPIC_SHIFT + TOPIC_BITS;

pub const PARTITION_MASK: u64 = (1u64 << PARTITION_BITS) - 1;
pub const TOPIC_MASK: u64 = (1u64 << TOPIC_BITS) - 1;
pub const STREAM_MASK: u64 = (1u64 << STREAM_BITS) - 1;

#[derive(Debug)]
pub struct IggyFullNamespace {
    stream: Identifier,
    topic: Identifier,
    partition: partitions::ContainerId,
}

impl IggyFullNamespace {
    pub fn new(stream: Identifier, topic: Identifier, partition: partitions::ContainerId) -> Self {
        Self {
            stream,
            topic,
            partition,
        }
    }

    pub fn stream_id(&self) -> &Identifier {
        &self.stream
    }

    pub fn topic_id(&self) -> &Identifier {
        &self.topic
    }

    pub fn partition_id(&self) -> partitions::ContainerId {
        self.partition
    }

    pub fn decompose(self) -> (Identifier, Identifier, partitions::ContainerId) {
        (self.stream, self.topic, self.partition)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IggyNamespace(u64);

impl IggyNamespace {
    #[inline]
    pub fn inner(&self) -> u64 {
        self.0
    }

    #[inline]
    pub fn stream_id(&self) -> usize {
        ((self.0 >> STREAM_SHIFT) & STREAM_MASK) as usize
    }

    #[inline]
    pub fn topic_id(&self) -> usize {
        ((self.0 >> TOPIC_SHIFT) & TOPIC_MASK) as usize
    }

    #[inline]
    pub fn partition_id(&self) -> usize {
        ((self.0 >> PARTITION_SHIFT) & PARTITION_MASK) as usize
    }

    #[inline]
    pub fn new(stream: usize, topic: usize, partition: usize) -> Self {
        let value = ((stream as u64) & STREAM_MASK) << STREAM_SHIFT
            | ((topic as u64) & TOPIC_MASK) << TOPIC_SHIFT
            | ((partition as u64) & PARTITION_MASK) << PARTITION_SHIFT;
        Self(value)
    }
}
