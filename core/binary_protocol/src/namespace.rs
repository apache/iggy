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

//! Wire-format namespace routing constants.
//!
//! Both the SDK encoder (which writes `RequestHeader.namespace`) and the
//! server-side sharding layer (which hashes it to a shard) must agree on
//! how stream/topic/partition triples pack into the namespace `u64`. Any
//! drift between the two silently routes writes to the wrong shard, so the
//! single source of truth lives here in the wire-format crate that both
//! sides already depend on.

pub const MAX_STREAMS: usize = 4096;
pub const MAX_TOPICS: usize = 4096;
pub const MAX_PARTITIONS: usize = 1_000_000;

#[must_use]
pub const fn bits_required(mut n: u64) -> u32 {
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

/// Total bits used by the packed namespace layout (stream | topic | partition).
/// Any `u64` whose set bits all sit within this range is a legal packable namespace.
pub const PACKED_NAMESPACE_BITS: u32 = STREAM_BITS + TOPIC_BITS + PARTITION_BITS;

/// Largest `u64` value producible by the packed layout.
/// Equivalent to `(1 << PACKED_NAMESPACE_BITS) - 1`.
pub const PACKED_NAMESPACE_MAX: u64 = (1u64 << PACKED_NAMESPACE_BITS) - 1;

/// Reserved consensus-namespace identifier for the cluster's metadata replica.
///
/// The packed layout uses only bits `0..PACKED_NAMESPACE_BITS`, so the top
/// bit is unreachable from any packed namespace value. Routers distinguish
/// metadata's single global consensus group from per-partition consensus
/// groups by value alone.
pub const METADATA_CONSENSUS_NAMESPACE: u64 = 1u64 << 63;
