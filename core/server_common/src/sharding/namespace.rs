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

// Packed namespace layout
// +----------------+----------------+----------------+----------------+
// |    stream_id   |    topic_id    |  partition_id  |     unused     |
// |    STREAM_BITS |    TOPIC_BITS  | PARTITION_BITS |  (64 - total)  |
// +----------------+----------------+----------------+----------------+
//
// The layout constants live in `iggy_binary_protocol::namespace` because
// both the SDK encoder and this server-side router depend on them and they
// MUST stay in lockstep -- any drift silently routes writes to the wrong
// shard. Re-exported here for ergonomics of existing call sites.

pub use iggy_binary_protocol::namespace::{
    MAX_PARTITIONS, MAX_STREAMS, MAX_TOPICS, METADATA_CONSENSUS_NAMESPACE, PACKED_NAMESPACE_BITS,
    PACKED_NAMESPACE_MAX, PARTITION_BITS, PARTITION_MASK, PARTITION_SHIFT, STREAM_BITS,
    STREAM_MASK, STREAM_SHIFT, TOPIC_BITS, TOPIC_MASK, TOPIC_SHIFT, bits_required,
};
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum NamespaceCapacityError {
    #[error("max_streams must be greater than 0")]
    ZeroStreams,
    #[error("max_topics must be greater than 0")]
    ZeroTopics,
    #[error("max_partitions must be greater than 0")]
    ZeroPartitions,
    #[error("namespace capacity requires {required_bits} bits, which does not fit in u64")]
    ExceedsU64 { required_bits: u32 },
    #[error(
        "namespace capacity requires stream/topic/partition bits of {stream_bits}/{topic_bits}/{partition_bits}, but IggyNamespace supports {STREAM_BITS}/{TOPIC_BITS}/{PARTITION_BITS}"
    )]
    ExceedsLayout {
        stream_bits: u32,
        topic_bits: u32,
        partition_bits: u32,
    },
}

/// Packed namespace identifier for shard assignment.
///
/// Encodes stream_id (12 bits), topic_id (12 bits), and partition_id (20 bits)
/// into a single u64 for efficient hashing and routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct IggyNamespace(u64);

impl IggyNamespace {
    #[inline]
    pub fn from_raw(value: u64) -> Self {
        Self(value)
    }

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

    /// Returns `true` when `value` could have been produced by [`Self::new`].
    /// False for the metadata sentinel and for any `u64` with bits set above
    /// the packed range.
    #[inline]
    #[must_use]
    pub const fn is_packable(value: u64) -> bool {
        value <= PACKED_NAMESPACE_MAX
    }

    pub fn validate_capacity(
        max_streams: usize,
        max_topics: usize,
        max_partitions: usize,
    ) -> Result<(), NamespaceCapacityError> {
        let stream_bits = if max_streams == 0 {
            return Err(NamespaceCapacityError::ZeroStreams);
        } else {
            bits_required((max_streams - 1) as u64)
        };
        let topic_bits = if max_topics == 0 {
            return Err(NamespaceCapacityError::ZeroTopics);
        } else {
            bits_required((max_topics - 1) as u64)
        };
        let partition_bits = if max_partitions == 0 {
            return Err(NamespaceCapacityError::ZeroPartitions);
        } else {
            bits_required((max_partitions - 1) as u64)
        };

        let required_bits = stream_bits
            .checked_add(topic_bits)
            .and_then(|bits| bits.checked_add(partition_bits))
            .ok_or(NamespaceCapacityError::ExceedsU64 {
                required_bits: u32::MAX,
            })?;
        if required_bits > u64::BITS {
            return Err(NamespaceCapacityError::ExceedsU64 { required_bits });
        }

        if stream_bits > STREAM_BITS || topic_bits > TOPIC_BITS || partition_bits > PARTITION_BITS {
            return Err(NamespaceCapacityError::ExceedsLayout {
                stream_bits,
                topic_bits,
                partition_bits,
            });
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        IggyNamespace, MAX_PARTITIONS, MAX_STREAMS, MAX_TOPICS, METADATA_CONSENSUS_NAMESPACE,
        NamespaceCapacityError, PACKED_NAMESPACE_BITS, PACKED_NAMESPACE_MAX,
    };

    // Both static facts (sentinel above the packed range; bit layout matches
    // module constants) are checked at compile time so any future tweak to
    // STREAM_BITS / TOPIC_BITS / PARTITION_BITS that closes the gap between
    // the packed range and the sentinel will fail to build.
    const _: () = {
        assert!(METADATA_CONSENSUS_NAMESPACE > PACKED_NAMESPACE_MAX);
        assert!(PACKED_NAMESPACE_BITS == 12 + 12 + 20);
        assert!(PACKED_NAMESPACE_MAX == (1u64 << PACKED_NAMESPACE_BITS) - 1);
    };

    #[test]
    fn metadata_sentinel_cannot_collide_with_any_packable_namespace() {
        assert!(!IggyNamespace::is_packable(METADATA_CONSENSUS_NAMESPACE));

        // The (0, 0, 0) corner is intentionally a legal partition, which is
        // precisely why `0` is unsuitable as the metadata sentinel.
        let zero = IggyNamespace::new(0, 0, 0);
        assert_eq!(zero.inner(), 0);
        assert!(IggyNamespace::is_packable(zero.inner()));
        assert_ne!(zero.inner(), METADATA_CONSENSUS_NAMESPACE);

        // Maximum packable triple stays inside the packed range.
        let max = IggyNamespace::new(MAX_STREAMS - 1, MAX_TOPICS - 1, MAX_PARTITIONS - 1);
        assert!(IggyNamespace::is_packable(max.inner()));
        assert_ne!(max.inner(), METADATA_CONSENSUS_NAMESPACE);
    }

    #[test]
    fn validates_default_namespace_capacity() {
        assert!(IggyNamespace::validate_capacity(MAX_STREAMS, MAX_TOPICS, MAX_PARTITIONS).is_ok());
    }

    #[test]
    fn rejects_zero_capacity_values() {
        assert_eq!(
            IggyNamespace::validate_capacity(0, MAX_TOPICS, MAX_PARTITIONS),
            Err(NamespaceCapacityError::ZeroStreams)
        );
        assert_eq!(
            IggyNamespace::validate_capacity(MAX_STREAMS, 0, MAX_PARTITIONS),
            Err(NamespaceCapacityError::ZeroTopics)
        );
        assert_eq!(
            IggyNamespace::validate_capacity(MAX_STREAMS, MAX_TOPICS, 0),
            Err(NamespaceCapacityError::ZeroPartitions)
        );
    }

    #[test]
    fn rejects_capacity_that_exceeds_current_layout() {
        let err = IggyNamespace::validate_capacity(MAX_STREAMS + 1, MAX_TOPICS, MAX_PARTITIONS);
        assert!(matches!(
            err,
            Err(NamespaceCapacityError::ExceedsLayout { .. })
        ));
    }
}
