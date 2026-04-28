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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NamespaceCapacityError {
    ZeroStreams,
    ZeroTopics,
    ZeroPartitions,
    ExceedsU64 {
        required_bits: u32,
    },
    ExceedsLayout {
        stream_bits: u32,
        topic_bits: u32,
        partition_bits: u32,
    },
}

impl std::fmt::Display for NamespaceCapacityError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ZeroStreams => write!(f, "max_streams must be greater than 0"),
            Self::ZeroTopics => write!(f, "max_topics must be greater than 0"),
            Self::ZeroPartitions => write!(f, "max_partitions must be greater than 0"),
            Self::ExceedsU64 { required_bits } => write!(
                f,
                "namespace capacity requires {required_bits} bits, which does not fit in u64"
            ),
            Self::ExceedsLayout {
                stream_bits,
                topic_bits,
                partition_bits,
            } => write!(
                f,
                "namespace capacity requires stream/topic/partition bits of \
                 {stream_bits}/{topic_bits}/{partition_bits}, but IggyNamespace supports \
                 {STREAM_BITS}/{TOPIC_BITS}/{PARTITION_BITS}"
            ),
        }
    }
}

impl std::error::Error for NamespaceCapacityError {}

/// Packed namespace identifier for shard assignment.
///
/// Encodes stream_id (12 bits), topic_id (12 bits), and partition_id (20 bits)
/// into a single u64 for efficient hashing and routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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
    use super::{IggyNamespace, MAX_PARTITIONS, MAX_STREAMS, MAX_TOPICS, NamespaceCapacityError};

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
