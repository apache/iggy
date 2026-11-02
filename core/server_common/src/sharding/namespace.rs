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

// Packed namespace layout, drawn most-significant bit first.
//
//  bit 63     62 ......... 52   51 ....... 32   31 ..... 20   19 ......... 0
// +----------+-----------------+--------------+-------------+--------------+
// | METADATA |     unused      |  stream_id   |  topic_id   | partition_id |
// |  _GROUP  | (63 - total)    | STREAM_BITS  | TOPIC_BITS  | PARTITION_.. |
// +----------+-----------------+--------------+-------------+--------------+
//
// Slack sits between the highest field and the sentinel, not below partition.
// Stream is highest, so it is the only field that widens without re-encoding
// every value already on the wire.
//
// The layout constants live in `iggy_binary_protocol::namespace` because
// both the SDK encoder and this server-side router depend on them and they
// MUST stay in lockstep -- any drift silently routes writes to the wrong
// shard. Re-exported here for ergonomics of existing call sites.

pub use iggy_binary_protocol::namespace::{
    MAX_PARTITIONS, MAX_STREAMS, MAX_TOPICS, METADATA_GROUP, PACKED_NAMESPACE_BITS,
    PACKED_NAMESPACE_MAX, PARTITION_BITS, PARTITION_MASK, PARTITION_SHIFT, STREAM_BITS,
    STREAM_MASK, STREAM_SHIFT, TOPIC_BITS, TOPIC_MASK, TOPIC_SHIFT,
};
/// Packed namespace identifier for shard assignment.
///
/// Packs stream, topic and partition ids into one u64 for hashing and routing.
/// Widths are compile-asserted in `iggy_binary_protocol`. Do not restate them
/// here, where they go stale unnoticed.
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

    /// Packs a committed `(stream, topic, partition)` slab address.
    ///
    /// # Panics
    /// If any component is at or above its declared maximum. Metadata admission
    /// rejects such a create before it commits, so reaching here means an
    /// invariant is already broken. Masking, which this used to do, aliases two
    /// partitions onto one consensus group, shard, directory and authorization
    /// scope, silently and unrecoverably. Untrusted or recovered ids want
    /// [`Self::try_new`].
    #[inline]
    #[must_use]
    pub fn new(stream: usize, topic: usize, partition: usize) -> Self {
        Self::try_new(stream, topic, partition).unwrap_or_else(|| {
            panic!(
                "namespace ({stream}, {topic}, {partition}) exceeds the packed layout \
                 ({MAX_STREAMS}, {MAX_TOPICS}, {MAX_PARTITIONS})"
            )
        })
    }

    /// Fallible [`Self::new`]. `None` when any component is at or above its
    /// declared maximum. Bounds compare against the `MAX_*` constants, not the
    /// field masks: `MAX_PARTITIONS` is not a power of two, so its mask admits
    /// ids the maximum forbids.
    #[inline]
    #[must_use]
    pub fn try_new(stream: usize, topic: usize, partition: usize) -> Option<Self> {
        if stream >= MAX_STREAMS || topic >= MAX_TOPICS || partition >= MAX_PARTITIONS {
            return None;
        }
        Some(Self(
            (stream as u64) << STREAM_SHIFT
                | (topic as u64) << TOPIC_SHIFT
                | (partition as u64) << PARTITION_SHIFT,
        ))
    }

    /// `true` when `value` has no bits set above the packed range, so false for
    /// the metadata sentinel.
    ///
    /// Strictly LOOSER than [`Self::new`], which also rejects any component at
    /// or above its maximum: 48,576 partition-field values per `(stream, topic)`
    /// pass here and can never come out of `new`. Not an admission gate.
    #[inline]
    #[must_use]
    pub const fn is_packable(value: u64) -> bool {
        value <= PACKED_NAMESPACE_MAX
    }
}

#[cfg(test)]
mod tests {
    use super::{
        IggyNamespace, MAX_PARTITIONS, MAX_STREAMS, MAX_TOPICS, METADATA_GROUP,
        PACKED_NAMESPACE_BITS, PACKED_NAMESPACE_MAX,
    };

    /// Byte-exact packings that MUST NOT drift. The first four are reproduced
    /// from the old 12/12/20 layout, so altering one re-encodes every group id
    /// on the wire. The last two are only representable since the widening.
    const GOLDEN: &[(usize, usize, usize, u64)] = &[
        (0, 0, 0, 0x0),
        (5, 2, 7, 0x5_0020_0007),
        (1, 0, 0, 0x1_0000_0000),
        (4095, 4095, 999_999, 0xFFF_FFFF_423F),
        (4096, 0, 0, 0x1000_0000_0000),
        (
            MAX_STREAMS - 1,
            MAX_TOPICS - 1,
            MAX_PARTITIONS - 1,
            0xF_FFFF_FFFF_423F,
        ),
    ];

    const _: () = {
        assert!(METADATA_GROUP > PACKED_NAMESPACE_MAX);
        assert!(PACKED_NAMESPACE_MAX == (1u64 << PACKED_NAMESPACE_BITS) - 1);
    };

    #[test]
    fn given_known_triples_when_packing_should_match_the_golden_vectors() {
        for &(stream, topic, partition, expected) in GOLDEN {
            let namespace = IggyNamespace::new(stream, topic, partition);
            assert_eq!(
                namespace.inner(),
                expected,
                "({stream}, {topic}, {partition}) packed to {:#x}, expected {expected:#x}",
                namespace.inner()
            );
            assert_eq!(namespace.stream_id(), stream);
            assert_eq!(namespace.topic_id(), topic);
            assert_eq!(namespace.partition_id(), partition);
        }
    }

    #[test]
    fn metadata_sentinel_cannot_collide_with_any_packable_namespace() {
        assert!(!IggyNamespace::is_packable(METADATA_GROUP));

        // The (0, 0, 0) corner is intentionally a legal partition, which is
        // precisely why `0` is unsuitable as the metadata sentinel.
        let zero = IggyNamespace::new(0, 0, 0);
        assert_eq!(zero.inner(), 0);
        assert!(IggyNamespace::is_packable(zero.inner()));
        assert_ne!(zero.inner(), METADATA_GROUP);

        // Maximum packable triple stays inside the packed range.
        let max = IggyNamespace::new(MAX_STREAMS - 1, MAX_TOPICS - 1, MAX_PARTITIONS - 1);
        assert!(IggyNamespace::is_packable(max.inner()));
        assert_ne!(max.inner(), METADATA_GROUP);
    }

    /// The regression this change exists for. The previous `new` masked, so the
    /// first out-of-range slab key packed byte-identically to slab 0: same
    /// shard, consensus group, directory and authorization scope, no error.
    #[test]
    fn given_out_of_range_components_when_packing_should_refuse_rather_than_alias() {
        for (stream, topic, partition) in [
            (MAX_STREAMS, 2, 7),
            (0, MAX_TOPICS, 7),
            (0, 2, MAX_PARTITIONS),
        ] {
            assert_eq!(
                IggyNamespace::try_new(stream, topic, partition),
                None,
                "({stream}, {topic}, {partition}) must not pack"
            );
        }

        let in_range = IggyNamespace::new(0, 2, 7);
        assert!(IggyNamespace::try_new(MAX_STREAMS, 2, 7).is_none());
        assert_eq!(in_range.stream_id(), 0);
    }

    /// The mask admits 48,576 ids the maximum forbids, so bound checks compare
    /// against the constant.
    #[test]
    fn given_a_partition_id_between_the_maximum_and_the_mask_when_packing_should_refuse() {
        assert!(IggyNamespace::try_new(0, 0, MAX_PARTITIONS - 1).is_some());
        assert!(IggyNamespace::try_new(0, 0, MAX_PARTITIONS).is_none());
        assert!(IggyNamespace::try_new(0, 0, super::PARTITION_MASK as usize).is_none());
    }

    #[test]
    #[should_panic(expected = "exceeds the packed layout")]
    fn given_an_out_of_range_stream_when_calling_new_should_panic() {
        let _ = IggyNamespace::new(MAX_STREAMS, 0, 0);
    }
}
