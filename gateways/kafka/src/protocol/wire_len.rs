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

//! Shared Kafka wire-size pricing primitives.
//!
//! `ListGroups`' hand-computed response pricing and the consumer-group coordinator's own
//! stored-state pricing both estimate encoded field sizes before `kafka_protocol` ever builds a
//! response, so a frame that would exceed `max_frame_size` can be caught up front. Kept in one
//! place so the two copies cannot drift out of sync with each other or with `kafka_protocol`'s
//! actual encoder arithmetic.

/// Length, in bytes, of the unsigned varint `kafka_protocol` writes for every compact-encoding
/// length prefix.
#[must_use]
pub const fn unsigned_varint_len(value: u32) -> usize {
    match value {
        0x00..=0x7f => 1,
        0x80..=0x3fff => 2,
        0x4000..=0x001f_ffff => 3,
        0x0020_0000..=0x0fff_ffff => 4,
        _ => 5,
    }
}

/// Length prefix of a compact string, compact bytes field, or compact array: the unsigned
/// varint of `len + 1`.
#[must_use]
pub fn kafka_compact_prefix(len: usize) -> usize {
    let wire = u32::try_from(len.saturating_add(1)).unwrap_or(u32::MAX);
    unsigned_varint_len(wire)
}

/// A legacy (2-byte length) or compact (varint length) string field's total wire size.
#[must_use]
pub fn kafka_string_len(flexible: bool, bytes: usize) -> usize {
    if flexible {
        kafka_compact_prefix(bytes) + bytes
    } else {
        2 + bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsigned_varint_len_matches_kafka_protocols_own_boundaries() {
        assert_eq!(unsigned_varint_len(0), 1);
        assert_eq!(unsigned_varint_len(0x7f), 1);
        assert_eq!(unsigned_varint_len(0x80), 2);
        assert_eq!(unsigned_varint_len(0x3fff), 2);
        assert_eq!(unsigned_varint_len(0x4000), 3);
        assert_eq!(unsigned_varint_len(u32::MAX), 5);
    }

    #[test]
    fn kafka_compact_prefix_prices_len_plus_one() {
        assert_eq!(kafka_compact_prefix(0), 1); // N+1 = 1
        assert_eq!(kafka_compact_prefix(126), 1); // N+1 = 127
        assert_eq!(kafka_compact_prefix(127), 2); // N+1 = 128
    }

    #[test]
    fn kafka_string_len_differs_between_flexible_and_legacy() {
        assert_eq!(kafka_string_len(false, 10), 12); // 2-byte length + bytes
        assert_eq!(kafka_string_len(true, 10), 11); // 1-byte compact prefix + bytes
    }
}
