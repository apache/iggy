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

//! Shared first-seen-order deduplication for Kafka name lists.
//!
//! `Metadata`'s requested topic names and `DescribeGroups`' requested group ids both need to
//! collapse a repeated name to one answer rather than multiplying the encoded response by the
//! repeat count. Kept in one place so the two copies cannot drift apart.

use std::collections::HashSet;

use kafka_protocol::protocol::StrBytes;

/// `names` with every repeat after the first dropped, in first-seen order.
///
/// Borrows into the dedup set rather than cloning into it; only a kept name's single `clone`
/// (into the result) ever runs.
#[must_use]
pub fn dedup_first_seen(names: &[StrBytes]) -> Vec<StrBytes> {
    let mut seen = HashSet::with_capacity(names.len());
    let mut distinct = Vec::with_capacity(names.len());
    for name in names {
        if seen.insert(name.as_str()) {
            distinct.push(name.clone());
        }
    }
    distinct
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dedup_first_seen_finds_a_name_repeated_across_the_list() {
        let names = [
            StrBytes::from_static_str("orders"),
            StrBytes::from_static_str("payments"),
            StrBytes::from_static_str("orders"),
        ];
        let distinct = dedup_first_seen(&names);
        assert_eq!(
            distinct,
            vec![
                StrBytes::from_static_str("orders"),
                StrBytes::from_static_str("payments"),
            ]
        );
    }

    #[test]
    fn dedup_first_seen_is_unchanged_when_every_name_is_unique() {
        let names = [
            StrBytes::from_static_str("orders"),
            StrBytes::from_static_str("payments"),
        ];
        assert_eq!(dedup_first_seen(&names), names.to_vec());
    }
}
