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

//! The workload of an OMB run, as far as the result file records it.

use std::fmt;

use serde::{Deserialize, Serialize};

use crate::omb::result::OmbResult;

/// Every workload value that an OMB result records, so two runs with the same key ran the same
/// workload as far as the files can tell. The driver is not part of the key: the driver name is
/// the only label of a driver configuration, and comparing two configurations of one workload is
/// a main use of the compare page. The file doesn't record the target rate, so two workloads that
/// differ only in rate share a key unless their names differ.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct OmbWorkloadKey {
    /// The `name` from the workload file, free text.
    pub name: String,
    /// In bytes. For a workload with a message size distribution, the weighted average.
    pub message_size: u64,
    pub topics: u32,
    /// Partitions per topic.
    pub partitions: u32,
    pub producers_per_topic: u32,
    /// Consumers per subscription of a topic.
    pub consumers_per_topic: u32,
}

/// One field of an [`OmbWorkloadKey`], so two keys can name what differs between them. The
/// variants follow the key's field order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OmbWorkloadField {
    Name,
    MessageSize,
    Topics,
    Partitions,
    ProducersPerTopic,
    ConsumersPerTopic,
}

impl From<&OmbResult> for OmbWorkloadKey {
    fn from(result: &OmbResult) -> Self {
        Self {
            name: result.workload.clone(),
            message_size: result.message_size,
            topics: result.topics,
            partitions: result.partitions,
            producers_per_topic: result.producers_per_topic,
            consumers_per_topic: result.consumers_per_topic,
        }
    }
}

impl OmbWorkloadKey {
    /// The fields where `other` differs from this key, in key order. The destructuring makes a new
    /// key field a compile error here, so it can't be left out of the comparison.
    pub fn differences(&self, other: &Self) -> Vec<OmbWorkloadField> {
        let Self {
            name,
            message_size,
            topics,
            partitions,
            producers_per_topic,
            consumers_per_topic,
        } = self;
        [
            (OmbWorkloadField::Name, *name == other.name),
            (
                OmbWorkloadField::MessageSize,
                *message_size == other.message_size,
            ),
            (OmbWorkloadField::Topics, *topics == other.topics),
            (
                OmbWorkloadField::Partitions,
                *partitions == other.partitions,
            ),
            (
                OmbWorkloadField::ProducersPerTopic,
                *producers_per_topic == other.producers_per_topic,
            ),
            (
                OmbWorkloadField::ConsumersPerTopic,
                *consumers_per_topic == other.consumers_per_topic,
            ),
        ]
        .into_iter()
        .filter_map(|(field, same)| (!same).then_some(field))
        .collect()
    }
}

impl OmbWorkloadField {
    /// Every field, in key order.
    pub const ALL: [Self; 6] = [
        Self::Name,
        Self::MessageSize,
        Self::Topics,
        Self::Partitions,
        Self::ProducersPerTopic,
        Self::ConsumersPerTopic,
    ];
}

impl fmt::Display for OmbWorkloadField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Name => "Workload",
            Self::MessageSize => "Message size",
            Self::Topics => "Topics",
            Self::Partitions => "Partitions per topic",
            Self::ProducersPerTopic => "Producers per topic",
            Self::ConsumersPerTopic => "Consumers per topic",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::omb::test_fixtures::{
        EDGE8_100_PARTITIONS, EDGE8_DEFAULT, NO_CONSUMERS, RC1_DEFAULT, RC1_PERSISTED,
    };

    /// A change to one field, with the field that the key must name as changed.
    type FieldChange = (OmbWorkloadField, fn(&mut OmbWorkloadKey));

    fn key(result: &OmbResult) -> OmbWorkloadKey {
        OmbWorkloadKey::from(result)
    }

    #[test]
    fn given_real_result_when_keyed_should_take_workload_fields() {
        assert_eq!(
            key(&EDGE8_100_PARTITIONS.result()),
            OmbWorkloadKey {
                name: "1 topic x 100 partitions / 4p 4c / 1 KiB / 124.9 MB/s".to_owned(),
                message_size: 1024,
                topics: 1,
                partitions: 100,
                producers_per_topic: 4,
                consumers_per_topic: 4,
            }
        );
    }

    #[test]
    fn given_runs_of_one_workload_with_other_versions_and_drivers_when_diffed_should_find_no_field()
    {
        let default = key(&RC1_DEFAULT.result());

        assert_eq!(default.differences(&key(&RC1_PERSISTED.result())), []);
        assert_eq!(default.differences(&key(&EDGE8_DEFAULT.result())), []);
    }

    #[test]
    fn given_one_changed_field_when_diffed_should_name_it() {
        let original = key(&RC1_DEFAULT.result());
        let changes: [FieldChange; 6] = [
            (OmbWorkloadField::Name, |changed| {
                changed.name.push_str(" (rerun)")
            }),
            (OmbWorkloadField::MessageSize, |changed| {
                changed.message_size = 1024
            }),
            (OmbWorkloadField::Topics, |changed| changed.topics = 1),
            (OmbWorkloadField::Partitions, |changed| {
                changed.partitions = 16;
            }),
            (OmbWorkloadField::ProducersPerTopic, |changed| {
                changed.producers_per_topic = 4;
            }),
            (OmbWorkloadField::ConsumersPerTopic, |changed| {
                changed.consumers_per_topic = 0;
            }),
        ];

        for (field, change) in changes {
            let mut changed = original.clone();
            change(&mut changed);

            assert_eq!(original.differences(&changed), [field], "{field}");
            assert_eq!(changed.differences(&original), [field], "{field}");
        }
    }

    #[test]
    fn given_other_real_workloads_when_diffed_should_name_each_field_in_key_order() {
        let default = key(&RC1_DEFAULT.result());

        assert_eq!(
            default.differences(&key(&EDGE8_100_PARTITIONS.result())),
            OmbWorkloadField::ALL
        );
        assert_eq!(
            default.differences(&key(&NO_CONSUMERS.result())),
            [OmbWorkloadField::Name, OmbWorkloadField::ConsumersPerTopic]
        );
    }

    #[test]
    fn given_field_list_when_read_should_hold_every_field_once_in_key_order() {
        let fields = OmbWorkloadField::ALL;

        assert!(fields.windows(2).all(|pair| pair[0] < pair[1]));
        assert_eq!(
            fields.len(),
            OmbWorkloadField::ConsumersPerTopic as usize + 1
        );
    }
}
