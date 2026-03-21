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
use iggy::prelude::ConsumerGroupDetails as RustConsumerGroupDetails;
use iggy_common::ConsumerGroupMember as RustConsumerGroupMember;

impl From<RustConsumerGroupMember> for ffi::ConsumerGroupMember {
    fn from(member: RustConsumerGroupMember) -> Self {
        ffi::ConsumerGroupMember {
            id: member.id,
            partitions_count: member.partitions_count,
            partitions: member.partitions,
        }
    }
}

impl From<RustConsumerGroupDetails> for ffi::ConsumerGroupDetails {
    fn from(group: RustConsumerGroupDetails) -> Self {
        ffi::ConsumerGroupDetails {
            id: group.id,
            name: group.name,
            partitions_count: group.partitions_count,
            members_count: group.members_count,
            members: group
                .members
                .into_iter()
                .map(ffi::ConsumerGroupMember::from)
                .collect(),
        }
    }
}
