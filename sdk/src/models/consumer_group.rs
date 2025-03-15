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

use serde::{Deserialize, Serialize};

/// `ConsumerGroup` represents the information about a consumer group.
/// It consists of the following fields:
/// - `id`: the unique identifier (numeric) of the consumer group.
/// - `name`: the name of the consumer group.
/// - `partitions_count`: the number of partitions the consumer group is consuming.
/// - `members_count`: the number of members in the consumer group.
#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerGroup {
    /// The unique identifier (numeric) of the consumer group.
    pub id: u32,
    /// The name of the consumer group.
    pub name: String,
    /// The number of partitions the consumer group is consuming.
    pub partitions_count: u32,
    /// The number of members in the consumer group.
    pub members_count: u32,
}

/// `ConsumerGroupDetails` represents the detailed information about a consumer group.
/// It consists of the following fields:
/// - `id`: the unique identifier (numeric) of the consumer group.
/// - `name`: the name of the consumer group.
/// - `partitions_count`: the number of partitions the consumer group is consuming.
/// - `members_count`: the number of members in the consumer group.
#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerGroupDetails {
    /// The unique identifier (numeric) of the consumer group.
    pub id: u32,
    /// The name of the consumer group.
    pub name: String,
    /// The number of partitions the consumer group is consuming.
    pub partitions_count: u32,
    /// The number of members in the consumer group.
    pub members_count: u32,
    /// The collection of members in the consumer group.
    pub members: Vec<ConsumerGroupMember>,
}

/// `ConsumerGroupMember` represents the information about a consumer group member.
/// It consists of the following fields:
/// - `id`: the unique identifier (numeric) of the consumer group member.
/// - `partitions_count`: the number of partitions the consumer group member is consuming.
/// - `partitions`: the collection of partitions the consumer group member is consuming.
#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerGroupMember {
    /// The unique identifier (numeric) of the consumer group member.
    pub id: u32,
    /// The number of partitions the consumer group member is consuming.
    pub partitions_count: u32,
    /// The collection of partitions the consumer group member is consuming.
    pub partitions: Vec<u32>,
}
