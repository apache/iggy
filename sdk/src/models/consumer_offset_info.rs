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

/// `ConsumerOffsetInfo` represents the information about a consumer offset.
/// It consists of the following fields:
/// - `partition_id`: the unique identifier of the partition.
/// - `current_offset`: the current offset of the partition.
/// - `stored_offset`: the stored offset by the consumer in the partition.
#[derive(Debug, Serialize, Deserialize)]
pub struct ConsumerOffsetInfo {
    /// The unique identifier of the partition.
    pub partition_id: u32,
    /// The current offset of the partition.
    pub current_offset: u64,
    /// The stored offset by the consumer in the partition.
    pub stored_offset: u64,
}
