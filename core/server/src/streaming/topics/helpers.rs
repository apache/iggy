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

use iggy_common::calculate_32;

pub fn calculate_partition_id_by_messages_key_hash(
    upperbound: usize,
    messages_key: &[u8],
) -> usize {
    let messages_key_hash = calculate_32(messages_key) as usize;
    let partition_id = messages_key_hash % upperbound;
    tracing::trace!(
        "Calculated partition ID: {} for messages key: {:?}, hash: {}",
        partition_id,
        messages_key,
        messages_key_hash
    );
    partition_id
}
