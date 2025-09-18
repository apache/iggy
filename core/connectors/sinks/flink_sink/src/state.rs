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

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointState {
    pub last_checkpoint: DateTime<Utc>,
    pub last_offset: u64,
    pub partition_id: u32,
    pub records_processed: u64,
    pub checkpoint_id: Option<String>,
    pub savepoint_path: Option<String>,
}

impl CheckpointState {
    pub fn new() -> Self {
        CheckpointState {
            last_checkpoint: Utc::now(),
            last_offset: 0,
            partition_id: 0,
            records_processed: 0,
            checkpoint_id: None,
            savepoint_path: None,
        }
    }

    #[allow(dead_code)]
    pub fn to_bytes(&self) -> Vec<u8> {
        postcard::to_allocvec(self).unwrap_or_default()
    }

    #[allow(dead_code)]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        postcard::from_bytes(bytes).ok()
    }
}
