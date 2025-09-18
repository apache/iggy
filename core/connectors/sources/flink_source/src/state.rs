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
pub struct SourceState {
    pub last_offset: u64,
    pub messages_produced: u64,
    pub last_poll_time: DateTime<Utc>,
    pub checkpoint_id: Option<String>,
    pub subscription_id: Option<String>,
    pub error_count: u64,
    pub last_error: Option<String>,
}

impl SourceState {
    pub fn new() -> Self {
        SourceState {
            last_offset: 0,
            messages_produced: 0,
            last_poll_time: Utc::now(),
            checkpoint_id: None,
            subscription_id: None,
            error_count: 0,
            last_error: None,
        }
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        postcard::to_allocvec(self).unwrap_or_default()
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        postcard::from_bytes(bytes).ok()
    }

    #[allow(dead_code)]
    pub fn record_error(&mut self, error: String) {
        self.error_count += 1;
        self.last_error = Some(error);
    }

    #[allow(dead_code)]
    pub fn clear_errors(&mut self) {
        self.error_count = 0;
        self.last_error = None;
    }
}

