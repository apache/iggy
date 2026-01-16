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

use iggy_common::IggyMessagesBatch;

/// Holds batches that are being written to disk.
///
/// During async I/O, messages are transferred from the journal to disk.
/// This buffer holds frozen (immutable, Arc-backed) copies so consumers
/// can still read them during the write operation.
#[derive(Debug, Default)]
pub struct IggyMessagesBatchSetInFlight {
    batches: Vec<IggyMessagesBatch>,
    first_offset: u64,
    last_offset: u64,
}

impl IggyMessagesBatchSetInFlight {
    pub fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    pub fn first_offset(&self) -> u64 {
        self.first_offset
    }

    pub fn last_offset(&self) -> u64 {
        self.last_offset
    }

    pub fn set(&mut self, batches: Vec<IggyMessagesBatch>) {
        if batches.is_empty() {
            self.clear();
            return;
        }
        self.first_offset = batches.first().and_then(|b| b.first_offset()).unwrap_or(0);
        self.last_offset = batches.last().and_then(|b| b.last_offset()).unwrap_or(0);
        self.batches = batches;
    }

    pub fn clear(&mut self) {
        self.batches.clear();
        self.first_offset = 0;
        self.last_offset = 0;
    }

    pub fn get_by_offset(&self, start_offset: u64, count: u32) -> &[IggyMessagesBatch] {
        if self.is_empty() || start_offset > self.last_offset {
            return &[];
        }

        let end_offset = start_offset + count as u64 - 1;
        if end_offset < self.first_offset {
            return &[];
        }

        &self.batches
    }

    pub fn batches(&self) -> &[IggyMessagesBatch] {
        &self.batches
    }
}
