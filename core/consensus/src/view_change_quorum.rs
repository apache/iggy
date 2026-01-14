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

use crate::REPLICAS_MAX;

/// Stored information from a DoViewChange message.
#[derive(Debug, Clone, Copy)]
pub struct StoredDvc {
    pub replica: u8,
    /// The view when the replica's status was last normal.
    pub log_view: u32,
    pub op: u64,
    pub commit: u64,
}

impl StoredDvc {
    /// Compare for log selection: highest log_view, then highest op.
    pub fn is_better_than(&self, other: &StoredDvc) -> bool {
        if self.log_view != other.log_view {
            self.log_view > other.log_view
        } else {
            self.op > other.op
        }
    }
}

/// Array type for storing DVC messages from all replicas.
pub type DvcQuorumArray = [Option<StoredDvc>; REPLICAS_MAX];

/// Create an empty DVC quorum array.
pub const fn dvc_quorum_array_empty() -> DvcQuorumArray {
    [None; REPLICAS_MAX]
}

/// Record a DVC in the array. Returns true if this is a new entry (not duplicate).
pub fn dvc_record(array: &mut DvcQuorumArray, dvc: StoredDvc) -> bool {
    let slot = &mut array[dvc.replica as usize];
    if slot.is_some() {
        return false; // Duplicate
    }
    *slot = Some(dvc);
    true
}

/// Count how many DVCs have been received.
pub fn dvc_count(array: &DvcQuorumArray) -> usize {
    array.iter().filter(|m| m.is_some()).count()
}

/// Check if a specific replica has sent a DVC.
pub fn dvc_has_from(array: &DvcQuorumArray, replica: u8) -> bool {
    array
        .get(replica as usize)
        .map(|m| m.is_some())
        .unwrap_or(false)
}

/// Select the winning DVC (best log) from the quorum.
/// Returns the DVC with: highest log_view, then highest op.
pub fn dvc_select_winner(array: &DvcQuorumArray) -> Option<&StoredDvc> {
    array
        .iter()
        .filter_map(|m| m.as_ref())
        .max_by(|a, b| match a.log_view.cmp(&b.log_view) {
            std::cmp::Ordering::Equal => a.op.cmp(&b.op),
            other => other,
        })
}

/// Get the maximum commit number across all DVCs.
pub fn dvc_max_commit(array: &DvcQuorumArray) -> u64 {
    array
        .iter()
        .filter_map(|m| m.as_ref())
        .map(|dvc| dvc.commit)
        .max()
        .unwrap_or(0)
}

/// Reset the DVC quorum array.
pub fn dvc_reset(array: &mut DvcQuorumArray) {
    *array = dvc_quorum_array_empty();
}

/// Iterator over all stored DVCs.
pub fn dvc_iter(array: &DvcQuorumArray) -> impl Iterator<Item = &StoredDvc> {
    array.iter().filter_map(|m| m.as_ref())
}
