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

/// Stored information from a `DoViewChange` message.
#[derive(Debug, Clone, Copy)]
pub struct StoredDvc {
    pub replica: u8,
    /// The view when the replica's status was last normal.
    pub log_view: u32,
    pub op: u64,
    pub commit: u64,
}

impl StoredDvc {
    /// Compare for log selection: highest `log_view`, then highest op.
    #[must_use]
    pub const fn is_better_than(&self, other: &Self) -> bool {
        if self.log_view == other.log_view {
            self.op > other.op
        } else {
            self.log_view > other.log_view
        }
    }
}

/// Array type for storing DVC messages from all replicas.
pub type DvcQuorumArray = [Option<StoredDvc>; REPLICAS_MAX];

/// Create an empty DVC quorum array.
#[must_use]
pub const fn dvc_quorum_array_empty() -> DvcQuorumArray {
    [None; REPLICAS_MAX]
}

/// Record a DVC in the array. Returns true if this is a new entry (not duplicate).
pub const fn dvc_record(array: &mut DvcQuorumArray, dvc: StoredDvc) -> bool {
    let slot = &mut array[dvc.replica as usize];
    if slot.is_some() {
        return false; // Duplicate
    }
    *slot = Some(dvc);
    true
}

/// Count how many DVCs have been received.
#[must_use]
pub fn dvc_count(array: &DvcQuorumArray) -> usize {
    array.iter().filter(|m| m.is_some()).count()
}

/// Check if a specific replica has sent a DVC.
#[must_use]
pub fn dvc_has_from(array: &DvcQuorumArray, replica: u8) -> bool {
    array.get(replica as usize).is_some_and(Option::is_some)
}

/// Select the winning DVC (best log) from the quorum.
/// Returns the DVC with: highest `log_view`, then highest op.
#[must_use]
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
#[must_use]
pub fn dvc_max_commit(array: &DvcQuorumArray) -> u64 {
    array
        .iter()
        .filter_map(|m| m.as_ref())
        .map(|dvc| dvc.commit)
        .max()
        .unwrap_or(0)
}

/// Reset the DVC quorum array.
pub const fn dvc_reset(array: &mut DvcQuorumArray) {
    *array = dvc_quorum_array_empty();
}

/// Iterator over all stored DVCs.
// TODO: add #[must_use] -- pure iterator query, callers should not ignore.
pub fn dvc_iter(array: &DvcQuorumArray) -> impl Iterator<Item = &StoredDvc> {
    array.iter().filter_map(|m| m.as_ref())
}
