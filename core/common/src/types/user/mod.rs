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

pub(crate) mod user_identity_info;
pub(crate) mod user_info;
pub(crate) mod user_status;

/// User IDs above this threshold are synthetic (minted for external auth
/// inline-grant sessions). The slab allocator starts at 0 and grows upward,
/// so collisions require billions of persisted users.
pub const SYNTHETIC_USER_ID_THRESHOLD: u32 = u32::MAX - 1_000_000;

#[must_use]
pub const fn is_synthetic_user_id(user_id: u32) -> bool {
    user_id > SYNTHETIC_USER_ID_THRESHOLD
}
