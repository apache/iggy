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

use serde::{Deserialize, Serialize};
use strum::{AsRefStr, Display, EnumString};

/// Immutable topic policy for adding and deleting partitions.
/// Retention and deletion of the topic remain destructive under either policy.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    Serialize,
    Deserialize,
    Display,
    EnumString,
    AsRefStr,
)]
#[serde(rename_all = "lowercase")]
#[strum(serialize_all = "lowercase", ascii_case_insensitive)]
pub enum PartitionResizePolicy {
    /// Resize is allowed without a bounded cutover or per-key ordering across layouts.
    #[default]
    Mutable,
    /// The partition set is fixed at topic creation.
    Fixed,
}
