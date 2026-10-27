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

use crate::types::message::{HeaderKey, HeaderKind};
use serde::{Deserialize, Serialize};
use strum::{Display, EnumIter, EnumString, IntoStaticStr};

/// Every option key the catalog knows, spelled as it travels on the wire.
#[derive(
    Debug,
    Clone,
    Copy,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Serialize,
    Deserialize,
    Display,
    EnumString,
    IntoStaticStr,
    EnumIter,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case")]
pub enum OptionKey {
    Name,
    Username,
    Status,
    CompressionAlgorithm,
    MessageExpiry,
    MaxTopicSize,
    SegmentSize,
    Durability,
    ConsumerOffsetDurability,
    PartitionResizePolicy,
    MessagesRequiredToSave,
    SizeOfMessagesRequiredToSave,
    PreallocateSegments,
    EncryptionKey,
    EncryptionKeyWrapped,
    EncryptionKeySource,
    EncryptionKeyFingerprint,
    EncryptionKekFingerprint,
    CriticalKeys,
}

impl From<OptionKey> for HeaderKey {
    fn from(key: OptionKey) -> Self {
        let name: &'static str = key.into();
        // Catalog names are short ASCII, always a valid string header key.
        Self::new_unchecked(HeaderKind::String, name.as_bytes())
    }
}

impl TryFrom<&HeaderKey> for OptionKey {
    type Error = ();

    fn try_from(key: &HeaderKey) -> Result<Self, Self::Error> {
        if key.kind() != HeaderKind::String {
            return Err(());
        }
        std::str::from_utf8(key.as_bytes())
            .ok()
            .and_then(|name| name.parse().ok())
            .ok_or(())
    }
}
