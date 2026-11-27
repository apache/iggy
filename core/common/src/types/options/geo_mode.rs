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

use super::Durability;

/// How a geo topic writes its messages. Only a geo topic has a mode, and the
/// only change between modes is `geo` to `geo_tiered` or back.
#[derive(
    Debug,
    Clone,
    Copy,
    Default,
    PartialEq,
    Eq,
    Hash,
    Serialize,
    Deserialize,
    Display,
    EnumString,
    AsRefStr,
)]
#[serde(rename_all = "snake_case")]
#[strum(serialize_all = "snake_case", ascii_case_insensitive)]
pub enum GeoMode {
    /// Upload first: a batch goes to the object store before it commits, and
    /// consensus replicates where it sits there, not its messages.
    #[default]
    Geo,
    /// Replicate first, upload later: the tiered storage write path. Not
    /// selectable by clients yet.
    GeoTiered,
}

impl GeoMode {
    /// The durability of the messages and of the consumer offsets of a geo
    /// topic in this mode, whatever its create names, or `None` when the mode
    /// leaves both to the topic. It applies to each of [`Durability::KEYS`].
    ///
    /// A `geo` send already waits for its upload, but the record that places
    /// its batch is in the local journal, and a replicated acknowledgement
    /// does not wait for that record to reach disk. A crash of every node at
    /// once then loses the batch in every datacenter.
    ///
    /// A replica that acknowledges an offset store before it is on disk can
    /// restart without it, so after a crash of every node the partition
    /// trusts no replica's log and elects only once every replica is back.
    #[must_use]
    pub const fn fixed_durability(self) -> Option<Durability> {
        match self {
            Self::Geo => Some(Durability::Persisted),
            Self::GeoTiered => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_each_mode_when_round_tripped_through_text_should_keep_its_name() {
        for (mode, text) in [(GeoMode::Geo, "geo"), (GeoMode::GeoTiered, "geo_tiered")] {
            assert_eq!(mode.as_ref(), text);
            assert_eq!(text.parse::<GeoMode>(), Ok(mode));
            assert_eq!(text.to_uppercase().parse::<GeoMode>(), Ok(mode));
        }
    }

    #[test]
    fn given_each_mode_when_asked_for_its_durability_should_fix_it_only_for_geo() {
        assert_eq!(GeoMode::Geo.fixed_durability(), Some(Durability::Persisted));
        assert_eq!(GeoMode::GeoTiered.fixed_durability(), None);
    }

    #[test]
    fn given_a_normal_topic_mode_when_parsed_should_fail() {
        for text in ["local", "tiered", "normal", ""] {
            assert!(text.parse::<GeoMode>().is_err(), "{text} must not parse");
        }
    }
}
