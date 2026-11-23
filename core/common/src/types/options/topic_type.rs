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

/// What a topic is. Fixed at creation and never changed afterwards, in either
/// direction.
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
pub enum TopicType {
    /// Its messages stay in this cluster. Tiered storage can move its sealed
    /// segments to the object store.
    #[default]
    Normal,
    /// Its messages also become readable in the other datacenters, through
    /// the object store. Its [`super::GeoMode`] says how they get there.
    Geo,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn given_each_type_when_round_tripped_through_text_should_keep_its_name() {
        for (topic_type, text) in [(TopicType::Normal, "normal"), (TopicType::Geo, "geo")] {
            assert_eq!(topic_type.as_ref(), text);
            assert_eq!(text.parse::<TopicType>(), Ok(topic_type));
            assert_eq!(text.to_uppercase().parse::<TopicType>(), Ok(topic_type));
        }
    }

    #[test]
    fn given_an_unknown_name_when_parsed_should_fail() {
        assert!("geo_tiered".parse::<TopicType>().is_err());
        assert!("".parse::<TopicType>().is_err());
    }
}
