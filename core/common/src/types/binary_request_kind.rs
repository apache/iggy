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

use strum::{Display, EnumString};

/// How a raw binary request executes on the server.
///
/// A vendor command code is unknown to the SDK, so the caller has to say whether
/// it runs outside consensus or is replicated through it. The declaration is
/// never authoritative: a code the protocol tables already know keeps its own
/// class, and the server independently rejects a declaration it disagrees with.
///
/// Classic framing carries `[length][code][payload]` with no operation field, so
/// the kind is inert there and both variants encode identical bytes. It only
/// selects a wire path under VSR.
///
/// The string form is the cross-SDK spelling the shared BDD scenarios and the
/// PHP binding use.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Display, EnumString)]
#[strum(serialize_all = "snake_case")]
pub enum BinaryRequestKind {
    /// Runs on the receiving node only, outside consensus. Reads, pings, and
    /// vendor commands that own no replicated state.
    NonReplicated,
    /// Replicated through consensus before it takes effect.
    ///
    /// Only the standard replicated commands are supported today. A vendor code
    /// declared `Replicated` yields [`crate::IggyError::FeatureUnavailable`]:
    /// the protocol has no deterministic handler registry, replicated state
    /// ownership, or snapshot contract for one yet.
    Replicated,
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::BinaryRequestKind;

    #[test]
    fn given_display_output_when_parsed_should_round_trip() {
        for kind in [
            BinaryRequestKind::NonReplicated,
            BinaryRequestKind::Replicated,
        ] {
            assert_eq!(
                BinaryRequestKind::from_str(&kind.to_string()).unwrap(),
                kind
            );
        }
    }

    #[test]
    fn given_cross_sdk_names_when_parsed_should_match_the_other_bindings() {
        assert_eq!(
            BinaryRequestKind::NonReplicated.to_string(),
            "non_replicated"
        );
        assert_eq!(BinaryRequestKind::Replicated.to_string(), "replicated");
    }

    #[test]
    fn given_unknown_text_when_parsed_should_fail() {
        assert!(BinaryRequestKind::from_str("auto").is_err());
    }
}
