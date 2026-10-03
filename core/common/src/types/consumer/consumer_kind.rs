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

use crate::Identifier;
use crate::Validatable;
use crate::error::IggyError;
use clap::ValueEnum;
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt::Display;
use std::str::FromStr;

/// `Consumer` represents the type of consumer that is consuming a message.
/// It can be a `Consumer`, a `ConsumerGroup`, or an `ExternalGroup`.
/// It consists of the following fields:
/// - `kind`: the type of consumer.
/// - `id`: the unique identifier of the consumer.
#[derive(Debug, Serialize, Deserialize, PartialEq, Default, Clone)]
pub struct Consumer {
    /// The type of consumer.
    #[serde(skip)]
    pub kind: ConsumerKind,
    /// The unique identifier of the consumer.
    #[serde(rename = "consumer_id")]
    #[serde(serialize_with = "serialize_identifier")]
    #[serde(deserialize_with = "deserialize_identifier")]
    #[serde(default = "default_id")]
    pub id: Identifier,
}

/// `ConsumerKind` is an enum that represents the type of consumer.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, Hash, Default, Copy, Clone, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerKind {
    /// `Consumer` represents a regular consumer.
    #[default]
    #[value(name = "consumer", alias = "c")]
    Consumer,
    /// `ConsumerGroup` represents a consumer group.
    #[value(name = "consumer-group", alias = "cg")]
    ConsumerGroup,
    /// `ExternalGroup` holds the offsets of a group managed outside Iggy, such as a Kafka group
    #[value(name = "external-group", alias = "eg")]
    ExternalGroup,
}

fn default_id() -> Identifier {
    Identifier::numeric(1).unwrap()
}

impl Validatable<IggyError> for Consumer {
    fn validate(&self) -> Result<(), IggyError> {
        Ok(())
    }
}

impl Consumer {
    /// Creates a new `Consumer` from a `Consumer`.
    pub fn from_consumer(consumer: &Consumer) -> Self {
        Self {
            kind: consumer.kind,
            id: consumer.id.clone(),
        }
    }

    /// Creates a new `Consumer` from the `Identifier`.
    pub fn new(id: Identifier) -> Self {
        Self {
            kind: ConsumerKind::Consumer,
            id,
        }
    }

    // Creates a new `ConsumerGroup` from the `Identifier`.
    pub fn group(id: Identifier) -> Self {
        Self {
            kind: ConsumerKind::ConsumerGroup,
            id,
        }
    }

    /// Creates a new `ExternalGroup` from the `Identifier` of an Iggy consumer group.
    ///
    /// For offset calls only: no membership check, no range check, never polled, and no hold on
    /// retention. Deleting the group deletes its offsets.
    pub fn external_group(id: Identifier) -> Self {
        Self {
            kind: ConsumerKind::ExternalGroup,
            id,
        }
    }
}

/// `ConsumerKind` is an enum that represents the type of consumer.
impl ConsumerKind {
    /// The number of kinds.
    pub const COUNT: usize = 3;

    /// Every kind, in [`ConsumerKind::index`] order.
    pub const ALL: [ConsumerKind; Self::COUNT] = [
        ConsumerKind::Consumer,
        ConsumerKind::ConsumerGroup,
        ConsumerKind::ExternalGroup,
    ];

    /// The position of the kind in [`ConsumerKind::ALL`], for arrays with one slot per kind.
    pub const fn index(self) -> usize {
        match self {
            ConsumerKind::Consumer => 0,
            ConsumerKind::ConsumerGroup => 1,
            ConsumerKind::ExternalGroup => 2,
        }
    }

    /// The name that [`Display`] writes, as a static string for labels.
    pub const fn as_str(self) -> &'static str {
        match self {
            ConsumerKind::Consumer => "consumer",
            ConsumerKind::ConsumerGroup => "consumer_group",
            ConsumerKind::ExternalGroup => "external_group",
        }
    }

    /// Returns the code of the `ConsumerKind`.
    pub fn as_code(&self) -> u8 {
        match self {
            ConsumerKind::Consumer => 1,
            ConsumerKind::ConsumerGroup => 2,
            ConsumerKind::ExternalGroup => 3,
        }
    }

    /// Creates a new `ConsumerKind` from the code.
    pub fn from_code(code: u8) -> Result<Self, IggyError> {
        match code {
            1 => Ok(ConsumerKind::Consumer),
            2 => Ok(ConsumerKind::ConsumerGroup),
            3 => Ok(ConsumerKind::ExternalGroup),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}

impl Display for Consumer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}|{}", self.kind, self.id)
    }
}

impl Display for ConsumerKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

fn serialize_identifier<S>(id: &Identifier, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    serializer.serialize_str(&id.to_string())
}

fn deserialize_identifier<'de, D>(deserializer: D) -> Result<Identifier, D::Error>
where
    D: Deserializer<'de>,
{
    struct IdentifierVisitor;

    impl<'de> serde::de::Visitor<'de> for IdentifierVisitor {
        type Value = Identifier;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a string or number representing an identifier")
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Identifier::from_str(value).map_err(serde::de::Error::custom)
        }

        fn visit_u32<E>(self, value: u32) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            Identifier::numeric(value).map_err(serde::de::Error::custom)
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if value > u32::MAX as u64 {
                return Err(serde::de::Error::custom(
                    "numeric identifier must fit in u32",
                ));
            }
            Identifier::numeric(value as u32).map_err(serde::de::Error::custom)
        }

        fn visit_i32<E>(self, value: i32) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if value < 0 {
                return Err(serde::de::Error::custom(
                    "numeric identifier must be positive",
                ));
            }
            Identifier::numeric(value as u32).map_err(serde::de::Error::custom)
        }

        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
        where
            E: serde::de::Error,
        {
            if value < 0 || value > u32::MAX as i64 {
                return Err(serde::de::Error::custom(
                    "numeric identifier must be a positive u32",
                ));
            }
            Identifier::numeric(value as u32).map_err(serde::de::Error::custom)
        }
    }

    deserializer.deserialize_any(IdentifierVisitor)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_kind_round_trips_through_its_code() {
        for kind in ConsumerKind::ALL {
            assert_eq!(ConsumerKind::from_code(kind.as_code()).unwrap(), kind);
        }
        assert!(ConsumerKind::from_code(4).is_err());
    }

    #[test]
    fn every_kind_sits_at_its_index_and_displays_its_name() {
        for (index, kind) in ConsumerKind::ALL.into_iter().enumerate() {
            assert_eq!(kind.index(), index);
            assert_eq!(kind.to_string(), kind.as_str());
        }
    }
}
