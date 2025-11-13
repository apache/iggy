/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use crate::BytesSerializable;
use crate::Identifier;
use crate::Validatable;
use crate::error::IggyError;
use bytes::{BufMut, Bytes, BytesMut};
use clap::ValueEnum;
use serde::{Deserialize, Deserializer, Serialize};
use std::fmt::Display;
use std::str::FromStr;

/// `Consumer` represents the type of consumer that is consuming a message.
/// It can be either a `Consumer` or a `ConsumerGroup`.
/// It consists of the following fields:
/// - `kind`: the type of consumer. It can be either `Consumer` or `ConsumerGroup`.
/// - `id`: the unique identifier of the consumer.
#[derive(Debug, Serialize, Deserialize, PartialEq, Default, Clone)]
pub struct Consumer {
    /// The type of consumer. It can be either `Consumer` or `ConsumerGroup`.
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
#[derive(Debug, Serialize, Deserialize, PartialEq, Default, Copy, Clone, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum ConsumerKind {
    /// `Consumer` represents a regular consumer.
    #[default]
    #[value(name = "consumer", alias = "c")]
    Consumer,
    /// `ConsumerGroup` represents a consumer group.
    #[value(name = "consumer-group", alias = "cg")]
    ConsumerGroup,
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
}

impl BytesSerializable for Consumer {
    fn to_bytes(&self) -> Bytes {
        let id_bytes = self.id.to_bytes();
        let mut bytes = BytesMut::with_capacity(1 + id_bytes.len());
        bytes.put_u8(self.kind.as_code());
        bytes.put_slice(&id_bytes);
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError>
    where
        Self: Sized,
    {
        if bytes.len() < 4 {
            return Err(IggyError::InvalidCommand);
        }

        let kind = ConsumerKind::from_code(bytes[0])?;
        let id = Identifier::from_bytes(bytes.slice(1..))?;
        let consumer = Consumer { kind, id };
        consumer.validate()?;
        Ok(consumer)
    }
}

/// `ConsumerKind` is an enum that represents the type of consumer.
impl ConsumerKind {
    /// Returns the code of the `ConsumerKind`.
    pub fn as_code(&self) -> u8 {
        match self {
            ConsumerKind::Consumer => 1,
            ConsumerKind::ConsumerGroup => 2,
        }
    }

    /// Creates a new `ConsumerKind` from the code.
    pub fn from_code(code: u8) -> Result<Self, IggyError> {
        match code {
            1 => Ok(ConsumerKind::Consumer),
            2 => Ok(ConsumerKind::ConsumerGroup),
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
        match self {
            ConsumerKind::Consumer => write!(f, "consumer"),
            ConsumerKind::ConsumerGroup => write!(f, "consumer_group"),
        }
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
