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

use crate::{BytesSerializable, IggyError};
use bytes::{BufMut, Bytes, BytesMut};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{fmt, str::FromStr};
use strum::{Display, EnumString, IntoStaticStr};

#[derive(Clone, Copy, Debug, Default, Display, PartialEq, EnumString, IntoStaticStr, Eq)]
#[repr(u8)]
pub enum TransportProtocol {
    #[default]
    #[strum(to_string = "tcp")]
    Tcp = 1,
    #[strum(to_string = "quic")]
    Quic = 2,
    #[strum(to_string = "http")]
    Http = 3,
    #[strum(to_string = "ws")]
    WebSocket = 4,
}

impl TransportProtocol {
    pub fn as_str(&self) -> &'static str {
        self.into()
    }
}

impl TryFrom<u8> for TransportProtocol {
    type Error = IggyError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            v if v == TransportProtocol::Tcp as u8 => Ok(TransportProtocol::Tcp),
            v if v == TransportProtocol::Quic as u8 => Ok(TransportProtocol::Quic),
            v if v == TransportProtocol::Http as u8 => Ok(TransportProtocol::Http),
            v if v == TransportProtocol::WebSocket as u8 => Ok(TransportProtocol::WebSocket),
            _ => Err(IggyError::InvalidCommand),
        }
    }
}

// Custom serde implementation to serialize as string for HTTP/JSON/TOML
impl Serialize for TransportProtocol {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

// Custom deserializer that accepts both string and numeric values.
// This is useful for configuration files where the transport can be specified as either:
// - A string: "tcp", "quic", "http"
// - A number: 1 (TCP), 2 (QUIC), 3 (HTTP)
impl<'de> Deserialize<'de> for TransportProtocol {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct TransportProtocolVisitor;

        impl<'de> serde::de::Visitor<'de> for TransportProtocolVisitor {
            type Value = TransportProtocol;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str(
                    "a string (\"tcp\", \"quic\", \"http\", \"ws\") or a number (1, 2, 3, 4)",
                )
            }

            fn visit_str<E>(self, value: &str) -> Result<TransportProtocol, E>
            where
                E: serde::de::Error,
            {
                TransportProtocol::from_str(value).map_err(|_| {
                    serde::de::Error::custom(format!("unknown transport protocol: {}", value))
                })
            }

            fn visit_u8<E>(self, value: u8) -> Result<TransportProtocol, E>
            where
                E: serde::de::Error,
            {
                match value {
                    v if v == TransportProtocol::Tcp as u8 => Ok(TransportProtocol::Tcp),
                    v if v == TransportProtocol::Quic as u8 => Ok(TransportProtocol::Quic),
                    v if v == TransportProtocol::Http as u8 => Ok(TransportProtocol::Http),
                    v if v == TransportProtocol::WebSocket as u8 => {
                        Ok(TransportProtocol::WebSocket)
                    }
                    _ => Err(serde::de::Error::custom(format!(
                        "invalid transport protocol number: {}",
                        value
                    ))),
                }
            }

            fn visit_u64<E>(self, value: u64) -> Result<TransportProtocol, E>
            where
                E: serde::de::Error,
            {
                if value > u8::MAX as u64 {
                    return Err(serde::de::Error::custom(format!(
                        "transport protocol number too large: {}",
                        value
                    )));
                }
                self.visit_u8(value as u8)
            }

            fn visit_i64<E>(self, value: i64) -> Result<TransportProtocol, E>
            where
                E: serde::de::Error,
            {
                if value < 0 || value > u8::MAX as i64 {
                    return Err(serde::de::Error::custom(format!(
                        "transport protocol number out of range: {}",
                        value
                    )));
                }
                self.visit_u8(value as u8)
            }
        }

        deserializer.deserialize_any(TransportProtocolVisitor)
    }
}

impl BytesSerializable for TransportProtocol {
    fn get_buffer_size(&self) -> usize {
        1
    }

    fn to_bytes(&self) -> Bytes {
        let mut bytes = BytesMut::with_capacity(1);
        bytes.put_u8(*self as u8);
        bytes.freeze()
    }

    fn from_bytes(bytes: Bytes) -> Result<Self, IggyError> {
        if bytes.is_empty() {
            return Err(IggyError::InvalidCommand);
        }

        TransportProtocol::try_from(bytes[0])
    }
}
