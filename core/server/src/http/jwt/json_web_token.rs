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

use iggy_common::UserId;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::net::SocketAddr;
use std::{fmt, fmt::Display};

#[derive(Debug, Clone)]
pub struct Identity {
    pub token_id: String,
    pub token_expiry: u64,
    pub user_id: UserId,
    pub ip_address: SocketAddr,
}

#[derive(Debug, Clone)]
pub enum Audience {
    Single(String),
    Multiple(Vec<String>),
}

impl Audience {
    pub fn contains(&self, audience: &str) -> bool {
        match self {
            Audience::Single(aud) => aud == audience,
            Audience::Multiple(auds) => auds.iter().any(|a| a == audience),
        }
    }
}

impl Display for Audience {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Audience::Single(aud) => f.write_str(aud),
            Audience::Multiple(auds) => f.write_str(&auds.join(",")),
        }
    }
}

impl From<String> for Audience {
    fn from(aud: String) -> Self {
        Audience::Single(aud)
    }
}

impl From<&str> for Audience {
    fn from(aud: &str) -> Self {
        Audience::Single(aud.to_string())
    }
}

impl From<Vec<String>> for Audience {
    fn from(auds: Vec<String>) -> Self {
        if auds.len() == 1 {
            Audience::Single(auds.into_iter().next().unwrap())
        } else {
            Audience::Multiple(auds)
        }
    }
}

impl Serialize for Audience {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Audience::Single(aud) => serializer.serialize_str(aud),
            Audience::Multiple(auds) => auds.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for Audience {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct AudienceVisitor;

        impl<'de> serde::de::Visitor<'de> for AudienceVisitor {
            type Value = Audience;

            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a string or an array of strings")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Audience::Single(value.to_string()))
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(Audience::Single(value))
            }

            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: serde::de::SeqAccess<'de>,
            {
                let mut auds = Vec::new();
                while let Some(aud) = seq.next_element::<String>()? {
                    auds.push(aud);
                }
                Ok(Audience::Multiple(auds))
            }
        }

        deserializer.deserialize_any(AudienceVisitor)
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct JwtClaims {
    pub jti: String,
    pub iss: String,
    pub aud: Audience,
    pub sub: String,
    pub iat: u64,
    pub exp: u64,
    pub nbf: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RevokedAccessToken {
    pub id: String,
    pub expiry: u64,
}

#[derive(Debug)]
pub struct GeneratedToken {
    pub user_id: UserId,
    pub access_token: String,
    pub access_token_expiry: u64,
}
