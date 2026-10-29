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

//! Mirrors of the server's HTTP JSON shapes.
//!
//! `iggy_common` owns the real types but pulls tokio with `net`, which does
//! not build for `wasm32-unknown-unknown`, so the UI keeps its own copies of
//! the fields it renders, under the server's type names. Unknown fields are
//! ignored on purpose. The tests below decode the server's serialized types
//! into each mirror, so a renamed field fails on the host.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize)]
pub struct LoginUser {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct IdentityInfo {
    pub user_id: u32,
    pub access_token: TokenInfo,
}

#[derive(Debug, Clone, Deserialize)]
pub struct TokenInfo {
    pub token: String,
    pub expiry: u64,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct UserInfoDetails {
    pub username: String,
}

/// One entry of a resource's `options` map, as written by
/// `iggy_common::resource_options_json`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct OptionValue {
    pub value: String,
}

pub type ResourceOptions = BTreeMap<String, OptionValue>;

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Stream {
    pub id: u32,
    pub created_at: u64,
    pub name: String,
    pub size: String,
    pub messages_count: u64,
    pub topics_count: u32,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct StreamDetails {
    pub id: u32,
    pub created_at: u64,
    pub name: String,
    pub size: String,
    pub messages_count: u64,
    pub topics_count: u32,
    pub topics: Vec<Topic>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Topic {
    pub id: u32,
    pub created_at: u64,
    pub name: String,
    pub size: String,
    pub messages_count: u64,
    pub partitions_count: u32,
    #[serde(default)]
    pub options: ResourceOptions,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct TopicDetails {
    pub id: u32,
    pub created_at: u64,
    pub name: String,
    pub size: String,
    pub messages_count: u64,
    pub partitions_count: u32,
    pub partitions: Vec<Partition>,
    #[serde(default)]
    pub options: ResourceOptions,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Partition {
    pub id: u32,
    pub created_at: u64,
    pub segments_count: u32,
    pub current_offset: u64,
    pub size: String,
    pub messages_count: u64,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct PolledMessages {
    /// Offset of the newest message in the partition when the poll ran.
    pub current_offset: u64,
    pub messages: Vec<IggyMessage>,
}

/// `Eq` lets `Rc<IggyMessage>` props compare by pointer before comparing
/// payloads.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct IggyMessage {
    pub header: IggyMessageHeader,
    /// Standard padded base64.
    pub payload: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct IggyMessageHeader {
    pub id: u128,
    pub offset: u64,
    pub timestamp: u64,
    pub payload_length: u32,
}

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;
    use iggy_common::{
        HeaderKey, HeaderValue, IggyByteSize, IggyTimestamp, OptionKey, PartitionContext,
        UserStatus,
    };
    use serde::de::DeserializeOwned;

    use super::*;

    const CREATED_AT: u64 = 1_700_000_000_000_000;
    const MESSAGE_EXPIRY_MICROS: u64 = 86_400_000_000;

    #[test]
    fn given_server_login_reply_should_decode_identity() {
        let server = iggy_common::IdentityInfo {
            user_id: 7,
            access_token: Some(iggy_common::TokenInfo {
                token: "secret".to_owned(),
                expiry: 1_800_000_000,
            }),
        };
        let mirror: IdentityInfo = decode(&server);
        assert_eq!(mirror.user_id, 7);
        assert_eq!(mirror.access_token.token, "secret");
        assert_eq!(mirror.access_token.expiry, 1_800_000_000);
    }

    #[test]
    fn given_ui_login_should_decode_as_server_login_user() {
        let mirror = LoginUser {
            username: "iggy".to_owned(),
            password: "password".to_owned(),
        };
        let server: iggy_common::login_user::LoginUser = decode(&mirror);
        assert_eq!(server.username, "iggy");
    }

    #[test]
    fn given_server_user_should_decode_user_info_details() {
        let server = iggy_common::UserInfoDetails {
            id: 1,
            created_at: IggyTimestamp::from(CREATED_AT),
            status: UserStatus::Active,
            username: "iggy".to_owned(),
            permissions: None,
            options: Default::default(),
        };
        let mirror: UserInfoDetails = decode(&server);
        assert_eq!(mirror.username, "iggy");
    }

    #[test]
    fn given_server_streams_should_decode_stream_list() {
        let server = vec![iggy_common::Stream {
            id: 1,
            created_at: IggyTimestamp::from(CREATED_AT),
            name: "orders".to_owned(),
            size: IggyByteSize::from(2_048),
            messages_count: 10,
            topics_count: 2,
            options: Default::default(),
        }];
        let mirror: Vec<Stream> = decode(&server);
        assert_eq!(
            mirror,
            [Stream {
                id: 1,
                created_at: CREATED_AT,
                name: "orders".to_owned(),
                size: "2 KiB".to_owned(),
                messages_count: 10,
                topics_count: 2,
            }]
        );
    }

    #[test]
    fn given_server_stream_details_should_decode_topics() {
        let server = iggy_common::StreamDetails {
            id: 1,
            created_at: IggyTimestamp::from(CREATED_AT),
            name: "orders".to_owned(),
            size: IggyByteSize::from(2_048),
            messages_count: 10,
            topics_count: 1,
            topics: vec![server_topic()],
            options: Default::default(),
        };
        let mirror: StreamDetails = decode(&server);
        assert_eq!(mirror.name, "orders");
        assert_eq!(mirror.topics.len(), 1);
        let topic = &mirror.topics[0];
        assert_eq!((topic.id, topic.partitions_count), (3, 1));
        assert_eq!(
            topic.options["message_expiry"].value,
            MESSAGE_EXPIRY_MICROS.to_string()
        );
    }

    #[test]
    fn given_server_topic_details_should_decode_partitions() {
        let topic = server_topic();
        let server = iggy_common::TopicDetails {
            id: topic.id,
            created_at: topic.created_at,
            name: topic.name,
            size: topic.size,
            messages_count: topic.messages_count,
            partitions_count: topic.partitions_count,
            partitions: vec![iggy_common::Partition {
                id: 0,
                created_at: IggyTimestamp::from(CREATED_AT),
                segments_count: 1,
                current_offset: 9,
                size: IggyByteSize::from(1_024),
                messages_count: 10,
                context: PartitionContext::default(),
            }],
            options: topic.options,
        };
        let mirror: TopicDetails = decode(&server);
        assert_eq!(
            mirror.partitions,
            [Partition {
                id: 0,
                created_at: CREATED_AT,
                segments_count: 1,
                current_offset: 9,
                size: "1 KiB".to_owned(),
                messages_count: 10,
            }]
        );
        assert_eq!(
            mirror.options["message_expiry"].value,
            MESSAGE_EXPIRY_MICROS.to_string()
        );
    }

    #[test]
    fn given_server_polled_messages_should_decode_payload_and_header() {
        let mut message = iggy_common::IggyMessage::builder()
            .id(42)
            .payload("hello".into())
            .build()
            .expect("valid message");
        message.header.offset = 5;
        message.header.timestamp = CREATED_AT;
        let mut server = iggy_common::PolledMessages::empty();
        server.current_offset = 9;
        server.count = 1;
        server.messages = vec![message];

        let mirror: PolledMessages = decode(&server);
        assert_eq!(mirror.current_offset, 9);
        assert_eq!(
            mirror.messages,
            [IggyMessage {
                header: IggyMessageHeader {
                    id: 42,
                    offset: 5,
                    timestamp: CREATED_AT,
                    payload_length: 5,
                },
                payload: STANDARD.encode("hello"),
            }]
        );
    }

    fn server_topic() -> iggy_common::Topic {
        let options = iggy_common::ResourceOptions::from([(
            HeaderKey::from(OptionKey::MessageExpiry),
            iggy_common::OptionValue::explicit(HeaderValue::from(MESSAGE_EXPIRY_MICROS)),
        )]);
        iggy_common::Topic {
            id: 3,
            created_at: IggyTimestamp::from(CREATED_AT),
            name: "events".to_owned(),
            size: IggyByteSize::from(1_024),
            messages_count: 10,
            partitions_count: 1,
            options,
        }
    }

    fn decode<T: DeserializeOwned>(value: &impl Serialize) -> T {
        let json = serde_json::to_string(value).expect("serializable");
        serde_json::from_str(&json).expect("the mirror decodes the server's JSON")
    }
}
