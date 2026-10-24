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

//! Conversions between the wire and domain forms of the change log.

use super::{
    MetadataChange, MetadataChangeGroup, MetadataChangeRecord, MetadataChangeStart,
    MetadataChanges, MetadataChangesQuery, MetadataConsumerGroupCreated,
    MetadataConsumerGroupDeleted, MetadataConsumerGroupItem, MetadataCurrentItem,
    MetadataMemberItem, MetadataMemberJoined, MetadataMemberLeft, MetadataOptions,
    MetadataPartitionHandedOver, MetadataPartitionsChanged, MetadataPartitionsItem,
    MetadataSession, MetadataSessionClosed, MetadataStreamChange, MetadataStreamDeleted,
    MetadataTokenChange, MetadataTokenDeleted, MetadataTopicChange, MetadataTopicDeleted,
    MetadataTopicItem, MetadataUserChange, MetadataUserIdentity, MetadataUserPermissionsUpdated,
};
use crate::wire_conversions::resource_options_from_wire_split;
use crate::{IggyError, IggyTimestamp, Permissions, UserStatus};
use iggy_binary_protocol::primitives::metadata_changes::{
    WireChangeOptions, WireConsumerGroupCreatedBody, WireConsumerGroupDeletedBody,
    WireConsumerGroupItemBody, WireMemberItemBody, WireMemberJoinedBody, WireMemberLeftBody,
    WireMetadataChange, WireMetadataChangeGroup, WireMetadataCurrentItem,
    WirePartitionHandedOverBody, WirePartitionsChangedBody, WirePartitionsItemBody,
    WireSessionBody, WireSessionClosedBody, WireStreamBody, WireStreamDeletedBody, WireTokenBody,
    WireTokenDeletedBody, WireTopicBody, WireTopicDeletedBody, WireTopicItemBody, WireUserBody,
    WireUserIdentityBody, WireUserPermissionsBody,
};
use iggy_binary_protocol::requests::system::{GetMetadataChangesRequest, WireMetadataChangeStart};
use iggy_binary_protocol::responses::system::{
    GetMetadataChangesResponse, WireMetadataChangeRecord, WireMetadataItem,
};

impl TryFrom<WireChangeOptions> for MetadataOptions {
    type Error = IggyError;

    fn try_from(options: WireChangeOptions) -> Result<Self, Self::Error> {
        match options {
            WireChangeOptions::Readable { explicit, derived } => {
                resource_options_from_wire_split(&explicit, &derived).map(Self::Readable)
            }
            WireChangeOptions::Unreadable => Ok(Self::Unreadable),
        }
    }
}

impl TryFrom<WireStreamBody> for MetadataStreamChange {
    type Error = IggyError;

    fn try_from(body: WireStreamBody) -> Result<Self, Self::Error> {
        Ok(Self {
            stream_id: body.key.stream_id,
            stream_created_op: body.key.stream_created_op,
            created_at: body.created_at,
            name: body.name.as_str().to_owned(),
            options: body.options.try_into()?,
        })
    }
}

impl From<WireStreamDeletedBody> for MetadataStreamDeleted {
    fn from(body: WireStreamDeletedBody) -> Self {
        Self {
            stream_id: body.key.stream_id,
            stream_created_op: body.key.stream_created_op,
            created_at: body.created_at,
            name: body.name.as_str().to_owned(),
            requested_op: body.requested_op,
        }
    }
}

impl TryFrom<WireTopicBody> for MetadataTopicChange {
    type Error = IggyError;

    fn try_from(body: WireTopicBody) -> Result<Self, Self::Error> {
        Ok(Self {
            stream_id: body.key.stream.stream_id,
            stream_created_op: body.key.stream.stream_created_op,
            topic_id: body.key.topic_id,
            topic_created_op: body.key.topic_created_op,
            created_at: body.created_at,
            name: body.name.as_str().to_owned(),
            partitions_count: body.partitions_count,
            options: body.options.try_into()?,
        })
    }
}

impl TryFrom<WireTopicItemBody> for MetadataTopicItem {
    type Error = IggyError;

    fn try_from(body: WireTopicItemBody) -> Result<Self, Self::Error> {
        Ok(Self {
            stream_id: body.key.stream.stream_id,
            stream_created_op: body.key.stream.stream_created_op,
            topic_id: body.key.topic_id,
            topic_created_op: body.key.topic_created_op,
            created_at: body.created_at,
            name: body.name.as_str().to_owned(),
            options: body.options.try_into()?,
        })
    }
}

impl From<WireTopicDeletedBody> for MetadataTopicDeleted {
    fn from(body: WireTopicDeletedBody) -> Self {
        Self {
            stream_id: body.key.stream.stream_id,
            stream_created_op: body.key.stream.stream_created_op,
            topic_id: body.key.topic_id,
            topic_created_op: body.key.topic_created_op,
            created_at: body.created_at,
            name: body.name.as_str().to_owned(),
            requested_op: body.requested_op,
        }
    }
}

impl From<WirePartitionsChangedBody> for MetadataPartitionsChanged {
    fn from(body: WirePartitionsChangedBody) -> Self {
        Self {
            stream_id: body.topic.stream.stream_id,
            stream_created_op: body.topic.stream.stream_created_op,
            topic_id: body.topic.topic_id,
            topic_created_op: body.topic.topic_created_op,
            partitions_count: body.partitions_count,
            removed_partition_ids: body.removed_partition_ids,
            requested_op: body.requested_op,
        }
    }
}

impl From<WirePartitionsItemBody> for MetadataPartitionsItem {
    fn from(body: WirePartitionsItemBody) -> Self {
        Self {
            stream_id: body.topic.stream.stream_id,
            stream_created_op: body.topic.stream.stream_created_op,
            topic_id: body.topic.topic_id,
            topic_created_op: body.topic.topic_created_op,
            partitions_count: body.partitions_count,
        }
    }
}

impl From<WireConsumerGroupCreatedBody> for MetadataConsumerGroupCreated {
    fn from(body: WireConsumerGroupCreatedBody) -> Self {
        Self {
            stream_id: body.topic.stream.stream_id,
            stream_created_op: body.topic.stream.stream_created_op,
            topic_id: body.topic.topic_id,
            topic_created_op: body.topic.topic_created_op,
            group_id: body.group_id,
            name: body.name.as_str().to_owned(),
            consumer_groups_count: body.consumer_groups_count,
        }
    }
}

impl From<WireConsumerGroupDeletedBody> for MetadataConsumerGroupDeleted {
    fn from(body: WireConsumerGroupDeletedBody) -> Self {
        Self {
            stream_id: body.topic.stream.stream_id,
            stream_created_op: body.topic.stream.stream_created_op,
            topic_id: body.topic.topic_id,
            topic_created_op: body.topic.topic_created_op,
            group_id: body.group_id,
            name: body.name.as_str().to_owned(),
            consumer_groups_count: body.consumer_groups_count,
            requested_op: body.requested_op,
        }
    }
}

impl From<WireConsumerGroupItemBody> for MetadataConsumerGroupItem {
    fn from(body: WireConsumerGroupItemBody) -> Self {
        Self {
            stream_id: body.topic.stream.stream_id,
            stream_created_op: body.topic.stream.stream_created_op,
            topic_id: body.topic.topic_id,
            topic_created_op: body.topic.topic_created_op,
            group_id: body.group_id,
            name: body.name.as_str().to_owned(),
        }
    }
}

impl From<WireMemberJoinedBody> for MetadataMemberJoined {
    fn from(body: WireMemberJoinedBody) -> Self {
        Self {
            stream_id: body.topic.stream.stream_id,
            stream_created_op: body.topic.stream.stream_created_op,
            topic_id: body.topic.topic_id,
            topic_created_op: body.topic.topic_created_op,
            group_id: body.group_id,
            member_id: body.member_id,
            client_id: body.client_id,
            members_count: body.members_count,
            partition_ids: body.partition_ids,
        }
    }
}

impl From<WireMemberLeftBody> for MetadataMemberLeft {
    fn from(body: WireMemberLeftBody) -> Self {
        Self {
            stream_id: body.topic.stream.stream_id,
            stream_created_op: body.topic.stream.stream_created_op,
            topic_id: body.topic.topic_id,
            topic_created_op: body.topic.topic_created_op,
            group_id: body.group_id,
            member_id: body.member_id,
            client_id: body.client_id,
            members_count: body.members_count,
            reason: body.reason.into(),
        }
    }
}

impl From<WirePartitionHandedOverBody> for MetadataPartitionHandedOver {
    fn from(body: WirePartitionHandedOverBody) -> Self {
        Self {
            stream_id: body.topic.stream.stream_id,
            stream_created_op: body.topic.stream.stream_created_op,
            topic_id: body.topic.topic_id,
            topic_created_op: body.topic.topic_created_op,
            group_id: body.group_id,
            partition_id: body.partition_id,
            member_id: body.member_id,
        }
    }
}

impl From<WireMemberItemBody> for MetadataMemberItem {
    fn from(body: WireMemberItemBody) -> Self {
        Self {
            stream_id: body.topic.stream.stream_id,
            stream_created_op: body.topic.stream.stream_created_op,
            topic_id: body.topic.topic_id,
            topic_created_op: body.topic.topic_created_op,
            group_id: body.group_id,
            member_id: body.member_id,
            client_id: body.client_id,
            partition_ids: body.partition_ids,
        }
    }
}

impl TryFrom<WireUserBody> for MetadataUserChange {
    type Error = IggyError;

    fn try_from(body: WireUserBody) -> Result<Self, Self::Error> {
        Ok(Self {
            user_id: body.key.user_id,
            user_created_at: body.key.user_created_at,
            username: body.username.as_str().to_owned(),
            status: UserStatus::from_code(body.status)?,
            permissions: body.permissions.map(Permissions::from),
            options: body.options.try_into()?,
        })
    }
}

impl From<WireUserIdentityBody> for MetadataUserIdentity {
    fn from(body: WireUserIdentityBody) -> Self {
        Self {
            user_id: body.key.user_id,
            user_created_at: body.key.user_created_at,
            username: body.username.as_str().to_owned(),
        }
    }
}

impl From<WireUserPermissionsBody> for MetadataUserPermissionsUpdated {
    fn from(body: WireUserPermissionsBody) -> Self {
        Self {
            user_id: body.key.user_id,
            user_created_at: body.key.user_created_at,
            username: body.username.as_str().to_owned(),
            permissions: body.permissions.map(Permissions::from),
            reason: body.reason.into(),
        }
    }
}

impl From<WireTokenBody> for MetadataTokenChange {
    fn from(body: WireTokenBody) -> Self {
        Self {
            user_id: body.user.user_id,
            user_created_at: body.user.user_created_at,
            name: body.name.as_str().to_owned(),
            expiry: (body.expiry != 0).then_some(body.expiry),
        }
    }
}

impl From<WireTokenDeletedBody> for MetadataTokenDeleted {
    fn from(body: WireTokenDeletedBody) -> Self {
        Self {
            user_id: body.user.user_id,
            user_created_at: body.user.user_created_at,
            name: body.name.as_str().to_owned(),
            reason: body.reason.into(),
        }
    }
}

impl From<WireSessionBody> for MetadataSession {
    fn from(body: WireSessionBody) -> Self {
        Self {
            user_id: body.user.user_id,
            user_created_at: body.user.user_created_at,
            client_id: body.client_id,
            session: body.session,
        }
    }
}

impl From<WireSessionClosedBody> for MetadataSessionClosed {
    fn from(body: WireSessionClosedBody) -> Self {
        Self {
            user_id: body.user.user_id,
            user_created_at: body.user.user_created_at,
            client_id: body.client_id,
            session: body.session,
            reason: body.reason.into(),
        }
    }
}

impl TryFrom<WireMetadataChange> for MetadataChange {
    type Error = IggyError;

    fn try_from(change: WireMetadataChange) -> Result<Self, Self::Error> {
        Ok(match change {
            WireMetadataChange::StreamCreated(body) => Self::StreamCreated(body.try_into()?),
            WireMetadataChange::StreamUpdated(body) => Self::StreamUpdated(body.try_into()?),
            WireMetadataChange::StreamDeleted(body) => Self::StreamDeleted(body.into()),
            WireMetadataChange::TopicCreated(body) => Self::TopicCreated(body.try_into()?),
            WireMetadataChange::TopicUpdated(body) => Self::TopicUpdated(body.try_into()?),
            WireMetadataChange::TopicDeleted(body) => Self::TopicDeleted(body.into()),
            WireMetadataChange::PartitionsChanged(body) => Self::PartitionsChanged(body.into()),
            WireMetadataChange::ConsumerGroupCreated(body) => {
                Self::ConsumerGroupCreated(body.into())
            }
            WireMetadataChange::ConsumerGroupDeleted(body) => {
                Self::ConsumerGroupDeleted(body.into())
            }
            WireMetadataChange::ConsumerGroupMemberJoined(body) => {
                Self::ConsumerGroupMemberJoined(body.into())
            }
            WireMetadataChange::ConsumerGroupMemberLeft(body) => {
                Self::ConsumerGroupMemberLeft(body.into())
            }
            WireMetadataChange::ConsumerGroupPartitionHandedOver(body) => {
                Self::ConsumerGroupPartitionHandedOver(body.into())
            }
            WireMetadataChange::UserCreated(body) => Self::UserCreated(body.try_into()?),
            WireMetadataChange::UserUpdated(body) => Self::UserUpdated(body.try_into()?),
            WireMetadataChange::UserDeleted(body) => Self::UserDeleted(body.into()),
            WireMetadataChange::UserPasswordChanged(body) => Self::UserPasswordChanged(body.into()),
            WireMetadataChange::UserPermissionsUpdated(body) => {
                Self::UserPermissionsUpdated(body.into())
            }
            WireMetadataChange::PersonalAccessTokenCreated(body) => {
                Self::PersonalAccessTokenCreated(body.into())
            }
            WireMetadataChange::PersonalAccessTokenDeleted(body) => {
                Self::PersonalAccessTokenDeleted(body.into())
            }
            WireMetadataChange::SessionRegistered(body) => Self::SessionRegistered(body.into()),
            WireMetadataChange::SessionClosed(body) => Self::SessionClosed(body.into()),
        })
    }
}

impl TryFrom<WireMetadataCurrentItem> for MetadataCurrentItem {
    type Error = IggyError;

    fn try_from(item: WireMetadataCurrentItem) -> Result<Self, Self::Error> {
        Ok(match item {
            WireMetadataCurrentItem::Stream(body) => Self::Stream(body.try_into()?),
            WireMetadataCurrentItem::Topic(body) => Self::Topic(body.try_into()?),
            WireMetadataCurrentItem::Partitions(body) => Self::Partitions(body.into()),
            WireMetadataCurrentItem::ConsumerGroup(body) => Self::ConsumerGroup(body.into()),
            WireMetadataCurrentItem::ConsumerGroupMember(body) => {
                Self::ConsumerGroupMember(body.into())
            }
            WireMetadataCurrentItem::User(body) => Self::User(body.try_into()?),
            WireMetadataCurrentItem::PersonalAccessToken(body) => {
                Self::PersonalAccessToken(body.into())
            }
            WireMetadataCurrentItem::Session(body) => Self::Session(body.into()),
        })
    }
}

impl TryFrom<WireMetadataChangeRecord> for MetadataChangeRecord {
    type Error = IggyError;

    fn try_from(record: WireMetadataChangeRecord) -> Result<Self, Self::Error> {
        let change = match WireMetadataChange::decode(record.group, record.kind, &record.body)
            .map_err(|_| IggyError::InvalidFormat)?
        {
            Some(change) => change.try_into()?,
            None => MetadataChange::Unknown {
                group: record.group,
                kind: record.kind,
                body: record.body,
            },
        };
        Ok(Self {
            op: record.op,
            ordinal: record.ordinal,
            checksum: record.checksum,
            timestamp: record.timestamp,
            actor_user_id: record.actor_user_id,
            change,
        })
    }
}

impl TryFrom<WireMetadataItem> for MetadataCurrentItem {
    type Error = IggyError;

    fn try_from(item: WireMetadataItem) -> Result<Self, Self::Error> {
        match WireMetadataCurrentItem::decode(item.group, &item.body)
            .map_err(|_| IggyError::InvalidFormat)?
        {
            Some(decoded) => decoded.try_into(),
            None => Ok(Self::Unknown {
                group: item.group,
                body: item.body,
            }),
        }
    }
}

impl TryFrom<GetMetadataChangesResponse> for MetadataChanges {
    type Error = IggyError;

    fn try_from(response: GetMetadataChangesResponse) -> Result<Self, Self::Error> {
        Ok(Self {
            status: response.status.into(),
            phase: response.phase.into(),
            log_id: response.log_id,
            oldest_op: response.oldest_op,
            retained_since: response.retained_since,
            head_op: response.head_op,
            next: response.next.map(Into::into),
            has_more: response.has_more,
            records: response
                .records
                .into_iter()
                .map(MetadataChangeRecord::try_from)
                .collect::<Result<_, _>>()?,
            items: response
                .items
                .into_iter()
                .map(MetadataCurrentItem::try_from)
                .collect::<Result<_, _>>()?,
            current_page_token: response.current_page_token.map(Into::into),
        })
    }
}

impl From<&MetadataChangesQuery> for GetMetadataChangesRequest {
    fn from(query: &MetadataChangesQuery) -> Self {
        let start = match &query.start {
            MetadataChangeStart::Earliest => WireMetadataChangeStart::Earliest,
            MetadataChangeStart::Latest => WireMetadataChangeStart::Latest,
            MetadataChangeStart::After(cursor) => WireMetadataChangeStart::After((*cursor).into()),
            MetadataChangeStart::Current(token) => {
                WireMetadataChangeStart::Current(token.clone().map(Into::into))
            }
        };
        let mut groups: Vec<u8> = query
            .groups
            .iter()
            .map(|group| WireMetadataChangeGroup::from(*group).as_u8())
            .collect();
        if groups.len() > usize::from(u8::MAX) {
            let mut seen = [false; 256];
            groups.retain(|group| !std::mem::replace(&mut seen[usize::from(*group)], true));
        }
        Self {
            start,
            since: query.since.map(|since| since.as_micros()),
            until: query.until.map(|until| until.as_micros()),
            limit: query.limit,
            groups,
        }
    }
}

impl TryFrom<GetMetadataChangesRequest> for MetadataChangesQuery {
    type Error = IggyError;

    fn try_from(request: GetMetadataChangesRequest) -> Result<Self, Self::Error> {
        let timestamp =
            |micros| IggyTimestamp::checked_from_micros(micros).ok_or(IggyError::InvalidCommand);
        let start = match request.start {
            WireMetadataChangeStart::Earliest => MetadataChangeStart::Earliest,
            WireMetadataChangeStart::Latest => MetadataChangeStart::Latest,
            WireMetadataChangeStart::After(cursor) => MetadataChangeStart::After(cursor.into()),
            WireMetadataChangeStart::Current(token) => {
                MetadataChangeStart::Current(token.map(Into::into))
            }
        };
        let groups = request
            .groups
            .iter()
            .map(|&code| {
                WireMetadataChangeGroup::from_code(code)
                    .map(MetadataChangeGroup::from)
                    .map_err(|_| IggyError::InvalidCommand)
            })
            .collect::<Result<_, _>>()?;
        Ok(Self {
            start,
            groups,
            since: request.since.map(timestamp).transpose()?,
            until: request.until.map(timestamp).transpose()?,
            limit: request.limit,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MetadataChangeCursor, MetadataChangeStatus};
    use iggy_binary_protocol::primitives::metadata_changes::{
        WireMetadataChangePhase, WireMetadataChangeStatus, WireStreamKey, WireTopicKey, WireUserKey,
    };
    use iggy_binary_protocol::{WireDecode, WireName};
    use iggy_binary_protocol::{WireEncode, WireOptions};

    #[test]
    fn given_many_duplicate_groups_when_encoding_should_preserve_every_requested_group() {
        let mut groups = vec![crate::MetadataChangeGroup::Streams; 255];
        groups.push(crate::MetadataChangeGroup::Users);
        let query = MetadataChangesQuery {
            groups,
            ..MetadataChangesQuery::default()
        };
        let bytes = GetMetadataChangesRequest::from(&query).to_bytes();
        let decoded =
            MetadataChangesQuery::try_from(GetMetadataChangesRequest::decode_from(&bytes).unwrap())
                .unwrap();

        assert_eq!(decoded.validate().unwrap(), query.validate().unwrap());
    }

    fn topic_key() -> WireTopicKey {
        WireTopicKey {
            stream: WireStreamKey {
                stream_id: 3,
                stream_created_op: 40,
            },
            topic_id: 1,
            topic_created_op: 41,
        }
    }

    fn record(change: &WireMetadataChange) -> WireMetadataChangeRecord {
        WireMetadataChangeRecord {
            op: 41,
            ordinal: 0,
            checksum: 99,
            timestamp: 7,
            actor_user_id: Some(4),
            group: change.group().as_u8(),
            kind: change.kind().as_u8(),
            body: change.body_bytes(),
        }
    }

    #[test]
    fn given_topic_created_record_when_converting_from_wire_should_flatten_the_identity() {
        let change = WireMetadataChange::TopicCreated(WireTopicBody {
            key: topic_key(),
            created_at: 5,
            name: WireName::new("events").unwrap(),
            partitions_count: 8,
            options: WireChangeOptions::Readable {
                explicit: WireOptions::empty(),
                derived: WireOptions::empty(),
            },
        });
        let converted = MetadataChangeRecord::try_from(record(&change)).unwrap();
        let MetadataChange::TopicCreated(body) = converted.change else {
            panic!("expected a topic record");
        };
        assert_eq!(
            (
                body.stream_id,
                body.stream_created_op,
                body.topic_id,
                body.topic_created_op
            ),
            (3, 40, 1, 41)
        );
        assert_eq!(body.partitions_count, 8);
        assert_eq!(converted.actor_user_id, Some(4));
    }

    #[test]
    fn given_unknown_kind_when_converting_from_wire_should_keep_the_raw_body() {
        let wire = WireMetadataChangeRecord {
            group: 2,
            kind: 19,
            body: bytes::Bytes::from_static(&[1, 2, 3]),
            ..record(&WireMetadataChange::SessionRegistered(WireSessionBody {
                user: WireUserKey {
                    user_id: 1,
                    user_created_at: 2,
                },
                client_id: 3,
                session: 4,
            }))
        };
        let converted = MetadataChangeRecord::try_from(wire).unwrap();
        assert_eq!(
            converted.change,
            MetadataChange::Unknown {
                group: 2,
                kind: 19,
                body: bytes::Bytes::from_static(&[1, 2, 3])
            }
        );
    }

    #[test]
    fn given_malformed_known_body_when_converting_from_wire_should_refuse() {
        let mut wire = record(&WireMetadataChange::SessionRegistered(WireSessionBody {
            user: WireUserKey {
                user_id: 1,
                user_created_at: 2,
            },
            client_id: 3,
            session: 4,
        }));
        wire.body = wire.body.slice(..wire.body.len() - 1);
        assert_eq!(
            MetadataChangeRecord::try_from(wire),
            Err(IggyError::InvalidFormat)
        );
    }

    #[test]
    fn given_token_without_expiry_when_converting_from_wire_should_report_no_expiry() {
        let token = MetadataTokenChange::from(WireTokenBody {
            user: WireUserKey {
                user_id: 1,
                user_created_at: 2,
            },
            name: WireName::new("ci").unwrap(),
            expiry: 0,
        });
        assert_eq!(token.expiry, None);
    }

    #[test]
    fn given_query_when_round_tripping_through_the_wire_request_should_preserve_it() {
        let query = MetadataChangesQuery {
            start: MetadataChangeStart::After(MetadataChangeCursor {
                log_id: 1,
                op: 2,
                checksum: 3,
                ordinal: 4,
            }),
            groups: vec![MetadataChangeGroup::Users, MetadataChangeGroup::Sessions],
            since: Some(IggyTimestamp::from(10)),
            until: Some(IggyTimestamp::from(20)),
            limit: 50,
        };
        let request = GetMetadataChangesRequest::from(&query);
        assert_eq!(request.groups, vec![6, 8]);
        let decoded = GetMetadataChangesRequest::decode_from(&request.to_bytes()).unwrap();
        assert_eq!(MetadataChangesQuery::try_from(decoded).unwrap(), query);
    }

    #[test]
    fn given_unserved_group_code_when_converting_request_should_refuse() {
        let request = GetMetadataChangesRequest {
            start: WireMetadataChangeStart::Earliest,
            since: None,
            until: None,
            limit: 10,
            groups: vec![1, 200],
        };
        assert_eq!(
            MetadataChangesQuery::try_from(request),
            Err(IggyError::InvalidCommand)
        );
    }

    #[test]
    fn given_wire_response_when_converting_should_map_status_phase_and_positions() {
        let response = GetMetadataChangesResponse {
            status: WireMetadataChangeStatus::Expired,
            phase: WireMetadataChangePhase::Changes,
            log_id: Some(5),
            oldest_op: 9,
            retained_since: 3,
            head_op: 12,
            next: Some(MetadataChangeCursor::zero(5).into()),
            has_more: false,
            records: vec![],
            items: vec![],
            current_page_token: None,
        };
        let answer = MetadataChanges::try_from(response).unwrap();
        assert_eq!(answer.status, MetadataChangeStatus::Expired);
        assert_eq!(answer.next, Some(MetadataChangeCursor::zero(5)));
        assert_eq!(answer.head_op, 12);
    }
}
