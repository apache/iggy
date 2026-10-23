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

use crate::change_log::query::{CursorCheck, LogView, MAX_PAGE_BYTES};
use crate::change_log::store::ChangeLog;
use crate::change_log::{change_options, stream_body, user_body};
use crate::stm::stream::StreamsInner;
use crate::stm::user::UsersInner;
use bytes::{BufMut, Bytes, BytesMut};
use consensus::ClientTable;
use iggy_binary_protocol::WireName;
use iggy_binary_protocol::codec::{WireDecode, WireEncode, read_u32_le, read_u64_le};
use iggy_binary_protocol::primitives::metadata_changes::{
    WireConsumerGroupItemBody, WireMemberItemBody, WireMetadataChangeGroup,
    WireMetadataChangePhase, WireMetadataChangeStatus, WireMetadataCurrentItem,
    WireMetadataCurrentPageToken, WirePartitionsItemBody, WireSessionBody, WireTokenBody,
    WireTopicItemBody, WireUserKey,
};
use iggy_binary_protocol::responses::system::{GetMetadataChangesResponse, WireMetadataItem};
use iggy_common::{IggyError, MetadataChangeGroup, MetadataChangeGroups, OptionsScope};
use std::ops::Bound;
use std::sync::Arc;

/// The applied state a current page projects, all of one image.
pub struct CurrentState<'a> {
    pub streams: &'a StreamsInner,
    pub users: &'a UsersInner,
    /// The client table aligned with the applied state, `None` while the
    /// protection table runs ahead of it.
    pub sessions: Option<&'a ClientTable>,
}

/// One current state page: the projected items after the token's key, in
/// group order then key order.
///
/// # Errors
/// `InvalidCommand` for a last key that does not fit its group, which the
/// token decode refuses first.
pub fn current_page(
    log: &ChangeLog,
    head_op: u64,
    groups: MetadataChangeGroups,
    token: Option<&WireMetadataCurrentPageToken>,
    limit: u32,
    state: &CurrentState<'_>,
) -> Result<GetMetadataChangesResponse, IggyError> {
    let view = LogView::new(log, groups, head_op);
    let answer = |status, next| GetMetadataChangesResponse {
        phase: WireMetadataChangePhase::Current,
        ..view.answer(status, next)
    };
    let behind = || GetMetadataChangesResponse {
        current_page_token: token.cloned(),
        ..answer(
            WireMetadataChangeStatus::Behind,
            token.map(|token| token.anchor),
        )
    };
    let Some(log_id) = view.log().log_id() else {
        return Ok(behind());
    };
    let anchor = match token {
        None => view.latest_cursor(log_id),
        Some(token) if token.anchor.log_id != log_id => {
            return Ok(answer(WireMetadataChangeStatus::Reset, None));
        }
        Some(token) if view.head_op() < token.required_head => return Ok(behind()),
        Some(token) => match view.check(token.anchor) {
            CursorCheck::Valid(_) => token.anchor,
            CursorCheck::Behind => return Ok(behind()),
            CursorCheck::Expired => {
                return Ok(answer(WireMetadataChangeStatus::Expired, None));
            }
            CursorCheck::Reset => {
                return Ok(answer(WireMetadataChangeStatus::Reset, None));
            }
        },
    };
    if groups.contains(MetadataChangeGroup::Sessions) && state.sessions.is_none() {
        return Ok(behind());
    }

    let mut projection = Projection {
        state,
        limit: limit as usize,
        items: Vec::new(),
        bytes: 0,
        last: None,
        more: false,
    };
    let resume = token.map(|token| (MetadataChangeGroup::from(token.group), &token.last_key[..]));
    for group in groups.iter() {
        let after = match resume {
            Some((resume_group, _)) if group < resume_group => continue,
            Some((resume_group, last_key)) if group == resume_group => Some(last_key),
            _ => None,
        };
        if !projection.project(group, after)? {
            break;
        }
    }
    let current_page_token =
        projection
            .more
            .then_some(projection.last)
            .flatten()
            .map(|(group, last_key)| WireMetadataCurrentPageToken {
                anchor,
                required_head: view.head_op(),
                groups_mask: groups.mask(),
                group,
                last_key,
            });
    Ok(GetMetadataChangesResponse {
        has_more: current_page_token.is_some(),
        items: projection.items,
        current_page_token,
        ..answer(WireMetadataChangeStatus::Ok, Some(anchor))
    })
}

struct Projection<'a> {
    state: &'a CurrentState<'a>,
    limit: usize,
    items: Vec<WireMetadataItem>,
    bytes: usize,
    last: Option<(WireMetadataChangeGroup, Bytes)>,
    more: bool,
}

impl Projection<'_> {
    /// Projects `group` after `after`, `false` once the page is full.
    #[allow(clippy::too_many_lines, clippy::cast_possible_truncation)]
    fn project(
        &mut self,
        group: MetadataChangeGroup,
        after: Option<&[u8]>,
    ) -> Result<bool, IggyError> {
        let streams = self.state.streams;
        let users = self.state.users;
        let key = |index: usize| -> Result<Option<usize>, IggyError> {
            after
                .map(|key| {
                    read_u32_le(key, index * 4)
                        .map(|id| id as usize)
                        .map_err(|_| IggyError::InvalidCommand)
                })
                .transpose()
        };
        match group {
            MetadataChangeGroup::Streams => {
                let from = key(0)?.map_or(0, |stream_id| stream_id + 1);
                for (stream_id, stream) in streams.items.iter_from(from) {
                    let Some(body) = stream_body(stream) else {
                        continue;
                    };
                    if !self.push(group, &[stream_id], &WireMetadataCurrentItem::Stream(body)) {
                        return Ok(false);
                    }
                }
            }
            MetadataChangeGroup::Topics | MetadataChangeGroup::Partitions => {
                let (after_stream, after_topic) = (key(0)?, key(1)?);
                for (stream_id, stream) in streams.items.iter_from(after_stream.unwrap_or(0)) {
                    let from = after_topic
                        .filter(|_| after_stream == Some(stream_id))
                        .map_or(0, |topic_id| topic_id + 1);
                    for (topic_id, topic) in stream.topics.iter_from(from) {
                        let key = stream.topic_key(topic);
                        let item = if group == MetadataChangeGroup::Topics {
                            let Ok(name) = WireName::new(topic.name.as_ref()) else {
                                continue;
                            };
                            WireMetadataCurrentItem::Topic(WireTopicItemBody {
                                key,
                                created_at: topic.created_at.as_micros(),
                                name,
                                options: change_options(OptionsScope::Topic, &topic.options),
                            })
                        } else {
                            WireMetadataCurrentItem::Partitions(WirePartitionsItemBody {
                                topic: key,
                                partitions_count: topic.partitions.len() as u32,
                            })
                        };
                        if !self.push(group, &[stream_id, topic_id], &item) {
                            return Ok(false);
                        }
                    }
                }
            }
            MetadataChangeGroup::ConsumerGroups | MetadataChangeGroup::ConsumerGroupMembers => {
                let (after_stream, after_topic, after_group) = (key(0)?, key(1)?, key(2)?);
                let after_member = if group == MetadataChangeGroup::ConsumerGroupMembers {
                    key(3)?
                } else {
                    None
                };
                for (stream_id, stream) in streams.items.iter_from(after_stream.unwrap_or(0)) {
                    let same_stream = after_stream == Some(stream_id);
                    let topic_from = after_topic.filter(|_| same_stream).unwrap_or(0);
                    for (topic_id, topic) in stream.topics.iter_from(topic_from) {
                        let same_topic = same_stream && after_topic == Some(topic_id);
                        // Group ids are monotone and bounded per topic.
                        let mut group_ids: Vec<u64> = topic
                            .consumer_groups
                            .keys()
                            .copied()
                            .filter(|group_id| {
                                after_group.filter(|_| same_topic).is_none_or(|after| {
                                    let after = after as u64;
                                    *group_id > after
                                        || (group == MetadataChangeGroup::ConsumerGroupMembers
                                            && *group_id == after)
                                })
                            })
                            .collect();
                        group_ids.sort_unstable();
                        for group_id in group_ids {
                            let Some(consumer_group) = topic.consumer_groups.get(&group_id) else {
                                continue;
                            };
                            let wire_group_id = group_id as u32;
                            if group == MetadataChangeGroup::ConsumerGroups {
                                let Ok(name) = WireName::new(consumer_group.name.as_ref()) else {
                                    continue;
                                };
                                let item = WireMetadataCurrentItem::ConsumerGroup(
                                    WireConsumerGroupItemBody {
                                        topic: stream.topic_key(topic),
                                        group_id: wire_group_id,
                                        name,
                                    },
                                );
                                if !self.push(
                                    group,
                                    &[stream_id, topic_id, group_id as usize],
                                    &item,
                                ) {
                                    return Ok(false);
                                }
                                continue;
                            }
                            let member_from = after_member
                                .filter(|_| same_topic && after_group == Some(group_id as usize))
                                .map_or(0, |member_id| member_id + 1);
                            for (member_id, member) in consumer_group.members.iter_from(member_from)
                            {
                                let mut partition_ids: Vec<u32> = member
                                    .partitions
                                    .iter()
                                    .map(|&partition_id| partition_id as u32)
                                    .collect();
                                partition_ids.sort_unstable();
                                let item = WireMetadataCurrentItem::ConsumerGroupMember(
                                    WireMemberItemBody {
                                        topic: stream.topic_key(topic),
                                        group_id: wire_group_id,
                                        member_id: member_id as u32,
                                        client_id: member.client_id,
                                        partition_ids,
                                    },
                                );
                                if !self.push(
                                    group,
                                    &[stream_id, topic_id, group_id as usize, member_id],
                                    &item,
                                ) {
                                    return Ok(false);
                                }
                            }
                        }
                    }
                }
            }
            MetadataChangeGroup::Users => {
                let from = key(0)?.map_or(0, |user_id| user_id + 1);
                for (user_id, user) in users.items.iter_from(from) {
                    let Some(body) = user_body(user) else {
                        continue;
                    };
                    if !self.push(group, &[user_id], &WireMetadataCurrentItem::User(body)) {
                        return Ok(false);
                    }
                }
            }
            MetadataChangeGroup::PersonalAccessTokens => {
                let lower = match after {
                    Some(key) => {
                        let user_id = read_u32_le(key, 0).map_err(|_| IggyError::InvalidCommand)?;
                        let (name, _) =
                            WireName::decode(&key[4..]).map_err(|_| IggyError::InvalidCommand)?;
                        Bound::Excluded((user_id, Arc::from(name.as_str())))
                    }
                    None => Bound::Unbounded,
                };
                for (user_id, name) in users
                    .personal_access_token_name_index
                    .range((lower, Bound::Unbounded))
                {
                    let (Some(user), Some(token), Ok(wire_name)) = (
                        users.items.get(*user_id as usize),
                        users
                            .personal_access_tokens
                            .get(user_id)
                            .and_then(|tokens| tokens.get(name)),
                        WireName::new(name.as_ref()),
                    ) else {
                        continue;
                    };
                    let mut key = BytesMut::with_capacity(4 + wire_name.encoded_size());
                    key.put_u32_le(*user_id);
                    wire_name.encode(&mut key);
                    let item = WireMetadataCurrentItem::PersonalAccessToken(WireTokenBody {
                        user: WireUserKey::from(user),
                        name: wire_name,
                        expiry: token.expiry_at.map_or(0, |expiry_at| expiry_at.as_micros()),
                    });
                    if !self.push_keyed(group, key.freeze(), &item) {
                        return Ok(false);
                    }
                }
            }
            MetadataChangeGroup::Sessions => {
                let Some(table) = self.state.sessions else {
                    return Ok(true);
                };
                let after = after
                    .map(|key| read_u64_le(key, 0).map_err(|_| IggyError::InvalidCommand))
                    .transpose()?;
                let mut sessions: Vec<(u64, u128, u32)> = table
                    .active_sessions()
                    .filter(|(_, _, epoch)| after.is_none_or(|after| *epoch > after))
                    .map(|(client_id, user_id, epoch)| (epoch, client_id, user_id))
                    .collect();
                sessions.sort_unstable();
                for (epoch, client_id, user_id) in sessions {
                    let Some(user) = users.items.get(user_id as usize) else {
                        continue;
                    };
                    let item = WireMetadataCurrentItem::Session(WireSessionBody {
                        user: WireUserKey::from(user),
                        client_id,
                        session: epoch,
                    });
                    if !self.push_keyed(group, Bytes::copy_from_slice(&epoch.to_le_bytes()), &item)
                    {
                        return Ok(false);
                    }
                }
            }
        }
        Ok(true)
    }

    /// Adds an item keyed by little-endian `u32` ids, `false` when the page
    /// was already full.
    #[allow(clippy::cast_possible_truncation)]
    fn push(
        &mut self,
        group: MetadataChangeGroup,
        ids: &[usize],
        item: &WireMetadataCurrentItem,
    ) -> bool {
        let mut key = BytesMut::with_capacity(ids.len() * 4);
        for id in ids {
            key.put_u32_le(*id as u32);
        }
        self.push_keyed(group, key.freeze(), item)
    }

    fn push_keyed(
        &mut self,
        group: MetadataChangeGroup,
        key: Bytes,
        item: &WireMetadataCurrentItem,
    ) -> bool {
        let group = WireMetadataChangeGroup::from(group);
        let item = WireMetadataItem {
            group: group.as_u8(),
            body: Bytes::from(item.body_bytes()),
        };
        let bytes = self.bytes + item.encoded_size();
        if self.items.len() == self.limit || (!self.items.is_empty() && bytes > MAX_PAGE_BYTES) {
            self.more = true;
            return false;
        }
        self.bytes = bytes;
        self.items.push(item);
        self.last = Some((group, key));
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stm::stream::StreamsInner;
    use crate::stm::user::UsersInner;
    use iggy_binary_protocol::primitives::metadata_changes::{WireStreamKey, WireTopicKey};

    #[test]
    fn given_items_past_the_byte_budget_when_projecting_should_stop_before_the_overflow() {
        let streams = StreamsInner::new();
        let users = UsersInner::new();
        let state = CurrentState {
            streams: &streams,
            users: &users,
            sessions: None,
        };
        let mut projection = Projection {
            state: &state,
            limit: 1000,
            items: Vec::new(),
            bytes: 0,
            last: None,
            more: false,
        };
        let member = |member_id: u32, partitions: usize| {
            WireMetadataCurrentItem::ConsumerGroupMember(WireMemberItemBody {
                topic: WireTopicKey {
                    stream: WireStreamKey {
                        stream_id: 0,
                        stream_created_op: 1,
                    },
                    topic_id: 0,
                    topic_created_op: 2,
                },
                group_id: 0,
                member_id,
                client_id: 7,
                partition_ids: vec![0; partitions],
            })
        };
        let group = MetadataChangeGroup::ConsumerGroupMembers;

        assert!(
            projection.push(group, &[0, 0, 0, 0], &member(0, MAX_PAGE_BYTES / 4)),
            "an item larger than the budget still makes progress"
        );
        assert!(!projection.push(group, &[0, 0, 0, 1], &member(1, 0)));

        assert_eq!(projection.items.len(), 1);
        assert!(projection.more);
        assert_eq!(
            projection.last,
            Some((
                WireMetadataChangeGroup::ConsumerGroupMembers,
                Bytes::from_static(&[0; 16])
            ))
        );
    }
}
