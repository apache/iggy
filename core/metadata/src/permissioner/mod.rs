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

pub mod permissioner_rules;

use std::collections::BTreeMap;

use crate::stm::result::DeletedResource;
use crate::stm::stream::StreamsInner;
use crate::stm::walk::{Directed, range_after};
use ahash::{AHashMap, AHashSet};
use iggy_common::{GlobalPermissions, Permissions, StreamPermissions, TopicPermissions, UserId};

pub struct ReadGrants<'a> {
    global: Option<&'a GlobalPermissions>,
    stream: Option<&'a StreamPermissions>,
}

impl<'a> ReadGrants<'a> {
    #[must_use]
    pub fn for_stream(permissions: Option<&'a Permissions>, stream_id: Option<usize>) -> Self {
        Self {
            global: permissions.map(|permissions| &permissions.global),
            stream: permissions
                .and_then(|permissions| permissions.streams.as_ref()?.get(&stream_id?)),
        }
    }

    /// The streams `permissions` read through stream grants after the id cursor, or
    /// `None` when a global grant reads every stream.
    #[must_use]
    pub fn granted_streams(
        permissions: &'a Permissions,
        after: Option<usize>,
        descending: bool,
    ) -> Option<impl Iterator<Item = usize> + 'a> {
        if Self::for_stream(Some(permissions), None).reads_stream() {
            return None;
        }
        Some(
            permissions
                .streams
                .as_ref()
                .into_iter()
                .flat_map(move |streams| {
                    Directed::new(
                        streams.range(range_after(after.as_ref(), descending)),
                        descending,
                    )
                })
                .filter(|(_, stream)| {
                    Self {
                        global: None,
                        stream: Some(stream),
                    }
                    .reads_stream()
                })
                .map(|(&stream_id, _)| stream_id),
        )
    }

    #[must_use]
    pub fn reads_stream(&self) -> bool {
        self.global
            .is_some_and(|global| global.read_streams || global.manage_streams)
            || self
                .stream
                .is_some_and(|stream| stream.read_stream || stream.manage_stream)
    }

    #[must_use]
    pub fn reads_topics_globally(&self) -> bool {
        self.global.is_some_and(|global| {
            global.read_streams
                || global.manage_streams
                || global.manage_topics
                || global.read_topics
        })
    }

    #[must_use]
    pub fn reads_topic(&self, topic_id: usize) -> bool {
        self.reads_every_topic()
            || self
                .stream
                .and_then(|stream| stream.topics.as_ref()?.get(&topic_id))
                .is_some_and(Self::reads_topic_grant)
    }

    /// The topics the topic grants read after the id cursor, or `None` when the grants
    /// read every topic of the stream.
    #[must_use]
    pub fn granted_topics(
        &self,
        after: Option<usize>,
        descending: bool,
    ) -> Option<impl Iterator<Item = usize> + 'a> {
        if self.reads_every_topic() {
            return None;
        }
        Some(
            self.stream
                .and_then(|stream| stream.topics.as_ref())
                .into_iter()
                .flat_map(move |topics| {
                    Directed::new(
                        topics.range(range_after(after.as_ref(), descending)),
                        descending,
                    )
                })
                .filter(|(_, grant)| Self::reads_topic_grant(grant))
                .map(|(&topic_id, _)| topic_id),
        )
    }

    #[must_use]
    pub fn reads_every_topic(&self) -> bool {
        self.reads_topics_globally()
            || self.stream.is_some_and(|stream| {
                stream.manage_stream
                    || stream.read_stream
                    || stream.manage_topics
                    || stream.read_topics
            })
    }

    const fn reads_topic_grant(grant: &TopicPermissions) -> bool {
        grant.read_topic || grant.manage_topic
    }
}

#[derive(Debug, Default, Clone)]
pub struct GrantsByCreation {
    pub streams: Vec<(u64, usize)>,
    pub topics: BTreeMap<usize, Vec<(u64, usize)>>,
}

impl GrantsByCreation {
    #[must_use]
    pub fn from_catalog(permissions: &Permissions, streams: &StreamsInner) -> Self {
        Self::from_grants(
            permissions,
            |id| Some(streams.items.get(id)?.created_at.as_micros()),
            |id, topic| {
                Some(
                    streams
                        .items
                        .get(id)?
                        .topics
                        .get(topic)?
                        .created_at
                        .as_micros(),
                )
            },
        )
    }

    pub(crate) fn from_grants(
        permissions: &Permissions,
        stream_time: impl Fn(usize) -> Option<u64>,
        topic_time: impl Fn(usize, usize) -> Option<u64>,
    ) -> Self {
        let mut ordered = Self::default();
        if let Some(granted) = ReadGrants::granted_streams(permissions, None, false) {
            ordered.streams = granted
                .filter_map(|id| Some((stream_time(id)?, id)))
                .collect();
            ordered.streams.sort_unstable();
        }
        for &stream_id in permissions
            .streams
            .iter()
            .flat_map(|streams| streams.keys())
        {
            let grants = ReadGrants::for_stream(Some(permissions), Some(stream_id));
            if let Some(granted) = grants.granted_topics(None, false) {
                let mut keys: Vec<_> = granted
                    .filter_map(|id| Some((topic_time(stream_id, id)?, id)))
                    .collect();
                if !keys.is_empty() {
                    keys.sort_unstable();
                    ordered.topics.insert(stream_id, keys);
                }
            }
        }
        ordered
    }

    pub(crate) fn remove(&mut self, deleted: DeletedResource) {
        match deleted {
            DeletedResource::Stream(id) => {
                self.streams.retain(|key| key.1 != id);
                self.topics.remove(&id);
            }
            DeletedResource::Topic {
                stream_id,
                topic_id,
            } => {
                if let Some(keys) = self.topics.get_mut(&stream_id) {
                    keys.retain(|key| key.1 != topic_id);
                    if keys.is_empty() {
                        self.topics.remove(&stream_id);
                    }
                }
            }
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct Permissioner {
    pub users_permissions: AHashMap<UserId, GlobalPermissions>,
    pub users_streams_permissions: AHashMap<(UserId, usize), StreamPermissions>,
    pub users_that_can_poll_messages_from_all_streams: AHashSet<UserId>,
    pub users_that_can_send_messages_to_all_streams: AHashSet<UserId>,
    pub users_that_can_poll_messages_from_specific_streams: AHashSet<(UserId, usize)>,
    pub users_that_can_send_messages_to_specific_streams: AHashSet<(UserId, usize)>,
}

impl Permissioner {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn init_permissions_for_user(&mut self, user_id: UserId, permissions: Option<Permissions>) {
        let Some(permissions) = permissions else {
            return;
        };
        if permissions.global.poll_messages {
            self.users_that_can_poll_messages_from_all_streams
                .insert(user_id);
        }

        if permissions.global.send_messages {
            self.users_that_can_send_messages_to_all_streams
                .insert(user_id);
        }

        self.users_permissions.insert(user_id, permissions.global);
        let Some(streams) = permissions.streams else {
            return;
        };
        for (stream_id, stream) in streams {
            if stream.poll_messages {
                self.users_that_can_poll_messages_from_specific_streams
                    .insert((user_id, stream_id));
            }

            if stream.send_messages {
                self.users_that_can_send_messages_to_specific_streams
                    .insert((user_id, stream_id));
            }

            self.users_streams_permissions
                .insert((user_id, stream_id), stream);
        }
    }

    pub fn update_permissions_for_user(
        &mut self,
        user_id: UserId,
        permissions: Option<Permissions>,
    ) {
        self.delete_permissions_for_user(user_id);
        self.init_permissions_for_user(user_id, permissions);
    }

    pub fn delete_permissions_for_user(&mut self, user_id: UserId) {
        self.users_permissions.remove(&user_id);
        self.users_that_can_poll_messages_from_all_streams
            .remove(&user_id);
        self.users_that_can_send_messages_to_all_streams
            .remove(&user_id);
        self.users_streams_permissions
            .retain(|(id, _), _| *id != user_id);
        self.users_that_can_poll_messages_from_specific_streams
            .retain(|(id, _)| *id != user_id);
        self.users_that_can_send_messages_to_specific_streams
            .retain(|(id, _)| *id != user_id);
    }

    fn read_grants(&self, user_id: UserId, stream_id: usize) -> ReadGrants<'_> {
        ReadGrants {
            global: self.users_permissions.get(&user_id),
            stream: self.users_streams_permissions.get(&(user_id, stream_id)),
        }
    }
}
