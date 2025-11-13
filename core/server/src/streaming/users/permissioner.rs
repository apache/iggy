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

use crate::streaming::users::user::User;
use ahash::{AHashMap, AHashSet};
use iggy_common::UserId;
use iggy_common::{GlobalPermissions, Permissions, StreamPermissions};

#[derive(Debug, Default)]
pub struct Permissioner {
    pub(super) users_permissions: AHashMap<UserId, GlobalPermissions>,
    pub(super) users_streams_permissions: AHashMap<(UserId, usize), StreamPermissions>,
    pub(super) users_that_can_poll_messages_from_all_streams: AHashSet<UserId>,
    pub(super) users_that_can_send_messages_to_all_streams: AHashSet<UserId>,
    pub(super) users_that_can_poll_messages_from_specific_streams: AHashSet<(UserId, usize)>,
    pub(super) users_that_can_send_messages_to_specific_streams: AHashSet<(UserId, usize)>,
}

impl Permissioner {
    pub fn init(&mut self, users: &[&User]) {
        for user in users {
            self.init_permissions_for_user(user.id, user.permissions.clone());
        }
    }

    pub fn init_permissions_for_user(&mut self, user_id: UserId, permissions: Option<Permissions>) {
        if permissions.is_none() {
            return;
        }

        let permissions = permissions.unwrap();
        if permissions.global.poll_messages {
            self.users_that_can_poll_messages_from_all_streams
                .insert(user_id);
        }

        if permissions.global.send_messages {
            self.users_that_can_send_messages_to_all_streams
                .insert(user_id);
        }

        self.users_permissions.insert(user_id, permissions.global);
        if permissions.streams.is_none() {
            return;
        }

        let streams = permissions.streams.unwrap();
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
}
