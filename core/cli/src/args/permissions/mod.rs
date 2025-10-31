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

use self::{global::GlobalPermissionsArg, stream::StreamPermissionsArg};
use ahash::AHashMap;
use clap::ValueEnum;
use iggy::prelude::{Permissions, StreamPermissions, UserStatus};

pub(crate) mod constants;
pub(crate) mod global;
pub(crate) mod stream;
pub(crate) mod topic;

pub(crate) struct PermissionsArgs {
    global: Option<GlobalPermissionsArg>,
    stream: Vec<StreamPermissionsArg>,
}

impl PermissionsArgs {
    pub(crate) fn new(
        global: Option<GlobalPermissionsArg>,
        stream: Option<Vec<StreamPermissionsArg>>,
    ) -> Self {
        Self {
            global,
            stream: stream.unwrap_or_default(),
        }
    }
}

impl From<PermissionsArgs> for Option<Permissions> {
    fn from(value: PermissionsArgs) -> Self {
        let stream_permissions = value
            .stream
            .into_iter()
            .map(|s| (s.stream_id, s.into()))
            .collect::<AHashMap<usize, StreamPermissions>>();

        match (value.global, stream_permissions.is_empty()) {
            (Some(global), true) => Some(Permissions {
                global: global.into(),
                streams: None,
            }),
            (Some(global), false) => Some(Permissions {
                global: global.into(),
                streams: Some(stream_permissions),
            }),
            (None, true) => None,
            (None, false) => Some(Permissions {
                global: Default::default(),
                streams: Some(stream_permissions),
            }),
        }
    }
}

#[derive(Debug, Clone, ValueEnum, PartialEq)]
pub enum UserStatusArg {
    Active,
    Inactive,
}

impl Default for UserStatusArg {
    fn default() -> Self {
        Self::Active
    }
}

impl From<UserStatusArg> for UserStatus {
    fn from(value: UserStatusArg) -> Self {
        match value {
            UserStatusArg::Active => UserStatus::Active,
            UserStatusArg::Inactive => UserStatus::Inactive,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::args::permissions::global::GlobalPermission;

    #[test]
    fn should_convert_empty_permissions_args() {
        let permissions: Option<Permissions> = Option::from(PermissionsArgs::new(None, None));
        assert_eq!(permissions, None);
    }

    #[test]
    fn should_convert_only_global_permissions_args() {
        let global = GlobalPermissionsArg::new(vec![GlobalPermission::ManageServers]);
        let permissions_args: Option<Permissions> =
            Option::from(PermissionsArgs::new(Some(global), None));

        let mut permissions = Permissions::default();
        permissions.global.manage_servers = true;
        assert_eq!(permissions_args, Some(permissions));
    }

    #[test]
    fn should_convert_only_stream_permissions_args() {
        let stream = StreamPermissionsArg::new(1, vec![], vec![]);
        let permissions_args: Option<Permissions> =
            Option::from(PermissionsArgs::new(None, Some(vec![stream])));

        let permissions = Permissions {
            streams: Some(AHashMap::from([(1, StreamPermissions::default())])),
            ..Default::default()
        };
        assert_eq!(permissions_args, Some(permissions));
    }

    #[test]
    fn should_convert_full_permissions_args() {
        let global = GlobalPermissionsArg::new(vec![GlobalPermission::ManageTopics]);
        let stream = StreamPermissionsArg::new(1, vec![], vec![]);
        let permissions_args: Option<Permissions> =
            Option::from(PermissionsArgs::new(Some(global), Some(vec![stream])));

        let mut permissions = Permissions {
            streams: Some(AHashMap::from([(1, StreamPermissions::default())])),
            ..Default::default()
        };
        permissions.global.manage_topics = true;
        assert_eq!(permissions_args, Some(permissions));
    }

    #[test]
    fn should_deserialize_user_status() {
        assert_eq!(
            UserStatusArg::from_str("active", true).unwrap(),
            UserStatusArg::Active
        );
        assert_eq!(
            UserStatusArg::from_str("Active", true).unwrap(),
            UserStatusArg::Active
        );
        assert_eq!(
            UserStatusArg::from_str("inactive", true).unwrap(),
            UserStatusArg::Inactive
        );
        assert_eq!(
            UserStatusArg::from_str("Inactive", true).unwrap(),
            UserStatusArg::Inactive
        );
    }

    #[test]
    fn should_not_deserialize_user_status() {
        let wrong_status = UserStatusArg::from_str("act", true);
        assert!(wrong_status.is_err());
        assert_eq!(wrong_status.unwrap_err(), "invalid variant: act");
        let wrong_status = UserStatusArg::from_str("xyz", true);
        assert!(wrong_status.is_err());
        assert_eq!(wrong_status.unwrap_err(), "invalid variant: xyz");
    }

    #[test]
    fn should_convert_user_status() {
        assert_eq!(UserStatus::from(UserStatusArg::Active), UserStatus::Active);
        assert_eq!(
            UserStatus::from(UserStatusArg::Inactive),
            UserStatus::Inactive
        );
    }
}
