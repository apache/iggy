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

use crate::permissioner::Permissioner;
use iggy_common::IggyError;

#[allow(clippy::missing_errors_doc)]
impl Permissioner {
    pub fn get_stats(&self, user_id: u32) -> Result<(), IggyError> {
        self.get_server_info(user_id)
    }

    pub fn get_clients(&self, user_id: u32) -> Result<(), IggyError> {
        self.get_server_info(user_id)
    }

    pub fn get_client(&self, user_id: u32) -> Result<(), IggyError> {
        self.get_server_info(user_id)
    }

    /// The archive carries raw server logs and host diagnostics, so read-only
    /// server access is not enough.
    pub fn get_snapshot(&self, user_id: u32) -> Result<(), IggyError> {
        self.manage_servers(user_id)
    }

    /// The feed exposes deployment-wide state, sessions included, so it takes
    /// the gate of the snapshot whatever groups the caller selects.
    pub fn get_metadata_changes(&self, user_id: u32) -> Result<(), IggyError> {
        self.manage_servers(user_id)
    }

    fn get_server_info(&self, user_id: u32) -> Result<(), IggyError> {
        if let Some(global_permissions) = self.users_permissions.get(&user_id)
            && (global_permissions.manage_servers || global_permissions.read_servers)
        {
            return Ok(());
        }

        Err(IggyError::Unauthorized)
    }

    fn manage_servers(&self, user_id: u32) -> Result<(), IggyError> {
        if let Some(global_permissions) = self.users_permissions.get(&user_id)
            && global_permissions.manage_servers
        {
            return Ok(());
        }

        Err(IggyError::Unauthorized)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use iggy_common::{GlobalPermissions, Permissions};

    const USER_ID: u32 = 7;

    #[test]
    fn given_read_servers_only_when_requesting_a_snapshot_should_be_unauthorized() {
        let permissioner = permissioner_with(GlobalPermissions {
            read_servers: true,
            ..GlobalPermissions::default()
        });

        assert!(matches!(
            permissioner.get_snapshot(USER_ID),
            Err(IggyError::Unauthorized)
        ));
        permissioner
            .get_stats(USER_ID)
            .expect("read_servers still reads stats");
        permissioner
            .get_clients(USER_ID)
            .expect("read_servers still lists clients");
    }

    #[test]
    fn given_manage_servers_when_requesting_a_snapshot_should_be_allowed() {
        let permissioner = permissioner_with(GlobalPermissions {
            manage_servers: true,
            ..GlobalPermissions::default()
        });

        permissioner
            .get_snapshot(USER_ID)
            .expect("manage_servers grants the snapshot");
    }

    #[test]
    fn given_no_server_permission_when_requesting_a_snapshot_should_be_unauthorized() {
        let permissioner = permissioner_with(GlobalPermissions::default());

        assert!(matches!(
            permissioner.get_snapshot(USER_ID),
            Err(IggyError::Unauthorized)
        ));
    }

    #[test]
    fn given_read_grants_without_manage_servers_when_reading_metadata_changes_should_be_unauthorized()
     {
        let permissioner = permissioner_with(GlobalPermissions {
            read_servers: true,
            read_streams: true,
            read_topics: true,
            read_users: true,
            manage_streams: true,
            manage_users: true,
            ..GlobalPermissions::default()
        });

        assert!(matches!(
            permissioner.get_metadata_changes(USER_ID),
            Err(IggyError::Unauthorized)
        ));
    }

    #[test]
    fn given_manage_servers_when_reading_metadata_changes_should_be_allowed() {
        let permissioner = permissioner_with(GlobalPermissions {
            manage_servers: true,
            ..GlobalPermissions::default()
        });

        permissioner
            .get_metadata_changes(USER_ID)
            .expect("manage_servers grants the feed");
    }

    fn permissioner_with(global: GlobalPermissions) -> Permissioner {
        let mut permissioner = Permissioner::new();
        permissioner.init_permissions_for_user(
            USER_ID,
            Some(Permissions {
                global,
                streams: None,
            }),
        );
        permissioner
    }
}
