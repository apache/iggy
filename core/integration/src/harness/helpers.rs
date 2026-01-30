/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

//! Helper functions for common test operations.

use iggy::prelude::{
    DEFAULT_ROOT_PASSWORD, DEFAULT_ROOT_USERNAME, GlobalPermissions, Identifier, IdentityInfo,
    Permissions, StreamClient, UserClient, UserStatus,
};

pub const USER_PASSWORD: &str = "secret";

/// Login as root user.
pub async fn login_root(client: &impl UserClient) -> IdentityInfo {
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .expect("Failed to login as root")
}

/// Login as a specific user with the default test password.
pub async fn login_user(client: &impl UserClient, username: &str) -> IdentityInfo {
    client
        .login_user(username, USER_PASSWORD)
        .await
        .expect("Failed to login user")
}

/// Create a user with full permissions.
pub async fn create_user(client: &impl UserClient, username: &str) {
    client
        .create_user(
            username,
            USER_PASSWORD,
            UserStatus::Active,
            Some(full_permissions()),
        )
        .await
        .expect("Failed to create user");
}

/// Delete a user by username.
pub async fn delete_user(client: &impl UserClient, username: &str) {
    client
        .delete_user(&Identifier::named(username).expect("Invalid username"))
        .await
        .expect("Failed to delete user");
}

/// Assert the system is in a clean state (no streams, only root user).
pub async fn assert_clean_system<C: StreamClient + UserClient>(client: &C) {
    let streams = client.get_streams().await.expect("Failed to get streams");
    assert!(
        streams.is_empty(),
        "Expected no streams, found {}",
        streams.len()
    );

    let users = client.get_users().await.expect("Failed to get users");
    assert_eq!(
        users.len(),
        1,
        "Expected only root user, found {} users",
        users.len()
    );
}

fn full_permissions() -> Permissions {
    Permissions {
        global: GlobalPermissions {
            manage_servers: true,
            read_servers: true,
            manage_users: true,
            read_users: true,
            manage_streams: true,
            read_streams: true,
            manage_topics: true,
            read_topics: true,
            poll_messages: true,
            send_messages: true,
        },
        streams: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_full_permissions() {
        let perms = full_permissions();
        assert!(perms.global.manage_servers);
        assert!(perms.global.poll_messages);
        assert!(perms.global.send_messages);
    }
}
