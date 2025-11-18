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

use crate::server::scenarios::create_client;
use iggy::prelude::Identifier;
use iggy::prelude::PersonalAccessTokenExpiry;
use iggy::prelude::UserStatus;
use iggy::prelude::defaults::DEFAULT_ROOT_USERNAME;
use iggy::prelude::{GlobalPermissions, Permissions};
use iggy::prelude::{PersonalAccessTokenClient, SEC_IN_MICRO, SystemClient, UserClient};
use integration::test_server::{ClientFactory, assert_clean_system, login_root};

pub async fn run(client_factory: &dyn ClientFactory) {
    let client = create_client(client_factory).await;
    // 1. Ping should be allowed for unauthenticated users
    client.ping().await.unwrap();

    // 2. Any other operation except the login should be forbidden for unauthenticated users
    let get_users = client.get_users().await;
    assert!(get_users.is_err());

    // 3. Login as root user
    let identity_info = login_root(&client).await;

    assert_eq!(identity_info.user_id, 0);

    // 4. List all users
    let users = client.get_users().await.unwrap();
    assert_eq!(users.len(), 1);

    // 5. Get user details
    let user = users.first().unwrap();
    assert_eq!(user.id, 0);
    assert!(user.created_at.as_micros() > 0);
    assert_eq!(user.username, DEFAULT_ROOT_USERNAME);
    assert_eq!(user.status, UserStatus::Active);

    let user = client
        .get_user(&Identifier::named(DEFAULT_ROOT_USERNAME).unwrap())
        .await
        .unwrap()
        .expect("Failed to get user");

    assert_eq!(user.id, 0);
    assert!(user.created_at.as_micros() > 0);
    assert_eq!(user.username, DEFAULT_ROOT_USERNAME);
    assert_eq!(user.status, UserStatus::Active);
    assert!(user.permissions.is_some());

    // 6. Create a new user
    let test_user = "user";
    let test_password = "secret";
    client
        .create_user(
            test_user,
            test_password,
            UserStatus::Active,
            Some(Permissions {
                global: GlobalPermissions {
                    manage_servers: false,
                    read_servers: true,
                    manage_users: false,
                    read_users: true,
                    manage_streams: false,
                    read_streams: true,
                    manage_topics: false,
                    read_topics: true,
                    poll_messages: true,
                    send_messages: true,
                },
                streams: None,
            }),
        )
        .await
        .unwrap();

    // 7. Trying to login with the invalid username should fail
    let login_user = client.login_user("user2", test_password).await;

    assert!(login_user.is_err());

    // 8. Trying to login with the invalid password should fail
    let login_user = client.login_user(test_user, "secret123").await;

    assert!(login_user.is_err());

    // 9. Login as the new user
    let identity_info = client.login_user(test_user, test_password).await.unwrap();

    assert_eq!(identity_info.user_id, 1);

    // 10. Trying to create a new user (or any other resource) without the appropriate permissions should fail
    let create_user_result = client
        .create_user("user2", "secret", UserStatus::Active, None)
        .await;

    assert!(create_user_result.is_err());

    // 11. Updating your own password should be allowed
    let updated_test_password = "topsecret";

    client
        .change_password(
            &Identifier::named(test_user).unwrap(),
            test_password,
            updated_test_password,
        )
        .await
        .unwrap();

    // 12. Trying to change the password given the invalid current password should fail
    let change_password = client
        .change_password(
            &Identifier::named(test_user).unwrap(),
            test_password,
            updated_test_password,
        )
        .await;

    assert!(change_password.is_err());

    let pat_name1 = "test_token_1";
    let pat_name2 = "test_token_2";

    // 13. Create the personal access tokens
    let raw_pat1 = client
        .create_personal_access_token(
            pat_name1,
            PersonalAccessTokenExpiry::ExpireDuration((SEC_IN_MICRO * 3600).into()),
        )
        .await
        .unwrap();

    assert!(!raw_pat1.token.is_empty());

    let raw_pat2 = client
        .create_personal_access_token(pat_name2, PersonalAccessTokenExpiry::NeverExpire)
        .await
        .unwrap();

    assert!(!raw_pat2.token.is_empty());

    // 14. Get personal access tokens and verify that the token is there
    let personal_access_tokens = client.get_personal_access_tokens().await.unwrap();
    assert_eq!(personal_access_tokens.len(), 2);

    // 15. Logout
    client.logout_user().await.unwrap();

    // 16. Login with the personal access tokens
    let identity_info = client
        .login_with_personal_access_token(&raw_pat1.token)
        .await
        .unwrap();

    assert_eq!(identity_info.user_id, 1);

    let identity_info = client
        .login_with_personal_access_token(&raw_pat2.token)
        .await
        .unwrap();

    assert_eq!(identity_info.user_id, 1);

    // 17. Delete the personal access tokens
    client
        .delete_personal_access_token(pat_name1)
        .await
        .unwrap();

    client
        .delete_personal_access_token(pat_name2)
        .await
        .unwrap();

    // 18. Get personal access tokens and verify that the token is no longer available
    let personal_access_tokens = client.get_personal_access_tokens().await.unwrap();
    assert!(personal_access_tokens.is_empty());

    // 19. Login as root user again
    login_root(&client).await;

    // 20. Trying to create a new user with the same username should fail
    let create_duplicated_user = client
        .create_user(test_user, test_password, UserStatus::Active, None)
        .await;

    assert!(create_duplicated_user.is_err());

    // 21. Update user details
    let updated_test_user = "user2";

    client
        .update_user(
            &Identifier::named(test_user).unwrap(),
            Some(updated_test_user),
            Some(UserStatus::Inactive),
        )
        .await
        .unwrap();

    // 22. Update user permissions
    client
        .update_permissions(
            &Identifier::named(updated_test_user).unwrap(),
            Some(Permissions {
                global: GlobalPermissions {
                    manage_servers: false,
                    read_servers: true,
                    manage_users: false,
                    read_users: true,
                    manage_streams: false,
                    read_streams: true,
                    manage_topics: false,
                    read_topics: true,
                    poll_messages: true,
                    send_messages: true,
                },
                streams: None,
            }),
        )
        .await
        .unwrap();

    // 23. Deleting another user should be allowed
    client
        .delete_user(&Identifier::named(updated_test_user).unwrap())
        .await
        .unwrap();

    // 24. Trying to delete the root user should fail
    let delete_root_user = client
        .delete_user(&Identifier::named(DEFAULT_ROOT_USERNAME).unwrap())
        .await;

    assert!(delete_root_user.is_err());

    assert_clean_system(&client).await;

    // 25. Logout
    client.logout_user().await.unwrap();

    // 26. Trying to perform any secured operation after logout should fail
    let get_users = client.get_users().await;
    assert!(get_users.is_err());
}
