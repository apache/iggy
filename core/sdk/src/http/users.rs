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

use crate::http::http_client::HttpClient;
use crate::http::http_transport::HttpTransport;
use crate::http::path::encode_segment;
use crate::prelude::{IdKind, Identifier, IggyError};
use async_trait::async_trait;
use iggy_common::change_password::ChangePassword;
use iggy_common::create_user::CreateUser;
use iggy_common::login_user::LoginUser;
use iggy_common::update_permissions::UpdatePermissions;
use iggy_common::{
    IdentityInfo, OptionInputMap, Permissions, UserClient, UserCreateOptions, UserInfo,
    UserInfoDetails, UserStatus, UserUpdateOptions,
};
use secrecy::SecretString;

const PATH: &str = "/users";

#[async_trait]
impl UserClient for HttpClient {
    async fn get_user(&self, user_id: &Identifier) -> Result<Option<UserInfoDetails>, IggyError> {
        let response = self.get(&user_path(user_id)?).await;
        if let Err(error) = response {
            if matches!(error, IggyError::ResourceNotFound(_)) {
                return Ok(None);
            }

            return Err(error);
        }

        let user = response?
            .json()
            .await
            .map_err(|_| IggyError::InvalidJsonResponse)?;
        Ok(Some(user))
    }

    async fn get_users(&self) -> Result<Vec<UserInfo>, IggyError> {
        let response = self.get(PATH).await?;
        let users = response
            .json()
            .await
            .map_err(|_| IggyError::InvalidJsonResponse)?;
        Ok(users)
    }

    async fn create_user(
        &self,
        username: &str,
        password: &str,
        status: UserStatus,
        permissions: Option<Permissions>,
        options: &UserCreateOptions,
    ) -> Result<UserInfoDetails, IggyError> {
        let response = self
            .post(
                PATH,
                &CreateUser {
                    username: username.to_string(),
                    password: SecretString::from(password),
                    status,
                    permissions,
                    options: OptionInputMap(options.to_input_map()?),
                },
            )
            .await?;
        let user = response
            .json()
            .await
            .map_err(|_| IggyError::InvalidJsonResponse)?;
        Ok(user)
    }

    async fn delete_user(&self, user_id: &Identifier) -> Result<(), IggyError> {
        self.delete(&user_path(user_id)?).await?;
        Ok(())
    }

    async fn update_user(
        &self,
        user_id: &Identifier,
        options: &UserUpdateOptions,
    ) -> Result<(), IggyError> {
        self.put(&user_path(user_id)?, &options.to_update_body()?)
            .await?;
        Ok(())
    }

    async fn update_permissions(
        &self,
        user_id: &Identifier,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        self.put(
            &format!("{}/permissions", user_path(user_id)?),
            &UpdatePermissions {
                user_id: user_id.clone(),
                permissions,
            },
        )
        .await?;
        Ok(())
    }

    async fn change_password(
        &self,
        user_id: &Identifier,
        current_password: &str,
        new_password: &str,
    ) -> Result<(), IggyError> {
        self.put(
            &format!("{}/password", user_path(user_id)?),
            &ChangePassword {
                user_id: user_id.clone(),
                current_password: SecretString::from(current_password),
                new_password: SecretString::from(new_password),
            },
        )
        .await?;
        Ok(())
    }

    async fn login_user(&self, username: &str, password: &str) -> Result<IdentityInfo, IggyError> {
        let response = self
            .post(
                &format!("{PATH}/login"),
                &LoginUser {
                    username: username.to_string(),
                    password: SecretString::from(password),
                    version: Some(env!("CARGO_PKG_VERSION").to_string()),
                    context: Some("".to_string()),
                },
            )
            .await?;
        let identity_info = response
            .json()
            .await
            .map_err(|_| IggyError::InvalidJsonResponse)?;
        self.set_token_from_identity(&identity_info).await?;
        Ok(identity_info)
    }

    async fn logout_user(&self) -> Result<(), IggyError> {
        self.delete(&format!("{PATH}/logout")).await?;
        self.set_access_token(None).await;
        Ok(())
    }
}

fn user_path(user_id: &Identifier) -> Result<String, IggyError> {
    let identifier = match user_id.kind {
        IdKind::Numeric => user_id.get_u32_value()?.to_string(),
        IdKind::String => user_id.get_cow_str_value()?.into_owned(),
    };
    // `Url::join` collapses "." and ".." even when percent-encoded, and only
    // a segment of one or two dots matches. Encoding covers every other value,
    // so the result is one segment with no query or fragment. Usernames are at
    // least three bytes, so this guard cannot reject a valid username.
    if matches!(identifier.as_str(), "." | "..") {
        return Err(IggyError::InvalidUsername);
    }

    Ok(format!("{PATH}/{}", encode_segment(&identifier)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::scripted_server::{Reply, ScriptedServer};

    #[test]
    fn given_numeric_id_when_building_path_should_keep_the_number() {
        let user_id = Identifier::numeric(42).unwrap();

        assert_eq!(user_path(&user_id).unwrap(), "/users/42");
    }

    #[test]
    fn given_reserved_characters_when_building_path_should_percent_encode() {
        let user_id = Identifier::named("a/b?c").unwrap();

        assert_eq!(user_path(&user_id).unwrap(), "/users/a%2Fb%3Fc");

        let user_id = Identifier::named("a%2Fb").unwrap();

        assert_eq!(user_path(&user_id).unwrap(), "/users/a%252Fb");
    }

    #[test]
    fn given_malformed_numeric_id_when_building_path_should_return_an_error() {
        let user_id = Identifier {
            kind: IdKind::Numeric,
            length: 0,
            value: 42u32.to_le_bytes().to_vec(),
        };

        assert!(matches!(
            user_path(&user_id),
            Err(IggyError::InvalidIdentifier)
        ));
    }

    #[test]
    fn given_dot_segments_when_building_path_should_reject_them() {
        for name in [".", ".."] {
            let user_id = Identifier::named(name).unwrap();

            assert!(matches!(
                user_path(&user_id),
                Err(IggyError::InvalidUsername)
            ));
        }
    }

    #[tokio::test]
    async fn given_reserved_characters_in_name_when_deleting_should_send_one_path_segment() {
        let server = ScriptedServer::start(vec![Reply::empty(200)]).await;
        let user_id = Identifier::named("a/b?c").unwrap();

        server.client().delete_user(&user_id).await.unwrap();

        let requests = server.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].line, "DELETE /users/a%2Fb%3Fc HTTP/1.1");
    }

    #[tokio::test]
    async fn given_dot_segment_name_when_deleting_should_fail_before_sending() {
        let server = ScriptedServer::start(vec![Reply::empty(200)]).await;
        let user_id = Identifier::named("..").unwrap();

        let deleted = server.client().delete_user(&user_id).await;

        assert!(matches!(deleted, Err(IggyError::InvalidUsername)));
        assert!(server.requests().is_empty());
    }
}
