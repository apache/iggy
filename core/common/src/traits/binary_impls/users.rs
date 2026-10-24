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

use crate::traits::binary_auth::fail_if_not_authenticated;
use crate::wire_conversions::{identifier_to_wire, permissions_to_wire, users_from_wire};
use crate::{
    BinaryClient, Credentials, Identifier, IdentityInfo, IggyError, Permissions, UserClient,
    UserCreateOptions, UserInfo, UserInfoDetails, UserStatus, UserUpdateOptions,
};
use iggy_binary_protocol::WireName;
use iggy_binary_protocol::codec::WireEncode;
use iggy_binary_protocol::codes::{
    CHANGE_PASSWORD_CODE, CREATE_USER_CODE, DELETE_USER_CODE, GET_USER_CODE, GET_USERS_CODE,
    LOGOUT_USER_CODE, UPDATE_PERMISSIONS_CODE,
};
use iggy_binary_protocol::requests::system::ResourceTarget;
use iggy_binary_protocol::requests::users::{
    ChangePasswordRequest, CreateUserRequest, DeleteUserRequest, GetUserRequest, GetUsersRequest,
    LogoutUserRequest, UpdatePermissionsRequest,
};
use iggy_binary_protocol::responses::users::{GetUsersResponse, UserDetailsResponse};
use secrecy::SecretString;

#[async_trait::async_trait]
impl<B: BinaryClient> UserClient for B {
    async fn get_user(&self, user_id: &Identifier) -> Result<Option<UserInfoDetails>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let wire_id = identifier_to_wire(user_id)?;
        let response = self
            .send_raw_with_response(
                GET_USER_CODE,
                GetUserRequest { user_id: wire_id }.to_bytes(),
            )
            .await?;
        if response.is_empty() {
            return Ok(None);
        }
        let wire_resp = super::decode_response::<UserDetailsResponse>(&response)?;
        UserInfoDetails::try_from(wire_resp).map(Some)
    }

    async fn get_users(&self) -> Result<Vec<UserInfo>, IggyError> {
        fail_if_not_authenticated(self).await?;
        let response = self
            .send_raw_with_response(GET_USERS_CODE, GetUsersRequest.to_bytes())
            .await?;
        if response.is_empty() {
            return Ok(Vec::new());
        }
        let wire_resp = super::decode_response::<GetUsersResponse>(&response)?;
        users_from_wire(wire_resp)
    }

    async fn create_user(
        &self,
        username: &str,
        password: &str,
        status: UserStatus,
        permissions: Option<Permissions>,
        options: &UserCreateOptions,
    ) -> Result<UserInfoDetails, IggyError> {
        fail_if_not_authenticated(self).await?;
        super::validate_username(username)?;
        super::validate_password(password)?;
        let wire_name = WireName::new(username).map_err(|_| IggyError::InvalidFormat)?;
        let wire_perms = permissions.as_ref().map(permissions_to_wire);
        let response = self
            .send_raw_with_response(
                CREATE_USER_CODE,
                CreateUserRequest {
                    username: wire_name,
                    password: password.to_string(),
                    status: status.as_code(),
                    permissions: wire_perms,
                    options: options.to_wire()?,
                }
                .to_bytes(),
            )
            .await?;
        let wire_resp = super::decode_response::<UserDetailsResponse>(&response)?;
        UserInfoDetails::try_from(wire_resp)
    }

    async fn delete_user(&self, user_id: &Identifier) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        let wire_id = identifier_to_wire(user_id)?;
        self.send_raw_with_response(
            DELETE_USER_CODE,
            DeleteUserRequest { user_id: wire_id }.to_bytes(),
        )
        .await?;
        Ok(())
    }

    async fn update_user(
        &self,
        user_id: &Identifier,
        options: &UserUpdateOptions,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        let patch = options.to_patch()?;
        let target = ResourceTarget::User {
            user_id: identifier_to_wire(user_id)?,
        };
        super::update_resource(self, target, patch).await
    }

    async fn update_permissions(
        &self,
        user_id: &Identifier,
        permissions: Option<Permissions>,
    ) -> Result<(), IggyError> {
        fail_if_not_authenticated(self).await?;
        let wire_id = identifier_to_wire(user_id)?;
        let wire_perms = permissions.as_ref().map(permissions_to_wire);
        self.send_raw_with_response(
            UPDATE_PERMISSIONS_CODE,
            UpdatePermissionsRequest {
                user_id: wire_id,
                permissions: wire_perms,
            }
            .to_bytes(),
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
        fail_if_not_authenticated(self).await?;
        super::validate_password(current_password)?;
        super::validate_password(new_password)?;
        let wire_id = identifier_to_wire(user_id)?;
        self.send_raw_with_response(
            CHANGE_PASSWORD_CODE,
            ChangePasswordRequest {
                user_id: wire_id,
                current_password: current_password.to_string(),
                new_password: new_password.to_string(),
            }
            .to_bytes(),
        )
        .await?;
        Ok(())
    }

    async fn login_user(&self, username: &str, password: &str) -> Result<IdentityInfo, IggyError> {
        crate::with_binary_deadline(crate::binary_operation_deadline(), async {
            super::validate_username(username)?;
            super::validate_password(password)?;
            let credentials = Credentials::UsernamePassword(
                username.to_owned(),
                SecretString::from(password.to_string()),
            );
            self.sign_in_with_credentials(credentials).await
        })
        .await
    }

    async fn logout_user(&self) -> Result<(), IggyError> {
        crate::with_binary_lineage(self.operation_lineage(), logout(self, false)).await
    }
}

pub(super) async fn logout<B: BinaryClient>(client: &B, signing_in: bool) -> Result<(), IggyError> {
    if !signing_in {
        fail_if_not_authenticated(client).await?;
    }
    client
        .send_raw_with_response(LOGOUT_USER_CODE, LogoutUserRequest.to_bytes())
        .await?;
    client.finish_sign_out(signing_in).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        BinaryTransport, Client, ClientState, ConsumerGroupClientState, DiagnosticEvent,
        NonZeroIggyDuration, OptionKey, PartitionContext, VsrSessionControl, VsrSessionSealed,
    };
    use async_broadcast::Receiver;
    use bytes::Bytes;
    use iggy_binary_protocol::requests::messages::PollMessagesRequest;
    use iggy_binary_protocol::requests::system::SessionIdentity;
    use iggy_binary_protocol::requests::users::login_register::BindSecret;
    use std::collections::BTreeSet;
    use std::sync::{Arc, Mutex};

    #[tokio::test]
    async fn given_short_username_and_status_reset_when_updating_user_should_report_admission_category()
     {
        let client = RecordingClient::default();
        let outcome = client
            .update_user(
                &Identifier::numeric(5).expect("numeric id"),
                &UserUpdateOptions {
                    username: Some("ab".to_owned()),
                    reset: BTreeSet::from([OptionKey::Status]),
                    ..UserUpdateOptions::default()
                },
            )
            .await;
        assert_eq!(
            outcome,
            Err(IggyError::OptionNotResettable(
                OptionKey::Status.to_string()
            ))
        );
        assert!(client.sent_codes().is_empty());
    }

    /// An authenticated binary client that records the codes it sends.
    #[derive(Debug, Default)]
    struct RecordingClient {
        sent: Mutex<Vec<u32>>,
        consumer_groups: Arc<ConsumerGroupClientState>,
    }

    impl RecordingClient {
        fn sent_codes(&self) -> Vec<u32> {
            self.sent.lock().expect("lock").clone()
        }

        fn record(&self, code: u32) -> Result<Bytes, IggyError> {
            self.sent.lock().expect("lock").push(code);
            Ok(Bytes::new())
        }
    }

    #[async_trait::async_trait]
    impl BinaryTransport for RecordingClient {
        async fn get_state(&self) -> ClientState {
            ClientState::Authenticated
        }

        async fn set_state(&self, _state: ClientState) {}

        async fn publish_event(&self, _event: DiagnosticEvent) {}

        async fn send_raw_with_context(
            &self,
            code: u32,
            _payload: Bytes,
            _context: PartitionContext,
        ) -> Result<Bytes, IggyError> {
            self.record(code)
        }

        async fn send_offset_write_with_response(
            &self,
            code: u32,
            _payload: Bytes,
            _context: Option<PartitionContext>,
        ) -> Result<Bytes, IggyError> {
            self.record(code)
        }

        async fn send_poll_with_response(
            &self,
            _request: &PollMessagesRequest,
            _context: Option<PartitionContext>,
        ) -> Result<Bytes, IggyError> {
            Err(IggyError::FeatureUnavailable)
        }

        fn get_heartbeat_interval(&self) -> NonZeroIggyDuration {
            NonZeroIggyDuration::ONE_SECOND
        }

        fn consumer_group_state(&self) -> Arc<ConsumerGroupClientState> {
            Arc::clone(&self.consumer_groups)
        }
    }

    #[async_trait::async_trait]
    impl Client for RecordingClient {
        async fn connect(&self) -> Result<(), IggyError> {
            Ok(())
        }

        async fn disconnect(&self) -> Result<(), IggyError> {
            Ok(())
        }

        async fn shutdown(&self) -> Result<(), IggyError> {
            Ok(())
        }

        async fn subscribe_events(&self) -> Receiver<DiagnosticEvent> {
            async_broadcast::broadcast(1).1
        }
    }

    impl VsrSessionSealed for RecordingClient {}

    #[async_trait::async_trait]
    impl VsrSessionControl for RecordingClient {
        async fn session_bind_secret(&self) -> Result<BindSecret, IggyError> {
            Err(IggyError::FeatureUnavailable)
        }

        async fn session_identity(&self) -> Result<SessionIdentity, IggyError> {
            Err(IggyError::FeatureUnavailable)
        }

        async fn bind_vsr_session(&self, _session: u64) -> Result<(), IggyError> {
            Ok(())
        }

        async fn reset_vsr_session(&self) -> Result<(), IggyError> {
            Ok(())
        }

        fn sdk_version(&self) -> &'static str {
            env!("CARGO_PKG_VERSION")
        }
    }

    impl BinaryClient for RecordingClient {}
}
