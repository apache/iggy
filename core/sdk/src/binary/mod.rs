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

pub use iggy_common::{BinaryClient, BinaryTransport};

macro_rules! impl_binary_client {
    ($client:ty) => {
        impl $client {
            pub(crate) fn retains_operation(&self, op: u64) -> bool {
                self.driver.session().retains_operation(op)
            }

            pub(crate) async fn keepalive(&self) -> Result<bool, iggy_common::IggyError> {
                self.driver.links().keepalive().await
            }

            pub(crate) fn connection_address(&self) -> String {
                self.driver.links().connection_address()
            }
        }

        #[async_trait::async_trait]
        impl iggy_common::Client for $client {
            async fn connect(&self) -> Result<(), iggy_common::IggyError> {
                self.driver.links().connect().await
            }

            async fn disconnect(&self) -> Result<(), iggy_common::IggyError> {
                self.driver.links().disconnect();
                self.driver.session().forget();
                self.driver.session().reset().map(|_| ())
            }

            async fn shutdown(&self) -> Result<(), iggy_common::IggyError> {
                self.driver.links().shutdown().await;
                self.driver.session().forget();
                self.driver.session().reset().map(|_| ())
            }

            async fn subscribe_events(
                &self,
            ) -> async_broadcast::Receiver<iggy_common::DiagnosticEvent> {
                self.driver.links().subscribe()
            }

            async fn resolve_operation(
                &self,
                op: u64,
                deadline: std::time::Instant,
            ) -> Result<(), iggy_common::IggyError> {
                self.driver.resolve(op, deadline.into()).await
            }

            async fn abandon_operation(&self, op: u64) -> Result<(), iggy_common::IggyError> {
                self.driver.abandon(op)
            }
        }

        #[async_trait::async_trait]
        impl iggy_common::BinaryTransport for $client {
            async fn get_state(&self) -> iggy_common::ClientState {
                self.driver.links().state()
            }

            async fn set_state(&self, state: iggy_common::ClientState) {
                self.driver.links().set_state(state);
            }

            async fn publish_event(&self, event: iggy_common::DiagnosticEvent) {
                self.driver.links().publish(event);
            }

            async fn send_raw_with_context(
                &self,
                code: u32,
                payload: bytes::Bytes,
                context: iggy_common::PartitionContext,
            ) -> Result<bytes::Bytes, iggy_common::IggyError> {
                self.driver
                    .send_raw(
                        code,
                        payload,
                        (context != iggy_common::PartitionContext::default()).then_some(context),
                    )
                    .await
            }

            async fn send_offset_write_with_response(
                &self,
                code: u32,
                payload: bytes::Bytes,
                context: Option<iggy_common::PartitionContext>,
            ) -> Result<bytes::Bytes, iggy_common::IggyError> {
                self.driver.send_raw(code, payload, context).await
            }

            async fn send_poll_with_response(
                &self,
                request: &iggy_binary_protocol::requests::messages::PollMessagesRequest,
                context: Option<iggy_common::PartitionContext>,
            ) -> Result<bytes::Bytes, iggy_common::IggyError> {
                self.driver.poll(request, context).await
            }

            fn routed_partition_count(
                &self,
                stream_id: &iggy_common::Identifier,
                topic_id: &iggy_common::Identifier,
            ) -> Option<u32> {
                self.driver.partition_count(stream_id, topic_id)
            }

            fn get_heartbeat_interval(&self) -> iggy_common::NonZeroIggyDuration {
                self.config.heartbeat_interval
            }

            fn consumer_group_state(
                &self,
            ) -> std::sync::Arc<iggy_common::ConsumerGroupClientState> {
                self.driver.session().consumer_group_state()
            }
        }

        impl iggy_common::VsrSessionSealed for $client {}

        #[async_trait::async_trait]
        impl iggy_common::VsrSessionControl for $client {
            async fn prepare_sign_in(&self) -> Result<(), iggy_common::IggyError> {
                self.driver.links().prepare_sign_in().await
            }

            async fn session_identity(
                &self,
            ) -> Result<
                iggy_binary_protocol::requests::system::SessionIdentity,
                iggy_common::IggyError,
            > {
                self.driver.session().identity()
            }

            async fn session_bind_secret(
                &self,
            ) -> Result<
                iggy_binary_protocol::requests::users::login_register::BindSecret,
                iggy_common::IggyError,
            > {
                self.driver.session().bind_secret()
            }

            async fn bind_vsr_session(&self, session: u64) -> Result<(), iggy_common::IggyError> {
                self.driver
                    .session()
                    .bind_at(self.driver.session().fence(), session)
                    .map(|_| ())
            }

            fn session_fence(&self) -> u64 {
                self.driver.session().fence()
            }

            async fn bind_vsr_session_at(
                &self,
                fence: u64,
                session: u64,
            ) -> Result<(), iggy_common::IggyError> {
                self.driver.session().bind_at(fence, session).map(|_| ())
            }

            async fn reset_vsr_session(&self) -> Result<(), iggy_common::IggyError> {
                self.driver.session().reset().map(|_| ())
            }

            async fn remember_session_credentials(
                &self,
                credentials: iggy_common::Credentials,
                user_id: u32,
            ) {
                self.driver.remember(credentials, user_id).await;
            }

            async fn forget_session_credentials(&self) {
                self.driver.session().forget();
            }

            fn sdk_version(&self) -> &'static str {
                crate::SDK_VERSION
            }
        }

        impl iggy_common::BinaryClient for $client {}
    };
}

pub(crate) use impl_binary_client;
