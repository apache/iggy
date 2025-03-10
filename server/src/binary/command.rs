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

use crate::binary::handlers::consumer_groups::{
    create_consumer_group_handler, delete_consumer_group_handler, get_consumer_group_handler,
    get_consumer_groups_handler, join_consumer_group_handler, leave_consumer_group_handler,
};
use crate::binary::handlers::consumer_offsets::*;
use crate::binary::handlers::messages::*;
use crate::binary::handlers::partitions::*;
use crate::binary::handlers::personal_access_tokens::{
    create_personal_access_token_handler, delete_personal_access_token_handler,
    get_personal_access_tokens_handler, login_with_personal_access_token_handler,
};
use crate::binary::handlers::streams::*;
use crate::binary::handlers::system::*;
use crate::binary::handlers::topics::*;
use crate::binary::handlers::users::{
    change_password_handler, create_user_handler, delete_user_handler, get_user_handler,
    get_users_handler, login_user_handler, logout_user_handler, update_permissions_handler,
    update_user_handler,
};
use crate::binary::sender::SenderKind;
use crate::binary::COMPONENT;
use crate::command::ServerCommand;
use crate::streaming::session::Session;
use crate::streaming::systems::system::SharedSystem;
use error_set::ErrContext;
use iggy::error::IggyError;
use tracing::{debug, error};

pub async fn handle(
    command: ServerCommand,
    sender: &mut SenderKind,
    session: &Session,
    system: SharedSystem,
) -> Result<(), IggyError> {
    match try_handle(command, sender, session, &system).await {
        Ok(_) => {
            debug!("Command was handled successfully, session: {session}. TCP response was sent.");
            Ok(())
        }
        Err(error) => {
            error!("Command was not handled successfully, session: {session}, error: {error}.");
            if let IggyError::ClientNotFound(_) = error {
                sender
                    .send_error_response(error)
                    .await
                    .with_error_context(|error| {
                        format!("{COMPONENT} (error: {error}) - failed to send error response, session: {session}")
                    })?;
                debug!("TCP error response was sent to: {session}.");
                error!("Session: {session} will be deleted.");
                Err(IggyError::ClientNotFound(session.client_id))
            } else {
                sender
                    .send_error_response(error)
                    .await
                    .with_error_context(|error| {
                        format!("{COMPONENT} (error: {error}) - failed to send error response, session: {session}")
                    })?;
                debug!("TCP error response was sent to: {session}.");
                Ok(())
            }
        }
    }
}

async fn try_handle(
    command: ServerCommand,
    sender: &mut SenderKind,
    session: &Session,
    system: &SharedSystem,
) -> Result<(), IggyError> {
    debug!("Handling command '{command}', session: {session}...");
    match command {
        ServerCommand::Ping(command) => {
            ping_handler::handle(command, sender, session, system).await
        }
        ServerCommand::GetStats(command) => {
            get_stats_handler::handle(command, sender, session, system).await
        }
        ServerCommand::GetMe(command) => {
            get_me_handler::handle(command, sender, session, system).await
        }
        ServerCommand::GetClient(command) => {
            get_client_handler::handle(command, sender, session, system).await
        }
        ServerCommand::GetClients(command) => {
            get_clients_handler::handle(command, sender, session, system).await
        }
        ServerCommand::GetUser(command) => {
            get_user_handler::handle(command, sender, session, system).await
        }
        ServerCommand::GetUsers(command) => {
            get_users_handler::handle(command, sender, session, system).await
        }
        ServerCommand::CreateUser(command) => {
            create_user_handler::handle(command, sender, session, system).await
        }
        ServerCommand::DeleteUser(command) => {
            delete_user_handler::handle(command, sender, session, system).await
        }
        ServerCommand::UpdateUser(command) => {
            update_user_handler::handle(command, sender, session, system).await
        }
        ServerCommand::UpdatePermissions(command) => {
            update_permissions_handler::handle(command, sender, session, system).await
        }
        ServerCommand::ChangePassword(command) => {
            change_password_handler::handle(command, sender, session, system).await
        }
        ServerCommand::LoginUser(command) => {
            login_user_handler::handle(command, sender, session, system).await
        }
        ServerCommand::LogoutUser(command) => {
            logout_user_handler::handle(command, sender, session, system).await
        }
        ServerCommand::GetPersonalAccessTokens(command) => {
            get_personal_access_tokens_handler::handle(command, sender, session, system).await
        }
        ServerCommand::CreatePersonalAccessToken(command) => {
            create_personal_access_token_handler::handle(command, sender, session, system).await
        }
        ServerCommand::DeletePersonalAccessToken(command) => {
            delete_personal_access_token_handler::handle(command, sender, session, system).await
        }
        ServerCommand::LoginWithPersonalAccessToken(command) => {
            login_with_personal_access_token_handler::handle(command, sender, session, system).await
        }
        ServerCommand::SendMessages(command) => {
            send_messages_handler::handle(command, sender, session, system).await
        }
        ServerCommand::PollMessages(command) => {
            poll_messages_handler::handle(command, sender, session, system).await
        }
        ServerCommand::GetConsumerOffset(command) => {
            get_consumer_offset_handler::handle(command, sender, session, system).await
        }
        ServerCommand::StoreConsumerOffset(command) => {
            store_consumer_offset_handler::handle(command, sender, session, system).await
        }
        ServerCommand::DeleteConsumerOffset(command) => {
            delete_consumer_offset_handler::handle(command, sender, session, system).await
        }
        ServerCommand::GetStream(command) => {
            get_stream_handler::handle(command, sender, session, system).await
        }
        ServerCommand::GetStreams(command) => {
            get_streams_handler::handle(command, sender, session, system).await
        }
        ServerCommand::CreateStream(command) => {
            create_stream_handler::handle(command, sender, session, system).await
        }
        ServerCommand::DeleteStream(command) => {
            delete_stream_handler::handle(command, sender, session, system).await
        }
        ServerCommand::UpdateStream(command) => {
            update_stream_handler::handle(command, sender, session, system).await
        }
        ServerCommand::PurgeStream(command) => {
            purge_stream_handler::handle(command, sender, session, system).await
        }
        ServerCommand::GetTopic(command) => {
            get_topic_handler::handle(command, sender, session, system).await
        }
        ServerCommand::GetTopics(command) => {
            get_topics_handler::handle(command, sender, session, system).await
        }
        ServerCommand::CreateTopic(command) => {
            create_topic_handler::handle(command, sender, session, system).await
        }
        ServerCommand::DeleteTopic(command) => {
            delete_topic_handler::handle(command, sender, session, system).await
        }
        ServerCommand::UpdateTopic(command) => {
            update_topic_handler::handle(command, sender, session, system).await
        }
        ServerCommand::PurgeTopic(command) => {
            purge_topic_handler::handle(command, sender, session, system).await
        }
        ServerCommand::CreatePartitions(command) => {
            create_partitions_handler::handle(command, sender, session, system).await
        }
        ServerCommand::DeletePartitions(command) => {
            delete_partitions_handler::handle(command, sender, session, system).await
        }
        ServerCommand::GetConsumerGroup(command) => {
            get_consumer_group_handler::handle(command, sender, session, system).await
        }
        ServerCommand::GetConsumerGroups(command) => {
            get_consumer_groups_handler::handle(command, sender, session, system).await
        }
        ServerCommand::CreateConsumerGroup(command) => {
            create_consumer_group_handler::handle(command, sender, session, system).await
        }
        ServerCommand::DeleteConsumerGroup(command) => {
            delete_consumer_group_handler::handle(command, sender, session, system).await
        }
        ServerCommand::JoinConsumerGroup(command) => {
            join_consumer_group_handler::handle(command, sender, session, system).await
        }
        ServerCommand::LeaveConsumerGroup(command) => {
            leave_consumer_group_handler::handle(command, sender, session, system).await
        }
        ServerCommand::FlushUnsavedBuffer(command) => {
            flush_unsaved_buffer_handler::handle(command, sender, session, system).await
        }
        ServerCommand::GetSnapshotFile(command) => {
            get_snapshot::handle(command, sender, session, system).await
        }
    }
}
