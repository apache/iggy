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

mod args;
mod credentials;
mod error;
mod logging;

use crate::args::{
    Command, IggyConsoleArgs, client::ClientAction, consumer_group::ConsumerGroupAction,
    consumer_offset::ConsumerOffsetAction, permissions::PermissionsArgs,
    personal_access_token::PersonalAccessTokenAction, stream::StreamAction, topic::TopicAction,
};
use crate::credentials::IggyCredentials;
use crate::error::{CmdToolError, IggyCmdError};
use crate::logging::Logging;
use args::context::ContextAction;
use args::message::MessageAction;
use args::partition::PartitionAction;
use args::segment::SegmentAction;
use args::user::UserAction;
use args::{CliOptions, IggyMergedConsoleArgs};
use clap::Parser;
use iggy::client_provider::{self, ClientProviderConfig};
use iggy::clients::client::IggyClient;
use iggy::prelude::{Aes256GcmEncryptor, Args, EncryptorKind, PersonalAccessTokenExpiry};
use iggy_binary_protocol::cli::binary_context::common::ContextManager;
use iggy_binary_protocol::cli::binary_context::use_context::UseContextCmd;
use iggy_binary_protocol::cli::binary_segments::delete_segments::DeleteSegmentsCmd;
use iggy_binary_protocol::cli::binary_system::snapshot::GetSnapshotCmd;
use iggy_binary_protocol::cli::cli_command::{CliCommand, PRINT_TARGET};
use iggy_binary_protocol::cli::{
    binary_client::{get_client::GetClientCmd, get_clients::GetClientsCmd},
    binary_consumer_groups::{
        create_consumer_group::CreateConsumerGroupCmd,
        delete_consumer_group::DeleteConsumerGroupCmd, get_consumer_group::GetConsumerGroupCmd,
        get_consumer_groups::GetConsumerGroupsCmd,
    },
    binary_consumer_offsets::{
        get_consumer_offset::GetConsumerOffsetCmd, set_consumer_offset::SetConsumerOffsetCmd,
    },
    binary_context::get_contexts::GetContextsCmd,
    binary_message::{
        flush_messages::FlushMessagesCmd, poll_messages::PollMessagesCmd,
        send_messages::SendMessagesCmd,
    },
    binary_partitions::{
        create_partitions::CreatePartitionsCmd, delete_partitions::DeletePartitionsCmd,
    },
    binary_personal_access_tokens::{
        create_personal_access_token::CreatePersonalAccessTokenCmd,
        delete_personal_access_tokens::DeletePersonalAccessTokenCmd,
        get_personal_access_tokens::GetPersonalAccessTokensCmd,
    },
    binary_streams::{
        create_stream::CreateStreamCmd, delete_stream::DeleteStreamCmd, get_stream::GetStreamCmd,
        get_streams::GetStreamsCmd, purge_stream::PurgeStreamCmd, update_stream::UpdateStreamCmd,
    },
    binary_system::{me::GetMeCmd, ping::PingCmd, stats::GetStatsCmd},
    binary_topics::{
        create_topic::CreateTopicCmd, delete_topic::DeleteTopicCmd, get_topic::GetTopicCmd,
        get_topics::GetTopicsCmd, purge_topic::PurgeTopicCmd, update_topic::UpdateTopicCmd,
    },
    binary_users::{
        change_password::ChangePasswordCmd,
        create_user::CreateUserCmd,
        delete_user::DeleteUserCmd,
        get_user::GetUserCmd,
        get_users::GetUsersCmd,
        update_permissions::UpdatePermissionsCmd,
        update_user::{UpdateUserCmd, UpdateUserType},
    },
};
use std::sync::Arc;
use tracing::{Level, event};

#[cfg(feature = "login-session")]
mod main_login_session {
    pub(crate) use iggy_binary_protocol::cli::binary_system::{login::LoginCmd, logout::LogoutCmd};
    pub(crate) use iggy_binary_protocol::cli::utils::login_session_expiry::LoginSessionExpiry;
}

#[cfg(feature = "login-session")]
use main_login_session::*;

fn get_command(
    command: Command,
    cli_options: &CliOptions,
    iggy_args: &Args,
) -> Box<dyn CliCommand> {
    #[warn(clippy::let_and_return)]
    match command {
        Command::Stream(command) => match command {
            StreamAction::Create(args) => Box::new(CreateStreamCmd::new(args.name.clone())),
            StreamAction::Delete(args) => Box::new(DeleteStreamCmd::new(args.stream_id.clone())),
            StreamAction::Update(args) => Box::new(UpdateStreamCmd::new(
                args.stream_id.clone(),
                args.name.clone(),
            )),
            StreamAction::Get(args) => Box::new(GetStreamCmd::new(args.stream_id.clone())),
            StreamAction::List(args) => Box::new(GetStreamsCmd::new(args.list_mode.into())),
            StreamAction::Purge(args) => Box::new(PurgeStreamCmd::new(args.stream_id.clone())),
        },
        Command::Topic(command) => match command {
            TopicAction::Create(args) => Box::new(CreateTopicCmd::new(
                args.stream_id.clone(),
                args.partitions_count,
                args.compression_algorithm,
                args.name.clone(),
                args.message_expiry.clone().into(),
                args.max_topic_size,
                args.replication_factor,
            )),
            TopicAction::Delete(args) => Box::new(DeleteTopicCmd::new(
                args.stream_id.clone(),
                args.topic_id.clone(),
            )),
            TopicAction::Update(args) => Box::new(UpdateTopicCmd::new(
                args.stream_id.clone(),
                args.topic_id.clone(),
                args.compression_algorithm,
                args.name.clone(),
                args.message_expiry.clone().into(),
                args.max_topic_size,
                args.replication_factor,
            )),
            TopicAction::Get(args) => Box::new(GetTopicCmd::new(
                args.stream_id.clone(),
                args.topic_id.clone(),
            )),
            TopicAction::List(args) => Box::new(GetTopicsCmd::new(
                args.stream_id.clone(),
                args.list_mode.into(),
            )),
            TopicAction::Purge(args) => Box::new(PurgeTopicCmd::new(
                args.stream_id.clone(),
                args.topic_id.clone(),
            )),
        },
        Command::Partition(command) => match command {
            PartitionAction::Create(args) => Box::new(CreatePartitionsCmd::new(
                args.stream_id.clone(),
                args.topic_id.clone(),
                args.partitions_count,
            )),
            PartitionAction::Delete(args) => Box::new(DeletePartitionsCmd::new(
                args.stream_id.clone(),
                args.topic_id.clone(),
                args.partitions_count,
            )),
        },
        Command::Segment(command) => match command {
            SegmentAction::Delete(args) => Box::new(DeleteSegmentsCmd::new(
                args.stream_id.clone(),
                args.topic_id.clone(),
                args.partition_id,
                args.segments_count,
            )),
        },
        Command::Ping(args) => Box::new(PingCmd::new(args.count)),
        Command::Me => Box::new(GetMeCmd::new()),
        Command::Stats(args) => Box::new(GetStatsCmd::new(cli_options.quiet, args.output.into())),
        Command::Snapshot(args) => Box::new(GetSnapshotCmd::new(
            args.compression,
            args.snapshot_types,
            args.out_dir,
        )),
        Command::Pat(command) => match command {
            PersonalAccessTokenAction::Create(pat_create_args) => {
                Box::new(CreatePersonalAccessTokenCmd::new(
                    pat_create_args.name.clone(),
                    PersonalAccessTokenExpiry::new(pat_create_args.expiry.clone()),
                    cli_options.quiet,
                    pat_create_args.store_token,
                    iggy_args.get_server_address().unwrap(),
                ))
            }
            PersonalAccessTokenAction::Delete(pat_delete_args) => {
                Box::new(DeletePersonalAccessTokenCmd::new(
                    pat_delete_args.name.clone(),
                    iggy_args.get_server_address().unwrap(),
                ))
            }
            PersonalAccessTokenAction::List(pat_list_args) => Box::new(
                GetPersonalAccessTokensCmd::new(pat_list_args.list_mode.into()),
            ),
        },
        Command::User(command) => match command {
            UserAction::Create(create_args) => Box::new(CreateUserCmd::new(
                create_args.username.clone(),
                create_args.password.clone(),
                create_args.user_status.clone().into(),
                PermissionsArgs::new(
                    create_args.global_permissions.clone(),
                    create_args.stream_permissions.clone(),
                )
                .into(),
            )),
            UserAction::Delete(delete_args) => {
                Box::new(DeleteUserCmd::new(delete_args.user_id.clone()))
            }
            UserAction::Get(get_args) => Box::new(GetUserCmd::new(get_args.user_id.clone())),
            UserAction::List(list_args) => Box::new(GetUsersCmd::new(list_args.list_mode.into())),
            UserAction::Name(name_args) => Box::new(UpdateUserCmd::new(
                name_args.user_id.clone(),
                UpdateUserType::Name(name_args.username.clone()),
            )),
            UserAction::Status(status_args) => Box::new(UpdateUserCmd::new(
                status_args.user_id.clone(),
                UpdateUserType::Status(status_args.status.clone().into()),
            )),
            UserAction::Password(change_pwd_args) => Box::new(ChangePasswordCmd::new(
                change_pwd_args.user_id,
                change_pwd_args.current_password,
                change_pwd_args.new_password,
            )),
            UserAction::Permissions(permissions_args) => Box::new(UpdatePermissionsCmd::new(
                permissions_args.user_id.clone(),
                PermissionsArgs::new(
                    permissions_args.global_permissions.clone(),
                    permissions_args.stream_permissions.clone(),
                )
                .into(),
            )),
        },
        Command::Client(command) => match command {
            ClientAction::Get(get_args) => Box::new(GetClientCmd::new(get_args.client_id)),
            ClientAction::List(list_args) => {
                Box::new(GetClientsCmd::new(list_args.list_mode.into()))
            }
        },
        Command::ConsumerGroup(command) => match command {
            ConsumerGroupAction::Create(create_args) => Box::new(CreateConsumerGroupCmd::new(
                create_args.stream_id.clone(),
                create_args.topic_id.clone(),
                create_args.name.clone(),
            )),
            ConsumerGroupAction::Delete(delete_args) => Box::new(DeleteConsumerGroupCmd::new(
                delete_args.stream_id.clone(),
                delete_args.topic_id.clone(),
                delete_args.group_id.clone(),
            )),
            ConsumerGroupAction::Get(get_args) => Box::new(GetConsumerGroupCmd::new(
                get_args.stream_id.clone(),
                get_args.topic_id.clone(),
                get_args.group_id.clone(),
            )),
            ConsumerGroupAction::List(list_args) => Box::new(GetConsumerGroupsCmd::new(
                list_args.stream_id.clone(),
                list_args.topic_id.clone(),
                list_args.list_mode.into(),
            )),
        },
        Command::Message(command) => match command {
            MessageAction::Send(send_args) => Box::new(SendMessagesCmd::new(
                send_args.stream_id.clone(),
                send_args.topic_id.clone(),
                send_args.partition_id,
                send_args.message_key.clone(),
                send_args.messages.clone(),
                send_args.headers.clone(),
                send_args.input_file.clone(),
            )),
            MessageAction::Poll(poll_args) => Box::new(PollMessagesCmd::new(
                poll_args.stream_id.clone(),
                poll_args.topic_id.clone(),
                poll_args.partition_id,
                poll_args.message_count,
                poll_args.auto_commit,
                poll_args.offset,
                poll_args.first,
                poll_args.last,
                poll_args.next,
                poll_args.consumer.clone(),
                poll_args.show_headers,
                poll_args.output_file.clone(),
            )),
            MessageAction::Flush(flush_args) => Box::new(FlushMessagesCmd::new(
                flush_args.stream_id.clone(),
                flush_args.topic_id.clone(),
                flush_args.partition_id,
                flush_args.fsync,
            )),
        },
        Command::ConsumerOffset(command) => match command {
            ConsumerOffsetAction::Get(get_args) => Box::new(GetConsumerOffsetCmd::new(
                get_args.consumer_id.clone(),
                get_args.stream_id.clone(),
                get_args.topic_id.clone(),
                get_args.partition_id,
                get_args.kind,
            )),
            ConsumerOffsetAction::Set(set_args) => Box::new(SetConsumerOffsetCmd::new(
                set_args.consumer_id.clone(),
                set_args.stream_id.clone(),
                set_args.topic_id.clone(),
                set_args.partition_id,
                set_args.offset,
                set_args.kind,
            )),
        },
        Command::Context(command) => match command {
            ContextAction::List(list_args) => {
                Box::new(GetContextsCmd::new(list_args.list_mode.into()))
            }
            ContextAction::Use(use_args) => {
                Box::new(UseContextCmd::new(use_args.context_name.clone()))
            }
        },
        #[cfg(feature = "login-session")]
        Command::Login(login_args) => Box::new(LoginCmd::new(
            iggy_args.get_server_address().unwrap(),
            LoginSessionExpiry::new(login_args.expiry.clone()),
        )),
        #[cfg(feature = "login-session")]
        Command::Logout => Box::new(LogoutCmd::new(iggy_args.get_server_address().unwrap())),
    }
}

#[tokio::main]
async fn main() -> Result<(), IggyCmdError> {
    let args = IggyConsoleArgs::parse();

    if let Some(generator) = args.cli.generator {
        args.generate_completion(generator);
        return Ok(());
    }

    if args.command.is_none() {
        IggyConsoleArgs::print_overview();
        return Ok(());
    }

    let mut logging = Logging::new();
    logging.init(args.cli.quiet, &args.cli.debug);

    let command = args.command.clone().unwrap();

    let mut context_manager = ContextManager::default();
    let active_context = context_manager.get_active_context().await?;
    let merged_args = IggyMergedConsoleArgs::from_context(active_context, args);

    let iggy_args = merged_args.iggy;
    let cli_options = merged_args.cli;

    // Get command based on command line arguments
    let mut command = get_command(command, &cli_options, &iggy_args);

    // Create credentials based on command line arguments and command
    let mut credentials = IggyCredentials::new(&cli_options, &iggy_args, command.login_required())?;

    let encryptor = match iggy_args.encryption_key.is_empty() {
        true => None,
        false => {
            let encryption_key = Aes256GcmEncryptor::from_base64_key(&iggy_args.encryption_key)
                .map_err(|_| {
                    <IggyCmdError as Into<anyhow::Error>>::into(IggyCmdError::CmdToolError(
                        CmdToolError::InvalidEncryptionKey,
                    ))
                })?;
            Some(Arc::new(EncryptorKind::Aes256Gcm(encryption_key)))
        }
    };
    let client_provider_config = Arc::new(ClientProviderConfig::from_args_set_autologin(
        iggy_args.clone(),
        false,
    )?);

    let client =
        client_provider::get_raw_client(client_provider_config, command.connection_required())
            .await?;
    let client = IggyClient::create(client, None, encryptor);

    credentials.set_iggy_client(&client);
    credentials.login_user().await?;

    if command.use_tracing() {
        event!(target: PRINT_TARGET, Level::INFO, "Executing {}", command.explain());
    } else {
        println!("Executing {}", command.explain());
    }
    command.execute_cmd(&client).await?;

    credentials.logout_user().await?;

    Ok(())
}
