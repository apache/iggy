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

use crate::args::common::ListMode;
use crate::args::permissions::UserStatusArg;
use crate::args::permissions::stream::StreamPermissionsArg;
use clap::{Args, Subcommand};
use iggy::prelude::Identifier;

use super::permissions::global::GlobalPermissionsArg;

#[derive(Debug, Clone, Subcommand)]
pub(crate) enum UserAction {
    /// Create user with given username and password
    ///
    /// Examples
    ///  iggy user create testuser pass#1%X!
    ///  iggy user create guest guess --user-status inactive
    #[clap(verbatim_doc_comment, visible_alias = "c")]
    Create(UserCreateArgs),
    /// Delete user with given ID
    ///
    /// The user ID can be specified as either a username or an ID
    ///
    /// Examples:
    ///  iggy user delete 2
    ///  iggy user delete testuser
    #[clap(verbatim_doc_comment, visible_alias = "d")]
    Delete(UserDeleteArgs),
    /// Get details of a single user with given ID
    ///
    /// The user ID can be specified as either a username or an ID
    ///
    /// Examples:
    ///  iggy user get 2
    ///  iggy user get testuser
    #[clap(verbatim_doc_comment, visible_alias = "g")]
    Get(UserGetArgs),
    /// List all users
    ///
    /// Examples:
    ///  iggy user list
    ///  iggy user list --list-mode table
    ///  iggy user list -l table
    #[clap(verbatim_doc_comment, visible_alias = "l")]
    List(UserListArgs),
    /// Change username for user with given ID
    ///
    /// The user ID can be specified as either a username or an ID
    ///
    /// Examples:
    ///  iggy user name 2 new_user_name
    ///  iggy user name testuser test_user
    #[clap(verbatim_doc_comment, visible_alias = "n")]
    Name(UserNameArgs),
    /// Change status for user with given ID
    ///
    /// The user ID can be specified as either a username or an ID
    ///
    /// Examples:
    ///  iggy user status 2 active
    ///  iggy user status testuser inactive
    #[clap(verbatim_doc_comment, visible_alias = "s")]
    Status(UserStatusArgs),
    /// Change password for user with given ID
    ///
    /// The user ID can be specified as either a username or an ID
    ///
    /// Examples:
    ///  iggy user password 2
    ///  iggy user password client
    ///  iggy user password 3 current_password new_password
    ///  iggy user password testuser curpwd p@sswor4
    #[clap(verbatim_doc_comment, visible_alias = "pwd")]
    Password(UserPasswordArgs),
    /// Set permissions for user with given ID
    ///
    /// The user ID can be specified as either a username or an ID. Permissions
    /// are configured based on the options provided with this command. If no
    /// options are set, the default behavior is to remove permissions for the
    /// specified user.
    ///
    /// Examples:
    ///  iggy user permissions 2
    ///  iggy user permissions client
    #[clap(verbatim_doc_comment, visible_alias = "p")]
    Permissions(UserPermissionsArgs),
}

#[derive(Debug, Clone, Args)]
pub(crate) struct UserCreateArgs {
    /// Username
    ///
    /// Unique identifier for the user account on iggy server,
    /// must be between 3 and 50 characters long.
    #[clap(verbatim_doc_comment)]
    pub(crate) username: String,
    /// Password
    ///
    /// Password of the user, must be between 3 and 100 characters long.
    #[clap(verbatim_doc_comment)]
    pub(crate) password: String,
    /// User status
    #[clap(short, long)]
    #[arg(value_enum, default_value_t = UserStatusArg::default())]
    pub(crate) user_status: UserStatusArg,
    /// Set global permissions for created user
    ///
    /// All global permissions by default are set to false and this command line option
    /// allows to set each permission individually. Permissions are separated
    /// by comma and each permission is identified by the same name as in the iggy
    /// SDK in iggy::models::permissions::GlobalPermissions struct. For each permission
    /// there's long variant (same as in SDK) and short variant.
    ///
    /// Available permissions (long and short versions):  manage_servers / m_srv,
    /// read_servers / r_srv, manage_users / m_usr, read_users / r_usr,
    /// manage_streams / m_str, read_streams / r_str, manage_topics / m_top,
    /// read_topics / r_top, poll_messages / p_msg, send_messages / s_msg
    ///
    /// Examples:
    ///  iggy user create guest guess --global-permissions p_msg,s_msg
    ///  iggy user create admin pass#1%X! -g m_srv,r_srv,m_usr,r_usr,m_str,r_str,m_top,r_top,p_msg,s_msg
    #[clap(short, long, verbatim_doc_comment)]
    #[arg(value_parser = clap::value_parser!(GlobalPermissionsArg))]
    pub(crate) global_permissions: Option<GlobalPermissionsArg>,
    /// Set stream permissions for created user
    ///
    /// Stream permissions are defined by each stream separately. Setting permission for stream
    /// allows to set each permission individually, by default, if no permission is provided
    /// (only stream ID is provided) all are set to false. Stream permission format consists
    /// of stream ID followed by colon (:) and list of permissions separated by comma (,).
    /// For each stream permission there's long variant (same as in SDK in
    /// iggy::models::permissions::StreamPermissions) and short variant.
    ///
    /// Available stream permissions: manage_stream / m_str, read_stream / r_str, manage_topics / m_top,
    /// read_topics / r_top, poll_messages / p_msg, send_messages / s_msg.
    ///
    /// For each stream one can set permissions for each topic separately. Topic permissions
    /// are defined for each topic separately. Setting permission for topic allows to set each
    /// permission individually, by default, if no permission is provided (only topic ID is provided)
    /// all are set to false. Topic permission format consists of topic ID followed by colon (:)
    /// and list of permissions separated by comma (,). For each topic permission there's long
    /// variant (same as in SDK in iggy::models::permissions::TopicPermissions) and short variant.
    /// Topic permissions are separated by hash (#) after stream permissions.
    ///
    /// Available topic permissions: manage_topic / m_top, read_topic / r_top, poll_messages / p_msg,
    /// send_messages / s_msg.
    ///
    /// Permissions format: STREAM_ID\[:STREAM_PERMISSIONS\]\[#TOPIC_ID\[:TOPIC_PERMISSIONS\]\]
    ///
    /// Examples:
    ///  iggy user create guest guest -s 1:manage_topics,read_topics
    ///  iggy user create admin p@Ss! --stream-permissions 2:m_str,r_str,m_top,r_top,p_msg,s_msg
    ///  iggy user create sender s3n43r -s 3#1:s_msg#2:s_msg
    ///  iggy user create user1 test12 -s 4:manage_stream,r_top#1:s_msg,p_msg#2:manage_topic
    #[clap(short, long, verbatim_doc_comment)]
    #[arg(value_parser = clap::value_parser!(StreamPermissionsArg))]
    pub(crate) stream_permissions: Option<Vec<StreamPermissionsArg>>,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct UserDeleteArgs {
    /// User ID to delete
    ///
    /// The user ID can be specified as either a username or an ID
    pub(crate) user_id: Identifier,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct UserGetArgs {
    /// User ID to get
    ///
    /// The user ID can be specified as either a username or an ID
    pub(crate) user_id: Identifier,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct UserListArgs {
    /// List mode (table or list)
    #[clap(short, long, value_enum, default_value_t = ListMode::Table)]
    pub(crate) list_mode: ListMode,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct UserNameArgs {
    /// User ID to update
    ///
    /// The user ID can be specified as either a username or an ID
    pub(crate) user_id: Identifier,
    /// New username
    ///
    /// New and unique identifier for the user account on iggy server,
    /// must be between 3 and 50 characters long.
    #[clap(verbatim_doc_comment)]
    pub(crate) username: String,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct UserStatusArgs {
    /// User ID to update
    ///
    /// The user ID can be specified as either a username or an ID
    pub(crate) user_id: Identifier,
    /// New status
    pub(crate) status: UserStatusArg,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct UserPasswordArgs {
    /// User ID to update
    ///
    /// The user ID can be specified as either a username or an ID
    pub(crate) user_id: Identifier,
    /// Current password
    ///
    /// Current password, must be between 3 and 100 characters long.
    /// An optional parameter to specify the current password for the given user.
    /// If not provided, the user will be prompted interactively to enter the
    /// password securely, and the quiet mode option will not have any effect.
    #[clap(verbatim_doc_comment)]
    pub(crate) current_password: Option<String>,
    /// New password
    ///
    /// New password, must be between 3 and 100 characters long.
    /// An optional parameter to specify the new password for the given user.
    /// If not provided, the user will be prompted interactively to enter the
    /// password securely, and the quiet mode option will not have any effect.
    #[clap(verbatim_doc_comment)]
    pub(crate) new_password: Option<String>,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct UserPermissionsArgs {
    /// User ID to update
    ///
    /// The user ID can be specified as either a username or an ID
    pub(crate) user_id: Identifier,
    /// Set global permissions for created user
    ///
    /// All global permissions by default are set to false and this command line option
    /// allows to set each permission individually. Permissions are separated
    /// by comma and each permission is identified by the same name as in the iggy
    /// SDK in iggy::models::permissions::GlobalPermissions struct. For each permission
    /// there's long variant (same as in SDK) and short variant.
    ///
    /// Available permissions (long and short versions):  manage_servers / m_srv,
    /// read_servers / r_srv, manage_users / m_usr, read_users / r_usr,
    /// manage_streams / m_str, read_streams / r_str, manage_topics / m_top,
    /// read_topics / r_top, poll_messages / p_msg, send_messages / s_msg
    ///
    /// Examples:
    ///  iggy user create guest guess --global-permissions p_msg,s_msg
    ///  iggy user create admin pass#1%X! -g m_srv,r_srv,m_usr,r_usr,m_str,r_str,m_top,r_top,p_msg,s_msg
    #[clap(short, long, verbatim_doc_comment)]
    #[arg(value_parser = clap::value_parser!(GlobalPermissionsArg))]
    pub(crate) global_permissions: Option<GlobalPermissionsArg>,
    /// Set stream permissions for created user
    ///
    /// Stream permissions are defined by each stream separately. Setting permission for stream
    /// allows to set each permission individually, by default, if no permission is provided
    /// (only stream ID is provided) all are set to false. Stream permission format consists
    /// of stream ID followed by colon (:) and list of permissions separated by comma (,).
    /// For each stream permission there's long variant (same as in SDK in
    /// iggy::models::permissions::StreamPermissions) and short variant.
    ///
    /// Available stream permissions: manage_stream / m_str, read_stream / r_str, manage_topics / m_top,
    /// read_topics / r_top, poll_messages / p_msg, send_messages / s_msg.
    ///
    /// For each stream one can set permissions for each topic separately. Topic permissions
    /// are defined for each topic separately. Setting permission for topic allows to set each
    /// permission individually, by default, if no permission is provided (only topic ID is provided)
    /// all are set to false. Topic permission format consists of topic ID followed by colon (:)
    /// and list of permissions separated by comma (,). For each topic permission there's long
    /// variant (same as in SDK in iggy::models::permissions::TopicPermissions) and short variant.
    /// Topic permissions are separated by hash (#) after stream permissions.
    ///
    /// Available topic permissions: manage_topic / m_top, read_topic / r_top, poll_messages / p_msg,
    /// send_messages / s_msg.
    ///
    /// Permissions format: STREAM_ID\[:STREAM_PERMISSIONS\]\[#TOPIC_ID\[:TOPIC_PERMISSIONS\]\]
    ///
    /// Examples:
    ///  iggy user create guest guest -s 1:manage_topics,read_topics
    ///  iggy user create admin p@Ss! --stream-permissions 2:m_str,r_str,m_top,r_top,p_msg,s_msg
    ///  iggy user create sender s3n43r -s 3#1:s_msg#2:s_msg
    ///  iggy user create user1 test12 -s 4:manage_stream,r_top#1:s_msg,p_msg#2:manage_topic
    #[clap(short, long, verbatim_doc_comment)]
    #[arg(value_parser = clap::value_parser!(StreamPermissionsArg))]
    pub(crate) stream_permissions: Option<Vec<StreamPermissionsArg>>,
}
