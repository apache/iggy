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

/// Generates `ServerCommand` enum with `@unauth`/`@auth` split and `dispatch()` method.
/// For `@auth` commands, `dispatch()` calls `shard.auth(session)?` before handler.
#[macro_export]
macro_rules! define_server_command_enum {
    (
        @unauth {
            $(
                $unauth_variant:ident ( $unauth_ty:ty ), $unauth_code:ident, $unauth_display:expr, $unauth_show_payload:expr
            );* $(;)?
        }
        @auth {
            $(
                $auth_variant:ident ( $auth_ty:ty ), $auth_code:ident, $auth_display:expr, $auth_show_payload:expr
            );* $(;)?
        }
    ) => {
        #[derive(Debug, PartialEq, EnumString)]
        pub enum ServerCommand {
            $(
                $unauth_variant($unauth_ty),
            )*
            $(
                $auth_variant($auth_ty),
            )*
        }

        impl ServerCommand {
            pub fn code(&self) -> u32 {
                match self {
                    $(
                        ServerCommand::$unauth_variant(_) => $unauth_code,
                    )*
                    $(
                        ServerCommand::$auth_variant(_) => $auth_code,
                    )*
                }
            }

            pub fn from_code_and_payload(code: u32, payload: Bytes) -> Result<Self, IggyError> {
                match code {
                    $(
                        $unauth_code => Ok(ServerCommand::$unauth_variant(
                            <$unauth_ty>::from_bytes(payload)?
                        )),
                    )*
                    $(
                        $auth_code => Ok(ServerCommand::$auth_variant(
                            <$auth_ty>::from_bytes(payload)?
                        )),
                    )*
                    _ => {
                        error!("Invalid server command: {}", code);
                        Err(IggyError::InvalidCommand)
                    }
                }
            }

            pub async fn from_code_and_reader(
                code: u32,
                sender: &mut SenderKind,
                length: u32,
            ) -> Result<Self, IggyError> {
                match code {
                    $(
                        $unauth_code => Ok(ServerCommand::$unauth_variant(
                            <$unauth_ty as BinaryServerCommand>::from_sender(sender, code, length).await?
                        )),
                    )*
                    $(
                        $auth_code => Ok(ServerCommand::$auth_variant(
                            <$auth_ty as BinaryServerCommand>::from_sender(sender, code, length).await?
                        )),
                    )*
                    _ => Err(IggyError::InvalidCommand),
                }
            }

            pub fn to_bytes(&self) -> Bytes {
                match self {
                    $(
                        ServerCommand::$unauth_variant(payload) => as_bytes(payload),
                    )*
                    $(
                        ServerCommand::$auth_variant(payload) => as_bytes(payload),
                    )*
                }
            }

            pub fn validate(&self) -> Result<(), IggyError> {
                match self {
                    $(
                        ServerCommand::$unauth_variant(cmd) => <$unauth_ty as iggy_common::Validatable<iggy_common::IggyError>>::validate(cmd),
                    )*
                    $(
                        ServerCommand::$auth_variant(cmd) => <$auth_ty as iggy_common::Validatable<iggy_common::IggyError>>::validate(cmd),
                    )*
                }
            }

            pub async fn dispatch(
                self,
                sender: &mut SenderKind,
                length: u32,
                session: &Session,
                shard: &Rc<IggyShard>,
            ) -> Result<HandlerResult, IggyError> {
                match self {
                    $(
                        ServerCommand::$unauth_variant(cmd) => {
                            <$unauth_ty as UnauthenticatedHandler>::handle(
                                cmd,
                                sender,
                                length,
                                session,
                                shard,
                            ).await
                        }
                    )*
                    $(
                        ServerCommand::$auth_variant(cmd) => {
                            let auth = shard.auth(session)?;
                            <$auth_ty as AuthenticatedHandler>::handle(
                                cmd,
                                sender,
                                length,
                                auth,
                                session,
                                shard,
                            ).await
                        }
                    )*
                }
            }
        }


        impl std::fmt::Display for ServerCommand {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    $(
                        ServerCommand::$unauth_variant(payload) => {
                            if $unauth_show_payload {
                                write!(formatter, "{}|{payload:?}", $unauth_display)
                            } else {
                                write!(formatter, "{}", $unauth_display)
                            }
                        },
                    )*
                    $(
                        ServerCommand::$auth_variant(payload) => {
                            if $auth_show_payload {
                                write!(formatter, "{}|{payload:?}", $auth_display)
                            } else {
                                write!(formatter, "{}", $auth_display)
                            }
                        },
                    )*
                }
            }
        }
    };
}
