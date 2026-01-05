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

/// This macro generates the ServerCommand enum and associated dispatch logic.
///
/// It performs the following expansions:
/// 1) The `pub enum ServerCommand` with all variants
/// 2) The `from_code_and_payload(code, payload)` function
/// 3) The `from_code_and_reader` async function
/// 4) The `to_bytes()` function
/// 5) The `validate()` function
/// 6) The `dispatch()` function that performs authentication and calls handlers
/// 7) The `code()` method
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
            // Unauthenticated variants
            $(
                $unauth_variant($unauth_ty),
            )*
            // Authenticated variants
            $(
                $auth_variant($auth_ty),
            )*
        }

        impl ServerCommand {
            /// Returns the command code.
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

            /// Constructs a `ServerCommand` from its numeric code and payload.
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

            /// Constructs a ServerCommand from its numeric code by reading from the provided async reader.
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

            /// Converts the command into raw bytes.
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

            /// Validate the command by delegating to the inner command's implementation.
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

            /// Dispatch the command to its handler.
            ///
            /// For authenticated commands, this performs authentication first and passes
            /// the `Auth` proof token to the handler.
            ///
            /// For unauthenticated commands, the handler is called directly.
            pub async fn dispatch(
                self,
                sender: &mut SenderKind,
                length: u32,
                session: &Session,
                shard: &Rc<IggyShard>,
            ) -> Result<HandlerResult, IggyError> {
                match self {
                    // Unauthenticated commands - call handler directly
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
                    // Authenticated commands - authenticate first, then call handler
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
