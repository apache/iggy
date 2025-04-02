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

/// This macro does 4 expansions in one go:
///
/// 1) The `#[enum_dispatch(ServerCommandHandler)] pub enum ServerCommand`
///    with all variants.
/// 2) The `from_code_and_payload(code, payload)` function that matches each code.
/// 3) The `to_bytes()` function that matches each variant.
/// 4) The `validate()` function that matches each variant.
#[macro_export]
macro_rules! define_server_command_enum {
    (
                $(
            // Macro pattern:
            // variant name     inner type    numeric code   display string   show_payload?
            $variant:ident ( $ty:ty ), $code:ident, $display_str:expr, $show_payload:expr
        );* $(;)?
    ) => {
        #[enum_dispatch(ServerCommandHandler)]
        #[derive(Debug, PartialEq, EnumString)]
        pub enum ServerCommand {
            $(
                $variant($ty),
            )*
        }

        impl ServerCommand {
            /// Constructs a `ServerCommand` from its numeric code and payload.
            pub fn from_code_and_payload(code: u32, payload: Bytes) -> Result<Self, IggyError> {
                match code {
                    $(
                        $code => Ok(ServerCommand::$variant(
                            <$ty>::from_bytes(payload)?
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
                        $code => Ok(ServerCommand::$variant(
                            <$ty as BinaryServerCommand>::from_sender(sender, code, length).await?
                        )),
                    )*
                    _ => Err(IggyError::InvalidCommand),
                }
            }

            /// Converts the command into raw bytes.
            pub fn to_bytes(&self) -> Bytes {
                match self {
                    $(
                        ServerCommand::$variant(payload) => as_bytes(payload),
                    )*
                }
            }

            /// Validate the command by delegating to the inner command’s implementation.
            pub fn validate(&self) -> Result<(), IggyError> {
                match self {
                    $(
                        ServerCommand::$variant(cmd) => <$ty as iggy::validatable::Validatable<iggy::error::IggyError>>::validate(cmd),
                    )*
                }
            }
        }


        impl std::fmt::Display for ServerCommand {
            fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                match self {
                    $(
                        // If $show_payload is true, we display the command name + payload;
                        // otherwise, we just display the command name.
                        ServerCommand::$variant(payload) => {
                            if $show_payload {
                                write!(formatter, "{}|{payload:?}", $display_str)
                            } else {
                                write!(formatter, "{}", $display_str)
                            }
                        },
                    )*
                }
            }
        }
    };

}
