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

pub mod consumer_group;
pub mod mux;
pub mod stream;
pub mod user;

/// Macro to generate a `{State}Command` enum and implement `StateCommand` trait.
///
/// # Arguments
/// * `$state_type` - The type that implements `ApplyState` trait
/// * `$command_enum` - The name of the command enum to generate (e.g., StreamsCommand)
/// * `$operations` - Array of Operation enum variants (also used as payload type names)
///
/// # Example
/// ```ignore
/// define_state_command! {
///     Streams,
///     StreamsCommand,
///     [CreateStream, UpdateStream, DeleteStream, PurgeStream]
/// }
/// ```
#[macro_export]
macro_rules! define_state_command {
    (
        $state_type:ty,
        $command_enum:ident,
        [$($operation:ident),* $(,)?]
    ) => {
        #[derive(Debug)]
        pub enum $command_enum {
            $(
                $operation($operation),
            )*
        }

        impl $crate::stm::StateCommand for $state_type {
            type Command = $command_enum;
            type Input = ::iggy_common::message::Message<::iggy_common::header::PrepareHeader>;

            fn into_command(input: &Self::Input) -> Option<Self::Command> {
                use ::iggy_common::BytesSerializable;
                use ::bytes::Bytes;
                use ::iggy_common::header::Operation;

                // TODO: rework this thing, so we don't copy the bytes on each request
                let body = Bytes::copy_from_slice(input.body());
                match input.header().operation {
                    $(
                        Operation::$operation => {
                            Some($command_enum::$operation(
                                $operation::from_bytes(body.clone()).unwrap()
                            ))
                        },
                    )*
                    _ => None,
                }
            }
        }

        // Compile-time check that the type implements ApplyState
        const _: () = {
            const fn assert_impl_apply_state<T: $crate::stm::ApplyState>() {}
            assert_impl_apply_state::<$state_type>();
        };
    };
}

// This is public interface to state, therefore it will be imported from different crate, for now during development I am leaving it there.
pub trait State
where
    Self: Sized,
{
    type Output;
    type Input;

    // Apply the state machine logic and return an optional output.
    // The output is optional, as we model the `StateMachine`, as an variadic list,
    // where not all state machines will produce an output for every input event.
    fn apply(&self, input: &Self::Input) -> Option<Self::Output>;
}

// TODO: This interface should be private to the stm module.
pub trait StateMachine {
    type Input;
    type Output;
    fn update(&self, input: &Self::Input, output: &mut Vec<Self::Output>);
}

pub trait StateCommand {
    type Command;
    type Input;

    fn into_command(input: &Self::Input) -> Option<Self::Command>;
}

pub trait ApplyState: StateCommand {
    type Output;

    fn do_apply(&self, cmd: Self::Command) -> Self::Output;
}

impl<T> State for T
where
    T: ApplyState,
{
    type Output = T::Output;
    type Input = T::Input;

    fn apply(&self, input: &Self::Input) -> Option<Self::Output> {
        T::into_command(input).map(|cmd| self.do_apply(cmd))
    }
}
