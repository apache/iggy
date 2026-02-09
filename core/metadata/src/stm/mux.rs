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

use iggy_common::{header::PrepareHeader, message::Message};

use crate::stm::{State, StateMachine};

// MuxStateMachine that proxies to an tuple of variadic state machines
#[derive(Debug)]
pub struct MuxStateMachine<T>
where
    T: StateMachine,
{
    inner: T,
}

impl<T> MuxStateMachine<T>
where
    T: StateMachine,
{
    pub fn new(inner: T) -> Self {
        Self { inner }
    }
}

impl<T> StateMachine for MuxStateMachine<T>
where
    T: StateMachine,
{
    type Input = T::Input;
    type Output = T::Output;

    fn update(&self, input: Self::Input) -> Self::Output {
        self.inner.update(input)
    }
}

//TODO: Move to common
#[macro_export]
macro_rules! variadic {
    () => ( () );
    (...$a:ident  $(,)? ) => ( $a );
    (...$a:expr  $(,)? ) => ( $a );
    ($a:ident  $(,)? ) => ( ($a, ()) );
    ($a:expr  $(,)? ) => ( ($a, ()) );
    ($a:ident,  $( $b:tt )+) => ( ($a, variadic!( $( $b )* )) );
    ($a:expr,  $( $b:tt )+) => ( ($a, variadic!( $( $b )* )) );
}

// TODO: Figure out how to get around the fact that we need to hardcode the Input/Output type for base case.
// TODO: I think we could move the base case to the impl site of `State`, so this way we know the `Input` and `Output` types.
// Base case of the recursive resolution.
impl StateMachine for () {
    type Input = Message<PrepareHeader>;
    // TODO: Make sure that the `Output` matches to the output type of the rest of list.
    // TODO: Add a trait bound to the output that will allow us to get the response in bytes.
    type Output = ();

    fn update(&self, _input: Self::Input) -> Self::Output {}
}

// Recursive case: process head and recurse on tail
// No Clone bound needed - ownership passes through via Result
impl<O, S, Rest> StateMachine for variadic!(S, ...Rest)
where
    S: State<Output = O>,
    Rest: StateMachine<Input = S::Input, Output = O>,
{
    type Input = Rest::Input;
    type Output = O;

    fn update(&self, input: Self::Input) -> Self::Output {
        match self.0.apply(input) {
            Ok(result) => result,
            Err(input) => self.1.update(input),
        }
    }
}

mod tests {

    #[test]
    fn construct_mux_state_machine_from_states_with_same_output() {
        use crate::stm::StateMachine;
        use crate::stm::mux::MuxStateMachine;
        use crate::stm::stream::{Streams, StreamsInner};
        use crate::stm::user::{Users, UsersInner};
        use iggy_common::header::PrepareHeader;
        use iggy_common::message::Message;

        let users: Users = UsersInner::new().into();
        let streams: Streams = StreamsInner::new().into();
        let mux = MuxStateMachine::new(variadic!(users, streams));

        let input = Message::new(std::mem::size_of::<PrepareHeader>());

        mux.update(input);
    }
}
