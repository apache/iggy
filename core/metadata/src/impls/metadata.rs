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
use clock::Clock;
use consensus::{Consensus, Project};
use std::marker::PhantomData;
use tracing::debug;

// TODO: Define a trait (probably in some external crate)
#[expect(unused)]
trait Metadata<C, T>
where
    C: Consensus<T>,
    T: Clock,
{
    fn on_request(&self, message: C::RequestMessage);
    fn on_replicate(&self, message: C::ReplicateMessage);
    fn on_ack(&self, message: C::AckMessage);
}

#[expect(unused)]
struct IggyMetadata<C, M, J, S, T>
where
    C: Consensus<T>,
    T: Clock,
{
    consensus: C,
    mux_stm: M,
    journal: J,
    snapshot: S,

    _t: PhantomData<T>,
}

impl<C, M, J, S, T> Metadata<C, T> for IggyMetadata<C, M, J, S, T>
where
    C: Consensus<T>,
    T: Clock,
{
    fn on_request(&self, message: C::RequestMessage) {
        debug!("handling metadata request");
        let _message = message.project(&self.consensus);
    }

    fn on_replicate(&self, _message: C::ReplicateMessage) {
        todo!()
    }

    fn on_ack(&self, _message: C::AckMessage) {
        todo!()
    }
}

// TODO: Hide with associated types all of those generics, so they are not leaking to the upper layer
#[expect(unused)]
pub trait MetadataHandle {}
