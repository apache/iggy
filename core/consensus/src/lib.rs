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

pub trait Project<U> {
    type Consensus: Consensus;
    fn project(self, consensus: &Self::Consensus) -> U;
}

pub trait Consensus {
    // I am wondering, whether we should create a dedicated trait for cloning, so it's explicit that we do ref counting.
    type RequestMessage: Project<Self::ReplicateMessage, Consensus = Self> + Clone;
    type ReplicateMessage: Project<Self::AckMessage, Consensus = Self> + Clone;
    type AckMessage;

    fn pipeline_message(&self, message: Self::ReplicateMessage);
    fn verify_pipeline(&self);

    // TODO: Figure out how we can achieve that without exposing such methods in the Consensus trait.
    fn post_replicate_verify(&self, message: &Self::ReplicateMessage);
}

mod impls;
pub use impls::*;
