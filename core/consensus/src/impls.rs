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

use crate::{Consensus, Project};

pub struct VsrConsensus;

#[derive(Clone)]
pub struct Request;

impl Project<Prepare> for Request {
    type Consensus = VsrConsensus;
    fn project(self, _consensus: &Self::Consensus) -> Prepare {
        Prepare
    }
}

#[derive(Clone)]
pub struct Prepare;

impl Project<PrepareOk> for Prepare {
    type Consensus = VsrConsensus;
    fn project(self, _consensus: &Self::Consensus) -> PrepareOk {
        PrepareOk
    }
}

#[derive(Clone)]
pub struct PrepareOk;

impl Consensus for VsrConsensus {
    type RequestMessage = Request;
    type ReplicateMessage = Prepare;
    type AckMessage = PrepareOk;

    fn pipeline_message(&self, _message: Self::ReplicateMessage) {
        todo!()
    }

    fn verify_pipeline(&self) {
        todo!()
    }

    fn post_replicate_verify(&self, _message: &Self::ReplicateMessage) {
        todo!()
    }
}
