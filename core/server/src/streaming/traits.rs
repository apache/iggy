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

use crate::configs::system::SystemConfig;
use crate::shard::task_registry::TaskRegistry;
use std::future::Future;
use std::rc::Rc;

// TODO: Major revision of this trait.
pub trait MainOps {
    type Namespace;
    type PollingArgs;
    type Consumer;
    type In;
    type Out;
    type Error;

    fn append_messages(
        &self,
        config: &SystemConfig,
        registry: &Rc<TaskRegistry>,
        ns: &Self::Namespace,
        input: Self::In,
    ) -> impl Future<Output = Result<(), Self::Error>>;
    fn poll_messages(
        &self,
        ns: &Self::Namespace,
        consumer: Self::Consumer,
        args: Self::PollingArgs,
    ) -> impl Future<Output = Result<Self::Out, Self::Error>>;
}
