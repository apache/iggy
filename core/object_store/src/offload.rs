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

use futures::channel::oneshot;
use std::io;

/// Runs a call io_uring cannot express on its own OS thread. Shard executors have no blocking
/// pool, and the shard's shared `run_blocking` permit also serves partition file work, which a
/// hung store mount must never stall.
pub(crate) async fn offload<T: Send + 'static>(
    name: &'static str,
    operation: impl FnOnce() -> T + Send + 'static,
) -> io::Result<T> {
    let (sender, receiver) = oneshot::channel();
    std::thread::Builder::new()
        .name(name.to_owned())
        .spawn(move || {
            let _ = sender.send(operation());
        })?;
    receiver
        .await
        .map_err(|_| io::Error::other(format!("{name} stopped")))
}
