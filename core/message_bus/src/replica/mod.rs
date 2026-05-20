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

//! Replica plane: shard-0 bootstrap orchestrator (`io`) and inbound
//! TCP listener (`listener`).
//!
//! The replica plane is TCP-only by design — see [`listener`] for the
//! load-bearing invariants. Outbound dialing and the per-process
//! shard-0 bootstrap (which also binds the SDK client listeners) live
//! in [`io`].

pub mod io;
pub mod listener;
