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

mod questdb_sink;

const TEST_MESSAGE_COUNT: usize = 3;

/// Shared poll budget for the waits in this suite. 120 attempts at 100 ms gives
/// twelve seconds, which covers a container that is slow to apply a write
/// without masking a real stall.
const POLL_ATTEMPTS: usize = 120;
const POLL_INTERVAL_MS: u64 = 100;
