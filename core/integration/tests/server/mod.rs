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

// a2a_jwt is HTTP-only (JWT against the HTTP transport); vsr has no HTTP.
#[cfg(not(feature = "vsr"))]
mod a2a_jwt;
// Consumer groups are not implemented under vsr yet.
#[cfg(not(feature = "vsr"))]
mod cg;
// 80-case race matrix with hardcoded HTTP variants (test_matrix bypasses
// the harness transport filter); revisit under vsr once basics are green.
#[cfg(not(feature = "vsr"))]
mod concurrent_addition;
mod general;
// Asserts the periodic messages cleaner deletes expired segments from disk;
// server-ng has no data-maintenance cleaner yet.
#[cfg(not(feature = "vsr"))]
mod message_cleanup;
mod message_retrieval;
// Mixes server restarts, consumer-group barriers, and DeleteSegments
// maintenance; out of vsr scope for now.
#[cfg(not(feature = "vsr"))]
mod purge_delete;
mod scenarios;
mod specific;
