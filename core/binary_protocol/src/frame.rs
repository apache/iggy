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

// TODO(hubcio): Legacy framing constants for the current binary protocol.
// Once VSR consensus is integrated, both client-server and
// replica-replica traffic will use the unified 256-byte
// consensus header (`consensus::header::HEADER_SIZE`).
// These constants will be removed at that point.

/// Request frame: `[length:4 LE][code:4 LE][payload:N]`
/// `length` = size of code + payload = 4 + N
pub const REQUEST_HEADER_SIZE: usize = 4;

/// Response frame: `[status:4 LE][length:4 LE][payload:N]`
pub const RESPONSE_HEADER_SIZE: usize = 8;

/// Status code for a successful response.
pub const STATUS_OK: u32 = 0;
