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

namespace Apache.Iggy.Vsr;

/// <summary>
///     Reason carried at byte 255 of an eviction frame. Session-terminal, never transient.
///     Discriminants are wire-pinned; a value outside this set decodes as <see cref="Reserved" />.
/// </summary>
internal enum EvictionReason : byte
{
    Reserved = 0,
    NoSession = 1,
    ClientReleaseTooLow = 2,
    ClientReleaseTooHigh = 3,
    InvalidRequestOperation = 4,
    InvalidRequestBody = 5,
    InvalidRequestBodySize = 6,
    SessionTooLow = 7,
    SessionReleaseMismatch = 8,
    InvalidCredentials = 9,
    InvalidToken = 10,
    UserInactive = 11,
    SessionError = 12,
    StaleClient = 13,
    IncompatibleProtocol = 14,
    MalformedLogin = 15
}
