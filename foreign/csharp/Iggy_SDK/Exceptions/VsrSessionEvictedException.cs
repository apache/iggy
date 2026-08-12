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

namespace Apache.Iggy.Exceptions;

/// <summary>
///     An eviction frame arrived where a reply was expected. Internal: the transport decides whether the caller
///     sees <see cref="Verdict" /> or an unknown outcome, depending on whether the outstanding request could
///     still have committed.
/// </summary>
/// <remarks>
///     The server emits evictions off its own heartbeat timer rather than as an answer, so the frame carries no
///     correlation with the request it interrupts and that request's real reply is never read.
/// </remarks>
internal sealed class VsrSessionEvictedException(Exception verdict)
    : Exception("The consensus session was evicted by the server.", verdict)
{
    /// <summary>The error the eviction reason maps to, for the requests it can safely be reported as.</summary>
    internal Exception Verdict { get; } = verdict;
}
