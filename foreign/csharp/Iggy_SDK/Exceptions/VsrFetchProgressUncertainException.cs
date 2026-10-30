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

using Apache.Iggy.Vsr;

namespace Apache.Iggy.Exceptions;

/// <summary>
///     An uncertain committing fetch retains local progress blocks. Its operation id is a recovery key, not a
///     replayable server receipt. Explicit abandonment accepts that the old read may still commit.
/// </summary>
public sealed class VsrFetchProgressUncertainException(ulong operationId, Exception? cause = null)
    : VsrRequestOutcomeUnknownException(cause ?? new InvalidOperationException(
        $"Read {operationId} has unknown progress and requires explicit abandonment."), operationId)
{
    /// <summary>The SDK-local uncertain-fetch status.</summary>
    public const int STATUS_CODE = VsrError.FETCH_PROGRESS_UNCERTAIN;

    /// <summary>The SDK-local uncertain-fetch status.</summary>
    public int StatusCode => STATUS_CODE;
}
