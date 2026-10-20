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
///     The client neither retains this operation nor keeps a proved outcome for it. A session reset or an abandon
///     dropped the operation before a replay proved its outcome, or that outcome aged out. Its absence does not prove
///     that it did not commit.
/// </summary>
public sealed class VsrOperationNotRetainedException(ulong operationId)
    : Exception("The operation is not retained and the client keeps no proved outcome for it; it may have committed.")
{
    /// <summary>The request id of the operation.</summary>
    public ulong OperationId { get; } = operationId;
}
