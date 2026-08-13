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
///     The request produced no server verdict after transmission began, so the server may already have committed
///     it. Replaying it on a fresh consensus session would bypass server-side deduplication, so the SDK refuses to
///     retry and surfaces this instead: the caller decides whether re-issuing the operation is safe.
/// </summary>
/// <remarks>
///     Deliberately derived from <see cref="Exception" /> rather than <see cref="IOException" /> or
///     <see cref="OperationCanceledException" />, whichever ended the request. Both of those are routinely caught
///     and either retried or swallowed, which are the two responses this type exists to prevent. The triggering
///     exception is preserved as <see cref="Exception.InnerException" />.
/// </remarks>
public sealed class VsrRequestOutcomeUnknownException(Exception innerException)
    : Exception("The VSR request outcome is unknown because no server verdict arrived after transmission began.",
        innerException);
