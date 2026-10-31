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

/// <summary>Where a committing deferred poll (106) or fetch (109) reports an error.</summary>
internal enum CommitErrorScope
{
    /// <summary>The whole reply is the error.</summary>
    Request,

    /// <summary>One section of a fetch reply carries the error status.</summary>
    Section
}

internal static class CommitOutcome
{
    /// <summary>
    ///     Whether <paramref name="status" />, reported in <paramref name="scope" />, proves that the server stored no
    ///     offset. Any other status, a lost reply and a transport failure after the send leave the outcome unknown.
    /// </summary>
    internal static bool ProvesNothingStored(this CommitErrorScope scope, int status)
    {
        return scope switch
        {
            // A first message above the byte limit and a new offset key above the limit are refused before the store.
            CommitErrorScope.Request => status is VsrError.FEATURE_UNAVAILABLE or VsrError.INVALID_IDENTIFIER
                or VsrError.TRANSIENT_NOT_ACCEPTED or VsrError.INVALID_SIZE_BYTES or VsrError.HISTORY_UNAVAILABLE
                or VsrError.NOT_PRIMARY or VsrError.DEFERRED_POLL_CAPACITY_EXCEEDED or VsrError.STREAM_ID_NOT_FOUND
                or VsrError.STREAM_NAME_NOT_FOUND or VsrError.TOPIC_ID_NOT_FOUND or VsrError.TOPIC_NAME_NOT_FOUND
                or VsrError.PARTITION_NOT_FOUND or VsrError.TOO_MANY_CONSUMER_OFFSETS or VsrError.INVALID_OFFSET
                or VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED,
            // A section whose read may have stored never carries one of these.
            _ => status is VsrError.TRANSIENT_NOT_ACCEPTED or VsrError.INVALID_SIZE_BYTES
                or VsrError.HISTORY_UNAVAILABLE or VsrError.NOT_PRIMARY or VsrError.PARTITION_NOT_FOUND
                or VsrError.TOO_MANY_CONSUMER_OFFSETS or VsrError.CONSUMER_GROUP_PARTITION_NOT_OWNED
        };
    }
}
