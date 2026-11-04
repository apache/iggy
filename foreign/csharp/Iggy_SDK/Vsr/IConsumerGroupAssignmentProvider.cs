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

using Apache.Iggy.IggyClient;

namespace Apache.Iggy.Vsr;

/// <summary>
///     Exposes the partitions that the consumer-group member of a transport owns, so a group reader can read them. A
///     transport without it, such as HTTP, cannot serve a group reader.
/// </summary>
internal interface IConsumerGroupAssignmentProvider
{
    /// <summary>
    ///     The partitions the member owns, in assignment order, or an empty list while it owns none. The client asks
    ///     the coordinator again when it holds no assignment, when the one it holds is older than its refresh
    ///     interval, or when <paramref name="resync" /> is set.
    /// </summary>
    /// <param name="streamId">The identifier of the stream containing the topic (numeric ID or name).</param>
    /// <param name="topicId">The identifier of the topic (numeric ID or name).</param>
    /// <param name="groupId">The identifier of the consumer group (numeric ID or name).</param>
    /// <param name="resync">Whether to ask the coordinator even for a fresh assignment, after a fence.</param>
    /// <param name="token">The cancellation token to cancel the operation.</param>
    Task<IReadOnlyList<uint>> GetAssignedPartitionsAsync(Identifier streamId, Identifier topicId, Identifier groupId,
        bool resync, CancellationToken token = default);
}
