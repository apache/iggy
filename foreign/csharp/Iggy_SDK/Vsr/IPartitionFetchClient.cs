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

using Apache.Iggy.Contracts;
using Apache.Iggy.Kinds;

namespace Apache.Iggy.Vsr;

/// <summary>
///     A transport that reads several partitions of one primary in one request (code 109). A partition reader groups
///     its partitions with it, and reads each partition with its own poll over a transport without it.
/// </summary>
internal interface IPartitionFetchClient
{
    Task<FetchedMessages> FetchMessagesAsync(Identifier streamId, Identifier topicId,
        IReadOnlyList<(uint PartitionId, PollingStrategy Strategy)> targets, Consumer consumer, uint count,
        bool autoCommit, DeferredPollOptions options, CancellationToken token = default);

    Task<IReadOnlyList<IReadOnlyList<uint>>> ReadinessGroupsAsync(Identifier streamId, Identifier topicId,
        IReadOnlyList<uint> partitions, CancellationToken token = default);
}
