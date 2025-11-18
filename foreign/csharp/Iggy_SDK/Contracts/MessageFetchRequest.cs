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

using Apache.Iggy.Kinds;

namespace Apache.Iggy.Contracts;

/// <summary>
///     Request for fetching messages from a topic.
/// </summary>
public sealed class MessageFetchRequest
{
    /// <summary>
    ///     Consumer requesting the messages.
    /// </summary>
    public required Consumer Consumer { get; init; }

    /// <summary>
    ///     Stream identifier.
    /// </summary>
    public required Identifier StreamId { get; init; }

    /// <summary>
    ///     Topic identifier.
    /// </summary>
    public required Identifier TopicId { get; init; }

    /// <summary>
    ///     Partition identifier.
    /// </summary>
    public uint? PartitionId { get; init; }

    /// <summary>
    ///     Polling strategy.
    /// </summary>
    public required PollingStrategy PollingStrategy { get; init; }

    /// <summary>
    ///     Maximum number of messages to fetch.
    /// </summary>
    public required uint Count { get; init; }

    /// <summary>
    ///     Whether to commit the offset automatically.
    /// </summary>
    public required bool AutoCommit { get; init; }
}
