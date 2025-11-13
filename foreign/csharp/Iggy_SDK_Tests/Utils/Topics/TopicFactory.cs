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

using Apache.Iggy.Contracts.Http;
using Apache.Iggy.Enums;

namespace Apache.Iggy.Tests.Utils.Topics;

internal static class TopicFactory
{
    internal static (uint topicId, uint partitionsCount, string topicName, uint messageExpriy, ulong sizeBytes, ulong
        messagesCount, ulong createdAt, byte replicationFactor, ulong maxTopicSize)
        CreateTopicResponseFields()
    {
        var topicId = (uint)Random.Shared.Next(1, 69);
        var partitionsCount = (uint)Random.Shared.Next(1, 69);
        var topicName = "Topic " + Random.Shared.Next(1, 69);
        var messageExpiry = (uint)Random.Shared.Next(1, 69);
        var sizeBytes = (ulong)Random.Shared.Next(1, 69);
        var messagesCount = (ulong)Random.Shared.Next(69, 42069);
        var createdAt = (ulong)Random.Shared.Next(69, 42069);
        var maxTopicSize = (ulong)Random.Shared.NextInt64(2_000_000_000, 10_000_000_000);
        var replicationFactor = (byte)Random.Shared.Next(1, 8);
        return (topicId, partitionsCount, topicName, messageExpiry, sizeBytes, messagesCount, createdAt,
            replicationFactor, maxTopicSize);
    }
}
