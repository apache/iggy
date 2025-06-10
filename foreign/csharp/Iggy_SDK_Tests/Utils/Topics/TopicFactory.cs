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
    internal static (int topicId, int partitionsCount, string topicName, int messageExpriy, ulong sizeBytes, ulong
        messagesCount, ulong createdAt, byte replicationFactor, ulong maxTopicSize)
        CreateTopicResponseFields()
    {
        int topicId = Random.Shared.Next(1, 69);
        int partitionsCount = Random.Shared.Next(1, 69);
        string topicName = "Topic " + Random.Shared.Next(1, 69);
        int messageExpiry = Random.Shared.Next(1, 69);
        ulong sizeBytes = (ulong)Random.Shared.Next(1, 69);
        ulong messagesCount = (ulong)Random.Shared.Next(69, 42069);
        ulong createdAt = (ulong)Random.Shared.Next(69, 42069);
        ulong maxTopicSize = (ulong)Random.Shared.NextInt64(2_000_000_000, 10_000_000_000);
        byte replicationFactor = (byte)Random.Shared.Next(1, 8);
        return (topicId, partitionsCount, topicName, messageExpiry, sizeBytes, messagesCount, createdAt, replicationFactor, maxTopicSize);
    }
    
    internal static TopicRequest CreateTopicRequest()
    {
        return new TopicRequest(
            TopicId: Random.Shared.Next(1, 9999),
            Name: "test_topic" + Random.Shared.Next(1, 69) + Utility.RandomString(12).ToLower(),
            CompressionAlgorithm: CompressionAlgorithm.Gzip,
            MessageExpiry: (ulong)Random.Shared.Next(1, 69),
            MaxTopicSize: (ulong)Random.Shared.NextInt64(2_000_000_000, 10_000_000_000),
            ReplicationFactor: (byte)Random.Shared.Next(1, 8),
            PartitionsCount: Random.Shared.Next(5, 25));
    }

    internal static UpdateTopicRequest CreateUpdateTopicRequest()
    {
        return new UpdateTopicRequest(
            Name: "updated_topic" + Random.Shared.Next(1, 69),
            CompressionAlgorithm: CompressionAlgorithm.Gzip,
            MaxTopicSize: (ulong)Random.Shared.NextInt64(2_000_000_000, 10_000_000_000),
            MessageExpiry: (ulong)Random.Shared.Next(1, 69),
            ReplicationFactor: (byte)Random.Shared.Next(1, 8));
    }

    internal static TopicRequest CreateTopicRequest(int topicId)
    {
        return new TopicRequest(
            TopicId: topicId,
            Name: "test_topic" + Random.Shared.Next(1, 69) + Utility.RandomString(12).ToLower(),
            CompressionAlgorithm: CompressionAlgorithm.None,
            MessageExpiry: (ulong)Random.Shared.Next(1, 69),
            MaxTopicSize: (ulong)Random.Shared.NextInt64(2_000_000_000, 10_000_000_000),
            ReplicationFactor: (byte)Random.Shared.Next(1, 8),
            PartitionsCount: Random.Shared.Next(5, 25));
    }
    
    internal static TopicResponse CreateTopicsResponse()
    {
        return new TopicResponse
        {

            Id = Random.Shared.Next(1, 10),
            Name = "Test Topic" + Random.Shared.Next(1, 69),
            MessagesCount = (ulong)Random.Shared.Next(1, 10),
            MessageExpiry = (ulong)Random.Shared.Next(1, 69),
            ReplicationFactor = (byte)Random.Shared.Next(1,8),
            PartitionsCount = Random.Shared.Next(1, 10),
            Size = (ulong)Random.Shared.Next(1, 10),
            MaxTopicSize = (ulong)Random.Shared.Next(69,420),
            CreatedAt = DateTimeOffset.UtcNow,
            Partitions = new List<PartitionContract>
            {
                new PartitionContract
                {
                    MessagesCount = (ulong)Random.Shared.Next(1, 10),
                    Id = Random.Shared.Next(1, 10),
                    CurrentOffset = (ulong)Random.Shared.Next(1, 10),
                    SegmentsCount = Random.Shared.Next(1, 10),
                    Size = (ulong)Random.Shared.Next(1, 10),
                    CreatedAt = DateTimeOffset.UtcNow
                }
            }

        };
    }
}