using Iggy_SDK.Contracts.Http;
using Iggy_SDK.Enums;

namespace Iggy_SDK_Tests.Utils.Topics;

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
        ulong maxTopicSize = (ulong)Random.Shared.NextInt64(1_000_000_000, 10_000_000_000);
        byte replicationFactor = (byte)Random.Shared.Next(1, 8);
        return (topicId, partitionsCount, topicName, messageExpiry, sizeBytes, messagesCount, createdAt, replicationFactor, maxTopicSize);
    }
    
    internal static TopicRequest CreateTopicRequest()
    {
        return new TopicRequest(
            TopicId: Random.Shared.Next(1, 9999),
            Name: "test_topic" + Random.Shared.Next(1, 69) + Utility.RandomString(12).ToLower(),
            CompressionAlgorithm: CompressionAlgorithm.None,
            MessageExpiry: Random.Shared.Next(1, 69),
            MaxTopicSize: (ulong)Random.Shared.NextInt64(1_000_000_000, 10_000_000_000),
            ReplicationFactor: (byte)Random.Shared.Next(1, 8),
            PartitionsCount: Random.Shared.Next(5, 25));
    }

    internal static UpdateTopicRequest CreateUpdateTopicRequest()
    {
        return new UpdateTopicRequest(
            Name: "updated_topic" + Random.Shared.Next(1, 69),
            CompressionAlgorithm: CompressionAlgorithm.None,
            MaxTopicSize: (ulong)Random.Shared.NextInt64(1_000_000_000, 10_000_000_000),
            MessageExpiry: Random.Shared.Next(1, 69),
            ReplicationFactor: (byte)Random.Shared.Next(1,8));
    }

    internal static TopicRequest CreateTopicRequest(int topicId)
    {
        return new TopicRequest(
            TopicId: topicId,
            Name: "test_topic" + Random.Shared.Next(1, 69) + Utility.RandomString(12).ToLower(),
            CompressionAlgorithm: CompressionAlgorithm.None,
            MessageExpiry: Random.Shared.Next(1, 69),
            MaxTopicSize: (ulong)Random.Shared.NextInt64(1_000_000_000, 10_000_000_000),
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
            MessageExpiry = Random.Shared.Next(1, 69),
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