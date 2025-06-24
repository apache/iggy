using Apache.Iggy.Contracts.Http;

namespace Apache.Iggy.Tests.Integrations.Helpers;

public static class TopicFactory
{
    public static TopicRequest CreateTopic(int topicId = 1, int partitionsCount = 1)
    {
        return new TopicRequest
        {
            TopicId = topicId,
            Name = $"TestTopic {topicId}",
            PartitionsCount = partitionsCount
        };
    }
}