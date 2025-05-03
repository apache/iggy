using Iggy_SDK.Contracts.Http;

namespace Iggy_SDK_Tests.Utils.Streams;

internal static class StreamFactory
{
    internal static (int id, int topicsCount, ulong sizeBytes, ulong messagesCount, string name, ulong createdAt)
        CreateStreamsResponseFields()
    {
        int id = Random.Shared.Next(1, 69);
        var topicsCount = Random.Shared.Next(1, 69);
        var sizeBytes = (ulong)Random.Shared.Next(69, 42069);
        var messageCount = (ulong)Random.Shared.Next(2, 3);
        var name = "Stream " + Random.Shared.Next(1, 4) + Utility.RandomString(3).ToLower();
        var createdAt = (ulong)Random.Shared.Next(69, 42069);
        return (id, topicsCount, sizeBytes, messageCount, name, createdAt);
    }
    internal static StreamRequest CreateStreamRequest()
    {
        return new StreamRequest
        {
            Name = "test-stream" + Random.Shared.Next(1, 69) + Utility.RandomString(12).ToLower(),
            StreamId = Random.Shared.Next(1, 2137),
        };
    }

    internal static UpdateStreamRequest CreateUpdateStreamRequest()
    {
        return new UpdateStreamRequest { Name = "updated-stream" + Random.Shared.Next(1, 69) };
    }
    internal static StreamResponse CreateStreamsResponse()
    {
        return new StreamResponse
        {
            Id = Random.Shared.Next(1, 10),
            Size = (ulong)Random.Shared.Next(1, 10),
            MessagesCount = (ulong)Random.Shared.Next(1, 10),
            CreatedAt = DateTimeOffset.UtcNow,
            Name = "Test Topic" + Random.Shared.Next(1, 69),
            TopicsCount = Random.Shared.Next(1, 10),
        };
    }
}