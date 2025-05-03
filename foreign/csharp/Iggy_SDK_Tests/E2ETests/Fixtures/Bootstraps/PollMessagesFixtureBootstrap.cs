using Iggy_SDK;
using Iggy_SDK_Tests.Utils.DummyObj;
using Iggy_SDK_Tests.Utils.Messages;
using Iggy_SDK_Tests.Utils.Streams;
using Iggy_SDK_Tests.Utils.Topics;
using Iggy_SDK.Contracts.Http;
using Iggy_SDK.IggyClient;
using Iggy_SDK.Kinds;

namespace Iggy_SDK_Tests.E2ETests.Fixtures.Bootstraps;

public class PollMessagesFixtureBootstrap : IIggyBootstrap
{
    private static readonly StreamRequest StreamRequest = StreamFactory.CreateStreamRequest();
    private static readonly TopicRequest TopicRequest = TopicFactory.CreateTopicRequest();
    private static readonly TopicRequest HeadersTopicRequest = TopicFactory.CreateTopicRequest();
    public const int MessageCount = 100;

    public static readonly int StreamId = (int)StreamRequest.StreamId!;
    public static readonly int TopicId = (int)TopicRequest.TopicId!;
    public static readonly int HeadersTopicId = (int)HeadersTopicRequest.TopicId!;

    public const int PartitionId = 1;
    public const int HeadersCount = 3;

    public async Task BootstrapResourcesAsync(int tcpPort, int httpPort, IIggyClient httpClient, IIggyClient tcpClient)
    {
        await tcpClient.CreateStreamAsync(StreamRequest);
        await tcpClient.CreateTopicAsync(Identifier.Numeric((int)StreamRequest.StreamId!), TopicRequest);
        await tcpClient.CreateTopicAsync(Identifier.Numeric((int)StreamRequest.StreamId), HeadersTopicRequest);
        await tcpClient.SendMessagesAsync(new MessageSendRequest<DummyMessage>
            {
                Messages = MessageFactory.GenerateDummyMessages(MessageCount),
                Partitioning = Partitioning.PartitionId(PartitionId),
                StreamId = Identifier.Numeric(StreamId),
                TopicId = Identifier.Numeric(TopicId)
            },
            MessageFactory.Serializer,
            headers: MessageFactory.GenerateMessageHeaders(HeadersCount));

        await httpClient.CreateStreamAsync(StreamRequest);
        await httpClient.CreateTopicAsync(Identifier.Numeric((int)StreamRequest.StreamId!), TopicRequest);
        await httpClient.CreateTopicAsync(Identifier.Numeric((int)StreamRequest.StreamId), HeadersTopicRequest);
        await httpClient.SendMessagesAsync(new MessageSendRequest<DummyMessage>
            {
                Messages = MessageFactory.GenerateDummyMessages(MessageCount),
                Partitioning = Partitioning.PartitionId(PartitionId),
                StreamId = Identifier.Numeric(StreamId),
                TopicId = Identifier.Numeric(TopicId)
            },
            MessageFactory.Serializer,
            headers: MessageFactory.GenerateMessageHeaders(HeadersCount));
    }
}