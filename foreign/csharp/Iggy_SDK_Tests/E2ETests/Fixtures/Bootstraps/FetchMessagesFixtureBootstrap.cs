using Iggy_SDK;
using Iggy_SDK_Tests.Utils.Messages;
using Iggy_SDK_Tests.Utils.Streams;
using Iggy_SDK_Tests.Utils.Topics;
using Iggy_SDK.Contracts.Http;
using Iggy_SDK.IggyClient;

namespace Iggy_SDK_Tests.E2ETests.Fixtures.Bootstraps;

public class FetchMessagesFixtureBootstrap : IIggyBootstrap
{
    private static readonly StreamRequest StreamRequest = StreamFactory.CreateStreamRequest();
    private static readonly StreamRequest NonExistingStreamRequest = StreamFactory.CreateStreamRequest();
    private static readonly TopicRequest NonExistingTopicRequest = TopicFactory.CreateTopicRequest(3000);
    private static readonly TopicRequest TopicRequest = TopicFactory.CreateTopicRequest();
    private static readonly TopicRequest HeadersTopicRequest = TopicFactory.CreateTopicRequest();

    public static readonly int StreamId = (int)StreamRequest.StreamId!;
    public static readonly int TopicId = (int)TopicRequest.TopicId!;
    public static readonly int HeadersTopicId = (int)HeadersTopicRequest.TopicId!;

    public static readonly int InvalidStreamId = (int)NonExistingStreamRequest.StreamId!;
    public static readonly int InvalidTopicId = (int)NonExistingTopicRequest.TopicId!;
    public const int PartitionId = 1;

    public async Task BootstrapResourcesAsync(int tcpPort, int httpPort, IIggyClient httpClient, IIggyClient tcpClient)
    {
        await tcpClient.CreateStreamAsync(StreamRequest);
        await tcpClient.CreateTopicAsync(Identifier.Numeric((int)StreamRequest.StreamId!), TopicRequest);
        await tcpClient.CreateTopicAsync(Identifier.Numeric((int)StreamRequest.StreamId), HeadersTopicRequest);

        var request = MessageFactory.CreateMessageSendRequest(
            (int)StreamRequest.StreamId, (int)TopicRequest.TopicId!, PartitionId,
            MessageFactory.GenerateMessages(20));
        var requestWithHeaders = MessageFactory.CreateMessageSendRequest(
            (int)StreamRequest.StreamId, (int)HeadersTopicRequest.TopicId!, PartitionId,
            MessageFactory.GenerateMessages(20, MessageFactory.GenerateMessageHeaders(3)));
        await tcpClient.SendMessagesAsync(request);
        await tcpClient.SendMessagesAsync(requestWithHeaders);
        await Task.Delay(1000);
        
        await httpClient.CreateStreamAsync(StreamRequest);
        await httpClient.CreateTopicAsync(Identifier.Numeric((int)StreamRequest.StreamId!), TopicRequest);
        await httpClient.CreateTopicAsync(Identifier.Numeric((int)StreamRequest.StreamId), HeadersTopicRequest);

        await httpClient.SendMessagesAsync(request);
        await httpClient.SendMessagesAsync(requestWithHeaders);
        await Task.Delay(1000);
    }
}