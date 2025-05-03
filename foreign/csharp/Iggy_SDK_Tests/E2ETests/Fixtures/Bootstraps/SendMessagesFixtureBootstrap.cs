using Iggy_SDK;
using Iggy_SDK_Tests.Utils.Streams;
using Iggy_SDK_Tests.Utils.Topics;
using Iggy_SDK.Contracts.Http;
using Iggy_SDK.IggyClient;

namespace Iggy_SDK_Tests.E2ETests.Fixtures.Bootstraps;

public class SendMessagesFixtureBootstrap : IIggyBootstrap
{
    private static readonly StreamRequest StreamRequest = StreamFactory.CreateStreamRequest();
    private static readonly StreamRequest NonExistingStreamRequest = StreamFactory.CreateStreamRequest();
    private static readonly TopicRequest NonExistingTopicRequest = TopicFactory.CreateTopicRequest();
    private static readonly TopicRequest TopicRequest = TopicFactory.CreateTopicRequest();

    public static readonly int StreamId = (int)StreamRequest.StreamId!;
    public static readonly int TopicId = (int)TopicRequest.TopicId!;

    public static readonly int InvalidStreamId = (int)NonExistingStreamRequest.StreamId!;
    public static readonly int InvalidTopicId = (int)NonExistingTopicRequest.TopicId!;

    public const int PartitionId = 1;
    public async Task BootstrapResourcesAsync(int tcpPort, int httpPort, IIggyClient httpClient, IIggyClient tcpClient)
    {
        await tcpClient.CreateStreamAsync(StreamRequest);
        await tcpClient.CreateTopicAsync(Identifier.Numeric((int)StreamRequest.StreamId!), TopicRequest);
        
        await httpClient.CreateStreamAsync(StreamRequest);
        await httpClient.CreateTopicAsync(Identifier.Numeric((int)StreamRequest.StreamId!), TopicRequest);
    }
}