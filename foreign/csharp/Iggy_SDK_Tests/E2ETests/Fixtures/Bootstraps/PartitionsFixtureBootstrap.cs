using Iggy_SDK;
using Iggy_SDK_Tests.Utils.Streams;
using Iggy_SDK_Tests.Utils.Topics;
using Iggy_SDK.Contracts.Http;
using Iggy_SDK.IggyClient;

namespace Iggy_SDK_Tests.E2ETests.Fixtures.Bootstraps;

public class PartitionsFixtureBootstrap : IIggyBootstrap
{
    public static readonly StreamRequest StreamRequest = StreamFactory.CreateStreamRequest();
    public static readonly TopicRequest TopicRequest = TopicFactory.CreateTopicRequest();
    public async Task BootstrapResourcesAsync(int tcpPort, int httpPort, IIggyClient httpClient, IIggyClient tcpClient)
    {
        await tcpClient.CreateStreamAsync(StreamRequest);
        await tcpClient.CreateTopicAsync(Identifier.Numeric((int)StreamRequest.StreamId!), TopicRequest);
        
        await httpClient.CreateStreamAsync(StreamRequest);
        await httpClient.CreateTopicAsync(Identifier.Numeric((int)StreamRequest.StreamId!), TopicRequest);
    }
}