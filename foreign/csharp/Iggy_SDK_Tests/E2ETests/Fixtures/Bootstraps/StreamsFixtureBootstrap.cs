using Iggy_SDK_Tests.Utils.Streams;
using Iggy_SDK.Contracts.Http;
using Iggy_SDK.IggyClient;

namespace Iggy_SDK_Tests.E2ETests.Fixtures.Bootstraps;

public class StreamsFixtureBootstrap : IIggyBootstrap
{
    public static readonly StreamRequest StreamRequest = StreamFactory.CreateStreamRequest();
    public static readonly UpdateStreamRequest UpdateStreamRequest = StreamFactory.CreateUpdateStreamRequest();
    public Task BootstrapResourcesAsync(int tcpPort, int httpPort, IIggyClient httpClient, IIggyClient tcpClient)
    {
        return Task.CompletedTask;
    }
}