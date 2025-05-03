using Iggy_SDK.IggyClient;

namespace Iggy_SDK_Tests.E2ETests.Fixtures;

public interface IIggyBootstrap
{
    public Task BootstrapResourcesAsync(int tcpPort, int httpPort, IIggyClient httpClient, IIggyClient tcpClient);
}