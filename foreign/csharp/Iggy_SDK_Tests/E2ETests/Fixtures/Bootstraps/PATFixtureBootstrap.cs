using Iggy_SDK.Contracts.Http;
using Iggy_SDK.IggyClient;

namespace Iggy_SDK_Tests.E2ETests.Fixtures.Bootstraps;

public class PATFixtureBootstrap : IIggyBootstrap
{
    public static readonly CreatePersonalAccessTokenRequest CreatePersonalAccessTokenRequest = new()
    {
        Name = "test",
        Expiry = 69420
    };
    public Task BootstrapResourcesAsync(int tcpPort, int httpPort, IIggyClient httpClient, IIggyClient tcpClient)
    {
        return Task.CompletedTask;
    }
}