using Iggy_SDK.Contracts.Http;
using Iggy_SDK.Contracts.Http.Auth;
using Iggy_SDK.Enums;
using Iggy_SDK.Factory;
using Iggy_SDK.IggyClient;
using Microsoft.Extensions.Logging.Abstractions;

namespace Iggy_SDK_Tests.E2ETests.Fixtures.Bootstraps;

public class ClientsFixtureBootstrap : IIggyBootstrap
{
    private const int FRESH_CLIENTS_COUNT = 6;
    
    public const int TotalClientsCount = FRESH_CLIENTS_COUNT + 1;

    public ClientsFixtureBootstrap()
    {
    }
    
    public async Task BootstrapResourcesAsync(int tcpPort, int httpPort, IIggyClient httpClient, IIggyClient tcpClient)
    {
        for (int i = 0; i < FRESH_CLIENTS_COUNT; i++)
        {
            var client = MessageStreamFactory.CreateMessageStream(options =>
            {
                options.BaseAdress = $"127.0.0.1:{tcpPort}";
                options.Protocol = Protocol.Tcp;
                options.MessageBatchingSettings = x =>
                {
                    x.MaxMessagesPerBatch = 1000;
                    x.Interval = TimeSpan.FromMilliseconds(100);
                };
            }, NullLoggerFactory.Instance);
            await client.LoginUser(new LoginUserRequest
            {
                Password = "iggy",
                Username = "iggy"
            });
        }
    }
}