using Iggy_SDK_Tests.E2ETests.Fixtures.Bootstraps;
using Iggy_SDK_Tests.E2ETests.Fixtures.Configs;

namespace Iggy_SDK_Tests.E2ETests.Fixtures;

public sealed class IggyClientsFixture : IggyBaseFixture 
{
    public IggyClientsFixture() : base(new ClientsFixtureBootstrap(),
        IggyFixtureClientMessagingSettings.PollingSettings, IggyFixtureClientMessagingSettings.BatchingSettings)
    {
    }
}