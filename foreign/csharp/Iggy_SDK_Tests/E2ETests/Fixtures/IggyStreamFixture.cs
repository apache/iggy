using Iggy_SDK_Tests.E2ETests.Fixtures.Bootstraps;
using Iggy_SDK_Tests.E2ETests.Fixtures.Configs;

namespace Iggy_SDK_Tests.E2ETests.Fixtures;

public sealed class IggyStreamFixture : IggyBaseFixture
{
    public IggyStreamFixture() : base(new StreamsFixtureBootstrap(), 
        IggyFixtureClientMessagingSettings.PollingSettings,
        IggyFixtureClientMessagingSettings.BatchingSettings)
    {
    }
}