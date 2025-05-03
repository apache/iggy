using Iggy_SDK_Tests.E2ETests.Fixtures.Bootstraps;
using Iggy_SDK_Tests.E2ETests.Fixtures.Configs;

namespace Iggy_SDK_Tests.E2ETests.Fixtures;

public sealed class IggyPATFixture : IggyBaseFixture
{
    public IggyPATFixture() : base(new PATFixtureBootstrap(),
        IggyFixtureClientMessagingSettings.PollingSettings,
        IggyFixtureClientMessagingSettings.BatchingSettings)
    {
    }
}