using Iggy_SDK_Tests.E2ETests.Fixtures.Bootstraps;
using Iggy_SDK_Tests.E2ETests.Fixtures.Configs;

namespace Iggy_SDK_Tests.E2ETests.Fixtures;

public sealed class IggyFetchMessagesFixture : IggyBaseFixture
{
    public IggyFetchMessagesFixture() : base(new FetchMessagesFixtureBootstrap(),
        IggyFixtureClientMessagingSettings.PollingSettings, IggyFixtureClientMessagingSettings.BatchingSettingsFetchFixture)
    {
    }
}