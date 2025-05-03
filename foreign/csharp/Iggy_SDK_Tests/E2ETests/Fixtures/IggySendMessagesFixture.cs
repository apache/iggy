using Iggy_SDK_Tests.E2ETests.Fixtures.Bootstraps;
using Iggy_SDK_Tests.E2ETests.Fixtures.Configs;

namespace Iggy_SDK_Tests.E2ETests.Fixtures;

public sealed class IggySendMessagesFixture : IggyBaseFixture
{
    public IggySendMessagesFixture() : base(new SendMessagesFixtureBootstrap(),
        IggyFixtureClientMessagingSettings.PollingSettings,
        IggyFixtureClientMessagingSettings.BatchingSettingsSendFixture)
    {
    }
}