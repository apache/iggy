using Iggy_SDK.Configuration;
using Iggy_SDK.Enums;

namespace Iggy_SDK_Tests.E2ETests.Fixtures.Configs;

public static class IggyFixtureClientMessagingSettings
{
    public static Action<MessagePollingSettings> PollingSettings { get; set; } = options =>
    {
        options.Interval = TimeSpan.FromMilliseconds(100);
        options.StoreOffsetStrategy = StoreOffset.WhenMessagesAreReceived;
    };
    public static Action<MessageBatchingSettings> BatchingSettings { get; set; } = options =>
    {
        options.Interval = TimeSpan.FromMilliseconds(100);
        options.MaxMessagesPerBatch = 1000;
        options.MaxRequests = 4096;
    };
    
    public static Action<MessageBatchingSettings> BatchingSettingsSendFixture { get; set; } = options =>
    {
        options.Enabled = false;
        options.Interval = TimeSpan.FromMilliseconds(100);
        options.MaxMessagesPerBatch = 1000;
        options.MaxRequests = 4096;
    };

    public static Action<MessageBatchingSettings> BatchingSettingsFetchFixture { get; set; } = options =>
    {
        options.Interval = TimeSpan.FromMilliseconds(100);
        options.MaxMessagesPerBatch = 1000;
        options.MaxRequests = 8912;
    };

    public static Action<MessageBatchingSettings> BatchingSettingsPollMessagesFixture { get; set; } = options =>
    {
        options.Enabled = true;
        options.Interval = TimeSpan.FromMilliseconds(99);
        options.MaxMessagesPerBatch = 1000;
        options.MaxRequests = 8912;
    };
}