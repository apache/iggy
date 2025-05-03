using Iggy_SDK.Enums;

namespace Iggy_SDK.Configuration;

public sealed class MessageStreamConfigurator : IMessageStreamConfigurator
{
    public string BaseAdress { get; set; } = "http://127.0.0.1:3000";
    public Protocol Protocol { get; set; } = Protocol.Http;
    public Action<MessagePollingSettings> MessagePollingSettings { get; set; } = options =>
    {
        options.Interval = TimeSpan.FromMilliseconds(100);
        options.StoreOffsetStrategy = StoreOffset.WhenMessagesAreReceived;
    };

    public Action<TlsSettings> TlsSettings { get; set; } = options =>
    {
        options.Enabled = false;
        options.Hostname = default!;
        options.Authenticate = false;
    };

    public Action<MessageBatchingSettings> MessageBatchingSettings { get; set; } = options =>
    {
        options.Interval = TimeSpan.FromMilliseconds(100);
        options.MaxMessagesPerBatch = 1000;
        options.MaxRequests = 4096;
    };
    public int ReceiveBufferSize { get; set; } = 4096;
    public int SendBufferSize { get; set; } = 4096;
}