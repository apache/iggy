namespace Iggy_SDK.Configuration;

public sealed class MessageBatchingSettings
{
    public bool Enabled { get; set; } = true;
    public TimeSpan Interval { get; set; } = TimeSpan.FromMilliseconds(150);
    public int MaxMessagesPerBatch { get; set; } = 1000;
    /// <summary>
    /// Defines maximum number of requests in interval 
    /// Used mainly to avoid flooding the channel (default value 2056).
    /// </summary>
    public int MaxRequests { get; set; } = 2056;
}