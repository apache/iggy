using Iggy_SDK.Enums;
namespace Iggy_SDK.Configuration;

public sealed class MessagePollingSettings
{
    public TimeSpan Interval { get; set; }
    public StoreOffset StoreOffsetStrategy { get; set; }
}