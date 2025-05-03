using Iggy_SDK.Enums;

namespace Iggy_SDK.Kinds;

public readonly struct PollingStrategy
{
    public required MessagePolling Kind { get; init; }
    public required ulong Value { get; init; }

    public static PollingStrategy Offset(ulong value)
    {
        return new PollingStrategy
        {
            Kind = MessagePolling.Offset,
            Value = value
        };
    }
    
    public static PollingStrategy Timestamp(ulong value)
    {
        return new PollingStrategy
        {
            Kind = MessagePolling.Timestamp,
            Value = value
        };
    }

    public static PollingStrategy First()
    {
        return new PollingStrategy
        {
            Kind = MessagePolling.First,
            Value = 0
        };
    }

    public static PollingStrategy Last()
    {
        return new PollingStrategy
        {
            Kind = MessagePolling.Last,
            Value = 0
        };
    }

    public static PollingStrategy Next()
    {
        return new PollingStrategy
        {
            Kind = MessagePolling.Next,
            Value = 0
        };
    }
}