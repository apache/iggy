using Iggy_SDK.Enums;

namespace Iggy_SDK.Kinds;

public readonly struct Consumer
{
    public required ConsumerType Type { get; init; }
    public required Identifier Id { get; init; }

    public static Consumer New(int id)
    {
        return new Consumer
        {
            Id = Identifier.Numeric(id),
            Type = ConsumerType.Consumer
        };
    }
    public static Consumer New(string id)
    {
        return new Consumer
        {
            Id = Identifier.String(id),
            Type = ConsumerType.Consumer
        };
    }

    public static Consumer Group(int id)
    {
        return new Consumer
        {
            Id = Identifier.Numeric(id),
            Type = ConsumerType.ConsumerGroup
        };
    }
    public static Consumer Group(string id)
    {
        return new Consumer
        {
            Id = Identifier.String(id),
            Type = ConsumerType.ConsumerGroup
        };
    }
}