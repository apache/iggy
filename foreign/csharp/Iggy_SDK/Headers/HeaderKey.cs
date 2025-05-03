namespace Iggy_SDK.Headers;

public readonly struct HeaderKey
{
    public required string Value { get; init; }

    public static HeaderKey New(string val)
    {
        return new HeaderKey
        {
            Value = val.Length is 0 or > 255
                ? throw new ArgumentException("Value has incorrect size, must be between 1 and 255", nameof(val))
                : val
        };
    }

    public override string ToString()
    {
        return Value;
    }
}