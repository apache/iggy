namespace Iggy_SDK.Messages;

public readonly struct MessageHeader
{
    public ulong Checksum { get; init; }
    public UInt128 Id { get; init; }
    public ulong Offset { get; init; }
    public DateTimeOffset Timestamp { get; init; }
    public DateTimeOffset OriginTimestamp { get; init; }
    public int UserHeadersLength { get; init; }
    public int PayloadLength { get; init; }
}