namespace Iggy_SDK.Contracts.Http;

public sealed class Permissions
{
    public required GlobalPermissions Global { get; init; }
    public Dictionary<int, StreamPermissions>? Streams { get; init; }
}