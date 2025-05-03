namespace Iggy_SDK.Contracts.Http;

public sealed class StreamPermissions
{
    public required bool ManageStream { get; init; }
    public required bool ReadStream { get; init; }
    public required bool ManageTopics { get; init; }
    public required bool ReadTopics { get; init; }
    public required bool PollMessages { get; init; }
    public required bool SendMessages { get; init; }
    public Dictionary<int, TopicPermissions>? Topics { get; init; }
}