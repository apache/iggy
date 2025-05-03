namespace Iggy_SDK.Contracts.Http;

public sealed class TopicPermissions
{
    public required bool ManageTopic { get; init; }
    public required bool ReadTopic { get; init; }
    public required bool PollMessages { get; init; }
    public required bool SendMessages { get; init; }
}