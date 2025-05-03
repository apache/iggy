namespace Iggy_SDK.Contracts.Http;

public sealed class GlobalPermissions
{
    public required bool ManageServers { get; init; }
    public required bool ReadServers { get; init; }
    public required bool ManageUsers { get; init; }
    public required bool ReadUsers { get; init; }
    public required bool ManageStreams { get; init; }
    public required bool ReadStreams { get; init; }
    public required bool ManageTopics { get; init; }
    public required bool ReadTopics { get; init; }
    public required bool PollMessages { get; init; }
    public required bool SendMessages { get; init; }
}