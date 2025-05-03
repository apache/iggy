namespace Iggy_SDK.Contracts.Http;

public sealed class Stats
{
    public required int ProcessId { get; init; }
    public required float CpuUsage { get; init; }
    public required float TotalCpuUsage { get; init; }
    public required ulong MemoryUsage { get; init; }
    public required ulong TotalMemory { get; init; }
    public required ulong AvailableMemory { get; init; }
    public required ulong RunTime { get; init; }
    public required DateTimeOffset StartTime { get; init; }
    public required ulong ReadBytes { get; init; }
    public required ulong WrittenBytes { get; init; }
    public required ulong MessagesSizeBytes { get; init; }
    public required int StreamsCount { get; init; }
    public required int TopicsCount { get; init; }
    public required int PartitionsCount { get; init; }
    public required int SegmentsCount { get; init; }
    public required ulong MessagesCount { get; init; }
    public required int ClientsCount { get; init; }
    public required int ConsumerGroupsCount { get; init; }
    public required string Hostname { get; init; }
    public required string OsName { get; init; }
    public required string OsVersion { get; init; }
    public required string KernelVersion { get; init; }

}