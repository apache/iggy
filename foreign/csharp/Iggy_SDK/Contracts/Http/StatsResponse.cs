
namespace Iggy_SDK.Contracts.Http;

public sealed class StatsResponse
{
    public required int ProcessId { get; init; }
    public required float CpuUsage { get; init; }
    public required float TotalCpuUsage { get; init; }
    public required ulong MemoryUsage { get; init; }
    public required ulong TotalMemory { get; init; }
    public required ulong AvailableMemory { get; init; }
    public required ulong RunTime { get; init; }
    public required ulong StartTime { get; init; }
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

    internal Stats ToStats()
    {
        return new Stats
        {
            ProcessId = ProcessId,
            CpuUsage = CpuUsage,
            TotalCpuUsage = TotalCpuUsage,
            MemoryUsage = MemoryUsage,
            TotalMemory = TotalMemory,
            AvailableMemory = AvailableMemory,
            RunTime = RunTime,
            StartTime = DateTimeOffset.FromUnixTimeSeconds((long)StartTime),
            ReadBytes = ReadBytes,
            WrittenBytes = WrittenBytes,
            MessagesSizeBytes = MessagesSizeBytes,
            StreamsCount = StreamsCount,
            TopicsCount = TopicsCount,
            PartitionsCount = PartitionsCount,
            SegmentsCount = SegmentsCount,
            MessagesCount = MessagesCount,
            ClientsCount = ClientsCount,
            ConsumerGroupsCount = ConsumerGroupsCount,
            Hostname = Hostname,
            OsName = OsName,
            OsVersion = OsVersion,
            KernelVersion = KernelVersion
        };
    }

}