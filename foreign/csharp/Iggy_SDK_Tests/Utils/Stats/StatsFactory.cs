namespace Iggy_SDK_Tests.Utils.Stats;

public static class StatsFactory
{

    public static Iggy_SDK.Contracts.Http.Stats CreateFakeStatsObject()
    {
        return new Iggy_SDK.Contracts.Http.Stats
        {
            ProcessId = 123,
            CpuUsage = 12.34f,
            MemoryUsage = 567890,
            TotalCpuUsage = 56.78f,
            TotalMemory = 1234567890,
            AvailableMemory = 987654321,
            RunTime = 1234567890,
            StartTime = DateTimeOffset.FromUnixTimeSeconds(1628718600),
            ReadBytes = 1234567890,
            WrittenBytes = 987654321,
            StreamsCount = 10,
            KernelVersion = "4.18.0-305.el8.x86_64",
            MessagesCount = 100000,
            TopicsCount = 5,
            PartitionsCount = 20,
            SegmentsCount = 50,
            OsName = "Linux",
            OsVersion = "4.18.0",
            ConsumerGroupsCount = 8,
            MessagesSizeBytes = 1234567890,
            Hostname = "localhost",
            ClientsCount = 69,
        };

    }
}