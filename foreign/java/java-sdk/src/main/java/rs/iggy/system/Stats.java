package rs.iggy.system;

import java.math.BigInteger;

public record Stats(
        Long processId,
        Float cpuUsage,
        Float totalCpuUsage,
        String memoryUsage,
        String totalMemory,
        String availableMemory,
        BigInteger runTime,
        BigInteger startTime,
        String readBytes,
        String writtenBytes,
        String messagesSizeBytes,
        Long streamsCount,
        Long topicsCount,
        Long partitionsCount,
        Long segmentsCount,
        BigInteger messagesCount,
        Long clientsCount,
        Long consumerGroupsCount,
        String hostname,
        String osName,
        String osVersion,
        String kernelVersion
) {
}
