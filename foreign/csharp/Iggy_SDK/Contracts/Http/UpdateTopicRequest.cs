using Iggy_SDK.Enums;

namespace Iggy_SDK.Contracts.Http;

public record UpdateTopicRequest(
    string Name,
    CompressionAlgorithm CompressionAlgorithm,
    ulong MaxTopicSize,
    int MessageExpiry,
    byte ReplicationFactor);