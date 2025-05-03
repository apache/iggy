using Iggy_SDK.Enums;

namespace Iggy_SDK.Contracts.Http;

public record TopicRequest(
    int? TopicId, // If not provided, the Iggy server will generate one automatically
    string Name,
    CompressionAlgorithm CompressionAlgorithm,
    int MessageExpiry,
    int PartitionsCount,
    byte ReplicationFactor,
    ulong MaxTopicSize);