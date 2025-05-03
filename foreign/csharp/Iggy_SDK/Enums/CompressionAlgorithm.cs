namespace Iggy_SDK.Enums;

/// <summary>
/// Supported compression algorithms.
/// 
/// OBS.: For now only those, in the future will add snappy, lz4, zstd (same as in confluent kafka)
/// in addition to that we should consider brotli as well.
/// </summary>
public enum CompressionAlgorithm
{
    // No compression
    None,
    // Gzip compression algorithm
    Gzip,
}