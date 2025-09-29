using Apache.Iggy.Enums;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Publishers;

public class IggyPublisherConfig
{
    public Protocol Protocol { get; set; }
    public string Address { get; set; } = string.Empty;
    public string Login { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
    public Identifier StreamId { get; set; } 
    public Identifier TopicId  { get; set; }
    public int ReceiveBufferSize { get; set; } = 4096;

    public int SendBufferSize { get; set; } = 4096;
    public Partitioning Partitioning  { get; set; }
    public bool CreateStream { get; set; }
    public string? StreamName { get; set; }
    public bool TopicStream { get; set; }
    public string? TopicName { get; set; }
    public Func<byte[], byte[]>? Encryptor { get; set; } = null;
    
    public uint TopicPartitionsCount { get; set; }
    public CompressionAlgorithm TopicCompressionAlgorithm  { get; set; }
    public byte? TopicReplicationFactor  { get; set; }
    public ulong TopicMessageExpiry  { get; set; }
    public ulong TopicMaxTopicSize  { get; set; }

    public bool EnableBackgroundSending { get; set; } = false;
    public int BackgroundQueueCapacity { get; set; } = 10000;
    public int BackgroundBatchSize { get; set; } = 100;
    public TimeSpan BackgroundFlushInterval { get; set; } = TimeSpan.FromMilliseconds(100);
}
