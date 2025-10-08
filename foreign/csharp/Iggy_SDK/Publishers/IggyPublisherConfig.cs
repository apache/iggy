using Apache.Iggy.Encryption;
using Apache.Iggy.Enums;
using Microsoft.Extensions.Logging;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Publishers;

public class IggyPublisherConfig
{
    public bool CreateIggyClient { get; internal set; }
    public Protocol Protocol { get; internal set; }
    public string Address { get; internal set; } = string.Empty;
    public string Login { get; internal set; } = string.Empty;
    public string Password { get; internal set; } = string.Empty;
    public Identifier StreamId { get; internal set; }
    public Identifier TopicId  { get; internal set; }
    public int ReceiveBufferSize { get; internal set; } = 4096;

    public int SendBufferSize { get; internal set; } = 4096;
    public Partitioning Partitioning  { get; internal set; } = Partitioning.None();
    public bool CreateStream { get; internal set; }
    public string? StreamName { get; internal set; }
    public bool CreateTopic { get; internal set; }
    public string? TopicName { get; internal set; }
    public IMessageEncryptor? MessageEncryptor { get; internal set; } = null;
    public ILoggerFactory? LoggerFactory { get; internal set; } = null;
    
    public uint TopicPartitionsCount { get; internal set; }
    public CompressionAlgorithm TopicCompressionAlgorithm  { get; internal set; }
    public byte? TopicReplicationFactor  { get; internal set; }
    public ulong TopicMessageExpiry  { get; internal set; }
    public ulong TopicMaxTopicSize  { get; internal set; }

    public bool EnableBackgroundSending { get; internal set; } = false;
    public int BackgroundQueueCapacity { get; internal set; } = 10000;
    public int BackgroundBatchSize { get; internal set; } = 100;
    public TimeSpan BackgroundFlushInterval { get; internal set; } = TimeSpan.FromMilliseconds(100);
    
    public bool EnableRetry { get; internal set; } = true;
    public int MaxRetryAttempts { get; internal set; } = 3;
    public TimeSpan InitialRetryDelay { get; internal set; } = TimeSpan.FromMilliseconds(100);
    public TimeSpan MaxRetryDelay { get; internal set; } = TimeSpan.FromSeconds(10);
    public double RetryBackoffMultiplier { get; internal set; } = 2.0;
}
