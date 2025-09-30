using Apache.Iggy.Encryption;
using Apache.Iggy.Enums;
using Apache.Iggy.Kinds;
using Microsoft.Extensions.Logging;

namespace Apache.Iggy.Consumers;

public class IggyConsumerConfig
{
    public bool CreateIggyClient { get; set; }
    public Protocol Protocol { get; set; }
    public string Address { get; set; } = string.Empty;
    public string Login { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
    public int ReceiveBufferSize { get; set; } = 4096;
    public int SendBufferSize { get; set; } = 4096;

    public Identifier StreamId { get; set; }
    public Identifier TopicId  { get; set; }
    public IMessageEncryptor? MessageEncryptor { get; set; } = null;
    public uint? PartitionId { get; set; }
    public Kinds.Consumer Consumer { get; set; }
    public PollingStrategy PollingStrategy { get; set; }
    public uint BatchSize { get; set; }
    public bool AutoCommit { get; set; }
    public AutoCommitMode AutoCommitMode { get; set; }
    public int BufferSize { get; set; }

    public string? ConsumerGroupName { get; set; }
    public bool CreateConsumerGroupIfNotExists { get; set; } = true;
    public bool JoinConsumerGroup { get; set; } = true;

    public int PollingIntervalMs { get; set; } = 10;

    public ILoggerFactory? LoggerFactory { get; set; }

}