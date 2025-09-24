using Apache.Iggy.Enums;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Messages;

namespace Apache.Iggy.Publishers;

public class IggyPublisher
{
    private readonly IIggyClient _client;
    private readonly IggyPublisherConfig _config;
    private bool _isInitialized = false;

    public IggyPublisher(IIggyClient client, IggyPublisherConfig config)
    {
        _client = client;
        _config = config;
    }

    public async Task InitAsync(CancellationToken ct = default)
    {
        if (_isInitialized)
        {
            return;
        }

        await _client.LoginUser(_config.Login, _config.Password, ct);
        await CreateStreamIfNeeded(ct);
        await CreateTopicIfNeeded(ct);

        _isInitialized = true;
    }

    private async Task CreateStreamIfNeeded(CancellationToken ct)
    {
        if (await _client.GetStreamByIdAsync(_config.StreamId, ct) != null)
        {
            return;
        }

        if (!_config.CreateStream || string.IsNullOrEmpty(_config.StreamName))
        {
            throw new Exception($"Stream {_config.StreamId} not exists");
        }
        
        if (_config.StreamId.Kind is IdKind.String)
        {
            await _client.CreateStreamAsync(_config.StreamId.GetString(), null, ct);    
        }
        else
        {
            await _client.CreateStreamAsync(_config.StreamName, _config.StreamId.GetUInt32(), ct);
        }
    }

    private async Task CreateTopicIfNeeded(CancellationToken ct)
    {
        if (await _client.GetTopicByIdAsync(_config.StreamId, _config.TopicId, ct) != null)
        {
            return;
        }

        if (!_config.TopicStream || string.IsNullOrEmpty(_config.TopicName))
        {
            throw new Exception($"Topic {_config.TopicId} not exists");
        }
        
        if (_config.TopicId.Kind is IdKind.String)
        {
            await _client.CreateTopicAsync(_config.StreamId, _config.TopicId.GetString(),
                _config.TopicPartitionsCount, _config.TopicCompressionAlgorithm, null,
                _config.TopicReplicationFactor, _config.TopicMessageExpiry, _config.TopicMaxTopicSize, ct);
        }
        else
        {
            await _client.CreateTopicAsync(_config.StreamId, _config.TopicName, _config.TopicPartitionsCount,
                _config.TopicCompressionAlgorithm, _config.TopicId.GetUInt32(), _config.TopicReplicationFactor,
                _config.TopicMessageExpiry, _config.TopicMaxTopicSize, ct);
        }
    }

    public async Task SendMessage(Message message)
    {
        EncryptMessage(message);
        
        await _client.SendMessagesAsync(_config.StreamId, _config.TopicId, _config.Partitioning, [message]);
    }

    private void EncryptMessage(Message message)
    {
        if (_config.Encryptor == null)
        {
            return;
        }
        
        message.Payload = _config.Encryptor(message.Payload);
        var header = message.Header;
        header.PayloadLength = message.Payload.Length;
    }
}