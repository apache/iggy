using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Apache.Iggy.IggyClient;
using Microsoft.Extensions.Logging;

namespace Apache.Iggy.Consumers;

public class IggyConsumer
{
    private readonly IIggyClient _client;
    private readonly IggyConsumerConfig _config;
    private readonly ILogger<IggyConsumer> _logger;
    private bool _isInitialized;
    private readonly Channel<ReceivedMessage> _channel;

    public IggyConsumer(IIggyClient client, IggyConsumerConfig config, ILogger<IggyConsumer> logger)
    {
        _client = client;
        _config = config;
        _logger = logger;
        _channel = Channel.CreateBounded<ReceivedMessage>(new BoundedChannelOptions(config.BufferSize)
        {
            SingleReader = true,
            SingleWriter = true,
            FullMode = BoundedChannelFullMode.Wait
        });
    }
    
    public async Task InitAsync(CancellationToken ct = default)
    {
        if (_isInitialized)
        {
            return;
        }

        await _client.LoginUser(_config.Login, _config.Password, ct);

        _isInitialized = true;
    }
    
    public async IAsyncEnumerable<ReceivedMessage> ReceiveAsync([EnumeratorCancellation] CancellationToken ct = default)
    {
        if (!_isInitialized)
        {
            throw new Exception("IggyConsumer is not initialized. Call InitAsync first.");
        }

        _ = PollMessagesAsync(ct);
        
        do
        {
            var message = await _channel.Reader.ReadAsync(ct);
            if (_config.Decryptor != null)
            {
                message.Message.Payload = _config.Decryptor(message.Message.Payload);
            }

            yield return message;

            if (_config.AutoCommit || _config.AutoCommitMode != AutoCommitMode.AfterReceive)
            {
                continue;
            }

            await _client.StoreOffsetAsync(_config.Consumer, _config.StreamId, _config.TopicId,
                message.CurrentOffset, message.PartitionId, ct);

        } while (!ct.IsCancellationRequested);
    }

    private async Task PollMessagesAsync(CancellationToken ct)
    {
        do
        {
            try
            {
                var messages = await _client.PollMessagesAsync(_config.StreamId, _config.TopicId, _config.PartitionId,
                    _config.Consumer, _config.PollingStrategy, _config.BatchSize, _config.AutoCommit, _config.Decryptor,
                    ct);

                foreach (var message in messages.Messages)
                {
                    var receivedMessage = new ReceivedMessage()
                    {
                        Message = message,
                        CurrentOffset = message.Header.Offset,
                        PartitionId = (uint)messages.PartitionId
                    };
                    await _channel.Writer.WriteAsync(receivedMessage, ct);
                }

                if (messages.Messages.Any() && _config.AutoCommitMode == AutoCommitMode.AfterPoll)
                {
                    await _client.StoreOffsetAsync(_config.Consumer, _config.StreamId, _config.TopicId,
                        messages.Messages[^1].Header.Offset, (uint)messages.PartitionId, ct);
                }
            }
            //catch(IggyInvalidStatusCodeException ex)
            catch(Exception ex)
            {
                _logger.LogError(ex, "Failed to poll messages");
            }
            
            await Task.Delay(10, ct);
            
        } while (!ct.IsCancellationRequested);
    }
}