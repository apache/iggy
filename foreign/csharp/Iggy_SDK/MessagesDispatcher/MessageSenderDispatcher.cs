// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System.Buffers;
using System.Buffers.Binary;
using System.Threading.Channels;
using Apache.Iggy.Configuration;
using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Messages;
using Microsoft.Extensions.Logging;

namespace Apache.Iggy.MessagesDispatcher;

internal sealed class MessageSenderDispatcher
{
    private readonly Channel<MessageSendRequest> _channel;
    private readonly CancellationTokenSource _cts = new();
    private readonly ILogger<MessageSenderDispatcher> _logger;
    private readonly int _maxMessagesPerBatch;
    private readonly int _maxRequests;
    private readonly IMessageInvoker _messageInvoker;
    private readonly PeriodicTimer _timer;
    private Task? _timerTask;

    internal MessageSenderDispatcher(MessageBatchingSettings sendMessagesOptions, Channel<MessageSendRequest> channel,
        IMessageInvoker messageInvoker, ILoggerFactory loggerFactory)
    {
        _timer = new PeriodicTimer(sendMessagesOptions.Interval);
        _logger = loggerFactory.CreateLogger<MessageSenderDispatcher>();
        _messageInvoker = messageInvoker;
        _maxMessagesPerBatch = sendMessagesOptions.MaxMessagesPerBatch;
        _maxRequests = sendMessagesOptions.MaxRequests;
        _channel = channel;
    }

    internal void Start()
    {
        _timerTask = SendMessages();
    }

    internal async Task SendMessages()
    {
        var messagesSendRequests = new MessageSendRequest[_maxRequests];
        while (await _timer.WaitForNextTickAsync(_cts.Token))
        {
            var idx = 0;
            while (_channel.Reader.TryRead(out var msg))
            {
                messagesSendRequests[idx++] = msg;
            }

            if (idx == 0)
            {
                continue;
            }

            var canBatchMessages = CanBatchMessages(messagesSendRequests.AsSpan()[..idx]);
            if (!canBatchMessages)
            {
                for (var i = 0; i < idx; i++)
                {
                    try
                    {
                        await _messageInvoker.SendMessagesAsync(messagesSendRequests[i], _cts.Token);
                    }
                    catch
                    {
                        var partId = BinaryPrimitives.ReadInt32LittleEndian(messagesSendRequests[i].Partitioning.Value);
                        _logger.LogError(
                            "Error encountered while sending messages - Stream ID:{streamId}, Topic ID:{topicId}, Partition ID: {partitionId}",
                            messagesSendRequests[i].StreamId, messagesSendRequests[i].TopicId, partId);
                    }
                }

                continue;
            }

            MessageSendRequest[] messagesBatches = BatchMessages(messagesSendRequests.AsSpan()[..idx]);
            try
            {
                foreach (var messages in messagesBatches)
                {
                    try
                    {
                        if (messages is null)
                        {
                            break;
                        }

                        await _messageInvoker.SendMessagesAsync(messages, _cts.Token);
                    }
                    catch
                    {
                        var partId = BinaryPrimitives.ReadInt32LittleEndian(messages.Partitioning.Value);
                        _logger.LogError(
                            "Error encountered while sending messages - Stream ID:{streamId}, Topic ID:{topicId}, Partition ID: {partitionId}",
                            messages.StreamId, messages.TopicId, partId);
                    }
                }
            }
            finally
            {
                ArrayPool<MessageSendRequest?>.Shared.Return(messagesBatches);
            }
        }
    }

    private static bool CanBatchMessages(ReadOnlySpan<MessageSendRequest> requests)
    {
        for (var i = 0; i < requests.Length - 1; i++)
        {
            var start = requests[i];
            var next = requests[i + 1];

            if (!start.StreamId.Equals(next.StreamId)
                || !start.TopicId.Equals(next.TopicId)
                || start.Partitioning.Kind is not Partitioning.PartitionId
                || !start.Partitioning.Value.SequenceEqual(next.Partitioning.Value))
            {
                return false;
            }
        }

        return true;
    }

    private MessageSendRequest[] BatchMessages(Span<MessageSendRequest> requests)
    {
        var messagesCount = 0;
        for (var i = 0; i < requests.Length; i++)
        {
            messagesCount += requests[i].Messages.Count;
        }

        var batchesCount = (int)Math.Ceiling((decimal)messagesCount / _maxMessagesPerBatch);

        Message[] messagesBuffer = ArrayPool<Message>.Shared.Rent(_maxMessagesPerBatch);
        Span<Message> messages = messagesBuffer.AsSpan()[.._maxMessagesPerBatch];
        MessageSendRequest[] messagesBatchesBuffer = ArrayPool<MessageSendRequest>.Shared.Rent(batchesCount);

        var idx = 0;
        var batchCounter = 0;
        try
        {
            foreach (var request in requests)
            {
                foreach (var message in request.Messages)
                {
                    messages[idx++] = message;
                    if (idx >= _maxMessagesPerBatch)
                    {
                        var messageSendRequest = new MessageSendRequest
                        {
                            Partitioning = request.Partitioning,
                            StreamId = request.StreamId,
                            TopicId = request.TopicId,
                            Messages = messages.ToArray()
                        };
                        messagesBatchesBuffer[batchCounter] = messageSendRequest;
                        batchCounter++;
                        idx = 0;
                        messages.Clear();
                    }
                }
            }

            if (!messages.IsEmpty)
            {
                var messageSendRequest = new MessageSendRequest
                {
                    Partitioning = requests[0].Partitioning,
                    StreamId = requests[0].StreamId,
                    TopicId = requests[0].TopicId,
                    Messages = messages[..idx].ToArray()
                };
                messagesBatchesBuffer[batchCounter++] = messageSendRequest;
            }

            return messagesBatchesBuffer;
        }
        finally
        {
            ArrayPool<Message>.Shared.Return(messagesBuffer);
        }
    }

    internal async Task StopAsync()
    {
        if (_timerTask is null)
        {
            return;
        }

        _timer.Dispose();
        _cts.Cancel();
        await _timerTask;
        _cts.Dispose();
    }
}