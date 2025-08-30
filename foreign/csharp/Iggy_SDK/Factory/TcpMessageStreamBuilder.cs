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

using System.Threading.Channels;
using Apache.Iggy.Configuration;
using Apache.Iggy.ConnectionStream;
using Apache.Iggy.Contracts;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.MessagesDispatcher;
using Microsoft.Extensions.Logging;

namespace Apache.Iggy.Factory;

internal class TcpMessageStreamBuilder
{
    private readonly ILoggerFactory _loggerFactory;
    private readonly MessageBatchingSettings _messageBatchingOptions;
    private readonly MessagePollingSettings _messagePollingSettings;
    private readonly IConnectionStream _stream;
    private Channel<MessageSendRequest>? _channel;
    private TcpMessageInvoker? _messageInvoker;
    private MessageSenderDispatcher? _messageSenderDispatcher;

    internal TcpMessageStreamBuilder(IConnectionStream stream, IMessageStreamConfigurator options,
        ILoggerFactory loggerFactory)
    {
        var sendMessagesOptions = new MessageBatchingSettings();
        var messagePollingOptions = new MessagePollingSettings();
        options.MessagePollingSettings.Invoke(messagePollingOptions);
        options.MessageBatchingSettings.Invoke(sendMessagesOptions);
        _messageBatchingOptions = sendMessagesOptions;
        _messagePollingSettings = messagePollingOptions;
        _stream = stream;
        _loggerFactory = loggerFactory;
    }

    //TODO - this channel will probably need to be refactored, to accept a lambda instead of MessageSendRequest
    internal TcpMessageStreamBuilder WithSendMessagesDispatcher()
    {
        if (_messageBatchingOptions.Enabled)
        {
            _channel = Channel.CreateBounded<MessageSendRequest>(_messageBatchingOptions.MaxRequests);
            _messageInvoker = new TcpMessageInvoker(_stream);
            _messageSenderDispatcher =
                new MessageSenderDispatcher(_messageBatchingOptions, _channel, _messageInvoker, _loggerFactory);
        }
        else
        {
            _messageInvoker = new TcpMessageInvoker(_stream);
        }

        return this;
    }

    internal TcpMessageStream Build()
    {
        _messageSenderDispatcher?.Start();
        return _messageBatchingOptions.Enabled switch
        {
            true => new TcpMessageStream(_stream, _channel, _messagePollingSettings, _loggerFactory),
            false => new TcpMessageStream(_stream, _channel, _messagePollingSettings, _loggerFactory, _messageInvoker)
        };
    }
}