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
using Apache.Iggy.Contracts;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.MessagesDispatcher;
using Microsoft.Extensions.Logging;
using HttpMessageInvoker = Apache.Iggy.MessagesDispatcher.HttpMessageInvoker;

namespace Apache.Iggy.Factory;

internal class HttpMessageStreamBuilder
{
    private readonly HttpClient _client;
    private readonly ILoggerFactory _loggerFactory;
    private readonly MessageBatchingSettings _messageBatchingSettings;
    private readonly MessagePollingSettings _messagePollingSettings;
    private Channel<MessageSendRequest>? _channel;
    private HttpMessageInvoker? _messageInvoker;
    private MessageSenderDispatcher? _messageSenderDispatcher;

    internal HttpMessageStreamBuilder(HttpClient client, IMessageStreamConfigurator options,
        ILoggerFactory loggerFactory)
    {
        var sendMessagesOptions = new MessageBatchingSettings();
        var messagePollingOptions = new MessagePollingSettings();
        options.MessageBatchingSettings.Invoke(sendMessagesOptions);
        options.MessagePollingSettings.Invoke(messagePollingOptions);
        _messageBatchingSettings = sendMessagesOptions;
        _messagePollingSettings = messagePollingOptions;
        _client = client;
        _loggerFactory = loggerFactory;
    }

    //TODO - this channel will probably need to be refactored, to accept a lambda instead of MessageSendRequest
    internal HttpMessageStreamBuilder WithSendMessagesDispatcher()
    {
        if (_messageBatchingSettings.Enabled)
        {
            _channel = Channel.CreateBounded<MessageSendRequest>(_messageBatchingSettings.MaxRequests);
            _messageInvoker = new HttpMessageInvoker(_client);
            _messageSenderDispatcher =
                new MessageSenderDispatcher(_messageBatchingSettings, _channel, _messageInvoker, _loggerFactory);
        }
        else
        {
            _messageInvoker = new HttpMessageInvoker(_client);
        }

        return this;
    }

    internal HttpMessageStream Build()
    {
        _messageSenderDispatcher?.Start();
        return _messageBatchingSettings.Enabled switch
        {
            true => new HttpMessageStream(_client, _channel, _messagePollingSettings, _loggerFactory),
            false => new HttpMessageStream(_client, _channel, _messagePollingSettings, _loggerFactory, _messageInvoker)
        };
    }
}