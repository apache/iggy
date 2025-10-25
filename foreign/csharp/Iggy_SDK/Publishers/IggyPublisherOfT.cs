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

using Apache.Iggy.Headers;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Messages;
using Microsoft.Extensions.Logging;

namespace Apache.Iggy.Publishers;

/// <summary>
///     Typed publisher that automatically serializes objects of type T to message payloads.
///     Extends <see cref="IggyPublisher" /> with serialization capabilities.
/// </summary>
/// <typeparam name="T">The type to serialize to message payloads</typeparam>
public class IggyPublisher<T> : IggyPublisher
{
    private readonly IggyPublisherConfig<T> _typedConfig;
    private readonly ILogger<IggyPublisher<T>> _typedLogger;

    /// <summary>
    ///     Initializes a new instance of the typed <see cref="IggyPublisher{T}" /> class
    /// </summary>
    /// <param name="client">The Iggy client for server communication</param>
    /// <param name="config">Typed publisher configuration including serializer</param>
    /// <param name="logger">Logger instance for diagnostic output</param>
    public IggyPublisher(IIggyClient client, IggyPublisherConfig<T> config, ILogger<IggyPublisher<T>> logger) : base(
        client, config, logger)
    {
        _typedConfig = config;
        _typedLogger = logger;
    }

    /// <summary>
    ///     Serializes and sends a single object as a message
    /// </summary>
    /// <param name="data">The object to serialize and send</param>
    /// <param name="messageId">Optional message ID. If null, a new GUID will be generated</param>
    /// <param name="userHeaders">Optional user headers to attach to the message</param>
    /// <param name="ct">Cancellation token</param>
    public async Task SendAsync(T data, Guid? messageId = null,
        Dictionary<HeaderKey, HeaderValue>? userHeaders = null, CancellationToken ct = default)
    {
        var message = CreateMessage(data, messageId, userHeaders);
        await SendMessages([message], ct);
    }

    /// <summary>
    ///     Serializes and sends a collection of objects as messages
    /// </summary>
    /// <param name="data">The collection of objects to serialize and send</param>
    /// <param name="ct">Cancellation token</param>
    public async Task SendAsync(IEnumerable<T> data, CancellationToken ct = default)
    {
        var messages = data.Select(item => CreateMessage(item, null, null)).ToList();
        await SendMessages(messages, ct);
    }

    /// <summary>
    ///     Serializes and sends a collection of objects with custom message configuration
    /// </summary>
    /// <param name="items">The collection of items to send, each with optional message ID and headers</param>
    /// <param name="ct">Cancellation token</param>
    public async Task SendAsync(IEnumerable<(T data, Guid? messageId, Dictionary<HeaderKey, HeaderValue>? userHeaders)> items,
        CancellationToken ct = default)
    {
        var messages = items.Select(item => CreateMessage(item.data, item.messageId, item.userHeaders)).ToList();
        await SendMessages(messages, ct);
    }

    /// <summary>
    ///     Serializes an object using the configured serializer
    /// </summary>
    /// <param name="data">The object to serialize</param>
    /// <returns>The serialized byte array</returns>
    public byte[] Serialize(T data)
    {
        return _typedConfig.Serializer.Serialize(data);
    }

    /// <summary>
    ///     Creates a message from an object by serializing it
    /// </summary>
    /// <param name="data">The object to serialize</param>
    /// <param name="messageId">Optional message ID</param>
    /// <param name="userHeaders">Optional user headers</param>
    /// <returns>A new message with the serialized payload</returns>
    private Message CreateMessage(T data, Guid? messageId, Dictionary<HeaderKey, HeaderValue>? userHeaders)
    {
        try
        {
            var payload = Serialize(data);
            var id = messageId ?? Guid.NewGuid();
            return new Message(id, payload, userHeaders);
        }
        catch (Exception ex)
        {
            _typedLogger.LogError(ex, "Failed to serialize message of type {Type}", typeof(T).Name);
            throw;
        }
    }
}
