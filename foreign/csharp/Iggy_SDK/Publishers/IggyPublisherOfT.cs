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

using Apache.Iggy.Exceptions;
using Apache.Iggy.Extensions;
using Apache.Iggy.Headers;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Messages;
using Microsoft.Extensions.Logging;

namespace Apache.Iggy.Publishers;

/// <summary>
///     Typed publisher that serializes objects of type <typeparamref name="T" /> to message payloads through the
///     configured <see cref="ISerializer{T}" />. With background sending enabled, values are queued and serialized
///     at flush time by the <see cref="BackgroundMessageProcessor" />.
/// </summary>
/// <typeparam name="T">The type to serialize to message payloads</typeparam>
public class IggyPublisher<T> : IggyPublisher
{
    private readonly ISerializer<T> _serializer;

    /// <summary>
    ///     Initializes a new instance of the typed <see cref="IggyPublisher{T}" /> class
    /// </summary>
    /// <param name="client">The Iggy client for server communication</param>
    /// <param name="config">Typed publisher configuration including serializer</param>
    /// <param name="logger">Logger instance for diagnostic output</param>
    public IggyPublisher(IIggyClient client, IggyPublisherConfig<T> config, ILogger<IggyPublisher<T>> logger)
        : base(client, config, logger)
    {
        _serializer = config.Serializer;
    }

    /// <summary>
    ///     Serializes and sends a single object as a message. With background sending enabled the value is queued
    ///     and serialized at flush time; otherwise it is serialized into a pooled buffer and sent immediately.
    /// </summary>
    /// <remarks>
    ///     With background sending enabled, <paramref name="data" /> is serialized later, at flush time - not when
    ///     this method returns. If <typeparamref name="T" /> is a mutable reference type, mutating the instance after
    ///     the call (or returning it to a pool) before the flush changes what is sent. Pass an immutable value or a
    ///     snapshot, or call <see cref="IggyPublisher.WaitUntilAllSendsAsync" /> before reusing it.
    /// </remarks>
    /// <param name="data">The object to serialize and send</param>
    /// <param name="messageId">Optional message ID. If null, the message is sent with ID 0 and the server assigns one</param>
    /// <param name="userHeaders">Optional user headers to attach to the message</param>
    /// <param name="ct">Cancellation token</param>
    public async Task SendAsync(T data, Guid? messageId = null,
        Dictionary<HeaderKey, HeaderValue>? userHeaders = null, CancellationToken ct = default)
    {
        EnsureInitialized();

        var id = messageId?.ToUInt128() ?? UInt128.Zero;

        if (BackgroundProcessor != null)
        {
            await BackgroundProcessor.EnqueueAsync(TypedUnit<T>.Single(data, id, userHeaders, _serializer), ct);
            return;
        }

        var writer = new PooledBufferWriter();
        try
        {
            _serializer.Serialize(data, writer);
            var message = new Message(id, writer.Written, userHeaders);
            await Client.SendMessagesAsync(Config.StreamId, Config.TopicId, Config.Partitioning, message, ct);
        }
        finally
        {
            writer.Dispose();
        }
    }

    /// <summary>
    ///     Serializes and sends a collection of objects as messages
    /// </summary>
    /// <param name="data">The collection of objects to serialize and send</param>
    /// <param name="ct">Cancellation token</param>
    public async Task SendAsync(IEnumerable<T> data, CancellationToken ct = default)
    {
        EnsureInitialized();

        if (BackgroundProcessor != null)
        {
            TypedUnit<T> unit = TypedUnit<T>.Batch(data, _serializer);
            if (unit.Count > 0)
            {
                await BackgroundProcessor.EnqueueAsync(unit, ct);
            }

            return;
        }

        using var builder = new RentedMessageBatchBuilder();
        foreach (var item in data)
        {
            builder.Add((Data: item, Serializer: _serializer),
                static (state, writer) => state.Serializer.Serialize(state.Data, writer));
        }

        await SendDirectBatchAsync(builder, ct);
    }

    /// <summary>
    ///     Serializes and sends a collection of objects with custom message configuration
    /// </summary>
    /// <param name="items">The collection of items to send, each with optional message ID and headers</param>
    /// <param name="ct">Cancellation token</param>
    public async Task SendAsync(
        IEnumerable<(T data, Guid? messageId, Dictionary<HeaderKey, HeaderValue>? userHeaders)> items,
        CancellationToken ct = default)
    {
        EnsureInitialized();

        if (BackgroundProcessor != null)
        {
            TypedUnit<T> unit = TypedUnit<T>.Batch(items, _serializer);
            if (unit.Count > 0)
            {
                await BackgroundProcessor.EnqueueAsync(unit, ct);
            }

            return;
        }

        using var builder = new RentedMessageBatchBuilder();
        foreach ((T data, Guid? messageId, Dictionary<HeaderKey, HeaderValue>? userHeaders) item in items)
        {
            builder.Add((Data: item.data, Serializer: _serializer),
                static (state, writer) => state.Serializer.Serialize(state.Data, writer), item.messageId,
                item.userHeaders);
        }

        await SendDirectBatchAsync(builder, ct);
    }

    private async Task SendDirectBatchAsync(RentedMessageBatchBuilder builder, CancellationToken ct)
    {
        var batch = builder.Build();
        try
        {
            IList<Message> messages = batch.Messages;
            if (messages.Count == 0)
            {
                return;
            }

            await Client.SendMessagesAsync(Config.StreamId, Config.TopicId, Config.Partitioning, messages, ct);
        }
        finally
        {
            batch.Dispose();
        }
    }

    private void EnsureInitialized()
    {
        if (!IsInitialized)
        {
            throw new PublisherNotInitializedException();
        }
    }
}
