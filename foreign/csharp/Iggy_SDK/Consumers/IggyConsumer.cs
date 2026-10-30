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

using System.Runtime.CompilerServices;
using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Extensions;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Apache.Iggy.Utils;
using Microsoft.Extensions.Logging;

namespace Apache.Iggy.Consumers;

/// <summary>
///     Hands over the messages of a topic one at a time, on a <see cref="PartitionReader" />: a standalone consumer
///     reads the partitions of its configuration, a consumer group reads the assignment of its member.
/// </summary>
/// <remarks>
///     <para>
///         <see cref="IggyConsumerConfig.PollOptions" /> holds the <see cref="CommitPolicy" />. With
///         <see cref="CommitMode.Manual" /> the application commits <see cref="ReceivedMessage.Checkpoint" /> with
///         <see cref="CommitAsync" />. With <see cref="CommitMode.OnRead" /> the server stores the offset of each read,
///         and <see cref="ShutdownAsync" /> moves it back to the last message handed over. With
///         <see cref="CommitMode.Auto" /> the consumer counts each message, with <see cref="CommitAfter.Delivery" />
///         when it is handed over, with <see cref="CommitAfter.Processing" /> once the stream is read again, or once
///         the handler of <see cref="ConsumeAsync" /> returns with success, and with <see cref="CommitAfter.Marked" />
///         on <see cref="Mark" />. It stores the latest counted position at the interval or the event of the policy,
///         before a rebalance takes a partition away, and on shutdown.
///     </para>
///     <para>
///         A read error does not end the stream: the reader retries by itself, and the error reaches the callbacks of
///         <see cref="SubscribeToErrorEvents" />.
///     </para>
/// </remarks>
public partial class IggyConsumer : IAsyncDisposable
{
    private const int ConsumerGroupAlreadyExists = 5004;
    private const int StateReading = 1;
    private const int StateShutDown = 2;

    private readonly IIggyClient _client;
    private readonly IggyConsumerConfig _config;
    private readonly EventAggregator<ConsumerErrorEventArgs> _consumerErrorEvents;
    private readonly SemaphoreSlim _initialization = new(1, 1);
    private readonly ILogger<IggyConsumer> _logger;
    private readonly ILoggerFactory _loggerFactory;
    private readonly CancellationTokenSource _lifetime = new();
    private PartitionReader? _reader;
    private int _state;
    private (Checkpoint Checkpoint, bool EndsBatch)? _uncounted;

    /// <summary>
    ///     Initializes a new instance of the <see cref="IggyConsumer" /> class
    /// </summary>
    /// <param name="client">The Iggy client for server communication</param>
    /// <param name="config">Consumer configuration settings</param>
    /// <param name="loggerFactory">Logger for creating loggers</param>
    public IggyConsumer(IIggyClient client, IggyConsumerConfig config, ILoggerFactory loggerFactory)
    {
        _client = client;
        _config = config;
        _loggerFactory = loggerFactory;
        _logger = loggerFactory.CreateLogger<IggyConsumer>();
        _consumerErrorEvents = new EventAggregator<ConsumerErrorEventArgs>(loggerFactory);
    }

    /// <summary>Whether this consumer has been initialized via <see cref="InitAsync" />.</summary>
    protected bool IsInitialized => Volatile.Read(ref _state) == StateReading;

    /// <summary>Whether <see cref="ShutdownAsync" /> has started. The receive loops end once it has.</summary>
    private protected bool IsDisposed => Volatile.Read(ref _state) == StateShutDown;

    /// <summary>
    ///     The partitions that the consumer reads: those of its configuration, or the current assignment of a group
    ///     member.
    /// </summary>
    public IReadOnlyList<uint> Partitions => _reader?.Partitions ?? [];

    /// <summary>Shuts the consumer down as <see cref="ShutdownAsync" /> does, and logs its error instead of throwing it.</summary>
    public async ValueTask DisposeAsync()
    {
        try
        {
            await ShutdownAsync();
        }
        catch (Exception error)
        {
            _logger.LogWarning(error, "Shutting the consumer down failed.");
        }

        GC.SuppressFinalize(this);
    }

    /// <summary>
    ///     Connects the client when the consumer created it, creates the consumer group when it is missing and the
    ///     configuration asks for it, and starts the reader, which joins the group.
    /// </summary>
    /// <exception cref="ObjectDisposedException">The consumer was shut down.</exception>
    public async Task InitAsync(CancellationToken ct = default)
    {
        await _initialization.WaitAsync(ct);
        try
        {
            ObjectDisposedException.ThrowIf(IsDisposed, this);
            if (IsInitialized)
            {
                return;
            }

            await _client.ConnectAsync(ct);
            if (!string.IsNullOrEmpty(_config.Login) && !_config.CreateIggyClient)
            {
                await _client.LoginUserAsync(_config.Login, _config.Password, ct);
            }

            var consumer = _config.Consumer;
            if (consumer.Type == ConsumerType.ConsumerGroup)
            {
                consumer = await PrepareGroupAsync(ct);
            }

            var builder = _client.Reader(_config.StreamId, _config.TopicId, consumer)
                .WithStart(_config.PollingStrategy)
                .WithOptions(_config.PollOptions)
                .WithLoggerFactory(_loggerFactory)
                .CountingMessages();
            if (consumer.Type == ConsumerType.Consumer)
            {
                builder.WithPartitions([.. _config.Partitions]);
            }

            _reader = await builder.BuildAsync(ct);
            Volatile.Write(ref _state, StateReading);
        }
        finally
        {
            _initialization.Release();
        }
    }

    /// <summary>
    ///     Receives messages one at a time. The stream ends once the consumer shuts down. With
    ///     <see cref="CommitAfter.Processing" /> a message counts once the stream is read again, or on shutdown.
    /// </summary>
    /// <param name="ct">Cancellation token to stop receiving messages</param>
    /// <exception cref="ConsumerNotInitializedException">Thrown when InitAsync has not been called</exception>
    public async IAsyncEnumerable<ReceivedMessage> ReceiveAsync([EnumeratorCancellation] CancellationToken ct = default)
    {
        ThrowIfNotReading();
        while (await NextBatchAsync(ct) is { } batch)
        {
            for (var index = 0; index < batch.Messages.Count; index++)
            {
                if (ct.IsCancellationRequested || IsDisposed)
                {
                    yield break;
                }

                var message = batch.Messages[index];
                var checkpoint = batch.CheckpointAt(index)!;
                Hand(checkpoint, index == batch.Messages.Count - 1);
                yield return new ReceivedMessage
                {
                    Message = message,
                    CurrentOffset = message.Header.Offset,
                    PartitionId = batch.PartitionId,
                    Context = batch.Context,
                    Checkpoint = checkpoint
                };
                CountProcessed();
            }
        }
    }

    /// <summary>
    ///     Hands each message to <paramref name="handler" /> until the consumer shuts down or a handler fails. With
    ///     <see cref="CommitAfter.Delivery" /> a message counts before its handler runs, with
    ///     <see cref="CommitAfter.Processing" /> only after its handler returned with success.
    /// </summary>
    /// <exception cref="Exception">The error of the first handler that failed. Its message is not counted.</exception>
    public async Task ConsumeAsync(Func<ReceivedMessage, CancellationToken, Task> handler,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(handler);
        ThrowIfNotReading();
        while (await NextBatchAsync(ct) is { } batch)
        {
            for (var index = 0; index < batch.Messages.Count; index++)
            {
                if (ct.IsCancellationRequested || IsDisposed)
                {
                    return;
                }

                var message = batch.Messages[index];
                var checkpoint = batch.CheckpointAt(index)!;
                var endsBatch = index == batch.Messages.Count - 1;
                Hand(checkpoint, endsBatch);
                try
                {
                    await handler(new ReceivedMessage
                    {
                        Message = message,
                        CurrentOffset = message.Header.Offset,
                        PartitionId = batch.PartitionId,
                        Context = batch.Context,
                        Checkpoint = checkpoint
                    }, ct);
                }
                catch
                {
                    // A message whose handler failed does not count.
                    _uncounted = null;
                    throw;
                }

                CountProcessed();
            }
        }
    }

    /// <summary>Stores the offset of a checkpoint that this consumer handed over.</summary>
    /// <exception cref="IggyInvalidStatusCodeException">
    ///     Status 3 with <see cref="CommitMode.OnRead" />, and for a checkpoint of another consumer.
    /// </exception>
    public Task CommitAsync(Checkpoint checkpoint, CancellationToken ct = default)
    {
        return Reader.CommitAsync(checkpoint, ct);
    }

    /// <summary>
    ///     Counts a checkpoint under <see cref="CommitMode.Auto" />, for messages processed concurrently with
    ///     <see cref="CommitAfter.Marked" />.
    /// </summary>
    /// <exception cref="IggyInvalidStatusCodeException">Status 3 without <see cref="CommitMode.Auto" />.</exception>
    public void Mark(Checkpoint checkpoint)
    {
        Reader.Mark(checkpoint);
    }

    /// <summary>Sets the next read position of a partition, as <see cref="PartitionReader.SeekAsync" /> does.</summary>
    public Task SeekAsync(uint partitionId, PollingStrategy start, CancellationToken ct = default)
    {
        return Reader.SeekAsync(partitionId, start, ct);
    }

    /// <summary>
    ///     Stops reading. It counts the last message under <see cref="CommitAfter.Processing" />, closes the reader,
    ///     which stores the counted positions or moves an on read offset back and leaves the group, and disposes a
    ///     client that the consumer created.
    /// </summary>
    /// <exception cref="Exception">The first failed store of the close.</exception>
    public async Task ShutdownAsync()
    {
        if (Interlocked.Exchange(ref _state, StateShutDown) == StateShutDown)
        {
            return;
        }

        await _lifetime.CancelAsync();
        Exception? failure = null;
        if (_reader is { } reader)
        {
            CountProcessed();
            try
            {
                await reader.CloseAsync();
            }
            catch (Exception error)
            {
                failure = error;
            }
        }

        if (_config.CreateIggyClient)
        {
            try
            {
                if (_reader is not null)
                {
                    await _client.LogoutUserAsync();
                }
            }
            catch (Exception error)
            {
                _logger.LogWarning(error, "The consumer could not log out its client.");
            }
            finally
            {
                _client.Dispose();
            }
        }

        _consumerErrorEvents.Clear();
        if (failure is not null)
        {
            throw failure;
        }
    }

    /// <summary>
    ///     Event raised when an error occurs during polling.
    /// </summary>
    /// <param name="callback">Callback method</param>
    public void SubscribeToErrorEvents(Func<ConsumerErrorEventArgs, Task> callback)
    {
        _consumerErrorEvents.Subscribe(callback);
    }

    /// <summary>
    ///     Unsubscribe from error events
    /// </summary>
    /// <param name="callback"></param>
    public void UnsubscribeFromErrorEvents(Func<ConsumerErrorEventArgs, Task> callback)
    {
        _consumerErrorEvents.Unsubscribe(callback);
    }

    private PartitionReader Reader => _reader ?? throw new ConsumerNotInitializedException();

    /// <summary>
    ///     The next batch of the reader, or null once the consumer shut down or <paramref name="ct" /> was canceled. A
    ///     read error goes to the error events and the reader goes on. A message that does not decode ends the stream
    ///     with its <see cref="ReadException" />, whose <see cref="ReadException.Skip" /> passes it.
    /// </summary>
    private protected async Task<PolledMessages?> NextBatchAsync(CancellationToken ct)
    {
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(ct, _lifetime.Token);
        while (!IsDisposed && !ct.IsCancellationRequested)
        {
            try
            {
                return await Reader.NextAsync(cancellation.Token);
            }
            catch (ReadException error) when (error.Skip is null)
            {
                _consumerErrorEvents.Publish(new ConsumerErrorEventArgs(error, error.Message));
            }
            catch (OperationCanceledException) when (IsDisposed || ct.IsCancellationRequested)
            {
                return null;
            }
            catch (Exception) when (IsDisposed)
            {
                return null;
            }
        }

        return null;
    }

    /// <summary>Records a message as handed over, and counts it there under <see cref="CommitAfter.Delivery" />.</summary>
    private protected void Hand(Checkpoint checkpoint, bool endsBatch)
    {
        Reader.Delivered(checkpoint);
        switch (_config.PollOptions.Commit.Settings?.After)
        {
            case CommitAfter.Delivery:
                Reader.CountDelivered(checkpoint, 1, endsBatch);
                break;
            case CommitAfter.Processing:
                _uncounted = (checkpoint, endsBatch);
                break;
        }
    }

    /// <summary>Under <see cref="CommitAfter.Processing" />, counts the message that was processed last.</summary>
    private protected void CountProcessed()
    {
        if (_uncounted is { } processed)
        {
            _uncounted = null;
            Reader.CountDelivered(processed.Checkpoint, 1, processed.EndsBatch);
        }
    }

    private void ThrowIfNotReading()
    {
        if (!IsInitialized && !IsDisposed)
        {
            throw new ConsumerNotInitializedException();
        }
    }

    /// <summary>Creates a missing group when the configuration asks for it, and returns the group to read.</summary>
    private async Task<Consumer> PrepareGroupAsync(CancellationToken ct)
    {
        var name = _config.Consumer.ConsumerId.Kind == IdKind.String
            ? _config.Consumer.ConsumerId.GetString()
            : _config.ConsumerGroupName;
        if (string.IsNullOrEmpty(name))
        {
            throw new InvalidConsumerGroupNameException("Consumer group name is empty or null.");
        }

        var group = Identifier.String(name);
        if (await _client.GetConsumerGroupByIdAsync(_config.StreamId, _config.TopicId, group, ct) is null)
        {
            if (!_config.CreateConsumerGroupIfNotExists)
            {
                throw new ConsumerGroupNotFoundException(name);
            }

            try
            {
                await _client.CreateConsumerGroupAsync(_config.StreamId, _config.TopicId, name, ct);
            }
            catch (IggyInvalidStatusCodeException error) when (error.StatusCode == ConsumerGroupAlreadyExists)
            {
            }
        }

        return Consumer.Group(name);
    }
}
