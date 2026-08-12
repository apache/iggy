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
using Apache.Iggy.Enums;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Messages;
using Apache.Iggy.Utils;
using Microsoft.Extensions.Logging;

namespace Apache.Iggy.Publishers;

/// <summary>
///     Background message processor: queues <see cref="SendUnit" /> items on a bounded channel and flushes them in
///     batches with retry/backoff, pausing while the client is disconnected. Each unit is sent in exactly one flush
///     and disposed once afterwards; it is never split, so an oversized unit ships whole in a single flush.
/// </summary>
internal sealed partial class BackgroundMessageProcessor : IAsyncDisposable
{
    private readonly EventAggregator<PublisherErrorEventArgs> _backgroundErrorAggregator;
    private readonly CancellationTokenSource _cancellationTokenSource;
    private readonly IIggyClient _client;
    private readonly IggyPublisherConfig _config;
    private readonly CancellationToken _disposalToken;
    private readonly ILogger<BackgroundMessageProcessor> _logger;
    private readonly EventAggregator<MessageBatchFailedEventArgs> _messageBatchErrorAggregator;
    private readonly int _payloadBufferBaselineCapacity;
    private readonly int _payloadBufferCapacityLimit;
    private readonly ChannelReader<SendUnit> _reader;
    private readonly List<SendUnit> _units;
    private readonly List<Message> _wire;
    private readonly ChannelWriter<SendUnit> _writer;
    private Task? _backgroundTask;
    private volatile bool _canSend = true;
    private Exception? _crashException;
    private bool _disposed;
    private TaskCompletionSource _drainedSignal = CreateCompletedSignal();
    private HashSet<SendUnit>? _failed;

    // Mutated only via Interlocked; OnUnitsCompleted resolves _drainedSignal on the 1->0 transition.
    private int _inFlight;
    private PooledBufferWriter _payloadBuffer;

    public BackgroundMessageProcessor(IIggyClient client, IggyPublisherConfig config, ILoggerFactory loggerFactory)
    {
        _client = client;
        _config = config;
        _logger = loggerFactory.CreateLogger<BackgroundMessageProcessor>();
        _cancellationTokenSource = new CancellationTokenSource();
        _disposalToken = _cancellationTokenSource.Token;
        _backgroundErrorAggregator = new EventAggregator<PublisherErrorEventArgs>(loggerFactory);
        _messageBatchErrorAggregator = new EventAggregator<MessageBatchFailedEventArgs>(loggerFactory);

        var options = new BoundedChannelOptions(config.BackgroundQueueCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleReader = true,
            SingleWriter = false
        };

        Channel<SendUnit> channel = Channel.CreateBounded<SendUnit>(options);
        _writer = channel.Writer;
        _reader = channel.Reader;
        _units = new List<SendUnit>(config.BackgroundBatchSize);
        _wire = new List<Message>(config.BackgroundBatchSize);
        _payloadBufferBaselineCapacity = Math.Max(config.BackgroundBatchSize * 64, 1024);
        _payloadBufferCapacityLimit = _payloadBufferBaselineCapacity * 4;
        _payloadBuffer = new PooledBufferWriter(_payloadBufferBaselineCapacity);

        _client.SubscribeConnectionEvents(ClientOnOnConnectionStateChanged);
    }

    /// <summary>
    ///     Disposes the background processor, cancels ongoing operations, waits for the background task to complete,
    ///     and releases any queued units still holding pooled buffers.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _client.UnsubscribeConnectionEvents(ClientOnOnConnectionStateChanged);

        await _cancellationTokenSource.CancelAsync();
        _writer.TryComplete();

        var backgroundTaskTimedOut = false;
        if (_backgroundTask != null)
        {
            LogWaitingForBackgroundTask();
            try
            {
                await _backgroundTask.WaitAsync(_config.BackgroundDisposalTimeout);
                LogBackgroundTaskCompleted();
            }
            catch (TimeoutException)
            {
                backgroundTaskTimedOut = true;
                LogBackgroundTaskTimeout();
            }
            catch (Exception e)
            {
                LogBackgroundProcessorError(e);
            }
        }
        else
        {
            DrainAndDispose();
        }

        // On timeout the orphaned task still uses the token; the CTS holds no unmanaged state after
        // CancelAsync, so skipping Dispose just leaves it to the GC.
        if (!backgroundTaskTimedOut)
        {
            _cancellationTokenSource.Dispose();
        }

        _backgroundErrorAggregator.Clear();
        _messageBatchErrorAggregator.Clear();
        _disposed = true;
    }

    /// <summary>
    ///     Starts the background message processing task. Does nothing if already running.
    /// </summary>
    public void Start()
    {
        if (_backgroundTask != null)
        {
            return;
        }

        _backgroundTask = RunBackgroundProcessor(_cancellationTokenSource.Token);
        LogBackgroundProcessorStarted();
    }

    /// <summary>
    ///     Enqueues a unit for background sending, transferring ownership of any pooled buffer it holds to the
    ///     processor, which disposes the unit after it is flushed (or discarded at disposal).
    /// </summary>
    public async ValueTask EnqueueAsync(SendUnit unit, CancellationToken ct)
    {
        Interlocked.Increment(ref _inFlight);
        try
        {
            await _writer.WriteAsync(unit, ct);
        }
        catch
        {
            // The unit never entered the channel, so the processor loop will not see it; release it here.
            OnUnitsCompleted(1);
            unit.Dispose();
            throw;
        }
    }

    /// <summary>
    ///     Completes once every unit enqueued so far has been sent and disposed; resolves immediately when nothing
    ///     is in flight. If the processor is disposed while units are in flight, those units are discarded rather
    ///     than sent and this throws <see cref="OperationCanceledException" /> instead of reporting a clean drain.
    ///     If the processor crashed, queued units were discarded and this throws
    ///     <see cref="InvalidOperationException" /> carrying the crash as its inner exception.
    /// </summary>
    public async Task WaitForDrainAsync(CancellationToken ct)
    {
        var waited = false;
        while (Volatile.Read(ref _inFlight) != 0)
        {
            _disposalToken.ThrowIfCancellationRequested();
            waited = true;

            var signal = Volatile.Read(ref _drainedSignal);

            if (signal.Task.IsCompleted)
            {
                Interlocked.CompareExchange(ref _drainedSignal,
                    new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously), signal);
                continue;
            }

            // Re-check after capturing: a completer may have zeroed _inFlight and resolved the previous
            // signal before this fresh one was installed, in which case nothing will ever resolve it.
            if (Volatile.Read(ref _inFlight) == 0)
            {
                continue;
            }

            await signal.Task.WaitAsync(ct);
        }

        if (waited)
        {
            _disposalToken.ThrowIfCancellationRequested();
        }

        if (Volatile.Read(ref _crashException) is { } crash)
        {
            throw new InvalidOperationException(
                "Background message processor terminated unexpectedly; queued messages were discarded.", crash);
        }
    }

    public void SubscribeOnBackgroundError(Func<PublisherErrorEventArgs, Task> handler)
    {
        _backgroundErrorAggregator.Subscribe(handler);
    }

    public void UnsubscribeOnBackgroundError(Func<PublisherErrorEventArgs, Task> handler)
    {
        _backgroundErrorAggregator.Unsubscribe(handler);
    }

    public void SubscribeOnMessageBatchFailed(Func<MessageBatchFailedEventArgs, Task> handler)
    {
        _messageBatchErrorAggregator.Subscribe(handler);
    }

    public void UnsubscribeOnMessageBatchFailed(Func<MessageBatchFailedEventArgs, Task> handler)
    {
        _messageBatchErrorAggregator.Unsubscribe(handler);
    }

    private async Task RunBackgroundProcessor(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(_config.BackgroundFlushInterval);

        try
        {
            while (!ct.IsCancellationRequested)
            {
                if (!_canSend)
                {
                    if (!await timer.WaitForNextTickAsync(ct))
                    {
                        break;
                    }

                    continue;
                }

                if (!AccumulateBatch())
                {
                    if (!await timer.WaitForNextTickAsync(ct))
                    {
                        break;
                    }

                    continue;
                }

                List<Message> wire = MaterializeWire();
                if (wire.Count > 0)
                {
                    await SendWithRetry(wire, ct);
                }

                RecycleBatch();
            }
        }
        catch (OperationCanceledException)
        {
            LogBackgroundProcessorCancelled();
        }
        catch (Exception ex)
        {
            // Latch before the finally drain zeroes _inFlight, so a drain waiter woken by the discard
            // sees the crash instead of reporting a clean drain over discarded messages.
            Volatile.Write(ref _crashException, ex);
            LogBackgroundProcessorCrashed(ex, Volatile.Read(ref _inFlight));
            _writer.TryComplete(ex);
            if (_backgroundErrorAggregator.HasSubscribers)
            {
                _backgroundErrorAggregator.Publish(new PublisherErrorEventArgs(ex,
                    "Background message processor terminated unexpectedly; all queued messages were discarded"));
            }
        }
        finally
        {
            // Close the channel before draining so no unit can slip in after the drain: once the loop is gone,
            // an accepted unit would never be sent or disposed and its producer's drain wait would hang.
            _writer.TryComplete();
            DrainAndDispose();
            LogBackgroundProcessorStopped();
        }
    }

    /// <summary>
    ///     Drains queued units into the internal batch without blocking, serializing each into the shared payload
    ///     buffer as it is read. Stops once the accumulated message count reaches
    ///     <see cref="IggyPublisherConfig.BackgroundBatchSize" /> or the accumulated payload size reaches
    ///     <see cref="IggyPublisherConfig.BackgroundMaxBatchBytes" />. A unit is never split, so either limit may
    ///     overshoot by the last unit's contribution and a unit larger than the byte limit ships whole. A unit
    ///     whose serializer throws is rolled back out and recorded in <see cref="_failed" /> so the wire pass
    ///     skips it; the remaining units still ship.
    /// </summary>
    private bool AccumulateBatch()
    {
        _payloadBuffer.Rewind(0);
        _failed = null;

        var maxBytes = _config.BackgroundMaxBatchBytes;
        var count = 0;
        var bytes = 0;
        while (count < _config.BackgroundBatchSize
               && (maxBytes <= 0 || bytes < maxBytes)
               && _reader.TryRead(out var unit))
        {
            _units.Add(unit);
            count += unit.Count;

            var checkpoint = _payloadBuffer.WrittenCount;
            try
            {
                unit.Serialize(_payloadBuffer);
            }
            catch (Exception ex)
            {
                _payloadBuffer.Rewind(checkpoint);
                (_failed ??= []).Add(unit);
                ReportUnitDropped(unit, ex);
            }

            bytes += _payloadBuffer.WrittenCount - checkpoint + unit.ReadyBytes;
        }

        return _units.Count > 0;
    }

    /// <summary>
    ///     Builds the reused wire-ready <see cref="Message" /> list from the units serialized by
    ///     <see cref="AccumulateBatch" />. Runs as a separate pass because the shared payload buffer can move while
    ///     it grows during serialization, so the slices are stable only once every unit has serialized. Units that
    ///     failed to materialize are rolled back out and dropped via <see cref="ReportUnitDropped" />. Encryption is
    ///     applied later by the client at send time, so an encryptor failure surfaces as a batch send failure.
    /// </summary>
    private List<Message> MaterializeWire()
    {
        ReadOnlyMemory<byte> buffer = _payloadBuffer.Written;
        _wire.Clear();
        foreach (var unit in _units)
        {
            if (_failed?.Contains(unit) == true)
            {
                continue;
            }

            var wireCount = _wire.Count;
            try
            {
                unit.AppendWire(_wire, buffer);
            }
            catch (Exception ex)
            {
                _wire.RemoveRange(wireCount, _wire.Count - wireCount);
                ReportUnitDropped(unit, ex);
            }
        }

        return _wire;
    }

    // Drops only the offending unit so a throwing serializer cannot kill the loop and discard the queue.
    private void ReportUnitDropped(SendUnit unit, Exception ex)
    {
        LogFailedToMaterializeUnit(ex);

        if (_backgroundErrorAggregator.HasSubscribers)
        {
            _backgroundErrorAggregator.Publish(new PublisherErrorEventArgs(ex,
                "Failed to serialize a queued message; the unit was dropped", unit.DroppedValues));
        }
    }

    /// <summary>
    ///     Disposes the just-sent units (returning any pooled buffers they own) and clears the internal batch so it
    ///     can be reused. The shared payload buffer is kept and rewound on the next flush.
    /// </summary>
    private void RecycleBatch()
    {
        var completed = _units.Count;
        foreach (var unit in _units)
        {
            unit.Dispose();
        }

        OnUnitsCompleted(completed);

        _units.Clear();
        _failed = null;

        _wire.Clear();
        if (_wire.Capacity > _config.BackgroundBatchSize * 4)
        {
            _wire.Capacity = _config.BackgroundBatchSize;
        }

        if (_payloadBuffer.Capacity > _payloadBufferCapacityLimit)
        {
            var replacement = new PooledBufferWriter(_payloadBufferBaselineCapacity);
            _payloadBuffer.Dispose();
            _payloadBuffer = replacement;
        }
    }

    private void DrainAndDispose()
    {
        var completed = 0;
        while (_reader.TryRead(out var unit))
        {
            unit.Dispose();
            completed++;
        }

        foreach (var unit in _units)
        {
            unit.Dispose();
            completed++;
        }

        OnUnitsCompleted(completed);
        _units.Clear();
        _failed = null;
        _wire.Clear();
        _payloadBuffer.Dispose();
    }

    private void OnUnitsCompleted(int count)
    {
        if (count <= 0)
        {
            return;
        }

        if (Interlocked.Add(ref _inFlight, -count) == 0)
        {
            Volatile.Read(ref _drainedSignal).TrySetResult();
        }
    }

    private static TaskCompletionSource CreateCompletedSignal()
    {
        var signal = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        signal.SetResult();
        return signal;
    }

    /// <summary>
    ///     Sends the materialized wire batch, applying exponential-backoff retry when enabled. On terminal failure
    ///     the batch is published to the message-batch-failed event (snapshotted, since the wire list is reused).
    /// </summary>
    private async Task SendWithRetry(List<Message> wire, CancellationToken ct)
    {
        if (!_config.EnableRetry)
        {
            try
            {
                await _client.SendMessagesAsync(_config.StreamId, _config.TopicId, _config.Partitioning, wire, ct);
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                // Disposal cancellation, not a send failure; let the loop's cancellation handling take over.
                throw;
            }
            catch (Exception ex)
            {
                LogFailedToSendBatch(ex, wire.Count);
                if (_messageBatchErrorAggregator.HasSubscribers)
                {
                    _messageBatchErrorAggregator.Publish(new MessageBatchFailedEventArgs(ex, SnapshotForFailure(wire)));
                }
            }

            return;
        }

        Exception? lastException = null;
        var delay = _config.InitialRetryDelay;

        var maxAttempts = Math.Max(1, _config.MaxRetryAttempts);

        for (var attempt = 0; attempt < maxAttempts; attempt++)
        {
            try
            {
                await _client.SendMessagesAsync(_config.StreamId, _config.TopicId, _config.Partitioning, wire, ct);
                return;
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                // Disposal cancellation, not a send failure; without this the loop would spin through the
                // remaining attempts instantly and publish a misleading "failed after N attempts" event.
                throw;
            }
            catch (Exception ex)
            {
                lastException = ex;

                if (attempt < maxAttempts - 1 && !ct.IsCancellationRequested)
                {
                    LogRetryingBatch(ex, wire.Count, attempt + 1, maxAttempts,
                        delay.TotalMilliseconds);
                    await Task.Delay(delay, ct);

                    var nextDelayMs = delay.TotalMilliseconds * _config.RetryBackoffMultiplier;

                    if (double.IsInfinity(nextDelayMs) || double.IsNaN(nextDelayMs) ||
                        nextDelayMs > _config.MaxRetryDelay.TotalMilliseconds)
                    {
                        delay = _config.MaxRetryDelay;
                    }
                    else
                    {
                        delay = TimeSpan.FromMilliseconds(Math.Min(nextDelayMs, TimeSpan.MaxValue.TotalMilliseconds));
                    }
                }
            }
        }

        LogFailedToSendBatchAfterRetries(lastException!, wire.Count, maxAttempts);
        if (_messageBatchErrorAggregator.HasSubscribers)
        {
            _messageBatchErrorAggregator.Publish(new MessageBatchFailedEventArgs(lastException!,
                SnapshotForFailure(wire), maxAttempts));
        }
    }

    // Payloads point into pooled buffers recycled right after the flush; copy them so the fire-and-forget event stays safe.
    private static Message[] SnapshotForFailure(List<Message> wire)
    {
        var snapshot = new Message[wire.Count];
        for (var i = 0; i < wire.Count; i++)
        {
            var source = wire[i];
            try
            {
                snapshot[i] = new Message
                {
                    Header = source.Header,
                    Payload = source.Payload.ToArray(),
                    UserHeaders = source.UserHeaders,
                    RawUserHeaders = source.RawUserHeaders.IsEmpty ? default : source.RawUserHeaders.ToArray()
                };
            }
            catch (ObjectDisposedException)
            {
                // The payload memory is already gone (e.g. the caller disposed a rented batch it had handed over).
                // Keep the header so the event still ships instead of crashing the processor loop.
                snapshot[i] = new Message
                {
                    Header = source.Header,
                    Payload = default,
                    UserHeaders = source.UserHeaders
                };
            }
        }

        return snapshot;
    }

    private Task ClientOnOnConnectionStateChanged(ConnectionStateChangedEventArgs e)
    {
        var canSend = e.CurrentState == ConnectionState.Authenticated;

        if (_canSend && !canSend)
        {
            LogClientIsDisconnected();
        }

        _canSend = canSend;
        return Task.CompletedTask;
    }
}
