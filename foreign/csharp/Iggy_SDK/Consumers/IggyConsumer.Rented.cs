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
using Apache.Iggy.Kinds;
using Microsoft.Extensions.Logging;

namespace Apache.Iggy.Consumers;

public partial class IggyConsumer
{
    /// <summary>
    ///     Receives messages asynchronously as an async stream of rented messages. Each yielded
    ///     <see cref="ReceivedRentedMessage" /> shares its underlying pooled buffer with the other messages from the
    ///     same poll and MUST be disposed by the caller when processing is complete. The buffer is returned to the
    ///     pool once every message of its batch has been disposed.
    /// </summary>
    /// <param name="ct">Cancellation token to stop receiving messages.</param>
    /// <returns>An async enumerable of rented messages.</returns>
    /// <exception cref="ConsumerNotInitializedException">Thrown when <see cref="InitAsync" /> has not been called.</exception>
    public async IAsyncEnumerable<ReceivedRentedMessage> ReceiveRentedAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        if (!_isInitialized)
        {
            throw new ConsumerNotInitializedException();
        }

        do
        {
            if (!_rentedChannel.Reader.TryRead(out var message))
            {
                await PollRentedMessagesAsync(ct);
                continue;
            }

            yield return message;

            if (_config.AutoCommitMode == AutoCommitMode.AfterReceive)
            {
                await StoreOffsetAsync(message.CurrentOffset, message.PartitionId, false, ct);
            }
        } while (!ct.IsCancellationRequested);
    }

    /// <summary>
    ///     Publishes a single rented message from a polled batch to the consumer channel. Called once per
    ///     message during <see cref="PollRentedMessagesAsync" />. The caller acquires a reference on
    ///     <paramref name="rental" /> before invocation; the channel reader is expected to release that
    ///     reference by disposing the produced <see cref="ReceivedRentedMessage" />. Override to redirect
    ///     rented batches to a different sink (e.g. typed deserialization) — overrides must ensure the
    ///     acquired reference is released exactly once on every path.
    /// </summary>
    /// <param name="rental">Reference-counted handle around the polled batch. Caller has already acquired one reference.</param>
    /// <param name="message">
    ///     The rented message to publish. Payload and raw headers are slices of pooled memory tied to
    ///     <paramref name="rental" />.
    /// </param>
    /// <param name="partitionId">Partition the message was polled from.</param>
    /// <param name="status">Outcome of any prior processing (e.g. decryption).</param>
    /// <param name="error">Exception captured if <paramref name="status" /> is non-success; otherwise null.</param>
    /// <param name="ct">Cancellation token.</param>
    protected virtual async Task PublishRentedAsync(RentedBatchHandle rental,
        RentedMessageResponse message,
        uint partitionId,
        MessageStatus status,
        Exception? error,
        CancellationToken ct)
    {
        await _rentedChannel.Writer.WriteAsync(new ReceivedRentedMessage
        {
            Handle = rental,
            Message = message,
            CurrentOffset = message.Header.Offset,
            PartitionId = partitionId,
            Status = status,
            Error = error
        }, ct);
    }

    /// <summary>
    ///     Polls a rented batch from the server and publishes it via <see cref="PublishRentedAsync" />.
    ///     Handles offset tracking and auto-commit logic. Rental lifetime is managed via
    ///     <see cref="RentedBatchHandle" /> shared by every produced message.
    /// </summary>
    protected async Task PollRentedMessagesAsync(CancellationToken ct)
    {
        if (!_joinedConsumerGroup)
        {
            LogConsumerGroupNotJoinedYetSkippingPolling();
            return;
        }

        ThrowIfAutoCommitWithEncryptor();

        await _pollingSemaphore.WaitAsync(ct);

        PolledMessagesRental? rental = null;
        RentedBatchHandle? batchHandle = null;
        try
        {
            if (_config.PollingIntervalMs > 0)
            {
                await WaitBeforePollingAsync(ct);
            }

            rental = await _client.PollMessagesRentedAsync(_config.StreamId, _config.TopicId,
                _config.PartitionId, _config.Consumer, _config.PollingStrategy, _config.BatchSize,
                _config.AutoCommit, ct);

            if (rental.Messages.Count == 0)
            {
                if (_logger.IsEnabled(LogLevel.Debug))
                {
                    _logger.LogDebug("No messages received from poll for partition {PartitionId}", rental.PartitionId);
                }

                return;
            }

            var partitionId = (uint)rental.PartitionId;

            var hasLastOffset = _lastPolledOffset.TryGetValue(rental.PartitionId, out var lastPolledPartitionOffset);

            var currentOffset = 0ul;

            batchHandle = new RentedBatchHandle(rental);
            var anyNewMessages = false;
            foreach (var message in rental.Messages)
            {
                if (hasLastOffset && message.Header.Offset <= lastPolledPartitionOffset)
                {
                    continue;
                }

                currentOffset = message.Header.Offset;
                batchHandle.Acquire();
                try
                {
                    await PublishRentedAsync(batchHandle, message, partitionId, MessageStatus.Success, null, ct);
                }
                catch
                {
                    batchHandle.Release();
                    throw;
                }

                anyNewMessages = true;
            }

            if (!anyNewMessages)
            {
                if (_config.AutoCommitMode != AutoCommitMode.Disabled)
                {
                    if (_logger.IsEnabled(LogLevel.Debug))
                    {
                        _logger.LogDebug(
                            "No new messages found, committing offset {Offset} for partition {PartitionId}",
                            lastPolledPartitionOffset, rental.PartitionId);
                    }

                    await StoreOffsetAsync(lastPolledPartitionOffset, partitionId, false, ct);
                }

                return;
            }

            _lastPolledOffset.AddOrUpdate(rental.PartitionId, currentOffset, (_, _) => currentOffset);

            if (_config.PollingStrategy.Kind == MessagePolling.Offset)
            {
                _config.PollingStrategy = PollingStrategy.Offset(currentOffset + 1);
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch (MessageDecryptionException ex)
        {
            LogFailedToDecryptMessage(ex, ex.Offset, ex.PartitionId);
            throw;
        }
        catch (MalformedResponseException)
        {
            // Non-transient poison: rethrow so the generic catch below does not swallow it and re-poll forever.
            // Base InvalidResponseException (server error status, possibly transient) falls through to retry.
            throw;
        }
        catch (Exception ex)
        {
            LogFailedToPollMessages(ex);
            _consumerErrorEvents.Publish(new ConsumerErrorEventArgs(ex, "Failed to poll messages"));
        }
        finally
        {
            if (batchHandle is not null)
            {
                batchHandle.Release();
            }
            else
            {
                rental?.Dispose();
            }

            _pollingSemaphore.Release();
        }
    }
}
