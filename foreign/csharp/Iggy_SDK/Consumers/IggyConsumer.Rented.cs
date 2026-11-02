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
using Apache.Iggy.Exceptions;
using Apache.Iggy.Mappers;

namespace Apache.Iggy.Consumers;

public partial class IggyConsumer
{
    /// <summary>
    ///     Receives messages as <see cref="ReceiveAsync" /> does, each a <see cref="ReceivedRentedMessage" /> that shares
    ///     the pooled buffer of its batch and MUST be disposed by the caller when processing is complete. The buffer
    ///     returns to the pool once every message of its batch has been disposed. The stream ends once the consumer
    ///     shuts down.
    /// </summary>
    /// <param name="ct">Cancellation token to stop receiving messages.</param>
    /// <exception cref="ConsumerNotInitializedException">Thrown when <see cref="InitAsync" /> has not been called.</exception>
    public async IAsyncEnumerable<ReceivedRentedMessage> ReceiveRentedAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ThrowIfNotReading();
        while (await NextBatchAsync(ct) is { } batch)
        {
            var checkpoints = Enumerable.Range(0, batch.Messages.Count).Select(batch.CheckpointAt).ToArray();
            var handle = new RentedBatchHandle(BinaryMapper.ToRentedMessages(batch));
            try
            {
                var rental = handle.Rental;
                for (var index = 0; index < rental.Messages.Count; index++)
                {
                    if (ct.IsCancellationRequested || IsDisposed)
                    {
                        yield break;
                    }

                    var message = rental.Messages[index];
                    Hand(checkpoints[index]!, index == rental.Messages.Count - 1);
                    handle.Acquire();
                    yield return new ReceivedRentedMessage
                    {
                        Handle = handle,
                        Message = message,
                        CurrentOffset = message.Header.Offset,
                        PartitionId = batch.PartitionId,
                        Context = batch.Context,
                        Checkpoint = checkpoints[index]
                    };
                    CountProcessed();
                }
            }
            finally
            {
                handle.Release();
            }
        }
    }
}
