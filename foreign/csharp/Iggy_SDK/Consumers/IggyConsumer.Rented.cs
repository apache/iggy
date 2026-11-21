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
    ///     Receives messages as <see cref="ReceiveAsync" /> does, with payload views into the reader batch.
    ///     The caller must dispose each <see cref="ReceivedRentedMessage" /> after processing it.
    ///     The stream ends once the consumer shuts down.
    /// </summary>
    /// <param name="ct">Cancellation token to stop receiving messages.</param>
    /// <exception cref="ConsumerNotInitializedException">Thrown when <see cref="InitAsync" /> has not been called.</exception>
    /// <exception cref="InvalidOperationException">Another receive or handler loop is active.</exception>
    public async IAsyncEnumerable<ReceivedRentedMessage> ReceiveRentedAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        using var loop = EnterLoop();
        CountProcessed();
        while (await NextBatchAsync(ct) is { } batch)
        {
            using var handle = new RentedBatchHandle(batch.Context);
            while (CanDeliver(batch))
            {
                if (ct.IsCancellationRequested || IsDisposed)
                {
                    yield break;
                }

                var index = _nextMessageIndex++;
                var message = BinaryMapper.ToRentedMessage(batch.Messages[index]);
                var checkpoint = batch.CheckpointAt(index)!;
                Hand(checkpoint, index == batch.Messages.Count - 1);
                handle.Acquire();
                yield return new ReceivedRentedMessage
                {
                    Handle = handle,
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
}
