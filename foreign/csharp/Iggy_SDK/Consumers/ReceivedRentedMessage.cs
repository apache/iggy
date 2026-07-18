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

using Apache.Iggy.Contracts;

namespace Apache.Iggy.Consumers;

/// <summary>
///     Represents a message received from the Iggy consumer whose payload and raw headers are backed by rented memory
///     shared across a polled batch. The caller MUST dispose the message once processing is complete; the underlying
///     pool buffer is returned only when the batch's last message is disposed (reference counted). There is no
///     finalizer: a batch left undisposed never returns its buffer to the pool - it is reclaimed by the GC as an
///     ordinary allocation, so forgetting to dispose leaks pool capacity rather than corrupting data.
/// </summary>
public sealed class ReceivedRentedMessage : IDisposable
{
    private int _disposed;
    internal RentedBatchHandle? Handle { get; init; }

    /// <summary>
    ///     The underlying rented message response containing headers and rented payload memory.
    /// </summary>
    public required RentedMessageResponse Message { get; init; }

    /// <summary>
    ///     The current offset of this message in the partition.
    /// </summary>
    public required ulong CurrentOffset { get; init; }

    /// <summary>
    ///     The partition ID from which this message was consumed.
    /// </summary>
    public uint PartitionId { get; init; }

    /// <summary>
    ///     The status of the message. The rented path performs no deserialization or decryption, so this is
    ///     always <see cref="MessageStatus.Success" />.
    /// </summary>
    public MessageStatus Status { get; init; } = MessageStatus.Success;

    /// <summary>
    ///     The exception that occurred during processing, if any.
    /// </summary>
    public Exception? Error { get; init; }

    /// <summary>
    ///     Releases this message's reference on the underlying rental. When the final message of a batch is disposed,
    ///     the rented buffer is returned to the pool and any payload/raw header slices are invalidated.
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        Handle?.Release();
    }
}

/// <summary>
///     Reference-counted handle around a <see cref="PolledMessagesRental" />. Shared by all
///     <see cref="ReceivedRentedMessage" /> instances produced from one poll; the rental is disposed
///     when the reference count drops to zero.
/// </summary>
public sealed class RentedBatchHandle : IDisposable
{
    private readonly PolledMessagesRental _rental;
    private int _refCount = 1;

    /// <summary>
    ///     Creates a new handle with a single self-reference held by the constructing producer. The producer
    ///     must call <see cref="Acquire" /> before each publish and release the self-reference (via
    ///     <see cref="Dispose" /> or <see cref="Release" />) once it has finished producing.
    /// </summary>
    /// <param name="rental">The polled messages rental whose lifetime this handle manages.</param>
    public RentedBatchHandle(PolledMessagesRental rental)
    {
        _rental = rental;
    }

    /// <summary>
    ///     Releases one reference on the underlying rental. Equivalent to <see cref="Release" />.
    /// </summary>
    public void Dispose()
    {
        Release();
    }

    /// <summary>
    ///     Acquires an additional reference. Must be balanced by a matching <see cref="Release" />.
    /// </summary>
    public void Acquire()
    {
        Interlocked.Increment(ref _refCount);
    }

    /// <summary>
    ///     Decrements the reference count. When the count reaches zero, the underlying rental is disposed
    ///     and its pool buffer returned.
    /// </summary>
    public void Release()
    {
        var remaining = Interlocked.Decrement(ref _refCount);
        if (remaining <= 0)
        {
            _rental.Dispose();
        }
    }
}
