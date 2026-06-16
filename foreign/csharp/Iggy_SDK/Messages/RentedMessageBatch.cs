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

using System.Buffers;
using Apache.Iggy.Extensions;
using Apache.Iggy.Headers;

namespace Apache.Iggy.Messages;

/// <summary>
///     A batch of <see cref="Message" /> instances whose payloads all live in a single buffer rented from the
///     shared <see cref="ArrayPool{T}" />. Build it with <see cref="RentedMessageBatchBuilder" />, pass
///     <see cref="Messages" /> to <c>SendMessagesAsync</c>, then dispose.
/// </summary>
/// <remarks>
///     The payload memory is valid only until <see cref="Dispose" />; afterwards <see cref="Messages" /> and any
///     <c>Payload.Span</c> access throw <see cref="ObjectDisposedException" /> instead of reading recycled pool
///     memory. For a direct send, await it before disposing. To use the background publisher, pass the batch to
///     <c>IggyPublisher.SendAsync(RentedMessageBatch)</c>, which takes ownership - do not dispose it yourself.
/// </remarks>
public sealed class RentedMessageBatch : IDisposable
{
    private readonly Message[] _messages;
    private readonly RentedMemoryManager _owner;
    private bool _disposed;

    /// <summary>
    ///     The messages in the batch. Each payload points into the shared rented buffer. Do not modify the list.
    /// </summary>
    public IList<Message> Messages
    {
        get
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            return _messages;
        }
    }

    internal RentedMessageBatch(RentedMemoryManager owner, Message[] messages)
    {
        _owner = owner;
        _messages = messages;
    }

    /// <summary>
    ///     Returns the shared rented buffer to the pool. Afterwards <see cref="Messages" /> and every payload's
    ///     <c>Span</c> throw <see cref="ObjectDisposedException" />.
    /// </summary>
    public void Dispose()
    {
        _disposed = true;
        ((IDisposable)_owner).Dispose();
    }
}

/// <summary>
///     Builds a <see cref="RentedMessageBatch" /> by serializing every payload into a single growable buffer
///     rented from <see cref="ArrayPool{T}" />. Add payloads with the <c>Add</c> overloads, then call
///     <see cref="Build" />.
/// </summary>
public sealed class RentedMessageBatchBuilder : IDisposable
{
    private readonly List<PendingMessage> _pending = [];
    private readonly PooledBufferWriter _writer;
    private bool _built;

    /// <summary>
    ///     Number of payloads added so far.
    /// </summary>
    public int Count => _pending.Count;

    /// <summary>
    ///     Initializes a new instance of the <see cref="RentedMessageBatchBuilder" /> class.
    /// </summary>
    /// <param name="sizeHint">Initial capacity hint for the rented buffer.</param>
    public RentedMessageBatchBuilder(int sizeHint = 1024)
    {
        _writer = new PooledBufferWriter(sizeHint);
    }

    /// <summary>
    ///     Disposes the builder, returning the buffer to the pool. Call only if you abandon the builder before
    ///     <see cref="Build" />; once built, dispose the returned <see cref="RentedMessageBatch" /> instead.
    /// </summary>
    public void Dispose()
    {
        // After Build, DetachBuffer left the writer disposed, so this is a no-op behind the new owner's back.
        _writer.Dispose();
    }

    /// <summary>
    ///     Adds a payload serialized through the supplied callback. <paramref name="state" /> is passed through
    ///     so a static callback can be used without a per-message closure allocation.
    /// </summary>
    /// <param name="state">State forwarded to <paramref name="writePayload" />.</param>
    /// <param name="writePayload">Writes the payload bytes into the provided <see cref="IBufferWriter{T}" />.</param>
    /// <param name="id">Optional message ID. When null the message is sent with ID 0 and the server assigns one.</param>
    /// <param name="userHeaders">Optional user headers to attach to the message.</param>
    public void Add<TState>(TState state, Action<TState, IBufferWriter<byte>> writePayload, Guid? id = null,
        Dictionary<HeaderKey, HeaderValue>? userHeaders = null)
    {
        ArgumentNullException.ThrowIfNull(writePayload);
        ThrowIfBuilt();

        var offset = _writer.WrittenCount;
        writePayload(state, _writer);
        _pending.Add(new PendingMessage(id, offset, _writer.WrittenCount - offset, userHeaders));
    }

    /// <summary>
    ///     Adds a payload by copying the supplied bytes into the shared buffer.
    /// </summary>
    /// <param name="payload">The payload bytes to copy.</param>
    /// <param name="id">Optional message ID. When null the message is sent with ID 0 and the server assigns one.</param>
    /// <param name="userHeaders">Optional user headers to attach to the message.</param>
    public void Add(ReadOnlySpan<byte> payload, Guid? id = null,
        Dictionary<HeaderKey, HeaderValue>? userHeaders = null)
    {
        ThrowIfBuilt();

        var offset = _writer.WrittenCount;
        payload.CopyTo(_writer.GetSpan(payload.Length));
        _writer.Advance(payload.Length);
        _pending.Add(new PendingMessage(id, offset, payload.Length, userHeaders));
    }

    /// <summary>
    ///     Materializes the batch, handing ownership of the rented buffer to the returned
    ///     <see cref="RentedMessageBatch" />. The builder must not be used afterwards.
    /// </summary>
    public RentedMessageBatch Build()
    {
        ThrowIfBuilt();

        var writtenCount = _writer.WrittenCount;
        var owner = new RentedMemoryManager(_writer.DetachBuffer(), writtenCount);
        try
        {
            ReadOnlyMemory<byte> buffer = owner.Memory;
            var messages = new Message[_pending.Count];
            for (var i = 0; i < _pending.Count; i++)
            {
                var pending = _pending[i];
                messages[i] = new Message(pending.Id?.ToUInt128() ?? UInt128.Zero,
                    buffer.Slice(pending.Offset, pending.Length), pending.UserHeaders);
            }

            _built = true;
            return new RentedMessageBatch(owner, messages);
        }
        catch
        {
            ((IDisposable)owner).Dispose();
            throw;
        }
    }

    // Use after Dispose throws ObjectDisposedException from the underlying writer, so only the built state is guarded here.
    private void ThrowIfBuilt()
    {
        if (_built)
        {
            throw new InvalidOperationException("The batch has already been built.");
        }
    }

    private readonly record struct PendingMessage(
        Guid? Id,
        int Offset,
        int Length,
        Dictionary<HeaderKey, HeaderValue>? UserHeaders);
}
