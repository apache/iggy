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

using Apache.Iggy.Extensions;
using Apache.Iggy.Headers;
using Apache.Iggy.Messages;

namespace Apache.Iggy.Publishers;

/// <summary>
///     A single unit of work queued for background sending: enqueued with one call, sent in exactly one flush, and
///     disposed exactly once afterwards. The unit owns any pooled buffer behind its messages, so a
///     <see cref="RentedMessageBatch" /> handed to the background processor is released only after the whole unit
///     has been sent.
/// </summary>
internal abstract class SendUnit : IDisposable
{
    /// <summary>
    ///     Number of messages this unit contributes to a flush. A unit is never split across flushes, so a unit
    ///     larger than the batch size ships whole.
    /// </summary>
    public abstract int Count { get; }

    /// <summary>
    ///     The original values queued in this unit, for units that serialize lazily; null for units whose messages
    ///     are already materialized. Read only when the unit is dropped because its serializer or encryptor threw,
    ///     so the caller can persist or re-send the source values.
    /// </summary>
    public virtual object?[]? DroppedValues => null;

    /// <summary>
    ///     Payload bytes this unit contributes to a flush without writing into the shared payload buffer, used by
    ///     the byte-size batch guard. Lazily-serialized units report 0 here because their bytes are measured from
    ///     the buffer after <see cref="Serialize" />; already-materialized units report their payload size.
    /// </summary>
    public virtual int ReadyBytes => 0;

    /// <summary>
    ///     Releases any pooled buffer this unit owns. Called once, after the unit's flush.
    /// </summary>
    public virtual void Dispose()
    {
    }

    /// <summary>
    ///     First flush pass: lazily-serialized units write their payloads into the shared
    ///     <paramref name="payloadBuffer" /> and record their slices.
    /// </summary>
    public abstract void Serialize(PooledBufferWriter payloadBuffer);

    /// <summary>
    ///     Second flush pass: appends the wire-ready messages onto <paramref name="wire" />. Runs after every unit
    ///     has serialized, so payload slices are stable.
    /// </summary>
    public abstract void AppendWire(List<Message> wire, ReadOnlyMemory<byte> payloadBuffer);
}

/// <summary>
///     A unit of already-materialized messages backed by an optional shared owner (such as a
///     <see cref="RentedMessageBatch" />), disposed once after the unit is sent.
/// </summary>
internal sealed class ReadyUnit : SendUnit
{
    private readonly IList<Message> _messages;
    private readonly IDisposable? _owner;

    public override int Count => _messages.Count;

    public override int ReadyBytes
    {
        get
        {
            var total = 0;
            for (var i = 0; i < _messages.Count; i++)
            {
                total += _messages[i].Payload.Length;
            }

            return total;
        }
    }

    public ReadyUnit(IList<Message> messages, IDisposable? owner)
    {
        _messages = messages;
        _owner = owner;
    }

    public override void Serialize(PooledBufferWriter payloadBuffer)
    {
    }

    public override void AppendWire(List<Message> wire, ReadOnlyMemory<byte> payloadBuffer)
    {
        wire.AddRange(_messages);
    }

    public override void Dispose()
    {
        _owner?.Dispose();
    }
}

/// <summary>
///     A unit of typed values serialized at flush time into the processor's shared payload buffer. Holds the
///     serializer it was created with, so the non-generic processor loop can drive it. Encryption is applied by
///     the client at send time, not here.
/// </summary>
/// <remarks>
///     Because serialization is deferred, a value is captured by reference when queued: mutating a queued mutable
///     reference type before the flush changes what is sent. Pass an immutable value or a snapshot.
/// </remarks>
internal sealed class TypedUnit<T> : SendUnit
{
    private readonly Item[] _items;
    private readonly ISerializer<T> _serializer;

    public override int Count => _items.Length;

    public override object?[] DroppedValues
    {
        get
        {
            var values = new object?[_items.Length];
            for (var i = 0; i < _items.Length; i++)
            {
                values[i] = _items[i].Value;
            }

            return values;
        }
    }

    private TypedUnit(Item[] items, ISerializer<T> serializer)
    {
        _items = items;
        _serializer = serializer;
    }

    public static TypedUnit<T> Single(T value, UInt128 id, Dictionary<HeaderKey, HeaderValue>? headers,
        ISerializer<T> serializer)
    {
        return new TypedUnit<T>([
            new Item
            {
                Value = value,
                Id = id,
                Headers = headers
            }
        ], serializer);
    }

    public static TypedUnit<T> Batch(IEnumerable<T> values, ISerializer<T> serializer)
    {
        Item[] items = Materialize(values, static value => new Item { Value = value });
        return new TypedUnit<T>(items, serializer);
    }

    public static TypedUnit<T> Batch(
        IEnumerable<(T data, Guid? messageId, Dictionary<HeaderKey, HeaderValue>? userHeaders)> values,
        ISerializer<T> serializer)
    {
        Item[] items = Materialize(values, static value => new Item
        {
            Value = value.data,
            Id = value.messageId?.ToUInt128() ?? UInt128.Zero,
            Headers = value.userHeaders
        });
        return new TypedUnit<T>(items, serializer);
    }

    private static Item[] Materialize<TSource>(IEnumerable<TSource> values, Func<TSource, Item> toItem)
    {
        if (values.TryGetNonEnumeratedCount(out var count))
        {
            var items = new Item[count];
            var i = 0;
            foreach (var value in values)
            {
                items[i++] = toItem(value);
            }

            return items;
        }

        var list = new List<Item>();
        foreach (var value in values)
        {
            list.Add(toItem(value));
        }

        return list.ToArray();
    }

    public override void Serialize(PooledBufferWriter payloadBuffer)
    {
        for (var i = 0; i < _items.Length; i++)
        {
            var offset = payloadBuffer.WrittenCount;
            _serializer.Serialize(_items[i].Value, payloadBuffer);
            _items[i].Offset = offset;
            _items[i].Length = payloadBuffer.WrittenCount - offset;
        }
    }

    public override void AppendWire(List<Message> wire, ReadOnlyMemory<byte> payloadBuffer)
    {
        for (var i = 0; i < _items.Length; i++)
        {
            ref var item = ref _items[i];
            var message = new Message(item.Id, payloadBuffer.Slice(item.Offset, item.Length), item.Headers);
            wire.Add(message);
        }
    }

    private struct Item
    {
        public T Value;
        public UInt128 Id;
        public Dictionary<HeaderKey, HeaderValue>? Headers;
        public int Offset;
        public int Length;
    }
}
