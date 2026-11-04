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
using Apache.Iggy.Encryption;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Kinds;
using Apache.Iggy.Mappers;
using Apache.Iggy.Vsr;

namespace Apache.Iggy.Contracts;

/// <summary>
///     The reply of a fetch: one section per target that the server read or refused, in reply order. A target
///     without a section keeps its position. The records stay undecoded until a section is decoded.
/// </summary>
/// <remarks>
///     The reply holds one rented buffer. <see cref="FetchedSection.Records" /> and the rentals of
///     <see cref="FetchedSection.DecodeRented" /> are slices of it, so the buffer goes back to the pool once the
///     reply and every such rental are disposed.
/// </remarks>
internal sealed class FetchedMessages : IDisposable
{
    internal const int SECTIONS_COUNT_SIZE = sizeof(uint);
    internal const int SECTION_HEADER_SIZE = 56;

    private readonly IMemoryOwner<byte> _reply;
    private int _references = 1;
    private int _disposed;

    internal FetchedMessages(IMemoryOwner<byte> reply)
    {
        _reply = reply;
    }

    /// <summary>The sections in reply order.</summary>
    public IReadOnlyList<FetchedSection> Sections { get; internal set; } = [];

    /// <summary>The bytes of a reply before its records: the section count and one header per section.</summary>
    internal static int HeadSize(int sections)
    {
        return SECTIONS_COUNT_SIZE + sections * SECTION_HEADER_SIZE;
    }

    /// <summary>Releases the reference of the reply on its buffer.</summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 0)
        {
            Release();
        }
    }

    /// <summary>A reference on the reply buffer for one rental, released when the rental is disposed.</summary>
    /// <exception cref="ObjectDisposedException">The reply was disposed.</exception>
    internal IMemoryOwner<byte> Acquire()
    {
        while (true)
        {
            var references = Volatile.Read(ref _references);
            ObjectDisposedException.ThrowIf(references == 0 || Volatile.Read(ref _disposed) == 1, this);
            if (Interlocked.CompareExchange(ref _references, references + 1, references) == references)
            {
                return new Reference(this);
            }
        }
    }

    private void Release()
    {
        if (Interlocked.Decrement(ref _references) == 0)
        {
            _reply.Dispose();
        }
    }

    private sealed class Reference(FetchedMessages reply) : IMemoryOwner<byte>
    {
        private int _disposed;

        public Memory<byte> Memory => reply._reply.Memory;

        public void Dispose()
        {
            if (Interlocked.Exchange(ref _disposed, 1) == 0)
            {
                reply.Release();
            }
        }
    }
}

/// <summary>What a fetch read from one target, or the status that refused it.</summary>
internal sealed class FetchedSection
{
    private readonly IMessageEncryptor? _encryptor;
    private readonly FetchedMessages _reply;

    internal FetchedSection(FetchedMessages reply, IMessageEncryptor? encryptor)
    {
        _reply = reply;
        _encryptor = encryptor;
    }

    /// <summary>The partition of the target.</summary>
    public required uint PartitionId { get; init; }

    /// <summary>
    ///     Zero when the server read the partition. Otherwise the status code that refused the target, such as 89
    ///     (NotPrimary) or 5009 (ConsumerGroupPartitionNotOwned). Local status 14010 carries <see cref="ProgressError" />
    ///     and blocks the target until explicit abandonment. A refused section carries no records or position.
    /// </summary>
    public required uint Status { get; init; }

    /// <summary>Local recovery for an uncertain committing section. Healthy sibling sections remain usable.</summary>
    public VsrFetchProgressUncertainException? ProgressError { get; init; }

    /// <summary>The current offset of the partition, as a poll reports it.</summary>
    public required ulong CurrentOffset { get; init; }

    /// <summary>
    ///     Where the next fetch of the partition starts. Send the target next as <see cref="PollingStrategy.Offset" />
    ///     of this value with <see cref="Context" />. A section of a <see cref="PollingStrategy.Next" /> target
    ///     carries it also when it holds no messages.
    /// </summary>
    public required ulong NextOffset { get; init; }

    /// <summary>The number of messages in <see cref="Records" />.</summary>
    public required uint MessagesCount { get; init; }

    /// <summary>The partition incarnation and owner that the server read the partition under.</summary>
    public required PartitionContext Context { get; init; }

    /// <summary>
    ///     The undecoded batch records, as a poll reply carries them after its header. They are a slice of the
    ///     reply buffer, valid until the reply is disposed.
    /// </summary>
    public required ReadOnlyMemory<byte> Records { get; init; }

    /// <summary>Decodes the messages into copies that stay valid after the reply is disposed.</summary>
    /// <exception cref="IggyInvalidStatusCodeException">The status of a refused section.</exception>
    /// <exception cref="VsrFetchProgressUncertainException">The section requires explicit recovery.</exception>
    /// <exception cref="MessageDecryptionException">A message does not decrypt with the client's encryptor.</exception>
    /// <exception cref="ObjectDisposedException">The reply was disposed.</exception>
    public PolledMessages Decode()
    {
        using var rental = DecodeRented();
        return BinaryMapper.MaterializeMessages(rental);
    }

    /// <summary>
    ///     Decodes the messages as slices of the reply buffer, decrypted into a buffer of their own when the client
    ///     has an encryptor. The rental keeps the reply buffer rented until it is disposed.
    /// </summary>
    /// <exception cref="IggyInvalidStatusCodeException">The status of a refused section.</exception>
    /// <exception cref="VsrFetchProgressUncertainException">The section requires explicit recovery.</exception>
    /// <exception cref="MessageDecryptionException">A message does not decrypt with the client's encryptor.</exception>
    /// <exception cref="ObjectDisposedException">The reply was disposed.</exception>
    public PolledMessagesRental DecodeRented()
    {
        if (Status != 0)
        {
            throw Refusal();
        }

        var reference = _reply.Acquire();
        try
        {
            return BinaryMapper.MapRentedRecords(Records, reference, PartitionId, CurrentOffset, MessagesCount,
                Context, _encryptor);
        }
        catch
        {
            reference.Dispose();
            throw;
        }
    }

    /// <summary>The refusal of a section with a nonzero <see cref="Status" />, as the server reported it.</summary>
    internal Exception Refusal()
    {
        return ProgressError ?? (Exception)VsrError.FromServer((int)Status, $"The fetch of partition {PartitionId} was refused.");
    }
}

internal sealed class FetchProgressReply(IMemoryOwner<byte> reply, ulong operationId, IReadOnlySet<uint> partitions)
    : IMemoryOwner<byte>
{
    internal ulong OperationId { get; } = operationId;
    internal IReadOnlySet<uint> Partitions { get; } = partitions;
    public Memory<byte> Memory => reply.Memory;
    public void Dispose() => reply.Dispose();
}
