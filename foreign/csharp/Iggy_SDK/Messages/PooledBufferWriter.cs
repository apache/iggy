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

namespace Apache.Iggy.Messages;

/// <summary>
///     A growable <see cref="IBufferWriter{T}" /> backed by a buffer rented from
///     <see cref="ArrayPool{T}" />.
/// </summary>
/// <remarks>
///     Growth re-rents the backing buffer, so any <see cref="Memory{T}" /> or span obtained before a write
///     may be invalidated by a subsequent write. Read <see cref="Written" /> only after all writes complete.
/// </remarks>
internal sealed class PooledBufferWriter : IBufferWriter<byte>, IDisposable
{
    private byte[]? _buffer;

    /// <summary>
    ///     The bytes written so far. Valid only until the next write (growth may move the buffer) and until
    ///     <see cref="Dispose" /> (which returns the buffer to the pool).
    /// </summary>
    public ReadOnlyMemory<byte> Written => Buffer.AsMemory(0, WrittenCount);

    /// <summary>
    ///     Number of bytes written so far.
    /// </summary>
    public int WrittenCount { get; private set; }

    /// <summary>
    ///     Capacity of the currently rented backing buffer. Lets a long-lived reused writer be discarded
    ///     once it has grown past a sensible retained size.
    /// </summary>
    public int Capacity => Buffer.Length;

    private byte[] Buffer => _buffer ?? throw new ObjectDisposedException(nameof(PooledBufferWriter));

    /// <summary>
    ///     Initializes a new instance of the <see cref="PooledBufferWriter" /> class.
    /// </summary>
    /// <param name="sizeHint">Initial capacity hint for the rented buffer.</param>
    public PooledBufferWriter(int sizeHint = 1024)
    {
        _buffer = ArrayPool<byte>.Shared.Rent(Math.Max(sizeHint, 1));
    }

    /// <inheritdoc />
    public void Advance(int count)
    {
        // Subtraction form so a huge count cannot wrap the addition and slip past the check.
        if (count < 0 || count > Buffer.Length - WrittenCount)
        {
            throw new ArgumentOutOfRangeException(nameof(count));
        }

        WrittenCount += count;
    }

    /// <inheritdoc />
    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return Buffer.AsMemory(WrittenCount);
    }

    /// <inheritdoc />
    public Span<byte> GetSpan(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return Buffer.AsSpan(WrittenCount);
    }

    /// <summary>
    ///     Returns the rented buffer to the pool. Any memory previously exposed by <see cref="Written" />,
    ///     <see cref="GetMemory" />, or <see cref="GetSpan" /> is invalid after this call.
    /// </summary>
    public void Dispose()
    {
        var buffer = _buffer;
        if (buffer is null)
        {
            return;
        }

        _buffer = null;
        WrittenCount = 0;
        // Clear on return so payloads (possibly plaintext) do not leak into later rents from the shared pool.
        ArrayPool<byte>.Shared.Return(buffer, true);
    }

    /// <summary>
    ///     Hands ownership of the rented buffer to the caller, who must return it to
    ///     <see cref="ArrayPool{T}.Shared" />. Leaves the writer disposed, so a later <see cref="Dispose" />
    ///     cannot return the detached buffer behind the new owner's back.
    /// </summary>
    public byte[] DetachBuffer()
    {
        var buffer = Buffer;
        _buffer = null;
        WrittenCount = 0;
        return buffer;
    }

    /// <summary>
    ///     Rewinds the writer back to a previously observed <see cref="WrittenCount" />, discarding everything
    ///     written since while keeping the rented buffer. Memory previously exposed by <see cref="Written" />,
    ///     <see cref="GetMemory" />, or <see cref="GetSpan" /> must no longer be used.
    /// </summary>
    /// <param name="count">A <see cref="WrittenCount" /> value captured earlier; must not exceed the current count.</param>
    public void Rewind(int count)
    {
        if (count < 0 || count > WrittenCount)
        {
            throw new ArgumentOutOfRangeException(nameof(count));
        }

        WrittenCount = count;
    }

    private void EnsureCapacity(int sizeHint)
    {
        var required = (long)WrittenCount + Math.Max(sizeHint, 1);
        if (required <= Buffer.Length)
        {
            return;
        }

        if (required > Array.MaxLength)
        {
            throw new OutOfMemoryException(
                $"PooledBufferWriter cannot grow to {required} bytes (max {Array.MaxLength}).");
        }

        var doubled = Math.Min((long)Buffer.Length * 2, Array.MaxLength);
        var grown = ArrayPool<byte>.Shared.Rent((int)Math.Max(doubled, required));
        Buffer.AsSpan(0, WrittenCount).CopyTo(grown);
        ArrayPool<byte>.Shared.Return(_buffer!, true);
        _buffer = grown;
    }
}
