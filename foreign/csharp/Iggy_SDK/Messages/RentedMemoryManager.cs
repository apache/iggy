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
///     Owns a buffer rented from <see cref="ArrayPool{T}" /> and exposes it as <see cref="Memory{T}" /> guarded
///     against use-after-dispose: <c>Span</c> access goes through <see cref="GetSpan" />, which throws
///     <see cref="ObjectDisposedException" /> once the buffer has been returned to the pool, where a plain
///     <c>buffer.AsMemory()</c> would silently read recycled pool memory.
/// </summary>
internal sealed class RentedMemoryManager : MemoryManager<byte>
{
    private readonly int _length;
    private byte[]? _buffer;

    /// <summary>
    ///     Takes ownership of <paramref name="buffer" />, exposing its first <paramref name="length" /> bytes.
    /// </summary>
    /// <param name="buffer">A buffer rented from <see cref="ArrayPool{T}.Shared" />.</param>
    /// <param name="length">Number of valid bytes at the start of <paramref name="buffer" />.</param>
    public RentedMemoryManager(byte[] buffer, int length)
    {
        _buffer = buffer;
        _length = length;
    }

    /// <inheritdoc />
    public override Span<byte> GetSpan()
    {
        var buffer = _buffer ?? throw new ObjectDisposedException(nameof(RentedMemoryManager));
        return buffer.AsSpan(0, _length);
    }

    /// <inheritdoc />
    public override MemoryHandle Pin(int elementIndex = 0)
    {
        throw new NotSupportedException("Memory backed by a pooled batch buffer cannot be pinned.");
    }

    /// <inheritdoc />
    public override void Unpin()
    {
    }

    /// <summary>
    ///     Returns the buffer to the pool exactly once, even under racing disposes.
    /// </summary>
    protected override void Dispose(bool disposing)
    {
        var buffer = Interlocked.Exchange(ref _buffer, null);
        if (buffer is not null)
        {
            // Clear on return so payloads (possibly plaintext) do not leak into later rents from the shared pool.
            ArrayPool<byte>.Shared.Return(buffer, true);
        }
    }
}
