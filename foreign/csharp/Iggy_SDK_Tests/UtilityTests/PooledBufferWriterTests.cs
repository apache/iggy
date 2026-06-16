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
using System.Runtime.InteropServices;
using Apache.Iggy.Messages;

namespace Apache.Iggy.Tests.UtilityTests;

public class PooledBufferWriterTests
{
    private const int BufferSize = 4096;

    [Fact]
    public void Write_ThenWritten_ReturnsExactBytesAndCount()
    {
        using var writer = new PooledBufferWriter();
        var payload = new byte[] { 1, 2, 3, 4, 5 };

        payload.CopyTo(writer.GetSpan(payload.Length));
        writer.Advance(payload.Length);

        Assert.Equal(payload.Length, writer.WrittenCount);
        Assert.Equal(payload, writer.Written.ToArray());
    }

    [Fact]
    public void GetSpan_ReturnsSpanAtLeastSizeHint()
    {
        using var writer = new PooledBufferWriter(4);

        Assert.True(writer.GetSpan(1000).Length >= 1000);
    }

    [Fact]
    public void Write_BeyondCapacity_GrowsAndPreservesEarlierBytes()
    {
        using var writer = new PooledBufferWriter(4);
        var first = GetUnderlyingArray(writer);
        var capacity = first.Length;

        Span<byte> head = writer.GetSpan(capacity);
        for (var i = 0; i < capacity; i++)
        {
            head[i] = (byte)(i % 251);
        }

        writer.Advance(capacity);

        // One more byte forces a re-rent + copy.
        writer.GetSpan(1)[0] = 0xAB;
        writer.Advance(1);

        var grown = GetUnderlyingArray(writer);
        Assert.NotSame(first, grown);

        var written = writer.Written.ToArray();
        Assert.Equal(capacity + 1, written.Length);
        for (var i = 0; i < capacity; i++)
        {
            Assert.Equal((byte)(i % 251), written[i]);
        }

        Assert.Equal(0xAB, written[capacity]);
    }

    [Fact]
    public void Advance_NegativeCount_Throws()
    {
        using var writer = new PooledBufferWriter();

        Assert.Throws<ArgumentOutOfRangeException>(() => writer.Advance(-1));
    }

    [Fact]
    public void Advance_BeyondCapacity_Throws()
    {
        using var writer = new PooledBufferWriter(4);
        var capacity = writer.GetSpan().Length;

        Assert.Throws<ArgumentOutOfRangeException>(() => writer.Advance(capacity + 1));
    }

    [Fact]
    public void Dispose_ReturnsBufferToPool()
    {
        var writer = new PooledBufferWriter(BufferSize);
        var underlying = GetUnderlyingArray(writer);
        writer.Dispose();

        var rerented = ArrayPool<byte>.Shared.Rent(underlying.Length);
        try
        {
            Assert.Same(underlying, rerented);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rerented);
        }
    }

    [Fact]
    public void Dispose_IsIdempotent()
    {
        var writer = new PooledBufferWriter();
        writer.Dispose();
        writer.Dispose();
        writer.Dispose();
    }

    [Fact]
    public void Written_AfterDispose_Throws()
    {
        var writer = new PooledBufferWriter();
        writer.Dispose();

        Assert.Throws<ObjectDisposedException>(() => _ = writer.Written);
    }

    [Fact]
    public void Dispose_ResetsWrittenCount()
    {
        var writer = new PooledBufferWriter();
        writer.GetSpan(4);
        writer.Advance(4);

        writer.Dispose();

        Assert.Equal(0, writer.WrittenCount);
        Assert.Throws<ArgumentOutOfRangeException>(() => writer.Rewind(4));
    }

    [Fact]
    public void DetachBuffer_TransfersOwnership()
    {
        var writer = new PooledBufferWriter(BufferSize);
        var underlying = GetUnderlyingArray(writer);

        var detached = writer.DetachBuffer();

        Assert.Same(underlying, detached);
        // Writer must stay disposed so a later Dispose cannot return the detached buffer.
        Assert.Throws<ObjectDisposedException>(() => _ = writer.Written);
        writer.Dispose();

        var rerented = ArrayPool<byte>.Shared.Rent(underlying.Length);
        try
        {
            Assert.NotSame(underlying, rerented);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rerented);
        }
    }

    [Fact]
    public void DetachBuffer_AfterDispose_Throws()
    {
        var writer = new PooledBufferWriter();
        writer.Dispose();

        Assert.Throws<ObjectDisposedException>(() => writer.DetachBuffer());
    }

    private static byte[] GetUnderlyingArray(PooledBufferWriter writer)
    {
        if (!MemoryMarshal.TryGetArray<byte>(writer.GetMemory(), out ArraySegment<byte> segment) ||
            segment.Array is null)
        {
            throw new InvalidOperationException("PooledBufferWriter must be array-backed.");
        }

        return segment.Array;
    }
}
