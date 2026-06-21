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
using Apache.Iggy.Messages;

namespace Apache.Iggy.Tests.UtilityTests;

public class RentedMessageBatchTests
{
    [Fact]
    public void Build_EmptyBuilder_ProducesEmptyBatch()
    {
        using var builder = new RentedMessageBatchBuilder();

        using var batch = builder.Build();

        Assert.Empty(batch.Messages);
    }

    [Fact]
    public void Add_IncrementsCount()
    {
        using var builder = new RentedMessageBatchBuilder();

        builder.Add(new byte[] { 1 });
        builder.Add(new byte[] { 2 });

        Assert.Equal(2, builder.Count);
    }

    [Fact]
    public void Build_PreservesPayloadOrderAndContent()
    {
        var payloads = new[] { new byte[] { 1, 1, 1 }, new byte[] { 2, 2 }, new byte[] { 3, 3, 3, 3 } };

        using var builder = new RentedMessageBatchBuilder();
        foreach (var payload in payloads)
        {
            builder.Add(payload);
        }

        using var batch = builder.Build();

        Assert.Equal(payloads.Length, batch.Messages.Count);
        for (var i = 0; i < payloads.Length; i++)
        {
            Assert.Equal(payloads[i], batch.Messages[i].Payload.ToArray());
        }
    }

    [Fact]
    public void Build_WithPayloadsExceedingInitialCapacity_SlicesEachCorrectly()
    {
        // Small hint forces the shared buffer to grow mid-batch.
        var payloads = new[] { Filled(100, 0xA1), Filled(200, 0xB2), Filled(50, 0xC3), Filled(300, 0xD4) };

        using var builder = new RentedMessageBatchBuilder(8);
        foreach (var payload in payloads)
        {
            builder.Add(payload);
        }

        using var batch = builder.Build();

        for (var i = 0; i < payloads.Length; i++)
        {
            Assert.Equal(payloads[i], batch.Messages[i].Payload.ToArray());
        }
    }

    [Fact]
    public void Add_CallbackAndSpanMixed_BothMaterialize()
    {
        using var builder = new RentedMessageBatchBuilder();
        builder.Add(new byte[] { 1, 2, 3 });
        builder.Add(0, static (_, writer) =>
        {
            Span<byte> span = stackalloc byte[] { 9, 8, 7 };
            span.CopyTo(writer.GetSpan(span.Length));
            writer.Advance(span.Length);
        });

        using var batch = builder.Build();

        Assert.Equal(new byte[] { 1, 2, 3 }, batch.Messages[0].Payload.ToArray());
        Assert.Equal(new byte[] { 9, 8, 7 }, batch.Messages[1].Payload.ToArray());
    }

    [Fact]
    public void Add_WithExplicitId_SetsHeaderId()
    {
        var id = Guid.NewGuid();

        using var builder = new RentedMessageBatchBuilder();
        builder.Add(new byte[] { 1 }, id);

        using var batch = builder.Build();

        Assert.Equal(id.ToUInt128(), batch.Messages[0].Header.Id);
    }

    [Fact]
    public void Add_WithoutId_LeavesZeroIdForServerAssignment()
    {
        using var builder = new RentedMessageBatchBuilder();
        builder.Add(new byte[] { 1 });
        builder.Add(new byte[] { 2 });

        using var batch = builder.Build();

        Assert.Equal(UInt128.Zero, batch.Messages[0].Header.Id);
        Assert.Equal(UInt128.Zero, batch.Messages[1].Header.Id);
    }

    [Fact]
    public void Add_WithUserHeaders_AttachesPerMessage()
    {
        var headers = new Dictionary<HeaderKey, HeaderValue>
        {
            [HeaderKey.FromString("k")] = HeaderValue.FromString("v")
        };

        using var builder = new RentedMessageBatchBuilder();
        builder.Add(new byte[] { 1 }, userHeaders: headers);
        builder.Add(new byte[] { 2 });

        using var batch = builder.Build();

        Assert.Same(headers, batch.Messages[0].UserHeaders);
        Assert.Null(batch.Messages[1].UserHeaders);
    }

    [Fact]
    public void Add_NullCallback_Throws()
    {
        using var builder = new RentedMessageBatchBuilder();

        Assert.Throws<ArgumentNullException>(() =>
            builder.Add(0, (Action<int, IBufferWriter<byte>>)null!));
    }

    [Fact]
    public void Add_AfterBuild_Throws()
    {
        var builder = new RentedMessageBatchBuilder();
        builder.Add(new byte[] { 1 });
        using var batch = builder.Build();

        Assert.Throws<InvalidOperationException>(() => builder.Add(new byte[] { 2 }));
    }

    [Fact]
    public void Build_Twice_Throws()
    {
        var builder = new RentedMessageBatchBuilder();
        builder.Add(new byte[] { 1 });
        using var batch = builder.Build();

        Assert.Throws<InvalidOperationException>(() => builder.Build());
    }

    [Fact]
    public void BuilderDispose_AfterBuild_LeavesBatchUsable()
    {
        var payload = new byte[] { 7, 7, 7 };
        var builder = new RentedMessageBatchBuilder();
        builder.Add(payload);
        using var batch = builder.Build();

        builder.Dispose();

        Assert.Equal(payload, batch.Messages[0].Payload.ToArray());
    }

    [Fact]
    public void BatchDispose_IsIdempotent()
    {
        var builder = new RentedMessageBatchBuilder();
        builder.Add(new byte[] { 1 });
        var batch = builder.Build();

        batch.Dispose();
        batch.Dispose();
    }

    [Fact]
    public void Messages_AfterBatchDispose_Throws()
    {
        var builder = new RentedMessageBatchBuilder();
        builder.Add(new byte[] { 1 });
        var batch = builder.Build();

        batch.Dispose();

        Assert.Throws<ObjectDisposedException>(() => _ = batch.Messages);
    }

    [Fact]
    public void PayloadSpan_AfterBatchDispose_Throws()
    {
        var builder = new RentedMessageBatchBuilder();
        builder.Add(new byte[] { 1, 2, 3 });
        var batch = builder.Build();

        // Captured before dispose, so the getter guard cannot help; the memory itself must reject the read.
        var message = batch.Messages[0];
        batch.Dispose();

        Assert.Throws<ObjectDisposedException>(() => _ = message.Payload.Span[0]);
        Assert.Throws<ObjectDisposedException>(() => message.Payload.ToArray());
    }

    [Fact]
    public void PayloadSpan_BeforeBatchDispose_StillReadable()
    {
        var builder = new RentedMessageBatchBuilder();
        builder.Add(new byte[] { 1, 2, 3 });
        using var batch = builder.Build();

        Assert.Equal(new byte[] { 1, 2, 3 }, batch.Messages[0].Payload.ToArray());
    }

    [Fact]
    public void Add_AfterBuilderDispose_Throws()
    {
        var builder = new RentedMessageBatchBuilder();
        builder.Dispose();

        Assert.Throws<ObjectDisposedException>(() => builder.Add(new byte[] { 1 }));
    }

    [Fact]
    public void Build_AfterBuilderDispose_Throws()
    {
        var builder = new RentedMessageBatchBuilder();
        builder.Add(new byte[] { 1 });
        builder.Dispose();

        Assert.Throws<ObjectDisposedException>(() => builder.Build());
    }

    private static byte[] Filled(int length, byte value)
    {
        var array = new byte[length];
        Array.Fill(array, value);
        return array;
    }
}
