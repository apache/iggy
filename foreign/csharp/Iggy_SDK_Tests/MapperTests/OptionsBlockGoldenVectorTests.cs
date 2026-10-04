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

using System.Buffers.Binary;
using System.Text;
using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Headers;

namespace Apache.Iggy.Tests.MapperTests;

/// <summary>
///     The cross-SDK golden vector for an options block.
///
///     Rust pins the identical bytes in core/binary_protocol/src/primitives/options.rs, as do the
///     Node, Java and Go SDKs. Round-tripping a block through this SDK's own decoder proves nothing
///     about interoperability; these bytes are the contract, and a change to the TLV layout has to
///     break every copy of them together.
///
///     The vector covers Bool, Uint64 and String values in insertion order. It deliberately
///     differs from Rust's sorted map order. Decoders accept either order.
/// </summary>
public sealed class OptionsBlockGoldenVectorTests
{
    private static readonly byte[] GoldenOptionsBlock =
    [
        2, 20, 0, 0, 0,
        (byte)'p', (byte)'r', (byte)'e', (byte)'a', (byte)'l', (byte)'l', (byte)'o', (byte)'c', (byte)'a', (byte)'t', (byte)'e', (byte)'_', (byte)'s', (byte)'e', (byte)'g', (byte)'m', (byte)'e', (byte)'n', (byte)'t', (byte)'s',
        3, 1, 0, 0, 0, 1,
        2, 12, 0, 0, 0,
        (byte)'s', (byte)'e', (byte)'g', (byte)'m', (byte)'e', (byte)'n', (byte)'t', (byte)'_', (byte)'s', (byte)'i',
        (byte)'z', (byte)'e',
        12, 8, 0, 0, 0,
        0, 0, 0, 64, 0, 0, 0, 0,
        2, 10, 0, 0, 0, 100, 117, 114, 97, 98, 105, 108, 105, 116, 121, 2, 9, 0, 0, 0, 112, 101, 114, 115, 105, 115, 116, 101, 100
    ];

    [Fact]
    public void WriteHeadersTo_EncodesTheCrossSdkGoldenVector()
    {
        var options = new Dictionary<HeaderKey, HeaderValue>
        {
            [HeaderKey.FromString("preallocate_segments")] = HeaderValue.FromBool(true),
            [HeaderKey.FromString("segment_size")] = HeaderValue.FromUInt64(1_073_741_824),
            [HeaderKey.FromString("durability")] = HeaderValue.FromString("persisted")
        };

        var encoded = new byte[TcpContracts.HeadersByteLength(options)];
        TcpContracts.WriteHeadersTo(encoded, options);

        Assert.Equal(GoldenOptionsBlock, encoded);
    }

    [Fact]
    public void MapTopic_DecodesTheCrossSdkGoldenVector()
    {
        var topic = Mappers.BinaryMapper.MapTopic(TopicPayloadWithOptions(GoldenOptionsBlock));

        Assert.NotNull(topic.Options);
        Assert.Equal(3, topic.Options.Count);
        Assert.Equal("persisted", topic.Options[HeaderKey.FromString("durability")].ToString());
        Assert.True(topic.Options[HeaderKey.FromString("preallocate_segments")].ToBool());
        Assert.Equal(1_073_741_824UL, topic.Options[HeaderKey.FromString("segment_size")].ToUInt64());
        Assert.Equal(HeaderKind.Bool, topic.Options[HeaderKey.FromString("preallocate_segments")].Kind);
        Assert.Equal(HeaderKind.Uint64, topic.Options[HeaderKey.FromString("segment_size")].Kind);
        Assert.Empty(topic.DerivedOptions!);
    }

    [Fact]
    public void MapTopic_WithOptionsLengthPrefixCutShort_ThrowsMalformedResponse()
    {
        var payload = TopicPayloadWithOptions(GoldenOptionsBlock);
        var truncated = payload[..(50 + "topic".Length + 2)];

        Assert.Throws<MalformedResponseException>(() => Mappers.BinaryMapper.MapTopic(truncated));
    }

    /// <summary>
    ///     A topic response carrying <paramref name="options" /> as its explicit block and an empty
    ///     derived block, with no partitions after it.
    /// </summary>
    private static byte[] TopicPayloadWithOptions(byte[] options)
    {
        var name = Encoding.UTF8.GetBytes("topic");
        var payload = new byte[50 + name.Length + 4 + options.Length + 4];

        BinaryPrimitives.WriteUInt32LittleEndian(payload, 1);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(4), 1750000000000000);
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(12), 1);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(16), 0);
        payload[24] = 1;
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(25), 0);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(33), 0);
        BinaryPrimitives.WriteUInt64LittleEndian(payload.AsSpan(41), 0);
        payload[49] = (byte)name.Length;
        name.CopyTo(payload.AsSpan(50));

        var position = 50 + name.Length;
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(position), (uint)options.Length);
        options.CopyTo(payload.AsSpan(position + 4));
        BinaryPrimitives.WriteUInt32LittleEndian(payload.AsSpan(position + 4 + options.Length), 0);

        return payload;
    }
}
