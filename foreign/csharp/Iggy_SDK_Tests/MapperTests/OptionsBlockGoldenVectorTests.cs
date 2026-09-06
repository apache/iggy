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
///     enforce_fsync (a one-byte Bool) and segment_size (an eight-byte Uint64) cover both value
///     widths. What the vector pins is the per-entry byte layout, not a key order: these two land
///     sorted only because the Rust core holds options in a BTreeMap, and the server accepts the
///     insertion order this SDK emits.
/// </summary>
public sealed class OptionsBlockGoldenVectorTests
{
    private static readonly byte[] GoldenOptionsBlock =
    [
        2, 13, 0, 0, 0,
        (byte)'e', (byte)'n', (byte)'f', (byte)'o', (byte)'r', (byte)'c', (byte)'e', (byte)'_', (byte)'f', (byte)'s',
        (byte)'y', (byte)'n', (byte)'c',
        3, 1, 0, 0, 0, 1,
        2, 12, 0, 0, 0,
        (byte)'s', (byte)'e', (byte)'g', (byte)'m', (byte)'e', (byte)'n', (byte)'t', (byte)'_', (byte)'s', (byte)'i',
        (byte)'z', (byte)'e',
        12, 8, 0, 0, 0,
        0, 0, 0, 64, 0, 0, 0, 0
    ];

    [Fact]
    public void WriteHeadersTo_EncodesTheCrossSdkGoldenVector()
    {
        var options = new Dictionary<HeaderKey, HeaderValue>
        {
            [HeaderKey.FromString("enforce_fsync")] = HeaderValue.FromBool(true),
            [HeaderKey.FromString("segment_size")] = HeaderValue.FromUInt64(1_073_741_824)
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
        Assert.Equal(2, topic.Options.Count);
        Assert.True(topic.Options[HeaderKey.FromString("enforce_fsync")].ToBool());
        Assert.Equal(1_073_741_824UL, topic.Options[HeaderKey.FromString("segment_size")].ToUInt64());
        Assert.Equal(HeaderKind.Bool, topic.Options[HeaderKey.FromString("enforce_fsync")].Kind);
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
