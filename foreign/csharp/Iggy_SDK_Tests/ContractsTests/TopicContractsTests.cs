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
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Headers;

namespace Apache.Iggy.Tests.ContractsTests;

public sealed class TopicContractsTests
{
    private const string NonAsciiName = "temat-żółć-日本語";
    private const uint StreamId = 42;

    [Fact]
    public void CreateTopic_WithANonAsciiName_PrefixesTheUtf8ByteCount()
    {
        var options = new Dictionary<string, HeaderValue>
        {
            ["segment_size"] = HeaderValue.FromUInt64(1024 * 1024)
        };

        var bytes = TcpContracts.CreateTopic(Identifier.Numeric(StreamId), NonAsciiName, 3, options);

        // [stream id: kind + length + 4][partition count: 4][name length: 1][name][options]
        Assert.Equal(3u, BinaryPrimitives.ReadUInt32LittleEndian(bytes.AsSpan(6, 4)));
        AssertNameAndOptions(bytes, 10, NonAsciiName, options);
    }

    [Fact]
    public void UpdateTopic_WithANonAsciiName_SetsTheNameInTheOptionsBlock()
    {
        var update = new TopicUpdate { Name = NonAsciiName, MaxTopicSize = 2_000_000 };

        var bytes = TcpContracts.UpdateTopic(Identifier.Numeric(StreamId), Identifier.Numeric(7), update.ToPatch());

        // [scope: 1][stream id: kind + length + 4][topic id: kind + length + 4][reset count: 1][options]
        Assert.Equal((byte)OptionsScope.Topic, bytes[0]);
        Assert.Equal(0, bytes[13]);
        var options = Mappers.BinaryMapper.MapHeaders(bytes.AsSpan(14));
        Assert.Equal(NonAsciiName, options[HeaderKey.FromString("name")].ToString());
        Assert.Equal(2_000_000UL, options[HeaderKey.FromString("max_topic_size")].ToUInt64());
    }

    [Fact]
    public void CreateTopic_WithANameOverTheWireLimitInBytes_Throws()
    {
        // 129 characters, but 258 UTF-8 bytes: a character count would let this through.
        var name = new string('ż', 129);

        Assert.Throws<ArgumentException>(() => TcpContracts.CreateTopic(Identifier.Numeric(StreamId), name, 1));
    }

    [Fact]
    public void CreateTopic_WithOptionsTooLargeForTheStack_SerializesThemAll()
    {
        var options = new Dictionary<string, HeaderValue>();
        for (var i = 0; i < 1024; i++)
        {
            options[$"option_{i}"] = HeaderValue.FromUInt64((ulong)i);
        }

        var bytes = TcpContracts.CreateTopic(Identifier.Numeric(StreamId), "topic", 1, options);

        AssertNameAndOptions(bytes, 10, "topic", options);
    }

    private static void AssertNameAndOptions(byte[] bytes, int namePosition, string expectedName,
        Dictionary<string, HeaderValue> expectedOptions)
    {
        var nameBytes = Encoding.UTF8.GetBytes(expectedName);
        Assert.Equal(nameBytes.Length, bytes[namePosition]);
        Assert.Equal(expectedName, Encoding.UTF8.GetString(bytes, namePosition + 1, nameBytes.Length));

        var options = Mappers.BinaryMapper.MapHeaders(bytes.AsSpan(namePosition + 1 + nameBytes.Length));
        Assert.Equal(expectedOptions.Count, options.Count);
        foreach (var (key, value) in expectedOptions)
        {
            var decoded = options[HeaderKey.FromString(key)];
            Assert.Equal(value.Kind, decoded.Kind);
            Assert.Equal(value.Value, decoded.Value);
        }
    }
}
