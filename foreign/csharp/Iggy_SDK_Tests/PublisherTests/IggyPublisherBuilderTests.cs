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
using System.Text;
using Apache.Iggy.Enums;
using Apache.Iggy.Publishers;

namespace Apache.Iggy.Tests.PublisherTests;

public class IggyPublisherBuilderTests
{
    private static readonly Identifier StreamId = Identifier.Numeric(1);
    private static readonly Identifier TopicId = Identifier.Numeric(1);

    [Fact]
    public void Build_WithDefaultSocketBufferSizes_CreatesTheClient()
    {
        var builder = IggyPublisherBuilder
            .Create(StreamId, TopicId)
            .WithConnection(Protocol.Tcp, "127.0.0.1:8090", "user", "pass");

        Assert.Null(builder.Config.ReceiveBufferSize);
        Assert.Null(builder.Config.SendBufferSize);
        Assert.NotNull(builder.Build());
    }

    [Theory]
    [InlineData(0, null)]
    [InlineData(-1, null)]
    [InlineData(null, 0)]
    [InlineData(null, -1)]
    public void Build_WithNonPositiveSocketBufferSize_Throws(int? receiveBufferSize, int? sendBufferSize)
    {
        var builder = IggyPublisherBuilder
            .Create(StreamId, TopicId)
            .WithConnection(Protocol.Tcp, "127.0.0.1:8090", "user", "pass",
                receiveBufferSize: receiveBufferSize, sendBufferSize: sendBufferSize);

        Assert.Throws<InvalidOperationException>(() => builder.Build());
    }

    [Fact]
    public void TypedBuild_OverTcp_CreatesTheClient()
    {
        IggyPublisherBuilder<string> builder
            = IggyPublisherBuilder<string>.Create(StreamId, TopicId, new StringSerializer());
        builder.WithConnection(Protocol.Tcp, "127.0.0.1:8090", "user", "pass");

        Assert.NotNull(builder.Build());
    }

    private sealed class StringSerializer : ISerializer<string>
    {
        public void Serialize(string data, IBufferWriter<byte> writer)
        {
            writer.Write(Encoding.UTF8.GetBytes(data));
        }
    }
}
