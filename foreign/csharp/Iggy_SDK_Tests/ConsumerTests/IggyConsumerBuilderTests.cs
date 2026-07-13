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

using Apache.Iggy.Consumers;
using Apache.Iggy.Encryption;
using Apache.Iggy.Enums;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Moq;

namespace Apache.Iggy.Tests.ConsumerTests;

public class IggyConsumerBuilderTests
{
    private static readonly Identifier StreamId = Identifier.Numeric(1);
    private static readonly Identifier TopicId = Identifier.Numeric(1);

    [Fact]
    public void Build_WithEncryptorAndAutoCommit_Throws()
    {
        var builder = IggyConsumerBuilder
            .Create(StreamId, TopicId, Consumer.New(1))
            .WithConnection(Protocol.Tcp, "127.0.0.1:8090", "user", "pass")
            .WithEncryptor(new AesMessageEncryptor(AesMessageEncryptor.GenerateKey()))
            .WithAutoCommitMode(AutoCommitMode.Auto);

        var ex = Assert.Throws<InvalidOperationException>(() => builder.Build());
        Assert.Contains("AutoCommitMode.Auto", ex.Message);
    }

    [Fact]
    public void Build_WithExternalClientEncryptorAndAutoCommit_Throws()
    {
        var client = new Mock<IIggyClient>();
        client.SetupGet(c => c.MessageEncryptor)
            .Returns(new AesMessageEncryptor(AesMessageEncryptor.GenerateKey()));

        var builder = IggyConsumerBuilder
            .Create(client.Object, StreamId, TopicId, Consumer.New(1))
            .WithAutoCommitMode(AutoCommitMode.Auto);

        var ex = Assert.Throws<InvalidOperationException>(() => builder.Build());
        Assert.Contains("AutoCommitMode.Auto", ex.Message);
    }

    [Fact]
    public void Build_WithEncryptorAndAfterReceiveCommit_DoesNotThrow()
    {
        var consumer = IggyConsumerBuilder
            .Create(StreamId, TopicId, Consumer.New(1))
            .WithConnection(Protocol.Tcp, "127.0.0.1:8090", "user", "pass")
            .WithEncryptor(new AesMessageEncryptor(AesMessageEncryptor.GenerateKey()))
            .WithAutoCommitMode(AutoCommitMode.AfterReceive)
            .Build();

        Assert.NotNull(consumer);
    }
}
