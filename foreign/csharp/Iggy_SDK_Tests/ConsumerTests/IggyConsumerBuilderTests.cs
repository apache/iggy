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

using System.Text;
using Apache.Iggy.Consumers;
using Apache.Iggy.Contracts;
using Apache.Iggy.Encryption;
using Apache.Iggy.Enums;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Apache.Iggy.Tests.ConsumerTests;

public class IggyConsumerBuilderTests
{
    private static readonly Identifier StreamId = Identifier.Numeric(1);
    private static readonly Identifier TopicId = Identifier.Numeric(1);

    [Fact]
    public void Build_WithAutoCommitWithoutAnIntervalOrAnEvent_Throws()
    {
        var client = new Mock<IIggyClient>();
        var builder = IggyConsumerBuilder.Create(client.Object, StreamId, TopicId, Consumer.Group("group"))
            .WithPollOptions(new PollOptions { Commit = CommitPolicy.Auto(new AutoCommit()) });

        var error = Assert.Throws<InvalidOperationException>(() => builder.Build());

        Assert.Contains("PollOptions", error.Message);
    }

    [Fact]
    public void Build_WithAStandaloneConsumerWithoutPartitions_Throws()
    {
        var builder = IggyConsumerBuilder.Create(new Mock<IIggyClient>().Object, StreamId, TopicId, Consumer.New(1));

        var error = Assert.Throws<InvalidOperationException>(() => builder.Build());

        Assert.Contains("partitions", error.Message);
    }

    [Fact]
    public void Build_WithDefaultSocketBufferSizes_CreatesTheClient()
    {
        var builder = IggyConsumerBuilder
            .Create(StreamId, TopicId, Consumer.New(1))
            .WithPartitions(1)
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
        var builder = IggyConsumerBuilder
            .Create(StreamId, TopicId, Consumer.New(1))
            .WithPartitions(1)
            .WithConnection(Protocol.Tcp, "127.0.0.1:8090", "user", "pass",
                receiveBufferSize: receiveBufferSize, sendBufferSize: sendBufferSize);

        Assert.Throws<InvalidOperationException>(() => builder.Build());
    }

    [Fact]
    public void Build_WithEncryptorAndOnReadCommit_Throws()
    {
        var builder = IggyConsumerBuilder
            .Create(StreamId, TopicId, Consumer.New(1))
            .WithPartitions(1)
            .WithConnection(Protocol.Tcp, "127.0.0.1:8090", "user", "pass")
            .WithEncryptor(new AesMessageEncryptor(AesMessageEncryptor.GenerateKey()))
            .WithPollOptions(new PollOptions { Commit = CommitPolicy.OnRead });

        var ex = Assert.Throws<InvalidOperationException>(() => builder.Build());
        Assert.Contains("on read", ex.Message);
    }

    [Fact]
    public void Build_WithExternalClientEncryptorAndOnReadCommit_Throws()
    {
        var client = new Mock<IIggyClient>();
        client.SetupGet(c => c.MessageEncryptor)
            .Returns(new AesMessageEncryptor(AesMessageEncryptor.GenerateKey()));

        var builder = IggyConsumerBuilder
            .Create(client.Object, StreamId, TopicId, Consumer.New(1))
            .WithPartitions(1)
            .WithPollOptions(new PollOptions { Commit = CommitPolicy.OnRead });

        var ex = Assert.Throws<InvalidOperationException>(() => builder.Build());
        Assert.Contains("on read", ex.Message);
    }

    [Fact]
    public void Build_WithEncryptorAndClientCommit_DoesNotThrow()
    {
        var consumer = IggyConsumerBuilder
            .Create(StreamId, TopicId, Consumer.New(1))
            .WithPartitions(1)
            .WithConnection(Protocol.Tcp, "127.0.0.1:8090", "user", "pass")
            .WithEncryptor(new AesMessageEncryptor(AesMessageEncryptor.GenerateKey()))
            .WithPollOptions(new PollOptions { Commit = CommitPolicy.Auto() })
            .Build();

        Assert.NotNull(consumer);
    }

    [Fact]
    public void Build_WithPersonalAccessToken_CreatesTheClient()
    {
        var consumer = IggyConsumerBuilder
            .Create(StreamId, TopicId, Consumer.New(1))
            .WithPartitions(1)
            .WithConnection(Protocol.Tcp, "127.0.0.1:8090", "token")
            .Build();

        Assert.NotNull(consumer);
    }

    [Fact]
    public void Build_WithoutCredentials_Throws()
    {
        var builder = IggyConsumerBuilder
            .Create(StreamId, TopicId, Consumer.New(1))
            .WithPartitions(1)
            .WithConnection(Protocol.Tcp, "127.0.0.1:8090", string.Empty);

        var ex = Assert.Throws<InvalidOperationException>(() => builder.Build());
        Assert.Contains("PersonalAccessToken", ex.Message);
    }

    [Fact]
    public void TypedBuild_OverTcp_CreatesTheClient()
    {
        IggyConsumerBuilder<string> builder = IggyConsumerBuilder<string>
            .Create(StreamId, TopicId, Consumer.New(1), new StringDeserializer());
        builder.WithPartitions(1);
        builder.WithConnection(Protocol.Tcp, "127.0.0.1:8090", "user", "pass");

        Assert.NotNull(builder.Build());
    }

    [Theory]
    [InlineData(true, true)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(false, false)]
    public async Task DisposeAsync_AfterLogoutFailure_Should_DisposeOnlyOwnedClient(bool ownsClient, bool initialized)
    {
        var client = new Mock<IIggyClient>();
        client.Setup(value => value.LogoutUserAsync(It.IsAny<CancellationToken>()))
            .ThrowsAsync(new IOException("Connection lost during logout."));
        client.Setup(value => value.PollMessagesAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(),
                It.IsAny<uint>(), It.IsAny<Consumer>(), It.IsAny<PollingStrategy>(), It.IsAny<PollOptions>(),
                It.IsAny<CancellationToken>()))
            .Returns((Identifier _, Identifier _, uint _, Consumer _, PollingStrategy _, PollOptions _,
                CancellationToken token) => ParkedPollAsync(token));
        var consumer = new IggyConsumer(client.Object, new IggyConsumerConfig
        {
            CreateIggyClient = ownsClient,
            StreamId = StreamId,
            TopicId = TopicId,
            Consumer = Consumer.New(1),
            Partitions = [1]
        }, NullLoggerFactory.Instance);

        if (initialized)
        {
            await consumer.InitAsync(TestContext.Current.CancellationToken);
        }
        else
        {
            client.Setup(value => value.ConnectAsync(It.IsAny<CancellationToken>()))
                .ThrowsAsync(new IOException("Connection lost during initialization."));
            await Assert.ThrowsAsync<IOException>(() => consumer.InitAsync(TestContext.Current.CancellationToken));
        }

        await consumer.DisposeAsync();
        await consumer.DisposeAsync();

        client.Verify(value => value.Dispose(), ownsClient ? Times.Once() : Times.Never());
        client.Verify(value => value.LogoutUserAsync(It.IsAny<CancellationToken>()),
            ownsClient && initialized ? Times.Once() : Times.Never());
    }

    /// <summary>A poll that the server holds until the caller cancels it.</summary>
    private static async Task<PolledMessages> ParkedPollAsync(CancellationToken token)
    {
        await Task.Delay(Timeout.Infinite, token);
        return PolledMessages.Empty;
    }

    private sealed class StringDeserializer : IDeserializer<string>
    {
        public string Deserialize(ReadOnlyMemory<byte> data)
        {
            return Encoding.UTF8.GetString(data.Span);
        }
    }
}
