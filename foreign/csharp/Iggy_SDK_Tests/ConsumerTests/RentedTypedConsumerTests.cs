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
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;

namespace Apache.Iggy.Tests.ConsumerTests;

public class RentedTypedConsumerTests
{
    [Fact]
    public async Task ReceiveDeserializedAsync_Should_YieldDeserialized_AndReturnBuffer()
    {
        var owner = new RentedConsumerTests.TrackingMemoryOwner(1024);
        IReadOnlyList<RentedMessageResponse> messages = RentedConsumerTests.BuildMessages(owner, 3);
        var rental = new PolledMessagesRental(owner)
        {
            PartitionId = 1,
            CurrentOffset = 2,
            Messages = messages
        };
        Mock<IIggyClient> client
            = RentedConsumerTests.BuildClientMock(new Queue<PolledMessagesRental>(new[] { rental }));
        var deserializer = new StringDeserializer();
        var consumer
            = new IggyConsumer<string>(client.Object, BuildTypedConfig(deserializer), NullLoggerFactory.Instance);
        await consumer.InitAsync(TestContext.Current.CancellationToken);

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var got = new List<ReceivedMessage<string>>();
        await using (IAsyncEnumerator<ReceivedMessage<string>> e = consumer
                         .ReceiveDeserializedAsync(cts.Token)
                         .GetAsyncEnumerator(TestContext.Current.CancellationToken))
        {
            for (var i = 0; i < 3; i++)
            {
                Assert.True(await e.MoveNextAsync());
                got.Add(e.Current);
            }
        }

        Assert.Equal(3, got.Count);
        for (var i = 0; i < 3; i++)
        {
            Assert.Equal(MessageStatus.Success, got[i].Status);
            Assert.Equal($"msg-{i}", got[i].Data);
            Assert.Equal((ulong)i, got[i].CurrentOffset);
            Assert.Equal(1u, got[i].PartitionId);
            Assert.Null(got[i].Error);
        }

        // Entire batch deserialized before first yield; rental already returned to pool.
        Assert.Equal(1, owner.DisposeCount);
        await consumer.DisposeAsync();
    }

    [Fact]
    public async Task ReceiveDeserializedAsync_Should_ReleaseEntireBatch_BeforeFirstYield()
    {
        var owner = new RentedConsumerTests.TrackingMemoryOwner(1024);
        IReadOnlyList<RentedMessageResponse> messages = RentedConsumerTests.BuildMessages(owner, 3);
        var rental = new PolledMessagesRental(owner)
        {
            PartitionId = 1,
            CurrentOffset = 2,
            Messages = messages
        };
        Mock<IIggyClient> client
            = RentedConsumerTests.BuildClientMock(new Queue<PolledMessagesRental>(new[] { rental }));
        var deserializer = new StringDeserializer();
        var consumer
            = new IggyConsumer<string>(client.Object, BuildTypedConfig(deserializer), NullLoggerFactory.Instance);
        await consumer.InitAsync(TestContext.Current.CancellationToken);

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        await using IAsyncEnumerator<ReceivedMessage<string>> e = consumer.ReceiveDeserializedAsync(cts.Token)
            .GetAsyncEnumerator(TestContext.Current.CancellationToken);

        Assert.True(await e.MoveNextAsync());
        Assert.Equal(1, owner.DisposeCount);
        Assert.Equal("msg-0", e.Current.Data);

        // Remaining messages come from the pre-deserialized list — no further disposal needed.
        Assert.True(await e.MoveNextAsync());
        Assert.Equal(1, owner.DisposeCount);
        Assert.Equal("msg-1", e.Current.Data);

        Assert.True(await e.MoveNextAsync());
        Assert.Equal(1, owner.DisposeCount);
        Assert.Equal("msg-2", e.Current.Data);

        await consumer.DisposeAsync();
    }

    [Fact]
    public async Task DeserializerThrows_Should_YieldDeserializationFailed_AndStillReturnBuffer()
    {
        var owner = new RentedConsumerTests.TrackingMemoryOwner(1024);
        IReadOnlyList<RentedMessageResponse> messages = RentedConsumerTests.BuildMessages(owner, 1);
        var rental = new PolledMessagesRental(owner)
        {
            PartitionId = 1,
            CurrentOffset = 0,
            Messages = messages
        };
        Mock<IIggyClient> client
            = RentedConsumerTests.BuildClientMock(new Queue<PolledMessagesRental>(new[] { rental }));
        var deserializer = new StringDeserializer { ThrowOnNext = true };
        var consumer
            = new IggyConsumer<string>(client.Object, BuildTypedConfig(deserializer), NullLoggerFactory.Instance);
        await consumer.InitAsync(TestContext.Current.CancellationToken);

        var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        ReceivedMessage<string>? got = null;
        await using (IAsyncEnumerator<ReceivedMessage<string>> e = consumer
                         .ReceiveDeserializedAsync(cts.Token)
                         .GetAsyncEnumerator(TestContext.Current.CancellationToken))
        {
            Assert.True(await e.MoveNextAsync());
            got = e.Current;
        }

        Assert.NotNull(got);
        Assert.Equal(MessageStatus.DeserializationFailed, got!.Status);
        Assert.Null(got.Data);
        Assert.IsType<InvalidOperationException>(got.Error);

        // Even on deserialization failure, the using-block inside the typed consumer releases the rental.
        Assert.Equal(1, owner.DisposeCount);
        await consumer.DisposeAsync();
    }

    private static IggyConsumerConfig<string> BuildTypedConfig(IDeserializer<string> deserializer)
    {
        return new IggyConsumerConfig<string>
        {
            StreamId = Identifier.Numeric(1),
            TopicId = Identifier.Numeric(1),
            Consumer = Consumer.New(1),
            PollingStrategy = PollingStrategy.Next(),
            BatchSize = 10,
            PartitionId = 1,
            AutoCommitMode = AutoCommitMode.Disabled,
            AutoCommit = false,
            PollingIntervalMs = 0,
            Deserializer = deserializer
        };
    }

    private sealed class StringDeserializer : IDeserializer<string>
    {
        public bool ThrowOnNext { get; set; }

        public string Deserialize(ReadOnlyMemory<byte> data)
        {
            if (ThrowOnNext)
            {
                throw new InvalidOperationException("deserialize fail");
            }

            return Encoding.UTF8.GetString(data.Span);
        }
    }
}
