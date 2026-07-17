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
using System.Diagnostics;
using Apache.Iggy.Extensions;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Messages;
using Apache.Iggy.Publishers;
using Microsoft.Extensions.Logging.Abstractions;
using Moq;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.UtilityTests;

public class SendUnitTests
{
    [Fact]
    public void ReadyUnit_AppendsAllMessages_AndReportsCount()
    {
        var messages = new[]
        {
            new Message(Guid.NewGuid(), new byte[] { 1 }), new Message(Guid.NewGuid(), new byte[] { 2, 3 })
        };
        var unit = new ReadyUnit(messages, null);

        Assert.Equal(2, unit.Count);

        using var payloadBuffer = new PooledBufferWriter();
        unit.Serialize(payloadBuffer);
        var wire = new List<Message>();
        unit.AppendWire(wire, payloadBuffer.Written);

        Assert.Equal(messages, wire);
    }

    [Fact]
    public void ReadyUnit_ReadyBytes_SumsPayloadLengths()
    {
        var unit = new ReadyUnit([
            new Message(Guid.NewGuid(), new byte[3]),
            new Message(Guid.NewGuid(), new byte[5])
        ], null);

        Assert.Equal(8, unit.ReadyBytes);
    }

    [Fact]
    public void TypedUnit_ReadyBytes_IsZero_BecauseBytesComeFromPayloadBuffer()
    {
        TypedUnit<byte[]> unit = TypedUnit<byte[]>.Batch([new byte[10]], new PassthroughSerializer());

        Assert.Equal(0, unit.ReadyBytes);
    }

    [Fact]
    public void ReadyUnit_HasNoDroppedValues()
    {
        var unit = new ReadyUnit([new Message(Guid.NewGuid(), new byte[] { 1 })], null);

        Assert.Null(unit.DroppedValues);
    }

    [Fact]
    public void TypedUnit_ExposesOriginalValuesAsDroppedValues()
    {
        var payloads = new[] { new byte[] { 1 }, new byte[] { 2, 3 } };
        TypedUnit<byte[]> unit = TypedUnit<byte[]>.Batch(payloads, new PassthroughSerializer());

        Assert.Equal(payloads.Cast<object?>().ToArray(), unit.DroppedValues);
    }

    [Fact]
    public void ReadyUnit_Dispose_DisposesOwnerOnce()
    {
        var owner = new TrackingDisposable();
        var unit = new ReadyUnit([new Message(Guid.NewGuid(), new byte[] { 1 })], owner);

        unit.Dispose();

        Assert.Equal(1, owner.DisposeCount);
    }

    [Fact]
    public void ReadyUnit_NullOwner_DisposeIsNoOp()
    {
        var unit = new ReadyUnit([new Message(Guid.NewGuid(), new byte[] { 1 })], null);

        var exception = Record.Exception(() => unit.Dispose());

        Assert.Null(exception);
    }

    [Fact]
    public void TypedUnit_Batch_SerializesIntoPayloadBuffer_AndSlicesInOrder()
    {
        var payloads = new[] { new byte[] { 1, 2 }, new byte[] { 3, 4, 5 }, new byte[] { 6 } };
        TypedUnit<byte[]> unit = TypedUnit<byte[]>.Batch(payloads, new PassthroughSerializer());

        using var payloadBuffer = new PooledBufferWriter(2);
        unit.Serialize(payloadBuffer);
        var wire = new List<Message>();
        unit.AppendWire(wire, payloadBuffer.Written);

        Assert.Equal(3, wire.Count);
        for (var i = 0; i < payloads.Length; i++)
        {
            Assert.Equal(payloads[i], wire[i].Payload.ToArray());
        }
    }

    [Fact]
    public void TypedUnit_Single_PreservesIdAndHeaders()
    {
        var id = Guid.NewGuid().ToUInt128();
        TypedUnit<byte[]> unit = TypedUnit<byte[]>.Single(new byte[] { 9, 9 }, id, null, new PassthroughSerializer());

        Assert.Equal(1, unit.Count);

        using var payloadBuffer = new PooledBufferWriter();
        unit.Serialize(payloadBuffer);
        var wire = new List<Message>();
        unit.AppendWire(wire, payloadBuffer.Written);

        Assert.Equal(id, wire[0].Header.Id);
        Assert.Equal(new byte[] { 9, 9 }, wire[0].Payload.ToArray());
    }

    [Fact]
    public async Task Processor_SendsUnitWhole_AndDisposesOwnerOnce()
    {
        var sentCounts = new List<int>();
        Mock<IIggyClient> client = MockClient(sentCounts);
        var config = BackgroundConfig(10);

        await using var processor = new BackgroundMessageProcessor(client.Object, config, NullLoggerFactory.Instance);
        processor.Start();

        var owner = new TrackingDisposable();
        var messages = new Message[25];
        for (var i = 0; i < messages.Length; i++)
        {
            messages[i] = new Message(Guid.NewGuid(), new[] { (byte)i });
        }

        await processor.EnqueueAsync(new ReadyUnit(messages, owner), CancellationToken.None);
        await WaitUntil(() => owner.DisposeCount == 1);

        Assert.Single(sentCounts);
        Assert.Equal(25, sentCounts[0]);
        Assert.Equal(1, owner.DisposeCount);
    }

    [Fact]
    public async Task Processor_FlushesOnByteLimit_BeforeCountLimit()
    {
        var sentCounts = new List<int>();
        Mock<IIggyClient> client = MockClient(sentCounts);
        // 100-byte messages, 250-byte cap => at most 3 messages per flush; the count limit never trips.
        var config = BackgroundConfig(1000);
        config.BackgroundMaxBatchBytes = 250;

        await using var processor = new BackgroundMessageProcessor(client.Object, config, NullLoggerFactory.Instance);

        // Enqueue before Start so the first flush drains the full backlog in cap-sized chunks deterministically.
        for (var i = 0; i < 10; i++)
        {
            await processor.EnqueueAsync(new ReadyUnit([new Message(Guid.NewGuid(), new byte[100])], null),
                CancellationToken.None);
        }

        processor.Start();
        await WaitUntil(() => sentCounts.Sum() == 10);

        Assert.All(sentCounts, count => Assert.True(count <= 3, $"flush of {count} messages exceeded the byte cap"));
        Assert.Equal(10, sentCounts.Sum());
    }

    [Fact]
    public async Task Processor_Dispose_DisposesQueuedOwners()
    {
        Mock<IIggyClient> client = MockClient(new List<int>());
        var config = BackgroundConfig(10);

        var processor = new BackgroundMessageProcessor(client.Object, config, NullLoggerFactory.Instance);

        var owner = new TrackingDisposable();
        await processor.EnqueueAsync(new ReadyUnit([new Message(Guid.NewGuid(), new byte[] { 1 })], owner),
            CancellationToken.None);

        await processor.DisposeAsync();

        Assert.Equal(1, owner.DisposeCount);
    }

    [Fact]
    public async Task Processor_DrainStaysPending_UntilSendCompletes()
    {
        // Gate the send so the unit is in flight; the drain must not complete mid-flight.
        var gate = new TaskCompletionSource();
        var client = new Mock<IIggyClient>();
        client.Setup(c => c.SendMessagesAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(),
                It.IsAny<Partitioning>(), It.IsAny<IList<Message>>(), It.IsAny<CancellationToken>()))
            .Returns(gate.Task);
        var config = BackgroundConfig(10);

        await using var processor = new BackgroundMessageProcessor(client.Object, config, NullLoggerFactory.Instance);
        processor.Start();

        await processor.EnqueueAsync(new ReadyUnit([new Message(Guid.NewGuid(), new byte[] { 1 })], null),
            CancellationToken.None);

        var drain = processor.WaitForDrainAsync(CancellationToken.None);

        await Task.Delay(100, TestContext.Current.CancellationToken);
        Assert.False(drain.IsCompleted);

        gate.SetResult();
        await drain.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
    }

    private static Mock<IIggyClient> MockClient(List<int> sentCounts)
    {
        var client = new Mock<IIggyClient>();
        client.Setup(c => c.SendMessagesAsync(It.IsAny<Identifier>(), It.IsAny<Identifier>(),
                It.IsAny<Partitioning>(), It.IsAny<IList<Message>>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask)
            .Callback((Identifier _, Identifier _, Partitioning _, IList<Message> messages, CancellationToken _) =>
                sentCounts.Add(messages.Count));
        return client;
    }

    private static IggyPublisherConfig BackgroundConfig(int batchSize)
    {
        return new IggyPublisherConfig
        {
            StreamId = Identifier.Numeric(1),
            TopicId = Identifier.Numeric(1),
            BackgroundBatchSize = batchSize,
            BackgroundQueueCapacity = 1000,
            BackgroundFlushInterval = TimeSpan.FromMilliseconds(10),
            BackgroundDisposalTimeout = TimeSpan.FromSeconds(5),
            EnableRetry = false
        };
    }

    private static async Task WaitUntil(Func<bool> condition)
    {
        var stopwatch = Stopwatch.StartNew();
        while (!condition())
        {
            if (stopwatch.Elapsed > TimeSpan.FromSeconds(5))
            {
                throw new TimeoutException("Condition was not met in time.");
            }

            await Task.Delay(5);
        }
    }

    private sealed class TrackingDisposable : IDisposable
    {
        public int DisposeCount { get; private set; }

        public void Dispose()
        {
            DisposeCount++;
        }
    }

    private sealed class PassthroughSerializer : ISerializer<byte[]>
    {
        public void Serialize(byte[] data, IBufferWriter<byte> writer)
        {
            data.CopyTo(writer.GetSpan(data.Length));
            writer.Advance(data.Length);
        }
    }
}
