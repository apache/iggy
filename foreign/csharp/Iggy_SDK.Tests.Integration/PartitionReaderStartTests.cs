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
using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Extensions;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations;

/// <summary>
///     The server takes only an offset or Next as the target of a fetch, so a reader finds a timestamp or Last position
///     with a poll first. A fake server that takes every target cannot catch a reader that fetches one, so these tests
///     read from a real server. Offsets 0 to 4 and 5 to 9 arrive in two batches, and the server stamps each batch with
///     its own append time.
/// </summary>
public class PartitionReaderStartTests
{
    private const string TopicName = "reader-start-topic";
    private const uint PartitionsCount = 1;
    private const uint PartitionId = 0;
    private const uint BatchMessages = 5;
    private const uint ReaderCount = 3;
    private const ulong FirstOffset = 0;
    private const ulong SecondBatchOffset = BatchMessages;
    private const ulong LastOffset = 2 * BatchMessages - 1;

    private static readonly Identifier Topic = Identifier.String(TopicName);

    /// <summary>Gives the second batch a later append time than the first.</summary>
    private static readonly TimeSpan BatchGap = TimeSpan.FromMilliseconds(20);

    private static readonly TimeSpan ReadWait = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan ReadTimeout = TimeSpan.FromSeconds(30);

    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture Fixture { get; init; }

    public static IEnumerable<Func<(Protocol, CommitPolicy)>> ProtocolAndCommitData()
    {
        return from protocol in IggyServerFixture.ProtocolData()
               from commit in new[] { CommitPolicy.OnRead, CommitPolicy.Manual }
               select new Func<(Protocol, CommitPolicy)>(() => (protocol(), commit));
    }

    [Test]
    [MethodDataSource(nameof(ProtocolAndCommitData))]
    public async Task PartitionReader_With_TimestampStart_Should_Deliver_TheMessageAtTheTimestampFirst(
        Protocol protocol, CommitPolicy commit)
    {
        await using var stream = await StartStream.CreateAsync(Fixture, protocol);
        await using var reader = await stream.ReaderAsync("timestamp-start",
            PollingStrategy.Timestamp(stream.SecondBatchAt), commit);

        (await TakeAsync(reader, 1)).ShouldBe([SecondBatchOffset]);
    }

    [Test]
    [MethodDataSource(nameof(ProtocolAndCommitData))]
    public async Task PartitionReader_With_LastStart_Should_Deliver_TheLastCountMessagesFirst(Protocol protocol,
        CommitPolicy commit)
    {
        await using var stream = await StartStream.CreateAsync(Fixture, protocol);
        await using var reader = await stream.ReaderAsync("last-start", PollingStrategy.Last(), commit);

        (await TakeAsync(reader, ReaderCount)).ShouldBe(Offsets(LastOffset + 1 - ReaderCount, ReaderCount));
    }

    [Test]
    [MethodDataSource(nameof(ProtocolAndCommitData))]
    public async Task PartitionReader_After_ATimestampSeek_Should_Deliver_TheMessageAtTheTimestampNext(
        Protocol protocol, CommitPolicy commit)
    {
        await using var stream = await StartStream.CreateAsync(Fixture, protocol);
        await using var reader = await stream.ReaderAsync("next-start", PollingStrategy.Next(), commit);
        var first = await TakeAsync(reader, 1);
        using var timeout = new CancellationTokenSource(ReadTimeout);

        await reader.SeekAsync(PartitionId, PollingStrategy.Timestamp(stream.SecondBatchAt), timeout.Token);

        first.ShouldBe([FirstOffset]);
        (await TakeAsync(reader, 1)).ShouldBe([SecondBatchOffset]);
    }

    /// <summary>
    ///     With a commit on read the start applies only to a partition without a stored offset, so a new reader with
    ///     the same start resumes after the offset that the close of the last reader left.
    /// </summary>
    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task OnReadReader_With_TimestampStart_Should_Resume_AfterTheStoredOffset(Protocol protocol)
    {
        await using var stream = await StartStream.CreateAsync(Fixture, protocol);
        var start = PollingStrategy.Timestamp(stream.SecondBatchAt);
        await using var reader = await stream.ReaderAsync("resume", start, CommitPolicy.OnRead);
        var read = await TakeAsync(reader, BatchMessages);
        await reader.CloseAsync();
        await stream.SendAsync(1);

        await using var resumed = await stream.ReaderAsync("resume", start, CommitPolicy.OnRead);

        read.ShouldBe(Offsets(SecondBatchOffset, BatchMessages));
        (await TakeAsync(resumed, 1)).ShouldBe([LastOffset + 1]);
    }

    /// <summary>The offsets of the first messages that the reader delivers, over as many batches as they take.</summary>
    private static async Task<ulong[]> TakeAsync(PartitionReader reader, uint count)
    {
        using var timeout = new CancellationTokenSource(ReadTimeout);
        List<ulong> offsets = [];
        try
        {
            while (offsets.Count < count)
            {
                var batch = await reader.NextAsync(timeout.Token);
                offsets.AddRange(batch.Messages.Select(message => message.Header.Offset));
            }
        }
        catch (OperationCanceledException) when (timeout.IsCancellationRequested)
        {
            // The assertion of the caller then shows the messages that did arrive.
        }

        return [.. offsets.Take((int)count)];
    }

    private static ulong[] Offsets(ulong first, uint count)
    {
        return [.. Enumerable.Range(0, (int)count).Select(index => first + (ulong)index)];
    }

    /// <summary>The stream of one test, deleted at its end, because the tests share the server.</summary>
    private sealed class StartStream(IIggyClient client, Identifier stream) : IAsyncDisposable
    {
        /// <summary>The append time of the second batch in microseconds, which offset 5 carries.</summary>
        public ulong SecondBatchAt { get; private set; }

        public static async Task<StartStream> CreateAsync(IggyServerFixture fixture, Protocol protocol)
        {
            var client = await fixture.CreateAuthenticatedClient(protocol);
            var name = $"reader-start-{Guid.NewGuid():N}";
            var stream = Identifier.String(name);
            await client.CreateStreamAsync(name);
            await client.CreateTopicAsync(stream, TopicName, PartitionsCount);
            var created = new StartStream(client, stream);
            await created.SendAsync(BatchMessages);
            await Task.Delay(BatchGap);
            await created.SendAsync(BatchMessages);

            var polled = await client.PollMessagesAsync(stream, Topic, PartitionId, Consumer.New("timestamps"),
                PollingStrategy.Offset(FirstOffset), new PollOptions { Count = 2 * BatchMessages });
            var stamps = polled.Messages.ToDictionary(message => message.Header.Offset,
                message => message.Header.Timestamp);
            stamps[SecondBatchOffset - 1].ShouldBeLessThan(stamps[SecondBatchOffset]);
            created.SecondBatchAt = DateTimeOffsetUtils.ToUnixTimeMicroSeconds(stamps[SecondBatchOffset]);

            return created;
        }

        public async ValueTask DisposeAsync()
        {
            await client.DeleteStreamAsync(stream);
            client.Dispose();
        }

        public Task SendAsync(uint count)
        {
            return client.SendMessagesAsync(stream, Topic, Partitioning.PartitionId(PartitionId),
                [.. Enumerable.Range(0, (int)count).Select(_ => new Message(Guid.NewGuid(), "start"u8.ToArray()))]);
        }

        public Task<PartitionReader> ReaderAsync(string consumer, PollingStrategy start, CommitPolicy commit)
        {
            return client.Reader(stream, Topic, Consumer.New(consumer))
                .WithPartitions(PartitionId)
                .WithStart(start)
                .WithOptions(new PollOptions { Count = ReaderCount, MaxWait = ReadWait, Commit = commit })
                .BuildAsync();
        }
    }
}
