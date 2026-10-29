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

using System.Diagnostics;
using System.Text;
using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Apache.Iggy.Tests.Integrations.Fixtures;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.Integrations;

/// <summary>
///     Deferred polls park on the server until messages arrive or their max wait ends. The
///     tests send while a request waits, so a request that the server did not park, or did not wake, fails them.
///     They run alone, because the load of the other tests on the shared server stretches a reply past the bounds.
/// </summary>
[NotInParallel]
public class DeferredPollTests
{
    private const uint PartitionsCount = 3;
    private const string TopicName = "deferred-topic";

    /// <summary>Longer than any test waits, so only an arriving message ends a parked request in time.</summary>
    private static readonly DeferredPollOptions LongWait = new() { MaxWait = TimeSpan.FromSeconds(10) };

    private static readonly DeferredPollOptions ShortWait = new() { MaxWait = TimeSpan.FromMilliseconds(300) };

    /// <summary>How long a request parks before the test sends the message that wakes it.</summary>
    private static readonly TimeSpan ParkDelay = TimeSpan.FromMilliseconds(500);

    /// <summary>A woken request replies well inside this, and a request that stays parked takes its max wait.</summary>
    private static readonly TimeSpan PromptReply = TimeSpan.FromSeconds(2);

    [ClassDataSource<IggyServerFixture>(Shared = SharedType.PerAssembly)]
    public required IggyServerFixture Fixture { get; init; }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DeferredPoll_Should_Return_AMessageThatArrivesWhileItWaits(Protocol protocol)
    {
        var (client, streamName) = await CreateTopicAsync(protocol);
        var polling = PollAsync(client, streamName, LongWait, CancellationToken.None);
        await Task.Delay(ParkDelay);
        polling.IsCompleted.ShouldBeFalse();

        var sent = Stopwatch.StartNew();
        await SendAsync(client, streamName, 0);
        var polled = await polling;

        sent.Elapsed.ShouldBeLessThan(PromptReply);
        polled.Messages.Count.ShouldBe(1);
    }

    [Test]
    [MethodDataSource<IggyServerFixture>(nameof(IggyServerFixture.ProtocolData))]
    public async Task DeferredPoll_Without_Messages_Should_ReturnEmpty_After_MaxWait(Protocol protocol)
    {
        var (client, streamName) = await CreateTopicAsync(protocol);
        var started = Stopwatch.StartNew();

        var polled = await PollAsync(client, streamName, ShortWait, CancellationToken.None);

        started.Elapsed.ShouldBeGreaterThanOrEqualTo(ShortWait.MaxWait);
        polled.Messages.ShouldBeEmpty();
    }

    /// <summary>
    ///     The server holds a bounded number of parked requests per session. Canceled polls free their places at
    ///     once, so a later poll without messages parks and ends after its own max wait. A server that still holds
    ///     the canceled polls refuses it until they time out. A message would hide that, because the server answers
    ///     a poll that finds messages without a place.
    /// </summary>
    [Test]
    public async Task DeferredPoll_After_ParkedPollsAreCanceled_Should_Park_For_ItsOwnMaxWait()
    {
        var (client, streamName) = await CreateTopicAsync(Protocol.Tcp);
        using var canceled = new CancellationTokenSource();
        var parked = Enumerable.Range(0, DeferredPollOptions.MAX_PARKED_REQUESTS)
            .Select(_ => PollAsync(client, streamName, LongWait, canceled.Token))
            .ToArray();
        await Task.Delay(ParkDelay);
        await canceled.CancelAsync();
        foreach (var poll in parked)
        {
            await Should.ThrowAsync<OperationCanceledException>(poll);
        }

        var started = Stopwatch.StartNew();
        var polled = await PollAsync(client, streamName, ShortWait, CancellationToken.None);

        started.Elapsed.ShouldBeInRange(ShortWait.MaxWait, PromptReply);
        polled.Messages.ShouldBeEmpty();
    }

    private async Task<(IIggyClient Client, string StreamName)> CreateTopicAsync(Protocol protocol)
    {
        var client = await Fixture.CreateAuthenticatedClient(protocol);
        var streamName = $"deferred-{Guid.NewGuid():N}";

        await client.CreateStreamAsync(streamName);
        await client.CreateTopicAsync(Identifier.String(streamName), TopicName, PartitionsCount);

        return (client, streamName);
    }

    private static Task SendAsync(IIggyClient client, string streamName, uint partitionId)
    {
        return client.SendMessagesAsync(Identifier.String(streamName), Identifier.String(TopicName),
            Partitioning.PartitionId(partitionId),
            new Message(Guid.NewGuid(), Encoding.UTF8.GetBytes($"deferred-payload-{partitionId}")));
    }

    private static Task<PolledMessages> PollAsync(IIggyClient client, string streamName, DeferredPollOptions options,
        CancellationToken token)
    {
        return client.PollMessagesAsync(Identifier.String(streamName), Identifier.String(TopicName), 0,
            Consumer.New(1), PollingStrategy.Offset(0), 10, false, options, token);
    }


}
