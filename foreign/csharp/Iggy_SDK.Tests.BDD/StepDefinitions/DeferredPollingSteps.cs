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
using Apache.Iggy.Exceptions;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Reqnroll;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;
using TestContext = Apache.Iggy.Tests.BDD.Context.TestContext;

namespace Apache.Iggy.Tests.BDD.StepDefinitions;

/// <summary>
///     A start step leaves its deferred poll or fetch parked on the server, and a later step sends the
///     message that wakes it. All calls share the client of the scenario, which multiplexes them on one link.
/// </summary>
[Binding]
public sealed class DeferredPollingSteps : IDisposable
{
    private const uint MessagesCount = 10;

    /// <summary>The SDK refuses a request timeout that does not cover the max wait plus this margin.</summary>
    private static readonly TimeSpan ReplyMargin = TimeSpan.FromSeconds(1);

    /// <summary>
    ///     A message that reaches the server before a call answers it at once, so a start step waits until its call
    ///     parks.
    /// </summary>
    private static readonly TimeSpan ParkTime = TimeSpan.FromMilliseconds(100);

    private static readonly TimeSpan AssignmentTimeout = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan AssignmentPollInterval = TimeSpan.FromMilliseconds(100);
    private static readonly Consumer DefaultConsumer = Consumer.New(0);

    private readonly CancellationTokenSource _cancellation = new();
    private readonly TestContext _context;
    private string? _groupName;
    private Task<PolledMessages>? _poll;
    private Stopwatch? _pollStarted;
    private Task<FetchedMessages>? _fetch;
    private StreamResponse? _stream;
    private TopicResponse? _topic;

    public DeferredPollingSteps(TestContext context)
    {
        _context = context;
    }

    public void Dispose()
    {
        _cancellation.Dispose();
    }

    // Other features expect a server without streams.
    [AfterScenario(Order = 0)]
    public async Task DeleteCreatedStream()
    {
        if (_stream is not null)
        {
            await _context.IggyClient.DeleteStreamAsync(StreamId());
        }
    }

    [Given("a stream named {string} with a topic named {string} with {int} partitions exists")]
    public async Task CreateStreamWithTopic(string streamName, string topicName, int partitions)
    {
        if (await _context.IggyClient.GetStreamByIdAsync(Identifier.String(streamName)) is not null)
        {
            await _context.IggyClient.DeleteStreamAsync(Identifier.String(streamName));
        }

        _stream = await _context.IggyClient.CreateStreamAsync(streamName);
        _topic = await _context.IggyClient.CreateTopicAsync(StreamId(), topicName, (uint)partitions);
    }

    [When("I start a deferred poll of partition {int} from offset {int} with a max wait of {int} second(s)")]
    public Task StartDeferredPoll(int partition, int offset, int maxWaitSeconds)
    {
        return StartDeferredPollAsync(partition, DefaultConsumer, PollingStrategy.Offset((ulong)offset), false,
            DeferredOptions(maxWaitSeconds));
    }

    [When("I start a deferred poll of partition {int} for consumer {string} with auto commit, a min count of {int} and a max wait of {int} second(s)")]
    public Task StartCommittingDeferredPoll(int partition, string consumerName, int minCount, int maxWaitSeconds)
    {
        return StartDeferredPollAsync(partition, Consumer.New(consumerName), PollingStrategy.Next(), true,
            DeferredOptions(maxWaitSeconds) with { MinCount = (uint)minCount });
    }

    [When("I send a message with payload {string} to partition {int}")]
    public Task SendToPartition(string payload, int partition)
    {
        return _context.IggyClient.SendMessagesAsync(StreamId(), TopicId(), Partitioning.PartitionId(partition),
            [new Message(Guid.NewGuid(), Encoding.UTF8.GetBytes(payload))]);
    }

    [Then("the deferred poll should return the payload {string} within {int} second(s)")]
    public async Task DeferredPollReturns(string payload, int withinSeconds)
    {
        var polled = await Within(_poll.ShouldNotBeNull(), TimeSpan.FromSeconds(withinSeconds), "deferred poll");
        ShouldHaveSinglePayload(polled, payload);
    }

    [Then("the deferred poll should return no messages between {int} and {int} second(s) after it started")]
    public async Task DeferredPollReturnsNothing(int fromSeconds, int toSeconds)
    {
        var started = _pollStarted.ShouldNotBeNull();
        var polled = await Within(_poll.ShouldNotBeNull(), TimeSpan.FromSeconds(toSeconds) - started.Elapsed,
            "deferred poll");

        started.Elapsed.ShouldBeGreaterThanOrEqualTo(TimeSpan.FromSeconds(fromSeconds));
        polled.Messages.ShouldBeEmpty();
    }

    [When("I start a fetch on partitions {int}, {int} and {int} from offset {int} with a max wait of {int} second(s)")]
    public async Task StartFetch(int first, int second, int third, int offset, int maxWaitSeconds)
    {
        var partitions = new[] { first, second, third }
            .Select(partition => (uint)partition).ToArray();
        var group = (await _context.IggyClient.ReadinessGroupsAsync(StreamId(), TopicId(), partitions))
            .ShouldHaveSingleItem();
        _fetch = _context.IggyClient.FetchMessagesAsync(StreamId(), TopicId(),
            [.. group.Select(partition => (partition, PollingStrategy.Offset((ulong)offset)))], DefaultConsumer,
            MessagesCount, false, DeferredOptions(maxWaitSeconds), _cancellation.Token);

        await Task.Delay(ParkTime);
        _fetch.IsCompleted.ShouldBeFalse("the fetch must park");
    }

    [Then("the fetch should return only partition {int} within {int} second(s)")]
    public async Task FetchReturns(int partition, int withinSeconds)
    {
        using var fetched = await Within(_fetch.ShouldNotBeNull(), TimeSpan.FromSeconds(withinSeconds), "fetch");
        var served = fetched.Sections.ShouldHaveSingleItem();
        served.PartitionId.ShouldBe((uint)partition);
        served.Decode().Messages.ShouldHaveSingleItem();
    }

    [Given("I am the only member of consumer group {string} on the topic")]
    public async Task JoinAsOnlyMember(string groupName)
    {
        var groupId = Identifier.String(groupName);
        await _context.IggyClient.CreateConsumerGroupAsync(StreamId(), TopicId(), groupName);
        await _context.IggyClient.JoinConsumerGroupAsync(StreamId(), TopicId(), groupId);

        // The join can return before the server assigns the partitions.
        var partitions = _topic.ShouldNotBeNull().PartitionsCount;
        var waited = Stopwatch.StartNew();
        while (await _context.IggyClient.GetConsumerGroupByIdAsync(StreamId(), TopicId(), groupId) is not
            { Members: [var member] } || member.PartitionsCount != partitions)
        {
            waited.Elapsed.ShouldBeLessThan(AssignmentTimeout, "the member did not get every partition");
            await Task.Delay(AssignmentPollInterval);
        }

        _groupName = groupName;
    }

    [Then("a group fetch with a max wait of {int} second(s) should return the payload {string} within {int} second(s)")]
    public async Task GroupFetchReturns(int maxWaitSeconds, string payload, int withinSeconds)
    {
        var partitions = Enumerable.Range(0, (int)_topic.ShouldNotBeNull().PartitionsCount)
            .Select(partition => (uint)partition).ToArray();
        var group = (await _context.IggyClient.ReadinessGroupsAsync(StreamId(), TopicId(), partitions))
            .ShouldHaveSingleItem();
        var fetch = _context.IggyClient.FetchMessagesAsync(StreamId(), TopicId(),
            [.. group.Select(partition => (partition, PollingStrategy.Next()))],
            Consumer.Group(_groupName.ShouldNotBeNull()), MessagesCount, true, DeferredOptions(maxWaitSeconds));
        using var fetched = await Within(fetch, TimeSpan.FromSeconds(withinSeconds), "group fetch");
        var served = fetched.Sections.Where(section => section.MessagesCount > 0).ShouldHaveSingleItem();
        ShouldHaveSinglePayload(served.Decode(), payload);
    }

    [When("I cancel the deferred poll after {int} milliseconds")]
    public async Task CancelDeferredPoll(int milliseconds)
    {
        var poll = _poll.ShouldNotBeNull();
        await Task.Delay(milliseconds);
        await _cancellation.CancelAsync();

        var error = await Record.ExceptionAsync(() => poll);
        // The server can commit before the cancel arrives, so the SDK reports a canceled poll that can move an offset
        // as an unknown outcome, with the cancellation inside.
        var cause = error is VsrRequestOutcomeUnknownException ? error.InnerException : error;
        cause.ShouldBeAssignableTo<OperationCanceledException>(
            $"the deferred poll must end as canceled, not with {error?.ToString() ?? "a result"}");
    }

    [When("I wait {int} second(s)")]
    public Task Wait(int seconds)
    {
        return Task.Delay(TimeSpan.FromSeconds(seconds));
    }

    [Then("a poll of partition {int} for consumer {string} with the next strategy should return the payload {string}")]
    public async Task NextPollReturns(int partition, string consumerName, string payload)
    {
        var polled = await _context.IggyClient.PollMessagesAsync(StreamId(), TopicId(), (uint)partition,
            Consumer.New(consumerName), PollingStrategy.Next(), MessagesCount, false);
        polled.Messages.Select(message => Encoding.UTF8.GetString(message.Payload)).FirstOrDefault()
            .ShouldBe(payload);
    }

    private async Task StartDeferredPollAsync(int partition, Consumer consumer, PollingStrategy pollingStrategy,
        bool autoCommit, DeferredPollOptions options)
    {
        _pollStarted = Stopwatch.StartNew();
        _poll = _context.IggyClient.PollMessagesAsync(StreamId(), TopicId(), (uint)partition, consumer,
            pollingStrategy, MessagesCount, autoCommit, options, _cancellation.Token);

        await Task.Delay(ParkTime);
        _poll.IsCompleted.ShouldBeFalse("the deferred poll must park");
    }

    /// <summary>The SDK defaults with the max wait of the step.</summary>
    private static DeferredPollOptions DeferredOptions(int maxWaitSeconds)
    {
        var defaults = new DeferredPollOptions();
        var maxWait = TimeSpan.FromSeconds(maxWaitSeconds);
        var shortestTimeout = maxWait + ReplyMargin;
        return defaults with
        {
            MaxWait = maxWait,
            RequestTimeout = shortestTimeout > defaults.RequestTimeout ? shortestTimeout : defaults.RequestTimeout
        };
    }

    private static async Task<T> Within<T>(Task<T> call, TimeSpan limit, string name)
    {
        if (await Task.WhenAny(call, Task.Delay(limit)) != call)
        {
            Assert.Fail($"The {name} did not return within {limit}.");
        }

        return await call;
    }

    private static void ShouldHaveSinglePayload(PolledMessages polled, string payload)
    {
        Encoding.UTF8.GetString(polled.Messages.ShouldHaveSingleItem().Payload).ShouldBe(payload);
    }

    private Identifier StreamId()
    {
        return Identifier.Numeric(_stream.ShouldNotBeNull().Id);
    }

    private Identifier TopicId()
    {
        return Identifier.Numeric(_topic.ShouldNotBeNull().Id);
    }
}
